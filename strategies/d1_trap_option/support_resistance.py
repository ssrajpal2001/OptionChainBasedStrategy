"""
strategies/d1_trap_option/support_resistance.py — ported from the user's
"ping-pong" Support & Resistance state machine (2026-08-07), for evaluation
as a replacement entry/SL mechanic inside BearTrap's existing HTF zone
(see scripts/d1trap_sr_zone_backtest.py -- this module is NOT wired into the
live D1TrapBearOnlyBook, evaluation only).

Core logic (process_straddle_candle, get_calculated_sr_state,
_initialize_instrument) is UNCHANGED from the source the user provided --
only the logging import was swapped (this repo has no utils.logger module)
and the pandas-specific `get_sr_status_shared` replay wrapper was dropped
since the backtest script drives process_straddle_candle directly per-bar
itself (same pattern every other backtest script in this repo uses), rather
than needing that helper's own OHLC-DataFrame-indexed replay logic.

Mechanic summary (see module docstring in the backtest script for the full
write-up): starts at a "base range" (Phase 0), and once price makes a clean
directional break (higher high AND higher low, or the mirror), tracks that
break's high as resistance (R1) while the prior candle's low locks in as
support (S1). A full reversal through the established level triggers a
"directional flip" (level resets, tracking swaps sides); a partial pullback
that itself reverses gets "promoted" to become the new S1/R1, letting the
ladder walk up (uptrend) or down (downtrend) as confirmed swing points form.
"""
from __future__ import annotations

import logging
from datetime import time
from typing import List, Optional

logger = logging.getLogger(__name__)


def log_sr_details(msg: str) -> None:
    """Verbose per-candle trace, split from the main logger since it's noisy
    -- mirrors the source's separate log_sr_details channel."""
    logger.debug(msg)


# ── S&R ping-pong entry/exit constants (2026-08-08) ─────────────────────────
# Moved here from scripts/d1trap_sr_zone_backtest.py so both the backtest
# script AND the live D1TrapSRBook (strategies/d1_trap_option/sr_book.py)
# import the same values -- these used to live only in a scripts/ file,
# which is the wrong layering direction for something live code now depends
# on. scripts/d1trap_sr_zone_backtest.py imports them back from here.
_ENTRY_CUTOFF = time(14, 30)
_EOD_TIME = time(15, 15)
_MAX_RISK_RS_PER_LOT = 2000.0   # hard backstop, enforced every bar regardless of exit_mode
_SL_BUFFER_PCT = 0.02          # exit_mode="buffered" default buffer: pad the SL 2% below live
                                # S1 -- SRPingPongTracker's own sl_buffer_pct constructor param
                                # overrides this per-instance for the 2026-08-08 wider-buffer sweep.
_PROFIT_TRIGGER_PCT = 0.05     # exit_mode="profit_trigger": SL stays frozen at entry-time S1
                                # until premium has moved 5% in favor, then trails live S1
# 2026-08-08: "hold_eod" added per direct user observation (BANKNIFTY sweep trade log,
# see project_banknifty_backtest_harness memory) -- almost all real losses in the
# validated month were "give-backs": the raw trailing SL climbs with every S1 promotion,
# then a normal pullback (not a genuine reversal) takes out the NEWLY-promoted stop,
# even though the position was never actually wrong. Max profit was consistently only
# realized on trades that ran untouched to EOD. hold_eod removes the soft/trailing SL
# entirely -- only the universal hard Rs2000/lot risk cap and the 15:15 EOD close apply.
_EXIT_MODES = ("raw", "bucket_close", "buffered", "profit_trigger", "hold_eod")


def _sl_for_mode(exit_mode: str, position: dict, live_s1: float, sl_buffer_pct: float = _SL_BUFFER_PCT) -> float:
    """The mode's own structural SL -- NOT risk-capped here. The hard Rs/lot risk cap is
    enforced separately, once per 1-min bar, regardless of exit_mode's own check cadence."""
    if exit_mode == "buffered":
        return live_s1 * (1 - sl_buffer_pct)
    if exit_mode == "profit_trigger":
        return live_s1 if position.get("triggered") else position["initial_sl"]
    return live_s1   # "raw" and "bucket_close" (bucket_close's own structural check lives elsewhere)


class SupportResistanceCalculator:
    def __init__(self):
        # self.states stores the S&R state for each instrument
        # Format: { inst_key: { 'current_phase': str, 'sr_levels': { 'S1': {...}, 'R1': {...}, ... } } }
        self.states = {}

    def get_calculated_sr_state(self, inst_key):
        state = self.states.get(inst_key, {'current_phase': 'UNKNOWN', 'sr_levels': {}})
        sr_levels = state.get('sr_levels', {})
        s1 = sr_levels.get('S1')
        r1 = sr_levels.get('R1')

        state['s1_established'] = s1.get('is_established', False) if s1 else False
        state['r1_established'] = r1.get('is_established', False) if r1 else False
        return state

    def reset_and_process_sequence(self, inst_key, candles):
        """Clears existing state for an instrument and replays a sequence of candles."""
        if inst_key in self.states:
            del self.states[inst_key]

        for candle in candles:
            self.process_straddle_candle(inst_key, candle)

        return self.get_calculated_sr_state(inst_key)

    def process_straddle_candle(self, inst_key, candle_data, silent=False):
        """
        Support & Resistance logic using a granular state machine.
        Follows Ping-Pong logic with Rule #1 as anchor.
        """
        if inst_key not in self.states:
            self._initialize_instrument(inst_key, candle_data)
            return

        ts = candle_data['timestamp']
        high = candle_data['high']
        low = candle_data['low']
        duration = candle_data.get('duration', 1)

        state = self.states[inst_key]
        last_candle = state['last_candle']

        # Skip if older
        if ts < last_candle['timestamp']:
            return
        # Skip if same time and NOT a longer duration (prevents double processing in history replay)
        if ts == last_candle['timestamp'] and duration <= last_candle.get('duration', 1):
            return

        phase = state['current_phase']
        sr_levels = state['sr_levels']
        s1 = sr_levels['S1']
        r1 = sr_levels['R1']

        # --- Phase 0: Base Range Breakout ---
        prev_high = last_candle['high']
        prev_low = last_candle['low']

        if phase == 'INITIAL_TREND_ESTABLISHMENT':
            # 1. OUTSIDE CANDLE: EXPAND BASE
            if high > prev_high and low < prev_low:
                s1['low'] = low
                r1['high'] = high
                s1['timestamp'] = ts
                r1['timestamp'] = ts
                if not silent:
                    logger.debug(f"S&R: Phase 0 Base Expansion (Outside Candle) | Strike: {inst_key} | at {ts} | Range: {low:.2f} - {high:.2f}")

            # 2. BREAKOUT HIGH (BOUNCE): S1 Established at PREVIOUS Low
            elif high > prev_high and low > prev_low:
                s1['low'] = prev_low
                s1['is_established'] = True
                s1['timestamp'] = last_candle['timestamp']

                r1['high'] = high
                r1['breakout_level'] = low
                r1['is_established'] = False
                r1['timestamp'] = ts

                state['current_phase'] = 'R1_TRACKING'
                msg = f"S&R: Phase 0 -> R1_TRACKING (High Breach at {ts}). S1 established at {s1['low']:.2f} | Hurdle: {prev_high:.2f}"
                if not silent:
                    logger.debug(msg)
                    log_sr_details(f"STRIKE: {inst_key} | {msg}")

            # 4. BREAKOUT LOW (PULLBACK): R1 Established at PREVIOUS High
            elif low < prev_low and high < prev_high:
                r1['high'] = prev_high
                r1['is_established'] = True
                r1['timestamp'] = last_candle['timestamp']

                s1['low'] = low
                s1['breakout_level'] = high
                s1['is_established'] = False
                s1['timestamp'] = ts

                state['current_phase'] = 'S1_TRACKING'
                msg = f"S&R: Phase 0 -> S1_TRACKING (Low Breach at {ts}). R1 established at {r1['high']:.2f} | Hurdle: {prev_low:.2f}"
                if not silent:
                    logger.debug(msg)
                    log_sr_details(f"STRIKE: {inst_key} | {msg}")

            state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
            return

        # --- Phase 1: Primary Trend Tracking ---
        if phase == 'R1_TRACKING':
            if low < s1['low']:
                s1_base = s1['low']
                s1['low'] = low
                s1['breakout_level'] = s1_base
                s1['timestamp'] = ts
                s1['is_established'] = False
                sr_levels['S2'] = None
                state['current_phase'] = 'S1_TRACKING'
                msg = f"S&R: Directional Flip -> S1_TRACKING (S1 Breached) | Strike: {inst_key} | at {ts}"
                if not silent:
                    logger.debug(msg)
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            if high > r1['high']:
                r1['high'] = high
                r1['timestamp'] = ts
                r1['is_established'] = False

            if high < prev_high and low < prev_low:
                r1['is_established'] = True
                state['current_phase'] = 'S2_TRACKING'
                sr_levels['S2'] = {'low': low, 'high': high, 'breakout_level': high, 'timestamp': ts, 'is_established': False}
                msg = f"S&R: R1 Established at {r1['high']:.2f} | Confirmed at {ts} (Phase: S2_TRACKING)"
                if not silent:
                    logger.debug(msg)
                    log_sr_details(f"STRIKE: {inst_key} | {msg}")

        elif phase == 'S1_TRACKING':
            if high > r1['high']:
                r1_base = r1['high']
                r1['high'] = high
                r1['breakout_level'] = r1_base
                r1['timestamp'] = ts
                r1['is_established'] = False
                sr_levels['R2'] = None
                state['current_phase'] = 'R1_TRACKING'
                msg = f"S&R: Directional Flip -> R1_TRACKING (R1 Breached) | Strike: {inst_key} | at {ts}"
                if not silent:
                    logger.debug(msg)
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            if low < s1['low']:
                s1['low'] = low
                s1['timestamp'] = ts
                s1['is_established'] = False

            if low > prev_low and high > prev_high:
                s1['is_established'] = True
                state['current_phase'] = 'R2_TRACKING'
                sr_levels['R2'] = {'high': high, 'low': low, 'breakout_level': low, 'timestamp': ts, 'is_established': False}
                msg = f"S&R: S1 Established at {s1['low']:.2f} | Confirmed at {ts} (Phase: R2_TRACKING)"
                if not silent:
                    logger.debug(msg)
                    log_sr_details(f"STRIKE: {inst_key} | {msg}")

        # --- Phase 2: Secondary Tracking (Ping-Pong / Promotion) ---
        elif phase == 'S2_TRACKING':
            s2 = sr_levels['S2']

            if high > r1['high']:
                r1_base = r1['high']
                if sr_levels.get('S2'):
                    old_s1 = s1['low']
                    sr_levels['S1'] = sr_levels['S2'].copy()
                    sr_levels['S1']['is_established'] = True
                    s1 = sr_levels['S1']
                    msg = f"S&R: Scenario A (R1 Breach) + S2->S1 Promotion | Strike: {inst_key} | S1Low: {old_s1:.2f} -> {s1['low']:.2f} | Resetting R1"
                    if not silent:
                        logger.debug(msg)
                        log_sr_details(msg)
                else:
                    msg = f"S&R: Scenario A (R1 Breach) -> R1_TRACKING | Strike: {inst_key} | at {ts}"
                    if not silent:
                        logger.debug(msg)
                        log_sr_details(msg)

                r1['high'] = high
                r1['breakout_level'] = low
                r1['timestamp'] = ts
                sr_levels['S2'] = None
                state['current_phase'] = 'R1_TRACKING'
                r1['is_established'] = False
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            if low < s1['low']:
                s1_base = s1['low']
                s1['low'] = low
                s1['breakout_level'] = s1_base
                s1['timestamp'] = ts
                sr_levels['S2'] = None
                state['current_phase'] = 'S1_TRACKING'
                s1['is_established'] = False
                msg = f"S&R: Directional Flip -> S1_TRACKING (S1 Breached) | Strike: {inst_key} | at {ts}"
                if not silent:
                    logger.debug(msg)
                    log_sr_details(msg)
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            elif low < s2['low']:
                s2['low'] = low
                s2['timestamp'] = ts

            elif low > prev_low and high > prev_high:
                old_s1 = s1['low']
                sr_levels['S1'] = sr_levels['S2'].copy()
                sr_levels['S1']['is_established'] = True
                s1 = sr_levels['S1']
                sr_levels['S2'] = None

                state['current_phase'] = 'R2_TRACKING'
                sr_levels['R2'] = {'high': high, 'low': low, 'breakout_level': low, 'timestamp': ts, 'is_established': False}
                msg = f"S&R: Scenario B (Bounce No Breach) + S2->S1 Promotion | Strike: {inst_key} | S1Low: {old_s1:.2f} -> {s1['low']:.2f} | Tracking R2"
                if not silent:
                    logger.debug(msg)
                    log_sr_details(msg)

        elif phase == 'R2_TRACKING':
            r2 = sr_levels['R2']

            if high > r1['high']:
                r1_base = r1['high']
                sr_levels['R1'] = sr_levels['R2'].copy()
                r1 = sr_levels['R1']
                r1['high'] = high
                r1['breakout_level'] = r1_base
                r1['timestamp'] = ts
                r1['is_established'] = False
                sr_levels['R2'] = None
                state['current_phase'] = 'R1_TRACKING'
                msg = f"S&R: Scenario A (R1 Breach) -> R1_TRACKING | Strike: {inst_key} | at {ts} | R1 took R2 values"
                if not silent:
                    logger.debug(msg)
                    log_sr_details(msg)
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            if low < s1['low']:
                s1_base = s1['low']
                if sr_levels.get('R2'):
                    old_r1 = r1['high']
                    sr_levels['R1'] = sr_levels['R2'].copy()
                    sr_levels['R1']['is_established'] = True
                    r1 = sr_levels['R1']
                    msg = f"S&R: Directional Flip (S1 Breach) + R2->R1 Promotion | Strike: {inst_key} | R1High: {old_r1:.2f} -> {r1['high']:.2f} | Resetting S1"
                    if not silent:
                        logger.debug(msg)
                        log_sr_details(msg)
                else:
                    msg = f"S&R: Directional Flip -> S1_TRACKING (S1 Breached) | Strike: {inst_key} | at {ts}"
                    if not silent:
                        logger.debug(msg)
                        log_sr_details(msg)

                s1['low'] = low
                s1['breakout_level'] = high
                s1['timestamp'] = ts
                sr_levels['R2'] = None
                state['current_phase'] = 'S1_TRACKING'
                s1['is_established'] = False
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            elif high > r2['high']:
                r2['high'] = high
                r2['timestamp'] = ts

            elif high < prev_high and low < prev_low:
                old_r1 = r1['high']
                sr_levels['R1'] = sr_levels['R2'].copy()
                sr_levels['R1']['is_established'] = True
                r1 = sr_levels['R1']
                sr_levels['R2'] = None

                state['current_phase'] = 'S2_TRACKING'
                sr_levels['S2'] = {'low': low, 'high': high, 'breakout_level': high, 'timestamp': ts, 'is_established': False}
                msg = f"S&R: Scenario B (Pullback No Breach) + R2->R1 Promotion | Strike: {inst_key} | R1High: {old_r1:.2f} -> {r1['high']:.2f} | Tracking S2"
                if not silent:
                    logger.debug(msg)
                    log_sr_details(msg)

        state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}

    def _initialize_instrument(self, inst_key, candle_data):
        ts = candle_data['timestamp']
        high = candle_data['high']
        low = candle_data['low']
        duration = candle_data.get('duration', 1)

        self.states[inst_key] = {
            'current_phase': 'INITIAL_TREND_ESTABLISHMENT',
            'last_candle': {'high': high, 'low': low, 'timestamp': ts, 'duration': duration},
            'sr_levels': {
                'S1': {'low': low, 'high': high, 'timestamp': ts, 'is_established': False},
                'R1': {'high': high, 'low': low, 'timestamp': ts, 'is_established': False},
                'S2': None,
                'R2': None
            }
        }


class SRPingPongTracker:
    """Incremental, bar-at-a-time S&R ping-pong state machine for ONE side (CE or PE)
    across a whole trading day, one zone pool. This is the single source of truth for
    the "outer zone (BearTrap's own HTF sweep+reclaim) + inner S&R tracker (entry on
    confirmed R2-breaches-R1 breakout, SL trails live S1)" mechanic validated 2026-08-08
    via the BANKNIFTY sweep (see scripts/d1trap_banknifty_sr_sweep.py) -- both
    scripts/d1trap_sr_zone_backtest.py's _run_sr_variant (backtest) and
    strategies/d1_trap_option/sr_book.py's D1TrapSRBook (live) drive THIS class so the
    two can never silently diverge (mirrors the class-driven-backtest discipline used
    everywhere else in this codebase, applied to a live-first mechanic instead).

    Construct one fresh instance per (side, trading day) -- state does not carry across
    days (matches _run_sr_variant's fresh active_sr={}/position=None per call). At most
    ONE position open at a time across the whole zone pool for this side; once that
    position exits (or the entry cutoff passes with nothing open), no further entries
    fire for the rest of the day -- same one-trade-per-side-per-day shape as the
    original backtest.

    Usage: call on_bar(bar) once per newly-closed 1-minute bar, in timestamp order.
    Returns None most bars; returns an event dict on an entry
    ({"type": "entry", "entry_ts", "entry_premium", "zone_ts", "initial_sl"}) or an exit
    ({"type": "exit", "reason", "exit_price", "exit_ts", "pnl", "entry_ts",
    "entry_premium", "zone_ts"})."""

    def __init__(self, zones: List[dict], tf_minutes: int, lot_size: int, exit_mode: str = "raw",
                 sl_buffer_pct: float = _SL_BUFFER_PCT, gate_mode: str = "touch"):
        self.zones = zones
        self.tf_minutes = tf_minutes
        self.lot_size = lot_size
        self.exit_mode = exit_mode
        self.sl_buffer_pct = sl_buffer_pct   # only read by exit_mode="buffered"
        # 2026-08-09, direct user question ("wait for MTF/LTF trap or immediately
        # start S&R?"): gate_mode controls WHEN a zone starts being fed into its own
        # SupportResistanceCalculator.
        #   "touch"  (Option A, the validated BANKNIFTY config): start the instant
        #            price touches the raw zone band (bar.low <= zone_hi). S&R's own
        #            multi-phase requirement (Phase0 -> R1_TRACKING -> S2_TRACKING ->
        #            R2_TRACKING -> R1_TRACKING) IS the confirmation -- no separate
        #            MTF gate on top of it.
        #   "breach" (Option B): don't start until the zone's own MTF ref-candle
        #            breach_ts has fired (same trigger condition D1TrapBearOnlyBook's
        #            T1 tranche uses) -- requires zones fed here to have been built by
        #            actually driving the real T1/T2 state machine (book._series[side]
        #            .zones), not a bare zone_lo/zone_hi/lock_ts dict, since only that
        #            real state machine populates zone["breach_ts"].
        self.gate_mode = gate_mode
        self.touched_zone_ts: set = set()
        self.active_sr: dict = {}
        self.position: Optional[dict] = None
        self.voided: List[dict] = []
        self.day_done = False

    def _exit(self, reason: str, exit_price: float, exit_ts) -> dict:
        pos = self.position
        pnl = (exit_price - pos["entry_premium"]) * self.lot_size
        ev = dict(type="exit", reason=reason, exit_price=exit_price, exit_ts=exit_ts, pnl=pnl,
                   entry_ts=pos["entry_ts"], entry_premium=pos["entry_premium"], zone_ts=pos["zone_ts"])
        self.position = None
        self.day_done = True
        return ev

    def on_bar(self, bar) -> Optional[dict]:
        if self.day_done:
            return None
        if bar.timestamp.time() >= _ENTRY_CUTOFF and self.position is None:
            self.day_done = True
            return None

        if self.position is not None:
            pos = self.position
            if bar.timestamp.time() >= _EOD_TIME:
                return self._exit("eod", bar.close, bar.timestamp)
            cap_floor = pos["entry_premium"] - (_MAX_RISK_RS_PER_LOT / self.lot_size)
            if bar.close <= cap_floor:
                return self._exit(f"risk_cap@{cap_floor:.2f}", bar.close, bar.timestamp)
            if self.exit_mode == "profit_trigger" and not pos["triggered"]:
                if bar.close >= pos["entry_premium"] * (1 + _PROFIT_TRIGGER_PCT):
                    pos["triggered"] = True
            # hold_eod: no soft/trailing SL at all -- only the hard risk cap (above) and
            # EOD close apply. bucket_close's own structural check lives in the zone loop.
            if self.exit_mode not in ("bucket_close", "hold_eod"):
                live_s1 = self.active_sr[pos["zone_ts"]]["calc"].get_calculated_sr_state(
                    "OPT")["sr_levels"]["S1"]["low"]
                sl = _sl_for_mode(self.exit_mode, pos, live_s1, self.sl_buffer_pct)
                if bar.close <= sl:
                    return self._exit(f"sl_{self.exit_mode}@{sl:.2f}", bar.close, bar.timestamp)

        pending_entry_event: Optional[dict] = None
        for zone in self.zones:
            if zone["lock_ts"] > bar.timestamp:
                continue
            already_watching = zone["lock_ts"] in self.touched_zone_ts
            if not already_watching:
                if self.gate_mode == "breach":
                    breach_ts = zone.get("breach_ts")
                    if breach_ts is None or bar.timestamp < breach_ts:
                        continue
                elif bar.low > zone["zone_hi"]:
                    continue
                self.touched_zone_ts.add(zone["lock_ts"])
                self.active_sr[zone["lock_ts"]] = {
                    "calc": SupportResistanceCalculator(), "bucket": [], "bucket_open": None, "void": False,
                    "trace": [],
                }

            entry = self.active_sr[zone["lock_ts"]]
            if entry["void"]:
                continue

            is_position_zone = self.position is not None and zone["lock_ts"] == self.position["zone_ts"]
            if self.position is not None and not is_position_zone:
                continue

            calc = entry["calc"]
            b_open = bar.timestamp.replace(
                minute=(bar.timestamp.minute // self.tf_minutes) * self.tf_minutes, second=0, microsecond=0)
            if entry["bucket_open"] is None:
                entry["bucket_open"] = b_open
            elif b_open != entry["bucket_open"]:
                bucket_bars = entry["bucket"]
                if bucket_bars:
                    tf_bar = dict(timestamp=entry["bucket_open"], high=max(b.high for b in bucket_bars),
                                   low=min(b.low for b in bucket_bars), close=bucket_bars[-1].close, duration=1)
                    st_before = calc.get_calculated_sr_state("OPT")
                    phase_before = st_before["current_phase"]
                    live_r1_high = (st_before.get("sr_levels", {}).get("R1") or {}).get("high")
                    calc.process_straddle_candle("OPT", tf_bar, silent=True)
                    st = calc.get_calculated_sr_state("OPT")
                    phase_after = st["current_phase"]
                    s1_low = st["sr_levels"]["S1"]["low"]
                    entry["trace"].append({
                        "ts": tf_bar["timestamp"], "high": tf_bar["high"], "low": tf_bar["low"],
                        "close": tf_bar["close"], "phase_before": phase_before, "phase_after": phase_after,
                        "s1_low": s1_low, "r1_high": st["sr_levels"]["R1"]["high"],
                    })
                    if self.position is None:
                        if s1_low < zone["zone_lo"]:
                            entry["void"] = True
                            entry["trace"][-1]["voided"] = True
                            self.voided.append({"zone_lo": zone["zone_lo"], "zone_hi": zone["zone_hi"],
                                                 "lock_ts": zone["lock_ts"], "voided_at": tf_bar["timestamp"],
                                                 "s1_low": s1_low})
                        elif phase_before == "R2_TRACKING" and phase_after == "R1_TRACKING":
                            breach_bar = next((b for b in bucket_bars if b.high >= live_r1_high), bucket_bars[0])
                            entry_premium = float(live_r1_high)
                            entry["trace"][-1]["breach_ts"] = breach_bar.timestamp
                            entry["trace"][-1]["breach_price"] = entry_premium
                            self.position = {"zone_ts": zone["lock_ts"], "entry_ts": breach_bar.timestamp,
                                              "entry_premium": entry_premium, "initial_sl": s1_low,
                                              "triggered": False}
                            pending_entry_event = dict(type="entry", entry_ts=breach_bar.timestamp,
                                                        entry_premium=entry_premium, zone_ts=zone["lock_ts"],
                                                        initial_sl=s1_low)
                    elif is_position_zone and self.exit_mode == "bucket_close":
                        if tf_bar["close"] <= s1_low:
                            return self._exit(f"sl_bucket_close@{s1_low:.2f}", tf_bar["close"], tf_bar["timestamp"])
                entry["bucket_open"] = b_open
                entry["bucket"] = []
            entry["bucket"].append(bar)

        return pending_entry_event


class PositionalSRTracker:
    """Positional/swing-scale S&R ping-pong for D1TrapOptionBook's d1_trap_fno
    mode (strategies/d1_trap_option/book.py) -- 2026-08-09, built per direct
    user request to bring the same S&R ping-pong entry mechanic validated for
    BANKNIFTY (SRPingPongTracker) into the positional FnO stock strategy,
    replacing that book's existing C2/TWEAK zone-confirm entry. Exit stays
    the book's own proven day-low/day-high TSL ratchet -- not reinvented.

    Differences from SRPingPongTracker (the intraday/index version):
      - Operates on whatever bar sequence it's fed directly, ONE call per
        bar -- no internal minute-bucketing. The CALLER picks the swing
        timeframe by choosing what bars to feed (daily bars = 1D swing tf;
        pre-resampled multi-day bars = coarser). Each incoming bar already
        IS one S&R "candle" -- no bucket accumulation needed.
      - Tracks BOTH directions at once: LONG off bear-trap zones (via
        find_all_bear_zones, buyers reclaim after a sweep -> bullish),
        SHORT off bull-trap zones (via find_all_bull_zones, mirror). FnO
        stocks genuinely move either way, unlike BANKNIFTY's buyer-only
        bear-trap-only design. Zone dicts use book.py's own existing
        zone_lo=min(entry_line,sweep_low)/zone_hi=max(...) formula (not
        bear_only_book.py's option-premium-specific boundary, which was
        validated only for 15m/60m intraday HTF, not daily equity bars).
      - No EOD force-close, no entry-time cutoff -- positional, holds
        indefinitely (across the whole backtest/live run) until stopped.
      - Exit = day-low/day-high TSL ratchet, mirroring book.py's existing
        positional TSL exactly (tsl_level = max(tsl_level, bar.low) for
        LONG / min(tsl_level, bar.high) for SHORT, i.e. only ever tightens),
        seeded at entry from the zone's own opposite extreme, PLUS a hard
        %-of-entry risk cap as a backstop (whichever is tighter) -- not the
        Rs/lot or premium-% caps the intraday versions use, since those
        don't scale across FnO stocks with wildly different price levels.

    Construct ONE instance per (stock, direction-pool) covering the WHOLE
    backtest/live run -- state does NOT reset daily/per-zone-pool the way
    SRPingPongTracker resets every trading day, since a positional trade can
    span many days and zones themselves persist across days too."""

    def __init__(self, zones_long: List[dict], zones_short: List[dict],
                 hard_risk_pct: float = 0.10):
        self.zones_long = zones_long
        self.zones_short = zones_short
        self.hard_risk_pct = hard_risk_pct
        self.touched_long: set = set()
        self.touched_short: set = set()
        self.active_long: dict = {}
        self.active_short: dict = {}
        self.position: Optional[dict] = None
        self.voided: List[dict] = []

    def _exit(self, reason: str, exit_price: float, exit_ts) -> dict:
        pos = self.position
        sign = 1 if pos["side"] == "LONG" else -1
        pnl_pct = sign * (exit_price - pos["entry_price"]) / pos["entry_price"]
        mfe_pct = sign * (pos["mfe_price"] - pos["entry_price"]) / pos["entry_price"]
        ev = dict(type="exit", side=pos["side"], reason=reason, exit_price=exit_price, exit_ts=exit_ts,
                   pnl_pct=pnl_pct, entry_ts=pos["entry_ts"], entry_price=pos["entry_price"],
                   zone_ts=pos["zone_ts"], mfe_price=pos["mfe_price"], mfe_pct=mfe_pct)
        self.position = None
        return ev

    def _check_zone_pool(self, zones: List[dict], touched: set, active: dict, side: str,
                          bar) -> Optional[dict]:
        """Shared touch-detect + S&R feed logic for one direction's zone pool.
        side: "LONG" (bear-trap zones, entry on R2->R1 breakout) or "SHORT"
        (bull-trap zones, entry on the mirror R2/S2->S1 breakdown)."""
        pending_entry: Optional[dict] = None
        for zone in zones:
            if zone["lock_ts"] > bar.timestamp:
                continue
            already_watching = zone["lock_ts"] in touched
            if not already_watching:
                touched_now = (bar.low <= zone["zone_hi"]) if side == "LONG" else (bar.high >= zone["zone_lo"])
                if not touched_now:
                    continue
                touched.add(zone["lock_ts"])
                active[zone["lock_ts"]] = {"calc": SupportResistanceCalculator(), "void": False}

            entry = active[zone["lock_ts"]]
            if entry["void"]:
                continue
            is_position_zone = (self.position is not None and self.position["zone_ts"] == zone["lock_ts"]
                                 and self.position["side"] == side)
            if self.position is not None and not is_position_zone:
                continue

            calc = entry["calc"]
            candle = dict(timestamp=bar.timestamp, high=bar.high, low=bar.low, close=bar.close, duration=1)
            st_before = calc.get_calculated_sr_state("OPT")
            phase_before = st_before["current_phase"]
            live_r1_high = (st_before.get("sr_levels", {}).get("R1") or {}).get("high")
            live_s1_low = (st_before.get("sr_levels", {}).get("S1") or {}).get("low")
            calc.process_straddle_candle("OPT", candle, silent=True)
            st = calc.get_calculated_sr_state("OPT")
            phase_after = st["current_phase"]

            if self.position is None:
                if side == "LONG":
                    s1_low = st["sr_levels"]["S1"]["low"]
                    if s1_low < zone["zone_lo"]:
                        entry["void"] = True
                        self.voided.append({"side": side, "zone_lo": zone["zone_lo"], "zone_hi": zone["zone_hi"],
                                             "lock_ts": zone["lock_ts"], "voided_at": bar.timestamp, "s1_low": s1_low})
                    elif phase_before == "R2_TRACKING" and phase_after == "R1_TRACKING" and live_r1_high:
                        self.position = {"side": "LONG", "zone_ts": zone["lock_ts"], "entry_ts": bar.timestamp,
                                          "entry_price": float(live_r1_high), "tsl_level": s1_low,
                                          "mfe_price": float(live_r1_high)}
                        pending_entry = dict(type="entry", side="LONG", entry_ts=bar.timestamp,
                                              entry_price=float(live_r1_high), zone_ts=zone["lock_ts"],
                                              initial_sl=s1_low)
                else:  # SHORT -- mirror: R2/S2 -> S1 confirms a bearish breakdown
                    r1_high = st["sr_levels"]["R1"]["high"]
                    if r1_high > zone["zone_hi"]:
                        entry["void"] = True
                        self.voided.append({"side": side, "zone_lo": zone["zone_lo"], "zone_hi": zone["zone_hi"],
                                             "lock_ts": zone["lock_ts"], "voided_at": bar.timestamp, "r1_high": r1_high})
                    elif phase_before in ("R2_TRACKING", "S2_TRACKING") and phase_after == "S1_TRACKING" and live_s1_low:
                        self.position = {"side": "SHORT", "zone_ts": zone["lock_ts"], "entry_ts": bar.timestamp,
                                          "entry_price": float(live_s1_low), "tsl_level": r1_high,
                                          "mfe_price": float(live_s1_low)}
                        pending_entry = dict(type="entry", side="SHORT", entry_ts=bar.timestamp,
                                              entry_price=float(live_s1_low), zone_ts=zone["lock_ts"],
                                              initial_sl=r1_high)
        return pending_entry

    def on_bar(self, bar) -> Optional[dict]:
        """Feed ONE new swing-tf bar (e.g. one daily candle). Returns None,
        an entry event, or an exit event -- same shape family as
        SRPingPongTracker.on_bar, with entry_price/pnl_pct instead of
        entry_premium/pnl (positional sizing is per-deployment, not fixed
        lot_size here)."""
        if self.position is not None:
            pos = self.position
            # Check against the stop AS RATCHETED THROUGH THE PREVIOUS bar first --
            # only fold today's own low/high into the ratchet AFTER this check, so
            # it takes effect starting the next bar. Folding today's own extreme in
            # before checking today's bar against it is a same-bar tautology (the
            # stop would be set to ~today's low right before asking "did today's
            # low touch the stop?", which is nearly always true) -- this was a real
            # bug found 2026-08-09: it stopped ~every position out within 1 day
            # instead of ever letting a swing trade run, confirmed by the backtest
            # showing median hold=1 day for a "positional" strategy before the fix.
            if pos["side"] == "LONG":
                hard_floor = pos["entry_price"] * (1 - self.hard_risk_pct)
                stop = max(pos["tsl_level"], hard_floor)
                if bar.low <= stop:
                    return self._exit(f"tsl@{stop:.2f}", min(bar.close, stop), bar.timestamp)
                pos["tsl_level"] = max(pos["tsl_level"], bar.low)
            else:
                hard_ceiling = pos["entry_price"] * (1 + self.hard_risk_pct)
                stop = min(pos["tsl_level"], hard_ceiling)
                if bar.high >= stop:
                    return self._exit(f"tsl@{stop:.2f}", max(bar.close, stop), bar.timestamp)
                pos["tsl_level"] = min(pos["tsl_level"], bar.high)

        ev = self._check_zone_pool(self.zones_long, self.touched_long, self.active_long, "LONG", bar)
        if ev is None:
            ev = self._check_zone_pool(self.zones_short, self.touched_short, self.active_short, "SHORT", bar)
        return ev
