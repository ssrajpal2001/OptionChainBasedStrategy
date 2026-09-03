"""
strategies/d1_trap_option/cascade_sr.py — CascadeSRTracker (2026-08-11).

Multi-tier cascade for FnO positional, built to the user's own manual
trading method (spec confirmed candle-by-candle, numeric examples, over
several rounds of correction on 2026-08-11 -- see
project_fno_cascade_sr_design_2026_08_11 memory for the full transcript):

ENTRY (LONG, mirror for SHORT):
  1. Daily bear-trap zone (find_all_bear_zones on daily bars) touched by
     live/intraday price.
  2. MID-TF ref candle: the first mid-tf bar after touch. Wait for the
     NEXT mid-tf bar to break the ref candle's high -- this "traps" the
     bears who had stops resting above that ref candle.
  3. Break that SPECIFIC ref candle down into FINE-tf bars, run zone
     detection (find_all_bear_zones) on just those bars -- a sub-zone
     scoped to that one reference candle's own structure.
  4. Wait for price to retest (re-enter) that fine-tf sub-zone.
  5. Run the SAME SupportResistanceCalculator sequence used everywhere
     else in this codebase on fine-tf bars from the retest onward.
     Entry fires on R2-breaches-R1 (LONG) / S2-breaches-S1 (SHORT) --
     the fine-tf S1 (LONG) / R1 (SHORT) at that moment becomes the SL.

EXIT (LONG, mirror for SHORT) -- structurally identical cascade, pointed
the other way, NO S&R step in the "simple" variant:
  1. Daily trigger (not close): today's low breaks below the previous
     day's low.
  2. MID-TF ref candle (a NEW one, after the trigger). Wait for the next
     mid-tf bar to break the ref candle's LOW -- traps the longs.
  3. Break that ref candle into fine-tf bars, zone it.
  4. "simple" variant: exit the instant price clears back above every
     zone found in step 3 (no further confirmation).
     "sr_confirm" variant: same, but don't exit until the fine-tf S&R
     ALSO shows a raw S1 breach (price closes below the calculator's
     live S1) -- guards against a false breakdown (price clears the
     zone, reverses back up without ever breaching S1, stays in the
     trade instead of exiting prematurely).

Interface note: the "rezone" step (splitting one specific mid-tf ref
candle into its fine-tf bars and running zone detection on just those)
needs a historical slice of fine-tf bars scoped to that ref candle's own
time window -- not just "the next incoming fine-tf bar". So on_mid_bar()
only detects the break and flips phase to REZONE; the caller (which owns
the full fine-tf bar list) is responsible for calling
complete_entry_rezone()/complete_exit_rezone() right after, passing the
correct slice. on_fine_bar() handles everything after that (retest,
S&R tracking, clear, confirm) on the live incoming bar stream.

This is deliberately independent of PositionalSRTracker (support_resistance.py)
-- that class's single-timeframe daily-only design is already validated and
live; this is a new, still-experimental mechanic being tested against it,
not a replacement, until proven.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones
from strategies.d1_trap_option.support_resistance import SupportResistanceCalculator


@dataclass
class _Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float


def resample_bars(bars: List[_Bar], minutes: int) -> List[_Bar]:
    """Group 1-min (or any finer) bars into `minutes`-wide buckets, bucketed
    on wall-clock boundaries within each trading day (e.g. 60min buckets
    align to the hour) so results are stable regardless of exactly when the
    session opened."""
    if not bars:
        return []
    buckets: Dict[tuple, list] = {}
    order: List[tuple] = []
    for b in bars:
        day = b.timestamp.date()
        minute_of_day = b.timestamp.hour * 60 + b.timestamp.minute
        bucket_start_min = (minute_of_day // minutes) * minutes
        key = (day, bucket_start_min)
        if key not in buckets:
            buckets[key] = []
            order.append(key)
        buckets[key].append(b)
    out = []
    for key in order:
        group = buckets[key]
        day, bucket_start_min = key
        ts = group[0].timestamp.replace(
            hour=bucket_start_min // 60, minute=bucket_start_min % 60, second=0, microsecond=0)
        out.append(_Bar(
            timestamp=ts, open=group[0].open, high=max(g.high for g in group),
            low=min(g.low for g in group), close=group[-1].close,
        ))
    return out


def _zone_dict(z, side: str) -> dict:
    return dict(side=side, zone_lo=min(z.entry_line, z.sweep_low), zone_hi=max(z.entry_line, z.sweep_low),
                lock_ts=z.lock_ts, ref_ts=z.reference_low_ts)


@dataclass
class _EntryCascadeState:
    zone: dict
    side: str                      # "LONG" | "SHORT"
    phase: str = "WAITING_MID_BREAK"   # -> REZONE -> WAITING_RETEST -> SR_TRACKING -> (consumed)
    ref_candle: Optional[_Bar] = None
    sub_zones: List[dict] = field(default_factory=list)
    active_sub_zone: Optional[dict] = None
    sr_calc: Optional[SupportResistanceCalculator] = None


@dataclass
class _ExitCascadeState:
    side: str
    phase: str = "WAITING_MID_BREAK"   # -> REZONE -> WAITING_CLEAR -> (SR_CONFIRM) -> (consumed)
    ref_candle: Optional[_Bar] = None
    sub_zones: List[dict] = field(default_factory=list)
    sr_calc: Optional[SupportResistanceCalculator] = None


class CascadeSRTracker:
    """One instance per (stock, exit_sr_confirm variant). Caller drives it by
    feeding, in strict chronological order: on_intraday_tick_for_touch() for
    every available price sample, on_daily_bar() once per new daily bar, and
    on_mid_bar()/on_fine_bar() once per completed mid/fine-tf bar -- checking
    after each on_mid_bar() call whether any state moved to REZONE and, if
    so, calling complete_entry_rezone()/complete_exit_rezone() with that
    specific ref candle's fine-tf bars before continuing."""

    def __init__(self, zones_long: List[dict], zones_short: List[dict], exit_sr_confirm: bool = False,
                 require_retest: bool = True):
        self.zones_long = zones_long
        self.zones_short = zones_short
        self.exit_sr_confirm = exit_sr_confirm
        # 2026-08-11: require_retest=False skips the "re-zone the ref candle at
        # fine-tf, wait for a retest" gate entirely -- go straight from a
        # confirmed mid-tf break to fine-tf S&R tracking. User's own alternate
        # spec to test against the retest-gated version.
        self.require_retest = require_retest
        self.position: Optional[dict] = None
        self._entry_states: Dict[tuple, _EntryCascadeState] = {}
        self._exit_state: Optional[_ExitCascadeState] = None
        self._prev_day_low: Optional[float] = None
        self._prev_day_high: Optional[float] = None
        self._today_low: Optional[float] = None
        self._today_high: Optional[float] = None

    # ── daily-level bookkeeping (touch detection + exit trigger) ────────────

    def on_daily_bar(self, bar: _Bar) -> None:
        """Call once per NEW daily bar (at/after that day's close), in order.
        Rolls prev-day high/low forward and resets today's running extremes."""
        if self._today_low is not None:
            self._prev_day_low = self._today_low
            self._prev_day_high = self._today_high
        self._today_low = bar.low
        self._today_high = bar.high

    def on_intraday_tick_for_touch(self, price: float, ts: datetime) -> None:
        """Cheap touch/trigger check using any available intraday price
        (call with fine-tf bar highs/lows too, not just a true tick feed)."""
        if self._today_low is None or price < self._today_low:
            self._today_low = price
        if self._today_high is None or price > self._today_high:
            self._today_high = price
        if self.position is None:
            # Touch condition matches PositionalSRTracker's own (support_resistance.py)
            # exactly -- LONG: price has come down to/through the zone's top edge;
            # SHORT: price has come up to/through the zone's bottom edge.
            for z in self.zones_long:
                key = ("LONG", z["lock_ts"])
                if key not in self._entry_states and price <= z["zone_hi"]:
                    self._entry_states[key] = _EntryCascadeState(zone=z, side="LONG")
            for z in self.zones_short:
                key = ("SHORT", z["lock_ts"])
                if key not in self._entry_states and price >= z["zone_lo"]:
                    self._entry_states[key] = _EntryCascadeState(zone=z, side="SHORT")
        elif self._exit_state is None:
            side = self.position["side"]
            if side == "LONG" and self._prev_day_low is not None and price < self._prev_day_low:
                self._exit_state = _ExitCascadeState(side="LONG")
            elif side == "SHORT" and self._prev_day_high is not None and price > self._prev_day_high:
                self._exit_state = _ExitCascadeState(side="SHORT")

    # ── mid-tf bar processing (ref-candle break detection only) ─────────────

    def on_mid_bar(self, bar: _Bar) -> None:
        for st in self._entry_states.values():
            if st.phase != "WAITING_MID_BREAK":
                continue
            if st.ref_candle is None:
                st.ref_candle = bar
                continue
            broke = (bar.high > st.ref_candle.high) if st.side == "LONG" else (bar.low < st.ref_candle.low)
            if broke:
                if self.require_retest:
                    st.phase = "REZONE"
                else:
                    # Skip re-zone/retest entirely -- start fine-tf S&R tracking
                    # immediately off the break itself.
                    st.sr_calc = SupportResistanceCalculator()
                    st.phase = "SR_TRACKING"
        if self._exit_state is not None and self._exit_state.phase == "WAITING_MID_BREAK":
            st = self._exit_state
            if st.ref_candle is None:
                st.ref_candle = bar
                return
            broke = (bar.low < st.ref_candle.low) if st.side == "LONG" else (bar.high > st.ref_candle.high)
            if broke:
                st.phase = "REZONE"

    def entry_keys_awaiting_rezone(self) -> List[tuple]:
        return [k for k, st in self._entry_states.items() if st.phase == "REZONE"]

    def exit_awaiting_rezone(self) -> bool:
        return self._exit_state is not None and self._exit_state.phase == "REZONE"

    # ── rezone completion (caller supplies the ref candle's own fine-tf bars) ──

    def complete_entry_rezone(self, key: tuple, ref_candle_fine_bars: List[_Bar]) -> None:
        st = self._entry_states.get(key)
        if st is None or st.phase != "REZONE":
            return
        if len(ref_candle_fine_bars) < 3:
            del self._entry_states[key]
            return
        zones = (find_all_bear_zones(ref_candle_fine_bars) if st.side == "LONG"
                 else find_all_bull_zones(ref_candle_fine_bars))
        if not zones:
            del self._entry_states[key]
            return
        st.sub_zones = [_zone_dict(z, st.side) for z in zones]
        st.phase = "WAITING_RETEST"

    def complete_exit_rezone(self, ref_candle_fine_bars: List[_Bar]) -> None:
        st = self._exit_state
        if st is None or st.phase != "REZONE":
            return
        if len(ref_candle_fine_bars) < 3:
            self._exit_state = None
            return
        zones = (find_all_bear_zones(ref_candle_fine_bars) if st.side == "LONG"
                 else find_all_bull_zones(ref_candle_fine_bars))
        if not zones:
            self._exit_state = None
            return
        st.sub_zones = [_zone_dict(z, st.side) for z in zones]
        st.phase = "WAITING_CLEAR"

    # ── fine-tf bar processing (retest, S&R tracking, exit-clear/confirm) ───

    def on_fine_bar(self, bar: _Bar) -> Optional[dict]:
        for key in list(self._entry_states.keys()):
            st = self._entry_states[key]
            if st.phase == "WAITING_RETEST":
                for sz in st.sub_zones:
                    if sz["zone_lo"] <= bar.low <= sz["zone_hi"] or sz["zone_lo"] <= bar.high <= sz["zone_hi"]:
                        st.active_sub_zone = sz
                        st.sr_calc = SupportResistanceCalculator()
                        st.phase = "SR_TRACKING"
                        break
                continue
            if st.phase == "SR_TRACKING" and st.sr_calc is not None and self.position is None:
                candle = dict(timestamp=bar.timestamp, high=bar.high, low=bar.low, close=bar.close, duration=1)
                before = st.sr_calc.get_calculated_sr_state("OPT")
                phase_before = before["current_phase"]
                live_r1 = (before.get("sr_levels", {}).get("R1") or {}).get("high")
                live_s1 = (before.get("sr_levels", {}).get("S1") or {}).get("low")
                st.sr_calc.process_straddle_candle("OPT", candle, silent=True)
                after = st.sr_calc.get_calculated_sr_state("OPT")
                phase_after = after["current_phase"]
                if st.side == "LONG" and phase_before == "R2_TRACKING" and phase_after == "R1_TRACKING" and live_r1:
                    self.position = dict(side="LONG", entry_price=float(live_r1), entry_ts=bar.timestamp,
                                          sl=live_s1, zone_ts=st.zone["lock_ts"])
                    self._entry_states.clear()
                    return dict(type="entry", side="LONG", entry_price=float(live_r1), entry_ts=bar.timestamp,
                                sl=live_s1)
                if st.side == "SHORT" and phase_before in ("R2_TRACKING", "S2_TRACKING") and phase_after == "S1_TRACKING" and live_s1:
                    self.position = dict(side="SHORT", entry_price=float(live_s1), entry_ts=bar.timestamp,
                                          sl=live_r1, zone_ts=st.zone["lock_ts"])
                    self._entry_states.clear()
                    return dict(type="entry", side="SHORT", entry_price=float(live_s1), entry_ts=bar.timestamp,
                                sl=live_r1)

        st = self._exit_state
        if st is None:
            return None
        if st.phase == "WAITING_CLEAR":
            if st.side == "LONG":
                cleared = bar.close > max(sz["zone_hi"] for sz in st.sub_zones)
            else:
                cleared = bar.close < min(sz["zone_lo"] for sz in st.sub_zones)
            if not cleared:
                return None
            if not self.exit_sr_confirm:
                ev = dict(type="exit", side=st.side, exit_price=bar.close, exit_ts=bar.timestamp,
                           reason="cascade_simple")
                self.position = None
                self._exit_state = None
                return ev
            st.sr_calc = SupportResistanceCalculator()
            st.phase = "SR_CONFIRM"
            return None
        if st.phase == "SR_CONFIRM" and st.sr_calc is not None:
            # Direct level-breach check, not a phase-name transition match -- "S1
            # breach"/"R1 breach" here means literally what it says: the
            # calculator's live S1/R1 level gets broken by price. Reusing the
            # entry side's phase-transition matching would be guessing at
            # semantics only verified for the entry use case.
            before = st.sr_calc.get_calculated_sr_state("OPT")
            live_r1 = (before.get("sr_levels", {}).get("R1") or {}).get("high")
            live_s1 = (before.get("sr_levels", {}).get("S1") or {}).get("low")
            if st.side == "LONG" and live_s1 is not None and bar.low < live_s1:
                ev = dict(type="exit", side="LONG", exit_price=float(live_s1), exit_ts=bar.timestamp,
                           reason="cascade_sr_confirm")
                self.position = None
                self._exit_state = None
                return ev
            if st.side == "SHORT" and live_r1 is not None and bar.high > live_r1:
                ev = dict(type="exit", side="SHORT", exit_price=float(live_r1), exit_ts=bar.timestamp,
                           reason="cascade_sr_confirm")
                self.position = None
                self._exit_state = None
                return ev
            candle = dict(timestamp=bar.timestamp, high=bar.high, low=bar.low, close=bar.close, duration=1)
            st.sr_calc.process_straddle_candle("OPT", candle, silent=True)
        return None
