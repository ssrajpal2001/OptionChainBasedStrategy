"""
strategies/oi_flow/engine.py — OIFlowStrategy, the live/paper book for the
OI-Flow Pre-Breakout strategy.

Fully standalone (see strategies/oi_flow/__init__.py) -- owns its own
OIFlowTracker, its own spot/option 1-min bar accumulators, its own
confirm-then-finalize position lifecycle and persistence namespace. Shares
no runtime state with any other strategy's book.

What IS reused (platform infra, not another strategy's logic -- see
execution_bridge/oi_flow_bridge.py's own docstring for the same
reasoning): strategies.core.base_book.AbstractStrategyBook,
data_layer.position_store, config.global_config.{IST,Topic}.

Entry sequencing, per side (CE/PE), on every new spot 1-min bar close:
  1. detect_pre_breakout_signal() -- spot vs. OI wall gate.
  2. If that fires, confirm_option_price_action() -- option premium gate.
  3. Only if BOTH pass: BUY at the option's current live LTP, SL = the
     option chart's own recent swing low (never a spot-derived offset).
Exit: SL hit (checked on every option tick for the open position's own
strike), EOD squareoff, or the universal hard Rs/lot risk cap.

Cannot be backtested (no historical OI via Upstox's intraday API) --
every signal evaluation (fired or not) should eventually be logged via
Phase 5's telemetry, once this book is running for real.
"""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Deque, Dict, Optional

from config.global_config import IST, Topic
from data_layer.base_feeder import IndexTick, OptionTick
from data_layer import position_store
from data_layer.instrument_registry import REGISTRY
from strategies.core.base_book import AbstractStrategyBook
from strategies.oi_flow.detector import (
    BarAccumulator, detect_pre_breakout_signal, confirm_option_price_action, swing_low,
)
from strategies.oi_flow.events import OIFlowOrderEvent, OIFlowFillEvent
from strategies.oi_flow.telemetry import log_signal_evaluation, new_row
from strategies.oi_flow.tracker import OIFlowTracker

logger = logging.getLogger(__name__)


def _make_strategy_logger(underlying: str, client_id: str = "", binding_id: str = "") -> logging.Logger:
    """Dedicated, rotating, per-(underlying,client,binding,day) log file --
    same utils.logging_utils.make_strategy_logger platform utility
    SellStraddle/V4Cascade already use (strategies/sell_straddle/engine.py's
    own _make_strategy_logger, logs/clients/ss_{UND}_{client}_{binding}_
    {date}.log), so OI-Flow gets the same dedicated-log-per-index behavior
    -- e.g. running OI-Flow on both NIFTY and SENSEX for the same client/
    binding writes to two separate files, never mixed together."""
    from utils.logging_utils import make_strategy_logger
    tag = f"{underlying}" + (f"_{client_id}_{binding_id}" if client_id and binding_id else "")
    date_str = datetime.now(IST).strftime("%Y%m%d")
    return make_strategy_logger(f"oiflow_{tag}_{date_str}", propagate=False)


_DEFAULT_WINDOW_SEC = 180
_DEFAULT_MAX_OPPOSING_ROC_PCT = -0.01
_DEFAULT_MIN_SUPPORTING_ROC_PCT = 0.02
_DEFAULT_MIN_PCR_BIAS = 1.2
_DEFAULT_MAX_PCR_BIAS = 0.7
_DEFAULT_PROXIMITY_PCT = 0.005
_DEFAULT_HARD_RISK_RS_PER_LOT = 2000.0
_EOD_TIME_DEFAULT = time(15, 15)
_EXIT_CONFIRM_TIMEOUT_SEC = 15.0

# Step-locked trailing profit-lock (2026-08-13, "target" concept -- there was
# no take-profit/trailing mechanism at all before this; only SL + hard risk
# cap + EOD). Same MECHANIC as strategies/fvg/engine.py's own validated
# _check_exit_premium ratchet (a bought CE or PE is always long its own
# premium, so "let a strong move run, then lock in gains as it extends" is
# equally valid here) -- but these specific DEFAULT VALUES are FVG's own
# tuned baseline (scripts/fvg_tsl_sweep.py), borrowed as a reasonable
# starting point, NOT independently validated for OI-Flow (no backtest is
# possible for this strategy at all -- see telemetry.py). Review against
# OI-Flow's own forward telemetry before trusting these numbers.
_DEFAULT_TRAIL_TRIGGER_PCT = 0.15
_DEFAULT_FIRST_LOCK_PCT = 0.08
_DEFAULT_STEP_PCT = 0.10
_DEFAULT_STEP_LOCK_PCT = 0.05


class OIFlowStrategy(AbstractStrategyBook):
    """One instance per (client, binding, underlying)."""

    def __init__(
        self,
        bus,
        cfg,
        underlying: str,
        client_id: str,
        binding_id: str,
        lot_multiplier: int = 1,
        window_sec: int = _DEFAULT_WINDOW_SEC,
        max_opposing_roc_pct: float = _DEFAULT_MAX_OPPOSING_ROC_PCT,
        min_supporting_roc_pct: float = _DEFAULT_MIN_SUPPORTING_ROC_PCT,
        min_pcr_bias: float = _DEFAULT_MIN_PCR_BIAS,
        max_pcr_bias: float = _DEFAULT_MAX_PCR_BIAS,
        proximity_pct: float = _DEFAULT_PROXIMITY_PCT,
        hard_risk_rs_per_lot: float = _DEFAULT_HARD_RISK_RS_PER_LOT,
        trail_trigger_pct: float = _DEFAULT_TRAIL_TRIGGER_PCT,
        first_lock_pct: float = _DEFAULT_FIRST_LOCK_PCT,
        step_pct: float = _DEFAULT_STEP_PCT,
        step_lock_pct: float = _DEFAULT_STEP_LOCK_PCT,
        product_type: str = "MIS",
        squareoff_time: str = "15:15",
    ) -> None:
        super().__init__(bus, cfg, underlying, client_id, binding_id)
        self._strategy_name = "oi_flow"
        self._lot_multiplier = max(1, lot_multiplier)
        self._window_sec = window_sec
        self._max_opposing_roc_pct = max_opposing_roc_pct
        self._min_supporting_roc_pct = min_supporting_roc_pct
        self._min_pcr_bias = min_pcr_bias
        self._max_pcr_bias = max_pcr_bias
        self._proximity_pct = proximity_pct
        self._hard_risk_rs_per_lot = hard_risk_rs_per_lot
        self._trail_trigger_pct = trail_trigger_pct
        self._first_lock_pct = first_lock_pct
        self._step_pct = step_pct
        self._step_lock_pct = step_lock_pct
        self._product_type = product_type
        try:
            _h, _m = str(squareoff_time or "15:15").split(":")
            self._squareoff_time = time(int(_h), int(_m))
        except Exception:
            self._squareoff_time = _EOD_TIME_DEFAULT

        self._lot_size = (cfg.exchange.lot_sizes.get(underlying, 75) if cfg else 75)
        self._strike_step = float(cfg.exchange.strike_steps.get(underlying, 100) if cfg else 100)
        self._persist_key = f"{client_id}_{binding_id}_{underlying}_oi_flow"
        # Dedicated per-(underlying,client,binding,day) rotating log file --
        # same as SellStraddle's own self._clog (strategies/sell_straddle/
        # engine.py). Never recreated on reset_session(), matching that same
        # convention (a live process rarely spans an actual midnight
        # rollover for intraday NSE trading).
        self._clog = _make_strategy_logger(underlying, client_id, binding_id)

        self._today: Optional[date] = None
        self._oi_tracker = OIFlowTracker(max_history_sec=max(window_sec * 2, 600))
        self._spot_acc = BarAccumulator(timeframe_min=1)
        self._option_acc: Dict[str, BarAccumulator] = {"CE": BarAccumulator(1), "PE": BarAccumulator(1)}
        self._latest_snap = None
        self._live_option_ltp: Dict[str, float] = {}   # "CE"/"PE" -> latest live LTP of that side's wall strike
        self._watched_strikes: Dict[tuple, str] = {}   # (strike, side) -> "opposing"|"supporting", for option bar routing

        self._position: Optional[dict] = None
        self._day_done = False
        self._event_counter = 0
        self._fill_waiters: Dict[str, asyncio.Event] = {}
        self._fill_results: Dict[str, object] = {}
        # Dashboard-facing, in-memory only (never persisted -- telemetry.py's
        # JSONL is the durable record). A short human-readable trail of the
        # last N signal evaluations so the UI can show "what just happened
        # and why" without the user tailing log files.
        self._recent_remarks: Deque[dict] = deque(maxlen=30)

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def reset_session(self) -> None:
        self._today = None
        self._spot_acc = BarAccumulator(timeframe_min=1)
        self._option_acc = {"CE": BarAccumulator(1), "PE": BarAccumulator(1)}
        self._live_option_ltp = {}
        self._day_done = False
        self._recent_remarks.clear()

    def start(self) -> None:
        super().start()
        self._subscribe(Topic.INDEX_TICK)
        self._subscribe(Topic.OPTION_TICK)
        self._subscribe(Topic.MATRIX_SNAPSHOT)
        self._subscribe(Topic.OI_FLOW_ORDER_FILL)
        self._restore_position()
        self._tasks.append(asyncio.create_task(self._index_tick_loop(), name=f"oiflow_idx_{self._underlying}"))
        self._tasks.append(asyncio.create_task(self._option_tick_loop(), name=f"oiflow_opt_{self._underlying}"))
        self._tasks.append(asyncio.create_task(self._matrix_snapshot_loop(), name=f"oiflow_snap_{self._underlying}"))
        self._tasks.append(asyncio.create_task(self._fill_loop(), name=f"oiflow_fill_{self._underlying}"))
        self._tasks.append(asyncio.create_task(self._eod_loop(), name=f"oiflow_eod_{self._underlying}"))

    # ── matrix snapshot -> OI wall tracking ─────────────────────────────────────

    def _is_own_underlying_snapshot(self, snap) -> bool:
        return str(getattr(snap, "underlying", "")).upper() == self._underlying.upper()

    async def _matrix_snapshot_loop(self) -> None:
        q = self._loop_queues.get(Topic.MATRIX_SNAPSHOT)
        if q is None:
            return
        while self._running:
            try:
                snap = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if not self._is_own_underlying_snapshot(snap):
                continue
            self._latest_snap = snap
            self._rewatch_oi_strikes(snap)

    def _rewatch_oi_strikes(self, snap) -> None:
        """Re-derive the OIFlowTracker's watch list from the current OI
        walls -- cheap and idempotent, safe to call on every new snapshot
        so the tracker always follows the CURRENT wall even if it shifts
        intraday."""
        call_wall = snap.max_call_oi_strike
        put_wall = snap.max_put_oi_strike
        watched: Dict[tuple, str] = {}
        if call_wall:
            watched[(call_wall, "CE")] = "opposing"        # CE side's resistance wall
            watched[(call_wall - self._strike_step, "PE")] = "supporting"
        if put_wall:
            watched[(put_wall, "PE")] = "opposing"          # PE side's support wall
            watched[(put_wall + self._strike_step, "CE")] = "supporting"
        self._watched_strikes = watched
        self._oi_tracker.watch_strikes({k: True for k in watched})

    # ── spot ticks -> signal evaluation ─────────────────────────────────────────

    async def _index_tick_loop(self) -> None:
        q = self._loop_queues.get(Topic.INDEX_TICK)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if not isinstance(ev, IndexTick) or not self._is_own_underlying_tick(ev.symbol):
                continue
            today = ev.timestamp.date() if hasattr(ev, "timestamp") else datetime.now(IST).date()
            if self._today != today:
                self.reset_session()
                self._today = today
            closed = self._spot_acc.on_tick(ev.timestamp, ev.ltp)
            if closed:
                self._on_spot_bar_close()

    def _is_own_underlying_tick(self, symbol: str) -> bool:
        u = self._underlying.upper()
        if symbol == self._underlying:
            return True
        aliases = {
            "NIFTY": ("NSE_INDEX|Nifty 50", "NIFTY", "NSE:NIFTY50-INDEX"),
            "SENSEX": ("BSE_INDEX|SENSEX", "SENSEX"),
            "BANKNIFTY": ("NSE_INDEX|Nifty Bank", "BANKNIFTY"),
        }
        return symbol in aliases.get(u, ())

    def _on_spot_bar_close(self) -> None:
        if self._day_done or self._position is not None:
            return
        spot = self._spot_acc.bars[-1].close if self._spot_acc.bars else None
        if self._latest_snap is None:
            # Every 1-min spot bar close until the first chain snapshot
            # arrives -- without this, the log file looks completely dead
            # during startup (same complaint SellStraddle's own WAIT line
            # exists to avoid): confirms the book is alive and ticking,
            # just still waiting on Topic.MATRIX_SNAPSHOT.
            self._clog.info("WAIT spot=%s -- waiting for first OI chain snapshot (Topic.MATRIX_SNAPSHOT)...",
                             f"{spot:.2f}" if spot is not None else "?")
            return
        now_t = self._spot_acc.bars[-1].timestamp.time() if self._spot_acc.bars else datetime.now(IST).time()
        if now_t >= self._squareoff_time:
            return
        for side in ("CE", "PE"):
            self._try_enter(side)
            if self._position is not None:
                break   # at most one position at a time for this book

    def _try_enter(self, side: str) -> None:
        row = new_row(self._underlying, side)
        try:
            self._try_enter_inner(side, row)
        finally:
            # Logged exactly once per evaluation, every path (entered or
            # not) -- this is the whole point: a rejection is just as
            # reviewable later as a fired signal, since there's no
            # backtest to compare against.
            log_signal_evaluation(row)
            remark = self._remark_for(row)
            self._recent_remarks.appendleft(remark)
            # 2026-08-13: also write every evaluation to the dedicated
            # per-underlying log file (self._clog), not just the in-memory
            # dashboard trail -- previously self._clog only got written to
            # on actual entries/exits, so during a long stretch of "no
            # signal yet" the file looked dead/silent, unlike SellStraddle's
            # own _clog which logs a WAIT/evaluation line every cycle even
            # while idle. This is the fix for that.
            self._clog.info(remark["text"])

    def _remark_for(self, row) -> dict:
        """Turns one telemetry row into a short, human-readable line for the
        dashboard's REMARKS panel -- the live-UI equivalent of the JSONL
        telemetry file, so the user can see "what just happened and why"
        without tailing logs/oi_flow/*.jsonl."""
        if row.entered:
            text = (f"{row.side} ENTERED @{row.option_premium:.2f} strike={row.wall_strike:.0f} "
                    f"SL={row.option_sl_level:.2f}" if row.option_premium is not None and row.option_sl_level is not None
                    else f"{row.side} ENTERED (strike={row.wall_strike})")
            level = "entry"
        elif row.skip_reason == "spot_gate_no_signal":
            wall_txt = f"{row.wall_strike:.0f}" if row.wall_strike else "?"
            text = f"{row.side}: no spot signal (spot={row.spot}, wall={wall_txt}) -- not consolidating at the wall yet"
            level = "info"
        elif row.skip_reason == "option_gate_blocked":
            text = f"{row.side}: spot signal fired (wall={row.wall_strike:.0f}) but option chart blocked entry ({row.option_gate_reason})"
            level = "watch"
        elif row.skip_reason == "no_live_ltp":
            text = f"{row.side}: both gates passed (wall={row.wall_strike:.0f}) but no live option price yet -- skipped"
            level = "warn"
        else:
            text = f"{row.side}: evaluated, no entry"
            level = "info"
        return {"ts": row.ts, "side": row.side, "level": level, "text": text}

    def _try_enter_inner(self, side: str, row) -> None:
        # Raw diagnostics -- cheap, read-only queries against the same
        # tracker/snap detect_pre_breakout_signal() itself reads; this does
        # NOT re-derive the pass/fail decision, only captures the numbers
        # for telemetry regardless of outcome.
        if self._spot_acc.bars:
            row.spot = self._spot_acc.bars[-1].close
        snap = self._latest_snap
        if snap is not None:
            wall = snap.max_call_oi_strike if side == "CE" else snap.max_put_oi_strike
            supporting_strike = (wall - self._strike_step) if side == "CE" else (wall + self._strike_step)
            supporting_side = "PE" if side == "CE" else "CE"
            row.wall_strike = wall
            row.opposing_roc = self._oi_tracker.oi_roc(wall, side, self._window_sec) if wall else None
            row.supporting_roc = (self._oi_tracker.oi_roc(supporting_strike, supporting_side, self._window_sec)
                                   if wall else None)
            row.pcr = snap.pcr_smooth()

        spot_signal = detect_pre_breakout_signal(
            side, self._oi_tracker, self._latest_snap, self._spot_acc.bars,
            window_sec=self._window_sec,
            max_opposing_roc_pct=self._max_opposing_roc_pct,
            min_supporting_roc_pct=self._min_supporting_roc_pct,
            min_pcr_bias=self._min_pcr_bias, max_pcr_bias=self._max_pcr_bias,
            proximity_pct=self._proximity_pct, strike_step=self._strike_step,
        )
        row.spot_gate_fired = spot_signal is not None
        if spot_signal is None:
            row.skip_reason = "spot_gate_no_signal"
            return

        option_bars = self._option_acc[side].bars
        confirmation = confirm_option_price_action(option_bars, side)
        row.option_vwap = confirmation.vwap
        row.option_sl_level = confirmation.sl_level
        row.option_gate_ok = confirmation.ok
        row.option_gate_reason = confirmation.reason
        row.volume_spike = confirmation.volume_spike
        row.volume_ratio = confirmation.volume_ratio
        if option_bars:
            row.option_premium = option_bars[-1].close
        if not confirmation.ok:
            row.skip_reason = "option_gate_blocked"
            logger.info(
                "OIFlow[%s]: %s spot signal fired (wall=%.0f) but option confirmation blocked "
                "(reason=%s) -- no entry.",
                self._underlying, side, spot_signal.wall_strike, confirmation.reason,
            )
            return

        entry_price = self._live_option_ltp.get(side)
        if entry_price is None:
            row.skip_reason = "no_live_ltp"
            logger.warning(
                "OIFlow[%s]: %s both gates passed but no live option LTP yet for the wall strike "
                "-- skipping this entry rather than trading a stale/unknown price.",
                self._underlying, side,
            )
            return

        row.entered = True
        self._enter(side, spot_signal.wall_strike, entry_price, confirmation.sl_level)

    # ── option ticks -> OI tracker + option bars + live LTP + exit checks ──────

    async def _option_tick_loop(self) -> None:
        q = self._loop_queues.get(Topic.OPTION_TICK)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if not isinstance(ev, OptionTick) or ev.underlying != self._underlying or not ev.ltp:
                continue
            self._oi_tracker.on_option_tick(ev)

            side = str(ev.option_type).upper()
            has_position = self._position is not None and self._position["side"] == side
            # 2026-08-13 fix: which strike _option_acc[side]/_live_option_ltp
            # track must switch the moment a position opens. Before this,
            # both ALWAYS followed the CURRENT OI wall (snap.max_call_oi_
            # strike/max_put_oi_strike) -- correct while scanning/flat, but
            # _rewatch_oi_strikes() re-derives the wall on every new
            # MATRIX_SNAPSHOT regardless of position state, so a wall that
            # drifts to a DIFFERENT strike while a position is open would
            # silently redirect these onto the NEW wall's premium -- a
            # different instrument's price series entirely -- corrupting
            # the EOD exit-price fallback, the dashboard's live P&L, and
            # (2026-08-13) the S1 trailing-stop swing-low calculation,
            # while the actual held position sits at the OLD strike.
            # Post-entry: lock onto the position's own strike, immune to
            # wall drift. Pre-entry/flat: keep following the current wall
            # (that IS the correct behavior while scanning for an entry).
            if has_position:
                target_strike = float(self._position["strike"])
            else:
                snap = self._latest_snap
                wall = (snap.max_call_oi_strike if side == "CE" else snap.max_put_oi_strike) if snap else None
                target_strike = float(wall) if wall else None
            is_target_strike = target_strike is not None and float(ev.strike) == target_strike
            if is_target_strike:
                self._live_option_ltp[side] = ev.ltp
                closed = self._option_acc[side].on_tick(ev.timestamp, ev.ltp, ev.volume)
                if closed and has_position:
                    self._maybe_promote_s1(side)

            if has_position and is_target_strike:
                self._check_exit(ev.ltp)

    def _check_exit(self, ltp: float) -> None:
        # Both CE and PE positions BUY the option -- long its own premium
        # either way, so every check here is identical for both sides:
        # premium falling to/through a floor, regardless of which side's
        # option it is. 2026-08-13 fix: PE used to check `ltp >= sl_price`
        # (fires on a RISE), a leftover from confirm_option_price_action()'s
        # own now-fixed PE mirroring bug (detector.py) -- was never actually
        # consistent with this line's own "sl_option_swing_low" label,
        # which already assumed a falling-through-a-low semantic for both
        # sides.
        pos = self._position
        if pos is None or pos.get("_closing"):
            return

        # Step-locked trailing profit-lock ("target" concept, 2026-08-13):
        # once profit_pct >= trail_trigger_pct, lock first_lock_pct; every
        # further step_pct of additional profit locks another
        # step_lock_pct (repeating, never un-ratchets). No fixed take-
        # profit ceiling -- once active, the floor only ever rises,
        # letting a strong move keep running. Same formula as
        # strategies/fvg/engine.py's own _check_exit_premium, written
        # fresh here (no import) per this strategy's standalone mandate.
        entry = pos["entry_price"]
        profit_pct = (ltp - entry) / entry if entry else 0.0
        if profit_pct >= self._trail_trigger_pct:
            steps = int((profit_pct - self._trail_trigger_pct) // self._step_pct)
            calc_lock = self._first_lock_pct + steps * self._step_lock_pct
            pos["high_lock_pct"] = max(pos.get("high_lock_pct", 0.0), calc_lock)

        high_lock_pct = pos.get("high_lock_pct", 0.0)
        pct_floor = entry * (1 + high_lock_pct) if high_lock_pct > 0 else pos["sl_price"]
        # S1 trailing stop (2026-08-13, user's own framing: "S1 will act as
        # TSL"): the option's own most recent CONFIRMED swing low since
        # entry, promoted (ratcheted, never lowered) as new higher lows
        # print -- see _maybe_promote_s1(). Combined with the percentage
        # ratchet above via max(): whichever floor is currently TIGHTER
        # (higher) binds, so S1 can tighten the stop beyond what the flat
        # percentage alone would give (a real, confirmed price-action
        # level rather than an arbitrary percentage step), while never
        # loosening protection the percentage ratchet already earned.
        s1_floor = pos.get("s1_floor", pos["sl_price"])
        stop_price = max(pct_floor, s1_floor)
        if ltp <= stop_price:
            # Label only credits S1/TSL when one of them actually PROMOTED
            # past the original swing-low anchor -- a plain SL hit (neither
            # ever activated) must still read as "sl_option_swing_low", not
            # a misleading "s1_hit"/"tsl_hit" implying a promotion that
            # never happened.
            if s1_floor > pos["sl_price"] and s1_floor >= pct_floor:
                reason = f"s1_hit@{stop_price:.2f}"
            elif high_lock_pct > 0:
                reason = f"tsl_hit@{stop_price:.2f}"
            else:
                reason = f"sl_option_swing_low@{stop_price:.2f}"
            self._exit(reason=reason, exit_price=ltp)
            return

        risk_floor = entry - (self._hard_risk_rs_per_lot / (self._lot_size * self._lot_multiplier))
        if ltp <= risk_floor:
            self._exit(reason=f"hard_risk_cap@{risk_floor:.2f}", exit_price=ltp)

    def _maybe_promote_s1(self, side: str) -> None:
        """S1 trailing stop, per the user's own framing ('S1 will act as
        TSL'): as the option's own premium chart prints a new CONFIRMED
        swing low (swing_low(), the SAME function already used for the
        entry-time SL anchor -- this strategy's own utility, not another
        strategy's S&R code) ABOVE the current s1_floor, promote the floor
        to it. Ratchets only, same discipline as the percentage TSL. Called
        only on an option-bar CLOSE for the position's OWN strike (a
        confirmed swing point can't change mid-bar), never on the non-
        position side."""
        pos = self._position
        if pos is None or pos["side"] != side:
            return
        new_s1 = swing_low(self._option_acc[side].bars, pivot=2)
        if new_s1 is not None and new_s1 > pos.get("s1_floor", pos["sl_price"]):
            old = pos.get("s1_floor", pos["sl_price"])
            pos["s1_floor"] = new_s1
            logger.info("OIFlow[%s]: S1 promoted %s: %.2f -> %.2f", self._underlying, side, old, new_s1)
            self._clog.info("S1 promoted %s: %.2f -> %.2f", side, old, new_s1)

    async def _eod_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            if self._position is None or self._position.get("_closing"):
                continue
            now_t = datetime.now(IST).time()
            if now_t >= self._squareoff_time:
                exit_price = self._live_option_ltp.get(self._position["side"], self._position["entry_price"])
                self._exit(reason="eod", exit_price=exit_price)
                self._day_done = True

    # ── entry / exit ─────────────────────────────────────────────────────────

    def _enter(self, side: str, strike: float, entry_price: float, sl_price: Optional[float]) -> None:
        qty = self._lot_size * self._lot_multiplier
        self._event_counter += 1
        eid = f"{self._underlying}_{side}{int(strike)}_ENTRY_{self._event_counter}"
        _initial_sl = sl_price if sl_price is not None else entry_price * 0.8
        self._position = dict(
            side=side, strike=strike, entry_price=entry_price,
            sl_price=_initial_sl,
            entry_ts=datetime.now(IST), qty=qty, _event_id=eid,
            high_lock_pct=0.0,   # step-locked TSL ratchet, see _check_exit
            s1_floor=_initial_sl,   # S1 trailing stop, see _maybe_promote_s1
        )
        self._persist_position()
        logger.info(
            "OIFlow[%s]: ENTER BUY %s %d entry=%.2f sl=%.2f (awaiting broker confirmation, event_id=%s)",
            self._underlying, side, strike, entry_price, self._position["sl_price"], eid,
        )
        self._clog.info("ENTER BUY %s %d entry=%.2f sl=%.2f event_id=%s",
                         side, strike, entry_price, self._position["sl_price"], eid)
        expiry = REGISTRY.get_active_expiry_strict(self._underlying, self._today or datetime.now(IST).date())
        order_ev = OIFlowOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id, action="BUY",
            underlying=self._underlying, option_type=side, strike=int(strike), expiry=expiry,
            quantity=qty, entry_price=entry_price, sl_price=self._position["sl_price"],
            reason="oi_flow_pre_breakout", event_id=eid, product_type=self._product_type,
        )
        if self._bus is not None:
            asyncio.create_task(self._bus.publish(Topic.OI_FLOW_ORDER_REQUEST, order_ev))

    def _exit(self, reason: str, exit_price: float) -> None:
        pos = self._position
        if pos is None or pos.get("_closing"):
            return
        pos["_closing"] = True
        asyncio.create_task(self._square_off(pos, reason, exit_price))

    async def _square_off(self, pos: dict, reason: str, exit_price: float) -> None:
        try:
            self._event_counter += 1
            eid = f"{self._underlying}_{pos['side']}{int(pos['strike'])}_EXIT_{self._event_counter}"
            expiry = REGISTRY.get_active_expiry_strict(self._underlying, self._today or datetime.now(IST).date())
            order_ev = OIFlowOrderEvent(
                client_id=self._client_id, binding_id=self._binding_id, action="SELL",
                underlying=self._underlying, option_type=pos["side"], strike=int(pos["strike"]),
                expiry=expiry, quantity=pos["qty"], entry_price=pos["entry_price"],
                sl_price=pos["sl_price"], exit_price=exit_price, reason=reason, event_id=eid,
                product_type=self._product_type,
            )
            logger.info(
                "OIFlow[%s]: SELL %s %d reason=%s exit=%.2f (awaiting broker confirmation, event_id=%s)",
                self._underlying, pos["side"], pos["strike"], reason, exit_price, eid,
            )
            waiter = asyncio.Event()
            self._fill_waiters[eid] = waiter
            try:
                if self._bus is not None:
                    await self._bus.publish(Topic.OI_FLOW_ORDER_REQUEST, order_ev)
                try:
                    await asyncio.wait_for(waiter.wait(), timeout=_EXIT_CONFIRM_TIMEOUT_SEC)
                except asyncio.TimeoutError:
                    logger.critical(
                        "OIFlow[%s]: EXIT %s%d fill NOT CONFIRMED within %.0fs (event_id=%s reason=%s) "
                        "-- leg stays OPEN; will retry on a later tick/EOD pass.",
                        self._underlying, pos["side"], pos["strike"], _EXIT_CONFIRM_TIMEOUT_SEC, eid, reason,
                    )
                    self._clog.info("EXIT %s%d fill NOT CONFIRMED within %.0fs event_id=%s reason=%s -- retrying later",
                                     pos["side"], pos["strike"], _EXIT_CONFIRM_TIMEOUT_SEC, eid, reason)
                    return
            finally:
                self._fill_waiters.pop(eid, None)

            fill = self._fill_results.pop(eid, None)
            if fill is not None and getattr(fill, "exit_failed", False):
                logger.critical(
                    "OIFlow[%s]: EXIT %s%d ABORTED by bridge (broker unavailable, event_id=%s reason=%s) "
                    "-- leg stays OPEN; will retry on a later tick/EOD pass.",
                    self._underlying, pos["side"], pos["strike"], eid, reason,
                )
                self._clog.info("EXIT %s%d ABORTED by bridge (broker unavailable) event_id=%s reason=%s -- leg stays OPEN",
                                 pos["side"], pos["strike"], eid, reason)
                return

            if self._position is pos:
                self._position = None
            self._persist_position()
            logger.info("OIFlow[%s]: SELL %s %d reason=%s exit=%.2f CONFIRMED (event_id=%s)",
                        self._underlying, pos["side"], pos["strike"], reason, exit_price, eid)
            self._clog.info("SELL %s %d reason=%s exit=%.2f CONFIRMED event_id=%s",
                             pos["side"], pos["strike"], reason, exit_price, eid)
        finally:
            pos["_closing"] = False

    # ── fill confirmation ────────────────────────────────────────────────────

    async def _fill_loop(self) -> None:
        q = self._loop_queues.get(Topic.OI_FLOW_ORDER_FILL)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if not isinstance(ev, OIFlowFillEvent):
                continue
            if ev.client_id != self._client_id or ev.binding_id != self._binding_id \
                    or ev.underlying != self._underlying:
                continue
            try:
                self._on_fill(ev)
            except Exception:
                logger.exception("OIFlow[%s]: _on_fill error (recovered).", self._underlying)

    def _on_fill(self, fill: OIFlowFillEvent) -> None:
        if fill.action == "BUY":
            if fill.entry_aborted and self._position is not None \
                    and self._position.get("_event_id") == fill.event_id:
                logger.critical(
                    "OIFlow[%s]: ENTRY ABORTED (broker unavailable/gate closed, event_id=%s) "
                    "-- discarding optimistic position.", self._underlying, fill.event_id,
                )
                self._clog.info("ENTRY ABORTED (broker unavailable/gate closed) event_id=%s -- discarding position",
                                 fill.event_id)
                self._position = None
                self._persist_position()
            return
        if fill.action == "SELL":
            self._fill_results[fill.event_id] = fill
            waiter = self._fill_waiters.get(fill.event_id)
            if waiter is not None:
                try:
                    waiter.set()
                except RuntimeError:
                    pass

    # ── persistence ──────────────────────────────────────────────────────────

    def _persist_position(self) -> None:
        if self._position is not None:
            d = {k: v for k, v in self._position.items() if not k.startswith("_")}
            d["entry_ts"] = self._position["entry_ts"].isoformat() if self._position.get("entry_ts") else None
            position_store.save(self._persist_key, {"leg": d}, product_type=self._product_type)
        else:
            position_store.clear(self._persist_key)

    def _restore_position(self) -> None:
        data = position_store.load(self._persist_key)
        if not data or not data.get("leg"):
            return
        d = dict(data["leg"])
        try:
            d["entry_ts"] = (datetime.fromisoformat(d["entry_ts"]) if d.get("entry_ts")
                              else datetime.now(IST))
        except Exception:
            logger.exception("OIFlow[%s]: failed to parse stored leg timestamp -- discarding.",
                              self._underlying)
            return
        self._position = d
        logger.info("OIFlow[%s]: RESTORED open leg from disk on restart -- %s%s@%.2f",
                    self._underlying, d["side"], int(d["strike"]), d["entry_price"])
        self._clog.info("RESTORED open leg from disk on restart -- %s%s@%.2f",
                         d["side"], int(d["strike"]), d["entry_price"])

    # ── dashboard ────────────────────────────────────────────────────────────

    def monitoring_state(self) -> dict:
        """Live state for the dashboard's OI-Flow panel -- mirrors D1TrapOptionBook.
        monitoring_zones() / FVGStrategy.monitoring_fvgs()'s own dashboard-surface
        pattern. Shows the OI wall/buildup per strike (the "which strike, what's
        building" the user explicitly wants to see), the recent remarks trail,
        and the live position with running P&L."""
        snap = self._latest_snap
        walls = []
        if snap is not None:
            for side, wall, supporting_side_label in (
                ("CE", snap.max_call_oi_strike, "opposing"), ("PE", snap.max_put_oi_strike, "opposing"),
            ):
                if not wall:
                    continue
                supporting_strike = (wall - self._strike_step) if side == "CE" else (wall + self._strike_step)
                supporting_side = "PE" if side == "CE" else "CE"
                walls.append({
                    "side": side,
                    "wall_strike": wall,
                    "wall_oi": self._oi_tracker.oi_now(wall, side),
                    "wall_oi_roc": self._oi_tracker.oi_roc(wall, side, self._window_sec),
                    "supporting_strike": supporting_strike,
                    "supporting_side": supporting_side,
                    "supporting_oi": self._oi_tracker.oi_now(supporting_strike, supporting_side),
                    "supporting_oi_roc": self._oi_tracker.oi_roc(supporting_strike, supporting_side, self._window_sec),
                })

        position = None
        if self._position is not None:
            pos = self._position
            ltp = self._live_option_ltp.get(pos["side"])
            pnl = ((ltp - pos["entry_price"]) * pos["qty"]) if ltp is not None else None
            high_lock_pct = pos.get("high_lock_pct", 0.0)
            pct_floor = pos["entry_price"] * (1 + high_lock_pct) if high_lock_pct > 0 else pos["sl_price"]
            s1_floor = pos.get("s1_floor", pos["sl_price"])
            position = {
                "side": pos["side"], "strike": pos["strike"], "entry_price": pos["entry_price"],
                "sl_price": pos["sl_price"], "qty": pos["qty"],
                "entry_ts": pos["entry_ts"].isoformat() if pos.get("entry_ts") else None,
                "ltp": ltp, "pnl": pnl,
                "s1_floor": s1_floor, "tsl_floor": pct_floor,
                "effective_stop": max(pct_floor, s1_floor),
            }

        return {
            "underlying": self._underlying,
            "client_id": self._client_id,
            "binding_id": self._binding_id,
            "spot": self._spot_acc.bars[-1].close if self._spot_acc.bars else None,
            "pcr": snap.pcr_smooth() if snap is not None else None,
            "walls": walls,
            "position": position,
            "remarks": list(self._recent_remarks)[:15],
        }
