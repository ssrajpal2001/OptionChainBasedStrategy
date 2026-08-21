"""
strategies/liquidity_sweep/engine.py — LiquiditySweepStrategy, the live/paper
book for the Liquidity Sweep (Sweep + Displacement + FVG + Retest) strategy.

Fully standalone (see strategies/liquidity_sweep/__init__.py) -- owns its own
single-timeframe spot bar accumulator, its own confirm-then-finalize
position lifecycle and persistence namespace. Shares no runtime state with
any other strategy's book. Direct Python port of the iteratively-tuned,
real-chart-validated pinescript/liquidity_sweep_indicator_with_risk.pine --
every parameter default below matches that script's final tuned values (see
CLAUDE.md's "Liquidity Sweep Strategy" section for the tuning history).

SL/Target1/Target2 are computed and monitored in SPOT-INDEX terms, exactly
like the validated Pine script (sweep-candle extreme for SL, R-multiples off
spot risk for Target1, the opposing side's own spot liquidity level for
Target2). This is a DELIBERATE, honestly-flagged design choice, not an
oversight: translating a spot-terms stop into option-premium terms needs a
live delta/greeks model this codebase doesn't have (OptionTick.delta exists
as a field but its reliability across both Upstox/Fyers feeders was not
verified here under time pressure -- inventing a premium-conversion formula
now would be new, unvalidated logic on top of an already price-action-only,
never-backtested-against-real-premium strategy). The option's own live LTP
is simply the fill price whenever a spot-level entry/exit condition fires --
never itself a premium-based SL/target. See events.py's own field comments.

Entry sequencing, on every new LTF spot bar close (mirrors the validated
Pine script's bar-by-bar state machine exactly -- pending-sweep watch ->
displacement window -> FVG confirmation window -> retest-armed window):
  1. Recompute swing points + the active liquidity level (rolling_base /
     swing_pivots / liquidity_pool, per liq_source) + BoS/CHoCH structure
     bias from the full closed-bar history.
  2. detect_sweep() against the active level, gated by structure bias.
  3. Pending sweep -> check_displacement() within disp_window bars.
  4. Displacement -> check_fvg() retried within fvg_confirm_window bars.
  5. FVG confirmed -> check_retest() retried within stale_bars bars.
  6. Retest hit -> compute_trade_plan() (spot SL/T1/T2) -> BUY the option
     at ATM +/- itm_offset_pts, at its current live LTP.
Exit (checked on every live spot INDEX_TICK, more responsive than the
Pine script's own bar-close-only check -- a live/paper position is
monitored MORE often than the validated backtest checked, never less):
  SL hit / Target1 hit (arms a move-to-breakeven, does not exit) /
  breakeven-stop after Target1 / Target2 hit / EOD squareoff / a hard
  Rs-per-lot option-premium risk-cap backstop (independent safety net,
  same constant every other strategy in this codebase uses).

Intraday only -- reset_session() wipes ALL pipeline state (bars, pending
sweep, awaiting-FVG, armed-retest) at the start of every trading day, per
direct user spec ("liquidity sweep is intraday trade, same day close").
"""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import date, datetime, time, timedelta
from typing import Deque, Dict, Optional

from config.global_config import IST, Topic
from data_layer.base_feeder import IndexTick, OptionTick
from data_layer import position_store
from data_layer.instrument_registry import REGISTRY
from strategies.core.base_book import AbstractStrategyBook
from strategies.liquidity_sweep.detector import (
    Bar, BarAccumulator, SwingPoint, compute_rolling_base, find_swing_points,
    latest_swing_level, latest_pool_level, compute_market_structure,
    detect_sweep, check_displacement, check_fvg, check_retest, compute_trade_plan,
)
from strategies.liquidity_sweep.events import LiquiditySweepOrderEvent, LiquiditySweepFillEvent

logger = logging.getLogger(__name__)


def _make_strategy_logger(underlying: str, client_id: str = "", binding_id: str = "") -> logging.Logger:
    """Dedicated, rotating, per-(underlying,client,binding,day) log file --
    explicitly required by the user ("it shodul haev its won log to knwo
    what exactly happend"), same utils.logging_utils.make_strategy_logger
    platform utility every other strategy's own _clog already uses."""
    from utils.logging_utils import make_strategy_logger
    tag = f"{underlying}" + (f"_{client_id}_{binding_id}" if client_id and binding_id else "")
    date_str = datetime.now(IST).strftime("%Y%m%d")
    return make_strategy_logger(f"liqsweep_{tag}_{date_str}", propagate=False)


# ── validated Pine-script defaults (pinescript/liquidity_sweep_indicator_with_risk.pine) ──
_DEFAULT_LTF_MIN = 5
_DEFAULT_HTF_MIN = 75             # rolling_base mode only, not the validated default
_DEFAULT_LIQ_SOURCE = "liquidity_pool"   # "rolling_base" | "swing_pivots" | "liquidity_pool"
_DEFAULT_PIVOT_LEFT = 5
_DEFAULT_PIVOT_RIGHT = 5
_DEFAULT_POOL_TOL_PTS = 5.0
_DEFAULT_POOL_MIN_TOUCHES = 2
_DEFAULT_USE_STRUCT_BIAS = True
_DEFAULT_ATR_LEN = 14
_DEFAULT_ATR_MULT = 0.7
_DEFAULT_DISP_WINDOW = 6
_DEFAULT_SWING_LEN = 3
_DEFAULT_FVG_CONFIRM_WINDOW = 3
_DEFAULT_STALE_BARS = 12
_DEFAULT_TGT1_RR = 1.5
_DEFAULT_USE_LIQUIDITY_TARGET2 = True
_DEFAULT_TGT2_RR = 3.0
_DEFAULT_ITM_OFFSET_PTS = 0.0     # unvalidated for real option execution -- ATM by default
_DEFAULT_HARD_RISK_RS_PER_LOT = 2000.0
_DEFAULT_SL_COOLDOWN_MINUTES = 15.0
_EOD_TIME_DEFAULT = time(15, 15)
_EXIT_CONFIRM_TIMEOUT_SEC = 15.0
_MAX_PLAUSIBLE_TICK_DATE_DRIFT_DAYS = 1
_SESSION_OPEN = time(9, 15)

# Standalone (per this strategy's own zero-shared-runtime mandate -- same
# reasoning already applied throughout this module, not imported from any
# other strategy's own key table).
_UPSTOX_INDEX_KEYS = {
    "NIFTY": "NSE_INDEX|Nifty 50",
    "SENSEX": "BSE_INDEX|SENSEX",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
}


class LiquiditySweepStrategy(AbstractStrategyBook):
    """One instance per (client, binding, underlying)."""

    def __init__(
        self,
        bus,
        cfg,
        underlying: str,
        client_id: str,
        binding_id: str,
        lot_multiplier: int = 1,
        ltf_min: int = _DEFAULT_LTF_MIN,
        htf_min: int = _DEFAULT_HTF_MIN,
        liq_source: str = _DEFAULT_LIQ_SOURCE,
        pivot_left: int = _DEFAULT_PIVOT_LEFT,
        pivot_right: int = _DEFAULT_PIVOT_RIGHT,
        pool_tol_pts: float = _DEFAULT_POOL_TOL_PTS,
        pool_min_touches: int = _DEFAULT_POOL_MIN_TOUCHES,
        use_struct_bias: bool = _DEFAULT_USE_STRUCT_BIAS,
        atr_len: int = _DEFAULT_ATR_LEN,
        atr_mult: float = _DEFAULT_ATR_MULT,
        disp_window: int = _DEFAULT_DISP_WINDOW,
        swing_len: int = _DEFAULT_SWING_LEN,
        fvg_confirm_window: int = _DEFAULT_FVG_CONFIRM_WINDOW,
        stale_bars: int = _DEFAULT_STALE_BARS,
        tgt1_rr: float = _DEFAULT_TGT1_RR,
        use_liquidity_target2: bool = _DEFAULT_USE_LIQUIDITY_TARGET2,
        tgt2_rr: float = _DEFAULT_TGT2_RR,
        itm_offset_pts: float = _DEFAULT_ITM_OFFSET_PTS,
        hard_risk_rs_per_lot: float = _DEFAULT_HARD_RISK_RS_PER_LOT,
        sl_cooldown_minutes: float = _DEFAULT_SL_COOLDOWN_MINUTES,
        product_type: str = "MIS",
        squareoff_time: str = "15:15",
        feeder_token: str = "",
    ) -> None:
        super().__init__(bus, cfg, underlying, client_id, binding_id)
        self._strategy_name = "liquidity_sweep"
        self._feeder_token = feeder_token
        self._warming_up = False   # True while replaying today's REST history on a mid-day (re)start
        self._lot_multiplier = max(1, lot_multiplier)
        self._ltf_min = max(1, int(ltf_min))
        self._htf_min = max(1, int(htf_min))
        self._liq_source = liq_source
        self._pivot_left = pivot_left
        self._pivot_right = pivot_right
        self._pool_tol_pts = pool_tol_pts
        self._pool_min_touches = pool_min_touches
        self._use_struct_bias = use_struct_bias
        self._atr_len = atr_len
        self._atr_mult = atr_mult
        self._disp_window = disp_window
        self._swing_len = swing_len
        self._fvg_confirm_window = fvg_confirm_window
        self._stale_bars = stale_bars
        self._tgt1_rr = tgt1_rr
        self._use_liquidity_target2 = use_liquidity_target2
        self._tgt2_rr = tgt2_rr
        self._itm_offset_pts = itm_offset_pts
        self._hard_risk_rs_per_lot = hard_risk_rs_per_lot
        self._sl_cooldown_minutes = sl_cooldown_minutes
        self._product_type = product_type
        try:
            _h, _m = str(squareoff_time or "15:15").split(":")
            self._squareoff_time = time(int(_h), int(_m))
        except Exception:
            self._squareoff_time = _EOD_TIME_DEFAULT

        self._lot_size = (cfg.exchange.lot_sizes.get(underlying, 75) if cfg else 75)
        self._strike_step = float(cfg.exchange.strike_steps.get(underlying, 100) if cfg else 100)
        self._persist_key = f"{client_id}_{binding_id}_{underlying}_liquidity_sweep"
        self._clog = _make_strategy_logger(underlying, client_id, binding_id)

        self._today: Optional[date] = None
        self._ltf_acc = BarAccumulator(timeframe_min=self._ltf_min)
        self._htf_acc = BarAccumulator(timeframe_min=self._htf_min)
        self._live_ltp: Dict[tuple, float] = {}   # (strike, "CE"|"PE") -> latest live option LTP

        # ── pipeline state (mirrors the validated Pine script's own var state exactly) ──
        self._pending_dir = 0            # 0 | 1 (bull) | -1 (bear) -- sweep armed, watching for displacement
        self._pending_bars_left = 0
        self._sweep_extreme: Optional[float] = None
        self._disp_c1: Optional[Bar] = None            # bar immediately before the displacement candle
        self._awaiting_fvg_dir = 0
        self._fvg_bars_left = 0
        self._armed_sweep_extreme: Optional[float] = None
        self._fvg_dir = 0                # retest-armed direction, 0 if not armed
        self._fvg_lo: Optional[float] = None
        self._fvg_hi: Optional[float] = None
        self._sl_anchor: Optional[float] = None
        self._retest_bars_left = 0
        self._last_bias = 0
        self._last_level_high: Optional[SwingPoint] = None
        self._last_level_low: Optional[SwingPoint] = None

        self._position: Optional[dict] = None
        self._cooldown_until: Optional[datetime] = None
        self._day_done = False
        self._event_counter = 0
        self._fill_waiters: Dict[str, asyncio.Event] = {}
        self._fill_results: Dict[str, object] = {}
        self._recent_remarks: Deque[dict] = deque(maxlen=30)

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def reset_session(self) -> None:
        self._today = None
        self._ltf_acc = BarAccumulator(timeframe_min=self._ltf_min)
        self._htf_acc = BarAccumulator(timeframe_min=self._htf_min)
        self._live_ltp = {}
        self._pending_dir = 0
        self._pending_bars_left = 0
        self._sweep_extreme = None
        self._disp_c1 = None
        self._awaiting_fvg_dir = 0
        self._fvg_bars_left = 0
        self._armed_sweep_extreme = None
        self._fvg_dir = 0
        self._fvg_lo = None
        self._fvg_hi = None
        self._sl_anchor = None
        self._retest_bars_left = 0
        self._last_bias = 0
        self._last_level_high = None
        self._last_level_low = None
        self._cooldown_until = None
        self._day_done = False
        self._recent_remarks.clear()

    def start(self) -> None:
        super().start()
        self._subscribe(Topic.INDEX_TICK)
        self._subscribe(Topic.OPTION_TICK)
        self._subscribe(Topic.LIQUIDITY_SWEEP_ORDER_FILL)
        self._restore_position()
        # Subscribing above already starts buffering live ticks onto this book's
        # own queue even though nothing drains it yet -- so warmup can safely
        # await the REST fetch+replay first (no live tick is lost, just queued)
        # and only THEN start draining/processing them, avoiding any interleaving
        # between historical (possibly-earlier) timestamps and live ones inside
        # the same BarAccumulators. Same pattern as strategies/liquidity_trap/
        # engine.py's own _warmup_then_index_tick_loop.
        self._tasks.append(asyncio.create_task(self._warmup_then_index_tick_loop(), name=f"liqsweep_idx_{self._underlying}"))
        self._tasks.append(asyncio.create_task(self._option_tick_loop(), name=f"liqsweep_opt_{self._underlying}"))
        self._tasks.append(asyncio.create_task(self._fill_loop(), name=f"liqsweep_fill_{self._underlying}"))
        self._tasks.append(asyncio.create_task(self._eod_loop(), name=f"liqsweep_eod_{self._underlying}"))

    async def _warmup_then_index_tick_loop(self) -> None:
        await self._warmup_intraday()
        await self._index_tick_loop()

    async def _warmup_intraday(self) -> None:
        """On a mid-day (re)start, REST-fetch today's real 1-min spot history
        and replay it through the SAME BarAccumulators + _on_ltf_bar_close()
        pipeline live ticks use, so the book doesn't sit silently rebuilding
        its entire sweep/structure/FVG pipeline from scratch for however long
        it takes live ticks alone to accumulate (this strategy is fully
        intraday -- reset_session() wipes everything daily, so a restart with
        no warmup means literally zero history until now). Confirmed gap
        found 2026-08-21 (this strategy had never been warmup-covered at all,
        unlike D1Trap/FVG/Liquidity Trap, which already have this).

        Order placement (_try_enter) is suppressed during replay via
        self._warming_up -- a retest that already fired hours ago can't be
        safely entered now at a stale historical price; _try_enter() logs and
        no-ops instead of firing a live order off old data (mirrors
        D1TrapOptionBook / LiquidityTrapStrategy's own established pattern)."""
        now = datetime.now(IST)
        if now.time() < _SESSION_OPEN:
            return  # pre-market -- nothing has traded yet today, nothing to replay
        if not self._feeder_token:
            self._clog.warning(
                "LiquiditySweep[%s]: no Upstox feeder token available -- skipping intraday "
                "warmup, will build up the sweep/structure/FVG pipeline live from here instead.",
                self._underlying,
            )
            return
        key = _UPSTOX_INDEX_KEYS.get(self._underlying.upper(), f"NSE_INDEX|{self._underlying}")
        try:
            from data_layer.historical_candles import fetch_upstox_intraday_1m
            raw_bars = await fetch_upstox_intraday_1m(key, self._feeder_token)
        except Exception as exc:
            self._clog.warning("LiquiditySweep[%s]: intraday warmup fetch failed: %s", self._underlying, exc)
            return
        if not raw_bars:
            self._clog.warning("LiquiditySweep[%s]: intraday warmup -- 0 bars returned (API empty or key mismatch).",
                                self._underlying)
            return

        self._warming_up = True
        try:
            # CRITICAL: set _today BEFORE replaying -- _index_tick_loop's own
            # "if self._today != today: reset_session()" new-day check runs on
            # the very first LIVE tick it processes, right after this method
            # returns. self._today is still None at this point, so without this
            # line that first live tick would silently wipe out everything just
            # replayed via reset_session(), with zero log trace (the exact bug
            # found and fixed in strategies/liquidity_trap/engine.py earlier
            # today).
            self._today = now.date()
            replayed = 0
            for b in raw_bars:
                try:
                    ts = datetime.fromisoformat(b["ts"]).astimezone(IST)
                    o, h, l, c = float(b["open"]), float(b["high"]), float(b["low"]), float(b["close"])
                except Exception:
                    continue
                if ts.date() != now.date() or ts.time() < _SESSION_OPEN:
                    continue
                # Feed open->high->low->close as 4 synthetic ticks at the bar's own
                # timestamp -- reconstructs the exact same OHLC bucket the live
                # tick-based BarAccumulator would have built minute-by-minute,
                # with zero changes to that already-validated pure class.
                for px in (o, h, l, c):
                    if self._liq_source == "rolling_base":
                        self._htf_acc.on_tick(ts, px)
                    closed = self._ltf_acc.on_tick(ts, px)
                    if closed:
                        self._on_ltf_bar_close()
                replayed += 1
            self._clog.info(
                "LiquiditySweep[%s]: intraday warmup complete -- %d 1m bars replayed, "
                "pending_dir=%d awaiting_fvg_dir=%d fvg_dir=%d position=%s day_done=%s.",
                self._underlying, replayed, self._pending_dir, self._awaiting_fvg_dir,
                self._fvg_dir, "OPEN" if self._position else "flat", self._day_done,
            )
        finally:
            self._warming_up = False

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

    # ── spot ticks -> bars -> pipeline + live exit checks ───────────────────────

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
            real_today = datetime.now(IST).date()
            if abs((today - real_today).days) > _MAX_PLAUSIBLE_TICK_DATE_DRIFT_DAYS:
                logger.warning(
                    "LiquiditySweep[%s]: REJECTED tick with implausible date %s (real date %s) -- ignoring.",
                    self._underlying, today, real_today,
                )
                continue
            if self._today != today:
                self.reset_session()
                self._today = today

            if self._liq_source == "rolling_base":
                self._htf_acc.on_tick(ev.timestamp, ev.ltp)
            closed = self._ltf_acc.on_tick(ev.timestamp, ev.ltp)
            if closed:
                self._on_ltf_bar_close()

            # Live, tick-by-tick exit monitoring -- MORE responsive than the
            # validated Pine script's own bar-close-only check, never less.
            self._check_exit_on_spot(ev.ltp)

    # ── pipeline: sweep -> displacement -> FVG -> retest -> entry ───────────────

    def _active_level(self, bars) -> tuple:
        """Returns (level_high, level_low) per self._liq_source, each a
        SwingPoint (or a SwingPoint-shaped stand-in for rolling_base, whose
        .price == .body_extreme -- see detect_sweep()'s own docstring)."""
        if self._liq_source == "rolling_base":
            base = compute_rolling_base(self._htf_acc.bars)
            if base is None:
                return None, None
            hi, lo = base
            ts = bars[-1].timestamp if bars else datetime.now(IST)
            level_high = SwingPoint(index=-1, timestamp=ts, price=hi, body_extreme=hi, kind="HIGH")
            level_low = SwingPoint(index=-1, timestamp=ts, price=lo, body_extreme=lo, kind="LOW")
            return level_high, level_low

        swings = find_swing_points(bars, self._pivot_left, self._pivot_right)
        if self._liq_source == "swing_pivots":
            return latest_swing_level(swings, "HIGH"), latest_swing_level(swings, "LOW")
        # default: liquidity_pool
        return (
            latest_pool_level(swings, "HIGH", self._pool_tol_pts, self._pool_min_touches),
            latest_pool_level(swings, "LOW", self._pool_tol_pts, self._pool_min_touches),
        )

    def _on_ltf_bar_close(self) -> None:
        bars = self._ltf_acc.bars
        if len(bars) < (self._pivot_left + self._pivot_right + 1):
            return   # not enough history yet to confirm any swing point

        level_high, level_low = self._active_level(bars)
        self._last_level_high, self._last_level_low = level_high, level_low

        bias = 0
        if self._use_struct_bias:
            swings = find_swing_points(bars, self._pivot_left, self._pivot_right)
            bias = compute_market_structure(bars, swings, self._pivot_right).trend
        self._last_bias = bias

        cur = bars[-1]
        sweep = detect_sweep(cur, level_high, level_low)
        bear_ok = sweep.bear and bias != 1
        bull_ok = sweep.bull and bias != -1

        just_armed = False
        if bear_ok and self._pending_dir == 0:
            self._pending_dir = -1
            self._pending_bars_left = self._disp_window
            self._sweep_extreme = cur.high
            just_armed = True
            self._clog.info("SWEEP HIGH armed @ %.2f (level=%.2f) bias=%d", cur.high, level_high.price, bias)
        if bull_ok and self._pending_dir == 0:
            self._pending_dir = 1
            self._pending_bars_left = self._disp_window
            self._sweep_extreme = cur.low
            just_armed = True
            self._clog.info("SWEEP LOW armed @ %.2f (level=%.2f) bias=%d", cur.low, level_low.price, bias)

        if self._pending_dir != 0:
            prior_bars = bars[:-1]
            is_disp = check_displacement(
                cur, prior_bars, self._pending_dir,
                swing_len=self._swing_len, atr_len=self._atr_len, atr_mult=self._atr_mult,
            )
            if is_disp:
                self._disp_c1 = prior_bars[-1] if prior_bars else cur
                self._awaiting_fvg_dir = self._pending_dir
                self._fvg_bars_left = self._fvg_confirm_window
                self._armed_sweep_extreme = self._sweep_extreme
                self._clog.info("DISPLACEMENT dir=%d close=%.2f -- awaiting FVG (window=%d bars)",
                                 self._pending_dir, cur.close, self._fvg_confirm_window)
                self._pending_dir = 0
            elif not just_armed:
                self._pending_bars_left -= 1
                if self._pending_bars_left <= 0:
                    self._pending_dir = 0

        if self._awaiting_fvg_dir != 0:
            gap = check_fvg(self._disp_c1, cur, self._awaiting_fvg_dir)
            if gap is not None:
                self._fvg_dir = self._awaiting_fvg_dir
                self._fvg_lo, self._fvg_hi = gap
                self._sl_anchor = self._armed_sweep_extreme
                self._retest_bars_left = self._stale_bars
                self._clog.info("FVG confirmed dir=%d zone=[%.2f,%.2f] sl_anchor=%.2f -- armed for retest (window=%d bars)",
                                 self._fvg_dir, self._fvg_lo, self._fvg_hi, self._sl_anchor, self._stale_bars)
                self._awaiting_fvg_dir = 0
            else:
                self._fvg_bars_left -= 1
                if self._fvg_bars_left <= 0:
                    self._clog.info("FVG confirmation window expired dir=%d -- giving up", self._awaiting_fvg_dir)
                    self._awaiting_fvg_dir = 0

        if self._fvg_dir != 0:
            if check_retest(cur, self._fvg_lo, self._fvg_hi):
                self._clog.info("RETEST hit dir=%d @ close=%.2f zone=[%.2f,%.2f]",
                                 self._fvg_dir, cur.close, self._fvg_lo, self._fvg_hi)
                self._try_enter(self._fvg_dir, cur.close, level_high, level_low)
                self._fvg_dir = 0
            else:
                self._retest_bars_left -= 1
                if self._retest_bars_left <= 0:
                    self._clog.info("Retest window expired dir=%d -- giving up", self._fvg_dir)
                    self._fvg_dir = 0

    def _try_enter(self, direction: int, entry_spot: float, level_high, level_low) -> None:
        side = "CE" if direction == 1 else "PE"
        if self._warming_up:
            # This retest already happened hours ago, before this process was
            # even watching -- entering NOW would mean paying a live fill price
            # against an hours-stale spot reference. No retroactive entry; the
            # caller (_on_ltf_bar_close) always resets _fvg_dir=0 right after
            # calling this regardless, so the pipeline moves on to watch for the
            # next sweep normally once live ticks resume.
            self._clog.info("%s retest already fired @ %.2f before this session started watching "
                             "-- missed for today (no stale-price retroactive entry).", side, entry_spot)
            return
        if self._day_done or self._position is not None:
            self._clog.info("%s retest fired but skipped (day_done=%s, already_in_position=%s)",
                             side, self._day_done, self._position is not None)
            return
        now = datetime.now(IST)
        if now.time() >= self._squareoff_time:
            self._clog.info("%s retest fired but skipped (past squareoff time)", side)
            return
        if self._cooldown_until is not None:
            if now < self._cooldown_until:
                remaining_min = (self._cooldown_until - now).total_seconds() / 60.0
                self._clog.info("%s retest fired but COOLDOWN active -- %.1f min remaining", side, remaining_min)
                return
            self._cooldown_until = None

        opposing_liquidity = None
        if self._use_liquidity_target2:
            # Bullish (CE, direction=1) targets the resistance ABOVE (level_high);
            # bearish (PE, direction=-1) targets the support BELOW (level_low) --
            # both are "the next opposing liquidity in the trade's profit
            # direction". compute_trade_plan() itself double-checks the level
            # actually sits on the correct side of entry before using it, so
            # picking the wrong one here would just silently fall back to the
            # R-multiple rather than corrupt the trade -- still worth getting
            # right, since it's the whole point of use_liquidity_target2.
            opp_level = level_high if direction == 1 else level_low
            if opp_level is not None:
                opposing_liquidity = opp_level.price

        plan = compute_trade_plan(
            direction, entry_spot, self._sl_anchor,
            tgt1_rr=self._tgt1_rr, tgt2_rr=self._tgt2_rr, opposing_liquidity=opposing_liquidity,
        )

        atm = round(entry_spot / self._strike_step) * self._strike_step
        offset = self._itm_offset_pts
        strike = (atm - offset) if side == "CE" else (atm + offset)
        strike = round(strike / self._strike_step) * self._strike_step

        option_ltp = self._live_ltp.get((strike, side))
        if option_ltp is None:
            self._clog.info("%s both gates passed (strike=%s) but no live option LTP yet -- skipping this entry",
                             side, int(strike))
            return

        self._clog.info(
            "%s TRADE PLAN entry_spot=%.2f sl=%.2f t1=%.2f t2=%.2f (t2_is_liquidity=%s) strike=%d option_ltp=%.2f",
            side, plan.entry, plan.sl, plan.t1, plan.t2, plan.t2_is_liquidity, int(strike), option_ltp,
        )
        self._enter(side, strike, option_ltp, plan)

    # ── option ticks -> live LTP ─────────────────────────────────────────────

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
            self._live_ltp[(float(ev.strike), str(ev.option_type).upper())] = ev.ltp

    # ── exit checks (spot-level SL/T1/T2, live tick-by-tick) ────────────────────

    def _check_exit_on_spot(self, spot_ltp: float) -> None:
        pos = self._position
        if pos is None or pos.get("_closing"):
            return
        direction = pos["direction"]
        sl = pos["sl_spot"]
        t1 = pos["t1_spot"]
        t2 = pos["t2_spot"]
        entry_spot = pos["entry_spot"]

        # Target1 hit -> arm move-to-breakeven, DO NOT exit -- matches the
        # validated Pine script's useBreakeven mechanic exactly (no partial
        # booking at T1 in the current tuned version, see engine.py's own
        # module docstring / CLAUDE.md).
        if not pos.get("t1_hit"):
            hit_t1 = (spot_ltp >= t1) if direction == 1 else (spot_ltp <= t1)
            if hit_t1:
                pos["t1_hit"] = True
                logger.info("LiquiditySweep[%s]: %s Target1 hit @ spot=%.2f -> SL moved to breakeven %.2f",
                            self._underlying, pos["side"], spot_ltp, entry_spot)
                self._clog.info("TARGET1 hit @ spot=%.2f -> SL moved to breakeven %.2f", spot_ltp, entry_spot)

        effective_sl = entry_spot if pos.get("t1_hit") else sl
        option_ltp = self._live_ltp.get((pos["strike"], pos["side"]), pos["entry_price"])

        if direction == 1:
            if spot_ltp <= effective_sl:
                reason = "breakeven_stop" if pos.get("t1_hit") else "sl_hit"
                self._exit(reason=reason, exit_price=option_ltp)
                return
            if spot_ltp >= t2:
                self._exit(reason="target2_hit", exit_price=option_ltp)
                return
        else:
            if spot_ltp >= effective_sl:
                reason = "breakeven_stop" if pos.get("t1_hit") else "sl_hit"
                self._exit(reason=reason, exit_price=option_ltp)
                return
            if spot_ltp <= t2:
                self._exit(reason="target2_hit", exit_price=option_ltp)
                return

        # Independent safety backstop, option-PREMIUM terms (unlike the main
        # SL/T1/T2 pipeline above) -- same hard Rs/lot cap constant every
        # other option-buyer strategy in this codebase uses, guards against
        # the option premium diverging badly from the spot-based read (IV
        # crush, a wide bid/ask, a stale/thin strike) that the spot SL alone
        # would not catch.
        risk_floor = pos["entry_price"] - (self._hard_risk_rs_per_lot / (self._lot_size * self._lot_multiplier))
        if option_ltp <= risk_floor:
            self._exit(reason=f"hard_risk_cap@{risk_floor:.2f}", exit_price=option_ltp)

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
                exit_price = self._live_ltp.get(
                    (self._position["strike"], self._position["side"]), self._position["entry_price"],
                )
                self._exit(reason="eod", exit_price=exit_price)
                self._day_done = True

    # ── entry / exit ─────────────────────────────────────────────────────────

    def _enter(self, side: str, strike: float, entry_price: float, plan) -> None:
        qty = self._lot_size * self._lot_multiplier
        self._event_counter += 1
        eid = f"{self._underlying}_{side}{int(strike)}_ENTRY_{self._event_counter}"
        self._position = dict(
            side=side, direction=plan.direction, strike=strike, entry_price=entry_price,
            entry_spot=plan.entry, sl_spot=plan.sl, t1_spot=plan.t1, t2_spot=plan.t2,
            t1_hit=False, entry_ts=datetime.now(IST), qty=qty, _event_id=eid,
        )
        self._persist_position()
        logger.info(
            "LiquiditySweep[%s]: ENTER BUY %s %d entry=%.2f sl_spot=%.2f t1_spot=%.2f t2_spot=%.2f "
            "(awaiting broker confirmation, event_id=%s)",
            self._underlying, side, strike, entry_price, plan.sl, plan.t1, plan.t2, eid,
        )
        self._clog.info("ENTER BUY %s %d entry=%.2f sl_spot=%.2f t1_spot=%.2f t2_spot=%.2f event_id=%s",
                         side, strike, entry_price, plan.sl, plan.t1, plan.t2, eid)
        remark = f"{side} ENTERED @{entry_price:.2f} strike={int(strike)} sl_spot={plan.sl:.2f} t1_spot={plan.t1:.2f} t2_spot={plan.t2:.2f}"
        self._recent_remarks.appendleft({"ts": datetime.now(IST).isoformat(), "side": side, "level": "entry", "text": remark})
        expiry = REGISTRY.get_active_expiry_strict(self._underlying, self._today or datetime.now(IST).date())
        order_ev = LiquiditySweepOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id, action="BUY",
            underlying=self._underlying, option_type=side, strike=int(strike), expiry=expiry,
            quantity=qty, entry_price=entry_price, sl_price=plan.sl, target1_price=plan.t1,
            target2_price=plan.t2, reason="liquidity_sweep_retest", event_id=eid,
            product_type=self._product_type,
        )
        if self._bus is not None:
            asyncio.create_task(self._bus.publish(Topic.LIQUIDITY_SWEEP_ORDER_REQUEST, order_ev))

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
            order_ev = LiquiditySweepOrderEvent(
                client_id=self._client_id, binding_id=self._binding_id, action="SELL",
                underlying=self._underlying, option_type=pos["side"], strike=int(pos["strike"]),
                expiry=expiry, quantity=pos["qty"], entry_price=pos["entry_price"],
                sl_price=pos["sl_spot"], target1_price=pos["t1_spot"], target2_price=pos["t2_spot"],
                exit_price=exit_price, reason=reason, event_id=eid,
                product_type=self._product_type, entry_ts=pos.get("entry_ts"),
            )
            logger.info(
                "LiquiditySweep[%s]: SELL %s %d reason=%s exit=%.2f (awaiting broker confirmation, event_id=%s)",
                self._underlying, pos["side"], pos["strike"], reason, exit_price, eid,
            )
            waiter = asyncio.Event()
            self._fill_waiters[eid] = waiter
            try:
                if self._bus is not None:
                    await self._bus.publish(Topic.LIQUIDITY_SWEEP_ORDER_REQUEST, order_ev)
                try:
                    await asyncio.wait_for(waiter.wait(), timeout=_EXIT_CONFIRM_TIMEOUT_SEC)
                except asyncio.TimeoutError:
                    logger.critical(
                        "LiquiditySweep[%s]: EXIT %s%d fill NOT CONFIRMED within %.0fs (event_id=%s reason=%s) "
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
                    "LiquiditySweep[%s]: EXIT %s%d ABORTED by bridge (broker unavailable, event_id=%s reason=%s) "
                    "-- leg stays OPEN; will retry on a later tick/EOD pass.",
                    self._underlying, pos["side"], pos["strike"], eid, reason,
                )
                self._clog.info("EXIT %s%d ABORTED by bridge (broker unavailable) event_id=%s reason=%s -- leg stays OPEN",
                                 pos["side"], pos["strike"], eid, reason)
                return

            filled_qty = getattr(fill, "filled_qty", pos["qty"]) if fill is not None else pos["qty"]
            if 0 < filled_qty < pos["qty"]:
                logger.critical(
                    "LiquiditySweep[%s]: EXIT %s%d PARTIAL FILL (event_id=%s reason=%s): closed %d of %d lots -- "
                    "%d lots STILL OPEN, will retry closing the remainder on a later tick/EOD pass.",
                    self._underlying, pos["side"], pos["strike"], eid, reason,
                    filled_qty, pos["qty"], pos["qty"] - filled_qty,
                )
                self._clog.info("EXIT %s%d PARTIAL FILL event_id=%s: closed %d of %d lots -- %d STILL OPEN",
                                 pos["side"], pos["strike"], eid, filled_qty, pos["qty"], pos["qty"] - filled_qty)
                pos["qty"] -= filled_qty
                self._persist_position()
                return

            if self._position is pos:
                self._position = None
            if reason != "eod" and self._sl_cooldown_minutes > 0:
                self._cooldown_until = datetime.now(IST) + timedelta(minutes=self._sl_cooldown_minutes)
                logger.info("LiquiditySweep[%s]: cooldown active for %.0f min after %s exit (until %s)",
                            self._underlying, self._sl_cooldown_minutes, reason, self._cooldown_until.isoformat())
                self._clog.info("COOLDOWN started: %.0f min after exit reason=%s",
                                 self._sl_cooldown_minutes, reason)
            self._persist_position()
            logger.info("LiquiditySweep[%s]: SELL %s %d reason=%s exit=%.2f CONFIRMED (event_id=%s)",
                        self._underlying, pos["side"], pos["strike"], reason, exit_price, eid)
            self._clog.info("SELL %s %d reason=%s exit=%.2f CONFIRMED event_id=%s",
                             pos["side"], pos["strike"], reason, exit_price, eid)
            remark = f"{pos['side']} EXITED reason={reason} @{exit_price:.2f}"
            self._recent_remarks.appendleft({"ts": datetime.now(IST).isoformat(), "side": pos["side"], "level": "exit", "text": remark})
        finally:
            pos["_closing"] = False

    # ── fill confirmation ────────────────────────────────────────────────────

    async def _fill_loop(self) -> None:
        q = self._loop_queues.get(Topic.LIQUIDITY_SWEEP_ORDER_FILL)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if not isinstance(ev, LiquiditySweepFillEvent):
                continue
            if ev.client_id != self._client_id or ev.binding_id != self._binding_id \
                    or ev.underlying != self._underlying:
                continue
            try:
                self._on_fill(ev)
            except Exception:
                logger.exception("LiquiditySweep[%s]: _on_fill error (recovered).", self._underlying)

    def _on_fill(self, fill: LiquiditySweepFillEvent) -> None:
        if fill.action == "BUY":
            if self._position is None or self._position.get("_event_id") != fill.event_id:
                return
            if fill.entry_aborted:
                logger.critical(
                    "LiquiditySweep[%s]: ENTRY ABORTED (broker unavailable/gate closed, event_id=%s) "
                    "-- discarding optimistic position.", self._underlying, fill.event_id,
                )
                self._clog.info("ENTRY ABORTED (broker unavailable/gate closed) event_id=%s -- discarding position",
                                 fill.event_id)
                self._position = None
                self._persist_position()
                return
            filled_qty = fill.filled_qty or self._position["qty"]
            if 0 < filled_qty < self._position["qty"]:
                logger.critical(
                    "LiquiditySweep[%s]: ENTRY %s%d PARTIAL FILL (event_id=%s): requested %d, filled %d -- "
                    "position qty reconciled down.", self._underlying, self._position["side"],
                    self._position["strike"], fill.event_id, self._position["qty"], filled_qty,
                )
                self._clog.info("ENTRY %s%d PARTIAL FILL event_id=%s: requested %d, filled %d -- qty reconciled",
                                 self._position["side"], self._position["strike"], fill.event_id,
                                 self._position["qty"], filled_qty)
                self._position["qty"] = filled_qty
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
            logger.exception("LiquiditySweep[%s]: failed to parse stored leg timestamp -- discarding.",
                              self._underlying)
            return
        self._position = d
        logger.info("LiquiditySweep[%s]: RESTORED open leg from disk on restart -- %s%s@%.2f",
                    self._underlying, d["side"], int(d["strike"]), d["entry_price"])
        self._clog.info("RESTORED open leg from disk on restart -- %s%s@%.2f",
                         d["side"], int(d["strike"]), d["entry_price"])

    # ── dashboard ────────────────────────────────────────────────────────────

    def monitoring_state(self) -> dict:
        """Live state for the dashboard's Liquidity Sweep panel -- mirrors
        FVGStrategy.monitoring_fvgs()/OIFlowStrategy.monitoring_state()'s
        own dashboard-surface pattern."""
        position = None
        if self._position is not None:
            pos = self._position
            ltp = self._live_ltp.get((pos["strike"], pos["side"]))
            pnl = ((ltp - pos["entry_price"]) * pos["qty"]) if ltp is not None else None
            position = {
                "side": pos["side"], "strike": pos["strike"], "entry_price": pos["entry_price"],
                "entry_spot": pos["entry_spot"], "sl_spot": pos["sl_spot"], "t1_spot": pos["t1_spot"],
                "t2_spot": pos["t2_spot"], "t1_hit": pos.get("t1_hit", False), "qty": pos["qty"],
                "entry_ts": pos["entry_ts"].isoformat() if pos.get("entry_ts") else None,
                "ltp": ltp, "pnl": pnl,
            }

        pipeline = "idle"
        if self._fvg_dir != 0:
            pipeline = "retest_armed"
        elif self._awaiting_fvg_dir != 0:
            pipeline = "awaiting_fvg"
        elif self._pending_dir != 0:
            pipeline = "awaiting_displacement"

        return {
            "underlying": self._underlying,
            "client_id": self._client_id,
            "binding_id": self._binding_id,
            "spot": self._ltf_acc.bars[-1].close if self._ltf_acc.bars else None,
            "bias": self._last_bias,
            "level_high": self._last_level_high.price if self._last_level_high else None,
            "level_low": self._last_level_low.price if self._last_level_low else None,
            "pipeline_state": pipeline,
            "position": position,
            "remarks": list(self._recent_remarks)[:15],
        }
