"""
strategies/liquidity_trap/engine.py — LiquidityTrapStrategy.

Live per-(client,binding,underlying) book driving strategies/liquidity_trap/
detector.py's pipeline. Re-scans GROWING per-day bar lists on every new bar
close (not an incremental/streaming state machine) -- exactly mirrors
scripts/liquidity_trap_multiref_backtest.py, the real-data-validated source
of truth, so this can never behaviorally drift from what was actually
backtested.

MULTI-REF mechanic (2026-08-21, user spec, superseding the original single-
lock design scripts/liquidity_trap_backtest.py validated): "each candle can
be a separate ref, for long or short" -- every consecutive 15m-or-configured
ref-tf candle pair independently spawns its own setup on a clean one-sided
breach, tracked fully in parallel via self._setups (Stage 1-3 all run
per-setup, simultaneously, regardless of what any other setup is doing).
Only ONE option position open at a time: a setup reaching Stage 4 (CHoCH)
while flat enters; while ALREADY in a position, a same-direction CHoCH is
silently ignored (already in that direction) and an opposite-direction CHoCH
is also skipped, not a flip (skip-if-blocked variant -- the higher-PF of the
two variants backtested; flip was tested too but not adopted, see
scripts/liquidity_trap_multiref_backtest.py's own history/results).

Real-data-validated optimization pass (scripts/liquidity_trap_
tf_and_trend_sweep.py) on top of the multi-ref mechanic: ref_tf=20m /
confirm_tf=3m (was 15m/5m) + a 60m/10-period-SMA higher-timeframe trend
filter (only enter WITH the trend) together took the 1-year SENSEX backtest
from 930 trades/77.8% win/PF 1.81 to 392 trades/82.7% win/PF 2.67 -- fewer
trades, higher win rate, AND higher PF simultaneously, the only tested
config that hit all three. Both are now the LIVE defaults (still fully
overridable per-deployment via strategy_params, same as every other tunable
in this strategy).

SL/Target are SPOT-INDEX levels (see strategies/liquidity_trap/__init__.py's
own docstring for the full rationale, same honest design choice as
strategies/liquidity_sweep/): checked every spot tick, MORE responsive than
the validated backtest's own bar-close-only checks, never less. The
option's own live LTP is simply the fill price whenever a spot-level
entry/exit/add-on condition fires.

VWAP / change-in-OI / max-pain / open-interest filters were explicitly
considered and are NOT implemented here -- confirmed impossible to backtest
with this codebase's historical data source (Upstox's historical index-
candle API returns volume=0 and oi=0 on every row for spot indices, same
root limitation strategies/oi_flow/ already hit and documented). Only
forward/live telemetry could validate those; not attempted in this pass.

Already live-deployed on NIFTY/SENSEX as of this build (see CLAUDE.md) --
this mechanic change ships to that existing deployment, not a fresh rollout.
"""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import date, datetime, time, timedelta
from typing import Deque, Dict, List, Optional

from config.global_config import IST, Topic
from data_layer.base_feeder import IndexTick, OptionTick
from data_layer import position_store
from strategies.core.base_book import AbstractStrategyBook
from strategies.liquidity_trap.detector import (
    Bar, BarAccumulator,
    find_sl_hit, find_5m_confirmation,
    find_choch_entry, compute_sl_target, find_scale_in_level,
    find_all_setups, compute_trend,
)
from strategies.liquidity_trap.events import LiquidityTrapOrderEvent, LiquidityTrapFillEvent

logger = logging.getLogger(__name__)


def _make_strategy_logger(underlying: str, client_id: str = "", binding_id: str = "") -> logging.Logger:
    """Dedicated, rotating, per-(underlying,client,binding,day) log file --
    same utils.logging_utils.make_strategy_logger platform utility every
    other strategy's own _clog already uses."""
    from utils.logging_utils import make_strategy_logger
    tag = f"{underlying}" + (f"_{client_id}_{binding_id}" if client_id and binding_id else "")
    date_str = datetime.now(IST).strftime("%Y%m%d")
    return make_strategy_logger(f"liqtrap_{tag}_{date_str}", propagate=False)


_DEFAULT_ITM_OFFSET_PTS = 0.0
_DEFAULT_LOTS_INITIAL = 2
_DEFAULT_RR = 2.0
_DEFAULT_HARD_RISK_RS_PER_LOT = 2000.0
_EOD_TIME_DEFAULT = time(15, 15)
_EXIT_CONFIRM_TIMEOUT_SEC = 15.0
_MAX_PLAUSIBLE_TICK_DATE_DRIFT_DAYS = 1
_SESSION_OPEN = time(9, 15)

# Real-data-validated optimization defaults (2026-08-21,
# scripts/liquidity_trap_tf_and_trend_sweep.py) -- see module docstring.
_DEFAULT_REF_TF_MIN = 20
_DEFAULT_CONFIRM_TF_MIN = 3
_DEFAULT_TREND_TF_MIN = 60
_DEFAULT_TREND_SMA_LEN = 10
_DEFAULT_TREND_FILTER_ENABLED = True

# Standalone (per this strategy's own zero-shared-runtime mandate -- not
# imported from strategies/d1_trap_option/book.py's own _upstox_key_for,
# same reasoning already applied to detector.py's swing/pool logic).
_UPSTOX_INDEX_KEYS = {
    "NIFTY": "NSE_INDEX|Nifty 50",
    "SENSEX": "BSE_INDEX|SENSEX",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
}


class _LiveSetup:
    """Mutable per-setup pipeline state (multi-ref, 2026-08-21) -- one of
    these is created for every setup find_all_setups() spawns, tracked
    independently in LiquidityTrapStrategy._setups until it's consumed
    (entered, or its one CHoCH moment fires but gets skipped/filtered)."""

    __slots__ = ("ref_idx", "direction", "locked_idx", "sl_hit_ts", "confirm_ts",
                 "sweep_extreme", "dead")

    def __init__(self, ref_idx: int, direction: str, locked_idx: int) -> None:
        self.ref_idx = ref_idx
        self.direction = direction
        self.locked_idx = locked_idx
        self.sl_hit_ts: Optional[datetime] = None
        self.confirm_ts: Optional[datetime] = None
        self.sweep_extreme: Optional[float] = None
        self.dead = False   # entered, or its CHoCH fired but was filtered/skipped/stale


class LiquidityTrapStrategy(AbstractStrategyBook):
    """One instance per (client, binding, underlying)."""

    def __init__(
        self,
        bus,
        cfg,
        underlying: str,
        client_id: str,
        binding_id: str,
        lot_multiplier: int = 1,
        lots_initial: int = _DEFAULT_LOTS_INITIAL,
        rr: float = _DEFAULT_RR,
        itm_offset_pts: float = _DEFAULT_ITM_OFFSET_PTS,
        scale_in_enabled: bool = True,
        hard_risk_rs_per_lot: float = _DEFAULT_HARD_RISK_RS_PER_LOT,
        product_type: str = "MIS",
        squareoff_time: str = "15:15",
        feeder_token: str = "",
        ref_tf_min: int = _DEFAULT_REF_TF_MIN,
        confirm_tf_min: int = _DEFAULT_CONFIRM_TF_MIN,
        trend_tf_min: int = _DEFAULT_TREND_TF_MIN,
        trend_sma_len: int = _DEFAULT_TREND_SMA_LEN,
        trend_filter_enabled: bool = _DEFAULT_TREND_FILTER_ENABLED,
    ) -> None:
        super().__init__(bus, cfg, underlying, client_id, binding_id)
        self._strategy_name = "liquidity_trap"
        self._feeder_token = feeder_token
        self._warming_up = False   # True while replaying today's REST history on a mid-day (re)start
        self._lot_multiplier = max(1, lot_multiplier)
        self._lots_initial = max(1, int(lots_initial))
        self._rr = rr
        self._itm_offset_pts = itm_offset_pts
        self._scale_in_enabled = scale_in_enabled
        self._hard_risk_rs_per_lot = hard_risk_rs_per_lot
        self._product_type = product_type
        self._ref_tf_min = max(1, int(ref_tf_min))
        self._confirm_tf_min = max(1, int(confirm_tf_min))
        self._trend_tf_min = max(1, int(trend_tf_min))
        self._trend_sma_len = max(1, int(trend_sma_len))
        self._trend_filter_enabled = bool(trend_filter_enabled)
        try:
            _h, _m = str(squareoff_time or "15:15").split(":")
            self._squareoff_time = time(int(_h), int(_m))
        except Exception:
            self._squareoff_time = _EOD_TIME_DEFAULT

        self._lot_size = (cfg.exchange.lot_sizes.get(underlying, 75) if cfg else 75)
        self._strike_step = float(cfg.exchange.strike_steps.get(underlying, 100) if cfg else 100)
        self._persist_key = f"{client_id}_{binding_id}_{underlying}_liquidity_trap"
        self._clog = _make_strategy_logger(underlying, client_id, binding_id)

        self._today: Optional[date] = None
        self._acc_ref = BarAccumulator(timeframe_min=self._ref_tf_min)
        self._acc_confirm = BarAccumulator(timeframe_min=self._confirm_tf_min)
        self._acc_trend = BarAccumulator(timeframe_min=self._trend_tf_min)
        self._acc_1m = BarAccumulator(timeframe_min=1)
        self._live_ltp: Dict[tuple, float] = {}

        # ── pipeline state (multi-ref, mirrors scripts/liquidity_trap_multiref_
        # backtest.py exactly) -- any number of setups tracked in parallel,
        # instead of one single global bias/ref_idx/lock_idx. ──
        self._setups: List["_LiveSetup"] = []
        self._day_done = False       # past squareoff time only -- NOT "one trade per day"
        self._ref_watch_count = 0    # len(bars_ref) as of the last REF log line, so we
                                      # only log once per new ref-tf close, not every 1m tick

        self._position: Optional[dict] = None
        self._cooldown_until: Optional[datetime] = None
        self._event_counter = 0
        self._fill_waiters: Dict[str, asyncio.Event] = {}
        self._fill_results: Dict[str, object] = {}
        self._recent_remarks: Deque[dict] = deque(maxlen=30)

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def reset_session(self) -> None:
        self._today = None
        self._acc_ref = BarAccumulator(timeframe_min=self._ref_tf_min)
        self._acc_confirm = BarAccumulator(timeframe_min=self._confirm_tf_min)
        # _acc_trend is deliberately NOT reset here -- trend context spans
        # multiple days (see _seed_trend_history's own docstring); wiping it
        # daily would mean the 10-period SMA can never have enough history
        # within a single ~6.25-hour trading day.
        self._acc_1m = BarAccumulator(timeframe_min=1)
        self._live_ltp = {}
        self._setups = []
        self._day_done = False
        self._ref_watch_count = 0
        self._cooldown_until = None
        self._recent_remarks.clear()

    def start(self) -> None:
        super().start()
        self._subscribe(Topic.INDEX_TICK)
        self._subscribe(Topic.OPTION_TICK)
        self._subscribe(Topic.LIQUIDITY_TRAP_ORDER_FILL)
        self._restore_position()
        # Subscribing above already starts buffering live ticks onto this book's
        # own queue even though nothing drains it yet -- so warmup can safely
        # await the REST fetch+replay first (no live tick is lost, just queued)
        # and only THEN start draining/processing them, avoiding any interleaving
        # between historical (possibly-earlier) timestamps and live ones inside
        # the same BarAccumulators.
        self._tasks.append(asyncio.create_task(self._warmup_then_index_tick_loop(), name=f"liqtrap_idx_{self._underlying}"))
        self._tasks.append(asyncio.create_task(self._option_tick_loop(), name=f"liqtrap_opt_{self._underlying}"))
        self._tasks.append(asyncio.create_task(self._fill_loop(), name=f"liqtrap_fill_{self._underlying}"))
        self._tasks.append(asyncio.create_task(self._eod_loop(), name=f"liqtrap_eod_{self._underlying}"))

    async def _warmup_then_index_tick_loop(self) -> None:
        await self._seed_trend_history()
        await self._warmup_intraday()
        await self._index_tick_loop()

    async def _seed_trend_history(self) -> None:
        """The 60m/SMA10 trend filter needs 10 HOURS of closed trend-tf bars
        -- a single trading day (~6.25 hours) can never accumulate that much,
        so self._acc_trend legitimately spans MULTIPLE days (same reasoning
        as FVG's own PDH/PDL and swing structure staying multi-day while its
        intraday-only FVG pool doesn't -- see CLAUDE.md's FVG section,
        mechanic 5b). reset_session() never touches self._acc_trend (unlike
        _acc_ref/_acc_confirm/_acc_1m, which are correctly intraday-only).
        Seeds with real PAST trading days' data via REST before the live
        tick loop starts; a failure here just means the trend filter starts
        blind (compute_trend() returns None -> no entries at all until it
        warms up from scratch on live ticks over the following days) rather
        than blocking startup."""
        if not self._feeder_token or self._acc_trend.bars:
            return   # no token, or already seeded (never re-seed on a same-process restart)
        key = _UPSTOX_INDEX_KEYS.get(self._underlying.upper(), f"NSE_INDEX|{self._underlying}")
        try:
            from data_layer.historical_candles import fetch_upstox_range_1m
            end = datetime.now(IST).date() - timedelta(days=1)
            start = end - timedelta(days=10)   # comfortably covers >=2 real trading days past weekends/holidays
            raw_bars = await fetch_upstox_range_1m(key, self._feeder_token, start, end)
        except Exception as exc:
            self._clog.warning(
                "LiquidityTrap[%s]: trend history seed fetch failed: %s -- trend filter starts "
                "blind, will warm up live over the following days instead.", self._underlying, exc,
            )
            return
        if not raw_bars:
            self._clog.warning("LiquidityTrap[%s]: trend history seed -- 0 bars returned.", self._underlying)
            return
        tmp = BarAccumulator(timeframe_min=self._trend_tf_min)
        for b in raw_bars:
            try:
                ts = datetime.fromisoformat(b["ts"]).astimezone(IST)
                o, h, l, c = float(b["open"]), float(b["high"]), float(b["low"]), float(b["close"])
            except Exception:
                continue
            for px in (o, h, l, c):
                tmp.on_tick(ts, px)
        self._acc_trend.bars = tmp.bars[-(self._trend_sma_len * 3):]   # generous cap, not unbounded growth
        self._clog.info(
            "LiquidityTrap[%s]: trend history seeded -- %d closed %dm bars from %s..%s.",
            self._underlying, len(self._acc_trend.bars), self._trend_tf_min, start, end,
        )

    async def _warmup_intraday(self) -> None:
        """On a mid-day (re)start, REST-fetch today's real 1-min spot history and
        replay it through the SAME BarAccumulators + _on_bar_close() pipeline live
        ticks use, so the book doesn't sit silently 'waiting for ref candle' for
        hours it already lived through before this process started.

        Order placement (Stage4 CHoCH -> _try_enter) is suppressed during replay
        via self._warming_up: a setup whose CHoCH already fired hours ago can't
        be safely entered now at that stale historical price, so it's simply
        marked dead/consumed instead of firing a live order off old data
        (mirrors D1TrapOptionBook._warmup_intraday's own established pattern --
        state catch-up yes, phantom/stale-price live orders no). Other setups
        that haven't reached Stage 4 yet are unaffected and keep being tracked
        normally once live ticks resume."""
        now = datetime.now(IST)
        if now.time() < _SESSION_OPEN:
            return  # pre-market -- nothing has traded yet today, nothing to replay
        if not self._feeder_token:
            self._clog.warning(
                "LiquidityTrap[%s]: no Upstox feeder token available -- skipping intraday "
                "warmup, will build up setup state live from here instead.",
                self._underlying,
            )
            return
        key = _UPSTOX_INDEX_KEYS.get(self._underlying.upper(), f"NSE_INDEX|{self._underlying}")
        try:
            from data_layer.historical_candles import fetch_upstox_intraday_1m
            raw_bars = await fetch_upstox_intraday_1m(key, self._feeder_token)
        except Exception as exc:
            self._clog.warning("LiquidityTrap[%s]: intraday warmup fetch failed: %s", self._underlying, exc)
            return
        if not raw_bars:
            self._clog.warning("LiquidityTrap[%s]: intraday warmup -- 0 bars returned (API empty or key mismatch).",
                                self._underlying)
            return

        self._warming_up = True
        try:
            # CRITICAL: set _today BEFORE replaying. _index_tick_loop's own
            # "if self._today != today: reset_session()" new-day check runs on
            # the very first LIVE tick it processes -- which happens right after
            # this method returns. self._today is still None at this point
            # (never set anywhere else before the first tick), so without this
            # line that very first live tick would silently wipe out everything
            # just replayed via reset_session(), with zero log trace -- exactly
            # what happened on a real EC2 run before this fix (state locked
            # during warmup, then silently reverted the moment live ticks
            # resumed).
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
                # tick-based BarAccumulator would have built minute-by-minute, with
                # zero changes to that already-validated pure class. Also feeds
                # _acc_trend so today's still-forming trend-tf bar builds up
                # correctly on top of the multi-day seed above.
                for px in (o, h, l, c):
                    closed_1m = self._acc_1m.on_tick(ts, px)
                    self._acc_confirm.on_tick(ts, px)
                    self._acc_ref.on_tick(ts, px)
                    self._acc_trend.on_tick(ts, px)
                    if closed_1m:
                        self._on_bar_close()
                    # 2026-08-22 fix: a position restored from persistence in
                    # start() (self._restore_position(), called before this
                    # warmup task even runs) is a REAL, still-open broker
                    # position -- unlike a stale Stage4 CHoCH (correctly never
                    # chased into a live entry during replay, see this
                    # method's own docstring), an SL/target level crossed on
                    # the REAL underlying's REAL price action earlier today
                    # is a REAL risk-control breach that already happened,
                    # not a signal that goes stale. Before this fix, replay
                    # only ever fed _on_bar_close() (new-setup discovery) --
                    # never re-checked an already-open position's own SL/
                    # target against the replayed price path at all. A
                    # restart after price had crossed SL/target and then
                    # drifted back away from that level by the time live
                    # ticks resumed would leave that position completely
                    # unprotected for the rest of the session (live checks
                    # only ever compare the CURRENT tick against the level,
                    # never "was this level crossed at any point since
                    # entry"). Deliberately NOT gated by self._warming_up --
                    # that flag only suppresses NEW entries off stale
                    # signals; an already-open real position's own exit
                    # check must run regardless, exactly like the live
                    # per-tick check it's reusing verbatim (no
                    # reimplementation), so a genuinely-breached SL/target
                    # gets closed for real instead of riding to EOD.
                    self._check_exit_and_scale_in(px)
                replayed += 1
            active = sum(1 for s in self._setups if not s.dead)
            self._clog.info(
                "LiquidityTrap[%s]: intraday warmup complete -- %d 1m bars replayed, "
                "%d setups tracked (%d still active), position=%s, day_done=%s.",
                self._underlying, replayed, len(self._setups), active,
                "OPEN" if self._position else "flat", self._day_done,
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

    # ── spot ticks -> bars -> pipeline + live exit/add-on checks ────────────────

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
                    "LiquidityTrap[%s]: REJECTED tick with implausible date %s (real date %s) -- ignoring.",
                    self._underlying, today, real_today,
                )
                continue
            if self._today != today:
                self.reset_session()
                self._today = today

            closed_1m = self._acc_1m.on_tick(ev.timestamp, ev.ltp)
            self._acc_confirm.on_tick(ev.timestamp, ev.ltp)
            self._acc_ref.on_tick(ev.timestamp, ev.ltp)
            self._acc_trend.on_tick(ev.timestamp, ev.ltp)
            if closed_1m:
                self._on_bar_close()

            if self._position is not None:
                self._check_exit_and_scale_in(ev.ltp)

    # ── pipeline (Stages 1-4): re-scan CLOSED bars on every new 1m close ────────
    # Multi-ref (2026-08-21): re-derives the FULL day's setup list fresh every
    # call (matches this module's own "never behaviorally drift from the
    # backtest" discipline), reconciles against self._setups to find genuinely
    # NEW setups, then advances every still-pending setup through Stage 2/3/4
    # independently. See module docstring for the full mechanic.

    def _on_bar_close(self) -> None:
        if self._day_done:
            return
        bars_ref = self._acc_ref.bars   # closed only, per the validated backtest
        bars_confirm = self._acc_confirm.bars
        bars_1m = self._acc_1m.bars
        bars_trend = self._acc_trend.bars

        # ── Stage 1: find genuinely NEW setups ──────────────────────────────
        if bars_ref:
            all_found = find_all_setups(bars_ref)
            known = {(s.ref_idx, s.direction, s.locked_idx) for s in self._setups}
            new_ones = [f for f in all_found if (f.ref_idx, f.direction, f.locked_idx) not in known]
            for f in new_ones:
                self._setups.append(_LiveSetup(f.ref_idx, f.direction, f.locked_idx))
            active_n = sum(1 for s in self._setups if not s.dead)
            if new_ones:
                for f in new_ones:
                    self._clog.info(
                        "STAGE1 NEW setup dir=%s ref=[%.2f,%.2f] locked_by_candle_idx=%d (active setups=%d)",
                        f.direction, bars_ref[f.ref_idx].low, bars_ref[f.ref_idx].high, f.locked_idx, active_n,
                    )
            elif len(bars_ref) != self._ref_watch_count:
                ref = bars_ref[-1]
                self._clog.info(
                    "REF candle [%s] high=%.2f low=%.2f -- watching for new setups (active=%d)",
                    ref.ts.strftime("%H:%M"), ref.high, ref.low, active_n,
                )
            self._ref_watch_count = len(bars_ref)

        # ── Stage 2: SL-hit, per still-pending setup ────────────────────────
        for s in self._setups:
            if s.dead or s.sl_hit_ts is not None:
                continue
            ts = find_sl_hit(bars_ref, s.ref_idx, s.locked_idx, s.direction)
            if ts is not None:
                s.sl_hit_ts = ts
                self._clog.info("STAGE2 SL-hit @ %s (dir=%s ref=[%.2f,%.2f])",
                                ts.strftime("%H:%M"), s.direction,
                                bars_ref[s.ref_idx].low, bars_ref[s.ref_idx].high)

        # ── Stage 3: 5m/confirm-tf single-fixed-reference confirmation ──────
        for s in self._setups:
            if s.dead or s.sl_hit_ts is None or s.confirm_ts is not None:
                continue
            bars_confirm_since = [b for b in bars_confirm if b.ts >= s.sl_hit_ts]
            res = find_5m_confirmation(bars_confirm_since, s.direction)
            if res is not None:
                s.confirm_ts, s.sweep_extreme = res
                self._clog.info("STAGE3 confirmed @ %s sweep_extreme=%.2f (dir=%s)",
                                s.confirm_ts.strftime("%H:%M"), s.sweep_extreme, s.direction)

        # ── Stage 4: 1m CHoCH -> entry (or filtered/skipped/stale) ──────────
        for s in self._setups:
            if s.dead or s.confirm_ts is None:
                continue
            bars_1m_since = [b for b in bars_1m if b.ts >= s.confirm_ts]
            res = find_choch_entry(bars_1m_since, s.direction)
            if res is None:
                continue
            entry_ts, entry_price = res
            s.dead = True   # this setup's one CHoCH moment is consumed either way

            if self._warming_up:
                # CHoCH already happened earlier today, before this process was
                # even watching -- entering NOW would mean paying a live fill
                # price against an hours-stale spot reference. No retroactive
                # entry; this setup's one attempt is simply already gone, same
                # as a real trader who wasn't looking when it printed. Other
                # still-pending setups are unaffected.
                self._clog.info(
                    "STAGE4 CHoCH already fired @ %s price=%.2f dir=%s before this session "
                    "started watching -- setup consumed, no retroactive entry.",
                    entry_ts.strftime("%H:%M"), entry_price, s.direction,
                )
                continue

            if self._trend_filter_enabled:
                trend = compute_trend(bars_trend, self._trend_sma_len)
                if trend is None:
                    self._clog.info(
                        "STAGE4 CHoCH @ %s price=%.2f dir=%s -- skipped, not enough %dm trend "
                        "history yet", entry_ts.strftime("%H:%M"), entry_price, s.direction, self._trend_tf_min,
                    )
                    continue
                wants = "BULL" if trend == "UP" else "BEAR"
                if s.direction != wants:
                    self._clog.info(
                        "STAGE4 CHoCH @ %s price=%.2f dir=%s -- filtered out, against %dm trend (%s)",
                        entry_ts.strftime("%H:%M"), entry_price, s.direction, self._trend_tf_min, trend,
                    )
                    continue

            if self._position is None:
                self._clog.info("STAGE4 CHoCH entry @ %s price=%.2f dir=%s",
                                entry_ts.strftime("%H:%M"), entry_price, s.direction)
                self._try_enter(entry_ts, entry_price, s.direction, s.sweep_extreme)
            else:
                pos_dir = "BULL" if self._position["direction"] == 1 else "BEAR"
                if s.direction == pos_dir:
                    self._clog.info(
                        "STAGE4 CHoCH @ %s price=%.2f dir=%s -- same direction as the running "
                        "position, ignored", entry_ts.strftime("%H:%M"), entry_price, s.direction,
                    )
                else:
                    self._clog.info(
                        "STAGE4 CHoCH @ %s price=%.2f dir=%s -- opposite of the running position, "
                        "but only one position at a time (skip-if-blocked, not flip) -- missed",
                        entry_ts.strftime("%H:%M"), entry_price, s.direction,
                    )

    def _try_enter(self, entry_ts: datetime, entry_price: float, bias: str, sweep_extreme: float) -> None:
        if self._day_done or self._position is not None:
            return
        now = datetime.now(IST)
        if now.time() >= self._squareoff_time:
            self._clog.info("CHoCH fired but skipped (past squareoff time)")
            self._day_done = True
            return
        if self._cooldown_until is not None and now < self._cooldown_until:
            return

        direction = 1 if bias == "BULL" else -1
        side = "CE" if direction == 1 else "PE"
        sl, target = compute_sl_target(direction, entry_price, sweep_extreme, rr=self._rr)

        atm = round(entry_price / self._strike_step) * self._strike_step
        offset = self._itm_offset_pts
        strike = (atm - offset) if side == "CE" else (atm + offset)
        strike = round(strike / self._strike_step) * self._strike_step

        option_ltp = self._live_ltp.get((strike, side))
        if option_ltp is None:
            self._clog.info("%s CHoCH fired (strike=%s) but no live option LTP yet -- skipping this entry",
                            side, int(strike))
            return

        # Multi-ref: NOT setting self._day_done here -- multiple sequential
        # trades per day are expected (one setup at a time, gated only by
        # "only one position open at once" in _on_bar_close's Stage 4, not by
        # a day-level trade cap). self._day_done is now reserved solely for
        # "past squareoff time" (set above).
        qty_unit = self._lot_size * self._lot_multiplier
        self._event_counter += 1
        eid = f"{self._underlying}_{side}{int(strike)}_ENTRY_{self._event_counter}"
        expiry = self._resolve_expiry()
        self._position = dict(
            side=side, direction=direction, strike=strike, entry_price=option_ltp,
            entry_spot=entry_price, sl_spot=sl, target_spot=target,
            lots=self._lots_initial, qty_unit=qty_unit, add_on_done=False,
            zone_bars_since_entry=[], entry_ts=datetime.now(IST), expiry=expiry,
            _entry_event_id=eid,
        )
        self._persist_position()
        logger.info(
            "LiquidityTrap[%s]: ENTER BUY %s %d lots=%d entry=%.2f sl_spot=%.2f target_spot=%.2f "
            "event_id=%s", self._underlying, side, strike, self._lots_initial, option_ltp, sl, target, eid,
        )
        self._clog.info("ENTER BUY %s %d lots=%d entry=%.2f sl_spot=%.2f target_spot=%.2f event_id=%s",
                        side, strike, self._lots_initial, option_ltp, sl, target, eid)
        remark = f"{side} ENTERED @{option_ltp:.2f} strike={int(strike)} sl_spot={sl:.2f} target_spot={target:.2f}"
        self._recent_remarks.appendleft({"ts": datetime.now(IST).isoformat(), "side": side,
                                         "level": "entry", "text": remark})
        order_ev = LiquidityTrapOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id, action="BUY",
            underlying=self._underlying, option_type=side, strike=int(strike), expiry=expiry,
            quantity=self._lots_initial * qty_unit, entry_price=option_ltp,
            sl_price=sl, target_price=target, reason="liquidity_trap_choch", event_id=eid,
            product_type=self._product_type,
        )
        if self._bus is not None:
            asyncio.create_task(self._bus.publish(Topic.LIQUIDITY_TRAP_ORDER_REQUEST, order_ev))

    def _resolve_expiry(self):
        from data_layer.instrument_registry import REGISTRY
        return REGISTRY.get_active_expiry_strict(self._underlying, self._today or datetime.now(IST).date())

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

    # ── Stage 6 (scale-in) + exit checks, live tick-by-tick ─────────────────────

    def _check_exit_and_scale_in(self, spot_ltp: float) -> None:
        pos = self._position
        if pos is None or pos.get("_closing"):
            return
        direction = pos["direction"]
        sl, target = pos["sl_spot"], pos["target_spot"]
        option_ltp = self._live_ltp.get((pos["strike"], pos["side"]), pos["entry_price"])

        if direction == 1:
            if spot_ltp <= sl:
                self._exit(reason="sl_hit", exit_price=option_ltp)
                return
            if spot_ltp >= target:
                self._exit(reason="target_hit", exit_price=option_ltp)
                return
        else:
            if spot_ltp >= sl:
                self._exit(reason="sl_hit", exit_price=option_ltp)
                return
            if spot_ltp <= target:
                self._exit(reason="target_hit", exit_price=option_ltp)
                return

        # Independent safety backstop, option-PREMIUM terms -- same hard Rs/lot
        # cap constant every other option-buyer strategy in this codebase uses.
        risk_floor = pos["entry_price"] - (self._hard_risk_rs_per_lot / pos["qty_unit"])
        if option_ltp <= risk_floor:
            self._exit(reason=f"hard_risk_cap@{risk_floor:.2f}", exit_price=option_ltp)
            return

        if self._scale_in_enabled and not pos["add_on_done"]:
            self._try_scale_in(spot_ltp)

    def _try_scale_in(self, spot_ltp: float) -> None:
        pos = self._position
        entry_ts = pos["entry_ts"]
        # _acc_1m.all_bars() only ever contains bars built from real, already-
        # arrived ticks (closed bars + the one currently forming) -- never
        # "future" data, so no extra now()-based filter is needed here.
        zone_bars = [b for b in self._acc_1m.all_bars() if b.ts.date() == self._today and b.ts >= entry_ts]
        res = find_scale_in_level(zone_bars, pos["direction"])
        if res is None:
            return
        add_on_level, _lock_ts = res
        hit = (spot_ltp <= add_on_level) if pos["direction"] == 1 else (spot_ltp >= add_on_level)
        if not hit:
            return
        option_ltp = self._live_ltp.get((pos["strike"], pos["side"]), pos["entry_price"])
        pos["add_on_done"] = True   # mark immediately -- never re-attempt even if this fill aborts
        pos["lots"] += self._lots_initial
        self._persist_position()
        self._event_counter += 1
        eid = f"{self._underlying}_{pos['side']}{int(pos['strike'])}_ADDON_{self._event_counter}"
        logger.info(
            "LiquidityTrap[%s]: SCALE-IN ADD-ON %s %d +%d lots (now %d) @ zone_level=%.2f event_id=%s",
            self._underlying, pos["side"], pos["strike"], self._lots_initial, pos["lots"], add_on_level, eid,
        )
        self._clog.info("SCALE-IN ADD-ON %s %d +%d lots (now %d) @ zone_level=%.2f event_id=%s",
                        pos["side"], pos["strike"], self._lots_initial, pos["lots"], add_on_level, eid)
        order_ev = LiquidityTrapOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id, action="BUY",
            underlying=self._underlying, option_type=pos["side"], strike=int(pos["strike"]),
            expiry=pos["expiry"], quantity=self._lots_initial * pos["qty_unit"], entry_price=option_ltp,
            sl_price=pos["sl_spot"], target_price=pos["target_spot"], reason="liquidity_trap_scale_in",
            event_id=eid, is_add_on=True, product_type=self._product_type,
        )
        if self._bus is not None:
            asyncio.create_task(self._bus.publish(Topic.LIQUIDITY_TRAP_ORDER_REQUEST, order_ev))

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

    # ── entry / exit ─────────────────────────────────────────────────────────

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
            order_ev = LiquidityTrapOrderEvent(
                client_id=self._client_id, binding_id=self._binding_id, action="SELL",
                underlying=self._underlying, option_type=pos["side"], strike=int(pos["strike"]),
                expiry=pos["expiry"], quantity=pos["lots"] * pos["qty_unit"], entry_price=pos["entry_price"],
                sl_price=pos["sl_spot"], target_price=pos["target_spot"],
                exit_price=exit_price, reason=reason, event_id=eid,
                product_type=self._product_type, entry_ts=pos.get("entry_ts"),
            )
            logger.info(
                "LiquidityTrap[%s]: SELL %s %d lots=%d reason=%s exit=%.2f (awaiting broker confirmation, "
                "event_id=%s)", self._underlying, pos["side"], pos["strike"], pos["lots"], reason, exit_price, eid,
            )
            waiter = asyncio.Event()
            self._fill_waiters[eid] = waiter
            try:
                if self._bus is not None:
                    await self._bus.publish(Topic.LIQUIDITY_TRAP_ORDER_REQUEST, order_ev)
                try:
                    await asyncio.wait_for(waiter.wait(), timeout=_EXIT_CONFIRM_TIMEOUT_SEC)
                except asyncio.TimeoutError:
                    logger.critical(
                        "LiquidityTrap[%s]: EXIT %s%d fill NOT CONFIRMED within %.0fs (event_id=%s reason=%s) "
                        "-- leg stays OPEN; will retry on a later tick/EOD pass.",
                        self._underlying, pos["side"], pos["strike"], _EXIT_CONFIRM_TIMEOUT_SEC, eid, reason,
                    )
                    self._clog.info("EXIT %s%d fill NOT CONFIRMED within %.0fs event_id=%s reason=%s -- retrying later",
                                    pos["side"], pos["strike"], _EXIT_CONFIRM_TIMEOUT_SEC, eid, reason)
                    pos["_closing"] = False
                    return
            finally:
                self._fill_waiters.pop(eid, None)

            fill = self._fill_results.pop(eid, None)
            if fill is not None and getattr(fill, "exit_failed", False):
                logger.critical(
                    "LiquidityTrap[%s]: EXIT %s%d ABORTED by bridge (broker unavailable, event_id=%s reason=%s) "
                    "-- leg stays OPEN; will retry on a later tick/EOD pass.",
                    self._underlying, pos["side"], pos["strike"], eid, reason,
                )
                pos["_closing"] = False
                return

            final_price = fill.fill_price if (fill is not None and fill.fill_price > 0) else exit_price
            pnl = (final_price - pos["entry_price"]) * pos["lots"] * pos["qty_unit"]
            logger.info(
                "LiquidityTrap[%s]: CLOSED %s%d lots=%d reason=%s exit=%.2f pnl=%.2f (event_id=%s).",
                self._underlying, pos["side"], pos["strike"], pos["lots"], reason, final_price, pnl, eid,
            )
            self._clog.info("CLOSED %s%d lots=%d reason=%s exit=%.2f pnl=%.2f", pos["side"], pos["strike"],
                            pos["lots"], reason, final_price, pnl)
            self._recent_remarks.appendleft({"ts": datetime.now(IST).isoformat(), "side": pos["side"],
                                             "level": "exit", "text": f"CLOSED @{final_price:.2f} reason={reason} pnl={pnl:.2f}"})
            self._position = None
            self._persist_position()
        except Exception:
            logger.exception("LiquidityTrap[%s]: _square_off error.", self._underlying)
            pos["_closing"] = False

    # ── fills ────────────────────────────────────────────────────────────────

    async def _fill_loop(self) -> None:
        q = self._loop_queues.get(Topic.LIQUIDITY_TRAP_ORDER_FILL)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if not isinstance(ev, LiquidityTrapFillEvent):
                continue
            if ev.client_id != self._client_id or ev.binding_id != self._binding_id \
                    or ev.underlying != self._underlying:
                continue
            try:
                self._on_fill(ev)
            except Exception:
                logger.exception("LiquidityTrap[%s]: _on_fill error (recovered).", self._underlying)

    def _on_fill(self, fill: LiquidityTrapFillEvent) -> None:
        if fill.action == "BUY":
            if self._position is None:
                return
            if not fill.is_add_on:
                if self._position.get("_entry_event_id") != fill.event_id:
                    return
                if fill.entry_aborted:
                    logger.critical(
                        "LiquidityTrap[%s]: ENTRY ABORTED (broker unavailable/gate closed, event_id=%s) "
                        "-- discarding optimistic position.", self._underlying, fill.event_id,
                    )
                    self._clog.info("ENTRY ABORTED event_id=%s -- discarding position", fill.event_id)
                    self._position = None
                    self._persist_position()
                    return
                # 2026-08-22 fix: `requested` must be THIS fill's own originally-
                # requested quantity (fill.qty, echoed back verbatim from the
                # order this specific fill answers -- see LiquidityTrapFillEvent's
                # own docstring: "qty: the REQUESTED quantity"), NOT re-derived
                # from the CURRENT self._position["lots"]. A scale-in add-on can
                # legitimately bump pos["lots"] (_try_scale_in) BEFORE this
                # original entry's own fill event arrives (real broker round-trip
                # takes time; scale-in fires off live ticks independently). Using
                # the current (already-inflated) lots as "requested" made a
                # genuinely FULL fill for the original 2-lot entry look partial
                # against the post-scale-in 4-lot total, wrongly shrinking
                # pos["lots"] back down and silently discarding the scale-in
                # add-on from tracked state while the broker still held the full
                # quantity -- software position size diverging from the real one.
                filled_qty = fill.filled_qty or fill.qty
                requested = fill.qty
                if 0 < filled_qty < requested:
                    logger.critical(
                        "LiquidityTrap[%s]: ENTRY %s%d PARTIAL FILL (event_id=%s): requested %d, filled %d.",
                        self._underlying, self._position["side"], self._position["strike"],
                        fill.event_id, requested, filled_qty,
                    )
                    # A genuine partial fill on the ORIGINAL entry must not
                    # discard a scale-in add-on that already happened in the
                    # meantime (pos["add_on_done"]) -- add its lots back on
                    # top of the original fill's own reconciled lots, instead
                    # of overwriting the total with just the original's share.
                    original_lots = max(1, filled_qty // self._position["qty_unit"])
                    addon_lots = self._lots_initial if self._position.get("add_on_done") else 0
                    self._position["lots"] = original_lots + addon_lots
                    self._persist_position()
                return
            # add-on fill
            if fill.entry_aborted or fill.routing_failed:
                logger.critical(
                    "LiquidityTrap[%s]: SCALE-IN ADD-ON FAILED (event_id=%s) -- reverting to original "
                    "%d lots; original entry is UNTOUCHED.", self._underlying, fill.event_id, self._lots_initial,
                )
                self._clog.info("SCALE-IN ADD-ON FAILED event_id=%s -- reverting lots", fill.event_id)
                self._position["lots"] = self._lots_initial
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
            d = {k: v for k, v in self._position.items() if not k.startswith("_") and k != "zone_bars_since_entry"}
            d["entry_ts"] = self._position["entry_ts"].isoformat() if self._position.get("entry_ts") else None
            d["expiry"] = self._position["expiry"].isoformat() if self._position.get("expiry") else None
            position_store.save(self._persist_key, {"leg": d}, product_type=self._product_type)
        else:
            position_store.clear(self._persist_key)

    def _restore_position(self) -> None:
        data = position_store.load(self._persist_key)
        if not data or not data.get("leg"):
            return
        d = dict(data["leg"])
        if d.get("entry_ts"):
            d["entry_ts"] = datetime.fromisoformat(d["entry_ts"])
        if d.get("expiry"):
            d["expiry"] = date.fromisoformat(d["expiry"])
        d["zone_bars_since_entry"] = []
        self._position = d
        logger.info("LiquidityTrap[%s]: restored open position from store (%s %d lots=%d).",
                    self._underlying, d.get("side"), d.get("strike", 0), d.get("lots", 0))

    # ── monitoring / UI ──────────────────────────────────────────────────────

    def monitoring_state(self) -> dict:
        bars_ref = self._acc_ref.bars
        active = [s for s in self._setups if not s.dead]
        # Top-level bias/sl_hit/confirmed/ref_* mirror whichever ACTIVE setup is
        # furthest along the pipeline (confirmed > sl_hit > locked-only), so the
        # existing dashboard panel (built for the old single-setup shape) still
        # shows something sensible without needing its own rewrite; the full
        # per-setup detail is in `setups` below for a future richer panel.
        lead = None
        for s in sorted(active, key=lambda s: (s.confirm_ts is not None, s.sl_hit_ts is not None), reverse=True):
            lead = s
            break
        ref_bar = None
        if lead is not None and lead.ref_idx < len(bars_ref):
            ref_bar = bars_ref[lead.ref_idx]
        elif bars_ref:
            ref_bar = bars_ref[-1]
        trend = compute_trend(self._acc_trend.bars, self._trend_sma_len) if self._trend_filter_enabled else None
        return dict(
            underlying=self._underlying, client_id=self._client_id, binding_id=self._binding_id,
            bias=(lead.direction if lead else None),
            sl_hit=lead.sl_hit_ts.isoformat() if (lead and lead.sl_hit_ts) else None,
            confirmed=lead.confirm_ts.isoformat() if (lead and lead.confirm_ts) else None,
            day_done=self._day_done,
            ref_ts=ref_bar.ts.isoformat() if ref_bar else None,
            ref_high=ref_bar.high if ref_bar else None,
            ref_low=ref_bar.low if ref_bar else None,
            trend=trend,
            active_setup_count=len(active),
            setups=[dict(
                direction=s.direction,
                ref_high=bars_ref[s.ref_idx].high if s.ref_idx < len(bars_ref) else None,
                ref_low=bars_ref[s.ref_idx].low if s.ref_idx < len(bars_ref) else None,
                sl_hit=s.sl_hit_ts.isoformat() if s.sl_hit_ts else None,
                confirmed=s.confirm_ts.isoformat() if s.confirm_ts else None,
            ) for s in active],
            position=(dict(self._position) if self._position else None),
            recent_remarks=list(self._recent_remarks),
        )
