"""
strategies/liquidity_trap/engine.py — LiquidityTrapStrategy.

Live per-(client,binding,underlying) book driving strategies/liquidity_trap/
detector.py's pipeline. Re-scans GROWING per-day bar lists on every new bar
close (not an incremental/streaming state machine) -- exactly mirrors
scripts/liquidity_trap_backtest.py, the real-data-validated source of truth,
so this can never behaviorally drift from what was actually backtested.

SL/Target are SPOT-INDEX levels (see strategies/liquidity_trap/__init__.py's
own docstring for the full rationale, same honest design choice as
strategies/liquidity_sweep/): checked every spot tick, MORE responsive than
the validated backtest's own bar-close-only checks, never less. The
option's own live LTP is simply the fill price whenever a spot-level
entry/exit/add-on condition fires.

Not yet deployed even in paper mode as of this build -- built and unit-
tested first, per this codebase's established discipline (see CLAUDE.md).
"""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import date, datetime, time
from typing import Deque, Dict, Optional

from config.global_config import IST, Topic
from data_layer.base_feeder import IndexTick, OptionTick
from data_layer import position_store
from strategies.core.base_book import AbstractStrategyBook
from strategies.liquidity_trap.detector import (
    Bar, BarAccumulator,
    find_ref_and_bias, find_sl_hit, find_5m_confirmation,
    find_choch_entry, compute_sl_target, find_scale_in_level,
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

# Standalone (per this strategy's own zero-shared-runtime mandate -- not
# imported from strategies/d1_trap_option/book.py's own _upstox_key_for,
# same reasoning already applied to detector.py's swing/pool logic).
_UPSTOX_INDEX_KEYS = {
    "NIFTY": "NSE_INDEX|Nifty 50",
    "SENSEX": "BSE_INDEX|SENSEX",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
}


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
        self._acc_15m = BarAccumulator(timeframe_min=15)
        self._acc_5m = BarAccumulator(timeframe_min=5)
        self._acc_1m = BarAccumulator(timeframe_min=1)
        self._live_ltp: Dict[tuple, float] = {}

        # ── pipeline state (mirrors scripts/liquidity_trap_backtest.py exactly) ──
        self._bias: Optional[str] = None            # "BULL" | "BEAR", locked for the day
        self._ref_idx: Optional[int] = None
        self._lock_idx: Optional[int] = None
        self._sl_hit_ts: Optional[datetime] = None
        self._confirm_ts: Optional[datetime] = None
        self._sweep_extreme: Optional[float] = None
        self._day_done = False                        # one trade attempt per day
        self._ref_watch_count = 0    # len(bars_15m) as of the last REF log line, so we
                                      # only log once per new 15m close, not every 1m tick

        self._position: Optional[dict] = None
        self._cooldown_until: Optional[datetime] = None
        self._event_counter = 0
        self._fill_waiters: Dict[str, asyncio.Event] = {}
        self._fill_results: Dict[str, object] = {}
        self._recent_remarks: Deque[dict] = deque(maxlen=30)

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def reset_session(self) -> None:
        self._today = None
        self._acc_15m = BarAccumulator(timeframe_min=15)
        self._acc_5m = BarAccumulator(timeframe_min=5)
        self._acc_1m = BarAccumulator(timeframe_min=1)
        self._live_ltp = {}
        self._bias = None
        self._ref_idx = None
        self._lock_idx = None
        self._sl_hit_ts = None
        self._confirm_ts = None
        self._sweep_extreme = None
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
        await self._warmup_intraday()
        await self._index_tick_loop()

    async def _warmup_intraday(self) -> None:
        """On a mid-day (re)start, REST-fetch today's real 1-min spot history and
        replay it through the SAME BarAccumulators + _on_bar_close() pipeline live
        ticks use, so the book doesn't sit silently 'waiting for ref candle' for
        hours it already lived through before this process started.

        Order placement (Stage4 CHoCH -> _try_enter) is suppressed during replay
        via self._warming_up: a CHoCH that already fired hours ago can't be safely
        entered now at that stale historical price, so _on_bar_close() marks the
        day as done instead of firing a live order off old data (mirrors
        D1TrapOptionBook._warmup_intraday's own established pattern -- state
        catch-up yes, phantom/stale-price live orders no)."""
        now = datetime.now(IST)
        if now.time() < _SESSION_OPEN:
            return  # pre-market -- nothing has traded yet today, nothing to replay
        if not self._feeder_token:
            self._clog.warning(
                "LiquidityTrap[%s]: no Upstox feeder token available -- skipping intraday "
                "warmup, will build up ref/bias state live from here instead.",
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
            # just replayed (bias/ref_idx/sl_hit_ts/confirm_ts + the accumulators
            # themselves) via reset_session(), with zero log trace -- exactly
            # what happened on a real EC2 run before this fix (STAGE1 locked
            # during warmup, then silently reverted to "watching for ref" the
            # moment live ticks resumed).
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
                # zero changes to that already-validated pure class.
                for px in (o, h, l, c):
                    closed_1m = self._acc_1m.on_tick(ts, px)
                    self._acc_5m.on_tick(ts, px)
                    self._acc_15m.on_tick(ts, px)
                    if closed_1m:
                        self._on_bar_close()
                replayed += 1
            self._clog.info(
                "LiquidityTrap[%s]: intraday warmup complete -- %d 1m bars replayed, "
                "bias=%s sl_hit=%s confirmed=%s day_done=%s.",
                self._underlying, replayed, self._bias,
                self._sl_hit_ts.strftime("%H:%M") if self._sl_hit_ts else None,
                self._confirm_ts.strftime("%H:%M") if self._confirm_ts else None,
                self._day_done,
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
            self._acc_5m.on_tick(ev.timestamp, ev.ltp)
            self._acc_15m.on_tick(ev.timestamp, ev.ltp)
            if closed_1m:
                self._on_bar_close()

            if self._position is not None:
                self._check_exit_and_scale_in(ev.ltp)

    # ── pipeline (Stages 1-4): re-scan CLOSED bars on every new 1m close ────────

    def _on_bar_close(self) -> None:
        if self._day_done or self._position is not None:
            return
        bars_15m = self._acc_15m.bars   # closed only, per the validated backtest
        bars_5m = self._acc_5m.bars
        bars_1m = self._acc_1m.bars

        if self._bias is None:
            res = find_ref_and_bias(bars_15m)
            if res is None:
                # Not locked yet -- but log the tentative ref (always the most
                # recently closed 15m candle while unlocked, per find_ref_and_bias'
                # own roll-forward rule) once per new 15m close, so the log/UI
                # aren't silent while we're still watching for a breach.
                if bars_15m and len(bars_15m) != self._ref_watch_count:
                    self._ref_watch_count = len(bars_15m)
                    ref = bars_15m[-1]
                    self._clog.info(
                        "REF candle [%s] high=%.2f low=%.2f -- watching next 15m candle for a breach",
                        ref.ts.strftime("%H:%M"), ref.high, ref.low,
                    )
                return
            self._ref_watch_count = len(bars_15m)
            self._bias, self._ref_idx, self._lock_idx = res
            self._clog.info("STAGE1 bias=%s ref_idx=%d lock_idx=%d ref=[%.2f,%.2f]",
                            self._bias, self._ref_idx, self._lock_idx,
                            bars_15m[self._ref_idx].low, bars_15m[self._ref_idx].high)

        if self._sl_hit_ts is None:
            ts = find_sl_hit(bars_15m, self._ref_idx, self._lock_idx, self._bias)
            if ts is None:
                return
            self._sl_hit_ts = ts
            self._clog.info("STAGE2 SL-hit @ %s", ts.strftime("%H:%M"))

        if self._confirm_ts is None:
            bars_5m_since = [b for b in bars_5m if b.ts >= self._sl_hit_ts]
            res = find_5m_confirmation(bars_5m_since, self._bias)
            if res is None:
                return
            self._confirm_ts, self._sweep_extreme = res
            self._clog.info("STAGE3 confirmed @ %s sweep_extreme=%.2f",
                            self._confirm_ts.strftime("%H:%M"), self._sweep_extreme)

        bars_1m_since = [b for b in bars_1m if b.ts >= self._confirm_ts]
        res = find_choch_entry(bars_1m_since, self._bias)
        if res is None:
            return
        entry_ts, entry_price = res
        if self._warming_up:
            # CHoCH already happened earlier today, before this process was even
            # watching -- entering NOW would mean paying a live fill price against
            # an hours-stale spot reference. No retroactive entry; today's one
            # attempt is simply already gone, same as a real trader who wasn't
            # looking when it printed.
            self._clog.info(
                "STAGE4 CHoCH already fired @ %s price=%.2f before this session started "
                "watching -- missed for today (no stale-price retroactive entry).",
                entry_ts.strftime("%H:%M"), entry_price,
            )
            self._day_done = True
            return
        self._clog.info("STAGE4 CHoCH entry @ %s price=%.2f", entry_ts.strftime("%H:%M"), entry_price)
        self._try_enter(entry_ts, entry_price)

    def _try_enter(self, entry_ts: datetime, entry_price: float) -> None:
        if self._day_done or self._position is not None:
            return
        now = datetime.now(IST)
        if now.time() >= self._squareoff_time:
            self._clog.info("CHoCH fired but skipped (past squareoff time)")
            self._day_done = True
            return
        if self._cooldown_until is not None and now < self._cooldown_until:
            return

        direction = 1 if self._bias == "BULL" else -1
        side = "CE" if direction == 1 else "PE"
        sl, target = compute_sl_target(direction, entry_price, self._sweep_extreme, rr=self._rr)

        atm = round(entry_price / self._strike_step) * self._strike_step
        offset = self._itm_offset_pts
        strike = (atm - offset) if side == "CE" else (atm + offset)
        strike = round(strike / self._strike_step) * self._strike_step

        option_ltp = self._live_ltp.get((strike, side))
        if option_ltp is None:
            self._clog.info("%s CHoCH fired (strike=%s) but no live option LTP yet -- skipping this entry",
                            side, int(strike))
            return

        self._day_done = True   # one trade attempt per day, whether it fills or not
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
                filled_qty = fill.filled_qty or (self._position["lots"] * self._position["qty_unit"])
                requested = self._position["lots"] * self._position["qty_unit"]
                if 0 < filled_qty < requested:
                    logger.critical(
                        "LiquidityTrap[%s]: ENTRY %s%d PARTIAL FILL (event_id=%s): requested %d, filled %d.",
                        self._underlying, self._position["side"], self._position["strike"],
                        fill.event_id, requested, filled_qty,
                    )
                    self._position["lots"] = max(1, filled_qty // self._position["qty_unit"])
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
        bars_15m = self._acc_15m.bars
        ref_bar = None
        if self._bias is not None and self._ref_idx is not None and self._ref_idx < len(bars_15m):
            ref_bar = bars_15m[self._ref_idx]      # locked ref
        elif bars_15m:
            ref_bar = bars_15m[-1]                 # tentative ref while still watching
        return dict(
            underlying=self._underlying, client_id=self._client_id, binding_id=self._binding_id,
            bias=self._bias, sl_hit=self._sl_hit_ts.isoformat() if self._sl_hit_ts else None,
            confirmed=self._confirm_ts.isoformat() if self._confirm_ts else None,
            day_done=self._day_done,
            ref_ts=ref_bar.ts.isoformat() if ref_bar else None,
            ref_high=ref_bar.high if ref_bar else None,
            ref_low=ref_bar.low if ref_bar else None,
            position=(dict(self._position) if self._position else None),
            recent_remarks=list(self._recent_remarks),
        )
