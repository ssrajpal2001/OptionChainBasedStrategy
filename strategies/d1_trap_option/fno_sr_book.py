"""
strategies/d1_trap_option/fno_sr_book.py — D1TrapFnOSRBook (NEW, 2026-08-09).

Positional FnO stock strategy using the S&R ping-pong entry/exit mechanic
(strategies.d1_trap_option.support_resistance.PositionalSRTracker) instead of
D1TrapOptionBook's (book.py) existing C2/TWEAK zone-confirm entry, per direct
user request the same evening as a next-day go-live decision. Built alongside
scripts/fno_positional_sr_backtest.py -- see that script's module docstring
for the validated config and its explicit limitation (signal validated on
real stock price, NOT real option premium).

Reuses book.py's proven pieces by import, never duplicated: _fetch_bars,
_Bar, _upstox_key_for, _get_expiry, D1TrapOrderEvent, FNO_STOCK_CONFIG/
fno_stock_lot/fno_stock_step. Reuses find_all_bear_zones (LONG)/
find_all_bull_zones (SHORT) with book.py's own zone_lo=min(entry_line,
sweep_low)/zone_hi=max(...) formula -- NOT bear_only_book.py's option-
premium-specific boundary (validated only for 15m/60m intraday HTF, not
daily equity bars).

Mechanic (validated via scripts/fno_positional_sr_backtest.py, 10-stock
2-year signal backtest, daily bars beat 2/3/5-day resampled bars on every
metric -- PF 6.40 vs 3.52/3.35/2.20, so daily is the swing timeframe here,
not something coarser):
  - Daily (D1) bars only, fetched once at startup (200-day warmup, matches
    book.py's own _HTF_WARMUP_DAYS) and refreshed once per day after close
    (~15:30 IST, matches book.py's own _d1_daily_refresh_loop cadence) --
    growing history, NEVER reset (positional zones/position persist across
    days, same as book.py's own reset_session() already does for d1_trap_fno).
  - Zone pool grows incrementally each new D1 bar (find_all_bear_zones/
    find_all_bull_zones with known_ref_ts dedup), 20-day zone-age cutoff
    (matches book.py's _MAX_ZONE_AGE_DAYS).
  - Entry: PositionalSRTracker's S&R ping-pong (LONG off bear-trap zones via
    confirmed R2-breaches-R1 breakout; SHORT off bull-trap zones via the
    mirror R2/S2-breaches-S1 breakdown). Exit: PositionalSRTracker's own
    day-low/day-high TSL ratchet + 10%-of-entry hard cap backstop.
  - Option selection identical to book.py's own d1_trap_fno: 1-ITM CE for
    LONG, 1-ITM PE for SHORT, off the stock's OWN spot price (ATM = round
    (spot/strike_step)*strike_step). D1TrapOrderEvent's entry_price/sl_price/
    tsl_level are SPOT-denominated (matches book.py's _open_position/
    _square_off exactly) -- the execution bridge fills the real option and
    books P&L off the real fill, these are signal/audit levels only.
  - Product NRML, no EOD close -- positional, matches book.py's own
    self._positional=True behavior for strategy_name="d1_trap_fno".

Status: NOT deployed yet -- built 2026-08-09, same evening as a next-day
go-live discussion. Signal backtested (10 stocks, 2 years, real stock
price) but NOT smoke-tested end-to-end against real data the way
D1TrapSRBook (BANKNIFTY) was. Recommend a smoke-test pass before any real
capital, same discipline every other live module got today.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional

from config.global_config import IST, Topic, FNO_STOCK_CONFIG, fno_stock_lot, fno_stock_step
from data_layer.base_feeder import OptionTick
from data_layer.instrument_registry import REGISTRY
from data_layer import position_store
from strategies.core.base_book import AbstractStrategyBook
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones
from strategies.d1_trap_option.book import (
    _Bar, _fetch_bars, _upstox_key_for, _get_expiry, D1TrapOrderEvent,
    _HTF_WARMUP_DAYS, _D1_CLOSE,
)
from strategies.d1_trap_option.support_resistance import PositionalSRTracker

logger = logging.getLogger(__name__)

_MAX_ZONE_AGE_DAYS = 20   # matches book.py's own D1 zone-age constant
_HARD_RISK_PCT_DEFAULT = 0.10
_EXIT_CONFIRM_TIMEOUT_SEC = 15.0


def _zone_dict(z, side: str) -> dict:
    return dict(side=side, zone_lo=min(z.entry_line, z.sweep_low), zone_hi=max(z.entry_line, z.sweep_low),
                lock_ts=z.lock_ts, ref_ts=z.reference_low_ts)


class D1TrapFnOSRBook(AbstractStrategyBook):
    """Per-(client, binding, underlying stock) positional live book."""

    def __init__(
        self,
        bus,
        cfg,
        underlying: str,
        client_id: str,
        binding_id: str,
        lot_multiplier: int = 1,
        feeder_token: str = "",
        itm_offset: int = 1,
        hard_risk_pct: float = _HARD_RISK_PCT_DEFAULT,
        upstox_key: str = "",
        lot_override: int = 0,
        step_override: int = 0,
    ) -> None:
        super().__init__(bus, cfg, underlying, client_id, binding_id)
        self._strategy_name = "d1_trap_fno_sr"
        self._lot_multiplier = max(1, lot_multiplier)
        self._feeder_token = feeder_token
        self._itm_offset = max(1, itm_offset)
        self._hard_risk_pct = hard_risk_pct
        self._upstox_key_override = upstox_key
        self._lot_size = lot_override if lot_override > 0 else fno_stock_lot(underlying)
        self._strike_step = step_override if step_override > 0 else fno_stock_step(underlying)
        self._product_type = "NRML"
        self._persist_key = f"{client_id}_{binding_id}_{underlying}_d1_trap_fno_sr"

        self._daily_bars: List[_Bar] = []
        self._zones_long: List[dict] = []
        self._zones_short: List[dict] = []
        self._known_bear: set = set()
        self._known_bull: set = set()
        self._tracker: Optional[PositionalSRTracker] = None
        self._last_spot: Optional[float] = None
        self._d1_refreshed_for_date: Optional[date] = None
        # Display-only live touch tracking (2026-08-11) -- separate from
        # PositionalSRTracker's own touched_long/touched_short, which only
        # update once daily inside on_bar() (correctly, since the validated
        # S&R entry mechanic is daily-bar-driven). The WAITING->MONITORING
        # state shown in monitoring_zones() shouldn't have to wait for
        # end-of-day just to reflect that live price has already entered a
        # zone -- this set updates on every tick, purely for that display,
        # and never feeds into the tracker's own entry-evaluation logic.
        self._live_touched: set = set()
        self._loaded = False

        self._position: Optional[dict] = None
        self._event_counter = 0
        self._fill_waiters: Dict[str, asyncio.Event] = {}
        self._fill_results: Dict[str, object] = {}
        self._stop_for_day = False
        self._consecutive_entry_rejections = 0

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def reset_session(self) -> None:
        """Positional -- daily bars, zones, and any open position all persist
        across days by design (mirrors book.py's own reset_session() for
        strategy_name="d1_trap_fno", which also keeps self._position).
        Nothing to actually reset here; required by AbstractStrategyBook."""
        pass

    def start(self) -> None:
        super().start()
        self._subscribe(Topic.INDEX_TICK)   # equity spot ticks arrive on this topic too
        self._subscribe(Topic.D1_TRAP_ORDER_FILL)
        self._tasks.append(asyncio.create_task(self._startup_load(), name=f"fnosr_startup_{self._underlying}"))
        self._tasks.append(asyncio.create_task(self._tick_loop(), name=f"fnosr_tick_{self._underlying}"))
        self._tasks.append(asyncio.create_task(self._d1_refresh_loop(), name=f"fnosr_d1refresh_{self._underlying}"))
        self._tasks.append(asyncio.create_task(self._fill_loop(), name=f"fnosr_fill_{self._underlying}"))

    async def _fill_loop(self) -> None:
        from execution_bridge.d1_trap_bridge import D1TrapFillEvent
        q = self._loop_queues.get(Topic.D1_TRAP_ORDER_FILL)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if not isinstance(ev, D1TrapFillEvent):
                continue
            if (ev.client_id != self._client_id or ev.binding_id != self._binding_id
                    or ev.underlying != self._underlying):
                continue
            try:
                self._on_fill(ev)
            except Exception:
                logger.exception("D1TrapFnOSR[%s]: _on_fill error (recovered).", self._underlying)

    def _on_fill(self, fill) -> None:
        eid = getattr(fill, "event_id", "")
        if fill.action == "BUY":
            if getattr(fill, "entry_aborted", False):
                if self._position is not None and self._position.get("_event_id") == eid:
                    self._position = None
                    self._persist_position()
                    logger.critical("D1TrapFnOSR[%s]: ENTRY ABORTED (event_id=%s) -- discarding.",
                                     self._underlying, eid)
                self._consecutive_entry_rejections += 1
                if self._consecutive_entry_rejections >= 3:
                    self._stop_for_day = True
                    logger.critical("D1TrapFnOSR[%s]: STOPPING ENTRIES -- %d consecutive rejections.",
                                     self._underlying, self._consecutive_entry_rejections)
                return
            self._consecutive_entry_rejections = 0
            return
        if fill.action == "SELL":
            if eid:
                self._fill_results[eid] = fill
            waiter = self._fill_waiters.get(eid)
            if waiter is not None:
                try:
                    waiter.set()
                except RuntimeError:
                    pass

    async def _startup_load(self) -> None:
        if not self._feeder_token:
            logger.warning("D1TrapFnOSR[%s]: no feeder token -- idle.", self._underlying)
            self._loaded = True
            return
        try:
            self._restore_position()
            today = datetime.now(IST).date()
            key = self._upstox_key_override or _upstox_key_for(self._underlying)
            start = today - timedelta(days=_HTF_WARMUP_DAYS)
            bars = await asyncio.to_thread(_fetch_bars, key, "day", start, today, self._feeder_token)
            self._daily_bars = bars
            self._rebuild_zones(today)
            self._tracker = PositionalSRTracker(self._zones_long, self._zones_short,
                                                 hard_risk_pct=self._hard_risk_pct)
            if self._daily_bars:
                self._last_spot = self._daily_bars[-1].close
                self._update_live_touch(self._last_spot)
            logger.info("D1TrapFnOSR[%s]: %d D1 bars -> zones(L=%d/S=%d)", self._underlying,
                        len(self._daily_bars), len(self._zones_long), len(self._zones_short))
            self._loaded = True
        except Exception:
            logger.exception("D1TrapFnOSR[%s]: startup load failed.", self._underlying)
            self._loaded = True

    def _rebuild_zones(self, today: date) -> None:
        self._zones_long.clear()
        self._zones_short.clear()
        self._known_bear.clear()
        self._known_bull.clear()
        avail = [b for b in self._daily_bars if b.timestamp.date() < today]
        if len(avail) < 3:
            return
        age_cutoff = (datetime.combine(today, datetime.min.time()).replace(tzinfo=IST)
                      - timedelta(days=_MAX_ZONE_AGE_DAYS))
        for z in find_all_bear_zones(avail):
            if z.reference_low_ts in self._known_bear or z.lock_ts < age_cutoff:
                continue
            self._known_bear.add(z.reference_low_ts)
            self._zones_long.append(_zone_dict(z, "LONG"))
        for z in find_all_bull_zones(avail):
            if z.reference_low_ts in self._known_bull or z.lock_ts < age_cutoff:
                continue
            self._known_bull.add(z.reference_low_ts)
            self._zones_short.append(_zone_dict(z, "SHORT"))

    def _discover_new_zones(self, today: date) -> None:
        age_cutoff = (datetime.combine(today, datetime.min.time()).replace(tzinfo=IST)
                      - timedelta(days=_MAX_ZONE_AGE_DAYS))
        for z in find_all_bear_zones(self._daily_bars, known_ref_ts=self._known_bear):
            if z.reference_low_ts not in self._known_bear and z.lock_ts >= age_cutoff:
                self._known_bear.add(z.reference_low_ts)
                self._zones_long.append(_zone_dict(z, "LONG"))
        for z in find_all_bull_zones(self._daily_bars, known_ref_ts=self._known_bull):
            if z.reference_low_ts not in self._known_bull and z.lock_ts >= age_cutoff:
                self._known_bull.add(z.reference_low_ts)
                self._zones_short.append(_zone_dict(z, "SHORT"))

    # ── live ticks (spot price tracking only -- entries/exits are D1-close-driven) ──

    async def _tick_loop(self) -> None:
        q = self._loop_queues.get(Topic.INDEX_TICK)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if getattr(ev, "symbol", None) == self._underlying and getattr(ev, "ltp", None):
                self._last_spot = ev.ltp
                self._update_live_touch(ev.ltp)

    def _update_live_touch(self, spot: float) -> None:
        """Display-only: mark a zone as touched the instant live price enters
        it, independent of the tracker's own once-daily touch check. Never
        creates/advances an S&R calculator and never gates entry -- purely
        flips WAITING->MONITORING in monitoring_zones()."""
        for z in self._zones_long:
            if z["lock_ts"] not in self._live_touched and spot <= z["zone_hi"]:
                self._live_touched.add(z["lock_ts"])
        for z in self._zones_short:
            if z["lock_ts"] not in self._live_touched and spot >= z["zone_lo"]:
                self._live_touched.add(z["lock_ts"])

    async def _d1_refresh_loop(self) -> None:
        """Once per day after D1 close, fetch today's real daily bar and feed it
        through zone discovery + PositionalSRTracker.on_bar -- mirrors book.py's
        own _d1_daily_refresh_loop cadence exactly (~15:30 IST).

        2026-08-11 bug fix: this used to gate on
        `self._daily_bars[-1].timestamp.date() == today` ("already fetched
        today, skip"). But _startup_load()'s own fetch also ends at
        `today` -- if Upstox's daily-candle endpoint hands back an
        in-progress/current-day entry when queried mid-session (any
        startup/restart before 15:30, which is every restart in practice),
        that guard was ALREADY satisfied the instant the process started,
        permanently blocking the real post-close refresh for that entire
        day. Confirmed live: 3 restarts across 2 trading days, zero "D1
        close bar" log lines ever -- the actual entry-evaluation code
        never ran, not "no signal fired." Fixed to track completion of
        THIS loop's own refresh explicitly (_d1_refreshed_for_date), never
        inferred from what date happens to be in the loaded bar data."""
        while self._running:
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            if not self._loaded or self._tracker is None:
                continue
            now = datetime.now(IST)
            if now.time() < _D1_CLOSE:
                continue
            today = now.date()
            if self._d1_refreshed_for_date == today:
                continue   # this loop already completed today's refresh
            if not self._feeder_token:
                continue
            try:
                key = self._upstox_key_override or _upstox_key_for(self._underlying)
                bars = await asyncio.to_thread(_fetch_bars, key, "day", today, today, self._feeder_token)
                if not bars or bars[-1].timestamp.date() != today:
                    continue
                new_bar = bars[-1]
                if not self._daily_bars or self._daily_bars[-1].timestamp.date() != today:
                    self._daily_bars.append(new_bar)
                else:
                    self._daily_bars[-1] = new_bar   # replace startup's stale/partial same-day bar
                self._discover_new_zones(today)
                if self._last_spot is not None:
                    self._update_live_touch(self._last_spot)   # catch newly-discovered zones already in-range
                ev = self._tracker.on_bar(new_bar)
                if ev is not None:
                    await self._handle_tracker_event(ev, new_bar)
                self._d1_refreshed_for_date = today
                logger.info("D1TrapFnOSR[%s]: D1 close bar %.2f/%.2f/%.2f/%.2f -> zones(L=%d/S=%d)",
                            self._underlying, new_bar.open, new_bar.high, new_bar.low, new_bar.close,
                            len(self._zones_long), len(self._zones_short))
            except Exception:
                logger.exception("D1TrapFnOSR[%s]: D1 refresh failed.", self._underlying)

    async def _handle_tracker_event(self, ev: dict, bar: _Bar) -> None:
        if ev["type"] == "entry":
            await self._open_position(ev)
        elif ev["type"] == "exit":
            await self._square_off(ev["reason"], ev["exit_price"])

    # ── entry / exit ─────────────────────────────────────────────────────────

    async def _open_position(self, ev: dict) -> None:
        if self._stop_for_day:
            logger.warning("D1TrapFnOSR[%s]: skip entry -- stopped for the day.", self._underlying)
            return
        spot = ev["entry_price"]
        direction = ev["side"]
        atm = round(spot / self._strike_step) * self._strike_step
        if direction == "LONG":
            strike = int(atm - self._itm_offset * self._strike_step)
            opt_type = "CE"
        else:
            strike = int(atm + self._itm_offset * self._strike_step)
            opt_type = "PE"

        today = ev["entry_ts"].date() if hasattr(ev["entry_ts"], "date") else datetime.now(IST).date()
        expiry = _get_expiry(self._underlying, today, positional=True)
        if not expiry:
            logger.warning("D1TrapFnOSR[%s]: no active expiry -- cannot enter.", self._underlying)
            return

        qty = self._lot_size * self._lot_multiplier
        self._event_counter += 1
        eid = f"{self._underlying}_ENTRY_{self._event_counter}"
        self._position = dict(
            direction=direction, entry=spot, sl=ev["initial_sl"], tsl_level=ev["initial_sl"],
            option_type=opt_type, strike=strike, expiry=expiry, qty=qty,
            entry_ts=ev["entry_ts"], zone_ts=ev["zone_ts"], _event_id=eid,
        )
        self._persist_position()
        logger.info("D1TrapFnOSR[%s]: ENTER BUY %s %d exp=%s qty=%d spot=%.2f sl=%.2f (event_id=%s)",
                    self._underlying, opt_type, strike, expiry, qty, spot, ev["initial_sl"], eid)

        order_ev = D1TrapOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id, strategy=self._strategy_name,
            direction=direction, action="BUY", quantity=qty, entry_price=spot, sl_price=ev["initial_sl"],
            tsl_level=ev["initial_sl"], trigger_ts=datetime.now(IST), reason="sr_ping_pong_entry",
            underlying=self._underlying, option_type=opt_type, strike=strike, expiry=expiry,
            product_type=self._product_type, event_id=eid,
        )
        if self._bus is not None:
            asyncio.create_task(self._bus.publish(Topic.D1_TRAP_ORDER_REQUEST, order_ev))

    async def _square_off(self, reason: str, exit_spot: float) -> None:
        pos = self._position
        if pos is None or pos.get("_closing"):
            return
        pos["_closing"] = True
        try:
            self._event_counter += 1
            eid = f"{self._underlying}_EXIT_{self._event_counter}"
            order_ev = D1TrapOrderEvent(
                client_id=self._client_id, binding_id=self._binding_id, strategy=self._strategy_name,
                direction=pos["direction"], action="SELL", quantity=pos["qty"], entry_price=pos["entry"],
                sl_price=pos["sl"], tsl_level=pos["tsl_level"], trigger_ts=datetime.now(IST), reason=reason,
                underlying=self._underlying, option_type=pos["option_type"], strike=pos["strike"],
                expiry=pos["expiry"], product_type=self._product_type, exit_price=exit_spot,
                entry_reason="sr_ping_pong_entry", entry_ts=pos.get("entry_ts"), event_id=eid,
            )
            logger.info("D1TrapFnOSR[%s]: SELL %s %d reason=%s spot=%.2f (event_id=%s)",
                        self._underlying, pos["option_type"], pos["strike"], reason, exit_spot, eid)

            waiter = asyncio.Event()
            self._fill_waiters[eid] = waiter
            try:
                if self._bus is not None:
                    await self._bus.publish(Topic.D1_TRAP_ORDER_REQUEST, order_ev)
                try:
                    await asyncio.wait_for(waiter.wait(), timeout=_EXIT_CONFIRM_TIMEOUT_SEC)
                except asyncio.TimeoutError:
                    logger.critical("D1TrapFnOSR[%s]: EXIT fill NOT CONFIRMED within %.0fs (event_id=%s) "
                                     "-- position stays OPEN, will retry.", self._underlying,
                                     _EXIT_CONFIRM_TIMEOUT_SEC, eid)
                    return
            finally:
                self._fill_waiters.pop(eid, None)

            fill = self._fill_results.pop(eid, None)
            if fill is not None and getattr(fill, "exit_failed", False):
                logger.critical("D1TrapFnOSR[%s]: EXIT ABORTED by bridge (event_id=%s) -- position stays OPEN.",
                                 self._underlying, eid)
                return

            self._position = None
            self._persist_position()
            logger.info("D1TrapFnOSR[%s]: SELL %s %d reason=%s CONFIRMED (event_id=%s)",
                        self._underlying, pos["option_type"], pos["strike"], reason, eid)
        finally:
            if self._position is not None:
                pos["_closing"] = False

    async def liquidate(self, reason: str = "kill_switch") -> None:
        if self._position is not None:
            await self._square_off(reason, self._last_spot or self._position["entry"])

    # ── persistence ──────────────────────────────────────────────────────────

    def _persist_position(self) -> None:
        if self._position is not None:
            d = {k: v for k, v in self._position.items() if not k.startswith("_")}
            d["entry_ts"] = self._position["entry_ts"].isoformat() if self._position.get("entry_ts") else None
            d["zone_ts"] = self._position["zone_ts"].isoformat() if self._position.get("zone_ts") else None
            d["expiry"] = self._position["expiry"].isoformat() if self._position.get("expiry") else None
            position_store.save(self._persist_key, {"position": d}, product_type=self._product_type)
        else:
            position_store.clear(self._persist_key)

    def _restore_position(self) -> None:
        data = position_store.load(self._persist_key)
        if not data or not data.get("position"):
            return
        import pandas as pd
        d = dict(data["position"])
        try:
            d["entry_ts"] = pd.Timestamp(d["entry_ts"]).to_pydatetime() if d.get("entry_ts") else datetime.now(IST)
            d["zone_ts"] = pd.Timestamp(d["zone_ts"]).to_pydatetime() if d.get("zone_ts") else None
            d["expiry"] = date.fromisoformat(d["expiry"]) if d.get("expiry") else None
        except Exception:
            logger.exception("D1TrapFnOSR[%s]: failed to parse stored position -- discarding.", self._underlying)
            return
        self._position = d
        logger.info("D1TrapFnOSR[%s]: RESTORED open position -- %s %d@%.2f", self._underlying,
                    d["option_type"], d["strike"], d["entry"])

    # ── status ───────────────────────────────────────────────────────────────

    def status(self) -> dict:
        return dict(
            strategy="d1_trap_fno_sr", underlying=self._underlying, last_spot=self._last_spot,
            position=self._position, zones_long=len(self._zones_long), zones_short=len(self._zones_short),
        )

    def get_state(self) -> dict:
        """Same shape as FnOPositionalBook.get_state() (strategies/fno_positional/book.py)
        so the existing "FnO Positional Positions" admin/client tables can render this book
        too -- 2026-08-12 fix, same class of gap as monitoring_zones() above: those tables
        only ever read the OLD fno_positional module, so this book was invisible there
        despite running and holding real positions.

        Deliberately does NOT track live option premium (entry_ltp/current_ltp/pnl stay
        0.0) -- this book only ever subscribes to spot/INDEX_TICK, never OPTION_TICK, by
        design (SL/TSL are spot-level, not premium-level, matching the D1 zone's own
        spot-based mechanic). Adding live premium tracking would mean new tick-subscription
        code in a running live strategy, not just a read-only display method -- out of
        scope for this fix. current_spot/spot_sl are real and meaningful for this strategy
        specifically (unlike the legacy module, where they're a secondary field alongside
        premium P&L)."""
        pos = self._position
        positions = []
        if pos is not None:
            entry_ts = pos.get("entry_ts")
            positions.append({
                "slot_id":       f"{self._underlying}_{entry_ts.isoformat() if entry_ts else 'open'}",
                "symbol":        self._underlying,
                "direction":     pos.get("option_type", "?"),
                "strike":        pos.get("strike"),
                "expiry_str":    pos["expiry"].isoformat() if pos.get("expiry") else "",
                "lot_size":      self._lot_size,
                "qty":           pos.get("qty"),
                "spot_entry":    pos.get("entry"),
                "spot_sl":       pos.get("sl"),
                "day_t1":        0.0,
                "entry_ltp":     0.0,
                "current_spot":  self._last_spot,
                "current_ltp":   0.0,
                "status":        "CLOSING" if pos.get("_closing") else "OPEN",
                "open_time":     entry_ts.isoformat() if entry_ts else "",
                "close_time":    "",
                "close_reason":  "",
                "t1_alerted":    False,
                "pnl":           0.0,
                "client_id":     self._client_id,
                "binding_id":    self._binding_id,
            })
        return {
            "client_id":     self._client_id,
            "binding_id":    self._binding_id,
            # This book has no reference to its own binding's trading_mode (live/paper is
            # resolved at the execution-bridge layer, not held here) -- "unknown" rather
            # than guessing "live", since the UI badge is a live-trading confidence signal.
            "mode":          "unknown",
            "max_slots":     1,
            "open_count":    len(positions),
            "pending_count": 0,
            "pending":       [],
            "positions":     positions,
        }

    def monitoring_zones(self) -> dict:
        """Same shape as D1TrapOptionBook.monitoring_zones() (book.py) so the
        existing WATCHLIST TRACKER UI (/api/d1trap/zones) can render this book
        too -- 2026-08-11 fix: that endpoint skips any book without this exact
        method (hasattr check), so this class was silently invisible to that
        panel despite running and building zones correctly."""
        spot = self._last_spot
        touched = self._live_touched | ((self._tracker.touched_long | self._tracker.touched_short)
                                         if self._tracker else set())
        zones = []
        for z in self._zones_long + self._zones_short:
            dist = None
            if spot:
                mid = (z["zone_lo"] + z["zone_hi"]) / 2
                dist = round((spot - mid) / mid * 100, 2) if mid else None
            zones.append({
                "direction": z["side"],
                "zone_lo": round(z["zone_lo"], 2),
                "zone_hi": round(z["zone_hi"], 2),
                "state": "MONITORING" if z["lock_ts"] in touched else "WAITING",
                "dist_pct": dist,
                "ref_ts": z["lock_ts"].strftime("%Y-%m-%d") if z.get("lock_ts") else None,
            })
        zones.sort(key=lambda zz: (0 if zz["state"] == "MONITORING" else 1,
                                    abs(zz["dist_pct"]) if zz["dist_pct"] is not None else 999))
        return {
            "underlying": self._underlying,
            "client_id": self._client_id,
            "binding_id": self._binding_id,
            "spot": round(spot, 2) if spot else None,
            "zones": zones[:10],
            "total_zones": len(self._zones_long) + len(self._zones_short),
            "pending": None,
            "position": bool(self._position),
        }
