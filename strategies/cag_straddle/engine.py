"""
strategies/cag_straddle/engine.py — CagStraddleStrategy.

Live per-(client,binding,underlying) book driving strategies/cag_straddle/
detector.py's S&R breach mechanic on BOTH the ATM-area CE and PE premium
charts simultaneously (hence "straddle" in the name -- both sides are
watched at once, even though at most one side is ever actually held long
at a time; see detector.py's own module docstring for the full mechanic).
Built from a real-data-validated backtest --
scripts/nifty_1500_sr_breakout_backtest.py -- refined through several
rounds of direct user review against real minute-by-minute charts. This
engine re-implements that SAME validated mechanic incrementally (bar-by-
bar as live ticks arrive) rather than the backtest's own batch/whole-day
replay, per this codebase's "backtest must drive the real class" discipline
applied in reverse: the live engine must never behaviorally drift from what
was actually validated.

Mechanic summary (full detail in detector.py's own module docstring):
  1. At entry_start (default 15:00 IST), pick the CE strike and PE strike
     (independently) whose LIVE premium is closest to target_premium_rs
     (default Rs100), searched within ATM +/- strike_search_steps*
     strike_step. Each side then gets a FRESH SupportResistanceCalculator
     (strategies/d1_trap_option/support_resistance.py, reused platform
     infra) -- no pre-15:00 history feeds in.
  2. ENTRY: a phase transition from S2_TRACKING/R2_TRACKING back into
     R1_TRACKING ("R2 breaches R1") arms a standing order at the breaching
     bar's own high; the first LATER bar (any bar, not just the next one)
     whose own high exceeds it fills the order.
  3. Once filled, only S1/S2 matter for that side -- SL mirrors entry
     exactly (a bar closing below S1 arms a standing order at that bar's
     own low; the first later bar whose low breaches it exits). This is
     "trailing SL as S1 itself".
  4. Force-exit at force_exit_time (default 15:35 IST) regardless.
  5. An SL exit (not EOD) resumes scanning BOTH sides immediately for the
     next confirmed signal -- multiple sequential trades per day are
     expected, not capped at one.

SL is an OPTION-PREMIUM level (not spot-index) -- the whole S&R read runs
directly on each side's own premium chart, so no spot-to-premium
translation is needed (see events.py's own module docstring for the same
point). A hard Rs/lot risk-cap backstop (same _MAX_RISK_RS_PER_LOT-style
constant every other option-buyer strategy in this codebase uses) runs
alongside the structural SL as a safety net against IV crush/bid-ask
blowouts the S&R read alone wouldn't catch.

Known, honestly-flagged limitation (matching this codebase's own honesty
convention for genuinely-unbuilt pieces rather than silently pretending
otherwise): strike selection at 15:00 depends on this book already having
LIVE ticks for its candidate strikes by then (self._live_premium, built
passively from every Topic.OPTION_TICK for this underlying that arrives --
no dedicated subscription request is made by this book itself). In
practice the platform's shared strike-rebalancer keeps a reasonably wide
ATM-centered band subscribed for every deployed underlying already, so
candidates within +/- strike_search_steps should normally have live data;
if a specific candidate strike genuinely hasn't ticked yet, it's silently
excluded from that side's pick (falls back to whichever candidates DID
tick) rather than blocking the whole day. Similarly, this pass does not
implement a REST-based intraday warmup/replay (unlike e.g. Liquidity Trap's
_warmup_intraday) -- a restart during the narrow 15:00-15:35 window will
lose in-progress standing-order/tracker state and simply resume watching
fresh from whatever live ticks arrive after the restart; an already-OPEN
position still survives via the existing position_store persistence, same
as every other strategy.
"""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import date, datetime, time as dtime
from typing import Deque, Dict, Optional

from config.global_config import IST, Topic
from data_layer.base_feeder import IndexTick, OptionTick
from data_layer import position_store
from strategies.core.base_book import AbstractStrategyBook
from strategies.cag_straddle.detector import Bar, BarAccumulator, SideTracker, pick_strike
from strategies.cag_straddle.events import CagStraddleOrderEvent, CagStraddleFillEvent

logger = logging.getLogger(__name__)

_DEFAULT_TARGET_PREMIUM_RS = 100.0
_DEFAULT_STRIKE_SEARCH_STEPS = 6
_DEFAULT_HARD_RISK_RS_PER_LOT = 2000.0
_EXIT_CONFIRM_TIMEOUT_SEC = 15.0
_SIDES = ("CE", "PE")


def _make_strategy_logger(underlying: str, client_id: str = "", binding_id: str = "") -> logging.Logger:
    from utils.logging_utils import make_strategy_logger
    tag = f"{underlying}" + (f"_{client_id}_{binding_id}" if client_id and binding_id else "")
    date_str = datetime.now(IST).strftime("%Y%m%d")
    return make_strategy_logger(f"cagstraddle_{tag}_{date_str}", propagate=False)


class CagStraddleStrategy(AbstractStrategyBook):
    """One instance per (client, binding, underlying)."""

    def __init__(
        self,
        bus,
        cfg,
        underlying: str,
        client_id: str,
        binding_id: str,
        lot_multiplier: int = 1,
        target_premium_rs: float = _DEFAULT_TARGET_PREMIUM_RS,
        strike_search_steps: int = _DEFAULT_STRIKE_SEARCH_STEPS,
        hard_risk_rs_per_lot: float = _DEFAULT_HARD_RISK_RS_PER_LOT,
        product_type: str = "MIS",
        entry_start: str = "15:00",
        force_exit_time: str = "15:35",
    ) -> None:
        super().__init__(bus, cfg, underlying, client_id, binding_id)
        self._strategy_name = "cag_straddle"
        self._lot_multiplier = max(1, lot_multiplier)
        self._target_premium_rs = float(target_premium_rs)
        self._strike_search_steps = max(1, int(strike_search_steps))
        self._hard_risk_rs_per_lot = float(hard_risk_rs_per_lot)
        self._product_type = product_type

        def _parse_time(s: str, default: dtime) -> dtime:
            try:
                h, m = str(s or "").split(":")
                return dtime(int(h), int(m))
            except Exception:
                return default

        self._entry_start = _parse_time(entry_start, dtime(15, 0))
        self._force_exit_time = _parse_time(force_exit_time, dtime(15, 35))

        self._lot_size = (cfg.exchange.lot_sizes.get(underlying, 75) if cfg else 75)
        self._strike_step = float(cfg.exchange.strike_steps.get(underlying, 50) if cfg else 50)
        self._persist_key = f"{client_id}_{binding_id}_{underlying}_cag_straddle"
        self._clog = _make_strategy_logger(underlying, client_id, binding_id)

        self._today: Optional[date] = None
        self._spot: float = 0.0
        self._entry_window_started = False
        self._day_done = False

        self._selected_strikes: Dict[str, Optional[int]] = {"CE": None, "PE": None}
        self._trackers: Dict[str, SideTracker] = {}
        self._bar_accs: Dict[str, BarAccumulator] = {}
        self._live_premium: Dict[tuple, float] = {}   # (strike, side) -> ltp, same-expiry ticks only
        # 2026-09-08 CRITICAL FIX, real incident: this book used to accept
        # ANY OptionTick matching (strike, side) regardless of expiry --
        # harmless on a normal day (only one expiry's contracts tick near
        # ATM), but today SellStraddle independently subscribed NEXT WEEK's
        # expiry window (its own 0DTE-avoidance feature) while today (a
        # Tuesday) was itself NIFTY's current-week 0DTE expiry -- both
        # expiries' ticks for the same strike numbers flowed on the shared
        # bus simultaneously. CAG entered CE23800 at 83.15 (next-week
        # premium) but its SL check kept ingesting ANY CE23800 tick,
        # including today's near-worthless 0DTE one (7.75) -- an 91% "drop"
        # in 111ms, triggering hard_risk_cap on a contract it never
        # actually held. Fixed: this book now resolves and pins its OWN
        # expiry once per day (self._day_expiry, set the moment self._today
        # is known) and filters every OptionTick against it -- never trusts
        # the ambient shared-feed expiry mix, per direct user spec ("when
        # ever we require expiry date for any strategy it should get its
        # own expiry value").
        self._day_expiry: Optional[date] = None

        self._position: Optional[dict] = None
        self._event_counter = 0
        self._fill_waiters: Dict[str, asyncio.Event] = {}
        self._fill_results: Dict[str, object] = {}
        self._recent_remarks: Deque[dict] = deque(maxlen=30)

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def reset_session(self) -> None:
        self._today = None
        self._entry_window_started = False
        self._day_done = False
        self._selected_strikes = {"CE": None, "PE": None}
        self._trackers = {}
        self._bar_accs = {}
        self._live_premium = {}
        self._day_expiry = None
        self._recent_remarks.clear()

    def start(self) -> None:
        super().start()
        self._subscribe(Topic.INDEX_TICK)
        self._subscribe(Topic.OPTION_TICK)
        self._subscribe(Topic.CAG_STRADDLE_ORDER_FILL)
        self._restore_position()
        self._tasks.append(asyncio.create_task(self._index_tick_loop(), name=f"cagstraddle_idx_{self._underlying}"))
        self._tasks.append(asyncio.create_task(self._option_tick_loop(), name=f"cagstraddle_opt_{self._underlying}"))
        self._tasks.append(asyncio.create_task(self._fill_loop(), name=f"cagstraddle_fill_{self._underlying}"))
        self._tasks.append(asyncio.create_task(self._eod_loop(), name=f"cagstraddle_eod_{self._underlying}"))

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

    # ── spot ticks -> ATM + entry-window trigger ────────────────────────────────

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
            try:
                if not isinstance(ev, IndexTick) or not self._is_own_underlying_tick(ev.symbol):
                    continue
                if getattr(ev, "source", "spot") != "spot":
                    continue
                today = ev.timestamp.date() if hasattr(ev, "timestamp") else datetime.now(IST).date()
                if self._today != today:
                    self.reset_session()
                    self._today = today
                    self._day_expiry = self._resolve_expiry()
                self._spot = ev.ltp
                if not self._entry_window_started and not self._day_done \
                        and ev.timestamp.time() >= self._entry_start:
                    self._start_entry_window()
            except Exception:
                logger.exception("CagStraddle[%s]: _index_tick_loop iteration error (recovered).", self._underlying)

    def _start_entry_window(self) -> None:
        self._entry_window_started = True
        atm = round(self._spot / self._strike_step) * self._strike_step
        for side in _SIDES:
            candidates = {}
            for k in range(-self._strike_search_steps, self._strike_search_steps + 1):
                strike = int(atm + k * self._strike_step)
                ltp = self._live_premium.get((strike, side))
                if ltp is not None and ltp > 0:
                    candidates[strike] = ltp
            strike = pick_strike(candidates, self._target_premium_rs)
            if strike is None:
                self._clog.warning(
                    "%s: no live premium data for any candidate strike near ATM=%d at entry-window "
                    "start -- this side will not be tracked today.", side, int(atm),
                )
                continue
            self._selected_strikes[side] = strike
            self._trackers[side] = SideTracker()
            self._bar_accs[side] = BarAccumulator()
            self._clog.info(
                "%s: selected strike %d (premium=%.2f, target=Rs%.0f) near ATM=%d -- tracking fresh from now.",
                side, strike, candidates[strike], self._target_premium_rs, int(atm),
            )
        logger.info(
            "CagStraddle[%s]: entry window started -- ATM=%d CE=%s PE=%s",
            self._underlying, int(atm), self._selected_strikes["CE"], self._selected_strikes["PE"],
        )

    # ── option ticks -> live premium + per-side bars -> S&R pipeline ────────────

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
            try:
                if not isinstance(ev, OptionTick) or ev.underlying != self._underlying or not ev.ltp:
                    continue
                # 2026-09-08 fix (see self._day_expiry's own comment in __init__):
                # never trust a strike/side match alone -- multiple expiries can
                # tick the same strike numbers simultaneously on the shared feed.
                # Ticks arriving before self._day_expiry is resolved (very first
                # moments of the day) are held back rather than risking a wrong
                # expiry's price seeding self._live_premium.
                if self._day_expiry is None or ev.expiry != self._day_expiry:
                    continue
                strike = int(ev.strike)
                side = str(ev.option_type).upper()
                self._live_premium[(strike, side)] = ev.ltp

                if not self._entry_window_started or side not in _SIDES:
                    continue
                if self._selected_strikes.get(side) != strike:
                    continue   # not the strike this side is tracking -- ignore

                ts = ev.timestamp if hasattr(ev, "timestamp") else datetime.now(IST)
                closed_bar = self._bar_accs[side].on_tick(ts, ev.ltp)
                if closed_bar is not None:
                    self._on_bar_close(side, closed_bar)

                if self._position is not None and self._position["side"] == side \
                        and not self._position.get("_closing"):
                    risk_floor = self._position["entry_price"] - \
                        (self._hard_risk_rs_per_lot / self._position["qty_unit"])
                    if ev.ltp <= risk_floor:
                        self._exit(reason=f"hard_risk_cap@{risk_floor:.2f}", exit_price=ev.ltp)
            except Exception:
                logger.exception("CagStraddle[%s]: _option_tick_loop iteration error (recovered).", self._underlying)

    def _on_bar_close(self, side: str, bar: Bar) -> None:
        if self._day_done:
            return
        tracker = self._trackers.get(side)
        if tracker is None:
            return
        info = tracker.on_bar(bar)

        if self._position is None:
            fill = tracker.check_entry_fill(bar, info["r1_breach_event"])
            if fill is not None:
                self._try_enter(side, fill, bar.ts)
        elif self._position["side"] == side and not self._position.get("_closing"):
            sl_fill = tracker.check_sl_fill(bar, info["s1_before"])
            if sl_fill is not None:
                self._exit(reason=f"sl_s1_breach@{sl_fill:.2f}", exit_price=sl_fill)

    def _try_enter(self, side: str, entry_price: float, entry_ts: datetime) -> None:
        if self._day_done or self._position is not None:
            return
        now = datetime.now(IST)
        if now.time() >= self._force_exit_time:
            self._day_done = True
            return
        strike = self._selected_strikes.get(side)
        if strike is None:
            return

        qty_unit = self._lot_size * self._lot_multiplier
        self._event_counter += 1
        eid = f"{self._underlying}_{side}{int(strike)}_ENTRY_{self._event_counter}"
        expiry = self._day_expiry or self._resolve_expiry()
        self._position = dict(
            side=side, strike=strike, entry_price=entry_price, entry_ts=entry_ts,
            qty_unit=qty_unit, expiry=expiry, _entry_event_id=eid,
        )
        self._persist_position()
        logger.info(
            "CagStraddle[%s]: ENTER BUY %s %d entry=%.2f event_id=%s",
            self._underlying, side, strike, entry_price, eid,
        )
        self._clog.info("ENTER BUY %s %d entry=%.2f event_id=%s", side, strike, entry_price, eid)
        self._recent_remarks.appendleft({
            "ts": datetime.now(IST).isoformat(), "side": side, "level": "entry",
            "text": f"{side} ENTERED @{entry_price:.2f} strike={int(strike)}",
        })
        order_ev = CagStraddleOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id, action="BUY",
            underlying=self._underlying, option_type=side, strike=int(strike), expiry=expiry,
            quantity=qty_unit, entry_price=entry_price, sl_price=0.0,
            reason="cag_straddle_r1_breach", event_id=eid, entry_ts=entry_ts,
            product_type=self._product_type,
        )
        if self._bus is not None:
            asyncio.create_task(self._bus.publish(Topic.CAG_STRADDLE_ORDER_REQUEST, order_ev))

    def _resolve_expiry(self):
        from data_layer.instrument_registry import REGISTRY
        return REGISTRY.get_active_expiry_strict(self._underlying, self._today or datetime.now(IST).date())

    async def _eod_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            now_t = datetime.now(IST).time()
            if now_t >= self._force_exit_time:
                self._day_done = True
                if self._position is not None and not self._position.get("_closing"):
                    side = self._position["side"]
                    strike = self._position["strike"]
                    exit_price = self._live_premium.get((strike, side), self._position["entry_price"])
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
            order_ev = CagStraddleOrderEvent(
                client_id=self._client_id, binding_id=self._binding_id, action="SELL",
                underlying=self._underlying, option_type=pos["side"], strike=int(pos["strike"]),
                expiry=pos["expiry"], quantity=pos["qty_unit"], entry_price=pos["entry_price"],
                sl_price=0.0, exit_price=exit_price, reason=reason, event_id=eid,
                product_type=self._product_type, entry_ts=pos.get("entry_ts"),
            )
            logger.info(
                "CagStraddle[%s]: SELL %s %d reason=%s exit=%.2f (awaiting broker confirmation, event_id=%s)",
                self._underlying, pos["side"], pos["strike"], reason, exit_price, eid,
            )
            waiter = asyncio.Event()
            self._fill_waiters[eid] = waiter
            try:
                if self._bus is not None:
                    await self._bus.publish(Topic.CAG_STRADDLE_ORDER_REQUEST, order_ev)
                try:
                    await asyncio.wait_for(waiter.wait(), timeout=_EXIT_CONFIRM_TIMEOUT_SEC)
                except asyncio.TimeoutError:
                    logger.critical(
                        "CagStraddle[%s]: EXIT %s%d fill NOT CONFIRMED within %.0fs (event_id=%s reason=%s) "
                        "-- leg stays OPEN; will retry on a later tick/EOD pass.",
                        self._underlying, pos["side"], pos["strike"], _EXIT_CONFIRM_TIMEOUT_SEC, eid, reason,
                    )
                    pos["_closing"] = False
                    return
            finally:
                self._fill_waiters.pop(eid, None)

            fill = self._fill_results.pop(eid, None)
            if fill is not None and getattr(fill, "exit_failed", False):
                logger.critical(
                    "CagStraddle[%s]: EXIT %s%d ABORTED by bridge (broker unavailable, event_id=%s reason=%s) "
                    "-- leg stays OPEN; will retry on a later tick/EOD pass.",
                    self._underlying, pos["side"], pos["strike"], eid, reason,
                )
                pos["_closing"] = False
                return

            final_price = fill.fill_price if (fill is not None and fill.fill_price > 0) else exit_price
            pnl = (final_price - pos["entry_price"]) * pos["qty_unit"]
            logger.info(
                "CagStraddle[%s]: CLOSED %s%d reason=%s exit=%.2f pnl=%.2f (event_id=%s).",
                self._underlying, pos["side"], pos["strike"], reason, final_price, pnl, eid,
            )
            self._clog.info("CLOSED %s%d reason=%s exit=%.2f pnl=%.2f", pos["side"], pos["strike"],
                            reason, final_price, pnl)
            self._recent_remarks.appendleft({
                "ts": datetime.now(IST).isoformat(), "side": pos["side"], "level": "exit",
                "text": f"CLOSED @{final_price:.2f} reason={reason} pnl={pnl:.2f}",
            })
            self._position = None
            self._persist_position()
            # A stop-out (not EOD) resumes scanning immediately -- day_done is
            # reserved solely for "past force_exit_time" (set in _eod_loop),
            # per the re-entry-after-SL mechanic (detector.py module docstring
            # point 5). Trackers are untouched -- they keep advancing live.
        except Exception:
            logger.exception("CagStraddle[%s]: _square_off error.", self._underlying)
            pos["_closing"] = False

    # ── fills ────────────────────────────────────────────────────────────────

    async def _fill_loop(self) -> None:
        q = self._loop_queues.get(Topic.CAG_STRADDLE_ORDER_FILL)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if not isinstance(ev, CagStraddleFillEvent):
                continue
            if ev.client_id != self._client_id or ev.binding_id != self._binding_id \
                    or ev.underlying != self._underlying:
                continue
            try:
                self._on_fill(ev)
            except Exception:
                logger.exception("CagStraddle[%s]: _on_fill error (recovered).", self._underlying)

    def _on_fill(self, fill: CagStraddleFillEvent) -> None:
        if fill.action == "BUY":
            if self._position is None:
                return
            if self._position.get("_entry_event_id") != fill.event_id:
                return
            if fill.entry_aborted:
                logger.critical(
                    "CagStraddle[%s]: ENTRY ABORTED (broker unavailable/gate closed, event_id=%s) "
                    "-- discarding optimistic position.", self._underlying, fill.event_id,
                )
                self._clog.info("ENTRY ABORTED event_id=%s -- discarding position", fill.event_id)
                self._position = None
                self._persist_position()
                return
            filled_qty = fill.filled_qty or fill.qty
            requested = fill.qty
            if 0 < filled_qty < requested:
                logger.critical(
                    "CagStraddle[%s]: ENTRY %s%d PARTIAL FILL (event_id=%s): requested %d, filled %d.",
                    self._underlying, self._position["side"], self._position["strike"],
                    fill.event_id, requested, filled_qty,
                )
                self._position["qty_unit"] = filled_qty
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
        self._position = d
        logger.info("CagStraddle[%s]: restored open position from store (%s %d).",
                    self._underlying, d.get("side"), d.get("strike", 0))

    # ── monitoring / UI ──────────────────────────────────────────────────────

    def monitoring_state(self) -> dict:
        sides = {}
        for side in _SIDES:
            t = self._trackers.get(side)
            sides[side] = dict(
                strike=self._selected_strikes.get(side),
                r1=(t.last_r1 if t else None),
                s1=(t.last_s1 if t else None),
                phase=(t.last_phase if t else None),
            )
        position = None
        if self._position:
            position = dict(self._position)
            # 2026-08-28 fix: self._live_premium already updates on every
            # OPTION_TICK (see _on_index_tick/_on_option_tick), it just was
            # never surfaced here -- the dashboard's live LTP/P&L fields had
            # nothing to read, even mid-trade. CAG Straddle only ever goes
            # long, so P&L is always (current - entry) * qty_unit.
            current_price = self._live_premium.get((position["strike"], position["side"]))
            position["current_price"] = current_price
            if current_price is not None:
                position["unrealized_pnl"] = (current_price - position["entry_price"]) * position["qty_unit"]
        return dict(
            underlying=self._underlying, client_id=self._client_id, binding_id=self._binding_id,
            entry_window_started=self._entry_window_started, day_done=self._day_done,
            sides=sides,
            position=position,
            recent_remarks=list(self._recent_remarks),
        )
