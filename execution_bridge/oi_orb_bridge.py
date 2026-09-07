"""
execution_bridge/oi_orb_bridge.py -- OiOrbExecutionBridge.

Fully standalone execution bridge for the OI-Spurt + ORB screener strategy.
Modeled directly on execution_bridge/oi_flow_bridge.py's own paper_route
contract (same confirm-then-finalize shape this codebase always uses) --
written fresh, no inheritance from that or any other strategy's bridge, per
strategies/oi_orb_screener/__init__.py's zero-shared-runtime mandate.

What IS reused (platform infra, not another strategy's logic -- same
reasoning as every other standalone bridge in this codebase):
execution_bridge.base_broker.{OrderRequest,OrderSide,OrderType},
execution_bridge.broker_resolve.resolve_broker_or_alert,
strategies.core.gate.can_trade, data_layer.instrument_registry.REGISTRY.

paper_route (this strategy's ONLY deployed mode this pass, per direct user
instruction 2026-08-24): a real order genuinely reaches the broker via
broker.place_order() -- verifies routing/symbol/lot end-to-end -- but the
strategy's own state always advances on a fill, using the broker's real
avg_price if confirmed (>0), else a LOCAL SIMULATED fill at the strategy's
own passed-in price. Real live mode (avg<=0 aborts instead of faking a
fill) is implemented identically to every other bridge here but not
expected to be used until the SL/target follow-up pass.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime
from typing import Dict, Optional, Tuple

from config.global_config import IST, Topic, order_exchange
from data_layer.base_feeder import EventBus
from data_layer.instrument_registry import REGISTRY
from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType
from execution_bridge.broker_resolve import resolve_broker_or_alert
from strategies.oi_orb_screener.events import OiOrbOrderEvent, OiOrbFillEvent

logger = logging.getLogger(__name__)

_LOG_DIR = "logs/trades"
_GATE_STRATEGY = "oi_orb_screener"
# strategies.core.gate.can_trade() matches a running deployment's OWN
# `underlying` column exactly against whatever underlying is passed in --
# but this strategy's deployment row always stores the sentinel "SCREENER"
# (one row governs the whole multi-stock book; the screener itself decides
# which real stocks to trade each day, not the deployment). Gating on the
# real stock symbol (ev.underlying) here would never match any real
# deployment row and would silently block EVERY entry in production.
_GATE_UNDERLYING = "SCREENER"
_BROKER_RESOLVE_LABEL = "OiOrb"
_ORDER_TAG_PREFIX = "OIORB_"


class _OiOrbTradeLogger:
    """Per-(client,binding,day) append-only trade log -- own filename
    namespace (key_tag="oi_orb"), never collides with any other strategy's
    own trade log."""

    def __init__(self, log_dir: str = _LOG_DIR) -> None:
        os.makedirs(log_dir, exist_ok=True)
        self._log_dir = log_dir
        self._handles: Dict[str, object] = {}

    def _handle(self, client_id: str, binding_id: str):
        today = datetime.now(IST).strftime("%Y%m%d")
        key = f"{client_id}-{binding_id}-oi_orb-{today}"
        if key not in self._handles:
            path = os.path.join(self._log_dir, f"{key}.log")
            self._handles[key] = open(path, "a", encoding="utf-8", buffering=1)
        return self._handles[key]

    def log(self, client_id: str, binding_id: str, message: str) -> None:
        ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        self._handle(client_id, binding_id).write(f"{ts}  OIORB  {message}\n")

    def close_all(self) -> None:
        for h in self._handles.values():
            try:
                h.close()
            except Exception:
                pass
        self._handles.clear()


class OiOrbExecutionBridge:
    """One instance for the whole process -- routes purely off
    (client_id, binding_id) per event, not per-book."""

    def __init__(self, bus: EventBus, router, log_dir: str = _LOG_DIR) -> None:
        self._bus = bus
        self._router = router
        self._trade_log = _OiOrbTradeLogger(log_dir)
        self._running = False
        self._q = bus.subscribe(Topic.OI_ORB_ORDER_REQUEST)
        self._key_tasks: Dict[Tuple[str, str], "asyncio.Task"] = {}

    async def run(self) -> None:
        self._running = True
        logger.info("OiOrbExecutionBridge: started.")
        while self._running:
            try:
                ev = await asyncio.wait_for(self._q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if not isinstance(ev, OiOrbOrderEvent):
                continue
            key = (ev.client_id or "", ev.binding_id or "")
            prev_task = self._key_tasks.get(key)
            task = asyncio.create_task(self._handle_chained(ev, prev_task, key))
            self._key_tasks[key] = task

    async def _handle_chained(self, ev: OiOrbOrderEvent, prev_task: Optional["asyncio.Task"],
                               key: Tuple[str, str]) -> None:
        if prev_task is not None and not prev_task.done():
            try:
                await prev_task
            except Exception:
                pass
        try:
            await self._handle(ev)
        except Exception:
            logger.exception("OiOrbExecutionBridge: _handle error for %s %s.",
                              ev.action, ev.underlying)
        finally:
            if self._key_tasks.get(key) is asyncio.current_task():
                self._key_tasks.pop(key, None)

    def stop(self) -> None:
        self._running = False
        self._trade_log.close_all()
        logger.info("OiOrbExecutionBridge: stopped.")

    # ── routing ───────────────────────────────────────────────────────────────

    async def _handle(self, ev: OiOrbOrderEvent) -> None:
        if not ev.client_id or not ev.binding_id:
            logger.error("OiOrbExecutionBridge: event missing client_id/binding_id — dropped.")
            return

        db = getattr(self._router, "_client_db", None) or getattr(self._router, "_db", None)
        live_binding = None
        if db is not None and hasattr(db, "get_bindings_safe_sync"):
            try:
                for b in await asyncio.to_thread(db.get_bindings_safe_sync, ev.client_id):
                    if b.get("binding_id") == ev.binding_id:
                        live_binding = b
                        break
            except Exception:
                live_binding = None

        if live_binding is None or not live_binding.get("terminal_connected"):
            logger.warning(
                "OiOrbExecutionBridge: %s %s — [%s/%s] terminal not connected, no route.",
                ev.action, ev.underlying, ev.client_id, ev.binding_id,
            )
            await self._abort(ev, routing_failed=True)
            return

        # EXIT must always route -- gate only ENTRY on the shared can_trade() gate.
        if ev.action == "BUY" and db is not None:
            from strategies.core.gate import can_trade
            if not can_trade(ev.client_id, ev.binding_id, db, ev.strategy or _GATE_STRATEGY, _GATE_UNDERLYING):
                logger.warning(
                    "OiOrbExecutionBridge: BUY %s — [%s/%s] can_trade() gate closed.",
                    ev.underlying, ev.client_id, ev.binding_id,
                )
                await self._abort(ev, routing_failed=True)
                return

        mode = live_binding.get("trading_mode", "paper") or "paper"

        if mode == "paper":
            logger.info(
                "OiOrbExecutionBridge: %s %s %s%d exp=%s qty=%d → [%s/%s] mode=paper",
                ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
                ev.quantity, ev.client_id, ev.binding_id,
            )
            await self._paper_fill(ev)
            return

        # mode in {"paper_route", <live>}: both need a REAL broker instance.
        broker = await resolve_broker_or_alert(
            self._bus, self._router, ev.client_id, ev.binding_id, _BROKER_RESOLVE_LABEL,
            context=f"{ev.action} {ev.underlying} {ev.option_type}{ev.strike}",
        )

        logger.info(
            "OiOrbExecutionBridge: %s %s %s%d exp=%s qty=%d → [%s/%s] mode=%s broker=%s",
            ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
            ev.quantity, ev.client_id, ev.binding_id, mode,
            "resolved" if broker is not None else "UNAVAILABLE",
        )

        if broker is None:
            await self._abort(ev, routing_failed=True)
            return

        await self._live_fill(ev, broker, paper_route=(mode == "paper_route"))

    async def _abort(self, ev: OiOrbOrderEvent, routing_failed: bool = False) -> None:
        await self._bus.publish(Topic.OI_ORB_ORDER_FILL, OiOrbFillEvent(
            action=ev.action, underlying=ev.underlying, option_type=ev.option_type or "",
            strike=int(ev.strike or 0), fill_price=0.0, qty=int(ev.quantity or 0),
            client_id=ev.client_id, binding_id=ev.binding_id, event_id=ev.event_id or "",
            entry_aborted=(ev.action == "BUY"),
            exit_failed=(ev.action == "SELL"),
            routing_failed=routing_failed,
        ))

    # ── dashboard trade history ──────────────────────────────────────────────

    def _record_history(self, ev: OiOrbOrderEvent, fill_price: float) -> None:
        """Record a CLOSED trade to the dashboard's History tab -- own
        implementation, no import from any other strategy's bridge, same
        reasoning as oi_flow_bridge.py's own _record_history()
        (data_layer.trade_history is platform infra, not another
        strategy's logic)."""
        if ev.action != "SELL":
            return
        try:
            from data_layer import trade_history as _th
            pnl = round((fill_price - ev.entry_price) * ev.quantity, 2)
            _entry_ts = ev.entry_ts
            _th.record(
                ev.client_id, ev.strategy or _GATE_STRATEGY, ev.underlying,
                ev.entry_price, fill_price, ev.reason, pnl,
                binding_id=ev.binding_id,
                legs=[{
                    "side": ev.option_type,
                    "strike": ev.strike,
                    "entry": ev.entry_price,
                    "exit": fill_price,
                    "pnl": pnl,
                    "entry_reason": "oi_orb_screener",
                    "entry_ts": _entry_ts.isoformat() if hasattr(_entry_ts, "isoformat") else _entry_ts,
                    "exit_ts": datetime.now(IST).isoformat(),
                }],
            )
        except Exception:
            logger.exception("OiOrbExecutionBridge: trade_history.record failed (non-fatal).")

    # ── paper ─────────────────────────────────────────────────────────────────

    async def _paper_fill(self, ev: OiOrbOrderEvent) -> None:
        fill_price = ev.entry_price
        if ev.action == "SELL" and ev.exit_price > 0:
            fill_price = ev.exit_price
        logger.info(
            "[PAPER] OIORB %s %s %s%d exp=%s qty=%d @ %.2f | client=%s/%s",
            ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
            ev.quantity, fill_price, ev.client_id, ev.binding_id,
        )
        self._trade_log.log(
            ev.client_id, ev.binding_id,
            f"[PAPER] {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} "
            f"exp={ev.expiry} qty={ev.quantity} @ {fill_price:.2f} reason={ev.reason}",
        )
        self._record_history(ev, fill_price)
        await self._bus.publish(Topic.OI_ORB_ORDER_FILL, OiOrbFillEvent(
            action=ev.action, underlying=ev.underlying, option_type=ev.option_type or "",
            strike=int(ev.strike or 0), fill_price=fill_price, qty=int(ev.quantity or 0),
            client_id=ev.client_id, binding_id=ev.binding_id, event_id=ev.event_id or "",
            paper_mode=True,
        ))

    # ── live ──────────────────────────────────────────────────────────────────

    def _resolve_symbol(self, ev: OiOrbOrderEvent, broker) -> str:
        if not ev.expiry or not ev.strike or not ev.option_type:
            return ""
        # 2026-09-07 CRITICAL FIX, real incident: every broker class
        # (UpstoxBroker, ZerodhaBroker, FyersBroker, AngelBroker, DhanBroker,
        # DeltaBroker) stores its BrokerBinding as self._b, NEVER self._binding
        # -- this always returned None, so `provider` always fell through to
        # the "mock" default, and get_broker_symbol()'s if/elif chain has no
        # "mock" branch -- it silently returned the bare InternalSymbol
        # canonical string (e.g. "ICICIPRULI:29SEP26:485:PE") as if it were a
        # real broker symbol. For SellStraddle/CAG (index-only underlyings)
        # this was invisibly masked -- UpstoxBroker._instrument_map is
        # pre-populated at auth time with exactly these canonical strings as
        # KEYS for monitored_indices only (execution_router.py's own
        # build_instrument_map() injection loop), so the wrong "mock" symbol
        # still happened to translate correctly by luck. OI-ORB trades
        # individual F&O STOCKS, never in monitored_indices -- the same bug
        # had no lucky fallback there, so Upstox rejected every single order
        # with "UDAPI100011: Invalid Instrument key" (confirmed via the real
        # 2026-09-07 order logs for SOLARINDS/ICICIPRULI/LTM), and since that
        # rejection happens before an order even exists, none of them showed
        # up in the user's broker app at all -- unlike SellStraddle's real
        # (margin-rejected but genuinely placed) orders. Same latent bug also
        # existed in straddle_bridge.py/straddle_hedge_bridge.py/
        # cag_straddle_bridge.py -- fixed in all four the same way.
        #
        # 2026-09-07 CRITICAL FIX #2, real LIVE incident: the "_b" fix above
        # broke ZerodhaBroker, which stores its binding as self._binding
        # instead (opposite convention from Upstox/Fyers/Angel/Dhan/Delta) --
        # a real SA5770/Zerodha LIVE SellStraddle position could not exit
        # for minutes, retrying every ~6s against a broken canonical-string
        # symbol. Check BOTH attribute names.
        _b = getattr(broker, "_binding", None) or getattr(broker, "_b", None)
        provider = _b.provider if _b else getattr(broker, "provider", "mock")
        return REGISTRY.get_broker_symbol(ev.underlying, ev.expiry, int(ev.strike), ev.option_type, provider)

    async def _live_fill(self, ev: OiOrbOrderEvent, broker, paper_route: bool = False) -> None:
        """paper_route=True: real order attempted via broker.place_order()
        (verifies routing/symbol/lot end-to-end from the whitelisted IP --
        a no-fund rejection is EXPECTED and fine), always finalizes a fill
        for the strategy -- broker's own avg_price if confirmed (>0), else
        a LOCAL SIMULATED fill at the strategy's own passed-in price.
        paper_route=False (real live): an unconfirmed fill (avg<=0) is
        NEVER faked -- aborts instead."""
        symbol = self._resolve_symbol(ev, broker)
        if not symbol:
            logger.error(
                "OiOrbExecutionBridge: no tradable symbol for %s %s%d — paper fallback.",
                ev.underlying, ev.option_type, ev.strike,
            )
            await self._paper_fill(ev)
            return

        side = OrderSide.BUY if ev.action == "BUY" else OrderSide.SELL
        exchange = order_exchange(ev.underlying)
        req = OrderRequest(
            broker_symbol=symbol,
            exchange=exchange,
            side=side,
            qty=ev.quantity,
            order_type=OrderType.MARKET,
            product=ev.product_type or "MIS",
            # 2026-09-07 CRITICAL FIX, real incident: Upstox's real API
            # rejects a MARKET order that carries a non-zero price
            # ("UDAPI1040: Price not required") -- the stale "MockBroker
            # uses as fill" comment this replaces was wrong for this code
            # path: _live_fill is ONLY ever called with a REAL broker
            # (paper_route/live both need one) -- MockBroker is never
            # reached here, paper mode bypasses this entirely via
            # _paper_fill. straddle_bridge.py never sets price at all
            # (defaults to 0.0) for exactly this reason -- match it.
            price=0.0,
            tag=f"{_ORDER_TAG_PREFIX}{ev.underlying}_{ev.action}"[:20],
        )

        avg = 0.0
        filled_qty = 0
        order_id = ""
        _tag = "PAPER_ROUTE" if paper_route else "LIVE"
        try:
            order_id = await broker.place_order(req)
            fill = await broker.get_order_status(str(order_id))
            avg = float(getattr(fill, "avg_price", 0.0) or 0.0)
            filled_qty = int(getattr(fill, "qty", 0) or 0)
            if avg > 0:
                if 0 < filled_qty < ev.quantity:
                    logger.critical(
                        "[%s] OIORB %s %s %s%d — PARTIAL FILL: requested %d, filled %d @ %.2f "
                        "order_id=%s | client=%s/%s",
                        _tag, ev.action, ev.underlying, ev.option_type, ev.strike,
                        ev.quantity, filled_qty, avg, order_id, ev.client_id, ev.binding_id,
                    )
                else:
                    filled_qty = ev.quantity
                logger.info(
                    "[%s] OIORB %s %s %s%d exp=%s qty=%d filled=%d @ %.2f order_id=%s | client=%s/%s",
                    _tag, ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
                    ev.quantity, filled_qty, avg, order_id, ev.client_id, ev.binding_id,
                )
                self._trade_log.log(
                    ev.client_id, ev.binding_id,
                    f"[{_tag}] {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} "
                    f"exp={ev.expiry} qty={ev.quantity} filled={filled_qty} @ {avg:.2f} symbol={symbol} "
                    f"order_id={order_id} reason={ev.reason}",
                )
            else:
                logger.info(
                    "[%s] OIORB %s %s %s%d exp=%s qty=%d — order reached broker (order_id=%s) "
                    "but no confirmed fill (avg_price<=0) | client=%s/%s",
                    _tag, ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
                    ev.quantity, order_id, ev.client_id, ev.binding_id,
                )
        except Exception as exc:
            logger.error(
                "[%s] OIORB %s %s %s%d order FAILED: %s.",
                _tag, ev.action, ev.underlying, ev.option_type, ev.strike, exc,
            )
            self._trade_log.log(
                ev.client_id, ev.binding_id,
                f"{_tag} {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} FAILED: {exc}",
            )
            avg = 0.0

        if avg <= 0:
            if paper_route:
                sim_price = ev.exit_price if (ev.action == "SELL" and ev.exit_price > 0) else ev.entry_price
                logger.info(
                    "[PAPER_ROUTE] OIORB %s %s %s%d — no confirmed broker fill (expected for "
                    "no-fund account); booking SIMULATED fill @ %.2f | client=%s/%s",
                    ev.action, ev.underlying, ev.option_type, ev.strike, sim_price,
                    ev.client_id, ev.binding_id,
                )
                self._trade_log.log(
                    ev.client_id, ev.binding_id,
                    f"[PAPER_ROUTE] {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} "
                    f"exp={ev.expiry} qty={ev.quantity} @ {sim_price:.2f} (simulated, real order "
                    f"attempted order_id={order_id or 'none'}) reason={ev.reason}",
                )
                self._record_history(ev, sim_price)
                await self._bus.publish(Topic.OI_ORB_ORDER_FILL, OiOrbFillEvent(
                    action=ev.action, underlying=ev.underlying, option_type=ev.option_type or "",
                    strike=int(ev.strike or 0), fill_price=sim_price, qty=int(ev.quantity or 0),
                    client_id=ev.client_id, binding_id=ev.binding_id, event_id=ev.event_id or "",
                    paper_mode=True, symbol=symbol,
                ))
                return
            logger.error(
                "[LIVE] OIORB %s %s %s%d — NO confirmed fill, %s NOT reported as a fill "
                "(no phantom position/close). client=%s/%s",
                ev.action, ev.underlying, ev.option_type, ev.strike,
                "entry" if ev.action == "BUY" else "exit", ev.client_id, ev.binding_id,
            )
            await self._abort(ev, routing_failed=False)
            return

        self._record_history(ev, avg)
        await self._bus.publish(Topic.OI_ORB_ORDER_FILL, OiOrbFillEvent(
            action=ev.action, underlying=ev.underlying, option_type=ev.option_type or "",
            strike=int(ev.strike or 0), fill_price=avg, qty=int(ev.quantity or 0),
            client_id=ev.client_id, binding_id=ev.binding_id, event_id=ev.event_id or "",
            paper_mode=paper_route, symbol=symbol, filled_qty=filled_qty,
        ))
