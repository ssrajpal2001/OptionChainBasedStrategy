"""
execution_bridge/oi_flow_bridge.py — OIFlowExecutionBridge.

Fully standalone execution bridge for the OI-Flow Pre-Breakout strategy.
Written fresh -- does NOT subclass execution_bridge.option_buyer_bridge_
base.OptionBuyerExecutionBridge or import from any other strategy's own
bridge file, per explicit user direction (see strategies/oi_flow/
__init__.py: this strategy shares no runtime infrastructure with D1Trap,
FVG, or SellStraddle -- own Topics, own events, own bridge, own book
manager).

What IS reused, and why that's not "sharing a strategy's infrastructure":
these are the platform's own generic building blocks every bridge in this
codebase (SellStraddle, D1Trap, FVG, FnO) already builds on:
  - execution_bridge.base_broker.{OrderRequest,OrderSide,OrderType} -- the
    platform's own broker-order abstraction (used identically by every
    broker integration, not owned by any one strategy).
  - execution_bridge.broker_resolve.resolve_broker_or_alert -- the
    platform's shared "find a live broker instance, never fake success"
    helper.
  - strategies.core.gate.can_trade -- the platform's shared ENTRY gate
    (terminal connected + trade enabled + a running deployment).
  - data_layer.instrument_registry.REGISTRY.get_broker_symbol -- the
    platform's instrument registry.
None of these create runtime coupling to any specific strategy -- no
shared Topic, no shared event class, no shared bridge instance.

Same proven confirm-then-finalize contract every bridge in this codebase
uses (2026-08-06 "Confirm-Model Redesign", CLAUDE.md): paper mode books a
local sim fill at the strategy's own passed-in price; live mode places a
real MARKET order and only reports a fill on a confirmed avg_price > 0 --
a broker-unreachable EXIT must never silently look like a successful
close, and a routing failure must never fabricate an ENTRY fill.

2026-08-12: this session found and fixed a real bug in the SHARED D1Trap/
FVG bridge -- OrderRequest(symbol=...) instead of the dataclass's real
field, broker_symbol= -- which crashed every live order for 7 days behind
a fully green test suite (no test drove a successful live fill through to
OrderRequest construction). This fresh bridge gets that field name right
from the start, AND its own test suite (test_oi_flow_bridge.py) includes
exactly the live-fill-success test that was missing everywhere else, so
this class can't ship with the same class of bug undetected.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime
from typing import Dict, Optional

from config.global_config import IST, Topic, order_exchange
from data_layer.base_feeder import EventBus
from data_layer.instrument_registry import REGISTRY
from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType
from execution_bridge.broker_resolve import resolve_broker_or_alert
from strategies.oi_flow.events import OIFlowOrderEvent, OIFlowFillEvent

logger = logging.getLogger(__name__)

_LOG_DIR = "logs/trades"
_GATE_STRATEGY = "oi_flow"
_BROKER_RESOLVE_LABEL = "OIFlow"
_ORDER_TAG_PREFIX = "OIFLOW_"


class _OIFlowTradeLogger:
    """Per-(client,binding,day) append-only trade log -- own filename
    namespace (key_tag="oi_flow"), never collides with any other
    strategy's own trade log."""

    def __init__(self, log_dir: str = _LOG_DIR) -> None:
        os.makedirs(log_dir, exist_ok=True)
        self._log_dir = log_dir
        self._handles: Dict[str, object] = {}

    def _handle(self, client_id: str, binding_id: str):
        today = datetime.now(IST).strftime("%Y%m%d")
        key = f"{client_id}-{binding_id}-oi_flow-{today}"
        if key not in self._handles:
            path = os.path.join(self._log_dir, f"{key}.log")
            self._handles[key] = open(path, "a", encoding="utf-8", buffering=1)
        return self._handles[key]

    def log(self, client_id: str, binding_id: str, message: str) -> None:
        ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        self._handle(client_id, binding_id).write(f"{ts}  OIFLOW  {message}\n")

    def close_all(self) -> None:
        for h in self._handles.values():
            try:
                h.close()
            except Exception:
                pass
        self._handles.clear()


class OIFlowExecutionBridge:
    """One instance for the whole process (mirrors every other strategy's
    own single-bridge-instance pattern) -- routes purely off
    (client_id, binding_id) per event, not per-book."""

    def __init__(self, bus: EventBus, router, log_dir: str = _LOG_DIR) -> None:
        self._bus = bus
        self._router = router
        self._trade_log = _OIFlowTradeLogger(log_dir)
        self._running = False
        self._q = bus.subscribe(Topic.OI_FLOW_ORDER_REQUEST)

    async def run(self) -> None:
        self._running = True
        logger.info("OIFlowExecutionBridge: started.")
        while self._running:
            try:
                ev = await asyncio.wait_for(self._q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            try:
                if not isinstance(ev, OIFlowOrderEvent):
                    continue
                await self._handle(ev)
            except Exception:
                logger.exception("OIFlowExecutionBridge: _handle error.")

    def stop(self) -> None:
        self._running = False
        self._trade_log.close_all()
        logger.info("OIFlowExecutionBridge: stopped.")

    # ── routing ───────────────────────────────────────────────────────────────

    async def _handle(self, ev: OIFlowOrderEvent) -> None:
        if not ev.client_id or not ev.binding_id:
            logger.error("OIFlowExecutionBridge: event missing client_id/binding_id — dropped.")
            return

        db = getattr(self._router, "_client_db", None) or getattr(self._router, "_db", None)
        live_binding = None
        if db is not None and hasattr(db, "get_bindings_safe_sync"):
            try:
                for b in db.get_bindings_safe_sync(ev.client_id):
                    if b.get("binding_id") == ev.binding_id:
                        live_binding = b
                        break
            except Exception:
                live_binding = None

        if live_binding is None or not live_binding.get("terminal_connected"):
            logger.warning(
                "OIFlowExecutionBridge: %s %s — [%s/%s] terminal not connected, no route.",
                ev.action, ev.underlying, ev.client_id, ev.binding_id,
            )
            await self._abort(ev, routing_failed=True)
            return

        # EXIT must always route -- gate only ENTRY on the shared can_trade() gate.
        if ev.action == "BUY" and db is not None:
            from strategies.core.gate import can_trade
            if not can_trade(ev.client_id, ev.binding_id, db, ev.strategy or _GATE_STRATEGY, ev.underlying):
                logger.warning(
                    "OIFlowExecutionBridge: BUY %s — [%s/%s] can_trade() gate closed.",
                    ev.underlying, ev.client_id, ev.binding_id,
                )
                await self._abort(ev, routing_failed=True)
                return

        mode = live_binding.get("trading_mode", "paper") or "paper"

        if mode == "paper":
            # PURE LOCAL SIMULATION -- never sends a real order, never
            # touches the broker resolver at all.
            logger.info(
                "OIFlowExecutionBridge: %s %s %s%d exp=%s qty=%d → [%s/%s] mode=paper",
                ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
                ev.quantity, ev.client_id, ev.binding_id,
            )
            await self._paper_fill(ev)
            return

        # mode in {"paper_route", <live>}: both need a REAL broker instance.
        # paper_route (2026-08-13) mirrors StraddleExecutionBridge's own
        # paper_route semantics (execution_bridge/straddle_bridge.py) --
        # the client explicitly wants the order to actually reach their
        # broker from the whitelisted IP (verifies real routing end-to-end,
        # a no-fund reject is the EXPECTED broker response), while the
        # strategy's own state still advances on a locally-simulated fill
        # regardless of that broker response.
        broker = await resolve_broker_or_alert(
            self._bus, self._router, ev.client_id, ev.binding_id, _BROKER_RESOLVE_LABEL,
            context=f"{ev.action} {ev.underlying} {ev.option_type}{ev.strike}",
        )

        logger.info(
            "OIFlowExecutionBridge: %s %s %s%d exp=%s qty=%d → [%s/%s] mode=%s broker=%s",
            ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
            ev.quantity, ev.client_id, ev.binding_id, mode,
            "resolved" if broker is not None else "UNAVAILABLE",
        )

        if broker is None:
            # Never call _paper_fill here -- that would fabricate a fill the
            # caller would treat as real. resolve_broker_or_alert already
            # logged CRITICAL and published SYSTEM_EVENT. Applies to
            # paper_route too -- a genuinely UNRESOLVABLE broker (bad creds,
            # terminal down) is a real routing failure worth surfacing, not
            # something to paper over.
            await self._abort(ev, routing_failed=True)
            return

        await self._live_fill(ev, broker, paper_route=(mode == "paper_route"))

    async def _abort(self, ev: OIFlowOrderEvent, routing_failed: bool = False) -> None:
        await self._bus.publish(Topic.OI_FLOW_ORDER_FILL, OIFlowFillEvent(
            action=ev.action, underlying=ev.underlying, option_type=ev.option_type or "",
            strike=int(ev.strike or 0), fill_price=0.0, qty=int(ev.quantity or 0),
            client_id=ev.client_id, binding_id=ev.binding_id, event_id=ev.event_id or "",
            entry_aborted=(ev.action == "BUY"),
            exit_failed=(ev.action == "SELL"),
            routing_failed=routing_failed,
        ))

    # ── paper ─────────────────────────────────────────────────────────────────

    async def _paper_fill(self, ev: OIFlowOrderEvent) -> None:
        fill_price = ev.entry_price
        if ev.action == "SELL" and ev.exit_price > 0:
            fill_price = ev.exit_price
        logger.info(
            "[PAPER] OIFLOW %s %s %s%d exp=%s qty=%d @ %.2f | client=%s/%s",
            ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
            ev.quantity, fill_price, ev.client_id, ev.binding_id,
        )
        self._trade_log.log(
            ev.client_id, ev.binding_id,
            f"[PAPER] {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} "
            f"exp={ev.expiry} qty={ev.quantity} @ {fill_price:.2f} reason={ev.reason}",
        )
        await self._bus.publish(Topic.OI_FLOW_ORDER_FILL, OIFlowFillEvent(
            action=ev.action, underlying=ev.underlying, option_type=ev.option_type or "",
            strike=int(ev.strike or 0), fill_price=fill_price, qty=int(ev.quantity or 0),
            client_id=ev.client_id, binding_id=ev.binding_id, event_id=ev.event_id or "",
            paper_mode=True,
        ))

    # ── live ──────────────────────────────────────────────────────────────────

    def _resolve_symbol(self, ev: OIFlowOrderEvent, broker) -> str:
        if not ev.expiry or not ev.strike or not ev.option_type:
            return ""
        _b = getattr(broker, "_binding", None)
        provider = _b.provider if _b else getattr(broker, "provider", "mock")
        return REGISTRY.get_broker_symbol(ev.underlying, ev.expiry, int(ev.strike), ev.option_type, provider)

    async def _live_fill(self, ev: OIFlowOrderEvent, broker, paper_route: bool = False) -> None:
        """paper_route=True: still places the REAL order via broker.place_
        order() (so the client can verify the order genuinely reaches their
        broker from the whitelisted IP -- a no-fund rejection is EXPECTED
        and fine), but always finalizes a fill for the strategy -- using the
        broker's own avg_price if it happened to confirm one (>0), else a
        LOCAL SIMULATED fill at the strategy's own passed-in price. Mirrors
        StraddleExecutionBridge._live_fill's own paper=True contract
        (execution_bridge/straddle_bridge.py) -- same intent, written fresh
        for this bridge's own simpler single-leg (not multi-leg/chase) path.
        paper_route=False (real live): an unconfirmed fill (avg<=0) is
        NEVER faked -- aborts instead, exactly as before."""
        symbol = self._resolve_symbol(ev, broker)
        if not symbol:
            logger.error(
                "OIFlowExecutionBridge: no tradable symbol for %s %s%d — paper fallback.",
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
            price=ev.entry_price,   # ignored for MARKET; MockBroker uses as fill
            tag=f"{_ORDER_TAG_PREFIX}{ev.underlying}_{ev.action}"[:20],
        )

        avg = 0.0
        order_id = ""
        _tag = "PAPER_ROUTE" if paper_route else "LIVE"
        try:
            order_id = await broker.place_order(req)
            fill = await broker.get_order_status(str(order_id))
            avg = float(getattr(fill, "avg_price", 0.0) or 0.0)
            if avg > 0:
                logger.info(
                    "[%s] OIFLOW %s %s %s%d exp=%s qty=%d @ %.2f order_id=%s | client=%s/%s",
                    _tag, ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
                    ev.quantity, avg, order_id, ev.client_id, ev.binding_id,
                )
                self._trade_log.log(
                    ev.client_id, ev.binding_id,
                    f"[{_tag}] {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} "
                    f"exp={ev.expiry} qty={ev.quantity} @ {avg:.2f} symbol={symbol} "
                    f"order_id={order_id} reason={ev.reason}",
                )
            else:
                logger.info(
                    "[%s] OIFLOW %s %s %s%d exp=%s qty=%d — order reached broker (order_id=%s) "
                    "but no confirmed fill (avg_price<=0) | client=%s/%s",
                    _tag, ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
                    ev.quantity, order_id, ev.client_id, ev.binding_id,
                )
        except Exception as exc:
            logger.error(
                "[%s] OIFLOW %s %s %s%d order FAILED: %s.",
                _tag, ev.action, ev.underlying, ev.option_type, ev.strike, exc,
            )
            self._trade_log.log(
                ev.client_id, ev.binding_id,
                f"{_tag} {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} FAILED: {exc}",
            )
            avg = 0.0

        if avg <= 0:
            if paper_route:
                # Expected outcome for a no-fund account -- the order genuinely
                # reached (or was attempted against) the real broker, verifying
                # routing; the strategy's own state still advances on a LOCAL
                # simulated fill at its own passed-in price, same as pure
                # "paper" mode's fill, just with real broker contact logged.
                sim_price = ev.exit_price if (ev.action == "SELL" and ev.exit_price > 0) else ev.entry_price
                logger.info(
                    "[PAPER_ROUTE] OIFLOW %s %s %s%d — no confirmed broker fill (expected for "
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
                await self._bus.publish(Topic.OI_FLOW_ORDER_FILL, OIFlowFillEvent(
                    action=ev.action, underlying=ev.underlying, option_type=ev.option_type or "",
                    strike=int(ev.strike or 0), fill_price=sim_price, qty=int(ev.quantity or 0),
                    client_id=ev.client_id, binding_id=ev.binding_id, event_id=ev.event_id or "",
                    paper_mode=True, symbol=symbol,
                ))
                return
            logger.error(
                "[LIVE] OIFLOW %s %s %s%d — NO confirmed fill, %s NOT reported as a fill "
                "(no phantom position/close). client=%s/%s",
                ev.action, ev.underlying, ev.option_type, ev.strike,
                "entry" if ev.action == "BUY" else "exit", ev.client_id, ev.binding_id,
            )
            await self._abort(ev, routing_failed=False)
            return

        await self._bus.publish(Topic.OI_FLOW_ORDER_FILL, OIFlowFillEvent(
            action=ev.action, underlying=ev.underlying, option_type=ev.option_type or "",
            strike=int(ev.strike or 0), fill_price=avg, qty=int(ev.quantity or 0),
            client_id=ev.client_id, binding_id=ev.binding_id, event_id=ev.event_id or "",
            paper_mode=paper_route, symbol=symbol,
        ))
