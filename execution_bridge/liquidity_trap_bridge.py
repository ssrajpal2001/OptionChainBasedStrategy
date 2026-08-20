"""
execution_bridge/liquidity_trap_bridge.py — LiquidityTrapExecutionBridge.

Fully standalone execution bridge for the Liquidity Trap strategy. Written
fresh -- does NOT subclass execution_bridge.option_buyer_bridge_base.
OptionBuyerExecutionBridge or import from any other strategy's own bridge
file, same standalone mandate as execution_bridge/oi_flow_bridge.py and
execution_bridge/liquidity_sweep_bridge.py (see strategies/liquidity_trap/
__init__.py). Modeled closely on liquidity_sweep_bridge.py (identical
shape: single-leg option BUYER, confirm-then-finalize, a scale-in add-on is
just a second BUY of the same instrument) -- not imported from it, kept as
its own file so no bridge can ever accidentally share mutable state.

What IS reused (platform infra, not another bridge's own logic):
execution_bridge.base_broker.{OrderRequest,OrderSide,OrderType},
execution_bridge.broker_resolve.resolve_broker_or_alert,
strategies.core.gate.can_trade, data_layer.instrument_registry.REGISTRY,
config.global_config.order_exchange.

Same proven confirm-then-finalize contract every bridge in this codebase
uses: paper mode books a local sim fill at the strategy's own passed-in
price; live mode places a real MARKET order and only reports a fill on a
confirmed avg_price > 0.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime
from typing import Dict

from config.global_config import IST, Topic, order_exchange
from data_layer.base_feeder import EventBus
from data_layer.instrument_registry import REGISTRY
from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType
from execution_bridge.broker_resolve import resolve_broker_or_alert
from strategies.liquidity_trap.events import LiquidityTrapOrderEvent, LiquidityTrapFillEvent

logger = logging.getLogger(__name__)

_LOG_DIR = "logs/trades"
_GATE_STRATEGY = "liquidity_trap"
_BROKER_RESOLVE_LABEL = "LiquidityTrap"
_ORDER_TAG_PREFIX = "LIQTRAP_"


class _LiquidityTrapTradeLogger:
    """Per-(client,binding,day) append-only trade log — own filename
    namespace (key_tag="liquidity_trap"), never collides with any other
    strategy's own trade log."""

    def __init__(self, log_dir: str = _LOG_DIR) -> None:
        os.makedirs(log_dir, exist_ok=True)
        self._log_dir = log_dir
        self._handles: Dict[str, object] = {}

    def _handle(self, client_id: str, binding_id: str):
        today = datetime.now(IST).strftime("%Y%m%d")
        key = f"{client_id}-{binding_id}-liquidity_trap-{today}"
        if key not in self._handles:
            path = os.path.join(self._log_dir, f"{key}.log")
            self._handles[key] = open(path, "a", encoding="utf-8", buffering=1)
        return self._handles[key]

    def log(self, client_id: str, binding_id: str, message: str) -> None:
        ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        self._handle(client_id, binding_id).write(f"{ts}  LIQTRAP  {message}\n")

    def close_all(self) -> None:
        for h in self._handles.values():
            try:
                h.close()
            except Exception:
                pass
        self._handles.clear()


class LiquidityTrapExecutionBridge:
    """One instance for the whole process — routes purely off
    (client_id, binding_id) per event, not per-book."""

    def __init__(self, bus: EventBus, router, log_dir: str = _LOG_DIR) -> None:
        self._bus = bus
        self._router = router
        self._trade_log = _LiquidityTrapTradeLogger(log_dir)
        self._running = False
        self._q = bus.subscribe(Topic.LIQUIDITY_TRAP_ORDER_REQUEST)

    async def run(self) -> None:
        self._running = True
        logger.info("LiquidityTrapExecutionBridge: started.")
        while self._running:
            try:
                ev = await asyncio.wait_for(self._q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            try:
                if not isinstance(ev, LiquidityTrapOrderEvent):
                    continue
                await self._handle(ev)
            except Exception:
                logger.exception("LiquidityTrapExecutionBridge: _handle error.")

    def stop(self) -> None:
        self._running = False
        self._trade_log.close_all()
        logger.info("LiquidityTrapExecutionBridge: stopped.")

    # ── routing ───────────────────────────────────────────────────────────────

    async def _handle(self, ev: LiquidityTrapOrderEvent) -> None:
        if not ev.client_id or not ev.binding_id:
            logger.error("LiquidityTrapExecutionBridge: event missing client_id/binding_id — dropped.")
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
                "LiquidityTrapExecutionBridge: %s %s — [%s/%s] terminal not connected, no route.",
                ev.action, ev.underlying, ev.client_id, ev.binding_id,
            )
            await self._abort(ev, routing_failed=True)
            return

        # SELL (closing) must always route -- gate only the BUY (entry / add-on) side.
        if ev.action == "BUY" and db is not None:
            from strategies.core.gate import can_trade
            if not can_trade(ev.client_id, ev.binding_id, db, ev.strategy or _GATE_STRATEGY, ev.underlying):
                logger.warning(
                    "LiquidityTrapExecutionBridge: BUY %s — [%s/%s] can_trade() gate closed.",
                    ev.underlying, ev.client_id, ev.binding_id,
                )
                await self._abort(ev, routing_failed=True)
                return

        mode = live_binding.get("trading_mode", "paper") or "paper"

        if mode == "paper":
            logger.info(
                "LiquidityTrapExecutionBridge: %s %s %s%d exp=%s qty=%d add_on=%s → [%s/%s] mode=paper",
                ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
                ev.quantity, ev.is_add_on, ev.client_id, ev.binding_id,
            )
            await self._paper_fill(ev)
            return

        broker = await resolve_broker_or_alert(
            self._bus, self._router, ev.client_id, ev.binding_id, _BROKER_RESOLVE_LABEL,
            context=f"{ev.action} {ev.underlying} {ev.option_type}{ev.strike}",
        )

        logger.info(
            "LiquidityTrapExecutionBridge: %s %s %s%d exp=%s qty=%d add_on=%s → [%s/%s] mode=%s broker=%s",
            ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
            ev.quantity, ev.is_add_on, ev.client_id, ev.binding_id, mode,
            "resolved" if broker is not None else "UNAVAILABLE",
        )

        if broker is None:
            await self._abort(ev, routing_failed=True)
            return

        await self._live_fill(ev, broker, paper_route=(mode == "paper_route"))

    async def _abort(self, ev: LiquidityTrapOrderEvent, routing_failed: bool = False) -> None:
        await self._bus.publish(Topic.LIQUIDITY_TRAP_ORDER_FILL, LiquidityTrapFillEvent(
            action=ev.action, underlying=ev.underlying, option_type=ev.option_type or "",
            strike=int(ev.strike or 0), fill_price=0.0, qty=int(ev.quantity or 0),
            client_id=ev.client_id, binding_id=ev.binding_id, event_id=ev.event_id or "",
            is_add_on=ev.is_add_on,
            entry_aborted=(ev.action == "BUY"),
            exit_failed=(ev.action == "SELL"),
            routing_failed=routing_failed,
        ))

    # ── dashboard trade history ──────────────────────────────────────────────

    def _record_history(self, ev: LiquidityTrapOrderEvent, fill_price: float) -> None:
        """Record a CLOSED trade to the dashboard's History tab — own
        implementation, no import from any other strategy's bridge.
        data_layer.trade_history IS platform infra (same category as
        position_store/instrument_registry already reused above)."""
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
                    "entry_reason": "liquidity_trap_choch",
                    "entry_ts": _entry_ts.isoformat() if hasattr(_entry_ts, "isoformat") else _entry_ts,
                    "exit_ts": datetime.now(IST).isoformat(),
                }],
            )
        except Exception:
            logger.exception("LiquidityTrapExecutionBridge: trade_history.record failed (non-fatal).")

    # ── paper ─────────────────────────────────────────────────────────────────

    async def _paper_fill(self, ev: LiquidityTrapOrderEvent) -> None:
        fill_price = ev.entry_price
        if ev.action == "SELL" and ev.exit_price > 0:
            fill_price = ev.exit_price
        logger.info(
            "[PAPER] LIQTRAP %s %s %s%d exp=%s qty=%d @ %.2f add_on=%s | client=%s/%s",
            ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
            ev.quantity, fill_price, ev.is_add_on, ev.client_id, ev.binding_id,
        )
        self._trade_log.log(
            ev.client_id, ev.binding_id,
            f"[PAPER] {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} "
            f"exp={ev.expiry} qty={ev.quantity} @ {fill_price:.2f} add_on={ev.is_add_on} reason={ev.reason}",
        )
        self._record_history(ev, fill_price)
        await self._bus.publish(Topic.LIQUIDITY_TRAP_ORDER_FILL, LiquidityTrapFillEvent(
            action=ev.action, underlying=ev.underlying, option_type=ev.option_type or "",
            strike=int(ev.strike or 0), fill_price=fill_price, qty=int(ev.quantity or 0),
            client_id=ev.client_id, binding_id=ev.binding_id, event_id=ev.event_id or "",
            is_add_on=ev.is_add_on, paper_mode=True,
        ))

    # ── live ──────────────────────────────────────────────────────────────────

    def _resolve_symbol(self, ev: LiquidityTrapOrderEvent, broker) -> str:
        if not ev.expiry or not ev.strike or not ev.option_type:
            return ""
        _b = getattr(broker, "_binding", None)
        provider = _b.provider if _b else getattr(broker, "provider", "mock")
        return REGISTRY.get_broker_symbol(ev.underlying, ev.expiry, int(ev.strike), ev.option_type, provider)

    async def _live_fill(self, ev: LiquidityTrapOrderEvent, broker, paper_route: bool = False) -> None:
        """paper_route=True: places the REAL order via broker.place_order()
        (verifies real routing from a whitelisted IP -- a no-fund rejection
        is EXPECTED and fine), but always finalizes a fill for the strategy
        -- the broker's own avg_price if it happened to confirm one (>0),
        else a LOCAL SIMULATED fill at the strategy's own passed-in price.
        paper_route=False (real live): an unconfirmed fill (avg<=0) is
        NEVER faked -- aborts instead."""
        symbol = self._resolve_symbol(ev, broker)
        if not symbol:
            logger.error(
                "LiquidityTrapExecutionBridge: no tradable symbol for %s %s%d — paper fallback.",
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
                        "[%s] LIQTRAP %s %s %s%d — PARTIAL FILL: requested %d, filled %d @ %.2f "
                        "order_id=%s | client=%s/%s",
                        _tag, ev.action, ev.underlying, ev.option_type, ev.strike,
                        ev.quantity, filled_qty, avg, order_id, ev.client_id, ev.binding_id,
                    )
                else:
                    filled_qty = ev.quantity
                logger.info(
                    "[%s] LIQTRAP %s %s %s%d exp=%s qty=%d filled=%d @ %.2f order_id=%s add_on=%s | client=%s/%s",
                    _tag, ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
                    ev.quantity, filled_qty, avg, order_id, ev.is_add_on, ev.client_id, ev.binding_id,
                )
                self._trade_log.log(
                    ev.client_id, ev.binding_id,
                    f"[{_tag}] {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} "
                    f"exp={ev.expiry} qty={ev.quantity} filled={filled_qty} @ {avg:.2f} symbol={symbol} "
                    f"order_id={order_id} add_on={ev.is_add_on} reason={ev.reason}",
                )
            else:
                logger.info(
                    "[%s] LIQTRAP %s %s %s%d exp=%s qty=%d — order reached broker (order_id=%s) "
                    "but no confirmed fill (avg_price<=0) | client=%s/%s",
                    _tag, ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
                    ev.quantity, order_id, ev.client_id, ev.binding_id,
                )
        except Exception as exc:
            logger.error(
                "[%s] LIQTRAP %s %s %s%d order FAILED: %s.",
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
                    "[PAPER_ROUTE] LIQTRAP %s %s %s%d — no confirmed broker fill (expected for "
                    "no-fund account); booking SIMULATED fill @ %.2f | client=%s/%s",
                    ev.action, ev.underlying, ev.option_type, ev.strike, sim_price,
                    ev.client_id, ev.binding_id,
                )
                self._trade_log.log(
                    ev.client_id, ev.binding_id,
                    f"[PAPER_ROUTE] {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} "
                    f"exp={ev.expiry} qty={ev.quantity} @ {sim_price:.2f} (simulated, real order "
                    f"attempted order_id={order_id or 'none'}) add_on={ev.is_add_on} reason={ev.reason}",
                )
                self._record_history(ev, sim_price)
                await self._bus.publish(Topic.LIQUIDITY_TRAP_ORDER_FILL, LiquidityTrapFillEvent(
                    action=ev.action, underlying=ev.underlying, option_type=ev.option_type or "",
                    strike=int(ev.strike or 0), fill_price=sim_price, qty=int(ev.quantity or 0),
                    client_id=ev.client_id, binding_id=ev.binding_id, event_id=ev.event_id or "",
                    is_add_on=ev.is_add_on, paper_mode=True, symbol=symbol,
                ))
                return
            logger.error(
                "[LIVE] LIQTRAP %s %s %s%d — NO confirmed fill, %s NOT reported as a fill "
                "(no phantom position/close). client=%s/%s",
                ev.action, ev.underlying, ev.option_type, ev.strike,
                "entry" if ev.action == "BUY" else "exit", ev.client_id, ev.binding_id,
            )
            await self._abort(ev, routing_failed=False)
            return

        self._record_history(ev, avg)
        await self._bus.publish(Topic.LIQUIDITY_TRAP_ORDER_FILL, LiquidityTrapFillEvent(
            action=ev.action, underlying=ev.underlying, option_type=ev.option_type or "",
            strike=int(ev.strike or 0), fill_price=avg, qty=int(ev.quantity or 0),
            client_id=ev.client_id, binding_id=ev.binding_id, event_id=ev.event_id or "",
            is_add_on=ev.is_add_on, paper_mode=paper_route, symbol=symbol, filled_qty=filled_qty,
        ))
