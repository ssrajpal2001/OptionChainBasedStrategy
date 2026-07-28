"""
execution_bridge/d1_trap_bridge.py — D1 Trap + Option order router.

Subscribes to Topic.D1_TRAP_ORDER_REQUEST for D1TrapOrderEvent objects
published by strategies/d1_trap_option/book.py.

Routes STRICTLY to the event's own (client_id, binding_id) broker — never
broadcasts. Symbol resolution is per-broker at order time (Zerodha weekly
format: NIFTY2672924300CE; Upstox: numeric instrument key, etc.).

Paper mode  — local simulated fill at strategy spot price; NO real order.
Live mode   — MARKET order via broker.place_order() + get_order_status();
              position booked from the real fill price.

Product: MIS (intraday — all positions are squared off by EOD in book.py).
Exchange: NFO (NIFTY), BFO (SENSEX), etc. resolved via order_exchange().
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional

from config.global_config import IST, Topic, order_exchange
from data_layer.base_feeder import EventBus
from execution_bridge.straddle_bridge import _resolve_option_symbol

logger = logging.getLogger(__name__)


class _D1TrapTradeLogger:
    def __init__(self, log_dir: str = "logs/trades") -> None:
        os.makedirs(log_dir, exist_ok=True)
        self._log_dir = log_dir
        self._handles: Dict[str, object] = {}

    def _handle(self, client_id: str, binding_id: str):
        today = datetime.now(IST).strftime("%Y%m%d")
        key = f"{client_id}-{binding_id}-d1_trap-{today}"
        if key not in self._handles:
            path = os.path.join(self._log_dir, f"{key}.log")
            self._handles[key] = open(path, "a", encoding="utf-8", buffering=1)
        return self._handles[key]

    def log(self, client_id: str, binding_id: str, message: str) -> None:
        ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        self._handle(client_id, binding_id).write(f"{ts}  D1TRAP  {message}\n")

    def close_all(self) -> None:
        for h in self._handles.values():
            try:
                h.close()
            except Exception:
                pass
        self._handles.clear()


class D1TrapExecutionBridge:
    """
    Listens for D1TrapOrderEvent on Topic.D1_TRAP_ORDER_REQUEST.

    Routes BUY (ENTRY) and SELL (EXIT) to the owning (client_id, binding_id)
    broker.  Resolves the correct broker option symbol (Zerodha NIFTY2672924300CE,
    Upstox numeric key, etc.) at order time using the broker's provider field.
    """

    def __init__(self, bus: EventBus, router, log_dir: str = "logs/trades") -> None:
        self._bus = bus
        self._router = router
        self._trade_log = _D1TrapTradeLogger(log_dir)
        self._running = False
        self._q = bus.subscribe(Topic.D1_TRAP_ORDER_REQUEST)

    async def run(self) -> None:
        self._running = True
        logger.info("D1TrapExecutionBridge: started.")
        while self._running:
            try:
                ev = await asyncio.wait_for(self._q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            try:
                from strategies.d1_trap_option.book import D1TrapOrderEvent
                if not isinstance(ev, D1TrapOrderEvent):
                    continue
                await self._handle(ev)
            except Exception:
                logger.exception("D1TrapExecutionBridge: _handle error.")

    def stop(self) -> None:
        self._running = False
        self._trade_log.close_all()
        logger.info("D1TrapExecutionBridge: stopped.")

    # ── routing ───────────────────────────────────────────────────────────────

    async def _handle(self, ev) -> None:
        if not ev.client_id or not ev.binding_id:
            logger.error("D1TrapExecutionBridge: event missing client_id/binding_id — dropped.")
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
                "D1TrapExecutionBridge: %s %s — [%s/%s] terminal not connected, no route.",
                ev.action, ev.underlying, ev.client_id, ev.binding_id,
            )
            return

        # EXIT must always route — gate only ENTRY on is_running
        if ev.action == "BUY" and db is not None and hasattr(db, "get_deployments_sync"):
            try:
                deployments = db.get_deployments_sync(ev.client_id)
            except Exception:
                deployments = []
            matching = [
                d for d in deployments
                if d.get("binding_id") == ev.binding_id
                and d.get("strategy_name") == "d1_trap_option"
                and str(d.get("underlying", "")).upper() == ev.underlying.upper()
                and int(d.get("is_running", 0) or 0) == 1
            ]
            if not matching:
                logger.warning(
                    "D1TrapExecutionBridge: BUY %s — [%s/%s] no running d1_trap_option deployment.",
                    ev.underlying, ev.client_id, ev.binding_id,
                )
                return

        broker = (self._router._brokers or {}).get(ev.client_id, {}).get(ev.binding_id)
        mode = live_binding.get("trading_mode", "paper") or "paper"

        logger.info(
            "D1TrapExecutionBridge: %s %s %s%d exp=%s qty=%d → [%s/%s] mode=%s",
            ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
            ev.quantity, ev.client_id, ev.binding_id, mode,
        )

        if broker is None or mode == "paper":
            await self._paper_fill(ev)
        else:
            await self._live_fill(ev, broker)

    # ── paper ─────────────────────────────────────────────────────────────────

    async def _paper_fill(self, ev) -> None:
        logger.info(
            "[PAPER] D1Trap %s %s %s%d exp=%s qty=%d spot=%.2f | client=%s/%s",
            ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
            ev.quantity, ev.entry_price, ev.client_id, ev.binding_id,
        )
        self._trade_log.log(
            ev.client_id, ev.binding_id,
            f"[PAPER] {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} "
            f"exp={ev.expiry} qty={ev.quantity} spot={ev.entry_price:.2f} reason={ev.reason}",
        )
        self._record_history(ev, ev.entry_price, paper=True)

    # ── live ──────────────────────────────────────────────────────────────────

    async def _live_fill(self, ev, broker) -> None:
        symbol = self._resolve_symbol(ev, broker)
        if not symbol:
            logger.error(
                "D1TrapExecutionBridge: no tradable symbol for %s %s%d — paper fallback.",
                ev.underlying, ev.option_type, ev.strike,
            )
            await self._paper_fill(ev)
            return

        from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType
        side = OrderSide.BUY if ev.action == "BUY" else OrderSide.SELL
        exchange = order_exchange(ev.underlying)

        req = OrderRequest(
            symbol=symbol,
            exchange=exchange,
            side=side,
            qty=ev.quantity,
            order_type=OrderType.MARKET,
            product="MIS",
            price=ev.entry_price,  # ignored for MARKET; MockBroker uses as fill
            tag=f"D1T_{ev.underlying}_{ev.action}"[:20],
        )

        avg = 0.0
        order_id = ""
        try:
            order_id = await broker.place_order(req)
            fill = await broker.get_order_status(str(order_id))
            avg = float(getattr(fill, "avg_price", 0.0) or 0.0)
            if avg <= 0:
                avg = ev.entry_price
            logger.info(
                "[LIVE] D1Trap %s %s %s%d exp=%s qty=%d @ %.2f order_id=%s | client=%s/%s",
                ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
                ev.quantity, avg, order_id, ev.client_id, ev.binding_id,
            )
            self._trade_log.log(
                ev.client_id, ev.binding_id,
                f"[LIVE] {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} "
                f"exp={ev.expiry} qty={ev.quantity} @ {avg:.2f} symbol={symbol} "
                f"order_id={order_id} reason={ev.reason}",
            )
        except Exception as exc:
            logger.error(
                "[LIVE] D1Trap %s %s %s%d order FAILED: %s — falling back to spot price.",
                ev.action, ev.underlying, ev.option_type, ev.strike, exc,
            )
            self._trade_log.log(
                ev.client_id, ev.binding_id,
                f"LIVE {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} "
                f"FAILED: {exc}",
            )
            avg = ev.entry_price

        self._record_history(ev, avg, paper=False)

    def _resolve_symbol(self, ev, broker) -> str:
        if not ev.expiry or not ev.strike or not ev.option_type:
            return ""
        _b = getattr(broker, "_binding", None)
        provider = _b.provider if _b else getattr(broker, "provider", "mock")
        return _resolve_option_symbol(
            ev.underlying, ev.expiry, int(ev.strike), ev.option_type, provider
        )

    def _record_history(self, ev, fill_price: float, paper: bool) -> None:
        if ev.action != "SELL":
            return
        try:
            from data_layer import trade_history as _th
            pnl = 0.0  # bridge doesn't know entry fill price for options; book logs spot P&L
            _th.record(
                ev.client_id, "d1_trap_option", ev.underlying,
                ev.entry_price, fill_price, ev.reason, pnl,
                binding_id=ev.binding_id,
                legs=[{
                    "side": ev.option_type,
                    "strike": ev.strike,
                    "entry": ev.entry_price,
                    "exit": fill_price,
                    "pnl": pnl,
                    "entry_reason": ev.reason,
                }],
            )
        except Exception:
            logger.exception(
                "D1TrapExecutionBridge: trade_history record failed for %s/%s",
                ev.client_id, ev.binding_id,
            )
