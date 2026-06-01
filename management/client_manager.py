"""
management/client_manager.py — Runtime client lifecycle and risk enforcement.

Responsibilities:
  - Subscribe to ORDER_FILL topic and update per-client P&L
  - Enforce daily drawdown limits; auto-halt clients that breach them
  - Provide signal validation gate: does this client accept this signal?
  - Broadcast SYSTEM_EVENT on halt/resume
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Optional

from config.global_config import IST, Topic
from config.client_profiles import ClientProfile, ClientRegistry
from data_layer.base_feeder import EventBus
from execution_bridge.base_broker import OrderFill, OrderStatus

logger = logging.getLogger(__name__)


class ClientManager:
    """
    Monitors order fills and enforces risk limits for all clients.

    Runs as a background task alongside ExecutionRouter.
    """

    def __init__(self, bus: EventBus, registry: ClientRegistry) -> None:
        self._bus = bus
        self._registry = registry
        self._fill_queue = bus.subscribe(Topic.ORDER_FILL)
        self._running = False

    async def run(self) -> None:
        self._running = True
        logger.info("ClientManager: Started.")
        while self._running:
            try:
                fill: OrderFill = await asyncio.wait_for(
                    self._fill_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            await self._process_fill(fill)

    async def stop(self) -> None:
        self._running = False

    async def _process_fill(self, fill: OrderFill) -> None:
        # Topic.ORDER_FILL also carries bridge fill events (ICFillEvent /
        # StraddleFillEvent) without a .status field — skip those here.
        if getattr(fill, "status", None) != OrderStatus.COMPLETE:
            return
        client = self._registry.get(fill.client_id)
        if client is None:
            return

        # P&L is tracked properly on exit fills; entry fills record 0 cost here.
        # The backtester / exit handler should call record_trade() with actual P&L.
        # Here we enforce the limit check after any trade update.
        if not client.is_tradeable():
            await self._emit_halt(client, "Drawdown limit breached after fill.")

    async def _emit_halt(self, client: ClientProfile, reason: str) -> None:
        client.halt()
        logger.warning("ClientManager: HALTED %s — %s", client.client_id, reason)
        await self._bus.publish(Topic.SYSTEM_EVENT, {
            "event": "CLIENT_HALTED",
            "client_id": client.client_id,
            "reason": reason,
            "timestamp": datetime.now(IST).isoformat(),
        })

    def validate_signal(self, client: ClientProfile, strategy_id: str) -> bool:
        """
        Returns True if client is eligible to trade this strategy signal.
        Called by ExecutionRouter before building order tasks.
        """
        if not client.is_tradeable():
            return False
        sid = strategy_id.upper().replace("STRATEGY_", "")
        return sid in [s.upper() for s in client.enabled_strategies]

    async def daily_reset(self) -> None:
        """Call once at market open (09:15 IST) to reset daily P&L counters."""
        self._registry.reset_all_daily()
        logger.info("ClientManager: Daily P&L counters reset.")
        await self._bus.publish(Topic.SYSTEM_EVENT, {
            "event": "DAILY_RESET",
            "timestamp": datetime.now(IST).isoformat(),
        })
