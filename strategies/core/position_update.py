"""
strategies/core/position_update.py — reusable position-update broadcaster.

Strategy books call notify_position_update() whenever their position state changes
meaningfully (entry, exit, rollover, fill). The WsBridge forwards these events to
connected browsers so the UI positions panel is real-time without polling the
backend on every tick.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

from config.global_config import IST, Topic
from data_layer.base_feeder import EventBus

logger = logging.getLogger(__name__)


class PositionUpdateMixin:
    """Mixin that publishes lightweight position-update events over the EventBus.

    Throttles high-frequency updates (e.g. per-tick P&L) to a configurable minimum
    interval so the WebSocket link is not saturated.
    """

    _MIN_PUSH_INTERVAL_SECONDS = 0.25  # max 4 position pushes/sec per book

    def __init__(
        self,
        bus: EventBus,
        client_id: str,
        binding_id: str,
        strategy_name: str,
        underlying: str,
    ) -> None:
        self._pu_bus = bus
        self._pu_client_id = client_id
        self._pu_binding_id = binding_id
        self._pu_strategy_name = strategy_name
        self._pu_underlying = underlying
        self._pu_last_push_ts: float = 0.0

    def notify_position_update(
        self,
        position_data: Optional[Dict[str, Any]],
        *,
        force: bool = False,
    ) -> None:
        """Broadcast a position_update event.

        position_data: JSON-serialisable snapshot of the position (or None when flat).
        force: if True, bypass the throttle (use for entry/exit/roll events).
        """
        if self._pu_bus is None:
            return
        now = time.monotonic()
        if not force and (now - self._pu_last_push_ts) < self._MIN_PUSH_INTERVAL_SECONDS:
            return
        self._pu_last_push_ts = now
        try:
            from datetime import datetime
            payload = {
                "type": "position_update",
                "client_id": self._pu_client_id,
                "binding_id": self._pu_binding_id,
                "strategy_name": self._pu_strategy_name,
                "underlying": self._pu_underlying,
                "position": position_data,
                "ts": datetime.now(IST).isoformat(),
            }
            # fire-and-forget; WsBridge is the only expected consumer
            asyncio = __import__("asyncio")
            asyncio.create_task(self._pu_bus.publish(Topic.POSITION_UPDATE, payload))
        except Exception as exc:
            logger.debug("PositionUpdateMixin: publish failed: %s", exc)
