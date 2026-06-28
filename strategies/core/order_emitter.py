"""
strategies/core/order_emitter.py — reusable order-event emitter.

Stamps a book's identity (client_id, binding_id) onto an event before
publishing so the execution bridge can route to a single broker binding.
"""
from __future__ import annotations

from typing import Any

from data_layer.base_feeder import EventBus


class OrderEmitter:
    def __init__(self, bus: EventBus, client_id: str, binding_id: str) -> None:
        self._bus = bus
        self.client_id = client_id
        self.binding_id = binding_id

    async def emit(self, topic: str, event: Any) -> None:
        """Stamp ``client_id`` / ``binding_id`` on ``event`` and publish."""
        event.client_id = self.client_id
        event.binding_id = self.binding_id
        await self._bus.publish(topic, event)
