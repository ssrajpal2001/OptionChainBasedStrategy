"""
strategies/core/base_book.py — abstract base for per-binding strategy books.

Provides common lifecycle scaffolding (start/stop/stop_async), EventBus
subscription helpers, and cleanup. Strategy-specific logic stays in subclasses.
"""
from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional

from data_layer.base_feeder import EventBus

logger = logging.getLogger(__name__)


class AbstractStrategyBook(ABC):
    """Minimal base class for one independent trading book per (client, binding, underlying)."""

    def __init__(
        self,
        bus: EventBus,
        cfg,
        underlying: str,
        client_id: str,
        binding_id: str,
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._underlying = underlying
        self._client_id = client_id
        self._binding_id = binding_id

        self._running = False
        self._tasks: list = []
        self._loop_queues: Dict[str, asyncio.Queue] = {}

    def start(self) -> None:
        """Start the book. Subclasses override to spawn their feed loops."""
        self._running = True
        logger.info("%s[%s/%s/%s]: started.", self.__class__.__name__,
                    self._client_id, self._binding_id, self._underlying)

    def stop(self) -> None:
        """Cancel all running tasks. Await cleanup via :meth:`stop_async`."""
        self._running = False
        for t in self._tasks:
            if not t.done():
                t.cancel()

    async def stop_async(self) -> None:
        """Cancel and await all tasks, then unsubscribe every stored queue."""
        self._running = False
        for t in self._tasks:
            if not t.done():
                t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._unsubscribe_all()

    def _subscribe(self, topic: str) -> asyncio.Queue:
        """Subscribe to ``topic`` and remember the queue for cleanup."""
        q = self._bus.subscribe(topic)
        self._loop_queues[topic] = q
        return q

    def _unsubscribe_all(self) -> None:
        """Unsubscribe every queue created via :meth:`_subscribe`."""
        for topic, q in list(self._loop_queues.items()):
            try:
                self._bus.unsubscribe(topic, q)
            except Exception:
                pass
        self._loop_queues.clear()

    @abstractmethod
    def reset_session(self) -> None:
        """Reset intraday/session state. Subclasses must implement."""
        raise NotImplementedError

    async def _tick_loop(self) -> None:
        """Override in subclasses to consume :attr:`Topic.INDEX_TICK`."""
        pass

    async def _candle_loop(self) -> None:
        """Override in subclasses to consume :attr:`Topic.CANDLE_CLOSE`."""
        pass

    async def _option_loop(self) -> None:
        """Override in subclasses to consume :attr:`Topic.OPTION_TICK`."""
        pass
