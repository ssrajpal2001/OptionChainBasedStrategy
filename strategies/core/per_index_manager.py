"""
strategies/core/per_index_manager.py — wrapper for one strategy instance per monitored index.

Used for strategies (e.g. iron_condor) that spawn one engine per underlying rather
than one per (client, binding, underlying). It exposes the same lifecycle hooks the
launcher expects (`books`, `find`, `start`, `stop`, `stop_async`, `run`, `set_rebalancer`,
`set_feeder`) so it can live side-by-side with per-binding managers in the strategy
registry.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, List, Optional

logger = logging.getLogger(__name__)


class PerIndexManager:
    """
    Maintain one ``strategy_class`` instance for every monitored index.

    Parameters
    ----------
    bus: EventBus
    cfg: GlobalConfig
    monitored_indices: list[str]
    strategy_class: class
        Constructor signature: ``strategy_class(bus, cfg, underlying=idx)``.
    """

    def __init__(self, bus, cfg, monitored_indices, strategy_class) -> None:
        self._bus = bus
        self._cfg = cfg
        self._strategy_class = strategy_class
        self._indices = [str(i).upper() for i in (monitored_indices or [])]
        self._rebalancer = None
        self._feeder = None

        self._books: List[Any] = [
            strategy_class(bus, cfg, underlying=idx)
            for idx in self._indices
        ]

    @property
    def books(self) -> List[Any]:
        return list(self._books)

    def find(self, underlying: str) -> Optional[Any]:
        """Return the book whose underlying matches ``underlying`` (case-insensitive)."""
        needle = str(underlying).upper()
        for book in self._books:
            if str(getattr(book, "_underlying", "")).upper() == needle:
                return book
        return None

    def start(self) -> None:
        for book in self._books:
            try:
                book.start()
            except Exception as exc:
                logger.warning("PerIndexManager: start failed for %s: %s",
                               getattr(book, "_underlying", "?"), exc)

    def stop(self) -> None:
        for book in self._books:
            try:
                book.stop()
            except Exception as exc:
                logger.warning("PerIndexManager: stop failed for %s: %s",
                               getattr(book, "_underlying", "?"), exc)

    async def stop_async(self) -> None:
        """Cancel and await each book's tasks, mirroring per-binding managers."""
        self.stop()
        tasks: List[asyncio.Task] = []
        for book in self._books:
            for t in getattr(book, "_tasks", []) or []:
                if isinstance(t, asyncio.Task) and not t.done():
                    tasks.append(t)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def run(self) -> None:
        """No-op: per-index strategies are self-driven via tasks spawned in ``start()``."""
        pass

    def set_rebalancer(self, rebalancer) -> None:
        self._rebalancer = rebalancer
        for book in self._books:
            if hasattr(book, "set_rebalancer"):
                try:
                    book.set_rebalancer(rebalancer)
                except Exception as exc:
                    logger.warning("PerIndexManager: set_rebalancer failed for %s: %s",
                                   getattr(book, "_underlying", "?"), exc)

    def set_feeder(self, feeder) -> None:
        self._feeder = feeder
        for book in self._books:
            if hasattr(book, "set_feeder"):
                try:
                    book.set_feeder(feeder)
                except Exception as exc:
                    logger.warning("PerIndexManager: set_feeder failed for %s: %s",
                                   getattr(book, "_underlying", "?"), exc)
