"""
strategies/core/book_manager.py — generic per-binding strategy book manager.

Reconciles a set of wanted (client, binding, underlying) books against a DB
query, spawns new books, stops removed books, and re-spawns books when their
lot multiplier changes while flat.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

Key = Tuple[str, str, str]  # (client_id, binding_id, underlying)


class StrategyBookManager:
    """
    Generic manager for one independent book per (client, binding, underlying).

    Subclasses supply:
      - ``_wanted()`` -> {key: value}
      - ``_spawn_book(key, value)`` -> book instance
      - ``_stop_book(book)`` (optional; default calls ``book.stop()``)
      - ``_should_respawn(book, value)`` -> bool (optional; default False)
    """

    def __init__(
        self,
        bus,
        cfg,
        client_db,
        monitored_indices,
        reconcile_sec: float = 5.0,
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._db = client_db
        self._indices = {str(i).upper() for i in (monitored_indices or [])}
        self._reconcile_sec = reconcile_sec
        self._books: Dict[Key, Any] = {}
        self._rebalancer = None
        self._running = False

    def set_rebalancer(self, rebalancer) -> None:
        self._rebalancer = rebalancer
        for book in self._books.values():
            if hasattr(book, "set_rebalancer"):
                book.set_rebalancer(rebalancer)

    @property
    def books(self) -> List[Any]:
        return list(self._books.values())

    def find(self, client_id: str, binding_id: str, underlying: str) -> Optional[Any]:
        return self._books.get((client_id, binding_id, str(underlying).upper()))

    async def run(self) -> None:
        self._running = True
        logger.info("%s: started (indices=%s).", self.__class__.__name__, sorted(self._indices))
        while self._running:
            try:
                self._reconcile()
            except Exception as exc:
                logger.warning("%s.reconcile error: %s", self.__class__.__name__, exc)
            try:
                await asyncio.sleep(self._reconcile_sec)
            except asyncio.CancelledError:
                break

    def _wanted(self) -> Dict[Key, Any]:
        """Return {(client_id, binding_id, underlying): value} for books that should exist."""
        raise NotImplementedError

    def _spawn_book(self, key: Key, value: Any) -> Any:
        """Create a new book for ``key`` with configuration ``value``."""
        raise NotImplementedError

    def _stop_book(self, book: Any) -> None:
        """Stop a book being removed."""
        try:
            book.stop()
        except Exception:
            pass

    def _should_respawn(self, book: Any, value: Any) -> bool:
        """Return True when an existing book should be torn down and recreated."""
        return False

    def _log_spawned(self, key: Key, value: Any) -> None:
        """Log line emitted after a new book is spawned."""
        pass

    def _log_stopped(self, key: Key) -> None:
        """Log line emitted after a book is stopped."""
        pass

    def _log_respawned(self, key: Key, value: Any) -> None:
        """Log line emitted after a book is re-spawned."""
        pass

    def _is_flat(self, book: Any) -> bool:
        """True when the book has no open position. Override if the book uses a different field."""
        return getattr(book, "_position", None) is None

    def _reconcile(self) -> None:
        wanted = self._wanted()

        # Spawn books for newly-wanted keys.
        for key in set(wanted) - set(self._books):
            try:
                book = self._spawn_book(key, wanted[key])
                book.start()
                self._books[key] = book
                self._log_spawned(key, wanted[key])
            except Exception as exc:
                logger.warning("%s: spawn %s failed: %s",
                               self.__class__.__name__, key, exc, exc_info=True)

        # Stop books whose key is no longer wanted.
        for key in set(self._books) - set(wanted):
            book = self._books.pop(key)
            self._stop_book(book)
            self._log_stopped(key)

        # Re-spawn on configuration change only when flat.
        for key, value in wanted.items():
            book = self._books.get(key)
            if book is None:
                continue
            if not self._is_flat(book):
                continue
            if not self._should_respawn(book, value):
                continue
            try:
                self._stop_book(book)
                nb = self._spawn_book(key, value)
                nb.start()
                self._books[key] = nb
                self._log_respawned(key, value)
            except Exception as exc:
                logger.warning("%s: re-spawn %s failed: %s",
                               self.__class__.__name__, key, exc)

    def stop(self) -> None:
        self._running = False
        for book in self._books.values():
            try:
                book.stop()
            except Exception:
                pass

    async def stop_async(self) -> None:
        """Graceful shutdown — awaits each book's task cancellation."""
        self._running = False
        await asyncio.gather(
            *[book.stop_async() for book in self._books.values()],
            return_exceptions=True,
        )
        self._books.clear()
