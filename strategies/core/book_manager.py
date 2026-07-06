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

from config.global_config import Topic

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
        # Lazy subscribe so unit tests that pass bus=None don't crash.
        self._kill_switch_q = bus.subscribe(Topic.SYSTEM_EVENT) if bus is not None else None

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
        # Firm-wide kill-switch listener runs in parallel with the reconcile loop.
        kill_task = None
        if self._kill_switch_q is not None:
            kill_task = asyncio.create_task(self._kill_switch_loop(), name=f"{self.__class__.__name__}_kill_switch")
        try:
            while self._running:
                try:
                    self._reconcile()
                except Exception as exc:
                    logger.warning("%s.reconcile error: %s", self.__class__.__name__, exc)
                try:
                    await asyncio.sleep(self._reconcile_sec)
                except asyncio.CancelledError:
                    break
        finally:
            if kill_task is not None:
                kill_task.cancel()
                try:
                    await kill_task
                except asyncio.CancelledError:
                    pass

    async def _kill_switch_loop(self) -> None:
        """Listen for firm-wide KILL_SWITCH events and liquidate all books immediately."""
        while self._running:
            try:
                ev = await asyncio.wait_for(self._kill_switch_q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                if isinstance(ev, dict) and ev.get("event") == "KILL_SWITCH":
                    await self.liquidate_all(scope=ev.get("scope", "FIRM_WIDE"))
            except Exception as exc:
                logger.exception("%s.kill_switch_loop error: %s", self.__class__.__name__, exc)

    async def liquidate_all(self, scope: str = "FIRM_WIDE") -> None:
        """Emergency liquidation of every managed book. Positions are market-closed
        and books are stopped. Safe to call multiple times."""
        if not self._books:
            return
        logger.warning("%s: KILL_SWITCH received (%s) — liquidating %d book(s).",
                       self.__class__.__name__, scope, len(self._books))
        # Snapshot the books so a concurrent reconcile mutation does not change
        # the iteration while we are liquidating.
        books_snapshot = list(self._books.items())
        _reason = "kill_switch" if scope == "FIRM_WIDE" else scope
        results = await asyncio.gather(
            *[self._liquidate_book(book, key, reason=_reason) for key, book in books_snapshot],
            return_exceptions=True,
        )
        for key, res in zip([k for k, _ in books_snapshot], results):
            if isinstance(res, Exception):
                logger.error("%s: liquidation failed for %s: %s", self.__class__.__name__, key, res)
        logger.warning("%s: liquidation complete.", self.__class__.__name__)

    async def _liquidate_book(self, book: Any, key: Key, reason: str = "kill_switch") -> None:
        """Close any open position on ``book`` with the given reason and stop its tasks."""
        if hasattr(book, "liquidate"):
            try:
                await book.liquidate(reason)
            except Exception as exc:
                logger.warning("%s: book.liquidate(%s) failed: %s", self.__class__.__name__, key, exc)
        try:
            if hasattr(book, "stop_async"):
                await book.stop_async()
            elif hasattr(book, "stop"):
                book.stop()
        except Exception as exc:
            logger.warning("%s: stop book %s failed: %s", self.__class__.__name__, key, exc)

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
        if hasattr(self, "_log_reconcile"):
            self._log_reconcile(wanted, self._books)

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

        # Stop books whose key is no longer wanted. If a book still has an open
        # position, liquidate it first so we do not orphan broker legs.
        for key in set(self._books) - set(wanted):
            book = self._books.pop(key)
            if not self._is_flat(book):
                logger.warning(
                    "%s: removing %s with open position — liquidating before stop.",
                    self.__class__.__name__, key,
                )
                asyncio.create_task(self._liquidate_book(book, key, reason="deployment_stop"))
            else:
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
        # Best-effort: if we are on a running loop, schedule emergency liquidation.
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(self.liquidate_all(scope="sync_stop"))
        except Exception:
            pass
        for book in self._books.values():
            try:
                book.stop()
            except Exception:
                pass

    async def stop_async(self) -> None:
        """Graceful shutdown — liquidate open positions, then cancel tasks."""
        self._running = False
        try:
            await self.liquidate_all(scope="system_shutdown")
        except Exception as exc:
            logger.warning("%s: liquidation during shutdown failed: %s", self.__class__.__name__, exc)
        # liquidate_all already stopped each book; just clear the manager's references.
        self._books.clear()
