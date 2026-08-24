"""
strategies/core/book_manager.py — generic per-binding strategy book manager.

Reconciles a set of wanted (client, binding, underlying) books against a DB
query, spawns new books, stops removed books, and re-spawns books when their
lot multiplier changes while flat.
"""
from __future__ import annotations

import asyncio
import inspect
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
        self._stopping: Dict[Key, asyncio.Task] = {}
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

    async def _binding_trading_mode(self, client_id: str, binding_id: str) -> str:
        """Best-effort lookup of a binding's trading_mode. Defaults to "live"
        on ANY failure or missing binding -- fail toward MORE protection
        (always liquidate), never silently skip a real position because a
        lookup happened to fail.

        2026-08-24: confirmed live this DID default to "live" (real close
        still fired) for a genuinely paper_route binding at least once --
        root cause not yet identified. Every branch now logs explicitly so
        the next occurrence shows exactly which path was taken, instead of
        defaulting silently the way the earlier NSESession/backfill bugs
        did for hours before anyone could tell what was actually happening."""
        if self._db is None:
            logger.warning("%s: _binding_trading_mode(%s/%s): self._db is None -- defaulting to live.",
                            self.__class__.__name__, client_id, binding_id)
            return "live"
        if not hasattr(self._db, "get_bindings_safe_sync"):
            logger.warning("%s: _binding_trading_mode(%s/%s): self._db has no get_bindings_safe_sync "
                            "(type=%s) -- defaulting to live.",
                            self.__class__.__name__, client_id, binding_id, type(self._db).__name__)
            return "live"
        try:
            bindings = await asyncio.to_thread(self._db.get_bindings_safe_sync, client_id)
            ids_seen = [b.get("binding_id") for b in (bindings or [])]
            for b in bindings or []:
                if b.get("binding_id") == binding_id:
                    mode = str(b.get("trading_mode") or "live")
                    logger.info("%s: _binding_trading_mode(%s/%s) = %r.",
                                self.__class__.__name__, client_id, binding_id, mode)
                    return mode
            logger.warning("%s: _binding_trading_mode(%s/%s): binding_id not found among %r -- "
                            "defaulting to live.", self.__class__.__name__, client_id, binding_id, ids_seen)
        except Exception as exc:
            logger.warning("%s: _binding_trading_mode(%s/%s): lookup raised %r -- defaulting to live.",
                            self.__class__.__name__, client_id, binding_id, exc)
        return "live"

    async def liquidate_all(self, scope: str = "FIRM_WIDE") -> None:
        """Emergency liquidation of every managed book. Positions are market-closed
        and books are stopped. Safe to call multiple times.

        2026-08-24: scope="system_shutdown" (the ONLY caller is stop_async(),
        which is itself only reached via a genuine graceful shutdown -- a
        SIGTERM/SIGINT from `pm2 restart`/`pm2 stop`/a plain kill, or the
        admin console's own shutdown button, see run_system.py's
        _register_graceful_shutdown_signals) now SKIPS the real close for any
        binding in paper/paper_route mode. Confirmed live 2026-08-24: forcing
        a real close on every routine restart was disrupting same-day paper
        testing for zero real safety benefit -- a paper/paper_route
        "position" is local simulated bookkeeping, no real broker exposure is
        left unmanaged by NOT closing it (a paper_route order is expected to
        be broker-rejected anyway; nothing real is actually held). The
        book's own tasks are still stopped/cancelled normally either way --
        only the real-close call itself is skipped. A genuine kill-switch
        (scope="FIRM_WIDE", a deliberate explicit emergency action, not an
        incidental restart) is UNCHANGED -- it still closes everything
        regardless of trading_mode, on purpose. The moment a binding flips to
        live trading_mode, the exact same restart goes back to protecting it
        exactly as the 2026-08-23 fix originally intended -- nothing about
        real live-capital safety changes.

        Note on the spawn-race this class otherwise guards against via
        self._stopping: this method does NOT need that tracking. It never pops
        keys out of self._books (only reconcile()'s stop/respawn loops do that,
        and _reconcile() is a plain synchronous function that always runs to
        completion in one go -- it cannot interleave with this coroutine's
        awaits). So for the whole duration of this liquidation, every key here
        remains present in self._books, and the reconcile spawn loop only ever
        considers `set(wanted) - set(self._books)` -- a key that's still in
        self._books can never be spawned again, liquidating or not. The
        narrower risk (reconcile's stop loop popping the same key and calling
        stop_async() a second time on a book we are concurrently liquidating,
        if that key also drops out of `_wanted()` mid-liquidation) is real but
        is a redundant-stop-call/idempotency concern, not a duplicate-live-
        instance one -- out of scope for this fix.
        """
        if not self._books:
            return
        logger.warning("%s: KILL_SWITCH received (%s) — liquidating %d book(s).",
                       self.__class__.__name__, scope, len(self._books))
        # Snapshot the books so a concurrent reconcile mutation does not change
        # the iteration while we are liquidating.
        books_snapshot = list(self._books.items())
        _reason = "kill_switch" if scope == "FIRM_WIDE" else scope

        skip_flags: Dict[Key, bool] = {}
        if scope == "system_shutdown":
            for key, _book in books_snapshot:
                client_id, binding_id = key[0], key[1]
                mode = await self._binding_trading_mode(client_id, binding_id)
                skip_close = mode in ("paper", "paper_route")
                skip_flags[key] = skip_close
                if skip_close:
                    logger.warning(
                        "%s: %s is trading_mode=%s -- skipping real close on graceful shutdown "
                        "(no real broker exposure to protect), book still stops normally.",
                        self.__class__.__name__, key, mode,
                    )
                else:
                    logger.warning(
                        "%s: %s resolved trading_mode=%s (not paper/paper_route) -- "
                        "WILL perform a real close on graceful shutdown.",
                        self.__class__.__name__, key, mode,
                    )

        results = await asyncio.gather(
            *[self._liquidate_book(book, key, reason=_reason, skip_close=skip_flags.get(key, False))
              for key, book in books_snapshot],
            return_exceptions=True,
        )
        for key, res in zip([k for k, _ in books_snapshot], results):
            if isinstance(res, Exception):
                logger.error("%s: liquidation failed for %s: %s", self.__class__.__name__, key, res)
        logger.warning("%s: liquidation complete.", self.__class__.__name__)

    async def _liquidate_book(self, book: Any, key: Key, reason: str = "kill_switch",
                               skip_close: bool = False) -> None:
        """Close any open position on ``book`` with the given reason and stop its tasks.
        skip_close=True (see liquidate_all's own docstring) skips ONLY the
        real book.liquidate() close call -- the book is still stopped normally."""
        if skip_close:
            logger.info("%s: %s -- skip_close=True, not calling book.liquidate().",
                        self.__class__.__name__, key)
        elif hasattr(book, "liquidate"):
            try:
                await book.liquidate(reason)
            except Exception as exc:
                logger.warning("%s: book.liquidate(%s) failed: %s", self.__class__.__name__, key, exc)
        try:
            coro_fn = self._resolve_async_stop(book)
            if coro_fn is not None:
                await coro_fn()
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

    def _resolve_async_stop(self, book: Any) -> Optional[Callable[[], Any]]:
        """Return a zero-arg callable that, when called, returns the awaitable
        that fully stops ``book`` (proper task-completion wait + EventBus
        unsubscribe) -- or None if the book only exposes a synchronous stop().

        Prefers ``stop_async()`` (the AbstractStrategyBook convention used by
        SellStraddle/D1Trap/FVG/V4Cascade) but ALSO detects any other
        coroutine-function named ``stop()`` -- not every book class extends
        AbstractStrategyBook or follows its naming convention (e.g.
        strategies/fno_positional/book.py's FnOPositionalBook is a plain
        class with `async def stop(self)`, no `stop_async` at all). Without
        this fallback, such a book's cleanup coroutine would be called
        synchronously (`book.stop()`), producing an un-awaited coroutine
        object that never actually runs -- its cancel/await logic silently
        never executes, and the key is never tracked in self._stopping, so
        the spawn-race guard this whole class exists for would not apply to
        it at all."""
        stop_async = getattr(book, "stop_async", None)
        if callable(stop_async):
            return stop_async
        stop_fn = getattr(book, "stop", None)
        if inspect.iscoroutinefunction(stop_fn):
            return stop_fn
        return None

    def _stop_book(self, book: Any, key: Optional[Key] = None) -> None:
        """Stop a book being removed. Prefers the book's async stop method
        (see _resolve_async_stop) over an incomplete sync stop(), scheduling
        it as a tracked background task so a respawn of the same key can wait
        for it to actually finish instead of running a second live instance
        alongside a not-yet-dead one."""
        coro_fn = self._resolve_async_stop(book)
        if coro_fn is not None:
            task = asyncio.create_task(self._run_stop_async(coro_fn, key))
            if key is not None:
                self._stopping[key] = task
        else:
            try:
                book.stop()
            except Exception:
                pass

    async def _run_stop_async(self, coro_fn: Callable[[], Any], key: Optional[Key]) -> None:
        try:
            await coro_fn()
        except Exception as exc:
            logger.warning("%s: async stop failed for %s: %s",
                            self.__class__.__name__, key, exc)
        finally:
            if key is not None:
                self._stopping.pop(key, None)

    async def _run_liquidate_and_stop(self, book: Any, key: Key, reason: str) -> None:
        """Same tracked-task discipline as _run_stop_async, but for a book that
        still has an open position and must be liquidated (not just stopped)
        before it's gone. Registered in self._stopping so the spawn loop
        defers a replacement for this key until the real broker-flatten +
        stop_async() has actually finished -- a book carrying a live position
        is exactly the highest-stakes case for the duplicate-instance race
        this class exists to prevent."""
        try:
            await self._liquidate_book(book, key, reason=reason)
        finally:
            self._stopping.pop(key, None)

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

    def _enable_chain(self, underlying: str) -> None:
        """Ensure the option chain is subscribed for ``underlying`` before a
        book starts trading it. Shared by every manager's _spawn_book -- was
        previously copy-pasted per-manager (2026-08-03 fix, applied 3
        separate times as new strategies hit the same missing-chain bug)."""
        if self._rebalancer is not None and hasattr(self._rebalancer, "enable_chain"):
            self._rebalancer.enable_chain(underlying)

    def _reconcile(self) -> None:
        try:
            wanted = self._wanted()
        except Exception as exc:
            logger.warning(
                "%s: _wanted() failed (%s) — skipping this reconcile tick, "
                "existing books left unchanged.",
                self.__class__.__name__, exc,
            )
            return
        if hasattr(self, "_log_reconcile"):
            self._log_reconcile(wanted, self._books)

        # Snapshot which keys already had a live book BEFORE this tick's spawn
        # loop runs. The config-change respawn loop below must only consider
        # these -- never a book the spawn loop just created in this same
        # tick -- otherwise an always-true _should_respawn() (or one that
        # happens to re-trigger on the freshly spawned book's initial state)
        # would immediately tear down the replacement we just spawned,
        # defeating the whole stop-then-later-spawn discipline.
        pre_existing_keys = set(self._books)

        # Spawn books for newly-wanted keys -- but not if the previous
        # instance for this key is still finishing its async stop.
        for key in set(wanted) - set(self._books):
            if key in self._stopping:
                continue
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
                task = asyncio.create_task(self._run_liquidate_and_stop(book, key, reason="deployment_stop"))
                self._stopping[key] = task
            else:
                self._stop_book(book, key)
            self._log_stopped(key)

        # Re-spawn on configuration change only when flat. The replacement is
        # NOT spawned in this same tick -- removing it from self._books here
        # makes it fall into the spawn loop above on a LATER reconcile tick,
        # once self._stopping no longer holds this key (i.e. the old
        # instance's stop_async() has actually finished). This costs one
        # extra reconcile_sec (~5s) of latency on a lot_multiplier-style
        # config change, in exchange for guaranteeing the old and new
        # instances are never both alive at once.
        for key, value in list(wanted.items()):
            if key not in pre_existing_keys:
                continue
            if key in self._stopping:
                continue
            book = self._books.get(key)
            if book is None:
                continue
            if not self._is_flat(book):
                continue
            if not self._should_respawn(book, value):
                continue
            self._books.pop(key, None)
            self._stop_book(book, key)
            self._log_stopped(key)

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
