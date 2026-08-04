"""
Covers two fixes to strategies/core/book_manager.py's reconcile loop:
1. A _wanted() exception must skip the tick entirely (no books touched),
   not be treated as "nothing is wanted" (which would tear every book down).
2. A book being removed/respawned must not let a new instance for the same
   key start before the old instance's stop_async() has actually finished
   (see Task 2's tests, appended to this same file).

Uses a minimal fake manager/book instead of any real strategy, so this is a
regression guard on the shared base class itself, independent of
SellStraddle/FnO/D1Trap/FVG/V4Cascade specifics.
"""
import asyncio
import pytest

from strategies.core.book_manager import StrategyBookManager


class _FakeBook:
    def __init__(self, key):
        self.key = key
        self.started = False
        self.stop_async_called = False
        self._position = None  # flat by default

    def start(self):
        self.started = True

    async def stop_async(self):
        self.stop_async_called = True


class _FlakyWantedManager(StrategyBookManager):
    """_wanted() raises on demand, to simulate a transient DB hiccup."""

    def __init__(self):
        super().__init__(bus=None, cfg=None, client_db=None, monitored_indices=[])
        self.should_raise = False
        self.spawned_books = {}

    def _wanted(self):
        if self.should_raise:
            raise RuntimeError("simulated DB read failure")
        return {("c1", "b1", "NIFTY"): 1}

    def _spawn_book(self, key, value):
        book = _FakeBook(key)
        self.spawned_books[key] = book
        return book


def test_wanted_exception_leaves_existing_books_untouched():
    mgr = _FlakyWantedManager()

    # First tick: normal, spawns the book.
    mgr._reconcile()
    assert ("c1", "b1", "NIFTY") in mgr._books
    original_book = mgr._books[("c1", "b1", "NIFTY")]
    assert original_book.started is True

    # Second tick: DB read fails.
    mgr.should_raise = True
    mgr._reconcile()  # must not raise, must not touch self._books

    # The book from tick 1 must still be there, completely untouched --
    # not stopped, not replaced.
    assert mgr._books[("c1", "b1", "NIFTY")] is original_book
    assert original_book.stop_async_called is False


def test_wanted_exception_does_not_prevent_recovery_next_tick():
    mgr = _FlakyWantedManager()
    mgr.should_raise = True
    mgr._reconcile()
    assert mgr._books == {}  # nothing wanted yet, DB was down, nothing spawned -- fine, none existed

    mgr.should_raise = False
    mgr._reconcile()
    assert ("c1", "b1", "NIFTY") in mgr._books
