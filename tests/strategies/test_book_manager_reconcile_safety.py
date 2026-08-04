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


class _ControllableStopBook(_FakeBook):
    """Like _FakeBook, but stop_async() blocks until a test-controlled event is set,
    so the test can observe the window where the old instance isn't dead yet."""

    def __init__(self, key):
        super().__init__(key)
        self.stop_release = asyncio.Event()

    async def stop_async(self):
        await self.stop_release.wait()
        self.stop_async_called = True


class _RespawnableManager(StrategyBookManager):
    def __init__(self):
        super().__init__(bus=None, cfg=None, client_db=None, monitored_indices=[])
        self._wanted_keys = {("c1", "b1", "NIFTY"): 1}
        self.spawned = []  # every book ever created, in order

    def _wanted(self):
        return dict(self._wanted_keys)

    def _spawn_book(self, key, value):
        book = _ControllableStopBook(key)
        self.spawned.append(book)
        return book


@pytest.mark.asyncio
async def test_respawn_waits_for_old_instance_stop_async_to_finish():
    mgr = _RespawnableManager()

    # Tick 1: spawn the first instance.
    mgr._reconcile()
    assert len(mgr.spawned) == 1
    old_book = mgr.spawned[0]
    assert old_book.started is True

    # Simulate the key disappearing (deployment stopped) then immediately
    # reappearing (deployment restarted) -- exactly the flap this fix guards
    # against. Tick 2: key no longer wanted -> old_book gets stopped.
    mgr._wanted_keys = {}
    mgr._reconcile()
    assert ("c1", "b1", "NIFTY") not in mgr._books
    # stop_async was scheduled but is BLOCKED (stop_release not set yet) --
    # the old book is not actually dead yet.
    assert old_book.stop_async_called is False

    # Tick 3: key wanted again, but old instance's cleanup hasn't finished.
    # Must NOT spawn a second live instance yet.
    mgr._wanted_keys = {("c1", "b1", "NIFTY"): 1}
    mgr._reconcile()
    assert len(mgr.spawned) == 1, "must not create a second instance while the first is still stopping"
    assert ("c1", "b1", "NIFTY") not in mgr._books

    # Now let the old instance's stop_async() actually complete.
    old_book.stop_release.set()
    await asyncio.sleep(0)  # let the scheduled stop_async task run to completion
    await asyncio.sleep(0)

    # Tick 4: NOW a replacement may be spawned.
    mgr._reconcile()
    assert len(mgr.spawned) == 2
    assert ("c1", "b1", "NIFTY") in mgr._books
    assert mgr._books[("c1", "b1", "NIFTY")] is mgr.spawned[1]


@pytest.mark.asyncio
async def test_config_change_respawn_also_waits_for_old_stop():
    """The lot_multiplier-changed respawn path must go through the same
    stop-then-later-spawn discipline as a plain removal+re-add."""
    mgr = _RespawnableManager()
    mgr._reconcile()
    old_book = mgr.spawned[0]

    # Force a respawn via _should_respawn.
    mgr._should_respawn = lambda book, value: True
    mgr._reconcile()

    # Old instance removed from _books, stop_async scheduled but blocked.
    assert ("c1", "b1", "NIFTY") not in mgr._books
    assert len(mgr.spawned) == 1, "must not spawn the replacement in the same tick as the stop"

    old_book.stop_release.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    mgr._reconcile()
    assert len(mgr.spawned) == 2
    assert mgr._books[("c1", "b1", "NIFTY")] is mgr.spawned[1]


class _ControllableLiquidateBook(_FakeBook):
    """Like _FakeBook, but starts with an OPEN position and a controllable
    async liquidate() that blocks until a test-controlled event is set --
    exercises the not-flat removal branch of _reconcile(), which must
    liquidate (real broker-flatten) before the book is actually gone. This is
    the highest-stakes variant of the duplicate-instance race: a live
    position, not just an idle book."""

    def __init__(self, key):
        super().__init__(key)
        self._position = {"open": True}  # not flat
        self.liquidate_release = asyncio.Event()
        self.liquidate_called = False

    async def liquidate(self, reason):
        await self.liquidate_release.wait()
        self.liquidate_called = True
        self._position = None  # now flat, post-liquidation

    async def stop_async(self):
        self.stop_async_called = True


class _LiquidatableManager(StrategyBookManager):
    def __init__(self):
        super().__init__(bus=None, cfg=None, client_db=None, monitored_indices=[])
        self._wanted_keys = {("c1", "b1", "NIFTY"): 1}
        self.spawned = []  # every book ever created, in order

    def _wanted(self):
        return dict(self._wanted_keys)

    def _spawn_book(self, key, value):
        book = _ControllableLiquidateBook(key)
        self.spawned.append(book)
        return book


@pytest.mark.asyncio
async def test_spawn_waits_for_open_position_liquidation_to_finish():
    """The not-flat removal branch (asyncio.create_task(_liquidate_book))
    must be tracked in self._stopping exactly like the flat/_stop_book path,
    so a book carrying an OPEN POSITION can't get a duplicate live instance
    spawned while the real liquidation (broker-flatten + stop_async) is still
    in flight."""
    mgr = _LiquidatableManager()

    # Tick 1: spawn the first instance, holding an open position.
    mgr._reconcile()
    assert len(mgr.spawned) == 1
    old_book = mgr.spawned[0]
    assert old_book.started is True
    assert not mgr._is_flat(old_book)

    # Tick 2: key no longer wanted (deployment stopped) while the position is
    # still open -> not-flat branch schedules liquidation, tracked.
    mgr._wanted_keys = {}
    mgr._reconcile()
    assert ("c1", "b1", "NIFTY") not in mgr._books
    assert ("c1", "b1", "NIFTY") in mgr._stopping
    # Liquidation was scheduled but is BLOCKED (release not set yet) -- the
    # old book's position has not actually been flattened yet.
    assert old_book.liquidate_called is False

    # Tick 3: key wanted again immediately (deployment restarted) -- must NOT
    # spawn a second live instance while the old one's open position is still
    # being liquidated.
    mgr._wanted_keys = {("c1", "b1", "NIFTY"): 1}
    mgr._reconcile()
    assert len(mgr.spawned) == 1, "must not spawn a duplicate while the old instance's open position is still being liquidated"
    assert ("c1", "b1", "NIFTY") not in mgr._books

    # Now let the liquidation (and the old instance's stop_async) actually finish.
    old_book.liquidate_release.set()
    await asyncio.sleep(0)  # let the scheduled task run to completion
    await asyncio.sleep(0)
    assert old_book.liquidate_called is True
    assert ("c1", "b1", "NIFTY") not in mgr._stopping

    # Tick 4: NOW a replacement may be spawned.
    mgr._reconcile()
    assert len(mgr.spawned) == 2
    assert ("c1", "b1", "NIFTY") in mgr._books
    assert mgr._books[("c1", "b1", "NIFTY")] is mgr.spawned[1]
