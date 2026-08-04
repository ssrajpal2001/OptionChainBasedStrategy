# Book-Manager Flap-Restart + Duplicate-Instance Race Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop `StrategyBookManager` (the shared reconcile loop behind every live strategy — SellStraddle, FnO Positional, D1Trap, FVG, V4 Cascade) from (1) tearing a book down and rebuilding it from scratch on a transient DB read hiccup, and (2) ever allowing two live instances of the same `(client, binding, underlying)` book to run concurrently, both reacting to the same ticks/fills/orders.

**Architecture:** Two independent, additive fixes to `strategies/core/book_manager.py`, the shared base class every per-strategy manager subclasses:
1. `_reconcile()` currently calls `self._wanted()` unguarded; every subclass's `_wanted()` independently wraps its own DB read in `try/except Exception: return {}` (or equivalent), so a single transient SQLite hiccup makes the whole book set look empty for that tick, and the reconcile loop tears every book down. Fix: remove the per-subclass swallow so the exception propagates, and let `_reconcile()` itself catch it once, centrally, and skip the tick entirely (no spawn, no stop, no respawn) rather than treating "couldn't read the DB" as "nothing should be running."
2. `_stop_book()` defaults to calling the target book's `stop()` — which, per every book's own base class (`strategies/core/base_book.py`), only cancels tasks without waiting for them to finish and never unsubscribes from the EventBus; the complete cleanup is `stop_async()`, which nothing in the reconcile loop ever calls. Fix: make `_stop_book()` schedule `stop_async()` as a tracked background task, and make the spawn logic refuse to create a replacement for a key whose old instance's stop is still in flight — deferring the respawn to a later tick instead of racing.

**Tech Stack:** Python 3, asyncio, pytest, existing `EventBus` pub-sub, existing `ClientDB` sync accessors.

## Global Constraints

- No new dependencies — stdlib + existing project modules only.
- No `time.sleep` — async-only I/O, matching project-wide rule.
- Never let a DB read failure be interpreted as "no books should be running" — a failed reconcile tick must leave existing books completely untouched.
- Never allow two live instances of the same `(client_id, binding_id, underlying)` key to be started before the previous instance's async cleanup (`stop_async()`) has actually completed.
- This changes timing for the existing "re-spawn on config change" path (e.g. `lot_multiplier` edits): a respawn now takes one extra reconcile tick (~5s) to actually happen, since the new instance is deferred until the old one's `stop_async()` finishes. This is an intentional, acceptable trade for correctness — call it out explicitly in any test that previously asserted same-tick respawn.
- All existing tests (`python -m pytest tests/ -q`) must keep passing after every task, adapted where they assumed the old same-tick respawn timing. Accepted pre-existing baseline: 3 failures unrelated to this work — `test_upstox_converts_fyers_mcx_option_symbol`, `test_pair_indicators_combined_close_and_vwap`, `test_slope_and_vwap_ignore_seed_atp_contamination`.

---

## File Structure

- **Modify** `strategies/core/book_manager.py` — the shared reconcile loop: centralize the `_wanted()` exception guard, rework `_stop_book`/spawn/respawn to be stop-then-later-spawn instead of stop-then-immediately-spawn.
- **Modify** `strategies/fno_positional/book_manager.py` — remove the `_wanted()` swallow.
- **Modify** `strategies/straddle_book_manager.py` — remove the `_wanted()` swallow.
- **Modify** `strategies/d1_trap_option/book_manager.py` — remove the per-strategy-name `_wanted()` swallow.
- **Modify** `strategies/fvg/book_manager.py` — remove the `_wanted()` swallow.
- **Modify** `strategies/v4_cascade_book_manager.py` — remove the `_wanted()` swallow.
- **Test** `tests/strategies/test_book_manager_reconcile_safety.py` — new file, covers both fixes against the generic base class with a minimal fake book/DB, independent of any specific strategy.
- **Test** existing per-manager test files (`tests/strategies/test_straddle_book_manager.py` and any FnO/D1Trap/FVG/V4Cascade manager tests) — audit and update any assertion that relied on same-tick respawn.

---

## Task 1: Centralize the `_wanted()` failure guard in the base class, remove the per-subclass swallows

**Files:**
- Modify: `strategies/core/book_manager.py:172-176` (top of `_reconcile`)
- Modify: `strategies/fno_positional/book_manager.py:29-34`
- Modify: `strategies/straddle_book_manager.py:41-50` (exact line numbers may drift slightly — locate by the method name and DB call shown below)
- Modify: `strategies/d1_trap_option/book_manager.py:46-53`
- Modify: `strategies/fvg/book_manager.py:54-59`
- Modify: `strategies/v4_cascade_book_manager.py:96-101`
- Test: `tests/strategies/test_book_manager_reconcile_safety.py`

**Interfaces:**
- Produces: `StrategyBookManager._reconcile()` no longer assumes `self._wanted()` always succeeds — a raised exception from it is caught once, logged, and the tick is skipped (`self._books` untouched). This is the contract every subclass's `_wanted()` must now honor: raise on failure, don't swallow to `{}`.

- [ ] **Step 1: Write the failing test for the centralized guard**

```python
# tests/strategies/test_book_manager_reconcile_safety.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_book_manager_reconcile_safety.py -v`
Expected: FAIL — `test_wanted_exception_leaves_existing_books_untouched` fails because today's `_reconcile()` doesn't catch the exception from `_wanted()` (the base class's `run()` loop catches it one level up, but that means the ENTIRE `_reconcile()` call aborts including no-op — actually in the CURRENT code, calling `mgr._reconcile()` directly, uncaught, will raise `RuntimeError` right out of the test call, so the test fails with an unhandled exception, not a clean assertion failure. Confirms the guard doesn't exist yet.)

- [ ] **Step 3: Add the centralized guard to `_reconcile()`**

In `strategies/core/book_manager.py`, replace the top of `_reconcile()` (currently line 172-176):

```python
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
```

(Everything below this — the three loops — is unchanged for this step; Task 2 modifies them.)

- [ ] **Step 4: Remove the per-subclass swallows so exceptions actually propagate**

In `strategies/fno_positional/book_manager.py`, find:
```python
    def _wanted(self) -> Dict[tuple, int]:
        wanted: Dict[tuple, int] = {}
        try:
            rows = self._db.get_running_deployments_by_strategy_sync(_STRATEGY_NAME)
        except Exception:
            return wanted
        for d in rows:
```
Replace with:
```python
    def _wanted(self) -> Dict[tuple, int]:
        wanted: Dict[tuple, int] = {}
        rows = self._db.get_running_deployments_by_strategy_sync(_STRATEGY_NAME)
        for d in rows:
```

In `strategies/straddle_book_manager.py`, find the equivalent block (method `_wanted`, calls `self._db.get_running_straddle_deployments_sync()` inside a `try/except Exception: return wanted`) and apply the same change — delete the `try:`/`except Exception: return wanted` wrapper, leaving the bare `rows = self._db.get_running_straddle_deployments_sync()` call.

In `strategies/d1_trap_option/book_manager.py`, find:
```python
        for strategy_name in _STRATEGY_NAMES:
            try:
                rows = self._db.get_running_deployments_by_strategy_sync(strategy_name)
            except Exception:
                continue
            for d in rows or []:
```
Replace with:
```python
        for strategy_name in _STRATEGY_NAMES:
            rows = self._db.get_running_deployments_by_strategy_sync(strategy_name)
            for d in rows or []:
```
(A failure on any one `strategy_name` now aborts the whole `_wanted()` call for this manager, which the base class's new guard turns into "skip this tick" — safer than silently proceeding with a partial, possibly-stale result for the other strategy names.)

In `strategies/fvg/book_manager.py`, find:
```python
        try:
            rows = self._db.get_running_deployments_by_strategy_sync(_STRATEGY_NAME)
        except Exception:
            rows = []
```
Replace with:
```python
        rows = self._db.get_running_deployments_by_strategy_sync(_STRATEGY_NAME)
```

In `strategies/v4_cascade_book_manager.py`, find:
```python
    def _wanted(self) -> Dict[tuple, int]:
        wanted: Dict[tuple, int] = {}
        try:
            rows = self._db.get_running_deployments_by_strategy_sync("v4_cascade")
        except Exception:
            return wanted
        for d in rows:
```
Replace with:
```python
    def _wanted(self) -> Dict[tuple, int]:
        wanted: Dict[tuple, int] = {}
        rows = self._db.get_running_deployments_by_strategy_sync("v4_cascade")
        for d in rows:
```

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_book_manager_reconcile_safety.py -v`
Expected: PASS (2 tests)

- [ ] **Step 6: Run the full test suite**

Run: `python -m pytest tests/ -q`
Expected: same baseline as before (3 accepted pre-existing failures) plus the 2 new tests passing. If any existing test for FnO/Straddle/D1Trap/FVG/V4Cascade managers directly tested the swallow-to-empty behavior (search first: `grep -rn "_wanted" tests/strategies/ tests/execution/` to find them), update it to expect the exception to propagate out of `_wanted()` instead, or to expect `_reconcile()` to leave books untouched on a DB-mock-raises scenario — whichever the specific test was actually checking.

- [ ] **Step 7: Commit**

```bash
git add strategies/core/book_manager.py strategies/fno_positional/book_manager.py strategies/straddle_book_manager.py strategies/d1_trap_option/book_manager.py strategies/fvg/book_manager.py strategies/v4_cascade_book_manager.py tests/strategies/test_book_manager_reconcile_safety.py
git commit -m "Book managers: a DB read failure must skip the reconcile tick, not tear down every running book"
```

---

## Task 2: Stop-then-later-spawn — never let two live instances of the same key coexist

**Files:**
- Modify: `strategies/core/book_manager.py` (`_stop_book`, `_reconcile`'s stop/respawn loops, `__init__`)
- Test: `tests/strategies/test_book_manager_reconcile_safety.py` (append to the file from Task 1)

**Interfaces:**
- Consumes: `AbstractStrategyBook.stop_async()` (already exists on every book via `strategies/core/base_book.py:53-61`) — an async method that cancels and awaits all tasks, then unsubscribes every EventBus queue.
- Produces: `StrategyBookManager._stopping: Dict[Key, asyncio.Task]` — tracks in-flight async stops; the spawn loop consults it to defer creating a replacement for a key still being cleaned up.

- [ ] **Step 1: Write the failing test**

Append to `tests/strategies/test_book_manager_reconcile_safety.py`:

```python
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
```

Check `pytest.ini`/existing async test patterns first (`grep -n "asyncio_mode\|pytest.mark.asyncio" pytest.ini tests/execution/test_broker_resolve.py`) — this project uses `pytest-asyncio` in STRICT mode per an earlier session's finding, so `@pytest.mark.asyncio` is the correct, already-working pattern here; use it as shown above.

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_book_manager_reconcile_safety.py -v`
Expected: FAIL — `test_respawn_waits_for_old_instance_stop_async_to_finish` and `test_config_change_respawn_also_waits_for_old_stop` both fail at `assert len(mgr.spawned) == 1` (today's code spawns the replacement immediately, giving `len(mgr.spawned) == 2` already at tick 3 / immediately after the forced respawn).

- [ ] **Step 3: Implement the fix**

In `strategies/core/book_manager.py`:

Add to `__init__` (after `self._books: Dict[Key, Any] = {}` on line 45):
```python
        self._stopping: Dict[Key, asyncio.Task] = {}
```

Replace `_stop_book` (currently lines 145-150):
```python
    def _stop_book(self, book: Any, key: Optional[Key] = None) -> None:
        """Stop a book being removed. Prefers the book's async stop_async()
        (proper task-completion wait + EventBus unsubscribe) over the
        incomplete sync stop(), scheduling it as a tracked background task so
        a respawn of the same key can wait for it to actually finish instead
        of running a second live instance alongside a not-yet-dead one."""
        stop_async = getattr(book, "stop_async", None)
        if callable(stop_async):
            task = asyncio.create_task(self._run_stop_async(book, key))
            if key is not None:
                self._stopping[key] = task
        else:
            try:
                book.stop()
            except Exception:
                pass

    async def _run_stop_async(self, book: Any, key: Optional[Key]) -> None:
        try:
            await book.stop_async()
        except Exception as exc:
            logger.warning("%s: stop_async failed for %s: %s",
                            self.__class__.__name__, key, exc)
        finally:
            if key is not None:
                self._stopping.pop(key, None)
```

Replace the three loops in `_reconcile()` (currently lines 178-219, i.e. everything after the `wanted`/`_log_reconcile` block from Task 1):
```python
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
                asyncio.create_task(self._liquidate_book(book, key, reason="deployment_stop"))
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
```

(This drops `_log_respawned` from the live call path since the respawn is now just a stop this tick + an ordinary spawn next tick, which already calls `_log_spawned`. Leave the `_log_respawned` method defined on the base class for backward compatibility with any subclass override — just note in your report that it's no longer called from `_reconcile()` directly. Do not delete the method itself, some subclass tests may still reference it.)

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_book_manager_reconcile_safety.py -v`
Expected: PASS (4 tests total: 2 from Task 1, 2 from this task)

- [ ] **Step 5: Run the full test suite, paying close attention to respawn-timing assumptions**

Run: `python -m pytest tests/ -q`

Search first for any existing test asserting same-tick respawn: `grep -rln "_should_respawn\|lot_multiplier.*respawn\|re-spawn" tests/`. For each hit, read it and update any assertion that expected the new book to exist immediately after one `_reconcile()` call following a config change — it now needs: `_reconcile()` (removes old, schedules stop) → let the scheduled stop task actually run (e.g. `await asyncio.sleep(0)` twice in an async test, or drive the fake book's `stop_async` to completion synchronously if it's not artificially blocked) → `_reconcile()` again (spawns the replacement). Confirm no new failures beyond the accepted 3-failure baseline.

- [ ] **Step 6: Commit**

```bash
git add strategies/core/book_manager.py tests/strategies/test_book_manager_reconcile_safety.py
git commit -m "Book managers: never spawn a replacement instance before the old one's stop_async() finishes"
```

(If Step 5 required updating other test files, include them in this commit too.)

---

## Self-Review

**Spec coverage:** DB-read-failure-skips-tick fix (Task 1) ✅; two-live-instances-never-coexist fix (Task 2) ✅; applied to all 5 live/near-live strategy managers via the shared base class plus each subclass's `_wanted()` ✅.

**Placeholder scan:** no TBD/TODO markers; both tests contain real, runnable assertions against a minimal fake manager rather than a vague "write tests for the above."

**Type consistency:** `_stop_book(self, book: Any, key: Optional[Key] = None)` signature is introduced once in Task 2 and is the only place it's defined; `self._stopping: Dict[Key, asyncio.Task]` is introduced in `__init__` and consumed identically in both the spawn loop and the respawn loop.
