# Code Audit Cleanup, Reuse, and D1Trap/FVG Fill-Confirmation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Work through every finding from the 2026-08-05 pre-commercial code audit, in priority order: safe mechanical dedup first, then the two decided-on features (FVG persistence, `can_trade()` wired up everywhere), then the one real safety gap the audit surfaced — D1Trap-BearOnly and FVG have **no fill-confirmation feedback loop at all** (their bridges never publish a fill/abort event; their books mutate state optimistically and never learn whether an order actually reached the broker) — closing with a shared-base extraction of the two now-identical bridges.

**Architecture:** Tasks 1-4 are pure, behavior-preserving deduplication/dead-code removal — no strategy decision logic changes. Task 5 adds position persistence to FVG using the existing `PositionStoreMixin` (already used by SellStraddle). Tasks 6-7 are the substantial ones: give D1Trap-BearOnly and FVG the same confirm-then-finalize architecture SellStraddle already has (`strategies/sell_straddle/exits.py`'s `_close_position`/`_close_leg`, reviewed and approved earlier in this project) — dispatch an order, wait for a real fill/abort event, only mutate persisted state on confirmation. Task 8 extracts the now-fully-shared D1Trap/FVG bridge logic into one base class, done last so the extraction captures the final, correct behavior rather than something that would need redoing.

**Tech Stack:** Python 3, asyncio, pytest, existing `EventBus`/`Topic` pub-sub, existing `PositionStoreMixin` (`strategies/core/position.py`), existing `data_layer/position_store.py`.

## Global Constraints

- No new dependencies — stdlib + existing project modules only.
- No `time.sleep` — async-only I/O.
- Tasks 1-5 and 8 must be **behavior-preserving** — no change to what orders get placed, when, or at what price. They are refactors/additions, not strategy logic changes.
- Tasks 6-7 **do** change behavior (a broker-unreachable EXIT now leaves the position open instead of silently believing it closed) — this is the intended fix, not a regression, but must be called out clearly in commit messages and PR-equivalent review, since it's the one place in this plan where "what happens on a failure" genuinely changes.
- All existing tests (`python -m pytest tests/ -q`) must keep passing after every task. Accepted pre-existing baseline: 3 failures unrelated to this work — `test_upstox_converts_fyers_mcx_option_symbol`, `test_pair_indicators_combined_close_and_vwap`, `test_slope_and_vwap_ignore_seed_atp_contamination`. A 4th, wall-clock-timing-sensitive test (`test_v4_cascade_session_open_wait`) has been observed to flake independently of any code change in this project — don't chase it if it flakes and nothing else is red.
- Mirror existing patterns exactly where one already exists and is proven (Task 6/7 must mirror `strategies/sell_straddle/exits.py`'s confirm-then-finalize shape and `execution_bridge/cascade_bridge.py`'s `_abort()` shape — both already built, reviewed, and live in this codebase — rather than inventing a new shape).

---

## File Structure

- **Modify** `strategies/hourly_breakout/book_manager.py` — remove the `_wanted()` DB-exception swallow (Task 1).
- **Delete** `fix_dashboard.py` (Task 2).
- **Modify** `strategies/core/book_manager.py` — add a shared `_enable_chain(underlying)` helper (Task 3).
- **Modify** `strategies/straddle_book_manager.py`, `strategies/d1_trap_option/book_manager.py`, `strategies/v4_cascade_book_manager.py`, `strategies/fvg/book_manager.py` — call the new shared helper instead of each's own copy (Task 3).
- **Modify** `strategies/core/gate.py` — fix/clarify the `can_trade()` gate semantics (Task 4).
- **Modify** `execution_bridge/straddle_bridge.py`, `execution_bridge/d1_trap_bridge.py`, `execution_bridge/fvg_bridge.py`, `execution_bridge/cascade_bridge.py`, `strategies/fno_positional/book_manager.py` — replace each's inline deployment-matching predicate with `gate.py::can_trade()` (Task 4).
- **Modify** `strategies/fvg/engine.py` — add `PositionStoreMixin`-backed persistence (Task 5).
- **Modify** `execution_bridge/d1_trap_bridge.py`, `strategies/d1_trap_option/bear_only_book.py` — fill-confirmation feedback loop (Task 6).
- **Modify** `execution_bridge/fvg_bridge.py`, `strategies/fvg/engine.py` — fill-confirmation feedback loop (Task 7).
- **Create** `execution_bridge/option_buyer_bridge_base.py` — shared base for D1Trap/FVG bridges (Task 8).
- **Modify** `execution_bridge/d1_trap_bridge.py`, `execution_bridge/fvg_bridge.py` — become thin subclasses of the new base (Task 8).

---

## Task 1: Fix `hourly_breakout` manager's swallowed DB exception

Identical to a bug already fixed today in the other 5 strategy managers (SellStraddle, FnO, D1Trap, FVG, V4Cascade) — this one was outside that session's named scope and got missed.

**Files:**
- Modify: `strategies/hourly_breakout/book_manager.py:23-28`
- Test: `tests/strategies/` (check for an existing hourly_breakout manager test file first: `grep -rln "hourly_breakout" tests/strategies/`)

- [ ] **Step 1: Confirm current code**

Run: `grep -n "_wanted" -A 8 strategies/hourly_breakout/book_manager.py`
Expected to show:
```python
    def _wanted(self) -> Dict[tuple, int]:
        wanted: Dict[tuple, int] = {}
        try:
            rows = self._db.get_running_deployments_by_strategy_sync(_STRATEGY_NAME)
        except Exception:
            return wanted
        for d in rows or []:
```

- [ ] **Step 2: Write the failing test** (or extend an existing hourly_breakout manager test file if one exists — check first)

```python
# tests/strategies/test_hourly_breakout_book_manager_wanted.py
"""hourly_breakout's _wanted() must propagate a DB read failure, not swallow
it into an empty dict -- strategies/core/book_manager.py's _reconcile()
centrally catches this and skips the tick, leaving existing books untouched.
Same contract as the other 5 strategy managers, fixed earlier."""
import pytest
from strategies.hourly_breakout.book_manager import HourlyBreakoutBookManager


class _RaisingDB:
    def get_running_deployments_by_strategy_sync(self, name):
        raise RuntimeError("simulated DB failure")


def test_wanted_propagates_db_exception_instead_of_swallowing():
    mgr = HourlyBreakoutBookManager(bus=None, cfg=None, client_db=_RaisingDB(), monitored_indices=[])
    with pytest.raises(RuntimeError):
        mgr._wanted()
```

(Confirm the real constructor signature first: `grep -n "class HourlyBreakoutBookManager" -A 5 strategies/hourly_breakout/book_manager.py` — adjust the test's constructor call to match if it differs.)

- [ ] **Step 3: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_hourly_breakout_book_manager_wanted.py -v`
Expected: FAIL — `_wanted()` currently catches the `RuntimeError` and returns `{}`, so `pytest.raises(RuntimeError)` doesn't see it.

- [ ] **Step 4: Fix**

```python
    def _wanted(self) -> Dict[tuple, int]:
        wanted: Dict[tuple, int] = {}
        rows = self._db.get_running_deployments_by_strategy_sync(_STRATEGY_NAME)
        for d in rows or []:
```

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_hourly_breakout_book_manager_wanted.py -v`
Expected: PASS

- [ ] **Step 6: Run the full suite**

Run: `python -m pytest tests/ -q`
Expected: baseline + 1 new passing test, no new failures.

- [ ] **Step 7: Commit**

```bash
git add strategies/hourly_breakout/book_manager.py tests/strategies/test_hourly_breakout_book_manager_wanted.py
git commit -m "hourly_breakout manager: propagate _wanted() DB failures instead of swallowing them"
```

---

## Task 2: Delete dead `fix_dashboard.py`

**Files:**
- Delete: `fix_dashboard.py`

- [ ] **Step 1: Confirm it's truly unreferenced**

Run: `grep -rln "fix_dashboard" --include=*.py . | grep -v "^./fix_dashboard.py$"`
Expected: no output (nothing else imports or references it).

- [ ] **Step 2: Delete it**

```bash
git rm fix_dashboard.py
```

- [ ] **Step 3: Run the full suite** (sanity check — should be unaffected, this file was never imported)

Run: `python -m pytest tests/ -q`
Expected: unchanged from baseline.

- [ ] **Step 4: Commit**

```bash
git commit -m "Remove dead fix_dashboard.py (stale pre-2026-07-18 IronCondor/TrapScanner patch script)"
```

---

## Task 3: Extract duplicated `enable_chain()` call into a shared base-class helper

**Files:**
- Modify: `strategies/core/book_manager.py`
- Modify: `strategies/straddle_book_manager.py:83-84`, `strategies/d1_trap_option/book_manager.py:201-202`, `strategies/v4_cascade_book_manager.py:157-158`, `strategies/fvg/book_manager.py:95-96`
- Test: extend `tests/strategies/test_book_manager_reconcile_safety.py` or create a focused new test

**Interfaces:**
- Produces: `StrategyBookManager._enable_chain(underlying: str) -> None` on the base class.

- [ ] **Step 1: Confirm the current duplicated block** in all 4 files

Run: `grep -n "enable_chain" -B3 -A3 strategies/straddle_book_manager.py strategies/d1_trap_option/book_manager.py strategies/v4_cascade_book_manager.py strategies/fvg/book_manager.py`

Confirm each is the same shape:
```python
        if self._rebalancer is not None and hasattr(self._rebalancer, "enable_chain"):
            self._rebalancer.enable_chain(<underlying_var>)
```

- [ ] **Step 2: Write the failing test**

```python
# In tests/strategies/test_book_manager_reconcile_safety.py, append:

def test_enable_chain_helper_calls_rebalancer_when_present():
    from strategies.core.book_manager import StrategyBookManager

    class _FakeRebalancer:
        def __init__(self):
            self.enabled = []
        def enable_chain(self, underlying):
            self.enabled.append(underlying)

    mgr = StrategyBookManager(bus=None, cfg=None, client_db=None, monitored_indices=[])
    mgr._rebalancer = _FakeRebalancer()
    mgr._enable_chain("NIFTY")
    assert mgr._rebalancer.enabled == ["NIFTY"]


def test_enable_chain_helper_noop_when_no_rebalancer():
    from strategies.core.book_manager import StrategyBookManager
    mgr = StrategyBookManager(bus=None, cfg=None, client_db=None, monitored_indices=[])
    mgr._rebalancer = None
    mgr._enable_chain("NIFTY")  # must not raise
```

- [ ] **Step 3: Run to verify it fails**

Run: `python -m pytest tests/strategies/test_book_manager_reconcile_safety.py -k enable_chain -v`
Expected: FAIL — `AttributeError: 'StrategyBookManager' object has no attribute '_enable_chain'`

- [ ] **Step 4: Add the helper to the base class**

In `strategies/core/book_manager.py`, add near `_is_flat`/other protected helpers:
```python
    def _enable_chain(self, underlying: str) -> None:
        """Ensure the option chain is subscribed for ``underlying`` before a
        book starts trading it. Shared by every manager's _spawn_book — was
        previously copy-pasted per-manager (2026-08-03 fix, applied 3
        separate times as new strategies hit the same missing-chain bug)."""
        if self._rebalancer is not None and hasattr(self._rebalancer, "enable_chain"):
            self._rebalancer.enable_chain(underlying)
```

- [ ] **Step 5: Replace each of the 4 duplicated call sites**

In each of `strategies/straddle_book_manager.py`, `strategies/d1_trap_option/book_manager.py`, `strategies/v4_cascade_book_manager.py`, `strategies/fvg/book_manager.py`, replace the duplicated block with:
```python
        self._enable_chain(<same underlying variable already used at that call site>)
```
(Keep whatever local variable name each file already uses for the underlying — don't rename anything else in these files.)

- [ ] **Step 6: Run to verify it passes**

Run: `python -m pytest tests/strategies/test_book_manager_reconcile_safety.py -v`
Expected: all pass.

- [ ] **Step 7: Run the full suite**

Run: `python -m pytest tests/ -q`
Expected: baseline + 2 new tests, no new failures. Specifically re-run any existing straddle/d1_trap/v4_cascade/fvg book manager tests to confirm `enable_chain` still fires (`grep -rln "enable_chain" tests/`).

- [ ] **Step 8: Commit**

```bash
git add strategies/core/book_manager.py strategies/straddle_book_manager.py strategies/d1_trap_option/book_manager.py strategies/v4_cascade_book_manager.py strategies/fvg/book_manager.py tests/strategies/test_book_manager_reconcile_safety.py
git commit -m "Extract duplicated enable_chain() call into a shared StrategyBookManager helper"
```

---

## Task 4: Fix and wire up `gate.py::can_trade()` everywhere

**Investigation required first** — this is not a pure rename. `strategies/core/gate.py::can_trade()` currently checks `binding.get("is_trade_enabled")`, a real DB column (`broker_bindings.is_trade_enabled`, default 1). But the actually-live gating code (`execution_bridge/straddle_bridge.py:444,592`, `strategies/sell_straddle/entries.py:85,108`) checks `binding.get("engine_active")` instead — a **different** column (`data_layer/client_db.py:515-522`'s `set_engine_active`, whose own docstring says "Does NOT touch is_trade_enabled" — confirming these are two independently-toggled concepts, not the same thing under two names). Per CLAUDE.md, SellStraddle's entry gate is described as "Gated on Terminal ON + Trade ON" — "Trade ON" strongly suggests `is_trade_enabled` is meant to matter too, alongside `engine_active` ("Terminal"/"Engine" toggle) and `terminal_connected`.

**Files:**
- Modify: `strategies/core/gate.py`
- Modify: `execution_bridge/straddle_bridge.py`, `execution_bridge/d1_trap_bridge.py`, `execution_bridge/fvg_bridge.py`, `execution_bridge/cascade_bridge.py`, `strategies/fno_positional/book_manager.py`, `strategies/v4_cascade_book_manager.py` (the files with the duplicated deployment-matching inline predicate — confirm each with `grep -n "is_running.*==.*1\|is_running.*== 1" <file>`)
- Test: `tests/strategies/test_gate_can_trade.py` (new, or extend if one already exists — check `tests/strategies/test_trap_can_trade_gate.py` first, an older test file with a similar name may already partially cover this)

**Interfaces:**
- Produces: `can_trade(client_id, binding_id, client_db, strategy_name, underlying) -> bool` (signature unchanged) now correctly checks `terminal_connected AND engine_active AND is_trade_enabled AND a running deployment of this strategy/underlying on this binding`.

- [ ] **Step 1: Trace the dashboard UI toggles to their DB columns**

Read `ui_layer/dashboard_server.py` for the endpoints that call `set_engine_active` and whatever endpoint updates `is_trade_enabled` (`grep -n "set_engine_active\|is_trade_enabled" ui_layer/dashboard_server.py`). Confirm in your task report which UI toggle (Terminal ON/OFF vs Trade ON/OFF, per CLAUDE.md's "Gated on Terminal ON + Trade ON" phrasing) maps to which column. This determines whether `can_trade()` should check one, the other, or both — write your finding down before touching code, since the existing live gates (`engine_active`-only) may themselves be missing a check, not just `gate.py` having a stale one.

- [ ] **Step 2: Check `tests/strategies/test_trap_can_trade_gate.py` and `tests/strategies/test_straddle_entry_gating.py`** (or similarly named existing files — `grep -rln "can_trade\|engine_active.*terminal_connected" tests/`) for any test that already encodes an expectation about which fields matter. Reconcile your Step 1 finding against these before writing new code — if an existing test contradicts your Step 1 finding, flag it in your report rather than silently overriding it.

- [ ] **Step 3: Write the failing test(s)** for the corrected `can_trade()`, covering: terminal disconnected → False; engine_active False → False; is_trade_enabled False → False; no running deployment for this exact binding/strategy/underlying → False; all conditions met → True. Use the existing `_evaluate`/`can_trade` structure in `strategies/core/gate.py` as the shape to test against — mock `client_db.get_bindings_safe_sync`/`get_deployments_sync` the same way `_evaluate` consumes them.

- [ ] **Step 4: Run to verify failures**, then fix `_evaluate()` in `strategies/core/gate.py` to check whichever field(s) Step 1 determined are actually correct (likely adding an `engine_active` check alongside the existing `is_trade_enabled` one, based on the evidence gathered — but let your Step 1 investigation decide this, don't assume).

- [ ] **Step 5: Replace each bridge's/manager's inline deployment-matching predicate** with a call to `can_trade()`. Example shape (adapt to each file's actual variable names — `client_id`/`binding_id`/`ev.underlying` etc. differ slightly per file):
```python
from strategies.core.gate import can_trade
...
if not can_trade(ev.client_id, ev.binding_id, db, "d1_trap_bear_only", ev.underlying):
    logger.warning(...)
    return
```
Do this one file at a time, re-running that file's existing tests after each change before moving to the next, since this touches the live ENTRY gate for every strategy — a mistake here blocks real trading, not just adds noise.

- [ ] **Step 6: Run the full suite**

Run: `python -m pytest tests/ -q`
Expected: baseline + new gate tests, no new failures. Pay special attention to any SellStraddle/D1Trap/FVG/V4Cascade/FnO entry-gating test — this step has the widest blast radius in this whole plan.

- [ ] **Step 7: Commit**

```bash
git add strategies/core/gate.py execution_bridge/straddle_bridge.py execution_bridge/d1_trap_bridge.py execution_bridge/fvg_bridge.py execution_bridge/cascade_bridge.py strategies/fno_positional/book_manager.py strategies/v4_cascade_book_manager.py tests/strategies/test_gate_can_trade.py
git commit -m "Fix and wire up gate.py::can_trade() as the single shared entry gate for all 5 strategies"
```

---

## Task 5: Add position persistence to FVG

**Files:**
- Modify: `strategies/fvg/engine.py`
- Test: `tests/strategies/` (check for an existing FVG engine test file: `grep -rln "FVGStrategy" tests/strategies/`)

**Interfaces:**
- Consumes: `PositionStoreMixin.persist(key, data, product_type)` / `.load(key)` / `.clear(key)` (`strategies/core/position.py`, already built — mixin methods, not free functions).

- [ ] **Step 1: Confirm `FVGStrategy`'s current base classes and position lifecycle**

Run: `grep -n "^class FVGStrategy" strategies/fvg/engine.py` and `grep -n "self\._position\b" strategies/fvg/engine.py` to see every place the position is set/cleared (entry, square-off, and — critically — check if there's a `start()`/`__init__` that should attempt a restore).

- [ ] **Step 2: Write the failing test**

```python
# In whatever FVG engine test file exists, or a new tests/strategies/test_fvg_position_persistence.py:
"""FVG must persist its open position the same way SellStraddle does
(strategies/core/position.py's PositionStoreMixin), so a mid-day restart
restores it instead of silently losing track while the real broker leg
stays open."""
from data_layer import position_store


def test_fvg_persists_position_on_open_and_clears_on_close(tmp_path, monkeypatch):
    # Point position_store at a scratch dir so this test doesn't touch real data/positions/
    monkeypatch.setattr(position_store, "_DIR", str(tmp_path))
    from strategies.fvg.engine import FVGStrategy

    strat = FVGStrategy.__new__(FVGStrategy)
    strat._client_id = "c1"
    strat._binding_id = "b1"
    strat._underlying = "NIFTY"
    # ... construct/set whatever minimal state _open_position's persist call needs ...
    # Call the real position-open path (adapt to the actual method name/signature found in Step 1),
    # then assert position_store.load(strat._persist_key) returns non-None with the right shape.
    # Then call the real square-off path and assert position_store.load(...) returns None.
```

(This test's exact body depends on what Step 1 finds — the brief gives the intent and the scratch-dir isolation technique; adapt the construction calls to the real `_open_position`/`_square_off` signatures.)

- [ ] **Step 3: Run to verify it fails**

Run: `python -m pytest tests/strategies/test_fvg_position_persistence.py -v`
Expected: FAIL — no persistence call happens today.

- [ ] **Step 4: Wire in `PositionStoreMixin`**

Add `PositionStoreMixin` to `FVGStrategy`'s base classes (mirror `strategies/sell_straddle/engine.py:57`'s usage exactly — same import, same mixin ordering relative to other base classes). Add a `_persist_key` property following the established convention (`f"{client_id}_{binding_id}_{underlying}_fvg"`, matching the pattern documented for other strategies: `{client}_{binding}_{und}_sell_straddle`, `{client_id}_{binding_id}_{underlying}_d1_trap_bear_only`). Call `self.persist(self._persist_key, position_dict)` wherever a position opens (right after `self._position` is set), and `self.clear(self._persist_key)` wherever it closes (right after `self._position = None`). On `start()`/`__init__`, attempt `self.load(self._persist_key)` and restore `self._position` if present — mirror how SellStraddle's `start()` restores from its own persisted position (find via `grep -n "def start" -A 20 strategies/sell_straddle/engine.py` for the restore-on-start shape to copy).

- [ ] **Step 5: Run to verify it passes**

Run: `python -m pytest tests/strategies/test_fvg_position_persistence.py -v`

- [ ] **Step 6: Run the full suite**

Run: `python -m pytest tests/ -q`
Expected: baseline + new test, no new failures. Specifically re-run `tests/strategies/test_fvg_detector.py` (the existing 19-test FVG suite) to confirm nothing about FVG's construction broke.

- [ ] **Step 7: Commit**

```bash
git add strategies/fvg/engine.py tests/strategies/test_fvg_position_persistence.py
git commit -m "FVG: persist open position via PositionStoreMixin, restore on restart"
```

---

## Task 6: D1Trap-BearOnly — fill-confirmation feedback loop (confirm-then-finalize)

**This is the highest-value, highest-risk task in this plan.** Confirmed by direct code inspection: `execution_bridge/d1_trap_bridge.py` never publishes anything to `Topic.D1_TRAP_ORDER_FILL` at all — it only calls `self._record_history(...)` (writes to the trade-history ledger) after a paper or live fill attempt, win or lose. `strategies/d1_trap_option/bear_only_book.py`'s `_square_off_leg` (line 1574) removes the leg from `self._positions` and persists that removal (via whatever persistence call it uses today — confirm exact line with `grep -n "position_store\|self\._positions = \[p for p in" strategies/d1_trap_option/bear_only_book.py`) **before** publishing the order (`await self._bus.publish(Topic.D1_TRAP_ORDER_REQUEST, ev)`, line 1591) — and nothing ever tells the book whether that order actually reached the broker.

**Files:**
- Modify: `execution_bridge/d1_trap_bridge.py` — add a fill/abort event dataclass and publish it on both success and terminal-disconnect/broker-unavailable failure.
- Modify: `strategies/d1_trap_option/bear_only_book.py` — subscribe to the new fill topic, rework `_square_off_leg` (and `_enter_leg` if it has the same optimistic-mutation-before-confirmation shape — check) to confirm-then-finalize.
- Test: `tests/execution/test_d1trap_bridge_fail_loud.py` (extend — already exists from earlier work) and a new/extended strategy-level test mirroring `tests/strategies/test_sell_straddle_safety.py`'s `test_close_position_leaves_position_open_when_bridge_reports_exit_aborted`.

**Interfaces:**
- Produces: `Topic.D1_TRAP_ORDER_FILL` now actually gets published to (currently: never). A new `D1TrapFillEvent` dataclass (or reuse/extend whatever's closest — check if one is half-defined anywhere first: `grep -rn "D1TrapFillEvent" .`) with at minimum: `action`, `event_id`, `fill_price`, `qty`, `client_id`, `binding_id`, `exit_failed: bool = False`, `entry_aborted: bool = False` — mirror `CascadeFillEvent`'s field shape (`execution_bridge/cascade_bridge.py:77-104`) exactly, since that's the most recently-reviewed, proven shape for this exact problem.

- [ ] **Step 1: Read the reference implementations fully before writing anything**

Read `strategies/sell_straddle/exits.py`'s `_close_position` and `_close_leg` in full (the confirm-then-finalize pattern: dispatch order → register a waiter in `self._roll_close_waiters[event_id]` → `await asyncio.wait_for(waiter.wait(), timeout=...)` → finalize only on real confirmation, revert nothing on abort/timeout). Read `strategies/sell_straddle/engine.py`'s `_on_fill` EXIT branch (how it signals the waiter). Read `execution_bridge/cascade_bridge.py`'s `_abort()` method in full (how a bridge-side failure gets converted into a fill-shaped event instead of silence). These three pieces, already built and reviewed in this codebase, are the exact template — do not invent a different mechanism.

- [ ] **Step 2: Write the failing tests first**, mirroring `tests/execution/test_straddle_exit_abort.py`'s shape: (a) a bridge-level test asserting `D1_TRAP_ORDER_FILL` is published with `exit_failed=True` when the broker is unavailable/terminal disconnected during a live EXIT (no more silent `return`); (b) a strategy-level test that calls the REAL `_square_off_leg` on a real `D1TrapBearOnlyBook` instance with an open position, captures the dispatched event's `event_id`, feeds a synthetic `exit_failed=True` fill back through the book's new fill-handling method, and asserts the leg is STILL in `self._positions`, unchanged — not the isolated-harness-hand-builds-state anti-pattern that caused a full redo earlier in this project's history; drive it through the real dispatch → real fill-handler round trip.

- [ ] **Step 3: Run to verify both fail**

- [ ] **Step 4: Implement the bridge side** — add the fill-event dataclass, publish it on every fill (paper and live, success and failure) instead of only calling `_record_history`, and add the terminal-disconnected/broker-unavailable abort publish (mirror `cascade_bridge.py`'s `_abort()` exactly, same `await self._abort(ev, routing_failed=True)` shape, adapted to the new event class).

- [ ] **Step 5: Implement the book side** — add a fill-consuming loop (subscribe to `Topic.D1_TRAP_ORDER_FILL`, filter by `isinstance`/underlying same as other strategies' fill loops do) and rework `_square_off_leg` into confirm-then-finalize: compute what's needed, dispatch, register a waiter keyed by event_id (same dict-based pattern as SellStraddle's `_roll_close_waiters`, new dict on this class), await it with a timeout, finalize (remove from `self._positions`, persist, apply any cooldown-equivalent) ONLY on real confirmation; on abort or timeout, leave `self._positions` and its persisted state completely unchanged, log CRITICAL. Check `_enter_leg` (line 1372) for the same optimistic-mutation-before-confirmation shape — if it has it, apply the identical treatment; if entries are already safe (e.g. genuinely fire-and-forget with no state mutation before confirmation), say so explicitly in your report rather than assuming.

- [ ] **Step 6: Run to verify tests pass**

- [ ] **Step 7: Run the full suite**

Run: `python -m pytest tests/ -q`
Expected: baseline + new tests, no new failures. Specifically re-run every existing `d1trap_*`/`bear_only_book` test to confirm the live zone-detection/entry/exit mechanic is unaffected — this task changes WHEN state finalizes, not the trading decisions themselves.

- [ ] **Step 8: Commit**

```bash
git add execution_bridge/d1_trap_bridge.py strategies/d1_trap_option/bear_only_book.py tests/execution/test_d1trap_bridge_fail_loud.py <new test files>
git commit -m "D1Trap-BearOnly: confirm-then-finalize exits, closes the silent-naked-position gap"
```

---

## Task 7: FVG — fill-confirmation feedback loop (confirm-then-finalize)

Same shape as Task 6, applied to `execution_bridge/fvg_bridge.py` + `strategies/fvg/engine.py`. Do this AFTER Task 5 (FVG persistence) so the confirm-then-finalize rework operates on a strategy that already correctly persists/restores its position — the two features compose naturally (a reverted-on-abort position must also stay correctly persisted as open, not just correct in memory).

**Files:**
- Modify: `execution_bridge/fvg_bridge.py`
- Modify: `strategies/fvg/engine.py`
- Test: `tests/execution/test_fvg_bridge_fail_loud.py` (extend — already exists) + a new strategy-level test mirroring Task 6's Step 2(b).

**Interfaces:**
- Consumes: Task 5's `_persist_key`/persist/clear calls in `FVGStrategy` — the confirm-then-finalize rework must call these at the FINALIZE point, not at decision time (i.e. Task 5's "persist on open, clear on close" calls need to move from "immediately when the position variable changes" to "only inside the new fill-confirmed finalize step," exactly mirroring how SellStraddle's `_persist()` call is inside the confirmed branch, not the optimistic one).
- Produces: same `FVGOrderFillEvent`-shaped event as Task 6's `D1TrapFillEvent` (separate class, same field shape — do not share a literal Python class between the two strategies, each bridge keeps its own event type per this codebase's existing per-strategy-event convention, e.g. `CascadeFillEvent`/`StraddleFillEvent` are separate classes despite near-identical shape).

- [ ] **Step 1: Read `strategies/fvg/engine.py`'s `_square_off` (line 737) and `_open_position` (line 629) in full**, plus Task 6's finished `bear_only_book.py` changes as a second, now-proven-in-this-codebase reference alongside SellStraddle's.

- [ ] **Step 2: Write the failing tests**, same two-part shape as Task 6 Step 2, adapted to FVG's real method names (`_open_position`/`_square_off` instead of `_enter_leg`/`_square_off_leg`).

- [ ] **Step 3: Run to verify both fail**

- [ ] **Step 4: Implement the bridge side** — same shape as Task 6 Step 4.

- [ ] **Step 5: Implement the book side** — same shape as Task 6 Step 5, additionally confirming Task 5's persist/clear calls now happen at the confirmed-finalize point, not at optimistic-decision time.

- [ ] **Step 6: Run to verify tests pass**

- [ ] **Step 7: Run the full suite**

Run: `python -m pytest tests/ -q`
Expected: baseline + new tests, no new failures. Re-run `tests/strategies/test_fvg_detector.py` (19 tests) and Task 5's persistence test to confirm both still pass together.

- [ ] **Step 8: Commit**

```bash
git add execution_bridge/fvg_bridge.py strategies/fvg/engine.py tests/execution/test_fvg_bridge_fail_loud.py <new test files>
git commit -m "FVG: confirm-then-finalize exits, closes the silent-naked-position gap"
```

---

## Task 8: Extract the now-fully-duplicated D1Trap/FVG bridges into a shared base

After Tasks 6-7, `execution_bridge/d1_trap_bridge.py` and `execution_bridge/fvg_bridge.py` are (by construction — Task 7 mirrors Task 6 exactly) once again near-identical files, this time including the new fill/abort-publishing logic too. Extract now, once, rather than maintaining two copies going forward.

**Files:**
- Create: `execution_bridge/option_buyer_bridge_base.py`
- Modify: `execution_bridge/d1_trap_bridge.py` — becomes a thin subclass.
- Modify: `execution_bridge/fvg_bridge.py` — becomes a thin subclass.
- Test: existing `tests/execution/test_d1trap_bridge_fail_loud.py` and `tests/execution/test_fvg_bridge_fail_loud.py` must both still pass unchanged (this task is a pure refactor — if either test needs to change, that's a sign the extraction altered behavior, stop and re-check).

**Interfaces:**
- Produces: `class OptionBuyerExecutionBridge` (or similar name — your call) parameterized by: the request `Topic`, the request event class, the fill `Topic`, the fill event class, the strategy-name string(s) used for the ENTRY `can_trade()` gate (from Task 4), and the trade-log tag string. D1Trap and FVG bridges each become a small subclass supplying these five things plus whatever strategy-specific symbol-resolution logic differs (check `_resolve_symbol` in both files — confirm whether it's actually identical or has real per-strategy differences before assuming it can also be shared).

- [ ] **Step 1: Diff the two files post-Task-7** to confirm the actual extent of duplication (re-verify the audit's ~280-line estimate is still accurate after Tasks 6-7 added new code to both) and identify anything that's genuinely NOT shared (e.g. `_resolve_symbol`'s strike/expiry logic may differ between D1Trap's OI-wall-aware selection and FVG's fixed ITM-offset selection — check before assuming it's shareable).

- [ ] **Step 2: Write the base class**, moving every genuinely-identical method (trade logger, `_handle`'s routing skeleton, `_paper_fill`, `_live_fill`'s broker-order-placement mechanics, `_record_history`, the new fill/abort publishing from Tasks 6-7) into it, parameterized as described above.

- [ ] **Step 3: Rewrite both `d1_trap_bridge.py` and `fvg_bridge.py`** as thin subclasses.

- [ ] **Step 4: Run both existing test suites unchanged**

Run: `python -m pytest tests/execution/test_d1trap_bridge_fail_loud.py tests/execution/test_fvg_bridge_fail_loud.py -v`
Expected: PASS, with zero test modifications needed (if a test needed changing to pass, this refactor altered behavior — investigate before proceeding).

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest tests/ -q`
Expected: baseline, no new failures, no fewer passing tests than after Task 7.

- [ ] **Step 6: Commit**

```bash
git add execution_bridge/option_buyer_bridge_base.py execution_bridge/d1_trap_bridge.py execution_bridge/fvg_bridge.py
git commit -m "Extract shared OptionBuyerExecutionBridge base from D1Trap/FVG bridges"
```

---

## Self-Review

**Spec coverage:** all 8 audit-derived items covered — hourly_breakout fix (1), dead code removal (2), enable_chain dedup (3), can_trade wiring (4), FVG persistence (5, per user decision), D1Trap+FVG fill-confirmation (6-7, the real safety gap, per audit finding #6), bridge dedup (8, deferred to last per audit's own recommendation).

**Placeholder scan:** Task 6/7's exact test bodies are intentionally left to adapt to real method signatures discovered in each task's Step 1 — this is not a placeholder in the forbidden sense (vague "write tests for the above"), it's the same honest pattern used earlier in this project for a task whose target code hadn't been read in detail yet at plan-writing time (the implementer is given the exact reference pattern to mirror, the exact assertions to make, and instructed to adapt method names after reading — not left to invent behavior).

**Type consistency:** `can_trade(client_id, binding_id, client_db, strategy_name, underlying)` signature (Task 4) used identically at every call site described in Task 4 Step 5. `_enable_chain(underlying)` (Task 3) used identically at all 4 call sites. Task 6/7's fill-event field names (`exit_failed`, `entry_aborted`, `event_id`, `client_id`, `binding_id`) match `CascadeFillEvent`'s existing, reviewed shape exactly, so Task 8's extraction has a consistent shape to generalize over.
