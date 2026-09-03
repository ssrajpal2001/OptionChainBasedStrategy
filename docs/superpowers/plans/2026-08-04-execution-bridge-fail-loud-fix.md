# Execution-Bridge Fail-Loud Fix + Pre-Commercial Fault-Injection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the silent live→paper fallback that has already cost gurmeet two unmanaged real-money incidents on 2026-08-04 (11:57:05 CE roll, 12:19:23 Day-Loss-SL exit — both tagged `[PAPER]` while `trading_mode=live`), replicate the same anti-pattern fix across every execution bridge, and add a fault-injection test/checklist that exercises broker-unavailable scenarios in paper/demo mode before this system goes into commercial (multi-tenant, real-money) use.

**Architecture:** Every order bridge (`execution_bridge/{straddle,cascade,d1_trap,fvg}_bridge.py`) currently resolves `broker = router._brokers.get(client_id, {}).get(binding_id)` and, if `None`, silently calls its own `_paper_fill()` — fabricating a successful simulated fill even when the deployment is configured `trading_mode=live`. `execution_bridge/fno_bridge.py` already does this correctly (logs `logger.error`, emits a `order_failed=True` fill, never fakes success) — that's the reference pattern this plan generalizes. The fix has two layers everywhere: (1) a shared broker-resolution helper that retries briefly then loudly alerts via `Topic.SYSTEM_EVENT` instead of returning silently, and (2) per-strategy "the fill I got back says the broker attempt failed — do not advance my state as if it succeeded" handling, which already exists for SellStraddle's ENTRY path (`entry_aborted`/`routing_failed` on `StraddleFillEvent`) but is missing entirely for SellStraddle's EXIT path and for every path in D1Trap/FVG (those two strategies don't even wait for a fill confirmation today — fire-and-forget optimistic state updates, a materially bigger gap flagged as follow-up work, not fixed in this plan).

**Tech Stack:** Python 3, asyncio, pytest, existing `EventBus`/`Topic`/`SysEvent` pub-sub (`config/global_config.py`), existing `ExecutionRouter._brokers` dict (`execution_bridge/execution_router.py`).

## Global Constraints

- No new dependencies — stdlib + existing project modules only.
- No `time.sleep` — retries use `asyncio.sleep`, matching project-wide async-only I/O rule (CLAUDE.md "Development Notes").
- Never silently fabricate a live fill: after this plan, "broker missing" in `mode != "paper"` must always produce a `logger.critical` + a `Topic.SYSTEM_EVENT` publish — no code path may return to business-as-usual without one of those two.
- Do not change any `mode == "paper"` behavior — pure local simulation stays exactly as-is; this plan only touches the `mode != "paper"` (live / paper_route) path.
- All existing tests (`python -m pytest tests/ -q`) must keep passing after every task; the 3 pre-existing unrelated failures (`test_upstox_converts_fyers_mcx_option_symbol`, `test_pair_indicators_combined_close_and_vwap`, `test_slope_and_vwap_ignore_seed_atp_contamination`) are an accepted baseline, not regressions to chase.

---

## File Structure

- **Create** `execution_bridge/broker_resolve.py` — shared retry-then-alert broker lookup, used by every bridge.
- **Modify** `config/global_config.py` — add `SysEvent.BROKER_UNAVAILABLE`.
- **Modify** `execution_bridge/straddle_bridge.py` — routing block uses the shared resolver for `mode != "paper"`; add `exit_aborted` to the failure path for both ENTRY (already has `entry_aborted`) and EXIT (new).
- **Modify** `strategies/sell_straddle/dataclasses.py` or wherever `StraddleFillEvent` lives — add `exit_aborted: bool = False` field.
- **Modify** `strategies/sell_straddle/engine.py` — `_on_fill`'s `EXIT` branch checks `exit_aborted`, bails without touching `self._position`, clears `_order_pending`.
- **Modify** `execution_bridge/d1_trap_bridge.py` — routing block uses the shared resolver for `mode != "paper"`; refuse to call `_paper_fill` on broker-unavailable, log critical + alert instead. (Feedback-loop gap into `bear_only_book.py` explicitly flagged, not fixed here — see "Follow-up" section.)
- **Modify** `execution_bridge/fvg_bridge.py` — same shape as d1_trap_bridge.py.
- **Modify** `execution_bridge/cascade_bridge.py` + `strategies/v4_cascade/book.py` — same shape as straddle_bridge.py (this one already has the `entry_aborted`/`routing_failed`/`_fill_loop` plumbing, so it gets the full loop fix, not just the bridge-level alert).
- **Modify** `execution_bridge/fno_bridge.py` — swap its already-correct manual check to use the shared resolver too, for consistency and the retry behavior (low risk, it already fails loud).
- **Create** `tests/execution/test_broker_resolve.py` — unit tests for the shared helper.
- **Create** `tests/execution/test_straddle_exit_abort.py` — fault-injection test: broker missing mid-EXIT in live mode must not close the position.
- **Create** `scripts/fault_injection_broker_dropout.py` — operational simulator script: runs each strategy in demo/paper mode, forces `router._brokers` gaps at controlled points, and reports whether any bridge faked a fill.
- **Create** `docs/PRE_COMMERCIAL_SIMULATOR_CHECKLIST.md` — the broader scenario checklist requested for pre-launch validation (broker dropout, feed dropout, mid-position restart, duplicate-entry regression, rate-limit/reject handling).

---

## Task 1: Shared broker-resolution helper (retry + loud failure)

**Files:**
- Create: `execution_bridge/broker_resolve.py`
- Modify: `config/global_config.py:222-227` (add `BROKER_UNAVAILABLE` to `SysEvent`)
- Test: `tests/execution/test_broker_resolve.py`

**Interfaces:**
- Produces: `async def resolve_broker_or_alert(bus, router, client_id: str, binding_id: str, strategy: str, context: str = "", attempts: int = 3, delay_sec: float = 1.0) -> Optional[object]` — returns the broker instance, or `None` after retries are exhausted (having already logged `logger.critical` and published `Topic.SYSTEM_EVENT` with `event=SysEvent.BROKER_UNAVAILABLE`).

- [ ] **Step 1: Add the new SysEvent code**

In `config/global_config.py`, inside `class SysEvent` (around line 227), add:

```python
    BROKER_UNAVAILABLE = "BROKER_UNAVAILABLE"  # live/paper_route order could not resolve a broker instance
```

- [ ] **Step 2: Write the failing test**

```python
# tests/execution/test_broker_resolve.py
import asyncio
import pytest
from execution_bridge.broker_resolve import resolve_broker_or_alert
from config.global_config import Topic, SysEvent


class _FakeBus:
    def __init__(self):
        self.published = []

    async def publish(self, topic, event):
        self.published.append((topic, event))


class _FakeRouter:
    def __init__(self, brokers):
        self._brokers = brokers


@pytest.mark.asyncio
async def test_resolves_immediately_when_broker_present():
    bus = _FakeBus()
    router = _FakeRouter({"c1": {"b1": "BROKER_OBJ"}})
    result = await resolve_broker_or_alert(bus, router, "c1", "b1", "SellStraddle",
                                            attempts=3, delay_sec=0)
    assert result == "BROKER_OBJ"
    assert bus.published == []


@pytest.mark.asyncio
async def test_retries_then_recovers_within_attempts():
    calls = {"n": 0}
    router = _FakeRouter({})

    class _FlakyRouter(_FakeRouter):
        @property
        def _brokers(self):
            calls["n"] += 1
            if calls["n"] < 2:
                return {}
            return {"c1": {"b1": "BROKER_OBJ"}}

        @_brokers.setter
        def _brokers(self, value):
            pass

    bus = _FakeBus()
    result = await resolve_broker_or_alert(bus, _FlakyRouter({}), "c1", "b1", "SellStraddle",
                                            attempts=3, delay_sec=0)
    assert result == "BROKER_OBJ"
    assert bus.published == []


@pytest.mark.asyncio
async def test_alerts_and_returns_none_after_exhausting_retries():
    bus = _FakeBus()
    router = _FakeRouter({})  # never has a broker for c1/b1
    result = await resolve_broker_or_alert(bus, router, "c1", "b1", "SellStraddle",
                                            context="EXIT day_loss_sl", attempts=3, delay_sec=0)
    assert result is None
    assert len(bus.published) == 1
    topic, event = bus.published[0]
    assert topic == Topic.SYSTEM_EVENT
    assert event["event"] == SysEvent.BROKER_UNAVAILABLE
    assert event["client_id"] == "c1"
    assert event["binding_id"] == "b1"
    assert "EXIT day_loss_sl" in event["message"]


@pytest.mark.asyncio
async def test_no_bus_does_not_crash():
    router = _FakeRouter({})
    result = await resolve_broker_or_alert(None, router, "c1", "b1", "SellStraddle",
                                            attempts=1, delay_sec=0)
    assert result is None
```

- [ ] **Step 3: Run test to verify it fails**

Run: `python -m pytest tests/execution/test_broker_resolve.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'execution_bridge.broker_resolve'`

- [ ] **Step 4: Write the implementation**

```python
# execution_bridge/broker_resolve.py
"""
Shared "resolve a live broker instance, never fake success" helper used by
every order bridge (straddle_bridge, cascade_bridge, d1_trap_bridge,
fvg_bridge, fno_bridge).

Why this exists: every bridge independently did
    broker = router._brokers.get(client_id, {}).get(binding_id)
    if broker is None:
        await self._paper_fill(...)   # silent -- fabricates a fill even in live mode
which on 2026-08-04 caused two real, unmanaged live-order failures for a real
client (gurmeet, SellStraddle) -- the bridge told the strategy an EXIT
succeeded when the order never reached Zerodha. This module centralizes the
lookup with a short retry (covers a transient gap during a broker hot-swap,
see execution_bridge/parallel_worker_pool.py add_broker_to_worker) and, if
still unresolved, logs CRITICAL and publishes a SYSTEM_EVENT instead of
letting the caller silently proceed as if nothing happened. The caller is
responsible for NOT faking a fill when this returns None.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from config.global_config import Topic, SysEvent

logger = logging.getLogger(__name__)


async def resolve_broker_or_alert(
    bus,
    router,
    client_id: str,
    binding_id: str,
    strategy: str,
    context: str = "",
    attempts: int = 3,
    delay_sec: float = 1.0,
) -> Optional[object]:
    for attempt in range(attempts):
        broker = (router._brokers or {}).get(client_id, {}).get(binding_id)
        if broker is not None:
            return broker
        if attempt < attempts - 1:
            await asyncio.sleep(delay_sec)

    logger.critical(
        "%s: broker unavailable for %s/%s after %d attempt(s) — refusing to fake a live "
        "fill (%s). No order was sent to the broker.",
        strategy, client_id, binding_id, attempts, context,
    )
    if bus is not None:
        try:
            await bus.publish(Topic.SYSTEM_EVENT, {
                "event": SysEvent.BROKER_UNAVAILABLE,
                "message": (
                    f"{strategy}: broker unavailable for {client_id}/{binding_id} — "
                    f"live order NOT sent ({context})"
                ),
                "client_id": client_id,
                "binding_id": binding_id,
                "strategy": strategy,
            })
        except Exception:
            logger.exception("%s: failed to publish BROKER_UNAVAILABLE system event", strategy)
    return None
```

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m pytest tests/execution/test_broker_resolve.py -v`
Expected: PASS (4 tests)

- [ ] **Step 6: Commit**

```bash
git add execution_bridge/broker_resolve.py config/global_config.py tests/execution/test_broker_resolve.py
git commit -m "Add shared fail-loud broker-resolution helper for execution bridges"
```

---

## Task 2: SellStraddle — full fix (bridge + engine EXIT-abort handling)

This is the flagship fix — the strategy that already suffered two live incidents, and the only one with an existing fill-confirmation loop to extend cleanly.

**Files:**
- Modify: `execution_bridge/straddle_bridge.py:435-459` (routing block), and the `StraddleFillEvent` dataclass definition (search the file for `class StraddleFillEvent`)
- Modify: `strategies/sell_straddle/engine.py:807-819` (`_on_fill` EXIT branch)
- Test: `tests/execution/test_straddle_exit_abort.py`

**Interfaces:**
- Consumes: `resolve_broker_or_alert` from Task 1.
- Produces: `StraddleFillEvent.exit_aborted: bool = False` — later tasks / dashboard code reading fill events should treat `exit_aborted=True` the same severity class as the existing `entry_aborted=True`.

- [ ] **Step 1: Find the exact `StraddleFillEvent` dataclass and confirm current fields**

Run: `grep -n "class StraddleFillEvent" -A 20 execution_bridge/straddle_bridge.py`

Add `exit_aborted: bool = False` alongside the existing `entry_aborted: bool = False` / `routing_failed: bool = False` fields (keep the exact same style/defaults already there).

- [ ] **Step 2: Write the failing test**

```python
# tests/execution/test_straddle_exit_abort.py
"""
Fault-injection: if the broker is unavailable at the moment a live EXIT tries
to route, the strategy must NOT believe the position closed. This reproduces
the 2026-08-04 gurmeet incident (Day-Loss-SL EXIT tagged [PAPER] while the
real Zerodha position stayed open) in a controlled test.
"""
import pytest
from execution_bridge.straddle_bridge import StraddleFillEvent


def test_fill_event_has_exit_aborted_field_defaulting_false():
    ev = StraddleFillEvent(
        action="EXIT", underlying="NIFTY", atm=24500,
        ce_strike=24650, pe_strike=24600, ce_fill=0.0, pe_fill=0.0,
        client_id="gurmeet", binding_id="zerodha", event_id="e1",
    )
    assert ev.exit_aborted is False


def test_on_fill_exit_aborted_does_not_close_position(monkeypatch):
    from strategies.sell_straddle.engine import SellStraddleStrategy
    # Build a minimal strategy instance with an open position and pending flag set,
    # exactly the state right before a Day-Loss-SL EXIT is dispatched.
    strat = SellStraddleStrategy.__new__(SellStraddleStrategy)
    strat._underlying = "NIFTY"
    strat._order_pending = True
    strat._clog = __import__("logging").getLogger("test")
    class _FakePos:
        status = "open"
    strat._position = _FakePos()

    fill = StraddleFillEvent(
        action="EXIT", underlying="NIFTY", atm=24500,
        ce_strike=24650, pe_strike=24600, ce_fill=0.0, pe_fill=0.0,
        client_id="gurmeet", binding_id="zerodha", event_id="e1",
        exit_aborted=True,
    )
    strat._on_fill(fill)

    # Position must still be the SAME open object -- not closed, not replaced.
    assert strat._position is not None
    assert strat._position.status == "open"
    # Pending flag must clear so the next qualifying tick can retry the exit.
    assert strat._order_pending is False
```

- [ ] **Step 3: Run test to verify it fails**

Run: `python -m pytest tests/execution/test_straddle_exit_abort.py -v`
Expected: FAIL — `test_fill_event_has_exit_aborted_field_defaulting_false` fails with `TypeError: __init__() got an unexpected keyword argument 'exit_aborted'`.

- [ ] **Step 4: Add the field**

In `execution_bridge/straddle_bridge.py`, in the `StraddleFillEvent` dataclass, add:

```python
    exit_aborted: bool = False   # True when a live EXIT could not reach the broker -- see broker_resolve.py
```

- [ ] **Step 5: Wire the routing block to use the shared resolver and never fake a live fill**

Replace `execution_bridge/straddle_bridge.py:435-459`:

```python
                mode = live_b.get("trading_mode", "paper") or "paper"

                if mode == "paper":
                    # PAPER = PURE LOCAL SIMULATION — never send a real order.
                    broker = (self._router._brokers or {}).get(client.client_id, {}).get(binding_id)
                    logger.info(
                        "StraddleExecutionBridge: routing %s %s → [%s/%s] mode=paper",
                        ev.action, ev.underlying, client.client_id, binding_id,
                    )
                    await self._paper_fill(ev, client.client_id, binding_id, broker)
                    routed += 1
                    continue

                from execution_bridge.broker_resolve import resolve_broker_or_alert
                broker = await resolve_broker_or_alert(
                    self._bus, self._router, client.client_id, binding_id, "SellStraddle",
                    context=f"{ev.action} {ev.underlying}",
                )

                logger.info(
                    "StraddleExecutionBridge: routing %s %s → [%s/%s] mode=%s broker=%s",
                    ev.action, ev.underlying, client.client_id, binding_id, mode,
                    "resolved" if broker is not None else "UNAVAILABLE",
                )

                if broker is None:
                    # Never fake a live fill. Tell the strategy this specific action failed
                    # so it does not silently believe the broker state changed.
                    await self._bus.publish(
                        Topic.ORDER_FILL,
                        StraddleFillEvent(
                            action=ev.action,
                            underlying=ev.underlying,
                            atm=ev.atm,
                            ce_strike=ev.ce_strike,
                            pe_strike=ev.pe_strike,
                            ce_fill=0.0,
                            pe_fill=0.0,
                            client_id=client.client_id,
                            binding_id=binding_id,
                            event_id=ev.event_id,
                            entry_aborted=(ev.action == "ENTRY"),
                            exit_aborted=(ev.action == "EXIT"),
                            routing_failed=True,
                        ),
                    )
                    routed += 1
                    continue
                elif mode == "paper_route":
                    await self._live_fill(ev, client.client_id, binding_id, broker, paper=True)
                else:
                    await self._live_fill(ev, client.client_id, binding_id, broker, paper=False)
                routed += 1
```

(Keep everything above line 435 — the binding/deployment gating loop — unchanged; only the body inside the `for live_b in live_bindings:` loop from the old `broker = ...` line through the old `routed += 1` changes.)

- [ ] **Step 6: Wire `_on_fill`'s EXIT branch to respect `exit_aborted`**

In `strategies/sell_straddle/engine.py`, at the top of the `elif fill.action == "EXIT":` branch (currently line 807), add before the existing "EXIT confirmed" logging:

```python
            elif fill.action == "EXIT":
                if getattr(fill, "exit_aborted", False):
                    logger.critical(
                        "SellStraddle[%s|%s|%s]: EXIT ABORTED — broker unavailable, position "
                        "REMAINS OPEN and unchanged. Will re-evaluate on next qualifying tick.",
                        self._underlying, fill.client_id, fill.binding_id,
                    )
                    self._clog.critical(
                        "EXIT ABORTED (broker unavailable) — position still OPEN, retrying on next tick"
                    )
                    self._order_pending = False
                    self._roll_in_progress = False
                    return
                _exit_legs = getattr(fill, "legs", ["CE", "PE"])
```

(The rest of the EXIT branch — currently starting at the old line 808 `_exit_legs = ...` — is unchanged, just now reached only for a real confirmed exit.)

- [ ] **Step 7: Run test to verify it passes**

Run: `python -m pytest tests/execution/test_straddle_exit_abort.py -v`
Expected: PASS (2 tests)

- [ ] **Step 8: Run the full test suite for regressions**

Run: `python -m pytest tests/ -q`
Expected: same pass count as baseline plus the new tests; only the 3 pre-existing unrelated failures remain.

- [ ] **Step 9: Commit**

```bash
git add execution_bridge/straddle_bridge.py strategies/sell_straddle/engine.py tests/execution/test_straddle_exit_abort.py
git commit -m "SellStraddle: never fake a live fill when broker is unavailable (fixes 2026-08-04 gurmeet incidents)"
```

---

## Task 3: D1Trap bridge — bridge-level fail-loud fix

D1TrapBearOnlyBook (the live BearTrap engine, `strategies/d1_trap_option/bear_only_book.py`) is fire-and-forget: it never subscribes to `Topic.D1_TRAP_ORDER_FILL` and updates its own state optimistically the instant it decides to act, independent of the bridge's outcome. This task stops the bridge from silently faking success — it does **not** close the bigger gap (the book still won't know an order failed even after this fix). That gap is tracked in "Follow-up Work" below, not fixed in this plan.

**Files:**
- Modify: `execution_bridge/d1_trap_bridge.py:146-158`
- Test: extend `tests/execution/test_broker_resolve.py` usage — see Step 2.

**Interfaces:**
- Consumes: `resolve_broker_or_alert` from Task 1.

- [ ] **Step 1: Write the failing test**

```python
# tests/execution/test_d1trap_bridge_fail_loud.py
import asyncio
import pytest


@pytest.mark.asyncio
async def test_no_paper_fallback_when_broker_missing_in_live_mode(monkeypatch):
    from execution_bridge.d1_trap_bridge import D1TrapExecutionBridge

    calls = {"paper_fill": 0, "alerts": 0}

    class _FakeBus:
        async def publish(self, topic, event):
            calls["alerts"] += 1

        def subscribe(self, topic):
            class _Q:
                async def get(self):
                    await asyncio.sleep(3600)
            return _Q()

    class _FakeRouter:
        _brokers = {}  # always empty -- broker never resolves

    bridge = D1TrapExecutionBridge.__new__(D1TrapExecutionBridge)
    bridge._bus = _FakeBus()
    bridge._router = _FakeRouter()
    bridge._trade_log = None

    async def _fake_paper_fill(ev):
        calls["paper_fill"] += 1
    bridge._paper_fill = _fake_paper_fill

    class _Ev:
        action = "BUY"
        client_id = "gurmeet"
        binding_id = "zerodha"
        underlying = "NIFTY"
        option_type = "PE"
        strike = 24600
        expiry = "2026-08-06"
        quantity = 75
        entry_price = 100.0
        exit_price = 0.0
        reason = "t2_swing_breach"

    live_binding = {"trading_mode": "live", "terminal_connected": True}
    await bridge._route(_Ev(), live_binding, db=None)

    assert calls["paper_fill"] == 0, "must never fabricate a fill when broker is unavailable in live mode"
    assert calls["alerts"] >= 1, "must publish a SYSTEM_EVENT alert instead"
```

Run `grep -n "async def _route\|async def _handle" execution_bridge/d1_trap_bridge.py` first to confirm the actual method name/signature this test should call — adjust the call in the test to match (the plan assumes a `_route(ev, live_binding, db)`-shaped entry point based on the code excerpt read during planning; if the real signature differs, use the real one, the assertions do not change).

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/execution/test_d1trap_bridge_fail_loud.py -v`
Expected: FAIL (`calls["paper_fill"] == 1`, current silent-fallback behavior).

- [ ] **Step 3: Fix the routing block**

Replace `execution_bridge/d1_trap_bridge.py:146-158`:

```python
        mode = live_binding.get("trading_mode", "paper") or "paper"

        if mode == "paper":
            broker = (self._router._brokers or {}).get(ev.client_id, {}).get(ev.binding_id)
            logger.info(
                "D1TrapExecutionBridge: %s %s %s%d exp=%s qty=%d → [%s/%s] mode=paper",
                ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
                ev.quantity, ev.client_id, ev.binding_id,
            )
            await self._paper_fill(ev)
            return

        from execution_bridge.broker_resolve import resolve_broker_or_alert
        broker = await resolve_broker_or_alert(
            self._bus, self._router, ev.client_id, ev.binding_id, "D1Trap",
            context=f"{ev.action} {ev.underlying} {ev.option_type}{ev.strike}",
        )

        logger.info(
            "D1TrapExecutionBridge: %s %s %s%d exp=%s qty=%d → [%s/%s] mode=%s broker=%s",
            ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
            ev.quantity, ev.client_id, ev.binding_id, mode,
            "resolved" if broker is not None else "UNAVAILABLE",
        )

        if broker is None:
            # Do NOT call _paper_fill here -- that would fabricate a fill the book
            # would treat as real. resolve_broker_or_alert already logged CRITICAL
            # and published SYSTEM_EVENT; the order is simply dropped.
            return

        await self._live_fill(ev, broker)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/execution/test_d1trap_bridge_fail_loud.py -v`
Expected: PASS

- [ ] **Step 5: Run the full test suite**

Run: `python -m pytest tests/ -q`
Expected: same baseline pass count plus the new test.

- [ ] **Step 6: Commit**

```bash
git add execution_bridge/d1_trap_bridge.py tests/execution/test_d1trap_bridge_fail_loud.py
git commit -m "D1Trap bridge: never fake a live fill when broker is unavailable"
```

---

## Task 4: FVG bridge — bridge-level fail-loud fix

Same shape as Task 3, same caveat (FVG's `strategies/fvg/engine.py` also does not wait for a fill confirmation — see Follow-up Work).

**Files:**
- Modify: `execution_bridge/fvg_bridge.py:152-153` (and surrounding `_route`/`_handle` block, same shape as d1_trap_bridge.py)
- Test: `tests/execution/test_fvg_bridge_fail_loud.py`

- [ ] **Step 1: Read the exact current block**

Run: `grep -n "broker is None or mode\|async def _route\|async def _handle" execution_bridge/fvg_bridge.py`

- [ ] **Step 2: Write the failing test**

Mirror `tests/execution/test_d1trap_bridge_fail_loud.py` from Task 3 Step 1, importing `FVGExecutionBridge` (or whatever the actual class name in `execution_bridge/fvg_bridge.py` is — confirm via `grep -n "^class" execution_bridge/fvg_bridge.py`) instead of `D1TrapExecutionBridge`, using an FVG-shaped fake event (strike/option_type/expiry/qty fields matching whatever `fvg_bridge.py`'s event dataclass expects — confirm via `grep -n "class.*Event" strategies/fvg/engine.py` or the bridge file's imports).

- [ ] **Step 3: Run test to verify it fails**

Run: `python -m pytest tests/execution/test_fvg_bridge_fail_loud.py -v`
Expected: FAIL (silent paper-fill still happens).

- [ ] **Step 4: Apply the same fix shape as Task 3 Step 3** to `execution_bridge/fvg_bridge.py`'s routing block (swap the `if broker is None or mode == "paper": await self._paper_fill(ev)` line for the paper-only-fast-path + `resolve_broker_or_alert` + drop-on-None pattern).

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m pytest tests/execution/test_fvg_bridge_fail_loud.py -v`
Expected: PASS

- [ ] **Step 6: Run the full test suite**

Run: `python -m pytest tests/ -q`

- [ ] **Step 7: Commit**

```bash
git add execution_bridge/fvg_bridge.py tests/execution/test_fvg_bridge_fail_loud.py
git commit -m "FVG bridge: never fake a live fill when broker is unavailable"
```

---

## Task 5: Cascade bridge (V4 Cascade) — full fix

`v4_cascade/book.py` already has a `_fill_loop`/`_on_fill` with `entry_aborted`/`routing_failed` handling (confirmed via `grep -n "entry_aborted" strategies/v4_cascade/book.py` during planning, line ~2141), so this strategy gets the same full ENTRY+EXIT abort loop as SellStraddle, not just the bridge-level alert. Lower urgency than Tasks 2-4 since `use_pool_engine` defaults `False` everywhere (not live anywhere today per CLAUDE.md), but should not be left inconsistent once the other three are fixed.

**Files:**
- Modify: `execution_bridge/cascade_bridge.py:241-242` (and surrounding routing block)
- Modify: `strategies/v4_cascade/book.py` around line 2141 (`_on_fill`) — add the same `exit_aborted` check used in Task 2 Step 6, adapted to this file's fill-event class name and EXIT-branch structure.
- Test: `tests/execution/test_cascade_exit_abort.py`, mirroring `tests/execution/test_straddle_exit_abort.py` from Task 2.

- [ ] **Step 1: Read the current fill-event class and `_on_fill` EXIT branch**

Run: `grep -n "class.*FillEvent\|def _on_fill" -A 30 strategies/v4_cascade/book.py execution_bridge/cascade_bridge.py`

- [ ] **Step 2: Write the failing test** (same two-test shape as Task 2 Step 2 — field-default test + `_on_fill` does-not-close-position test — adapted to V4 Cascade's actual fill-event class name/fields found in Step 1).

- [ ] **Step 3: Run test to verify it fails**

Run: `python -m pytest tests/execution/test_cascade_exit_abort.py -v`

- [ ] **Step 4: Apply the Task 2 Step 4-6 pattern** (add `exit_aborted` field to the fill event, swap `execution_bridge/cascade_bridge.py`'s routing block for the `resolve_broker_or_alert` pattern, add the `exit_aborted` early-return in `_on_fill`'s EXIT branch) to this strategy's actual file structure.

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m pytest tests/execution/test_cascade_exit_abort.py -v`

- [ ] **Step 6: Run the full test suite**

Run: `python -m pytest tests/ -q`

- [ ] **Step 7: Commit**

```bash
git add execution_bridge/cascade_bridge.py strategies/v4_cascade/book.py tests/execution/test_cascade_exit_abort.py
git commit -m "V4 Cascade: never fake a live fill when broker is unavailable"
```

---

## Task 6: FnO bridge — adopt shared resolver for consistency + retry

`execution_bridge/fno_bridge.py:124-134` already fails loud (`logger.error` + `order_failed=True` fill) — this task only swaps its manual one-shot lookup for the shared retry-then-alert helper so a transient hot-swap gap doesn't trip a false failure, and so its alerting goes through the same `Topic.SYSTEM_EVENT`/`SysEvent.BROKER_UNAVAILABLE` channel as every other strategy (today it only logs, no dashboard-visible alert).

**Files:**
- Modify: `execution_bridge/fno_bridge.py:124-134`
- Test: extend an existing FnO bridge test file if one exists (`grep -rl "FnOBridge\|FnOExecutionBridge" tests/`), else create `tests/execution/test_fno_bridge_fail_loud.py` mirroring Task 3's test shape.

- [ ] **Step 1: Write/extend the test** confirming a `SYSTEM_EVENT` with `SysEvent.BROKER_UNAVAILABLE` is published (in addition to the existing `order_failed=True` fill behavior, which must be preserved) when the broker is unresolved after retries.

- [ ] **Step 2: Run test to verify it fails** (today no `SYSTEM_EVENT` is published — only the `FNO_ORDER_FILL` with `order_failed=True`).

- [ ] **Step 3: Fix**

Replace `execution_bridge/fno_bridge.py:124-134`:

```python
    async def _handle(self, ev: FnOOrderEvent) -> None:
        from execution_bridge.broker_resolve import resolve_broker_or_alert
        broker = await resolve_broker_or_alert(
            self._bus, self._router, ev.client_id, ev.binding_id, "FnOPositional",
            context=f"{ev.action} {ev.symbol}",
        )
        if not broker:
            await self._bus.publish(Topic.FNO_ORDER_FILL, FnOFillEvent(
                event_id=ev.event_id, action=ev.action, symbol=ev.symbol,
                fill_price=0.0, qty=ev.qty,
                client_id=ev.client_id, binding_id=ev.binding_id,
                order_failed=True,
            ))
            return
```

(Keep everything below the old `if not broker:` block — the `req = OrderRequest(...)` and everything after — unchanged.)

- [ ] **Step 4: Run test to verify it passes**

- [ ] **Step 5: Run the full test suite**

Run: `python -m pytest tests/ -q`

- [ ] **Step 6: Commit**

```bash
git add execution_bridge/fno_bridge.py tests/execution/test_fno_bridge_fail_loud.py
git commit -m "FnO bridge: use shared broker-resolution helper for retry + dashboard-visible alert"
```

---

## Task 7: Fault-injection simulator script (pre-commercial validation)

This is the operational deliverable requested directly: "run the application as a simulator and check all scenarios and issues... so when we go 100% live in commercial market we don't get any hiccups." It runs the real strategy/bridge code in demo mode with a controllable fake router, so the same class of bug (and its sibling scenarios) can be caught before real capital is at risk again.

**Files:**
- Create: `scripts/fault_injection_broker_dropout.py`
- Create: `docs/PRE_COMMERCIAL_SIMULATOR_CHECKLIST.md`

**Interfaces:**
- Consumes: `resolve_broker_or_alert`, all four fixed bridges from Tasks 2-5.

- [ ] **Step 1: Write the script**

```python
"""
scripts/fault_injection_broker_dropout.py — pre-commercial fault-injection
harness. Runs each execution bridge against a fake router whose `_brokers`
dict can be told to "go missing" for a window, verifying that in `mode=live`
no bridge ever fabricates a fill (checked by asserting zero [PAPER]-tagged
log lines / zero order_failed=False fills during the dropout window) and
that a SYSTEM_EVENT is published for every dropout.

Usage: python3 scripts/fault_injection_broker_dropout.py
Exits non-zero if any bridge is caught faking a live fill during a dropout.
"""
import asyncio
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fault_injection")


class TogglableRouter:
    """A router whose _brokers dict can be forced empty on demand, to simulate
    the exact ExecutionRouter._brokers gap seen in the 2026-08-04 incidents."""

    def __init__(self, real_brokers: dict):
        self._real = real_brokers
        self._dropped = False

    @property
    def _brokers(self):
        return {} if self._dropped else self._real

    def drop(self):
        self._dropped = True

    def restore(self):
        self._dropped = False


class AlertCollector:
    """Wraps a real EventBus, recording every SYSTEM_EVENT published so the
    script can assert BROKER_UNAVAILABLE fired for each induced dropout."""

    def __init__(self, real_bus):
        self._real = real_bus
        self.system_events = []

    def subscribe(self, topic):
        return self._real.subscribe(topic)

    async def publish(self, topic, event):
        from config.global_config import Topic
        if topic == Topic.SYSTEM_EVENT:
            self.system_events.append(event)
        await self._real.publish(topic, event)


async def run_scenario(name: str, coro_factory) -> bool:
    """Run one fault-injection scenario, return True if it passed."""
    logger.info("=== SCENARIO: %s ===", name)
    try:
        ok = await coro_factory()
        logger.info("=== %s: %s ===", name, "PASS" if ok else "FAIL")
        return ok
    except Exception:
        logger.exception("=== %s: ERROR ===", name)
        return False


async def scenario_broker_drop_mid_exit() -> bool:
    """
    Reproduces the 2026-08-04 gurmeet incident directly: a live SellStraddle
    position gets a Day-Loss-SL EXIT decision while the router's broker entry
    is (simulated) transiently missing. Assert: no [PAPER] fill is faked, a
    BROKER_UNAVAILABLE SYSTEM_EVENT fires, and the strategy's position is
    still open afterward (not falsely closed).
    """
    from config.global_config import EventBus
    from execution_bridge.straddle_bridge import StraddleExecutionBridge, StraddleOrderEvent
    from execution_bridge.execution_router import ExecutionRouter

    bus = EventBus()
    alert_bus = AlertCollector(bus)
    real_router = ExecutionRouter.__new__(ExecutionRouter)
    real_router._brokers = {"sim_client": {"sim_binding": object()}}
    router = TogglableRouter(real_router._brokers)
    router.drop()  # simulate the exact gap from the incident

    bridge = StraddleExecutionBridge.__new__(StraddleExecutionBridge)
    bridge._bus = alert_bus
    bridge._router = router

    # NOTE: wiring bridge._route's other dependencies (client_db, live_bindings
    # lookup) is deployment-specific -- fill in against the real constructor
    # signature (`grep -n "class StraddleExecutionBridge" -A 40
    # execution_bridge/straddle_bridge.py`) before this scenario is trusted;
    # this function's skeleton and assertions define the pass/fail bar.
    raise NotImplementedError(
        "Wire StraddleExecutionBridge's real constructor/dependencies here, "
        "then dispatch a live-mode EXIT order event and assert: "
        "0 [PAPER] log lines emitted, 1 BROKER_UNAVAILABLE alert in "
        "alert_bus.system_events, position still open."
    )


SCENARIOS = {
    "broker_drop_mid_exit": scenario_broker_drop_mid_exit,
    # Add: broker_drop_mid_entry, feed_drop_mid_position (see checklist doc),
    # restart_mid_position (position_store round-trip), duplicate_entry_regression
    # (BearTrap 10:04:00 double-entry, still open per handoff notes).
}


async def main():
    results = {}
    for name, factory in SCENARIOS.items():
        results[name] = await run_scenario(name, factory)
    failed = [n for n, ok in results.items() if not ok]
    if failed:
        logger.error("FAILED scenarios: %s", failed)
        sys.exit(1)
    logger.info("All fault-injection scenarios passed.")


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Run it and confirm it currently reports the `NotImplementedError`** (expected — the skeleton documents exactly what wiring is needed against the real `StraddleExecutionBridge` constructor, which depends on runtime objects — `client_db`, live broker instances — not fully mockable from a plan document; this is why it's a `NotImplementedError` marker rather than fabricated passing code).

Run: `python3 scripts/fault_injection_broker_dropout.py`
Expected: scenario reports `ERROR` with the `NotImplementedError` message, exit code 1.

- [ ] **Step 3: Wire the real constructor call**

Read `execution_bridge/straddle_bridge.py`'s `StraddleExecutionBridge.__init__` and the method that owns the routing block edited in Task 2 (confirm its exact name/signature — the plan's Task 2 excerpt shows the body but the enclosing method name should be confirmed with `grep -n "async def " execution_bridge/straddle_bridge.py`). Replace the `raise NotImplementedError(...)` with: constructing whatever minimal `client_db`/`live_bindings` stand-ins that method needs (a small in-file fake, same style as `_FakeBus`/`_FakeRouter` in Task 1's tests), publishing a live-mode `StraddleOrderEvent(action="EXIT", ...)` onto `bus`, running the bridge's routing coroutine once, then asserting:
  - No log record containing `[PAPER]` was emitted during this call (capture via `caplog`-style handler or a custom logging.Handler appended to `execution_bridge.straddle_bridge`'s logger for the duration of the call).
  - `len(alert_bus.system_events) == 1` and `alert_bus.system_events[0]["event"] == SysEvent.BROKER_UNAVAILABLE`.
  - Return `True` only if both hold.

- [ ] **Step 4: Run it end-to-end**

Run: `python3 scripts/fault_injection_broker_dropout.py`
Expected: `broker_drop_mid_exit: PASS`, exit code 0.

- [ ] **Step 5: Commit**

```bash
git add scripts/fault_injection_broker_dropout.py
git commit -m "Add fault-injection simulator for broker-dropout scenarios"
```

---

## Task 8: Pre-commercial simulator checklist document

**Files:**
- Create: `docs/PRE_COMMERCIAL_SIMULATOR_CHECKLIST.md`

- [ ] **Step 1: Write the checklist**

```markdown
# Pre-Commercial Simulator Checklist

Run in `--mode paper` / `--mode demo` (see CLAUDE.md "Launch Commands") against
every strategy before any new client goes live with real capital. Each item
must be exercised at least once and its outcome recorded (pass/fail + log
excerpt) before sign-off.

## 1. Broker-unavailable fail-loud (this plan, Tasks 1-6)
- [ ] Force `ExecutionRouter._brokers[client][binding]` to `None`/missing while
      a live-mode ENTRY is pending for each of: SellStraddle, D1Trap BearOnly,
      FVG, FnO Positional, V4 Cascade. Confirm: no `[PAPER]` fill logged, a
      `BROKER_UNAVAILABLE` SYSTEM_EVENT fires, strategy state does not
      optimistically advance.
- [ ] Same, but for an EXIT with an already-open position. Confirm the
      position remains open and unmanaged-but-unclosed (not falsely booked
      as closed) — SellStraddle and V4 Cascade only (Tasks 2 and 5 give them
      the full state-safe loop); D1Trap and FVG are a KNOWN GAP (see
      "Follow-up Work" in the implementation plan) — confirm this gap is
      still explicitly flagged, not silently "fixed" by omission.
- [ ] Confirm `pm2 logs terminus` / the client's dedicated log file surfaces
      the CRITICAL log line clearly enough that on-call would notice within
      one trading session, not just in a post-mortem grep.

## 2. Feed dropout mid-position
- [ ] Kill the feed (Upstox/Fyers) mid-open-position in paper mode for each
      strategy. Confirm: `SysEvent.FEEDER_DOWN` fires, no exit misfires on
      stale/zero ticks, and the GlobalFeeder heartbeat provider-switch (see
      CLAUDE.md "Development Notes") does not duplicate an order on
      reconnect.

## 3. Restart mid-position (position_store round-trip)
- [ ] For every strategy that persists to `data/positions/*.json`
      (sell_straddle, d1_trap_bear_only, v4_cascade), open a paper position,
      kill and restart the process, confirm the restored position matches
      exactly (strikes, entry prices, TSL/peak state) — this is the exact
      class of corruption the 2026-08-04 gurmeet reconciliation script
      (`scripts/fix_gurmeet_straddle_reconcile.py`) had to repair by hand.

## 4. Duplicate-entry regression (known open item)
- [ ] Confirm the BearTrap NIFTY 10:04:00 double-entry pattern (two duplicate
      "no running trap deployment" BUY rejections, flagged but not yet
      root-caused per prior session notes) is reproduced or ruled out in
      paper mode before commercial launch — this predates and is independent
      of the broker-dropout fix in this plan.

## 5. Rate-limit / order-rejection handling
- [ ] Force a broker rejection (e.g. bad symbol, insufficient margin in a
      paper/no-fund account) mid-ENTRY and mid-EXIT for each strategy.
      Confirm the strategy's abort path (Task 2-5's `entry_aborted`/
      `exit_aborted`, or the equivalent existing ENTRY-only path for
      D1Trap/FVG) fires the same way it does for a missing broker — a
      rejection and a missing-broker should both count as "did not fill,"
      never silently treated as a fill.

## 6. Multi-tenant isolation
- [ ] With two paper clients running the same strategy/underlying
      simultaneously, force a broker dropout on ONLY one client's binding.
      Confirm the other client's orders are unaffected — this plan's
      `resolve_broker_or_alert` is per (client_id, binding_id), so this
      should hold, but must be verified end-to-end, not just at the unit
      level.
```

- [ ] **Step 2: Commit**

```bash
git add docs/PRE_COMMERCIAL_SIMULATOR_CHECKLIST.md
git commit -m "Add pre-commercial fault-injection checklist"
```

---

## Follow-up Work (explicitly NOT built in this plan — flag to the user before commercial launch)

1. **D1Trap BearOnly and FVG have no fill-confirmation feedback loop at all.** `bear_only_book.py` and `strategies/fvg/engine.py` update their own in-memory state the instant they decide to act, never waiting for `Topic.D1_TRAP_ORDER_FILL` / `Topic.FVG_ORDER_FILL`. Tasks 3-4 stop the bridge from *fabricating* a fill, but the book's internal state can still drift from the broker if an order is rejected or partially fills (a different failure mode than the one that hit gurmeet, but the same category of "system believes something that isn't true at the broker"). Closing this gap fully requires giving both books a `_fill_loop`/`_on_fill` pair modeled on `sell_straddle/engine.py`'s, gating position-state transitions on a real fill event — a materially larger change than this plan, deserving its own plan and its own backtest/paper validation pass before being trusted live.
2. **Root cause of the `ExecutionRouter._brokers` gap is still unconfirmed.** This plan makes the failure loud and safe instead of silent and dangerous, but does not explain *why* the dict entry went missing twice in ~20 minutes on gurmeet's binding specifically with no restart involved. Worth instrumenting `parallel_worker_pool.py`'s `add_broker_to_worker` hot-swap path (logging every time it's invoked, with the trigger reason) so the next occurrence has a paper trail instead of just the symptom.

---

## Self-Review

**Spec coverage:** shared fail-loud helper (Task 1) ✓; SellStraddle full fix — the strategy that actually broke twice (Task 2) ✓; same pattern replicated to every other bridge, D1Trap/FVG/Cascade/FnO (Tasks 3-6) ✓; "run as a simulator and check all scenarios" (Tasks 7-8) ✓; "explain core changes needed" — covered in this plan's Architecture section and the Follow-up Work section, which explicitly separates what's fixed now from what's a bigger, later lift ✓.

**Placeholder scan:** Task 7's `scenario_broker_drop_mid_exit` intentionally contains a `NotImplementedError` marker rather than fabricated mock wiring, because the real `StraddleExecutionBridge` constructor's dependencies are runtime objects not fully knowable from a planning pass — Step 3 of Task 7 explicitly directs the implementer to inspect the real signature and complete it as a discrete, verifiable step (run it, see it fail with that exact message, then wire it, then see it pass) rather than leaving a vague "handle this" instruction — this follows the skill's own guidance to prefer an honest, verifiable placeholder over fabricated code that would silently pass without exercising anything.

**Type consistency:** `resolve_broker_or_alert(bus, router, client_id, binding_id, strategy, context, attempts, delay_sec)` signature from Task 1 is used identically in Tasks 2, 3, 4, 6 (Task 5 reuses it for Cascade's bridge-level call too). `exit_aborted: bool` field name is used identically across Task 2 (SellStraddle) and Task 5 (Cascade). `SysEvent.BROKER_UNAVAILABLE` is defined once in Task 1 and referenced by name (not re-declared) everywhere else.
