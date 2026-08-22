"""
tests/execution/test_oi_flow_bridge_concurrency.py -- regression for the
2026-08-23 fix porting straddle_bridge.py's own 2026-06-12 head-of-line-
blocking fix to OIFlowExecutionBridge.

Before this fix, run()'s consumer loop called `await self._handle(ev)`
directly and sequentially -- a slow/hung broker call for ONE client's
order fully blocked every OTHER client's order (even a completely
unrelated client, different broker) from being processed until it
finished. _handle_chained() now dispatches each event as its own task,
tracked per (client_id, binding_id) key: different keys run fully
concurrently; the SAME key's events still process strictly in order (an
entry then its own exit never race against each other).
"""
import asyncio

import pytest

from execution_bridge.oi_flow_bridge import OIFlowExecutionBridge


class _CapturingBus:
    def __init__(self) -> None:
        self.published: list = []
        self._q: asyncio.Queue = asyncio.Queue()

    async def publish(self, topic, event):
        self.published.append((topic, event))

    def subscribe(self, topic):
        return self._q


class _FakeEvent:
    def __init__(self, client_id: str, binding_id: str, tag: str) -> None:
        self.client_id = client_id
        self.binding_id = binding_id
        self.tag = tag
        self.action = "BUY"
        self.underlying = "TEST"


def _make_bridge(tmp_path) -> OIFlowExecutionBridge:
    bus = _CapturingBus()
    bridge = OIFlowExecutionBridge.__new__(OIFlowExecutionBridge)
    bridge._bus = bus
    bridge._router = None
    bridge._running = True
    bridge._q = bus._q
    bridge._key_tasks = {}

    class _NoopLog:
        def log(self, *a, **k):
            pass
        def close_all(self):
            pass
    bridge._trade_log = _NoopLog()
    return bridge


@pytest.mark.asyncio
async def test_two_different_clients_run_concurrently_not_blocked():
    bridge = _make_bridge(None)
    order = []
    release_a = asyncio.Event()

    async def _fake_handle(ev):
        order.append(f"{ev.tag}:start")
        if ev.tag == "A":
            await release_a.wait()   # client A's order deliberately hangs
        order.append(f"{ev.tag}:end")

    bridge._handle = _fake_handle

    ev_a = _FakeEvent("clientA", "bindA", "A")
    ev_b = _FakeEvent("clientB", "bindB", "B")

    key_a = (ev_a.client_id, ev_a.binding_id)
    key_b = (ev_b.client_id, ev_b.binding_id)
    task_a = asyncio.create_task(bridge._handle_chained(ev_a, None, key_a))
    task_b = asyncio.create_task(bridge._handle_chained(ev_b, None, key_b))
    bridge._key_tasks[key_a] = task_a
    bridge._key_tasks[key_b] = task_b

    await asyncio.wait_for(task_b, timeout=1.0)   # client B must complete WITHOUT waiting on A
    assert order == ["A:start", "B:start", "B:end"], (
        "client B's order must complete even though client A's is still hanging -- "
        "this is the exact head-of-line-blocking bug being fixed"
    )

    release_a.set()
    await asyncio.wait_for(task_a, timeout=1.0)
    assert order == ["A:start", "B:start", "B:end", "A:end"]


@pytest.mark.asyncio
async def test_same_client_events_still_process_in_order():
    bridge = _make_bridge(None)
    order = []

    async def _fake_handle(ev):
        order.append(f"{ev.tag}:start")
        await asyncio.sleep(0.01)
        order.append(f"{ev.tag}:end")

    bridge._handle = _fake_handle

    key = ("clientA", "bindA")
    ev1 = _FakeEvent("clientA", "bindA", "1")
    ev2 = _FakeEvent("clientA", "bindA", "2")

    task1 = asyncio.create_task(bridge._handle_chained(ev1, None, key))
    bridge._key_tasks[key] = task1
    prev = bridge._key_tasks.get(key)
    task2 = asyncio.create_task(bridge._handle_chained(ev2, prev, key))
    bridge._key_tasks[key] = task2

    await asyncio.wait_for(task2, timeout=1.0)
    assert order == ["1:start", "1:end", "2:start", "2:end"], (
        "the second order for the SAME (client,binding) must not start until "
        "the first one's own handling has fully finished"
    )


@pytest.mark.asyncio
async def test_one_clients_exception_does_not_affect_another_clients_task():
    bridge = _make_bridge(None)
    results = []

    async def _fake_handle(ev):
        if ev.tag == "BAD":
            raise RuntimeError("simulated broker failure for client A")
        results.append(ev.tag)

    bridge._handle = _fake_handle

    ev_bad = _FakeEvent("clientA", "bindA", "BAD")
    ev_good = _FakeEvent("clientB", "bindB", "GOOD")
    key_bad = ("clientA", "bindA")
    key_good = ("clientB", "bindB")

    task_bad = asyncio.create_task(bridge._handle_chained(ev_bad, None, key_bad))
    task_good = asyncio.create_task(bridge._handle_chained(ev_good, None, key_good))
    bridge._key_tasks[key_bad] = task_bad
    bridge._key_tasks[key_good] = task_good

    await asyncio.wait_for(task_bad, timeout=1.0)   # must not raise out of _handle_chained
    await asyncio.wait_for(task_good, timeout=1.0)
    assert results == ["GOOD"]
    # Both slots cleared -- the bridge is left in a clean state for future events.
    assert key_bad not in bridge._key_tasks
    assert key_good not in bridge._key_tasks


@pytest.mark.asyncio
async def test_run_dispatches_via_handle_chained_per_key():
    """Integration-style: drive the real run() loop (not just
    _handle_chained directly) with a real OIFlowOrderEvent to confirm it
    correctly derives the (client_id, binding_id) key and spawns a chained
    task per event, rather than awaiting _handle() inline."""
    from datetime import date
    import execution_bridge.oi_flow_bridge as mod

    bridge = _make_bridge(None)
    processed = []

    async def _fake_handle(ev):
        processed.append(ev.client_id)

    bridge._handle = _fake_handle

    run_task = asyncio.create_task(bridge.run())
    ev = mod.OIFlowOrderEvent(
        client_id="clientA", binding_id="bindA", action="BUY",
        underlying="TEST", option_type="CE", strike=100, expiry=date(2026, 1, 1),
        quantity=1, entry_price=1.0, sl_price=0.5, reason="test", event_id="e1",
    )
    await bridge._q.put(ev)
    key = ("clientA", "bindA")
    for _ in range(100):   # poll for run() to have dispatched and registered the chained task
        await asyncio.sleep(0.01)
        if key in bridge._key_tasks:
            await bridge._key_tasks[key]
            break

    bridge._running = False
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    assert processed == ["clientA"]
    assert key not in bridge._key_tasks
