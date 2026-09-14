"""Lifecycle smoke tests for strategies/iron_fly/engine.py's IronFlyStrategy
(the Phase 2 async live wrapper). Not a full integration test of the real
EventBus/broker stack -- that's this strategy's first paper_route deployment
itself, per this codebase's own established graduation discipline. These
tests exist to catch import errors, wrong Topic subscriptions, and
constructor/lifecycle wiring bugs before that deployment, using a minimal
fake bus.
"""
import asyncio
from types import SimpleNamespace

import pytest

from config.global_config import Topic
from strategies.iron_fly.engine import IronFlyStrategy


class _FakeQueue:
    async def get(self):
        await asyncio.sleep(3600)


class _FakeBus:
    def __init__(self):
        self.subscribed = []
        self.published = []

    def subscribe(self, topic):
        self.subscribed.append(topic)
        return _FakeQueue()

    def unsubscribe(self, topic, q):
        pass

    async def publish(self, topic, event):
        self.published.append((topic, event))


def _fake_cfg():
    exchange = SimpleNamespace(lot_sizes={"NIFTY": 75}, strike_steps={"NIFTY": 50})
    return SimpleNamespace(exchange=exchange)


@pytest.mark.asyncio
async def test_start_subscribes_all_three_topics_and_spawns_three_tasks():
    bus = _FakeBus()
    book = IronFlyStrategy(bus, _fake_cfg(), "NIFTY", "ssrajpal2001", "SA5770", lot_multiplier=1)
    book.start()
    try:
        assert set(bus.subscribed) == {Topic.INDEX_TICK, Topic.OPTION_TICK, Topic.IRON_FLY_ORDER_FILL}
        assert len(book._tasks) == 3
        assert book.is_flat() is True
        assert book._engine.qty == 75  # 1 lot x lot_size 75
    finally:
        await book.stop_async()


@pytest.mark.asyncio
async def test_lot_multiplier_scales_qty():
    bus = _FakeBus()
    book = IronFlyStrategy(bus, _fake_cfg(), "NIFTY", "c1", "b1", lot_multiplier=3)
    assert book._engine.qty == 225  # 3 x 75


@pytest.mark.asyncio
async def test_stop_async_cancels_tasks_and_unsubscribes():
    bus = _FakeBus()
    book = IronFlyStrategy(bus, _fake_cfg(), "NIFTY", "c1", "b1")
    book.start()
    await book.stop_async()
    assert all(t.done() for t in book._tasks)
    assert book._loop_queues == {}


def test_monitoring_state_when_flat():
    bus = _FakeBus()
    book = IronFlyStrategy(bus, _fake_cfg(), "NIFTY", "c1", "b1")
    state = book.monitoring_state()
    assert state["is_flat"] is True
    assert state["legs"]["short_ce"] is None
    assert state["underlying"] == "NIFTY"
