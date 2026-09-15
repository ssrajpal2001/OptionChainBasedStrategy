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


# ── expiry-day / day-before-expiry rule (real incident, 2026-09-15/16) ──────

def _patch_registry_and_clock(monkeypatch, frozen_dt, expiry_map):
    from datetime import datetime as real_datetime
    import strategies.iron_fly.engine as engine_mod
    import sys

    class _FrozenDatetime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            return frozen_dt.replace(tzinfo=tz)

    monkeypatch.setattr(engine_mod, "datetime", _FrozenDatetime)

    class _FakeRegistry:
        @staticmethod
        def get_active_expiry_strict(underlying, from_date):
            return expiry_map.get(from_date)

    fake_module = type(sys)("data_layer.instrument_registry")
    fake_module.REGISTRY = _FakeRegistry()
    monkeypatch.setitem(sys.modules, "data_layer.instrument_registry", fake_module)


def test_resolve_expiry_jumps_to_next_week_on_expiry_day_even_in_the_morning(monkeypatch):
    """Real incident (2026-09-15), simplified spec (2026-09-16): "we will
    NEVER take trade of same week expiry on expiry day" -- unconditional,
    no time-of-day check. This would have changed the real 09:21 first
    entry of the day to use next week's contract instead."""
    from datetime import date, datetime as real_datetime

    bus = _FakeBus()
    book = IronFlyStrategy(bus, _fake_cfg(), "NIFTY", "c1", "b1")
    _patch_registry_and_clock(
        monkeypatch, real_datetime(2026, 9, 15, 9, 21),
        {date(2026, 9, 15): date(2026, 9, 15), date(2026, 9, 16): date(2026, 9, 22)},
    )

    assert book._resolve_expiry() == date(2026, 9, 22)


def test_resolve_expiry_jumps_to_next_week_day_before_expiry_too(monkeypatch):
    """Direct user spec: "if 1 day before ... 65% is achieved then also it
    will jump to next week" -- any time of day, same as expiry day itself."""
    from datetime import date, datetime as real_datetime

    bus = _FakeBus()
    book = IronFlyStrategy(bus, _fake_cfg(), "NIFTY", "c1", "b1")
    _patch_registry_and_clock(
        monkeypatch, real_datetime(2026, 9, 14, 15, 20),
        {date(2026, 9, 14): date(2026, 9, 15), date(2026, 9, 16): date(2026, 9, 22)},
    )

    assert book._resolve_expiry() == date(2026, 9, 22)


def test_resolve_expiry_uses_current_contract_two_days_before_expiry(monkeypatch):
    from datetime import date, datetime as real_datetime

    bus = _FakeBus()
    book = IronFlyStrategy(bus, _fake_cfg(), "NIFTY", "c1", "b1")
    _patch_registry_and_clock(
        monkeypatch, real_datetime(2026, 9, 13, 15, 20),
        {date(2026, 9, 13): date(2026, 9, 15), date(2026, 9, 14): date(2026, 9, 22)},
    )

    assert book._resolve_expiry() == date(2026, 9, 15)
