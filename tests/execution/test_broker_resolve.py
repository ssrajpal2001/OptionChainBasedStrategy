"""Tests for resolve_broker_or_alert fail-loud broker resolution helper."""
import asyncio

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


def test_resolves_immediately_when_broker_present():
    async def run():
        bus = _FakeBus()
        router = _FakeRouter({"c1": {"b1": "BROKER_OBJ"}})
        result = await resolve_broker_or_alert(bus, router, "c1", "b1", "SellStraddle",
                                                attempts=3, delay_sec=0)
        assert result == "BROKER_OBJ"
        assert bus.published == []

    asyncio.run(run())


def test_retries_then_recovers_within_attempts():
    async def run():
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

    asyncio.run(run())


def test_alerts_and_returns_none_after_exhausting_retries():
    async def run():
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

    asyncio.run(run())


def test_no_bus_does_not_crash():
    async def run():
        router = _FakeRouter({})
        result = await resolve_broker_or_alert(None, router, "c1", "b1", "SellStraddle",
                                                attempts=1, delay_sec=0)
        assert result is None

    asyncio.run(run())
