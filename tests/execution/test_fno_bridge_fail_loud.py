import asyncio
import pytest


@pytest.mark.asyncio
async def test_broker_unavailable_publishes_system_event(monkeypatch):
    """
    FnOExecutionBridge._handle(ev) with an unavailable broker must publish
    BOTH a FNO_ORDER_FILL with order_failed=True AND a SYSTEM_EVENT with
    BROKER_UNAVAILABLE, rather than just silently dropping the order.
    """
    from execution_bridge.fno_bridge import FnOExecutionBridge, FnOOrderEvent
    from config.global_config import Topic, SysEvent

    calls = {"order_fills": [], "system_events": []}

    class _FakeBus:
        async def publish(self, topic, event):
            if topic == Topic.FNO_ORDER_FILL:
                calls["order_fills"].append(event)
            elif topic == Topic.SYSTEM_EVENT:
                calls["system_events"].append(event)

        def subscribe(self, topic):
            class _Q:
                async def get(self):
                    await asyncio.sleep(3600)
            return _Q()

    class _FakeRouter:
        _brokers = {}  # always empty -- broker never resolves

    bridge = FnOExecutionBridge.__new__(FnOExecutionBridge)
    bridge._bus = _FakeBus()
    bridge._router = _FakeRouter()
    bridge._tlog = None

    ev = FnOOrderEvent(
        action="ENTRY",
        symbol="RELIANCE",
        direction="CE",
        strike=2700,
        expiry_str="29 AUG 26",
        broker_symbol="RELIANCE26AUG2700CE",
        qty=1,
        price_hint=100.0,
        client_id="gurmeet",
        binding_id="zerodha",
        event_id="ev-12345",
        mode="live",
    )

    await bridge._handle(ev)

    # Must publish FNO_ORDER_FILL with order_failed=True
    assert len(calls["order_fills"]) == 1
    fill = calls["order_fills"][0]
    assert fill.order_failed is True
    assert fill.fill_price == 0.0
    assert fill.event_id == "ev-12345"

    # Must publish SYSTEM_EVENT with BROKER_UNAVAILABLE
    assert len(calls["system_events"]) == 1
    system_event = calls["system_events"][0]
    assert system_event.get("event") == SysEvent.BROKER_UNAVAILABLE
    assert "gurmeet" in system_event.get("message", "")
    assert "zerodha" in system_event.get("message", "")
