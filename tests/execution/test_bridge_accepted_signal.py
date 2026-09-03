"""2026-08-06 CONFIRM-MODEL REDESIGN: straddle_bridge.py must publish a fast 'accepted'
StraddleFillEvent (no price, accepted=True) the instant every expected leg has a real
order_id -- before waiting for any fill -- and a 'placement_failed' event if placement
itself never succeeds after 3 retries (distinct from a broker-side rejection, which DOES
reach the broker)."""
import asyncio

from config.global_config import Topic
from data_layer.base_feeder import EventBus
from execution_bridge.straddle_bridge import StraddleExecutionBridge, StraddleOrderEvent
from execution_bridge.smart_executor import LegFill


class _Binding:
    provider = "zerodha"
    trading_mode = "live"
    binding_id = "B1"


class _InstantFillBroker:
    """Places instantly, fills instantly -- the common/fast real-world case."""
    def __init__(self):
        self._binding = _Binding()
        self.placed = []

    async def place_order(self, req):
        self.placed.append(req)
        return f"O{len(self.placed)}"

    async def get_order_status(self, oid):
        from execution_bridge.base_broker import OrderFill, OrderSide, OrderStatus
        return OrderFill(order_id=oid, broker_symbol="X", side=OrderSide.BUY, qty=65,
                         avg_price=100.0, status=OrderStatus.COMPLETE)

    async def get_positions(self):
        return []


def _ev(action="EXIT") -> StraddleOrderEvent:
    return StraddleOrderEvent(action=action, underlying="NIFTY", atm=24650,
                              ce_strike=24700, pe_strike=24600, ce_ltp=90.0, pe_ltp=70.0,
                              lot_size=65, lot_multiplier=1, client_id="cli", binding_id="B1")


def test_accepted_event_published_before_fill_event(monkeypatch):
    """The core speed/UI-feedback fix: an 'accepted' event (no price) must publish BEFORE
    the final fill event -- proving the strategy can react to 'order reached the broker'
    without waiting for the full confirm cycle."""
    async def run():
        import execution_bridge.straddle_bridge as sb
        bus = EventBus()
        broker = _InstantFillBroker()
        br = StraddleExecutionBridge(bus, registry=None, router=None)
        monkeypatch.setattr(sb, "_resolve_option_symbol", lambda *a, **k: f"SYM-{a[3]}")
        monkeypatch.setattr(sb, "order_exchange", lambda *_a, **_k: "NFO")

        published: list = []
        real_publish = bus.publish

        async def _spy_publish(topic, ev):
            if topic == Topic.ORDER_FILL:
                published.append(ev)
            await real_publish(topic, ev)

        bus.publish = _spy_publish

        ev = _ev("EXIT")
        await br._live_fill(ev, "cli", "B1", broker, paper=False)

        assert len(published) >= 2, f"expected at least accepted + final fill, got {published}"
        assert published[0].accepted is True
        assert published[0].ce_fill == 0.0 and published[0].pe_fill == 0.0
        assert published[-1].accepted is False
        assert published[-1].ce_fill > 0 and published[-1].pe_fill > 0

    asyncio.run(run())


def test_accepted_fires_once_for_the_whole_order_not_once_per_leg():
    """Both legs place their own orders -- accepted must fire exactly ONCE for the pair,
    once BOTH legs have a real order_id, not twice."""
    async def run():
        import execution_bridge.straddle_bridge as sb
        bus = EventBus()
        broker = _InstantFillBroker()
        br = StraddleExecutionBridge(bus, registry=None, router=None)
        import unittest.mock as _m
        with _m.patch.object(sb, "_resolve_option_symbol", lambda *a, **k: f"SYM-{a[3]}"), \
             _m.patch.object(sb, "order_exchange", lambda *_a, **_k: "NFO"):

            accepted_events: list = []
            fills = bus.subscribe(Topic.ORDER_FILL)

            ev = _ev("EXIT")
            await br._live_fill(ev, "cli", "B1", broker, paper=False)

            while not fills.empty():
                f = fills.get_nowait()
                if getattr(f, "accepted", False):
                    accepted_events.append(f)

            assert len(accepted_events) == 1

    asyncio.run(run())


class _AlwaysFailsToPlaceBroker:
    _binding = _Binding()

    async def place_order(self, req):
        raise ConnectionError("broker unreachable")

    async def get_order_status(self, oid):
        raise AssertionError("should never be called -- placement never succeeded")

    async def get_positions(self):
        return []


def test_placement_failed_event_published_when_broker_unreachable(monkeypatch):
    """3 retries exhausted at the SmartOrderExecutor layer -> straddle_bridge must publish
    placement_failed=True (never a fabricated fill, never silently nothing)."""
    async def run():
        import execution_bridge.straddle_bridge as sb
        bus = EventBus()
        broker = _AlwaysFailsToPlaceBroker()
        br = StraddleExecutionBridge(bus, registry=None, router=None)
        monkeypatch.setattr(sb, "_resolve_option_symbol", lambda *a, **k: f"SYM-{a[3]}")
        monkeypatch.setattr(sb, "order_exchange", lambda *_a, **_k: "NFO")
        _real_sleep = asyncio.sleep

        async def _fast_sleep(_secs, *a, **k):
            await _real_sleep(0)

        monkeypatch.setattr(sb.asyncio, "sleep", _fast_sleep)

        fills = bus.subscribe(Topic.ORDER_FILL)
        ev = _ev("EXIT")
        await br._live_fill(ev, "cli", "B1", broker, paper=False)

        seen = []
        while not fills.empty():
            seen.append(fills.get_nowait())

        assert any(getattr(f, "placement_failed", False) for f in seen)
        assert not any(getattr(f, "accepted", False) for f in seen)

    asyncio.run(run())
