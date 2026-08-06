"""2026-08-06 SPEED FIX: straddle_bridge.py's _do_leg under-fill retry loop used to
blindly poll get_order_status up to 15 times (15s) regardless of what the broker
actually reported -- including a REJECTED order on the very first poll, which can
never later show a fill (paper_route's expected no-funds rejection hits this on
EVERY single order). SmartOrderExecutor._await_fill already short-circuits on a
terminal REJECTED/CANCELLED status; this loop was blind to the same signal and
re-polled from scratch regardless. Fixed to stop polling the instant the broker
reports a terminal REJECTED/CANCELLED status, only continuing to wait out a
genuinely still-PENDING/OPEN order (the real scenario that justified extending
this loop to 15s in the first place)."""
import asyncio

from data_layer.base_feeder import EventBus
from execution_bridge.straddle_bridge import StraddleExecutionBridge, StraddleOrderEvent
from execution_bridge.smart_executor import LegFill
from execution_bridge.base_broker import OrderFill, OrderSide, OrderStatus


class _Binding:
    provider = "zerodha"
    trading_mode = "paper_route"
    binding_id = "B1"


class _RejectingBroker:
    """Simulates a real no-funds rejection: get_order_status reports REJECTED on
    every poll (matching Kite's real behaviour for a margin-rejected order). Tracks
    polls PER order_id so two legs racing concurrently each get an independent count."""

    def __init__(self):
        self._binding = _Binding()
        self.status_calls: dict = {}

    async def get_order_status(self, oid):
        self.status_calls[oid] = self.status_calls.get(oid, 0) + 1
        return OrderFill(order_id=oid, broker_symbol="X", side=OrderSide.BUY, qty=0,
                         avg_price=0.0, status=OrderStatus.REJECTED)

    async def get_positions(self):
        return []


class _StillPendingBroker:
    """Simulates a genuinely slow-but-real fill: OPEN for the first few polls, then
    fills. The loop must NOT be short-circuited by this -- it's exactly the real
    scenario that justified the 15s retry window in the first place. Tracks polls
    PER order_id so two legs racing concurrently against the same broker instance
    each get their own independent, deterministic poll count."""

    def __init__(self, fill_after: int):
        self._binding = _Binding()
        self.status_calls: dict = {}
        self._fill_after = fill_after

    async def get_order_status(self, oid):
        self.status_calls[oid] = self.status_calls.get(oid, 0) + 1
        if self.status_calls[oid] >= self._fill_after:
            return OrderFill(order_id=oid, broker_symbol="X", side=OrderSide.BUY, qty=65,
                             avg_price=42.5, status=OrderStatus.COMPLETE)
        return OrderFill(order_id=oid, broker_symbol="X", side=OrderSide.BUY, qty=0,
                         avg_price=0.0, status=OrderStatus.OPEN)


def _fast_sleep_factory(calls: list):
    async def _fast_sleep(*_a, **_k):
        calls.append(1)
    return _fast_sleep


def _underfilled_exec_leg(broker_, *, broker_symbol, side, qty, **kw):
    async def _inner():
        return LegFill(filled_qty=0, avg_price=0.0, order_ids=[f"o-{broker_symbol}"], completed=False)
    return _inner()


def test_rejected_order_stops_polling_immediately(monkeypatch):
    """The core fix: a REJECTED order must not cost the full 15-attempt/15s wait."""
    async def run():
        import execution_bridge.straddle_bridge as sb
        bus = EventBus()
        broker = _RejectingBroker()
        br = StraddleExecutionBridge(bus, registry=None, router=None)
        monkeypatch.setattr(sb, "_resolve_option_symbol", lambda *a, **k: f"SYM-{a[3]}")
        monkeypatch.setattr(sb, "order_exchange", lambda *_a, **_k: "NFO")

        sleep_calls: list = []
        monkeypatch.setattr(sb.asyncio, "sleep", _fast_sleep_factory(sleep_calls))

        br._executor.execute_leg = _underfilled_exec_leg
        br._exit_executor.execute_leg = _underfilled_exec_leg

        ev = StraddleOrderEvent(action="EXIT", underlying="NIFTY", atm=24650,
                                ce_strike=24700, pe_strike=24600, ce_ltp=90.0, pe_ltp=70.0,
                                lot_size=65, lot_multiplier=1, client_id="cli", binding_id="B1")
        await br._live_fill(ev, "cli", "B1", broker, paper=True)

        # CE and PE legs run concurrently, each with their own retry loop and order_id.
        # Without the fix this would be 15 polls EACH (30 total), all wasted on an
        # order that was already known-dead on the very first poll.
        assert list(broker.status_calls.values()) == [1, 1], (
            f"expected exactly 1 status poll per leg before recognizing the terminal "
            f"REJECTED status, got {broker.status_calls} -- the fast-path short-circuit "
            f"isn't working."
        )
        assert len(sleep_calls) == 2

    asyncio.run(run())


def test_genuinely_slow_real_fill_still_gets_the_full_window(monkeypatch):
    """Sanity check: a still-PENDING order (the real scenario that justified the 15s
    window) must NOT be cut short -- only a terminal REJECTED/CANCELLED status
    short-circuits the wait."""
    async def run():
        import execution_bridge.straddle_bridge as sb
        bus = EventBus()
        broker = _StillPendingBroker(fill_after=6)
        br = StraddleExecutionBridge(bus, registry=None, router=None)
        monkeypatch.setattr(sb, "_resolve_option_symbol", lambda *a, **k: f"SYM-{a[3]}")
        monkeypatch.setattr(sb, "order_exchange", lambda *_a, **_k: "NFO")

        sleep_calls: list = []
        monkeypatch.setattr(sb.asyncio, "sleep", _fast_sleep_factory(sleep_calls))

        br._executor.execute_leg = _underfilled_exec_leg
        br._exit_executor.execute_leg = _underfilled_exec_leg

        ev = StraddleOrderEvent(action="EXIT", underlying="NIFTY", atm=24650,
                                ce_strike=24700, pe_strike=24600, ce_ltp=90.0, pe_ltp=70.0,
                                lot_size=65, lot_multiplier=1, client_id="cli", binding_id="B1")
        await br._live_fill(ev, "cli", "B1", broker, paper=True)

        # Each leg polled independently until its own fill showed up on attempt 6 --
        # not cut short, not run past it.
        assert list(broker.status_calls.values()) == [6, 6]

    asyncio.run(run())
