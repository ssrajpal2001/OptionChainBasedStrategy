"""
2026-08-24: unit tests for execution_bridge/oi_orb_bridge.py
(OiOrbExecutionBridge) -- fully standalone bridge for the OI-Spurt + ORB
screener strategy, written fresh (not subclassing any other strategy's
bridge), per strategies/oi_orb_screener/__init__.py's zero-shared-runtime
mandate.

Mirrors tests/execution/test_oi_flow_bridge.py's coverage style, including
the critical live-fill-success regression guard (OrderRequest(broker_
symbol=...) vs. the wrong symbol= field name that shipped undetected in
the shared D1Trap/FVG bridge for 7 days earlier this project).
"""
import asyncio
from datetime import date

import pytest

from config.global_config import Topic
from execution_bridge.base_broker import OrderRequest
from execution_bridge.oi_orb_bridge import OiOrbExecutionBridge
from strategies.oi_orb_screener.events import OiOrbOrderEvent, OiOrbFillEvent
import strategies.core.gate as gate_module


@pytest.fixture(autouse=True)
def _reset_gate_cache():
    gate_module._cache.clear()
    yield
    gate_module._cache.clear()


class _CapturingBus:
    def __init__(self) -> None:
        self.published: list = []

    async def publish(self, topic, event):
        self.published.append((topic, event))

    def subscribe(self, topic):
        class _Q:
            async def get(self):
                await asyncio.sleep(3600)
        return _Q()


def _entry_ev(**overrides) -> OiOrbOrderEvent:
    base = dict(
        client_id="ssrajpal2001", binding_id="SA5770", action="BUY",
        underlying="MANAPPURAM", option_type="CE", strike=365, expiry=date(2026, 8, 27),
        quantity=6900, entry_price=8.5, reason="orb_high_breakout",
        event_id="MANAPPURAM_CE365_ENTRY_1",
    )
    base.update(overrides)
    return OiOrbOrderEvent(**base)


class _FakeDB:
    def __init__(self, trading_mode="live", terminal_connected=True, is_trade_enabled=True):
        self._trading_mode = trading_mode
        self._terminal_connected = terminal_connected
        self._is_trade_enabled = is_trade_enabled

    def get_bindings_safe_sync(self, client_id):
        return [{
            "binding_id": "SA5770",
            "trading_mode": self._trading_mode,
            "terminal_connected": self._terminal_connected,
            "is_trade_enabled": self._is_trade_enabled,
        }]

    def get_deployments_sync(self, client_id):
        return [{
            "binding_id": "SA5770",
            "strategy_name": "oi_orb_screener",
            "underlying": "SCREENER",
            "is_running": 1,
        }]


class _FakeRouter:
    def __init__(self, db):
        self._brokers = {}
        self._client_db = db


@pytest.mark.asyncio
async def test_paper_mode_local_sim_fill_no_broker_touch():
    db = _FakeDB(trading_mode="paper")
    bus = _CapturingBus()
    bridge = OiOrbExecutionBridge.__new__(OiOrbExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)

    class _FakeTradeLog:
        def log(self, *a, **kw):
            pass
    bridge._trade_log = _FakeTradeLog()

    await bridge._handle(_entry_ev())

    assert len(bus.published) == 1
    topic, fill = bus.published[0]
    assert topic == Topic.OI_ORB_ORDER_FILL
    assert fill.paper_mode is True
    assert fill.fill_price == 8.5


@pytest.mark.asyncio
async def test_no_paper_fallback_when_broker_missing_in_live_mode():
    db = _FakeDB(trading_mode="live")
    bus = _CapturingBus()
    bridge = OiOrbExecutionBridge.__new__(OiOrbExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = None

    calls = {"paper_fill": 0}
    async def _fake_paper_fill(ev):
        calls["paper_fill"] += 1
    bridge._paper_fill = _fake_paper_fill

    await bridge._handle(_entry_ev())

    assert calls["paper_fill"] == 0
    fills = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_FILL]
    assert len(fills) == 1
    assert fills[0].entry_aborted is True
    assert fills[0].routing_failed is True
    assert fills[0].fill_price == 0.0


@pytest.mark.asyncio
async def test_entry_aborted_when_can_trade_gate_closed():
    db = _FakeDB(trading_mode="live", is_trade_enabled=False)
    bus = _CapturingBus()
    bridge = OiOrbExecutionBridge.__new__(OiOrbExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = None

    await bridge._handle(_entry_ev())

    assert len(bus.published) == 1
    topic, fill = bus.published[0]
    assert fill.entry_aborted is True
    assert fill.routing_failed is True


@pytest.mark.asyncio
async def test_exit_still_routes_even_though_can_trade_gate_would_block_entry():
    db = _FakeDB(trading_mode="live", is_trade_enabled=False)
    bus = _CapturingBus()
    bridge = OiOrbExecutionBridge.__new__(OiOrbExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = None

    ev = _entry_ev(action="SELL", exit_price=9.2, event_id="MANAPPURAM_CE365_EXIT_1")
    await bridge._handle(ev)

    fills = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_FILL]
    assert len(fills) == 1
    assert fills[0].exit_failed is True
    assert fills[0].entry_aborted is False


@pytest.mark.asyncio
async def test_paper_route_sends_real_order_and_books_simulated_fill_on_no_fund_reject(monkeypatch):
    db = _FakeDB(trading_mode="paper_route")
    calls = {"place_order": 0}

    class _FakeRejectedFill:
        avg_price = 0.0

    class _FakeBroker:
        provider = "zerodha"
        async def place_order(self, req):
            calls["place_order"] += 1
            assert isinstance(req, OrderRequest)
            return "ORDER999"
        async def get_order_status(self, order_id):
            assert order_id == "ORDER999"
            return _FakeRejectedFill()

    async def _fake_resolve(bus, router, client_id, binding_id, strategy, context="", **kw):
        return _FakeBroker()
    monkeypatch.setattr("execution_bridge.oi_orb_bridge.resolve_broker_or_alert", _fake_resolve)

    class _FakeTradeLog:
        def log(self, *a, **kw):
            pass

    bus = _CapturingBus()
    bridge = OiOrbExecutionBridge.__new__(OiOrbExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = _FakeTradeLog()
    bridge._resolve_symbol = lambda ev, broker: "MANAPPURAM26AUG365CE"

    await bridge._handle(_entry_ev())

    assert calls["place_order"] == 1
    fills = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_FILL]
    assert len(fills) == 1
    fill = fills[0]
    assert fill.entry_aborted is False
    assert fill.paper_mode is True
    assert fill.fill_price == 8.5


@pytest.mark.asyncio
async def test_paper_route_books_real_avg_price_when_broker_actually_fills(monkeypatch):
    db = _FakeDB(trading_mode="paper_route")

    class _FakeRealFill:
        avg_price = 8.65

    class _FakeBroker:
        provider = "zerodha"
        async def place_order(self, req):
            return "ORDER111"
        async def get_order_status(self, order_id):
            return _FakeRealFill()

    async def _fake_resolve(bus, router, client_id, binding_id, strategy, context="", **kw):
        return _FakeBroker()
    monkeypatch.setattr("execution_bridge.oi_orb_bridge.resolve_broker_or_alert", _fake_resolve)

    class _FakeTradeLog:
        def log(self, *a, **kw):
            pass

    bus = _CapturingBus()
    bridge = OiOrbExecutionBridge.__new__(OiOrbExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = _FakeTradeLog()
    bridge._resolve_symbol = lambda ev, broker: "MANAPPURAM26AUG365CE"

    await bridge._handle(_entry_ev())

    fills = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_FILL]
    assert len(fills) == 1
    fill = fills[0]
    assert fill.paper_mode is True
    assert fill.fill_price == 8.65


@pytest.mark.asyncio
async def test_paper_route_broker_unresolvable_still_aborts():
    db = _FakeDB(trading_mode="paper_route")
    bus = _CapturingBus()
    bridge = OiOrbExecutionBridge.__new__(OiOrbExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = None

    await bridge._handle(_entry_ev())   # no resolve_broker_or_alert patch -- real one runs, fails

    fills = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_FILL]
    assert len(fills) == 1
    assert fills[0].entry_aborted is True
    assert fills[0].routing_failed is True


@pytest.mark.asyncio
async def test_live_fill_success_reaches_broker_and_publishes_real_fill(monkeypatch):
    """Critical regression guard: drives _handle() through a real resolved+
    filling broker, all the way through OrderRequest construction -- would
    catch the exact symbol= vs broker_symbol= class of bug found in the
    shared D1Trap/FVG bridge earlier this project."""
    db = _FakeDB(trading_mode="live")
    captured = {}

    class _FakeFill:
        avg_price = 9.15

    class _FakeBroker:
        provider = "zerodha"
        async def place_order(self, req):
            assert isinstance(req, OrderRequest)
            captured["req"] = req
            return "ORDER456"
        async def get_order_status(self, order_id):
            assert order_id == "ORDER456"
            return _FakeFill()

    async def _fake_resolve(bus, router, client_id, binding_id, strategy, context="", **kw):
        return _FakeBroker()
    monkeypatch.setattr("execution_bridge.oi_orb_bridge.resolve_broker_or_alert", _fake_resolve)

    class _FakeTradeLog:
        def log(self, *a, **kw):
            pass

    bus = _CapturingBus()
    bridge = OiOrbExecutionBridge.__new__(OiOrbExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = _FakeTradeLog()
    bridge._resolve_symbol = lambda ev, broker: "MANAPPURAM26AUG365CE"

    await bridge._handle(_entry_ev())   # must not raise

    assert captured["req"].broker_symbol == "MANAPPURAM26AUG365CE"
    fills = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_FILL]
    assert len(fills) == 1
    fill = fills[0]
    assert isinstance(fill, OiOrbFillEvent)
    assert fill.paper_mode is False
    assert fill.fill_price == 9.15
    assert fill.symbol == "MANAPPURAM26AUG365CE"


@pytest.mark.asyncio
async def test_live_fill_partial_fill_reports_actual_filled_qty_not_requested(monkeypatch):
    db = _FakeDB(trading_mode="live")

    class _FakePartialFill:
        avg_price = 9.15
        qty = 3450   # requested 6900 (see _entry_ev's default quantity)

    class _FakeBroker:
        provider = "zerodha"
        async def place_order(self, req):
            return "ORDER222"
        async def get_order_status(self, order_id):
            return _FakePartialFill()

    async def _fake_resolve(bus, router, client_id, binding_id, strategy, context="", **kw):
        return _FakeBroker()
    monkeypatch.setattr("execution_bridge.oi_orb_bridge.resolve_broker_or_alert", _fake_resolve)

    class _FakeTradeLog:
        def log(self, *a, **kw):
            pass

    bus = _CapturingBus()
    bridge = OiOrbExecutionBridge.__new__(OiOrbExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = _FakeTradeLog()
    bridge._resolve_symbol = lambda ev, broker: "MANAPPURAM26AUG365CE"

    await bridge._handle(_entry_ev())

    fills = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_FILL]
    assert len(fills) == 1
    fill = fills[0]
    assert fill.qty == 6900
    assert fill.filled_qty == 3450
    assert fill.entry_aborted is False


@pytest.mark.asyncio
async def test_live_fill_zero_avg_price_aborts_not_fabricates(monkeypatch):
    db = _FakeDB(trading_mode="live")

    class _FakeZeroFill:
        avg_price = 0.0

    class _FakeBroker:
        provider = "zerodha"
        async def place_order(self, req):
            return "ORDER789"
        async def get_order_status(self, order_id):
            return _FakeZeroFill()

    async def _fake_resolve(bus, router, client_id, binding_id, strategy, context="", **kw):
        return _FakeBroker()
    monkeypatch.setattr("execution_bridge.oi_orb_bridge.resolve_broker_or_alert", _fake_resolve)

    class _FakeTradeLog:
        def log(self, *a, **kw):
            pass

    bus = _CapturingBus()
    bridge = OiOrbExecutionBridge.__new__(OiOrbExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = _FakeTradeLog()
    bridge._resolve_symbol = lambda ev, broker: "MANAPPURAM26AUG365CE"

    await bridge._handle(_entry_ev())

    assert len(bus.published) == 1
    topic, fill = bus.published[0]
    assert fill.entry_aborted is True
    assert fill.fill_price == 0.0
