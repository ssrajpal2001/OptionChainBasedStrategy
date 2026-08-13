"""
2026-08-12: unit tests for execution_bridge/oi_flow_bridge.py
(OIFlowExecutionBridge) -- fully standalone bridge for the OI-Flow
Pre-Breakout strategy, written fresh (not subclassing option_buyer_
bridge_base.OptionBuyerExecutionBridge), per explicit user direction.

Mirrors tests/execution/test_d1trap_bridge_fail_loud.py's coverage style
and, critically, its LATEST addition: a real live-fill-success test that
drives _handle() all the way through OrderRequest construction with a
broker that actually resolves and fills. That exact gap (every other test
in every bridge only covered abort/reject paths, none drove a successful
live fill through to OrderRequest) is what let a real bug -- OrderRequest
(symbol=...) instead of the dataclass's actual field, broker_symbol= --
ship for 7 days behind a fully green suite in the SHARED D1Trap/FVG
bridge earlier this session. This fresh, standalone bridge must not repeat
that gap.
"""
import asyncio
from datetime import date

import pytest

from config.global_config import Topic
from execution_bridge.base_broker import OrderRequest
from execution_bridge.oi_flow_bridge import OIFlowExecutionBridge
from strategies.oi_flow.events import OIFlowOrderEvent, OIFlowFillEvent
import strategies.core.gate as gate_module


@pytest.fixture(autouse=True)
def _reset_gate_cache():
    # strategies.core.gate.can_trade() caches by (..., id(client_db)) for
    # 5s. Every test here builds a fresh, short-lived _FakeDB(); CPython
    # can reuse a just-GC'd object's id() for the next one, so without
    # clearing this between tests a later test's gate check can silently
    # read an earlier test's cached (and possibly opposite) result.
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


def _entry_ev(**overrides) -> OIFlowOrderEvent:
    base = dict(
        client_id="ssrajpal2001", binding_id="SA5770", action="BUY",
        underlying="BANKNIFTY", option_type="CE", strike=57700, expiry=date(2026, 8, 25),
        quantity=30, entry_price=480.0, sl_price=450.0, reason="oi_flow_pre_breakout",
        event_id="BANKNIFTY_CE57700_ENTRY_1",
    )
    base.update(overrides)
    return OIFlowOrderEvent(**base)


class _FakeDB:
    def __init__(self, trading_mode="live", terminal_connected=True, is_trade_enabled=True,
                 running=True):
        # trading_mode also accepts "paper_route": order routes to a REAL
        # broker (verifies routing/whitelist) but the strategy's own fill
        # is always simulated -- see execution_bridge/oi_flow_bridge.py's
        # _live_fill(paper_route=...) docstring.
        self._trading_mode = trading_mode
        self._terminal_connected = terminal_connected
        self._is_trade_enabled = is_trade_enabled
        self._running = running

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
            "strategy_name": "oi_flow",
            "underlying": "BANKNIFTY",
            "is_running": 1 if self._running else 0,
        }]


class _FakeRouter:
    def __init__(self, db):
        self._brokers = {}
        self._client_db = db


@pytest.mark.asyncio
async def test_no_paper_fallback_when_broker_missing_in_live_mode():
    """A live-mode binding whose broker never resolves must NOT fall back
    to _paper_fill (that would fabricate a fill), and must abort loudly."""
    db = _FakeDB(trading_mode="live")
    bus = _CapturingBus()
    bridge = OIFlowExecutionBridge.__new__(OIFlowExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = None

    calls = {"paper_fill": 0}
    async def _fake_paper_fill(ev):
        calls["paper_fill"] += 1
    bridge._paper_fill = _fake_paper_fill

    await bridge._handle(_entry_ev())

    assert calls["paper_fill"] == 0
    fills = [e for t, e in bus.published if t == Topic.OI_FLOW_ORDER_FILL]
    assert len(fills) == 1
    fill = fills[0]
    assert isinstance(fill, OIFlowFillEvent)
    assert fill.entry_aborted is True
    assert fill.routing_failed is True
    assert fill.fill_price == 0.0


@pytest.mark.asyncio
async def test_paper_mode_local_sim_fill_no_broker_touch():
    """mode == 'paper' must go straight to a local sim fill without ever
    touching broker resolution."""
    db = _FakeDB(trading_mode="paper")
    bus = _CapturingBus()
    bridge = OIFlowExecutionBridge.__new__(OIFlowExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)

    class _FakeTradeLog:
        def log(self, *a, **kw):
            pass
    bridge._trade_log = _FakeTradeLog()

    await bridge._handle(_entry_ev())

    assert len(bus.published) == 1
    topic, fill = bus.published[0]
    assert topic == Topic.OI_FLOW_ORDER_FILL
    assert fill.paper_mode is True
    assert fill.fill_price == 480.0   # ev.entry_price, since action="BUY"


@pytest.mark.asyncio
async def test_entry_aborted_when_can_trade_gate_closed(monkeypatch):
    db = _FakeDB(trading_mode="live", is_trade_enabled=False)   # gate closed
    bus = _CapturingBus()
    bridge = OIFlowExecutionBridge.__new__(OIFlowExecutionBridge)
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
    """EXIT (SELL) must always route regardless of the can_trade() gate --
    only ENTRY (BUY) is gated. Broker unavailable here still exercises the
    routing-failure path, but critically the gate check itself must never
    be consulted for a SELL."""
    db = _FakeDB(trading_mode="live", is_trade_enabled=False)
    bus = _CapturingBus()
    bridge = OIFlowExecutionBridge.__new__(OIFlowExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = None

    ev = _entry_ev(action="SELL", exit_price=520.0, event_id="BANKNIFTY_CE57700_EXIT_1")
    await bridge._handle(ev)

    fills = [e for t, e in bus.published if t == Topic.OI_FLOW_ORDER_FILL]
    assert len(fills) == 1
    fill = fills[0]
    assert fill.exit_failed is True   # routing failed (no broker), not gate-blocked
    assert fill.entry_aborted is False


@pytest.mark.asyncio
async def test_paper_route_sends_real_order_and_books_simulated_fill_on_no_fund_reject(monkeypatch):
    """The client's whole reason for paper_route: verify the order genuinely
    reaches their real broker (place_order IS called), while a no-fund
    rejection (avg_price<=0) must NOT abort the strategy -- it books a
    LOCAL SIMULATED fill at the strategy's own entry_price, same as pure
    'paper' mode's fill, just with real broker contact attempted+logged."""
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
    monkeypatch.setattr("execution_bridge.oi_flow_bridge.resolve_broker_or_alert", _fake_resolve)

    class _FakeTradeLog:
        def log(self, *a, **kw):
            pass

    bus = _CapturingBus()
    bridge = OIFlowExecutionBridge.__new__(OIFlowExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = _FakeTradeLog()
    bridge._resolve_symbol = lambda ev, broker: "BANKNIFTY25AUG57700CE"

    await bridge._handle(_entry_ev())

    assert calls["place_order"] == 1   # the real order DID reach the broker
    fills = [e for t, e in bus.published if t == Topic.OI_FLOW_ORDER_FILL]
    assert len(fills) == 1
    fill = fills[0]
    assert fill.entry_aborted is False   # never aborted -- simulated fill instead
    assert fill.paper_mode is True
    assert fill.fill_price == 480.0   # ev.entry_price (simulated), not the broker's 0.0


@pytest.mark.asyncio
async def test_paper_route_books_real_avg_price_when_broker_actually_fills(monkeypatch):
    """If the paper_route account happens to have funds and the broker DOES
    confirm a real fill, use that real avg_price (still tagged paper_mode
    since the binding itself is paper_route, not live)."""
    db = _FakeDB(trading_mode="paper_route")

    class _FakeRealFill:
        avg_price = 481.5

    class _FakeBroker:
        provider = "zerodha"
        async def place_order(self, req):
            return "ORDER111"
        async def get_order_status(self, order_id):
            return _FakeRealFill()

    async def _fake_resolve(bus, router, client_id, binding_id, strategy, context="", **kw):
        return _FakeBroker()
    monkeypatch.setattr("execution_bridge.oi_flow_bridge.resolve_broker_or_alert", _fake_resolve)

    class _FakeTradeLog:
        def log(self, *a, **kw):
            pass

    bus = _CapturingBus()
    bridge = OIFlowExecutionBridge.__new__(OIFlowExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = _FakeTradeLog()
    bridge._resolve_symbol = lambda ev, broker: "BANKNIFTY25AUG57700CE"

    await bridge._handle(_entry_ev())

    fills = [e for t, e in bus.published if t == Topic.OI_FLOW_ORDER_FILL]
    assert len(fills) == 1
    fill = fills[0]
    assert fill.paper_mode is True
    assert fill.fill_price == 481.5   # the real confirmed avg_price, not entry_price


@pytest.mark.asyncio
async def test_paper_route_broker_unresolvable_still_aborts(monkeypatch):
    """paper_route is not a blanket 'never fail' mode -- a genuinely
    UNRESOLVABLE broker (bad creds, terminal down) must still abort loudly,
    same as live. Only an order that reached the broker and came back
    unconfirmed gets the simulated-fill treatment."""
    db = _FakeDB(trading_mode="paper_route")
    bus = _CapturingBus()
    bridge = OIFlowExecutionBridge.__new__(OIFlowExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = None

    await bridge._handle(_entry_ev())   # no resolve_broker_or_alert patch -- real one runs, fails

    fills = [e for t, e in bus.published if t == Topic.OI_FLOW_ORDER_FILL]
    assert len(fills) == 1
    assert fills[0].entry_aborted is True
    assert fills[0].routing_failed is True


@pytest.mark.asyncio
async def test_live_fill_success_reaches_broker_and_publishes_real_fill(monkeypatch):
    """The critical regression guard: drives _handle() through a real
    resolved+filling broker, all the way through OrderRequest construction
    -- would have caught the exact symbol= vs broker_symbol= class of bug
    found elsewhere in this codebase this session."""
    db = _FakeDB(trading_mode="live")
    captured = {}

    class _FakeFill:
        avg_price = 482.35

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
    # oi_flow_bridge.py does `from execution_bridge.broker_resolve import
    # resolve_broker_or_alert` -- that binds a COPY of the reference into
    # oi_flow_bridge's own module namespace, so patching the original
    # execution_bridge.broker_resolve.resolve_broker_or_alert would NOT
    # affect the bridge's already-bound name. Patch it where it's actually
    # looked up: execution_bridge.oi_flow_bridge.resolve_broker_or_alert.
    monkeypatch.setattr(
        "execution_bridge.oi_flow_bridge.resolve_broker_or_alert", _fake_resolve,
    )

    class _FakeTradeLog:
        def log(self, *a, **kw):
            pass

    bus = _CapturingBus()
    bridge = OIFlowExecutionBridge.__new__(OIFlowExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = _FakeTradeLog()
    bridge._resolve_symbol = lambda ev, broker: "BANKNIFTY25AUG57700CE"

    await bridge._handle(_entry_ev())   # must not raise

    assert captured["req"].broker_symbol == "BANKNIFTY25AUG57700CE"
    fills = [e for t, e in bus.published if t == Topic.OI_FLOW_ORDER_FILL]
    assert len(fills) == 1
    fill = fills[0]
    assert isinstance(fill, OIFlowFillEvent)
    assert fill.paper_mode is False
    assert fill.fill_price == 482.35
    assert fill.symbol == "BANKNIFTY25AUG57700CE"


@pytest.mark.asyncio
async def test_live_fill_partial_fill_reports_actual_filled_qty_not_requested(monkeypatch):
    """A broker that only fills PART of the requested lots (real, not
    uncommon on a moderately-liquid strike for a MARKET order) must never
    be silently reported as a full fill -- the published event's
    filled_qty must reflect what actually filled (15), not qty (30, the
    requested amount)."""
    db = _FakeDB(trading_mode="live")

    class _FakePartialFill:
        avg_price = 482.35
        qty = 15   # requested 30 (see _entry_ev's default quantity)

    class _FakeBroker:
        provider = "zerodha"
        async def place_order(self, req):
            return "ORDER222"
        async def get_order_status(self, order_id):
            return _FakePartialFill()

    async def _fake_resolve(bus, router, client_id, binding_id, strategy, context="", **kw):
        return _FakeBroker()
    monkeypatch.setattr("execution_bridge.oi_flow_bridge.resolve_broker_or_alert", _fake_resolve)

    class _FakeTradeLog:
        def log(self, *a, **kw):
            pass

    bus = _CapturingBus()
    bridge = OIFlowExecutionBridge.__new__(OIFlowExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = _FakeTradeLog()
    bridge._resolve_symbol = lambda ev, broker: "BANKNIFTY25AUG57700CE"

    await bridge._handle(_entry_ev())   # quantity=30 in _entry_ev's defaults

    fills = [e for t, e in bus.published if t == Topic.OI_FLOW_ORDER_FILL]
    assert len(fills) == 1
    fill = fills[0]
    assert fill.qty == 30          # the ORIGINAL requested quantity, unchanged
    assert fill.filled_qty == 15   # what ACTUALLY filled
    assert fill.entry_aborted is False   # a partial fill is still a real, reportable fill -- not an abort


@pytest.mark.asyncio
async def test_live_fill_zero_avg_price_aborts_not_fabricates():
    """A broker call that 'succeeds' (no exception) but confirms avg_price
    <= 0 (rejected/zero-fill) must still abort, never publish a fake fill."""
    db = _FakeDB(trading_mode="live")

    class _FakeZeroFill:
        avg_price = 0.0

    class _FakeBroker:
        provider = "zerodha"
        async def place_order(self, req):
            return "ORDER789"
        async def get_order_status(self, order_id):
            return _FakeZeroFill()

    bus = _CapturingBus()
    bridge = OIFlowExecutionBridge.__new__(OIFlowExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)

    class _FakeTradeLog:
        def log(self, *a, **kw):
            pass
    bridge._trade_log = _FakeTradeLog()
    bridge._resolve_symbol = lambda ev, broker: "BANKNIFTY25AUG57700CE"

    async def _fake_resolve(bus, router, client_id, binding_id, strategy, context="", **kw):
        return _FakeBroker()
    import execution_bridge.oi_flow_bridge as m
    orig = m.resolve_broker_or_alert
    m.resolve_broker_or_alert = _fake_resolve
    try:
        await bridge._handle(_entry_ev())
    finally:
        m.resolve_broker_or_alert = orig

    assert len(bus.published) == 1
    topic, fill = bus.published[0]
    assert fill.paper_mode is True or fill.entry_aborted is True   # never a real, unconfirmed fill
    assert fill.fill_price == 0.0
    assert fill.entry_aborted is True
