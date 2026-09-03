import asyncio
import pytest

from config.global_config import Topic


class _CapturingBus:
    """Fake EventBus that records every published (topic, event) pair instead of
    a bare counter -- lets the new tests below inspect the actual
    FVGOrderFillEvent the bridge publishes, not just that "something" was
    published."""

    def __init__(self) -> None:
        self.published: list = []

    async def publish(self, topic, event):
        self.published.append((topic, event))

    def subscribe(self, topic):
        class _Q:
            async def get(self):
                await asyncio.sleep(3600)
        return _Q()


class _Ev:
    """Minimal FVGOrderEvent stand-in, event_id included (2026-08-05:
    FVGOrderEvent grew an event_id field for the confirm-then-finalize fill
    loop) -- tests override class attrs per-instance as needed."""
    action = "SELL"
    client_id = "gurmeet"
    binding_id = "zerodha"
    underlying = "NIFTY"
    option_type = "PE"
    strike = 24600
    expiry = "2026-08-06"
    quantity = 75
    entry_price = 100.0
    exit_price = 90.0
    reason = "sl_hit"
    event_id = "NIFTY_PE24600_EXIT_1"


@pytest.mark.asyncio
async def test_no_paper_fallback_when_broker_missing_in_live_mode(monkeypatch):
    """
    Real routing entry point is FVGExecutionBridge._handle(ev) -- it computes
    live_binding/db internally from self._router. This test drives it through
    that real path: a live-mode binding whose broker never resolves must NOT
    fall back to _paper_fill, and must publish a SYSTEM_EVENT alert instead.
    """
    from execution_bridge.fvg_bridge import FVGExecutionBridge

    calls = {"paper_fill": 0, "alerts": 0}

    class _FakeBus:
        async def publish(self, topic, event):
            calls["alerts"] += 1

        def subscribe(self, topic):
            class _Q:
                async def get(self):
                    await asyncio.sleep(3600)
            return _Q()

    class _FakeDB:
        def get_bindings_safe_sync(self, client_id):
            return [{
                "binding_id": "zerodha",
                "trading_mode": "live",
                "terminal_connected": True,
                "engine_active": True,
                "is_trade_enabled": True,
            }]

        def get_deployments_sync(self, client_id):
            return [{
                "binding_id": "zerodha",
                "strategy_name": "fvg",
                "underlying": "NIFTY",
                "is_running": 1,
            }]

    class _FakeRouter:
        _brokers = {}  # always empty -- broker never resolves
        _client_db = _FakeDB()

    bridge = FVGExecutionBridge.__new__(FVGExecutionBridge)
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
        reason = "fvg_retest"

    await bridge._handle(_Ev())

    assert calls["paper_fill"] == 0, "must never fabricate a fill when broker is unavailable in live mode"
    assert calls["alerts"] >= 1, "must publish a SYSTEM_EVENT alert instead"


@pytest.mark.asyncio
async def test_paper_mode_still_local_sim_untouched(monkeypatch):
    """mode == 'paper' must still go straight to _paper_fill without ever
    touching resolve_broker_or_alert (no alert, pure local simulation)."""
    from execution_bridge.fvg_bridge import FVGExecutionBridge

    calls = {"paper_fill": 0, "alerts": 0}

    class _FakeBus:
        async def publish(self, topic, event):
            calls["alerts"] += 1

        def subscribe(self, topic):
            class _Q:
                async def get(self):
                    await asyncio.sleep(3600)
            return _Q()

    class _FakeDB:
        def get_bindings_safe_sync(self, client_id):
            return [{
                "binding_id": "zerodha",
                "trading_mode": "paper",
                "terminal_connected": True,
                "engine_active": True,
                "is_trade_enabled": True,
            }]

        def get_deployments_sync(self, client_id):
            return [{
                "binding_id": "zerodha",
                "strategy_name": "fvg",
                "underlying": "NIFTY",
                "is_running": 1,
            }]

    class _FakeRouter:
        _brokers = {}
        _client_db = _FakeDB()

    bridge = FVGExecutionBridge.__new__(FVGExecutionBridge)
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
        reason = "fvg_retest"

    await bridge._handle(_Ev())

    assert calls["paper_fill"] == 1
    assert calls["alerts"] == 0


# ── 2026-08-05: confirm-then-finalize fill-confirmation feedback loop ────────
# Previously this bridge NEVER published to Topic.FVG_ORDER_FILL at all -- the
# three "can't route" cases below just logged and returned. strategies/fvg/
# engine.py now dispatches BUY/SELL and blocks on a waiter keyed by event_id
# (see _open_position/_square_off) -- a silent return here would leave that
# waiter hanging until its own timeout, and worse, a caller written against
# the OLD "believe every order succeeded" assumption would never learn the
# order never reached the broker. These tests assert the bridge now publishes
# an FVGOrderFillEvent (exit_failed=True / entry_aborted=True) on every one of
# the three no-route paths, matching d1_trap_bridge.py's _abort() contract
# exactly.


@pytest.mark.asyncio
async def test_exit_publishes_exit_failed_fill_when_terminal_disconnected():
    from execution_bridge.fvg_bridge import FVGExecutionBridge, FVGOrderFillEvent

    class _FakeDB:
        def get_bindings_safe_sync(self, client_id):
            return [{
                "binding_id": "zerodha",
                "trading_mode": "live",
                "terminal_connected": False,
                "engine_active": True,
                "is_trade_enabled": True,
            }]

    class _FakeRouter:
        _brokers = {}
        _client_db = _FakeDB()

    bus = _CapturingBus()
    bridge = FVGExecutionBridge.__new__(FVGExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter()
    bridge._trade_log = None

    ev = _Ev()  # action="SELL" by default
    await bridge._handle(ev)

    assert len(bus.published) == 1
    topic, fill = bus.published[0]
    assert topic == Topic.FVG_ORDER_FILL
    assert isinstance(fill, FVGOrderFillEvent)
    assert fill.exit_failed is True
    assert fill.entry_aborted is False
    assert fill.routing_failed is True
    assert fill.event_id == ev.event_id
    assert fill.fill_price == 0.0


@pytest.mark.asyncio
async def test_entry_publishes_entry_aborted_fill_when_can_trade_gate_closed():
    from execution_bridge.fvg_bridge import FVGExecutionBridge, FVGOrderFillEvent

    class _FakeDB:
        def get_bindings_safe_sync(self, client_id):
            return [{
                "binding_id": "zerodha",
                "trading_mode": "live",
                "terminal_connected": True,
                "engine_active": True,
                "is_trade_enabled": False,  # gate closed
            }]

        def get_deployments_sync(self, client_id):
            return [{
                "binding_id": "zerodha",
                "strategy_name": "fvg",
                "underlying": "NIFTY",
                "is_running": 1,
            }]

    class _FakeRouter:
        _brokers = {}
        _client_db = _FakeDB()

    bus = _CapturingBus()
    bridge = FVGExecutionBridge.__new__(FVGExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter()
    bridge._trade_log = None

    class _BuyEv(_Ev):
        action = "BUY"
        event_id = "NIFTY_PE24600_ENTRY_1"

    ev = _BuyEv()
    await bridge._handle(ev)

    assert len(bus.published) == 1
    topic, fill = bus.published[0]
    assert topic == Topic.FVG_ORDER_FILL
    assert isinstance(fill, FVGOrderFillEvent)
    assert fill.entry_aborted is True
    assert fill.exit_failed is False
    assert fill.event_id == ev.event_id


@pytest.mark.asyncio
async def test_exit_publishes_exit_failed_fill_when_broker_unresolved_live(monkeypatch):
    from execution_bridge.fvg_bridge import FVGExecutionBridge, FVGOrderFillEvent

    class _FakeDB:
        def get_bindings_safe_sync(self, client_id):
            return [{
                "binding_id": "zerodha",
                "trading_mode": "live",
                "terminal_connected": True,
                "engine_active": True,
                "is_trade_enabled": True,
            }]

    class _FakeRouter:
        _brokers = {}  # always empty -- broker never resolves
        _client_db = _FakeDB()

    async def _fake_resolve(bus, router, client_id, binding_id, strategy, context="", **kw):
        return None
    monkeypatch.setattr(
        "execution_bridge.broker_resolve.resolve_broker_or_alert", _fake_resolve,
    )

    bus = _CapturingBus()
    bridge = FVGExecutionBridge.__new__(FVGExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter()
    bridge._trade_log = None

    ev = _Ev()  # action="SELL", so no can_trade gate check
    await bridge._handle(ev)

    assert len(bus.published) == 1
    topic, fill = bus.published[0]
    assert topic == Topic.FVG_ORDER_FILL
    assert fill.exit_failed is True
    assert fill.event_id == ev.event_id
