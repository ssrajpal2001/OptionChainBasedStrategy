"""
Unit tests for execution_bridge/iron_fly_bridge.py (IronFlyExecutionBridge)
-- fully standalone, leg-centric bridge for the NIFTY Weekly Iron Condor ->
Iron Fly strategy. Mirrors tests/execution/test_cag_straddle_bridge.py's
coverage style, adapted for open/close-leg events instead of a single
BUY/SELL position lifecycle.
"""
import asyncio
from datetime import date

import pytest

from config.global_config import Topic
from execution_bridge.iron_fly_bridge import IronFlyExecutionBridge
from strategies.iron_fly.events import IronFlyOrderEvent
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


def _open_ev(**overrides) -> IronFlyOrderEvent:
    base = dict(
        client_id="ssrajpal2001", binding_id="SA5770",
        underlying="NIFTY", option_type="CE", strike=24100, expiry=date(2026, 9, 15),
        quantity=75, order_side="SELL", is_open=True, is_short=True, price=25.5,
        reason="entry", event_id="NIFTY_CE24100_OPEN_1",
    )
    base.update(overrides)
    return IronFlyOrderEvent(**base)


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
            "strategy_name": "iron_fly",
            "underlying": "NIFTY",
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
    bridge = IronFlyExecutionBridge.__new__(IronFlyExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)

    class _FakeTradeLog:
        def log(self, *a, **kw):
            pass
    bridge._trade_log = _FakeTradeLog()

    await bridge._handle(_open_ev())

    assert len(bus.published) == 1
    topic, fill = bus.published[0]
    assert topic == Topic.IRON_FLY_ORDER_FILL
    assert fill.paper_mode is True
    assert fill.fill_price == 25.5
    assert fill.is_open is True


@pytest.mark.asyncio
async def test_no_paper_fallback_when_broker_missing_in_live_mode():
    db = _FakeDB(trading_mode="live")
    bus = _CapturingBus()
    bridge = IronFlyExecutionBridge.__new__(IronFlyExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = None

    calls = {"paper_fill": 0}
    async def _fake_paper_fill(ev):
        calls["paper_fill"] += 1
    bridge._paper_fill = _fake_paper_fill

    await bridge._handle(_open_ev())

    assert calls["paper_fill"] == 0
    fills = [e for t, e in bus.published if t == Topic.IRON_FLY_ORDER_FILL]
    assert len(fills) == 1
    assert fills[0].aborted is True
    assert fills[0].routing_failed is True
    assert fills[0].fill_price == 0.0


@pytest.mark.asyncio
async def test_open_aborted_when_can_trade_gate_closed():
    db = _FakeDB(trading_mode="live", is_trade_enabled=False)
    bus = _CapturingBus()
    bridge = IronFlyExecutionBridge.__new__(IronFlyExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = None

    await bridge._handle(_open_ev())

    fills = [e for t, e in bus.published if t == Topic.IRON_FLY_ORDER_FILL]
    assert len(fills) == 1
    assert fills[0].aborted is True
    assert fills[0].routing_failed is True


@pytest.mark.asyncio
async def test_close_always_routes_even_with_gate_closed():
    """A CLOSE must always route -- the can_trade gate only ever applies to opens."""
    db = _FakeDB(trading_mode="paper", is_trade_enabled=False)
    bus = _CapturingBus()
    bridge = IronFlyExecutionBridge.__new__(IronFlyExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)

    class _FakeTradeLog:
        def log(self, *a, **kw):
            pass
    bridge._trade_log = _FakeTradeLog()

    close_ev = _open_ev(
        order_side="BUY", is_open=False, price=10.1, entry_price=25.5,
        reason="roll_call", event_id="NIFTY_CE24100_CLOSE_1",
    )
    await bridge._handle(close_ev)

    fills = [e for t, e in bus.published if t == Topic.IRON_FLY_ORDER_FILL]
    assert len(fills) == 1
    assert fills[0].aborted is False
    assert fills[0].fill_price == 10.1
    assert fills[0].is_open is False


@pytest.mark.asyncio
async def test_record_history_pnl_sign_for_short_vs_long_leg():
    """Short leg profits as premium falls; long leg profits as premium rises
    -- _record_history must apply the correct sign per leg type."""
    db = _FakeDB(trading_mode="paper")
    bus = _CapturingBus()
    bridge = IronFlyExecutionBridge.__new__(IronFlyExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)

    class _FakeTradeLog:
        def log(self, *a, **kw):
            pass
    bridge._trade_log = _FakeTradeLog()

    recorded = {}
    def _fake_record(client_id, strategy, underlying, entry, exit_, reason, pnl, **kw):
        recorded["pnl"] = pnl
    import data_layer.trade_history as th
    orig = th.record
    th.record = _fake_record
    try:
        short_close = _open_ev(
            order_side="BUY", is_open=False, is_short=True, price=10.1, entry_price=25.5,
            reason="roll_call",
        )
        await bridge._handle(short_close)
        assert recorded["pnl"] == round((25.5 - 10.1) * 75, 2)

        recorded.clear()
        long_close = _open_ev(
            order_side="SELL", is_open=False, is_short=False, price=8.15, entry_price=19.65,
            reason="roll_call",
        )
        await bridge._handle(long_close)
        assert recorded["pnl"] == round((8.15 - 19.65) * 75, 2)
    finally:
        th.record = orig
