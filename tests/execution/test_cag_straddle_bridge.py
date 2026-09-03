"""
2026-08-27: unit tests for execution_bridge/cag_straddle_bridge.py
(CagStraddleExecutionBridge) -- fully standalone bridge for the CAG Long
Straddle strategy, written fresh (not subclassing any other strategy's
bridge), per strategies/cag_straddle/__init__.py's zero-shared-runtime
mandate. Mirrors tests/execution/test_oi_orb_bridge.py's coverage style.
"""
import asyncio
from datetime import date

import pytest

from config.global_config import Topic
from execution_bridge.cag_straddle_bridge import CagStraddleExecutionBridge
from strategies.cag_straddle.events import CagStraddleOrderEvent
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


def _entry_ev(**overrides) -> CagStraddleOrderEvent:
    base = dict(
        client_id="ssrajpal2001", binding_id="SA5770", action="BUY",
        underlying="NIFTY", option_type="CE", strike=24500, expiry=date(2026, 9, 1),
        quantity=65, entry_price=100.0, sl_price=0.0, reason="cag_straddle_r1_breach",
        event_id="NIFTY_CE24500_ENTRY_1",
    )
    base.update(overrides)
    return CagStraddleOrderEvent(**base)


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
            "strategy_name": "cag_straddle",
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
    bridge = CagStraddleExecutionBridge.__new__(CagStraddleExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)

    class _FakeTradeLog:
        def log(self, *a, **kw):
            pass
    bridge._trade_log = _FakeTradeLog()

    await bridge._handle(_entry_ev())

    assert len(bus.published) == 1
    topic, fill = bus.published[0]
    assert topic == Topic.CAG_STRADDLE_ORDER_FILL
    assert fill.paper_mode is True
    assert fill.fill_price == 100.0


@pytest.mark.asyncio
async def test_no_paper_fallback_when_broker_missing_in_live_mode():
    db = _FakeDB(trading_mode="live")
    bus = _CapturingBus()
    bridge = CagStraddleExecutionBridge.__new__(CagStraddleExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = None

    calls = {"paper_fill": 0}
    async def _fake_paper_fill(ev):
        calls["paper_fill"] += 1
    bridge._paper_fill = _fake_paper_fill

    await bridge._handle(_entry_ev())

    assert calls["paper_fill"] == 0
    fills = [e for t, e in bus.published if t == Topic.CAG_STRADDLE_ORDER_FILL]
    assert len(fills) == 1
    assert fills[0].entry_aborted is True
    assert fills[0].routing_failed is True
    assert fills[0].fill_price == 0.0


@pytest.mark.asyncio
async def test_entry_aborted_when_can_trade_gate_closed():
    db = _FakeDB(trading_mode="live", is_trade_enabled=False)
    bus = _CapturingBus()
    bridge = CagStraddleExecutionBridge.__new__(CagStraddleExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)
    bridge._trade_log = None

    await bridge._handle(_entry_ev())

    fills = [e for t, e in bus.published if t == Topic.CAG_STRADDLE_ORDER_FILL]
    assert len(fills) == 1
    assert fills[0].entry_aborted is True
    assert fills[0].routing_failed is True


@pytest.mark.asyncio
async def test_sell_always_routes_even_with_gate_closed():
    """EXIT must always route -- gate only ever applies to entries."""
    db = _FakeDB(trading_mode="paper", is_trade_enabled=False)
    bus = _CapturingBus()
    bridge = CagStraddleExecutionBridge.__new__(CagStraddleExecutionBridge)
    bridge._bus = bus
    bridge._router = _FakeRouter(db)

    class _FakeTradeLog:
        def log(self, *a, **kw):
            pass
    bridge._trade_log = _FakeTradeLog()

    sell_ev = _entry_ev(action="SELL", exit_price=95.0, reason="sl_s1_breach@95.00",
                        event_id="NIFTY_CE24500_EXIT_1")
    await bridge._handle(sell_ev)

    fills = [e for t, e in bus.published if t == Topic.CAG_STRADDLE_ORDER_FILL]
    assert len(fills) == 1
    assert fills[0].entry_aborted is False
    assert fills[0].fill_price == 95.0
