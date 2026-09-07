"""Phase 1 of per-binding refactor: a StraddleOrderEvent stamped with client_id/binding_id must
route to ONLY that broker (no mirror). An unstamped event keeps the legacy route-to-all behaviour."""
import asyncio
import pytest

from execution_bridge.straddle_bridge import StraddleExecutionBridge, StraddleOrderEvent
from data_layer.base_feeder import EventBus


class _Client:
    def __init__(self, cid): self.client_id = cid


class _Registry:
    def __init__(self, cids): self._cs = [_Client(c) for c in cids]
    def all_active(self): return self._cs


class _DB:
    # Two clients A and B, each one binding, both engine_active+terminal+deployed on NIFTY.
    def get_bindings_safe_sync(self, cid):
        return [{"binding_id": f"{cid}_b1", "engine_active": True,
                 "terminal_connected": True, "is_trade_enabled": True,
                 "trading_mode": "paper"}]
    def get_deployments_sync(self, cid):
        return [{"binding_id": f"{cid}_b1", "strategy_name": "sell_straddle",
                 "underlying": "NIFTY", "is_running": 1}]


class _Router:
    def __init__(self): self._client_db = _DB(); self._brokers = {}


def _bridge():
    b = StraddleExecutionBridge(EventBus(), _Registry(["A", "B"]), _Router())
    routed = []
    async def _fake_fill(ev, cid, bid, broker):
        routed.append((cid, bid))
    b._paper_fill = _fake_fill
    return b, routed


def _ev(**kw):
    return StraddleOrderEvent(action="ENTRY", underlying="NIFTY", atm=23000,
                              ce_strike=23000, pe_strike=23000, ce_ltp=100, pe_ltp=100, **kw)


def test_untagged_routes_to_all():
    b, routed = _bridge()
    asyncio.run(b._handle(_ev()))
    assert set(routed) == {("A", "A_b1"), ("B", "B_b1")}   # legacy mirror-to-all


def test_tagged_routes_to_only_that_binding():
    b, routed = _bridge()
    asyncio.run(b._handle(_ev(client_id="A", binding_id="A_b1")))
    assert routed == [("A", "A_b1")]                       # ONLY the stamped binding


def test_tagged_other_client_not_touched():
    b, routed = _bridge()
    asyncio.run(b._handle(_ev(client_id="B", binding_id="B_b1")))
    assert routed == [("B", "B_b1")]


class _DBCalcVwap:
    """A_b1's only deployment is 'sell_straddle_calc_vwap', not 'sell_straddle'."""
    def get_bindings_safe_sync(self, cid):
        return [{"binding_id": f"{cid}_b1", "engine_active": True,
                 "terminal_connected": True, "is_trade_enabled": True,
                 "trading_mode": "paper"}]
    def get_deployments_sync(self, cid):
        return [{"binding_id": f"{cid}_b1", "strategy_name": "sell_straddle_calc_vwap",
                 "underlying": "NIFTY", "is_running": 1}]


def test_calc_vwap_event_routes_using_its_own_strategy_name():
    """2026-09-07 real incident: this can_trade() gate was hardcoded to
    "sell_straddle" regardless of the event's own strategy_name -- a
    sell_straddle_calc_vwap order could never find its OWN running deployment
    row under that literal, so it always `continue`d before ever reaching the
    broker, surfacing downstream as a generic "no engine-active brokers found"
    -> routing_failed=True abort. The event must carry its real
    strategy_name (stamped by SellStraddleStrategy._emit_order) for this gate
    to find the right deployment row."""
    router = _Router()
    router._client_db = _DBCalcVwap()
    b = StraddleExecutionBridge(EventBus(), _Registry(["A"]), router)
    routed = []
    async def _fake_fill(ev, cid, bid, broker):
        routed.append((cid, bid))
    b._paper_fill = _fake_fill
    ev = _ev(client_id="A", binding_id="A_b1", strategy_name="sell_straddle_calc_vwap")
    asyncio.run(b._handle(ev))
    assert routed == [("A", "A_b1")]
