"""
StraddleExecutionBridge must never fake a fill when the broker DOES resolve
and the order IS submitted, but the broker REJECTS it (e.g. Fyers: "Algo
orders are not allowed from this app"). This is a different failure mode
from tests/execution/test_straddle_bridge_fail_loud.py (broker unresolvable
entirely) -- here the broker instance exists and resolve_broker_or_alert
succeeds, but SmartOrderExecutor.execute_leg raises for every leg.

2026-08-05 live incident: exactly this happened for a real client's Fyers
binding on both ENTRY (14:59) and EXIT (15:20) -- both legs rejected
identically. The ENTRY atomicity guard only fired on ASYMMETRIC fills
(`if _any_filled and len(_full) < len(_legs)`), so a SYMMETRIC total
failure (_any_filled empty) fell straight through and was reported as a
normal successful entry/exit at the fallback LTP price -- a fully
fabricated position, no real order ever reached the broker.
"""
import asyncio

from execution_bridge.straddle_bridge import StraddleExecutionBridge, StraddleOrderEvent
from data_layer.base_feeder import EventBus
from config.global_config import Topic


class _Client:
    def __init__(self, cid):
        self.client_id = cid


class _Registry:
    def __init__(self, cids):
        self._cs = [_Client(c) for c in cids]

    def all_active(self):
        return self._cs


class _DB:
    def __init__(self, mode="live"):
        self._mode = mode

    def get_bindings_safe_sync(self, cid):
        return [{"binding_id": f"{cid}_b1", "engine_active": True,
                 "terminal_connected": True, "is_trade_enabled": True,
                 "trading_mode": self._mode}]

    def get_deployments_sync(self, cid):
        return [{"binding_id": f"{cid}_b1", "strategy_name": "sell_straddle",
                 "underlying": "NIFTY", "is_running": 1}]


class _FakeBroker:
    """A resolvable broker instance -- resolve_broker_or_alert just needs
    router._brokers[cid][bid] to be non-None."""
    provider = "fyers"


class _Router:
    def __init__(self, mode="live"):
        self._client_db = _DB(mode)
        self._brokers = {"A": {"A_b1": _FakeBroker()}}


def _ev(action="ENTRY", **kw):
    return StraddleOrderEvent(action=action, underlying="NIFTY", atm=23000,
                               ce_strike=23000, pe_strike=23000, ce_ltp=100, pe_ltp=100,
                               client_id="A", binding_id="A_b1", event_id="evt1", **kw)


def _bridge_with_rejecting_executor(mode="live"):
    bus = EventBus()
    b = StraddleExecutionBridge(bus, _Registry(["A"]), _Router(mode))
    fills = []
    q = bus.subscribe(Topic.ORDER_FILL)

    async def _reject(*a, **kw):
        raise RuntimeError(
            "Fyers place_order failed: {'code': -50, 'message': 'Request rejected: "
            "Order placement restricted. Algo orders are not allowed from this app "
            "2TTPL4SVOP-100', 's': 'error'}"
        )
    b._executor.execute_leg = _reject
    b._exit_executor.execute_leg = _reject

    async def _drain():
        while True:
            fills.append(await q.get())

    return b, fills, _drain


async def _run_and_collect_fills(bridge, ev, drain_coro):
    task = asyncio.create_task(drain_coro())
    await bridge._handle(ev)
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def test_live_entry_both_legs_rejected_by_broker_is_aborted_not_faked():
    async def run():
        bridge, fills, drain = _bridge_with_rejecting_executor(mode="live")
        await _run_and_collect_fills(bridge, _ev(action="ENTRY"), drain)

        assert len(fills) == 1
        f = fills[0]
        assert f.entry_aborted is True, "a total (symmetric) broker rejection must abort, not fake a fill"
        assert f.ce_fill == 0.0 and f.pe_fill == 0.0, "must not report the fallback LTP as a real fill price"

    asyncio.run(run())


def test_live_exit_both_legs_rejected_by_broker_is_aborted_not_faked():
    async def run():
        bridge, fills, drain = _bridge_with_rejecting_executor(mode="live")
        await _run_and_collect_fills(bridge, _ev(action="EXIT"), drain)

        assert len(fills) == 1
        f = fills[0]
        assert f.exit_aborted is True, "a broker-rejected EXIT must never be reported as a successful close"
        assert f.ce_fill == 0.0 and f.pe_fill == 0.0, "must not report the fallback LTP as a real close price"

    asyncio.run(run())


def test_paper_route_mode_still_books_local_sim_fill_despite_broker_rejection():
    """paper_route intentionally sends the real order for connectivity verification
    and expects the broker to reject it (no-fund account) -- that rejection must
    NOT be treated as entry_aborted/exit_aborted; the local sim fill still books."""
    async def run():
        bridge, fills, drain = _bridge_with_rejecting_executor(mode="paper_route")
        await _run_and_collect_fills(bridge, _ev(action="ENTRY"), drain)

        assert len(fills) == 1
        f = fills[0]
        assert f.entry_aborted is False
        assert f.paper_mode is True

    asyncio.run(run())
