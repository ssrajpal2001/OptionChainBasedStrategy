"""StraddleExecutionBridge must never fake a fill when the broker instance can't be resolved
in a non-paper mode -- this is the exact mechanism behind the 2026-08-04 incident where a
gurmeet SellStraddle EXIT was silently "confirmed" without ever reaching Zerodha.

Drives the real _handle() entry point (not an isolated harness) so both the ENTRY and EXIT
abort paths, and the `routed` counting fix (an aborted resolution must not count as routed),
are exercised end-to-end.
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
                 "terminal_connected": True, "trading_mode": self._mode}]

    def get_deployments_sync(self, cid):
        return [{"binding_id": f"{cid}_b1", "strategy_name": "sell_straddle",
                 "underlying": "NIFTY", "is_running": 1}]


class _Router:
    """_brokers is always empty -- the broker never resolves, in any mode."""
    def __init__(self, mode="live"):
        self._client_db = _DB(mode)
        self._brokers = {}


def _ev(action="ENTRY", **kw):
    return StraddleOrderEvent(action=action, underlying="NIFTY", atm=23000,
                               ce_strike=23000, pe_strike=23000, ce_ltp=100, pe_ltp=100,
                               client_id="A", binding_id="A_b1", event_id="evt1", **kw)


def _bridge(mode="live"):
    bus = EventBus()
    b = StraddleExecutionBridge(bus, _Registry(["A"]), _Router(mode))
    fills = []
    q = bus.subscribe(Topic.ORDER_FILL)

    async def _drain():
        while True:
            fills.append(await q.get())

    return b, fills, _drain


async def _run_and_collect_fills(bridge, ev, drain_coro):
    task = asyncio.create_task(drain_coro())
    await bridge._handle(ev)
    await asyncio.sleep(0.05)  # let the publish land in the queue
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def test_live_entry_never_fakes_fill_when_broker_unresolvable():
    async def run():
        bridge, fills, drain = _bridge(mode="live")
        # Patch _paper_fill / _live_fill so we can assert neither is ever called.
        calls = {"paper": 0, "live": 0}

        async def _fake_paper(*a, **kw):
            calls["paper"] += 1
        async def _fake_live(*a, **kw):
            calls["live"] += 1
        bridge._paper_fill = _fake_paper
        bridge._live_fill = _fake_live

        await _run_and_collect_fills(bridge, _ev(action="ENTRY"), drain)

        assert calls["paper"] == 0
        assert calls["live"] == 0
        assert len(fills) == 1
        assert fills[0].entry_aborted is True
        assert fills[0].routing_failed is True
        assert fills[0].exit_aborted is False

    asyncio.run(run())


def test_live_exit_never_fakes_fill_when_broker_unresolvable():
    """The exact incident mechanism: a live EXIT whose broker can't be resolved must publish
    exit_aborted=True, never a fabricated successful fill via _paper_fill."""
    async def run():
        bridge, fills, drain = _bridge(mode="live")
        calls = {"paper": 0, "live": 0}

        async def _fake_paper(*a, **kw):
            calls["paper"] += 1
        async def _fake_live(*a, **kw):
            calls["live"] += 1
        bridge._paper_fill = _fake_paper
        bridge._live_fill = _fake_live

        await _run_and_collect_fills(bridge, _ev(action="EXIT"), drain)

        assert calls["paper"] == 0, "must never fabricate a fill via _paper_fill for a live EXIT"
        assert calls["live"] == 0
        assert len(fills) == 1
        assert fills[0].exit_aborted is True
        assert fills[0].routing_failed is True
        assert fills[0].entry_aborted is False

    asyncio.run(run())


def test_paper_mode_untouched_no_resolver_no_alert():
    """mode == 'paper' must still go straight to _paper_fill without ever touching the
    broker resolver (pure local simulation, no SYSTEM_EVENT alert)."""
    async def run():
        bridge, fills, drain = _bridge(mode="paper")
        calls = {"paper": 0}

        async def _fake_paper(ev, cid, bid, broker):
            calls["paper"] += 1
        bridge._paper_fill = _fake_paper

        await _run_and_collect_fills(bridge, _ev(action="ENTRY"), drain)

        assert calls["paper"] == 1
        # No alert-worthy fill was published by the resolver path (paper is a local sim; the
        # only fills on the bus, if any, come from the strategy's own paper-fill logic, which
        # is stubbed out here).
        assert not any(getattr(f, "routing_failed", False) for f in fills)

    asyncio.run(run())
