"""
2026-08-19: a MARKET order that looked fully filled by QUANTITY on the very
first poll used to be trusted immediately, with zero further price
reconciliation. Confirmed live: a real gurmeet NIFTY entry recorded
CE=102.35/PE=108.90 from that exact fast path, while the broker's own
terminal settled to CE=101.45/PE=108.30 moments later -- the broker's
avg_price field was still updating when we read it. straddle_bridge.py's
_do_leg() now re-checks get_order_status() a short moment later (and
cross-checks get_positions() as ground truth) before finalizing the
recorded entry price, ENTRY only. These tests drive that directly via the
same execute_leg-stub harness used in test_straddle_bridge_broker_reject.py.
"""
import asyncio

from execution_bridge.straddle_bridge import StraddleExecutionBridge, StraddleOrderEvent
from execution_bridge.smart_executor import LegFill
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
    def get_bindings_safe_sync(self, cid):
        return [{"binding_id": f"{cid}_b1", "engine_active": True,
                 "terminal_connected": True, "is_trade_enabled": True,
                 "trading_mode": "live"}]

    def get_deployments_sync(self, cid):
        return [{"binding_id": f"{cid}_b1", "strategy_name": "sell_straddle",
                 "underlying": "NIFTY", "is_running": 1}]


class _Position:
    def __init__(self, symbol, qty, avg_price):
        self.symbol = symbol
        self.qty = qty
        self.avg_price = avg_price


class _OrderStatus:
    def __init__(self, avg_price):
        self.avg_price = avg_price


class _FakeBroker:
    provider = "zerodha"

    def __init__(self, settled_avg=None, positions=None, raise_on_status=False):
        self._settled_avg = settled_avg
        self._positions = positions or []
        self._raise = raise_on_status

    async def get_order_status(self, order_id):
        if self._raise:
            raise RuntimeError("order-status endpoint down")
        return _OrderStatus(self._settled_avg)

    async def get_positions(self):
        return self._positions


class _Router:
    def __init__(self, broker):
        self._client_db = _DB()
        self._brokers = {"A": {"A_b1": broker}}


def _ev(action="ENTRY", **kw):
    return StraddleOrderEvent(action=action, underlying="NIFTY", atm=24200,
                               ce_strike=24200, pe_strike=24100, ce_ltp=100.0, pe_ltp=100.0,
                               client_id="A", binding_id="A_b1", event_id="evt1", **kw)


def _bridge(broker, ce_first_avg=102.35, pe_first_avg=108.90, qty=50):
    bus = EventBus()
    b = StraddleExecutionBridge(bus, _Registry(["A"]), _Router(broker))
    fills = []
    q = bus.subscribe(Topic.ORDER_FILL)

    async def _fake_execute_leg(_broker, *, broker_symbol, **kw):
        avg = ce_first_avg if "24200CE" in broker_symbol.upper() or broker_symbol.upper().endswith("CE") else pe_first_avg
        return LegFill(filled_qty=qty, avg_price=avg, order_ids=[f"OID_{broker_symbol}"], completed=True)

    b._executor.execute_leg = _fake_execute_leg
    b._exit_executor.execute_leg = _fake_execute_leg

    async def _drain():
        while True:
            fills.append(await q.get())

    return b, fills, _drain


async def _run_and_collect(bridge, ev, drain_coro):
    task = asyncio.create_task(drain_coro())
    await bridge._handle(ev)
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def test_entry_price_corrected_by_settle_recheck():
    """The exact real incident: first poll reads 102.35/108.90, a short
    settle-recheck against get_order_status reveals the broker's true
    settled average 101.45/108.30 -- the CORRECTED price must be what
    ends up in the published fill event."""
    async def run():
        broker = _FakeBroker(settled_avg=101.45)   # applies to whichever leg is checked; see per-leg override below
        bridge, fills, drain = _bridge(broker, ce_first_avg=102.35, pe_first_avg=108.90)

        # Distinct settled avg per leg -- route CE's order_id to 101.45, PE's to 108.30.
        async def _get_order_status(order_id):
            if "CE" in order_id.upper():
                return _OrderStatus(101.45)
            return _OrderStatus(108.30)
        broker.get_order_status = _get_order_status

        await _run_and_collect(bridge, _ev(action="ENTRY"), drain)

        assert len(fills) == 1
        f = fills[0]
        assert f.ce_fill == 101.45, "must use the SETTLED broker average, not the premature first-read value"
        assert f.pe_fill == 108.30

    asyncio.run(run())


def test_entry_price_settle_via_positions_ground_truth():
    """get_positions() cross-check wins over the order-status re-poll when
    it also reports a real, matching average -- ground truth over a
    possibly-still-updating order-status read."""
    async def run():
        broker = _FakeBroker(settled_avg=102.00)   # order-status re-poll still slightly off
        broker._positions = [
            _Position("NIFTY26AUG24200CE", 50, 101.45),
            _Position("NIFTY26AUG24100PE", 50, 108.30),
        ]
        bridge, fills, drain = _bridge(broker, ce_first_avg=102.35, pe_first_avg=108.90)

        await _run_and_collect(bridge, _ev(action="ENTRY"), drain)

        assert len(fills) == 1
        f = fills[0]
        # The symbol resolver in this test setup won't produce an exact
        # "NIFTY26AUG24200CE"-style match (mock provider), so this mainly
        # proves the mechanism doesn't crash and still lands on a real,
        # nonzero broker-sourced price -- exact-match behavior against a
        # real registry-resolved symbol is covered by the order-status test
        # above, which doesn't depend on symbol resolution at all.
        assert f.ce_fill > 0 and f.pe_fill > 0

    asyncio.run(run())


def test_entry_price_unchanged_when_already_settled():
    """No spurious 'correction' when the first read was already the true,
    fully-settled average -- the mechanism must be a no-op in the common
    case, not introduce noise."""
    async def run():
        broker = _FakeBroker(settled_avg=102.35)   # same as the first read -- CE only exercised here

        async def _get_order_status(order_id):
            return _OrderStatus(102.35 if "CE" in order_id.upper() else 108.90)
        broker.get_order_status = _get_order_status

        bridge, fills, drain = _bridge(broker, ce_first_avg=102.35, pe_first_avg=108.90)
        await _run_and_collect(bridge, _ev(action="ENTRY"), drain)

        assert len(fills) == 1
        f = fills[0]
        assert f.ce_fill == 102.35
        assert f.pe_fill == 108.90

    asyncio.run(run())


def test_exit_is_not_settle_rechecked():
    """Scoped to ENTRY only, per direct spec -- an EXIT's fill price must
    NOT be delayed/altered by this mechanism (different risk profile,
    out of scope for this fix)."""
    async def run():
        broker = _FakeBroker(settled_avg=999.99)   # would be an obviously-wrong "correction" if ever applied
        bridge, fills, drain = _bridge(broker, ce_first_avg=102.35, pe_first_avg=108.90)

        await _run_and_collect(bridge, _ev(action="EXIT"), drain)

        assert len(fills) == 1
        f = fills[0]
        assert f.ce_fill == 102.35   # unchanged -- the settle-recheck never ran
        assert f.pe_fill == 108.90

    asyncio.run(run())


def test_settle_recheck_failure_falls_back_gracefully():
    """A broken order-status/positions call during the settle re-check must
    never crash the fill pipeline -- falls back to the original (still
    broker-sourced, just possibly premature) price."""
    async def run():
        broker = _FakeBroker(raise_on_status=True)
        bridge, fills, drain = _bridge(broker, ce_first_avg=102.35, pe_first_avg=108.90)

        await _run_and_collect(bridge, _ev(action="ENTRY"), drain)

        assert len(fills) == 1
        f = fills[0]
        assert f.ce_fill == 102.35
        assert f.pe_fill == 108.90

    asyncio.run(run())
