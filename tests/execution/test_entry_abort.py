"""Live ENTRY atomicity: an asymmetric fill (one leg fills, the other gets 0 — the Delta cancel-race)
must FLATTEN the filled leg and ABORT (publish entry_aborted), never leave a naked leg."""
import asyncio

from config.global_config import GlobalConfig, Topic
from data_layer.base_feeder import EventBus
from execution_bridge.straddle_bridge import StraddleExecutionBridge, StraddleOrderEvent
from execution_bridge.smart_executor import LegFill
from execution_bridge.base_broker import OrderFill, OrderSide, OrderStatus, PositionRecord


class _Binding:
    provider = "delta"
    trading_mode = "live"
    binding_id = "Delta1"


class _Broker:
    def __init__(self, residual=None):
        self._binding = _Binding()
        self.placed = []          # flatten orders land here
        self.cancelled = []
        self._residual = residual or []   # PositionRecords get_positions returns (late-fill residual)

    async def get_quote(self, symbol):
        return 60.0, 70.0

    async def place_order(self, req):
        self.placed.append((req.broker_symbol, str(req.side), req.qty, str(req.order_type)))
        return "FLAT1"

    async def cancel_order(self, oid):
        self.cancelled.append(str(oid))
        return True

    async def get_positions(self):
        return self._residual

    async def get_order_status(self, oid):
        return OrderFill(order_id=oid, broker_symbol="X", side=OrderSide.BUY, qty=6,
                         avg_price=70.0, status=OrderStatus.COMPLETE)


class _Router:
    def __init__(self, broker):
        self._brokers = {"cli": {"Delta1": broker}}
        self._client_db = None


def test_asymmetric_entry_flattens_and_aborts(monkeypatch):
    async def run():
        bus = EventBus()
        broker = _Broker()
        br = StraddleExecutionBridge(bus, registry=None, router=_Router(broker))

        # Force a truthy Delta symbol + DELTA exchange regardless of registry/config state.
        import execution_bridge.straddle_bridge as sb
        monkeypatch.setattr(sb, "_resolve_option_symbol", lambda *a, **k: f"SYM-{a[3]}")
        monkeypatch.setattr(sb, "order_exchange", lambda *_a, **_k: "DELTA")
        # Don't actually wait the reconcile settle window in the test.
        async def _fast_sleep(*_a, **_k):
            return None
        monkeypatch.setattr(sb.asyncio, "sleep", _fast_sleep)

        # CE fills full (6); PE fills 0 — the asymmetric case.
        async def _exec_leg(broker_, *, broker_symbol, side, qty, **kw):
            if "CE" in broker_symbol:
                return LegFill(filled_qty=6, avg_price=65.5, order_ids=["o-ce"], completed=True)
            return LegFill(filled_qty=0, avg_price=0.0, order_ids=["o-pe"], completed=False)
        br._executor.execute_leg = _exec_leg
        br._exit_executor.execute_leg = _exec_leg

        fills = bus.subscribe(Topic.ORDER_FILL)
        ev = StraddleOrderEvent(action="ENTRY", underlying="BTC", atm=63600,
                                ce_strike=64000, pe_strike=63200, ce_ltp=45.0, pe_ltp=37.0,
                                lot_size=6, lot_multiplier=1, client_id="cli", binding_id="Delta1")
        await br._live_fill(ev, "cli", "Delta1", broker, paper=False)

        # 1) The filled CE leg was flattened (opposite BUY-to-close for 6).
        assert any(o[1].endswith("BUY") and o[2] == 6 for o in broker.placed), broker.placed
        # 2) An entry_aborted fill was published so the strategy discards its optimistic position.
        seen = []
        while not fills.empty():
            seen.append(fills.get_nowait())
        assert any(getattr(f, "entry_aborted", False) for f in seen)

    asyncio.run(run())


def test_abort_reconciles_late_residual_position(monkeypatch):
    """The recurrence: the unfilled PE leg's order fills LATE → a real short shows on the broker
    while the app thinks it's flat. _reconcile_flat must read REAL positions and market-flatten it."""
    async def run():
        bus = EventBus()
        # get_positions reports a residual SHORT 6 of the PE leg symbol (filled after the abort).
        residual = [PositionRecord(symbol="SYM-PE", qty=-6, avg_price=75.0, pnl=0.0, product="MARGIN")]
        broker = _Broker(residual=residual)
        br = StraddleExecutionBridge(bus, registry=None, router=_Router(broker))

        import execution_bridge.straddle_bridge as sb
        monkeypatch.setattr(sb, "_resolve_option_symbol", lambda *a, **k: f"SYM-{a[3]}")
        monkeypatch.setattr(sb, "order_exchange", lambda *_a, **_k: "DELTA")
        async def _fast_sleep(*_a, **_k):
            return None
        monkeypatch.setattr(sb.asyncio, "sleep", _fast_sleep)

        async def _exec_leg(broker_, *, broker_symbol, side, qty, **kw):
            if "CE" in broker_symbol:
                return LegFill(filled_qty=6, avg_price=65.5, order_ids=["o-ce"], completed=True)
            return LegFill(filled_qty=0, avg_price=0.0, order_ids=["o-pe"], completed=False)
        br._executor.execute_leg = _exec_leg
        br._exit_executor.execute_leg = _exec_leg

        ev = StraddleOrderEvent(action="ENTRY", underlying="BTC", atm=63600,
                                ce_strike=63600, pe_strike=63400, ce_ltp=97.0, pe_ltp=75.0,
                                lot_size=6, lot_multiplier=1, client_id="cli", binding_id="Delta1")
        await br._live_fill(ev, "cli", "Delta1", broker, paper=False)

        # The residual short PE was discovered and flattened with an opposite BUY-6 MARKET order.
        recon = [o for o in broker.placed if o[0] == "SYM-PE" and o[1].endswith("BUY") and o[2] == 6]
        assert recon, broker.placed
        assert broker.cancelled, "should have cancelled the unfilled leg's resting orders"

    asyncio.run(run())
