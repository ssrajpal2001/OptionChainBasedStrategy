import asyncio, datetime
from config.global_config import IST, GlobalConfig
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg
from data_layer.base_feeder import EventBus


class _FakeBroker:
    def __init__(self):
        self.orders = []
        self._binding = type("B", (), {"provider": "zerodha"})()

    async def place_order(self, req):
        self.orders.append(req)
        return type("F", (), {"avg_price": 1.0})()


class _FakeRouter:
    def __init__(self, broker):
        self._brokers = {"cli": {"Z1": broker}}


def test_square_off_only_that_binding(monkeypatch):
    async def run():
        import execution_bridge.straddle_bridge as sb
        from execution_bridge.straddle_bridge import StraddleExecutionBridge

        # Ensure symbol lookup returns a truthy symbol regardless of registry state.
        monkeypatch.setattr(
            sb._REG, "get_broker_symbol",
            lambda *a, **k: "DUMMYSYM",
        )

        bus = EventBus()
        broker = _FakeBroker()
        br = StraddleExecutionBridge(bus, registry=None, router=_FakeRouter(broker))
        ss = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        ss._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=23500, entry_spot=23500,
            ce_leg=StraddleLeg("CE", 23550, 100.0, 90.0),
            pe_leg=StraddleLeg("PE", 23450, 100.0, 95.0),
            net_credit=200.0, status="open", lot_size=75,
        )
        n = await br.square_off_binding("cli", "Z1", [ss])
        assert n == 2                       # CE + PE closed
        assert len(broker.orders) == 2
        assert all(str(o.side).endswith("BUY") for o in broker.orders)   # buy-to-close
        # unknown binding -> no-op
        assert await br.square_off_binding("cli", "NOPE", [ss]) == 0

    asyncio.run(run())
