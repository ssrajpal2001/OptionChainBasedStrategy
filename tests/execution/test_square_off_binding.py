import asyncio, datetime
from config.global_config import IST, GlobalConfig, Topic
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg
from data_layer.base_feeder import EventBus


class _FakeBroker:
    def __init__(self):
        self.orders = []
        self._binding = type("B", (), {"provider": "zerodha", "trading_mode": "live"})()

    async def place_order(self, req):
        self.orders.append(req)
        return type("F", (), {"avg_price": 1.0})()


class _FakeRouter:
    def __init__(self, broker):
        self._brokers = {"cli": {"Z1": broker}}


def test_square_off_only_that_binding(monkeypatch):
    async def run():
        from execution_bridge.straddle_bridge import StraddleExecutionBridge

        bus = EventBus()
        # Capture the EXIT order events square-off publishes (it now routes through the strategy's
        # own _close_position → real exit pipeline, NOT raw place_order, so the close hits the
        # exchange AND records history).
        order_q = bus.subscribe(Topic.ORDER_REQUEST)

        broker = _FakeBroker()
        br = StraddleExecutionBridge(bus, registry=None, router=_FakeRouter(broker))
        ss = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY",
                                  client_id="cli", binding_id="Z1")
        ss._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=23500, entry_spot=23500,
            ce_leg=StraddleLeg("CE", 23550, 100.0, 90.0),
            pe_leg=StraddleLeg("PE", 23450, 100.0, 95.0),
            net_credit=200.0, status="open", lot_size=75,
        )
        # A DIFFERENT client's book with an open position must NOT be flattened (cross-client safety).
        other = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY",
                                     client_id="other", binding_id="Z9")
        other._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=23500, entry_spot=23500,
            ce_leg=StraddleLeg("CE", 23550, 100.0, 90.0),
            pe_leg=StraddleLeg("PE", 23450, 100.0, 95.0),
            net_credit=200.0, status="open", lot_size=75,
        )
        n = await br.square_off_binding("cli", "Z1", [ss, other])
        assert n == 2                       # CE + PE of THIS binding only
        # square-off routed the close via the exit pipeline → exactly ONE EXIT order event, stamped
        # with THIS binding's identity, for the bridge consumer to buy-to-close + log to history.
        evs = []
        while not order_q.empty():
            evs.append(order_q.get_nowait())
        exits = [e for e in evs if getattr(e, "action", "") == "EXIT"]
        assert len(exits) == 1
        assert exits[0].client_id == "cli" and exits[0].binding_id == "Z1"
        assert ss._position is None                 # this book's position closed/cleared
        assert ss._stop_for_day is True             # re-entry blocked during teardown
        assert other._position.status == "open"     # other client's book untouched
        # unknown binding -> no-op
        assert await br.square_off_binding("cli", "NOPE", [ss]) == 0

    asyncio.run(run())
