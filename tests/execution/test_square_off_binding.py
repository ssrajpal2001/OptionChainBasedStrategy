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
        from execution_bridge.straddle_bridge import StraddleExecutionBridge, StraddleFillEvent

        bus = EventBus()
        # Capture the EXIT order events square-off publishes (it now routes through the strategy's
        # own _close_position → real exit pipeline, NOT raw place_order, so the close hits the
        # exchange AND records history). _close_position now WAITS for the bridge to confirm the
        # fill before finalizing (2026-08-04 fail-loud fix), so this test plays the bridge's part:
        # watch ORDER_REQUEST and immediately hand back a confirmed fill via _on_fill, like a
        # live broker that always fills.
        order_q = bus.subscribe(Topic.ORDER_REQUEST)
        evs: list = []

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
        _books = {("cli", "Z1"): ss, ("other", "Z9"): other}

        async def _confirm_exits():
            while True:
                ev = await order_q.get()
                evs.append(ev)
                if getattr(ev, "action", "") == "EXIT":
                    fill = StraddleFillEvent(
                        action="EXIT", underlying=ev.underlying, atm=ev.atm,
                        ce_strike=ev.ce_strike, pe_strike=ev.pe_strike,
                        ce_fill=ev.ce_ltp, pe_fill=ev.pe_ltp,
                        client_id=ev.client_id, binding_id=ev.binding_id,
                        event_id=ev.event_id, legs=ev.legs,
                    )
                    target = _books.get((ev.client_id, ev.binding_id))
                    if target is not None:
                        target._on_fill(fill)

        task = asyncio.create_task(_confirm_exits())
        try:
            n = await br.square_off_binding("cli", "Z1", [ss, other])
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        assert n == 2                       # CE + PE of THIS binding only
        # square-off routed the close via the exit pipeline → exactly ONE EXIT order event, stamped
        # with THIS binding's identity, for the bridge consumer to buy-to-close + log to history.
        exits = [e for e in evs if getattr(e, "action", "") == "EXIT"]
        assert len(exits) == 1
        assert exits[0].client_id == "cli" and exits[0].binding_id == "Z1"
        assert ss._position is None                 # this book's position closed/cleared
        assert ss._stop_for_day is True             # re-entry blocked during teardown
        assert other._position.status == "open"     # other client's book untouched
        # unknown binding -> no-op
        assert await br.square_off_binding("cli", "NOPE", [ss]) == 0

    asyncio.run(run())


def test_square_off_skips_hedged_positional_carry(monkeypatch):
    """2026-08-27, real incident: a manual Trade/Terminal-OFF square-off
    tried to buy-to-close the sold legs of a position that had already
    converted to a deliberate EOD hedge-and-carry (is_hedged_positional=
    True) -- exactly the position the hedge exists to protect from being
    flattened by routine controls. square_off_binding must skip it
    entirely, not attempt _close_position at all."""
    async def run():
        from execution_bridge.straddle_bridge import StraddleExecutionBridge

        bus = EventBus()
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
            hedge_ce_leg=StraddleLeg("CE", 23700, 40.0, 40.0),
            hedge_pe_leg=StraddleLeg("PE", 23300, 45.0, 45.0),
            is_hedged_positional=True,
        )

        n = await br.square_off_binding("cli", "Z1", [ss])

        assert n == 0
        assert ss._position is not None and ss._position.status == "open"
        assert ss._position.is_hedged_positional is True
        # No EXIT order should have been dispatched at all for this hedged carry.
        assert order_q.empty()

    asyncio.run(run())
