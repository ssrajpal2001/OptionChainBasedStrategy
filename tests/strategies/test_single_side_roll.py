import asyncio
import datetime
from unittest.mock import patch

from data_layer.base_feeder import EventBus
from config.global_config import IST, GlobalConfig
from execution_bridge.straddle_bridge import StraddleFillEvent
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg


def test_single_side_roll_no_candidate_closes_position():
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=23500, entry_spot=23500,
            ce_leg=StraddleLeg("CE", 23500, 80.0, 10.0),
            pe_leg=StraddleLeg("PE", 23500, 80.0, 70.0),
            net_credit=160.0, status="open",
        )
        s._spot = 23500
        s._strike_prem = {}   # empty → no rollover partner found
        await s._single_side_roll(datetime.datetime.now(IST), "ltp_decay")
        assert s._position is None   # no partner → full exit
    asyncio.run(run())


def test_single_side_roll_waits_for_close_fill_before_open():
    """A single-side roll must buy-to-close the good leg and only sell-to-open the
    new partner AFTER the close fill is confirmed. The untouched leg must keep its
    original entry price and strike."""
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._spot = 24400.0
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=24500, entry_spot=24500,
            ce_leg=StraddleLeg("CE", 24450, 152.75, 107.0,
                               open_time=datetime.datetime.now(IST)),
            pe_leg=StraddleLeg("PE", 24450, 132.05, 162.95,
                               open_time=datetime.datetime.now(IST)),
            net_credit=284.8, status="open",
        )

        emitted = []
        orig_emit = s._emit_order

        async def capture_emit(ev):
            emitted.append(ev)
            await orig_emit(ev)

        s._emit_order = capture_emit

        with patch("strategies.sell_straddle.selection.select_partner_for",
                   return_value=(24350, 156.90)):
            async def deliver_fills():
                await asyncio.sleep(0.02)
                close_ev = [o for o in emitted if o.action == "EXIT"][0]
                # Confirm ONLY the CE leg was closed.
                s._on_fill(StraddleFillEvent(
                    action="EXIT", underlying="NIFTY", atm=24500.0,
                    ce_strike=24450.0, pe_strike=24450.0,
                    ce_fill=107.0, pe_fill=0.0,
                    client_id="C", binding_id="B",
                    event_id=close_ev.event_id, legs=["CE"],
                ))
                await asyncio.sleep(0.02)
                open_ev = [o for o in emitted if o.action == "ENTRY"][0]
                # Confirm ONLY the CE leg was opened.
                s._on_fill(StraddleFillEvent(
                    action="ENTRY", underlying="NIFTY", atm=24500.0,
                    ce_strike=24350.0, pe_strike=24450.0,
                    ce_fill=156.90, pe_fill=0.0,
                    client_id="C", binding_id="B",
                    event_id=open_ev.event_id, legs=["CE"],
                ))

            task = asyncio.create_task(deliver_fills())
            await s._single_side_roll(datetime.datetime.now(IST), "scalable_tsl")
            await task

        assert len(emitted) == 2
        assert emitted[0].action == "EXIT"
        assert emitted[0].legs == ["CE"]
        assert emitted[1].action == "ENTRY"
        assert emitted[1].legs == ["CE"]

        pos = s._position
        assert pos is not None
        assert pos.status == "open"
        # Rolled leg updated.
        assert pos.ce_leg.strike == 24350
        assert pos.ce_leg.entry_price == 156.90
        # Kept leg untouched.
        assert pos.pe_leg.strike == 24450
        assert pos.pe_leg.entry_price == 132.05
        assert s._roll_in_progress is False

    asyncio.run(run())
