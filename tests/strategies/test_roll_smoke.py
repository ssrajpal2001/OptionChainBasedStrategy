import asyncio
import datetime
from data_layer.base_feeder import EventBus, OptionTick
from config.global_config import IST, Topic, GlobalConfig
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg
from execution_bridge.straddle_bridge import StraddleOrderEvent


def test_single_side_roll_emits_close_and_open_when_candidate_exists():
    async def run():
        bus = EventBus()
        seen = []
        q = bus.subscribe(Topic.ORDER_REQUEST)
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._spot = 23500
        # Decayed CE leg is FAR OTM (strike 23650) — a realistic ltp_decay roll moves it inward to
        # a DIFFERENT strike. (If the best partner were the same strike, the roll is correctly a no-op.)
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=23500, entry_spot=23500,
            ce_leg=StraddleLeg("CE", 23650, 80.0, 10.0),
            pe_leg=StraddleLeg("PE", 23500, 80.0, 70.0),
            net_credit=160.0, status="open",
        )
        # populate cache so scan_pool can find a CE candidate >= target on both sides.
        # ATM CE=75 > PE=60 → ce_bias_stronger=True (ce_corr 75 > pe_corr 60).
        # Non-ATM CE=60 < PE=72 satisfies bias filter (ce_ltp < pe_ltp).
        # entry_rules_reentry requires CLOSE < VWAP (ltp < atp) and SLOPE < 0
        # (current_atp < prev_atp), so atp > ltp and prev_atp > atp for each leg.
        s._strike_prem[(23500, "CE")] = {"ltp": 75.0, "atp": 80.0}
        s._strike_prem[(23500, "PE")] = {"ltp": 60.0, "atp": 65.0}
        for k in (23450, 23550):
            s._strike_prem[(k, "CE")] = {"ltp": 60.0, "atp": 65.0}
            s._strike_prem[(k, "PE")] = {"ltp": 72.0, "atp": 78.0}
        for k in (23450, 23500, 23550):
            s._prev_atp_closed[(k, "CE")] = s._strike_prem[(k, "CE")]["atp"] + 5.0
            s._prev_atp_closed[(k, "PE")] = s._strike_prem[(k, "PE")]["atp"] + 5.0
        await s._single_side_roll("CE", datetime.datetime.now(IST), "ltp_decay_CE")
        while not q.empty():
            seen.append(q.get_nowait())
        actions = [(e.action, tuple(e.legs)) for e in seen if isinstance(e, StraddleOrderEvent)]
        assert ("EXIT", ("CE",)) in actions
        assert ("ENTRY", ("CE",)) in actions
        assert s._position is not None   # survived (re-entered), not full-closed
        assert int(s._position.ce_leg.strike) != 23650   # rolled to a DIFFERENT strike (no same-strike wash)


def test_single_side_roll_skips_when_best_partner_is_same_strike():
    """A roll whose best balanced partner IS the leg's current strike must fire NO orders
    (no buy-to-close + re-sell wash on the identical strike — the order-book bug)."""
    async def run():
        bus = EventBus()
        seen = []
        q = bus.subscribe(Topic.ORDER_REQUEST)
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._spot = 23500
        # CE leg already AT the strike select_partner_for will pick (ATM 23500) → no-op roll.
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=23500, entry_spot=23500,
            ce_leg=StraddleLeg("CE", 23500, 80.0, 10.0),
            pe_leg=StraddleLeg("PE", 23500, 80.0, 70.0),
            net_credit=160.0, status="open",
        )
        s._strike_prem[(23500, "CE")] = {"ltp": 70.0, "atp": 80.0}   # ==kept PE(70): eligible (<=70) & closest → picked (same strike → skip)
        s._strike_prem[(23500, "PE")] = {"ltp": 60.0, "atp": 65.0}
        for k in (23450, 23550):
            s._strike_prem[(k, "CE")] = {"ltp": 60.0, "atp": 65.0}
            s._strike_prem[(k, "PE")] = {"ltp": 72.0, "atp": 78.0}
        for k in (23450, 23500, 23550):
            s._prev_atp_closed[(k, "CE")] = s._strike_prem[(k, "CE")]["atp"] + 5.0
            s._prev_atp_closed[(k, "PE")] = s._strike_prem[(k, "PE")]["atp"] + 5.0
        await s._single_side_roll("CE", datetime.datetime.now(IST), "ltp_decay_CE")
        while not q.empty():
            seen.append(q.get_nowait())
        orders = [e for e in seen if isinstance(e, StraddleOrderEvent)]
        assert orders == []                                 # NO orders fired
        assert int(s._position.ce_leg.strike) == 23500      # position unchanged
    asyncio.run(run())
    asyncio.run(run())
