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
        await s._single_side_roll(datetime.datetime.now(IST), "ltp_decay")
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
        # Only the SAME strike (23500 CE) is eligible; adjacent CE strikes are below ltp_target.
        s._strike_prem[(23500, "CE")] = {"ltp": 70.0, "atp": 80.0}
        s._strike_prem[(23500, "PE")] = {"ltp": 60.0, "atp": 65.0}
        for k in (23450, 23550):
            s._strike_prem[(k, "CE")] = {"ltp": 10.0, "atp": 65.0}   # below ltp_target → skipped
            s._strike_prem[(k, "PE")] = {"ltp": 72.0, "atp": 78.0}
        for k in (23450, 23500, 23550):
            s._prev_atp_closed[(k, "CE")] = s._strike_prem[(k, "CE")]["atp"] + 5.0
            s._prev_atp_closed[(k, "PE")] = s._strike_prem[(k, "PE")]["atp"] + 5.0
        await s._single_side_roll(datetime.datetime.now(IST), "ltp_decay")
        while not q.empty():
            seen.append(q.get_nowait())
        orders = [e for e in seen if isinstance(e, StraddleOrderEvent)]
        assert orders == []                                 # NO orders fired
        assert int(s._position.ce_leg.strike) == 23500      # position unchanged
    asyncio.run(run())
    asyncio.run(run())


def _neutralize_other_exits(s):
    """Turn off every _check_exits branch EXCEPT vwap_rise so a test can isolate it."""
    s._force_exit = datetime.time(23, 59)          # not EOD
    s._guardrail_pnl_enabled = False
    s._day_profit_target_pct = 0.0
    s._day_loss_sl_pct = 0.0
    s._initial_net_credit = 0.0                    # skips the day-% block
    s._ltp_decay_enabled = False
    s._ratio_threshold = 99.0                      # ratio 1.0 < 99 → no ratio exit
    s._tsl_enabled = False
    s._guardrail_roc_enabled = False
    s._roc_guardrail_enabled = False
    s._exit_rules = []                             # no Dynamic / EXIT-EVAL dump
    s._vwap_rise_enabled = True
    s._vwap_rise_threshold = 1.0
    s._vwap_stale_sec = 90.0


def _arm_vwap_rise(s, strike=23500):
    """Open position + pool so the vwap_rise block, IF it runs, lowers session_min_vwap to 90."""
    s._spot = strike
    s._position = StraddlePosition(
        underlying="NIFTY", atm_at_entry=strike, entry_spot=strike,
        ce_leg=StraddleLeg("CE", strike, 80.0, 50.0),
        pe_leg=StraddleLeg("PE", strike, 80.0, 50.0),
        net_credit=160.0, status="open",
    )
    s._position.session_min_vwap = 100.0
    # combined vwap = 45+45 = 90 (>= 0.60*close=60 → passes sanity bound); close = 50+50 = 100
    s._pool_engine.update_tick(strike, "CE", 50.0, 45.0)
    s._pool_engine.update_tick(strike, "PE", 50.0, 45.0)


def test_vwap_rise_runs_when_legs_fresh():
    async def run():
        s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
        _neutralize_other_exits(s)
        _arm_vwap_rise(s)
        await s._check_exits()
        # fresh → block executed → session_min_vwap pulled down to the live 90
        assert s._position.session_min_vwap == 90.0
    asyncio.run(run())


def test_vwap_rise_skipped_when_leg_stale():
    import time
    async def run():
        s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
        seen = []
        q = s._bus.subscribe(Topic.ORDER_REQUEST)
        _neutralize_other_exits(s)
        _arm_vwap_rise(s)
        # Freeze the PE leg's ATP age beyond the window → pair is stale.
        s._pool_engine._last_atp_ts[(23500, "PE")] = time.time() - 200
        await s._check_exits()
        while not q.empty():
            seen.append(q.get_nowait())
        # stale → vwap_rise block skipped entirely: baseline untouched, no orders.
        assert s._position.session_min_vwap == 100.0
        assert [e for e in seen if isinstance(e, StraddleOrderEvent)] == []
    asyncio.run(run())
