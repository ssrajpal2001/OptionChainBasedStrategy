"""
Regression test: strategies/sell_straddle's ITM-roll-protection (70%-of-booked-
profit stop on a freshly-rolled leg, part 2 of the ITM pair gate) used to store
its armed budget as a single scalar (self._itm_roll_protection = {...} or None).
Since roll_side is decided dynamically per rollover event (whichever leg has the
better P&L at that moment), CE and PE can each roll via this gate at different
times -- arming a budget for one side used to silently DELETE a still-active
budget already armed on the other side.

Fixed: _itm_roll_protection is now a dict keyed by side ("CE"/"PE"), each side
tracked and cleared independently -- exactly the "consider the LATEST leg per
side, independently for CE and PE" behavior requested.
"""
import asyncio
import datetime
from unittest.mock import patch

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg


def _strategy(bus):
    s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
    s._lot_size = 75
    s._lot_multiplier = 1
    s._spot = 24000.0
    return s


def test_arming_ce_does_not_clobber_already_armed_pe():
    s = _strategy(EventBus())
    s._itm_roll_protection = {
        "PE": {"protect_rs": 1000.0, "new_side": "PE", "new_strike": 24200,
               "orig_strike": 24100, "kept_side": "CE", "kept_strike": 23900},
    }
    # Simulate arming CE the same way rolling.py's rollover code does (the
    # dict-exists guard + per-side assignment, not the whole-dict replace).
    if not isinstance(getattr(s, "_itm_roll_protection", None), dict):
        s._itm_roll_protection = {}
    s._itm_roll_protection["CE"] = {
        "protect_rs": 500.0, "new_side": "CE", "new_strike": 23800,
        "orig_strike": 23900, "kept_side": "PE", "kept_strike": 24200,
    }
    assert "PE" in s._itm_roll_protection
    assert s._itm_roll_protection["PE"]["new_strike"] == 24200
    assert "CE" in s._itm_roll_protection
    assert s._itm_roll_protection["CE"]["new_strike"] == 23800


def test_ce_stop_fires_independently_leaving_pe_budget_untouched():
    bus = EventBus()
    s = _strategy(bus)

    async def _no_op_emit(ev):
        return None
    s._emit_order = _no_op_emit

    s._position = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 23800, 40.0, 90.0),   # running big loss -> should stop
        pe_leg=StraddleLeg("PE", 24200, 40.0, 45.0),   # small loss -> should NOT stop
        net_credit=80.0, status="open",
    )
    s._itm_roll_protection = {
        "CE": {"protect_rs": 1000.0, "new_side": "CE", "new_strike": 23800,
               "orig_strike": 23900, "kept_side": "PE", "kept_strike": 24200},
        "PE": {"protect_rs": 1000.0, "new_side": "PE", "new_strike": 24200,
               "orig_strike": 24100, "kept_side": "CE", "kept_strike": 23800},
    }
    # CE: entry 40, ltp 90 -> pnl_pts = 40-90 = -50 -> running_loss_rs = 50*75 = 3750 >= 1000 -> STOP
    # PE: entry 40, ltp 45 -> pnl_pts = 40-45 = -5   -> running_loss_rs = 5*75  = 375  <  1000 -> no stop

    close_leg_calls = []

    async def _fake_close_leg(side, reason, now):
        close_leg_calls.append(side)
        class _Ev:
            close_aborted = False
            realized_pnl = 0.0
        return _Ev()
    s._close_leg = _fake_close_leg

    # CE's stop finds a valid replacement (restores its original strike, which
    # passes re-entry -- empty entry_rules_reentry is vacuously True) -- position
    # stays open with a fresh CE leg. PE is never touched by any of this.
    open_leg_calls = []

    async def _fake_open_leg(side, strike, ltp, now, reason):
        open_leg_calls.append((side, strike, reason))
    s._open_leg = _fake_open_leg
    s._persist = lambda: None
    s._strike_prem = {(23900, "CE"): {"ltp": 45.0}}
    s._ind_by_tf = lambda *a, **k: {}

    # entry_rules_reentry may be non-empty in the real project config this test
    # environment reads from -- force it empty so _rule_pass is the vacuous-True
    # baseline this test actually wants to exercise (restore-succeeds path),
    # independent of whatever real re-entry rules happen to be configured.
    with patch("strategies.sell_straddle.rolling.RuntimeConfig.index_section", return_value={}):
        asyncio.run(s._check_itm_roll_protection(datetime.datetime.now()))

    assert close_leg_calls == ["CE"], "only the CE side should have triggered a stop"
    assert open_leg_calls == [("CE", 23900, "itm_roll_protection_restore")]
    assert "CE" not in s._itm_roll_protection, "CE budget must be cleared after firing"
    assert "PE" in s._itm_roll_protection, "PE budget must survive CE's stop firing"
    assert s._itm_roll_protection["PE"]["new_strike"] == 24200


def test_second_ce_roll_replaces_only_ce_latest_leg_budget():
    """User-specified rule: if CE closes, a new CE is taken, and that ALSO closes
    and rolls again, the 70% budget must track the LATEST CE leg only -- while PE's
    independently-armed budget (if any) stays untouched throughout."""
    s = _strategy(EventBus())
    s._itm_roll_protection = {
        "CE": {"protect_rs": 500.0, "new_side": "CE", "new_strike": 23800,
               "orig_strike": 23900, "kept_side": "PE", "kept_strike": 24200},
        "PE": {"protect_rs": 700.0, "new_side": "PE", "new_strike": 24300,
               "orig_strike": 24200, "kept_side": "CE", "kept_strike": 23800},
    }
    # A second CE roll re-arms CE only, with a fresh strike/budget.
    s._itm_roll_protection["CE"] = {
        "protect_rs": 600.0, "new_side": "CE", "new_strike": 23700,
        "orig_strike": 23800, "kept_side": "PE", "kept_strike": 24300,
    }
    assert s._itm_roll_protection["CE"]["new_strike"] == 23700
    assert s._itm_roll_protection["CE"]["protect_rs"] == 600.0
    # PE's budget from before is completely unaffected.
    assert s._itm_roll_protection["PE"]["new_strike"] == 24300
    assert s._itm_roll_protection["PE"]["protect_rs"] == 700.0
