"""
Regression test: strategies/sell_straddle/exits.py's per-tick exit-check loop
used to check the ITM-roll 70%% protection stop (rolling.py's
_check_itm_roll_protection) LAST -- after ratio_exit/ltp_decay/TSL/exit_rules/
vwap_rise, each of which `return`s the moment its own condition fires. An
itm-pair-gate pair (two ITM legs with a wide strike gap, by definition) is
exactly the shape most likely to keep the CE/PE premium ratio persistently
elevated -- so a persistently-true ratio_exit condition could starve the 70%%
protection check forever, even though its running-loss budget had genuinely
been breached. Real 2026-08-17 incident: a protective stop should have fired
and didn't.

Fixed: the ITM pair gate + 70%% protection check now run FIRST in
_check_exits (right after the day-level %% guardrails), before ratio_exit/
ltp_decay/TSL/exit_rules/vwap_rise -- so a hard rupee-loss cap always gets
first look, regardless of what else is also true on the same tick.
"""
import asyncio
import datetime
from unittest.mock import patch

from data_layer.base_feeder import EventBus
from config.global_config import GlobalConfig
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg


def test_itm_roll_protection_stop_fires_even_when_ratio_exit_persistently_true():
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._lot_size = 75
        s._lot_multiplier = 1
        s._spot = 24000.0
        s._force_exit = datetime.time(23, 59)
        s._itm_pair_gate_enabled = False   # only the 70% protection stop is under test
        s._ltp_decay_enabled = False
        s._tsl_enabled = False
        s._vwap_rise_enabled = False
        s._exit_rules = []
        s._day_profit_target_pct = 0.0
        s._day_loss_sl_pct = 0.0
        # Deliberately near-1.0 so ratio_exit's own condition is persistently TRUE
        # against the CE/PE pair below -- this is exactly what starved the
        # protection check under the old (checked-last) ordering.
        s._ratio_threshold = 1.01

        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
            ce_leg=StraddleLeg("CE", 23800, 40.0, 90.0),   # big running loss -> should stop
            pe_leg=StraddleLeg("PE", 24200, 40.0, 45.0),
            net_credit=80.0, status="open",
        )
        s._itm_roll_protection = {
            "CE": {"protect_rs": 1000.0, "new_side": "CE", "new_strike": 23800,
                   "orig_strike": 23900, "kept_side": "PE", "kept_strike": 24200},
        }
        # CE: entry 40, ltp 90 -> pnl_pts=-50 -> running_loss_rs=50*75=3750 >= 1000 -> STOP

        close_leg_calls = []

        async def _fake_close_leg(side, reason, now):
            close_leg_calls.append((side, reason))
            class _Ev:
                close_aborted = False
                realized_pnl = 0.0
            return _Ev()
        s._close_leg = _fake_close_leg

        open_leg_calls = []

        async def _fake_open_leg(side, strike, ltp, now, reason):
            # Mirror the real _open_leg's effect closely enough for this test:
            # the freshly-opened leg's entry price is the fill price, so its
            # running P&L (and therefore the CE/PE ratio) resets to ~flat.
            open_leg_calls.append((side, strike, reason))
            leg = StraddleLeg(side, strike, ltp, ltp)
            if side == "CE":
                s._position.ce_leg = leg
            else:
                s._position.pe_leg = leg
        s._open_leg = _fake_open_leg

        roll_calls = []

        async def _fake_single_side_roll(now, reason):
            roll_calls.append(reason)
            return True
        s._single_side_roll = _fake_single_side_roll

        s._persist = lambda: None
        s._strike_prem = {(23900, "CE"): {"ltp": 45.0}}
        s._ind_by_tf = lambda *a, **k: {}

        with patch("strategies.sell_straddle.rolling.RuntimeConfig.index_section", return_value={}):
            await s._check_exits()

        assert ("CE", "itm_roll_protection_stop") in close_leg_calls, (
            "the 70% protection stop must fire even though ratio_exit's own "
            "condition was also persistently true this tick"
        )
        assert open_leg_calls == [("CE", 23900, "itm_roll_protection_restore")]
        assert not roll_calls, (
            "ratio_exit must not preempt the protection stop -- protection now "
            "runs first in priority order, and the freshly-restored leg is no "
            "longer ratio-extreme"
        )
    asyncio.run(run())
