"""
Regression tests for the 2026-08-05 user-specified BEGINNING entry redesign:
instead of rounding spot to one nearest strike, evaluate the two strikes that
actually bracket spot (near = floor(spot/step)*step, far = near+step) as two
independent anchor candidates, each via the existing anchor+partner balanced-
pair search. Entry criteria is checked on BOTH resulting pairs:
  - only one passes -> take it directly
  - both pass -> the max/min-premium ratio decides (lower ratio wins)
  - neither passes -> no trade this cycle (BEGINNING keeps retrying every
    eligible cycle regardless, unchanged from the existing hybrid-entry gate)

RE-ENTRY is explicitly unchanged (still single-ATM select_balanced_pair) --
not covered here, already covered by existing selection tests.
"""
import asyncio
from datetime import datetime
from unittest.mock import patch

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy

_RULES = [{
    "indicator": "advanced", "operand1": "slope", "operand2": "value",
    "operand2_val": 0.0, "operator_sym": "<", "tf": 1,
}]


def _strategy():
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
    s._spot = 24512.0  # near=24500, far=24550 at step=50
    s._entry_expiry_date = "2026-08-11"
    opens = []

    async def _fake_open_position(now, ce_strike, pe_strike, ce_ltp, pe_ltp, rule_key, reason, expiry_date=None):
        opens.append((ce_strike, pe_strike, ce_ltp, pe_ltp))
    s._open_position = _fake_open_position
    return s, opens


def _fake_select_at(near_pair, far_pair):
    """Return a stand-in for select_balanced_pair_at() keyed by which atm it's
    called with (24500 -> near_pair, 24550 -> far_pair), matching the real
    signature's positional args (strike_prem, atm, spot, step, ...)."""
    def _sel(strike_prem, atm, spot, step, offset, ltp_target, **kwargs):
        if atm == 24500:
            return near_pair
        if atm == 24550:
            return far_pair
        return None
    return _sel


def test_only_near_passes_takes_near_pair():
    s, opens = _strategy()
    near_pair = (24450, 24500, 120.0, 100.0)  # ratio 1.2
    far_pair = (24600, 24550, 90.0, 92.0)     # ratio ~1.022, but will fail rules

    def _ind_by_tf(ce, pe, rules):
        # near pair (ce=24450) passes; far pair (ce=24600) fails.
        return {1: {"slope": -0.5 if ce == 24450 else 0.5}}
    s._ind_by_tf = _ind_by_tf

    with patch("strategies.sell_straddle.selection.select_balanced_pair_at",
               side_effect=_fake_select_at(near_pair, far_pair)):
        asyncio.run(s._eval_beginning_near_far(
            datetime.now(IST), "entry_rules_beginning", _RULES,
            step=50, offset=5, ltp_target=50.0, theta_target=20.0,
            variable_strikes=False, balance_ratio=1.0,
        ))

    assert opens == [(24450, 24500, 120.0, 100.0)]


def test_only_far_passes_takes_far_pair():
    s, opens = _strategy()
    near_pair = (24450, 24500, 120.0, 100.0)
    far_pair = (24600, 24550, 90.0, 92.0)

    def _ind_by_tf(ce, pe, rules):
        # near pair fails; far pair passes.
        return {1: {"slope": 0.5 if ce == 24450 else -0.5}}
    s._ind_by_tf = _ind_by_tf

    with patch("strategies.sell_straddle.selection.select_balanced_pair_at",
               side_effect=_fake_select_at(near_pair, far_pair)):
        asyncio.run(s._eval_beginning_near_far(
            datetime.now(IST), "entry_rules_beginning", _RULES,
            step=50, offset=5, ltp_target=50.0, theta_target=20.0,
            variable_strikes=False, balance_ratio=1.0,
        ))

    assert opens == [(24600, 24550, 90.0, 92.0)]


def test_both_pass_lower_ratio_wins():
    s, opens = _strategy()
    near_pair = (24450, 24500, 120.0, 100.0)  # ratio 1.20
    far_pair = (24600, 24550, 90.0, 92.0)     # ratio 1.022 -- more balanced, must win

    def _ind_by_tf(ce, pe, rules):
        return {1: {"slope": -0.5}}  # both pass
    s._ind_by_tf = _ind_by_tf

    with patch("strategies.sell_straddle.selection.select_balanced_pair_at",
               side_effect=_fake_select_at(near_pair, far_pair)):
        asyncio.run(s._eval_beginning_near_far(
            datetime.now(IST), "entry_rules_beginning", _RULES,
            step=50, offset=5, ltp_target=50.0, theta_target=20.0,
            variable_strikes=False, balance_ratio=1.0,
        ))

    assert opens == [(24600, 24550, 90.0, 92.0)], "the far pair has the lower (more balanced) ratio and must win"


def test_both_pass_near_wins_when_it_is_more_balanced():
    """Swap which side is more balanced -- confirms the tiebreak genuinely
    compares ratios rather than having a hardcoded near/far preference."""
    s, opens = _strategy()
    near_pair = (24450, 24500, 91.0, 90.0)   # ratio ~1.011 -- more balanced this time
    far_pair = (24600, 24550, 130.0, 100.0)  # ratio 1.30

    def _ind_by_tf(ce, pe, rules):
        return {1: {"slope": -0.5}}  # both pass
    s._ind_by_tf = _ind_by_tf

    with patch("strategies.sell_straddle.selection.select_balanced_pair_at",
               side_effect=_fake_select_at(near_pair, far_pair)):
        asyncio.run(s._eval_beginning_near_far(
            datetime.now(IST), "entry_rules_beginning", _RULES,
            step=50, offset=5, ltp_target=50.0, theta_target=20.0,
            variable_strikes=False, balance_ratio=1.0,
        ))

    assert opens == [(24450, 24500, 91.0, 90.0)]


def test_neither_passes_no_trade():
    s, opens = _strategy()
    near_pair = (24450, 24500, 120.0, 100.0)
    far_pair = (24600, 24550, 90.0, 92.0)

    def _ind_by_tf(ce, pe, rules):
        return {1: {"slope": 0.5}}  # both fail (slope must be < 0)
    s._ind_by_tf = _ind_by_tf

    with patch("strategies.sell_straddle.selection.select_balanced_pair_at",
               side_effect=_fake_select_at(near_pair, far_pair)):
        asyncio.run(s._eval_beginning_near_far(
            datetime.now(IST), "entry_rules_beginning", _RULES,
            step=50, offset=5, ltp_target=50.0, theta_target=20.0,
            variable_strikes=False, balance_ratio=1.0,
        ))

    assert opens == []


def test_neither_strike_has_a_pair_no_crash():
    s, opens = _strategy()

    with patch("strategies.sell_straddle.selection.select_balanced_pair_at", return_value=None):
        asyncio.run(s._eval_beginning_near_far(
            datetime.now(IST), "entry_rules_beginning", _RULES,
            step=50, offset=5, ltp_target=50.0, theta_target=20.0,
            variable_strikes=False, balance_ratio=1.0,
        ))

    assert opens == []


def test_near_far_strikes_computed_correctly_for_spot_between_them():
    """spot=24512 with step=50 must anchor at near=24500 and far=24550 --
    verify select_balanced_pair_at is actually called with those two atms."""
    s, opens = _strategy()
    called_atms = []

    def _sel(strike_prem, atm, spot, step, offset, ltp_target, **kwargs):
        called_atms.append(atm)
        return None
    s._ind_by_tf = lambda ce, pe, rules: {1: {"slope": -0.5}}

    with patch("strategies.sell_straddle.selection.select_balanced_pair_at", side_effect=_sel):
        asyncio.run(s._eval_beginning_near_far(
            datetime.now(IST), "entry_rules_beginning", _RULES,
            step=50, offset=5, ltp_target=50.0, theta_target=20.0,
            variable_strikes=False, balance_ratio=1.0,
        ))

    assert called_atms == [24500, 24550]


# ── 2026-08-26 direct user confirmation: near/far AND intrinsic/time-value ──
# stripping both move to the mean-of-spot-and-futures reference for a
# futures_atm underlying -- "it should be from the mean which we calculated."

def test_near_far_and_spot_arg_both_use_atm_ref_when_set():
    """self._atm_ref (mean) must drive BOTH which strikes bracket as near/far
    AND the `spot` argument select_balanced_pair_at strips intrinsic value
    with -- real spot (self._spot) must not leak into either."""
    s, opens = _strategy()
    s._spot = 24277.0          # real spot -- must NOT be used for near/far or spot arg
    s._atm_ref = 24363.5       # mean -- near=24350, far=24400 at step=50
    calls = []

    def _sel(strike_prem, atm, spot, step, offset, ltp_target, **kwargs):
        calls.append((atm, spot))
        return None
    s._ind_by_tf = lambda ce, pe, rules: {1: {"slope": -0.5}}

    with patch("strategies.sell_straddle.selection.select_balanced_pair_at", side_effect=_sel):
        asyncio.run(s._eval_beginning_near_far(
            datetime.now(IST), "entry_rules_beginning", _RULES,
            step=50, offset=5, ltp_target=50.0, theta_target=20.0,
            variable_strikes=False, balance_ratio=1.0,
        ))

    assert calls == [(24350, 24363.5), (24400, 24363.5)]


def test_near_far_falls_back_to_spot_when_atm_ref_unset():
    """A non-futures_atm underlying (self._atm_ref stays 0.0) must behave
    exactly as before -- near/far and the spot arg both come from real spot."""
    s, opens = _strategy()
    assert s._atm_ref == 0.0
    calls = []

    def _sel(strike_prem, atm, spot, step, offset, ltp_target, **kwargs):
        calls.append((atm, spot))
        return None
    s._ind_by_tf = lambda ce, pe, rules: {1: {"slope": -0.5}}

    with patch("strategies.sell_straddle.selection.select_balanced_pair_at", side_effect=_sel):
        asyncio.run(s._eval_beginning_near_far(
            datetime.now(IST), "entry_rules_beginning", _RULES,
            step=50, offset=5, ltp_target=50.0, theta_target=20.0,
            variable_strikes=False, balance_ratio=1.0,
        ))

    assert calls == [(24500, 24512.0), (24550, 24512.0)]
