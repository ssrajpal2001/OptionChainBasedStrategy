"""
Regression tests for BEGINNING entry's ATM anchor selection.

2026-09-01, direct user spec: replaced the prior near/far dual-bracket
approach (floor-round + floor-round+step, tried as two independent
candidates, tie-broken by premium-ratio balance -- see git history for the
original 2026-08-05 design) with a SINGLE nearest-round ATM anchor --
atm = round(spot/step)*step, the same rounding convention already used
everywhere else in this codebase (raw-anchor fallback, RE-ENTRY, scan_pool).
The existing 1-OTM shift + anchor/partner balanced-pair search still runs,
just against one anchor instead of two -- there is no more near/far bracket
or ratio tie-break.

RE-ENTRY is unchanged (still single-ATM select_balanced_pair) -- not covered
here, already covered by existing selection tests.
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


def _strategy(spot=24530.0):
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
    # 24530/50 = 490.6 -> round=491 -> atm=24550. Deliberately NOT the same
    # as the old floor-based near (24500) -- proves nearest-round, not floor.
    s._spot = spot
    s._entry_expiry_date = "2026-08-11"
    opens = []

    async def _fake_open_position(now, ce_strike, pe_strike, ce_ltp, pe_ltp, rule_key, reason, expiry_date=None):
        opens.append((ce_strike, pe_strike, ce_ltp, pe_ltp))
    s._open_position = _fake_open_position
    return s, opens


def test_atm_is_nearest_round_not_floor():
    """spot=24530, step=50 -> nearest-round anchor is 24550 (not the old
    floor-based 24500) -- verifies select_balanced_pair_at is actually
    called with the nearest-round strike."""
    s, opens = _strategy(spot=24530.0)
    called = []

    def _sel(strike_prem, atm, spot, step, offset, ltp_target, **kwargs):
        called.append((atm, spot))
        return None
    s._ind_by_tf = lambda ce, pe, rules: {1: {"slope": -0.5}}

    with patch("strategies.sell_straddle.selection.select_balanced_pair_at", side_effect=_sel):
        asyncio.run(s._eval_beginning_near_far(
            datetime.now(IST), "entry_rules_beginning", _RULES,
            step=50, offset=5, ltp_target=50.0, theta_target=20.0,
            variable_strikes=False, balance_ratio=1.0,
        ))

    assert called == [(24550, 24530.0)], "nearest-round(24530/50)=24550, not floor's 24500"


def test_atm_floor_and_nearest_round_agree_below_midpoint():
    """spot=24512, step=50 -> both floor and nearest-round give 24500 (below
    the strike midpoint) -- confirms the new logic isn't just always
    rounding up, it's genuine nearest-round."""
    s, opens = _strategy(spot=24512.0)
    called = []

    def _sel(strike_prem, atm, spot, step, offset, ltp_target, **kwargs):
        called.append((atm, spot))
        return None
    s._ind_by_tf = lambda ce, pe, rules: {1: {"slope": -0.5}}

    with patch("strategies.sell_straddle.selection.select_balanced_pair_at", side_effect=_sel):
        asyncio.run(s._eval_beginning_near_far(
            datetime.now(IST), "entry_rules_beginning", _RULES,
            step=50, offset=5, ltp_target=50.0, theta_target=20.0,
            variable_strikes=False, balance_ratio=1.0,
        ))

    assert called == [(24500, 24512.0)]


def test_pair_passes_rules_opens_position():
    s, opens = _strategy(spot=24530.0)
    pair = (24600, 24550, 90.0, 92.0)

    with patch("strategies.sell_straddle.selection.select_balanced_pair_at", return_value=pair):
        s._ind_by_tf = lambda ce, pe, rules: {1: {"slope": -0.5}}   # passes (slope<0)
        asyncio.run(s._eval_beginning_near_far(
            datetime.now(IST), "entry_rules_beginning", _RULES,
            step=50, offset=5, ltp_target=50.0, theta_target=20.0,
            variable_strikes=False, balance_ratio=1.0,
        ))

    assert opens == [(24600, 24550, 90.0, 92.0)]


def test_pair_fails_rules_no_trade():
    s, opens = _strategy(spot=24530.0)
    pair = (24600, 24550, 90.0, 92.0)

    with patch("strategies.sell_straddle.selection.select_balanced_pair_at", return_value=pair):
        s._ind_by_tf = lambda ce, pe, rules: {1: {"slope": 0.5}}   # fails (slope must be < 0)
        asyncio.run(s._eval_beginning_near_far(
            datetime.now(IST), "entry_rules_beginning", _RULES,
            step=50, offset=5, ltp_target=50.0, theta_target=20.0,
            variable_strikes=False, balance_ratio=1.0,
        ))

    assert opens == []


def test_no_pair_no_crash():
    s, opens = _strategy(spot=24530.0)

    with patch("strategies.sell_straddle.selection.select_balanced_pair_at", return_value=None):
        asyncio.run(s._eval_beginning_near_far(
            datetime.now(IST), "entry_rules_beginning", _RULES,
            step=50, offset=5, ltp_target=50.0, theta_target=20.0,
            variable_strikes=False, balance_ratio=1.0,
        ))

    assert opens == []


def test_select_balanced_pair_at_called_exactly_once():
    """The old near/far mechanism called select_balanced_pair_at up to twice
    per cycle; the single-anchor replacement must call it exactly once."""
    s, opens = _strategy(spot=24530.0)
    calls = []

    def _sel(strike_prem, atm, spot, step, offset, ltp_target, **kwargs):
        calls.append(atm)
        return None
    s._ind_by_tf = lambda ce, pe, rules: {1: {"slope": -0.5}}

    with patch("strategies.sell_straddle.selection.select_balanced_pair_at", side_effect=_sel):
        asyncio.run(s._eval_beginning_near_far(
            datetime.now(IST), "entry_rules_beginning", _RULES,
            step=50, offset=5, ltp_target=50.0, theta_target=20.0,
            variable_strikes=False, balance_ratio=1.0,
        ))

    assert len(calls) == 1


# ── 2026-08-26 direct user confirmation, still applies to the single anchor ──
# now: the mean-of-spot-and-futures reference drives BOTH which strike is the
# anchor AND the `spot` argument select_balanced_pair_at strips intrinsic
# value with -- real spot (self._spot) must not leak into either.

def test_atm_and_spot_arg_both_use_atm_ref_when_set():
    s, opens = _strategy(spot=24277.0)   # real spot -- must NOT be used
    s._atm_ref = 24363.5                 # mean -- nearest-round -> 24350
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

    assert calls == [(24350, 24363.5)]


def test_atm_falls_back_to_spot_when_atm_ref_unset():
    """A non-futures_atm underlying (self._atm_ref stays 0.0) must behave
    exactly as before -- the anchor and the spot arg both come from real spot."""
    s, opens = _strategy(spot=24530.0)
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

    assert calls == [(24550, 24530.0)]
