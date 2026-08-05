"""
Regression tests for select_balanced_pair_at() -- the explicit-anchor variant
of select_balanced_pair() extracted 2026-08-05 to support BEGINNING entry's
near/far dual-anchor selection (see test_sell_straddle_beginning_near_far.py
for the decision-layer tests). This file covers the pure selection math only:
select_balanced_pair_at(strike_prem, atm, ...) must behave identically to
select_balanced_pair(strike_prem, spot, ...) when atm is exactly what
select_balanced_pair would itself compute by rounding, AND must correctly
anchor at an arbitrary explicit strike that is NOT the rounded-nearest one.
"""
from strategies.sell_straddle.selection import select_balanced_pair, select_balanced_pair_at


def _cache():
    return {
        (24500, "CE"): {"ltp": 184.25, "atp": 180.0},
        (24500, "PE"): {"ltp": 133.75, "atp": 130.0},
        (24550, "CE"): {"ltp": 156.85, "atp": 150.0},
        (24550, "PE"): {"ltp": 156.00, "atp": 150.0},
        (24600, "CE"): {"ltp": 133.05, "atp": 130.0},
        (24600, "PE"): {"ltp": 185.00, "atp": 180.0},
        (24450, "CE"): {"ltp": 213.65, "atp": 210.0},
        (24450, "PE"): {"ltp": 112.00, "atp": 110.0},
        (24650, "CE"): {"ltp": 100.00, "atp": 98.0},
        (24650, "PE"): {"ltp": 210.00, "atp": 205.0},
    }


def test_select_balanced_pair_at_matches_rounded_wrapper_at_same_atm():
    cache = _cache()
    spot = 24512.90  # rounds to 24500 with step=50
    wrapped = select_balanced_pair(cache, spot=spot, step=50, offset=4, ltp_target=50.0)
    direct = select_balanced_pair_at(cache, atm=24500, spot=spot, step=50, offset=4, ltp_target=50.0)
    assert wrapped == direct


def test_select_balanced_pair_at_anchors_at_explicit_non_rounded_strike():
    """spot=24512.90 rounds to 24500 -- select_balanced_pair() would never look
    at 24550 at all. select_balanced_pair_at() must be able to anchor there
    explicitly and find a genuinely different pair."""
    cache = _cache()
    spot = 24512.90
    at_far = select_balanced_pair_at(cache, atm=24550, spot=spot, step=50, offset=4, ltp_target=50.0)
    assert at_far is not None
    ce_strike, pe_strike, ce_ltp, pe_ltp = at_far
    # Anchor at 24550 must be one of the 24550 legs (lower time value side).
    assert ce_strike == 24550 or pe_strike == 24550


def test_select_balanced_pair_at_returns_none_when_no_quotes_at_atm():
    cache = _cache()
    assert select_balanced_pair_at(cache, atm=99999, spot=24512.90, step=50, offset=4, ltp_target=50.0) is None
