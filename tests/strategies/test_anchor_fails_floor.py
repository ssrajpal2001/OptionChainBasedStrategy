"""
tests/strategies/test_anchor_fails_floor.py -- regression for
strategies/sell_straddle/selection.py's anchor_fails_floor(), the pure
diagnostic added 2026-08-23 (direct user spec: "if ltp is less than
threshold then jump to next week expiry -- applicable for anchor
selection part"). Deliberately standalone/additive -- does not touch or
refactor select_balanced_pair_at's own already-proven, live-money
selection logic, just replicates its anchor-selection + dual-floor check
(the SAME threshold, no new one) as a small, separately-testable function.
"""
from strategies.sell_straddle.selection import anchor_fails_floor


def _cache():
    return {
        (24500, "CE"): {"ltp": 184.25, "atp": 180.0},
        (24500, "PE"): {"ltp": 133.75, "atp": 130.0},
        (24550, "CE"): {"ltp": 156.85, "atp": 150.0},
        (24550, "PE"): {"ltp": 156.00, "atp": 150.0},
    }


def test_anchor_passes_floor_when_ltp_target_is_low():
    # spot near 24500 -> anchor = PE (lower time value), ltp=133.75
    cache = _cache()
    assert anchor_fails_floor(cache, atm=24500, spot=24512.90, ltp_target=50.0) is False


def test_anchor_fails_floor_when_ltp_target_is_high():
    cache = _cache()
    assert anchor_fails_floor(cache, atm=24500, spot=24512.90, ltp_target=200.0) is True


def test_anchor_fails_floor_when_theta_target_not_met():
    cache = _cache()
    # PE anchor tv = 133.75 (no intrinsic near ATM) -- theta_target above that fails it.
    assert anchor_fails_floor(cache, atm=24500, spot=24512.90, ltp_target=0.0, theta_target=200.0) is True


def test_anchor_fails_when_atm_not_quoted_at_all():
    cache = _cache()
    assert anchor_fails_floor(cache, atm=25000, spot=25000, ltp_target=50.0) is True


def test_anchor_fails_when_one_leg_missing():
    cache = {(24500, "CE"): {"ltp": 184.25, "atp": 180.0}}   # PE missing
    assert anchor_fails_floor(cache, atm=24500, spot=24500, ltp_target=50.0) is True


def test_anchor_fails_when_a_quoted_leg_has_zero_ltp():
    cache = {
        (24500, "CE"): {"ltp": 184.25, "atp": 180.0},
        (24500, "PE"): {"ltp": 0.0, "atp": 0.0},
    }
    assert anchor_fails_floor(cache, atm=24500, spot=24500, ltp_target=50.0) is True


def test_anchor_otm_shift_matches_select_balanced_pair_at_rejection():
    """When anchor_otm_steps shifts the anchor to an unquoted strike,
    anchor_fails_floor must agree with select_balanced_pair_at's own
    REJECT-for-missing-shifted-quote behavior (same underlying rule,
    verified consistent rather than assumed)."""
    from strategies.sell_straddle.selection import select_balanced_pair_at
    cache = _cache()   # no 24450/24600 CE quotes -- the shifted anchor strike
    sel = select_balanced_pair_at(cache, atm=24500, spot=24512.90, step=50, offset=4,
                                   ltp_target=50.0, anchor_otm_steps=1)
    assert sel is None   # confirms the shifted anchor strike genuinely has no quote
    assert anchor_fails_floor(cache, atm=24500, spot=24512.90, ltp_target=50.0,
                               anchor_otm_steps=1, step=50) is True


def test_anchor_side_selection_matches_select_balanced_pair_at():
    """anchor_fails_floor must pick the SAME anchor side select_balanced_pair_at
    would -- verified by constructing a case where CE is the anchor (lower
    time value) instead of PE, and confirming a CE-specific floor failure
    is detected."""
    # Deep ITM CE (large intrinsic stripped away) -> low CE time value -> CE anchor.
    cache = {
        (24500, "CE"): {"ltp": 20.0, "atp": 20.0},   # spot far above strike -> tiny/negative tv
        (24500, "PE"): {"ltp": 500.0, "atp": 500.0},
    }
    spot = 25000.0   # CE intrinsic = 500, tv = 20-500 = -480 (very low) -> CE is anchor
    assert anchor_fails_floor(cache, atm=24500, spot=spot, ltp_target=50.0) is True   # CE ltp=20 < 50
    assert anchor_fails_floor(cache, atm=24500, spot=spot, ltp_target=10.0) is False  # CE ltp=20 >= 10
