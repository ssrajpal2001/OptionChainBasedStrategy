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
from strategies.sell_straddle.selection import anchor_fails_floor, anchor_floor_detail


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


# ── anchor_floor_detail (2026-08-26, direct user request) ──────────────────
# The EXPIRY-SHIFT log used to only show the floor thresholds it needed to
# clear, never the actual observed anchor value -- so a real "premium
# genuinely too cheap" shift couldn't be told apart, after the fact, from
# "no live quote had arrived yet for the current week's contract at all".
# This is the pure diagnostic twin of anchor_fails_floor that fixes that.

def test_anchor_floor_detail_reports_measured_value_when_quoted():
    cache = _cache()   # spot near 24500 -> anchor = PE, ltp=133.75
    detail = anchor_floor_detail(cache, atm=24500, spot=24512.90, theta_target=0.0)
    assert detail["reason"] == "measured"
    assert detail["anchor_side"] == "PE"
    assert detail["anchor_strike"] == 24500
    assert detail["anchor_ltp"] == 133.75


def test_anchor_floor_detail_agrees_with_anchor_fails_floor_on_which_leg_is_anchor():
    """Same CE-anchor construction as test_anchor_side_selection_matches_
    select_balanced_pair_at above -- the detail function must pick the
    identical anchor side/strike anchor_fails_floor itself used."""
    cache = {
        (24500, "CE"): {"ltp": 20.0, "atp": 20.0},
        (24500, "PE"): {"ltp": 500.0, "atp": 500.0},
    }
    spot = 25000.0
    detail = anchor_floor_detail(cache, atm=24500, spot=spot, theta_target=0.0)
    assert detail["reason"] == "measured"
    assert detail["anchor_side"] == "CE"
    assert detail["anchor_ltp"] == 20.0


def test_anchor_floor_detail_reports_no_quote_when_atm_not_quoted_at_all():
    cache = _cache()
    detail = anchor_floor_detail(cache, atm=25000, spot=25000, theta_target=0.0)
    assert detail["reason"] == "no_quote_at_atm"


def test_anchor_floor_detail_reports_zero_ltp_distinctly_from_missing_quote():
    """The exact real-world case that prompted this fix: both legs are
    QUOTED (present in the pool) but still reading 0.00 because no live
    tick has landed yet for that specific contract -- must be distinguishable
    from a genuine measured-but-low premium, not silently conflated."""
    cache = {
        (24500, "CE"): {"ltp": 0.0, "atp": 0.0},
        (24500, "PE"): {"ltp": 0.0, "atp": 0.0},
    }
    detail = anchor_floor_detail(cache, atm=24500, spot=24500, theta_target=0.0)
    assert detail["reason"] == "zero_ltp_at_atm"
    assert detail["ce_ltp"] == 0.0
    assert detail["pe_ltp"] == 0.0


def test_anchor_floor_detail_reports_no_quote_at_shifted_anchor():
    cache = _cache()   # no 24450/24600 CE quotes -- the shifted anchor strike
    detail = anchor_floor_detail(cache, atm=24500, spot=24512.90, theta_target=0.0,
                                  anchor_otm_steps=1, step=50)
    assert detail["reason"] == "no_quote_at_shifted_anchor"


def test_anchor_floor_detail_measured_value_matches_what_anchor_fails_floor_rejected_on():
    """End-to-end consistency: when anchor_fails_floor(...) is True because of a
    genuinely LOW measured premium (not a missing quote), anchor_floor_detail's
    own "measured" ltp must be the exact same number that failed the check."""
    cache = _cache()   # PE anchor ltp=133.75
    ltp_target = 200.0
    assert anchor_fails_floor(cache, atm=24500, spot=24512.90, ltp_target=ltp_target) is True
    detail = anchor_floor_detail(cache, atm=24500, spot=24512.90, theta_target=0.0)
    assert detail["reason"] == "measured"
    assert detail["anchor_ltp"] == 133.75 < ltp_target
