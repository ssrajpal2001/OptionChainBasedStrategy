"""
Regression tests for the 2026-09-24 R1-breach re-entry search additions to
strategies/sell_straddle/selection.py (direct user spec, several rounds of
clarification with strategies/sell_straddle/r1_breach_reentry.py):

- `_evaluate_roll_candidate`'s new `max_ltp_exclusive` premium-gate mode:
  candidate LTP must be STRICTLY LESS than a given value (the R1-breached
  leg's own LTP at the moment it closed) -- the inverse of the pre-existing
  `min_ltp_exclusive` mode (main rollover: candidate must be STRICTLY
  GREATER than the closing leg's LTP).
- `select_partner_for(anchor_strike=..., max_ltp_exclusive=...)` threading
  that mode through the both-sides (ITM+OTM) ring search.
"""
from strategies.sell_straddle.selection import (
    _evaluate_roll_candidate, select_partner_for,
)


def _sp(d):
    return {k: {"ltp": v} for k, v in d.items()}


def _always_pass(ce_s, pe_s):
    return True, ""


def test_evaluate_roll_candidate_max_ltp_exclusive_rejects_at_or_above():
    sp = _sp({(23100, "CE"): 195.80})
    diag = _evaluate_roll_candidate(
        sp, "CE", 23100, kept_strike=23200, kept_ltp=99.3, spot=23150, step=50,
        ltp_target=0.0, theta_target=0.0, max_itm_steps=None, ltp_le_kept=False,
        rule_pass=_always_pass, metric="closest_to_kept", max_ltp_exclusive=195.80,
    )
    assert diag["reject_reason"] is not None
    assert diag["reject_reason"].startswith("ltp_not_below_closing")


def test_evaluate_roll_candidate_max_ltp_exclusive_accepts_strictly_below():
    sp = _sp({(23100, "CE"): 195.80})
    diag = _evaluate_roll_candidate(
        sp, "CE", 23100, kept_strike=23200, kept_ltp=99.3, spot=23150, step=50,
        ltp_target=0.0, theta_target=0.0, max_itm_steps=None, ltp_le_kept=False,
        rule_pass=_always_pass, metric="closest_to_kept", max_ltp_exclusive=195.81,
    )
    assert diag["reject_reason"] is None


def test_evaluate_roll_candidate_min_and_max_are_mutually_exclusive_min_wins():
    """If a caller somehow passes both, min_ltp_exclusive (the main rollover's
    existing mode) is checked first and wins -- documents the existing
    if/elif precedence rather than asserting new behavior."""
    sp = _sp({(23100, "CE"): 50.0})
    diag = _evaluate_roll_candidate(
        sp, "CE", 23100, kept_strike=23200, kept_ltp=99.3, spot=23150, step=50,
        ltp_target=0.0, theta_target=0.0, max_itm_steps=None, ltp_le_kept=False,
        rule_pass=_always_pass, metric="closest_to_kept",
        min_ltp_exclusive=60.0, max_ltp_exclusive=195.80,
    )
    assert diag["reject_reason"].startswith("ltp_not_above_closing")


def test_ring_search_checks_both_itm_and_otm_sides_of_anchor():
    """anchor=23100 (the closed CE strike), step=50 -- ring 1 candidates are
    23050 (toward spot / ITM-er) and 23150 (away from spot / OTM-er). Only
    the OTM side (23150) passes the inverted premium gate here (< closing
    150.0) -- confirms the ring search actually considers BOTH, not just one
    fixed direction."""
    sp = _sp({(23050, "CE"): 220.0, (23150, "CE"): 140.0})
    result = select_partner_for(
        sp, roll_side="CE", kept_strike=23300, kept_ltp=90.0, spot=23100,
        step=50, offset=3, ltp_target=0.0, rule_pass=_always_pass,
        anchor_strike=23100, max_ltp_exclusive=150.0,
    )
    assert result == (23150, 140.0)


def test_ring_search_picks_itm_side_when_only_that_side_passes():
    sp = _sp({(23050, "CE"): 120.0, (23150, "CE"): 999.0})
    result = select_partner_for(
        sp, roll_side="CE", kept_strike=23300, kept_ltp=90.0, spot=23100,
        step=50, offset=3, ltp_target=0.0, rule_pass=_always_pass,
        anchor_strike=23100, max_ltp_exclusive=150.0,
    )
    assert result == (23050, 120.0)


def test_ring_search_widens_to_next_ring_when_ring1_has_no_passing_side():
    """Ring 1 (23050/23150) both fail the premium gate; ring 2 (23000/23200)
    has a passer at 23200."""
    sp = _sp({(23050, "CE"): 999.0, (23150, "CE"): 999.0,
              (23000, "CE"): 999.0, (23200, "CE"): 145.0})
    result = select_partner_for(
        sp, roll_side="CE", kept_strike=23300, kept_ltp=90.0, spot=23100,
        step=50, offset=3, ltp_target=0.0, rule_pass=_always_pass,
        anchor_strike=23100, max_ltp_exclusive=150.0,
    )
    assert result == (23200, 145.0)


def test_ring_search_returns_none_when_every_ring_exhausted():
    sp = _sp({(23050, "CE"): 999.0, (23150, "CE"): 999.0})
    result = select_partner_for(
        sp, roll_side="CE", kept_strike=23300, kept_ltp=90.0, spot=23100,
        step=50, offset=1, ltp_target=0.0, rule_pass=_always_pass,
        anchor_strike=23100, max_ltp_exclusive=150.0,
    )
    assert result is None
