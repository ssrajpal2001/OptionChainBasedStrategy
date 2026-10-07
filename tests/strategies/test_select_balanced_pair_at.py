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
from strategies.sell_straddle.selection import select_balanced_pair, select_balanced_pair_at, reentry_block_reason


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


def test_partner_window_centers_on_real_spot_atm_not_shifted_anchor():
    """2026-10-07 CORRECTION, direct user spec: the PARTNER search window
    must center on real SPOT ATM, not the anchor's own (possibly OTM-
    shifted, and -- with forced_anchor_side -- possibly monthly-atm-based)
    strike. Superseded the 2026-08-24 "center on shifted anchor" spec after
    a real live incident (2026-10-07): with the anchor shifted 2 strikes
    away from real spot, a configured pool_otm_depth=4/itm_depth=4 ended up
    skewed (2 OTM / 6 ITM measured from spot) instead of symmetric.

    atm=24500 and the shifted CE anchor=24550 are both DELIBERATELY
    different from spot=24600, so this only succeeds if the window
    genuinely centers on spot (24600 +/- step*1 = 24550/24600/24650) --
    not atm (24450-24550) and not the shifted anchor (24500-24600), both of
    which would miss PE24650 entirely."""
    cache = {
        (24500, "CE"): {"ltp": 150.0, "atp": 148.0},
        (24500, "PE"): {"ltp": 200.0, "atp": 198.0},
        (24550, "CE"): {"ltp": 120.0, "atp": 118.0},   # shifted anchor strike (atm+step)
        (24650, "PE"): {"ltp": 80.0, "atp": 78.0},     # only reachable centered on real spot
    }
    result = select_balanced_pair_at(
        cache, atm=24500, spot=24600.0, step=50, offset=1, ltp_target=50.0,
        anchor_otm_steps=1,
    )
    assert result is not None
    ce_strike, pe_strike, ce_ltp, pe_ltp = result
    assert ce_strike == 24550   # the shifted anchor (CE has lower TV at raw ATM)
    assert pe_strike == 24650   # only reachable once the partner window centers on spot


def test_partner_window_unchanged_for_reentry_anchor_otm_steps_zero():
    """RE-ENTRY always calls with anchor_otm_steps=0, where anchor_strike ==
    atm -- the fix above must be a complete no-op for that path. Same cache
    as the previous test, but WITHOUT the OTM shift: PE24600 must NOT be
    reachable (outside atm=24500 +/- offset=1), matching the original,
    unshifted behaviour exactly."""
    cache = {
        (24500, "CE"): {"ltp": 150.0, "atp": 148.0},
        (24500, "PE"): {"ltp": 200.0, "atp": 198.0},
        (24550, "CE"): {"ltp": 120.0, "atp": 118.0},
        (24600, "PE"): {"ltp": 80.0, "atp": 78.0},
    }
    result = select_balanced_pair_at(
        cache, atm=24500, spot=24500.0, step=50, offset=1, ltp_target=50.0,
        anchor_otm_steps=0,
    )
    assert result is None  # no partner reachable within the un-shifted window


# ── atm_ref (2026-08-26, direct user spec: mean-of-spot-and-futures ATM) ────

def test_select_balanced_pair_atm_ref_overrides_spot_for_strike_rounding():
    """atm_ref, when given, decides which strike ATM rounds to -- spot itself
    stays the value used for intrinsic/time-value stripping. spot=24512.90
    would round to 24500; atm_ref=24560 rounds to 24550 instead -- this only
    succeeds if atm_ref genuinely won the rounding decision (matches the
    explicit-anchor result already proven in
    test_select_balanced_pair_at_anchors_at_explicit_non_rounded_strike)."""
    cache = _cache()
    at_explicit = select_balanced_pair_at(cache, atm=24550, spot=24512.90, step=50, offset=4, ltp_target=50.0)
    via_atm_ref = select_balanced_pair(
        cache, spot=24512.90, step=50, offset=4, ltp_target=50.0, atm_ref=24560.0,
    )
    assert via_atm_ref is not None
    assert via_atm_ref == at_explicit


def test_select_balanced_pair_atm_ref_none_preserves_spot_only_behavior():
    """Default (no atm_ref) must be byte-identical to the pre-2026-08-26
    behavior -- every existing caller that omits it is unaffected."""
    cache = _cache()
    spot = 24512.90
    without = select_balanced_pair(cache, spot=spot, step=50, offset=4, ltp_target=50.0)
    with_none = select_balanced_pair(cache, spot=spot, step=50, offset=4, ltp_target=50.0, atm_ref=None)
    assert without is not None
    assert without == with_none


def test_reentry_block_reason_atm_ref_matches_select_balanced_pair():
    """reentry_block_reason must diagnose against the SAME atm_ref the real
    selection call used, never disagree with it."""
    cache = _cache()
    diag = reentry_block_reason(
        cache, spot=24512.90, step=50, offset=4, ltp_target=50.0,
        rule_eval=lambda cs, ps: (True, "ok"), atm_ref=24560.0,
    )
    at_explicit = select_balanced_pair_at(cache, atm=24550, spot=24512.90, step=50, offset=4, ltp_target=50.0)
    assert diag["kind"] == "passed"
    assert (diag["ce"], diag["pe"]) == (at_explicit[0], at_explicit[1])


def _cache_all_partners_under_ceiling():
    """2026-10-06: separate from _cache() specifically so every CE partner
    candidate clears the ltp<anchor_ltp ceiling (restored per direct user
    spec) and reaches the rule-pass check -- _cache() has several CE
    candidates priced ABOVE the PE-24500 anchor (184.25, 213.65 > 133.75),
    which is exactly the real scenario the ceiling now correctly rejects
    before rule_pass ever runs, and would make these rule-reason-visibility
    tests assert on the wrong trace line."""
    # CE24500 is the lower-time-value side at ATM -> becomes the anchor
    # (ltp=125). The partner side's OWN atm-strike candidate (PE24500) is
    # structurally guaranteed to fail the ceiling no matter what it's set
    # to -- the anchor is BY DEFINITION the lower of the ATM pair, so the
    # other side at that same strike can never be lower than it. Every
    # OTHER-strike PE candidate is priced well under 125, so those clear
    # the ceiling and reach the rule-pass check; the tests below filter
    # out the one degenerate same-strike line rather than fight that
    # structural fact with cache numbers.
    return {
        (24500, "CE"): {"ltp": 125.0, "atp": 120.0},
        (24500, "PE"): {"ltp": 140.0, "atp": 135.0},
        (24450, "PE"): {"ltp": 90.0, "atp": 88.0},
        (24550, "PE"): {"ltp": 95.0, "atp": 92.0},
        (24600, "PE"): {"ltp": 100.0, "atp": 98.0},
    }


def test_trace_shows_rule_reason_not_bare_word(monkeypatch):
    """2026-09-30, direct user request: the trace used to collapse every
    rule-rejected candidate to the bare word "rule", giving zero visibility
    into which indicator (e.g. SLOPE) was actually checked or its value --
    indistinguishable from a genuinely-failing condition vs an indicator
    that simply wasn't computable yet (both read as "rule"/N/A). rule_pass
    may now return (bool, reason) and that reason must show up verbatim in
    the trace line."""
    cache = _cache_all_partners_under_ceiling()
    trace: list = []

    def _rule_pass(cs, ps):
        return False, "SLOPE(-1.37)<VALUE(0.00)=✗"

    result = select_balanced_pair_at(
        cache, 24500, 24512.0, 50, 2, 50.0, trace=trace, rule_pass=_rule_pass,
    )
    assert result is None
    # The PE24500 line is the degenerate same-strike-as-anchor candidate --
    # structurally always ceiling-rejected (see the cache helper's own
    # docstring), not part of what this test is verifying.
    cand_lines = [ln for ln in trace if ln.strip().startswith("cand") and "PE24500" not in ln]
    assert cand_lines, "expected at least one candidate trace line"
    for ln in cand_lines:
        assert "SLOPE(-1.37)<VALUE(0.00)=✗" in ln
        assert ln.rstrip().endswith("]")


def test_trace_still_shows_bare_rule_word_when_rule_pass_returns_plain_bool():
    """Backward compat: an older-style rule_pass returning a plain bool
    (no reason) must still work, falling back to the original bare "rule"
    tag rather than crashing or showing an empty bracket."""
    cache = _cache_all_partners_under_ceiling()
    trace: list = []

    result = select_balanced_pair_at(
        cache, 24500, 24512.0, 50, 2, 50.0, trace=trace, rule_pass=lambda cs, ps: False,
    )
    assert result is None
    cand_lines = [ln for ln in trace if ln.strip().startswith("cand") and "PE24500" not in ln]
    assert cand_lines
    for ln in cand_lines:
        assert ln.rstrip().endswith("rule")
        assert "[" not in ln
