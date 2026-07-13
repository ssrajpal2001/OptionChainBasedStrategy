"""Rollover partner selection — balance the new leg against the running (kept) leg."""

from strategies.straddle_selection import select_partner_for


def _cache(d):
    return {k: {"ltp": v, "atp": v} for k, v in d.items()}


def test_partner_picks_closest_premium_ignoring_ltp_cap():
    # Keep CE (running) at ltp 60. Roll PE — ltp_le_kept is DISABLED by default for rollover,
    # so the new partner is simply the one CLOSEST to 60 (above or below).
    cache = _cache({
        (100, "CE"): 60.0,
        (95,  "PE"): 95.0,   # diff 35
        (105, "PE"): 62.0,   # diff 2   ← closest
        (110, "PE"): 58.0,   # diff 2   (encountered after 105)
        (115, "PE"): 40.0,   # diff 20
    })
    res = select_partner_for(cache, roll_side="PE", kept_strike=100, kept_ltp=60.0,
                             spot=100, step=5, offset=4, ltp_target=30.0,
                             rule_pass=lambda cs, ps: True)
    assert res == (105, 62.0)


def test_partner_can_enforce_ltp_le_kept():
    # With ltp_le_kept=True, the partner must NOT be richer than the kept leg.
    cache = _cache({
        (100, "CE"): 60.0,
        (95,  "PE"): 95.0,   # > 60 -> EXCLUDED
        (105, "PE"): 62.0,   # > 60 -> EXCLUDED
        (110, "PE"): 58.0,   # <= 60, diff 2   ← best eligible
        (115, "PE"): 40.0,   # <= 60, diff 20
    })
    res = select_partner_for(cache, roll_side="PE", kept_strike=100, kept_ltp=60.0,
                             spot=100, step=5, offset=4, ltp_target=30.0,
                             rule_pass=lambda cs, ps: True, ltp_le_kept=True)
    assert res == (110, 58.0)


def test_partner_respects_ltp_target_and_rules():
    cache = _cache({(100, "CE"): 60.0, (105, "PE"): 62.0, (110, "PE"): 20.0})
    # 110 PE (20) is below target 30 → excluded; rule blocks 105 → no candidate
    res = select_partner_for(cache, "PE", 100, 60.0, 100, 5, 4, 30.0,
                             rule_pass=lambda cs, ps: False)
    assert res is None


def test_partner_uses_balanced_ratio_metric():
    # kept CE @100. Two PE candidates:
    #   PE95 @30 -> diff 70, ratio 70/130 = 0.538
    #   PE105 @180 -> diff 80, ratio 80/280 = 0.286
    # With balanced_ratio the lower ratio wins even though it is farther in absolute terms.
    cache = _cache({
        (100, "CE"): 100.0,
        (95, "PE"): 30.0,
        (105, "PE"): 180.0,
    })
    res = select_partner_for(cache, roll_side="PE", kept_strike=100, kept_ltp=100.0,
                             spot=100, step=5, offset=4, ltp_target=10.0,
                             rule_pass=lambda cs, ps: True, metric="balanced_ratio")
    assert res == (105, 180.0)


def test_partner_none_when_no_strikes():
    res = select_partner_for(_cache({(100, "CE"): 60.0}), "PE", 100, 60.0,
                             100, 5, 4, 30.0, rule_pass=lambda cs, ps: True)
    assert res is None
