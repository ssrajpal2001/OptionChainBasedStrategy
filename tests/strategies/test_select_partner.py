"""Rollover partner selection — balance the new leg against the running (kept) leg."""

from strategies.straddle_selection import select_partner_for


def _cache(d):
    return {k: {"ltp": v, "atp": v} for k, v in d.items()}


def test_partner_closest_to_running_leg_premium():
    # Keep CE (running) at ltp 60. Roll PE — pick the PE strike whose premium is
    # CLOSEST to 60 (balanced), among valid candidates.
    cache = _cache({
        (100, "CE"): 60.0,
        (100, "PE"): 95.0,   # diff 35
        (105, "PE"): 62.0,   # diff 2   ← best (closest to 60)
        (110, "PE"): 40.0,   # diff 20
    })
    res = select_partner_for(cache, roll_side="PE", kept_strike=100, kept_ltp=60.0,
                             spot=100, step=5, offset=4, ltp_target=30.0,
                             rule_pass=lambda cs, ps: True)
    assert res == (105, 62.0)


def test_partner_respects_ltp_target_and_rules():
    cache = _cache({(100, "CE"): 60.0, (105, "PE"): 62.0, (110, "PE"): 20.0})
    # 110 PE (20) is below target 30 → excluded; rule blocks 105 → no candidate
    res = select_partner_for(cache, "PE", 100, 60.0, 100, 5, 4, 30.0,
                             rule_pass=lambda cs, ps: False)
    assert res is None


def test_partner_none_when_no_strikes():
    res = select_partner_for(_cache({(100, "CE"): 60.0}), "PE", 100, 60.0,
                             100, 5, 4, 30.0, rule_pass=lambda cs, ps: True)
    assert res is None
