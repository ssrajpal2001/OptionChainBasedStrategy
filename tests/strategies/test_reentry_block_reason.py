"""Re-entry diagnostic: distinguish 'no balanced pair' from 'pairs blocked by rule'."""

from strategies.straddle_selection import reentry_block_reason


def _cache(d):
    return {k: {"ltp": v, "atp": v} for k, v in d.items()}


def test_no_pair_when_anchor_below_target():
    # CE anchor 20 < target 30 → select_balanced_pair returns None → 'no_pair'
    cache = _cache({(100, "CE"): 20.0, (100, "PE"): 80.0, (105, "PE"): 15.0})
    out = reentry_block_reason(cache, spot=100, step=5, offset=4, ltp_target=30.0,
                               rule_eval=lambda cs, ps: (True, "ok"))
    assert out["kind"] == "no_pair"


def test_blocked_when_pair_exists_but_rule_fails():
    cache = _cache({(100, "CE"): 60.0, (100, "PE"): 80.0, (105, "PE"): 55.0})
    out = reentry_block_reason(cache, spot=100, step=5, offset=4, ltp_target=30.0,
                               rule_eval=lambda cs, ps: (False, "SLOPE(9.10)<VALUE(0)=✗"))
    assert out["kind"] == "blocked"
    assert out["ce"] == 100 and out["pe"] == 105
    assert out["ce_ltp"] == 60.0 and out["pe_ltp"] == 55.0
    assert "SLOPE" in out["reason"]


def test_passed_when_pair_exists_and_rule_passes():
    cache = _cache({(100, "CE"): 60.0, (100, "PE"): 80.0, (105, "PE"): 55.0})
    out = reentry_block_reason(cache, spot=100, step=5, offset=4, ltp_target=30.0,
                               rule_eval=lambda cs, ps: (True, "all pass"))
    assert out["kind"] == "passed"
    assert out["ce"] == 100 and out["pe"] == 105
