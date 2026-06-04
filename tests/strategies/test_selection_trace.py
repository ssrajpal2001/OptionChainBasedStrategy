from strategies.straddle_selection import select_balanced_pair, scan_pool

def _sp(d):  # build strike_prem
    return {k: {"ltp": v[0], "atp": v[1]} for k, v in d.items()}

def test_beginning_trace_records_anchor_and_candidates():
    sp = _sp({(100,"CE"):(60,60),(100,"PE"):(55,55),(105,"PE"):(50,50),(95,"PE"):(48,48)})
    tr = []
    select_balanced_pair(sp, spot=100, step=5, offset=2, ltp_target=40, trace=tr)
    joined = " ".join(tr)
    assert "ANCHOR" in joined and "cand" in joined

def test_scan_pool_trace_records_anchor_and_outcome():
    sp = _sp({(100,"CE"):(60,60),(100,"PE"):(55,55),(105,"CE"):(58,58),(95,"PE"):(52,52)})
    tr = []
    scan_pool(sp, spot=100, step=5, offset=2, ltp_target=40,
              rule_pass=lambda c,p: True, trace=tr)
    joined = " ".join(tr)
    assert "ANCHOR" in joined

def test_trace_is_optional_and_default_behavior_unchanged():
    sp = _sp({(100,"CE"):(60,60),(100,"PE"):(55,55),(105,"PE"):(50,50)})
    # no trace arg -> must still work and return same result
    r1 = select_balanced_pair(sp, 100, 5, 2, 40)
    r2 = select_balanced_pair(sp, 100, 5, 2, 40, trace=[])
    assert r1 == r2
