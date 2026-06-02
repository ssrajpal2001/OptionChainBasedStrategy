from strategies.straddle_selection import classify_roll


def test_no_candidates_full_exit():
    assert classify_roll(ce_same=True, pe_same=True, has_candidates=False) == "full_exit"


def test_both_same_virtual():
    assert classify_roll(ce_same=True, pe_same=True, has_candidates=True) == "virtual"


def test_ce_same_pe_changed_partial_pe():
    assert classify_roll(ce_same=True, pe_same=False, has_candidates=True) == "partial_pe"


def test_pe_same_ce_changed_partial_ce():
    assert classify_roll(ce_same=False, pe_same=True, has_candidates=True) == "partial_ce"


def test_both_changed_physical():
    assert classify_roll(ce_same=False, pe_same=False, has_candidates=True) == "physical"
