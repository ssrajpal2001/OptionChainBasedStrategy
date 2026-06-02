"""IC min-LTP expiry shift: choose_expiry picks the first expiry whose both short
premiums meet the floor; expiry-aware cache key avoids cross-expiry collision."""
import datetime

from strategies.iron_condor import choose_expiry, _pkey

W1 = datetime.date(2026, 6, 2)   # current week
W2 = datetime.date(2026, 6, 9)   # next week


def test_pkey_distinguishes_expiries():
    assert _pkey(W1, 23500, "CE") != _pkey(W2, 23500, "CE")
    assert _pkey(W1, 23500, "CE") == "2026-06-02:23500CE"


def test_choose_current_when_it_meets_floor():
    rows = [(W1, 60.0, 58.0), (W2, 120.0, 118.0)]
    assert choose_expiry(rows, min_ltp=50.0) == W1


def test_shift_to_next_when_current_too_cheap():
    # current-week shorts 1.65/1.60 (far-OTM near expiry) < 50 → shift to next week
    rows = [(W1, 1.65, 1.60), (W2, 60.0, 58.0)]
    assert choose_expiry(rows, min_ltp=50.0) == W2


def test_none_when_no_expiry_meets_floor():
    rows = [(W1, 1.65, 1.60), (W2, 10.0, 9.0)]
    assert choose_expiry(rows, min_ltp=50.0) is None


def test_skip_expiry_with_missing_premium():
    # current week not streamed yet (0) → skip to next
    rows = [(W1, 0.0, 0.0), (W2, 60.0, 58.0)]
    assert choose_expiry(rows, min_ltp=50.0) == W2


def test_floor_disabled_picks_first_present():
    rows = [(W1, 1.65, 1.60), (W2, 60.0, 58.0)]
    assert choose_expiry(rows, min_ltp=0.0) == W1
