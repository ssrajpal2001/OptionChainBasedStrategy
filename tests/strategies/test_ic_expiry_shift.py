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


def test_update_leg_ltp_respects_position_expiry():
    """Exit-side guard: a next-expiry tick at the same strike must NOT bleed into
    the open position's leg LTP (only the position's own expiry prices its legs)."""
    from data_layer.base_feeder import EventBus
    from config.global_config import GlobalConfig
    from strategies.iron_condor import IronCondorStrategy, IronCondorPosition, IronCondorLeg

    eng = IronCondorStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    eng._position = IronCondorPosition(
        underlying="NIFTY", expiry=W1, atm_at_entry=23300,
        short_ce=IronCondorLeg("sell", "CE", 23600, 2.0, 2.0),
        short_pe=IronCondorLeg("sell", "PE", 23000, 2.0, 2.0),
        long_ce=IronCondorLeg("buy", "CE", 23750, 1.0, 1.0),
        long_pe=IronCondorLeg("buy", "PE", 22850, 1.0, 1.0),
    )

    class _T:
        def __init__(self, strike, otype, ltp, exp):
            self.strike, self.option_type, self.ltp, self.expiry = strike, otype, ltp, exp

    eng._update_leg_ltp(_T(23600, "CE", 99.0, W2))   # next week → ignored
    assert eng._position.short_ce.ltp == 2.0
    eng._update_leg_ltp(_T(23600, "CE", 5.0, W1))    # current week → applied
    assert eng._position.short_ce.ltp == 5.0
