from datetime import date

from data_layer.trading_calendar import NSE_HOLIDAYS, is_trading_day, previous_trading_day


def test_empty_holiday_list_is_pure_weekend_skip():
    # Tuesday -> previous trading day is Monday, with no holidays configured.
    assert previous_trading_day(date(2026, 10, 6)) == date(2026, 10, 5)


def test_monday_is_trading_day_by_default():
    assert is_trading_day(date(2026, 10, 5)) is True


def test_weekend_is_never_a_trading_day():
    assert is_trading_day(date(2026, 10, 4)) is False  # Sunday
    assert is_trading_day(date(2026, 10, 3)) is False  # Saturday


def test_previous_trading_day_skips_weekend():
    # Monday's previous trading day is the prior Friday, not Sunday/Saturday.
    assert previous_trading_day(date(2026, 10, 5)) == date(2026, 10, 2)


def test_holiday_adjacent_to_weekend_steps_back_to_friday():
    """The exact scenario this fix exists for: Tuesday expiry, Monday is a
    real NSE holiday -- the T-1 trading day must be Friday, not Monday
    (no trading happens) and not Sunday/Saturday."""
    monday = date(2026, 10, 5)
    tuesday = date(2026, 10, 6)
    friday = date(2026, 10, 2)
    NSE_HOLIDAYS.add(monday)
    try:
        assert is_trading_day(monday) is False
        assert previous_trading_day(tuesday) == friday
    finally:
        NSE_HOLIDAYS.discard(monday)


def test_consecutive_holidays_step_back_further():
    tuesday = date(2026, 10, 6)
    monday = date(2026, 10, 5)
    friday = date(2026, 10, 2)
    thursday = date(2026, 10, 1)
    NSE_HOLIDAYS.add(monday)
    NSE_HOLIDAYS.add(friday)
    try:
        assert previous_trading_day(tuesday) == thursday
    finally:
        NSE_HOLIDAYS.discard(monday)
        NSE_HOLIDAYS.discard(friday)
