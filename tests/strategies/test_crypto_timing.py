"""Crypto (Delta) 24/7 entry window + expiry-gap square-off: trade everywhere EXCEPT the daily
expiry gap [squareoff, entry_start] (e.g. 16:30→18:30 around the 17:30 IST expiry)."""
from datetime import datetime, time

from config.global_config import IST, GlobalConfig
from strategies.sell_straddle import SellStraddleStrategy
from data_layer.base_feeder import EventBus


def _book(crypto=True):
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="BTC" if crypto else "NIFTY")
    s._is_crypto = crypto
    s._entry_start = time(18, 30)
    s._entry_cutoff = time(16, 30)
    s._force_exit = time(16, 30)
    return s


def _at(h, m):
    return datetime(2026, 6, 13, h, m, tzinfo=IST)


def test_crypto_entry_window_wraps_excluding_gap():
    s = _book(crypto=True)
    # Inside the gap 16:30–18:30 → NO entry, and must be flat.
    assert s._is_in_entry_window(_at(17, 0)) is False
    assert s._past_squareoff(_at(17, 0)) is True
    assert s._past_squareoff(_at(16, 30)) is True
    # Outside the gap (wraps midnight) → entry allowed, not flat.
    for h, m in [(18, 30), (23, 0), (2, 0), (9, 0), (16, 29)]:
        assert s._is_in_entry_window(_at(h, m)) is True, f"{h}:{m} should allow entry"
        assert s._past_squareoff(_at(h, m)) is False, f"{h}:{m} should NOT be flat"


def test_nse_window_unchanged():
    s = _book(crypto=False)
    s._entry_start, s._entry_cutoff, s._force_exit = time(9, 20), time(15, 15), time(15, 15)
    assert s._is_in_entry_window(_at(10, 0)) is True
    assert s._is_in_entry_window(_at(8, 0)) is False
    assert s._is_in_entry_window(_at(15, 30)) is False
    assert s._past_squareoff(_at(15, 30)) is True
    assert s._past_squareoff(_at(10, 0)) is False


def test_crypto_session_day_rolls_at_1730_not_midnight():
    s = _book(crypto=True)
    from datetime import date
    # 18:30 (start) through next-day 16:30 (and across midnight) = ONE session day (the 17:30 expiry).
    d_1830 = s._session_day(_at(18, 30))
    d_2359 = s._session_day(datetime(2026, 6, 13, 23, 59, tzinfo=IST))
    d_0001 = s._session_day(datetime(2026, 6, 14, 0, 1, tzinfo=IST))   # past midnight
    d_1629 = s._session_day(datetime(2026, 6, 14, 16, 29, tzinfo=IST))
    assert d_1830 == d_2359 == d_0001 == d_1629    # midnight does NOT change the session day
    # At 17:30 the expiry rolls → new session day.
    assert s._session_day(datetime(2026, 6, 14, 17, 30, tzinfo=IST)) != d_1629


def test_nse_session_day_is_calendar_date():
    s = _book(crypto=False)
    from datetime import date
    assert s._session_day(_at(10, 0)) == date(2026, 6, 13)
    assert s._session_day(datetime(2026, 6, 14, 0, 1, tzinfo=IST)) == date(2026, 6, 14)
