"""resample_bars() must clock-anchor buckets to a configurable session-open
time (default 09:15 for NIFTY/NSE), not a hardcoded one -- CRUDEOIL/MCX
opens at 09:00."""
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from strategies.v4_cascade.rolling_base import resample_bars
from strategies.v4_cascade.dataclasses import RollingBaseZone  # noqa: F401 (import sanity)

IST = ZoneInfo("Asia/Kolkata")


@dataclass
class SimpleBar:
    """Minimal bar object matching the _Bar Protocol."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    tf: int = 5


def _bar(hour, minute, o, h, l, c):
    # Handle minute overflow into next hour
    actual_hour = hour + (minute // 60)
    actual_minute = minute % 60
    ts = datetime(2026, 7, 20, actual_hour, actual_minute, tzinfo=IST)
    return SimpleBar(ts, o, h, l, c, tf=5)


def test_default_session_open_matches_nifty_0915():
    # Five 5m bars from 09:15 -> should form exactly one 75m bucket labeled 09:15.
    bars = [_bar(9, 15 + i * 5, 100, 101, 99, 100) for i in range(15)]  # 09:15..10:10, 15 bars = 75min
    out = resample_bars(bars, 75)
    assert len(out) == 1
    assert out[0].timestamp.hour == 9 and out[0].timestamp.minute == 15


def test_custom_session_open_0900_for_mcx():
    # Same 15 bars, but starting at 09:00 (MCX) -- with session_open=(9,15) (default),
    # a 09:00 bar is BEFORE the session open, so it lands in the PREVIOUS day's
    # bucket_idx (negative), grouping incorrectly. With session_open=(9,0), it's
    # correctly bucket 0.
    bars = [_bar(9, 0 + i * 5, 100, 101, 99, 100) for i in range(15)]  # 09:00..09:55
    out_default = resample_bars(bars, 75, session_open=(9, 15))
    out_mcx = resample_bars(bars, 75, session_open=(9, 0))
    assert len(out_mcx) == 1
    assert out_mcx[0].timestamp.hour == 9 and out_mcx[0].timestamp.minute == 0
    # default (9,15) anchor treats these as pre-open (negative bucket_idx), so
    # they land in a DIFFERENT bucket than the mcx-anchored version
    assert out_default[0].timestamp != out_mcx[0].timestamp
