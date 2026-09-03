"""_bucket_start/_bucket_end/_bucket_key must anchor to a configurable
session-open time -- CRUDEOIL/MCX opens at 09:00, not NIFTY's 09:15."""
from datetime import datetime
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _bucket_start, _bucket_end, _bucket_key

IST = ZoneInfo("Asia/Kolkata")


def test_bucket_start_default_anchors_to_0915():
    ts = datetime(2026, 7, 20, 10, 32, tzinfo=IST)
    assert _bucket_start(ts, 5) == datetime(2026, 7, 20, 10, 30, tzinfo=IST)


def test_bucket_start_session_open_changes_75m_boundary():
    # At 5m/15m granularity, a 15-minute anchor shift (09:00 vs 09:15) never
    # changes bucket boundaries -- 15 is an exact multiple of both 5 and 15,
    # so the floor-division grids coincide regardless of anchor (verified:
    # _bucket_start(9:03, 5, session_open=X) is IDENTICAL for X=(9,15) and
    # X=(9,0), both give 09:00 -- there is no timestamp at 5m/15m granularity
    # that can distinguish the two anchors). It DOES matter at 75m
    # granularity (15 is NOT a multiple of 75) -- Gate 1's real timeframe,
    # and where this must actually differ.
    ts = datetime(2026, 7, 20, 10, 20, tzinfo=IST)
    assert _bucket_start(ts, 75, session_open=(9, 15)) == datetime(2026, 7, 20, 9, 15, tzinfo=IST)
    assert _bucket_start(ts, 75, session_open=(9, 0)) == datetime(2026, 7, 20, 10, 15, tzinfo=IST)


def test_bucket_end_respects_session_open():
    # The 5m bar starting at 10:10 is the LAST bar of the 09:00-10:15 75m
    # bucket under a (9,0) anchor (70+5=75, evenly divisible by 75), but is
    # NOT the last bar of any bucket under the default (9,15) anchor (whose
    # 75m buckets run 09:15-10:30, so 10:10 is mid-bucket there: 55+5=60,
    # not evenly divisible by 75).
    ts = datetime(2026, 7, 20, 10, 10, tzinfo=IST)
    assert _bucket_end(ts, 75, session_open=(9, 0)) is True
    assert _bucket_end(ts, 75, session_open=(9, 15)) is False


def test_bucket_key_identity_stable_across_multiplier():
    ts1 = datetime(2026, 7, 20, 9, 0, tzinfo=IST)
    ts2 = datetime(2026, 7, 20, 9, 3, tzinfo=IST)
    # Both fall in the same 5-minute bucket under a 09:00 anchor.
    assert _bucket_key(ts1, 5, session_open=(9, 0)) == _bucket_key(ts2, 5, session_open=(9, 0))
