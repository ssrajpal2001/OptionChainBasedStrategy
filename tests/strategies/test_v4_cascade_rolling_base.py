"""resample_bars() must clock-anchor buckets to a configurable session-open
time (default 09:15 for NIFTY/NSE), not a hardcoded one -- CRUDEOIL/MCX
opens at 09:00."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.rolling_base import resample_bars

IST = ZoneInfo("Asia/Kolkata")


def _bar(offset_minutes, o, h, l, c, base_hour=9, base_minute=15):
    """A 5m bar at (base_hour:base_minute) + offset_minutes. Uses timedelta
    arithmetic (not raw hour/minute construction) so callers can freely pass
    offsets >= 45 without a minute-overflow ValueError (e.g. 09:15 + 50min)."""
    from strategies.v4_cascade.book import _Bar  # concrete dataclass-like _Bar (rolling_base._Bar is a Protocol, not instantiable)
    base = datetime(2026, 7, 20, base_hour, base_minute, tzinfo=IST)
    return _Bar(base + timedelta(minutes=offset_minutes), o, h, l, c, tf=5)


def test_default_session_open_matches_nifty_0915():
    # Five 5m bars from 09:15 -> should form exactly one 75m bucket labeled 09:15.
    bars = [_bar(i * 5, 100, 101, 99, 100) for i in range(15)]  # 09:15..10:10, 15 bars = 75min
    out = resample_bars(bars, 75)
    assert len(out) == 1
    assert out[0].timestamp.hour == 9 and out[0].timestamp.minute == 15


def test_custom_session_open_changes_bucket_boundaries():
    # 17 bars from 09:15 to 10:35 -- deliberately ALL at/after both candidate
    # session-open times (09:00 and 09:15), so there is no pre-session bar in
    # either case and resample_bars's grouping-only contract (every input bar
    # lands in exactly one output bucket, nothing dropped) is exercised
    # cleanly. A (9,15) anchor's first 75m bucket runs 09:15-10:30 (bar at
    # 10:30 starts a new bucket); a (9,0) anchor's first 75m bucket runs
    # 09:00-10:15, so for this SAME bar sequence the bucket boundary falls at
    # 10:15 instead -- proving session_open genuinely changes the grouping.
    bars = [_bar(i * 5, 100, 101, 99, 100) for i in range(17)]  # 09:15..10:35
    out_nifty = resample_bars(bars, 75, session_open=(9, 15))
    out_mcx = resample_bars(bars, 75, session_open=(9, 0))
    assert len(out_nifty) == 2 and len(out_mcx) == 2
    # both start their first bucket labeled with the first bar's own ts (09:15)
    assert out_nifty[0].timestamp.hour == 9 and out_nifty[0].timestamp.minute == 15
    assert out_mcx[0].timestamp.hour == 9 and out_mcx[0].timestamp.minute == 15
    # but the SECOND bucket's boundary differs -- this is what actually
    # proves session_open took effect, not an artifact of which bar happens
    # to be first in a chunk.
    assert out_nifty[1].timestamp.hour == 10 and out_nifty[1].timestamp.minute == 30
    assert out_mcx[1].timestamp.hour == 10 and out_mcx[1].timestamp.minute == 15


def test_no_bars_dropped_for_pre_anchor_timestamps():
    # Regression guard: resample_bars must group EVERY input bar into some
    # output bucket, including bars timestamped before session_open (e.g.
    # crypto's 24/7 data, which has no real "pre-session" bars at all --
    # book.py's crypto call sites rely on this NOT filtering anything).
    #
    # 12 bars from 08:15 to 09:10 (all BEFORE the default 09:15 anchor --
    # bucket_idx -1) followed by 12 bars from 09:15 to 10:10 (bucket_idx 0).
    # The first pre-anchor bar carries a deliberately extreme low (50) found
    # nowhere else, so if that whole bucket got silently dropped (the old
    # `bucket_idx < 0: continue` bug), both len(out) AND the combined low
    # would visibly change -- not just a count that could coincidentally
    # still look right.
    pre_bars = [_bar(-60 + i * 5, 100, 101, 50 if i == 0 else 99, 100) for i in range(12)]   # 08:15..09:10
    post_bars = [_bar(i * 5, 100, 101, 99, 100) for i in range(12)]                          # 09:15..10:10
    bars = pre_bars + post_bars

    out = resample_bars(bars, 75)  # default session_open=(9, 15)

    assert len(bars) == 24  # sanity: input size unchanged, nothing pre-filtered
    # Every bar must land in SOME bucket: 2 distinct 75m buckets are expected
    # here (bucket_idx -1 and bucket_idx 0). A dropped pre-anchor bucket
    # would collapse this to 1.
    assert len(out) == 2
    assert out[0].timestamp.hour == 8 and out[0].timestamp.minute == 15
    assert out[1].timestamp.hour == 9 and out[1].timestamp.minute == 15
    # The pre-anchor bucket's extreme low (50) must survive into the output --
    # a dropped bucket would silently raise the combined minimum to 99.
    assert min(b.low for b in out) == 50
