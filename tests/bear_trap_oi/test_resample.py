from datetime import datetime, timedelta, timezone
from strategies.bear_trap_oi.models import Bar
from scripts.bear_trap_oi_backtest import _resample_1m_to_5m


def _m(minute, o, h, l, c):
    base = datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc)
    return Bar(ts=base + timedelta(minutes=minute), open=o, high=h, low=l, close=c)


def test_resample_preserves_real_highs_and_lows_not_just_close_price():
    # 5 real 1-min bars with a high/low that never appears as any bar's
    # CLOSE -- feeding only closes into the accumulator (the bug) would
    # silently drop 110 and 85 from the resulting 5-min bar's high/low.
    bars_1m = [
        _m(0, 100, 101, 99, 100),
        _m(1, 100, 110, 100, 103),   # real high 110, close only 103
        _m(2, 103, 104, 85, 90),     # real low 85, close only 90
        _m(3, 90, 95, 88, 92),
        _m(4, 92, 96, 91, 95),
        _m(5, 95, 97, 94, 96),       # starts next 5-min bucket
    ]
    result = _resample_1m_to_5m(bars_1m)
    assert len(result) == 2  # minutes 0-4 complete the first bucket, minute 5 starts a second
    five_min_bar = result[0]
    assert five_min_bar.open == 100
    assert five_min_bar.high == 110   # must survive even though no bar CLOSED at 110
    assert five_min_bar.low == 85     # must survive even though no bar CLOSED at 85
    assert five_min_bar.close == 95   # close of the last 1-min bar in the bucket (minute 4)
