from datetime import datetime, timezone
from strategies.bear_trap_oi.models import Bar
from scripts.bear_trap_oi_backtest import _truncate_to_eod


def _bar_at(hour, minute):
    return Bar(ts=datetime(2026, 10, 1, hour, minute, tzinfo=timezone.utc),
                open=1, high=1, low=1, close=1)


def test_bars_after_1515_are_dropped():
    bars = [_bar_at(9, 20), _bar_at(15, 10), _bar_at(15, 15), _bar_at(15, 20),
            _bar_at(15, 35)]
    truncated = _truncate_to_eod(bars, eod_hour=15, eod_minute=15)
    assert [b.ts.hour * 60 + b.ts.minute for b in truncated] == [
        9 * 60 + 20, 15 * 60 + 10, 15 * 60 + 15,
    ]


def test_no_bars_past_eod_leaves_list_unchanged():
    bars = [_bar_at(9, 20), _bar_at(9, 25)]
    truncated = _truncate_to_eod(bars, eod_hour=15, eod_minute=15)
    assert truncated == bars
