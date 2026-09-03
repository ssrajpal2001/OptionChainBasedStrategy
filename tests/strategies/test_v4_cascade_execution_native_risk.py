"""strategies/v4_cascade/execution_risk.py's compute_execution_native_risk --
part of the 2026-07-21 execution-native risk design
(docs/superpowers/specs/2026-07-21-v4-cascade-execution-native-risk-design.md).
Runs the SAME sweep-detection logic Gate 2 already uses
(find_all_bear_traps_2candle, 5m-then-15m-fallback) against the EXECUTION
strike's own bars, once, at entry -- not a continuous scanner. Returns None
(triggering the caller's fallback to today's tracking-scaled approach) when
neither timeframe finds a valid zone."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _Bar
from strategies.v4_cascade.execution_risk import compute_execution_native_risk

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 21, 9, 15, tzinfo=IST)


def _bar(offset_5m, o, h, l, c):
    return _Bar(_BASE + timedelta(minutes=5 * offset_5m), o, h, l, c, tf=5)


def _trap_pattern_5m(offset0, level_offset=0.0):
    """entry_line=100+level_offset, sweep_low=95+level_offset,
    sl_level=110+level_offset (ref.high)."""
    lo = level_offset
    return [
        _bar(offset0, 105 + lo, 110 + lo, 100 + lo, 105 + lo),      # ref
        _bar(offset0 + 1, 98 + lo, 105 + lo, 95 + lo, 100 + lo),    # sweep
        _bar(offset0 + 2, 110 + lo, 115 + lo, 105 + lo, 112 + lo),  # reclaim (trapped)
    ]


def test_finds_zone_on_5m_and_returns_correct_sl_and_target_long():
    bars = _trap_pattern_5m(0)
    result = compute_execution_native_risk(
        bars, exec_entry_price=99.0, sl_buffer=5.0, is_short=False,
    )
    assert result is not None
    sl_price, target_price = result
    # zone_low = min(entry_line=100, sweep_low=95) = 95; risk = (99-95)+5 = 9
    assert abs(sl_price - (99.0 - 9.0)) < 1e-6
    # target = zone.sl_level (ref.high=110) distance from entry, floored at risk (9)
    # raw distance = 110-99 = 11, > risk(9), so raw distance is used
    assert abs(target_price - (99.0 + 11.0)) < 1e-6


def test_falls_back_to_15m_when_5m_finds_nothing():
    # A pattern that only resolves at 15m (mirrors
    # test_v4_cascade_index_gated_scanner.py's equivalent fixture): bucket0
    # (bars 0-2) is the 15m ref (non-decreasing internally, no 5m dips),
    # bucket1 (bars 3-5) supplies both the 15m sweep and reclaim in one
    # bucket, bucket2 stays flat.
    bucket0 = [_bar(0, 104, 106, 100, 104), _bar(1, 104, 107, 101, 105), _bar(2, 105, 108, 102, 106)]
    bucket1 = [_bar(3, 115, 130, 110, 120), _bar(4, 97, 98, 95, 96), _bar(5, 96, 99, 96, 97)]
    bucket2 = [_bar(6, 97, 100, 97, 98), _bar(7, 98, 101, 98, 99), _bar(8, 99, 102, 99, 100)]
    bars = bucket0 + bucket1 + bucket2
    result = compute_execution_native_risk(
        bars, exec_entry_price=99.0, sl_buffer=5.0, is_short=False,
    )
    assert result is not None


def test_returns_none_when_no_zone_found_on_either_timeframe():
    # A flat, featureless series -- no sweep, no reclaim, nothing at 5m or 15m.
    bars = [_bar(i, 100, 101, 99, 100) for i in range(10)]
    result = compute_execution_native_risk(
        bars, exec_entry_price=100.0, sl_buffer=5.0, is_short=False,
    )
    assert result is None


def test_returns_none_when_fewer_than_3_bars():
    bars = _trap_pattern_5m(0)[:2]
    result = compute_execution_native_risk(bars, exec_entry_price=99.0, sl_buffer=5.0)
    assert result is None


def test_short_geometry_returns_sl_above_and_target_below_entry():
    # Bull-zone (short, crypto PE only): ref.high=entry_line, ref.low=sl_level.
    bars = [
        _bar(0, 105, 110, 100, 105),      # ref: high=110 (entry_line), low=100 (sl_level)
        _bar(1, 112, 115, 108, 110),      # sweep up (buyers in)
        _bar(2, 95, 100, 90, 96),         # reclaim down through ref.low -- trapped
    ]
    result = compute_execution_native_risk(
        bars, exec_entry_price=101.0, sl_buffer=5.0, is_short=True,
    )
    assert result is not None
    sl_price, target_price = result
    assert sl_price > 101.0
    assert target_price < 101.0


def test_picks_the_most_recent_zone_when_multiple_exist():
    # Two independent patterns at different price levels -- the most
    # recently discovered (by reference_low_ts) must win.
    bars = _trap_pattern_5m(0) + _trap_pattern_5m(10, level_offset=50.0)
    result = compute_execution_native_risk(
        bars, exec_entry_price=149.0, sl_buffer=5.0, is_short=False,
    )
    assert result is not None
    sl_price, target_price = result
    # Should use the SECOND (later, shifted) zone: zone_low=145, risk=(149-145)+5=9
    assert abs(sl_price - (149.0 - 9.0)) < 1e-6
