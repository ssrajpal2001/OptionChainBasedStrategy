"""strategies/v4_cascade/rolling_base.py's find_all_bear_zones/
find_all_bull_zones -- the multi-zone-pool counterpart to find_bear_zone/
find_bull_zone (which only ever return the single newest match)."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _Bar
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 1, 9, 15, tzinfo=IST)


def _bar(offset, o, h, l, c):
    return _Bar(_BASE + timedelta(minutes=75 * offset), o, h, l, c, tf=75)


def test_finds_multiple_distinct_bear_zones():
    # Zone A: ref@0 (low=100,high=110), sweep@1 (low=90), reclaim@2 (high=115).
    # Zone B: ref@3 (low=200,high=210), sweep@4 (low=190), reclaim@5 (high=215).
    bars = [
        _bar(0, 105, 110, 100, 105),
        _bar(1, 95, 100, 90, 95),
        _bar(2, 96, 115, 95, 112),
        _bar(3, 205, 210, 200, 205),
        _bar(4, 195, 200, 190, 195),
        _bar(5, 196, 215, 195, 212),
    ]
    zones = find_all_bear_zones(bars)
    ref_ts = sorted(z.reference_low_ts for z in zones)
    assert ref_ts == [bars[0].timestamp, bars[3].timestamp]


def test_known_ref_ts_excludes_already_handled_zones():
    bars = [
        _bar(0, 105, 110, 100, 105),
        _bar(1, 95, 100, 90, 95),
        _bar(2, 96, 115, 95, 112),
    ]
    known = {bars[0].timestamp}
    assert find_all_bear_zones(bars, known_ref_ts=known) == []


def test_same_candle_sweep_and_reclaim_rejected():
    # candle 1 both sweeps below ref's low AND reclaims above ref's high
    # within its own range -- must NOT be accepted (3-candle rule).
    bars = [
        _bar(0, 105, 110, 100, 105),
        _bar(1, 95, 120, 90, 112),
    ]
    assert find_all_bear_zones(bars) == []


def test_finds_multiple_distinct_bull_zones():
    bars = [
        _bar(0, 105, 110, 100, 105),
        _bar(1, 112, 120, 108, 115),
        _bar(2, 95, 100, 90, 92),
    ]
    zones = find_all_bull_zones(bars)
    assert len(zones) == 1
    assert zones[0].reference_low_ts == bars[0].timestamp
    assert zones[0].entry_line == 110  # ref.high
    assert zones[0].sl_level == 100    # ref.low
