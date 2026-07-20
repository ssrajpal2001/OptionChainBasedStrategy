"""strategies/v4_cascade/zone_state.py's IndexGatedPremiumScanner -- the core
2026-07-20 Index/Premium decoupling gate machine (NIFTY/CRUDEOIL real-options
path). Covers: armed hard-gates discovery, the scan-window anchor is
monotonic, an already-discovered setup is never aborted once unarmed (the
"never abort in-flight" decision), multi-zone discovery, the 15m fallback,
close-based invalidation, and pop/invalidate."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _Bar
from strategies.v4_cascade.dataclasses import PremiumZoneState
from strategies.v4_cascade.zone_state import IndexGatedPremiumScanner

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 20, 9, 15, tzinfo=IST)


def _bar5(offset_5m, o, h, l, c):
    return _Bar(_BASE + timedelta(minutes=5 * offset_5m), o, h, l, c, tf=5)


def _trap_pattern(offset0):
    """A 3-bar 2-candle bear-trap starting at 5m-offset ``offset0``: ref
    (low=100, high=110), an IMMEDIATE next candle that sweeps below it
    (low=95), then a later candle whose high reclaims back above the ref's
    high (115) -- exactly what find_all_bear_traps_2candle looks for.
    entry_line=100, sweep_low=95, limit_entry_price=100-(5/3)=98.33."""
    return [
        _bar5(offset0, 105, 110, 100, 105),      # ref
        _bar5(offset0 + 1, 98, 105, 95, 100),    # sweep (immediate next candle)
        _bar5(offset0 + 2, 110, 115, 105, 112),  # trapped (reclaim)
    ]


def test_discovery_does_not_run_while_unarmed():
    s = IndexGatedPremiumScanner()
    for b in _trap_pattern(0):
        s.on_5m_bar(b)
    assert s.setups == []
    assert s.armed is False


def test_discovery_runs_once_armed_with_window_anchor():
    s = IndexGatedPremiumScanner()
    confirmed_ts = _BASE  # anchor at/before the ref candle -- window includes it
    s.set_armed(True, confirmed_ts=confirmed_ts)
    for b in _trap_pattern(0):
        s.on_5m_bar(b)
    assert len(s.setups) == 1
    setup = s.setups[0]
    assert setup.state == PremiumZoneState.PREMIUM_LOCKED
    assert setup.zone.entry_line == 100
    assert setup.zone.sweep_low == 95
    assert setup.timeframe == 5


def test_bars_before_confirmed_ts_are_excluded_from_the_window():
    s = IndexGatedPremiumScanner()
    bars = _trap_pattern(0)
    # Anchor the window at (just after) the reclaim candle's own timestamp --
    # the ref+sweep candles that formed this exact pattern are now BEFORE the
    # window, so this specific pattern must not be (re)discovered.
    late_anchor = bars[-1].timestamp + timedelta(minutes=1)
    s.set_armed(True, confirmed_ts=late_anchor)
    for b in bars:
        s.on_5m_bar(b)
    assert s.setups == []


def test_window_anchor_is_monotonic_never_rewinds():
    s = IndexGatedPremiumScanner()
    later = _BASE + timedelta(minutes=100)
    earlier = _BASE
    s.set_armed(True, confirmed_ts=later)
    assert s._scan_window_start_ts == later
    # A STALE/older re-confirmation must not rewind the window backward.
    s.set_armed(True, confirmed_ts=earlier)
    assert s._scan_window_start_ts == later
    # A genuinely later re-confirmation DOES advance it.
    even_later = later + timedelta(minutes=50)
    s.set_armed(True, confirmed_ts=even_later)
    assert s._scan_window_start_ts == even_later


def test_never_aborts_in_flight_setup_when_unarmed():
    s = IndexGatedPremiumScanner()
    s.set_armed(True, confirmed_ts=_BASE)
    bars = _trap_pattern(0)
    for b in bars:
        s.on_5m_bar(b)
    assert len(s.setups) == 1

    # Unarm -- the already-discovered setup must survive and keep advancing.
    s.set_armed(False)
    assert s.armed is False
    assert len(s.setups) == 1

    # Feed a bar that re-enters the zone [95, 100] -- this should still
    # advance the setup to LIMIT_ARMED even though the scanner is unarmed,
    # since "armed" only gates NEW discovery, never in-flight progress.
    reentry_bar = _bar5(10, 99, 100, 96, 98)
    s.on_5m_bar(reentry_bar)
    assert s.setups[0].state == PremiumZoneState.LIMIT_ARMED
    assert s.setups[0].limit_entry_price is not None
    expected_limit = 100 - (100 - 95) / 3.0
    assert abs(s.setups[0].limit_entry_price - expected_limit) < 1e-9


def test_multi_zone_two_independent_setups_tracked_concurrently():
    s = IndexGatedPremiumScanner()
    s.set_armed(True, confirmed_ts=_BASE)
    # Two independent trap patterns at different price levels, far enough
    # apart in time that they don't interfere with each other's ref search.
    bars = _trap_pattern(0) + _trap_pattern(10)
    # Shift the second pattern's prices to a different level so it's a
    # genuinely distinct zone, not a re-discovery of the same one.
    shifted = [
        _Bar(b.timestamp, b.open + 50, b.high + 50, b.low + 50, b.close + 50, tf=5)
        for b in bars[3:]
    ]
    for b in bars[:3] + shifted:
        s.on_5m_bar(b)
    assert len(s.setups) == 2
    entry_lines = sorted(setup.zone.entry_line for setup in s.setups)
    assert entry_lines == [100, 150]


def test_15m_fallback_only_when_5m_finds_nothing_new():
    s = IndexGatedPremiumScanner()
    s.set_armed(True, confirmed_ts=_BASE)
    # A pattern that resolves ONLY once resampled to 15m, never at 5m -- by
    # construction (verified by direct execution while designing this test,
    # not just by hand): bucket0 (bars 0-2) is the 15m ref (low=100,
    # high=108, internally non-decreasing -- no 5m adjacent-pair dips).
    # bucket1 (bars 3-5) supplies BOTH the 15m sweep (bar4's low=95 < 100)
    # AND the 15m reclaim (bar3's high=130 > 108) in one bucket. At 5m
    # granularity this DOES produce sweep candidates (e.g. ref=bar3, since
    # bar4's low dips below it), but bar3's own high (130) is never exceeded
    # by anything later, so no 5m ref is ever "trapped" -- confirmed
    # separately for every candidate ref (bar0-bar4) before finalizing this
    # fixture. bucket2 (bars 6-8) stays flat/modest so it introduces no new
    # 5m dips or reclaims of its own.
    bucket0 = [_bar5(0, 104, 106, 100, 104), _bar5(1, 104, 107, 101, 105), _bar5(2, 105, 108, 102, 106)]
    bucket1 = [_bar5(3, 115, 130, 110, 120), _bar5(4, 97, 98, 95, 96), _bar5(5, 96, 99, 96, 97)]
    bucket2 = [_bar5(6, 97, 100, 97, 98), _bar5(7, 98, 101, 98, 99), _bar5(8, 99, 102, 99, 100)]
    for b in bucket0 + bucket1 + bucket2:
        s.on_5m_bar(b)
    assert len(s.setups) == 1
    assert s.setups[0].timeframe == 15
    assert s.setups[0].zone.entry_line == 100
    assert s.setups[0].zone.sweep_low == 95


def test_invalidate_broken_setup_on_close_through_zone_low():
    s = IndexGatedPremiumScanner()
    s.set_armed(True, confirmed_ts=_BASE)
    for b in _trap_pattern(0):
        s.on_5m_bar(b)
    assert len(s.setups) == 1
    # A wick through the zone low (95) is the signal itself, not a failure --
    # low pierces but close stays above -- must NOT invalidate.
    wick_bar = _bar5(10, 96, 97, 90, 96)
    s.on_5m_bar(wick_bar)
    assert len(s.setups) == 1
    # A genuine CLOSE below the zone low -- this IS a structural failure.
    close_break_bar = _bar5(11, 94, 95, 90, 92)
    s.on_5m_bar(close_break_bar)
    assert s.setups == []


def test_pop_setup_and_invalidate_setup_remove_only_the_target():
    s = IndexGatedPremiumScanner()
    s.set_armed(True, confirmed_ts=_BASE)
    bars = _trap_pattern(0)
    shifted = [
        _Bar(b.timestamp, b.open + 50, b.high + 50, b.low + 50, b.close + 50, tf=5)
        for b in _trap_pattern(10)
    ]
    for b in bars + shifted:
        s.on_5m_bar(b)
    assert len(s.setups) == 2
    victim = s.setups[0]
    survivor = s.setups[1]
    s.pop_setup(victim, victim.zone.lock_ts)
    assert s.setups == [survivor]
    s.invalidate_setup(survivor)
    assert s.setups == []


def test_reset_clears_all_state():
    s = IndexGatedPremiumScanner()
    s.set_armed(True, confirmed_ts=_BASE)
    for b in _trap_pattern(0):
        s.on_5m_bar(b)
    assert s.setups
    s.reset()
    assert s.setups == []
    assert s.armed is False
    assert s._scan_window_start_ts is None
