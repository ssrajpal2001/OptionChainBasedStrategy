"""
tests/liquidity_trap/test_detector.py — regression tests for strategies/
liquidity_trap/detector.py, the pure logic direct-ported from scripts/
liquidity_trap_backtest.py (the real-data-validated SENSEX-spot backtest,
1 year, PF 1.95 lot-weighted with the scale-in). These tests lock in the
exact mechanic against hand-built synthetic bars so any future edit to this
module can't silently drift from what was actually validated.
"""
from datetime import datetime, timedelta

from config.global_config import IST
from strategies.liquidity_trap.detector import (
    Bar, BarAccumulator, find_ref_and_bias, find_sl_hit, find_5m_confirmation,
    find_swing_points, find_choch_entry, compute_sl_target, find_scale_in_level,
)

BASE = datetime(2026, 8, 20, 9, 15, tzinfo=IST)


def _bar(minute_offset: int, o, h, l, c) -> Bar:
    return Bar(ts=BASE + timedelta(minutes=minute_offset), open=o, high=h, low=l, close=c)


# ── Stage 1: ref-candle rolling ─────────────────────────────────────────────

def test_ref_bias_locks_bullish_on_clean_high_breach():
    bars = [
        _bar(0, 100, 105, 95, 102),     # candle 1 -- initial ref, H=105 L=95
        _bar(15, 102, 108, 101, 106),   # breaches ref's HIGH only -> BULL locked
    ]
    res = find_ref_and_bias(bars)
    assert res == ("BULL", 0, 1)


def test_ref_bias_locks_bearish_on_clean_low_breach():
    bars = [
        _bar(0, 100, 105, 95, 98),
        _bar(15, 98, 104, 90, 92),       # breaches ref's LOW only -> BEAR locked
    ]
    res = find_ref_and_bias(bars)
    assert res == ("BEAR", 0, 1)


def test_ref_rolls_forward_on_inside_bar():
    bars = [
        _bar(0, 100, 105, 95, 102),      # ref
        _bar(15, 101, 103, 97, 100),     # INSIDE ref's range -> becomes new ref
        _bar(30, 100, 110, 98, 108),     # breaches new ref's (bar 1) high only (L=98 >= 97) -> BULL locked vs ref_idx=1
    ]
    res = find_ref_and_bias(bars)
    assert res == ("BULL", 1, 2)


def test_ref_rolls_forward_on_outside_bar():
    bars = [
        _bar(0, 100, 105, 95, 102),       # ref
        _bar(15, 96, 110, 90, 100),       # breaches BOTH high and low -> becomes new ref
        _bar(30, 100, 115, 92, 112),      # breaches new ref's high only -> BULL locked vs ref_idx=1
    ]
    res = find_ref_and_bias(bars)
    assert res == ("BULL", 1, 2)


def test_ref_bias_none_until_a_clean_breach_happens():
    bars = [_bar(0, 100, 105, 95, 102)]   # only one candle -- nothing to compare yet
    assert find_ref_and_bias(bars) is None


# ── Stage 2: SL-hit (ref candle's own opposite level) ───────────────────────

def test_sl_hit_watches_ref_candles_own_low_for_bull():
    bars = [
        _bar(0, 100, 105, 95, 102),    # ref: L=95
        _bar(15, 102, 108, 101, 106),  # lock candle
        _bar(30, 106, 107, 99, 100),   # doesn't hit 95 yet
        _bar(45, 100, 101, 93, 94),    # hits 95 (L=93 <= 95)
    ]
    ts = find_sl_hit(bars, ref_idx=0, lock_idx=1, bias="BULL")
    assert ts == bars[3].ts


def test_sl_hit_none_when_never_hit():
    bars = [
        _bar(0, 100, 105, 95, 102),
        _bar(15, 102, 108, 101, 106),
        _bar(30, 106, 107, 99, 100),
    ]
    assert find_sl_hit(bars, ref_idx=0, lock_idx=1, bias="BULL") is None


# ── Stage 3: simplified single-fixed-5m-reference confirmation ─────────────

def test_5m_confirmation_fixed_reference_bull():
    bars_5m = [
        _bar(0, 100, 101, 90, 95),    # fixed 5m ref: H=101
        _bar(5, 95, 99, 88, 90),      # doesn't break ref's high; sweep_extreme updates to 88
        _bar(10, 90, 103, 89, 102),   # breaks ref's high (103 > 101) -> confirmed
    ]
    res = find_5m_confirmation(bars_5m, "BULL")
    assert res is not None
    confirm_ts, sweep_extreme = res
    assert confirm_ts == bars_5m[2].ts
    assert sweep_extreme == 88   # deepest low reached through the confirm bar


def test_5m_confirmation_none_until_breached():
    bars_5m = [_bar(0, 100, 101, 90, 95), _bar(5, 95, 100, 92, 96)]
    assert find_5m_confirmation(bars_5m, "BULL") is None


def test_5m_confirmation_does_not_roll_the_reference():
    """2026-08-21 simplified rule: even an inside/outside bar must NOT change
    the reference (unlike Stage 1) -- only the FIRST bar is ever the
    reference for this stage."""
    bars_5m = [
        _bar(0, 100, 101, 90, 95),     # fixed ref: H=101 L=90
        _bar(5, 100, 100.5, 92, 96),   # inside bar -- must NOT become the new ref
        _bar(10, 96, 101.5, 91, 100),  # breaks the ORIGINAL ref's high (101.5 > 101) -> confirmed
    ]
    res = find_5m_confirmation(bars_5m, "BULL")
    assert res is not None
    assert res[0] == bars_5m[2].ts


# ── Stage 4: 1m CHoCH ────────────────────────────────────────────────────────

def test_choch_entry_fires_on_close_above_confirmed_swing_high():
    # Need pivot=2 fractal: bars[2] is a confirmed swing high once bars[3],[4] exist.
    bars = [
        _bar(0, 100, 101, 99, 100),
        _bar(1, 100, 102, 100, 101),
        _bar(2, 101, 105, 101, 104),   # swing HIGH candidate (105)
        _bar(3, 104, 104.5, 102, 103),
        _bar(4, 103, 103.5, 101, 102),  # confirms bar[2] as swing high (index 2 + pivot(2) <= 4)
        _bar(5, 102, 106, 101, 106),    # close (106) > confirmed swing high (105) -> CHoCH
    ]
    res = find_choch_entry(bars, "BULL")
    assert res is not None
    entry_ts, entry_price = res
    assert entry_ts == bars[5].ts
    assert entry_price == 106


def test_choch_entry_none_without_a_break():
    bars = [_bar(i, 100, 101, 99, 100) for i in range(8)]  # flat, no swing ever breaks
    assert find_choch_entry(bars, "BULL") is None


# ── Stage 5: 1:2 risk-reward ─────────────────────────────────────────────────

def test_compute_sl_target_bull():
    sl, target = compute_sl_target(1, entry_price=100.0, sweep_extreme=90.0, rr=2.0)
    assert sl == 90.0
    assert target == 100.0 + 2 * 10.0  # 120.0


def test_compute_sl_target_bear():
    sl, target = compute_sl_target(-1, entry_price=100.0, sweep_extreme=110.0, rr=2.0)
    assert sl == 110.0
    assert target == 100.0 - 2 * 10.0  # 80.0


# ── Stage 6: bear-trap (long) / bull-trap (short) scale-in zone ─────────────

def test_scale_in_level_bear_trap_for_long():
    # ref/sweep/reclaim: ref candle low=95 high=101; sweep candle breaks low (low=90);
    # reclaim candle breaks back above ref's high (high=103).
    bars = [
        _bar(0, 100, 101, 95, 98),     # ref
        _bar(1, 98, 99, 90, 92),       # sweep (sellers_in): low < ref.low
        _bar(2, 92, 103, 91, 101),     # reclaim: high > ref.high
    ]
    res = find_scale_in_level(bars, direction=1)
    assert res is not None
    level, lock_ts = res
    # zone = [entry_line=ref.low=95, sweep_low=90] -> lo=90, hi=95, size=5
    # add_on_level = lo + size/3 = 90 + 1.667 = 91.667
    assert round(level, 3) == round(90 + 5 / 3.0, 3)
    assert lock_ts == bars[2].ts


def test_scale_in_level_bull_trap_for_short():
    # Mirror: ref high=101 low=98; sweep breaks high (high=108); reclaim breaks low (low=96).
    bars = [
        _bar(0, 100, 101, 98, 99),
        _bar(1, 99, 108, 97, 105),      # sweep: high > ref.high
        _bar(2, 105, 106, 96, 100),     # reclaim: low < ref.low
    ]
    res = find_scale_in_level(bars, direction=-1)
    assert res is not None
    level, lock_ts = res
    # zone = [entry_line=ref.high=101, sweep_low_field=108] -> lo=101, hi=108, size=7
    # add_on_level = hi - size/3 = 108 - 2.333 = 105.667
    assert round(level, 3) == round(108 - 7 / 3.0, 3)


def test_scale_in_level_none_with_fewer_than_3_bars():
    bars = [_bar(0, 100, 101, 95, 98), _bar(1, 98, 99, 90, 92)]
    assert find_scale_in_level(bars, direction=1) is None


def test_scale_in_level_none_when_no_zone_confirmed():
    # sweep happens but reclaim never does -- no zone.
    bars = [
        _bar(0, 100, 101, 95, 98),
        _bar(1, 98, 99, 90, 92),
        _bar(2, 92, 94, 89, 91),   # never reclaims above ref.high (101)
    ]
    assert find_scale_in_level(bars, direction=1) is None


# ── BarAccumulator ───────────────────────────────────────────────────────────

def test_bar_accumulator_buckets_and_reports_close():
    acc = BarAccumulator(timeframe_min=5)
    t0 = BASE
    assert acc.on_tick(t0, 100.0) is False
    assert acc.on_tick(t0 + timedelta(minutes=2), 105.0) is False
    assert acc.on_tick(t0 + timedelta(minutes=4), 98.0) is False
    closed = acc.on_tick(t0 + timedelta(minutes=5), 110.0)   # new 5m bucket
    assert closed is True
    assert len(acc.bars) == 1
    b = acc.bars[0]
    assert b.open == 100.0 and b.high == 105.0 and b.low == 98.0 and b.close == 98.0
