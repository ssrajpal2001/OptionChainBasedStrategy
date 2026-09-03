"""
Unit tests for strategies/liquidity_sweep/detector.py -- pure logic, hand-
built synthetic bar sequences (this is a fresh, direct Python port of a
Pine Script indicator validated on real TradingView charts, not a
Python-backtested strategy -- these tests validate the PORT is faithful to
the documented contracts, not the strategy's real-market edge).
"""
from datetime import datetime, timedelta

from config.global_config import IST
from strategies.liquidity_sweep.detector import (
    Bar, BarAccumulator, SwingPoint, compute_rolling_base, find_swing_points,
    latest_swing_level, latest_pool_level, compute_market_structure,
    detect_sweep, simple_atr, micro_swing_extreme, check_displacement,
    check_fvg, check_retest, compute_trade_plan,
)

BASE = datetime(2026, 8, 18, 9, 15, tzinfo=IST)


def bar(i, o, h, l, c):
    return Bar(timestamp=BASE + timedelta(minutes=5 * i), open=o, high=h, low=l, close=c)


# ── BarAccumulator ───────────────────────────────────────────────────────────

def test_bar_accumulator_buckets_by_timeframe():
    acc = BarAccumulator(timeframe_min=5)
    t0 = BASE
    assert acc.on_tick(t0, 100.0) is False
    assert acc.on_tick(t0 + timedelta(minutes=1), 105.0) is False
    assert acc.on_tick(t0 + timedelta(minutes=2), 95.0) is False
    closed = acc.on_tick(t0 + timedelta(minutes=5), 110.0)
    assert closed is True
    assert len(acc.bars) == 1
    b = acc.bars[0]
    assert b.open == 100.0 and b.high == 105.0 and b.low == 95.0 and b.close == 95.0


def test_bar_accumulator_all_bars_includes_forming_bucket():
    acc = BarAccumulator(timeframe_min=5)
    acc.on_tick(BASE, 100.0)
    assert len(acc.all_bars()) == 1
    assert len(acc.bars) == 0


# ── swing points ──────────────────────────────────────────────────────────

def _fractal_bars():
    # A clean up-down-up sequence so bar index 5 is a confirmed swing HIGH
    # and bar index 10 a confirmed swing LOW, each surrounded by pivot_left=
    # pivot_right=2 strictly-lower/-higher neighbors.
    highs = [100, 101, 102, 110, 102, 101, 100, 99, 98, 90, 98, 99, 100, 101]
    bars = []
    for i, h in enumerate(highs):
        bars.append(bar(i, h - 1, h, h - 2, h - 1))
    return bars


def test_find_swing_points_detects_high_and_low():
    bars = _fractal_bars()
    swings = find_swing_points(bars, pivot_left=2, pivot_right=2)
    highs = [s for s in swings if s.kind == "HIGH"]
    lows = [s for s in swings if s.kind == "LOW"]
    assert any(s.index == 3 for s in highs)
    assert any(s.index == 9 for s in lows)


def test_find_swing_points_confirmation_delay():
    # A pivot at index i is only confirmable once pivot_right bars exist
    # after it -- find_swing_points naturally can't emit anything within
    # the trailing pivot_right window of the bar list.
    bars = _fractal_bars()[:6]   # only 2 bars after the index-3 high
    swings = find_swing_points(bars, pivot_left=2, pivot_right=3)
    assert swings == []


def test_latest_swing_level_picks_most_recent():
    bars = _fractal_bars()
    swings = find_swing_points(bars, pivot_left=2, pivot_right=2)
    latest_high = latest_swing_level(swings, "HIGH")
    assert latest_high is not None
    assert latest_high.index == max(s.index for s in swings if s.kind == "HIGH")


def test_latest_pool_level_requires_min_touches():
    # Two swing highs clustering within tol_pts=5, one lone outlier far away.
    bars = [
        bar(0, 99, 100, 98, 99),
        bar(1, 100, 101, 99, 100),
        bar(2, 98, 99, 97, 98),  # pivot low candidate area
        bar(3, 99, 100, 98, 99),
        bar(4, 100, 101, 99, 100),
        bar(5, 99, 100, 98, 99),
        bar(6, 100, 101, 99, 100),
        bar(7, 99, 100, 98, 99),  # cluster high #2 near 101
        bar(8, 98, 99, 97, 98),
        bar(9, 97, 98, 96, 97),
    ]
    swings = [
        SwingPoint(index=1, timestamp=bar(1, 0, 0, 0, 0).timestamp, price=101.0, body_extreme=100.5, kind="HIGH"),
        SwingPoint(index=4, timestamp=bar(4, 0, 0, 0, 0).timestamp, price=200.0, body_extreme=199.5, kind="HIGH"),  # lone outlier
        SwingPoint(index=7, timestamp=bar(7, 0, 0, 0, 0).timestamp, price=103.0, body_extreme=102.5, kind="HIGH"),  # within 5pts of index=1's 101
    ]
    pool = latest_pool_level(swings, "HIGH", tol_pts=5.0, min_touches=2)
    assert pool is not None
    assert pool.index == 7   # the most recent swing whose cluster reached min_touches
    # A lone outlier alone never forms a pool.
    lone_only = latest_pool_level(swings[1:2], "HIGH", tol_pts=5.0, min_touches=2)
    assert lone_only is None


# ── market structure (BoS/CHoCH) ────────────────────────────────────────────

def test_compute_market_structure_flips_on_close_break():
    bars = [bar(i, 100, 101, 99, 100) for i in range(10)]
    # A confirmed swing HIGH at index 3 (price=105), available from index 3+pivot_right.
    swings = [SwingPoint(index=3, timestamp=bars[3].timestamp, price=105.0, body_extreme=104.5, kind="HIGH")]
    # Bar 8 closes above 105 -- should flip trend bullish, but ONLY once the
    # swing is "available" (index + pivot_right <= i).
    bars[8] = bar(8, 104, 106, 103, 106)
    state = compute_market_structure(bars, swings, pivot_right=2)
    assert state.trend == 1
    assert state.last_event_index == 8


def test_compute_market_structure_respects_pivot_right_delay():
    """A swing must not be usable before index + pivot_right -- this is the
    real look-ahead bug this module's own docstring documents finding and
    fixing during construction."""
    bars = [bar(i, 100, 101, 99, 100) for i in range(6)]
    swings = [SwingPoint(index=3, timestamp=bars[3].timestamp, price=105.0, body_extreme=104.5, kind="HIGH")]
    # Bar 4 closes above 105, but pivot_right=5 means the swing isn't
    # "available" until index 3+5=8 -- bar 4 must NOT react to it.
    bars[4] = bar(4, 104, 106, 103, 106)
    state = compute_market_structure(bars, swings, pivot_right=5)
    assert state.trend == 0
    assert state.last_event is None


# ── sweep ─────────────────────────────────────────────────────────────────

def test_detect_sweep_bear_requires_wick_pierce_and_body_close_back():
    level_high = SwingPoint(index=0, timestamp=BASE, price=110.0, body_extreme=109.0, kind="HIGH")
    # Genuine sweep: high pierces 110 (wick), close falls back under 109 (body).
    sweeping_bar = bar(1, 108, 111, 107, 108.5)
    result = detect_sweep(sweeping_bar, level_high, None)
    assert result.bear is True

    # NOT a sweep: high pierces the wick but close stays ABOVE the body edge
    # (no real rejection).
    weak_bar = bar(2, 108, 111, 107, 109.5)
    result2 = detect_sweep(weak_bar, level_high, None)
    assert result2.bear is False


def test_detect_sweep_bull_mirrors_bear():
    level_low = SwingPoint(index=0, timestamp=BASE, price=90.0, body_extreme=91.0, kind="LOW")
    sweeping_bar = bar(1, 92, 93, 89, 91.5)
    result = detect_sweep(sweeping_bar, None, level_low)
    assert result.bull is True


# ── displacement ──────────────────────────────────────────────────────────

def test_simple_atr_unsmoothed_average():
    bars = [bar(i, 100, 100 + i, 100 - i, 100) for i in range(1, 6)]  # ranges: 2,4,6,8,10
    atr = simple_atr(bars, length=5)
    assert atr == sum([2, 4, 6, 8, 10]) / 5


def test_check_displacement_requires_ratio_and_micro_break():
    prior = [bar(i, 100, 101, 99, 100) for i in range(5)]
    # Strong bullish candle: big body, breaks micro-swing high.
    candidate = bar(5, 100, 108, 100, 107)
    assert check_displacement(candidate, prior, direction=1, swing_len=3, atr_len=5, atr_mult=0.7) is True

    # Weak candle: small body -- should fail the ATR ratio gate.
    weak = bar(5, 100, 101.2, 99.8, 101.0)
    assert check_displacement(weak, prior, direction=1, swing_len=3, atr_len=5, atr_mult=0.7) is False


# ── FVG ───────────────────────────────────────────────────────────────────

def test_check_fvg_bullish_gap():
    c1 = bar(0, 100, 101, 99, 100)
    c3 = bar(2, 103, 105, 102.5, 104)   # low (102.5) > c1.high (101) -> gap
    gap = check_fvg(c1, c3, direction=1)
    assert gap == (101, 102.5)


def test_check_fvg_none_when_no_gap():
    c1 = bar(0, 100, 101, 99, 100)
    c3 = bar(2, 100.5, 101.5, 100.0, 101)   # overlaps c1's high -- no gap
    assert check_fvg(c1, c3, direction=1) is None


# ── retest ────────────────────────────────────────────────────────────────

def test_check_retest_overlap():
    assert check_retest(bar(0, 100, 101, 99.5, 100.5), fvg_lo=99.0, fvg_hi=100.0) is True
    assert check_retest(bar(0, 105, 106, 104, 105), fvg_lo=99.0, fvg_hi=100.0) is False


# ── trade plan ────────────────────────────────────────────────────────────

def test_compute_trade_plan_bullish_rmultiple_fallback():
    plan = compute_trade_plan(direction=1, entry=100.0, sl_anchor=95.0, tgt1_rr=1.5, tgt2_rr=3.0)
    assert plan.sl == 95.0
    assert plan.t1 == 100.0 + 5.0 * 1.5
    assert plan.t2 == 100.0 + 5.0 * 3.0
    assert plan.t2_is_liquidity is False


def test_compute_trade_plan_uses_opposing_liquidity_when_valid():
    plan = compute_trade_plan(direction=1, entry=100.0, sl_anchor=95.0, opposing_liquidity=120.0)
    assert plan.t2 == 120.0
    assert plan.t2_is_liquidity is True


def test_compute_trade_plan_ignores_opposing_liquidity_on_wrong_side():
    # For a bullish (direction=1) trade, an "opposing" level BELOW entry is
    # not a valid target -- must fall back to the R-multiple.
    plan = compute_trade_plan(direction=1, entry=100.0, sl_anchor=95.0, opposing_liquidity=90.0, tgt2_rr=3.0)
    assert plan.t2_is_liquidity is False
    assert plan.t2 == 100.0 + 5.0 * 3.0


def test_compute_trade_plan_bearish_mirrors():
    plan = compute_trade_plan(direction=-1, entry=100.0, sl_anchor=105.0, tgt1_rr=1.5, tgt2_rr=3.0)
    assert plan.t1 == 100.0 - 5.0 * 1.5
    assert plan.t2 == 100.0 - 5.0 * 3.0


# ── rolling base ──────────────────────────────────────────────────────────

def test_compute_rolling_base_tracks_last_breakout():
    bars = [
        bar(0, 100, 102, 98, 100),
        bar(1, 100, 103, 99, 104),   # close (104) > prev.high (102) -> new base = this bar
        bar(2, 104, 104.5, 100, 101),  # close (101) stays within bar1's [99,103] range -- no new breakout
    ]
    base = compute_rolling_base(bars)
    assert base == (bars[1].high, bars[1].low)


def test_compute_rolling_base_none_when_no_breakout():
    bars = [bar(i, 100, 101, 99, 100) for i in range(3)]
    assert compute_rolling_base(bars) is None
