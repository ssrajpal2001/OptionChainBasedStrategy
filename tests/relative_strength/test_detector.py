"""
Unit tests for strategies/relative_strength/detector.py -- the pure Python
port of bharatTrader's "Relative Strength" Pine Script v6 indicator.
"""
from strategies.relative_strength.detector import (
    sma, compute_rs_series, evaluate_relative_strength, rank_by_relative_strength,
    align_closes,
)


def test_align_closes_keeps_only_common_timestamps_sorted():
    base = [{"ts": "2026-09-01", "close": 100.0}, {"ts": "2026-09-08", "close": 105.0},
            {"ts": "2026-09-15", "close": 110.0}]
    comp = [{"ts": "2026-09-08", "close": 200.0}, {"ts": "2026-09-15", "close": 205.0},
            {"ts": "2026-09-22", "close": 210.0}]  # missing 09-01, has an extra 09-22
    b, c = align_closes(base, comp)
    assert b == [105.0, 110.0]
    assert c == [200.0, 205.0]


def test_align_closes_skips_candles_with_no_close():
    base = [{"ts": "2026-09-01", "close": None}, {"ts": "2026-09-08", "close": 105.0}]
    comp = [{"ts": "2026-09-01", "close": 50.0}, {"ts": "2026-09-08", "close": 52.0}]
    b, c = align_closes(base, comp)
    assert b == [105.0]
    assert c == [52.0]


def test_sma_none_before_warmup_then_correct_average():
    vals = [1.0, 2.0, 3.0, 4.0, 5.0]
    out = sma(vals, 3)
    assert out[0] is None and out[1] is None
    assert out[2] == (1.0 + 2.0 + 3.0) / 3
    assert out[3] == (2.0 + 3.0 + 4.0) / 3
    assert out[4] == (3.0 + 4.0 + 5.0) / 3


def test_compute_rs_series_matches_pine_formula_hand_computed():
    # base doubles over the window (100->200), comparative rises 50% (100->150).
    # res = (200/100) / (150/100) - 1 = 2/1.5 - 1 = 0.333...
    base = [100.0] * 5 + [200.0]
    comp = [100.0] * 5 + [150.0]
    series = compute_rs_series(base, comp, length=5)
    assert series[4] is None  # not enough history yet (needs index >= length)
    assert series[5] is not None
    assert abs(series[5] - (2 / 1.5 - 1)) < 1e-9


def test_compute_rs_series_none_on_zero_denominator():
    base = [0.0, 100.0]
    comp = [50.0, 60.0]
    series = compute_rs_series(base, comp, length=1)
    assert series[1] is None, "base_then=0 must degrade to None, never a ZeroDivisionError or 0"


def test_compute_rs_series_length_mismatch_returns_all_none():
    series = compute_rs_series([1.0, 2.0, 3.0], [1.0, 2.0], length=1)
    assert series == [None, None, None]


def test_evaluate_relative_strength_outperformer_is_positive_and_underperformer_negative():
    n = 130
    # Base outperforms comparative steadily -> RS should be positive at the end.
    base = [100.0 + i * 1.0 for i in range(n)]
    comp = [100.0 + i * 0.3 for i in range(n)]
    out = evaluate_relative_strength("OUTPERFORMER", base, comp, length=123, trend_base=5, ma_length=10)
    assert out is not None
    assert out.rs > 0

    base2 = [100.0 + i * 0.3 for i in range(n)]
    comp2 = [100.0 + i * 1.0 for i in range(n)]
    out2 = evaluate_relative_strength("UNDERPERFORMER", base2, comp2, length=123, trend_base=5, ma_length=10)
    assert out2 is not None
    assert out2.rs < 0


def test_evaluate_relative_strength_rs_trend_rising_when_rs_climbing():
    n = 135
    base = [100.0] * n
    comp = [100.0] * n
    # Make the base start outperforming only in the last stretch, so the RS
    # value itself is climbing over the last `trend_base` bars.
    for i in range(n - 10, n):
        base[i] = base[i - 1] * 1.01
    out = evaluate_relative_strength("RISER", base, comp, length=123, trend_base=5, ma_length=10)
    assert out is not None
    assert out.rs_trend == "rising"


def test_evaluate_relative_strength_returns_none_without_enough_history():
    out = evaluate_relative_strength("TOO_SHORT", [100.0] * 50, [100.0] * 50, length=123)
    assert out is None


def test_rank_by_relative_strength_orders_descending_by_rs():
    from strategies.relative_strength.detector import RSReading
    a = RSReading(symbol="A", rs=0.10, rs_trend="flat", rs_ma=None, ma_trend=None)
    b = RSReading(symbol="B", rs=0.25, rs_trend="flat", rs_ma=None, ma_trend=None)
    c = RSReading(symbol="C", rs=-0.05, rs_trend="flat", rs_ma=None, ma_trend=None)
    ranked = rank_by_relative_strength([a, b, c])
    assert [r.symbol for r in ranked] == ["B", "A", "C"]


def test_rank_by_relative_strength_breaks_near_tie_by_rising_trend():
    from strategies.relative_strength.detector import RSReading
    falling = RSReading(symbol="FALLING", rs=0.200000, rs_trend="falling", rs_ma=None, ma_trend=None)
    rising = RSReading(symbol="RISING", rs=0.200000, rs_trend="rising", rs_ma=None, ma_trend=None)
    ranked = rank_by_relative_strength([falling, rising])
    assert ranked[0].symbol == "RISING", "an exact RS tie must prefer the rising-trend candidate"
