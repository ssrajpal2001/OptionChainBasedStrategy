"""Regression test for the 2026-09-30 CRITICAL FIX, real live incident:
matrix_engine/indicators.py's rsi() truncated its input to the last 15
closes before computing anything, which left exactly 14 deltas -- so the
recursive Wilder-smoothing loop (`for i in range(period, len(deltas))`,
i.e. `range(14, 14)`) was an empty range that never executed. Every call
was silently just a plain average of the last 14 gains/losses, re-seeded
from scratch every time -- not genuine Wilder smoothing at all, despite
the function's own docstring and the module's "Wilder's smoothing"
comment. Confirmed live: this diverged sharply (18.13 vs a real
third-party chart's genuine Wilder RSI of 34.54) on the identical
combined premium series at the same moment.

Fixed by dropping the truncation -- every real caller already passes the
full accumulated closes series, and Wilder's RSI is fully determined by
the complete delta sequence from the start, so recomputing over the whole
array each call is mathematically equivalent to true incremental
smoothing."""
import numpy as np

from matrix_engine.indicators import rsi


def test_rsi_recursive_smoothing_loop_actually_executes():
    """With more than 15 closes, the recursive smoothing loop must run at
    least once -- confirms the previous `range(14, 14)` dead-code bug is
    gone. Seeds two series that are IDENTICAL for their last 15 bars but
    different before that; genuine Wilder smoothing (which remembers
    everything) must produce different RSI values, while the old buggy
    version (blind to anything before the last 15 bars) would have
    returned the exact same value for both."""
    last_15_shared = [110.0, 108.0, 112.0, 109.0, 111.0, 107.0, 113.0,
                       106.0, 114.0, 105.0, 115.0, 104.0, 116.0, 103.0, 117.0]
    series_a = [100.0] * 10 + last_15_shared        # flat history before
    series_b = list(range(50, 60)) + last_15_shared  # rising history before

    rsi_a = rsi(np.array(series_a, dtype=np.float64))
    rsi_b = rsi(np.array(series_b, dtype=np.float64))

    assert rsi_a != rsi_b, (
        "genuine Wilder smoothing must be sensitive to history before the "
        "last 15 bars -- identical results here would mean the truncation "
        "bug is still present"
    )


def test_rsi_returns_50_when_insufficient_data():
    """Baseline, unchanged: fewer than RSI_PERIOD+1=15 closes -> 50.0."""
    assert rsi(np.array([1.0, 2.0, 3.0])) == 50.0


def test_rsi_matches_hand_computed_wilder_value_on_a_known_series():
    """A simple, hand-verifiable series: 15 closes rising by exactly 1.0
    each bar (14 deltas, all +1.0, zero losses) -- Wilder RSI must be
    exactly 100.0 (avg_loss=0)."""
    closes = np.array([float(100 + i) for i in range(15)])
    assert rsi(closes) == 100.0
