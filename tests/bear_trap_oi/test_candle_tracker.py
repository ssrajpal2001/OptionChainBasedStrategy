from datetime import datetime, timezone
from strategies.bear_trap_oi.candle_tracker import BarAccumulator


def _ts(minute, second=0):
    return datetime(2026, 10, 1, 9, minute, second, tzinfo=timezone.utc)


def test_ticks_within_one_bucket_produce_no_completed_bar_yet():
    acc = BarAccumulator(bucket_minutes=5)
    assert acc.on_tick(_ts(15, 0), 100.0) is None
    assert acc.on_tick(_ts(16, 30), 105.0) is None
    assert acc.on_tick(_ts(19, 59), 98.0) is None


def test_crossing_into_a_new_bucket_emits_the_completed_prior_bar():
    acc = BarAccumulator(bucket_minutes=5)
    acc.on_tick(_ts(15, 0), 100.0)
    acc.on_tick(_ts(16, 30), 105.0)
    acc.on_tick(_ts(19, 59), 98.0)
    completed = acc.on_tick(_ts(20, 0), 102.0)
    assert completed is not None
    assert completed.open == 100.0
    assert completed.high == 105.0
    assert completed.low == 98.0
    assert completed.close == 98.0


def test_current_partial_reflects_the_in_progress_bar():
    acc = BarAccumulator(bucket_minutes=5)
    acc.on_tick(_ts(15, 0), 100.0)
    acc.on_tick(_ts(16, 30), 93.0)
    partial = acc.current_partial()
    assert partial.low == 93.0
    assert partial.close == 93.0
