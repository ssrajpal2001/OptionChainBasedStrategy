"""
tests/oi_bias_breakout/test_detector.py -- locks in every frozen decision
from the "9:25 AM OI + Price Action" spec (2026-09-25) against the real
pure functions in strategies/oi_bias_breakout/detector.py.
"""
from datetime import datetime, timedelta

import pytest

from strategies.core.trap_zone_utils import Bar
from strategies.oi_bias_breakout import detector as d


def _bar(ts: str, o: float, h: float, l: float, c: float) -> Bar:
    return Bar(ts=datetime.fromisoformat(ts), open=o, high=h, low=l, close=c)


# ── Step 2: signal-strike freeze ────────────────────────────────────────────

def test_freeze_signal_strikes_matches_the_spec_worked_example():
    # 9:15 open = 1247, step = 10 -> ATM 1250, OTM Call 1260, OTM Put 1240
    strikes = d.freeze_signal_strikes(open_915_price=1247, strike_step=10)
    assert strikes.atm == 1250
    assert strikes.otm_call == 1260
    assert strikes.otm_put == 1240


def test_freeze_signal_strikes_rejects_non_positive_step():
    with pytest.raises(ValueError):
        d.freeze_signal_strikes(open_915_price=1000, strike_step=0)


def test_resolve_entry_atm_is_independent_of_the_915_freeze():
    # Same stock, same step, but a materially different price by the time
    # Entry 1 actually fires -- must NOT reuse the 9:15-derived ATM.
    entry1_atm = d.resolve_entry_atm(spot_price=1289, strike_step=10)
    entry2_atm = d.resolve_entry_atm(spot_price=1312, strike_step=10)
    assert entry1_atm == 1290
    assert entry2_atm == 1310
    assert entry1_atm != entry2_atm


# ── Step 5: directional OI bias ─────────────────────────────────────────────

def test_classify_oi_bias_bullish():
    bias = d.classify_oi_bias(
        otm_call_oi_920=1000, otm_call_oi_925=800,   # falling
        atm_put_oi_920=500, atm_put_oi_925=700,       # rising
        otm_put_oi_920=300, otm_put_oi_925=300,       # unchanged, irrelevant here
        atm_call_oi_920=400, atm_call_oi_925=400,     # unchanged, irrelevant here
    )
    assert bias == d.BIAS_BULLISH


def test_classify_oi_bias_bearish():
    bias = d.classify_oi_bias(
        otm_call_oi_920=1000, otm_call_oi_925=1000,
        atm_put_oi_920=500, atm_put_oi_925=500,
        otm_put_oi_920=900, otm_put_oi_925=600,       # falling
        atm_call_oi_920=200, atm_call_oi_925=350,     # rising
    )
    assert bias == d.BIAS_BEARISH


def test_classify_oi_bias_conflict_when_both_true_at_once():
    bias = d.classify_oi_bias(
        otm_call_oi_920=1000, otm_call_oi_925=800,    # falling -> bullish leg 1
        atm_put_oi_920=500, atm_put_oi_925=700,        # rising -> bullish leg 2
        otm_put_oi_920=900, otm_put_oi_925=600,        # falling -> bearish leg 1
        atm_call_oi_920=200, atm_call_oi_925=350,      # rising -> bearish leg 2
    )
    assert bias == d.BIAS_CONFLICT


def test_classify_oi_bias_none_when_neither_condition_holds():
    bias = d.classify_oi_bias(
        otm_call_oi_920=1000, otm_call_oi_925=1000,
        atm_put_oi_920=500, atm_put_oi_925=500,
        otm_put_oi_920=900, otm_put_oi_925=900,
        atm_call_oi_920=200, atm_call_oi_925=200,
    )
    assert bias == d.BIAS_NONE


def test_classify_oi_bias_missing_reading_never_raises_and_reads_as_no_signal():
    # Real incident: a thin single-stock option can have no real trade/OI
    # update at exactly 9:20 or 9:25 -- must degrade to BIAS_NONE, not crash.
    bias = d.classify_oi_bias(
        otm_call_oi_920=1000, otm_call_oi_925=None,
        atm_put_oi_920=500, atm_put_oi_925=700,
        otm_put_oi_920=300, otm_put_oi_925=300,
        atm_call_oi_920=400, atm_call_oi_925=400,
    )
    assert bias == d.BIAS_NONE


def test_classify_oi_bias_unchanged_oi_is_not_a_rise_or_a_fall():
    # OTM Call OI unchanged (not falling) -> bullish condition must NOT hold
    # even though ATM Put OI genuinely rose.
    bias = d.classify_oi_bias(
        otm_call_oi_920=1000, otm_call_oi_925=1000,
        atm_put_oi_920=500, atm_put_oi_925=700,
        otm_put_oi_920=300, otm_put_oi_925=300,
        atm_call_oi_920=400, atm_call_oi_925=400,
    )
    assert bias == d.BIAS_NONE


# ── Step 7: Entry 1 trigger (09:15-09:30 opening range, close-through-only) ─

def _opening_range_bars():
    """15 real 1-min candles spanning 09:15-09:29 -- the full opening range.
    Highest high = 108 (09:22), lowest low = 95 (09:27)."""
    return [
        _bar("2026-09-25T09:15:00", 100, 103, 98, 101),
        _bar("2026-09-25T09:16:00", 101, 104, 99, 102),
        _bar("2026-09-25T09:17:00", 102, 105, 100, 103),
        _bar("2026-09-25T09:18:00", 103, 106, 101, 104),
        _bar("2026-09-25T09:19:00", 104, 107, 102, 105),
        _bar("2026-09-25T09:20:00", 105, 106, 101, 103),
        _bar("2026-09-25T09:21:00", 103, 105, 100, 102),
        _bar("2026-09-25T09:22:00", 102, 108, 100, 105),   # sets the range HIGH (108)
        _bar("2026-09-25T09:23:00", 105, 106, 99, 101),
        _bar("2026-09-25T09:24:00", 101, 103, 98, 100),
        _bar("2026-09-25T09:25:00", 100, 102, 97, 99),
        _bar("2026-09-25T09:26:00", 99, 101, 96, 98),
        _bar("2026-09-25T09:27:00", 98, 100, 95, 97),       # sets the range LOW (95)
        _bar("2026-09-25T09:28:00", 97, 99, 96, 98),
        _bar("2026-09-25T09:29:00", 98, 100, 97, 99),
    ]


def test_opening_range_high_low_scans_the_full_window():
    or_high, or_low = d.opening_range_high_low(_opening_range_bars())
    assert or_high == 108
    assert or_low == 95


def test_opening_range_high_low_empty_bars_returns_none_none():
    assert d.opening_range_high_low([]) == (None, None)


def test_find_entry1_trigger_bullish_fires_on_a_genuine_close_above_the_range_high():
    or_high, or_low = 108, 95
    later = [
        _bar("2026-09-25T09:30:00", 106, 109, 105, 107),   # high 109 > 108 but CLOSE 107 < 108 -- wick only
        _bar("2026-09-25T09:31:00", 107, 112, 106, 110),   # close 110 > 108 -- genuine trigger
    ]
    trigger = d.find_entry1_trigger(or_high, or_low, later, d.BIAS_BULLISH)
    assert trigger is not None
    assert trigger.ts == datetime.fromisoformat("2026-09-25T09:31:00")


def test_find_entry1_trigger_bearish_fires_on_a_genuine_close_below_the_range_low():
    or_high, or_low = 108, 95
    later = [
        _bar("2026-09-25T09:30:00", 97, 98, 93, 96),   # low 93 < 95 but close 96 > 95 -- wick only
        _bar("2026-09-25T09:31:00", 96, 97, 90, 92),   # close 92 < 95 -- genuine trigger
    ]
    trigger = d.find_entry1_trigger(or_high, or_low, later, d.BIAS_BEARISH)
    assert trigger is not None
    assert trigger.ts == datetime.fromisoformat("2026-09-25T09:31:00")


def test_find_entry1_trigger_returns_none_when_it_has_not_fired_yet():
    or_high, or_low = 108, 95
    later = [_bar("2026-09-25T09:30:00", 100, 104, 99, 101)]
    assert d.find_entry1_trigger(or_high, or_low, later, d.BIAS_BULLISH) is None


def test_find_entry1_trigger_returns_none_for_conflict_or_no_signal():
    or_high, or_low = 108, 95
    later = [_bar("2026-09-25T09:30:00", 102, 200, 101, 150)]
    assert d.find_entry1_trigger(or_high, or_low, later, d.BIAS_CONFLICT) is None
    assert d.find_entry1_trigger(or_high, or_low, later, d.BIAS_NONE) is None


def test_find_entry1_trigger_returns_none_when_range_is_incomplete():
    later = [_bar("2026-09-25T09:30:00", 102, 200, 101, 150)]
    assert d.find_entry1_trigger(None, None, later, d.BIAS_BULLISH) is None


# ── Step 8: VWAP retest (1-min candle shape) ────────────────────────────────

def test_check_vwap_retest_bullish_open_above_low_dips_below():
    candle = _bar("2026-09-25T09:40:00", 101, 102, 98, 100)  # open>vwap, low<vwap
    assert d.check_vwap_retest(candle, vwap=100, bias=d.BIAS_BULLISH) is True


def test_check_vwap_retest_bullish_false_when_low_never_dips_below():
    candle = _bar("2026-09-25T09:40:00", 101, 102, 100.5, 101.5)
    assert d.check_vwap_retest(candle, vwap=100, bias=d.BIAS_BULLISH) is False


def test_check_vwap_retest_bearish_open_below_high_pokes_above():
    candle = _bar("2026-09-25T09:40:00", 99, 102, 98, 100.5)  # open<vwap, high>vwap
    assert d.check_vwap_retest(candle, vwap=100, bias=d.BIAS_BEARISH) is True


def test_check_vwap_retest_false_for_non_positive_vwap():
    candle = _bar("2026-09-25T09:40:00", 101, 102, 98, 100)
    assert d.check_vwap_retest(candle, vwap=0, bias=d.BIAS_BULLISH) is False


# ── Step 9a: 20-min VWAP close exit (close only, wick never counts) ────────

def test_check_vwap_close_exit_bullish_fires_only_on_a_real_close_below():
    wick_only = _bar("2026-09-25T10:00:00", 105, 106, 99, 101)  # low<vwap but close>vwap
    real_close = _bar("2026-09-25T10:20:00", 101, 102, 97, 98)   # close<vwap
    assert d.check_vwap_close_exit(wick_only, vwap=100, bias=d.BIAS_BULLISH) is False
    assert d.check_vwap_close_exit(real_close, vwap=100, bias=d.BIAS_BULLISH) is True


def test_check_vwap_close_exit_bearish_mirrors_bullish():
    real_close = _bar("2026-09-25T10:20:00", 99, 103, 98, 102)   # close>vwap
    assert d.check_vwap_close_exit(real_close, vwap=100, bias=d.BIAS_BEARISH) is True


# ── Step 9b: 60-min stagnation, anchored at Entry 1, never reset ──────────

def test_check_stagnation_exit_fires_once_60_minutes_pass_with_no_new_high():
    # Peak at 10:05 (per the spec's own worked example), no new high until 11:05.
    bars = [
        _bar("2026-09-25T10:00:00", 100, 101, 99, 100),
        _bar("2026-09-25T10:05:00", 100, 110, 100, 108),   # the real peak
        _bar("2026-09-25T10:30:00", 108, 108, 105, 106),
        _bar("2026-09-25T11:04:00", 106, 109, 105, 107),   # still no new high (109 < 110)
        _bar("2026-09-25T11:05:00", 107, 109, 105, 107),   # exactly 60 min since 10:05 peak
    ]
    assert d.check_stagnation_exit(bars, d.BIAS_BULLISH, stagnation_minutes=60.0) is True


def test_check_stagnation_exit_does_not_fire_before_60_minutes():
    bars = [
        _bar("2026-09-25T10:00:00", 100, 101, 99, 100),
        _bar("2026-09-25T10:05:00", 100, 110, 100, 108),
        _bar("2026-09-25T10:59:00", 108, 109, 105, 106),   # 54 min since the peak
    ]
    assert d.check_stagnation_exit(bars, d.BIAS_BULLISH, stagnation_minutes=60.0) is False


def test_check_stagnation_exit_resets_on_a_genuine_new_high():
    bars = [
        _bar("2026-09-25T10:00:00", 100, 101, 99, 100),
        _bar("2026-09-25T10:05:00", 100, 110, 100, 108),
        _bar("2026-09-25T10:50:00", 108, 115, 107, 112),   # genuine new high -- clock restarts here
        _bar("2026-09-25T11:30:00", 112, 113, 110, 111),   # only 40 min since the 10:50 high
    ]
    assert d.check_stagnation_exit(bars, d.BIAS_BULLISH, stagnation_minutes=60.0) is False


def test_check_stagnation_exit_bearish_uses_running_low():
    bars = [
        _bar("2026-09-25T10:00:00", 100, 101, 99, 100),
        _bar("2026-09-25T10:05:00", 100, 100, 90, 92),     # the real trough
        _bar("2026-09-25T11:05:00", 92, 95, 91, 93),       # 60 min since trough, no new low
    ]
    assert d.check_stagnation_exit(bars, d.BIAS_BEARISH, stagnation_minutes=60.0) is True


def test_check_stagnation_exit_empty_bars_is_a_safe_false():
    assert d.check_stagnation_exit([], d.BIAS_BULLISH) is False


# ── Step 9c: 75-min bull-trap target (zone-touch only) ─────────────────────

def test_check_trap_target_hit_true_inside_a_zone():
    zones = [{"zone_lo": 45.0, "zone_hi": 53.0}]
    assert d.check_trap_target_hit(zones, current_price=48.0) is True


def test_check_trap_target_hit_false_outside_every_zone():
    zones = [{"zone_lo": 45.0, "zone_hi": 53.0}]
    assert d.check_trap_target_hit(zones, current_price=60.0) is False


def test_check_trap_target_hit_checks_all_zones():
    zones = [{"zone_lo": 10.0, "zone_hi": 20.0}, {"zone_lo": 45.0, "zone_hi": 53.0}]
    assert d.check_trap_target_hit(zones, current_price=50.0) is True


def test_check_trap_target_hit_empty_zones_is_a_safe_false():
    assert d.check_trap_target_hit([], current_price=50.0) is False
