"""
2026-08-12: unit tests for strategies/oi_flow/detector.py -- hand-built bar
sequences and hand-injected OIFlowTracker/ChainSnapshot state, mirroring
tests/strategies/test_sr_ping_pong_confluence_filters.py's injection style.
No historical data, no network -- this strategy cannot be backtested (see
plan doc), so correctness here rests entirely on precisely-verified
synthetic inputs.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta

import pytest

from config.global_config import IST
from strategies.oi_flow.detector import (
    Bar, BarAccumulator, SwingPoint, find_swing_points, has_recent_structure_break,
    swing_low, swing_high, detect_pre_breakout_signal, confirm_option_price_action,
    detect_volume_spike,
)
from strategies.oi_flow.tracker import OIFlowTracker


def _bar(ts, o, h, l, c) -> Bar:
    return Bar(timestamp=ts, open=o, high=h, low=l, close=c)


def _base():
    return datetime(2026, 8, 12, 9, 20, tzinfo=IST)


# ── BarAccumulator ────────────────────────────────────────────────────────────

def test_bar_accumulator_buckets_ticks_and_reports_bar_close():
    base = _base()
    acc = BarAccumulator(timeframe_min=1)
    assert acc.on_tick(base, 100.0) is False
    assert acc.on_tick(base + timedelta(seconds=30), 102.0) is False
    closed = acc.on_tick(base + timedelta(minutes=1), 99.0)
    assert closed is True
    assert len(acc.bars) == 1
    assert acc.bars[0].open == 100.0
    assert acc.bars[0].high == 102.0
    assert acc.bars[0].close == 102.0   # last tick BEFORE the new bucket started


def test_bar_accumulator_tracks_per_bar_volume_from_cumulative_session_volume():
    """Upstox/Fyers report cumulative SESSION volume on every tick -- a
    bar's own volume must be the delta between its first and last tick's
    cumulative reading, not the raw cumulative number itself."""
    base = _base()
    acc = BarAccumulator(timeframe_min=1)
    acc.on_tick(base, 100.0, volume=10_000)
    acc.on_tick(base + timedelta(seconds=30), 102.0, volume=10_400)
    closed = acc.on_tick(base + timedelta(minutes=1), 99.0, volume=10_900)   # closes bar 1, opens bar 2
    assert closed is True
    assert acc.bars[0].volume == 400   # 10_400 - 10_000 (last tick INSIDE bar 1 vs. its own open)
    acc.on_tick(base + timedelta(minutes=1, seconds=30), 101.0, volume=11_200)
    assert acc.all_bars()[-1].volume == 300   # 11_200 - 10_900, bar 2's own delta so far


def test_bar_accumulator_volume_stays_zero_when_no_volume_threaded_in():
    """Spot IndexTick volume is not meaningful for an index -- callers that
    never pass `volume` (the spot accumulator) must not crash and must
    just leave every bar's volume at its 0.0 default."""
    base = _base()
    acc = BarAccumulator(timeframe_min=1)
    acc.on_tick(base, 100.0)
    acc.on_tick(base + timedelta(minutes=1), 99.0)
    assert acc.bars[0].volume == 0.0


# ── detect_volume_spike ───────────────────────────────────────────────────────

def test_detect_volume_spike_none_on_fewer_than_two_bars():
    base = _base()
    assert detect_volume_spike([_bar(base, 500, 502, 498, 500)]) is None
    assert detect_volume_spike([]) is None


def test_detect_volume_spike_none_when_no_volume_data_threaded_in():
    """Bars built without volume (the 0.0 default) must read as 'no data'
    (None), never as a false 'confirmed no spike' (False)."""
    base = _base()
    bars = [Bar(base + timedelta(minutes=i), 500, 502, 498, 500) for i in range(5)]
    assert detect_volume_spike(bars) is None


def test_detect_volume_spike_true_when_current_bar_well_above_trailing_average():
    base = _base()
    bars = [Bar(base + timedelta(minutes=i), 500, 502, 498, 500, volume=1000.0) for i in range(20)]
    bars.append(Bar(base + timedelta(minutes=20), 500, 505, 498, 503, volume=2000.0))   # 2x avg
    info = detect_volume_spike(bars, lookback=20, spike_ratio=1.5)
    assert info is not None
    assert info.is_spike is True
    assert info.ratio == pytest.approx(2.0)
    assert info.avg_volume == pytest.approx(1000.0)


def test_detect_volume_spike_false_when_current_bar_in_line_with_average():
    base = _base()
    bars = [Bar(base + timedelta(minutes=i), 500, 502, 498, 500, volume=1000.0) for i in range(20)]
    bars.append(Bar(base + timedelta(minutes=20), 500, 502, 498, 500, volume=1050.0))   # ~normal
    info = detect_volume_spike(bars, lookback=20, spike_ratio=1.5)
    assert info is not None
    assert info.is_spike is False


# ── find_swing_points / has_recent_structure_break ───────────────────────────

def test_find_swing_points_detects_a_single_swing_high():
    base = _base()
    bars = [
        _bar(base, 8, 10, 5, 9),
        _bar(base + timedelta(minutes=1), 12, 15, 8, 14),   # the peak
        _bar(base + timedelta(minutes=2), 7, 9, 4, 6),
    ]
    swings = find_swing_points(bars, pivot=1)
    highs = [s for s in swings if s.kind == "HIGH"]
    assert len(highs) == 1
    assert highs[0].index == 1
    assert highs[0].price == 15


def test_find_swing_points_detects_a_single_swing_low():
    base = _base()
    bars = [
        _bar(base, 12, 15, 10, 13),
        _bar(base + timedelta(minutes=1), 6, 12, 4, 8),     # the trough
        _bar(base + timedelta(minutes=2), 14, 16, 9, 15),
    ]
    swings = find_swing_points(bars, pivot=1)
    lows = [s for s in swings if s.kind == "LOW"]
    assert len(lows) == 1
    assert lows[0].index == 1
    assert lows[0].price == 4


def test_has_recent_structure_break_true_when_a_later_close_breaks_the_swing_high():
    base = _base()
    bars = [
        _bar(base, 8, 10, 5, 8),
        _bar(base + timedelta(minutes=1), 12, 15, 8, 12),   # swing high @ 15
        _bar(base + timedelta(minutes=2), 6, 9, 4, 7),
        _bar(base + timedelta(minutes=3), 17, 20, 16, 18),  # closes (18) ABOVE the 15 swing high
    ]
    swings = find_swing_points(bars, pivot=1)
    assert has_recent_structure_break(bars, swings, "BULLISH") is True


def test_has_recent_structure_break_false_when_still_inside_the_range():
    base = _base()
    bars = [
        _bar(base, 8, 10, 5, 8),
        _bar(base + timedelta(minutes=1), 12, 15, 8, 12),   # swing high @ 15
        _bar(base + timedelta(minutes=2), 6, 9, 4, 7),
        _bar(base + timedelta(minutes=3), 13, 14, 11, 14),  # closes (14) still BELOW 15 -- no break
    ]
    swings = find_swing_points(bars, pivot=1)
    assert has_recent_structure_break(bars, swings, "BULLISH") is False


def test_swing_low_and_swing_high_return_the_most_recent_confirmed_value():
    base = _base()
    bars = [
        _bar(base, 12, 15, 10, 13),
        _bar(base + timedelta(minutes=1), 6, 12, 4, 8),     # swing low @ 4
        _bar(base + timedelta(minutes=2), 14, 16, 9, 15),
    ]
    assert swing_low(bars, pivot=1) == 4
    assert swing_high(bars, pivot=1) is None   # no confirmed swing high in this sequence


# ── detect_pre_breakout_signal ───────────────────────────────────────────────

class _FakeSnap:
    def __init__(self, max_call_oi_strike=57700.0, max_put_oi_strike=57200.0, pcr=1.3):
        self.max_call_oi_strike = max_call_oi_strike
        self.max_put_oi_strike = max_put_oi_strike
        self._pcr = pcr

    def pcr_smooth(self, n: int = 5) -> float:
        return self._pcr


@dataclass
class _FakeTick:
    strike: float
    option_type: str
    oi: int
    timestamp: datetime


def _flat_spot_bars(base, n=10, price=57690.0):
    """No pivot structure at all -- has_recent_structure_break trivially
    False, isolating the OI/PCR/proximity conditions in these tests."""
    return [_bar(base + timedelta(minutes=i), price, price, price, price) for i in range(n)]


def _tracker_with(base, wall, opposing_start, opposing_end, supporting_strike,
                   supporting_start, supporting_end, window_sec=180):
    tracker = OIFlowTracker(max_history_sec=600)
    tracker.watch_strikes({(wall, "CE"): True, (supporting_strike, "PE"): True})
    t0 = base
    t1 = base + timedelta(seconds=window_sec + 10)
    tracker.on_option_tick(_FakeTick(wall, "CE", opposing_start, t0))
    tracker.on_option_tick(_FakeTick(wall, "CE", opposing_end, t1))
    tracker.on_option_tick(_FakeTick(supporting_strike, "PE", supporting_start, t0))
    tracker.on_option_tick(_FakeTick(supporting_strike, "PE", supporting_end, t1))
    return tracker, t1


def test_ce_signal_fires_when_everything_aligns():
    base = _base()
    spot_bars = _flat_spot_bars(base, price=57690.0)
    tracker, now = _tracker_with(base, wall=57700.0, opposing_start=100_000, opposing_end=95_000,
                                  supporting_strike=57600.0, supporting_start=50_000, supporting_end=53_000)
    snap = _FakeSnap(max_call_oi_strike=57700.0, pcr=1.3)
    sig = detect_pre_breakout_signal("CE", tracker, snap, spot_bars, window_sec=180,
                                      strike_step=100.0, now=now)
    assert sig is not None
    assert sig.side == "CE"
    assert sig.wall_strike == 57700.0


def test_ce_signal_none_when_opposing_wall_oi_still_rising():
    base = _base()
    spot_bars = _flat_spot_bars(base, price=57690.0)
    # opposing (call wall) OI RISES instead of dropping -- writers still defending.
    tracker, now = _tracker_with(base, wall=57700.0, opposing_start=95_000, opposing_end=100_000,
                                  supporting_strike=57600.0, supporting_start=50_000, supporting_end=53_000)
    snap = _FakeSnap(max_call_oi_strike=57700.0, pcr=1.3)
    sig = detect_pre_breakout_signal("CE", tracker, snap, spot_bars, window_sec=180,
                                      strike_step=100.0, now=now)
    assert sig is None


def test_ce_signal_none_when_supporting_side_not_building():
    base = _base()
    spot_bars = _flat_spot_bars(base, price=57690.0)
    # supporting (put) OI stays flat -- no real floor forming underneath.
    tracker, now = _tracker_with(base, wall=57700.0, opposing_start=100_000, opposing_end=95_000,
                                  supporting_strike=57600.0, supporting_start=50_000, supporting_end=50_100)
    snap = _FakeSnap(max_call_oi_strike=57700.0, pcr=1.3)
    sig = detect_pre_breakout_signal("CE", tracker, snap, spot_bars, window_sec=180,
                                      strike_step=100.0, now=now)
    assert sig is None


def test_ce_signal_none_when_structure_already_broken():
    """The single most important negative test -- proves this is genuinely
    a PRE-breakout signal, not a relabeled post-breakout detector. Even
    with perfect OI conditions, an already-confirmed bullish structure
    break must block the signal."""
    base = _base()
    # A real swing high forms then gets closed through -- structure has broken.
    spot_bars = [
        _bar(base, 57650, 57660, 57630, 57650),
        _bar(base + timedelta(minutes=1), 57680, 57700, 57660, 57690),   # swing high @ 57700
        _bar(base + timedelta(minutes=2), 57640, 57660, 57610, 57630),
        _bar(base + timedelta(minutes=3), 57720, 57750, 57700, 57740),   # closes ABOVE 57700 -- broken
    ]
    tracker, now = _tracker_with(base, wall=57700.0, opposing_start=100_000, opposing_end=95_000,
                                  supporting_strike=57600.0, supporting_start=50_000, supporting_end=53_000)
    snap = _FakeSnap(max_call_oi_strike=57700.0, pcr=1.3)
    sig = detect_pre_breakout_signal("CE", tracker, snap, spot_bars, window_sec=180,
                                      strike_step=100.0, swing_pivot=1, now=now)
    assert sig is None


def test_ce_signal_none_on_insufficient_oi_history():
    base = _base()
    spot_bars = _flat_spot_bars(base, price=57690.0)
    tracker = OIFlowTracker(max_history_sec=600)
    tracker.watch_strikes({(57700.0, "CE"): True, (57600.0, "PE"): True})
    # Only ONE sample each -- oi_roc must return None (no window to anchor on).
    tracker.on_option_tick(_FakeTick(57700.0, "CE", 95_000, base))
    tracker.on_option_tick(_FakeTick(57600.0, "PE", 53_000, base))
    snap = _FakeSnap(max_call_oi_strike=57700.0, pcr=1.3)
    sig = detect_pre_breakout_signal("CE", tracker, snap, spot_bars, window_sec=180,
                                      strike_step=100.0, now=base)
    assert sig is None


def test_ce_signal_none_when_pcr_outside_bullish_band():
    base = _base()
    spot_bars = _flat_spot_bars(base, price=57690.0)
    tracker, now = _tracker_with(base, wall=57700.0, opposing_start=100_000, opposing_end=95_000,
                                  supporting_strike=57600.0, supporting_start=50_000, supporting_end=53_000)
    snap = _FakeSnap(max_call_oi_strike=57700.0, pcr=0.9)   # below the 1.2 CE threshold
    sig = detect_pre_breakout_signal("CE", tracker, snap, spot_bars, window_sec=180,
                                      strike_step=100.0, min_pcr_bias=1.2, now=now)
    assert sig is None


def test_ce_signal_none_when_spot_not_near_wall():
    base = _base()
    spot_bars = _flat_spot_bars(base, price=55000.0)   # far from the 57700 wall
    tracker, now = _tracker_with(base, wall=57700.0, opposing_start=100_000, opposing_end=95_000,
                                  supporting_strike=57600.0, supporting_start=50_000, supporting_end=53_000)
    snap = _FakeSnap(max_call_oi_strike=57700.0, pcr=1.3)
    sig = detect_pre_breakout_signal("CE", tracker, snap, spot_bars, window_sec=180,
                                      strike_step=100.0, now=now)
    assert sig is None


# ── confirm_option_price_action ──────────────────────────────────────────────

def test_option_confirmation_ok_above_vwap_no_rejection():
    base = _base()
    bars = [
        _bar(base, 512, 515, 510, 513),
        _bar(base + timedelta(minutes=1), 506, 512, 502, 508),   # dip -> confirmed swing low @ 502
        _bar(base + timedelta(minutes=2), 514, 520, 511, 518),
        _bar(base + timedelta(minutes=3), 519, 525, 516, 523),   # last bar: closes near its high
    ]
    conf = confirm_option_price_action(bars, "CE", lookback=4, swing_pivot=1)
    assert conf.ok is True
    assert conf.vwap is not None and conf.vwap < 523   # last close sits above the rolling VWAP
    assert conf.sl_level == 502


def test_option_confirmation_blocked_below_vwap():
    base = _base()
    # Flat-then-drop: last close well below the rolling VWAP.
    bars = [_bar(base + timedelta(minutes=i), 500, 505, 495, 500) for i in range(5)]
    bars.append(_bar(base + timedelta(minutes=5), 480, 482, 460, 462))   # sharp drop, closes low
    conf = confirm_option_price_action(bars, "CE", lookback=6, swing_pivot=1)
    assert conf.ok is False
    assert conf.reason == "below_vwap"


def test_option_confirmation_blocked_on_upper_wick_rejection_even_above_vwap():
    base = _base()
    bars = [_bar(base + timedelta(minutes=i), 500, 502, 498, 500) for i in range(4)]
    # Last candle closes (515) clearly above the ~503.6 rolling VWAP -- passes
    # that check -- but most of its range is an upper wick above the close
    # (high=540 vs close=515, range=41, upper_wick=25 -> ratio 0.61): rejection.
    bars.append(_bar(base + timedelta(minutes=4), 500, 540, 499, 515))
    conf = confirm_option_price_action(bars, "CE", lookback=5, swing_pivot=1)
    assert conf.ok is False
    assert conf.reason == "upper_wick_rejection"


def test_option_confirmation_sl_level_matches_real_swing_low():
    base = _base()
    bars = [
        _bar(base, 512, 515, 510, 513),
        _bar(base + timedelta(minutes=1), 506, 512, 502, 508),   # swing low @ 502
        _bar(base + timedelta(minutes=2), 514, 520, 511, 518),
        _bar(base + timedelta(minutes=3), 519, 525, 516, 523),
    ]
    conf = confirm_option_price_action(bars, "CE", lookback=4, swing_pivot=1)
    assert conf.ok is True
    assert conf.sl_level == 502


def test_option_confirmation_surfaces_volume_spike_without_blocking_ok():
    """A confirmed, otherwise-passing entry with an elevated final-bar
    volume must still pass (volume is a soft/logged dimension, not a
    fourth hard gate) while surfacing the spike for telemetry review."""
    base = _base()
    bars = [
        Bar(base, 512, 515, 510, 513, volume=1000.0),
        Bar(base + timedelta(minutes=1), 506, 512, 502, 508, volume=1000.0),   # swing low @ 502
        Bar(base + timedelta(minutes=2), 514, 520, 511, 518, volume=1000.0),
        Bar(base + timedelta(minutes=3), 519, 525, 516, 523, volume=2500.0),   # absorption spike
    ]
    conf = confirm_option_price_action(bars, "CE", lookback=4, swing_pivot=1)
    assert conf.ok is True
    assert conf.sl_level == 502
    assert conf.volume_spike is True
    assert conf.volume_ratio == pytest.approx(2.5)


def test_option_confirmation_volume_fields_populated_even_when_blocked():
    """A rejection (below_vwap here) must still carry the volume read --
    was there real absorption happening even on a setup we didn't take?"""
    base = _base()
    bars = [Bar(base + timedelta(minutes=i), 500, 505, 495, 500, volume=1000.0) for i in range(5)]
    bars.append(Bar(base + timedelta(minutes=5), 480, 482, 460, 462, volume=3000.0))
    conf = confirm_option_price_action(bars, "CE", lookback=6, swing_pivot=1)
    assert conf.ok is False
    assert conf.reason == "below_vwap"
    assert conf.volume_spike is True
    assert conf.volume_ratio == pytest.approx(3.0)
