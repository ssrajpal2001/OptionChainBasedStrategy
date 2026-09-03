from dataclasses import dataclass
from datetime import datetime, timedelta

import pytest

from strategies.fvg.detector import (
    SwingPoint,
    consequent_encroachment,
    detect_fvg,
    detect_liquidity_sweep,
    detect_mss,
    find_swing_points,
    group_equal_levels,
    tag_high_liquidity,
    update_fvg_state,
)


@dataclass(frozen=True)
class Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float


_T0 = datetime(2026, 8, 3, 9, 15)


def bars(rows, step_minutes=15):
    """rows: list of (open, high, low, close) tuples -> list[Bar], 15m apart."""
    return [
        Bar(_T0 + timedelta(minutes=step_minutes * i), o, h, l, c)
        for i, (o, h, l, c) in enumerate(rows)
    ]


# ── detect_fvg ───────────────────────────────────────────────────────────────

def test_bullish_fvg_detected():
    b = bars([
        (100, 105, 98, 104),     # candle1: high=105
        (104, 130, 103, 128),    # candle2: strong displacement up, body_ratio high
        (128, 135, 120, 130),    # candle3: low=120 > candle1.high=105 -> gap [105,120]
    ])
    found = detect_fvg(b)
    assert len(found) == 1
    fvg = found[0]
    assert fvg["direction"] == "BULLISH"
    assert fvg["zone_lo"] == 105
    assert fvg["zone_hi"] == 120
    assert fvg["ce"] == pytest.approx(112.5)
    assert fvg["state"] == "UNMITIGATED"
    assert fvg["high_liquidity"] is False


def test_bearish_fvg_detected():
    b = bars([
        (200, 202, 195, 196),    # candle1: low=195
        (196, 197, 165, 168),    # candle2: strong displacement down
        (168, 180, 160, 175),    # candle3: high=180 < candle1.low=195 -> gap [180,195]
    ])
    found = detect_fvg(b)
    assert len(found) == 1
    fvg = found[0]
    assert fvg["direction"] == "BEARISH"
    assert fvg["zone_lo"] == 180
    assert fvg["zone_hi"] == 195
    assert fvg["ce"] == pytest.approx(187.5)


def test_no_fvg_when_candles_overlap():
    b = bars([
        (100, 110, 98, 105),
        (105, 112, 100, 108),
        (108, 111, 102, 109),   # candle3.low=102 < candle1.high=110 -> no gap
    ])
    assert detect_fvg(b) == []


def test_no_fvg_without_displacement_candle():
    # gap exists geometrically but candle2's body is tiny (doji) -> filtered
    b = bars([
        (100, 105, 98, 104),
        (110, 111, 109, 110),   # body_ratio = |110-110|... small range, tiny body
        (110, 135, 120, 130),
    ])
    found = detect_fvg(b, min_body_ratio=0.9)
    assert found == []


# ── consequent_encroachment ─────────────────────────────────────────────────

def test_consequent_encroachment_midpoint():
    assert consequent_encroachment(100, 200) == 150
    assert consequent_encroachment(0, 10) == 5


# ── swing points / equal levels ─────────────────────────────────────────────

def test_find_swing_points_high_and_low():
    b = bars([
        (100, 101, 99, 100),
        (100, 103, 99, 102),
        (102, 110, 101, 108),   # swing high candidate at index 2 (high=110)
        (108, 109, 103, 105),
        (105, 106, 104, 105),
        (105, 106, 90, 92),     # swing low candidate at index 5 (low=90)
        (92, 95, 91, 93),
        (93, 96, 92, 94),
    ])
    points = find_swing_points(b, pivot=2)
    kinds = {(p.kind, p.index) for p in points}
    assert ("HIGH", 2) in kinds
    assert ("LOW", 5) in kinds


def test_group_equal_levels_clusters_within_tolerance():
    pts = [
        SwingPoint("HIGH", 24500.0, _T0, 0),
        SwingPoint("HIGH", 24503.0, _T0, 5),   # within 5pt tolerance -> same pool
        SwingPoint("HIGH", 24600.0, _T0, 10),  # far away -> lone point, not a pool (<2 members)
        SwingPoint("LOW", 24000.0, _T0, 15),
    ]
    highs = group_equal_levels(pts, "HIGH", tolerance_pts=5.0)
    assert highs == [pytest.approx(24501.5)]
    lows = group_equal_levels(pts, "LOW", tolerance_pts=5.0)
    assert lows == []


# ── liquidity sweep ──────────────────────────────────────────────────────────

def test_detect_liquidity_sweep_low_bullish():
    b = bars([
        (100, 101, 99, 100),
        (100, 101, 95, 96),     # wicks below 97 but closes below too -> not a sweep
        (96, 98, 94, 99.5),     # wicks below 97, closes above 97 -> bullish sweep
    ])
    sweep = detect_liquidity_sweep(b, level=97.0, kind="LOW")
    assert sweep is not None
    assert sweep.index == 2
    assert sweep.kind == "LOW"


def test_detect_liquidity_sweep_none_when_never_swept():
    b = bars([(100, 101, 99, 100), (100, 102, 99, 101)])
    assert detect_liquidity_sweep(b, level=50.0, kind="LOW") is None


# ── MSS ──────────────────────────────────────────────────────────────────────

def test_detect_mss_bullish_breaks_swing_high():
    b = bars([
        (100, 101, 99, 100),
        (100, 103, 99, 102),
        (102, 110, 101, 108),   # swing high at index 2, price=110
        (108, 109, 103, 105),
        (105, 106, 104, 105),
        (105, 112, 104, 111),   # closes 111 > 110 -> MSS confirmed here
    ])
    points = find_swing_points(b, pivot=2)
    mss = detect_mss(b, points, "BULLISH")
    assert mss is not None
    assert mss.price == 110


def test_detect_mss_none_when_not_broken():
    b = bars([
        (100, 101, 99, 100),
        (100, 103, 99, 102),
        (102, 110, 101, 108),
        (108, 109, 103, 105),
        (105, 106, 104, 105),
        (105, 106, 104, 105),
    ])
    points = find_swing_points(b, pivot=2)
    assert detect_mss(b, points, "BULLISH") is None


# ── state machine (void-lift / retest) ──────────────────────────────────────

def _fvg(direction, zone_lo, zone_hi, candle3_ts):
    return dict(direction=direction, zone_lo=zone_lo, zone_hi=zone_hi,
                ce=consequent_encroachment(zone_lo, zone_hi),
                candle1_ts=candle3_ts - timedelta(minutes=30), candle3_ts=candle3_ts,
                candle1_low=0, candle1_high=0, index=2,
                state="UNMITIGATED", high_liquidity=False,
                mitigated_ts=None, invalidated_ts=None)


def test_bullish_fvg_partial_fill_then_mitigated():
    fvg = _fvg("BULLISH", zone_lo=105, zone_hi=120, candle3_ts=_T0)
    shallow = Bar(_T0 + timedelta(minutes=15), 122, 123, 118, 121)  # touches top(120), not CE(112.5)
    update_fvg_state(fvg, shallow)
    assert fvg["state"] == "PARTIALLY_FILLED"

    deep = Bar(_T0 + timedelta(minutes=30), 118, 119, 108, 115)  # reaches CE (112.5)
    update_fvg_state(fvg, deep)
    assert fvg["state"] == "MITIGATED"
    assert fvg["mitigated_ts"] == deep.timestamp


def test_bullish_fvg_gap_through_is_invalidated_not_retest():
    fvg = _fvg("BULLISH", zone_lo=105, zone_hi=120, candle3_ts=_T0)
    gap_through = Bar(_T0 + timedelta(minutes=15), 118, 119, 100, 102)  # closes below zone_lo(105)
    update_fvg_state(fvg, gap_through)
    assert fvg["state"] == "INVALIDATED"
    assert fvg["invalidated_ts"] == gap_through.timestamp


def test_bearish_fvg_mitigated_on_ce_touch():
    fvg = _fvg("BEARISH", zone_lo=180, zone_hi=195, candle3_ts=_T0)
    deep = Bar(_T0 + timedelta(minutes=15), 183, 190, 182, 185)  # high=190 >= CE(187.5)
    update_fvg_state(fvg, deep)
    assert fvg["state"] == "MITIGATED"


def test_bearish_fvg_gap_through_is_invalidated():
    fvg = _fvg("BEARISH", zone_lo=180, zone_hi=195, candle3_ts=_T0)
    gap_through = Bar(_T0 + timedelta(minutes=15), 190, 198, 189, 197)  # closes above zone_hi(195)
    update_fvg_state(fvg, gap_through)
    assert fvg["state"] == "INVALIDATED"


def test_terminal_states_are_frozen():
    fvg = _fvg("BULLISH", zone_lo=105, zone_hi=120, candle3_ts=_T0)
    fvg["state"] = "INVALIDATED"
    later = Bar(_T0 + timedelta(minutes=15), 110, 121, 106, 119)
    update_fvg_state(fvg, later)
    assert fvg["state"] == "INVALIDATED"


def test_bar_at_or_before_candle3_is_ignored():
    fvg = _fvg("BULLISH", zone_lo=105, zone_hi=120, candle3_ts=_T0)
    same_bar = Bar(_T0, 110, 111, 104, 106)  # would invalidate, but timestamp <= candle3_ts
    update_fvg_state(fvg, same_bar)
    assert fvg["state"] == "UNMITIGATED"


# ── high-liquidity tagging ───────────────────────────────────────────────────

def test_tag_high_liquidity_true_with_sweep_and_mss():
    # HTF bars: sweep of PDL=95 at index 2, then MSS (bullish) breaking swing high 110 at index 5
    htf = bars([
        (100, 101, 99, 100),
        (100, 103, 99, 102),
        (102, 104, 93, 103),     # sweeps PDL=95 (low=93, close=103>95)
        (103, 110, 102, 108),    # swing high candidate (needs pivot neighbors)
        (108, 109, 103, 105),
        (105, 106, 104, 105),
        (105, 112, 104, 111),    # MSS: closes above swing high 110
    ], step_minutes=15)
    points = find_swing_points(htf, pivot=2)

    fvg_candle1_ts = htf[-1].timestamp + timedelta(minutes=30)
    fvg = dict(direction="BULLISH", zone_lo=112, zone_hi=118,
               ce=115, candle1_ts=fvg_candle1_ts, candle3_ts=fvg_candle1_ts,
               candle1_low=0, candle1_high=0, index=99,
               state="UNMITIGATED", high_liquidity=False,
               mitigated_ts=None, invalidated_ts=None)

    tagged = tag_high_liquidity(fvg, htf, points, pdl=95.0)
    assert tagged["high_liquidity"] is True


def test_tag_high_liquidity_false_without_sweep():
    htf = bars([
        (100, 101, 99, 100),
        (100, 103, 99, 102),
        (102, 104, 101, 103),
        (103, 110, 102, 108),
        (108, 109, 103, 105),
        (105, 106, 104, 105),
        (105, 112, 104, 111),    # MSS happens, but no PDL sweep occurred
    ], step_minutes=15)
    points = find_swing_points(htf, pivot=2)
    fvg_candle1_ts = htf[-1].timestamp + timedelta(minutes=30)
    fvg = dict(direction="BULLISH", zone_lo=112, zone_hi=118,
               ce=115, candle1_ts=fvg_candle1_ts, candle3_ts=fvg_candle1_ts,
               candle1_low=0, candle1_high=0, index=99,
               state="UNMITIGATED", high_liquidity=False,
               mitigated_ts=None, invalidated_ts=None)

    tagged = tag_high_liquidity(fvg, htf, points, pdl=95.0)
    assert tagged["high_liquidity"] is False
