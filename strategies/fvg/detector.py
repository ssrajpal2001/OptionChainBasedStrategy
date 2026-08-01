"""
strategies/fvg/detector.py — pure, unit-testable Fair Value Gap detection.

Operates on plain OHLC bar sequences (any object with .timestamp/.open/
.high/.low/.close — the same shape as bear_only_book.py's `_Bar`/`_to_bars`
output and pandas `itertuples()` rows), so these functions run identically
in the live engine (strategies/fvg/engine.py, fed CandleEvent-derived bars)
and in scripts/fvg_backtest.py (fed resampled historical bars).

Per direct user spec (2026-08-01): NO indicators (no RSI/VWAP/ADX/ATR) —
pure price action only. Detection runs on the underlying spot/index chart,
not option premium (matches D1TrapOptionBook's design, not
D1TrapBearOnlyBook's option-native one).

Concepts implemented:
  - Swing high/low points (fractal pivot) on HTF bars.
  - Liquidity sweep: a wick beyond a level (PDH/PDL or an equal-highs/lows
    pool) that closes back inside — the SMC "stop hunt" pattern.
  - Market Structure Shift (MSS): a later candle's CLOSE breaks the most
    recent opposing swing point.
  - Fair Value Gap (FVG): the classic 3-candle imbalance. Bullish:
    candle1.high < candle3.low. Bearish: candle1.low > candle3.high.
    candle2 must be a "displacement" candle (body >= _MIN_BODY_RATIO of its
    own high-low range) — pure price-action proxy for institutional
    displacement, not an indicator.
  - "High liquidity" tagging: an FVG is tagged high_liquidity=True only if
    it forms after a confirmed MSS in its own direction, itself preceded by
    a liquidity sweep — isolated/internal FVGs are recorded but left
    untagged so callers can filter them out.
  - FVG state machine: UNMITIGATED -> PARTIALLY_FILLED -> MITIGATED (price
    retraced at least to the 50% Consequent Encroachment level and can be
    traded) / INVALIDATED (price closed all the way through the FAR
    boundary instead of just retesting). The mitigation-touch check always
    runs BEFORE the invalidation-close check on a given bar so a gap-through
    candle can never be misread as a valid retest.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Protocol, Sequence


class Bar(Protocol):
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float


_SWING_PIVOT_BARS = 2                  # 2 bars either side -> 5-bar fractal pivot
_EQUAL_LEVEL_TOLERANCE_PTS = 5.0       # NIFTY-scale fixed-point tolerance for grouping
                                        # equal highs/lows into one liquidity pool
_MIN_DISPLACEMENT_BODY_RATIO = 0.5     # candle2's body must be >= 50% of its own H-L
                                        # range to count as a displacement candle


@dataclass(frozen=True)
class SwingPoint:
    kind: str          # "HIGH" | "LOW"
    price: float
    timestamp: datetime
    index: int


@dataclass(frozen=True)
class SweepEvent:
    level: float
    kind: str           # "LOW" (swept a low, bullish) | "HIGH" (swept a high, bearish)
    wick_price: float
    close_price: float
    timestamp: datetime
    index: int


# ── swing structure ─────────────────────────────────────────────────────────

def find_swing_points(bars: Sequence[Bar], pivot: int = _SWING_PIVOT_BARS) -> List[SwingPoint]:
    """Fractal-style swing high/low: bar i is a swing high if its high is
    strictly greater than every bar's high in the `pivot`-bar window on each
    side (swing low: strictly less than every neighbor's low). Ties record no
    swing point (conservative — avoids ambiguous double-tops/bottoms)."""
    n = len(bars)
    points: List[SwingPoint] = []
    for i in range(pivot, n - pivot):
        center = bars[i]
        neighbors = list(bars[i - pivot:i]) + list(bars[i + 1:i + pivot + 1])
        if all(center.high > b.high for b in neighbors):
            points.append(SwingPoint("HIGH", center.high, center.timestamp, i))
        if all(center.low < b.low for b in neighbors):
            points.append(SwingPoint("LOW", center.low, center.timestamp, i))
    return points


def group_equal_levels(swing_points: Sequence[SwingPoint], kind: str,
                        tolerance_pts: float = _EQUAL_LEVEL_TOLERANCE_PTS) -> List[float]:
    """Group swing points of `kind` ('HIGH'/'LOW') into equal-level clusters
    within tolerance_pts of each other. Returns the average level of each
    cluster with >=2 members — a genuine 'equal highs/lows' liquidity pool
    (a single, unrepeated swing point is not a liquidity pool)."""
    pts = sorted(p.price for p in swing_points if p.kind == kind)
    if not pts:
        return []
    clusters: List[List[float]] = [[pts[0]]]
    for p in pts[1:]:
        if p - clusters[-1][-1] <= tolerance_pts:
            clusters[-1].append(p)
        else:
            clusters.append([p])
    return [sum(c) / len(c) for c in clusters if len(c) >= 2]


# ── liquidity sweep + market structure shift ────────────────────────────────

def detect_liquidity_sweep(bars: Sequence[Bar], level: float, kind: str) -> Optional[SweepEvent]:
    """First bar in `bars` that sweeps `level`:
      kind='LOW':  bar.low < level and bar.close > level  -> bullish sweep
                   (liquidity resting below the level was taken, price closed
                   back above it — buyers regained control).
      kind='HIGH': bar.high > level and bar.close < level -> bearish sweep.
    Returns None if no sweep occurs anywhere in `bars`."""
    for i, bar in enumerate(bars):
        if kind == "LOW" and bar.low < level and bar.close > level:
            return SweepEvent(level, "LOW", bar.low, bar.close, bar.timestamp, i)
        if kind == "HIGH" and bar.high > level and bar.close < level:
            return SweepEvent(level, "HIGH", bar.high, bar.close, bar.timestamp, i)
    return None


def detect_mss(bars: Sequence[Bar], swing_points: Sequence[SwingPoint],
                direction: str) -> Optional[SwingPoint]:
    """direction='BULLISH': returns the most recent prior swing HIGH the
    first time a later bar's CLOSE breaks above it (confirms a bullish
    market structure shift). direction='BEARISH': most recent prior swing
    LOW, broken below on close. Returns None if no such swing point exists
    or it never gets broken within `bars`."""
    kind = "HIGH" if direction == "BULLISH" else "LOW"
    relevant = [p for p in swing_points if p.kind == kind]
    if not relevant:
        return None
    last_swing = relevant[-1]
    for bar in bars[last_swing.index + 1:]:
        if direction == "BULLISH" and bar.close > last_swing.price:
            return last_swing
        if direction == "BEARISH" and bar.close < last_swing.price:
            return last_swing
    return None


# ── FVG detection ────────────────────────────────────────────────────────────

def consequent_encroachment(zone_lo: float, zone_hi: float) -> float:
    """CE = the 50% equilibrium level of the gap."""
    return zone_lo + (zone_hi - zone_lo) / 2.0


def _body_ratio(bar: Bar) -> float:
    rng = bar.high - bar.low
    return abs(bar.close - bar.open) / rng if rng > 0 else 0.0


def detect_fvg(bars: Sequence[Bar], min_body_ratio: float = _MIN_DISPLACEMENT_BODY_RATIO,
                min_body_pts: float = 0.0) -> List[dict]:
    """Scan every 3-candle window for a Fair Value Gap. candle2 (the middle
    candle) must be a displacement candle: body >= min_body_ratio of its own
    H-L range (relative, pure price action) AND, if min_body_pts > 0, its
    absolute body size (|close-open|) must also exceed that many points —
    an optional stricter impulse filter (e.g. 15-20 NIFTY points) to reject
    micro-gaps formed during low-momentum chop that would otherwise pass
    the relative-ratio test on a quiet, low-range candle. Returns a list of
    FVG dicts, oldest first; each starts UNMITIGATED/high_liquidity=False."""
    out: List[dict] = []
    n = len(bars)
    for i in range(2, n):
        c1, c2, c3 = bars[i - 2], bars[i - 1], bars[i]
        if _body_ratio(c2) < min_body_ratio:
            continue
        if min_body_pts > 0 and abs(c2.close - c2.open) < min_body_pts:
            continue
        if c1.high < c3.low:
            out.append(_new_fvg("BULLISH", c1.high, c3.low, c1, c3, i))
        elif c1.low > c3.high:
            out.append(_new_fvg("BEARISH", c3.high, c1.low, c1, c3, i))
    return out


def _new_fvg(direction: str, zone_lo: float, zone_hi: float, c1: Bar, c3: Bar, index: int) -> dict:
    return dict(
        direction=direction,
        zone_lo=zone_lo, zone_hi=zone_hi,
        ce=consequent_encroachment(zone_lo, zone_hi),
        candle1_ts=c1.timestamp, candle3_ts=c3.timestamp,
        candle1_low=c1.low, candle1_high=c1.high,
        index=index,
        state="UNMITIGATED",
        high_liquidity=False,
        mitigated_ts=None,
        invalidated_ts=None,
    )


# ── FVG state machine (void-lift / retest logic) ────────────────────────────

def update_fvg_state(fvg: dict, bar: Bar) -> dict:
    """Advance one FVG's state given a new LTF bar (mutates and returns the
    same dict). Terminal states (MITIGATED/INVALIDATED) are frozen. The
    mitigation-touch condition is always checked BEFORE the invalidation-
    close condition, so a bar that gaps straight through the zone is
    correctly INVALIDATED rather than misread as a valid retest.

    Bullish FVG: touched  = bar.low  <= zone_hi (top)
                 closed_through = bar.close < zone_lo (bottom)
    Bearish FVG: touched  = bar.high >= zone_lo (bottom)
                 closed_through = bar.close > zone_hi (top)
    A touch that reaches the 50% CE level marks the gap MITIGATED (tradeable
    retest); a shallower touch is PARTIALLY_FILLED."""
    if fvg["state"] in ("MITIGATED", "INVALIDATED"):
        return fvg
    if bar.timestamp <= fvg["candle3_ts"]:
        return fvg

    if fvg["direction"] == "BULLISH":
        touched = bar.low <= fvg["zone_hi"]
        closed_through = bar.close < fvg["zone_lo"]
        reached_ce = bar.low <= fvg["ce"]
    else:
        touched = bar.high >= fvg["zone_lo"]
        closed_through = bar.close > fvg["zone_hi"]
        reached_ce = bar.high >= fvg["ce"]

    if touched and not closed_through:
        if reached_ce:
            fvg["state"] = "MITIGATED"
            fvg["mitigated_ts"] = bar.timestamp
        else:
            fvg["state"] = "PARTIALLY_FILLED"
    elif closed_through:
        fvg["state"] = "INVALIDATED"
        fvg["invalidated_ts"] = bar.timestamp
    return fvg


# ── high-liquidity tagging ───────────────────────────────────────────────────

def tag_high_liquidity(fvg: dict, htf_bars: Sequence[Bar], htf_swing_points: Sequence[SwingPoint],
                        pdh: Optional[float] = None, pdl: Optional[float] = None) -> dict:
    """Mark fvg['high_liquidity']=True only if, on HTF bars strictly before
    this FVG's candle1, there is BOTH a liquidity sweep (of PDH/PDL if given,
    else the nearest equal-highs/lows pool) AND a subsequent confirmed MSS in
    the FVG's own direction. Isolated FVGs with no such HTF confirmation stay
    high_liquidity=False (still returned, callers filter them out)."""
    direction = fvg["direction"]
    prior_htf = [b for b in htf_bars if b.timestamp < fvg["candle1_ts"]]
    if len(prior_htf) < 2 * _SWING_PIVOT_BARS + 1:
        return fvg

    mss = detect_mss(prior_htf, htf_swing_points, direction)
    if mss is None:
        return fvg

    sweep_kind = "LOW" if direction == "BULLISH" else "HIGH"
    sweep_level = pdl if direction == "BULLISH" else pdh
    sweep: Optional[SweepEvent] = None
    if sweep_level is not None:
        sweep = detect_liquidity_sweep(prior_htf, sweep_level, sweep_kind)
    if sweep is None:
        for lvl in group_equal_levels(htf_swing_points, sweep_kind):
            sweep = detect_liquidity_sweep(prior_htf, lvl, sweep_kind)
            if sweep is not None:
                break

    if sweep is not None and sweep.index <= mss.index:
        fvg["high_liquidity"] = True
    return fvg
