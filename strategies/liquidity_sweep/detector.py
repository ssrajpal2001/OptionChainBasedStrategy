"""
strategies/liquidity_sweep/detector.py — pure signal logic for the Liquidity
Sweep strategy (option buyer).

Fully standalone by explicit user direction, same mandate as
strategies/oi_flow/ (see strategies/liquidity_sweep/__init__.py): every
function here is a fresh, independent implementation. Bar/BarAccumulator/
SwingPoint/find_swing_points below are structurally similar to
strategies/oi_flow/detector.py's own versions -- that similarity is
deliberate (a proven, battle-tested shape), not a shared import.

This is a direct Python port of a Pine Script v5 indicator
(pinescript/liquidity_sweep_indicator_with_risk.pine) that was iteratively
built, tested, and tuned against real NIFTY 5-minute chart data across an
extended live session before being ported here -- every default below
matches that validated script's final tuned values, and every design
decision documented in this file's comments was arrived at from real
evidence (a funnel diagnostic showing exactly which stage was blocking
signals), not guessed. See CLAUDE.md's "Liquidity Sweep Strategy" section
for the full tuning history.

Pipeline, per LTF bar close (orchestrated by engine.py, which holds ALL the
cross-bar state -- pending sweep watch, awaiting-FVG, armed-retest -- since
that state is fundamentally bar-by-bar and stateful, same as the Pine
script itself; the functions below are deliberately pure/stateless, each
taking whatever state it needs as an explicit parameter):

  1. compute_rolling_base()      -- HTF candle-breakout reference level
     (only used when liq_source="rolling_base"; the validated default is
     "liquidity_pool", not this one -- kept for parity/comparison, same as
     the Pine script's own 3-way toggle).
  2. find_swing_points()         -- confirmed swing highs/lows (pivotLeft/
     pivotRight bars each side).
  3. latest_pool_level()         -- classic ICT equal-highs/equal-lows:
     requires 2+ confirmed swings clustering within a small tolerance
     before counting as real liquidity (the codebase's own FVG strategy's
     group_equal_levels concept, independently re-derived here).
  4. compute_market_structure()  -- BoS/CHoCH directional bias, replayed
     bar-by-bar (a real structural read, not a crude midpoint check).
  5. detect_sweep()              -- pierce the level's outer (wick) edge,
     close back past the inner (body) edge -- a genuine rejection, not
     just a marginal tick back under the exact wick tip.
  6. check_displacement()        -- body/ATR ratio + micro-swing break in
     the sweep's direction, within a multi-candle window.
  7. check_fvg()                 -- classic 3-candle imbalance, retried
     across a multi-candle window (not a one-shot check on a single bar).
  8. check_retest()              -- first later candle whose range overlaps
     the FVG zone.
  9. compute_trade_plan()        -- SL = the swept candle's own extreme;
     Target1 = R-multiple (repo-confirmed formula); Target2 = the next
     opposing liquidity level if one is active, else a fallback R-multiple.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

# ── Bars ──────────────────────────────────────────────────────────────────

@dataclass
class Bar:
    """Self-contained OHLC bar -- deliberately not imported from any other
    strategy's own bar dataclass."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float


class BarAccumulator:
    """Buckets a live ltp tick stream into fixed-timeframe bars.
    Self-contained, mirrors strategies/oi_flow/detector.py's own
    BarAccumulator (independently written, not imported)."""

    def __init__(self, timeframe_min: int = 1) -> None:
        self._tf = timeframe_min
        self.bars: List[Bar] = []
        self._bucket_open_ts: Optional[datetime] = None
        self._bucket: Optional[Bar] = None

    def on_tick(self, ts: datetime, ltp: float) -> bool:
        """Returns True iff this tick closed a bar (a new bucket started)."""
        bucket_ts = ts.replace(minute=(ts.minute // self._tf) * self._tf, second=0, microsecond=0)
        if self._bucket_open_ts is None:
            self._bucket_open_ts = bucket_ts
            self._bucket = Bar(timestamp=bucket_ts, open=ltp, high=ltp, low=ltp, close=ltp)
            return False
        if bucket_ts != self._bucket_open_ts:
            self.bars.append(self._bucket)
            self._bucket_open_ts = bucket_ts
            self._bucket = Bar(timestamp=bucket_ts, open=ltp, high=ltp, low=ltp, close=ltp)
            return True
        self._bucket.high = max(self._bucket.high, ltp)
        self._bucket.low = min(self._bucket.low, ltp)
        self._bucket.close = ltp
        return False

    def all_bars(self) -> List[Bar]:
        """Closed bars plus the currently-forming one, if any."""
        return self.bars + ([self._bucket] if self._bucket is not None else [])


# ── Rolling HTF base (liq_source="rolling_base" only) ───────────────────────

def compute_rolling_base(htf_bars: List[Bar]) -> Optional[Tuple[float, float]]:
    """Scans htf_bars from the start, tracking the most recent 'rolling
    base' -- the last HTF bar whose CLOSE broke the PREVIOUS bar's high/low
    range. Returns (base_high, base_low) of that base bar, or None if no
    breakout has occurred yet. Matches the validated Pine script's
    HTF-rolling-base definition exactly (close vs. previous bar's
    high/low, not previous bar's close -- a materially stricter, less
    noisy trigger)."""
    base: Optional[Tuple[float, float]] = None
    for i in range(1, len(htf_bars)):
        prev = htf_bars[i - 1]
        cur = htf_bars[i]
        if cur.close > prev.high or cur.close < prev.low:
            base = (cur.high, cur.low)
    return base


# ── Swing points ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SwingPoint:
    index: int
    timestamp: datetime
    price: float          # the wick extreme (outer boundary)
    body_extreme: float   # max(open,close) for a HIGH, min(open,close) for a LOW -- inner boundary
    kind: str              # "HIGH" | "LOW"


def find_swing_points(bars: List[Bar], pivot_left: int = 5, pivot_right: int = 5) -> List[SwingPoint]:
    """N-bar fractal pivot detector, asymmetric left/right (matches
    ta.pivothigh/ta.pivotlow's own (leftbars, rightbars) signature). Bar i
    is a swing HIGH if its high strictly exceeds the highs of pivot_left
    bars before it AND pivot_right bars after it (mirror for LOW). A pivot
    only confirms pivot_right bars after it happened -- a known, expected
    delay (both live and on replay), not repainting: once confirmed here,
    a SwingPoint's fields never change."""
    points: List[SwingPoint] = []
    n = len(bars)
    for i in range(pivot_left, n - pivot_right):
        bar = bars[i]
        before = bars[i - pivot_left:i]
        after = bars[i + 1:i + 1 + pivot_right]
        if all(bar.high > b.high for b in before) and all(bar.high > b.high for b in after):
            points.append(SwingPoint(i, bar.timestamp, bar.high, max(bar.open, bar.close), "HIGH"))
        if all(bar.low < b.low for b in before) and all(bar.low < b.low for b in after):
            points.append(SwingPoint(i, bar.timestamp, bar.low, min(bar.open, bar.close), "LOW"))
    return points


def latest_swing_level(swings: List[SwingPoint], kind: str) -> Optional[SwingPoint]:
    """Most recent confirmed swing of `kind` -- the "Swing Pivots" liquidity
    source (any single confirmed swing counts, no clustering required)."""
    relevant = [s for s in swings if s.kind == kind]
    if not relevant:
        return None
    return max(relevant, key=lambda s: s.index)


def latest_pool_level(
    swings: List[SwingPoint], kind: str, tol_pts: float = 5.0, min_touches: int = 2,
) -> Optional[SwingPoint]:
    """Liquidity Pool (Equal Highs/Lows) -- MY synthesized, validated answer
    to "what counts as real liquidity" after reviewing multiple independent
    reference indicators plus this codebase's own prior art (see
    pinescript/liquidity_sweep_indicator_with_risk.pine's header comment for
    the full reasoning). A lone swing isn't real liquidity by itself --
    what actually accumulates resting stop orders is MULTIPLE swings
    clustering near the same price (classic ICT equal-highs/equal-lows,
    same concept as strategies/fvg/detector.py's group_equal_levels).

    Walks the confirmed swing list in chronological order; each swing
    counts how many EARLIER same-kind swings already sit within tol_pts of
    it. The most recent swing whose own cluster size (itself + prior
    nearby swings) reaches min_touches becomes the active pool level.
    Returns None if no swing has ever reached that threshold."""
    relevant = sorted((s for s in swings if s.kind == kind), key=lambda s: s.index)
    active: Optional[SwingPoint] = None
    for i, s in enumerate(relevant):
        cluster = 1 + sum(1 for prior in relevant[:i] if abs(prior.price - s.price) <= tol_pts)
        if cluster >= min_touches:
            active = s
    return active


# ── Market structure (BoS/CHoCH) ────────────────────────────────────────────

@dataclass(frozen=True)
class StructureState:
    trend: int                    # 0 undetermined, 1 bullish, -1 bearish
    last_event: Optional[str]     # "BoS" | "CHoCH" -- only set on the bar that produced it
    last_event_index: Optional[int]


def compute_market_structure(bars: List[Bar], swings: List[SwingPoint], pivot_right: int = 5) -> StructureState:
    """Real structural directional state, replayed bar-by-bar, instead of a
    crude "price vs. level midpoint" check. Bias flips to bullish the
    instant a bar's close breaks above the most recent CONFIRMED swing
    HIGH (a Break of Structure if already bullish, a Change of Character if
    it was bearish -- the label differs, the directional read doesn't) and
    mirrors for bearish against the most recent confirmed swing LOW. Once a
    swing is consumed by a break, the NEXT confirmed swing of that kind
    becomes the new reference (matches the validated Pine script's
    "consumed -- the next confirmed high becomes the new reference"
    behavior).

    A swing only becomes available to this replay at bar index
    swing.index + pivot_right -- NOT swing.index itself. SwingPoint.index
    is the pivot bar's own position, but find_swing_points() can't confirm
    it exists until pivot_right bars later (the same delay ta.pivothigh has
    in the Pine version). Using swing.index directly here would let a bar
    react to a swing before it could possibly have been known yet -- a
    genuine look-ahead bug, not just a cosmetic difference from Pine."""
    highs = sorted((s for s in swings if s.kind == "HIGH"), key=lambda s: s.index)
    lows = sorted((s for s in swings if s.kind == "LOW"), key=lambda s: s.index)
    trend = 0
    last_event: Optional[str] = None
    last_event_index: Optional[int] = None
    hi_ptr = 0
    lo_ptr = 0
    active_high: Optional[SwingPoint] = None
    active_low: Optional[SwingPoint] = None

    for i, bar in enumerate(bars):
        while hi_ptr < len(highs) and highs[hi_ptr].index + pivot_right <= i:
            active_high = highs[hi_ptr]
            hi_ptr += 1
        while lo_ptr < len(lows) and lows[lo_ptr].index + pivot_right <= i:
            active_low = lows[lo_ptr]
            lo_ptr += 1

        if active_high is not None and bar.close > active_high.price:
            last_event = "BoS" if trend == 1 else "CHoCH"
            last_event_index = i
            trend = 1
            active_high = None   # consumed
        if active_low is not None and bar.close < active_low.price:
            last_event = "BoS" if trend == -1 else "CHoCH"
            last_event_index = i
            trend = -1
            active_low = None   # consumed

    return StructureState(trend=trend, last_event=last_event, last_event_index=last_event_index)


# ── Sweep ─────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SweepResult:
    bear: bool   # swept a HIGH -> bearish setup (PE)
    bull: bool   # swept a LOW -> bullish setup (CE)


def detect_sweep(
    bar: Bar,
    level_high: Optional[SwingPoint],
    level_low: Optional[SwingPoint],
) -> SweepResult:
    """A sweep = the bar's range pierces the level's OUTER (wick) boundary
    but CLOSES back past the level's INNER (body) boundary -- the LuxAlgo
    "Liquidity Swings" zone-rejection idea: a genuine stop-hunt candle
    shows real rejection back into the level's own body range, not just a
    marginal tick under the exact wick tip (a much weaker, almost-always-
    true bar). For liq_source="rolling_base" callers, pass a SwingPoint-
    shaped level whose .price and .body_extreme are equal (no separate body
    concept there) -- see engine.py's _base_as_level() helper."""
    bear = level_high is not None and bar.high > level_high.price and bar.close < level_high.body_extreme
    bull = level_low is not None and bar.low < level_low.price and bar.close > level_low.body_extreme
    return SweepResult(bear=bear, bull=bull)


# ── Displacement ──────────────────────────────────────────────────────────

def simple_atr(bars: List[Bar], length: int) -> Optional[float]:
    """Simple (unsmoothed) average of H-L range over the trailing `length`
    bars, INCLUDING the current/last bar -- matches the validated Pine
    script's ta.sma(high-low, atrLen), deliberately not Wilder/RMA-smoothed
    ta.atr()."""
    if len(bars) < 1:
        return None
    window = bars[-length:] if length <= len(bars) else bars
    if not window:
        return None
    return sum(b.high - b.low for b in window) / len(window)


def micro_swing_extreme(bars: List[Bar], lookback: int, kind: str) -> Optional[float]:
    """Highest high / lowest low over the trailing `lookback` bars EXCLUDING
    the current (last) bar -- the "has this candle broken recent micro-
    structure" check feeding check_displacement(). `bars` should already
    exclude the current bar (caller passes bars[:-1][-lookback:] or
    equivalent) -- see check_displacement()'s own slicing."""
    if not bars:
        return None
    window = bars[-lookback:] if lookback <= len(bars) else bars
    if not window:
        return None
    return max(b.high for b in window) if kind == "HIGH" else min(b.low for b in window)


def check_displacement(
    candidate: Bar,
    prior_bars: List[Bar],
    direction: int,          # 1 = watching for bullish displacement, -1 = bearish
    swing_len: int = 3,
    atr_len: int = 14,
    atr_mult: float = 0.7,
) -> bool:
    """Is `candidate` a genuine displacement candle in `direction`? Requires
    BOTH: (1) body/ATR ratio clears atr_mult (an aggressive, larger-than-
    typical move), AND (2) the candle's own close breaks the recent
    micro-swing (swing_len bars, excluding candidate itself) in the
    reversal direction. `prior_bars` is the bar history up to but NOT
    including `candidate` (used for both the ATR window and the
    micro-swing window, appending candidate for the ATR calc per the
    validated Pine script's own ta.sma(high-low,...) which includes the
    current bar).

    atr_mult default (0.7) and the compound-condition structure were both
    tuned against real evidence: a live funnel diagnostic isolated the ATR
    ratio as the binding constraint at higher thresholds (1.5, then 1.0) --
    see CLAUDE.md's Liquidity Sweep section for the exact numbers."""
    atr_window = prior_bars + [candidate]
    atr = simple_atr(atr_window, atr_len)
    if not atr or atr <= 0:
        return False
    body = abs(candidate.close - candidate.open)
    ratio = body / atr
    if ratio <= atr_mult:
        return False
    micro = micro_swing_extreme(prior_bars, swing_len, "HIGH" if direction == 1 else "LOW")
    if micro is None:
        return False
    if direction == 1:
        return candidate.close > candidate.open and candidate.close > micro
    return candidate.close < candidate.open and candidate.close < micro


# ── FVG ───────────────────────────────────────────────────────────────────

def check_fvg(candle1: Bar, candle3: Bar, direction: int) -> Optional[Tuple[float, float]]:
    """Classic 3-candle imbalance: candle1 = the bar immediately before the
    displacement candle (fixed once at displacement time), candle3 = a
    LATER candle being tested for a gap against candle1 (may be the
    displacement candle itself, or any candle within the confirmation
    window -- see engine.py's fvg_bars_left countdown, which retries this
    check across multiple candles rather than a one-shot check on a single
    bar, per real evidence this was silently killing the only displacement
    candidate that got through during tuning).

    Bullish (direction=1): candle1.high < candle3.low -- returns (gap_lo,
    gap_hi) = (candle1.high, candle3.low). Bearish mirrors. Returns None if
    no gap exists yet on this candle3."""
    if direction == 1:
        if candle1.high < candle3.low:
            return (candle1.high, candle3.low)
        return None
    if candle1.low > candle3.high:
        return (candle3.high, candle1.low)
    return None


# ── Retest ────────────────────────────────────────────────────────────────

def check_retest(bar: Bar, fvg_lo: float, fvg_hi: float) -> bool:
    """First later candle whose range overlaps the FVG zone."""
    return bar.low <= fvg_hi and bar.high >= fvg_lo


# ── Trade plan (entry / SL / Target1 / Target2) ─────────────────────────────

@dataclass(frozen=True)
class TradePlan:
    direction: int          # 1 = CE (long), -1 = PE (short)
    entry: float
    sl: float
    t1: float
    t2: float
    t2_is_liquidity: bool   # True if Target2 used the real opposing-liquidity definition, False if it fell back to the R-multiple


def compute_trade_plan(
    direction: int,
    entry: float,
    sl_anchor: float,       # the swept candle's own extreme
    tgt1_rr: float = 1.5,
    tgt2_rr: float = 3.0,
    opposing_liquidity: Optional[float] = None,
) -> TradePlan:
    """SL = sl_anchor directly (the swept candle's own extreme -- if the
    sweep was real, price should never trade back past its own origin).
    Target1 = entry + risk_distance * tgt1_rr (CONFIRMED formula from the
    original Liquidity-Sweep repo's execution/risk_engine.py). Target2 =
    that repo's REAL definition ("opposing PDH/PDL or unmitigated HTF
    zone") when `opposing_liquidity` is a valid, correctly-sided level;
    otherwise falls back to an R-multiple (tgt2_rr) -- an approximation
    used only when no genuine opposing structure is available at entry
    time."""
    risk = abs(entry - sl_anchor)
    t1 = entry + risk * tgt1_rr if direction == 1 else entry - risk * tgt1_rr
    use_liq = (
        opposing_liquidity is not None
        and ((direction == 1 and opposing_liquidity > entry) or (direction == -1 and opposing_liquidity < entry))
    )
    if use_liq:
        t2 = opposing_liquidity
    else:
        t2 = entry + risk * tgt2_rr if direction == 1 else entry - risk * tgt2_rr
    return TradePlan(direction=direction, entry=entry, sl=sl_anchor, t1=t1, t2=t2, t2_is_liquidity=use_liq)
