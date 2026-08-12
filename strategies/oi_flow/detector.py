"""
strategies/oi_flow/detector.py — signal logic for the OI-Flow Pre-Breakout
strategy. Fully self-contained by explicit design (see
strategies/oi_flow/__init__.py): the swing-point/structure-break detector
here is a FRESH, independent implementation, not imported from
strategies/fvg/detector.py even though that module already has an
equivalent -- this strategy shares zero runtime logic with any other
strategy in the codebase.

Two separate gates, checked at two different points in the entry sequence
by strategies/oi_flow/engine.py (Phase 4):

  detect_pre_breakout_signal()   -- SPOT chart + OI wall: is price still
    consolidating near a wall, with the opposing side's OI flattening/
    dropping while the supporting side builds? This is the "before the
    breakout" half.

  confirm_option_price_action()  -- OPTION PREMIUM chart: is the actual
    tradeable instrument holding above its own VWAP with no rejection
    wick, and where should the stop sit (the option's own recent swing
    low, never a spot-derived offset)? This is the execution-quality half,
    checked only once the spot gate has already fired.

Kept as two separate functions (not folded together) so each stays
single-purpose and independently testable, and so the engine can log,
per gate, exactly which one blocked a given evaluation -- essential
telemetry given this strategy cannot be backtested (see plan doc; no
historical OI data exists via Upstox's intraday API).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

_UPPER_WICK_REJECTION_RATIO = 0.55   # upper wick / total range >= this -> rejection candle


@dataclass
class Bar:
    """Self-contained OHLC bar -- deliberately not imported from any other
    strategy's own bar dataclass, however structurally similar."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float


class BarAccumulator:
    """Buckets a live ltp tick stream (spot OR option, caller's choice) into
    fixed-timeframe bars. Self-contained -- does not reuse any other
    strategy's own tick-to-bar bucket logic."""

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
        """Closed bars plus the currently-forming one (if any) -- callers
        that want to react to the live-forming bar (e.g. VWAP-hold checks
        intraday, not just on close) need this; callers that only want
        confirmed closed bars should use .bars directly."""
        return self.bars + ([self._bucket] if self._bucket is not None else [])


# ── 2a. Self-contained swing-point / structure-break detection ──────────────

@dataclass(frozen=True)
class SwingPoint:
    index: int
    timestamp: datetime
    price: float
    kind: str   # "HIGH" | "LOW"


def find_swing_points(bars: List[Bar], pivot: int = 2) -> List[SwingPoint]:
    """Simple N-bar fractal pivot detector: bar i is a swing HIGH if its
    high strictly exceeds the highs of `pivot` bars on both sides (mirror
    for LOW). The most recent `pivot` bars can never be confirmed yet --
    same inherent lag as any fractal method, expected and fine here since
    we only ever look at the MOST RECENT *confirmed* swing anyway."""
    points: List[SwingPoint] = []
    n = len(bars)
    for i in range(pivot, n - pivot):
        bar = bars[i]
        before = bars[i - pivot:i]
        after = bars[i + 1:i + 1 + pivot]
        if all(bar.high > b.high for b in before) and all(bar.high > b.high for b in after):
            points.append(SwingPoint(i, bar.timestamp, bar.high, "HIGH"))
        if all(bar.low < b.low for b in before) and all(bar.low < b.low for b in after):
            points.append(SwingPoint(i, bar.timestamp, bar.low, "LOW"))
    return points


def has_recent_structure_break(bars: List[Bar], swings: List[SwingPoint], direction: str) -> bool:
    """True if any bar AFTER the most recent opposing swing point closed
    through it. direction="BULLISH" checks against the last swing HIGH
    (a close above it = the breakout already happened); "BEARISH" mirrors
    against the last swing LOW. Absence of a break is exactly "still
    consolidating" -- the polarity a pre-breakout gate needs."""
    kind = "HIGH" if direction == "BULLISH" else "LOW"
    relevant = [s for s in swings if s.kind == kind]
    if not relevant:
        return False
    last_swing = max(relevant, key=lambda s: s.index)
    for bar in bars[last_swing.index + 1:]:
        if direction == "BULLISH" and bar.close > last_swing.price:
            return True
        if direction == "BEARISH" and bar.close < last_swing.price:
            return True
    return False


def swing_low(bars: List[Bar], pivot: int = 2) -> Optional[float]:
    """Most recent confirmed swing LOW price -- used as the option-chart-
    native stop-loss anchor in confirm_option_price_action(). None if no
    swing low is confirmed yet (too few bars)."""
    lows = [s for s in find_swing_points(bars, pivot=pivot) if s.kind == "LOW"]
    if not lows:
        return None
    return max(lows, key=lambda s: s.index).price


def swing_high(bars: List[Bar], pivot: int = 2) -> Optional[float]:
    """Mirror of swing_low() for PE-side stop-loss anchoring."""
    highs = [s for s in find_swing_points(bars, pivot=pivot) if s.kind == "HIGH"]
    if not highs:
        return None
    return max(highs, key=lambda s: s.index).price


# ── 2c. Spot-side pre-breakout signal ────────────────────────────────────────

@dataclass(frozen=True)
class OIPreEntrySignal:
    side: str            # "CE" | "PE"
    reason: str
    wall_strike: float
    opposing_roc: int
    supporting_roc: int
    pcr: float


def detect_pre_breakout_signal(
    side: str,
    oi_tracker,                       # strategies.oi_flow.tracker.OIFlowTracker
    snap,                              # matrix_engine.option_matrix.ChainSnapshot (duck-typed)
    spot_bars_1m: List[Bar],
    window_sec: int = 180,
    max_opposing_roc_pct: float = -0.01,
    min_supporting_roc_pct: float = 0.02,
    min_pcr_bias: float = 1.2,
    max_pcr_bias: float = 0.7,
    proximity_pct: float = 0.005,
    strike_step: float = 100.0,
    swing_pivot: int = 2,
    now: Optional[datetime] = None,
) -> Optional[OIPreEntrySignal]:
    """CE watches the call-OI wall (resistance); PE watches the put-OI wall
    (support), mirrored logic. Returns None (never a false signal) on any
    rejected condition -- callers/telemetry should distinguish "rejected
    because X" from "fired" using the returned None vs. the object's own
    .reason, i.e. this function purposely does NOT itself log why it
    returned None; the caller (engine.py) is expected to re-derive that
    for telemetry using the same tracker/snap it already has, keeping this
    function a pure boolean-shaped decision, not a logger."""
    if side not in ("CE", "PE"):
        raise ValueError(f"side must be CE or PE, got {side!r}")
    if not spot_bars_1m:
        return None
    spot = spot_bars_1m[-1].close

    if side == "CE":
        wall = snap.max_call_oi_strike
        supporting_strike = wall - strike_step
        opposing_type, supporting_type = "CE", "PE"
        direction = "BULLISH"
    else:
        wall = snap.max_put_oi_strike
        supporting_strike = wall + strike_step
        opposing_type, supporting_type = "PE", "CE"
        direction = "BEARISH"

    if not wall:
        return None
    if abs(spot - wall) / wall > proximity_pct:
        return None

    swings = find_swing_points(spot_bars_1m, pivot=swing_pivot)
    if has_recent_structure_break(spot_bars_1m, swings, direction):
        return None   # already broken out -- exactly the lagging state this exists to get ahead of

    opposing_now = oi_tracker.oi_now(wall, opposing_type)
    opposing_roc = oi_tracker.oi_roc(wall, opposing_type, window_sec, now=now)
    supporting_now = oi_tracker.oi_now(supporting_strike, supporting_type)
    supporting_roc = oi_tracker.oi_roc(supporting_strike, supporting_type, window_sec, now=now)
    if opposing_roc is None or supporting_roc is None:
        return None
    if not opposing_now or not supporting_now:
        return None

    if opposing_roc > max_opposing_roc_pct * opposing_now:
        return None   # opposing wall's OI is still building -- writers still defending, not fleeing
    if supporting_roc < min_supporting_roc_pct * supporting_now:
        return None   # supporting side isn't building fast enough to call this a real floor forming

    pcr = snap.pcr_smooth()
    if side == "CE" and pcr <= min_pcr_bias:
        return None
    if side == "PE" and pcr >= max_pcr_bias:
        return None

    return OIPreEntrySignal(
        side=side, reason="oi_flow_pre_breakout", wall_strike=wall,
        opposing_roc=opposing_roc, supporting_roc=supporting_roc, pcr=pcr,
    )


# ── 2d. Option-side confirmation gate ────────────────────────────────────────

@dataclass(frozen=True)
class OptionConfirmation:
    ok: bool
    reason: str
    vwap: Optional[float] = None
    sl_level: Optional[float] = None


def _vwap(bars: List[Bar]) -> Optional[float]:
    """Simple session-anchored typical-price VWAP over the given bars (this
    strategy's own, not borrowed from any other strategy's indicator
    engine). Uses (H+L+C)/3 as the proxy typical price since real per-bar
    traded volume isn't threaded into Bar (ltp-tick-derived OHLC only) --
    documented limitation, adequate for a "holding above recent value
    area" check, not a precision volume-weighted calculation."""
    if not bars:
        return None
    total = 0.0
    for b in bars:
        total += (b.high + b.low + b.close) / 3.0
    return total / len(bars)


def confirm_option_price_action(
    option_bars_1m: List[Bar],
    side: str,
    lookback: int = 20,
    swing_pivot: int = 2,
    wick_rejection_ratio: float = _UPPER_WICK_REJECTION_RATIO,
) -> OptionConfirmation:
    """CE: premium must be holding ABOVE its own recent VWAP with no active
    upper-wick rejection on the most recent candle (rejection = seller
    pressure right at the current price, a bad time to buy into strength).
    PE mirrors: below VWAP, no lower-wick rejection. sl_level is the
    option's own most recent confirmed swing low (CE) / swing high (PE) --
    the stop anchor engine.py must use, never a spot-derived offset."""
    if side not in ("CE", "PE"):
        raise ValueError(f"side must be CE or PE, got {side!r}")
    if not option_bars_1m:
        return OptionConfirmation(ok=False, reason="no_option_bars")

    window = option_bars_1m[-lookback:] if lookback else option_bars_1m
    vwap = _vwap(window)
    if vwap is None:
        return OptionConfirmation(ok=False, reason="no_vwap")

    last = option_bars_1m[-1]
    rng = last.high - last.low
    if side == "CE":
        if last.close < vwap:
            return OptionConfirmation(ok=False, reason="below_vwap", vwap=vwap)
        upper_wick = last.high - max(last.open, last.close)
        if rng > 0 and (upper_wick / rng) >= wick_rejection_ratio:
            return OptionConfirmation(ok=False, reason="upper_wick_rejection", vwap=vwap)
        sl = swing_low(window, pivot=swing_pivot)
    else:
        if last.close > vwap:
            return OptionConfirmation(ok=False, reason="above_vwap", vwap=vwap)
        lower_wick = min(last.open, last.close) - last.low
        if rng > 0 and (lower_wick / rng) >= wick_rejection_ratio:
            return OptionConfirmation(ok=False, reason="lower_wick_rejection", vwap=vwap)
        sl = swing_high(window, pivot=swing_pivot)

    if sl is None:
        return OptionConfirmation(ok=False, reason="no_swing_sl_anchor_yet", vwap=vwap)
    return OptionConfirmation(ok=True, reason="confirmed", vwap=vwap, sl_level=sl)
