"""
strategies/liquidity_trap/detector.py — pure signal logic for the Liquidity
Trap strategy (option buyer).

Direct, faithful port of scripts/liquidity_trap_backtest.py (the real-data
SENSEX-spot backtest this mechanic was validated against, 1 year, Upstox) --
every function here mirrors that script's logic exactly, not a
reimplementation. Fully standalone (own module, no imports from any other
strategy's own detector), per strategies/liquidity_trap/__init__.py's
standalone mandate. `find_all_bear_zones`/`find_all_bull_zones` ARE reused
directly from strategies/v4_cascade/rolling_base.py -- that module is
generic platform zone-detection math (the same "3-candle ref/sweep/reclaim"
utility D1TrapBearOnlyBook already builds on), not another strategy's own
runtime state, so reusing it is the same category as reusing
matrix_engine/option_matrix.py elsewhere in this codebase -- not a
violation of the standalone mandate.

Pipeline, replayed on GROWING per-day bar lists exactly like the validated
backtest script (not an incremental/streaming state machine) -- the bar
counts involved (≤25 15m bars, ≤75 5m bars, ≤375 1m bars per day) make a
full re-scan on every new bar trivially cheap, and this guarantees the live
engine can never behaviorally drift from what was actually validated:

  1. find_ref_and_bias()      -- 15m ref-candle-rolling bias lock (Stage 1).
  2. find_sl_hit()             -- watch the locked ref candle's own opposite
                                   level on later 15m bars (Stage 2).
  3. find_5m_confirmation()    -- single FIXED 5m reference (the first 5m bar
                                   at/after the SL-hit), first later 5m bar to
                                   breach it confirms (Stage 3, 2026-08-21
                                   simplified per user spec -- no more ref-
                                   rolling on 5m).
  4. find_choch_entry()        -- 1m CHoCH: close breaks the most recent
                                   confirmed 2-bar-fractal opposing swing
                                   point (Stage 4).
  5. compute_sl_target()       -- SL = the deepest 5m sweep extreme; Target =
                                   entry + 2x that risk distance (1:2 RR,
                                   Stage 5).
  6. find_scale_in_level()     -- bear-trap (long) / bull-trap (short)
                                   3-candle zone on 1m bars since entry;
                                   add-on level = 1/3 above zone low (long) /
                                   1/3 below zone high (short) (Stage 6).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones

# ── Bars ──────────────────────────────────────────────────────────────────

@dataclass
class Bar:
    """Self-contained OHLC bar -- deliberately not imported from any other
    strategy's own bar dataclass. Exposes BOTH .ts and .timestamp (the
    latter for duck-typed compatibility with find_all_bear_zones/
    find_all_bull_zones, which read .timestamp)."""
    ts: datetime
    open: float
    high: float
    low: float
    close: float

    @property
    def timestamp(self) -> datetime:
        return self.ts


class BarAccumulator:
    """Buckets a live ltp tick stream into fixed-timeframe bars. Self-
    contained, mirrors strategies/liquidity_sweep/detector.py's own
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
            self._bucket = Bar(ts=bucket_ts, open=ltp, high=ltp, low=ltp, close=ltp)
            return False
        if bucket_ts != self._bucket_open_ts:
            self.bars.append(self._bucket)
            self._bucket_open_ts = bucket_ts
            self._bucket = Bar(ts=bucket_ts, open=ltp, high=ltp, low=ltp, close=ltp)
            return True
        self._bucket.high = max(self._bucket.high, ltp)
        self._bucket.low = min(self._bucket.low, ltp)
        self._bucket.close = ltp
        return False

    def all_bars(self) -> List[Bar]:
        """Closed bars plus the currently-forming one, if any."""
        return self.bars + ([self._bucket] if self._bucket is not None else [])


# ── Stage 1: 15m ref-candle-rolling bias lock ───────────────────────────────

def find_ref_and_bias(bars_15m_today: List[Bar]) -> Optional[Tuple[str, int, int]]:
    """Candle 1 of the day = initial ref. Compare each next candle to the
    CURRENT ref candle: breaches only the high -> bullish LOCKED, ref stays;
    breaches only the low -> bearish LOCKED, ref stays; breaches both or
    neither -> this candle becomes the new ref, keep rolling. Returns
    (bias, ref_idx, lock_idx) or None if nothing has locked yet today."""
    if len(bars_15m_today) < 2:
        return None
    ref_idx = 0
    for i in range(1, len(bars_15m_today)):
        ref = bars_15m_today[ref_idx]
        cur = bars_15m_today[i]
        broke_high = cur.high > ref.high
        broke_low = cur.low < ref.low
        if broke_high and not broke_low:
            return ("BULL", ref_idx, i)
        if broke_low and not broke_high:
            return ("BEAR", ref_idx, i)
        ref_idx = i
    return None


# ── Stage 1 (multi-ref variant, 2026-08-21 user spec) ────────────────────────
# "Each candle can be a separate ref, for long or short" -- every consecutive
# pair is checked independently, instead of a single rolling ref for the
# whole day. Backtested against real SENSEX data (scripts/liquidity_trap_
# multiref_backtest.py) before being ported here; find_ref_and_bias() above
# is kept as-is (superseded live, not deleted, in case of a future rollback).

@dataclass
class Setup:
    ref_idx: int
    direction: str   # "BULL" | "BEAR"
    locked_idx: int  # index of the candle that did the one-sided breach


def find_all_setups(bars_ref_today: List[Bar]) -> List[Setup]:
    """Every consecutive pair (candle[i-1] as ref, candle[i] as the breach
    check) independently spawns its own setup on a clean one-sided breach.
    Any number of setups, in either direction, across the day -- not a
    single rolling ref."""
    setups: List[Setup] = []
    for i in range(1, len(bars_ref_today)):
        ref = bars_ref_today[i - 1]
        cur = bars_ref_today[i]
        broke_high = cur.high > ref.high
        broke_low = cur.low < ref.low
        if broke_high and not broke_low:
            setups.append(Setup(ref_idx=i - 1, direction="BULL", locked_idx=i))
        elif broke_low and not broke_high:
            setups.append(Setup(ref_idx=i - 1, direction="BEAR", locked_idx=i))
    return setups


def compute_trend(bars_trend_tf: List[Bar], sma_len: int) -> Optional[str]:
    """Higher-timeframe trend filter (2026-08-21, real-data-validated
    optimization -- see scripts/liquidity_trap_tf_and_trend_sweep.py):
    'UP'/'DOWN' off the latest CLOSED trend-tf bar's close vs a simple
    sma_len-period SMA of trend-tf closes. None if not enough history yet
    (never guess). Live callers only ever pass already-closed bars, so
    there's no lookahead concern here (unlike the backtest sweep's own
    as-of-timestamp lookup, which had to guard against seeing future bars)."""
    if len(bars_trend_tf) < sma_len:
        return None
    closes = [b.close for b in bars_trend_tf[-sma_len:]]
    sma = sum(closes) / sma_len
    return "UP" if bars_trend_tf[-1].close > sma else "DOWN"


# ── Stage 2: watch the locked ref candle's own opposite level ───────────────

def find_sl_hit(bars_15m_today: List[Bar], ref_idx: int, lock_idx: int, bias: str) -> Optional[datetime]:
    """First later 15m candle whose low (bull) / high (bear) breaches the
    LOCKED ref candle's own opposite level. None if not hit yet today."""
    ref_candle = bars_15m_today[ref_idx]
    watch_level = ref_candle.low if bias == "BULL" else ref_candle.high
    for j in range(lock_idx + 1, len(bars_15m_today)):
        cj = bars_15m_today[j]
        if bias == "BULL" and cj.low <= watch_level:
            return cj.ts
        if bias == "BEAR" and cj.high >= watch_level:
            return cj.ts
    return None


# ── Stage 3: single fixed 5m reference confirmation ──────────────────────────

def find_5m_confirmation(bars_5m_since_sl_hit: List[Bar], bias: str) -> Optional[Tuple[datetime, float]]:
    """2026-08-21 simplified (user spec): the FIRST 5m bar at/after the SL-
    hit is a single FIXED reference (no rolling). The first LATER 5m candle
    whose high (bull) / low (bear) breaches that one reference confirms.
    Returns (confirm_ts, sweep_extreme) where sweep_extreme is the deepest
    low (bull) / highest high (bear) reached from the reference bar through
    the confirmation bar inclusive -- this becomes the trade's SL. None if
    not confirmed yet."""
    if len(bars_5m_since_sl_hit) < 2:
        return None
    ref5 = bars_5m_since_sl_hit[0]
    sweep_extreme = ref5.low if bias == "BULL" else ref5.high
    for k in range(1, len(bars_5m_since_sl_hit)):
        cur5 = bars_5m_since_sl_hit[k]
        sweep_extreme = (min(sweep_extreme, cur5.low) if bias == "BULL"
                         else max(sweep_extreme, cur5.high))
        if bias == "BULL" and cur5.high > ref5.high:
            return (cur5.ts, sweep_extreme)
        if bias == "BEAR" and cur5.low < ref5.low:
            return (cur5.ts, sweep_extreme)
    return None


# ── Stage 4: 1m CHoCH ────────────────────────────────────────────────────────

def find_swing_points(bars: List[Bar], pivot: int = 2) -> List[Tuple[int, str, float]]:
    """(index, kind, price) for confirmed 2-bar-fractal swing highs/lows."""
    out: List[Tuple[int, str, float]] = []
    n = len(bars)
    for i in range(pivot, n - pivot):
        bar = bars[i]
        before = bars[i - pivot:i]
        after = bars[i + 1:i + 1 + pivot]
        if all(bar.high > b.high for b in before) and all(bar.high > b.high for b in after):
            out.append((i, "HIGH", bar.high))
        if all(bar.low < b.low for b in before) and all(bar.low < b.low for b in after):
            out.append((i, "LOW", bar.low))
    return out


def find_choch_entry(bars_1m_since_confirm: List[Bar], bias: str, pivot: int = 2) -> Optional[Tuple[datetime, float]]:
    """A 1m close breaking the most recent confirmed opposing swing point --
    entry fires immediately at CHoCH confirmation (user confirmed: no retest
    wait). Returns (entry_ts, entry_price) or None."""
    sw = find_swing_points(bars_1m_since_confirm, pivot=pivot)
    if bias == "BULL":
        highs = sorted((s for s in sw if s[1] == "HIGH"), key=lambda s: s[0])
        hi_ptr, active_high = 0, None
        for i, bar in enumerate(bars_1m_since_confirm):
            while hi_ptr < len(highs) and highs[hi_ptr][0] + pivot <= i:
                active_high = highs[hi_ptr]
                hi_ptr += 1
            if active_high is not None and bar.close > active_high[2]:
                return (bar.ts, bar.close)
        return None
    lows = sorted((s for s in sw if s[1] == "LOW"), key=lambda s: s[0])
    lo_ptr, active_low = 0, None
    for i, bar in enumerate(bars_1m_since_confirm):
        while lo_ptr < len(lows) and lows[lo_ptr][0] + pivot <= i:
            active_low = lows[lo_ptr]
            lo_ptr += 1
        if active_low is not None and bar.close < active_low[2]:
            return (bar.ts, bar.close)
    return None


# ── Stage 5: 1:2 risk-reward ─────────────────────────────────────────────────

def compute_sl_target(direction: int, entry_price: float, sweep_extreme: float,
                      rr: float = 2.0) -> Tuple[float, float]:
    """SL = sweep_extreme (the 5m sweep's own deepest point) directly.
    Target = entry +/- rr * risk_distance (1:2 RR by default, user spec).
    direction: 1 = BULL (CE), -1 = BEAR (PE)."""
    risk = abs(entry_price - sweep_extreme)
    target = entry_price + rr * risk if direction == 1 else entry_price - rr * risk
    return sweep_extreme, target


# ── Stage 6: bear-trap (long) / bull-trap (short) scale-in zone ─────────────

def find_scale_in_level(zone_bars_1m: List[Bar], direction: int) -> Optional[Tuple[float, datetime]]:
    """direction=1 (BULL/long): bear-trap zone (find_all_bear_zones -- sellers
    trapped, bullish continuation) on 1m bars since entry; add-on level =
    zone_lo + (zone_hi-zone_lo)/3.
    direction=-1 (BEAR/short): bull-trap zone (find_all_bull_zones -- buyers
    trapped, bearish continuation); add-on level = zone_hi - (zone_hi-zone_lo)/3.
    Takes the FRESHEST confirmed zone (max lock_ts) if more than one exists.
    Returns (add_on_level, zone_lock_ts) or None if no zone has locked yet
    (needs >= 3 distinct bars for ref/sweep/reclaim)."""
    if len(zone_bars_1m) < 3:
        return None
    zones = find_all_bear_zones(zone_bars_1m) if direction == 1 else find_all_bull_zones(zone_bars_1m)
    if not zones:
        return None
    zone = max(zones, key=lambda z: z.lock_ts)
    lo, hi = sorted((zone.entry_line, zone.sweep_low))
    size = hi - lo
    if size <= 0:
        return None
    level = lo + size / 3.0 if direction == 1 else hi - size / 3.0
    return (level, zone.lock_ts)
