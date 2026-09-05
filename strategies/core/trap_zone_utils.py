"""
strategies/core/trap_zone_utils.py -- shared trap/zone-detection primitives.

2026-09-06: extracted here (not deleted) while removing the D1 Trap FnO/
Index and Liquidity Trap strategies, which the user has stopped running.
`strategies/oi_orb_screener/` (a KEPT strategy) depends directly on
`Bar`/`BarAccumulator`/`find_all_setups` (originally in
strategies/liquidity_trap/detector.py) and `_collapse_nearby_zones`
(originally in strategies/d1_trap_option/bear_only_book.py) for its own
trap-zone target mechanic -- moved verbatim, not reimplemented, so
oi_orb_screener's behavior doesn't drift from what was actually validated.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

# ── Bars ──────────────────────────────────────────────────────────────────

@dataclass
class Bar:
    """Self-contained OHLC bar. Exposes BOTH .ts and .timestamp (the latter
    for duck-typed compatibility with any zone-detection helper that reads
    .timestamp)."""
    ts: datetime
    open: float
    high: float
    low: float
    close: float

    @property
    def timestamp(self) -> datetime:
        return self.ts


class BarAccumulator:
    """Buckets a live ltp tick stream into fixed-timeframe bars."""

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


# ── Multi-ref setup detection (originally liquidity_trap/detector.py) ─────

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


# ── Zone collapsing (originally d1_trap_option/bear_only_book.py) ─────────

_ZONE_MERGE_THRESHOLD_PTS = 20.0
_ZONE_MAX_REF_GAP_BARS = 2


def _collapse_nearby_zones(zones: List[dict], threshold_pts: float = _ZONE_MERGE_THRESHOLD_PTS,
                            max_ref_gap: int = _ZONE_MAX_REF_GAP_BARS) -> List[dict]:
    """Merge zones whose bands are within threshold_pts of each other AND
    whose ref candles are within max_ref_gap bars of each other into one,
    taking max(zone_hi)/min(zone_lo) across the group. The merged zone's
    entry_line/lock_ts/ref_ts come from whichever member has the MOST
    RECENT lock_ts (the most current reference level in the group)."""
    if not zones:
        return []
    ordered = sorted(zones, key=lambda z: (z["zone_lo"], z["zone_hi"]))
    groups = [[ordered[0]]]
    for z in ordered[1:]:
        grp = groups[-1]
        group_hi = max(g["zone_hi"] for g in grp)
        near_in_time = any(abs(z["ref_idx"] - g["ref_idx"]) <= max_ref_gap for g in grp)
        truly_overlaps = z["zone_lo"] <= group_hi
        if near_in_time and (truly_overlaps or z["zone_lo"] <= group_hi + threshold_pts):
            grp.append(z)
        else:
            groups.append([z])
    collapsed = []
    for group in groups:
        newest = max(group, key=lambda g: g["lock_ts"])
        collapsed.append(dict(
            zone_lo=min(g["zone_lo"] for g in group), zone_hi=max(g["zone_hi"] for g in group),
            entry_line=newest["entry_line"], lock_ts=newest["lock_ts"],
            ref_ts=newest["ref_ts"], ref_idx=newest["ref_idx"],
            state="WAITING", ref_bar=None, done=False, invalid=False,
            contact_ts=None, ref_open=None, ref_close_time=None,
            breach_ts=None, sub_lo=None, sub_hi=None,
        ))
    return collapsed
