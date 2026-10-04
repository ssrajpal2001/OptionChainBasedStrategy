"""Session Volume Profile (SVP) -- spec row 1 of the OI+VP decision layer.

Pure, standalone, no live wiring. Bins a single session's traded price range
into a fixed number of rows, accumulates volume per row, and derives:
  - POC (Point of Control): the row with the most volume.
  - Value Area (VAH/VAL): expand outward from POC, always adding whichever
    neighboring row has more volume, until cumulative volume reaches
    ``value_area_pct`` (default 70%) of the session total.
  - LVN / HVN (Low/High Volume Nodes): rows below/above a volume-percentile
    threshold relative to the profile's own distribution.

Runs on SPOT/FUTURES price (confirmed with the user, not option premium) --
feed it the underlying's own 1-minute (high, low, volume) bars. Resets every
session (a Session Volume Profile is a single-day concept, same discipline
as strategies/fvg/'s intraday-only FVG pool).

2026-10-03 CRITICAL FIX, confirmed against a real TradingView Fixed Range
Volume Profile the user planted on the real NIFTY 27OCT26 FUT chart (same
real contract/day this module's own backtest uses): the original version of
this class binned each bar by its CLOSE price only, discarding the bar's own
High-Low range entirely. A real Volume Profile indicator (TradingView's own
FRVP included) distributes a bar's volume ACROSS every price row its real
[low, high] range touches, not onto a single point. Verified on a real,
narrow 2-candle window (09:15-09:24, 10 real 1-min bars): close-only binning
gave VAL=22639.75, ~12pts off the chart's real VAL (~22627); switching to
high-low volume spreading gave VAL=22627.92, matching the chart almost
exactly. Fixed: add_bar() now takes (high, low, volume) and spreads each
bar's volume evenly across every row between its own low and high.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class VolumeProfileSnapshot:
    poc: float
    vah: float
    val: float
    lvn_rows: List[float]
    hvn_rows: List[float]
    total_volume: float
    row_size: float
    rows: Dict[float, float]  # row-low-edge -> accumulated volume

    def location(self, price: float) -> str:
        """Where ``price`` sits relative to the Value Area -- 'above_vah',
        'inside_va', or 'below_val'. Matches the decision matrix's own
        "Price above POC, testing VAH" / "Price below POC, testing VAL" /
        "Price inside Value Area" language."""
        if price > self.vah:
            return "above_vah"
        if price < self.val:
            return "below_val"
        return "inside_va"

    def in_lvn(self, price: float, side: str) -> bool:
        """True if ``price`` sits inside an LVN row beyond the Value Area on
        the given side ('above' beyond VAH, 'below' beyond VAL) -- the
        "volume acceptance in LVN above VAH / below VAL" breakout-
        confirmation check the matrix's Algo S/R Trigger column describes."""
        for row_low in self.lvn_rows:
            row_high = row_low + self.row_size
            if side == "above" and row_low >= self.vah and row_low <= price < row_high:
                return True
            if side == "below" and row_high <= self.val and row_low <= price < row_high:
                return True
        return False


class SessionVolumeProfile:
    """Accumulates (price, volume) bars for one trading session and derives
    POC/VAH/VAL/LVN/HVN on demand. Reset at the start of every new session."""

    def __init__(
        self,
        rows: int = 80,
        value_area_pct: float = 0.70,
        lvn_percentile: float = 0.30,
        hvn_percentile: float = 0.70,
    ) -> None:
        if not (70 <= rows <= 100):
            raise ValueError(f"rows must be 70-100 per spec, got {rows}")
        self.rows = rows
        self.value_area_pct = value_area_pct
        self.lvn_percentile = lvn_percentile
        self.hvn_percentile = hvn_percentile
        self._bars: List[Tuple[float, float, float]] = []  # (high, low, volume)
        self._session_low: Optional[float] = None
        self._session_high: Optional[float] = None

    def reset(self) -> None:
        self._bars.clear()
        self._session_low = None
        self._session_high = None

    def add_bar(self, high: float, low: float, volume: float) -> None:
        if volume <= 0 or high < low:
            return
        self._bars.append((high, low, volume))
        self._session_low = low if self._session_low is None else min(self._session_low, low)
        self._session_high = high if self._session_high is None else max(self._session_high, high)

    def snapshot(self) -> Optional[VolumeProfileSnapshot]:
        """Returns None until at least one real bar has been added, or the
        session's traded range has zero width (can't bin a single price into
        >1 row -- treat as not-yet-ready rather than dividing by zero)."""
        if not self._bars or self._session_low is None or self._session_high is None:
            return None
        lo, hi = self._session_low, self._session_high
        if hi <= lo:
            return None

        row_size = (hi - lo) / self.rows
        row_vol: Dict[int, float] = {}
        for bar_high, bar_low, vol in self._bars:
            lo_idx = max(0, min(int((bar_low - lo) / row_size), self.rows - 1))
            hi_idx = max(0, min(int((bar_high - lo) / row_size), self.rows - 1))
            n_touched = hi_idx - lo_idx + 1
            per_row = vol / n_touched
            for idx in range(lo_idx, hi_idx + 1):
                row_vol[idx] = row_vol.get(idx, 0.0) + per_row

        total_volume = sum(row_vol.values())
        if total_volume <= 0:
            return None

        poc_idx = max(row_vol, key=row_vol.get)

        # Expand outward from POC, always adding whichever neighbor row has
        # more volume, until cumulative volume reaches value_area_pct.
        in_va = {poc_idx}
        cum = row_vol[poc_idx]
        lo_idx, hi_idx = poc_idx, poc_idx
        target = total_volume * self.value_area_pct
        while cum < target and (lo_idx > 0 or hi_idx < self.rows - 1):
            below_vol = row_vol.get(lo_idx - 1, 0.0) if lo_idx > 0 else -1.0
            above_vol = row_vol.get(hi_idx + 1, 0.0) if hi_idx < self.rows - 1 else -1.0
            if above_vol >= below_vol and hi_idx < self.rows - 1:
                hi_idx += 1
                cum += row_vol.get(hi_idx, 0.0)
                in_va.add(hi_idx)
            elif lo_idx > 0:
                lo_idx -= 1
                cum += row_vol.get(lo_idx, 0.0)
                in_va.add(lo_idx)
            else:
                break

        vah = lo + (hi_idx + 1) * row_size
        val = lo + lo_idx * row_size
        poc = lo + poc_idx * row_size + row_size / 2.0

        # LVN/HVN relative to this profile's own distribution: rank every
        # row's volume as a percentile of the max row volume, classify low/
        # high nodes accordingly. Empty (zero-volume) rows are LVNs too --
        # they are, definitionally, the lowest-volume rows in the profile.
        max_row_vol = row_vol[poc_idx]
        lvn_rows: List[float] = []
        hvn_rows: List[float] = []
        for idx in range(self.rows):
            v = row_vol.get(idx, 0.0)
            pct_of_max = v / max_row_vol if max_row_vol > 0 else 0.0
            row_low = lo + idx * row_size
            if pct_of_max <= self.lvn_percentile:
                lvn_rows.append(row_low)
            elif pct_of_max >= self.hvn_percentile:
                hvn_rows.append(row_low)

        rows_out = {lo + idx * row_size: row_vol.get(idx, 0.0) for idx in range(self.rows)}

        return VolumeProfileSnapshot(
            poc=poc, vah=vah, val=val, lvn_rows=lvn_rows, hvn_rows=hvn_rows,
            total_volume=total_volume, row_size=row_size, rows=rows_out,
        )
