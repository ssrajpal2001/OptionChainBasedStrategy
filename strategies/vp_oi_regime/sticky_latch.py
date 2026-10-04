"""Reusable sticky/latched trend state machine (hysteresis), 2026-10-03
direct user spec: a fixed reference ANCHOR is kept; the trend only updates
(and the anchor only moves) when the current value crosses +/-trend_pct away
from that anchor. A reading that falls back inside the band does NOT reset
the trend to "No Change" -- it holds whatever was last confirmed, against
the SAME anchor, until a genuine new crossing happens.

Originally built inside OiRegimeTracker for its Call/Put band totals;
extracted here so the SAME mechanic can also drive the Future OI trend
(FuturesRolloverTracker) without duplicating the logic -- the decision
matrix needs all three (Future/Put/Call OI) computed the identical way.
"""
from __future__ import annotations

from typing import Optional, Tuple


class StickyTrendLatch:
    def __init__(self, trend_pct: float = 3.0, eval_window_min: int = 5) -> None:
        self.trend_pct = trend_pct
        self.eval_window_min = eval_window_min
        self._anchor: Optional[float] = None
        self._trend: str = "No Change"
        self._last_eval_ts: Optional[float] = None

    def reset(self) -> None:
        self._anchor = None
        self._trend = "No Change"
        self._last_eval_ts = None

    def update(self, now_value: float, ts: float) -> Tuple[str, float, float]:
        """Returns (trend, pct_vs_anchor, anchor_used_for_this_reading)."""
        if self._anchor is None:
            self._anchor = now_value
            self._last_eval_ts = ts
            return self._trend, 0.0, now_value

        pct = (now_value - self._anchor) / self._anchor * 100.0 if self._anchor > 0 else 0.0
        if self._last_eval_ts is not None and (ts - self._last_eval_ts) < self.eval_window_min * 60.0:
            return self._trend, pct, self._anchor

        self._last_eval_ts = ts
        if pct >= self.trend_pct:
            self._anchor = now_value
            self._trend = "Rise"
        elif pct <= -self.trend_pct:
            self._anchor = now_value
            self._trend = "Fall"
        # else: sticky -- anchor and trend both stay as they were.
        return self._trend, pct, self._anchor
