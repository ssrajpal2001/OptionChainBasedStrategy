"""Reusable trend state machine, 2026-10-03, REVISED 2026-10-12 direct user
spec (peak-anchor design, replaces the prior symmetric +/-threshold-crossing
design): the anchor is a RUNNING PEAK (the highest value seen). On each
evaluation (throttled to eval_window_min, same cadence as before):
  - a NEW HIGH (now_value > anchor) immediately becomes the new anchor and
    is labeled "Rise".
  - otherwise, if now_value has fallen more than trend_pct BELOW that peak
    anchor, labeled "Fall" (anchor stays at the peak -- Fall never moves it,
    only a fresh new high does).
  - otherwise (below the peak but not fallen past trend_pct yet), labeled
    "No Change". No hold-over of a prior label -- re-evaluated fresh every
    cadence tick.

Originally built inside OiRegimeTracker for its Call/Put band totals;
extracted here so the SAME mechanic can also drive the Future OI trend
(FuturesRolloverTracker) without duplicating the logic -- the decision
matrix needs all three (Future/Put/Call OI) computed the identical way.
"""
from __future__ import annotations

from typing import Optional, Tuple


class StickyTrendLatch:
    def __init__(self, trend_pct: float = 10.0, eval_window_min: int = 5) -> None:
        self.trend_pct = trend_pct
        self.eval_window_min = eval_window_min
        self._anchor: Optional[float] = None
        self._trend: str = "No Change"
        self._last_eval_ts: Optional[float] = None
        # 2026-10-14, direct user spec: a restart (or the initial seed) only
        # establishes a BASELINE -- it must not count as a confirmed reading
        # of the trend. Only incremented on a genuine cadence-gated
        # re-evaluation (never on the first anchor-establishment, never on
        # a seed, never on an early-return inside the cadence window).
        # confirmed_cycles lets the caller require N real evaluations since
        # the last reset/seed before trusting this latch enough to ACT on.
        self.confirmed_cycles: int = 0

    def reset(self) -> None:
        self._anchor = None
        self._trend = "No Change"
        self._last_eval_ts = None
        self.confirmed_cycles = 0

    def seed_anchor(self, anchor_value: float, ts: float) -> None:
        """2026-10-13, direct user spec: a mid-day process restart used to
        reset the anchor to whatever OI happened to be live at that exact
        moment -- wiping out the real session peak tracked since market
        open and making the very next reading look like a huge, fake move
        off a wrong baseline. Call this ONCE right after construction (before
        any real update()) with the true peak OI recorded so far today
        (reconstructed from strategies/vp_oi_regime/recorder.py's own DB --
        every real evaluation this session is already logged there). Sets
        the anchor directly without emitting a Rise/Fall -- a cold seed, not
        a crossing."""
        # 2026-10-14 real incident: a stored 0/negative value means "no real
        # data was ever captured," never a genuine reading -- seeding from
        # it would freeze the anchor at 0 and make the next real tick look
        # like a huge false Rise. Guard here too, not just at the query layer.
        if anchor_value is None or anchor_value <= 0:
            return
        self._anchor = anchor_value
        self._last_eval_ts = ts
        self._trend = "No Change"
        self.confirmed_cycles = 0

    def update(self, now_value: float, ts: float) -> Tuple[str, float, float]:
        """Returns (trend, pct_vs_anchor, anchor_used_for_this_reading)."""
        if self._anchor is None:
            self._anchor = now_value
            self._last_eval_ts = ts
            return self._trend, 0.0, now_value

        if self._last_eval_ts is not None and (ts - self._last_eval_ts) < self.eval_window_min * 60.0:
            pct = (now_value - self._anchor) / self._anchor * 100.0 if self._anchor > 0 else 0.0
            return self._trend, pct, self._anchor

        self._last_eval_ts = ts
        self.confirmed_cycles += 1
        if now_value > self._anchor:
            # New peak -- anchor ratchets up, unconditionally labeled Rise.
            self._anchor = now_value
            self._trend = "Rise"
            return self._trend, 0.0, self._anchor

        pct = (now_value - self._anchor) / self._anchor * 100.0 if self._anchor > 0 else 0.0
        if pct <= -self.trend_pct:
            self._trend = "Fall"   # anchor (the peak) does NOT move on a Fall
        else:
            self._trend = "No Change"
        return self._trend, pct, self._anchor
