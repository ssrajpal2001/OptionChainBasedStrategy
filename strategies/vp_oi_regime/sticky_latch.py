"""Reusable trend state machine, 2026-10-03, REVISED 2026-10-15 direct user
spec (dual high/low ratcheting-anchor design, replaces the single
rolling-checkpoint design from earlier the same day): TWO anchors are kept
per side, both initialized to the session's first reading and persisting
(never reset) for the rest of the day:
  - high_anchor: the highest value ever seen -- ratchets UP on a new high.
  - low_anchor: the lowest value ever seen -- ratchets DOWN on a new low.

On each evaluation (throttled to eval_window_min -- e.g. every 5 minutes,
9:15 -> 9:20 -> 9:25 ...):
  1. Ratchet first: if now_value is a new high, high_anchor = now_value; if
     a new low, low_anchor = now_value. (A value can only ratchet one of
     the two, never both, since high_anchor >= low_anchor always.)
  2. Then compare now_value against BOTH (possibly just-ratcheted) anchors:
       pct_vs_high = (now_value - high_anchor) / high_anchor * 100
       pct_vs_low  = (now_value - low_anchor)  / low_anchor  * 100
     - pct_vs_high <= -fall_pct AND pct_vs_low >= +rise_pct (both true at
       once, e.g. low=100/high=200/now=150 -- genuinely possible when the
       reading sits strictly between a wide high/low spread): whichever
       move has the LARGER magnitude wins (direct user spec 2026-10-15).
     - elif pct_vs_high <= -fall_pct -> "Fall"
     - elif pct_vs_low >= +rise_pct -> "Rise"
     - else -> "No Change"
Between checkpoints, the last confirmed label is held (not re-derived) --
only a genuine checkpoint boundary produces a new label.

Originally built inside OiRegimeTracker for its Call/Put band totals;
extracted here so the SAME mechanic can also drive the Future OI trend
(FuturesRolloverTracker) without duplicating the logic -- the decision
matrix needs all three (Future/Put/Call OI) computed the identical way.
"""
from __future__ import annotations

from typing import Optional, Tuple


class StickyTrendLatch:
    def __init__(self, rise_pct: float = 20.0, fall_pct: float = 10.0, eval_window_min: int = 5) -> None:
        self.rise_pct = rise_pct
        self.fall_pct = fall_pct
        self.eval_window_min = eval_window_min
        self._high_anchor: Optional[float] = None
        self._low_anchor: Optional[float] = None
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
        self._high_anchor = None
        self._low_anchor = None
        self._trend = "No Change"
        self._last_eval_ts = None
        self.confirmed_cycles = 0

    def seed_anchor(self, anchor_value: float, ts: float) -> None:
        """2026-10-13, direct user spec: a mid-day process restart used to
        reset the anchors to whatever OI happened to be live at that exact
        moment -- wiping out the real session high/low tracked since market
        open and making the very next reading look like a huge, fake move
        off a wrong baseline. Call this ONCE right after construction (before
        any real update()) with the last confirmed value recorded so far
        today (reconstructed from strategies/vp_oi_regime/recorder.py's own
        DB). Seeds BOTH anchors to this one value (the real session-wide
        high/low split isn't separately recoverable from the DB today) --
        a cold seed, not a crossing."""
        # 2026-10-14 real incident: a stored 0/negative value means "no real
        # data was ever captured," never a genuine reading -- seeding from
        # it would freeze the anchor at 0 and make the next real tick look
        # like a huge false Rise. Guard here too, not just at the query layer.
        if anchor_value is None or anchor_value <= 0:
            return
        self._high_anchor = anchor_value
        self._low_anchor = anchor_value
        self._last_eval_ts = ts
        self._trend = "No Change"
        self.confirmed_cycles = 0

    def update(self, now_value: float, ts: float) -> Tuple[str, float, float, float, float]:
        """Returns (trend, pct_vs_high_anchor, pct_vs_low_anchor, high_anchor, low_anchor)."""
        if self._high_anchor is None:
            self._high_anchor = now_value
            self._low_anchor = now_value
            self._last_eval_ts = ts
            return self._trend, 0.0, 0.0, now_value, now_value

        if self._last_eval_ts is not None and (ts - self._last_eval_ts) < self.eval_window_min * 60.0:
            pct_hi = (now_value - self._high_anchor) / self._high_anchor * 100.0 if self._high_anchor > 0 else 0.0
            pct_lo = (now_value - self._low_anchor) / self._low_anchor * 100.0 if self._low_anchor > 0 else 0.0
            return self._trend, pct_hi, pct_lo, self._high_anchor, self._low_anchor

        # Checkpoint boundary -- ratchet the anchors first (new high / new
        # low), THEN compare against the (possibly just-updated) anchors.
        if now_value > self._high_anchor:
            self._high_anchor = now_value
        if now_value < self._low_anchor:
            self._low_anchor = now_value

        pct_hi = (now_value - self._high_anchor) / self._high_anchor * 100.0 if self._high_anchor > 0 else 0.0
        pct_lo = (now_value - self._low_anchor) / self._low_anchor * 100.0 if self._low_anchor > 0 else 0.0

        fall_hit = pct_hi <= -self.fall_pct
        rise_hit = pct_lo >= self.rise_pct
        if fall_hit and rise_hit:
            # Both genuinely true at once -- the larger-magnitude move wins
            # (direct user spec 2026-10-15).
            self._trend = "Fall" if abs(pct_hi) >= abs(pct_lo) else "Rise"
        elif fall_hit:
            self._trend = "Fall"
        elif rise_hit:
            self._trend = "Rise"
        else:
            self._trend = "No Change"

        self._last_eval_ts = ts
        self.confirmed_cycles += 1
        return self._trend, pct_hi, pct_lo, self._high_anchor, self._low_anchor
