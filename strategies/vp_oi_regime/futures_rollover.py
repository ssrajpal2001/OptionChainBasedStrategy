"""Futures expiry/rollover-aware OI tracking -- spec row 6, entirely new
(no prior implementation anywhere in this codebase; the Delta/crypto
DeltaRolloverWorker is a different asset class with an unrelated mechanic).

Tracks near-month and (during expiry week) next-month futures OI/volume in
parallel, flags a genuine rollover (near-month OI falling while next-month
rises by a comparable magnitude) so OiRegimeTracker can suppress a false
directional-reversal call, and switches the "active" contract once
next-month's own volume or OI overtakes near-month's.

Tolerance for "comparable magnitude" (``rollover_match_tol``) is a tunable
default, NOT yet confirmed against a real expiry week's data -- flagged in
the build plan as an open item to revisit once real rollover-week data is
available.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional, Tuple

from strategies.vp_oi_regime.sticky_latch import StickyTrendLatch


@dataclass
class RolloverState:
    is_expiry_week: bool
    active_contract: str          # "near" | "next"
    near_oi: Optional[float]
    next_oi: Optional[float]
    near_oi_change_pct: Optional[float]
    next_oi_change_pct: Optional[float]
    rollover_detected: bool
    reason: str
    # 2026-10-03, direct user spec: the "Future OI" column feeding the 27-row
    # decision matrix -- same sticky-latch mechanic as Put/Call OI, computed
    # off whichever contract is currently ACTIVE (near, or next once the
    # Contract Switch Rule has fired) so the matrix always reads the
    # currently-relevant contract's own momentum, not a stale one.
    future_oi_trend: str = "No Change"
    future_oi_pct_vs_anchor: float = 0.0
    future_oi_anchor: float = 0.0


class FuturesRolloverTracker:
    """Feed near-month and next-month futures OI/volume via update_near()/
    update_next(); call classify() to get the current rollover state."""

    def __init__(
        self,
        change_window_min: int = 15,
        rollover_match_tol_pct: float = 25.0,
        min_move_pct: float = 10.0,
        maxlen: int = 240,
    ) -> None:
        # rollover_match_tol_pct: how close the near-month FALL magnitude and
        # next-month RISE magnitude must be (as a % of each other) to call it
        # a rollover rather than a coincidence -- tunable, see module docstring.
        # min_move_pct: minimum OI change (either side) before even
        # considering a rollover call -- avoids flagging routine noise.
        self.change_window_min = change_window_min
        self.rollover_match_tol_pct = rollover_match_tol_pct
        self.min_move_pct = min_move_pct
        self._near: Deque[Tuple[float, float, float]] = deque(maxlen=maxlen)  # (ts, oi, volume)
        self._next: Deque[Tuple[float, float, float]] = deque(maxlen=maxlen)
        self._active_contract = "near"
        self._future_oi_latch = StickyTrendLatch(trend_pct=3.0, eval_window_min=change_window_min)

    def reset(self) -> None:
        self._near.clear()
        self._next.clear()
        self._active_contract = "near"
        self._future_oi_latch.reset()

    def update_near(self, ts: float, oi: float, volume: float) -> None:
        self._near.append((ts, oi, volume))

    def update_next(self, ts: float, oi: float, volume: float) -> None:
        self._next.append((ts, oi, volume))

    def _at_or_before(self, dq: Deque[Tuple[float, float, float]], ts: float) -> Optional[Tuple[float, float]]:
        val = None
        for row_ts, oi, vol in dq:
            if row_ts > ts:
                break
            val = (oi, vol)
        return val

    def classify(self, now_ts: float, is_expiry_week: bool) -> RolloverState:
        if not is_expiry_week or not self._next:
            # Standard tracking: near-month only, no rollover possible by
            # definition outside expiry week (spec row 6, "Standard Tracking").
            near_now = self._near[-1][1] if self._near else None
            f_trend, f_pct, f_anchor = (
                self._future_oi_latch.update(near_now, now_ts) if near_now is not None
                else ("No Change", 0.0, 0.0)
            )
            return RolloverState(
                is_expiry_week=is_expiry_week, active_contract="near",
                near_oi=near_now, next_oi=None,
                near_oi_change_pct=None, next_oi_change_pct=None,
                rollover_detected=False, reason="standard tracking (near-month only)",
                future_oi_trend=f_trend, future_oi_pct_vs_anchor=f_pct, future_oi_anchor=f_anchor,
            )

        near_now = self._near[-1][1] if self._near else None
        next_now = self._next[-1][1] if self._next else None
        near_vol_now = self._near[-1][2] if self._near else None
        next_vol_now = self._next[-1][2] if self._next else None

        prev_ts = now_ts - self.change_window_min * 60.0
        near_prev = self._at_or_before(self._near, prev_ts)
        next_prev = self._at_or_before(self._next, prev_ts)

        near_chg_pct = None
        if near_now is not None and near_prev and near_prev[0] > 0:
            near_chg_pct = (near_now - near_prev[0]) / near_prev[0] * 100.0
        next_chg_pct = None
        if next_now is not None and next_prev and next_prev[0] > 0:
            next_chg_pct = (next_now - next_prev[0]) / next_prev[0] * 100.0

        # Contract switch rule: once next-month's own volume or absolute OI
        # exceeds near-month's, flip active_contract for all subsequent reads.
        if (next_now is not None and near_now is not None and next_now > near_now) or (
            next_vol_now is not None and near_vol_now is not None and next_vol_now > near_vol_now
        ):
            self._active_contract = "next"

        rollover_detected = False
        reason = "no rollover signal"
        if near_chg_pct is not None and next_chg_pct is not None:
            near_falling = near_chg_pct <= -self.min_move_pct
            next_rising = next_chg_pct >= self.min_move_pct
            if near_falling and next_rising:
                # "Comparable magnitude" -- the two percentage moves (one
                # negative, one positive) must be within rollover_match_tol_pct
                # of each other, relative to the larger of the two.
                mag_a, mag_b = abs(near_chg_pct), abs(next_chg_pct)
                larger = max(mag_a, mag_b)
                diff_pct = abs(mag_a - mag_b) / larger * 100.0 if larger > 0 else 0.0
                if diff_pct <= self.rollover_match_tol_pct:
                    rollover_detected = True
                    reason = (
                        f"ROLLOVER: near-month OI {near_chg_pct:+.1f}%, "
                        f"next-month OI {next_chg_pct:+.1f}% -- comparable magnitude "
                        f"(diff={diff_pct:.1f}% <= {self.rollover_match_tol_pct}%), "
                        f"treat as position shift, NOT Long Unwinding/Short Covering."
                    )
                else:
                    reason = (
                        f"near-month falling ({near_chg_pct:+.1f}%) and next-month rising "
                        f"({next_chg_pct:+.1f}%) but magnitudes diverge (diff={diff_pct:.1f}% "
                        f"> {self.rollover_match_tol_pct}%) -- NOT classified as rollover, "
                        f"treat as a genuine directional signal."
                    )

        active_oi = next_now if self._active_contract == "next" else near_now
        f_trend, f_pct, f_anchor = (
            self._future_oi_latch.update(active_oi, now_ts) if active_oi is not None
            else ("No Change", 0.0, 0.0)
        )

        return RolloverState(
            is_expiry_week=is_expiry_week, active_contract=self._active_contract,
            near_oi=near_now, next_oi=next_now,
            near_oi_change_pct=near_chg_pct, next_oi_change_pct=next_chg_pct,
            rollover_detected=rollover_detected, reason=reason,
            future_oi_trend=f_trend, future_oi_pct_vs_anchor=f_pct, future_oi_anchor=f_anchor,
        )
