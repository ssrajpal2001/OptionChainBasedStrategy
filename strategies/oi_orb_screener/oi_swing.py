"""
strategies/oi_orb_screener/oi_swing.py -- pure, unit-tested decision logic
originally built for the "Future OI-Price Swing Breakout Strategy" entry/
exit mode (entry_exit_mode="oi_swing_v1" in strategy_params).

2026-09-18, direct user decision: entry_exit_mode="oi_swing_v1" itself
(engine.py's _oi_swing_entry_scan/_oi_swing_exit_check/_seed_oi_swing_
history, and this module's own check_immediate_entry_trigger/is_entry_
within_cutoff/is_min_hold_satisfied -- Fixes 2/3 below) was removed
entirely ahead of a brand new replacement entry/exit design. This file is
KEPT (not deleted) because update_swing_state/check_oi_swing_breakout/
floor_to_bucket are pure, already-validated building blocks the new
design's own Section 16 ("OI Swing High/Low on Futures OI") is expected to
reuse directly -- see CLAUDE.md's OI-ORB Screener section for that plan.

Ported from a real-data backtest series
(scripts/oi_orb_futures_oi_swing_management_backtest.py +
scripts/oi_orb_swing_optimization_variants.py, validated across 13 real
trading days once a real prev-close date-anchoring bug in that backtest
was found and fixed) -- NOT reimplemented from scratch. This module
intentionally reuses ONLY the plain, spec-exact 3-point swing rule (no
gap-amplitude filter, no 5-point lookback) -- those were exploratory
optimization-sweep variants, never approved for production.

======================= MECHANIC =======================

1. Objective -- classify Futures-OI + price co-movement (diagnostic
   framing only; the actual entry stays the plain 2% price trigger,
   never OI-gated):
     Long build-up:  OI up + Price up
     Short build-up: OI up + Price down
   Options OI is NEVER used -- Futures OI + stock/underlying price only.
2. ENTRY: immediate on the real intraday 2.0% price trigger vs
   yesterday's real close -- CALL the instant price first crosses +2%,
   PUT the instant it first crosses -2%. No VWAP-retest wait, no OI-
   confirmation-before-entry gate.
3. OI SWING STRUCTURE: built on a 5-minute Futures-OI series from session
   start (09:15). A 3-point immediate-neighbor local extremum
   (OI[i]>OI[i-1] and OI[i]>OI[i+1] for a high, mirrored for a low) is
   confirmed ONE BAR LATE -- i.e. only once OI[i+1] is also known.
4. TRADE MANAGEMENT (the live position's own exit decision): on each new
   5-min bar, check whether CURRENT OI has broken the latest CONFIRMED
   swing high (current > swing_high) or swing low (current < swing_low).
   If not, do nothing -- the mechanic must NOT react to every 5-min OI
   wiggle, only a genuine breakout/breakdown of a CONFIRMED level.
   If it has, check the 5-min price direction (current close vs the
   previous 5-min close):
     LONG (CALL):  price UP   -> HOLD; price DOWN or FLAT -> EXIT.
     SHORT (PUT):  price DOWN -> HOLD; price UP or FLAT   -> EXIT.
   Flat defaults to EXIT -- the mechanic's own principle throughout is
   that price must ACTIVELY confirm the held direction to continue; a
   flat print confirms nothing. (Documented, single-comparison-operator
   flip in check_oi_swing_breakout if this default is ever revisited.)

======================= REMOVED 2026-09-18 =======================
The entry mechanic (check_immediate_entry_trigger, the plain 2% price
trigger) and the two production fixes that gated it end-to-end --
Fix 2 (is_entry_within_cutoff, a hard 14:30 IST entry cutoff) and Fix 3
(is_min_hold_satisfied, a 10-minute minimum hold before an "oi_swing_exit"
could fire) -- were removed along with entry_exit_mode="oi_swing_v1"
itself. (Fix 1, the hard risk-cap re-enablement, was engine.py-only wiring
and never had any code in this module either way.) Only the pure OI-swing
STRUCTURE/breakout functions below survive, kept for reuse by the new
entry/exit design's own Section 16.
"""
from __future__ import annotations

from datetime import datetime, timedelta, time as dtime
from typing import List, Optional, Tuple

BUCKET_MINUTES = 5
SESSION_START = dtime(9, 15)


def floor_to_bucket(ts: datetime, bucket_minutes: int = BUCKET_MINUTES,
                     session_start: dtime = SESSION_START) -> datetime:
    """Session-anchored 5-min bucket floor (09:15, 09:20, 09:25, ...) --
    same anchoring convention as the validated backtest's own
    resample_5min(), so a live 5-min series lines up with what was
    actually backtested rather than a midnight-aligned bucket."""
    anchor = ts.replace(hour=session_start.hour, minute=session_start.minute,
                         second=0, microsecond=0)
    if ts < anchor:
        anchor -= timedelta(days=1)
    elapsed_min = int((ts - anchor).total_seconds() // 60)
    bucket_idx = elapsed_min // bucket_minutes
    return anchor + timedelta(minutes=bucket_idx * bucket_minutes)


def update_swing_state(oi_series: List[float], swing_high: Optional[float],
                        swing_low: Optional[float],
                        ) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """Step 3/4 -- the plain 3-point immediate-neighbor swing rule,
    confirmed exactly one bar late. oi_series is the FULL real-time
    series so far (oldest-first); only the last 3 points are ever
    inspected (the newest point is what makes the middle one "confirmed").
    Returns (new_swing_high, new_swing_low, confirmed) where confirmed is
    "HIGH"/"LOW"/None. swing_high/swing_low carry forward UNCHANGED when
    nothing new confirms this call (the "latest CONFIRMED" level is a
    ratchet -- it only ever advances forward in time, never resets)."""
    n = len(oi_series)
    if n < 3:
        return swing_high, swing_low, None
    prev2, prev1, cur = oi_series[-3], oi_series[-2], oi_series[-1]
    if prev1 > prev2 and prev1 > cur:
        return prev1, swing_low, "HIGH"
    if prev1 < prev2 and prev1 < cur:
        return swing_high, prev1, "LOW"
    return swing_high, swing_low, None


def check_oi_swing_breakout(side: str, current_oi: float, swing_high: Optional[float],
                             swing_low: Optional[float], price_prev: float, price_cur: float,
                             ) -> Tuple[bool, Optional[str]]:
    """Step 4 -- the core HOLD/EXIT decision matrix (sections 5-9 of the
    user's own spec, both directions confirmed symmetric). Returns
    (breakout_happened, decision) -- decision is None iff breakout_
    happened is False (sections 10-11: no reaction at all to a non-
    breakout bar). Flat price (price_cur == price_prev) at a genuine
    breakout bar defaults to EXIT for both sides -- see this module's own
    docstring for the rationale; flip the two `else` branches below if
    that default is ever revisited."""
    broke_high = swing_high is not None and current_oi > swing_high
    broke_low = swing_low is not None and current_oi < swing_low
    if not (broke_high or broke_low):
        return False, None
    if side == "CALL":
        decision = "HOLD" if price_cur > price_prev else "EXIT"   # DOWN or FLAT -> EXIT
    else:
        decision = "HOLD" if price_cur < price_prev else "EXIT"   # UP or FLAT -> EXIT
    return True, decision
