"""
strategies/oi_orb_screener/oi_swing.py -- pure, unit-tested decision logic
for the "Future OI-Price Swing Breakout Strategy" entry/exit mode
(entry_exit_mode="oi_swing_v1" in strategy_params).

Ported from this session's own real-data backtest series
(scripts/oi_orb_futures_oi_swing_management_backtest.py +
scripts/oi_orb_swing_optimization_variants.py, validated across 13 real
trading days once a real prev-close date-anchoring bug in that backtest
was found and fixed) -- NOT reimplemented from scratch. This module
intentionally reuses ONLY the plain, spec-exact 3-point swing rule (no
gap-amplitude filter, no 5-point lookback) -- those were exploratory
optimization-sweep variants, never approved for production; production
ships the mechanic exactly as the user's own spec describes it, plus the
three specific fixes below (independently validated as net-positive on
the same 13-day sweep).

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

======================= THE THREE PRODUCTION FIXES =======================
(validated on the same 13-day backtest sweep -- see CLAUDE.md-adjacent
session notes / the sweep script's own module docstring for the full
comparison numbers; NOT optional, all three ship together)

  Fix 1 -- hard risk-cap check at (effectively continuous, real option-
    tick) cadence, fully independent of the 5-min OI-swing loop. Reuses
    the engine's EXISTING OiOrbScreenerStrategy._check_hard_risk_cap +
    strategies.core.support_resistance._MAX_RISK_RS_PER_LOT (Rs2000/lot)
    -- not reimplemented, not redefined. This module has no code for it;
    engine.py simply re-enables that existing method, gated to this mode.
  Fix 2 -- ENTRY_WINDOW_END-equivalent hard cutoff: no NEW entry may open
    after 14:30 IST (is_entry_within_cutoff below). The raw backtest had
    NO cutoff at all -- confirmed a real bug (PREMIERENE entered at 15:28
    on 2026-09-17, 13 minutes before its own 15:15 EOD square-off).
  Fix 3 -- 10-minute minimum hold after entry before an "oi_swing_exit"
    is allowed to fire (is_min_hold_satisfied below). The hard risk cap
    and EOD square-off are explicitly NOT subject to this minimum -- they
    can fire at any time, including inside the first 10 minutes.
"""
from __future__ import annotations

from datetime import datetime, time as dtime, timedelta
from typing import List, Optional, Tuple

PRICE_TRIGGER_PCT_DEFAULT = 2.0
ENTRY_CUTOFF_DEFAULT = dtime(14, 30)
MIN_HOLD_MINUTES_DEFAULT = 10
BUCKET_MINUTES = 5
SESSION_START = dtime(9, 15)


def check_immediate_entry_trigger(pchange: float, min_pct: float = PRICE_TRIGGER_PCT_DEFAULT
                                   ) -> Optional[str]:
    """Step 2 -- the ENTRY mechanic, unchanged/unreplaced by anything OI-
    related. Returns "CALL" if pchange has crossed +min_pct, "PUT" if it
    has crossed -min_pct, else None (no trigger yet). Mirrors the real
    backtest's own _find_price_trigger threshold check and screener.
    side_from_pchange's CALL/PUT convention exactly -- callers evaluate
    this on every live price update; the first call that returns non-None
    IS the trigger (the live engine's own "first crossing" semantics,
    since it re-evaluates on live ticks rather than replaying historical
    bars)."""
    if pchange >= min_pct:
        return "CALL"
    if pchange <= -min_pct:
        return "PUT"
    return None


def is_entry_within_cutoff(now: dtime, cutoff: dtime = ENTRY_CUTOFF_DEFAULT) -> bool:
    """Fix 2. True iff a NEW entry may still open at this wall-clock time."""
    return now <= cutoff


def is_min_hold_satisfied(entry_ts: datetime, now: datetime,
                           min_hold_minutes: int = MIN_HOLD_MINUTES_DEFAULT) -> bool:
    """Fix 3. True iff at least min_hold_minutes have elapsed since entry --
    ONLY gates an "oi_swing_exit" decision; never called for the hard risk
    cap or EOD square-off paths (those remain unconditional, per spec)."""
    if entry_ts is None:
        return True   # no known entry time -- fail open rather than block a real exit forever
    return now >= entry_ts + timedelta(minutes=min_hold_minutes)


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
