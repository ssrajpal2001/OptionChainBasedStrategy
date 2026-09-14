"""strategies/iron_fly/detector.py -- pure Iron Condor -> Iron Fly logic for
the NIFTY Weekly Iron Condor -> Iron Fly strategy (approved plan, 2026-09-13).

Fully standalone, per this codebase's zero-shared-runtime mandate for new
strategies -- no state, no I/O, no imports from any other strategy package.
Every function here is deterministic and independently testable; engine.py
is the only caller, wiring these against live/replayed ticks.

Mechanic (see the approved plan / docs/IRON_CONDOR_FLY_CLIENT_GUIDE.md for
the full client-facing spec -- this docstring only summarizes):
  - OTM1 = 50 NIFTY points, adjustment distance = 100 NIFTY points, profit
    target = 65% of a cycle's own expected max profit (all defaults,
    overridable per-deployment).
  - Initial entry: sell the strike farthest from ATM whose LTP is still
    > Rs20 (find_short_strike), buy the first strike beyond it whose LTP
    is < Rs20 (find_long_strike), on both Call and Put sides.
  - +/-100 roll: classify_move() names which side (if any) needs rolling
    and flags whether the move was a "gap" (overshot by a full extra
    increment) -- a gap arms a PendingAdjustment at the ORIGINAL trigger
    price instead of rolling immediately at the overshot price;
    check_pending_fire() reports when price has genuinely retraced back to
    that trigger.
  - Iron Fly conversion: check_atm_conversion() detects a short strike
    becoming the live ATM strike; reconcile_protective_leg() decides
    whether the existing protective long on that side is already the
    correct OTM1 strike or needs replacing. Once converted, a side is
    permanently done with the +/-100 roll (per the approved plan -- no
    fly->condor reversion exists in the source doc).
  - Cycle profit tracking: expected_max_profit() is computed ONCE at cycle
    start (net credit received x qty) and never recalculated; cycle_pnl()
    combines realized P&L from legs already closed this cycle with the
    live mark-to-market of whatever legs are still open;
    profit_target_hit() compares that against the frozen target.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple


def round_to_atm(spot: float, strike_step: float) -> int:
    return int(round(spot / strike_step) * strike_step)


def find_short_strike(
    strikes_ordered: Sequence[int], premiums: Dict[int, float], threshold: float = 20.0
) -> Optional[int]:
    """`strikes_ordered` must be sorted nearest-ATM-first, outward in one
    direction. Returns the farthest strike in that list whose premium is
    still > threshold, or None if even the nearest strike doesn't qualify.
    Stops (returning whatever was found so far) the moment premium data is
    missing for a candidate, since further strikes can't be judged without it.
    """
    result: Optional[int] = None
    for strike in strikes_ordered:
        premium = premiums.get(strike)
        if premium is None:
            break
        if premium > threshold:
            result = strike
        else:
            break
    return result


def find_long_strike(
    strikes_ordered: Sequence[int], premiums: Dict[int, float], threshold: float = 20.0
) -> Optional[int]:
    """`strikes_ordered` continues outward past the short strike. Returns the
    first strike whose premium is < threshold, or None if data runs out (or
    the whole passed sequence is exhausted) before finding one."""
    for strike in strikes_ordered:
        premium = premiums.get(strike)
        if premium is None:
            return None
        if premium < threshold:
            return strike
    return None


def classify_move(
    price: float, reference: float, distance: float = 100.0
) -> Tuple[Optional[str], bool]:
    """Returns (side, is_gap). side is "CALL" if price fell past the call
    adjustment trigger (reference-distance), "PUT" if it rose past the put
    trigger (reference+distance), else None. is_gap is True when the move
    overshot by a full extra increment (skipped the exact trigger level
    without a tick landing near it first) -- the doc's own gap-down/gap-up
    example."""
    if price <= reference - distance:
        return "CALL", price <= reference - 2 * distance
    if price >= reference + distance:
        return "PUT", price >= reference + 2 * distance
    return None, False


@dataclass
class PendingAdjustment:
    side: str  # "CALL" or "PUT"
    trigger_price: float


def check_pending_fire(pending: PendingAdjustment, price: float) -> bool:
    """A gapped-past CALL adjustment fires once price rises back UP to the
    original trigger; a gapped-past PUT adjustment fires once price falls
    back DOWN to it."""
    if pending.side == "CALL":
        return price >= pending.trigger_price
    return price <= pending.trigger_price


def check_atm_conversion(
    nifty_price: float,
    short_ce_strike: Optional[int],
    short_pe_strike: Optional[int],
) -> Optional[str]:
    """Returns "PUT_BECOMES_ATM" if NIFTY has reached OR FALLEN THROUGH the
    short PE strike (falling-market conversion, doc Point 8), "CALL_BECOMES_
    ATM" if NIFTY has reached OR RISEN THROUGH the short CE strike (rising-
    market conversion, doc Point 9), else None.

    Direct user spec (2026-09-14, "gap-through rule"): this is an AT-OR-
    BEYOND comparison against the raw spot price, NOT an exact ATM-rounding
    match. A gap that skips straight past a sold strike without a tick ever
    landing exactly on it (e.g. sold call=25,000, NIFTY opens at 25,080)
    must still convert immediately -- an exact-equality check (the original
    implementation, checking round_to_atm(spot) == strike) would silently
    miss this, since the rounded ATM (25,100) never equals the sold strike
    (25,000). This at-or-beyond form is a strict superset of the old exact
    match, so ordinary gradual-approach conversions (doc Points 7/9) still
    fire at exactly the same moment they always did."""
    if short_pe_strike is not None and nifty_price <= short_pe_strike:
        return "PUT_BECOMES_ATM"
    if short_ce_strike is not None and nifty_price >= short_ce_strike:
        return "CALL_BECOMES_ATM"
    return None


def reconcile_protective_leg(
    short_strike: int, existing_long_strike: Optional[int], otm1: int, direction: int
) -> Tuple[bool, int]:
    """direction=+1 for the Call side (long strike sits above the short),
    -1 for the Put side (long strike sits below). Returns (needs_replace,
    correct_long_strike)."""
    correct = short_strike + direction * otm1
    return existing_long_strike != correct, correct


def expected_max_profit(
    short_ce_premium: float,
    long_ce_premium: float,
    short_pe_premium: float,
    long_pe_premium: float,
    qty: int,
) -> float:
    """Max profit of a credit spread = the net credit received, per the
    approved plan's Point 3/10 -- computed once at cycle start."""
    net_credit = (short_ce_premium - long_ce_premium) + (short_pe_premium - long_pe_premium)
    return net_credit * qty


@dataclass
class Leg:
    strike: int
    entry_price: float
    qty: int
    is_short: bool
    side: str  # "CE" or "PE" -- required so a live_premiums lookup can never
               # collide between a CE and PE leg that happen to share a
               # strike (the normal case once a position is Iron-Fly-
               # converted; a real bug, found via the first live backtest
               # run, when this dict was keyed by strike alone).


def leg_pnl(leg: Leg, live_price: float) -> float:
    if leg.is_short:
        return (leg.entry_price - live_price) * leg.qty
    return (live_price - leg.entry_price) * leg.qty


def cycle_pnl(
    realized_pnl: float, open_legs: Sequence[Leg], live_premiums: Dict[Tuple[int, str], float]
) -> float:
    """CURRENT_CYCLE_PNL = TOTAL_REALIZED_PNL + CURRENT_OPEN_POSITION_PNL,
    per the approved plan's Point 10/11. `live_premiums` is keyed by
    (strike, side) -- NOT strike alone, since a CE leg and a PE leg can
    share the same strike once a side has converted to Iron Fly."""
    open_pnl = sum(leg_pnl(leg, live_premiums[(leg.strike, leg.side)]) for leg in open_legs)
    return realized_pnl + open_pnl


def profit_target_hit(
    current_cycle_pnl: float, expected_max_profit: float, target_pct: float = 0.65
) -> bool:
    return current_cycle_pnl >= target_pct * expected_max_profit
