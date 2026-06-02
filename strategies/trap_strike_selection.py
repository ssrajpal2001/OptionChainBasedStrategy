"""
strategies/trap_strike_selection.py — Pure TT strike-selection logic.

Trap Trading tracks option contracts (CE + PE) chosen from the PREVIOUS day's
range and the days-to-expiry (DTE). Kept pure (no I/O, no engine state) so the
rules are unit-testable in isolation, mirroring straddle_selection.py.

Algorithm (authoritative — user spec 2026-06-02):
  1. ATM = round( (prev_day_high + prev_day_low) / 2 , step )   # day-fixed
  2. offset_steps by DTE (top-down):
         DTE > 5 -> 5 ;  > 4 -> 4 ;  > 3 -> 3 ;  > 2 -> 2 ;  > 1 -> 1 ;  else 0
     i.e. offset_steps = min(max(DTE - 1, 0), 5)
  3. offset is in STRIKE-STEP MULTIPLES:  offset_pts = offset_steps * step
         (CRUDEOIL step=100 -> DTE>5 = 500 ITM ; NIFTY step=50 -> DTE>5 = 250 ITM)
  4. Track:  CE = ATM - offset_pts  (ITM call) ,  PE = ATM + offset_pts  (ITM put)
"""

from __future__ import annotations

from dataclasses import dataclass

_MAX_OFFSET_STEPS = 5


@dataclass(frozen=True)
class TrapStrikes:
    atm: int            # day-fixed ATM (rounded to step)
    ce_strike: int      # ITM call strike to track  (ATM - offset)
    pe_strike: int      # ITM put strike to track   (ATM + offset)
    offset_steps: int   # number of strike-steps ITM
    offset_pts: int     # offset in points (offset_steps * step)
    dte: int            # days to expiry used


def dte_offset_steps(dte: int) -> int:
    """Strike-step offset for a given days-to-expiry, capped at 5 steps.

    DTE > 5 -> 5, > 4 -> 4, > 3 -> 3, > 2 -> 2, > 1 -> 1, else 0.
    Equivalent to min(max(dte - 1, 0), 5).
    """
    return min(max(int(dte) - 1, 0), _MAX_OFFSET_STEPS)


def prev_day_atm(prev_high: float, prev_low: float, step: float) -> int:
    """Day-fixed ATM = round( (prev_high + prev_low) / 2 ) to the nearest step."""
    if step <= 0:
        raise ValueError("step must be > 0")
    mid = (float(prev_high) + float(prev_low)) / 2.0
    return int(round(mid / step) * step)


def select_trap_strikes(
    prev_high: float, prev_low: float, dte: int, step: float
) -> TrapStrikes:
    """Pick the CE/PE ITM strikes to track for the day.

    CE (call) is ITM below the ATM; PE (put) is ITM above the ATM.
    Offset is expressed in strike-step multiples (per the user spec).
    """
    if step <= 0:
        raise ValueError("step must be > 0")
    atm = prev_day_atm(prev_high, prev_low, step)
    steps = dte_offset_steps(dte)
    offset_pts = int(steps * step)
    return TrapStrikes(
        atm=atm,
        ce_strike=atm - offset_pts,
        pe_strike=atm + offset_pts,
        offset_steps=steps,
        offset_pts=offset_pts,
        dte=int(dte),
    )
