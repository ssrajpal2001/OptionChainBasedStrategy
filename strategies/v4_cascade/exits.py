"""
strategies/v4_cascade/exits.py — pure exit/risk management.

Tranche 1 (fixed): runs raw to a fixed 2R target OR the frozen HTF structural
floor SL. NO break-even trail for T1 — it is intentionally left uninhibited.

Tranche 2 (trailing): a 4x5m "Rolling Base" trailing stop — reuses the same
3-candle sweep+reclaim lock mechanism (TrackingZoneScanner with a single-step
ladder of just [5]) fed 5m bars; each time a NEW base locks, it's pushed onto
a 4-slot window and the trailing stop moves up to the newest locked base
(never trails down), mapped proportionally onto the execution contract the
same way entries.py maps SL/target.

Structural flip: if the OPPOSITE tracking contract's scanner independently
reaches RETEST_PENDING and its own pierce+spot-confirm conditions hold while
a position is live, close the live position immediately (this overrides T1/T2)
and — per the engine's contract — the caller (engine.py) evaluates whether to
immediately open the opposite side in the same update() call.

No bus/broker/DB dependency.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional

from strategies.v4_cascade.dataclasses import TrancheLeg
from strategies.v4_cascade.zone_state import TrackingZoneScanner


@dataclass(frozen=True)
class ExitCheck:
    hit: bool
    price: float = 0.0
    reason: str = ""


def check_t1(t1: TrancheLeg, bar, is_short: bool = False) -> ExitCheck:
    """Intrabar check against T1's fixed SL / 2R target using the bar's
    high/low. SL takes priority if both would be hit within the same bar
    (conservative). ``is_short`` mirrors the check for a short position (SL
    above entry, hit on the bar's HIGH; target below entry, hit on the
    bar's LOW) — crypto's PE side only; NIFTY (always long) is unaffected."""
    if is_short:
        if t1.sl_price and bar.high >= t1.sl_price:
            return ExitCheck(hit=True, price=t1.sl_price, reason="t1_sl_structural_floor")
        if t1.target_price and bar.low <= t1.target_price:
            return ExitCheck(hit=True, price=t1.target_price, reason="t1_target_2r")
        return ExitCheck(hit=False)
    if t1.sl_price and bar.low <= t1.sl_price:
        return ExitCheck(hit=True, price=t1.sl_price, reason="t1_sl_structural_floor")
    if t1.target_price and bar.high >= t1.target_price:
        return ExitCheck(hit=True, price=t1.target_price, reason="t1_target_2r")
    return ExitCheck(hit=False)


class TrailingBaseTracker:
    """T2's "4x5m Rolling Base" trailing stop. ``bear=True`` for a bought CE
    position (tracks CE-tracking-style floor bases); ``bear=False`` for PE."""

    def __init__(self, bear: bool, lookback_bases: int = 4) -> None:
        self._bear = bear
        self._scanner = TrackingZoneScanner(bear=bear, ladder=[5])
        self._locked_bases: Deque[float] = deque(maxlen=lookback_bases)
        self.current_stop: Optional[float] = None

    def on_5m_bar(self, bar) -> bool:
        """Feed one 5m bar. Returns True if the trailing stop moved up this call."""
        self._scanner.on_5m_bar(bar)
        moved = False
        if self._scanner.state.value == "retest_pending" and self._scanner.active_zone is not None:
            new_base = self._scanner.active_zone.entry_line
            # Immediately consume so the next call resumes scanning for the
            # NEXT new base rather than re-reporting this same one.
            self._scanner.consume(bar.timestamp)
            if new_base is not None:
                self._locked_bases.append(new_base)
                if self.current_stop is None or (
                    (self._bear and new_base > self.current_stop) or
                    (not self._bear and new_base < self.current_stop)
                ):
                    self.current_stop = new_base
                    moved = True
        return moved

    def check_hit(self, bar) -> ExitCheck:
        if self.current_stop is None:
            return ExitCheck(hit=False)
        if self._bear and bar.low <= self.current_stop:
            return ExitCheck(hit=True, price=self.current_stop, reason="t2_trailing_base_stop")
        if not self._bear and bar.high >= self.current_stop:
            return ExitCheck(hit=True, price=self.current_stop, reason="t2_trailing_base_stop")
        return ExitCheck(hit=False)

    def reset(self) -> None:
        self._scanner.reset()
        self._locked_bases.clear()
        self.current_stop = None


def map_trailing_stop_to_execution(
    tracking_stop: float, tracking_entry_price: float, exec_entry_price: float,
) -> float:
    """Proportional scale of a tracking-contract trailing-stop level onto the
    execution contract, same ratio approach as entries.compute_risk_mapping."""
    if tracking_entry_price <= 0:
        return exec_entry_price
    scale = exec_entry_price / tracking_entry_price
    return max(0.0, tracking_stop * scale)
