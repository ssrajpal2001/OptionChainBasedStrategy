"""
strategies/cag_straddle/detector.py — pure S&R breach/standing-order logic
for the CAG Long Straddle strategy (2026-08-27, 8th standalone strategy,
explicit exception to CLAUDE.md's prior 7-strategy cap -- see CLAUDE.md's
own CAG Straddle section for the full mechanic history).

Reuses the REAL, already-validated strategies/d1_trap_option/support_
resistance.py SupportResistanceCalculator (platform infra already reused by
D1TrapSRBook/PositionalSRTracker/SRPingPongTracker) -- never reimplements
the ping-pong state machine itself. Everything else here (Bar/
BarAccumulator, the standing-order signal mechanic, strike selection) is a
fresh, independent implementation per this codebase's zero-shared-runtime
mandate for new strategies.

Mechanic (validated via scripts/nifty_1500_sr_breakout_backtest.py against
real NIFTY option premium history, refined through several rounds of direct
user review of real minute-by-minute charts -- see that script's own module
docstring for the full correction history this mirrors exactly):
  1. At the entry-window start (default 15:00 IST), CE and PE each get a
     FRESH SupportResistanceCalculator -- the first bar fed is genuinely
     Phase 0's own first candle, no pre-window history carried in (a real,
     user-caught bug: carrying pre-15:00 history in let a bar AT 15:00
     already read as mid-cycle, which is impossible for a genuine breach
     that early).
  2. ENTRY signal: a phase transition from S2_TRACKING/R2_TRACKING back
     into R1_TRACKING ("R2 breaches R1") arms a STANDING order at the
     breaching bar's own HIGH. The order stays live across as many later
     bars as it takes -- the first later bar whose own HIGH exceeds it
     fills the order (entry price = the armed level). is_established is
     explicitly NOT a precondition (that flag legitimately reads False
     right after this exact promotion -- normal internal bookkeeping, not
     evidence the breach didn't happen; requiring it was a real bug caught
     against a real chart).
  3. Once filled, only S1/S2 are watched (R1/R2 no longer matter for that
     side until flat again). SL mirrors the entry mechanic exactly: a bar
     closing below the pre-bar S1 arms a standing SL order at that bar's
     own LOW; the first later bar whose LOW breaches it exits -- this is
     "trailing SL as S1 itself" (S1 keeps evolving as the calculator's own
     ping-pong state advances).
  4. Force-exit at the entry-window end (default 15:35 IST) regardless.
  5. If a trade exits via SL (not EOD), scanning resumes on BOTH sides
     again from that point onward for the next confirmed signal --
     multiple sequential trades per day are expected, not capped at one.

SL/target are OPTION-PREMIUM levels here (S&R runs directly on each side's
own premium chart, not spot) -- see engine.py's own module docstring.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from strategies.core.support_resistance import SupportResistanceCalculator

# The strict ping-pong sense of "R1/S1 is breached" -- a phase transition
# FROM one of these secondary-tracking phases INTO R1_TRACKING/S1_TRACKING.
# Deliberately excludes the very first INITIAL_TREND_ESTABLISHMENT ->
# R1_TRACKING/S1_TRACKING transition (a fresh base-candle breakout is NOT
# the same event -- direct user clarification).
_BREACH_FROM_PHASES = ("S2_TRACKING", "R2_TRACKING")


@dataclass
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float


class BarAccumulator:
    """Buckets a live ltp tick stream into 1-min bars. Self-contained,
    mirrors every other strategy's own independently-written BarAccumulator
    (e.g. strategies/liquidity_trap/detector.py) -- not imported."""

    def __init__(self) -> None:
        self.bars: List[Bar] = []
        self._bucket_open_ts: Optional[datetime] = None
        self._bucket: Optional[Bar] = None

    def on_tick(self, ts: datetime, ltp: float) -> Optional[Bar]:
        """Feed one tick. Returns the closed Bar the moment a new minute
        starts, else None (the still-forming bucket is not returned)."""
        bucket_ts = ts.replace(second=0, microsecond=0)
        if self._bucket_open_ts is None:
            self._bucket_open_ts = bucket_ts
            self._bucket = Bar(ts=bucket_ts, open=ltp, high=ltp, low=ltp, close=ltp)
            return None
        if bucket_ts != self._bucket_open_ts:
            closed = self._bucket
            self.bars.append(closed)
            self._bucket_open_ts = bucket_ts
            self._bucket = Bar(ts=bucket_ts, open=ltp, high=ltp, low=ltp, close=ltp)
            return closed
        self._bucket.high = max(self._bucket.high, ltp)
        self._bucket.low = min(self._bucket.low, ltp)
        self._bucket.close = ltp
        return None


class SideTracker:
    """One instance per side (CE or PE), constructed FRESH at the entry-
    window start each day (see module docstring point 1) -- never reused
    across days or re-seeded from earlier history. Feed closed 1-min bars
    via on_bar() in timestamp order; the returned dict tells the caller
    whether a breach event happened this bar. check_entry_fill()/
    check_sl_fill() then check/arm the standing order using that same
    bar's own high/low."""

    def __init__(self) -> None:
        self._calc = SupportResistanceCalculator()
        self._pending_entry: Optional[dict] = None   # {"level": float, "ts": datetime}
        self._pending_sl: Optional[dict] = None
        self.last_r1: Optional[float] = None
        self.last_s1: Optional[float] = None
        self.last_phase: str = "UNKNOWN"

    def on_bar(self, bar: Bar) -> dict:
        state_before = self._calc.get_calculated_sr_state("OPT")
        levels_before = state_before.get("sr_levels") or {}
        r1_before = (levels_before.get("R1") or {}).get("high")
        s1_before = (levels_before.get("S1") or {}).get("low")
        phase_before = state_before.get("current_phase", "UNKNOWN")

        candle = {"timestamp": bar.ts, "high": bar.high, "low": bar.low, "duration": 1}
        self._calc.process_straddle_candle("OPT", candle, silent=True)
        phase_after = self._calc.get_calculated_sr_state("OPT").get("current_phase", "UNKNOWN")

        self.last_r1 = r1_before
        self.last_s1 = s1_before
        self.last_phase = phase_after

        r1_breach_event = phase_before in _BREACH_FROM_PHASES and phase_after == "R1_TRACKING"
        s1_breach_event = phase_before in _BREACH_FROM_PHASES and phase_after == "S1_TRACKING"
        return {
            "r1_before": r1_before, "s1_before": s1_before,
            "phase_before": phase_before, "phase_after": phase_after,
            "r1_breach_event": r1_breach_event, "s1_breach_event": s1_breach_event,
        }

    def check_entry_fill(self, bar: Bar, r1_breach_event: bool) -> Optional[float]:
        """Standing-order entry check -- call once per bar, AFTER on_bar().
        Returns the fill price if this bar fills a standing entry order,
        else None. Arms/replaces the standing order if r1_breach_event
        fired on this same bar (order level = this bar's own high)."""
        filled = None
        if self._pending_entry is not None and bar.high > self._pending_entry["level"]:
            filled = self._pending_entry["level"]
            self._pending_entry = None
        if r1_breach_event:
            self._pending_entry = {"level": bar.high, "ts": bar.ts}
        return filled

    def check_sl_fill(self, bar: Bar, s1_before: Optional[float]) -> Optional[float]:
        """Standing-order SL check -- call once per bar while a position is
        open on this side, AFTER on_bar(). Returns the fill price if this
        bar fills a standing SL order, else None."""
        filled = None
        if self._pending_sl is not None and bar.low < self._pending_sl["low"]:
            filled = self._pending_sl["low"]
            self._pending_sl = None
        if s1_before is not None and bar.close < s1_before:
            self._pending_sl = {"low": bar.low, "ts": bar.ts}
        return filled


def pick_strike(candidates: Dict[int, float], target: float) -> Optional[int]:
    """Pick whichever candidate strike's live premium is closest to
    `target`. candidates: {strike: live_premium}. None if candidates is
    empty (e.g. nothing in the requested band has ticked yet)."""
    if not candidates:
        return None
    return min(candidates, key=lambda s: abs(candidates[s] - target))
