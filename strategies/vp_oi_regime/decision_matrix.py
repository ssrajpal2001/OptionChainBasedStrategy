"""The 27-row Future-OI/Put-OI/Call-OI decision matrix, verbatim from the
user's spec, plus the premium-match hedge-sizing correction confirmed this
session (15-20%/5% of the EXITED/SURVIVING leg's own entry premium -- not a
same-strike re-buy), the Re-entry rule, and the Hedge-Exit (9-EMA trailing
stop / Value-Area-re-entry fakeout) rule.

Pure functions only -- no I/O, no live wiring. Consumed by the backtest
harness today; a future, separate decision would wire this into
strategies/sell_straddle/exits.py's own hedge-build logic.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

Trend = str  # "Rise" | "Fall" | "NoChange"


@dataclass
class HedgeSpend:
    enabled: bool
    pct_of_premium: Optional[Tuple[float, float]]  # (lo, hi) fraction, e.g. (0.15, 0.20)


@dataclass
class DecisionResult:
    regime: str
    call_action: str
    put_action: str
    call_hedge: HedgeSpend
    put_hedge: HedgeSpend
    algo_sr_trigger: str
    reentry_rule: str
    hedge_exit_rule: str


_NO_HEDGE = HedgeSpend(enabled=False, pct_of_premium=None)
_HEDGE_5 = HedgeSpend(enabled=True, pct_of_premium=(0.05, 0.05))
_HEDGE_15_20 = HedgeSpend(enabled=True, pct_of_premium=(0.15, 0.20))
_HEDGE_50 = HedgeSpend(enabled=True, pct_of_premium=(0.50, 0.50))
_HEDGE_70 = HedgeSpend(enabled=True, pct_of_premium=(0.70, 0.70))

_BEARISH_SR = (
    "Price below POC, testing VAL. Real breakout confirmed by volume "
    "acceptance in LVN below VAL."
)
_BULLISH_SR = (
    "Price above POC, testing VAH. Real breakout confirmed by volume "
    "acceptance in LVN above VAH."
)
_NEUTRAL_SR = (
    "Price inside Value Area, POC acting as magnet. Fakeouts rejected "
    "back into VA."
)
_VOLATILE_SR = (
    "Price traversing Value Area rapidly. POC rejection, HVN chopping. "
    "Watch for fakeouts at VAH/VAL."
)
_NA_REENTRY = "N/A - Leg not exited"
_NA_HEDGE_EXIT = "N/A"

# 2026-10-04, authoritative source document (38-entry workbook PDF): the
# re-entry/hedge-exit text is NOT uniform across every Highly-Bearish or
# Highly-Bullish row the way the earlier chat-pasted table implied -- the
# source literally uses different reference levels per observation (POC vs
# VAL vs VAH) and the "Buildup" rows (Future OI Rising) add an extra 10-min
# OI-flip re-entry clause the "Unwinding/Covering" rows (Future OI Falling)
# don't have. Kept verbatim per-row rather than forcing false consistency.
_PUT_REENTRY_BUILDUP = (
    "Re-enter Short Put ONLY IF price is rejected from LVN and closes back "
    "above POC (Fake Breakdown confirmed). ALSO re-enter if the OI logic "
    "changes to Bullish within 10 minutes."
)
_CALL_REENTRY_BUILDUP = (
    "Re-enter Short Call ONLY IF price is rejected from LVN and closes back "
    "below POC (Fake Breakout confirmed)."
)
# 2026-10-04 SECOND correction: Obs 23/24 now reuse _PUT_REENTRY_BUILDUP/
# _CALL_REENTRY_BUILDUP (Obs 5/6's own POC-based wording) rather than their
# own VAL/VAH-based wording -- "identical to Obs 5/6" per direct user spec
# means the whole row, not just the hedge split. The original VAL/VAH-based
# constants this replaced are preserved in git history if ever needed.
_PUT_HEDGE_EXIT = (
    "If Short Leg is open: Close hedge ONLY when short straddle is closed. "
    "If Short Leg is EXITED (Naked Long Put): Ride downside breakout using "
    "9-EMA trailing stop. EXIT IMMEDIATELY if price re-enters Value Area "
    "(Fakeout)."
)
_CALL_HEDGE_EXIT = (
    "If Short Leg is open: Close hedge ONLY when short straddle is closed. "
    "If Short Leg is EXITED (Naked Long Call): Ride upside breakout using "
    "9-EMA trailing stop. EXIT IMMEDIATELY if price re-enters Value Area "
    "(Fakeout)."
)
_VOLATILE_HEDGE_EXIT = (
    "If Short Leg is open: Close hedge ONLY when short straddle is closed. "
    "If Short Leg is EXITED (Naked Long): Ride breakout using 9-EMA "
    "trailing stop. EXIT IMMEDIATELY if price re-enters Value Area "
    "(Fakeout)."
)
_WAIT_NEW_POC = "Wait for new POC (Point of Control) formation before re-entering."

_HOLD_BOUNCE = "Hold legs, expect bounce"
_HOLD_REJECTION = "Hold legs, expect rejection"


def _hold_row(regime: str, sr: str) -> DecisionResult:
    return DecisionResult(
        regime=regime, call_action=_HOLD_BOUNCE, put_action=_HOLD_REJECTION,
        call_hedge=_NO_HEDGE, put_hedge=_NO_HEDGE, algo_sr_trigger=sr,
        reentry_rule=_NA_REENTRY, hedge_exit_rule=_NA_HEDGE_EXIT,
    )


def _highly_bearish_row(regime: str, call_hedge: HedgeSpend, put_hedge: HedgeSpend,
                         reentry: str) -> DecisionResult:
    """2026-10-04 direct user correction: Obs 5 and Obs 23 use the SAME
    hedge split regardless of Future OI direction -- surviving Call leg
    gets NOTHING, exited Put leg gets 70%. (An earlier revision gave Obs 23
    a 15-20%/5% split; that was wrong and has been removed.)"""
    return DecisionResult(
        regime=regime, call_action="Shift put to OTM", put_action="Exit put leg",
        call_hedge=call_hedge, put_hedge=put_hedge,
        algo_sr_trigger=(
            "Price breaks below VAL. Volume acceptance in LVN below VAL "
            "confirms trend."
        ),
        reentry_rule=reentry, hedge_exit_rule=_PUT_HEDGE_EXIT,
    )


def _highly_bullish_row(regime: str, call_hedge: HedgeSpend, put_hedge: HedgeSpend,
                         reentry: str) -> DecisionResult:
    """2026-10-04 direct user correction: Obs 6 and Obs 24 use the SAME
    hedge split regardless of Future OI direction -- exited Call leg gets
    70%, surviving Put leg gets NOTHING. (An earlier revision gave Obs 24 a
    15-20%/5% split; that was wrong and has been removed.)"""
    return DecisionResult(
        regime=regime, call_action="Exit call leg", put_action="Shift call to OTM",
        call_hedge=call_hedge, put_hedge=put_hedge,
        algo_sr_trigger=(
            "Price breaks above VAH. Volume acceptance in LVN above VAH "
            "confirms trend."
        ),
        reentry_rule=reentry, hedge_exit_rule=_CALL_HEDGE_EXIT,
    )


def _volatile_row(call_hedge: HedgeSpend, put_hedge: HedgeSpend) -> DecisionResult:
    """2026-10-04 direct user correction: ALL THREE Volatile rows (Future OI
    Rise=Obs 9, No Change=Obs 18, Fall=Obs 27) use the SAME 50%/50% hedge
    split. (An earlier revision gave Obs 27 a 15-20%/15-20% split; that was
    wrong and has been removed -- Future OI direction does not change
    hedge sizing for this regime.)"""
    return DecisionResult(
        regime="Volatile", call_action="Buy put hedge", put_action="Buy call hedge",
        call_hedge=call_hedge, put_hedge=put_hedge,
        algo_sr_trigger=_VOLATILE_SR, reentry_rule=_WAIT_NEW_POC,
        hedge_exit_rule=_VOLATILE_HEDGE_EXIT,
    )


# (future_oi, put_oi, call_oi) -> DecisionResult, all 27 rows verbatim from spec.
_MATRIX: Dict[Tuple[Trend, Trend, Trend], DecisionResult] = {
    ("Rise", "Fall", "No Change"): _hold_row("Bearish", _BEARISH_SR),
    ("Rise", "No Change", "Rise"): _hold_row("Bearish", _BEARISH_SR),
    ("Rise", "Rise", "No Change"): _hold_row("Bullish", _BULLISH_SR),
    ("Rise", "No Change", "Fall"): _hold_row("Bullish", _BULLISH_SR),
    ("Rise", "Fall", "Rise"): _highly_bearish_row(
        "Highly Bearish (Short Buildup)", _NO_HEDGE, _HEDGE_70, _PUT_REENTRY_BUILDUP),
    ("Rise", "Rise", "Fall"): _highly_bullish_row(
        "Highly Bullish (Long Buildup)", _HEDGE_70, _NO_HEDGE, _CALL_REENTRY_BUILDUP),
    ("Rise", "No Change", "No Change"): _hold_row("Neutral", _NEUTRAL_SR),
    ("Rise", "Rise", "Rise"): _hold_row("Neutral / Rangebound", _NEUTRAL_SR),
    ("Rise", "Fall", "Fall"): _volatile_row(_HEDGE_50, _HEDGE_50),

    ("No Change", "Fall", "Rise"): _hold_row("Bearish", _BEARISH_SR),
    ("No Change", "Rise", "Fall"): _hold_row("Bullish", _BULLISH_SR),
    ("No Change", "Fall", "No Change"): _hold_row("Mildly Bearish", _BEARISH_SR),
    ("No Change", "No Change", "Rise"): _hold_row("Mildly Bearish", _BEARISH_SR),
    ("No Change", "Rise", "No Change"): _hold_row("Mildly Bullish", _BULLISH_SR),
    ("No Change", "No Change", "Fall"): _hold_row("Mildly Bullish", _BULLISH_SR),
    ("No Change", "No Change", "No Change"): _hold_row("Neutral", _NEUTRAL_SR),
    ("No Change", "Rise", "Rise"): _hold_row("Neutral / Rangebound", _NEUTRAL_SR),
    ("No Change", "Fall", "Fall"): _volatile_row(_HEDGE_50, _HEDGE_50),

    ("Fall", "Fall", "No Change"): _hold_row("Bearish (Long Unwinding)", _BEARISH_SR),
    ("Fall", "No Change", "Rise"): _hold_row("Bearish (Long Unwinding)", _BEARISH_SR),
    ("Fall", "Rise", "No Change"): _hold_row("Bullish (Short Covering)", _BULLISH_SR),
    ("Fall", "No Change", "Fall"): _hold_row("Bullish (Short Covering)", _BULLISH_SR),
    # 2026-10-04, direct user correction: Obs 23 must behave IDENTICALLY to
    # Obs 5 (same hedge split: surviving Call leg gets NOTHING, exited Put
    # leg gets 70%) -- NOT the earlier 15-20%/5% split. Future-OI direction
    # (Rise vs Fall) no longer changes the hedge sizing for this regime.
    # 2026-10-04 SECOND correction: "identical to Obs 5" means the WHOLE row's
    # mechanic, not just the hedge split -- the Re-entry Rule also now uses
    # Obs 5's own POC-based wording (+ the 10-min OI-flip clause), not Obs
    # 23's original VAL-based wording.
    ("Fall", "Fall", "Rise"): _highly_bearish_row(
        "Highly Bearish (Long Unwinding)", _NO_HEDGE, _HEDGE_70, _PUT_REENTRY_BUILDUP),
    # 2026-10-04 SECOND correction: same for Obs 24 -- now uses Obs 6's own
    # POC-based Re-entry Rule wording, not Obs 24's original VAH-based one.
    ("Fall", "Rise", "Fall"): _highly_bullish_row(
        "Highly Bullish (Short Covering)", _HEDGE_70, _NO_HEDGE, _CALL_REENTRY_BUILDUP),
    ("Fall", "No Change", "No Change"): _hold_row("Neutral", _NEUTRAL_SR),
    ("Fall", "Rise", "Rise"): _hold_row("Neutral / Rangebound", _NEUTRAL_SR),
    # 2026-10-04, direct user correction: Obs 27 must behave IDENTICALLY to
    # Obs 9 (50%/50%) -- NOT 15-20%/15-20%. All three Volatile rows (9, 18,
    # 27) now share one hedge sizing regardless of Future OI direction.
    ("Fall", "Fall", "Fall"): _volatile_row(_HEDGE_50, _HEDGE_50),
}

assert len(_MATRIX) == 27, f"expected exactly 27 matrix rows, got {len(_MATRIX)}"


def decide(future_oi_trend: Trend, put_oi_trend: Trend, call_oi_trend: Trend) -> DecisionResult:
    """Pure lookup against the 27-row matrix, verbatim from spec."""
    key = (future_oi_trend, put_oi_trend, call_oi_trend)
    if key not in _MATRIX:
        raise ValueError(f"no matrix row for {key} -- trends must be Rise/Fall/'No Change'")
    return _MATRIX[key]


def hedge_target_premium(hedge: HedgeSpend, exited_leg_entry_premium: float) -> Optional[Tuple[float, float]]:
    """Premium-match target band for the hedge/flip strike, per this
    session's confirmed correction: the new long's own live premium should
    cost pct_of_premium% of the EXITED (or surviving, for the 5% leg) leg's
    own ENTRY premium -- NOT a same-strike re-buy at the current (much
    higher) price."""
    if not hedge.enabled or hedge.pct_of_premium is None:
        return None
    lo_pct, hi_pct = hedge.pct_of_premium
    return (lo_pct * exited_leg_entry_premium, hi_pct * exited_leg_entry_premium)


def pick_strike_by_premium_match(
    chain: Dict[int, float], target_lo: float, target_hi: float,
) -> Optional[Tuple[int, float]]:
    """Scans a {strike: live_premium} chain and returns the strike whose own
    premium falls inside [target_lo, target_hi]; if none land exactly
    inside, falls back to the strike with the highest premium that is still
    <= target_hi (closest-from-below, never overshoot into a strike that
    costs MORE than the hedge is supposed to spend)."""
    mid = (target_lo + target_hi) / 2.0
    inside = [(k, v) for k, v in chain.items() if target_lo <= v <= target_hi]
    if inside:
        return min(inside, key=lambda kv: abs(kv[1] - mid))
    below = [(k, v) for k, v in chain.items() if v <= target_hi]
    if below:
        return max(below, key=lambda kv: kv[1])
    return None


class NineEmaTrailingStop:
    """9-period EMA trailing stop on a naked long's own live price (per the
    Hedge Exit Rule: 'Ride breakout using 9-EMA trailing stop'). Tracks the
    EMA of the long's own price series; the stop is the EMA itself -- a
    close back through the EMA (against the trade's direction) exits."""

    def __init__(self, period: int = 9) -> None:
        self.period = period
        self._k = 2.0 / (period + 1)
        self._ema: Optional[float] = None

    def reset(self) -> None:
        self._ema = None

    def update(self, price: float) -> float:
        self._ema = price if self._ema is None else (price - self._ema) * self._k + self._ema
        return self._ema

    def stop_hit(self, price: float, direction: str) -> bool:
        """direction='long' (naked long call/put riding a breakout, exits on
        the underlying falling through the EMA) -- stop_hit when price <
        EMA. The matrix's long positions are always price-direction trades
        (naked long call rides price UP, naked long put rides price DOWN),
        so 'direction' here distinguishes which side of the EMA is the
        trail: 'up' for a long call (exit on a close below EMA) and 'down'
        for a long put (exit on a close above EMA)."""
        if self._ema is None:
            return False
        if direction == "up":
            return price < self._ema
        if direction == "down":
            return price > self._ema
        raise ValueError("direction must be 'up' or 'down'")
