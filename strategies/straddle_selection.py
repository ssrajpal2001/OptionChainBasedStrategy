"""
strategies/straddle_selection.py — pure candidate-selection math for the
sell-straddle. No async, no EventBus, no I/O. Exact port of the reference
Option_Selling_May_2026 sell_v3 entry_logic.py selection logic, restricted to
feed-available indicators (LTP + broker ATP = VWAP). Unit-testable in isolation.

Cache shape (built by the strategy from option ticks):
    strike_prem: Dict[Tuple[int, str], dict]   # (int strike, "CE"/"PE") -> {"ltp", "atp"}
    prev_atp_closed: Dict[Tuple[int, str], float]  # previous closed-candle ATP per leg
"""
from __future__ import annotations

from typing import Dict, Optional, Tuple

Key = Tuple[int, str]


def strip_intrinsic(ltp: float, side: str, strike: float, spot: float) -> float:
    """Time-value-only LTP. CE intrinsic = max(0, spot-strike); PE = max(0, strike-spot)."""
    if side == "CE":
        intrinsic = max(0.0, spot - strike)
    else:
        intrinsic = max(0.0, strike - spot)
    return ltp - intrinsic


def pair_indicators(
    strike_prem: Dict[Key, dict],
    prev_atp_closed: Dict[Key, float],
    ce_strike: int,
    pe_strike: int,
) -> Optional[Dict[str, float]]:
    """
    Per-pair indicators from feed data only:
      close = ce_ltp + pe_ltp
      vwap  = ce_atp + pe_atp          (broker ATP, never computed)
      slope = current combined VWAP - previous closed combined VWAP   (if both prev present)
    Returns None if either leg's LTP/ATP is missing or non-positive.
    'slope' key is omitted when either leg lacks a previous closed ATP.
    """
    ce = strike_prem.get((int(ce_strike), "CE"))
    pe = strike_prem.get((int(pe_strike), "PE"))
    if not ce or not pe:
        return None
    ce_ltp, ce_atp = ce.get("ltp", 0.0), ce.get("atp", 0.0)
    pe_ltp, pe_atp = pe.get("ltp", 0.0), pe.get("atp", 0.0)
    if ce_ltp <= 0 or pe_ltp <= 0 or ce_atp <= 0 or pe_atp <= 0:
        return None
    ind: Dict[str, float] = {
        "close": ce_ltp + pe_ltp,
        "vwap": ce_atp + pe_atp,
    }
    ce_prev = prev_atp_closed.get((int(ce_strike), "CE"))
    pe_prev = prev_atp_closed.get((int(pe_strike), "PE"))
    if ce_prev and pe_prev and ce_prev > 0 and pe_prev > 0:
        cur = ce_atp + pe_atp
        prev = ce_prev + pe_prev
        ind["slope"] = cur - prev
    return ind


def select_balanced_pair(
    strike_prem: Dict[Key, dict],
    spot: float,
    step: float,
    offset: int,
    ltp_target: float,
) -> Optional[Tuple[int, int, float, float]]:
    """
    Beginning concept (reference _get_strictly_lower_balanced_pair):
      1. ATM both sides; require both LTP > 0.
      2. Anchor = side with LOWER time-value (intrinsic-stripped) LTP.
      3. Anchor raw LTP must be >= ltp_target.
      4. Partner = scan other side over ATM +/- offset for ltp_target <= ltp < anchor_ltp;
         pick the HIGHEST such LTP (closest below anchor).
    Returns (ce_strike, pe_strike, ce_ltp, pe_ltp) or None.
    """
    atm = int(round(spot / step) * step)
    ce_atm = strike_prem.get((atm, "CE"))
    pe_atm = strike_prem.get((atm, "PE"))
    if not ce_atm or not pe_atm:
        return None
    ce_ltp = ce_atm.get("ltp", 0.0)
    pe_ltp = pe_atm.get("ltp", 0.0)
    if ce_ltp <= 0 or pe_ltp <= 0:
        return None

    ce_corr = strip_intrinsic(ce_ltp, "CE", atm, spot)
    pe_corr = strip_intrinsic(pe_ltp, "PE", atm, spot)

    if ce_corr < pe_corr:
        anchor_side, anchor_strike, anchor_ltp, partner_side = "CE", atm, ce_ltp, "PE"
    else:
        anchor_side, anchor_strike, anchor_ltp, partner_side = "PE", atm, pe_ltp, "CE"

    if anchor_ltp < ltp_target:
        return None

    best = None  # (ltp, strike)
    for i in range(-offset, offset + 1):
        s = int(atm + i * step)
        leg = strike_prem.get((s, partner_side))
        if not leg:
            continue
        ltp = leg.get("ltp", 0.0)
        if ltp_target <= ltp < anchor_ltp:
            if best is None or ltp > best[0]:
                best = (ltp, s)
    if best is None:
        return None

    partner_ltp, partner_strike = best
    if anchor_side == "CE":
        return anchor_strike, partner_strike, anchor_ltp, partner_ltp
    return partner_strike, anchor_strike, partner_ltp, anchor_ltp


def scan_pool(
    strike_prem: Dict[Key, dict],
    spot: float,
    step: float,
    offset: int,
    ltp_target: float,
    rule_pass,                      # callable(ce_strike:int, pe_strike:int) -> bool
    metric: str = "balanced_premium",
) -> Optional[Tuple[int, int, float, float]]:
    """
    Re-entry concept (reference _scan_v_slope_pool, balanced_premium metric):
      1. Strikes = ATM +/- offset.
      2. ATM bias from corrected ATM LTP: CE stronger if ce_corr > pe_corr.
      3. N x N over (s_ce, s_pe): both LTP >= ltp_target; bias filter
         (CE stronger -> ce_ltp < pe_ltp; else pe_ltp < ce_ltp).
      4. rule_pass(ce_strike, pe_strike) must be True (dynamic technical gate).
      5. balanced_score = abs(ce-pe)/(ce+pe); pick MIN score.
    Returns (ce_strike, pe_strike, ce_ltp, pe_ltp) or None.
    """
    atm = int(round(spot / step) * step)
    ce_atm = strike_prem.get((atm, "CE"))
    pe_atm = strike_prem.get((atm, "PE"))
    if not ce_atm or not pe_atm:
        return None
    ce_corr = strip_intrinsic(ce_atm.get("ltp", 0.0), "CE", atm, spot)
    pe_corr = strip_intrinsic(pe_atm.get("ltp", 0.0), "PE", atm, spot)
    ce_bias_stronger = ce_corr > pe_corr

    strikes = [int(atm + i * step) for i in range(-offset, offset + 1)]
    best = None  # (score, ce_strike, pe_strike, ce_ltp, pe_ltp)
    for s_ce in strikes:
        ce = strike_prem.get((s_ce, "CE"))
        if not ce:
            continue
        ce_ltp = ce.get("ltp", 0.0)
        if ce_ltp <= 0:
            continue
        for s_pe in strikes:
            pe = strike_prem.get((s_pe, "PE"))
            if not pe:
                continue
            pe_ltp = pe.get("ltp", 0.0)
            if pe_ltp <= 0:
                continue
            if ce_ltp < ltp_target or pe_ltp < ltp_target:
                continue
            if ce_bias_stronger:
                if ce_ltp >= pe_ltp:
                    continue
            else:
                if pe_ltp >= ce_ltp:
                    continue
            if not rule_pass(s_ce, s_pe):
                continue
            denom = ce_ltp + pe_ltp
            score = abs(ce_ltp - pe_ltp) / denom if denom > 0 else 999.0
            if best is None or score < best[0]:
                best = (score, s_ce, s_pe, ce_ltp, pe_ltp)
    if best is None:
        return None
    _, s_ce, s_pe, ce_ltp, pe_ltp = best
    return s_ce, s_pe, ce_ltp, pe_ltp


def classify_roll(ce_same: bool, pe_same: bool, has_candidates: bool) -> str:
    """Smart-roll outcome (reference exit_logic.perform_smart_roll):
      no candidates       -> "full_exit"
      both strikes same    -> "virtual"
      only PE changed      -> "partial_pe"   (CE stays)
      only CE changed      -> "partial_ce"   (PE stays)
      both changed         -> "physical"
    """
    if not has_candidates:
        return "full_exit"
    if ce_same and pe_same:
        return "virtual"
    if ce_same and not pe_same:
        return "partial_pe"
    if pe_same and not ce_same:
        return "partial_ce"
    return "physical"
