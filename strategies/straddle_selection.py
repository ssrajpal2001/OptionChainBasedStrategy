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
