"""
VWAP SLOPE — change of VWAP between timeframe boundaries.

Ported from Option_Selling_May_2026 indicator_manager.get_vwap_slope_status:
compares current VWAP (v_curr) vs the VWAP one timeframe-boundary ago (v_prev),
and counts consecutive rising/falling occurrences.
`SLOPE < 0` == VWAP falling (v_curr < v_prev).
"""
from __future__ import annotations

from typing import List, Tuple


def vwap_slope(vwaps: List[float], occurrences: int = 1) -> Tuple[bool, bool, float, float, int, int]:
    """
    vwaps: closed-minute combined VWAPs, NEWEST FIRST [v_curr, v_prev, ...],
           each one timeframe-boundary apart.
    Returns (rising_now_ok, falling_now_ok, v_curr, v_prev, cons_rising, cons_falling).
    rising_now_ok/falling_now_ok require the consecutive count to reach `occurrences`.
    """
    if len(vwaps) < 2:
        v = vwaps[0] if vwaps else 0.0
        return False, False, v, v, 0, 0
    v_curr, v_prev = vwaps[0], vwaps[1]
    cons_rising = 0
    for i in range(len(vwaps) - 1):
        if vwaps[i] > vwaps[i + 1]:
            cons_rising += 1
        else:
            break
    cons_falling = 0
    for i in range(len(vwaps) - 1):
        if vwaps[i] < vwaps[i + 1]:
            cons_falling += 1
        else:
            break
    return (
        (v_curr > v_prev) and cons_rising >= occurrences,
        (v_curr < v_prev) and cons_falling >= occurrences,
        v_curr, v_prev, cons_rising, cons_falling,
    )
