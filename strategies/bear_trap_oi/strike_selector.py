"""PDH/PDL -> traded-strike mapping. See spec Section 3.

CE strike = strike closest to the previous day's LOW.
PE strike = strike closest to the previous day's HIGH.
"""
from __future__ import annotations

import math


def round_to_strike_step(price: float, step: int) -> int:
    # Explicit round-half-up (not Python's round-half-to-even banker's
    # rounding) -- a .5 strike boundary rounds away from zero.
    return int(math.floor(price / step + 0.5) * step)


def map_strikes(pdh: float, pdl: float, step: int) -> tuple[int, int]:
    ce_strike = round_to_strike_step(pdl, step)
    pe_strike = round_to_strike_step(pdh, step)
    return ce_strike, pe_strike
