"""strategies/strike_utils.py — shared strike-price arithmetic."""
from __future__ import annotations


def compute_atm(spot: float, step: float) -> float:
    """Round spot to the nearest strike step. Single source of truth for ATM calc."""
    return round(spot / step) * step
