"""
VWAP — sourced from the broker feed (exchange ATP), NEVER self-computed.

The exchange-reported Average Traded Price (`avg_trade_price` in Fyers,
`atp` in Upstox full mode) IS that instrument's VWAP. A strategy's combined
VWAP is the sum of its own legs' ATP (straddle = ATM CE ATP + ATM PE ATP).
This matches the Option_Selling_May_2026 reference, which uses exchange ATP.
"""
from __future__ import annotations

from typing import List, Optional

import numpy as np
from numpy.typing import NDArray

from matrix_engine.indicators.constants import VWAP_WINDOW


def vwap(
    highs: NDArray[np.float64],
    lows: NDArray[np.float64],
    closes: NDArray[np.float64],
    volumes: NDArray[np.float64],
) -> float:
    """
    DEPRECATED self-computed VWAP(500). Kept only for legacy callers
    (strategy_a/b/c, base_strategy). The correct VWAP is the broker ATP —
    use leg_atp()/combined_vwap(). Strategies are migrated off this in the
    indicator-engine refactor (Task 6+).
    """
    w = VWAP_WINDOW
    h, l, c, v = highs[-w:], lows[-w:], closes[-w:], volumes[-w:]
    tp = (h + l + c) / 3.0
    total_vol = float(v.sum())
    if total_vol == 0:
        return float(c[-1]) if len(c) > 0 else 0.0
    return float((tp * v).sum() / total_vol)


def leg_atp(tick) -> float:
    """Return the exchange ATP (instrument VWAP) carried on an OptionTick."""
    return float(getattr(tick, "atp", 0.0) or 0.0)


def combined_vwap(atps: List[float]) -> Optional[float]:
    """
    Sum the per-leg ATPs for a strategy's legs. Returns None if any leg's ATP
    is missing/zero (so a rule using VWAP evaluates N/A rather than wrong).
    """
    if not atps or any(a is None or a <= 0 for a in atps):
        return None
    return float(sum(atps))
