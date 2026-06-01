"""RSI — Wilder's 14-period RSI. Matches Option_Selling_May_2026 RSIIndicator."""
from __future__ import annotations

import numpy as np
from numpy.typing import NDArray

from matrix_engine.indicators.constants import RSI_PERIOD


def rsi(closes: NDArray[np.float64]) -> float:
    """Wilder's RSI(14). Returns 50.0 when fewer than RSI_PERIOD+1 candles."""
    period = RSI_PERIOD
    n = period + 1
    if len(closes) < n:
        return 50.0
    deltas = np.diff(closes[-n:])
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_g = float(gains[:period].mean())
    avg_l = float(losses[:period].mean())
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + float(gains[i])) / period
        avg_l = (avg_l * (period - 1) + float(losses[i])) / period
    if avg_l == 0:
        return 100.0
    return float(100.0 - 100.0 / (1.0 + avg_g / avg_l))
