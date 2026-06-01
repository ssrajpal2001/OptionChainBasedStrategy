"""EMA — exponential moving average (configurable period)."""
from __future__ import annotations

import numpy as np
from numpy.typing import NDArray


def ema(closes: NDArray[np.float64], period: int) -> float:
    if len(closes) < period:
        return float(closes[-1]) if len(closes) > 0 else 0.0
    k = 2.0 / (period + 1)
    val = float(closes[:period].mean())
    for p in closes[period:]:
        val = float(p) * k + val * (1.0 - k)
    return val
