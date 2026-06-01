"""ATR — Average True Range (configurable period, default 14)."""
from __future__ import annotations

import numpy as np
from numpy.typing import NDArray


def atr(
    highs: NDArray[np.float64],
    lows: NDArray[np.float64],
    closes: NDArray[np.float64],
    period: int = 14,
) -> float:
    if len(closes) < 2:
        return 0.0
    n = min(len(closes), period + 1)
    h, l, c = highs[-n:], lows[-n:], closes[-n:]
    prev_c = c[:-1]
    tr = np.maximum(
        h[1:] - l[1:],
        np.maximum(np.abs(h[1:] - prev_c), np.abs(l[1:] - prev_c)),
    )
    return float(tr[-period:].mean() if len(tr) >= period else tr.mean())
