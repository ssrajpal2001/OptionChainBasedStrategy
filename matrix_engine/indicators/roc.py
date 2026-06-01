"""ROC — Rate of Change. Matches Option_Selling_May_2026 ROCIndicator:
    roc = 100 * (source - source[length]) / source[length]
"""
from __future__ import annotations

import numpy as np
from numpy.typing import NDArray


def roc(closes: NDArray[np.float64], length: int = 9) -> float:
    """100 * (src - src[length]) / src[length]. 0.0 if insufficient/undefined."""
    if len(closes) <= length:
        return 0.0
    ref = float(closes[-1 - length])
    if ref == 0:
        return 0.0
    return float(100.0 * (float(closes[-1]) - ref) / ref)
