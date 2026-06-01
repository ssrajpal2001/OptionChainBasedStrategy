"""Volume spike detector."""
from __future__ import annotations

import numpy as np
from numpy.typing import NDArray


def volume_spike(
    volumes: NDArray[np.float64],
    current_vol: float,
    period: int = 20,
    multiplier: float = 2.0,
) -> bool:
    if len(volumes) < period:
        return False
    avg = float(volumes[-period:].mean())
    return avg > 0 and current_vol > avg * multiplier
