"""ADX + DI — strictly 20-period (Wilder's smoothing)."""
from __future__ import annotations

import numpy as np
from numpy.typing import NDArray

from matrix_engine.indicators.constants import ADX_PERIOD


def adx(
    highs: NDArray[np.float64],
    lows: NDArray[np.float64],
    closes: NDArray[np.float64],
) -> tuple[float, float, float]:
    """ADX, +DI, -DI — always ADX(20). (0,0,0) if < 2*ADX_PERIOD+2 candles."""
    period = ADX_PERIOD
    need = period * 2 + 2
    if len(closes) < need:
        return 0.0, 0.0, 0.0

    h, l, c = highs[-need:], lows[-need:], closes[-need:]
    up = np.diff(h)
    dn = -np.diff(l)
    pdm = np.where((up > dn) & (up > 0), up, 0.0)
    mdm = np.where((dn > up) & (dn > 0), dn, 0.0)
    prev_c = c[:-1]
    tr = np.maximum(
        np.diff(h) - np.diff(l),
        np.maximum(np.abs(np.diff(h) - prev_c), np.abs(np.diff(l) - prev_c)),
    )

    def _wilder(arr: NDArray) -> NDArray:
        out = np.zeros(len(arr))
        out[period - 1] = arr[:period].sum()
        for i in range(period, len(arr)):
            out[i] = out[i - 1] - out[i - 1] / period + arr[i]
        return out

    tr_s = _wilder(tr)
    pdm_s = _wilder(pdm)
    mdm_s = _wilder(mdm)

    with np.errstate(divide="ignore", invalid="ignore"):
        pdi = np.where(tr_s > 0, 100.0 * pdm_s / tr_s, 0.0)
        mdi = np.where(tr_s > 0, 100.0 * mdm_s / tr_s, 0.0)
        dx = np.where(
            (pdi + mdi) > 0,
            100.0 * np.abs(pdi - mdi) / (pdi + mdi),
            0.0,
        )

    adx_arr = _wilder(dx[period - 1:])
    adx_val = float(np.clip(adx_arr[-1], 0.0, 100.0))
    return adx_val, float(pdi[-1]), float(mdi[-1])
