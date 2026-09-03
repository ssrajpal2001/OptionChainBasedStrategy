"""
strategies/v4_cascade/indicators.py — RSI(14)/VWAP(500)/ADX(20) on a single
tracking contract's own premium bar series.

Thin accumulator wrapping matrix_engine/indicators.py's hard-pinned
rsi()/vwap()/adx() functions (RSI_PERIOD=14, VWAP_WINDOW=500, ADX_PERIOD=20 —
these are module constants there, not parameterised, so there's nothing to
override here). Pure — no bus/broker dependency, fed bars directly.
"""
from __future__ import annotations

from collections import deque
from typing import Deque, Dict

import numpy as np

from matrix_engine.indicators import rsi, vwap, adx

_MAX_BARS = 600


class PremiumIndicatorSeries:
    """One instance per tracking contract, fed its own 5m bar closes."""

    def __init__(self) -> None:
        self._highs: Deque[float] = deque(maxlen=_MAX_BARS)
        self._lows: Deque[float] = deque(maxlen=_MAX_BARS)
        self._closes: Deque[float] = deque(maxlen=_MAX_BARS)
        self._volumes: Deque[float] = deque(maxlen=_MAX_BARS)

    def on_bar(self, bar) -> None:
        self._highs.append(bar.high)
        self._lows.append(bar.low)
        self._closes.append(bar.close)
        self._volumes.append(getattr(bar, "volume", 0) or 0)

    def snapshot(self) -> Dict[str, float]:
        if len(self._closes) < 2:
            return {"rsi": 50.0, "vwap": 0.0, "adx": 0.0, "plus_di": 0.0, "minus_di": 0.0}
        closes = np.array(self._closes, dtype=np.float64)
        highs = np.array(self._highs, dtype=np.float64)
        lows = np.array(self._lows, dtype=np.float64)
        volumes = np.array(self._volumes, dtype=np.float64)
        adx_v, pdi, mdi = adx(highs, lows, closes)
        return {
            "rsi": rsi(closes),
            "vwap": vwap(highs, lows, closes, volumes),
            "adx": adx_v, "plus_di": pdi, "minus_di": mdi,
        }

    def reset(self) -> None:
        self._highs.clear()
        self._lows.clear()
        self._closes.clear()
        self._volumes.clear()
