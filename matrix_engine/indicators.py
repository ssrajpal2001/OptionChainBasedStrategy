"""
matrix_engine/indicators.py — Vectorized indicator library.

Period lengths are HARD-PINNED as module constants:
  RSI_PERIOD  = 14   (Wilder's 14-period RSI — spec requirement)
  VWAP_WINDOW = 500  (500-candle rolling VWAP — spec requirement)
  ADX_PERIOD  = 20   (20-period ADX+DI — spec requirement)

The public functions rsi(), vwap(), adx() do NOT accept period arguments.
This is intentional: if the period were parameterised, a call-site could
silently pass the wrong value. All callers use the same constant, always.

EMA and ATR retain configurable periods (fast=9, slow=21, atr=14) because
they are supporting indicators, not specification-constrained signals.
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray

# ── Hard-pinned spec constants ────────────────────────────────────────────────
RSI_PERIOD:  int = 14
VWAP_WINDOW: int = 500
ADX_PERIOD:  int = 20


# ─────────────────────────────────────────────────────────────────────────────
# RSI — strictly 14 candles (Wilder's smoothing)
# ─────────────────────────────────────────────────────────────────────────────

def rsi(closes: NDArray[np.float64]) -> float:
    """
    Wilder's RSI — always RSI(14).
    Returns 50.0 when fewer than RSI_PERIOD+1 candles are available.
    """
    period = RSI_PERIOD
    n = period + 1
    if len(closes) < n:
        return 50.0
    deltas = np.diff(closes[-n:])
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_g = float(gains[:period].mean())
    avg_l = float(losses[:period].mean())
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + float(gains[i]))  / period
        avg_l = (avg_l * (period - 1) + float(losses[i])) / period
    if avg_l == 0:
        return 100.0
    return float(100.0 - 100.0 / (1.0 + avg_g / avg_l))


# ─────────────────────────────────────────────────────────────────────────────
# VWAP — strictly 500-candle rolling window
# ─────────────────────────────────────────────────────────────────────────────

def vwap(
    highs:   NDArray[np.float64],
    lows:    NDArray[np.float64],
    closes:  NDArray[np.float64],
    volumes: NDArray[np.float64],
) -> float:
    """
    Volume-Weighted Average Price — always VWAP(500).
    Uses the last min(len, 500) candles.
    """
    w = VWAP_WINDOW
    h, l, c, v = highs[-w:], lows[-w:], closes[-w:], volumes[-w:]
    tp = (h + l + c) / 3.0
    total_vol = float(v.sum())
    if total_vol == 0:
        return float(c[-1]) if len(c) > 0 else 0.0
    return float((tp * v).sum() / total_vol)


# ─────────────────────────────────────────────────────────────────────────────
# ATR — configurable (default 14, not spec-pinned)
# ─────────────────────────────────────────────────────────────────────────────

def atr(
    highs:  NDArray[np.float64],
    lows:   NDArray[np.float64],
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


# ─────────────────────────────────────────────────────────────────────────────
# ADX + ±DI — strictly 20-period
# ─────────────────────────────────────────────────────────────────────────────

def adx(
    highs:  NDArray[np.float64],
    lows:   NDArray[np.float64],
    closes: NDArray[np.float64],
) -> tuple[float, float, float]:
    """
    ADX, +DI, -DI — always ADX(20).
    Returns (0.0, 0.0, 0.0) if fewer than 2*ADX_PERIOD+2 candles available.
    """
    period = ADX_PERIOD
    need = period * 2 + 2
    if len(closes) < need:
        return 0.0, 0.0, 0.0

    h, l, c = highs[-need:], lows[-need:], closes[-need:]
    up  = np.diff(h)
    dn  = -np.diff(l)
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

    tr_s  = _wilder(tr)
    pdm_s = _wilder(pdm)
    mdm_s = _wilder(mdm)

    with np.errstate(divide="ignore", invalid="ignore"):
        pdi = np.where(tr_s > 0, 100.0 * pdm_s / tr_s, 0.0)
        mdi = np.where(tr_s > 0, 100.0 * mdm_s / tr_s, 0.0)
        dx  = np.where(
            (pdi + mdi) > 0,
            100.0 * np.abs(pdi - mdi) / (pdi + mdi),
            0.0,
        )

    adx_arr = _wilder(dx[period - 1:])
    # Clamp to [0, 100] — synthetic data edge cases can produce tiny overshoots
    adx_val = float(np.clip(adx_arr[-1], 0.0, 100.0))
    return adx_val, float(pdi[-1]), float(mdi[-1])


# ─────────────────────────────────────────────────────────────────────────────
# EMA — configurable
# ─────────────────────────────────────────────────────────────────────────────

def ema(closes: NDArray[np.float64], period: int) -> float:
    if len(closes) < period:
        return float(closes[-1]) if len(closes) > 0 else 0.0
    k = 2.0 / (period + 1)
    val = float(closes[:period].mean())
    for p in closes[period:]:
        val = float(p) * k + val * (1.0 - k)
    return val


# ─────────────────────────────────────────────────────────────────────────────
# Volume spike detector
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# Composite snapshot struct
# ─────────────────────────────────────────────────────────────────────────────

from dataclasses import dataclass
from datetime import datetime


@dataclass()
class TechSnapshot:
    """All computed indicators for one symbol x timeframe at one moment."""
    symbol: str
    timeframe: int
    timestamp: datetime
    ltp: float
    rsi: float = 50.0
    vwap_val: float = 0.0
    adx_val: float = 0.0
    plus_di: float = 0.0
    minus_di: float = 0.0
    ema_fast: float = 0.0
    ema_slow: float = 0.0
    atr_val: float = 0.0
    vol_ma: float = 0.0
    # Last completed candle OHLCV
    c_open: float = 0.0
    c_high: float = 0.0
    c_low: float = 0.0
    c_close: float = 0.0
    c_volume: int = 0
    # Previous candle
    p_open: float = 0.0
    p_high: float = 0.0
    p_low: float = 0.0
    p_close: float = 0.0

    @property
    def is_bullish(self) -> bool:
        return self.c_close > self.c_open

    @property
    def body_ratio(self) -> float:
        r = self.c_high - self.c_low
        return abs(self.c_close - self.c_open) / r if r > 0 else 0.0

    @property
    def lower_wick_ratio(self) -> float:
        r = self.c_high - self.c_low
        return (min(self.c_open, self.c_close) - self.c_low) / r if r > 0 else 0.0

    @property
    def upper_wick_ratio(self) -> float:
        r = self.c_high - self.c_low
        return (self.c_high - max(self.c_open, self.c_close)) / r if r > 0 else 0.0

    @property
    def is_vol_spike(self) -> bool:
        return self.vol_ma > 0 and self.c_volume > self.vol_ma * 2.0
