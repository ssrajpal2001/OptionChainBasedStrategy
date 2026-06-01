"""TechSnapshot — all computed indicators for one symbol x timeframe at one moment."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass()
class TechSnapshot:
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
    c_open: float = 0.0
    c_high: float = 0.0
    c_low: float = 0.0
    c_close: float = 0.0
    c_volume: int = 0
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
