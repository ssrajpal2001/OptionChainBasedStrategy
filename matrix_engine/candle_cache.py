"""
matrix_engine/candle_cache.py — Async OHLCV aggregation engine.

Subscribes to INDEX_TICK on the EventBus.  For every tick, updates all
configured timeframe candle buckets.  When a bucket closes (timestamp
flips to a new period), emits a CandleEvent on CANDLE_CLOSE topic and
a TechSnapshot on MATRIX_SNAPSHOT topic.

No time.sleep, no blocking I/O.  All computation is NumPy.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime
from threading import RLock
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from config.global_config import IST, Topic, GlobalConfig
from data_layer.base_feeder import EventBus, IndexTick, CandleEvent
from matrix_engine.indicators import (
    TechSnapshot, rsi, vwap, atr, adx, ema, volume_spike,
)

logger = logging.getLogger(__name__)

_MAX_CANDLES = 600    # Keep last 600 bars per symbol × timeframe


# ─────────────────────────────────────────────────────────────────────────────
# Candle Bucket
# ─────────────────────────────────────────────────────────────────────────────

class _Bucket:
    """Single in-progress candle."""
    __slots__ = ("ts", "open", "high", "low", "close", "volume")

    def __init__(self, ts: datetime, o: float, h: float, l: float, c: float, v: int) -> None:
        self.ts = ts
        self.open = o
        self.high = h
        self.low = l
        self.close = c
        self.volume = v

    def update(self, high: float, low: float, close: float, volume: int) -> None:
        self.high = max(self.high, high)
        self.low = min(self.low, low)
        self.close = close
        self.volume += volume


# ─────────────────────────────────────────────────────────────────────────────
# Per-Symbol, Per-Timeframe Candle Series
# ─────────────────────────────────────────────────────────────────────────────

class _CandleSeries:
    def __init__(self) -> None:
        self._lock = RLock()
        # Completed candles (deque of dicts)
        self._candles: deque = deque(maxlen=_MAX_CANDLES)
        self._bucket: Optional[_Bucket] = None

    def on_tick(self, tick: IndexTick, tf_minutes: int) -> Optional[CandleEvent]:
        """
        Update the in-progress bucket. If tick belongs to a new period,
        commit the old bucket and return it as a CandleEvent; else None.
        """
        bucket_ts = _floor_ts(tick.timestamp, tf_minutes)
        closed_event: Optional[CandleEvent] = None

        with self._lock:
            if self._bucket is None or self._bucket.ts != bucket_ts:
                # New period — commit previous bucket
                if self._bucket is not None:
                    b = self._bucket
                    closed_event = CandleEvent(
                        symbol=tick.symbol, timeframe=tf_minutes,
                        open=b.open, high=b.high, low=b.low, close=b.close,
                        volume=b.volume, timestamp=b.ts,
                        is_bullish=b.close >= b.open,
                    )
                    self._candles.append({
                        "ts": b.ts, "open": b.open, "high": b.high,
                        "low": b.low, "close": b.close, "volume": b.volume,
                    })
                self._bucket = _Bucket(bucket_ts, tick.open, tick.high, tick.low, tick.ltp, tick.volume)
            else:
                self._bucket.update(tick.high, tick.low, tick.ltp, tick.volume)

        return closed_event

    def load_history(self, df: pd.DataFrame) -> None:
        with self._lock:
            for _, r in df.iterrows():
                self._candles.append({
                    "ts": r.name if isinstance(r.name, datetime) else pd.Timestamp(r.name),
                    "open": float(r["open"]), "high": float(r["high"]),
                    "low": float(r["low"]), "close": float(r["close"]),
                    "volume": int(r["volume"]),
                })

    def arrays(self) -> Tuple[
        np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray
    ]:
        """Return (opens, highs, lows, closes, volumes) as NumPy arrays."""
        with self._lock:
            if not self._candles:
                empty = np.array([], dtype=np.float64)
                return empty, empty, empty, empty, np.array([], dtype=np.int64)
            data = list(self._candles)
        opens   = np.array([d["open"]   for d in data], dtype=np.float64)
        highs   = np.array([d["high"]   for d in data], dtype=np.float64)
        lows    = np.array([d["low"]    for d in data], dtype=np.float64)
        closes  = np.array([d["close"]  for d in data], dtype=np.float64)
        volumes = np.array([d["volume"] for d in data], dtype=np.float64)
        return opens, highs, lows, closes, volumes

    def last_two(self) -> Tuple[Optional[Dict], Optional[Dict]]:
        with self._lock:
            lst = list(self._candles)
        last = lst[-1] if lst else None
        prev = lst[-2] if len(lst) >= 2 else None
        return last, prev

    def length(self) -> int:
        with self._lock:
            return len(self._candles)


# ─────────────────────────────────────────────────────────────────────────────
# Candle Cache — manages all symbol × timeframe series
# ─────────────────────────────────────────────────────────────────────────────

class CandleCache:
    """
    Subscribes to INDEX_TICK events, aggregates candles, and publishes:
      • CANDLE_CLOSE  on each closed bar
      • MATRIX_SNAPSHOT  with computed TechSnapshot (every closed bar)
    """

    def __init__(self, bus: EventBus, cfg: GlobalConfig) -> None:
        self._bus = bus
        self._cfg = cfg
        self._series: Dict[Tuple[str, int], _CandleSeries] = defaultdict(_CandleSeries)
        self._tick_queue = bus.subscribe(Topic.INDEX_TICK)
        self._running = False

    def load_history(self, symbol: str, timeframe: int, df: pd.DataFrame) -> None:
        self._series[(symbol, timeframe)].load_history(df)
        logger.debug("CandleCache: Loaded %d historical candles for %s/%dm.", len(df), symbol, timeframe)

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                tick: IndexTick = await asyncio.wait_for(self._tick_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            for tf in self._cfg.candle_timeframes:
                key = (tick.symbol, tf)
                closed = self._series[key].on_tick(tick, tf)
                if closed:
                    await self._bus.publish(Topic.CANDLE_CLOSE, closed)
                    snap = self._compute_snapshot(tick.symbol, tf, tick.ltp, tick.timestamp)
                    if snap:
                        await self._bus.publish(Topic.MATRIX_SNAPSHOT, snap)

    def stop(self) -> None:
        self._running = False

    def _compute_snapshot(
        self, symbol: str, tf: int, ltp: float, ts: datetime
    ) -> Optional[TechSnapshot]:
        key = (symbol, tf)
        series = self._series[key]
        if series.length() < 5:
            return None

        opens, highs, lows, closes, volumes = series.arrays()
        cfg_i = self._cfg.indicators
        last, prev = series.last_two()
        if last is None:
            return None

        adx_v, pdi, mdi = adx(highs, lows, closes)
        vol_ma = float(volumes[-cfg_i.volume_ma_period:].mean()) if len(volumes) >= cfg_i.volume_ma_period else float(volumes.mean())

        return TechSnapshot(
            symbol=symbol, timeframe=tf, timestamp=ts, ltp=ltp,
            rsi=rsi(closes),
            vwap_val=vwap(highs, lows, closes, volumes),
            adx_val=adx_v, plus_di=pdi, minus_di=mdi,
            ema_fast=ema(closes, cfg_i.ema_fast),
            ema_slow=ema(closes, cfg_i.ema_slow),
            atr_val=atr(highs, lows, closes, cfg_i.atr_period),
            vol_ma=vol_ma,
            c_open=float(last["open"]),  c_high=float(last["high"]),
            c_low=float(last["low"]),    c_close=float(last["close"]),
            c_volume=int(last["volume"]),
            p_open=float(prev["open"])  if prev else float(last["open"]),
            p_high=float(prev["high"])  if prev else float(last["high"]),
            p_low=float(prev["low"])    if prev else float(last["low"]),
            p_close=float(prev["close"]) if prev else float(last["close"]),
        )

    def get_snapshot(self, symbol: str, tf: int, ltp: float) -> Optional[TechSnapshot]:
        """On-demand snapshot (called by backtester between tick events)."""
        return self._compute_snapshot(symbol, tf, ltp, datetime.now(IST))

    def get_all_snapshots(self, symbol: str, ltp: float) -> List[Optional[TechSnapshot]]:
        return [self.get_snapshot(symbol, tf, ltp) for tf in self._cfg.candle_timeframes]


# ─────────────────────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────────────────────

def _floor_ts(ts: datetime, minutes: int) -> datetime:
    total_min = ts.hour * 60 + ts.minute
    floored = (total_min // minutes) * minutes
    return ts.replace(
        hour=floored // 60, minute=floored % 60,
        second=0, microsecond=0,
    )
