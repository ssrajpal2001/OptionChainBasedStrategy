"""
matrix_engine.py — Market Matrix Engine.

Maintains two live, thread-safe data stores:
  1. CandleCache  — rolling OHLCV DataFrames per symbol × timeframe
  2. OptionChainMatrix — ATM ± N strikes with OI, ΔOI, Volume, PCR

Also computes all technical indicators (RSI, VWAP, ADX, EMA, ATR) and
derived option metrics (PCR, Max OI strikes, Unwinding flags) used by
the strategy confluence engine.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, date
from threading import RLock
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from config import SystemConfig
from data_provider import IndexTick, OptionTick, OHLCV

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Technical Indicator Helpers (vectorized, stateless)
# ---------------------------------------------------------------------------

def compute_rsi(closes: np.ndarray, period: int = 14) -> float:
    """Return the latest RSI value. Returns 50.0 if insufficient data."""
    if len(closes) < period + 1:
        return 50.0
    deltas = np.diff(closes[-(period + 1):])
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = gains.mean()
    avg_loss = losses.mean()
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - (100.0 / (1.0 + rs)))


def compute_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> float:
    """Return the latest ATR."""
    if len(closes) < 2:
        return 0.0
    n = min(len(closes), period + 1)
    h = highs[-n:]
    l = lows[-n:]
    c = closes[-n:]
    prev_c = c[:-1]
    tr = np.maximum(h[1:] - l[1:], np.maximum(abs(h[1:] - prev_c), abs(l[1:] - prev_c)))
    return float(tr.mean())


def compute_adx(
    highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14
) -> Tuple[float, float, float]:
    """Return (ADX, +DI, -DI). Returns (0, 0, 0) if insufficient data."""
    n = period * 2 + 1
    if len(closes) < n:
        return 0.0, 0.0, 0.0
    h = highs[-n:]
    l = lows[-n:]
    c = closes[-n:]
    up_move = np.diff(h)
    down_move = -np.diff(l)
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    tr = np.maximum(np.diff(h) - np.diff(l),
                    np.maximum(abs(np.diff(h) - c[:-1]), abs(np.diff(l) - c[:-1])))

    def _smooth(arr: np.ndarray, p: int) -> np.ndarray:
        result = np.zeros(len(arr))
        result[p - 1] = arr[:p].sum()
        for i in range(p, len(arr)):
            result[i] = result[i - 1] - result[i - 1] / p + arr[i]
        return result

    atr_s = _smooth(tr, period)
    pdm_s = _smooth(plus_dm, period)
    mdm_s = _smooth(minus_dm, period)
    with np.errstate(divide="ignore", invalid="ignore"):
        pdi = np.where(atr_s != 0, 100 * pdm_s / atr_s, 0.0)
        mdi = np.where(atr_s != 0, 100 * mdm_s / atr_s, 0.0)
        dx = np.where((pdi + mdi) != 0, 100 * abs(pdi - mdi) / (pdi + mdi), 0.0)
    adx_s = _smooth(dx[period - 1:], period)
    return float(adx_s[-1]), float(pdi[-1]), float(mdi[-1])


def compute_ema(closes: np.ndarray, period: int) -> float:
    """Return the latest EMA value."""
    if len(closes) < period:
        return float(closes[-1]) if len(closes) > 0 else 0.0
    k = 2.0 / (period + 1)
    ema = closes[:period].mean()
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return float(ema)


def compute_vwap(df: pd.DataFrame) -> float:
    """Compute VWAP from a DataFrame with columns: high, low, close, volume."""
    if df.empty:
        return 0.0
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    total_vol = df["volume"].sum()
    if total_vol == 0:
        return float(typical_price.iloc[-1])
    return float((typical_price * df["volume"]).sum() / total_vol)


# ---------------------------------------------------------------------------
# Derived Indicator Snapshot — passed to strategy engine
# ---------------------------------------------------------------------------

@dataclass
class TechnicalSnapshot:
    symbol: str
    timeframe: int
    timestamp: datetime
    ltp: float
    rsi: float = 50.0
    vwap: float = 0.0
    adx: float = 0.0
    plus_di: float = 0.0
    minus_di: float = 0.0
    ema_fast: float = 0.0
    ema_slow: float = 0.0
    atr: float = 0.0
    # Last few candles for pattern detection
    last_open: float = 0.0
    last_high: float = 0.0
    last_low: float = 0.0
    last_close: float = 0.0
    prev_open: float = 0.0
    prev_high: float = 0.0
    prev_low: float = 0.0
    prev_close: float = 0.0
    # Volume metrics
    volume_ma: float = 0.0
    current_volume: int = 0

    @property
    def is_bullish_candle(self) -> bool:
        return self.last_close > self.last_open

    @property
    def body_ratio(self) -> float:
        total_range = self.last_high - self.last_low
        if total_range == 0:
            return 0.0
        return abs(self.last_close - self.last_open) / total_range

    @property
    def upper_wick_ratio(self) -> float:
        total_range = self.last_high - self.last_low
        if total_range == 0:
            return 0.0
        upper_wick = self.last_high - max(self.last_open, self.last_close)
        return upper_wick / total_range

    @property
    def lower_wick_ratio(self) -> float:
        total_range = self.last_high - self.last_low
        if total_range == 0:
            return 0.0
        lower_wick = min(self.last_open, self.last_close) - self.last_low
        return lower_wick / total_range

    @property
    def is_volume_spike(self) -> bool:
        return self.volume_ma > 0 and self.current_volume > self.volume_ma * 2.0


# ---------------------------------------------------------------------------
# Option Chain Row
# ---------------------------------------------------------------------------

@dataclass
class OptionChainRow:
    strike: float
    call_oi: int = 0
    call_change_oi: int = 0
    call_volume: int = 0
    call_ltp: float = 0.0
    call_iv: float = 0.0
    put_ltp: float = 0.0
    put_iv: float = 0.0
    put_volume: int = 0
    put_change_oi: int = 0
    put_oi: int = 0
    # Historical ΔOI to detect spikes
    call_oi_history: deque = field(default_factory=lambda: deque(maxlen=20))
    put_oi_history: deque = field(default_factory=lambda: deque(maxlen=20))

    @property
    def pcr_oi(self) -> float:
        """Put-Call Ratio by OI."""
        return self.put_oi / self.call_oi if self.call_oi > 0 else 0.0

    @property
    def pcr_volume(self) -> float:
        return self.put_volume / self.call_volume if self.call_volume > 0 else 0.0


@dataclass
class OptionChainSnapshot:
    """Full chain snapshot used by strategy engine."""
    underlying: str
    spot_price: float
    atm_strike: float
    expiry: date
    timestamp: datetime
    rows: Dict[float, OptionChainRow] = field(default_factory=dict)

    # Derived aggregates — computed by MatrixEngine after each update
    max_call_oi_strike: float = 0.0
    max_put_oi_strike: float = 0.0
    max_call_delta_oi_strike: float = 0.0
    max_put_delta_oi_strike: float = 0.0
    total_call_oi: int = 0
    total_put_oi: int = 0
    pcr: float = 1.0                  # Overall chain PCR
    pcr_history: deque = field(default_factory=lambda: deque(maxlen=50))

    def get_row(self, strike: float) -> Optional[OptionChainRow]:
        return self.rows.get(strike)

    def strikes_sorted(self) -> List[float]:
        return sorted(self.rows.keys())

    def call_oi_series(self) -> pd.Series:
        return pd.Series({s: r.call_oi for s, r in self.rows.items()}).sort_index()

    def put_oi_series(self) -> pd.Series:
        return pd.Series({s: r.put_oi for s, r in self.rows.items()}).sort_index()

    def delta_call_oi_series(self) -> pd.Series:
        return pd.Series({s: r.call_change_oi for s, r in self.rows.items()}).sort_index()

    def delta_put_oi_series(self) -> pd.Series:
        return pd.Series({s: r.put_change_oi for s, r in self.rows.items()}).sort_index()


# ---------------------------------------------------------------------------
# Candle Cache
# ---------------------------------------------------------------------------

class CandleCache:
    """
    Per-symbol, per-timeframe rolling OHLCV store.

    Ticks are aggregated into candles of the specified timeframe minutes.
    Thread-safe via RLock for cross-task access.
    """

    _MAX_CANDLES = 500   # Keep last N candles in memory

    def __init__(self, config: SystemConfig) -> None:
        self._config = config
        self._lock = RLock()
        # {symbol: {timeframe: pd.DataFrame}}
        self._candles: Dict[str, Dict[int, pd.DataFrame]] = defaultdict(dict)
        # In-progress candle being built from ticks
        self._in_progress: Dict[Tuple[str, int], Dict] = {}

    def on_tick(self, tick: IndexTick) -> None:
        """Update all configured timeframe candles with a new price tick."""
        for tf in self._config.assets.candle_timeframes:
            self._aggregate_tick(tick, tf)

    def _aggregate_tick(self, tick: IndexTick, timeframe: int) -> None:
        """Bucket the tick into the current candle for this timeframe."""
        key = (tick.symbol, timeframe)
        bucket_ts = self._floor_timestamp(tick.timestamp, timeframe)

        with self._lock:
            in_prog = self._in_progress.get(key)

            if in_prog is None or in_prog["timestamp"] != bucket_ts:
                # New candle started — flush the previous one
                if in_prog is not None:
                    self._commit_candle(tick.symbol, timeframe, in_prog)
                self._in_progress[key] = {
                    "timestamp": bucket_ts,
                    "open": tick.open,
                    "high": tick.high,
                    "low": tick.low,
                    "close": tick.ltp,
                    "volume": tick.volume,
                }
            else:
                in_prog["high"] = max(in_prog["high"], tick.high)
                in_prog["low"] = min(in_prog["low"], tick.low)
                in_prog["close"] = tick.ltp
                in_prog["volume"] += tick.volume

    def _commit_candle(self, symbol: str, timeframe: int, candle: Dict) -> None:
        row = pd.DataFrame([{
            "open": candle["open"],
            "high": candle["high"],
            "low": candle["low"],
            "close": candle["close"],
            "volume": candle["volume"],
        }], index=[candle["timestamp"]])

        if timeframe not in self._candles[symbol]:
            self._candles[symbol][timeframe] = row
        else:
            self._candles[symbol][timeframe] = pd.concat(
                [self._candles[symbol][timeframe], row]
            ).tail(self._MAX_CANDLES)

    @staticmethod
    def _floor_timestamp(ts: datetime, timeframe: int) -> datetime:
        minutes = (ts.hour * 60 + ts.minute) // timeframe * timeframe
        return ts.replace(hour=minutes // 60, minute=minutes % 60, second=0, microsecond=0)

    def get_candles(self, symbol: str, timeframe: int) -> pd.DataFrame:
        with self._lock:
            return self._candles.get(symbol, {}).get(timeframe, pd.DataFrame())

    def load_historical(self, symbol: str, timeframe: int, df: pd.DataFrame) -> None:
        """Seed the cache with historical candle data."""
        with self._lock:
            self._candles[symbol][timeframe] = df.tail(self._MAX_CANDLES).copy()
        logger.debug("CandleCache: Loaded %d historical %dm candles for %s.", len(df), timeframe, symbol)

    def compute_snapshot(self, symbol: str, timeframe: int, cfg: SystemConfig) -> Optional[TechnicalSnapshot]:
        """Compute all indicators and return a TechnicalSnapshot."""
        df = self.get_candles(symbol, timeframe)
        if len(df) < 5:
            return None

        closes = df["close"].to_numpy()
        highs = df["high"].to_numpy()
        lows = df["low"].to_numpy()
        volumes = df["volume"].to_numpy()

        adx, pdi, mdi = compute_adx(highs, lows, closes, cfg.indicators.adx_period)
        vol_ma = float(volumes[-cfg.indicators.volume_ma_period:].mean()) if len(volumes) >= cfg.indicators.volume_ma_period else float(volumes.mean())

        # Use last committed candle for pattern detection (in-progress has only 1 tick)
        key = (symbol, timeframe)
        in_prog = self._in_progress.get(key, {})
        ltp = float(in_prog.get("close", closes[-1]))
        # Pattern detection uses the last CLOSED candle — gives complete OHLC bars
        l_open = float(df["open"].iloc[-1])
        l_high = float(df["high"].iloc[-1])
        l_low = float(df["low"].iloc[-1])
        l_close = float(df["close"].iloc[-1])
        l_vol = int(df["volume"].iloc[-1])

        return TechnicalSnapshot(
            symbol=symbol,
            timeframe=timeframe,
            timestamp=datetime.now(),
            ltp=ltp,
            rsi=compute_rsi(closes, cfg.indicators.rsi_period),
            vwap=compute_vwap(df.tail(cfg.indicators.vwap_period)),
            adx=adx,
            plus_di=pdi,
            minus_di=mdi,
            ema_fast=compute_ema(closes, cfg.indicators.ema_fast),
            ema_slow=compute_ema(closes, cfg.indicators.ema_slow),
            atr=compute_atr(highs, lows, closes, cfg.indicators.atr_period),
            last_open=l_open,
            last_high=l_high,
            last_low=l_low,
            last_close=l_close,
            prev_open=float(df["open"].iloc[-2]) if len(df) >= 2 else l_open,
            prev_high=float(df["high"].iloc[-2]) if len(df) >= 2 else l_high,
            prev_low=float(df["low"].iloc[-2]) if len(df) >= 2 else l_low,
            prev_close=float(df["close"].iloc[-2]) if len(df) >= 2 else l_close,
            volume_ma=vol_ma,
            current_volume=l_vol,
        )


# ---------------------------------------------------------------------------
# Option Chain Matrix
# ---------------------------------------------------------------------------

class OptionChainMatrix:
    """
    Maintains the live option chain for one underlying across its active strikes.

    Thread-safe. Updated by the data provider's option tick stream.
    Exposes aggregated snapshots consumed by the strategy engine.
    """

    def __init__(self, underlying: str, config: SystemConfig) -> None:
        self.underlying = underlying
        self._config = config
        self._lock = RLock()
        self._snapshot: Optional[OptionChainSnapshot] = None
        self._pcr_history: deque = deque(maxlen=100)

    def initialize(self, spot: float, atm: float, expiry: date) -> None:
        step = self._strike_step()
        depth = self._config.assets.otm_depth
        with self._lock:
            rows: Dict[float, OptionChainRow] = {}
            for i in range(-depth, depth + 1):
                strike = atm + i * step
                rows[strike] = OptionChainRow(strike=strike)
            self._snapshot = OptionChainSnapshot(
                underlying=self.underlying,
                spot_price=spot,
                atm_strike=atm,
                expiry=expiry,
                timestamp=datetime.now(),
                rows=rows,
            )
        logger.info(
            "OptionChainMatrix: Initialized %s — ATM=%.0f, expiry=%s, strikes=%d",
            self.underlying, atm, expiry, len(rows),
        )

    def on_option_tick(self, tick: OptionTick) -> None:
        if tick.underlying != self.underlying:
            return
        with self._lock:
            if self._snapshot is None:
                return
            row = self._snapshot.rows.get(tick.strike)
            if row is None:
                return                 # Tick outside our monitored strikes
            if tick.option_type == "CE":
                row.call_oi_history.append(row.call_oi)
                row.call_oi = tick.oi
                row.call_change_oi = tick.change_oi
                row.call_volume = tick.volume
                row.call_ltp = tick.ltp
                row.call_iv = tick.iv
            else:
                row.put_oi_history.append(row.put_oi)
                row.put_oi = tick.oi
                row.put_change_oi = tick.change_oi
                row.put_volume = tick.volume
                row.put_ltp = tick.ltp
                row.put_iv = tick.iv
            self._snapshot.timestamp = tick.timestamp

    def on_spot_tick(self, tick: IndexTick) -> None:
        with self._lock:
            if self._snapshot is not None:
                self._snapshot.spot_price = tick.ltp

    def recompute_aggregates(self) -> None:
        """Recompute derived fields: max OI strikes, PCR, etc. Called after each tick batch."""
        with self._lock:
            snap = self._snapshot
            if snap is None or not snap.rows:
                return

            total_call_oi = sum(r.call_oi for r in snap.rows.values())
            total_put_oi = sum(r.put_oi for r in snap.rows.values())

            snap.total_call_oi = total_call_oi
            snap.total_put_oi = total_put_oi
            snap.pcr = total_put_oi / total_call_oi if total_call_oi > 0 else 1.0
            snap.pcr_history.append(snap.pcr)

            # Max absolute OI strikes
            if snap.rows:
                snap.max_call_oi_strike = max(snap.rows, key=lambda s: snap.rows[s].call_oi)
                snap.max_put_oi_strike = max(snap.rows, key=lambda s: snap.rows[s].put_oi)
                snap.max_call_delta_oi_strike = max(snap.rows, key=lambda s: snap.rows[s].call_change_oi)
                snap.max_put_delta_oi_strike = max(snap.rows, key=lambda s: snap.rows[s].put_change_oi)

    def get_snapshot(self) -> Optional[OptionChainSnapshot]:
        with self._lock:
            return self._snapshot

    def get_chain_dataframe(self) -> pd.DataFrame:
        """Return the full chain as a printable/loggable DataFrame."""
        with self._lock:
            if self._snapshot is None:
                return pd.DataFrame()
            records = []
            for s in sorted(self._snapshot.rows):
                r = self._snapshot.rows[s]
                records.append({
                    "strike": s,
                    "call_oi": r.call_oi,
                    "call_ΔOI": r.call_change_oi,
                    "call_vol": r.call_volume,
                    "call_ltp": r.call_ltp,
                    "put_ltp": r.put_ltp,
                    "put_vol": r.put_volume,
                    "put_ΔOI": r.put_change_oi,
                    "put_oi": r.put_oi,
                    "pcr_oi": round(r.pcr_oi, 3),
                })
            df = pd.DataFrame(records).set_index("strike")
            return df

    def _strike_step(self) -> float:
        steps = {
            "NIFTY": 50.0, "FINNIFTY": 50.0, "MIDCPNIFTY": 50.0,
            "BANKNIFTY": 100.0, "SENSEX": 100.0,
        }
        return steps.get(self.underlying, 50.0)

    def detect_oi_spike(self, strike: float, option_type: str, multiplier: float = 2.0) -> bool:
        """Return True if current ΔOI is significantly above recent history for this row."""
        with self._lock:
            if self._snapshot is None:
                return False
            row = self._snapshot.rows.get(strike)
            if row is None:
                return False
            if option_type == "CE":
                history = list(row.call_oi_history)
                current = row.call_change_oi
            else:
                history = list(row.put_oi_history)
                current = row.put_change_oi
            if len(history) < 5:
                return False
            avg_delta = abs(np.mean(np.diff(history))) if len(history) > 1 else 1
            return abs(current) > avg_delta * multiplier


# ---------------------------------------------------------------------------
# Master Matrix Engine — orchestrates both caches
# ---------------------------------------------------------------------------

class MarketMatrixEngine:
    """
    Central engine that consumes raw ticks and updates all internal caches.

    The main trading loop calls process_tick() with every incoming event.
    After processing, it emits updated snapshots via asyncio.Queue for
    downstream strategy evaluation.
    """

    def __init__(self, config: SystemConfig) -> None:
        self._config = config
        self.candle_cache = CandleCache(config)
        self.option_chains: Dict[str, OptionChainMatrix] = {}
        self._snapshot_queue: asyncio.Queue[
            Tuple[TechnicalSnapshot, OptionChainSnapshot]
        ] = asyncio.Queue(maxsize=200)
        self._tick_counter = 0
        self._aggregate_every = 5      # Recompute aggregates every N ticks

        # Initialize chains for all configured indices
        for index in config.assets.indices:
            self.option_chains[index] = OptionChainMatrix(index, config)

    def initialize_chain(self, underlying: str, spot: float, atm: float, expiry: date) -> None:
        self.option_chains[underlying].initialize(spot, atm, expiry)

    def load_historical_candles(self, symbol: str, timeframe: int, df: pd.DataFrame) -> None:
        self.candle_cache.load_historical(symbol, timeframe, df)

    async def process_tick(self, tick: IndexTick | OptionTick) -> None:
        """Route a tick to the appropriate cache and emit snapshots."""
        if isinstance(tick, IndexTick):
            self.candle_cache.on_tick(tick)
            if tick.symbol in self.option_chains:
                self.option_chains[tick.symbol].on_spot_tick(tick)
        elif isinstance(tick, OptionTick):
            if tick.underlying in self.option_chains:
                self.option_chains[tick.underlying].on_option_tick(tick)

        self._tick_counter += 1
        if self._tick_counter % self._aggregate_every == 0:
            await self._emit_snapshots()

    async def _emit_snapshots(self) -> None:
        """Compute and queue snapshots for the active index."""
        active = self._config.assets.active_index
        primary_tf = self._config.assets.candle_timeframes[0]

        tech = self.candle_cache.compute_snapshot(active, primary_tf, self._config)
        chain = self.option_chains.get(active)
        if chain:
            chain.recompute_aggregates()
            chain_snap = chain.get_snapshot()
        else:
            chain_snap = None

        if tech and chain_snap and not self._snapshot_queue.full():
            await self._snapshot_queue.put((tech, chain_snap))

    def get_snapshot_queue(self) -> asyncio.Queue:
        return self._snapshot_queue

    def get_chain_report(self, underlying: str) -> pd.DataFrame:
        chain = self.option_chains.get(underlying)
        if chain:
            return chain.get_chain_dataframe()
        return pd.DataFrame()

    def get_multi_timeframe_snapshots(self, symbol: str) -> List[Optional[TechnicalSnapshot]]:
        """Return snapshots for all configured timeframes (used by multi-TF strategies)."""
        return [
            self.candle_cache.compute_snapshot(symbol, tf, self._config)
            for tf in self._config.assets.candle_timeframes
        ]
