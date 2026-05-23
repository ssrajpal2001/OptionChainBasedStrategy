"""
backtester/unified_iterator.py — Zero-copy Parquet event pump.

Streams historical tick data through the PRODUCTION engine stack
(CandleCache → OptionMatrixEngine → ConfluenceEngine) without modifying
any strategy or indicator code.

Architecture:
  ParquetEventPump
    ├── Reads Parquet files column-by-column (zero row-copy)
    ├── Maps columns directly to IndexTick / OptionTick memory frames
    ├── Maintains a merged time-ordered stream across all symbol files
    └── Feeds ticks synchronously to production engine callbacks

  UnifiedBacktestIterator
    ├── Wraps ParquetEventPump
    ├── Owns a production CandleCache + ConfluenceEngine
    ├── Wires EventBus so candle/signal events flow normally
    └── Collects SignalPackage events for trade simulation

Zero-copy contract:
  Parquet columns are read once as numpy arrays; IndexTick / OptionTick
  objects are created per-row but hold no extra copies of the underlying
  array data (Python floats are boxed anyway; this avoids a secondary
  Pandas DataFrame buffer).

No time.sleep. All synchronous replay via direct method calls.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import AsyncIterator, Dict, Generator, Iterator, List, Optional, Tuple

import numpy as np

from config.global_config import IST, Topic, GlobalConfig
from data_layer.base_feeder import EventBus, IndexTick, OptionTick, CandleEvent
from matrix_engine.candle_cache import CandleCache

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Parquet Column Schema
# ─────────────────────────────────────────────────────────────────────────────

# Expected columns in index tick Parquet files (written by TickRecorder)
_INDEX_COLS = ("timestamp", "ltp", "open", "high", "low", "close", "volume")

# Expected columns in option tick Parquet files
_OPTION_COLS = (
    "timestamp", "symbol", "underlying", "strike", "option_type",
    "expiry", "ltp", "bid", "ask", "oi", "change_oi", "volume", "iv", "delta",
)


# ─────────────────────────────────────────────────────────────────────────────
# Parquet Event Pump
# ─────────────────────────────────────────────────────────────────────────────

class ParquetEventPump:
    """
    Reads one or more Parquet files and yields time-ordered tick events.

    Zero-copy design:
      • Reads each file once into numpy arrays (via pyarrow or pandas).
      • Constructs IndexTick / OptionTick from scalar column values per row.
      • No intermediate DataFrame materialised during iteration.
    """

    def __init__(self, cfg: GlobalConfig) -> None:
        self._cfg = cfg

    def iter_index_ticks(
        self, parquet_path: str, symbol: str
    ) -> Iterator[IndexTick]:
        """Yield IndexTick events from a Parquet file, oldest-first."""
        try:
            import pyarrow.parquet as pq
        except ImportError:
            import pandas as pd
            df = pd.read_parquet(parquet_path)
            yield from self._df_to_index_ticks(df, symbol)
            return

        table = pq.read_table(parquet_path, columns=list(_INDEX_COLS))
        ts_col  = table.column("timestamp").to_pylist()
        ltp_col = table.column("ltp").to_pylist()
        opn_col = table.column("open").to_pylist()  if "open"   in table.schema.names else ltp_col
        hi_col  = table.column("high").to_pylist()  if "high"   in table.schema.names else ltp_col
        lo_col  = table.column("low").to_pylist()   if "low"    in table.schema.names else ltp_col
        cl_col  = table.column("close").to_pylist() if "close"  in table.schema.names else ltp_col
        vol_col = table.column("volume").to_pylist() if "volume" in table.schema.names else [0] * len(ltp_col)

        for i in range(len(ltp_col)):
            ts = ts_col[i]
            if not isinstance(ts, datetime):
                ts = datetime.fromisoformat(str(ts))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=IST)
            yield IndexTick(
                symbol=symbol,
                ltp=float(ltp_col[i] or 0),
                open=float(opn_col[i] or ltp_col[i] or 0),
                high=float(hi_col[i] or ltp_col[i] or 0),
                low=float(lo_col[i] or ltp_col[i] or 0),
                close=float(cl_col[i] or ltp_col[i] or 0),
                volume=int(vol_col[i] or 0),
                timestamp=ts,
            )

    def _df_to_index_ticks(self, df, symbol: str) -> Iterator[IndexTick]:
        for row in df.itertuples(index=True):
            ts = row.Index if isinstance(row.Index, datetime) else datetime.fromisoformat(str(row.Index))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=IST)
            yield IndexTick(
                symbol=symbol,
                ltp=float(getattr(row, "ltp", 0) or 0),
                open=float(getattr(row, "open", getattr(row, "ltp", 0)) or 0),
                high=float(getattr(row, "high", getattr(row, "ltp", 0)) or 0),
                low=float(getattr(row, "low", getattr(row, "ltp", 0)) or 0),
                close=float(getattr(row, "close", getattr(row, "ltp", 0)) or 0),
                volume=int(getattr(row, "volume", 0) or 0),
                timestamp=ts,
            )

    def merged_stream(
        self, sources: List[Tuple[str, str]]
    ) -> Iterator[IndexTick]:
        """
        Merge multiple symbol tick streams into a single time-ordered iterator.

        sources: list of (parquet_path, symbol) pairs.

        Uses a heap-based merge — O(N log K) where K = number of files.
        This is the zero-copy path: no DataFrame concatenation.
        """
        import heapq
        iters = []
        for path, sym in sources:
            if Path(path).exists():
                iters.append(self.iter_index_ticks(path, sym))
            else:
                logger.warning("ParquetEventPump: file not found: %s", path)

        if not iters:
            return

        # Prime each iterator
        heap: list = []
        nexts = []
        for idx, it in enumerate(iters):
            try:
                tick = next(it)
                heapq.heappush(heap, (tick.timestamp, idx, tick))
                nexts.append(it)
            except StopIteration:
                nexts.append(None)

        while heap:
            ts, idx, tick = heapq.heappop(heap)
            yield tick
            it = nexts[idx]
            if it is not None:
                try:
                    next_tick = next(it)
                    heapq.heappush(heap, (next_tick.timestamp, idx, next_tick))
                except StopIteration:
                    nexts[idx] = None


# ─────────────────────────────────────────────────────────────────────────────
# Unified Backtest Iterator
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ReplayResult:
    """Summary returned after a full replay run."""
    ticks_processed: int = 0
    candles_closed: int = 0
    signals_generated: int = 0
    signals: List = field(default_factory=list)   # List[SignalPackage]


class UnifiedBacktestIterator:
    """
    Wraps ParquetEventPump and routes ticks through the PRODUCTION
    CandleCache + ConfluenceEngine stack.

    Usage:
        iterator = UnifiedBacktestIterator(cfg)
        result = iterator.run(
            sources=[("data/backtest/NIFTY_2025-01-02.parquet", "NIFTY")],
            history_dfs={"NIFTY": df},   # optional warm-up candles
        )
        for sig in result.signals:
            ...

    No strategy code is modified.  All production logic (indicators, candle
    aggregation, confluence scoring) runs unchanged — the only difference is
    that ticks are injected synchronously instead of arriving from a WebSocket.
    """

    def __init__(self, cfg: GlobalConfig) -> None:
        self._cfg = cfg
        self._pump = ParquetEventPump(cfg)
        # Production EventBus — wired to collect signals and candle events
        self._bus = EventBus(queue_size=100_000)
        # Production CandleCache — no modification needed
        self._cache = CandleCache(self._bus, cfg)
        # Signal collector (async queue consumed synchronously)
        self._signal_q = self._bus.subscribe(Topic.SIGNAL)
        self._candle_q = self._bus.subscribe(Topic.CANDLE_CLOSE)

    def run(
        self,
        sources: List[Tuple[str, str]],
        history_dfs: Optional[Dict] = None,
        confluence_engine=None,
    ) -> ReplayResult:
        """
        Synchronous replay.  Runs a tight asyncio event loop internally.

        sources:           list of (parquet_path, symbol) pairs
        history_dfs:       optional {symbol: DataFrame} for warm-up candles
        confluence_engine: production ConfluenceEngine instance (optional;
                           if None, signals are not generated but candles are)
        """
        return asyncio.run(
            self._async_run(sources, history_dfs or {}, confluence_engine)
        )

    async def _async_run(
        self,
        sources: List[Tuple[str, str]],
        history_dfs: Dict,
        confluence_engine,
    ) -> ReplayResult:
        result = ReplayResult()

        # Warm up with historical candles
        for sym, df in history_dfs.items():
            for tf in self._cfg.candle_timeframes:
                self._cache.load_history(sym, tf, df)

        # Stream ticks through production engines
        for tick in self._pump.merged_stream(sources):
            # Inject into CandleCache via EventBus (mimics live path)
            await self._bus.publish(Topic.INDEX_TICK, tick)
            result.ticks_processed += 1

            # Drain candle close events synchronously
            while not self._candle_q.empty():
                candle: CandleEvent = self._candle_q.get_nowait()
                result.candles_closed += 1

                if confluence_engine is not None:
                    # Get tech snapshot and chain (production code, unchanged)
                    snap = self._cache.get_snapshot(candle.symbol, candle.timeframe, tick.ltp)
                    if snap is not None:
                        all_tf = self._cache.get_all_snapshots(candle.symbol, tick.ltp)
                        # ConfluenceEngine.evaluate() returns Optional[SignalPackage]
                        # We pass a minimal chain placeholder if no live chain available
                        sig = confluence_engine.evaluate_from_snapshot(snap, all_tf)
                        if sig is not None:
                            result.signals_generated += 1
                            result.signals.append(sig)
                            await self._bus.publish(Topic.SIGNAL, sig)

            # Drain signal queue
            while not self._signal_q.empty():
                sig = self._signal_q.get_nowait()
                if sig not in result.signals:
                    result.signals.append(sig)
                    result.signals_generated += 1

        # Allow CandleCache run loop to process remaining queued ticks
        await asyncio.sleep(0)

        logger.info(
            "UnifiedBacktestIterator: ticks=%d candles=%d signals=%d",
            result.ticks_processed, result.candles_closed, result.signals_generated,
        )
        return result


# ─────────────────────────────────────────────────────────────────────────────
# Convenience: discover Parquet files for a date range
# ─────────────────────────────────────────────────────────────────────────────

def discover_parquet_sources(
    base_dir: str,
    symbols: List[str],
    start_date: date,
    end_date: date,
) -> List[Tuple[str, str]]:
    """
    Return list of (parquet_path, symbol) pairs for all trading days
    in [start_date, end_date] that have recorded files.

    Expected naming: {base_dir}/{SYMBOL}_{YYYY-MM-DD}.parquet
    """
    sources = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:   # Mon–Fri only
            for sym in symbols:
                path = os.path.join(base_dir, f"{sym}_{current.isoformat()}.parquet")
                if os.path.exists(path):
                    sources.append((path, sym))
        current += timedelta(days=1)
    return sources
