"""
data_layer/tick_recorder.py — High-performance async tick recorder.

Subscribes to INDEX_TICK and OPTION_TICK topics on the EventBus,
buffers rows in memory, and periodically flushes them to compressed
Parquet files on disk using ZStandard compression.

Design:
  • All disk I/O is offloaded to a thread pool via asyncio.to_thread
    so the event loop is never blocked.
  • Each day produces two files per underlying:
      data/recorded/YYYYMMDD/{NIFTY}_spot.parquet
      data/recorded/YYYYMMDD/{NIFTY}_chain.parquet
  • Files are append-written by converting each batch to a new
    Parquet file and concatenating with the existing day file.

No time.sleep. Flush is triggered by a periodic asyncio.sleep(N) task.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import Dict, List

import pandas as pd

from config.global_config import IST, Topic, StorageConfig
from data_layer.base_feeder import EventBus, IndexTick, OptionTick

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Row builders
# ─────────────────────────────────────────────────────────────────────────────

def _index_row(t: IndexTick) -> Dict:
    return {
        "ts": t.timestamp.isoformat(),
        "symbol": t.symbol,
        "ltp": t.ltp,
        "open": t.open,
        "high": t.high,
        "low": t.low,
        "close": t.close,
        "volume": t.volume,
    }


def _option_row(t: OptionTick) -> Dict:
    return {
        "ts": t.timestamp.isoformat(),
        "symbol": t.symbol,
        "underlying": t.underlying,
        "strike": t.strike,
        "option_type": t.option_type,
        "expiry": str(t.expiry),
        "ltp": t.ltp,
        "bid": t.bid,
        "ask": t.ask,
        "oi": t.oi,
        "change_oi": t.change_oi,
        "volume": t.volume,
        "iv": t.iv,
        "delta": t.delta,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Tick Recorder
# ─────────────────────────────────────────────────────────────────────────────

class TickRecorder:
    """
    Subscribes to tick events and flushes buffered rows to Parquet.

    Usage:
        recorder = TickRecorder(bus, storage_cfg)
        asyncio.create_task(recorder.run())
        # later:
        await recorder.stop()
    """

    def __init__(self, bus: EventBus, cfg: StorageConfig) -> None:
        self._bus = bus
        self._cfg = cfg
        self._running = False

        # Per-underlying in-memory row buffers
        self._spot_buf: Dict[str, List[Dict]] = defaultdict(list)
        self._chain_buf: Dict[str, List[Dict]] = defaultdict(list)

        self._idx_queue = bus.subscribe(Topic.INDEX_TICK)
        self._opt_queue = bus.subscribe(Topic.OPTION_TICK)

    async def run(self) -> None:
        """Launch consumer tasks and periodic flush task."""
        self._running = True
        await asyncio.gather(
            self._consume_index(),
            self._consume_option(),
            self._flush_loop(),
        )

    async def stop(self) -> None:
        self._running = False
        await self._flush_all()     # Final flush on shutdown

    # ── Consumers ─────────────────────────────────────────────────────────────

    async def _consume_index(self) -> None:
        while self._running:
            try:
                tick: IndexTick = await asyncio.wait_for(self._idx_queue.get(), timeout=1.0)
                self._spot_buf[tick.symbol].append(_index_row(tick))
            except asyncio.TimeoutError:
                continue

    async def _consume_option(self) -> None:
        while self._running:
            try:
                tick: OptionTick = await asyncio.wait_for(self._opt_queue.get(), timeout=1.0)
                self._chain_buf[tick.underlying].append(_option_row(tick))
            except asyncio.TimeoutError:
                continue

    # ── Flush ─────────────────────────────────────────────────────────────────

    async def _flush_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._cfg.recorder_flush_interval_seconds)
            await self._flush_all()

    async def _flush_all(self) -> None:
        date_str = datetime.now(IST).strftime("%Y%m%d")
        day_dir = os.path.join(self._cfg.recorded_dir, date_str)
        await asyncio.to_thread(os.makedirs, day_dir, exist_ok=True)

        for symbol, rows in list(self._spot_buf.items()):
            if not rows:
                continue
            batch = rows.copy()
            self._spot_buf[symbol].clear()
            path = os.path.join(day_dir, f"{symbol}_spot.parquet")
            await asyncio.to_thread(self._append_parquet, path, batch)

        for underlying, rows in list(self._chain_buf.items()):
            if not rows:
                continue
            batch = rows.copy()
            self._chain_buf[underlying].clear()
            path = os.path.join(day_dir, f"{underlying}_chain.parquet")
            await asyncio.to_thread(self._append_parquet, path, batch)

    def _append_parquet(self, path: str, rows: List[Dict]) -> None:
        """Append rows to a Parquet file (creates file if absent)."""
        new_df = pd.DataFrame(rows)
        compression = self._cfg.recorder_compression
        if os.path.exists(path):
            try:
                existing = pd.read_parquet(path)
                combined = pd.concat([existing, new_df], ignore_index=True)
            except Exception as exc:
                logger.warning("TickRecorder: Could not read existing file %s: %s. Overwriting.", path, exc)
                combined = new_df
        else:
            combined = new_df
        combined.to_parquet(
            path,
            compression=compression,
            row_group_size=self._cfg.recorder_row_group_size,
            index=False,
        )
        logger.debug("TickRecorder: Flushed %d rows to %s.", len(new_df), path)

    def buffer_stats(self) -> Dict[str, int]:
        stats = {f"spot_{k}": len(v) for k, v in self._spot_buf.items()}
        stats.update({f"chain_{k}": len(v) for k, v in self._chain_buf.items()})
        return stats
