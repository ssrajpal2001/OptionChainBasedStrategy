"""
matrix_engine/option_matrix.py — Live option chain matrix engine.

Subscribes to OPTION_TICK on the EventBus.  Maintains a thread-safe
ATM ± depth option chain, recomputes aggregates (max OI strikes, PCR,
ΔOI histories) after each tick batch, and publishes enriched
ChainSnapshot objects for strategy consumption.

No time.sleep. All synchronization via RLock (fine-grained per-chain).
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime
from threading import RLock
from typing import Dict, List, Optional

import numpy as np

from config.global_config import IST, Topic, GlobalConfig
from data_layer.base_feeder import EventBus, OptionTick, IndexTick

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Per-Strike Row
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ChainRow:
    strike: float
    call_oi: int = 0
    call_doi: int = 0           # ΔOI (change in OI)
    call_vol: int = 0
    call_ltp: float = 0.0
    call_iv: float = 0.0
    put_ltp: float = 0.0
    put_iv: float = 0.0
    put_vol: int = 0
    put_doi: int = 0
    put_oi: int = 0
    # Rolling ΔOI history for spike detection
    _call_oi_hist: deque = field(default_factory=lambda: deque(maxlen=30))
    _put_oi_hist:  deque = field(default_factory=lambda: deque(maxlen=30))

    @property
    def pcr(self) -> float:
        return self.put_oi / self.call_oi if self.call_oi > 0 else 0.0

    def call_doi_spike(self, multiplier: float = 2.0) -> bool:
        hist = list(self._call_oi_hist)
        if len(hist) < 5:
            return False
        avg = abs(np.mean(np.diff(hist))) if len(hist) > 1 else 1.0
        return abs(self.call_doi) > avg * multiplier

    def put_doi_spike(self, multiplier: float = 2.0) -> bool:
        hist = list(self._put_oi_hist)
        if len(hist) < 5:
            return False
        avg = abs(np.mean(np.diff(hist))) if len(hist) > 1 else 1.0
        return abs(self.put_doi) > avg * multiplier


# ─────────────────────────────────────────────────────────────────────────────
# Chain Snapshot — passed to strategy engine
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ChainSnapshot:
    underlying: str
    spot: float
    atm_strike: float
    expiry: date
    timestamp: datetime
    rows: Dict[float, ChainRow] = field(default_factory=dict)

    # Aggregates — set by recompute()
    max_call_oi_strike: float = 0.0
    max_put_oi_strike: float = 0.0
    max_call_doi_strike: float = 0.0
    max_put_doi_strike: float = 0.0
    total_call_oi: int = 0
    total_put_oi: int = 0
    pcr: float = 1.0
    pcr_history: deque = field(default_factory=lambda: deque(maxlen=100))

    def row(self, strike: float) -> Optional[ChainRow]:
        return self.rows.get(strike)

    def strikes(self) -> List[float]:
        return sorted(self.rows)

    def call_oi_at(self, strike: float) -> int:
        r = self.rows.get(strike)
        return r.call_oi if r else 0

    def put_oi_at(self, strike: float) -> int:
        r = self.rows.get(strike)
        return r.put_oi if r else 0

    def recompute(self) -> None:
        if not self.rows:
            return
        tc = sum(r.call_oi for r in self.rows.values())
        tp = sum(r.put_oi for r in self.rows.values())
        self.total_call_oi = tc
        self.total_put_oi = tp
        self.pcr = tp / tc if tc > 0 else 1.0
        self.pcr_history.append(self.pcr)
        self.max_call_oi_strike  = max(self.rows, key=lambda s: self.rows[s].call_oi, default=0.0)
        self.max_put_oi_strike   = max(self.rows, key=lambda s: self.rows[s].put_oi,  default=0.0)
        self.max_call_doi_strike = max(self.rows, key=lambda s: self.rows[s].call_doi, default=0.0)
        self.max_put_doi_strike  = max(self.rows, key=lambda s: self.rows[s].put_doi,  default=0.0)

    def pcr_smooth(self, n: int = 5) -> float:
        hist = list(self.pcr_history)
        if not hist:
            return self.pcr
        return float(np.mean(hist[-n:] if len(hist) >= n else hist))

    def total_doi_near_atm(self, opt_type: str, half_width: int = 1) -> int:
        step = self._step()
        strikes = [self.atm_strike + i * step for i in range(-half_width, half_width + 1)]
        total = 0
        for s in strikes:
            r = self.rows.get(s)
            if r:
                total += r.call_doi if opt_type == "CE" else r.put_doi
        return total

    def _step(self) -> float:
        steps = {"BANKNIFTY": 100.0, "SENSEX": 100.0}
        return steps.get(self.underlying, 50.0)


# ─────────────────────────────────────────────────────────────────────────────
# Single-Underlying Option Matrix
# ─────────────────────────────────────────────────────────────────────────────

class OptionMatrix:
    """
    Maintains the live chain for one underlying.
    Thread-safe.  Updated by OptionMatrixEngine on every option tick.
    """

    def __init__(self, underlying: str, cfg: GlobalConfig) -> None:
        self._underlying = underlying
        self._cfg = cfg
        self._lock = RLock()
        self._snap: Optional[ChainSnapshot] = None
        self._tick_count = 0
        self._recompute_every = 10     # Recompute aggregates every N ticks

    def initialize(self, spot: float, expiry: date) -> None:
        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0)
        atm = round(spot / step) * step
        depth = self._cfg.chain_depth
        with self._lock:
            rows = {atm + i * step: ChainRow(strike=atm + i * step)
                    for i in range(-depth, depth + 1)}
            self._snap = ChainSnapshot(
                underlying=self._underlying, spot=spot,
                atm_strike=atm, expiry=expiry,
                timestamp=datetime.now(IST), rows=rows,
            )
        logger.info("OptionMatrix: %s initialized | ATM=%.0f expiry=%s strikes=%d",
                    self._underlying, atm, expiry, len(rows))

    def on_option_tick(self, tick: OptionTick) -> bool:
        """Update the chain row. Returns True if aggregates should be recomputed."""
        with self._lock:
            if self._snap is None:
                return False
            row = self._snap.rows.get(tick.strike)
            if row is None:
                return False
            if tick.option_type == "CE":
                row._call_oi_hist.append(row.call_oi)
                row.call_oi, row.call_doi, row.call_vol = tick.oi, tick.change_oi, tick.volume
                row.call_ltp, row.call_iv = tick.ltp, tick.iv
            else:
                row._put_oi_hist.append(row.put_oi)
                row.put_oi, row.put_doi, row.put_vol = tick.oi, tick.change_oi, tick.volume
                row.put_ltp, row.put_iv = tick.ltp, tick.iv
            self._snap.timestamp = tick.timestamp
            self._tick_count += 1
            return self._tick_count % self._recompute_every == 0

    def on_spot_tick(self, spot: float) -> None:
        with self._lock:
            if self._snap:
                self._snap.spot = spot

    def recompute(self) -> None:
        with self._lock:
            if self._snap:
                self._snap.recompute()

    def snapshot(self) -> Optional[ChainSnapshot]:
        with self._lock:
            return self._snap

    def is_initialized(self) -> bool:
        with self._lock:
            return self._snap is not None


# ─────────────────────────────────────────────────────────────────────────────
# Option Matrix Engine — multi-underlying orchestrator
# ─────────────────────────────────────────────────────────────────────────────

class OptionMatrixEngine:
    """
    Subscribes to OPTION_TICK and INDEX_TICK.
    After each batch of ticks triggers a recompute cycle, publishes an
    enriched ChainSnapshot on MATRIX_SNAPSHOT topic.

    CandleCache and OptionMatrixEngine both publish to MATRIX_SNAPSHOT —
    the strategy engine selects the most recent one.
    """

    def __init__(self, bus: EventBus, cfg: GlobalConfig) -> None:
        self._bus = bus
        self._cfg = cfg
        self._matrices: Dict[str, OptionMatrix] = {
            idx: OptionMatrix(idx, cfg) for idx in cfg.monitored_indices
        }
        self._opt_queue = bus.subscribe(Topic.OPTION_TICK)
        self._idx_queue = bus.subscribe(Topic.INDEX_TICK)
        self._running = False

    def initialize(self, underlying: str, spot: float, expiry: date) -> None:
        self._matrices[underlying].initialize(spot, expiry)

    async def run(self) -> None:
        self._running = True
        await asyncio.gather(self._consume_options(), self._consume_index())

    def stop(self) -> None:
        self._running = False

    async def _consume_options(self) -> None:
        while self._running:
            try:
                tick: OptionTick = await asyncio.wait_for(self._opt_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            mat = self._matrices.get(tick.underlying)
            if mat:
                should_emit = mat.on_option_tick(tick)
                if should_emit:
                    mat.recompute()
                    snap = mat.snapshot()
                    if snap:
                        await self._bus.publish(Topic.MATRIX_SNAPSHOT, snap)

    async def _consume_index(self) -> None:
        while self._running:
            try:
                tick: IndexTick = await asyncio.wait_for(self._idx_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            mat = self._matrices.get(tick.symbol)
            if mat:
                mat.on_spot_tick(tick.ltp)

    def get_snapshot(self, underlying: str) -> Optional[ChainSnapshot]:
        mat = self._matrices.get(underlying)
        return mat.snapshot() if mat else None
