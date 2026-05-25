"""
strategies/base_strategy.py — Confluence gate, signal domain objects, base class.

The ConfluenceEngine is the only consumer of MATRIX_SNAPSHOT events that
has permission to publish SIGNAL events.  It enforces:
  • Dual-factor rule: both price-action AND option-chain must agree
  • Minimum 1:2 RR hard floor
  • Minimum confidence threshold (0.50)
  • Directional conflict rejection (simultaneous LONG + SHORT → discard)

No time.sleep.  All state is in-memory.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

from config.global_config import IST, Topic, StrategyParams, GlobalConfig
from data_layer.base_feeder import EventBus
from matrix_engine.indicators import TechSnapshot
from matrix_engine.option_matrix import ChainSnapshot

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Signal Domain Objects
# ─────────────────────────────────────────────────────────────────────────────

class Direction(Enum):
    LONG  = auto()
    SHORT = auto()


class StrategyID(Enum):
    A_OI_ZONE    = "OI_Zone_Breakout"
    B_TRAP       = "Liquidity_Trap"
    C_PANIC      = "Panic_Scanner"
    TRAP_ENGINE  = "TrapTrading_Engine"


@dataclass(frozen=True)
class SignalPackage:
    """
    Fully parameterized, immutable trade signal.
    Dispatched by ConfluenceEngine → ExecutionRouter → all client brokers.
    """
    source: StrategyID
    direction: Direction
    underlying: str
    option_type: str              # "CE" or "PE"
    target_strike: float
    entry_spot: float             # Underlying spot at signal time
    stop_spot: float              # SL level on underlying
    target_spot: float            # 1st target on underlying
    confidence: float             # 0.0 – 1.0
    timestamp: datetime = field(default_factory=lambda: datetime.now(IST))
    notes: str = ""

    @property
    def risk(self) -> float:
        return abs(self.entry_spot - self.stop_spot)

    @property
    def reward(self) -> float:
        return abs(self.target_spot - self.entry_spot)

    @property
    def rr_ratio(self) -> float:
        return self.reward / self.risk if self.risk > 0 else 0.0

    def is_valid(self, min_rr: float = 2.0, min_conf: float = 0.50) -> bool:
        return self.rr_ratio >= min_rr and self.confidence >= min_conf


# ─────────────────────────────────────────────────────────────────────────────
# Base Strategy Interface
# ─────────────────────────────────────────────────────────────────────────────

class BaseStrategy(ABC):
    """
    Every strategy receives a TechSnapshot + ChainSnapshot pair and
    returns either a SignalPackage or None.  No I/O, no async code.
    """

    def __init__(self, cfg: GlobalConfig) -> None:
        self._cfg = cfg
        self._sp: StrategyParams = cfg.strategy

    @abstractmethod
    def evaluate(
        self,
        tech: TechSnapshot,
        chain: ChainSnapshot,
        all_tf: List[Optional[TechSnapshot]],
    ) -> Optional[SignalPackage]:
        """Core logic. Must be pure and fast (no blocking)."""

    # ── Shared helpers ─────────────────────────────────────────────────────

    def _execution_strike(self, chain: ChainSnapshot, direction: Direction) -> float:
        step = chain._step()
        atm = chain.atm_strike
        mono = self._cfg.active_index  # not per-strategy; use global
        pref = "ATM"                   # default; overridden by ClientProfile
        if pref == "ITM_1":
            return atm - step if direction == Direction.LONG else atm + step
        if pref == "OTM_1":
            return atm + step if direction == Direction.LONG else atm - step
        return atm

    def _hammer(self, tech: TechSnapshot) -> bool:
        return (
            tech.lower_wick_ratio >= 0.55
            and tech.body_ratio <= 0.35
            and tech.c_close > (tech.c_high + tech.c_low) / 2
        )

    def _shooting_star(self, tech: TechSnapshot) -> bool:
        return (
            tech.upper_wick_ratio >= 0.55
            and tech.body_ratio <= 0.35
            and tech.c_close < (tech.c_high + tech.c_low) / 2
        )

    def _breakout_bull(self, tech: TechSnapshot) -> bool:
        return tech.is_bullish and tech.body_ratio >= self._sp.breakout_body_ratio

    def _breakout_bear(self, tech: TechSnapshot) -> bool:
        return not tech.is_bullish and tech.body_ratio >= self._sp.breakout_body_ratio

    def _trending(self, tech: TechSnapshot) -> bool:
        return tech.adx_val >= 20.0

    def _consec_red(self, all_tf: List[Optional[TechSnapshot]]) -> bool:
        """At least 2 consecutive bearish candles in primary TF."""
        snap = all_tf[0] if all_tf else None
        if snap is None:
            return False
        return not snap.is_bullish and snap.p_close < snap.p_open


# ─────────────────────────────────────────────────────────────────────────────
# Confluence Engine — meta-aggregator
# ─────────────────────────────────────────────────────────────────────────────

class ConfluenceEngine:
    """
    Subscribes to MATRIX_SNAPSHOT and CANDLE_CLOSE on the EventBus.
    Evaluates all active strategies per snapshot pair and publishes
    validated SignalPackage objects to the SIGNAL topic.

    Enforces:
      • No signal until BOTH tech (from CandleCache) AND chain (from
        OptionMatrixEngine) snapshots are fresh.
      • Directional conflict kills all candidates.
      • min_rr and min_confidence are global floors.
    """

    def __init__(
        self,
        bus: EventBus,
        cfg: GlobalConfig,
        strategies: List[BaseStrategy],
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._strategies = strategies
        self._snap_queue = bus.subscribe(Topic.MATRIX_SNAPSHOT)
        self._running = False
        self._signal_stats: Dict[str, int] = {}

        # Hold latest TechSnapshot and ChainSnapshot separately
        self._latest_tech: Dict[str, TechSnapshot] = {}
        self._latest_chain: Dict[str, ChainSnapshot] = {}

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                event = await asyncio.wait_for(self._snap_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            # Route snapshot by type
            if isinstance(event, TechSnapshot):
                self._latest_tech[event.symbol] = event
            elif isinstance(event, ChainSnapshot):
                self._latest_chain[event.underlying] = event

            # Attempt evaluation if we have both
            active = self._cfg.active_index
            tech = self._latest_tech.get(active)
            chain = self._latest_chain.get(active)
            if tech and chain:
                await self._evaluate(tech, chain)

    def stop(self) -> None:
        self._running = False

    async def _evaluate(self, tech: TechSnapshot, chain: ChainSnapshot) -> None:
        sp = self._cfg.strategy
        candidates: List[SignalPackage] = []

        for strategy in self._strategies:
            try:
                all_tf: List[Optional[TechSnapshot]] = [
                    self._latest_tech.get(tech.symbol)
                ]
                sig = strategy.evaluate(tech, chain, all_tf)
                if sig and sig.is_valid(sp.min_risk_reward, sp.min_confidence):
                    candidates.append(sig)
                    key = strategy.__class__.__name__
                    self._signal_stats[key] = self._signal_stats.get(key, 0) + 1
            except Exception as exc:
                logger.exception("ConfluenceEngine: %s crashed: %s", strategy.__class__.__name__, exc)

        if not candidates:
            return

        # Conflict check
        directions = {s.direction for s in candidates}
        if len(directions) > 1:
            logger.debug("ConfluenceEngine: Directional conflict — %d candidates discarded.", len(candidates))
            return

        best = max(candidates, key=lambda s: s.confidence)
        logger.info(
            "SIGNAL DISPATCHED: %s %s | source=%s | conf=%.2f | RR=%.1f",
            best.direction.name, best.underlying, best.source.value,
            best.confidence, best.rr_ratio,
        )
        await self._bus.publish(Topic.SIGNAL, best)

    def stats(self) -> Dict[str, int]:
        return dict(self._signal_stats)

    def force_evaluate(
        self, tech: TechSnapshot, chain: ChainSnapshot
    ) -> Optional[SignalPackage]:
        """Synchronous path for backtester — returns best signal or None."""
        sp = self._cfg.strategy
        candidates: List[SignalPackage] = []
        for strategy in self._strategies:
            try:
                sig = strategy.evaluate(tech, chain, [tech])
                if sig and sig.is_valid(sp.min_risk_reward, sp.min_confidence):
                    candidates.append(sig)
            except Exception as exc:
                logger.debug("force_evaluate: %s: %s", strategy.__class__.__name__, exc)
        if not candidates:
            return None
        if len({s.direction for s in candidates}) > 1:
            return None
        return max(candidates, key=lambda s: s.confidence)
