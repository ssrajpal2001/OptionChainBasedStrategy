"""
strategies/base_strategy.py — signal domain objects.

Holds the immutable trade-signal value objects shared across the system
(SignalPackage + its Direction / StrategyID enums). The ExecutionRouter and
parallel worker pool import SignalPackage from here.

The legacy ConfluenceEngine + BaseStrategy ABC (the A/B/C confluence path) were
removed — the three live strategies (SellStraddle, IronCondor, TrapScanner) emit
their own order events directly and do not go through this module.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto

from config.global_config import IST


# ─────────────────────────────────────────────────────────────────────────────
# Signal Domain Objects
# ─────────────────────────────────────────────────────────────────────────────

class Direction(Enum):
    LONG  = auto()
    SHORT = auto()


class StrategyID(Enum):
    TRAP_SCANNER = "TrapScanner"
    SELL_STRADDLE = "SellStraddle"
    IRON_CONDOR = "IronCondor"


@dataclass(frozen=True)
class SignalPackage:
    """Fully parameterized, immutable trade signal → ExecutionRouter."""
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
