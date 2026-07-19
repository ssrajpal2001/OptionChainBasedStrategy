"""
strategies/base_strategy.py — signal domain objects.

Holds the immutable trade-signal value objects shared across the system
(SignalPackage + its Direction / StrategyID enums). The ExecutionRouter and
parallel worker pool import SignalPackage from here.

The legacy ConfluenceEngine + BaseStrategy ABC (the A/B/C confluence path) were
removed — SellStraddle emits its own order events directly and does not go
through this module. IronCondor and TrapScanner were removed 2026-07-18.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Optional

from config.global_config import IST


# ─────────────────────────────────────────────────────────────────────────────
# Signal Domain Objects
# ─────────────────────────────────────────────────────────────────────────────

class Direction(Enum):
    LONG  = auto()
    SHORT = auto()


class StrategyID(Enum):
    SELL_STRADDLE = "SellStraddle"
    V4_CASCADE = "V4Cascade"


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
    # 2026-07-19 — option-premium-denominated risk (v4_cascade): when
    # populated, is_valid()/rr_ratio use THESE instead of the spot-based
    # fields above, since v4_cascade's real risk lives on the tracking
    # contract's own premium (HTF/MTF zone structure), not a fabricated
    # spot-equivalent. None for strategies (e.g. sell_straddle) that don't
    # set them — behavior is unchanged for those.
    premium_entry: Optional[float] = None
    premium_sl: Optional[float] = None
    premium_target: Optional[float] = None

    @property
    def _uses_premium_risk(self) -> bool:
        return self.premium_entry is not None and self.premium_sl is not None and self.premium_target is not None

    @property
    def risk(self) -> float:
        if self._uses_premium_risk:
            return abs(self.premium_entry - self.premium_sl)
        return abs(self.entry_spot - self.stop_spot)

    @property
    def reward(self) -> float:
        if self._uses_premium_risk:
            return abs(self.premium_target - self.premium_entry)
        return abs(self.target_spot - self.entry_spot)

    @property
    def rr_ratio(self) -> float:
        return self.reward / self.risk if self.risk > 0 else 0.0

    def is_valid(self, min_rr: float = 2.0, min_conf: float = 0.50) -> bool:
        return self.rr_ratio >= min_rr and self.confidence >= min_conf
