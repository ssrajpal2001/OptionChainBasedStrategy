"""
strategies/v4_cascade/spot_confirm.py — Index/Futures-chart structural
confirmation (Gate 1 of the 3-gate funnel, per the 2026-07-20 Index/Premium
decoupling).

Spec: "the Index/Futures chart must be printing a V4 Bear Trap close out of
its corresponding 75m demand zone" (arms CE) / "a V4 Bull Trap close ...
sweeping structural highs" (arms PE). Genuine institutional liquidity sweeps
happen where resting retail stop pools actually live — the Index/Futures
order book — not on an option premium chart, which is warped by theta decay
and IV and has no real resting-liquidity structure of its own. This module's
classification is now consumed as a HARD discovery gate by
IndexGatedPremiumScanner (zone_state.py) for NIFTY/CRUDEOIL, not merely a
late trigger-time bias check.

Fixed at 75m — no multiplier ladder.

Pure — fed 75m CandleEvent-shaped bars directly, no bus/broker dependency.
"""
from __future__ import annotations

from collections import deque
from datetime import datetime
from typing import Deque, Optional

from strategies.v4_cascade.dataclasses import IndexTrapKind, RollingBaseZone
from strategies.v4_cascade.rolling_base import find_bear_zone, find_bull_zone

_MAX_75M_BARS = 200


class SpotConfirmTracker:
    """Classifies each closed 75m Index/Futures bar's bucket as NONE /
    BEAR_TRAP_CONFIRMED / BULL_TRAP_CONFIRMED, based on the same 3-candle
    sweep+reclaim pattern applied to the Index's own structure (demand-zone
    sweep+reclaim = bear trap = bullish read, arms CE; structural-high
    sweep+reclaim = bull trap = bearish read, arms PE)."""

    def __init__(self) -> None:
        self._bars_75m: Deque = deque(maxlen=_MAX_75M_BARS)
        self.current_kind: IndexTrapKind = IndexTrapKind.NONE
        self.current_bucket_ts: Optional[datetime] = None
        self.current_zone: Optional[RollingBaseZone] = None

    def on_75m_bar(self, bar) -> IndexTrapKind:
        """Feed one closed 75m Index/Futures bar; returns the classification
        that now applies to this bucket (also stored on
        .current_kind/.current_bucket_ts/.current_zone)."""
        self._bars_75m.append(bar)
        bars = list(self._bars_75m)

        bear = find_bear_zone(bars)
        bull = find_bull_zone(bars)

        kind = IndexTrapKind.NONE
        zone: Optional[RollingBaseZone] = None
        # If both somehow resolve (shouldn't for the same 3 bars, but a longer
        # history could hold both an older bear and an older bull pattern),
        # prefer whichever reclaimed most recently.
        if bear is not None and (bull is None or bear.lock_ts >= bull.lock_ts):
            kind = IndexTrapKind.BEAR_TRAP_CONFIRMED
            zone = bear
        elif bull is not None:
            kind = IndexTrapKind.BULL_TRAP_CONFIRMED
            zone = bull

        self.current_kind = kind
        self.current_bucket_ts = bar.timestamp
        self.current_zone = zone
        return kind

    def confirms(self, side: str) -> bool:
        """side='CE' needs a bear trap confirmed; side='PE' needs a bull trap confirmed."""
        if side == "CE":
            return self.current_kind == IndexTrapKind.BEAR_TRAP_CONFIRMED
        if side == "PE":
            return self.current_kind == IndexTrapKind.BULL_TRAP_CONFIRMED
        return False

    def confirmation_ts(self, side: str) -> Optional[datetime]:
        """The Index-chart reclaim timestamp that confirmed `side`'s arming,
        or None if that side isn't currently confirmed. Used to anchor the
        premium-chart Gate 2 scan window (IndexGatedPremiumScanner)."""
        if self.confirms(side) and self.current_zone is not None:
            return self.current_zone.lock_ts
        return None

    def reset(self) -> None:
        self._bars_75m.clear()
        self.current_kind = IndexTrapKind.NONE
        self.current_bucket_ts = None
        self.current_zone = None
