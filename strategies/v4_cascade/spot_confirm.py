"""
strategies/v4_cascade/spot_confirm.py — NIFTY spot-side concurrent confirmation.

Spec: "the NIFTY Spot Index must be printing a V4 Bear Trap close out of its
corresponding 75m demand zone" (CE side) / "a V4 Bull Trap close ... sweeping
structural highs" (PE side). Unlike the tracking-contract premium scan
(zone_state.py), spot confirmation is fixed at 75m — no multiplier ladder.

Pure — fed 75m CandleEvent-shaped bars directly, no bus/broker dependency.
"""
from __future__ import annotations

from collections import deque
from datetime import datetime
from typing import Deque, Optional

from strategies.v4_cascade.dataclasses import SpotTrapKind
from strategies.v4_cascade.rolling_base import find_bear_zone, find_bull_zone

_MAX_75M_BARS = 200


class SpotConfirmTracker:
    """Classifies each closed 75m spot bar's bucket as NONE / BEAR_TRAP_CLOSE /
    BULL_TRAP_CLOSE, based on the same 3-candle sweep+reclaim pattern applied
    to spot's own structure (demand-zone sweep+reclaim = bear trap = bullish
    read; structural-high sweep+reclaim = bull trap = bearish read)."""

    def __init__(self) -> None:
        self._bars_75m: Deque = deque(maxlen=_MAX_75M_BARS)
        self.current_kind: SpotTrapKind = SpotTrapKind.NONE
        self.current_bucket_ts: Optional[datetime] = None

    def on_75m_bar(self, bar) -> SpotTrapKind:
        """Feed one closed 75m spot bar; returns the classification that now
        applies to this bucket (also stored on .current_kind/.current_bucket_ts)."""
        self._bars_75m.append(bar)
        bars = list(self._bars_75m)

        bear = find_bear_zone(bars)
        bull = find_bull_zone(bars)

        kind = SpotTrapKind.NONE
        # If both somehow resolve (shouldn't for the same 3 bars, but a longer
        # history could hold both an older bear and an older bull pattern),
        # prefer whichever reclaimed most recently.
        if bear is not None and (bull is None or bear.lock_ts >= bull.lock_ts):
            kind = SpotTrapKind.BEAR_TRAP_CLOSE
        elif bull is not None:
            kind = SpotTrapKind.BULL_TRAP_CLOSE

        self.current_kind = kind
        self.current_bucket_ts = bar.timestamp
        return kind

    def confirms(self, side: str) -> bool:
        """side='CE' needs a bear trap close; side='PE' needs a bull trap close."""
        if side == "CE":
            return self.current_kind == SpotTrapKind.BEAR_TRAP_CLOSE
        if side == "PE":
            return self.current_kind == SpotTrapKind.BULL_TRAP_CLOSE
        return False

    def reset(self) -> None:
        self._bars_75m.clear()
        self.current_kind = SpotTrapKind.NONE
        self.current_bucket_ts = None
