"""5-minute (configurable) bar accumulator fed by raw live ticks.

Mirrors the bucket-accumulation pattern already used by
strategies/cag_straddle and the (removed) FVG strategy in this codebase --
built fresh here per this strategy's standalone mandate, no import.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from strategies.bear_trap_oi.models import Bar


def _bucket_start(ts: datetime, bucket_minutes: int) -> datetime:
    floored_minute = (ts.minute // bucket_minutes) * bucket_minutes
    return ts.replace(minute=floored_minute, second=0, microsecond=0)


class BarAccumulator:
    def __init__(self, bucket_minutes: int = 5):
        self._bucket_minutes = bucket_minutes
        self._bucket_ts: Optional[datetime] = None
        self._open: Optional[float] = None
        self._high: Optional[float] = None
        self._low: Optional[float] = None
        self._close: Optional[float] = None

    def on_tick(self, ts: datetime, ltp: float) -> Optional[Bar]:
        bucket_ts = _bucket_start(ts, self._bucket_minutes)
        completed: Optional[Bar] = None

        if self._bucket_ts is None:
            self._bucket_ts = bucket_ts
            self._open = self._high = self._low = self._close = ltp
            return None

        if bucket_ts != self._bucket_ts:
            completed = Bar(ts=self._bucket_ts, open=self._open,
                             high=self._high, low=self._low, close=self._close)
            self._bucket_ts = bucket_ts
            self._open = self._high = self._low = self._close = ltp
            return completed

        self._high = max(self._high, ltp)
        self._low = min(self._low, ltp)
        self._close = ltp
        return None

    def current_partial(self) -> Optional[Bar]:
        if self._bucket_ts is None:
            return None
        return Bar(ts=self._bucket_ts, open=self._open, high=self._high,
                    low=self._low, close=self._close)
