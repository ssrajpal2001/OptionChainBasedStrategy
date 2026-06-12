"""
data_layer/delta_rollover.py — Delta daily-expiry hot-swap worker (Stage 3).

Delta crypto options expire DAILY at 17:30 IST (12:00 UTC). At each boundary the front-day contract
dies and the next day's mints. This worker sleeps until the next boundary (no busy loop — it sleeps on
`seconds_to_next_rollover`), then calls back to recompute the active strikes and re-subscribe the
feeder, so the running app swaps contracts WITHOUT a restart and keeps indicator state.

Pure-ish: takes async callables so it's testable without a live feeder.
    on_rollover(old_active_expiry: date, new_active_expiry: date) -> awaitable
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Awaitable, Callable, Optional

from data_layer.universal_option_mapper import UniversalOptionMapper as _M

logger = logging.getLogger(__name__)


class DeltaRolloverWorker:
    def __init__(self, on_rollover: Callable[[date, date], Awaitable[None]],
                 lead_seconds: float = 2.0) -> None:
        self._on_rollover = on_rollover
        self._lead = lead_seconds          # fire just AFTER the boundary so the new contract exists
        self._running = False
        self._active = _M.active_daily_expiry()

    async def run(self) -> None:
        self._running = True
        logger.info("DeltaRolloverWorker: started (active expiry=%s).", self._active)
        while self._running:
            wait = max(1.0, _M.seconds_to_next_rollover() + self._lead)
            try:
                await asyncio.sleep(wait)
            except asyncio.CancelledError:
                break
            if not self._running:
                break
            new_active = _M.active_daily_expiry()
            if new_active != self._active:
                old, self._active = self._active, new_active
                logger.info("DeltaRolloverWorker: 17:30 IST rollover %s → %s — hot-swapping contracts.",
                            old, new_active)
                try:
                    await self._on_rollover(old, new_active)
                except Exception as exc:
                    logger.error("DeltaRolloverWorker: on_rollover failed: %s", exc)

    def stop(self) -> None:
        self._running = False
