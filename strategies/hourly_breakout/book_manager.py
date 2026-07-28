"""
strategies/hourly_breakout/book_manager.py — lifecycle manager for hourly breakout books.

Maintains one ``HourlyBreakoutBook`` per (client, binding, underlying) deployment
that has strategy_name='hourly_breakout' and is_running=1 in strategy_deployments.
"""
from __future__ import annotations

import logging
from typing import Dict

from strategies.core import StrategyBookManager

logger = logging.getLogger(__name__)

_STRATEGY_NAME = "hourly_breakout"

HourlyBreakoutBook = None  # lazy-imported to avoid circular imports


class HourlyBreakoutBookManager(StrategyBookManager):

    def _wanted(self) -> Dict[tuple, int]:
        wanted: Dict[tuple, int] = {}
        try:
            rows = self._db.get_running_deployments_by_strategy_sync(_STRATEGY_NAME)
        except Exception:
            return wanted
        for d in rows or []:
            cid = d.get("client_id", "")
            bid = d.get("binding_id", "")
            underlying = str(d.get("underlying") or "").upper()
            if not cid or not bid or not underlying:
                continue
            try:
                lots = max(1, int(round(float(d.get("lot_multiplier", 1) or 1))))
            except Exception:
                lots = 1
            wanted[(cid, bid, underlying)] = lots
        return wanted

    def _spawn_book(self, key, lots):
        global HourlyBreakoutBook
        if HourlyBreakoutBook is None:
            from strategies.hourly_breakout.book import HourlyBreakoutBook as _cls
            HourlyBreakoutBook = _cls

        cid, bid, underlying = key
        book = HourlyBreakoutBook(
            bus=self._bus,
            cfg=self._cfg,
            underlying=underlying,
            client_id=cid,
            binding_id=bid,
            lot_multiplier=lots,
        )
        logger.info("HourlyBreakoutBookManager: spawned book %s/%s/%s (lots=%d)", cid, bid, underlying, lots)
        return book
