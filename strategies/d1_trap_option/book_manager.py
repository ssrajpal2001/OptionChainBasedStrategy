"""
strategies/d1_trap_option/book_manager.py — lifecycle manager for D1 Trap + Option books.

One D1TrapOptionBook per (client, binding, underlying) deployment with
strategy_name='d1_trap_option' and is_running=1 in strategy_deployments.
"""
from __future__ import annotations

import logging
from typing import Dict

from strategies.core import StrategyBookManager

logger = logging.getLogger(__name__)

_STRATEGY_NAME = "d1_trap_option"

_D1TrapOptionBook = None  # lazy-imported


class D1TrapOptionBookManager(StrategyBookManager):

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
        global _D1TrapOptionBook
        if _D1TrapOptionBook is None:
            from strategies.d1_trap_option.book import D1TrapOptionBook as _cls
            _D1TrapOptionBook = _cls

        cid, bid, underlying = key

        feeder_token = ""
        try:
            creds = self._db.get_feeder_creds_sync("upstox") or {}
            feeder_token = creds.get("access_token", "")
        except Exception:
            pass

        book = _D1TrapOptionBook(
            bus=self._bus,
            cfg=self._cfg,
            underlying=underlying,
            client_id=cid,
            binding_id=bid,
            lot_multiplier=lots,
            feeder_token=feeder_token,
        )
        logger.info(
            "D1TrapOptionBookManager: spawned book %s/%s/%s (lots=%d)",
            cid, bid, underlying, lots,
        )
        return book
