"""
strategies/fno_positional/book_manager.py — FnO Positional lifecycle manager.

Maintains one FnOPositionalBook per (client, binding) deployment that has
strategy_name='fno_positional' and is_running=1 in strategy_deployments.

The "underlying" stored in strategy_deployments is "FNO_STOCKS" (a sentinel
meaning "scan all 30 FnO stocks") — the book manages which stocks to trade
internally based on zone-detection signals.
"""
from __future__ import annotations

import logging
import os
from typing import Dict, Optional

from strategies.core import StrategyBookManager

logger = logging.getLogger(__name__)

_STRATEGY_NAME = "fno_positional"
_UNDERLYING    = "FNO_STOCKS"

FnOPositionalBook = None   # local-imported in _spawn_book to avoid circulars


class FnOPositionalBookManager(StrategyBookManager):

    def _wanted(self) -> Dict[tuple, int]:
        wanted: Dict[tuple, int] = {}
        rows = self._db.get_running_deployments_by_strategy_sync(_STRATEGY_NAME)
        for d in rows:
            cid = d.get("client_id", "")
            bid = d.get("binding_id", "")
            if not cid or not bid:
                continue
            try:
                lots = max(1, int(round(float(d.get("lot_multiplier", 1) or 1))))
            except Exception:
                lots = 1
            # Use "FNO_STOCKS" as the underlying key (sentinel for all stocks)
            wanted[(cid, bid, _UNDERLYING)] = lots
        return wanted

    def _spawn_book(self, key, lots):
        global FnOPositionalBook
        if FnOPositionalBook is None:
            from strategies.fno_positional.book import FnOPositionalBook as _cls
            FnOPositionalBook = _cls

        cid, bid, _ = key

        # Fetch Upstox token from DB (needed for scan + LTP polling)
        upstox_token = ""
        try:
            creds = self._db.get_feeder_creds_sync("upstox")
            upstox_token = (creds or {}).get("access_token", "")
        except Exception as exc:
            logger.warning("FnOPositionalBookManager: could not load Upstox token: %s", exc)

        # Resolve trading mode from the deployment row
        mode = "paper"
        try:
            for d in (self._db.get_deployments_sync(cid) or []):
                if (d.get("binding_id") == bid
                        and d.get("strategy_name") == _STRATEGY_NAME):
                    raw = str(d.get("trading_mode") or "paper").lower()
                    mode = "live" if raw == "live" else "paper"
                    break
        except Exception:
            pass

        book = FnOPositionalBook(
            bus=self._bus,
            upstox_token=upstox_token,
            client_id=cid,
            binding_id=bid,
            mode=mode,
            max_slots=2,
        )
        logger.info("FnOPositionalBookManager: spawned book %s/%s mode=%s", cid, bid, mode)
        return book
