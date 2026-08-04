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

    def _is_flat(self, book) -> bool:
        """FnOPositionalBook stores its positions as a list (self._positions /
        the _open_positions property), not a singular self._position like
        AbstractStrategyBook-based books -- the base class's default
        `getattr(book, "_position", None) is None` is unconditionally True
        for FnO (it has no `_position` attribute at all), so an FnO
        deployment with real open positions was always (incorrectly) treated
        as flat on removal, meaning it always took the plain _stop_book()
        path and never _liquidate_book()/_run_liquidate_and_stop(). This
        override reads the book's own open-position accessor instead.

        Note: FnO intentionally has no liquidate() method (see _spawn_book /
        module docstring) -- FnO Positional is a carry-forward NRML strategy
        (positions are meant to persist across days, unlike an intraday
        strategy's kill-switch flatten). Even with _is_flat now correctly
        returning False for an FnO book with open positions,
        _liquidate_book()'s `if hasattr(book, "liquidate")` check is a no-op
        for FnO and it falls straight through to stopping the book (now
        correctly, via the stop_async/async-stop() detection in
        _resolve_async_stop). That's the desired behavior: removing an FnO
        deployment should NOT force-flatten real broker positions -- they
        stay open and continue to be tracked/managed by FnO's own SL/EOD
        logic; only the manager's book instance is stopped cleanly.
        """
        positions = getattr(book, "_positions", None)
        if positions is None:
            return True
        open_positions = getattr(book, "_open_positions", None)
        if isinstance(open_positions, (list, tuple)):
            return len(open_positions) == 0
        if callable(open_positions):
            return len(open_positions()) == 0
        return len(positions) == 0

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
