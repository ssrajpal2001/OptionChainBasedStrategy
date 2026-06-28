"""
strategies/straddle_book_manager.py — per-binding SellStraddle lifecycle.

Maintains ONE independent SellStraddleStrategy "book" per (client, binding, underlying) that has a
sell_straddle deployment. Each book trades fully independently — its own beginning entry (anchored
to when THAT terminal turns ON), own strikes, rolls, exits, position and P&L — sharing only the
admin-configured generic rules and the per-index market feed (the books read the same EventBus
ticks; each keeps its own pool engine).

Reconciles against the DB on an interval so a deployment added in the UI auto-spawns a book
(auto-start-on-deploy) and a removed deployment stops its book. No order mirroring: each book stamps
its own client/binding so the bridge routes only to that broker.
"""
from __future__ import annotations

import logging
from typing import Dict

from strategies.core import StrategyBookManager
from strategies.sell_straddle import SellStraddleStrategy

logger = logging.getLogger(__name__)


class StraddleBookManager(StrategyBookManager):
    def __init__(self, bus, cfg, client_db, monitored_indices, reconcile_sec: float = 5.0) -> None:
        super().__init__(bus, cfg, client_db, monitored_indices, reconcile_sec)

    def _wanted(self) -> Dict[tuple, int]:
        """Map of (client,binding,underlying) → lot_multiplier for every sell_straddle
        deployment that is RUNNING (is_running=1). Single JOIN query — O(1) regardless
        of client count (replaces N+1 per-client loop).
        """
        wanted: Dict[tuple, int] = {}
        try:
            rows = self._db.get_running_straddle_deployments_sync()
        except Exception:
            return wanted
        for d in rows:
            cid = d.get("client_id", "")
            bid = d.get("binding_id", "")
            und = str(d.get("underlying", "") or d.get("assigned_instrument", "")).upper()
            if not cid or not bid:
                continue
            if self._indices and und not in self._indices:
                continue
            try:
                lots = max(1, int(round(float(d.get("lot_multiplier", 1) or 1))))
            except Exception:
                lots = 1
            wanted[(cid, bid, und)] = lots
        return wanted

    def _spawn_book(self, key, lots):
        cid, bid, und = key
        book = SellStraddleStrategy(
            self._bus, self._cfg, underlying=und,
            lot_multiplier=lots, client_id=cid, binding_id=bid,
        )
        book.set_client_db(self._db)
        if self._rebalancer is not None and hasattr(self._rebalancer, "enable_chain"):
            self._rebalancer.enable_chain(und)
        return book

    def _should_respawn(self, book, lots):
        return getattr(book, "_lot_multiplier", 1) != lots

    def _log_spawned(self, key, lots):
        logger.info("StraddleBookManager: spawned book %s/%s/%s (lots=%d)", *key, lots)

    def _log_stopped(self, key):
        logger.info("StraddleBookManager: stopped book %s/%s/%s", *key)

    def _log_respawned(self, key, lots):
        logger.info("StraddleBookManager: re-spawned %s/%s/%s lots→%d", *key, lots)
