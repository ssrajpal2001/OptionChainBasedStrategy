"""
strategies/v4_cascade_book_manager.py — per-binding V4Cascade lifecycle.

Mirrors strategies/straddle_book_manager.py exactly. Maintains ONE
independent V4CascadeBook per (client, binding, NIFTY) deployment.
NIFTY-only (the ATM-200/ATM+200 tracking-strike offsets and monthly-expiry
resolution are calibrated specifically for NIFTY) — any deployment targeting
a different underlying is defensively skipped with a warning log.

On/off control reuses the SAME mechanism sell_straddle already has — the
generic per-deployment Run/Stop toggle (is_running, via the strategy-agnostic
/api/client/deployment/{deploy_id}/run endpoint) already filters
get_running_deployments_by_strategy_sync() to is_running=1 rows. No separate
global flag — that would just duplicate this existing control.
"""
from __future__ import annotations

import logging
from typing import Dict

from strategies.core import StrategyBookManager

logger = logging.getLogger(__name__)

# Local import inside _spawn_book() to avoid a circular import with
# strategies.v4_cascade.__init__. Unit tests monkeypatch this attribute.
V4CascadeBook = None


class V4CascadeBookManager(StrategyBookManager):
    def _wanted(self) -> Dict[tuple, int]:
        wanted: Dict[tuple, int] = {}
        try:
            rows = self._db.get_running_deployments_by_strategy_sync("v4_cascade")
        except Exception:
            return wanted
        for d in rows:
            cid = d.get("client_id", "")
            bid = d.get("binding_id", "")
            und = str(d.get("underlying", "") or d.get("assigned_instrument", "")).upper()
            if not cid or not bid:
                continue
            if und != "NIFTY":
                logger.warning(
                    "V4CascadeBookManager: skipping %s/%s/%s — v4_cascade is NIFTY-only "
                    "(ATM offsets are NIFTY-calibrated).", cid, bid, und,
                )
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
        cls = V4CascadeBook
        if cls is None:
            from strategies.v4_cascade.book import V4CascadeBook as cls
        cid, bid, und = key
        book = cls(self._bus, self._cfg, underlying=und, client_id=cid, binding_id=bid, lot_multiplier=lots)
        book.set_client_db(self._db)
        if self._rebalancer is not None and hasattr(book, "set_rebalancer"):
            book.set_rebalancer(self._rebalancer)
        if self._rebalancer is not None and hasattr(self._rebalancer, "enable_chain"):
            self._rebalancer.enable_chain(und)
        return book

    def _should_respawn(self, book, lots):
        return getattr(book, "_lot_multiplier", 1) != lots

    def _log_spawned(self, key, lots):
        logger.info("V4CascadeBookManager: spawned book %s/%s/%s (lots=%d)", *key, lots)

    def _log_stopped(self, key):
        logger.info("V4CascadeBookManager: stopped book %s/%s/%s", *key)

    def _log_respawned(self, key, lots):
        logger.info("V4CascadeBookManager: re-spawned %s/%s/%s lots->%d", *key, lots)

    def force_ingest(self, client_id: str, binding_id: str, underlying: str) -> bool:
        """Admin 'Force Ingest Zones' action — re-runs deep history ingestion
        on an already-running book. Returns False if no matching book exists
        (endpoint should report 404)."""
        import asyncio
        book = self.find(client_id, binding_id, underlying)
        if book is None or not hasattr(book, "force_ingest"):
            return False
        asyncio.create_task(book.force_ingest())
        return True
