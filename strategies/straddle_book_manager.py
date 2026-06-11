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

import asyncio
import logging
from typing import Dict, List, Optional, Tuple

from strategies.sell_straddle import SellStraddleStrategy

logger = logging.getLogger(__name__)

Key = Tuple[str, str, str]   # (client_id, binding_id, underlying)


class StraddleBookManager:
    def __init__(self, bus, cfg, client_db, monitored_indices, reconcile_sec: float = 5.0) -> None:
        self._bus = bus
        self._cfg = cfg
        self._db = client_db
        self._indices = {str(i).upper() for i in (monitored_indices or [])}
        self._reconcile_sec = reconcile_sec
        self._books: Dict[Key, SellStraddleStrategy] = {}
        self._running = False

    # ── Accessors (used by dashboard + bridge) ────────────────────────────────
    @property
    def books(self) -> List[SellStraddleStrategy]:
        return list(self._books.values())

    def find(self, client_id: str, binding_id: str, underlying: str) -> Optional[SellStraddleStrategy]:
        return self._books.get((client_id, binding_id, str(underlying).upper()))

    # ── Lifecycle ─────────────────────────────────────────────────────────────
    async def run(self) -> None:
        self._running = True
        logger.info("StraddleBookManager: started (indices=%s).", sorted(self._indices))
        while self._running:
            try:
                self._reconcile()
            except Exception as exc:
                logger.warning("StraddleBookManager.reconcile error: %s", exc)
            try:
                await asyncio.sleep(self._reconcile_sec)
            except asyncio.CancelledError:
                break

    def _wanted_keys(self) -> set:
        wanted: set = set()
        try:
            clients = self._db.get_all_clients_sync()
        except Exception:
            clients = []
        for c in clients:
            cid = c.get("client_id", "")
            if not cid:
                continue
            try:
                deps = self._db.get_deployments_sync(cid)
            except Exception:
                deps = []
            for d in deps:
                if str(d.get("strategy_name", "")).lower() != "sell_straddle":
                    continue
                und = str(d.get("underlying", "") or d.get("assigned_instrument", "")).upper()
                if self._indices and und not in self._indices:
                    continue           # only spawn books for indices the feeder actually subscribes
                bid = d.get("binding_id", "")
                if bid:
                    wanted.add((cid, bid, und))
        return wanted

    def _reconcile(self) -> None:
        wanted = self._wanted_keys()
        # Spawn books for new deployments (auto-start-on-deploy).
        for key in wanted - set(self._books):
            cid, bid, und = key
            try:
                book = SellStraddleStrategy(self._bus, self._cfg, underlying=und,
                                            client_id=cid, binding_id=bid)
                book.set_client_db(self._db)
                book.start()
                self._books[key] = book
                logger.info("StraddleBookManager: spawned book %s/%s/%s", cid, bid, und)
            except Exception as exc:
                logger.warning("StraddleBookManager: spawn %s failed: %s", key, exc)
        # Stop books whose deployment was removed.
        for key in set(self._books) - wanted:
            book = self._books.pop(key)
            try:
                book.stop()
            except Exception:
                pass
            logger.info("StraddleBookManager: stopped book %s/%s/%s", *key)

    def stop(self) -> None:
        self._running = False
        for book in self._books.values():
            try:
                book.stop()
            except Exception:
                pass
