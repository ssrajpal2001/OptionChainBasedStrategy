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

    def _wanted(self) -> Dict[Key, int]:
        """Map of (client,binding,underlying) → lot_multiplier for every sell_straddle
        deployment that is RUNNING (is_running=1). A deployed-but-stopped strategy is NOT
        wanted — its book only spawns when the Run toggle is ON, so a re-selected/already-
        ticked deployment with is_running=0 never silently trades, and toggling OFF stops it.
        """
        wanted: Dict[Key, int] = {}
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
                if int(d.get("is_running", 0) or 0) != 1:
                    continue           # deployed but Run toggle OFF → do not spawn/trade
                und = str(d.get("underlying", "") or d.get("assigned_instrument", "")).upper()
                if self._indices and und not in self._indices:
                    continue           # only spawn books for indices the feeder actually subscribes
                bid = d.get("binding_id", "")
                if bid:
                    try:
                        lots = max(1, int(round(float(d.get("lot_multiplier", 1) or 1))))
                    except Exception:
                        lots = 1
                    wanted[(cid, bid, und)] = lots
        return wanted

    def _reconcile(self) -> None:
        wanted = self._wanted()
        # Spawn books for newly-RUNNING deployments (auto-start on Run-toggle ON).
        for key in set(wanted) - set(self._books):
            cid, bid, und = key
            try:
                book = SellStraddleStrategy(self._bus, self._cfg, underlying=und,
                                            lot_multiplier=wanted[key],
                                            client_id=cid, binding_id=bid)
                book.set_client_db(self._db)
                book.start()
                self._books[key] = book
                logger.info("StraddleBookManager: spawned book %s/%s/%s (lots=%d)",
                            cid, bid, und, wanted[key])
            except Exception as exc:
                logger.warning("StraddleBookManager: spawn %s failed: %s", key, exc)
        # Stop books whose deployment was removed OR toggled OFF (is_running=0).
        for key in set(self._books) - set(wanted):
            book = self._books.pop(key)
            try:
                book.stop()
            except Exception:
                pass
            logger.info("StraddleBookManager: stopped book %s/%s/%s", *key)
        # Re-spawn a running book if its lot_multiplier changed in the deployment (so a
        # client-side LOT MULTIPLIER edit takes effect — drives both qty and scalable-TSL scaling).
        for key, lots in wanted.items():
            book = self._books.get(key)
            if book is not None and getattr(book, "_lot_multiplier", 1) != lots and book._position is None:
                try:
                    book.stop()
                    nb = SellStraddleStrategy(self._bus, self._cfg, underlying=key[2],
                                              lot_multiplier=lots,
                                              client_id=key[0], binding_id=key[1])
                    nb.set_client_db(self._db)
                    nb.start()
                    self._books[key] = nb
                    logger.info("StraddleBookManager: re-spawned %s/%s/%s lots→%d", *key, lots)
                except Exception as exc:
                    logger.warning("StraddleBookManager: re-spawn %s failed: %s", key, exc)

    def stop(self) -> None:
        self._running = False
        for book in self._books.values():
            try:
                book.stop()
            except Exception:
                pass
