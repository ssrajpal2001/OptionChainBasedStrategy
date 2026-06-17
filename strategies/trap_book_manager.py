"""
strategies/trap_book_manager.py — Per-binding TrapScanner lifecycle manager.

One TrapScannerEngine per (client_id, binding_id, underlying).
Mirrors the StraddleBookManager pattern: reconciles against DB every N seconds,
auto-spawns on deploy (is_running=1), auto-stops on undeploy or toggle-off.

Lot multiplier must be a multiple of 2 (50% T1 exit requires even lots).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional, Tuple

from strategies.trap_scanner_engine import TrapScannerEngine

logger = logging.getLogger(__name__)

Key = Tuple[str, str, str]   # (client_id, binding_id, underlying)


class TrapBookManager:
    def __init__(self, bus, cfg, client_db, monitored_indices,
                 reconcile_sec: float = 5.0) -> None:
        self._bus = bus
        self._cfg = cfg
        self._db = client_db
        self._indices = {str(i).upper() for i in (monitored_indices or [])}
        self._reconcile_sec = reconcile_sec
        self._books: Dict[Key, TrapScannerEngine] = {}
        self._rebalancer = None
        self._running = False

    def set_rebalancer(self, rebalancer) -> None:
        self._rebalancer = rebalancer
        # Apply to any already-running books
        for eng in self._books.values():
            eng.set_rebalancer(rebalancer)

    @property
    def books(self) -> List[TrapScannerEngine]:
        return list(self._books.values())

    def find(self, client_id: str, binding_id: str, underlying: str
             ) -> Optional[TrapScannerEngine]:
        return self._books.get((client_id, binding_id, str(underlying).upper()))

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        logger.info("TrapBookManager: started (indices=%s).", sorted(self._indices))
        while self._running:
            try:
                self._reconcile()
            except Exception as exc:
                logger.warning("TrapBookManager.reconcile error: %s", exc)
            try:
                await asyncio.sleep(self._reconcile_sec)
            except asyncio.CancelledError:
                break

    def _wanted(self) -> Dict[Key, int]:
        """All is_running=1 trap_scanner deployments → {key: lot_multiplier}."""
        wanted: Dict[Key, int] = {}
        try:
            rows = self._db.get_running_trap_deployments_sync()
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
                lots = int(round(float(d.get("lot_multiplier", 2) or 2)))
                if lots % 2 != 0:
                    lots = max(2, lots + 1)   # enforce multiple of 2
            except Exception:
                lots = 2
            wanted[(cid, bid, und)] = lots
        return wanted

    def _ts_admin_cfg(self) -> dict:
        """Fetch current trap_scanner admin config from system settings DB."""
        try:
            import json
            raw = self._db.get_setting_sync("trap_scanner", "")
            if raw:
                return json.loads(raw)
        except Exception:
            pass
        return {}

    def _reconcile(self) -> None:
        wanted = self._wanted()
        ts_cfg = self._ts_admin_cfg()

        # Spawn new books
        for key in set(wanted) - set(self._books):
            cid, bid, und = key
            try:
                eng = TrapScannerEngine(
                    bus=self._bus,
                    cfg=self._cfg,
                    underlying=und,
                    lot_multiplier=wanted[key],
                    client_id=cid,
                    binding_id=bid,
                    ts_admin_cfg=ts_cfg,
                    client_db=self._db,
                )
                if self._rebalancer is not None:
                    eng.set_rebalancer(self._rebalancer)
                eng.start()
                self._books[key] = eng
                logger.info("TrapBookManager: spawned %s/%s/%s (lots=%d)",
                            cid, bid, und, wanted[key])
            except Exception as exc:
                logger.warning("TrapBookManager: spawn %s failed: %s", key, exc, exc_info=True)

        # Stop removed books
        for key in set(self._books) - set(wanted):
            eng = self._books.pop(key)
            loop = asyncio.get_event_loop()
            loop.create_task(eng.stop_async())
            logger.info("TrapBookManager: stopped %s/%s/%s", *key)

        # Re-spawn on lot_multiplier change (only when flat)
        for key, lots in wanted.items():
            eng = self._books.get(key)
            if eng and getattr(eng, "_lot_mul", 2) != lots and eng._position is None:
                try:
                    asyncio.get_event_loop().create_task(eng.stop_async())
                    nb = TrapScannerEngine(
                        bus=self._bus, cfg=self._cfg, underlying=key[2],
                        lot_multiplier=lots, client_id=key[0], binding_id=key[1],
                        ts_admin_cfg=ts_cfg, client_db=self._db,
                    )
                    nb.start()
                    self._books[key] = nb
                    logger.info("TrapBookManager: re-spawned %s/%s/%s lots→%d", *key, lots)
                except Exception as exc:
                    logger.warning("TrapBookManager: re-spawn %s failed: %s", key, exc)

    async def stop_async(self) -> None:
        self._running = False
        await asyncio.gather(
            *[e.stop_async() for e in self._books.values()],
            return_exceptions=True,
        )
        self._books.clear()

    def telemetry_all(self) -> list:
        return [e.telemetry_snapshot() for e in self._books.values()]
