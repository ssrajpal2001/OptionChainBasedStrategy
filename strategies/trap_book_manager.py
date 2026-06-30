"""
strategies/trap_book_manager.py — Per-binding TrapScanner lifecycle manager.

One TrapScannerEngine per (client_id, binding_id, underlying).
Mirrors the StraddleBookManager pattern: reconciles against DB every N seconds,
auto-spawns on deploy (is_running=1), auto-stops on undeploy or toggle-off.

Lot multiplier must be a multiple of 2 (50% T1 exit requires even lots).
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Dict, Tuple

from strategies.core import StrategyBookManager
from strategies.trap_scanner_engine import TrapScannerEngine

logger = logging.getLogger(__name__)


class TrapBookManager(StrategyBookManager):
    def __init__(self, bus, cfg, client_db, monitored_indices,
                 reconcile_sec: float = 5.0) -> None:
        super().__init__(bus, cfg, client_db, monitored_indices, reconcile_sec)
        self._mcx_feeder = None    # dedicated Upstox2 feeder for MCX indices
        self._delta_feeder = None  # DeltaFeeder for BTC/ETH (client's Delta exchange)

    def set_rebalancer(self, rebalancer) -> None:
        super().set_rebalancer(rebalancer)

    def set_mcx_feeder(self, feeder) -> None:
        self._mcx_feeder = feeder
        for eng in self.books:
            if eng._cfg.exchange.is_mcx(eng._und):
                eng.set_mcx_feeder(feeder)

    def set_delta_feeder(self, feeder) -> None:
        """Wire DeltaFeeder to all existing and future BTC/ETH books."""
        self._delta_feeder = feeder
        for eng in self.books:
            if eng._cfg.exchange.is_crypto(eng._und):
                eng.set_mcx_feeder(feeder)  # reuses same slot; crypto and MCX never coexist

    def _ts_admin_cfg(self) -> dict:
        """Fetch current trap_scanner admin config from system settings DB."""
        try:
            raw = self._db.get_setting_sync("trap_scanner", "")
            if raw:
                return json.loads(raw)
        except Exception:
            pass
        return {}

    def _wanted(self) -> Dict[tuple, Tuple[int, str]]:
        """All is_running=1 trap_scanner deployments → {key: (lot_multiplier, expiry_mode)}."""
        wanted: Dict[tuple, Tuple[int, str]] = {}
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
            # Crypto (BTC/ETH) is always deployable — has its own Delta feed, not dependent on
            # monitored_indices / Upstox. Only filter non-crypto by the index whitelist.
            if self._indices and und not in self._indices and not self._cfg.exchange.is_crypto(und):
                continue
            try:
                lots = int(round(float(d.get("lot_multiplier", 2) or 2)))
                if lots % 2 != 0:
                    lots = max(2, lots + 1)   # enforce multiple of 2
            except Exception:
                lots = 2
            expiry_mode = str(d.get("expiry_mode", "current") or "current").strip()
            wanted[(cid, bid, und)] = (lots, expiry_mode)
        return wanted

    def _spawn_book(self, key, value):
        cid, bid, und = key
        lots, expiry_mode = value
        ts_cfg = self._ts_admin_cfg()
        eng = TrapScannerEngine(
            bus=self._bus,
            cfg=self._cfg,
            underlying=und,
            lot_multiplier=lots,
            client_id=cid,
            binding_id=bid,
            ts_admin_cfg=ts_cfg,
            client_db=self._db,
            expiry_mode=expiry_mode,
        )
        if self._rebalancer is not None:
            eng.set_rebalancer(self._rebalancer)
        if self._mcx_feeder is not None and self._cfg.exchange.is_mcx(und):
            eng.set_mcx_feeder(self._mcx_feeder)
        if self._delta_feeder is not None and self._cfg.exchange.is_crypto(und):
            eng.set_mcx_feeder(self._delta_feeder)
        return eng

    def _stop_book(self, book) -> None:
        # TrapScannerEngine stop is async; schedule it on the running loop.
        try:
            asyncio.get_event_loop().create_task(book.stop_async())
        except Exception:
            pass

    def _should_respawn(self, book, value):
        lots, expiry_mode = value
        lots_changed = getattr(book, "_lot_mul", 2) != lots
        expiry_changed = getattr(book, "_expiry_mode", "current") != expiry_mode
        return lots_changed or expiry_changed

    def _log_spawned(self, key, value):
        lots, expiry_mode = value
        logger.info("TrapBookManager: spawned %s/%s/%s (lots=%d expiry=%s)",
                    *key, lots, expiry_mode)

    def _log_stopped(self, key):
        logger.info("TrapBookManager: stopped %s/%s/%s", *key)

    def _log_respawned(self, key, value):
        lots, expiry_mode = value
        logger.info("TrapBookManager: re-spawned %s/%s/%s lots→%d expiry→%s",
                    *key, lots, expiry_mode)

    async def set_expiry_mode(self, client_id: str, binding_id: str, underlying: str,
                               expiry_mode: str) -> None:
        """Hot-apply expiry mode — calls engine.set_expiry_mode() which resets init state."""
        key = (client_id, binding_id, underlying.upper())
        eng = self._books.get(key)
        if eng is not None and hasattr(eng, "set_expiry_mode"):
            eng.set_expiry_mode(expiry_mode)
            logger.info("TrapBookManager: expiry_mode applied for %s/%s/%s → %s",
                        client_id, binding_id, underlying, expiry_mode)

    def telemetry_all(self) -> list:
        return [e.telemetry_snapshot() for e in self.books]
