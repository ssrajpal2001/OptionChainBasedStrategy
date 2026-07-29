"""
strategies/d1_trap_option/book_manager.py — lifecycle manager for Trap Scanner books.

Handles two strategy names:
  "d1_trap_index"  — Indices (NIFTY/SENSEX/BANKNIFTY), intraday MIS, configurable HTF/MTF
  "d1_trap_fno"    — FnO stocks (RELIANCE etc.), positional NRML, D1 HTF / 1H MTF

One D1TrapOptionBook per (client, binding, underlying) with is_running=1.
strategy_params JSON in the deployment row configures htf/mtf/ltf/itm per book.
"""
from __future__ import annotations

import json
import logging
from typing import Dict

from strategies.core import StrategyBookManager

logger = logging.getLogger(__name__)

_STRATEGY_NAMES = {"d1_trap_index", "d1_trap_fno", "d1_trap_option"}

_D1TrapOptionBook = None  # lazy-imported


def _parse_params(raw: str, strategy_name: str) -> dict:
    """Parse strategy_params JSON with per-strategy defaults."""
    try:
        params = json.loads(raw or "{}")
    except Exception:
        params = {}

    if strategy_name == "d1_trap_fno":
        defaults = {"htf": "D1", "mtf": "75min", "ltf": "5min", "itm": 1}
    else:
        defaults = {"htf": "75min", "mtf": "15min", "ltf": "5min", "itm": 1}

    for k, v in defaults.items():
        params.setdefault(k, v)
    return params


class D1TrapOptionBookManager(StrategyBookManager):

    def _wanted(self) -> Dict[tuple, dict]:
        """Return {(cid, bid, underlying): book_config_dict} for all running trap deployments."""
        wanted: Dict[tuple, dict] = {}
        for strategy_name in _STRATEGY_NAMES:
            try:
                rows = self._db.get_running_deployments_by_strategy_sync(strategy_name)
            except Exception:
                continue
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
                params = _parse_params(d.get("strategy_params", "{}"), strategy_name)
                product = d.get("product_type") or (
                    "NRML" if strategy_name == "d1_trap_fno" else "MIS"
                )
                cfg = {
                    "lots": lots,
                    "strategy_name": strategy_name,
                    "htf_tf": params["htf"],
                    "mtf_tf": params["mtf"],
                    "itm_offset": int(params.get("itm", 1)),
                    "product_type": product,
                }

                # ALL_FNO sentinel: one deployment record → 30 per-stock books.
                if underlying == "ALL_FNO" and strategy_name == "d1_trap_fno":
                    try:
                        from config.global_config import FNO_STOCK_CONFIG
                        for stock_sym in FNO_STOCK_CONFIG:
                            wanted[(cid, bid, stock_sym.upper())] = cfg.copy()
                    except Exception:
                        logger.warning("TrapBookManager: FNO_STOCK_CONFIG not available")
                    continue

                wanted[(cid, bid, underlying)] = cfg
        return wanted

    def _should_respawn(self, book, value) -> bool:
        """Re-spawn when lot_multiplier changes while the book is flat."""
        return getattr(book, "_lot_multiplier", None) != value.get("lots", 1)

    # The base _reconcile() handles spawn/stop/respawn logic using _wanted(), _spawn_book(),
    # _should_respawn(), and _is_flat(). No need to override it — just supply the right values.

    def _spawn_book(self, key, cfg):
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

        strategy_name = cfg.get("strategy_name", "d1_trap_index")

        book = _D1TrapOptionBook(
            bus=self._bus,
            cfg=self._cfg,
            underlying=underlying,
            client_id=cid,
            binding_id=bid,
            strategy_name=strategy_name,
            lot_multiplier=cfg.get("lots", 1),
            feeder_token=feeder_token,
            htf_tf=cfg.get("htf_tf", "D1"),
            mtf_tf=cfg.get("mtf_tf", "1H"),
            itm_offset=cfg.get("itm_offset", 1),
            product_type=cfg.get("product_type", "MIS"),
        )

        # Register FnO equity subscriptions on Fyers feeder
        if strategy_name == "d1_trap_fno":
            self._register_fno_equity(underlying)

        return book

    def _register_fno_equity(self, underlying: str) -> None:
        """Tell FyersFeeder to emit EQUITY_TICK for this FnO stock symbol."""
        try:
            from config.global_config import FNO_STOCK_CONFIG
            stock = FNO_STOCK_CONFIG.get(underlying.upper())
            if not stock:
                return
            fyers_sym = stock["fyers"]   # e.g. "NSE:RELIANCE-EQ"
            # Access the feeder through the bus's global feeder reference
            gf = getattr(self._bus, "_global_feeder", None)
            if gf is not None and hasattr(gf, "subscribe_fno_equity"):
                gf.subscribe_fno_equity(fyers_sym, underlying)
        except Exception:
            logger.debug("TrapBookManager: FnO equity registration skipped for %s", underlying)
