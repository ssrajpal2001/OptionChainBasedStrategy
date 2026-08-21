"""
strategies/liquidity_trap/book_manager.py — lifecycle manager for
LiquidityTrapStrategy books.

One LiquidityTrapStrategy per (client, binding, underlying) with a running
"liquidity_trap" deployment. Fresh, independent implementation -- reads its
own "liquidity_trap"-tagged deployment rows only, never touches or is
touched by any other strategy's manager, per strategies/liquidity_trap/
__init__.py's standalone design.
"""
from __future__ import annotations

import json
import logging
from typing import Dict

from strategies.core.book_manager import StrategyBookManager
from strategies.liquidity_trap.engine import LiquidityTrapStrategy

logger = logging.getLogger(__name__)

_STRATEGY_NAME = "liquidity_trap"
_DEFAULT_PARAMS = {
    "lots_initial": 2,
    "rr": 2.0,
    "itm_offset_pts": 0.0,
    "hard_risk_rs_per_lot": 2000.0,
}
_FLOAT_KEYS = tuple(_DEFAULT_PARAMS.keys())
_BOOL_DEFAULT_SCALE_IN_ENABLED = True


def _parse_params(raw: str) -> dict:
    try:
        params = json.loads(raw or "{}")
    except Exception:
        params = {}
    for k, v in _DEFAULT_PARAMS.items():
        params.setdefault(k, v)
    params.setdefault("scale_in_enabled", _BOOL_DEFAULT_SCALE_IN_ENABLED)
    return params


class LiquidityTrapBookManager(StrategyBookManager):

    def _wanted(self) -> Dict[tuple, dict]:
        wanted: Dict[tuple, dict] = {}
        rows = self._db.get_running_deployments_by_strategy_sync(_STRATEGY_NAME)
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
            params = _parse_params(d.get("strategy_params", "{}"))
            cfg = {"lots": lots, "product_type": d.get("product_type") or "MIS",
                   "squareoff_time": d.get("squareoff_time") or "15:15",
                   "scale_in_enabled": bool(params["scale_in_enabled"])}
            for k in _FLOAT_KEYS:
                cfg[k] = float(params.get(k, _DEFAULT_PARAMS[k]))
            wanted[(cid, bid, underlying)] = cfg
        return wanted

    def _spawn_book(self, key: tuple, value: dict) -> LiquidityTrapStrategy:
        client_id, binding_id, underlying = key
        self._enable_chain(underlying)
        feeder_token = ""
        try:
            creds = self._db.get_feeder_creds_sync("upstox") or {}
            feeder_token = creds.get("access_token", "")
        except Exception:
            pass
        book = LiquidityTrapStrategy(
            self._bus, self._cfg, underlying, client_id, binding_id,
            lot_multiplier=value["lots"],
            lots_initial=int(value["lots_initial"]), rr=value["rr"],
            itm_offset_pts=value["itm_offset_pts"],
            scale_in_enabled=value["scale_in_enabled"],
            hard_risk_rs_per_lot=value["hard_risk_rs_per_lot"],
            product_type=value["product_type"], squareoff_time=value["squareoff_time"],
            feeder_token=feeder_token,
        )
        logger.info(
            "LiquidityTrapBookManager: spawned %s/%s/%s (lots=%d lots_initial=%d rr=%.1f scale_in=%s).",
            client_id, binding_id, underlying, value["lots"], value["lots_initial"],
            value["rr"], value["scale_in_enabled"],
        )
        return book

    def _should_respawn(self, book: LiquidityTrapStrategy, value: dict) -> bool:
        if book._lot_multiplier != value["lots"]:
            return True
        if book._scale_in_enabled != value["scale_in_enabled"]:
            return True
        return any(getattr(book, f"_{k}") != value[k] for k in _FLOAT_KEYS)

    def _log_spawned(self, key: tuple, value: dict) -> None:
        logger.info("LiquidityTrapBookManager: reconcile spawned %s -> %s", key, value)

    def _log_stopped(self, key: tuple) -> None:
        logger.info("LiquidityTrapBookManager: reconcile stopped %s", key)
