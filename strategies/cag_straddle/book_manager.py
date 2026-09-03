"""
strategies/cag_straddle/book_manager.py — lifecycle manager for
CagStraddleStrategy books.

One CagStraddleStrategy per (client, binding, underlying) with a running
"cag_straddle" deployment. Fresh, independent implementation -- reads its
own "cag_straddle"-tagged deployment rows only, never touches or is touched
by any other strategy's manager, per strategies/cag_straddle/__init__.py's
standalone design.
"""
from __future__ import annotations

import json
import logging
from typing import Dict

from strategies.core.book_manager import StrategyBookManager
from strategies.cag_straddle.engine import CagStraddleStrategy

logger = logging.getLogger(__name__)

_STRATEGY_NAME = "cag_straddle"
_DEFAULT_PARAMS = {
    "target_premium_rs": 100.0,
    "strike_search_steps": 6,
    "hard_risk_rs_per_lot": 2000.0,
}
_FLOAT_KEYS = tuple(_DEFAULT_PARAMS.keys())


def _parse_params(raw: str) -> dict:
    try:
        params = json.loads(raw or "{}")
    except Exception:
        params = {}
    for k, v in _DEFAULT_PARAMS.items():
        params.setdefault(k, v)
    params.setdefault("entry_start", "15:00")
    params.setdefault("force_exit_time", "15:35")
    return params


class CagStraddleBookManager(StrategyBookManager):

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
            cfg = {
                "lots": lots, "product_type": d.get("product_type") or "MIS",
                "entry_start": params["entry_start"], "force_exit_time": params["force_exit_time"],
            }
            for k in _FLOAT_KEYS:
                cfg[k] = float(params.get(k, _DEFAULT_PARAMS[k]))
            wanted[(cid, bid, underlying)] = cfg
        return wanted

    def _spawn_book(self, key: tuple, value: dict) -> CagStraddleStrategy:
        client_id, binding_id, underlying = key
        self._enable_chain(underlying)
        book = CagStraddleStrategy(
            self._bus, self._cfg, underlying, client_id, binding_id,
            lot_multiplier=value["lots"],
            target_premium_rs=value["target_premium_rs"],
            strike_search_steps=int(value["strike_search_steps"]),
            hard_risk_rs_per_lot=value["hard_risk_rs_per_lot"],
            product_type=value["product_type"],
            entry_start=value["entry_start"], force_exit_time=value["force_exit_time"],
        )
        logger.info(
            "CagStraddleBookManager: spawned %s/%s/%s (lots=%d target_premium_rs=%.0f "
            "strike_search_steps=%d entry_start=%s force_exit_time=%s).",
            client_id, binding_id, underlying, value["lots"], value["target_premium_rs"],
            int(value["strike_search_steps"]), value["entry_start"], value["force_exit_time"],
        )
        return book

    def _should_respawn(self, book: CagStraddleStrategy, value: dict) -> bool:
        if book._lot_multiplier != value["lots"]:
            return True
        return any(getattr(book, f"_{k}") != value[k] for k in _FLOAT_KEYS)

    def _log_spawned(self, key: tuple, value: dict) -> None:
        logger.info("CagStraddleBookManager: reconcile spawned %s -> %s", key, value)

    def _log_stopped(self, key: tuple) -> None:
        logger.info("CagStraddleBookManager: reconcile stopped %s", key)
