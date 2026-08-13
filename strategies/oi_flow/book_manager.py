"""
strategies/oi_flow/book_manager.py — lifecycle manager for OIFlowStrategy
books.

One OIFlowStrategy per (client, binding, underlying) with a running
"oi_flow" deployment. strategy_params JSON configures window_sec/
max_opposing_roc_pct/min_supporting_roc_pct/min_pcr_bias/max_pcr_bias/
proximity_pct/hard_risk_rs_per_lot per book.

Mirrors the established StrategyBookManager subclass pattern (same shape
as strategies/fvg/book_manager.py, strategies/d1_trap_option/book_manager.py)
but is a fresh, independent implementation -- reads its own
"oi_flow"-tagged deployment rows only, never touches or is touched by any
other strategy's manager, per strategies/oi_flow/__init__.py's standalone
design.
"""
from __future__ import annotations

import json
import logging
from typing import Dict

from strategies.core.book_manager import StrategyBookManager
from strategies.oi_flow.engine import OIFlowStrategy

logger = logging.getLogger(__name__)

_STRATEGY_NAME = "oi_flow"
_DEFAULT_PARAMS = {
    "window_sec": 180,
    "max_opposing_roc_pct": -0.01,
    "min_supporting_roc_pct": 0.02,
    "min_pcr_bias": 1.2,
    "max_pcr_bias": 0.7,
    "proximity_pct": 0.005,
    "hard_risk_rs_per_lot": 2000.0,
    # Step-locked trailing profit-lock (2026-08-13) -- see engine.py's own
    # _DEFAULT_TRAIL_TRIGGER_PCT etc. docstring: same mechanic as FVG's
    # validated TSL, these specific numbers are FVG's tuned baseline
    # borrowed as a starting point, not independently validated for
    # OI-Flow (no backtest possible for this strategy at all).
    "trail_trigger_pct": 0.15,
    "first_lock_pct": 0.08,
    "step_pct": 0.10,
    "step_lock_pct": 0.05,
}
_FLOAT_KEYS = tuple(_DEFAULT_PARAMS.keys())


def _parse_params(raw: str) -> dict:
    try:
        params = json.loads(raw or "{}")
    except Exception:
        params = {}
    for k, v in _DEFAULT_PARAMS.items():
        params.setdefault(k, v)
    return params


class OIFlowBookManager(StrategyBookManager):

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
                   "squareoff_time": d.get("squareoff_time") or "15:15"}
            for k in _FLOAT_KEYS:
                cfg[k] = float(params.get(k, _DEFAULT_PARAMS[k]))
            wanted[(cid, bid, underlying)] = cfg
        return wanted

    def _spawn_book(self, key: tuple, value: dict) -> OIFlowStrategy:
        client_id, binding_id, underlying = key
        self._enable_chain(underlying)
        book = OIFlowStrategy(
            self._bus, self._cfg, underlying, client_id, binding_id,
            lot_multiplier=value["lots"],
            window_sec=int(value["window_sec"]),
            max_opposing_roc_pct=value["max_opposing_roc_pct"],
            min_supporting_roc_pct=value["min_supporting_roc_pct"],
            min_pcr_bias=value["min_pcr_bias"], max_pcr_bias=value["max_pcr_bias"],
            proximity_pct=value["proximity_pct"],
            hard_risk_rs_per_lot=value["hard_risk_rs_per_lot"],
            trail_trigger_pct=value["trail_trigger_pct"], first_lock_pct=value["first_lock_pct"],
            step_pct=value["step_pct"], step_lock_pct=value["step_lock_pct"],
            product_type=value["product_type"], squareoff_time=value["squareoff_time"],
        )
        logger.info(
            "OIFlowBookManager: spawned %s/%s/%s (lots=%d window=%ds opposing_roc<=%.3f "
            "supporting_roc>=%.3f pcr band=[%.2f,%.2f] proximity=%.3f%%).",
            client_id, binding_id, underlying, value["lots"], value["window_sec"],
            value["max_opposing_roc_pct"], value["min_supporting_roc_pct"],
            value["max_pcr_bias"], value["min_pcr_bias"], value["proximity_pct"] * 100,
        )
        return book

    def _should_respawn(self, book: OIFlowStrategy, value: dict) -> bool:
        if book._lot_multiplier != value["lots"]:
            return True
        return (
            book._window_sec != int(value["window_sec"])
            or book._max_opposing_roc_pct != value["max_opposing_roc_pct"]
            or book._min_supporting_roc_pct != value["min_supporting_roc_pct"]
            or book._min_pcr_bias != value["min_pcr_bias"]
            or book._max_pcr_bias != value["max_pcr_bias"]
            or book._proximity_pct != value["proximity_pct"]
            or book._hard_risk_rs_per_lot != value["hard_risk_rs_per_lot"]
            or book._trail_trigger_pct != value["trail_trigger_pct"]
            or book._first_lock_pct != value["first_lock_pct"]
            or book._step_pct != value["step_pct"]
            or book._step_lock_pct != value["step_lock_pct"]
        )

    def _log_spawned(self, key: tuple, value: dict) -> None:
        logger.info("OIFlowBookManager: reconcile spawned %s -> %s", key, value)

    def _log_stopped(self, key: tuple) -> None:
        logger.info("OIFlowBookManager: reconcile stopped %s", key)
