"""
strategies/oi_bias_rsi_exit/book_manager.py — lifecycle manager for
OiBiasRsiExitStrategy books.

One book per (client, binding) with a running "oi_bias_rsi_exit"
deployment -- underlying is always the sentinel "SCREENER" (mirrors OI-ORB
Screener's own precedent: the real stocks traded are chosen dynamically
each day, not fixed at deployment time).
"""
from __future__ import annotations

import json
import logging
from typing import Dict

from strategies.core.book_manager import StrategyBookManager
from strategies.oi_bias_rsi_exit.engine import OiBiasRsiExitStrategy, UNDERLYING_SENTINEL

logger = logging.getLogger(__name__)

_STRATEGY_NAME = "oi_bias_rsi_exit"
_DEFAULT_PARAMS = {
    "oi_spurt_min_pct": 7.0,
    "top_n_per_side": 10,
    "poll_seconds": 60.0,
    # Direct user spec (2026-09-29): defaults match the validated backtest.
    "start_time": "09:25",
    "force_exit_time": "15:25",
    # 2026-09-30 CRITICAL FIX, direct user audit request: these used to be
    # module-level constants in engine.py with ZERO per-deployment override
    # -- now genuinely configurable, same as every other tunable here.
    # Defaults match the 2026-09-27 optimize.py-validated values.
    "entry_timeframe_min": 3,
    "entry_stoch_rsi_lengths": [21, 21, 3, 3],
    "exit_timeframe_min": 75,
    "exit_stoch_rsi_lengths": [21, 21, 3, 3],
    "oi_recheck_minutes": 5,
    "oi_bias_flip_count": 2,
}
_FLOAT_KEYS = ("oi_spurt_min_pct", "poll_seconds")
_INT_KEYS = ("entry_timeframe_min", "exit_timeframe_min", "oi_recheck_minutes", "oi_bias_flip_count")


def _parse_params(raw: str) -> dict:
    try:
        params = json.loads(raw or "{}")
    except Exception:
        params = {}
    for k, v in _DEFAULT_PARAMS.items():
        params.setdefault(k, v)
    return params


class OiBiasRsiExitBookManager(StrategyBookManager):

    def _wanted(self) -> Dict[tuple, dict]:
        wanted: Dict[tuple, dict] = {}
        rows = self._db.get_running_deployments_by_strategy_sync(_STRATEGY_NAME)
        for d in rows or []:
            cid = d.get("client_id", "")
            bid = d.get("binding_id", "")
            if not cid or not bid:
                continue
            try:
                lots = max(1, int(round(float(d.get("lot_multiplier", 1) or 1))))
            except Exception:
                lots = 1
            params = _parse_params(d.get("strategy_params", "{}"))
            cfg = {
                "lots": lots, "product_type": d.get("product_type") or "NRML",
                "start_time": params["start_time"], "force_exit_time": params["force_exit_time"],
                "top_n_per_side": int(params["top_n_per_side"]),
                "entry_stoch_rsi_lengths": tuple(params["entry_stoch_rsi_lengths"]),
                "exit_stoch_rsi_lengths": tuple(params["exit_stoch_rsi_lengths"]),
            }
            for k in _FLOAT_KEYS:
                cfg[k] = float(params.get(k, _DEFAULT_PARAMS[k]))
            for k in _INT_KEYS:
                cfg[k] = int(params.get(k, _DEFAULT_PARAMS[k]))
            wanted[(cid, bid, UNDERLYING_SENTINEL)] = cfg
        return wanted

    def _spawn_book(self, key: tuple, value: dict) -> OiBiasRsiExitStrategy:
        client_id, binding_id, _ = key
        book = OiBiasRsiExitStrategy(
            self._bus, self._cfg, client_id, binding_id,
            lot_multiplier=value["lots"], product_type=value["product_type"],
            start_time=value["start_time"], force_exit_time=value["force_exit_time"],
            oi_spurt_min_pct=value["oi_spurt_min_pct"], top_n_per_side=value["top_n_per_side"],
            poll_seconds=value["poll_seconds"],
            entry_timeframe_min=value["entry_timeframe_min"],
            entry_stoch_rsi_lengths=value["entry_stoch_rsi_lengths"],
            exit_timeframe_min=value["exit_timeframe_min"],
            exit_stoch_rsi_lengths=value["exit_stoch_rsi_lengths"],
            oi_recheck_minutes=value["oi_recheck_minutes"],
            oi_bias_flip_count=value["oi_bias_flip_count"],
        )
        logger.info(
            "OiBiasRsiExitBookManager: spawned %s/%s (lots=%d product=%s start=%s exit=%s).",
            client_id, binding_id, value["lots"], value["product_type"],
            value["start_time"], value["force_exit_time"],
        )
        return book

    def _is_flat(self, book: OiBiasRsiExitStrategy) -> bool:
        return not book._positions

    def _should_respawn(self, book: OiBiasRsiExitStrategy, value: dict) -> bool:
        if book._lot_multiplier != value["lots"]:
            return True
        return (
            book._product_type != value["product_type"]
            or book._start_time.strftime("%H:%M") != value["start_time"]
            or book._force_exit_time.strftime("%H:%M") != value["force_exit_time"]
            or book._oi_spurt_min_pct != value["oi_spurt_min_pct"]
            or book._top_n_per_side != value["top_n_per_side"]
            or book._poll_seconds != value["poll_seconds"]
            or book._entry_timeframe_min != value["entry_timeframe_min"]
            or book._entry_stoch_rsi_lengths != value["entry_stoch_rsi_lengths"]
            or book._exit_timeframe_min != value["exit_timeframe_min"]
            or book._exit_stoch_rsi_lengths != value["exit_stoch_rsi_lengths"]
            or book._oi_recheck_minutes != value["oi_recheck_minutes"]
            or book._oi_bias_flip_count != value["oi_bias_flip_count"]
        )

    def _log_spawned(self, key: tuple, value: dict) -> None:
        logger.info("OiBiasRsiExitBookManager: reconcile spawned %s -> %s", key, value)

    def _log_stopped(self, key: tuple) -> None:
        logger.info("OiBiasRsiExitBookManager: reconcile stopped %s", key)
