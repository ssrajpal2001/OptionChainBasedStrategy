"""
strategies/liquidity_sweep/book_manager.py — lifecycle manager for
LiquiditySweepStrategy books.

One LiquiditySweepStrategy per (client, binding, underlying) with a running
"liquidity_sweep" deployment. strategy_params JSON configures the full
sweep/displacement/FVG/retest/risk parameter set per book (all default to
the validated Pine script's own tuned values -- see engine.py).

Fresh, independent implementation -- reads its own "liquidity_sweep"-tagged
deployment rows only, never touches or is touched by any other strategy's
manager, per strategies/liquidity_sweep/__init__.py's standalone design.
"""
from __future__ import annotations

import json
import logging
from typing import Dict

from strategies.core.book_manager import StrategyBookManager
from strategies.liquidity_sweep.engine import LiquiditySweepStrategy

logger = logging.getLogger(__name__)

_STRATEGY_NAME = "liquidity_sweep"
_DEFAULT_PARAMS = {
    "ltf_min": 5,
    "htf_min": 75,
    "pivot_left": 5,
    "pivot_right": 5,
    "pool_tol_pts": 5.0,
    "pool_min_touches": 2,
    "atr_len": 14,
    "atr_mult": 0.7,
    "disp_window": 6,
    "swing_len": 3,
    "fvg_confirm_window": 3,
    "stale_bars": 12,
    "tgt1_rr": 1.5,
    "tgt2_rr": 3.0,
    "itm_offset_pts": 0.0,
    "hard_risk_rs_per_lot": 2000.0,
    "sl_cooldown_minutes": 15.0,
}
_FLOAT_KEYS = tuple(_DEFAULT_PARAMS.keys())
# Non-numeric per-book toggles, parsed separately from the plain float/int params above.
_STR_DEFAULT_LIQ_SOURCE = "liquidity_pool"
_BOOL_DEFAULT_USE_STRUCT_BIAS = True
_BOOL_DEFAULT_USE_LIQUIDITY_TARGET2 = True


def _parse_params(raw: str) -> dict:
    try:
        params = json.loads(raw or "{}")
    except Exception:
        params = {}
    for k, v in _DEFAULT_PARAMS.items():
        params.setdefault(k, v)
    params.setdefault("liq_source", _STR_DEFAULT_LIQ_SOURCE)
    params.setdefault("use_struct_bias", _BOOL_DEFAULT_USE_STRUCT_BIAS)
    params.setdefault("use_liquidity_target2", _BOOL_DEFAULT_USE_LIQUIDITY_TARGET2)
    return params


class LiquiditySweepBookManager(StrategyBookManager):

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
                   "liq_source": params["liq_source"],
                   "use_struct_bias": bool(params["use_struct_bias"]),
                   "use_liquidity_target2": bool(params["use_liquidity_target2"])}
            for k in _FLOAT_KEYS:
                cfg[k] = float(params.get(k, _DEFAULT_PARAMS[k]))
            wanted[(cid, bid, underlying)] = cfg
        return wanted

    def _spawn_book(self, key: tuple, value: dict) -> LiquiditySweepStrategy:
        client_id, binding_id, underlying = key
        self._enable_chain(underlying)
        feeder_token = ""
        try:
            creds = self._db.get_feeder_creds_sync("upstox") or {}
            feeder_token = creds.get("access_token", "")
        except Exception:
            pass
        book = LiquiditySweepStrategy(
            self._bus, self._cfg, underlying, client_id, binding_id,
            lot_multiplier=value["lots"],
            ltf_min=int(value["ltf_min"]), htf_min=int(value["htf_min"]),
            liq_source=value["liq_source"],
            pivot_left=int(value["pivot_left"]), pivot_right=int(value["pivot_right"]),
            pool_tol_pts=value["pool_tol_pts"], pool_min_touches=int(value["pool_min_touches"]),
            use_struct_bias=value["use_struct_bias"],
            atr_len=int(value["atr_len"]), atr_mult=value["atr_mult"],
            disp_window=int(value["disp_window"]), swing_len=int(value["swing_len"]),
            fvg_confirm_window=int(value["fvg_confirm_window"]), stale_bars=int(value["stale_bars"]),
            tgt1_rr=value["tgt1_rr"], use_liquidity_target2=value["use_liquidity_target2"],
            tgt2_rr=value["tgt2_rr"], itm_offset_pts=value["itm_offset_pts"],
            hard_risk_rs_per_lot=value["hard_risk_rs_per_lot"],
            sl_cooldown_minutes=value["sl_cooldown_minutes"],
            product_type=value["product_type"], squareoff_time=value["squareoff_time"],
            feeder_token=feeder_token,
        )
        logger.info(
            "LiquiditySweepBookManager: spawned %s/%s/%s (lots=%d ltf=%dm liq_source=%s "
            "atr_mult=%.2f disp_window=%d tgt1_rr=%.1f).",
            client_id, binding_id, underlying, value["lots"], value["ltf_min"], value["liq_source"],
            value["atr_mult"], value["disp_window"], value["tgt1_rr"],
        )
        return book

    def _should_respawn(self, book: LiquiditySweepStrategy, value: dict) -> bool:
        if book._lot_multiplier != value["lots"]:
            return True
        if book._liq_source != value["liq_source"]:
            return True
        if book._use_struct_bias != value["use_struct_bias"]:
            return True
        if book._use_liquidity_target2 != value["use_liquidity_target2"]:
            return True
        if int(book._ltf_min) != int(value["ltf_min"]) or int(book._htf_min) != int(value["htf_min"]):
            return True
        return any(getattr(book, f"_{k}") != value[k] for k in _FLOAT_KEYS
                   if k not in ("ltf_min", "htf_min"))

    def _log_spawned(self, key: tuple, value: dict) -> None:
        logger.info("LiquiditySweepBookManager: reconcile spawned %s -> %s", key, value)

    def _log_stopped(self, key: tuple) -> None:
        logger.info("LiquiditySweepBookManager: reconcile stopped %s", key)
