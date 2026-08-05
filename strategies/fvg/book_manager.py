"""
strategies/fvg/book_manager.py — lifecycle manager for FVGStrategy books.

One FVGStrategy per (client, binding, underlying) with a running "fvg"
deployment. strategy_params JSON configures itm_offset_pts/htf_tf/ltf_tf/
direction_mode plus the step-locked TSL params (initial_sl_pct,
trail_trigger_pct, first_lock_pct, step_pct, step_lock_pct) per book.
Mirrors D1TrapOptionBookManager (strategies/d1_trap_option/book_manager.py).
"""
from __future__ import annotations

import json
import logging
from typing import Dict

from strategies.core.book_manager import StrategyBookManager
from strategies.fvg.engine import FVGStrategy

logger = logging.getLogger(__name__)

_STRATEGY_NAME = "fvg"
_DIRECTION_MODES = ("BOTH", "CE_ONLY", "PE_ONLY")
# 2026-08-03 validated baseline: HTF=10m/LTF=3m (scripts/fvg_tf_sweep.py --
# PF 1.79, win% 56.2%, balanced CE/PE), itm_offset_pts=50 (1-strike ITM),
# intraday-only FVG pool (a gap from an earlier day can never fire an entry
# today -- see strategies/fvg/engine.py's reset_session()/_rebuild_fvg_pool()),
# and a re-tuned step-locked TSL (trigger 15%/lock 8%/step 10%/step_lock 5% --
# the earlier 25% trigger rarely fired before the 40min stagnation timer;
# scripts/fvg_tsl_sweep.py + follow-up re-tune: PF 1.43, Net +Rs1,979, n=13
# after removing stale multi-day-FVG trades). All overridable per deployment
# via strategy_params.
_DEFAULT_PARAMS = {
    "itm_offset_pts": 50, "min_rr": 2.0, "htf_tf": 10, "ltf_tf": 3, "direction_mode": "BOTH",
    "initial_sl_pct": 0.20, "trail_trigger_pct": 0.15, "first_lock_pct": 0.08,
    "step_pct": 0.10, "step_lock_pct": 0.05,
}
_TSL_KEYS = ("initial_sl_pct", "trail_trigger_pct", "first_lock_pct", "step_pct", "step_lock_pct")


def _parse_params(raw: str) -> dict:
    try:
        params = json.loads(raw or "{}")
    except Exception:
        params = {}
    for k, v in _DEFAULT_PARAMS.items():
        params.setdefault(k, v)
    if params.get("direction_mode") not in _DIRECTION_MODES:
        params["direction_mode"] = "BOTH"
    return params


class FVGBookManager(StrategyBookManager):

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
                "lots": lots,
                "itm_offset_pts": int(params.get("itm_offset_pts", 50)),
                "min_rr": float(params.get("min_rr", 2.0)),
                "htf_tf": int(params.get("htf_tf", 10)),
                "ltf_tf": int(params.get("ltf_tf", 3)),
                "direction_mode": params.get("direction_mode", "BOTH"),
                "product_type": d.get("product_type") or "MIS",
            }
            for k in _TSL_KEYS:
                cfg[k] = float(params.get(k, _DEFAULT_PARAMS[k]))
            wanted[(cid, bid, underlying)] = cfg
        return wanted

    def _spawn_book(self, key: tuple, value: dict) -> FVGStrategy:
        client_id, binding_id, underlying = key
        feeder_token = ""
        try:
            creds = self._db.get_feeder_creds_sync("upstox") or {}
            feeder_token = creds.get("access_token", "")
        except Exception:
            pass
        # 2026-08-03: same fix as D1TrapBookManager -- FVG never enabled the option
        # chain for its own underlying either. StrikeRebalancer's ATM+/-chain_depth
        # subscription only activates once SOME strategy calls enable_chain(); without
        # this, an FVG deployment on an underlying with no sell_straddle/v4_cascade
        # also running there would spawn fine but never receive live option ticks.
        self._enable_chain(underlying)
        book = FVGStrategy(
            self._bus, self._cfg, underlying, client_id, binding_id,
            lot_multiplier=value["lots"], feeder_token=feeder_token,
            itm_offset_pts=value["itm_offset_pts"], min_rr=value["min_rr"],
            product_type=value["product_type"],
            htf_mins=value["htf_tf"], ltf_mins=value["ltf_tf"],
            direction_mode=value["direction_mode"],
            initial_sl_pct=value["initial_sl_pct"], trail_trigger_pct=value["trail_trigger_pct"],
            first_lock_pct=value["first_lock_pct"], step_pct=value["step_pct"],
            step_lock_pct=value["step_lock_pct"],
        )
        logger.info("FVGBookManager: spawned %s/%s/%s (lots=%d itm=%d htf=%dm ltf=%dm mode=%s "
                    "| TSL trigger=%.1f%% lock=%.1f%% step=%.1f%%/%.1f%%).",
                    client_id, binding_id, underlying, value["lots"], value["itm_offset_pts"],
                    value["htf_tf"], value["ltf_tf"], value["direction_mode"],
                    value["trail_trigger_pct"] * 100, value["first_lock_pct"] * 100,
                    value["step_pct"] * 100, value["step_lock_pct"] * 100)
        return book

    def _should_respawn(self, book: FVGStrategy, value: dict) -> bool:
        if (book._lot_multiplier != value["lots"]
                or book._htf_mins != value["htf_tf"]
                or book._ltf_mins != value["ltf_tf"]
                or book._direction_mode != value["direction_mode"]):
            return True
        return (book._initial_sl_pct != value["initial_sl_pct"]
                or book._trail_trigger_pct != value["trail_trigger_pct"]
                or book._first_lock_pct != value["first_lock_pct"]
                or book._step_pct != value["step_pct"]
                or book._step_lock_pct != value["step_lock_pct"])

    def _log_spawned(self, key: tuple, value: dict) -> None:
        logger.info("FVGBookManager: reconcile spawned %s -> %s", key, value)

    def _log_stopped(self, key: tuple) -> None:
        logger.info("FVGBookManager: reconcile stopped %s", key)
