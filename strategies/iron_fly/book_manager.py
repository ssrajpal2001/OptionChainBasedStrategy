"""
strategies/iron_fly/book_manager.py — lifecycle manager for IronFlyStrategy
books.

One IronFlyStrategy per (client, binding, underlying) with a running
"iron_fly" deployment. Fresh, independent implementation -- reads its own
"iron_fly"-tagged deployment rows only, same standalone mandate as every
other strategy manager in this codebase.

Direct user spec (2026-09-14): every tunable must be dynamic via
strategy_params, not hardcoded -- see _DEFAULT_PARAMS below for the full
list and their Phase-1-validated defaults.
"""
from __future__ import annotations

import json
import logging
from typing import Dict

from strategies.core.book_manager import StrategyBookManager
from strategies.iron_fly.engine import IronFlyStrategy

logger = logging.getLogger(__name__)

_STRATEGY_NAME = "iron_fly"

# Every one of these is exposed as a dynamic, per-deployment override via
# the dashboard's strategy_params JSON -- direct user spec, "all variables
# as dynamic". Defaults match the values IronFlyEngine itself defaults to
# (Phase 1's backtest-validated mechanic), so an empty '{}' deploys exactly
# what was already validated.
_DEFAULT_PARAMS = {
    "otm1": 50,
    "adjustment_distance": 100.0,
    "short_threshold": 20.0,
    "long_threshold": 20.0,
    "profit_target_pct": 0.65,
    "chain_depth_strikes": 20,
}
_INT_KEYS = ("otm1", "chain_depth_strikes")
_FLOAT_KEYS = tuple(k for k in _DEFAULT_PARAMS if k not in _INT_KEYS)


def _parse_params(raw: str) -> dict:
    try:
        params = json.loads(raw or "{}")
    except Exception:
        params = {}
    for k, v in _DEFAULT_PARAMS.items():
        params.setdefault(k, v)
    return params


class IronFlyBookManager(StrategyBookManager):

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
            cfg = {"lots": lots, "product_type": d.get("product_type") or "NRML"}
            for k in _INT_KEYS:
                cfg[k] = int(params.get(k, _DEFAULT_PARAMS[k]))
            for k in _FLOAT_KEYS:
                cfg[k] = float(params.get(k, _DEFAULT_PARAMS[k]))
            wanted[(cid, bid, underlying)] = cfg
        return wanted

    def _spawn_book(self, key: tuple, value: dict) -> IronFlyStrategy:
        client_id, binding_id, underlying = key
        self._enable_chain(underlying)
        self._widen_chain_depth(underlying, value["chain_depth_strikes"])
        book = IronFlyStrategy(
            self._bus, self._cfg, underlying, client_id, binding_id,
            lot_multiplier=value["lots"],
            otm1=value["otm1"],
            adjustment_distance=value["adjustment_distance"],
            short_threshold=value["short_threshold"],
            long_threshold=value["long_threshold"],
            profit_target_pct=value["profit_target_pct"],
            chain_depth_strikes=value["chain_depth_strikes"],
            product_type=value["product_type"],
        )
        # 2026-09-17 CRITICAL FIX, real gap found via direct user log check:
        # the base StrategyBookManager.set_rebalancer() only reaches books
        # that ALREADY EXIST at the moment it's called (once, at process
        # startup, per run_system.py's own wiring) -- it never re-injects
        # into a book spawned LATER by this manager's own reconcile loop,
        # which is exactly how every Iron Fly book actually comes to exist.
        # Confirmed live: zero "strikes pinned in StrikeRebalancer" log
        # lines ever appeared despite a real, actively-adjusting Iron Fly
        # position running all day -- the 2026-09-17 pin fix in engine.py
        # was correct but silently inert because self._rebalancer was
        # always None on every real book. StraddleBookManager._spawn_book
        # already does this same injection -- mirrored here.
        if self._rebalancer is not None and hasattr(book, "set_rebalancer"):
            book.set_rebalancer(self._rebalancer)
        logger.info(
            "IronFlyBookManager: spawned %s/%s/%s (lots=%d otm1=%d adjustment_distance=%.0f "
            "short_threshold=%.1f long_threshold=%.1f profit_target_pct=%.2f chain_depth_strikes=%d).",
            client_id, binding_id, underlying, value["lots"], value["otm1"],
            value["adjustment_distance"], value["short_threshold"], value["long_threshold"],
            value["profit_target_pct"], value["chain_depth_strikes"],
        )
        return book

    def _widen_chain_depth(self, underlying: str, depth: int) -> None:
        """Persists this deployment's own chain_depth_strikes into
        RuntimeConfig so data_layer/strike_rebalancer.py's
        _effective_chain_depth() actually widens the live WS subscription
        window to match -- without this, wing strikes near the Rs20
        threshold sit well outside the platform's default ATM+/-4 window
        and simply never tick (see the approved plan's own infra note)."""
        try:
            from data_layer.runtime_config import RuntimeConfig
            RuntimeConfig.set_index_section(underlying, "iron_fly", {"chain_depth": int(depth)})
        except Exception:
            logger.exception("IronFlyBookManager: failed to widen chain depth for %s (non-fatal).", underlying)

    def _is_flat(self, book: IronFlyStrategy) -> bool:
        return book.is_flat()

    def _should_respawn(self, book: IronFlyStrategy, value: dict) -> bool:
        if book._lot_multiplier != value["lots"]:
            return True
        for k in _INT_KEYS + _FLOAT_KEYS:
            if getattr(book, f"_{k}") != value[k]:
                return True
        return False

    def _log_spawned(self, key: tuple, value: dict) -> None:
        logger.info("IronFlyBookManager: reconcile spawned %s -> %s", key, value)

    def _log_stopped(self, key: tuple) -> None:
        logger.info("IronFlyBookManager: reconcile stopped %s", key)
