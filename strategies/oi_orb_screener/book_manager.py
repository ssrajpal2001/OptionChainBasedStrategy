"""
strategies/oi_orb_screener/book_manager.py — lifecycle manager for
OiOrbScreenerStrategy books.

One OiOrbScreenerStrategy per (client, binding) with a running
"oi_orb_screener" deployment -- underlying is always the sentinel
"SCREENER" (mirrors D1 Trap FnO's own WATCHLIST sentinel: the real stocks
traded are chosen dynamically each day by the screener itself, not fixed
at deployment time). strategy_params JSON configures the screener
thresholds per book (see _DEFAULT_PARAMS).

Fresh, independent implementation -- reads its own "oi_orb_screener"-tagged
deployment rows only, never touches or is touched by any other strategy's
manager, per strategies/oi_orb_screener/__init__.py's standalone design.
"""
from __future__ import annotations

import json
import logging
from typing import Dict

from strategies.core.book_manager import StrategyBookManager
from strategies.oi_orb_screener.engine import OiOrbScreenerStrategy, _UNDERLYING_SENTINEL

logger = logging.getLogger(__name__)

_STRATEGY_NAME = "oi_orb_screener"
_DEFAULT_PARAMS = {
    "oi_spurt_min_pct": 7.0,
    "price_move_min_pct": 2.0,
    "stock_move_abort_pct": 4.0,
    "top_n_per_side": 5,
    "poll_seconds": 20,
    "regime_filter_enabled": True,
    # 2026-08-24, direct user request -- TEMPORARY connectivity-test toggle.
    # Default False. See screener.py's own CONFIG["IGNORE_TIME_WINDOWS"]
    # docstring for what this actually does and when to turn it back off.
    "ignore_time_windows": False,
}
_FLOAT_KEYS = ("oi_spurt_min_pct", "price_move_min_pct", "stock_move_abort_pct")
_INT_KEYS = ("top_n_per_side", "poll_seconds")


def _parse_params(raw: str) -> dict:
    try:
        params = json.loads(raw or "{}")
    except Exception:
        params = {}
    for k, v in _DEFAULT_PARAMS.items():
        params.setdefault(k, v)
    return params


class OiOrbScreenerBookManager(StrategyBookManager):

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
                "lots": lots,
                "product_type": d.get("product_type") or "MIS",
                "squareoff_time": d.get("squareoff_time") or "15:15",
            }
            for k in _FLOAT_KEYS:
                cfg[k] = float(params.get(k, _DEFAULT_PARAMS[k]))
            for k in _INT_KEYS:
                cfg[k] = int(params.get(k, _DEFAULT_PARAMS[k]))
            cfg["regime_filter_enabled"] = bool(params.get("regime_filter_enabled",
                                                             _DEFAULT_PARAMS["regime_filter_enabled"]))
            cfg["ignore_time_windows"] = bool(params.get("ignore_time_windows",
                                                           _DEFAULT_PARAMS["ignore_time_windows"]))
            # Key on the sentinel underlying so this fits the base class's
            # generic (client_id, binding_id, underlying) Key shape without
            # a real per-stock underlying -- the screener itself decides
            # which real stocks to trade each day, inside the one book.
            wanted[(cid, bid, _UNDERLYING_SENTINEL)] = cfg
        return wanted

    def _spawn_book(self, key: tuple, value: dict) -> OiOrbScreenerStrategy:
        client_id, binding_id, _underlying = key
        book = OiOrbScreenerStrategy(
            self._bus, self._cfg, client_id, binding_id,
            lot_multiplier=value["lots"],
            product_type=value["product_type"],
            squareoff_time=value["squareoff_time"],
            oi_spurt_min_pct=value["oi_spurt_min_pct"],
            price_move_min_pct=value["price_move_min_pct"],
            stock_move_abort_pct=value["stock_move_abort_pct"],
            top_n_per_side=value["top_n_per_side"],
            poll_seconds=value["poll_seconds"],
            regime_filter_enabled=value["regime_filter_enabled"],
            ignore_time_windows=value["ignore_time_windows"],
        )
        logger.info(
            "OiOrbScreenerBookManager: spawned %s/%s (lots=%d oi_spurt>=%.1f%% price_move>=%.1f%% "
            "top_n=%d regime_filter=%s ignore_time_windows=%s).",
            client_id, binding_id, value["lots"], value["oi_spurt_min_pct"],
            value["price_move_min_pct"], value["top_n_per_side"], value["regime_filter_enabled"],
            value["ignore_time_windows"],
        )
        if value["ignore_time_windows"]:
            logger.warning(
                "OiOrbScreenerBookManager: %s/%s spawned with ignore_time_windows=True -- "
                "TEMPORARY connectivity-test mode, real ORB/entry-window timing is bypassed. "
                "Turn this back off (strategy_params) once connectivity is confirmed.",
                client_id, binding_id,
            )
        return book

    def _is_flat(self, book: OiOrbScreenerStrategy) -> bool:
        """Overrides the base class's single-`_position` check -- this book
        can hold several concurrent stock positions in self._positions."""
        return not book._positions

    def _should_respawn(self, book: OiOrbScreenerStrategy, value: dict) -> bool:
        if book._lot_multiplier != value["lots"]:
            return True
        return (
            book._product_type != value["product_type"]
            or book._screener_cfg["OI_SPURT_MIN_PCT"] != value["oi_spurt_min_pct"]
            or book._screener_cfg["PRICE_MOVE_MIN_PCT"] != value["price_move_min_pct"]
            or book._screener_cfg["STOCK_MOVE_ABORT_PCT"] != value["stock_move_abort_pct"]
            or book._screener_cfg["TOP_N_PER_SIDE"] != value["top_n_per_side"]
            or book._screener_cfg["POLL_SECONDS"] != value["poll_seconds"]
            or book._screener_cfg["REGIME_FILTER_ENABLED"] != value["regime_filter_enabled"]
            or book._screener_cfg["IGNORE_TIME_WINDOWS"] != value["ignore_time_windows"]
        )

    def _log_spawned(self, key: tuple, value: dict) -> None:
        logger.info("OiOrbScreenerBookManager: reconcile spawned %s -> %s", key, value)

    def _log_stopped(self, key: tuple) -> None:
        logger.info("OiOrbScreenerBookManager: reconcile stopped %s", key)
