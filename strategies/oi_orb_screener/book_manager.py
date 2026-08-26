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
    # 2026-08-25, direct user request: expose every remaining screener.CONFIG
    # tunable per-deployment too (admin panel, ui_layer/dashboard_server.py's
    # /api/admin/oiorb/config/{deploy_id}), not just the original 7. SCORE_WEIGHTS
    # deliberately excluded -- a 4-way dict, not a scalar knob, lower priority.
    "nifty_bullish_pct": 0.3,
    "nifty_bearish_pct": -0.3,
    "max_monitor_minutes": 90,
    "rejection_min_rise_pct": 2.0,
    "rejection_retrace_fraction": 0.5,
    "sma_period": 8,
    "sma_exit_consec_closes": 2,
    "sma_tf_min": 5,
    "sma_seed_lookback_days": 5,
    "strike_otm_pct": 2.0,
    "orb_start": "09:15",
    "orb_end": "09:25",
    "scan_start": "09:25",
    "entry_window_start": "09:25",
    "entry_window_end": "10:30",
    # 2026-08-25, direct user spec: five additive, independently-toggleable
    # filters (see strategies/oi_orb_screener/filters.py's module docstring
    # for the real incident -- a SAIL CALL breakout fired right under a
    # large Call-OI wall). Each *_enabled flag ONLY controls whether that
    # filter can actually block a trade -- every filter always evaluates
    # and logs to its own dedicated file regardless. Default OFF (log-only)
    # until real forward telemetry earns a promotion to a real gate.
    "oi_wall_check_enabled": False,
    "oi_wall_dominance_ratio": 1.5,
    "distance_to_wall_enabled": False,
    "distance_to_wall_min_pct": 1.5,
    "pcr_gate_enabled": False,
    "pcr_max_for_call": 1.2,
    "pcr_min_for_put": 0.8,
    "volume_confirmation_enabled": False,
    "volume_confirmation_min_ratio": 1.5,
    "oi_roc_enabled": False,
    "oi_roc_min_pct": 3.0,
    "oi_roc_lookback_sec": 300.0,
    # 2026-08-25, direct user spec: the WS feed subscription is a single
    # SHARED budget across every strategy in this app (~50 symbols/broker
    # connection) -- watching every shortlisted stock's full option chain
    # unbounded could silently starve ticks for a completely different
    # strategy. Cap chain-watching (the 3 chain-dependent filters: oi_wall/
    # distance_to_wall/pcr) to the top N shortlisted stocks by rank until
    # this strategy gets its own dedicated feeder connection.
    "chain_watch_max_stocks": 2,
}
_FLOAT_KEYS = ("oi_spurt_min_pct", "price_move_min_pct", "stock_move_abort_pct",
               "nifty_bullish_pct", "nifty_bearish_pct", "rejection_min_rise_pct",
               "rejection_retrace_fraction", "strike_otm_pct",
               "oi_wall_dominance_ratio", "distance_to_wall_min_pct",
               "pcr_max_for_call", "pcr_min_for_put", "volume_confirmation_min_ratio",
               "oi_roc_min_pct", "oi_roc_lookback_sec")
_INT_KEYS = ("top_n_per_side", "poll_seconds", "max_monitor_minutes",
             "sma_period", "sma_exit_consec_closes", "chain_watch_max_stocks",
             "sma_tf_min", "sma_seed_lookback_days")
_STR_KEYS = ("orb_start", "orb_end", "scan_start", "entry_window_start", "entry_window_end")
_FILTER_BOOL_KEYS = ("oi_wall_check_enabled", "distance_to_wall_enabled", "pcr_gate_enabled",
                      "volume_confirmation_enabled", "oi_roc_enabled")


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
            for k in _STR_KEYS:
                cfg[k] = str(params.get(k, _DEFAULT_PARAMS[k]))
            cfg["regime_filter_enabled"] = bool(params.get("regime_filter_enabled",
                                                             _DEFAULT_PARAMS["regime_filter_enabled"]))
            cfg["ignore_time_windows"] = bool(params.get("ignore_time_windows",
                                                           _DEFAULT_PARAMS["ignore_time_windows"]))
            for k in _FILTER_BOOL_KEYS:
                cfg[k] = bool(params.get(k, _DEFAULT_PARAMS[k]))
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
            nifty_bullish_pct=value["nifty_bullish_pct"],
            nifty_bearish_pct=value["nifty_bearish_pct"],
            max_monitor_minutes=value["max_monitor_minutes"],
            rejection_min_rise_pct=value["rejection_min_rise_pct"],
            rejection_retrace_fraction=value["rejection_retrace_fraction"],
            sma_period=value["sma_period"],
            sma_exit_consec_closes=value["sma_exit_consec_closes"],
            sma_tf_min=value["sma_tf_min"],
            sma_seed_lookback_days=value["sma_seed_lookback_days"],
            strike_otm_pct=value["strike_otm_pct"],
            orb_start=value["orb_start"],
            orb_end=value["orb_end"],
            scan_start=value["scan_start"],
            entry_window_start=value["entry_window_start"],
            entry_window_end=value["entry_window_end"],
            oi_wall_check_enabled=value["oi_wall_check_enabled"],
            oi_wall_dominance_ratio=value["oi_wall_dominance_ratio"],
            distance_to_wall_enabled=value["distance_to_wall_enabled"],
            distance_to_wall_min_pct=value["distance_to_wall_min_pct"],
            pcr_gate_enabled=value["pcr_gate_enabled"],
            pcr_max_for_call=value["pcr_max_for_call"],
            pcr_min_for_put=value["pcr_min_for_put"],
            volume_confirmation_enabled=value["volume_confirmation_enabled"],
            volume_confirmation_min_ratio=value["volume_confirmation_min_ratio"],
            oi_roc_enabled=value["oi_roc_enabled"],
            oi_roc_min_pct=value["oi_roc_min_pct"],
            oi_roc_lookback_sec=value["oi_roc_lookback_sec"],
            chain_watch_max_stocks=value["chain_watch_max_stocks"],
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
            or book._screener_cfg["NIFTY_BULLISH_PCT"] != value["nifty_bullish_pct"]
            or book._screener_cfg["NIFTY_BEARISH_PCT"] != value["nifty_bearish_pct"]
            or book._screener_cfg["MAX_MONITOR_MINUTES"] != value["max_monitor_minutes"]
            or book._screener_cfg["REJECTION_MIN_RISE_PCT"] != value["rejection_min_rise_pct"]
            or book._screener_cfg["REJECTION_RETRACE_FRACTION"] != value["rejection_retrace_fraction"]
            or book._screener_cfg["SMA_PERIOD"] != value["sma_period"]
            or book._screener_cfg["SMA_EXIT_CONSEC_CLOSES"] != value["sma_exit_consec_closes"]
            or book._screener_cfg["SMA_TF_MIN"] != value["sma_tf_min"]
            or book._screener_cfg["SMA_SEED_LOOKBACK_DAYS"] != value["sma_seed_lookback_days"]
            or book._screener_cfg["STRIKE_OTM_PCT"] != value["strike_otm_pct"]
            or book._screener_cfg["ORB_START"] != value["orb_start"]
            or book._screener_cfg["ORB_END"] != value["orb_end"]
            or book._screener_cfg["SCAN_START"] != value["scan_start"]
            or book._screener_cfg["ENTRY_WINDOW_START"] != value["entry_window_start"]
            or book._screener_cfg["ENTRY_WINDOW_END"] != value["entry_window_end"]
            or any(book._filters_cfg[k] != value[k] for k in (
                "oi_wall_check_enabled", "oi_wall_dominance_ratio",
                "distance_to_wall_enabled", "distance_to_wall_min_pct",
                "pcr_gate_enabled", "pcr_max_for_call", "pcr_min_for_put",
                "volume_confirmation_enabled", "volume_confirmation_min_ratio",
                "oi_roc_enabled", "oi_roc_min_pct", "oi_roc_lookback_sec",
            ))
            or book._chain_watch_max_stocks != value["chain_watch_max_stocks"]
        )

    def _log_spawned(self, key: tuple, value: dict) -> None:
        logger.info("OiOrbScreenerBookManager: reconcile spawned %s -> %s", key, value)

    def _log_stopped(self, key: tuple) -> None:
        logger.info("OiOrbScreenerBookManager: reconcile stopped %s", key)
