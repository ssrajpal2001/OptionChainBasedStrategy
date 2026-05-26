"""
data_layer/runtime_config.py — Runtime strategy configuration persistence.

All strategy thresholds, indicator periods, and RMS limits live here instead
of being hardcoded in execution modules.  Written to data/strategy_config.json
on every admin update so the last operator-set state survives a restart.

Usage:
    from data_layer.runtime_config import RuntimeConfig
    cfg = RuntimeConfig.get()
    rsi_period = cfg["indicators"]["rsi_period"]          # 14
    ic_rsi_min = cfg["iron_condor"]["rsi_min"]            # 40.0

    RuntimeConfig.update(patch_dict)   # live-update + persist
"""

from __future__ import annotations

import copy
import json
import logging
import os
from typing import Any, Dict

logger = logging.getLogger(__name__)

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "strategy_config.json")

_DEFAULTS: Dict[str, Any] = {
    "rms": {
        "max_drawdown_pct":       5.0,
        "order_throttle_per_sec": 5,
        "squareoff_time":         "15:15",
        "distance_filter_pct":    5.0,
    },
    "indicators": {
        "rsi_period":   14,
        "vwap_window":  500,
        "adx_period":   20,
        "ema_fast":     9,
        "ema_slow":     21,
        "htf_minutes":  75,
        "ltf_minutes":  5,
    },
    "iron_condor": {
        "squareoff_time": "15:15",
        "rsi_min":  40.0,
        "rsi_max":  60.0,
        "adx_max":  25.0,
        "profit_pct": 50.0,
        "sl_pct":     200.0,
        "per_index": {
            "NIFTY":      {"short_otm_pts": 200.0, "wing_width_pts": 200.0},
            "BANKNIFTY":  {"short_otm_pts": 400.0, "wing_width_pts": 500.0},
            "FINNIFTY":   {"short_otm_pts": 200.0, "wing_width_pts": 200.0},
            "SENSEX":     {"short_otm_pts": 500.0, "wing_width_pts": 500.0},
            "MIDCPNIFTY": {"short_otm_pts": 150.0, "wing_width_pts": 200.0},
        },
    },
    "sell_straddle": {
        "entry_start":     "09:20",
        "entry_end":       "12:00",
        "squareoff_time":  "15:15",
        "rsi_min":         35.0,
        "rsi_max":         65.0,
        "adx_max":         30.0,
        "profit_pct":      30.0,
        "sl_pct":          200.0,
        "trail_lock_pct":  20.0,
        "trail_floor_pct": 10.0,
        "max_trades":      1,
        "roc_limit_pct":   1.5,
    },
    "trap_trading": {
        "htf_minutes":            75,
        "ltf_minutes":            5,
        "adx_threshold":          20.0,
        "volume_spike_multiplier": 1.5,
        "swing_lookback":         5,
        "zone_tolerance_pct":     0.5,
        "void_atr_mult":          2.0,
    },
}

# In-memory live copy — mutated by update()
_live: Dict[str, Any] = {}


def _deep_merge(base: dict, patch: dict) -> dict:
    """Recursively merge patch into base, returning a new dict."""
    result = copy.deepcopy(base)
    for k, v in patch.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def _load_from_disk() -> Dict[str, Any]:
    path = os.path.abspath(_CONFIG_PATH)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        logger.warning("RuntimeConfig: failed to load %s: %s", path, exc)
        return {}


def _save_to_disk(data: Dict[str, Any]) -> None:
    path = os.path.abspath(_CONFIG_PATH)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as exc:
        logger.error("RuntimeConfig: failed to save %s: %s", path, exc)


def _ensure_loaded() -> None:
    global _live
    if not _live:
        disk = _load_from_disk()
        _live = _deep_merge(_DEFAULTS, disk)


class RuntimeConfig:
    """Singleton accessor for the live runtime configuration."""

    @staticmethod
    def get() -> Dict[str, Any]:
        _ensure_loaded()
        return _live

    @staticmethod
    def section(name: str) -> Dict[str, Any]:
        _ensure_loaded()
        return _live.get(name, {})

    @staticmethod
    def update(patch: Dict[str, Any]) -> None:
        global _live
        _ensure_loaded()
        _live = _deep_merge(_live, patch)
        _save_to_disk(_live)
        logger.info("RuntimeConfig: updated and persisted.")

    @staticmethod
    def reload_from_disk() -> None:
        global _live
        disk = _load_from_disk()
        _live = _deep_merge(_DEFAULTS, disk)
        logger.info("RuntimeConfig: reloaded from disk.")

    @staticmethod
    def defaults() -> Dict[str, Any]:
        return copy.deepcopy(_DEFAULTS)
