"""
data_layer/runtime_config.py — Runtime strategy configuration persistence.

All strategy thresholds, indicator periods, and RMS limits live here instead
of being hardcoded in execution modules.  Written to data/strategy_config.json
on every admin update so the last operator-set state survives a restart.

Usage:
    from data_layer.runtime_config import RuntimeConfig
    cfg = RuntimeConfig.get()
    rsi_period = cfg["indicators"]["rsi_period"]          # 14

    # Per-index config (new, rule-builder based):
    ss_cfg = RuntimeConfig.index_section("NIFTY", "sell_straddle")
    ic_cfg = RuntimeConfig.index_section("NIFTY", "iron_condor")

    RuntimeConfig.update(patch_dict)                       # flat section update
    RuntimeConfig.set_index_section("NIFTY", "sell_straddle", data)  # per-index
"""

from __future__ import annotations

import copy
import json
import logging
import os
from typing import Any, Dict

logger = logging.getLogger(__name__)

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "strategy_config.json")

# ── Per-index sell_straddle defaults ─────────────────────────────────────────
_SS_INDEX_DEFAULT: Dict[str, Any] = {
    "entry_start":           "09:20",
    "entry_end":             "15:15",
    "squareoff_time":        "15:15",
    "entry_workflow_mode":   "hybrid",       # hybrid | beginning_only | reentry_only
    # Pre-entry LTP filter: BOTH CE and PE must individually be >= ltp_target.
    # 0 = disabled. sell_v3 default is 50.0; set to 0 until admin configures per index.
    "ltp_target":            0.0,
    # Trailing SL: activates at trail_lock_pct% profit, floor = trail_floor_pct% below peak.
    # Set trail_lock_pct = 0 to disable. Values are percentages (divided by 100 in strategy).
    "trail_lock_pct":        20.0,
    "trail_floor_pct":       10.0,
    "pool_itm_depth":        4,
    "pool_otm_depth":        4,
    "entry_rules_beginning": [],
    "entry_rules_reentry":   [],
    "exit_rules":            [],
    "profit_target_enabled": True,
    "profit_pct":            30.0,   # per-trade target as % of credit
    "sl_enabled":            True,
    "sl_pct":                200.0,
    # Day-level % guardrails (% of initial net credit). 0 = disabled.
    "profit_target_pct":     0.0,    # stop for day when session P&L ≥ X% of credit
    "loss_sl_pct":           0.0,    # stop for day when session loss ≥ X% of credit
    "tsl_enabled":           False,
    "trail_lock_pct":        20.0,
    "trail_floor_pct":       10.0,
    "tsl_scalable": {
        "enabled":      False,
        "base_profit":  3000,
        "base_lock":    1500,
        "step_profit":  1000,
        "step_lock":    500,
    },
    # ROC guardrail: exit if ROC-of-combined-premium exceeds bounds (pts)
    "guardrail_roc": {"enabled": False, "tf": 15, "length": 9, "target": 20.0, "stoploss": -40.0},
    # Session P&L guardrail (points): optional per-day overrides in per_day section
    "guardrail_pnl": {"enabled": False, "target_pts": 100.0, "stoploss_pts": -60.0},
    # Ratio exit: exit when max(CE_ltp, PE_ltp) / min(CE_ltp, PE_ltp) >= threshold
    "ratio_exit":    {"enabled": False, "threshold": 3.0},
    # LTP decay: smart-roll or exit when either leg LTP decays below ltp_exit_min
    "ltp_decay":     {"enabled": False, "ltp_exit_min": 20.0},
    # Smart rolling: scan candidate strikes before rolling on ATM shift
    "smart_rolling_enabled": False,
    # VWAP rise SL: exit when combined VWAP rises >= threshold% above session-low VWAP
    "vwap_rise_sl":  {"enabled": False, "tf": 1, "threshold": 1.0},
    # SL cooldown: after SL hit, block re-entry for (max_entry_rule_tf × multiplier) minutes
    "sl_cooldown_tf_multiplier": 1.0,
    # Capital-based profit target: if > 0, profit_pct is applied to this ₹ amount per day
    "capital_deployed_inr": 0,
    "max_trades": 1,
    "per_day": {
        "monday":    {"enabled": False, "profit_target_pct": 0.0, "loss_sl_pct": 0.0},
        "tuesday":   {"enabled": False, "profit_target_pct": 0.0, "loss_sl_pct": 0.0},
        "wednesday": {"enabled": False, "profit_target_pct": 0.0, "loss_sl_pct": 0.0},
        "thursday":  {"enabled": False, "profit_target_pct": 0.0, "loss_sl_pct": 0.0},
        "friday":    {"enabled": False, "profit_target_pct": 0.0, "loss_sl_pct": 0.0},
    },
}

# ── Per-index iron_condor defaults — matches old repo iron_condor_manager.py ──
# Entry is purely time-gated; NO RSI/ADX filter.
# P&L targets are in ₹, not %; roll side on ratio breach instead of full exit.
_IC_BASE_DEFAULT: Dict[str, Any] = {
    "enabled":                  True,
    "start_time":               "09:16",
    "squareoff_time":           "15:15",
    "entry_day":                "daily",     # daily | monday | monday,thursday
    "product_type":             "MIS",
    "lot_size":                 65,
    "strike_step":              50,
    "max_adjustments_per_side": 3,
    "roll_step_pts":            5,
    "profit_target_inr":        5000.0,      # ₹ profit to exit all 4 legs
    "stoploss_inr":             2000.0,      # ₹ loss to exit all 4 legs
    "ratio_exit_threshold":     3.0,         # short_call_ltp/short_put_ltp ratio to roll
}

_IC_STRIKE_DEFAULTS: Dict[str, Dict[str, float]] = {
    "NIFTY":      {"short_leg_otm_pts": 200.0, "long_leg_otm_pts": 300.0},
    "BANKNIFTY":  {"short_leg_otm_pts": 400.0, "long_leg_otm_pts": 600.0},
    "FINNIFTY":   {"short_leg_otm_pts": 200.0, "long_leg_otm_pts": 300.0},
    "SENSEX":     {"short_leg_otm_pts": 500.0, "long_leg_otm_pts": 750.0},
    "MIDCPNIFTY": {"short_leg_otm_pts": 150.0, "long_leg_otm_pts": 250.0},
    "CRUDEOIL":   {"short_leg_otm_pts": 100.0, "long_leg_otm_pts": 200.0},
}

# MCX commodities trade the evening session — different hours/lots/strikes.
_MCX_INDICES = {"CRUDEOIL", "CRUDEOILM", "NATURALGAS"}
_ALL_INDICES = ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY", "CRUDEOIL"]


def _ss_index_default(index: str) -> Dict[str, Any]:
    base = copy.deepcopy(_SS_INDEX_DEFAULT)
    if index.upper() in _MCX_INDICES:
        # MCX session: start 09:00, NO new trade after 23:15, square off 23:30.
        base.update({"entry_start": "09:00", "entry_end": "23:15", "squareoff_time": "23:30"})
    return base


def _ic_index_default(index: str) -> Dict[str, Any]:
    strikes = _IC_STRIKE_DEFAULTS.get(index, {"short_leg_otm_pts": 200.0, "long_leg_otm_pts": 300.0})
    base = {**_IC_BASE_DEFAULT, **strikes}
    if index.upper() in _MCX_INDICES:
        base.update({"start_time": "09:00", "squareoff_time": "23:30",
                     "strike_step": 100, "lot_size": 100})
    return base


def _build_index_defaults() -> Dict[str, Any]:
    return {
        idx: {
            "sell_straddle": _ss_index_default(idx),
            "iron_condor":   _ic_index_default(idx),
        }
        for idx in _ALL_INDICES
    }

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
    # Legacy flat section — use indices[idx][iron_condor] for per-index config
    "iron_condor": {
        "enabled": True, "start_time": "09:16", "squareoff_time": "15:15",
        "entry_day": "daily", "product_type": "MIS", "lot_size": 65, "strike_step": 50,
        "max_adjustments_per_side": 3, "roll_step_pts": 5,
        "profit_target_inr": 5000.0, "stoploss_inr": 2000.0,
        "ratio_exit_threshold": 3.0,
    },
    "sell_straddle": {
        "entry_start":              "09:20",
        "entry_end":                "15:15",
        "squareoff_time":           "15:15",
        # Per-TRADE exit thresholds (% of credit collected on this trade)
        "profit_pct":               30.0,   # exit this trade when it reaches 30% of its credit
        "sl_pct":                   200.0,  # hard SL: exit when loss = 2× credit
        "trail_lock_pct":           20.0,
        "trail_floor_pct":          10.0,
        # DAY-LEVEL % guardrails (% of initial net credit — fires stop_for_day)
        # 0 = disabled. Resolution: per_day[today] → global → 0 (off)
        "profit_target_pct":        0.0,    # e.g. 12.5 → stop when total day P&L ≥ 12.5% of credit
        "loss_sl_pct":              0.0,    # e.g. 8.0  → stop when total day loss ≥ 8% of credit
        "max_trades":               1,
        "roc_limit_pct":            1.5,
        "ratio_exit_threshold":     3.0,
        "sl_cooldown_tf_multiplier": 1.0,
        "capital_deployed_inr":     0,
        "lot_size":                 50,
        "smart_rolling_enabled":    True,
        "vwap_rise_sl_enabled":     False,
        "vwap_rise_sl_threshold_pct": 1.0,
        "tsl_scalable_enabled":     False,
        "tsl_base_profit_rs":       1000.0,
        "tsl_base_lock_rs":         250.0,
        "tsl_step_profit_rs":       250.0,
        "tsl_step_lock_rs":         250.0,
        "entry_rules_beginning":    [],
        "entry_rules_reentry":      [],
        # Per-day overrides: profit_target_pct and loss_sl_pct for each weekday
        # 0 in either field → fall back to global value above
        "per_day": {
            "monday":    {"enabled": False, "profit_target_pct": 0.0, "loss_sl_pct": 0.0},
            "tuesday":   {"enabled": False, "profit_target_pct": 0.0, "loss_sl_pct": 0.0},
            "wednesday": {"enabled": False, "profit_target_pct": 0.0, "loss_sl_pct": 0.0},
            "thursday":  {"enabled": False, "profit_target_pct": 0.0, "loss_sl_pct": 0.0},
            "friday":    {"enabled": False, "profit_target_pct": 0.0, "loss_sl_pct": 0.0},
        },
    },
    # New per-index config section
    "indices": _build_index_defaults(),
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

    @staticmethod
    def index_section(index: str, strategy: str) -> Dict[str, Any]:
        """Return per-index strategy config, falling back to defaults."""
        _ensure_loaded()
        return copy.deepcopy(
            _live.get("indices", {}).get(index, {}).get(strategy, {})
            or _build_index_defaults().get(index, {}).get(strategy, {})
        )

    @staticmethod
    def get_all_indices() -> Dict[str, Any]:
        """Return the full per-index config block."""
        _ensure_loaded()
        defaults = _build_index_defaults()
        stored = _live.get("indices", {})
        result = {}
        for idx in _ALL_INDICES:
            result[idx] = {
                "sell_straddle": _deep_merge(
                    defaults[idx]["sell_straddle"],
                    stored.get(idx, {}).get("sell_straddle", {}),
                ),
                "iron_condor": _deep_merge(
                    defaults[idx]["iron_condor"],
                    stored.get(idx, {}).get("iron_condor", {}),
                ),
            }
        return result

    @staticmethod
    def set_index_section(index: str, strategy: str, data: Dict[str, Any]) -> None:
        """Persist per-index strategy config.

        MERGES into the existing section (does NOT wholesale-replace), so a partial save
        from a UI form that doesn't round-trip every field (e.g. the entry/exit rule arrays)
        can never WIPE the unsent keys. Fields present in `data` win (lists replaced);
        keys absent from `data` are preserved from the stored config.
        """
        global _live
        _ensure_loaded()
        sect = _live.setdefault("indices", {}).setdefault(index, {})
        existing = sect.get(strategy, {})
        sect[strategy] = _deep_merge(existing, data) if isinstance(existing, dict) else data
        _save_to_disk(_live)
        logger.info("RuntimeConfig: index[%s][%s] saved (merged %d keys).",
                    index, strategy, len(data) if isinstance(data, dict) else 0)


_REQUIRED_SS_KEYS: frozenset[str] = frozenset({"entry_rules_beginning", "exit_rules"})


def validate_index_section(index: str, section: str, raw: dict) -> None:
    """Warn about missing required keys in a strategy config section."""
    for key in _REQUIRED_SS_KEYS:
        if key not in raw:
            logger.warning(
                "RuntimeConfig[%s/%s]: missing expected key '%s' — defaults will apply.",
                index, section, key,
            )

    @staticmethod
    def set_index_config(index: str, data: Dict[str, Any]) -> None:
        """Persist full per-index config (sell_straddle + iron_condor together)."""
        global _live
        _ensure_loaded()
        _live.setdefault("indices", {})[index] = data
        _save_to_disk(_live)
        logger.info("RuntimeConfig: index[%s] full config saved.", index)
