"""
strategies/sell_straddle/config.py — SellStraddleConfig dataclass + loader.

Reads the per-index ``sell_straddle`` section from RuntimeConfig and exposes it as a
typed dataclass.  The engine copies these values onto ``self`` so existing callers can
keep reading ``ss._ltp_target``, ``ss._entry_start``, etc.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, time as dtime
from typing import List

from config.global_config import IST
from data_layer.runtime_config import RuntimeConfig, validate_index_section

logger = logging.getLogger(__name__)


def _parse_time(s: str) -> dtime:
    try:
        h, m = s.split(":")
        return dtime(int(h), int(m))
    except Exception:
        return dtime(15, 15)


@dataclass
class SellStraddleConfig:
    entry_start: dtime
    entry_cutoff: dtime
    force_exit: dtime
    is_crypto: bool
    max_trades: int
    sl_cooldown_minutes: float
    lot_size: int

    trail_sl_enabled: bool
    trail_lock_pct: float
    trail_floor_pct: float
    trail_basis: str

    vwap_rise_enabled: bool
    vwap_rise_threshold: float
    vwap_stale_sec: float

    ratio_threshold: float
    max_entry_ratio: float

    tsl_enabled: bool
    tsl_base_profit_rs: float
    tsl_base_lock_rs: float
    tsl_step_profit_rs: float
    tsl_step_lock_rs: float
    tsl_basis: str

    day_profit_target_pct: float
    day_loss_sl_pct: float
    day_exit_basis: str

    ltp_target: float
    entry_basis: str
    theta_target: float

    ltp_decay_enabled: bool
    ltp_exit_min: float

    exit_rules: List[dict]

    guardrail_pnl_enabled: bool
    guardrail_pnl_target_pts: float
    guardrail_pnl_sl_pts: float

    guardrail_roc_enabled: bool
    guardrail_roc_tf: int
    guardrail_roc_length: int
    guardrail_roc_target: float
    guardrail_roc_stoploss: float


def load_sell_straddle_config(underlying: str, cfg) -> SellStraddleConfig:
    """Load and validate the sell_straddle section for ``underlying``."""
    ss = RuntimeConfig.index_section(underlying, "sell_straddle")
    if not ss:
        logger.warning(
            "SellStraddle[%s]: 'sell_straddle' config section missing from runtime config — using defaults.",
            underlying,
        )
    validate_index_section(underlying, "sell_straddle", ss)

    def _cfg(key: str, default):
        """Dot-notation config reader."""
        parts = key.split(".")
        node = ss
        for part in parts:
            if not isinstance(node, dict):
                return default
            node = node.get(part)
            if node is None:
                return default
        return node if node is not None else default

    entry_start = _parse_time(ss.get("entry_start", "09:20"))
    entry_cutoff = _parse_time(ss.get("entry_end", "15:15"))
    force_exit = _parse_time(ss.get("squareoff_time", "15:15"))
    is_crypto = bool(cfg and cfg.exchange.is_crypto(underlying))
    max_trades = int(ss.get("max_trades", 1))
    sl_cooldown_minutes = float(
        ss.get("sl_cooldown_minutes", ss.get("sl_cooldown_tf_multiplier", 1.0) * 5.0)
    )
    _exch_lots = cfg.exchange.lot_sizes if cfg else {}
    lot_size = int(_exch_lots.get(underlying, ss.get("lot_size", 50)))

    trail_sl_enabled = bool(ss.get("tsl_enabled", True))
    trail_lock_pct = float(ss.get("trail_lock_pct", 20.0)) / 100.0
    trail_floor_pct = float(ss.get("trail_floor_pct", 10.0)) / 100.0
    trail_basis = str(ss.get("trail_basis", "ltp")).lower()

    _vwap_sl = ss.get("vwap_rise_sl", {})
    vwap_rise_enabled = bool(_vwap_sl.get("enabled", ss.get("vwap_rise_sl_enabled", False)))
    vwap_rise_threshold = float(_vwap_sl.get("threshold", ss.get("vwap_rise_sl_threshold_pct", 1.0)))
    _stale_default = 150.0 if str(underlying).upper() in ("CRUDEOIL", "NATURALGAS", "GOLD", "GOLDM", "SILVER") else 90.0
    vwap_stale_sec = float(_vwap_sl.get("stale_sec", ss.get("vwap_stale_sec", _stale_default)))

    _ratio = ss.get("ratio_exit", {})
    ratio_threshold = float(_ratio.get("threshold", ss.get("ratio_exit_threshold", 3.0)))
    max_entry_ratio = float(_ratio.get("max_entry_ratio", ss.get("max_entry_ratio", 0.0)))

    _tsl = ss.get("tsl_scalable", {})
    tsl_enabled = bool(_tsl.get("enabled", ss.get("tsl_scalable_enabled", False)))
    tsl_base_profit_rs = float(_tsl.get("base_profit", ss.get("tsl_base_profit_rs", 1000.0)))
    tsl_base_lock_rs = float(_tsl.get("base_lock", ss.get("tsl_base_lock_rs", 250.0)))
    tsl_step_profit_rs = float(_tsl.get("step_profit", ss.get("tsl_step_profit_rs", 250.0)))
    tsl_step_lock_rs = float(_tsl.get("step_lock", ss.get("tsl_step_lock_rs", 250.0)))
    tsl_basis = str(_tsl.get("basis", ss.get("tsl_basis", "ltp"))).lower()

    now_day = datetime.now(IST).strftime("%A").lower()
    _day = ss.get("per_day", {}).get(now_day, {})
    _day_on = bool(_day.get("enabled", True))
    _pt = float(_day.get("profit_target_pct", 0)) if _day_on else 0.0
    day_profit_target_pct = _pt if _pt > 0 else float(ss.get("profit_target_pct", 0))
    _ls = float(_day.get("loss_sl_pct", 0)) if _day_on else 0.0
    day_loss_sl_pct = _ls if _ls > 0 else float(ss.get("loss_sl_pct", 0))
    day_exit_basis = str(_day.get("exit_basis", ss.get("exit_basis", "ltp"))).lower()

    ltp_target = float(ss.get("ltp_target") or ss.get("min_ltp") or ss.get("ltp_min") or 0.0)
    entry_basis = str(_day.get("entry_basis", ss.get("entry_basis", "ltp"))).lower()
    theta_target = float(_day.get("theta_target", ss.get("theta_target") or ss.get("entry_theta_target") or 0.0))

    _ltp_d = ss.get("ltp_decay", {})
    ltp_decay_enabled = bool(_ltp_d.get("enabled", ss.get("ltp_decay_enabled", False)))
    ltp_exit_min = float(_ltp_d.get("ltp_exit_min", ss.get("ltp_exit_min", 20.0)))

    exit_rules = ss.get("exit_rules", [])

    _pnl_g = ss.get("guardrail_pnl", {})
    guardrail_pnl_enabled = bool(_pnl_g.get("enabled", False))
    guardrail_pnl_target_pts = float(_pnl_g.get("target_pts", 0.0))
    guardrail_pnl_sl_pts = float(_pnl_g.get("stoploss_pts", 0.0))

    _roc_g = ss.get("guardrail_roc", {})
    guardrail_roc_enabled = bool(_roc_g.get("enabled", False))
    guardrail_roc_tf = int(_roc_g.get("tf", 15))
    guardrail_roc_length = int(_roc_g.get("length", 9))
    guardrail_roc_target = float(_roc_g.get("target", -20.0))
    guardrail_roc_stoploss = float(_roc_g.get("stoploss", 10.0))

    return SellStraddleConfig(
        entry_start=entry_start,
        entry_cutoff=entry_cutoff,
        force_exit=force_exit,
        is_crypto=is_crypto,
        max_trades=max_trades,
        sl_cooldown_minutes=sl_cooldown_minutes,
        lot_size=lot_size,
        trail_sl_enabled=trail_sl_enabled,
        trail_lock_pct=trail_lock_pct,
        trail_floor_pct=trail_floor_pct,
        trail_basis=trail_basis,
        vwap_rise_enabled=vwap_rise_enabled,
        vwap_rise_threshold=vwap_rise_threshold,
        vwap_stale_sec=vwap_stale_sec,
        ratio_threshold=ratio_threshold,
        max_entry_ratio=max_entry_ratio,
        tsl_enabled=tsl_enabled,
        tsl_base_profit_rs=tsl_base_profit_rs,
        tsl_base_lock_rs=tsl_base_lock_rs,
        tsl_step_profit_rs=tsl_step_profit_rs,
        tsl_step_lock_rs=tsl_step_lock_rs,
        tsl_basis=tsl_basis,
        day_profit_target_pct=day_profit_target_pct,
        day_loss_sl_pct=day_loss_sl_pct,
        day_exit_basis=day_exit_basis,
        ltp_target=ltp_target,
        entry_basis=entry_basis,
        theta_target=theta_target,
        ltp_decay_enabled=ltp_decay_enabled,
        ltp_exit_min=ltp_exit_min,
        exit_rules=exit_rules,
        guardrail_pnl_enabled=guardrail_pnl_enabled,
        guardrail_pnl_target_pts=guardrail_pnl_target_pts,
        guardrail_pnl_sl_pts=guardrail_pnl_sl_pts,
        guardrail_roc_enabled=guardrail_roc_enabled,
        guardrail_roc_tf=guardrail_roc_tf,
        guardrail_roc_length=guardrail_roc_length,
        guardrail_roc_target=guardrail_roc_target,
        guardrail_roc_stoploss=guardrail_roc_stoploss,
    )


class ConfigMixin:
    """Provides ``_load_thresholds`` / ``reconfigure`` for the sell-straddle engine."""

    def _load_thresholds(self) -> None:
        cfg = load_sell_straddle_config(self._underlying, self._cfg)
        self._config = cfg
        self._entry_start = cfg.entry_start
        self._entry_cutoff = cfg.entry_cutoff
        self._force_exit = cfg.force_exit
        self._is_crypto = cfg.is_crypto
        self._max_trades = cfg.max_trades
        self._sl_cooldown_minutes = cfg.sl_cooldown_minutes
        self._lot_size = cfg.lot_size

        self._trail_sl_enabled = cfg.trail_sl_enabled
        self._trail_lock_pct = cfg.trail_lock_pct
        self._trail_floor_pct = cfg.trail_floor_pct
        self._trail_basis = cfg.trail_basis

        self._vwap_rise_enabled = cfg.vwap_rise_enabled
        self._vwap_rise_threshold = cfg.vwap_rise_threshold
        self._vwap_stale_sec = cfg.vwap_stale_sec

        self._ratio_threshold = cfg.ratio_threshold
        self._max_entry_ratio = cfg.max_entry_ratio

        self._tsl_enabled = cfg.tsl_enabled
        self._tsl_base_profit_rs = cfg.tsl_base_profit_rs
        self._tsl_base_lock_rs = cfg.tsl_base_lock_rs
        self._tsl_step_profit_rs = cfg.tsl_step_profit_rs
        self._tsl_step_lock_rs = cfg.tsl_step_lock_rs
        self._tsl_basis = cfg.tsl_basis

        self._day_profit_target_pct = cfg.day_profit_target_pct
        self._day_loss_sl_pct = cfg.day_loss_sl_pct
        self._day_exit_basis = cfg.day_exit_basis

        self._ltp_target = cfg.ltp_target
        self._entry_basis = cfg.entry_basis
        self._theta_target = cfg.theta_target

        self._ltp_decay_enabled = cfg.ltp_decay_enabled
        self._ltp_exit_min = cfg.ltp_exit_min

        self._exit_rules = cfg.exit_rules

        self._guardrail_pnl_enabled = cfg.guardrail_pnl_enabled
        self._guardrail_pnl_target_pts = cfg.guardrail_pnl_target_pts
        self._guardrail_pnl_sl_pts = cfg.guardrail_pnl_sl_pts

        self._guardrail_roc_enabled = cfg.guardrail_roc_enabled
        self._guardrail_roc_tf = cfg.guardrail_roc_tf
        self._guardrail_roc_length = cfg.guardrail_roc_length
        self._guardrail_roc_target = cfg.guardrail_roc_target
        self._guardrail_roc_stoploss = cfg.guardrail_roc_stoploss

    def reconfigure(self) -> None:
        self._load_thresholds()
        logger.info("SellStraddle[%s]: reconfigured.", self._underlying)
