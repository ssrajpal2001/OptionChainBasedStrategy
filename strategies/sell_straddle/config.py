"""
strategies/sell_straddle/config.py — SellStraddleConfig dataclass + loader.

Reads the per-index ``sell_straddle`` section from RuntimeConfig and exposes it as a
typed dataclass.  The engine copies these values onto ``self`` so existing callers can
keep reading ``ss._ltp_target``, ``ss._entry_start``, etc.

Risk-management values are loaded from the admin RuntimeConfig first, then optionally
overridden by per-client settings in ``ClientProfile.strategy_risk_overrides``.  A
missing/null client override falls back to the admin default.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, fields
from datetime import datetime, time as dtime
from typing import Any, Dict, List, Optional

from config.global_config import IST
from data_layer.runtime_config import RuntimeConfig, validate_index_section

logger = logging.getLogger(__name__)


def _parse_time(s: str) -> dtime:
    try:
        h, m = s.split(":")
        return dtime(int(h), int(m))
    except Exception:
        return dtime(15, 20)


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
    balance_ratio: float

    ltp_decay_enabled: bool
    ltp_exit_min: float

    exit_rules: List[dict]

    itm_pair_gate_enabled: bool
    itm_pair_gate_profit_inr: float
    itm_pair_gate_min_strike_gap: float

    day_low_exit_enabled: bool
    day_low_freeze_time: dtime

    post1500_exit_enabled: bool
    shadow_vwap_enabled: bool
    vwap_source: str

    same_day_expiry_enabled: bool

    hedge_carry_enabled: bool


def _apply_client_overrides(
    cfg: SellStraddleConfig,
    overrides: Dict[str, Any],
    client_id: str,
    underlying: str,
) -> None:
    """Apply client-provided risk overrides onto the admin config.

    Only fields that are explicitly present and non-None in ``overrides`` are changed.
    Time fields may be sent as ``"HH:MM"`` strings and are parsed.
    """
    if not overrides:
        return

    _time_fields = {"entry_start", "entry_cutoff", "force_exit", "day_low_freeze_time"}
    applied: List[str] = []

    for field in fields(cfg):
        key = field.name
        if key not in overrides:
            continue
        raw = overrides[key]
        if raw is None:
            continue

        try:
            if key in _time_fields and isinstance(raw, str):
                value = _parse_time(raw)
            else:
                # Let Python coerce the value; dataclass type hints are not enforced
                # at runtime, so a bool/int/float/list/string will simply replace the
                # admin value.
                value = raw
            setattr(cfg, key, value)
            applied.append(key)
        except Exception as exc:
            logger.warning(
                "SellStraddle[%s|%s]: ignoring invalid client override %s=%r: %s",
                underlying, client_id, key, raw, exc,
            )

    if applied:
        logger.info(
            "SellStraddle[%s|%s]: client risk overrides applied: %s",
            underlying, client_id, ", ".join(applied),
        )


def load_sell_straddle_config(
    underlying: str,
    cfg,
    client_id: str = "",
) -> SellStraddleConfig:
    """Load and validate the sell_straddle section for ``underlying``.

    If ``client_id`` is provided and the client profile contains
    ``strategy_risk_overrides["sell_straddle:<underlying>"]``, those values override
    the admin RuntimeConfig values.
    """
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
    entry_cutoff = _parse_time(ss.get("entry_end", "15:20"))
    force_exit = _parse_time(ss.get("squareoff_time", "15:20"))
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
    balance_ratio = float(ss.get("balance_ratio", 1.0))

    _ltp_d = ss.get("ltp_decay", {})
    ltp_decay_enabled = bool(_ltp_d.get("enabled", ss.get("ltp_decay_enabled", False)))
    ltp_exit_min = float(_ltp_d.get("ltp_exit_min", ss.get("ltp_exit_min", 20.0)))

    exit_rules = ss.get("exit_rules", [])

    itm_pair_gate_enabled = bool(ss.get("itm_pair_gate_enabled", True))
    itm_pair_gate_profit_inr = float(ss.get("itm_pair_gate_profit_inr", 500.0))
    itm_pair_gate_min_strike_gap = float(ss.get("itm_pair_gate_min_strike_gap", 100.0))

    # Day-low reversal exit (2026-08-18, user spec): the straddle's combined
    # premium tends to bottom out somewhere in the 09:15-15:00 window then
    # reverse upward into the close. Track the day's running-min combined
    # premium from entry up to a freeze point (default 15:00 IST); from then
    # until squareoff, exit the whole position the moment the current combined
    # premium reaches that frozen low again (including the freeze tick itself,
    # if that reading happens to BE the day's low). Opt-in (default OFF) --
    # unlike itm_pair_gate this is brand new and unvalidated; must not silently
    # activate on an existing live deployment.
    day_low_exit_enabled = bool(ss.get("day_low_exit_enabled", False))
    day_low_freeze_time = _parse_time(ss.get("day_low_freeze_time", "15:00"))

    # Post-15:00 per-leg R1 exit (2026-08-28, direct user spec): replaces the
    # old day-low-reversal-exit's own action ("close both legs") for
    # bindings that opt into this instead -- the day-low condition (and a
    # 15:15 profit check) now only ARMS a per-leg R1/S1 watch; each leg is
    # then closed independently the moment its OWN R1 breaches, never both
    # together. See exits.py's own _check_post1500_r1_exit docstring for the
    # full state machine. Opt-in, default OFF -- must not silently change
    # behavior for an existing live deployment (e.g. day_low_exit_enabled
    # stays fully intact/unchanged for anyone not opting into this).
    post1500_exit_enabled = bool(ss.get("post1500_exit_enabled", True))

    # Shadow VWAP (2026-08-28, direct user spec): runs a second, self-computed
    # VWAP in parallel with the live broker-ATP VWAP that actually drives every
    # decision -- purely logged for after-market comparison, NEVER read by any
    # decision path. Opt-in, default OFF.
    shadow_vwap_enabled = bool(ss.get("shadow_vwap_enabled", True))

    # VWAP source (2026-09-03, direct user spec): which VWAP actually drives
    # every entry/exit/roll decision. "broker_atp" (default, unchanged
    # behavior for every existing deployment) feeds the pool engine the
    # broker's own live ATP. "calculative" feeds it the same self-computed
    # cumulative VWAP the shadow-VWAP feature already calculates (see
    # _update_shadow_vwap) -- lets one binding run live on the calculative
    # VWAP for direct paper-trading comparison against a sibling binding
    # left on broker_atp, same underlying, same day.
    vwap_source = str(ss.get("vwap_source", "broker_atp")).lower()
    if vwap_source not in ("broker_atp", "calculative"):
        vwap_source = "broker_atp"

    same_day_expiry_enabled = bool(ss.get("same_day_expiry_enabled", False))

    # EOD hedge-and-carry (2026-08-20, user spec): if BOTH sold legs are running in
    # loss at close-of-day (and it isn't T-1 from expiry), buy a protective leg on
    # each side (further OTM, LTP <=50% of the running sold leg) instead of the
    # normal EOD square-off, and carry the whole 4-leg position forward as a
    # positional (NRML) trade. Opt-in, default OFF -- brand new, must not silently
    # activate on an existing live deployment. Requires product_type=NRML on this
    # binding to actually survive the broker's own overnight square-off.
    hedge_carry_enabled = bool(ss.get("hedge_carry_enabled", False))

    config = SellStraddleConfig(
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
        balance_ratio=balance_ratio,
        ltp_decay_enabled=ltp_decay_enabled,
        ltp_exit_min=ltp_exit_min,
        exit_rules=exit_rules,
        itm_pair_gate_enabled=itm_pair_gate_enabled,
        itm_pair_gate_profit_inr=itm_pair_gate_profit_inr,
        itm_pair_gate_min_strike_gap=itm_pair_gate_min_strike_gap,
        day_low_exit_enabled=day_low_exit_enabled,
        day_low_freeze_time=day_low_freeze_time,
        post1500_exit_enabled=post1500_exit_enabled,
        shadow_vwap_enabled=shadow_vwap_enabled,
        vwap_source=vwap_source,
        same_day_expiry_enabled=same_day_expiry_enabled,
        hedge_carry_enabled=hedge_carry_enabled,
    )

    # Apply per-client risk overrides if a client_id is provided.
    if client_id:
        try:
            from config.client_profiles import REGISTRY
            profile = REGISTRY.get(client_id)
            if profile is not None:
                overrides = getattr(profile, "strategy_risk_overrides", {}).get(
                    f"sell_straddle:{underlying}", {}
                )
                _apply_client_overrides(config, overrides, client_id, underlying)
        except Exception as exc:
            logger.warning(
                "SellStraddle[%s|%s]: failed to apply client risk overrides: %s",
                underlying, client_id, exc,
            )

    return config


class ConfigMixin:
    """Provides ``_load_thresholds`` / ``reconfigure`` for the sell-straddle engine."""

    def _load_thresholds(self) -> None:
        client_id = getattr(self, "_client_id", "")
        cfg = load_sell_straddle_config(self._underlying, self._cfg, client_id=client_id)
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
        self._balance_ratio = cfg.balance_ratio

        self._ltp_decay_enabled = cfg.ltp_decay_enabled
        self._ltp_exit_min = cfg.ltp_exit_min

        self._exit_rules = cfg.exit_rules

        self._itm_pair_gate_enabled = cfg.itm_pair_gate_enabled
        self._itm_pair_gate_profit_inr = cfg.itm_pair_gate_profit_inr
        self._itm_pair_gate_min_strike_gap = cfg.itm_pair_gate_min_strike_gap
        if not hasattr(self, "_itm_roll_protection") or not isinstance(self._itm_roll_protection, dict):
            # Keyed by side ("CE"/"PE") -- each side's 70%-of-booked-profit budget is
            # tracked independently. A rollover on one side must never wipe out a
            # still-active budget already armed on the other side.
            self._itm_roll_protection = {}

        self._day_low_exit_enabled = cfg.day_low_exit_enabled
        self._day_low_freeze_time = cfg.day_low_freeze_time
        if not hasattr(self, "_session_min_straddle_frozen"):
            self._session_min_straddle_frozen = None

        self._post1500_exit_enabled = cfg.post1500_exit_enabled
        self._shadow_vwap_enabled = cfg.shadow_vwap_enabled
        # A per-deployment vwap_source_override (see SellStraddleStrategy.__init__)
        # wins permanently over the admin/client-level RuntimeConfig value -- this
        # method re-runs on every periodic config reload, so without this check a
        # reload would silently clobber the override back to the shared default.
        if getattr(self, "_vwap_source_override", None):
            self._vwap_source = self._vwap_source_override
        else:
            self._vwap_source = cfg.vwap_source
        if not hasattr(self, "_post1500_pair"):
            self._post1500_pair = None
            self._post1500_armed = False
            self._post1500_armed_reason = None
            self._post1500_leg_closed = {"CE": False, "PE": False}
            # 2026-09-03 CRITICAL FIX: in-flight guard against a real duplicate
            # close order. _close_leg's own await (order placement -> broker
            # confirmation) takes over a second; ce_leg_closed/pe_leg_closed on
            # the position only flip True AFTER that await returns. A second
            # exit-check tick landing during that window (confirmed live: two
            # ticks 138ms apart, same R1.high, same ltp, both fired a real
            # close -> 2 broker orders for the same leg) saw the leg as still
            # open and fired again. This flag is set True BEFORE the await
            # (not after) so a concurrent re-entry sees the leg as already
            # being closed and skips; cleared only if the close aborts, so a
            # genuine retry after a broker-confirm timeout is still possible.
            self._post1500_closing = {"CE": False, "PE": False}
            self._post1500_calc = {}       # side -> SupportResistanceCalculator
            self._post1500_bar_acc = {}    # side -> {"minute": datetime, "h":, "l":, "c":}
        if not hasattr(self, "_shadow_vwap"):
            self._shadow_vwap = {}         # (strike, side) -> {"cum_pv":, "cum_v":, "last":}
        if not hasattr(self, "_shadow_vwap_seeding"):
            self._shadow_vwap_seeding = set()   # (strike, side) keys with a REST seed task in flight
        if not hasattr(self, "_day_low_tracked_pair"):
            self._day_low_tracked_pair = None
        if not hasattr(self, "_day_low_computing"):
            self._day_low_computing = False

        self._same_day_expiry_enabled = cfg.same_day_expiry_enabled

        self._hedge_carry_enabled = cfg.hedge_carry_enabled

    def reconfigure(self) -> None:
        self._load_thresholds()
        logger.info("SellStraddle[%s]: reconfigured.", self._underlying)
