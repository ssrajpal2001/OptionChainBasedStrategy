"""
strategies/sell_straddle.py — ATM Straddle selling strategy.

Mirrors Option_Selling_May_2026 sell_v3 logic exactly.

PRIMING WAIT:
  wait_minutes = max_rule_tf × 2  (if any rule uses SLOPE/VWAP_SLOPE)
               = max_rule_tf × 1  (otherwise)
  No entry evaluation until market_open + wait_minutes has elapsed.
  This matches base.py _is_in_priming_wait() exactly.

ENTRY MODES:
  BEGINNING — first trade of the session, uses entry_rules_beginning
  RE-ENTRY  — after any close (profit/SL/ratio etc), uses entry_rules_reentry

ENTRY LOGIC:
  Evaluate configured rules against live computed indicators.
  Rules control everything — no hardcoded thresholds.

EXIT CONDITIONS:
  1. Profit target  — net_premium × profit_pct  OR capital-based ₹ target
  2. Stop loss      — net_premium × sl_pct
  3. Scalable TSL   — per-lot rupee staircase lock (base_lock + N × step_lock)
  4. VWAP Rise SL   — combined VWAP rises > threshold% above session low
  5. Ratio exit     — max(CE,PE) LTP / min(CE,PE) LTP ≥ threshold
  6. ROC guardrail  — spot moves > roc_limit_pct% in one tick
  7. Time exit      — squareoff_time IST

SMART ROLLING (on profit target / ratio exit):
  1. Evaluate entry_rules_reentry against current indicators
  2. If rules PASS on SAME strikes → Virtual Roll (refresh entry prices, keep position)
  3. If rules PASS on DIFFERENT strikes → Physical Roll (new ATM, new position)
  4. If rules FAIL → plain close, wait for next regular entry window
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np

from config.global_config import IST, Topic
from data_layer.base_feeder import EventBus, CandleEvent
from data_layer.runtime_config import RuntimeConfig
from matrix_engine.indicators import rsi, vwap, adx, ema

import os

logger = logging.getLogger(__name__)

_BUF             = 600    # ring-buffer depth ≥ VWAP_WINDOW(500)
_MARKET_OPEN     = dtime(9, 15)   # NSE session start


def _make_strategy_logger(underlying: str, client_id: str = "", binding_id: str = "") -> logging.Logger:
    from utils.logging_utils import make_strategy_logger
    from datetime import datetime
    tag = f"{underlying}" + (f"_{client_id}_{binding_id}" if client_id and binding_id else "")
    date_str = datetime.now().strftime("%Y%m%d")
    return make_strategy_logger(f"ss_{tag}_{date_str}", propagate=False)


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class StraddleLeg:
    option_type: str
    strike: float
    entry_price: float
    ltp: float = 0.0
    mark: float = 0.0          # broker mark/ATP (fair value) — used for crypto P&L display (LTP is noisy)
    open_time: Optional[datetime] = None
    close_time: Optional[datetime] = None
    open_reason: str = ""
    symbol: str = ""            # full broker symbol e.g. C-BTC-64000-140626 or NIFTY24600CE


@dataclass
class StraddlePosition:
    underlying: str
    atm_at_entry: float
    entry_spot: float
    ce_leg: StraddleLeg = field(default_factory=lambda: StraddleLeg("CE", 0, 0))
    pe_leg: StraddleLeg = field(default_factory=lambda: StraddleLeg("PE", 0, 0))

    net_credit: float = 0.0       # CE_entry + PE_entry at open
    tsl_high_lock_rs: float = 0.0  # Highest scalable TSL lock reached in ₹
    peak_profit: float = 0.0       # Highest unrealized P&L seen (for trailing SL)
    trailing_active: bool = False  # True once profit crossed trail_lock threshold

    open_time: Optional[datetime] = None
    close_time: Optional[datetime] = None
    close_reason: str = ""
    realized_pnl: float = 0.0
    status: str = "open"           # "open" | "closed"

    entry_indicators: Dict[str, float] = field(default_factory=dict)

    # Session VWAP tracking for VWAP Rise SL
    session_min_vwap: float = float("inf")

    # Trailing-SL (lock-%/floor-%) peak profit % since entry (basis ltp or theta). Highest
    # profit% seen; once it crosses lock%, exit when profit drops floor% below this peak.
    trail_peak_pct: float = 0.0

    # Last ACCEPTED combined VWAP (dropout filter for vwap_rise): a sudden crater vs this (one
    # leg's ATP dropping out) is rejected so it can't poison session_min_vwap → false vwap_rise.
    vwap_last_good: float = 0.0

    # Day-wise THETA exit: combined option TIME VALUE (extrinsic) captured at entry. The
    # theta-based day exit measures how far the live combined time value has decayed from this.
    entry_time_value: float = 0.0

    # Total contracts per leg (lot_size × lot_multiplier) — used by the dashboard
    # to render qty and rupee P&L. Without it the UI shows qty=0 → P&L always 0.
    lot_size: int = 0

    def to_dict(self) -> dict:
        """JSON-serialisable snapshot for PositionStore."""
        def _leg(l: StraddleLeg) -> dict:
            return {"option_type": l.option_type, "strike": l.strike,
                    "entry_price": l.entry_price, "ltp": l.ltp,
                    "open_time": l.open_time.isoformat() if l.open_time else None,
                    "close_time": l.close_time.isoformat() if l.close_time else None,
                    "open_reason": l.open_reason}
        return {
            "underlying": self.underlying, "atm_at_entry": self.atm_at_entry,
            "entry_spot": self.entry_spot,
            "ce_leg": _leg(self.ce_leg), "pe_leg": _leg(self.pe_leg),
            "net_credit": self.net_credit, "tsl_high_lock_rs": self.tsl_high_lock_rs,
            "peak_profit": self.peak_profit, "trailing_active": self.trailing_active,
            "open_time": self.open_time.isoformat() if self.open_time else None,
            "realized_pnl": self.realized_pnl, "status": self.status,
            "entry_indicators": dict(self.entry_indicators),
            "lot_size": self.lot_size,
            "entry_time_value": self.entry_time_value,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "StraddlePosition":
        from datetime import datetime as _dt
        def _leg(x: dict) -> StraddleLeg:
            return StraddleLeg(option_type=x["option_type"], strike=x["strike"],
                               entry_price=x["entry_price"], ltp=x.get("ltp", 0.0),
                               open_time=_dt.fromisoformat(x["open_time"]) if x.get("open_time") else None,
                               close_time=_dt.fromisoformat(x["close_time"]) if x.get("close_time") else None,
                               open_reason=x.get("open_reason", ""))
        return cls(
            underlying=d["underlying"], atm_at_entry=d.get("atm_at_entry", 0.0),
            entry_spot=d.get("entry_spot", 0.0),
            ce_leg=_leg(d["ce_leg"]), pe_leg=_leg(d["pe_leg"]),
            net_credit=d.get("net_credit", 0.0), tsl_high_lock_rs=d.get("tsl_high_lock_rs", 0.0),
            peak_profit=d.get("peak_profit", 0.0), trailing_active=d.get("trailing_active", False),
            open_time=_dt.fromisoformat(d["open_time"]) if d.get("open_time") else None,
            realized_pnl=d.get("realized_pnl", 0.0), status=d.get("status", "open"),
            entry_indicators=dict(d.get("entry_indicators", {})),
            lot_size=d.get("lot_size", 0),
            entry_time_value=d.get("entry_time_value", 0.0),
        )

    @property
    def current_value(self) -> float:
        return self.ce_leg.ltp + self.pe_leg.ltp

    @property
    def unrealized_pnl(self) -> float:
        return self.net_credit - self.current_value

    def current_time_value(self, spot: float) -> float:
        """Live combined option time value (extrinsic) at the given spot — for theta-based exit."""
        from strategies.theta_calc import combined_time_value
        return combined_time_value(self.ce_leg.strike, self.pe_leg.strike, spot,
                                   self.ce_leg.ltp, self.pe_leg.ltp)

    def theta_decay_pct(self, spot: float) -> float:
        """Signed % the combined time value has decayed since entry (positive = profit)."""
        from strategies.theta_calc import theta_decay_pct as _tdp
        return _tdp(self.entry_time_value, self.current_time_value(spot))

    def premium_decay_pct(self) -> float:
        """CLEAN theta% (user spec 2026-06-10): the decay tracked against the TOTAL THETA
        RECEIVED AT ENTRY. = (entry premium − current premium) / entry_time_value × 100, where
        entry_time_value is the combined TIME VALUE captured at entry ('total theta received';
        for an ATM straddle it equals the entry premium). The numerator is the premium decay
        (= running P&L in pts). Because the denominator is fixed at entry, the absolute profit/SL
        thresholds (entry_theta × day%) are known at the start. Positive = decayed = profit; it
        tracks P&L and rises cleanly (no spot-driven oscillation)."""
        base = float(getattr(self, "entry_time_value", 0.0) or 0.0) or float(self.net_credit or 0.0)
        if base <= 0:
            return 0.0
        return (self.net_credit - self.current_value) / base * 100.0


def format_exit_eval(underlying: str, pnl_pts: float, credit: float, criteria) -> str:
    """One EXIT-EVAL log line showing every exit criterion checked on the max-TF close.
    `criteria`: list of (name, detail, hit:bool). Shows current-vs-threshold + ✓/✗ per
    criterion and the overall HOLD/EXIT outcome — mirrors the entry EVAL line."""
    parts, fired = [], []
    for name, detail, hit in criteria:
        parts.append(f"{name}({detail})={'✓HIT' if hit else '✗'}")
        if hit:
            fired.append(name)
    pct = (pnl_pts / credit * 100.0) if credit else 0.0
    outcome = ("EXIT:" + ",".join(fired)) if fired else "HOLD"
    return (f"EXIT-EVAL {underlying} pnl={pnl_pts:.2f} ({pct:.1f}% of credit) | "
            + " | ".join(parts) + f" → {outcome}")


# ── Strategy ──────────────────────────────────────────────────────────────────

class SellStraddleStrategy:

    def __init__(
        self,
        bus: EventBus,
        cfg=None,
        underlying: str = "NIFTY",
        lot_multiplier: int = 1,
        client_id: str = "",
        binding_id: str = "",
    ) -> None:
        self._bus            = bus
        self._cfg            = cfg
        self._underlying     = underlying
        self._lot_multiplier = lot_multiplier
        self._running        = False
        self._client_db      = None
        # Per-binding refactor: when set, this is an INDEPENDENT book for exactly one
        # (client, broker-binding). It tags its orders so the bridge routes to only that broker,
        # gates on only that binding's Terminal+Trade, and persists under a per-binding key.
        # Empty = legacy per-index engine (mirrors to all eligible brokers).
        self._client_id      = client_id
        self._binding_id     = binding_id

        self._position: Optional[StraddlePosition] = None
        self._trades_today: int = 0

        self._spot: float      = 0.0
        self._ce_ltp: float    = 0.0
        self._pe_ltp: float    = 0.0
        # Broker ATP (exchange VWAP) for the ATM legs — VWAP is NEVER computed,
        # it comes from the feed. Combined VWAP = CE ATP + PE ATP.
        self._ce_atp: float    = 0.0
        self._pe_atp: float    = 0.0
        self._prev_vwap_atp: Optional[float] = None   # previous closed-candle combined VWAP
        # Per-strike feed cache for balanced-pair selection (all subscribed strikes).
        # Key = (int strike, "CE"/"PE") -> {"ltp": float, "atp": float}.
        self._strike_prem: Dict[Tuple[int, str], dict] = {}
        # Previous closed-candle ATP per leg, for per-pair VWAP slope.
        self._prev_atp_closed: Dict[Tuple[int, str], float] = {}
        # Hybrid workflow: set when the beginning concept's pair fails its gate,
        # routes the next pulse to the pool scan even while trades_today == 0.
        self._beginning_failed: bool = False
        self._ltp_target: float = 0.0

        # Trailing SL — enable toggle + thresholds
        self._trail_sl_enabled: bool  = True
        self._trail_lock_pct:   float = 0.20   # default 20% of credit
        self._trail_floor_pct:  float = 0.10   # default 10% below peak

        self._ltp_decay_enabled: bool  = False
        self._ltp_exit_min:      float = 20.0

        self._exit_rules:            list = []
        self._last_exit_rules_bucket: str  = ""
        self._last_entry_bucket_b:    str  = ""   # beginning rule-set max-tf bucket
        self._last_entry_bucket_r:    str  = ""   # reentry rule-set max-tf bucket

        self._guardrail_pnl_enabled:    bool  = False
        self._guardrail_pnl_target_pts: float = 0.0
        self._guardrail_pnl_sl_pts:     float = 0.0

        self._guardrail_roc_enabled:  bool  = False
        self._guardrail_roc_tf:       int   = 15
        self._guardrail_roc_length:   int   = 9
        self._guardrail_roc_target:   float = -20.0
        self._guardrail_roc_stoploss: float = 10.0
        self._last_roc_guard_bucket:  str   = ""

        # Market-open timestamp for this session (set on first candle of the day)
        self._market_open_dt: Optional[datetime] = None
        self._primed: bool = False        # True once priming wait is over
        self._order_pending: bool = False  # True between publish and fill confirmation

        # Day-level P&L tracking (mirrors old sell_v3 session guardrail logic)
        self._session_realized_pnl_pts: float = 0.0   # sum of all closed trade P&L today (in premium pts)
        self._initial_net_credit: float = 0.0         # credit from first trade — LTP denominator for day %
        self._initial_entry_time_value: float = 0.0   # theta (time-value) from first trade — theta denominator
        self._stop_for_day: bool = False               # True after day-profit-target or day-loss-SL fires

        # Post-restore warm-up: hold P&L exits until a restored position's legs tick fresh.
        self._post_restore_warmup: bool = False
        self._post_restore_at:     float = 0.0
        self._ce_ltp_fresh:        bool = True
        self._pe_ltp_fresh:        bool = True

        self._tasks: list = []
        self._sl_cooldown_until: Optional[datetime] = None
        self._event_counter: int = 0

        # Combined CE+PE premium candle buffer
        self._prem_closes:  deque = deque(maxlen=_BUF)
        self._prem_volumes: deque = deque(maxlen=_BUF)
        # Timestamped 1-min chart series (combined premium + VWAP/RSI/SLOPE) for the
        # client-side chart endpoint. One point per 1-min candle close.
        self._chart_series: deque = deque(maxlen=375)   # ~one full trading day of 1-min bars

        from strategies.pool_indicator_engine import PoolIndicatorEngine
        self._pool_engine = PoolIndicatorEngine(rsi_len=14, roc_len=10)

        # Index candle buffer for ADX
        self._idx_highs:  deque = deque(maxlen=_BUF)
        self._idx_lows:   deque = deque(maxlen=_BUF)
        self._idx_closes: deque = deque(maxlen=_BUF)

        # Latest computed indicators
        self._ind: Dict[str, float] = {
            "rsi": 50.0, "vwap": 0.0,
            "adx": 0.0,  "pdi":  0.0, "mdi": 0.0,
            "ema_fast": 0.0, "ema_slow": 0.0,
            "ltp": 0.0,  "close": 0.0,
        }

        self._clog: logging.Logger = _make_strategy_logger(underlying, client_id, binding_id)
        self._load_thresholds()

    # ── Config ────────────────────────────────────────────────────────────────

    def _load_thresholds(self) -> None:
        from data_layer.runtime_config import validate_index_section
        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        if not ss:
            logger.warning(
                "SellStraddle[%s]: 'sell_straddle' config section missing from runtime config — using defaults.",
                self._underlying,
            )
        validate_index_section(self._underlying, "sell_straddle", ss)

        def _cfg(key: str, default):
            """
            Dot-notation config reader — matches Option_Selling_May_2026 sell_v3 convention.
            'tsl_scalable.enabled' resolves ss['tsl_scalable']['enabled'].
            Falls back to flat key lookup for backward compatibility, then to default.
            """
            parts = key.split(".")
            node = ss
            for part in parts:
                if not isinstance(node, dict):
                    return default
                node = node.get(part)
                if node is None:
                    return default
            return node if node is not None else default

        self._entry_start     = _parse_time(ss.get("entry_start",    "09:20"))
        self._entry_cutoff    = _parse_time(ss.get("entry_end",      "15:15"))
        self._force_exit      = _parse_time(ss.get("squareoff_time", "15:15"))
        # Crypto (Delta) is 24/7 with a DAILY expiry at 17:30 IST. The trading window WRAPS the day
        # and excludes an expiry GAP [entry_cutoff, entry_start] (e.g. squareoff 16:30 → resume
        # 18:30 on the fresh contract). NSE/MCX keep the simple same-day start<cutoff window.
        self._is_crypto = bool(self._cfg and self._cfg.exchange.is_crypto(self._underlying))
        self._max_trades      = int(ss.get("max_trades", 1))
        # Re-entry cooldown after a FULL exit, in MINUTES (direct). Enter 5 → 5-minute
        # cooldown. Falls back to the legacy tf-multiplier × 5 for configs saved before
        # this became a direct-minutes field.
        self._sl_cooldown_minutes = float(
            ss.get("sl_cooldown_minutes", ss.get("sl_cooldown_tf_multiplier", 1.0) * 5.0)
        )
        # Per-lot exchange lot size (NIFTY 65, FINNIFTY 60, …) — NOT a config default
        # of 50. lot_multiplier (set per binding) is the NUMBER OF LOTS; total
        # contracts = lot_size × lot_multiplier.
        _exch_lots = self._cfg.exchange.lot_sizes if self._cfg else {}
        self._lot_size        = int(_exch_lots.get(self._underlying, ss.get("lot_size", 50)))

        # Trailing SL — enable/disable toggle + thresholds
        self._trail_sl_enabled = bool(ss.get("tsl_enabled", True))
        self._trail_lock_pct   = float(ss.get("trail_lock_pct",  20.0)) / 100.0
        self._trail_floor_pct  = float(ss.get("trail_floor_pct", 10.0)) / 100.0
        # Trailing-SL BASIS: "ltp" → profit% = running LTP P&L / entry credit × 100 (legacy);
        # "theta" → profit% = combined time-value decay % (pos.premium_decay_pct()).
        self._trail_basis      = str(ss.get("trail_basis", "ltp")).lower()

        # VWAP Rise SL — UI saves as nested {"enabled": bool, "threshold": float}
        _vwap_sl = ss.get("vwap_rise_sl", {})
        self._vwap_rise_enabled   = bool(_vwap_sl.get("enabled", ss.get("vwap_rise_sl_enabled", False)))
        self._vwap_rise_threshold = float(_vwap_sl.get("threshold", ss.get("vwap_rise_sl_threshold_pct", 1.0)))
        # Skip vwap_rise (and session_min_vwap updates) when either leg's broker ATP is stale — a
        # frozen illiquid leg (e.g. CRUDEOIL PE) must not poison the baseline → false vwap_rise.
        # 0/negative disables the freshness check. Per-index overridable like other params.
        # DEFAULT is index-aware: illiquid MCX commodities (e.g. CRUDEOIL) forward-fill ATP for
        # longer than the 90s liquid-index default, so they need a wider freshness window.
        _stale_default = 150.0 if str(self._underlying).upper() in ("CRUDEOIL", "NATURALGAS", "GOLD", "SILVER") else 90.0
        self._vwap_stale_sec = float(_vwap_sl.get("stale_sec", ss.get("vwap_stale_sec", _stale_default)))

        # Ratio exit — UI saves as nested {"enabled": bool, "threshold": float}
        _ratio = ss.get("ratio_exit", {})
        self._ratio_threshold = float(_ratio.get("threshold", ss.get("ratio_exit_threshold", 3.0)))
        # Max ratio allowed at entry — block scan_pool result if already too skewed at fill time
        self._max_entry_ratio = float(_ratio.get("max_entry_ratio", ss.get("max_entry_ratio", 0.0)))

        # Scalable TSL — UI saves as nested {"enabled": bool, "base_profit": int, ...}
        _tsl = ss.get("tsl_scalable", {})
        self._tsl_enabled        = bool(_tsl.get("enabled", ss.get("tsl_scalable_enabled", False)))
        self._tsl_base_profit_rs = float(_tsl.get("base_profit", ss.get("tsl_base_profit_rs", 1000.0)))
        self._tsl_base_lock_rs   = float(_tsl.get("base_lock",   ss.get("tsl_base_lock_rs",   250.0)))
        self._tsl_step_profit_rs = float(_tsl.get("step_profit", ss.get("tsl_step_profit_rs",  250.0)))
        self._tsl_step_lock_rs   = float(_tsl.get("step_lock",   ss.get("tsl_step_lock_rs",    250.0)))
        # Trailing-SL BASIS: "ltp" → profit measured from LTP P&L (legacy); "theta" → profit
        # measured as combined TIME-VALUE decay (points). Lets the staircase trail theta decay.
        self._tsl_basis          = str(_tsl.get("basis", ss.get("tsl_basis", "ltp"))).lower()

        # Day-level % guardrails — per_day[today] overrides global; enabled flag respected
        now_day = datetime.now(IST).strftime("%A").lower()
        _day    = ss.get("per_day", {}).get(now_day, {})
        _day_on = bool(_day.get("enabled", True))   # default True for backward compat
        _pt     = float(_day.get("profit_target_pct", 0)) if _day_on else 0.0
        self._day_profit_target_pct = _pt if _pt > 0 else float(ss.get("profit_target_pct", 0))
        _ls     = float(_day.get("loss_sl_pct", 0)) if _day_on else 0.0
        self._day_loss_sl_pct       = _ls if _ls > 0 else float(ss.get("loss_sl_pct", 0))
        # Day-wise exit BASIS — per-weekday choice of how the day target/SL % is measured:
        #   "ltp"   → (realized + running LTP P&L) / entry credit × 100   (default, legacy)
        #   "theta" → combined option TIME-VALUE decay % since entry (simple intrinsic-based theta)
        self._day_exit_basis = str(_day.get("exit_basis", ss.get("exit_basis", "ltp"))).lower()

        # min_ltp / ltp_target — minimum combined premium floor for entry
        # Config saves as "ltp_target" or "min_ltp" or "ltp_min" — try all three
        self._ltp_target = float(
            ss.get("ltp_target") or ss.get("min_ltp") or ss.get("ltp_min") or 0.0
        )
        # Entry threshold BASIS — the MIN-LTP filter can instead floor on per-leg THETA
        #   "ltp"   → each leg's raw LTP must be ≥ ltp_target (default, legacy)
        #   "theta" → each leg's TIME VALUE must be ≥ theta_target (intrinsic-stripped)
        # Per-day override falls back to the global sell_straddle setting.
        self._entry_basis = str(_day.get("entry_basis", ss.get("entry_basis", "ltp"))).lower()
        self._theta_target = float(
            _day.get("theta_target", ss.get("theta_target") or ss.get("entry_theta_target") or 0.0)
        )

        # LTP Decay — fires when either CE or PE LTP falls below threshold
        _ltp_d = ss.get("ltp_decay", {})
        self._ltp_decay_enabled = bool(_ltp_d.get("enabled", ss.get("ltp_decay_enabled", False)))
        self._ltp_exit_min      = float(_ltp_d.get("ltp_exit_min", ss.get("ltp_exit_min", 20.0)))

        # Dynamic exit rules (same format as entry_rules_beginning)
        self._exit_rules: list = ss.get("exit_rules", [])

        # Global PnL guardrail — cumulative session premium points
        _pnl_g = ss.get("guardrail_pnl", {})
        self._guardrail_pnl_enabled    = bool(_pnl_g.get("enabled", False))
        self._guardrail_pnl_target_pts = float(_pnl_g.get("target_pts", 0.0))
        self._guardrail_pnl_sl_pts     = float(_pnl_g.get("stoploss_pts", 0.0))

        # Global ROC guardrail — TF-boundary ROC monitoring
        _roc_g = ss.get("guardrail_roc", {})
        self._guardrail_roc_enabled  = bool(_roc_g.get("enabled", False))
        self._guardrail_roc_tf       = int(_roc_g.get("tf", 15))
        self._guardrail_roc_length   = int(_roc_g.get("length", 9))
        self._guardrail_roc_target   = float(_roc_g.get("target", -20.0))
        self._guardrail_roc_stoploss = float(_roc_g.get("stoploss", 10.0))

    def reconfigure(self) -> None:
        self._load_thresholds()
        logger.info("SellStraddle[%s]: reconfigured.", self._underlying)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    @property
    def _persist_key(self) -> str:
        # Per-binding books persist independently so each restores its OWN position.
        if self._client_id and self._binding_id:
            return f"{self._client_id}_{self._binding_id}_{self._underlying}_sell_straddle"
        return f"{self._underlying}_sell_straddle"

    async def _emit_order(self, ev) -> None:
        """Stamp this book's identity on every order so the bridge routes to ONLY this binding
        (no mirror), then publish. Legacy per-index engine leaves the tags empty → route-to-all."""
        ev.client_id  = self._client_id
        ev.binding_id = self._binding_id
        await self._bus.publish(Topic.ORDER_REQUEST, ev)

    def _persist(self) -> None:
        try:
            from data_layer import position_store as _ps
            if self._position and self._position.status == "open":
                _ps.save(self._persist_key, self._position.to_dict(),
                         product_type=getattr(self, "_product_type", "MIS"))
            else:
                _ps.clear(self._persist_key)
        except Exception as exc:
            logger.warning("SellStraddle[%s]: persist failed: %s", self._underlying, exc)
        self._persist_session()

    def _persist_session(self) -> None:
        """Persist the day's session aggregates (booked P&L, trades, stop-for-day) so a
        same-day restart keeps the correct Booked P&L instead of resetting it to 0. Stamped
        as MIS so the store auto-discards it on a NEW day (session resets daily)."""
        try:
            from data_layer import position_store as _ps
            _ps.save(self._persist_key + "_session", {
                "session_realized_pnl_pts": self._session_realized_pnl_pts,
                "trades_today":             self._trades_today,
                "stop_for_day":             self._stop_for_day,
                # Stamp the SESSION day (crypto: entry_start-anchored; NSE: calendar date) so a
                # restart that crosses midnight inside a crypto session keeps the right count, and a
                # restart in a NEW session starts fresh — independent of the position_store's
                # calendar-date MIS rule.
                "session_day":              str(self._session_day(datetime.now(IST))),
            }, product_type="MIS")
        except Exception as exc:
            logger.debug("SellStraddle[%s]: session persist failed: %s", self._underlying, exc)

    def _restore_session(self) -> None:
        """Restore the day's booked P&L / trade count on a same-day restart (the store discards
        a prior-day MIS file, so a new day starts fresh) — keeps Booked P&L from resetting to 0."""
        try:
            from data_layer import position_store as _ps
            _sess = _ps.load(self._persist_key + "_session")
            # Only restore if the saved session belongs to the CURRENT session day — for crypto this
            # is the entry_start-anchored day (so a midnight-crossing restart restores correctly, and
            # a restart in a new session ignores the stale count). NSE keeps calendar-date semantics.
            if _sess and str(_sess.get("session_day", str(self._session_day(datetime.now(IST))))) \
                    != str(self._session_day(datetime.now(IST))):
                logger.info("SellStraddle[%s]: persisted session is from a prior trading day "
                            "(%s) — starting fresh.", self._underlying, _sess.get("session_day"))
                _sess = None
            if _sess:
                self._session_realized_pnl_pts = float(_sess.get("session_realized_pnl_pts", 0.0) or 0.0)
                self._trades_today = max(self._trades_today, int(_sess.get("trades_today", 0) or 0))
                self._stop_for_day = bool(_sess.get("stop_for_day", False))
                logger.info("SellStraddle[%s]: restored session — booked=%.2f pts trades=%d stop_for_day=%s",
                            self._underlying, self._session_realized_pnl_pts, self._trades_today, self._stop_for_day)
        except Exception as exc:
            logger.debug("SellStraddle[%s]: session restore failed: %s", self._underlying, exc)

    def start(self) -> None:
        self._running = True
        # Restore an open position across restarts (MIS prior-day positions are
        # discarded by the store — broker squared them off at EOD).
        self._restore_session()
        try:
            from data_layer import position_store as _ps
            _saved = _ps.load(self._persist_key)
            if _saved:
                self._position = StraddlePosition.from_dict(_saved)
                # Heal positions persisted before lot_size was tracked (would show qty=0).
                if not self._position.lot_size:
                    self._position.lot_size = self._lot_size * self._lot_multiplier
                self._trades_today = max(self._trades_today, 1)
                # Restore the day-% denominator too. Without this, _initial_net_credit
                # stays 0 → the entire Day% guardrail block (`if _initial_net_credit > 0`)
                # is SKIPPED on a restored position → the day loss-SL never fires.
                if self._initial_net_credit <= 0 and self._position.net_credit > 0:
                    self._initial_net_credit = self._position.net_credit
                if self._initial_entry_time_value <= 0:
                    self._initial_entry_time_value = float(
                        getattr(self._position, "entry_time_value", 0.0) or 0.0
                    ) or self._initial_net_credit
                # WARM-UP GUARD: the persisted leg LTPs are from the LAST structural event
                # (entry/roll), NOT the latest tick — so running P&L computed off them right
                # after restore is stale and can FALSELY trip the Day-loss-SL (closing a
                # position the user wanted kept). Hold P&L-based exits until BOTH legs receive a
                # fresh live option tick (or a short timeout), so exits judge real prices only.
                import time as _t
                self._post_restore_warmup = True
                self._post_restore_at     = _t.monotonic()
                self._ce_ltp_fresh = False
                self._pe_ltp_fresh = False
                logger.info("SellStraddle[%s]: restored open position from store (credit=%.2f, qty=%d) "
                            "— exits HELD until fresh LTPs arrive.",
                            self._underlying, self._position.net_credit, self._position.lot_size)
        except Exception as exc:
            logger.warning("SellStraddle[%s]: restore failed: %s", self._underlying, exc)
        _tag = f"{self._underlying}" + (f"_{self._client_id}_{self._binding_id}"
                                        if self._client_id and self._binding_id else "")
        # Store queue references so stop_async() can unsubscribe them from EventBus,
        # preventing orphaned Queue objects accumulating across book restarts.
        self._loop_queues: Dict[str, asyncio.Queue] = {}
        self._tasks = [
            asyncio.create_task(self._candle_loop(), name=f"ss_{_tag}_candle"),
            asyncio.create_task(self._tick_loop(),   name=f"ss_{_tag}_tick"),
            asyncio.create_task(self._option_loop(), name=f"ss_{_tag}_opt"),
            asyncio.create_task(self._fill_loop(),   name=f"ss_{_tag}_fill"),
        ]
        async def _seed_pool():
            try:
                from data_layer.historical_candles import fetch_upstox_warm_1m
                from data_layer.instrument_registry import REGISTRY
                from data_layer.client_db import ClientDB
                import asyncio as _aio
                # wait briefly for the first spot tick so we know ATM
                for _ in range(30):
                    if self._spot > 0:
                        break
                    await _aio.sleep(2)
                creds = await _aio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
                token = (creds or {}).get("access_token", "")
                if not token or self._spot <= 0:
                    logger.info("SellStraddle[%s]: pool seed skipped (no token/spot).", self._underlying)
                    return
                step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
                ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
                itm = int(ss.get("pool_itm_depth", 4)); otm = int(ss.get("pool_otm_depth", 4))
                strikes = pool_strike_set(self._spot, step, itm, otm)
                exp = REGISTRY.get_active_expiry(self._underlying, datetime.now(IST).date())
                seeded = 0
                for stk in strikes:
                    for side in ("CE", "PE"):
                        ikey = REGISTRY.get_broker_symbol(self._underlying, exp, int(stk), side, "upstox")
                        if not ikey:
                            continue
                        bars = await fetch_upstox_warm_1m(ikey, token)
                        if bars:
                            closes = [b["close"] for b in bars]
                            self._pool_engine.seed_strike(int(stk), side, closes, closes)
                            seeded += 1
                logger.info("SellStraddle[%s]: pool engine seeded %d legs (warm RSI/ROC).",
                            self._underlying, seeded)
            except Exception as exc:
                logger.warning("SellStraddle[%s]: pool seed failed: %s", self._underlying, exc)
        asyncio.create_task(_seed_pool())
        logger.info("SellStraddleStrategy[%s]: started.", self._underlying)
        try:
            self._log_settings_banner()
        except Exception as exc:
            logger.warning("SellStraddle[%s]: settings banner failed: %s", self._underlying, exc)

    def _log_settings_banner(self) -> None:
        """Print a boxed summary of active settings at startup (what's configured and
        about to happen) — mirrors the Option_Selling_May_2026 'ACTIVE V3 STRATEGY' banner."""
        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")

        def _render(rules: list) -> str:
            if not rules:
                return "(none — immediate when LTP target met)"
            parts: list = []
            for i, r in enumerate(rules):
                if (r.get("indicator") or "").lower() == "advanced":
                    o1, o2 = (r.get("operand1") or "").upper(), (r.get("operand2") or "").upper()
                    seg = f"{o1}{r.get('operator_sym','')}{o2}({r.get('tf','')}m)"
                else:
                    seg = f"{(r.get('indicator') or '').upper()}{r.get('operator_sym','')}{r.get('threshold','')}({r.get('tf','')}m)"
                if i > 0:
                    parts.append((r.get("operator") or "AND").upper())
                parts.append(seg)
            return " ".join(parts)

        workflow   = ss.get("entry_workflow_mode", "hybrid")
        offset     = int(max(int(ss.get("pool_otm_depth", 0) or 0), int(ss.get("pool_itm_depth", 0) or 0)) or ss.get("v_slope_pool_offset") or ss.get("reentry_offset") or 4)
        beg        = _render(ss.get("entry_rules_beginning", []))
        ren        = _render(ss.get("entry_rules_reentry", []))
        exit_rules = _render(ss.get("exit_rules", []))
        ratio_on   = ss.get("ratio_exit", {}).get("enabled", True)
        decay_on   = self._ltp_decay_enabled

        L = [
            "╔══════════════════════════════════════════════════════════════════════",
            f"║ ACTIVE SELL-STRADDLE SETTINGS — {self._underlying}",
            "╠══════════════════════════════════════════════════════════════════════",
            f"║ TIMING: Start:{self._entry_start.strftime('%H:%M')} | EntryEnd:{self._entry_cutoff.strftime('%H:%M')} | "
            f"SquareOff:{self._force_exit.strftime('%H:%M')} | Lot:{self._lot_size} x{self._lot_multiplier}",
            f"║ SELECTION: workflow={workflow} | pool_offset=±{offset} | "
            f"ENTRY BASIS:{self._entry_basis.upper()} | "
            f"floor:{(self._theta_target if self._entry_basis=='theta' else self._ltp_target):.0f}"
            f"({'theta' if self._entry_basis=='theta' else 'ltp'})",
            f"║ BEGINNING ENTRY: {beg}",
            f"║ RE-ENTRY GATES:  {ren}",
            f"║ ROLLOVERS: Decay:{'ON' if decay_on else 'OFF'}({self._ltp_exit_min:.0f}) | "
            f"Ratio:{'ON' if ratio_on else 'OFF'}({self._ratio_threshold:.1f}x"
            + (f" MaxEntry:{self._max_entry_ratio:.1f}x" if self._max_entry_ratio > 0 else "")
            + ") | SmartRoll:ON",
            f"║ TRAILING SL: {'ON' if self._trail_sl_enabled else 'OFF'} "
            f"Lock:{self._trail_lock_pct*100:.1f}% Floor:{self._trail_floor_pct*100:.1f}% "
            f"BASIS:{self._trail_basis.upper()}",
            f"║ SCALABLE TSL: {'ON' if self._tsl_enabled else 'OFF'} "
            f"Base:{self._tsl_base_profit_rs:.0f}/{self._tsl_base_lock_rs:.0f} "
            f"Step:{self._tsl_step_profit_rs:.0f}/{self._tsl_step_lock_rs:.0f} ({self._ccy_symbol}/BTC if crypto) "
            f"BASIS:{self._tsl_basis.upper()}",
            f"║ VWAP RISE SL: {'ON' if self._vwap_rise_enabled else 'OFF'}({self._vwap_rise_threshold:.2f}%) | "
            f"ROC GUARDRAIL: {'ON' if self._guardrail_roc_enabled else 'OFF'}"
            f"({self._guardrail_roc_tf}m T:{self._guardrail_roc_target}/SL:{self._guardrail_roc_stoploss})",
            f"║ PNL GUARDRAIL: {'ON' if self._guardrail_pnl_enabled else 'OFF'} "
            f"T:{self._guardrail_pnl_target_pts:.0f}pts SL:{self._guardrail_pnl_sl_pts:.0f}pts | "
            f"DAY: T:{self._day_profit_target_pct:.0f}% SL:{self._day_loss_sl_pct:.0f}% "
            f"BASIS:{self._day_exit_basis.upper()}",
            f"║ DYNAMIC EXITS: {exit_rules}",
            f"║ EXIT PRIORITY: EOD→PnLguard→Day%→LTPdecay→Ratio→ScalableTSL→ROC→VWAPrise→exit_rules",
            f"║ LIMITS: Max Daily Trades:{self._max_trades}",
            "╚══════════════════════════════════════════════════════════════════════",
        ]
        for line in L:
            logger.info(line)
            self._clog.info(line)

    def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            if not t.done():
                t.cancel()
        # Tasks are awaited by the event loop after cancel(); callers that need to
        # guarantee cleanup should await stop_async() instead.

    async def stop_async(self) -> None:
        """Cancel all tasks and await their completion so EventBus queues are freed."""
        self._running = False
        for t in self._tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def reset_session(self) -> None:
        self._trades_today              = 0
        self._position                  = None
        self._sl_cooldown_until         = None
        self._market_open_dt            = None
        self._primed                    = False
        self._session_realized_pnl_pts  = 0.0
        self._initial_net_credit        = 0.0
        self._initial_entry_time_value  = 0.0
        self._stop_for_day              = False
        self._prem_closes.clear()
        self._prem_volumes.clear()
        self._chart_series.clear()
        self._chart_last_min = None
        self._idx_highs.clear()
        self._idx_lows.clear()
        self._idx_closes.clear()
        self._last_exit_rules_bucket = ""
        self._last_entry_bucket_b    = ""
        self._last_entry_bucket_r    = ""
        self._last_roc_guard_bucket  = ""
        self._strike_prem.clear()
        self._prev_atp_closed.clear()
        self._beginning_failed = False
        logger.info("SellStraddleStrategy[%s]: session reset.", self._underlying)

    # ── EventBus loops ────────────────────────────────────────────────────────

    async def _candle_loop(self) -> None:
        q = self._bus.subscribe(Topic.CANDLE_CLOSE)
        self._loop_queues["candle"] = q
        try:
            while self._running:
                try:
                    ev: CandleEvent = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break
                if ev.symbol != self._underlying:
                    continue
                try:
                    await self._on_candle(ev)
                except Exception as exc:
                    # Never let one candle error kill the loop (was stopping entries).
                    logger.exception("SellStraddle[%s]: _on_candle error: %s", self._underlying, exc)
        finally:
            self._bus.unsubscribe(Topic.CANDLE_CLOSE, q)
            self._loop_queues.pop("candle", None)

    async def _tick_loop(self) -> None:
        from data_layer.base_feeder import IndexTick
        import time as _t
        q = self._bus.subscribe(Topic.INDEX_TICK)
        self._loop_queues["tick"] = q
        _idx_count = 0
        _last_hb = 0.0
        try:
          while self._running:
            try:
                tick: IndexTick = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if tick.symbol != self._underlying:
                continue
            self._spot = tick.ltp
            _idx_count += 1
            # Keep the client premium chart filling from the index-tick stream (always flowing),
            # independent of CandleCache which can stall (no traded volume on the index).
            try:
                self._append_chart_point(datetime.now(IST))
            except Exception:
                pass
            # Heartbeat (once/60s): prove the INDEX-tick engine is alive and show the
            # entry/exit gate state even when nothing fires — so a silent stall (e.g. no
            # index feed, terminal off, stop_for_day) is diagnosable instead of invisible.
            _now_m = _t.monotonic()
            if _now_m - _last_hb >= 60.0:
                _last_hb = _now_m
                if self._position and self._position.status == "open":
                    _state = "position OPEN — exit-checking"
                elif self._sl_cooldown_until and datetime.now(IST) < self._sl_cooldown_until:
                    # Cooldown active: entry is blocked, but the pool engine keeps building
                    # per-strike VWAP/SLOPE/RSI so re-entry can fire instantly when it lifts.
                    _left = int((self._sl_cooldown_until - datetime.now(IST)).total_seconds())
                    _strikes = len(getattr(self._pool_engine, "_closes", {}) or {})
                    _state = (f"COOLDOWN active — re-entry at "
                              f"{self._sl_cooldown_until.strftime('%H:%M:%S')} ({_left}s left) | "
                              f"data flowing: {_strikes} pool strikes tracked")
                else:
                    _state = (f"no position — entry path (trades_today={self._trades_today} "
                              f"stop_for_day={self._stop_for_day} term={self._any_active_terminal()})")
                self._clog.info("IDX_TICKS: %d index ticks/60s spot=%.2f | %s",
                                _idx_count, self._spot, _state)
                _idx_count = 0
            # GUARD: an exception in _check_exits/_maybe_try_entry previously propagated and
            # KILLED this task — entry+exit went permanently silent while OPT_TICKS kept
            # logging from the separate option loop. Recover per-tick instead.
            try:
                if self._position and self._position.status == "open":
                    await self._check_exits()
                else:
                    await self._maybe_try_entry(datetime.now(IST))
            except Exception as _exc:
                logger.exception("SellStraddle[%s]: tick-handler error (recovered, engine alive): %s",
                                 self._underlying, _exc)
        finally:
            self._bus.unsubscribe(Topic.INDEX_TICK, q)
            self._loop_queues.pop("tick", None)

    async def _fill_loop(self) -> None:
        """Receive fill confirmations from StraddleExecutionBridge."""
        from execution_bridge.straddle_bridge import StraddleFillEvent
        q = self._bus.subscribe(Topic.ORDER_FILL)
        self._loop_queues["fill"] = q
        try:
          while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if not isinstance(ev, StraddleFillEvent):
                continue
            if ev.underlying != self._underlying:
                continue
            self._on_fill(ev)
        finally:
            self._bus.unsubscribe(Topic.ORDER_FILL, q)
            self._loop_queues.pop("fill", None)

    def _on_fill(self, fill) -> None:
        """Handle fill confirmation — finalize entry or exit prices."""
        if fill.action == "ENTRY":
            # ATOMICITY: the bridge aborted an asymmetric live entry (one leg only → it flattened the
            # filled leg). Discard the optimistic position so we NEVER manage a naked leg, and roll
            # back the trade counter so this failed attempt doesn't burn a daily trade.
            if getattr(fill, "entry_aborted", False):
                logger.error(
                    "SellStraddle[%s]: ENTRY ABORTED by bridge (asymmetric fill) — discarding "
                    "optimistic position; broker leg(s) were flattened. [%s/%s]",
                    self._underlying, getattr(fill, "client_id", ""), getattr(fill, "binding_id", ""),
                )
                self._position = None
                self._trades_today = max(0, self._trades_today - 1)
                self._order_pending = False
                self._persist()
                self._apply_sl_cooldown()   # brief breather before retrying the entry
                return
            if self._position and self._position.status == "open":
                _legs = getattr(fill, "legs", ["CE", "PE"])
                # GUARD: only adopt a fill price that is POSITIVE. A 0/missing fill (e.g. a
                # single-side roll where the other leg's fill comes back 0, or a glitched fill)
                # must never overwrite a real entry_price with 0 — that later books a phantom
                # -32360 loss and can falsely trip day_loss_sl.
                if "CE" in _legs and fill.ce_fill and fill.ce_fill > 0:
                    self._position.ce_leg.ltp         = fill.ce_fill
                    self._position.ce_leg.entry_price = fill.ce_fill
                    if getattr(fill, "ce_symbol", ""):
                        self._position.ce_leg.symbol = fill.ce_symbol
                if "PE" in _legs and fill.pe_fill and fill.pe_fill > 0:
                    self._position.pe_leg.ltp         = fill.pe_fill
                    self._position.pe_leg.entry_price = fill.pe_fill
                    if getattr(fill, "pe_symbol", ""):
                        self._position.pe_leg.symbol = fill.pe_symbol
                self._position.net_credit = self._position.ce_leg.entry_price + self._position.pe_leg.entry_price
                self._persist()   # re-persist confirmed entry so it survives a restart
                _ce_disp = self._position.ce_leg.symbol or f"CE{int(self._position.ce_leg.strike)}"
                _pe_disp = self._position.pe_leg.symbol or f"PE{int(self._position.pe_leg.strike)}"
                logger.info(
                    "SellStraddle[%s]: ENTRY confirmed — %s=%.2f %s=%.2f credit=%.2f [%s/%s] legs=%s",
                    self._underlying, _ce_disp, self._position.ce_leg.entry_price,
                    _pe_disp, self._position.pe_leg.entry_price,
                    self._position.net_credit, fill.client_id, fill.binding_id, _legs,
                )
            self._order_pending = False
        elif fill.action == "EXIT":
            logger.info(
                "SellStraddle[%s]: EXIT confirmed — CE=%.2f PE=%.2f [%s/%s]",
                self._underlying, fill.ce_fill, fill.pe_fill,
                fill.client_id, fill.binding_id,
            )
            self._order_pending = False

    async def _option_loop(self) -> None:
        from data_layer.base_feeder import OptionTick
        q = self._bus.subscribe(Topic.OPTION_TICK)
        self._loop_queues["option"] = q
        _tick_count = 0
        _last_log_ts = 0.0
        import time as _time
        try:
          while self._running:
            try:
                tick: OptionTick = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if tick.underlying != self._underlying:
                continue
            _tick_count += 1
            now_ts = _time.monotonic()
            if now_ts - _last_log_ts >= 60.0:
                _step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
                _atm = int(round(self._spot / _step) * _step) if self._spot > 0 else 0
                self._clog.info("OPT_TICKS: %d option ticks/60s  ATM=%d  CE%d=%.2f PE%d=%.2f",
                                _tick_count, _atm, _atm, self._ce_ltp, _atm, self._pe_ltp)
                _tick_count = 0
                _last_log_ts = now_ts
            # Only capture the ATM strike's premium for entry — otherwise a
            # far-OTM CE/PE tick would corrupt _ce_ltp/_pe_ltp and the straddle
            # would enter on the wrong (non-ATM) premium. Straddle sells ATM.
            step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
            atm = round(self._spot / step) * step if self._spot > 0 else 0
            # Ignore zero/garbage premium ticks — they corrupt VWAP/SLOPE and the
            # OPT_TICKS log (seen as CE=0.00 PE=0.00).
            # Per-strike cache (every subscribed strike) for balanced-pair selection.
            if tick.ltp > 0:
                _k = (int(tick.strike), tick.option_type)
                _a = float(getattr(tick, "atp", 0.0) or 0.0)
                entry = self._strike_prem.get(_k)
                if entry is None:
                    self._strike_prem[_k] = {"ltp": float(tick.ltp), "atp": _a}
                else:
                    entry["ltp"] = float(tick.ltp)
                    if _a > 0:
                        entry["atp"] = _a
                _eng_atp = float(self._strike_prem[_k].get("atp", 0.0) or 0.0)
                self._pool_engine.update_tick(
                    int(tick.strike), tick.option_type,
                    ltp=float(tick.ltp), atp=_eng_atp)
            if atm > 0 and tick.ltp > 0 and abs(tick.strike - atm) < step / 2:
                _atp = float(getattr(tick, "atp", 0.0) or 0.0)  # broker VWAP for this leg
                if tick.option_type == "CE":
                    self._ce_ltp = tick.ltp
                    if _atp > 0:
                        self._ce_atp = _atp
                elif tick.option_type == "PE":
                    self._pe_ltp = tick.ltp
                    if _atp > 0:
                        self._pe_atp = _atp
            if self._position and self._position.status == "open":
                pos = self._position
                _mk = float(getattr(tick, "atp", 0.0) or 0.0)
                if tick.option_type == "CE" and abs(tick.strike - pos.ce_leg.strike) < 0.01:
                    pos.ce_leg.ltp = tick.ltp
                    if _mk > 0:
                        pos.ce_leg.mark = _mk
                    self._ce_ltp_fresh = True
                elif tick.option_type == "PE" and abs(tick.strike - pos.pe_leg.strike) < 0.01:
                    pos.pe_leg.ltp = tick.ltp
                    if _mk > 0:
                        pos.pe_leg.mark = _mk
                    self._pe_ltp_fresh = True
        finally:
            self._bus.unsubscribe(Topic.OPTION_TICK, q)
            self._loop_queues.pop("option", None)

    # ── Candle processing ─────────────────────────────────────────────────────

    async def _on_candle(self, ev: CandleEvent) -> None:
        # Build the indicator series from ONE base timeframe (1-minute) only.
        # The candle bus emits 1/2/5/15/75m events for the same symbol; appending
        # all of them mixed/over-sampled the premium series and corrupted
        # VWAP/RSI/SLOPE. Using 1m gives a clean intraday series so the rolling
        # VWAP window (>1 day of 1m bars) ≈ cumulative intraday VWAP, matching the
        # reference system. (Exits are tick-driven, so this does not delay them.)
        if getattr(ev, "timeframe", 1) != 1:
            return

        now = datetime.now(IST)
        self._load_thresholds()

        # New trading day — wipe all intraday state (sell_straddle is intraday only). The "day" for
        # crypto is the Delta contract lifecycle (rolls at 17:30 IST), NOT calendar midnight — so a
        # BTC session (18:30→16:30, crossing midnight) is ONE day and resets only at the 17:30 expiry.
        if self._market_open_dt is not None and self._session_day(self._market_open_dt) != self._session_day(now):
            logger.info(
                "SellStraddle[%s]: new %s detected (%s→%s) — resetting session state.",
                self._underlying, "expiry-day (17:30 IST)" if self._is_crypto else "day",
                self._session_day(self._market_open_dt), self._session_day(now),
            )
            self.reset_session()

        # Record market-open for this session (first candle of the day)
        if self._market_open_dt is None or self._session_day(self._market_open_dt) != self._session_day(now):
            # Session open is instrument-specific: MCX commodities (CRUDEOIL…) open 09:00, NSE/BSE
            # indices 09:15. CRYPTO is 24/7 with NO fixed clock open and NO historical seed — so its
            # indicators warm from live bars starting when THIS book begins receiving data; priming
            # must count from NOW (the first candle of the session), not a fixed 09:15 (which would
            # leave it 'priming' until 09:19 if the book starts in the early hours).
            _mcx = set(getattr(self._cfg, "mcx_underlyings", ())) if self._cfg else set()
            if self._is_crypto:
                self._market_open_dt = now.replace(second=0, microsecond=0)
            else:
                _open = dtime(9, 0) if self._underlying in _mcx else _MARKET_OPEN
                self._market_open_dt = now.replace(
                    hour=_open.hour, minute=_open.minute, second=0, microsecond=0,
                )
            self._primed = False

        # Update buffers
        self._idx_highs.append(float(ev.high))
        self._idx_lows.append(float(ev.low))
        self._idx_closes.append(float(ev.close))
        _c, _p, _, _ = self._active_premium()
        combined = _c + _p
        if combined > 0:
            self._prem_closes.append(combined)
            self._prem_volumes.append(float(ev.volume) if ev.volume else 1.0)

        # Commit one warm bar per 1-min close for EVERY tracked strike — continuous,
        # independent of the active position (so re-entry/roll never resets the series).
        self._pool_engine.commit_bar(minute=ev.timestamp.hour * 60 + ev.timestamp.minute)

        self._recompute_indicators()

        # Record one timestamped chart point per 1-min close (combined premium + indicators).
        self._append_chart_point(ev.timestamp)

        # Force-exit
        if self._past_squareoff(now):
            if self._position and self._position.status == "open":
                await self._close_position("time_exit_eod")
            return

        # Snapshot every cached leg's current ATP as its "previous closed" value
        # for the NEXT candle's per-pair slope. CRITICAL: this MUST run AFTER the
        # entry evaluation above — otherwise prev == current within the same candle
        # and every per-pair slope reads 0.00 (SLOPE<0 can never pass → no entries).
        # Only overwrite on a valid ATP so a missing tick never corrupts the slope.
        for _k, _v in self._strike_prem.items():
            _a = _v.get("atp", 0.0)
            if _a and _a > 0:
                self._prev_atp_closed[_k] = _a

    def _active_premium(self) -> Tuple[float, float, float, float]:
        """(ce_ltp, pe_ltp, ce_atp, pe_atp) for the indicator series. When a position is
        OPEN, source from the POSITION's own legs (via _strike_prem) so the dynamic exit
        tracks the position — not the live ATM straddle (which may have drifted away or
        gone 0 after a restart, killing CLOSE/VWAP/SLOPE)."""
        if self._position and self._position.status == "open":
            pos = self._position
            _ce = self._strike_prem.get((int(pos.ce_leg.strike), "CE"), {})
            _pe = self._strike_prem.get((int(pos.pe_leg.strike), "PE"), {})
            return (float(_ce.get("ltp", 0.0) or 0.0), float(_pe.get("ltp", 0.0) or 0.0),
                    float(_ce.get("atp", 0.0) or 0.0), float(_pe.get("atp", 0.0) or 0.0))
        return (self._ce_ltp, self._pe_ltp, self._ce_atp, self._pe_atp)

    def _recompute_indicators(self) -> None:
        closes = np.array(self._prem_closes, dtype=np.float64)
        vols   = np.array(self._prem_volumes, dtype=np.float64)
        idx_h  = np.array(self._idx_highs,   dtype=np.float64)
        idx_l  = np.array(self._idx_lows,    dtype=np.float64)
        idx_c  = np.array(self._idx_closes,  dtype=np.float64)
        _ce_ltp, _pe_ltp, _ce_atp, _pe_atp = self._active_premium()
        ltp = _ce_ltp + _pe_ltp
        self._ind["ltp"]   = ltp
        self._ind["close"] = ltp
        # When a position is OPEN, the WARM pool engine is the source of truth for the
        # active pair's indicators — it never resets on re-entry/roll (unlike the
        # active-series buffers above), so it cannot produce false exits.
        if self._position and self._position.status == "open":
            _pe = self._pool_engine.pair_indicators(
                int(self._position.ce_leg.strike), int(self._position.pe_leg.strike))
            # Use the engine whenever it returns ANY data (close/vwap always present; slope/rsi/roc
            # when enough bars). Requiring "rsi" before let the code fall through to the legacy
            # active-series path, which produced inconsistent CLOSE (ATM, not the position) and a
            # garbage VWAP/SLOPE after re-entry → a FALSE vwap_rise_sl. The engine is the position
            # pair's source of truth; missing rsi/roc just stay N/A (rule treats as not-met).
            if _pe:
                for _k in ("rsi", "roc", "slope", "vwap", "vwap_prev", "close"):
                    if _k in _pe:
                        self._ind[_k] = _pe[_k]
                self._ind["ltp"] = ltp
                # Throttled marker so the shared log PROVES the warm engine is feeding the
                # active pair (sane RSI/SLOPE after re-entry, vs the old reset garbage).
                import time as _t
                if _t.monotonic() - getattr(self, "_ind_src_log", 0.0) > 60.0:
                    self._ind_src_log = _t.monotonic()
                    _ce_d = self._position.ce_leg.symbol or f"CE{int(self._position.ce_leg.strike)}"
                    _pe_d = self._position.pe_leg.symbol or f"PE{int(self._position.pe_leg.strike)}"
                    logger.info(
                        "SellStraddle[%s]: INDICATORS src=WARM-POOL-ENGINE %s/%s | "
                        "close=%.2f vwap=%.2f (prev=%.2f) slope=%.2f rsi=%.1f roc=%.2f",
                        self._underlying, _ce_d, _pe_d, _pe.get("close", 0.0),
                        _pe.get("vwap", 0.0), _pe.get("vwap_prev", 0.0), _pe.get("slope", 0.0),
                        _pe.get("rsi", 0.0), _pe.get("roc", 0.0))
                return   # warm engine data is the source of truth for the active pair
            else:
                import time as _t
                if _t.monotonic() - getattr(self, "_ind_src_log", 0.0) > 60.0:
                    self._ind_src_log = _t.monotonic()
                    logger.info("SellStraddle[%s]: INDICATORS src=FALLBACK-ACTIVE-SERIES "
                                "(pool engine not warm yet for CE%d/PE%d)", self._underlying,
                                int(self._position.ce_leg.strike), int(self._position.pe_leg.strike))
        if len(closes) >= 15:
            self._ind["rsi"] = rsi(closes)
        if len(closes) >= 9:
            self._ind["ema_fast"] = ema(closes, 9)
        if len(closes) >= 21:
            self._ind["ema_slow"] = ema(closes, 21)
        if len(idx_c) >= 42:
            adx_val, pdi_val, mdi_val = adx(idx_h, idx_l, idx_c)
            self._ind["adx"] = adx_val
            self._ind["pdi"] = pdi_val
            self._ind["mdi"] = mdi_val
        # ── VWAP from the BROKER (exchange ATP), NEVER computed ──────────────
        # Combined VWAP for the ATM straddle = ATM CE ATP + ATM PE ATP.
        # SLOPE = current closed-candle VWAP − previous closed-candle VWAP
        # (needs 2 valid closed VWAPs). CRITICAL: a candle with no valid VWAP
        # does NOT overwrite prev_vwap, so the next slope stays correct — this
        # fixes the intermittent VWAP=0 / slope=huge bug from the self-computed
        # VWAP. Negative slope => VWAP falling => favourable for selling.
        _cur_vwap = None
        if _ce_atp > 0 and _pe_atp > 0:
            _cur_vwap = float(_ce_atp + _pe_atp)
            self._ind["vwap"] = _cur_vwap
            _prev = self._prev_vwap_atp
            if _prev is not None and _prev > 0:
                _slope = float(_cur_vwap - _prev)
                self._ind["slope"]      = _slope
                self._ind["vwap_slope"] = _slope
                self._ind["slope_curr"] = _cur_vwap
                self._ind["slope_prev"] = _prev
            self._prev_vwap_atp = _cur_vwap   # update ONLY on a valid broker VWAP
        # ROC — rate of change of the combined premium over the last N closes (%).
        # Used by exit rules (e.g. ROC > 10). Standard ROC formula.
        if len(closes) >= 10:
            _ref = closes[-10]
            if _ref != 0:
                self._ind["roc"] = float((closes[-1] - _ref) / _ref * 100.0)

    def _pair_indicators(self, ce_strike: int, pe_strike: int) -> Optional[Dict[str, float]]:
        """Per-pair {close, vwap, slope, rsi, roc}. Prefer the WARM pool engine (continuous
        per-strike series); fall back to the feed-only cache computation when not yet warm."""
        ind = self._pool_engine.pair_indicators(int(ce_strike), int(pe_strike))
        if ind is not None and "rsi" in ind:
            return ind
        from strategies.straddle_selection import pair_indicators
        return pair_indicators(self._strike_prem, self._prev_atp_closed, ce_strike, pe_strike)

    def _ind_by_tf(self, ce_strike: int, pe_strike: int, *rule_lists) -> dict:
        """Map each tf used by the given rule list(s) -> that pair's indicators resampled to that tf.
        Always includes tf=1. Missing/None -> empty dict (rule operands become unavailable -> not-met)."""
        tfs = {1}
        for rl in rule_lists:
            for r in (rl or []):
                try:
                    tfs.add(int(r.get("tf", 1)))
                except Exception:
                    tfs.add(1)
        out = {}
        for tf in tfs:
            if tf <= 1:
                # tf=1: prefer pool engine, else fall back to feed-only cache (pre-warm).
                out[tf] = self._pair_indicators(int(ce_strike), int(pe_strike)) or {}
            else:
                out[tf] = self._pool_engine.pair_indicators_tf(int(ce_strike), int(pe_strike), tf) or {}
        return out

    # ── Priming wait ──────────────────────────────────────────────────────────

    def _priming_wait_minutes(self, rules: List[dict]) -> int:
        """
        Mirrors old base.py _is_in_priming_wait():
          wait = max_rule_tf × 2   if any rule uses SLOPE / VWAP_SLOPE
               = max_rule_tf × 1   otherwise
        """
        if not rules:
            return 0
        tfs = [int(r.get("tf", 1)) for r in rules if r.get("tf")]
        max_tf = max(tfs) if tfs else 1
        slope_names = {"slope", "vwap_slope", "slope_curr", "slope_prev"}
        has_slope = any(
            r.get("indicator", "").lower() in slope_names   # direct slope rule (non-advanced)
            for r in rules
            if r.get("indicator", "").lower() != "advanced"  # exclude advanced rule type
        )
        return max_tf * (2 if has_slope else 1)

    def _is_primed(self, now: datetime, rules: List[dict]) -> bool:
        """True once market_open + wait_minutes has passed."""
        if self._primed:
            return True
        wait_min = self._priming_wait_minutes(rules)
        if wait_min == 0:
            self._primed = True
            return True
        ready_at = self._market_open_dt + timedelta(minutes=wait_min)
        if now >= ready_at:
            self._primed = True
            logger.info(
                "SellStraddle[%s]: priming complete — waited %d min (ready at %s)",
                self._underlying, wait_min, ready_at.strftime("%H:%M"),
            )
            return True
        remaining = max(0, int((ready_at - now).total_seconds() / 60.0 + 0.999))
        logger.info(
            "SellStraddle[%s]: priming — ~%d min remaining (ready %s; wait=%d min from your rules: "
            "max_tf=%d ×%d slope)",
            self._underlying, remaining, ready_at.strftime("%H:%M"), wait_min,
            max((int(r.get("tf", 1)) for r in rules if r.get("tf")), default=1),
            2 if wait_min > max((int(r.get("tf", 1)) for r in rules if r.get("tf")), default=1) else 1,
        )
        return False

    # ── Entry ─────────────────────────────────────────────────────────────────

    def set_client_db(self, db) -> None:
        """Inject the shared ClientDB so entry can be gated on terminal+trade activation."""
        self._client_db = db

    def get_premium_series(self) -> list:
        """Timestamped 1-min combined-premium chart series with VWAP/RSI/SLOPE overlays.
        Consumed by the client-side chart endpoint. Returns a shallow copy (newest last)."""
        return list(self._chart_series)

    def _append_chart_point(self, ts: datetime) -> None:
        """Append ONE chart point for the given minute (idempotent per minute). Called from
        the CANDLE_CLOSE handler AND the index-tick loop, so the client premium chart keeps
        filling even if CandleCache stalls (it depends on no traded volume on the index)."""
        _m = ts.hour * 60 + ts.minute
        if getattr(self, "_chart_last_min", None) == _m:
            return
        self._chart_last_min = _m
        # Combined premium + the broker-VWAP/RSI/SLOPE the strategy trades on. With a position
        # open, pull VWAP/RSI/SLOPE from the WARM pool engine for the exact open pair.
        _ce_l, _pe_l, _, _ = self._active_premium()
        _ci = self._ind
        if self._position and self._position.status == "open":
            _pi = self._pool_engine.pair_indicators_tf(
                int(self._position.ce_leg.strike), int(self._position.pe_leg.strike), 1) or {}
            if _pi:
                _ci = {**self._ind, **_pi}
        self._chart_series.append({
            "ts":       ts.timestamp(),
            "combined": round(float(_ce_l + _pe_l), 2),
            "ce_ltp":   round(float(_ce_l), 2),
            "pe_ltp":   round(float(_pe_l), 2),
            "vwap":     round(float(_ci.get("vwap", 0.0) or 0.0), 2),
            "rsi":      round(float(_ci.get("rsi", 0.0) or 0.0), 2),
            "slope":    round(float(_ci.get("slope", 0.0) or 0.0), 2),
        })

    def _any_active_terminal(self) -> bool:
        """True if at least one client has a binding with terminal_connected AND engine_active,
        deployed to sell_straddle for this underlying. Fail-OPEN when no DB is wired (tests/dev)
        so unit tests and headless runs are unaffected; production injects the DB via run_system."""
        db = self._client_db
        if db is None:
            return True
        import time as _t
        _now = _t.monotonic()
        if _now - getattr(self, "_term_check_t", 0.0) < 5.0:
            return getattr(self, "_term_active_cached", False)
        self._term_check_t = _now
        active = False
        try:
            if self._client_id and self._binding_id:
                # PER-BINDING book: gate on THIS binding's Terminal (broker connected) AND THIS
                # deployment's per-strategy Run toggle (is_running). Other clients are irrelevant.
                _binds = {b.get("binding_id"): b for b in db.get_bindings_safe_sync(self._client_id)}
                _deps  = db.get_deployments_sync(self._client_id)
                _dep_running = any(
                    str(d.get("strategy_name", "")).lower() == "sell_straddle"
                    and str(d.get("underlying", "") or d.get("assigned_instrument", "")).upper() == self._underlying.upper()
                    and d.get("binding_id") == self._binding_id
                    and int(d.get("is_running", 0) or 0) == 1
                    for d in _deps
                )
                _b = _binds.get(self._binding_id)
                active = bool(_dep_running and _b and _b.get("terminal_connected"))
            else:
                for _client in db.get_all_clients_sync():
                    _cid = _client.get("client_id", "")
                    if not _cid:
                        continue
                    _binds = {b.get("binding_id"): b for b in db.get_bindings_safe_sync(_cid)}
                    for _dep in db.get_deployments_sync(_cid):
                        _sn = str(_dep.get("strategy_name", "")).lower()
                        _ul = str(_dep.get("underlying", "") or _dep.get("assigned_instrument", "")).upper()
                        if _sn == "sell_straddle" and _ul == self._underlying.upper():
                            _b = _binds.get(_dep.get("binding_id"))
                            if _b and _b.get("engine_active") and _b.get("terminal_connected"):
                                active = True
                                break
                    if active:
                        break
        except Exception as _exc:
            logger.debug("SellStraddle[%s]: terminal-active check error: %s", self._underlying, _exc)
            active = False
        self._term_active_cached = active
        return active

    def _granular_audit_clients(self) -> list:
        """Return [(client_id, binding_id), …] for bindings that (a) are deployed to
        sell_straddle for this underlying AND (b) have show_granular_ticks ON. Cached 5s
        so the per-tick EXIT-EVAL gate is cheap. Empty list ⇒ suppress the audit payload."""
        db = self._client_db
        if db is None:
            return []
        import time as _t
        _now = _t.monotonic()
        if _now - getattr(self, "_gran_check_t", 0.0) < 5.0:
            return getattr(self, "_gran_cached", [])
        self._gran_check_t = _now
        out: list = []
        try:
            for _client in db.get_all_clients_sync():
                _cid = _client.get("client_id", "")
                if not _cid:
                    continue
                _binds = {b.get("binding_id"): b for b in db.get_bindings_safe_sync(_cid)}
                for _dep in db.get_deployments_sync(_cid):
                    _sn = str(_dep.get("strategy_name", "")).lower()
                    _ul = str(_dep.get("underlying", "") or _dep.get("assigned_instrument", "")).upper()
                    if _sn == "sell_straddle" and _ul == self._underlying.upper():
                        _b = _binds.get(_dep.get("binding_id"))
                        if _b and _b.get("show_granular_ticks"):
                            out.append((_cid, _b.get("binding_id")))
        except Exception as _exc:
            logger.debug("SellStraddle[%s]: granular-audit check error: %s", self._underlying, _exc)
            out = []
        self._gran_cached = out
        return out

    @staticmethod
    def _at_tf_boundary(minute: int, second: int, max_tf: int) -> bool:
        return minute % max_tf == 0 and second >= 5

    async def _maybe_try_entry(self, now: datetime) -> None:
        """For each applicable rule set, fire only at ITS OWN max-tf boundary (+5s), once per
        bucket. Hybrid first-trade evaluates beginning AND reentry (each on its own cadence) until
        a position opens — no permanent flip, no dead gap."""
        if self._position and self._position.status == "open":
            return
        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        workflow = ss.get("entry_workflow_mode", "hybrid")
        is_beginning = (self._trades_today == 0)

        # BEGINNING is a first-trade-of-day concept. Once it has had its first WARM evaluation and
        # failed (_beginning_failed), the rest of the day uses the re-entry pool only — beginning is
        # NOT re-checked every 2 min. It re-arms only via reset_session / a fresh first trade.
        want_beg = (workflow == "beginning_only") or (
            workflow == "hybrid" and is_beginning and not self._beginning_failed)
        want_re  = (workflow == "reentry_only") or (workflow == "hybrid")

        due_beg = False
        if want_beg:
            rb = ss.get("entry_rules_beginning", [])
            mtf = max((int(r.get("tf", 1)) for r in rb), default=1)
            if self._at_tf_boundary(now.minute, now.second, mtf):
                bkt = f"{now:%Y%m%d_%H}{(now.minute // mtf) * mtf:02d}"
                if bkt != self._last_entry_bucket_b:
                    self._last_entry_bucket_b = bkt
                    due_beg = True
        due_re = False
        if want_re:
            rr = ss.get("entry_rules_reentry", [])
            mtf = max((int(r.get("tf", 1)) for r in rr), default=1)
            if self._at_tf_boundary(now.minute, now.second, mtf):
                bkt = f"{now:%Y%m%d_%H}{(now.minute // mtf) * mtf:02d}"
                if bkt != self._last_entry_bucket_r:
                    self._last_entry_bucket_r = bkt
                    due_re = True

        if due_beg or due_re:
            await self._try_entry(now, due_beg, due_re)

    async def _try_entry(self, now: datetime, due_beginning: bool = True,
                         due_reentry: bool = True) -> None:
        if self._stop_for_day:
            return  # Day profit-target or day-loss-SL already hit today
        if not self._any_active_terminal():
            import time as _t
            if _t.monotonic() - getattr(self, "_no_term_log", 0.0) > 60.0:
                self._no_term_log = _t.monotonic()
                logger.info("SellStraddle[%s]: WAITING — no terminal+trade active "
                            "(feeder running; entry starts when a client turns Terminal ON + Trade ON).",
                            self._underlying)
            return
        if not self._is_in_entry_window(now):
            return
        if self._trades_today >= self._max_trades:
            return
        if self._sl_cooldown_until and now < self._sl_cooldown_until:
            return
        if self._order_pending:
            return  # Waiting for fill confirmation from bridge
        if self._spot <= 0 or self._ce_ltp <= 0 or self._pe_ltp <= 0:
            _step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
            _atm = int(round(self._spot / _step) * _step) if self._spot > 0 else 0
            self._clog.info(
                "WAIT  spot=%.2f ATM=%d CE%d_ltp=%.2f PE%d_ltp=%.2f — waiting for option ticks",
                self._spot, _atm, _atm, self._ce_ltp, _atm, self._pe_ltp,
            )
            return

        # NOTE: the ltp_target floor is enforced INSIDE selection (reference parity):
        # select_balanced_pair requires anchor>=target and partner>=target; scan_pool
        # requires both legs>=target. We deliberately do NOT pre-gate on the ATM
        # legs here — an ATM-premium pre-check would wrongly block valid non-ATM
        # balanced pairs (ITM strikes carry higher premium than ATM). The spot/ce/pe>0
        # wait above already guarantees ATM ticks exist, which selection needs.

        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        workflow_mode = ss.get("entry_workflow_mode", "hybrid")
        is_beginning = (self._trades_today == 0)
        if workflow_mode == "beginning_only":
            if due_beginning:
                await self._eval_ruleset(now, "entry_rules_beginning", use_beginning_sel=True)
            return
        if workflow_mode == "reentry_only":
            if due_reentry:
                await self._eval_ruleset(now, "entry_rules_reentry", use_beginning_sel=False)
            return
        # hybrid: first trade tries beginning at its boundary; if it does NOT enter, the reentry
        # pool is tried at its own boundary (same pulse when both are due). Both run until a
        # position opens — no permanent flip.
        if is_beginning and due_beginning:
            await self._eval_ruleset(now, "entry_rules_beginning", use_beginning_sel=True)
            if self._position and self._position.status == "open":
                return
        if due_reentry:
            await self._eval_ruleset(now, "entry_rules_reentry", use_beginning_sel=False)

    async def _eval_ruleset(self, now: datetime, rule_key: str, use_beginning_sel: bool) -> None:
        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        rules = ss.get(rule_key, [])
        concept = "beginning" if use_beginning_sel else "reentry"

        if not self._is_primed(now, rules):
            self._clog.info(
                "EVAL %s [%s] PRIMING — waiting for indicator priming", self._underlying, rule_key,
            )
            return

        step   = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        offset = int(max(int(ss.get("pool_otm_depth", 0) or 0), int(ss.get("pool_itm_depth", 0) or 0)) or ss.get("v_slope_pool_offset") or ss.get("reentry_offset") or 4)
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0
        _eff_target = self._theta_target if self._entry_basis == "theta" else ltp_target

        # Granular-audit heartbeat: when admin has AUDIT on for a client AND there is NO
        # open position, surface the live entry-scan status so the client UI panel appears
        # immediately (otherwise it stays hidden until the first exit-eval of an open trade).
        if (self._position is None or self._position.status != "open"):
            _audit_clients = self._granular_audit_clients()
            if _audit_clients:
                try:
                    _atm = round(self._spot / step) * step if self._spot else 0
                    _crit_h = [
                        {"name": "Status", "detail": f"no open position — {concept} scan", "hit": False},
                        {"name": "Spot/ATM", "detail": f"{self._spot:.2f} / {int(_atm)}", "hit": False},
                        {"name": "Target/Offset", "detail": f"{self._entry_basis}≥{_eff_target:.0f}, ±{offset}", "hit": False},
                    ]
                    for _cid, _bid in _audit_clients:
                        await self._bus.publish(Topic.EXIT_AUDIT, {
                            "type": "exit_audit", "client_id": _cid, "binding_id": _bid,
                            "underlying": self._underlying, "pnl": 0.0, "credit": 0.0,
                            "criteria": _crit_h, "ind_by_tf": {}, "ts": now.timestamp(),
                        })
                except Exception:
                    pass

        from strategies.straddle_selection import select_balanced_pair, scan_pool

        _trace: list = []
        if use_beginning_sel:
            sel = select_balanced_pair(
                self._strike_prem, self._spot, step, offset, ltp_target, trace=_trace,
                entry_basis=self._entry_basis, theta_target=self._theta_target,
            )
        else:
            sel = scan_pool(
                self._strike_prem, self._spot, step, offset, ltp_target,
                rule_pass=lambda cs, ps: _eval_rules(rules, self._ind_by_tf(cs, ps, rules))[0],
                metric=ss.get("reentry_best_metric", "balanced_premium"),
                trace=_trace,
                entry_basis=self._entry_basis, theta_target=self._theta_target,
            )

        for _ln in _trace:
            self._clog.info("SELECT %s | %s", self._underlying, _ln)

        # Block entry if selected pair is already too skewed (avoids instant ratio_exit)
        if sel and self._max_entry_ratio > 0:
            _ce_ltp = self._strike_prem.get(f"{sel[0]}:CE", 0.0) or 0.0
            _pe_ltp = self._strike_prem.get(f"{sel[1]}:PE", 0.0) or 0.0
            if _ce_ltp > 0 and _pe_ltp > 0:
                _entry_ratio = max(_ce_ltp, _pe_ltp) / min(_ce_ltp, _pe_ltp)
                if _entry_ratio > self._max_entry_ratio:
                    self._clog.info(
                        "EVAL %s [%s] ENTRY-BLOCKED ratio=%.2fx > max_entry_ratio=%.2fx — pair CE%d/PE%d skewed, skipping",
                        self._underlying, rule_key, _entry_ratio, self._max_entry_ratio, sel[0], sel[1],
                    )
                    sel = None

        if not sel:
            if use_beginning_sel:
                self._clog.info(
                    "EVAL %s [%s] NO-PAIR — spot=%.2f no balanced pair (target=%.2f[%s] offset=%d)",
                    self._underlying, rule_key, self._spot, _eff_target, self._entry_basis, offset,
                )
            else:
                # Re-entry pool returned nothing — distinguish "no balanced pair exists"
                # from "pairs exist but all blocked by the re-entry gate" (e.g. SLOPE>0).
                from strategies.straddle_selection import reentry_block_reason
                diag = reentry_block_reason(
                    self._strike_prem, self._spot, step, offset, ltp_target,
                    rule_eval=lambda cs, ps: _eval_rules(rules, self._ind_by_tf(cs, ps, rules)),
                )
                if diag["kind"] == "no_pair":
                    self._clog.info(
                        "EVAL %s [%s] NO-PAIR — spot=%.2f no balanced pair exists (target=%.2f[%s] offset=%d)",
                        self._underlying, rule_key, self._spot, _eff_target, self._entry_basis, offset,
                    )
                else:  # blocked
                    self._clog.info(
                        "EVAL %s [%s] BLOCK — best pair CE%d=%.2f PE%d=%.2f credit=%.2f | %s "
                        "(pairs exist but none passed the re-entry gate)",
                        self._underlying, rule_key, diag["ce"], diag["ce_ltp"],
                        diag["pe"], diag["pe_ltp"], diag["ce_ltp"] + diag["pe_ltp"], diag["reason"],
                    )
            return

        ce_strike, pe_strike, ce_ltp, pe_ltp = sel
        ind_by_tf = self._ind_by_tf(ce_strike, pe_strike, rules)
        passed, reason = _eval_rules(rules, ind_by_tf)
        _dump = {tf: {k: round(v, 2) for k, v in (d or {}).items()} for tf, d in ind_by_tf.items()}
        self._clog.info(
            "EVAL %s [%s/%s] sell CE%d=%.2f + PE%d=%.2f credit=%.2f | rules: %s | result=%s | ind_by_tf=%s",
            self._underlying, rule_key, concept, ce_strike, ce_ltp, pe_strike, pe_ltp,
            ce_ltp + pe_ltp, reason, "PASS" if passed else "BLOCK", _dump,
        )
        if not passed:
            # Hybrid: a WARM beginning BLOCK flips this cycle to the re-entry pool for the rest of
            # the day (beginning is first-trade-of-day only). Flip ONLY when the rules were genuinely
            # evaluable (all operands present) — never on a warming/'N/A' block, so we don't skip
            # beginning before its slow-tf indicators are ready.
            if use_beginning_sel and "N/A" not in reason:
                self._beginning_failed = True
            return

        self._clog.info(
            "ENTRY attempting — CE%d=%.2f PE%d=%.2f credit=%.2f rules_passed",
            ce_strike, ce_ltp, pe_strike, pe_ltp, ce_ltp + pe_ltp,
        )
        await self._open_position(now, ce_strike, pe_strike, ce_ltp, pe_ltp, rule_key, reason)

    async def _open_position(
        self, now: datetime, ce_strike: int, pe_strike: int,
        ce_ltp: float, pe_ltp: float, rule_key: str, reason: str,
    ) -> None:
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        atm  = round(self._spot / step) * step

        self._event_counter += 1
        event_id = f"{self._underlying}_ENTRY_{self._event_counter}"

        # Per-leg open reason: first trade of the day = "beginning", else "reentry".
        _open_reason = "beginning" if rule_key == "entry_rules_beginning" else "reentry"

        # Create position immediately (paper fill will update entry prices)
        self._position = StraddlePosition(
            underlying        = self._underlying,
            atm_at_entry      = atm,
            entry_spot        = self._spot,
            ce_leg            = StraddleLeg("CE", ce_strike, ce_ltp, ce_ltp, open_time=now, open_reason=_open_reason),
            pe_leg            = StraddleLeg("PE", pe_strike, pe_ltp, pe_ltp, open_time=now, open_reason=_open_reason),
            net_credit        = ce_ltp + pe_ltp,
            open_time         = now,
            status            = "open",
            # inf -> the first tick re-captures THIS pair's VWAP as the low (rise starts at 0).
            # Using self._ind here would seed the OLD pair's VWAP (it lags one candle) and could
            # fire a false vwap_rise on a fresh entry right after a full close.
            session_min_vwap  = float("inf"),
            entry_indicators  = self._pair_indicators(ce_strike, pe_strike) or dict(self._ind),
            lot_size          = self._lot_size * self._lot_multiplier,
        )
        # Capture combined TIME VALUE at entry for the theta-based day exit (intrinsic from entry spot).
        from strategies.theta_calc import combined_time_value as _ctv
        self._position.entry_time_value = _ctv(ce_strike, pe_strike, self._spot, ce_ltp, pe_ltp)
        self._persist()   # survive restarts
        # Warm the EXACT selected strikes' RSI/ROC from REST history so the Dynamic exit isn't
        # 'RSI(N/A)' for ~30 min after entry (background — never blocks the entry path).
        asyncio.create_task(self._seed_exec_legs(int(ce_strike), int(pe_strike)))
        self._trades_today  += 1
        self._beginning_failed = False
        self._order_pending  = True
        # Lock initial credit/theta as denominators for day-% calculations.
        # LTP credit: fixed at first entry (never rises — credit doesn't grow on re-entry).
        if self._initial_net_credit <= 0:
            self._initial_net_credit = ce_ltp + pe_ltp
        # Theta denominator: update to the HIGHER value if a re-entry/roll collects more theta
        # than the original entry — so the profit target scales up with the new theta collected.
        # If re-entry theta < original, keep the original bar (client spec point 3).
        if self._position:
            _new_etv = float(getattr(self._position, "entry_time_value", 0.0) or 0.0) or (ce_ltp + pe_ltp)
            if _new_etv > self._initial_entry_time_value:
                self._initial_entry_time_value = _new_etv

        logger.info(
            "SellStraddle[%s]: ENTERED — CE%d=%.2f PE%d=%.2f credit=%.2f | %s=PASS [%s]",
            self._underlying, ce_strike, ce_ltp, pe_strike, pe_ltp, ce_ltp + pe_ltp,
            rule_key, reason,
        )

        # Publish to StraddleExecutionBridge → paper/live fill
        order_ev = StraddleOrderEvent(
            action         = "ENTRY",
            underlying     = self._underlying,
            atm            = atm,
            ce_strike      = ce_strike,
            pe_strike      = pe_strike,
            ce_ltp         = ce_ltp,
            pe_ltp         = pe_ltp,
            lot_multiplier = self._lot_multiplier,
            lot_size       = self._lot_size,
            spot           = self._spot,
            indicators     = dict(self._ind),
            event_id       = event_id,
        )
        await self._emit_order(order_ev)

    # ── Exit ─────────────────────────────────────────────────────────────────

    async def _seed_exec_legs(self, ce_strike: int, pe_strike: int) -> None:
        """At strike selection (entry), warm the EXACT exec strikes' RSI/ROC from REST 1m history
        so the Dynamic exit (RSI/ROC read at its timeframe) is available from the first exit-eval —
        instead of 'RSI(N/A)' for ~30 min while live tf-bars accumulate. Prepend-safe seed; skips
        legs already warm at the max exit-rule timeframe. Background task — never blocks entry."""
        try:
            from data_layer.historical_candles import fetch_upstox_warm_1m
            from data_layer.instrument_registry import REGISTRY
            from data_layer.client_db import ClientDB
            import asyncio as _aio
            _max_tf = max((int(r.get("tf", 1)) for r in (self._exit_rules or [])), default=2)
            if self._pool_engine.warm_tf(ce_strike, pe_strike, _max_tf):
                return   # startup seed already covered it
            creds = await _aio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
            token = (creds or {}).get("access_token", "")
            if not token:
                return
            exp = REGISTRY.get_active_expiry(self._underlying, datetime.now(IST).date())
            _need = self._pool_engine._rsi_len * max(_max_tf, 1) + 5
            for stk, side in ((ce_strike, "CE"), (pe_strike, "PE")):
                ikey = REGISTRY.get_broker_symbol(self._underlying, exp, int(stk), side, "upstox")
                if not ikey:
                    continue
                bars = await fetch_upstox_warm_1m(ikey, token, min_bars=_need)
                if bars:
                    closes = [b["close"] for b in bars]
                    self._pool_engine.seed_strike(int(stk), side, closes, closes)
            logger.info("SellStraddle[%s]: entry-seeded exec legs CE%d/PE%d from REST "
                        "(warm RSI/ROC up to tf=%d).", self._underlying, int(ce_strike),
                        int(pe_strike), _max_tf)
        except Exception as exc:
            logger.warning("SellStraddle[%s]: entry-seed exec legs failed: %s", self._underlying, exc)

    def _build_exit_criteria(self, pos, pnl: float, credit: float):
        """Build the live exit-criteria list (and per-tf indicator dump) for logging AND the
        granular client audit. Pure read — never mutates state, never raises. Tick-level rows
        (Day%/LTPdecay/Ratio/P&L) reflect the current tick; TF-based rows (Dynamic) reflect
        their own timeframe bucket. Returns (crit_list, exit_dump_or_None)."""
        _crit = []
        try:
            _dpt = float(getattr(self, "_day_profit_target_pct", 0.0) or 0.0)
            _dsl = float(getattr(self, "_day_loss_sl_pct", 0.0) or 0.0)
            if credit and (_dpt or _dsl):
                # CUMULATIVE for both bases (closed trades + open running) — matches the live check.
                _dpct = (self._session_realized_pnl_pts + pnl) / credit * 100.0
                _lbl = "Day%(θ)" if self._day_exit_basis == "theta" else "Day%"
                _crit.append((_lbl, f"{_dpct:.1f}% vs T{_dpt:.0f}/SL{_dsl:.0f}",
                              (_dpt > 0 and _dpct >= _dpt) or (_dsl > 0 and _dpct <= -_dsl)))
            elif not credit:
                _crit.append(("Day%", "SKIPPED (initial_credit=0!)", False))
            _ce_ltp = float(getattr(getattr(pos, "ce_leg", None), "ltp", 0) or 0)
            _pe_ltp = float(getattr(getattr(pos, "pe_leg", None), "ltp", 0) or 0)
            if self._ltp_decay_enabled:
                _lo = min(_ce_ltp, _pe_ltp) if (_ce_ltp > 0 and _pe_ltp > 0) else 0.0
                _crit.append(("LTPdecay", f"min({_lo:.1f}) < {self._ltp_exit_min:.0f}",
                              _lo > 0 and _lo < self._ltp_exit_min))
            if _ce_ltp > 0 and _pe_ltp > 0 and getattr(self, "_ratio_threshold", 0.0):
                _r = max(_ce_ltp, _pe_ltp) / min(_ce_ltp, _pe_ltp)
                _crit.append(("Ratio", f"{_r:.2f} vs {self._ratio_threshold:.1f}x", _r >= self._ratio_threshold))
            if self._tsl_enabled:
                _crit.append(("TSL", "ON (scalable)", False))
            if self._vwap_rise_enabled:
                _stale = not self._pool_engine.pair_atp_fresh(
                    pos.ce_leg.strike, pos.pe_leg.strike, self._vwap_stale_sec)
                _crit.append(("VWAPrise",
                              f"ON {self._vwap_rise_threshold:.1f}%{' STALE-skip' if _stale else ''}", False))
            if self._guardrail_pnl_enabled:
                _crit.append(("PnLguard", f"T{self._guardrail_pnl_target_pts:.0f}/SL{self._guardrail_pnl_sl_pts:.0f}pts", False))
            if self._guardrail_roc_enabled:
                _crit.append(("ROCguard", "ON", False))
            _exit_dump = None
            if self._exit_rules:
                _exit_ind_by_tf = self._ind_by_tf(pos.ce_leg.strike, pos.pe_leg.strike, self._exit_rules)
                _passed, _reason = _eval_rules(self._exit_rules, _exit_ind_by_tf)
                _crit.append(("Dynamic", _reason, _passed))
                _exit_dump = {tf: {k: round(v, 2) for k, v in (d or {}).items()}
                              for tf, d in _exit_ind_by_tf.items()}
                if 1 in _exit_dump:
                    _exit_dump[1]["stale"] = (0.0 if self._pool_engine.pair_atp_fresh(
                        pos.ce_leg.strike, pos.pe_leg.strike, self._vwap_stale_sec) else 1.0)
            return _crit, _exit_dump
        except Exception:
            return _crit, None

    async def _publish_exit_audit(self, pos, pnl: float, now: datetime) -> None:
        """Publish the live exit-criteria to enabled client UIs, throttled to ~3s so the
        panel updates on EVERY tick's data (P&L/Day%/LTPdecay/Ratio move live) — not only
        once per max-TF bucket like the log line."""
        _audit_clients = self._granular_audit_clients()
        if not _audit_clients:
            return
        import time as _t
        if _t.monotonic() - getattr(self, "_last_audit_pub", 0.0) < 3.0:
            return
        self._last_audit_pub = _t.monotonic()
        _credit = self._initial_net_credit or pos.net_credit or 0.0
        _crit, _exit_dump = self._build_exit_criteria(pos, pnl, _credit)
        _criteria = [{"name": _n, "detail": _d, "hit": bool(_h)} for (_n, _d, _h) in _crit]
        for _cid, _bid in _audit_clients:
            await self._bus.publish(Topic.EXIT_AUDIT, {
                "type": "exit_audit", "client_id": _cid, "binding_id": _bid,
                "underlying": self._underlying, "pnl": round(pnl, 2),
                "credit": round(_credit, 2), "criteria": _criteria,
                "ind_by_tf": _exit_dump or {}, "ts": now.timestamp(),
            })

    async def _check_exits(self) -> None:
        pos = self._position
        if not pos:
            return
        now = datetime.now(IST)
        pnl = pos.unrealized_pnl

        # Cache latest exit eval for dashboard (always updated, no audit-client gate).
        import time as _t_ev
        if _t_ev.monotonic() - getattr(self, "_last_eval_cache_t", 0.0) >= 3.0:
            self._last_eval_cache_t = _t_ev.monotonic()
            _credit_ev = self._initial_net_credit or pos.net_credit or 0.0
            _crit_ev, _dump_ev = self._build_exit_criteria(pos, pnl, _credit_ev)
            _max_tf_ev = max((int(r.get("tf", 1)) for r in (self._exit_rules or [])), default=1)
            self._last_exit_eval = {
                "criteria": [{"name": n, "detail": d, "hit": bool(h)} for n, d, h in _crit_ev],
                "ind_by_tf": {str(tf): {k: round(v, 2) for k, v in (idict or {}).items()}
                              for tf, idict in (_dump_ev or {}).items()},
                "max_tf": _max_tf_ev,
                "ts": now.timestamp(),
            }

        # Live granular exit audit → client UI, every tick (self-throttled to ~3s). This is
        # what makes the panel MOVE — the per-bucket EXIT-EVAL log below only refreshes once
        # per max-TF boundary, but tick-level rows (P&L/Day%/LTPdecay/Ratio) change each tick.
        await self._publish_exit_audit(pos, pnl, now)

        # ── Visibility: SHOW that exits are being evaluated each candle, with the
        #    live P&L vs the active thresholds (throttled to once/min, not per tick).
        import time as _t
        if _t.monotonic() - getattr(self, "_last_exit_log", 0.0) > 60.0:
            self._last_exit_log = _t.monotonic()
            _active = "".join([
                " PnLguard" if self._guardrail_pnl_enabled else "",
                " Decay"    if self._ltp_decay_enabled else "",
                " Ratio"    if getattr(self, "_ratio_exit_enabled", False) else "",
                " TSL"      if self._tsl_enabled else "",
                " ROC"      if getattr(self, "_guardrail_roc_enabled", getattr(self, "_roc_guardrail_enabled", False)) else "",
                " VWAPrise" if self._vwap_rise_enabled else "",
                " exit_rules" if getattr(self, "_exit_rules", None) else "",
            ]) or " (none)"
            logger.info(
                "SellStraddle[%s]: EXIT-CHECK pnl=%.2f pts | Day%% T:%.0f%%/SL:%.0f%% (credit=%.2f) | "
                "EOD@%s | active exits:%s",
                self._underlying, pnl, self._day_profit_target_pct, self._day_loss_sl_pct,
                self._initial_net_credit, self._force_exit.strftime("%H:%M"), _active,
            )

        # ── EOD FORCE SQUARE-OFF — highest priority, checked before all else ──────
        if self._past_squareoff(now):
            if self._position and self._position.status == "open":
                logger.info("SellStraddle[%s]: EOD SQUAREOFF — time=%s", self._underlying, now.strftime("%H:%M"))
                await self._close_position("eod_squareoff")
                self._stop_for_day = True
            return

        # ── POST-RESTORE WARM-UP GUARD ────────────────────────────────────────────
        # A just-restored position carries STALE persisted leg LTPs (saved at the last
        # entry/roll, not per tick). Computing running P&L off them can FALSELY trip the
        # Day-loss-SL and close a position the user wanted kept. Hold ALL P&L-based exits
        # (EOD above still runs) until both legs tick fresh, or a 20s safety timeout.
        if self._post_restore_warmup:
            if (self._ce_ltp_fresh and self._pe_ltp_fresh) or (_t.monotonic() - self._post_restore_at > 20.0):
                self._post_restore_warmup = False
                logger.info("SellStraddle[%s]: post-restore warm-up complete — exits armed "
                            "(CE_ltp=%.2f PE_ltp=%.2f pnl=%.2f pts).",
                            self._underlying, pos.ce_leg.ltp, pos.pe_leg.ltp, pos.unrealized_pnl)
            else:
                return   # hold exits until real prices arrive

        # ── MANDATORY GLOBAL GUARDRAILS (first — reference exit_logic order) ──────
        # guardrail_pnl — cumulative session premium points target / SL.
        # Runs BEFORE the day-% checks to mirror Option_Selling_May_2026 sell_v3.
        if self._guardrail_pnl_enabled:
            _session_pts = self._session_realized_pnl_pts + pnl
            if self._guardrail_pnl_target_pts > 0 and _session_pts >= self._guardrail_pnl_target_pts:
                logger.info(
                    "SellStraddle[%s]: GUARDRAIL_PNL TARGET — session=%.2f pts >= %.2f",
                    self._underlying, _session_pts, self._guardrail_pnl_target_pts,
                )
                await self._close_position("guardrail_pnl_target")
                self._stop_for_day = True
                return
            if self._guardrail_pnl_sl_pts != 0 and _session_pts <= self._guardrail_pnl_sl_pts:
                logger.info(
                    "SellStraddle[%s]: GUARDRAIL_PNL SL — session=%.2f pts <= %.2f",
                    self._underlying, _session_pts, self._guardrail_pnl_sl_pts,
                )
                await self._close_position("guardrail_pnl_sl")
                return

        # ── DAY-LEVEL % GUARDRAILS (stops trading for the day) ──
        # Metric depends on the per-weekday basis:
        #   "ltp"   → (all closed trades + running LTP P&L) / initial credit × 100  (legacy)
        #   "theta" → % of the TOTAL PREMIUM SOLD that has decayed for the live position
        #             = (entry premium − current premium)/entry premium × 100  (clean, premium-
        #             based; chosen 2026-06-10 — tracks P&L, doesn't oscillate with spot).
        # Both use the same per-day target/SL thresholds; positive = profit in either basis.
        if self._initial_net_credit > 0:
            # LTP basis  → denominator = initial LTP credit (net_credit per unit).
            # Theta basis → denominator = initial time-value received (entry_time_value × lot_size × qty);
            #               since lot factors cancel in %, use entry_time_value per unit as denominator;
            #               running component uses theta decay (entry_tv − current_tv) not raw LTP.
            if self._day_exit_basis == "theta" and self._initial_entry_time_value > 0:
                _etv = float(getattr(pos, "entry_time_value", 0.0) or 0.0)
                _running_theta = (_etv - pos.current_time_value(self._spot)) if _etv > 0 else pnl
                total_day_pts = self._session_realized_pnl_pts + _running_theta
                _day_denom = self._initial_entry_time_value
                _basis_lbl = "theta(cumulative)"
            else:
                total_day_pts = self._session_realized_pnl_pts + pnl
                _day_denom = self._initial_net_credit
                _basis_lbl = "ltp"
            total_day_pct = total_day_pts / _day_denom * 100

            if self._day_profit_target_pct > 0 and total_day_pct >= self._day_profit_target_pct:
                logger.info(
                    "SellStraddle[%s]: DAY PROFIT TARGET [%s] — day=%.1f%% (≥%.1f%%) | "
                    "closed=%.2f running=%.2f credit=%.2f prem(sold=%.2f cur=%.2f)",
                    self._underlying, _basis_lbl, total_day_pct, self._day_profit_target_pct,
                    self._session_realized_pnl_pts, pnl, _day_denom,
                    pos.net_credit, pos.current_value,
                )
                await self._close_position("day_profit_target")
                self._stop_for_day = True
                logger.info("SellStraddle[%s]: STOPPED FOR DAY (profit target reached).", self._underlying)
                return

            if self._day_loss_sl_pct > 0 and total_day_pct <= -self._day_loss_sl_pct:
                logger.info(
                    "SellStraddle[%s]: DAY LOSS SL [%s] — day=%.1f%% (≤-%.1f%%) | "
                    "closed=%.2f running=%.2f credit=%.2f prem(sold=%.2f cur=%.2f)",
                    self._underlying, _basis_lbl, total_day_pct, self._day_loss_sl_pct,
                    self._session_realized_pnl_pts, pnl, _day_denom,
                    pos.net_credit, pos.current_value,
                )
                await self._close_position("day_loss_sl")
                self._stop_for_day = True
                logger.info("SellStraddle[%s]: STOPPED FOR DAY (loss SL hit).", self._underlying)
                return

        # ── TRAILING SL (lock-%/floor-%-below-peak) — basis ltp | theta ──
        # Activates once profit% crosses lock%; then exits (full) if profit drops floor%
        # (percentage points) below the running peak. profit% measured per _trail_basis.
        if self._trail_sl_enabled:
            if self._trail_basis == "theta":
                _profit_pct = pos.premium_decay_pct()
            elif pos.net_credit > 0:
                _profit_pct = pnl / pos.net_credit * 100.0
            else:
                _profit_pct = None
            if _profit_pct is not None:
                if _profit_pct > pos.trail_peak_pct:
                    pos.trail_peak_pct = _profit_pct
                _lock_pts  = self._trail_lock_pct  * 100.0
                _floor_pts = self._trail_floor_pct * 100.0
                if pos.trail_peak_pct >= _lock_pts and _profit_pct <= (pos.trail_peak_pct - _floor_pts):
                    logger.info(
                        "SellStraddle[%s]: TRAILING SL [%s] — profit=%.1f%% dropped to peak(%.1f%%)−floor(%.1f%%) → full exit",
                        self._underlying, self._trail_basis, _profit_pct, pos.trail_peak_pct, _floor_pts,
                    )
                    await self._close_position(f"trailing_sl_{self._trail_basis}")
                    return

        # LTP Decay → single-side roll per decayed leg (reference exit_logic step 2)
        if self._ltp_decay_enabled:
            rolled_any = False
            for _side, _ltp in (("CE", pos.ce_leg.ltp), ("PE", pos.pe_leg.ltp)):
                if 0 < _ltp < self._ltp_exit_min and self._position and self._position.status == "open":
                    logger.info("SellStraddle[%s]: LTP DECAY %s ltp=%.2f < %.2f — single-side roll",
                                self._underlying, _side, _ltp, self._ltp_exit_min)
                    await self._single_side_roll(_side, now, f"ltp_decay_{_side}")
                    rolled_any = True
            if rolled_any:
                return

        # ── Exit priority below matches Option_Selling_May_2026 sell_v3 corrected order:
        #    Trailing-SL (lock/floor, ltp|theta) → LTP-decay → Ratio → Scalable TSL →
        #    ROC guardrail → VWAP-Rise → exit_rules.

        # 6. Ratio exit → ROLLOVER: close the LESS-PAIN (cheaper) leg and re-sell it closer,
        #    keeping the expensive (running) leg. Partner balanced against the running leg.
        if pos.ce_leg.ltp > 0 and pos.pe_leg.ltp > 0:
            ratio = max(pos.ce_leg.ltp, pos.pe_leg.ltp) / min(pos.ce_leg.ltp, pos.pe_leg.ltp)
            if ratio >= self._ratio_threshold:
                cheap = "PE" if pos.ce_leg.ltp > pos.pe_leg.ltp else "CE"   # less-pain side → roll
                keep  = "CE" if cheap == "PE" else "PE"
                logger.info("SellStraddle[%s]: RATIO EXIT ratio=%.2fx — roll %s (less-pain), keep %s",
                            self._underlying, ratio, cheap, keep)
                await self._single_side_roll(cheap, now, "ratio_exit")
                return

        # 7. Scalable TSL → SINGLE-SIDE roll (keep the losing/expensive leg, roll only the
        #    decayed/cheaper leg) — same rule as ltp_decay/ratio/vwap_rise. _single_side_roll
        #    re-pairs near ATM and closes both only if no valid partner exists.
        if self._tsl_enabled:
            # Profit fed to the TSL staircase: LTP P&L (default) or combined time-value decay
            # (points) when tsl_basis="theta" — so the trail measures theta decay, not LTP.
            _tsl_pnl = pnl
            if self._tsl_basis == "theta":
                _etv = float(getattr(pos, "entry_time_value", 0.0) or 0.0)
                if _etv > 0:
                    _tsl_pnl = _etv - pos.current_time_value(self._spot)
            if self._check_scalable_tsl(pos, _tsl_pnl):
                logger.info("SellStraddle[%s]: SCALABLE TSL (%s) — locked=%s%.4f pnl=%s%.4f",
                            self._underlying, self._tsl_basis,
                            self._ccy_symbol, pos.tsl_high_lock_rs,
                            self._ccy_symbol, self._pnl_rs(_tsl_pnl))
                # Roll the cheaper (decayed) leg; keep the richer (losing) leg.
                _roll_side = "CE" if pos.ce_leg.ltp <= pos.pe_leg.ltp else "PE"
                await self._single_side_roll(_roll_side, now, "scalable_tsl")
                return

        # 8. guardrail_roc — TF-boundary ROC of combined premium → smart roll first
        if self._guardrail_roc_enabled and len(self._prem_closes) >= self._guardrail_roc_length + 1:
            _rg_bucket = f"{now.strftime('%Y%m%d_%H')}{(now.minute // self._guardrail_roc_tf) * self._guardrail_roc_tf:02d}"
            if _rg_bucket != self._last_roc_guard_bucket:
                self._last_roc_guard_bucket = _rg_bucket
                _closes = list(self._prem_closes)
                _denom  = _closes[-(self._guardrail_roc_length + 1)]
                if _denom == 0:
                    _roc_val = None
                else:
                    _roc_val = (_closes[-1] - _denom) / _denom * 100
                if _roc_val is not None and self._guardrail_roc_target < 0 and _roc_val <= self._guardrail_roc_target:
                    logger.info(
                        "SellStraddle[%s]: ROC GUARDRAIL TARGET — roc=%.2f <= target=%.2f",
                        self._underlying, _roc_val, self._guardrail_roc_target,
                    )
                    await self._close_position("guardrail_roc_target")  # full exit → fresh re-entry
                    return
                if _roc_val is not None and self._guardrail_roc_stoploss >= 0 and _roc_val >= self._guardrail_roc_stoploss:
                    logger.info(
                        "SellStraddle[%s]: ROC GUARDRAIL SL — roc=%.2f >= sl=%.2f",
                        self._underlying, _roc_val, self._guardrail_roc_stoploss,
                    )
                    await self._close_position("guardrail_roc_sl")  # full exit → fresh re-entry
                    return

        # 9. VWAP Rise SL → smart roll first.
        # VWAP comes STRICTLY from the pool engine's continuous per-strike broker ATP for the EXACT
        # current pair — NOT the ATM/fallback path. Right after a roll the fallback can read a stale
        # or single-leg ATP and produce a corrupted-LOW VWAP that poisons session_min_vwap and fires
        # a FALSE vwap_rise (the cause of the constant vwap_rise churn). The pool engine stores every
        # subscribed pool strike's real-time ATP, so a rolled-to strike (always from the pool) is
        # already warm and gives the exact combined VWAP immediately. If a leg isn't warm yet, _vp is
        # None → skip this tick. Also reject a reading absurdly below the live combined premium
        # (close): combined ATP can never be ~half of combined LTP, so <60% of close is corruption.
        # STALENESS GUARD: if either leg's broker ATP hasn't ticked within _vwap_stale_sec, the
        # combined VWAP is built on a frozen leg (illiquid CRUDEOIL PE forward-filled). Skip the
        # whole vwap_rise step — do NOT read/update session_min_vwap — so a stale read can't set a
        # low baseline that later normal reads "rise" above (the false vwap_rise churn).
        if self._vwap_rise_enabled and self._pool_engine.pair_atp_fresh(
                int(pos.ce_leg.strike), int(pos.pe_leg.strike), self._vwap_stale_sec):
            _vp = self._pool_engine.pair_indicators(int(pos.ce_leg.strike), int(pos.pe_leg.strike))
            curr_vwap = float(_vp.get("vwap", 0.0)) if _vp else 0.0
            _vp_close = float(_vp.get("close", 0.0)) if _vp else 0.0
            # DROPOUT FILTER: a combined VWAP that suddenly craters >20% vs the last accepted
            # reading is a single-leg ATP dropout (seen on illiquid CRUDEOIL — VWAP 435→98→435).
            # Accepting it would set session_min to an absurd low and fire false vwap_rise on every
            # normal tick thereafter. Skip the whole step on such a glitch (don't read/update min).
            _glitch = (pos.vwap_last_good > 0 and curr_vwap > 0
                       and curr_vwap < 0.80 * pos.vwap_last_good)
            if curr_vwap > 0 and not _glitch and (_vp_close <= 0 or curr_vwap >= 0.60 * _vp_close):
                pos.vwap_last_good = curr_vwap
                if curr_vwap < pos.session_min_vwap:
                    pos.session_min_vwap = curr_vwap
                if pos.session_min_vwap < float("inf"):
                    rise_pct = (curr_vwap - pos.session_min_vwap) / pos.session_min_vwap * 100
                    if rise_pct >= self._vwap_rise_threshold:
                        # ROLLOVER (not full exit): close the LESS-BURNING (most-decayed/profitable)
                        # leg and re-sell it balanced against the running (burning) leg. For a short
                        # leg, pnl = entry - ltp; the higher pnl is the less-burning side.
                        _ce_pnl = float(pos.ce_leg.entry_price) - float(getattr(pos.ce_leg, "ltp", 0.0) or 0.0)
                        _pe_pnl = float(pos.pe_leg.entry_price) - float(getattr(pos.pe_leg, "ltp", 0.0) or 0.0)
                        _less_burning = "CE" if _ce_pnl >= _pe_pnl else "PE"
                        logger.info(
                            "SellStraddle[%s]: VWAP RISE — rise=%.2f%% curr=%.2f low=%.2f → roll "
                            "less-burning %s (CE pnl=%.2f PE pnl=%.2f)",
                            self._underlying, rise_pct, curr_vwap, pos.session_min_vwap,
                            _less_burning, _ce_pnl, _pe_pnl,
                        )
                        await self._single_side_roll(_less_burning, now, "vwap_rise_roll")
                        # Re-baseline the VWAP-rise low against the new (rolled) position so it does
                        # not immediately re-trigger every tick (natural cooldown until it rises again).
                        if self._position and self._position.status == "open":
                            self._position.session_min_vwap = float("inf")
                        return

        # EXIT-EVAL — once per max-TF bucket, log EVERY active exit criterion's live
        # evaluation (mirrors the entry EVAL line), then act on the dynamic exit_rules.
        _max_tf = (max((int(r.get("tf", 1)) for r in self._exit_rules), default=1)
                   if self._exit_rules else 5)
        _er_bucket = f"{now.strftime('%Y%m%d_%H')}{(now.minute // _max_tf) * _max_tf:02d}"
        if (now.minute % _max_tf == 0 and now.second >= 5
                and _er_bucket != self._last_exit_rules_bucket):
            self._last_exit_rules_bucket = _er_bucket
            # Use the SAME denominator as the real Day% check (_initial_net_credit), so
            # the log matches what actually fires.
            _credit = self._initial_net_credit or pos.net_credit or 0.0
            _passed, _reason = (False, "—")
            # Build the criteria list defensively — logging must never break the exits.
            # (The client audit is published per-tick via _publish_exit_audit; here we only
            #  log the per-bucket EXIT-EVAL line + extract the dynamic pass/reason.)
            try:
                _crit, _exit_dump = self._build_exit_criteria(pos, pnl, _credit)
                self._clog.info(format_exit_eval(self._underlying, pnl, _credit, _crit))
                if _exit_dump is not None:
                    self._clog.info("EXIT-EVAL %s exit_ind_by_tf=%s", self._underlying, _exit_dump)
                for _n, _d, _h in _crit:
                    if _n == "Dynamic":
                        _passed, _reason = bool(_h), _d
                        break
            except Exception as _exc:
                self._clog.info("EXIT-EVAL %s (formatting error: %s)", self._underlying, _exc)
                if self._exit_rules:
                    _passed, _reason = _eval_rules(
                        self._exit_rules,
                        self._ind_by_tf(pos.ce_leg.strike, pos.pe_leg.strike, self._exit_rules),
                    )

            if self._exit_rules and _passed:
                logger.info("SellStraddle[%s]: EXIT_RULES triggered — %s", self._underlying, _reason)
                await self._close_position("exit_rules")  # full exit → fresh re-entry
                return

    @property
    def _contract_cv(self) -> float:
        """Contract value multiplier: BTC=0.001, ETH=0.01, NSE/MCX=1.0.
        Converts premium-pts × contracts → actual currency ($ for crypto, ₹ for NSE)."""
        u = str(self._underlying).upper()
        if u == "BTC": return 0.001
        if u == "ETH": return 0.01
        return 1.0

    @property
    def _ccy_symbol(self) -> str:
        return "$" if self._contract_cv < 1.0 else "₹"

    def _pnl_rs(self, pnl_pts: float) -> float:
        """Convert P&L in premium points to currency units ($ for crypto, ₹ for NSE)."""
        qty = self._lot_size * self._lot_multiplier
        return pnl_pts * qty * self._contract_cv

    def _check_scalable_tsl(self, pos: StraddlePosition, pnl_pts: float) -> bool:
        """
        Rupee-based per-lot scalable TSL.
        Matches old exit_logic.py scalable TSL exactly.

        Lock staircase:
          PnL ≥ base_profit          → lock base_lock
          PnL ≥ base + 1×step_profit → lock base_lock + 1×step_lock
          PnL ≥ base + 2×step_profit → lock base_lock + 2×step_lock
          ...
        Once locked, exit when PnL drops below locked amount.
        """
        # Config values are "per contract unit" (₹ for NSE, $ per BTC for crypto).
        # Scale by lot_multiplier × contract_cv to get actual currency threshold.
        _cv          = self._contract_cv
        qty_mult     = self._lot_multiplier
        base_profit  = self._tsl_base_profit_rs  * qty_mult * _cv
        base_lock    = self._tsl_base_lock_rs    * qty_mult * _cv
        step_profit  = self._tsl_step_profit_rs  * qty_mult * _cv
        step_lock    = self._tsl_step_lock_rs    * qty_mult * _cv

        profit_rs = self._pnl_rs(pnl_pts)   # already uses _contract_cv via _pnl_rs

        if profit_rs >= base_profit and step_profit > 0:
            num_steps       = int((profit_rs - base_profit) // step_profit)
            calc_lock       = base_lock + num_steps * step_lock
            if calc_lock > pos.tsl_high_lock_rs:
                pos.tsl_high_lock_rs = calc_lock
                logger.debug(
                    "SellStraddle[%s]: TSL lock updated — %s%.4f (profit=%s%.4f step=%d)",
                    self._underlying, self._ccy_symbol, calc_lock,
                    self._ccy_symbol, profit_rs, num_steps,
                )

        if pos.tsl_high_lock_rs > 0 and profit_rs < pos.tsl_high_lock_rs:
            return True   # Exit
        return False

    # ── Smart Rolling ─────────────────────────────────────────────────────────

    # ── Close ─────────────────────────────────────────────────────────────────

    async def _close_leg(self, side: str, reason: str, now: datetime) -> float:
        """Close ONE leg (publish EXIT legs=[side]); book that leg's P&L into the
        session total. Returns leg P&L (pts). Does NOT clear the position by itself —
        the caller (single-side roll) enforces the 0-or-2 invariant."""
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        pos = self._position
        if not pos:
            return 0.0
        leg = pos.ce_leg if side == "CE" else pos.pe_leg
        # GUARD: a leg whose entry_price was lost (0/negative) would book a garbage P&L like
        # (0 - 323.60)*qty = -32360 and could falsely trip day_loss_sl. Treat unknown entry as
        # break-even (pnl=0) and log loudly instead of recording a phantom loss.
        if leg.entry_price and leg.entry_price > 0:
            leg_pnl = leg.entry_price - leg.ltp  # short option: credit - buyback
        else:
            leg_pnl = 0.0
            logger.error("SellStraddle[%s]: %s%d entry_price=%.2f invalid at close — booking pnl=0 "
                         "(NOT a real loss; entry was lost). reason=%s",
                         self._underlying, side, int(leg.strike), float(leg.entry_price or 0.0), reason)
        leg.close_time = now
        self._event_counter += 1
        order_ev = StraddleOrderEvent(
            action="EXIT", underlying=self._underlying, atm=pos.atm_at_entry,
            ce_strike=pos.ce_leg.strike, pe_strike=pos.pe_leg.strike,
            ce_ltp=pos.ce_leg.ltp, pe_ltp=pos.pe_leg.ltp,
            lot_multiplier=self._lot_multiplier, lot_size=self._lot_size,
            spot=self._spot, close_reason=reason, realized_pnl=leg_pnl,
            ce_entry=pos.ce_leg.entry_price, pe_entry=pos.pe_leg.entry_price,
            event_id=f"{self._underlying}_EXITLEG_{side}_{self._event_counter}",
            legs=[side],
            leg_open_times={side: leg.open_time.isoformat() if leg.open_time else None},
            leg_open_reasons={side: leg.open_reason},
        )
        await self._emit_order(order_ev)
        self._session_realized_pnl_pts += leg_pnl
        logger.info("SellStraddle[%s]: CLOSE LEG %s strike=%.0f pnl=%.2fpts [%s]",
                    self._underlying, side, leg.strike, leg_pnl, reason)
        return leg_pnl

    async def _open_leg(self, side: str, strike: int, ltp: float, now: datetime, reason: str) -> None:
        """Open ONE leg at a new strike (publish ENTRY legs=[side]); update the leg."""
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        pos = self._position
        if not pos:
            return
        leg = pos.ce_leg if side == "CE" else pos.pe_leg
        leg.strike = strike
        leg.entry_price = ltp
        leg.ltp = ltp
        leg.open_time = now          # per-leg source of truth (kept leg keeps its original open_time)
        leg.open_reason = reason
        leg.close_time = None
        pos.net_credit = pos.ce_leg.entry_price + pos.pe_leg.entry_price
        pos.tsl_high_lock_rs = 0.0
        pos.open_time = now
        self._event_counter += 1
        order_ev = StraddleOrderEvent(
            action="ENTRY", underlying=self._underlying, atm=pos.atm_at_entry,
            ce_strike=pos.ce_leg.strike, pe_strike=pos.pe_leg.strike,
            ce_ltp=pos.ce_leg.ltp, pe_ltp=pos.pe_leg.ltp,
            lot_multiplier=self._lot_multiplier, lot_size=self._lot_size,
            spot=self._spot, indicators=dict(self._ind),
            event_id=f"{self._underlying}_OPENLEG_{side}_{self._event_counter}",
            legs=[side],
        )
        await self._emit_order(order_ev)
        logger.info("SellStraddle[%s]: OPEN LEG %s strike=%.0f @%.2f [%s]",
                    self._underlying, side, strike, ltp, reason)

    async def _single_side_roll(self, side: str, now: datetime, reason: str) -> None:
        """Rollover one leg: close `side` (the less-pain / decayed leg) and re-sell it, KEEPING
        the other (running) leg fixed. The new leg is picked from the ATM±offset pool BALANCED
        against the running leg's premium. No valid partner → close all and start fresh (0-or-2)."""
        from strategies.straddle_selection import select_partner_for
        other = "PE" if side == "CE" else "CE"
        pos = self._position
        if not pos:
            return
        # Running (kept) leg = the OTHER side — capture its strike/premium BEFORE closing anything;
        # the re-sold leg is balanced against it.
        run_leg = pos.ce_leg if other == "CE" else pos.pe_leg
        run_strike = int(run_leg.strike)
        run_ltp = float(getattr(run_leg, "ltp", 0.0) or getattr(run_leg, "entry_price", 0.0) or 0.0)
        orig_strike = int((pos.ce_leg if side == "CE" else pos.pe_leg).strike)

        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        rules = ss.get("entry_rules_reentry", [])
        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        offset = int(max(int(ss.get("pool_otm_depth", 0) or 0), int(ss.get("pool_itm_depth", 0) or 0)) or ss.get("v_slope_pool_offset") or ss.get("reentry_offset") or 4)
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0
        # Keep rolled re-entries near ATM (real straddle) — cap how deep ITM the new leg may be.
        max_itm = int(ss.get("roll_max_itm_steps", 2))

        # SELECT THE REPLACEMENT BEFORE CLOSING — so a same-strike / no-op roll fires NO orders.
        # (Previously we closed first, then if selection returned the SAME strike we'd re-sell it:
        #  a buy-to-close + re-sell on the identical strike — a pointless wash + 2 broker orders.)
        sel = select_partner_for(
            self._strike_prem, side, run_strike, run_ltp,
            self._spot, step, offset, ltp_target,
            rule_pass=lambda cs, ps: _eval_rules(rules, self._ind_by_tf(cs, ps, rules))[0],
            max_itm_steps=max_itm,
        )
        if sel and int(sel[0]) == orig_strike:
            logger.info("SellStraddle[%s]: roll %s SKIPPED — best partner is the SAME strike %d "
                        "(no-op, no orders sent).", self._underlying, side, orig_strike)
            return

        await self._close_leg(side, reason, now)
        if sel:
            new_strike, new_ltp = sel
            logger.info("SellStraddle[%s]: ROLL %s → %s%d @%.2f (balanced vs running %s%d @%.2f)",
                        self._underlying, side, side, new_strike, new_ltp, other, run_strike, run_ltp)
            await self._open_leg(side, new_strike, new_ltp, now, f"single_side_roll_{reason}")
            # Re-baseline the per-position trackers vs the NEW combined pair: a rolled leg shifts the
            # combined VWAP (else vwap_rise re-fires vs the OLD low) and the credit (else the scalable
            # TSL stays anchored to the old peak).
            if self._position:
                self._position.session_min_vwap  = float("inf")
                self._position.peak_profit        = 0.0
                self._position.tsl_high_lock_rs   = 0.0
                self._position.trailing_active    = False
                # Re-arm the lock/floor TRAILING SL vs the NEW pair — without this it kept the OLD
                # peak% and could fire a 'trailing SL' the instant after a roll (and on the THETA
                # basis the % is measured against entry_time_value, which must also be re-baselined to
                # the new pair or every decay% reads against the wrong denominator).
                self._position.trail_peak_pct     = 0.0
                try:
                    self._position.entry_time_value = self._position.current_time_value(self._spot)
                    # Update day% theta denominator if roll collected more theta (client spec point 3)
                    if self._position.entry_time_value > self._initial_entry_time_value:
                        self._initial_entry_time_value = self._position.entry_time_value
                except Exception:
                    pass
            self._persist()
            return
        # No valid partner in the pool → close all and start fresh (re-entry loop re-enters).
        # This is a FULL exit, so the re-entry cooldown applies (same as _close_position).
        logger.warning("SellStraddle[%s]: roll %s found no partner for running %s — closing all (fresh).",
                       self._underlying, side, other)
        await self._close_leg(other, f"single_side_cleanup_{reason}", now)
        self._position = None
        self._persist()
        self._apply_sl_cooldown()

    async def _single_side_roll_to(self, side: str, strike: int, ltp: float, now: datetime, reason: str) -> None:
        """Partial roll: close one side and open a pre-selected candidate strike on that side.
        0-or-2 invariant: if ltp invalid, close the surviving leg too."""
        other = "PE" if side == "CE" else "CE"
        await self._close_leg(side, f"partial_roll_{reason}", now)
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0
        if strike and ltp and ltp >= ltp_target:
            await self._open_leg(side, strike, ltp, now, f"partial_roll_{reason}")
            if self._position:   # re-baseline vwap-rise + scalable-TSL vs the NEW pair
                self._position.session_min_vwap  = float("inf")
                self._position.peak_profit        = 0.0
                self._position.tsl_high_lock_rs   = 0.0
                self._position.trailing_active    = False
                # Re-arm the lock/floor TRAILING SL vs the NEW pair — without this it kept the OLD
                # peak% and could fire a 'trailing SL' the instant after a roll (and on the THETA
                # basis the % is measured against entry_time_value, which must also be re-baselined to
                # the new pair or every decay% reads against the wrong denominator).
                self._position.trail_peak_pct     = 0.0
                try:
                    self._position.entry_time_value = self._position.current_time_value(self._spot)
                    # Update day% theta denominator if roll collected more theta (client spec point 3)
                    if self._position.entry_time_value > self._initial_entry_time_value:
                        self._initial_entry_time_value = self._position.entry_time_value
                except Exception:
                    pass
            self._persist()
            return
        logger.warning("SellStraddle[%s]: partial roll %s invalid candidate — closing %s (0-or-2).",
                       self._underlying, side, other)
        await self._close_leg(other, f"partial_cleanup_{reason}", now)
        self._position = None
        self._persist()

    def discard_position_after_squareoff(self, reason: str) -> None:
        """Clear the in-memory + persisted position WITHOUT sending any exit orders. Used when an
        EXTERNAL square-off (Trade/Terminal OFF for the last active broker) has already flattened
        the broker legs — so the bot must forget the logical position too, otherwise start()
        restores a GHOST position on the next restart and tries to manage/exit a position that no
        longer exists at the broker. Books the running P&L into the session for day-% consistency."""
        if not self._position:
            return
        pos = self._position
        pos.realized_pnl = pos.unrealized_pnl
        pos.status = "closed"
        self._session_realized_pnl_pts += pos.realized_pnl
        logger.info(
            "SellStraddle[%s]: position DISCARDED after external square-off (%s) — pnl=%.2f pts; "
            "cleared persisted store so it will NOT restore on restart.",
            self._underlying, reason, pos.realized_pnl,
        )
        self._position = None
        self._persist()   # no open position → clears data/positions/<key>.json

    async def _close_position(self, reason: str) -> None:
        if not self._position:
            return
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        pos = self._position
        pos.realized_pnl = pos.unrealized_pnl
        pos.close_reason  = reason
        pos.close_time    = datetime.now(IST)
        pos.ce_leg.close_time = pos.close_time
        pos.pe_leg.close_time = pos.close_time
        pos.status        = "closed"

        logger.info(
            "SellStraddle[%s]: CLOSED — reason=%s pnl=%s%.4f (%.2f pts) "
            "CE %.2f→%.2f PE %.2f→%.2f",
            self._underlying, reason,
            self._ccy_symbol, self._pnl_rs(pos.realized_pnl), pos.realized_pnl,
            pos.ce_leg.entry_price, pos.ce_leg.ltp,
            pos.pe_leg.entry_price, pos.pe_leg.ltp,
        )

        self._event_counter += 1
        order_ev = StraddleOrderEvent(
            action         = "EXIT",
            underlying     = self._underlying,
            atm            = pos.atm_at_entry,
            ce_strike      = pos.ce_leg.strike,
            pe_strike      = pos.pe_leg.strike,
            ce_ltp         = pos.ce_leg.ltp,
            pe_ltp         = pos.pe_leg.ltp,
            lot_multiplier = self._lot_multiplier,
            lot_size       = self._lot_size,
            spot           = self._spot,
            close_reason   = reason,
            realized_pnl   = pos.realized_pnl,
            # Real per-leg ENTRY (sold) prices — carried so history records the true sold rate
            # and P&L even when the bridge's in-memory last-entry is gone (after a restart).
            ce_entry       = pos.ce_leg.entry_price,
            pe_entry       = pos.pe_leg.entry_price,
            event_id       = f"{self._underlying}_EXIT_{self._event_counter}",
            leg_open_times = {
                "CE": pos.ce_leg.open_time.isoformat() if pos.ce_leg.open_time else None,
                "PE": pos.pe_leg.open_time.isoformat() if pos.pe_leg.open_time else None,
            },
            leg_open_reasons = {
                "CE": pos.ce_leg.open_reason,
                "PE": pos.pe_leg.open_reason,
            },
        )
        await self._emit_order(order_ev)

        # Accumulate session realized P&L (in premium points)
        self._session_realized_pnl_pts += pos.realized_pnl
        logger.info(
            "SellStraddle[%s]: Session P&L — trade=%.2fpts cumulative=%.2fpts "
            "(day=%.1f%% of initial credit=%.2f)",
            self._underlying, pos.realized_pnl, self._session_realized_pnl_pts,
            (self._session_realized_pnl_pts / self._initial_net_credit * 100)
            if self._initial_net_credit > 0 else 0.0,
            self._initial_net_credit,
        )

        self._position = None
        self._persist()   # clears the stored position
        # Apply the configured re-entry cooldown after EVERY full exit (was: only stop_loss).
        # vwap_rise_sl / guardrail_roc / exit_rules now FULL-exit + re-enter, and with no cooldown
        # they re-entered the same candle → exit → re-enter → order CHURN (many rejected broker
        # orders). The cooldown (sl_cooldown_tf_multiplier, set in the UI) gives a one-candle
        # breather before re-entry. EOD/day-stop already block re-entry, so it's a no-op there.
        self._apply_sl_cooldown()

    def _apply_sl_cooldown(self) -> None:
        """Block re-entry for the configured number of MINUTES after a full exit."""
        cooldown_min = int(self._sl_cooldown_minutes)
        if cooldown_min > 0:
            self._sl_cooldown_until = datetime.now(IST) + timedelta(minutes=cooldown_min)
            logger.info("SellStraddle[%s]: re-entry cooldown %d min (no re-entry until %s).",
                        self._underlying, cooldown_min, self._sl_cooldown_until.strftime("%H:%M"))

    def _session_day(self, when: datetime):
        """The 'trading day' key for daily-reset. NSE/MCX → calendar date. Crypto → keyed to the
        CONFIGURED entry_start (e.g. 18:30): the day rolls when a new session begins, so a BTC
        session 18:30→16:30 (crossing midnight) is ONE day. The reset fires at entry_start — NOT
        calendar midnight, NOT a hardcoded expiry — and it's SAFE because reset_session wipes the
        position, and by entry_start we're already squared off (at squareoff/entry_end) and flat,
        so no live broker leg is orphaned. Before entry_start we're still in the prior session."""
        if self._is_crypto:
            from datetime import timedelta as _td
            return when.date() if when.time() >= self._entry_start else (when.date() - _td(days=1))
        return when.date()

    def _is_in_entry_window(self, now: datetime) -> bool:
        t = now.time()
        if self._is_crypto:
            # 24/7: allowed EXCEPT the expiry gap [entry_cutoff, entry_start] (e.g. 16:30→18:30).
            return not (self._entry_cutoff <= t < self._entry_start)
        return self._entry_start <= t < self._entry_cutoff

    def _past_squareoff(self, now: datetime) -> bool:
        """True when the book must be FLAT. NSE/MCX: now ≥ squareoff. Crypto: inside the daily
        expiry gap [squareoff, entry_start] (square off before the 17:30 expiry, resume after)."""
        t = now.time()
        if self._is_crypto:
            return self._force_exit <= t < self._entry_start
        return t >= self._force_exit

    # ── Public accessors ─────────────────────────────────────────────────────

    @property
    def has_open_position(self) -> bool:
        return self._position is not None and self._position.status == "open"

    @property
    def position(self) -> Optional[StraddlePosition]:
        return self._position

    @property
    def trades_today(self) -> int:
        return self._trades_today

    @property
    def indicators(self) -> Dict[str, float]:
        return dict(self._ind)


# ── Rule evaluator ────────────────────────────────────────────────────────────

def _compare(v1: float, v2: float, sym: str) -> bool:
    if sym == ">":  return v1 > v2
    if sym == "<":  return v1 < v2
    if sym == ">=": return v1 >= v2
    if sym == "<=": return v1 <= v2
    if sym == "==": return abs(v1 - v2) < 1e-9
    return False


def _eval_rules(rules: List[dict], ind_by_tf: Dict[int, Dict[str, float]]) -> Tuple[bool, str]:
    """
    Evaluate admin rule-builder rules against per-timeframe indicator values.
    Supports AND/OR with brackets — identical to old Rust-bridge token evaluator,
    but implemented in pure Python.

    ``ind_by_tf`` maps {tf:int -> {operand:value}}. Each rule is evaluated against
    the indicators resampled to THAT rule's ``tf`` (falling back to tf=1).

    Backward compat: if a flat single-tf dict {operand:value} is passed, it is
    wrapped as {1: ind} so old callers keep working.
    """
    if not rules:
        return True, "No rules — always allowed"

    # Backward-compat: flat {operand:value} dict -> treat as tf=1
    if ind_by_tf and not isinstance(next(iter(ind_by_tf.values())), dict):
        ind_by_tf = {1: ind_by_tf}

    tokens:  List[str] = []
    reasons: List[str] = []

    for i, rule in enumerate(rules):
        try:
            _tf = int(rule.get("tf", 1))
        except Exception:
            _tf = 1
        ind = ind_by_tf.get(_tf) or ind_by_tf.get(1, {})

        indicator = (rule.get("indicator") or "").lower()
        op_sym    = rule.get("operator_sym", "<")
        passed    = False
        label     = ""

        if indicator == "advanced":
            op1 = (rule.get("operand1") or "").lower()
            op2 = (rule.get("operand2") or "").lower()
            v1  = ind.get(op1)
            v2  = float(rule.get("operand2_val", 0)) if op2 == "value" else ind.get(op2)
            if v1 is not None and v2 is not None:
                passed = _compare(v1, v2, op_sym)
            v1s = f"{v1:.2f}" if isinstance(v1, float) else "N/A"
            v2s = f"{v2:.2f}" if isinstance(v2, float) else "N/A"
            label = f"{op1.upper()}({v1s}){op_sym}{op2.upper()}({v2s})"
        else:
            val = ind.get(indicator)
            thr = float(rule.get("threshold", 0))
            if val is not None:
                passed = _compare(val, thr, op_sym)
            lv = f"{val:.2f}" if isinstance(val, float) else "N/A"
            label = f"{indicator.upper()}({lv}){op_sym}{thr}"

        reasons.append(f"{label}={'✓' if passed else '✗'}")

        for b in str(rule.get("openBrackets", "")):
            tokens.append(b)
        tokens.append("True" if passed else "False")
        for b in str(rule.get("closeBrackets", "")):
            tokens.append(b)
        if i < len(rules) - 1:
            op = (rule.get("operator") or "AND").upper()
            tokens.append("and" if op == "AND" else "or")

    try:
        result = bool(eval(" ".join(tokens)))  # noqa: S307
    except Exception as exc:
        logger.error("SellStraddle rule eval error: %s tokens=%s", exc, tokens)
        result = False

    return result, " | ".join(reasons)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_time(s: str) -> dtime:
    try:
        h, m = s.split(":")
        return dtime(int(h), int(m))
    except Exception:
        return dtime(15, 15)


def pool_strike_set(atm: float, step: float, itm_depth: int, otm_depth: int,
                    pinned: Optional[set] = None) -> set:
    """Strikes to keep subscribed: ATM-itm_depth*step .. ATM+otm_depth*step (inclusive),
    PLUS any pinned strikes (the running position's legs — never dropped even if out of range)."""
    atm_r = round(atm / step) * step
    out = {int(atm_r + i * step) for i in range(-itm_depth, otm_depth + 1)}
    if pinned:
        out |= {int(p) for p in pinned}
    return out
