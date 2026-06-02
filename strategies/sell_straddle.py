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


def _make_strategy_logger(underlying: str) -> logging.Logger:
    """Write per-strategy evaluation log to logs/clients/ss_{underlying}_YYYYMMDD.log"""
    name = f"client.ss.{underlying}"
    lg = logging.getLogger(name)
    if lg.handlers:
        return lg
    lg.setLevel(logging.DEBUG)
    log_dir = os.path.join("logs", "clients")
    os.makedirs(log_dir, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    fh = logging.FileHandler(
        os.path.join(log_dir, f"ss_{underlying}_{date_str}.log"), encoding="utf-8"
    )
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))
    lg.addHandler(fh)
    lg.propagate = False
    return lg


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class StraddleLeg:
    option_type: str
    strike: float
    entry_price: float
    ltp: float = 0.0


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

    # Total contracts per leg (lot_size × lot_multiplier) — used by the dashboard
    # to render qty and rupee P&L. Without it the UI shows qty=0 → P&L always 0.
    lot_size: int = 0

    def to_dict(self) -> dict:
        """JSON-serialisable snapshot for PositionStore."""
        def _leg(l: StraddleLeg) -> dict:
            return {"option_type": l.option_type, "strike": l.strike,
                    "entry_price": l.entry_price, "ltp": l.ltp}
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
        }

    @classmethod
    def from_dict(cls, d: dict) -> "StraddlePosition":
        from datetime import datetime as _dt
        def _leg(x: dict) -> StraddleLeg:
            return StraddleLeg(option_type=x["option_type"], strike=x["strike"],
                               entry_price=x["entry_price"], ltp=x.get("ltp", 0.0))
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
        )

    @property
    def current_value(self) -> float:
        return self.ce_leg.ltp + self.pe_leg.ltp

    @property
    def unrealized_pnl(self) -> float:
        return self.net_credit - self.current_value


# ── Strategy ──────────────────────────────────────────────────────────────────

class SellStraddleStrategy:

    def __init__(
        self,
        bus: EventBus,
        cfg=None,
        underlying: str = "NIFTY",
        lot_multiplier: int = 1,
    ) -> None:
        self._bus            = bus
        self._cfg            = cfg
        self._underlying     = underlying
        self._lot_multiplier = lot_multiplier
        self._running        = False

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
        self._initial_net_credit: float = 0.0         # credit from first trade — fixed denominator for day %
        self._stop_for_day: bool = False               # True after day-profit-target or day-loss-SL fires

        self._tasks: list = []
        self._sl_cooldown_until: Optional[datetime] = None
        self._event_counter: int = 0

        # Combined CE+PE premium candle buffer
        self._prem_closes:  deque = deque(maxlen=_BUF)
        self._prem_volumes: deque = deque(maxlen=_BUF)

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

        self._clog: logging.Logger = _make_strategy_logger(underlying)
        self._load_thresholds()

    # ── Config ────────────────────────────────────────────────────────────────

    def _load_thresholds(self) -> None:
        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")

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
        self._max_trades      = int(ss.get("max_trades", 1))
        self._sl_cooldown_tf_mult = float(ss.get("sl_cooldown_tf_multiplier", 1.0))
        # Per-lot exchange lot size (NIFTY 65, FINNIFTY 60, …) — NOT a config default
        # of 50. lot_multiplier (set per binding) is the NUMBER OF LOTS; total
        # contracts = lot_size × lot_multiplier.
        _exch_lots = self._cfg.exchange.lot_sizes if self._cfg else {}
        self._lot_size        = int(_exch_lots.get(self._underlying, ss.get("lot_size", 50)))

        # Trailing SL — enable/disable toggle + thresholds
        self._trail_sl_enabled = bool(ss.get("tsl_enabled", True))
        self._trail_lock_pct   = float(ss.get("trail_lock_pct",  20.0)) / 100.0
        self._trail_floor_pct  = float(ss.get("trail_floor_pct", 10.0)) / 100.0

        # VWAP Rise SL — UI saves as nested {"enabled": bool, "threshold": float}
        _vwap_sl = ss.get("vwap_rise_sl", {})
        self._vwap_rise_enabled   = bool(_vwap_sl.get("enabled", ss.get("vwap_rise_sl_enabled", False)))
        self._vwap_rise_threshold = float(_vwap_sl.get("threshold", ss.get("vwap_rise_sl_threshold_pct", 1.0)))

        # Ratio exit — UI saves as nested {"enabled": bool, "threshold": float}
        _ratio = ss.get("ratio_exit", {})
        self._ratio_threshold = float(_ratio.get("threshold", ss.get("ratio_exit_threshold", 3.0)))

        # Scalable TSL — UI saves as nested {"enabled": bool, "base_profit": int, ...}
        _tsl = ss.get("tsl_scalable", {})
        self._tsl_enabled        = bool(_tsl.get("enabled", ss.get("tsl_scalable_enabled", False)))
        self._tsl_base_profit_rs = float(_tsl.get("base_profit", ss.get("tsl_base_profit_rs", 1000.0)))
        self._tsl_base_lock_rs   = float(_tsl.get("base_lock",   ss.get("tsl_base_lock_rs",   250.0)))
        self._tsl_step_profit_rs = float(_tsl.get("step_profit", ss.get("tsl_step_profit_rs",  250.0)))
        self._tsl_step_lock_rs   = float(_tsl.get("step_lock",   ss.get("tsl_step_lock_rs",    250.0)))

        # Day-level % guardrails — per_day[today] overrides global; enabled flag respected
        now_day = datetime.now(IST).strftime("%A").lower()
        _day    = ss.get("per_day", {}).get(now_day, {})
        _day_on = bool(_day.get("enabled", True))   # default True for backward compat
        _pt     = float(_day.get("profit_target_pct", 0)) if _day_on else 0.0
        self._day_profit_target_pct = _pt if _pt > 0 else float(ss.get("profit_target_pct", 0))
        _ls     = float(_day.get("loss_sl_pct", 0)) if _day_on else 0.0
        self._day_loss_sl_pct       = _ls if _ls > 0 else float(ss.get("loss_sl_pct", 0))

        # min_ltp / ltp_target — minimum combined premium floor for entry
        # Config saves as "ltp_target" or "min_ltp" or "ltp_min" — try all three
        self._ltp_target = float(
            ss.get("ltp_target") or ss.get("min_ltp") or ss.get("ltp_min") or 0.0
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
        return f"{self._underlying}_sell_straddle"

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

    def start(self) -> None:
        self._running = True
        # Restore an open position across restarts (MIS prior-day positions are
        # discarded by the store — broker squared them off at EOD).
        try:
            from data_layer import position_store as _ps
            _saved = _ps.load(self._persist_key)
            if _saved:
                self._position = StraddlePosition.from_dict(_saved)
                # Heal positions persisted before lot_size was tracked (would show qty=0).
                if not self._position.lot_size:
                    self._position.lot_size = self._lot_size * self._lot_multiplier
                self._trades_today = max(self._trades_today, 1)
                logger.info("SellStraddle[%s]: restored open position from store (credit=%.2f, qty=%d).",
                            self._underlying, self._position.net_credit, self._position.lot_size)
        except Exception as exc:
            logger.warning("SellStraddle[%s]: restore failed: %s", self._underlying, exc)
        self._tasks = [
            asyncio.create_task(self._candle_loop(), name=f"ss_{self._underlying}_candle"),
            asyncio.create_task(self._tick_loop(),   name=f"ss_{self._underlying}_tick"),
            asyncio.create_task(self._option_loop(), name=f"ss_{self._underlying}_opt"),
            asyncio.create_task(self._fill_loop(),   name=f"ss_{self._underlying}_fill"),
        ]
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
        offset     = int(ss.get("v_slope_pool_offset") or ss.get("reentry_offset") or 4)
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
            f"║ SELECTION: workflow={workflow} | pool_offset=±{offset} | Target LTP(floor):{self._ltp_target:.0f}",
            f"║ BEGINNING ENTRY: {beg}",
            f"║ RE-ENTRY GATES:  {ren}",
            f"║ ROLLOVERS: Decay:{'ON' if decay_on else 'OFF'}({self._ltp_exit_min:.0f}) | "
            f"Ratio:{'ON' if ratio_on else 'OFF'}({self._ratio_threshold:.1f}x) | SmartRoll:ON",
            f"║ SCALABLE TSL: {'ON' if self._tsl_enabled else 'OFF'} "
            f"Base:{self._tsl_base_profit_rs:.0f}/{self._tsl_base_lock_rs:.0f} "
            f"Step:{self._tsl_step_profit_rs:.0f}/{self._tsl_step_lock_rs:.0f} (₹)",
            f"║ VWAP RISE SL: {'ON' if self._vwap_rise_enabled else 'OFF'}({self._vwap_rise_threshold:.2f}%) | "
            f"ROC GUARDRAIL: {'ON' if self._guardrail_roc_enabled else 'OFF'}"
            f"({self._guardrail_roc_tf}m T:{self._guardrail_roc_target}/SL:{self._guardrail_roc_stoploss})",
            f"║ PNL GUARDRAIL: {'ON' if self._guardrail_pnl_enabled else 'OFF'} "
            f"T:{self._guardrail_pnl_target_pts:.0f}pts SL:{self._guardrail_pnl_sl_pts:.0f}pts | "
            f"DAY: T:{self._day_profit_target_pct:.0f}% SL:{self._day_loss_sl_pct:.0f}%",
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

    def reset_session(self) -> None:
        self._trades_today              = 0
        self._position                  = None
        self._sl_cooldown_until         = None
        self._market_open_dt            = None
        self._primed                    = False
        self._session_realized_pnl_pts  = 0.0
        self._initial_net_credit        = 0.0
        self._stop_for_day              = False
        self._prem_closes.clear()
        self._prem_volumes.clear()
        self._idx_highs.clear()
        self._idx_lows.clear()
        self._idx_closes.clear()
        self._last_exit_rules_bucket = ""
        self._last_roc_guard_bucket  = ""
        self._strike_prem.clear()
        self._prev_atp_closed.clear()
        self._beginning_failed = False
        logger.info("SellStraddleStrategy[%s]: session reset.", self._underlying)

    # ── EventBus loops ────────────────────────────────────────────────────────

    async def _candle_loop(self) -> None:
        q = self._bus.subscribe(Topic.CANDLE_CLOSE)
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

    async def _tick_loop(self) -> None:
        from data_layer.base_feeder import IndexTick
        q = self._bus.subscribe(Topic.INDEX_TICK)
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
            if self._position and self._position.status == "open":
                await self._check_exits()

    async def _fill_loop(self) -> None:
        """Receive fill confirmations from StraddleExecutionBridge."""
        from execution_bridge.straddle_bridge import StraddleFillEvent
        q = self._bus.subscribe(Topic.ORDER_FILL)
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

    def _on_fill(self, fill) -> None:
        """Handle fill confirmation — finalize entry or exit prices."""
        if fill.action == "ENTRY":
            if self._position and self._position.status == "open":
                _legs = getattr(fill, "legs", ["CE", "PE"])
                if "CE" in _legs:
                    self._position.ce_leg.ltp         = fill.ce_fill
                    self._position.ce_leg.entry_price = fill.ce_fill
                if "PE" in _legs:
                    self._position.pe_leg.ltp         = fill.pe_fill
                    self._position.pe_leg.entry_price = fill.pe_fill
                self._position.net_credit = self._position.ce_leg.entry_price + self._position.pe_leg.entry_price
                logger.info(
                    "SellStraddle[%s]: ENTRY confirmed — CE=%.2f PE=%.2f credit=%.2f [%s/%s] legs=%s",
                    self._underlying, self._position.ce_leg.entry_price, self._position.pe_leg.entry_price,
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
        _tick_count = 0
        _last_log_ts = 0.0
        import time as _time
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
                self._clog.info("OPT_TICKS: %d option ticks received in last 60s  CE=%.2f PE=%.2f",
                                _tick_count, self._ce_ltp, self._pe_ltp)
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
                if tick.option_type == "CE" and abs(tick.strike - pos.ce_leg.strike) < 0.01:
                    pos.ce_leg.ltp = tick.ltp
                elif tick.option_type == "PE" and abs(tick.strike - pos.pe_leg.strike) < 0.01:
                    pos.pe_leg.ltp = tick.ltp

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

        # New trading day — wipe all intraday state (sell_straddle is intraday only)
        if self._market_open_dt is not None and self._market_open_dt.date() != now.date():
            logger.info(
                "SellStraddle[%s]: new day detected (%s→%s) — resetting session state.",
                self._underlying,
                self._market_open_dt.date(), now.date(),
            )
            self.reset_session()

        # Record market-open for this session (first candle of the day)
        if self._market_open_dt is None or self._market_open_dt.date() != now.date():
            self._market_open_dt = now.replace(
                hour=_MARKET_OPEN.hour, minute=_MARKET_OPEN.minute,
                second=0, microsecond=0,
            )
            self._primed = False

        # Update buffers
        self._idx_highs.append(float(ev.high))
        self._idx_lows.append(float(ev.low))
        self._idx_closes.append(float(ev.close))
        combined = self._ce_ltp + self._pe_ltp
        if combined > 0:
            self._prem_closes.append(combined)
            self._prem_volumes.append(float(ev.volume) if ev.volume else 1.0)

        self._recompute_indicators()

        # Force-exit
        if now.time() >= self._force_exit:
            if self._position and self._position.status == "open":
                await self._close_position("time_exit_eod")
            return

        # Entry evaluation (no open position)
        if not self._position or self._position.status != "open":
            await self._try_entry(now)

        # Snapshot every cached leg's current ATP as its "previous closed" value
        # for the NEXT candle's per-pair slope. CRITICAL: this MUST run AFTER the
        # entry evaluation above — otherwise prev == current within the same candle
        # and every per-pair slope reads 0.00 (SLOPE<0 can never pass → no entries).
        # Only overwrite on a valid ATP so a missing tick never corrupts the slope.
        for _k, _v in self._strike_prem.items():
            _a = _v.get("atp", 0.0)
            if _a and _a > 0:
                self._prev_atp_closed[_k] = _a

    def _recompute_indicators(self) -> None:
        closes = np.array(self._prem_closes, dtype=np.float64)
        vols   = np.array(self._prem_volumes, dtype=np.float64)
        idx_h  = np.array(self._idx_highs,   dtype=np.float64)
        idx_l  = np.array(self._idx_lows,    dtype=np.float64)
        idx_c  = np.array(self._idx_closes,  dtype=np.float64)
        ltp = self._ce_ltp + self._pe_ltp
        self._ind["ltp"]   = ltp
        self._ind["close"] = ltp
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
        if self._ce_atp > 0 and self._pe_atp > 0:
            _cur_vwap = float(self._ce_atp + self._pe_atp)
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
        """Per-pair {close, vwap, slope} from the feed cache (broker ATP). None if not ready."""
        from strategies.straddle_selection import pair_indicators
        return pair_indicators(self._strike_prem, self._prev_atp_closed, ce_strike, pe_strike)

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
        remaining = int((ready_at - now).total_seconds() / 60)
        logger.debug(
            "SellStraddle[%s]: priming — %d min remaining (ready at %s)",
            self._underlying, remaining, ready_at.strftime("%H:%M"),
        )
        return False

    # ── Entry ─────────────────────────────────────────────────────────────────

    async def _try_entry(self, now: datetime) -> None:
        if self._stop_for_day:
            return  # Day profit-target or day-loss-SL already hit today
        if not (self._entry_start <= now.time() < self._entry_cutoff):
            return
        if self._trades_today >= self._max_trades:
            return
        if self._sl_cooldown_until and now < self._sl_cooldown_until:
            return
        if self._order_pending:
            return  # Waiting for fill confirmation from bridge
        if self._spot <= 0 or self._ce_ltp <= 0 or self._pe_ltp <= 0:
            self._clog.info(
                "WAIT  spot=%.2f CE_ltp=%.2f PE_ltp=%.2f — waiting for option ticks",
                self._spot, self._ce_ltp, self._pe_ltp,
            )
            return

        # NOTE: the ltp_target floor is enforced INSIDE selection (reference parity):
        # select_balanced_pair requires anchor>=target and partner>=target; scan_pool
        # requires both legs>=target. We deliberately do NOT pre-gate on the ATM
        # legs here — an ATM-premium pre-check would wrongly block valid non-ATM
        # balanced pairs (ITM strikes carry higher premium than ATM). The spot/ce/pe>0
        # wait above already guarantees ATM ticks exist, which selection needs.

        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        is_beginning = (self._trades_today == 0)

        # Workflow mode: hybrid (default) → beginning concept first trade, else pool.
        # _beginning_failed flips a hybrid first-trade pulse to the pool scan.
        workflow_mode = ss.get("entry_workflow_mode", "hybrid")
        if workflow_mode == "beginning_only":
            use_beginning = True
        elif workflow_mode == "reentry_only":
            use_beginning = False
        else:  # hybrid
            use_beginning = is_beginning and not self._beginning_failed

        rule_key = "entry_rules_beginning" if use_beginning else "entry_rules_reentry"
        rules    = ss.get(rule_key, [])

        if not self._is_primed(now, rules):
            self._clog.info(
                "EVAL %s [%s] PRIMING — waiting for indicator priming", self._underlying, rule_key,
            )
            return

        step   = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        offset = int(ss.get("v_slope_pool_offset") or ss.get("reentry_offset") or 4)
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0

        from strategies.straddle_selection import select_balanced_pair, scan_pool

        if use_beginning:
            sel = select_balanced_pair(self._strike_prem, self._spot, step, offset, ltp_target)
            concept = "beginning"
        else:
            sel = scan_pool(
                self._strike_prem, self._spot, step, offset, ltp_target,
                rule_pass=lambda cs, ps: _eval_rules(rules, self._pair_indicators(cs, ps) or {})[0],
                metric=ss.get("reentry_best_metric", "balanced_premium"),
            )
            concept = "reentry"

        if not sel:
            self._clog.info(
                "EVAL %s [%s] NO-PAIR — spot=%.2f no balanced pair (target=%.2f offset=%d)",
                self._underlying, rule_key, self._spot, ltp_target, offset,
            )
            return

        ce_strike, pe_strike, ce_ltp, pe_ltp = sel
        ind = self._pair_indicators(ce_strike, pe_strike) or dict(self._ind)
        passed, reason = _eval_rules(rules, ind)
        self._clog.info(
            "EVAL %s [%s/%s] sell CE%d=%.2f + PE%d=%.2f credit=%.2f | rules: %s | result=%s | ind=%s",
            self._underlying, rule_key, concept,
            ce_strike, ce_ltp, pe_strike, pe_ltp, ce_ltp + pe_ltp,
            reason, "PASS" if passed else "BLOCK",
            {k: round(v, 2) for k, v in ind.items()},
        )
        if not passed:
            # Hybrid: beginning concept failed its gate → next pulse uses the pool scan.
            if use_beginning and workflow_mode == "hybrid":
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

        # Create position immediately (paper fill will update entry prices)
        self._position = StraddlePosition(
            underlying        = self._underlying,
            atm_at_entry      = atm,
            entry_spot        = self._spot,
            ce_leg            = StraddleLeg("CE", ce_strike, ce_ltp, ce_ltp),
            pe_leg            = StraddleLeg("PE", pe_strike, pe_ltp, pe_ltp),
            net_credit        = ce_ltp + pe_ltp,
            open_time         = now,
            status            = "open",
            session_min_vwap  = self._ind.get("vwap", float("inf")),
            entry_indicators  = self._pair_indicators(ce_strike, pe_strike) or dict(self._ind),
            lot_size          = self._lot_size * self._lot_multiplier,
        )
        self._persist()   # survive restarts
        self._trades_today  += 1
        self._beginning_failed = False
        self._order_pending  = True
        # Lock initial credit as the denominator for all day-% calculations
        if self._initial_net_credit <= 0:
            self._initial_net_credit = ce_ltp + pe_ltp

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
        await self._bus.publish(Topic.ORDER_REQUEST, order_ev)

    # ── Exit ─────────────────────────────────────────────────────────────────

    async def _check_exits(self) -> None:
        pos = self._position
        if not pos:
            return
        now = datetime.now(IST)
        pnl = pos.unrealized_pnl

        # ── EOD FORCE SQUARE-OFF — highest priority, checked before all else ──────
        if now.time() >= self._force_exit:
            if self._position and self._position.status == "open":
                logger.info("SellStraddle[%s]: EOD SQUAREOFF — time=%s", self._underlying, now.strftime("%H:%M"))
                await self._close_position("eod_squareoff")
                self._stop_for_day = True
            return

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
        # total_day_pct = (all closed trades + running P&L) / initial credit × 100
        if self._initial_net_credit > 0:
            total_day_pts = self._session_realized_pnl_pts + pnl
            total_day_pct = total_day_pts / self._initial_net_credit * 100

            if self._day_profit_target_pct > 0 and total_day_pct >= self._day_profit_target_pct:
                logger.info(
                    "SellStraddle[%s]: DAY PROFIT TARGET — day=%.1f%% (≥%.1f%%) | "
                    "closed=%.2f running=%.2f credit=%.2f",
                    self._underlying, total_day_pct, self._day_profit_target_pct,
                    self._session_realized_pnl_pts, pnl, self._initial_net_credit,
                )
                await self._close_position("day_profit_target")
                self._stop_for_day = True
                logger.info("SellStraddle[%s]: STOPPED FOR DAY (profit target reached).", self._underlying)
                return

            if self._day_loss_sl_pct > 0 and total_day_pct <= -self._day_loss_sl_pct:
                logger.info(
                    "SellStraddle[%s]: DAY LOSS SL — day=%.1f%% (≤-%.1f%%) | "
                    "closed=%.2f running=%.2f credit=%.2f",
                    self._underlying, total_day_pct, self._day_loss_sl_pct,
                    self._session_realized_pnl_pts, pnl, self._initial_net_credit,
                )
                await self._close_position("day_loss_sl")
                self._stop_for_day = True
                logger.info("SellStraddle[%s]: STOPPED FOR DAY (loss SL hit).", self._underlying)
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
        #    LTP-decay → Ratio → Scalable TSL → ROC guardrail → VWAP-Rise → exit_rules.
        #    (No separate pct-based trailing SL — the reference uses Scalable TSL only.)

        # 6. Ratio exit → smart roll first
        if pos.ce_leg.ltp > 0 and pos.pe_leg.ltp > 0:
            ratio = max(pos.ce_leg.ltp, pos.pe_leg.ltp) / min(pos.ce_leg.ltp, pos.pe_leg.ltp)
            if ratio >= self._ratio_threshold:
                blown = "CE" if pos.ce_leg.ltp > pos.pe_leg.ltp else "PE"
                logger.info("SellStraddle[%s]: RATIO EXIT — %s ratio=%.2fx", self._underlying, blown, ratio)
                rolled = await self._try_smart_roll(now, "ratio_exit")
                if not rolled:
                    await self._close_position("ratio_exit")
                return

        # 7. Scalable TSL → smart roll first, then full exit
        if self._tsl_enabled:
            if self._check_scalable_tsl(pos, pnl):
                logger.info("SellStraddle[%s]: SCALABLE TSL — locked=₹%.0f pnl=₹%.0f", self._underlying, pos.tsl_high_lock_rs, self._pnl_rs(pnl))
                rolled = await self._try_smart_roll(now, "scalable_tsl")
                if not rolled:
                    await self._close_position("scalable_tsl")
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
                    rolled = await self._try_smart_roll(now, "guardrail_roc_target")
                    if not rolled:
                        await self._close_position("guardrail_roc_target")
                    return
                if _roc_val is not None and self._guardrail_roc_stoploss >= 0 and _roc_val >= self._guardrail_roc_stoploss:
                    logger.info(
                        "SellStraddle[%s]: ROC GUARDRAIL SL — roc=%.2f >= sl=%.2f",
                        self._underlying, _roc_val, self._guardrail_roc_stoploss,
                    )
                    rolled = await self._try_smart_roll(now, "guardrail_roc_sl")
                    if not rolled:
                        await self._close_position("guardrail_roc_sl")
                    return

        # 9. VWAP Rise SL → smart roll first
        if self._vwap_rise_enabled:
            curr_vwap = self._ind.get("vwap", 0)
            if curr_vwap > 0:
                if curr_vwap < pos.session_min_vwap:
                    pos.session_min_vwap = curr_vwap
                if pos.session_min_vwap < float("inf"):
                    rise_pct = (curr_vwap - pos.session_min_vwap) / pos.session_min_vwap * 100
                    if rise_pct >= self._vwap_rise_threshold:
                        logger.info(
                            "SellStraddle[%s]: VWAP RISE SL — rise=%.2f%% curr=%.2f low=%.2f",
                            self._underlying, rise_pct, curr_vwap, pos.session_min_vwap,
                        )
                        rolled = await self._try_smart_roll(now, "vwap_rise_sl")
                        if not rolled:
                            await self._close_position("vwap_rise_sl")
                        return

        # exit_rules — dynamic technical exit conditions from admin config
        if self._exit_rules:
            _max_tf = max((int(r.get("tf", 1)) for r in self._exit_rules), default=1)
            _er_bucket = f"{now.strftime('%Y%m%d_%H')}{(now.minute // _max_tf) * _max_tf:02d}"
            if _er_bucket != self._last_exit_rules_bucket:
                self._last_exit_rules_bucket = _er_bucket
                _passed, _reason = _eval_rules(self._exit_rules, self._ind)
                # Always log the exit-rule evaluation so you can see what's checked
                # on the open ATM position each timeframe bucket.
                self._clog.info(
                    "EXIT-EVAL %s ATM=%.0f pnl=%.2f (credit=%.2f) | rules: %s | result=%s",
                    self._underlying, pos.atm_at_entry, pnl, pos.net_credit,
                    _reason, "EXIT" if _passed else "HOLD",
                )
                if _passed:
                    logger.info(
                        "SellStraddle[%s]: EXIT_RULES triggered — %s",
                        self._underlying, _reason,
                    )
                    rolled = await self._try_smart_roll(now, "exit_rules")
                    if not rolled:
                        await self._close_position("exit_rules")
                    return

    def _pnl_rs(self, pnl_pts: float) -> float:
        """Convert P&L in premium points to rupees."""
        qty = self._lot_size * self._lot_multiplier
        return pnl_pts * qty

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
        qty_mult     = self._lot_multiplier
        base_profit  = self._tsl_base_profit_rs  * qty_mult
        base_lock    = self._tsl_base_lock_rs    * qty_mult
        step_profit  = self._tsl_step_profit_rs  * qty_mult
        step_lock    = self._tsl_step_lock_rs    * qty_mult

        profit_rs = self._pnl_rs(pnl_pts)

        if profit_rs >= base_profit and step_profit > 0:
            num_steps       = int((profit_rs - base_profit) // step_profit)
            calc_lock       = base_lock + num_steps * step_lock
            if calc_lock > pos.tsl_high_lock_rs:
                pos.tsl_high_lock_rs = calc_lock
                logger.debug(
                    "SellStraddle[%s]: TSL lock updated — ₹%.0f (profit=₹%.0f step=%d)",
                    self._underlying, calc_lock, profit_rs, num_steps,
                )

        if pos.tsl_high_lock_rs > 0 and profit_rs < pos.tsl_high_lock_rs:
            return True   # Exit
        return False

    # ── Smart Rolling ─────────────────────────────────────────────────────────

    async def _try_smart_roll(self, now: datetime, trigger: str) -> bool:
        """Reference exit_logic.perform_smart_roll — scan pool, then by per-leg strike:
        Virtual / Partial / Physical / Full-exit. Returns True (caller does not also close)."""
        from strategies.straddle_selection import scan_pool, classify_roll
        pos = self._position
        if not pos:
            return True
        if not self._is_in_entry_window(now) or self._trades_today >= self._max_trades:
            await self._close_position(f"full_exit_{trigger}")
            return True

        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        rules = ss.get("entry_rules_reentry", [])
        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        offset = int(ss.get("v_slope_pool_offset") or ss.get("reentry_offset") or 4)
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0

        sel = scan_pool(
            self._strike_prem, self._spot, step, offset, ltp_target,
            rule_pass=lambda cs, ps: _eval_rules(rules, self._pair_indicators(cs, ps) or {})[0],
            metric=ss.get("reentry_best_metric", "balanced_premium"),
        )
        ce_same = pe_same = False
        ce_s = pe_s = ce_l = pe_l = None
        if sel:
            ce_s, pe_s, ce_l, pe_l = sel
            ce_same = int(ce_s) == int(pos.ce_leg.strike)
            pe_same = int(pe_s) == int(pos.pe_leg.strike)

        outcome = classify_roll(ce_same, pe_same, has_candidates=bool(sel))
        logger.info("SellStraddle[%s]: SMART ROLL (%s) → %s | cand=%s",
                    self._underlying, trigger, outcome,
                    f"{ce_s}/{pe_s}" if sel else "none")

        if outcome == "full_exit":
            await self._close_position(f"full_exit_{trigger}")
            return True
        if outcome == "virtual":
            pos.ce_leg.entry_price = ce_l
            pos.pe_leg.entry_price = pe_l
            pos.ce_leg.ltp = ce_l
            pos.pe_leg.ltp = pe_l
            pos.net_credit = ce_l + pe_l
            pos.tsl_high_lock_rs = 0.0
            pos.peak_profit = 0.0
            pos.trailing_active = False
            pos.open_time = now
            pos.session_min_vwap = self._ind.get("vwap", float("inf"))
            self._persist()
            return True
        if outcome == "partial_pe":
            await self._single_side_roll_to("PE", pe_s, pe_l, now, trigger)
            return True
        if outcome == "partial_ce":
            await self._single_side_roll_to("CE", ce_s, ce_l, now, trigger)
            return True
        # physical — close both, open new pair
        await self._close_leg("CE", f"physical_roll_{trigger}", now)
        await self._close_leg("PE", f"physical_roll_{trigger}", now)
        self._position = StraddlePosition(
            underlying=self._underlying, atm_at_entry=round(self._spot / step) * step,
            entry_spot=self._spot,
            ce_leg=StraddleLeg("CE", ce_s, ce_l, ce_l),
            pe_leg=StraddleLeg("PE", pe_s, pe_l, pe_l),
            net_credit=ce_l + pe_l, open_time=now, status="open",
            session_min_vwap=self._ind.get("vwap", float("inf")),
            entry_indicators=dict(self._ind),
            lot_size=self._lot_size * self._lot_multiplier,
        )
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        self._event_counter += 1
        await self._bus.publish(Topic.ORDER_REQUEST, StraddleOrderEvent(
            action="ENTRY", underlying=self._underlying, atm=self._position.atm_at_entry,
            ce_strike=ce_s, pe_strike=pe_s, ce_ltp=ce_l, pe_ltp=pe_l,
            lot_multiplier=self._lot_multiplier, lot_size=self._lot_size, spot=self._spot,
            indicators=dict(self._ind),
            event_id=f"{self._underlying}_PHYSROLL_{self._event_counter}",
        ))
        self._persist()
        logger.info("SellStraddle[%s]: PHYSICAL ROLL → CE%s PE%s", self._underlying, ce_s, pe_s)
        return True

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
        leg_pnl = leg.entry_price - leg.ltp  # short option: credit - buyback
        self._event_counter += 1
        order_ev = StraddleOrderEvent(
            action="EXIT", underlying=self._underlying, atm=pos.atm_at_entry,
            ce_strike=pos.ce_leg.strike, pe_strike=pos.pe_leg.strike,
            ce_ltp=pos.ce_leg.ltp, pe_ltp=pos.pe_leg.ltp,
            lot_multiplier=self._lot_multiplier, lot_size=self._lot_size,
            spot=self._spot, close_reason=reason, realized_pnl=leg_pnl,
            event_id=f"{self._underlying}_EXITLEG_{side}_{self._event_counter}",
            legs=[side],
        )
        await self._bus.publish(Topic.ORDER_REQUEST, order_ev)
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
        await self._bus.publish(Topic.ORDER_REQUEST, order_ev)
        logger.info("SellStraddle[%s]: OPEN LEG %s strike=%.0f @%.2f [%s]",
                    self._underlying, side, strike, ltp, reason)

    async def _single_side_roll(self, side: str, now: datetime, reason: str) -> None:
        """Close decayed leg, re-enter that side via pool scan; enforce 0-or-2 invariant."""
        from strategies.straddle_selection import scan_pool
        other = "PE" if side == "CE" else "CE"
        pos = self._position
        if not pos:
            return
        await self._close_leg(side, reason, now)

        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        rules = ss.get("entry_rules_reentry", [])
        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        offset = int(ss.get("v_slope_pool_offset") or ss.get("reentry_offset") or 4)
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0

        sel = scan_pool(
            self._strike_prem, self._spot, step, offset, ltp_target,
            rule_pass=lambda cs, ps: _eval_rules(rules, self._pair_indicators(cs, ps) or {})[0],
            metric=ss.get("reentry_best_metric", "balanced_premium"),
        )
        new_strike = new_ltp = None
        if sel:
            ce_s, pe_s, ce_l, pe_l = sel
            new_strike, new_ltp = (ce_s, ce_l) if side == "CE" else (pe_s, pe_l)

        if new_strike and new_ltp and new_ltp >= ltp_target:
            await self._open_leg(side, new_strike, new_ltp, now, f"single_side_roll_{reason}")
            self._persist()
            return
        # 0-or-2 invariant — no valid re-entry → close the surviving leg too.
        logger.warning("SellStraddle[%s]: single-side roll %s found no candidate — closing %s (0-or-2).",
                       self._underlying, side, other)
        await self._close_leg(other, f"single_side_cleanup_{reason}", now)
        self._position = None
        self._persist()

    async def _single_side_roll_to(self, side: str, strike: int, ltp: float, now: datetime, reason: str) -> None:
        """Partial roll: close one side and open a pre-selected candidate strike on that side.
        0-or-2 invariant: if ltp invalid, close the surviving leg too."""
        other = "PE" if side == "CE" else "CE"
        await self._close_leg(side, f"partial_roll_{reason}", now)
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0
        if strike and ltp and ltp >= ltp_target:
            await self._open_leg(side, strike, ltp, now, f"partial_roll_{reason}")
            self._persist()
            return
        logger.warning("SellStraddle[%s]: partial roll %s invalid candidate — closing %s (0-or-2).",
                       self._underlying, side, other)
        await self._close_leg(other, f"partial_cleanup_{reason}", now)
        self._position = None
        self._persist()

    async def _close_position(self, reason: str) -> None:
        if not self._position:
            return
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        pos = self._position
        pos.realized_pnl = pos.unrealized_pnl
        pos.close_reason  = reason
        pos.close_time    = datetime.now(IST)
        pos.status        = "closed"

        logger.info(
            "SellStraddle[%s]: CLOSED — reason=%s pnl=₹%.0f (%.2f pts) "
            "CE %.2f→%.2f PE %.2f→%.2f",
            self._underlying, reason,
            self._pnl_rs(pos.realized_pnl), pos.realized_pnl,
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
            event_id       = f"{self._underlying}_EXIT_{self._event_counter}",
        )
        await self._bus.publish(Topic.ORDER_REQUEST, order_ev)

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
        if reason == "stop_loss":
            self._apply_sl_cooldown()

    def _apply_sl_cooldown(self) -> None:
        ss    = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        rules = ss.get("entry_rules_beginning", []) + ss.get("entry_rules_reentry", [])
        tfs   = [int(r.get("tf", 5)) for r in rules if r.get("tf")]
        max_tf = max(tfs) if tfs else 5
        cooldown_min = int(max_tf * self._sl_cooldown_tf_mult)
        if cooldown_min > 0:
            self._sl_cooldown_until = datetime.now(IST) + timedelta(minutes=cooldown_min)
            logger.info("SellStraddle[%s]: SL cooldown %d min.", self._underlying, cooldown_min)

    def _is_in_entry_window(self, now: datetime) -> bool:
        return self._entry_start <= now.time() < self._entry_cutoff

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


def _eval_rules(rules: List[dict], ind: Dict[str, float]) -> Tuple[bool, str]:
    """
    Evaluate admin rule-builder rules against current indicator values.
    Supports AND/OR with brackets — identical to old Rust-bridge token evaluator,
    but implemented in pure Python.
    """
    if not rules:
        return True, "No rules — always allowed"

    tokens:  List[str] = []
    reasons: List[str] = []

    for i, rule in enumerate(rules):
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
