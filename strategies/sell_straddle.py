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

logger = logging.getLogger(__name__)

_BUF             = 600    # ring-buffer depth ≥ VWAP_WINDOW(500)
_MARKET_OPEN     = dtime(9, 15)   # NSE session start


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
    peak_profit: float = 0.0
    trailing_active: bool = False
    tsl_high_lock_rs: float = 0.0  # Highest TSL lock reached in ₹

    open_time: Optional[datetime] = None
    close_time: Optional[datetime] = None
    close_reason: str = ""
    realized_pnl: float = 0.0
    status: str = "open"           # "open" | "closed"

    entry_indicators: Dict[str, float] = field(default_factory=dict)

    # Session VWAP tracking for VWAP Rise SL
    session_min_vwap: float = float("inf")

    # Thresholds frozen at entry
    _profit_pct:      float = field(default=0.30, repr=False)
    _sl_pct:          float = field(default=2.00, repr=False)
    _trail_lock_pct:  float = field(default=0.20, repr=False)
    _trail_floor_pct: float = field(default=0.10, repr=False)
    _capital_target:  float = field(default=0.0,  repr=False)

    @property
    def profit_target(self) -> float:
        return self._capital_target if self._capital_target > 0 else self.net_credit * self._profit_pct

    @property
    def stop_loss_limit(self) -> float:
        return self.net_credit * self._sl_pct

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
        self._prev_spot: float = 0.0
        self._ce_ltp: float    = 0.0
        self._pe_ltp: float    = 0.0

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
        self._entry_cutoff    = _parse_time(ss.get("entry_end",      "12:00"))
        self._force_exit      = _parse_time(ss.get("squareoff_time", "15:15"))
        self._profit_pct      = float(ss.get("profit_pct",      30.0)) / 100.0
        self._sl_pct          = float(ss.get("sl_pct",         200.0)) / 100.0
        self._trail_lock_pct  = float(ss.get("trail_lock_pct",  20.0)) / 100.0
        self._trail_floor_pct = float(ss.get("trail_floor_pct", 10.0)) / 100.0
        self._max_trades      = int(ss.get("max_trades", 1))
        self._roc_limit_pct   = float(ss.get("roc_limit_pct", 1.5))
        self._sl_cooldown_tf_mult = float(ss.get("sl_cooldown_tf_multiplier", 1.0))
        self._capital_deployed_inr = float(ss.get("capital_deployed_inr", 0))
        self._lot_size        = int(ss.get("lot_size", 50))
        self._smart_roll_enabled = bool(ss.get("smart_rolling_enabled", True))

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

    def reconfigure(self) -> None:
        self._load_thresholds()
        logger.info("SellStraddle[%s]: reconfigured.", self._underlying)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        self._tasks = [
            asyncio.create_task(self._candle_loop(), name=f"ss_{self._underlying}_candle"),
            asyncio.create_task(self._tick_loop(),   name=f"ss_{self._underlying}_tick"),
            asyncio.create_task(self._option_loop(), name=f"ss_{self._underlying}_opt"),
            asyncio.create_task(self._fill_loop(),   name=f"ss_{self._underlying}_fill"),
        ]
        logger.info("SellStraddleStrategy[%s]: started.", self._underlying)

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
            await self._on_candle(ev)

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
                # Update with actual fill prices (matters in live mode)
                self._position.ce_leg.ltp         = fill.ce_fill
                self._position.pe_leg.ltp         = fill.pe_fill
                self._position.ce_leg.entry_price = fill.ce_fill
                self._position.pe_leg.entry_price = fill.pe_fill
                self._position.net_credit         = fill.ce_fill + fill.pe_fill
                logger.info(
                    "SellStraddle[%s]: ENTRY confirmed — CE=%.2f PE=%.2f credit=%.2f [%s/%s]",
                    self._underlying, fill.ce_fill, fill.pe_fill,
                    fill.ce_fill + fill.pe_fill, fill.client_id, fill.binding_id,
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
        while self._running:
            try:
                tick: OptionTick = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if tick.underlying != self._underlying:
                continue
            if tick.option_type == "CE":
                self._ce_ltp = tick.ltp
            elif tick.option_type == "PE":
                self._pe_ltp = tick.ltp
            if self._position and self._position.status == "open":
                if abs(tick.strike - self._position.atm_at_entry) < 0.01:
                    if tick.option_type == "CE":
                        self._position.ce_leg.ltp = tick.ltp
                    elif tick.option_type == "PE":
                        self._position.pe_leg.ltp = tick.ltp

    # ── Candle processing ─────────────────────────────────────────────────────

    async def _on_candle(self, ev: CandleEvent) -> None:
        now = datetime.now(IST)
        self._load_thresholds()

        # Record market-open once per session (first candle of the day)
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
        if len(closes) >= 2:
            self._ind["vwap"] = vwap(closes, closes, closes, vols)
        if len(closes) >= 9:
            self._ind["ema_fast"] = ema(closes, 9)
        if len(closes) >= 21:
            self._ind["ema_slow"] = ema(closes, 21)
        if len(idx_c) >= 42:
            adx_val, pdi_val, mdi_val = adx(idx_h, idx_l, idx_c)
            self._ind["adx"] = adx_val
            self._ind["pdi"] = pdi_val
            self._ind["mdi"] = mdi_val

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
            r.get("indicator", "").lower() in slope_names or
            r.get("operand1", "").lower() in slope_names or
            r.get("operand2", "").lower() in slope_names
            for r in rules
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
            return

        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        is_beginning = (self._trades_today == 0)
        rule_key = "entry_rules_beginning" if is_beginning else "entry_rules_reentry"
        rules    = ss.get(rule_key, [])

        if not self._is_primed(now, rules):
            return

        passed, reason = _eval_rules(rules, self._ind)
        if not passed:
            logger.debug("SellStraddle[%s]: %s blocked — %s", self._underlying, rule_key, reason)
            return

        await self._open_position(now, ss, rule_key, reason)

    async def _open_position(
        self, now: datetime, ss: dict, rule_key: str, reason: str,
    ) -> None:
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        atm  = round(self._spot / step) * step
        capital_target = self._compute_capital_target(now)

        self._event_counter += 1
        event_id = f"{self._underlying}_ENTRY_{self._event_counter}"

        # Create position immediately (paper fill will update entry prices)
        self._position = StraddlePosition(
            underlying        = self._underlying,
            atm_at_entry      = atm,
            entry_spot        = self._spot,
            ce_leg            = StraddleLeg("CE", atm, self._ce_ltp, self._ce_ltp),
            pe_leg            = StraddleLeg("PE", atm, self._pe_ltp, self._pe_ltp),
            net_credit        = self._ce_ltp + self._pe_ltp,
            open_time         = now,
            status            = "open",
            session_min_vwap  = self._ind.get("vwap", float("inf")),
            entry_indicators  = dict(self._ind),
            _profit_pct       = self._profit_pct,
            _sl_pct           = self._sl_pct,
            _trail_lock_pct   = self._trail_lock_pct,
            _trail_floor_pct  = self._trail_floor_pct,
            _capital_target   = capital_target,
        )
        self._trades_today  += 1
        self._prev_spot      = self._spot
        self._order_pending  = True
        # Lock initial credit as the denominator for all day-% calculations
        if self._initial_net_credit <= 0:
            self._initial_net_credit = self._ce_ltp + self._pe_ltp

        logger.info(
            "SellStraddle[%s]: ENTERED — ATM=%.0f CE=%.2f PE=%.2f credit=%.2f "
            "target=%.2f sl=%.2f | %s=PASS [%s]",
            self._underlying, atm,
            self._ce_ltp, self._pe_ltp, self._position.net_credit,
            self._position.profit_target, self._position.stop_loss_limit,
            rule_key, reason,
        )

        # Publish to StraddleExecutionBridge → paper/live fill
        order_ev = StraddleOrderEvent(
            action         = "ENTRY",
            underlying     = self._underlying,
            atm            = atm,
            ce_strike      = atm,
            pe_strike      = atm,
            ce_ltp         = self._ce_ltp,
            pe_ltp         = self._pe_ltp,
            lot_multiplier = self._lot_multiplier,
            lot_size       = self._lot_size,
            spot           = self._spot,
            indicators     = dict(self._ind),
            event_id       = event_id,
        )
        self._bus.publish(Topic.ORDER_REQUEST, order_ev)

    # ── Exit ─────────────────────────────────────────────────────────────────

    async def _check_exits(self) -> None:
        pos = self._position
        if not pos:
            return
        now = datetime.now(IST)
        pnl = pos.unrealized_pnl

        # ── DAY-LEVEL % GUARDRAILS (highest priority, stops trading for the day) ──
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

        # 1. Time exit
        if now.time() >= self._force_exit:
            await self._close_position("time_exit")
            return

        # 2. Update trailing SL (basic % of credit)
        if pnl > pos.peak_profit:
            pos.peak_profit = pnl
            if pnl >= pos.net_credit * pos._trail_lock_pct:
                pos.trailing_active = True
        if pos.trailing_active:
            trail_floor = pos.peak_profit - pos.net_credit * pos._trail_floor_pct
            if pnl < trail_floor:
                logger.info("SellStraddle[%s]: TRAILING SL — pnl=%.2f floor=%.2f", self._underlying, pnl, trail_floor)
                await self._close_position("trailing_sl")
                return

        # 3. Scalable TSL → smart roll first, then full exit
        if self._tsl_enabled:
            if self._check_scalable_tsl(pos, pnl):
                logger.info("SellStraddle[%s]: SCALABLE TSL — locked=₹%.0f pnl=₹%.0f", self._underlying, pos.tsl_high_lock_rs, self._pnl_rs(pnl))
                rolled = await self._try_smart_roll(now, "scalable_tsl")
                if not rolled:
                    await self._close_position("scalable_tsl")
                return

        # 4. Profit target → smart roll first
        if pnl >= pos.profit_target:
            logger.info("SellStraddle[%s]: PROFIT TARGET — pnl=%.2f target=%.2f", self._underlying, pnl, pos.profit_target)
            rolled = await self._try_smart_roll(now, "profit_target")
            if not rolled:
                await self._close_position("profit_target")
            return

        # 5. Stop loss — direct exit, no roll (need to get out immediately)
        if -pnl >= pos.stop_loss_limit:
            logger.info("SellStraddle[%s]: STOP LOSS — loss=%.2f limit=%.2f", self._underlying, -pnl, pos.stop_loss_limit)
            await self._close_position("stop_loss")
            return

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

        # 7. VWAP Rise SL → smart roll first
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

        # 8. ROC guardrail → smart roll first
        if self._prev_spot > 0:
            roc_pct = abs(self._spot - self._prev_spot) / self._prev_spot * 100
            if roc_pct > self._roc_limit_pct:
                logger.warning("SellStraddle[%s]: ROC GUARDRAIL — %.2f%%", self._underlying, roc_pct)
                rolled = await self._try_smart_roll(now, "roc_guardrail")
                if not rolled:
                    await self._close_position("roc_guardrail")
                return

        self._prev_spot = self._spot

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
        """
        After profit target or ratio exit, try to re-enter immediately
        using entry_rules_reentry instead of waiting for next candle.

        Virtual roll  — same ATM strike → refresh entry prices, keep position open
        Physical roll — new ATM differs → close old, open new immediately

        Returns True if rolled (no further action needed), False if caller should close.
        """
        if not self._smart_roll_enabled:
            return False
        if self._trades_today >= self._max_trades:
            return False
        if not self._is_in_entry_window(now):
            return False

        ss    = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        rules = ss.get("entry_rules_reentry", [])
        passed, reason = _eval_rules(rules, self._ind)

        if not passed:
            logger.info("SellStraddle[%s]: Smart roll REJECTED — %s [%s]", self._underlying, trigger, reason)
            return False

        step    = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        new_atm = round(self._spot / step) * step
        pos     = self._position

        if new_atm == pos.atm_at_entry:
            # ── VIRTUAL ROLL: same strike, refresh entry prices ───────────────
            logger.info(
                "SellStraddle[%s]: VIRTUAL ROLL (%s) — same ATM %.0f, refreshing entry prices. "
                "CE %.2f→%.2f PE %.2f→%.2f | [%s]",
                self._underlying, trigger, new_atm,
                pos.ce_leg.entry_price, self._ce_ltp,
                pos.pe_leg.entry_price, self._pe_ltp,
                reason,
            )
            pos.ce_leg.entry_price = self._ce_ltp
            pos.pe_leg.entry_price = self._pe_ltp
            pos.net_credit         = self._ce_ltp + self._pe_ltp
            pos.peak_profit        = 0.0
            pos.trailing_active    = False
            pos.tsl_high_lock_rs   = 0.0
            pos.open_time          = now
            pos.session_min_vwap   = self._ind.get("vwap", float("inf"))
            # Frozen thresholds stay the same — new position on same strikes
            # COUNT as a new trade for max_trades tracking
            self._trades_today += 1
        else:
            # ── PHYSICAL ROLL: new ATM, close old and open new ────────────────
            logger.info(
                "SellStraddle[%s]: PHYSICAL ROLL (%s) — ATM %.0f→%.0f | [%s]",
                self._underlying, trigger, pos.atm_at_entry, new_atm, reason,
            )
            # Record partial close P&L before replacing position
            pos.realized_pnl = pos.unrealized_pnl
            pos.close_reason  = f"physical_roll_{trigger}"
            pos.close_time    = now
            pos.status        = "closed"
            logger.info(
                "SellStraddle[%s]: Physical roll close — pnl=%.2f",
                self._underlying, pos.realized_pnl,
            )
            logger.info(
                "SellStraddle[%s]: ORDER INTENT — BUY %s%.0fCE + BUY %s%.0fPE (close)",
                self._underlying,
                self._underlying, pos.atm_at_entry,
                self._underlying, pos.atm_at_entry,
            )
            # Open new position immediately (no cooldown for rolls)
            capital_target = self._compute_capital_target(now)
            self._position = StraddlePosition(
                underlying        = self._underlying,
                atm_at_entry      = new_atm,
                entry_spot        = self._spot,
                ce_leg            = StraddleLeg("CE", new_atm, self._ce_ltp, self._ce_ltp),
                pe_leg            = StraddleLeg("PE", new_atm, self._pe_ltp, self._pe_ltp),
                net_credit        = self._ce_ltp + self._pe_ltp,
                open_time         = now,
                status            = "open",
                session_min_vwap  = self._ind.get("vwap", float("inf")),
                entry_indicators  = dict(self._ind),
                _profit_pct       = self._profit_pct,
                _sl_pct           = self._sl_pct,
                _trail_lock_pct   = self._trail_lock_pct,
                _trail_floor_pct  = self._trail_floor_pct,
                _capital_target   = capital_target,
            )
            self._trades_today += 1
            logger.info(
                "SellStraddle[%s]: ORDER INTENT — SELL %s%.0fCE + SELL %s%.0fPE (roll re-entry)",
                self._underlying,
                self._underlying, new_atm, self._underlying, new_atm,
            )

        return True  # Rolled — caller should NOT also close

    # ── Close ─────────────────────────────────────────────────────────────────

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
        self._bus.publish(Topic.ORDER_REQUEST, order_ev)

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

    def _compute_capital_target(self, now: datetime) -> float:
        if self._capital_deployed_inr <= 0:
            return 0.0
        ss       = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        day_name = now.strftime("%A").lower()
        day_cfg  = ss.get("per_day", {}).get(day_name, {})
        pct = float(day_cfg.get("profit_target_pct", 0))
        if pct <= 0:
            pct = float(ss.get("profit_pct", 30.0))
        return self._capital_deployed_inr * pct / 100.0

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
