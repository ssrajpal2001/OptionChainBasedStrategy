"""
strategies/sell_straddle.py — ATM Straddle / Strangle selling strategy.

All thresholds are read from RuntimeConfig at runtime — nothing is hardcoded.
The admin can change any parameter via the Strategy Admin UI and it takes
effect on the next candle evaluation without a restart.

Entry Conditions:
  • Configurable entry time window (default 09:20 – 12:00 IST).
  • RSI between rsi_min–rsi_max (default 35–65).
  • ADX < adx_max (default 30).
  • Max N trades per session (default 1).

Exit Conditions (any one triggers full close):
  1. Profit target: net_premium × profit_pct/100.
  2. Stop loss: net_premium × sl_pct/100.
  3. Trailing SL: activates after trail_lock_pct% profit; trails at trail_floor_pct% floor.
  4. Time exit: squareoff_time IST.
  5. ROC guardrail: spot moves > roc_limit_pct% in a single tick.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Optional

from config.global_config import IST, Topic
from data_layer.base_feeder import EventBus, CandleEvent
from data_layer.runtime_config import RuntimeConfig

logger = logging.getLogger(__name__)


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class StraddleLeg:
    option_type: str     # "CE" | "PE"
    strike: float
    entry_price: float
    ltp: float = 0.0
    filled: bool = False
    fill_time: Optional[datetime] = None


@dataclass
class StraddlePosition:
    underlying: str
    expiry: date
    atm_at_entry: float
    entry_spot: float
    ce_leg: StraddleLeg = field(default_factory=lambda: StraddleLeg("CE", 0, 0))
    pe_leg: StraddleLeg = field(default_factory=lambda: StraddleLeg("PE", 0, 0))

    net_credit: float = 0.0
    peak_profit: float = 0.0
    trailing_active: bool = False
    open_time: Optional[datetime] = None
    close_time: Optional[datetime] = None
    close_reason: str = ""
    realized_pnl: float = 0.0
    status: str = "open"

    # Greeks snapshot at entry
    entry_rsi: float = 0.0
    entry_adx: float = 0.0

    # Runtime thresholds captured at position open (so mid-trade changes don't
    # shift the goalposts on an already-open trade)
    _profit_pct:      float = field(default=0.30, repr=False)
    _sl_pct:          float = field(default=2.00, repr=False)
    _trail_lock_pct:  float = field(default=0.20, repr=False)
    _trail_floor_pct: float = field(default=0.10, repr=False)
    # Capital-based profit target in ₹ (0 = fall back to credit-based)
    _capital_target:  float = field(default=0.0,  repr=False)

    @property
    def profit_target(self) -> float:
        if self._capital_target > 0:
            return self._capital_target
        return self.net_credit * self._profit_pct

    @property
    def stop_loss_limit(self) -> float:
        return self.net_credit * self._sl_pct

    @property
    def current_value(self) -> float:
        return self.ce_leg.ltp + self.pe_leg.ltp

    @property
    def unrealized_pnl(self) -> float:
        return self.net_credit - self.current_value


# ── Strategy Engine ───────────────────────────────────────────────────────────

class SellStraddleStrategy:
    """
    ATM straddle seller with trailing SL and ROC guardrail.
    All thresholds read from RuntimeConfig — fully reconfigurable at runtime.
    """

    def __init__(
        self,
        bus: EventBus,
        cfg=None,
        underlying: str = "NIFTY",
        lot_multiplier: int = 1,
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._underlying = underlying
        self._lot_multiplier = lot_multiplier
        self._running = False
        self._position: Optional[StraddlePosition] = None
        self._spot: float = 0.0
        self._atm: float = 0.0
        self._rsi: float = 50.0
        self._adx: float = 0.0
        self._trades_today: int = 0
        self._prev_spot: float = 0.0
        self._tasks: list = []
        self._sl_cooldown_until: Optional[datetime] = None
        self._pending_capital_target: float = 0.0

        # Runtime-configurable thresholds — updated by reconfigure()
        self._load_thresholds()

    def _load_thresholds(self) -> None:
        """Pull current thresholds from RuntimeConfig into instance attributes."""
        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        self._entry_start    = _parse_time(ss.get("entry_start",    "09:20"))
        self._entry_cutoff   = _parse_time(ss.get("entry_end",      "12:00"))
        self._force_exit     = _parse_time(ss.get("squareoff_time", "15:15"))
        self._rsi_min        = float(ss.get("rsi_min",         35.0))
        self._rsi_max        = float(ss.get("rsi_max",         65.0))
        self._adx_max        = float(ss.get("adx_max",         30.0))
        self._profit_pct     = float(ss.get("profit_pct",      30.0)) / 100.0
        self._sl_pct         = float(ss.get("sl_pct",         200.0)) / 100.0
        self._trail_lock_pct = float(ss.get("trail_lock_pct",  20.0)) / 100.0
        self._trail_floor_pct= float(ss.get("trail_floor_pct", 10.0)) / 100.0
        self._max_trades     = int(ss.get("max_trades", 1))
        self._roc_limit_pct  = float(ss.get("roc_limit_pct", 1.5))
        self._sl_cooldown_tf_mult = float(ss.get("sl_cooldown_tf_multiplier", 1.0))
        self._capital_deployed_inr = float(ss.get("capital_deployed_inr", 0))

    def reconfigure(self) -> None:
        """Live-reload thresholds from RuntimeConfig without restarting."""
        self._load_thresholds()
        logger.info(
            "SellStraddle[%s]: reconfigured — rsi=%.0f–%.0f adx<%.0f profit=%.0f%% sl=%.0f%%",
            self._underlying,
            self._rsi_min, self._rsi_max, self._adx_max,
            self._profit_pct * 100, self._sl_pct * 100,
        )

    def start(self) -> None:
        self._running = True
        self._tasks = [
            asyncio.create_task(self._candle_loop(),  name=f"ss_{self._underlying}_candle"),
            asyncio.create_task(self._tick_loop(),    name=f"ss_{self._underlying}_tick"),
            asyncio.create_task(self._option_loop(),  name=f"ss_{self._underlying}_opt"),
        ]
        logger.info("SellStraddleStrategy[%s]: started.", self._underlying)

    def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            if not t.done():
                t.cancel()
        logger.info("SellStraddleStrategy[%s]: stopped.", self._underlying)

    def reset_session(self) -> None:
        self._trades_today = 0
        self._position = None
        self._sl_cooldown_until = None
        self._pending_capital_target = 0.0
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
            if not self._position or tick.underlying != self._underlying:
                continue
            if abs(tick.strike - self._position.atm_at_entry) < 0.01:
                if tick.option_type == "CE":
                    self._position.ce_leg.ltp = tick.ltp
                elif tick.option_type == "PE":
                    self._position.pe_leg.ltp = tick.ltp

    # ── Entry logic ───────────────────────────────────────────────────────────

    async def _on_candle(self, ev: CandleEvent) -> None:
        now = datetime.now(IST)

        # Reload thresholds on every candle so config changes take effect live
        self._load_thresholds()

        # Force-exit
        if now.time() >= self._force_exit and self._position and self._position.status == "open":
            await self._close_position("time_exit_eod")
            return

        if not (self._entry_start <= now.time() < self._entry_cutoff):
            return

        if self._position and self._position.status == "open":
            return

        if self._trades_today >= self._max_trades:
            return

        if self._sl_cooldown_until and now < self._sl_cooldown_until:
            remaining = int((self._sl_cooldown_until - now).total_seconds() / 60)
            logger.debug(
                "SellStraddle[%s]: entry blocked — SL cooldown active, %d min remaining.",
                self._underlying, remaining,
            )
            return

        if not (self._rsi_min <= self._rsi <= self._rsi_max):
            logger.debug(
                "SellStraddle[%s]: entry blocked — RSI=%.1f (need %.0f–%.0f).",
                self._underlying, self._rsi, self._rsi_min, self._rsi_max,
            )
            return
        if self._adx >= self._adx_max:
            logger.debug(
                "SellStraddle[%s]: entry blocked — ADX=%.1f (need < %.0f).",
                self._underlying, self._adx, self._adx_max,
            )
            return

        if self._spot <= 0:
            return

        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        atm  = round(self._spot / step) * step

        self._pending_capital_target = self._compute_capital_target(now)
        logger.info(
            "SellStraddle[%s]: entry signal — ATM=%.0f RSI=%.1f ADX=%.1f capital_target=₹%.0f",
            self._underlying, atm, self._rsi, self._adx, self._pending_capital_target,
        )
        logger.info(
            "SellStraddle[%s]: ORDER INTENT — SELL %s%.0fCE + SELL %s%.0fPE (×%d lot)",
            self._underlying,
            self._underlying, atm,
            self._underlying, atm,
            self._lot_multiplier,
        )

    # ── Exit logic ────────────────────────────────────────────────────────────

    async def _check_exits(self) -> None:
        pos = self._position
        if not pos:
            return

        now = datetime.now(IST)

        if now.time() >= self._force_exit:
            await self._close_position("time_exit")
            return

        pnl = pos.unrealized_pnl

        if pnl > pos.peak_profit:
            pos.peak_profit = pnl
            if pnl >= pos.net_credit * pos._trail_lock_pct:
                pos.trailing_active = True

        if pos.trailing_active:
            trail_floor = pos.peak_profit - pos.net_credit * pos._trail_floor_pct
            if pnl < trail_floor:
                logger.info(
                    "SellStraddle[%s]: trailing SL hit — pnl=%.2f trail_floor=%.2f",
                    self._underlying, pnl, trail_floor,
                )
                await self._close_position("trailing_sl")
                return

        if pnl >= pos.profit_target:
            logger.info(
                "SellStraddle[%s]: profit target hit — pnl=%.2f target=%.2f",
                self._underlying, pnl, pos.profit_target,
            )
            await self._close_position("profit_target")
            return

        if -pnl >= pos.stop_loss_limit:
            logger.info(
                "SellStraddle[%s]: stop loss hit — loss=%.2f limit=%.2f",
                self._underlying, -pnl, pos.stop_loss_limit,
            )
            await self._close_position("stop_loss")
            return

        if self._prev_spot > 0:
            roc = abs(self._spot - self._prev_spot) / self._prev_spot * 100
            if roc > self._roc_limit_pct:
                logger.warning(
                    "SellStraddle[%s]: ROC guardrail — move=%.2f%% > limit=%.2f%%",
                    self._underlying, roc, self._roc_limit_pct,
                )
                await self._close_position("roc_guardrail")
                return

        self._prev_spot = self._spot

    async def _close_position(self, reason: str) -> None:
        if not self._position:
            return
        pos = self._position
        pos.realized_pnl = pos.unrealized_pnl
        pos.close_reason  = reason
        pos.close_time    = datetime.now(IST)
        pos.status        = "closed"
        logger.info(
            "SellStraddle[%s]: position closed — reason=%s pnl=%.2f",
            self._underlying, reason, pos.realized_pnl,
        )
        self._position = None
        if reason == "stop_loss":
            self._apply_sl_cooldown()

    def _apply_sl_cooldown(self) -> None:
        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        rules = ss.get("entry_rules_beginning", []) + ss.get("entry_rules_reentry", [])
        tfs = [int(r.get("tf", 5)) for r in rules if r.get("tf")]
        max_tf = max(tfs) if tfs else 5
        cooldown_minutes = max_tf * self._sl_cooldown_tf_mult
        if cooldown_minutes > 0:
            self._sl_cooldown_until = datetime.now(IST) + timedelta(minutes=cooldown_minutes)
            logger.info(
                "SellStraddle[%s]: SL cooldown set — %d min (max TF=%d × %.1f)",
                self._underlying, cooldown_minutes, max_tf, self._sl_cooldown_tf_mult,
            )

    def _compute_capital_target(self, now: datetime) -> float:
        """Return ₹ profit target based on capital_deployed_inr × per-day or global pct."""
        if self._capital_deployed_inr <= 0:
            return 0.0
        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        day_name = now.strftime("%A").lower()
        day_cfg = ss.get("per_day", {}).get(day_name, {})
        pct = float(day_cfg.get("profit_target_pct", 0))
        if pct <= 0:
            pct = float(ss.get("profit_pct", 30.0))
        return self._capital_deployed_inr * pct / 100.0

    # ── Public accessors ─────────────────────────────────────────────────────

    def update_indicators(self, rsi: float, adx: float, atm: float) -> None:
        self._rsi = rsi
        self._adx = adx
        self._atm = atm

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
    def entry_allowed(self) -> bool:
        """True when the time window is open and session trade limit not reached."""
        from datetime import datetime
        now = datetime.now(IST).time()
        if not (self._entry_start <= now < self._entry_cutoff):
            return False
        if self._trades_today >= self._max_trades:
            return False
        if self._sl_cooldown_until and datetime.now(IST) < self._sl_cooldown_until:
            return False
        return True


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_time(s: str) -> dtime:
    try:
        h, m = s.split(":")
        return dtime(int(h), int(m))
    except Exception:
        return dtime(15, 15)
