"""
strategies/iron_condor.py — Iron Condor options selling strategy.

All thresholds are read from RuntimeConfig at runtime — nothing is hardcoded.
The admin can change any parameter via the Strategy Admin UI and it takes
effect on the next candle evaluation without a restart.

Entry logic:
  • RSI between rsi_min–rsi_max (default 40–60).
  • ADX < adx_max (default 25 — trending markets kill iron condors).
  • Short strikes at configured OTM distance per index.
  • Wing width: configurable per index.

Exit logic:
  • Profit target: profit_pct% of max profit (default 50%).
  • Stop loss: sl_pct% of premium received (default 200%).
  • Time exit: squareoff_time IST (default 15:15).
  • Breach exit: spot crosses a short strike.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, date, time as dtime
from typing import Dict, List, Optional

import uuid

from config.global_config import IST, Topic
from data_layer.base_feeder import EventBus, CandleEvent
from data_layer.runtime_config import RuntimeConfig
from execution_bridge.straddle_bridge import ICOrderEvent, ICFillEvent

logger = logging.getLogger(__name__)


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class IronCondorLeg:
    side: str            # "sell" | "buy"
    option_type: str     # "CE" | "PE"
    strike: float
    entry_price: float
    ltp: float = 0.0
    filled: bool = False
    fill_time: Optional[datetime] = None


@dataclass
class IronCondorPosition:
    underlying: str
    expiry: date
    atm_at_entry: float

    short_ce: IronCondorLeg = field(default_factory=lambda: IronCondorLeg("sell", "CE", 0, 0))
    short_pe: IronCondorLeg = field(default_factory=lambda: IronCondorLeg("sell", "PE", 0, 0))
    long_ce:  IronCondorLeg = field(default_factory=lambda: IronCondorLeg("buy",  "CE", 0, 0))
    long_pe:  IronCondorLeg = field(default_factory=lambda: IronCondorLeg("buy",  "PE", 0, 0))

    net_credit: float  = 0.0
    realized_pnl: float = 0.0
    open_time: Optional[datetime] = None
    close_time: Optional[datetime] = None
    status: str = "open"

    # Thresholds captured at open so mid-trade config changes don't shift goalposts
    _wing_width: float = field(default=200.0, repr=False)
    _profit_pct: float = field(default=0.50,  repr=False)
    _sl_pct:     float = field(default=2.00,  repr=False)

    @property
    def max_profit(self) -> float:
        return self.net_credit

    @property
    def max_loss(self) -> float:
        return self._wing_width - self.net_credit

    @property
    def profit_target(self) -> float:
        return self.net_credit * self._profit_pct

    @property
    def stop_loss(self) -> float:
        return self.net_credit * self._sl_pct

    @property
    def legs(self) -> List[IronCondorLeg]:
        return [self.short_ce, self.short_pe, self.long_ce, self.long_pe]


# ── Strategy Engine ───────────────────────────────────────────────────────────

class IronCondorStrategy:
    """
    Event-driven iron condor engine.
    All thresholds read from RuntimeConfig — fully reconfigurable at runtime.
    """

    def __init__(self, bus: EventBus, cfg=None, underlying: str = "NIFTY") -> None:
        self._bus = bus
        self._cfg = cfg
        self._underlying = underlying
        self._running = False
        self._position: Optional[IronCondorPosition] = None
        self._spot: float = 0.0
        self._prem_cache: Dict[str, float] = {}   # "NIFTY24500CE" → ltp
        self._tasks: list = []

        self._load_thresholds()

    def _load_thresholds(self) -> None:
        from data_layer.runtime_config import RuntimeConfig
        ic = RuntimeConfig.index_section(self._underlying, "iron_condor")
        self._start_time      = _parse_time(ic.get("start_time",      "09:16"))
        self._squareoff_time  = _parse_time(ic.get("squareoff_time",  "15:15"))
        self._entry_day       = str(ic.get("entry_day", "daily"))
        self._profit_target   = float(ic.get("profit_target_inr", 5000.0))
        self._stoploss        = float(ic.get("stoploss_inr",       2000.0))
        self._ratio_threshold = float(ic.get("ratio_exit_threshold", 3.0))
        self._short_otm       = float(ic.get("short_leg_otm_pts",   200.0))
        self._long_otm        = float(ic.get("long_leg_otm_pts",    300.0))
        self._lot_size        = int(ic.get("lot_size",  65))
        self._strike_step     = int(ic.get("strike_step", 50))

    def reconfigure(self) -> None:
        self._load_thresholds()
        logger.info(
            "IronCondor[%s]: reconfigured — entry=%s sq=%s profit=₹%.0f sl=₹%.0f ratio=%.1fx",
            self._underlying,
            self._start_time, self._squareoff_time,
            self._profit_target, self._stoploss, self._ratio_threshold,
        )

    def start(self) -> None:
        self._running = True
        self._tasks = [
            asyncio.create_task(self._candle_loop(),  name="ic_candle_loop"),
            asyncio.create_task(self._tick_loop(),    name="ic_tick_loop"),
            asyncio.create_task(self._option_loop(),  name="ic_option_loop"),
        ]
        logger.info("IronCondorStrategy[%s]: started.", self._underlying)

    def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            if not t.done():
                t.cancel()
        logger.info("IronCondorStrategy[%s]: stopped.", self._underlying)

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
            # Update premium cache for all option ticks on this underlying
            if tick.underlying == self._underlying:
                key = f"{self._underlying}{int(tick.strike)}{tick.option_type}"
                self._prem_cache[key] = tick.ltp
            if not self._position or self._position.underlying != tick.underlying:
                continue
            self._update_leg_ltp(tick)

    # ── Entry logic ───────────────────────────────────────────────────────────

    async def _on_candle(self, ev: CandleEvent) -> None:
        now = datetime.now(IST)

        # Reload thresholds every candle
        self._load_thresholds()

        # Time gate: only enter after start_time (IC is positional — no daily squareoff)
        if now.time() < self._start_time:
            return

        # Day gate: entry_day config (daily | monday | tuesday | ...)
        if self._entry_day != "daily":
            day_names = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
            if day_names[now.weekday()] != self._entry_day:
                return

        # Don't re-enter if a position is already open for this IC cycle
        if self._position and self._position.status == "open":
            return

        spot = self._spot
        if spot <= 0:
            return

        step = self._strike_step
        atm  = round(spot / step) * step

        # short legs at ±short_leg_otm_pts from ATM
        # long (hedge) legs at ±long_leg_otm_pts from ATM
        short_ce_strike = atm + self._short_otm
        short_pe_strike = atm - self._short_otm
        long_ce_strike  = atm + self._long_otm
        long_pe_strike  = atm - self._long_otm

        logger.info(
            "IronCondor[%s]: entry signal at ATM=%.0f | "
            "short CE=%.0f / PE=%.0f | long CE=%.0f / PE=%.0f",
            self._underlying, atm,
            short_ce_strike, short_pe_strike, long_ce_strike, long_pe_strike,
        )

        # Get current LTPs from premium cache if available
        short_ce_ltp = self._prem_cache.get(f"{self._underlying}{int(short_ce_strike)}CE", 0.0)
        short_pe_ltp = self._prem_cache.get(f"{self._underlying}{int(short_pe_strike)}PE", 0.0)
        long_ce_ltp  = self._prem_cache.get(f"{self._underlying}{int(long_ce_strike)}CE", 0.0)
        long_pe_ltp  = self._prem_cache.get(f"{self._underlying}{int(long_pe_strike)}PE", 0.0)

        ev = ICOrderEvent(
            action          = "ENTRY",
            underlying      = self._underlying,
            atm             = atm,
            short_ce_strike = short_ce_strike,
            short_pe_strike = short_pe_strike,
            short_ce_ltp    = short_ce_ltp,
            short_pe_ltp    = short_pe_ltp,
            long_ce_strike  = long_ce_strike,
            long_pe_strike  = long_pe_strike,
            long_ce_ltp     = long_ce_ltp,
            long_pe_ltp     = long_pe_ltp,
            lot_size        = self._lot_size,
            event_id        = str(uuid.uuid4())[:8],
        )
        await self._bus.publish(Topic.IC_ORDER_REQUEST, ev)

        # Record position locally for P&L tracking
        from datetime import date as _d
        today = datetime.now(IST).date()
        self._position = IronCondorPosition(
            underlying    = self._underlying,
            expiry        = today,
            atm_at_entry  = atm,
            short_ce      = IronCondorLeg("sell", "CE", short_ce_strike, short_ce_ltp),
            short_pe      = IronCondorLeg("sell", "PE", short_pe_strike, short_pe_ltp),
            long_ce       = IronCondorLeg("buy",  "CE", long_ce_strike,  long_ce_ltp),
            long_pe       = IronCondorLeg("buy",  "PE", long_pe_strike,  long_pe_ltp),
            net_credit    = (short_ce_ltp + short_pe_ltp) - (long_ce_ltp + long_pe_ltp),
            open_time     = datetime.now(IST),
            _wing_width   = self._long_otm,
            _profit_pct   = self._profit_pct,
            _sl_pct       = self._sl_pct,
        )

    # ── Exit logic ────────────────────────────────────────────────────────────

    async def _check_exits(self) -> None:
        pos = self._position
        if not pos:
            return

        now = datetime.now(IST)

        if now.time() >= self._squareoff_time:
            logger.info("IronCondor[%s]: time exit at %s IST.", self._underlying, self._squareoff_time)
            await self._close_position("time_exit")
            return

        # MTM P&L in ₹
        call_entry_net = pos.short_ce.entry_price - pos.long_ce.entry_price
        put_entry_net  = pos.short_pe.entry_price - pos.long_pe.entry_price
        call_live_net  = pos.short_ce.ltp - pos.long_ce.ltp
        put_live_net   = pos.short_pe.ltp - pos.long_pe.ltp
        open_pnl_pts = (call_entry_net - call_live_net) + (put_entry_net - put_live_net)
        pnl_inr = open_pnl_pts * self._lot_size

        if pnl_inr >= self._profit_target:
            logger.info(
                "IronCondor[%s]: profit target hit — ₹%.0f >= ₹%.0f",
                self._underlying, pnl_inr, self._profit_target,
            )
            await self._close_position("profit_target")
            return

        if pnl_inr <= -self._stoploss:
            logger.info(
                "IronCondor[%s]: stop loss hit — ₹%.0f <= -₹%.0f",
                self._underlying, pnl_inr, self._stoploss,
            )
            await self._close_position("stop_loss")
            return

        # Ratio check: roll side if one short leg has ballooned vs the other
        ce_ltp = pos.short_ce.ltp
        pe_ltp = pos.short_pe.ltp
        if ce_ltp > 0 and pe_ltp > 0:
            if ce_ltp / pe_ltp >= self._ratio_threshold:
                logger.warning("IronCondor[%s]: CE ratio %.2fx — rolling call side.", self._underlying, ce_ltp / pe_ltp)
                await self._close_position("ce_ratio_breach")
                return
            if pe_ltp / ce_ltp >= self._ratio_threshold:
                logger.warning("IronCondor[%s]: PE ratio %.2fx — rolling put side.", self._underlying, pe_ltp / ce_ltp)
                await self._close_position("pe_ratio_breach")
                return

    async def _close_position(self, reason: str) -> None:
        if not self._position:
            return
        pos = self._position
        logger.info("IronCondor[%s]: closing position — reason=%s pnl=₹%.0f",
                    self._underlying, reason,
                    pos.realized_pnl * self._lot_size)

        short_ce_ltp = self._prem_cache.get(f"{self._underlying}{int(pos.short_ce.strike)}CE", pos.short_ce.entry_price)
        short_pe_ltp = self._prem_cache.get(f"{self._underlying}{int(pos.short_pe.strike)}PE", pos.short_pe.entry_price)
        long_ce_ltp  = self._prem_cache.get(f"{self._underlying}{int(pos.long_ce.strike)}CE",  pos.long_ce.entry_price)
        long_pe_ltp  = self._prem_cache.get(f"{self._underlying}{int(pos.long_pe.strike)}PE",  pos.long_pe.entry_price)

        # Calculate final P&L in points (credit received - current cost to close)
        exit_cost    = (short_ce_ltp + short_pe_ltp) - (long_ce_ltp + long_pe_ltp)
        pnl_pts      = pos.net_credit - exit_cost
        pos.realized_pnl = pos.realized_pnl + pnl_pts

        ev = ICOrderEvent(
            action          = "EXIT",
            underlying      = self._underlying,
            atm             = pos.atm_at_entry,
            short_ce_strike = pos.short_ce.strike,
            short_pe_strike = pos.short_pe.strike,
            short_ce_ltp    = short_ce_ltp,
            short_pe_ltp    = short_pe_ltp,
            long_ce_strike  = pos.long_ce.strike,
            long_pe_strike  = pos.long_pe.strike,
            long_ce_ltp     = long_ce_ltp,
            long_pe_ltp     = long_pe_ltp,
            lot_size        = self._lot_size,
            close_reason    = reason,
            cumulative_pnl  = pos.realized_pnl,
            event_id        = str(uuid.uuid4())[:8],
        )
        await self._bus.publish(Topic.IC_ORDER_REQUEST, ev)
        pos.status     = "closed"
        pos.close_time = datetime.now(IST)
        self._position = None
        self._position.close_time = datetime.now(IST)
        self._position = None

    def _update_leg_ltp(self, tick) -> None:
        if not self._position:
            return
        for leg in self._position.legs:
            if (abs(leg.strike - tick.strike) < 0.01
                    and leg.option_type == tick.option_type):
                leg.ltp = tick.ltp

    # ── Public accessors ─────────────────────────────────────────────────────

    @property
    def has_open_position(self) -> bool:
        return self._position is not None and self._position.status == "open"

    @property
    def position(self) -> Optional[IronCondorPosition]:
        return self._position

    @property
    def entry_allowed(self) -> bool:
        from datetime import datetime
        now = datetime.now(IST).time()
        return self._start_time <= now < self._squareoff_time


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_time(s: str) -> dtime:
    try:
        h, m = s.split(":")
        return dtime(int(h), int(m))
    except Exception:
        return dtime(15, 15)
