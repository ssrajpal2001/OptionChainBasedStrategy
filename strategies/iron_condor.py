"""
strategies/iron_condor.py — Iron Condor positional options strategy.

Entry: SELL short CE/PE at ATM ± short_otm_pts, BUY hedge CE/PE at short ± wing_pts.
No daily squareoff — NRML positional. Exits on P&L targets or expiry.

Dynamic Rolling (Ratio Shift):
  When CE_ltp / PE_ltp (or reverse) >= ratio_trigger (default 2.0):
  1. Close the PROFITABLE side at 2× quantity:
       - Buy back 2× the short leg quantity (closes original 1× + opens new 1× long)
       - Sell 1× the hedge leg (close)
     Net result: 1× long at old short strike = new hedge for the rolled side.
  2. New short = ATM ± (original_diff / 2) — converges toward ATM each roll.
  3. Cumulative P&L from closed legs saved to DB per trade_id.
  4. Increment adjustment_count for that side.

Hard stop: When adjustment_count reaches max_adjustments_per_side:
  - Close the entire IC immediately.
  - Re-enter a fresh IC at current ATM if within entry window.

Min LTP filter: Short legs must each have LTP >= min_ltp before entry fires.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, date, time as dtime
from typing import Dict, List, Optional, Tuple

from config.global_config import IST, Topic
from data_layer.base_feeder import EventBus, CandleEvent
from data_layer.runtime_config import RuntimeConfig
from execution_bridge.straddle_bridge import ICOrderEvent, ICFillEvent

logger = logging.getLogger(__name__)

_RETRY_LIMIT = 3  # max broker retries per leg on adjustment


def _make_ic_logger(underlying: str) -> logging.Logger:
    name = f"client.ic.{underlying}"
    lg = logging.getLogger(name)
    if lg.handlers:
        return lg
    lg.setLevel(logging.DEBUG)
    log_dir = os.path.join("logs", "clients")
    os.makedirs(log_dir, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    fh = logging.FileHandler(
        os.path.join(log_dir, f"ic_{underlying}_{date_str}.log"), encoding="utf-8"
    )
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))
    lg.addHandler(fh)
    lg.propagate = False
    return lg


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class IronCondorLeg:
    side:        str    # "sell" | "buy"
    option_type: str    # "CE" | "PE"
    strike:      float
    entry_price: float
    ltp:         float = 0.0
    filled:      bool  = False
    fill_time:   Optional[datetime] = None


@dataclass
class IronCondorPosition:
    underlying:   str
    expiry:       date
    atm_at_entry: float
    trade_id:     str = field(default_factory=lambda: str(uuid.uuid4())[:12])

    # Current active legs
    short_ce: IronCondorLeg = field(default_factory=lambda: IronCondorLeg("sell","CE",0,0))
    short_pe: IronCondorLeg = field(default_factory=lambda: IronCondorLeg("sell","PE",0,0))
    long_ce:  IronCondorLeg = field(default_factory=lambda: IronCondorLeg("buy", "CE",0,0))
    long_pe:  IronCondorLeg = field(default_factory=lambda: IronCondorLeg("buy", "PE",0,0))

    # P&L tracking
    net_credit:         float = 0.0   # credit from current open legs
    cumulative_adj_pnl: float = 0.0   # realized P&L from all closed adjustment legs
    open_time:          Optional[datetime] = None
    close_time:         Optional[datetime] = None
    status:             str   = "open"  # "open" | "adjusting" | "closed"

    # Adjustment counters (per side)
    adj_count_ce: int = 0
    adj_count_pe: int = 0

    # Captured at entry — don't shift goalposts during trade
    original_diff:    float = 300.0   # ATM ± this = short strikes
    wing_pts:         float = 150.0   # short ± this = hedge strikes
    profit_target_rs: float = 5000.0
    sl_rs:            float = 2000.0
    max_adj:          int   = 4
    lot_size:         int   = 65

    @property
    def legs(self) -> List[IronCondorLeg]:
        return [self.short_ce, self.short_pe, self.long_ce, self.long_pe]

    def to_dict(self) -> dict:
        """JSON-serialisable snapshot for PositionStore."""
        def _leg(l: IronCondorLeg) -> dict:
            return {"side": l.side, "option_type": l.option_type, "strike": l.strike,
                    "entry_price": l.entry_price, "ltp": l.ltp, "filled": l.filled}
        return {
            "underlying": self.underlying,
            "expiry": self.expiry.isoformat() if self.expiry else None,
            "atm_at_entry": self.atm_at_entry, "trade_id": self.trade_id,
            "short_ce": _leg(self.short_ce), "short_pe": _leg(self.short_pe),
            "long_ce": _leg(self.long_ce), "long_pe": _leg(self.long_pe),
            "net_credit": self.net_credit, "cumulative_adj_pnl": self.cumulative_adj_pnl,
            "open_time": self.open_time.isoformat() if self.open_time else None,
            "status": self.status, "adj_count_ce": self.adj_count_ce, "adj_count_pe": self.adj_count_pe,
            "original_diff": self.original_diff, "wing_pts": self.wing_pts,
            "profit_target_rs": self.profit_target_rs, "sl_rs": self.sl_rs,
            "max_adj": self.max_adj, "lot_size": self.lot_size,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "IronCondorPosition":
        from datetime import date as _date, datetime as _dt
        def _leg(x: dict) -> IronCondorLeg:
            return IronCondorLeg(side=x["side"], option_type=x["option_type"],
                                 strike=x["strike"], entry_price=x["entry_price"],
                                 ltp=x.get("ltp", 0.0), filled=x.get("filled", False))
        return cls(
            underlying=d["underlying"],
            expiry=_date.fromisoformat(d["expiry"]) if d.get("expiry") else _date.today(),
            atm_at_entry=d.get("atm_at_entry", 0.0), trade_id=d.get("trade_id", ""),
            short_ce=_leg(d["short_ce"]), short_pe=_leg(d["short_pe"]),
            long_ce=_leg(d["long_ce"]), long_pe=_leg(d["long_pe"]),
            net_credit=d.get("net_credit", 0.0), cumulative_adj_pnl=d.get("cumulative_adj_pnl", 0.0),
            open_time=_dt.fromisoformat(d["open_time"]) if d.get("open_time") else None,
            status=d.get("status", "open"),
            adj_count_ce=d.get("adj_count_ce", 0), adj_count_pe=d.get("adj_count_pe", 0),
            original_diff=d.get("original_diff", 300.0), wing_pts=d.get("wing_pts", 150.0),
            profit_target_rs=d.get("profit_target_rs", 5000.0), sl_rs=d.get("sl_rs", 2000.0),
            max_adj=d.get("max_adj", 4), lot_size=d.get("lot_size", 65),
        )

    @property
    def total_pnl_pts(self) -> float:
        """Current open P&L in points + cumulative closed adj P&L."""
        # MTM on open legs: credit received - cost to close now
        close_cost = (self.short_ce.ltp + self.short_pe.ltp) - (self.long_ce.ltp + self.long_pe.ltp)
        open_pnl   = self.net_credit - close_cost
        return open_pnl + self.cumulative_adj_pnl

    @property
    def total_pnl_rs(self) -> float:
        return self.total_pnl_pts * self.lot_size


# ── Strategy Engine ───────────────────────────────────────────────────────────

class IronCondorStrategy:
    """
    Event-driven Iron Condor engine with dynamic rolling / ratio-shift adjustments.
    All thresholds read from RuntimeConfig — fully reconfigurable at runtime.
    """

    def __init__(self, bus: EventBus, cfg=None, underlying: str = "NIFTY") -> None:
        self._bus        = bus
        self._cfg        = cfg
        self._underlying = underlying
        self._running    = False
        self._position:  Optional[IronCondorPosition] = None
        self._spot:      float = 0.0
        self._prem_cache: Dict[str, float] = {}   # "NIFTY24500CE" → ltp
        self._bid_cache:  Dict[str, float] = {}   # same key → best bid
        self._ask_cache:  Dict[str, float] = {}   # same key → best ask
        self._tasks:     list = []
        self._adjusting: bool = False  # lock to prevent re-entrant adjustments
        self._clog: logging.Logger = _make_ic_logger(underlying)
        self._load_thresholds()

    # ── Config ────────────────────────────────────────────────────────────────

    def _load_thresholds(self) -> None:
        ic = RuntimeConfig.index_section(self._underlying, "iron_condor")
        self._start_time      = _parse_time(ic.get("start_time",       "09:16"))
        self._entry_day       = str(ic.get("entry_day",                "daily"))
        self._product_type    = str(ic.get("product_type",            "MIS")).upper()
        self._profit_target   = float(ic.get("profit_target_inr",      5000.0))
        self._stoploss        = float(ic.get("stoploss_inr",           2000.0))
        self._ratio_trigger   = float(ic.get("ratio_trigger",           2.0))   # NEW: 2:1 ratio
        self._short_otm       = float(ic.get("short_leg_otm_pts",       300.0))
        self._wing_pts        = float(ic.get("long_leg_otm_pts",        150.0))
        self._lot_size        = int(ic.get("lot_size",                   65))
        self._strike_step     = int(ic.get("strike_step",                50))
        self._max_adj         = int(ic.get("max_adjustments_per_side",   4))
        self._min_ltp         = float(ic.get("min_ltp",                  0.0))  # NEW: min LTP filter

    def reconfigure(self) -> None:
        self._load_thresholds()
        logger.info("IronCondor[%s]: reconfigured ratio_trigger=%.1f min_ltp=%.2f max_adj=%d",
                    self._underlying, self._ratio_trigger, self._min_ltp, self._max_adj)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    @property
    def _persist_key(self) -> str:
        return f"{self._underlying}_iron_condor"

    def start(self) -> None:
        self._running = True
        # Restore an open position across restarts (MIS positions from a prior
        # day are auto-discarded by the store — broker squared them off).
        try:
            from data_layer import position_store as _ps
            _saved = _ps.load(self._persist_key)
            if _saved:
                self._position = IronCondorPosition.from_dict(_saved)
                logger.info("IronCondor[%s]: restored open position from store (status=%s, net_credit=%.2f).",
                            self._underlying, self._position.status, self._position.net_credit)
        except Exception as exc:
            logger.warning("IronCondor[%s]: restore failed: %s", self._underlying, exc)
        self._tasks = [
            asyncio.create_task(self._candle_loop(),  name="ic_candle"),
            asyncio.create_task(self._tick_loop(),    name="ic_tick"),
            asyncio.create_task(self._option_loop(),  name="ic_option"),
        ]
        logger.info("IronCondorStrategy[%s]: started.", self._underlying)

    def _persist(self) -> None:
        """Write the current open position to the store (or clear if none)."""
        try:
            from data_layer import position_store as _ps
            if self._position and self._position.status == "open":
                _ps.save(self._persist_key, self._position.to_dict(),
                         product_type=getattr(self, "_product_type", "MIS"))
            else:
                _ps.clear(self._persist_key)
        except Exception as exc:
            logger.warning("IronCondor[%s]: persist failed: %s", self._underlying, exc)

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
            if ev.symbol != self._underlying or ev.timeframe != 5:
                continue
            try:
                # Candle close also triggers an entry attempt (belt-and-suspenders);
                # the primary immediate trigger is the tick loop.
                await self._try_entry()
            except Exception as exc:
                logger.exception("IronCondor[%s]: candle error: %s", self._underlying, exc)

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
            if self._position and self._position.status == "open" and not self._adjusting:
                await self._check_exits()
                await self._check_adjustment_criteria()
            elif not self._position or self._position.status != "open":
                # No open position → attempt entry IMMEDIATELY on every tick
                # (throttled inside _try_entry). IC needs no candle close.
                try:
                    await self._try_entry()
                except Exception as exc:
                    logger.exception("IronCondor[%s]: entry error: %s", self._underlying, exc)

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
            if tick.underlying == self._underlying:
                key = f"{self._underlying}{int(tick.strike)}{tick.option_type}"
                self._prem_cache[key] = tick.ltp
                if tick.bid > 0:
                    self._bid_cache[key] = tick.bid
                if tick.ask > 0:
                    self._ask_cache[key] = tick.ask
            if self._position and self._position.underlying == tick.underlying:
                self._update_leg_ltp(tick)

    # ── Entry logic ───────────────────────────────────────────────────────────

    async def _try_entry(self) -> None:
        """
        Attempt an Iron Condor entry IMMEDIATELY (driven by index ticks, not
        candle closes). IC has no indicator/timeframe gate — it enters as soon
        as the entry window is open, no position is held, and live premiums are
        present. Throttled to avoid re-evaluating on every tick.
        """
        import time as _time
        nowm = _time.monotonic()
        if nowm - getattr(self, "_last_entry_attempt", 0.0) < 1.0:
            return
        self._last_entry_attempt = nowm

        self._load_thresholds()
        now = datetime.now(IST)

        # Time gate: only enter after start_time
        if now.time() < self._start_time:
            return

        # Day gate
        if self._entry_day != "daily":
            day_names = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
            if day_names[now.weekday()] != self._entry_day:
                return

        # Don't re-enter while position is open
        if self._position and self._position.status == "open":
            return

        spot = self._spot
        if spot <= 0:
            self._clog.info("WAIT  spot=0 — no index tick received yet")
            return

        step = self._strike_step
        atm  = round(spot / step) * step

        short_ce_strike = atm + self._short_otm
        short_pe_strike = atm - self._short_otm
        long_ce_strike  = short_ce_strike + self._wing_pts
        long_pe_strike  = short_pe_strike - self._wing_pts

        # Min LTP filter — both short legs must have meaningful premium
        short_ce_ltp = self._prem_cache.get(f"{self._underlying}{int(short_ce_strike)}CE", 0.0)
        short_pe_ltp = self._prem_cache.get(f"{self._underlying}{int(short_pe_strike)}PE", 0.0)

        if self._min_ltp > 0 and (short_ce_ltp < self._min_ltp or short_pe_ltp < self._min_ltp):
            self._clog.info(
                "BLOCK min_ltp=%.2f  short_CE[%s]=%.2f  short_PE[%s]=%.2f — leg below floor",
                self._min_ltp,
                f"{self._underlying}{int(short_ce_strike)}CE", short_ce_ltp,
                f"{self._underlying}{int(short_pe_strike)}PE", short_pe_ltp,
            )
            return

        long_ce_ltp = self._prem_cache.get(f"{self._underlying}{int(long_ce_strike)}CE", 0.0)
        long_pe_ltp = self._prem_cache.get(f"{self._underlying}{int(long_pe_strike)}PE", 0.0)

        net_credit = (short_ce_ltp + short_pe_ltp) - (long_ce_ltp + long_pe_ltp)

        # Guard 1: never fire a zero/negative-credit order. Missing premiums
        # (option feed not yet populated for these strikes) yield net_credit<=0.
        # Throttle this log so it doesn't spam on every tick while waiting.
        if net_credit <= 0 or short_ce_ltp <= 0 or short_pe_ltp <= 0:
            if nowm - getattr(self, "_last_wait_log", 0.0) > 30.0:
                self._last_wait_log = nowm
                self._clog.info(
                    "WAIT premiums — short CE[%d]=%.2f PE[%d]=%.2f net_credit=%.2f (feed not ready)",
                    int(short_ce_strike), short_ce_ltp,
                    int(short_pe_strike), short_pe_ltp, net_credit,
                )
            return

        # Guard 2: registry/expiry must be loaded BEFORE we route any order.
        from data_layer.instrument_registry import next_expiry as _nexp
        _expiry = _nexp(self._underlying)
        if not _expiry:
            logger.warning("IronCondor[%s]: registry not loaded, skipping entry — authenticate Upstox first.", self._underlying)
            return

        logger.info(
            "IronCondor[%s]: ENTRY ATM=%.0f | short CE=%.0f(%.2f) PE=%.0f(%.2f) "
            "| hedge CE=%.0f(%.2f) PE=%.0f(%.2f) | net_credit=%.2f",
            self._underlying, atm,
            short_ce_strike, short_ce_ltp, short_pe_strike, short_pe_ltp,
            long_ce_strike,  long_ce_ltp,  long_pe_strike,  long_pe_ltp,
            net_credit,
        )

        trade_id = str(uuid.uuid4())[:12]
        ev_order = ICOrderEvent(
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
            event_id        = trade_id,
        )
        await self._bus.publish(Topic.IC_ORDER_REQUEST, ev_order)

        self._position = IronCondorPosition(
            underlying      = self._underlying,
            expiry          = _expiry,
            atm_at_entry    = atm,
            trade_id        = trade_id,
            # Seed ltp = entry price so total_pnl_pts is ~0 at entry (else legs'
            # ltp default to 0 -> close_cost=0 -> fake instant profit = net_credit).
            short_ce        = IronCondorLeg("sell","CE", short_ce_strike, short_ce_ltp, short_ce_ltp),
            short_pe        = IronCondorLeg("sell","PE", short_pe_strike, short_pe_ltp, short_pe_ltp),
            long_ce         = IronCondorLeg("buy", "CE", long_ce_strike,  long_ce_ltp,  long_ce_ltp),
            long_pe         = IronCondorLeg("buy", "PE", long_pe_strike,  long_pe_ltp,  long_pe_ltp),
            net_credit      = net_credit,
            open_time       = datetime.now(IST),
            original_diff   = self._short_otm,
            wing_pts        = self._wing_pts,
            profit_target_rs = self._profit_target,
            sl_rs           = self._stoploss,
            max_adj         = self._max_adj,
            lot_size        = self._lot_size,
        )
        self._persist()   # survive restarts
        await self._log_trade_db("ENTRY")

    # ── Exit checks ───────────────────────────────────────────────────────────

    async def _check_exits(self) -> None:
        pos = self._position
        if not pos:
            return

        pnl_rs = pos.total_pnl_rs

        if pnl_rs >= pos.profit_target_rs:
            logger.info("IronCondor[%s]: PROFIT TARGET ₹%.0f >= ₹%.0f",
                        self._underlying, pnl_rs, pos.profit_target_rs)
            await self._close_position("profit_target")
            return

        if pnl_rs <= -pos.sl_rs:
            logger.info("IronCondor[%s]: STOP LOSS ₹%.0f <= -₹%.0f",
                        self._underlying, pnl_rs, pos.sl_rs)
            await self._close_position("stop_loss")
            return

    # ── Adjustment / Rolling ──────────────────────────────────────────────────

    def check_adjustment_criteria(self) -> Optional[str]:
        """
        Returns 'CE' if CE side needs adjustment, 'PE' if PE side does, else None.
        Trigger: one short leg's ltp >= ratio_trigger × the other short leg's ltp.
        """
        pos = self._position
        if not pos or pos.status != "open":
            return None
        ce_ltp = pos.short_ce.ltp
        pe_ltp = pos.short_pe.ltp
        if ce_ltp <= 0 or pe_ltp <= 0:
            return None

        if ce_ltp / pe_ltp >= self._ratio_trigger:
            return "PE"   # PE is profitable (CE is bleeding); roll the profitable PE side
        if pe_ltp / ce_ltp >= self._ratio_trigger:
            return "CE"   # CE is profitable (PE is bleeding); roll the profitable CE side
        return None

    async def _check_adjustment_criteria(self) -> None:
        if self._adjusting:
            return
        side = self.check_adjustment_criteria()
        if side:
            await self.adjust_iron_condor(side)

    def _exec_price(self, key: str, side: str) -> float:
        """
        Return the correct execution price for a leg.
          side='buy'  → ask price (what you pay to buy back a short)
          side='sell' → bid price (what you receive when selling)
        Falls back to LTP if bid/ask not yet populated (non-live / stale quote).
        """
        ltp = self._prem_cache.get(key, 0.0)
        if side == "buy":
            return self._ask_cache.get(key, ltp) or ltp
        return self._bid_cache.get(key, ltp) or ltp

    async def adjust_iron_condor(self, profitable_side: str) -> None:
        """
        Roll the profitable side using the ratio-shift mechanism.

        profitable_side: 'CE' (PE is bleeding, CE is profit) or 'PE' (CE is bleeding).

        Mechanics:
          1. Close the profitable side at 2× quantity:
               Buy back 2× short leg  → closes 1× original + opens 1× new long
               Sell 1× long (hedge)   → closes hedge position
             Net: 1× long at old short strike = new hedge for rolled position.
          2. New short strike = current_ATM ± (original_diff / 2) → converges to ATM.
          3. Increment adj_count for that side.
          4. If adj_count >= max_adj: hard stop → close all → re-enter.

        Execution prices use ask-to-buy / bid-to-sell to avoid stale-LTP slippage.
        On any mid-sequence exception the position is marked 'broken' and locked from
        further auto-adjustments until manually reviewed.
        """
        pos = self._position
        if not pos or self._adjusting:
            return
        if pos.status == "broken":
            logger.error(
                "IronCondor[%s]: position %s is BROKEN — manual review required, "
                "no further auto-adjustments.",
                self._underlying, pos.trade_id,
            )
            return

        self._adjusting = True
        try:
            # Determine which side is profitable vs bleeding
            if profitable_side == "CE":
                profit_short  = pos.short_ce
                profit_hedge  = pos.long_ce
                bleed_short   = pos.short_pe
                bleed_hedge   = pos.long_pe
                direction     = +1
                adj_count     = pos.adj_count_ce
            else:
                profit_short  = pos.short_pe
                profit_hedge  = pos.long_pe
                bleed_short   = pos.short_ce
                bleed_hedge   = pos.long_ce
                direction     = -1
                adj_count     = pos.adj_count_pe

            ot = profitable_side   # "CE" or "PE"

            # Execution prices — ask to buy back, bid to sell
            ps_key = f"{self._underlying}{int(profit_short.strike)}{ot}"
            ph_key = f"{self._underlying}{int(profit_hedge.strike)}{ot}"
            ps_close_px = self._exec_price(ps_key, "buy")   # buying back 2× short
            ph_close_px = self._exec_price(ph_key, "sell")  # selling 1× hedge

            logger.info(
                "IronCondor[%s]: ADJUSTMENT — profitable_side=%s "
                "short_ask=%.2f hedge_bid=%.2f bleed_short_ltp=%.2f adj_count=%d/%d",
                self._underlying, profitable_side,
                ps_close_px, ph_close_px, bleed_short.ltp, adj_count + 1, pos.max_adj,
            )

            # ── Step 1: Realized P&L uses actual execution prices ─────────────
            # Short was sold at entry; buying back at ask → cost = ask price
            # Hedge was bought at entry; selling at bid → receive = bid price
            pnl_from_short = (profit_short.entry_price - ps_close_px) * pos.lot_size
            pnl_from_hedge = (ph_close_px - profit_hedge.entry_price) * pos.lot_size
            adj_pnl = pnl_from_short + pnl_from_hedge
            pos.cumulative_adj_pnl += adj_pnl / pos.lot_size  # store in points

            # ── Step 2: Publish ADJUST_CLOSE ─────────────────────────────────
            close_ev = ICOrderEvent(
                action          = "ADJUST_CLOSE",
                underlying      = pos.underlying,
                atm             = self._spot,
                short_ce_strike = profit_short.strike if ot == "CE" else bleed_short.strike,
                short_pe_strike = profit_short.strike if ot == "PE" else bleed_short.strike,
                short_ce_ltp    = ps_close_px if ot == "CE" else 0.0,   # ask price used
                short_pe_ltp    = ps_close_px if ot == "PE" else 0.0,
                long_ce_strike  = profit_hedge.strike if ot == "CE" else bleed_hedge.strike,
                long_pe_strike  = profit_hedge.strike if ot == "PE" else bleed_hedge.strike,
                long_ce_ltp     = ph_close_px if ot == "CE" else 0.0,   # bid price used
                long_pe_ltp     = ph_close_px if ot == "PE" else 0.0,
                lot_size        = pos.lot_size,
                close_reason    = f"ratio_shift_{profitable_side}",
                cumulative_pnl  = pos.cumulative_adj_pnl,
                event_id        = f"{pos.trade_id}_adj{adj_count+1}",
            )
            await self._bus.publish(Topic.IC_ORDER_REQUEST, close_ev)

            # ── Step 3: New short strike converges → ATM ± (original_diff / 2) ─
            step             = self._strike_step
            atm_now          = round(self._spot / step) * step
            new_diff         = pos.original_diff / 2
            new_short_strike = round((atm_now + direction * new_diff) / step) * step
            new_hedge_strike = round((new_short_strike + direction * pos.wing_pts) / step) * step

            ns_key = f"{self._underlying}{int(new_short_strike)}{ot}"
            nh_key = f"{self._underlying}{int(new_hedge_strike)}{ot}"
            # New short: we're selling → use bid price
            # New hedge: we're buying → use ask price
            new_short_px  = self._exec_price(ns_key, "sell")
            new_hedge_px  = self._exec_price(nh_key, "buy")
            old_short_ltp = profit_short.ltp  # 1× long at old short strike from 2× buyback

            # ── Step 4: Publish ADJUST_OPEN ───────────────────────────────────
            open_ev = ICOrderEvent(
                action          = "ADJUST_OPEN",
                underlying      = pos.underlying,
                atm             = atm_now,
                short_ce_strike = new_short_strike if ot == "CE" else pos.short_ce.strike,
                short_pe_strike = new_short_strike if ot == "PE" else pos.short_pe.strike,
                short_ce_ltp    = new_short_px if ot == "CE" else pos.short_ce.ltp,
                short_pe_ltp    = new_short_px if ot == "PE" else pos.short_pe.ltp,
                long_ce_strike  = new_hedge_strike if ot == "CE" else pos.long_ce.strike,
                long_pe_strike  = new_hedge_strike if ot == "PE" else pos.long_pe.strike,
                long_ce_ltp     = new_hedge_px if ot == "CE" else pos.long_ce.ltp,
                long_pe_ltp     = new_hedge_px if ot == "PE" else pos.long_pe.ltp,
                lot_size        = pos.lot_size,
                event_id        = f"{pos.trade_id}_adj{adj_count+1}_open",
            )
            await self._bus.publish(Topic.IC_ORDER_REQUEST, open_ev)

            # ── Step 5: Update position state ────────────────────────────────
            if ot == "CE":
                pos.long_ce   = IronCondorLeg("buy", "CE", profit_short.strike, old_short_ltp, old_short_ltp)
                pos.short_ce  = IronCondorLeg("sell","CE", new_short_strike,    new_short_px,  new_short_px)
                pos.adj_count_ce += 1
                adj_count = pos.adj_count_ce
            else:
                pos.long_pe   = IronCondorLeg("buy", "PE", profit_short.strike, old_short_ltp, old_short_ltp)
                pos.short_pe  = IronCondorLeg("sell","PE", new_short_strike,    new_short_px,  new_short_px)
                pos.adj_count_pe += 1
                adj_count = pos.adj_count_pe

            pos.net_credit = (
                (pos.short_ce.entry_price + pos.short_pe.entry_price)
                - (pos.long_ce.entry_price  + pos.long_pe.entry_price)
            )

            await self._log_trade_db("ADJUST")
            logger.info(
                "IronCondor[%s]: adjustment complete — new_short=%.0f "
                "adj_pnl=₹%.0f cumulative=₹%.0f adj_count=%d/%d",
                self._underlying, new_short_strike,
                adj_pnl, pos.cumulative_adj_pnl * pos.lot_size,
                adj_count, pos.max_adj,
            )

            # ── Step 6: Hard stop if max_adjustments reached ──────────────────
            if adj_count >= pos.max_adj:
                logger.warning(
                    "IronCondor[%s]: MAX ADJUSTMENTS REACHED (%d/%d) — closing all.",
                    self._underlying, adj_count, pos.max_adj,
                )
                await self._close_position("max_adjustments_reached")

        except Exception as exc:
            # Mid-sequence failure: one or more legs may have been placed but not the
            # counterpart. Mark the position broken so no further auto-adjustments run.
            # The operator MUST manually reconcile the open legs in the broker terminal.
            if pos and pos.status != "closed":
                pos.status = "broken"
                await self._log_trade_db("BROKEN")
            logger.critical(
                "IronCondor[%s]: ADJUSTMENT FAILED mid-sequence — position %s marked BROKEN. "
                "Manual reconciliation required. Error: %s",
                self._underlying,
                pos.trade_id if pos else "?",
                exc,
                exc_info=True,
            )
        finally:
            self._adjusting = False

    # ── Position close ────────────────────────────────────────────────────────

    async def _close_position(self, reason: str) -> None:
        pos = self._position
        if not pos:
            return

        pos_pnl_rs = pos.total_pnl_rs
        logger.info(
            "IronCondor[%s]: CLOSE reason=%s total_pnl=₹%.0f adj_ce=%d adj_pe=%d",
            self._underlying, reason, pos_pnl_rs,
            pos.adj_count_ce, pos.adj_count_pe,
        )

        short_ce_ltp = self._prem_cache.get(f"{self._underlying}{int(pos.short_ce.strike)}CE", pos.short_ce.entry_price)
        short_pe_ltp = self._prem_cache.get(f"{self._underlying}{int(pos.short_pe.strike)}PE", pos.short_pe.entry_price)
        long_ce_ltp  = self._prem_cache.get(f"{self._underlying}{int(pos.long_ce.strike)}CE",  pos.long_ce.entry_price)
        long_pe_ltp  = self._prem_cache.get(f"{self._underlying}{int(pos.long_pe.strike)}PE",  pos.long_pe.entry_price)

        exit_ev = ICOrderEvent(
            action          = "EXIT",
            underlying      = pos.underlying,
            atm             = pos.atm_at_entry,
            short_ce_strike = pos.short_ce.strike,
            short_pe_strike = pos.short_pe.strike,
            short_ce_ltp    = short_ce_ltp,
            short_pe_ltp    = short_pe_ltp,
            long_ce_strike  = pos.long_ce.strike,
            long_pe_strike  = pos.long_pe.strike,
            long_ce_ltp     = long_ce_ltp,
            long_pe_ltp     = long_pe_ltp,
            lot_size        = pos.lot_size,
            close_reason    = reason,
            cumulative_pnl  = pos.cumulative_adj_pnl,
            event_id        = pos.trade_id,
        )
        await self._bus.publish(Topic.IC_ORDER_REQUEST, exit_ev)

        pos.status     = "closed"
        pos.close_time = datetime.now(IST)
        await self._log_trade_db("EXIT")
        self._position = None
        self._persist()   # clears the stored position

    # ── DB logging ────────────────────────────────────────────────────────────

    async def _log_trade_db(self, event: str) -> None:
        """Log trade state to DB for audit and reporting."""
        if not self._position:
            return
        pos = self._position
        try:
            from data_layer.client_db import ClientDB
            db = getattr(self._cfg, "_client_db", None) if self._cfg else None
            if db and hasattr(db, "upsert_ic_trade_log"):
                await asyncio.to_thread(
                    db.upsert_ic_trade_log,
                    trade_id         = pos.trade_id,
                    underlying       = pos.underlying,
                    event            = event,
                    short_ce_strike  = pos.short_ce.strike,
                    short_pe_strike  = pos.short_pe.strike,
                    long_ce_strike   = pos.long_ce.strike,
                    long_pe_strike   = pos.long_pe.strike,
                    net_credit       = pos.net_credit,
                    cumulative_adj_pnl = pos.cumulative_adj_pnl,
                    total_pnl_rs     = pos.total_pnl_rs,
                    adj_count_ce     = pos.adj_count_ce,
                    adj_count_pe     = pos.adj_count_pe,
                    status           = pos.status,
                    timestamp        = datetime.now(IST).isoformat(),
                )
        except Exception as exc:
            logger.warning("IronCondor[%s]: DB log failed: %s", self._underlying, exc)

    # ── Leg LTP updater ───────────────────────────────────────────────────────

    def _update_leg_ltp(self, tick) -> None:
        if not self._position:
            return
        for leg in self._position.legs:
            if abs(leg.strike - tick.strike) < 0.01 and leg.option_type == tick.option_type:
                leg.ltp = tick.ltp

    # ── Public accessors ──────────────────────────────────────────────────────

    @property
    def has_open_position(self) -> bool:
        return self._position is not None and self._position.status == "open"

    @property
    def position(self) -> Optional[IronCondorPosition]:
        return self._position


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_time(s: str) -> dtime:
    try:
        h, m = s.split(":")
        return dtime(int(h), int(m))
    except Exception:
        return dtime(9, 16)
