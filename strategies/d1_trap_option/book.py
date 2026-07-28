"""
strategies/d1_trap_option/book.py — live wrapper for D1 Trap + 2-ITM Option strategy.

Signal logic (ported from backtest/d1_trap_1h_entry/backtest.py C2+TWEAK):
  D1 3-candle trap zones are built from 200 days of daily bars at startup.
  C2:    1H bar enters zone → 1H breach of ref candle → set 5M trigger → BUY option.
  TWEAK: zone fails while MONITORING → counter-direction on next 1H breach → BUY option.
  Exit:  TSL (1H low/high ratchet checked on every 5M bar close) or EOD at 15:15 IST.

Option selection (NIFTY):
  LONG  → buy CE at ATM - 2×step (2-ITM)
  SHORT → buy PE at ATM + 2×step (2-ITM)
  Nearest active expiry from InstrumentRegistry.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Deque, Dict, List, Optional, Set

from config.global_config import IST, Topic
from data_layer.base_feeder import CandleEvent
from data_layer.instrument_registry import REGISTRY
from strategies.core import OrderEmitter
from strategies.core.base_book import AbstractStrategyBook
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones

logger = logging.getLogger(__name__)

_EOD_TIME = time(15, 15)
_ENTRY_CUTOFF = time(14, 0)
_SESSION_OPEN = time(9, 15)
_MAX_ZONE_AGE_DAYS = 20
_ITM_STRIKES = 2   # number of strikes in-the-money for option selection

# D1 data fetch key for NIFTY
_NIFTY_INDEX_KEY = "NSE_INDEX|Nifty 50"

# Cache directory shared with the D1 backtest
_CACHE_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "backtest", "d1_trap_1h_entry", "data_cache"
)


# ── internal bar / state dataclasses ─────────────────────────────────────────

@dataclass(frozen=True)
class _Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float


@dataclass
class _Monitor:
    direction: str          # "LONG" | "SHORT"
    d1_ref_ts: datetime
    d1_sweep_ts: datetime
    d1_reclaim_ts: datetime
    zone_lo: float
    zone_hi: float
    state: str = "WAITING"  # "WAITING" | "MONITORING"
    zone_entry_ts: Optional[datetime] = None
    ref_bar: Optional[_Bar] = None
    done: bool = False
    invalid: bool = False


@dataclass
class _TweakSetup:
    trade_dir: str          # "LONG" | "SHORT"
    failure_bar: _Bar
    zone_lo: float
    zone_hi: float
    d1_ref_ts: datetime
    d1_sweep_ts: datetime
    d1_reclaim_ts: datetime
    zone_entry_ts: datetime
    done: bool = False


@dataclass
class D1TrapOrderEvent:
    """Order event published on Topic.D1_TRAP_ORDER_REQUEST."""
    client_id: str
    binding_id: str
    strategy: str
    direction: str          # "LONG" | "SHORT"
    action: str             # "BUY" | "SELL"
    quantity: int
    entry_price: float      # spot price at trigger
    sl_price: float         # initial hard SL (spot)
    tsl_level: float        # current trailing SL (spot)
    trigger_ts: datetime
    reason: str
    underlying: str = ""
    option_symbol: str = ""
    option_type: str = ""   # "CE" | "PE"
    strike: int = 0
    expiry: Optional[date] = None
    order_type: str = "MARKET"


# ── book ─────────────────────────────────────────────────────────────────────

class D1TrapOptionBook(AbstractStrategyBook):
    """
    Live per-(client, binding, underlying) book for the D1 Trap + Option strategy.

    At startup: fetches 200 days of D1 bars and builds zone monitors.
    Live: aggregates 5M spot candles into 1H bars; runs C2+TWEAK zone logic;
          enters via 2-ITM option BUY; exits via TSL on 5M bars or EOD.
    """

    def __init__(
        self,
        bus,
        cfg,
        underlying: str,
        client_id: str,
        binding_id: str,
        lot_multiplier: int = 1,
        feeder_token: str = "",
    ) -> None:
        super().__init__(bus, cfg, underlying, client_id, binding_id)
        self._lot_multiplier = max(1, lot_multiplier)
        self._lot_size = cfg.exchange.lot_sizes.get(underlying, 75) if cfg else 75
        self._strike_step = int(cfg.exchange.strike_steps.get(underlying, 50) if cfg else 50)
        self._feeder_token = feeder_token
        self._emitter = OrderEmitter(bus, client_id, binding_id) if bus is not None else None

        # D1 zone state (rebuilt at startup, not reset intraday)
        self._monitors: List[_Monitor] = []
        self._tweak_setups: List[_TweakSetup] = []
        self._known_bear: Set[datetime] = set()
        self._known_bull: Set[datetime] = set()
        self._d1_loaded = False

        # Live 1H bar aggregation
        self._5m_deque: Deque[_Bar] = deque(maxlen=500)
        self._current_1h_open: Optional[datetime] = None
        self._current_1h_5m: List[_Bar] = []
        self._prev_h1_bar: Optional[_Bar] = None

        # Position / signal state
        self._position: Optional[Dict] = None
        self._pending_5m: Optional[Dict] = None

        self._last_spot: Optional[float] = None
        self._day_done = False

        # Spot symbol as published by the feeder/candle-cache
        self._spot_symbol = f"NSE_INDEX|{underlying}"

    # ── lifecycle ─────────────────────────────────────────────────────────────
    def start(self) -> None:
        super().start()
        self._subscribe(Topic.CANDLE_CLOSE)
        self._tasks.append(asyncio.create_task(
            self._candle_loop(), name=f"d1opt_candle_{self._underlying}"))
        self._tasks.append(asyncio.create_task(
            self._eod_loop(), name=f"d1opt_eod_{self._underlying}"))
        self._tasks.append(asyncio.create_task(
            self._startup_load(), name=f"d1opt_startup_{self._underlying}"))
        logger.info(
            "D1TrapOptionBook[%s/%s/%s]: started.",
            self._client_id, self._binding_id, self._underlying,
        )

    def reset_session(self) -> None:
        """Reset intraday state at end of day; zone monitors persist across days."""
        self._current_1h_open = None
        self._current_1h_5m.clear()
        self._prev_h1_bar = None
        self._position = None
        self._pending_5m = None
        self._last_spot = None
        self._day_done = False
        # Ref candles do not carry over to the next day
        for m in self._monitors:
            if m.state == "MONITORING":
                m.state = "WAITING"
                m.ref_bar = None
                m.zone_entry_ts = None

    # ── D1 data startup ───────────────────────────────────────────────────────
    async def _startup_load(self) -> None:
        if not self._feeder_token:
            logger.warning(
                "D1TrapOptionBook[%s]: no feeder token — D1 zones cannot be loaded; "
                "strategy will not trade.",
                self._underlying,
            )
            self._d1_loaded = True
            return
        try:
            today = datetime.now(IST).date()
            start = today - timedelta(days=200)
            d1_bars = await asyncio.to_thread(
                _fetch_d1_bars, _NIFTY_INDEX_KEY, start, today, self._feeder_token
            )
            self._rebuild_monitors(d1_bars, today)
            self._d1_loaded = True
            logger.info(
                "D1TrapOptionBook[%s]: %d D1 bars → %d zone monitors active.",
                self._underlying, len(d1_bars), len(self._monitors),
            )
        except Exception:
            logger.exception("D1TrapOptionBook[%s]: startup D1 load failed.", self._underlying)
            self._d1_loaded = True  # allow ticking without crash

    def _rebuild_monitors(self, d1_bars: List[_Bar], as_of: date) -> None:
        self._monitors.clear()
        self._known_bear.clear()
        self._known_bull.clear()
        self._tweak_setups.clear()

        avail = [b for b in d1_bars if b.timestamp.date() < as_of]
        if len(avail) < 3:
            return

        age_cutoff = (
            datetime.combine(as_of, datetime.min.time()).replace(tzinfo=IST)
            - timedelta(days=_MAX_ZONE_AGE_DAYS)
        )

        for z in find_all_bear_zones(avail):
            if z.reference_low_ts in self._known_bear:
                continue
            if z.lock_ts < age_cutoff:
                continue
            self._known_bear.add(z.reference_low_ts)
            self._monitors.append(_Monitor(
                direction="LONG",
                d1_ref_ts=z.reference_low_ts,
                d1_sweep_ts=z.sweep_started_ts,
                d1_reclaim_ts=z.lock_ts,
                zone_lo=min(z.entry_line, z.sweep_low),
                zone_hi=max(z.entry_line, z.sweep_low),
            ))

        for z in find_all_bull_zones(avail):
            if z.reference_low_ts in self._known_bull:
                continue
            if z.lock_ts < age_cutoff:
                continue
            self._known_bull.add(z.reference_low_ts)
            self._monitors.append(_Monitor(
                direction="SHORT",
                d1_ref_ts=z.reference_low_ts,
                d1_sweep_ts=z.sweep_started_ts,
                d1_reclaim_ts=z.lock_ts,
                zone_lo=min(z.entry_line, z.sweep_low),
                zone_hi=max(z.entry_line, z.sweep_low),
            ))

    # ── feed loops ────────────────────────────────────────────────────────────
    async def _candle_loop(self) -> None:
        q = self._loop_queues.get(Topic.CANDLE_CLOSE)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                self._on_candle(ev)
            except Exception:
                logger.exception(
                    "D1TrapOptionBook[%s]: error in candle handler.", self._underlying
                )

    async def _eod_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            now = datetime.now(IST)
            if now.time() >= _EOD_TIME and not self._day_done:
                if self._position is not None:
                    await self._square_off("eod")
                self._day_done = True

    # ── candle processing ─────────────────────────────────────────────────────
    def _on_candle(self, ev: CandleEvent) -> None:
        if not isinstance(ev, CandleEvent):
            return
        if not self._d1_loaded:
            return
        # Accept spot symbol from candle cache (NSE_INDEX|NIFTY or NSE_INDEX|Nifty 50)
        if ev.timeframe != 5:
            return
        is_spot = (
            ev.symbol == self._spot_symbol
            or (self._underlying == "NIFTY" and ev.symbol == "NSE_INDEX|Nifty 50")
        )
        if not is_spot:
            return

        now_t = ev.timestamp.time()
        if self._day_done or now_t < _SESSION_OPEN:
            return

        bar = _Bar(
            timestamp=ev.timestamp,
            open=ev.open, high=ev.high, low=ev.low, close=ev.close,
        )
        self._last_spot = ev.close
        self._5m_deque.append(bar)

        # ── 1H bucket management ─────────────────────────────────────────────
        h1_open = _h1_bucket(bar.timestamp)
        if self._current_1h_open is None:
            self._current_1h_open = h1_open
        elif h1_open != self._current_1h_open:
            # Previous 1H bar just closed
            if self._current_1h_5m:
                closed_h1 = _build_h1(self._current_1h_open, self._current_1h_5m)
                self._on_h1_close(closed_h1)
            self._current_1h_open = h1_open
            self._current_1h_5m = []

        self._current_1h_5m.append(bar)

        # ── Every-5M checks ──────────────────────────────────────────────────
        self._check_pending_trigger(bar)
        self._check_tsl_exit(bar)

    def _on_h1_close(self, bar: _Bar) -> None:
        """Called once per completed 1H bar. Zone monitoring + entry logic."""

        # TSL ratchet: update after each 1H bar close (uses previous bar's extreme)
        if self._position is not None and self._prev_h1_bar is not None:
            pos = self._position
            if pos["direction"] == "LONG":
                pos["tsl_level"] = max(pos["tsl_level"], self._prev_h1_bar.low)
            else:
                pos["tsl_level"] = min(pos["tsl_level"], self._prev_h1_bar.high)

        # Evict stale zones
        today = bar.timestamp.date()
        age_cutoff = (
            datetime.combine(today, datetime.min.time()).replace(tzinfo=IST)
            - timedelta(days=_MAX_ZONE_AGE_DAYS)
        )
        self._monitors = [
            m for m in self._monitors
            if not m.done and not m.invalid and m.d1_reclaim_ts >= age_cutoff
        ]
        self._tweak_setups = [
            t for t in self._tweak_setups
            if not t.done and t.d1_reclaim_ts >= age_cutoff
        ]

        # ── Zone monitoring + zone invalidation / TWEAK detection ────────────
        for m in self._monitors:
            if m.done or m.invalid:
                continue
            was_monitoring = (m.state == "MONITORING")

            if m.state == "WAITING":
                if m.direction == "LONG" and bar.low <= m.zone_hi:
                    m.state = "MONITORING"
                    m.zone_entry_ts = bar.timestamp
                    m.ref_bar = bar
                    was_monitoring = False
                elif m.direction == "SHORT" and bar.high >= m.zone_lo:
                    m.state = "MONITORING"
                    m.zone_entry_ts = bar.timestamp
                    m.ref_bar = bar
                    was_monitoring = False

            # Invalidation: 1H closes through the far zone boundary
            if m.direction == "LONG" and bar.close < m.zone_lo:
                m.invalid = True
                if was_monitoring and m.zone_entry_ts is not None:
                    self._tweak_setups.append(_TweakSetup(
                        trade_dir="SHORT", failure_bar=bar,
                        zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                        d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                        d1_reclaim_ts=m.d1_reclaim_ts, zone_entry_ts=m.zone_entry_ts,
                    ))
                    logger.info(
                        "D1TrapOptionBook[%s]: LONG zone %.2f–%.2f failed while MONITORING → TWEAK SHORT queued",
                        self._underlying, m.zone_lo, m.zone_hi,
                    )
            elif m.direction == "SHORT" and bar.close > m.zone_hi:
                m.invalid = True
                if was_monitoring and m.zone_entry_ts is not None:
                    self._tweak_setups.append(_TweakSetup(
                        trade_dir="LONG", failure_bar=bar,
                        zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                        d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                        d1_reclaim_ts=m.d1_reclaim_ts, zone_entry_ts=m.zone_entry_ts,
                    ))
                    logger.info(
                        "D1TrapOptionBook[%s]: SHORT zone %.2f–%.2f failed while MONITORING → TWEAK LONG queued",
                        self._underlying, m.zone_lo, m.zone_hi,
                    )

        self._prev_h1_bar = bar

        # No new entries at EOD cutoff or while flat
        if (bar.timestamp.time() >= _ENTRY_CUTOFF
                or self._position is not None
                or self._day_done):
            return

        # ── PRIORITY 1: C2 regular — 1H breach of zone ref candle ────────────
        if self._pending_5m is None:
            for m in self._monitors:
                if m.done or m.invalid or m.state != "MONITORING" or m.ref_bar is None:
                    continue
                ref = m.ref_bar
                direction = m.direction
                triggered = (
                    (direction == "LONG" and bar.high > ref.high)
                    or (direction == "SHORT" and bar.low < ref.low)
                )
                if not triggered:
                    m.ref_bar = bar  # roll ref forward
                    continue
                # Breach confirmed — entry at ref candle extreme
                sl_p = ref.low if direction == "LONG" else ref.high
                entry = ref.high if direction == "LONG" else ref.low
                if abs(entry - sl_p) <= 0:
                    m.ref_bar = bar
                    continue
                m.done = True
                self._pending_5m = {
                    "direction": direction,
                    "trigger": entry,
                    "sl": sl_p,
                    "zone_lo": m.zone_lo, "zone_hi": m.zone_hi,
                    "d1_ref_ts": m.d1_ref_ts,
                    "d1_sweep_ts": m.d1_sweep_ts,
                    "d1_reclaim_ts": m.d1_reclaim_ts,
                    "zone_entry_ts": m.zone_entry_ts,
                    "source": "C2",
                }
                logger.info(
                    "D1TrapOptionBook[%s]: C2 %s breach → pending trigger %.2f SL %.2f",
                    self._underlying, direction, entry, sl_p,
                )
                break

        # ── PRIORITY 2: TWEAK — immediate counter-trade on failure bar breach ─
        if self._pending_5m is None:
            for ts in self._tweak_setups:
                if ts.done or bar.timestamp <= ts.failure_bar.timestamp:
                    continue
                ref = ts.failure_bar
                if ts.trade_dir == "SHORT" and bar.low < ref.low:
                    sl_p = ref.high
                    entry = ref.low
                    if sl_p - entry > 0:
                        self._pending_5m = {
                            "direction": "SHORT",
                            "trigger": entry,
                            "sl": sl_p,
                            "zone_lo": ts.zone_lo, "zone_hi": ts.zone_hi,
                            "d1_ref_ts": ts.d1_ref_ts,
                            "d1_sweep_ts": ts.d1_sweep_ts,
                            "d1_reclaim_ts": ts.d1_reclaim_ts,
                            "zone_entry_ts": ts.zone_entry_ts,
                            "source": "TWEAK",
                        }
                        logger.info(
                            "D1TrapOptionBook[%s]: TWEAK SHORT trigger %.2f SL %.2f",
                            self._underlying, entry, sl_p,
                        )
                    ts.done = True
                    break
                elif ts.trade_dir == "LONG" and bar.high > ref.high:
                    sl_p = ref.low
                    entry = ref.high
                    if entry - sl_p > 0:
                        self._pending_5m = {
                            "direction": "LONG",
                            "trigger": entry,
                            "sl": sl_p,
                            "zone_lo": ts.zone_lo, "zone_hi": ts.zone_hi,
                            "d1_ref_ts": ts.d1_ref_ts,
                            "d1_sweep_ts": ts.d1_sweep_ts,
                            "d1_reclaim_ts": ts.d1_reclaim_ts,
                            "zone_entry_ts": ts.zone_entry_ts,
                            "source": "TWEAK",
                        }
                        logger.info(
                            "D1TrapOptionBook[%s]: TWEAK LONG trigger %.2f SL %.2f",
                            self._underlying, entry, sl_p,
                        )
                    ts.done = True
                    break

    # ── every-5M checks ───────────────────────────────────────────────────────
    def _check_pending_trigger(self, bar: _Bar) -> None:
        if self._pending_5m is None or self._position is not None:
            return
        pl = self._pending_5m
        hit = (
            (pl["direction"] == "LONG" and bar.high >= pl["trigger"])
            or (pl["direction"] == "SHORT" and bar.low <= pl["trigger"])
        )
        if not hit:
            return
        self._pending_5m = None
        asyncio.create_task(self._open_position(pl, bar.timestamp))

    def _check_tsl_exit(self, bar: _Bar) -> None:
        if self._position is None:
            return
        pos = self._position
        # Hard SL
        hard_hit = (
            (pos["direction"] == "LONG" and bar.close <= pos["sl"])
            or (pos["direction"] == "SHORT" and bar.close >= pos["sl"])
        )
        # Trailing SL (ratcheted after each 1H close; checked on every 5M close)
        tsl = pos["tsl_level"]
        tsl_hit = (
            (pos["direction"] == "LONG" and bar.close < tsl)
            or (pos["direction"] == "SHORT" and bar.close > tsl)
        )
        if hard_hit:
            asyncio.create_task(self._square_off("sl_hit"))
        elif tsl_hit:
            asyncio.create_task(self._square_off("tsl_hit"))

    # ── position open / close ─────────────────────────────────────────────────
    async def _open_position(self, trigger: Dict, ts: datetime) -> None:
        """Resolve option instrument key and emit BUY order event."""
        direction = trigger["direction"]
        spot = self._last_spot
        if spot is None:
            logger.warning("D1TrapOptionBook[%s]: trigger fired but no spot price.", self._underlying)
            return

        # Option selection: 2-ITM
        atm = round(spot / self._strike_step) * self._strike_step
        if direction == "LONG":
            strike = int(atm - _ITM_STRIKES * self._strike_step)
            opt_type = "CE"
        else:
            strike = int(atm + _ITM_STRIKES * self._strike_step)
            opt_type = "PE"

        today = ts.date()
        expiry = REGISTRY.get_active_expiry(self._underlying, today)
        if not expiry:
            logger.warning(
                "D1TrapOptionBook[%s]: no active expiry for %s — cannot enter.",
                self._underlying, today,
            )
            return

        opt_key = REGISTRY.get_upstox_key(self._underlying, expiry, strike, opt_type)
        if not opt_key:
            logger.warning(
                "D1TrapOptionBook[%s]: no Upstox key for %s%d%s exp=%s.",
                self._underlying, self._underlying, strike, opt_type, expiry,
            )
            return

        qty = self._lot_size * self._lot_multiplier
        self._position = {
            "direction": direction,
            "entry": trigger["trigger"],
            "sl": trigger["sl"],
            "tsl_level": trigger["sl"],  # initial TSL = initial SL
            "option_key": opt_key,
            "option_type": opt_type,
            "strike": strike,
            "expiry": expiry,
            "qty": qty,
            "entry_ts": ts,
            "source": trigger.get("source", "C2"),
        }

        ev = D1TrapOrderEvent(
            client_id=self._client_id,
            binding_id=self._binding_id,
            strategy="d1_trap_option",
            direction=direction,
            action="BUY",
            quantity=qty,
            entry_price=spot,
            sl_price=trigger["sl"],
            tsl_level=trigger["sl"],
            trigger_ts=ts,
            reason=trigger.get("source", "C2"),
            underlying=self._underlying,
            option_symbol=opt_key,
            option_type=opt_type,
            strike=strike,
            expiry=expiry,
            order_type="MARKET",
        )
        if self._emitter is not None:
            await self._emitter.emit(Topic.D1_TRAP_ORDER_REQUEST, ev)
        logger.info(
            "D1TrapOptionBook[%s]: BUY %s %d%s exp=%s qty=%d spot=%.2f SL=%.2f source=%s",
            self._underlying, opt_type, strike, self._underlying,
            expiry, qty, spot, trigger["sl"], trigger.get("source"),
        )

    async def _square_off(self, reason: str) -> None:
        pos = self._position
        if pos is None:
            return
        self._position = None
        self._pending_5m = None

        spot = self._last_spot or pos["entry"]
        ev = D1TrapOrderEvent(
            client_id=self._client_id,
            binding_id=self._binding_id,
            strategy="d1_trap_option",
            direction=pos["direction"],
            action="SELL",
            quantity=pos["qty"],
            entry_price=pos["entry"],
            sl_price=pos["sl"],
            tsl_level=pos["tsl_level"],
            trigger_ts=datetime.now(IST),
            reason=reason,
            underlying=self._underlying,
            option_symbol=pos["option_key"],
            option_type=pos["option_type"],
            strike=pos["strike"],
            expiry=pos["expiry"],
            order_type="MARKET",
        )
        if self._emitter is not None:
            await self._emitter.emit(Topic.D1_TRAP_ORDER_REQUEST, ev)
        logger.info(
            "D1TrapOptionBook[%s]: SELL %s strike=%d reason=%s spot=%.2f",
            self._underlying, pos["option_type"], pos["strike"], reason, spot,
        )

    async def liquidate(self, reason: str = "kill_switch") -> None:
        await self._square_off(reason)

    async def _tick_loop(self) -> None:
        pass

    async def _option_loop(self) -> None:
        pass


# ── helpers ───────────────────────────────────────────────────────────────────

def _h1_bucket(ts: datetime) -> datetime:
    """Return the open timestamp of the 1H bar that contains the given 5M bar timestamp."""
    open_dt = ts.replace(hour=9, minute=15, second=0, microsecond=0)
    mins_since_open = max(0, int((ts - open_dt).total_seconds() // 60))
    bucket_idx = mins_since_open // 60
    return open_dt + timedelta(hours=bucket_idx)


def _build_h1(h1_open: datetime, bars_5m: List[_Bar]) -> _Bar:
    """Aggregate a list of 5M bars into a single completed 1H bar."""
    return _Bar(
        timestamp=h1_open,
        open=bars_5m[0].open,
        high=max(b.high for b in bars_5m),
        low=min(b.low for b in bars_5m),
        close=bars_5m[-1].close,
    )


def _fetch_d1_bars(instrument_key: str, start: date, end: date, token: str) -> List[_Bar]:
    """Synchronous D1 fetch with disk cache. Call via asyncio.to_thread()."""
    from urllib.parse import quote as _q
    try:
        from curl_cffi import requests as _cc
    except ImportError:
        import urllib.request as _ureq

        class _cc:  # minimal fallback
            @staticmethod
            def get(url, headers, **kw):
                import json as _json
                req = _ureq.Request(url, headers=headers)
                with _ureq.urlopen(req, timeout=30) as r:
                    class _R:
                        def json(self): return _json.loads(r.read())
                    return _R()

    os.makedirs(_CACHE_DIR, exist_ok=True)
    safe = instrument_key.replace("|", "_").replace(" ", "_")
    cache_file = os.path.join(_CACHE_DIR, f"{safe}_day_{start}_{end}.json")

    if os.path.exists(cache_file):
        with open(cache_file) as f:
            raw = json.load(f)
    else:
        url = (
            f"https://api.upstox.com/v2/historical-candle/"
            f"{_q(instrument_key, safe='')}/day/{end.isoformat()}/{start.isoformat()}"
        )
        hdrs = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
        try:
            resp = _cc.get(url, headers=hdrs, impersonate="chrome131", timeout=30)
            raw = resp.json()
        except Exception as exc:
            logger.warning("D1 fetch failed: %s", exc)
            raw = {}
        if (raw.get("data") or {}).get("candles"):
            tmp = cache_file + ".tmp"
            with open(tmp, "w") as f:
                json.dump(raw, f)
            os.replace(tmp, cache_file)

    bars: List[_Bar] = []
    for c in reversed((raw.get("data") or {}).get("candles") or []):
        try:
            ts = datetime.fromisoformat(c[0]).astimezone(IST)
            bars.append(_Bar(
                timestamp=ts,
                open=float(c[1]), high=float(c[2]),
                low=float(c[3]), close=float(c[4]),
            ))
        except Exception:
            pass
    return bars
