"""
strategies/d1_trap_option/book.py — Trap Scanner live book (Index + FnO).

Supports two strategy modes, selected by strategy_name:

  "d1_trap_index"  — Index (NIFTY/SENSEX/BANKNIFTY)
    HTF: configurable (default D1 | 75min | 1H) for zone detection
    MTF: configurable (default 1H | 15min) for monitoring + entry
    LTF: 5M for trigger
    Product: MIS — EOD force-exit at 15:15 IST
    TSL: ratchets after every MTF bar close

  "d1_trap_fno"  — FnO Stocks (RELIANCE, HDFCBANK, ...)
    HTF: D1 (fetched from Upstox REST at startup + refreshed daily at 15:30)
    MTF: 1H (default) for monitoring + entry
    LTF: 5M for trigger
    Product: NRML — NO EOD close; holds overnight as positional
    TSL: ratchets ONLY on D1 bar close (once per day at 15:30 IST)

Option selection:
    LONG  → buy 1-ITM CE (ATM − 1×step)
    SHORT → buy 1-ITM PE (ATM + 1×step)
    Configurable via itm_offset param.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional, Set

from config.global_config import IST, Topic, FNO_STOCK_CONFIG, fno_stock_lot, fno_stock_step
from data_layer.base_feeder import CandleEvent
from data_layer.instrument_registry import REGISTRY
from strategies.core.base_book import AbstractStrategyBook
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones

logger = logging.getLogger(__name__)

_EOD_TIME     = time(15, 15)
_D1_CLOSE     = time(15, 30)   # when we fetch the new D1 bar for positional mode
_ENTRY_CUTOFF = time(14, 0)
_SESSION_OPEN = time(9, 15)
_MAX_ZONE_AGE_DAYS = 20        # D1 zone max age
_MAX_ZONE_BARS_75M = 50        # ~10 trading days of 75M bars for zone age cap

# Startup fetch window
_HTF_WARMUP_DAYS   = 200       # D1: 200 days history
_HTF_WARMUP_1M_DAYS = 30       # 75M: 30 days of 1M bars → resample

_CACHE_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "trap_zone_cache"
)

# Timeframe string → integer minutes (0 = D1/daily)
_TF_MINUTES: Dict[str, int] = {
    "D1": 0, "day": 0, "daily": 0,
    "75min": 75, "75m": 75,
    "1H": 60, "60min": 60,
    "15min": 15, "15m": 15,
    "5min": 5,  "5m": 5,
}


def _parse_tf(tf_str: str, default: int) -> int:
    return _TF_MINUTES.get(str(tf_str).strip(), default)


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
    htf_ref_ts: datetime
    htf_sweep_ts: datetime
    htf_reclaim_ts: datetime
    zone_lo: float
    zone_hi: float
    state: str = "WAITING"  # "WAITING" | "MONITORING"
    zone_entry_ts: Optional[datetime] = None
    ref_bar: Optional[_Bar] = None
    done: bool = False
    invalid: bool = False


@dataclass
class _TweakSetup:
    trade_dir: str
    failure_bar: _Bar
    zone_lo: float
    zone_hi: float
    htf_ref_ts: datetime
    htf_sweep_ts: datetime
    htf_reclaim_ts: datetime
    zone_entry_ts: datetime
    done: bool = False


@dataclass
class D1TrapOrderEvent:
    """Order event published on Topic.D1_TRAP_ORDER_REQUEST."""
    client_id: str
    binding_id: str
    strategy: str            # "d1_trap_index" | "d1_trap_fno"
    direction: str           # "LONG" | "SHORT"
    action: str              # "BUY" | "SELL"
    quantity: int
    entry_price: float       # spot price at trigger
    sl_price: float
    tsl_level: float
    trigger_ts: datetime
    reason: str
    underlying: str = ""
    option_symbol: str = ""
    option_type: str = ""    # "CE" | "PE"
    strike: int = 0
    expiry: Optional[date] = None
    order_type: str = "MARKET"
    product_type: str = "MIS"
    exit_price: float = 0.0  # 2026-08-03: real fill price for a SELL/exit event --
                              # entry_price on a SELL event is the ORIGINAL entry (carried
                              # through for reference/P&L), not the exit fill. Without this,
                              # the bridge had no way to know the real exit price and
                              # silently reused entry_price for both, making every recorded
                              # trade look like a zero-P&L round trip at the entry price.
    entry_reason: str = ""   # 2026-08-03: the ORIGINAL order_reason this leg was opened
                              # with (e.g. "bear_trap_flip_t1") -- a SELL event's own
                              # `reason` field is the CLOSE reason (eod/sl_hit/...); without
                              # this the History ledger's "open" row showed the close
                              # reason on the entry leg, since that was the only reason
                              # string the bridge ever had access to.
    entry_ts: Optional[datetime] = None  # 2026-08-03: real entry timestamp, so the History
                              # ledger's open row shows the actual entry time, not blank.
    event_id: str = ""       # 2026-08-05: correlates this request with the D1TrapFillEvent
                              # the bridge publishes back on Topic.D1_TRAP_ORDER_FILL, so the
                              # book can match a confirm/abort to the exact leg that dispatched
                              # it (confirm-then-finalize, mirrors StraddleOrderEvent.event_id).


# ── book ─────────────────────────────────────────────────────────────────────

class D1TrapOptionBook(AbstractStrategyBook):
    """
    Per-(client, binding, underlying) Trap Scanner live book.

    strategy_name = "d1_trap_index": intraday MIS, configurable HTF/MTF.
    strategy_name = "d1_trap_fno":   positional NRML, D1 zones, D1 TSL.
    """

    def __init__(
        self,
        bus,
        cfg,
        underlying: str,
        client_id: str,
        binding_id: str,
        strategy_name: str = "d1_trap_index",
        lot_multiplier: int = 1,
        feeder_token: str = "",
        htf_tf: str = "D1",      # "D1" | "75min" | "1H"
        mtf_tf: str = "75min",   # "75min" | "1H" | "15min"
        itm_offset: int = 1,     # 1-ITM default
        product_type: str = "MIS",
        upstox_key: str = "",    # override instrument key (for WATCHLIST stocks not in FNO_STOCK_CONFIG)
        lot_override: int = 0,   # override lot size (from watchlist JSON)
        step_override: int = 0,  # override strike step (from watchlist JSON)
    ) -> None:
        super().__init__(bus, cfg, underlying, client_id, binding_id)
        self._strategy_name = strategy_name
        self._positional = (strategy_name == "d1_trap_fno")
        self._lot_multiplier = max(1, lot_multiplier)
        self._feeder_token = feeder_token
        self._product_type = product_type
        self._upstox_key_override = upstox_key  # used in _load_htf_bars if set

        # Lot size and strike step — explicit override (WATCHLIST) > FNO_STOCK_CONFIG > index config
        if lot_override > 0:
            self._lot_size = lot_override
        elif self._positional:
            self._lot_size = fno_stock_lot(underlying)
        else:
            self._lot_size = (cfg.exchange.lot_sizes.get(underlying, 75) if cfg else 75)

        if step_override > 0:
            self._strike_step = step_override
        elif self._positional:
            self._strike_step = fno_stock_step(underlying)
        else:
            self._strike_step = int(cfg.exchange.strike_steps.get(underlying, 50) if cfg else 50)

        self._itm_offset = max(1, itm_offset)

        # Timeframe parameters
        self._htf_mins = _parse_tf(htf_tf, 0)   # 0 = D1/daily
        self._mtf_mins = _parse_tf(mtf_tf, 75)  # 75 = 75min default

        # HTF zone state
        self._htf_bars: List[_Bar] = []          # accumulated HTF bars (D1 or 75M etc.)
        self._monitors: List[_Monitor] = []
        self._tweak_setups: List[_TweakSetup] = []
        self._known_bear: Set[datetime] = set()
        self._known_bull: Set[datetime] = set()
        self._htf_loaded = False

        # MTF bar accumulation (built from 5M candles)
        self._current_mtf_open: Optional[datetime] = None
        self._current_mtf_5m: List[_Bar] = []
        self._prev_mtf_bar: Optional[_Bar] = None

        # HTF intraday bar accumulation (only when htf_mins > 0 and not D1)
        self._current_htf_open: Optional[datetime] = None
        self._current_htf_5m: List[_Bar] = []

        # Position / signal state
        self._position: Optional[Dict] = None
        self._pending_5m: Optional[Dict] = None
        self._last_spot: Optional[float] = None
        self._day_done = False
        self._d1_fetched_today = False   # guard against re-fetching D1 bar same day
        self._warming_up = False         # True while replaying intraday history at startup
        # 2026-08-22 fix: reset_session() had zero live call sites anywhere in
        # this file (same class of bug independently found and fixed in
        # strategies/fvg/engine.py the same day) -- see _on_candle's new
        # day-check and _startup_load's new self._today assignment below.
        self._today: Optional[date] = None

        # Candle symbol filter — index vs equity
        if self._positional:
            self._spot_symbol = underlying.upper()  # "RELIANCE" from equity tick
        else:
            self._spot_symbol = f"NSE_INDEX|{underlying}"

        logger.info(
            "D1TrapOptionBook[%s/%s/%s]: strategy=%s htf=%s mtf=%s itm=%d positional=%s lot=%d step=%d",
            client_id, binding_id, underlying,
            strategy_name, htf_tf, mtf_tf, itm_offset,
            self._positional, self._lot_size, self._strike_step,
        )

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        super().start()
        self._subscribe(Topic.CANDLE_CLOSE)
        self._subscribe(Topic.INDEX_TICK)   # for real-time _last_spot update
        self._tasks.append(asyncio.create_task(
            self._candle_loop(), name=f"trap_candle_{self._underlying}"))
        self._tasks.append(asyncio.create_task(
            self._tick_loop(), name=f"trap_tick_{self._underlying}"))
        if not self._positional:
            self._tasks.append(asyncio.create_task(
                self._eod_loop(), name=f"trap_eod_{self._underlying}"))
        self._tasks.append(asyncio.create_task(
            self._d1_daily_refresh_loop(), name=f"trap_d1refresh_{self._underlying}"))
        self._tasks.append(asyncio.create_task(
            self._startup_load(), name=f"trap_startup_{self._underlying}"))

    def reset_session(self) -> None:
        """Reset intraday state; HTF zone monitors and positional trade persist across days."""
        self._current_mtf_open = None
        self._current_mtf_5m.clear()
        self._prev_mtf_bar = None
        self._current_htf_open = None
        self._current_htf_5m.clear()
        self._last_spot = None
        self._day_done = False
        self._d1_fetched_today = False
        self._pending_5m = None
        # Positional: keep the open position across reset_session
        if not self._positional:
            self._position = None
        # Ref candles don't carry over to a new day
        for m in self._monitors:
            if m.state == "MONITORING":
                m.state = "WAITING"
                m.ref_bar = None
                m.zone_entry_ts = None

    # ── HTF zone loading ──────────────────────────────────────────────────────

    async def _startup_load(self) -> None:
        # CRITICAL (2026-08-22, same pattern as every other strategy's mid-day
        # warmup in this codebase): self._today must be set to TODAY on every
        # exit path of this method, BEFORE the first live candle can trigger
        # _on_candle's own "if self._today != today: reset_session()" check --
        # otherwise that first live candle (self._today still None) would
        # itself fire reset_session() and wipe out everything just warmed up
        # (monitors, zone state) with zero log trace.
        today = datetime.now(IST).date()
        if not self._feeder_token:
            logger.warning(
                "TrapBook[%s]: no feeder token — HTF zones cannot be loaded; strategy idle.",
                self._underlying,
            )
            self._today = today
            self._htf_loaded = True
            return
        try:
            if self._htf_mins == 0:
                # D1 mode — fetch daily bars
                key = self._upstox_key_override or _upstox_key_for(self._underlying)
                start = today - timedelta(days=_HTF_WARMUP_DAYS)
                bars = await asyncio.to_thread(
                    _fetch_bars, key, "day", start, today, self._feeder_token
                )
                self._htf_bars = bars
            else:
                # Sub-daily HTF (75min / 60min) — fetch 1M bars and resample
                key = self._upstox_key_override or _upstox_key_for(self._underlying)
                start = today - timedelta(days=_HTF_WARMUP_1M_DAYS)
                bars_1m = await asyncio.to_thread(
                    _fetch_1m_bars, key, start, today, self._feeder_token
                )
                self._htf_bars = _resample(bars_1m, self._htf_mins)

            self._rebuild_monitors(today)
            logger.info(
                "TrapBook[%s]: %d HTF bars → %d zone monitors.",
                self._underlying, len(self._htf_bars), len(self._monitors),
            )
            # Replay today's intraday bars so zone states reflect current market
            await self._warmup_intraday(today)
            self._today = today
            self._htf_loaded = True
        except Exception:
            logger.exception("TrapBook[%s]: startup load failed.", self._underlying)
            self._today = today
            self._htf_loaded = True

    async def _warmup_intraday(self, today: date) -> None:
        """Fetch today's 5M bars and replay through zone logic to seed correct state.

        Zone contact (WAITING→MONITORING) and zone failure (invalid) are applied.
        C2 trigger and order placement are suppressed via self._warming_up flag
        so no phantom trades fire from historical intraday data.
        """
        now = datetime.now(IST)
        if now.time() < _SESSION_OPEN:
            return  # pre-market, nothing to replay

        key = self._upstox_key_override or _upstox_key_for(self._underlying)
        try:
            bars_5m = await asyncio.to_thread(
                _fetch_intraday_5m, key, self._feeder_token
            )
        except Exception as exc:
            logger.warning("TrapBook[%s]: intraday warmup fetch failed: %s", self._underlying, exc)
            return

        if not bars_5m:
            logger.warning("TrapBook[%s]: intraday warmup — 0 bars returned (API empty or key mismatch).", self._underlying)
            return

        self._warming_up = True
        try:
            mtf_bucket_bars: List[_Bar] = []
            current_bucket: Optional[datetime] = None

            for bar in bars_5m:
                if bar.timestamp.time() < _SESSION_OPEN:
                    continue
                self._last_spot = bar.close

                # Zone contact: WAITING → MONITORING
                self._check_zone_contact(bar)

                # Accumulate into MTF bucket
                bucket_open = _mtf_bucket(bar.timestamp, self._mtf_mins)
                if current_bucket is None:
                    current_bucket = bucket_open

                if bucket_open != current_bucket:
                    # MTF bar just closed — build synthetic bar and process
                    if mtf_bucket_bars:
                        mtf_bar = _Bar(
                            timestamp=current_bucket + timedelta(minutes=self._mtf_mins),
                            open=mtf_bucket_bars[0].open,
                            high=max(b.high for b in mtf_bucket_bars),
                            low=min(b.low for b in mtf_bucket_bars),
                            close=mtf_bucket_bars[-1].close,
                        )
                        self._on_mtf_close(mtf_bar)
                    current_bucket = bucket_open
                    mtf_bucket_bars = []

                mtf_bucket_bars.append(bar)

            active = sum(1 for m in self._monitors if not m.done and not m.invalid)
            monitoring = sum(1 for m in self._monitors
                             if not m.done and not m.invalid and m.state == "MONITORING")
            logger.info(
                "TrapBook[%s]: intraday warmup complete — %d 5M bars replayed, "
                "%d monitors active (%d MONITORING).",
                self._underlying, len(bars_5m), active, monitoring,
            )
        finally:
            self._warming_up = False

    def _rebuild_monitors(self, as_of: date) -> None:
        """Rebuild zone monitors from self._htf_bars."""
        self._monitors.clear()
        self._known_bear.clear()
        self._known_bull.clear()
        self._tweak_setups.clear()

        avail = [b for b in self._htf_bars if b.timestamp.date() < as_of]
        if len(avail) < 3:
            return

        age_cutoff = (
            datetime.combine(as_of, datetime.min.time()).replace(tzinfo=IST)
            - timedelta(days=_MAX_ZONE_AGE_DAYS)
        )

        for z in find_all_bear_zones(avail):
            if z.reference_low_ts in self._known_bear or z.lock_ts < age_cutoff:
                continue
            self._known_bear.add(z.reference_low_ts)
            self._monitors.append(_Monitor(
                direction="LONG",
                htf_ref_ts=z.reference_low_ts,
                htf_sweep_ts=z.sweep_started_ts,
                htf_reclaim_ts=z.lock_ts,
                zone_lo=min(z.entry_line, z.sweep_low),
                zone_hi=max(z.entry_line, z.sweep_low),
            ))

        for z in find_all_bull_zones(avail):
            if z.reference_low_ts in self._known_bull or z.lock_ts < age_cutoff:
                continue
            self._known_bull.add(z.reference_low_ts)
            self._monitors.append(_Monitor(
                direction="SHORT",
                htf_ref_ts=z.reference_low_ts,
                htf_sweep_ts=z.sweep_started_ts,
                htf_reclaim_ts=z.lock_ts,
                zone_lo=min(z.entry_line, z.sweep_low),
                zone_hi=max(z.entry_line, z.sweep_low),
            ))

    def _discover_new_zones(self, today: date) -> None:
        """Incremental zone discovery after a new HTF bar is added."""
        if len(self._htf_bars) < 3:
            return
        age_cutoff = (
            datetime.combine(today, datetime.min.time()).replace(tzinfo=IST)
            - timedelta(days=_MAX_ZONE_AGE_DAYS)
        )
        for z in find_all_bear_zones(self._htf_bars, known_ref_ts=self._known_bear):
            if z.reference_low_ts not in self._known_bear and z.lock_ts >= age_cutoff:
                self._known_bear.add(z.reference_low_ts)
                self._monitors.append(_Monitor(
                    direction="LONG",
                    htf_ref_ts=z.reference_low_ts,
                    htf_sweep_ts=z.sweep_started_ts,
                    htf_reclaim_ts=z.lock_ts,
                    zone_lo=min(z.entry_line, z.sweep_low),
                    zone_hi=max(z.entry_line, z.sweep_low),
                ))
        for z in find_all_bull_zones(self._htf_bars, known_ref_ts=self._known_bull):
            if z.reference_low_ts not in self._known_bull and z.lock_ts >= age_cutoff:
                self._known_bull.add(z.reference_low_ts)
                self._monitors.append(_Monitor(
                    direction="SHORT",
                    htf_ref_ts=z.reference_low_ts,
                    htf_sweep_ts=z.sweep_started_ts,
                    htf_reclaim_ts=z.lock_ts,
                    zone_lo=min(z.entry_line, z.sweep_low),
                    zone_hi=max(z.entry_line, z.sweep_low),
                ))

    # ── feed loops ────────────────────────────────────────────────────────────

    async def _tick_loop(self) -> None:
        """Update _last_spot from live INDEX_TICK so SPOT shows real price immediately
        (not just from 5M candle closes, which lag by up to 5 minutes at startup)."""
        from data_layer.base_feeder import IndexTick
        q = self._loop_queues.get(Topic.INDEX_TICK)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                if not isinstance(ev, IndexTick):
                    continue
                is_spot = (
                    ev.symbol == self._spot_symbol
                    or ev.symbol == self._underlying
                    or (self._underlying == "NIFTY"
                        and ev.symbol in ("NSE_INDEX|Nifty 50", "NIFTY", "NSE:NIFTY50-INDEX"))
                    or (self._underlying == "SENSEX"
                        and ev.symbol in ("BSE_INDEX|SENSEX", "SENSEX"))
                    or (self._underlying == "BANKNIFTY"
                        and ev.symbol in ("NSE_INDEX|Nifty Bank", "BANKNIFTY"))
                )
                if is_spot and ev.ltp and ev.ltp > 0:
                    self._last_spot = float(ev.ltp)
            except Exception:
                pass

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
                logger.exception("TrapBook[%s]: candle handler error.", self._underlying)

    async def _eod_loop(self) -> None:
        """Intraday only: force-exit at 15:15 IST."""
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

    async def _d1_daily_refresh_loop(self) -> None:
        """
        At 15:30 IST: fetch today's completed D1 (or HTF) bar and add to zone list.
        For D1 positional mode: also ratchets TSL with today's completed bar.
        For sub-daily HTF: handled live by on_candle; this loop is a no-op.
        """
        while self._running:
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            now = datetime.now(IST)
            if now.time() < _D1_CLOSE:
                continue
            if self._d1_fetched_today:
                continue
            if self._htf_mins != 0:
                # Sub-daily HTF: bars are built live; no REST fetch needed here
                self._d1_fetched_today = True
                continue
            # D1 mode: fetch today's bar
            self._d1_fetched_today = True
            if not self._feeder_token:
                continue
            try:
                today = now.date()
                key = self._upstox_key_override or _upstox_key_for(self._underlying)
                bars = await asyncio.to_thread(
                    _fetch_bars, key, "day", today, today, self._feeder_token
                )
                if bars:
                    today_bar = bars[-1]
                    if today_bar.timestamp.date() == today:
                        self._htf_bars.append(today_bar)
                        self._discover_new_zones(today)
                        # Positional D1 TSL: ratchet on daily close
                        if self._positional and self._position is not None:
                            pos = self._position
                            if pos["direction"] == "LONG":
                                pos["tsl_level"] = max(pos["tsl_level"], today_bar.low)
                            else:
                                pos["tsl_level"] = min(pos["tsl_level"], today_bar.high)
                            logger.info(
                                "TrapBook[%s]: D1 close TSL ratchet → %.2f (bar lo=%.2f hi=%.2f)",
                                self._underlying, pos["tsl_level"], today_bar.low, today_bar.high,
                            )
            except Exception:
                logger.exception("TrapBook[%s]: D1 daily refresh failed.", self._underlying)

    # ── candle processing ─────────────────────────────────────────────────────

    def _on_candle(self, ev: CandleEvent) -> None:
        if not isinstance(ev, CandleEvent):
            return
        if not self._htf_loaded:
            return
        if ev.timeframe != 5:
            return
        # Accept both canonical format and short name
        is_spot = (
            ev.symbol == self._spot_symbol
            or ev.symbol == self._underlying
            or (self._underlying == "NIFTY" and ev.symbol in ("NSE_INDEX|Nifty 50", "NIFTY"))
            or (self._underlying == "SENSEX" and ev.symbol in ("BSE_INDEX|SENSEX", "SENSEX"))
        )
        if not is_spot:
            return

        # 2026-08-22 CRITICAL FIX: reset_session() had zero live call sites --
        # self._day_done, once set True by _eod_loop at 15:15 (non-positional/
        # d1_trap_index only -- positional/d1_trap_fno never runs _eod_loop,
        # so it was never actually exposed to this bug), was never reset back
        # to False. Every candle for every day after the first would be
        # silently dropped by the `not self._positional and self._day_done`
        # gate below, forever. Applied uniformly to both variants for
        # consistency -- harmless no-op timing for positional, since
        # reset_session() already deliberately preserves self._position and
        # HTF zone monitors across days for that case.
        today = ev.timestamp.date()
        if self._today != today:
            self.reset_session()
            self._today = today

        now_t = ev.timestamp.time()
        if now_t < _SESSION_OPEN:
            return
        if not self._positional and self._day_done:
            return

        bar = _Bar(
            timestamp=ev.timestamp,
            open=ev.open, high=ev.high, low=ev.low, close=ev.close,
        )
        self._last_spot = ev.close

        # ── MTF bar management (1H or 15M) ───────────────────────────────────
        mtf_open = _mtf_bucket(bar.timestamp, self._mtf_mins)
        if self._current_mtf_open is None:
            self._current_mtf_open = mtf_open
        elif mtf_open != self._current_mtf_open:
            if self._current_mtf_5m:
                closed_mtf = _build_bar(self._current_mtf_open, self._current_mtf_5m)
                self._on_mtf_close(closed_mtf)
            self._current_mtf_open = mtf_open
            self._current_mtf_5m = []
        self._current_mtf_5m.append(bar)

        # ── Sub-daily HTF bar management (75M etc.) ───────────────────────────
        if self._htf_mins > 0:
            htf_open = _mtf_bucket(bar.timestamp, self._htf_mins)
            if self._current_htf_open is None:
                self._current_htf_open = htf_open
            elif htf_open != self._current_htf_open:
                if self._current_htf_5m:
                    closed_htf = _build_bar(self._current_htf_open, self._current_htf_5m)
                    self._htf_bars.append(closed_htf)
                    self._discover_new_zones(bar.timestamp.date())
                    logger.debug(
                        "TrapBook[%s]: new %dM HTF bar closed @ %.2f → %d zones",
                        self._underlying, self._htf_mins, closed_htf.close, len(self._monitors),
                    )
                self._current_htf_open = htf_open
                self._current_htf_5m = []
            self._current_htf_5m.append(bar)

        # ── Every-5M checks ───────────────────────────────────────────────────
        self._check_zone_contact(bar)  # WAITING → MONITORING as soon as price touches zone
        self._check_pending_trigger(bar)
        self._check_exit(bar)

    def _check_zone_contact(self, bar: _Bar) -> None:
        """Per-5M: flip WAITING → MONITORING as soon as price enters a zone.
        ref_bar is left None here — the FIRST MTF close after contact sets it,
        and the SECOND MTF close checks for the C2 trigger (avoids premature entry)."""
        today = bar.timestamp.date()
        age_cutoff = (
            datetime.combine(today, datetime.min.time()).replace(tzinfo=IST)
            - timedelta(days=_MAX_ZONE_AGE_DAYS)
        )
        for m in self._monitors:
            if m.done or m.invalid or m.state != "WAITING":
                continue
            if m.htf_reclaim_ts < age_cutoff:
                continue
            if m.direction == "LONG" and bar.low <= m.zone_hi:
                m.state = "MONITORING"
                m.zone_entry_ts = bar.timestamp
                m.ref_bar = None   # first MTF close will set the proper ref bar
                logger.info(
                    "TrapBook[%s]: LONG zone %.2f-%.2f → MONITORING at 5M close (spot=%.2f)",
                    self._underlying, m.zone_lo, m.zone_hi, bar.close,
                )
            elif m.direction == "SHORT" and bar.high >= m.zone_lo:
                m.state = "MONITORING"
                m.zone_entry_ts = bar.timestamp
                m.ref_bar = None
                logger.info(
                    "TrapBook[%s]: SHORT zone %.2f-%.2f → MONITORING at 5M close (spot=%.2f)",
                    self._underlying, m.zone_lo, m.zone_hi, bar.close,
                )

    def _on_mtf_close(self, bar: _Bar) -> None:
        """C2/TWEAK entry detection, zone invalidation, and TSL ratchet."""

        # Intraday TSL: ratchet after each MTF close (positional TSL done in D1 loop)
        if not self._positional and self._position is not None and self._prev_mtf_bar is not None:
            pos = self._position
            if pos["direction"] == "LONG":
                pos["tsl_level"] = max(pos["tsl_level"], self._prev_mtf_bar.low)
            else:
                pos["tsl_level"] = min(pos["tsl_level"], self._prev_mtf_bar.high)

        # Evict stale zones
        today = bar.timestamp.date()
        age_cutoff = (
            datetime.combine(today, datetime.min.time()).replace(tzinfo=IST)
            - timedelta(days=_MAX_ZONE_AGE_DAYS)
        )
        self._monitors = [
            m for m in self._monitors
            if not m.done and not m.invalid and m.htf_reclaim_ts >= age_cutoff
        ]
        self._tweak_setups = [
            t for t in self._tweak_setups
            if not t.done and t.htf_reclaim_ts >= age_cutoff
        ]

        # ── Zone invalidation / TWEAK (no WAITING→MONITORING here; done per 5M) ─
        for m in self._monitors:
            if m.done or m.invalid:
                continue
            was_monitoring = (m.state == "MONITORING")

            # If zone is MONITORING but ref_bar not yet set: this MTF bar becomes the ref.
            # Don't trigger yet — wait for the NEXT MTF bar to confirm the C2 breach.
            if m.state == "MONITORING" and m.ref_bar is None:
                m.ref_bar = bar
                continue

            # Zone failure → TWEAK
            if m.direction == "LONG" and bar.close < m.zone_lo:
                m.invalid = True
                if was_monitoring and m.zone_entry_ts is not None:
                    self._tweak_setups.append(_TweakSetup(
                        trade_dir="SHORT", failure_bar=bar,
                        zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                        htf_ref_ts=m.htf_ref_ts, htf_sweep_ts=m.htf_sweep_ts,
                        htf_reclaim_ts=m.htf_reclaim_ts, zone_entry_ts=m.zone_entry_ts,
                    ))
                    logger.info(
                        "TrapBook[%s]: LONG zone %.2f-%.2f failed → TWEAK SHORT queued",
                        self._underlying, m.zone_lo, m.zone_hi,
                    )
            elif m.direction == "SHORT" and bar.close > m.zone_hi:
                m.invalid = True
                if was_monitoring and m.zone_entry_ts is not None:
                    self._tweak_setups.append(_TweakSetup(
                        trade_dir="LONG", failure_bar=bar,
                        zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                        htf_ref_ts=m.htf_ref_ts, htf_sweep_ts=m.htf_sweep_ts,
                        htf_reclaim_ts=m.htf_reclaim_ts, zone_entry_ts=m.zone_entry_ts,
                    ))
                    logger.info(
                        "TrapBook[%s]: SHORT zone %.2f-%.2f failed → TWEAK LONG queued",
                        self._underlying, m.zone_lo, m.zone_hi,
                    )

        self._prev_mtf_bar = bar

        # Gate: no new entry during warmup replay, at cutoff, or if position/pending open
        if (self._warming_up
                or bar.timestamp.time() >= _ENTRY_CUTOFF
                or self._position is not None
                or self._pending_5m is not None
                or (not self._positional and self._day_done)):
            return

        # ── C2: MTF breach of zone ref candle → pending 5M trigger ───────────
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
                m.ref_bar = bar   # roll ref forward
                continue
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
                "htf_ref_ts": m.htf_ref_ts,
                "htf_sweep_ts": m.htf_sweep_ts,
                "htf_reclaim_ts": m.htf_reclaim_ts,
                "zone_entry_ts": m.zone_entry_ts,
                "source": "C2",
            }
            logger.info(
                "TrapBook[%s]: C2 %s breach → pending 5M trigger %.2f SL %.2f",
                self._underlying, direction, entry, sl_p,
            )
            return

        # ── TWEAK: failure bar breach → counter-direction pending trigger ─────
        for ts in self._tweak_setups:
            if ts.done or bar.timestamp <= ts.failure_bar.timestamp:
                continue
            ref = ts.failure_bar
            if ts.trade_dir == "SHORT" and bar.low < ref.low:
                sl_p = ref.high
                entry = ref.low
                if sl_p - entry > 0:
                    self._pending_5m = {
                        "direction": "SHORT", "trigger": entry, "sl": sl_p,
                        "zone_lo": ts.zone_lo, "zone_hi": ts.zone_hi,
                        "htf_ref_ts": ts.htf_ref_ts, "htf_sweep_ts": ts.htf_sweep_ts,
                        "htf_reclaim_ts": ts.htf_reclaim_ts, "zone_entry_ts": ts.zone_entry_ts,
                        "source": "TWEAK",
                    }
                    logger.info(
                        "TrapBook[%s]: TWEAK SHORT trigger %.2f SL %.2f",
                        self._underlying, entry, sl_p,
                    )
                ts.done = True
                return
            elif ts.trade_dir == "LONG" and bar.high > ref.high:
                sl_p = ref.low
                entry = ref.high
                if entry - sl_p > 0:
                    self._pending_5m = {
                        "direction": "LONG", "trigger": entry, "sl": sl_p,
                        "zone_lo": ts.zone_lo, "zone_hi": ts.zone_hi,
                        "htf_ref_ts": ts.htf_ref_ts, "htf_sweep_ts": ts.htf_sweep_ts,
                        "htf_reclaim_ts": ts.htf_reclaim_ts, "zone_entry_ts": ts.zone_entry_ts,
                        "source": "TWEAK",
                    }
                    logger.info(
                        "TrapBook[%s]: TWEAK LONG trigger %.2f SL %.2f",
                        self._underlying, entry, sl_p,
                    )
                ts.done = True
                return

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

    def _check_exit(self, bar: _Bar) -> None:
        if self._position is None:
            return
        pos = self._position
        hard_hit = (
            (pos["direction"] == "LONG" and bar.close <= pos["sl"])
            or (pos["direction"] == "SHORT" and bar.close >= pos["sl"])
        )
        tsl_hit = (
            (pos["direction"] == "LONG" and bar.close < pos["tsl_level"])
            or (pos["direction"] == "SHORT" and bar.close > pos["tsl_level"])
        )
        if hard_hit:
            asyncio.create_task(self._square_off("sl_hit"))
        elif tsl_hit:
            asyncio.create_task(self._square_off("tsl_hit"))

    # ── position open / close ─────────────────────────────────────────────────

    async def _open_position(self, trigger: Dict, ts: datetime) -> None:
        direction = trigger["direction"]
        spot = self._last_spot
        if spot is None:
            logger.warning("TrapBook[%s]: trigger fired but no spot price.", self._underlying)
            return

        # 1-ITM option selection
        atm = round(spot / self._strike_step) * self._strike_step
        if direction == "LONG":
            strike = int(atm - self._itm_offset * self._strike_step)
            opt_type = "CE"
        else:
            strike = int(atm + self._itm_offset * self._strike_step)
            opt_type = "PE"

        today = ts.date()
        expiry = _get_expiry(self._underlying, today, self._positional)
        if not expiry:
            logger.warning(
                "TrapBook[%s]: no active expiry for %s — cannot enter.", self._underlying, today,
            )
            return

        qty = self._lot_size * self._lot_multiplier
        self._position = {
            "direction": direction,
            "entry": trigger["trigger"],
            "sl": trigger["sl"],
            "tsl_level": trigger["sl"],
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
            strategy=self._strategy_name,
            direction=direction,
            action="BUY",
            quantity=qty,
            entry_price=spot,
            sl_price=trigger["sl"],
            tsl_level=trigger["sl"],
            trigger_ts=ts,
            reason=trigger.get("source", "C2"),
            underlying=self._underlying,
            option_type=opt_type,
            strike=strike,
            expiry=expiry,
            product_type=self._product_type,
        )
        if self._bus is not None:
            await self._bus.publish(Topic.D1_TRAP_ORDER_REQUEST, ev)
        logger.info(
            "TrapBook[%s]: BUY 1-ITM %s strike=%d exp=%s qty=%d spot=%.2f SL=%.2f src=%s",
            self._underlying, opt_type, strike, expiry, qty,
            spot, trigger["sl"], trigger.get("source"),
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
            strategy=self._strategy_name,
            direction=pos["direction"],
            action="SELL",
            quantity=pos["qty"],
            entry_price=pos["entry"],
            sl_price=pos["sl"],
            tsl_level=pos["tsl_level"],
            trigger_ts=datetime.now(IST),
            reason=reason,
            underlying=self._underlying,
            option_type=pos["option_type"],
            strike=pos["strike"],
            expiry=pos["expiry"],
            product_type=self._product_type,
        )
        if self._bus is not None:
            await self._bus.publish(Topic.D1_TRAP_ORDER_REQUEST, ev)
        logger.info(
            "TrapBook[%s]: SELL %s strike=%d reason=%s spot=%.2f",
            self._underlying, pos["option_type"], pos["strike"], reason, spot,
        )

    async def liquidate(self, reason: str = "kill_switch") -> None:
        await self._square_off(reason)

    async def _option_loop(self) -> None:
        pass

    # ── status ────────────────────────────────────────────────────────────────

    def status(self) -> dict:
        pos = self._position
        return {
            "strategy": self._strategy_name,
            "underlying": self._underlying,
            "htf_mins": self._htf_mins,
            "mtf_mins": self._mtf_mins,
            "positional": self._positional,
            "htf_zones": len(self._monitors),
            "position": {
                "direction": pos["direction"],
                "entry": pos["entry"],
                "sl": pos["sl"],
                "tsl": pos["tsl_level"],
                "option_type": pos["option_type"],
                "strike": pos["strike"],
                "expiry": str(pos["expiry"]),
                "qty": pos["qty"],
                "source": pos.get("source"),
            } if pos else None,
            "pending_5m": bool(self._pending_5m),
        }

    def monitoring_zones(self) -> dict:
        """Return live monitoring state for the dashboard zone tracker."""
        spot = self._last_spot
        zones = []
        for m in self._monitors:
            if m.done or m.invalid:
                continue
            dist = None
            if spot:
                mid = (m.zone_lo + m.zone_hi) / 2
                dist = round((spot - mid) / mid * 100, 2)  # % from zone midpoint
            zones.append({
                "direction": m.direction,
                "zone_lo": round(m.zone_lo, 2),
                "zone_hi": round(m.zone_hi, 2),
                "state": m.state,          # "WAITING" | "MONITORING"
                "dist_pct": dist,          # +ve = spot above zone, -ve = below
                "ref_ts": m.htf_reclaim_ts.strftime("%Y-%m-%d") if m.htf_reclaim_ts else None,
            })
        # Sort: MONITORING first, then by abs distance ascending
        zones.sort(key=lambda z: (0 if z["state"] == "MONITORING" else 1,
                                  abs(z["dist_pct"]) if z["dist_pct"] is not None else 999))
        pending = self._pending_5m
        return {
            "underlying": self._underlying,
            "client_id": self._client_id,
            "binding_id": self._binding_id,
            "spot": round(spot, 2) if spot else None,
            "zones": zones[:10],   # top 10 closest/active
            "total_zones": len([m for m in self._monitors if not m.done and not m.invalid]),
            "pending": {
                "direction": pending["direction"],
                "trigger": pending["trigger"],
                "sl": pending["sl"],
            } if pending else None,
            "position": bool(self._position),
        }


# ── helpers ───────────────────────────────────────────────────────────────────

def _mtf_bucket(ts: datetime, mins: int) -> datetime:
    """Open timestamp of the N-minute bar containing ts (anchored at 09:15)."""
    open_dt = ts.replace(hour=9, minute=15, second=0, microsecond=0)
    elapsed = max(0, int((ts - open_dt).total_seconds() // 60))
    idx = elapsed // mins
    return open_dt + timedelta(minutes=idx * mins)


def _build_bar(bar_open: datetime, bars_5m: List[_Bar]) -> _Bar:
    return _Bar(
        timestamp=bar_open,
        open=bars_5m[0].open,
        high=max(b.high for b in bars_5m),
        low=min(b.low for b in bars_5m),
        close=bars_5m[-1].close,
    )


def _upstox_key_for(underlying: str) -> str:
    """Upstox instrument key for an underlying (index or FnO stock)."""
    from config.global_config import FNO_STOCK_CONFIG
    cfg = FNO_STOCK_CONFIG.get(underlying.upper())
    if cfg:
        return cfg["upstox_key"]
    # Indices
    _INDEX_KEYS = {
        "NIFTY":      "NSE_INDEX|Nifty 50",
        "BANKNIFTY":  "NSE_INDEX|Nifty Bank",
        "FINNIFTY":   "NSE_INDEX|Nifty Fin Service",
        "SENSEX":     "BSE_INDEX|SENSEX",
        "MIDCPNIFTY": "NSE_INDEX|NIFTY MID SELECT",
    }
    return _INDEX_KEYS.get(underlying.upper(), f"NSE_INDEX|{underlying}")


def _resample(bars_1m: List[_Bar], mins: int) -> List[_Bar]:
    """Resample 1-min bars to N-minute bars, clock-anchored at 09:15 IST."""
    buckets: Dict = {}
    order: List = []
    for b in bars_1m:
        open_dt = b.timestamp.replace(hour=9, minute=15, second=0, microsecond=0)
        elapsed = max(0, int((b.timestamp - open_dt).total_seconds() // 60))
        bucket_idx = elapsed // mins
        key = (b.timestamp.date(), bucket_idx)
        if key not in buckets:
            buckets[key] = []
            order.append(key)
        buckets[key].append(b)
    out: List[_Bar] = []
    for k in order:
        chunk = buckets[k]
        bar_open = chunk[0].timestamp.replace(hour=9, minute=15, second=0, microsecond=0) \
                   + timedelta(minutes=k[1] * mins)
        out.append(_Bar(
            timestamp=bar_open,
            open=chunk[0].open,
            high=max(b.high for b in chunk),
            low=min(b.low for b in chunk),
            close=chunk[-1].close,
        ))
    return out


def _get_expiry(underlying: str, today: date, positional: bool) -> Optional[date]:
    """
    Get option expiry:
    - Index (intraday): current weekly/nearest expiry from registry.
    - FnO stock (positional): current monthly; if <= 5 calendar days to expiry → next month.
    """
    expiry = REGISTRY.get_active_expiry_strict(underlying, today)
    if expiry is None:
        return None
    if positional:
        # FnO stocks are monthly; roll over if <= 5 days
        if (expiry - today).days <= 5:
            # Try to get next month's expiry
            next_try = expiry + timedelta(days=10)
            next_exp = REGISTRY.get_active_expiry_strict(underlying, next_try)
            if next_exp and next_exp > expiry:
                expiry = next_exp
    return expiry


def _fetch_bars(
    instrument_key: str, interval: str, start: date, end: date, token: str
) -> List[_Bar]:
    """Synchronous bar fetch with disk cache. Call via asyncio.to_thread()."""
    from urllib.parse import quote as _q
    try:
        from curl_cffi import requests as _cc
        def _get(url, hdrs): return _cc.get(url, headers=hdrs, impersonate="chrome131", timeout=30).json()
    except ImportError:
        import urllib.request as _ureq, json as _json
        def _get(url, hdrs):
            req = _ureq.Request(url, headers=hdrs)
            with _ureq.urlopen(req, timeout=30) as r:
                return _json.loads(r.read())

    os.makedirs(_CACHE_DIR, exist_ok=True)
    safe = instrument_key.replace("|", "_").replace(" ", "_")
    cache_file = os.path.join(_CACHE_DIR, f"{safe}_{interval}_{start}_{end}.json")

    if os.path.exists(cache_file):
        with open(cache_file) as f:
            raw = json.load(f)
    else:
        url = (
            f"https://api.upstox.com/v2/historical-candle/"
            f"{_q(instrument_key, safe='')}/{interval}/{end.isoformat()}/{start.isoformat()}"
        )
        hdrs = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
        try:
            raw = _get(url, hdrs)
        except Exception as exc:
            logger.warning("TrapBook: fetch %s failed: %s", instrument_key, exc)
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
            bars.append(_Bar(timestamp=ts, open=float(c[1]), high=float(c[2]),
                             low=float(c[3]), close=float(c[4])))
        except Exception:
            pass
    return bars


def _fetch_intraday_5m(instrument_key: str, token: str) -> List[_Bar]:
    """Fetch today's intraday 5M bars from Upstox (no cache — always live data)."""
    from urllib.parse import quote as _q
    try:
        from curl_cffi import requests as _cc
        def _get(url, hdrs): return _cc.get(url, headers=hdrs, impersonate="chrome131", timeout=30).json()
    except ImportError:
        import urllib.request as _ureq, json as _json
        def _get(url, hdrs):
            req = _ureq.Request(url, headers=hdrs)
            with _ureq.urlopen(req, timeout=30) as r:
                return _json.loads(r.read())

    url = (
        f"https://api.upstox.com/v2/historical-candle/intraday/"
        f"{_q(instrument_key, safe='')}/1minute"
    )
    hdrs = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    try:
        raw = _get(url, hdrs)
    except Exception as exc:
        logger.warning("TrapBook intraday 5M fetch failed for %s: %s", instrument_key, exc)
        return []

    candles = (raw.get("data") or {}).get("candles") or []
    if not candles:
        logger.warning(
            "TrapBook intraday 5M: 0 candles for %s — API status=%s errors=%s",
            instrument_key, raw.get("status"), raw.get("errors"),
        )
    bars: List[_Bar] = []
    for c in reversed(candles):
        try:
            ts = datetime.fromisoformat(c[0]).astimezone(IST)
            bars.append(_Bar(timestamp=ts, open=float(c[1]), high=float(c[2]),
                             low=float(c[3]), close=float(c[4])))
        except Exception:
            pass
    return bars


def _fetch_1m_bars(
    instrument_key: str, start: date, end: date, token: str
) -> List[_Bar]:
    """Fetch 1-min bars in 30-day chunks and merge."""
    all_bars: List[_Bar] = []
    seen: set = set()
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(chunk_start + timedelta(days=29), end)
        chunk = _fetch_bars(instrument_key, "1minute", chunk_start, chunk_end, token)
        for b in chunk:
            if b.timestamp not in seen:
                all_bars.append(b)
                seen.add(b.timestamp)
        chunk_start = chunk_end + timedelta(days=1)
    all_bars.sort(key=lambda b: b.timestamp)
    return all_bars
