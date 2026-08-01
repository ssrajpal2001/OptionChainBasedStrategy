"""
strategies/fvg/engine.py — FVGStrategy: per-(client, binding, underlying) live book
for the "High Liquidity Fair Value Gap" Smart Money Concepts strategy.

Detection runs on the underlying SPOT/INDEX chart (matches D1TrapOptionBook's
design, not D1TrapBearOnlyBook's option-native one). HTF/LTF are configurable
per deployment (default HTF=10min/LTF=3min, the validated baseline from
scripts/fvg_tf_sweep.py -- PF 1.79, win% 56.2%, balanced CE/PE) -- HTF for
swing structure/MSS/liquidity-sweep, LTF for FVG detection + retest entry.
No indicators (no RSI/VWAP/ADX) — pure price action, per direct user spec.
Intraday only: MIS, EOD square-off, no overnight carry.

2026-08-03: EXIT mechanics are option-native (entry/signal detection stays on
spot) -- a real-premium backtest of the earlier spot-based SL/TP showed it
desynchronizes from actual option P&L (theta decay, delta/IV shifts let a
spot "stop" fire with premium unmoved, or premium bleed while spot sat inside
its band; PF 0.75, net -Rs5,723 real vs -Rs907 naive-estimate over the same
28 trades). SL/exit now trigger off the position's own live premium
(Topic.OPTION_TICK): SL = whichever is tighter of entry_premium*(1-initial_sl_pct)
and the hard Rs/lot risk cap. No fixed take-profit -- once profit reaches
trail_trigger_pct, a step-locked TSL locks in first_lock_pct, then every
further step_pct of profit locks another step_lock_pct, letting a strong
move run instead of capping it at a fixed R:R (all five params are fully
configurable per deployment via strategy_params -- see
scripts/fvg_tsl_sweep.py for the optimization pass this baseline came from). A
time-based stagnation exit closes any position whose TSL never activated
within self._stagnation_bars LTF candles (~40 real minutes, scaled to
whatever ltf_mins is configured) to cap theta bleed on a rangebound spot --
once the TSL DOES activate, stagnation no longer applies and the trade runs
under trailing-stop management. `direction_mode` ("BOTH" default | "CE_ONLY"
| "PE_ONLY") can restrict entries to one side.

Mirrors D1TrapOptionBook's lifecycle shape closely (same
_tick_loop/_candle_loop/_eod_loop/_startup_load/_open_position/_square_off
pattern) and reuses its module-level helpers (_Bar, _upstox_key_for,
_resample, _fetch_1m_bars, _fetch_intraday_5m, _mtf_bucket, _build_bar)
rather than reimplementing them — same cross-strategy reuse already
established by bear_only_book.py importing from book.py.

2026-08-03 (theta-decay baseline): entries trade the NEXT-WEEK expiry, not
the current week's -- a real-premium backtest comparison (same 13 signals,
only the contract changed) showed next-week meaningfully outperforms
current-week under the same exit rules: PF 1.50 vs 1.43, Net +Rs2,762 vs
+Rs1,979, smaller Max DD -Rs3,010 vs -Rs3,520 (scripts/fvg_next_week_expiry_test.py).
Slower theta decay per unit of holding time is the mechanism -- resolved via
_next_week_expiry() below (double REGISTRY.get_active_expiry() call, never
a hardcoded calendar date), not book.py's _get_expiry() (current-week only).
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional, Tuple

from config.global_config import IST, Topic
from data_layer.base_feeder import CandleEvent, IndexTick, OptionTick
from data_layer.instrument_registry import REGISTRY
from strategies.core.base_book import AbstractStrategyBook
from strategies.d1_trap_option.book import (
    _Bar,
    _build_bar,
    _fetch_1m_bars,
    _fetch_intraday_5m,
    _mtf_bucket,
    _resample,
    _upstox_key_for,
)
from strategies.fvg.detector import (
    SwingPoint,
    detect_fvg,
    detect_mss,
    find_swing_points,
    tag_high_liquidity,
    update_fvg_state,
)

logger = logging.getLogger(__name__)

_SESSION_OPEN = time(9, 15)
_ENTRY_CUTOFF = time(14, 30)      # no new entries after this, mirrors bear_only_book
_EOD_TIME = time(15, 15)          # force square-off, MIS/intraday only

# 2026-08-03 validated baseline (scripts/fvg_tf_sweep.py, NIFTY 2026-07-23..07-31,
# real option premium): HTF=10m/LTF=3m was the best BALANCED result (PF 1.79,
# win% 56.2%, CE net +Rs5,964 / PE net -Rs1,079 -- 8 CE / 8 PE, not skewed to one
# side like the faster 10m/1m combo was). Now the DEFAULT, still overridable per
# deployment via strategy_params.
_DEFAULT_HTF_MINS = 10
_DEFAULT_LTF_MINS = 3
_HIST_WARMUP_DAYS = 14            # matches D1Trap's _HIST_WARMUP_DAYS convention
_MAX_FVG_AGE_DAYS = 14

_ATM_ROUND_STEP = 100
_DEFAULT_ITM_OFFSET_PTS = 50      # 1-strike ITM (NIFTY step = 50pts) -- validated baseline

_MAX_RISK_RS_PER_LOT = 2000.0     # same hard cap as bear_only_book.py
_OPTION_DELTA_APPROX = 0.5        # ATM/near-ATM approx delta: spot SL distance ->
                                   # option premium SL distance, used ONLY as a
                                   # pre-trade risk-cap ESTIMATE before entry (no
                                   # live premium exists yet at that instant) --
                                   # NOT used for the actual SL/TP trigger, which
                                   # is option-native (see below).

_DIRECTION_MODES = ("BOTH", "CE_ONLY", "PE_ONLY")


def _next_week_expiry(underlying: str, from_date: date):
    """Resolve the NEXT-WEEK expiry via two REGISTRY calls -- never a
    hardcoded calendar date. current_week = nearest active expiry on/after
    from_date; next_week = nearest active expiry strictly after that. Returns
    None if either lookup fails (registry not loaded / no contracts)."""
    current_week = REGISTRY.get_active_expiry(underlying, from_date)
    if current_week is None:
        return None
    return REGISTRY.get_active_expiry(underlying, current_week + timedelta(days=1))

# 2026-08-03 option-native exit rewrite: the earlier spot-based SL/TP
# desynchronized from real option P&L (theta decay, delta/IV shifts meant a
# spot "stop" could fire while premium hadn't moved, or premium could bleed
# while spot sat inside its band -- confirmed on a real-premium backtest,
# PF 0.75 net -Rs5,723 vs the naive spot-based read of -Rs907). SL/exit now
# trigger off the option's OWN live premium (Topic.OPTION_TICK), not spot.
#
# Step-locked trailing stop (direct user spec, all five FULLY CONFIGURABLE
# per deployment via strategy_params JSON -- these module constants are only
# the fallback DEFAULTS): SL starts at initial_sl_pct below entry premium
# (floored by the hard Rs/lot risk cap, whichever is tighter). Once profit
# reaches trail_trigger_pct, the stop is raised and locked to first_lock_pct
# profit. Every further step_pct of additional gain beyond the trigger locks
# another step_lock_pct (e.g. +25% gain -> lock +15%; +40% gain -> lock
# +22.5%; +55% gain -> lock +30%; ...).
#
# 2026-08-03 optimized (scripts/fvg_tsl_sweep.py, 4-combo x 2-timeframe sweep
# on real NIFTY option premium, 2026-07-23..07-31): "Wider Runner" (trigger
# 25%/lock 15%/step 15%/step_lock 7.5%) won that sweep on PF/Net/drawdown,
# but only fired on 1 of 16 trades -- 14 exited via the 40min stagnation
# timer before ever reaching a 25% premium swing. Re-tuned DOWN to trigger
# on realistic 10m/3m swings (validated together with the intraday-only FVG
# fix in the same pass -- see reset_session()/_rebuild_fvg_pool() below):
# trigger 15%/lock 8%/step 10%/step_lock 5% -- TSL usage went from 1/16 to
# 2/13 trades (stale multi-day FVG trades also removed by the intraday fix,
# taking n from 16 to 13), PF 1.43, Net +Rs1,979.
_DEFAULT_INITIAL_SL_PCT = 0.20
_DEFAULT_TRAIL_TRIGGER_PCT = 0.15
_DEFAULT_FIRST_LOCK_PCT = 0.08
_DEFAULT_STEP_PCT = 0.10
_DEFAULT_STEP_LOCK_PCT = 0.05

_STAGNATION_MINUTES = 40          # a position whose TSL has never activated within this
                                   # many REAL minutes is exited at market to avoid pure
                                   # theta bleed on a rangebound spot. Bar count is derived
                                   # from this fixed time window / whatever ltf_mins is
                                   # configured (matches scripts/fvg_tf_sweep.py's
                                   # `max(1, 40 // ltf_mins)` -- the validated 10m/3m
                                   # baseline used 13 bars, NOT a fixed 8, to hold this
                                   # same ~40min window at LTF=3m).


@dataclass
class FVGOrderEvent:
    """Order event published on Topic.FVG_ORDER_REQUEST. Same shape as
    D1TrapOrderEvent (strategies/d1_trap_option/book.py) for bridge parity."""
    client_id: str
    binding_id: str
    strategy: str = "fvg"
    direction: str = "LONG"     # "LONG" | "SHORT"
    action: str = "BUY"          # "BUY" | "SELL"
    quantity: int = 0
    entry_price: float = 0.0     # spot price at trigger
    sl_price: float = 0.0
    trigger_ts: Optional[datetime] = None
    reason: str = ""
    underlying: str = ""
    option_symbol: str = ""
    option_type: str = ""        # "CE" | "PE"
    strike: int = 0
    expiry: Optional[date] = None
    order_type: str = "MARKET"
    product_type: str = "MIS"


class FVGStrategy(AbstractStrategyBook):
    """One independent FVG trading book per (client, binding, underlying)."""

    def __init__(
        self,
        bus,
        cfg,
        underlying: str,
        client_id: str,
        binding_id: str,
        lot_multiplier: int = 1,
        feeder_token: str = "",
        itm_offset_pts: int = _DEFAULT_ITM_OFFSET_PTS,
        min_rr: float = 2.0,   # vestigial -- see self._min_rr comment below
        product_type: str = "MIS",
        htf_mins: int = _DEFAULT_HTF_MINS,
        ltf_mins: int = _DEFAULT_LTF_MINS,
        direction_mode: str = "BOTH",
        initial_sl_pct: float = _DEFAULT_INITIAL_SL_PCT,
        trail_trigger_pct: float = _DEFAULT_TRAIL_TRIGGER_PCT,
        first_lock_pct: float = _DEFAULT_FIRST_LOCK_PCT,
        step_pct: float = _DEFAULT_STEP_PCT,
        step_lock_pct: float = _DEFAULT_STEP_LOCK_PCT,
    ) -> None:
        super().__init__(bus, cfg, underlying, client_id, binding_id)
        self._lot_multiplier = max(1, lot_multiplier)
        self._feeder_token = feeder_token
        self._itm_offset_pts = itm_offset_pts
        self._min_rr = min_rr   # kept for API/strategy_params compatibility; no longer
                                 # drives the exit (fixed R:R target replaced by the
                                 # step-locked TSL below -- see _check_exit_premium)
        self._product_type = product_type
        self._htf_mins = int(htf_mins) if htf_mins else _DEFAULT_HTF_MINS
        self._ltf_mins = int(ltf_mins) if ltf_mins else _DEFAULT_LTF_MINS
        self._direction_mode = direction_mode if direction_mode in _DIRECTION_MODES else "BOTH"
        self._stagnation_bars = max(1, _STAGNATION_MINUTES // self._ltf_mins)
        self._initial_sl_pct = float(initial_sl_pct)
        self._trail_trigger_pct = float(trail_trigger_pct)
        self._first_lock_pct = float(first_lock_pct)
        self._step_pct = float(step_pct)
        self._step_lock_pct = float(step_lock_pct)
        self._strike_step = int(cfg.exchange.strike_steps.get(underlying, 50) if cfg else 50)
        self._lot_size = int(cfg.exchange.lot_sizes.get(underlying, 75) if cfg else 75)
        self._spot_symbol = f"NSE_INDEX|{underlying}"

        self._htf_bars: List[_Bar] = []
        self._ltf_bars: List[_Bar] = []
        self._htf_swings: List[SwingPoint] = []
        self._pdh: Optional[float] = None
        self._pdl: Optional[float] = None
        self._fvgs: List[dict] = []
        self._known_fvg_ts: set = set()

        self._current_htf_open: Optional[datetime] = None
        self._current_htf_5m: List[_Bar] = []
        self._current_ltf_open: Optional[datetime] = None
        self._current_ltf_1m: List[_Bar] = []

        self._position: Optional[Dict] = None
        self._last_spot: Optional[float] = None
        self._day_done = False
        self._htf_loaded = False
        self._warming_up = False

        # Live option premium book: {(strike, option_type): last_ltp}. Updated
        # from every OPTION_TICK for this underlying regardless of whether a
        # position is open yet -- needed so a freshly-computed entry strike
        # already has a usable premium at the moment of entry (ticks flow via
        # the existing StrikeRebalancer ATM+/-N auto-subscribe range, same
        # precondition already documented for bear_only_book.py).
        self._option_ltp: Dict[Tuple[int, str], float] = {}
        self._ltf_bars_since_entry = 0

        logger.info(
            "FVGStrategy[%s/%s/%s]: htf=%dm ltf=%dm itm_offset=%d direction_mode=%s lot=%d step=%d "
            "| TSL: initial_sl=%.1f%% trigger=%.1f%% first_lock=%.1f%% step=%.1f%% step_lock=%.1f%%",
            client_id, binding_id, underlying, self._htf_mins, self._ltf_mins, itm_offset_pts,
            self._direction_mode, self._lot_size, self._strike_step,
            self._initial_sl_pct * 100, self._trail_trigger_pct * 100, self._first_lock_pct * 100,
            self._step_pct * 100, self._step_lock_pct * 100,
        )

    # ── lifecycle ────────────────────────────────────────────────────────────

    def start(self) -> None:
        super().start()
        self._subscribe(Topic.CANDLE_CLOSE)
        self._subscribe(Topic.INDEX_TICK)
        self._subscribe(Topic.OPTION_TICK)
        self._tasks.append(asyncio.create_task(
            self._candle_loop(), name=f"fvg_candle_{self._underlying}"))
        self._tasks.append(asyncio.create_task(
            self._tick_loop(), name=f"fvg_tick_{self._underlying}"))
        self._tasks.append(asyncio.create_task(
            self._option_tick_loop(), name=f"fvg_option_tick_{self._underlying}"))
        self._tasks.append(asyncio.create_task(
            self._eod_loop(), name=f"fvg_eod_{self._underlying}"))
        self._tasks.append(asyncio.create_task(
            self._startup_load(), name=f"fvg_startup_{self._underlying}"))

    def reset_session(self) -> None:
        """New trading day: roll PDH/PDL from yesterday's HTF bars, clear
        today's LTF accumulation state, AND clear the FVG pool.

        2026-08-03 correction: this strategy is intraday-only -- a gap that
        formed (and never got retested) on some earlier day must NOT still
        be tradeable today. Confirmed live: a backtest trade fired off an
        FVG whose candle1/candle3 were 24 days old, using today's ATM
        strike against a price structure from three weeks earlier. HTF
        structure (self._htf_bars/_htf_swings, PDH/PDL) legitimately spans
        multiple days -- that context (yesterday's high, recent swing
        points) is supposed to carry over. The FVG pool does NOT: unlike
        D1TrapOptionBook's zones (deliberately multi-day, D1Trap watches
        zones for up to 14 days), an FVG's gap AND its retest must both
        happen within the same session to count as a valid intraday setup."""
        self._roll_pdh_pdl()
        self._current_htf_open = None
        self._current_htf_5m.clear()
        self._current_ltf_open = None
        self._current_ltf_1m.clear()
        self._fvgs.clear()
        self._known_fvg_ts.clear()
        self._last_spot = None
        self._day_done = False

    def _roll_pdh_pdl(self) -> None:
        if not self._htf_bars:
            return
        last_day = self._htf_bars[-1].timestamp.date()
        prior_day_bars = [b for b in self._htf_bars if b.timestamp.date() == last_day]
        if prior_day_bars:
            self._pdh = max(b.high for b in prior_day_bars)
            self._pdl = min(b.low for b in prior_day_bars)

    # ── startup warmup ───────────────────────────────────────────────────────

    async def _startup_load(self) -> None:
        if not self._feeder_token:
            logger.warning("FVGStrategy[%s]: no feeder token — cannot warm history; idle.",
                            self._underlying)
            self._htf_loaded = True
            return
        try:
            today = datetime.now(IST).date()
            key = _upstox_key_for(self._underlying)
            start = today - timedelta(days=_HIST_WARMUP_DAYS)
            bars_1m = await asyncio.to_thread(_fetch_1m_bars, key, start, today, self._feeder_token)
            self._htf_bars = _resample(bars_1m, self._htf_mins)
            self._ltf_bars = _resample(bars_1m, self._ltf_mins)
            self._htf_swings = find_swing_points(self._htf_bars)
            self._roll_pdh_pdl()
            self._rebuild_fvg_pool()
            logger.info(
                "FVGStrategy[%s]: warmed %d HTF(%dm) / %d LTF(%dm) bars -> %d FVGs (%d high-liquidity).",
                self._underlying, len(self._htf_bars), self._htf_mins, len(self._ltf_bars), self._ltf_mins,
                len(self._fvgs),
                sum(1 for f in self._fvgs if f["high_liquidity"]),
            )
            await self._warmup_intraday(today)
            self._htf_loaded = True
        except Exception:
            logger.exception("FVGStrategy[%s]: startup load failed.", self._underlying)
            self._htf_loaded = True

    def _rebuild_fvg_pool(self) -> None:
        """(Re)detect FVGs on TODAY's LTF bars ONLY and tag high-liquidity
        ones against the current (multi-day) HTF structure. Only newly seen
        candle3 timestamps are added — existing FVGs (with their live
        mitigation/invalidation state) are left untouched.

        Intraday-only correction (2026-08-03): `detect_fvg` must NOT scan
        self._ltf_bars' full multi-day history -- a gap that formed on an
        earlier day and never got retested has no business still being
        tradeable today (confirmed live: a 24-day-old FVG fired a real
        trade before this fix). HTF structure (self._htf_bars/_htf_swings,
        PDH/PDL passed into tag_high_liquidity) legitimately stays
        multi-day -- only the FVG gap itself and its retest are
        same-session-only."""
        today = datetime.now(IST).date()
        todays_ltf_bars = [b for b in self._ltf_bars if b.timestamp.date() == today]
        found = detect_fvg(todays_ltf_bars)
        for fvg in found:
            if fvg["candle3_ts"] in self._known_fvg_ts:
                continue
            tag_high_liquidity(fvg, self._htf_bars, self._htf_swings, pdh=self._pdh, pdl=self._pdl)
            self._fvgs.append(fvg)
            self._known_fvg_ts.add(fvg["candle3_ts"])

    async def _warmup_intraday(self, today: date) -> None:
        """Replay today's own bars so far (if the market has already opened)
        so FVG mitigation/invalidation state and swing structure catch up to
        'now' before live ticks arrive. _warming_up suppresses real order
        placement, mirroring D1TrapOptionBook._warmup_intraday.

        _fetch_intraday_5m's name is misleading -- it actually returns raw
        1-MINUTE bars (confirmed: its Upstox URL requests /1minute; D1Trap's
        own _warmup_intraday bucket-accumulates its result the same way).
        Bucket them into self._ltf_mins-sized bars ourselves before feeding
        _process_ltf_bar, which expects already-closed LTF bars -- feeding it
        raw 1-minute ticks directly would corrupt the LTF series whenever
        ltf_mins != 1."""
        now = datetime.now(IST)
        if now.time() < _SESSION_OPEN:
            return
        key = _upstox_key_for(self._underlying)
        try:
            bars_1m = await asyncio.to_thread(_fetch_intraday_5m, key, self._feeder_token)
        except Exception as exc:
            logger.warning("FVGStrategy[%s]: intraday warmup fetch failed: %s", self._underlying, exc)
            return
        if not bars_1m:
            return
        self._warming_up = True
        try:
            replayed = 0
            bucket_open = None
            bucket_1m: List[_Bar] = []
            for bar in bars_1m:
                if bar.timestamp.time() < _SESSION_OPEN:
                    continue
                b_open = _mtf_bucket(bar.timestamp, self._ltf_mins)
                if bucket_open is None:
                    bucket_open = b_open
                elif b_open != bucket_open:
                    if bucket_1m:
                        self._process_ltf_bar(_build_bar(bucket_open, bucket_1m))
                        replayed += 1
                    bucket_open = b_open
                    bucket_1m = []
                bucket_1m.append(bar)
            logger.info("FVGStrategy[%s]: intraday warmup replayed %d %dm bars from %d 1m ticks -> %d FVGs live.",
                        self._underlying, replayed, self._ltf_mins, len(bars_1m), len(self._fvgs))
        finally:
            self._warming_up = False

    # ── live loops ───────────────────────────────────────────────────────────

    async def _tick_loop(self) -> None:
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
                )
                if is_spot and ev.ltp and ev.ltp > 0:
                    self._last_spot = float(ev.ltp)
            except Exception:
                pass

    async def _option_tick_loop(self) -> None:
        """Tracks live premium for every option tick on this underlying (not
        just the open position's strike -- so a freshly-computed entry strike
        already has a usable premium the instant it's chosen), and drives the
        option-native SL/TP check on every tick matching the OPEN position."""
        q = self._loop_queues.get(Topic.OPTION_TICK)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                if not isinstance(ev, OptionTick) or not ev.ltp or ev.ltp <= 0:
                    continue
                if str(ev.underlying).upper() != self._underlying.upper():
                    continue
                key = (int(ev.strike), ev.option_type)
                self._option_ltp[key] = float(ev.ltp)
                pos = self._position
                if pos is not None and key == (pos["strike"], pos["option_type"]):
                    ts = getattr(ev, "timestamp", None) or datetime.now(IST)
                    self._check_exit_premium(float(ev.ltp), ts)
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
                logger.exception("FVGStrategy[%s]: candle handler error.", self._underlying)

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

    # ── candle processing ────────────────────────────────────────────────────

    def _on_candle(self, ev: CandleEvent) -> None:
        """Consumes the always-available 1-MINUTE candle stream and
        self-buckets it into self._ltf_mins-sized bars (rather than relying
        on CandleCache already publishing that exact timeframe -- the
        default GlobalConfig.candle_timeframes is [1,2,5,15,75], which does
        NOT include arbitrary configured values like 10/3; 1-minute is
        always present, so bucketing locally works for any htf/ltf combo)."""
        if not isinstance(ev, CandleEvent) or not self._htf_loaded:
            return
        if ev.timeframe != 1:
            return
        is_spot = (
            ev.symbol == self._spot_symbol
            or ev.symbol == self._underlying
            or (self._underlying == "NIFTY" and ev.symbol in ("NSE_INDEX|Nifty 50", "NIFTY"))
        )
        if not is_spot:
            return
        if ev.timestamp.time() < _SESSION_OPEN or self._day_done:
            return

        bar = _Bar(timestamp=ev.timestamp, open=ev.open, high=ev.high, low=ev.low, close=ev.close)
        self._last_spot = ev.close

        ltf_open = _mtf_bucket(bar.timestamp, self._ltf_mins)
        if self._current_ltf_open is None:
            self._current_ltf_open = ltf_open
        elif ltf_open != self._current_ltf_open:
            if self._current_ltf_1m:
                self._process_ltf_bar(_build_bar(self._current_ltf_open, self._current_ltf_1m))
            self._current_ltf_open = ltf_open
            self._current_ltf_1m = []
        self._current_ltf_1m.append(bar)

    def _process_ltf_bar(self, bar: _Bar) -> None:
        """One LTF bar close: accumulate into the HTF bucket, refresh the
        FVG pool, advance every active FVG's state machine, and check for a
        retest entry."""
        self._ltf_bars.append(bar)

        htf_open = _mtf_bucket(bar.timestamp, self._htf_mins)
        if self._current_htf_open is None:
            self._current_htf_open = htf_open
        elif htf_open != self._current_htf_open:
            if self._current_htf_5m:
                closed_htf = _build_bar(self._current_htf_open, self._current_htf_5m)
                self._htf_bars.append(closed_htf)
                self._htf_swings = find_swing_points(self._htf_bars)
            self._current_htf_open = htf_open
            self._current_htf_5m = []
        self._current_htf_5m.append(bar)

        self._rebuild_fvg_pool()
        for fvg in self._fvgs:
            update_fvg_state(fvg, bar)
        self._check_pending_cancellation(bar)
        self._check_retest_entry(bar)
        self._check_stagnation_exit(bar.timestamp)

    def _check_pending_cancellation(self, bar: _Bar) -> None:
        """An UNMITIGATED/PARTIALLY_FILLED FVG whose own structure gets
        broken the wrong way before ever being retested is stale — mark it
        invalidated so it stops being offered as an entry (same intent as
        D1Trap's per-15m invalidation check, applied here per-5m since LTF=5m)."""
        for fvg in self._fvgs:
            if fvg["state"] not in ("UNMITIGATED", "PARTIALLY_FILLED"):
                continue
            if bar.timestamp <= fvg["candle3_ts"]:
                continue
            age = bar.timestamp - fvg["candle3_ts"]
            if age > timedelta(days=_MAX_FVG_AGE_DAYS):
                fvg["state"] = "INVALIDATED"
                fvg["invalidated_ts"] = bar.timestamp

    # ── entry / exit ─────────────────────────────────────────────────────────

    def _check_retest_entry(self, bar: _Bar) -> None:
        if self._warming_up or self._position is not None:
            return
        if bar.timestamp.time() >= _ENTRY_CUTOFF:
            return
        for fvg in self._fvgs:
            if not fvg["high_liquidity"] or fvg["state"] != "MITIGATED":
                continue
            direction = "LONG" if fvg["direction"] == "BULLISH" else "SHORT"
            # direction_mode filter: CE_ONLY skips bearish(SHORT/PE) setups,
            # PE_ONLY skips bullish(LONG/CE) setups, BOTH takes either.
            if self._direction_mode == "CE_ONLY" and direction != "LONG":
                continue
            if self._direction_mode == "PE_ONLY" and direction != "SHORT":
                continue
            sl_price = fvg["candle1_low"] if direction == "LONG" else fvg["candle1_high"]
            entry_price = self._last_spot or bar.close
            sl_distance = abs(entry_price - sl_price)
            if sl_distance <= 0:
                continue
            if not self._risk_within_cap(sl_distance):
                logger.info("FVGStrategy[%s]: retest entry skipped -- estimated risk exceeds cap "
                            "(sl_distance=%.2f).", self._underlying, sl_distance)
                fvg["state"] = "INVALIDATED"   # consumed either way -- don't re-check every bar
                continue
            asyncio.create_task(self._open_position(fvg, direction, entry_price, sl_price, bar.timestamp))
            fvg["state"] = "INVALIDATED"  # consumed -- entered, stop offering it again
            return

    def _risk_within_cap(self, spot_sl_distance: float) -> bool:
        """Pre-trade filter: approximate the option-premium risk from the
        spot SL distance via a fixed ATM delta (~0.5, no live premium feed
        consulted), and reject the entry if the estimated ₹/lot risk would
        exceed _MAX_RISK_RS_PER_LOT -- same intent as bear_only_book.py's
        hard SL cap, applied up front since this book's SL/TSL triggers are
        evaluated in spot terms, not live option LTP."""
        est_premium_distance = spot_sl_distance * _OPTION_DELTA_APPROX
        est_risk_rs = est_premium_distance * self._lot_size
        return est_risk_rs <= _MAX_RISK_RS_PER_LOT

    async def _open_position(self, fvg: dict, direction: str, entry_price: float,
                              sl_price: float, ts: datetime) -> None:
        if self._position is not None:
            return
        atm = round(entry_price / _ATM_ROUND_STEP) * _ATM_ROUND_STEP
        if direction == "LONG":
            strike = int(atm - self._itm_offset_pts)
            opt_type = "CE"
        else:
            strike = int(atm + self._itm_offset_pts)
            opt_type = "PE"

        expiry = _next_week_expiry(self._underlying, ts.date())
        if not expiry:
            logger.warning("FVGStrategy[%s]: no active next-week expiry -- cannot enter.", self._underlying)
            return

        # Option-native SL/TP: entry premium must already be tracked (ticks
        # flow via the ATM+/-N auto-subscribe range) -- don't trade blind if
        # it isn't, same "no data -> no trade" philosophy as bear_only_book.py.
        premium_entry = self._option_ltp.get((strike, opt_type))
        if premium_entry is None or premium_entry <= 0:
            logger.warning("FVGStrategy[%s]: no live premium for %s%d yet -- entry skipped.",
                            self._underlying, opt_type, strike)
            return

        # SL = whichever is TIGHTER (higher price / smaller loss) of the
        # configurable initial_sl_pct stop and the hard Rs/lot risk cap --
        # mirrors bear_only_book.py's max(structural_sl, entry - MAX_RISK/lot_size).
        pct_sl = premium_entry * (1 - self._initial_sl_pct)
        cap_sl = premium_entry - (_MAX_RISK_RS_PER_LOT / self._lot_size)
        premium_sl = max(pct_sl, cap_sl)
        qty = self._lot_size * self._lot_multiplier

        self._position = {
            "direction": direction, "entry": entry_price, "sl": sl_price,
            "premium_entry": premium_entry, "premium_sl": premium_sl,
            "high_lock_pct": 0.0,   # staircase TSL ratchet, see _check_exit_premium
            "option_type": opt_type, "strike": strike, "expiry": expiry, "qty": qty,
            "entry_ts": ts, "fvg_zone": (fvg["zone_lo"], fvg["zone_hi"]),
        }
        self._ltf_bars_since_entry = 0

        ev = FVGOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id,
            direction=direction, action="BUY", quantity=qty,
            entry_price=premium_entry, sl_price=premium_sl, trigger_ts=ts,
            reason="fvg_retest", underlying=self._underlying,
            option_type=opt_type, strike=strike, expiry=expiry,
            product_type=self._product_type,
        )
        if self._bus is not None:
            await self._bus.publish(Topic.FVG_ORDER_REQUEST, ev)
        logger.info(
            "FVGStrategy[%s]: BUY %s strike=%d exp=%s qty=%d spot=%.2f premium=%.2f "
            "SL=%.2f(prem) zone=[%.2f,%.2f]",
            self._underlying, opt_type, strike, expiry, qty, entry_price, premium_entry,
            premium_sl, fvg["zone_lo"], fvg["zone_hi"],
        )

    def _check_exit_premium(self, premium: float, ts: datetime) -> None:
        """Option-native SL/step-locked-TSL -- triggered off the position's
        OWN live premium (Topic.OPTION_TICK), not spot. Both CE and PE
        positions here are always BUY (long option), so exit direction is
        symmetric: premium falling to the (ratcheting) stop, regardless of
        the underlying spot direction that originally picked CE vs PE.

        Step-locked trail: once profit_pct >= self._trail_trigger_pct, lock
        self._first_lock_pct; every further self._step_pct of additional
        profit locks another self._step_lock_pct (repeating) -- same ratchet
        formula as bear_only_book.py's TSL, tiers fully configurable per
        deployment (see __init__). No fixed take-profit ceiling: a strong
        move keeps running until the trailing floor catches it."""
        pos = self._position
        if pos is None:
            return
        entry = pos["premium_entry"]
        profit_pct = (premium - entry) / entry
        if profit_pct >= self._trail_trigger_pct:
            steps = int((profit_pct - self._trail_trigger_pct) // self._step_pct)
            calc_lock = self._first_lock_pct + steps * self._step_lock_pct
            pos["high_lock_pct"] = max(pos["high_lock_pct"], calc_lock)
        stop_price = entry * (1 + pos["high_lock_pct"]) if pos["high_lock_pct"] > 0 else pos["premium_sl"]
        if premium <= stop_price:
            reason = "tsl_hit" if pos["high_lock_pct"] > 0 else "sl_hit"
            asyncio.create_task(self._square_off(reason))

    def _check_stagnation_exit(self, ts: datetime) -> None:
        """Time-based theta-decay guard: a position that hasn't yet
        activated its TSL (never reached self._trail_trigger_pct profit) within
        self._stagnation_bars LTF candles (~_STAGNATION_MINUTES real
        minutes, scaled to whatever ltf_mins is configured) is closed at
        market rather than left to bleed premium on a rangebound spot. A
        position whose TSL has already activated is left to run under
        trailing-stop management instead -- it has already proven itself
        profitable, so the stagnation guard's job (killing a flat/losing
        theta-bleed trade) no longer applies to it."""
        pos = self._position
        if pos is None:
            return
        self._ltf_bars_since_entry += 1
        if pos["high_lock_pct"] > 0:
            return
        if self._ltf_bars_since_entry >= self._stagnation_bars:
            asyncio.create_task(self._square_off("stagnation_exit"))

    async def _square_off(self, reason: str) -> None:
        pos = self._position
        if pos is None:
            return
        self._position = None
        spot = self._last_spot or pos["entry"]
        exit_premium = self._option_ltp.get((pos["strike"], pos["option_type"]), pos["premium_entry"])

        ev = FVGOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id,
            direction=pos["direction"], action="SELL", quantity=pos["qty"],
            entry_price=pos["premium_entry"], sl_price=pos["premium_sl"], trigger_ts=datetime.now(IST),
            reason=reason, underlying=self._underlying,
            option_type=pos["option_type"], strike=pos["strike"], expiry=pos["expiry"],
            product_type=self._product_type,
        )
        if self._bus is not None:
            await self._bus.publish(Topic.FVG_ORDER_REQUEST, ev)
        logger.info("FVGStrategy[%s]: SELL %s strike=%d reason=%s spot=%.2f exit_premium=%.2f "
                    "entry_premium=%.2f",
                    self._underlying, pos["option_type"], pos["strike"], reason, spot,
                    exit_premium, pos["premium_entry"])

    async def liquidate(self, reason: str = "kill_switch") -> None:
        await self._square_off(reason)

    # ── status / dashboard ───────────────────────────────────────────────────

    def status(self) -> dict:
        pos = self._position
        return {
            "underlying": self._underlying,
            "htf_bars": len(self._htf_bars),
            "ltf_bars": len(self._ltf_bars),
            "active_fvgs": sum(1 for f in self._fvgs if f["state"] not in ("MITIGATED", "INVALIDATED")),
            "high_liquidity_fvgs": sum(1 for f in self._fvgs if f["high_liquidity"]),
            "pdh": self._pdh, "pdl": self._pdl,
            "position": {
                "direction": pos["direction"], "spot_entry": pos["entry"], "spot_sl": pos["sl"],
                "premium_entry": pos["premium_entry"], "premium_sl": pos["premium_sl"],
                "tsl_locked_pct": pos["high_lock_pct"],
                "option_type": pos["option_type"], "strike": pos["strike"], "expiry": str(pos["expiry"]),
                "qty": pos["qty"],
            } if pos else None,
        }

    def monitoring_fvgs(self) -> dict:
        """Live FVG state for the dashboard, mirrors D1TrapOptionBook.monitoring_zones()."""
        spot = self._last_spot
        zones = []
        for fvg in self._fvgs:
            if fvg["state"] in ("MITIGATED", "INVALIDATED"):
                continue
            mid = (fvg["zone_lo"] + fvg["zone_hi"]) / 2
            dist_pct = round(abs(spot - mid) / mid * 100, 3) if spot else None
            zones.append(dict(
                direction=fvg["direction"], zone_lo=fvg["zone_lo"], zone_hi=fvg["zone_hi"],
                ce=fvg["ce"], state=fvg["state"], high_liquidity=fvg["high_liquidity"],
                dist_pct=dist_pct, candle3_ts=fvg["candle3_ts"].isoformat(),
            ))
        return dict(underlying=self._underlying, client_id=self._client_id,
                    binding_id=self._binding_id, spot=spot, zones=zones,
                    position=self.status()["position"])
