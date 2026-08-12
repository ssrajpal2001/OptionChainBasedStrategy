"""
strategies/d1_trap_option/sr_book.py — D1TrapSRBook (NEW, 2026-08-08).

Live/paper implementation of the "S&R ping-pong" mechanic validated for
BANKNIFTY via scripts/d1trap_banknifty_sr_sweep.py (full-month real-premium
backtest, tf=3m/exit_mode=raw: n=36, win%=44.4, PF=3.819, net=+Rs43,581 --
decisively beat D1TrapBearOnlyBook's own T1/T2 tranche mechanic on the SAME
zone pool: best T1/T2 result was PF=1.534/net=+Rs23,370). See CLAUDE.md's
"D1 Trap BearTrap" section and the project_banknifty_backtest_harness /
project_sr_pingpong_concept memory files for the full sweep writeup.

Distinct from D1TrapBearOnlyBook (bear_only_book.py) ONLY in entry/exit
mechanic -- reuses its HTF zone detection (_detect_bear_zones,
_collapse_nearby_zones, _prevalidate_zones, _resample, _to_bars, _Bar,
_OptionSeries) UNCHANGED, by import, never a second implementation. Where
BearOnly's ref-candle breach + 5m subzone + arm + swing-breach fires T1/T2
tranches, this book instead feeds each closed 1-min bar to
strategies.d1_trap_option.support_resistance.SRPingPongTracker -- the SAME
class scripts/d1trap_sr_zone_backtest.py's _run_sr_variant (backtest) drives,
so live and backtest can never silently diverge (see that class's own
docstring). Entry = a confirmed R2-breaches-R1 breakout on the S&R
tracker's TF-minute bucket inside a touched zone; exit = the tracker's own
EOD / hard-risk-cap / SL logic, already fully encapsulated there -- this
book does NOT reimplement any exit rule, it only acts on the events the
tracker returns.

2026-08-08 validated backtest DOES NOT use BearOnly's own WAITING /
MONITORING / ongoing-15m-close-invalidation zone states at all -- the
sweep fed SRPingPongTracker the RAW, unfiltered zone pool every day
(book._series[side].zones straight off _detect_bear_zones), and the
tracker's own touch/void logic (S1 dropping below zone_lo) is the only
invalidation-equivalent it ever applies. This book's own zone maintenance
(_update_zones) is therefore detection+merge ONLY, deliberately -- not a
simplification shortcut, it's matching exactly what was backtested. Each
zone's own `lock_ts` gates its eligibility per bar inside the tracker
(`zone["lock_ts"] > bar.timestamp: continue`), so growing the zone list
live (same list object, by reference) behaves identically to the backtest's
end-of-day-snapshot zone list -- no lookahead bias either way.

Mechanic:
  - Daily strike selection identical to BearOnly: ATM = round(spot_open/100)*100,
    CE = ATM - itm_offset_pts, PE = ATM + itm_offset_pts. Fixed-offset only
    -- no OI-wall strike selection, no spot HTF bias filter (the validated
    config used neither; explicit scope decision, see the implementation plan).
  - Same 15m/60m HTF sweep+reclaim zone pool, 14-day REST warmup at
    strike-selection time, exactly like BearOnly.
  - Per closed 1-min bar, per side: feed the bar to that side's
    SRPingPongTracker (fresh instance each trading day). An "entry" event
    places a BUY; an "exit" event (eod/risk_cap/sl_<mode>) places the
    matching SELL. At most ONE leg open per side per day -- the tracker
    enforces this internally (matches the validated backtest's shape).
  - Config defaults (BANKNIFTY, from the 2026-08-08 sweep): htf_minutes=15,
    itm_offset_pts=300, sr_tf_minutes=3, exit_mode="raw". All overridable
    per-deployment via strategy_params. itm_offset_pts/htf_minutes fall back
    to bear_only_book.py's own per-underlying default dicts if not given.
  - Order/fill plumbing fully reused from D1Trap's existing infra --
    D1TrapOrderEvent / Topic.D1_TRAP_ORDER_REQUEST / D1_TRAP_ORDER_FILL /
    D1TrapFillEvent / execution_bridge/d1_trap_bridge.py -- tagged
    strategy="d1_trap_sr". No new bridge. Confirm-then-finalize
    entry/exit + position_store persistence copied from BearOnly's own
    proven, tested pattern (bear_only_book.py's _enter_leg/_square_off_leg/
    _on_fill/_fill_loop/_persist_positions/_restore_positions).

Status: NOT deployed yet -- built 2026-08-08. Smoke-test against real
BANKNIFTY data before first paper deployment (see the implementation plan's
verification section). Deploy PAPER mode only initially.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional

import pandas as pd

from config.global_config import IST, Topic
from data_layer.base_feeder import OptionTick, IndexTick
from data_layer.historical_candles import fetch_upstox_range_1m
from data_layer.instrument_registry import REGISTRY
from data_layer import position_store
from strategies.core.base_book import AbstractStrategyBook
from strategies.d1_trap_option.book import D1TrapOrderEvent, _upstox_key_for
from strategies.d1_trap_option.support_resistance import SRPingPongTracker
from strategies.d1_trap_option.bear_only_book import (
    _Bar, _OptionSeries, _resample, _to_bars, _detect_bear_zones, _prevalidate_zones,
    _ITM_OFFSET_DEFAULT_BY_UNDERLYING, _HTF_MINUTES_DEFAULT_BY_UNDERLYING,
    _ATM_ROUND_STEP, _HIST_WARMUP_DAYS, _MAX_ZONE_AGE_DAYS,
    _STRIKE_SELECT_TIME, _SESSION_CLOSE, _EARLY_SESSION_CUTOFF,
)

logger = logging.getLogger(__name__)

_SR_TF_MINUTES_DEFAULT = 3
_SR_EXIT_MODE_DEFAULT = "raw"
_EOD_TIME = time(15, 15)
_EXIT_CONFIRM_TIMEOUT_SEC = 15.0

# 2026-08-12, direct user request/validated backtest (scripts/d1trap_fixed_
# monthly_strike_sweep.py, scripts/d1trap_fixed_weekly_strike_sweep.py,
# scripts/d1trap_scan_vs_execute_strike_sweep.py): instead of recomputing
# CE/PE off TODAY's own ATM every day, anchor to the PREVIOUS period's real
# spot high/low and hold that SAME strike pair for the whole current period
# -- consistently beat daily-ATM at deeper ITM depth across all three
# underlyings tested (BANKNIFTY @300, NIFTY @300, SENSEX @600 all showed
# real, repeated PF/win% gains; the exact PF numbers on the thin n=9-14
# samples should NOT be taken at face value, but the DIRECTION repeated
# three independent times). BANKNIFTY/FINNIFTY are monthly expiry so
# "period" = calendar month; NIFTY/SENSEX are weekly, so "period" = ISO
# week (Mon-Sun, a simplification -- not exactly aligned to the real
# NSE/BSE weekly expiry cycle, noted since it could matter later).
_MONTHLY_EXPIRY_UNDERLYINGS = {"BANKNIFTY", "FINNIFTY"}
_STRIKE_MODE_DEFAULT = "daily_atm"          # "daily_atm" | "fixed_period"
_EXECUTE_STRIKE_MODE_DEFAULT = "same"       # "same" | "daily_itm1"


class D1TrapSRBook(AbstractStrategyBook):
    """Per-(client, binding) live book. S&R ping-pong entry/exit driven off
    BearOnly's own HTF zone pool. Always underlying="BANKNIFTY" today (the
    only index this mechanic has been validated for); other underlyings are
    a config change once validated there too, same as BearOnly's own history."""

    def __init__(
        self,
        bus,
        cfg,
        underlying: str,
        client_id: str,
        binding_id: str,
        lot_multiplier: int = 1,
        feeder_token: str = "",
        itm_offset_pts: Optional[int] = None,
        htf_minutes: Optional[int] = None,
        sr_tf_minutes: int = _SR_TF_MINUTES_DEFAULT,
        exit_mode: str = _SR_EXIT_MODE_DEFAULT,
        product_type: str = "MIS",
        carry_forward: bool = False,
        squareoff_time: str = "15:15",
        strike_mode: str = _STRIKE_MODE_DEFAULT,
        execute_strike_mode: str = _EXECUTE_STRIKE_MODE_DEFAULT,
    ) -> None:
        super().__init__(bus, cfg, underlying, client_id, binding_id)
        self._strategy_name = "d1_trap_sr"
        self._lot_multiplier = max(1, lot_multiplier)
        self._feeder_token = feeder_token
        self._itm_offset_pts = (
            itm_offset_pts if itm_offset_pts is not None
            else _ITM_OFFSET_DEFAULT_BY_UNDERLYING.get(underlying.upper(), 200)
        )
        self._htf_minutes = (
            htf_minutes if htf_minutes is not None
            else _HTF_MINUTES_DEFAULT_BY_UNDERLYING.get(underlying.upper(), 60)
        )
        self._sr_tf_minutes = int(sr_tf_minutes)
        self._exit_mode = exit_mode
        self._strike_mode = strike_mode if strike_mode in ("daily_atm", "fixed_period") else "daily_atm"
        self._execute_strike_mode = (
            execute_strike_mode if execute_strike_mode in ("same", "daily_itm1") else "same"
        )
        self._product_type = product_type
        self._carry_forward = bool(carry_forward)
        try:
            _h, _m = str(squareoff_time or "15:15").split(":")
            self._squareoff_time = time(int(_h), int(_m))
        except Exception:
            self._squareoff_time = _EOD_TIME
        self._lot_size = (cfg.exchange.lot_sizes.get(underlying, 75) if cfg else 75)
        self._strike_step = int(cfg.exchange.strike_steps.get(underlying, 50) if cfg else 50)
        self._persist_key = f"{client_id}_{binding_id}_{underlying}_d1_trap_sr"

        self._today: Optional[date] = None
        self._ce_strike: Optional[int] = None
        self._pe_strike: Optional[int] = None
        self._series: Dict[str, _OptionSeries] = {}
        self._sr_trackers: Dict[str, SRPingPongTracker] = {}   # "CE"/"PE" -> today's tracker
        self._last_spot_open: Optional[float] = None
        self._last_spot: Optional[float] = None   # live-updating, for monitoring_zones() display only
        self._selection_reason: Optional[str] = None
        # strike_mode="fixed_period": cache survives reset_session() (NOT cleared
        # daily) since the whole point is the strike stays fixed across many days
        # within one period -- keyed by _period_key() so a new period recomputes.
        self._period_strike_cache: Dict[tuple, tuple] = {}
        # execute_strike_mode="daily_itm1": the SCAN strike (self._ce_strike/
        # _pe_strike) only ever drives zone detection + entry/exit TIMING; these
        # are the strike REAL orders actually get placed on, recomputed fresh
        # every day off that day's own ATM +/- 1 real strike step, independent
        # of whatever strike_mode picked for scanning.
        self._exec_ce_strike: Optional[int] = None
        self._exec_pe_strike: Optional[int] = None
        self._execute_ltp: Dict[str, float] = {}   # "CE"/"PE" -> latest live LTP on the execute strike
        # At most ONE leg per side, but keep a list for shape-parity with
        # BearOnly's status()/persistence -- SRPingPongTracker itself already
        # enforces the one-position-at-a-time invariant per side per day.
        self._positions: List[dict] = []
        self._day_done = False
        self._selecting_strikes = False
        self._warming_up = False
        self._rest_open_attempted = False
        self._event_counter = 0
        self._fill_waiters: Dict[str, asyncio.Event] = {}
        self._fill_results: Dict[str, object] = {}
        self._stop_for_day = False
        self._consecutive_entry_rejections = 0

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        super().start()
        self._subscribe(Topic.INDEX_TICK)
        self._subscribe(Topic.OPTION_TICK)
        self._subscribe(Topic.D1_TRAP_ORDER_FILL)
        self._tasks.append(asyncio.create_task(
            self._index_tick_loop(), name=f"sr_idx_{self._underlying}"))
        self._tasks.append(asyncio.create_task(
            self._option_tick_loop(), name=f"sr_opt_{self._underlying}"))
        self._tasks.append(asyncio.create_task(
            self._eod_loop(), name=f"sr_eod_{self._underlying}"))
        self._tasks.append(asyncio.create_task(
            self._startup_open_fetch(), name=f"sr_openfetch_{self._underlying}"))
        self._tasks.append(asyncio.create_task(
            self._fill_loop(), name=f"sr_fill_{self._underlying}"))

    async def _fill_loop(self) -> None:
        """Mirrors bear_only_book.py's _fill_loop exactly -- consumes
        Topic.D1_TRAP_ORDER_FILL, the confirm-then-finalize round trip's
        other half."""
        from execution_bridge.d1_trap_bridge import D1TrapFillEvent
        q = self._loop_queues.get(Topic.D1_TRAP_ORDER_FILL)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if not isinstance(ev, D1TrapFillEvent):
                continue
            if (ev.client_id != self._client_id or ev.binding_id != self._binding_id
                    or ev.underlying != self._underlying):
                continue
            try:
                self._on_fill(ev)
            except Exception:
                logger.exception("D1TrapSR[%s]: _on_fill error (recovered, fill loop alive).",
                                  self._underlying)

    def _on_fill(self, fill) -> None:
        """Mirrors bear_only_book.py's _on_fill exactly."""
        eid = getattr(fill, "event_id", "")
        if fill.action == "BUY":
            if getattr(fill, "entry_aborted", False):
                before = len(self._positions)
                self._positions = [p for p in self._positions if p.get("_event_id") != eid]
                if len(self._positions) != before:
                    self._persist_positions()
                    logger.critical(
                        "D1TrapSR[%s]: ENTRY ABORTED (broker unavailable/routing failed, "
                        "event_id=%s) -- discarding optimistic leg.", self._underlying, eid,
                    )
                self._consecutive_entry_rejections += 1
                if self._consecutive_entry_rejections >= 3:
                    self._stop_for_day = True
                    logger.critical(
                        "D1TrapSR[%s]: STOPPING ENTRIES FOR TODAY -- %d consecutive entry "
                        "rejections.", self._underlying, self._consecutive_entry_rejections,
                    )
                return
            self._consecutive_entry_rejections = 0
            return
        if fill.action == "SELL":
            if eid:
                self._fill_results[eid] = fill
            waiter = self._fill_waiters.get(eid)
            if waiter is not None:
                try:
                    waiter.set()
                except RuntimeError:
                    pass

    async def _startup_open_fetch(self) -> None:
        """Mirrors bear_only_book.py's _startup_open_fetch exactly."""
        if not self._feeder_token:
            self._rest_open_attempted = True
            return
        today = datetime.now(IST).date()
        if today.weekday() >= 5:
            self._rest_open_attempted = True
            return
        try:
            key = _upstox_key_for(self._underlying)
            from data_layer.historical_candles import fetch_upstox_intraday_1m
            rows = await fetch_upstox_intraday_1m(key, self._feeder_token)
            if not rows:
                logger.info("D1TrapSR[%s]: no intraday bars yet for %s (pre-market) — "
                            "will select strikes off the first live tick after 09:16.",
                            self._underlying, today)
                return
            open_px = float(rows[0]["open"])
            if self._today != today:
                self.reset_session()
                self._today = today
            if self._last_spot_open is None and not self._selecting_strikes:
                self._last_spot_open = open_px
                self._selecting_strikes = True
                logger.info("D1TrapSR[%s]: fetched TODAY's real open=%.2f via REST -- "
                            "selecting strikes now.", self._underlying, open_px)
                asyncio.create_task(self._select_strikes_for_today(open_px))
        except Exception:
            logger.exception("D1TrapSR[%s]: startup open-fetch failed — falling back to live tick.",
                              self._underlying)
        finally:
            self._rest_open_attempted = True

    def reset_session(self) -> None:
        self._today = None
        self._ce_strike = None
        self._pe_strike = None
        self._series = {}
        self._sr_trackers = {}
        self._last_spot_open = None
        self._selection_reason = None
        self._day_done = False
        self._stop_for_day = False
        self._consecutive_entry_rejections = 0
        # NOTE: self._period_strike_cache is deliberately NOT cleared here --
        # strike_mode="fixed_period"'s whole point is holding the same strike
        # across many days within one period; only a new period_key recomputes.
        self._exec_ce_strike = None
        self._exec_pe_strike = None
        self._execute_ltp = {}

    # ── daily strike selection ──────────────────────────────────────────────

    async def _index_tick_loop(self) -> None:
        q = self._loop_queues.get(Topic.INDEX_TICK)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if not isinstance(ev, IndexTick):
                continue
            is_own_underlying = (
                ev.symbol == self._underlying
                or (self._underlying == "NIFTY"
                    and ev.symbol in ("NSE_INDEX|Nifty 50", "NIFTY", "NSE:NIFTY50-INDEX"))
                or (self._underlying == "SENSEX"
                    and ev.symbol in ("BSE_INDEX|SENSEX", "SENSEX"))
                or (self._underlying == "BANKNIFTY"
                    and ev.symbol in ("NSE_INDEX|Nifty Bank", "BANKNIFTY"))
            )
            if not is_own_underlying or not ev.ltp:
                continue
            self._last_spot = ev.ltp
            today = ev.timestamp.date() if hasattr(ev, "timestamp") else datetime.now(IST).date()
            if self._today != today:
                self.reset_session()
                self._today = today
            now_t = datetime.now(IST).time()
            if (self._last_spot_open is None and _STRIKE_SELECT_TIME <= now_t <= _SESSION_CLOSE
                    and not self._selecting_strikes and self._rest_open_attempted):
                self._last_spot_open = ev.ltp
                self._selecting_strikes = True
                asyncio.create_task(self._select_strikes_for_today(ev.ltp))

    def _fixed_offset_strikes(self, spot_open: float) -> tuple[int, int]:
        atm = round(spot_open / _ATM_ROUND_STEP) * _ATM_ROUND_STEP
        return int(atm - self._itm_offset_pts), int(atm + self._itm_offset_pts)

    def _period_key(self, d: date) -> tuple:
        if self._underlying.upper() in _MONTHLY_EXPIRY_UNDERLYINGS:
            return (d.year, d.month)
        y, w, _ = d.isocalendar()
        return (y, w)

    async def _prev_period_hilo(self, today: date) -> tuple[Optional[float], Optional[float]]:
        """Real spot high/low over the PREVIOUS period (calendar month for
        monthly-expiry underlyings, ISO week for weekly-expiry ones)."""
        if self._underlying.upper() in _MONTHLY_EXPIRY_UNDERLYINGS:
            first_of_this_month = today.replace(day=1)
            prev_end = first_of_this_month - timedelta(days=1)
            prev_start = prev_end.replace(day=1)
        else:
            this_monday = today - timedelta(days=today.weekday())
            prev_start = this_monday - timedelta(days=7)
            prev_end = this_monday - timedelta(days=1)
        if not self._feeder_token:
            return None, None
        spot_key = _upstox_key_for(self._underlying)
        rows = await fetch_upstox_range_1m(spot_key, self._feeder_token, prev_start, prev_end)
        if not rows:
            return None, None
        return min(r["low"] for r in rows), max(r["high"] for r in rows)

    async def _fixed_period_strikes(self, spot_open: float) -> tuple[int, int]:
        """strike_mode="fixed_period": anchor CE/PE to the PREVIOUS period's
        real spot low/high and cache per period_key so the SAME strikes hold
        across every day within the current period (not recomputed daily).
        Falls back to daily-ATM if prev-period data can't be resolved (no
        token, holiday-only range, etc.) rather than leaving strikes unset."""
        today = self._today or datetime.now(IST).date()
        period_key = self._period_key(today)
        cached = self._period_strike_cache.get(period_key)
        if cached is not None:
            return cached
        prev_lo, prev_hi = await self._prev_period_hilo(today)
        if prev_lo is None or prev_hi is None:
            logger.warning(
                "D1TrapSR[%s]: strike_mode=fixed_period but could not resolve prev-period "
                "high/low -- falling back to daily ATM for %s.", self._underlying, period_key,
            )
            return self._fixed_offset_strikes(spot_open)
        ce = int(round(prev_lo / _ATM_ROUND_STEP) * _ATM_ROUND_STEP - self._itm_offset_pts)
        pe = int(round(prev_hi / _ATM_ROUND_STEP) * _ATM_ROUND_STEP + self._itm_offset_pts)
        self._period_strike_cache[period_key] = (ce, pe)
        logger.info(
            "D1TrapSR[%s]: fixed_period strikes for %s -- prev period low=%.2f high=%.2f -> CE=%d PE=%d",
            self._underlying, period_key, prev_lo, prev_hi, ce, pe,
        )
        return ce, pe

    async def _setup_execute_strikes(self, spot_open: float) -> None:
        """execute_strike_mode="daily_itm1": REAL orders are placed on this
        strike (today's own ATM +/- 1 real strike step), independent of the
        SCAN strike driving zone detection/timing. Subscribes the feeder to
        both legs so _option_tick_loop can track their live LTP -- paper mode
        trusts whatever entry_price/exit_price the book passes in (it does
        NOT look up real market data itself), so without this, a paper fill
        would silently book the SCAN strike's price against the EXECUTE
        strike's instrument -- a real mismatch, not just a display issue."""
        atm = round(spot_open / _ATM_ROUND_STEP) * _ATM_ROUND_STEP
        self._exec_ce_strike = int(atm - self._strike_step)
        self._exec_pe_strike = int(atm + self._strike_step)
        self._execute_ltp = {}
        today = self._today or datetime.now(IST).date()
        expiry = REGISTRY.get_active_expiry_strict(self._underlying, today)
        logger.info("D1TrapSR[%s]: execute strikes (daily 1-ITM, step=%d) CE=%d PE=%d",
                    self._underlying, self._strike_step, self._exec_ce_strike, self._exec_pe_strike)
        if expiry is None:
            return
        gf = getattr(self._bus, "_global_feeder", None)
        for side, strike in (("CE", self._exec_ce_strike), ("PE", self._exec_pe_strike)):
            key = REGISTRY.get_upstox_key(self._underlying, expiry, strike, side)
            if key and gf is not None and hasattr(gf, "subscribe_tokens"):
                asyncio.create_task(gf.subscribe_tokens([key]))

    async def _select_strikes_for_today(self, spot_open: float) -> None:
        try:
            if self._strike_mode == "fixed_period":
                ce_strike, pe_strike = await self._fixed_period_strikes(spot_open)
            else:
                ce_strike, pe_strike = self._fixed_offset_strikes(spot_open)
            atm = round(spot_open / _ATM_ROUND_STEP) * _ATM_ROUND_STEP
            logger.info(
                "D1TrapSR[%s]: spot_open=%.2f ATM=%d -> CE=%d PE=%d | "
                "effective config: htf=%dm itm_offset=%dpt sr_tf=%dm exit_mode=%s strike_mode=%s "
                "execute_strike_mode=%s",
                self._underlying, spot_open, atm, ce_strike, pe_strike,
                self._htf_minutes, self._itm_offset_pts, self._sr_tf_minutes, self._exit_mode,
                self._strike_mode, self._execute_strike_mode,
            )
            self._ce_strike, self._pe_strike = ce_strike, pe_strike
            self._selection_reason = (
                f"ATM={atm} (spot_open={spot_open:.0f}) -> fixed-offset: "
                f"CE=ATM-{self._itm_offset_pts}, PE=ATM+{self._itm_offset_pts}"
                if self._strike_mode == "daily_atm" else
                f"fixed_period ({self._period_key(self._today or datetime.now(IST).date())}): CE={ce_strike} PE={pe_strike}"
            )
            if self._execute_strike_mode == "daily_itm1":
                await self._setup_execute_strikes(spot_open)
            self._restore_positions()

            today = self._today or datetime.now(IST).date()
            expiry = REGISTRY.get_active_expiry_strict(self._underlying, today)
            if expiry is None or not self._feeder_token:
                logger.warning("D1TrapSR[%s]: no expiry/token — cannot warm history.", self._underlying)
                self._series["CE"] = _OptionSeries(strike=ce_strike, side="CE")
                self._series["PE"] = _OptionSeries(strike=pe_strike, side="PE")
            else:
                for side, strike in (("CE", ce_strike), ("PE", pe_strike)):
                    key = REGISTRY.get_upstox_key(self._underlying, expiry, strike, side)
                    series = _OptionSeries(strike=strike, side=side)
                    if key:
                        start = today - timedelta(days=_HIST_WARMUP_DAYS)
                        rows = await fetch_upstox_range_1m(key, self._feeder_token, start,
                                                            today - timedelta(days=1))
                        series.bars_1m = [
                            _Bar(pd.Timestamp(r["ts"]).tz_convert(IST) if pd.Timestamp(r["ts"]).tzinfo
                                 else pd.Timestamp(r["ts"]).tz_localize(IST),
                                 r["open"], r["high"], r["low"], r["close"])
                            for r in rows
                        ]
                        m_htf = _resample(series.to_df(), self._htf_minutes)
                        m15_hist = _resample(series.to_df(), 15)
                        series.zones = _prevalidate_zones(_detect_bear_zones(_to_bars(m_htf)), m15_hist)
                        logger.info("D1TrapSR[%s]: %s %d warmed %d 1m bars -> %d HTF zones",
                                    self._underlying, side, strike, len(series.bars_1m), len(series.zones))
                    self._series[side] = series

            # Fresh S&R tracker per side per day -- matches the validated backtest's
            # one-instance-per-(side,day) shape exactly.
            for side in ("CE", "PE"):
                if side in self._series:
                    self._sr_trackers[side] = SRPingPongTracker(
                        self._series[side].zones, self._sr_tf_minutes, self._lot_size * self._lot_multiplier,
                        exit_mode=self._exit_mode,
                    )
        except Exception:
            logger.exception("D1TrapSR[%s]: strike selection failed.", self._underlying)
        finally:
            self._selecting_strikes = False

    # ── live option ticks ────────────────────────────────────────────────────

    async def _option_tick_loop(self) -> None:
        q = self._loop_queues.get(Topic.OPTION_TICK)
        if q is None:
            logger.warning("D1TrapSR[%s]: OPTION_TICK queue is None -- subscribe() failed at start().",
                            self._underlying)
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if not isinstance(ev, OptionTick) or not ev.ltp:
                continue
            if self._execute_strike_mode == "daily_itm1":
                ev_strike = int(getattr(ev, "strike", 0) or 0)
                ev_side = str(getattr(ev, "option_type", "")).upper()
                if self._exec_ce_strike and ev_strike == self._exec_ce_strike and ev_side == "CE":
                    self._execute_ltp["CE"] = ev.ltp
                elif self._exec_pe_strike and ev_strike == self._exec_pe_strike and ev_side == "PE":
                    self._execute_ltp["PE"] = ev.ltp
            side = self._match_side(ev)
            if side is None:
                continue
            series = self._series.get(side)
            if series is None:
                continue
            ts = getattr(ev, "timestamp", None) or datetime.now(IST)
            closed = series.on_tick(ts, ev.ltp)
            if closed:
                self._process_new_bar(side)

    def _match_side(self, ev: OptionTick) -> Optional[str]:
        if self._ce_strike and int(getattr(ev, "strike", 0) or 0) == self._ce_strike \
                and str(getattr(ev, "option_type", "")).upper() == "CE":
            return "CE"
        if self._pe_strike and int(getattr(ev, "strike", 0) or 0) == self._pe_strike \
                and str(getattr(ev, "option_type", "")).upper() == "PE":
            return "PE"
        return None

    # ── zone maintenance + S&R tracking ─────────────────────────────────────

    def _update_zones(self, side: str) -> None:
        """Detection + merge ONLY -- deliberately does not run BearOnly's own
        WAITING/MONITORING/ongoing-invalidation states, since the validated
        S&R backtest never used them either (see module docstring)."""
        series = self._series[side]
        df_1m = series.to_df()
        if len(df_1m) < 30:
            return
        m_htf = _resample(df_1m, self._htf_minutes)
        m15 = _resample(df_1m, 15)
        bars_htf = _to_bars(m_htf)
        existing_refs = {z["ref_ts"] for z in series.zones}
        new_zones = [z for z in _detect_bear_zones(bars_htf) if z["ref_ts"] not in existing_refs]
        new_zones = _prevalidate_zones(new_zones, m15)
        series.zones.extend(new_zones)

    def _process_new_bar(self, side: str) -> None:
        if self._day_done:
            return
        series = self._series[side]
        if not series.bars_1m:
            return
        last_bar_ts = series.bars_1m[-1].timestamp
        if last_bar_ts.time() >= self._squareoff_time:
            return
        self._update_zones(side)
        tracker = self._sr_trackers.get(side)
        if tracker is None:
            return
        ev = tracker.on_bar(series.bars_1m[-1])
        if ev is None:
            return
        if ev["type"] == "entry":
            self._enter_leg(side, ev)
        elif ev["type"] == "exit":
            asyncio.create_task(self._square_off_from_event(side, ev))

    # ── entry / exit ─────────────────────────────────────────────────────────

    def _enter_leg(self, side: str, ev: dict) -> None:
        if self._stop_for_day:
            logger.warning("D1TrapSR[%s]: skip %s entry @ %.2f -- stopped for the day.",
                            self._underlying, side, ev["entry_premium"])
            return
        qty = self._lot_size * self._lot_multiplier
        scan_strike = self._ce_strike if side == "CE" else self._pe_strike
        strike, entry_price = scan_strike, ev["entry_premium"]
        if self._execute_strike_mode == "daily_itm1":
            exec_strike = self._exec_ce_strike if side == "CE" else self._exec_pe_strike
            real_ltp = self._execute_ltp.get(side)
            if exec_strike is None or real_ltp is None:
                logger.warning(
                    "D1TrapSR[%s]: %s entry signal fired on scan strike %d but execute strike/live "
                    "LTP not ready (exec_strike=%s ltp=%s) -- skipping this entry rather than trading "
                    "a stale/wrong price.", self._underlying, side, scan_strike, exec_strike, real_ltp,
                )
                return
            strike, entry_price = exec_strike, real_ltp
        pos = dict(
            side=side, strike=strike, scan_strike=scan_strike,
            entry_price=entry_price, initial_sl=ev["initial_sl"], entry_ts=ev["entry_ts"],
            qty=qty, order_reason="sr_ping_pong_entry",
        )
        self._event_counter += 1
        eid = f"{self._underlying}_{side}{pos['strike']}_ENTRY_{self._event_counter}"
        pos["_event_id"] = eid
        self._positions.append(pos)
        self._persist_positions()
        logger.info("D1TrapSR[%s]: ENTER BUY %s %d entry=%.2f initial_sl=%.2f "
                    "(awaiting broker confirmation, event_id=%s)",
                    self._underlying, side, pos["strike"], pos["entry_price"], pos["initial_sl"], eid)

        expiry = REGISTRY.get_active_expiry_strict(self._underlying, self._today or datetime.now(IST).date())
        order_ev = D1TrapOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id,
            strategy="d1_trap_sr", direction="LONG", action="BUY",
            quantity=qty, entry_price=pos["entry_price"], sl_price=pos["initial_sl"],
            tsl_level=pos["initial_sl"], trigger_ts=datetime.now(IST), reason="sr_ping_pong_entry",
            underlying=self._underlying, option_type=side, strike=pos["strike"], expiry=expiry,
            product_type=self._product_type, event_id=eid,
        )
        if self._bus is not None:
            asyncio.create_task(self._bus.publish(Topic.D1_TRAP_ORDER_REQUEST, order_ev))

    async def _square_off_from_event(self, side: str, ev: dict) -> None:
        pos = next((p for p in self._positions if p["side"] == side and not p.get("_closing")), None)
        if pos is None:
            return
        exit_price = ev["exit_price"]
        if self._execute_strike_mode == "daily_itm1":
            real_ltp = self._execute_ltp.get(side)
            if real_ltp is not None:
                exit_price = real_ltp
            else:
                # Exits must always proceed (an open real position needs closing)
                # -- fall back to the scan-strike exit price rather than leaving
                # the leg open, but log loudly since this means the booked P&L
                # won't reflect the actually-executed strike's real price.
                logger.warning(
                    "D1TrapSR[%s]: %s exit signal fired but execute strike live LTP unavailable -- "
                    "using scan-strike exit price %.2f as fallback so the real leg still closes.",
                    self._underlying, side, exit_price,
                )
        await self._square_off_leg(pos, ev["reason"], exit_price)

    async def _square_off_leg(self, pos: dict, reason: str, exit_price: float) -> None:
        """Confirm-then-finalize EXIT -- mirrors bear_only_book.py's
        _square_off_leg exactly (same proven pattern, same timeout)."""
        if not any(p is pos for p in self._positions):
            return
        if pos.get("_closing"):
            return
        pos["_closing"] = True
        try:
            self._event_counter += 1
            eid = f"{self._underlying}_{pos['side']}{pos['strike']}_EXIT_{self._event_counter}"
            expiry = REGISTRY.get_active_expiry_strict(self._underlying, self._today or datetime.now(IST).date())
            order_ev = D1TrapOrderEvent(
                client_id=self._client_id, binding_id=self._binding_id,
                strategy="d1_trap_sr", direction="LONG", action="SELL",
                quantity=pos["qty"], entry_price=pos["entry_price"], sl_price=pos["initial_sl"],
                tsl_level=pos["initial_sl"], trigger_ts=datetime.now(IST), reason=reason,
                underlying=self._underlying, option_type=pos["side"], strike=pos["strike"],
                expiry=expiry, product_type=self._product_type, exit_price=exit_price,
                entry_reason=pos.get("order_reason", "") or "", entry_ts=pos.get("entry_ts"),
                event_id=eid,
            )
            logger.info("D1TrapSR[%s]: SELL %s %d reason=%s exit=%.2f "
                        "(awaiting broker confirmation, event_id=%s)",
                        self._underlying, pos["side"], pos["strike"], reason, exit_price, eid)

            waiter = asyncio.Event()
            self._fill_waiters[eid] = waiter
            try:
                if self._bus is not None:
                    await self._bus.publish(Topic.D1_TRAP_ORDER_REQUEST, order_ev)
                try:
                    await asyncio.wait_for(waiter.wait(), timeout=_EXIT_CONFIRM_TIMEOUT_SEC)
                except asyncio.TimeoutError:
                    logger.critical(
                        "D1TrapSR[%s]: EXIT %s%d fill NOT CONFIRMED within %.0fs (event_id=%s "
                        "reason=%s) -- leg stays OPEN; will retry on a later tick/EOD pass.",
                        self._underlying, pos["side"], pos["strike"], _EXIT_CONFIRM_TIMEOUT_SEC, eid, reason,
                    )
                    return
            finally:
                self._fill_waiters.pop(eid, None)

            fill = self._fill_results.pop(eid, None)
            if fill is not None and getattr(fill, "exit_failed", False):
                logger.critical(
                    "D1TrapSR[%s]: EXIT %s%d ABORTED by bridge (broker unavailable, event_id=%s "
                    "reason=%s) -- leg stays OPEN; will retry on a later tick/EOD pass.",
                    self._underlying, pos["side"], pos["strike"], eid, reason,
                )
                return

            self._positions = [p for p in self._positions if p is not pos]
            self._persist_positions()
            logger.info("D1TrapSR[%s]: SELL %s %d reason=%s exit=%.2f CONFIRMED (event_id=%s)",
                        self._underlying, pos["side"], pos["strike"], reason, exit_price, eid)
        finally:
            pos["_closing"] = False

    async def _eod_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            if self._carry_forward:
                continue
            now = datetime.now(IST)
            if now.time() >= self._squareoff_time and not self._day_done:
                for pos in list(self._positions):
                    if pos.get("_closing"):
                        continue
                    series = self._series.get(pos["side"])
                    ltp = series.last_ltp if series else pos["entry_price"]
                    await self._square_off_leg(pos, "eod", ltp)
                self._day_done = True

    async def liquidate(self, reason: str = "kill_switch") -> None:
        for pos in list(self._positions):
            series = self._series.get(pos["side"])
            ltp = series.last_ltp if series else pos["entry_price"]
            await self._square_off_leg(pos, reason, ltp)

    # ── position persistence ─────────────────────────────────────────────────

    def _persist_positions(self) -> None:
        if self._positions:
            legs = []
            for pos in self._positions:
                d = {k: v for k, v in pos.items() if not k.startswith("_")}
                d["entry_ts"] = pos["entry_ts"].isoformat() if pos.get("entry_ts") else None
                legs.append(d)
            position_store.save(self._persist_key, {"legs": legs}, product_type=self._product_type)
        else:
            position_store.clear(self._persist_key)

    def _restore_positions(self) -> None:
        data = position_store.load(self._persist_key)
        if not data:
            return
        restored = []
        for d in (data.get("legs") or []):
            side = d.get("side")
            # A stored leg's strike is whatever REAL orders trade on -- the
            # execute strike when execute_strike_mode="daily_itm1" is active
            # (recomputed same-day so this matches), else the scan strike.
            if self._execute_strike_mode == "daily_itm1":
                expected_strike = self._exec_ce_strike if side == "CE" else self._exec_pe_strike
            else:
                expected_strike = self._ce_strike if side == "CE" else self._pe_strike
            if d.get("strike") != expected_strike:
                logger.warning("D1TrapSR[%s]: discarding stored %s leg -- strike %s doesn't match "
                                "today's selected %s.", self._underlying, side, d.get("strike"), expected_strike)
                continue
            try:
                d["entry_ts"] = (pd.Timestamp(d["entry_ts"]).to_pydatetime()
                                  if d.get("entry_ts") else datetime.now(IST))
            except Exception:
                logger.exception("D1TrapSR[%s]: failed to parse stored leg timestamps -- discarding.",
                                  self._underlying)
                continue
            restored.append(d)
        if restored:
            self._positions = restored
            logger.info("D1TrapSR[%s]: RESTORED %d open leg(s) from disk on restart -- %s",
                        self._underlying, len(restored),
                        ", ".join(f"{p['side']}{p['strike']}@{p['entry_price']:.2f}" for p in restored))

    # ── status / UI ──────────────────────────────────────────────────────────

    def _leg_view(self, pos: dict) -> dict:
        series = self._series.get(pos["side"])
        return dict(
            side=pos["side"], strike=pos["strike"], entry=pos["entry_price"],
            initial_sl=pos["initial_sl"], qty=pos["qty"],
            ltp=series.last_ltp if series else None,
        )

    def status(self) -> dict:
        legs = [self._leg_view(p) for p in self._positions]
        return dict(
            strategy="d1_trap_sr", underlying=self._underlying,
            ce_strike=self._ce_strike, pe_strike=self._pe_strike,
            spot_open=self._last_spot_open, selection_reason=self._selection_reason,
            position=legs[0] if legs else None, positions=legs,
            sr_tf_minutes=self._sr_tf_minutes, exit_mode=self._exit_mode,
        )

    def monitoring_zones(self) -> dict:
        """Same shape as D1TrapOptionBook.monitoring_zones() (book.py) so the existing
        WATCHLIST TRACKER UI (/api/d1trap/zones) can render this book too -- 2026-08-12
        fix: this class never had this method at all, so NIFTY/SENSEX/BANKNIFTY d1_trap_sr
        books were completely invisible in that panel despite running and holding real
        zones/positions, same class of gap fno_sr_book.py's monitoring_zones() fixed for
        the FnO WATCHLIST book on 2026-08-11.

        "MONITORING" here means the zone has been touched -- i.e. its lock_ts is a key in
        that side's SRPingPongTracker.active_sr dict (the tracker only starts a zone's own
        SupportResistanceCalculator once price has actually touched it, gate_mode="touch").
        No separate WAITING->MONITORING->invalid state machine exists on this book's own
        zone dicts the way bear_only_book.py has -- that state lives inside the tracker,
        not here, by design (see this file's own module docstring)."""
        spot = self._last_spot
        zones = []
        by_side: Dict[str, list] = {"CE": [], "PE": []}
        for side in ("CE", "PE"):
            series = self._series.get(side)
            tracker = self._sr_trackers.get(side)
            if series is None:
                continue
            touched_lock_ts = set((tracker.active_sr or {}).keys()) if tracker is not None else set()
            # 2026-08-12 fix: these zones are detected on the OPTION'S OWN premium chart
            # (bear_only_book.py's design, reused unchanged -- see this file's module
            # docstring), NOT the index/spot. dist_pct MUST compare against that side's
            # own live premium (series.last_ltp), never self._last_spot (index points vs
            # option premium rupees -- comparing them produced nonsense like +7713%,
            # confirmed against real live BANKNIFTY/NIFTY/SENSEX numbers).
            side_ltp = series.last_ltp
            for z in series.zones:
                dist = None
                if side_ltp:
                    mid = (z["zone_lo"] + z["zone_hi"]) / 2
                    dist = round((side_ltp - mid) / mid * 100, 2) if mid else None
                zd = {
                    # "direction" kept as LONG/SHORT to match the WATCHLIST TRACKER UI's
                    # existing vocabulary (built for fno_sr_book.py's spot-bias zones) --
                    # CE zone -> LONG-biased trade, PE zone -> SHORT-biased trade, same
                    # mapping this mechanic already uses when it actually enters. Strike
                    # and the literal CE/PE side are carried separately so the UI can show
                    # exactly which contract each zone belongs to, not just the bias arrow.
                    "direction": "LONG" if side == "CE" else "SHORT",
                    "option_side": side,
                    "strike": series.strike,
                    "ltp": round(side_ltp, 2) if side_ltp else None,
                    "zone_lo": round(z["zone_lo"], 2),
                    "zone_hi": round(z["zone_hi"], 2),
                    "state": "MONITORING" if z["lock_ts"] in touched_lock_ts else "WAITING",
                    "dist_pct": dist,
                    "ref_ts": z["lock_ts"].strftime("%Y-%m-%d %H:%M") if z.get("lock_ts") else None,
                }
                zones.append(zd)
                by_side[side].append(zd)

        def _rank(zz: dict) -> tuple:
            return (0 if zz["state"] == "MONITORING" else 1,
                    abs(zz["dist_pct"]) if zz["dist_pct"] is not None else 999)

        zones.sort(key=_rank)
        for side in by_side:
            by_side[side].sort(key=_rank)

        # 2026-08-12, direct request: a mixed top-10 across both sides can crowd one side
        # out entirely (e.g. 10 CE zones outranking every PE zone) -- ce_zone/pe_zone
        # guarantee the UI can always show each side's own best zone side by side, not
        # just whichever side happened to win the combined ranking. `zones` (mixed,
        # capped 10) kept unchanged for anything else already reading it.
        return {
            "underlying": self._underlying,
            "client_id": self._client_id,
            "binding_id": self._binding_id,
            "spot": round(spot, 2) if spot else None,
            "zones": zones[:10],
            "ce_zone": by_side["CE"][0] if by_side["CE"] else None,
            "pe_zone": by_side["PE"][0] if by_side["PE"] else None,
            "ce_zone_count": len(by_side["CE"]),
            "pe_zone_count": len(by_side["PE"]),
            "total_zones": sum(len(s.zones) for s in self._series.values()),
            "pending": None,
            "position": bool(self._positions),
        }
