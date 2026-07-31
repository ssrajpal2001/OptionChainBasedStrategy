"""
strategies/d1_trap_option/bear_only_book.py — D1TrapBearOnlyBook (NEW, 2026-07-30).

Live implementation of the option-chart-native "bear-trap-only" mechanic
validated via scripts/d1trap_*.py backtests this session. This is a DIFFERENT
mechanic from D1TrapOptionBook (book.py) — it does NOT run on spot candles.

Mechanic:
  - Every trading day at/after 09:16 IST, once NIFTY's opening price is known,
    compute ATM = round(spot_open / 50) * 50, then:
        CE strike = ATM - itm_offset_pts   (200 default, 200-ITM call)
        PE strike = ATM + itm_offset_pts   (200 default, 200-ITM put)
    If either strike differs from what was tracked yesterday, that side's
    entire zone pool + 1-min bar history resets (new instrument, no stitching
    across strikes -- confirmed empirically that stitching different strikes'
    bars together corrupts zone detection with fake jumps).
  - REST-fetches ~25 trading days of that strike's own 1-min history at
    selection time to seed a 60-min zone pool immediately (no cold-start wait).
  - 60-min BEAR-TRAP-ONLY zones (find_all_bear_zones from v4_cascade's proven
    sweep+reclaim detector) on EACH option's own chart -- CE and PE scanned
    independently and simultaneously. Never bull-trap: this strategy only
    ever BUYS (goes long) whichever of CE/PE fires -- it never shorts an
    option, so a bull-trap (which would imply "sell this option") is unused.
  - Per zone: tick-wise CONTACT -> 15-min ref-candle (must close) -> tick-wise
    BREACH of its high -> 5-min sub-zone decomposition inside the ref candle
    -> ARM (retracement scaled by zone size vs 0.20% threshold) -> SWING
    BREACH of the sub-zone's own high = ENTRY. No 5-min sub-zone found ->
    raw_breakout fallback (enter at ref_high directly) -- empirically the
    stronger of the two options in the July 2026 backtest.
  - No continuation/retest flip (both would require a SHORT-direction trade,
    which this buyers-only strategy never takes).
  - Exit: SL = ref_low - 20pt buffer, hard-capped so max risk never exceeds
    Rs2000/lot regardless of zone width (2026-07-30 fix after a Rs3,776 SL
    was found in backtest). TSL = staircase, matching SellStraddle's
    tsl_scalable shape: activates at +20% premium profit -> locks 12.5%;
    every further +20% profit gained locks another +12.5% (repeating).
    EOD force-exit 15:15 IST (MIS, intraday only, never held overnight).

Requires the deployment's StrikeRebalancer chain_depth to cover
itm_offset_pts / strike_step strikes (>=4 for 200pt/50-step) so option ticks
for the required CE/PE actually arrive -- this book does not force-subscribe
new strikes itself; if ticks aren't flowing for the needed strike it will
report "no data" for that side rather than trade blind.

Status: live-paper deployed 2026-07-30/31. 2026-08-01: zone invalidation
(_prevalidate_zones + ongoing per-15m-close check) and the flip concept
(_create_flip_candidate / _process_flip_cancellation / _process_flip_entry)
rewritten to match the final corrected spec and validated via a 1-month
backtest against real Aug-4-expiry option data before going live again
(scripts/d1trap_month_backtest_v2.py -- 27 trades, win% 25.9, PF 0.53, net
Rs-15,658 over 2026-06-29..07-31; the flip concept itself fired once and
lost Rs2,000 -- inconclusive on this one month, going live-paper for 2 weeks
per explicit direction to get a larger sample before judging it).
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional

import pandas as pd

from config.global_config import IST, Topic
from data_layer.base_feeder import OptionTick, IndexTick
from data_layer.historical_candles import fetch_upstox_range_1m
from data_layer.instrument_registry import REGISTRY
from strategies.core.base_book import AbstractStrategyBook
from strategies.v4_cascade.rolling_base import find_all_bear_zones
from strategies.d1_trap_option.book import D1TrapOrderEvent, _upstox_key_for

logger = logging.getLogger(__name__)

_SESSION_OPEN = time(9, 15)
_STRIKE_SELECT_TIME = time(9, 16)
_SESSION_CLOSE = time(15, 30)   # upper bound so an off-hours/heartbeat tick can't
                                # trigger strike selection (found live 2026-07-30:
                                # feed still publishes last-known-price ticks after
                                # close, and an unbounded "now >= 09:16" check fired
                                # off one at 20:56 IST with the market shut).
_ENTRY_CUTOFF = time(14, 30)
_EARLY_SESSION_CUTOFF = time(9, 35)   # 2026-07-30 tweak: today's real 24000CE Trade 1
                                       # used the very first ref candle (09:15-09:30) --
                                       # opening-range volatility, wide/gap-driven, not
                                       # genuine intraday structure -- and whipsawed
                                       # within 9 minutes. Ref candles can't be assigned
                                       # until after this cutoff; a zone in MONITORING
                                       # during the excluded window just waits for a
                                       # later, more settled 15-min candle instead.
_EOD_TIME = time(15, 15)
_STRIKE_STEP = 50
_ATM_ROUND_STEP = 100   # ATM rounds to nearest 100 (2026-07-30 change); option
                        # strikes themselves stay on the normal 50pt grid --
                        # CE/PE = ATM +/- itm_offset_pts still land on valid strikes.
_MAX_ZONE_AGE_DAYS = 14   # 2026-07-31 tweak (was 20): matches _HIST_WARMUP_DAYS below --
                          # no point allowing zones up to 20 days old when only 14 days
                          # of history are ever fetched to find them in.
_ZONE_SIZE_THRESHOLD_PCT = 0.20
_ZONE_MERGE_THRESHOLD_PTS = 20.0   # 2026-07-31: collapse 60m zones within 20 option-pts of
                                   # each other into one (max/min), same principle as the
                                   # 5m sub-zone collapse -- cuts down near-duplicate zones.
                                   # Validated 2026-08-01 via a 1-month backtest against real
                                   # option data with the correct rolling _HIST_WARMUP_DAYS
                                   # window (scripts/d1trap_month_backtest_v2.py) -- collapsing
                                   # over the FULL month instead of the 14-day window it
                                   # actually runs on chains distant zones into one mega-band;
                                   # scoped correctly to 14 days it behaves sanely.
_SL_BUFFER_PTS = 20.0
_MAX_RISK_RS_PER_LOT = 2000.0
_TSL_BASE_PCT = 0.10        # 2026-07-30 tweak (was 0.20/0.125): today's 24000CE Trade 2
_TSL_BASE_LOCK_PCT = 0.07   # peaked at +21.3% profit, just past the old +20% tier, but
_TSL_STEP_PCT = 0.10        # the lock stayed flat at 12.5% the whole time (next tier
_TSL_STEP_LOCK_PCT = 0.07   # needed +40%) -- gave back ~9 points of a real move purely
                             # from step-size coarseness. Tighter 10%/7% steps let the
                             # floor climb within a single strong move instead of
                             # waiting for the next round-number tier.
_HIST_WARMUP_DAYS = 14   # 2026-07-31 tweak (was 25): current week + previous week
                         # is enough history to seed the 60m bear-trap zone pool.
_TSL_TRANCHE_BASE_PCT = 0.20        # 2026-08-01: per-lot staircase for the flip
_TSL_TRANCHE_BASE_LOCK_PCT = 0.125  # concept's T1/T2 tranches specifically --
_TSL_TRANCHE_STEP_PCT = 0.20        # deliberately the OLDER 20%/12.5% shape, NOT
_TSL_TRANCHE_STEP_LOCK_PCT = 0.125  # the tightened 10%/7% used for regular single-
                                     # shot entries elsewhere in this book. Each
                                     # tranche leg is tracked and exited fully
                                     # independently off its OWN entry price.


@dataclass(frozen=True)
class _Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float


@dataclass
class _OptionSeries:
    """Live 1-min bar accumulator + zone pool for one specific option strike."""
    strike: int
    side: str          # "CE" | "PE"
    bars_1m: List[_Bar] = field(default_factory=list)
    zones: List[dict] = field(default_factory=list)
    flip_candidates: List[dict] = field(default_factory=list)   # 2026-07-31 flip concept
    prev15_high: Optional[float] = None   # 2026-08-01: cached most-recently-CLOSED
    prev15_low: Optional[float] = None    # 15m candle's H/L, refreshed once per bar-
                                           # close in _process_new_bar -- lets the
                                           # tick-level T1 fast-breach check compare
                                           # every tick against a cheap O(1) value
                                           # instead of resampling on every tick.
    _cur_open: Optional[datetime] = None
    _cur_o: float = 0.0
    _cur_h: float = 0.0
    _cur_l: float = 0.0
    _cur_c: float = 0.0
    last_ltp: float = 0.0

    def on_tick(self, ts: datetime, ltp: float) -> bool:
        """Returns True if a new 1-min bar just closed."""
        self.last_ltp = ltp
        bucket = ts.replace(second=0, microsecond=0)
        if self._cur_open is None:
            self._cur_open, self._cur_o, self._cur_h, self._cur_l, self._cur_c = bucket, ltp, ltp, ltp, ltp
            return False
        if bucket == self._cur_open:
            self._cur_h = max(self._cur_h, ltp)
            self._cur_l = min(self._cur_l, ltp)
            self._cur_c = ltp
            return False
        # new minute -> commit
        self.bars_1m.append(_Bar(self._cur_open, self._cur_o, self._cur_h, self._cur_l, self._cur_c))
        self._cur_open, self._cur_o, self._cur_h, self._cur_l, self._cur_c = bucket, ltp, ltp, ltp, ltp
        return True

    def to_df(self) -> pd.DataFrame:
        if not self.bars_1m:
            return pd.DataFrame(columns=["datetime", "open", "high", "low", "close"])
        return pd.DataFrame([
            {"datetime": b.timestamp, "open": b.open, "high": b.high, "low": b.low, "close": b.close}
            for b in self.bars_1m
        ])


def _resample(df_1m: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df_1m.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close"])
    frames = []
    for day, g in df_1m.groupby(df_1m["datetime"].dt.date):
        g = g.set_index("datetime").sort_index()
        origin = pd.Timestamp(f"{day} 09:15:00", tz=IST)
        r = g.resample(f"{minutes}min", origin=origin).agg(
            {"open": "first", "high": "max", "low": "min", "close": "last"}
        ).dropna().reset_index()
        r = r.rename(columns={"datetime": "timestamp"})
        frames.append(r)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _to_bars(df: pd.DataFrame):
    if df.empty:
        return []
    cols = df.rename(columns={"datetime": "timestamp"}) if "timestamp" not in df.columns else df
    return list(cols[["timestamp", "open", "high", "low", "close"]].itertuples(index=False, name="Bar"))


def _detect_bear_zones(bars_60m) -> List[dict]:
    out = []
    for z in find_all_bear_zones(bars_60m):
        lo, hi = min(z.entry_line, z.sweep_low), max(z.entry_line, z.sweep_low)
        out.append(dict(zone_lo=lo, zone_hi=hi, entry_line=z.entry_line, lock_ts=z.lock_ts,
                         state="WAITING", ref_bar=None, done=False, invalid=False,
                         contact_ts=None, ref_open=None, ref_close_time=None,
                         breach_ts=None, sub_lo=None, sub_hi=None))
    return _collapse_nearby_zones(out)


def _collapse_nearby_zones(zones: List[dict], threshold_pts: float = _ZONE_MERGE_THRESHOLD_PTS) -> List[dict]:
    """Merge zones whose bands are within threshold_pts of each other into one,
    taking max(zone_hi)/min(zone_lo) across the group -- same collapse principle
    as the 5-min sub-zone decomposition, applied to the 60-min zone pool itself.
    The merged zone's entry_line/lock_ts come from whichever member zone has the
    MOST RECENT lock_ts (the most current reference level in the group)."""
    if not zones:
        return []
    ordered = sorted(zones, key=lambda z: z["zone_lo"])
    groups = [[ordered[0]]]
    for z in ordered[1:]:
        group_hi = max(g["zone_hi"] for g in groups[-1])
        if z["zone_lo"] <= group_hi + threshold_pts:
            groups[-1].append(z)
        else:
            groups.append([z])
    collapsed = []
    for group in groups:
        newest = max(group, key=lambda g: g["lock_ts"])
        collapsed.append(dict(
            zone_lo=min(g["zone_lo"] for g in group), zone_hi=max(g["zone_hi"] for g in group),
            entry_line=newest["entry_line"], lock_ts=newest["lock_ts"],
            state="WAITING", ref_bar=None, done=False, invalid=False,
            contact_ts=None, ref_open=None, ref_close_time=None,
            breach_ts=None, sub_lo=None, sub_hi=None,
        ))
    return collapsed


def _prevalidate_zones(zones: List[dict], m15: pd.DataFrame) -> List[dict]:
    """2026-07-31: if a 15m candle has ALREADY closed below a zone's own zone_lo
    at any point since it locked, the reclaim that originally formed this zone
    has already failed -- mark invalid immediately rather than let it sit in the
    pool looking tradeable when the underlying has already moved decisively
    past it (confirmed live: PE24600's zone locked at zone_lo=289.35 while PE
    later fell to the 250s with no invalidation catching it)."""
    for z in zones:
        later = m15[(m15["timestamp"] > z["lock_ts"]) & (m15["close"] < z["zone_lo"])]
        if not later.empty:
            z["invalid"] = True
    return zones


def _collapse_subzones(bars_5m_window) -> Optional[tuple]:
    if len(bars_5m_window) < 3:
        return None
    found = find_all_bear_zones(bars_5m_window)
    if not found:
        return None
    los = [min(z.entry_line, z.sweep_low) for z in found]
    his = [max(z.entry_line, z.sweep_low) for z in found]
    return min(los), max(his)


def _arm_level(zone_lo: float, zone_hi: float, threshold_pts: float, direction: str = "LONG") -> float:
    size = zone_hi - zone_lo
    large = size > threshold_pts
    if direction == "LONG":   # approaching from above (bear-trap, main pipeline)
        return zone_hi - size / 3.0 if large else zone_lo + size / 3.0
    else:                     # SHORT -- approaching from below (bull-trap flip check)
        return zone_lo + size / 3.0 if large else zone_hi - size / 3.0


class D1TrapBearOnlyBook(AbstractStrategyBook):
    """
    Per-(client, binding) live book. Always underlying="NIFTY" today (single-
    index strategy; extending to other indices is a config change, not a
    logic change, once this has run live).
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
        itm_offset_pts: int = 200,
        product_type: str = "MIS",
    ) -> None:
        super().__init__(bus, cfg, underlying, client_id, binding_id)
        self._strategy_name = "d1_trap_bear_only"
        self._lot_multiplier = max(1, lot_multiplier)
        self._feeder_token = feeder_token
        self._itm_offset_pts = itm_offset_pts
        self._product_type = product_type
        self._lot_size = (cfg.exchange.lot_sizes.get(underlying, 75) if cfg else 75)
        self._strike_step = int(cfg.exchange.strike_steps.get(underlying, 50) if cfg else 50)

        self._today: Optional[date] = None
        self._ce_strike: Optional[int] = None
        self._pe_strike: Optional[int] = None
        self._series: Dict[str, _OptionSeries] = {}   # "CE" | "PE" -> _OptionSeries
        self._last_spot_open: Optional[float] = None
        # 2026-08-01: list of open legs, not a single position -- the flip
        # concept's T1 (fast tick-level breach) and T2 (confirmed retracement)
        # tranches are independent legs that can both be open on the SAME side
        # at once, each with its own entry/SL/staircase-TSL state. Regular
        # single-shot entries (raw_breakout/swing_breach) still only ever
        # produce one leg. Invariant: every leg in this list shares the same
        # `side` -- the book never holds CE and PE simultaneously.
        self._positions: List[dict] = []
        self._day_done = False
        self._selecting_strikes = False
        self._warming_up = False
        self._rest_open_attempted = False   # 2026-07-31 fix: REST-open must always get
                                             # first attempt before the live-tick fallback
                                             # is allowed to fire -- see _startup_open_fetch.

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        super().start()
        self._subscribe(Topic.INDEX_TICK)
        self._subscribe(Topic.OPTION_TICK)
        self._tasks.append(asyncio.create_task(
            self._index_tick_loop(), name=f"beartrap_idx_{self._underlying}"))
        self._tasks.append(asyncio.create_task(
            self._option_tick_loop(), name=f"beartrap_opt_{self._underlying}"))
        self._tasks.append(asyncio.create_task(
            self._eod_loop(), name=f"beartrap_eod_{self._underlying}"))
        self._tasks.append(asyncio.create_task(
            self._startup_open_fetch(), name=f"beartrap_openfetch_{self._underlying}"))

    async def _startup_open_fetch(self) -> None:
        """Get TODAY's real 09:15 open via REST the moment the book starts, regardless
        of wall-clock time -- fixes 2026-07-30 finding: waiting for "the next live tick"
        as a proxy for "today's open" is wrong whenever the app starts after the open
        already happened (mid-day, after close, or on a restart) -- it picks up
        whatever price is ticking NOW, not the actual 09:15 open. If the market
        genuinely hasn't opened yet (no bars available), this is a no-op and the
        live-tick fallback in _index_tick_loop handles it once the open actually prints."""
        if not self._feeder_token:
            self._rest_open_attempted = True
            return
        today = datetime.now(IST).date()
        if today.weekday() >= 5:   # weekend -- nothing to fetch
            self._rest_open_attempted = True
            return
        try:
            key = _upstox_key_for(self._underlying)
            from data_layer.historical_candles import fetch_upstox_intraday_1m
            rows = await fetch_upstox_intraday_1m(key, self._feeder_token)
            if not rows:
                logger.info("BearTrap[%s]: no intraday bars yet for %s (pre-market) — "
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
                logger.info("BearTrap[%s]: fetched TODAY's real open=%.2f via REST (bar ts=%s) "
                            "-- selecting strikes now regardless of current time.",
                            self._underlying, open_px, rows[0].get("ts"))
                asyncio.create_task(self._select_strikes_for_today(open_px))
        except Exception:
            logger.exception("BearTrap[%s]: startup open-fetch failed — falling back to live tick.",
                              self._underlying)
        finally:
            # ALWAYS mark the REST attempt done, whether it found bars or not -- this is
            # the gate that stops the live-tick path in _index_tick_loop from racing ahead
            # of this REST call and picking up "whatever price is ticking right now"
            # instead of the true 09:15 open. Fixes 2026-07-31 finding: on every mid-day
            # restart, the live tick (already flowing continuously from other strategies)
            # was winning the race against this REST call almost every time, silently.
            self._rest_open_attempted = True

    def reset_session(self) -> None:
        self._today = None
        self._ce_strike = None
        self._pe_strike = None
        self._series = {}
        self._last_spot_open = None
        self._day_done = False
        if not self._positions:
            pass  # nothing open, clean reset

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
            is_nifty = ev.symbol in ("NSE_INDEX|Nifty 50", "NIFTY", "NSE:NIFTY50-INDEX")
            if not is_nifty or not ev.ltp:
                continue
            today = ev.timestamp.date() if hasattr(ev, "timestamp") else datetime.now(IST).date()
            if self._today != today:
                self.reset_session()
                self._today = today
            now_t = datetime.now(IST).time()
            if (self._last_spot_open is None and _STRIKE_SELECT_TIME <= now_t <= _SESSION_CLOSE
                    and not self._selecting_strikes and self._rest_open_attempted):
                # Gated on _rest_open_attempted so this can never race ahead of
                # _startup_open_fetch -- REST always gets first crack at the real
                # 09:15 open; this path only engages once that attempt has finished
                # (found nothing = genuine pre-market cold start) or, mid-day, would
                # already have been satisfied by REST before any tick reaches here.
                self._last_spot_open = ev.ltp
                self._selecting_strikes = True
                asyncio.create_task(self._select_strikes_for_today(ev.ltp))

    async def _select_strikes_for_today(self, spot_open: float) -> None:
        try:
            atm = round(spot_open / _ATM_ROUND_STEP) * _ATM_ROUND_STEP
            ce_strike = int(atm - self._itm_offset_pts)
            pe_strike = int(atm + self._itm_offset_pts)
            logger.info(
                "BearTrap[%s]: spot_open=%.2f ATM=%d -> CE=%d PE=%d",
                self._underlying, spot_open, atm, ce_strike, pe_strike,
            )
            self._ce_strike, self._pe_strike = ce_strike, pe_strike

            today = self._today or datetime.now(IST).date()
            expiry = REGISTRY.get_active_expiry(self._underlying, today)
            if expiry is None or not self._feeder_token:
                logger.warning("BearTrap[%s]: no expiry/token — cannot warm history.", self._underlying)
                self._series["CE"] = _OptionSeries(strike=ce_strike, side="CE")
                self._series["PE"] = _OptionSeries(strike=pe_strike, side="PE")
                return

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
                    m60 = _resample(series.to_df(), 60)
                    m15_hist = _resample(series.to_df(), 15)
                    series.zones = _prevalidate_zones(_detect_bear_zones(_to_bars(m60)), m15_hist)
                    logger.info("BearTrap[%s]: %s %d warmed %d 1m bars -> %d bear zones",
                                self._underlying, side, strike, len(series.bars_1m), len(series.zones))
                    if series.zones:
                        # Summary only (2026-07-31: dialed back from a full per-zone dump --
                        # that was a one-time diagnostic need, not needed every restart, and
                        # heavy synchronous logging was a plausible contributor to reported
                        # UI/dashboard slowness on this same event loop).
                        his = [z["zone_hi"] for z in series.zones]
                        los = [z["zone_lo"] for z in series.zones]
                        logger.info("BearTrap[%s]: %s %d zone_hi range=[%.2f, %.2f]  "
                                    "zone_lo range=[%.2f, %.2f]",
                                    self._underlying, side, strike, min(his), max(his),
                                    min(los), max(los))
                self._series[side] = series

            # Replay TODAY's own intraday bars (if the market has already opened) so
            # zone/stage state catches up to "now" before live ticks arrive -- fixes
            # 2026-07-30 finding: starting mid-day otherwise misses any zone that
            # already reached MONITORING/ARMED/entered+exited earlier today.
            # _warming_up suppresses real order placement (mirrors
            # D1TrapOptionBook._warmup_intraday's _warming_up guard).
            self._warming_up = True
            try:
                from data_layer.historical_candles import fetch_upstox_intraday_1m
                # Fetch BOTH sides' today bars first, then replay INTERLEAVED by
                # timestamp (not one side fully, then the other) -- the flip concept
                # needs CE to see PE's TODAY invalidation state (and vice versa) at
                # the correct point in time; replaying CE's whole day before PE has
                # even started would make CE blind to any flip candidate PE created
                # earlier today, since flip_candidates only exist once that side's
                # own intraday bars have actually been processed.
                today_rows_by_side: Dict[str, list] = {}
                for side, strike in (("CE", ce_strike), ("PE", pe_strike)):
                    if self._series.get(side) is None:
                        continue
                    key = REGISTRY.get_upstox_key(self._underlying, expiry, strike, side)
                    if not key:
                        continue
                    rows = await fetch_upstox_intraday_1m(key, self._feeder_token)
                    parsed = []
                    for r in rows:
                        ts = pd.Timestamp(r["ts"])
                        ts = ts.tz_convert(IST) if ts.tzinfo else ts.tz_localize(IST)
                        parsed.append((ts, r["open"], r["high"], r["low"], r["close"]))
                    today_rows_by_side[side] = parsed

                merged = sorted(
                    ((ts, side, o, h, l, c) for side, rows in today_rows_by_side.items()
                     for ts, o, h, l, c in rows),
                    key=lambda row: row[0],
                )
                replayed_counts = {"CE": 0, "PE": 0}
                for ts, side, o, h, l, c in merged:
                    series = self._series.get(side)
                    if series is None:
                        continue
                    series.bars_1m.append(_Bar(ts, o, h, l, c))
                    series.last_ltp = c
                    self._process_new_bar(side)
                    # T1's fast tick-level breach check normally runs off raw ticks
                    # (_option_tick_loop), which replay doesn't have -- approximate
                    # with the bar's own high so a restart still reconstructs a T1
                    # that would have fired earlier today instead of silently
                    # missing it (would-have-entered logging only during warmup).
                    self._check_fast_flip_tranche1(side, h, ts)
                    replayed_counts[side] += 1
                for side, n in replayed_counts.items():
                    if n:
                        logger.info("BearTrap[%s]: %s replayed %d intraday bars (market already open, "
                                    "interleaved with other side) -- zone/stage state caught up to now.",
                                    self._underlying, side, n)
            finally:
                self._warming_up = False
        except Exception:
            logger.exception("BearTrap[%s]: strike selection failed.", self._underlying)
        finally:
            self._selecting_strikes = False

    # ── live option ticks ────────────────────────────────────────────────────

    async def _option_tick_loop(self) -> None:
        q = self._loop_queues.get(Topic.OPTION_TICK)
        if q is None:
            logger.warning("BearTrap[%s]: OPTION_TICK queue is None -- subscribe() failed at start().",
                            self._underlying)
            return
        # 2026-07-31: dialed back from a per-200-ticks diagnostic (was logging
        # continuously for hours -- confirmed useful once, but heavy synchronous
        # logging on this same asyncio event loop was a plausible contributor to
        # reported UI/dashboard slowness). Keep only a low-frequency idle check.
        _diag_total = 0
        _diag_last_log = datetime.now(IST)
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                now = datetime.now(IST)
                if (now - _diag_last_log).total_seconds() >= 300 and self._ce_strike is not None:
                    logger.info("BearTrap[%s]: no OPTION_TICK in the last 5 min. "
                                "Total seen so far=%d (watching CE=%s PE=%s)",
                                self._underlying, _diag_total, self._ce_strike, self._pe_strike)
                    _diag_last_log = now
                continue
            if not isinstance(ev, OptionTick) or not ev.ltp:
                continue
            _diag_total += 1
            side = self._match_side(ev)
            if side is None:
                continue
            series = self._series.get(side)
            if series is None:
                continue
            ts = getattr(ev, "timestamp", None) or datetime.now(IST)
            closed = series.on_tick(ts, ev.ltp)

            # 2026-08-01: exits + the flip's fast T1 breach check run on EVERY
            # tick (not gated to bar-close) -- T1 exists specifically to catch a
            # hard trending move that never waits for a candle to close. The
            # heavier per-minute pipeline (zone stages, flip candidate create/
            # cancel, T2 confirmation) still only runs once a 1-min bar closes.
            self._check_exit(side, ev.ltp, ts)
            self._check_fast_flip_tranche1(side, ev.ltp, ts)
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

    # ── per-bar fractal stage processing ────────────────────────────────────

    def _process_new_bar(self, side: str) -> None:
        if self._day_done or datetime.now(IST).time() >= _ENTRY_CUTOFF:
            return
        series = self._series[side]
        df_1m = series.to_df()
        if len(df_1m) < 30:
            return

        m60 = _resample(df_1m, 60)
        m15 = _resample(df_1m, 15)
        m5 = _resample(df_1m, 5)
        bars_60 = _to_bars(m60)
        # MERGE, don't replace: a fresh detect_bear_zones() returns brand-new WAITING
        # zone dicts every call. Replacing series.zones wholesale (as this did before
        # 2026-07-30) would wipe every zone's accumulated stage progress -- MONITORING,
        # ref-candle, breach_ts, sub-zone, armed -- on every single bar close, meaning
        # no zone could ever survive past Stage 1. Only append genuinely new zones.
        existing_lock_ts = {z["lock_ts"] for z in series.zones}
        new_zones = [z for z in _detect_bear_zones(bars_60) if z["lock_ts"] not in existing_lock_ts]
        new_zones = _prevalidate_zones(new_zones, m15)
        series.zones.extend(new_zones)

        known_from_cutoff = datetime.now(IST) - timedelta(days=_MAX_ZONE_AGE_DAYS)
        last_bar = df_1m.iloc[-1]
        last_ts = last_bar["datetime"]
        last_low, last_high = last_bar["low"], last_bar["high"]

        # ONGOING invalidation (2026-07-31): a zone already in the pool (WAITING or
        # MONITORING) whose zone_lo gets closed below by a 15m candle AFTER it was
        # added -- the reclaim has failed since we started watching it. Check every
        # call against the latest closed 15m candle.
        latest15 = self._find_latest_closed_ref_bar(m15, last_ts)
        if latest15 is not None:
            series.prev15_high, series.prev15_low = latest15.high, latest15.low
            for z in series.zones:
                if not z["done"] and not z["invalid"] and latest15.close < z["zone_lo"]:
                    z["invalid"] = True
                    z["invalid_ts"] = latest15.timestamp + timedelta(minutes=15)
                    logger.info("BearTrap[%s]: %s zone [%.2f,%.2f] INVALIDATED -- 15m close %.2f "
                                "< zone_lo %.2f @ %s",
                                self._underlying, side, z["zone_lo"], z["zone_hi"],
                                latest15.close, z["zone_lo"], latest15.timestamp)
                    self._create_flip_candidate(side, z, latest15)
            self._process_flip_cancellation(side, latest15)
            self._process_flip_entry(side, latest15, m15, m5)   # T2 (confirmed) tranche

        # Regular zone-stage pipeline (raw_breakout/swing_breach) only runs
        # while the book is completely flat -- unchanged from before. The flip
        # concept's T1/T2 tranches are handled separately above/in
        # _check_fast_flip_tranche1 and are NOT gated by this, so they can
        # still fire (or add a second lot) even once one tranche leg is open.
        if self._positions:
            return

        for zone in series.zones:
            if zone["done"] or zone["invalid"] or zone["lock_ts"] < known_from_cutoff:
                continue

            # Stage 1: contact
            if zone["state"] == "WAITING":
                if last_low <= zone["zone_hi"]:
                    zone["state"] = "MONITORING"
                    zone["contact_ts"] = last_ts
                    logger.info("BearTrap[%s]: %s zone [%.2f,%.2f] -> MONITORING @ %s",
                                self._underlying, side, zone["zone_lo"], zone["zone_hi"], last_ts)
                continue

            if zone["state"] != "MONITORING":
                continue

            # Stage 2: 15m ref-candle assignment + tick-wise breach
            if zone["ref_open"] is None:
                if last_ts.time() < _EARLY_SESSION_CUTOFF:
                    continue   # too early -- wait for a more settled candle, see 2026-07-30 note
                ref = self._find_latest_closed_ref_bar(m15, last_ts)
                if ref is not None:
                    zone["ref_open"] = ref.timestamp
                    zone["ref_close_time"] = ref.timestamp + timedelta(minutes=15)
                    zone["ref_high"], zone["ref_low"] = ref.high, ref.low
                    logger.info("BearTrap[%s]: %s ref candle ASSIGNED %s (H=%.2f L=%.2f) @ processing_ts=%s",
                                self._underlying, side, ref.timestamp, ref.high, ref.low, last_ts)
                continue

            if zone["breach_ts"] is None:
                if last_ts >= zone["ref_close_time"] and last_high >= zone["ref_high"]:
                    zone["breach_ts"] = last_ts
                    logger.info("BearTrap[%s]: %s ref-candle breach @ %s (high=%.2f)",
                                self._underlying, side, last_ts, zone["ref_high"])
                    continue
                # ROLL FORWARD (2026-07-31 fix, matches D1TrapOptionBook's existing
                # roll-ref-forward-on-no-trigger mechanic): if a NEWER 15m candle has
                # fully closed since the current ref without breaching it, that candle
                # becomes the new ref -- otherwise a zone gets permanently stuck on
                # whatever candle it first attached to, even if that candle's high was
                # an outlier never matched by later, more relevant price action.
                new_ref = self._find_latest_closed_ref_bar(m15, last_ts)
                if new_ref is not None and new_ref.timestamp > zone["ref_open"]:
                    logger.info("BearTrap[%s]: %s ref candle rolled forward %s -> %s "
                                "(old H=%.2f -> new H=%.2f, no breach yet)",
                                self._underlying, side, zone["ref_open"], new_ref.timestamp,
                                zone["ref_high"], new_ref.high)
                    zone["ref_open"] = new_ref.timestamp
                    zone["ref_close_time"] = new_ref.timestamp + timedelta(minutes=15)
                    zone["ref_high"], zone["ref_low"] = new_ref.high, new_ref.low
                continue

            # Stage 3: 5m sub-zone decomposition (once, right after breach)
            if zone["sub_lo"] is None:
                window_5m = m5[(m5["timestamp"] >= zone["ref_open"]) &
                                (m5["timestamp"] < zone["ref_close_time"])]
                collapse = _collapse_subzones(_to_bars(window_5m))
                if collapse is None:
                    # raw_breakout fallback -- enter immediately at ref_high
                    self._enter_leg(side, tranche="single", entry_price=zone["ref_high"], sl=zone["ref_low"],
                                     zone_lock_ts=zone["lock_ts"], order_reason="bear_trap_raw_breakout")
                    zone["done"] = True
                    return
                zone["sub_lo"], zone["sub_hi"] = collapse
                threshold_pts = _ZONE_SIZE_THRESHOLD_PCT / 100.0 * zone["ref_high"]
                zone["arm_level"] = _arm_level(zone["sub_lo"], zone["sub_hi"], threshold_pts)
                zone["armed"] = False
                continue

            # Stage 4: arm (retracement into sub-zone)
            if not zone.get("armed"):
                if last_low <= zone["arm_level"] and last_low >= zone["sub_lo"]:
                    zone["armed"] = True
                    logger.info("BearTrap[%s]: %s ARMED @ %s (level=%.2f)",
                                self._underlying, side, last_ts, zone["arm_level"])
                continue

            # Stage 5: swing breach = entry
            if last_high >= zone["sub_hi"]:
                self._enter_leg(side, tranche="single", entry_price=zone["sub_hi"], sl=zone["ref_low"],
                                 zone_lock_ts=zone["lock_ts"], order_reason="bear_trap_swing_breach")
                zone["done"] = True
                return

    # ── flip concept (2026-08-01, final corrected design, validated via a
    # 1-month backtest against real Aug-4-expiry option data before going
    # live -- scripts/d1trap_month_backtest_v2.py) ──────────────────────────
    # When a zone on one side (say PE) gets invalidated (15m close below its
    # zone_lo), candle A = that invalidating 15m candle -- FIXED forever,
    # never rolls forward. On each subsequent 15m close on PE's own chart:
    #   Check 1 (evaluated first): new low below candle A's low? -> no
    #       cancellation, PE stays invalid, flip candidate stays active.
    #   Check 2 (only if Check 1 is false): closed back inside PE's own
    #       zone band [zone_lo, zone_hi]? -> cancel the flip candidate AND
    #       re-validate PE's zone (people came back to defend it).
    # Independently, on CE's own chart: CE's own 60-min zone requirement is
    # SKIPPED entirely while a PE flip candidate is active -- CE watches only
    # for its current 15m candle to break the PREVIOUS 15m candle's high,
    # with a 5-min subzone found inside that same breakout candle (CE's own
    # chart, normal bear-trap _collapse_subzones -- not the bull variant).
    # That directly triggers an entry: price=breakout high, SL=breakout low.
    # Same logic applies symmetrically PE-flipped-by-CE.

    def _create_flip_candidate(self, side: str, zone: dict, candleA) -> None:
        series = self._series.get(side)
        if series is None:
            return
        series.flip_candidates.append(dict(
            candleA_low=candleA.low, candleA_high=candleA.high, candleA_ts=candleA.timestamp,
            zone_lo=zone["zone_lo"], zone_hi=zone["zone_hi"], zone_lock_ts=zone["lock_ts"],
            parent_zone=zone, confirmed=False, cancelled=False, t1_taken=False,
        ))
        logger.info("BearTrap[%s]: %s FLIP CANDIDATE created -- candle A @ %s low=%.2f, zone=[%.2f,%.2f]. "
                    "Other side may fast-track entry (skipping its own 60m zone) if it breaks its own "
                    "prev-15m high with a 5m subzone.",
                    self._underlying, side, candleA.timestamp, candleA.low, zone["zone_lo"], zone["zone_hi"])

    def _process_flip_cancellation(self, side: str, m15_bar) -> None:
        """Candle A is fixed and never rolls forward. Check 1 (new low below
        candle A) is evaluated BEFORE Check 2 (close back inside the zone) --
        Check 2 only applies when Check 1 is false."""
        series = self._series.get(side)
        if series is None:
            return
        for fc in series.flip_candidates:
            if fc["confirmed"] or fc["cancelled"]:
                continue
            if m15_bar.timestamp <= fc["candleA_ts"]:
                continue
            if m15_bar.low < fc["candleA_low"]:
                continue   # reconfirms breakdown -- no cancellation, flip stays active
            if fc["zone_lo"] <= m15_bar.close <= fc["zone_hi"]:
                fc["cancelled"] = True
                fc["parent_zone"]["invalid"] = False
                logger.info("BearTrap[%s]: %s FLIP CANDIDATE cancelled -- 15m closed back inside "
                            "zone @ %.2f, zone [%.2f,%.2f] RE-VALIDATED.",
                            self._underlying, side, m15_bar.close, fc["zone_lo"], fc["zone_hi"])

    def _process_flip_entry(self, side: str, m15_bar, m15: pd.DataFrame, m5: pd.DataFrame) -> None:
        """T2 (confirmed) tranche -- side possibly being fast-tracked by the
        OTHER side's invalidation. Skips side's own 60m zone requirement
        entirely -- simplified trigger only: current 15m high > previous 15m
        high (own chart) + 5m subzone found in that breakout candle (own
        chart). 2026-08-01: fires independently of T1 (the fast tick-level
        tranche) -- if T1 is still open this ADDS a second lot on top of it;
        if T1 was never taken or already stopped out, this becomes a normal
        standalone confirmed entry (same mechanics as before this tranche
        split existed)."""
        if self._positions and self._positions[0]["side"] != side:
            return   # book already committed to the OTHER side
        if any(p["side"] == side and p["tranche"] == "T2" for p in self._positions):
            return   # T2 already taken for this side
        flip_source_side = "PE" if side == "CE" else "CE"
        flip_source = self._series.get(flip_source_side)
        if flip_source is None or not flip_source.flip_candidates:
            return
        idx = m15.index[m15["timestamp"] == m15_bar.timestamp]
        if not len(idx) or idx[0] == 0:
            return
        prev15 = m15.iloc[idx[0] - 1]
        if m15_bar.high <= prev15["high"]:
            return
        for fc in flip_source.flip_candidates:
            if fc["confirmed"] or fc["cancelled"]:
                continue
            if m15_bar.timestamp <= fc["candleA_ts"]:
                continue   # timestamps only -- a shared timeline, valid to compare
            # 2026-08-01 fix: candleA_low/zone_lo/zone_hi live on the FLIP-SOURCE
            # side's (e.g. PE's) own premium scale, but m15_bar here is THIS
            # side's (e.g. CE's) own bar -- comparing CE's price against PE's
            # reference levels is a cross-instrument scale mismatch (those
            # Check-1/Check-2 price comparisons belong only in
            # _process_flip_cancellation, which correctly uses the flip
            # SOURCE's own bar). This side's entry gating is price-scale-free:
            # only its own prev-15m-high breakout + its own 5m subzone.
            window_5m = m5[(m5["timestamp"] >= m15_bar.timestamp) &
                            (m5["timestamp"] < m15_bar.timestamp + timedelta(minutes=15))]
            collapse = _collapse_subzones(_to_bars(window_5m))
            if collapse is None:
                continue
            fc["confirmed"] = True
            has_t1 = any(p["side"] == side and p["tranche"] == "T1" for p in self._positions)
            logger.info("BearTrap[%s]: %s FLIP T2 ENTRY (%s) -- breakout 15m %s high=%.2f > prev high=%.2f, "
                        "5m subzone [%.2f,%.2f] found. Triggered by %s candle-A @ %s (zone locked %s).",
                        self._underlying, side, "adding to open T1" if has_t1 else "standalone, T1 not open",
                        m15_bar.timestamp, m15_bar.high, prev15["high"],
                        collapse[0], collapse[1], flip_source_side, fc["candleA_ts"], fc["zone_lock_ts"])
            self._enter_leg(side, tranche="T2", entry_price=m15_bar.high, sl=m15_bar.low,
                             zone_lock_ts=fc["zone_lock_ts"], order_reason="bear_trap_flip_t2",
                             use_tranche_tsl=True)
            return

    def _check_fast_flip_tranche1(self, side: str, ltp: float, ts: datetime) -> None:
        """T1 (2026-08-01): the flip's fast, unconfirmed tranche -- checked on
        EVERY tick (not gated to a 15m close) so a hard trending move that
        never retraces into a clean 5m subzone still gets caught, instead of
        only ever depending on T2's slower, candle-close-gated confirmation.
        SL = the prev-15m reference candle's own low -- the same structural
        level the entry trigger (breaking that candle's high) is measured
        against -- buffered/capped exactly like every other entry in this book."""
        if self._day_done or ts.time() >= _ENTRY_CUTOFF:
            return
        if self._positions and self._positions[0]["side"] != side:
            return
        if any(p["side"] == side for p in self._positions):
            return   # this side already has a leg open (T1 and/or T2) -- no re-firing T1
        series = self._series.get(side)
        if series is None or series.prev15_high is None:
            return
        flip_source_side = "PE" if side == "CE" else "CE"
        flip_source = self._series.get(flip_source_side)
        if flip_source is None:
            return
        for fc in flip_source.flip_candidates:
            if fc["confirmed"] or fc["cancelled"] or fc.get("t1_taken"):
                continue
            if ts <= fc["candleA_ts"] + timedelta(minutes=15):
                continue   # candle A itself must have fully closed already
            if ltp <= series.prev15_high:
                continue
            fc["t1_taken"] = True
            logger.info("BearTrap[%s]: %s FLIP T1 ENTRY (fast) -- tick ltp=%.2f > prev15_high=%.2f. "
                        "Triggered by %s candle-A @ %s (zone locked %s).",
                        self._underlying, side, ltp, series.prev15_high,
                        flip_source_side, fc["candleA_ts"], fc["zone_lock_ts"])
            self._enter_leg(side, tranche="T1", entry_price=ltp, sl=series.prev15_low,
                             zone_lock_ts=fc["zone_lock_ts"], order_reason="bear_trap_flip_t1",
                             use_tranche_tsl=True)
            return

    @staticmethod
    def _find_ref_bar(anchor_ts, m15: pd.DataFrame):
        """Used for REPLAY/BACKTEST-style lookups where anchor_ts is a point in the
        past relative to data that already fully exists -- returns the bucket
        containing anchor_ts, or the first one starting after it."""
        for row in m15.itertuples(index=False):
            bar_open = row.timestamp
            bar_close = bar_open + timedelta(minutes=15)
            if bar_open <= anchor_ts < bar_close or bar_open >= anchor_ts:
                return row
        return None

    @staticmethod
    def _find_latest_closed_ref_bar(m15: pd.DataFrame, last_ts):
        """2026-07-31 fix: for LIVE ref-candle assignment, "now" (last_ts) always
        falls inside the currently-forming, not-yet-closed bucket -- _find_ref_bar
        would keep returning that same open bucket forever, which then always fails
        the "must be closed" check downstream. This instead finds the MOST RECENTLY
        CLOSED same-day 15-min bucket at/after the early-session cutoff -- the
        correct notion of "ref candle" when running live, not against historical
        data that already has a known future."""
        candidates = [
            row for row in m15.itertuples(index=False)
            if row.timestamp.date() == last_ts.date()
            and row.timestamp.time() >= _EARLY_SESSION_CUTOFF
            and (row.timestamp + timedelta(minutes=15)) <= last_ts
        ]
        return candidates[-1] if candidates else None

    # ── entry / exit ─────────────────────────────────────────────────────────

    def _enter_leg(self, side: str, tranche: str, entry_price: float, sl: float,
                    zone_lock_ts, order_reason: str, use_tranche_tsl: bool = False) -> None:
        """tranche: 'single' (regular raw_breakout/swing_breach, one-shot, never
        coexists with anything else) | 'T1' (flip fast/unconfirmed) | 'T2' (flip
        confirmed retracement, may stack on top of an open T1 on the same side)."""
        if self._warming_up:
            # Replaying today's already-elapsed bars at startup -- this zone's
            # opportunity already came and went before we were watching live.
            # Skip the real order; the caller still marks the zone done so it
            # won't re-fire once live ticks resume.
            logger.info("BearTrap[%s]: %s would have ENTERED [%s] @ %.2f during replay "
                        "(warmup, no order placed) — already resolved earlier today.",
                        self._underlying, side, tranche, entry_price)
            return
        if tranche == "single" and self._positions:
            logger.info("BearTrap[%s]: skip %s single entry -- book already has open leg(s).",
                        self._underlying, side)
            return
        if self._positions and self._positions[0]["side"] != side:
            logger.warning("BearTrap[%s]: refusing %s [%s] entry -- book already holding %s leg(s).",
                           self._underlying, side, tranche, self._positions[0]["side"])
            return

        sl_buffered = sl - _SL_BUFFER_PTS
        max_risk_pts = _MAX_RISK_RS_PER_LOT / self._lot_size
        sl_final = max(sl_buffered, entry_price - max_risk_pts)
        qty = self._lot_size * self._lot_multiplier

        if use_tranche_tsl:
            base_pct, base_lock = _TSL_TRANCHE_BASE_PCT, _TSL_TRANCHE_BASE_LOCK_PCT
            step_pct, step_lock = _TSL_TRANCHE_STEP_PCT, _TSL_TRANCHE_STEP_LOCK_PCT
        else:
            base_pct, base_lock = _TSL_BASE_PCT, _TSL_BASE_LOCK_PCT
            step_pct, step_lock = _TSL_STEP_PCT, _TSL_STEP_LOCK_PCT

        pos = dict(
            side=side, strike=self._ce_strike if side == "CE" else self._pe_strike,
            entry_price=entry_price, sl=sl_final, entry_ts=datetime.now(IST),
            high_lock_pct=0.0, qty=qty, zone_lock_ts=zone_lock_ts, tranche=tranche,
            tsl_base_pct=base_pct, tsl_base_lock_pct=base_lock,
            tsl_step_pct=step_pct, tsl_step_lock_pct=step_lock,
        )
        self._positions.append(pos)
        logger.info("BearTrap[%s]: ENTER BUY %s %d [%s] entry=%.2f sl=%.2f (risk=Rs%.0f/lot) reason=%s",
                    self._underlying, side, pos["strike"], tranche, entry_price, sl_final,
                    (entry_price - sl_final) * self._lot_size, order_reason)

        expiry = REGISTRY.get_active_expiry(self._underlying, self._today or datetime.now(IST).date())
        ev = D1TrapOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id,
            strategy="d1_trap_bear_only", direction="LONG", action="BUY",
            quantity=qty, entry_price=entry_price, sl_price=sl_final, tsl_level=sl_final,
            trigger_ts=datetime.now(IST), reason=order_reason,
            underlying=self._underlying, option_type=side,
            strike=pos["strike"], expiry=expiry,
            product_type=self._product_type,
        )
        if self._bus is not None:
            asyncio.create_task(self._bus.publish(Topic.D1_TRAP_ORDER_REQUEST, ev))

    def _check_exit(self, side: str, ltp: float, ts: datetime) -> None:
        """Each leg (single/T1/T2) is checked and exited fully independently --
        no averaging, no combined exit. A leg's own staircase-TSL profile
        (tranche legs use 20%/12.5%, regular entries use 10%/7%) is stored on
        the leg itself at entry time."""
        now_t = ts.time() if hasattr(ts, "time") else datetime.now(IST).time()
        for pos in list(self._positions):
            if pos["side"] != side:
                continue
            entry = pos["entry_price"]
            profit_pct = (ltp - entry) / entry

            if profit_pct >= pos["tsl_base_pct"]:
                num_steps = int((profit_pct - pos["tsl_base_pct"]) // pos["tsl_step_pct"])
                calc_lock = pos["tsl_base_lock_pct"] + num_steps * pos["tsl_step_lock_pct"]
                pos["high_lock_pct"] = max(pos["high_lock_pct"], calc_lock)

            stop_price = entry * (1 + pos["high_lock_pct"]) if pos["high_lock_pct"] > 0 else pos["sl"]

            if ltp <= stop_price:
                reason = "tsl_hit" if pos["high_lock_pct"] > 0 else "sl_hit"
                asyncio.create_task(self._square_off_leg(pos, reason, stop_price))
                continue

            if now_t >= _EOD_TIME:
                asyncio.create_task(self._square_off_leg(pos, "eod", ltp))

    async def _eod_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            now = datetime.now(IST)
            if now.time() >= _EOD_TIME and not self._day_done:
                for pos in list(self._positions):
                    series = self._series.get(pos["side"])
                    ltp = series.last_ltp if series else pos["entry_price"]
                    await self._square_off_leg(pos, "eod", ltp)
                self._day_done = True

    async def _square_off_leg(self, pos: dict, reason: str, exit_price: float) -> None:
        if not any(p is pos for p in self._positions):
            return   # already closed by a concurrent check (e.g. tsl + eod racing)
        self._positions = [p for p in self._positions if p is not pos]
        expiry = REGISTRY.get_active_expiry(self._underlying, self._today or datetime.now(IST).date())
        ev = D1TrapOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id,
            strategy="d1_trap_bear_only", direction="LONG", action="SELL",
            quantity=pos["qty"], entry_price=pos["entry_price"], sl_price=pos["sl"],
            tsl_level=pos["sl"], trigger_ts=datetime.now(IST), reason=reason,
            underlying=self._underlying, option_type=pos["side"], strike=pos["strike"],
            expiry=expiry, product_type=self._product_type,
        )
        if self._bus is not None:
            await self._bus.publish(Topic.D1_TRAP_ORDER_REQUEST, ev)
        logger.info("BearTrap[%s]: SELL %s %d [%s] reason=%s exit=%.2f",
                    self._underlying, pos["side"], pos["strike"], pos.get("tranche", "single"),
                    reason, exit_price)

    async def liquidate(self, reason: str = "kill_switch") -> None:
        for pos in list(self._positions):
            series = self._series.get(pos["side"])
            ltp = series.last_ltp if series else pos["entry_price"]
            await self._square_off_leg(pos, reason, ltp)

    # ── status / UI ──────────────────────────────────────────────────────────

    def _leg_view(self, pos: dict) -> dict:
        series = self._series.get(pos["side"])
        return dict(
            side=pos["side"], strike=pos["strike"], entry=pos["entry_price"], sl=pos["sl"],
            locked_pct=round(pos["high_lock_pct"] * 100, 1), qty=pos["qty"],
            tranche=pos.get("tranche", "single"),
            ltp=series.last_ltp if series else None,
        )

    def status(self) -> dict:
        legs = [self._leg_view(p) for p in self._positions]
        return dict(
            strategy="d1_trap_bear_only", underlying=self._underlying,
            ce_strike=self._ce_strike, pe_strike=self._pe_strike,
            spot_open=self._last_spot_open,
            selection_reason=(f"ATM={round((self._last_spot_open or 0)/_ATM_ROUND_STEP)*_ATM_ROUND_STEP} "
                               f"(spot_open={self._last_spot_open}) -> CE=ATM-{self._itm_offset_pts}, "
                               f"PE=ATM+{self._itm_offset_pts}") if self._last_spot_open else None,
            position=legs[0] if legs else None,   # backward-compat: first open leg (or None)
            positions=legs,                        # full list -- may hold both T1 and T2
        )

    def monitoring_zones(self) -> dict:
        def _zone_view(side: str) -> dict:
            series = self._series.get(side)
            if series is None:
                return dict(strike=None, zones=[], stage="NO_DATA")
            active = [z for z in series.zones if not z["done"] and not z["invalid"]]
            monitoring = [z for z in active if z["state"] == "MONITORING"]
            waiting = [z for z in active if z["state"] == "WAITING"]
            stage = "NO_ZONES"
            top = None
            if monitoring:
                z = monitoring[0]
                top = z
                if z.get("armed"):
                    stage = "ARMED_5M"
                elif z["sub_lo"] is not None:
                    stage = "5M_TRACKING"
                elif z["breach_ts"] is not None:
                    stage = "15M_BREACHED"
                elif z["ref_open"] is not None:
                    stage = "15M_TRACKING"
                else:
                    stage = "ZONE_ENTERED"
            elif waiting:
                # 2026-07-31 fix: a WAITING zone (real zone, not yet touched) was
                # previously invisible in the UI -- showed as blank "IDLE" with no
                # bounds at all, indistinguishable from "no zones exist". Surface the
                # nearest one (closest zone_hi to current LTP) so it's visible.
                ltp = series.last_ltp or 0
                top = min(waiting, key=lambda z: abs(z["zone_hi"] - ltp)) if ltp else waiting[0]
                stage = "WAITING"

            # 2026-08-01 fix: once a side's only zone(s) invalidate, `active` goes
            # empty and this fell through to blank "NO_ZONES" (shown as "idle" in
            # the dashboard) even though the flip concept keeps real, live state
            # going for that side -- watching for either a cancellation (close back
            # inside its own zone, re-validating it) or feeding the other side's
            # fast-tracked entry. Surface the most recently created live (not yet
            # confirmed/cancelled) flip candidate so the UI reflects what's actually
            # being tracked instead of looking dead.
            live_flips = [fc for fc in series.flip_candidates if not fc["confirmed"] and not fc["cancelled"]]
            flip_view = None
            if live_flips:
                fc = max(live_flips, key=lambda f: f["candleA_ts"])
                flip_view = dict(
                    candleA_ts=fc["candleA_ts"].isoformat(), candleA_low=round(fc["candleA_low"], 2),
                    zone_lo=round(fc["zone_lo"], 2), zone_hi=round(fc["zone_hi"], 2),
                )
                if not active:
                    stage = "INVALID_FLIP_WATCH"

            return dict(
                strike=series.strike, zones_total=len(active), stage=stage,
                current_zone=dict(zone_lo=round(top["zone_lo"], 2), zone_hi=round(top["zone_hi"], 2),
                                   entry_line=round(top["entry_line"], 2)) if top else None,
                flip=flip_view,
                last_ltp=series.last_ltp,
            )

        st = self.status()
        return dict(
            underlying=self._underlying, client_id=self._client_id, binding_id=self._binding_id,
            ce=_zone_view("CE"), pe=_zone_view("PE"),
            position=st["position"],
            positions=st["positions"],
        )

    async def _option_loop(self) -> None:
        pass
