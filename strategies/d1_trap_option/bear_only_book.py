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

Status: NEW as of 2026-07-30, built and wired but NOT yet run against a live
feed. Verify once in `--mode demo` before trusting live broker orders.
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
from strategies.d1_trap_option.book import D1TrapOrderEvent

logger = logging.getLogger(__name__)

_SESSION_OPEN = time(9, 15)
_STRIKE_SELECT_TIME = time(9, 16)
_ENTRY_CUTOFF = time(14, 30)
_EOD_TIME = time(15, 15)
_STRIKE_STEP = 50
_MAX_ZONE_AGE_DAYS = 20
_ZONE_SIZE_THRESHOLD_PCT = 0.20
_SL_BUFFER_PTS = 20.0
_MAX_RISK_RS_PER_LOT = 2000.0
_TSL_BASE_PCT = 0.20
_TSL_BASE_LOCK_PCT = 0.125
_TSL_STEP_PCT = 0.20
_TSL_STEP_LOCK_PCT = 0.125
_HIST_WARMUP_DAYS = 25


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
    return out


def _collapse_subzones(bars_5m_window) -> Optional[tuple]:
    if len(bars_5m_window) < 3:
        return None
    found = find_all_bear_zones(bars_5m_window)
    if not found:
        return None
    los = [min(z.entry_line, z.sweep_low) for z in found]
    his = [max(z.entry_line, z.sweep_low) for z in found]
    return min(los), max(his)


def _arm_level(zone_lo: float, zone_hi: float, threshold_pts: float) -> float:
    size = zone_hi - zone_lo
    large = size > threshold_pts
    return zone_hi - size / 3.0 if large else zone_lo + size / 3.0


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
        self._position: Optional[dict] = None
        self._day_done = False
        self._selecting_strikes = False

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

    def reset_session(self) -> None:
        self._today = None
        self._ce_strike = None
        self._pe_strike = None
        self._series = {}
        self._last_spot_open = None
        self._day_done = False
        if self._position is None:
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
            if (self._last_spot_open is None and now_t >= _STRIKE_SELECT_TIME
                    and not self._selecting_strikes):
                self._last_spot_open = ev.ltp
                self._selecting_strikes = True
                asyncio.create_task(self._select_strikes_for_today(ev.ltp))

    async def _select_strikes_for_today(self, spot_open: float) -> None:
        try:
            atm = round(spot_open / self._strike_step) * self._strike_step
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
                    series.zones = _detect_bear_zones(_to_bars(m60))
                    logger.info("BearTrap[%s]: %s %d warmed %d 1m bars -> %d bear zones",
                                self._underlying, side, strike, len(series.bars_1m), len(series.zones))
                self._series[side] = series
        except Exception:
            logger.exception("BearTrap[%s]: strike selection failed.", self._underlying)
        finally:
            self._selecting_strikes = False

    # ── live option ticks ────────────────────────────────────────────────────

    async def _option_tick_loop(self) -> None:
        q = self._loop_queues.get(Topic.OPTION_TICK)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if not isinstance(ev, OptionTick) or not ev.ltp:
                continue
            side = self._match_side(ev)
            if side is None:
                continue
            series = self._series.get(side)
            if series is None:
                continue
            ts = getattr(ev, "timestamp", None) or datetime.now(IST)
            closed = series.on_tick(ts, ev.ltp)

            if self._position is not None and self._position["side"] == side:
                self._check_exit(side, ev.ltp, ts)
            elif closed and self._position is None:
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
        series.zones = _detect_bear_zones(bars_60)

        known_from_cutoff = datetime.now(IST) - timedelta(days=_MAX_ZONE_AGE_DAYS)
        last_bar = df_1m.iloc[-1]
        last_ts = last_bar["datetime"]
        last_low, last_high = last_bar["low"], last_bar["high"]

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
                ref = self._find_ref_bar(last_ts, m15)
                if ref is not None and (ref.timestamp + timedelta(minutes=15)) <= last_ts:
                    zone["ref_open"] = ref.timestamp
                    zone["ref_close_time"] = ref.timestamp + timedelta(minutes=15)
                    zone["ref_high"], zone["ref_low"] = ref.high, ref.low
                continue

            if zone["breach_ts"] is None:
                if last_ts >= zone["ref_close_time"] and last_high >= zone["ref_high"]:
                    zone["breach_ts"] = last_ts
                    logger.info("BearTrap[%s]: %s ref-candle breach @ %s (high=%.2f)",
                                self._underlying, side, last_ts, zone["ref_high"])
                continue

            # Stage 3: 5m sub-zone decomposition (once, right after breach)
            if zone["sub_lo"] is None:
                window_5m = m5[(m5["timestamp"] >= zone["ref_open"]) &
                                (m5["timestamp"] < zone["ref_close_time"])]
                collapse = _collapse_subzones(_to_bars(window_5m))
                if collapse is None:
                    # raw_breakout fallback -- enter immediately at ref_high
                    self._enter(side, zone, entry_price=zone["ref_high"], sl=zone["ref_low"])
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
                self._enter(side, zone, entry_price=zone["sub_hi"], sl=zone["ref_low"])
                zone["done"] = True
                return

    @staticmethod
    def _find_ref_bar(anchor_ts, m15: pd.DataFrame):
        for row in m15.itertuples(index=False):
            bar_open = row.timestamp
            bar_close = bar_open + timedelta(minutes=15)
            if bar_open <= anchor_ts < bar_close or bar_open >= anchor_ts:
                return row
        return None

    # ── entry / exit ─────────────────────────────────────────────────────────

    def _enter(self, side: str, zone: dict, entry_price: float, sl: float) -> None:
        sl_buffered = sl - _SL_BUFFER_PTS
        max_risk_pts = _MAX_RISK_RS_PER_LOT / self._lot_size
        sl_final = max(sl_buffered, entry_price - max_risk_pts)

        qty = self._lot_size * self._lot_multiplier
        self._position = dict(
            side=side, strike=self._ce_strike if side == "CE" else self._pe_strike,
            entry_price=entry_price, sl=sl_final, entry_ts=datetime.now(IST),
            high_lock_pct=0.0, qty=qty, zone_lock_ts=zone["lock_ts"],
        )
        logger.info("BearTrap[%s]: ENTER BUY %s %d entry=%.2f sl=%.2f (risk=Rs%.0f/lot)",
                    self._underlying, side, self._position["strike"], entry_price, sl_final,
                    (entry_price - sl_final) * self._lot_size)

        expiry = REGISTRY.get_active_expiry(self._underlying, self._today or datetime.now(IST).date())
        ev = D1TrapOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id,
            strategy="d1_trap_bear_only", direction="LONG", action="BUY",
            quantity=qty, entry_price=entry_price, sl_price=sl_final, tsl_level=sl_final,
            trigger_ts=datetime.now(IST), reason="bear_trap_swing_breach",
            underlying=self._underlying, option_type=side,
            strike=self._position["strike"], expiry=expiry,
            product_type=self._product_type,
        )
        if self._bus is not None:
            asyncio.create_task(self._bus.publish(Topic.D1_TRAP_ORDER_REQUEST, ev))

    def _check_exit(self, side: str, ltp: float, ts: datetime) -> None:
        pos = self._position
        if pos is None or pos["side"] != side:
            return
        entry = pos["entry_price"]
        profit_pct = (ltp - entry) / entry

        if profit_pct >= _TSL_BASE_PCT:
            num_steps = int((profit_pct - _TSL_BASE_PCT) // _TSL_STEP_PCT)
            calc_lock = _TSL_BASE_LOCK_PCT + num_steps * _TSL_STEP_LOCK_PCT
            pos["high_lock_pct"] = max(pos["high_lock_pct"], calc_lock)

        stop_price = entry * (1 + pos["high_lock_pct"]) if pos["high_lock_pct"] > 0 else pos["sl"]

        if ltp <= stop_price:
            reason = "tsl_hit" if pos["high_lock_pct"] > 0 else "sl_hit"
            asyncio.create_task(self._square_off(reason, stop_price))
            return

        now_t = ts.time() if hasattr(ts, "time") else datetime.now(IST).time()
        if now_t >= _EOD_TIME:
            asyncio.create_task(self._square_off("eod", ltp))

    async def _eod_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            now = datetime.now(IST)
            if now.time() >= _EOD_TIME and not self._day_done:
                if self._position is not None:
                    series = self._series.get(self._position["side"])
                    ltp = series.last_ltp if series else self._position["entry_price"]
                    await self._square_off("eod", ltp)
                self._day_done = True

    async def _square_off(self, reason: str, exit_price: float) -> None:
        pos = self._position
        if pos is None:
            return
        self._position = None
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
        logger.info("BearTrap[%s]: SELL %s %d reason=%s exit=%.2f",
                    self._underlying, pos["side"], pos["strike"], reason, exit_price)

    async def liquidate(self, reason: str = "kill_switch") -> None:
        if self._position is not None:
            series = self._series.get(self._position["side"])
            ltp = series.last_ltp if series else self._position["entry_price"]
            await self._square_off(reason, ltp)

    # ── status / UI ──────────────────────────────────────────────────────────

    def status(self) -> dict:
        pos = self._position
        return dict(
            strategy="d1_trap_bear_only", underlying=self._underlying,
            ce_strike=self._ce_strike, pe_strike=self._pe_strike,
            spot_open=self._last_spot_open,
            selection_reason=(f"ATM={round((self._last_spot_open or 0)/self._strike_step)*self._strike_step} "
                               f"(spot_open={self._last_spot_open}) -> CE=ATM-{self._itm_offset_pts}, "
                               f"PE=ATM+{self._itm_offset_pts}") if self._last_spot_open else None,
            position=dict(
                side=pos["side"], strike=pos["strike"], entry=pos["entry_price"], sl=pos["sl"],
                locked_pct=round(pos["high_lock_pct"] * 100, 1), qty=pos["qty"],
                ltp=self._series[pos["side"]].last_ltp if pos["side"] in self._series else None,
            ) if pos else None,
        )

    def monitoring_zones(self) -> dict:
        def _zone_view(side: str) -> dict:
            series = self._series.get(side)
            if series is None:
                return dict(strike=None, zones=[], stage="NO_DATA")
            active = [z for z in series.zones if not z["done"] and not z["invalid"]]
            monitoring = [z for z in active if z["state"] == "MONITORING"]
            stage = "IDLE"
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
            return dict(
                strike=series.strike, zones_total=len(active), stage=stage,
                current_zone=dict(zone_lo=round(top["zone_lo"], 2), zone_hi=round(top["zone_hi"], 2),
                                   entry_line=round(top["entry_line"], 2)) if top else None,
                last_ltp=series.last_ltp,
            )

        return dict(
            underlying=self._underlying, client_id=self._client_id, binding_id=self._binding_id,
            ce=_zone_view("CE"), pe=_zone_view("PE"),
            position=self.status()["position"],
        )

    async def _option_loop(self) -> None:
        pass
