"""
backtest/fno_scanner/backtest.py — FnO Positional Trap Scanner Backtest
========================================================================
Strategy: Detect bull/bear trap zones on FnO stock D1 (daily) spot bars.
Enter long (for CE) on bear trap retest, short (for PE) on bull trap retest.
Hold for the move, managing with TSL + hedge logic.

Two-phase exit:
  Phase 1 — Day T1 hit (day high for long / day low for short)
           → ADD opposite side position to lock profit, wait for weekly T1
  Phase 2 — Weekly T1 hit → close both legs

TSL logic:
  TSL hit (tighter than hard SL) → ADD hedge at TSL price
           → if price recovers past TSL → close hedge, original continues
           → if hard SL hit → close both

Hard SL: zone_low - hard_sl_buf_pct (for long) — zone structure invalidated.

Parameter sweep across:
  - t1_type:      "rolling" | "fixed_at_entry"
  - tsl_method:   "pct_from_peak" | "fixed_points" | "prev_day_low" | "atr"
  - tsl_value:    method-specific (pct, points, ATR multiplier)
  - hedge_close:  "tsl_level" | "day_t1"
  - hard_sl_buf:  % below zone_low (default 0.5%)
  - min_rr:       minimum R:R to take a trade (default 1.5)

Usage:
    UPSTOX_TOKEN=<token> python backtest/fno_scanner/backtest.py
    UPSTOX_TOKEN=<token> python backtest/fno_scanner/backtest.py --months 3
    UPSTOX_TOKEN=<token> python backtest/fno_scanner/backtest.py --stocks RELIANCE,HDFCBANK
    UPSTOX_TOKEN=<token> python backtest/fno_scanner/backtest.py --single --tsl-method pct_from_peak --tsl-value 2.0 --t1-type rolling
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from itertools import groupby
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote as _q

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.global_config import IST
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones

# ── Constants ─────────────────────────────────────────────────────────────────

CACHE_DIR = Path(__file__).parent / "data_cache"
CACHE_DIR.mkdir(exist_ok=True)

# Top 30 most liquid FnO stocks by option OI + daily volume
TOP_30_STOCKS: Dict[str, str] = {
    "RELIANCE":   "NSE_EQ|INE002A01018",
    "HDFCBANK":   "NSE_EQ|INE040A01034",
    "ICICIBANK":  "NSE_EQ|INE090A01021",
    "INFY":       "NSE_EQ|INE009A01021",
    "TCS":        "NSE_EQ|INE467B01029",
    "BHARTIARTL": "NSE_EQ|INE397D01024",
    "SBIN":       "NSE_EQ|INE062A01020",
    "ITC":        "NSE_EQ|INE154A01025",
    "WIPRO":      "NSE_EQ|INE075A01022",
    "BAJFINANCE": "NSE_EQ|INE296A01032",   # face-value split: was INE296A01024
    "LT":         "NSE_EQ|INE018A01030",
    "AXISBANK":   "NSE_EQ|INE238A01034",
    "KOTAKBANK":  "NSE_EQ|INE237A01036",   # face-value split: was INE237A01028
    "HCLTECH":    "NSE_EQ|INE860A01027",
    "TITAN":      "NSE_EQ|INE280A01028",
    "MARUTI":     "NSE_EQ|INE585B01010",
    "NTPC":       "NSE_EQ|INE733E01010",
    "TATASTEEL":  "NSE_EQ|INE081A01020",
    "HINDALCO":   "NSE_EQ|INE038A01020",
    "GRASIM":     "NSE_EQ|INE047A01021",
    "ONGC":       "NSE_EQ|INE213A01029",
    "COALINDIA":  "NSE_EQ|INE522F01014",
    "TMPV":       "NSE_EQ|INE155A01022",     # post-demerger: was TATAMOTORS
    "SUNPHARMA":  "NSE_EQ|INE044A01036",
    "DRREDDY":    "NSE_EQ|INE089A01031",    # face-value split: was INE089A01023
    "CIPLA":      "NSE_EQ|INE059A01026",
    "JSWSTEEL":   "NSE_EQ|INE019A01038",
    "ADANIENT":   "NSE_EQ|INE423A01024",
    "TATACONSUM": "NSE_EQ|INE192A01025",
    "BAJAJFINSV": "NSE_EQ|INE918I01026",
}

# ── Bar dataclass ─────────────────────────────────────────────────────────────

@dataclass
class Bar:
    timestamp: datetime
    open:  float
    high:  float
    low:   float
    close: float
    volume: int = 0

    # Required by zone detection functions
    def __repr__(self):
        return f"Bar({self.timestamp.date()} O={self.open:.2f} H={self.high:.2f} L={self.low:.2f} C={self.close:.2f})"


# ── Upstox daily candle fetch ─────────────────────────────────────────────────

def _http_get(url: str, token: str) -> dict:
    try:
        from curl_cffi import requests as _cc
        r = _cc.get(url, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }, impersonate="chrome131", timeout=15)
        return r.json()
    except Exception as e:
        print(f"  HTTP error: {e}")
        return {}


def fetch_daily_bars(instrument_key: str, token: str,
                     start: date, end: date) -> List[Bar]:
    """Fetch D1 OHLCV bars from Upstox historical-candle (day interval)."""
    url = (
        f"https://api.upstox.com/v2/historical-candle/"
        f"{_q(instrument_key, safe='')}/"
        f"day/{end.strftime('%Y-%m-%d')}/{start.strftime('%Y-%m-%d')}"
    )
    data = _http_get(url, token)
    candles = (data.get("data") or {}).get("candles") or []
    bars: List[Bar] = []
    for c in candles:
        # Upstox format: [ts, open, high, low, close, volume, oi]
        if len(c) < 6:
            continue
        ts_str = c[0]
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(IST)
        except Exception:
            continue
        bars.append(Bar(
            timestamp=ts,
            open=float(c[1]),
            high=float(c[2]),
            low=float(c[3]),
            close=float(c[4]),
            volume=int(c[5]),
        ))
    # Upstox returns newest-first; reverse to oldest-first
    bars.sort(key=lambda b: b.timestamp)
    return bars


def load_or_fetch(symbol: str, key: str, token: str,
                  start: date, end: date) -> List[Bar]:
    """Cache D1 bars to disk; fetch from Upstox only once per symbol."""
    cache_file = CACHE_DIR / f"{symbol}_D1_{start}_{end}.json"
    if cache_file.exists():
        with open(cache_file) as f:
            raw = json.load(f)
        return [Bar(
            timestamp=datetime.fromisoformat(r["ts"]),
            open=r["o"], high=r["h"], low=r["l"], close=r["c"], volume=r["v"]
        ) for r in raw]

    bars = fetch_daily_bars(key, token, start, end)
    if bars:
        with open(cache_file, "w") as f:
            json.dump([{
                "ts": b.timestamp.isoformat(),
                "o": b.open, "h": b.high, "l": b.low,
                "c": b.close, "v": b.volume
            } for b in bars], f)
    return bars


# ── Weekly resampler (D1 → W1) ────────────────────────────────────────────────

@dataclass
class WBar:
    """Weekly bar."""
    timestamp: datetime
    high:  float
    low:   float
    close: float


def resample_d1_to_w1(d1_bars: List[Bar]) -> List[WBar]:
    """Group D1 bars by ISO week (Monday anchor). Returns oldest-first."""
    def week_key(b: Bar) -> date:
        d = b.timestamp.date()
        return d - timedelta(days=d.weekday())  # Monday of that week

    result: List[WBar] = []
    for _wstart, group in groupby(d1_bars, key=week_key):
        chunk = list(group)
        result.append(WBar(
            timestamp=chunk[0].timestamp,
            high=max(b.high for b in chunk),
            low=min(b.low for b in chunk),
            close=chunk[-1].close,
        ))
    return result


# ── ATR helper ────────────────────────────────────────────────────────────────

def _atr(bars: List[Bar], period: int = 14) -> float:
    """Wilder ATR(period) on the last `period` bars. Returns 0 if insufficient."""
    if len(bars) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(bars)):
        b, prev = bars[i], bars[i - 1]
        trs.append(max(b.high - b.low, abs(b.high - prev.close), abs(b.low - prev.close)))
    if len(trs) < period:
        return 0.0
    # Wilder smoothing: seed with simple average, then smooth
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


# ── Parameter grid ────────────────────────────────────────────────────────────

@dataclass
class Params:
    t1_type:        str    # "rolling" | "fixed_at_entry"
    tsl_method:     str    # "pct_from_peak" | "fixed_points" | "prev_day_low" | "atr"
    tsl_value:      float  # pct (%), points, or ATR multiplier depending on method
    hedge_close:    str    # "tsl_level" | "day_t1"
    hard_sl_buf:    float  # % below/above zone boundary  (default 0.5)
    min_rr:         float  # minimum R:R to enter          (default 1.5)
    max_zone_age:   int    # max bars since lock_ts to still enter (default 60)

    def label(self) -> str:
        return (f"t1={self.t1_type[:3]} tsl={self.tsl_method[:3]}"
                f"({self.tsl_value}) hc={self.hedge_close[:3]}"
                f" sl={self.hard_sl_buf}%")


def build_param_grid() -> List[Params]:
    """Build all combinations to sweep."""
    grid: List[Params] = []
    t1_types   = ["rolling", "fixed_at_entry"]
    hedge_closes = ["tsl_level", "day_t1"]
    sl_bufs    = [0.3, 0.5, 0.8]
    min_rrs    = [1.0, 1.5, 2.0]

    for t1 in t1_types:
        for hc in hedge_closes:
            for sl in sl_bufs:
                for rr in min_rrs:
                    # pct_from_peak: 1.0 / 1.5 / 2.0 / 2.5 / 3.0 %
                    for pct in [1.0, 1.5, 2.0, 2.5, 3.0]:
                        grid.append(Params(t1, "pct_from_peak", pct, hc, sl, rr, 60))
                    # fixed_points: 10 / 15 / 20 / 25 / 30 pts
                    for pts in [10.0, 15.0, 20.0, 25.0, 30.0]:
                        grid.append(Params(t1, "fixed_points", pts, hc, sl, rr, 60))
                    # prev_day_low: no tunable value (use 0 as placeholder)
                    grid.append(Params(t1, "prev_day_low", 0.0, hc, sl, rr, 60))
                    # atr: 1.0 / 1.5 / 2.0 × ATR(14)
                    for mult in [1.0, 1.5, 2.0]:
                        grid.append(Params(t1, "atr", mult, hc, sl, rr, 60))

    return grid


# ── Trade state machine ───────────────────────────────────────────────────────

@dataclass
class Trade:
    symbol:     str
    direction:  str         # "LONG" (bear trap → buy CE) | "SHORT" (bull trap → buy PE)
    entry_date: date
    entry_price: float
    zone_low:   float       # bear trap zone boundary (low end)
    zone_high:  float       # bear trap zone boundary (high end)
    hard_sl:    float       # absolute price level
    day_t1_fixed: float     # entry day high (for fixed_at_entry t1_type)
    week_t1_fixed: float    # entry week high (for fixed_at_entry t1_type)

    # Rolling tracking (updates each day)
    peak_price:     float = 0.0   # highest (long) or lowest (short) price reached
    tsl_price:      float = 0.0   # current TSL level
    day_t1_rolling: float = 0.0   # running day high / day low
    week_t1_rolling: float = 0.0  # running week high / week low
    prev_day_low:   float = 0.0   # for prev_day_low TSL method
    prev_day_high:  float = 0.0   # for short prev_day_high TSL

    # Phase tracking
    phase:          int = 1       # 1=open, 2=day_t1_hit+hedged, 3=tsl_hit+hedged
    hedge_price:    float = 0.0   # price at which hedge was added
    hedge_closed:   bool = False  # hedge leg closed (waiting for original to finish)

    # Exit
    exit_date:      Optional[date] = None
    exit_price:     float = 0.0
    exit_reason:    str = ""
    phase1_pnl_pct: float = 0.0
    phase2_pnl_pct: float = 0.0
    total_pnl_pct:  float = 0.0

    @property
    def is_open(self) -> bool:
        return self.exit_date is None

    def rr(self) -> float:
        risk = abs(self.entry_price - self.hard_sl)
        if risk <= 0:
            return 0.0
        reward = abs(self.day_t1_fixed - self.entry_price)
        return reward / risk

    def _pnl_pct(self, price: float) -> float:
        """Net spot P&L % from entry, accounting for hedge positions."""
        if self.direction == "LONG":
            raw = (price - self.entry_price) / self.entry_price * 100
        else:
            raw = (self.entry_price - price) / self.entry_price * 100

        # Hedge offsets: subtract hedge leg P&L if hedge is still open
        if self.phase in (2, 3) and not self.hedge_closed:
            if self.direction == "LONG":
                hedge_pnl = (self.hedge_price - price) / self.entry_price * 100
            else:
                hedge_pnl = (price - self.hedge_price) / self.entry_price * 100
            return raw + hedge_pnl
        return raw


@dataclass
class BacktestResult:
    params:     Params
    trades:     List[Trade] = field(default_factory=list)

    def completed(self) -> List[Trade]:
        return [t for t in self.trades if not t.is_open]

    def win_rate(self) -> float:
        c = self.completed()
        if not c:
            return 0.0
        return sum(1 for t in c if t.total_pnl_pct > 0) / len(c) * 100

    def profit_factor(self) -> float:
        c = self.completed()
        gross_profit = sum(t.total_pnl_pct for t in c if t.total_pnl_pct > 0)
        gross_loss   = abs(sum(t.total_pnl_pct for t in c if t.total_pnl_pct <= 0))
        return gross_profit / gross_loss if gross_loss > 0 else float("inf")

    def net_pnl_pct(self) -> float:
        return sum(t.total_pnl_pct for t in self.completed())

    def avg_hold_days(self) -> float:
        c = self.completed()
        if not c:
            return 0.0
        return sum((t.exit_date - t.entry_date).days for t in c) / len(c)


# ── Core simulation ───────────────────────────────────────────────────────────

def _compute_tsl(trade: Trade, params: Params,
                 bars_so_far: List[Bar]) -> float:
    """Compute TSL level given current trade state."""
    if trade.direction == "LONG":
        peak = trade.peak_price
        if params.tsl_method == "pct_from_peak":
            return peak * (1 - params.tsl_value / 100)
        elif params.tsl_method == "fixed_points":
            return peak - params.tsl_value
        elif params.tsl_method == "prev_day_low":
            return trade.prev_day_low if trade.prev_day_low > 0 else trade.hard_sl
        elif params.tsl_method == "atr":
            atr_val = _atr(bars_so_far[-20:], 14) if len(bars_so_far) >= 15 else 0.0
            return peak - params.tsl_value * atr_val if atr_val > 0 else trade.hard_sl
    else:  # SHORT
        peak = trade.peak_price  # for short, peak = lowest price reached
        if params.tsl_method == "pct_from_peak":
            return peak * (1 + params.tsl_value / 100)
        elif params.tsl_method == "fixed_points":
            return peak + params.tsl_value
        elif params.tsl_method == "prev_day_low":
            return trade.prev_day_high if trade.prev_day_high > 0 else trade.hard_sl
        elif params.tsl_method == "atr":
            atr_val = _atr(bars_so_far[-20:], 14) if len(bars_so_far) >= 15 else 0.0
            return peak + params.tsl_value * atr_val if atr_val > 0 else trade.hard_sl
    return trade.hard_sl


def _last_week_of_month_start(d: date) -> date:
    """Return the Monday that begins the last week before month-end expiry.
    NSE monthly expiry = last Thursday of month. Last week = 7 days before that."""
    # Find last Thursday of month
    next_month = d.replace(day=28) + timedelta(days=4)
    last_day = next_month - timedelta(days=next_month.day)
    # Walk back to last Thursday
    while last_day.weekday() != 3:  # 3 = Thursday
        last_day -= timedelta(days=1)
    # Last week starts 7 days before last Thursday (previous Thursday)
    return last_day - timedelta(days=7)


def simulate_stock(symbol: str, bars: List[Bar],
                   params: Params) -> List[Trade]:
    """Simulate all trades on a single stock's D1 bar series."""
    if len(bars) < 20:
        return []

    trades: List[Trade] = []
    active_trade: Optional[Trade] = None

    w1_bars = resample_d1_to_w1(bars)
    # Map each bar's date to its week's high/low for easy lookup
    week_highs: Dict[date, float] = {}
    week_lows:  Dict[date, float] = {}
    for wb in w1_bars:
        wstart = wb.timestamp.date()
        # Mark all 5 days of the week
        for delta in range(7):
            d = wstart + timedelta(days=delta)
            week_highs[d] = wb.high
            week_lows[d]  = wb.low

    for i, bar in enumerate(bars):
        today = bar.timestamp.date()

        # ── Step 1: update or close active trade ──────────────────────────
        if active_trade is not None and active_trade.is_open:
            t = active_trade
            is_long = t.direction == "LONG"
            bars_so_far = bars[:i + 1]

            # Update peak price
            if is_long:
                t.peak_price = max(t.peak_price, bar.high)
            else:
                t.peak_price = min(t.peak_price, bar.low)

            # Update TSL level
            t.tsl_price = _compute_tsl(t, params, bars_so_far)

            # Rolling T1 updates
            if is_long:
                t.day_t1_rolling = max(t.day_t1_rolling, bar.high)
                t.week_t1_rolling = week_highs.get(today, t.week_t1_rolling)
            else:
                t.day_t1_rolling = min(t.day_t1_rolling, bar.low)
                t.week_t1_rolling = week_lows.get(today, t.week_t1_rolling)

            # Which T1 to use
            day_t1  = t.day_t1_rolling  if params.t1_type == "rolling" else t.day_t1_fixed
            week_t1 = t.week_t1_rolling if params.t1_type == "rolling" else t.week_t1_fixed

            # ── Check exits in priority order ──────────────────────────────

            # Force-close: last week of monthly expiry starts
            if today >= _last_week_of_month_start(today):
                exit_p = bar.close
                t.exit_date   = today
                t.exit_price  = exit_p
                t.exit_reason = "expiry_week"
                t.total_pnl_pct = t._pnl_pct(exit_p)
                active_trade = None
                continue

            # Hard SL
            hard_sl_breach = (bar.low <= t.hard_sl) if is_long else (bar.high >= t.hard_sl)
            if hard_sl_breach:
                exit_p = t.hard_sl
                t.exit_date   = today
                t.exit_price  = exit_p
                t.exit_reason = "hard_sl"
                t.phase1_pnl_pct = t._pnl_pct(exit_p)
                t.total_pnl_pct  = t.phase1_pnl_pct
                active_trade = None
                continue

            # Weekly T1 hit → close all
            weekly_t1_hit = (bar.high >= week_t1) if is_long else (bar.low <= week_t1)
            if weekly_t1_hit and t.phase >= 2:
                exit_p = week_t1
                t.exit_date   = today
                t.exit_price  = exit_p
                t.exit_reason = "weekly_t1"
                t.phase1_pnl_pct = (
                    (t.hedge_price - t.entry_price) / t.entry_price * 100
                    if is_long else
                    (t.entry_price - t.hedge_price) / t.entry_price * 100
                )
                t.phase2_pnl_pct = t._pnl_pct(exit_p) - t.phase1_pnl_pct
                t.total_pnl_pct  = t._pnl_pct(exit_p)
                active_trade = None
                continue

            # Phase 1: Day T1 hit → add hedge, move to phase 2
            day_t1_hit = (bar.high >= day_t1) if is_long else (bar.low <= day_t1)
            if t.phase == 1 and day_t1_hit:
                t.phase = 2
                t.hedge_price = day_t1  # add hedge at day T1 level
                t.phase1_pnl_pct = (
                    (day_t1 - t.entry_price) / t.entry_price * 100
                    if is_long else
                    (t.entry_price - day_t1) / t.entry_price * 100
                )

            # TSL hit: add hedge at TSL level, move to phase 3
            tsl_hit = (bar.low <= t.tsl_price) if is_long else (bar.high >= t.tsl_price)
            if t.phase == 1 and tsl_hit:
                t.phase = 3
                t.hedge_price = t.tsl_price

            # Phase 3: hedge close logic
            if t.phase == 3 and not t.hedge_closed:
                if params.hedge_close == "tsl_level":
                    # Close hedge when price recovers past the TSL price
                    recovered = (bar.high >= t.tsl_price) if is_long else (bar.low <= t.tsl_price)
                    if recovered:
                        t.hedge_closed = True
                elif params.hedge_close == "day_t1":
                    # Close hedge only when day T1 is hit
                    if day_t1_hit:
                        t.hedge_closed = True
                        t.phase = 2  # transition to phase 2 now

            # Update prev_day for next iteration's TSL
            t.prev_day_low  = bar.low
            t.prev_day_high = bar.high
            continue

        # ── Step 2: scan for new entry (no active trade) ──────────────────
        if i < 5:   # need at least 5 bars for a meaningful zone
            continue

        lookback = bars[:i]  # bars seen before today
        bear_zones = find_all_bear_zones(lookback)
        bull_zones = find_all_bull_zones(lookback)

        for zone in bear_zones + bull_zones:
            is_bear = zone in bear_zones
            direction = "LONG" if is_bear else "SHORT"

            # Zone bounds
            zone_low  = min(zone.entry_line or 0, zone.sweep_low or 0)
            zone_high = max(zone.entry_line or 0, zone.sweep_low or 0)

            if zone_low <= 0 or zone_high <= 0:
                continue

            # Zone age check
            if zone.lock_ts is not None:
                age_days = (today - zone.lock_ts.date()).days
                if age_days > params.max_zone_age:
                    continue

            # Entry trigger: today's price retests the zone
            if is_bear:
                # Bear trap: price comes back down to entry_line (zone support)
                retest = bar.low <= zone.entry_line and bar.close >= zone_low
            else:
                # Bull trap: price comes back up to entry_line (zone resistance)
                retest = bar.high >= zone.entry_line and bar.close <= zone_high

            if not retest:
                continue

            entry_price = zone.entry_line

            # Hard SL
            if is_bear:
                hard_sl = zone_low * (1 - params.hard_sl_buf / 100)
            else:
                hard_sl = zone_high * (1 + params.hard_sl_buf / 100)

            # Day T1 (fixed at entry day)
            day_t1_fixed = bar.high if is_bear else bar.low

            # Week T1 (fixed at entry week)
            if is_bear:
                week_t1_fixed = week_highs.get(today, bar.high)
            else:
                week_t1_fixed = week_lows.get(today, bar.low)

            # R:R check
            risk   = abs(entry_price - hard_sl)
            reward = abs(day_t1_fixed - entry_price)
            if risk <= 0 or (reward / risk) < params.min_rr:
                continue

            # Create trade
            t = Trade(
                symbol=symbol,
                direction=direction,
                entry_date=today,
                entry_price=entry_price,
                zone_low=zone_low,
                zone_high=zone_high,
                hard_sl=hard_sl,
                day_t1_fixed=day_t1_fixed,
                week_t1_fixed=week_t1_fixed,
                peak_price=bar.high if is_bear else bar.low,
                day_t1_rolling=bar.high if is_bear else bar.low,
                week_t1_rolling=week_t1_fixed,
                prev_day_low=bar.low,
                prev_day_high=bar.high,
            )
            t.tsl_price = _compute_tsl(t, params, bars[:i + 1])
            active_trade = t
            trades.append(t)
            break  # one trade at a time per stock

    return trades


# ── Multi-stock runner ────────────────────────────────────────────────────────

def run_backtest_for_params(
    stock_bars: Dict[str, List[Bar]],
    params: Params,
) -> BacktestResult:
    result = BacktestResult(params=params)
    for symbol, bars in stock_bars.items():
        trades = simulate_stock(symbol, bars, params)
        result.trades.extend(trades)
    return result


# ── Report printer ────────────────────────────────────────────────────────────

def print_stock_report(symbol: str, trades: List[Trade]) -> None:
    completed = [t for t in trades if not t.is_open]
    if not completed:
        return
    wins = [t for t in completed if t.total_pnl_pct > 0]
    gross_p = sum(t.total_pnl_pct for t in completed if t.total_pnl_pct > 0)
    gross_l = abs(sum(t.total_pnl_pct for t in completed if t.total_pnl_pct <= 0))
    pf = gross_p / gross_l if gross_l > 0 else float("inf")
    reasons = {}
    for t in completed:
        reasons[t.exit_reason] = reasons.get(t.exit_reason, 0) + 1
    reason_str = "  ".join(f"{k}={v}" for k, v in sorted(reasons.items()))
    print(f"  {symbol:<14} trades={len(completed):>3}  "
          f"win={len(wins)/len(completed)*100:>5.1f}%  "
          f"PF={pf:>5.2f}  net={sum(t.total_pnl_pct for t in completed):>+7.2f}%  "
          f"avghold={sum((t.exit_date-t.entry_date).days for t in completed)/len(completed):>4.1f}d  "
          f"exits: {reason_str}")


def print_sweep_report(results: List[BacktestResult],
                       stock_bars: Dict[str, List[Bar]],
                       top_n: int = 10) -> None:
    print("\n" + "=" * 80)
    print("FnO SCANNER — PARAMETER SWEEP RESULTS")
    print("=" * 80)

    # Sort by profit factor, then win rate
    valid = [r for r in results if len(r.completed()) >= 5]
    valid.sort(key=lambda r: (r.profit_factor(), r.win_rate()), reverse=True)

    print(f"\nTop {top_n} parameter configs (min 5 completed trades):\n")
    print(f"  {'Rank':<5} {'PF':>6} {'Win%':>7} {'Trades':>8} {'Net%':>8}  Config")
    print(f"  {'-'*5} {'-'*6} {'-'*7} {'-'*8} {'-'*8}  {'─'*40}")
    for rank, r in enumerate(valid[:top_n], 1):
        print(f"  {rank:<5} {r.profit_factor():>6.2f} {r.win_rate():>6.1f}%"
              f" {len(r.completed()):>8} {r.net_pnl_pct():>+8.2f}%  {r.params.label()}")

    # Per-stock breakdown using the best config
    if valid:
        best = valid[0]
        print(f"\n{'─'*80}")
        print(f"Per-stock breakdown — best config: {best.params.label()}")
        print(f"{'─'*80}")

        stock_trades: Dict[str, List[Trade]] = {}
        for t in best.trades:
            stock_trades.setdefault(t.symbol, []).append(t)

        stock_rows = []
        for sym, trades in stock_trades.items():
            completed = [t for t in trades if not t.is_open]
            if not completed:
                continue
            wins = sum(1 for t in completed if t.total_pnl_pct > 0)
            gl = abs(sum(t.total_pnl_pct for t in completed if t.total_pnl_pct <= 0))
            gp = sum(t.total_pnl_pct for t in completed if t.total_pnl_pct > 0)
            pf = gp / gl if gl > 0 else float("inf")
            stock_rows.append((sym, len(completed), wins / len(completed) * 100,
                                pf, sum(t.total_pnl_pct for t in completed)))

        stock_rows.sort(key=lambda x: x[3], reverse=True)
        print(f"\n  {'Symbol':<14} {'Trades':>8} {'Win%':>7} {'PF':>7} {'Net%':>8}")
        print(f"  {'─'*14} {'─'*8} {'─'*7} {'─'*7} {'─'*8}")
        for sym, n, wr, pf, net in stock_rows:
            flag = "  ← TRADE" if pf >= 2.0 and wr >= 55 and n >= 3 else ""
            print(f"  {sym:<14} {n:>8} {wr:>6.1f}% {pf:>7.2f} {net:>+8.2f}%{flag}")

        print(f"\n  Stocks marked ← TRADE: PF≥2.0, Win≥55%, Trades≥3 → recommended for live")

    print(f"\n{'='*80}\n")


# ── CLI entry point ───────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="FnO Scanner Positional Backtest")
    parser.add_argument("--months",    type=int,   default=6,    help="Lookback months (default 6)")
    parser.add_argument("--stocks",    type=str,   default="",   help="Comma-separated symbols (default: all 30)")
    parser.add_argument("--single",    action="store_true",      help="Run single config instead of sweep")
    parser.add_argument("--t1-type",   default="rolling",        help="rolling | fixed_at_entry")
    parser.add_argument("--tsl-method",default="pct_from_peak",  help="pct_from_peak | fixed_points | prev_day_low | atr")
    parser.add_argument("--tsl-value", type=float, default=2.0,  help="TSL param value")
    parser.add_argument("--hedge-close",default="tsl_level",     help="tsl_level | day_t1")
    parser.add_argument("--hard-sl",   type=float, default=0.5,  help="Hard SL buffer %%")
    parser.add_argument("--min-rr",    type=float, default=1.5,  help="Min R:R to enter")
    args = parser.parse_args()

    token = os.environ.get("UPSTOX_TOKEN", "").strip()
    if not token:
        print("ERROR: Set UPSTOX_TOKEN environment variable.")
        sys.exit(1)

    # Date range
    end_date   = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=args.months * 31)
    print(f"\nFnO Scanner Backtest: {start_date} to {end_date} ({args.months} months)")

    # Stock selection
    if args.stocks:
        symbols = {s.strip(): TOP_30_STOCKS[s.strip()]
                   for s in args.stocks.split(",")
                   if s.strip() in TOP_30_STOCKS}
    else:
        symbols = TOP_30_STOCKS

    # Fetch / load D1 bars
    print(f"Loading D1 bars for {len(symbols)} stocks...")
    stock_bars: Dict[str, List[Bar]] = {}
    for symbol, key in symbols.items():
        bars = load_or_fetch(symbol, key, token, start_date, end_date)
        if len(bars) < 20:
            print(f"  {symbol}: insufficient data ({len(bars)} bars) — skipping")
            continue
        stock_bars[symbol] = bars
        print(f"  {symbol}: {len(bars)} bars ({bars[0].timestamp.date()} to {bars[-1].timestamp.date()})")

    if not stock_bars:
        print("No data loaded. Check token and connectivity.")
        sys.exit(1)

    if args.single:
        # Single config detailed run
        params = Params(
            t1_type=args.t1_type,
            tsl_method=args.tsl_method,
            tsl_value=args.tsl_value,
            hedge_close=args.hedge_close,
            hard_sl_buf=args.hard_sl,
            min_rr=args.min_rr,
            max_zone_age=60,
        )
        print(f"\nSingle run: {params.label()}\n")
        result = run_backtest_for_params(stock_bars, params)
        for symbol in sorted(stock_bars.keys()):
            sym_trades = [t for t in result.trades if t.symbol == symbol]
            if sym_trades:
                print_stock_report(symbol, sym_trades)
        print(f"\nOVERALL: trades={len(result.completed())}  "
              f"win={result.win_rate():.1f}%  PF={result.profit_factor():.2f}  "
              f"net={result.net_pnl_pct():+.2f}%  avghold={result.avg_hold_days():.1f}d")
    else:
        # Full sweep
        grid = build_param_grid()
        print(f"\nRunning parameter sweep: {len(grid)} configs × {len(stock_bars)} stocks...")
        results: List[BacktestResult] = []
        for idx, params in enumerate(grid):
            if idx % 50 == 0:
                print(f"  {idx}/{len(grid)} configs...")
            results.append(run_backtest_for_params(stock_bars, params))

        print_sweep_report(results, stock_bars)


if __name__ == "__main__":
    main()
