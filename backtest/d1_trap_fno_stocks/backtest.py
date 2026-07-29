"""
backtest/d1_trap_fno_stocks/backtest.py — D1 Trap + 75M C2/TWEAK on FnO stocks (spot)

Applies the same D1 3-candle trap detection + MTF entry strategy used for NIFTY
to the top 30 NSE FnO stocks using spot (equity) price data.

Strategy:
  D1 zones  — bear trap (sellers swept → reclaim → LONG) / bull trap (buyers swept → reclaim → SHORT)
  75M MTF   — price enters zone → next 75M breach of ref candle = ENTRY (C2)   [--mtf to change]
  TWEAK     — zone fails WHILE monitoring (MTF closes through far boundary) → counter-direction on next MTF breach
  Exit      — TSL (MTF low/high ratchet), hard SL (original ref-candle extreme), or EOD 15:15 IST

P&L is in spot points. Multiply by per-stock lot size for rupee value.

Usage:
  python backtest/d1_trap_fno_stocks/backtest.py
  python backtest/d1_trap_fno_stocks/backtest.py --months 3
  python backtest/d1_trap_fno_stocks/backtest.py --months 3 --mtf 75
  python backtest/d1_trap_fno_stocks/backtest.py --stocks RELIANCE,HDFCBANK
  UPSTOX_TOKEN=<token> python backtest/d1_trap_fno_stocks/backtest.py --months 3
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import quote as _q

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.global_config import IST
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones

# ── Constants ─────────────────────────────────────────────────────────────────

SESSION_OPEN  = (9, 15)
EOD_HM        = (15, 15)
ENTRY_CUTOFF  = (14, 0)
MAX_ZONE_DAYS = 20          # discard D1 zones older than 20 calendar days

# D1 data from fno_scanner cache (already fetched)
_FNO_D1_CACHE = Path(__file__).resolve().parents[2] / "backtest" / "fno_scanner" / "data_cache"
# 30-min intraday cache (new, local)
_INTRA_CACHE  = Path(__file__).parent / "data_cache"

# Top 30 FnO stocks — Upstox NSE_EQ instrument keys
TOP_30: Dict[str, str] = {
    "RELIANCE":   "NSE_EQ|INE002A01018",
    "HDFCBANK":   "NSE_EQ|INE040A01034",
    "ICICIBANK":  "NSE_EQ|INE090A01021",
    "INFY":       "NSE_EQ|INE009A01021",
    "TCS":        "NSE_EQ|INE467B01029",
    "BHARTIARTL": "NSE_EQ|INE397D01024",
    "SBIN":       "NSE_EQ|INE062A01020",
    "ITC":        "NSE_EQ|INE154A01025",
    "WIPRO":      "NSE_EQ|INE075A01022",
    "BAJFINANCE": "NSE_EQ|INE296A01032",
    "LT":         "NSE_EQ|INE018A01030",
    "AXISBANK":   "NSE_EQ|INE238A01034",
    "KOTAKBANK":  "NSE_EQ|INE237A01036",
    "HCLTECH":    "NSE_EQ|INE860A01027",
    "TITAN":      "NSE_EQ|INE280A01028",
    "MARUTI":     "NSE_EQ|INE585B01010",
    "NTPC":       "NSE_EQ|INE733E01010",
    "TATASTEEL":  "NSE_EQ|INE081A01020",
    "HINDALCO":   "NSE_EQ|INE038A01020",
    "GRASIM":     "NSE_EQ|INE047A01021",
    "ONGC":       "NSE_EQ|INE213A01029",
    "COALINDIA":  "NSE_EQ|INE522F01014",
    "TATAMOTORS": "NSE_EQ|INE155A01022",
    "SUNPHARMA":  "NSE_EQ|INE044A01036",
    "DRREDDY":    "NSE_EQ|INE089A01031",
    "CIPLA":      "NSE_EQ|INE059A01026",
    "JSWSTEEL":   "NSE_EQ|INE019A01038",
    "ADANIENT":   "NSE_EQ|INE423A01024",
    "TATACONSUM": "NSE_EQ|INE192A01025",
    "BAJAJFINSV": "NSE_EQ|INE918I01026",
}

# Approximate FnO lot sizes (NSE current)
# Lot sizes verified from Upstox NSE instrument master 2026-07-29
LOT_SIZES: Dict[str, int] = {
    "RELIANCE": 500, "HDFCBANK": 650, "ICICIBANK": 700, "INFY": 400,
    "TCS": 225, "BHARTIARTL": 475, "SBIN": 750, "ITC": 1725,
    "WIPRO": 3000, "BAJFINANCE": 750, "LT": 175, "AXISBANK": 625,
    "KOTAKBANK": 2000, "HCLTECH": 400, "TITAN": 175, "MARUTI": 50,
    "NTPC": 1500, "TATASTEEL": 2750, "HINDALCO": 700, "GRASIM": 250,
    "ONGC": 2250, "COALINDIA": 1350, "TATAMOTORS": 2150, "SUNPHARMA": 350,
    "DRREDDY": 625, "CIPLA": 425, "JSWSTEEL": 675, "ADANIENT": 309,
    "TATACONSUM": 550, "BAJAJFINSV": 300,
}


# ── Bar dataclass ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class _Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float


# ── Upstox fetch helpers ──────────────────────────────────────────────────────

def _http_get(url: str, token: str) -> dict:
    try:
        from curl_cffi import requests as _cc
        hdrs = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
        return _cc.get(url, headers=hdrs, impersonate="chrome131", timeout=20).json()
    except Exception as exc:
        print(f"  [HTTP] {exc}")
        return {}


def _parse_candles(raw: dict) -> List[_Bar]:
    rows = (raw.get("data") or {}).get("candles") or []
    out: List[_Bar] = []
    for c in reversed(rows):   # newest-first → oldest-first
        try:
            ts = datetime.fromisoformat(c[0]).astimezone(IST)
            out.append(_Bar(timestamp=ts, open=float(c[1]), high=float(c[2]),
                            low=float(c[3]), close=float(c[4])))
        except Exception:
            pass
    return out


def _fetch_d1_from_fno_cache(symbol: str, start: date, end: date, token: str) -> List[_Bar]:
    """
    Try to load D1 bars from fno_scanner's cache first.
    Falls back to Upstox API if cache miss or date range doesn't cover needed data.
    """
    # fno_scanner cache: {SYMBOL}_D1_{start}_{end}.json — stored as list of dicts
    for f in sorted(_FNO_D1_CACHE.glob(f"{symbol}_D1_*.json")):
        try:
            with open(f) as fp:
                raw = json.load(fp)
            bars: List[_Bar] = []
            for r in raw:
                ts = datetime.fromisoformat(r["ts"])
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=IST)
                bars.append(_Bar(timestamp=ts, open=r["o"], high=r["h"], low=r["l"], close=r["c"]))
            bars.sort(key=lambda b: b.timestamp)
            # Check coverage
            if bars and bars[0].timestamp.date() <= start and bars[-1].timestamp.date() >= end:
                print(f"  [D1 cache] {symbol}: {len(bars)} bars from fno_scanner cache")
                return bars
        except Exception:
            pass

    # Cache miss → fetch from Upstox
    key = TOP_30.get(symbol, "")
    if not key or not token:
        print(f"  [D1] {symbol}: no instrument key or token — skipping")
        return []

    cache_path = _INTRA_CACHE / f"{symbol}_D1_{start}_{end}.json"
    if cache_path.exists():
        with open(cache_path) as fp:
            raw = json.load(fp)
        return _parse_candles(raw)

    url = (f"https://api.upstox.com/v2/historical-candle/{_q(key, safe='')}"
           f"/day/{end.isoformat()}/{start.isoformat()}")
    raw = _http_get(url, token)
    bars = _parse_candles(raw)
    print(f"  [D1 API] {symbol}: {len(bars)} bars ({start}..{end})")
    if bars:
        tmp = str(cache_path) + ".tmp"
        with open(tmp, "w") as fp:
            json.dump(raw, fp)
        os.replace(tmp, cache_path)
    return bars


def _fetch_intraday_chunk(symbol: str, key: str, interval: str,
                          start: date, end: date, token: str) -> List[_Bar]:
    cache_path = _INTRA_CACHE / f"{symbol}_{interval}_{start}_{end}.json"
    if cache_path.exists():
        with open(cache_path) as fp:
            raw = json.load(fp)
        return _parse_candles(raw)
    url = (f"https://api.upstox.com/v2/historical-candle/{_q(key, safe='')}"
           f"/{interval}/{end.isoformat()}/{start.isoformat()}")
    raw = _http_get(url, token)
    bars = _parse_candles(raw)
    print(f"  [{interval} API] {symbol}: {len(bars)} bars ({start}..{end})")
    if bars:
        tmp = str(cache_path) + ".tmp"
        with open(tmp, "w") as fp:
            json.dump(raw, fp)
        os.replace(tmp, cache_path)
    return bars


def fetch_intraday(symbol: str, start: date, end: date, token: str,
                   interval: str = "30minute") -> List[_Bar]:
    """Fetch intraday bars in 30-day chunks (Upstox limit per request)."""
    key = TOP_30.get(symbol, "")
    if not key or not token:
        return []
    all_bars: List[_Bar] = []
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(chunk_start + timedelta(days=29), end)
        chunk = _fetch_intraday_chunk(symbol, key, interval, chunk_start, chunk_end, token)
        all_bars.extend(chunk)
        chunk_start = chunk_end + timedelta(days=1)
    # deduplicate + sort
    seen: Set[datetime] = set()
    out: List[_Bar] = []
    for b in sorted(all_bars, key=lambda b: b.timestamp):
        if b.timestamp not in seen:
            seen.add(b.timestamp)
            out.append(b)
    return out


# ── Resample 30m → 1H ────────────────────────────────────────────────────────

def resample_to_Nm(bars_30m: List[_Bar], mins: int) -> List[_Bar]:
    """Resample 30-min bars to N-min bars, clock-anchored at 09:15 IST."""
    buckets: Dict = {}
    order: List = []
    for b in bars_30m:
        open_dt = b.timestamp.replace(hour=9, minute=15, second=0, microsecond=0)
        elapsed = max(0, int((b.timestamp - open_dt).total_seconds() // 60))
        bucket = (b.timestamp.date(), elapsed // mins)
        if bucket not in buckets:
            buckets[bucket] = []
            order.append(bucket)
        buckets[bucket].append(b)
    out: List[_Bar] = []
    for key in order:
        chunk = buckets[key]
        out.append(_Bar(
            timestamp=chunk[0].timestamp,
            open=chunk[0].open,
            high=max(b.high for b in chunk),
            low=min(b.low for b in chunk),
            close=chunk[-1].close,
        ))
    return out


# ── Monitor / Trade dataclasses ───────────────────────────────────────────────

@dataclass
class _Monitor:
    direction: str
    d1_ref_ts: datetime
    d1_sweep_ts: datetime
    d1_reclaim_ts: datetime
    zone_lo: float
    zone_hi: float
    state: str = "WAITING"
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
    d1_ref_ts: datetime
    d1_sweep_ts: datetime
    d1_reclaim_ts: datetime
    zone_entry_ts: datetime
    done: bool = False


@dataclass
class Trade:
    symbol: str
    direction: str
    entry_ts: datetime
    entry: float
    sl: float
    tsl_level: float
    zone_lo: float
    zone_hi: float
    source: str           # "C2" | "TWEAK"
    exit_ts: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: str = ""  # "SL" | "TSL" | "EOD"
    pnl_pts: Optional[float] = None

    @property
    def pnl_rs(self) -> float:
        ls = LOT_SIZES.get(self.symbol, 1)
        return 0.0 if self.pnl_pts is None else self.pnl_pts * ls

    def as_row(self) -> dict:
        fmt = lambda dt: dt.strftime("%Y-%m-%d %H:%M") if dt else ""
        return {
            "Symbol":    self.symbol,
            "Dir":       self.direction,
            "Source":    self.source,
            "Entry_TS":  fmt(self.entry_ts),
            "Entry":     f"{self.entry:.2f}",
            "SL":        f"{self.sl:.2f}",
            "Risk_pts":  f"{abs(self.entry - self.sl):.2f}",
            "Exit_TS":   fmt(self.exit_ts),
            "Exit":      f"{self.exit_price:.2f}" if self.exit_price is not None else "",
            "Reason":    self.exit_reason,
            "PnL_pts":   f"{self.pnl_pts:.2f}" if self.pnl_pts is not None else "",
            "PnL_Rs":    f"{self.pnl_rs:.0f}",
            "Zone_Lo":   f"{self.zone_lo:.2f}",
            "Zone_Hi":   f"{self.zone_hi:.2f}",
        }


# ── Simulation ────────────────────────────────────────────────────────────────

def run_c2_tweak_tsl(symbol: str, d1_bars: List[_Bar], h1_bars: List[_Bar]) -> List[Trade]:
    """
    C2 + TWEAK entry, TSL exit on 1H bars.

    C2:    zone WAITING → 1H enters → MONITORING; next 1H breaches ref candle → ENTRY
    TWEAK: while MONITORING, 1H closes through far zone boundary → queue counter-direction;
           on the NEXT 1H bar's breach of the failure bar → ENTRY

    Exit (checked after each 1H bar closes):
      1. Hard SL:  bar.low <= sl  (LONG) or bar.high >= sl (SHORT)
      2. TSL:      bar.close < tsl_level (LONG) or bar.close > tsl_level (SHORT)
         TSL ratchets AFTER exit check: tsl = max(tsl, bar.low) for LONG each bar
      3. EOD:      bar.timestamp.time() >= 15:15 → exit at bar.open
    """
    from datetime import time as _time

    trades: List[Trade] = []
    monitors: List[_Monitor] = []
    known_bear: Set[datetime] = set()
    known_bull: Set[datetime] = set()
    tweak_setups: List[_TweakSetup] = []

    h1_by_date: Dict[date, List[_Bar]] = {}
    for b in h1_bars:
        h1_by_date.setdefault(b.timestamp.date(), []).append(b)

    eod_t = _time(15, 15)
    cut_t = _time(14, 0)
    sess_t = _time(9, 15)

    for today in sorted(h1_by_date):
        # D1 bars closed BEFORE today
        d1_avail = [b for b in d1_bars if b.timestamp.date() < today]

        # Discover new zones
        if len(d1_avail) >= 3:
            age_cutoff = (datetime.combine(today, datetime.min.time()).replace(tzinfo=IST)
                          - timedelta(days=MAX_ZONE_DAYS))
            for z in find_all_bear_zones(d1_avail):
                if z.reference_low_ts not in known_bear:
                    if z.lock_ts >= age_cutoff:
                        known_bear.add(z.reference_low_ts)
                        monitors.append(_Monitor(
                            direction="LONG",
                            d1_ref_ts=z.reference_low_ts,
                            d1_sweep_ts=z.sweep_started_ts,
                            d1_reclaim_ts=z.lock_ts,
                            zone_lo=min(z.entry_line, z.sweep_low),
                            zone_hi=max(z.entry_line, z.sweep_low),
                        ))
            for z in find_all_bull_zones(d1_avail):
                if z.reference_low_ts not in known_bull:
                    if z.lock_ts >= age_cutoff:
                        known_bull.add(z.reference_low_ts)
                        monitors.append(_Monitor(
                            direction="SHORT",
                            d1_ref_ts=z.reference_low_ts,
                            d1_sweep_ts=z.sweep_started_ts,
                            d1_reclaim_ts=z.lock_ts,
                            zone_lo=min(z.entry_line, z.sweep_low),
                            zone_hi=max(z.entry_line, z.sweep_low),
                        ))

        # Evict done/stale zones
        age_cutoff = (datetime.combine(today, datetime.min.time()).replace(tzinfo=IST)
                      - timedelta(days=MAX_ZONE_DAYS))
        monitors = [m for m in monitors if not m.done and not m.invalid
                    and m.d1_reclaim_ts >= age_cutoff]
        tweak_setups = [t for t in tweak_setups if not t.done
                        and t.d1_reclaim_ts >= age_cutoff]

        day_bars = sorted(h1_by_date[today], key=lambda b: b.timestamp)
        open_trade: Optional[Trade] = None
        prev_bar: Optional[_Bar] = None

        for bar in day_bars:
            hm = (bar.timestamp.hour, bar.timestamp.minute)
            if hm < SESSION_OPEN:
                prev_bar = bar
                continue

            # EOD exit
            if bar.timestamp.time() >= eod_t:
                if open_trade is not None:
                    pnl = (bar.open - open_trade.entry if open_trade.direction == "LONG"
                           else open_trade.entry - bar.open)
                    open_trade.exit_ts = bar.timestamp
                    open_trade.exit_price = bar.open
                    open_trade.exit_reason = "EOD"
                    open_trade.pnl_pts = pnl
                    trades.append(open_trade)
                    open_trade = None
                prev_bar = bar
                break

            # Check exits for open trade BEFORE TSL ratchet
            if open_trade is not None:
                pos = open_trade
                hard_hit = (
                    (pos.direction == "LONG"  and bar.low  <= pos.sl)
                    or (pos.direction == "SHORT" and bar.high >= pos.sl)
                )
                tsl_hit = (
                    (pos.direction == "LONG"  and bar.close < pos.tsl_level)
                    or (pos.direction == "SHORT" and bar.close > pos.tsl_level)
                )
                if hard_hit:
                    pnl = (pos.sl - pos.entry if pos.direction == "LONG"
                           else pos.entry - pos.sl)
                    pos.exit_ts = bar.timestamp
                    pos.exit_price = pos.sl
                    pos.exit_reason = "SL"
                    pos.pnl_pts = pnl
                    trades.append(open_trade)
                    open_trade = None
                elif tsl_hit:
                    pnl = (pos.tsl_level - pos.entry if pos.direction == "LONG"
                           else pos.entry - pos.tsl_level)
                    pos.exit_ts = bar.timestamp
                    pos.exit_price = pos.tsl_level
                    pos.exit_reason = "TSL"
                    pos.pnl_pts = pnl
                    trades.append(open_trade)
                    open_trade = None
                else:
                    # Ratchet TSL from this bar
                    if pos.direction == "LONG":
                        pos.tsl_level = max(pos.tsl_level, bar.low)
                    else:
                        pos.tsl_level = min(pos.tsl_level, bar.high)

            # No new entry after cutoff or while in trade
            if open_trade is not None or bar.timestamp.time() >= cut_t:
                prev_bar = bar
                continue

            # ── C2: zone monitoring + ref candle breach ────────────────────
            for m in monitors:
                if m.done or m.invalid:
                    continue

                if m.state == "WAITING":
                    if m.direction == "LONG" and bar.low <= m.zone_hi:
                        m.state = "MONITORING"
                        m.zone_entry_ts = bar.timestamp
                        m.ref_bar = bar
                    elif m.direction == "SHORT" and bar.high >= m.zone_lo:
                        m.state = "MONITORING"
                        m.zone_entry_ts = bar.timestamp
                        m.ref_bar = bar

                elif m.state == "MONITORING" and m.ref_bar is not None:
                    ref = m.ref_bar
                    was_monitoring = True

                    # Invalidation: 1H closes through far zone boundary
                    if m.direction == "LONG" and bar.close < m.zone_lo:
                        m.invalid = True
                        # TWEAK: enter SHORT on next 1H breach of this failure bar
                        if m.zone_entry_ts is not None:
                            tweak_setups.append(_TweakSetup(
                                trade_dir="SHORT", failure_bar=bar,
                                zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                                d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                                d1_reclaim_ts=m.d1_reclaim_ts,
                                zone_entry_ts=m.zone_entry_ts,
                            ))
                        continue
                    elif m.direction == "SHORT" and bar.close > m.zone_hi:
                        m.invalid = True
                        if m.zone_entry_ts is not None:
                            tweak_setups.append(_TweakSetup(
                                trade_dir="LONG", failure_bar=bar,
                                zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                                d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                                d1_reclaim_ts=m.d1_reclaim_ts,
                                zone_entry_ts=m.zone_entry_ts,
                            ))
                        continue

                    # C2 breach
                    if m.direction == "LONG" and bar.high > ref.high:
                        entry, sl = ref.high, ref.low
                        risk = entry - sl
                        if risk > 0:
                            open_trade = Trade(
                                symbol=symbol, direction="LONG",
                                entry_ts=bar.timestamp, entry=entry, sl=sl,
                                tsl_level=sl,   # TSL starts at hard SL
                                zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                                source="C2",
                            )
                            m.done = True
                            break
                        m.ref_bar = bar
                    elif m.direction == "SHORT" and bar.low < ref.low:
                        entry, sl = ref.low, ref.high
                        risk = sl - entry
                        if risk > 0:
                            open_trade = Trade(
                                symbol=symbol, direction="SHORT",
                                entry_ts=bar.timestamp, entry=entry, sl=sl,
                                tsl_level=sl,
                                zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                                source="C2",
                            )
                            m.done = True
                            break
                        m.ref_bar = bar
                    else:
                        m.ref_bar = bar  # roll forward

            # ── TWEAK: entry on breach of failure bar ───────────────────────
            if open_trade is None:
                for ts in tweak_setups:
                    if ts.done or bar.timestamp <= ts.failure_bar.timestamp:
                        continue
                    ref = ts.failure_bar
                    if ts.trade_dir == "SHORT" and bar.low < ref.low:
                        sl_p = ref.high
                        entry = ref.low
                        if sl_p - entry > 0:
                            open_trade = Trade(
                                symbol=symbol, direction="SHORT",
                                entry_ts=bar.timestamp, entry=entry, sl=sl_p,
                                tsl_level=sl_p,
                                zone_lo=ts.zone_lo, zone_hi=ts.zone_hi,
                                source="TWEAK",
                            )
                        ts.done = True
                        break
                    elif ts.trade_dir == "LONG" and bar.high > ref.high:
                        sl_p = ref.low
                        entry = ref.high
                        if entry - sl_p > 0:
                            open_trade = Trade(
                                symbol=symbol, direction="LONG",
                                entry_ts=bar.timestamp, entry=entry, sl=sl_p,
                                tsl_level=sl_p,
                                zone_lo=ts.zone_lo, zone_hi=ts.zone_hi,
                                source="TWEAK",
                            )
                        ts.done = True
                        break

            prev_bar = bar

        # End of day: reset MONITORING state (no cross-day ref candles)
        for m in monitors:
            if m.state == "MONITORING":
                m.state = "WAITING"
                m.ref_bar = None
                m.zone_entry_ts = None

        # Force-close any open trade if loop exited without hitting EOD bar
        if open_trade is not None:
            last = day_bars[-1]
            pnl = (last.close - open_trade.entry if open_trade.direction == "LONG"
                   else open_trade.entry - last.close)
            open_trade.exit_ts = last.timestamp
            open_trade.exit_price = last.close
            open_trade.exit_reason = "EOD"
            open_trade.pnl_pts = pnl
            trades.append(open_trade)

    return trades


# ── Summary helpers ───────────────────────────────────────────────────────────

def _summary_row(symbol: str, trades: List[Trade]) -> dict:
    if not trades:
        return {
            "Symbol": symbol, "Trades": 0, "Win": 0, "Loss": 0,
            "WinRate": "-", "TotalPts": "-", "TotalRs": "-",
            "AvgPts": "-", "BestPts": "-", "WorstPts": "-",
            "C2": 0, "TWEAK": 0,
        }
    pts = [t.pnl_pts for t in trades if t.pnl_pts is not None]
    wins = [p for p in pts if p > 0]
    losses = [p for p in pts if p <= 0]
    rs = [t.pnl_rs for t in trades if t.pnl_pts is not None]
    return {
        "Symbol":   symbol,
        "Trades":   len(pts),
        "Win":      len(wins),
        "Loss":     len(losses),
        "WinRate":  f"{100*len(wins)/len(pts):.0f}%" if pts else "-",
        "TotalPts": f"{sum(pts):+.1f}",
        "TotalRs":  f"{sum(rs):+.0f}",
        "AvgPts":   f"{sum(pts)/len(pts):.1f}" if pts else "-",
        "BestPts":  f"{max(pts):.1f}" if pts else "-",
        "WorstPts": f"{min(pts):.1f}" if pts else "-",
        "C2":       sum(1 for t in trades if t.source == "C2"),
        "TWEAK":    sum(1 for t in trades if t.source == "TWEAK"),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="D1 Trap + MTF C2/TWEAK backtest on FnO stocks")
    ap.add_argument("--months", type=int, default=3, help="Months of simulation (default 3)")
    ap.add_argument("--stocks", default="", help="Comma-separated symbols (default: all 30)")
    ap.add_argument("--token", default="", help="Upstox access token (or set UPSTOX_TOKEN env)")
    ap.add_argument("--mtf", type=int, default=75, help="MTF minutes: 60=1H, 75=75min (default 75)")
    args = ap.parse_args()

    token = args.token or os.environ.get("UPSTOX_TOKEN", "")
    if not token:
        # Try to load from DB
        try:
            sys.path.insert(0, ".")
            from data_layer.client_db import ClientDB
            import asyncio
            db = ClientDB()
            asyncio.run(db.initialise())
            creds = db.get_feeder_creds_sync("upstox") or {}
            token = creds.get("access_token", "")
            if token:
                print("[Token] loaded from data/clients.db")
        except Exception:
            pass

    if not token:
        print("[ERROR] No Upstox token. Pass --token <TOKEN> or set UPSTOX_TOKEN env var.")
        sys.exit(1)

    symbols = [s.strip().upper() for s in args.stocks.split(",") if s.strip()] or list(TOP_30)
    today = datetime.now(IST).date()
    sim_start = today - timedelta(days=args.months * 31)
    d1_start  = sim_start - timedelta(days=200)   # need historical context for D1 zones

    mtf_mins = args.mtf
    print(f"\nD1 Trap FnO Stocks Backtest  (D1 zones + {mtf_mins}M MTF C2/TWEAK)")
    print(f"Simulation: {sim_start} to {today}  ({args.months} months)")
    print(f"Stocks: {len(symbols)}")
    print(f"D1 zone context from: {d1_start}\n")

    all_trades: List[Trade] = []
    summaries: List[dict] = []

    for sym in symbols:
        if sym not in TOP_30:
            print(f"[SKIP] {sym} — not in TOP_30 list")
            continue

        print(f"\n-- {sym} --")

        # Fetch D1 bars (zone building context)
        d1_bars = _fetch_d1_from_fno_cache(sym, d1_start, today, token)
        if len(d1_bars) < 10:
            print(f"  [SKIP] {sym}: not enough D1 bars ({len(d1_bars)})")
            summaries.append({"Symbol": sym, "Trades": 0, "Note": "no D1 data"})
            continue

        # Fetch 30-min intraday for simulation period
        print(f"  Fetching 30-min intraday ({sim_start}..{today})…")
        m30_bars = fetch_intraday(sym, sim_start, today, token, interval="30minute")
        if len(m30_bars) < 10:
            print(f"  [SKIP] {sym}: not enough 30-min bars ({len(m30_bars)})")
            summaries.append({"Symbol": sym, "Trades": 0, "Note": "no intraday data"})
            continue

        # Resample to MTF (75min by default)
        mtf_bars = resample_to_Nm(m30_bars, mtf_mins)
        # Only simulate from sim_start onwards
        mtf_bars = [b for b in mtf_bars if b.timestamp.date() >= sim_start]
        print(f"  D1 bars: {len(d1_bars)}, 30m bars: {len(m30_bars)}, {mtf_mins}M bars (sim): {len(mtf_bars)}")

        trades = run_c2_tweak_tsl(sym, d1_bars, mtf_bars)
        all_trades.extend(trades)

        row = _summary_row(sym, trades)
        summaries.append(row)

        if trades:
            pts = [t.pnl_pts for t in trades if t.pnl_pts is not None]
            rs = [t.pnl_rs for t in trades if t.pnl_pts is not None]
            print(f"  Trades: {len(pts)}  Win: {sum(1 for p in pts if p>0)}/"
                  f"{len(pts)}  Total: {sum(pts):+.1f} pts  Rs{sum(rs):+,.0f}")
        else:
            print(f"  No trades.")

    # ── Save trades CSV ────────────────────────────────────────────────────────
    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)

    trades_csv = out_dir / f"trades_{today}.csv"
    if all_trades:
        with open(trades_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(all_trades[0].as_row().keys()))
            w.writeheader()
            w.writerows(t.as_row() for t in all_trades)
        print(f"\nTrades saved: {trades_csv}  ({len(all_trades)} rows)")

    # ── Print summary table ────────────────────────────────────────────────────
    print(f"\n{'='*80}")
    print(f"D1 Trap C2+TWEAK ({mtf_mins}M MTF) -- {args.months}-Month Backtest Summary ({sim_start} to {today})")
    print(f"{'='*80}")
    hdr = f"{'Symbol':<12} {'T':>4} {'W':>4} {'L':>4} {'WR':>6} {'PnL_pts':>9} {'PnL_Rs':>11} {'Avg':>7} {'Best':>7} {'Worst':>7} {'C2':>4} {'TW':>4}"
    print(hdr)
    print("-" * 80)

    total_pts = 0.0
    total_rs  = 0.0
    total_t = total_w = total_l = 0

    for s in summaries:
        if "Note" in s:
            print(f"  {s['Symbol']:<12}  — {s.get('Note','')}")
            continue
        n = int(s["Trades"] or 0)
        total_t += n
        w = int(s.get("Win") or 0)
        total_w += w
        l = int(s.get("Loss") or 0)
        total_l += l
        pts_str = s.get("TotalPts", "")
        rs_str  = s.get("TotalRs", "")
        try:
            total_pts += float(pts_str)
            total_rs  += float(rs_str)
        except Exception:
            pass
        print(f"  {s['Symbol']:<12} {n:>4} {w:>4} {l:>4} "
              f"{s.get('WinRate',''):>6} {pts_str:>9} {rs_str:>11} "
              f"{s.get('AvgPts',''):>7} {s.get('BestPts',''):>7} {s.get('WorstPts',''):>7} "
              f"{s.get('C2',0):>4} {s.get('TWEAK',0):>4}")

    print("-" * 80)
    wr = f"{100*total_w/total_t:.0f}%" if total_t else "-"
    print(f"  {'TOTAL':<12} {total_t:>4} {total_w:>4} {total_l:>4} "
          f"{wr:>6} {total_pts:>+9.1f} {total_rs:>+11,.0f}")
    print(f"{'='*80}")

    # Save summary CSV
    if summaries:
        summ_csv = out_dir / f"summary_{today}.csv"
        clean = [s for s in summaries if "Note" not in s]
        if clean:
            with open(summ_csv, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(clean[0].keys()))
                w.writeheader()
                w.writerows(clean)
            print(f"Summary saved → {summ_csv}")


if __name__ == "__main__":
    main()
