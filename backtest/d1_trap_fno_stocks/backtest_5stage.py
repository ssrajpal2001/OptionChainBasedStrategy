"""
backtest/d1_trap_fno_stocks/backtest_5stage.py — D1 + 75M C2 → 30M LTF trigger (Mode B)

Five stages:
  1. D1 bear/bull trap zone  (3-candle sweep + reclaim)
  2. 75M ref candle inside the zone  (lowest-low or highest-high bar while in zone)
  3. Next 75M candle breaches ref candle high/low  → _PendingTrigger set
  4. First 30M bar AFTER the C2 bar that crosses trigger level  → ENTRY
  5. TSL ratchets on 30M bar close; hard SL = ref candle extreme

Compare with backtest.py (Mode A) where entry fires on the 75M C2 bar itself.

Usage:
  python backtest/d1_trap_fno_stocks/backtest_5stage.py
  python backtest/d1_trap_fno_stocks/backtest_5stage.py --months 3
  python backtest/d1_trap_fno_stocks/backtest_5stage.py --stocks RELIANCE,HDFCBANK
  UPSTOX_TOKEN=<token> python backtest/d1_trap_fno_stocks/backtest_5stage.py --months 3
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import quote as _q

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.global_config import IST
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones

# ── Constants ──────────────────────────────────────────────────────────────────

SESSION_OPEN  = (9, 15)
EOD_HM        = (15, 15)
ENTRY_CUTOFF  = (14, 0)
MAX_ZONE_DAYS = 20
MTF_MINS      = 75
LTF_MINS      = 5    # resampled from 1-minute bars

_FNO_D1_CACHE = Path(__file__).resolve().parents[2] / "backtest" / "fno_scanner" / "data_cache"
_INTRA_CACHE  = Path(__file__).parent / "data_cache"

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
    "TATAMOTOR":  "NSE_EQ|INE155A01022",
    "SUNPHARMA":  "NSE_EQ|INE044A01036",
    "DRREDDY":    "NSE_EQ|INE089A01031",
    "CIPLA":      "NSE_EQ|INE059A01026",
    "JSWSTEEL":   "NSE_EQ|INE019A01038",
    "ADANIENT":   "NSE_EQ|INE423A01024",
    "TATACONSUM": "NSE_EQ|INE192A01025",
    "BAJAJFINSV": "NSE_EQ|INE918I01026",
}

LOT_SIZES: Dict[str, int] = {
    "RELIANCE": 500, "HDFCBANK": 650, "ICICIBANK": 700, "INFY": 400,
    "TCS": 225, "BHARTIARTL": 475, "SBIN": 750, "ITC": 1725,
    "WIPRO": 3000, "BAJFINANCE": 750, "LT": 175, "AXISBANK": 625,
    "KOTAKBANK": 2000, "HCLTECH": 400, "TITAN": 175, "MARUTI": 50,
    "NTPC": 1500, "TATASTEEL": 2750, "HINDALCO": 700, "GRASIM": 250,
    "ONGC": 2250, "COALINDIA": 1350, "TATAMOTOR": 1600, "SUNPHARMA": 350,
    "DRREDDY": 625, "CIPLA": 425, "JSWSTEEL": 675, "ADANIENT": 309,
    "TATACONSUM": 550, "BAJAJFINSV": 300,
}


# ── Bar ────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class _Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float


# ── State dataclasses ──────────────────────────────────────────────────────────

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
class _PendingTrigger:
    direction: str
    trigger_level: float
    sl: float
    zone_lo: float
    zone_hi: float
    source: str            # "C2" | "TWEAK"
    set_at: datetime       # timestamp of the 75M C2 bar — entry only fires on LATER 30M bars


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
    source: str
    exit_ts: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: str = ""
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


# ── Fetch helpers (same as backtest.py) ───────────────────────────────────────

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
    for c in reversed(rows):
        try:
            ts = datetime.fromisoformat(c[0]).astimezone(IST)
            out.append(_Bar(timestamp=ts, open=float(c[1]), high=float(c[2]),
                            low=float(c[3]), close=float(c[4])))
        except Exception:
            pass
    return out


def _fetch_d1_from_fno_cache(symbol: str, start: date, end: date, token: str) -> List[_Bar]:
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
            if bars and bars[0].timestamp.date() <= start and bars[-1].timestamp.date() >= end:
                print(f"  [D1 cache] {symbol}: {len(bars)} bars from fno_scanner cache")
                return bars
        except Exception:
            pass

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
    seen: Set[datetime] = set()
    out: List[_Bar] = []
    for b in sorted(all_bars, key=lambda b: b.timestamp):
        if b.timestamp not in seen:
            seen.add(b.timestamp)
            out.append(b)
    return out


# ── Resample ───────────────────────────────────────────────────────────────────

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


def _group_by_date(bars: List[_Bar]) -> Dict[date, List[_Bar]]:
    d: Dict[date, List[_Bar]] = {}
    for b in bars:
        d.setdefault(b.timestamp.date(), []).append(b)
    return d


# ── Core 5-stage simulation ────────────────────────────────────────────────────

def run_5stage(symbol: str, d1_bars: List[_Bar], bars_5m: List[_Bar],
               mtf_mins: int = MTF_MINS) -> List[Trade]:
    """
    Stage 1: D1 zone
    Stage 2: 75M ref candle in zone
    Stage 3: 75M C2 breach → _PendingTrigger
    Stage 4: Next 5M bar crossing trigger_level → ENTRY
    Stage 5: TSL ratchets on 75M bar close; hard SL = ref candle extreme
    """
    bars_mtf = resample_to_Nm(bars_5m, mtf_mins)

    m5_by_date  = _group_by_date(bars_5m)
    mtf_by_date = _group_by_date(bars_mtf)

    monitors: List[_Monitor] = []
    known_bear: Set[datetime] = set()
    known_bull: Set[datetime] = set()
    tweak_setups: List[_TweakSetup] = []
    trades: List[Trade] = []

    age_cutoff_base = timedelta(days=MAX_ZONE_DAYS)

    for today in sorted(m5_by_date):
        # D1 zone discovery (using all D1 bars before today)
        d1_avail = [b for b in d1_bars if b.timestamp.date() < today]
        if len(d1_avail) >= 3:
            age_cutoff = (datetime.combine(today, datetime.min.time()).replace(tzinfo=IST)
                          - age_cutoff_base)
            for z in find_all_bear_zones(d1_avail):
                if z.reference_low_ts not in known_bear and z.lock_ts >= age_cutoff:
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
                if z.reference_low_ts not in known_bull and z.lock_ts >= age_cutoff:
                    known_bull.add(z.reference_low_ts)
                    monitors.append(_Monitor(
                        direction="SHORT",
                        d1_ref_ts=z.reference_low_ts,
                        d1_sweep_ts=z.sweep_started_ts,
                        d1_reclaim_ts=z.lock_ts,
                        zone_lo=min(z.entry_line, z.sweep_low),
                        zone_hi=max(z.entry_line, z.sweep_low),
                    ))

        # Evict done/stale
        age_cutoff = (datetime.combine(today, datetime.min.time()).replace(tzinfo=IST)
                      - age_cutoff_base)
        monitors = [m for m in monitors if not m.done and not m.invalid
                    and m.d1_reclaim_ts >= age_cutoff]
        tweak_setups = [t for t in tweak_setups if not t.done
                        and t.d1_reclaim_ts >= age_cutoff]

        today_5m  = sorted(m5_by_date.get(today, []), key=lambda b: b.timestamp)
        today_mtf = sorted(mtf_by_date.get(today, []), key=lambda b: b.timestamp)

        if not today_5m:
            continue

        next_mtf_idx = 0
        open_trade: Optional[Trade] = None
        pending: Optional[_PendingTrigger] = None

        # Reset MONITORING state — ref candles don't carry over to next day
        for m in monitors:
            if m.state == "MONITORING":
                m.state = "WAITING"
                m.ref_bar = None
                m.zone_entry_ts = None

        for bar5 in today_5m:
            hm = (bar5.timestamp.hour, bar5.timestamp.minute)

            # Skip pre-session bars
            if hm < SESSION_OPEN:
                continue

            # EOD exit
            if hm >= EOD_HM:
                if open_trade is not None:
                    pnl = (bar5.open - open_trade.entry if open_trade.direction == "LONG"
                           else open_trade.entry - bar5.open)
                    open_trade.exit_ts = bar5.timestamp
                    open_trade.exit_price = bar5.open
                    open_trade.exit_reason = "EOD"
                    open_trade.pnl_pts = pnl
                    trades.append(open_trade)
                    open_trade = None
                break

            bar5_end = bar5.timestamp + timedelta(minutes=LTF_MINS)

            # ── Process any 75M bars that closed during this 5M bar ─────────
            while next_mtf_idx < len(today_mtf):
                bar_mtf = today_mtf[next_mtf_idx]
                mtf_end = bar_mtf.timestamp + timedelta(minutes=mtf_mins)
                if mtf_end > bar5_end:
                    break

                # TSL ratchet on MTF close
                if open_trade is not None:
                    if open_trade.direction == "LONG":
                        open_trade.tsl_level = max(open_trade.tsl_level, bar_mtf.low)
                    else:
                        open_trade.tsl_level = min(open_trade.tsl_level, bar_mtf.high)

                # C2/TWEAK detection — only when no open trade and no pending
                mtf_hm = (bar_mtf.timestamp.hour, bar_mtf.timestamp.minute)
                if open_trade is None and pending is None and mtf_hm < ENTRY_CUTOFF:
                    pending = _process_mtf_for_pending(bar_mtf, monitors, tweak_setups)

                next_mtf_idx += 1

            # ── 5M exit: check SL / TSL ─────────────────────────────────────
            if open_trade is not None:
                pos = open_trade
                hard_hit = (
                    (pos.direction == "LONG"  and bar5.low  <= pos.sl)
                    or (pos.direction == "SHORT" and bar5.high >= pos.sl)
                )
                tsl_hit = (
                    (pos.direction == "LONG"  and bar5.close < pos.tsl_level)
                    or (pos.direction == "SHORT" and bar5.close > pos.tsl_level)
                )
                if hard_hit:
                    pnl = (pos.sl - pos.entry if pos.direction == "LONG"
                           else pos.entry - pos.sl)
                    pos.exit_ts = bar5.timestamp
                    pos.exit_price = pos.sl
                    pos.exit_reason = "SL"
                    pos.pnl_pts = pnl
                    trades.append(open_trade)
                    open_trade = None
                elif tsl_hit:
                    pnl = (pos.tsl_level - pos.entry if pos.direction == "LONG"
                           else pos.entry - pos.tsl_level)
                    pos.exit_ts = bar5.timestamp
                    pos.exit_price = pos.tsl_level
                    pos.exit_reason = "TSL"
                    pos.pnl_pts = pnl
                    trades.append(open_trade)
                    open_trade = None

            # ── 5M entry: fire pending trigger ──────────────────────────────
            # Only trigger on 5M bars AFTER the 75M C2 bar closed (pending.set_at)
            if (pending is not None and open_trade is None
                    and hm < ENTRY_CUTOFF
                    and bar5.timestamp > pending.set_at):
                trig = pending
                if trig.direction == "LONG" and bar5.high >= trig.trigger_level:
                    open_trade = Trade(
                        symbol=symbol, direction="LONG",
                        entry_ts=bar5.timestamp, entry=trig.trigger_level,
                        sl=trig.sl, tsl_level=trig.sl,
                        zone_lo=trig.zone_lo, zone_hi=trig.zone_hi,
                        source=trig.source,
                    )
                    pending = None
                elif trig.direction == "SHORT" and bar5.low <= trig.trigger_level:
                    open_trade = Trade(
                        symbol=symbol, direction="SHORT",
                        entry_ts=bar5.timestamp, entry=trig.trigger_level,
                        sl=trig.sl, tsl_level=trig.sl,
                        zone_lo=trig.zone_lo, zone_hi=trig.zone_hi,
                        source=trig.source,
                    )
                    pending = None
                elif (bar5.timestamp - trig.set_at).total_seconds() > 7200:
                    # Expire pending after 2 hours (didn't retrace to trigger level)
                    pending = None

        # Force-close any open trade if EOD bar wasn't hit
        if open_trade is not None:
            last = today_5m[-1]
            pnl = (last.close - open_trade.entry if open_trade.direction == "LONG"
                   else open_trade.entry - last.close)
            open_trade.exit_ts = last.timestamp
            open_trade.exit_price = last.close
            open_trade.exit_reason = "EOD"
            open_trade.pnl_pts = pnl
            trades.append(open_trade)

    return trades


def _process_mtf_for_pending(
    bar_mtf: _Bar,
    monitors: List[_Monitor],
    tweak_setups: List[_TweakSetup],
) -> Optional[_PendingTrigger]:
    """
    Process a closed MTF bar: update monitor states AND return a _PendingTrigger
    if C2 or TWEAK fires. Entry fires on the NEXT 5M bar (pending.set_at guard).
    """
    hm = (bar_mtf.timestamp.hour, bar_mtf.timestamp.minute)
    if hm >= ENTRY_CUTOFF:
        return None

    # ── Zone WAITING → MONITORING transitions ─────────────────────────────────
    for m in monitors:
        if m.done or m.invalid or m.state != "WAITING":
            continue
        if m.direction == "LONG" and bar_mtf.low <= m.zone_hi:
            m.state = "MONITORING"
            m.zone_entry_ts = bar_mtf.timestamp
            m.ref_bar = bar_mtf
        elif m.direction == "SHORT" and bar_mtf.high >= m.zone_lo:
            m.state = "MONITORING"
            m.zone_entry_ts = bar_mtf.timestamp
            m.ref_bar = bar_mtf

    # ── MONITORING: check C2 breach / TWEAK / roll ref_bar ───────────────────
    for m in monitors:
        if m.done or m.invalid or m.state != "MONITORING" or m.ref_bar is None:
            continue
        ref = m.ref_bar

        # Zone invalidation → TWEAK setup
        if m.direction == "LONG" and bar_mtf.close < m.zone_lo:
            m.invalid = True
            if m.zone_entry_ts is not None:
                tweak_setups.append(_TweakSetup(
                    trade_dir="SHORT", failure_bar=bar_mtf,
                    zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                    d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                    d1_reclaim_ts=m.d1_reclaim_ts,
                    zone_entry_ts=m.zone_entry_ts,
                ))
            continue
        elif m.direction == "SHORT" and bar_mtf.close > m.zone_hi:
            m.invalid = True
            if m.zone_entry_ts is not None:
                tweak_setups.append(_TweakSetup(
                    trade_dir="LONG", failure_bar=bar_mtf,
                    zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                    d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                    d1_reclaim_ts=m.d1_reclaim_ts,
                    zone_entry_ts=m.zone_entry_ts,
                ))
            continue

        # C2 breach → _PendingTrigger (entry fires on next 5M bar)
        if m.direction == "LONG" and bar_mtf.high > ref.high:
            risk = ref.high - ref.low
            if risk > 0:
                m.done = True
                return _PendingTrigger(
                    direction="LONG",
                    trigger_level=ref.high,
                    sl=ref.low,
                    zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                    source="C2",
                    set_at=bar_mtf.timestamp,
                )
            m.ref_bar = bar_mtf  # zero-risk bar — roll forward
        elif m.direction == "SHORT" and bar_mtf.low < ref.low:
            risk = ref.high - ref.low
            if risk > 0:
                m.done = True
                return _PendingTrigger(
                    direction="SHORT",
                    trigger_level=ref.low,
                    sl=ref.high,
                    zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                    source="C2",
                    set_at=bar_mtf.timestamp,
                )
            m.ref_bar = bar_mtf
        else:
            # Still in zone — roll ref_bar to worst-case
            if m.direction == "LONG":
                m.ref_bar = bar_mtf if bar_mtf.low < ref.low else ref
            else:
                m.ref_bar = bar_mtf if bar_mtf.high > ref.high else ref

    # ── TWEAK breach → _PendingTrigger ───────────────────────────────────────
    for ts in tweak_setups:
        if ts.done or bar_mtf.timestamp <= ts.failure_bar.timestamp:
            continue
        ref = ts.failure_bar
        if ts.trade_dir == "SHORT" and bar_mtf.low < ref.low:
            risk = ref.high - ref.low
            ts.done = True
            if risk > 0:
                return _PendingTrigger(
                    direction="SHORT",
                    trigger_level=ref.low,
                    sl=ref.high,
                    zone_lo=ts.zone_lo, zone_hi=ts.zone_hi,
                    source="TWEAK",
                    set_at=bar_mtf.timestamp,
                )
        elif ts.trade_dir == "LONG" and bar_mtf.high > ref.high:
            risk = ref.high - ref.low
            ts.done = True
            if risk > 0:
                return _PendingTrigger(
                    direction="LONG",
                    trigger_level=ref.high,
                    sl=ref.low,
                    zone_lo=ts.zone_lo, zone_hi=ts.zone_hi,
                    source="TWEAK",
                    set_at=bar_mtf.timestamp,
                )

    return None


# ── Summary helpers ────────────────────────────────────────────────────────────

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
    rs = [t.pnl_rs for t in trades if t.pnl_pts is not None]
    return {
        "Symbol":   symbol,
        "Trades":   len(pts),
        "Win":      len(wins),
        "Loss":     len(pts) - len(wins),
        "WinRate":  f"{100*len(wins)/len(pts):.0f}%" if pts else "-",
        "TotalPts": f"{sum(pts):+.1f}",
        "TotalRs":  f"{sum(rs):+.0f}",
        "AvgPts":   f"{sum(pts)/len(pts):.1f}" if pts else "-",
        "BestPts":  f"{max(pts):.1f}" if pts else "-",
        "WorstPts": f"{min(pts):.1f}" if pts else "-",
        "C2":       sum(1 for t in trades if t.source == "C2"),
        "TWEAK":    sum(1 for t in trades if t.source == "TWEAK"),
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="D1 Trap + 75M C2 → 30M LTF trigger backtest (Mode B / 5-stage)")
    ap.add_argument("--months", type=int, default=3, help="Months of simulation (default 3)")
    ap.add_argument("--stocks", default="", help="Comma-separated symbols (default: all 30)")
    ap.add_argument("--token", default="", help="Upstox access token")
    ap.add_argument("--mtf", type=int, default=75, help="MTF minutes (default 75)")
    args = ap.parse_args()

    token = args.token or os.environ.get("UPSTOX_TOKEN", "")
    if not token:
        try:
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

    _INTRA_CACHE.mkdir(parents=True, exist_ok=True)

    symbols = [s.strip().upper() for s in args.stocks.split(",") if s.strip()] or list(TOP_30)
    today = datetime.now(IST).date()
    sim_start = today - timedelta(days=args.months * 31)
    d1_start  = sim_start - timedelta(days=200)
    mtf_mins  = args.mtf

    print(f"\nD1 Trap FnO Stocks — 5-Stage Backtest (Mode B)")
    print(f"  Stage 1: D1 zone  |  Stage 2: {mtf_mins}M ref candle  |  Stage 3: {mtf_mins}M C2")
    print(f"  Stage 4: 5M LTF entry confirmation  |  Stage 5: 75M TSL ratchet")
    print(f"Simulation: {sim_start} to {today}  ({args.months} months)")
    print(f"Stocks: {len(symbols)}")
    print(f"(Compare with backtest.py Mode A: enters on {mtf_mins}M C2 bar directly)\n")

    all_trades: List[Trade] = []
    summaries: List[dict] = []

    for sym in symbols:
        if sym not in TOP_30:
            print(f"[SKIP] {sym} — not in TOP_30 list")
            continue

        print(f"\n-- {sym} --")

        d1_bars = _fetch_d1_from_fno_cache(sym, d1_start, today, token)
        if len(d1_bars) < 10:
            print(f"  [SKIP] {sym}: not enough D1 bars ({len(d1_bars)})")
            summaries.append({"Symbol": sym, "Trades": 0, "Note": "no D1 data"})
            continue

        print(f"  Fetching 1-min intraday ({sim_start}..{today})…")
        m1_bars = fetch_intraday(sym, sim_start, today, token, interval="1minute")
        if len(m1_bars) < 10:
            print(f"  [SKIP] {sym}: not enough 1-min bars ({len(m1_bars)})")
            summaries.append({"Symbol": sym, "Trades": 0, "Note": "no intraday data"})
            continue

        m1_sim  = [b for b in m1_bars if b.timestamp.date() >= sim_start]
        m5_sim  = resample_to_Nm(m1_sim, LTF_MINS)  # 1M → 5M
        print(f"  D1 bars: {len(d1_bars)}, 1m bars: {len(m1_sim)}, 5m bars: {len(m5_sim)}")

        sym_trades = run_5stage(sym, d1_bars, m5_sim, mtf_mins=mtf_mins)
        all_trades.extend(sym_trades)
        row = _summary_row(sym, sym_trades)
        summaries.append(row)

        if sym_trades:
            pts = [t.pnl_pts for t in sym_trades if t.pnl_pts is not None]
            rs  = [t.pnl_rs for t in sym_trades if t.pnl_pts is not None]
            print(f"  Trades: {len(pts)}  Win: {sum(1 for p in pts if p>0)}/"
                  f"{len(pts)}  Total: {sum(pts):+.1f} pts  Rs{sum(rs):+,.0f}")
        else:
            print(f"  No trades.")

    # Save trades CSV
    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    trades_csv = out_dir / f"trades_5stage_5m_{today}.csv"
    if all_trades:
        with open(trades_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(all_trades[0].as_row().keys()))
            w.writeheader()
            w.writerows(t.as_row() for t in all_trades)
        print(f"\nTrades saved: {trades_csv}  ({len(all_trades)} rows)")

    # Print summary table
    print(f"\n{'='*80}")
    print(f"D1 Trap 5-Stage ({mtf_mins}M C2 + 5M LTF trigger) — {args.months}-Month Summary")
    print(f"{'='*80}")
    hdr = (f"{'Symbol':<12} {'T':>4} {'W':>4} {'L':>4} {'WR':>6} "
           f"{'PnL_pts':>9} {'PnL_Rs':>11} {'Avg':>7} {'Best':>7} {'Worst':>7} "
           f"{'C2':>4} {'TW':>4}")
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
        lo = int(s.get("Loss") or 0)
        total_l += lo
        pts_str = s.get("TotalPts", "")
        rs_str  = s.get("TotalRs", "")
        try:
            total_pts += float(pts_str)
            total_rs  += float(rs_str)
        except Exception:
            pass
        print(f"  {s['Symbol']:<12} {n:>4} {w:>4} {lo:>4} "
              f"{s.get('WinRate',''):>6} {pts_str:>9} {rs_str:>11} "
              f"{s.get('AvgPts',''):>7} {s.get('BestPts',''):>7} {s.get('WorstPts',''):>7} "
              f"{s.get('C2',0):>4} {s.get('TWEAK',0):>4}")

    print("-" * 80)
    wr = f"{100*total_w/total_t:.0f}%" if total_t else "-"
    print(f"  {'TOTAL':<12} {total_t:>4} {total_w:>4} {total_l:>4} "
          f"{wr:>6} {total_pts:>+9.1f} {total_rs:>+11,.0f}")
    print(f"{'='*80}")

    # Save summary CSV
    sum_csv = out_dir / f"summary_5stage_5m_{today}.csv"
    if summaries:
        valid = [s for s in summaries if "Note" not in s]
        if valid:
            with open(sum_csv, "w", newline="") as f:
                w2 = csv.DictWriter(f, fieldnames=list(valid[0].keys()))
                w2.writeheader()
                w2.writerows(valid)
            print(f"Summary saved: {sum_csv}")


if __name__ == "__main__":
    main()
