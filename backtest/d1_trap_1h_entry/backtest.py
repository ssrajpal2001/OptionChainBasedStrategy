"""
backtest/d1_trap_1h_entry/backtest.py -- D1 Trap + 1H Candle Breach backtest on NIFTY spot.

Strategy (same trap logic as V4 Cascade rolling_base.py):
  Step 1 — D1 trap detection (3-candle sweep+reclaim pattern):
    Bear trap (bullish): ref candle low swept below, then price reclaims above ref.high → LONG
    Bull trap (bearish): ref candle high swept above, then price reclaims below ref.low → SHORT

  Step 2 — 1H zone-entry and candle breach:
    When a 1H bar enters the confirmed D1 zone, that bar becomes the first "ref candle"
    Next 1H bar:
      LONG:  if bar.high > ref.high → entry = ref.high, SL = ref.low
      SHORT: if bar.low  < ref.low  → entry = ref.low,  SL = ref.high
    If no breach, the new bar becomes the ref candle; repeat within same day
    Monitoring state resets at EOD (no cross-day ref candles)

  Exit: 1:2 R:R target, hard SL, or EOD at 15:15 IST (no new entries after 14:00)

P&L: NIFTY spot points x LOT_SIZE x QTY

Usage:
  UPSTOX_TOKEN=xxx python backtest/d1_trap_1h_entry/backtest.py [--months 6]
  (token also read from data/clients.db feeder creds if env var absent)
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set
from urllib.parse import quote as _q

sys.path.insert(0, ".")

from config.global_config import IST
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones

# ── constants ─────────────────────────────────────────────────────────────────
LOT_SIZE = 65
QTY = 1
NIFTY_KEY = "NSE_INDEX|Nifty 50"
SESSION_OPEN = (9, 15)
EOD_HOUR_MIN = (15, 15)
ENTRY_CUTOFF = (14, 0)      # no new entries after 14:00 IST
MAX_ZONE_AGE_DAYS = 20      # discard D1 zones older than 20 calendar days

_CACHE_DIR = os.path.join(os.path.dirname(__file__), "data_cache")
_RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")


# ── bar dataclass (duck-type compatible with rolling_base._Bar protocol) ──────
@dataclass(frozen=True)
class _Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float


# ── Upstox data fetch with disk cache ────────────────────────────────────────
def _http_get_json(url: str, token: str) -> dict:
    try:
        from curl_cffi import requests as _cc  # type: ignore
        hdrs = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
        return _cc.get(url, headers=hdrs, impersonate="chrome131", timeout=20).json()
    except Exception as exc:
        print(f"[WARN] HTTP error — {exc}")
        return {}


def _parse_candles(raw: dict) -> List[_Bar]:
    rows = (raw.get("data") or {}).get("candles") or []
    bars: List[_Bar] = []
    for c in reversed(rows):   # Upstox returns newest-first; reverse to oldest-first
        try:
            ts = datetime.fromisoformat(c[0]).astimezone(IST)
            bars.append(_Bar(timestamp=ts, open=float(c[1]), high=float(c[2]),
                              low=float(c[3]), close=float(c[4])))
        except Exception:
            pass
    return bars


def _fetch_bars_chunk(key: str, interval: str, start: date, end: date, token: str) -> List[_Bar]:
    """Single-range fetch with disk cache. Upstox URL: /v2/historical-candle/{key}/{interval}/{to}/{from}"""
    os.makedirs(_CACHE_DIR, exist_ok=True)
    safe = key.replace("|", "_").replace(" ", "_")
    cache = os.path.join(_CACHE_DIR, f"{safe}_{interval}_{start}_{end}.json")
    if os.path.exists(cache):
        with open(cache) as f:
            raw = json.load(f)
        return _parse_candles(raw)
    url = (f"https://api.upstox.com/v2/historical-candle/"
           f"{_q(key, safe='')}/{interval}/{end.isoformat()}/{start.isoformat()}")
    raw = _http_get_json(url, token)
    bars = _parse_candles(raw)
    if bars:
        tmp = cache + ".tmp"
        with open(tmp, "w") as f:
            json.dump(raw, f)
        os.replace(tmp, cache)
    return bars


def fetch_bars(key: str, interval: str, start: date, end: date, token: str,
               chunk_days: int = 30) -> List[_Bar]:
    """Fetch historical candles, chunking by chunk_days to respect Upstox range limits.
    'day' interval supports multi-year ranges (no chunking needed).
    '30minute' is limited to ~30 days per request."""
    if interval == "day":
        bars = _fetch_bars_chunk(key, interval, start, end, token)
        print(f"  [Upstox day] {start}..{end}: {len(bars)} bars")
        return bars
    # chunk-fetch for intraday intervals
    all_bars: List[_Bar] = []
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(chunk_start + timedelta(days=chunk_days - 1), end)
        chunk = _fetch_bars_chunk(key, interval, chunk_start, chunk_end, token)
        all_bars.extend(chunk)
        print(f"  [Upstox {interval}] {chunk_start}..{chunk_end}: {len(chunk)} bars")
        chunk_start = chunk_end + timedelta(days=1)
    # deduplicate and sort
    seen: Set[datetime] = set()
    out: List[_Bar] = []
    for b in sorted(all_bars, key=lambda b: b.timestamp):
        if b.timestamp not in seen:
            seen.add(b.timestamp)
            out.append(b)
    return out


# ── zone monitor ──────────────────────────────────────────────────────────────
@dataclass
class _Monitor:
    direction: str           # 'LONG' (bear zone/bullish) | 'SHORT' (bull zone/bearish)
    trap_ts: datetime        # D1 reclaim candle timestamp (zone.lock_ts)
    zone_lo: float           # lower zone bound = min(entry_line, sweep_extreme)
    zone_hi: float           # upper zone bound = max(entry_line, sweep_extreme)
    state: str = "WAITING"   # 'WAITING' | 'MONITORING'
    zone_entry_ts: Optional[datetime] = None
    ref_bar: Optional[_Bar] = None


# ── trade record ──────────────────────────────────────────────────────────────
@dataclass
class Trade:
    direction: str
    trap_ts: datetime
    zone_entry_ts: datetime
    ref_bar_ts: datetime
    entry_ts: datetime
    entry: float
    sl: float
    target: float
    exit_ts: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    pnl_pts: Optional[float] = None

    @property
    def pnl_rs(self) -> float:
        return 0.0 if self.pnl_pts is None else self.pnl_pts * LOT_SIZE * QTY

    def as_row(self) -> dict:
        return {
            "Direction":      self.direction,
            "Trap_TS":        self.trap_ts.strftime("%Y-%m-%d %H:%M"),
            "Zone_Entry_TS":  self.zone_entry_ts.strftime("%Y-%m-%d %H:%M") if self.zone_entry_ts else "",
            "Ref_Bar_TS":     self.ref_bar_ts.strftime("%Y-%m-%d %H:%M"),
            "Entry_TS":       self.entry_ts.strftime("%Y-%m-%d %H:%M"),
            "Entry":          f"{self.entry:.2f}",
            "SL":             f"{self.sl:.2f}",
            "Target":         f"{self.target:.2f}",
            "Risk_pts":       f"{abs(self.entry - self.sl):.2f}",
            "Exit_TS":        self.exit_ts.strftime("%Y-%m-%d %H:%M") if self.exit_ts else "",
            "Exit_Price":     f"{self.exit_price:.2f}" if self.exit_price is not None else "",
            "Exit_Reason":    self.exit_reason or "",
            "PnL_pts":        f"{self.pnl_pts:.2f}" if self.pnl_pts is not None else "",
            "PnL_Rs":         f"{self.pnl_rs:.0f}",
        }


# ── 30m -> 60m resampling ─────────────────────────────────────────────────────
def resample_to_60m(bars_30m: List[_Bar]) -> List[_Bar]:
    """Group consecutive 30m bars into 60m bars, clock-anchored to session open (09:15)."""
    from collections import defaultdict
    buckets: Dict = {}
    order: List = []
    for b in bars_30m:
        d = b.timestamp.date()
        open_dt = b.timestamp.replace(hour=9, minute=15, second=0, microsecond=0)
        mins_since_open = int((b.timestamp - open_dt).total_seconds() // 60)
        bucket_idx = mins_since_open // 60
        key = (d, bucket_idx)
        if key not in buckets:
            buckets[key] = []
            order.append(key)
        buckets[key].append(b)
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


# ── simulation ────────────────────────────────────────────────────────────────
def run_backtest(d1_bars: List[_Bar], h1_bars: List[_Bar]) -> List[Trade]:
    trades: List[Trade] = []
    monitors: List[_Monitor] = []
    known_bear: Set[datetime] = set()
    known_bull: Set[datetime] = set()

    # Group 1H bars by calendar date
    h1_by_date: Dict[date, List[_Bar]] = {}
    for b in h1_bars:
        h1_by_date.setdefault(b.timestamp.date(), []).append(b)

    for today in sorted(h1_by_date):
        # D1 bars whose date < today (yesterday and older = confirmed closes)
        d1_avail = [b for b in d1_bars if b.timestamp.date() < today]

        if len(d1_avail) >= 3:
            # Discover new bear zones (sellers trapped → bullish → LONG)
            for z in find_all_bear_zones(d1_avail, known_ref_ts=known_bear.copy()):
                if z.reference_low_ts not in known_bear:
                    known_bear.add(z.reference_low_ts)
                    monitors.append(_Monitor(
                        direction="LONG",
                        trap_ts=z.lock_ts,
                        zone_lo=min(z.entry_line, z.sweep_low),
                        zone_hi=max(z.entry_line, z.sweep_low),
                    ))
            # Discover new bull zones (buyers trapped → bearish → SHORT)
            for z in find_all_bull_zones(d1_avail, known_ref_ts=known_bull.copy()):
                if z.reference_low_ts not in known_bull:
                    known_bull.add(z.reference_low_ts)
                    monitors.append(_Monitor(
                        direction="SHORT",
                        trap_ts=z.lock_ts,
                        zone_lo=min(z.entry_line, z.sweep_low),
                        zone_hi=max(z.entry_line, z.sweep_low),
                    ))

        # Evict zones older than MAX_ZONE_AGE_DAYS
        age_cutoff = datetime.combine(today, datetime.min.time()).replace(tzinfo=IST) \
                     - timedelta(days=MAX_ZONE_AGE_DAYS)
        monitors = [m for m in monitors if m.trap_ts >= age_cutoff]

        # ── today's 1H bars ────────────────────────────────────────────────
        day_bars = sorted(h1_by_date[today], key=lambda b: b.timestamp)
        open_trade: Optional[Trade] = None

        for bar in day_bars:
            hm = (bar.timestamp.hour, bar.timestamp.minute)
            if hm < SESSION_OPEN:
                continue

            # ── EOD: force-exit any open position ──────────────────────────
            if hm >= EOD_HOUR_MIN:
                if open_trade is not None:
                    open_trade.exit_ts = bar.timestamp
                    open_trade.exit_price = bar.open   # exit at open of 15:15 bar
                    open_trade.exit_reason = "eod"
                    open_trade.pnl_pts = (
                        bar.open - open_trade.entry if open_trade.direction == "LONG"
                        else open_trade.entry - bar.open
                    )
                    trades.append(open_trade)
                    open_trade = None
                break

            # ── check exits for open trade ─────────────────────────────────
            if open_trade is not None:
                if open_trade.direction == "LONG":
                    # SL checked first (conservative when both trigger same bar)
                    if bar.low <= open_trade.sl:
                        open_trade.exit_ts = bar.timestamp
                        open_trade.exit_price = open_trade.sl
                        open_trade.exit_reason = "sl"
                        open_trade.pnl_pts = open_trade.sl - open_trade.entry
                        trades.append(open_trade)
                        open_trade = None
                    elif bar.high >= open_trade.target:
                        open_trade.exit_ts = bar.timestamp
                        open_trade.exit_price = open_trade.target
                        open_trade.exit_reason = "target"
                        open_trade.pnl_pts = open_trade.target - open_trade.entry
                        trades.append(open_trade)
                        open_trade = None
                else:  # SHORT
                    if bar.high >= open_trade.sl:
                        open_trade.exit_ts = bar.timestamp
                        open_trade.exit_price = open_trade.sl
                        open_trade.exit_reason = "sl"
                        open_trade.pnl_pts = open_trade.entry - open_trade.sl
                        trades.append(open_trade)
                        open_trade = None
                    elif bar.low <= open_trade.target:
                        open_trade.exit_ts = bar.timestamp
                        open_trade.exit_price = open_trade.target
                        open_trade.exit_reason = "target"
                        open_trade.pnl_pts = open_trade.entry - open_trade.target
                        trades.append(open_trade)
                        open_trade = None
                if open_trade is not None:
                    continue   # still in trade — no new entries

            # No new entries after 14:00 or if already in a trade
            if open_trade is not None or hm >= ENTRY_CUTOFF:
                continue

            # ── zone monitoring + breach entry ─────────────────────────────
            for m in monitors:
                if m.state == "WAITING":
                    # Bear zone entry: 1H bar's low dips into zone (below zone_hi = ref.low)
                    # Bull zone entry: 1H bar's high rises into zone (above zone_lo = ref.high)
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
                    if m.direction == "LONG":
                        if bar.high > ref.high:
                            entry, sl = ref.high, ref.low
                            risk = entry - sl
                            if risk > 0:
                                open_trade = Trade(
                                    direction="LONG", trap_ts=m.trap_ts,
                                    zone_entry_ts=m.zone_entry_ts, ref_bar_ts=ref.timestamp,
                                    entry_ts=bar.timestamp, entry=entry, sl=sl,
                                    target=entry + 2 * risk,
                                )
                                m.state = "WAITING"; m.ref_bar = None
                                break   # one trade per day
                        m.ref_bar = bar  # roll ref forward
                    else:  # SHORT
                        if bar.low < ref.low:
                            entry, sl = ref.low, ref.high
                            risk = sl - entry
                            if risk > 0:
                                open_trade = Trade(
                                    direction="SHORT", trap_ts=m.trap_ts,
                                    zone_entry_ts=m.zone_entry_ts, ref_bar_ts=ref.timestamp,
                                    entry_ts=bar.timestamp, entry=entry, sl=sl,
                                    target=entry - 2 * risk,
                                )
                                m.state = "WAITING"; m.ref_bar = None
                                break
                        m.ref_bar = bar  # roll ref forward

        # End of day: reset monitoring state (no cross-day ref candles)
        for m in monitors:
            if m.state == "MONITORING":
                m.state = "WAITING"
                m.ref_bar = None
                m.zone_entry_ts = None

    return trades


# ── reporting ─────────────────────────────────────────────────────────────────
_COL_W = {
    "Direction": 7, "Trap_TS": 18, "Zone_Entry_TS": 18, "Ref_Bar_TS": 18,
    "Entry_TS": 18, "Entry": 9, "SL": 9, "Target": 9, "Risk_pts": 9,
    "Exit_TS": 18, "Exit_Price": 10, "Exit_Reason": 8, "PnL_pts": 8, "PnL_Rs": 9,
}


def _fmt(row: dict) -> str:
    return "  ".join(str(row[k]).rjust(_COL_W[k]) for k in _COL_W)


def _header() -> str:
    return "  ".join(k.rjust(_COL_W[k]) for k in _COL_W)


def print_report(trades: List[Trade], start: date, end: date) -> None:
    sep = "-" * (sum(_COL_W.values()) + 2 * (len(_COL_W) - 1))
    print(f"\nD1 Trap + 1H Candle Breach Backtest  {start} to {end}")
    print(sep)
    print(_header())
    print(sep)
    for t in trades:
        print(_fmt(t.as_row()))
    print(sep)

    if not trades:
        print("No trades found in the period.")
        return

    winners = [t for t in trades if (t.pnl_pts or 0) > 0]
    losers  = [t for t in trades if (t.pnl_pts or 0) <= 0]
    total_rs = sum(t.pnl_rs for t in trades)
    gross_profit = sum(t.pnl_rs for t in winners)
    gross_loss   = abs(sum(t.pnl_rs for t in losers))
    pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    running = peak = max_dd = 0.0
    for t in trades:
        running += t.pnl_rs
        peak = max(peak, running)
        max_dd = max(max_dd, peak - running)

    reason_counts: Dict[str, int] = {}
    for t in trades:
        r = t.exit_reason or "open"
        reason_counts[r] = reason_counts.get(r, 0) + 1

    long_trades  = [t for t in trades if t.direction == "LONG"]
    short_trades = [t for t in trades if t.direction == "SHORT"]

    print(f"\n{'Trades':20}: {len(trades)}"
          f"  (LONG={len(long_trades)}, SHORT={len(short_trades)})")
    print(f"{'Winners':20}: {len(winners)}"
          f"  ({100*len(winners)//len(trades)}%)")
    print(f"{'Losers':20}: {len(losers)}"
          f"  ({100*len(losers)//len(trades)}%)")
    print(f"{'Gross profit':20}: Rs {gross_profit:,.0f}")
    print(f"{'Gross loss':20}: Rs {gross_loss:,.0f}")
    print(f"{'Profit factor':20}: {pf:.2f}")
    print(f"{'Net P&L':20}: Rs {total_rs:,.0f}")
    print(f"{'Max drawdown':20}: Rs {max_dd:,.0f}")
    print(f"{'Exit reasons':20}: {reason_counts}")


def save_csv(trades: List[Trade]) -> str:
    os.makedirs(_RESULTS_DIR, exist_ok=True)
    path = os.path.join(_RESULTS_DIR, "trades.csv")
    if not trades:
        return path
    fields = list(_COL_W.keys())
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for t in trades:
            w.writerow(t.as_row())
    return path


# ── main ──────────────────────────────────────────────────────────────────────
async def main() -> None:
    parser = argparse.ArgumentParser(description="D1 Trap + 1H Candle Breach backtest")
    parser.add_argument("--months", type=int, default=6,
                        help="calendar months of history to fetch (default: 6)")
    args = parser.parse_args()

    token = os.environ.get("UPSTOX_TOKEN", "")
    if not token:
        try:
            from data_layer.client_db import ClientDB
            creds = ClientDB().get_feeder_creds_sync() or {}
            token = creds.get("upstox_access_token", "")
        except Exception:
            pass
    if not token:
        print("ERROR: set UPSTOX_TOKEN env var or configure Upstox feeder creds in DB")
        sys.exit(1)

    end = date.today()
    # start = first day of (today's month - months + 1)
    start = date(end.year, end.month, 1)
    for _ in range(args.months - 1):
        start = (start - timedelta(days=1))
        start = date(start.year, start.month, 1)

    print(f"\nFetching D1 bars  {start} to {end} ...")
    d1_bars = await asyncio.to_thread(fetch_bars, NIFTY_KEY, "day", start, end, token)
    print(f"  -> {len(d1_bars)} D1 bars")

    print(f"Fetching 30m bars {start} to {end} (will resample to 60m) ...")
    bars_30m = await asyncio.to_thread(fetch_bars, NIFTY_KEY, "30minute", start, end, token)
    print(f"  -> {len(bars_30m)} 30-minute bars fetched")
    h1_bars = resample_to_60m(bars_30m)
    print(f"  -> {len(h1_bars)} 60-minute bars after resampling")

    if not d1_bars or not h1_bars:
        print("No data fetched - check token and network.")
        sys.exit(1)

    print("\nRunning backtest ...")
    trades = run_backtest(d1_bars, h1_bars)
    print(f"  -> {len(trades)} trades generated")

    print_report(trades, start, end)

    csv_path = save_csv(trades)
    print(f"\nTrades saved to: {csv_path}")


if __name__ == "__main__":
    asyncio.run(main())
