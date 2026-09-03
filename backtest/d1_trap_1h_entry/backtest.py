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
    d1_ref_ts: datetime      # D1 ref candle (Candle 1 — whose low/high defines entry_line)
    d1_sweep_ts: datetime    # D1 sweep candle (Candle 2 — sweeps past ref extreme)
    d1_reclaim_ts: datetime  # D1 reclaim candle (Candle 3 — confirms trap, zone.lock_ts)
    zone_lo: float           # lower zone bound = min(entry_line, sweep_extreme)
    zone_hi: float           # upper zone bound = max(entry_line, sweep_extreme)
    state: str = "WAITING"   # 'WAITING' | 'MONITORING'
    zone_entry_ts: Optional[datetime] = None
    ref_bar: Optional[_Bar] = None
    done: bool = False       # True once this zone has fired a trade — never re-enters pool
    invalid: bool = False    # True when 1H bar closes THROUGH zone boundary (zone SL hit)


@dataclass
class _FlipZone:
    """D1 zone that was invalidated and is now tracked for a counter-direction (flip) trade.
    When a bear zone (LONG) fails: zone_lo becomes resistance -> SHORT flip trade.
    When a bull zone (SHORT) fails: zone_hi becomes support -> LONG flip trade.
    """
    original_dir: str          # "LONG" | "SHORT" (the original zone direction)
    trade_dir: str             # "SHORT" | "LONG"  (opposite — the trade we take)
    zone_lo: float
    zone_hi: float
    d1_ref_ts: datetime
    d1_sweep_ts: datetime
    d1_reclaim_ts: datetime
    invalidated_ts: datetime
    state: str = "WAITING"     # "WAITING" | "MONITORING"
    ref_bar: Optional[_Bar] = None   # 1H bar touching the flip level from opposite side
    tap_ts: Optional[datetime] = None
    done: bool = False


@dataclass
class _TweakSetup:
    """Immediate counter-trade when a zone fails WHILE in MONITORING state.
    The failure bar itself becomes the ref bar for an opposite-direction trade.
    """
    trade_dir: str             # "SHORT" (for failed LONG zone) | "LONG" (for failed SHORT zone)
    failure_bar: _Bar          # 1H bar that was monitoring AND closed through the zone boundary
    zone_lo: float
    zone_hi: float
    d1_ref_ts: datetime
    d1_sweep_ts: datetime
    d1_reclaim_ts: datetime
    zone_entry_ts: datetime
    done: bool = False


# ── trade record ──────────────────────────────────────────────────────────────
@dataclass
class Trade:
    direction: str
    zone_lo: float           # D1 zone lower bound
    zone_hi: float           # D1 zone upper bound
    d1_ref_ts: datetime      # D1 Candle 1 — ref candle (entry_line = its low/high)
    d1_sweep_ts: datetime    # D1 Candle 2 — sweep candle (broke past ref extreme)
    d1_reclaim_ts: datetime  # D1 Candle 3 — reclaim candle (zone confirmed)
    zone_entry_ts: datetime  # first 1H bar that entered the zone
    ref_bar_ts: datetime     # 1H ref candle whose breach triggered entry
    entry_ts: datetime       # bar whose breach caused the entry
    entry: float
    sl: float
    target: Optional[float] = None   # None for positional strategies
    sl_ts: Optional[datetime] = None
    target_ts: Optional[datetime] = None
    eod_ts: Optional[datetime] = None
    opp_exit_ts: Optional[datetime] = None   # positional: opposite D1 trap zone entered
    exit_price: Optional[float] = None
    pnl_pts: Optional[float] = None
    # Strategy B/C fields
    method: str = "1H"              # "1H" | "5M" | "5M_FB" (fallback) | "SKIP"
    m5_zone_lo: Optional[float] = None
    m5_zone_hi: Optional[float] = None

    @property
    def outcome(self) -> str:
        if self.sl_ts:       return "SL"
        if self.opp_exit_ts: return "OPP_TRAP"
        if self.target_ts:   return "TARGET"
        if self.eod_ts:      return "EOD"
        return "OPEN"

    @property
    def exit_ts(self) -> Optional[datetime]:
        return self.sl_ts or self.target_ts or self.opp_exit_ts or self.eod_ts

    @property
    def pnl_rs(self) -> float:
        return 0.0 if self.pnl_pts is None else self.pnl_pts * LOT_SIZE * QTY

    def as_row(self) -> dict:
        fmt = lambda dt: dt.strftime("%Y-%m-%d %H:%M") if dt else ""
        return {
            "Dir":           self.direction,
            "Zone_Lo":       f"{self.zone_lo:.2f}",
            "Zone_Hi":       f"{self.zone_hi:.2f}",
            "D1_Ref_TS":     fmt(self.d1_ref_ts),
            "D1_Sweep_TS":   fmt(self.d1_sweep_ts),
            "D1_Reclaim_TS": fmt(self.d1_reclaim_ts),
            "Zone_Tap_TS":   fmt(self.zone_entry_ts),
            "Ref_Bar_TS":    fmt(self.ref_bar_ts),
            "Entry_TS":      fmt(self.entry_ts),
            "Entry":         f"{self.entry:.2f}",
            "SL_Level":      f"{self.sl:.2f}",
            "Target":        f"{self.target:.2f}" if self.target is not None else "",
            "Risk_pts":      f"{abs(self.entry - self.sl):.2f}",
            "SL_Hit_TS":     fmt(self.sl_ts),
            "Target_TS":     fmt(self.target_ts),
            "OppExit_TS":    fmt(self.opp_exit_ts),
            "EOD_TS":        fmt(self.eod_ts),
            "Exit_Price":    f"{self.exit_price:.2f}" if self.exit_price is not None else "",
            "Outcome":       self.outcome,
            "PnL_pts":       f"{self.pnl_pts:.2f}" if self.pnl_pts is not None else "",
            "PnL_Rs":        f"{self.pnl_rs:.0f}",
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


# ── 1m -> 5m resampling ───────────────────────────────────────────────────────
def resample_to_5m(bars_1m: List[_Bar]) -> List[_Bar]:
    """Group 1-min bars into 5-min bars, clock-anchored to session open (09:15)."""
    buckets: Dict = {}
    order: List = []
    for b in bars_1m:
        open_dt = b.timestamp.replace(hour=9, minute=15, second=0, microsecond=0)
        mins = int((b.timestamp - open_dt).total_seconds() // 60)
        key = (b.timestamp.date(), mins // 5)
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


def _detect_5m_zone(direction: str, ref_bar: _Bar,
                    day_m5: List[_Bar]) -> Optional[tuple]:
    """
    Find all 5-min trap zones inside the ref candle's 1H window, merge them.
    Returns (zone_lo, zone_hi) or None if no zones found.
    """
    ref_end = ref_bar.timestamp + timedelta(hours=1)
    ref_5m = [b for b in day_m5 if ref_bar.timestamp <= b.timestamp < ref_end]
    if len(ref_5m) < 3:
        return None
    zones = (find_all_bear_zones(ref_5m) if direction == "LONG"
             else find_all_bull_zones(ref_5m))
    if not zones:
        return None
    lo = min(min(z.entry_line, z.sweep_low) for z in zones)
    hi = max(max(z.entry_line, z.sweep_low) for z in zones)
    return (lo, hi)


def _apply_exit(trade: Trade, bar: _Bar) -> bool:
    """Check SL/Target on bar. Updates trade in-place. Returns True if closed."""
    if trade.direction == "LONG":
        if bar.low <= trade.sl:
            trade.sl_ts = bar.timestamp
            trade.exit_price = trade.sl
            trade.pnl_pts = trade.sl - trade.entry
            return True
        if bar.high >= trade.target:
            trade.target_ts = bar.timestamp
            trade.exit_price = trade.target
            trade.pnl_pts = trade.target - trade.entry
            return True
    else:
        if bar.high >= trade.sl:
            trade.sl_ts = bar.timestamp
            trade.exit_price = trade.sl
            trade.pnl_pts = trade.entry - trade.sl
            return True
        if bar.low <= trade.target:
            trade.target_ts = bar.timestamp
            trade.exit_price = trade.target
            trade.pnl_pts = trade.entry - trade.target
            return True
    return False


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
                        d1_ref_ts=z.reference_low_ts,
                        d1_sweep_ts=z.sweep_started_ts,
                        d1_reclaim_ts=z.lock_ts,
                        zone_lo=min(z.entry_line, z.sweep_low),
                        zone_hi=max(z.entry_line, z.sweep_low),
                    ))
            # Discover new bull zones (buyers trapped → bearish → SHORT)
            for z in find_all_bull_zones(d1_avail, known_ref_ts=known_bull.copy()):
                if z.reference_low_ts not in known_bull:
                    known_bull.add(z.reference_low_ts)
                    monitors.append(_Monitor(
                        direction="SHORT",
                        d1_ref_ts=z.reference_low_ts,
                        d1_sweep_ts=z.sweep_started_ts,
                        d1_reclaim_ts=z.lock_ts,
                        zone_lo=min(z.entry_line, z.sweep_low),
                        zone_hi=max(z.entry_line, z.sweep_low),
                    ))

        # Evict zones: too old OR already fired a trade (done=True)
        age_cutoff = datetime.combine(today, datetime.min.time()).replace(tzinfo=IST) \
                     - timedelta(days=MAX_ZONE_AGE_DAYS)
        monitors = [m for m in monitors if not m.done and m.d1_reclaim_ts >= age_cutoff]

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
                    open_trade.eod_ts = bar.timestamp
                    open_trade.exit_price = bar.open
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
                        open_trade.sl_ts = bar.timestamp
                        open_trade.exit_price = open_trade.sl
                        open_trade.pnl_pts = open_trade.sl - open_trade.entry
                        trades.append(open_trade)
                        open_trade = None
                    elif bar.high >= open_trade.target:
                        open_trade.target_ts = bar.timestamp
                        open_trade.exit_price = open_trade.target
                        open_trade.pnl_pts = open_trade.target - open_trade.entry
                        trades.append(open_trade)
                        open_trade = None
                else:  # SHORT
                    if bar.high >= open_trade.sl:
                        open_trade.sl_ts = bar.timestamp
                        open_trade.exit_price = open_trade.sl
                        open_trade.pnl_pts = open_trade.entry - open_trade.sl
                        trades.append(open_trade)
                        open_trade = None
                    elif bar.low <= open_trade.target:
                        open_trade.target_ts = bar.timestamp
                        open_trade.exit_price = open_trade.target
                        open_trade.pnl_pts = open_trade.entry - open_trade.target
                        trades.append(open_trade)
                        open_trade = None
                if open_trade is not None:
                    continue   # still in trade — no new entries

            # No new entries after cutoff or if already in a trade
            if open_trade is not None or hm >= ENTRY_CUTOFF:
                continue

            # ── zone monitoring + breach entry ─────────────────────────────
            for m in monitors:
                if m.done:
                    continue

                if m.state == "WAITING":
                    # Bear zone: 1H bar dips into zone (bar.low <= zone_hi = ref.low)
                    # Bull zone: 1H bar rises into zone (bar.high >= zone_lo = ref.high)
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
                                    direction="LONG",
                                    zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                                    d1_ref_ts=m.d1_ref_ts,
                                    d1_sweep_ts=m.d1_sweep_ts,
                                    d1_reclaim_ts=m.d1_reclaim_ts,
                                    zone_entry_ts=m.zone_entry_ts,
                                    ref_bar_ts=ref.timestamp,
                                    entry_ts=bar.timestamp,
                                    entry=entry, sl=sl, target=entry + 2 * risk,
                                )
                                m.done = True   # zone permanently consumed
                                break           # one trade per day
                        m.ref_bar = bar  # roll ref forward (no breach yet)
                    else:  # SHORT
                        if bar.low < ref.low:
                            entry, sl = ref.low, ref.high
                            risk = sl - entry
                            if risk > 0:
                                open_trade = Trade(
                                    direction="SHORT",
                                    zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                                    d1_ref_ts=m.d1_ref_ts,
                                    d1_sweep_ts=m.d1_sweep_ts,
                                    d1_reclaim_ts=m.d1_reclaim_ts,
                                    zone_entry_ts=m.zone_entry_ts,
                                    ref_bar_ts=ref.timestamp,
                                    entry_ts=bar.timestamp,
                                    entry=entry, sl=sl, target=entry - 2 * risk,
                                )
                                m.done = True   # zone permanently consumed
                                break
                        m.ref_bar = bar  # roll ref forward

        # End of day: reset monitoring state (no cross-day ref candles)
        for m in monitors:
            if m.state == "MONITORING":
                m.state = "WAITING"
                m.ref_bar = None
                m.zone_entry_ts = None

    return trades


# ── Strategy B: 5-min zone retest entry ───────────────────────────────────────
def run_backtest_5m(d1_bars: List[_Bar], h1_bars: List[_Bar],
                    m5_bars: List[_Bar]) -> List[Trade]:
    """
    Same D1 zone + 1H monitoring as run_backtest(), but after 1H breach:
    - Detect 5-min zones inside the ref candle → merge → limit order at 1/3 inside
    - If no 5-min zones: fall back to 1H breach entry (method='5M_FB')
    - If limit never fills by EOD: emit method='SKIP' trade (pnl_pts=None)
    Exits checked on 5-min bars (more granular than Strategy A).
    """
    trades: List[Trade] = []
    monitors: List[_Monitor] = []
    known_bear: Set[datetime] = set()
    known_bull: Set[datetime] = set()

    h1_by_date: Dict[date, List[_Bar]] = {}
    for b in h1_bars:
        h1_by_date.setdefault(b.timestamp.date(), []).append(b)

    m5_by_date: Dict[date, List[_Bar]] = {}
    for b in m5_bars:
        m5_by_date.setdefault(b.timestamp.date(), []).append(b)

    for today in sorted(h1_by_date):
        d1_avail = [b for b in d1_bars if b.timestamp.date() < today]
        if len(d1_avail) >= 3:
            for z in find_all_bear_zones(d1_avail, known_ref_ts=known_bear.copy()):
                if z.reference_low_ts not in known_bear:
                    known_bear.add(z.reference_low_ts)
                    monitors.append(_Monitor(
                        direction="LONG",
                        d1_ref_ts=z.reference_low_ts,
                        d1_sweep_ts=z.sweep_started_ts,
                        d1_reclaim_ts=z.lock_ts,
                        zone_lo=min(z.entry_line, z.sweep_low),
                        zone_hi=max(z.entry_line, z.sweep_low),
                    ))
            for z in find_all_bull_zones(d1_avail, known_ref_ts=known_bull.copy()):
                if z.reference_low_ts not in known_bull:
                    known_bull.add(z.reference_low_ts)
                    monitors.append(_Monitor(
                        direction="SHORT",
                        d1_ref_ts=z.reference_low_ts,
                        d1_sweep_ts=z.sweep_started_ts,
                        d1_reclaim_ts=z.lock_ts,
                        zone_lo=min(z.entry_line, z.sweep_low),
                        zone_hi=max(z.entry_line, z.sweep_low),
                    ))

        age_cutoff = (datetime.combine(today, datetime.min.time()).replace(tzinfo=IST)
                      - timedelta(days=MAX_ZONE_AGE_DAYS))
        monitors = [m for m in monitors if not m.done and m.d1_reclaim_ts >= age_cutoff]

        day_h1 = sorted(h1_by_date[today], key=lambda b: b.timestamp)
        day_m5 = sorted(m5_by_date.get(today, []), key=lambda b: b.timestamp)

        open_trade: Optional[Trade] = None
        pending: Optional[dict] = None   # pending limit order metadata

        def _m5_in(h1_bar: _Bar) -> List[_Bar]:
            end = h1_bar.timestamp + timedelta(hours=1)
            return [b for b in day_m5 if h1_bar.timestamp <= b.timestamp < end]

        for bar in day_h1:
            hm = (bar.timestamp.hour, bar.timestamp.minute)
            if hm < SESSION_OPEN:
                continue

            m5s = _m5_in(bar)

            # ── EOD ────────────────────────────────────────────────────────────
            if hm >= EOD_HOUR_MIN:
                if open_trade is not None:
                    open_trade.eod_ts = bar.timestamp
                    open_trade.exit_price = bar.open
                    open_trade.pnl_pts = (bar.open - open_trade.entry
                                          if open_trade.direction == "LONG"
                                          else open_trade.entry - bar.open)
                    trades.append(open_trade)
                    open_trade = None
                if pending is not None:
                    # Limit never filled — emit SKIP record
                    trades.append(Trade(
                        method="SKIP", direction=pending["direction"],
                        zone_lo=pending["zone_lo_d1"], zone_hi=pending["zone_hi_d1"],
                        d1_ref_ts=pending["d1_ref_ts"], d1_sweep_ts=pending["d1_sweep_ts"],
                        d1_reclaim_ts=pending["d1_reclaim_ts"],
                        zone_entry_ts=pending["zone_entry_ts"],
                        ref_bar_ts=pending["ref_bar_ts"],
                        entry_ts=bar.timestamp,
                        entry=pending["limit"], sl=pending["sl"], target=pending["target"],
                        m5_zone_lo=pending.get("m5_lo"), m5_zone_hi=pending.get("m5_hi"),
                    ))
                    pending = None
                break

            # ── Check exits (5-min granularity) ───────────────────────────────
            if open_trade is not None:
                for m5 in m5s:
                    if _apply_exit(open_trade, m5):
                        trades.append(open_trade)
                        open_trade = None
                        break
                if open_trade is not None:
                    continue

            # ── Check pending limit fill ───────────────────────────────────────
            if pending is not None and open_trade is None:
                pl = pending
                for i, m5 in enumerate(m5s):
                    hit = ((pl["direction"] == "LONG" and m5.low  <= pl["limit"]) or
                           (pl["direction"] == "SHORT" and m5.high >= pl["limit"]))
                    if hit:
                        t = Trade(
                            method=pl["method"], direction=pl["direction"],
                            zone_lo=pl["zone_lo_d1"], zone_hi=pl["zone_hi_d1"],
                            d1_ref_ts=pl["d1_ref_ts"], d1_sweep_ts=pl["d1_sweep_ts"],
                            d1_reclaim_ts=pl["d1_reclaim_ts"],
                            zone_entry_ts=pl["zone_entry_ts"],
                            ref_bar_ts=pl["ref_bar_ts"],
                            entry_ts=m5.timestamp, entry=pl["limit"],
                            sl=pl["sl"], target=pl["target"],
                            m5_zone_lo=pl.get("m5_lo"), m5_zone_hi=pl.get("m5_hi"),
                        )
                        pending = None
                        # Check exits on remaining 5-min bars of this 1H period
                        for m5_ex in m5s[i + 1:]:
                            if _apply_exit(t, m5_ex):
                                trades.append(t)
                                t = None  # type: ignore
                                break
                        if t is not None:
                            open_trade = t
                        break
                if open_trade is not None or pending is None:
                    continue

            # ── Zone monitoring + breach → set limit / fallback ────────────────
            if open_trade is not None or pending is not None or hm >= ENTRY_CUTOFF:
                continue

            for m in monitors:
                if m.done:
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
                    direction = m.direction
                    triggered = ((direction == "LONG"  and bar.high > ref.high) or
                                 (direction == "SHORT" and bar.low  < ref.low))
                    if not triggered:
                        m.ref_bar = bar
                        continue

                    # Breach confirmed — detect 5-min zones in ref candle
                    result = _detect_5m_zone(direction, ref, day_m5)
                    meta = dict(
                        direction=direction,
                        zone_lo_d1=m.zone_lo, zone_hi_d1=m.zone_hi,
                        d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                        d1_reclaim_ts=m.d1_reclaim_ts,
                        zone_entry_ts=m.zone_entry_ts, ref_bar_ts=ref.timestamp,
                    )
                    m.done = True

                    if result is not None:
                        z_lo, z_hi = result
                        sz = z_hi - z_lo
                        limit = (z_hi - sz / 3) if direction == "LONG" else (z_lo + sz / 3)
                        sl_p  = z_lo if direction == "LONG" else z_hi
                        risk  = abs(limit - sl_p)
                        if risk > 0:
                            pending = {**meta, "method": "5M",
                                       "limit": limit, "sl": sl_p,
                                       "target": (limit + 2*risk if direction == "LONG"
                                                  else limit - 2*risk),
                                       "m5_lo": z_lo, "m5_hi": z_hi}
                            # Immediately check fills in current 1H bar's 5-min bars
                            pl = pending
                            for i, m5 in enumerate(m5s):
                                hit = ((pl["direction"] == "LONG" and m5.low  <= pl["limit"]) or
                                       (pl["direction"] == "SHORT" and m5.high >= pl["limit"]))
                                if hit:
                                    t = Trade(
                                        method=pl["method"], direction=pl["direction"],
                                        zone_lo=pl["zone_lo_d1"], zone_hi=pl["zone_hi_d1"],
                                        d1_ref_ts=pl["d1_ref_ts"], d1_sweep_ts=pl["d1_sweep_ts"],
                                        d1_reclaim_ts=pl["d1_reclaim_ts"],
                                        zone_entry_ts=pl["zone_entry_ts"],
                                        ref_bar_ts=pl["ref_bar_ts"],
                                        entry_ts=m5.timestamp, entry=pl["limit"],
                                        sl=pl["sl"], target=pl["target"],
                                        m5_zone_lo=pl.get("m5_lo"), m5_zone_hi=pl.get("m5_hi"),
                                    )
                                    pending = None
                                    for m5_ex in m5s[i + 1:]:
                                        if _apply_exit(t, m5_ex):
                                            trades.append(t)
                                            t = None  # type: ignore
                                            break
                                    if t is not None:
                                        open_trade = t
                                    break
                            break
                    else:
                        # Fallback: 1H breach entry
                        if direction == "LONG":
                            entry, sl_p = ref.high, ref.low
                        else:
                            entry, sl_p = ref.low, ref.high
                        risk = abs(entry - sl_p)
                        if risk > 0:
                            t = Trade(
                                method="5M_FB", direction=direction,
                                zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                                d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                                d1_reclaim_ts=m.d1_reclaim_ts,
                                zone_entry_ts=m.zone_entry_ts,
                                ref_bar_ts=ref.timestamp,
                                entry_ts=bar.timestamp, entry=entry, sl=sl_p,
                                target=(entry + 2*risk if direction == "LONG"
                                        else entry - 2*risk),
                            )
                            # Check exits on 5-min bars of this 1H period
                            for m5 in m5s:
                                if _apply_exit(t, m5):
                                    trades.append(t)
                                    t = None  # type: ignore
                                    break
                            if t is not None:
                                open_trade = t
                        break

        # End of day: reset monitoring state
        for m in monitors:
            if m.state == "MONITORING":
                m.state = "WAITING"
                m.ref_bar = None
                m.zone_entry_ts = None

    return trades


# ── Positional backtest (no EOD, opposite-trap or 1H-TSL exit) ───────────────
def run_backtest_positional(
    d1_bars: List[_Bar],
    h1_bars: List[_Bar],
    m5_bars: List[_Bar],
    use_5m: bool = False,
    exit_mode: str = "opp_trap",       # "opp_trap" | "tsl_1h"
    strict_zone_tap: bool = False,      # NEW: enforce approach-direction + zone invalidation
) -> List[Trade]:
    """
    Positional strategy — no EOD exit, trade spans multiple days.
    entry:
      use_5m=False  -> 1H breach (same as Strategy A)
      use_5m=True   -> 5-min zone_hi/lo breakout; fallback to 1H if no zone found
    exit:
      exit_mode="opp_trap" -> exit at D1 opposite-zone boundary when price enters it
      exit_mode="tsl_1h"   -> trail SL to each new 1H candle's low/high;
                               exit when a 5-min bar CLOSES beyond trailing level
    SL: always 1H ref candle LOW (LONG) / HIGH (SHORT)

    strict_zone_tap=True adds two rules (user-specified 2026-07-28):
      Rule 1 — Direction of approach:
        Bear zone (LONG): price must arrive FROM ABOVE — prev 1H bar closed above zone_hi
        Bull zone (SHORT): price must arrive FROM BELOW — prev 1H bar closed below zone_lo
      Rule 2 — Zone invalidation:
        Bear zone (LONG): if any 1H bar CLOSES below zone_lo → zone is dead (SL hit)
        Bull zone (SHORT): if any 1H bar CLOSES above zone_hi → zone is dead (SL hit)
    """
    trades: List[Trade] = []
    monitors: List[_Monitor] = []
    known_bear: Set[datetime] = set()
    known_bull: Set[datetime] = set()

    h1_by_date: Dict[date, List[_Bar]] = {}
    for b in h1_bars:
        h1_by_date.setdefault(b.timestamp.date(), []).append(b)

    m5_by_date: Dict[date, List[_Bar]] = {}
    for b in m5_bars:
        m5_by_date.setdefault(b.timestamp.date(), []).append(b)

    open_trade: Optional[Trade] = None   # persists across days
    tsl_level: float = 0.0              # current trailing SL (only used in tsl_1h mode)
    prev_h1_bar: Optional[_Bar] = None  # last 1H bar processed (for TSL update)

    for today in sorted(h1_by_date):
        d1_avail = [b for b in d1_bars if b.timestamp.date() < today]

        if len(d1_avail) >= 3:
            for z in find_all_bear_zones(d1_avail, known_ref_ts=known_bear.copy()):
                if z.reference_low_ts not in known_bear:
                    known_bear.add(z.reference_low_ts)
                    monitors.append(_Monitor(
                        direction="LONG",
                        d1_ref_ts=z.reference_low_ts,
                        d1_sweep_ts=z.sweep_started_ts,
                        d1_reclaim_ts=z.lock_ts,
                        zone_lo=min(z.entry_line, z.sweep_low),
                        zone_hi=max(z.entry_line, z.sweep_low),
                    ))
            for z in find_all_bull_zones(d1_avail, known_ref_ts=known_bull.copy()):
                if z.reference_low_ts not in known_bull:
                    known_bull.add(z.reference_low_ts)
                    monitors.append(_Monitor(
                        direction="SHORT",
                        d1_ref_ts=z.reference_low_ts,
                        d1_sweep_ts=z.sweep_started_ts,
                        d1_reclaim_ts=z.lock_ts,
                        zone_lo=min(z.entry_line, z.sweep_low),
                        zone_hi=max(z.entry_line, z.sweep_low),
                    ))

        age_cutoff = (datetime.combine(today, datetime.min.time()).replace(tzinfo=IST)
                      - timedelta(days=MAX_ZONE_AGE_DAYS))
        monitors = [m for m in monitors
                    if not m.done and not m.invalid and m.d1_reclaim_ts >= age_cutoff]

        day_h1 = sorted(h1_by_date[today], key=lambda b: b.timestamp)
        day_m5 = sorted(m5_by_date.get(today, []), key=lambda b: b.timestamp)

        pending_5m: Optional[dict] = None   # pending 5-min breakout trigger

        for bar in day_h1:
            hm = (bar.timestamp.hour, bar.timestamp.minute)
            if hm < SESSION_OPEN:
                prev_h1_bar = bar
                continue

            # ── TSL update: ratchet after each prior 1H bar closes ─────────
            if open_trade is not None and exit_mode == "tsl_1h" and prev_h1_bar is not None:
                if open_trade.direction == "LONG":
                    tsl_level = max(tsl_level, prev_h1_bar.low)
                else:
                    tsl_level = min(tsl_level, prev_h1_bar.high)

            m5s = [b for b in day_m5
                   if bar.timestamp <= b.timestamp < bar.timestamp + timedelta(hours=1)]

            # ── Exit checks for open trade ─────────────────────────────────
            if open_trade is not None:
                exited = False

                # Hard SL (initial 1H ref candle low/high)
                if open_trade.direction == "LONG" and bar.low <= open_trade.sl:
                    open_trade.sl_ts = bar.timestamp
                    open_trade.exit_price = open_trade.sl
                    open_trade.pnl_pts = open_trade.sl - open_trade.entry
                    trades.append(open_trade)
                    open_trade = None
                    exited = True
                elif open_trade.direction == "SHORT" and bar.high >= open_trade.sl:
                    open_trade.sl_ts = bar.timestamp
                    open_trade.exit_price = open_trade.sl
                    open_trade.pnl_pts = open_trade.entry - open_trade.sl
                    trades.append(open_trade)
                    open_trade = None
                    exited = True

                if not exited and open_trade is not None:
                    if exit_mode == "tsl_1h":
                        # Trail exit: 5-min close beyond trailing level
                        for m5 in m5s:
                            tsl_hit = ((open_trade.direction == "LONG"
                                        and m5.close < tsl_level) or
                                       (open_trade.direction == "SHORT"
                                        and m5.close > tsl_level))
                            if tsl_hit:
                                open_trade.sl_ts = m5.timestamp
                                open_trade.exit_price = tsl_level
                                open_trade.pnl_pts = (
                                    tsl_level - open_trade.entry
                                    if open_trade.direction == "LONG"
                                    else open_trade.entry - tsl_level
                                )
                                trades.append(open_trade)
                                open_trade = None
                                exited = True
                                break
                    else:
                        # OPP_TRAP: exit when opposite D1 zone is entered
                        opp_dir = "SHORT" if open_trade.direction == "LONG" else "LONG"
                        for m in monitors:
                            if m.done or m.direction != opp_dir:
                                continue
                            # Only zones formed AFTER trade entry
                            if m.d1_reclaim_ts <= open_trade.entry_ts:
                                continue
                            entered = ((opp_dir == "SHORT" and bar.high >= m.zone_lo) or
                                       (opp_dir == "LONG"  and bar.low  <= m.zone_hi))
                            if entered:
                                ep = m.zone_lo if opp_dir == "SHORT" else m.zone_hi
                                open_trade.opp_exit_ts = bar.timestamp
                                open_trade.exit_price = ep
                                open_trade.pnl_pts = (
                                    ep - open_trade.entry
                                    if open_trade.direction == "LONG"
                                    else open_trade.entry - ep
                                )
                                trades.append(open_trade)
                                open_trade = None
                                exited = True
                                break

            # ── Rule 2: Zone invalidation (strict mode) ───────────────────
            # If a 1H bar CLOSES through zone boundary → zone SL is hit → dead
            if strict_zone_tap:
                for m in monitors:
                    if m.done or m.invalid:
                        continue
                    if m.direction == "LONG" and bar.close < m.zone_lo:
                        m.invalid = True   # bear zone: 1H closed below zone low
                    elif m.direction == "SHORT" and bar.close > m.zone_hi:
                        m.invalid = True   # bull zone: 1H closed above zone high

            prev_h1_bar = bar

            if open_trade is not None:
                continue   # still in trade — no new entries

            if hm >= ENTRY_CUTOFF:
                continue

            # ── 5-min pending breakout check ──────────────────────────────
            if use_5m and pending_5m is not None:
                pl = pending_5m
                for m5 in m5s:
                    hit = ((pl["direction"] == "LONG"  and m5.high > pl["trigger"]) or
                           (pl["direction"] == "SHORT" and m5.low  < pl["trigger"]))
                    if hit:
                        open_trade = Trade(
                            method="5M",
                            direction=pl["direction"],
                            zone_lo=pl["zone_lo_d1"], zone_hi=pl["zone_hi_d1"],
                            d1_ref_ts=pl["d1_ref_ts"],
                            d1_sweep_ts=pl["d1_sweep_ts"],
                            d1_reclaim_ts=pl["d1_reclaim_ts"],
                            zone_entry_ts=pl["zone_entry_ts"],
                            ref_bar_ts=pl["ref_bar_ts"],
                            entry_ts=m5.timestamp,
                            entry=pl["trigger"], sl=pl["sl"],
                            m5_zone_lo=pl.get("m5_lo"), m5_zone_hi=pl.get("m5_hi"),
                        )
                        tsl_level = pl["sl"]
                        pending_5m = None
                        break
                if open_trade is not None:
                    continue

            # ── Zone monitoring + entry ───────────────────────────────────
            for m in monitors:
                if m.done or m.invalid:
                    continue

                if m.state == "WAITING":
                    # Rule 1: Direction-of-approach gate (strict mode)
                    # Bear zone: price must arrive FROM ABOVE (prev bar closed above zone_hi)
                    # Bull zone: price must arrive FROM BELOW (prev bar closed below zone_lo)
                    if m.direction == "LONG" and bar.low <= m.zone_hi:
                        approach_ok = (not strict_zone_tap or prev_h1_bar is None
                                       or prev_h1_bar.close > m.zone_hi)
                        if approach_ok:
                            m.state = "MONITORING"
                            m.zone_entry_ts = bar.timestamp
                            m.ref_bar = bar
                    elif m.direction == "SHORT" and bar.high >= m.zone_lo:
                        approach_ok = (not strict_zone_tap or prev_h1_bar is None
                                       or prev_h1_bar.close < m.zone_lo)
                        if approach_ok:
                            m.state = "MONITORING"
                            m.zone_entry_ts = bar.timestamp
                            m.ref_bar = bar

                elif m.state == "MONITORING" and m.ref_bar is not None:
                    ref = m.ref_bar
                    direction = m.direction
                    triggered = ((direction == "LONG"  and bar.high > ref.high) or
                                 (direction == "SHORT" and bar.low  < ref.low))

                    if not triggered:
                        m.ref_bar = bar
                        continue

                    # Breach confirmed — compute SL (always 1H ref candle low/high)
                    sl_p = ref.low if direction == "LONG" else ref.high
                    m.done = True

                    if use_5m:
                        result = _detect_5m_zone(direction, ref, day_m5)
                        if result is not None:
                            z_lo, z_hi = result
                            trigger_lvl = z_hi if direction == "LONG" else z_lo
                            risk = abs(trigger_lvl - sl_p)
                            if risk > 0:
                                meta = dict(
                                    direction=direction,
                                    zone_lo_d1=m.zone_lo, zone_hi_d1=m.zone_hi,
                                    d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                                    d1_reclaim_ts=m.d1_reclaim_ts,
                                    zone_entry_ts=m.zone_entry_ts,
                                    ref_bar_ts=ref.timestamp,
                                    trigger=trigger_lvl, sl=sl_p,
                                    m5_lo=z_lo, m5_hi=z_hi,
                                )
                                # Check current breach bar's 5-min bars first
                                filled = False
                                for m5 in m5s:
                                    hit = ((direction == "LONG"  and m5.high > trigger_lvl) or
                                           (direction == "SHORT" and m5.low  < trigger_lvl))
                                    if hit:
                                        open_trade = Trade(
                                            method="5M", direction=direction,
                                            zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                                            d1_ref_ts=m.d1_ref_ts,
                                            d1_sweep_ts=m.d1_sweep_ts,
                                            d1_reclaim_ts=m.d1_reclaim_ts,
                                            zone_entry_ts=m.zone_entry_ts,
                                            ref_bar_ts=ref.timestamp,
                                            entry_ts=m5.timestamp,
                                            entry=trigger_lvl, sl=sl_p,
                                            m5_zone_lo=z_lo, m5_zone_hi=z_hi,
                                        )
                                        tsl_level = sl_p
                                        filled = True
                                        break
                                if not filled:
                                    pending_5m = meta
                        else:
                            # Fallback: immediate 1H breach entry
                            entry = ref.high if direction == "LONG" else ref.low
                            risk = abs(entry - sl_p)
                            if risk > 0:
                                open_trade = Trade(
                                    method="5M_FB", direction=direction,
                                    zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                                    d1_ref_ts=m.d1_ref_ts,
                                    d1_sweep_ts=m.d1_sweep_ts,
                                    d1_reclaim_ts=m.d1_reclaim_ts,
                                    zone_entry_ts=m.zone_entry_ts,
                                    ref_bar_ts=ref.timestamp,
                                    entry_ts=bar.timestamp,
                                    entry=entry, sl=sl_p,
                                )
                                tsl_level = sl_p
                    else:
                        # A_pos: 1H breach entry
                        entry = ref.high if direction == "LONG" else ref.low
                        risk = abs(entry - sl_p)
                        if risk > 0:
                            open_trade = Trade(
                                direction=direction,
                                zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                                d1_ref_ts=m.d1_ref_ts,
                                d1_sweep_ts=m.d1_sweep_ts,
                                d1_reclaim_ts=m.d1_reclaim_ts,
                                zone_entry_ts=m.zone_entry_ts,
                                ref_bar_ts=ref.timestamp,
                                entry_ts=bar.timestamp,
                                entry=entry, sl=sl_p,
                            )
                            tsl_level = sl_p

                    if open_trade is not None:
                        break

        # End of day: reset monitoring state (ref candles don't cross days)
        for m in monitors:
            if m.state == "MONITORING":
                m.state = "WAITING"
                m.ref_bar = None
                m.zone_entry_ts = None
        pending_5m = None   # no cross-day pending

    # Any still-open trade at end of data
    if open_trade is not None:
        trades.append(open_trade)

    return trades


# ── COMBINED strategy: C2 + optional FLIP + optional TWEAK ───────────────────
def run_backtest_combined(
    d1_bars: List[_Bar],
    h1_bars: List[_Bar],
    m5_bars: List[_Bar],
    exit_mode: str = "tsl_1h",
    use_flip: bool = True,
    use_tweak: bool = True,
) -> List[Trade]:
    """
    Runs C2 (5M breakout + trail SL) together with FLIP and TWEAK entry sources.
    All three compete for the single open-position slot.
    Priority per bar: C2 regular entries first, then TWEAK immediate-failure,
    then FLIP delayed-retest.
    """
    trades: List[Trade] = []
    monitors: List[_Monitor] = []
    flip_zones: List[_FlipZone] = []
    tweak_setups: List[_TweakSetup] = []
    known_bear: Set[datetime] = set()
    known_bull: Set[datetime] = set()

    h1_by_date: Dict[date, List[_Bar]] = {}
    for b in h1_bars:
        h1_by_date.setdefault(b.timestamp.date(), []).append(b)
    m5_by_date: Dict[date, List[_Bar]] = {}
    for b in m5_bars:
        m5_by_date.setdefault(b.timestamp.date(), []).append(b)

    open_trade: Optional[Trade] = None
    tsl_level: float = 0.0
    prev_h1_bar: Optional[_Bar] = None

    for today in sorted(h1_by_date):
        d1_avail = [b for b in d1_bars if b.timestamp.date() < today]
        if len(d1_avail) >= 3:
            for z in find_all_bear_zones(d1_avail, known_ref_ts=known_bear.copy()):
                if z.reference_low_ts not in known_bear:
                    known_bear.add(z.reference_low_ts)
                    monitors.append(_Monitor(
                        direction="LONG",
                        d1_ref_ts=z.reference_low_ts, d1_sweep_ts=z.sweep_started_ts,
                        d1_reclaim_ts=z.lock_ts,
                        zone_lo=min(z.entry_line, z.sweep_low),
                        zone_hi=max(z.entry_line, z.sweep_low),
                    ))
            for z in find_all_bull_zones(d1_avail, known_ref_ts=known_bull.copy()):
                if z.reference_low_ts not in known_bull:
                    known_bull.add(z.reference_low_ts)
                    monitors.append(_Monitor(
                        direction="SHORT",
                        d1_ref_ts=z.reference_low_ts, d1_sweep_ts=z.sweep_started_ts,
                        d1_reclaim_ts=z.lock_ts,
                        zone_lo=min(z.entry_line, z.sweep_low),
                        zone_hi=max(z.entry_line, z.sweep_low),
                    ))

        age_cutoff = (datetime.combine(today, datetime.min.time()).replace(tzinfo=IST)
                      - timedelta(days=MAX_ZONE_AGE_DAYS))
        monitors = [m for m in monitors
                    if not m.done and not m.invalid and m.d1_reclaim_ts >= age_cutoff]
        flip_zones = [f for f in flip_zones
                      if not f.done and f.d1_reclaim_ts >= age_cutoff]
        tweak_setups = [t for t in tweak_setups
                        if not t.done and t.d1_reclaim_ts >= age_cutoff]

        day_h1 = sorted(h1_by_date[today], key=lambda b: b.timestamp)
        day_m5 = sorted(m5_by_date.get(today, []), key=lambda b: b.timestamp)
        pending_5m: Optional[dict] = None

        for bar in day_h1:
            hm = (bar.timestamp.hour, bar.timestamp.minute)
            if hm < SESSION_OPEN:
                prev_h1_bar = bar
                continue

            if open_trade is not None and exit_mode == "tsl_1h" and prev_h1_bar is not None:
                if open_trade.direction == "LONG":
                    tsl_level = max(tsl_level, prev_h1_bar.low)
                else:
                    tsl_level = min(tsl_level, prev_h1_bar.high)

            m5s = [b for b in day_m5
                   if bar.timestamp <= b.timestamp < bar.timestamp + timedelta(hours=1)]

            # Exit checks
            if open_trade is not None:
                exited = False
                if open_trade.direction == "LONG" and bar.low <= open_trade.sl:
                    open_trade.sl_ts = bar.timestamp
                    open_trade.exit_price = open_trade.sl
                    open_trade.pnl_pts = open_trade.sl - open_trade.entry
                    trades.append(open_trade); open_trade = None; exited = True
                elif open_trade.direction == "SHORT" and bar.high >= open_trade.sl:
                    open_trade.sl_ts = bar.timestamp
                    open_trade.exit_price = open_trade.sl
                    open_trade.pnl_pts = open_trade.entry - open_trade.sl
                    trades.append(open_trade); open_trade = None; exited = True
                if not exited and open_trade is not None:
                    if exit_mode == "tsl_1h":
                        for m5 in m5s:
                            tsl_hit = ((open_trade.direction == "LONG" and m5.close < tsl_level) or
                                       (open_trade.direction == "SHORT" and m5.close > tsl_level))
                            if tsl_hit:
                                open_trade.sl_ts = m5.timestamp
                                open_trade.exit_price = tsl_level
                                open_trade.pnl_pts = (tsl_level - open_trade.entry
                                    if open_trade.direction == "LONG"
                                    else open_trade.entry - tsl_level)
                                trades.append(open_trade); open_trade = None; exited = True; break
                    else:
                        opp_dir = "SHORT" if open_trade.direction == "LONG" else "LONG"
                        for m in monitors:
                            if m.done or m.invalid or m.direction != opp_dir:
                                continue
                            if m.d1_reclaim_ts <= open_trade.entry_ts:
                                continue
                            entered = ((opp_dir == "SHORT" and bar.high >= m.zone_lo) or
                                       (opp_dir == "LONG" and bar.low <= m.zone_hi))
                            if entered:
                                ep = m.zone_lo if opp_dir == "SHORT" else m.zone_hi
                                open_trade.opp_exit_ts = bar.timestamp
                                open_trade.exit_price = ep
                                open_trade.pnl_pts = (ep - open_trade.entry
                                    if open_trade.direction == "LONG"
                                    else open_trade.entry - ep)
                                trades.append(open_trade); open_trade = None; exited = True; break

            # Zone monitoring + invalidation (generates flip/tweak when zone fails)
            for m in monitors:
                if m.done or m.invalid:
                    continue
                if m.state == "WAITING":
                    if m.direction == "LONG" and bar.low <= m.zone_hi:
                        m.state = "MONITORING"; m.zone_entry_ts = bar.timestamp; m.ref_bar = bar
                    elif m.direction == "SHORT" and bar.high >= m.zone_lo:
                        m.state = "MONITORING"; m.zone_entry_ts = bar.timestamp; m.ref_bar = bar
                was_monitoring = (m.state == "MONITORING")
                if m.direction == "LONG" and bar.close < m.zone_lo:
                    m.invalid = True
                    if use_flip:
                        flip_zones.append(_FlipZone(
                            original_dir="LONG", trade_dir="SHORT",
                            zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                            d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                            d1_reclaim_ts=m.d1_reclaim_ts, invalidated_ts=bar.timestamp,
                        ))
                    if use_tweak and was_monitoring and m.zone_entry_ts is not None:
                        tweak_setups.append(_TweakSetup(
                            trade_dir="SHORT", failure_bar=bar,
                            zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                            d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                            d1_reclaim_ts=m.d1_reclaim_ts, zone_entry_ts=m.zone_entry_ts,
                        ))
                elif m.direction == "SHORT" and bar.close > m.zone_hi:
                    m.invalid = True
                    if use_flip:
                        flip_zones.append(_FlipZone(
                            original_dir="SHORT", trade_dir="LONG",
                            zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                            d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                            d1_reclaim_ts=m.d1_reclaim_ts, invalidated_ts=bar.timestamp,
                        ))
                    if use_tweak and was_monitoring and m.zone_entry_ts is not None:
                        tweak_setups.append(_TweakSetup(
                            trade_dir="LONG", failure_bar=bar,
                            zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                            d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                            d1_reclaim_ts=m.d1_reclaim_ts, zone_entry_ts=m.zone_entry_ts,
                        ))

            prev_h1_bar = bar

            if open_trade is not None or hm >= ENTRY_CUTOFF:
                continue

            # ── PRIORITY 1: C2 — regular 5M breakout from valid zones ─────
            if pending_5m is not None:
                pl = pending_5m
                for m5 in m5s:
                    hit = ((pl["direction"] == "LONG"  and m5.high > pl["trigger"]) or
                           (pl["direction"] == "SHORT" and m5.low  < pl["trigger"]))
                    if hit:
                        open_trade = Trade(
                            method="5M", direction=pl["direction"],
                            zone_lo=pl["zone_lo_d1"], zone_hi=pl["zone_hi_d1"],
                            d1_ref_ts=pl["d1_ref_ts"], d1_sweep_ts=pl["d1_sweep_ts"],
                            d1_reclaim_ts=pl["d1_reclaim_ts"],
                            zone_entry_ts=pl["zone_entry_ts"], ref_bar_ts=pl["ref_bar_ts"],
                            entry_ts=m5.timestamp, entry=pl["trigger"], sl=pl["sl"],
                            m5_zone_lo=pl.get("m5_lo"), m5_zone_hi=pl.get("m5_hi"),
                        )
                        tsl_level = pl["sl"]; pending_5m = None; break
                if open_trade is not None:
                    continue

            for m in monitors:
                if m.done or m.invalid:
                    continue
                if m.state == "MONITORING" and m.ref_bar is not None:
                    ref = m.ref_bar
                    direction = m.direction
                    triggered = ((direction == "LONG"  and bar.high > ref.high) or
                                 (direction == "SHORT" and bar.low  < ref.low))
                    if not triggered:
                        m.ref_bar = bar; continue
                    sl_p = ref.low if direction == "LONG" else ref.high
                    m.done = True
                    result = _detect_5m_zone(direction, ref, day_m5)
                    if result is not None:
                        z_lo, z_hi = result
                        trigger_lvl = z_hi if direction == "LONG" else z_lo
                        risk = abs(trigger_lvl - sl_p)
                        if risk > 0:
                            meta = dict(
                                direction=direction,
                                zone_lo_d1=m.zone_lo, zone_hi_d1=m.zone_hi,
                                d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                                d1_reclaim_ts=m.d1_reclaim_ts,
                                zone_entry_ts=m.zone_entry_ts, ref_bar_ts=ref.timestamp,
                                trigger=trigger_lvl, sl=sl_p, m5_lo=z_lo, m5_hi=z_hi,
                            )
                            filled = False
                            for m5 in m5s:
                                hit = ((direction == "LONG"  and m5.high > trigger_lvl) or
                                       (direction == "SHORT" and m5.low  < trigger_lvl))
                                if hit:
                                    open_trade = Trade(
                                        method="5M", direction=direction,
                                        zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                                        d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                                        d1_reclaim_ts=m.d1_reclaim_ts,
                                        zone_entry_ts=m.zone_entry_ts, ref_bar_ts=ref.timestamp,
                                        entry_ts=m5.timestamp, entry=trigger_lvl, sl=sl_p,
                                        m5_zone_lo=z_lo, m5_zone_hi=z_hi,
                                    )
                                    tsl_level = sl_p; filled = True; break
                            if not filled:
                                pending_5m = meta
                    else:
                        entry = ref.high if direction == "LONG" else ref.low
                        risk = abs(entry - sl_p)
                        if risk > 0:
                            open_trade = Trade(
                                method="5M_FB", direction=direction,
                                zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                                d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                                d1_reclaim_ts=m.d1_reclaim_ts,
                                zone_entry_ts=m.zone_entry_ts, ref_bar_ts=ref.timestamp,
                                entry_ts=bar.timestamp, entry=entry, sl=sl_p,
                            )
                            tsl_level = sl_p
                    if open_trade is not None:
                        break

            if open_trade is not None:
                continue

            # ── PRIORITY 2: TWEAK — immediate failure counter-trade ───────
            if use_tweak:
                for ts in tweak_setups:
                    if ts.done or bar.timestamp <= ts.failure_bar.timestamp:
                        continue
                    ref = ts.failure_bar
                    if ts.trade_dir == "SHORT":
                        if bar.low < ref.low:
                            result = _detect_5m_zone("SHORT", ref, day_m5)
                            sl_p = ref.high; entry = ref.low; method = "TWEAK_FB"
                            m5_lo = m5_hi = None
                            if result is not None:
                                z_lo, z_hi = result
                                if sl_p - z_lo > 0:
                                    entry = z_lo; method = "TWEAK_5M"; m5_lo, m5_hi = z_lo, z_hi
                            if sl_p - entry > 0:
                                open_trade = Trade(
                                    method=method, direction="SHORT",
                                    zone_lo=ts.zone_lo, zone_hi=ts.zone_hi,
                                    d1_ref_ts=ts.d1_ref_ts, d1_sweep_ts=ts.d1_sweep_ts,
                                    d1_reclaim_ts=ts.d1_reclaim_ts,
                                    zone_entry_ts=ts.zone_entry_ts, ref_bar_ts=ref.timestamp,
                                    entry_ts=bar.timestamp, entry=entry, sl=sl_p,
                                    m5_zone_lo=m5_lo, m5_zone_hi=m5_hi,
                                )
                                tsl_level = sl_p
                            ts.done = True
                    else:
                        if bar.high > ref.high:
                            result = _detect_5m_zone("LONG", ref, day_m5)
                            sl_p = ref.low; entry = ref.high; method = "TWEAK_FB"
                            m5_lo = m5_hi = None
                            if result is not None:
                                z_lo, z_hi = result
                                if z_hi - sl_p > 0:
                                    entry = z_hi; method = "TWEAK_5M"; m5_lo, m5_hi = z_lo, z_hi
                            if entry - sl_p > 0:
                                open_trade = Trade(
                                    method=method, direction="LONG",
                                    zone_lo=ts.zone_lo, zone_hi=ts.zone_hi,
                                    d1_ref_ts=ts.d1_ref_ts, d1_sweep_ts=ts.d1_sweep_ts,
                                    d1_reclaim_ts=ts.d1_reclaim_ts,
                                    zone_entry_ts=ts.zone_entry_ts, ref_bar_ts=ref.timestamp,
                                    entry_ts=bar.timestamp, entry=entry, sl=sl_p,
                                    m5_zone_lo=m5_lo, m5_zone_hi=m5_hi,
                                )
                                tsl_level = sl_p
                            ts.done = True
                    if open_trade is not None:
                        break

            if open_trade is not None:
                continue

            # ── PRIORITY 3: FLIP — delayed retest of failed zone boundary ─
            if use_flip:
                for fz in flip_zones:
                    if fz.done:
                        continue
                    if fz.state == "WAITING":
                        if fz.trade_dir == "SHORT":
                            if bar.high >= fz.zone_lo and prev_h1_bar is not None and prev_h1_bar.close < fz.zone_lo:
                                fz.state = "MONITORING"; fz.ref_bar = bar; fz.tap_ts = bar.timestamp
                        else:
                            if bar.low <= fz.zone_hi and prev_h1_bar is not None and prev_h1_bar.close > fz.zone_hi:
                                fz.state = "MONITORING"; fz.ref_bar = bar; fz.tap_ts = bar.timestamp
                    elif fz.state == "MONITORING" and fz.ref_bar is not None:
                        ref = fz.ref_bar
                        if fz.trade_dir == "SHORT":
                            if bar.low < ref.low:
                                result = _detect_5m_zone("SHORT", ref, day_m5)
                                sl_p = fz.zone_hi; entry = ref.low; method = "FLIP_FB"
                                m5_lo = m5_hi = None
                                if result is not None:
                                    z_lo, z_hi = result
                                    if sl_p - z_lo > 0:
                                        entry = z_lo; method = "FLIP_5M"; m5_lo, m5_hi = z_lo, z_hi
                                if sl_p - entry > 0:
                                    open_trade = Trade(
                                        method=method, direction="SHORT",
                                        zone_lo=fz.zone_lo, zone_hi=fz.zone_hi,
                                        d1_ref_ts=fz.d1_ref_ts, d1_sweep_ts=fz.d1_sweep_ts,
                                        d1_reclaim_ts=fz.d1_reclaim_ts,
                                        zone_entry_ts=fz.tap_ts, ref_bar_ts=ref.timestamp,
                                        entry_ts=bar.timestamp, entry=entry, sl=sl_p,
                                        m5_zone_lo=m5_lo, m5_zone_hi=m5_hi,
                                    )
                                    tsl_level = sl_p
                                fz.done = True
                            else:
                                fz.ref_bar = bar
                        else:
                            if bar.high > ref.high:
                                result = _detect_5m_zone("LONG", ref, day_m5)
                                sl_p = fz.zone_lo; entry = ref.high; method = "FLIP_FB"
                                m5_lo = m5_hi = None
                                if result is not None:
                                    z_lo, z_hi = result
                                    if z_hi - sl_p > 0:
                                        entry = z_hi; method = "FLIP_5M"; m5_lo, m5_hi = z_lo, z_hi
                                if entry - sl_p > 0:
                                    open_trade = Trade(
                                        method=method, direction="LONG",
                                        zone_lo=fz.zone_lo, zone_hi=fz.zone_hi,
                                        d1_ref_ts=fz.d1_ref_ts, d1_sweep_ts=fz.d1_sweep_ts,
                                        d1_reclaim_ts=fz.d1_reclaim_ts,
                                        zone_entry_ts=fz.tap_ts, ref_bar_ts=ref.timestamp,
                                        entry_ts=bar.timestamp, entry=entry, sl=sl_p,
                                        m5_zone_lo=m5_lo, m5_zone_hi=m5_hi,
                                    )
                                    tsl_level = sl_p
                                fz.done = True
                            else:
                                fz.ref_bar = bar
                    if open_trade is not None:
                        break

        # EOD resets
        for m in monitors:
            if m.state == "MONITORING":
                m.state = "WAITING"; m.ref_bar = None; m.zone_entry_ts = None
        for fz in flip_zones:
            if fz.state == "MONITORING":
                fz.state = "WAITING"; fz.ref_bar = None; fz.tap_ts = None
        pending_5m = None

    if open_trade is not None:
        trades.append(open_trade)
    return trades


# ── FLIP strategy ─────────────────────────────────────────────────────────────
def run_backtest_flip(
    d1_bars: List[_Bar],
    h1_bars: List[_Bar],
    m5_bars: List[_Bar],
    exit_mode: str = "tsl_1h",
) -> List[Trade]:
    """
    Zone Flip strategy — when a D1 zone fails, the broken boundary flips role:

    Bear zone (LONG direction) fails (1H closes below zone_lo):
      zone_lo becomes RESISTANCE.
      Wait for 1H bar to touch zone_lo from BELOW (bar.close < zone_lo = still below).
      That bar = Flip Ref Bar.
      Next 1H bar LOW < Flip Ref Bar LOW -> rejection confirmed -> SHORT entry.
      Find 5M BULL TRAP inside Flip Ref Bar -> SHORT entry at 5M zone_lo.
      SL = D1 zone_hi (original zone ceiling).

    Bull zone (SHORT direction) fails (1H closes above zone_hi):
      zone_hi becomes SUPPORT.
      Wait for 1H bar to touch zone_hi from ABOVE (bar.close > zone_hi = still above).
      Next 1H bar HIGH > Flip Ref Bar HIGH -> rejection confirmed -> LONG entry.
      Find 5M BEAR TRAP inside Flip Ref Bar -> LONG entry at 5M zone_hi.
      SL = D1 zone_lo.
    """
    trades: List[Trade] = []
    monitors: List[_Monitor] = []
    flip_zones: List[_FlipZone] = []
    known_bear: Set[datetime] = set()
    known_bull: Set[datetime] = set()

    h1_by_date: Dict[date, List[_Bar]] = {}
    for b in h1_bars:
        h1_by_date.setdefault(b.timestamp.date(), []).append(b)
    m5_by_date: Dict[date, List[_Bar]] = {}
    for b in m5_bars:
        m5_by_date.setdefault(b.timestamp.date(), []).append(b)

    open_trade: Optional[Trade] = None
    tsl_level: float = 0.0
    prev_h1_bar: Optional[_Bar] = None

    for today in sorted(h1_by_date):
        d1_avail = [b for b in d1_bars if b.timestamp.date() < today]
        if len(d1_avail) >= 3:
            for z in find_all_bear_zones(d1_avail, known_ref_ts=known_bear.copy()):
                if z.reference_low_ts not in known_bear:
                    known_bear.add(z.reference_low_ts)
                    monitors.append(_Monitor(
                        direction="LONG",
                        d1_ref_ts=z.reference_low_ts, d1_sweep_ts=z.sweep_started_ts,
                        d1_reclaim_ts=z.lock_ts,
                        zone_lo=min(z.entry_line, z.sweep_low),
                        zone_hi=max(z.entry_line, z.sweep_low),
                    ))
            for z in find_all_bull_zones(d1_avail, known_ref_ts=known_bull.copy()):
                if z.reference_low_ts not in known_bull:
                    known_bull.add(z.reference_low_ts)
                    monitors.append(_Monitor(
                        direction="SHORT",
                        d1_ref_ts=z.reference_low_ts, d1_sweep_ts=z.sweep_started_ts,
                        d1_reclaim_ts=z.lock_ts,
                        zone_lo=min(z.entry_line, z.sweep_low),
                        zone_hi=max(z.entry_line, z.sweep_low),
                    ))

        age_cutoff = (datetime.combine(today, datetime.min.time()).replace(tzinfo=IST)
                      - timedelta(days=MAX_ZONE_AGE_DAYS))
        monitors = [m for m in monitors
                    if not m.done and not m.invalid and m.d1_reclaim_ts >= age_cutoff]
        flip_zones = [f for f in flip_zones
                      if not f.done and f.d1_reclaim_ts >= age_cutoff]

        day_h1 = sorted(h1_by_date[today], key=lambda b: b.timestamp)
        day_m5 = sorted(m5_by_date.get(today, []), key=lambda b: b.timestamp)

        for bar in day_h1:
            hm = (bar.timestamp.hour, bar.timestamp.minute)
            if hm < SESSION_OPEN:
                prev_h1_bar = bar
                continue

            if open_trade is not None and exit_mode == "tsl_1h" and prev_h1_bar is not None:
                if open_trade.direction == "LONG":
                    tsl_level = max(tsl_level, prev_h1_bar.low)
                else:
                    tsl_level = min(tsl_level, prev_h1_bar.high)

            m5s = [b for b in day_m5
                   if bar.timestamp <= b.timestamp < bar.timestamp + timedelta(hours=1)]

            # Exit checks
            if open_trade is not None:
                exited = False
                if open_trade.direction == "LONG" and bar.low <= open_trade.sl:
                    open_trade.sl_ts = bar.timestamp
                    open_trade.exit_price = open_trade.sl
                    open_trade.pnl_pts = open_trade.sl - open_trade.entry
                    trades.append(open_trade); open_trade = None; exited = True
                elif open_trade.direction == "SHORT" and bar.high >= open_trade.sl:
                    open_trade.sl_ts = bar.timestamp
                    open_trade.exit_price = open_trade.sl
                    open_trade.pnl_pts = open_trade.entry - open_trade.sl
                    trades.append(open_trade); open_trade = None; exited = True
                if not exited and open_trade is not None:
                    if exit_mode == "tsl_1h":
                        for m5 in m5s:
                            tsl_hit = ((open_trade.direction == "LONG" and m5.close < tsl_level) or
                                       (open_trade.direction == "SHORT" and m5.close > tsl_level))
                            if tsl_hit:
                                open_trade.sl_ts = m5.timestamp
                                open_trade.exit_price = tsl_level
                                open_trade.pnl_pts = (tsl_level - open_trade.entry
                                    if open_trade.direction == "LONG"
                                    else open_trade.entry - tsl_level)
                                trades.append(open_trade); open_trade = None; exited = True; break
                    else:  # opp_trap
                        opp_dir = "SHORT" if open_trade.direction == "LONG" else "LONG"
                        for m in monitors:
                            if m.done or m.invalid or m.direction != opp_dir:
                                continue
                            if m.d1_reclaim_ts <= open_trade.entry_ts:
                                continue
                            entered = ((opp_dir == "SHORT" and bar.high >= m.zone_lo) or
                                       (opp_dir == "LONG" and bar.low <= m.zone_hi))
                            if entered:
                                ep = m.zone_lo if opp_dir == "SHORT" else m.zone_hi
                                open_trade.opp_exit_ts = bar.timestamp
                                open_trade.exit_price = ep
                                open_trade.pnl_pts = (ep - open_trade.entry
                                    if open_trade.direction == "LONG"
                                    else open_trade.entry - ep)
                                trades.append(open_trade); open_trade = None; exited = True; break

            # Zone invalidation -> generate flip zones
            for m in monitors:
                if m.done or m.invalid:
                    continue
                if m.direction == "LONG" and bar.close < m.zone_lo:
                    m.invalid = True
                    flip_zones.append(_FlipZone(
                        original_dir="LONG", trade_dir="SHORT",
                        zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                        d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                        d1_reclaim_ts=m.d1_reclaim_ts,
                        invalidated_ts=bar.timestamp,
                    ))
                elif m.direction == "SHORT" and bar.close > m.zone_hi:
                    m.invalid = True
                    flip_zones.append(_FlipZone(
                        original_dir="SHORT", trade_dir="LONG",
                        zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                        d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                        d1_reclaim_ts=m.d1_reclaim_ts,
                        invalidated_ts=bar.timestamp,
                    ))

            prev_h1_bar = bar

            if open_trade is not None or hm >= ENTRY_CUTOFF:
                continue

            # Process flip zones
            for fz in flip_zones:
                if fz.done:
                    continue

                if fz.state == "WAITING":
                    if fz.trade_dir == "SHORT":
                        # bar touches zone_lo from below; approach_ok = bar still closed BELOW
                        if bar.high >= fz.zone_lo and prev_h1_bar is not None and prev_h1_bar.close < fz.zone_lo:
                            fz.state = "MONITORING"
                            fz.ref_bar = bar
                            fz.tap_ts = bar.timestamp
                    else:  # LONG: bar touches zone_hi from above
                        if bar.low <= fz.zone_hi and prev_h1_bar is not None and prev_h1_bar.close > fz.zone_hi:
                            fz.state = "MONITORING"
                            fz.ref_bar = bar
                            fz.tap_ts = bar.timestamp

                elif fz.state == "MONITORING" and fz.ref_bar is not None:
                    ref = fz.ref_bar
                    if fz.trade_dir == "SHORT":
                        if bar.low < ref.low:  # rejection at zone_lo confirmed
                            result = _detect_5m_zone("SHORT", ref, day_m5)
                            sl_p = fz.zone_hi
                            entry = ref.low  # fallback
                            method = "FLIP_FB"
                            m5_lo = m5_hi = None
                            if result is not None:
                                z_lo, z_hi = result
                                if sl_p - z_lo > 0:
                                    # check if 5M breakdown already happened in this bar
                                    for m5 in m5s:
                                        if m5.low < z_lo:
                                            entry = z_lo; method = "FLIP_5M"
                                            m5_lo, m5_hi = z_lo, z_hi; break
                                    if method == "FLIP_FB":
                                        entry = z_lo; method = "FLIP_5M"
                                        m5_lo, m5_hi = z_lo, z_hi
                            risk = sl_p - entry
                            if risk > 0:
                                open_trade = Trade(
                                    method=method, direction="SHORT",
                                    zone_lo=fz.zone_lo, zone_hi=fz.zone_hi,
                                    d1_ref_ts=fz.d1_ref_ts, d1_sweep_ts=fz.d1_sweep_ts,
                                    d1_reclaim_ts=fz.d1_reclaim_ts,
                                    zone_entry_ts=fz.tap_ts, ref_bar_ts=ref.timestamp,
                                    entry_ts=bar.timestamp, entry=entry, sl=sl_p,
                                    m5_zone_lo=m5_lo, m5_zone_hi=m5_hi,
                                )
                                tsl_level = sl_p
                            fz.done = True
                        else:
                            fz.ref_bar = bar  # roll forward (no breach yet)
                    else:  # LONG
                        if bar.high > ref.high:  # rejection at zone_hi confirmed
                            result = _detect_5m_zone("LONG", ref, day_m5)
                            sl_p = fz.zone_lo
                            entry = ref.high
                            method = "FLIP_FB"
                            m5_lo = m5_hi = None
                            if result is not None:
                                z_lo, z_hi = result
                                if z_hi - sl_p > 0:
                                    for m5 in m5s:
                                        if m5.high > z_hi:
                                            entry = z_hi; method = "FLIP_5M"
                                            m5_lo, m5_hi = z_lo, z_hi; break
                                    if method == "FLIP_FB":
                                        entry = z_hi; method = "FLIP_5M"
                                        m5_lo, m5_hi = z_lo, z_hi
                            risk = entry - sl_p
                            if risk > 0:
                                open_trade = Trade(
                                    method=method, direction="LONG",
                                    zone_lo=fz.zone_lo, zone_hi=fz.zone_hi,
                                    d1_ref_ts=fz.d1_ref_ts, d1_sweep_ts=fz.d1_sweep_ts,
                                    d1_reclaim_ts=fz.d1_reclaim_ts,
                                    zone_entry_ts=fz.tap_ts, ref_bar_ts=ref.timestamp,
                                    entry_ts=bar.timestamp, entry=entry, sl=sl_p,
                                    m5_zone_lo=m5_lo, m5_zone_hi=m5_hi,
                                )
                                tsl_level = sl_p
                            fz.done = True
                        else:
                            fz.ref_bar = bar

                if open_trade is not None:
                    break

        for fz in flip_zones:
            if fz.state == "MONITORING":
                fz.state = "WAITING"
                fz.ref_bar = None
                fz.tap_ts = None

    if open_trade is not None:
        trades.append(open_trade)
    return trades


# ── TWEAK strategy ────────────────────────────────────────────────────────────
def run_backtest_tweak(
    d1_bars: List[_Bar],
    h1_bars: List[_Bar],
    m5_bars: List[_Bar],
    exit_mode: str = "tsl_1h",
) -> List[Trade]:
    """
    Immediate Failure Counter-Trade (Tweak).

    Requires a zone to be in MONITORING state (price already inside zone) when it
    gets invalidated. The failure bar itself becomes the Ref Bar for the opposite trade:

    Bear zone (LONG) was monitoring AND 1H bar closes below zone_lo:
      failure_bar = that 1H bar.
      Next 1H bar LOW < failure_bar.LOW -> bearish continuation -> SHORT entry.
      Find 5M BULL TRAP inside failure_bar -> SHORT at 5M zone_lo (buyers trapped).
      SL = failure_bar.HIGH.

    Bull zone (SHORT) was monitoring AND 1H bar closes above zone_hi:
      Next 1H bar HIGH > failure_bar.HIGH -> bullish continuation -> LONG entry.
      Find 5M BEAR TRAP inside failure_bar -> LONG at 5M zone_hi.
      SL = failure_bar.LOW.
    """
    trades: List[Trade] = []
    monitors: List[_Monitor] = []
    tweak_setups: List[_TweakSetup] = []
    known_bear: Set[datetime] = set()
    known_bull: Set[datetime] = set()

    h1_by_date: Dict[date, List[_Bar]] = {}
    for b in h1_bars:
        h1_by_date.setdefault(b.timestamp.date(), []).append(b)
    m5_by_date: Dict[date, List[_Bar]] = {}
    for b in m5_bars:
        m5_by_date.setdefault(b.timestamp.date(), []).append(b)

    open_trade: Optional[Trade] = None
    tsl_level: float = 0.0
    prev_h1_bar: Optional[_Bar] = None

    for today in sorted(h1_by_date):
        d1_avail = [b for b in d1_bars if b.timestamp.date() < today]
        if len(d1_avail) >= 3:
            for z in find_all_bear_zones(d1_avail, known_ref_ts=known_bear.copy()):
                if z.reference_low_ts not in known_bear:
                    known_bear.add(z.reference_low_ts)
                    monitors.append(_Monitor(
                        direction="LONG",
                        d1_ref_ts=z.reference_low_ts, d1_sweep_ts=z.sweep_started_ts,
                        d1_reclaim_ts=z.lock_ts,
                        zone_lo=min(z.entry_line, z.sweep_low),
                        zone_hi=max(z.entry_line, z.sweep_low),
                    ))
            for z in find_all_bull_zones(d1_avail, known_ref_ts=known_bull.copy()):
                if z.reference_low_ts not in known_bull:
                    known_bull.add(z.reference_low_ts)
                    monitors.append(_Monitor(
                        direction="SHORT",
                        d1_ref_ts=z.reference_low_ts, d1_sweep_ts=z.sweep_started_ts,
                        d1_reclaim_ts=z.lock_ts,
                        zone_lo=min(z.entry_line, z.sweep_low),
                        zone_hi=max(z.entry_line, z.sweep_low),
                    ))

        age_cutoff = (datetime.combine(today, datetime.min.time()).replace(tzinfo=IST)
                      - timedelta(days=MAX_ZONE_AGE_DAYS))
        monitors = [m for m in monitors
                    if not m.done and not m.invalid and m.d1_reclaim_ts >= age_cutoff]
        tweak_setups = [t for t in tweak_setups
                        if not t.done and t.d1_reclaim_ts >= age_cutoff]

        day_h1 = sorted(h1_by_date[today], key=lambda b: b.timestamp)
        day_m5 = sorted(m5_by_date.get(today, []), key=lambda b: b.timestamp)

        for bar in day_h1:
            hm = (bar.timestamp.hour, bar.timestamp.minute)
            if hm < SESSION_OPEN:
                prev_h1_bar = bar
                continue

            if open_trade is not None and exit_mode == "tsl_1h" and prev_h1_bar is not None:
                if open_trade.direction == "LONG":
                    tsl_level = max(tsl_level, prev_h1_bar.low)
                else:
                    tsl_level = min(tsl_level, prev_h1_bar.high)

            m5s = [b for b in day_m5
                   if bar.timestamp <= b.timestamp < bar.timestamp + timedelta(hours=1)]

            # Exit checks
            if open_trade is not None:
                exited = False
                if open_trade.direction == "LONG" and bar.low <= open_trade.sl:
                    open_trade.sl_ts = bar.timestamp
                    open_trade.exit_price = open_trade.sl
                    open_trade.pnl_pts = open_trade.sl - open_trade.entry
                    trades.append(open_trade); open_trade = None; exited = True
                elif open_trade.direction == "SHORT" and bar.high >= open_trade.sl:
                    open_trade.sl_ts = bar.timestamp
                    open_trade.exit_price = open_trade.sl
                    open_trade.pnl_pts = open_trade.entry - open_trade.sl
                    trades.append(open_trade); open_trade = None; exited = True
                if not exited and open_trade is not None:
                    if exit_mode == "tsl_1h":
                        for m5 in m5s:
                            tsl_hit = ((open_trade.direction == "LONG" and m5.close < tsl_level) or
                                       (open_trade.direction == "SHORT" and m5.close > tsl_level))
                            if tsl_hit:
                                open_trade.sl_ts = m5.timestamp
                                open_trade.exit_price = tsl_level
                                open_trade.pnl_pts = (tsl_level - open_trade.entry
                                    if open_trade.direction == "LONG"
                                    else open_trade.entry - tsl_level)
                                trades.append(open_trade); open_trade = None; exited = True; break
                    else:  # opp_trap
                        opp_dir = "SHORT" if open_trade.direction == "LONG" else "LONG"
                        for m in monitors:
                            if m.done or m.invalid or m.direction != opp_dir:
                                continue
                            if m.d1_reclaim_ts <= open_trade.entry_ts:
                                continue
                            entered = ((opp_dir == "SHORT" and bar.high >= m.zone_lo) or
                                       (opp_dir == "LONG" and bar.low <= m.zone_hi))
                            if entered:
                                ep = m.zone_lo if opp_dir == "SHORT" else m.zone_hi
                                open_trade.opp_exit_ts = bar.timestamp
                                open_trade.exit_price = ep
                                open_trade.pnl_pts = (ep - open_trade.entry
                                    if open_trade.direction == "LONG"
                                    else open_trade.entry - ep)
                                trades.append(open_trade); open_trade = None; exited = True; break

            # Zone monitoring + invalidation -> tweak setup when zone was monitoring
            for m in monitors:
                if m.done or m.invalid:
                    continue
                # Update zone monitoring state (same logic as positional)
                if m.state == "WAITING":
                    if m.direction == "LONG" and bar.low <= m.zone_hi:
                        m.state = "MONITORING"
                        m.zone_entry_ts = bar.timestamp
                        m.ref_bar = bar
                    elif m.direction == "SHORT" and bar.high >= m.zone_lo:
                        m.state = "MONITORING"
                        m.zone_entry_ts = bar.timestamp
                        m.ref_bar = bar
                # Invalidation check
                was_monitoring = (m.state == "MONITORING")
                if m.direction == "LONG" and bar.close < m.zone_lo:
                    m.invalid = True
                    if was_monitoring and m.zone_entry_ts is not None:
                        tweak_setups.append(_TweakSetup(
                            trade_dir="SHORT", failure_bar=bar,
                            zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                            d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                            d1_reclaim_ts=m.d1_reclaim_ts,
                            zone_entry_ts=m.zone_entry_ts,
                        ))
                elif m.direction == "SHORT" and bar.close > m.zone_hi:
                    m.invalid = True
                    if was_monitoring and m.zone_entry_ts is not None:
                        tweak_setups.append(_TweakSetup(
                            trade_dir="LONG", failure_bar=bar,
                            zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                            d1_ref_ts=m.d1_ref_ts, d1_sweep_ts=m.d1_sweep_ts,
                            d1_reclaim_ts=m.d1_reclaim_ts,
                            zone_entry_ts=m.zone_entry_ts,
                        ))

            prev_h1_bar = bar

            if open_trade is not None or hm >= ENTRY_CUTOFF:
                continue

            # Process tweak setups (only bars AFTER the failure bar)
            for ts in tweak_setups:
                if ts.done or bar.timestamp <= ts.failure_bar.timestamp:
                    continue
                ref = ts.failure_bar
                if ts.trade_dir == "SHORT":
                    if bar.low < ref.low:  # momentum continues down
                        result = _detect_5m_zone("SHORT", ref, day_m5)
                        sl_p = ref.high
                        entry = ref.low
                        method = "TWEAK_FB"
                        m5_lo = m5_hi = None
                        if result is not None:
                            z_lo, z_hi = result
                            if sl_p - z_lo > 0:
                                entry = z_lo; method = "TWEAK_5M"
                                m5_lo, m5_hi = z_lo, z_hi
                        risk = sl_p - entry
                        if risk > 0:
                            open_trade = Trade(
                                method=method, direction="SHORT",
                                zone_lo=ts.zone_lo, zone_hi=ts.zone_hi,
                                d1_ref_ts=ts.d1_ref_ts, d1_sweep_ts=ts.d1_sweep_ts,
                                d1_reclaim_ts=ts.d1_reclaim_ts,
                                zone_entry_ts=ts.zone_entry_ts, ref_bar_ts=ref.timestamp,
                                entry_ts=bar.timestamp, entry=entry, sl=sl_p,
                                m5_zone_lo=m5_lo, m5_zone_hi=m5_hi,
                            )
                            tsl_level = sl_p
                        ts.done = True
                else:  # LONG
                    if bar.high > ref.high:
                        result = _detect_5m_zone("LONG", ref, day_m5)
                        sl_p = ref.low
                        entry = ref.high
                        method = "TWEAK_FB"
                        m5_lo = m5_hi = None
                        if result is not None:
                            z_lo, z_hi = result
                            if z_hi - sl_p > 0:
                                entry = z_hi; method = "TWEAK_5M"
                                m5_lo, m5_hi = z_lo, z_hi
                        risk = entry - sl_p
                        if risk > 0:
                            open_trade = Trade(
                                method=method, direction="LONG",
                                zone_lo=ts.zone_lo, zone_hi=ts.zone_hi,
                                d1_ref_ts=ts.d1_ref_ts, d1_sweep_ts=ts.d1_sweep_ts,
                                d1_reclaim_ts=ts.d1_reclaim_ts,
                                zone_entry_ts=ts.zone_entry_ts, ref_bar_ts=ref.timestamp,
                                entry_ts=bar.timestamp, entry=entry, sl=sl_p,
                                m5_zone_lo=m5_lo, m5_zone_hi=m5_hi,
                            )
                            tsl_level = sl_p
                        ts.done = True

                if open_trade is not None:
                    break

        for m in monitors:
            if m.state == "MONITORING":
                m.state = "WAITING"
                m.ref_bar = None
                m.zone_entry_ts = None

    if open_trade is not None:
        trades.append(open_trade)
    return trades


# ── Positional comparison stats + print ───────────────────────────────────────
def _pos_stats(trades: List[Trade], label: str) -> None:
    real = [t for t in trades if t.pnl_pts is not None]
    open_ = [t for t in trades if t.pnl_pts is None]
    if not trades:
        print(f"  {label}: no trades")
        return
    win = [t for t in real if (t.pnl_pts or 0) > 0]
    los = [t for t in real if (t.pnl_pts or 0) <= 0]
    gp  = sum(t.pnl_rs for t in win)
    gl  = abs(sum(t.pnl_rs for t in los))
    pf  = gp / gl if gl > 0 else float("inf")
    running = peak = max_dd = 0.0
    for t in real:
        running += t.pnl_rs
        peak = max(peak, running)
        max_dd = max(max_dd, peak - running)
    outcomes: Dict[str, int] = {}
    for t in trades:
        outcomes[t.outcome] = outcomes.get(t.outcome, 0) + 1
    hold_days = []
    for t in real:
        if t.exit_ts:
            hold_days.append((t.exit_ts - t.entry_ts).days)
    avg_hold = sum(hold_days) / len(hold_days) if hold_days else 0
    print(f"\n  {label}")
    print(f"    Trades executed : {len(real)}  (still open: {len(open_)})")
    print(f"    Winners         : {len(win)} ({100*len(win)//len(real) if real else 0}%)")
    print(f"    Losers          : {len(los)}")
    print(f"    Profit factor   : {pf:.2f}")
    print(f"    Net P&L         : Rs {sum(t.pnl_rs for t in real):,.0f}")
    print(f"    Max drawdown    : Rs {max_dd:,.0f}")
    print(f"    Avg hold (days) : {avg_hold:.1f}")
    print(f"    Outcomes        : {outcomes}")


def print_positional_summary(
    trades_a_opp: List[Trade],
    trades_a_tsl: List[Trade],
    trades_c_opp: List[Trade],
    trades_c_tsl: List[Trade],
    start: date, end: date,
    trades_a_opp_s: Optional[List[Trade]] = None,
    trades_a_tsl_s: Optional[List[Trade]] = None,
    trades_c_opp_s: Optional[List[Trade]] = None,
    trades_c_tsl_s: Optional[List[Trade]] = None,
    trades_flip_opp: Optional[List[Trade]] = None,
    trades_flip_tsl: Optional[List[Trade]] = None,
    trades_tweak_opp: Optional[List[Trade]] = None,
    trades_tweak_tsl: Optional[List[Trade]] = None,
    trades_c2_tweak: Optional[List[Trade]] = None,
    trades_c2_flip: Optional[List[Trade]] = None,
    trades_c2_all: Optional[List[Trade]] = None,
) -> None:
    sep = "=" * 80
    print(f"\n{sep}")
    print(f"  POSITIONAL BACKTEST  {start} to {end}  (no EOD, positional exit)")
    print(sep)

    print("\n  -- GROUP 1: OLD LOGIC (no approach/invalidation filter) --")
    print("  Entry: price dips into D1 zone (any direction), 1H ref + breach")
    _pos_stats(trades_a_opp, "A1  1H entry       + Opposite D1 trap exit")
    _pos_stats(trades_a_tsl, "A2  1H entry       + 1H Trailing SL (5-min close)")
    _pos_stats(trades_c_opp, "C1  5M breakout    + Opposite D1 trap exit")
    _pos_stats(trades_c_tsl, "C2  5M breakout    + 1H Trailing SL   [BEST BASELINE]")

    has_strict = trades_a_opp_s is not None
    if has_strict:
        print("\n  -- GROUP 2: STRICT LOGIC (approach-direction + zone-invalidation) --")
        print("  Extra rules: price must come FROM correct side; zone dies if 1H closes through it")
        _pos_stats(trades_a_opp_s, "A1s 1H entry       + Opposite D1 trap exit    [STRICT]")
        _pos_stats(trades_a_tsl_s, "A2s 1H entry       + 1H Trailing SL           [STRICT]")
        _pos_stats(trades_c_opp_s, "C1s 5M breakout    + Opposite D1 trap exit    [STRICT]")
        _pos_stats(trades_c_tsl_s, "C2s 5M breakout    + 1H Trailing SL           [STRICT]")

    has_flip = trades_flip_tsl is not None
    if has_flip:
        print("\n  -- GROUP 3: ZONE FLIP (trade the failed zone boundary from opposite side) --")
        print("  Logic: zone fails -> boundary flips role -> price returns -> 5M trap -> counter trade")
        print("  FLIP LONG example: bear zone zone_lo fails -> zone_lo = resistance")
        print("    Price bounces back UP to zone_lo from below -> 5M BULL trap at resistance")
        print("    -> SHORT entry at 5M zone_lo, SL = original zone_hi")
        _pos_stats(trades_flip_opp, "FP_OPP  Flip trade  + Opposite D1 trap exit")
        _pos_stats(trades_flip_tsl, "FP_TSL  Flip trade  + 1H Trailing SL")

    has_tweak = trades_tweak_tsl is not None
    if has_tweak:
        print("\n  -- GROUP 4: TWEAK (immediate counter-trade on zone failure while monitoring) --")
        print("  Logic: zone was monitoring (price inside zone) -> 1H closes through boundary")
        print("    That failure bar = Ref Bar. Next 1H bar breaches it in same direction -> entry")
        print("    TWEAK SHORT: bear zone (LONG) fails -> next bar goes below failure low")
        print("    -> 5M BULL trap inside failure bar -> SHORT entry, SL = failure bar high")
        _pos_stats(trades_tweak_opp, "TW_OPP  Tweak trade + Opposite D1 trap exit")
        _pos_stats(trades_tweak_tsl, "TW_TSL  Tweak trade + 1H Trailing SL")

    has_combined = trades_c2_all is not None
    if has_combined:
        print("\n  -- GROUP 5: COMBINED (C2 + extra sources, single position, TSL exit) --")
        print("  All entry sources compete for the single slot. Priority: C2 first, then TWEAK, then FLIP.")
        _pos_stats(trades_c2_tweak, "C2+TW   C2 + Tweak entries")
        _pos_stats(trades_c2_flip,  "C2+FP   C2 + Flip entries")
        _pos_stats(trades_c2_all,   "C2+ALL  C2 + Tweak + Flip entries")

    print(f"\n{sep}")

    def st(trades: List[Trade]) -> dict:
        real = [t for t in trades if t.pnl_pts is not None]
        if not real:
            return dict(n=0, wpct=0, pf=0.0, net=0.0, dd=0.0)
        win = [t for t in real if (t.pnl_pts or 0) > 0]
        los = [t for t in real if (t.pnl_pts or 0) <= 0]
        gp = sum(t.pnl_rs for t in win)
        gl = abs(sum(t.pnl_rs for t in los))
        running = peak = max_dd = 0.0
        for t in real:
            running += t.pnl_rs
            peak = max(peak, running)
            max_dd = max(max_dd, peak - running)
        return dict(n=len(real), wpct=100*len(win)//len(real) if real else 0,
                    pf=gp/gl if gl else float("inf"),
                    net=sum(t.pnl_rs for t in real), dd=max_dd)

    # ── Comparison table ─────────────────────────────────────────────
    cols: List[str] = []
    stats: List[dict] = []
    for label, bucket in [
        ("A2",      trades_a_tsl),
        ("C2",      trades_c_tsl),
        ("C2s",     trades_c_tsl_s),
        ("FP_TSL",  trades_flip_tsl),
        ("TW_TSL",  trades_tweak_tsl),
        ("C2+TW",   trades_c2_tweak),
        ("C2+FP",   trades_c2_flip),
        ("C2+ALL",  trades_c2_all),
    ]:
        if bucket is not None:
            cols.append(label)
            stats.append(st(bucket))

    hdr = f"{'Metric':<26}" + "".join(f"{c:>10}" for c in cols)
    print(f"\n{hdr}")
    print("-" * len(hdr))
    for label, key, fmt in [
        ("Trades executed",   "n",    lambda v: f"{v:>10}"),
        ("Win %",             "wpct", lambda v: f"{v:>9}%"),
        ("Profit factor",     "pf",   lambda v: f"{v:>10.2f}"),
        ("Net P&L (Rs)",      "net",  lambda v: f"{v:>10,.0f}"),
        ("Max drawdown (Rs)", "dd",   lambda v: f"{v:>10,.0f}"),
    ]:
        row = f"  {label:<24}" + "".join(fmt(s[key]) for s in stats)
        print(row)

    print(f"\n  Plain-English guide to each variant:")
    print("  A2       : 1H bar breach entry, trail SL each hour — no 5M zone filter")
    print("  C1       : 5M breakout inside 1H ref bar, exit at next opposite D1 zone")
    print("  C2       : 5M breakout inside 1H ref bar, trail SL each hour  [BASELINE BEST]")
    print("  C2s      : C2 + price must approach from correct side + zone dies if 1H closes thru")
    print("  FP_OPP/TSL: Zone failed -> broken boundary = new resistance/support -> trade FLIP")
    print("             Price returns to that boundary from opposite side -> 5M trap -> entry")
    print("  TW_OPP/TSL: Zone failed WHILE price was inside (monitoring) -> immediate counter-")
    print("             trade on next bar breach; 5M trap inside failure bar guides entry")


def save_csv_positional(trades: List[Trade], filename: str) -> str:
    os.makedirs(_RESULTS_DIR, exist_ok=True)
    path = os.path.join(_RESULTS_DIR, filename)
    if not trades:
        return path
    fields = ["Dir", "Zone_Lo", "Zone_Hi", "D1_Ref_TS", "D1_Sweep_TS",
              "D1_Reclaim_TS", "Zone_Tap_TS", "Ref_Bar_TS", "Entry_TS",
              "Entry", "SL_Level", "Risk_pts", "Method",
              "M5_Zone_Lo", "M5_Zone_Hi",
              "SL_Hit_TS", "OppExit_TS", "Exit_Price",
              "Outcome", "PnL_pts", "PnL_Rs"]
    fmt = lambda dt: dt.strftime("%Y-%m-%d %H:%M") if dt else ""
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for t in trades:
            w.writerow({
                "Dir": t.direction, "Zone_Lo": f"{t.zone_lo:.2f}",
                "Zone_Hi": f"{t.zone_hi:.2f}",
                "D1_Ref_TS": fmt(t.d1_ref_ts), "D1_Sweep_TS": fmt(t.d1_sweep_ts),
                "D1_Reclaim_TS": fmt(t.d1_reclaim_ts),
                "Zone_Tap_TS": fmt(t.zone_entry_ts), "Ref_Bar_TS": fmt(t.ref_bar_ts),
                "Entry_TS": fmt(t.entry_ts), "Entry": f"{t.entry:.2f}",
                "SL_Level": f"{t.sl:.2f}",
                "Risk_pts": f"{abs(t.entry - t.sl):.2f}",
                "Method": t.method,
                "M5_Zone_Lo": f"{t.m5_zone_lo:.2f}" if t.m5_zone_lo else "",
                "M5_Zone_Hi": f"{t.m5_zone_hi:.2f}" if t.m5_zone_hi else "",
                "SL_Hit_TS": fmt(t.sl_ts), "OppExit_TS": fmt(t.opp_exit_ts),
                "Exit_Price": f"{t.exit_price:.2f}" if t.exit_price is not None else "",
                "Outcome": t.outcome,
                "PnL_pts": f"{t.pnl_pts:.2f}" if t.pnl_pts is not None else "",
                "PnL_Rs": f"{t.pnl_rs:.0f}",
            })
    return path


# ── reporting ─────────────────────────────────────────────────────────────────
_COL_W = {
    "Dir": 5, "Zone_Lo": 10, "Zone_Hi": 10,
    "D1_Ref_TS": 17, "D1_Sweep_TS": 17, "D1_Reclaim_TS": 17,
    "Zone_Tap_TS": 17, "Ref_Bar_TS": 17, "Entry_TS": 17,
    "Entry": 9, "SL_Level": 9, "Target": 9, "Risk_pts": 8,
    "SL_Hit_TS": 17, "Target_TS": 17, "OppExit_TS": 17, "EOD_TS": 17,
    "Exit_Price": 10, "Outcome": 9, "PnL_pts": 8, "PnL_Rs": 9,
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

    outcome_counts: Dict[str, int] = {}
    for t in trades:
        outcome_counts[t.outcome] = outcome_counts.get(t.outcome, 0) + 1

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
    print(f"{'Outcomes':20}: {outcome_counts}")


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


# ── Side-by-side comparison ───────────────────────────────────────────────────
_CMP_A = {"A_Entry": 9, "A_SL": 9, "A_Risk": 7, "A_Out": 7, "A_PnL": 8}
_CMP_B = {"B_Method": 7, "B_M5Lo": 9, "B_M5Hi": 9,
          "B_Entry": 9, "B_SL": 9, "B_Risk": 7, "B_Out": 7, "B_PnL": 8}
_CMP_KEY = {"Dir": 5, "D1_Ref": 11, "D1_Sweep": 11, "D1_Reclaim": 11,
            "Ref_Bar": 17, "Entry_TS": 17}
_CMP_W = {**_CMP_KEY, **_CMP_A, **_CMP_B}


def _stats_summary(trades: List[Trade]) -> dict:
    real = [t for t in trades if t.method != "SKIP"]
    if not real:
        return {}
    win = [t for t in real if (t.pnl_pts or 0) > 0]
    los = [t for t in real if (t.pnl_pts or 0) <= 0]
    gp = sum(t.pnl_rs for t in win)
    gl = abs(sum(t.pnl_rs for t in los))
    outcomes: Dict[str, int] = {}
    for t in real:
        outcomes[t.outcome] = outcomes.get(t.outcome, 0) + 1
    running = peak = max_dd = 0.0
    for t in real:
        running += t.pnl_rs
        peak = max(peak, running)
        max_dd = max(max_dd, peak - running)
    return dict(n=len(real), win=len(win), los=len(los),
                win_pct=100*len(win)//len(real) if real else 0,
                net=sum(t.pnl_rs for t in real),
                gp=gp, gl=gl, pf=gp/gl if gl else float("inf"),
                max_dd=max_dd, outcomes=outcomes)


def print_comparison(trades_a: List[Trade], trades_5m: List[Trade],
                     start: date, end: date) -> None:
    """Print one row per setup with Strategy A and B results side by side."""
    fmt = lambda dt: dt.strftime("%Y-%m-%d %H:%M") if dt else ""
    fmtd = lambda dt: dt.strftime("%Y-%m-%d") if dt else ""
    fn = lambda v: f"{v:.2f}" if v is not None else ""

    # Index A trades by ref_bar_ts + direction
    a_idx: Dict[tuple, Trade] = {(t.ref_bar_ts, t.direction): t for t in trades_a}
    # Index 5M trades
    b_idx: Dict[tuple, Trade] = {(t.ref_bar_ts, t.direction): t for t in trades_5m}

    # All unique setups (union of both)
    all_keys = sorted(set(list(a_idx.keys()) + list(b_idx.keys())),
                      key=lambda k: k[0])

    sep = "-" * (sum(_CMP_W.values()) + 2 * (len(_CMP_W) - 1))
    hdr = "  ".join(k.rjust(_CMP_W[k]) for k in _CMP_W)
    print(f"\nD1 Trap Comparison (Strategy A=1H breach  |  Strategy B=5M zone)  {start} to {end}")
    print(sep)
    print(hdr)
    print(sep)

    for key in all_keys:
        a = a_idx.get(key)
        b = b_idx.get(key)
        ref_ts = key[0]
        direction = key[1]

        # Common key columns (use A if available, else B)
        src = a or b
        row = {
            "Dir":        direction,
            "D1_Ref":     fmtd(src.d1_ref_ts),
            "D1_Sweep":   fmtd(src.d1_sweep_ts),
            "D1_Reclaim": fmtd(src.d1_reclaim_ts),
            "Ref_Bar":    fmt(ref_ts),
            "Entry_TS":   fmt(src.entry_ts),
        }

        if a:
            row["A_Entry"] = fn(a.entry)
            row["A_SL"]    = fn(a.sl)
            row["A_Risk"]  = fn(abs(a.entry - a.sl))
            row["A_Out"]   = a.outcome
            row["A_PnL"]   = f"{a.pnl_rs:.0f}" if a.pnl_pts is not None else ""
        else:
            for k in _CMP_A:
                row[k] = "-"

        if b:
            row["B_Method"] = b.method
            row["B_M5Lo"]   = fn(b.m5_zone_lo)
            row["B_M5Hi"]   = fn(b.m5_zone_hi)
            row["B_Entry"]  = fn(b.entry) if b.method != "SKIP" else "SKIP"
            row["B_SL"]     = fn(b.sl) if b.method != "SKIP" else ""
            row["B_Risk"]   = fn(abs(b.entry - b.sl)) if b.method != "SKIP" else ""
            row["B_Out"]    = b.outcome if b.method != "SKIP" else "SKIP"
            row["B_PnL"]    = f"{b.pnl_rs:.0f}" if b.pnl_pts is not None else ""
        else:
            for k in _CMP_B:
                row[k] = "-"

        print("  ".join(str(row[k]).rjust(_CMP_W[k]) for k in _CMP_W))

    print(sep)

    # Summary stats
    sa = _stats_summary(trades_a)
    sb = _stats_summary(trades_5m)
    skip_ct = sum(1 for t in trades_5m if t.method == "SKIP")
    fb_ct   = sum(1 for t in trades_5m if t.method == "5M_FB")
    zm_ct   = sum(1 for t in trades_5m if t.method == "5M")

    print(f"\n{'Metric':<22} {'Strategy A (1H)':>20} {'Strategy B (5M)':>20}")
    print("-" * 64)
    print(f"{'Trades (executed)':<22} {sa.get('n',0):>20} {sb.get('n',0):>20}")
    print(f"{'  5M zone entry':<22} {'':>20} {zm_ct:>20}")
    print(f"{'  1H fallback':<22} {'':>20} {fb_ct:>20}")
    print(f"{'  SKIP (unfilled)':<22} {'':>20} {skip_ct:>20}")
    print(f"{'Winners':<22} {sa.get('win',0):>20} {sb.get('win',0):>20}")
    print(f"{'Win %':<22} {sa.get('win_pct',0):>19}% {sb.get('win_pct',0):>19}%")
    print(f"{'Profit factor':<22} {sa.get('pf',0):>20.2f} {sb.get('pf',0):>20.2f}")
    print(f"{'Net P&L (Rs)':<22} {sa.get('net',0):>20,.0f} {sb.get('net',0):>20,.0f}")
    print(f"{'Max drawdown':<22} {sa.get('max_dd',0):>20,.0f} {sb.get('max_dd',0):>20,.0f}")
    print(f"{'Outcomes':<22} {str(sa.get('outcomes',{})):>20} {str(sb.get('outcomes',{})):>20}")


def save_csv_compare(trades_a: List[Trade], trades_5m: List[Trade]) -> str:
    os.makedirs(_RESULTS_DIR, exist_ok=True)
    path = os.path.join(_RESULTS_DIR, "compare.csv")
    fmt = lambda dt: dt.strftime("%Y-%m-%d %H:%M") if dt else ""
    fmtd = lambda dt: dt.strftime("%Y-%m-%d") if dt else ""
    fn = lambda v: f"{v:.2f}" if v is not None else ""

    a_idx = {(t.ref_bar_ts, t.direction): t for t in trades_a}
    b_idx = {(t.ref_bar_ts, t.direction): t for t in trades_5m}
    all_keys = sorted(set(list(a_idx.keys()) + list(b_idx.keys())), key=lambda k: k[0])

    fields = ["Dir", "D1_Ref", "D1_Sweep", "D1_Reclaim", "Ref_Bar", "Entry_TS",
              "A_Entry", "A_SL", "A_Risk", "A_Outcome", "A_PnL_Rs",
              "B_Method", "B_M5Lo", "B_M5Hi", "B_Entry", "B_SL", "B_Risk",
              "B_Outcome", "B_PnL_Rs"]

    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for key in all_keys:
            a = a_idx.get(key)
            b = b_idx.get(key)
            src = a or b
            row: dict = {
                "Dir": key[1],
                "D1_Ref":     fmtd(src.d1_ref_ts),
                "D1_Sweep":   fmtd(src.d1_sweep_ts),
                "D1_Reclaim": fmtd(src.d1_reclaim_ts),
                "Ref_Bar":    fmt(key[0]),
                "Entry_TS":   fmt(src.entry_ts),
            }
            if a:
                row.update({"A_Entry": fn(a.entry), "A_SL": fn(a.sl),
                             "A_Risk": fn(abs(a.entry-a.sl)), "A_Outcome": a.outcome,
                             "A_PnL_Rs": f"{a.pnl_rs:.0f}" if a.pnl_pts is not None else ""})
            else:
                row.update({k: "" for k in ["A_Entry","A_SL","A_Risk","A_Outcome","A_PnL_Rs"]})
            if b:
                row.update({"B_Method": b.method,
                             "B_M5Lo": fn(b.m5_zone_lo), "B_M5Hi": fn(b.m5_zone_hi),
                             "B_Entry": fn(b.entry) if b.method != "SKIP" else "SKIP",
                             "B_SL": fn(b.sl) if b.method != "SKIP" else "",
                             "B_Risk": fn(abs(b.entry-b.sl)) if b.method != "SKIP" else "",
                             "B_Outcome": b.outcome if b.method != "SKIP" else "SKIP",
                             "B_PnL_Rs": f"{b.pnl_rs:.0f}" if b.pnl_pts is not None else ""})
            else:
                row.update({k: "" for k in ["B_Method","B_M5Lo","B_M5Hi","B_Entry",
                                             "B_SL","B_Risk","B_Outcome","B_PnL_Rs"]})
            w.writerow(row)
    return path


# ── main ──────────────────────────────────────────────────────────────────────
async def main() -> None:
    parser = argparse.ArgumentParser(description="D1 Trap + 1H Candle Breach backtest")
    parser.add_argument("--months", type=int, default=6,
                        help="calendar months of history to fetch (default: 6)")
    parser.add_argument("--compare", action="store_true",
                        help="also run Strategy B (5-min zone retest) and show side-by-side")
    parser.add_argument("--positional", action="store_true",
                        help="run 4 positional variants (1H/5M entry x OPP_TRAP/TSL exit)")
    args = parser.parse_args()

    token = os.environ.get("UPSTOX_TOKEN", "")
    if not token:
        try:
            from data_layer.client_db import ClientDB
            creds = ClientDB().get_feeder_creds_sync("upstox") or {}
            token = creds.get("access_token", "")
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

    print("\nRunning Strategy A (1H breach, EOD exit) ...")
    trades_a = run_backtest(d1_bars, h1_bars)
    print(f"  -> {len(trades_a)} trades")

    if args.positional:
        print(f"\nFetching 1m bars {start} to {end} (will resample to 5m) ...")
        bars_1m = await asyncio.to_thread(fetch_bars, NIFTY_KEY, "1minute", start, end, token,
                                          chunk_days=30)
        print(f"  -> {len(bars_1m)} 1-minute bars fetched")
        m5_bars_pos = resample_to_5m(bars_1m)
        print(f"  -> {len(m5_bars_pos)} 5-minute bars after resampling")

        # ── OLD logic (original 4 variants) ──────────────────────────
        print("\nRunning A1  1H entry + Opposite D1 trap exit  [old logic] ...")
        t_a_opp = run_backtest_positional(d1_bars, h1_bars, m5_bars_pos,
                                          use_5m=False, exit_mode="opp_trap")
        print(f"  -> {len(t_a_opp)} records")

        print("Running A2  1H entry + 1H Trailing SL  [old logic] ...")
        t_a_tsl = run_backtest_positional(d1_bars, h1_bars, m5_bars_pos,
                                          use_5m=False, exit_mode="tsl_1h")
        print(f"  -> {len(t_a_tsl)} records")

        print("Running C1  5M breakout + Opposite D1 trap exit  [old logic] ...")
        t_c_opp = run_backtest_positional(d1_bars, h1_bars, m5_bars_pos,
                                          use_5m=True, exit_mode="opp_trap")
        print(f"  -> {len(t_c_opp)} records")

        print("Running C2  5M breakout + 1H Trailing SL  [old logic] ...")
        t_c_tsl = run_backtest_positional(d1_bars, h1_bars, m5_bars_pos,
                                          use_5m=True, exit_mode="tsl_1h")
        print(f"  -> {len(t_c_tsl)} records")

        # ── NEW logic (approach-direction + zone-invalidation rules) ─
        print("\nRunning A1s 1H entry + Opposite D1 trap exit  [STRICT] ...")
        t_a_opp_s = run_backtest_positional(d1_bars, h1_bars, m5_bars_pos,
                                            use_5m=False, exit_mode="opp_trap",
                                            strict_zone_tap=True)
        print(f"  -> {len(t_a_opp_s)} records")

        print("Running A2s 1H entry + 1H Trailing SL  [STRICT] ...")
        t_a_tsl_s = run_backtest_positional(d1_bars, h1_bars, m5_bars_pos,
                                            use_5m=False, exit_mode="tsl_1h",
                                            strict_zone_tap=True)
        print(f"  -> {len(t_a_tsl_s)} records")

        print("Running C1s 5M breakout + Opposite D1 trap exit  [STRICT] ...")
        t_c_opp_s = run_backtest_positional(d1_bars, h1_bars, m5_bars_pos,
                                            use_5m=True, exit_mode="opp_trap",
                                            strict_zone_tap=True)
        print(f"  -> {len(t_c_opp_s)} records")

        print("Running C2s 5M breakout + 1H Trailing SL  [STRICT] ...")
        t_c_tsl_s = run_backtest_positional(d1_bars, h1_bars, m5_bars_pos,
                                            use_5m=True, exit_mode="tsl_1h",
                                            strict_zone_tap=True)
        print(f"  -> {len(t_c_tsl_s)} records")

        # ── COMBINED variants ─────────────────────────────────────────
        print("\nRunning COMBINED  C2 + TWEAK (TSL) ...")
        t_c2_tweak = run_backtest_combined(d1_bars, h1_bars, m5_bars_pos,
                                           exit_mode="tsl_1h", use_flip=False, use_tweak=True)
        print(f"  -> {len(t_c2_tweak)} records")

        print("Running COMBINED  C2 + FLIP (TSL) ...")
        t_c2_flip = run_backtest_combined(d1_bars, h1_bars, m5_bars_pos,
                                          exit_mode="tsl_1h", use_flip=True, use_tweak=False)
        print(f"  -> {len(t_c2_flip)} records")

        print("Running COMBINED  C2 + FLIP + TWEAK (TSL) ...")
        t_c2_all = run_backtest_combined(d1_bars, h1_bars, m5_bars_pos,
                                         exit_mode="tsl_1h", use_flip=True, use_tweak=True)
        print(f"  -> {len(t_c2_all)} records")

        # ── FLIP variants ─────────────────────────────────────────────
        print("\nRunning FLIP  Zone flip + Opposite D1 trap exit ...")
        t_flip_opp = run_backtest_flip(d1_bars, h1_bars, m5_bars_pos, exit_mode="opp_trap")
        print(f"  -> {len(t_flip_opp)} records")

        print("Running FLIP  Zone flip + 1H Trailing SL ...")
        t_flip_tsl = run_backtest_flip(d1_bars, h1_bars, m5_bars_pos, exit_mode="tsl_1h")
        print(f"  -> {len(t_flip_tsl)} records")

        # ── TWEAK variants ────────────────────────────────────────────
        print("\nRunning TWEAK Immediate failure trade + Opposite D1 trap exit ...")
        t_tweak_opp = run_backtest_tweak(d1_bars, h1_bars, m5_bars_pos, exit_mode="opp_trap")
        print(f"  -> {len(t_tweak_opp)} records")

        print("Running TWEAK Immediate failure trade + 1H Trailing SL ...")
        t_tweak_tsl = run_backtest_tweak(d1_bars, h1_bars, m5_bars_pos, exit_mode="tsl_1h")
        print(f"  -> {len(t_tweak_tsl)} records")

        print_positional_summary(
            t_a_opp, t_a_tsl, t_c_opp, t_c_tsl, start, end,
            t_a_opp_s, t_a_tsl_s, t_c_opp_s, t_c_tsl_s,
            t_flip_opp, t_flip_tsl, t_tweak_opp, t_tweak_tsl,
            t_c2_tweak, t_c2_flip, t_c2_all,
        )

        for trades_pos, fname in [
            (t_a_opp,    "pos_a1_opp.csv"),
            (t_a_tsl,    "pos_a2_tsl.csv"),
            (t_c_opp,    "pos_c1_opp.csv"),
            (t_c_tsl,    "pos_c2_tsl.csv"),
            (t_a_opp_s,  "pos_a1_opp_strict.csv"),
            (t_a_tsl_s,  "pos_a2_tsl_strict.csv"),
            (t_c_opp_s,  "pos_c1_opp_strict.csv"),
            (t_c_tsl_s,  "pos_c2_tsl_strict.csv"),
            (t_flip_opp, "pos_flip_opp.csv"),
            (t_flip_tsl, "pos_flip_tsl.csv"),
            (t_tweak_opp,"pos_tweak_opp.csv"),
            (t_tweak_tsl,"pos_tweak_tsl.csv"),
            (t_c2_tweak, "pos_c2_tweak.csv"),
            (t_c2_flip,  "pos_c2_flip.csv"),
            (t_c2_all,   "pos_c2_all.csv"),
        ]:
            p = save_csv_positional(trades_pos, fname)
            print(f"  Saved {fname} -> {p}")
        return

    if args.compare:
        print(f"\nFetching 1m bars {start} to {end} (will resample to 5m) ...")
        bars_1m = await asyncio.to_thread(fetch_bars, NIFTY_KEY, "1minute", start, end, token,
                                          chunk_days=30)
        print(f"  -> {len(bars_1m)} 1-minute bars fetched")
        m5_bars = resample_to_5m(bars_1m)
        print(f"  -> {len(m5_bars)} 5-minute bars after resampling")

        print("\nRunning Strategy B (5-min zone retest) ...")
        trades_5m = run_backtest_5m(d1_bars, h1_bars, m5_bars)
        print(f"  -> {len(trades_5m)} records ({sum(1 for t in trades_5m if t.method!='SKIP')} trades, "
              f"{sum(1 for t in trades_5m if t.method=='SKIP')} skipped)")

        print_comparison(trades_a, trades_5m, start, end)
        cmp_path = save_csv_compare(trades_a, trades_5m)
        print(f"\nComparison CSV saved to: {cmp_path}")
    else:
        print_report(trades_a, start, end)
        csv_path = save_csv(trades_a)
        print(f"\nTrades saved to: {csv_path}")


if __name__ == "__main__":
    asyncio.run(main())
