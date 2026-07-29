"""
backtest/d1_trap_75m/backtest.py — D1 Trap strategy at 75M/15M/5M timeframes

Applies the same 3-candle trap detection + C2/TWEAK entry to NIFTY and SENSEX
using intraday 75-minute zones (analogous to D1 zones) with 15M monitoring and
5M entry trigger.

Strategy:
  75M zones  — bear trap (sellers swept/trapped -> LONG)
               bull trap (buyers swept/trapped -> SHORT)
  15M monitor — price enters zone -> ref bar; next 15M breach of ref = pending trigger
  5M trigger — first 5M bar hitting trigger level = ENTRY
  TWEAK       — while monitoring, 15M closes through far zone boundary (zone fails)
                -> counter-direction on next 15M breach of the failure bar
  Exit:
    hard SL  — original zone extreme
    TSL      — ratchets from 15M lows (LONG) / highs (SHORT) after each 15M close
    EOD      — 15:15 IST force-exit at bar.open

P&L is in spot points x lot size (index points).

Usage:
  python backtest/d1_trap_75m/backtest.py
  python backtest/d1_trap_75m/backtest.py --months 3
  python backtest/d1_trap_75m/backtest.py --symbols NIFTY,SENSEX
  UPSTOX_TOKEN=<token> python backtest/d1_trap_75m/backtest.py --months 3
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.global_config import IST
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones

# ── Constants ─────────────────────────────────────────────────────────────────

SESSION_OPEN  = time(9, 15)
EOD_HM        = (15, 15)
ENTRY_CUTOFF  = time(14, 0)
HTF_MINS      = 75    # zone detection timeframe
MTF_MINS      = 15    # monitoring + entry confirmation
LTF_MINS      = 5     # trigger entry
MAX_ZONE_DAYS = 10    # discard 75M zones older than 10 calendar days
                       # (10 days * ~5 bars/day = ~50 75M bars context — analogous to 20 D1 bars)

# Instrument config
INSTRUMENTS: Dict[str, dict] = {
    "NIFTY":  {"key": "NSE_INDEX|Nifty 50",   "lot": 75,  "step": 50},
    "SENSEX": {"key": "BSE_INDEX|SENSEX",      "lot": 20,  "step": 100},
}

_NIFTY_1M_CACHE = (
    Path(__file__).resolve().parents[2]
    / "backtest" / "d1_trap_1h_entry" / "data_cache"
)
_LOCAL_CACHE = Path(__file__).parent / "data_cache"
_RESULTS_DIR = Path(__file__).parent / "results"


# ── Bar dataclass ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class _Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float


# ── HTTP / cache helpers ──────────────────────────────────────────────────────

def _http_get(url: str, token: str) -> dict:
    try:
        from curl_cffi import requests as _cc
        hdrs = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
        return _cc.get(url, headers=hdrs, impersonate="chrome131", timeout=20).json()
    except ImportError:
        import urllib.request
        req = urllib.request.Request(
            url,
            headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode())
    except Exception as exc:
        print(f"  [HTTP] {exc}")
        return {}


def _parse_candles(raw: dict) -> List[_Bar]:
    rows = (raw.get("data") or {}).get("candles") or []
    out: List[_Bar] = []
    for c in reversed(rows):       # newest-first in response -> sort oldest-first
        try:
            ts = datetime.fromisoformat(c[0]).astimezone(IST)
            out.append(_Bar(timestamp=ts, open=float(c[1]), high=float(c[2]),
                            low=float(c[3]), close=float(c[4])))
        except Exception:
            pass
    return out


def _load_nifty_1m_from_cache(start: date, end: date) -> List[_Bar]:
    """Try to load NIFTY 1-min bars from the d1_trap_1h_entry cache."""
    all_bars: List[_Bar] = []
    seen: Set[datetime] = set()
    for f in sorted(_NIFTY_1M_CACHE.glob("NSE_INDEX_Nifty_50_1minute_*.json")):
        try:
            with open(f) as fp:
                raw = json.load(fp)
            for b in _parse_candles(raw):
                if start <= b.timestamp.date() <= end and b.timestamp not in seen:
                    all_bars.append(b)
                    seen.add(b.timestamp)
        except Exception:
            pass
    all_bars.sort(key=lambda b: b.timestamp)
    return all_bars


def _fetch_1m_chunk(sym: str, key: str, start: date, end: date, token: str) -> List[_Bar]:
    fname = f"{sym}_1minute_{start}_{end}.json"
    cache_path = _LOCAL_CACHE / fname
    if cache_path.exists():
        with open(cache_path) as fp:
            raw = json.load(fp)
        return _parse_candles(raw)
    from urllib.parse import quote as _q
    url = (f"https://api.upstox.com/v2/historical-candle/{_q(key, safe='')}"
           f"/1minute/{end.isoformat()}/{start.isoformat()}")
    raw = _http_get(url, token)
    bars = _parse_candles(raw)
    print(f"  [1M API] {sym}: {len(bars)} bars ({start}..{end})")
    if bars:
        tmp = str(cache_path) + ".tmp"
        with open(tmp, "w") as fp:
            json.dump(raw, fp)
        os.replace(tmp, cache_path)
    return bars


def fetch_1m_bars(sym: str, start: date, end: date, token: str) -> List[_Bar]:
    """Fetch 1-min bars (30-day chunks, cached). Reuses d1_trap_1h_entry cache for NIFTY."""
    key = INSTRUMENTS[sym]["key"]
    all_bars: List[_Bar] = []

    # Try the shared 1-min cache for NIFTY first
    if sym == "NIFTY":
        cached = _load_nifty_1m_from_cache(start, end)
        if cached and cached[0].timestamp.date() <= start and cached[-1].timestamp.date() >= end:
            print(f"  [1M cache] {sym}: {len(cached)} bars from d1_trap_1h_entry cache")
            return cached
        # Partial cache — supplement with API
        covered_end = cached[-1].timestamp.date() if cached else None
        if cached:
            print(f"  [1M cache] {sym}: partial ({len(cached)} bars up to {covered_end})")
            all_bars.extend(cached)
            if covered_end and covered_end >= end:
                return all_bars
            start = (covered_end + timedelta(days=1)) if covered_end else start

    # API fetch in 30-day chunks
    seen: Set[datetime] = set()
    for b in all_bars:
        seen.add(b.timestamp)

    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(chunk_start + timedelta(days=29), end)
        chunk = _fetch_1m_chunk(sym, key, chunk_start, chunk_end, token)
        for b in chunk:
            if b.timestamp not in seen:
                all_bars.append(b)
                seen.add(b.timestamp)
        chunk_start = chunk_end + timedelta(days=1)

    all_bars.sort(key=lambda b: b.timestamp)
    return all_bars


# ── Resample helpers ──────────────────────────────────────────────────────────

def resample(bars_1m: List[_Bar], mins: int) -> List[_Bar]:
    """Resample 1-min bars to `mins`-minute bars, clock-anchored at 09:15 IST."""
    buckets: Dict = {}
    order: List = []
    for b in bars_1m:
        open_dt = b.timestamp.replace(hour=9, minute=15, second=0, microsecond=0)
        elapsed = max(0, int((b.timestamp - open_dt).total_seconds() // 60))
        if elapsed < 0:
            continue
        bucket_idx = elapsed // mins
        bucket_key = (b.timestamp.date(), bucket_idx)
        if bucket_key not in buckets:
            buckets[bucket_key] = []
            order.append(bucket_key)
        buckets[bucket_key].append(b)
    out: List[_Bar] = []
    for key in order:
        chunk = buckets[key]
        bar_open_dt = chunk[0].timestamp.replace(
            hour=9, minute=15, second=0, microsecond=0
        ) + timedelta(minutes=key[1] * mins)
        out.append(_Bar(
            timestamp=bar_open_dt,
            open=chunk[0].open,
            high=max(b.high for b in chunk),
            low=min(b.low for b in chunk),
            close=chunk[-1].close,
        ))
    return out


def group_by_date(bars: List[_Bar]) -> Dict[date, List[_Bar]]:
    d: Dict[date, List[_Bar]] = {}
    for b in bars:
        d.setdefault(b.timestamp.date(), []).append(b)
    return {k: sorted(v, key=lambda b: b.timestamp) for k, v in d.items()}


# ── State dataclasses ─────────────────────────────────────────────────────────

@dataclass
class _Monitor:
    """A live 75M zone being watched for 15M entry."""
    direction: str           # "LONG" | "SHORT"
    htf_ref_ts: datetime
    htf_sweep_ts: datetime
    htf_reclaim_ts: datetime
    zone_lo: float
    zone_hi: float
    state: str = "WAITING"   # WAITING | MONITORING
    ref_bar: Optional[_Bar] = None
    done: bool = False
    invalid: bool = False


@dataclass
class _TweakSetup:
    """A zone-failure triggered counter-trade setup."""
    direction: str           # "LONG" | "SHORT" — counter to zone direction
    failure_bar: _Bar
    zone_lo: float
    zone_hi: float
    htf_ref_ts: datetime
    htf_sweep_ts: datetime
    htf_reclaim_ts: datetime
    activated_ts: datetime
    done: bool = False


@dataclass
class _PendingTrigger:
    """A 5M trigger waiting to fire after 15M confirmation."""
    direction: str           # "LONG" | "SHORT"
    trigger_level: float     # ref_bar.high (LONG) or ref_bar.low (SHORT)
    sl: float                # hard SL
    zone_lo: float
    zone_hi: float
    source: str              # "C2" | "TWEAK"
    htf_ref_ts: datetime
    htf_sweep_ts: datetime
    htf_reclaim_ts: datetime
    set_at: datetime


@dataclass
class Trade:
    sym: str
    direction: str
    entry_ts: datetime
    entry: float
    sl: float
    tsl: float               # current TSL level
    zone_lo: float
    zone_hi: float
    source: str              # "C2" | "TWEAK"
    exit_ts: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: str = ""

    @property
    def pnl_pts(self) -> Optional[float]:
        if self.exit_price is None:
            return None
        return (self.exit_price - self.entry) if self.direction == "LONG" else (self.entry - self.exit_price)

    def as_row(self, lot_size: int) -> dict:
        fmt = lambda dt: dt.strftime("%Y-%m-%d %H:%M") if dt else ""
        pnl = self.pnl_pts
        return {
            "Symbol":    self.sym,
            "Dir":       self.direction,
            "Source":    self.source,
            "Entry_TS":  fmt(self.entry_ts),
            "Entry":     f"{self.entry:.2f}",
            "SL":        f"{self.sl:.2f}",
            "Risk_pts":  f"{abs(self.entry - self.sl):.2f}",
            "Exit_TS":   fmt(self.exit_ts),
            "Exit":      f"{self.exit_price:.2f}" if self.exit_price is not None else "",
            "Reason":    self.exit_reason,
            "PnL_pts":   f"{pnl:.2f}" if pnl is not None else "",
            "PnL_Rs":    f"{pnl * lot_size:.0f}" if pnl is not None else "",
            "Zone_Lo":   f"{self.zone_lo:.2f}",
            "Zone_Hi":   f"{self.zone_hi:.2f}",
        }


# ── Core simulation ───────────────────────────────────────────────────────────

def run_simulation(sym: str, bars_1m: List[_Bar], lot_size: int) -> List[Trade]:
    """
    75M zone + 15M monitoring + 5M trigger simulation.

    Returns all completed trades with P&L.
    """
    bars_5m  = resample(bars_1m, LTF_MINS)
    bars_15m = resample(bars_1m, MTF_MINS)
    bars_75m = resample(bars_1m, HTF_MINS)

    m5_by_date  = group_by_date(bars_5m)
    m15_by_date = group_by_date(bars_15m)
    m75_by_date = group_by_date(bars_75m)

    sim_days = sorted(set(b.timestamp.date() for b in bars_5m))

    # Rolling state
    all_75m_closed: List[_Bar] = []     # accumulates as each 75M bar closes
    monitors: List[_Monitor] = []
    tweak_setups: List[_TweakSetup] = []
    known_bear: Set[datetime] = set()
    known_bull: Set[datetime] = set()
    all_trades: List[Trade] = []

    def _discover_zones(new_bars: List[_Bar], ref_date: date) -> None:
        """Add zones from `new_bars` to monitors (skips already-known ref_ts)."""
        nonlocal monitors
        if len(all_75m_closed) < 3:
            return
        age_cutoff = datetime.combine(ref_date, datetime.min.time()).replace(tzinfo=IST) \
                     - timedelta(days=MAX_ZONE_DAYS)
        for z in find_all_bear_zones(all_75m_closed, known_ref_ts=known_bear):
            if z.reference_low_ts not in known_bear:
                if z.lock_ts >= age_cutoff:
                    known_bear.add(z.reference_low_ts)
                    monitors.append(_Monitor(
                        direction="LONG",
                        htf_ref_ts=z.reference_low_ts,
                        htf_sweep_ts=z.sweep_started_ts,
                        htf_reclaim_ts=z.lock_ts,
                        zone_lo=min(z.entry_line, z.sweep_low),
                        zone_hi=max(z.entry_line, z.sweep_low),
                    ))
        for z in find_all_bull_zones(all_75m_closed, known_ref_ts=known_bull):
            if z.reference_low_ts not in known_bull:
                if z.lock_ts >= age_cutoff:
                    known_bull.add(z.reference_low_ts)
                    monitors.append(_Monitor(
                        direction="SHORT",
                        htf_ref_ts=z.reference_low_ts,
                        htf_sweep_ts=z.sweep_started_ts,
                        htf_reclaim_ts=z.lock_ts,
                        zone_lo=min(z.entry_line, z.sweep_low),
                        zone_hi=max(z.entry_line, z.sweep_low),
                    ))

    def _evict_stale(ref_date: date) -> None:
        nonlocal monitors, tweak_setups
        age_cutoff = datetime.combine(ref_date, datetime.min.time()).replace(tzinfo=IST) \
                     - timedelta(days=MAX_ZONE_DAYS)
        monitors = [m for m in monitors
                    if not m.done and not m.invalid and m.htf_reclaim_ts >= age_cutoff]
        tweak_setups = [t for t in tweak_setups
                        if not t.done and t.htf_reclaim_ts >= age_cutoff]

    def _process_15m_close(
        bar15: _Bar,
        open_trade: Optional[Trade],
        pending: Optional[_PendingTrigger],
    ):
        """
        Process a just-closed 15M bar. Returns updated (open_trade, pending).
        """
        # If a trade is open: ratchet TSL
        if open_trade is not None:
            if open_trade.direction == "LONG":
                open_trade.tsl = max(open_trade.tsl, bar15.low)
            else:
                open_trade.tsl = min(open_trade.tsl, bar15.high)

        hm = (bar15.timestamp.hour, bar15.timestamp.minute)

        # Check TWEAK setups: failure_bar breach
        for tw in tweak_setups:
            if tw.done:
                continue
            if open_trade is not None or pending is not None:
                continue
            if (bar15.timestamp.hour, bar15.timestamp.minute) >= EOD_HM:
                break
            if (bar15.timestamp.hour, bar15.timestamp.minute) >= (14, 0):
                break
            if tw.direction == "SHORT" and bar15.low < tw.failure_bar.low:
                sl = max(tw.zone_hi, tw.failure_bar.high)
                pending = _PendingTrigger(
                    direction="SHORT",
                    trigger_level=tw.failure_bar.low,
                    sl=sl,
                    zone_lo=tw.zone_lo, zone_hi=tw.zone_hi,
                    source="TWEAK",
                    htf_ref_ts=tw.htf_ref_ts,
                    htf_sweep_ts=tw.htf_sweep_ts,
                    htf_reclaim_ts=tw.htf_reclaim_ts,
                    set_at=bar15.timestamp,
                )
                tw.done = True
            elif tw.direction == "LONG" and bar15.high > tw.failure_bar.high:
                sl = min(tw.zone_lo, tw.failure_bar.low)
                pending = _PendingTrigger(
                    direction="LONG",
                    trigger_level=tw.failure_bar.high,
                    sl=sl,
                    zone_lo=tw.zone_lo, zone_hi=tw.zone_hi,
                    source="TWEAK",
                    htf_ref_ts=tw.htf_ref_ts,
                    htf_sweep_ts=tw.htf_sweep_ts,
                    htf_reclaim_ts=tw.htf_reclaim_ts,
                    set_at=bar15.timestamp,
                )
                tw.done = True

        # Check monitors: zone state updates + C2 breach detection
        for m in monitors:
            if m.done or m.invalid:
                continue
            if open_trade is not None or pending is not None:
                # Only update MONITORING state (ref_bar/TWEAK), don't trigger
                if m.state == "WAITING":
                    _update_monitor_waiting(m, bar15)
                elif m.state == "MONITORING":
                    _check_tweak(m, bar15)
                continue
            if hm >= EOD_HM:
                break
            if hm >= (14, 0) and m.state == "WAITING":
                continue

            if m.state == "WAITING":
                _update_monitor_waiting(m, bar15)
            elif m.state == "MONITORING":
                new_pending = _check_c2_breach(m, bar15)
                if new_pending is not None:
                    pending = new_pending
                    m.done = True
                else:
                    _check_tweak(m, bar15)

        return open_trade, pending

    def _update_monitor_waiting(m: _Monitor, bar15: _Bar) -> None:
        """Transition WAITING -> MONITORING when bar15 enters the zone."""
        if m.direction == "LONG":
            # Bar low enters zone from above (close still above zone_lo)
            in_zone = bar15.low <= m.zone_hi and bar15.close >= m.zone_lo
        else:
            in_zone = bar15.high >= m.zone_lo and bar15.close <= m.zone_hi
        if in_zone:
            m.state = "MONITORING"
            m.ref_bar = bar15

    def _check_c2_breach(m: _Monitor, bar15: _Bar) -> Optional[_PendingTrigger]:
        """
        While MONITORING: check if bar15 breaches the ref candle.
        Returns a _PendingTrigger if so, else None.
        Also handles ref-candle update if price is still in zone.
        """
        if m.ref_bar is None:
            return None
        hm = (bar15.timestamp.hour, bar15.timestamp.minute)
        if hm >= EOD_HM or hm >= (14, 0):
            return None

        if m.direction == "LONG":
            if bar15.high > m.ref_bar.high:
                # C2 breach: trigger at ref_bar.high (LONG entry)
                sl = m.zone_lo
                return _PendingTrigger(
                    direction="LONG",
                    trigger_level=m.ref_bar.high,
                    sl=sl,
                    zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                    source="C2",
                    htf_ref_ts=m.htf_ref_ts,
                    htf_sweep_ts=m.htf_sweep_ts,
                    htf_reclaim_ts=m.htf_reclaim_ts,
                    set_at=bar15.timestamp,
                )
            else:
                # Still in zone — update ref_bar to lowest-low bar
                if bar15.low < m.ref_bar.low:
                    m.ref_bar = bar15
        else:  # SHORT
            if bar15.low < m.ref_bar.low:
                sl = m.zone_hi
                return _PendingTrigger(
                    direction="SHORT",
                    trigger_level=m.ref_bar.low,
                    sl=sl,
                    zone_lo=m.zone_lo, zone_hi=m.zone_hi,
                    source="C2",
                    htf_ref_ts=m.htf_ref_ts,
                    htf_sweep_ts=m.htf_sweep_ts,
                    htf_reclaim_ts=m.htf_reclaim_ts,
                    set_at=bar15.timestamp,
                )
            else:
                if bar15.high > m.ref_bar.high:
                    m.ref_bar = bar15
        return None

    def _check_tweak(m: _Monitor, bar15: _Bar) -> None:
        """
        While MONITORING: if bar15 closes THROUGH the far zone boundary,
        the zone has failed -> queue a TWEAK (counter-direction) setup.
        """
        if m.direction == "LONG":
            zone_failed = bar15.close < m.zone_lo
        else:
            zone_failed = bar15.close > m.zone_hi
        if zone_failed:
            m.invalid = True
            counter_dir = "SHORT" if m.direction == "LONG" else "LONG"
            tweak_setups.append(_TweakSetup(
                direction=counter_dir,
                failure_bar=bar15,
                zone_lo=m.zone_lo,
                zone_hi=m.zone_hi,
                htf_ref_ts=m.htf_ref_ts,
                htf_sweep_ts=m.htf_sweep_ts,
                htf_reclaim_ts=m.htf_reclaim_ts,
                activated_ts=bar15.timestamp,
            ))

    # ── Day-by-day simulation loop ────────────────────────────────────────────

    for today in sim_days:
        # Pre-session: populate 75M history from PREVIOUS days (already in all_75m_closed)
        # Discover zones from history built up so far
        _discover_zones(all_75m_closed, today)
        _evict_stale(today)

        today_5m  = m5_by_date.get(today, [])
        today_15m = m15_by_date.get(today, [])
        today_75m = m75_by_date.get(today, [])

        if not today_5m:
            continue

        # Pointers into this day's 15M/75M bar lists
        next_15m_idx = 0
        next_75m_idx = 0

        open_trade: Optional[Trade] = None
        pending: Optional[_PendingTrigger] = None

        # Reset daily monitors to WAITING for bars that were in MONITORING from yesterday
        # (don't carry over MONITORING state across days — fresh session)
        for m in monitors:
            if m.state == "MONITORING" and not m.done and not m.invalid:
                m.state = "WAITING"
                m.ref_bar = None

        for bar5m in today_5m:
            hm5 = (bar5m.timestamp.hour, bar5m.timestamp.minute)

            # EOD force-exit
            if hm5 >= EOD_HM:
                if open_trade is not None:
                    open_trade.exit_price = bar5m.open
                    open_trade.exit_ts = bar5m.timestamp
                    open_trade.exit_reason = "EOD"
                    all_trades.append(open_trade)
                    open_trade = None
                break

            bar5m_end = bar5m.timestamp + timedelta(minutes=LTF_MINS)

            # Check if any 75M bars have closed during this 5M bar
            while next_75m_idx < len(today_75m):
                bar75 = today_75m[next_75m_idx]
                if bar75.timestamp + timedelta(minutes=HTF_MINS) <= bar5m_end:
                    all_75m_closed.append(bar75)
                    _discover_zones([bar75], today)
                    next_75m_idx += 1
                else:
                    break

            # Check if any 15M bars have closed during this 5M bar
            while next_15m_idx < len(today_15m):
                bar15 = today_15m[next_15m_idx]
                if bar15.timestamp + timedelta(minutes=MTF_MINS) <= bar5m_end:
                    open_trade, pending = _process_15m_close(bar15, open_trade, pending)
                    next_15m_idx += 1
                else:
                    break

            # ── 5M trigger: check entry ───────────────────────────────────────
            if pending is not None and open_trade is None and hm5 < (14, 0):
                trig = pending
                if trig.direction == "LONG" and bar5m.high >= trig.trigger_level:
                    entry_price = trig.trigger_level
                    sl = trig.sl
                    open_trade = Trade(
                        sym=sym,
                        direction="LONG",
                        entry_ts=bar5m.timestamp,
                        entry=entry_price,
                        sl=sl,
                        tsl=sl,
                        zone_lo=trig.zone_lo,
                        zone_hi=trig.zone_hi,
                        source=trig.source,
                    )
                    pending = None
                elif trig.direction == "SHORT" and bar5m.low <= trig.trigger_level:
                    entry_price = trig.trigger_level
                    sl = trig.sl
                    open_trade = Trade(
                        sym=sym,
                        direction="SHORT",
                        entry_ts=bar5m.timestamp,
                        entry=entry_price,
                        sl=sl,
                        tsl=sl,
                        zone_lo=trig.zone_lo,
                        zone_hi=trig.zone_hi,
                        source=trig.source,
                    )
                    pending = None
                # Expire stale pending trigger after half a session
                elif (bar5m.timestamp - trig.set_at).total_seconds() > 3600 * 2:
                    pending = None

            # ── 5M exit: check SL / TSL ───────────────────────────────────────
            if open_trade is not None:
                t = open_trade
                if t.direction == "LONG":
                    hit_sl  = bar5m.low <= t.sl
                    hit_tsl = bar5m.close < t.tsl
                else:
                    hit_sl  = bar5m.high >= t.sl
                    hit_tsl = bar5m.close > t.tsl

                if hit_sl:
                    t.exit_price = t.sl
                    t.exit_ts = bar5m.timestamp
                    t.exit_reason = "SL"
                    all_trades.append(t)
                    open_trade = None
                elif hit_tsl:
                    t.exit_price = t.tsl
                    t.exit_ts = bar5m.timestamp
                    t.exit_reason = "TSL"
                    all_trades.append(t)
                    open_trade = None

        # End of day: any remaining open trade is kept for next day
        # (no overnight carry — force-exit already handled above)

    return all_trades


# ── Reporting ─────────────────────────────────────────────────────────────────

def _print_summary(sym: str, trades: List[Trade], lot_size: int) -> dict:
    total = len(trades)
    wins = sum(1 for t in trades if (t.pnl_pts or 0) > 0)
    losses = total - wins
    pnl_pts = sum(t.pnl_pts or 0 for t in trades)
    pnl_rs = pnl_pts * lot_size
    wr = wins / total * 100 if total else 0
    by_src = {}
    for t in trades:
        by_src.setdefault(t.source, []).append(t.pnl_pts or 0)

    print(f"\n{'='*60}")
    print(f"  {sym}  (lot={lot_size})")
    print(f"  Trades: {total}   Win: {wins}   Loss: {losses}   WR: {wr:.0f}%")
    print(f"  Total PnL: {pnl_pts:+.1f} pts  |  Rs {pnl_rs:+,.0f}")
    for src, pts_list in by_src.items():
        n = len(pts_list); w = sum(1 for p in pts_list if p > 0)
        print(f"  {src}: {n} trades, {w}/{n} wins, {sum(pts_list):+.1f} pts")
    by_reason = {}
    for t in trades:
        by_reason.setdefault(t.exit_reason, 0)
        by_reason[t.exit_reason] += 1
    print(f"  Exit reasons: {by_reason}")
    print(f"{'='*60}")
    return {"sym": sym, "T": total, "W": wins, "L": losses, "WR": wr,
            "PnL_pts": pnl_pts, "PnL_Rs": pnl_rs}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months",  type=int, default=3, help="Lookback months (default 3)")
    ap.add_argument("--symbols", default="NIFTY,SENSEX", help="Comma-separated symbols")
    ap.add_argument("--token",   default=os.environ.get("UPSTOX_TOKEN", ""),
                    help="Upstox bearer token")
    args = ap.parse_args()

    token = args.token.strip()
    if not token:
        print("[WARN] No Upstox token — cached data only. Set UPSTOX_TOKEN or --token.")

    syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    end_date   = date.today()
    start_date = end_date - timedelta(days=30 * args.months)

    print(f"\nD1 Trap 75M/15M/5M Backtest | {start_date} to {end_date}")
    print(f"Symbols: {syms}   HTF={HTF_MINS}M  MTF={MTF_MINS}M  LTF={LTF_MINS}M\n")

    _LOCAL_CACHE.mkdir(parents=True, exist_ok=True)
    _RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    all_rows: List[dict] = []
    summaries: List[dict] = []

    for sym in syms:
        cfg = INSTRUMENTS.get(sym)
        if cfg is None:
            print(f"[SKIP] Unknown symbol: {sym}")
            continue

        lot = cfg["lot"]
        print(f"\n-- {sym} --")

        # Fetch data: need a warm-up period for zone detection (extra MAX_ZONE_DAYS)
        warmup_start = start_date - timedelta(days=MAX_ZONE_DAYS + 5)
        bars_1m = fetch_1m_bars(sym, warmup_start, end_date, token)
        if not bars_1m:
            print(f"  [SKIP] No 1-min data for {sym}")
            continue

        print(f"  Loaded {len(bars_1m)} 1-min bars ({bars_1m[0].timestamp.date()} .. {bars_1m[-1].timestamp.date()})")

        trades = run_simulation(sym, bars_1m, lot)

        # Filter to simulation period only (exclude warmup trades)
        sim_cutoff = datetime.combine(start_date, time(9, 15)).replace(tzinfo=IST)
        trades = [t for t in trades if t.entry_ts >= sim_cutoff]

        sm = _print_summary(sym, trades, lot)
        summaries.append(sm)

        for t in trades:
            all_rows.append(t.as_row(lot))

    # Save CSV
    today_str = date.today().isoformat()
    csv_path = _RESULTS_DIR / f"trades_{today_str}.csv"
    if all_rows:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(all_rows[0].keys()))
            w.writeheader()
            w.writerows(all_rows)
        print(f"\nTrades saved: {csv_path}")

    # Summary table
    if summaries:
        print(f"\n{'Symbol':<10} {'T':>5} {'W':>5} {'L':>5} {'WR':>6} {'PnL_pts':>10} {'PnL_Rs':>12}")
        print("-" * 60)
        for s in summaries:
            print(f"{s['sym']:<10} {s['T']:>5} {s['W']:>5} {s['L']:>5} "
                  f"{s['WR']:>5.0f}% {s['PnL_pts']:>+10.1f} {s['PnL_Rs']:>+12,.0f}")


if __name__ == "__main__":
    main()
