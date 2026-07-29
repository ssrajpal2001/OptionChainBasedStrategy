"""
backtest/fno_scanner/scan_live.py  —  FnO Live Zone Scanner
=============================================================
Scans all 30 FnO stocks for active trap zones as of the last trading day.
Reports:
  - TRIGGERED  : last bar already retested the zone (enter at Monday open)
  - APPROACHING: zone entry_line within 1.5% of Friday close (watch Monday)

Output: ranked list + suggested August expiry option entry for each signal.

Usage:
    UPSTOX_TOKEN=<token> python backtest/fno_scanner/scan_live.py
"""
from __future__ import annotations

import gzip, json, os, sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from config.global_config import IST
from backtest.fno_scanner.backtest import TOP_30_STOCKS, load_or_fetch, Bar
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones

def _fetch_stock_expiries(token: str) -> Dict[str, List[date]]:
    """Download NSE instrument master and extract all future FnO expiry dates
    per stock symbol.  Returns {symbol: sorted_list_of_expiry_dates}."""
    from curl_cffi import requests as cc
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    try:
        r = cc.get(url, impersonate="chrome131", timeout=30)
        instruments = json.loads(gzip.decompress(r.content))
    except Exception as e:
        print(f"  [warn] Could not fetch instrument master: {e}")
        return {}

    today = date.today()
    result: Dict[str, List[date]] = {}

    # Build reverse map: trading_symbol (NSE_EQ) -> FnO trading_symbol
    # Upstox uses the stock's underlying name in the FnO contract name
    # e.g. "RELIANCE 1260 CE 28 JUL 26" -> underlying=RELIANCE
    for inst in instruments:
        seg = inst.get("segment", "")
        if seg != "NSE_FO":
            continue
        ts = inst.get("trading_symbol", "")
        itype = inst.get("instrument_type", "")
        if itype not in ("CE", "PE"):
            continue
        # Parse: "{UNDERLYING} {STRIKE} {TYPE} {DD} {MON} {YY}"
        parts = ts.split()
        if len(parts) < 6:
            continue
        underlying = parts[0]
        if underlying not in TOP_30_STOCKS:
            continue
        try:
            exp_str = f"{parts[3]} {parts[4]} {parts[5]}"  # "28 JUL 26"
            exp_date = datetime.strptime(exp_str, "%d %b %y").date()
        except ValueError:
            continue
        if exp_date < today:
            continue
        result.setdefault(underlying, [])
        if exp_date not in result[underlying]:
            result[underlying].append(exp_date)

    # Sort each list
    for sym in result:
        result[sym].sort()
    return result


def _next_monthly_expiry(expiries: List[date], from_month: int, from_year: int) -> Optional[date]:
    """Return the last (furthest) expiry in the given calendar month."""
    candidates = [d for d in expiries if d.month == from_month and d.year == from_year]
    return max(candidates) if candidates else None


# Best config from the parameter sweep
HARD_SL_BUF   = 0.8    # % beyond zone boundary
MIN_RR        = 1.5    # minimum R:R
APPROACH_PCT  = 1.5    # % distance to call a zone "approaching"
MAX_ZONE_AGE  = 60     # days


EARNINGS_MOVE_PCT = 4.5   # single D1 bar move above this % = likely earnings event
EARNINGS_LOOKBACK = 5    # check last N bars for earnings spikes

@dataclass
class Signal:
    symbol:      str
    direction:   str        # "CE" (bear trap -> long) or "PE" (bull trap -> short)
    status:      str        # "TRIGGERED" | "APPROACHING"
    entry_line:  float      # zone level to buy at
    current:     float      # Friday close
    dist_pct:    float      # % distance of close from entry_line
    hard_sl:     float      # spot SL level
    day_t1:      float      # Day-1 target (Friday high or low)
    zone_age:    int        # days since zone locked
    lock_date:   str        # date zone was locked e.g. "24 Jul"
    rr:          float
    suggested_strike: int   # nearest round-step to entry_line
    expiry:      str = ""   # actual contract expiry from registry e.g. "28 AUG 26"


def _nearest_strike(price: float, step: int = 50) -> int:
    return round(price / step) * step


EARNINGS_CUMULATIVE_PCT = 8.0  # 5-day cumulative move above this % = likely earnings rally

def _has_recent_earnings(bars: list, lookback: int = EARNINGS_LOOKBACK,
                         threshold_pct: float = EARNINGS_MOVE_PCT) -> bool:
    """True if recent bars show signs of an earnings-driven move:
    1. Any single D1 bar with close-to-close move > threshold_pct% (gap-style earnings), OR
    2. Cumulative 5-day close-to-close move > EARNINGS_CUMULATIVE_PCT% (gradual earnings rally).
    """
    tail = bars[-lookback:] if len(bars) >= lookback else bars
    if len(tail) < 2:
        return False
    # Check 1: single-bar spike
    for i in range(1, len(tail)):
        prev_c = tail[i - 1].close
        if prev_c <= 0:
            continue
        if abs((tail[i].close - prev_c) / prev_c * 100) >= threshold_pct:
            return True
    # Check 2: cumulative multi-day drift (earnings beat spread over several sessions)
    base_c = tail[0].close
    last_c = tail[-1].close
    if base_c > 0 and abs((last_c - base_c) / base_c * 100) >= EARNINGS_CUMULATIVE_PCT:
        return True
    return False


def scan(token: str) -> List[Signal]:
    end_date   = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=6 * 31)

    # Determine which month to target for positional trading.
    # If we're in the last week of current month (expiry within 7 days),
    # target NEXT month; otherwise target current month.
    today = date.today()
    # Find next month's expiry target: use 2 months ahead if within last week
    target_month = today.month + 1 if today.day >= 24 else today.month
    target_year  = today.year
    if target_month > 12:
        target_month = 1
        target_year += 1

    print(f"\nFetching real expiry dates from Upstox instrument master...")
    stock_expiries = _fetch_stock_expiries(token)
    print(f"  Found expiry data for {len(stock_expiries)} stocks")

    signals: List[Signal] = []

    print(f"\nFnO Live Scanner  --  data up to {end_date}  |  target expiry month: {target_month}/{target_year}")
    print(f"{'─'*70}")

    earnings_skipped: List[str] = []

    for symbol, key in TOP_30_STOCKS.items():
        bars = load_or_fetch(symbol, key, token, start_date, end_date)
        if len(bars) < 20:
            print(f"  {symbol:<14} insufficient data — skip")
            continue

        # Earnings filter: if any recent bar had a >4.5% single-day move, skip.
        # These stocks have zone logic invalidated by earnings surprises (e.g. INFY +4.5%).
        if _has_recent_earnings(bars):
            earnings_skipped.append(symbol)
            print(f"  {symbol:<14} SKIP — recent earnings move >={EARNINGS_MOVE_PCT}%")
            continue

        last_bar = bars[-1]
        today    = last_bar.timestamp.date()

        # All confirmed zones in full history
        bear_zones = find_all_bear_zones(bars)
        bull_zones = find_all_bull_zones(bars)
        all_zones  = [(z, "CE") for z in bear_zones] + [(z, "PE") for z in bull_zones]

        best: Optional[Signal] = None

        for zone, direction in all_zones:
            if zone.entry_line is None or zone.sweep_low is None:
                continue
            if zone.lock_ts is None:
                continue

            age_days = (today - zone.lock_ts.date()).days
            if age_days > MAX_ZONE_AGE:
                continue

            entry_line = zone.entry_line
            sweep_ref  = zone.sweep_low   # sweep_low stores sweep-extreme for both directions

            # zone bounds
            zone_lo = min(entry_line, sweep_ref)
            zone_hi = max(entry_line, sweep_ref)

            if zone_lo <= 0 or zone_hi <= 0:
                continue

            # SL and T1
            if direction == "CE":          # bear trap -> long
                hard_sl   = zone_lo * (1 - HARD_SL_BUF / 100)
                day_t1    = last_bar.high
                risk      = entry_line - hard_sl
                reward    = day_t1 - entry_line
                retest    = last_bar.low <= entry_line and last_bar.close >= zone_lo
                dist_pct  = (last_bar.close - entry_line) / entry_line * 100  # positive = above
            else:                          # bull trap -> short
                hard_sl   = zone_hi * (1 + HARD_SL_BUF / 100)
                day_t1    = last_bar.low
                risk      = hard_sl - entry_line
                reward    = entry_line - day_t1
                retest    = last_bar.high >= entry_line and last_bar.close <= zone_hi
                dist_pct  = (entry_line - last_bar.close) / entry_line * 100  # positive = below

            if risk <= 0 or reward < 0:
                continue

            rr = reward / risk
            if rr < MIN_RR:
                continue

            # Skip if last bar's close has already consumed >80% of entry→T1 headroom.
            # This prevents "stale T1" signals where the stock already ran most of the
            # trade in a previous session and has almost no upside left.
            if direction == "CE":
                t1_room_consumed = (last_bar.close - entry_line) / (day_t1 - entry_line) if (day_t1 - entry_line) > 0 else 1.0
            else:
                t1_room_consumed = (entry_line - last_bar.close) / (entry_line - day_t1) if (entry_line - day_t1) > 0 else 1.0
            if t1_room_consumed > 0.8:
                continue

            # Classify status
            if retest:
                status = "TRIGGERED"
            elif abs(dist_pct) <= APPROACH_PCT:
                status = "APPROACHING"
            else:
                continue

            # Suggested strike: nearest 50-pt round to entry_line
            # Adjust step by stock price range
            if entry_line > 5000:
                step = 100
            elif entry_line > 2000:
                step = 50
            elif entry_line > 500:
                step = 20
            else:
                step = 10
            strike = _nearest_strike(entry_line, step)

            # Real expiry from instrument master
            sym_expiries = stock_expiries.get(symbol, [])
            exp_date = _next_monthly_expiry(sym_expiries, target_month, target_year)
            if exp_date:
                expiry_str = f"{exp_date.day} {exp_date.strftime('%b %y').upper()}"
            else:
                expiry_str = f"? {target_month}/{target_year}"

            lock_date_str = (zone.lock_ts.strftime("%d %b").lstrip("0") if zone.lock_ts else "?")

            sig = Signal(
                symbol=symbol, direction=direction, status=status,
                entry_line=entry_line, current=last_bar.close,
                dist_pct=dist_pct, hard_sl=hard_sl, day_t1=day_t1,
                zone_age=age_days, lock_date=lock_date_str, rr=rr,
                suggested_strike=strike, expiry=expiry_str,
            )

            # Keep the signal with best R:R per stock (TRIGGERED beats APPROACHING)
            if best is None:
                best = sig
            elif sig.status == "TRIGGERED" and best.status != "TRIGGERED":
                best = sig
            elif sig.status == best.status and sig.rr > best.rr:
                best = sig

        if best:
            signals.append(best)

    if earnings_skipped:
        print(f"\n  Earnings-excluded (zone invalidated by recent >{EARNINGS_MOVE_PCT}% move): {', '.join(earnings_skipped)}")

    return signals


def print_report(signals: List[Signal]) -> None:
    triggered   = [s for s in signals if s.status == "TRIGGERED"]
    approaching = [s for s in signals if s.status == "APPROACHING"]

    # Sort each group by R:R descending
    triggered.sort(key=lambda s: s.rr, reverse=True)
    approaching.sort(key=lambda s: abs(s.dist_pct))  # closest first

    print(f"\n{'='*70}")
    print(f"  FnO LIVE SIGNALS  --  positional option picks")
    print(f"{'='*70}")

    if triggered:
        print(f"\n  ** TRIGGERED (enter at next open) **\n")
        print(f"  {'Stock':<12} {'Dir':<4} {'Entry':>7} {'Close':>7} {'SL':>7} {'T1':>7} {'R:R':>5} {'ZoneLock':<9}  Contract")
        print(f"  {'-'*12} {'-'*4} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*5} {'-'*9}  {'-'*20}")
        for s in triggered:
            print(f"  {s.symbol:<12} {s.direction:<4} {s.entry_line:>7.1f} {s.current:>7.1f} "
                  f"{s.hard_sl:>7.1f} {s.day_t1:>7.1f} {s.rr:>5.2f} {s.lock_date:<9}  "
                  f"{s.suggested_strike} {s.direction} {s.expiry}")
    else:
        print("\n  No TRIGGERED signals on last bar.")

    if approaching:
        print(f"\n  -- APPROACHING (watch next session — zone within {APPROACH_PCT}%) --\n")
        print(f"  {'Stock':<12} {'Dir':<4} {'Entry':>7} {'Close':>7} {'Dist%':>6} {'SL':>7} {'R:R':>5} {'ZoneLock':<9}  Contract")
        print(f"  {'-'*12} {'-'*4} {'-'*7} {'-'*7} {'-'*6} {'-'*7} {'-'*5} {'-'*9}  {'-'*20}")
        for s in approaching:
            arrow = "v" if s.direction == "CE" else "^"
            print(f"  {s.symbol:<12} {s.direction:<4} {s.entry_line:>7.1f} {s.current:>7.1f} "
                  f"{arrow}{abs(s.dist_pct):>5.2f}% {s.hard_sl:>7.1f} {s.rr:>5.2f} {s.lock_date:<9}  "
                  f"{s.suggested_strike} {s.direction} {s.expiry}")

    all_sigs = triggered + approaching
    if not all_sigs:
        print("\n  No signals found. Check back after next session.")
        return

    print(f"\n{'='*70}")
    print(f"  TOP 2 PICKS FOR PAPER TRADING")
    print(f"{'='*70}")

    # Pick top 2: prefer TRIGGERED, then best R:R
    top2 = (triggered + approaching)[:2]
    for i, s in enumerate(top2, 1):
        print(f"\n  Pick {i}: {s.symbol} {s.direction}  [{s.status}]")
        print(f"    Contract: {s.suggested_strike} {s.direction} {s.expiry}")
        print(f"    Entry   : buy near spot {s.entry_line:.1f} (buy option at market open)")
        print(f"    Spot SL : {s.hard_sl:.1f}  ({HARD_SL_BUF}% beyond zone boundary)")
        print(f"    Day T1  : {s.day_t1:.1f}  (last session's {'high' if s.direction=='CE' else 'low'} — update intraday)")
        print(f"    R:R     : {s.rr:.2f}")
        print(f"    Zone age: {s.zone_age} days since lock ({s.lock_date})")
        print(f"    Exit    : Day T1 hit -> add hedge ({s.suggested_strike} {'PE' if s.direction=='CE' else 'CE'} {s.expiry})")
        print(f"              Weekly T1 hit -> close both legs")

    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    token = os.environ.get("UPSTOX_TOKEN", "").strip()
    if not token:
        print("ERROR: Set UPSTOX_TOKEN environment variable.")
        sys.exit(1)
    signals = scan(token)
    print_report(signals)
