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

import os, sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from config.global_config import IST
from backtest.fno_scanner.backtest import TOP_30_STOCKS, load_or_fetch, Bar
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones

# Best config from the parameter sweep
HARD_SL_BUF   = 0.8    # % beyond zone boundary
MIN_RR        = 1.0    # minimum R:R
APPROACH_PCT  = 1.5    # % distance to call a zone "approaching"
MAX_ZONE_AGE  = 60     # days


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
    zone_age:    int         # days since zone locked
    rr:          float
    suggested_strike: int   # nearest 100-round to entry_line


def _nearest_strike(price: float, step: int = 50) -> int:
    return round(price / step) * step


def scan(token: str) -> List[Signal]:
    end_date   = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=6 * 31)

    signals: List[Signal] = []

    print(f"\nFnO Live Scanner  —  data up to {end_date}")
    print(f"{'─'*70}")

    for symbol, key in TOP_30_STOCKS.items():
        bars = load_or_fetch(symbol, key, token, start_date, end_date)
        if len(bars) < 20:
            print(f"  {symbol:<14} insufficient data — skip")
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

            sig = Signal(
                symbol=symbol, direction=direction, status=status,
                entry_line=entry_line, current=last_bar.close,
                dist_pct=dist_pct, hard_sl=hard_sl, day_t1=day_t1,
                zone_age=age_days, rr=rr, suggested_strike=strike,
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

    return signals


def print_report(signals: List[Signal]) -> None:
    triggered   = [s for s in signals if s.status == "TRIGGERED"]
    approaching = [s for s in signals if s.status == "APPROACHING"]

    # Sort each group by R:R descending
    triggered.sort(key=lambda s: s.rr, reverse=True)
    approaching.sort(key=lambda s: abs(s.dist_pct))  # closest first

    aug_expiry = "25 AUG 26"

    print(f"\n{'='*70}")
    print(f"  FnO LIVE SIGNALS  —  August Expiry ({aug_expiry})")
    print(f"{'='*70}")

    if triggered:
        print(f"\n  ** TRIGGERED (enter at Monday open) **\n")
        print(f"  {'Stock':<12} {'Dir':<4} {'Entry':>7} {'Close':>7} {'SL':>7} {'T1':>7} {'R:R':>5}  {'Strike'}")
        print(f"  {'-'*12} {'-'*4} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*5}  {'-'*12}")
        for s in triggered:
            print(f"  {s.symbol:<12} {s.direction:<4} {s.entry_line:>7.1f} {s.current:>7.1f} "
                  f"{s.hard_sl:>7.1f} {s.day_t1:>7.1f} {s.rr:>5.2f}  "
                  f"{s.suggested_strike} {s.direction} {aug_expiry}")
    else:
        print("\n  No TRIGGERED signals on last bar.")

    if approaching:
        print(f"\n  -- APPROACHING (watch Monday — zone within {APPROACH_PCT}%) --\n")
        print(f"  {'Stock':<12} {'Dir':<4} {'Entry':>7} {'Close':>7} {'Dist%':>6} {'SL':>7} {'R:R':>5}  {'Strike'}")
        print(f"  {'-'*12} {'-'*4} {'-'*7} {'-'*7} {'-'*6} {'-'*7} {'-'*5}  {'-'*12}")
        for s in approaching:
            arrow = "v" if s.direction == "CE" else "^"
            print(f"  {s.symbol:<12} {s.direction:<4} {s.entry_line:>7.1f} {s.current:>7.1f} "
                  f"{arrow}{abs(s.dist_pct):>5.2f}% {s.hard_sl:>7.1f} {s.rr:>5.2f}  "
                  f"{s.suggested_strike} {s.direction} {aug_expiry}")

    all_sigs = triggered + approaching
    if not all_sigs:
        print("\n  No signals found. Check back Monday after open.")
        return

    print(f"\n{'='*70}")
    print(f"  TOP 2 PICKS FOR PAPER TRADING (August expiry)")
    print(f"{'='*70}")

    # Pick top 2: prefer TRIGGERED, then best R:R
    top2 = (triggered + approaching)[:2]
    for i, s in enumerate(top2, 1):
        print(f"\n  Pick {i}: {s.symbol} {s.direction}  [{s.status}]")
        print(f"    Option  : {s.suggested_strike} {s.direction} {aug_expiry}")
        print(f"    Entry   : buy near spot {s.entry_line:.1f} (buy option at market open Monday)")
        print(f"    Spot SL : {s.hard_sl:.1f}  ({HARD_SL_BUF}% below/above zone)")
        print(f"    Day T1  : {s.day_t1:.1f}  (Friday's {'high' if s.direction=='CE' else 'low'} — recheck Monday high/low)")
        print(f"    R:R     : {s.rr:.2f}")
        print(f"    Zone age: {s.zone_age} days since lock")
        print(f"    Exit    : Day T1 hit -> add hedge ({s.suggested_strike} {'PE' if s.direction=='CE' else 'CE'})")
        print(f"              Weekly T1 hit -> close both legs")

    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    token = os.environ.get("UPSTOX_TOKEN", "").strip()
    if not token:
        print("ERROR: Set UPSTOX_TOKEN environment variable.")
        sys.exit(1)
    signals = scan(token)
    print_report(signals)
