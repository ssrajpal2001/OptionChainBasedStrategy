"""
scripts/oi_bias_rsi_exit_optimize.py -- parameter sweep for the entry (5-min
default) and exit (1-hour default) StochRSI timeframe + (rsi_period,
stoch_period, k_smooth, d_smooth) lengths, driven entirely off the LOCAL
cache built by scripts/oi_bias_rsi_exit_cache.py (zero network calls here --
this codebase's own documented real incident: a multi-day backtest firing
many real Upstox range-fetches back-to-back tripped a sustained rate limit,
see fetch_upstox_range_1m's own docstring).

Reuses the SAME real functions the live backtest uses (strategies.oi_bias_
rsi_exit.detector.compute_stoch_rsi_double_smoothed/check_entry_state/
check_exit_cross, strategies.core.candle_indicators.to_n_min_bars_market_
anchored) -- this sweep can never behaviorally drift from what
oi_bias_rsi_exit_backtest.py itself would compute for the same parameters.

Approach: greedy two-stage sweep (entry first with exit held at the current
default, then exit with entry fixed at whatever the entry sweep found best)
rather than a full joint grid -- a full (entry_tf x entry_lengths) x
(exit_tf x exit_lengths) grid would be the product of both sweeps' sizes;
greedy is the standard, tractable way to explore a 2-stage pipeline like
this one without that blowup, at the cost of not proving the joint optimum.
Flagged explicitly in the report, not hidden.

Usage: python scripts/oi_bias_rsi_exit_optimize.py [--csv path]
(No token needed -- reads the local cache only.)
"""
from __future__ import annotations

import glob
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional

sys.path.insert(0, ".")

from strategies.core.trap_zone_utils import Bar
from strategies.core.candle_indicators import to_n_min_bars_market_anchored
from strategies.oi_bias_rsi_exit.detector import (
    compute_stoch_rsi_double_smoothed, check_entry_state, check_exit_cross,
)

CACHE_DIR = "data/oi_bias_rsi_exit_cache"
ENTRY_SCAN_START = "09:26"

# (rsi_period, stoch_period, k_smooth, d_smooth) candidates -- kept modest
# and symmetric (rsi_period==stoch_period, k==d) to keep the sweep
# tractable; TradingView's own default (14,14,3,3) is included as the
# baseline every other candidate is judged against.
LENGTH_CANDIDATES = [
    (7, 7, 3, 3),
    (9, 9, 3, 3),
    (14, 14, 3, 3),
    (21, 21, 3, 3),
    (14, 14, 5, 5),
    (9, 9, 5, 5),
]
ENTRY_TF_CANDIDATES = [3, 5, 10, 15]
EXIT_TF_CANDIDATES = [30, 45, 60, 75, 90]

DEFAULT_ENTRY = {"tf": 5, "lengths": (14, 14, 3, 3)}
DEFAULT_EXIT = {"tf": 60, "lengths": (14, 14, 3, 3)}


@dataclass
class CachedRow:
    symbol: str
    trade_date: date
    bias: str
    option_type: str
    strike: int
    lot: int
    stock_bars: List[Bar]
    option_bars: List[Bar]


def _rows_to_bars(rows) -> List[Bar]:
    out = []
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        out.append(Bar(ts=ts, open=r["open"], high=r["high"], low=r["low"], close=r["close"]))
    return out


def _price_near(bars_1m: List[Bar], ts: datetime, max_minutes: int = 15) -> Optional[float]:
    after = [b for b in bars_1m if b.ts >= ts and (b.ts - ts).total_seconds() <= max_minutes * 60]
    if after:
        return min(after, key=lambda b: b.ts).close
    before = [b for b in bars_1m if b.ts < ts and (ts - b.ts).total_seconds() <= max_minutes * 60]
    if before:
        return max(before, key=lambda b: b.ts).close
    return None


def load_cache(csv_symbols_dates=None) -> List[CachedRow]:
    out = []
    for path in sorted(glob.glob(f"{CACHE_DIR}/*.json")):
        with open(path) as f:
            d = json.load(f)
        out.append(CachedRow(
            symbol=d["symbol"], trade_date=date.fromisoformat(d["trade_date"]), bias=d["bias"],
            option_type=d["option_type"], strike=d["strike"], lot=d["lot"],
            stock_bars=sorted(_rows_to_bars(d["stock_bars"]), key=lambda b: b.ts),
            option_bars=sorted(_rows_to_bars(d["option_bars"]), key=lambda b: b.ts),
        ))
    return out


def simulate_one(row: CachedRow, entry_tf: int, entry_lengths, exit_tf: int, exit_lengths):
    """Pure, local (no network) re-simulation of one trade for a given
    (entry_tf, entry_lengths, exit_tf, exit_lengths) candidate -- returns
    pnl_pts or None if this candidate never produces an entry for this row."""
    bars_entry = to_n_min_bars_market_anchored(row.stock_bars, entry_tf)
    bars_exit = to_n_min_bars_market_anchored(row.stock_bars, exit_tf)
    closes_entry = [b.close for b in bars_entry]
    closes_exit = [b.close for b in bars_exit]
    k_e, d_e = compute_stoch_rsi_double_smoothed(closes_entry, *entry_lengths)
    k_x, d_x = compute_stoch_rsi_double_smoothed(closes_exit, *exit_lengths)

    entry_scan_start_time = datetime.strptime(ENTRY_SCAN_START, "%H:%M").time()
    entry_idx = next(
        (i for i, b in enumerate(bars_entry)
         if b.ts.date() == row.trade_date and b.ts.time() >= entry_scan_start_time
         and check_entry_state(k_e[i], d_e[i], row.bias)),
        None)
    if entry_idx is None:
        return None
    entry_ts = bars_entry[entry_idx].ts

    exit_ts = None
    for i in range(1, len(bars_exit)):
        bar = bars_exit[i]
        if bar.ts.date() != row.trade_date:
            continue
        bucket_close = bar.ts + timedelta(minutes=exit_tf)
        if bucket_close <= entry_ts:
            continue
        if check_exit_cross(k_x[i - 1], d_x[i - 1], k_x[i], d_x[i], row.bias):
            exit_ts = bucket_close
            break
    if exit_ts is None:
        trade_day_bars = [b for b in bars_entry if b.ts.date() == row.trade_date]
        exit_ts = trade_day_bars[-1].ts

    entry_price = _price_near(row.option_bars, entry_ts)
    exit_price = _price_near(row.option_bars, exit_ts)
    if entry_price is None or exit_price is None:
        return None
    return (exit_price - entry_price) * row.lot


def run_config(rows: List[CachedRow], entry_tf, entry_lengths, exit_tf, exit_lengths):
    total = 0.0
    n = 0
    wins = 0
    for row in rows:
        pnl = simulate_one(row, entry_tf, entry_lengths, exit_tf, exit_lengths)
        if pnl is None:
            continue
        total += pnl
        n += 1
        if pnl > 0:
            wins += 1
    return {"net": total, "n": n, "win_pct": (100.0 * wins / n) if n else 0.0}


def main() -> None:
    rows = load_cache()
    if not rows:
        print(f"No cached rows found in {CACHE_DIR}/ -- run scripts/oi_bias_rsi_exit_cache.py "
              "with a valid Upstox token first.")
        return
    print(f"Loaded {len(rows)} cached real (symbol, date) rows.\n")

    baseline = run_config(rows, DEFAULT_ENTRY["tf"], DEFAULT_ENTRY["lengths"],
                           DEFAULT_EXIT["tf"], DEFAULT_EXIT["lengths"])
    print(f"BASELINE (entry {DEFAULT_ENTRY['tf']}m {DEFAULT_ENTRY['lengths']}, "
          f"exit {DEFAULT_EXIT['tf']}m {DEFAULT_EXIT['lengths']}): "
          f"n={baseline['n']} win%={baseline['win_pct']:.1f} net=Rs{baseline['net']:+.0f}\n")

    # ---- Stage 1: sweep ENTRY tf + lengths, exit held at default ----
    print("=== STAGE 1: entry timeframe + StochRSI length sweep (exit held at default) ===")
    entry_results = []
    for tf in ENTRY_TF_CANDIDATES:
        for lengths in LENGTH_CANDIDATES:
            res = run_config(rows, tf, lengths, DEFAULT_EXIT["tf"], DEFAULT_EXIT["lengths"])
            entry_results.append((tf, lengths, res))
    entry_results.sort(key=lambda r: r[2]["net"], reverse=True)
    for tf, lengths, res in entry_results:
        print(f"  entry_tf={tf:>3}m  lengths={lengths}  n={res['n']:>2}  "
              f"win%={res['win_pct']:>5.1f}  net=Rs{res['net']:+.0f}")
    best_entry_tf, best_entry_lengths, best_entry_res = entry_results[0]
    print(f"\nBest entry config: {best_entry_tf}m {best_entry_lengths} "
          f"-> net=Rs{best_entry_res['net']:+.0f} (win%={best_entry_res['win_pct']:.1f}, n={best_entry_res['n']})\n")

    # ---- Stage 2: sweep EXIT tf + lengths, entry fixed at Stage 1's best ----
    print("=== STAGE 2: exit timeframe + StochRSI length sweep (entry fixed at Stage 1 best) ===")
    exit_results = []
    for tf in EXIT_TF_CANDIDATES:
        for lengths in LENGTH_CANDIDATES:
            res = run_config(rows, best_entry_tf, best_entry_lengths, tf, lengths)
            exit_results.append((tf, lengths, res))
    exit_results.sort(key=lambda r: r[2]["net"], reverse=True)
    for tf, lengths, res in exit_results:
        print(f"  exit_tf={tf:>3}m  lengths={lengths}  n={res['n']:>2}  "
              f"win%={res['win_pct']:>5.1f}  net=Rs{res['net']:+.0f}")
    best_exit_tf, best_exit_lengths, best_exit_res = exit_results[0]

    print(f"\n=== RECOMMENDED CONFIG ===")
    print(f"Entry: {best_entry_tf}-min StochRSI{best_entry_lengths}")
    print(f"Exit:  {best_exit_tf}-min StochRSI{best_exit_lengths}")
    print(f"Result: n={best_exit_res['n']} win%={best_exit_res['win_pct']:.1f} net=Rs{best_exit_res['net']:+.0f} "
          f"(baseline was net=Rs{baseline['net']:+.0f})")
    print("\nNote: this is a GREEDY 2-stage sweep (entry optimized first, then exit fixed to it), "
          "not a full joint grid search -- the true joint optimum could differ. Small sample "
          f"(n={len(rows)} real trades) -- treat as directional, not final.")


if __name__ == "__main__":
    main()
