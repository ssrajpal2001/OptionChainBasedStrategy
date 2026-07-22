"""backtest/v4_cascade/main.py -- fetch NIFTY spot history, grid-search V4
Cascade's exit parameters, write a report.

Usage:
    UPSTOX_TOKEN=... python backtest/v4_cascade/main.py [--days 90]

Token is read from the UPSTOX_TOKEN env var only -- never written to any
cached/output file, never logged."""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, ".")

from backtest.v4_cascade.data_fetch import fetch_nifty_spot_1m
from backtest.v4_cascade.optimizer import grid_search
from backtest.v4_cascade.reporting import write_report
from backtest.v4_cascade.run_backtest import build_5m_bars


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=90, help="calendar days back to fetch")
    parser.add_argument("--lot-size", type=int, default=65)
    parser.add_argument("--lot-multiplier", type=int, default=2)
    args = parser.parse_args()

    token = os.environ.get("UPSTOX_TOKEN", "")
    if not token:
        print("UPSTOX_TOKEN not set"); return

    end = date.today()
    start = end - timedelta(days=args.days)
    print(f"Fetching NIFTY spot 1m history {start} .. {end} ...")
    rows = await fetch_nifty_spot_1m(token, start, end)
    print(f"Fetched {len(rows)} 1-minute rows.")
    if not rows:
        print("No data fetched -- aborting."); return

    bars_5m = build_5m_bars(rows)
    print(f"Built {len(bars_5m)} 5-minute bars.")

    print("Running grid search (this replays the full history once per parameter combo)...")
    results = grid_search(bars_5m, underlying="NIFTY", lot_size=args.lot_size,
                           lot_multiplier=args.lot_multiplier)
    print(f"Grid search complete: {len(results)} parameter combinations evaluated.")

    write_report(results, (start, end))
    best = results[0]
    print(f"\nBest: sl_buffer={best['params']['sl_buffer']} "
          f"target_floor_x={best['params']['target_floor_multiple']} "
          f"tsl_bases={best['params']['t2_trail_lookback_bases']}")
    print(f"  trades={best['metrics']['trades']} win_rate={best['metrics']['win_rate']}% "
          f"PF={best['metrics']['profit_factor']} max_dd=Rs {best['metrics']['max_drawdown']} "
          f"net_pnl=Rs {best['metrics']['net_pnl']}")
    print("\nWrote backtest/v4_cascade/results/report.md, trades.csv, best_params.json")


if __name__ == "__main__":
    asyncio.run(main())
