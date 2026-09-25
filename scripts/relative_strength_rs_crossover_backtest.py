"""
scripts/relative_strength_rs_crossover_backtest.py

Direct user spec, 2026-09-24: backtest the RS sign-crossover rule (buy a
stock the hour its RS vs its own sector index flips negative->positive,
exit the hour RS goes negative again) over the last 2 years, across all 5
sectors from the earlier scan (NIFTY PHARMA, NIFTY HEALTHCARE, NIFTY METAL,
NIFTY SMLCAP 50, NIFTY MICROCAP250) and their real, live-fetched
constituent stocks (356 total).

Real 2-year hourly data throughout (data_layer.historical_candles.
fetch_upstox_hourly_range, chunked in ~85-day windows per Upstox's
confirmed real API ceiling, paced to avoid the rate-limit incident already
documented elsewhere in this codebase). This is a genuinely long-running
script (356+5 instruments x ~9 chunked calls each, sequential + paced) --
expect it to take a long while; progress is printed per symbol so it can be
tailed while running.

MUST run on EC2 (or wherever a real Upstox access token is available).

Usage: python scripts/relative_strength_rs_crossover_backtest.py [--out path.json]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer.client_db import ClientDB
from data_layer.historical_candles import fetch_upstox_hourly_range
from strategies.oi_orb_screener.screener import NSESession
from strategies.oi_orb_screener.stock_resolve import resolve_eq_instrument_key
from strategies.relative_strength.backtest import (
    backtest_rs_crossover, summarize_trades, aggregate_summaries,
)
from strategies.relative_strength.detector import align_closes_with_ts
from strategies.relative_strength.sectors import resolve_index_key, fetch_sector_constituents

SECTORS = ["NIFTY PHARMA", "NIFTY HEALTHCARE", "NIFTY METAL", "NIFTY SMLCAP 50", "NIFTY MICROCAP250"]
RS_LENGTH = 123
YEARS_BACK = 2


def _access_token() -> str:
    db = ClientDB()
    for account in ("upstox2", "upstox"):
        creds = db.get_feeder_creds_sync(account)
        if creds and creds.get("access_token"):
            return creds["access_token"]
    raise RuntimeError("No upstox/upstox2 feeder access_token found -- run this on EC2.")


async def _run(token: str, out_path: str) -> None:
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=365 * YEARS_BACK)

    nse = NSESession()
    all_summaries = []
    all_trades_by_symbol = {}
    per_sector = {}

    for sector_name in SECTORS:
        t0 = time.time()
        print(f"\n{'='*70}\n{sector_name}\n{'='*70}", flush=True)

        sector_key = resolve_index_key(sector_name)
        if not sector_key:
            print(f"  SKIP: could not resolve Upstox key for {sector_name}", flush=True)
            continue

        sector_candles = await fetch_upstox_hourly_range(sector_key, token, start, end)
        if not sector_candles:
            print(f"  SKIP: no hourly history for sector index {sector_name}", flush=True)
            continue
        print(f"  sector index: {len(sector_candles)} hourly candles", flush=True)

        constituents = fetch_sector_constituents(nse, sector_name)
        if not constituents:
            print(f"  SKIP: no live NSE constituents for {sector_name}", flush=True)
            continue
        print(f"  {len(constituents)} constituents to backtest", flush=True)

        sector_summaries = []
        for i, symbol in enumerate(constituents, 1):
            stock_key = resolve_eq_instrument_key(symbol)
            if not stock_key:
                print(f"  [{i}/{len(constituents)}] {symbol}: no Upstox key, skipping", flush=True)
                continue
            stock_candles = await fetch_upstox_hourly_range(stock_key, token, start, end)
            if not stock_candles:
                print(f"  [{i}/{len(constituents)}] {symbol}: no hourly history, skipping", flush=True)
                continue
            ts, stock_closes, sector_closes = align_closes_with_ts(stock_candles, sector_candles)
            if len(ts) <= RS_LENGTH:
                print(f"  [{i}/{len(constituents)}] {symbol}: only {len(ts)} aligned bars "
                      f"(need >{RS_LENGTH}), skipping", flush=True)
                continue
            trades = backtest_rs_crossover(symbol, ts, stock_closes, sector_closes, length=RS_LENGTH)
            summary = summarize_trades(symbol, trades)
            sector_summaries.append(summary)
            all_summaries.append(summary)
            all_trades_by_symbol[symbol] = trades
            print(f"  [{i}/{len(constituents)}] {symbol}: {summary.total_trades} trades, "
                  f"win_rate={summary.win_rate}, total_return={summary.total_return_pct:+.4f}", flush=True)

        agg = aggregate_summaries(sector_summaries)
        per_sector[sector_name] = agg
        print(f"  SECTOR AGGREGATE: {agg}  ({time.time()-t0:.0f}s)", flush=True)

    overall = aggregate_summaries(all_summaries)
    print(f"\n{'='*70}\nOVERALL: {overall}\n{'='*70}", flush=True)

    payload = {
        "generated_at": datetime_now_iso(),
        "years_back": YEARS_BACK,
        "rs_length": RS_LENGTH,
        "per_sector": per_sector,
        "overall": overall,
        "per_symbol": [
            {
                "symbol": s.symbol, "total_trades": s.total_trades, "closed_trades": s.closed_trades,
                "open_trades": s.open_trades, "wins": s.wins, "losses": s.losses,
                "win_rate": s.win_rate, "total_return_pct": s.total_return_pct,
                "avg_return_pct": s.avg_return_pct, "best_trade_pct": s.best_trade_pct,
                "worst_trade_pct": s.worst_trade_pct,
            }
            for s in all_summaries
        ],
        "trades": [
            {
                "symbol": t.symbol, "entry_ts": t.entry_ts, "entry_price": t.entry_price,
                "exit_ts": t.exit_ts, "exit_price": t.exit_price, "return_pct": t.return_pct,
                "still_open": t.still_open,
            }
            for trades in all_trades_by_symbol.values() for t in trades
        ],
    }
    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    print(f"\nSaved full results to {out_path}", flush=True)


def datetime_now_iso() -> str:
    from datetime import datetime
    return datetime.now(IST).isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="data/relative_strength_rs_crossover_backtest.json")
    args = parser.parse_args()
    token = _access_token()
    asyncio.run(_run(token, args.out))


if __name__ == "__main__":
    main()
