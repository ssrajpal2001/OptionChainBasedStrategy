"""
scripts/diag_v4_ce_pe_traps.py — diagnostic: run the REAL production Gate-1
(75m 2-candle sweep+reclaim) scanner against the actual CE/PE tracking
contracts, using the exact same fetch/merge/resample functions the live
V4CascadeBook uses, so the results can be checked directly against a real
chart. Prints every zone found, oldest first, with full ref/sweep/lock
detail.

Unlike scripts/v4_htf_trap_report.py (stale — spot-only, uses the deprecated
multiplier ladder, and the dated-range-only fetch that never includes
today's data), this uses:
  - data_layer.historical_candles.fetch_upstox_range_1m + fetch_upstox_intraday_1m
    (merged, same as strategies/v4_cascade/book.py._ingest_history)
  - strategies.v4_cascade.rolling_base.find_all_bear_traps_2candle (the real
    Gate-1 scanner — NOT scan_ladder/build_ladder, which the current funnel
    no longer uses)
  - strategies.v4_cascade.rolling_base.resample_bars (real production resample)

Reads the Upstox access token from data/clients.db (same token the live app
already uses) — no separate token needed.

Usage:
    python scripts/diag_v4_ce_pe_traps.py --strike 24400 --opt-type PE --expiry 2026-07-21
    python scripts/diag_v4_ce_pe_traps.py --strike 24000 --opt-type CE --expiry 2026-07-21 --days 14
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date, timedelta

sys.path.insert(0, ".")

from data_layer.client_db import ClientDB
from data_layer.historical_candles import fetch_upstox_range_1m, fetch_upstox_intraday_1m
from data_layer.instrument_registry import REGISTRY
from strategies.v4_cascade.book import _merge_rows, _to_5m_bars
from strategies.v4_cascade.rolling_base import find_all_bear_traps_2candle, resample_bars


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--underlying", default="NIFTY")
    ap.add_argument("--strike", type=int, required=True)
    ap.add_argument("--opt-type", choices=["CE", "PE"], required=True)
    ap.add_argument("--expiry", required=True, help="YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=14, help="lookback days from today")
    args = ap.parse_args()

    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    if not creds or not creds.get("access_token"):
        print("FATAL: no Upstox access_token in data/clients.db.")
        sys.exit(1)
    token = creds["access_token"]

    print(f"Loading {args.underlying} contract registry ...")
    REGISTRY.load_sync(args.underlying, token)

    expiry = date.fromisoformat(args.expiry)
    key = REGISTRY.get_upstox_key(args.underlying, expiry, args.strike, args.opt_type)
    print(f"instrument_key = {key}")
    if not key:
        print("FATAL: could not resolve instrument_key — check strike/expiry/underlying.")
        sys.exit(1)

    today = date.today()
    start = today - timedelta(days=args.days)
    print(f"Fetching {start} -> {today} (range) + today (intraday) ...")
    range_rows, today_rows = await asyncio.gather(
        fetch_upstox_range_1m(key, token, start, today),
        fetch_upstox_intraday_1m(key, token),
    )
    print(f"  range rows: {len(range_rows)}   today (intraday) rows: {len(today_rows)}")
    merged = _merge_rows(range_rows, today_rows)
    print(f"  merged total: {len(merged)}")
    if not merged:
        print("FATAL: no candles at all — check token validity / instrument_key / market days.")
        sys.exit(1)

    # Show today's own candle count specifically, so it's obvious whether
    # today's data actually made it into the merge.
    today_str = today.isoformat()
    today_count = sum(1 for r in merged if r["ts"].startswith(today_str))
    print(f"  of which TODAY ({today_str}): {today_count} 1m candles")
    if today_count == 0:
        print("  *** WARNING: zero candles for today made it into the merged data. ***")

    bars_5m = _to_5m_bars(merged, filter_zero_volume=True)
    print(f"Resampled to {len(bars_5m)} x 5m bars.")
    bars_75m = resample_bars(bars_5m, 75)
    print(f"Resampled to {len(bars_75m)} x 75m bars (Gate 1's real timeframe).")

    zones = find_all_bear_traps_2candle(bars_75m)
    print(f"\n=== BEAR-TRAP zones found (Gate 1, real production scanner): {len(zones)} ===")
    for z in sorted(zones, key=lambda zz: zz.reference_low_ts):
        print(
            f"  ref_ts={z.reference_low_ts.isoformat()}  entry_line={z.entry_line:.2f}  "
            f"sl_level={z.sl_level:.2f}  sweep_ts={z.sweep_started_ts.isoformat() if z.sweep_started_ts else None}  "
            f"sweep_low={z.sweep_low:.2f}  TRAPPED(lock)_ts={z.lock_ts.isoformat()}"
        )
    if not zones:
        print("  (none found — either genuinely no valid pattern, or a real bug)")


if __name__ == "__main__":
    asyncio.run(main())
