"""
scripts/relative_strength_scan.py

Direct user spec, 2026-09-24: port bharatTrader's "Relative Strength" Pine
Script indicator to Python, use it on WEEKLY timeframe to rank NSE sectoral
indices against NIFTY, then on HOURLY timeframe to rank the best sector's
real (live NSE-fetched) constituent stocks against that sector's own index.
No backtest/validation pass (direct user instruction) -- this is a live
scanner only, run manually or via cron, same pattern as the existing FnO
nightly scanner.

MUST run on EC2 (real Upstox account access token + real historical
candle/NSE data).

Usage:
    python scripts/relative_strength_scan.py
    python scripts/relative_strength_scan.py --top-n 15
    python scripts/relative_strength_scan.py --save
    python scripts/relative_strength_scan.py --save --out data/relative_strength_scan.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer.client_db import ClientDB
from strategies.relative_strength.scan import run_full_scan


def _access_token() -> str:
    db = ClientDB()
    for account in ("upstox2", "upstox"):
        creds = db.get_feeder_creds_sync(account)
        if creds and creds.get("access_token"):
            return creds["access_token"]
    raise RuntimeError("No upstox/upstox2 feeder access_token found -- run this on EC2.")


def _print_report(result) -> None:
    print("=" * 78)
    print("SECTOR RANKING (weekly RS vs NIFTY)")
    print("=" * 78)
    if not result.sectors_ranked:
        print("  (no sectors ranked -- see warnings above for why)")
    for i, s in enumerate(result.sectors_ranked, 1):
        trend = s.rs.rs_trend or "?"
        print(f"  {i:>2}. {s.sector_name:<26} RS={s.rs.rs:+.4f}  trend={trend:<8} "
              f"ma_trend={s.rs.ma_trend or '?'}")

    if result.best_sector is None:
        print("\nNo best sector -- stock ranking skipped.")
        return

    print()
    print("=" * 78)
    print(f"BEST SECTOR: {result.best_sector.sector_name}  "
          f"(RS={result.best_sector.rs.rs:+.4f})")
    print("STOCK RANKING (hourly RS vs sector index)")
    print("=" * 78)
    if not result.stocks_ranked:
        print("  (no stocks ranked -- see warnings above for why)")
    for i, st in enumerate(result.stocks_ranked, 1):
        trend = st.rs.rs_trend or "?"
        cap_tag = "BLUE-CHIP" if st.is_large_cap else "mid/small"
        print(f"  {i:>2}. {st.symbol:<15} RS={st.rs.rs:+.4f}  cap_weighted={st.cap_weighted_rs:+.4f} "
              f" [{cap_tag}]  trend={trend:<8} ma_trend={st.rs.ma_trend or '?'}")


def _to_jsonable(result) -> dict:
    return {
        "generated_at": datetime.now(IST).isoformat(),
        "sectors_ranked": [
            {"sector_name": s.sector_name, "sector_key": s.sector_key, "rs": s.rs.rs,
             "rs_trend": s.rs.rs_trend, "rs_ma": s.rs.rs_ma, "ma_trend": s.rs.ma_trend}
            for s in result.sectors_ranked
        ],
        "best_sector": result.best_sector.sector_name if result.best_sector else None,
        "stocks_ranked": [
            {"symbol": st.symbol, "stock_key": st.stock_key, "rs": st.rs.rs,
             "rs_trend": st.rs.rs_trend, "rs_ma": st.rs.rs_ma, "ma_trend": st.rs.ma_trend,
             "is_large_cap": st.is_large_cap, "cap_weighted_rs": st.cap_weighted_rs}
            for st in result.stocks_ranked
        ],
    }


async def _run(args) -> None:
    token = _access_token()
    result = await run_full_scan(token, top_n_stocks=args.top_n)
    _print_report(result)
    if args.save:
        payload = _to_jsonable(result)
        with open(args.out, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"\nSaved to {args.out}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--top-n", type=int, default=10,
                         help="How many top-ranked stocks to show/save (default 10).")
    parser.add_argument("--save", action="store_true", help="Save the result as JSON.")
    parser.add_argument("--out", default="data/relative_strength_scan.json",
                         help="Output path when --save is set.")
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
