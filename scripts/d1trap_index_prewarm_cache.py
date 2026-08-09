"""
scripts/d1trap_index_prewarm_cache.py — generic (any monthly-expiry index)
version of scripts/d1trap_banknifty_prewarm_cache.py, 2026-08-09. Sequential
cache warm-up so parallel sweep runs (scripts/d1trap_index_full_sweep.py)
never race on the same data_layer/trap_zone_cache/ file (confirmed real
Windows race earlier this session -- see that script's own docstring).

Usage:
    python3 scripts/d1trap_index_prewarm_cache.py --underlying FINNIFTY --itm-offset 150
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.global_config import IST  # noqa: E402
from data_layer.client_db import ClientDB  # noqa: E402
from data_layer.instrument_registry import REGISTRY  # noqa: E402
from strategies.d1_trap_option.book import _fetch_1m_bars, _upstox_key_for  # noqa: E402
import strategies.d1_trap_option.bear_only_book as bb  # noqa: E402

START_DATE = date(2026, 7, 1)
ATM_ROUND_STEP = 100


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--underlying", required=True)
    ap.add_argument("--itm-offset", type=int, required=True)
    ap.add_argument("--start-date", default=START_DATE.isoformat())
    args = ap.parse_args()
    underlying = args.underlying
    start_date = date.fromisoformat(args.start_date)

    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    token = (creds or {}).get("access_token", "")
    if not token:
        print("FATAL: no Upstox access_token in data/clients.db.")
        return 1

    today = datetime.now(IST).date()
    await asyncio.to_thread(REGISTRY.load_sync, underlying, token)
    expiry = REGISTRY.get_active_expiry(underlying)
    if not expiry:
        print(f"FATAL: could not resolve an active {underlying} expiry.")
        return 1
    print(f"Resolved expiry: {expiry}")

    spot_key = _upstox_key_for(underlying)
    fetch_start = start_date - timedelta(days=bb._HIST_WARMUP_DAYS)
    print(f"Fetching spot 1m bars {fetch_start} .. {today} ...")
    spot_bars = await asyncio.to_thread(_fetch_1m_bars, spot_key, fetch_start, today, token)
    if not spot_bars:
        print("FATAL: no real spot data returned.")
        return 1

    import pandas as pd
    spot_df = pd.DataFrame([{"datetime": b.timestamp, "open": b.open} for b in spot_bars])
    trading_days = sorted(d for d in spot_df["datetime"].dt.date.unique() if start_date <= d <= today)
    print(f"{len(trading_days)} trading day(s): {trading_days[0]} .. {trading_days[-1]}")

    needed: dict[tuple, None] = {}
    for day in trading_days:
        day_opens = spot_df[spot_df["datetime"].dt.date == day]
        if day_opens.empty:
            continue
        spot_open = float(day_opens.iloc[0]["open"])
        atm = round(spot_open / ATM_ROUND_STEP) * ATM_ROUND_STEP
        ce_strike, pe_strike = int(atm - args.itm_offset), int(atm + args.itm_offset)
        needed[(ce_strike, "CE")] = None
        needed[(pe_strike, "PE")] = None
    print(f"{len(needed)} unique (strike, side) instrument(s) to warm at itm_offset={args.itm_offset}.")

    for i, (strike, side) in enumerate(needed, 1):
        opt_key = REGISTRY.get_upstox_key(underlying, expiry, strike, side)
        if not opt_key:
            print(f"  [{i}/{len(needed)}] {strike}{side}: SKIP -- no instrument key.")
            continue
        bars = await asyncio.to_thread(_fetch_1m_bars, opt_key, fetch_start, today, token)
        print(f"  [{i}/{len(needed)}] {strike}{side}: {len(bars)} bars cached.")

    print("\nCache warm-up complete -- safe to launch parallel sweep runs now.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
