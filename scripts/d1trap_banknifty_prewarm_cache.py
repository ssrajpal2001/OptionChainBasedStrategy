"""
scripts/d1trap_banknifty_prewarm_cache.py — one-time, SEQUENTIAL cache warm-up
for the BANKNIFTY sweep (scripts/d1trap_banknifty_sweep.py).

Why this exists: strategies/d1_trap_option/book.py's _fetch_bars() caches
each historical-candle chunk to data/trap_zone_cache/<key>_<interval>_
<start>_<end>.json via a tmp-file + os.replace(). That's safe for a single
process, but running several sweep configs in parallel (e.g. the HTF 60m/
30m/15m sweep, which all use the SAME 300pt-offset strikes and date range)
makes multiple processes race to write the SAME cache file at the same
time -- confirmed live: stage1_htf60 crashed with
PermissionError: [WinError 32] ... .json.tmp -> ...json (another process
had the same tmp file open). This script fetches every (strike, side)
instrument needed for the full sweep date range ONCE, sequentially, so the
cache is fully populated before any parallel sweep run starts -- every
subsequent _fetch_bars() call then hits the "if os.path.exists(cache_file):
read" branch and never writes, so concurrent sweep processes can safely
read the same files with zero race.

Only fetches for itm_offset_pts=300 (BANKNIFTY's current default, and what
all of Stage 1/most later stages use) -- Stage 2 (ITM depth sweep) trades
different strikes and needs its own pre-warm pass with --itm-offset.

Usage:
    python3 scripts/d1trap_banknifty_prewarm_cache.py --itm-offset 300
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

UNDERLYING = "BANKNIFTY"
START_DATE = date(2026, 7, 1)
ATM_ROUND_STEP = 100


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--itm-offset", type=int, default=300)
    ap.add_argument("--start-date", default=START_DATE.isoformat())
    args = ap.parse_args()
    start_date = date.fromisoformat(args.start_date)

    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    token = (creds or {}).get("access_token", "")
    if not token:
        print("FATAL: no Upstox access_token in data/clients.db.")
        return 1

    today = datetime.now(IST).date()
    await asyncio.to_thread(REGISTRY.load_sync, UNDERLYING, token)
    expiry = REGISTRY.get_active_expiry(UNDERLYING)
    if not expiry:
        print("FATAL: could not resolve an active BANKNIFTY expiry.")
        return 1
    print(f"Resolved expiry: {expiry}")

    spot_key = _upstox_key_for(UNDERLYING)
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
        opt_key = REGISTRY.get_upstox_key(UNDERLYING, expiry, strike, side)
        if not opt_key:
            print(f"  [{i}/{len(needed)}] {strike}{side}: SKIP -- no instrument key.")
            continue
        bars = await asyncio.to_thread(_fetch_1m_bars, opt_key, fetch_start, today, token)
        print(f"  [{i}/{len(needed)}] {strike}{side}: {len(bars)} bars cached.")

    print("\nCache warm-up complete -- safe to launch parallel sweep runs now.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
