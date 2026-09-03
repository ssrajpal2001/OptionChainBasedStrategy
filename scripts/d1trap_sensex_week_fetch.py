"""Fetch real 1-min premium history for the SENSEX week backtest (07-27..07-31),
all under the SAME currently-active weekly contract (expiry 2026-08-06) -- this
contract has genuinely been trading since 2026-07-10 (BSE lists several weeklies
in parallel, unlike NSE's single-week model), so it's real, un-contaminated data
for the whole window: no rollover, no expired-instrument-master gap.

Daily-rolling ATM+/-500 CE/PE strikes computed from the cached spot parquet
(sensex_1m_fullmonth_spot.parquet), one pair per trading day 07-27..07-31, plus
the 14-day warmup window (from 07-13) so zone detection has real history to
seed from -- same shape as scripts/d1trap_month_rolling_backtest.py's per-day
strike selection, just scoped to a single real week.
"""
import sys
sys.path.insert(0, ".")
import asyncio
from datetime import date, timedelta
import pandas as pd
from data_layer.instrument_registry import REGISTRY
from data_layer.historical_candles import fetch_upstox_range_1m

TOKEN = open(r"C:\Users\SERVER\AppData\Local\Temp\claude\e--AlgoSoft-OptionChainBasedStrategy\3f952902-2e64-455f-be1c-fac0a7378cbc\scratchpad\upstox_token.txt").read().strip()
OUT_DIR = "data/d1trap_fractal_cache/sensex_week"
EXPIRY = date(2026, 8, 6)
FETCH_START = date(2026, 7, 10)
FETCH_END = date(2026, 7, 31)
WEEK_DAYS = [date(2026, 7, 27), date(2026, 7, 28), date(2026, 7, 29), date(2026, 7, 30), date(2026, 7, 31)]
OFFSET, STEP = 500, 100


async def main():
    import os
    os.makedirs(OUT_DIR, exist_ok=True)
    REGISTRY.load_sync("SENSEX", TOKEN)

    spot = pd.read_parquet("data/d1trap_fractal_cache/sensex_1m_fullmonth_spot.parquet")
    spot["datetime"] = pd.to_datetime(spot["datetime"])

    strikes = set()
    for d in WEEK_DAYS:
        day_df = spot[spot["datetime"].dt.date == d]
        if day_df.empty:
            print(f"  {d}: NO SPOT DATA -- skip")
            continue
        o = day_df.iloc[0]["open"]
        atm = round(o / 100) * 100
        ce, pe = int(atm - OFFSET), int(atm + OFFSET)
        print(f"{d}  spot_open={o:.2f}  ATM={atm}  CE={ce}  PE={pe}")
        strikes.add((ce, "CE"))
        strikes.add((pe, "PE"))

    print(f"\nFetching {len(strikes)} unique (strike,side) pairs, {FETCH_START}..{FETCH_END}, expiry={EXPIRY}...")
    for strike, side in sorted(strikes):
        key = REGISTRY.get_upstox_key("SENSEX", EXPIRY, strike, side)
        if not key:
            print(f"  SKIP {strike}{side}: no key")
            continue
        rows = await fetch_upstox_range_1m(key, TOKEN, FETCH_START, FETCH_END)
        df = pd.DataFrame(rows)
        if df.empty:
            print(f"  {strike}{side}: 0 rows")
            continue
        df = df.rename(columns={"ts": "datetime"})
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)
        fname = f"{OUT_DIR}/sensexweek_{strike}_{side}.parquet"
        df.to_parquet(fname)
        print(f"  {strike}{side}: {len(df)} rows  {df['datetime'].min()} .. {df['datetime'].max()}")

    print("DONE")


if __name__ == "__main__":
    asyncio.run(main())
