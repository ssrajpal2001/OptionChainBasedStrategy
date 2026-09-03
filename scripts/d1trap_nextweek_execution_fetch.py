"""Fetch NEXT-WEEK expiry (2026-08-11) premium for NIFTY ATM and 1-ITM CE/PE
strikes, for every trading day 07-23..07-31 -- the EXECUTION contract for
scripts/d1trap_nextweek_execution_test.py: zones are scanned on THIS-WEEK's
contract (08-04, already cached), but the actual trade is priced/entered/
exited on this 08-11 contract instead."""
import sys
sys.path.insert(0, ".")
import asyncio
from datetime import date
import pandas as pd
from data_layer.instrument_registry import REGISTRY
from data_layer.historical_candles import fetch_upstox_range_1m

TOKEN = open(r"C:\Users\SERVER\AppData\Local\Temp\claude\e--AlgoSoft-OptionChainBasedStrategy\3f952902-2e64-455f-be1c-fac0a7378cbc\scratchpad\upstox_token.txt").read().strip()
OUT_DIR = "data/d1trap_fractal_cache/nextweek_exec"
EXPIRY = date(2026, 8, 11)
FETCH_START, FETCH_END = date(2026, 7, 20), date(2026, 7, 31)
WINDOW_DAYS = [date(2026,7,23), date(2026,7,24), date(2026,7,27), date(2026,7,28),
               date(2026,7,29), date(2026,7,30), date(2026,7,31)]


async def main():
    import os
    os.makedirs(OUT_DIR, exist_ok=True)
    REGISTRY.load_sync("NIFTY", TOKEN)

    spot = pd.read_parquet("data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet")
    spot["datetime"] = pd.to_datetime(spot["datetime"])

    strikes = set()
    for d in WINDOW_DAYS:
        day_df = spot[spot["datetime"].dt.date == d]
        if day_df.empty:
            continue
        o = day_df.iloc[0]["open"]
        atm = round(o / 100) * 100
        strikes.add((int(atm), "CE")); strikes.add((int(atm - 50), "CE"))
        strikes.add((int(atm), "PE")); strikes.add((int(atm + 50), "PE"))

    print(f"{len(strikes)} unique (strike,side) pairs needed, expiry={EXPIRY}")
    for strike, side in sorted(strikes):
        key = REGISTRY.get_upstox_key("NIFTY", EXPIRY, strike, side)
        rows = await fetch_upstox_range_1m(key, TOKEN, FETCH_START, FETCH_END)
        df = pd.DataFrame(rows)
        if df.empty:
            print(f"  {strike}{side}: 0 rows")
            continue
        df = df.rename(columns={"ts": "datetime"})
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)
        df.to_parquet(f"{OUT_DIR}/nextweek_{strike}_{side}.parquet")
        print(f"  {strike}{side}: {len(df)} rows  {df['datetime'].min()} .. {df['datetime'].max()}")

    print("DONE")


if __name__ == "__main__":
    asyncio.run(main())
