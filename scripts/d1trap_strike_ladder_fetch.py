"""Fetch real 1-min premium for the Stage-1 strike-ladder optimization: for
every trading day in the month window, compute ATM + 0/1/2/3-ITM CE and PE
strikes, and fetch each unique (strike,side) pair's FULL history across the
whole window -- all under ONE currently-active weekly contract per
underlying (NIFTY 2026-08-04, SENSEX 2026-08-06), both confirmed to have
real trading history back through the whole prior month, so this is a
single consistent expiry with no rollover contamination.

0-ITM = ATM itself, 1/2/3-ITM = ATM -/+ 1/2/3 strike steps (CE subtracts,
PE adds), matching the "which strike actually behaves like a bear-trap"
question -- the current live default (200pt/500pt offset) is ~4 steps ITM
for NIFTY/SENSEX respectively, deliberately NOT included in this ladder so
Stage 1 can judge 0-3 steps ITM on its own merits first.
"""
import sys
sys.path.insert(0, ".")
import asyncio
from datetime import date, timedelta
import pandas as pd
from data_layer.instrument_registry import REGISTRY
from data_layer.historical_candles import fetch_upstox_range_1m

TOKEN = open(r"C:\Users\SERVER\AppData\Local\Temp\claude\e--AlgoSoft-OptionChainBasedStrategy\3f952902-2e64-455f-be1c-fac0a7378cbc\scratchpad\upstox_token.txt").read().strip()
OUT_DIR = "data/d1trap_fractal_cache/strike_ladder"
DAY_MIN, DAY_MAX = date(2026, 6, 29), date(2026, 7, 31)

UNDERLYINGS = {
    "NIFTY": dict(expiry=date(2026, 8, 4), step=50, round_step=100,
                  spot_path="data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet"),
    "SENSEX": dict(expiry=date(2026, 8, 6), step=100, round_step=100,
                   spot_path="data/d1trap_fractal_cache/sensex_1m_fullmonth_spot.parquet"),
}


async def fetch_underlying(name, cfg):
    print(f"\n{'='*90}\n{name}\n{'='*90}")
    REGISTRY.load_sync(name, TOKEN)
    spot = pd.read_parquet(cfg["spot_path"])
    spot["datetime"] = pd.to_datetime(spot["datetime"])
    days = sorted(d for d in spot["datetime"].dt.date.unique() if DAY_MIN <= d <= DAY_MAX)

    strikes = set()
    for d in days:
        day_df = spot[spot["datetime"].dt.date == d]
        if day_df.empty:
            continue
        o = day_df.iloc[0]["open"]
        atm = round(o / cfg["round_step"]) * cfg["round_step"]
        for n in (0, 1, 2, 3):
            strikes.add((int(atm - n * cfg["step"]), "CE"))
            strikes.add((int(atm + n * cfg["step"]), "PE"))

    print(f"{len(days)} trading days -> {len(strikes)} unique (strike,side) pairs to fetch")
    for strike, side in sorted(strikes):
        key = REGISTRY.get_upstox_key(name, cfg["expiry"], strike, side)
        if not key:
            print(f"  SKIP {strike}{side}: no key")
            continue
        rows = await fetch_upstox_range_1m(key, TOKEN, DAY_MIN, DAY_MAX)
        df = pd.DataFrame(rows)
        if df.empty:
            print(f"  {strike}{side}: 0 rows")
            continue
        df = df.rename(columns={"ts": "datetime"})
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)
        fname = f"{OUT_DIR}/{name.lower()}ladder_{strike}_{side}.parquet"
        df.to_parquet(fname)
        print(f"  {strike}{side}: {len(df)} rows  {df['datetime'].min()} .. {df['datetime'].max()}")


async def main():
    import os
    os.makedirs(OUT_DIR, exist_ok=True)
    for name, cfg in UNDERLYINGS.items():
        await fetch_underlying(name, cfg)
    print("\nDONE")


if __name__ == "__main__":
    asyncio.run(main())
