"""Fetch 14-day 1-min history for SENSEX 77500 CE / 78500 PE (+ spot) so
the corrected-boundary backtest has a real warmup window, not just today's
data. Uses instrument_registry (live BSE master, since SENSEX's active
expiry doesn't match the static config's hardcoded weekday)."""
import sys
sys.path.insert(0, ".")
import asyncio
from datetime import date, timedelta
import pandas as pd
from data_layer.instrument_registry import REGISTRY
from data_layer.historical_candles import fetch_upstox_range_1m, fetch_upstox_intraday_1m

TOKEN = open("C:/Users/SERVER/AppData/Local/Temp/claude/e--AlgoSoft-OptionChainBasedStrategy/3f952902-2e64-455f-be1c-fac0a7378cbc/scratchpad/upstox_token.txt").read().strip()
OUT_DIR = "data/d1trap_fractal_cache"
DAY = date(2026, 7, 31)
START = DAY - timedelta(days=14)


async def main():
    REGISTRY.load_sync("SENSEX", TOKEN)
    expiry = REGISTRY.get_active_expiry("SENSEX", from_date=DAY)
    print("active expiry:", expiry)

    for strike, side, fname in [(77500, "CE", "sensex_77500_CE_month.parquet"),
                                  (78500, "PE", "sensex_78500_PE_month.parquet")]:
        key = REGISTRY.get_upstox_key("SENSEX", expiry, strike, side)
        print(strike, side, key)
        rows = await fetch_upstox_range_1m(key, TOKEN, START, DAY)
        all_rows = rows
        df = pd.DataFrame(all_rows)
        df = df.rename(columns={"ts": "datetime"})
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)
        print(f"  {strike}{side}: {len(df)} rows, {df['datetime'].min()} .. {df['datetime'].max()}")
        df.to_parquet(f"{OUT_DIR}/{fname}")

    # spot index history
    spot_key = "BSE_INDEX|SENSEX"
    rows = await fetch_upstox_range_1m(spot_key, TOKEN, START, DAY)
    df = pd.DataFrame(rows)
    df = df.rename(columns={"ts": "datetime"})
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)
    print(f"  spot: {len(df)} rows, {df['datetime'].min()} .. {df['datetime'].max()}")
    df.to_parquet(f"{OUT_DIR}/sensex_1m_month_backtest.parquet")


if __name__ == "__main__":
    asyncio.run(main())
