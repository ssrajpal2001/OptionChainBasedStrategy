"""Fetch real 1-min option premium for the exact strikes scripts/fvg_backtest.py
traded, so the backtest's P&L can be computed from actual option prices
instead of the illustrative 0.5-delta spot approximation."""
import sys
sys.path.insert(0, ".")
from datetime import date, timedelta
from urllib.parse import quote as _q
import pandas as pd
from data_layer.instrument_registry import REGISTRY
from data_layer.historical_candles import _http_get_json, _parse_candles

TOKEN = open("C:/Users/SERVER/AppData/Local/Temp/claude/e--AlgoSoft-OptionChainBasedStrategy/3f952902-2e64-455f-be1c-fac0a7378cbc/scratchpad/upstox_token.txt").read().strip()
OUT_DIR = "data/d1trap_fractal_cache/fvg_trade_strikes"
CHUNK_DAYS = 28
START, END = date(2026, 6, 29), date(2026, 7, 31)

STRIKES = [
    (23700, "CE"), (23800, "CE"), (23800, "PE"), (23900, "CE"), (23900, "PE"),
    (24000, "CE"), (24000, "PE"), (24100, "CE"), (24100, "PE"), (24200, "CE"),
    (24200, "PE"), (24300, "CE"), (24300, "PE"), (24400, "PE"), (24500, "PE"),
    (24600, "PE"),
]


def fetch_range(key, start, end):
    all_rows = []
    cur_end = end
    while cur_end >= start:
        cur_start = max(start, cur_end - timedelta(days=CHUNK_DAYS - 1))
        url = f"https://api.upstox.com/v2/historical-candle/{_q(key, safe='')}/1minute/{cur_end.isoformat()}/{cur_start.isoformat()}"
        r = _http_get_json(url, TOKEN)
        rows = _parse_candles(r)
        if not rows and "errors" in r:
            print(f"    WARN {key} [{cur_start}..{cur_end}]: {r['errors']}")
        all_rows.extend(rows)
        cur_end = cur_start - timedelta(days=1)
    df = pd.DataFrame(all_rows)
    if df.empty:
        return df
    df = df.rename(columns={"ts": "datetime"})
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)


if __name__ == "__main__":
    import os
    os.makedirs(OUT_DIR, exist_ok=True)
    REGISTRY.load_sync("NIFTY", TOKEN)
    expiry = REGISTRY.get_active_expiry("NIFTY", from_date=END)
    print("active expiry:", expiry)
    for strike, side in STRIKES:
        key = REGISTRY.get_upstox_key("NIFTY", expiry, strike, side)
        if not key:
            print(f"SKIP {strike}{side}: no upstox key")
            continue
        df = fetch_range(key, START, END)
        fname = f"{OUT_DIR}/{strike}_{side}.parquet"
        df.to_parquet(fname)
        print(f"{strike}{side}: {len(df)} rows  "
              f"{df['datetime'].min() if not df.empty else '-'} .. {df['datetime'].max() if not df.empty else '-'}")
    print("DONE")
