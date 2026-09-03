"""Fetch NEXT-WEEK expiry option premium for the exact same strikes the
validated 13-signal backtest uses, so exits can be re-resolved against a
farther-dated (lower theta decay) contract instead of the current-week one.

Expiry dates are resolved via REGISTRY.get_active_expiry() (the registry
function), never hardcoded calendar dates: current-week = nearest expiry
on/after the backtest end date; next-week = nearest expiry strictly after
that (both queries go through the same registry function)."""
import sys
sys.path.insert(0, ".")
from datetime import timedelta
from urllib.parse import quote as _q
import pandas as pd

from data_layer.instrument_registry import REGISTRY
from data_layer.historical_candles import _http_get_json, _parse_candles
import scripts.fvg_backtest as fb

TOKEN_PATH = fb.TOKEN_PATH
OUT_DIR = "data/d1trap_fractal_cache/fvg_nextweek_strikes"
CHUNK_DAYS = 28

STRIKES = [
    (23650, "CE"), (23650, "PE"), (23750, "CE"), (23750, "PE"), (23850, "CE"), (23850, "PE"),
    (23950, "CE"), (23950, "PE"), (24150, "CE"), (24250, "CE"), (24350, "CE"),
]


def fetch_range(key, token, start, end):
    all_rows = []
    cur_end = end
    while cur_end >= start:
        cur_start = max(start, cur_end - timedelta(days=CHUNK_DAYS - 1))
        url = f"https://api.upstox.com/v2/historical-candle/{_q(key, safe='')}/1minute/{cur_end.isoformat()}/{cur_start.isoformat()}"
        r = _http_get_json(url, token)
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
    token = open(TOKEN_PATH).read().strip()
    REGISTRY.load_sync("NIFTY", token)

    current_week_expiry = REGISTRY.get_active_expiry("NIFTY", from_date=fb.BACKTEST_END)
    next_week_expiry = REGISTRY.get_active_expiry("NIFTY", from_date=current_week_expiry + timedelta(days=1))
    print(f"current_week_expiry (via registry) = {current_week_expiry}")
    print(f"next_week_expiry (via registry)    = {next_week_expiry}")

    start = fb.BACKTEST_START - timedelta(days=1)
    end = fb.BACKTEST_END
    for strike, side in STRIKES:
        key = REGISTRY.get_upstox_key("NIFTY", next_week_expiry, strike, side)
        if not key:
            print(f"SKIP {strike}{side}: no upstox key for expiry {next_week_expiry}")
            continue
        df = fetch_range(key, token, start, end)
        fname = f"{OUT_DIR}/{strike}_{side}.parquet"
        df.to_parquet(fname)
        print(f"{strike}{side}: {len(df)} rows  "
              f"{df['datetime'].min() if not df.empty else '-'} .. {df['datetime'].max() if not df.empty else '-'}")
    print("DONE")
