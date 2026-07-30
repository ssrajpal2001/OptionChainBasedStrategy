"""Fetch 1-min option premium history for the Aug-4 2026 NIFTY expiry, strikes 23500-24600,
for the option-contract validation backtest."""
import json
import os
import time
from datetime import date, timedelta
from urllib.parse import quote

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(ROOT, "data", "d1trap_fractal_cache")


def _get(url, token):
    from curl_cffi import requests as cc
    r = cc.get(url, headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
               impersonate="chrome131", timeout=30)
    return r.json()


def fetch_option_1m(instrument_key, token, start, end):
    key = quote(instrument_key, safe="")
    url = f"https://api.upstox.com/v2/historical-candle/{key}/1minute/{end.isoformat()}/{start.isoformat()}"
    data = _get(url, token)
    if data.get("status") != "success":
        print(f"  WARN {instrument_key}: {data.get('errors')}")
        return pd.DataFrame()
    candles = (data.get("data") or {}).get("candles") or []
    rows = [{"datetime": pd.Timestamp(c[0]), "open": c[1], "high": c[2], "low": c[3], "close": c[4]}
            for c in reversed(candles)]
    df = pd.DataFrame(rows)
    if not df.empty:
        df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_convert("Asia/Kolkata")
    return df


def main():
    token = open(r"C:\Users\SERVER\AppData\Local\Temp\claude\e--AlgoSoft-OptionChainBasedStrategy\3f952902-2e64-455f-be1c-fac0a7378cbc\scratchpad\upstox_token.txt").read().strip()
    keys = json.load(open(os.path.join(CACHE_DIR, "aug4_keys.json")))
    end = date(2026, 7, 29)
    start = date(2026, 6, 29)

    out_dir = os.path.join(CACHE_DIR, "aug4_options")
    os.makedirs(out_dir, exist_ok=True)

    for name, ikey in keys.items():
        path = os.path.join(out_dir, f"{name}.parquet")
        if os.path.exists(path):
            print(f"cache hit {name}")
            continue
        for attempt in range(3):
            try:
                df = fetch_option_1m(ikey, token, start, end)
                df.to_parquet(path)
                print(f"{name}: {len(df)} rows")
                break
            except Exception as exc:
                print(f"  {name} attempt {attempt+1} failed: {exc}")
                time.sleep(2)
        else:
            print(f"  {name}: GIVING UP after 3 attempts")
        time.sleep(0.25)


if __name__ == "__main__":
    main()
