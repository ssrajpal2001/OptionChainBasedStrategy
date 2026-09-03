"""
scripts/d1trap_fetch_data.py — fetch + cache NIFTY D1 and 1-min spot history
from Upstox for the D1-trap fractal backtest (scripts/d1trap_fractal_backtest.py).

Upstox limits: /day endpoint accepts wide ranges (tested: 2yr in one call).
/1minute endpoint caps at ~30 calendar days per call -> chunked.

Usage:
    python scripts/d1trap_fetch_data.py --token-file <path> --start 2025-07-01 --end 2026-07-29
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import date, timedelta
from urllib.parse import quote

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(ROOT, "data", "d1trap_fractal_cache")
NIFTY_KEY = "NSE_INDEX|Nifty 50"


def _get(url: str, token: str) -> dict:
    from curl_cffi import requests as cc
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    r = cc.get(url, headers=headers, impersonate="chrome131", timeout=30)
    return r.json()


def fetch_day_bars(token: str, start: date, end: date) -> pd.DataFrame:
    key = quote(NIFTY_KEY, safe="")
    url = f"https://api.upstox.com/v2/historical-candle/{key}/day/{end.isoformat()}/{start.isoformat()}"
    data = _get(url, token)
    candles = (data.get("data") or {}).get("candles") or []
    rows = [
        {"datetime": pd.Timestamp(c[0]), "open": c[1], "high": c[2], "low": c[3], "close": c[4]}
        for c in reversed(candles)
    ]
    df = pd.DataFrame(rows)
    if not df.empty:
        df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_convert("Asia/Kolkata")
    return df


def fetch_1m_chunk(token: str, start: date, end: date) -> pd.DataFrame:
    key = quote(NIFTY_KEY, safe="")
    url = f"https://api.upstox.com/v2/historical-candle/{key}/1minute/{end.isoformat()}/{start.isoformat()}"
    data = _get(url, token)
    if data.get("status") != "success":
        print(f"  WARN chunk {start}->{end}: {data.get('errors')}")
        return pd.DataFrame()
    candles = (data.get("data") or {}).get("candles") or []
    rows = [
        {"datetime": pd.Timestamp(c[0]), "open": c[1], "high": c[2], "low": c[3],
         "close": c[4], "volume": c[5]}
        for c in reversed(candles)
    ]
    df = pd.DataFrame(rows)
    if not df.empty:
        df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_convert("Asia/Kolkata")
    return df


def fetch_1m_range(token: str, start: date, end: date) -> pd.DataFrame:
    frames = []
    cur_end = end
    while cur_end >= start:
        cur_start = max(start, cur_end - timedelta(days=29))
        print(f"  1m chunk {cur_start} -> {cur_end} ...", flush=True)
        df = fetch_1m_chunk(token, cur_start, cur_end)
        if not df.empty:
            frames.append(df)
        time.sleep(0.3)
        cur_end = cur_start - timedelta(days=1)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates("datetime").sort_values("datetime").reset_index(drop=True)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--token-file", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    args = ap.parse_args()

    token = open(args.token_file).read().strip()
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    os.makedirs(CACHE_DIR, exist_ok=True)

    d1_path = os.path.join(CACHE_DIR, f"nifty_d1_{start}_{end}.parquet")
    if not os.path.exists(d1_path):
        print("Fetching D1 bars...", flush=True)
        d1 = fetch_day_bars(token, start - timedelta(days=30), end)
        d1.to_parquet(d1_path)
        print(f"  saved {len(d1)} D1 bars -> {d1_path}")
    else:
        print(f"D1 cache hit: {d1_path}")

    m1_path = os.path.join(CACHE_DIR, f"nifty_1m_{start}_{end}.parquet")
    if not os.path.exists(m1_path):
        print("Fetching 1-min bars (chunked)...", flush=True)
        m1 = fetch_1m_range(token, start, end)
        m1.to_parquet(m1_path)
        print(f"  saved {len(m1)} 1m bars -> {m1_path}")
    else:
        print(f"1m cache hit: {m1_path}")


if __name__ == "__main__":
    main()
