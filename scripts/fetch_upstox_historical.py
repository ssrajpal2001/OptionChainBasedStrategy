#!/usr/bin/env python3
"""
scripts/fetch_upstox_historical.py

Fetch 1-minute NIFTY 50 spot historical candles from the Upstox API and save
as a single parquet file in the project's cache directory.

Security: the Upstox access token is read from the UPSTOX_ACCESS_TOKEN
environment variable. No credentials are hardcoded.

Usage:
    export UPSTOX_ACCESS_TOKEN="your_token_here"
    python scripts/fetch_upstox_historical.py

Output:
    data/nse_option_cache/spot_NIFTY_1m_historical_1year.parquet
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import List, Optional

import pandas as pd
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")
OUTPUT_FILE = os.path.join(CACHE_DIR, "spot_NIFTY_1m_historical_1year.parquet")

INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
INTERVAL = "1minute"
BASE_URL = "https://api.upstox.com/v2/historical-candle"

START_DATE = date(2025, 7, 1)
END_DATE = date(2026, 7, 3)
CHUNK_DAYS = 30
RATE_LIMIT_DELAY_SECONDS = 0.5


def get_access_token() -> str:
    token = os.environ.get("UPSTOX_ACCESS_TOKEN")
    if not token:
        raise RuntimeError(
            "UPSTOX_ACCESS_TOKEN environment variable is not set. "
            "Please set it before running this script."
        )
    return token


def fetch_chunk(
    instrument_key: str,
    interval: str,
    from_date: date,
    to_date: date,
    access_token: str,
) -> Optional[pd.DataFrame]:
    """Fetch one historical candle chunk from Upstox."""
    url = (
        f"{BASE_URL}/{instrument_key}/{interval}/"
        f"{to_date.isoformat()}/{from_date.isoformat()}"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    print(f"  Fetching {from_date} -> {to_date} ...")
    try:
        resp = requests.get(url, headers=headers, timeout=30)
    except requests.RequestException as e:
        print(f"    Network error: {e}")
        return None

    if resp.status_code != 200:
        print(f"    HTTP {resp.status_code}: {resp.text[:200]}")
        return None

    data = resp.json()
    candles = data.get("data", {}).get("candles")
    if not candles:
        print(f"    No candles returned for this chunk.")
        return None

    df = pd.DataFrame(candles)
    # Upstox returns [timestamp, open, high, low, close, volume, oi]
    df = df.iloc[:, :6]
    df.columns = ["timestamp", "open", "high", "low", "close", "volume"]
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna()
    return df


def generate_date_chunks(start: date, end: date, chunk_days: int) -> List[tuple]:
    chunks = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def main() -> None:
    access_token = get_access_token()
    print(f"Fetching 1m NIFTY 50 spot data from {START_DATE} to {END_DATE}")
    print(f"Output: {OUTPUT_FILE}")

    os.makedirs(CACHE_DIR, exist_ok=True)

    chunks = generate_date_chunks(START_DATE, END_DATE, CHUNK_DAYS)
    frames: List[pd.DataFrame] = []

    for i, (from_date, to_date) in enumerate(chunks, 1):
        print(f"\nChunk {i}/{len(chunks)}")
        df = fetch_chunk(INSTRUMENT_KEY, INTERVAL, from_date, to_date, access_token)
        if df is not None and not df.empty:
            print(f"    Got {len(df):,} rows")
            frames.append(df)
        if i < len(chunks):
            time.sleep(RATE_LIMIT_DELAY_SECONDS)

    if not frames:
        print("No data fetched. Exiting.")
        sys.exit(1)

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)
    print(f"\nCombined unique rows: {len(combined):,}")
    print(f"Date range: {combined['timestamp'].min()} to {combined['timestamp'].max()}")

    combined.to_parquet(OUTPUT_FILE, index=False)
    print(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
