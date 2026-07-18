#!/usr/bin/env python3
"""
scripts/fetch_upstox_historical.py
===================================
Fetch NIFTY spot and monthly-option 1-minute historical candles from the
Upstox API v2 and save them in the standard cache parquet format.

Usage:
    export UPSTOX_ACCESS_TOKEN="your_token"
    python scripts/fetch_upstox_historical.py \
        --start 2026-07-01 --end 2026-07-30 --expiry 2026-07-30

Required Upstox access scopes:
    - Historical candle data (spot + F&O)
    - Option chain read

The script chunks requests by month to stay within Upstox limits and sleeps
between requests to avoid rate-limit hits.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta, time as dt_time
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import pytz
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from strategies.trap_scanner.monthly_option_cascade import (
    select_execution_strikes,
    select_tracking_strikes,
)

IST = pytz.timezone("Asia/Kolkata")
BASE_URL = "https://api.upstox.com/v2"
CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")

INDEX_INSTRUMENT_KEYS = {
    "NIFTY": "NSE_INDEX|Nifty 50",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
    "SENSEX": "BSE_INDEX|SENSEX",
    "FINNIFTY": "NSE_INDEX|Nifty Fin Services",
    "MIDCPNIFTY": "NSE_INDEX|NIFTY MIDCAP SELECT",
}

DEFAULT_RATE_LIMIT_SLEEP = 0.25  # seconds between requests
CHUNK_DAYS = 30                  # max days per historical-candles request


def _headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def _date_chunks(start: date, end: date, chunk_days: int = CHUNK_DAYS) -> List[Tuple[date, date]]:
    """Split a date range into (from, to) chunks of at most chunk_days."""
    chunks = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def _request_json(url: str, token: str, retries: int = 3) -> Optional[Dict]:
    """GET with rate-limit retry and short back-off."""
    headers = _headers(token)
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 429:
                wait = 2 ** attempt + 1
                print(f"  Rate limited. Sleeping {wait}s ...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            print(f"  Request error ({attempt + 1}/{retries}): {e}")
            time.sleep(1)
    return None


def fetch_historical_candles(
    token: str,
    instrument_key: str,
    interval: str,
    start: date,
    end: date,
) -> Optional[pd.DataFrame]:
    """
    Fetch 1m candles for a single instrument across the date range.
    Upstox endpoint: /v2/historical-candles/{instrument_key}/{interval}/{to}/{from}
    Returns DataFrame with columns: timestamp, open, high, low, close, volume, open_interest
    """
    frames: List[pd.DataFrame] = []
    for from_d, to_d in _date_chunks(start, end):
        url = (
            f"{BASE_URL}/historical-candles/{instrument_key}/{interval}"
            f"/{to_d.isoformat()}/{from_d.isoformat()}"
        )
        print(f"  Fetching {instrument_key} {interval} {from_d} -> {to_d}")
        data = _request_json(url, token)
        if data is None:
            print(f"  Failed to fetch candles for {instrument_key}")
            return None
        candles = data.get("data", {}).get("candles", [])
        if not candles:
            print(f"  No candles returned for {instrument_key} {from_d} -> {to_d}")
            time.sleep(DEFAULT_RATE_LIMIT_SLEEP)
            continue
        df = pd.DataFrame(
            candles,
            columns=["timestamp", "open", "high", "low", "close", "volume", "open_interest"],
        )
        for col in ["open", "high", "low", "close", "volume", "open_interest"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        frames.append(df)
        time.sleep(DEFAULT_RATE_LIMIT_SLEEP)

    if not frames:
        return None
    return pd.concat(frames, ignore_index=True).drop_duplicates("timestamp").sort_values("timestamp")


def fetch_spot(
    token: str,
    index_name: str,
    start: date,
    end: date,
) -> Optional[pd.DataFrame]:
    """Fetch NIFTY spot 1m candles and return standard-format DataFrame."""
    instrument_key = INDEX_INSTRUMENT_KEYS.get(index_name.upper())
    if instrument_key is None:
        raise ValueError(f"Unknown index {index_name}. Supported: {list(INDEX_INSTRUMENT_KEYS)}")

    print(f"\n[SPOT] Fetching {index_name} ({instrument_key}) 1m candles {start} -> {end}")
    df = fetch_historical_candles(token, instrument_key, "1minute", start, end)
    if df is None or df.empty:
        return None

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    if df["timestamp"].dt.tz is None:
        df["timestamp"] = df["timestamp"].dt.tz_localize(
            IST, ambiguous="NaT", nonexistent="shift_forward"
        )
    else:
        df["timestamp"] = df["timestamp"].dt.tz_convert(IST)

    # Keep only market hours (09:15 - 15:30) to match existing cache format
    df = df[
        (df["timestamp"].dt.time >= dt_time(9, 15))
        & (df["timestamp"].dt.time <= dt_time(15, 30))
    ].copy()

    df = df[["timestamp", "open", "high", "low", "close", "volume"]].reset_index(drop=True)
    return df


def fetch_option_chain(
    token: str,
    index_name: str,
    expiry_date: date,
) -> Optional[Dict]:
    """Fetch option chain for the index and expiry."""
    instrument_key = INDEX_INSTRUMENT_KEYS.get(index_name.upper())
    if instrument_key is None:
        raise ValueError(f"Unknown index {index_name}")
    url = (
        f"{BASE_URL}/option/chain?"
        f"instrument_key={instrument_key}&expiry_date={expiry_date.isoformat()}"
    )
    print(f"\n[OPTION CHAIN] Fetching {index_name} expiry {expiry_date}")
    data = _request_json(url, token)
    if data is None:
        return None
    return data.get("data")


def build_instrument_key_map(
    option_chain: List[Dict],
    expiry_date: date,
) -> Dict[Tuple[int, str], str]:
    """
    Map (strike, opt_type) -> instrument_key from the option chain response.
    opt_type is 'CE' or 'PE'.
    """
    mapping: Dict[Tuple[int, str], str] = {}
    for item in option_chain:
        strike = int(item.get("strike_price", 0))
        for side in ("CE", "PE"):
            side_data = item.get(side)
            if side_data and side_data.get("instrument_key"):
                mapping[(strike, side)] = side_data["instrument_key"]
    return mapping


def _round_strike(spot: float, step: float = 50) -> int:
    return int(round(spot / step) * step)


def collect_required_strikes(
    df_spot: pd.DataFrame,
    step: int = 50,
) -> Set[Tuple[int, str]]:
    """
    For each trading day, compute the daily ATM and the required tracking and
    execution strikes. Return the set of all (strike, opt_type) pairs needed.
    """
    required: Set[Tuple[int, str]] = set()
    for day, day_df in df_spot.groupby(df_spot["timestamp"].dt.date):
        if day_df.empty:
            continue
        spot_open = float(day_df.iloc[0]["open"])
        ce_track, pe_track = select_tracking_strikes(spot_open, step=step)
        ce_exec, pe_exec = select_execution_strikes(spot_open, step=step)
        required.add((ce_track, "CE"))
        required.add((pe_track, "PE"))
        required.add((ce_exec, "CE"))
        required.add((pe_exec, "PE"))
    return required


def fetch_monthly_options(
    token: str,
    index_name: str,
    expiry_date: date,
    df_spot: pd.DataFrame,
    start: date,
    end: date,
) -> Optional[pd.DataFrame]:
    """Fetch all required monthly option 1m candles and return a combined DataFrame."""
    option_chain = fetch_option_chain(token, index_name, expiry_date)
    if option_chain is None:
        print("[OPTION CHAIN] Failed to fetch option chain.")
        return None

    instrument_map = build_instrument_key_map(option_chain, expiry_date)
    required_strikes = collect_required_strikes(df_spot)

    # Add a safety margin around the spot range to handle edge cases
    spot_min = int(df_spot["low"].min())
    spot_max = int(df_spot["high"].max())
    atm_low = _round_strike(spot_min, 50) - 300
    atm_high = _round_strike(spot_max, 50) + 300
    for strike in range(atm_low, atm_high + 1, 50):
        for opt_type in ("CE", "PE"):
            required_strikes.add((strike, opt_type))

    print(f"\n[OPTIONS] Total unique strike/type pairs to fetch: {len(required_strikes)}")

    records: List[pd.DataFrame] = []
    for (strike, opt_type), instrument_key in instrument_map.items():
        if (strike, opt_type) not in required_strikes:
            continue
        print(f"\n[OPTIONS] {strike}{opt_type} -> {instrument_key}")
        df = fetch_historical_candles(token, instrument_key, "1minute", start, end)
        if df is None or df.empty:
            print(f"  Skipping {strike}{opt_type} (no data)")
            continue
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        if df["timestamp"].dt.tz is None:
            df["timestamp"] = df["timestamp"].dt.tz_localize(
                IST, ambiguous="NaT", nonexistent="shift_forward"
            )
        else:
            df["timestamp"] = df["timestamp"].dt.tz_convert(IST)
        df = df[
            (df["timestamp"].dt.time >= dt_time(9, 15))
            & (df["timestamp"].dt.time <= dt_time(15, 30))
        ].copy()
        df["strike"] = strike
        df["opt_type"] = opt_type
        records.append(df[["timestamp", "strike", "opt_type", "open", "high", "low", "close", "volume"]])

    if not records:
        print("[OPTIONS] No option data fetched.")
        return None
    return pd.concat(records, ignore_index=True).drop_duplicates(
        ["timestamp", "strike", "opt_type"]
    ).sort_values(["timestamp", "strike", "opt_type"]).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Upstox historical NIFTY spot + monthly options")
    parser.add_argument("--start", type=date.fromisoformat, required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", type=date.fromisoformat, required=True, help="YYYY-MM-DD")
    parser.add_argument("--expiry", type=date.fromisoformat, required=True, help="Monthly expiry YYYY-MM-DD")
    parser.add_argument("--index", type=str, default="NIFTY", help="Index name")
    parser.add_argument("--spot-only", action="store_true", help="Fetch only spot data")
    parser.add_argument("--options-only", action="store_true", help="Fetch only option data (spot parquet must exist)")
    parser.add_argument("--spot-file", type=str, default=None, help="Existing spot parquet file to use with --options-only")
    args = parser.parse_args()

    token = os.environ.get("UPSTOX_ACCESS_TOKEN")
    if not token:
        print("ERROR: UPSTOX_ACCESS_TOKEN environment variable is not set.")
        print("Set it with: export UPSTOX_ACCESS_TOKEN='your_token'")
        sys.exit(1)

    os.makedirs(CACHE_DIR, exist_ok=True)

    # Spot
    df_spot = None
    if not args.options_only:
        df_spot = fetch_spot(token, args.index, args.start, args.end)
        if df_spot is None or df_spot.empty:
            print("[ERROR] No spot data fetched. Aborting.")
            sys.exit(1)
        out_spot = os.path.join(
            CACHE_DIR, f"spot_{args.index.upper()}_1m_{args.start}_{args.end}.parquet"
        )
        df_spot.to_parquet(out_spot, index=False)
        print(f"[SAVED] Spot data -> {out_spot} ({len(df_spot):,} rows)")
    else:
        spot_file = args.spot_file or os.path.join(
            CACHE_DIR, f"spot_{args.index.upper()}_1m_{args.start}_{args.end}.parquet"
        )
        if not os.path.exists(spot_file):
            print(f"[ERROR] Spot file not found: {spot_file}")
            sys.exit(1)
        df_spot = pd.read_parquet(spot_file)
        df_spot["timestamp"] = pd.to_datetime(df_spot["timestamp"])
        print(f"[LOADED] Spot data from {spot_file} ({len(df_spot):,} rows)")

    # Options
    if not args.spot_only:
        df_opt = fetch_monthly_options(token, args.index, args.expiry, df_spot, args.start, args.end)
        if df_opt is None or df_opt.empty:
            print("[ERROR] No option data fetched. Aborting.")
            sys.exit(1)
        out_opt = os.path.join(
            CACHE_DIR, f"opt_{args.index.upper()}_monthly_{args.expiry.isoformat()}_1m.parquet"
        )
        df_opt.to_parquet(out_opt, index=False)
        print(f"[SAVED] Option data -> {out_opt} ({len(df_opt):,} rows)")

    print("\nDone.")


if __name__ == "__main__":
    main()
