"""
Fetch 1-min history for every unique ATM-derived strike needed for a true
daily-ATM-rolling 1-month backtest (NIFTY ATM+/-200, SENSEX ATM+/-500,
2026-06-29..07-31 trading sessions), each with a 14-calendar-day warmup
buffer before its first day of use. Upstox's 1-minute range endpoint caps
out around ~30 calendar days per call, so each strike's needed window is
split into <=28-day chunks and concatenated.
"""
import sys
sys.path.insert(0, ".")
from datetime import date, timedelta
from urllib.parse import quote as _q
import pandas as pd
from data_layer.instrument_registry import REGISTRY
from data_layer.historical_candles import _http_get_json, _parse_candles

TOKEN = open("C:/Users/SERVER/AppData/Local/Temp/claude/e--AlgoSoft-OptionChainBasedStrategy/3f952902-2e64-455f-be1c-fac0a7378cbc/scratchpad/upstox_token.txt").read().strip()
OUT_DIR = "data/d1trap_fractal_cache/month_roll"
CHUNK_DAYS = 28


def compute_ranges(spot_path, offset, round_step, day_min, day_max):
    spot = pd.read_parquet(spot_path)
    spot["datetime"] = pd.to_datetime(spot["datetime"])
    spot = spot[(spot["datetime"].dt.date >= day_min) & (spot["datetime"].dt.date <= day_max)]
    strike_days = {}
    for day, g in spot.groupby(spot["datetime"].dt.date):
        o = g.iloc[0]["open"]
        atm = round(o / round_step) * round_step
        ce, pe = atm - offset, atm + offset
        strike_days.setdefault((ce, "CE"), set()).add(day)
        strike_days.setdefault((pe, "PE"), set()).add(day)
    ranges = {}
    for k, days in strike_days.items():
        ranges[k] = (min(days) - timedelta(days=14), max(days))
    return ranges


def fetch_range(key, start, end):
    """Fetch [start,end] in <=CHUNK_DAYS windows, oldest-first concatenated."""
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


def run_underlying(underlying, spot_path, offset, round_step, fname_prefix, bse=False):
    REGISTRY.load_sync(underlying, TOKEN)
    day_min, day_max = date(2026, 6, 29), date(2026, 7, 31)
    ranges = compute_ranges(spot_path, offset, round_step, day_min, day_max)
    print(f"\n{underlying}: {len(ranges)} unique strikes needed")
    manifest = []
    for (strike, side), (start, end) in sorted(ranges.items()):
        expiry = REGISTRY.get_active_expiry(underlying, from_date=end)
        key = REGISTRY.get_upstox_key(underlying, expiry, strike, side)
        if not key:
            print(f"  SKIP {strike}{side}: no upstox key (expiry={expiry})")
            continue
        df = fetch_range(key, start, end)
        fname = f"{OUT_DIR}/{fname_prefix}_{strike}_{side}.parquet"
        df.to_parquet(fname)
        print(f"  {strike}{side}: {len(df)} rows  {df['datetime'].min() if not df.empty else '-'} .. {df['datetime'].max() if not df.empty else '-'}")
        manifest.append((strike, side, fname))
    return manifest


if __name__ == "__main__":
    import os
    os.makedirs(OUT_DIR, exist_ok=True)
    run_underlying("NIFTY", "data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet", 200, 100, "nifty")
    run_underlying("SENSEX", "data/d1trap_fractal_cache/sensex_1m_fullmonth_spot.parquet", 500, 100, "sensex")
    print("\nDONE")
