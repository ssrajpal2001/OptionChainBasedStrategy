#!/usr/bin/env python3
"""
scripts/nifty_macro_htf_trap_scan.py

Detect macro structural HTF traps on NIFTY spot (June-July 2026) across
75m / 150m / 225m multipliers.

Output columns (per user spec):
  Trap Date/Time | Trap Type (Bull/Bear) | Multiplier (75m/150m/225m) |
  Range Breakout Line | Anchor SL Level | Peak Trap High/Low | Validated Trap Zone

No lower-timeframe entries are executed in this script.
"""
from __future__ import annotations

import glob
import os
import sys
from datetime import date, datetime, time

import pandas as pd
import pytz

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from strategies.trap_scanner import v4_spot_cascade as v4

IST = pytz.timezone("Asia/Kolkata")
CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")
START = date(2026, 6, 1)
END = date(2026, 7, 31)
MULTIPLIERS = [75, 150, 225]


def load_1m_spot(start: date, end: date) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(CACHE_DIR, "spot_NIFTY_1m_*.parquet")))
    frames = []
    for f in files:
        df = pd.read_parquet(f)
        df["datetime"] = pd.to_datetime(df["datetime"])
        if df["datetime"].dt.tz is None:
            df["datetime"] = df["datetime"].dt.tz_localize(
                "Asia/Kolkata", ambiguous="NaT", nonexistent="shift_forward"
            )
        else:
            df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata")
        frames.append(df)

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates("datetime").sort_values("datetime").reset_index(drop=True)
    df = df[(df["datetime"].dt.date >= start) & (df["datetime"].dt.date <= end)]
    df = df[(df["datetime"].dt.time >= time(9, 15)) & (df["datetime"].dt.time <= time(15, 30))]
    return df


def main() -> None:
    print("Loading NIFTY 1m spot data ...")
    df_1m = load_1m_spot(START, END)
    if df_1m.empty:
        print("No NIFTY 1m spot data found in the requested range.")
        return

    print(
        f"Loaded {len(df_1m):,} 1m bars from "
        f"{df_1m['datetime'].dt.date.min()} to {df_1m['datetime'].dt.date.max()}"
    )

    rows = []
    for mult in MULTIPLIERS:
        df_htf = v4._resample_per_day(df_1m, mult)
        if df_htf.empty or len(df_htf) < 5:
            continue
        traps = v4.detect_macro_htf_traps(df_htf, lookback=3)
        for t in traps:
            t["multiplier"] = f"{mult}m"
            rows.append(t)

    if not rows:
        print("No macro structural traps detected.")
        return

    rows.sort(key=lambda x: x["trap_ts"])

    header = (
        f"{'Ref Candle Time':<20} {'Breakout Candle Time':<20} {'Trap Confirmed Time':<20} "
        f"{'Trap Type':<12} {'Multiplier':<14} "
        f"{'Range Breakout Line':>18} {'Anchor SL Level':>16} "
        f"{'Peak Trap High/Low':>18} {'Validated Trap Zone':>22}"
    )
    print("\n" + header)
    print("-" * len(header))
    for r in rows:
        ref_ts = r["ref_ts"]
        bo_ts = r["breakout_ts"]
        conf_ts = r["confirm_ts"]
        fmt = lambda ts: ts.strftime("%Y-%m-%d %H:%M") if isinstance(ts, pd.Timestamp) else str(ts)[:16]
        if r["type"] == "Bull":
            peak_str = f"H {r['peak']:>14.2f}"
        else:
            peak_str = f"L {r['peak']:>14.2f}"
        zone_str = f"{r['zone_low']:.2f} - {r['zone_high']:.2f}"
        print(
            f"{fmt(ref_ts):<20} {fmt(bo_ts):<20} {fmt(conf_ts):<20} "
            f"{r['type']:<12} {r['multiplier']:<14} "
            f"{r['breakout_line']:>18.2f} {r['anchor_sl']:>16.2f} "
            f"{peak_str:>18} {zone_str:>22}"
        )

    print(f"\n{'Summary':=^60}")
    print(f"Total structural traps: {len(rows)}")
    print(f"  Bull traps: {sum(1 for r in rows if r['type'] == 'Bull')}")
    print(f"  Bear traps: {sum(1 for r in rows if r['type'] == 'Bear')}")
    for mult in MULTIPLIERS:
        cnt = sum(1 for r in rows if r["multiplier"] == f"{mult}m")
        print(f"  {mult}m: {cnt}")


if __name__ == "__main__":
    main()
