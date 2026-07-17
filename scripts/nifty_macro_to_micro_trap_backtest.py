#!/usr/bin/env python3
"""
scripts/nifty_macro_to_micro_trap_backtest.py

Run the full macro-to-micro trap execution engine on NIFTY spot data with a
parameter + entry-mode sensitivity sweep.

Entry modes:
  close : 1m candle close must break past prior 1m extreme + cross V4 trigger.
  limit : pure limit order at the V4 1/3 retracement trigger (touch entry).
  wick  : 1m wick must breach prior 1m extreme + cross V4 trigger.

Sweep matrix:
  MAX_ADX  = [20.0, 25.0, 30.0, 35.0]
  USE_VWAP = [True, False]
  entry_mode = [close, limit, wick]
"""
from __future__ import annotations

import glob
import os
import sys
from datetime import date, time

import pandas as pd
import pytz

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from strategies.trap_scanner import v4_spot_cascade as v4

IST = pytz.timezone("Asia/Kolkata")
CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")
OUTPUT_DIR = os.path.join(ROOT, "data")

START = date(2026, 6, 1)
END = date(2026, 7, 3)
MULTIPLIERS = [75, 150, 225]

ENTRY_MODES = ["close", "limit", "wick"]
MAX_ADX_VALUES = [20.0, 25.0, 30.0, 35.0]
USE_VWAP_VALUES = [True, False]

RSI_LONG_MIN = 40.0
RSI_SHORT_MAX = 60.0
USE_ADX = True
USE_RSI = True


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


def print_macro_table(macro_df: pd.DataFrame) -> None:
    if macro_df.empty:
        print("No macro structural traps detected.")
        return

    macro_df = macro_df.sort_values("trap_ts").reset_index(drop=True)
    fmt = lambda ts: ts.strftime("%Y-%m-%d %H:%M") if isinstance(ts, pd.Timestamp) else str(ts)[:16]

    header = (
        f"{'Ref Candle Time':<20} {'Breakout Candle Time':<20} {'Trap Confirmed Time':<20} "
        f"{'Trap Type':<12} {'Multiplier':<14} "
        f"{'Range Breakout Line':>18} {'Anchor SL Level':>16} "
        f"{'Peak Trap High/Low':>18} {'Validated Trap Zone':>22}"
    )
    print("\n" + header)
    print("-" * len(header))

    for _, r in macro_df.iterrows():
        peak_str = f"H {r['peak']:>14.2f}" if r['type'] == 'Bull' else f"L {r['peak']:>14.2f}"
        zone_str = f"{r['zone_low']:.2f} - {r['zone_high']:.2f}"
        print(
            f"{fmt(r['ref_ts']):<20} {fmt(r['breakout_ts']):<20} {fmt(r['confirm_ts']):<20} "
            f"{r['type']:<12} {r['multiplier']:<14} "
            f"{r['breakout_line']:>18.2f} {r['anchor_sl']:>16.2f} "
            f"{peak_str:>18} {zone_str:>22}"
        )

    print(f"\n{'Macro Trap Summary':=^60}")
    print(f"Total structural traps: {len(macro_df)}")
    print(f"  Bull traps: {sum(macro_df['type'] == 'Bull')}")
    print(f"  Bear traps: {sum(macro_df['type'] == 'Bear')}")
    for mult in MULTIPLIERS:
        cnt = sum(macro_df["multiplier"] == f"{mult}m")
        print(f"  {mult}m: {cnt}")


def run_sweep(df_1m: pd.DataFrame) -> pd.DataFrame:
    """Run the full parameter × entry-mode sweep and return a results table."""
    rows = []
    total_runs = len(ENTRY_MODES) * len(MAX_ADX_VALUES) * len(USE_VWAP_VALUES)
    run_no = 0

    for entry_mode in ENTRY_MODES:
        for max_adx in MAX_ADX_VALUES:
            for use_vwap in USE_VWAP_VALUES:
                run_no += 1
                print(
                    f"\n[{run_no}/{total_runs}] Running: entry_mode={entry_mode} | "
                    f"MAX_ADX={max_adx} | VWAP={use_vwap} ..."
                )
                _, trades_df = v4.backtest_macro_to_micro(
                    df_1m,
                    multipliers=MULTIPLIERS,
                    lookback=3,
                    max_adx=max_adx,
                    rsi_long_min=RSI_LONG_MIN,
                    rsi_short_max=RSI_SHORT_MAX,
                    use_vwap=use_vwap,
                    use_adx=USE_ADX,
                    use_rsi=USE_RSI,
                    entry_mode=entry_mode,
                )
                s = v4.summarize_macro_to_micro_trades(trades_df)
                rows.append({
                    "entry_mode": entry_mode,
                    "max_adx": max_adx,
                    "use_vwap": use_vwap,
                    "trades": s["total"],
                    "wins": s["wins"],
                    "losses": s["losses"],
                    "win_pct": round(s["win_rate"], 1),
                    "profit_factor": round(s["profit_factor"], 2) if s["profit_factor"] != float("inf") else "inf",
                    "net_pnl": round(s["net_pnl"], 2),
                    "max_dd": round(s["max_dd"], 2),
                    "avg_win": round(s["avg_win"], 2),
                    "avg_loss": round(s["avg_loss"], 2),
                    "rr": round(s["rr"], 2),
                })

    return pd.DataFrame(rows)


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
    print(f"\nSweep matrix: entry_modes={ENTRY_MODES} | MAX_ADX={MAX_ADX_VALUES} | VWAP={USE_VWAP_VALUES}")

    # Run one baseline pass to print the macro trap table
    macro_df, _ = v4.backtest_macro_to_micro(
        df_1m, multipliers=MULTIPLIERS, entry_mode="close"
    )
    print_macro_table(macro_df)

    results = run_sweep(df_1m)

    # Sort by Net P&L desc, then by trade count desc
    results_sorted = results.sort_values(
        ["net_pnl", "trades"], ascending=[False, False]
    ).reset_index(drop=True)

    print("\n" + "=" * 120)
    print("Parameter + Entry-Mode Sweep Results (sorted by Net P&L, then Trades)")
    print("=" * 120)
    print(results_sorted.to_string(index=False))
    print("=" * 120)

    out_results = os.path.join(
        OUTPUT_DIR, f"nifty_macro_to_micro_sweep_{START}_{END}.csv"
    )
    results_sorted.to_csv(out_results, index=False)
    print(f"\nSaved sweep results to {out_results}")


if __name__ == "__main__":
    main()
