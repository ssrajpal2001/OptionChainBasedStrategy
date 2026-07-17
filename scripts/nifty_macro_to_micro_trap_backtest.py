#!/usr/bin/env python3
"""
scripts/nifty_macro_to_micro_trap_backtest.py

Production-grade macro-to-micro trap backtest on NIFTY spot data.

Defaults to a 1-year window (2025-07-01 to 2026-07-03) and runs the top
performing configuration in pure price action mode:
  - ADX/RSI/VWAP indicators disabled (use_filters=False)
  - Zone re-entry required (require_zone_reentry=True)
  - 15m/5m structural rejection gate required (require_mtf_ltf_rejection=True)
  - Entry mode: close (1m candle close past prior 1m extreme + V4 1/3 trigger)

Outputs:
  1. Macro structural trap summary.
  2. Chronological trade execution log (one row per trade).
  3. Aggregate performance matrix.
  4. CSV files with the raw data.
"""
from __future__ import annotations

import argparse
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

DEFAULT_START = date(2025, 7, 1)
DEFAULT_END = date(2026, 7, 3)
MULTIPLIERS = [75, 150, 225]
ENTRY_MODE = "close"


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


def print_macro_summary(macro_df: pd.DataFrame) -> None:
    if macro_df.empty:
        print("No macro structural traps detected.")
        return

    print(f"\n{'Macro Structural Trap Summary':=^60}")
    print(f"Total structural traps: {len(macro_df)}")
    print(f"  Bull traps (short setups): {sum(macro_df['type'] == 'Bull')}")
    print(f"  Bear traps (long setups):  {sum(macro_df['type'] == 'Bear')}")
    for mult in MULTIPLIERS:
        cnt = sum(macro_df["multiplier"] == f"{mult}m")
        print(f"  {mult}m: {cnt}")

    print("\nFirst 5 macro traps (chronological):")
    fmt = lambda ts: ts.strftime("%Y-%m-%d %H:%M") if isinstance(ts, pd.Timestamp) else str(ts)[:16]
    cols = ["ref_ts", "breakout_ts", "confirm_ts", "type", "multiplier", "breakout_line", "anchor_sl", "peak"]
    head = macro_df.sort_values("trap_ts").head(5)[cols]
    for _, r in head.iterrows():
        print(
            f"  {fmt(r['ref_ts'])} -> {fmt(r['breakout_ts'])} -> {fmt(r['confirm_ts'])} | "
            f"{r['type']:<4} {r['multiplier']:<5} | zone {r['breakout_line']:.2f} / anchor {r['anchor_sl']:.2f} / peak {r['peak']:.2f}"
        )


def print_trade_log(trades_df: pd.DataFrame) -> None:
    if trades_df.empty:
        print("\nNo trades generated.")
        return

    trades_df = trades_df.sort_values("entry_ts").reset_index(drop=True)
    print("\n" + "=" * 110)
    print("Chronological Trade Execution Log — Close Entry Mode")
    print("=" * 110)
    header = (
        f"{'#':<4} {'Execution Time':<20} {'Direction':<8} {'Macro TF':<8} "
        f"{'Entry Price':>12} {'SL':>12} {'Target':>12} {'Exit Time':<20} "
        f"{'Outcome':<10} {'P&L (Rs.)':>12}"
    )
    print(header)
    print("-" * 110)

    for i, r in trades_df.iterrows():
        direction = "LONG" if r["direction"] == "LONG" else "SHORT"
        outcome = r["exit_reason"]
        print(
            f"{i+1:<4} "
            f"{r['entry_ts'].strftime('%Y-%m-%d %H:%M'):<20} "
            f"{direction:<8} "
            f"{r['multiplier']:<8} "
            f"{r['entry_price']:>12.2f} "
            f"{r['sl']:>12.2f} "
            f"{r['target']:>12.2f} "
            f"{r['exit_ts'].strftime('%Y-%m-%d %H:%M'):<20} "
            f"{outcome:<10} "
            f"{r['pnl_rs']:>12,.2f}"
        )
    print("=" * 110)


def print_performance_summary(trades_df: pd.DataFrame) -> None:
    if trades_df.empty:
        print("\nNo trades available for performance summary.")
        return

    s = v4.summarize_macro_to_micro_trades(trades_df)
    print("\n" + "=" * 70)
    print("Aggregate Performance Matrix — Close Entry Mode")
    print("=" * 70)
    print(f"Total trades        : {s['total']}")
    print(f"Wins                : {s['wins']} ({s['win_rate']:.1f}%)")
    print(f"Losses              : {s['losses']}")
    print(f"Gross profit        : Rs. {s['gross_profit']:,.2f}")
    print(f"Gross loss          : Rs. {s['gross_loss']:,.2f}")
    print(f"Net P&L             : Rs. {s['net_pnl']:,.2f}")
    print(f"Profit factor       : {s['profit_factor']:.2f}")
    print(f"Avg win / avg loss  : Rs. {s['avg_win']:,.2f} / Rs. {s['avg_loss']:,.2f} (R:R = {s['rr']:.2f})")
    print(f"Max drawdown        : Rs. {s['max_dd']:,.2f}")
    print("=" * 70)
    print("\nExit reason distribution:")
    print(trades_df["exit_reason"].value_counts().to_string())
    print("\nDirection distribution:")
    print(trades_df["direction"].value_counts().to_string())


def main() -> None:
    parser = argparse.ArgumentParser(description="NIFTY Macro-to-Micro Trap Backtest")
    parser.add_argument("--start", type=date.fromisoformat, default=DEFAULT_START, help="YYYY-MM-DD")
    parser.add_argument("--end", type=date.fromisoformat, default=DEFAULT_END, help="YYYY-MM-DD")
    args = parser.parse_args()

    start, end = args.start, args.end
    print(f"Loading NIFTY 1m spot data from {start} to {end} ...")
    df_1m = load_1m_spot(start, end)
    if df_1m.empty:
        print("No NIFTY 1m spot data found in the requested range.")
        return

    print(
        f"Loaded {len(df_1m):,} 1m bars from "
        f"{df_1m['datetime'].dt.date.min()} to {df_1m['datetime'].dt.date.max()}"
    )
    print(
        f"\nEngine config: use_filters=False | require_zone_reentry=True | "
        f"require_mtf_ltf_rejection=True | entry_mode={ENTRY_MODE}"
    )

    macro_df, trades_df = v4.backtest_macro_to_micro(
        df_1m,
        multipliers=MULTIPLIERS,
        lookback=3,
        use_filters=False,
        require_zone_reentry=True,
        require_mtf_ltf_rejection=True,
        entry_mode=ENTRY_MODE,
    )

    print_macro_summary(macro_df)
    print_trade_log(trades_df)
    print_performance_summary(trades_df)

    out_macro = os.path.join(OUTPUT_DIR, f"nifty_macro_traps_{start}_{end}.csv")
    out_trades = os.path.join(OUTPUT_DIR, f"nifty_macro_to_micro_trades_{start}_{end}.csv")
    macro_df.to_csv(out_macro, index=False)
    trades_df.to_csv(out_trades, index=False)
    print(f"\nSaved macro traps to {out_macro}")
    print(f"Saved trade log to {out_trades}")


if __name__ == "__main__":
    main()
