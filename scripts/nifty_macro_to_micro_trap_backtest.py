#!/usr/bin/env python3
"""
scripts/nifty_macro_to_micro_trap_backtest.py

Production-grade macro-to-micro trap backtest on NIFTY spot data with advanced
dual-tranche risk management.

Defaults to the 1-year window 2025-07-01 to 2026-07-03. The script reads every
`spot_NIFTY_1m_*.parquet` file in data/nse_option_cache/ (including the file
produced by scripts/fetch_upstox_historical.py).

Engine config:
  - use_filters=False              (pure price action)
  - require_zone_reentry=True      (validated trap zone re-entry)
  - require_mtf_ltf_rejection=True (15m/5m structural rejection gate)
  - entry_mode=close               (1m close trigger + V4 1/3 retracement)
  - dual_tranche=True              (50/50 split with 1R break-even, 2R target
                                    on tranche 1, and 5m structural trailing
                                    stop on tranche 2 after 1.5R)

Outputs:
  1. Macro structural trap summary.
  2. Chronological trade execution log (one row per tranche).
  3. Aggregate performance matrix.
  4. CSV files with raw macro/trade data.
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
        # Normalize column name: Upstox fetcher uses 'timestamp', existing cache uses 'datetime'
        if "timestamp" in df.columns and "datetime" not in df.columns:
            df = df.rename(columns={"timestamp": "datetime"})
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
            f"{r['type']:<4} {r['multiplier']:<5} | "
            f"zone {r['breakout_line']:.2f} / anchor {r['anchor_sl']:.2f} / peak {r['peak']:.2f}"
        )


def print_trade_log(trades_df: pd.DataFrame) -> None:
    if trades_df.empty:
        print("\nNo trades generated.")
        return

    trades_df = trades_df.sort_values(["entry_ts", "tranche"]).reset_index(drop=True)
    print("\n" + "=" * 120)
    print("Chronological Trade Execution Log — Dual-Tranche Close Mode")
    print("=" * 120)
    header = (
        f"{'#':<4} {'Setup Time':<20} {'Execution Time':<20} {'Dir':<6} {'TF':<8} "
        f"{'T':<4} {'Entry':>12} {'SL':>12} {'2R Target':>12} {'Exit Time':<20} "
        f"{'Outcome':<10} {'P&L (Rs.)':>12}"
    )
    print(header)
    print("-" * 120)

    for i, r in trades_df.iterrows():
        direction = "LONG" if r["direction"] == "LONG" else "SHORT"
        t1_target = r["entry_price"] + 2 * r["initial_risk"] if r["direction"] == "LONG" else r["entry_price"] - 2 * r["initial_risk"]
        print(
            f"{i+1:<4} "
            f"{r['setup_ts'].strftime('%Y-%m-%d %H:%M'):<20} "
            f"{r['entry_ts'].strftime('%Y-%m-%d %H:%M'):<20} "
            f"{direction:<6} "
            f"{r['multiplier']:<8} "
            f"{r['tranche']:<4} "
            f"{r['entry_price']:>12.2f} "
            f"{r['sl']:>12.2f} "
            f"{t1_target:>12.2f} "
            f"{r['exit_ts'].strftime('%Y-%m-%d %H:%M'):<20} "
            f"{r['exit_reason']:<10} "
            f"{r['pnl_rs']:>12,.2f}"
        )
    print("=" * 120)


def print_performance_summary(trades_df: pd.DataFrame) -> None:
    if trades_df.empty:
        print("\nNo trades available for performance summary.")
        return

    s = v4.summarize_macro_to_micro_trades(trades_df)
    print("\n" + "=" * 70)
    print("Aggregate Performance Matrix — Dual-Tranche Close Mode")
    print("=" * 70)
    print(f"Setups executed     : {s['setup_count']}")
    print(f"Tranche executions  : {s['total']}")
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
    print("\nTranche distribution:")
    print(trades_df["tranche"].value_counts().to_string())


def run_comparison(df_1m: pd.DataFrame) -> pd.DataFrame:
    """Compare single-tranche (baseline) vs dual-tranche (new risk engine)."""
    rows = []
    for label, dual in [("single_tranche", False), ("dual_tranche", True)]:
        print(f"\nRunning {label} mode ...")
        _, trades_df = v4.backtest_macro_to_micro(
            df_1m,
            multipliers=MULTIPLIERS,
            lookback=3,
            use_filters=False,
            require_zone_reentry=True,
            require_mtf_ltf_rejection=True,
            entry_mode=ENTRY_MODE,
            dual_tranche=dual,
        )
        s = v4.summarize_macro_to_micro_trades(trades_df)
        rows.append({
            "mode": label,
            "setups": s["setup_count"],
            "tranches": s["total"],
            "wins": s["wins"],
            "losses": s["losses"],
            "win_pct": round(s["win_rate"], 1),
            "profit_factor": round(s["profit_factor"], 2) if s["profit_factor"] != float("inf") else "inf",
            "max_dd": round(s["max_dd"], 2),
            "net_pnl": round(s["net_pnl"], 2),
        })
    return pd.DataFrame(rows)


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

    # Detailed dual-tranche run
    print(
        "\nEngine config: use_filters=False | require_zone_reentry=True | "
        "require_mtf_ltf_rejection=True | entry_mode=close | dual_tranche=True"
    )
    macro_df, trades_df = v4.backtest_macro_to_micro(
        df_1m,
        multipliers=MULTIPLIERS,
        lookback=3,
        use_filters=False,
        require_zone_reentry=True,
        require_mtf_ltf_rejection=True,
        entry_mode=ENTRY_MODE,
        dual_tranche=True,
    )

    print_macro_summary(macro_df)
    print_trade_log(trades_df)
    print_performance_summary(trades_df)

    # Single vs dual tranche comparison
    print("\n" + "=" * 80)
    print("Single-Tranche vs Dual-Tranche Comparison")
    print("=" * 80)
    comparison = run_comparison(df_1m)
    print(comparison.to_string(index=False))
    print("=" * 80)

    out_macro = os.path.join(OUTPUT_DIR, f"nifty_macro_traps_{start}_{end}.csv")
    out_trades = os.path.join(OUTPUT_DIR, f"nifty_macro_to_micro_trades_{start}_{end}.csv")
    out_comparison = os.path.join(OUTPUT_DIR, f"nifty_tranche_comparison_{start}_{end}.csv")
    macro_df.to_csv(out_macro, index=False)
    trades_df.to_csv(out_trades, index=False)
    comparison.to_csv(out_comparison, index=False)
    print(f"\nSaved macro traps to {out_macro}")
    print(f"Saved trade log to {out_trades}")
    print(f"Saved comparison to {out_comparison}")


if __name__ == "__main__":
    main()
