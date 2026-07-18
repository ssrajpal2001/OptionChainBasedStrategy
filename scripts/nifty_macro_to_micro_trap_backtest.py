#!/usr/bin/env python3
"""
scripts/nifty_macro_to_micro_trap_backtest.py

Multi-index macro-to-micro trap backtest on spot data (NIFTY / SENSEX / BANKNIFTY).
Defaults to the 1-year window 2025-07-01 to 2026-07-03 and loads the appropriate
`spot_<INDEX>_1m_*.parquet` cache files.

Engine config:
  - use_filters=False              (pure price action)
  - require_zone_reentry=True      (validated trap zone re-entry)
  - require_mtf_ltf_rejection=True (15m/5m structural rejection gate)
  - entry_mode=close               (1m close trigger + V4 1/3 retracement)
  - dual_tranche=True              (50/50 split with 1R break-even, 2R target
                                    on tranche 1, and structural trailing stop
                                    on tranche 2)

Outputs per index:
  1. Macro structural trap summary.
  2. Chronological trade execution log (one row per tranche).
  3. Aggregate performance matrix.
  4. CSV files with raw macro/trade/comparison data.
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
INDICES = ["NIFTY", "SENSEX", "BANKNIFTY"]


def load_1m_spot(index_name: str, start: date, end: date) -> pd.DataFrame:
    pattern = os.path.join(CACHE_DIR, f"spot_{index_name}_1m_*.parquet")
    files = sorted(glob.glob(pattern))
    if not files:
        raise RuntimeError(f"No {index_name} 1m spot parquet files found in {CACHE_DIR}")

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


def print_macro_summary(index_name: str, macro_df: pd.DataFrame) -> None:
    if macro_df.empty:
        print(f"\n[{index_name}] No macro structural traps detected.")
        return

    print(f"\n{'[' + index_name + '] Macro Structural Trap Summary':=^60}")
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


def print_trade_log(index_name: str, trades_df: pd.DataFrame) -> None:
    if trades_df.empty:
        print(f"\n[{index_name}] No trades generated.")
        return

    trades_df = trades_df.sort_values(["entry_ts", "tranche"]).reset_index(drop=True)
    print("\n" + "=" * 130)
    print(f"[{index_name}] Chronological Trade Execution Log — Dual-Tranche Close Mode")
    print("=" * 130)
    header = (
        f"{'#':<4} {'Setup Time':<20} {'Execution Time':<20} {'Dir':<6} {'TF':<8} "
        f"{'T':<4} {'Lots':<6} {'Entry':>12} {'SL':>12} {'2R Target':>12} {'Exit Time':<20} "
        f"{'Outcome':<10} {'P&L (Rs.)':>12}"
    )
    print(header)
    print("-" * 130)

    for i, r in trades_df.iterrows():
        direction = "LONG" if r["direction"] == "LONG" else "SHORT"
        t1_target = (
            r["entry_price"] + 2 * r["initial_risk"]
            if r["direction"] == "LONG"
            else r["entry_price"] - 2 * r["initial_risk"]
        )
        tranche_units = v4.INDEX_LOT_CONFIG.get(index_name, 75)
        lots = int(tranche_units / v4.INDEX_LOT_CONFIG.get(index_name, 75))
        print(
            f"{i+1:<4} "
            f"{r['setup_ts'].strftime('%Y-%m-%d %H:%M'):<20} "
            f"{r['entry_ts'].strftime('%Y-%m-%d %H:%M'):<20} "
            f"{direction:<6} "
            f"{r['multiplier']:<8} "
            f"{r['tranche']:<4} "
            f"{lots:<6} "
            f"{r['entry_price']:>12.2f} "
            f"{r['sl']:>12.2f} "
            f"{t1_target:>12.2f} "
            f"{r['exit_ts'].strftime('%Y-%m-%d %H:%M'):<20} "
            f"{r['exit_reason']:<10} "
            f"{r['pnl_rs']:>12,.2f}"
        )
    print("=" * 130)


def print_performance_summary(index_name: str, trades_df: pd.DataFrame) -> None:
    if trades_df.empty:
        print(f"\n[{index_name}] No trades available for performance summary.")
        return

    s = v4.summarize_macro_to_micro_trades(trades_df)
    print("\n" + "=" * 70)
    print(f"[{index_name}] Aggregate Performance Matrix — Dual-Tranche Close Mode")
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


def run_comparison(index_name: str, df_1m: pd.DataFrame) -> pd.DataFrame:
    """
    Compare single-tranche vs original dual-tranche vs optimized dual-tranche
    trailing configurations for a given index.
    """
    configs = [
        ("single_tranche", False, None, None),
        ("dual_1.5R_2x5m", True, 1.5, ("5m", 2)),
        ("dual_2R_4x5m", True, 2.0, ("5m", 4)),
        ("dual_2R_2x15m", True, 2.0, ("15m", 2)),
    ]
    rows = []
    for label, dual, act_r, trail_cfg in configs:
        print(f"\n[{index_name}] Running {label} mode ...")
        kwargs = {
            "multipliers": MULTIPLIERS,
            "lookback": 3,
            "use_filters": False,
            "require_zone_reentry": True,
            "require_mtf_ltf_rejection": True,
            "entry_mode": ENTRY_MODE,
            "dual_tranche": dual,
            "index_name": index_name,
        }
        if trail_cfg:
            kwargs["trailing_activation_r"] = act_r
            kwargs["trailing_tf"] = trail_cfg[0]
            kwargs["trailing_lookback"] = trail_cfg[1]
        _, trades_df = v4.backtest_macro_to_micro(df_1m, **kwargs)
        s = v4.summarize_macro_to_micro_trades(trades_df)
        rows.append({
            "index": index_name,
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


def run_single_index(index_name: str, start: date, end: date, trailing_config: str) -> None:
    print(f"\nLoading {index_name} 1m spot data from {start} to {end} ...")
    try:
        df_1m = load_1m_spot(index_name, start, end)
    except RuntimeError as exc:
        print(f"SKIP: {exc}")
        return

    if df_1m.empty:
        print(f"No {index_name} 1m spot data found in the requested range.")
        return

    print(
        f"Loaded {len(df_1m):,} 1m bars from "
        f"{df_1m['datetime'].dt.date.min()} to {df_1m['datetime'].dt.date.max()}"
    )

    # Map CLI choice to engine parameters
    trail_map = {
        "1.5R_2x5m": (1.5, "5m", 2),
        "2R_4x5m": (2.0, "5m", 4),
        "2R_2x15m": (2.0, "15m", 2),
    }
    act_r, trail_tf, trail_lb = trail_map[trailing_config]

    # Detailed dual-tranche run
    print(
        f"\n[{index_name}] Engine config: use_filters=False | require_zone_reentry=True | "
        "require_mtf_ltf_rejection=True | entry_mode=close | dual_tranche=True | "
        f"trailing={act_r}R {trail_lb}x{trail_tf}"
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
        trailing_activation_r=act_r,
        trailing_tf=trail_tf,
        trailing_lookback=trail_lb,
        index_name=index_name,
    )

    print_macro_summary(index_name, macro_df)
    print_trade_log(index_name, trades_df)
    print_performance_summary(index_name, trades_df)

    # Single vs original vs optimized comparison
    print("\n" + "=" * 100)
    print(f"[{index_name}] Single-Tranche vs Dual-Tranche Trailing Comparison")
    print("=" * 100)
    comparison = run_comparison(index_name, df_1m)
    print(comparison.to_string(index=False))
    print("=" * 100)

    out_macro = os.path.join(OUTPUT_DIR, f"{index_name.lower()}_macro_traps_{start}_{end}.csv")
    out_trades = os.path.join(OUTPUT_DIR, f"{index_name.lower()}_macro_to_micro_trades_{start}_{end}.csv")
    out_comparison = os.path.join(OUTPUT_DIR, f"{index_name.lower()}_tranche_comparison_{start}_{end}.csv")
    macro_df.to_csv(out_macro, index=False)
    trades_df.to_csv(out_trades, index=False)
    comparison.to_csv(out_comparison, index=False)
    print(f"\n[{index_name}] Saved macro traps to {out_macro}")
    print(f"[{index_name}] Saved trade log to {out_trades}")
    print(f"[{index_name}] Saved comparison to {out_comparison}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-Index Macro-to-Micro Trap Backtest")
    parser.add_argument("--start", type=date.fromisoformat, default=DEFAULT_START, help="YYYY-MM-DD")
    parser.add_argument("--end", type=date.fromisoformat, default=DEFAULT_END, help="YYYY-MM-DD")
    parser.add_argument(
        "--index",
        type=str,
        default="NIFTY",
        choices=INDICES,
        help="Index to backtest (default: NIFTY).",
    )
    parser.add_argument(
        "--multi-index",
        action="store_true",
        help="Run the backtest for NIFTY, SENSEX, and BANKNIFTY sequentially.",
    )
    parser.add_argument(
        "--trailing-config",
        type=str,
        default="2R_4x5m",
        choices=["1.5R_2x5m", "2R_4x5m", "2R_2x15m"],
        help="Dual-tranche trailing configuration for the detailed run",
    )
    args = parser.parse_args()

    if args.multi_index:
        indices = INDICES
    else:
        indices = [args.index]

    for index_name in indices:
        run_single_index(index_name, args.start, args.end, args.trailing_config)


if __name__ == "__main__":
    main()
