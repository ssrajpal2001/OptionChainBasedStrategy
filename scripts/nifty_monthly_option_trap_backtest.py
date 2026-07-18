#!/usr/bin/env python3
"""
scripts/nifty_monthly_option_trap_backtest.py
===============================================
NIFTY Monthly Option Premium Trap Cascade backtest.

Loads NIFTY spot 1m data and the monthly option premium 1m parquet,
then runs the dual-layered structural engine from
strategies/trap_scanner/monthly_option_cascade.

Data expectation:
  data/nse_option_cache/spot_NIFTY_1m_*.parquet
  data/nse_option_cache/opt_NIFTY_monthly_<expiry>_1m.parquet

If the option data file is missing, use --use-synthetic to generate it from spot.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, time

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from strategies.trap_scanner import monthly_option_cascade as moc

CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")
OUTPUT_DIR = os.path.join(ROOT, "data")

DEFAULT_START = date(2026, 6, 1)
DEFAULT_END = date(2026, 7, 3)
DEFAULT_EXPIRY = date(2026, 6, 30)


def print_macro_summary(macro_df: pd.DataFrame) -> None:
    if macro_df.empty:
        print("\n[NIFTY Monthly] No macro premium traps detected.")
        return

    print("\n" + "=" * 150)
    print("[NIFTY Monthly] Macro Premium Trap Summary")
    print("=" * 150)
    print(f"Total structural traps: {len(macro_df)}")
    print(f"  Bull traps (short setups): {sum(macro_df['type'] == 'Bull')}")
    print(f"  Bear traps (long setups):  {sum(macro_df['type'] == 'Bear')}")
    print(f"  Spot-confirmed traps:      {sum(macro_df['spot_confirmed'])}")
    for mult in moc.DEFAULT_MULTIPLIERS:
        cnt = sum(macro_df["multiplier"] == f"{mult}m")
        print(f"  {mult}m: {cnt}")

    print("\nFirst 10 macro traps (chronological):")
    fmt = lambda ts: ts.strftime("%Y-%m-%d %H:%M") if isinstance(ts, pd.Timestamp) else str(ts)[:16]
    cols = ["ref_ts", "breakout_ts", "confirm_ts", "type", "multiplier",
            "tracking_strike", "opt_type", "execution_strike", "breakout_line", "anchor_sl", "peak", "spot_confirmed"]
    head = macro_df.sort_values("confirm_ts").head(10)[cols]
    for _, r in head.iterrows():
        print(
            f"  {fmt(r['ref_ts'])} -> {fmt(r['breakout_ts'])} -> {fmt(r['confirm_ts'])} | "
            f"{r['type']:<4} {r['multiplier']:<5} | "
            f"track={r['tracking_strike']}{r['opt_type']} exec={r['execution_strike']}{r['opt_type']} | "
            f"zone {r['breakout_line']:.2f} / anchor {r['anchor_sl']:.2f} / peak {r['peak']:.2f} | "
            f"spot_conf={r['spot_confirmed']}"
        )
    print("=" * 150)


def print_trade_log(trades_df: pd.DataFrame) -> None:
    if trades_df.empty:
        print("\n[NIFTY Monthly] No trades generated.")
        return

    trades_df = trades_df.sort_values(["entry_ts", "tranche"]).reset_index(drop=True)
    print("\n" + "=" * 160)
    print("[NIFTY Monthly] Chronological Trade Execution Log — Dual-Tranche Close Mode")
    print("=" * 160)
    header = (
        f"{'#':<4} {'Setup Time':<20} {'Execution Time':<20} {'Dir':<6} {'Track':<10} {'Exec':<10} "
        f"{'TF':<8} {'T':<4} {'Lots':<6} {'Entry':>12} {'SL':>12} {'2R Target':>12} "
        f"{'Exit Time':<20} {'Outcome':<12} {'P&L (Rs.)':>12}"
    )
    print(header)
    print("-" * 160)

    for i, r in trades_df.iterrows():
        direction = "LONG" if r["direction"] == "LONG" else "SHORT"
        lots = int(moc.LOT_SIZE / moc.LOT_SIZE)  # 1 lot per tranche
        print(
            f"{i+1:<4} "
            f"{r['setup_ts'].strftime('%Y-%m-%d %H:%M'):<20} "
            f"{r['entry_ts'].strftime('%Y-%m-%d %H:%M'):<20} "
            f"{direction:<6} "
            f"{r['tracking_strike']}{r['opt_type']:<4} "
            f"{r['execution_strike']}{r['opt_type']:<4} "
            f"{r['multiplier']:<8} "
            f"{r['tranche']:<4} "
            f"{lots:<6} "
            f"{r['entry_price_exec']:>12.2f} "
            f"{r['sl']:>12.2f} "
            f"{r['t1_target_track']:>12.2f} "
            f"{r['exit_ts'].strftime('%Y-%m-%d %H:%M'):<20} "
            f"{r['exit_reason']:<12} "
            f"{r['pnl_rs']:>12,.2f}"
        )
    print("=" * 160)


def print_performance_summary(trades_df: pd.DataFrame) -> None:
    if trades_df.empty:
        print("\n[NIFTY Monthly] No trades available for performance summary.")
        return

    s = moc.summarize_monthly_trades(trades_df)
    print("\n" + "=" * 70)
    print("[NIFTY Monthly] Aggregate Performance Matrix — Dual-Tranche Close Mode")
    print("=" * 70)
    print(f"Index               : NIFTY Monthly")
    print(f"Total Premium Traps : {s['setup_count']}")
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


def main() -> None:
    parser = argparse.ArgumentParser(description="NIFTY Monthly Option Premium Trap Backtest")
    parser.add_argument("--start", type=date.fromisoformat, default=DEFAULT_START, help="YYYY-MM-DD")
    parser.add_argument("--end", type=date.fromisoformat, default=DEFAULT_END, help="YYYY-MM-DD")
    parser.add_argument("--expiry", type=date.fromisoformat, default=DEFAULT_EXPIRY, help="Monthly expiry YYYY-MM-DD")
    parser.add_argument("--use-synthetic", action="store_true", help="Generate synthetic option premium from spot")
    parser.add_argument("--spot-aligned-synthetic", action="store_true",
                        help="Use shifted-spot synthetic premiums so macro traps align with spot traps (testing only)")
    parser.add_argument("--entry-mode", type=str, default="close", choices=["close", "limit", "wick"])
    parser.add_argument("--trailing-activation-r", type=float, default=2.0, help="R multiple to activate T2 trail")
    parser.add_argument("--trailing-lookback", type=int, default=4, help="Number of 5m candles for T2 trail")
    args = parser.parse_args()

    print(f"Loading NIFTY spot 1m data from {args.start} to {args.end} ...")
    df_spot = moc.load_spot_data("NIFTY", args.start, args.end)
    if df_spot.empty:
        print("No NIFTY spot data found in the requested range.")
        return
    print(
        f"Loaded {len(df_spot):,} 1m bars from "
        f"{df_spot['datetime'].dt.date.min()} to {df_spot['datetime'].dt.date.max()}"
    )

    # Determine required strikes for the month
    spot_open = float(df_spot.iloc[0]["open"])
    ce_track, pe_track = moc.select_tracking_strikes(spot_open)
    ce_exec, pe_exec = moc.select_execution_strikes(spot_open)
    required_strikes = sorted(set([ce_track, pe_track, ce_exec, pe_exec]))
    print(f"Day-open spot={spot_open:.2f}  tracking_strikes={ce_track}CE/{pe_track}PE  "
          f"execution_strikes={ce_exec}CE/{pe_exec}PE")

    df_opt = moc.load_monthly_option_data(args.expiry, required_strikes=None)
    if df_opt is None:
        if not args.use_synthetic:
            print(
                f"\nMonthly option premium data not found: "
                f"{CACHE_DIR}/opt_NIFTY_monthly_{args.expiry.isoformat()}_1m.parquet\n"
                f"Please place the real premium parquet file or re-run with --use-synthetic."
            )
            return
        print(f"\nGenerating synthetic option premium data for expiry {args.expiry} ...")
        # Generate a wide strike grid so daily ATM-based selection always finds data.
        df_opt = moc.generate_synthetic_monthly_option_file(
            df_spot, args.expiry, None, spot_aligned=args.spot_aligned_synthetic
        )
        print(f"Synthetic option premium saved; strikes={sorted(df_opt['strike'].unique())[:12]}...")
        if args.spot_aligned_synthetic:
            print("WARNING: --spot-aligned-synthetic produces spot-aligned premiums for pipeline testing only; P&L is NOT realistic.")
    else:
        print(f"Loaded monthly option premium data for {args.expiry}.")

    print(
        f"\n[NIFTY Monthly] Engine config: "
        f"entry_mode={args.entry_mode} | zone_reentry=True | mtf_ltf_rejection=True | "
        f"dual_tranche=True | trailing={args.trailing_activation_r}R {args.trailing_lookback}x5m"
    )
    macro_df, trades_df = moc.backtest_monthly_option_cascade(
        df_spot,
        df_opt,
        args.expiry,
        multipliers=moc.DEFAULT_MULTIPLIERS,
        lookback=moc.DEFAULT_LOOKBACK,
        entry_mode=args.entry_mode,
        require_zone_reentry=True,
        require_mtf_ltf_rejection=True,
        trailing_activation_r=args.trailing_activation_r,
        trailing_lookback=args.trailing_lookback,
        index_name="NIFTY",
    )

    print_macro_summary(macro_df)
    print_trade_log(trades_df)
    print_performance_summary(trades_df)

    out_macro = os.path.join(OUTPUT_DIR, f"nifty_monthly_opt_macro_{args.expiry}.csv")
    out_trades = os.path.join(OUTPUT_DIR, f"nifty_monthly_opt_trades_{args.expiry}.csv")
    macro_df.to_csv(out_macro, index=False)
    trades_df.to_csv(out_trades, index=False)
    print(f"\nSaved macro traps to {out_macro}")
    print(f"Saved trade log to {out_trades}")


if __name__ == "__main__":
    main()
