"""
scripts/liquidity_trap_today_whatif.py -- one-off "what if the Dhan orders
had actually gone through today" backtest, using the EXACT currently-live
mechanic (multi-ref, skip-if-blocked, ref_tf=20m/confirm_tf=3m, 60m/SMA10
trend filter) against TODAY's real spot data for both NIFTY and SENSEX.

Reuses the pure functions already built and validated in
scripts/liquidity_trap_tf_and_trend_sweep.py (same resample/_trend_sma/
run_day_multiref_filtered -- no reimplementation, so this can't
behaviorally drift from what was actually optimized/deployed).

Usage: python scripts/liquidity_trap_today_whatif.py <upstox_token>
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer.historical_candles import fetch_upstox_range_1m, fetch_upstox_intraday_1m
from scripts.liquidity_trap_multiref_backtest import Bar, swing_points
from scripts.liquidity_trap_tf_and_trend_sweep import (
    resample, _trend_sma, run_day_multiref_filtered, _summarize,
)

TOKEN = sys.argv[1] if len(sys.argv) > 1 else ""
REF_TF, CONFIRM_TF, TREND_TF, TREND_SMA_LEN = 20, 3, 60, 10
KEYS = {"NIFTY": "NSE_INDEX|Nifty 50", "SENSEX": "BSE_INDEX|SENSEX"}


async def run_for(underlying: str, key: str):
    today = datetime.now(IST).date()
    # Range endpoint only returns COMPLETED days (matches _seed_trend_history's
    # own past-days-only fetch) -- today itself needs the separate intraday
    # endpoint (matches _warmup_intraday's own fetch), same two-call split the
    # live engine uses.
    start = today - timedelta(days=12)
    past_rows, today_rows = await asyncio.gather(
        fetch_upstox_range_1m(key, TOKEN, start, today - timedelta(days=1)),
        fetch_upstox_intraday_1m(key, TOKEN),
    )
    rows = (past_rows or []) + (today_rows or [])
    if not rows:
        print(f"{underlying}: no data returned.")
        return
    bars_1m = [Bar(ts=datetime.fromisoformat(r["ts"]), open=r["open"], high=r["high"],
                   low=r["low"], close=r["close"]) for r in rows]
    bars_1m.sort(key=lambda b: b.ts)
    print(f"\n{underlying}: {len(bars_1m)} 1m bars, {bars_1m[0].ts.date()}..{bars_1m[-1].ts.date()}")

    bars_ref = resample(bars_1m, REF_TF)
    bars_confirm = resample(bars_1m, CONFIRM_TF)
    bars_trend = resample(bars_1m, TREND_TF)
    swings_ref = swing_points(bars_ref, pivot=2)
    trend_by_ts = _trend_sma(bars_trend, TREND_SMA_LEN)

    day_1m = [b for b in bars_1m if b.ts.date() == today]
    trades = run_day_multiref_filtered(
        today, day_1m, bars_1m, bars_ref, bars_confirm, swings_ref, "liquidity",
        trend_by_ts=trend_by_ts, trend_bars=bars_trend,
    )
    if not trades:
        print(f"{underlying}: no trades would have fired today under the live config.")
        return
    print(f"{underlying}: {len(trades)} trade(s) today under the live config (20m/3m + 60m trend filter):\n")
    header = f"{'DIR':4} {'ENTRY':16} {'ENTRY@':>9} {'SL':>9} {'TARGET':>9} {'EXIT':16} {'EXIT@':>9} {'REASON':10} {'PNL(pts)':>9}"
    print(header)
    print("-" * len(header))
    for t in trades:
        print(f"{t.direction:4} {t.entry_ts.strftime('%H:%M'):16} {t.entry_price:9.2f} {t.sl:9.2f} "
              f"{t.target:9.2f} {(t.exit_ts.strftime('%H:%M') if t.exit_ts else '-'):16} "
              f"{(t.exit_price or 0):9.2f} {t.exit_reason:10} {t.pnl_pts:9.2f}")
    s = _summarize(trades)
    print(f"\n{underlying} TOTAL: n={s['n']} win%={s['win_pct']:.1f} PF(lot)={s['pf_lot']:.2f} "
          f"net_Rs(lot=NIFTY75/SENSEX20)={s['net_rupees']:.2f}")


async def main():
    if not TOKEN:
        print("Usage: python scripts/liquidity_trap_today_whatif.py <upstox_token>")
        return
    for underlying, key in KEYS.items():
        await run_for(underlying, key)


if __name__ == "__main__":
    asyncio.run(main())
