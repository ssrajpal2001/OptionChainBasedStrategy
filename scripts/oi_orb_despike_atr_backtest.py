"""
scripts/oi_orb_despike_atr_backtest.py -- 2026-09-06, direct user follow-up:
does de-spiking the ATR calculation (median-of-TR vs mean-of-TR, or a
winsorized/capped mean) fix BSE's regression under the winning R:R ladder
config (SL=2.0x ATR, step=1:1.5R)? Also checks the aggregate impact across
all 42 trades -- a de-spike fix that helps BSE but hurts the total would
not be worth adopting.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_despike_atr_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from functools import partial

sys.path.insert(0, ".")

from scripts.oi_orb_atr_chandelier_backtest import (
    fetch_all, run_one, summarize, simulate_rr_ladder_exit,
    compute_atr_series, compute_atr_series_median, compute_atr_series_capped,
)
from scripts.oi_orb_entry_mode_backtest import simulate_fixed_sl_exit

TOKEN = os.environ.get("UPSTOX_TOKEN", "")


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows once...")
    cache = await fetch_all()

    print("\n" + "=" * 110)
    print("DE-SPIKE ATR COMPARISON -- winning ladder (SL=2.0x, step=1:1.5R) with 3 ATR calculators")
    print("=" * 110)
    results = {}
    results["baseline"] = summarize("baseline(fixed SL/EOD)", run_one(cache, simulate_fixed_sl_exit))
    results["mean"] = summarize("ladder, mean-ATR (current best)", run_one(cache, partial(
        simulate_rr_ladder_exit, sl_mult=2.0, step_r=1.5, atr_fn=compute_atr_series)))
    results["median"] = summarize("ladder, median-ATR", run_one(cache, partial(
        simulate_rr_ladder_exit, sl_mult=2.0, step_r=1.5, atr_fn=compute_atr_series_median)))
    for cap in (1.2, 1.3, 1.5):
        key = f"capped_{cap}"
        results[key] = summarize(f"ladder, capped-ATR (x{cap} median)", run_one(cache, partial(
            simulate_rr_ladder_exit, sl_mult=2.0, step_r=1.5,
            atr_fn=partial(compute_atr_series_capped, cap_mult=cap))))

    print("\n" + "=" * 110)
    print("BSE / EICHERMOT / COALINDIA DETAIL ACROSS ALL ATR CALCULATORS")
    print("=" * 110)
    watch = {"BSE", "EICHERMOT", "COALINDIA", "BOSCHLTD", "FORCEMOT"}
    for key, r in results.items():
        print(f"\n-- {r['label']} --")
        for t in r["trades"]:
            if t.symbol in watch and t.entry_price is not None:
                print(f"  {t.date} {t.symbol:<12} {t.side:<4} entry={t.entry_price:9.2f}@{t.entry_ts.strftime('%H:%M')} "
                      f"exit={t.exit_price:9.2f}@{t.exit_ts.strftime('%H:%M')} ({t.reason})  pts={t.points:+8.2f}")

    print("\n" + "=" * 110)
    print("SUMMARY")
    print("=" * 110)
    for key, r in results.items():
        print(f"{r['label']:>36}  total={r['total']:+9.2f}  PF={r['pf']:6.2f}  win%={r['win_pct']:5.1f}")


asyncio.run(main())
