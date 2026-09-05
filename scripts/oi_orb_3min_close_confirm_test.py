"""
scripts/oi_orb_3min_close_confirm_test.py -- 2026-09-06: tests TRUE 3-min
close-confirmation (matching the ATR's own timeframe) against the frozen
config's current 1-min close-confirmation, per the real BSE/ATHERENERG
finding that 1-min close-confirm triggers too easily.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_3min_close_confirm_test.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from functools import partial

sys.path.insert(0, ".")

from scripts.oi_orb_atr_chandelier_backtest import (
    fetch_all, run_one, summarize, simulate_rr_ladder_exit_v2, compute_atr_series_capped,
)
from scripts.oi_orb_entry_mode_backtest import simulate_fixed_sl_exit

TOKEN = os.environ.get("UPSTOX_TOKEN", "")


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows once...")
    cache = await fetch_all()

    atr_fn = partial(compute_atr_series_capped, cap_mult=1.5)
    results = {}
    results["baseline"] = summarize("baseline(fixed SL/EOD)", run_one(cache, simulate_fixed_sl_exit))
    results["frozen_1min_confirm"] = summarize("FROZEN (1-min close-confirm)", run_one(cache, partial(
        simulate_rr_ladder_exit_v2, atr_fn=atr_fn, close_confirm=True, risk_pct_floor=0.005)))
    results["3min_confirm"] = summarize("3-min close-confirm (matches ATR tf)", run_one(cache, partial(
        simulate_rr_ladder_exit_v2, atr_fn=atr_fn, close_confirm=True, close_confirm_tf=3, risk_pct_floor=0.005)))

    print("\n" + "=" * 110)
    print("BSE / ATHERENERG / BAJAJ-AUTO DETAIL -- frozen vs 3-min confirm")
    print("=" * 110)
    watch = {"BSE", "ATHERENERG", "BAJAJ-AUTO", "EICHERMOT", "GODREJCP", "SWIGGY", "COALINDIA", "HEROMOTOCO", "BOSCHLTD", "FORCEMOT", "MARUTI", "POLYCAB"}
    for key in ("frozen_1min_confirm", "3min_confirm"):
        print(f"\n-- {results[key]['label']} --")
        for t in results[key]["trades"]:
            if t.symbol in watch and t.entry_price is not None:
                print(f"  {t.date} {t.symbol:<12} {t.side:<4} entry={t.entry_price:9.2f}@{t.entry_ts.strftime('%H:%M')} "
                      f"exit={t.exit_price:9.2f}@{t.exit_ts.strftime('%H:%M')} ({t.reason})  pts={t.points:+8.2f}")

    print("\n" + "=" * 110)
    print("SUMMARY")
    print("=" * 110)
    for key, r in results.items():
        print(f"{r['label']:>36}  total={r['total']:+9.2f}  PF={r['pf']:6.2f}  win%={r['win_pct']:5.1f}")


asyncio.run(main())
