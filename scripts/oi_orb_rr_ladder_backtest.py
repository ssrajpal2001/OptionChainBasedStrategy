"""
scripts/oi_orb_rr_ladder_backtest.py -- 2026-09-05, focused comparison:
baseline vs fixed R:R 1:2 (the prior winner) vs the new R-multiple staircase
(simulate_rr_ladder_exit) -- "if 1:2 reaches we jump sl to cost and from
then 1:2 position again" -- lets a winner keep running leg after leg
instead of capping at the first target, per direct user follow-up.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_rr_ladder_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from functools import partial

sys.path.insert(0, ".")

from scripts.oi_orb_atr_chandelier_backtest import (
    fetch_all, run_one, summarize, simulate_rr_target_exit, simulate_rr_ladder_exit,
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
    print("BASELINE vs FIXED R:R 1:2 vs R-MULTIPLE LADDER (3-min ATR throughout)")
    print("=" * 110)
    results = {}
    results["baseline"] = summarize("baseline(fixed SL/EOD)", run_one(cache, simulate_fixed_sl_exit))
    results["rr_fixed_2.0"] = summarize("Fixed R:R 1:2.0 (capped)", run_one(cache, partial(simulate_rr_target_exit, sl_mult=1.5, rr_multiple=2.0)))
    results["rr_ladder_2.0"] = summarize("R:R LADDER step=2.0 (lock&run)", run_one(cache, partial(simulate_rr_ladder_exit, sl_mult=1.5, step_r=2.0)))
    results["rr_ladder_1.5"] = summarize("R:R LADDER step=1.5 (lock&run)", run_one(cache, partial(simulate_rr_ladder_exit, sl_mult=1.5, step_r=1.5)))

    print("\n" + "=" * 110)
    print("FULL PER-TRADE DETAIL WITH TIMESTAMPS -- R:R LADDER step=2.0")
    print("=" * 110)
    print(f"{'Date':<12}{'Symbol':<13}{'Side':<5}{'Entry TS':<9}{'Entry Px':>10}  {'Exit TS':<9}{'Exit Px':>10}  {'Reason':<20}{'Points':>10}")
    for t in results["rr_ladder_2.0"]["trades"]:
        if t.entry_price is None:
            print(f"{t.date:<12}{t.symbol:<13}{t.side:<5} NO ENTRY")
        else:
            print(f"{t.date:<12}{t.symbol:<13}{t.side:<5}{t.entry_ts.strftime('%H:%M'):<9}{t.entry_price:>10.2f}  "
                  f"{t.exit_ts.strftime('%H:%M'):<9}{t.exit_price:>10.2f}  {t.reason:<20}{t.points:>+10.2f}")

    print("\n" + "=" * 110)
    print("KEY TRADES: BOSCHLTD / MARUTI / POLYCAB / FORCEMOT -- baseline vs capped R:R vs ladder")
    print("=" * 110)
    watch = {"BOSCHLTD", "MARUTI", "POLYCAB", "FORCEMOT", "SOLARINDS", "PERSISTENT"}
    for key in ("baseline", "rr_fixed_2.0", "rr_ladder_2.0"):
        print(f"\n-- {results[key]['label']} --")
        for t in results[key]["trades"]:
            if t.symbol in watch and t.entry_price is not None:
                print(f"  {t.date} {t.symbol:<12} {t.side:<4} entry={t.entry_price:9.2f}@{t.entry_ts.strftime('%H:%M')} "
                      f"exit={t.exit_price:9.2f}@{t.exit_ts.strftime('%H:%M')} ({t.reason})  pts={t.points:+8.2f}")


asyncio.run(main())
