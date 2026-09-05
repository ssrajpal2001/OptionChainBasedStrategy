"""
scripts/oi_orb_final_permutation_sweep.py -- 2026-09-05, direct user spec:
"i have suggested many approach u need to chk from ur end permutation
combination which is best sl ad tsl logic." Consolidated final sweep across
every mechanic tried this session's key parameters, plus the ladder x
hybrid combinations, to settle on one definitive answer.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_final_permutation_sweep.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from functools import partial

sys.path.insert(0, ".")

from scripts.oi_orb_atr_chandelier_backtest import (
    fetch_all, run_one, summarize,
    simulate_atr_chandelier_exit, simulate_rr_target_exit,
    simulate_rr_ladder_exit, simulate_rr_then_chandelier_exit,
)
from scripts.oi_orb_entry_mode_backtest import simulate_fixed_sl_exit

TOKEN = os.environ.get("UPSTOX_TOKEN", "")


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows once (cached for the whole sweep)...")
    cache = await fetch_all()

    results = {}
    results["baseline"] = summarize("baseline(fixed SL/EOD)", run_one(cache, simulate_fixed_sl_exit))

    print("\n" + "=" * 110)
    print("R:R LADDER GRID -- sl_mult x step_r")
    print("=" * 110)
    for sl_mult in (1.0, 1.5, 2.0):
        for step_r in (1.5, 2.0, 2.5, 3.0):
            key = f"ladder_sl{sl_mult}_step{step_r}"
            exit_fn = partial(simulate_rr_ladder_exit, sl_mult=sl_mult, step_r=step_r)
            results[key] = summarize(f"Ladder SL={sl_mult}x step=1:{step_r}", run_one(cache, exit_fn))

    print("\n" + "=" * 110)
    print("HYBRID GRID -- fixed R:R first leg (breakeven-lock) then Chandelier trail")
    print("=" * 110)
    for sl_mult in (1.0, 1.5, 2.0):
        for first_r in (1.5, 2.0, 2.5):
            for tsl_mult in (2.0, 2.5, 3.0):
                key = f"hybrid_sl{sl_mult}_r{first_r}_tsl{tsl_mult}"
                exit_fn = partial(simulate_rr_then_chandelier_exit, sl_mult=sl_mult, first_r=first_r, tsl_mult=tsl_mult)
                results[key] = summarize(f"Hybrid SL={sl_mult}x 1st={first_r}R Chand={tsl_mult}x", run_one(cache, exit_fn))

    print("\n" + "=" * 110)
    print("TOP 10 BY TOTAL POINTS (all candidates incl. baseline)")
    print("=" * 110)
    ranked = sorted(results.values(), key=lambda r: r["total"], reverse=True)
    for r in ranked[:10]:
        boschltd = next((t for t in r["trades"] if t.symbol == "BOSCHLTD"), None)
        b_str = f"{boschltd.points:+.2f}" if boschltd else "n/a"
        print(f"{r['label']:>36}  total={r['total']:+9.2f}  PF={r['pf']:6.2f}  win%={r['win_pct']:5.1f}  BOSCHLTD={b_str}")

    print("\n" + "=" * 110)
    print("TOP 10 BY PF (min 30 trades entered)")
    print("=" * 110)
    ranked_pf = sorted((r for r in results.values() if r["entered"] >= 30 and r["pf"] != float("inf")),
                        key=lambda r: r["pf"], reverse=True)
    for r in ranked_pf[:10]:
        boschltd = next((t for t in r["trades"] if t.symbol == "BOSCHLTD"), None)
        b_str = f"{boschltd.points:+.2f}" if boschltd else "n/a"
        print(f"{r['label']:>36}  total={r['total']:+9.2f}  PF={r['pf']:6.2f}  win%={r['win_pct']:5.1f}  BOSCHLTD={b_str}")

    best = ranked[0] if ranked[0]["label"] != results["baseline"]["label"] else ranked[1]
    print(f"\n{'='*110}\nBEST OVERALL: {best['label']} (total={best['total']:+.2f}, PF={best['pf']:.2f}, win%={best['win_pct']:.1f})")
    print("=" * 110)
    print(f"{'Date':<12}{'Symbol':<13}{'Side':<5}{'Entry TS':<9}{'Entry Px':>10}  {'Exit TS':<9}{'Exit Px':>10}  {'Reason':<22}{'Points':>10}")
    for t in best["trades"]:
        if t.entry_price is None:
            print(f"{t.date:<12}{t.symbol:<13}{t.side:<5} NO ENTRY")
        else:
            print(f"{t.date:<12}{t.symbol:<13}{t.side:<5}{t.entry_ts.strftime('%H:%M'):<9}{t.entry_price:>10.2f}  "
                  f"{t.exit_ts.strftime('%H:%M'):<9}{t.exit_price:>10.2f}  {t.reason:<22}{t.points:>+10.2f}")


asyncio.run(main())
