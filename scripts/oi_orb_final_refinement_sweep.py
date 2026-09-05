"""
scripts/oi_orb_final_refinement_sweep.py -- 2026-09-06, final round: tests
close-confirmed SL (expert-panel convergent recommendation) and %-clamped
risk sizing (direct user follow-up on price-tier normalization) -- alone
and combined -- against the current best (capped-ATR ladder, SL=2.0x,
step=1:1.5R, intrabar-touch trigger, unclamped risk).

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_final_refinement_sweep.py
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
    results["current_best"] = summarize("current best (touch, no clamp)", run_one(cache, partial(
        simulate_rr_ladder_exit_v2, atr_fn=atr_fn, close_confirm=False)))
    results["close_confirm"] = summarize("+ close-confirmed SL", run_one(cache, partial(
        simulate_rr_ladder_exit_v2, atr_fn=atr_fn, close_confirm=True)))

    print("\n" + "=" * 110)
    print("PCT-CLAMP GRID -- risk floor/cap as % of entry price (intrabar touch, no close-confirm)")
    print("=" * 110)
    for floor, cap in [(0.002, None), (0.003, None), (0.005, None),
                        (None, 0.015), (None, 0.02), (None, 0.025),
                        (0.003, 0.02), (0.005, 0.025)]:
        key = f"clamp_f{floor}_c{cap}"
        results[key] = summarize(f"clamp floor={floor} cap={cap}", run_one(cache, partial(
            simulate_rr_ladder_exit_v2, atr_fn=atr_fn, close_confirm=False,
            risk_pct_floor=floor, risk_pct_cap=cap)))

    print("\n" + "=" * 110)
    print("COMBINED -- close-confirm + best clamp")
    print("=" * 110)
    best_clamp = max((v for k, v in results.items() if k.startswith("clamp_")), key=lambda r: r["total"])
    best_clamp_key = next(k for k, v in results.items() if v is best_clamp)
    floor_s, cap_s = best_clamp_key.replace("clamp_f", "").split("_c")
    floor_v = None if floor_s == "None" else float(floor_s)
    cap_v = None if cap_s == "None" else float(cap_s)
    results["combined"] = summarize(f"close-confirm + clamp(f={floor_v},c={cap_v})", run_one(cache, partial(
        simulate_rr_ladder_exit_v2, atr_fn=atr_fn, close_confirm=True,
        risk_pct_floor=floor_v, risk_pct_cap=cap_v)))

    print("\n" + "=" * 110)
    print("RANKED BY TOTAL POINTS")
    print("=" * 110)
    ranked = sorted(results.values(), key=lambda r: r["total"], reverse=True)
    for r in ranked:
        boschltd = next((t for t in r["trades"] if t.symbol == "BOSCHLTD"), None)
        adanient = next((t for t in r["trades"] if t.symbol == "ADANIENT"), None)
        heromotoco1 = next((t for t in r["trades"] if t.symbol == "HEROMOTOCO" and t.date == "2026-09-01"), None)
        b = f"{boschltd.points:+.2f}" if boschltd else "n/a"
        a = f"{adanient.points:+.2f}" if adanient else "n/a"
        h = f"{heromotoco1.points:+.2f}" if heromotoco1 else "n/a"
        print(f"{r['label']:>40}  total={r['total']:+9.2f}  PF={r['pf']:6.2f}  win%={r['win_pct']:5.1f}  "
              f"BOSCHLTD={b:>9}  ADANIENT={a:>9}  HEROMOTOCO0901={h:>9}")

    best = ranked[0]
    print(f"\n{'='*110}\nFINAL WINNER: {best['label']} (total={best['total']:+.2f})\n{'='*110}")
    print(f"{'Date':<12}{'Symbol':<13}{'Side':<5}{'Entry TS':<9}{'Entry Px':>10}  {'Exit TS':<9}{'Exit Px':>10}  {'Reason':<22}{'Points':>10}")
    for t in best["trades"]:
        if t.entry_price is None:
            print(f"{t.date:<12}{t.symbol:<13}{t.side:<5} NO ENTRY")
        else:
            print(f"{t.date:<12}{t.symbol:<13}{t.side:<5}{t.entry_ts.strftime('%H:%M'):<9}{t.entry_price:>10.2f}  "
                  f"{t.exit_ts.strftime('%H:%M'):<9}{t.exit_price:>10.2f}  {t.reason:<22}{t.points:>+10.2f}")


asyncio.run(main())
