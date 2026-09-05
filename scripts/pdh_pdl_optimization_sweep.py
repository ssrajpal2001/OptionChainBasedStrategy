"""
scripts/pdh_pdl_optimization_sweep.py — parameter sweep over the PDH-PDL
mechanic (2026-08-28, direct user request: "do an optimisation on all points
and provide the best result").

Reuses the already-cached 2-year 1-min NIFTY spot data
(scratch_pdh_pdl_2y_1m_cache.json) -- no new API calls needed. Sweeps:

  - retest_tol_pts: how close price must come back to PDH/PDL to count as a
    "retest" -- 0 (exact touch, the original spec) through a widening band.
  - sl_buffer_pts: the fixed buffer added to S1's low (long) / R1's high
    (short) for both the initial SL and every TSL ratchet.
  - breach_phases: "narrow" (S2_TRACKING/R2_TRACKING only, the original
    ping-pong-promotion-only rule) vs "broad" (also allows a straight
    Directional Flip through the current S1/R1, per the direct user chart
    review on 2026-08-28).

Ranks by Profit Factor among combos with at least MIN_TRADES trades (a combo
with a handful of lucky trades and a huge PF is not a real edge -- same
"too small a sample" caveat every other backtest in this codebase applies).
Reports the full leaderboard plus the single best combo's own trade log.
"""
from __future__ import annotations

import json
import sys
from typing import List

sys.path.insert(0, ".")

from scripts.pdh_pdl_backtest import (
    Bar, Trade, LOT_SIZE, group_by_day, daily_high_low, to_3min_bars, run_day,
)

CACHE_PATH = "scratch_pdh_pdl_2y_1m_cache.json"
MIN_TRADES = 30

RETEST_TOL_GRID = [0.0, 2.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 40.0, 50.0]
SL_BUFFER_GRID = [2.0, 5.0, 10.0, 15.0, 20.0]
BREACH_RULE_GRID = {
    "narrow": (("S2_TRACKING", "R2_TRACKING"), ("S2_TRACKING", "R2_TRACKING")),
    "broad": (("S1_TRACKING", "S2_TRACKING", "R2_TRACKING"),
              ("R1_TRACKING", "S2_TRACKING", "R2_TRACKING")),
}


def load_1m_bars() -> List[Bar]:
    from datetime import datetime
    with open(CACHE_PATH, "r", encoding="utf-8") as f:
        rows = json.load(f)
    bars = [Bar(ts=datetime.fromisoformat(r["ts"]), open=r["open"], high=r["high"],
                low=r["low"], close=r["close"]) for r in rows]
    bars.sort(key=lambda b: b.ts)
    return bars


def stats_for(trades: List[Trade]) -> dict:
    n = len(trades)
    if n == 0:
        return {"n": 0, "win_pct": 0.0, "pf": 0.0, "net": 0.0, "max_dd": 0.0}
    wins = [t for t in trades if t.pnl_pts > 0]
    losses = [t for t in trades if t.pnl_pts <= 0]
    gross_profit = sum(t.pnl_rs for t in wins)
    gross_loss = -sum(t.pnl_rs for t in losses)
    net = sum(t.pnl_rs for t in trades)
    win_pct = len(wins) / n * 100.0
    pf = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")
    equity = peak = max_dd = 0.0
    for t in trades:
        equity += t.pnl_rs
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return {"n": n, "win_pct": round(win_pct, 1), "pf": round(pf, 3) if pf != float("inf") else pf,
            "net": round(net, 2), "max_dd": round(max_dd, 2)}


def main():
    print("Loading cached 1-min bars ...", flush=True)
    bars_1m = load_1m_bars()
    days = group_by_day(bars_1m)
    sorted_dates = sorted(days.keys())
    print(f"{len(sorted_dates)} trading days loaded.", flush=True)

    # Pre-aggregate 3-min bars + PDH/PDL once per day -- reused across every combo.
    day_data = []
    prev_high = prev_low = None
    for d in sorted_dates:
        day_bars = sorted(days[d], key=lambda b: b.ts)
        if prev_high is not None:
            day_3m = to_3min_bars(day_bars)
            day_data.append((prev_high, prev_low, day_bars, day_3m))
        prev_high, prev_low = daily_high_low(day_bars)
    print(f"{len(day_data)} days ready for sweeping.\n", flush=True)

    results = []
    combo_count = len(RETEST_TOL_GRID) * len(SL_BUFFER_GRID) * len(BREACH_RULE_GRID)
    done = 0
    best_trades_by_combo = {}
    for tol in RETEST_TOL_GRID:
        for buf in SL_BUFFER_GRID:
            for rule_name, (long_phases, short_phases) in BREACH_RULE_GRID.items():
                all_trades: List[Trade] = []
                for prev_high, prev_low, day_bars, day_3m in day_data:
                    all_trades.extend(run_day(
                        prev_high, prev_low, day_bars, day_3m, inst_key="NIFTY",
                        retest_tol_pts=tol, sl_buffer_pts=buf,
                        long_breach_phases=long_phases, short_breach_phases=short_phases,
                    ))
                s = stats_for(all_trades)
                s.update({"retest_tol_pts": tol, "sl_buffer_pts": buf, "breach_rule": rule_name})
                results.append(s)
                best_trades_by_combo[(tol, buf, rule_name)] = all_trades
                done += 1
                print(f"[{done}/{combo_count}] tol={tol} buf={buf} rule={rule_name} "
                      f"-> n={s['n']} win%={s['win_pct']} PF={s['pf']} net=Rs{s['net']:,.0f} "
                      f"maxDD=Rs{s['max_dd']:,.0f}", flush=True)

    print("\n=== LEADERBOARD (min {} trades, ranked by PF) ===".format(MIN_TRADES), flush=True)
    qualifying = [r for r in results if r["n"] >= MIN_TRADES]
    qualifying.sort(key=lambda r: (r["pf"] if r["pf"] != float("inf") else 999, r["net"]), reverse=True)
    for r in qualifying[:15]:
        print(f"tol={r['retest_tol_pts']:>5} buf={r['sl_buffer_pts']:>5} rule={r['breach_rule']:>6} "
              f"| n={r['n']:>4} win%={r['win_pct']:>5} PF={r['pf']:>6} "
              f"net=Rs{r['net']:>12,.0f} maxDD=Rs{r['max_dd']:>12,.0f}", flush=True)

    with open("scratch_pdh_pdl_sweep_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=lambda x: None if x == float("inf") else x)
    print("\nFull sweep results written to scratch_pdh_pdl_sweep_results.json", flush=True)

    if qualifying:
        best = qualifying[0]
        key = (best["retest_tol_pts"], best["sl_buffer_pts"], best["breach_rule"])
        best_trades = best_trades_by_combo[key]
        with open("scratch_pdh_pdl_best_trades.json", "w", encoding="utf-8") as f:
            json.dump([{
                "direction": t.direction, "entry_ts": t.entry_ts.isoformat(),
                "entry_price": t.entry_price, "initial_sl": t.initial_sl,
                "exit_ts": t.exit_ts.isoformat() if t.exit_ts else None,
                "exit_price": t.exit_price, "final_sl": t.final_sl,
                "exit_reason": t.exit_reason, "pnl_pts": t.pnl_pts, "pnl_rs": t.pnl_rs,
            } for t in best_trades], f, indent=2)
        print(f"\nBEST: tol={best['retest_tol_pts']} buf={best['sl_buffer_pts']} "
              f"rule={best['breach_rule']} -> PF={best['pf']} net=Rs{best['net']:,.2f} "
              f"maxDD=Rs{best['max_dd']:,.2f} n={best['n']}", flush=True)
        print("Best combo's trade log written to scratch_pdh_pdl_best_trades.json", flush=True)
    else:
        print("\nNo combo reached the minimum trade count.", flush=True)


if __name__ == "__main__":
    main()
