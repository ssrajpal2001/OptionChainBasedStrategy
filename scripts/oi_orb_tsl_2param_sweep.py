"""
scripts/oi_orb_tsl_2param_sweep.py -- 2026-09-05, direct user spec follow-up
to the single-% TSL sweep: decouple activation threshold from trail
distance (simulate_two_param_tsl_exit) -- activate_pct gates the mechanism
off until real momentum shows, trail_pct (tighter) only governs the lock
once active. Sweeps a 3x3 grid (trail always <= activate) against the same
cached real 1-min bars used by the single-% sweep, plus baseline (no TSL)
and the single-% sweep's own best-total candidate for reference.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_tsl_2param_sweep.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date
from functools import partial

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_range_1m
from scripts.oi_orb_entry_mode_backtest import (
    ROWS, SIDE, Trade, compute_orb, resolve_eq_key, simulate_fixed_sl_exit,
    simulate_pct_tsl_exit, simulate_two_param_tsl_exit,
    run_vwap_retest_immediate_if_historically_fulfilled,
    to_bars, volume_by_ts,
)

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
ACTIVATE_CANDIDATES = [0.5, 0.75, 1.0]
TRAIL_CANDIDATES = [0.15, 0.2, 0.3]


async def fetch_all():
    cache = {}
    for trade_date, symbol, side_bias in ROWS:
        key = (trade_date, symbol)
        if key in cache:
            continue
        eq_key = resolve_eq_key(symbol)
        if eq_key is None:
            cache[key] = None
            continue
        d = date.fromisoformat(trade_date)
        rows = await fetch_upstox_range_1m(eq_key, TOKEN, d, d)
        if not rows:
            cache[key] = None
            continue
        bars_1m = to_bars(rows)
        vol_by_ts = volume_by_ts(rows)
        orb = compute_orb(bars_1m)
        if orb is None:
            cache[key] = None
            continue
        orb_h, orb_l = orb
        cache[key] = (bars_1m, vol_by_ts, orb_h, orb_l)
    print(f"Fetched {sum(1 for v in cache.values() if v)} usable rows.")
    return cache


def run_one(cache, exit_fn):
    trades = []
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l = cached
        vwap_trades = run_vwap_retest_immediate_if_historically_fulfilled(
            bars_1m, side, orb_h, orb_l, vol_by_ts, exit_fn=exit_fn)
        for (entry_ts, entry_price, exit_ts, exit_price, reason) in vwap_trades:
            trades.append(Trade(trade_date, symbol, side, "vwap_retest", entry_ts, entry_price,
                                 exit_ts, exit_price, reason))
    return trades


def summarize(label, trades):
    entered = [t for t in trades if t.entry_price is not None]
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    avg = (total / len(entered)) if entered else 0.0
    tsl_hits = len([t for t in entered if t.reason.endswith("tsl_hit")])
    bosch = next((t for t in entered if t.symbol == "BOSCHLTD"), None)
    bosch_str = f"pts={bosch.points:+.2f}" if bosch else "n/a"
    print(f"{label:>22}  entered={len(entered):2d}  win%={win_pct:5.1f}  PF={pf:6.2f}  "
          f"total={total:+9.2f}  avg/trade={avg:+7.2f}  tsl_hits={tsl_hits:2d}  BOSCHLTD={bosch_str}")
    return {"label": label, "entered": len(entered), "win_pct": win_pct, "pf": pf,
            "total": total, "avg": avg, "tsl_hits": tsl_hits, "trades": trades}


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows once (cached for the whole grid)...")
    cache = await fetch_all()

    print("\n" + "=" * 110)
    print("2-PARAM TSL GRID -- activate_pct (gate) x trail_pct (lock distance once active)")
    print("=" * 110)
    results = []
    results.append(summarize("baseline(no TSL)", run_one(cache, simulate_fixed_sl_exit)))
    results.append(summarize("single-% best(2.0%)", run_one(cache, partial(simulate_pct_tsl_exit, activate_pct=2.0))))
    for act in ACTIVATE_CANDIDATES:
        for trail in TRAIL_CANDIDATES:
            if trail > act:
                continue
            label = f"act={act}%/trail={trail}%"
            exit_fn = partial(simulate_two_param_tsl_exit, activate_pct=act, trail_pct=trail)
            results.append(summarize(label, run_one(cache, exit_fn)))

    grid_only = results[2:]
    best_total = max(grid_only, key=lambda r: r["total"])
    print(f"\nBest total-points among 2-param grid: {best_total['label']} (total={best_total['total']:+.2f}, PF={best_total['pf']:.2f})")

    print("\n" + "=" * 110)
    print(f"FULL PER-TRADE DETAIL -- {best_total['label']}")
    print("=" * 110)
    for t in best_total["trades"]:
        if t.entry_price is None:
            print(f"{t.date} {t.symbol:<12} {t.side:<4} NO ENTRY")
        else:
            print(f"{t.date} {t.symbol:<12} {t.side:<4} entry={t.entry_price:9.2f}@{t.entry_ts.strftime('%H:%M')} "
                  f"exit={t.exit_price:9.2f}@{t.exit_ts.strftime('%H:%M')} ({t.reason})  pts={t.points:+8.2f}")


asyncio.run(main())
