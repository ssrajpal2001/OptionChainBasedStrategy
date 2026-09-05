"""
scripts/oi_orb_tsl_pct_sweep.py -- 2026-09-05, direct user spec: "threshold
system where when stock moves certain % then we move SL above and trail SL
accordingly ... need to do optimisation what % should activate the TSL
logic."

Fetches each real (date, symbol) row's 1-min bars ONCE (cached in memory),
then reruns the identical variant-3 entry logic (run_vwap_retest_immediate_
if_historically_fulfilled, which itself falls back to run_vwap_retest_no_
reentry_breach_cancel) across several candidate activate_pct thresholds for
simulate_pct_tsl_exit, so only the EXIT mechanic varies between runs --
isolates the TSL activation/trail % as the only variable, same isolation
discipline as the original 3-mode entry comparison.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_tsl_pct_sweep.py
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
    simulate_pct_tsl_exit, run_vwap_retest_immediate_if_historically_fulfilled,
    to_bars, volume_by_ts,
)

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
CANDIDATE_PCTS = [0.25, 0.5, 0.75, 1.0, 1.5, 2.0]


async def fetch_all():
    """One real fetch per (date, symbol) -- cached, reused across every
    threshold in the sweep so the sweep itself makes zero extra API calls."""
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
        print(f"  fetched {trade_date} {symbol} ({len(bars_1m)} bars)")
    return cache


def run_one_threshold(cache, activate_pct):
    exit_fn = simulate_fixed_sl_exit if activate_pct is None else \
        partial(simulate_pct_tsl_exit, activate_pct=activate_pct)
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
    print(f"{label:>18}  entered={len(entered):2d}  win%={win_pct:5.1f}  PF={pf:6.2f}  "
          f"total={total:+9.2f}  avg/trade={avg:+7.2f}  tsl_hits={tsl_hits}")
    return {"label": label, "entered": len(entered), "win_pct": win_pct, "pf": pf,
            "total": total, "avg": avg, "tsl_hits": tsl_hits, "trades": trades}


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows once (cached for the whole sweep)...")
    cache = await fetch_all()

    print("\n" + "=" * 100)
    print("TSL ACTIVATE% SWEEP -- baseline (no TSL, fixed SL only) vs candidate thresholds")
    print("=" * 100)
    results = []
    results.append(summarize("baseline(no TSL)", run_one_threshold(cache, None)))
    for pct in CANDIDATE_PCTS:
        results.append(summarize(f"activate={pct}%", run_one_threshold(cache, pct)))

    best = max(results[1:], key=lambda r: r["pf"])
    print(f"\nBest PF among TSL candidates: {best['label']} (PF={best['pf']:.2f}, total={best['total']:+.2f})")

    # Detail for baseline and the best candidate, side by side for BOSCHLTD specifically
    print("\n" + "=" * 100)
    print(f"BOSCHLTD detail -- baseline vs {best['label']}")
    print("=" * 100)
    for r in [results[0], best]:
        for t in r["trades"]:
            if t.symbol == "BOSCHLTD":
                print(f"  {r['label']:>18}  entry={t.entry_price:.2f}@{t.entry_ts.strftime('%H:%M') if t.entry_ts else '-'}  "
                      f"exit={t.exit_price:.2f}@{t.exit_ts.strftime('%H:%M') if t.exit_ts else '-'} ({t.reason})  pts={t.points:+.2f}")

    print("\n" + "=" * 100)
    print(f"FULL PER-TRADE DETAIL -- {best['label']}")
    print("=" * 100)
    for t in best["trades"]:
        if t.entry_price is None:
            print(f"{t.date} {t.symbol:<12} {t.side:<4} NO ENTRY")
        else:
            print(f"{t.date} {t.symbol:<12} {t.side:<4} entry={t.entry_price:9.2f}@{t.entry_ts.strftime('%H:%M')} "
                  f"exit={t.exit_price:9.2f}@{t.exit_ts.strftime('%H:%M')} ({t.reason})  pts={t.points:+8.2f}")


asyncio.run(main())
