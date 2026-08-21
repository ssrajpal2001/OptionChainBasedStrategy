"""
scripts/liquidity_trap_nifty_tf_and_trend_sweep.py -- the SENSEX tf/trend-
filter optimization pass (scripts/liquidity_trap_tf_and_trend_sweep.py,
2026-08-21) repeated on real NIFTY spot data. The user explicitly asked for
NIFTY to get the same optimization treatment SENSEX already got -- the prior
sweep only ever ran against scratch_liquidity_trap_1m_cache.json, which is
SENSEX-only (BSE_INDEX|SENSEX, confirmed via direct inspection: every row's
`close` sits in the 76k-81k SENSEX range, not NIFTY's ~24k range).

Same multi-ref, skip-if-blocked base variant. Same three parts (timeframe
sweep, HTF trend filter, combined). Same limitation carried over: VWAP /
change-in-OI / max-pain / open-interest remain impossible to backtest
(Upstox's historical index-candle API hardcodes volume=0, oi=0 for spot
indices) -- only timeframe + a pure-price HTF-SMA trend filter are testable
here, identically to the SENSEX pass.

Usage: python scripts/liquidity_trap_nifty_tf_and_trend_sweep.py <upstox_token>
(token only needed the first time -- caches to scratch_liquidity_trap_nifty_1m_cache.json
for reuse, same convention as the SENSEX script's scratch_liquidity_trap_1m_cache.json)
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer.historical_candles import fetch_upstox_range_1m
from scripts.liquidity_trap_multiref_backtest import Bar, Trade, by_day, swing_points
from scripts.liquidity_trap_tf_and_trend_sweep import (
    resample, _trend_sma, _trend_as_of, run_day_multiref_filtered,
)

TOKEN = sys.argv[1] if len(sys.argv) > 1 else ""
INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
LOT_SIZE = 75          # NIFTY lot size (vs SENSEX's 20 -- the source sweep's
                        # _summarize() hardcodes *20; reimplemented below with *75)
CACHE_PATH = "scratch_liquidity_trap_nifty_1m_cache.json"


def _summarize(trades: List[Trade]) -> dict:
    n = len(trades)
    wins = sum(1 for t in trades if t.pnl_pts > 0)
    win_pct = (wins / n * 100.0) if n else 0.0
    gross_win_lot = sum(t.pnl_lotpts for t in trades if t.pnl_lotpts > 0)
    gross_loss_lot = -sum(t.pnl_lotpts for t in trades if t.pnl_lotpts < 0)
    pf_lot = (gross_win_lot / gross_loss_lot) if gross_loss_lot > 0 else float("inf")
    net_lot = sum(t.pnl_lotpts for t in trades)
    return dict(n=n, win_pct=win_pct, pf_lot=pf_lot, net_rupees=net_lot * LOT_SIZE)


async def load_1m_bars() -> List[Bar]:
    import json, os
    if os.path.exists(CACHE_PATH):
        print(f"Loading cached 1-min bars from {CACHE_PATH} ...", flush=True)
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            rows = json.load(f)
        print(f"Loaded {len(rows)} cached 1-min candles.", flush=True)
    else:
        if not TOKEN:
            print(f"No cache at {CACHE_PATH} and no token given.\n"
                  f"Usage: python scripts/liquidity_trap_nifty_tf_and_trend_sweep.py <upstox_token>")
            return []
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=365)
        print(f"Fetching 1-min NIFTY spot ({INSTRUMENT_KEY}) from {start} to {end} ...", flush=True)
        rows = await fetch_upstox_range_1m(INSTRUMENT_KEY, TOKEN, start, end)
        rows = [dict(ts=r["ts"], open=r["open"], high=r["high"], low=r["low"],
                     close=r["close"], volume=r.get("volume", 0), oi=r.get("oi", 0)) for r in rows]
        if rows:
            with open(CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(rows, f)
            print(f"Cached to {CACHE_PATH} for reuse.", flush=True)
    bars = [Bar(ts=datetime.fromisoformat(r["ts"]), open=r["open"], high=r["high"],
                low=r["low"], close=r["close"]) for r in rows]
    bars.sort(key=lambda b: b.ts)
    return bars


async def main():
    bars_1m = await load_1m_bars()
    if not bars_1m:
        return
    days_1m = by_day(bars_1m)
    print(f"Real NIFTY data: {len(days_1m)} trading days, {len(bars_1m)} 1m bars.\n")

    print("=" * 100)
    print("PART 1 -- TIMEFRAME SWEEP (ref TF x confirm TF), multi-ref skip-if-blocked, target_mode=liquidity")
    print("=" * 100)
    header = f"{'REF_TF':>7} {'CONFIRM_TF':>10} {'trades':>7} {'win%':>7} {'PF(lot)':>8} {'net_Rs':>12}"
    print(header)
    print("-" * len(header))
    baseline_15_5 = None
    for ref_tf in (10, 15, 20, 30):
        bars_ref = resample(bars_1m, ref_tf)
        swings_ref = swing_points(bars_ref, pivot=2)
        for confirm_tf in (3, 5, 10):
            if confirm_tf >= ref_tf:
                continue
            bars_confirm = resample(bars_1m, confirm_tf)
            trades: List[Trade] = []
            for day in sorted(days_1m.keys()):
                trades.extend(run_day_multiref_filtered(
                    day, days_1m[day], bars_1m, bars_ref, bars_confirm, swings_ref,
                    "liquidity"))
            s = _summarize(trades)
            if ref_tf == 15 and confirm_tf == 5:
                baseline_15_5 = s
            print(f"{ref_tf:>7} {confirm_tf:>10} {s['n']:>7} {s['win_pct']:>6.1f}% "
                  f"{s['pf_lot']:>8.2f} {s['net_rupees']:>12.2f}")

    print("\n" + "=" * 100)
    print("PART 2 -- HTF TREND FILTER (baseline ref=15m/confirm=5m, only trade WITH the trend)")
    print("=" * 100)
    bars_ref_15 = resample(bars_1m, 15)
    bars_confirm_5 = resample(bars_1m, 5)
    swings_ref_15 = swing_points(bars_ref_15, pivot=2)
    header2 = f"{'TREND_TF':>9} {'SMA_LEN':>8} {'trades':>7} {'win%':>7} {'PF(lot)':>8} {'net_Rs':>12}"
    print(header2)
    print("-" * len(header2))
    for trend_tf in (60, 75, 120):
        bars_trend = resample(bars_1m, trend_tf)
        for sma_len in (10, 20):
            trend_by_ts = _trend_sma(bars_trend, sma_len)
            trades = []
            for day in sorted(days_1m.keys()):
                trades.extend(run_day_multiref_filtered(
                    day, days_1m[day], bars_1m, bars_ref_15, bars_confirm_5, swings_ref_15,
                    "liquidity", trend_by_ts=trend_by_ts, trend_bars=bars_trend))
            s = _summarize(trades)
            print(f"{trend_tf:>9} {sma_len:>8} {s['n']:>7} {s['win_pct']:>6.1f}% "
                  f"{s['pf_lot']:>8.2f} {s['net_rupees']:>12.2f}")

    if baseline_15_5:
        print(f"\n(For reference, no-filter baseline ref=15m/confirm=5m: n={baseline_15_5['n']}, "
              f"win%={baseline_15_5['win_pct']:.1f}, PF(lot)={baseline_15_5['pf_lot']:.2f}, "
              f"net=Rs{baseline_15_5['net_rupees']:.2f})")

    print("\n" + "=" * 100)
    print("PART 3 -- COMBINED: 60m/SMA10 trend filter stacked on top of the best timeframe combos")
    print("=" * 100)
    header3 = f"{'REF_TF':>7} {'CONFIRM_TF':>10} {'trades':>7} {'win%':>7} {'PF(lot)':>8} {'net_Rs':>12}"
    print(header3)
    print("-" * len(header3))
    bars_trend_60 = resample(bars_1m, 60)
    trend_by_ts_60_10 = _trend_sma(bars_trend_60, 10)
    for ref_tf, confirm_tf in ((15, 3), (15, 5), (20, 3), (30, 3), (30, 5)):
        bars_ref = resample(bars_1m, ref_tf)
        swings_ref = swing_points(bars_ref, pivot=2)
        bars_confirm = resample(bars_1m, confirm_tf)
        trades = []
        for day in sorted(days_1m.keys()):
            trades.extend(run_day_multiref_filtered(
                day, days_1m[day], bars_1m, bars_ref, bars_confirm, swings_ref,
                "liquidity", trend_by_ts=trend_by_ts_60_10, trend_bars=bars_trend_60))
        s = _summarize(trades)
        print(f"{ref_tf:>7} {confirm_tf:>10} {s['n']:>7} {s['win_pct']:>6.1f}% "
              f"{s['pf_lot']:>8.2f} {s['net_rupees']:>12.2f}")


if __name__ == "__main__":
    asyncio.run(main())
