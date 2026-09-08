"""
scripts/oi_orb_sr_pool_and_trap_optimize.py -- 2026-09-08, direct user
follow-up: optimize the two exit families confirmed to have NO RSI/StochRSI
warmup dependency:

  1. HTF S&R Pool Lock, run STANDALONE (NOT raced against the cold-start
     HA+StochRSI baseline the way "F_htf_sr_pool_lock" in
     oi_orb_trend_capture_exit_backtest.py was -- that number was a hybrid,
     not a pure measurement of this mechanic alone). Sweeps htf_min x
     tol_pct x min_touches.
  2. Same-side trap + S1/R1 ladder (oi_orb_trap_target_full_htf_ltf_sweep.py),
     extended to a finer HTF x LTF grid beyond the original 1h/30min/15min x
     5/3/1min pass.

Same real VWAP-retest entries (find_entry, byte-identical across this whole
family of scripts) on the same 51-row shortlist dataset.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_sr_pool_and_trap_optimize.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from typing import Optional

sys.path.insert(0, ".")

from scripts.oi_orb_entry_mode_backtest import ROWS, SIDE, to_n_min_bars
from scripts.oi_orb_atr_chandelier_backtest import fetch_all
from scripts.oi_orb_trap_target_full_htf_ltf_sweep import find_entry, trap_target_exit
from scripts.oi_orb_trend_capture_exit_backtest import htf_sr_pool_lock_exit
from strategies.oi_orb_screener import screener

TOKEN = os.environ.get("UPSTOX_TOKEN", "")


@dataclass
class Trade:
    date: str
    symbol: str
    side: str
    entry_ts: object
    entry_price: Optional[float]
    exit_ts: object
    exit_price: Optional[float]
    reason: str

    @property
    def points(self) -> Optional[float]:
        if self.entry_price is None or self.exit_price is None:
            return None
        raw = self.exit_price - self.entry_price
        return raw if self.side == "CALL" else -raw


def summarize(trades):
    entered = [t for t in trades if t.entry_price is not None]
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    return {"entered": len(entered), "win_pct": win_pct, "pf": pf, "total": total,
            "avg": (total / len(entered)) if entered else 0.0}


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows (real Upstox 1-min NSE_EQ history)...")
    cache = await fetch_all()

    entries = {}
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            entries[(trade_date, symbol)] = None
            continue
        bars_1m, vol_by_ts, orb_h, orb_l = cached
        entries[(trade_date, symbol)] = find_entry(bars_1m, side, orb_h, orb_l, vol_by_ts)

    # ---- Sweep 1: HTF S&R Pool Lock, STANDALONE (no race vs baseline) ----
    print("\n=== Sweep 1: HTF S&R Pool Lock (standalone) ===")
    pool_results = {}
    HTF_GRID = [5, 10, 15, 20, 30]
    TOL_GRID = [0.03, 0.05, 0.08, 0.12]
    TOUCH_GRID = [2, 3]
    n = len(HTF_GRID) * len(TOL_GRID) * len(TOUCH_GRID)
    done = 0
    for htf_min in HTF_GRID:
        for tol_pct in TOL_GRID:
            for min_touches in TOUCH_GRID:
                trades = []
                for trade_date, symbol, side_bias in ROWS:
                    side = SIDE[side_bias]
                    cached = cache.get((trade_date, symbol))
                    entry = entries.get((trade_date, symbol))
                    if cached is None or entry is None:
                        continue
                    bars_1m, vol_by_ts, orb_h, orb_l = cached
                    entry_ts, entry_price = entry
                    exit_ts, exit_price, reason = htf_sr_pool_lock_exit(
                        bars_1m, side, entry_ts, entry_price,
                        htf_min=htf_min, tol_pct=tol_pct, min_touches=min_touches)
                    trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price, exit_ts, exit_price, reason))
                key = f"htf{htf_min}_tol{tol_pct}_touch{min_touches}"
                pool_results[key] = {"htf_min": htf_min, "tol_pct": tol_pct, "min_touches": min_touches,
                                      **summarize(trades),
                                      "trades": [(t.date, t.symbol, t.side, t.entry_ts.strftime("%H:%M"),
                                                  round(t.entry_price, 2), t.exit_ts.strftime("%H:%M"),
                                                  round(t.exit_price, 2), t.reason, round(t.points, 2))
                                                 for t in sorted(trades, key=lambda x: (x.date, x.symbol))]}
                done += 1
                print(f"  [{done}/{n}] {key}: PF={pool_results[key]['pf']:.2f} total={pool_results[key]['total']:+.2f}")

    # ---- Sweep 2: Same-side trap + S1/R1, finer HTF x LTF grid ----
    print("\n=== Sweep 2: Same-side trap + S1/R1 (finer grid) ===")
    trap_results = {}
    HTF_OPTIONS = [("1h", 60), ("30min", 30), ("15min", 15), ("10min", 10), ("5min", 5)]
    LTF_OPTIONS = [5, 3, 2, 1]
    n2 = len(HTF_OPTIONS) * len(LTF_OPTIONS)
    done = 0
    for htf_label, bucket_min in HTF_OPTIONS:
        for ltf_min in LTF_OPTIONS:
            trades = []
            for trade_date, symbol, side_bias in ROWS:
                side = SIDE[side_bias]
                cached = cache.get((trade_date, symbol))
                entry = entries.get((trade_date, symbol))
                if cached is None or entry is None:
                    continue
                bars_1m, vol_by_ts, orb_h, orb_l = cached
                entry_ts, entry_price = entry
                htf_bars = to_n_min_bars(bars_1m, bucket_min)
                ltf_bars = to_n_min_bars(bars_1m, ltf_min)
                exit_ts, exit_price, reason = trap_target_exit(
                    entry_ts, entry_price, side, bars_1m, htf_bars, ltf_bars, ltf_min)
                trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price, exit_ts, exit_price, reason))
            key = f"{htf_label}_{ltf_min}m"
            trap_results[key] = {"htf": htf_label, "ltf": ltf_min, **summarize(trades),
                                  "trades": [(t.date, t.symbol, t.side, t.entry_ts.strftime("%H:%M"),
                                              round(t.entry_price, 2), t.exit_ts.strftime("%H:%M"),
                                              round(t.exit_price, 2), t.reason, round(t.points, 2))
                                             for t in sorted(trades, key=lambda x: (x.date, x.symbol))]}
            done += 1
            print(f"  [{done}/{n2}] {key}: PF={trap_results[key]['pf']:.2f} total={trap_results[key]['total']:+.2f}")

    with open("data/oi_orb_sr_pool_and_trap_optimize_report.json", "w") as f:
        json.dump({"pool": pool_results, "trap": trap_results}, f)
    print("\nWrote data/oi_orb_sr_pool_and_trap_optimize_report.json")


if __name__ == "__main__":
    asyncio.run(main())
