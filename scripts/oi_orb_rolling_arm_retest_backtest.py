"""
scripts/oi_orb_rolling_arm_retest_backtest.py -- 2026-09-09, direct user
correction after the COFORGE/BSE/MUTHOOTFIN vwap_retest_historical
investigation: "rule 1 is correct" (arm-then-retest IS the right entry
sequence) but it must be (a) checked CONTINUOUSLY from the moment a stock
is scanned/shortlisted, not as a single one-shot historical replay done
once at shortlist time, and (b) BOUNDED to a rolling last-15x1min-candle
window -- an arm that never retests within that window should expire and
require a fresh arm, not stay armed indefinitely the way the live
_vwap_check_entry/_apply_historical_vwap_retest combination currently does.

Same real 46-row shortlist dataset, same exact exit mechanic (30-min HA
VWAP-close SL + 75min/3min multiday/intraday trap target + 1x
re-entry-after-SL), reused byte-for-byte from
oi_orb_full_live_logic_backtest.py -- ONLY the entry mechanic changes, to
screener.RollingVwapRetestTracker, called once per real 1-min bar
continuously from ORB_START (09:15) through ENTRY_WINDOW_END, matching
"start checking the instant the stock is scanned."

Reports THREE columns side by side against the SAME dataset:
  - "unbounded" (current live behaviour): find_first_entry from
    oi_orb_full_live_logic_backtest.py (one-shot ORB-window historical
    check, then unbounded-lifetime live arm).
  - "rolling15" (proposed fix): RollingVwapRetestTracker, continuous,
    15-min arm expiry.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_rolling_arm_retest_backtest.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

sys.path.insert(0, ".")

from scripts.oi_orb_entry_mode_backtest import (
    ROWS, SIDE, ORB_START, ENTRY_WINDOW_END, _key_range,
    resolve_eq_key, to_bars, volume_by_ts, compute_orb,
)
from scripts.oi_orb_full_live_logic_backtest import (
    _vwap_series_full_day, resolve_exit, find_first_entry, find_reentry,
    Leg, Trade, LOOKBACK_CALENDAR_DAYS, TRAP_HTF_MULTIDAY_MIN,
)
from scripts.oi_orb_same_side_trap_multiday_htf_sweep import _to_n_min_bars_dateaware
from strategies.oi_orb_screener import screener
from data_layer.historical_candles import fetch_upstox_range_1m

TOKEN = os.environ.get("UPSTOX_TOKEN", "")


def find_first_entry_rolling(bars_1m, side, vol_by_ts, vwap_state, window_min: float = 15.0) -> Optional[tuple]:
    """Continuous replay of screener.RollingVwapRetestTracker (the real
    class, not reimplemented) over every real 1-min bar from ORB_START
    through ENTRY_WINDOW_END -- "start checking the instant the stock is
    scanned," bounded to a rolling window rather than the current one-shot
    historical replay + unbounded live arm."""
    tracker = screener.RollingVwapRetestTracker(window_min=window_min)
    all_bars = _key_range(bars_1m, ORB_START, ENTRY_WINDOW_END)
    for b in all_bars:
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        vwap = vwap_state.current("SYM")
        if vwap is None:
            continue
        if tracker.check(side, b.ts, b.close, vwap):
            return b.ts, b.close
    return None


async def run_variant(cache, label: str, entry_fn) -> dict:
    trades = []
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l, htf_multiday = cached
        vwap_state = screener.VwapState()
        vwap_by_ts = _vwap_series_full_day(bars_1m, vol_by_ts)

        entry = entry_fn(bars_1m, side, orb_h, orb_l, vol_by_ts, vwap_state)
        if entry is None:
            continue
        entry_ts, entry_price = entry

        legs = []
        sl_reentry_used = False
        cur_entry_ts, cur_entry_price = entry_ts, entry_price
        while True:
            exit_ts, exit_price, reason = resolve_exit(cur_entry_ts, cur_entry_price, side, bars_1m,
                                                         vwap_by_ts, htf_multiday)
            legs.append(Leg(cur_entry_ts, cur_entry_price, exit_ts, exit_price, reason))
            if reason != "vwap_close_sl" or sl_reentry_used:
                break
            sl_reentry_used = True
            nxt = find_reentry(bars_1m, side, exit_ts, vwap_state)
            if nxt is None:
                break
            cur_entry_ts, cur_entry_price = nxt

        trades.append(Trade(trade_date, symbol, side, legs))

    wins = [t for t in trades if t.points > 0]
    losses = [t for t in trades if t.points <= 0]
    total = sum(t.points for t in trades)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(trades) * 100) if trades else 0.0
    max_loss = min((t.points for t in trades), default=0.0)
    sl_hits = sum(1 for t in trades for leg in t.legs if leg.reason == "vwap_close_sl")

    print(f"\n{'='*110}\n{label}\n{'='*110}")
    for t in sorted(trades, key=lambda x: (x.date, x.symbol)):
        leg_str = " -> ".join(f"{lg.entry_price:.2f}@{lg.entry_ts.strftime('%H:%M')}"
                               f"..{lg.exit_price:.2f}@{lg.exit_ts.strftime('%H:%M')}({lg.reason})"
                               for lg in t.legs)
        print(f"  {t.date} {t.symbol:<12} {t.side:<4} pts={t.points:+8.2f}  {leg_str}")
    print(f"\nentered={len(trades)}  win%={win_pct:5.1f}  PF={pf:6.2f}  total={total:+9.2f}  "
          f"sl_hits={sl_hits}  max_single_trade_loss={max_loss:+8.2f}")
    return {"label": label, "entered": len(trades), "win_pct": win_pct,
            "pf": (pf if pf != float("inf") else 9999.0), "total": total,
            "sl_hits": sl_hits, "max_loss": max_loss}


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching multi-day + today history (real Upstox 1-min NSE_EQ)...")
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
        start = d - timedelta(days=LOOKBACK_CALENDAR_DAYS)
        rows = await fetch_upstox_range_1m(eq_key, TOKEN, start, d)
        if not rows:
            cache[key] = None
            continue
        all_bars = to_bars(rows)
        today_bars = [b for b in all_bars if b.ts.date() == d]
        if not today_bars:
            cache[key] = None
            continue
        vol_by_ts = volume_by_ts([r for r in rows if r["ts"].startswith(trade_date)])
        orb = compute_orb(today_bars)
        if orb is None:
            cache[key] = None
            continue
        orb_h, orb_l = orb
        htf_multiday = _to_n_min_bars_dateaware(all_bars, TRAP_HTF_MULTIDAY_MIN)
        cache[key] = (today_bars, vol_by_ts, orb_h, orb_l, htf_multiday)
    n_ok = sum(1 for v in cache.values() if v)
    print(f"Fetched {n_ok}/{len(cache)} usable rows.")

    unbounded = await run_variant(
        cache, "UNBOUNDED (current live: one-shot historical replay + never-expiring arm)",
        find_first_entry)
    rolling15 = await run_variant(
        cache, "ROLLING15 (proposed fix: continuous from scan-time, 15-min arm expiry)",
        lambda bars, side, orb_h, orb_l, vol_by_ts, vwap_state:
            find_first_entry_rolling(bars, side, vol_by_ts, vwap_state))

    print(f"\n{'='*110}\nCOMPARISON\n{'='*110}")
    print(f"{'':45} {'entered':>8} {'win%':>7} {'PF':>8} {'total':>10} {'sl_hits':>8} {'max_loss':>10}")
    for r in (unbounded, rolling15):
        print(f"{r['label'][:45]:45} {r['entered']:>8} {r['win_pct']:>7.1f} {r['pf']:>8.2f} "
              f"{r['total']:>10.2f} {r['sl_hits']:>8} {r['max_loss']:>10.2f}")

    with open("data/oi_orb_rolling_arm_retest_backtest_report.json", "w") as f:
        json.dump({"unbounded": unbounded, "rolling15": rolling15}, f, indent=2)
    print("\nWrote data/oi_orb_rolling_arm_retest_backtest_report.json")


if __name__ == "__main__":
    asyncio.run(main())
