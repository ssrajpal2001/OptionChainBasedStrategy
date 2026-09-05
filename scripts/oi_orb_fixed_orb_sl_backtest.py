"""
scripts/oi_orb_fixed_orb_sl_backtest.py -- 2026-09-05, direct user spec, a
SIMPLER exit/re-entry scheme to compare against the frozen ATR R:R-ladder
config (scripts/oi_orb_atr_chandelier_backtest.py):

  1. SL = fixed at the 09:15-09:25 opening-range LOW for a long (CALL) --
     never ratchets, no ATR, plain and simple.
  2. Re-entry allowed after an SL-triggered exit, but only ONCE (max 2
     entries/day for a given side).
  3. If the entry price itself is already below the 09:15-09:25 low, that
     entry is discarded (rejected) -- keeps scanning for a later, valid
     touch-back instead of taking a structurally-broken entry.
  4. "Target": at T+1 hour after entry, if price has NOT yet made a new
     high beyond the opening-range HIGH, exit at market. If it already has,
     keep holding (subject only to the SL above) until EOD.
  5. No re-entry after a target-reached (or EOD) exit -- only an SL exit
     unlocks the one allowed re-entry.

Entry timing itself is UNCHANGED from the already-validated VWAP-retest
mechanic (arm away from VWAP, fire on touch-back, historical-immediate at
09:25 if already resolved in the 09:15-09:25 window) -- this pass only
replaces the SL/target/re-entry rules, not how a signal is timed.

Same real dataset as every other OI-ORB backtest this week: the last-5-
trading-day shortlist in scripts/oi_orb_entry_mode_backtest.py's ROWS,
real Upstox 1-min NSE_EQ history.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_fixed_orb_sl_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from datetime import timedelta
from typing import List, Optional

sys.path.insert(0, ".")

from scripts.oi_orb_entry_mode_backtest import (
    ROWS, SIDE, ORB_START, ORB_END, ENTRY_WINDOW_END, _key_range,
    resolve_eq_key, to_bars, volume_by_ts, compute_orb,
)
from scripts.oi_orb_atr_chandelier_backtest import fetch_all
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


def new_sl_target_exit(entry_ts, entry_price, side, orb_h, orb_l, bars_1m):
    """Rule 1 (fixed SL at the ORB extreme) + Rule 4 (1-hour "did we make a
    new high beyond the ORB extreme yet" check, else exit)."""
    ref = orb_l if side == "CALL" else orb_h
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]
    one_hour_ts = entry_ts + timedelta(hours=1)
    made_new_extreme = False
    checked_1hr = False

    for b in post_entry:
        breach = (b.low <= ref) if side == "CALL" else (b.high >= ref)
        if breach:
            return b.ts, ref, "fixed_orb_sl"

        if side == "CALL" and b.high > orb_h:
            made_new_extreme = True
        elif side == "PUT" and b.low < orb_l:
            made_new_extreme = True

        if not checked_1hr and b.ts >= one_hour_ts:
            checked_1hr = True
            if not made_new_extreme:
                return b.ts, b.close, "no_new_high_1hr_exit"

    if post_entry:
        last = post_entry[-1]
        return last.ts, last.close, "eod_close"
    return entry_ts, entry_price, "no_data_after_entry"


def run_new_strategy(bars_1m, side, orb_h, orb_l, vol_by_ts):
    orb_bars = _key_range(bars_1m, ORB_START, ORB_END)
    vwap_state = screener.VwapState()
    armed = False
    historically_fulfilled = False
    for b in orb_bars:
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        vwap = vwap_state.current("SYM")
        if vwap is None:
            continue
        armed, fire = screener.check_vwap_retest_entry(side, b.close, vwap, armed, 0.15)
        if fire:
            historically_fulfilled = True
            break

    entry_window = _key_range(bars_1m, ORB_END, ENTRY_WINDOW_END)
    if not entry_window:
        return []

    def entry_valid(price):
        return (price >= orb_l) if side == "CALL" else (price <= orb_h)

    trades: List[tuple] = []
    in_position_until = None
    day_done = False

    if historically_fulfilled:
        b0 = entry_window[0]
        if entry_valid(b0.close):
            exit_ts, exit_price, reason = new_sl_target_exit(b0.ts, b0.close, side, orb_h, orb_l, bars_1m)
            trades.append((b0.ts, b0.close, exit_ts, exit_price, "immediate_historical:" + reason))
            in_position_until = exit_ts
            if reason != "fixed_orb_sl":
                day_done = True
        # else: rule 3 -- discarded, fall through to the live scan below

    idx = 0
    while idx < len(entry_window) and not day_done and len(trades) < 2:
        b = entry_window[idx]
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        vwap = vwap_state.current("SYM")

        if in_position_until is not None and b.ts <= in_position_until:
            if vwap is not None:
                armed, _fire = screener.check_vwap_retest_entry(side, b.close, vwap, armed, 0.15)
            idx += 1
            continue

        if vwap is None:
            idx += 1
            continue
        armed, fire = screener.check_vwap_retest_entry(side, b.close, vwap, armed, 0.15)
        if not fire:
            idx += 1
            continue
        armed = False
        if not entry_valid(b.close):
            idx += 1
            continue   # rule 3: discard, keep scanning

        entry_ts, entry_price = b.ts, b.close
        exit_ts, exit_price, reason = new_sl_target_exit(entry_ts, entry_price, side, orb_h, orb_l, bars_1m)
        trades.append((entry_ts, entry_price, exit_ts, exit_price, reason))
        in_position_until = exit_ts
        if reason != "fixed_orb_sl":
            day_done = True
        idx += 1

    return trades


def run_all(cache):
    trades = []
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l = cached
        for (entry_ts, entry_price, exit_ts, exit_price, reason) in run_new_strategy(
                bars_1m, side, orb_h, orb_l, vol_by_ts):
            trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price, exit_ts, exit_price, reason))
    return trades


def summarize(trades):
    entered = [t for t in trades if t.entry_price is not None]
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    print(f"\nentered={len(entered)}  win%={win_pct:.1f}  PF={pf:.2f}  total={total:+.2f} pts  "
          f"avg/trade={(total/len(entered) if entered else 0):+.2f}")
    return {"trades": trades, "total": total, "pf": pf, "win_pct": win_pct, "entered": len(entered)}


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows (real Upstox 1-min NSE_EQ history)...")
    cache = await fetch_all()

    trades = run_all(cache)

    print("\n" + "=" * 118)
    print("FIXED-ORB-SL / 1HR-NO-NEW-HIGH-TARGET / ONE-REENTRY-AFTER-SL -- trade log")
    print("=" * 118)
    for t in sorted(trades, key=lambda x: (x.date, x.symbol, x.entry_ts or 0)):
        if t.entry_price is None:
            print(f"  {t.date} {t.symbol:<12} {t.side:<4} NO ENTRY")
            continue
        print(f"  {t.date} {t.symbol:<12} {t.side:<4} entry={t.entry_price:9.2f}@{t.entry_ts.strftime('%H:%M')} "
              f"exit={t.exit_price:9.2f}@{t.exit_ts.strftime('%H:%M')} ({t.reason})  pts={t.points:+8.2f}")

    summarize(trades)


asyncio.run(main())
