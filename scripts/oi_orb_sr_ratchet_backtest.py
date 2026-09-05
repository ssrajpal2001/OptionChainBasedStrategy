"""
scripts/oi_orb_sr_ratchet_backtest.py -- 2026-09-05, direct user follow-up
after finding the fixed-ORB-SL / 1hr-no-new-high spec both too tight (SL)
and too arbitrary (a flat 1-hour clock as a "target"): "use the S&R logic
at a higher tf to check for target and stoploss."

Expert call (not a fresh invention): this codebase ALREADY has exactly that
mechanic, live, in strategies/oi_orb_screener/engine.py's
`_immediate_update_tsl_and_check_exit` -- faithfully mirrored here via
`simulate_hybrid_exit` (scripts/oi_orb_entry_mode_backtest.py), per this
codebase's own feedback_backtest_drive_real_class discipline. It was never
actually run/backtested before now -- only unit-referenced.

The mechanic:
  - Initial SL = the 09:15-09:25 ORB extreme (low for CALL, high for PUT) --
    protects the position from minute one, same starting point as the fixed-
    SL variant, so it's not "too tight from the first candle" the way a
    naive % or ATR guess could be.
  - In parallel, a REAL 15-minute S1(CALL)/R1(PUT) support/resistance ladder
    builds on the stock's own price structure (SupportResistanceCalculator --
    the exact ping-pong S&R engine already validated and live in D1TrapSRBook
    elsewhere in this codebase, not a new implementation).
  - The moment that ladder produces a genuinely ESTABLISHED level, the stop
    RATCHETS to it if -- and only if -- it's tighter (closer to price) than
    the current stop. Ratchet-only: it never loosens back to the ORB floor.
  - No fixed target at all. This directly answers "how do I fix stoploss AND
    target": the target IS the trailing structure -- once price makes real
    higher-timeframe structure in your favor, that structure itself becomes
    the new floor, and the trade just keeps running (protected, not capped)
    until either the ratchet catches a reversal or EOD square-off closes it.
    This removes both problems found earlier: a flat ORB-SL alone is too
    tight for normal opening noise; a flat 1-hour clock cuts winners that
    just haven't made a NEW high yet but haven't broken down either.

On top of the (unchanged) hybrid TSL mechanic, this script keeps the two
re-entry/discard rules already agreed in the prior round:
  - Re-entry allowed once after an SL/TSL-triggered exit (never after EOD).
  - An entry whose own price is already through the ORB extreme is discarded
    (this falls out naturally from the existing breach-cancel entry filter,
    unchanged from every other OI-ORB Screener backtest this week).

Same real dataset (last 5 trading days, real Upstox 1-min NSE_EQ history)
and same VWAP-retest/historical-immediate entry timing as every prior
OI-ORB backtest this week -- only the exit mechanic changes.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_sr_ratchet_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from typing import List, Optional

sys.path.insert(0, ".")

from scripts.oi_orb_entry_mode_backtest import (
    ROWS, SIDE, ORB_START, ORB_END, ENTRY_WINDOW_END, _key_range,
    to_n_min_bars, simulate_hybrid_exit,
)
from scripts.oi_orb_atr_chandelier_backtest import fetch_all
from strategies.oi_orb_screener import screener

TOKEN = os.environ.get("UPSTOX_TOKEN", "")


@dataclass
class Trade:
    date: str
    symbol: str
    side: str
    n: int
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


def run_sr_ratchet_strategy(bars_1m, side, orb_h, orb_l, vol_by_ts):
    bars_15m = to_n_min_bars(bars_1m, 15)

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
    breached = False   # breach-cancel, same as every other OI-ORB backtest

    if historically_fulfilled:
        b0 = entry_window[0]
        if entry_valid(b0.close):
            exit_ts, exit_price, reason = simulate_hybrid_exit(
                b0.ts, b0.close, side, orb_h, orb_l, bars_1m, bars_15m)
            trades.append((b0.ts, b0.close, exit_ts, exit_price, "immediate_historical:" + reason))
            in_position_until = exit_ts
            if reason != "hybrid_sl":
                day_done = True
        # else: discarded per rule 3 -- fall through to the live scan below

    idx = 0
    while idx < len(entry_window) and not day_done and len(trades) < 2:
        b = entry_window[idx]
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        vwap = vwap_state.current("SYM")

        if side == "CALL" and b.low <= orb_l:
            breached = True
        elif side == "PUT" and b.high >= orb_h:
            breached = True

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
        if breached or not entry_valid(b.close):
            idx += 1
            continue   # rule 3 / breach-cancel: discard, keep scanning

        entry_ts, entry_price = b.ts, b.close
        exit_ts, exit_price, reason = simulate_hybrid_exit(
            entry_ts, entry_price, side, orb_h, orb_l, bars_1m, bars_15m)
        trades.append((entry_ts, entry_price, exit_ts, exit_price, reason))
        in_position_until = exit_ts
        if reason != "hybrid_sl":
            day_done = True
        idx += 1

    numbered = [(t[0], t[1], t[2], t[3], t[4], i + 1) for i, t in enumerate(trades)]
    return numbered


def run_all(cache):
    trades = []
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l = cached
        for (entry_ts, entry_price, exit_ts, exit_price, reason, n) in run_sr_ratchet_strategy(
                bars_1m, side, orb_h, orb_l, vol_by_ts):
            trades.append(Trade(trade_date, symbol, side, n, entry_ts, entry_price, exit_ts, exit_price, reason))
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
    print("15-MIN S&R RATCHET (ORB floor -> S1/R1 ratchet, NO fixed target) -- trade log")
    print("=" * 118)
    for t in sorted(trades, key=lambda x: (x.date, x.symbol, x.entry_ts or 0)):
        if t.entry_price is None:
            print(f"  {t.date} {t.symbol:<12} {t.side:<4} NO ENTRY")
            continue
        print(f"  {t.date} {t.symbol:<12} {t.side:<4} #{t.n} entry={t.entry_price:9.2f}@{t.entry_ts.strftime('%H:%M')} "
              f"exit={t.exit_price:9.2f}@{t.exit_ts.strftime('%H:%M')} ({t.reason})  pts={t.points:+8.2f}")

    summarize(trades)


asyncio.run(main())
