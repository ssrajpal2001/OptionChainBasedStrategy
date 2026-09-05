"""
scripts/oi_orb_loss_sr_check.py -- 2026-09-05, direct user spec: for every
real losing VWAP-retest trade (variant 3 backtest), check whether the
trade's own reversal point (its running favorable peak before price turned
back against it) coincides with a genuine support/resistance level, on a
15-MINUTE HTF resample (direct user correction: 1-min is too noisy for a
full-day S/R read) -- rather than just accepting the loss as unexplained
noise.

Two independent, already-validated S/R signals, reused rather than
reinvented (feedback_backtest_drive_real_class discipline):
  1. strategies.core.support_resistance.SupportResistanceCalculator
     -- the same R1/S1/R2/S2 ping-pong engine already live-driving CAG
     Straddle. Driven bar-by-bar on the stock's own 15-min HTF bars, fresh
     per day. At the reversal bar, checks the calculator's live R1/S1
     against the reversal price.
  2. strategies.liquidity_sweep.detector.find_swing_points -- 5-bar
     fractal pivot (pivot_left=pivot_right=2 on 15-min bars, since a full
     day only has ~25 bars -- 5 would leave too few confirmable pivots).
     Independent of the S&R state machine, catches simple prior swing
     highs/lows near the reversal point.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_loss_sr_check.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_range_1m
from scripts.oi_orb_entry_mode_backtest import compute_orb, resolve_eq_key, to_bars, to_n_min_bars
from strategies.core.support_resistance import SupportResistanceCalculator
from strategies.liquidity_sweep.detector import Bar as LSBar, find_swing_points

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
HTF_MIN = 15

# (date, symbol, side, entry_time, entry_price, min_move_pts_to_bother)
LOSING_TRADES = [
    ("2026-08-31", "ATHERENERG", "CALL", "09:25", 1666.20),
    ("2026-09-01", "FORCEMOT",   "CALL", "09:38", 17597.00),
    ("2026-09-01", "HEROMOTOCO", "CALL", "09:25", 5588.50),
    ("2026-09-02", "BSE",        "PUT",  "09:25", 3209.80),
    ("2026-09-02", "BOSCHLTD",   "PUT",  "11:39", 46885.00),
    ("2026-09-02", "EICHERMOT",  "PUT",  "13:37", 7613.50),
    ("2026-09-03", "GODREJCP",   "PUT",  "09:25", 868.40),
]


def find_reversal_point(bars_1m, side, entry_ts, entry_price):
    """The trade's own running favorable extreme (intrabar) before EOD --
    same definition used throughout this conversation's manual checks."""
    post = [b for b in bars_1m if b.ts.strftime("%H:%M") >= entry_ts]
    if not post:
        return None, None
    if side == "CALL":
        best = max(post, key=lambda b: b.high)
        return best.ts, best.high
    best = min(post, key=lambda b: b.low)
    return best.ts, best.low


def nearest_sr_at(calc, inst_key, reversal_price):
    st = calc.get_calculated_sr_state(inst_key)
    levels = st.get("sr_levels", {})
    candidates = []
    for name in ("S1", "R1", "S2", "R2"):
        lvl = levels.get(name)
        if not lvl:
            continue
        val = lvl.get("low") if name in ("S1", "S2") else lvl.get("high")
        if val:
            candidates.append((name, val, lvl.get("is_established", False)))
    if not candidates:
        return None
    name, val, established = min(candidates, key=lambda c: abs(c[1] - reversal_price))
    dist_pct = abs(val - reversal_price) / reversal_price * 100
    return name, val, established, dist_pct


def nearest_swing_at(bars_15m, reversal_ts, reversal_price):
    ls_bars = [LSBar(timestamp=b.ts, open=b.open, high=b.high, low=b.low, close=b.close) for b in bars_15m]
    swings = find_swing_points(ls_bars, pivot_left=2, pivot_right=2)
    prior = [s for s in swings if s.timestamp <= reversal_ts]
    if not prior:
        return None
    best = min(prior, key=lambda s: abs(s.price - reversal_price))
    dist_pct = abs(best.price - reversal_price) / reversal_price * 100
    return best.kind, best.price, best.timestamp, dist_pct


async def check_one(trade_date, symbol, side, entry_time, entry_price):
    eq_key = resolve_eq_key(symbol)
    if eq_key is None:
        print(f"{symbol}: no instrument key.")
        return
    d = date.fromisoformat(trade_date)
    rows = await fetch_upstox_range_1m(eq_key, TOKEN, d, d)
    if not rows:
        print(f"{symbol}: no data.")
        return
    bars_1m = to_bars(rows)
    bars_15m = to_n_min_bars(bars_1m, HTF_MIN)

    rev_ts, rev_price = find_reversal_point(bars_1m, side, entry_time, entry_price)
    if rev_ts is None:
        print(f"{symbol}: no post-entry bars.")
        return
    move_pct = ((rev_price - entry_price) / entry_price * 100) if side == "CALL" else \
               ((entry_price - rev_price) / entry_price * 100)

    calc = SupportResistanceCalculator()
    for b in bars_15m:
        if b.ts > rev_ts:
            break
        calc.process_straddle_candle(symbol, dict(timestamp=b.ts, high=b.high, low=b.low, close=b.close), silent=True)
    sr_hit = nearest_sr_at(calc, symbol, rev_price)
    swing_hit = nearest_swing_at(bars_15m, rev_ts, rev_price)

    print(f"\n{symbol} {side}  entry={entry_price:.2f}@{entry_time}")
    print(f"  Reversal point: {rev_price:.2f} @ {rev_ts.strftime('%H:%M')}  (favorable move {move_pct:+.2f}%)")
    if sr_hit:
        name, val, established, dist_pct = sr_hit
        tag = "ESTABLISHED" if established else "forming"
        print(f"  Nearest S&R level (15m calc): {name}={val:.2f} ({tag})  distance={dist_pct:.2f}%")
    else:
        print("  Nearest S&R level (15m calc): none computed yet at that point")
    if swing_hit:
        kind, price, ts, dist_pct = swing_hit
        ts_str = ts.strftime("%H:%M") if ts else "?"
        print(f"  Nearest swing pivot (15m, 2-bar): {kind} @ {price:.2f} ({ts_str})  distance={dist_pct:.2f}%")
    else:
        print("  Nearest swing pivot (15m, 2-bar): none confirmed yet at that point")


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    for trade_date, symbol, side, entry_time, entry_price in LOSING_TRADES:
        await check_one(trade_date, symbol, side, entry_time, entry_price)


asyncio.run(main())
