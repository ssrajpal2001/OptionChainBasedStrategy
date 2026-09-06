"""
scripts/oi_orb_stoch_rsi_backtest.py -- 2026-09-05, direct user spec: a
THIRD, fully standalone entry+exit method (independent of the VWAP/trap
pipeline) -- StochRSI, swept across timeframe and parameter values, same
"optimization technique" pattern as the HTF/trap/LTF sweep.

StochRSI (classic definition, Chande/Kroll): RSI(rsi_period) on N-min bar
closes, then Stochastic-of-RSI: %K = (RSI - min(RSI, stoch_period)) /
(max(RSI, stoch_period) - min(RSI, stoch_period)) * 100, %D = SMA(%K, 3).
Expanding-window-safe (same warm-up idiom this codebase already uses for
ATR elsewhere -- returns None until enough bars exist, never guesses).

Entry (CALL): %K crosses above %D on a bar where the PRIOR bar's %K was
below the oversold threshold (classic "bullish StochRSI cross out of
oversold"). Entry (PUT), mirrored: %K crosses below %D with the prior
bar's %K above the overbought threshold.

Exit (CALL): %K crosses below %D with the prior bar's %K above
overbought (bearish reversal) -- pure signal-based exit, matching this
week's running theme of preferring structure/indicator-based exits over
arbitrary fixed levels. No SL. Exit (PUT), mirrored. Falls back to EOD if
no reversal signal ever fires.

Grid swept: tf in {5min, 15min}, rsi_period in {9, 14}, stoch_period in
{9, 14} -- smoothing (3) and thresholds (20/80) held at their standard,
universally-cited defaults (not swept, to keep the grid from ballooning
into over-fitting territory on a 51-signal week per the quant-skeptic
discipline already established this session). 8 combos x (CALL+PUT).

Reuses the same real dataset, same entry-window/day-boundary constants,
and the same per-day OI-ORB shortlist rows as every other backtest this
week -- this is genuinely a fresh, independent entry+exit method, not a
variant of the VWAP-retest/trap pipeline.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_stoch_rsi_backtest.py > out.json
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from typing import List, Optional

sys.path.insert(0, ".")

from scripts.oi_orb_entry_mode_backtest import ROWS, ORB_END, ENTRY_WINDOW_END, _key_range, to_n_min_bars
from scripts.oi_orb_atr_chandelier_backtest import fetch_all

TOKEN = os.environ.get("UPSTOX_TOKEN", "")

TF_GRID = [(5, "5min"), (15, "15min")]
RSI_GRID = [9, 14]
STOCH_GRID = [9, 14]
SMOOTH = 3
OVERSOLD, OVERBOUGHT = 20.0, 80.0


# 2026-09-06: compute_rsi_series/compute_stoch_rsi moved to
# strategies/core/candle_indicators.py (ported into the live oi_orb_screener
# engine) -- imported here, not duplicated, so this backtest can never
# drift from the live version.
from strategies.core.candle_indicators import compute_rsi_series, compute_stoch_rsi  # noqa: E402,F401


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


def run_one(bars_1m, side, tf_min, rsi_period, stoch_period):
    bars_tf = to_n_min_bars(bars_1m, tf_min)
    closes = [b.close for b in bars_tf]
    k, d = compute_stoch_rsi(closes, rsi_period, stoch_period, SMOOTH)

    entry_window_tf = [i for i, b in enumerate(bars_tf) if ORB_END <= b.ts.strftime("%H:%M") < ENTRY_WINDOW_END]
    if not entry_window_tf:
        return None

    entry_idx = None
    for i in entry_window_tf:
        if i == 0 or k[i] is None or d[i] is None or k[i - 1] is None or d[i - 1] is None:
            continue
        crossed_up = k[i - 1] <= d[i - 1] and k[i] > d[i]
        crossed_dn = k[i - 1] >= d[i - 1] and k[i] < d[i]
        if side == "CALL" and crossed_up and k[i - 1] < OVERSOLD:
            entry_idx = i
            break
        if side == "PUT" and crossed_dn and k[i - 1] > OVERBOUGHT:
            entry_idx = i
            break
    if entry_idx is None:
        return None

    entry_ts, entry_price = bars_tf[entry_idx].ts, bars_tf[entry_idx].close

    exit_idx = None
    for j in range(entry_idx + 1, len(bars_tf)):
        if k[j] is None or d[j] is None or k[j - 1] is None or d[j - 1] is None:
            continue
        crossed_dn = k[j - 1] >= d[j - 1] and k[j] < d[j]
        crossed_up = k[j - 1] <= d[j - 1] and k[j] > d[j]
        if side == "CALL" and crossed_dn and k[j - 1] > OVERBOUGHT:
            exit_idx = j
            break
        if side == "PUT" and crossed_up and k[j - 1] < OVERSOLD:
            exit_idx = j
            break

    if exit_idx is not None:
        return entry_ts, entry_price, bars_tf[exit_idx].ts, bars_tf[exit_idx].close, "stoch_reversal"
    last = bars_tf[-1]
    return entry_ts, entry_price, last.ts, last.close, "eod_close"


def run_all(cache, tf_min, rsi_period, stoch_period):
    trades = []
    for trade_date, symbol, side_bias in ROWS:
        side = "CALL" if side_bias == "bullish" else "PUT"
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, _vol_by_ts, _orb_h, _orb_l = cached
        result = run_one(bars_1m, side, tf_min, rsi_period, stoch_period)
        if result is None:
            continue
        entry_ts, entry_price, exit_ts, exit_price, reason = result
        trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price, exit_ts, exit_price, reason))
    return trades


def trade_to_row(t):
    return [
        t.date, t.symbol, t.side,
        t.entry_ts.strftime("%H:%M") if t.entry_ts else None,
        round(t.entry_price, 2) if t.entry_price is not None else None,
        t.exit_ts.strftime("%H:%M") if t.exit_ts else None,
        round(t.exit_price, 2) if t.exit_price is not None else None,
        t.reason,
        round(t.points, 2) if t.points is not None else None,
    ]


def summarize(trades):
    entered = [t for t in trades if t.entry_price is not None]
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (999.0 if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    return {
        "entered": len(entered), "win_pct": round(win_pct, 1), "pf": round(min(pf, 999.0), 2),
        "total": round(total, 2), "avg": round(total / len(entered), 2) if entered else 0.0,
    }


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.", file=sys.stderr)
        return
    print("Fetching all rows once (real Upstox 1-min NSE_EQ history)...", file=sys.stderr)
    _real_stdout = sys.stdout
    sys.stdout = sys.stderr
    try:
        cache = await fetch_all()
    finally:
        sys.stdout = _real_stdout

    out = {"combos": {}}
    total = len(TF_GRID) * len(RSI_GRID) * len(STOCH_GRID)
    done = 0
    for tf_min, tf_label in TF_GRID:
        for rsi_period in RSI_GRID:
            for stoch_period in STOCH_GRID:
                trades = run_all(cache, tf_min, rsi_period, stoch_period)
                key = f"{tf_label}|rsi{rsi_period}|stoch{stoch_period}"
                out["combos"][key] = {
                    "tf": tf_label, "rsi_period": rsi_period, "stoch_period": stoch_period,
                    "rows": [trade_to_row(t) for t in sorted(trades, key=lambda x: (x.date, x.symbol))],
                    "call": summarize([t for t in trades if t.side == "CALL"]),
                    "put": summarize([t for t in trades if t.side == "PUT"]),
                    "combined": summarize(trades),
                }
                done += 1
                print(f"  [{done}/{total}] {key}", file=sys.stderr)

    print(json.dumps(out))


asyncio.run(main())
