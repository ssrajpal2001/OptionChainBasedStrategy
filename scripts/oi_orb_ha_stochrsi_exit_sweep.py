"""
scripts/oi_orb_ha_stochrsi_exit_sweep.py -- 2026-09-05, direct user
follow-up: "which tf for entry and trap r u using and which tf r u using
for heik, that is not show in backtest, I want everything there all
combination and default the best one."

Clarifies + sweeps every adjustable knob in the HA-shape+StochRSI exit
mechanic (scripts/oi_orb_ha_stochrsi_exit_backtest.py):
  - Entry timing itself has NO separate timeframe knob -- the VWAP-retest
    arm/touch-back (same mechanic used everywhere this week) runs on the
    raw 1-min bar series (or the 1-min-built Heikin-Ashi series in HA
    mode) by construction; there's no HTF/trigger step in THIS mechanic
    the way the Trap mechanic had one.
  - What DOES vary and IS swept here: candle type (Normal / Heikin-Ashi,
    for entry timing + P&L tracking), the HA exit-signal timeframe
    (5min/15min/30min -- the "15 minutes" in the original spec), and the
    two StochRSI lookback periods (9/14 each for RSI and Stoch).

2 candle types x 3 exit-TF x 2 RSI-period x 2 Stoch-period = 24 combos,
all against the same cached real Upstox 1-min data (pure resampling, no
extra network calls) -- dumped as one JSON blob for a client-side-
adjustable HTML artifact, same pattern as every other sweep this week.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_ha_stochrsi_exit_sweep.py > out.json
"""
from __future__ import annotations

import asyncio
import json
import os
import sys

sys.path.insert(0, ".")

from scripts.oi_orb_ha_stochrsi_exit_backtest import run_all
from scripts.oi_orb_atr_chandelier_backtest import fetch_all

TOKEN = os.environ.get("UPSTOX_TOKEN", "")

CANDLE_GRID = [("normal", "Normal"), ("heikin_ashi", "Heikin-Ashi")]
EXIT_TF_GRID = [(5, "5min"), (15, "15min"), (30, "30min")]
RSI_GRID = [9, 14]
STOCH_GRID = [9, 14]
OP_GRID = [(False, "strict(>)"), (True, "inclusive(>=)")]


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
    total_combos = len(CANDLE_GRID) * len(EXIT_TF_GRID) * len(RSI_GRID) * len(STOCH_GRID) * len(OP_GRID)
    done = 0
    for candle_key, candle_label in CANDLE_GRID:
        for exit_tf_min, exit_tf_label in EXIT_TF_GRID:
            for rsi_period in RSI_GRID:
                for stoch_period in STOCH_GRID:
                    for inclusive, op_label in OP_GRID:
                        trades = run_all(cache, candle_key, exit_tf_min=exit_tf_min,
                                          rsi_period=rsi_period, stoch_period=stoch_period,
                                          inclusive=inclusive)
                        key = f"{candle_key}|{exit_tf_label}|rsi{rsi_period}|stoch{stoch_period}|{op_label}"
                        out["combos"][key] = {
                            "candle": candle_label, "exit_tf": exit_tf_label,
                            "rsi_period": rsi_period, "stoch_period": stoch_period, "op": op_label,
                            "rows": [trade_to_row(t) for t in sorted(trades, key=lambda x: (x.date, x.symbol))],
                            "call": summarize([t for t in trades if t.side == "CALL"]),
                            "put": summarize([t for t in trades if t.side == "PUT"]),
                            "combined": summarize(trades),
                        }
                        done += 1
                        print(f"  [{done}/{total_combos}] {key}", file=sys.stderr)

    print(json.dumps(out))


asyncio.run(main())
