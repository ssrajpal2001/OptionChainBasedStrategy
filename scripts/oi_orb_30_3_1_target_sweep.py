"""
scripts/oi_orb_30_3_1_target_sweep.py -- 2026-09-05, direct user follow-up:
"I want that to have diff tf selection which will change the backtest
results, htf and ltf can be adjusted from our side to see the backtest
results." Runs the 30/3/1 trap-target mechanic (scripts/
oi_orb_30_3_1_target_backtest.py) across a grid of HTF (trigger tf) x
TRAP_TF (zone-detection tf) x LTF (S&R exit-confirmation tf) combinations,
using the SAME already-fetched single-day real Upstox bars for every
combo (pure resampling, no extra network calls) -- and dumps one JSON
blob covering every combo so an HTML artifact can let the timeframes be
changed client-side without re-running Python.

HTF in {30min, 1h, 2h}; TRAP_TF in {3min, 5min, 15min}; LTF in {1min,
3min} -- 18 combos x (CALL+PUT) each. Same entry timing (VWAP-retest /
historical-immediate / no-reentry / breach-cancel) and no-SL exit
philosophy as the base script, unchanged across every combo -- only the
three timeframes vary.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_30_3_1_target_sweep.py > /tmp/sweep.json
"""
from __future__ import annotations

import asyncio
import json
import os
import sys

sys.path.insert(0, ".")

from scripts.oi_orb_30_3_1_target_backtest import run_all
from scripts.oi_orb_atr_chandelier_backtest import fetch_all

TOKEN = os.environ.get("UPSTOX_TOKEN", "")

HTF_GRID = [(30, "30min"), (60, "1h"), (120, "2h")]
TRAP_GRID = [(3, "3min"), (5, "5min"), (15, "15min")]
LTF_GRID = [(1, "1min"), (3, "3min")]


def trade_to_row(t):
    return [
        t.date, t.symbol, t.side,
        t.entry_ts.strftime("%H:%M") if t.entry_ts else None,
        round(t.entry_price, 2) if t.entry_price is not None else None,
        t.trigger_ts.strftime("%H:%M") if t.trigger_ts else None,
        t.zone_touched_ts.strftime("%H:%M") if t.zone_touched_ts else None,
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
    candle_types = [("normal", "Normal"), ("heikin_ashi", "Heikin-Ashi")]
    total_combos = len(HTF_GRID) * len(TRAP_GRID) * len(LTF_GRID) * len(candle_types)
    done = 0
    for candle_key, candle_label in candle_types:
        for htf_min, htf_label in HTF_GRID:
            for trap_min, trap_label in TRAP_GRID:
                for ltf_min, ltf_label in LTF_GRID:
                    trades = run_all(cache, htf_min=htf_min, trap_tf_min=trap_min, ltf_min=ltf_min,
                                      candle_type=candle_key)
                    key = f"{candle_key}|{htf_label}|{trap_label}|{ltf_label}"
                    out["combos"][key] = {
                        "candle": candle_label, "htf": htf_label, "trap": trap_label, "ltf": ltf_label,
                        "rows": [trade_to_row(t) for t in sorted(trades, key=lambda x: (x.date, x.symbol))],
                        "call": summarize([t for t in trades if t.side == "CALL"]),
                        "put": summarize([t for t in trades if t.side == "PUT"]),
                        "combined": summarize(trades),
                    }
                    done += 1
                    print(f"  [{done}/{total_combos}] {key}", file=sys.stderr)

    print(json.dumps(out))


asyncio.run(main())
