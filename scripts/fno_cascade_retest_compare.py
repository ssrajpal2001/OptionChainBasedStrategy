"""
scripts/fno_cascade_retest_compare.py — 2026-08-11.

Direct A/B: does skipping the fine-tf re-zone/retest gate (go straight from
a confirmed mid-tf ref-candle break to fine-tf S&R tracking) beat the
retest-gated version? exit_sr_confirm fixed to False (simple exit already
won decisively in the prior sweep, no need to re-test it here).
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.global_config import FNO_STOCK_CONFIG  # noqa: E402
from data_layer.client_db import ClientDB  # noqa: E402
from scripts.fno_cascade_sr_backtest import backtest_one  # noqa: E402

MID_SWEEP = (30, 60, 120)
FINE_SWEEP = (3, 5, 15)


async def main() -> int:
    db = ClientDB()
    token = db.get_feeder_creds_sync("upstox")["access_token"]
    stocks = list(FNO_STOCK_CONFIG.keys())
    print(f"Retest-gate A/B: {len(stocks)} stocks, exit=simple fixed\n")

    all_results = {}
    for mid_m in MID_SWEEP:
        for fine_m in FINE_SWEEP:
            if fine_m >= mid_m:
                continue
            for require_retest in (True, False):
                tag = f"mid{mid_m}_fine{fine_m}_retest{'Y' if require_retest else 'N'}"
                results = []
                for sym in stocks:
                    try:
                        r = await backtest_one(sym, token, mid_m, fine_m, exit_sr_confirm=False,
                                                require_retest=require_retest)
                    except Exception as exc:
                        r = dict(symbol=sym, error=f"{type(exc).__name__}: {exc}")
                    results.append(r)
                all_results[tag] = results
                valid = [r for r in results if not r.get("error")]
                all_trades = [t for r in valid for t in r["trades"]]
                if all_trades:
                    wins = [t["pnl_pct"] for t in all_trades if t["pnl_pct"] > 0]
                    losses = [t["pnl_pct"] for t in all_trades if t["pnl_pct"] <= 0]
                    gl = abs(sum(losses))
                    pf = (sum(wins) / gl) if gl > 0 else (float("inf") if wins else 0.0)
                    win_pct = 100 * len(wins) / len(all_trades)
                    sl_dists = [t["sl_dist_pct"] for t in all_trades if t["sl_dist_pct"] is not None]
                    avg_sl = sum(sl_dists) / len(sl_dists) if sl_dists else None
                    pf_str = "inf" if pf == float("inf") else f"{pf:.2f}"
                    avg_sl_str = f"{avg_sl:.2f}%" if avg_sl is not None else "—"
                    print(f"  {tag:<24} n={len(all_trades):>4} win%={win_pct:>5.1f} PF={pf_str:>6} "
                          f"avgSL%={avg_sl_str}")
                else:
                    print(f"  {tag:<24} n=0 (no trades)")

    out_path = Path(__file__).resolve().parents[1] / "data" / "sweeps" / "fno_cascade_retest_compare.json"
    out_path.write_text(json.dumps(all_results, indent=2, default=str), encoding="utf-8")
    print(f"\n-> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
