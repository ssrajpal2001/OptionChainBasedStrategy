"""
scripts/fno_positional_sl_target_analysis.py — 2026-08-10.

Two questions on top of the already-validated daily/daily FnO positional
config (zone_days=1, entry_days=1, full 30-stock universe, 2yr window):

1. SL optimization -- sweep PositionalSRTracker's hard_risk_pct (the %-of-
   entry backstop cap; the ACTIVE stop is whichever is tighter of this and
   the day-low/day-high TSL ratchet -- see support_resistance.py's on_bar()).
2. Max-target analysis -- Maximum Favorable Excursion (MFE) per trade, now
   tracked by PositionalSRTracker (2026-08-10 addition: position["mfe_price"],
   updated every bar, included in the exit event as mfe_pct). This answers
   "how much was actually achievable" vs what the TSL-only exit (no fixed
   take-profit) actually captured -- the gap between the two is exactly the
   room a target/partial-booking rule could try to claim.

Usage:
    python3 scripts/fno_positional_sl_target_analysis.py --years 2.0
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.global_config import IST  # noqa: E402
from data_layer.client_db import ClientDB  # noqa: E402
from scripts.fno_positional_sr_backtest import backtest_one, STOCK_SUBSET  # noqa: E402

SL_SWEEP = (0.03, 0.05, 0.07, 0.10, 0.12, 0.15, 0.20)


def _pf(trades):
    if not trades:
        return 0.0
    wins = [t["pnl_pct"] for t in trades if t["pnl_pct"] > 0]
    losses = [t["pnl_pct"] for t in trades if t["pnl_pct"] <= 0]
    gl = abs(sum(losses))
    return (sum(wins) / gl) if gl > 0 else (float("inf") if wins else 0.0)


async def main() -> int:
    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    token = (creds or {}).get("access_token", "")
    if not token:
        print("FATAL: no Upstox access_token in data/clients.db.")
        return 1

    end = datetime.now(IST).date()
    start = end - timedelta(days=730)
    print(f"FnO positional SL + max-target analysis: {start} .. {end}  ({len(STOCK_SUBSET)} stocks)\n")

    # ── 1. SL sweep (zone_days=entry_days=1, the validated config) ──────────
    print("=" * 70)
    print("SL SWEEP (hard_risk_pct)")
    sl_results = {}
    for hrp in SL_SWEEP:
        results = []
        for symbol in STOCK_SUBSET:
            r = await backtest_one(symbol, start, end, token, zone_days=1, entry_days=1,
                                    hard_risk_pct=hrp)
            results.append(r)
        sl_results[hrp] = results

    print(f"  {'SL%':<8}{'Trades':>8}{'Win%':>7}{'AvgPnL%':>10}{'PF':>8}{'MedHold':>9}")
    best = None
    for hrp, results in sl_results.items():
        valid = [r for r in results if not r.get("error")]
        all_trades = [t for r in valid for t in r["trades"]]
        if not all_trades:
            continue
        wins = [t["pnl_pct"] for t in all_trades if t["pnl_pct"] > 0]
        pf = _pf(all_trades)
        win_pct = 100 * len(wins) / len(all_trades)
        avg_pnl = sum(t["pnl_pct"] for t in all_trades) / len(all_trades)
        holds = sorted(t["hold_days"] for t in all_trades)
        med_hold = holds[len(holds) // 2]
        pf_str = "inf" if pf == float("inf") else f"{pf:.2f}"
        print(f"  {hrp*100:<7.0f}%{len(all_trades):>8}{win_pct:>6.1f}%{avg_pnl:>+9.2f}%{pf_str:>8}{med_hold:>9}")
        if best is None or (pf if pf != float("inf") else 999) > best[0]:
            best = (pf if pf != float("inf") else 999, hrp, len(all_trades), win_pct, avg_pnl)
    if best:
        print(f"\nBest hard_risk_pct: {best[1]*100:.0f}%  n={best[2]} win%={best[3]:.1f} "
              f"avg_pnl%={best[4]:+.2f} PF={'inf' if best[0]==999 else best[0]:.2f}")

    # ── 2. Max-target (MFE) analysis at the validated 10% SL ────────────────
    print(f"\n{'='*70}\nMAX-TARGET (MFE) ANALYSIS  (hard_risk_pct=10%, the deployed default)")
    baseline = sl_results.get(0.10) or []
    valid = [r for r in baseline if not r.get("error")]
    all_trades = [t for r in valid for t in r["trades"]]
    mfe_vals = sorted((t.get("mfe_pct", 0.0) for t in all_trades), reverse=True)
    captured = [t["pnl_pct"] for t in all_trades]

    def pctile(vals, p):
        if not vals:
            return 0.0
        idx = min(len(vals) - 1, int(len(vals) * p))
        return sorted(vals, reverse=True)[idx]

    print(f"  Trades analyzed: {len(all_trades)}")
    print(f"  Avg MFE (best unrealized gain reached, any trade): {sum(mfe_vals)/len(mfe_vals):+.2f}%")
    print(f"  Avg actual captured P&L (what the TSL-only exit got): {sum(captured)/len(captured):+.2f}%")
    print(f"  Capture ratio (avg realized / avg MFE): {100*(sum(captured)/len(captured))/(sum(mfe_vals)/len(mfe_vals)):.1f}%")
    print(f"  Max single-trade MFE seen (any stock): {mfe_vals[0]:+.2f}%")
    print(f"  MFE percentiles: p50={pctile(mfe_vals,0.50):+.2f}%  p75={pctile(mfe_vals,0.25):+.2f}%  "
          f"p90={pctile(mfe_vals,0.10):+.2f}%  p95={pctile(mfe_vals,0.05):+.2f}%")

    print(f"\n  {'Symbol':<14}{'n':>5}{'MaxMFE%':>10}{'AvgMFE%':>10}{'AvgCaptured%':>14}{'Capture%':>10}")
    per_stock_max = []
    for r in valid:
        trades = r["trades"]
        if not trades:
            continue
        mfes = [t.get("mfe_pct", 0.0) for t in trades]
        caps = [t["pnl_pct"] for t in trades]
        max_mfe = max(mfes)
        avg_mfe = sum(mfes) / len(mfes)
        avg_cap = sum(caps) / len(caps)
        cap_ratio = (100 * avg_cap / avg_mfe) if avg_mfe else 0.0
        per_stock_max.append((r["symbol"], max_mfe))
        print(f"  {r['symbol']:<14}{len(trades):>5}{max_mfe:>9.2f}%{avg_mfe:>9.2f}%{avg_cap:>13.2f}%{cap_ratio:>9.1f}%")

    per_stock_max.sort(key=lambda x: -x[1])
    print(f"\n  Highest single max-target ever seen, by stock (top 10):")
    for sym, mfe in per_stock_max[:10]:
        print(f"    {sym:<14} {mfe:+.2f}%")

    out = {"sl_sweep": {str(k): v for k, v in sl_results.items()}}
    out_path = Path(__file__).resolve().parents[1] / "data" / "sweeps" / "fno_positional_sl_target_analysis.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(f"\n-> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
