"""backtest/v4_cascade/reporting.py -- writes report.md (ranked grid +
best-set trade table you can manually cross-check against a real NIFTY
chart) + trades.csv + best_params.json to backtest/v4_cascade/results/."""
from __future__ import annotations

import csv
import json
import os
from datetime import datetime
from typing import List

_RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")


def _fmt_ts(ts) -> str:
    if ts is None:
        return "-"
    if isinstance(ts, str):
        return ts
    return ts.isoformat(sep=" ", timespec="seconds")


def write_report(results: List[dict], date_range: tuple, out_dir: str = _RESULTS_DIR) -> None:
    os.makedirs(out_dir, exist_ok=True)
    best = results[0]

    lines = []
    lines.append("# V4 Cascade Exit-Parameter Backtest — Report\n")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"**Period:** {date_range[0].isoformat()} .. {date_range[1].isoformat()} (NIFTY spot, real Upstox history)")
    lines.append("**Model:** entries/gates via real, unmodified production code "
                  "(SpotConfirmTracker, IndexGatedPremiumScanner, engine.py); SL/target/TSL in "
                  "NIFTY spot points, P&L approximated at 1:1 spot-point-to-1-ITM-premium-move "
                  "(±5% real-fill variance should be read as accepted slippage, not modeled here).\n")

    lines.append("## Section 1: Grid results (ranked by profit factor, drawdown tie-break)\n")
    lines.append("| rank | sl_buffer | target_floor_x | tsl_bases (5m) | trades | win% | PF | max_dd(₹) | net_pnl(₹) |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for i, r in enumerate(results[:20], start=1):
        p, m = r["params"], r["metrics"]
        pf = "inf" if m["profit_factor"] == float("inf") else m["profit_factor"]
        lines.append(f"| {i} | {p['sl_buffer']} | {p['target_floor_multiple']} | "
                      f"{p['t2_trail_lookback_bases']} | "
                      f"{m['trades']} | {m['win_rate']} | {pf} | {m['max_drawdown']} | {m['net_pnl']} |")
    lines.append("")

    bp, bm = best["params"], best["metrics"]
    lines.append("## Section 2: Best parameter set\n")
    lines.append(f"- SL buffer: **{bp['sl_buffer']} pts**")
    lines.append(f"- Target floor multiple: **{bp['target_floor_multiple']}x** (of SL distance)")
    lines.append(f"- T2 trailing-stop: **{bp['t2_trail_lookback_bases']} bases** (5m, the only timeframe production supports today)")
    lines.append(f"- Trades: {bm['trades']} (wins {bm['wins']} / losses {bm['losses']}, win rate {bm['win_rate']}%)")
    pf_disp = "inf (no losses)" if bm["profit_factor"] == float("inf") else bm["profit_factor"]
    lines.append(f"- Profit factor: **{pf_disp}**, max drawdown: ₹{bm['max_drawdown']}, net P&L: ₹{bm['net_pnl']}\n")

    ce_legs = [l for l in best["legs"] if l["side"] == "CE"]
    pe_legs = [l for l in best["legs"] if l["side"] == "PE"]
    ce_pnl = sum((l["pnl_points"] or 0) * (l["qty"] or 0) for l in ce_legs)
    pe_pnl = sum((l["pnl_points"] or 0) * (l["qty"] or 0) for l in pe_legs)
    lines.append("## Section 3: Bear-trap (CE/long) vs bull-trap (PE/short) breakdown, best set\n")
    lines.append("| side | trap type | legs | net_pnl(₹) |")
    lines.append("| --- | --- | --- | --- |")
    lines.append(f"| CE | bear_trap_confirmed (long) | {len(ce_legs)} | {round(ce_pnl, 2)} |")
    lines.append(f"| PE | bull_trap_confirmed (short) | {len(pe_legs)} | {round(pe_pnl, 2)} |")
    lines.append("")

    lines.append("## Section 4: Every trade, best parameter set — cross-check against your NIFTY chart\n")
    lines.append("| trap ref (candle) | trap confirmed | side | trap type | entry time | entry px | SL | target | close time | close px | reason | pnl(₹) |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for leg in sorted(best["legs"], key=lambda l: l["entry_ts"] or l["close_ts"] or ""):
        pnl_rs = round((leg["pnl_points"] or 0) * (leg["qty"] or 0), 2) if leg["pnl_points"] is not None else "-"
        lines.append(
            f"| {_fmt_ts(leg.get('ref_ts'))} | {_fmt_ts(leg.get('lock_ts'))} | "
            f"{leg['side']} {leg['tranche']} | {leg.get('index_kind') or '-'} | "
            f"{_fmt_ts(leg.get('entry_ts'))} | {leg.get('entry_price')} | {leg.get('sl_price')} | "
            f"{leg.get('target_price')} | {_fmt_ts(leg.get('close_ts'))} | {leg.get('close_price')} | "
            f"{leg.get('close_reason')} | {pnl_rs} |"
        )
    lines.append("")

    with open(os.path.join(out_dir, "report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    with open(os.path.join(out_dir, "trades.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ref_ts", "lock_ts", "side", "tranche", "index_kind", "entry_ts", "entry_price",
                    "sl_price", "target_price", "close_ts", "close_price", "close_reason", "qty",
                    "pnl_points", "pnl_rupees"])
        for leg in sorted(best["legs"], key=lambda l: l["entry_ts"] or l["close_ts"] or ""):
            pnl_rs = (leg["pnl_points"] or 0) * (leg["qty"] or 0) if leg["pnl_points"] is not None else ""
            w.writerow([_fmt_ts(leg.get("ref_ts")), _fmt_ts(leg.get("lock_ts")), leg["side"], leg["tranche"],
                        leg.get("index_kind"), _fmt_ts(leg.get("entry_ts")), leg.get("entry_price"),
                        leg.get("sl_price"), leg.get("target_price"), _fmt_ts(leg.get("close_ts")),
                        leg.get("close_price"), leg.get("close_reason"), leg.get("qty"),
                        leg.get("pnl_points"), pnl_rs])

    with open(os.path.join(out_dir, "best_params.json"), "w", encoding="utf-8") as f:
        json.dump({"params": bp, "metrics": bm}, f, indent=2, default=str)
