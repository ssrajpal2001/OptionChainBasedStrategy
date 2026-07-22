"""backtest/v4_cascade/htf_ltf_main.py -- runs htf_ltf_backtest.py (the
HTF-gated LTF cascade spec) over real NIFTY spot history, grid-searching the
entry/SL offset from HTF zone_low/high.

Usage: UPSTOX_TOKEN=... python backtest/v4_cascade/htf_ltf_main.py --days 90"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, ".")

from backtest.v4_cascade.data_fetch import fetch_nifty_spot_1m
from backtest.v4_cascade.htf_ltf_backtest import build_5m_bars, run_backtest
from backtest.v4_cascade.optimizer import compute_metrics
from backtest.v4_cascade.reporting import write_report

OFFSET_GRID = [5.0, 10.0, 15.0, 20.0]


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--qty", type=int, default=130)
    args = parser.parse_args()

    token = os.environ.get("UPSTOX_TOKEN", "")
    if not token:
        print("UPSTOX_TOKEN not set"); return

    end = date.today()
    start = end - timedelta(days=args.days)
    print(f"Fetching NIFTY spot 1m history {start} .. {end} ...")
    rows = await fetch_nifty_spot_1m(token, start, end)
    print(f"Fetched {len(rows)} 1-minute rows.")
    if not rows:
        print("No data fetched -- aborting."); return

    bars_5m = build_5m_bars(rows)
    print(f"Built {len(bars_5m)} 5-minute bars.")

    results = []
    for offset in OFFSET_GRID:
        legs = run_backtest(bars_5m, entry_offset=offset, qty=args.qty)
        metrics = compute_metrics(legs)
        results.append({"params": {"entry_offset": offset}, "metrics": metrics, "legs": legs})
        print(f"offset={offset}: trades={metrics['trades']} win_rate={metrics['win_rate']}% "
              f"PF={metrics['profit_factor']} net_pnl=Rs{metrics['net_pnl']}")

    def rank_key(r):
        pf = r["metrics"]["profit_factor"]
        pf_key = pf if pf != float("inf") else 1e9
        return (-pf_key, -r["metrics"]["max_drawdown"])
    results.sort(key=rank_key)

    write_report_htf_ltf(results, (start, end))
    best = results[0]
    print(f"\nBest: entry_offset={best['params']['entry_offset']}")
    print(f"  trades={best['metrics']['trades']} win_rate={best['metrics']['win_rate']}% "
          f"PF={best['metrics']['profit_factor']} max_dd=Rs{best['metrics']['max_drawdown']} "
          f"net_pnl=Rs{best['metrics']['net_pnl']}")
    print("\nWrote backtest/v4_cascade/results/htf_ltf_report.md, htf_ltf_trades.csv, htf_ltf_best_params.json")


def write_report_htf_ltf(results, date_range) -> None:
    import csv
    import json
    import os as _os
    out_dir = _os.path.join(_os.path.dirname(__file__), "results")
    _os.makedirs(out_dir, exist_ok=True)
    best = results[0]

    def fmt(ts):
        if ts is None:
            return "-"
        return ts.isoformat(sep=" ", timespec="seconds") if hasattr(ts, "isoformat") else str(ts)

    lines = ["# V4 Cascade HTF-Gated LTF Cascade Backtest\n",
             f"**Period:** {date_range[0].isoformat()} .. {date_range[1].isoformat()} (NIFTY spot)",
             "**Model:** HTF(75m) zone via find_bear_zone/find_bull_zone (unchanged production "
             "functions) -> wait for re-entry -> 15m nested zone (T1 target) -> 5m break-of-structure "
             "trigger -> limit at HTF zone_low/high +/- offset, SL at the mirror offset, T2 target = "
             "HTF ref candle's opposite extreme, T2 trails to breakeven after T1 hits.\n",
             "## Grid (entry/SL offset from HTF zone_low/high)\n",
             "| offset | trades | win% | PF | max_dd(Rs) | net_pnl(Rs) |",
             "| --- | --- | --- | --- | --- | --- |"]
    for r in results:
        p, m = r["params"], r["metrics"]
        pf = "inf" if m["profit_factor"] == float("inf") else m["profit_factor"]
        lines.append(f"| {p['entry_offset']} | {m['trades']} | {m['win_rate']} | {pf} | "
                      f"{m['max_drawdown']} | {m['net_pnl']} |")

    bp, bm = best["params"], best["metrics"]
    lines.append(f"\n## Best: offset={bp['entry_offset']}\n")
    lines.append(f"- Trades: {bm['trades']} (wins {bm['wins']} / losses {bm['losses']}, "
                  f"win rate {bm['win_rate']}%)")
    pf_disp = "inf" if bm["profit_factor"] == float("inf") else bm["profit_factor"]
    lines.append(f"- Profit factor: **{pf_disp}**, max drawdown: Rs{bm['max_drawdown']}, "
                  f"net P&L: Rs{bm['net_pnl']}\n")

    ce_legs = [l for l in best["legs"] if l["side"] == "CE"]
    pe_legs = [l for l in best["legs"] if l["side"] == "PE"]
    ce_pnl = sum((l["pnl_points"] or 0) * (l["qty"] or 0) for l in ce_legs)
    pe_pnl = sum((l["pnl_points"] or 0) * (l["qty"] or 0) for l in pe_legs)
    lines.append("## CE (bear-trap/long) vs PE (bull-trap/short), best offset\n")
    lines.append("| side | legs | net_pnl(Rs) |\n| --- | --- | --- |")
    lines.append(f"| CE | {len(ce_legs)} | {round(ce_pnl, 2)} |")
    lines.append(f"| PE | {len(pe_legs)} | {round(pe_pnl, 2)} |\n")

    lines.append("## Every trade, best offset -- full audit trail\n")
    lines.append("| side | 1.ref candle (val, ts) | 2.trapped (75m confirmed) | 3.re-entered zone | "
                  "4.LTF ref (val, ts) | 5.5m trigger | 6.limit filled (entry) | SL | T1 target | "
                  "T2 target | close | reason | pnl(Rs) |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for leg in sorted(best["legs"], key=lambda l: l["entry_ts"] or l["close_ts"] or ""):
        pnl_rs = round((leg["pnl_points"] or 0) * (leg["qty"] or 0), 2) if leg["pnl_points"] is not None else "-"
        ref_val = leg.get("htf_ref_high") if leg["side"] == "CE" else leg.get("htf_ref_low")
        ltf_val = leg.get("ltf_ref_high") if leg["side"] == "CE" else leg.get("ltf_ref_low")
        lines.append(
            f"| {leg['side']} {leg['tranche']} | {ref_val} @ {fmt(leg.get('htf_ref_ts'))} | "
            f"{fmt(leg.get('htf_lock_ts'))} | {fmt(leg.get('reentry_ts'))} | "
            f"{ltf_val} @ {fmt(leg.get('ltf_ref_ts'))} | {fmt(leg.get('trigger_ts'))} | "
            f"{leg.get('entry_price')} @ {fmt(leg.get('entry_ts'))} | {leg.get('sl_price')} | "
            f"{leg.get('target_price') if leg['tranche']=='T1' else '-'} | "
            f"{leg.get('target_price') if leg['tranche']=='T2' else '-'} | "
            f"{leg.get('close_price')} @ {fmt(leg.get('close_ts'))} | {leg.get('close_reason')} | {pnl_rs} |"
        )

    with open(_os.path.join(out_dir, "htf_ltf_report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    with open(_os.path.join(out_dir, "htf_ltf_trades.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["side", "tranche", "htf_ref_ts", "htf_ref_high", "htf_ref_low", "htf_lock_ts",
                    "reentry_ts", "ltf_ref_ts", "ltf_ref_high", "ltf_ref_low", "trigger_ts",
                    "entry_ts", "entry_price", "sl_price", "target_price", "close_ts", "close_price",
                    "close_reason", "qty", "pnl_points", "pnl_rupees"])
        for leg in sorted(best["legs"], key=lambda l: l["entry_ts"] or l["close_ts"] or ""):
            pnl_rs = (leg["pnl_points"] or 0) * (leg["qty"] or 0) if leg["pnl_points"] is not None else ""
            w.writerow([leg["side"], leg["tranche"], fmt(leg.get("htf_ref_ts")), leg.get("htf_ref_high"),
                        leg.get("htf_ref_low"), fmt(leg.get("htf_lock_ts")), fmt(leg.get("reentry_ts")),
                        fmt(leg.get("ltf_ref_ts")), leg.get("ltf_ref_high"), leg.get("ltf_ref_low"),
                        fmt(leg.get("trigger_ts")), fmt(leg.get("entry_ts")), leg.get("entry_price"),
                        leg.get("sl_price"), leg.get("target_price"), fmt(leg.get("close_ts")),
                        leg.get("close_price"), leg.get("close_reason"), leg.get("qty"),
                        leg.get("pnl_points"), pnl_rs])

    with open(_os.path.join(out_dir, "htf_ltf_best_params.json"), "w", encoding="utf-8") as f:
        json.dump({"params": bp, "metrics": bm}, f, indent=2, default=str)


if __name__ == "__main__":
    asyncio.run(main())
