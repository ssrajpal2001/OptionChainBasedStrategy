"""
scripts/pdh_pdl_equity_curve.py — is the PDH-PDL edge (currently PF ~1.1 on the
best swept combo) spread evenly across the 2-year sample, or is it one lucky
stretch carrying the whole result? 2026-08-28, direct user request as part of
"run both parallel" (extended tolerance sweep + this).

Loads scratch_pdh_pdl_best_trades.json (the current best combo's own trade
log, written by pdh_pdl_optimization_sweep.py) and breaks P&L down by
calendar month, plus a running equity curve with running peak/drawdown, so a
single-stretch-carries-it-all pattern (or a genuinely-broken later regime)
is visible directly instead of buried in one aggregate PF number.
"""
import json
from collections import OrderedDict
from datetime import datetime

with open("scratch_pdh_pdl_best_trades.json", encoding="utf-8") as f:
    trades = json.load(f)

trades.sort(key=lambda t: t["entry_ts"])

# ── monthly breakdown ────────────────────────────────────────────────────
months: "OrderedDict[str, dict]" = OrderedDict()
for t in trades:
    key = t["entry_ts"][:7]  # YYYY-MM
    m = months.setdefault(key, {"n": 0, "wins": 0, "net": 0.0, "gross_profit": 0.0, "gross_loss": 0.0})
    m["n"] += 1
    m["net"] += t["pnl_rs"]
    if t["pnl_pts"] > 0:
        m["wins"] += 1
        m["gross_profit"] += t["pnl_rs"]
    else:
        m["gross_loss"] += -t["pnl_rs"]

print(f"{'Month':<9} {'Trades':>7} {'Win%':>6} {'PF':>7} {'Net (Rs)':>14} {'Cum (Rs)':>14}")
print("-" * 62)
cum = 0.0
monthly_rows = []
for key, m in months.items():
    pf = (m["gross_profit"] / m["gross_loss"]) if m["gross_loss"] > 0 else float("inf")
    cum += m["net"]
    win_pct = m["wins"] / m["n"] * 100.0
    pf_str = f"{pf:.2f}" if pf != float("inf") else "inf"
    print(f"{key:<9} {m['n']:>7} {win_pct:>5.1f}% {pf_str:>7} {m['net']:>14,.0f} {cum:>14,.0f}")
    monthly_rows.append({"month": key, "n": m["n"], "win_pct": round(win_pct, 1),
                          "pf": (round(pf, 3) if pf != float("inf") else None),
                          "net": round(m["net"], 2), "cum": round(cum, 2)})

# ── running equity curve + drawdown ──────────────────────────────────────
equity = 0.0
peak = 0.0
max_dd = 0.0
max_dd_date = None
peak_date = None
curve = []
for t in trades:
    equity += t["pnl_rs"]
    if equity > peak:
        peak = equity
        peak_date = t["entry_ts"][:10]
    dd = equity - peak
    if dd < max_dd:
        max_dd = dd
        max_dd_date = t["entry_ts"][:10]
    curve.append({"ts": t["entry_ts"], "equity": round(equity, 2), "drawdown": round(dd, 2)})

print(f"\nMax drawdown Rs{max_dd:,.2f} reached on {max_dd_date} "
      f"(measured from the prior peak set on {peak_date}).")

# How much of total net P&L came from the single best month vs the rest?
best_month = max(monthly_rows, key=lambda r: r["net"])
worst_month = min(monthly_rows, key=lambda r: r["net"])
total_net = sum(r["net"] for r in monthly_rows)
print(f"\nBest month:  {best_month['month']}  net=Rs{best_month['net']:,.0f}  "
      f"({best_month['net'] / total_net * 100:.1f}% of total net P&L)" if total_net else "")
print(f"Worst month: {worst_month['month']}  net=Rs{worst_month['net']:,.0f}")

n_profitable_months = sum(1 for r in monthly_rows if r["net"] > 0)
print(f"\n{n_profitable_months}/{len(monthly_rows)} months were net profitable "
      f"({n_profitable_months / len(monthly_rows) * 100:.1f}%).")

with open("scratch_pdh_pdl_equity_curve.json", "w", encoding="utf-8") as f:
    json.dump({"monthly": monthly_rows, "curve": curve, "max_dd": round(max_dd, 2),
                "max_dd_date": max_dd_date, "peak_date": peak_date}, f, indent=2)
print("\nWritten to scratch_pdh_pdl_equity_curve.json")
