"""
scripts/d1trap_strike_ladder_backtest.py -- Stage 1 of the intraday D1-Trap
optimization: which strike (ATM / 1-ITM / 2-ITM / 3-ITM) actually exhibits
the expected bear-trap price action, holding the rest of the mechanic fixed
(current default: 60m zone timeframe, ref.low zone boundary, existing
breach/arm/swing-breach entries, existing SL/TSL). Real premium, month
window 06-29..07-31, single consistent weekly contract per underlying (no
rollover), fetched by scripts/d1trap_strike_ladder_fetch.py.

Reuses scripts/d1trap_month_rolling_backtest.py's zone engine and day-loop
(same mechanic already validated on the NIFTY month baseline) -- only the
strike offset varies across the 4 runs per underlying.
"""
import sys
sys.path.insert(0, ".")

import scripts.d1trap_month_rolling_backtest as mrb

LADDER_DIR = "data/d1trap_fractal_cache/strike_ladder"

CONFIGS = {
    "NIFTY":  dict(fname_prefix="niftyladder", step=50, round_step=100, lot=65,
                   spot_path="data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet"),
    "SENSEX": dict(fname_prefix="sensexladder", step=100, round_step=100, lot=20,
                   spot_path="data/d1trap_fractal_cache/sensex_1m_fullmonth_spot.parquet"),
}
LADDER_LABELS = {0: "0-ITM (ATM)", 1: "1-ITM", 2: "2-ITM", 3: "3-ITM"}


def run_all():
    mrb.MONTH_DIR = LADDER_DIR
    mrb.DAY_MIN, mrb.DAY_MAX = mrb.DAY_MIN, mrb.DAY_MAX  # keep existing 06-29..07-31 window

    results = {}
    for underlying, cfg in CONFIGS.items():
        print(f"\n{'#'*100}\n{underlying}\n{'#'*100}")
        results[underlying] = {}
        for n in (0, 1, 2, 3):
            offset = n * cfg["step"]
            trades = mrb.run_month(underlying, cfg["fname_prefix"], cfg["spot_path"],
                                    offset, cfg["round_step"], cfg["lot"])
            stats = mrb.summarize(f"{underlying} {LADDER_LABELS[n]} (offset={offset})", trades)
            results[underlying][n] = stats
    return results


if __name__ == "__main__":
    results = run_all()

    print(f"\n{'='*100}\nSTAGE 1 SUMMARY -- STRIKE LADDER COMPARISON\n{'='*100}")
    for underlying, by_n in results.items():
        print(f"\n{underlying}")
        print(f"{'Strike':<16}{'Trades':>8}{'Win%':>8}{'PF':>8}{'NetPnL':>12}")
        for n in (0, 1, 2, 3):
            s = by_n[n]
            print(f"{LADDER_LABELS[n]:<16}{s['n']:>8}{s['win_pct']:>7.1f}%{s['pf']:>8.2f}{s['total']:>+12,.0f}")
        best = max(by_n.items(), key=lambda kv: kv[1]["pf"] if kv[1]["n"] >= 5 else -999)
        print(f"  -> best PF (n>=5 trades): {LADDER_LABELS[best[0]]}  PF={best[1]['pf']:.2f}  NET={best[1]['total']:+,.0f}")
