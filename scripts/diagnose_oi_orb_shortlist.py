"""
Diagnose why OI-ORB Screener's build_shortlist() found zero candidates on a
given day -- shows the RAW data at every stage instead of just the final
pass/fail, so you can see exactly where the funnel emptied out:

  1. NIFTY pChange (regime context only -- NOT what blocks the shortlist)
  2. Raw OI-Spurt list from NSE (oi_spurt_pct per symbol)
  3. Raw F&O price universe from NSE (pChange per symbol)
  4. The inner-join of the two (symbols present in BOTH lists)
  5. After the OI_SPURT_MIN_PCT filter
  6. After the PRICE_MOVE_MIN_PCT filter (= today's final shortlist)

NOTE: the NIFTY regime (bullish/bearish/neutral) is a SEPARATE, LATER gate
applied only to stocks that already made the shortlist (screener.py's
evaluate_breakout) -- it is NOT what empties build_shortlist() itself.
"no candidates passed the filters" in the live log means step 6 above came
back empty; regime never even entered the picture that day. This script
does not need a "skip regime" flag for that reason -- there's nothing
regime-related to skip before step 6.

Usage:
  python3 scripts/diagnose_oi_orb_shortlist.py [top_n]

top_n (default 20): how many rows to print per stage.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from strategies.oi_orb_screener import screener

TOP_N = int(sys.argv[1]) if len(sys.argv) > 1 else 20
pd.set_option("display.max_rows", 200)
pd.set_option("display.width", 160)

SEP = "=" * 70


def main():
    print(SEP)
    print("  OI-ORB Screener — Shortlist Diagnostic")
    print(SEP)

    nse = screener.NSESession()

    print("\n[ 1 ] NIFTY pChange (regime context only, not a shortlist filter)")
    try:
        nifty_pchange = screener.fetch_nifty_pchange(nse)
        bull_th = screener.CONFIG["NIFTY_BULLISH_PCT"]
        bear_th = screener.CONFIG["NIFTY_BEARISH_PCT"]
        if nifty_pchange >= bull_th:
            regime = "BULLISH"
        elif nifty_pchange <= bear_th:
            regime = "BEARISH"
        else:
            regime = "NEUTRAL"
        print(f"      NIFTY pChange = {nifty_pchange:+.2f}%  -> regime = {regime}"
              f"  (bull>={bull_th}%, bear<={bear_th}%)")
    except Exception as exc:
        print(f"      FAILED: {exc}")
        nifty_pchange = 0.0

    print("\n[ 2 ] Raw OI-Spurt list from NSE (fetch_oi_spurts_nse)")
    try:
        oi_spurts = screener.fetch_oi_spurts_nse(nse)
        print(f"      {len(oi_spurts)} symbols total. Top {TOP_N} by oi_spurt_pct:")
        print(oi_spurts.sort_values("oi_spurt_pct", ascending=False).head(TOP_N).to_string(index=False))
    except Exception as exc:
        print(f"      FAILED: {exc}")
        oi_spurts = pd.DataFrame(columns=["symbol", "oi_spurt_pct"])

    print("\n[ 3 ] Raw F&O price universe from NSE (fetch_fno_price_universe)")
    try:
        universe = screener.fetch_fno_price_universe(nse)
        print(f"      {len(universe)} symbols total. Top {TOP_N} by |pChange|:")
        print(universe.reindex(universe["pChange"].abs().sort_values(ascending=False).index)
              .head(TOP_N)[["symbol", "pChange", "lastPrice"]].to_string(index=False))
    except Exception as exc:
        print(f"      FAILED: {exc}")
        universe = pd.DataFrame(columns=["symbol", "pChange"])

    print("\n[ 4 ] Inner join (symbols present in BOTH lists)")
    merged = universe.merge(oi_spurts, on="symbol", how="inner") if not universe.empty and not oi_spurts.empty else pd.DataFrame()
    print(f"      {len(merged)} symbols overlap.")
    if not merged.empty:
        print(merged[["symbol", "pChange", "oi_spurt_pct"]]
              .sort_values("oi_spurt_pct", ascending=False).head(TOP_N).to_string(index=False))

    oi_min = screener.CONFIG["OI_SPURT_MIN_PCT"]
    price_min = screener.CONFIG["PRICE_MOVE_MIN_PCT"]

    print(f"\n[ 5 ] After OI_SPURT_MIN_PCT filter (oi_spurt_pct >= {oi_min}%)")
    after_oi = merged[merged["oi_spurt_pct"] >= oi_min] if not merged.empty else pd.DataFrame()
    print(f"      {len(after_oi)} symbols remain.")
    if not after_oi.empty:
        print(after_oi[["symbol", "pChange", "oi_spurt_pct"]].to_string(index=False))

    print(f"\n[ 6 ] After PRICE_MOVE_MIN_PCT filter (|pChange| >= {price_min}%) — THIS IS TODAY'S REAL SHORTLIST")
    after_price = after_oi[after_oi["pChange"].abs() >= price_min] if not after_oi.empty else pd.DataFrame()
    print(f"      {len(after_price)} symbols remain.")
    if not after_price.empty:
        print(after_price[["symbol", "pChange", "oi_spurt_pct"]].to_string(index=False))
    else:
        print("      EMPTY — this matches the live 'no candidates passed the filters' log line.")
        if not after_oi.empty:
            closest = after_oi.reindex(after_oi["pChange"].abs().sort_values(ascending=False).index).head(5)
            print(f"      Closest misses on price move (had OI spurt, but |pChange| < {price_min}%):")
            print(closest[["symbol", "pChange", "oi_spurt_pct"]].to_string(index=False))
        elif not merged.empty:
            closest = merged.sort_values("oi_spurt_pct", ascending=False).head(5)
            print(f"      Closest misses on OI spurt (overlap existed, but oi_spurt_pct < {oi_min}%):")
            print(closest[["symbol", "pChange", "oi_spurt_pct"]].to_string(index=False))
        else:
            print("      The two source lists didn't overlap in symbols at all today — check step 2/3 above.")

    print("\n" + SEP)
    print("  Diagnostic COMPLETE")
    print(SEP)


main()
