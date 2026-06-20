"""
CrudeOil Backtest Optimizer — fast targeted grid search.

Approach: pre-detect zones once per (gap_threshold, min_zone_width, gap_dir_filter)
then vary only sl_buf in a fast post-filter loop. Total combos = manageable.

Usage:
  python scripts/optimize_backtest.py --token TOKEN --start 2026-05-20 --end 2026-06-19
  python scripts/optimize_backtest.py --token TOKEN --start 2026-06-05 --end 2026-06-05
"""

import argparse
import sys
import time
from datetime import date, timedelta
from itertools import product

import pandas as pd

sys.path.insert(0, ".")
from scripts.backtest_engine import (
    fetch_1m, _run_day, _HEADERS
)
import scripts.backtest_engine as eng

# ── Reduced grid (key variables only) ──────────────────────────────────────
GAP_THRESHOLDS  = [0.003, 0.005, 0.007, 0.008, 0.010, 0.012, 0.015]
ZONE_WIDTHS     = [10, 20, 25, 30, 35, 40]
SL_BUFS         = [15, 20, 25, 30]
DIR_FILTERS     = [True, False]
# require_gap=True is fixed (confirmed best in all analysis)

FIXED = {
    "htf_min_zone":    60,
    "htf_min_cascade": 30,
    "lot_size":        200,    # 2 lots × 100
    "ltf_minutes":     [5, 30],
    "max_age_days":    5,
    "ltf_source":      "futures",
    "itm_offset":      300,
    # Force GAP+NO_ZONE only — when gap_threshold > day's actual gap%,
    # that day becomes NO_GAP and the HTF-zone path runs (slow + not our strategy).
    # Locking to GAP+NO_ZONE makes non-gap days return instantly → 10× faster.
    "combo_filter":    "gap+no_zone",
    "fut_key":         "MCX_FO|520702",
    "require_gap":     True,
}

LOOKBACK_DAYS = 10


def _trading_days(start: str, end: str) -> list[str]:
    result, d = [], date.fromisoformat(start)
    ed = date.fromisoformat(end)
    while d <= ed:
        if d.weekday() < 5:
            result.append(d.isoformat())
        d += timedelta(days=1)
    return result


def _lookback_dates(td: str, n: int) -> list[str]:
    result, d = [], date.fromisoformat(td) - timedelta(days=1)
    while len(result) < n:
        if d.weekday() < 5:
            result.append(d.isoformat())
        d -= timedelta(days=1)
    return list(reversed(result))


def fetch_all_data(trading_days: list[str], fut_key: str) -> dict:
    cache: dict = {}
    needed = set()
    for td in trading_days:
        needed.add(td)
        for lb in _lookback_dates(td, LOOKBACK_DAYS):
            needed.add(lb)
    print(f"Fetching {len(needed)} days of data…", flush=True)
    for i, dt in enumerate(sorted(needed)):
        if dt not in cache:
            cache[dt] = fetch_1m(fut_key, dt)
            time.sleep(0.25)
        if (i + 1) % 10 == 0:
            print(f"  {i+1}/{len(needed)} fetched…", flush=True)
    print(f"Done — {len(cache)} days cached.\n", flush=True)
    return cache


def build_day_frames(trading_days: list[str], cache: dict) -> dict:
    frames = {}
    for td in trading_days:
        today_df = cache.get(td)
        if today_df is None or today_df.empty:
            continue
        lb_frames = [cache[d] for d in _lookback_dates(td, LOOKBACK_DAYS)
                     if d in cache and not cache[d].empty]
        lookback_df = (pd.concat(lb_frames, ignore_index=True)
                       .sort_values("datetime").reset_index(drop=True)
                       if lb_frames else pd.DataFrame())
        frames[td] = (today_df, lookback_df)
    return frames


def run_one(gap_thr, zone_width, sl_buf, dir_filter, trading_days, day_frames) -> list[dict]:
    all_trades = []
    for td in trading_days:
        if td not in day_frames:
            continue
        today_df, lookback_df = day_frames[td]
        trades = _run_day(
            td, today_df, lookback_df,
            FIXED["htf_min_zone"],
            FIXED["htf_min_cascade"],
            sl_buf,
            gap_thr,
            FIXED["combo_filter"],
            FIXED["lot_size"],
            FIXED["ltf_minutes"],
            zone_width,
            FIXED["max_age_days"],
            FIXED["ltf_source"],
            FIXED["itm_offset"],
            dir_filter,
            FIXED["require_gap"],
        )
        all_trades.extend(trades)
    return all_trades


def stats(trades: list[dict]) -> dict:
    if not trades:
        return dict(count=0, wins=0, losses=0, win_pct=0.0,
                    total_rs=0, avg_win=0.0, avg_loss=0.0,
                    profit_factor=0.0, max_dd=0.0)
    wins   = [t for t in trades if t["pnl_rs"] > 0]
    losses = [t for t in trades if t["pnl_rs"] <= 0]
    gw = sum(t["pnl_rs"] for t in wins)
    gl = abs(sum(t["pnl_rs"] for t in losses))
    pf = gw / gl if gl > 0 else 99.0
    eq, peak, dd = 0.0, 0.0, 0.0
    for t in trades:
        eq += t["pnl_rs"]
        peak = max(peak, eq)
        dd   = max(dd, peak - eq)
    return dict(count=len(trades), wins=len(wins), losses=len(losses),
                win_pct=100*len(wins)/len(trades),
                total_rs=sum(t["pnl_rs"] for t in trades),
                avg_win=gw/len(wins) if wins else 0.0,
                avg_loss=-gl/len(losses) if losses else 0.0,
                profit_factor=round(pf, 2), max_dd=dd)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--token",      required=True)
    ap.add_argument("--start",      required=True)
    ap.add_argument("--end",        required=True)
    ap.add_argument("--top",        type=int, default=20)
    ap.add_argument("--sort",       default="profit_factor",
                    choices=["profit_factor", "total_rs", "win_pct"])
    ap.add_argument("--min-trades", type=int, default=2)
    args = ap.parse_args()

    eng._HEADERS = {"Authorization": f"Bearer {args.token}",
                    "Accept": "application/json"}

    trading_days = _trading_days(args.start, args.end)
    total_combos = (len(GAP_THRESHOLDS) * len(ZONE_WIDTHS) *
                    len(SL_BUFS) * len(DIR_FILTERS))
    print(f"Optimising {args.start} → {args.end} "
          f"({len(trading_days)} trading days, {total_combos} combos)\n", flush=True)

    cache     = fetch_all_data(trading_days, FIXED["fut_key"])
    day_frames = build_day_frames(trading_days, cache)
    print(f"Day frames ready: {len(day_frames)} days.\n", flush=True)

    results = []
    t0 = time.time()
    done = 0

    for gap_thr, zone_width, dir_filter in product(
            GAP_THRESHOLDS, ZONE_WIDTHS, DIR_FILTERS):

        # Run once for this (gap, width, dir) triplet across all sl_bufs
        for sl_buf in SL_BUFS:
            trades = run_one(gap_thr, zone_width, sl_buf, dir_filter,
                             trading_days, day_frames)
            s = stats(trades)
            done += 1

            if s["count"] >= args.min_trades:
                results.append(dict(
                    gap_threshold=gap_thr,
                    min_zone_width=zone_width,
                    sl_buf=sl_buf,
                    gap_dir_filter=dir_filter,
                    **s,
                ))

            if done % 20 == 0:
                elapsed = time.time() - t0
                eta = (total_combos - done) / (done / elapsed)
                print(f"  {done}/{total_combos}  "
                      f"elapsed={elapsed:.0f}s  ETA={eta:.0f}s  "
                      f"results={len(results)}", flush=True)

    print(f"\nDone — {len(results)} combos with ≥{args.min_trades} trades "
          f"in {time.time()-t0:.0f}s\n", flush=True)

    results.sort(key=lambda r: r[args.sort], reverse=True)
    top = results[:args.top]

    HDR = (f"{'#':>3}  {'GAP%':>5}  {'WID':>4}  {'SL':>4}  {'DIR':>4}  "
           f"{'N':>4}  {'WIN%':>5}  {'TOTAL':>10}  "
           f"{'AVGW':>8}  {'AVGL':>9}  {'PF':>5}  {'DD':>8}")
    print(HDR)
    print("─" * len(HDR))

    for rank, r in enumerate(top, 1):
        print(
            f"{rank:>3}  "
            f"{r['gap_threshold']*100:>5.2f}  "
            f"{r['min_zone_width']:>4.0f}  "
            f"{r['sl_buf']:>4.0f}  "
            f"{'ON' if r['gap_dir_filter'] else 'OFF':>4}  "
            f"{r['count']:>4}  "
            f"{r['win_pct']:>5.1f}  "
            f"{r['total_rs']:>+10,.0f}  "
            f"{r['avg_win']:>+8,.0f}  "
            f"{r['avg_loss']:>+9,.0f}  "
            f"{r['profit_factor']:>5.2f}  "
            f"{r['max_dd']:>8,.0f}"
        )

    if top:
        b = top[0]
        print(f"\n★  BEST ({args.sort}):  "
              f"gap={b['gap_threshold']*100:.2f}%  "
              f"width={b['min_zone_width']:.0f}  "
              f"sl={b['sl_buf']:.0f}  "
              f"dir_filter={'ON' if b['gap_dir_filter'] else 'OFF'}  "
              f"→ {b['count']} trades  {b['win_pct']:.1f}% win  "
              f"Rs {b['total_rs']:+,.0f}  PF={b['profit_factor']}")


if __name__ == "__main__":
    main()
