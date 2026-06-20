"""
CrudeOil Backtest Parameter Optimizer
--------------------------------------
Grid search over all parameter combinations, ranked by profit factor.

Usage:
  python scripts/optimize_backtest.py --token <UPSTOX_TOKEN> --start 2026-05-20 --end 2026-06-19
  python scripts/optimize_backtest.py --token <UPSTOX_TOKEN> --start 2026-06-05 --end 2026-06-05

Output: ranked table of best parameter combinations printed to terminal.
"""

import argparse
import itertools
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, ".")
from scripts.backtest_engine import fetch_1m, _run_day

# ── Parameter grid ──────────────────────────────────────────────────────────
GRID = {
    "gap_threshold":  [0.003, 0.005, 0.007, 0.008, 0.010, 0.012, 0.015],
    "min_zone_width": [10, 15, 20, 25, 30, 35, 40],
    "sl_buf":         [10, 15, 20, 25, 30],
    "gap_dir_filter": [True, False],
    "require_gap":    [True, False],
}

# Fixed settings (not being optimised)
FIXED = {
    "htf_min_zone":    60,
    "htf_min_cascade": 30,
    "lot_size":        200,   # 2 lots × 100
    "ltf_minutes":     [5, 30],
    "max_age_days":    5,
    "ltf_source":      "futures",
    "itm_offset":      300,
    "combo_filter":    "all",
    "fut_key":         "MCX_FO|520702",
}

LOOKBACK_DAYS = 10


def _trading_days(start: str, end: str) -> list[str]:
    result = []
    d = date.fromisoformat(start)
    ed = date.fromisoformat(end)
    while d <= ed:
        if d.weekday() < 5:
            result.append(d.isoformat())
        d += timedelta(days=1)
    return result


def _lookback_dates(trade_dt: str, n: int) -> list[str]:
    result = []
    d = date.fromisoformat(trade_dt) - timedelta(days=1)
    while len(result) < n:
        if d.weekday() < 5:
            result.append(d.isoformat())
        d -= timedelta(days=1)
    return list(reversed(result))


def _stats(trades: list[dict]) -> dict:
    if not trades:
        return {"count": 0, "wins": 0, "losses": 0, "win_pct": 0.0,
                "total_rs": 0, "avg_win": 0.0, "avg_loss": 0.0,
                "profit_factor": 0.0, "max_dd": 0.0}
    wins   = [t for t in trades if t["pnl_rs"] > 0]
    losses = [t for t in trades if t["pnl_rs"] <= 0]
    gross_win  = sum(t["pnl_rs"] for t in wins)
    gross_loss = abs(sum(t["pnl_rs"] for t in losses))
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")

    # max drawdown from equity curve
    equity, peak, dd = 0.0, 0.0, 0.0
    for t in trades:
        equity += t["pnl_rs"]
        if equity > peak:
            peak = equity
        dd = max(dd, peak - equity)

    return {
        "count":         len(trades),
        "wins":          len(wins),
        "losses":        len(losses),
        "win_pct":       100 * len(wins) / len(trades),
        "total_rs":      sum(t["pnl_rs"] for t in trades),
        "avg_win":       gross_win / len(wins) if wins else 0.0,
        "avg_loss":      -gross_loss / len(losses) if losses else 0.0,
        "profit_factor": round(pf, 2),
        "max_dd":        dd,
    }


def fetch_all_data(trading_days: list[str], fut_key: str) -> dict:
    """Fetch and cache all 1m bar data upfront so the grid loop is fast."""
    print(f"Fetching data for {len(trading_days)} trading days + lookback…")
    cache: dict = {}
    needed = set()
    for td in trading_days:
        needed.add(td)
        for lb in _lookback_dates(td, LOOKBACK_DAYS):
            needed.add(lb)

    for i, dt in enumerate(sorted(needed)):
        if dt not in cache:
            df = fetch_1m(fut_key, dt)
            cache[dt] = df
            time.sleep(0.25)
        if (i + 1) % 10 == 0:
            print(f"  fetched {i+1}/{len(needed)} days…")

    print(f"Data ready — {len(cache)} days cached.\n")
    return cache


def build_day_frames(trading_days: list[str], cache: dict) -> dict:
    """Pre-build (today_df, lookback_df) once per trading day — reused across all combos."""
    import pandas as pd
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


def run_combo(params: dict, trading_days: list[str], day_frames: dict) -> list[dict]:
    """Run one parameter combination over all trading days using pre-built frames."""
    all_trades = []
    for td in trading_days:
        if td not in day_frames:
            continue
        today_df, lookback_df = day_frames[td]

        trades = _run_day(
            td, today_df, lookback_df,
            params["htf_min_zone"],
            params["htf_min_cascade"],
            params["sl_buf"],
            params["gap_threshold"],
            params["combo_filter"],
            params["lot_size"],
            params["ltf_minutes"],
            params["min_zone_width"],
            params["max_age_days"],
            params["ltf_source"],
            params["itm_offset"],
            params["gap_dir_filter"],
            params["require_gap"],
        )
        all_trades.extend(trades)
    return all_trades


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--token",  required=True, help="Upstox access token")
    ap.add_argument("--start",  required=True, help="Start date YYYY-MM-DD")
    ap.add_argument("--end",    required=True, help="End date YYYY-MM-DD")
    ap.add_argument("--top",    type=int, default=20, help="Show top N results")
    ap.add_argument("--sort",   default="profit_factor",
                    choices=["profit_factor", "total_rs", "win_pct"],
                    help="Sort metric")
    ap.add_argument("--min-trades", type=int, default=3,
                    help="Minimum trades to include in results")
    args = ap.parse_args()

    # Set auth token for all fetch_1m calls
    import scripts.backtest_engine as eng
    eng._HEADERS = {"Authorization": f"Bearer {args.token}", "Accept": "application/json"}

    trading_days = _trading_days(args.start, args.end)
    print(f"Optimising {args.start} → {args.end} ({len(trading_days)} trading days)")

    # Build grid
    keys   = list(GRID.keys())
    values = list(GRID.values())
    combos = list(itertools.product(*values))
    total  = len(combos)
    print(f"Grid: {' × '.join(str(len(v)) for v in values)} = {total} combinations\n")

    # Fetch all data once, then pre-build day frames (pd.concat done once per day)
    cache = fetch_all_data(trading_days, FIXED["fut_key"])
    print("Pre-building day frames (done once, reused across all 980 combos)…")
    day_frames = build_day_frames(trading_days, cache)
    print(f"Ready — {len(day_frames)} days with data.\n")

    # Run grid search — only require_gap=True combos (False adds noise, known loser)
    # This halves the grid to 490 combos and avoids the slow no-gap scan
    print(f"Running grid search ({total} combos, require_gap=True only → 490)…", flush=True)
    results = []
    t0 = time.time()
    for i, combo in enumerate(combos):
        params = dict(zip(keys, combo))
        params.update(FIXED)

        # Skip require_gap=False — confirmed loser in all analysis
        if not params["require_gap"]:
            continue

        trades = run_combo(params, trading_days, day_frames)
        s = _stats(trades)

        if s["count"] >= args.min_trades:
            results.append({**params, **s})

        done = i + 1
        if done % 50 == 0:
            elapsed = time.time() - t0
            rate = done / elapsed
            eta = (total - done) / rate
            print(f"  {done}/{total}  elapsed={elapsed:.0f}s  ETA={eta:.0f}s  "
                  f"found={len(results)}", flush=True)

    print(f"\nDone. {len(results)} combinations with ≥{args.min_trades} trades.\n")

    # Sort and display
    results.sort(key=lambda r: r[args.sort], reverse=True)
    top = results[:args.top]

    # Header
    w = 6
    print(f"{'RANK':>4}  {'GAP%':>5}  {'WIDTH':>5}  {'SL':>4}  {'DIR':>4}  {'REQ':>4}  "
          f"{'TRADES':>6}  {'WIN%':>5}  {'TOTAL_RS':>10}  {'AVG_WIN':>8}  {'AVG_LOSS':>9}  "
          f"{'PF':>5}  {'MAX_DD':>8}")
    print("─" * 100)

    for rank, r in enumerate(top, 1):
        dir_flag = "ON " if r["gap_dir_filter"] else "OFF"
        req_flag = "ON " if r["require_gap"]    else "OFF"
        print(
            f"{rank:>4}  "
            f"{r['gap_threshold']*100:>5.2f}  "
            f"{r['min_zone_width']:>5.0f}  "
            f"{r['sl_buf']:>4.0f}  "
            f"{dir_flag:>4}  "
            f"{req_flag:>4}  "
            f"{r['count']:>6}  "
            f"{r['win_pct']:>5.1f}  "
            f"{r['total_rs']:>+10,.0f}  "
            f"{r['avg_win']:>+8,.0f}  "
            f"{r['avg_loss']:>+9,.0f}  "
            f"{r['profit_factor']:>5.2f}  "
            f"{r['max_dd']:>8,.0f}"
        )

    print("\n─" * 100)
    if top:
        best = top[0]
        print(f"\n★ BEST SETTINGS ({args.sort}):")
        print(f"   Gap threshold:   {best['gap_threshold']*100:.2f}%")
        print(f"   Min zone width:  {best['min_zone_width']:.0f} pts")
        print(f"   SL buffer:       {best['sl_buf']:.0f} pts")
        print(f"   Gap dir filter:  {'ON' if best['gap_dir_filter'] else 'OFF'}")
        print(f"   Require gap:     {'ON' if best['require_gap'] else 'OFF'}")
        print(f"   → {best['count']} trades  {best['win_pct']:.1f}% win  "
              f"Rs {best['total_rs']:+,.0f}  PF={best['profit_factor']}")


if __name__ == "__main__":
    main()
