"""
CrudeOil Pure Intraday Optimizer
---------------------------------
NO gap filter. NO gap direction. Every day.
30min trap → 5min entry (IMMEDIATE / FIRST / LAST).

Grid: zone_width × sl_buf = 35 combos → fast.

Usage:
  python scripts/optimize_backtest.py --token TOKEN --start 2026-06-05 --end 2026-06-05
  python scripts/optimize_backtest.py --token TOKEN --start 2026-05-20 --end 2026-06-19
"""

import argparse, sys, time
from datetime import date, timedelta
from itertools import product

import pandas as pd

sys.path.insert(0, ".")
from scripts.backtest_engine import fetch_1m, _run_day
import scripts.backtest_engine as eng

# ── Grid: only the variables that matter for pure intraday ─────────────────
ZONE_WIDTHS = [10, 15, 20, 25, 30, 35, 40]
SL_BUFS     = [10, 15, 20, 25, 30]

# Fixed for pure intraday — no gap logic at all
FIXED = {
    "htf_min_zone":    60,
    "htf_min_cascade": 30,    # 30min HTF cascade
    "ltf_minutes":     [5],   # 5min LTF only (30min → 5min drill-down)
    "lot_size":        200,   # 2 lots × 100
    "max_age_days":    5,
    "ltf_source":      "futures",
    "itm_offset":      300,
    "combo_filter":    "all",
    "fut_key":         "MCX_FO|520702",
    "gap_threshold":   0.001, # effectively 0 → every day has a "gap"
    "gap_dir_filter":  False, # no direction bias
    "require_gap":     False, # EVERY day runs cascade
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


def run_one(zone_width: float, sl_buf: float,
            trading_days: list[str], day_frames: dict) -> list[dict]:
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
            FIXED["gap_threshold"],
            FIXED["combo_filter"],
            FIXED["lot_size"],
            FIXED["ltf_minutes"],
            zone_width,
            FIXED["max_age_days"],
            FIXED["ltf_source"],
            FIXED["itm_offset"],
            FIXED["gap_dir_filter"],
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


def stats_by_entry(trades: list[dict]) -> dict:
    """Break down by entry type (IMMEDIATE / FIRST / LAST)."""
    out = {}
    for etype in ("IMMEDIATE", "FIRST", "LAST"):
        sub = [t for t in trades if etype in t.get("combo", "")]
        out[etype] = stats(sub)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--token",      required=True)
    ap.add_argument("--start",      required=True)
    ap.add_argument("--end",        required=True)
    ap.add_argument("--top",        type=int, default=15)
    ap.add_argument("--sort",       default="profit_factor",
                    choices=["profit_factor", "total_rs", "win_pct"])
    ap.add_argument("--min-trades", type=int, default=2)
    args = ap.parse_args()

    eng._HEADERS = {"Authorization": f"Bearer {args.token}",
                    "Accept": "application/json"}

    trading_days = _trading_days(args.start, args.end)
    total_combos = len(ZONE_WIDTHS) * len(SL_BUFS)
    print(f"Pure Intraday Optimizer  |  30min HTF → 5min LTF  |  No gap filter", flush=True)
    print(f"Period: {args.start} → {args.end}  "
          f"({len(trading_days)} days, {total_combos} combos)\n", flush=True)

    cache      = fetch_all_data(trading_days, FIXED["fut_key"])
    day_frames = build_day_frames(trading_days, cache)
    print(f"Day frames ready: {len(day_frames)} days.\n", flush=True)

    results = []
    t0 = time.time()

    for i, (zone_width, sl_buf) in enumerate(product(ZONE_WIDTHS, SL_BUFS)):
        trades = run_one(zone_width, sl_buf, trading_days, day_frames)
        s      = stats(trades)
        by_e   = stats_by_entry(trades)

        done = i + 1
        elapsed = time.time() - t0
        eta     = (total_combos - done) / (done / elapsed) if done > 0 else 0
        print(f"  {done}/{total_combos}  width={zone_width}  sl={sl_buf}  "
              f"trades={s['count']}  win={s['win_pct']:.0f}%  "
              f"Rs{s['total_rs']:+,.0f}  PF={s['profit_factor']:.2f}  "
              f"ETA={eta:.0f}s", flush=True)

        if s["count"] >= args.min_trades:
            results.append(dict(
                zone_width=zone_width, sl_buf=sl_buf,
                imm_trades=by_e["IMMEDIATE"]["count"],
                imm_win=by_e["IMMEDIATE"]["win_pct"],
                imm_rs=by_e["IMMEDIATE"]["total_rs"],
                first_trades=by_e["FIRST"]["count"],
                first_win=by_e["FIRST"]["win_pct"],
                first_rs=by_e["FIRST"]["total_rs"],
                last_trades=by_e["LAST"]["count"],
                last_win=by_e["LAST"]["win_pct"],
                last_rs=by_e["LAST"]["total_rs"],
                **s,
            ))

    print(f"\nDone — {len(results)} combos  ({time.time()-t0:.0f}s)\n", flush=True)

    results.sort(key=lambda r: r[args.sort], reverse=True)
    top = results[:args.top]

    # Summary table
    print(f"{'#':>3}  {'WID':>4}  {'SL':>4}  "
          f"{'N':>4}  {'WIN%':>5}  {'TOTAL':>10}  {'PF':>5}  {'DD':>8}  "
          f"| IMM({' N':>3}/{' W%':>4}/{' Rs':>8})  "
          f"FIRST({' N':>3}/{' W%':>4}/{' Rs':>8})  "
          f"LAST({' N':>3}/{' W%':>4}/{' Rs':>8})")
    print("─" * 130)

    for rank, r in enumerate(top, 1):
        print(
            f"{rank:>3}  "
            f"{r['zone_width']:>4}  "
            f"{r['sl_buf']:>4}  "
            f"{r['count']:>4}  "
            f"{r['win_pct']:>5.1f}  "
            f"{r['total_rs']:>+10,.0f}  "
            f"{r['profit_factor']:>5.2f}  "
            f"{r['max_dd']:>8,.0f}  "
            f"| {r['imm_trades']:>4} {r['imm_win']:>5.0f}% {r['imm_rs']:>+8,.0f}  "
            f"  {r['first_trades']:>4} {r['first_win']:>5.0f}% {r['first_rs']:>+8,.0f}  "
            f"  {r['last_trades']:>4} {r['last_win']:>5.0f}% {r['last_rs']:>+8,.0f}"
        )

    if top:
        b = top[0]
        print(f"\n★  BEST ({args.sort}):")
        print(f"   Zone width: {b['zone_width']} pts")
        print(f"   SL buffer:  {b['sl_buf']} pts")
        print(f"   Total:      {b['count']} trades  {b['win_pct']:.1f}% win  "
              f"Rs {b['total_rs']:+,.0f}  PF={b['profit_factor']}")
        print(f"   IMMEDIATE:  {b['imm_trades']} trades  {b['imm_win']:.0f}% win  "
              f"Rs {b['imm_rs']:+,.0f}")
        print(f"   FIRST:      {b['first_trades']} trades  {b['first_win']:.0f}% win  "
              f"Rs {b['first_rs']:+,.0f}")
        print(f"   LAST:       {b['last_trades']} trades  {b['last_win']:.0f}% win  "
              f"Rs {b['last_rs']:+,.0f}")


if __name__ == "__main__":
    main()
