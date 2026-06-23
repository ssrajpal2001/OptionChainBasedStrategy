#!/usr/bin/env python3
"""
scripts/ic_backtest.py — Iron Condor INTRADAY backtest (NIFTY etc.).

Models the live IronCondor engine's core mechanics from your settings JSON:
  • Entry at start_time: SELL short CE/PE at ATM ± short_leg_otm_pts,
    BUY hedge CE/PE at short ± long_leg_otm_pts.
  • Square off intraday at squareoff_time (--mode intraday: re-enter next day).
  • Exit triggers per 1-min bar: profit_target_inr, stoploss_inr,
    ratio_exit_threshold (short-leg LTP ratio), and EOD squareoff.

NOT modelled (v1, documented): ratio_trigger ROLL adjustments
(max_adjustments_per_side / roll_step_pts). The engine rolls the tested side
out; here we exit on ratio_exit_threshold instead. Rolling is a v2 add.

VWAP/ATP are NOT used by Iron Condor (LTP + ratio only), so this backtest is
faithful to the live entry/exit logic — no ATP approximation needed.

Usage:
  python scripts/ic_backtest.py --token TOKEN --index NIFTY --days 20 --lots 1
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime, timedelta

import pandas as pd

sys.path.insert(0, ".")

from scripts.nifty_backtest import (
    _fetch_1m,
    _mkt_hours,
    _option_key,
    INDEX_CFG,
)
import scripts.nifty_backtest as _nb
from data_layer.instrument_registry import REGISTRY


# ── Iron Condor settings (from the deployment JSON, indices.<IDX>.iron_condor) ──
IC_CFG = {
    "NIFTY": {
        "start_time":        "09:16",
        "squareoff_time":    "15:15",
        "lot_size":          65,
        "strike_step":       50,
        "short_leg_otm_pts": 300,
        "long_leg_otm_pts":  150,
        "profit_target_inr": 5000.0,
        "stoploss_inr":      2000.0,
        "ratio_exit_threshold": 3.0,
    },
    "SENSEX": {
        "start_time":        "09:16",
        "squareoff_time":    "15:15",
        "lot_size":          20,
        "strike_step":       100,
        "short_leg_otm_pts": 500,
        "long_leg_otm_pts":  250,
        "profit_target_inr": 5000.0,
        "stoploss_inr":      2000.0,
        "ratio_exit_threshold": 3.0,
    },
}


def _hm(s: str) -> tuple[int, int]:
    h, m = s.split(":")
    return int(h), int(m)


def _bar_hm(ts) -> tuple[int, int]:
    return ts.hour, ts.minute


def _atm(spot_open: float, step: int) -> int:
    return int(round(spot_open / step) * step)


def _leg_series(index: str, strike: int, opt_type: str, td: date,
                fetch_from: str, today_str: str, cache: dict) -> pd.DataFrame:
    """1-min bars for one option leg on day `td` (intraday only)."""
    key = _option_key(index, strike, opt_type, td)
    if not key:
        return pd.DataFrame()

    def _has_day(df) -> bool:
        return (not df.empty) and "datetime" in df.columns \
            and not df[df["datetime"].dt.date == td].empty

    df_raw = cache.get(key, pd.DataFrame())
    # Retry until THIS day's bars are present. `_fetch_1m` returns PARTIAL data when a
    # 28-day chunk hits a transient DNS/rate-limit error — the df is non-empty but may be
    # missing `td`, so checking emptiness alone would wrongly skip a day whose data exists.
    if not _has_day(df_raw):
        for attempt in range(5):
            try:
                fresh = _fetch_1m(key, fetch_from, today_str)
            except Exception as exc:
                print(f"  {today_str} {strike}{opt_type} fetch error (try {attempt+1}): {exc}")
                fresh = pd.DataFrame()
            if _has_day(fresh):
                df_raw = fresh
                break
            # keep the larger partial so far (more chunks = closer to complete)
            if len(fresh) > len(df_raw):
                df_raw = fresh
            time.sleep(0.7 * (attempt + 1))
        cache[key] = df_raw
        time.sleep(0.25)
    if not _has_day(df_raw):
        return pd.DataFrame()
    df = _mkt_hours(df_raw)
    df = df[df["datetime"].dt.date == td][["datetime", "close"]].copy()
    return df.reset_index(drop=True)


def _run_day(index: str, ic: dict, td: date, df_spot_today: pd.DataFrame,
             lots: int, cache: dict, fetch_from: str) -> dict | None:
    if df_spot_today.empty:
        return None
    step = int(ic["strike_step"])
    spot_open = float(df_spot_today.iloc[0]["open"])
    atm = _atm(spot_open, step)
    short_ce = atm + int(ic["short_leg_otm_pts"])
    short_pe = atm - int(ic["short_leg_otm_pts"])
    long_ce  = short_ce + int(ic["long_leg_otm_pts"])
    long_pe  = short_pe - int(ic["long_leg_otm_pts"])

    today_str = td.isoformat()
    legs = {
        "sce": _leg_series(index, short_ce, "CE", td, fetch_from, today_str, cache),
        "spe": _leg_series(index, short_pe, "PE", td, fetch_from, today_str, cache),
        "lce": _leg_series(index, long_ce, "CE", td, fetch_from, today_str, cache),
        "lpe": _leg_series(index, long_pe, "PE", td, fetch_from, today_str, cache),
    }
    for name, df in legs.items():
        if df.empty:
            print(f"  {today_str}: no bars for {name} → skip day")
            return None

    # Align all 4 legs on the minute (inner join → only bars present in all legs).
    merged = legs["sce"].rename(columns={"close": "sce"})
    for k in ("spe", "lce", "lpe"):
        merged = merged.merge(legs[k].rename(columns={"close": k}), on="datetime", how="inner")
    if merged.empty:
        print(f"  {today_str}: legs do not overlap → skip")
        return None

    sh, sm = _hm(ic["start_time"])
    qh, qm = _hm(ic["squareoff_time"])
    qty = int(ic["lot_size"]) * lots

    # Entry = first aligned bar at/after start_time.
    entry_rows = merged[merged["datetime"].apply(lambda t: (t.hour, t.minute) >= (sh, sm))]
    if entry_rows.empty:
        return None
    e = entry_rows.iloc[0]
    sce0, spe0, lce0, lpe0 = float(e.sce), float(e.spe), float(e.lce), float(e.lpe)
    net_credit = (sce0 + spe0) - (lce0 + lpe0)      # per share
    entry_ts = e["datetime"]

    pt   = float(ic["profit_target_inr"])
    sl   = float(ic["stoploss_inr"])
    rexit = float(ic["ratio_exit_threshold"])

    exit_reason = "EOD"
    exit_ts = None
    pnl_inr = 0.0
    peak = float("-inf")
    trough = float("inf")
    for _, r in entry_rows.iterrows():
        ts = r["datetime"]
        if (ts.hour, ts.minute) >= (qh, qm):
            # square-off bar
            net_close = (float(r.sce) + float(r.spe)) - (float(r.lce) + float(r.lpe))
            pnl_inr = (net_credit - net_close) * qty
            exit_reason, exit_ts = "EOD", ts
            break
        net_close = (float(r.sce) + float(r.spe)) - (float(r.lce) + float(r.lpe))
        pnl = (net_credit - net_close) * qty
        peak = max(peak, pnl); trough = min(trough, pnl)
        # short-leg ratio (richer / cheaper)
        hi, lo = max(float(r.sce), float(r.spe)), min(float(r.sce), float(r.spe))
        ratio = (hi / lo) if lo > 0 else 0.0
        if pnl >= pt:
            pnl_inr, exit_reason, exit_ts = pnl, "TARGET", ts; break
        if pnl <= -sl:
            pnl_inr, exit_reason, exit_ts = pnl, "SL", ts; break
        if rexit > 0 and ratio >= rexit:
            pnl_inr, exit_reason, exit_ts = pnl, "RATIO", ts; break
    else:
        # ran out of bars before squareoff_time → mark to last bar
        r = entry_rows.iloc[-1]
        net_close = (float(r.sce) + float(r.spe)) - (float(r.lce) + float(r.lpe))
        pnl_inr = (net_credit - net_close) * qty
        exit_reason, exit_ts = "EOD", r["datetime"]

    return {
        "date": today_str,
        "atm": atm,
        "short_ce": short_ce, "short_pe": short_pe,
        "long_ce": long_ce, "long_pe": long_pe,
        "credit": round(net_credit, 2),
        "entry_ts": entry_ts.strftime("%H:%M"),
        "exit_ts": exit_ts.strftime("%H:%M") if exit_ts is not None else "—",
        "reason": exit_reason,
        "pnl": round(pnl_inr, 0),
        "max_fav": round(peak if peak != float("-inf") else 0, 0),
        "max_adv": round(trough if trough != float("inf") else 0, 0),
    }


def run_ic_backtest(token: str, index: str, days: int, lots: int) -> dict:
    index = index.upper()
    ic = IC_CFG.get(index)
    if ic is None:
        raise SystemExit(f"No IC config for {index}. Add it to IC_CFG.")
    _nb._HEADERS.clear()
    _nb._HEADERS.update({"Authorization": f"Bearer {token}", "Accept": "application/json"})

    if not REGISTRY.is_loaded(index):
        print(f"Loading REGISTRY for {index}...")
        REGISTRY.load_sync(index, token)

    spot_key = INDEX_CFG[index]["spot_key"]
    # Build trading-day list (skip weekends; holidays drop out when spot has no bars).
    trading_days: list[date] = []
    d = date.today() - timedelta(days=1)
    while len(trading_days) < days:
        if d.weekday() < 5:
            trading_days.append(d)
        d -= timedelta(days=1)
    trading_days.reverse()
    fetch_from = (trading_days[0] - timedelta(days=5)).isoformat()
    today_str = date.today().isoformat()

    print(f"Fetching spot ({spot_key}) ...")
    df_spot_raw = _fetch_1m(spot_key, fetch_from, today_str)
    df_spot = _mkt_hours(df_spot_raw)

    cache: dict = {}
    results: list[dict] = []
    for td in trading_days:
        df_spot_today = df_spot[df_spot["datetime"].dt.date == td]
        if df_spot_today.empty:
            continue
        print(f"\n--- {td} ---")
        res = _run_day(index, ic, td, df_spot_today, lots, cache, fetch_from)
        if res:
            print(f"  ATM={res['atm']}  SHORT {res['short_ce']}CE/{res['short_pe']}PE  "
                  f"LONG {res['long_ce']}CE/{res['long_pe']}PE  credit={res['credit']}")
            print(f"  entry {res['entry_ts']}  exit {res['exit_ts']} [{res['reason']}]  "
                  f"P&L=Rs{res['pnl']:+.0f}  (MFE {res['max_fav']:+.0f} / MAE {res['max_adv']:+.0f})")
            results.append(res)

    return _summarise(results, index, lots)


def _summarise(results: list[dict], index: str, lots: int) -> dict:
    n = len(results)
    wins = [r for r in results if r["pnl"] > 0]
    losses = [r for r in results if r["pnl"] <= 0]
    total = sum(r["pnl"] for r in results)
    gross_win = sum(r["pnl"] for r in wins)
    gross_loss = -sum(r["pnl"] for r in losses)
    pf = (gross_win / gross_loss) if gross_loss > 0 else float("inf")

    print("\n" + "=" * 55)
    print(f"  IRON CONDOR (intraday) — {index}  lots={lots}")
    print(f"  Days traded : {n}   Wins: {len(wins)}   Losses: {len(losses)}")
    if n:
        print(f"  Win Rate    : {100*len(wins)/n:.1f}%")
    print(f"  Total P&L   : Rs{total:+,.0f}")
    if wins:
        print(f"  Avg Win     : Rs{gross_win/len(wins):+,.0f}")
    if losses:
        print(f"  Avg Loss    : Rs{-gross_loss/len(losses):+,.0f}")
    print(f"  Prof. Fac.  : {pf:.2f}")
    # by reason
    by = {}
    for r in results:
        by.setdefault(r["reason"], []).append(r["pnl"])
    print("\n  By Exit Reason:")
    for reason, pnls in sorted(by.items()):
        w = sum(1 for p in pnls if p > 0)
        print(f"    {reason:<8} {len(pnls):>3} trades  Rs{sum(pnls):+,.0f}  win={100*w/len(pnls):.0f}%")
    return {"trades": results, "total": total, "pf": pf, "n": n, "wins": len(wins)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--token", required=True)
    ap.add_argument("--index", default="NIFTY")
    ap.add_argument("--days", type=int, default=20)
    ap.add_argument("--lots", type=int, default=1)
    args = ap.parse_args()
    run_ic_backtest(args.token, args.index, args.days, args.lots)
