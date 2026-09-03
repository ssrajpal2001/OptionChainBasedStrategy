"""
scripts/fno_cascade_sr_backtest.py — 2026-08-11.

Backtests CascadeSRTracker (strategies/d1_trap_option/cascade_sr.py) --
the 3-tier daily-zone / mid-tf ref-candle-break / fine-tf re-zone+S&R
mechanic built to the user's own manual trading method, distinct from and
NOT a replacement for the already-live, already-validated PositionalSRTracker
(daily-only) design.

LIMITATION (real, not a bug): Upstox's 1-minute intraday history for these
stocks only goes back reliably ~30 days (confirmed empirically -- 2+ months
back returns zero bars). Daily zone detection still uses the full 2-year
daily history; the intraday cascade simulation itself is bounded to
whichever zone-touches happen to fall within the last ~30 days. Expect a
much thinner sample than the daily-only backtest -- flagged in the output,
not hidden.

Sweeps mid-tf x fine-tf x exit-variant, reports PF/win%/n AND avg initial
SL distance (points and %) per config -- the SL-size metric is the whole
point of this experiment (daily-only R1/S1 anchors can be stale and wide
by the time the sequence finally completes; this checks whether tighter
intraday anchors actually help).

Usage:
    python3 scripts/fno_cascade_sr_backtest.py --stocks RELIANCE,TCS,INFY
    python3 scripts/fno_cascade_sr_backtest.py   # full FNO_STOCK_CONFIG universe
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.global_config import IST, FNO_STOCK_CONFIG  # noqa: E402
from data_layer.client_db import ClientDB  # noqa: E402
from strategies.d1_trap_option.book import _fetch_bars  # noqa: E402
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones  # noqa: E402
from strategies.d1_trap_option.cascade_sr import CascadeSRTracker, resample_bars, _Bar  # noqa: E402

_MAX_ZONE_AGE_DAYS = 20
INTRADAY_LOOKBACK_DAYS = 29   # empirically the real Upstox limit for 1minute


def _daily_bars_to_cascade_bars(bars) -> List[_Bar]:
    return [_Bar(timestamp=b.timestamp, open=b.open, high=b.high, low=b.low, close=b.close) for b in bars]


async def backtest_one(symbol: str, token: str, mid_minutes: int, fine_minutes: int,
                        exit_sr_confirm: bool, require_retest: bool = True) -> dict:
    cfg = FNO_STOCK_CONFIG.get(symbol)
    if not cfg:
        return dict(symbol=symbol, error="not in FNO_STOCK_CONFIG")
    key = cfg["upstox_key"]
    today = datetime.now(IST).date()

    daily_start = today - timedelta(days=730)
    daily_bars_raw = await asyncio.to_thread(_fetch_bars, key, "day", daily_start, today, token)
    if len(daily_bars_raw) < 30:
        return dict(symbol=symbol, error=f"only {len(daily_bars_raw)} daily bars")
    daily_bars = _daily_bars_to_cascade_bars(daily_bars_raw)

    intraday_start = today - timedelta(days=INTRADAY_LOOKBACK_DAYS)
    m1_raw = await asyncio.to_thread(_fetch_bars, key, "1minute", intraday_start, today, token)
    if len(m1_raw) < 100:
        return dict(symbol=symbol, error=f"only {len(m1_raw)} 1-min bars")
    m1_bars = _daily_bars_to_cascade_bars(m1_raw)

    mid_bars = resample_bars(m1_bars, mid_minutes)
    fine_bars = resample_bars(m1_bars, fine_minutes)
    if len(mid_bars) < 5 or len(fine_bars) < 5:
        return dict(symbol=symbol, error="not enough resampled bars")

    # Only simulate the cascade over the window where we actually have intraday
    # data -- daily zones are still built incrementally over the FULL daily
    # history up to each simulated day, no lookahead, same discipline as
    # fno_positional_sr_backtest.py.
    sim_start = m1_bars[0].timestamp.date()

    zones_long: List[dict] = []
    zones_short: List[dict] = []
    known_bear: set = set()
    known_bull: set = set()

    def _zd(z, side):
        return dict(side=side, zone_lo=min(z.entry_line, z.sweep_low), zone_hi=max(z.entry_line, z.sweep_low),
                    lock_ts=z.lock_ts, ref_ts=z.reference_low_ts)

    def grow_zones(as_of_date: date):
        avail = [b for b in daily_bars if b.timestamp.date() < as_of_date]
        if len(avail) < 3:
            return
        age_cutoff = (datetime.combine(as_of_date, datetime.min.time()).replace(tzinfo=IST)
                      - timedelta(days=_MAX_ZONE_AGE_DAYS))
        for z in find_all_bear_zones(avail, known_ref_ts=known_bear):
            if z.reference_low_ts not in known_bear and z.lock_ts >= age_cutoff:
                known_bear.add(z.reference_low_ts)
                zones_long.append(_zd(z, "LONG"))
        for z in find_all_bull_zones(avail, known_ref_ts=known_bull):
            if z.reference_low_ts not in known_bull and z.lock_ts >= age_cutoff:
                known_bull.add(z.reference_low_ts)
                zones_short.append(_zd(z, "SHORT"))

    # Seed with everything known as of the first simulated day.
    grow_zones(sim_start)

    tracker = CascadeSRTracker(zones_long, zones_short, exit_sr_confirm=exit_sr_confirm,
                                require_retest=require_retest)
    trades: List[dict] = []
    open_entry: Optional[dict] = None

    last_day = None
    mid_i = 0
    fine_i = 0

    for m1 in m1_bars:
        day = m1.timestamp.date()
        if day != last_day:
            if last_day is not None:
                # Feed the just-completed daily bar (real one from daily_bars, not
                # reconstructed from 1-min, so prev-day high/low match zone detection).
                prior = next((b for b in daily_bars if b.timestamp.date() == last_day), None)
                if prior is not None:
                    tracker.on_daily_bar(prior)
            grow_zones(day)
            last_day = day

        tracker.on_intraday_tick_for_touch(m1.close, m1.timestamp)

        # Process EVERY mid-tf bucket that has fully completed as of this m1 bar --
        # a while loop, not if/elif, because overnight/weekend gaps (market closes
        # 15:30, reopens 09:15 next day) cross several wall-clock buckets between
        # consecutive 1-min bars. An index-advance-then-process-only-the-latest
        # pattern silently skips every bucket but the last one across a gap --
        # confirmed live via a diagnostic trace: on_mid_bar was being called ZERO
        # times over a full month with that bug, not because the mechanic never
        # fires. This must catch up on all of them, in order, every time.
        while mid_i < len(mid_bars) and mid_bars[mid_i].timestamp + timedelta(minutes=mid_minutes) <= m1.timestamp:
            completed_mid = mid_bars[mid_i]
            tracker.on_mid_bar(completed_mid)
            mid_i += 1
            for ekey in tracker.entry_keys_awaiting_rezone():
                st_ref = tracker._entry_states[ekey].ref_candle
                window = [b for b in fine_bars
                          if st_ref.timestamp <= b.timestamp < st_ref.timestamp + timedelta(minutes=mid_minutes)]
                tracker.complete_entry_rezone(ekey, window)
            if tracker.exit_awaiting_rezone():
                st_ref = tracker._exit_state.ref_candle
                window = [b for b in fine_bars
                          if st_ref.timestamp <= b.timestamp < st_ref.timestamp + timedelta(minutes=mid_minutes)]
                tracker.complete_exit_rezone(window)

        while fine_i < len(fine_bars) and fine_bars[fine_i].timestamp + timedelta(minutes=fine_minutes) <= m1.timestamp:
            completed_fine = fine_bars[fine_i]
            fine_i += 1
            ev = tracker.on_fine_bar(completed_fine)
            if ev is not None:
                if ev["type"] == "entry":
                    open_entry = ev
                elif ev["type"] == "exit" and open_entry is not None:
                    sign = 1 if ev["side"] == "LONG" else -1
                    pnl_pct = sign * (ev["exit_price"] - open_entry["entry_price"]) / open_entry["entry_price"]
                    sl = open_entry.get("sl")
                    sl_dist_pct = (abs(open_entry["entry_price"] - sl) / open_entry["entry_price"]
                                   if sl else None)
                    trades.append(dict(
                        side=ev["side"], entry_ts=str(open_entry["entry_ts"]),
                        entry_price=open_entry["entry_price"], exit_ts=str(ev["exit_ts"]),
                        exit_price=ev["exit_price"], reason=ev["reason"],
                        pnl_pct=round(pnl_pct * 100, 2),
                        sl_dist_pct=round(sl_dist_pct * 100, 2) if sl_dist_pct is not None else None,
                    ))
                    open_entry = None

    wins = [t["pnl_pct"] for t in trades if t["pnl_pct"] > 0]
    losses = [t["pnl_pct"] for t in trades if t["pnl_pct"] <= 0]
    gl = abs(sum(losses))
    pf = (sum(wins) / gl) if gl > 0 else (float("inf") if wins else 0.0)
    sl_dists = [t["sl_dist_pct"] for t in trades if t["sl_dist_pct"] is not None]
    return dict(
        symbol=symbol, n=len(trades),
        win_pct=round(100 * len(wins) / len(trades), 1) if trades else 0.0,
        avg_pnl_pct=round(sum(t["pnl_pct"] for t in trades) / len(trades), 2) if trades else 0.0,
        pf=("inf" if pf == float("inf") else round(pf, 2)),
        avg_sl_dist_pct=round(sum(sl_dists) / len(sl_dists), 2) if sl_dists else None,
        trades=trades,
    )


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stocks", default="")
    args = ap.parse_args()

    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    token = (creds or {}).get("access_token", "")
    if not token:
        print("FATAL: no Upstox access_token in data/clients.db.")
        return 1

    stocks = [s.strip().upper() for s in args.stocks.split(",") if s.strip()] or list(FNO_STOCK_CONFIG.keys())
    print(f"CascadeSRTracker backtest: {len(stocks)} stocks, intraday lookback ~{INTRADAY_LOOKBACK_DAYS}d\n")

    MID_SWEEP = (30, 60, 120)
    FINE_SWEEP = (3, 5, 15)
    EXIT_VARIANTS = (False, True)   # exit_sr_confirm

    all_results = {}
    for mid_m in MID_SWEEP:
        for fine_m in FINE_SWEEP:
            if fine_m >= mid_m:
                continue
            for exit_confirm in EXIT_VARIANTS:
                tag = f"mid{mid_m}_fine{fine_m}_exit{'sr' if exit_confirm else 'simple'}"
                print(f"--- {tag} ---")
                results = []
                for sym in stocks:
                    try:
                        r = await backtest_one(sym, token, mid_m, fine_m, exit_confirm)
                    except Exception as exc:
                        r = dict(symbol=sym, error=f"{type(exc).__name__}: {exc}")
                    results.append(r)
                    if r.get("error"):
                        print(f"  {sym}: SKIP -- {r['error']}")
                    else:
                        print(f"  {sym}: n={r['n']} win%={r['win_pct']} PF={r['pf']} "
                              f"avg_sl_dist%={r['avg_sl_dist_pct']}")
                all_results[tag] = results

    print(f"\n{'='*70}\nCASCADE SWEEP SUMMARY")
    print(f"  {'Config':<28}{'Trades':>8}{'Win%':>7}{'PF':>8}{'AvgSLdist%':>12}")
    for tag, results in all_results.items():
        valid = [r for r in results if not r.get("error")]
        all_trades = [t for r in valid for t in r["trades"]]
        if not all_trades:
            print(f"  {tag:<28}{'0':>8}   (no trades)")
            continue
        wins = [t["pnl_pct"] for t in all_trades if t["pnl_pct"] > 0]
        losses = [t["pnl_pct"] for t in all_trades if t["pnl_pct"] <= 0]
        gl = abs(sum(losses))
        pf = (sum(wins) / gl) if gl > 0 else (float("inf") if wins else 0.0)
        win_pct = 100 * len(wins) / len(all_trades)
        sl_dists = [t["sl_dist_pct"] for t in all_trades if t["sl_dist_pct"] is not None]
        avg_sl = sum(sl_dists) / len(sl_dists) if sl_dists else None
        pf_str = "inf" if pf == float("inf") else f"{pf:.2f}"
        print(f"  {tag:<28}{len(all_trades):>8}{win_pct:>6.1f}%{pf_str:>8}"
              f"{(f'{avg_sl:.2f}%' if avg_sl is not None else '—'):>12}")

    out_path = Path(__file__).resolve().parents[1] / "data" / "sweeps" / "fno_cascade_sr_sweep.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(all_results, indent=2, default=str), encoding="utf-8")
    print(f"\n-> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
