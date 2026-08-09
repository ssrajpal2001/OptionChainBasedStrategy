"""
scripts/fno_positional_sr_backtest.py — S&R ping-pong signal validation for
D1TrapOptionBook's d1_trap_fno (positional) mode, 2026-08-09, built per direct
user request the same evening as a next-day go-live decision.

IMPORTANT LIMITATION, read before trusting these numbers: this backtests the
S&R signal on each stock's OWN real daily price (real Upstox NSE_EQ history),
NOT real option premium -- unlike every index (NIFTY/SENSEX/BANKNIFTY) S&R
backtest this session, which all used real option premium. Fetching 1-2 years
of real option premium for enough strikes across even a handful of FnO stocks
is a materially bigger data-fetch than a single day's underlying candles, and
wasn't feasible in the time available before tomorrow's go-live. This answers
"does the entry/exit SIGNAL have real directional edge on the stock itself" --
it does NOT answer "what would the option P&L actually have been" (theta
decay, IV changes, bid/ask spread on less-liquid stock options are all
unmodeled). Treat this as a lower bar than BANKNIFTY/NIFTY/SENSEX got, not
the same one.

Mechanic: real daily bars -> find_all_bear_zones (LONG) / find_all_bull_zones
(SHORT), using book.py's own existing zone_lo/zone_hi formula (NOT bear_only_
book.py's option-premium-specific one) -> strategies.d1_trap_option.
support_resistance.PositionalSRTracker for entry (S&R ping-pong, swing-scale
== 1 daily bar per S&R candle since these ARE daily bars already) and exit
(day-low/day-high TSL ratchet + hard %-of-entry cap, mirroring book.py's own
proven positional exit).

Usage:
    python3 scripts/fno_positional_sr_backtest.py --years 2
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date, datetime, timedelta
from typing import List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.global_config import IST, FNO_STOCK_CONFIG  # noqa: E402
from data_layer.client_db import ClientDB  # noqa: E402
from strategies.d1_trap_option.book import _fetch_bars, _Bar  # noqa: E402
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones  # noqa: E402
from strategies.d1_trap_option.support_resistance import PositionalSRTracker  # noqa: E402

# 2026-08-09: expanded from a 10-stock representative subset to the FULL
# FNO_STOCK_CONFIG universe per direct user request ("check backtest for 30
# fno stocks"), now that the signal looked worth the deeper scrutiny.
STOCK_SUBSET = list(FNO_STOCK_CONFIG.keys())
_MAX_ZONE_AGE_DAYS = 20   # matches book.py's own D1 zone-age constant
_HARD_RISK_PCT = 0.10     # 10% of entry -- backstop only, TSL usually tighter


def _resample_n_days(bars: List[_Bar], n: int) -> List[_Bar]:
    """Group every N consecutive TRADING-day bars into one coarser bar --
    2026-08-09, per direct user request: intraday (BANKNIFTY/NIFTY/SENSEX)
    uses a LOWER S&R timeframe, positional FnO should use a HIGHER one, for
    both the breakout entry AND the TSL (not just entry) -- both derive from
    whatever bar granularity is fed in here, so resampling before zone
    detection + PositionalSRTracker affects both consistently. n=1 is a
    no-op (plain daily bars)."""
    if n <= 1:
        return bars
    out = []
    for i in range(0, len(bars), n):
        chunk = bars[i:i + n]
        if not chunk:
            continue
        out.append(_Bar(
            timestamp=chunk[0].timestamp, open=chunk[0].open,
            high=max(b.high for b in chunk), low=min(b.low for b in chunk),
            close=chunk[-1].close,
        ))
    return out


def _zone_dicts(zones, side: str) -> List[dict]:
    out = []
    for z in zones:
        out.append(dict(
            side=side, zone_lo=min(z.entry_line, z.sweep_low), zone_hi=max(z.entry_line, z.sweep_low),
            lock_ts=z.lock_ts, ref_ts=z.reference_low_ts,
        ))
    return out


async def backtest_one(symbol: str, start: date, end: date, token: str,
                        zone_days: int = 1, entry_days: int = 1) -> dict:
    """zone_days/entry_days DECOUPLED -- 2026-08-09, per direct user request to
    check whether zone detection benefits from a coarser/more-mature timeframe
    even though entry granularity already conclusively favors daily
    (scripts/fno_positional_sr_backtest.py's earlier swing-tf sweep: PF 6.40 at
    1-day vs 3.52/3.35/2.20 at 2/3/5-day, entry+zone coupled together). Zones
    are detected on zone_days-resampled bars; entries/exits still evaluated
    bar-by-bar on entry_days-resampled bars -- only zone_bars that have
    already CLOSED strictly before the current entry bar's timestamp are ever
    used (no lookahead)."""
    cfg = FNO_STOCK_CONFIG.get(symbol)
    if not cfg:
        return dict(symbol=symbol, error="not in FNO_STOCK_CONFIG")
    key = cfg["upstox_key"]
    daily_bars = await asyncio.to_thread(_fetch_bars, key, "day", start, end, token)
    if len(daily_bars) < 30:
        return dict(symbol=symbol, error=f"only {len(daily_bars)} daily bars returned")
    zone_bars = _resample_n_days(daily_bars, zone_days)
    entry_bars = _resample_n_days(daily_bars, entry_days)
    if len(entry_bars) < 15 or len(zone_bars) < 15:
        return dict(symbol=symbol, error=f"only {len(entry_bars)} entry-bars/{len(zone_bars)} zone-bars after resample")

    zones_long: List[dict] = []
    zones_short: List[dict] = []
    known_bear: set = set()
    known_bull: set = set()
    tracker = PositionalSRTracker(zones_long, zones_short, hard_risk_pct=_HARD_RISK_PCT)
    trades: List[dict] = []
    open_ev = None
    age_cutoff_days = _MAX_ZONE_AGE_DAYS * max(1, zone_days)
    zone_ptr = 0

    for i, bar in enumerate(entry_bars):
        while zone_ptr < len(zone_bars) and zone_bars[zone_ptr].timestamp < bar.timestamp:
            zone_ptr += 1
        avail = zone_bars[:zone_ptr]   # only zone-bars strictly closed before this entry bar
        if len(avail) >= 3:
            age_cutoff = bar.timestamp - timedelta(days=age_cutoff_days)
            for z in find_all_bear_zones(avail, known_ref_ts=known_bear):
                if z.reference_low_ts not in known_bear and z.lock_ts >= age_cutoff:
                    known_bear.add(z.reference_low_ts)
                    zones_long.append(_zone_dicts([z], "LONG")[0])
            for z in find_all_bull_zones(avail, known_ref_ts=known_bull):
                if z.reference_low_ts not in known_bull and z.lock_ts >= age_cutoff:
                    known_bull.add(z.reference_low_ts)
                    zones_short.append(_zone_dicts([z], "SHORT")[0])

        ev = tracker.on_bar(bar)
        if ev is None:
            continue
        if ev["type"] == "entry":
            open_ev = ev
        elif ev["type"] == "exit":
            hold_days = (ev["exit_ts"].date() - ev["entry_ts"].date()).days
            trades.append(dict(
                side=ev["side"], entry_ts=str(ev["entry_ts"].date()), entry_price=ev["entry_price"],
                exit_ts=str(ev["exit_ts"].date()), exit_price=ev["exit_price"], reason=ev["reason"],
                pnl_pct=round(ev["pnl_pct"] * 100, 2), hold_days=hold_days,
            ))
            open_ev = None

    if tracker.position is not None:
        last = entry_bars[-1]
        sign = 1 if tracker.position["side"] == "LONG" else -1
        pnl_pct = sign * (last.close - tracker.position["entry_price"]) / tracker.position["entry_price"]
        trades.append(dict(
            side=tracker.position["side"], entry_ts=str(tracker.position["entry_ts"].date()),
            entry_price=tracker.position["entry_price"], exit_ts=str(last.timestamp.date()),
            exit_price=last.close, reason="still_open_at_backtest_end",
            pnl_pct=round(pnl_pct * 100, 2),
            hold_days=(last.timestamp.date() - tracker.position["entry_ts"].date()).days,
        ))

    wins = [t["pnl_pct"] for t in trades if t["pnl_pct"] > 0]
    losses = [t["pnl_pct"] for t in trades if t["pnl_pct"] <= 0]
    gl = abs(sum(losses))
    pf = (sum(wins) / gl) if gl > 0 else (float("inf") if wins else 0.0)
    return dict(symbol=symbol, n=len(trades), win_pct=round(100 * len(wins) / len(trades), 1) if trades else 0.0,
                avg_pnl_pct=round(sum(t["pnl_pct"] for t in trades) / len(trades), 2) if trades else 0.0,
                sum_pnl_pct=round(sum(t["pnl_pct"] for t in trades), 2),
                pf=("inf" if pf == float("inf") else round(pf, 2)),
                long_n=len([t for t in trades if t["side"] == "LONG"]),
                short_n=len([t for t in trades if t["side"] == "SHORT"]),
                trades=trades, zones_long=len(zones_long), zones_short=len(zones_short))


SWING_TF_SWEEP = (1, 2, 3, 5)   # trading days per S&R candle -- 1=daily, 5=~weekly.
                                 # 2026-08-09 per direct user request: FnO is a swing/
                                 # positional concept and should use a HIGHER S&R
                                 # timeframe than intraday's 1-10 MINUTE sweep, for
                                 # both the breakout entry and the TSL (both derive
                                 # from whatever bars are fed to PositionalSRTracker).


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=float, default=2.0)
    args = ap.parse_args()

    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    token = (creds or {}).get("access_token", "")
    if not token:
        print("FATAL: no Upstox access_token in data/clients.db.")
        return 1

    end = datetime.now(IST).date()
    start = end - timedelta(days=int(args.years * 365))
    print(f"FnO positional S&R swing-tf sweep: {start} .. {end}  ({len(STOCK_SUBSET)} stocks, "
          f"swing_days={SWING_TF_SWEEP})")
    print("LIMITATION: signal validated on real stock price, NOT real option premium -- see module docstring.\n")

    all_results = {}
    for swing_days in SWING_TF_SWEEP:
        print(f"\n--- swing_days={swing_days} ({'daily' if swing_days==1 else f'{swing_days}-day'} bars) ---")
        results = []
        for symbol in STOCK_SUBSET:
            r = await backtest_one(symbol, start, end, token, zone_days=swing_days, entry_days=swing_days)
            results.append(r)
            if r.get("error"):
                print(f"  {symbol}: SKIP -- {r['error']}")
                continue
            print(f"  {symbol}: n={r['n']} (L={r['long_n']}/S={r['short_n']}) win%={r['win_pct']} "
                  f"avg={r['avg_pnl_pct']:+.2f}% PF={r['pf']} zones(L={r['zones_long']}/S={r['zones_short']})")
        all_results[swing_days] = results

    print(f"\n{'='*70}\nSWING TIMEFRAME COMPARISON (aggregate across all stocks)")
    print(f"  {'SwingDays':<12}{'Trades':>8}{'Win%':>7}{'AvgPnL%':>10}{'PF':>8}{'MedHold':>9}")
    best = None
    for swing_days, results in all_results.items():
        valid = [r for r in results if not r.get("error")]
        all_trades = [t for r in valid for t in r["trades"]]
        if not all_trades:
            continue
        wins = [t["pnl_pct"] for t in all_trades if t["pnl_pct"] > 0]
        losses = [t["pnl_pct"] for t in all_trades if t["pnl_pct"] <= 0]
        gl = abs(sum(losses))
        pf = (sum(wins) / gl) if gl > 0 else (float("inf") if wins else 0.0)
        win_pct = 100 * len(wins) / len(all_trades)
        avg_pnl = sum(t["pnl_pct"] for t in all_trades) / len(all_trades)
        holds = sorted(t["hold_days"] for t in all_trades)
        med_hold = holds[len(holds) // 2]
        pf_str = "inf" if pf == float("inf") else f"{pf:.2f}"
        print(f"  {swing_days:<12}{len(all_trades):>8}{win_pct:>6.1f}%{avg_pnl:>+9.2f}%{pf_str:>8}{med_hold:>9}")
        if len(all_trades) >= 20 and (best is None or (pf if pf != float("inf") else 999) > best[0]):
            best = (pf if pf != float("inf") else 999, swing_days, len(all_trades), win_pct, avg_pnl)
    if best:
        print(f"\nBest swing_days (n>=20): {best[1]}  n={best[2]} win%={best[3]:.1f} avg_pnl%={best[4]:+.2f} "
              f"PF={'inf' if best[0]==999 else best[0]:.2f}")

    # 2026-08-09: decoupled zone/entry sweep -- entry_days FIXED at 1 (the swing_days
    # sweep above already proved daily entry wins decisively), only zone_days varies,
    # to test whether a MATURER/coarser zone-detection basis helps while still
    # entering daily. Per direct user request to check zone-tf and entry-tf separately.
    ZONE_TF_SWEEP = (1, 2, 3, 5, 10)
    print(f"\n{'='*70}\nDECOUPLED ZONE-TF SWEEP (entry_days fixed=1, {len(STOCK_SUBSET)} stocks)")
    decoupled_results = {}
    for zone_days in ZONE_TF_SWEEP:
        results = []
        for symbol in STOCK_SUBSET:
            r = await backtest_one(symbol, start, end, token, zone_days=zone_days, entry_days=1)
            results.append(r)
        decoupled_results[zone_days] = results

    print(f"  {'ZoneDays':<12}{'Trades':>8}{'Win%':>7}{'AvgPnL%':>10}{'PF':>8}{'MedHold':>9}")
    best_decoupled = None
    for zone_days, results in decoupled_results.items():
        valid = [r for r in results if not r.get("error")]
        all_trades = [t for r in valid for t in r["trades"]]
        if not all_trades:
            continue
        wins = [t["pnl_pct"] for t in all_trades if t["pnl_pct"] > 0]
        losses = [t["pnl_pct"] for t in all_trades if t["pnl_pct"] <= 0]
        gl = abs(sum(losses))
        pf = (sum(wins) / gl) if gl > 0 else (float("inf") if wins else 0.0)
        win_pct = 100 * len(wins) / len(all_trades)
        avg_pnl = sum(t["pnl_pct"] for t in all_trades) / len(all_trades)
        holds = sorted(t["hold_days"] for t in all_trades)
        med_hold = holds[len(holds) // 2]
        pf_str = "inf" if pf == float("inf") else f"{pf:.2f}"
        print(f"  {zone_days:<12}{len(all_trades):>8}{win_pct:>6.1f}%{avg_pnl:>+9.2f}%{pf_str:>8}{med_hold:>9}")
        if len(all_trades) >= 20 and (best_decoupled is None or (pf if pf != float("inf") else 999) > best_decoupled[0]):
            best_decoupled = (pf if pf != float("inf") else 999, zone_days, len(all_trades), win_pct, avg_pnl)
    if best_decoupled:
        print(f"\nBest zone_days @ entry_days=1 (n>=20): {best_decoupled[1]}  n={best_decoupled[2]} "
              f"win%={best_decoupled[3]:.1f} avg_pnl%={best_decoupled[4]:+.2f} "
              f"PF={'inf' if best_decoupled[0]==999 else best_decoupled[0]:.2f}")

    import json
    from pathlib import Path
    out_path = Path(__file__).resolve().parents[1] / "data" / "sweeps" / "fno_positional_sr_swingtf_sweep.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(all_results, indent=2, default=str), encoding="utf-8")
    print(f"\n-> {out_path}")

    out_path2 = Path(__file__).resolve().parents[1] / "data" / "sweeps" / "fno_positional_sr_zonetf_decoupled_sweep.json"
    out_path2.write_text(json.dumps(decoupled_results, indent=2, default=str), encoding="utf-8")
    print(f"-> {out_path2}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
