"""
scripts/oi_orb_ride_trend_comparison.py -- 2026-09-09, direct user follow-up
on the shaped-SL sweep: "check if there was another target or SL concept we
would have ride the trend... if sl was something else would we have ride
the trend" + "did you take a 2nd trade in same stock when SL is hit in the
backtest or not."

Reuses the exact same real 44-row dataset (46-row historical + 14 real
today-streamed rows, filtered by fetch success) and the winning config from
the sweep (TF=20min, shape gate OFF -- win%=90.9, PF=422.66) from
oi_orb_shaped_sl_streaming_backtest.py, and adds ONE new comparison per
trade: what if there were NO SL at all (target + EOD only) -- i.e. the
position just rides until the 75min/3min trap target fires or EOD, with
zero early stop-out. Reports both side by side per trade, plus totals, plus
an explicit re-entry-after-SL column (legs>1) to answer the second question
directly.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_ride_trend_comparison.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, ".")

from scripts.oi_orb_entry_mode_backtest import (
    ROWS as OLD_ROWS, SIDE, ORB_END, resolve_eq_key, to_bars, volume_by_ts, compute_orb,
)
from scripts.oi_orb_full_live_logic_backtest import (
    _vwap_series_full_day, simulate_target_exit, find_reentry, Leg, Trade,
    LOOKBACK_CALENDAR_DAYS, TRAP_HTF_MULTIDAY_MIN,
)
from scripts.oi_orb_same_side_trap_multiday_htf_sweep import _to_n_min_bars_dateaware
from scripts.oi_orb_shaped_sl_streaming_backtest import (
    TODAY_ROWS, resolve_exit_shaped, find_first_entry_rolling_from,
)
from strategies.oi_orb_screener import screener
from data_layer.historical_candles import fetch_upstox_range_1m, fetch_upstox_intraday_1m

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
WIN_TF, WIN_TOL = 20, 1.0   # sweep winner: 20min, shape gate disabled
TODAY_STR = "2026-09-09"


async def fetch_all():
    cache = {}
    all_keys = [(d, s) for d, s, sb in OLD_ROWS] + [(d, s) for d, s, sb, ts in TODAY_ROWS]
    for trade_date, symbol in all_keys:
        key = (trade_date, symbol)
        if key in cache:
            continue
        eq_key = resolve_eq_key(symbol)
        if eq_key is None:
            cache[key] = None
            continue
        d = date.fromisoformat(trade_date)
        start = d - timedelta(days=LOOKBACK_CALENDAR_DAYS)
        if trade_date == TODAY_STR:
            # fetch_upstox_range_1m's dated-historical endpoint does not
            # reliably serve TODAY's still-forming session -- fetch prior
            # days via range, today via the dedicated intraday endpoint,
            # merge (same pattern engine.py's own _seed_trap_exit_state uses).
            prior_rows = await fetch_upstox_range_1m(eq_key, TOKEN, start, d - timedelta(days=1))
            today_rows = await fetch_upstox_intraday_1m(eq_key, TOKEN)
            rows = sorted(prior_rows + today_rows, key=lambda r: r["ts"])
        else:
            rows = await fetch_upstox_range_1m(eq_key, TOKEN, start, d)
        if not rows:
            cache[key] = None
            continue
        all_bars = to_bars(rows)
        today_bars = [b for b in all_bars if b.ts.date() == d]
        if not today_bars:
            cache[key] = None
            continue
        vol_by_ts = volume_by_ts([r for r in rows if r["ts"].startswith(trade_date)])
        if compute_orb(today_bars) is None:
            cache[key] = None
            continue
        htf_multiday = _to_n_min_bars_dateaware(all_bars, TRAP_HTF_MULTIDAY_MIN)
        cache[key] = (today_bars, vol_by_ts, htf_multiday)
    return cache


def build_trade(trade_date, symbol, side, entry, bars_1m, vwap_by_ts, htf_multiday, vwap_state):
    """Actual (SL+target, with 1x re-entry-after-SL) exactly like the
    winning sweep config."""
    entry_ts, entry_price = entry
    legs = []
    sl_reentry_used = False
    cur_entry_ts, cur_entry_price = entry_ts, entry_price
    while True:
        exit_ts, exit_price, reason = resolve_exit_shaped(
            cur_entry_ts, cur_entry_price, side, bars_1m, vwap_by_ts, htf_multiday, WIN_TF, WIN_TOL)
        legs.append(Leg(cur_entry_ts, cur_entry_price, exit_ts, exit_price, reason))
        if reason != "vwap_close_sl" or sl_reentry_used:
            break
        sl_reentry_used = True
        nxt = find_reentry(bars_1m, side, exit_ts, vwap_state)
        if nxt is None:
            break
        cur_entry_ts, cur_entry_price = nxt
    return Trade(trade_date, symbol, side, legs)


def build_ride_trend_trade(trade_date, symbol, side, entry, bars_1m, htf_multiday):
    """NO SL at all -- ride purely to the 75min/3min trap target (or EOD if
    the target never fires), single leg, no re-entry (nothing to re-enter
    from since there's no SL stop-out)."""
    entry_ts, entry_price = entry
    exit_ts, exit_price, reason = simulate_target_exit(entry_ts, entry_price, side, bars_1m, htf_multiday)
    return Trade(trade_date, symbol, side, [Leg(entry_ts, entry_price, exit_ts, exit_price, reason)])


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching real Upstox 1-min history...")
    cache = await fetch_all()
    n_ok = sum(1 for v in cache.values() if v)
    print(f"Fetched {n_ok}/{len(cache)} usable rows.")

    pairs = []
    for trade_date, symbol, side_bias in OLD_ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, htf_multiday = cached
        vwap_state = screener.VwapState()
        vwap_by_ts = _vwap_series_full_day(bars_1m, vol_by_ts)
        orb_end_bars = [b for b in bars_1m if b.ts.strftime("%H:%M") >= ORB_END]
        if not orb_end_bars:
            continue
        entry = find_first_entry_rolling_from(bars_1m, side, vol_by_ts, vwap_state, orb_end_bars[0].ts)
        if entry is None:
            continue
        actual = build_trade(trade_date, symbol, side, entry, bars_1m, vwap_by_ts, htf_multiday, vwap_state)
        ride = build_ride_trend_trade(trade_date, symbol, side, entry, bars_1m, htf_multiday)
        pairs.append((actual, ride))

    for trade_date, symbol, side_bias, start_ts in TODAY_ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, htf_multiday = cached
        vwap_state = screener.VwapState()
        vwap_by_ts = _vwap_series_full_day(bars_1m, vol_by_ts)
        entry = find_first_entry_rolling_from(bars_1m, side, vol_by_ts, vwap_state, start_ts)
        if entry is None:
            continue
        actual = build_trade(trade_date, symbol, side, entry, bars_1m, vwap_by_ts, htf_multiday, vwap_state)
        ride = build_ride_trend_trade(trade_date, symbol, side, entry, bars_1m, htf_multiday)
        pairs.append((actual, ride))

    def summarize(trades):
        wins = [t for t in trades if t.points > 0]
        losses = [t for t in trades if t.points <= 0]
        total = sum(t.points for t in trades)
        loss_sum = sum(t.points for t in losses)
        pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
        win_pct = (len(wins) / len(trades) * 100) if trades else 0.0
        max_loss = min((t.points for t in trades), default=0.0)
        return {"entered": len(trades), "win_pct": win_pct,
                "pf": (pf if pf != float("inf") else 9999.0), "total": total, "max_loss": max_loss}

    actual_trades = [a for a, r in pairs]
    ride_trades = [r for a, r in pairs]
    actual_summary = summarize(actual_trades)
    ride_summary = summarize(ride_trades)
    reentry_count = sum(1 for t in actual_trades if len(t.legs) > 1)

    out_rows = []
    for a, r in pairs:
        out_rows.append({
            "date": a.date, "symbol": a.symbol, "side": a.side,
            "actual_points": round(a.points, 2), "actual_legs": len(a.legs),
            "actual_reentry": len(a.legs) > 1,
            "actual_detail": [
                {"entry_ts": lg.entry_ts.strftime("%H:%M"), "entry_price": lg.entry_price,
                 "exit_ts": lg.exit_ts.strftime("%H:%M"), "exit_price": lg.exit_price, "reason": lg.reason}
                for lg in a.legs
            ],
            "ride_points": round(r.points, 2),
            "ride_detail": {
                "entry_ts": r.legs[0].entry_ts.strftime("%H:%M"), "entry_price": r.legs[0].entry_price,
                "exit_ts": r.legs[0].exit_ts.strftime("%H:%M"), "exit_price": r.legs[0].exit_price,
                "reason": r.legs[0].reason,
            },
            "delta": round(r.points - a.points, 2),
        })

    report = {
        "config": {"tf_min": WIN_TF, "wick_tol": WIN_TOL},
        "actual_summary": actual_summary,
        "ride_summary": ride_summary,
        "reentry_count": reentry_count,
        "rows": out_rows,
    }
    with open("data/oi_orb_ride_trend_comparison_report.json", "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nACTUAL (SL+target, 1x reentry-after-SL): entered={actual_summary['entered']} "
          f"win%={actual_summary['win_pct']:.1f} PF={actual_summary['pf']:.2f} total={actual_summary['total']:+.2f} "
          f"max_loss={actual_summary['max_loss']:+.2f}  reentry_trades={reentry_count}")
    print(f"RIDE-THE-TREND (no SL, target+EOD only): entered={ride_summary['entered']} "
          f"win%={ride_summary['win_pct']:.1f} PF={ride_summary['pf']:.2f} total={ride_summary['total']:+.2f} "
          f"max_loss={ride_summary['max_loss']:+.2f}")
    print("\nWrote data/oi_orb_ride_trend_comparison_report.json")


if __name__ == "__main__":
    asyncio.run(main())
