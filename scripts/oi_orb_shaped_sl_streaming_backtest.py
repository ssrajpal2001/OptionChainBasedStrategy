"""
scripts/oi_orb_shaped_sl_streaming_backtest.py -- 2026-09-09, direct user
spec, final combined validation pass before live implementation:

  1. ENTRY: screener.RollingVwapRetestTracker (arm-then-retest, bounded to
     a rolling last-15x1min-candle window) -- already validated separately
     in oi_orb_rolling_arm_retest_backtest.py.
  2. ENTRY TIMING, streaming (direct user spec -- R&D phase, "we are still
     finding out what is the best time to take trade... as soon as a stock
     enters the top 20 scanner we start to check for entry trigger in that
     stock"): for TODAY's real stocks, each symbol's own tracker starts
     checking from the REAL wall-clock moment it first entered the top-20
     ranking (data/oi_orb_screener.db's oi_spurt_history table, 1-min
     cadence), not a fixed 09:25 anchor. The existing 46-row historical
     dataset (2026-08-31..09-04, predates oi_spurt_history's 2026-09-07
     build) has no such per-minute record available, so those rows keep
     their original ORB_END (09:25) start as the best available
     approximation.
  3. SL: 30-min (now swept: 15/20/30/45/60) Heikin-Ashi candle closing on
     the wrong side of VWAP, PLUS a NEW shape gate (direct user spec, swept
     tolerance 0/2/5/10/20%/disabled): the candle must also be "clean" in
     the direction of the adverse move -- PUT's SL candle (adverse = close
     ABOVE vwap, a bullish move) needs low==open (no lower wick); CALL's SL
     candle (adverse = close BELOW vwap, a bearish move) needs high==open
     (no upper wick). Exact equality never fires on real noisy data, hence
     the tolerance sweep (wick <= tol_pct * candle_range).
  4. TARGET: unchanged, the already-validated 75min/3min same-side trap
     (trap_target_exit / trap_target_exit_diag_multiday), reused verbatim.
  5. RE-ENTRY: unchanged, 1x after an SL stop-out only.

Reports stock/spot POINTS throughout (same methodology as every backtest
this session -- this strategy's option-side execution has never itself
been backtested against real option premium history; see this session's
own repeated documentation of that limitation).

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_shaped_sl_streaming_backtest.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, ".")

from scripts.oi_orb_entry_mode_backtest import (
    ROWS as OLD_ROWS, SIDE, ORB_START, ORB_END, ENTRY_WINDOW_END,
    resolve_eq_key, to_bars, volume_by_ts, compute_orb,
)
from scripts.oi_orb_full_live_logic_backtest import (
    _vwap_series_full_day, simulate_target_exit, find_reentry, Leg, Trade,
    LOOKBACK_CALENDAR_DAYS, TRAP_HTF_MULTIDAY_MIN, to_heikin_ashi, to_n_min_bars,
)
from scripts.oi_orb_same_side_trap_multiday_htf_sweep import _to_n_min_bars_dateaware
from strategies.oi_orb_screener import screener
from data_layer.historical_candles import fetch_upstox_range_1m, fetch_upstox_intraday_1m

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
IST_TODAY = "2026-09-09"

# (poll_ts, symbol, oi_spurt_pct, price_change_pct) -- real first-seen rows
# from oi_spurt_history for 2026-09-09, pulled directly by the user.
TODAY_FIRST_SEEN = [
    ("2026-09-09T09:17:51+05:30", "COFORGE", -5.79),
    ("2026-09-09T09:17:51+05:30", "HCLTECH", -2.61),
    ("2026-09-09T09:17:51+05:30", "INFY", -2.78),
    ("2026-09-09T09:17:51+05:30", "MUTHOOTFIN", -2.35),
    ("2026-09-09T09:22:41+05:30", "BSE", -2.71),
    ("2026-09-09T09:22:41+05:30", "MAXHEALTH", 2.86),
    ("2026-09-09T09:22:41+05:30", "TECHM", -3.0),
    ("2026-09-09T09:24:44+05:30", "TCS", -2.16),
    ("2026-09-09T09:48:10+05:30", "HDFCLIFE", -2.3),
    ("2026-09-09T10:39:24+05:30", "ADANIPORTS", 2.43),
    ("2026-09-09T11:09:07+05:30", "HYUNDAI", -2.0),
    ("2026-09-09T12:25:33+05:30", "LTM", -2.45),
    ("2026-09-09T14:49:25+05:30", "DLF", -2.65),
    ("2026-09-09T14:57:28+05:30", "TATAELXSI", -2.89),
]
TODAY_ROWS = [
    (IST_TODAY, sym, "bullish" if pchg > 0 else "bearish", datetime.fromisoformat(ts))
    for ts, sym, pchg in TODAY_FIRST_SEEN
]

TF_CANDIDATES = [15, 20, 30, 45, 60]
TOL_CANDIDATES = [0.0, 0.02, 0.05, 0.10, 0.20, 1.0]   # 1.0 = shape gate disabled (baseline)


def shape_ok(hb, side: str, tol_pct: float) -> bool:
    rng = hb.high - hb.low
    if tol_pct >= 1.0:
        return True
    if rng <= 0:
        return False
    wick = (hb.high - hb.open) if side == "CALL" else (hb.open - hb.low)
    return wick <= tol_pct * rng


def vwap_close_sl_exit_shaped(bars_1m, vwap_by_ts, side, entry_ts, tf_min, tol_pct):
    ha_1m = to_heikin_ashi(bars_1m)
    tf_bars = to_n_min_bars(ha_1m, tf_min)
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]
    if not post_entry:
        return None
    last_ts = post_entry[-1].ts
    for hb in tf_bars:
        if hb.ts < entry_ts:
            continue
        if last_ts < hb.ts + timedelta(minutes=tf_min):
            break
        vwap_at_close = vwap_by_ts.get(hb.ts)
        if vwap_at_close is None:
            continue
        adverse = (hb.close < vwap_at_close) if side == "CALL" else (hb.close > vwap_at_close)
        if not adverse or not shape_ok(hb, side, tol_pct):
            continue
        candidates = [b for b in post_entry if b.ts >= hb.ts]
        if candidates:
            return candidates[0].ts, candidates[0].close
    return None


def resolve_exit_shaped(entry_ts, entry_price, side, bars_1m, vwap_by_ts, htf_multiday_bars, tf_min, tol_pct):
    sl = vwap_close_sl_exit_shaped(bars_1m, vwap_by_ts, side, entry_ts, tf_min, tol_pct)
    t_ts, t_px, t_reason = simulate_target_exit(entry_ts, entry_price, side, bars_1m, htf_multiday_bars)
    if sl is not None and sl[0] <= t_ts:
        return sl[0], sl[1], "vwap_close_sl"
    return t_ts, t_px, t_reason


def find_first_entry_rolling_from(bars_1m, side, vol_by_ts, vwap_state, start_ts, window_min=15.0):
    tracker = screener.RollingVwapRetestTracker(window_min=window_min)
    for b in bars_1m:
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        if b.ts < start_ts:
            continue
        vwap = vwap_state.current("SYM")
        if vwap is None:
            continue
        if tracker.check(side, b.ts, b.close, vwap):
            return b.ts, b.close
    return None


async def fetch_all():
    print("Fetching multi-day + today history (real Upstox 1-min NSE_EQ)...")
    cache = {}
    all_rows = [(d, s, sb, None) for d, s, sb in OLD_ROWS] + TODAY_ROWS
    for row in all_rows:
        trade_date, symbol = row[0], row[1]
        key = (trade_date, symbol)
        if key in cache:
            continue
        eq_key = resolve_eq_key(symbol)
        if eq_key is None:
            cache[key] = None
            continue
        d = date.fromisoformat(trade_date)
        start = d - timedelta(days=LOOKBACK_CALENDAR_DAYS)
        if trade_date == IST_TODAY:
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
        orb = compute_orb(today_bars)
        if orb is None:
            cache[key] = None
            continue
        htf_multiday = _to_n_min_bars_dateaware(all_bars, TRAP_HTF_MULTIDAY_MIN)
        cache[key] = (today_bars, vol_by_ts, htf_multiday)
    n_ok = sum(1 for v in cache.values() if v)
    print(f"Fetched {n_ok}/{len(cache)} usable rows.")
    return cache


def run_combo(cache, tf_min: int, tol_pct: float):
    trades = []
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
        _run_trade(trades, trade_date, symbol, side, entry, bars_1m, vwap_by_ts, htf_multiday,
                   tf_min, tol_pct, vwap_state)

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
        _run_trade(trades, trade_date, symbol, side, entry, bars_1m, vwap_by_ts, htf_multiday,
                   tf_min, tol_pct, vwap_state)

    wins = [t for t in trades if t.points > 0]
    losses = [t for t in trades if t.points <= 0]
    total = sum(t.points for t in trades)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(trades) * 100) if trades else 0.0
    max_loss = min((t.points for t in trades), default=0.0)
    sl_hits = sum(1 for t in trades for leg in t.legs if leg.reason == "vwap_close_sl")
    return {"tf_min": tf_min, "tol_pct": tol_pct, "entered": len(trades), "win_pct": win_pct,
            "pf": (pf if pf != float("inf") else 9999.0), "total": total,
            "sl_hits": sl_hits, "max_loss": max_loss}, trades


def _run_trade(trades, trade_date, symbol, side, entry, bars_1m, vwap_by_ts, htf_multiday,
               tf_min, tol_pct, vwap_state):
    """vwap_state is the SAME object find_first_entry_rolling_from() was
    already incrementally feeding -- it stops advancing the instant entry
    fires, so it's left holding whatever VWAP was current AT ENTRY. Passed
    straight through to find_reentry() unchanged, matching the existing,
    already-validated oi_orb_full_live_logic_backtest.py re-entry
    methodology exactly (that script's own find_reentry never re-advances
    vwap_state either -- VWAP is intentionally frozen at its entry-time
    value for the rest of that day's re-entry scan, a pre-existing
    simplification baked into the validated 90%+ results, not something
    introduced here)."""
    entry_ts, entry_price = entry
    legs = []
    sl_reentry_used = False
    cur_entry_ts, cur_entry_price = entry_ts, entry_price
    while True:
        exit_ts, exit_price, reason = resolve_exit_shaped(
            cur_entry_ts, cur_entry_price, side, bars_1m, vwap_by_ts, htf_multiday, tf_min, tol_pct)
        legs.append(Leg(cur_entry_ts, cur_entry_price, exit_ts, exit_price, reason))
        if reason != "vwap_close_sl" or sl_reentry_used:
            break
        sl_reentry_used = True
        nxt = find_reentry(bars_1m, side, exit_ts, vwap_state)
        if nxt is None:
            break
        cur_entry_ts, cur_entry_price = nxt
    trades.append(Trade(trade_date, symbol, side, legs))


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    cache = await fetch_all()

    results = []
    for tf_min in TF_CANDIDATES:
        for tol_pct in TOL_CANDIDATES:
            r, _ = run_combo(cache, tf_min, tol_pct)
            results.append(r)
            print(f"tf={tf_min:>3}min  tol={tol_pct:>4.0%}  entered={r['entered']:>3}  "
                  f"win%={r['win_pct']:>5.1f}  PF={r['pf']:>7.2f}  total={r['total']:>+9.2f}  "
                  f"sl_hits={r['sl_hits']:>3}  max_loss={r['max_loss']:>+8.2f}")

    results.sort(key=lambda r: r["pf"], reverse=True)
    print(f"\n{'='*100}\nTOP 10 BY PF\n{'='*100}")
    for r in results[:10]:
        print(f"tf={r['tf_min']:>3}min  tol={r['tol_pct']:>4.0%}  entered={r['entered']:>3}  "
              f"win%={r['win_pct']:>5.1f}  PF={r['pf']:>7.2f}  total={r['total']:>+9.2f}")

    with open("data/oi_orb_shaped_sl_streaming_sweep_report.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\nWrote data/oi_orb_shaped_sl_streaming_sweep_report.json")


if __name__ == "__main__":
    asyncio.run(main())
