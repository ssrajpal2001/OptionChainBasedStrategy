"""
scripts/oi_orb_option_pnl_backtest.py -- 2026-09-09, direct user follow-up:
"can u also check what would have happeend if we would have taken trade in
option with capital invested and profit loss % with drawdown... as end of
the day we will be takign tarde in option chart not in spot... trade will
run in option."

Takes the min_gap_0.2% SL config (the winner of the SL-concept comparison:
same win% as today's baseline, higher PF, +430 more spot points, same max
loss) and, for every real trade it produced, resolves the REAL option
contract (current active expiry via InstrumentRegistry.get_active_expiry_
strict -- never guesses a rolled-off contract) and fetches REAL historical
1-min option PREMIUM (not spot) for that exact strike/expiry/day, using the
SAME entry/exit TIMESTAMPS the spot-driven signal already produced (spot
triggers the signal, the option is what's actually bought -- matches the
live engine's own design exactly, see engine.py's own module docstring).

Reports, per trade and in aggregate: capital invested (entry premium x lot
size), P&L in Rs and %, and a chronological equity curve with max drawdown.
Any leg whose option contract or historical premium can't be resolved is
excluded and reported as coverage, never guessed.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_option_pnl_backtest.py
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
    _vwap_series_full_day, LOOKBACK_CALENDAR_DAYS, TRAP_HTF_MULTIDAY_MIN,
)
from scripts.oi_orb_same_side_trap_multiday_htf_sweep import _to_n_min_bars_dateaware
from scripts.oi_orb_shaped_sl_streaming_backtest import TODAY_ROWS, find_first_entry_rolling_from
from scripts.oi_orb_sl_concept_comparison import (
    sl_min_gap, resolve_exit_family, find_reentry, Leg, Trade,
)
from strategies.oi_orb_screener import screener, stock_resolve
from data_layer.instrument_registry import REGISTRY
from data_layer.historical_candles import fetch_upstox_range_1m, fetch_upstox_intraday_1m

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
TODAY_STR = "2026-09-09"
SL_FN = lambda bars, vwap_by_ts, side, ets, epx: sl_min_gap(bars, vwap_by_ts, side, ets, 0.002)


async def fetch_all_spot():
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
    n_ok = sum(1 for v in cache.values() if v)
    print(f"Fetched {n_ok}/{len(cache)} usable spot rows.")
    return cache


def build_min_gap_trades(cache):
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
        _run_trade(trades, trade_date, symbol, side, entry, bars_1m, vwap_by_ts, htf_multiday, vwap_state)
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
        _run_trade(trades, trade_date, symbol, side, entry, bars_1m, vwap_by_ts, htf_multiday, vwap_state)
    return trades


def _run_trade(trades, trade_date, symbol, side, entry, bars_1m, vwap_by_ts, htf_multiday, vwap_state):
    entry_ts, entry_price = entry
    legs = []
    sl_reentry_used = False
    cur_entry_ts, cur_entry_price = entry_ts, entry_price
    while True:
        exit_ts, exit_price, reason = resolve_exit_family(
            SL_FN, cur_entry_ts, cur_entry_price, side, bars_1m, vwap_by_ts, htf_multiday)
        legs.append(Leg(cur_entry_ts, cur_entry_price, exit_ts, exit_price, reason))
        if reason != "sl" or sl_reentry_used:
            break
        sl_reentry_used = True
        nxt = find_reentry(bars_1m, side, exit_ts, vwap_state)
        if nxt is None:
            break
        cur_entry_ts, cur_entry_price = nxt
    trades.append(Trade(trade_date, symbol, side, legs))


_option_cache = {}   # (upstox_key, trade_date) -> sorted [(ts_str, close_premium), ...]


async def fetch_option_series(upstox_key, trade_date):
    key = (upstox_key, trade_date)
    if key in _option_cache:
        return _option_cache[key]
    d = date.fromisoformat(trade_date)
    if trade_date == TODAY_STR:
        rows = await fetch_upstox_intraday_1m(upstox_key, TOKEN)
    else:
        rows = await fetch_upstox_range_1m(upstox_key, TOKEN, d, d)
    series = sorted([(r["ts"][:16], float(r["close"])) for r in rows]) if rows else []
    _option_cache[key] = series
    return series


def premium_at_or_after(series, hhmm):
    for ts, px in series:
        if ts[11:16] >= hhmm:
            return px
    return None


async def resolve_and_price_leg(symbol, side, trade_date, lg):
    option_type = "CE" if side == "CALL" else "PE"
    if not REGISTRY.is_loaded(symbol):
        await asyncio.to_thread(REGISTRY.load_sync, symbol)
    d = date.fromisoformat(trade_date)
    expiry = REGISTRY.get_active_expiry_strict(symbol, from_date=d)
    if expiry is None:
        return None
    available = REGISTRY.get_available_strikes(symbol, expiry, option_type)
    if not available:
        return None
    strike = min(available, key=lambda s: abs(s - lg.entry_price))
    upstox_key = REGISTRY.get_upstox_key(symbol, expiry, strike, option_type)
    if not upstox_key:
        return None
    lot = await stock_resolve.resolve_lot_async(symbol)
    if lot <= 0:
        return None
    series = await fetch_option_series(upstox_key, trade_date)
    if not series:
        return None
    entry_hhmm = lg.entry_ts.strftime("%H:%M")
    exit_hhmm = lg.exit_ts.strftime("%H:%M")
    entry_prem = premium_at_or_after(series, entry_hhmm)
    exit_prem = premium_at_or_after(series, exit_hhmm)
    if entry_prem is None or exit_prem is None or entry_prem <= 0:
        return None
    capital = entry_prem * lot
    pnl = (exit_prem - entry_prem) * lot
    return {
        "expiry": expiry.isoformat(), "strike": strike, "option_type": option_type, "lot": lot,
        "entry_premium": round(entry_prem, 2), "exit_premium": round(exit_prem, 2),
        "capital": round(capital, 2), "pnl": round(pnl, 2), "pnl_pct": round(pnl / capital * 100, 2),
    }


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching real spot history...")
    cache = await fetch_all_spot()
    trades = build_min_gap_trades(cache)
    print(f"Built {len(trades)} spot trades (min_gap_0.2% SL config). Resolving real option premiums...")

    priced_rows = []
    unresolved = 0
    for t in trades:
        leg_prices = []
        for lg in t.legs:
            try:
                p = await resolve_and_price_leg(t.symbol, t.side, t.date, lg)
            except Exception as exc:
                print(f"  ! {t.symbol} {t.date} leg error: {exc!r}")
                p = None
            if p is None:
                unresolved += 1
            leg_prices.append(p)
        priced_rows.append({"date": t.date, "symbol": t.symbol, "side": t.side,
                             "spot_points": round(t.points, 2), "legs": leg_prices,
                             "leg_spot": [{"entry_ts": lg.entry_ts.strftime("%H:%M"),
                                           "entry_price": lg.entry_price,
                                           "exit_ts": lg.exit_ts.strftime("%H:%M"),
                                           "exit_price": lg.exit_price, "reason": lg.reason}
                                          for lg in t.legs]})

    total_legs = sum(len(r["legs"]) for r in priced_rows)
    print(f"\nResolved {total_legs - unresolved}/{total_legs} legs to real option premium.")

    # chronological equity curve across resolved legs only
    flat = []
    for r in priced_rows:
        for lg_px, lg_spot in zip(r["legs"], r["leg_spot"]):
            if lg_px is None:
                continue
            flat.append({"date": r["date"], "symbol": r["symbol"], "side": r["side"],
                         "exit_ts": lg_spot["exit_ts"], **lg_px})
    flat.sort(key=lambda x: (x["date"], x["exit_ts"]))

    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    total_capital = 0.0
    total_pnl = 0.0
    for f in flat:
        equity += f["pnl"]
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
        total_capital += f["capital"]
        total_pnl += f["pnl"]
        f["cum_pnl"] = round(equity, 2)

    wins = [f for f in flat if f["pnl"] > 0]
    win_pct = (len(wins) / len(flat) * 100) if flat else 0.0
    loss_sum = sum(f["pnl"] for f in flat if f["pnl"] <= 0)
    win_sum = sum(f["pnl"] for f in wins)
    pf = (win_sum / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)

    report = {
        "coverage": {"legs_total": total_legs, "legs_resolved": total_legs - unresolved},
        "summary": {
            "legs": len(flat), "win_pct": win_pct, "pf": (pf if pf != float("inf") else 9999.0),
            "total_capital": round(total_capital, 2), "total_pnl": round(total_pnl, 2),
            "total_pnl_pct": round(total_pnl / total_capital * 100, 2) if total_capital else 0.0,
            "max_drawdown": round(max_dd, 2),
        },
        "equity_curve": flat,
        "rows": priced_rows,
    }
    with open("data/oi_orb_option_pnl_backtest_report.json", "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report["summary"], indent=2))
    print("\nWrote data/oi_orb_option_pnl_backtest_report.json")


if __name__ == "__main__":
    asyncio.run(main())
