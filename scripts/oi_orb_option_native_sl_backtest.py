"""
scripts/oi_orb_option_native_sl_backtest.py -- 2026-09-09, direct user
follow-up: "shall v check for stoploss in option chart... worth checking if
it increases the profitability... in option we are buying option so we
need to check on one side only be it be ce or pe trade."

Every SL tested so far in this session was a SPOT-side proxy (VWAP-close,
ATR, flat %, LTF trap/S&R -- all computed off the underlying stock, never
the option itself). This script replaces the SL with a genuine OPTION-
PREMIUM stop: a flat percentage drop from the leg's own real entry premium,
checked directly against real historical 1-min option data -- not a spot
proxy at all. Since this strategy always BUYS (CE for CALL-bias, PE for
PUT-bias, never shorts), P&L is symmetric for both sides:
(exit_premium - entry_premium) x lot -- no sign-flip needed, unlike the
spot backtest's own CALL/PUT point convention.

Entry stays spot-driven (RollingVwapRetestTracker, unchanged -- the
strategy still WATCHES the stock to decide WHEN to buy). Target stays the
spot-side 75min/3min same-side trap (unchanged) -- only the STOP is moved
onto the option's own real premium chart. Whichever fires first (option SL
cross, or spot target-zone lock) decides the exit, priced in each case
against the real option premium at that instant. Re-entry (1x after an
SL-option stop-out) reuses the existing spot-side find_reentry, then
resolves a FRESH option contract for the new leg (strike may differ).

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_option_native_sl_backtest.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from collections import defaultdict
from datetime import date, timedelta

sys.path.insert(0, ".")

from scripts.oi_orb_entry_mode_backtest import (
    ROWS as OLD_ROWS, SIDE, ORB_END, resolve_eq_key, to_bars, volume_by_ts, compute_orb,
)
from scripts.oi_orb_full_live_logic_backtest import (
    _vwap_series_full_day, simulate_target_exit, find_reentry, LOOKBACK_CALENDAR_DAYS, TRAP_HTF_MULTIDAY_MIN,
)
from scripts.oi_orb_same_side_trap_multiday_htf_sweep import _to_n_min_bars_dateaware
from scripts.oi_orb_shaped_sl_streaming_backtest import TODAY_ROWS, find_first_entry_rolling_from
from strategies.oi_orb_screener import screener, stock_resolve
from data_layer.instrument_registry import REGISTRY
from data_layer.historical_candles import fetch_upstox_range_1m, fetch_upstox_intraday_1m

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
TODAY_STR = "2026-09-09"
SL_PCT_CANDIDATES = [0.10, 0.15, 0.20, 0.25, 0.30]

_option_cache = {}


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


async def resolve_contract_and_series(symbol, side, trade_date, entry_spot_price):
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
    strike = min(available, key=lambda s: abs(s - entry_spot_price))
    upstox_key = REGISTRY.get_upstox_key(symbol, expiry, strike, option_type)
    if not upstox_key:
        return None
    lot = await stock_resolve.resolve_lot_async(symbol)
    if lot <= 0:
        return None
    cache_key = (upstox_key, trade_date)
    if cache_key not in _option_cache:
        if trade_date == TODAY_STR:
            rows = await fetch_upstox_intraday_1m(upstox_key, TOKEN)
        else:
            rows = await fetch_upstox_range_1m(upstox_key, TOKEN, d, d)
        series = sorted([(r["ts"][11:16], float(r["close"])) for r in rows]) if rows else []
        _option_cache[cache_key] = series
    series = _option_cache[cache_key]
    if not series:
        return None
    return {"expiry": expiry.isoformat(), "strike": strike, "option_type": option_type,
            "lot": lot, "series": series}


def premium_at_or_after(series, hhmm):
    for ts, px in series:
        if ts >= hhmm:
            return ts, px
    return None, None


def option_native_sl_exit(series, entry_hhmm, entry_premium, pct):
    threshold = entry_premium * (1 - pct)
    for ts, px in series:
        if ts < entry_hhmm:
            continue
        if px <= threshold:
            return ts, px
    return None


async def run_trade_option_native(trade_date, symbol, side, bars_1m, vol_by_ts, htf_multiday,
                                   vwap_state, start_ts, sl_pct):
    entry = find_first_entry_rolling_from(bars_1m, side, vol_by_ts, vwap_state, start_ts)
    if entry is None:
        return None
    legs = []
    sl_reentry_used = False
    cur_entry_ts, cur_entry_spot = entry
    while True:
        contract = await resolve_contract_and_series(symbol, side, trade_date, cur_entry_spot)
        if contract is None:
            return legs if legs else None
        series = contract["series"]
        entry_hhmm = cur_entry_ts.strftime("%H:%M")
        e_ts, entry_premium = premium_at_or_after(series, entry_hhmm)
        if entry_premium is None or entry_premium <= 0:
            return legs if legs else None

        t_ts, t_spot_px, t_reason = simulate_target_exit(cur_entry_ts, cur_entry_spot, side, bars_1m, htf_multiday)
        t_hhmm = t_ts.strftime("%H:%M")

        sl_hit = option_native_sl_exit(series, entry_hhmm, entry_premium, sl_pct)

        if sl_hit is not None and sl_hit[0] <= t_hhmm:
            exit_ts_str, exit_premium, reason = sl_hit[0], sl_hit[1], "sl_option"
        else:
            _, exit_premium = premium_at_or_after(series, t_hhmm)
            if exit_premium is None:
                exit_premium = series[-1][1]
                exit_ts_str = series[-1][0]
            else:
                exit_ts_str = t_hhmm
            reason = t_reason

        capital = entry_premium * contract["lot"]
        pnl = (exit_premium - entry_premium) * contract["lot"]
        legs.append({
            "date": trade_date, "symbol": symbol, "side": side,
            "expiry": contract["expiry"], "strike": contract["strike"], "option_type": contract["option_type"],
            "lot": contract["lot"], "entry_ts": entry_hhmm, "entry_premium": round(entry_premium, 2),
            "exit_ts": exit_ts_str, "exit_premium": round(exit_premium, 2), "reason": reason,
            "capital": round(capital, 2), "pnl": round(pnl, 2),
            "pnl_pct": round(pnl / capital * 100, 2) if capital else 0.0,
        })

        if reason != "sl_option" or sl_reentry_used:
            break
        sl_reentry_used = True
        nxt = find_reentry(bars_1m, side, cur_entry_ts.replace(hour=int(exit_ts_str[:2]), minute=int(exit_ts_str[3:5])),
                            vwap_state)
        if nxt is None:
            break
        cur_entry_ts, cur_entry_spot = nxt
    return legs


async def run_sweep_value(cache, sl_pct):
    all_legs = []
    for trade_date, symbol, side_bias in OLD_ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, htf_multiday = cached
        vwap_state = screener.VwapState()
        orb_end_bars = [b for b in bars_1m if b.ts.strftime("%H:%M") >= ORB_END]
        if not orb_end_bars:
            continue
        legs = await run_trade_option_native(trade_date, symbol, side, bars_1m, vol_by_ts, htf_multiday,
                                              vwap_state, orb_end_bars[0].ts, sl_pct)
        if legs:
            all_legs.extend(legs)
    for trade_date, symbol, side_bias, start_ts in TODAY_ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, htf_multiday = cached
        vwap_state = screener.VwapState()
        legs = await run_trade_option_native(trade_date, symbol, side, bars_1m, vol_by_ts, htf_multiday,
                                              vwap_state, start_ts, sl_pct)
        if legs:
            all_legs.extend(legs)

    wins = [lg for lg in all_legs if lg["pnl"] > 0]
    losses = [lg for lg in all_legs if lg["pnl"] <= 0]
    total_pnl = sum(lg["pnl"] for lg in all_legs)
    loss_sum = sum(lg["pnl"] for lg in losses)
    pf = (sum(lg["pnl"] for lg in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(all_legs) * 100) if all_legs else 0.0

    by_day = defaultdict(list)
    for lg in all_legs:
        by_day[lg["date"]].append(lg)
    day_rows = []
    for day, legs in sorted(by_day.items()):
        events = []
        for lg in legs:
            events.append((lg["entry_ts"], 1, lg["capital"]))
            events.append((lg["exit_ts"], 0, -lg["capital"]))
        events.sort(key=lambda e: (e[0], -e[1]))
        running = peak = 0.0
        for ts, kind, delta in events:
            running += delta
            peak = max(peak, running)
        day_pnl = sum(lg["pnl"] for lg in legs)
        day_rows.append({"date": day, "n_legs": len(legs), "peak_capital": round(peak, 2),
                          "day_pnl": round(day_pnl, 2)})
    capital_required = max((r["peak_capital"] for r in day_rows), default=0.0)

    equity = peak_eq = max_dd = 0.0
    all_legs_sorted = sorted(all_legs, key=lambda lg: (lg["date"], lg["exit_ts"]))
    for lg in all_legs_sorted:
        equity += lg["pnl"]
        peak_eq = max(peak_eq, equity)
        max_dd = min(max_dd, equity - peak_eq)

    return {
        "sl_pct": sl_pct, "legs": len(all_legs), "win_pct": win_pct,
        "pf": (pf if pf != float("inf") else 9999.0), "total_pnl": round(total_pnl, 2),
        "capital_required": round(capital_required, 2),
        "total_pnl_pct": round(total_pnl / capital_required * 100, 2) if capital_required else 0.0,
        "max_drawdown": round(max_dd, 2), "day_rows": day_rows, "all_legs": all_legs_sorted,
    }


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching real spot history...")
    cache = await fetch_all_spot()

    results = {}
    for pct in SL_PCT_CANDIDATES:
        r = await run_sweep_value(cache, pct)
        results[f"sl_option_{int(pct*100)}%"] = r
        print(f"sl_option_{int(pct*100):>2}%  legs={r['legs']:>3}  win%={r['win_pct']:>5.1f}  "
              f"PF={r['pf']:>7.2f}  total_pnl={r['total_pnl']:>+10.2f}  capital_req={r['capital_required']:>10.2f}  "
              f"total_pnl_pct={r['total_pnl_pct']:>+7.2f}%  max_dd={r['max_drawdown']:>+9.2f}")

    with open("data/oi_orb_option_native_sl_report.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\nWrote data/oi_orb_option_native_sl_report.json")


if __name__ == "__main__":
    asyncio.run(main())
