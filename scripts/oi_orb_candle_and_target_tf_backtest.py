"""
scripts/oi_orb_candle_and_target_tf_backtest.py -- 2026-09-09, two direct
user follow-ups tested together (both re-run the full entry->exit->reentry
pipeline per variant, since either can shift WHEN an SL fires and therefore
where any re-entry resumes from):

  1. "if we check the vwap close below using the normal candle then what is
     the effect" -- SL candle type: Heikin-Ashi (current, validated) vs
     plain OHLC candles, same 20min bucket, same 0.2% min-gap buffer,
     target unchanged (75min/3min).

  2. "go ahead and check the target optimisation concept u may check diff
     tf for the same" -- target HTF timeframe sweep (60/75/90/120/180/240
     min), LTF fixed at 3min (already the established winner from this
     session's earlier HTF/LTF sweep), SL fixed at the current best
     (Heikin-Ashi, 20min, 0.2% gap).

Both priced in real option premium, both use the day-grouped peak-capital
methodology (not a naive sum across every leg).

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_candle_and_target_tf_backtest.py
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
    _vwap_series_full_day, simulate_target_exit, find_reentry,
    to_heikin_ashi, to_n_min_bars, LOOKBACK_CALENDAR_DAYS, TRAP_HTF_MULTIDAY_MIN,
)
from scripts.oi_orb_same_side_trap_multiday_htf_sweep import (
    _to_n_min_bars_dateaware, trap_target_exit_diag_multiday,
)
from scripts.oi_orb_trap_target_full_htf_ltf_sweep import trap_target_exit
from scripts.oi_orb_shaped_sl_streaming_backtest import TODAY_ROWS, find_first_entry_rolling_from
from strategies.oi_orb_screener import screener, stock_resolve
from data_layer.instrument_registry import REGISTRY
from data_layer.historical_candles import fetch_upstox_range_1m, fetch_upstox_intraday_1m

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
TODAY_STR = "2026-09-09"
TRAP_LTF_MIN = 3

_option_cache = {}


# ---------- SL: HA (current) vs normal-candle variant ----------
def sl_min_gap_candle(bars_1m, vwap_by_ts, side, entry_ts, gap_pct, tf_min, use_ha):
    src = to_heikin_ashi(bars_1m) if use_ha else bars_1m
    tf_bars = to_n_min_bars(src, tf_min)
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
        if vwap_at_close is None or vwap_at_close <= 0:
            continue
        adverse_and_beyond = ((vwap_at_close - hb.close) / vwap_at_close >= gap_pct) if side == "CALL" \
            else ((hb.close - vwap_at_close) / vwap_at_close >= gap_pct)
        if not adverse_and_beyond:
            continue
        candidates = [b for b in post_entry if b.ts >= hb.ts]
        if candidates:
            return candidates[0].ts, candidates[0].close
    return None


# ---------- Target: HTF sweep ----------
def simulate_target_exit_htf(entry_ts, entry_price, side, bars_1m, htf_multiday_bars, htf_min):
    if htf_multiday_bars and len(htf_multiday_bars) >= 3:
        ltf_today = to_n_min_bars(bars_1m, TRAP_LTF_MIN)
        diag = trap_target_exit_diag_multiday(entry_ts, entry_price, side, bars_1m, htf_multiday_bars,
                                               ltf_today, TRAP_LTF_MIN)
        if diag.exit_ts is not None:
            return diag.exit_ts, diag.exit_price, "trap_multiday_exit"
    # Intraday fallback stays FIXED at 15min (TRAP_HTF_INTRADAY_MIN, matching
    # the original validated pipeline) regardless of what the multi-day HTF
    # sweep value is -- these are two independent parameters. Deriving this
    # from htf_min was a bug (caught by comparing against two earlier,
    # independently-implemented scripts that agreed with each other on the
    # same 75min/HA config and disagreed with this one until fixed).
    htf15 = to_n_min_bars(bars_1m, 15)
    ltf3 = to_n_min_bars(bars_1m, TRAP_LTF_MIN)
    t_ts, t_px, t_reason = trap_target_exit(entry_ts, entry_price, side, bars_1m, htf15, ltf3, TRAP_LTF_MIN)
    return t_ts, t_px, t_reason


# ---------- Target: immediate zone-touch exit (skip the 3min S&R ladder) ----------
def simulate_target_exit_immediate_touch(entry_ts, entry_price, side, bars_1m, htf_multiday_bars, target_htf_min):
    """Direct user spec: 'u might also skip the s&r of 3 min logic in
    target if it reached target u can close the trade' -- same zone-locking
    mechanism as trap_target_exit_diag_multiday (verbatim, only the most
    recently locked zone as of each bar), but exits the INSTANT price
    touches the zone, without waiting for the 3-min S&R ladder's S1/R1
    breach confirmation. Falls back to the existing ladder-based mechanic
    (which already has its own intraday/EOD fallback) if no multiday zone
    is ever touched, so this never loses the safety net the ladder version
    already has."""
    if htf_multiday_bars and len(htf_multiday_bars) >= 3:
        zones_fn = screener.bull_trap_zones if side == "CALL" else screener.sharp_bear_zones
        zones = zones_fn(htf_multiday_bars)
        post_entry_1m = [b for b in bars_1m if b.ts >= entry_ts]
        for b in post_entry_1m:
            locked_so_far = [z for z in zones if z["lock_ts"] is not None and z["lock_ts"] <= b.ts]
            if locked_so_far:
                z = max(locked_so_far, key=lambda zz: zz["lock_ts"])
                touched = (b.low <= z["zone_hi"]) and (b.high >= z["zone_lo"])
                if touched:
                    return b.ts, b.close, "trap_zone_touch_immediate"
    return simulate_target_exit_htf(entry_ts, entry_price, side, bars_1m, htf_multiday_bars, target_htf_min)


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
        cache[key] = (today_bars, vol_by_ts, all_bars)
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


async def run_trade(trade_date, symbol, side, bars_1m, vol_by_ts, all_bars, vwap_state, start_ts,
                     use_ha, sl_gap_pct, sl_tf_min, target_htf_min, target_mode="ladder"):
    entry = find_first_entry_rolling_from(bars_1m, side, vol_by_ts, vwap_state, start_ts)
    if entry is None:
        return []
    vwap_by_ts = _vwap_series_full_day(bars_1m, vol_by_ts)
    htf_multiday = _to_n_min_bars_dateaware(all_bars, target_htf_min)
    legs = []
    sl_reentry_used = False
    cur_entry_ts, cur_entry_spot = entry
    while True:
        contract = await resolve_contract_and_series(symbol, side, trade_date, cur_entry_spot)
        if contract is None:
            break
        series = contract["series"]
        entry_hhmm = cur_entry_ts.strftime("%H:%M")
        _, entry_premium = premium_at_or_after(series, entry_hhmm)
        if entry_premium is None or entry_premium <= 0:
            break

        sl = sl_min_gap_candle(bars_1m, vwap_by_ts, side, cur_entry_ts, sl_gap_pct, sl_tf_min, use_ha)
        if target_mode == "immediate":
            t_ts, t_spot_px, t_reason = simulate_target_exit_immediate_touch(
                cur_entry_ts, cur_entry_spot, side, bars_1m, htf_multiday, target_htf_min)
        else:
            t_ts, t_spot_px, t_reason = simulate_target_exit_htf(cur_entry_ts, cur_entry_spot, side, bars_1m,
                                                                   htf_multiday, target_htf_min)
        t_hhmm = t_ts.strftime("%H:%M")

        if sl is not None and sl[0].strftime("%H:%M") <= t_hhmm:
            exit_hhmm = sl[0].strftime("%H:%M")
            _, exit_premium = premium_at_or_after(series, exit_hhmm)
            reason = "sl"
        else:
            exit_hhmm = t_hhmm
            _, exit_premium = premium_at_or_after(series, exit_hhmm)
            reason = t_reason
        if exit_premium is None:
            exit_hhmm, exit_premium = series[-1]
            reason = reason if reason != "sl" else "eod_close"

        capital = entry_premium * contract["lot"]
        pnl = (exit_premium - entry_premium) * contract["lot"]
        legs.append({
            "date": trade_date, "symbol": symbol, "side": side, "entry_ts": entry_hhmm,
            "exit_ts": exit_hhmm, "reason": reason, "capital": round(capital, 2), "pnl": round(pnl, 2),
        })

        if reason != "sl" or sl_reentry_used:
            break
        sl_reentry_used = True
        exit_dt = cur_entry_ts.replace(hour=int(exit_hhmm[:2]), minute=int(exit_hhmm[3:5]))
        nxt = find_reentry(bars_1m, side, exit_dt, vwap_state)
        if nxt is None:
            break
        cur_entry_ts, cur_entry_spot = nxt
    return legs


async def run_variant(cache, use_ha, sl_gap_pct, sl_tf_min, target_htf_min, target_mode="ladder"):
    all_legs = []
    for trade_date, symbol, side_bias in OLD_ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, all_bars = cached
        vwap_state = screener.VwapState()
        orb_end_bars = [b for b in bars_1m if b.ts.strftime("%H:%M") >= ORB_END]
        if not orb_end_bars:
            continue
        legs = await run_trade(trade_date, symbol, side, bars_1m, vol_by_ts, all_bars, vwap_state,
                                orb_end_bars[0].ts, use_ha, sl_gap_pct, sl_tf_min, target_htf_min, target_mode)
        all_legs.extend(legs)
    for trade_date, symbol, side_bias, start_ts in TODAY_ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, all_bars = cached
        vwap_state = screener.VwapState()
        legs = await run_trade(trade_date, symbol, side, bars_1m, vol_by_ts, all_bars, vwap_state,
                                start_ts, use_ha, sl_gap_pct, sl_tf_min, target_htf_min, target_mode)
        all_legs.extend(legs)
    return summarize(all_legs)


def summarize(all_legs):
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
        day_rows.append({"date": day, "peak_capital": round(peak, 2), "day_pnl": round(sum(lg["pnl"] for lg in legs), 2)})
    capital_required = max((r["peak_capital"] for r in day_rows), default=0.0)

    equity = peak_eq = max_dd = 0.0
    for lg in sorted(all_legs, key=lambda x: (x["date"], x["exit_ts"])):
        equity += lg["pnl"]
        peak_eq = max(peak_eq, equity)
        max_dd = min(max_dd, equity - peak_eq)

    return {"legs": len(all_legs), "win_pct": win_pct, "pf": (pf if pf != float("inf") else 9999.0),
            "total_pnl": round(total_pnl, 2), "capital_required": round(capital_required, 2),
            "total_pnl_pct": round(total_pnl / capital_required * 100, 2) if capital_required else 0.0,
            "max_drawdown": round(max_dd, 2)}


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching real spot history...")
    cache = await fetch_all_spot()

    results = {}
    print("\n--- SL candle type ---")
    for use_ha, label in [(True, "sl_HA_candle (current)"), (False, "sl_normal_candle")]:
        r = await run_variant(cache, use_ha, 0.002, 20, TRAP_HTF_MULTIDAY_MIN)
        results[label] = r
        print(f"{label:<28} legs={r['legs']:>3}  win%={r['win_pct']:>5.1f}  PF={r['pf']:>7.2f}  "
              f"total_pnl={r['total_pnl']:>+10.2f}  capital={r['capital_required']:>10.2f}  "
              f"ret%={r['total_pnl_pct']:>+7.2f}  max_dd={r['max_drawdown']:>+9.2f}")

    print("\n--- Target HTF sweep (LTF=3min fixed) ---")
    for htf in [60, 75, 90, 120, 180, 240]:
        label = f"target_htf_{htf}min"
        r = await run_variant(cache, True, 0.002, 20, htf)
        results[label] = r
        print(f"{label:<28} legs={r['legs']:>3}  win%={r['win_pct']:>5.1f}  PF={r['pf']:>7.2f}  "
              f"total_pnl={r['total_pnl']:>+10.2f}  capital={r['capital_required']:>10.2f}  "
              f"ret%={r['total_pnl_pct']:>+7.2f}  max_dd={r['max_drawdown']:>+9.2f}")

    print("\n--- Target: immediate zone-touch vs ladder (75min HTF fixed) ---")
    for target_mode, label in [("ladder", "target_ladder_75min (current)"), ("immediate", "target_immediate_touch_75min")]:
        r = await run_variant(cache, True, 0.002, 20, TRAP_HTF_MULTIDAY_MIN, target_mode)
        results[label] = r
        print(f"{label:<28} legs={r['legs']:>3}  win%={r['win_pct']:>5.1f}  PF={r['pf']:>7.2f}  "
              f"total_pnl={r['total_pnl']:>+10.2f}  capital={r['capital_required']:>10.2f}  "
              f"ret%={r['total_pnl_pct']:>+7.2f}  max_dd={r['max_drawdown']:>+9.2f}")

    with open("data/oi_orb_candle_and_target_tf_report.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\nWrote data/oi_orb_candle_and_target_tf_report.json")


if __name__ == "__main__":
    asyncio.run(main())
