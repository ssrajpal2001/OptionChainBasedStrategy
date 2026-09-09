"""
scripts/oi_orb_two_phase_sl_backtest.py -- 2026-09-09, direct user
follow-up: agreed the spot-based SL should stay for capital protection, but
"issue is we are coming out of trade quite early and missing the trend as
we are option buyers." Two-phase stop, same pattern already proven live
elsewhere in this codebase (FVG's and OI-Flow's own step-locked trailing
stops):

  Phase 1 (unproven trade): unchanged -- the validated spot VWAP-close SL
  (20min HA, 0.2% min-gap buffer). Protects capital exactly as it does
  today.

  Phase 2 (proven trade): the instant the option's OWN real premium first
  reaches entry_premium * (1 + trigger_pct), Phase 1 is switched OFF
  entirely and control hands to a step-locked trailing stop on the
  option's own premium -- once triggered, lock a floor at
  entry*(1+first_lock_pct); every further step_pct of additional gain
  (measured off the running PEAK premium since entry) ratchets the floor
  up another step_lock_pct (never loosens). Exit the instant premium falls
  to/through the current floor.

  Target (75min/3min same-side trap) and re-entry (1x after any SL,
  phase-1 or phase-2) are unchanged throughout.

trigger_pct/first_lock_pct/step_pct/step_lock_pct are all swept, per
direct user instruction ("these values are part of optimisation as well").

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_two_phase_sl_backtest.py
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
from scripts.oi_orb_sl_concept_comparison import sl_min_gap
from strategies.oi_orb_screener import screener, stock_resolve
from data_layer.instrument_registry import REGISTRY
from data_layer.historical_candles import fetch_upstox_range_1m, fetch_upstox_intraday_1m

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
TODAY_STR = "2026-09-09"

TRIGGER_PCTS = [0.10, 0.15, 0.20]
FIRST_LOCK_PCTS = [0.05, 0.08]
STEP_PCTS = [0.10, 0.15]
STEP_LOCK_PCTS = [0.05, 0.08]

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


async def build_trade_skeletons(cache):
    """Resolve entry + option contract/series + spot-SL-ts + target-ts ONCE
    per trade -- these never change across the parameter sweep, only the
    phase-2 trail decision does, so this is computed once and reused for
    every combo (fast, no repeated network calls)."""
    skeletons = []

    async def process(trade_date, symbol, side, bars_1m, vol_by_ts, htf_multiday, vwap_state, start_ts):
        entry = find_first_entry_rolling_from(bars_1m, side, vol_by_ts, vwap_state, start_ts)
        if entry is None:
            return
        legs = []
        sl_reentry_used = False
        cur_entry_ts, cur_entry_spot = entry
        vwap_by_ts = _vwap_series_full_day(bars_1m, vol_by_ts)
        while True:
            contract = await resolve_contract_and_series(symbol, side, trade_date, cur_entry_spot)
            if contract is None:
                break
            series = contract["series"]
            entry_hhmm = cur_entry_ts.strftime("%H:%M")
            e_ts, entry_premium = premium_at_or_after(series, entry_hhmm)
            if entry_premium is None or entry_premium <= 0:
                break

            t_ts, t_spot_px, t_reason = simulate_target_exit(cur_entry_ts, cur_entry_spot, side, bars_1m, htf_multiday)
            t_hhmm = t_ts.strftime("%H:%M")
            _, target_premium = premium_at_or_after(series, t_hhmm)
            if target_premium is None:
                target_premium = series[-1][1]
                t_hhmm = series[-1][0]

            sl1 = sl_min_gap(bars_1m, vwap_by_ts, side, cur_entry_ts, 0.002)
            sl1_hhmm = sl1[0].strftime("%H:%M") if sl1 is not None else None

            post_entry_series = [(ts, px) for ts, px in series if ts >= entry_hhmm]

            legs.append({
                "date": trade_date, "symbol": symbol, "side": side,
                "expiry": contract["expiry"], "strike": contract["strike"],
                "option_type": contract["option_type"], "lot": contract["lot"],
                "entry_ts": entry_hhmm, "entry_premium": entry_premium,
                "target_ts": t_hhmm, "target_premium": target_premium, "target_reason": t_reason,
                "sl1_ts": sl1_hhmm, "post_entry_series": post_entry_series,
            })

            # advance using phase-1-only outcome for re-entry chaining purposes
            # (re-entry only ever follows an SL stop-out; whether that SL was
            # phase-1 or phase-2 doesn't change WHERE the next scan resumes from,
            # since find_reentry only needs a resume timestamp+side).
            resume_ts = None
            if sl1_hhmm is not None and (sl1_hhmm <= t_hhmm):
                resume_ts = sl1[0]
            if resume_ts is None or sl_reentry_used:
                break
            sl_reentry_used = True
            nxt = find_reentry(bars_1m, side, resume_ts, vwap_state)
            if nxt is None:
                break
            cur_entry_ts, cur_entry_spot = nxt
        if legs:
            skeletons.append(legs)

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
        await process(trade_date, symbol, side, bars_1m, vol_by_ts, htf_multiday, vwap_state, orb_end_bars[0].ts)

    for trade_date, symbol, side_bias, start_ts in TODAY_ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, htf_multiday = cached
        vwap_state = screener.VwapState()
        await process(trade_date, symbol, side, bars_1m, vol_by_ts, htf_multiday, vwap_state, start_ts)

    return skeletons


def resolve_leg_for_params(leg, trigger_pct, first_lock_pct, step_pct, step_lock_pct):
    entry_premium = leg["entry_premium"]
    series = leg["post_entry_series"]
    trigger_price = entry_premium * (1 + trigger_pct)

    trigger_ts = None
    for ts, px in series:
        if px >= trigger_price:
            trigger_ts = ts
            break

    sl1_ts = leg["sl1_ts"]
    sl1_active = trigger_ts is None or (sl1_ts is not None and sl1_ts <= trigger_ts)

    candidates = []
    if sl1_active and sl1_ts is not None:
        _, sl1_premium = premium_at_or_after(series, sl1_ts)
        if sl1_premium is not None:
            candidates.append((sl1_ts, sl1_premium, "sl_phase1"))

    if trigger_ts is not None:
        peak = trigger_price
        floor = entry_premium * (1 + first_lock_pct)
        phase2_exit = None
        for ts, px in series:
            if ts < trigger_ts:
                continue
            peak = max(peak, px)
            extra_gain = (peak - trigger_price) / entry_premium
            n_steps = int(extra_gain // step_pct) if step_pct > 0 else 0
            new_floor = entry_premium * (1 + first_lock_pct + n_steps * step_lock_pct)
            floor = max(floor, new_floor)
            if px <= floor:
                phase2_exit = (ts, px, "sl_phase2_trail")
                break
        if phase2_exit is None and series:
            phase2_exit = (series[-1][0], series[-1][1], "eod_close")
        candidates.append(phase2_exit)

    candidates.append((leg["target_ts"], leg["target_premium"], leg["target_reason"]))
    candidates.sort(key=lambda c: c[0])
    exit_ts, exit_premium, reason = candidates[0]

    capital = entry_premium * leg["lot"]
    pnl = (exit_premium - entry_premium) * leg["lot"]
    return {**{k: leg[k] for k in ("date", "symbol", "side", "expiry", "strike", "option_type", "lot", "entry_ts")},
            "entry_premium": round(entry_premium, 2), "exit_ts": exit_ts, "exit_premium": round(exit_premium, 2),
            "reason": reason, "capital": round(capital, 2), "pnl": round(pnl, 2),
            "pnl_pct": round(pnl / capital * 100, 2) if capital else 0.0}


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
    print("Resolving option contracts/series and pre-computing trade skeletons...")
    skeletons = await build_trade_skeletons(cache)
    n_legs = sum(len(s) for s in skeletons)
    print(f"Built {len(skeletons)} trades / {n_legs} legs with real option data.")

    results = {}
    for trig in TRIGGER_PCTS:
        for fl in FIRST_LOCK_PCTS:
            for sp in STEP_PCTS:
                for sl in STEP_LOCK_PCTS:
                    all_legs = [resolve_leg_for_params(lg, trig, fl, sp, sl) for legs in skeletons for lg in legs]
                    r = summarize(all_legs)
                    name = f"trig{int(trig*100)}_lock{int(fl*100)}_step{int(sp*100)}_steplock{int(sl*100)}"
                    results[name] = r

    ranked = sorted(results.items(), key=lambda kv: -kv[1]["pf"])
    print(f"\n{'combo':<32} {'legs':>5} {'win%':>6} {'PF':>7} {'total_pnl':>11} {'capital':>10} {'ret%':>7} {'max_dd':>9}")
    for name, r in ranked[:15]:
        print(f"{name:<32} {r['legs']:>5} {r['win_pct']:>6.1f} {r['pf']:>7.2f} {r['total_pnl']:>11.2f} "
              f"{r['capital_required']:>10.2f} {r['total_pnl_pct']:>7.2f} {r['max_drawdown']:>9.2f}")

    with open("data/oi_orb_two_phase_sl_report.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\nWrote data/oi_orb_two_phase_sl_report.json")


if __name__ == "__main__":
    asyncio.run(main())
