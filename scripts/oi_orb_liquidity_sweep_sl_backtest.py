"""
scripts/oi_orb_liquidity_sweep_sl_backtest.py -- 2026-09-09, direct user
follow-up: "as an option buyers we would love to ride the trend but with
this logic we are coming out of trend quite early... can u check the
liquidity sweep logic for sl."

The Liquidity Sweep strategy (strategies/liquidity_sweep/) was fully
deleted 2026-09-06 (git commit 217e6b8, part of the 8->3 strategy
narrowing) -- recoverable via git history. Its SL concept (detector.py's
own compute_trade_plan docstring): "SL = the swept candle's own extreme --
if the sweep was real, price should never trade back past its own origin."
A genuine structural stop anchored to a real stop-hunt reversal point, not
a moving-average-style proxy (VWAP) -- exactly the kind of wider, price-
action-anchored stop that could let a real trend breathe instead of
getting cut by an ordinary VWAP cross.

Bar/SwingPoint/find_swing_points/latest_swing_level/detect_sweep below are
copied VERBATIM from the real, git-recovered detector.py (same functions,
same defaults) -- not reimplemented, per this codebase's own
feedback_backtest_drive_real_class discipline. latest_pool_level (the
clustering variant) is skipped here since its tol_pts=5.0 default was
tuned for NIFTY's own price scale and would need per-stock rescaling for
this dataset's wildly different price levels (46 to 47000) -- using the
simpler single-pivot latest_swing_level avoids that unresolved scaling
question for this first pass.

Mechanic: for each OI-ORB trade (same entry as the current best config),
walk the stock's own 5-min bars from the trading day's start up to entry,
tracking confirmed swing highs/lows and any sweep events found along the
way. The MOST RECENT sweep matching the trade's own direction (a bull
sweep -- swept a LOW -- for a CALL/long trade; a bear sweep -- swept a
HIGH -- for a PUT/short trade) becomes the SL anchor: that sweep candle's
own low (CALL) / high (PUT). If no such sweep exists before entry, falls
back to the current best SL (20min HA VWAP-close, 0.2% min-gap) as a
safety net. Target stays the validated 180min/3min same-side trap.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_liquidity_sweep_sl_backtest.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional

sys.path.insert(0, ".")

from scripts.oi_orb_entry_mode_backtest import (
    ROWS as OLD_ROWS, SIDE, ORB_END, resolve_eq_key, to_bars, volume_by_ts, compute_orb,
)
from scripts.oi_orb_full_live_logic_backtest import (
    _vwap_series_full_day, find_reentry, to_n_min_bars, LOOKBACK_CALENDAR_DAYS,
)
from scripts.oi_orb_same_side_trap_multiday_htf_sweep import _to_n_min_bars_dateaware
from scripts.oi_orb_candle_and_target_tf_backtest import sl_min_gap_candle, simulate_target_exit_htf
from scripts.oi_orb_shaped_sl_streaming_backtest import TODAY_ROWS, find_first_entry_rolling_from
from strategies.oi_orb_screener import screener, stock_resolve
from data_layer.instrument_registry import REGISTRY
from data_layer.historical_candles import fetch_upstox_range_1m, fetch_upstox_intraday_1m

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
TODAY_STR = "2026-09-09"
TARGET_HTF_MIN = 180   # this session's own just-validated winner
_option_cache = {}


# ── verbatim from the deleted strategies/liquidity_sweep/detector.py ──────
@dataclass(frozen=True)
class SwingPoint:
    index: int
    timestamp: datetime
    price: float
    body_extreme: float
    kind: str


def find_swing_points(bars, pivot_left: int = 5, pivot_right: int = 5) -> List[SwingPoint]:
    points: List[SwingPoint] = []
    n = len(bars)
    for i in range(pivot_left, n - pivot_right):
        bar = bars[i]
        before = bars[i - pivot_left:i]
        after = bars[i + 1:i + 1 + pivot_right]
        if all(bar.high > b.high for b in before) and all(bar.high > b.high for b in after):
            points.append(SwingPoint(i, bar.ts, bar.high, max(bar.open, bar.close), "HIGH"))
        if all(bar.low < b.low for b in before) and all(bar.low < b.low for b in after):
            points.append(SwingPoint(i, bar.ts, bar.low, min(bar.open, bar.close), "LOW"))
    return points


def latest_swing_level(swings: List[SwingPoint], kind: str) -> Optional[SwingPoint]:
    relevant = [s for s in swings if s.kind == kind]
    if not relevant:
        return None
    return max(relevant, key=lambda s: s.index)


@dataclass(frozen=True)
class SweepResult:
    bear: bool
    bull: bool


def detect_sweep(bar, level_high: Optional[SwingPoint], level_low: Optional[SwingPoint]) -> SweepResult:
    bear = level_high is not None and bar.high > level_high.price and bar.close < level_high.body_extreme
    bull = level_low is not None and bar.low < level_low.price and bar.close > level_low.body_extreme
    return SweepResult(bear=bear, bull=bull)
# ── end verbatim block ──────────────────────────────────────────────────


def find_sl_anchor(bars_5m, side, entry_ts):
    """Replays swing/sweep detection on the stock's own 5min bars up to
    entry, returns the most recent sweep candle's own extreme matching the
    trade's direction, or None if no such sweep exists yet."""
    pre_entry = [b for b in bars_5m if b.ts <= entry_ts]
    if len(pre_entry) < 12:
        return None
    swings = find_swing_points(pre_entry, pivot_left=5, pivot_right=5)
    best_anchor = None
    best_ts = None
    level_high = level_low = None
    hi_ptr = lo_ptr = 0
    highs = sorted((s for s in swings if s.kind == "HIGH"), key=lambda s: s.index)
    lows = sorted((s for s in swings if s.kind == "LOW"), key=lambda s: s.index)
    for i, bar in enumerate(pre_entry):
        while hi_ptr < len(highs) and highs[hi_ptr].index + 5 <= i:
            level_high = highs[hi_ptr]
            hi_ptr += 1
        while lo_ptr < len(lows) and lows[lo_ptr].index + 5 <= i:
            level_low = lows[lo_ptr]
            lo_ptr += 1
        sweep = detect_sweep(bar, level_high, level_low)
        if side == "CALL" and sweep.bull:
            best_anchor, best_ts = bar.low, bar.ts
            level_low = None   # consumed
        if side == "PUT" and sweep.bear:
            best_anchor, best_ts = bar.high, bar.ts
            level_high = None   # consumed
    return best_anchor


def liquidity_sweep_sl_exit(bars_1m, side, entry_ts, sl_anchor):
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]
    for b in post_entry:
        touched = (b.low <= sl_anchor) if side == "CALL" else (b.high >= sl_anchor)
        if touched:
            return b.ts, sl_anchor
    return None


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


async def run_trade(trade_date, symbol, side, bars_1m, vol_by_ts, all_bars, vwap_state, start_ts, use_liq_sweep):
    entry = find_first_entry_rolling_from(bars_1m, side, vol_by_ts, vwap_state, start_ts)
    if entry is None:
        return []
    vwap_by_ts = _vwap_series_full_day(bars_1m, vol_by_ts)
    htf_multiday = _to_n_min_bars_dateaware(all_bars, TARGET_HTF_MIN)
    bars_5m = to_n_min_bars(bars_1m, 5)
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

        sl = None
        if use_liq_sweep:
            anchor = find_sl_anchor(bars_5m, side, cur_entry_ts)
            if anchor is not None:
                sl = liquidity_sweep_sl_exit(bars_1m, side, cur_entry_ts, anchor)
        if sl is None:
            sl = sl_min_gap_candle(bars_1m, vwap_by_ts, side, cur_entry_ts, 0.002, 20, True)

        t_ts, t_spot_px, t_reason = simulate_target_exit_htf(cur_entry_ts, cur_entry_spot, side, bars_1m,
                                                               htf_multiday, TARGET_HTF_MIN)
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
        legs.append({"date": trade_date, "symbol": symbol, "side": side, "entry_ts": entry_hhmm,
                      "exit_ts": exit_hhmm, "reason": reason, "capital": round(capital, 2), "pnl": round(pnl, 2)})

        if reason != "sl" or sl_reentry_used:
            break
        sl_reentry_used = True
        exit_dt = cur_entry_ts.replace(hour=int(exit_hhmm[:2]), minute=int(exit_hhmm[3:5]))
        nxt = find_reentry(bars_1m, side, exit_dt, vwap_state)
        if nxt is None:
            break
        cur_entry_ts, cur_entry_spot = nxt
    return legs


async def run_variant(cache, use_liq_sweep):
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
                                orb_end_bars[0].ts, use_liq_sweep)
        all_legs.extend(legs)
    for trade_date, symbol, side_bias, start_ts in TODAY_ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, all_bars = cached
        vwap_state = screener.VwapState()
        legs = await run_trade(trade_date, symbol, side, bars_1m, vol_by_ts, all_bars, vwap_state,
                                start_ts, use_liq_sweep)
        all_legs.extend(legs)
    return summarize(all_legs), all_legs


def summarize(all_legs):
    wins = [lg for lg in all_legs if lg["pnl"] > 0]
    losses = [lg for lg in all_legs if lg["pnl"] <= 0]
    total_pnl = sum(lg["pnl"] for lg in all_legs)
    loss_sum = sum(lg["pnl"] for lg in losses)
    pf = (sum(lg["pnl"] for lg in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(all_legs) * 100) if all_legs else 0.0
    n_liq_sl = sum(1 for lg in all_legs if lg["reason"] == "sl")

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
            "max_drawdown": round(max_dd, 2), "sl_hits": n_liq_sl}


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching real spot history...")
    cache = await fetch_all_spot()

    results = {}
    for use_liq, label in [(False, "baseline_vwap_close_180htf"), (True, "liquidity_sweep_sl_180htf")]:
        r, legs = await run_variant(cache, use_liq)
        results[label] = r
        print(f"{label:<28} legs={r['legs']:>3}  win%={r['win_pct']:>5.1f}  PF={r['pf']:>7.2f}  "
              f"total_pnl={r['total_pnl']:>+10.2f}  capital={r['capital_required']:>10.2f}  "
              f"ret%={r['total_pnl_pct']:>+7.2f}  max_dd={r['max_drawdown']:>+9.2f}  sl_hits={r['sl_hits']}")

    with open("data/oi_orb_liquidity_sweep_sl_report.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\nWrote data/oi_orb_liquidity_sweep_sl_report.json")


if __name__ == "__main__":
    asyncio.run(main())
