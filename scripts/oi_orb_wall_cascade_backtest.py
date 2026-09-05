"""
scripts/oi_orb_wall_cascade_backtest.py -- 2026-09-05, direct user spec:
"for support resistance u should check 1 hr but for that u need to have
prev days data as well ... if it reached the support resistance we can
jump to 3 min support resistance as tsl and wait ... it might happen that
wall(s&r) might break as well and we continue the trend."

Cascade mechanic (regime-switching, not a flat %):
  NORMAL   -- position protected only by the original fixed ORB-extreme SL
              (unchanged from variant 3), same as if this feature didn't
              exist. Watches a genuine multi-day 1-HOUR S&R wall (the
              nearest ESTABLISHED R1/R2 above entry for a CALL, S1/S2 below
              entry for a PUT), built from strategies.d1_trap_option.
              support_resistance.SupportResistanceCalculator fed ~10 PRIOR
              trading days of real 60-min bars, continuously (no daily
              reset, same multi-day discipline as this codebase's own
              PositionalSRTracker) -- plus today's own 60-min bars as they
              close (walk-forward, never a future bar).
  AT_WALL  -- entered the instant price touches that wall (within
              wall_gate_pct). Switches the ACTIVE stop to a fresh 3-minute
              SupportResistanceCalculator's own live S1 (CALL) / R1 (PUT)
              -- the same reactive TSL SRPingPongTracker already uses live,
              just re-seeded at 3-min granularity right at the wall instead
              of running from entry. If price reverses off the wall and
              undercuts that 3-min level, EXIT there ("wall_reversal_tsl").
  BROKEN   -- if instead price's bar CLOSE clearly moves through the wall,
              that wall is marked broken (never re-checked) and state drops
              back to NORMAL -- the trade keeps running (the wall failed as
              resistance/support, the trend continued), watching for the
              NEXT established wall further out. This is what actually
              distinguishes a genuine reversal from a normal breakout,
              which no flat-% trail (the prior sweep) could ever tell
              apart.
  The original fixed ORB-extreme SL remains the absolute backstop in every
  state, same as EOD close.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_wall_cascade_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_range_1m
from scripts.oi_orb_entry_mode_backtest import (
    ROWS, SIDE, Trade, compute_orb, resolve_eq_key, simulate_fixed_sl_exit,
    simulate_pct_tsl_exit, simulate_two_param_tsl_exit,
    run_vwap_retest_immediate_if_historically_fulfilled,
    to_bars, volume_by_ts,
)
from strategies.core.support_resistance import SupportResistanceCalculator
from strategies.liquidity_trap.detector import Bar
from functools import partial

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
LOOKBACK_CALENDAR_DAYS = 15   # ~10 trading days
WALL_GATE_PCT = 0.15


def to_n_min_bars_dated(bars_1m, n):
    """Date-aware resample -- to_n_min_bars (oi_orb_entry_mode_backtest)
    buckets by (hour, floored_minute) only, silently merging same-clock-time
    bars across DIFFERENT days -- fine for that module's single-day callers,
    wrong for this script's multi-day HTF lookback."""
    buckets = {}
    for b in bars_1m:
        floored = (b.ts.minute // n) * n
        key = (b.ts.date(), b.ts.hour, floored)
        buckets.setdefault(key, []).append(b)
    out = []
    for key in sorted(buckets.keys()):
        g = sorted(buckets[key], key=lambda x: x.ts)
        out.append(Bar(ts=g[0].ts, open=g[0].open, high=max(x.high for x in g),
                        low=min(x.low for x in g), close=g[-1].close))
    return out


async def fetch_htf_calc(symbol: str, trade_date: str):
    eq_key = resolve_eq_key(symbol)
    if eq_key is None:
        return None
    d = date.fromisoformat(trade_date)
    start = d - timedelta(days=LOOKBACK_CALENDAR_DAYS)
    end = d - timedelta(days=1)
    rows = await fetch_upstox_range_1m(eq_key, TOKEN, start, end)
    calc = SupportResistanceCalculator()
    if not rows:
        return calc   # empty -- no prior walls known, degrades to NORMAL/fixed-SL behavior
    bars_1m = to_bars(rows)
    bars_60m = to_n_min_bars_dated(bars_1m, 60)
    for b in bars_60m:
        calc.process_straddle_candle(symbol, dict(timestamp=b.ts, high=b.high, low=b.low, close=b.close), silent=True)
    return calc


def simulate_wall_cascade_exit(entry_ts, entry_price, side, ref_h, ref_l, bars_1m,
                                htf_calc, symbol, wall_gate_pct=WALL_GATE_PCT):
    sl_orig = ref_l if side == "CALL" else ref_h
    stop_level = sl_orig
    state = "NORMAL"
    wall_level = None
    broken_walls = set()
    bars_60m = to_n_min_bars_dated(bars_1m, 60)
    fed_60m = set()
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]

    def nearest_wall():
        st = htf_calc.get_calculated_sr_state(symbol)
        levels = st.get("sr_levels", {})
        cands = []
        names = ("R1", "R2") if side == "CALL" else ("S1", "S2")
        for name in names:
            lvl = levels.get(name)
            if not lvl or not lvl.get("is_established") or name in broken_walls:
                continue
            val = lvl.get("high") if side == "CALL" else lvl.get("low")
            if not val:
                continue
            if side == "CALL" and val > entry_price:
                cands.append((name, val))
            elif side == "PUT" and val < entry_price:
                cands.append((name, val))
        if not cands:
            return None
        return min(cands, key=lambda c: c[1]) if side == "CALL" else max(cands, key=lambda c: c[1])

    for b in post_entry:
        b_min = b.ts.hour * 60 + b.ts.minute
        for hb in bars_60m:
            if hb.ts in fed_60m:
                continue
            hb_end = hb.ts.hour * 60 + hb.ts.minute + 60
            if hb_end <= b_min:
                htf_calc.process_straddle_candle(symbol, dict(timestamp=hb.ts, high=hb.high, low=hb.low, close=hb.close), silent=True)
                fed_60m.add(hb.ts)

        if state == "NORMAL":
            wall = nearest_wall()
            if wall:
                name, val = wall
                touched = (b.high >= val * (1 - wall_gate_pct / 100)) if side == "CALL" else \
                          (b.low <= val * (1 + wall_gate_pct / 100))
                if touched:
                    state, wall_level = "AT_WALL", (name, val)

        if state == "AT_WALL":
            name, val = wall_level
            broke = (b.close > val * 1.001) if side == "CALL" else (b.close < val * 0.999)
            if broke:
                broken_walls.add(name)
                state, wall_level = "NORMAL", None
            else:
                window_1m = [x for x in bars_1m if entry_ts <= x.ts <= b.ts]
                tsl3_bars = to_n_min_bars_dated(window_1m, 3)
                tsl3 = SupportResistanceCalculator()
                for tb in tsl3_bars:
                    tsl3.process_straddle_candle(symbol, dict(timestamp=tb.ts, high=tb.high, low=tb.low, close=tb.close), silent=True)
                st3 = tsl3.get_calculated_sr_state(symbol)
                s1_lvl = st3.get("sr_levels", {}).get("S1")
                r1_lvl = st3.get("sr_levels", {}).get("R1")
                if side == "CALL":
                    trail = s1_lvl["low"] if s1_lvl else None
                    if trail:
                        stop_level = max(stop_level, trail)
                    if b.low <= stop_level:
                        return b.ts, stop_level, "wall_reversal_tsl"
                else:
                    trail = r1_lvl["high"] if r1_lvl else None
                    if trail:
                        stop_level = min(stop_level, trail)
                    if b.high >= stop_level:
                        return b.ts, stop_level, "wall_reversal_tsl"

        breach_fixed = (b.low <= sl_orig) if side == "CALL" else (b.high >= sl_orig)
        if breach_fixed:
            return b.ts, sl_orig, "fixed_sl"

    if post_entry:
        last = post_entry[-1]
        return last.ts, last.close, "eod_close"
    return entry_ts, entry_price, "no_data_after_entry"


async def fetch_all():
    cache = {}
    htf_cache = {}
    for trade_date, symbol, side_bias in ROWS:
        key = (trade_date, symbol)
        if key in cache:
            continue
        eq_key = resolve_eq_key(symbol)
        if eq_key is None:
            cache[key] = None
            continue
        d = date.fromisoformat(trade_date)
        rows = await fetch_upstox_range_1m(eq_key, TOKEN, d, d)
        if not rows:
            cache[key] = None
            continue
        bars_1m = to_bars(rows)
        vol_by_ts = volume_by_ts(rows)
        orb = compute_orb(bars_1m)
        if orb is None:
            cache[key] = None
            continue
        orb_h, orb_l = orb
        cache[key] = (bars_1m, vol_by_ts, orb_h, orb_l)
        htf_cache[key] = await fetch_htf_calc(symbol, trade_date)
        print(f"  fetched {trade_date} {symbol} (+ {LOOKBACK_CALENDAR_DAYS}d HTF context)")
    return cache, htf_cache


def run_one(cache, htf_cache, mode):
    trades = []
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l = cached
        if mode == "baseline":
            exit_fn = simulate_fixed_sl_exit
        elif mode == "single_pct_2.0":
            exit_fn = partial(simulate_pct_tsl_exit, activate_pct=2.0)
        elif mode == "two_param_0.75_0.15":
            exit_fn = partial(simulate_two_param_tsl_exit, activate_pct=0.75, trail_pct=0.15)
        elif mode == "wall_cascade":
            htf_calc = htf_cache.get((trade_date, symbol))
            if htf_calc is None:
                exit_fn = simulate_fixed_sl_exit
            else:
                exit_fn = partial(simulate_wall_cascade_exit, htf_calc=htf_calc, symbol=symbol)
        vwap_trades = run_vwap_retest_immediate_if_historically_fulfilled(
            bars_1m, side, orb_h, orb_l, vol_by_ts, exit_fn=exit_fn)
        for (entry_ts, entry_price, exit_ts, exit_price, reason) in vwap_trades:
            trades.append(Trade(trade_date, symbol, side, "vwap_retest", entry_ts, entry_price,
                                 exit_ts, exit_price, reason))
    return trades


def summarize(label, trades):
    entered = [t for t in trades if t.entry_price is not None]
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    avg = (total / len(entered)) if entered else 0.0
    print(f"{label:>26}  entered={len(entered):2d}  win%={win_pct:5.1f}  PF={pf:6.2f}  "
          f"total={total:+9.2f}  avg/trade={avg:+7.2f}")
    return {"label": label, "trades": trades, "total": total, "pf": pf, "win_pct": win_pct}


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows + HTF context once (cached)...")
    cache, htf_cache = await fetch_all()

    print("\n" + "=" * 110)
    print("WALL-CASCADE COMPARISON -- baseline vs best single-% vs best 2-param vs wall cascade")
    print("=" * 110)
    results = {}
    for mode, label in [("baseline", "baseline(fixed SL/EOD)"),
                         ("single_pct_2.0", "single-% 2.0%"),
                         ("two_param_0.75_0.15", "2-param act=0.75/trail=0.15"),
                         ("wall_cascade", "wall-cascade (1H->3min)")]:
        results[mode] = summarize(label, run_one(cache, htf_cache, mode))

    print("\n" + "=" * 110)
    print("KEY LOSING-TRADE DETAIL ACROSS ALL 4 -- ATHERENERG/FORCEMOT/HEROMOTOCO/BSE/BOSCHLTD/EICHERMOT/GODREJCP")
    print("=" * 110)
    watch = {"ATHERENERG", "FORCEMOT", "HEROMOTOCO", "BSE", "BOSCHLTD", "EICHERMOT", "GODREJCP"}
    for mode, label in [("baseline", "baseline"), ("wall_cascade", "wall-cascade")]:
        print(f"\n-- {label} --")
        for t in results[mode]["trades"]:
            if t.symbol in watch and t.entry_price is not None:
                print(f"  {t.date} {t.symbol:<12} {t.side:<4} entry={t.entry_price:9.2f}@{t.entry_ts.strftime('%H:%M')} "
                      f"exit={t.exit_price:9.2f}@{t.exit_ts.strftime('%H:%M')} ({t.reason})  pts={t.points:+8.2f}")

    print("\n" + "=" * 110)
    print("FULL PER-TRADE DETAIL -- wall_cascade")
    print("=" * 110)
    for t in results["wall_cascade"]["trades"]:
        if t.entry_price is None:
            print(f"{t.date} {t.symbol:<12} {t.side:<4} NO ENTRY")
        else:
            print(f"{t.date} {t.symbol:<12} {t.side:<4} entry={t.entry_price:9.2f}@{t.entry_ts.strftime('%H:%M')} "
                  f"exit={t.exit_price:9.2f}@{t.exit_ts.strftime('%H:%M')} ({t.reason})  pts={t.points:+8.2f}")


asyncio.run(main())
