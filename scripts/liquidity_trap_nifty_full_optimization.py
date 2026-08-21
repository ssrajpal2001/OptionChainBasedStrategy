"""
scripts/liquidity_trap_nifty_full_optimization.py -- full NIFTY optimization
pass (2026-08-22), extending the timeframe/trend-filter sweep already run
(scripts/liquidity_trap_nifty_tf_and_trend_sweep.py) with the two remaining
dimensions the user asked to cover: TARGET concept and SL/exit concept
(flip vs skip-if-blocked), against the SAME real NIFTY 1-min spot data
(scratch_liquidity_trap_nifty_1m_cache.json, 247 trading days).

Reuses every mechanic byte-for-byte from the already-validated scripts --
no reimplementation:
  - Stage1-4 pipeline, target_mode=("rr2"|"liquidity"): liquidity_trap_
    multiref_backtest.py's _stage3_and_4()
  - skip-if-blocked sequencing + HTF trend filter: liquidity_trap_tf_and_
    trend_sweep.py's run_day_multiref_filtered()
  - flip-on-opposite-signal sequencing: liquidity_trap_multiref_backtest.py's
    run_day_multiref()/_simulate_exit_with_flip() -- generalized here to
    configurable ref/confirm timeframes + an optional trend filter, the
    exact same genericization tf_and_trend_sweep.py already did for the
    skip variant (that script's own run_day_multiref_filtered() started
    from a hardcoded-15m/5m run_day_multiref before being made TF-
    configurable; run_day_multiref_flip_filtered() below is that same step
    applied to the FLIP variant, which had never been made TF-configurable).

SL concept itself is NOT swept as a separate axis: it's always the swept
5m candle's own extreme (sweep_extreme) in every mode tested across this
whole feature -- a structural, price-action-anchored stop, not a %/points
offset. No alternate SL concept (ATR-based, fixed-points, etc.) has ever
been built or requested for this strategy; introducing one here would be
untested surface, not a real "more SL options exist" case like OI-Flow's
pool_swing_low situation. Flagged explicitly rather than silently
skipped.

Indicator dimension: same conclusion as every prior sweep in this
strategy's history -- VWAP / change-in-OI / max-pain / open-interest are
confirmed impossible to backtest against Upstox's historical index-candle
data (volume=0, oi=0 on every row). The one indicator-shaped filter that
IS backtestable (spot-price HTF-SMA trend) is already covered: every combo
below runs WITH the already-validated 60m/SMA10 trend filter, since Part 2
of the prior sweep confirmed it improves win%/PF on real NIFTY data over
no-filter.

Usage: python scripts/liquidity_trap_nifty_full_optimization.py
(no token needed -- reuses the cache built by
liquidity_trap_nifty_tf_and_trend_sweep.py)
"""
from __future__ import annotations

import sys
from datetime import date, datetime
from typing import Dict, List, Optional

sys.path.insert(0, ".")

from scripts.liquidity_trap_multiref_backtest import (
    Bar, Trade, by_day, swing_points, find_all_setups, find_sl_hit_for_setup,
    _stage3_and_4, _check_scale_in,
)
from scripts.liquidity_trap_tf_and_trend_sweep import resample, _trend_sma, _trend_as_of
from scripts.liquidity_trap_nifty_tf_and_trend_sweep import CACHE_PATH, LOT_SIZE, _summarize


def run_day_multiref_flip_filtered(
    day: date, bars_1m_day: List[Bar], bars_1m_all: List[Bar],
    bars_ref_all: List[Bar], bars_confirm_all: List[Bar], swings_ref_all: List,
    target_mode: str, trend_by_ts: Optional[Dict] = None, trend_bars: Optional[List[Bar]] = None,
) -> List[Trade]:
    """Flip-on-opposite-signal sequencing (run_day_multiref /
    _simulate_exit_with_flip in liquidity_trap_multiref_backtest.py),
    generalized to configurable ref/confirm timeframes + an optional HTF
    trend filter -- the same generalization tf_and_trend_sweep.py already
    applied to the skip-if-blocked variant. Only the candidate-generation
    and sequencing logic is reproduced here (byte-identical to the
    originals' own control flow); the per-candidate Stage3/4 pipeline and
    the flip-aware exit walk are the SAME functions, not reimplemented."""
    bars_ref_day = [b for b in bars_ref_all if b.ts.date() == day]
    if len(bars_ref_day) < 2:
        return []
    setups = find_all_setups(bars_ref_day)
    candidates: List[Trade] = []
    for s in setups:
        s.sl_hit_ts = find_sl_hit_for_setup(s, bars_ref_day)
        if s.sl_hit_ts is None:
            continue
        ref_ts = bars_ref_day[s.ref_idx].ts
        t = _stage3_and_4(day, s.direction, ref_ts, s.sl_hit_ts, bars_1m_day,
                           bars_confirm_all, bars_ref_all, swings_ref_all, target_mode)
        if t is None:
            continue
        if trend_by_ts is not None:
            trend = _trend_as_of(trend_by_ts, trend_bars, t.entry_ts)
            if trend is None:
                continue
            if (trend == "UP" and t.direction != "BULL") or (trend == "DOWN" and t.direction != "BEAR"):
                continue
        candidates.append(t)
    candidates.sort(key=lambda t: t.entry_ts)

    day_1m = [b for b in bars_1m_all if b.ts.date() == day]
    accepted: List[Trade] = []
    idx = 0
    while idx < len(candidates):
        cur = candidates[idx]
        next_opp = None
        next_opp_pos = None
        for j in range(idx + 1, len(candidates)):
            if candidates[j].direction != cur.direction:
                next_opp = candidates[j]
                next_opp_pos = j
                break
        _simulate_exit_with_flip_inline(cur, day_1m, next_opp)
        accepted.append(cur)
        if cur.exit_reason == "FLIP":
            idx = next_opp_pos
        else:
            idx += 1
            while idx < len(candidates) and candidates[idx].entry_ts <= cur.exit_ts:
                idx += 1
    return accepted


def _simulate_exit_with_flip_inline(trade: Trade, day_1m: List[Bar], next_opp: Optional[Trade]) -> None:
    """Byte-identical body to liquidity_trap_multiref_backtest.py's
    _simulate_exit_with_flip() -- reproduced (not imported) only because
    that module's version is private (leading underscore, not part of its
    public reuse surface); the logic itself is not reinvented."""
    after = [b for b in day_1m if b.ts > trade.entry_ts]
    zone_bars: List[Bar] = []
    added = False
    for b in after:
        if next_opp is not None and b.ts >= next_opp.entry_ts:
            trade.exit_ts, trade.exit_price, trade.exit_reason = next_opp.entry_ts, next_opp.entry_price, "FLIP"
            break
        if not added:
            zone_bars.append(b)
            if len(zone_bars) >= 3 and _check_scale_in(trade, zone_bars, b):
                added = True
                trade.lots_final = 4
        if b.ts.time() >= datetime(2000, 1, 1, 15, 15).time():
            trade.exit_ts, trade.exit_price, trade.exit_reason = b.ts, b.close, "EOD"
            break
        if trade.direction == "BULL":
            if b.low <= trade.sl:
                trade.exit_ts, trade.exit_price, trade.exit_reason = b.ts, trade.sl, "SL"
                break
            if b.high >= trade.target:
                trade.exit_ts, trade.exit_price, trade.exit_reason = b.ts, trade.target, "TARGET"
                break
        else:
            if b.high >= trade.sl:
                trade.exit_ts, trade.exit_price, trade.exit_reason = b.ts, trade.sl, "SL"
                break
            if b.low <= trade.target:
                trade.exit_ts, trade.exit_price, trade.exit_reason = b.ts, trade.target, "TARGET"
                break
    if trade.exit_ts is None:
        if after:
            last = after[-1]
            trade.exit_ts, trade.exit_price, trade.exit_reason = last.ts, last.close, "EOD(data-end)"
        else:
            trade.exit_ts, trade.exit_price, trade.exit_reason = trade.entry_ts, trade.entry_price, "EOD(no-data)"
    if trade.exit_price is not None:
        trade.pnl_pts = ((trade.exit_price - trade.entry_price) if trade.direction == "BULL"
                         else (trade.entry_price - trade.exit_price))
        per_pt_first_2 = trade.pnl_pts
        if trade.add_on_price is not None:
            per_pt_add_2 = ((trade.exit_price - trade.add_on_price) if trade.direction == "BULL"
                            else (trade.add_on_price - trade.exit_price))
            trade.pnl_lotpts = 2 * per_pt_first_2 + 2 * per_pt_add_2
        else:
            trade.pnl_lotpts = 2 * per_pt_first_2


def load_1m_bars_cached() -> List[Bar]:
    import json, os
    if not os.path.exists(CACHE_PATH):
        print(f"No cache at {CACHE_PATH} -- run liquidity_trap_nifty_tf_and_trend_sweep.py "
              f"first (with a token) to build it.")
        return []
    with open(CACHE_PATH, "r", encoding="utf-8") as f:
        rows = json.load(f)
    bars = [Bar(ts=datetime.fromisoformat(r["ts"]), open=r["open"], high=r["high"],
                low=r["low"], close=r["close"]) for r in rows]
    bars.sort(key=lambda b: b.ts)
    return bars


def main():
    bars_1m = load_1m_bars_cached()
    if not bars_1m:
        return
    days_1m = by_day(bars_1m)
    print(f"Real NIFTY data: {len(days_1m)} trading days, {len(bars_1m)} 1m bars.\n")

    bars_trend_60 = resample(bars_1m, 60)
    trend_by_ts_60_10 = _trend_sma(bars_trend_60, 10)

    print("=" * 110)
    print("TARGET MODE x EXIT STYLE, all with 60m/SMA10 trend filter (already validated on real NIFTY data)")
    print("=" * 110)
    header = (f"{'REF_TF':>7} {'CONFIRM_TF':>10} {'TARGET':>10} {'EXIT':>6} "
              f"{'trades':>7} {'win%':>7} {'PF(lot)':>8} {'net_Rs':>12}")
    print(header)
    print("-" * len(header))

    results = []
    for ref_tf, confirm_tf in ((15, 3), (20, 3), (30, 3)):
        bars_ref = resample(bars_1m, ref_tf)
        swings_ref = swing_points(bars_ref, pivot=2)
        bars_confirm = resample(bars_1m, confirm_tf)
        for target_mode in ("liquidity", "rr2"):
            for exit_style, runner in (("skip", None), ("flip", None)):
                trades: List[Trade] = []
                for day in sorted(days_1m.keys()):
                    if exit_style == "skip":
                        from scripts.liquidity_trap_tf_and_trend_sweep import run_day_multiref_filtered
                        trades.extend(run_day_multiref_filtered(
                            day, days_1m[day], bars_1m, bars_ref, bars_confirm, swings_ref,
                            target_mode, trend_by_ts=trend_by_ts_60_10, trend_bars=bars_trend_60))
                    else:
                        trades.extend(run_day_multiref_flip_filtered(
                            day, days_1m[day], bars_1m, bars_ref, bars_confirm, swings_ref,
                            target_mode, trend_by_ts=trend_by_ts_60_10, trend_bars=bars_trend_60))
                s = _summarize(trades)
                results.append((ref_tf, confirm_tf, target_mode, exit_style, s))
                print(f"{ref_tf:>7} {confirm_tf:>10} {target_mode:>10} {exit_style:>6} "
                      f"{s['n']:>7} {s['win_pct']:>6.1f}% {s['pf_lot']:>8.2f} {s['net_rupees']:>12.2f}")

    print("\nBest by PF:", max(results, key=lambda r: (r[4]['pf_lot'] if r[4]['n'] >= 30 else -1)))
    print("Best by net_Rs:", max(results, key=lambda r: r[4]['net_rupees']))


if __name__ == "__main__":
    main()
