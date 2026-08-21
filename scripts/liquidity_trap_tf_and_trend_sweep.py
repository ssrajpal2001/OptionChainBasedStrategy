"""
scripts/liquidity_trap_tf_and_trend_sweep.py -- optimization pass on the
"multi-ref, skip-if-blocked" Liquidity Trap variant (2026-08-21), against the
same real cached SENSEX 1-min spot data used by liquidity_trap_multiref_
backtest.py. User asked to optimize timeframe + explore concept-level
filters (HTF trend, VWAP, change-in-OI, max pain, open interest) aiming for
FEWER trades with HIGHER win%/PF.

VWAP / change-in-OI / max-pain / open-interest are confirmed IMPOSSIBLE to
backtest with this data source -- direct inspection of the cached 91,562-row
file shows volume=0 and oi=0 on every single row (Upstox's historical index-
candle API doesn't carry either field for spot indices). Same root
limitation OI-Flow already hit and documented. Only forward/live testing
could validate those; not attempted here.

What IS tested, both fully computable from real spot price alone:
  1. Timeframe sweep -- vary the ref-candle timeframe (baseline 15m) and the
     Stage-3 confirmation timeframe (baseline 5m) across a small grid.
  2. Higher-timeframe trend filter -- only take a multi-ref trade if its
     direction agrees with a slower/coarser trend read (a genuinely
     spot-price-only concept, same family as D1Trap's HTF zone bias and
     Liquidity Sweep's BoS/CHoCH structure bias, reimplemented fresh here
     rather than imported, matching this codebase's per-script isolation
     convention for backtest tooling). Trend = whether the current close is
     above/below a simple N-period SMA of the closes on a coarser bar
     series (e.g. 75m) -- the same "slower timeframe = context, faster
     timeframe = trigger" idea every other strategy in this codebase
     already uses, just applied as a pure filter here.

Base variant = multi-ref, skip-if-blocked (not the flip variant) -- it
already tested with the higher PF of the two multi-ref options (1.81 vs
1.67 lot-weighted), so it's the more sensible base to refine further.
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

CACHE_PATH = "scratch_liquidity_trap_1m_cache.json"


def resample(bars_1m: List[Bar], tf_min: int) -> List[Bar]:
    """Minutes-since-midnight bucketing (not just b.ts.minute) -- required
    for tf_min >= 60: b.ts.minute is always 0-59, so `minute // tf_min` is
    always 0 for any tf_min >= 60, silently collapsing 60m/75m/120m etc into
    plain hourly buckets. Bug found live in this sweep (60/75/120m trend
    reads all came out byte-identical) before this fix."""
    out: List[Bar] = []
    cur_key = None
    cur: Optional[Bar] = None
    for b in bars_1m:
        minutes_since_midnight = b.ts.hour * 60 + b.ts.minute
        bucket_start = (minutes_since_midnight // tf_min) * tf_min
        bucket_hour, bucket_minute = divmod(bucket_start, 60)
        key = (b.ts.date(), bucket_start)
        if key != cur_key:
            if cur is not None:
                out.append(cur)
            cur_key = key
            cur = Bar(ts=b.ts.replace(hour=bucket_hour, minute=bucket_minute, second=0, microsecond=0),
                      open=b.open, high=b.high, low=b.low, close=b.close)
        else:
            cur.high = max(cur.high, b.high)
            cur.low = min(cur.low, b.low)
            cur.close = b.close
    if cur is not None:
        out.append(cur)
    return out


def load_1m_bars_cached() -> List[Bar]:
    import json, os
    if not os.path.exists(CACHE_PATH):
        print(f"No cache at {CACHE_PATH} -- run liquidity_trap_multiref_backtest.py "
              f"first (with a token) to build it.")
        return []
    with open(CACHE_PATH, "r", encoding="utf-8") as f:
        rows = json.load(f)
    bars = [Bar(ts=datetime.fromisoformat(r["ts"]), open=r["open"], high=r["high"],
                low=r["low"], close=r["close"]) for r in rows]
    bars.sort(key=lambda b: b.ts)
    return bars


def _trend_sma(bars_coarse_all: List[Bar], sma_len: int) -> Dict[datetime, str]:
    """Simple SMA-slope trend read on a coarser bar series -- 'UP'/'DOWN' per
    bar close time, keyed by that bar's own ts, len(bars) < sma_len -> no
    entry (None, never guessed)."""
    out: Dict[datetime, Optional[str]] = {}
    closes = [b.close for b in bars_coarse_all]
    for i, b in enumerate(bars_coarse_all):
        if i + 1 < sma_len:
            out[b.ts] = None
            continue
        sma = sum(closes[i + 1 - sma_len:i + 1]) / sma_len
        out[b.ts] = "UP" if b.close > sma else "DOWN"
    return out


def _trend_as_of(trend_by_ts: Dict[datetime, Optional[str]], coarse_bars: List[Bar],
                  ts: datetime) -> Optional[str]:
    """Latest coarse-bar trend read strictly before `ts` (no lookahead)."""
    latest = None
    for b in coarse_bars:
        if b.ts >= ts:
            break
        v = trend_by_ts.get(b.ts)
        if v is not None:
            latest = v
    return latest


def run_day_multiref_filtered(day: date, bars_1m_day: List[Bar], bars_1m_all: List[Bar],
                               bars_ref_all: List[Bar], bars_confirm_all: List[Bar],
                               swings_ref_all: List, target_mode: str,
                               trend_by_ts: Optional[Dict] = None,
                               trend_bars: Optional[List[Bar]] = None) -> List[Trade]:
    """Same skip-if-blocked sequencing as liquidity_trap_multiref_backtest.
    run_day_multiref, generalized to configurable ref/confirm timeframes and
    an optional HTF-trend filter applied at candidate-generation time (a
    candidate is dropped entirely if its direction disagrees with the trend
    read as of its own entry_ts -- no lookahead)."""
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
                continue   # not enough HTF history yet -- skip, never guess
            if (trend == "UP" and t.direction != "BULL") or (trend == "DOWN" and t.direction != "BEAR"):
                continue   # against the higher-TF trend -- filtered out
        candidates.append(t)
    candidates.sort(key=lambda t: t.entry_ts)

    day_1m = [b for b in bars_1m_all if b.ts.date() == day]
    accepted: List[Trade] = []
    position_open_until: Optional[datetime] = None
    for t in candidates:
        if position_open_until is not None and t.entry_ts < position_open_until:
            continue
        # inline simulate_exit (skip-if-blocked variant has no flip trigger)
        after = [b for b in day_1m if b.ts > t.entry_ts]
        zone_bars: List[Bar] = []
        added = False
        for b in after:
            if not added:
                zone_bars.append(b)
                if len(zone_bars) >= 3 and _check_scale_in(t, zone_bars, b):
                    added = True
                    t.lots_final = 4
            if b.ts.time() >= datetime(2000, 1, 1, 15, 15).time():
                t.exit_ts, t.exit_price, t.exit_reason = b.ts, b.close, "EOD"
                break
            if t.direction == "BULL":
                if b.low <= t.sl:
                    t.exit_ts, t.exit_price, t.exit_reason = b.ts, t.sl, "SL"
                    break
                if b.high >= t.target:
                    t.exit_ts, t.exit_price, t.exit_reason = b.ts, t.target, "TARGET"
                    break
            else:
                if b.high >= t.sl:
                    t.exit_ts, t.exit_price, t.exit_reason = b.ts, t.sl, "SL"
                    break
                if b.low <= t.target:
                    t.exit_ts, t.exit_price, t.exit_reason = b.ts, t.target, "TARGET"
                    break
        if t.exit_ts is None:
            if after:
                last = after[-1]
                t.exit_ts, t.exit_price, t.exit_reason = last.ts, last.close, "EOD(data-end)"
            else:
                t.exit_ts, t.exit_price, t.exit_reason = t.entry_ts, t.entry_price, "EOD(no-data)"
        t.pnl_pts = ((t.exit_price - t.entry_price) if t.direction == "BULL"
                     else (t.entry_price - t.exit_price))
        per_pt_first_2 = t.pnl_pts
        if t.add_on_price is not None:
            per_pt_add_2 = ((t.exit_price - t.add_on_price) if t.direction == "BULL"
                            else (t.add_on_price - t.exit_price))
            t.pnl_lotpts = 2 * per_pt_first_2 + 2 * per_pt_add_2
        else:
            t.pnl_lotpts = 2 * per_pt_first_2
        accepted.append(t)
        position_open_until = t.exit_ts
    return accepted


def _summarize(trades: List[Trade]) -> dict:
    n = len(trades)
    wins = sum(1 for t in trades if t.pnl_pts > 0)
    win_pct = (wins / n * 100.0) if n else 0.0
    gross_win_lot = sum(t.pnl_lotpts for t in trades if t.pnl_lotpts > 0)
    gross_loss_lot = -sum(t.pnl_lotpts for t in trades if t.pnl_lotpts < 0)
    pf_lot = (gross_win_lot / gross_loss_lot) if gross_loss_lot > 0 else float("inf")
    net_lot = sum(t.pnl_lotpts for t in trades)
    return dict(n=n, win_pct=win_pct, pf_lot=pf_lot, net_rupees=net_lot * 20)


def main():
    bars_1m = load_1m_bars_cached()
    if not bars_1m:
        return
    days_1m = by_day(bars_1m)
    print(f"Real data: {len(days_1m)} trading days, {len(bars_1m)} 1m bars.\n")

    print("=" * 100)
    print("PART 1 -- TIMEFRAME SWEEP (ref TF x confirm TF), multi-ref skip-if-blocked, target_mode=liquidity")
    print("=" * 100)
    header = f"{'REF_TF':>7} {'CONFIRM_TF':>10} {'trades':>7} {'win%':>7} {'PF(lot)':>8} {'net_Rs':>12}"
    print(header)
    print("-" * len(header))
    tf_results = []
    for ref_tf in (10, 15, 20, 30):
        bars_ref = resample(bars_1m, ref_tf)
        swings_ref = swing_points(bars_ref, pivot=2)
        for confirm_tf in (3, 5, 10):
            if confirm_tf >= ref_tf:
                continue
            bars_confirm = resample(bars_1m, confirm_tf)
            trades: List[Trade] = []
            for day in sorted(days_1m.keys()):
                trades.extend(run_day_multiref_filtered(
                    day, days_1m[day], bars_1m, bars_ref, bars_confirm, swings_ref,
                    "liquidity"))
            s = _summarize(trades)
            tf_results.append((ref_tf, confirm_tf, s))
            print(f"{ref_tf:>7} {confirm_tf:>10} {s['n']:>7} {s['win_pct']:>6.1f}% "
                  f"{s['pf_lot']:>8.2f} {s['net_rupees']:>12.2f}")

    print("\n" + "=" * 100)
    print("PART 2 -- HTF TREND FILTER (baseline ref=15m/confirm=5m, only trade WITH the trend)")
    print("=" * 100)
    bars_ref_15 = resample(bars_1m, 15)
    bars_confirm_5 = resample(bars_1m, 5)
    swings_ref_15 = swing_points(bars_ref_15, pivot=2)
    header2 = f"{'TREND_TF':>9} {'SMA_LEN':>8} {'trades':>7} {'win%':>7} {'PF(lot)':>8} {'net_Rs':>12}"
    print(header2)
    print("-" * len(header2))
    for trend_tf in (60, 75, 120):
        bars_trend = resample(bars_1m, trend_tf)
        for sma_len in (10, 20):
            trend_by_ts = _trend_sma(bars_trend, sma_len)
            trades = []
            for day in sorted(days_1m.keys()):
                trades.extend(run_day_multiref_filtered(
                    day, days_1m[day], bars_1m, bars_ref_15, bars_confirm_5, swings_ref_15,
                    "liquidity", trend_by_ts=trend_by_ts, trend_bars=bars_trend))
            s = _summarize(trades)
            print(f"{trend_tf:>9} {sma_len:>8} {s['n']:>7} {s['win_pct']:>6.1f}% "
                  f"{s['pf_lot']:>8.2f} {s['net_rupees']:>12.2f}")

    print("\n(For reference, no-filter baseline ref=15m/confirm=5m: n=930, win%=77.8, PF(lot)=1.81, net=Rs706,493)")

    print("\n" + "=" * 100)
    print("PART 3 -- COMBINED: 60m/SMA10 trend filter stacked on top of the best timeframe combos")
    print("=" * 100)
    header3 = f"{'REF_TF':>7} {'CONFIRM_TF':>10} {'trades':>7} {'win%':>7} {'PF(lot)':>8} {'net_Rs':>12}"
    print(header3)
    print("-" * len(header3))
    bars_trend_60 = resample(bars_1m, 60)
    trend_by_ts_60_10 = _trend_sma(bars_trend_60, 10)
    for ref_tf, confirm_tf in ((15, 3), (15, 5), (20, 3), (30, 3), (30, 5)):
        bars_ref = resample(bars_1m, ref_tf)
        swings_ref = swing_points(bars_ref, pivot=2)
        bars_confirm = resample(bars_1m, confirm_tf)
        trades = []
        for day in sorted(days_1m.keys()):
            trades.extend(run_day_multiref_filtered(
                day, days_1m[day], bars_1m, bars_ref, bars_confirm, swings_ref,
                "liquidity", trend_by_ts=trend_by_ts_60_10, trend_bars=bars_trend_60))
        s = _summarize(trades)
        print(f"{ref_tf:>7} {confirm_tf:>10} {s['n']:>7} {s['win_pct']:>6.1f}% "
              f"{s['pf_lot']:>8.2f} {s['net_rupees']:>12.2f}")


if __name__ == "__main__":
    main()
