"""
scripts/oi_orb_trap_target_htf_sweep.py -- 2026-09-05, direct user spec:
sweep the TARGET mechanic's own trap-zone detection timeframe across
Day / 2h / 1h / 3min, all exiting the same way once the zone is reached --
"jump to LTF zone for exit condition" (fixed 3-min S&R S1/R1 breach).

Mechanic (unchanged from the explanation the user already confirmed):
  - LONG (CALL): watch for a BULL TRAP (fake breakout up that reverses
    down -- screener.bull_trap_zones, same real detector already used
    elsewhere in this codebase as an OI-ORB CALL-side ENTRY signal,
    re-purposed here as an exit warning) forming at the swept HTF.
  - SHORT (PUT): watch for a BEAR TRAP (screener.sharp_bear_zones) at the
    swept HTF.
  - The moment price touches an ALREADY-LOCKED trap zone of the opposite
    type, jump to the fixed LTF (3-min) and start a FRESH
    SupportResistanceCalculator from that instant. If/when that ladder's
    S1 (CALL) / R1 (PUT) becomes established and is then breached, close
    the trade -- "trap_target_hit".
  - If no zone ever forms/gets touched, the position just runs to EOD
    (unchanged fallback).
  - SL is UNCHANGED from the last round: the previous-day candle SL
    (green->prev close / red->prev open, mirrored for PUT), checked on an
    intrabar touch, still the hard risk control running in parallel.
    Whichever fires first between SL and trap-target ends the trade.
  - Entry timing, one-reentry-after-SL, and discard-if-already-through-SL
    are all unchanged from the last two rounds.

HTF zone-detection timeframe swept: {"1D": daily bars, "2h": 120-min,
"1h": 60-min, "3min": 3-min}. LTF (the exit-confirmation S&R) is fixed at
3-min for every variant (when HTF is itself 3min, HTF and LTF collapse to
the same chart -- the zone forms and breaks on one timeframe, which is a
legitimate 5th data point in the sweep, not an error).

Data note (honestly flagged): the 1D/2h/1h variants need REAL multi-day
history to have any daily/2h/1h bars to detect a setup on at all -- a
single trading day's own bars can't produce more than one daily candle.
Each (date, symbol) pair fetches its own trailing ~12 calendar days of
real Upstox 1-min history (resampled up into whichever HTF is being
tested) ON TOP OF the trade day itself. At only ~8-9 real trading days of
lookback, the 1D variant in particular has a thin sample for zone
detection -- flagged in the report, not swept further back this pass
(fetch volume already ~9x this script's own baseline).

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_trap_target_htf_sweep.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_range_1m
from strategies.core.support_resistance import SupportResistanceCalculator
from strategies.liquidity_trap.detector import Bar
from scripts.oi_orb_entry_mode_backtest import (
    ROWS, SIDE, ORB_START, ORB_END, ENTRY_WINDOW_END, _key_range,
    resolve_eq_key, to_bars, volume_by_ts, compute_orb,
)
from scripts.oi_orb_prevday_sl_backtest import fetch_prev_day_open_close, sl_level_for
from strategies.oi_orb_screener import screener

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
LOOKBACK_DAYS = 12
LTF_MIN = 3

HTF_OPTIONS = [("1D", None), ("2h", 120), ("1h", 60), ("3min", 3)]


def to_n_min_bars_dated(bars_1m: List[Bar], n: int) -> List[Bar]:
    """Same shape as oi_orb_entry_mode_backtest.to_n_min_bars but groups by
    (date, bucket) so multi-day series don't merge e.g. two different days'
    09:15 bars into one candle."""
    buckets: Dict[tuple, list] = {}
    for b in bars_1m:
        minute_of_day = b.ts.hour * 60 + b.ts.minute
        bucket = (minute_of_day // n) * n
        key = (b.ts.date(), bucket)
        buckets.setdefault(key, []).append(b)
    out = []
    for key in sorted(buckets.keys()):
        g = sorted(buckets[key], key=lambda x: x.ts)
        out.append(Bar(ts=g[0].ts, open=g[0].open, high=max(x.high for x in g),
                        low=min(x.low for x in g), close=g[-1].close))
    return out


def to_daily_bars(bars_1m: List[Bar]) -> List[Bar]:
    by_date: Dict[date, list] = defaultdict(list)
    for b in bars_1m:
        by_date[b.ts.date()].append(b)
    out = []
    for d in sorted(by_date.keys()):
        g = sorted(by_date[d], key=lambda x: x.ts)
        out.append(Bar(ts=g[0].ts, open=g[0].open, high=max(x.high for x in g),
                        low=min(x.low for x in g), close=g[-1].close))
    return out


async def fetch_multiday(eq_key: str, trade_date: date) -> tuple:
    start = trade_date - timedelta(days=LOOKBACK_DAYS)
    rows = await fetch_upstox_range_1m(eq_key, TOKEN, start, trade_date)
    return rows, to_bars(rows)


@dataclass
class Trade:
    date: str
    symbol: str
    side: str
    n: int
    entry_ts: object
    entry_price: Optional[float]
    exit_ts: object
    exit_price: Optional[float]
    reason: str

    @property
    def points(self) -> Optional[float]:
        if self.entry_price is None or self.exit_price is None:
            return None
        raw = self.exit_price - self.entry_price
        return raw if self.side == "CALL" else -raw


def trap_target_exit(entry_ts, entry_price, side, sl_level, bars_1m_multiday, htf_bars):
    """SL (unchanged, prev-day level) + trap-zone-then-LTF-S&R target."""
    zones_fn = screener.bull_trap_zones if side == "CALL" else screener.sharp_bear_zones
    zones = zones_fn(htf_bars)

    ltf_bars = to_n_min_bars_dated(bars_1m_multiday, LTF_MIN)
    post_entry_1m = [b for b in bars_1m_multiday if b.ts >= entry_ts]

    zone_touched_ts = None
    calc = None
    ltf_fed = 0

    for b in post_entry_1m:
        sl_breach = (b.low <= sl_level) if side == "CALL" else (b.high >= sl_level)
        if sl_breach:
            return b.ts, sl_level, "prev_day_sl"

        if zone_touched_ts is None:
            for z in zones:
                if z["lock_ts"] is None or z["lock_ts"] > b.ts:
                    continue   # not locked yet as of this bar -- no lookahead
                touched = (b.low <= z["zone_hi"]) and (b.high >= z["zone_lo"])
                if touched:
                    zone_touched_ts = b.ts
                    calc = SupportResistanceCalculator()
                    ltf_fed = 0
                    break

        if calc is not None:
            avail_ltf = [x for x in ltf_bars if entry_ts <= x.ts <= b.ts]
            for nb in avail_ltf[ltf_fed:]:
                calc.process_straddle_candle("SYM", {"timestamp": nb.ts, "high": nb.high,
                                                       "low": nb.low, "duration": LTF_MIN})
            ltf_fed = len(avail_ltf)
            sr = calc.get_calculated_sr_state("SYM").get("sr_levels", {})
            level = sr.get("S1") if side == "CALL" else sr.get("R1")
            if level is not None and level.get("is_established"):
                lvl = level["low"] if side == "CALL" else level["high"]
                breach = (b.low <= lvl) if side == "CALL" else (b.high >= lvl)
                if breach:
                    return b.ts, lvl, "trap_target_hit"

    if post_entry_1m:
        last = post_entry_1m[-1]
        return last.ts, last.close, "eod_close"
    return entry_ts, entry_price, "no_data_after_entry"


def run_strategy(bars_1m, side, orb_h, orb_l, vol_by_ts, sl_level, bars_1m_multiday, htf_bars):
    orb_bars = _key_range(bars_1m, ORB_START, ORB_END)
    vwap_state = screener.VwapState()
    armed = False
    historically_fulfilled = False
    for b in orb_bars:
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        vwap = vwap_state.current("SYM")
        if vwap is None:
            continue
        armed, fire = screener.check_vwap_retest_entry(side, b.close, vwap, armed, 0.15)
        if fire:
            historically_fulfilled = True
            break

    entry_window = _key_range(bars_1m, ORB_END, ENTRY_WINDOW_END)
    if not entry_window:
        return []

    def entry_valid(price):
        return (price > sl_level) if side == "CALL" else (price < sl_level)

    trades: List[tuple] = []
    in_position_until = None
    day_done = False
    breached = False

    if historically_fulfilled:
        b0 = entry_window[0]
        if entry_valid(b0.close):
            exit_ts, exit_price, reason = trap_target_exit(
                b0.ts, b0.close, side, sl_level, bars_1m_multiday, htf_bars)
            trades.append((b0.ts, b0.close, exit_ts, exit_price, "immediate_historical:" + reason))
            in_position_until = exit_ts
            if reason == "no_data_after_entry" or reason != "prev_day_sl":
                day_done = True

    idx = 0
    while idx < len(entry_window) and not day_done and len(trades) < 2:
        b = entry_window[idx]
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        vwap = vwap_state.current("SYM")

        if side == "CALL" and b.low <= sl_level:
            breached = True
        elif side == "PUT" and b.high >= sl_level:
            breached = True

        if in_position_until is not None and b.ts <= in_position_until:
            if vwap is not None:
                armed, _fire = screener.check_vwap_retest_entry(side, b.close, vwap, armed, 0.15)
            idx += 1
            continue

        if vwap is None:
            idx += 1
            continue
        armed, fire = screener.check_vwap_retest_entry(side, b.close, vwap, armed, 0.15)
        if not fire:
            idx += 1
            continue
        armed = False
        if breached or not entry_valid(b.close):
            idx += 1
            continue

        entry_ts, entry_price = b.ts, b.close
        exit_ts, exit_price, reason = trap_target_exit(
            entry_ts, entry_price, side, sl_level, bars_1m_multiday, htf_bars)
        trades.append((entry_ts, entry_price, exit_ts, exit_price, reason))
        in_position_until = exit_ts
        if reason != "prev_day_sl":
            day_done = True
        idx += 1

    return [(t[0], t[1], t[2], t[3], t[4], i + 1) for i, t in enumerate(trades)]


async def fetch_all():
    cache = {}
    for trade_date, symbol, side_bias in ROWS:
        key = (trade_date, symbol)
        if key in cache:
            continue
        eq_key = resolve_eq_key(symbol)
        if eq_key is None:
            cache[key] = None
            continue
        d = date.fromisoformat(trade_date)
        raw_rows, bars_1m_multiday = await fetch_multiday(eq_key, d)
        if not bars_1m_multiday:
            cache[key] = None
            continue
        bars_1m = [b for b in bars_1m_multiday if b.ts.date() == d]
        if not bars_1m:
            cache[key] = None
            continue
        rows_today = [r for r in raw_rows if str(r["ts"])[:10] == d.isoformat()]
        vol_by_ts = volume_by_ts(rows_today)
        orb = compute_orb(bars_1m)
        prevday = await fetch_prev_day_open_close(eq_key, d)
        if orb is None or prevday is None:
            cache[key] = None
            continue
        orb_h, orb_l = orb
        cache[key] = (bars_1m, vol_by_ts, orb_h, orb_l, prevday[0], prevday[1], bars_1m_multiday)
    print(f"Fetched {sum(1 for v in cache.values() if v)} usable rows.")
    return cache


def build_htf_bars(bars_1m_multiday, htf_label, bucket_min):
    if htf_label == "1D":
        return to_daily_bars(bars_1m_multiday)
    return to_n_min_bars_dated(bars_1m_multiday, bucket_min)


def run_all(cache, htf_label, bucket_min):
    trades = []
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l, prev_open, prev_close, bars_1m_multiday = cached
        sl_level = sl_level_for(side, prev_open, prev_close)
        htf_bars = build_htf_bars(bars_1m_multiday, htf_label, bucket_min)
        for (entry_ts, entry_price, exit_ts, exit_price, reason, n) in run_strategy(
                bars_1m, side, orb_h, orb_l, vol_by_ts, sl_level, bars_1m_multiday, htf_bars):
            trades.append(Trade(trade_date, symbol, side, n, entry_ts, entry_price, exit_ts, exit_price, reason))
    return trades


def summarize(label, trades):
    entered = [t for t in trades if t.entry_price is not None]
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    target_hits = sum(1 for t in entered if "trap_target_hit" in t.reason)
    print(f"{label:>8}  entered={len(entered):2d}  win%={win_pct:5.1f}  PF={pf:6.2f}  "
          f"total={total:+9.2f}  target_hits={target_hits}")
    return {"label": label, "trades": trades, "total": total, "pf": pf, "win_pct": win_pct,
            "entered": len(entered), "target_hits": target_hits}


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print(f"Fetching all rows + {LOOKBACK_DAYS}-day multiday history + prev-day OHLC "
          f"(real Upstox 1-min NSE_EQ history)... this will take a while.")
    cache = await fetch_all()

    print("\n" + "=" * 100)
    print("HTF TRAP-ZONE SWEEP FOR TARGET (LTF exit fixed at 3-min S&R) -- summary")
    print("=" * 100)
    results = {}
    for htf_label, bucket_min in HTF_OPTIONS:
        trades = run_all(cache, htf_label, bucket_min)
        results[htf_label] = summarize(htf_label, trades)

    print("\n" + "=" * 100)
    print("TRADE-LEVEL DETAIL PER HTF CONFIG")
    print("=" * 100)
    for htf_label, _ in HTF_OPTIONS:
        r = results[htf_label]
        print(f"\n-- HTF={htf_label} --")
        for t in sorted(r["trades"], key=lambda x: (x.date, x.symbol, x.entry_ts or 0)):
            if t.entry_price is None:
                continue
            print(f"  {t.date} {t.symbol:<12} {t.side:<4} #{t.n} entry={t.entry_price:9.2f}@{t.entry_ts.strftime('%H:%M')} "
                  f"exit={t.exit_price:9.2f}@{t.exit_ts.strftime('%H:%M')} ({t.reason})  pts={t.points:+8.2f}")


asyncio.run(main())
