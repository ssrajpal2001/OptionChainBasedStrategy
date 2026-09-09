"""
scripts/oi_orb_bear_trap_htf_ltf_sweep.py -- 2026-09-06, direct user follow-up:
"go ahead and provide your best optimised result, full power, take as much
time as needed." Sweeps the bear-trap-on-opposite-strike concept
(scripts/oi_orb_bear_trap_target_backtest.py) across its two real tunables --
HTF for zone detection (1D/4H/2H/1H) and LTF for the post-trigger exit
(1min/3min/5min) -- against the SAME cached opposite-strike premium data,
fetched ONCE per trade (the expensive part), reused across all 12 combos
(cheap, in-memory recomputation).

Zone lookback widened to 20 calendar days (~14 trading days) vs the first
pass's 6 days, specifically so the 1D HTF has a fair chance to accumulate
enough daily bars to ever produce a confirmed 3-candle zone at all (6 days
was only really adequate for 1H/2H/4H).

Same mechanic as the first pass, unchanged:
  - Bear zone: ref candle -> sellers_in (breaks ref.low) -> confirmed once a
    LATER candle's high reclaims ref.high. Zone = [sellers_in.low, ref.close].
  - Trigger: a 1-min wick on the OPPOSITE strike touches an already-confirmed
    zone (walk-forward, no lookahead).
  - Escalation: permanently switch from our own 15-min HA+StochRSI to the
    OPPOSITE strike's own LTF HA+StochRSI from the trigger bar onward.
  - No trap fired -> byte-identical to baseline.

Selection criterion for "best": total points is the primary ranking (this
week's whole methodology), but ties/near-ties are broken by which config
best preserves the BOSCHLTD-style flagship save (a big give-back trade
turned into a real capture) without giving back the aggregate gain on the
other 45 trades -- reported explicitly, not just picked silently.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_bear_trap_htf_ltf_sweep.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Optional

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_range_1m
from data_layer.instrument_registry import REGISTRY
from strategies.core.candle_indicators import to_heikin_ashi, to_n_min_bars, compute_stoch_rsi
from scripts.oi_orb_entry_mode_backtest import ROWS, SIDE, resolve_eq_key, to_bars, volume_by_ts, compute_orb
from scripts.oi_orb_atr_chandelier_backtest import fetch_all as fetch_all_spot
from scripts.oi_orb_ha_stochrsi_exit_backtest import ha_stoch_exit
from scripts.oi_orb_trap_target_full_htf_ltf_sweep import find_entry
from scripts.oi_orb_bear_trap_target_backtest import find_all_bear_zones, _pts

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
BASELINE_EXIT_TF_MIN = 15
RSI_PERIOD = 9
STOCH_PERIOD = 9
SMOOTH = 3
ZONE_LOOKBACK_DAYS = 20

HTF_GRID = [("1D", 1440), ("4H", 240), ("2H", 120), ("1H", 60)]
LTF_GRID = [("1min", 1), ("3min", 3), ("5min", 5)]


def _ha_stoch_series(bars_1m, tf_min):
    ha_1m = to_heikin_ashi(bars_1m)
    ha_tf = to_n_min_bars(ha_1m, tf_min)
    k, d = compute_stoch_rsi([b.close for b in ha_tf], RSI_PERIOD, STOCH_PERIOD, SMOOTH)
    return ha_tf, k, d


@dataclass
class OppositeContract:
    upstox_key: str
    strike: int
    opt_type: str
    bars_1m: list


async def resolve_opposite_and_fetch(symbol: str, trade_date: str, entry_strike: int, our_opt_type: str) -> Optional[OppositeContract]:
    opp_type = "PE" if our_opt_type == "CE" else "CE"
    d = date.fromisoformat(trade_date)
    try:
        await asyncio.to_thread(REGISTRY.load_sync, symbol, TOKEN)
    except Exception:
        pass
    exp = REGISTRY.get_active_expiry(symbol, d)
    if exp is None:
        return None
    upstox_key = REGISTRY.get_upstox_key(symbol, exp, entry_strike, opp_type)
    if not upstox_key:
        return None
    start = d - timedelta(days=ZONE_LOOKBACK_DAYS)
    rows = await fetch_upstox_range_1m(upstox_key, TOKEN, start, d)
    if not rows:
        return None
    bars_1m = to_bars(rows)
    if not bars_1m:
        return None
    return OppositeContract(upstox_key=upstox_key, strike=entry_strike, opt_type=opp_type, bars_1m=bars_1m)


class _DBar:
    __slots__ = ("ts", "open", "high", "low", "close")
    def __init__(self, ts, o, h, l, c):
        self.ts, self.open, self.high, self.low, self.close = ts, o, h, l, c


def to_n_min_bars_multiday(bars_1m, n: int):
    """Date-AND-time-aware N-minute bucketing, safe for a multi-day series
    and for n >= 60 -- fixes two real bugs found in strategies/core/
    candle_indicators.to_n_min_bars (which this script was originally
    reusing) when applied to multi-day HTF zone construction:
      1. That function's bucket key is (hour, minute % n) with NO DATE --
         correct for its own documented use case (single intraday
         session), but silently merges the same hour-of-day across
         DIFFERENT CALENDAR DAYS into one bucket when fed multi-day data
         (e.g. every 10:00-11:00 candle across a 20-day window landing in
         one "bucket").
      2. For any n >= 60, `minute % n` is always 0 (minute is 0-59), so
         the bucket silently degenerates to plain hourly regardless of the
         requested n -- confirmed empirically: 1H/2H/4H produced BYTE-
         IDENTICAL results in the first sweep pass, which is only possible
         if the coarser two never actually bucketed any wider than 1H.
    Buckets by (calendar date, minutes-since-midnight // n) instead --
    correct for both multi-day and n>=60 in one fix. n=1440 naturally
    reduces to one bucket per calendar day (the 1D case)."""
    buckets: dict = {}
    for b in bars_1m:
        total_min = b.ts.hour * 60 + b.ts.minute
        floored = (total_min // n) * n
        key = (b.ts.date(), floored)
        buckets.setdefault(key, []).append(b)
    out = []
    for key in sorted(buckets.keys()):
        g = sorted(buckets[key], key=lambda x: x.ts)
        out.append(_DBar(g[0].ts, g[0].open, max(x.high for x in g), min(x.low for x in g), g[-1].close))
    return out


def find_bear_trap_trigger(opp_bars_1m, entry_ts, trade_date: str, htf_min: int):
    d = date.fromisoformat(trade_date)
    today_bars = [b for b in opp_bars_1m if b.ts.date() == d]
    prior_bars = [b for b in opp_bars_1m if b.ts.date() < d]
    if not today_bars:
        return None, None
    candidate_bars = [b for b in today_bars if b.ts >= entry_ts]
    for b in candidate_bars:
        history_so_far = prior_bars + [tb for tb in today_bars if tb.ts < b.ts]
        if len(history_so_far) < 20:
            continue
        htf_bars = to_n_min_bars_multiday(history_so_far, htf_min)
        if len(htf_bars) < 3:
            continue
        zones = find_all_bear_zones(htf_bars)
        active = [z for z in zones if z["confirmed_ts"] < b.ts]
        for z in active:
            if z["zone_lo"] <= b.low <= z["zone_hi"]:
                return b.ts, z
    return None, None


@dataclass
class Trade:
    date: str
    symbol: str
    side: str
    entry_ts: object
    entry_price: Optional[float]
    exit_ts: object
    exit_price: Optional[float]
    reason: str
    fired: bool = False

    @property
    def points(self) -> Optional[float]:
        if self.entry_price is None or self.exit_price is None:
            return None
        return _pts(self.side, self.entry_price, self.exit_price)


def summarize(trades: List[Trade]):
    entered = [t for t in trades if t.entry_price is not None]
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    fired_n = sum(1 for t in entered if t.fired)
    return {"entered": len(entered), "pf": pf, "win_pct": win_pct, "total": total, "fired": fired_n, "trades": trades}


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching spot data for entries (real Upstox 1-min NSE_EQ)...")
    spot_cache = await fetch_all_spot()

    # ---- Baseline (computed once) ----
    baseline_trades: List[Trade] = []
    entries = {}   # (date,symbol) -> (entry_ts, entry_price, side, entry_strike, our_opt_type)
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = spot_cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l = cached
        entry = find_entry(bars_1m, side, orb_h, orb_l, vol_by_ts)
        if entry is None:
            continue
        entry_ts, entry_price = entry
        step = 50 if entry_price < 2000 else (100 if entry_price < 10000 else 500)
        entry_strike = int(round(entry_price / step) * step)
        our_opt_type = "CE" if side == "CALL" else "PE"
        ha_15m, k15, d15 = _ha_stoch_series(bars_1m, BASELINE_EXIT_TF_MIN)
        a_ts, a_px, a_reason = ha_stoch_exit(entry_ts, entry_price, side, bars_1m, ha_15m, k15, d15, inclusive=True)
        baseline_trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price, a_ts, a_px, a_reason))
        entries[(trade_date, symbol)] = (entry_ts, entry_price, side, entry_strike, our_opt_type, bars_1m)

    b_summary = summarize(baseline_trades)
    print(f"\n{'BASELINE':>10}  entered={b_summary['entered']:2d}  win%={b_summary['win_pct']:5.1f}  "
          f"PF={b_summary['pf']:7.2f}  total={b_summary['total']:+9.2f}")

    # ---- Fetch each opposite-strike's data ONCE (expensive part) ----
    print(f"\nFetching opposite-strike premium (once per trade, {ZONE_LOOKBACK_DAYS}d lookback)...")
    opp_cache = {}
    keys = list(entries.keys())
    for i, (trade_date, symbol) in enumerate(keys):
        entry_ts, entry_price, side, entry_strike, our_opt_type, bars_1m = entries[(trade_date, symbol)]
        print(f"  [{i+1}/{len(keys)}] {trade_date} {symbol}...")
        opp = await resolve_opposite_and_fetch(symbol, trade_date, entry_strike, our_opt_type)
        opp_cache[(trade_date, symbol)] = opp

    # ---- Sweep HTF x LTF against the cached data ----
    results = {}
    boschltd_by_combo = {}
    for htf_label, htf_min in HTF_GRID:
        for ltf_label, ltf_min in LTF_GRID:
            combo_key = f"HTF={htf_label}_LTF={ltf_label}"
            combo_trades: List[Trade] = []
            for trade_date, symbol in keys:
                entry_ts, entry_price, side, entry_strike, our_opt_type, bars_1m = entries[(trade_date, symbol)]
                b_trade = next(t for t in baseline_trades if t.date == trade_date and t.symbol == symbol)
                opp = opp_cache.get((trade_date, symbol))
                if opp is None:
                    combo_trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price,
                                               b_trade.exit_ts, b_trade.exit_price, b_trade.reason, fired=False))
                    continue
                wick_ts, zone = find_bear_trap_trigger(opp.bars_1m, entry_ts, trade_date, htf_min)
                if wick_ts is None:
                    combo_trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price,
                                               b_trade.exit_ts, b_trade.exit_price, b_trade.reason, fired=False))
                    continue
                opp_today = [b for b in opp.bars_1m if b.ts.date() == date.fromisoformat(trade_date) and b.ts >= wick_ts]
                if len(opp_today) < 4:
                    combo_trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price,
                                               b_trade.exit_ts, b_trade.exit_price, b_trade.reason, fired=False))
                    continue
                opp_side_for_check = "CALL" if opp.opt_type == "CE" else "PUT"
                ha_ltf, k_ltf, d_ltf = _ha_stoch_series(opp_today, ltf_min)
                ltf_ts, ltf_px, ltf_reason = ha_stoch_exit(wick_ts, opp_today[0].close, opp_side_for_check,
                                                            opp_today, ha_ltf, k_ltf, d_ltf, inclusive=True)
                our_post = [b for b in bars_1m if b.ts >= ltf_ts]
                if our_post:
                    exit_price, exit_ts_final = our_post[0].close, our_post[0].ts
                else:
                    exit_price, exit_ts_final = bars_1m[-1].close, bars_1m[-1].ts
                combo_trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price,
                                           exit_ts_final, exit_price, f"bear_trap_{ltf_reason}", fired=True))
                if symbol == "BOSCHLTD":
                    boschltd_by_combo[combo_key] = combo_trades[-1].points

            results[combo_key] = summarize(combo_trades)

    print("\n" + "=" * 110)
    print(f"SWEEP RESULTS -- {len(HTF_GRID)}x{len(LTF_GRID)} = {len(HTF_GRID)*len(LTF_GRID)} combos "
          f"(baseline total = {b_summary['total']:+.2f})")
    print("=" * 110)
    ranked = sorted(results.items(), key=lambda kv: kv[1]["total"], reverse=True)
    for combo_key, s in ranked:
        boschltd_pts = boschltd_by_combo.get(combo_key)
        beats_baseline = " <-- BEATS BASELINE" if s["total"] > b_summary["total"] else ""
        print(f"  {combo_key:<22}  entered={s['entered']:2d}  fired={s['fired']:2d}  win%={s['win_pct']:5.1f}  "
              f"PF={s['pf']:7.2f}  total={s['total']:+9.2f}  BOSCHLTD={boschltd_pts if boschltd_pts is not None else 'n/a'}{beats_baseline}")

    best_key, best = ranked[0]
    print(f"\nBEST BY TOTAL POINTS: {best_key} -> total={best['total']:+.2f} vs baseline {b_summary['total']:+.2f}")

    import json
    def _ser(trades):
        return [{"date": t.date, "symbol": t.symbol, "side": t.side,
                  "entry_ts": t.entry_ts.strftime("%Y-%m-%d %H:%M"), "entry_price": t.entry_price,
                  "exit_ts": t.exit_ts.strftime("%Y-%m-%d %H:%M"), "exit_price": t.exit_price,
                  "reason": t.reason, "points": t.points, "fired": t.fired}
                 for t in sorted(trades, key=lambda x: (x.date, x.symbol))]

    report = {
        "baseline": {"entered": b_summary["entered"], "pf": b_summary["pf"], "win_pct": b_summary["win_pct"],
                     "total": b_summary["total"], "trades": _ser(baseline_trades)},
        "sweep": {k: {"entered": v["entered"], "pf": v["pf"], "win_pct": v["win_pct"], "total": v["total"],
                      "fired": v["fired"]} for k, v in results.items()},
        "best_combo": best_key,
        "best_trades": _ser(best["trades"]),
    }
    out_path = os.path.join("data", "oi_orb_bear_trap_htf_ltf_sweep_report.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nFull JSON report written to {out_path}")


asyncio.run(main())
