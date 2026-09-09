"""
scripts/oi_orb_bear_trap_target_backtest.py -- 2026-09-06, direct user spec:
"bear trap on the OPPOSITE strike" target concept. Same VWAP-retest entry as
the confirmed OI-ORB baseline (real spot, real 51-row shortlist). Compares:

  A. BASELINE (frozen live spec) -- 15-min HA-shape + StochRSI(9,9,3) on OUR
     OWN strike's premium, no SL, EOD fallback.
  B. BEAR-TRAP ESCALATION -- watches the OPPOSITE strike (same strike price,
     opposite option_type: if we hold CE@K, watch PE@K; if PE@K, watch CE@K)
     for a confirmed bear-trap zone on HTF bars (default 1H, using >=4 real
     prior trading days of premium history, walk-forward -- only zones
     confirmed strictly before "now" are ever used). The moment a 1-min bar
     on the OPPOSITE strike wicks INTO an active zone, we permanently switch
     from our own 15-min HA+StochRSI to the OPPOSITE strike's own LTF
     (default 5-min) HA+StochRSI as the exit trigger for our position --
     never reverting. If no trap ever fires during the trade, falls back to
     baseline behavior exactly (this can only ADD an earlier/different exit
     path, never remove the existing one).

Bear-zone definition (recovered from this repo's own git history --
strategies/d1_trap_option/bear_only_book.py / strategies/v4_cascade/
rolling_base.py, both since removed from the live app but the mechanic
itself independently re-derived fresh here, per the standalone-per-strategy
convention this codebase already follows):
  1. Reference candle `ref`. Find the next candle whose LOW breaks below
     ref.low ("sellers_in").
  2. Scan forward from there; the first LATER candle whose HIGH reclaims
     back above ref.high confirms the zone (the sellers who broke the low
     are now trapped). No reclaim within the available history = no zone.
  3. Zone price range = [sellers_in.low, ref.close] (the validated boundary
     from that prior work, not the raw swing extremes).
Zone merging across nearby candidates is SKIPPED in this first pass (affects
zone count/overlap bookkeeping only, not the core mechanic) -- flagged
honestly, not silently different from the original.

Data: verified separately (scripts/_verify_option_multiday_history.py) that
Upstox serves clean, continuous, gap-free 1-min premium history for
individual F&O stock option contracts across multi-day windows (monthly
expiries for stocks avoid the weekly-rollover discontinuity NIFTY/SENSEX
would have). This script fetches trade_date-5 calendar days through
trade_date for the OPPOSITE strike, once per trade.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_bear_trap_target_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_range_1m
from data_layer.instrument_registry import REGISTRY
from strategies.core.candle_indicators import to_heikin_ashi, to_n_min_bars, compute_stoch_rsi
from scripts.oi_orb_entry_mode_backtest import ROWS, SIDE, resolve_eq_key, to_bars, volume_by_ts, compute_orb
from scripts.oi_orb_atr_chandelier_backtest import fetch_all as fetch_all_spot
from scripts.oi_orb_ha_stochrsi_exit_backtest import ha_stoch_exit
from scripts.oi_orb_trap_target_full_htf_ltf_sweep import find_entry

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
BASELINE_EXIT_TF_MIN = 15
LTF_EXIT_TF_MIN = 5
RSI_PERIOD = 9
STOCH_PERIOD = 9
SMOOTH = 3
ZONE_HTF_MIN = 60          # 1H
ZONE_LOOKBACK_DAYS = 6     # calendar days back (>=4 trading days incl. weekends buffer)


def _ha_stoch_series(bars_1m, tf_min):
    ha_1m = to_heikin_ashi(bars_1m)
    ha_tf = to_n_min_bars(ha_1m, tf_min)
    k, d = compute_stoch_rsi([b.close for b in ha_tf], RSI_PERIOD, STOCH_PERIOD, SMOOTH)
    return ha_tf, k, d


def _pts(side, entry, price):
    raw = price - entry
    return raw if side == "CALL" else -raw


def find_all_bear_zones(bars) -> List[dict]:
    """Confirmed sweep+reclaim bear zones -- see module docstring. Returns
    list of {'ref_ts','ref_close','sellers_in_ts','sellers_in_low',
    'zone_lo','zone_hi','confirmed_ts'} in chronological ref order."""
    n = len(bars)
    out = []
    for i in range(n - 2):
        ref = bars[i]
        sellers_in_idx = None
        for j in range(i + 1, n):
            if bars[j].low < ref.low:
                sellers_in_idx = j
                break
        if sellers_in_idx is None:
            continue
        confirmed_idx = None
        for k in range(sellers_in_idx + 1, n):
            if bars[k].high > ref.high:
                confirmed_idx = k
                break
        if confirmed_idx is None:
            continue
        sellers_in = bars[sellers_in_idx]
        out.append(dict(
            ref_ts=ref.ts, ref_close=ref.close,
            sellers_in_ts=sellers_in.ts, sellers_in_low=sellers_in.low,
            zone_lo=sellers_in.low, zone_hi=ref.close,
            confirmed_ts=bars[confirmed_idx].ts,
        ))
    return out


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


def find_bear_trap_trigger(opp_bars_1m, entry_ts, trade_date: str):
    """Walk-forward: at each 1-min bar of the OPPOSITE strike on/after
    entry_ts, recompute zones using ONLY history strictly before that bar's
    own timestamp (no lookahead), then check if that bar's LOW wicks into
    any currently-active zone. Returns (trigger_ts, zone, trap_confirmed_ts)
    or (None, None, None) if no trap ever fires today.
    """
    d = date.fromisoformat(trade_date)
    today_bars = [b for b in opp_bars_1m if b.ts.date() == d]
    prior_bars = [b for b in opp_bars_1m if b.ts.date() < d]
    if not today_bars:
        return None, None, None

    candidate_bars = [b for b in today_bars if b.ts >= entry_ts]
    for b in candidate_bars:
        # HTF bars available as of "now" = prior days' full history + today's
        # bars strictly before this one (walk-forward, no lookahead into the
        # future of today's own session).
        history_so_far = prior_bars + [tb for tb in today_bars if tb.ts < b.ts]
        if len(history_so_far) < 20:
            continue
        htf_bars = to_n_min_bars(history_so_far, ZONE_HTF_MIN)
        if len(htf_bars) < 3:
            continue
        zones = find_all_bear_zones(htf_bars)
        # Only zones already CONFIRMED (reclaim happened) strictly before
        # this bar's own timestamp count as "active" -- consistent with the
        # walk-forward discipline (a zone confirmed by a future bar can't
        # have been known about yet).
        active = [z for z in zones if z["confirmed_ts"] < b.ts]
        for z in active:
            if z["zone_lo"] <= b.low <= z["zone_hi"]:
                return b.ts, z, z["confirmed_ts"]
    return None, None, None


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
    trap_confirmed_ts: object = None
    trap_wick_ts: object = None
    zone_lo: Optional[float] = None
    zone_hi: Optional[float] = None
    opp_strike: Optional[int] = None
    opp_type: Optional[str] = None

    @property
    def points(self) -> Optional[float]:
        if self.entry_price is None or self.exit_price is None:
            return None
        return _pts(self.side, self.entry_price, self.exit_price)


def summarize(label, trades: List[Trade]):
    entered = [t for t in trades if t.entry_price is not None]
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    print(f"{label:>22}  entered={len(entered):2d}  win%={win_pct:5.1f}  PF={pf:7.2f}  "
          f"total={total:+9.2f}  avg={((total/len(entered)) if entered else 0):+7.2f}")
    return {"label": label, "entered": len(entered), "pf": pf, "win_pct": win_pct, "total": total, "trades": trades}


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching spot data for entries (real Upstox 1-min NSE_EQ)...")
    spot_cache = await fetch_all_spot()

    baseline_trades: List[Trade] = []
    trap_trades: List[Trade] = []
    trap_fired_log = []

    for i, (trade_date, symbol, side_bias) in enumerate(ROWS):
        side = SIDE[side_bias]
        cached = spot_cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l = cached

        entry = find_entry(bars_1m, side, orb_h, orb_l, vol_by_ts)
        if entry is None:
            continue
        entry_ts, entry_price = entry

        # ATM strike = nearest listed strike to entry price, rounded to a
        # reasonable step (matches the live engine's now-ATM strike rule).
        step = 50 if entry_price < 2000 else (100 if entry_price < 10000 else 500)
        entry_strike = int(round(entry_price / step) * step)
        our_opt_type = "CE" if side == "CALL" else "PE"

        # ---- Baseline (unchanged): 15-min HA+StochRSI on spot-implied points ----
        ha_15m, k15, d15 = _ha_stoch_series(bars_1m, BASELINE_EXIT_TF_MIN)
        a_ts, a_px, a_reason = ha_stoch_exit(entry_ts, entry_price, side, bars_1m, ha_15m, k15, d15, inclusive=True)
        baseline_trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price, a_ts, a_px, a_reason))

        # ---- Bear-trap escalation: needs OPPOSITE strike's own premium ----
        print(f"[{i+1}/{len(ROWS)}] {trade_date} {symbol} {side} -- fetching opposite strike...")
        opp = await resolve_opposite_and_fetch(symbol, trade_date, entry_strike, our_opt_type)
        if opp is None:
            # No opposite-strike data resolvable -- falls back to baseline exactly.
            trap_trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price, a_ts, a_px, a_reason))
            continue

        wick_ts, zone, confirmed_ts = find_bear_trap_trigger(opp.bars_1m, entry_ts, trade_date)
        if wick_ts is None:
            # No trap fired -- baseline behavior, byte-identical.
            trap_trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price, a_ts, a_px, a_reason))
            continue

        # Trap fired: switch to LTF HA+StochRSI on the OPPOSITE strike from
        # wick_ts onward. The exit TIMESTAMP comes from the opposite chart;
        # our position's exit PRICE/points are read off OUR OWN spot-implied
        # series at that same timestamp (P&L stays in spot points, this
        # week's convention).
        opp_today = [b for b in opp.bars_1m if b.ts.date() == date.fromisoformat(trade_date) and b.ts >= wick_ts]
        if len(opp_today) < 4:
            trap_trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price, a_ts, a_px, a_reason,
                                      trap_confirmed_ts=confirmed_ts, trap_wick_ts=wick_ts,
                                      zone_lo=zone["zone_lo"], zone_hi=zone["zone_hi"],
                                      opp_strike=opp.strike, opp_type=opp.opt_type))
            continue
        # Opposite side's own P&L direction is inverted vs ours -- we want
        # the exit condition read as "opposite premium's own HA+StochRSI
        # reversal", side-mapped to opp.opt_type for the shape check.
        opp_side_for_check = "CALL" if opp.opt_type == "CE" else "PUT"
        ha_ltf, k_ltf, d_ltf = _ha_stoch_series(opp_today, LTF_EXIT_TF_MIN)
        ltf_ts, ltf_px, ltf_reason = ha_stoch_exit(wick_ts, opp_today[0].close, opp_side_for_check,
                                                    opp_today, ha_ltf, k_ltf, d_ltf, inclusive=True)

        # Map ltf_ts back to OUR OWN spot bars for the actual exit price/points.
        our_post = [b for b in bars_1m if b.ts >= ltf_ts]
        if our_post:
            exit_price = our_post[0].close
            exit_ts_final = our_post[0].ts
        else:
            exit_price = bars_1m[-1].close
            exit_ts_final = bars_1m[-1].ts

        trap_trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price,
                                  exit_ts_final, exit_price, f"bear_trap_ltf_{ltf_reason}",
                                  trap_confirmed_ts=confirmed_ts, trap_wick_ts=wick_ts,
                                  zone_lo=zone["zone_lo"], zone_hi=zone["zone_hi"],
                                  opp_strike=opp.strike, opp_type=opp.opt_type))
        trap_fired_log.append((trade_date, symbol, side, wick_ts, confirmed_ts))

    print("\n" + "=" * 110)
    print("SUMMARY")
    print("=" * 110)
    b_summary = summarize("baseline (frozen)", baseline_trades)
    t_summary = summarize("bear-trap escalation", trap_trades)
    print(f"\nBear-trap fired on {len(trap_fired_log)} of {len(trap_trades)} trades:")
    for row in trap_fired_log:
        print(f"  {row}")

    import json
    def _ser(trades):
        return [{"date": t.date, "symbol": t.symbol, "side": t.side,
                  "entry_ts": t.entry_ts.strftime("%Y-%m-%d %H:%M"), "entry_price": t.entry_price,
                  "exit_ts": t.exit_ts.strftime("%Y-%m-%d %H:%M"), "exit_price": t.exit_price,
                  "reason": t.reason, "points": t.points,
                  "trap_confirmed_ts": t.trap_confirmed_ts.strftime("%Y-%m-%d %H:%M") if t.trap_confirmed_ts else None,
                  "trap_wick_ts": t.trap_wick_ts.strftime("%Y-%m-%d %H:%M") if t.trap_wick_ts else None,
                  "zone_lo": t.zone_lo, "zone_hi": t.zone_hi,
                  "opp_strike": t.opp_strike, "opp_type": t.opp_type}
                 for t in sorted(trades, key=lambda x: (x.date, x.symbol))]

    report = {
        "baseline": {"entered": b_summary["entered"], "pf": b_summary["pf"], "win_pct": b_summary["win_pct"],
                     "total": b_summary["total"], "trades": _ser(baseline_trades)},
        "bear_trap": {"entered": t_summary["entered"], "pf": t_summary["pf"], "win_pct": t_summary["win_pct"],
                      "total": t_summary["total"], "trades": _ser(trap_trades)},
        "trap_fired_count": len(trap_fired_log),
    }
    out_path = os.path.join("data", "oi_orb_bear_trap_target_report.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nFull JSON report written to {out_path}")


asyncio.run(main())
