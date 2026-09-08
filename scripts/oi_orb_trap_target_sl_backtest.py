"""
scripts/oi_orb_trap_target_sl_backtest.py -- 2026-09-08, direct user follow-up:
"we have successfully checked for the target using the same trap logic, but
what about the stop loss? We have not done anything for stop loss." Layers
three genuinely different SL candidates UNDER the already-validated intraday
same-side trap target (HTF=15min/LTF=3min, oi_orb_trap_target_full_htf_ltf_
sweep.py's trap_target_exit) -- same real entries, same 46-trade dataset --
so this measures "does adding a real stop-loss help or hurt", not a
from-scratch redesign:

  A. Structural price-action SL -- multi-touch swing-low/high pool on the
     underlying's own 1-min spot chart (same "2+ touch pool" discipline this
     codebase already learned the hard way elsewhere: a bare single-touch
     pivot caused a real OI-Flow production incident on 2026-08-19, fixed by
     requiring 2+ clustered touches before a level counts as real structure).
  B. ATR-based SL -- indicator-derived, volatility-scaled per stock (reuses
     the SAME prev-day-seeded, boundary-gap-excluded ATR(14,3min) already
     validated in oi_orb_trend_capture_exit_backtest.py).
  C. Fixed % SL -- simplest possible baseline, so A and B have something to
     beat.

All checked on SPOT price (commensurable with every other comparison this
session, which is all in spot points) -- an option-premium-based SL would
need per-strike premium history for all 46 trades again (like the
opposite-strike work), deliberately deferred rather than rushed.

For each trade: SL and the existing target exit race independently;
whichever fires EARLIER (by timestamp) wins. If SL never fires, the trade's
outcome is byte-identical to the no-SL baseline.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_trap_target_sl_backtest.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from typing import Optional

sys.path.insert(0, ".")

from scripts.oi_orb_entry_mode_backtest import ROWS, SIDE, to_n_min_bars
from scripts.oi_orb_30_3_1_target_backtest import to_heikin_ashi
from scripts.oi_orb_trend_capture_exit_backtest import (
    fetch_all_with_seed, find_swings, group_touch_pools, _seeded_atr_series,
)
from scripts.oi_orb_trap_target_full_htf_ltf_sweep import find_entry, trap_target_exit
from scripts.oi_orb_atr_chandelier_backtest import ATR_TF_MIN

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
TARGET_HTF_MIN = 15
TARGET_LTF_MIN = 3


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

    @property
    def points(self) -> Optional[float]:
        if self.entry_price is None or self.exit_price is None:
            return None
        raw = self.exit_price - self.entry_price
        return raw if self.side == "CALL" else -raw


def structural_swing_sl(bars_1m, side, entry_ts, entry_price, tol_pct=0.1, min_touches=2, pivot=2):
    tol_pts = entry_price * tol_pct / 100.0
    pools = group_touch_pools(find_swings(bars_1m, pivot=pivot), tol_pts)
    post = [b for b in bars_1m if b.ts >= entry_ts]
    sl = None
    for b in post:
        confirmed = [p for p in pools if p["last_ts"] < b.ts and len(p["touches"]) >= min_touches]
        if side == "CALL":
            cands = [p["level"] for p in confirmed if p["kind"] == "L" and p["level"] < entry_price]
            if cands:
                new_sl = max(cands)
                sl = new_sl if sl is None else max(sl, new_sl)
            if sl is not None and b.low <= sl:
                return b.ts, sl
        else:
            cands = [p["level"] for p in confirmed if p["kind"] == "H" and p["level"] > entry_price]
            if cands:
                new_sl = min(cands)
                sl = new_sl if sl is None else min(sl, new_sl)
            if sl is not None and b.high >= sl:
                return b.ts, sl
    return None


def atr_sl(bars_1m, seed_bars_1m, side, entry_ts, entry_price, atr_mult):
    today_tf, atrs = _seeded_atr_series(seed_bars_1m, bars_1m)
    entry_atr = None
    for i, b in enumerate(today_tf):
        if b.ts <= entry_ts:
            entry_atr = atrs[i]
        else:
            break
    if entry_atr is None and atrs:
        entry_atr = atrs[0]
    if not entry_atr:
        return None
    sl_level = entry_price - atr_mult * entry_atr if side == "CALL" else entry_price + atr_mult * entry_atr
    for b in [x for x in bars_1m if x.ts >= entry_ts]:
        if side == "CALL" and b.low <= sl_level:
            return b.ts, sl_level
        if side == "PUT" and b.high >= sl_level:
            return b.ts, sl_level
    return None


def _vwap_series(bars_1m, vol_by_ts):
    from strategies.oi_orb_screener import screener
    vwap_state = screener.VwapState()
    out = {}
    for b in bars_1m:
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        out[b.ts] = vwap_state.current("SYM")
    return out


def vwap_close_sl(bars_1m, vol_by_ts, side, entry_ts, entry_price, tf_min):
    """2026-09-08, direct user spec (corrected): SHORT (PUT) SL hit the
    instant a HEIKIN-ASHI candle CLOSES above VWAP; LONG (CALL) SL hit the
    instant an HA candle closes below VWAP -- same HA convention as the
    earlier VWAP-close EXIT variant tested this session, not a plain candle
    close. HA computed on the FULL day's 1-min bars first (never on a
    sliced/post-entry-only series -- that misaligns the first bucket, the
    exact bug found and fixed in the earlier plain-close exit variant), then
    resampled to tf_min; VWAP itself stays on REAL (non-HA) typical price,
    since VWAP is a real traded-price average, not a smoothed synthetic
    series."""
    from datetime import timedelta
    vwap_by_ts = _vwap_series(bars_1m, vol_by_ts)
    ha_1m = to_heikin_ashi(bars_1m)
    tf_bars = to_n_min_bars(ha_1m, tf_min)
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]
    last_ts = post_entry[-1].ts if post_entry else entry_ts
    for hb in tf_bars:
        if hb.ts < entry_ts:
            continue
        if last_ts < hb.ts + timedelta(minutes=tf_min):
            break   # this bucket hasn't genuinely closed yet
        vwap_at_close = vwap_by_ts.get(hb.ts)
        if vwap_at_close is None:
            continue
        adverse = (hb.close < vwap_at_close) if side == "CALL" else (hb.close > vwap_at_close)
        if adverse:
            candidates = [b for b in post_entry if b.ts >= hb.ts]
            if candidates:
                return candidates[0].ts, candidates[0].close
    return None


def fixed_pct_sl(bars_1m, side, entry_ts, entry_price, pct):
    sl_level = entry_price * (1 - pct / 100.0) if side == "CALL" else entry_price * (1 + pct / 100.0)
    for b in [x for x in bars_1m if x.ts >= entry_ts]:
        if side == "CALL" and b.low <= sl_level:
            return b.ts, sl_level
        if side == "PUT" and b.high >= sl_level:
            return b.ts, sl_level
    return None


def summarize(trades):
    entered = [t for t in trades if t.entry_price is not None]
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    sl_hits = sum(1 for t in entered if t.reason.endswith("_sl"))
    max_loss = min((t.points for t in entered), default=0.0)
    return {"entered": len(entered), "win_pct": win_pct, "pf": (pf if pf != float("inf") else 9999.0),
            "total": total, "sl_hits": sl_hits, "max_loss": max_loss}


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows + prev-day ATR seed (real Upstox 1-min NSE_EQ history)...")
    cache = await fetch_all_with_seed()

    entries = {}
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            entries[(trade_date, symbol)] = None
            continue
        bars_1m, vol_by_ts, orb_h, orb_l, seed_bars_1m = cached
        entries[(trade_date, symbol)] = find_entry(bars_1m, side, orb_h, orb_l, vol_by_ts)

    variants = {
        "A_baseline_no_sl": lambda bars, seed, vol, side, ets, epx: None,
        "B_structural_swing_sl": lambda bars, seed, vol, side, ets, epx: structural_swing_sl(bars, side, ets, epx),
        "C_atr_sl_1.0x": lambda bars, seed, vol, side, ets, epx: atr_sl(bars, seed, side, ets, epx, 1.0),
        "C_atr_sl_1.5x": lambda bars, seed, vol, side, ets, epx: atr_sl(bars, seed, side, ets, epx, 1.5),
        "C_atr_sl_2.0x": lambda bars, seed, vol, side, ets, epx: atr_sl(bars, seed, side, ets, epx, 2.0),
        "D_fixed_pct_sl_1.0": lambda bars, seed, vol, side, ets, epx: fixed_pct_sl(bars, side, ets, epx, 1.0),
        "D_fixed_pct_sl_1.5": lambda bars, seed, vol, side, ets, epx: fixed_pct_sl(bars, side, ets, epx, 1.5),
        "D_fixed_pct_sl_2.0": lambda bars, seed, vol, side, ets, epx: fixed_pct_sl(bars, side, ets, epx, 2.0),
        "E_vwap_close_sl_5m": lambda bars, seed, vol, side, ets, epx: vwap_close_sl(bars, vol, side, ets, epx, 5),
        "E_vwap_close_sl_10m": lambda bars, seed, vol, side, ets, epx: vwap_close_sl(bars, vol, side, ets, epx, 10),
        "E_vwap_close_sl_15m": lambda bars, seed, vol, side, ets, epx: vwap_close_sl(bars, vol, side, ets, epx, 15),
        "E_vwap_close_sl_30m": lambda bars, seed, vol, side, ets, epx: vwap_close_sl(bars, vol, side, ets, epx, 30),
    }

    results = {}
    for vkey, sl_fn in variants.items():
        trades = []
        for trade_date, symbol, side_bias in ROWS:
            side = SIDE[side_bias]
            cached = cache.get((trade_date, symbol))
            entry = entries.get((trade_date, symbol))
            if cached is None or entry is None:
                continue
            bars_1m, vol_by_ts, orb_h, orb_l, seed_bars_1m = cached
            entry_ts, entry_price = entry
            htf_bars = to_n_min_bars(bars_1m, TARGET_HTF_MIN)
            ltf_bars = to_n_min_bars(bars_1m, TARGET_LTF_MIN)
            t_ts, t_px, t_reason = trap_target_exit(entry_ts, entry_price, side, bars_1m, htf_bars, ltf_bars,
                                                      TARGET_LTF_MIN)
            sl_result = sl_fn(bars_1m, seed_bars_1m, vol_by_ts, side, entry_ts, entry_price)
            if sl_result is not None and sl_result[0] <= t_ts:
                exit_ts, exit_px, reason = sl_result[0], sl_result[1], vkey.split("_sl")[0].split("_", 1)[-1] + "_sl"
            else:
                exit_ts, exit_px, reason = t_ts, t_px, t_reason
            trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price, exit_ts, exit_px, reason))
        results[vkey] = {"summary": summarize(trades),
                          "trades": [(t.date, t.symbol, t.side, t.entry_ts.strftime("%H:%M"),
                                      round(t.entry_price, 2), t.exit_ts.strftime("%H:%M"),
                                      round(t.exit_price, 2), t.reason, round(t.points, 2))
                                     for t in sorted(trades, key=lambda x: (x.date, x.symbol))]}
        s = results[vkey]["summary"]
        print(f"{vkey:>24}  entered={s['entered']:2d}  win%={s['win_pct']:5.1f}  PF={s['pf']:6.2f}  "
              f"total={s['total']:+9.2f}  sl_hits={s['sl_hits']:2d}  max_loss={s['max_loss']:+8.2f}")

    with open("data/oi_orb_trap_target_sl_backtest_report.json", "w") as f:
        json.dump(results, f)
    print("\nWrote data/oi_orb_trap_target_sl_backtest_report.json")


if __name__ == "__main__":
    asyncio.run(main())
