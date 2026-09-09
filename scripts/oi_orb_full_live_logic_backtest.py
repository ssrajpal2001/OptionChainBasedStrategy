"""
scripts/oi_orb_full_live_logic_backtest.py -- 2026-09-08, direct user
follow-up: "after you are done, do a backtest once again with the complete
logic, which we have done for the live integration."

Every prior backtest this session tested ONE piece in isolation (the SL
alone under a fixed intraday-only target; the multiday target alone under
cold-start entries; etc). This script is the first to combine ALL of it,
matching exactly what strategies/oi_orb_screener/engine.py now runs live:

  1. ENTRY: VWAP-retest (screener.check_vwap_retest_entry), historical-
     immediate-fire check at ORB freeze, identical to find_entry().
  2. EXIT PRIORITY (checked in this order every tick, matches the live main
     loop's own ordering):
       a. Hard SL -- 30-min HEIKIN-ASHI candle closes on the wrong side of
          the running session VWAP (CALL: close < vwap; PUT: close > vwap).
       b. Multi-day 75min HTF same-side trap zone + 3min S&R ladder (>=15
          calendar days of real history).
       c. Fallback: intraday 15min HTF same-side trap + 3min S&R ladder
          (single-day bars), only if (b) never locked+touched a zone.
       d. EOD square-off (15:29, last available bar) -- final fallback,
          embedded in the trap-exit functions' own fallback.
  3. RE-ENTRY: exactly ONE re-entry allowed per (symbol, side) per day, and
     ONLY after an SL stop-out (never after a target-hit or EOD exit) --
     direct user spec ("I want re-entry allowed after a SL stopped out, but
     just once in that specific script for that day"). The re-entry scan
     continues the SAME session VWAP state (never resets) and starts a
     fresh arm/retest cycle from the SL exit instant through ENTRY_WINDOW_END.

Same real 51-row shortlist dataset (46 usable) used for every comparison
this session.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_full_live_logic_backtest.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Optional

sys.path.insert(0, ".")

from scripts.oi_orb_entry_mode_backtest import (
    ROWS, SIDE, ORB_START, ORB_END, ENTRY_WINDOW_END, _key_range, to_n_min_bars,
    resolve_eq_key, to_bars, volume_by_ts, compute_orb,
)
from scripts.oi_orb_30_3_1_target_backtest import to_heikin_ashi
from scripts.oi_orb_trap_target_full_htf_ltf_sweep import trap_target_exit
from scripts.oi_orb_same_side_trap_multiday_htf_sweep import (
    trap_target_exit_diag_multiday, _to_n_min_bars_dateaware,
)
from strategies.oi_orb_screener import screener
from data_layer.historical_candles import fetch_upstox_range_1m

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
TRAP_HTF_MULTIDAY_MIN = 75
TRAP_HTF_INTRADAY_MIN = 15
TRAP_LTF_MIN = 3
SL_TF_MIN = 30
LOOKBACK_CALENDAR_DAYS = 15


@dataclass
class Leg:
    entry_ts: object
    entry_price: float
    exit_ts: object
    exit_price: float
    reason: str

    @property
    def points(self) -> float:
        raw = self.exit_price - self.entry_price
        return raw


@dataclass
class Trade:
    date: str
    symbol: str
    side: str
    legs: List[Leg]

    @property
    def points(self) -> float:
        total = 0.0
        for leg in self.legs:
            raw = leg.exit_price - leg.entry_price
            total += raw if self.side == "CALL" else -raw
        return total


def _vwap_series_full_day(bars_1m, vol_by_ts):
    vwap_state = screener.VwapState()
    out = {}
    for b in bars_1m:
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        out[b.ts] = vwap_state.current("SYM")
    return out


def vwap_close_sl_exit(bars_1m, vwap_by_ts, side, entry_ts):
    """Same mechanic as the live _vwap_close_sl_check / the standalone SL
    backtest's vwap_close_sl -- HA computed on the FULL day first (avoids
    the misaligned-first-bucket bug), boundary-safe (only fully-closed
    30-min buckets)."""
    ha_1m = to_heikin_ashi(bars_1m)
    tf_bars = to_n_min_bars(ha_1m, SL_TF_MIN)
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]
    if not post_entry:
        return None
    last_ts = post_entry[-1].ts
    for hb in tf_bars:
        if hb.ts < entry_ts:
            continue
        if last_ts < hb.ts + timedelta(minutes=SL_TF_MIN):
            break
        vwap_at_close = vwap_by_ts.get(hb.ts)
        if vwap_at_close is None:
            continue
        adverse = (hb.close < vwap_at_close) if side == "CALL" else (hb.close > vwap_at_close)
        if adverse:
            candidates = [b for b in post_entry if b.ts >= hb.ts]
            if candidates:
                return candidates[0].ts, candidates[0].close
    return None


def simulate_target_exit(entry_ts, entry_price, side, bars_1m, htf_multiday_bars):
    """Exact live priority: multi-day trap first, intraday 15/3 fallback if
    it never locks+touches a zone (embeds EOD as its own final fallback)."""
    if htf_multiday_bars and len(htf_multiday_bars) >= 3:
        ltf_today = to_n_min_bars(bars_1m, TRAP_LTF_MIN)
        diag = trap_target_exit_diag_multiday(entry_ts, entry_price, side, bars_1m, htf_multiday_bars,
                                               ltf_today, TRAP_LTF_MIN)
        if diag.exit_ts is not None:
            return diag.exit_ts, diag.exit_price, "trap_multiday_exit"
    htf15 = to_n_min_bars(bars_1m, TRAP_HTF_INTRADAY_MIN)
    ltf3 = to_n_min_bars(bars_1m, TRAP_LTF_MIN)
    t_ts, t_px, t_reason = trap_target_exit(entry_ts, entry_price, side, bars_1m, htf15, ltf3, TRAP_LTF_MIN)
    return t_ts, t_px, t_reason


def resolve_exit(entry_ts, entry_price, side, bars_1m, vwap_by_ts, htf_multiday_bars):
    sl = vwap_close_sl_exit(bars_1m, vwap_by_ts, side, entry_ts)
    t_ts, t_px, t_reason = simulate_target_exit(entry_ts, entry_price, side, bars_1m, htf_multiday_bars)
    if sl is not None and sl[0] <= t_ts:
        return sl[0], sl[1], "vwap_close_sl"
    return t_ts, t_px, t_reason


def find_first_entry(bars_1m, side, orb_h, orb_l, vol_by_ts, vwap_state):
    """Same as find_entry() (historical-immediate + live scan) but takes an
    externally-owned vwap_state so it can be threaded through into the
    re-entry scan afterward -- VWAP is session-wide and must never reset
    mid-day."""
    orb_bars = _key_range(bars_1m, ORB_START, ORB_END)
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
        return None

    if historically_fulfilled:
        b0 = entry_window[0]
        return b0.ts, b0.close

    armed = False
    for b in entry_window:
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        vwap = vwap_state.current("SYM")
        if vwap is None:
            continue
        armed, fire = screener.check_vwap_retest_entry(side, b.close, vwap, armed, 0.15)
        if fire:
            return b.ts, b.close
    return None


def find_reentry(bars_1m, side, from_ts, vwap_state):
    """Continues the SAME running vwap_state, fresh arm state, scanning
    from_ts through ENTRY_WINDOW_END for the next confirmed VWAP-retest --
    live's own "resume scanning immediately" re-entry mechanic."""
    window = _key_range(bars_1m, from_ts.strftime("%H:%M"), ENTRY_WINDOW_END)
    window = [b for b in window if b.ts >= from_ts]
    armed = False
    for b in window:
        vwap = vwap_state.current("SYM")
        if vwap is None:
            continue
        armed, fire = screener.check_vwap_retest_entry(side, b.close, vwap, armed, 0.15)
        if fire:
            return b.ts, b.close
    return None


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching multi-day + today history (real Upstox 1-min NSE_EQ)...")
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
        start = d - timedelta(days=LOOKBACK_CALENDAR_DAYS)
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
        orb = compute_orb(today_bars)
        if orb is None:
            cache[key] = None
            continue
        orb_h, orb_l = orb
        htf_multiday = _to_n_min_bars_dateaware(all_bars, TRAP_HTF_MULTIDAY_MIN)
        cache[key] = (today_bars, vol_by_ts, orb_h, orb_l, htf_multiday)
    n_ok = sum(1 for v in cache.values() if v)
    print(f"Fetched {n_ok}/{len(cache)} usable rows.")

    trades = []
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l, htf_multiday = cached
        vwap_state = screener.VwapState()
        vwap_by_ts = _vwap_series_full_day(bars_1m, vol_by_ts)

        entry = find_first_entry(bars_1m, side, orb_h, orb_l, vol_by_ts, vwap_state)
        if entry is None:
            continue
        entry_ts, entry_price = entry

        legs = []
        sl_reentry_used = False
        cur_entry_ts, cur_entry_price = entry_ts, entry_price
        while True:
            exit_ts, exit_price, reason = resolve_exit(cur_entry_ts, cur_entry_price, side, bars_1m,
                                                         vwap_by_ts, htf_multiday)
            legs.append(Leg(cur_entry_ts, cur_entry_price, exit_ts, exit_price, reason))
            if reason != "vwap_close_sl" or sl_reentry_used:
                break
            sl_reentry_used = True
            nxt = find_reentry(bars_1m, side, exit_ts, vwap_state)
            if nxt is None:
                break
            cur_entry_ts, cur_entry_price = nxt

        trades.append(Trade(trade_date, symbol, side, legs))

    entered = trades
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    reentries = sum(1 for t in entered if len(t.legs) > 1)
    sl_hits = sum(1 for t in entered for leg in t.legs if leg.reason == "vwap_close_sl")
    max_loss = min((t.points for t in entered), default=0.0)

    print(f"\n{'='*120}\nFULL LIVE-LOGIC BACKTEST (SL + multiday/intraday trap target + 1x re-entry-after-SL)\n{'='*120}")
    for t in sorted(entered, key=lambda x: (x.date, x.symbol)):
        leg_str = " -> ".join(f"{lg.entry_price:.2f}@{lg.entry_ts.strftime('%H:%M')}"
                               f"..{lg.exit_price:.2f}@{lg.exit_ts.strftime('%H:%M')}({lg.reason})"
                               for lg in t.legs)
        print(f"  {t.date} {t.symbol:<12} {t.side:<4} pts={t.points:+8.2f}  legs={len(t.legs)}  {leg_str}")

    print(f"\nentered={len(entered)}  win%={win_pct:5.1f}  PF={pf:6.2f}  total={total:+9.2f}  "
          f"trades_with_reentry={reentries}  sl_hits(any leg)={sl_hits}  max_single_trade_loss={max_loss:+8.2f}")

    with open("data/oi_orb_full_live_logic_backtest_report.json", "w") as f:
        json.dump({
            "entered": len(entered), "win_pct": win_pct, "pf": (pf if pf != float("inf") else 9999.0),
            "total": total, "reentries": reentries, "sl_hits": sl_hits, "max_loss": max_loss,
            "trades": [
                {"date": t.date, "symbol": t.symbol, "side": t.side, "points": round(t.points, 2),
                 "legs": [{"entry_ts": lg.entry_ts.strftime("%H:%M"), "entry_price": lg.entry_price,
                           "exit_ts": lg.exit_ts.strftime("%H:%M"), "exit_price": lg.exit_price,
                           "reason": lg.reason} for lg in t.legs]}
                for t in sorted(entered, key=lambda x: (x.date, x.symbol))
            ],
        }, f)
    print("\nWrote data/oi_orb_full_live_logic_backtest_report.json")


if __name__ == "__main__":
    asyncio.run(main())
