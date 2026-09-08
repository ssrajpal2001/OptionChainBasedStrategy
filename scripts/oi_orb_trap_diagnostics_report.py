"""
scripts/oi_orb_trap_diagnostics_report.py -- 2026-09-08, direct user follow-up:
full stock-by-stock diagnostic timeline (zone formed / zone entered / SL hit)
for the two best trap-based exit candidates, so they can be compared side by
side:
  1. Same-side trap + S1/R1 ladder, best combo HTF=15min/LTF=3min
     (oi_orb_trap_target_full_htf_ltf_sweep.py's trap_target_exit).
  2. Opposite-strike trap escalation, best combo HTF=1D/LTF=5min
     (oi_orb_bear_trap_target_backtest.py's find_bear_trap_trigger +
     escalation to the opposite strike's own HA+StochRSI), with the
     escalated StochRSI leg now PRIMED from the previous trading day's own
     HA closes (per direct user requirement, not yet applied to this
     mechanic before now).

Both instrumented (not reimplemented) to also return the diagnostic
timestamps -- zone lock time, zone-touch time, established S1/R1 or
escalation-trigger time, and final exit -- alongside the same points/PF/win%
math already validated.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_trap_diagnostics_report.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import List, Optional

sys.path.insert(0, ".")

from strategies.core.support_resistance import SupportResistanceCalculator
from strategies.core.candle_indicators import to_heikin_ashi, to_n_min_bars, compute_stoch_rsi
from scripts.oi_orb_entry_mode_backtest import ROWS, SIDE, to_bars, volume_by_ts, resolve_eq_key
from scripts.oi_orb_atr_chandelier_backtest import fetch_all
from scripts.oi_orb_trap_target_full_htf_ltf_sweep import find_entry
from scripts.oi_orb_ha_stochrsi_exit_backtest import ha_stoch_exit
from strategies.oi_orb_screener import screener
from data_layer.historical_candles import fetch_upstox_range_1m
from data_layer.instrument_registry import REGISTRY
from scripts.oi_orb_bear_trap_target_backtest import find_all_bear_zones

TOKEN = os.environ.get("UPSTOX_TOKEN", "")

# ---- Same-side trap best combo ----
SAME_HTF_MIN = 15
SAME_LTF_MIN = 3

# ---- Opposite-strike trap best combo ----
OPP_ZONE_HTF_MIN = 24 * 60   # 1D treated as one bucket per real trading day via to_n_min_bars on daily-close bars below
OPP_LTF_MIN = 5
OPP_ZONE_LOOKBACK_DAYS = 20
RSI_PERIOD = 9
STOCH_PERIOD = 9
SMOOTH = 3


def _prev_weekday(d: date) -> date:
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    return prev


@dataclass
class SameSideDiag:
    date: str
    symbol: str
    side: str
    entry_ts: object
    entry_price: float
    zone_lo: Optional[float] = None
    zone_hi: Optional[float] = None
    zone_lock_ts: object = None
    zone_touch_ts: object = None
    level_name: str = ""
    level_price: Optional[float] = None
    level_established_ts: object = None
    exit_ts: object = None
    exit_price: Optional[float] = None
    reason: str = ""

    @property
    def points(self):
        if self.entry_price is None or self.exit_price is None:
            return None
        raw = self.exit_price - self.entry_price
        return raw if self.side == "CALL" else -raw


def trap_target_exit_diag(entry_ts, entry_price, side, bars_1m, htf_bars, ltf_bars, ltf_min) -> SameSideDiag:
    zones_fn = screener.bull_trap_zones if side == "CALL" else screener.sharp_bear_zones
    zones = zones_fn(htf_bars)
    post_entry_1m = [b for b in bars_1m if b.ts >= entry_ts]

    diag = SameSideDiag(date=None, symbol=None, side=side, entry_ts=entry_ts, entry_price=entry_price)

    zone_touched_ts = None
    touched_zone = None
    calc = None
    ltf_fed = 0
    level_established_ts = None

    for b in post_entry_1m:
        if zone_touched_ts is None:
            for z in zones:
                if z["lock_ts"] is None or z["lock_ts"] > b.ts:
                    continue
                touched = (b.low <= z["zone_hi"]) and (b.high >= z["zone_lo"])
                if touched:
                    zone_touched_ts = b.ts
                    touched_zone = z
                    calc = SupportResistanceCalculator()
                    ltf_fed = 0
                    diag.zone_lo, diag.zone_hi = z["zone_lo"], z["zone_hi"]
                    diag.zone_lock_ts = z["lock_ts"]
                    diag.zone_touch_ts = b.ts
                    break

        if calc is not None:
            avail_ltf = [x for x in ltf_bars if entry_ts <= x.ts <= b.ts]
            for nb in avail_ltf[ltf_fed:]:
                calc.process_straddle_candle("SYM", {"timestamp": nb.ts, "high": nb.high,
                                                       "low": nb.low, "duration": ltf_min})
            ltf_fed = len(avail_ltf)
            sr = calc.get_calculated_sr_state("SYM").get("sr_levels", {})
            level = sr.get("S1") if side == "CALL" else sr.get("R1")
            if level is not None and level.get("is_established"):
                if level_established_ts is None:
                    level_established_ts = b.ts
                    diag.level_name = "S1" if side == "CALL" else "R1"
                    diag.level_established_ts = b.ts
                lvl = level["low"] if side == "CALL" else level["high"]
                diag.level_price = lvl
                breach = (b.low <= lvl) if side == "CALL" else (b.high >= lvl)
                if breach:
                    diag.exit_ts, diag.exit_price, diag.reason = b.ts, lvl, "trap_target_hit"
                    return diag

    if post_entry_1m:
        last = post_entry_1m[-1]
        diag.exit_ts, diag.exit_price, diag.reason = last.ts, last.close, "eod_close"
    else:
        diag.exit_ts, diag.exit_price, diag.reason = entry_ts, entry_price, "no_data_after_entry"
    return diag


@dataclass
class OppSideDiag:
    date: str
    symbol: str
    side: str
    entry_ts: object
    entry_price: float
    opp_strike: Optional[int] = None
    opp_type: Optional[str] = None
    zone_lo: Optional[float] = None
    zone_hi: Optional[float] = None
    zone_confirmed_ts: object = None
    trigger_wick_ts: object = None
    escalated: bool = False
    exit_ts: object = None
    exit_price: Optional[float] = None
    reason: str = ""

    @property
    def points(self):
        if self.entry_price is None or self.exit_price is None:
            return None
        raw = self.exit_price - self.entry_price
        return raw if self.side == "CALL" else -raw


def _atm_strike_from_entry(entry_price):
    """Same price-tiered step the original oi_orb_bear_trap_target_backtest.py
    uses (matches the live engine's ATM convention) -- NOT a flat step, which
    was the root cause of most strikes failing to resolve in the first pass."""
    step = 50 if entry_price < 2000 else (100 if entry_price < 10000 else 500)
    return int(round(entry_price / step) * step)


async def opposite_strike_trap_diag(trade_date, symbol, side, entry_ts, entry_price, bars_1m,
                                     prev_day_closes_cache) -> OppSideDiag:
    diag = OppSideDiag(date=trade_date, symbol=symbol, side=side, entry_ts=entry_ts, entry_price=entry_price)
    our_opt_type = "CE" if side == "CALL" else "PE"
    opp_type = "PE" if our_opt_type == "CE" else "CE"

    # Own-strike primed HA+StochRSI -- computed FIRST and used as the fallback for
    # EVERY failure path below (contract/key/data unresolvable, or no trap fired),
    # matching the original oi_orb_bear_trap_target_backtest.py's own design exactly
    # ("No opposite-strike data resolvable -- falls back to baseline exactly").
    # An earlier version of this diagnostic script recorded a flat 0-point
    # "unresolvable" result instead, which silently undercounted ~7 of 46 trades.
    own_prev_closes = prev_day_closes_cache.get((symbol, trade_date))
    ha_1m_own = to_heikin_ashi(bars_1m)
    ha_15m_own = to_n_min_bars(ha_1m_own, 15)
    own_closes = [b.close for b in ha_15m_own]
    seed_own = own_prev_closes or []
    all_own_closes = seed_own + own_closes
    k_own, d_own = compute_stoch_rsi(all_own_closes, RSI_PERIOD, STOCH_PERIOD, SMOOTH)
    seed_len_own = len(seed_own)

    def own_exit():
        post = [b for b in bars_1m if b.ts >= entry_ts]
        from strategies.core.candle_indicators import ha_stoch_shape_exit_signal
        for i, hb in enumerate(ha_15m_own):
            if hb.ts < entry_ts:
                continue
            ki, di = k_own[seed_len_own + i], d_own[seed_len_own + i]
            if ha_stoch_shape_exit_signal(hb, ki, di, side, inclusive=True):
                cands = [x for x in post if x.ts >= hb.ts]
                if cands:
                    return cands[0].ts, cands[0].close, "ha_stoch_exit"
        if post:
            return post[-1].ts, post[-1].close, "eod_close"
        return entry_ts, entry_price, "no_data_after_entry"

    d = date.fromisoformat(trade_date)
    try:
        await asyncio.to_thread(REGISTRY.load_sync, symbol, TOKEN)
    except Exception:
        pass
    exp = REGISTRY.get_active_expiry(symbol, d)
    if exp is None:
        diag.exit_ts, diag.exit_price, diag.reason = own_exit()
        diag.reason = "opp_contract_unresolvable:" + diag.reason
        return diag

    strike = _atm_strike_from_entry(entry_price)
    upstox_key = REGISTRY.get_upstox_key(symbol, exp, int(strike), opp_type)
    if not upstox_key:
        diag.exit_ts, diag.exit_price, diag.reason = own_exit()
        diag.reason = "opp_key_unresolvable:" + diag.reason
        return diag

    start = d - timedelta(days=OPP_ZONE_LOOKBACK_DAYS)
    rows = await fetch_upstox_range_1m(upstox_key, TOKEN, start, d)
    if not rows:
        diag.exit_ts, diag.exit_price, diag.reason = own_exit()
        diag.reason = "opp_no_data:" + diag.reason
        return diag
    opp_bars = to_bars(rows)
    diag.opp_strike, diag.opp_type = int(strike), opp_type

    today_bars = [b for b in opp_bars if b.ts.date() == d]
    prior_bars = [b for b in opp_bars if b.ts.date() < d]
    if not today_bars:
        diag.exit_ts, diag.exit_price, diag.reason = own_exit()
        diag.reason = "opp_no_today_data:" + diag.reason
        return diag

    candidate_bars = [b for b in today_bars if b.ts >= entry_ts]
    trigger_ts, zone, trap_confirmed_ts = None, None, None
    for b in candidate_bars:
        history_so_far = prior_bars + [tb for tb in today_bars if tb.ts < b.ts]
        if len(history_so_far) < 20:
            continue
        htf_bars = to_n_min_bars(history_so_far, 24 * 60)
        # Daily bucketing via to_n_min_bars(...,24*60) degenerates on <1-day windows;
        # use calendar-day grouping directly for a real "1D" HTF.
        by_day = {}
        for hb in history_so_far:
            by_day.setdefault(hb.ts.date(), []).append(hb)
        from strategies.core.trap_zone_utils import Bar
        daily_bars = []
        for day in sorted(by_day):
            g = by_day[day]
            daily_bars.append(Bar(ts=g[0].ts, open=g[0].open, high=max(x.high for x in g),
                                   low=min(x.low for x in g), close=g[-1].close))
        if len(daily_bars) < 3:
            continue
        zones = find_all_bear_zones(daily_bars) if side == "CALL" else find_all_bear_zones(daily_bars)
        active = [z for z in zones if z["confirmed_ts"] < b.ts]
        for z in active:
            if z["zone_lo"] <= b.low <= z["zone_hi"]:
                trigger_ts, zone, trap_confirmed_ts = b.ts, z, z["confirmed_ts"]
                break
        if trigger_ts:
            break

    if trigger_ts is None:
        # No trap fired -- falls back to our OWN 15-min HA+StochRSI, primed from prev day.
        diag.reason_prefix = "no_trap_fired"
    else:
        diag.zone_lo, diag.zone_hi = zone["zone_lo"], zone["zone_hi"]
        diag.zone_confirmed_ts = trap_confirmed_ts
        diag.trigger_wick_ts = trigger_ts
        diag.escalated = True

    if not diag.escalated:
        ts_, px_, rs_ = own_exit()
        diag.exit_ts, diag.exit_price, diag.reason = ts_, px_, rs_
        return diag

    # Escalated: primed HA+StochRSI on the OPPOSITE strike, from trigger_ts onward, 5-min TF.
    opp_prev_closes = prev_day_closes_cache.get((symbol + "_OPP_" + opp_type, trade_date))
    ha_1m_opp = to_heikin_ashi(opp_bars)
    ha_5m_opp = to_n_min_bars(ha_1m_opp, OPP_LTF_MIN)
    opp_closes = [b.close for b in ha_5m_opp]
    seed_opp = opp_prev_closes or []
    all_opp_closes = seed_opp + opp_closes
    k_opp, d_opp = compute_stoch_rsi(all_opp_closes, RSI_PERIOD, STOCH_PERIOD, SMOOTH)
    seed_len_opp = len(seed_opp)

    # Opposite side's own P&L direction is inverted vs ours (mirrors the
    # original script exactly) -- the shape/cross check needs the OPPOSITE
    # option's own side label, not ours.
    opp_side_for_check = "CALL" if opp_type == "CE" else "PUT"
    post_opp = [b for b in opp_bars if b.ts >= trigger_ts]
    from strategies.core.candle_indicators import ha_stoch_shape_exit_signal
    ltf_ts = None
    for i, hb in enumerate(ha_5m_opp):
        if hb.ts < trigger_ts:
            continue
        ki, di = k_opp[seed_len_opp + i], d_opp[seed_len_opp + i]
        if ha_stoch_shape_exit_signal(hb, ki, di, opp_side_for_check, inclusive=True):
            cands = [x for x in post_opp if x.ts >= hb.ts]
            if cands:
                ltf_ts = cands[0].ts
                break
    if ltf_ts is None:
        ltf_ts = post_opp[-1].ts if post_opp else trigger_ts

    # CRITICAL: exit PRICE/points must come from OUR OWN spot-implied series
    # at that same timestamp, never the opposite strike's own option premium
    # (a completely different, much smaller-denominated instrument) -- exact
    # same convention as the original oi_orb_bear_trap_target_backtest.py's
    # own main(). Using the opposite premium directly here was the root
    # cause of the nonsensical -630/-644/-372 "loss" numbers in the first
    # pass of this script.
    our_post = [b for b in bars_1m if b.ts >= ltf_ts]
    if our_post:
        diag.exit_ts, diag.exit_price = our_post[0].ts, our_post[0].close
    else:
        diag.exit_ts, diag.exit_price = bars_1m[-1].ts, bars_1m[-1].close
    diag.reason = "bear_trap_ltf_exit"
    return diag


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows (real Upstox 1-min NSE_EQ history)...")
    cache = await fetch_all()

    entries = {}
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            entries[(trade_date, symbol)] = None
            continue
        bars_1m, vol_by_ts, orb_h, orb_l = cached
        entries[(trade_date, symbol)] = find_entry(bars_1m, side, orb_h, orb_l, vol_by_ts)

    # ---- Same-side trap diagnostics ----
    print("\n=== Same-side trap + S1/R1 (15min/3min) diagnostics ===")
    same_diags: List[SameSideDiag] = []
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        entry = entries.get((trade_date, symbol))
        if cached is None or entry is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l = cached
        entry_ts, entry_price = entry
        from scripts.oi_orb_entry_mode_backtest import to_n_min_bars as tnb
        htf_bars = tnb(bars_1m, SAME_HTF_MIN)
        ltf_bars = tnb(bars_1m, SAME_LTF_MIN)
        diag = trap_target_exit_diag(entry_ts, entry_price, side, bars_1m, htf_bars, ltf_bars, SAME_LTF_MIN)
        diag.date, diag.symbol = trade_date, symbol
        same_diags.append(diag)
        print(f"  {trade_date} {symbol:<12} {side:<4} entry={entry_price:9.2f}@{entry_ts.strftime('%H:%M')} "
              f"zone_lock={diag.zone_lock_ts.strftime('%H:%M') if diag.zone_lock_ts else '-'} "
              f"zone_touch={diag.zone_touch_ts.strftime('%H:%M') if diag.zone_touch_ts else '-'} "
              f"{diag.level_name}_est={diag.level_established_ts.strftime('%H:%M') if diag.level_established_ts else '-'} "
              f"exit={diag.exit_price:9.2f}@{diag.exit_ts.strftime('%H:%M')} ({diag.reason}) pts={diag.points:+8.2f}")

    # ---- Opposite-strike trap diagnostics (primed) ----
    print("\n=== Opposite-strike trap (1D/5min, primed) diagnostics ===")
    # Prime cache: own-strike prev-day HA(15m) closes + opposite-strike prev-day HA(5m) closes.
    prev_day_closes_cache = {}
    opp_diags: List[OppSideDiag] = []
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        entry = entries.get((trade_date, symbol))
        if cached is None or entry is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l = cached
        entry_ts, entry_price = entry

        eq_key = resolve_eq_key(symbol)
        if eq_key:
            prev_d = _prev_weekday(date.fromisoformat(trade_date))
            key = (symbol, trade_date)
            if key not in prev_day_closes_cache:
                prev_rows = await fetch_upstox_range_1m(eq_key, TOKEN, prev_d, prev_d)
                if prev_rows:
                    pb = to_bars(prev_rows)
                    ph = to_heikin_ashi(pb)
                    p15 = to_n_min_bars(ph, 15)
                    prev_day_closes_cache[key] = [b.close for b in p15]
                else:
                    prev_day_closes_cache[key] = []

        diag = await opposite_strike_trap_diag(trade_date, symbol, side, entry_ts, entry_price, bars_1m,
                                                prev_day_closes_cache)
        opp_diags.append(diag)
        zc = diag.zone_confirmed_ts.strftime('%H:%M') if diag.zone_confirmed_ts else '-'
        tw = diag.trigger_wick_ts.strftime('%H:%M') if diag.trigger_wick_ts else '-'
        print(f"  {trade_date} {symbol:<12} {side:<4} entry={entry_price:9.2f}@{entry_ts.strftime('%H:%M')} "
              f"opp={diag.opp_strike}{diag.opp_type} zone_confirmed={zc} trigger={tw} escalated={diag.escalated} "
              f"exit={diag.exit_price if diag.exit_price else 0:9.2f}@{diag.exit_ts.strftime('%H:%M') if diag.exit_ts else '-'} "
              f"({diag.reason}) pts={diag.points if diag.points is not None else 0:+8.2f}")

    def summarize(pts_list):
        entered = [p for p in pts_list if p is not None]
        wins = [p for p in entered if p > 0]
        losses = [p for p in entered if p <= 0]
        total = sum(entered)
        loss_sum = sum(losses)
        pf = (sum(wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
        win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
        return {"entered": len(entered), "win_pct": win_pct, "pf": pf, "total": total}

    same_summary = summarize([d.points for d in same_diags])
    opp_summary = summarize([d.points for d in opp_diags])
    print(f"\nSame-side trap:     entered={same_summary['entered']} win%={same_summary['win_pct']:.1f} "
          f"PF={same_summary['pf']:.2f} total={same_summary['total']:+.2f}")
    print(f"Opposite-strike:    entered={opp_summary['entered']} win%={opp_summary['win_pct']:.1f} "
          f"PF={opp_summary['pf']:.2f} total={opp_summary['total']:+.2f}")

    import json as _json
    with open("data/oi_orb_trap_diagnostics_report.json", "w") as f:
        _json.dump({
            "same_side": {"summary": same_summary, "trades": [
                {"date": d.date, "symbol": d.symbol, "side": d.side,
                 "entry_ts": d.entry_ts.strftime("%H:%M"), "entry_price": d.entry_price,
                 "zone_lo": d.zone_lo, "zone_hi": d.zone_hi,
                 "zone_lock_ts": d.zone_lock_ts.strftime("%H:%M") if d.zone_lock_ts else None,
                 "zone_touch_ts": d.zone_touch_ts.strftime("%H:%M") if d.zone_touch_ts else None,
                 "level_name": d.level_name, "level_price": d.level_price,
                 "level_established_ts": d.level_established_ts.strftime("%H:%M") if d.level_established_ts else None,
                 "exit_ts": d.exit_ts.strftime("%H:%M") if d.exit_ts else None, "exit_price": d.exit_price,
                 "reason": d.reason, "points": d.points}
                for d in same_diags]},
            "opposite_strike": {"summary": opp_summary, "trades": [
                {"date": d.date, "symbol": d.symbol, "side": d.side,
                 "entry_ts": d.entry_ts.strftime("%H:%M"), "entry_price": d.entry_price,
                 "opp_strike": d.opp_strike, "opp_type": d.opp_type,
                 "zone_lo": d.zone_lo, "zone_hi": d.zone_hi,
                 "zone_confirmed_ts": d.zone_confirmed_ts.strftime("%H:%M") if d.zone_confirmed_ts else None,
                 "trigger_wick_ts": d.trigger_wick_ts.strftime("%H:%M") if d.trigger_wick_ts else None,
                 "escalated": d.escalated,
                 "exit_ts": d.exit_ts.strftime("%H:%M") if d.exit_ts else None, "exit_price": d.exit_price,
                 "reason": d.reason, "points": d.points}
                for d in opp_diags]},
        }, f)
    print("Wrote data/oi_orb_trap_diagnostics_report.json")


if __name__ == "__main__":
    asyncio.run(main())
