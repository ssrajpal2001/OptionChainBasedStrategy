"""
scripts/oi_orb_sl_concept_comparison.py -- 2026-09-09, direct user follow-up:
"i think we need to search for better option for sl concept... suggest some
more options of sl tech... use ur experience" -- the plain VWAP-close SL
(no minimum distance, no grace period) is stopping out real, ultimately
profitable trades on noise right at/near entry (ADANIENT: stopped twice in
14/16 minutes, +1.90 actual vs +227.00 riding to EOD).

Same real 55-row dataset (46 historical + 9-14 real 2026-09-09 streamed
rows) and same entry (RollingVwapRetestTracker) and target (75min/3min
same-side trap) as every backtest this session -- ONLY the SL concept
varies, across 4 families:

  A. min_gap  -- current VWAP-close SL, but the adverse close must clear
     VWAP by a minimum % of price (0 = today's baseline, no buffer).
  B. grace    -- current VWAP-close SL, but the SL is not evaluated at all
     until `grace_min` minutes after entry (0 = today's baseline).
  C. atr      -- fixed distance from entry, entry +/- atr_mult * ATR(14)
     on 5-min bars computed from real pre-entry history, checked on
     INTRABAR touch (low/high, not close-only -- a resting stop order
     fills on touch in real trading).
  D. pct      -- flat percentage distance from entry, intrabar touch.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_sl_concept_comparison.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, ".")

from scripts.oi_orb_entry_mode_backtest import (
    ROWS as OLD_ROWS, SIDE, ORB_END, resolve_eq_key, to_bars, volume_by_ts, compute_orb,
)
from scripts.oi_orb_full_live_logic_backtest import (
    _vwap_series_full_day, simulate_target_exit, find_reentry, Leg, Trade,
    LOOKBACK_CALENDAR_DAYS, TRAP_HTF_MULTIDAY_MIN, to_heikin_ashi, to_n_min_bars,
)
from scripts.oi_orb_same_side_trap_multiday_htf_sweep import _to_n_min_bars_dateaware
from scripts.oi_orb_shaped_sl_streaming_backtest import TODAY_ROWS, find_first_entry_rolling_from
from strategies.oi_orb_screener import screener
from strategies.core.support_resistance import SupportResistanceCalculator
from data_layer.historical_candles import fetch_upstox_range_1m, fetch_upstox_intraday_1m

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
TODAY_STR = "2026-09-09"
BASE_TF = 20   # winning TF from the earlier sweep, shape gate off


# ---------- SL family A: min-gap VWAP-close ----------
def sl_min_gap(bars_1m, vwap_by_ts, side, entry_ts, gap_pct):
    ha_1m = to_heikin_ashi(bars_1m)
    tf_bars = to_n_min_bars(ha_1m, BASE_TF)
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]
    if not post_entry:
        return None
    last_ts = post_entry[-1].ts
    for hb in tf_bars:
        if hb.ts < entry_ts:
            continue
        if last_ts < hb.ts + timedelta(minutes=BASE_TF):
            break
        vwap_at_close = vwap_by_ts.get(hb.ts)
        if vwap_at_close is None or vwap_at_close <= 0:
            continue
        adverse_and_beyond = ((vwap_at_close - hb.close) / vwap_at_close >= gap_pct) if side == "CALL" \
            else ((hb.close - vwap_at_close) / vwap_at_close >= gap_pct)
        if not adverse_and_beyond:
            continue
        candidates = [b for b in post_entry if b.ts >= hb.ts]
        if candidates:
            return candidates[0].ts, candidates[0].close
    return None


# ---------- SL family B: grace period VWAP-close ----------
def sl_grace(bars_1m, vwap_by_ts, side, entry_ts, grace_min):
    ha_1m = to_heikin_ashi(bars_1m)
    tf_bars = to_n_min_bars(ha_1m, BASE_TF)
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]
    if not post_entry:
        return None
    last_ts = post_entry[-1].ts
    active_from = entry_ts + timedelta(minutes=grace_min)
    for hb in tf_bars:
        if hb.ts < entry_ts or hb.ts < active_from:
            continue
        if last_ts < hb.ts + timedelta(minutes=BASE_TF):
            break
        vwap_at_close = vwap_by_ts.get(hb.ts)
        if vwap_at_close is None:
            continue
        adverse = (hb.close < vwap_at_close) if side == "CALL" else (hb.close > vwap_at_close)
        if not adverse:
            continue
        candidates = [b for b in post_entry if b.ts >= hb.ts]
        if candidates:
            return candidates[0].ts, candidates[0].close
    return None


# ---------- SL family C: ATR stop (intrabar touch) ----------
def compute_atr(bars_5m, period=14):
    """Simple ATR over already-resampled 5-min bars, one value per bar
    (Wilder/simple moving TR average -- simple average used here, plain
    and auditable). Returns {bar.ts: atr_value_as_of_that_bar_close}."""
    out = {}
    trs = []
    prev_close = None
    for b in bars_5m:
        tr = (b.high - b.low)
        if prev_close is not None:
            tr = max(tr, abs(b.high - prev_close), abs(b.low - prev_close))
        trs.append(tr)
        if len(trs) > period:
            trs.pop(0)
        if len(trs) == period:
            out[b.ts] = sum(trs) / period
        prev_close = b.close
    return out


def sl_atr(bars_1m, side, entry_ts, entry_price, atr_mult):
    bars_5m = to_n_min_bars(bars_1m, 5)
    atr_by_ts = compute_atr(bars_5m, period=14)
    pre_entry_atrs = [v for ts, v in atr_by_ts.items() if ts < entry_ts]
    if not pre_entry_atrs:
        return None   # not enough history yet -- no ATR SL available for this trade
    atr_val = pre_entry_atrs[-1]
    if atr_val <= 0:
        return None
    sl_level = entry_price - atr_mult * atr_val if side == "CALL" else entry_price + atr_mult * atr_val
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]
    for b in post_entry:
        touched = (b.low <= sl_level) if side == "CALL" else (b.high >= sl_level)
        if touched:
            return b.ts, sl_level
    return None


# ---------- SL family D: flat % stop (intrabar touch) ----------
def sl_pct(bars_1m, side, entry_ts, entry_price, pct):
    sl_level = entry_price * (1 - pct) if side == "CALL" else entry_price * (1 + pct)
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]
    for b in post_entry:
        touched = (b.low <= sl_level) if side == "CALL" else (b.high >= sl_level)
        if touched:
            return b.ts, sl_level
    return None


# ---------- SL family E: LTF opposite-direction trap zone (pure price action) ----------
def sl_ltf_trap(bars_1m, side, entry_ts, htf_min):
    """Direct user spec: "1 ltf trap concept" -- reuses the SAME
    sharp_bear_zones/bull_trap_zones this codebase already validated for
    the TARGET side (verbatim, no reimplementation), but pointed at the
    OPPOSITE direction from the trade and run on a fast timeframe. A
    confirmed bull-trap forming after entry while short (or a confirmed
    bear-trap while long) is a real, price-action-only signal the market
    just structurally turned against the position -- exit on the zone's
    own lock (confirmation) bar."""
    bars_Nm = to_n_min_bars(bars_1m, htf_min)
    zones = screener.bull_trap_zones(bars_Nm) if side == "PUT" else screener.sharp_bear_zones(bars_Nm)
    candidates = [z for z in zones if z["lock_ts"] > entry_ts]
    if not candidates:
        return None
    candidates.sort(key=lambda z: z["lock_ts"])
    lock_ts = candidates[0]["lock_ts"]
    post_entry = [b for b in bars_1m if b.ts >= lock_ts]
    if not post_entry:
        return None
    return post_entry[0].ts, post_entry[0].close


# ---------- SL family F: LTF S&R breach (pure price action) ----------
def sl_ltf_sr(bars_1m, side, entry_ts, ltf_min):
    """Direct user spec: "2nd s&r in lower tf" -- reuses the SAME
    SupportResistanceCalculator every other S&R feature in this codebase
    already reuses (D1TrapSRBook, CAG Straddle, the target's own 3-min
    ladder), FRESH at entry (same "no pre-entry history" pattern CAG
    Straddle's SideTracker uses, so a bar right at entry can't misread as
    already mid-cycle). Exit the instant a candle's CLOSE breaches the
    calculator's own S1 (long)/R1 (short) level as it stood BEFORE that
    candle folded in -- a real, price-action-only structural stop."""
    calc = SupportResistanceCalculator()
    bars_Nm = to_n_min_bars(bars_1m, ltf_min)
    post_entry_bars = [b for b in bars_Nm if b.ts >= entry_ts]
    for b in post_entry_bars:
        state_before = calc.get_calculated_sr_state("OPT")
        levels_before = state_before.get("sr_levels") or {}
        s1_before = (levels_before.get("S1") or {}).get("low")
        r1_before = (levels_before.get("R1") or {}).get("high")
        candle = {"timestamp": b.ts, "high": b.high, "low": b.low, "duration": ltf_min}
        calc.process_straddle_candle("OPT", candle, silent=True)
        if side == "CALL" and s1_before is not None and b.close < s1_before:
            return b.ts, b.close
        if side == "PUT" and r1_before is not None and b.close > r1_before:
            return b.ts, b.close
    return None


SL_FAMILIES = {
    "baseline_vwap_close": lambda bars, vwap_by_ts, side, ets, epx: sl_min_gap(bars, vwap_by_ts, side, ets, 0.0),
    "min_gap_0.2%": lambda bars, vwap_by_ts, side, ets, epx: sl_min_gap(bars, vwap_by_ts, side, ets, 0.002),
    "min_gap_0.3%": lambda bars, vwap_by_ts, side, ets, epx: sl_min_gap(bars, vwap_by_ts, side, ets, 0.003),
    "min_gap_0.5%": lambda bars, vwap_by_ts, side, ets, epx: sl_min_gap(bars, vwap_by_ts, side, ets, 0.005),
    "grace_15min": lambda bars, vwap_by_ts, side, ets, epx: sl_grace(bars, vwap_by_ts, side, ets, 15),
    "grace_20min": lambda bars, vwap_by_ts, side, ets, epx: sl_grace(bars, vwap_by_ts, side, ets, 20),
    "grace_30min": lambda bars, vwap_by_ts, side, ets, epx: sl_grace(bars, vwap_by_ts, side, ets, 30),
    "atr_1.5x": lambda bars, vwap_by_ts, side, ets, epx: sl_atr(bars, side, ets, epx, 1.5),
    "atr_2x": lambda bars, vwap_by_ts, side, ets, epx: sl_atr(bars, side, ets, epx, 2.0),
    "atr_3x": lambda bars, vwap_by_ts, side, ets, epx: sl_atr(bars, side, ets, epx, 3.0),
    "pct_0.3%": lambda bars, vwap_by_ts, side, ets, epx: sl_pct(bars, side, ets, epx, 0.003),
    "pct_0.5%": lambda bars, vwap_by_ts, side, ets, epx: sl_pct(bars, side, ets, epx, 0.005),
    "pct_0.75%": lambda bars, vwap_by_ts, side, ets, epx: sl_pct(bars, side, ets, epx, 0.0075),
    "pct_1.0%": lambda bars, vwap_by_ts, side, ets, epx: sl_pct(bars, side, ets, epx, 0.01),
    "ltf_trap_5m": lambda bars, vwap_by_ts, side, ets, epx: sl_ltf_trap(bars, side, ets, 5),
    "ltf_trap_10m": lambda bars, vwap_by_ts, side, ets, epx: sl_ltf_trap(bars, side, ets, 10),
    "ltf_trap_15m": lambda bars, vwap_by_ts, side, ets, epx: sl_ltf_trap(bars, side, ets, 15),
    "ltf_sr_1m": lambda bars, vwap_by_ts, side, ets, epx: sl_ltf_sr(bars, side, ets, 1),
    "ltf_sr_3m": lambda bars, vwap_by_ts, side, ets, epx: sl_ltf_sr(bars, side, ets, 3),
    "ltf_sr_5m": lambda bars, vwap_by_ts, side, ets, epx: sl_ltf_sr(bars, side, ets, 5),
}


def resolve_exit_family(sl_fn, entry_ts, entry_price, side, bars_1m, vwap_by_ts, htf_multiday):
    sl = sl_fn(bars_1m, vwap_by_ts, side, entry_ts, entry_price)
    t_ts, t_px, t_reason = simulate_target_exit(entry_ts, entry_price, side, bars_1m, htf_multiday)
    if sl is not None and sl[0] <= t_ts:
        return sl[0], sl[1], "sl"
    return t_ts, t_px, t_reason


async def fetch_all():
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
        htf_multiday = _to_n_min_bars_dateaware(all_bars, TRAP_HTF_MULTIDAY_MIN)
        cache[key] = (today_bars, vol_by_ts, htf_multiday)
    n_ok = sum(1 for v in cache.values() if v)
    print(f"Fetched {n_ok}/{len(cache)} usable rows.")
    return cache


def run_family(cache, sl_fn):
    trades = []
    for trade_date, symbol, side_bias in OLD_ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, htf_multiday = cached
        vwap_state = screener.VwapState()
        vwap_by_ts = _vwap_series_full_day(bars_1m, vol_by_ts)
        orb_end_bars = [b for b in bars_1m if b.ts.strftime("%H:%M") >= ORB_END]
        if not orb_end_bars:
            continue
        entry = find_first_entry_rolling_from(bars_1m, side, vol_by_ts, vwap_state, orb_end_bars[0].ts)
        if entry is None:
            continue
        _run_trade(trades, trade_date, symbol, side, entry, bars_1m, vwap_by_ts, htf_multiday, vwap_state, sl_fn)

    for trade_date, symbol, side_bias, start_ts in TODAY_ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, htf_multiday = cached
        vwap_state = screener.VwapState()
        vwap_by_ts = _vwap_series_full_day(bars_1m, vol_by_ts)
        entry = find_first_entry_rolling_from(bars_1m, side, vol_by_ts, vwap_state, start_ts)
        if entry is None:
            continue
        _run_trade(trades, trade_date, symbol, side, entry, bars_1m, vwap_by_ts, htf_multiday, vwap_state, sl_fn)

    wins = [t for t in trades if t.points > 0]
    losses = [t for t in trades if t.points <= 0]
    total = sum(t.points for t in trades)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(trades) * 100) if trades else 0.0
    max_loss = min((t.points for t in trades), default=0.0)
    sl_hits = sum(1 for t in trades for leg in t.legs if leg.reason == "sl")
    return {"entered": len(trades), "win_pct": win_pct, "pf": (pf if pf != float("inf") else 9999.0),
            "total": total, "sl_hits": sl_hits, "max_loss": max_loss}


def _run_trade(trades, trade_date, symbol, side, entry, bars_1m, vwap_by_ts, htf_multiday, vwap_state, sl_fn):
    entry_ts, entry_price = entry
    legs = []
    sl_reentry_used = False
    cur_entry_ts, cur_entry_price = entry_ts, entry_price
    while True:
        exit_ts, exit_price, reason = resolve_exit_family(
            sl_fn, cur_entry_ts, cur_entry_price, side, bars_1m, vwap_by_ts, htf_multiday)
        legs.append(Leg(cur_entry_ts, cur_entry_price, exit_ts, exit_price, reason))
        if reason != "sl" or sl_reentry_used:
            break
        sl_reentry_used = True
        nxt = find_reentry(bars_1m, side, exit_ts, vwap_state)
        if nxt is None:
            break
        cur_entry_ts, cur_entry_price = nxt
    trades.append(Trade(trade_date, symbol, side, legs))


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching real Upstox 1-min history...")
    cache = await fetch_all()

    results = {}
    for name, fn in SL_FAMILIES.items():
        r = run_family(cache, fn)
        results[name] = r
        print(f"{name:<22} entered={r['entered']:>3}  win%={r['win_pct']:>5.1f}  PF={r['pf']:>8.2f}  "
              f"total={r['total']:>+9.2f}  sl_hits={r['sl_hits']:>3}  max_loss={r['max_loss']:>+8.2f}")

    with open("data/oi_orb_sl_concept_comparison_report.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\nWrote data/oi_orb_sl_concept_comparison_report.json")


if __name__ == "__main__":
    asyncio.run(main())
