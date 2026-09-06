"""
scripts/oi_orb_30_3_1_target_backtest.py -- 2026-09-05, direct user spec,
BOTH sides now (PUT mirror added 2026-09-05 same day) -- there is NO
stoploss at all in this version, pure target logic:

  LONG (CALL):
    1. Trigger: watch 30-min bars (intraday only, single trading day) from
       entry onward. The first 30-min bar whose LOW is below the PREVIOUS
       30-min bar's low arms the rest of the mechanic.
    2. Once armed, watch for a 3-min BULL TRAP zone (screener.bull_trap_zones)
       to form and for price to touch it.
    3. The moment price touches an already-locked bull-trap zone, jump to a
       FRESH SupportResistanceCalculator fed on 1-MINUTE bars from that
       instant. The moment its S1 level becomes established and is then
       breached (price trades below it), exit -- "target_hit".

  SHORT (PUT), mirrored:
    1. Trigger: the first 30-min bar whose HIGH is above the PREVIOUS
       30-min bar's high.
    2. Watch for a 3-min BEAR TRAP zone (screener.sharp_bear_zones).
    3. On zone touch, fresh 1-min SupportResistanceCalculator; exit the
       moment R1 becomes established and is breached (price trades above
       it) -- "target_hit".

  Either side: if the 30-min trigger never fires, or a zone never forms/
  gets touched, or S1/R1 never breaches -- the trade simply runs to EOD.
  No SL of any kind cuts it short.

Per direct instruction: 30-min and 3-min are themselves optimization
knobs (not swept this pass -- single config, exactly as specified: 30m
trigger / 3m trap / 1m S&R). Intraday-only (no multi-day history needed
this round, unlike the HTF sweep).

Entry timing is unchanged: same VWAP-retest / historical-immediate /
no-reentry / breach-cancel mechanic used in every OI-ORB backtest this
week (scripts/oi_orb_entry_mode_backtest.py). No SL means no reentry
trigger exists this round either (reentry was always SL-gated) -- at most
one trade per stock per day.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_30_3_1_target_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from typing import List, Optional

sys.path.insert(0, ".")

from strategies.core.support_resistance import SupportResistanceCalculator
from strategies.core.trap_zone_utils import Bar
from scripts.oi_orb_entry_mode_backtest import (
    ROWS, ORB_START, ORB_END, ENTRY_WINDOW_END, _key_range, to_n_min_bars, compute_orb,
)
from scripts.oi_orb_atr_chandelier_backtest import fetch_all
from strategies.oi_orb_screener import screener

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
HTF_MIN = 30
TRAP_TF_MIN = 3
LTF_MIN = 1


# 2026-09-06: to_heikin_ashi moved to strategies/core/candle_indicators.py
# (ported into the live oi_orb_screener engine) -- imported here, not
# duplicated, so this backtest can never drift from the live version.
from strategies.core.candle_indicators import to_heikin_ashi  # noqa: E402,F401


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
    trigger_ts: object
    zone_touched_ts: object

    @property
    def points(self) -> Optional[float]:
        if self.entry_price is None or self.exit_price is None:
            return None
        raw = self.exit_price - self.entry_price
        return raw if self.side == "CALL" else -raw


def target_30_3_1_exit(entry_ts, entry_price, side, bars_1m, bars_htf, bars_trap, ltf_min):
    post_entry_1m = [b for b in bars_1m if b.ts >= entry_ts]

    trigger_ts = None
    for i in range(1, len(bars_htf)):
        cur, prev = bars_htf[i], bars_htf[i - 1]
        if cur.ts < entry_ts:
            continue
        if side == "CALL" and cur.low < prev.low:
            trigger_ts = cur.ts
            break
        if side == "PUT" and cur.high > prev.high:
            trigger_ts = cur.ts
            break

    if trigger_ts is None:
        if post_entry_1m:
            last = post_entry_1m[-1]
            return last.ts, last.close, "eod_close", None, None
        return entry_ts, entry_price, "no_data_after_entry", None, None

    zones = screener.bull_trap_zones(bars_trap) if side == "CALL" else screener.sharp_bear_zones(bars_trap)
    zone_touched_ts = None
    calc = None
    fed = 0
    ltf_bars = bars_1m if ltf_min == 1 else to_n_min_bars(bars_1m, ltf_min)

    for b in post_entry_1m:
        if b.ts < trigger_ts:
            continue
        if zone_touched_ts is None:
            for z in zones:
                if z["lock_ts"] is None or z["lock_ts"] > b.ts:
                    continue
                if b.low <= z["zone_hi"] and b.high >= z["zone_lo"]:
                    zone_touched_ts = b.ts
                    calc = SupportResistanceCalculator()
                    fed = 0
                    break
        if calc is not None:
            window = [x for x in ltf_bars if zone_touched_ts <= x.ts <= b.ts]
            for nb in window[fed:]:
                calc.process_straddle_candle("SYM", {"timestamp": nb.ts, "high": nb.high,
                                                       "low": nb.low, "duration": ltf_min})
            fed = len(window)
            sr = calc.get_calculated_sr_state("SYM").get("sr_levels", {})
            level = sr.get("S1") if side == "CALL" else sr.get("R1")
            if level is not None and level.get("is_established"):
                lvl = level["low"] if side == "CALL" else level["high"]
                breach = (b.low <= lvl) if side == "CALL" else (b.high >= lvl)
                if breach:
                    return b.ts, lvl, "target_hit", trigger_ts, zone_touched_ts

    if post_entry_1m:
        last = post_entry_1m[-1]
        return last.ts, last.close, "eod_close", trigger_ts, zone_touched_ts
    return entry_ts, entry_price, "no_data_after_entry", trigger_ts, zone_touched_ts


def run_one(bars_1m, side, orb_h, orb_l, vol_by_ts, htf_min=HTF_MIN, trap_tf_min=TRAP_TF_MIN, ltf_min=LTF_MIN):
    bars_htf = to_n_min_bars(bars_1m, htf_min)
    bars_trap = to_n_min_bars(bars_1m, trap_tf_min)

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
        return None

    if historically_fulfilled:
        b0 = entry_window[0]
        exit_ts, exit_price, reason, trig, touch = target_30_3_1_exit(
            b0.ts, b0.close, side, bars_1m, bars_htf, bars_trap, ltf_min)
        return (b0.ts, b0.close, exit_ts, exit_price, "immediate_historical:" + reason, trig, touch)

    breached = False
    for b in entry_window:
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        if side == "CALL" and b.low <= orb_l:
            breached = True
        elif side == "PUT" and b.high >= orb_h:
            breached = True
        vwap = vwap_state.current("SYM")
        if vwap is None:
            continue
        armed, fire = screener.check_vwap_retest_entry(side, b.close, vwap, armed, 0.15)
        if not fire:
            continue
        armed = False
        if breached:
            continue
        exit_ts, exit_price, reason, trig, touch = target_30_3_1_exit(
            b.ts, b.close, side, bars_1m, bars_htf, bars_trap, ltf_min)
        return (b.ts, b.close, exit_ts, exit_price, reason, trig, touch)
    return None


def run_all(cache, htf_min=HTF_MIN, trap_tf_min=TRAP_TF_MIN, ltf_min=LTF_MIN, candle_type="normal"):
    trades = []
    ha_cache = {}
    for trade_date, symbol, side_bias in ROWS:
        side = "CALL" if side_bias == "bullish" else "PUT"
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l = cached
        if candle_type == "heikin_ashi":
            key = (trade_date, symbol)
            if key not in ha_cache:
                ha_bars = to_heikin_ashi(bars_1m)
                ha_orb = compute_orb(ha_bars)
                ha_cache[key] = (ha_bars, ha_orb)
            bars_1m, ha_orb = ha_cache[key]
            if ha_orb is None:
                continue
            orb_h, orb_l = ha_orb
        result = run_one(bars_1m, side, orb_h, orb_l, vol_by_ts, htf_min, trap_tf_min, ltf_min)
        if result is None:
            continue
        entry_ts, entry_price, exit_ts, exit_price, reason, trig, touch = result
        trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price, exit_ts, exit_price, reason, trig, touch))
    return trades


def summarize(label, trades):
    entered = [t for t in trades if t.entry_price is not None]
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    triggered = sum(1 for t in entered if t.trigger_ts is not None)
    touched = sum(1 for t in entered if t.zone_touched_ts is not None)
    target_hits = sum(1 for t in entered if t.reason.endswith("target_hit"))
    print(f"{label:>10}  entered={len(entered):2d}  30m-trig={triggered:2d}  zone-touch={touched:2d}  target_hit={target_hits:2d}  "
          f"win%={win_pct:5.1f}  PF={pf:6.2f}  total={total:+9.2f}")
    return {"trades": trades, "total": total, "pf": pf, "win_pct": win_pct, "entered": len(entered)}


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows (real Upstox 1-min NSE_EQ history)...")
    cache = await fetch_all()

    trades = run_all(cache)

    print("\n" + "=" * 130)
    print("30m TRIGGER -> 3m TRAP ZONE -> 1m S&R BREACH TARGET, no SL (CALL + PUT mirrored) -- trade log")
    print("=" * 130)
    for t in sorted(trades, key=lambda x: (x.date, x.symbol)):
        trig_s = t.trigger_ts.strftime("%H:%M") if t.trigger_ts else "never"
        touch_s = t.zone_touched_ts.strftime("%H:%M") if t.zone_touched_ts else "never"
        print(f"  {t.date} {t.symbol:<12} {t.side:<4} entry={t.entry_price:9.2f}@{t.entry_ts.strftime('%H:%M')} "
              f"30m-trig={trig_s:>5} zone-touch={touch_s:>5} "
              f"exit={t.exit_price:9.2f}@{t.exit_ts.strftime('%H:%M')} ({t.reason})  pts={t.points:+8.2f}")

    print()
    summarize("CALL only", [t for t in trades if t.side == "CALL"])
    summarize("PUT only", [t for t in trades if t.side == "PUT"])
    summarize("COMBINED", trades)


if __name__ == "__main__":
    asyncio.run(main())
