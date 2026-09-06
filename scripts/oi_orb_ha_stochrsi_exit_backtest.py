"""
scripts/oi_orb_ha_stochrsi_exit_backtest.py -- 2026-09-05, direct user
spec, a fresh exit criterion replacing the 30m-trigger/trap/LTF-S&R
mechanic (entry timing unchanged, still VWAP-retest / historical-
immediate / no-reentry / breach-cancel):

  LONG (CALL) exit: a 15-min HEIKIN-ASHI candle is "bearish type" --
  literally HA_high == HA_open (no upper wick at all, i.e. the real
  price never traded above the HA candle's own opening average during
  that 15-min bar) -- AND 15-min StochRSI %D > %K on that same bar.

  SHORT (PUT) exit, mirrored: HA_low == HA_open (a "bullish type" HA
  candle, no lower wick) AND %K > %D.

  No SL. Falls back to EOD if the condition never fires.

The HA_high==HA_open / HA_low==HA_open checks are EXACT float equality,
not a fragile rounding coincidence -- by construction, to_heikin_ashi's
`ha_high = max(b.high, ha_open, ha_close)` returns the ha_open value
completely unchanged (not computed via subtraction) whenever ha_open is
the winning branch, so `==` is a correct, safe check here.

The HA-candle-shape + StochRSI exit signal is always computed off a
FRESH 15-min Heikin-Ashi series built from the real 1-min bars --
"Heikin Ashi candle" in the exit criterion is a fixed part of the rule,
not toggled by the run's own candle-type choice. What DOES vary between
the two backtest runs below (per direct instruction "provide backtest
for normal and heik candle") is which series the ENTRY timing and P&L
tracking itself uses -- real OHLC bars, or the same Heikin-Ashi-
throughout pipeline already built in oi_orb_30_3_1_target_backtest.py
(HA also used for VWAP's own "typical price" input, ORB, entry price).

StochRSI params: rsi_period=9, stoch_period=9, smooth=3 -- NOT the
default 14/14 from the earlier StochRSI backtest. Chosen deliberately
for the fastest possible warm-up: a single trading day only has ~22
15-min bars (09:15-15:30), and 14+14+3 was already shown (scripts/
oi_orb_stoch_rsi_backtest.py) to never finish warming up intraday at
15-min. 9+9+3 needs fewer bars but may still not always warm up in time
-- flagged honestly in the report, not hidden.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_ha_stochrsi_exit_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from typing import List, Optional

sys.path.insert(0, ".")

from scripts.oi_orb_entry_mode_backtest import (
    ROWS, ORB_START, ORB_END, ENTRY_WINDOW_END, _key_range, to_n_min_bars, compute_orb,
)
from scripts.oi_orb_30_3_1_target_backtest import to_heikin_ashi
from scripts.oi_orb_stoch_rsi_backtest import compute_stoch_rsi
from strategies.core.candle_indicators import ha_stoch_shape_exit_signal
from scripts.oi_orb_atr_chandelier_backtest import fetch_all
from strategies.oi_orb_screener import screener

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
EXIT_TF_MIN = 15
RSI_PERIOD = 9
STOCH_PERIOD = 9
SMOOTH = 3


@dataclass
class Trade:
    date: str
    symbol: str
    side: str
    candle_type: str
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


def ha_stoch_exit(entry_ts, entry_price, side, bars_1m, ha_15m, k, d, inclusive=False):
    """inclusive=False: strict %D > %K (CALL) / %K > %D (PUT), the
    original spec. inclusive=True: %D >= %K / %K >= %D -- direct user
    follow-up ("also run the backtest where %D >= %K and vice versa"),
    letting a tie (the two lines sitting exactly on top of each other,
    which the discrete StochRSI formula can genuinely produce) count as
    a confirmed exit instead of requiring one to have already pulled
    strictly ahead."""
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]

    for i, hb in enumerate(ha_15m):
        if hb.ts < entry_ts:
            continue
        if ha_stoch_shape_exit_signal(hb, k[i], d[i], side, inclusive=inclusive):
            candidates = [b for b in post_entry if b.ts >= hb.ts]
            if candidates:
                exit_bar = candidates[0]
                return exit_bar.ts, exit_bar.close, "ha_stoch_exit"

    if post_entry:
        last = post_entry[-1]
        return last.ts, last.close, "eod_close"
    return entry_ts, entry_price, "no_data_after_entry"


def run_one(bars_1m, side, orb_h, orb_l, vol_by_ts, ha_15m, k, d, inclusive=False):
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
        exit_ts, exit_price, reason = ha_stoch_exit(b0.ts, b0.close, side, bars_1m, ha_15m, k, d, inclusive)
        return (b0.ts, b0.close, exit_ts, exit_price, "immediate_historical:" + reason)

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
        exit_ts, exit_price, reason = ha_stoch_exit(b.ts, b.close, side, bars_1m, ha_15m, k, d, inclusive)
        return (b.ts, b.close, exit_ts, exit_price, reason)
    return None


def run_all(cache, candle_type, exit_tf_min=EXIT_TF_MIN, rsi_period=RSI_PERIOD, stoch_period=STOCH_PERIOD,
            inclusive=False):
    trades = []
    ha_entry_cache = {}
    ha_signal_cache = {}
    for trade_date, symbol, side_bias in ROWS:
        side = "CALL" if side_bias == "bullish" else "PUT"
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        real_bars_1m, vol_by_ts, orb_h, orb_l = cached

        # exit signal: ALWAYS a fresh HA-exit_tf + StochRSI series off the real bars
        sig_key = (trade_date, symbol)
        if sig_key not in ha_signal_cache:
            ha_signal_cache[sig_key] = to_heikin_ashi(real_bars_1m)
        ha_1m_signal = ha_signal_cache[sig_key]
        ha_15m = to_n_min_bars(ha_1m_signal, exit_tf_min)
        k, d = compute_stoch_rsi([b.close for b in ha_15m], rsi_period, stoch_period, SMOOTH)

        # entry/price-tracking series: real or HA, per candle_type
        if candle_type == "heikin_ashi":
            key = (trade_date, symbol)
            if key not in ha_entry_cache:
                ha_entry_cache[key] = (ha_1m_signal, compute_orb(ha_1m_signal))
            bars_1m, ha_orb = ha_entry_cache[key]
            if ha_orb is None:
                continue
            orb_h, orb_l = ha_orb
        else:
            bars_1m = real_bars_1m

        result = run_one(bars_1m, side, orb_h, orb_l, vol_by_ts, ha_15m, k, d, inclusive)
        if result is None:
            continue
        entry_ts, entry_price, exit_ts, exit_price, reason = result
        trades.append(Trade(trade_date, symbol, side, candle_type, entry_ts, entry_price, exit_ts, exit_price, reason))
    return trades


def summarize(label, trades):
    entered = [t for t in trades if t.entry_price is not None]
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    exit_hits = sum(1 for t in entered if t.reason.endswith("ha_stoch_exit"))
    print(f"{label:>14}  entered={len(entered):2d}  ha_stoch_exit={exit_hits:2d}  win%={win_pct:5.1f}  "
          f"PF={pf:6.2f}  total={total:+9.2f}  avg={((total/len(entered)) if entered else 0):+7.2f}")
    return {"trades": trades, "total": total, "pf": pf, "win_pct": win_pct, "entered": len(entered)}


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows (real Upstox 1-min NSE_EQ history)...")
    cache = await fetch_all()

    for candle_type in ("normal", "heikin_ashi"):
        trades = run_all(cache, candle_type)
        print(f"\n{'='*130}\nCANDLE TYPE = {candle_type}\n{'='*130}")
        for t in sorted(trades, key=lambda x: (x.date, x.symbol)):
            print(f"  {t.date} {t.symbol:<12} {t.side:<4} entry={t.entry_price:9.2f}@{t.entry_ts.strftime('%H:%M')} "
                  f"exit={t.exit_price:9.2f}@{t.exit_ts.strftime('%H:%M')} ({t.reason})  pts={t.points:+8.2f}")
        print()
        summarize(f"{candle_type} CALL", [t for t in trades if t.side == "CALL"])
        summarize(f"{candle_type} PUT", [t for t in trades if t.side == "PUT"])
        summarize(f"{candle_type} ALL", trades)


if __name__ == "__main__":
    asyncio.run(main())
