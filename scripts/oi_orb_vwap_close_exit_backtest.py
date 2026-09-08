"""
scripts/oi_orb_vwap_close_exit_backtest.py -- 2026-09-08, direct user spec:
"add vwap 15 min closes as exit -- if we are in short trade and HA 15 min
comes and HA candle closes above vwap that is also exit for us, vice versa
for long side."

New exit family, not previously tested anywhere in this repo. Same VWAP-
retest entry as the confirmed baseline (identical entries, same ROWS/real
Upstox 1-min data). Exit: on each closed 15-min Heikin-Ashi bar, compare its
CLOSE against the SAME running session VWAP the entry mechanic itself uses
(cumulative typical-price*volume from ORB_START) --
  LONG (CALL): exit the instant a 15m HA bar CLOSES below VWAP.
  SHORT (PUT): exit the instant a 15m HA bar CLOSES above VWAP.
No RSI/StochRSI involved at all -- VWAP has no lookback "warm-up" the way
RSI/Stoch do (it's a running cumulative average from session start), so this
exit has NO cold-start warm-up gap, independent of the prev-day-priming
question raised for the HA+StochRSI exit.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_vwap_close_exit_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from typing import List, Optional

sys.path.insert(0, ".")

from scripts.oi_orb_entry_mode_backtest import ORB_START, ORB_END, ENTRY_WINDOW_END, _key_range, to_n_min_bars
from scripts.oi_orb_atr_chandelier_backtest import fetch_all
from scripts.oi_orb_30_3_1_target_backtest import to_heikin_ashi
from strategies.oi_orb_screener import screener

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
EXIT_TF_MIN = 15


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


def _full_day_vwap_series(bars_1m, vol_by_ts):
    """Running session VWAP at each 1-min bar close, from ORB_START, matching
    VwapState's own cumulative typical-price*volume math exactly."""
    vwap_state = screener.VwapState()
    out = {}
    for b in bars_1m:
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        out[b.ts] = vwap_state.current("SYM")
    return out


def vwap_close_exit(entry_ts, entry_price, side, bars_1m, vwap_by_ts, ha_15m_full_day):
    """ha_15m_full_day: 15-min HA bars built from the WHOLE day's 1-min bars
    (not sliced to post-entry) -- bucket boundaries are absolute-clock-
    aligned ([09:15,09:30), [09:30,09:45), ...), so slicing to post-entry
    BEFORE building HA would misalign the first bucket into a partial one
    (e.g. an entry at 09:25 would wrongly treat 09:25-09:29 alone as a full
    15-min candle). Matches the same whole-day-then-filter pattern the
    original oi_orb_ha_stochrsi_exit_backtest.py's ha_stoch_exit() uses."""
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]
    for hb in ha_15m_full_day:
        if hb.ts < entry_ts:
            continue
        vwap_at_close = vwap_by_ts.get(hb.ts)
        if vwap_at_close is None:
            continue
        adverse = (hb.close < vwap_at_close) if side == "CALL" else (hb.close > vwap_at_close)
        if adverse:
            candidates = [b for b in post_entry if b.ts >= hb.ts]
            if candidates:
                exit_bar = candidates[0]
                return exit_bar.ts, exit_bar.close, "vwap_close_exit"
    if post_entry:
        last = post_entry[-1]
        return last.ts, last.close, "eod_close"
    return entry_ts, entry_price, "no_data_after_entry"


def run_one(bars_1m, side, vol_by_ts):
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

    vwap_by_ts = _full_day_vwap_series(bars_1m, vol_by_ts)
    ha_1m_full_day = to_heikin_ashi(bars_1m)
    ha_15m_full_day = to_n_min_bars(ha_1m_full_day, EXIT_TF_MIN)

    if historically_fulfilled:
        b0 = entry_window[0]
        exit_ts, exit_price, reason = vwap_close_exit(b0.ts, b0.close, side, bars_1m, vwap_by_ts, ha_15m_full_day)
        return (b0.ts, b0.close, exit_ts, exit_price, "immediate_historical:" + reason)

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
        if not fire:
            continue
        exit_ts, exit_price, reason = vwap_close_exit(b.ts, b.close, side, bars_1m, vwap_by_ts, ha_15m_full_day)
        return (b.ts, b.close, exit_ts, exit_price, reason)
    return None


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows (real Upstox 1-min NSE_EQ history)...")
    cache = await fetch_all()

    from scripts.oi_orb_entry_mode_backtest import ROWS
    trades = []
    for trade_date, symbol, side_bias in ROWS:
        side = "CALL" if side_bias == "bullish" else "PUT"
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l = cached
        result = run_one(bars_1m, side, vol_by_ts)
        if result is None:
            continue
        entry_ts, entry_price, exit_ts, exit_price, reason = result
        trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price, exit_ts, exit_price, reason))

    for t in sorted(trades, key=lambda x: (x.date, x.symbol)):
        print(f"  {t.date} {t.symbol:<12} {t.side:<4} entry={t.entry_price:9.2f}@{t.entry_ts.strftime('%H:%M')} "
              f"exit={t.exit_price:9.2f}@{t.exit_ts.strftime('%H:%M')} ({t.reason})  pts={t.points:+8.2f}")

    entered = [t for t in trades if t.entry_price is not None]
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    print(f"\nVWAP-close exit  entered={len(entered):2d}  win%={win_pct:5.1f}  PF={pf:6.2f}  "
          f"total={total:+9.2f}  avg={((total/len(entered)) if entered else 0):+7.2f}")

    import json
    with open("data/oi_orb_vwap_close_exit_report.json", "w") as f:
        json.dump({
            "vwap_close_exit": {
                "entered": len(entered), "win_pct": win_pct, "pf": (pf if pf != float("inf") else 9999.0),
                "total": total,
                "trades": [
                    {"date": t.date, "symbol": t.symbol, "side": t.side,
                     "entry_ts": t.entry_ts.strftime("%H:%M"), "entry_price": t.entry_price,
                     "exit_ts": t.exit_ts.strftime("%H:%M"), "exit_price": t.exit_price,
                     "reason": t.reason, "points": t.points}
                    for t in trades
                ],
            }
        }, f)
    print("Wrote data/oi_orb_vwap_close_exit_report.json")


if __name__ == "__main__":
    asyncio.run(main())
