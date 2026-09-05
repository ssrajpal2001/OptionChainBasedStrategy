"""
scripts/oi_orb_prevday_sl_backtest.py -- 2026-09-05, direct user spec: the
S&R ratchet mechanic (scripts/oi_orb_sr_ratchet_backtest.py) still let too
many trades ride down to a real, sizeable loss (BOSCHLTD, FORCEMOT,
EICHERMOT) before the 15-min ladder ever tightened. New SL concept,
completely different family -- anchor the stop to the PREVIOUS DAY's own
candle, not anything computed from today's opening range or intraday ATR:

  LONG (CALL):
    - prev day candle GREEN (close > open)  -> SL = prev day CLOSE
    - prev day candle RED   (close < open)  -> SL = prev day OPEN
    - breach = price trades below that level

  SHORT (PUT), mirrored:
    - prev day candle RED   (close < open)  -> SL = prev day CLOSE
    - prev day candle GREEN (close > open)  -> SL = prev day OPEN
    - breach = price trades above that level

Rationale (this is genuine prior-day structure, not a same-day artifact):
a green prior day means yesterday's CLOSE is the more significant support
(price accepted and held above it into the close) -- if today's price
gives that back, yesterday's bullish story failed. A red prior day means
yesterday's OPEN is the more significant level (price opened, then sellers
took it all day) -- yesterday's LOW/close is not a useful reference for a
NEW long today since it was already the weak end of a bad day; the open is
where the day's story started and is the more meaningful line in the sand.

Target: NOT built yet, per direct instruction ("target will be defined
later, for now target is eod") -- a trade that isn't stopped simply runs
to the last available bar of the day, exactly like every "eod_close"
reason in every prior backtest this week.

Kept from the last two rounds (not countermanded, still the working
assumption): one re-entry allowed after an SL-triggered exit; an entry
whose own price already sits through the SL level is discarded. SL breach
is checked on an intrabar TOUCH (bar low/high), not a close-confirm --
this is a real prior-day structural line (usually inches or full percent
points away, not the 09:25 tight ATR guess that motivated close-confirm
before), so it should behave like a genuine resting stop order.

Same real dataset (last 5 trading days, real Upstox 1-min NSE_EQ history)
and same VWAP-retest/historical-immediate entry timing as every OI-ORB
backtest this week -- only the SL source changes.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_prevday_sl_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_range_1m
from scripts.oi_orb_entry_mode_backtest import (
    ROWS, SIDE, ORB_START, ORB_END, ENTRY_WINDOW_END, _key_range,
    resolve_eq_key, to_bars, volume_by_ts, compute_orb,
)
from strategies.oi_orb_screener import screener

TOKEN = os.environ.get("UPSTOX_TOKEN", "")


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
    sl_level: float
    prev_open: float
    prev_close: float

    @property
    def points(self) -> Optional[float]:
        if self.entry_price is None or self.exit_price is None:
            return None
        raw = self.exit_price - self.entry_price
        return raw if self.side == "CALL" else -raw


async def fetch_prev_day_open_close(eq_key: str, trade_date: date, max_step_back: int = 6) -> Optional[Tuple[float, float]]:
    """Steps back one calendar day at a time (skipping weekends), returns
    (day_open, day_close) of the first day with real 1-min data -- the
    genuine previous TRADING day, holidays included."""
    d = trade_date - timedelta(days=1)
    tried = 0
    while tried < max_step_back:
        if d.weekday() < 5:
            rows = await fetch_upstox_range_1m(eq_key, TOKEN, d, d)
            if rows:
                bars = to_bars(rows)
                return bars[0].open, bars[-1].close
            tried += 1
        d -= timedelta(days=1)
    return None


def prev_day_sl_exit(entry_ts, entry_price, side, sl_level, bars_1m):
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]
    for b in post_entry:
        breach = (b.low <= sl_level) if side == "CALL" else (b.high >= sl_level)
        if breach:
            return b.ts, sl_level, "prev_day_sl"
    if post_entry:
        last = post_entry[-1]
        return last.ts, last.close, "eod_close"
    return entry_ts, entry_price, "no_data_after_entry"


def sl_level_for(side: str, prev_open: float, prev_close: float) -> float:
    prev_green = prev_close > prev_open
    if side == "CALL":
        return prev_close if prev_green else prev_open
    else:
        prev_red = prev_close < prev_open
        return prev_close if prev_red else prev_open


def run_prevday_strategy(bars_1m, side, orb_h, orb_l, vol_by_ts, sl_level):
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
            exit_ts, exit_price, reason = prev_day_sl_exit(b0.ts, b0.close, side, sl_level, bars_1m)
            trades.append((b0.ts, b0.close, exit_ts, exit_price, "immediate_historical:" + reason))
            in_position_until = exit_ts
            if reason != "prev_day_sl":
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
        exit_ts, exit_price, reason = prev_day_sl_exit(entry_ts, entry_price, side, sl_level, bars_1m)
        trades.append((entry_ts, entry_price, exit_ts, exit_price, reason))
        in_position_until = exit_ts
        if reason != "prev_day_sl":
            day_done = True
        idx += 1

    return [(t[0], t[1], t[2], t[3], t[4], i + 1) for i, t in enumerate(trades)]


async def fetch_all_with_prevday():
    cache = {}
    prevday_cache: Dict[Tuple[str, str], Tuple[float, float]] = {}
    for trade_date, symbol, side_bias in ROWS:
        key = (trade_date, symbol)
        if key in cache:
            continue
        eq_key = resolve_eq_key(symbol)
        if eq_key is None:
            cache[key] = None
            continue
        d = date.fromisoformat(trade_date)
        rows = await fetch_upstox_range_1m(eq_key, TOKEN, d, d)
        if not rows:
            cache[key] = None
            continue
        bars_1m = to_bars(rows)
        vol_by_ts = volume_by_ts(rows)
        orb = compute_orb(bars_1m)
        if orb is None:
            cache[key] = None
            continue
        prevday = await fetch_prev_day_open_close(eq_key, d)
        if prevday is None:
            cache[key] = None
            continue
        orb_h, orb_l = orb
        cache[key] = (bars_1m, vol_by_ts, orb_h, orb_l, prevday[0], prevday[1])
    print(f"Fetched {sum(1 for v in cache.values() if v)} usable rows (incl. prev-day OHLC).")
    return cache


def run_all(cache):
    trades = []
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l, prev_open, prev_close = cached
        sl_level = sl_level_for(side, prev_open, prev_close)
        for (entry_ts, entry_price, exit_ts, exit_price, reason, n) in run_prevday_strategy(
                bars_1m, side, orb_h, orb_l, vol_by_ts, sl_level):
            trades.append(Trade(trade_date, symbol, side, n, entry_ts, entry_price, exit_ts, exit_price,
                                 reason, sl_level, prev_open, prev_close))
    return trades


def summarize(trades):
    entered = [t for t in trades if t.entry_price is not None]
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    print(f"\nentered={len(entered)}  win%={win_pct:.1f}  PF={pf:.2f}  total={total:+.2f} pts  "
          f"avg/trade={(total/len(entered) if entered else 0):+.2f}")
    return {"trades": trades, "total": total, "pf": pf, "win_pct": win_pct, "entered": len(entered)}


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows + previous-day OHLC (real Upstox 1-min NSE_EQ history)...")
    cache = await fetch_all_with_prevday()

    trades = run_all(cache)

    print("\n" + "=" * 130)
    print("PREV-DAY CANDLE SL (green->prev close / red->prev open, mirrored for PUT), target=EOD -- trade log")
    print("=" * 130)
    for t in sorted(trades, key=lambda x: (x.date, x.symbol, x.entry_ts or 0)):
        if t.entry_price is None:
            print(f"  {t.date} {t.symbol:<12} {t.side:<4} NO ENTRY")
            continue
        prev_color = "GREEN" if t.prev_close > t.prev_open else "RED"
        print(f"  {t.date} {t.symbol:<12} {t.side:<4} #{t.n} prevday={prev_color:<5} SL={t.sl_level:9.2f}  "
              f"entry={t.entry_price:9.2f}@{t.entry_ts.strftime('%H:%M')} "
              f"exit={t.exit_price:9.2f}@{t.exit_ts.strftime('%H:%M')} ({t.reason})  pts={t.points:+8.2f}")

    summarize(trades)


asyncio.run(main())
