"""
scripts/oi_orb_strike_comparison.py — 2026-08-28, direct user request: for
each of today's real OI-ORB Screener signals, replay the SAME entry timestamp
and the SAME SL/target mechanic (screener.pool_sl_from_adverse_lows +
compute_option_premium_target, the same code the live engine uses) against
SEVERAL NEARBY STRIKES' real intraday premium -- not just the one strike the
screener actually traded -- to see whether a different strike selection would
have produced a better outcome for these exact same real signals.

Real intraday data only (Upstox's own /historical-candle/intraday endpoint,
TODAY only -- no synthetic premium). Strikes are the real ones listed on the
exchange (via InstrumentRegistry.get_available_strikes), taken as a window
around each trade's ACTUALLY-traded strike (3 closer-to-ATM / more ITM, the
traded one, and further OTM ones).
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from datetime import date, datetime, time as dtime
from typing import List, Optional

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer.historical_candles import fetch_upstox_intraday_1m
from data_layer.instrument_registry import REGISTRY
from strategies.oi_orb_screener import screener

TOKEN = sys.argv[1] if len(sys.argv) > 1 else ""
RR_MULTIPLE = 2.0
VWAP_SL_TF_MINUTES = 5

# (symbol, option_type, actual_traded_strike, entry_time_HHMM, actual_entry_premium, qty)
REAL_TRADES = [
    ("COFORGE", "CE", 2000, "09:51", 55.00, 475),
    ("KPITTECH", "CE", 620, "09:55", 15.45, 775),
    ("OFSS", "CE", 12400, "09:55", 366.50, 100),
    ("LTM", "CE", 4750, "11:30", 103.00, 150),
    ("SAGILITY", "CE", 47, "12:15", 1.50, 12000),
    ("TVSMOTOR", "PE", 4150, "13:16", 60.30, 175),
]


@dataclass
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float


def to_5min_bars(bars_1m: List[Bar]) -> List[Bar]:
    buckets = {}
    for b in bars_1m:
        floored = (b.ts.minute // VWAP_SL_TF_MINUTES) * VWAP_SL_TF_MINUTES
        key = (b.ts.hour, floored)
        buckets.setdefault(key, []).append(b)
    out = []
    for key in sorted(buckets.keys()):
        g = sorted(buckets[key], key=lambda x: x.ts)
        out.append(Bar(ts=g[0].ts, open=g[0].open, high=max(x.high for x in g),
                        low=min(x.low for x in g), close=g[-1].close))
    return out


def simulate(bars_1m: List[Bar], entry_time: dtime, qty: int) -> dict:
    """Same mechanic as engine.py's _update_option_sl_target_and_check, but
    self-contained (VWAP = a running cumulative typical-price*volume-less
    proxy isn't available intraday without volume, so this uses the SAME
    "self-computed VWAP as backfill proxy" pattern already used elsewhere in
    this codebase -- cumulative mean of (h+l+c)/3, no volume weighting,
    since Upstox's intraday response for these small-cap stocks doesn't
    reliably carry volume on every bar)."""
    entry_bars = [b for b in bars_1m if b.ts.time() >= entry_time]
    if not entry_bars:
        return {"error": "no bars at/after entry time"}
    entry_price = entry_bars[0].open

    bars_5m = to_5min_bars(entry_bars)
    if not bars_5m:
        return {"error": "no 5min bars"}

    cum_sum = 0.0
    cum_n = 0
    adverse_lows: List[float] = []
    live_sl: Optional[float] = None
    live_target: Optional[float] = None

    for i, b5 in enumerate(bars_5m):
        typical = (b5.high + b5.low + b5.close) / 3.0
        cum_sum += typical
        cum_n += 1
        vwap_at_close = cum_sum / cum_n

        if screener.is_adverse_bar_close(b5.close, vwap_at_close):
            adverse_lows.append(b5.low)
            new_sl = screener.pool_sl_from_adverse_lows(adverse_lows)
            if new_sl is not None and new_sl != live_sl:
                live_sl = new_sl
                new_target = screener.compute_option_premium_target(entry_price, live_sl, RR_MULTIPLE)
                if new_target is not None:
                    live_target = new_target

        # check the NEXT 1-min bars inside/after this 5min bucket for a live breach
        next_bars = [b for b in entry_bars if b.ts > b5.ts and
                     (i + 1 >= len(bars_5m) or b.ts < bars_5m[i + 1].ts)]
        for nb in next_bars:
            hit = screener.check_option_premium_exit(live_sl, live_target, nb.close)
            if hit is not None:
                exit_price = live_sl if hit == "sl" else live_target
                pnl = (exit_price - entry_price) * qty
                return {"entry_price": entry_price, "exit_price": exit_price, "exit_reason": hit,
                        "exit_ts": nb.ts.strftime("%H:%M"), "pnl_rs": round(pnl, 2), "sl": live_sl,
                        "target": live_target}

    # never hit SL/target -- EOD at last available premium
    last = entry_bars[-1]
    pnl = (last.close - entry_price) * qty
    return {"entry_price": entry_price, "exit_price": last.close, "exit_reason": "eod",
            "exit_ts": last.ts.strftime("%H:%M"), "pnl_rs": round(pnl, 2), "sl": live_sl,
            "target": live_target}


async def fetch_premium(upstox_key: str) -> List[Bar]:
    rows = await fetch_upstox_intraday_1m(upstox_key, TOKEN)
    return [Bar(ts=datetime.fromisoformat(r["ts"]), open=r["open"], high=r["high"],
                low=r["low"], close=r["close"]) for r in sorted(rows, key=lambda x: x["ts"])]


async def main():
    for symbol, opt_type, actual_strike, entry_hhmm, actual_premium, qty in REAL_TRADES:
        print(f"\n{'=' * 70}")
        print(f"{symbol} {opt_type} (actually traded {actual_strike}, entry {entry_hhmm} @ {actual_premium})")
        print(f"{'=' * 70}")

        if not REGISTRY.is_loaded(symbol):
            REGISTRY.load_sync(symbol)
        expiry = REGISTRY.get_active_expiry(symbol)
        if expiry is None:
            print("  could not resolve active expiry -- skipping.")
            continue
        available = sorted(REGISTRY.get_available_strikes(symbol, expiry, opt_type))
        if actual_strike not in available:
            print(f"  actual strike {actual_strike} not in resolved list -- using nearest.")
            actual_strike = min(available, key=lambda s: abs(s - actual_strike))
        idx = available.index(actual_strike)

        # window: 3 strikes more ITM, the actual one, 3 more OTM
        if opt_type == "CE":
            window = available[max(0, idx - 3): idx + 4]
        else:
            window = available[max(0, idx - 3): idx + 4]

        entry_t = dtime(*map(int, entry_hhmm.split(":")))
        for strike in window:
            upstox_key = REGISTRY.get_upstox_key(symbol, expiry, strike, opt_type)
            if not upstox_key:
                print(f"  {strike:>8}: no upstox_key -- skip")
                continue
            bars = await fetch_premium(upstox_key)
            if not bars:
                print(f"  {strike:>8}: no premium data -- skip")
                continue
            result = simulate(bars, entry_t, qty)
            tag = "  <-- ACTUALLY TRADED" if strike == actual_strike else ""
            if "error" in result:
                print(f"  {strike:>8}: {result['error']}{tag}")
                continue
            print(f"  {strike:>8}: entry={result['entry_price']:>8.2f} "
                  f"exit={result['exit_price']:>8.2f} ({result['exit_reason']:<6}@{result['exit_ts']}) "
                  f"pnl=Rs{result['pnl_rs']:>10,.2f}{tag}")
            await asyncio.sleep(0.2)


if __name__ == "__main__":
    asyncio.run(main())
