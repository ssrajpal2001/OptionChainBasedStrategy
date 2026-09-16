"""
scripts/oi_orb_reversal_level_check.py

Direct user spec: "all 3 went high and was in our trend but reversed and
gave loss -- can u check all these and see why they reversed, was that
prev day high or resistance or fib 1.27 or weekly high or monthly high
or there would be something worth checking why they reversed from that
place -- that will help us defining the target."

Checks POWERINDIA (09-08), EICHERMOT (09-02), SOLARINDS (09-03) -- the
three real trades where a genuine peak profit reversed hard into a
worse outcome. IMPORTANT real distinction: for the two PUT trades, the
OPTION PREMIUM peak corresponds to the STOCK'S OWN LOW (a PE gains
value as spot falls), so their reversal is really "stock bottomed and
rallied back up against the PUT" -- checked against SUPPORT-type real
levels (prior lows, a downside Fib 1.272 extension), not
resistance/highs. SOLARINDS (CALL) is the mirror case -- its premium
peak corresponds to the stock's own HIGH, checked against
RESISTANCE-type levels.

For each, fetches the real underlying spot price at the option-premium
peak's own timestamp, then compares it against:
  - real previous trading day's high/low
  - real previous 5-trading-day (week) high/low
  - real previous ~22-trading-day (month) high/low
  - a Fib 1.272 extension of the PREVIOUS trading day's own real range
    (low + 1.272*(high-low) for resistance / high - 1.272*(high-low)
    for support) -- a standard, real, defensible extension definition,
    not an invented one

MUST run on EC2 (real Upstox account access tokens + real historical
range data).

Usage: python scripts/oi_orb_reversal_level_check.py
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer import historical_candles as hc
from data_layer.client_db import ClientDB
from strategies.core.trap_zone_utils import Bar
from strategies.oi_orb_screener import stock_resolve

# (symbol, trade_date, side, premium_peak_hhmm)
CASES = [
    ("POWERINDIA", "2026-09-08", "PUT", "13:39"),
    ("EICHERMOT", "2026-09-02", "PUT", "14:05"),
    ("SOLARINDS", "2026-09-03", "CALL", "14:00"),
]

WEEK_TRADING_DAYS = 5
MONTH_TRADING_DAYS = 22
FIB_EXT = 1.272


def _access_tokens():
    db = ClientDB()
    tokens = []
    for account in ("upstox2", "upstox"):
        creds = db.get_feeder_creds_sync(account)
        if creds and creds.get("access_token"):
            tokens.append(creds["access_token"])
    if not tokens:
        raise RuntimeError("No upstox/upstox2 feeder access_token found -- run this on EC2.")
    return tokens


def _to_bars(rows):
    out = []
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        out.append(Bar(ts=ts.astimezone(IST), open=float(r["open"]), high=float(r["high"]),
                        low=float(r["low"]), close=float(r["close"])))
    return out


async def analyze(tokens, symbol, trade_date_str, side, peak_hhmm):
    trade_date = date.fromisoformat(trade_date_str)
    peak_t = datetime.strptime(peak_hhmm, "%H:%M").time()
    eq_key = stock_resolve.resolve_eq_instrument_key(symbol)
    if not eq_key:
        return {"error": "NO_EQ_KEY"}

    # Real spot price at the option-premium peak's own timestamp
    day_rows = await hc.fetch_upstox_range_1m_multi_account(eq_key, tokens, trade_date, trade_date)
    day_bars = _to_bars(day_rows)
    peak_bar = min(day_bars, key=lambda b: abs((b.ts.time().hour * 60 + b.ts.time().minute) -
                                                (peak_t.hour * 60 + peak_t.minute)))
    # For PUT: the relevant spot extreme AT that bar is its LOW (stock bottoming).
    # For CALL: the relevant spot extreme AT that bar is its HIGH (stock topping).
    spot_extreme = peak_bar.low if side == "PUT" else peak_bar.high

    # Real trailing 22-trading-day window (covers prev day / week / month)
    start = trade_date - timedelta(days=45)  # generous calendar buffer for ~22 trading days
    prior_rows = await hc.fetch_upstox_range_1m_multi_account(eq_key, tokens, start, trade_date - timedelta(days=1))
    prior_bars = _to_bars(prior_rows)
    if not prior_bars:
        return {"error": "NO_PRIOR_BARS"}

    # Group by real trading date, most recent first
    by_day = {}
    for b in prior_bars:
        by_day.setdefault(b.ts.date(), []).append(b)
    trading_days = sorted(by_day.keys(), reverse=True)

    def _hi_lo(days_back_n):
        days = trading_days[:days_back_n]
        bars = [b for d in days for b in by_day[d]]
        return max(b.high for b in bars), min(b.low for b in bars)

    prev_day_hi, prev_day_lo = _hi_lo(1)
    week_hi, week_lo = _hi_lo(min(WEEK_TRADING_DAYS, len(trading_days)))
    month_hi, month_lo = _hi_lo(min(MONTH_TRADING_DAYS, len(trading_days)))

    # Fib 1.272 extension of the PREVIOUS real trading day's own range
    prev_range = prev_day_hi - prev_day_lo
    fib_resistance = prev_day_lo + FIB_EXT * prev_range
    fib_support = prev_day_hi - FIB_EXT * prev_range

    levels = {
        "prev_day_high": prev_day_hi, "prev_day_low": prev_day_lo,
        "week_high": week_hi, "week_low": week_lo,
        "month_high": month_hi, "month_low": month_lo,
        "fib_1.272_resistance": fib_resistance, "fib_1.272_support": fib_support,
    }
    # Distance of the real spot extreme from each real level, as %
    dists = {name: round((spot_extreme - lvl) / lvl * 100.0, 3) for name, lvl in levels.items()}

    return {"symbol": symbol, "date": trade_date_str, "side": side, "peak_hhmm": peak_hhmm,
            "peak_bar_ts": peak_bar.ts.strftime("%H:%M"), "spot_extreme": spot_extreme,
            "levels": levels, "dists": dists}


async def main():
    tokens = _access_tokens()
    print("=" * 130)
    print("OI-ORB Screener -- REVERSAL LEVEL check: why did POWERINDIA/EICHERMOT/SOLARINDS reverse from their peak?")
    print("PUT trades: option-premium peak = stock's own LOW (checked against SUPPORT levels).")
    print("CALL trades: option-premium peak = stock's own HIGH (checked against RESISTANCE levels).")
    print("=" * 130)

    for symbol, trade_date_str, side, peak_hhmm in CASES:
        r = await analyze(tokens, symbol, trade_date_str, side, peak_hhmm)
        print(f"\n{'-'*130}\n{symbol} ({trade_date_str}, {side}) -- option premium peaked at {peak_hhmm}\n{'-'*130}")
        if "error" in r:
            print(f"  {r['error']}")
            continue
        extreme_kind = "LOW (stock bottom, against the PUT)" if side == "PUT" else "HIGH (stock top, against nothing -- CALL peak)"
        print(f"  Real spot {extreme_kind} at {r['peak_bar_ts']}: {r['spot_extreme']}")
        print(f"\n  Real reference levels and distance from the reversal point:")
        for name, lvl in r["levels"].items():
            d = r["dists"][name]
            marker = "  <== CLOSEST" if abs(d) == min(abs(x) for x in r["dists"].values()) else ""
            print(f"    {name:22s} = {lvl:10.2f}   distance = {d:+.3f}%{marker}")

    print("\n" + "=" * 130)


if __name__ == "__main__":
    asyncio.run(main())
