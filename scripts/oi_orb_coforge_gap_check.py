"""
scripts/oi_orb_coforge_gap_check.py

Direct user follow-up: "check why COFORGE's direction call went wrong."
The raw session-movement script showed COFORGE's real 09-09 morning net
was +2.73% (bullish) while the mechanic entered PUT at 09:23 -- a real
directional mismatch. This checks the exact real prev_close and the
first few real 1-min bars to confirm (or correct) the working theory:
a real GAP-DOWN at the open (legitimately triggering PUT off the very
first bar, per the mechanic's own real logic) followed by a full
intraday reversal/recovery -- not a logic bug, a real gap-fade pattern.

MUST run on EC2 (real Upstox account access tokens).

Usage: python scripts/oi_orb_coforge_gap_check.py
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer import historical_candles as hc
from data_layer.client_db import ClientDB
from strategies.oi_orb_screener import stock_resolve

TRADE_DATE = date.fromisoformat("2026-09-09")


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


async def _prev_close_asof(eq_key, tokens, ref_date, max_step_back=10):
    d = ref_date - timedelta(days=1)
    for _ in range(max_step_back):
        if d.weekday() < 5:
            rows = await hc.fetch_upstox_range_1m_multi_account(eq_key, tokens, d, d)
            if rows:
                return float(rows[-1]["close"]), d
        d -= timedelta(days=1)
    return None, None


async def main():
    tokens = _access_tokens()
    eq_key = stock_resolve.resolve_eq_instrument_key("COFORGE")
    print("=" * 110)
    print("COFORGE 2026-09-09 -- real gap-down-then-recovery check")
    print("=" * 110)

    prev_close, prev_date = await _prev_close_asof(eq_key, tokens, TRADE_DATE)
    print(f"Real previous trading day's close ({prev_date}): {prev_close}")

    rows = await hc.fetch_upstox_range_1m_multi_account(eq_key, tokens, TRADE_DATE, TRADE_DATE)
    from strategies.core.trap_zone_utils import Bar
    bars = []
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        bars.append(Bar(ts=ts.astimezone(IST), open=float(r["open"]), high=float(r["high"]),
                         low=float(r["low"]), close=float(r["close"])))

    first = bars[0]
    pchange_first = (first.close - prev_close) / prev_close * 100.0
    print(f"\nFirst real bar of the day: {first.ts.strftime('%H:%M')} close={first.close} "
          f"-> pChange vs prev_close = {pchange_first:+.2f}%")
    print(f"  {'GAP-OPEN trigger' if abs(pchange_first) >= 2.0 else 'no trigger at open'} "
          f"(PUT triggers if <= -2.00%)")

    print("\nFirst 20 real 1-min bars (the gap + whatever happened right after):")
    for b in bars[:20]:
        pct = (b.close - prev_close) / prev_close * 100.0
        print(f"  {b.ts.strftime('%H:%M')}  O={b.open} H={b.high} L={b.low} C={b.close}  pChange={pct:+.2f}%")

    day_low = min(b.low for b in bars)
    day_low_ts = next(b.ts for b in bars if b.low == day_low)
    print(f"\nReal day low: {day_low} @ {day_low_ts.strftime('%H:%M')}")
    print(f"Real day close: {bars[-1].close} @ {bars[-1].ts.strftime('%H:%M')}")
    print(f"Net day move vs prev_close: {(bars[-1].close - prev_close) / prev_close * 100.0:+.2f}%")
    print("=" * 110)


if __name__ == "__main__":
    asyncio.run(main())
