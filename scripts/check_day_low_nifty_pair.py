"""
scripts/check_day_low_nifty_pair.py — one-off diagnostic: what was the REAL
lowest combined (CE+PE) premium today for a given pair, from market open up
to a cutoff time, per the exact same REST-fetch + minute-alignment
methodology strategies/sell_straddle/exits.py's _seed_day_low_for_pair()
uses (CE.close + PE.close aligned by timestamp -- NOT each leg's own
independent low, since summing two legs' separate lows would combine two
price extremes that almost certainly never occurred at the same instant).

Run on EC2 (uses the real stored Upstox feeder token from data/clients.db,
no token argument needed):
    python scripts/check_day_low_nifty_pair.py NIFTY 24200 PE 24350 CE
    python scripts/check_day_low_nifty_pair.py NIFTY 24200 PE 24350 CE --cutoff 15:00
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, time as dtime

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer.client_db import ClientDB
from data_layer.historical_candles import fetch_upstox_intraday_1m
from data_layer.instrument_registry import REGISTRY


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("underlying")
    ap.add_argument("strike1", type=int)
    ap.add_argument("opt1", choices=["CE", "PE"])
    ap.add_argument("strike2", type=int)
    ap.add_argument("opt2", choices=["CE", "PE"])
    ap.add_argument("--cutoff", default="15:00", help="HH:MM, only consider bars up to this time")
    ap.add_argument("--expiry", default="", help="YYYY-MM-DD override; default = today's active expiry")
    args = ap.parse_args()

    creds = await asyncio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
    token = (creds or {}).get("access_token", "")
    if not token:
        print("No Upstox feeder token found in data/clients.db -- is the feeder authenticated?")
        return

    await asyncio.to_thread(REGISTRY.load_sync, args.underlying, token)

    today = datetime.now(IST).date()
    if args.expiry:
        from datetime import date as _date
        expiry = _date.fromisoformat(args.expiry)
    else:
        expiry = REGISTRY.get_active_expiry(args.underlying, today)
    print(f"Underlying={args.underlying} expiry={expiry} "
          f"leg1={args.strike1}{args.opt1} leg2={args.strike2}{args.opt2} cutoff={args.cutoff}")

    key1 = REGISTRY.get_broker_symbol(args.underlying, expiry, args.strike1, args.opt1, "upstox")
    key2 = REGISTRY.get_broker_symbol(args.underlying, expiry, args.strike2, args.opt2, "upstox")
    if not key1 or not key2:
        print(f"Could not resolve Upstox instrument keys (leg1={key1!r} leg2={key2!r}) -- "
              f"is InstrumentRegistry loaded? Try REGISTRY.load_sync() first if running standalone.")
        return
    print(f"Upstox keys: leg1={key1}  leg2={key2}")

    bars1, bars2 = await asyncio.gather(
        fetch_upstox_intraday_1m(key1, token),
        fetch_upstox_intraday_1m(key2, token),
    )
    if not bars1 or not bars2:
        print(f"REST fetch returned empty (leg1={len(bars1)} bars, leg2={len(bars2)} bars) -- aborting.")
        return

    cutoff_h, cutoff_m = (int(x) for x in args.cutoff.split(":"))
    cutoff = dtime(cutoff_h, cutoff_m)

    by_ts2 = {b["ts"]: float(b["close"]) for b in bars2}
    rows = []
    for b in bars1:
        ts_str = b["ts"]
        ts = datetime.fromisoformat(ts_str)
        if ts.time() > cutoff:
            continue
        if ts_str not in by_ts2:
            continue
        combined = float(b["close"]) + by_ts2[ts_str]
        rows.append((ts, float(b["close"]), by_ts2[ts_str], combined))

    if not rows:
        print("No overlapping 1-min bars between the two legs up to cutoff -- nothing to compute.")
        return

    rows.sort(key=lambda r: r[0])
    low = min(rows, key=lambda r: r[3])
    print(f"\n{len(rows)} aligned 1-min bars, {rows[0][0].strftime('%H:%M')}..{rows[-1][0].strftime('%H:%M')}")
    print(f"\nLOWEST combined premium: {low[3]:.2f} at {low[0].strftime('%H:%M')} "
          f"(leg1={low[1]:.2f} leg2={low[2]:.2f})")
    print(f"\nFull minute-by-minute (first 5 + around the low + last 5):")
    low_idx = rows.index(low)
    show_idx = set(range(0, min(5, len(rows)))) | set(range(max(0, low_idx - 3), min(len(rows), low_idx + 4))) \
        | set(range(max(0, len(rows) - 5), len(rows)))
    for i in sorted(show_idx):
        ts, c1, c2, combined = rows[i]
        marker = "  <-- LOW" if i == low_idx else ""
        print(f"  {ts.strftime('%H:%M')}  leg1={c1:8.2f}  leg2={c2:8.2f}  combined={combined:8.2f}{marker}")


if __name__ == "__main__":
    asyncio.run(main())
