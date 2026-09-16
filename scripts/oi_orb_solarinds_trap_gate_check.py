"""
scripts/oi_orb_solarinds_trap_gate_check.py

Direct user spec, 2026-09-16: "note there is another entry logic part --
if when trade comes we check for HTF trap, if in intraday that HTF zone
is already being touched we don't take trade in that stock. I think that
happened with SOLARINDS." Checks the REAL pre-entry gate
(OiOrbScreenerStrategy._check_trap_target_touched_today, added
2026-09-16, currently LIVE -- runs the instant a VWAP-retest fires,
before the trade is actually taken) against SOLARINDS on 2026-09-07,
side=CALL, entry_ts=11:17 -- the exact real trade from the 7-day
frozen-mechanic backtest.

Faithfully replicates the real gate's own logic (NOT the same as the
earlier exit-side trap-target script's overlap check -- this one checks
bar CLOSE inside [zone_lo, zone_hi], matching
_check_trap_target_touched_today's own `zone_lo <= b.close <= zone_hi`
exactly) -- multi-day 180-min HTF zones (15 real calendar days),
most-recently-locked zone as of the entry moment, touched-today check
from market open through entry_ts. Read-only, no writes.

MUST run on EC2 (real Upstox2 access token + real 15-day history).

Usage: python scripts/oi_orb_solarinds_trap_gate_check.py
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime, time as dtime, timedelta

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer import historical_candles as hc
from data_layer.client_db import ClientDB
from strategies.core.trap_zone_utils import Bar
from strategies.core.candle_indicators import to_n_min_bars_dateaware
from strategies.oi_orb_screener import stock_resolve, screener
from strategies.oi_orb_screener.engine import (
    OiOrbScreenerStrategy, _TRAP_EXIT_HTF_MULTIDAY_MIN, _TRAP_EXIT_LOOKBACK_CALENDAR_DAYS,
)

SYMBOL = "SOLARINDS"
SIDE = "CALL"
TRADE_DATE = date.fromisoformat("2026-09-07")
ENTRY_TS = datetime(2026, 9, 7, 11, 17, tzinfo=IST)


def _access_token():
    creds = ClientDB().get_feeder_creds_sync("upstox2")
    if creds and creds.get("access_token"):
        return creds["access_token"]
    raise RuntimeError("No upstox2 feeder access_token found -- run this on EC2.")


def _to_bars(rows):
    out = []
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        out.append(Bar(ts=ts.astimezone(IST), open=float(r["open"]), high=float(r["high"]),
                        low=float(r["low"]), close=float(r["close"])))
    return out


async def main():
    token = _access_token()
    print("=" * 120)
    print(f"OI-ORB Screener -- real pre-entry TRAP-TARGET-ALREADY-TOUCHED gate check, {SYMBOL} "
          f"{TRADE_DATE.isoformat()} side={SIDE} entry_ts={ENTRY_TS.strftime('%H:%M')}")
    print("=" * 120)

    eq_key = stock_resolve.resolve_eq_instrument_key(SYMBOL)
    start = TRADE_DATE - timedelta(days=_TRAP_EXIT_LOOKBACK_CALENDAR_DAYS)
    prior_rows = await hc.fetch_upstox_range_1m(eq_key, token, start, TRADE_DATE)
    seen, all_rows = set(), []
    for r in sorted(prior_rows or [], key=lambda r: r["ts"]):
        if r["ts"] in seen:
            continue
        seen.add(r["ts"])
        all_rows.append(r)
    bars = _to_bars(all_rows)
    print(f"Real bars fetched: {len(bars)} (from {start} through {TRADE_DATE}, {_TRAP_EXIT_LOOKBACK_CALENDAR_DAYS} "
          f"calendar days lookback)")

    htf = to_n_min_bars_dateaware(bars, _TRAP_EXIT_HTF_MULTIDAY_MIN)
    print(f"180-min multi-day HTF bars: {len(htf)}")
    if len(htf) < 3:
        print("Too few HTF bars -- gate would return (False, None), entry proceeds normally.")
        return

    zones_fn = screener.bull_trap_zones if SIDE == "CALL" else screener.sharp_bear_zones
    zones_all = zones_fn(htf)
    print(f"\nAll real zones found ({len(zones_all)}):")
    for z in zones_all:
        print(f"  zone=[{z['zone_lo']:.2f}, {z['zone_hi']:.2f}]  ref_ts={z['ref_ts']}  lock_ts={z['lock_ts']}")

    # Most-recently-locked zone AS OF entry_ts (matches the live gate's own
    # datetime.now(IST) at the moment the VWAP-retest fires).
    zone = OiOrbScreenerStrategy._latest_locked_zone(zones_all, ENTRY_TS)
    if zone is None:
        print("\nNo zone locked by entry_ts -- gate would return (False, None), entry proceeds normally.")
        return
    print(f"\nMost-recently-locked zone as of {ENTRY_TS.strftime('%H:%M')}: "
          f"[{zone['zone_lo']:.2f}, {zone['zone_hi']:.2f}]  locked={zone['lock_ts']}")

    today_start = datetime.combine(TRADE_DATE, dtime.min, tzinfo=IST)
    today_bars_up_to_entry = [b for b in bars if today_start <= b.ts <= ENTRY_TS]
    print(f"\nReal bars from today's market open through entry_ts ({ENTRY_TS.strftime('%H:%M')}): "
          f"{len(today_bars_up_to_entry)}")

    touches = [b for b in today_bars_up_to_entry
               if b.ts >= zone["lock_ts"] and zone["zone_lo"] <= b.close <= zone["zone_hi"]]
    print(f"\nBars whose CLOSE fell inside [{zone['zone_lo']:.2f}, {zone['zone_hi']:.2f}] "
          f"(zone_lo <= close <= zone_hi), at/after lock_ts, before entry:")
    if touches:
        for b in touches[:10]:
            print(f"  {b.ts.strftime('%H:%M')}  close={b.close:.2f}")
        if len(touches) > 10:
            print(f"  ... and {len(touches) - 10} more")
    else:
        print("  NONE")

    touched = len(touches) > 0
    print("\n" + "=" * 120)
    print(f"RESULT: touched_today = {touched}")
    if touched:
        print(f"Under the REAL live gate (active since 2026-09-16), this SOLARINDS CALL entry at "
              f"{ENTRY_TS.strftime('%H:%M')} on {TRADE_DATE.isoformat()} would have been SKIPPED entirely -- "
              f"the -55.95 loss in the backtest is an ARTIFACT of not modeling this gate, not a real possible "
              f"outcome under the current live system.")
    else:
        print("Under the real live gate, this entry would NOT have been skipped -- the trap-target zone had "
              "genuinely not been touched yet by entry time. The -55.95 loss stands as a real possible "
              "outcome even with this gate active.")
    print("=" * 120)


if __name__ == "__main__":
    asyncio.run(main())
