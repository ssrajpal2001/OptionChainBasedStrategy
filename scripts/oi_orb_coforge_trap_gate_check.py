"""
scripts/oi_orb_coforge_trap_gate_check.py

Direct user follow-up: "did u checked that before taking trade did it
entered the bear HTF zone?? for bearish we check bear HTF and for bull
we check bull HTF trap zone." COFORGE's PUT entry on 2026-09-09
(09:23) was NOT flagged trap_gate_skipped in the 7-day backtest --
meaning the gate ran and found the real bear zone (screener.
sharp_bear_zones, since side=PUT) not yet touched by entry time. This
verifies that pass/fail with real numbers directly, same exact logic
already proven against SOLARINDS
(oi_orb_solarinds_trap_gate_check.py), retargeted here.

MUST run on EC2 (real Upstox account access tokens + real 15-day
history).

Usage: python scripts/oi_orb_coforge_trap_gate_check.py
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

SYMBOL = "COFORGE"
SIDE = "PUT"
TRADE_DATE = date.fromisoformat("2026-09-09")
ENTRY_TS = datetime(2026, 9, 9, 9, 23, tzinfo=IST)


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


async def main():
    tokens = _access_tokens()
    print("=" * 120)
    print(f"OI-ORB Screener -- real pre-entry TRAP-TARGET-ALREADY-TOUCHED gate check, {SYMBOL} "
          f"{TRADE_DATE.isoformat()} side={SIDE} entry_ts={ENTRY_TS.strftime('%H:%M')} "
          f"(checking the BEAR zone, since side=PUT)")
    print("=" * 120)

    eq_key = stock_resolve.resolve_eq_instrument_key(SYMBOL)
    start = TRADE_DATE - timedelta(days=_TRAP_EXIT_LOOKBACK_CALENDAR_DAYS)
    rows = await hc.fetch_upstox_range_1m_multi_account(eq_key, tokens, start, TRADE_DATE)
    seen, all_rows = set(), []
    for r in sorted(rows or [], key=lambda r: r["ts"]):
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
    print(f"\nAll real BEAR zones found ({len(zones_all)}):")
    for z in zones_all:
        print(f"  zone=[{z['zone_lo']:.2f}, {z['zone_hi']:.2f}]  ref_ts={z['ref_ts']}  lock_ts={z['lock_ts']}")

    zone = OiOrbScreenerStrategy._latest_locked_zone(zones_all, ENTRY_TS)
    if zone is None:
        print("\nNo zone locked by entry_ts -- gate would return (False, None), entry proceeds normally.")
        return
    print(f"\nMost-recently-locked BEAR zone as of {ENTRY_TS.strftime('%H:%M')}: "
          f"[{zone['zone_lo']:.2f}, {zone['zone_hi']:.2f}]  locked={zone['lock_ts']}")

    today_start = datetime.combine(TRADE_DATE, dtime.min, tzinfo=IST)
    today_bars_up_to_entry = [b for b in bars if today_start <= b.ts <= ENTRY_TS]
    print(f"\nReal bars from today's market open through entry_ts ({ENTRY_TS.strftime('%H:%M')}): "
          f"{len(today_bars_up_to_entry)}")

    touches_close = [b for b in today_bars_up_to_entry
                     if b.ts >= zone["lock_ts"] and zone["zone_lo"] <= b.close <= zone["zone_hi"]]
    print(f"\n[AS CURRENTLY CODED, real live gate] Bars whose CLOSE fell inside "
          f"[{zone['zone_lo']:.2f}, {zone['zone_hi']:.2f}], at/after lock_ts, before entry:")
    if touches_close:
        for b in touches_close[:10]:
            print(f"  {b.ts.strftime('%H:%M')}  close={b.close:.2f}")
    else:
        print("  NONE")

    # 2026-09-16, direct user correction: a BEAR zone should be checked
    # against the candle's own LOW (the wick reaching down into the
    # zone), a BULL zone against the candle's own HIGH -- not close.
    # This differs from _check_trap_target_touched_today's actual coded
    # behavior (close-based) -- checked here as a real, separate
    # definition to see whether it changes the real conclusion.
    extreme_field = "low" if SIDE == "PUT" else "high"
    touches_wick = [b for b in today_bars_up_to_entry
                     if b.ts >= zone["lock_ts"]
                     and zone["zone_lo"] <= getattr(b, extreme_field) <= zone["zone_hi"]]
    print(f"\n[USER-CORRECTED definition] Bars whose {extreme_field.upper()} fell inside "
          f"[{zone['zone_lo']:.2f}, {zone['zone_hi']:.2f}], at/after lock_ts, before entry:")
    if touches_wick:
        for b in touches_wick[:10]:
            print(f"  {b.ts.strftime('%H:%M')}  {extreme_field}={getattr(b, extreme_field):.2f}")
    else:
        print("  NONE")
    touched_close = len(touches_close) > 0
    touched_wick = len(touches_wick) > 0
    print("\n" + "=" * 120)
    print(f"RESULT (as coded, CLOSE-based): touched_today = {touched_close}")
    print(f"RESULT (user-corrected, {extreme_field.upper()}-based): touched_today = {touched_wick}")
    if touched_wick and not touched_close:
        print(f"\nDISCREPANCY: under the real live (close-based) code this entry was allowed through, but "
              f"under the corrected ({extreme_field}-based) definition it should have been SKIPPED -- "
              f"a real candidate bug in _check_trap_target_touched_today's own use of b.close instead of "
              f"b.{extreme_field}.")
    elif touched_wick:
        print(f"\nBoth definitions agree this entry should have been SKIPPED.")
    else:
        print(f"\nBoth definitions agree: the real bear-zone target had genuinely NOT been touched yet by "
              f"entry time -- the gate correctly let this trade through either way. The -13.70 loss is a "
              f"real, legitimate outcome of the gap-down-then-recovery pattern, not a gate failure.")
    print("=" * 120)


if __name__ == "__main__":
    asyncio.run(main())
