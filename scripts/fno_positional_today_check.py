"""
scripts/fno_positional_today_check.py — did any of today's watchlisted FnO stocks
actually hit a real entry trigger today, using REAL market data?

fno_positional was never deployed today (confirmed 2026-08-06: no
strategy_deployments row for any client), so nothing traded -- this answers
the separate, retrospective question: if it HAD been running, would any of
today's 5 scanned stocks have actually fired an entry, based on what
genuinely happened in the market?

For each stock in data/fno_positional_watchlist.json:
  - Fetches TODAY's real 1-min intraday candles (Upstox) and walks them
    chronologically applying the EXACT same touch/gap/already-past-SL logic
    FnOPositionalBook._try_enter_approaching / _try_enter_triggered use, to
    find the first real moment (if any) an entry would have fired.
  - Fetches real trailing daily candles as a sanity check on the zone itself
    (does zone_lo/zone_hi/entry_line look consistent with recent real
    closes, or does today's data suggest the zone was already stale/blown
    before the scan even ran).
  - If an entry would have fired, also reports whether the (simulated) spot
    afterwards went on to hit T1 or hard_sl before end of day.

Run on the box with a real Upstox access_token (data/clients.db) -- e.g. EC2:
    python3 scripts/fno_positional_today_check.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_layer.client_db import ClientDB  # noqa: E402
from data_layer.historical_candles import fetch_upstox_intraday_1m, fetch_upstox_daily  # noqa: E402

WATCHLIST_PATH = Path(__file__).resolve().parents[1] / "data" / "fno_positional_watchlist.json"
GAP_SKIP_PCT = 2.5  # mirrors strategies/fno_positional/book.py


def _already_sl_side(direction: str, spot: float, hard_sl: float) -> bool:
    return (direction == "CE" and spot <= hard_sl) or (direction == "PE" and spot >= hard_sl)


def _touched(direction: str, spot: float, entry_line: float) -> bool:
    return (
        (direction == "CE" and spot <= entry_line * 1.002) or
        (direction == "PE" and spot >= entry_line * 0.998)
    )


def _hit_t1_or_sl(direction: str, spot: float, day_t1: float, hard_sl: float) -> str:
    if direction == "CE":
        if spot >= day_t1:
            return "T1"
        if spot <= hard_sl:
            return "SL"
    else:
        if spot <= day_t1:
            return "T1"
        if spot >= hard_sl:
            return "SL"
    return ""


async def check_one(stock: dict, token: str) -> None:
    symbol = stock["symbol"]
    direction = stock["direction"]
    entry_line = float(stock["entry_line"])
    hard_sl = float(stock["hard_sl"])
    day_t1 = float(stock["day_t1"])
    zone_lo = float(stock.get("zone_lo", 0.0))
    zone_hi = float(stock.get("zone_hi", 0.0))
    status = stock.get("status", "APPROACHING")
    upstox_key = stock.get("upstox_key", "")

    print(f"\n{'='*70}\n{symbol} {direction}  status={status}  "
          f"entry_line={entry_line}  zone=[{zone_lo},{zone_hi}]  "
          f"hard_sl={hard_sl}  day_t1={day_t1}")

    if not upstox_key:
        print("  SKIP: no upstox_key in watchlist entry.")
        return

    # Sanity check the zone against real recent daily closes.
    daily = await fetch_upstox_daily(upstox_key, token, lookback_days=7)
    if daily:
        closes = [f"{c['close']:.1f}" for c in daily[-5:]]
        print(f"  Real trailing daily closes (last {len(closes)}): {closes}")
    else:
        print("  Real trailing daily closes: unavailable (fetch failed/empty)")

    intraday = await fetch_upstox_intraday_1m(upstox_key, token)
    if not intraday:
        print("  Real today's intraday candles: unavailable (fetch failed/empty) -- "
              "cannot verify.")
        return
    print(f"  Real today's intraday candles: {len(intraday)} bars "
          f"({intraday[0].get('ts','?')} -> {intraday[-1].get('ts','?')})")

    # Gap filter at the FIRST bar (mirrors _try_enter_triggered's one-time check).
    first_spot = float(intraday[0]["close"])
    gap_pct = abs(first_spot - entry_line) / entry_line * 100 if entry_line else 0.0
    if status == "TRIGGERED" and (gap_pct > GAP_SKIP_PCT or _already_sl_side(direction, first_spot, hard_sl)):
        print(f"  RESULT: would have been SKIPPED at open -- gap={gap_pct:.2f}% "
              f"(limit {GAP_SKIP_PCT}%) or already past hard_sl at first candle "
              f"(spot={first_spot}).")
        return

    entered_at = None
    entered_spot = None
    for c in intraday:
        spot = float(c["close"])
        if _already_sl_side(direction, spot, hard_sl) and entered_at is None:
            # Real spot reached the stop level before ever touching entry_line --
            # matches the book's own "refusing to enter pre-stopped" guard.
            print(f"  RESULT: spot reached hard_sl ({hard_sl}) at {c.get('ts')} "
                  f"(spot={spot:.2f}) BEFORE ever touching entry_line={entry_line} -- "
                  f"would have been blocked, never entered.")
            return
        if _touched(direction, spot, entry_line):
            entered_at = c.get("ts")
            entered_spot = spot
            break

    if entered_at is None:
        last_spot = float(intraday[-1]["close"])
        dist_pct = abs(last_spot - entry_line) / entry_line * 100 if entry_line else 0.0
        print(f"  RESULT: NO ENTRY today -- spot never touched entry_line={entry_line} "
              f"(closed today at {last_spot:.2f}, {dist_pct:.2f}% away).")
        return

    print(f"  RESULT: WOULD HAVE ENTERED at {entered_at} (spot={entered_spot:.2f} "
          f"touched entry_line={entry_line})")

    # What would have happened to the trade for the rest of the session.
    outcome = ""
    outcome_ts = ""
    saw_entry = False
    for c in intraday:
        if not saw_entry:
            if c.get("ts") == entered_at:
                saw_entry = True
            continue
        spot = float(c["close"])
        hit = _hit_t1_or_sl(direction, spot, day_t1, hard_sl)
        if hit:
            outcome, outcome_ts = hit, c.get("ts")
            break
    if outcome:
        print(f"  Post-entry: would have hit {outcome} at {outcome_ts}.")
    else:
        last_spot = float(intraday[-1]["close"])
        print(f"  Post-entry: still running as of today's last candle "
              f"(spot={last_spot:.2f}), neither T1 nor SL hit yet.")


async def main() -> int:
    if not WATCHLIST_PATH.exists():
        print(f"FATAL: {WATCHLIST_PATH} not found.")
        return 1
    data = json.loads(WATCHLIST_PATH.read_text(encoding="utf-8"))
    stocks = data.get("stocks", [])
    print(f"Watchlist generated: {data.get('generated', '?')}  ({len(stocks)} stocks)")

    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    token = (creds or {}).get("access_token", "")
    if not token:
        print("FATAL: no Upstox access_token in data/clients.db.")
        return 1

    for stock in stocks:
        await check_one(stock, token)

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
