"""Cross-side 1-min trap switch backtest (2026-10-04, direct user spec).

Pure price-action only -- no OI, no Volume Profile, no rollover/noise/
wall logic. Mechanic:
  1. Run the normal 5-min trap detector on a side (CE or PE).
  2. If that side's armed zone FAILS (a 5-min bar closes below zone_lo
     instead of re-entering), jump to the OPPOSITE side's 1-minute chart.
  3. Find that side's latest/freshest 1-min trap cycle and fire the
     moment price re-enters it -- immediately, no further confirmation.

Reuses detect_5m_zone_invalidation/enter_on_1m_trap from
scripts/bear_trap_oi_backtest.py (never reimplements the detector).

Usage:
    python -m scripts.bear_trap_switch_backtest --start 2026-09-29 --end 2026-10-01
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime, timedelta

from scripts.bear_trap_oi_backtest import (
    _candle_dicts_to_bars, _get_upstox_access_token, _resample_1m_to_5m,
    _truncate_to_eod, compute_daily_strikes, detect_5m_zone_invalidation,
    enter_on_1m_trap,
)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=str, default="2026-09-29")
    parser.add_argument("--end", type=str, default="2026-10-01")
    parser.add_argument("--strike-step", type=int, default=50)
    parser.add_argument("--lot-qty", type=int, default=75)
    args = parser.parse_args()
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    from data_layer.historical_candles import fetch_upstox_daily, fetch_upstox_range_1m
    from data_layer.instrument_registry import REGISTRY

    access_token = _get_upstox_access_token()
    index_key = REGISTRY.get_upstox_index_key("NIFTY")
    lookback_days = (date.today() - start).days + 3
    daily = await fetch_upstox_daily(index_key, access_token, lookback_days=lookback_days)
    daily_strikes = [row for row in compute_daily_strikes(daily, args.strike_step)
                      if start <= row[0] <= end]

    REGISTRY.load_sync("NIFTY", access_token)

    print(f"=== Cross-Side 1-Min Trap Switch Backtest: {start} .. {end} "
          f"({len(daily_strikes)} trading day(s)) ===\n")

    all_trades: list[dict] = []
    for trading_day, ce_strike, pe_strike in daily_strikes:
        expiry = REGISTRY.get_active_expiry_strict("NIFTY", from_date=trading_day)
        if expiry is None:
            print(f"[{trading_day}] SKIP -- could not resolve active expiry")
            continue
        ce_key = REGISTRY.get_upstox_key("NIFTY", expiry, ce_strike, "CE")
        pe_key = REGISTRY.get_upstox_key("NIFTY", expiry, pe_strike, "PE")
        print(f"[{trading_day}] ce_strike={ce_strike} pe_strike={pe_strike} expiry={expiry}")

        ce_rows = await fetch_upstox_range_1m(ce_key, access_token, trading_day, trading_day)
        pe_rows = await fetch_upstox_range_1m(pe_key, access_token, trading_day, trading_day)
        ce_1m = _truncate_to_eod(_candle_dicts_to_bars(ce_rows))
        pe_1m = _truncate_to_eod(_candle_dicts_to_bars(pe_rows))
        ce_5m = _truncate_to_eod(_resample_1m_to_5m(ce_1m))
        pe_5m = _truncate_to_eod(_resample_1m_to_5m(pe_1m))
        print(f"[{trading_day}] CE: {len(ce_1m)} 1m / {len(ce_5m)} 5m bars, "
              f"PE: {len(pe_1m)} 1m / {len(pe_5m)} 5m bars")

        for side, bars_5m, strike, other_side, other_bars_1m, other_strike in (
            ("CE", ce_5m, ce_strike, "PE", pe_1m, pe_strike),
            ("PE", pe_5m, pe_strike, "CE", ce_1m, ce_strike),
        ):
            inv_ts = detect_5m_zone_invalidation(bars_5m)
            if inv_ts is None:
                print(f"[{trading_day}] {side} {strike}: no 5m zone invalidation today")
                continue
            print(f"[{trading_day}] {side} {strike}: 5m ZONE INVALIDATED @{inv_ts} "
                  f"-> switching to {other_side} {other_strike} 1m chart")
            later_1m = [b for b in other_bars_1m if b.ts > inv_ts]
            entry = enter_on_1m_trap(later_1m, args.lot_qty)
            if entry is None:
                print(f"[{trading_day}] {other_side} {other_strike}: no fresh 1m trap "
                      f"re-entry found after {inv_ts}")
                continue
            exit_bar = later_1m[-1]
            if exit_bar.ts <= entry["entry_ts"]:
                print(f"[{trading_day}] {other_side} {other_strike}: entry fired on "
                      f"the last available bar -- no room for an EOD exit")
                continue
            exit_price = exit_bar.close
            pnl = (exit_price - entry["entry_price"]) * args.lot_qty
            print(f"[{trading_day}] {other_side} {other_strike}: 1m RE-ENTRY "
                  f"@{entry['entry_ts']} price={entry['entry_price']} "
                  f"(zone=[{entry['zone_lo']}, {entry['zone_hi']}]) -> "
                  f"EOD exit @{exit_bar.ts} price={exit_price} pnl={pnl:.2f}")
            all_trades.append({
                "day": trading_day, "side": other_side, "strike": other_strike,
                "triggered_by": side, "entry_ts": entry["entry_ts"],
                "entry_price": entry["entry_price"], "exit_ts": exit_bar.ts,
                "exit_price": exit_price, "pnl": pnl,
            })
        print()

    print("=== Summary ===")
    wins = [t for t in all_trades if t["pnl"] > 0]
    losses = [t for t in all_trades if t["pnl"] <= 0]
    print(f"Trades: {len(all_trades)}  Win/Loss: {len(wins)}W/{len(losses)}L")
    for t in all_trades:
        print(f"  [{t['day']}] {t['side']} {t['strike']} (triggered by {t['triggered_by']} "
              f"invalidation) entry @{t['entry_ts']} {t['entry_price']} -> "
              f"exit @{t['exit_ts']} {t['exit_price']} pnl={t['pnl']:.2f}")
    print(f"\nTotal P&L: {sum(t['pnl'] for t in all_trades):.2f}")


if __name__ == "__main__":
    asyncio.run(main())
