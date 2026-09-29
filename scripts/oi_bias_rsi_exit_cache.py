"""
scripts/oi_bias_rsi_exit_cache.py -- fetches and caches the RAW real 1-min
bars (stock + resolved option contract) for every row in the manual-bias
CSV, ONCE, to local JSON files -- so the optimizer (scripts/oi_bias_rsi_
exit_optimize.py) can sweep many entry/exit timeframe + StochRSI length
combinations purely by local resampling/recomputation, without re-hitting
Upstox per combination (this codebase's own documented real incident:
"a multi-day backtest script firing many of these back-to-back ... tripped
a sustained Upstox rate limit" -- see fetch_upstox_range_1m's own
docstring). Reuses exactly the same resolution logic as scripts/oi_bias_
rsi_exit_backtest.py's own _run_one (freeze_signal_strikes, resolve_contract,
REGISTRY.get_active_expiry_strict) so a cached contract is never a different
one than the live backtest would have picked.

Usage: python scripts/oi_bias_rsi_exit_cache.py <upstox_token> [--csv path]
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_range_1m
from data_layer.instrument_registry import REGISTRY
from strategies.oi_bias_breakout.detector import freeze_signal_strikes
from strategies.oi_orb_screener import stock_resolve
from scripts.oi_bias_rsi_exit_backtest import (
    load_manual_bias_rows, DEFAULT_CSV, _token_is_valid, WARMUP_CALENDAR_DAYS_BACK,
)

CACHE_DIR = "data/oi_bias_rsi_exit_cache"


def _cache_path(symbol: str, day: date) -> str:
    return os.path.join(CACHE_DIR, f"{symbol}_{day.isoformat()}.json")


async def fetch_and_cache_one(row, token: str) -> None:
    path = _cache_path(row.symbol, row.trade_date)
    if os.path.exists(path):
        print(f"  {row.symbol} {row.trade_date}: already cached, skipping.")
        return
    if row.bias not in ("bullish", "bearish"):
        print(f"  {row.symbol} {row.trade_date}: bias='{row.bias}' not tradeable -- skipped.")
        return

    stock_key = stock_resolve.resolve_eq_instrument_key(row.symbol)
    warmup_start = row.trade_date - timedelta(days=WARMUP_CALENDAR_DAYS_BACK)
    stock_rows = await fetch_upstox_range_1m(stock_key, token, warmup_start, row.trade_date)
    if not stock_rows:
        print(f"  {row.symbol} {row.trade_date}: no real stock spot data -- skipped.")
        return
    bar_915 = next(
        (r for r in stock_rows
         if (datetime.fromisoformat(r["ts"]) if isinstance(r["ts"], str) else r["ts"]).date() == row.trade_date
         and (datetime.fromisoformat(r["ts"]) if isinstance(r["ts"], str) else r["ts"]).hour == 9
         and (datetime.fromisoformat(r["ts"]) if isinstance(r["ts"], str) else r["ts"]).minute == 15),
        None)
    if bar_915 is None:
        print(f"  {row.symbol} {row.trade_date}: no real 09:15 stock bar -- skipped.")
        return

    strike_step = stock_resolve.resolve_strike_step_for_price(row.symbol, bar_915["open"])
    strikes = freeze_signal_strikes(open_915_price=bar_915["open"], strike_step=strike_step)
    option_type = "CE" if row.bias == "bullish" else "PE"

    REGISTRY.load_sync(row.symbol, token)
    expiry = REGISTRY.get_active_expiry_strict(row.symbol, from_date=row.trade_date)
    if expiry is None:
        print(f"  {row.symbol} {row.trade_date}: no active expiry resolved -- skipped.")
        return

    contract = await asyncio.to_thread(
        stock_resolve.resolve_contract, row.symbol, strikes.atm, option_type, ("upstox",))
    if contract is None:
        print(f"  {row.symbol} {row.trade_date}: could not resolve a real {option_type} contract -- skipped.")
        return

    prem_rows = await fetch_upstox_range_1m(contract.upstox_key, token, row.trade_date, row.trade_date)
    if not prem_rows:
        print(f"  {row.symbol} {row.trade_date}: no real premium data for "
              f"{contract.strike}{option_type} -- skipped.")
        return

    lot = await stock_resolve.resolve_lot_async(row.symbol)

    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(path, "w") as f:
        json.dump({
            "symbol": row.symbol, "trade_date": row.trade_date.isoformat(), "bias": row.bias,
            "option_type": option_type, "strike": contract.strike, "expiry": str(expiry),
            "lot": lot, "open_915": bar_915["open"], "strike_step": strike_step,
            "stock_bars": stock_rows, "option_bars": prem_rows,
        }, f)
    print(f"  {row.symbol} {row.trade_date}: cached ({len(stock_rows)} stock bars, "
          f"{len(prem_rows)} option bars, {contract.strike}{option_type}).")


async def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python scripts/oi_bias_rsi_exit_cache.py <upstox_token> [--csv path]")
        return
    token = sys.argv[1]
    if not await _token_is_valid(token):
        print("ERROR: Upstox token appears INVALID or EXPIRED.")
        return
    csv_path = DEFAULT_CSV
    if "--csv" in sys.argv:
        csv_path = sys.argv[sys.argv.index("--csv") + 1]
    rows = load_manual_bias_rows(csv_path)
    print(f"Caching {len(rows)} row(s) to {CACHE_DIR}/ ...")
    for row in rows:
        print(f"{row.symbol} {row.trade_date}:")
        try:
            await fetch_and_cache_one(row, token)
        except Exception as exc:
            print(f"  ERROR: {exc!r}")


if __name__ == "__main__":
    asyncio.run(main())
