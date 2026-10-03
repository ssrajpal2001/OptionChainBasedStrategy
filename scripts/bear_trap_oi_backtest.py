"""Historical backtest for the Bear Trap OI Confirmation strategy.

Drives the REAL strategies/bear_trap_oi/trap_detector.py state-machine
functions against historical 5-minute option premium bars (never a
reimplementation, per this codebase's "backtest must drive the real
class" rule).

LIMITATION (spec Section 9): Upstox's historical intraday candle API
returns oi=0 on every row for option contracts, so the multi-strike OI
filter CANNOT be backtested. This script evaluates ONLY the price-action
trap/zone engine, with the OI filter bypassed (every zone re-entry fires
unconditionally). Do not read these results as a validation of the full
live strategy -- only of its price-action half. See the spec for the
forward-telemetry plan that validates the OI half once live.

Usage:
    python scripts/bear_trap_oi_backtest.py --days 7 --strike-step 50
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import groupby
from typing import Literal

from strategies.bear_trap_oi.models import Bar, TrapZone, TrapZoneState
from strategies.bear_trap_oi.trap_detector import (
    on_bar_close, close_position,
)

Side = Literal["CE", "PE"]


@dataclass
class BacktestTrade:
    side: Side
    strike: int
    entry_price: float
    entry_ts: datetime
    exit_price: float
    exit_ts: datetime
    pnl: float
    c1: Bar
    c2: Bar


def _fresh_zone() -> TrapZone:
    return TrapZone(state=TrapZoneState.WAITING, c1=None, c2=None,
                     zone_lo=None, zone_hi=None, confirmed_ts=None)


def run_side_backtest(bars: list[Bar], side: Side, strike: int,
                       lot_qty: int) -> list[BacktestTrade]:
    """Replay one trading day's bars for one side through the real
    detector. OI filter is bypassed (see module docstring)."""
    trades: list[BacktestTrade] = []
    zone = _fresh_zone()
    open_entry: tuple[float, datetime, Bar, Bar] | None = None  # price, ts, c1, c2

    for i, bar in enumerate(bars):
        is_last_bar = i == len(bars) - 1

        if zone.state == TrapZoneState.ARMED_WAIT_REENTRY and open_entry is None:
            # Check re-entry using this bar's own OHLC range (backtest has
            # no sub-bar ticks) -- re-entry fires if the bar's range ever
            # touched the zone.
            touched = (zone.zone_lo <= bar.high and bar.low <= zone.zone_hi)
            if touched:
                entry_price = bar.close
                open_entry = (entry_price, bar.ts, zone.c1, zone.c2)
                zone = TrapZone(state=TrapZoneState.IN_POSITION, c1=zone.c1,
                                 c2=zone.c2, zone_lo=zone.zone_lo,
                                 zone_hi=zone.zone_hi,
                                 confirmed_ts=zone.confirmed_ts)
                continue  # don't also run on_bar_close on the entry bar

        if zone.state not in (TrapZoneState.IN_POSITION,):
            zone = on_bar_close(zone, bar)

        if is_last_bar and open_entry is not None:
            entry_price, entry_ts, c1, c2 = open_entry
            exit_price = bar.close
            pnl = (exit_price - entry_price) * lot_qty
            trades.append(BacktestTrade(
                side=side, strike=strike, entry_price=entry_price,
                entry_ts=entry_ts, exit_price=exit_price, exit_ts=bar.ts,
                pnl=pnl, c1=c1, c2=c2,
            ))
            open_entry = None
            zone = close_position(zone)

    return trades


def _group_by_trading_day(bars: list[Bar]) -> list[list[Bar]]:
    keyfunc = lambda b: b.ts.date()
    return [list(g) for _, g in groupby(bars, key=keyfunc)]


async def _fetch_week_of_premium(underlying: str, ce_key: str, pe_key: str,
                                  days: int):
    """Fetches real 5-min premium history via the platform's existing
    Upstox intraday fetcher. Imported lazily so unit tests (which only
    exercise run_side_backtest) never need network/broker config."""
    from data_layer.historical_candles import fetch_upstox_intraday_1m

    end = datetime.now()
    start = end - timedelta(days=days)
    ce_1m = await fetch_upstox_intraday_1m(ce_key, start, end)
    pe_1m = await fetch_upstox_intraday_1m(pe_key, start, end)
    return ce_1m, pe_1m


def _resample_1m_to_5m(bars_1m: list[Bar]) -> list[Bar]:
    from strategies.bear_trap_oi.candle_tracker import BarAccumulator
    acc = BarAccumulator(bucket_minutes=5)
    out: list[Bar] = []
    for b in bars_1m:
        completed = acc.on_tick(b.ts, b.close)
        if completed is not None:
            out.append(completed)
    partial = acc.current_partial()
    if partial is not None:
        out.append(partial)
    return out


def _print_report(side: str, trades: list[BacktestTrade]) -> None:
    print(f"\n=== {side} side: {len(trades)} trade(s) ===")
    wins = [t for t in trades if t.pnl > 0]
    losses = [t for t in trades if t.pnl <= 0]
    total_pnl = sum(t.pnl for t in trades)
    print(f"Win/Loss: {len(wins)}W / {len(losses)}L")
    if trades:
        win_rate = len(wins) / len(trades) * 100
        avg_win = sum(t.pnl for t in wins) / len(wins) if wins else 0.0
        avg_loss = sum(t.pnl for t in losses) / len(losses) if losses else 0.0
        print(f"Win rate: {win_rate:.1f}%  Avg win: {avg_win:.2f}  Avg loss: {avg_loss:.2f}")
        print(f"Total P&L: {total_pnl:.2f}")
    for t in trades:
        print(f"  [{t.entry_ts}] strike={t.strike} C1(close={t.c1.close}, "
              f"high={t.c1.high}, low={t.c1.low}) C2(low={t.c2.low}) "
              f"entry={t.entry_price} -> exit({t.exit_ts})={t.exit_price} "
              f"pnl={t.pnl:.2f}")


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--strike-step", type=int, default=50)
    parser.add_argument("--lot-qty", type=int, default=75)
    args = parser.parse_args()

    from data_layer.historical_candles import fetch_upstox_daily
    from data_layer.instrument_registry import REGISTRY
    from strategies.bear_trap_oi.strike_selector import map_strikes

    today = datetime.now()
    daily = await fetch_upstox_daily("NSE_INDEX|Nifty 50", lookback_days=args.days + 2)
    pdh, pdl = daily[-2].high, daily[-2].low  # previous trading day

    ce_strike, pe_strike = map_strikes(pdh, pdl, args.strike_step)
    expiry = REGISTRY.get_active_expiry_strict("NIFTY", from_date=today)
    ce_key = REGISTRY.get_option_key("NIFTY", expiry, ce_strike, "CE")
    pe_key = REGISTRY.get_option_key("NIFTY", expiry, pe_strike, "PE")

    ce_1m, pe_1m = await _fetch_week_of_premium("NIFTY", ce_key, pe_key, args.days)
    ce_5m = _resample_1m_to_5m(ce_1m)
    pe_5m = _resample_1m_to_5m(pe_1m)

    all_trades: list[BacktestTrade] = []
    for day_bars in _group_by_trading_day(ce_5m):
        all_trades += run_side_backtest(day_bars, "CE", ce_strike, args.lot_qty)
    for day_bars in _group_by_trading_day(pe_5m):
        all_trades += run_side_backtest(day_bars, "PE", pe_strike, args.lot_qty)

    _print_report("CE", [t for t in all_trades if t.side == "CE"])
    _print_report("PE", [t for t in all_trades if t.side == "PE"])
    print(f"\n=== Combined: {len(all_trades)} trade(s), "
          f"Total P&L: {sum(t.pnl for t in all_trades):.2f} ===")


if __name__ == "__main__":
    asyncio.run(main())
