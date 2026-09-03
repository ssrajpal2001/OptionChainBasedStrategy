"""
scripts/d1trap_fno_breakeven_trail_sweep.py -- TSL optimization for the
FnO positional engine (strategies/fno_positional/book.py), matching its
ACTUAL live exit mechanic exactly -- NOT the hedge/phase mechanic from
backtest.py's simulate_stock (that was the wrong model, per user
correction: "hedge concept is not required").

Live book.py exit logic (read directly from strategies/fno_positional/
book.py, 2026-08-03):
  1. Hard SL: spot-based, at the zone boundary (with hard_sl_buf%).
  2. Breakeven trail (ONE-TIME step, added 2026-07-27): once spot has moved
     >= trail_trigger_pct of the entry->T1 distance, SL moves to breakeven
     (entry price) and stays there -- no further trailing after that.
  3. Expiry-week forced exit (~7 days before monthly expiry) -- approximated
     here the same way backtest.py's original sweep did, via
     _last_week_of_month_start.
  4. T1 hit publishes an alert only -- NOT an auto-exit, NOT a hedge. A
     trade that hits T1 and pulls back to its (by-then-breakeven) SL exits
     at breakeven, not at a locked profit -- this is exactly the "gives
     back profit" scenario worth testing variants against.

Sweep: trail_trigger_pct in {0.3, 0.4, 0.5 (live default), 0.6, 0.7} to see
whether trailing earlier/later than the current 50% helps, plus a
"no trail" baseline (pure hard-SL-only) for comparison.
"""
import sys, time
sys.path.insert(0, ".")
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Optional
from backtest.fno_scanner.scan_live import _fetch_fno_universe
from backtest.fno_scanner.backtest import (
    load_or_fetch, Bar, resample_d1_to_w1, _last_week_of_month_start,
)
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones

TOKEN_PATH = r"C:\Users\SERVER\AppData\Local\Temp\claude\e--AlgoSoft-OptionChainBasedStrategy\3f952902-2e64-455f-be1c-fac0a7378cbc\scratchpad\upstox_token.txt"

HARD_SL_BUF = 0.8   # matches scan_live.py's live default
MIN_RR = 1.5        # matches scan_live.py's live default
MAX_ZONE_AGE = 30   # matches scan_live.py's live default


@dataclass
class LiveTrade:
    symbol: str
    direction: str
    entry_date: date
    entry_price: float
    hard_sl: float
    day_t1: float
    exit_date: Optional[date] = None
    exit_price: float = 0.0
    exit_reason: str = ""
    trailed: bool = False

    def pnl_pct(self) -> float:
        if self.direction == "LONG":
            return (self.exit_price - self.entry_price) / self.entry_price * 100
        return (self.entry_price - self.exit_price) / self.entry_price * 100


def simulate_stock_live_mechanic(symbol: str, bars: List[Bar], trail_trigger_pct: Optional[float]) -> List[LiveTrade]:
    """trail_trigger_pct=None means NO breakeven trail (pure hard-SL baseline)."""
    if len(bars) < 20:
        return []
    trades: List[LiveTrade] = []
    active: Optional[LiveTrade] = None
    active_sl = 0.0

    for i, bar in enumerate(bars):
        today = bar.timestamp.date()

        if active is not None:
            is_long = active.direction == "LONG"

            if today >= _last_week_of_month_start(today):
                active.exit_date, active.exit_price, active.exit_reason = today, bar.close, "expiry_week"
                trades.append(active); active = None
                continue

            if trail_trigger_pct is not None and not active.trailed:
                rng = abs(active.day_t1 - active.entry_price)
                progress = (bar.high - active.entry_price) if is_long else (active.entry_price - bar.low)
                if rng > 0 and progress >= trail_trigger_pct * rng:
                    new_sl = active.entry_price
                    if (is_long and new_sl > active_sl) or (not is_long and new_sl < active_sl):
                        active_sl = new_sl
                        active.trailed = True

            sl_hit = (bar.low <= active_sl) if is_long else (bar.high >= active_sl)
            if sl_hit:
                active.exit_date, active.exit_price = today, active_sl
                active.exit_reason = "breakeven_sl" if active.trailed else "hard_sl"
                trades.append(active); active = None
                continue
            continue

        if i < 5:
            continue
        lookback = bars[:i]
        bear_zones = find_all_bear_zones(lookback)
        bull_zones = find_all_bull_zones(lookback)

        for zone in bear_zones + bull_zones:
            is_bear = zone in bear_zones
            direction = "LONG" if is_bear else "SHORT"
            zone_low = min(zone.entry_line or 0, zone.sweep_low or 0)
            zone_high = max(zone.entry_line or 0, zone.sweep_low or 0)
            if zone_low <= 0 or zone_high <= 0:
                continue
            if zone.lock_ts is not None and (today - zone.lock_ts.date()).days > MAX_ZONE_AGE:
                continue
            retest = (bar.low <= zone.entry_line and bar.close >= zone_low) if is_bear else \
                     (bar.high >= zone.entry_line and bar.close <= zone_high)
            if not retest:
                continue
            entry_price = zone.entry_line
            hard_sl = zone_low * (1 - HARD_SL_BUF / 100) if is_bear else zone_high * (1 + HARD_SL_BUF / 100)
            day_t1 = bar.high if is_bear else bar.low
            risk, reward = abs(entry_price - hard_sl), abs(day_t1 - entry_price)
            if risk <= 0 or (reward / risk) < MIN_RR:
                continue
            active = LiveTrade(symbol, direction, today, entry_price, hard_sl, day_t1)
            active_sl = hard_sl
            trades.append(active)
            break

    if active is not None and active.exit_date is None:
        trades.remove(active)  # still open at data end -- exclude from stats
    return [t for t in trades if t.exit_date is not None]


def summarize(name, trades):
    if not trades:
        print(f"{name}: no trades")
        return
    wins = [t for t in trades if t.pnl_pct() > 0]
    gp = sum(t.pnl_pct() for t in trades if t.pnl_pct() > 0)
    gl = abs(sum(t.pnl_pct() for t in trades if t.pnl_pct() <= 0))
    pf = gp / gl if gl > 0 else float("inf")
    trailed_n = sum(1 for t in trades if t.trailed)
    print(f"{name}: n={len(trades)}  win%={len(wins)/len(trades)*100:.1f}  PF={pf:.2f}  "
          f"avg/trade={sum(t.pnl_pct() for t in trades)/len(trades):+.2f}%  "
          f"net_sum={sum(t.pnl_pct() for t in trades):+.1f}%  trailed={trailed_n}")


if __name__ == "__main__":
    token = open(TOKEN_PATH).read().strip()
    universe = _fetch_fno_universe(token)
    print(f"FnO universe: {len(universe.stocks)} stocks")

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=6 * 31)
    stock_bars: Dict[str, List[Bar]] = {}
    for symbol, key in sorted(universe.stocks.items()):
        bars = load_or_fetch(symbol, key, token, start_date, end_date)
        if len(bars) >= 20:
            stock_bars[symbol] = bars
    print(f"Loaded {len(stock_bars)} stocks.\n")

    t0 = time.time()
    for label, trigger in [("NO TRAIL (hard SL only)", None),
                            ("trail @ 30%", 0.3),
                            ("trail @ 40%", 0.4),
                            ("trail @ 50% (LIVE default)", 0.5),
                            ("trail @ 60%", 0.6),
                            ("trail @ 70%", 0.7)]:
        all_trades: List[LiveTrade] = []
        for symbol, bars in stock_bars.items():
            all_trades.extend(simulate_stock_live_mechanic(symbol, bars, trigger))
        summarize(label, all_trades)
    print(f"\n({time.time()-t0:.0f}s)")
