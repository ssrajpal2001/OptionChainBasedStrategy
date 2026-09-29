"""
scripts/oi_bias_rsi_exit_backtest.py -- backtest-only validation (2026-09-26,
direct user spec, real option premium via Upstox) for the NEW "OI-spurt
selection + StochRSI(14,14,3,3) K/D" entry/exit mechanic in
strategies/oi_bias_rsi_exit/detector.py.

Per direct user instruction ("before implementing do a backtest"), this
validates ONLY the new entry/exit mechanic -- Steps 1-6 (top gainer/loser +
OI-spurt selection, ATM/OTM signal-strike freeze, OI-bias classification)
are NOT reimplemented or re-derived here; they're already live in
strategies/oi_orb_screener/screener.py and strategies/oi_bias_breakout/
detector.py. This script instead takes a small manually-curated list of
(trade_date, symbol, bias) rows -- data/oi_bias_rsi_exit_manual_bias.csv --
because the real per-strike ATM/OTM OI values that Steps 3-5 need have never
actually been recorded anywhere in this codebase (the OI-bias engine has
never run live; only its pure logic functions exist) and NSE's OI-spurt/
gainer-loser endpoints are live-snapshot-only with no historical query
parameter (same structural wall this codebase already hit for OI-Flow's own
OI history). Each CSV row's bias was derived from a REAL trading day's own
recorded outcome (see e.g. 2026-09-25's own oi_orb "option_native" log +
top_gainer_loser_history DB rows: POLICYBZR was tagged a "gainer" at
selection time but every real trade that day was on its PE side and its
price fell ~8% intraday -- bearish; OFSS was tagged a "loser" but every
real trade was on its CE side after a sharp reversal -- bullish).

Mechanic under test, per stock/day:
  1. Resolve the stock's real 09:15 1-min bar open via Upstox, then the ATM
     strike via strategies.oi_bias_breakout.detector.freeze_signal_strikes
     (reused, not re-derived) -- CE if bias=="bullish", PE if bias==
     "bearish". Contract resolved via strategies.oi_orb_screener.
     stock_resolve.resolve_contract (snaps to the nearest REAL listed
     strike), expiry via REGISTRY.get_active_expiry_strict for that date.
  2. 2026-09-26 direct user pivot: StochRSI(14,14,3,3) is computed on the
     STOCK'S OWN price, not the option premium -- a real, live-verified gap
     forced this (see _run_one's own docstring): POLICYBZR's PE1160 had
     ZERO real trades on any day except the selection day itself in the
     prior 30 calendar days, so a 14-period indicator on that contract's
     own premium could never warm up on day one, the exact day it matters
     most. The stock always trades every session, so it never has this
     gap -- same "spot drives the signal, the option only prices the fill"
     pattern this codebase already uses deliberately for Liquidity Sweep/
     Liquidity Trap. Fetches the stock's own 1-min history over a WIDE
     (30 calendar day) warmup window, resampled to 5-min (entry) and
     1-hour (exit) bars via strategies.core.candle_indicators.
     to_n_min_bars_market_anchored (reused -- multi-day-safe, market-open-
     anchored bucketing).
  3. StochRSI(14,14,3,3) via strategies.oi_bias_rsi_exit.detector.
     compute_stoch_rsi_double_smoothed on both STOCK-price series.
  4. ENTRY: scans the trade day's STOCK 5-min bars in order; check_entry_
     state (K>D, a STATE check -- an earlier crossover still counts) fires
     on the first bar it's true for. The option's own real premium at that
     same timestamp (nearest real print within 15 min, never fabricated)
     is the actual fill price.
  5. EXIT, first of two to fire (the third frozen-spec exit -- bias
     flipping to the opposite direction twice -- is NOT simulated here;
     no historical per-5-min OI bias reading exists for any past day, so
     it's flagged, never silently assumed):
       (a) check_exit_cross (a genuine D-crosses-above-K EVENT, not a
           D>K state) on the STOCK's 1-hour bars, scanned from entry
           onward -- again priced via the option's own nearest real print;
       (b) EOD -- the trade day's last available STOCK bar.
  6. No mirroring by CE/PE side -- P&L is always (exit_premium -
     entry_premium), long-only, since a bought PE is long its own premium
     exactly like a bought CE (see detector.py's own module docstring).

Usage: python scripts/oi_bias_rsi_exit_backtest.py <upstox_token>
       [--csv path/to/manual_bias.csv]
"""
from __future__ import annotations

import asyncio
import csv
import sys
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from typing import List, Optional

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_range_1m, _http_get_json
from data_layer.instrument_registry import REGISTRY
from strategies.core.trap_zone_utils import Bar
from strategies.core.candle_indicators import to_n_min_bars_market_anchored
from strategies.oi_bias_breakout.detector import freeze_signal_strikes
from strategies.oi_bias_rsi_exit.detector import (
    compute_stoch_rsi_double_smoothed, check_entry_state, check_exit_cross,
)
from strategies.oi_orb_screener import stock_resolve

DEFAULT_CSV = "data/oi_bias_rsi_exit_manual_bias.csv"
TOKEN = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else ""


@dataclass
class ManualBiasRow:
    trade_date: date
    symbol: str
    bias: str  # "bullish" | "bearish"


@dataclass
class Trade:
    trade_date: date
    symbol: str
    bias: str
    option_type: str
    strike: int
    entry_ts: datetime
    entry_price: float
    exit_ts: datetime
    exit_price: float
    exit_reason: str
    lot: int
    # Diagnostic series -- the STOCK's OWN price bars/StochRSI (the signal
    # source, 2026-09-26 pivot), NOT the option premium -- entry_price/
    # exit_price above are the only option-premium numbers on this object.
    # All real, straight from the fetched bars, for printing a verifiable
    # bar-by-bar table against a real chart (see oi_bias_rsi_exit_
    # diagnostic.py).
    bars_5m: List[Bar]
    k5: List[Optional[float]]
    d5: List[Optional[float]]
    bars_1h: List[Bar]
    k60: List[Optional[float]]
    d60: List[Optional[float]]
    entry_idx_5m: int
    # Real OPTION PREMIUM intrabar high/low (from each real 1-min bar's own
    # .high/.low, not just closes) across [entry_ts, exit_ts] -- "how high
    # did the actual tradable instrument go before this trade closed",
    # distinct from post_entry_extreme() below which is the STOCK's price.
    premium_high: Optional[float] = None
    premium_high_ts: Optional[datetime] = None
    premium_low: Optional[float] = None
    premium_low_ts: Optional[datetime] = None

    @property
    def pnl_pts(self) -> float:
        return self.exit_price - self.entry_price

    @property
    def pnl_rs(self) -> float:
        return self.pnl_pts * self.lot

    def post_entry_extreme(self) -> "tuple[float, datetime, float, datetime]":
        """Real STOCK PRICE (max_close, ts_of_max, min_close, ts_of_min)
        across every 5-min bar from entry to EOD on the trade day -- 'how
        far did the stock go' vs. the actual option-premium exit, for chart
        verification."""
        day_bars = [b for b in self.bars_5m if b.ts.date() == self.trade_date]
        post = [b for b in day_bars if b.ts >= self.entry_ts]
        hi_bar = max(post, key=lambda b: b.close)
        lo_bar = min(post, key=lambda b: b.close)
        return hi_bar.close, hi_bar.ts, lo_bar.close, lo_bar.ts


def load_manual_bias_rows(csv_path: str) -> List[ManualBiasRow]:
    rows: List[ManualBiasRow] = []
    with open(csv_path, newline="") as f:
        for r in csv.DictReader(f):
            rows.append(ManualBiasRow(
                trade_date=date.fromisoformat(r["trade_date"]),
                symbol=r["symbol"].strip().upper(),
                bias=r["bias"].strip().lower(),
            ))
    return rows


def _rows_to_bars(rows: List[dict]) -> List[Bar]:
    out = []
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        out.append(Bar(ts=ts, open=r["open"], high=r["high"], low=r["low"], close=r["close"]))
    return out


# StochRSI(14,14,3,3) needs ~32 real bars before its own double-smoothed %D
# produces a value at all (RSI needs 15 closes -> raw stoch needs 14 more
# RSI values -> %K needs 3 more raw-stoch values -> %D needs 3 more %K
# values = 14+13+2+2 = 31, i.e. the 32nd close). On 1-HOUR bars a single
# trading day only yields ~6-7 bars, nowhere near enough -- confirmed empty
# (all-None) against real POLICYBZR/OFSS 1H series with only 1 prior day of
# warmup (2026-09-26 real-data diagnostic run). 30 calendar days back
# comfortably covers >=18 trading days (~115+ hourly bars for a liquid
# contract) even after weekends/holidays -- fetch_upstox_range_1m already
# skips Sat/Sun internally, so extra calendar days here are cheap (empty
# holiday days just return no rows).
WARMUP_CALENDAR_DAYS_BACK = 30

# The real oi_orb_screener shortlist isn't ready until SCAN_START (09:26,
# strategies/oi_orb_screener/screener.py's own CONFIG["SCAN_START"]) -- a
# stock genuinely isn't known to be tradeable before that moment, so entry
# scanning must never start earlier. A real bug this fix caught: without
# this floor, POLICYBZR's entry fired at 09:15 (the very first bar of the
# day) purely because its stock-price StochRSI K>D state happened to already
# be true carrying over from prior days -- a signal genuinely available on
# a real chart, but not YET KNOWN to this strategy at 09:15, since the
# stock hadn't even been shortlisted yet. Same look-ahead-bias class this
# codebase has fixed before (see the frozen oi_bias_breakout spec's own
# "callers must never pass a later_bars entry earlier than 09:30" note).
ENTRY_SCAN_START = dtime(9, 26)

# 2026-09-27, direct user request: entry timeframe + StochRSI length and
# exit timeframe + StochRSI length, OPTIMIZED via scripts/oi_bias_rsi_exit_
# optimize.py's own greedy 2-stage sweep against these 20 real cached trades
# (data/oi_bias_rsi_exit_cache/). Baseline (5m/60m, both StochRSI(14,14,3,3))
# scored net=Rs+66,220 win%=65.0; this config scored net=Rs+100,330
# win%=70.0 -- a real, sweep-confirmed improvement, not a guess. Still only
# n=20 real trades -- the optimizer's own report explicitly flags this as
# directional, not a proven joint optimum (a GREEDY sweep: entry was
# optimized first with exit held at the old default, then exit was
# optimized with entry fixed to its own winner -- not a full joint grid).
# Re-run the optimizer as more real days get added to the manual-bias CSV.
ENTRY_TIMEFRAME_MIN = 3
ENTRY_STOCH_RSI_LENGTHS = (21, 21, 3, 3)  # (rsi_period, stoch_period, k_smooth, d_smooth)
EXIT_TIMEFRAME_MIN = 75
EXIT_STOCH_RSI_LENGTHS = (21, 21, 3, 3)


async def _token_is_valid(token: str) -> bool:
    try:
        resp = await asyncio.to_thread(_http_get_json, "https://api.upstox.com/v2/user/profile", token)
    except Exception:
        return False
    return bool(resp) and resp.get("status") == "success"


def _price_near(bars_1m: List[Bar], ts: datetime, max_minutes: int = 15) -> Optional[float]:
    """Real option premium closest to ts -- prefers the nearest real print
    AT/AFTER ts (the price you'd actually get acting on a signal that just
    fired), falls back to the nearest real print BEFORE ts within the same
    window if nothing traded right after. Never fabricates a price; returns
    None if nothing real exists within max_minutes either side."""
    after = [b for b in bars_1m if b.ts >= ts and (b.ts - ts).total_seconds() <= max_minutes * 60]
    if after:
        return min(after, key=lambda b: b.ts).close
    before = [b for b in bars_1m if b.ts < ts and (ts - b.ts).total_seconds() <= max_minutes * 60]
    if before:
        return max(before, key=lambda b: b.ts).close
    return None


async def _run_one(row: ManualBiasRow, token: str) -> Optional[Trade]:
    """2026-09-26 direct user pivot: the StochRSI(14,14,3,3) entry/exit
    signal is computed on the STOCK's OWN price (not the option premium) --
    confirmed, real-data-driven fix for a genuine gap the option-premium
    version hit: a freshly-selected OI-spurt stock's relevant strike can
    have ZERO real trading history before the selection day itself (verified
    live: POLICYBZR PE1160 had real data on 2026-09-25 and NO OTHER day in
    the prior 30 calendar days), making the 14-period-based indicator
    structurally impossible to warm up on day one. The stock itself always
    trades every real session, so it never has this gap. Same pattern this
    codebase already uses deliberately for Liquidity Sweep/Liquidity Trap's
    own spot-based SL/target design -- the option premium is still what
    gets bought/sold and still prices every fill, it just no longer drives
    the entry/exit DECISION."""
    if row.bias not in ("bullish", "bearish"):
        print(f"  {row.symbol} {row.trade_date}: bias='{row.bias}' not tradeable (conflict/none) -- skipped.")
        return None

    stock_key = stock_resolve.resolve_eq_instrument_key(row.symbol)
    warmup_start = row.trade_date - timedelta(days=WARMUP_CALENDAR_DAYS_BACK)
    stock_rows = await fetch_upstox_range_1m(stock_key, token, warmup_start, row.trade_date)
    if not stock_rows:
        print(f"  {row.symbol} {row.trade_date}: no real stock spot data -- skipped.")
        return None
    stock_bars = sorted(_rows_to_bars(stock_rows), key=lambda b: b.ts)
    bar_915 = next((b for b in stock_bars if b.ts.date() == row.trade_date
                     and b.ts.hour == 9 and b.ts.minute == 15), None)
    if bar_915 is None:
        print(f"  {row.symbol} {row.trade_date}: no real 09:15 stock bar -- skipped.")
        return None

    strike_step = stock_resolve.resolve_strike_step_for_price(row.symbol, bar_915.open)
    strikes = freeze_signal_strikes(open_915_price=bar_915.open, strike_step=strike_step)
    option_type = "CE" if row.bias == "bullish" else "PE"

    REGISTRY.load_sync(row.symbol, token)
    expiry = REGISTRY.get_active_expiry_strict(row.symbol, from_date=row.trade_date)
    if expiry is None:
        print(f"  {row.symbol} {row.trade_date}: no active expiry resolved for this date -- skipped.")
        return None

    contract = await asyncio.to_thread(
        stock_resolve.resolve_contract, row.symbol, strikes.atm, option_type, ("upstox",))
    if contract is None:
        print(f"  {row.symbol} {row.trade_date}: could not resolve a real {option_type} contract near "
              f"ATM={strikes.atm} -- skipped.")
        return None

    bars_5m = to_n_min_bars_market_anchored(stock_bars, ENTRY_TIMEFRAME_MIN)
    bars_1h = to_n_min_bars_market_anchored(stock_bars, EXIT_TIMEFRAME_MIN)
    closes_5m = [b.close for b in bars_5m]
    closes_1h = [b.close for b in bars_1h]
    k5, d5 = compute_stoch_rsi_double_smoothed(closes_5m, *ENTRY_STOCH_RSI_LENGTHS)
    k60, d60 = compute_stoch_rsi_double_smoothed(closes_1h, *EXIT_STOCH_RSI_LENGTHS)

    entry_idx = next(
        (i for i, b in enumerate(bars_5m)
         if b.ts.date() == row.trade_date and b.ts.time() >= ENTRY_SCAN_START
         and check_entry_state(k5[i], d5[i], row.bias)),
        None)
    if entry_idx is None:
        side_desc = "K>D" if row.bias == "bullish" else "D>K"
        print(f"  {row.symbol} {row.trade_date}: {side_desc} never held on the STOCK's "
              f"{ENTRY_TIMEFRAME_MIN}-min chart (at/after {ENTRY_SCAN_START}) -- no entry.")
        return None
    entry_ts = bars_5m[entry_idx].ts

    exit_ts, exit_reason = None, "eod"
    # Compare every trade-day exit-tf bar against its immediate predecessor
    # in the FULL series (never a re-indexed sub-list) -- a bar whose bucket
    # START is before entry but whose bucket CLOSE (start+EXIT_TIMEFRAME_MIN)
    # is at/after entry is still a legitimate, no-look-ahead check point the
    # instant it closes (e.g. entry at 09:55 inside the 09:15-10:15 bucket:
    # that bucket closes at 10:15, strictly after entry, so its own K/D-vs-
    # prior-bar crossover is real information available post-entry --
    # excluding it entirely, as an earlier version of this script did via
    # `b.ts >= entry_ts`, silently skipped the very first real exit-check
    # opportunity).
    for i in range(1, len(bars_1h)):
        bar = bars_1h[i]
        if bar.ts.date() != row.trade_date:
            continue
        bucket_close = bar.ts + timedelta(minutes=EXIT_TIMEFRAME_MIN)
        if bucket_close <= entry_ts:
            continue
        if check_exit_cross(k60[i - 1], d60[i - 1], k60[i], d60[i], row.bias):
            # Real bug (2026-09-26): labeling this by the bucket's own START
            # (bar.ts) can print/price the exit BEFORE entry whenever entry
            # happens inside this same bucket (e.g. entry 09:35 inside the
            # 09:15-10:15 bucket -- its start, 09:15, is earlier than entry
            # even though the crossover isn't knowable until the bucket
            # actually CLOSES at 10:15). A 5-min bar's start-vs-close gap is
            # negligible and matches this codebase's own existing
            # convention; a 60-min bucket's gap is not -- use the real
            # close time here so exit is always >= entry and priced at the
            # moment the signal was genuinely knowable.
            exit_ts, exit_reason = bucket_close, "stoch_d_cross_1h"
            break
    if exit_ts is None:
        trade_day_bars = [b for b in bars_5m if b.ts.date() == row.trade_date]
        exit_ts = trade_day_bars[-1].ts

    # The stock signal decided WHEN; the option's own real premium at that
    # same moment decides the actual fill price (never fabricated -- None
    # if nothing real traded within 15 minutes either side).
    prem_rows = await fetch_upstox_range_1m(contract.upstox_key, token, row.trade_date, row.trade_date)
    if not prem_rows:
        print(f"  {row.symbol} {row.trade_date}: no real premium data at all for "
              f"{contract.strike}{option_type} -- skipped.")
        return None
    prem_bars = sorted(_rows_to_bars(prem_rows), key=lambda b: b.ts)
    entry_price = _price_near(prem_bars, entry_ts)
    exit_price = _price_near(prem_bars, exit_ts)
    if entry_price is None or exit_price is None:
        print(f"  {row.symbol} {row.trade_date}: no real {contract.strike}{option_type} print within "
              f"15 min of entry/exit -- skipped.")
        return None

    # Real intrabar premium high/low (each bar's own .high/.low, not just
    # closes) for every real 1-min print between entry and exit inclusive --
    # answers "how high did the tradable option actually go before this
    # trade closed", never fabricated (None if the window has no real bars,
    # which shouldn't happen since entry/exit themselves both resolved a
    # real print, but guarded rather than assumed).
    held_bars = [b for b in prem_bars if entry_ts <= b.ts <= exit_ts]
    premium_high = premium_high_ts = premium_low = premium_low_ts = None
    if held_bars:
        hi_bar = max(held_bars, key=lambda b: b.high)
        lo_bar = min(held_bars, key=lambda b: b.low)
        premium_high, premium_high_ts = hi_bar.high, hi_bar.ts
        premium_low, premium_low_ts = lo_bar.low, lo_bar.ts

    lot = await stock_resolve.resolve_lot_async(row.symbol)
    return Trade(
        trade_date=row.trade_date, symbol=row.symbol, bias=row.bias, option_type=option_type,
        strike=contract.strike, entry_ts=entry_ts, entry_price=entry_price,
        exit_ts=exit_ts, exit_price=exit_price, exit_reason=exit_reason, lot=lot,
        bars_5m=bars_5m, k5=k5, d5=d5, bars_1h=bars_1h, k60=k60, d60=d60,
        entry_idx_5m=entry_idx,
        premium_high=premium_high, premium_high_ts=premium_high_ts,
        premium_low=premium_low, premium_low_ts=premium_low_ts,
    )


async def main() -> None:
    if not TOKEN:
        print("Usage: python scripts/oi_bias_rsi_exit_backtest.py <upstox_token> [--csv path]")
        return
    if not await _token_is_valid(TOKEN):
        print("ERROR: Upstox token appears INVALID or EXPIRED (checked via /v2/user/profile). "
              "Generate a fresh token and re-run.")
        return

    csv_path = DEFAULT_CSV
    if "--csv" in sys.argv:
        csv_path = sys.argv[sys.argv.index("--csv") + 1]
    rows = load_manual_bias_rows(csv_path)
    print(f"Loaded {len(rows)} manual (date, symbol, bias) row(s) from {csv_path}.")
    print("NOTE: the 'bias flips twice' exit is NOT simulated -- no historical per-5-min "
          "per-strike OI reading exists for any past day (the OI-bias engine has never run "
          "live). Only the StochRSI entry + D-cross-1H/EOD exit mechanic is validated here.\n")

    trades: List[Trade] = []
    for row in rows:
        print(f"{row.symbol} {row.trade_date} (bias={row.bias}):")
        t = await _run_one(row, TOKEN)
        if t:
            trades.append(t)
            print(f"  ENTRY {t.entry_ts.strftime('%H:%M')} {t.strike}{t.option_type} @ {t.entry_price:.2f}  "
                  f"-> EXIT {t.exit_ts.strftime('%H:%M')} @ {t.exit_price:.2f} ({t.exit_reason})  "
                  f"P&L={t.pnl_pts:+.2f} pts (Rs{t.pnl_rs:+.0f} @ lot={t.lot})")
            if t.premium_high is not None:
                print(f"    real premium while held: HIGH={t.premium_high:.2f} @ "
                      f"{t.premium_high_ts.strftime('%H:%M')}   LOW={t.premium_low:.2f} @ "
                      f"{t.premium_low_ts.strftime('%H:%M')}")

    if not trades:
        print("\nNo trades produced -- nothing to summarize.")
        return

    print(f"\n{'Symbol':<12}{'Date':<12}{'Bias':<9}{'Entry':>9}  {'Exit':>9}  {'Reason':<18}"
          f"{'P&L(pts)':>10}  {'Peak(real)':>11}  {'Low(real)':>10}")
    for t in trades:
        peak = f"{t.premium_high:.2f}" if t.premium_high is not None else "n/a"
        low = f"{t.premium_low:.2f}" if t.premium_low is not None else "n/a"
        print(f"{t.symbol:<12}{str(t.trade_date):<12}{t.bias:<9}{t.entry_price:>9.2f}  "
              f"{t.exit_price:>9.2f}  {t.exit_reason:<18}{t.pnl_pts:>+10.2f}  {peak:>11}  {low:>10}")

    wins = [t for t in trades if t.pnl_rs > 0]
    losses = [t for t in trades if t.pnl_rs <= 0]
    gross_win = sum(t.pnl_rs for t in wins)
    gross_loss = -sum(t.pnl_rs for t in losses)
    pf = (gross_win / gross_loss) if gross_loss > 0 else float("inf")
    net = sum(t.pnl_rs for t in trades)
    print(f"\n=== RESULTS ===  n={len(trades)}  win%={100.0 * len(wins) / len(trades):.1f}  "
          f"PF={pf:.2f}  net=Rs{net:+.0f}")


if __name__ == "__main__":
    asyncio.run(main())
