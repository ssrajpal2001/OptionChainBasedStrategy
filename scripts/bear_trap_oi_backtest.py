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
from datetime import date, datetime, timedelta
from itertools import groupby
from typing import Callable, Literal, Optional

from strategies.bear_trap_oi.models import Bar, TrapZone, TrapZoneState
from strategies.bear_trap_oi.trap_detector import (
    on_bar_close, check_zone_reentry, close_position,
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
    pnl_pct: float
    c1: Bar
    c2: Bar
    mfe_price: float   # Maximum Favorable Excursion: highest premium reached after entry, before EOD
    mfe_ts: datetime
    mae_price: float   # Maximum Adverse Excursion: lowest premium reached after entry, before EOD
    mae_ts: datetime


def _fresh_zone() -> TrapZone:
    return TrapZone(state=TrapZoneState.WAITING, c1=None, c2=None,
                     zone_lo=None, zone_hi=None, confirmed_ts=None)


def run_side_backtest(bars: list[Bar], side: Side, strike: int,
                       lot_qty: int,
                       logger: Optional[Callable[[str], None]] = None,
                       ) -> list[BacktestTrade]:
    """Replay one trading day's bars for one side through the real
    detector. OI filter is bypassed (see module docstring).

    When `logger` is given, every state transition (C1 reference, C2
    breakdown, trap confirmation + zone, re-entry/entry, EOD exit) is
    emitted to it as a timestamped, human-readable line -- so a reviewer
    can see exactly why (or why not) a trade fired on a given day, not
    just the final trade list."""
    trades: list[BacktestTrade] = []
    zone = _fresh_zone()
    open_entry: tuple[float, datetime, Bar, Bar] | None = None  # price, ts, c1, c2
    mfe_price = mfe_ts = mae_price = mae_ts = None

    def log(msg: str) -> None:
        if logger is not None:
            logger(f"[{side} {strike}] {msg}")

    for i, bar in enumerate(bars):
        is_last_bar = i == len(bars) - 1

        if zone.state == TrapZoneState.ARMED_WAIT_REENTRY and open_entry is None:
            # Drives the REAL check_zone_reentry() against the bar's CLOSE
            # (backtest has no sub-bar ticks, so the bar close is the best
            # available stand-in for "live price") -- matches the live
            # engine's own semantics exactly: a bar whose range merely
            # swept through the zone without closing inside it is NOT a
            # valid re-entry.
            if check_zone_reentry(zone, bar.close):
                entry_price = bar.close
                open_entry = (entry_price, bar.ts, zone.c1, zone.c2)
                mfe_price, mfe_ts = entry_price, bar.ts
                mae_price, mae_ts = entry_price, bar.ts
                log(f"ZONE RE-ENTRY & ENTRY @{bar.ts} price={entry_price} "
                    f"(zone was [{zone.zone_lo}, {zone.zone_hi}])")
                zone = TrapZone(state=TrapZoneState.IN_POSITION, c1=zone.c1,
                                 c2=zone.c2, zone_lo=zone.zone_lo,
                                 zone_hi=zone.zone_hi,
                                 confirmed_ts=zone.confirmed_ts)
                continue  # don't also run on_bar_close on the entry bar

        if zone.state == TrapZoneState.IN_POSITION and open_entry is not None:
            # Post-entry excursion tracking (MFE/MAE), every bar through EOD.
            if bar.high > mfe_price:
                mfe_price, mfe_ts = bar.high, bar.ts
            if bar.low < mae_price:
                mae_price, mae_ts = bar.low, bar.ts

        if zone.state not in (TrapZoneState.IN_POSITION,):
            prev_state = zone.state
            zone = on_bar_close(zone, bar)
            if prev_state == TrapZoneState.WAITING and zone.state == TrapZoneState.BREAKDOWN_WATCH:
                log(f"C1 (CANDLE 1 / REFERENCE) @{bar.ts} O={bar.open} "
                    f"H={bar.high} L={bar.low} C={bar.close}")
            elif prev_state == TrapZoneState.BREAKDOWN_WATCH and zone.state == TrapZoneState.TRAP_WATCH:
                log(f"C2 (CANDLE 2 / BREAKDOWN) @{bar.ts} low={bar.low} "
                    f"-> zone_lo={zone.zone_lo}")
            elif prev_state == TrapZoneState.BREAKDOWN_WATCH and zone.c1 is bar:
                log(f"C1 (CANDLE 1 / REFERENCE, rolled -- no breakdown yet) "
                    f"@{bar.ts} O={bar.open} H={bar.high} L={bar.low} C={bar.close}")
            elif prev_state == TrapZoneState.TRAP_WATCH and zone.state == TrapZoneState.ARMED_WAIT_REENTRY:
                log(f"TRAP CONFIRMED @{bar.ts} close={bar.close} "
                    f"-> zone_hi={zone.zone_hi}")
                log(f"ZONE ACTIVE [{zone.zone_lo}, {zone.zone_hi}]")

        if is_last_bar and open_entry is not None:
            entry_price, entry_ts, c1, c2 = open_entry
            exit_price = bar.close
            pnl = (exit_price - entry_price) * lot_qty
            pnl_pct = ((exit_price - entry_price) / entry_price) * 100 if entry_price else 0.0
            log(f"EXIT (EOD) @{bar.ts} price={exit_price} "
                f"pnl_pts={pnl:.2f} pnl_pct={pnl_pct:.2f}%")
            log(f"  MFE (highest after entry) = {mfe_price} @{mfe_ts}")
            log(f"  MAE (lowest after entry)  = {mae_price} @{mae_ts}")
            trades.append(BacktestTrade(
                side=side, strike=strike, entry_price=entry_price,
                entry_ts=entry_ts, exit_price=exit_price, exit_ts=bar.ts,
                pnl=pnl, pnl_pct=pnl_pct, c1=c1, c2=c2,
                mfe_price=mfe_price, mfe_ts=mfe_ts,
                mae_price=mae_price, mae_ts=mae_ts,
            ))
            open_entry = None
            mfe_price = mfe_ts = mae_price = mae_ts = None
            zone = close_position(zone)

    return trades


def compute_daily_strikes(daily_candles: list[dict],
                           step: int) -> list[tuple[date, int, int]]:
    """For EVERY tradeable day in a daily-candle series (oldest-first dicts
    with 'ts'/'high'/'low'), computes that day's OWN ce_strike/pe_strike
    from its immediately preceding day's PDH/PDL -- per spec Section 3,
    strikes are fixed for a given trading day but must be recomputed fresh
    each day, never reused across the whole backtest window.

    Returns [(trading_day, ce_strike, pe_strike), ...] for every day from
    index 1 onward (index 0 has no preceding day and is not tradeable)."""
    from strategies.bear_trap_oi.strike_selector import map_strikes

    if len(daily_candles) < 2:
        return []

    out: list[tuple[date, int, int]] = []
    for i in range(1, len(daily_candles)):
        prev = daily_candles[i - 1]
        trading_day = datetime.fromisoformat(daily_candles[i]["ts"]).date()
        ce_strike, pe_strike = map_strikes(prev["high"], prev["low"], step)
        out.append((trading_day, ce_strike, pe_strike))
    return out


def _group_by_trading_day(bars: list[Bar]) -> list[list[Bar]]:
    keyfunc = lambda b: b.ts.date()
    return [list(g) for _, g in groupby(bars, key=keyfunc)]


def _truncate_to_eod(bars: list[Bar], eod_hour: int = 15,
                      eod_minute: int = 15) -> list[Bar]:
    """Drops any bar after the spec's EOD square-off time (3:15 PM IST,
    spec Section 6) so run_side_backtest's own last-bar-is-EOD convention
    force-closes at a bar the live engine would actually have been allowed
    to still be open on -- not a post-close bar Upstox happens to supply
    (e.g. 15:35) that the real strategy would never see."""
    return [b for b in bars
            if (b.ts.hour, b.ts.minute) <= (eod_hour, eod_minute)]


def _candle_dicts_to_bars(rows: list[dict]) -> list[Bar]:
    """Converts historical_candles.py's raw {'ts','open','high','low',
    'close',...} dicts (ts = Upstox ISO-8601 string) into our Bar
    dataclass."""
    out: list[Bar] = []
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        out.append(Bar(ts=ts, open=r["open"], high=r["high"], low=r["low"],
                        close=r["close"]))
    return out


def _resample_1m_to_5m(bars_1m: list[Bar], bucket_minutes: int = 5) -> list[Bar]:
    """Groups real 1-min OHLC bars into N-min buckets using each bar's own
    high/low (not its close alone) -- feeding only closes through a
    tick-style accumulator would silently discard any intra-bucket extreme
    that never happened to be a 1-min bar's own closing price, corrupting
    the trap detector's breakdown/zone/re-entry reads of bucket high/low."""
    out: list[Bar] = []
    bucket_ts = None
    o = h = l = c = None

    def _bucket_start(ts: datetime) -> datetime:
        floored_minute = (ts.minute // bucket_minutes) * bucket_minutes
        return ts.replace(minute=floored_minute, second=0, microsecond=0)

    for b in bars_1m:
        this_bucket = _bucket_start(b.ts)
        if bucket_ts is None:
            bucket_ts, o, h, l, c = this_bucket, b.open, b.high, b.low, b.close
            continue
        if this_bucket != bucket_ts:
            out.append(Bar(ts=bucket_ts, open=o, high=h, low=l, close=c))
            bucket_ts, o, h, l, c = this_bucket, b.open, b.high, b.low, b.close
            continue
        h = max(h, b.high)
        l = min(l, b.low)
        c = b.close

    if bucket_ts is not None:
        out.append(Bar(ts=bucket_ts, open=o, high=h, low=l, close=c))

    return out


def _get_upstox_access_token() -> str:
    """Reads the Upstox access token from the platform's own credentials
    store (data/clients.db, system_feeder_creds table) -- same source
    every other live/backtest script in this codebase uses."""
    import sqlite3

    conn = sqlite3.connect("data/clients.db")
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT access_token FROM system_feeder_creds WHERE provider='upstox'"
        )
        row = cur.fetchone()
        if not row or not row[0]:
            raise RuntimeError(
                "No Upstox access_token found in data/clients.db "
                "(system_feeder_creds) -- cannot run a real-data backtest."
            )
        return row[0]
    finally:
        conn.close()


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
              f"pnl={t.pnl:.2f} ({t.pnl_pct:.2f}%)")


def _print_mfe_mae_table(all_trades: list[BacktestTrade]) -> None:
    print("\n=== MFE / MAE Summary (for stop-loss & target design) ===")
    header = (f"{'Side':<4} {'Strike':<7} {'Entry Price':<12} {'Entry Time':<22} "
               f"{'MFE Price':<10} {'MFE Time':<22} {'MAE Price':<10} "
               f"{'MAE Time':<22} {'EOD Exit':<9} {'P&L (pts)':<10} {'P&L %':<8}")
    print(header)
    print("-" * len(header))
    for t in sorted(all_trades, key=lambda t: t.entry_ts):
        print(f"{t.side:<4} {t.strike:<7} {t.entry_price:<12} "
              f"{str(t.entry_ts):<22} {t.mfe_price:<10} {str(t.mfe_ts):<22} "
              f"{t.mae_price:<10} {str(t.mae_ts):<22} {t.exit_price:<9} "
              f"{t.pnl:<10.2f} {t.pnl_pct:<8.2f}")


# ─────────────────────────────────────────────────────────────────────────
# EXPERIMENTAL: dynamic 100-pt ITM strike rolling + Fibonacci dual-lot
# target/TSL management (direct user request, 2026-10-03). This is a
# backtest-only exploratory mechanic layered on top of the locked
# price-action trap detector -- it does NOT touch strategies/bear_trap_oi/
# (the core, spec-locked package) and is not itself part of the approved
# live spec (docs/superpowers/specs/2026-10-03-bear-trap-oi-design.md),
# which still fixes one PDH/PDL strike per day, single-lot, EOD-only exit.
# Kept self-contained here so the locked core stays stable while this
# variation is explored.
# ─────────────────────────────────────────────────────────────────────────

def detect_first_local_high(bars: list[Bar]) -> tuple[float, datetime] | None:
    """First local high after an impulse move: the first bar whose high is
    immediately followed by a bar with a strictly lower high (a real
    pullback). Returns (high_price, ts) of that first bar, or None if the
    series never shows a pullback (still strictly ascending, or too short
    to tell)."""
    for i in range(len(bars) - 1):
        if bars[i + 1].high < bars[i].high:
            return bars[i].high, bars[i].ts
    return None


def fib_extension_levels(entry_price: float, swing_high: float) -> dict:
    """1.272 / 1.618 Fibonacci extension targets off the Entry(0) ->
    swing_high(100%) base."""
    span = swing_high - entry_price
    return {
        "1.272": entry_price + span * 1.272,
        "1.618": entry_price + span * 1.618,
    }


def compute_itm_strike(spot: float, points: int, step: int, side: Side) -> int:
    """The strike `points` points in-the-money for `side` relative to
    `spot`: below spot for CE, above spot for PE."""
    from strategies.bear_trap_oi.strike_selector import round_to_strike_step
    target = spot - points if side == "CE" else spot + points
    return round_to_strike_step(target, step)


def compute_roll_schedule(spot_bars: list[Bar], initial_spot: float, step: int,
                           roll_points: int = 100,
                           ) -> list[tuple[datetime, int, int]]:
    """Walks spot bars (close-to-close) and emits a (ts, ce_strike,
    pe_strike) event every time price has moved `roll_points` from the
    last anchor, re-anchoring the reference spot to that bar's close each
    time. Does not emit an initial event for `initial_spot` itself --
    callers already have the 09:15 PDH/PDL-derived anchor strikes."""
    events: list[tuple[datetime, int, int]] = []
    anchor = initial_spot
    for bar in spot_bars:
        if abs(bar.close - anchor) >= roll_points:
            ce_strike = compute_itm_strike(bar.close, roll_points, step, "CE")
            pe_strike = compute_itm_strike(bar.close, roll_points, step, "PE")
            events.append((bar.ts, ce_strike, pe_strike))
            anchor = bar.close
    return events


def manage_fib_trade(bars_after_entry: list[Bar], entry_price: float,
                      lot_qty_each: int) -> dict:
    """Dual-lot Fib-extension trade management for ONE already-identified
    entry (spec: 2 lots in, 1 lot booked at the 1.272 extension, remaining
    lot managed two ways for comparison):

    - Variant A: once lot 1 books at 1.272, TSL for lot 2 moves to
      breakeven (entry_price); lot 2 exits at breakeven, 1.618, or EOD,
      whichever comes first.
    - Variant B: no TSL change after lot 1 books; lot 2 only exits at
      1.618 or EOD.

    `bars_after_entry` may be a STITCHED sequence spanning more than one
    option contract (see run_dynamic_side_backtest) -- entry_price and the
    resulting fib_1272/fib_1618 are plain numbers, so switching the bar
    SOURCE mid-sequence (per the user's explicit instruction: an open
    trade's target-tracking rolls onto the new 100-pt ITM strike's raw
    premium, while entry_price/P&L basis stays on the original contract)
    needs no special handling here -- the caller just hands over whichever
    bars are "current" at each point in time."""
    if not bars_after_entry:
        return {
            "swing_high": None, "swing_high_ts": None,
            "fib_1272": None, "fib_1618": None,
            "lot1_exit_price": entry_price, "lot1_exit_ts": None,
            "lot1_exit_reason": "no_data", "lot1_pnl": 0.0,
            "variant_a": {"lot2_exit_price": entry_price, "lot2_exit_ts": None,
                          "lot2_exit_reason": "no_data", "lot2_pnl": 0.0,
                          "total_pnl": 0.0},
            "variant_b": {"lot2_exit_price": entry_price, "lot2_exit_ts": None,
                          "lot2_exit_reason": "no_data", "lot2_pnl": 0.0,
                          "total_pnl": 0.0},
        }

    eod_bar = bars_after_entry[-1]
    swing = detect_first_local_high(bars_after_entry)

    if swing is None:
        eod_price = eod_bar.close
        pnl = (eod_price - entry_price) * lot_qty_each
        leg = {"lot2_exit_price": eod_price, "lot2_exit_ts": eod_bar.ts,
               "lot2_exit_reason": "eod_no_swing_high", "lot2_pnl": pnl,
               "total_pnl": pnl * 2}
        return {
            "swing_high": None, "swing_high_ts": None,
            "fib_1272": None, "fib_1618": None,
            "lot1_exit_price": eod_price, "lot1_exit_ts": eod_bar.ts,
            "lot1_exit_reason": "eod_no_swing_high", "lot1_pnl": pnl,
            "variant_a": dict(leg), "variant_b": dict(leg),
        }

    swing_high, swing_high_ts = swing
    levels = fib_extension_levels(entry_price, swing_high)
    fib_1272, fib_1618 = levels["1.272"], levels["1.618"]

    lot1_idx = None
    for i, b in enumerate(bars_after_entry):
        if b.high >= fib_1272:
            lot1_idx = i
            break

    if lot1_idx is None:
        eod_price = eod_bar.close
        pnl = (eod_price - entry_price) * lot_qty_each
        leg = {"lot2_exit_price": eod_price, "lot2_exit_ts": eod_bar.ts,
               "lot2_exit_reason": "eod_target_not_reached", "lot2_pnl": pnl,
               "total_pnl": pnl * 2}
        return {
            "swing_high": swing_high, "swing_high_ts": swing_high_ts,
            "fib_1272": fib_1272, "fib_1618": fib_1618,
            "lot1_exit_price": eod_price, "lot1_exit_ts": eod_bar.ts,
            "lot1_exit_reason": "eod_target_not_reached", "lot1_pnl": pnl,
            "variant_a": dict(leg), "variant_b": dict(leg),
        }

    lot1_bar = bars_after_entry[lot1_idx]
    lot1_exit_price = fib_1272
    lot1_pnl = (lot1_exit_price - entry_price) * lot_qty_each
    remaining = bars_after_entry[lot1_idx + 1:]

    def _variant(use_tsl: bool) -> dict:
        if not remaining:
            return {"lot2_exit_price": lot1_exit_price, "lot2_exit_ts": lot1_bar.ts,
                    "lot2_exit_reason": "target_1272_same_bar",
                    "lot2_pnl": lot1_pnl, "total_pnl": lot1_pnl * 2}
        for b in remaining:
            if use_tsl and b.low <= entry_price:
                return {"lot2_exit_price": entry_price, "lot2_exit_ts": b.ts,
                        "lot2_exit_reason": "tsl_breakeven", "lot2_pnl": 0.0,
                        "total_pnl": lot1_pnl}
            if b.high >= fib_1618:
                pnl2 = (fib_1618 - entry_price) * lot_qty_each
                return {"lot2_exit_price": fib_1618, "lot2_exit_ts": b.ts,
                        "lot2_exit_reason": "target_1618", "lot2_pnl": pnl2,
                        "total_pnl": lot1_pnl + pnl2}
        last = remaining[-1]
        pnl2 = (last.close - entry_price) * lot_qty_each
        return {"lot2_exit_price": last.close, "lot2_exit_ts": last.ts,
                "lot2_exit_reason": "eod", "lot2_pnl": pnl2,
                "total_pnl": lot1_pnl + pnl2}

    return {
        "swing_high": swing_high, "swing_high_ts": swing_high_ts,
        "fib_1272": fib_1272, "fib_1618": fib_1618,
        "lot1_exit_price": lot1_exit_price, "lot1_exit_ts": lot1_bar.ts,
        "lot1_exit_reason": "target_1272", "lot1_pnl": lot1_pnl,
        "variant_a": _variant(use_tsl=True), "variant_b": _variant(use_tsl=False),
    }


def run_dynamic_side_backtest(bars_by_strike: dict[int, dict[datetime, Bar]],
                               master_ts: list[datetime], initial_strike: int,
                               side_roll_schedule: list[tuple[datetime, int]],
                               side: Side, lot_qty_each: int,
                               logger: Optional[Callable[[str], None]] = None,
                               ) -> Optional[dict]:
    """One trading day, one side: runs the real trap detector while FLAT,
    switching the WATCHED strike (and resetting the detector) on every
    scheduled roll -- then, once a position opens, keeps tracking the
    SAME entry_price/fib levels but switches the post-entry bar SOURCE to
    whichever strike is current at each later timestamp (direct user
    spec: target-tracking rolls onto the new 100-pt ITM strike's raw
    premium even for an already-open trade; the entry/P&L basis itself
    does not move). Returns one trade result dict, or None if no entry
    fired this day (still one trade per day per side, matching
    run_side_backtest's own EOD-batch scope)."""

    def log(msg: str) -> None:
        if logger is not None:
            logger(f"[{side} dyn] {msg}")

    current_strike = initial_strike
    roll_ptr = 0
    zone = _fresh_zone()
    entry_price = entry_ts = entry_strike = None
    entry_idx = None

    def _apply_pending_rolls(ts: datetime, flat: bool) -> None:
        nonlocal current_strike, roll_ptr, zone
        while roll_ptr < len(side_roll_schedule) and side_roll_schedule[roll_ptr][0] <= ts:
            _, new_strike = side_roll_schedule[roll_ptr]
            roll_ptr += 1
            if new_strike == current_strike:
                continue
            if flat:
                log(f"ROLL (flat) @{ts} {current_strike} -> {new_strike} "
                    f"(detector reset -- new contract, old candle refs invalid)")
                zone = _fresh_zone()
            else:
                log(f"ROLL (in-position, target-tracking only) @{ts} "
                    f"{current_strike} -> {new_strike}")
            current_strike = new_strike

    # Phase 1: scan while flat for an entry.
    for i, ts in enumerate(master_ts):
        _apply_pending_rolls(ts, flat=True)
        bar = bars_by_strike.get(current_strike, {}).get(ts)
        if bar is None:
            continue

        if zone.state == TrapZoneState.ARMED_WAIT_REENTRY and check_zone_reentry(zone, bar.close):
            entry_price, entry_ts, entry_strike = bar.close, bar.ts, current_strike
            entry_idx = i
            log(f"ZONE RE-ENTRY & ENTRY @{bar.ts} strike={current_strike} "
                f"price={entry_price} (zone was [{zone.zone_lo}, {zone.zone_hi}])")
            break

        prev_state = zone.state
        zone = on_bar_close(zone, bar)
        if prev_state == TrapZoneState.WAITING and zone.state == TrapZoneState.BREAKDOWN_WATCH:
            log(f"C1 @{bar.ts} strike={current_strike} O={bar.open} H={bar.high} "
                f"L={bar.low} C={bar.close}")
        elif prev_state == TrapZoneState.BREAKDOWN_WATCH and zone.state == TrapZoneState.TRAP_WATCH:
            log(f"C2 (BREAKDOWN) @{bar.ts} strike={current_strike} low={bar.low} "
                f"-> zone_lo={zone.zone_lo}")
        elif prev_state == TrapZoneState.TRAP_WATCH and zone.state == TrapZoneState.ARMED_WAIT_REENTRY:
            log(f"TRAP CONFIRMED @{bar.ts} strike={current_strike} "
                f"close={bar.close} -> zone_hi={zone.zone_hi}")
            log(f"ZONE ACTIVE [{zone.zone_lo}, {zone.zone_hi}]")

    if entry_price is None:
        return None

    # Phase 2: accumulate post-entry bars, switching source strike on any
    # later roll, through to the end of the day's master_ts.
    post_entry_bars: list[Bar] = []
    for ts in master_ts[entry_idx + 1:]:
        _apply_pending_rolls(ts, flat=False)
        bar = bars_by_strike.get(current_strike, {}).get(ts)
        if bar is not None:
            post_entry_bars.append(bar)

    result = manage_fib_trade(post_entry_bars, entry_price, lot_qty_each)
    log(f"SWING HIGH = {result['swing_high']} @{result['swing_high_ts']} "
        f"FIB_1272={result['fib_1272']} FIB_1618={result['fib_1618']}")
    log(f"LOT1 EXIT @{result['lot1_exit_ts']} price={result['lot1_exit_price']} "
        f"reason={result['lot1_exit_reason']}")
    log(f"VARIANT A LOT2 EXIT @{result['variant_a']['lot2_exit_ts']} "
        f"price={result['variant_a']['lot2_exit_price']} "
        f"reason={result['variant_a']['lot2_exit_reason']} "
        f"total_pnl={result['variant_a']['total_pnl']}")
    log(f"VARIANT B LOT2 EXIT @{result['variant_b']['lot2_exit_ts']} "
        f"price={result['variant_b']['lot2_exit_price']} "
        f"reason={result['variant_b']['lot2_exit_reason']} "
        f"total_pnl={result['variant_b']['total_pnl']}")

    return {
        "side": side, "entry_strike": entry_strike, "entry_price": entry_price,
        "entry_ts": entry_ts, **result,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--strike-step", type=int, default=50)
    parser.add_argument("--lot-qty", type=int, default=75)
    args = parser.parse_args()

    from config.global_config import IST
    from data_layer.historical_candles import fetch_upstox_daily, fetch_upstox_range_1m
    from data_layer.instrument_registry import REGISTRY

    access_token = _get_upstox_access_token()
    today = datetime.now().date()

    index_key = REGISTRY.get_upstox_index_key("NIFTY")
    # +3 lookback margin so the FIRST tradeable day in --days still has a
    # real preceding day's candle to compute PDH/PDL from.
    daily = await fetch_upstox_daily(index_key, access_token,
                                      lookback_days=args.days + 3)
    daily_strikes = compute_daily_strikes(daily, args.strike_step)
    # Keep only the trading days actually within the requested window.
    cutoff = today - timedelta(days=args.days)
    daily_strikes = [row for row in daily_strikes if row[0] >= cutoff]

    if not daily_strikes:
        raise RuntimeError(
            f"No tradeable days with a resolvable preceding-day PDH/PDL "
            f"found in the last {args.days} day(s) for {index_key}."
        )

    REGISTRY.load_sync("NIFTY", access_token)

    print(f"=== Bear Trap OI backtest: {len(daily_strikes)} trading day(s) in "
          f"window, daily strikes recomputed per spec Section 3 ===\n")

    all_trades: list[BacktestTrade] = []
    all_dynamic_trades: list[dict] = []

    print("\n" + "=" * 78)
    print("SECTION 1: FIXED daily PDH/PDL strike, single-lot, EOD-only "
          "(locked spec baseline)")
    print("=" * 78)

    for trading_day, ce_strike, pe_strike in daily_strikes:
        expiry = REGISTRY.get_active_expiry_strict("NIFTY", from_date=trading_day)
        if expiry is None:
            print(f"[{trading_day}] SKIP -- could not resolve active expiry")
            continue

        ce_key = REGISTRY.get_upstox_key("NIFTY", expiry, ce_strike, "CE")
        pe_key = REGISTRY.get_upstox_key("NIFTY", expiry, pe_strike, "PE")
        print(f"[{trading_day}] DAY START: expiry={expiry} "
              f"ce_strike={ce_strike} pe_strike={pe_strike}")
        if not ce_key or not pe_key:
            print(f"[{trading_day}] SKIP -- could not resolve Upstox keys "
                  f"for CE {ce_strike}/PE {pe_strike} @ expiry {expiry}")
            continue

        ce_rows = await fetch_upstox_range_1m(ce_key, access_token,
                                               trading_day, trading_day)
        pe_rows = await fetch_upstox_range_1m(pe_key, access_token,
                                               trading_day, trading_day)
        if not ce_rows and not pe_rows:
            print(f"[{trading_day}] SKIP -- no premium history returned "
                  f"for either leg (holiday or no data)")
            continue

        ce_bars = _truncate_to_eod(_resample_1m_to_5m(_candle_dicts_to_bars(ce_rows)))
        pe_bars = _truncate_to_eod(_resample_1m_to_5m(_candle_dicts_to_bars(pe_rows)))
        print(f"[{trading_day}] fetched {len(ce_rows)} CE 1-min bars "
              f"({len(ce_bars)} 5-min), {len(pe_rows)} PE 1-min bars "
              f"({len(pe_bars)} 5-min)")

        all_trades += run_side_backtest(ce_bars, "CE", ce_strike, args.lot_qty,
                                         logger=print)
        all_trades += run_side_backtest(pe_bars, "PE", pe_strike, args.lot_qty,
                                         logger=print)
        print()

    _print_report("CE", [t for t in all_trades if t.side == "CE"])
    _print_report("PE", [t for t in all_trades if t.side == "PE"])
    print(f"\n=== Combined: {len(all_trades)} trade(s), "
          f"Total P&L: {sum(t.pnl for t in all_trades):.2f} ===")
    _print_mfe_mae_table(all_trades)

    print("\n" + "=" * 78)
    print("SECTION 2 (EXPERIMENTAL): simultaneous CE+PE, dynamic 100-pt ITM "
          "strike rolling, Fib 1.272/1.618 dual-lot management (2 lots in, "
          "1 lot @1.272, remaining lot: Variant A = TSL->breakeven, "
          "Variant B = no TSL, runs to 1.618/EOD)")
    print("=" * 78)

    for trading_day, ce_strike, pe_strike in daily_strikes:
        expiry = REGISTRY.get_active_expiry_strict("NIFTY", from_date=trading_day)
        if expiry is None:
            continue

        spot_rows = await fetch_upstox_range_1m(index_key, access_token,
                                                  trading_day, trading_day)
        if not spot_rows:
            print(f"[{trading_day}] SKIP (dynamic) -- no spot history returned")
            continue
        spot_bars = _truncate_to_eod(_resample_1m_to_5m(_candle_dicts_to_bars(spot_rows)))
        if not spot_bars:
            continue
        initial_spot = spot_bars[0].open
        roll_schedule = compute_roll_schedule(spot_bars, initial_spot,
                                               args.strike_step, roll_points=100)
        print(f"[{trading_day}] spot open={initial_spot}, "
              f"{len(roll_schedule)} roll event(s): "
              f"{[(str(ts), ce, pe) for ts, ce, pe in roll_schedule]}")

        ce_strikes_needed = {ce_strike} | {ce for _, ce, _ in roll_schedule}
        pe_strikes_needed = {pe_strike} | {pe for _, _, pe in roll_schedule}

        async def _fetch_strike_bars(strike: int, opt_type: str) -> dict[datetime, Bar]:
            key = REGISTRY.get_upstox_key("NIFTY", expiry, strike, opt_type)
            if not key:
                return {}
            rows = await fetch_upstox_range_1m(key, access_token, trading_day,
                                                trading_day)
            bars = _truncate_to_eod(_resample_1m_to_5m(_candle_dicts_to_bars(rows)))
            return {b.ts: b for b in bars}

        ce_bars_by_strike = {s: await _fetch_strike_bars(s, "CE") for s in ce_strikes_needed}
        pe_bars_by_strike = {s: await _fetch_strike_bars(s, "PE") for s in pe_strikes_needed}

        master_ts = sorted({ts for bars in ce_bars_by_strike.values() for ts in bars} |
                            {ts for bars in pe_bars_by_strike.values() for ts in bars})
        if not master_ts:
            continue

        ce_roll_sched = [(ts, ce) for ts, ce, _ in roll_schedule]
        pe_roll_sched = [(ts, pe) for ts, _, pe in roll_schedule]

        ce_dyn = run_dynamic_side_backtest(ce_bars_by_strike, master_ts, ce_strike,
                                            ce_roll_sched, "CE", args.lot_qty,
                                            logger=print)
        pe_dyn = run_dynamic_side_backtest(pe_bars_by_strike, master_ts, pe_strike,
                                            pe_roll_sched, "PE", args.lot_qty,
                                            logger=print)
        for t in (ce_dyn, pe_dyn):
            if t is not None:
                all_dynamic_trades.append(t)
        print()

    print("\n=== Dynamic/Fib Summary ===")
    header = (f"{'Side':<4} {'EntryStrike':<11} {'Entry':<8} {'EntryTime':<22} "
               f"{'SwingHi':<8} {'Fib1272':<9} {'Fib1618':<9} "
               f"{'VarA Exit':<10} {'VarA Reason':<16} {'VarA PnL':<10} "
               f"{'VarB Exit':<10} {'VarB Reason':<16} {'VarB PnL':<10}")
    print(header)
    print("-" * len(header))
    total_a = total_b = 0.0
    for t in sorted(all_dynamic_trades, key=lambda t: t["entry_ts"]):
        va, vb = t["variant_a"], t["variant_b"]
        total_a += va["total_pnl"]
        total_b += vb["total_pnl"]
        print(f"{t['side']:<4} {t['entry_strike']:<11} {t['entry_price']:<8} "
              f"{str(t['entry_ts']):<22} {str(t['swing_high']):<8} "
              f"{str(t['fib_1272']):<9} {str(t['fib_1618']):<9} "
              f"{va['lot2_exit_price']:<10} {va['lot2_exit_reason']:<16} "
              f"{va['total_pnl']:<10.2f} {vb['lot2_exit_price']:<10} "
              f"{vb['lot2_exit_reason']:<16} {vb['total_pnl']:<10.2f}")
    print(f"\n=== Dynamic/Fib Combined: {len(all_dynamic_trades)} trade(s) -- "
          f"Variant A total P&L: {total_a:.2f}  |  "
          f"Variant B total P&L: {total_b:.2f} ===")


if __name__ == "__main__":
    asyncio.run(main())
