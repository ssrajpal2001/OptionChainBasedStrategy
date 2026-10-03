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
from datetime import date, datetime, time, timedelta
from collections import Counter
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

def fib_extension_levels(base_price: float, top_price: float,
                          ratio1: float = 1.618, ratio2: float = 2.618,
                          ) -> tuple[float, float]:
    """(target1, target2) Fibonacci extension prices off a base(0%) ->
    top(100%) span, at `ratio1`/`ratio2`. Generic -- callers decide what
    base/top mean (see manage_fib_trade: base = trap zone low, top =
    running peak high). Defaults widened to 1.618/2.618 (2026-10-03,
    direct user fix) -- the original 1.272/1.618 pair was found, via a
    real-trade audit, to book both lots far too early relative to the
    actual continuation these breakout trades tend to produce."""
    span = top_price - base_price
    return base_price + span * ratio1, base_price + span * ratio2


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
                      zone_lo: float, lot_qty_each: int,
                      min_impulse_pct: float = 0.02,
                      fallback_r1: float = 2.0, fallback_r2: float = 3.0,
                      fib_ratio1: float = 1.618, fib_ratio2: float = 2.618,
                      ) -> dict:
    """Dual-lot target/TSL management for ONE already-identified entry
    (2026-10-03 rewrite, direct user fix for the earlier 1-bar
    micro-pullback flaw):

    - Fib span is now anchored on the TRAP ZONE LOW (0%, `zone_lo` -- the
      breakdown low from the C1/C2 sequence that triggered this entry) to
      the RUNNING PEAK HIGH reached so far during the trade's own
      lifecycle (100%), recomputed bar by bar as the peak grows -- never
      a single post-entry pullback bar.
    - MINIMUM IMPULSE GATE: until the running peak has risen at least
      `min_impulse_pct` (default 2%) above entry_price, the zone_lo-based
      Fib span is not trusted (it would still be a near-entry micro-span)
      -- target1/target2 fall back to a fixed R-multiple off
      `entry_price - zone_lo` (the trade's own structural risk):
      target1 = entry + fallback_r1*risk, target2 = entry + fallback_r2*risk.
      The moment the peak clears the gate, later bars switch to the real
      zone_lo->peak Fib 1.272/1.618 extension instead.
    - A target is checked against the PRIOR bar's peak/mode (computed
      before folding the current bar's own high into the running peak),
      so a bar cannot both set a brand-new peak AND simultaneously clear
      the target that peak implies (fib ratios >1 make that impossible by
      construction) -- the peak only updates for bars AFTER this check.
    - Target ratios widened to 1.618/2.618 (fib mode) and 2.0R/3.0R
      (fallback mode), per direct user fix (2026-10-03): an audit of 3
      real trades showed 1.272/1.618 booked both lots after capturing
      only ~10-16% of the eventual peak move.
    - Variant A: once lot 1 books, lot 2 trails a STOP at the most
      recent 3-bar low (ratcheting UP only, never down), re-evaluated one
      bar at a time -- never a flat breakeven. Direct user fix: a flat
      breakeven stop was found (via the same audit) to kick lot 2 out on
      an ordinary dip minutes before a real continuation leg. The stop is
      checked against the PRIOR bars' low (never the current bar's own,
      same anti-self-reference reasoning as the peak/target check above),
      then the window rolls forward to include the current bar for the
      next one. Lot 2 exits at the stop LEVEL (not the bar's raw low),
      target2, or EOD.
    - Variant B: no TSL; lot 2 only exits at target2 or EOD.

    `bars_after_entry` may be a STITCHED sequence spanning more than one
    option contract (see run_dynamic_side_backtest) -- entry_price/zone_lo
    and the resulting targets are plain numbers, so switching the bar
    SOURCE mid-sequence needs no special handling here."""
    if not bars_after_entry:
        return {
            "mode": "no_data", "target1": None, "target2": None,
            "lot1_exit_price": entry_price, "lot1_exit_ts": None,
            "lot1_exit_reason": "no_data", "lot1_pnl": 0.0,
            "variant_a": {"lot2_exit_price": entry_price, "lot2_exit_ts": None,
                          "lot2_exit_reason": "no_data", "lot2_pnl": 0.0,
                          "total_pnl": 0.0},
            "variant_b": {"lot2_exit_price": entry_price, "lot2_exit_ts": None,
                          "lot2_exit_reason": "no_data", "lot2_pnl": 0.0,
                          "total_pnl": 0.0},
        }

    # Floored risk (2026-10-03 fix, real incident: CE 22600 had a 0.05-pt
    # trap zone, producing a 113.375 fallback target indistinguishable
    # from entry 113.3) -- never smaller than 1% of entry_price, however
    # thin the actual trap zone was.
    risk = max(entry_price - zone_lo, entry_price * 0.01)
    eod_bar = bars_after_entry[-1]
    peak = entry_price
    lot1_idx = None
    lot1_mode = lot1_target = None

    for i, b in enumerate(bars_after_entry):
        qualifies = risk > 0 and (peak - entry_price) / entry_price >= min_impulse_pct
        if qualifies:
            mode = "fib_dynamic_peak"
            target1, _ = fib_extension_levels(zone_lo, peak, fib_ratio1, fib_ratio2)
        else:
            mode = "fallback_fixed_R"
            target1 = entry_price + fallback_r1 * max(risk, 0.0)
        if b.high >= target1:
            lot1_idx, lot1_mode, lot1_target = i, mode, target1
            break
        if b.high > peak:
            peak = b.high

    if lot1_idx is None:
        eod_price = eod_bar.close
        pnl = (eod_price - entry_price) * lot_qty_each
        final_qualifies = risk > 0 and (peak - entry_price) / entry_price >= min_impulse_pct
        if final_qualifies:
            final_target1, final_target2 = fib_extension_levels(zone_lo, peak, fib_ratio1, fib_ratio2)
        else:
            final_target1 = entry_price + fallback_r1 * max(risk, 0.0)
            final_target2 = entry_price + fallback_r2 * max(risk, 0.0)
        final_mode = "fib_dynamic_peak" if final_qualifies else "fallback_fixed_R"
        leg = {"lot2_exit_price": eod_price, "lot2_exit_ts": eod_bar.ts,
               "lot2_exit_reason": "eod_target1_not_reached", "lot2_pnl": pnl,
               "total_pnl": pnl * 2}
        return {
            "mode": final_mode, "target1": final_target1, "target2": final_target2,
            "lot1_exit_price": eod_price, "lot1_exit_ts": eod_bar.ts,
            "lot1_exit_reason": "eod_target1_not_reached", "lot1_pnl": pnl,
            "variant_a": dict(leg), "variant_b": dict(leg),
        }

    lot1_bar = bars_after_entry[lot1_idx]
    lot1_exit_price = lot1_target
    lot1_pnl = (lot1_exit_price - entry_price) * lot_qty_each
    if lot1_mode == "fib_dynamic_peak":
        _, target2 = fib_extension_levels(zone_lo, peak, fib_ratio1, fib_ratio2)
    else:
        target2 = entry_price + fallback_r2 * max(risk, 0.0)
    remaining = bars_after_entry[lot1_idx + 1:]

    def _rolling_3bar_low(upto_global_idx: int) -> float:
        start = max(0, upto_global_idx - 2)
        window = bars_after_entry[start:upto_global_idx + 1]
        return min(b.low for b in window)

    def _variant(use_tsl: bool) -> dict:
        if not remaining:
            return {"lot2_exit_price": lot1_exit_price, "lot2_exit_ts": lot1_bar.ts,
                    "lot2_exit_reason": "target1_same_bar",
                    "lot2_pnl": lot1_pnl, "total_pnl": lot1_pnl * 2}
        tsl = _rolling_3bar_low(lot1_idx) if use_tsl else None
        for offset, b in enumerate(remaining):
            global_idx = lot1_idx + 1 + offset
            if use_tsl and b.low <= tsl:
                pnl2 = (tsl - entry_price) * lot_qty_each
                return {"lot2_exit_price": tsl, "lot2_exit_ts": b.ts,
                        "lot2_exit_reason": "tsl_3bar_low", "lot2_pnl": pnl2,
                        "total_pnl": lot1_pnl + pnl2}
            if b.high >= target2:
                pnl2 = (target2 - entry_price) * lot_qty_each
                return {"lot2_exit_price": target2, "lot2_exit_ts": b.ts,
                        "lot2_exit_reason": "target2_hit", "lot2_pnl": pnl2,
                        "total_pnl": lot1_pnl + pnl2}
            if use_tsl:
                tsl = max(tsl, _rolling_3bar_low(global_idx))
        last = remaining[-1]
        pnl2 = (last.close - entry_price) * lot_qty_each
        return {"lot2_exit_price": last.close, "lot2_exit_ts": last.ts,
                "lot2_exit_reason": "eod", "lot2_pnl": pnl2,
                "total_pnl": lot1_pnl + pnl2}

    return {
        "mode": lot1_mode, "target1": lot1_target, "target2": target2,
        "lot1_exit_price": lot1_exit_price, "lot1_exit_ts": lot1_bar.ts,
        "lot1_exit_reason": "target1_hit", "lot1_pnl": lot1_pnl,
        "variant_a": _variant(use_tsl=True), "variant_b": _variant(use_tsl=False),
    }


def compute_running_excursions(bars_after_entry: list[Bar],
                                entry_price: float) -> list[dict]:
    """Per-bar OHLC plus the RUNNING MFE (max high seen so far, including
    this bar) / MAE (min low seen so far, including this bar) since entry
    -- for a candle-by-candle manual chart audit, not a single final
    number."""
    rows: list[dict] = []
    mfe = mae = entry_price
    for b in bars_after_entry:
        mfe = max(mfe, b.high)
        mae = min(mae, b.low)
        rows.append({"ts": b.ts, "open": b.open, "high": b.high, "low": b.low,
                     "close": b.close, "mfe": mfe, "mae": mae})
    return rows


def format_trade_audit(trade: dict, bars_after_entry: list[Bar]) -> str:
    """Full candle-by-candle audit for ONE dynamic/Fib trade: trade
    identification, a per-bar OHLC+running-MFE/MAE table, and the
    Fibonacci/target validation block for both variants -- the complete
    trail needed to manually re-check this trade against a real chart."""
    lines: list[str] = []
    lines.append(f"TRADE: {trade['side']} strike={trade['entry_strike']} "
                 f"entry_ts={trade['entry_ts']} entry_price={trade['entry_price']}")
    lines.append("")
    lines.append("Candle-by-candle (post-entry, running MFE/MAE):")
    header = f"{'Timestamp':<26} {'Open':<10} {'High':<10} {'Low':<10} {'Close':<10} {'MFE':<10} {'MAE':<10}"
    lines.append(header)
    lines.append("-" * len(header))
    for row in compute_running_excursions(bars_after_entry, trade["entry_price"]):
        lines.append(f"{str(row['ts']):<26} {row['open']:<10} {row['high']:<10} "
                     f"{row['low']:<10} {row['close']:<10} {row['mfe']:<10} {row['mae']:<10}")
    lines.append("")
    lines.append("Fibonacci / Target validation:")
    lines.append(f"  zone_lo (swing base, 0%)      = {trade.get('entry_zone_lo')}")
    lines.append(f"  mode                           = {trade['mode']}")
    lines.append(f"  Target1 (1.618 / fallback 2R)  = {trade['target1']}")
    lines.append(f"  Target2 (2.618 / fallback 3R)  = {trade['target2']}")
    lines.append(f"  Lot1 exit: @{trade['lot1_exit_ts']} price={trade['lot1_exit_price']} "
                 f"reason={trade['lot1_exit_reason']} pnl={trade['lot1_pnl']:.2f}")
    va, vb = trade["variant_a"], trade["variant_b"]
    lines.append(f"  Variant A (TSL trails 3-bar low after Lot1): "
                 f"@{va['lot2_exit_ts']} price={va['lot2_exit_price']} "
                 f"reason={va['lot2_exit_reason']} lot2_pnl={va['lot2_pnl']:.2f} "
                 f"total_pnl={va['total_pnl']:.2f}")
    lines.append(f"  Variant B (no TSL, runs to Target2/EOD): "
                 f"@{vb['lot2_exit_ts']} price={vb['lot2_exit_price']} "
                 f"reason={vb['lot2_exit_reason']} lot2_pnl={vb['lot2_pnl']:.2f} "
                 f"total_pnl={vb['total_pnl']:.2f}")
    return "\n".join(lines)


# Direct user fix (2026-10-03): block any new entry this late, since a
# post-entry swing has only ~30 min left before 15:15 EOD to develop into
# anything meaningful -- the exact failure mode a 15:05 entry exposed.
ENTRY_CUTOFF_TIME = time(14, 45)


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
    entry_price = entry_ts = entry_strike = entry_zone_lo = None
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
            if bar.ts.time() > ENTRY_CUTOFF_TIME:
                log(f"ENTRY BLOCKED (after {ENTRY_CUTOFF_TIME} cutoff) @{bar.ts} "
                    f"strike={current_strike} would-be price={bar.close} -- "
                    f"too little of the session left before 15:15 EOD for a "
                    f"meaningful post-entry swing")
            else:
                entry_price, entry_ts, entry_strike = bar.close, bar.ts, current_strike
                entry_zone_lo = zone.zone_lo
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

    result = manage_fib_trade(post_entry_bars, entry_price, entry_zone_lo, lot_qty_each)
    result["post_entry_bars"] = post_entry_bars
    result["entry_zone_lo"] = entry_zone_lo
    log(f"MODE={result['mode']} (zone_lo={entry_zone_lo}) "
        f"TARGET1={result['target1']} TARGET2={result['target2']}")
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


# ─────────────────────────────────────────────────────────────────────────
# Multi-strike OI confirmation filter (spec Section 5). Originally
# documented as un-backtestable (Upstox historical option candles assumed
# to carry oi=0, same as other strategies elsewhere in this codebase) --
# empirically DISPROVEN for this contract on 2026-10-03: real, varying OI
# IS present in fetch_upstox_range_1m's 1-min rows for NIFTY options.
# ─────────────────────────────────────────────────────────────────────────

def compute_oi_trend(now: int, baseline: int) -> str:
    if now > baseline:
        return "RISING"
    if now < baseline:
        return "FALLING"
    return "FLAT"


def sum_oi_band(oi_by_strike: dict[int, int], atm_strike: int, step: int,
                side: Side, depth: int = 5) -> int:
    """Sum of OI across `depth` strikes: ATM + (depth-1) strikes ITM for
    `side` (below ATM for CE, above ATM for PE) -- spec Section 5's
    "ATM + 4 strikes ITM" (depth=5 total strikes)."""
    if side == "CE":
        strikes = [atm_strike - i * step for i in range(depth)]
    else:
        strikes = [atm_strike + i * step for i in range(depth)]
    return sum(oi_by_strike.get(s, 0) for s in strikes)


def check_oi_filter(target_oi_now: int, target_oi_base: int,
                     opposing_oi_now: int, opposing_oi_base: int) -> dict:
    """Spec Section 5 pass condition: the traded side's own OI band must
    be FALLING vs session start, AND the opposing side's OI band over the
    SAME strikes must be RISING."""
    target_trend = compute_oi_trend(target_oi_now, target_oi_base)
    opposing_trend = compute_oi_trend(opposing_oi_now, opposing_oi_base)
    passed = target_trend == "FALLING" and opposing_trend == "RISING"
    return {
        "target_trend": target_trend, "opposing_trend": opposing_trend,
        "passed": passed,
        "detail": (f"target {target_trend} ({target_oi_base}->{target_oi_now}), "
                   f"opposing {opposing_trend} ({opposing_oi_base}->{opposing_oi_now})"),
    }


# ─────────────────────────────────────────────────────────────────────────
# Stage 3 (2026-10-03, direct user spec): institutional OI + Volume
# Profile confirmation matrix -- rollover guard, OI wall/noise filters,
# directional OI matrix, and Session Volume Profile structural
# acceptance. Volume Profile built on NIFTY FUTURES price (confirmed via
# real fetch: spot index 1-min candles carry volume=0, futures carries
# real non-zero volume AND OI -- the same contract "Future OI" in the
# directional matrix already refers to).
# ─────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class VolBar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


def compute_volume_profile(bars: list[VolBar], price_bin_size: float = 5.0,
                            value_area_pct: float = 0.70,
                            lvn_threshold_pct: float = 0.20) -> dict:
    """Session Volume Profile: POC (price bin with the most volume), VAH/
    VAL (the narrowest price band containing `value_area_pct` of total
    volume, built outward from POC bin-by-bin -- always includes POC),
    and LVNs (bins whose volume is below `lvn_threshold_pct` of the POC
    bin's volume AND are a local minimum vs both neighbors -- a real gap
    in traded volume, not just "somewhat thin")."""
    volume_by_bin: dict[float, int] = {}
    for b in bars:
        lo_bin = (b.low // price_bin_size) * price_bin_size
        hi_bin = (b.high // price_bin_size) * price_bin_size
        bin_count = max(1, int(round((hi_bin - lo_bin) / price_bin_size)) + 1)
        per_bin_vol = b.volume / bin_count
        bn = lo_bin
        while bn <= hi_bin + 1e-9:
            volume_by_bin[bn] = volume_by_bin.get(bn, 0) + per_bin_vol
            bn += price_bin_size

    if not volume_by_bin:
        return {"poc": None, "vah": None, "val": None, "lvns": []}

    sorted_bins = sorted(volume_by_bin.keys())
    poc_bin = max(sorted_bins, key=lambda bn: volume_by_bin[bn])
    poc_idx = sorted_bins.index(poc_bin)
    total_volume = sum(volume_by_bin.values())

    lo_idx = hi_idx = poc_idx
    captured = volume_by_bin[poc_bin]
    while captured < value_area_pct * total_volume and (lo_idx > 0 or hi_idx < len(sorted_bins) - 1):
        lo_vol = volume_by_bin[sorted_bins[lo_idx - 1]] if lo_idx > 0 else -1
        hi_vol = volume_by_bin[sorted_bins[hi_idx + 1]] if hi_idx < len(sorted_bins) - 1 else -1
        if hi_vol >= lo_vol:
            hi_idx += 1
            captured += hi_vol
        else:
            lo_idx -= 1
            captured += lo_vol

    poc_volume = volume_by_bin[poc_bin]
    lvns = []
    for i, bn in enumerate(sorted_bins):
        vol = volume_by_bin[bn]
        if vol >= lvn_threshold_pct * poc_volume:
            continue
        left = volume_by_bin[sorted_bins[i - 1]] if i > 0 else float("inf")
        right = volume_by_bin[sorted_bins[i + 1]] if i < len(sorted_bins) - 1 else float("inf")
        if vol <= left and vol <= right:
            lvns.append(bn + price_bin_size / 2)  # report the bin's midpoint

    return {
        "poc": poc_bin + price_bin_size / 2,
        "vah": sorted_bins[hi_idx] + price_bin_size,
        "val": sorted_bins[lo_idx],
        "lvns": lvns,
    }


def classify_rollover(curr_oi_now: int, curr_oi_base: int, next_oi_now: int,
                       next_oi_base: int, similarity_tol: float = 0.3) -> bool:
    """Spec guidelines 8-11: during expiry week, a sharp fall in the
    current-month contract's OI alongside a SIMILAR-MAGNITUDE rise in the
    next-month contract's OI is a rollover (positions migrating to the
    new contract), not a genuine directional signal -- block it."""
    curr_delta = curr_oi_now - curr_oi_base
    next_delta = next_oi_now - next_oi_base
    if curr_delta >= 0 or next_delta <= 0:
        return False  # not even a fall-then-rise shape
    magnitude_diff = abs(abs(curr_delta) - abs(next_delta))
    larger_magnitude = max(abs(curr_delta), abs(next_delta))
    return (magnitude_diff / larger_magnitude) <= similarity_tol if larger_magnitude else False


def is_strike_oi_significant(abs_oi_at_strike: int, peak_oi: int,
                              min_pct: float = 0.15) -> bool:
    """Spec guideline 4 (noise filter): a strike's absolute OI below
    `min_pct` of the session's peak absolute OI is too thin to treat its
    5m OI change as meaningful."""
    if peak_oi <= 0:
        return False
    return (abs_oi_at_strike / peak_oi) >= min_pct


def check_hard_wall(price: float, wall_strike: float, distance_pts: float,
                     oi_change_pct: float, override_pct: float = 0.30) -> bool:
    """Spec guidelines 5-7: returns True (BLOCKED) if price sits within
    `distance_pts` of a peak-OI wall and the OI move at that wall is NOT
    big enough to count as a momentum override (`oi_change_pct` must
    clear `override_pct`, i.e. >30% intraday change at that strike, to
    let a breakout through this close to the wall)."""
    if abs(price - wall_strike) > distance_pts:
        return False  # far from any wall -- never blocked by this rule
    return oi_change_pct < override_pct


def check_directional_matrix(fut_trend: str, put_trend: str, call_trend: str,
                              side: Side) -> bool:
    """Spec guideline set (strict directional OI confirmation matrix).
    PE/downside: (Fut RISING, Put FALLING, Call RISING) OR
                 (Fut FALLING, Put FALLING, Call RISING).
    CE/upside:   (Fut RISING, Put RISING, Call FALLING) OR
                 (Fut FALLING, Put RISING, Call FALLING)."""
    if side == "PE":
        return ((fut_trend == "RISING" and put_trend == "FALLING" and call_trend == "RISING") or
                (fut_trend == "FALLING" and put_trend == "FALLING" and call_trend == "RISING"))
    return ((fut_trend == "RISING" and put_trend == "RISING" and call_trend == "FALLING") or
            (fut_trend == "FALLING" and put_trend == "RISING" and call_trend == "FALLING"))


SVP_WARMUP_TIME = time(9, 45)  # calibration, 2026-10-03: 09:40/09:50 entries had too few bars for a real Value Area


def svp_ready(ts: datetime) -> bool:
    return ts.time() >= SVP_WARMUP_TIME


def check_price_acceptance(close: float, val: float, vah: float,
                            lvns: list[float], bar_low: float, bar_high: float,
                            direction: str, lvn_tolerance: float = 1.0) -> bool:
    """Spec guideline 5 (structural price acceptance): a breakout only
    counts if price closed beyond VAH (bullish) / VAL (bearish) AND the
    triggering bar's own range overlapped at least one LVN zone (volume
    genuinely traded/"accepted" through a prior low-volume gap, not a
    single-tick spike past it). Disclosed interpretation: "touched" means
    the bar's [low, high] range came within `lvn_tolerance` of any
    reported LVN price -- the spec does not give an exact mechanical
    definition of "shows volume acceptance", so this is the most literal
    reading of "breaks VAL/VAH and shows acceptance in the LVN"."""
    if direction == "bullish":
        if close <= vah:
            return False
    else:
        if close >= val:
            return False
    return any(bar_low - lvn_tolerance <= lvn <= bar_high + lvn_tolerance for lvn in lvns)


def _nearest_row_at_or_before(rows: list[dict], ts: datetime) -> dict:
    """rows ascending by ts (fetch_upstox_range_1m's own contract) -- the
    last row at or before `ts`, the closest real reading without looking
    into the future."""
    match = rows[0]
    for r in rows:
        if datetime.fromisoformat(r["ts"]) <= ts:
            match = r
        else:
            break
    return match


# ─────────────────────────────────────────────────────────────────────────
# Option-buyer momentum gate (2026-10-03, direct user philosophical pivot):
# walls/noise-floor/LVN-maturation are option-SELLER filters (built to
# catch slow chop); a buyer needs velocity. This gate drops the wall and
# noise-floor checks entirely and replaces price_acceptance's LVN-touch
# wait with a fast breakout+volume-or-EMA-slope trigger.
# ─────────────────────────────────────────────────────────────────────────

def compute_ema(values: list[float], period: int = 9) -> list[float]:
    """Standard EMA, seeded with the first value (no separate SMA
    warm-up -- fine for a short backtest window, same convention most
    simple EMA implementations use)."""
    if not values:
        return []
    k = 2 / (period + 1)
    ema = [values[0]]
    for v in values[1:]:
        ema.append(ema[-1] + k * (v - ema[-1]))
    return ema


def check_momentum_acceptance(close: float, val: float, vah: float, direction: str,
                               bar_volume: float, avg_volume: float,
                               ema_prev: float, ema_curr: float,
                               volume_surge_mult: float = 1.5) -> bool:
    """Fast momentum trigger replacing the slow LVN-touch wait: price
    must break VAH (bullish) / VAL (bearish), AND either a volume surge
    (bar_volume >= volume_surge_mult * avg_volume) or a 9-EMA slope in
    the trade's own direction confirms it -- not both required."""
    if direction == "bullish":
        if close <= vah:
            return False
        ema_confirms = ema_curr > ema_prev
    else:
        if close >= val:
            return False
        ema_confirms = ema_curr < ema_prev
    volume_surge = avg_volume > 0 and bar_volume >= volume_surge_mult * avg_volume
    return volume_surge or ema_confirms


def check_buyer_gate(fut_trend: str, put_trend: str, call_trend: str, side: Side,
                      close: float, val: float, vah: float, bar_volume: float,
                      avg_volume: float, ema_prev: float, ema_curr: float) -> bool:
    """Clean option-buyer activation gate: (5m OI directional matrix
    match) AND (momentum breakout). No wall, no noise floor -- a buyer
    wants to catch a wall breaking, not be kept out by one."""
    if not check_directional_matrix(fut_trend, put_trend, call_trend, side):
        return False
    direction = "bullish" if side == "CE" else "bearish"
    return check_momentum_acceptance(close, val, vah, direction, bar_volume,
                                      avg_volume, ema_prev, ema_curr)


# ─────────────────────────────────────────────────────────────────────────
# Velocity gate (2026-10-03, "use your own expert judgment" pivot): drops
# Volume Profile/VAH-VAL entirely from entry gating -- VAH/VAL is still a
# structural RANGE concept (mean-reversion/seller-flavored), not something
# an option buyer chasing a genuine directional move needs to wait for.
# Real quant option-buying principles applied instead:
#   - OI directional confirmation (kept -- this is positioning data, not a
#     seller-specific construct; smart money flow matters to a buyer too).
#   - VELOCITY: underlying must have actually moved a minimum % over a
#     short lookback (rate of change), in the trade's own direction --
#     the single most standard momentum/breakout-trader filter: don't buy
#     options on a underlying that hasn't actually started moving yet.
#   - Volume surge OR EMA slope as a second, independent momentum
#     confirmation (kept from check_momentum_acceptance's own logic).
# Known, disclosed gap: IV percentile/rank (the single most standard real
# option-buyer filter against paying a premium about to get IV-crushed)
# is NOT implemented -- this backtest has no options Greeks/IV data
# fetched anywhere in this script; adding it needs a new data source.
# ─────────────────────────────────────────────────────────────────────────

def compute_roc(closes: list[float], lookback: int = 3) -> float:
    """Rate of change of the LAST close vs the close `lookback` bars
    earlier. closes[-1] is "now"."""
    if len(closes) <= lookback or closes[-1 - lookback] == 0:
        return 0.0
    base = closes[-1 - lookback]
    return (closes[-1] - base) / base


def check_velocity_gate(fut_trend: str, put_trend: str, call_trend: str,
                         side: Side, roc_pct: float, roc_threshold: float,
                         bar_volume: float, avg_volume: float,
                         ema_prev: float, ema_curr: float) -> bool:
    """OI directional confirmation AND the underlying has genuinely
    already moved `roc_threshold` in the trade's own direction AND a
    second momentum confirmation (volume surge or EMA slope) -- no
    Volume Profile / VAH-VAL dependency at all."""
    if not check_directional_matrix(fut_trend, put_trend, call_trend, side):
        return False
    velocity_ok = roc_pct >= roc_threshold if side == "CE" else roc_pct <= -roc_threshold
    if not velocity_ok:
        return False
    volume_surge = avg_volume > 0 and bar_volume >= 1.5 * avg_volume
    ema_confirms = (ema_curr > ema_prev) if side == "CE" else (ema_curr < ema_prev)
    return volume_surge or ema_confirms


def diagnose_trap_funnel(bars: list[Bar]) -> str:
    """One trading day, one side: replays the REAL trap detector (same
    primitives run_side_backtest uses) and reports the FINAL stage
    reached by EOD, regardless of whether a trade fired -- the funnel
    breakdown diagnostic: "ENTERED" or whichever TrapZoneState the day's
    bars never got past (BREAKDOWN_WATCH = no C2 ever formed, TRAP_WATCH
    = C2 formed but trap never confirmed, ARMED_WAIT_REENTRY = trap
    confirmed but price never came back into the zone)."""
    zone = _fresh_zone()
    for bar in bars:
        if zone.state == TrapZoneState.ARMED_WAIT_REENTRY and check_zone_reentry(zone, bar.close):
            return "ENTERED"
        zone = on_bar_close(zone, bar)
    return zone.state.value


# ─────────────────────────────────────────────────────────────────────────
# LTP-Calculator-style dynamic S/R shift + writer panic gate (2026-10-03,
# direct user spec). Resistance/support treated as MOVING (the strike
# currently carrying peak Call/Put OI), not static walls -- and the real
# buyer fuel is writer unwinding (LTP surging while OI drops), not a
# strike's absolute OI level.
# ─────────────────────────────────────────────────────────────────────────

def peak_strike_shift(early: int, late: int, step: int) -> str:
    """Direction the peak-OI strike (resistance=call peak, support=put
    peak) has moved between two snapshots."""
    if late < early:
        return "DOWN"
    if late > early:
        return "UP"
    return "STABLE"


def detect_writer_panic(ltp_now: float, ltp_base: float, oi_now: int,
                         oi_base: int, ltp_surge_pct: float = 0.20,
                         oi_drop_pct: float = 0.10) -> bool:
    """Writers unwinding/short-covering: LTP has surged >= ltp_surge_pct
    AND OI has dropped >= oi_drop_pct over the same window, at the SAME
    strike -- an LTP rise with OI also rising is just fresh buying/selling
    interest, not panic."""
    if ltp_base <= 0 or oi_base <= 0:
        return False
    ltp_change = (ltp_now - ltp_base) / ltp_base
    oi_change = (oi_now - oi_base) / oi_base
    return ltp_change >= ltp_surge_pct and oi_change <= -oi_drop_pct


def check_ltp_calc_gate(price_tests_level: bool, sr_shift_favorable: bool,
                         writer_panic: bool) -> bool:
    """Unified option-buyer execution gate: price must test a key level
    AND the S/R peak must have shifted favorably (resistance caving for a
    CE buyer, support caving for a PE buyer) AND writer panic must be
    detected on the breakout side -- all three, not any one alone."""
    return price_tests_level and sr_shift_favorable and writer_panic


# ─────────────────────────────────────────────────────────────────────────
# Cross-side 1-min trap switch (2026-10-04, direct user spec). Pure
# price-action only -- no OI, no Volume Profile. When a side's 5-min trap
# zone FAILS (armed, then a 5-min bar closes below zone_lo instead of
# re-entering), jump to the OPPOSITE side's 1-minute chart, find its
# latest/freshest 1-min trap cycle, and fire the moment price re-enters
# that 1-min zone -- immediately, no further confirmation.
# ─────────────────────────────────────────────────────────────────────────

def detect_5m_zone_invalidation(bars_5m: list[Bar]) -> Optional[datetime]:
    """Replays the real trap detector on 5-min bars. Returns the
    timestamp of the bar whose CLOSE breaks below zone_lo while armed
    (zone failure) -- or None if the zone instead re-enters normally (or
    never arms at all)."""
    zone = _fresh_zone()
    for bar in bars_5m:
        if zone.state == TrapZoneState.ARMED_WAIT_REENTRY:
            if check_zone_reentry(zone, bar.close):
                return None
            if bar.close < zone.zone_lo:
                return bar.ts
        zone = on_bar_close(zone, bar)
    return None


def enter_on_1m_trap(bars_1m: list[Bar], lot_qty: int) -> Optional[dict]:
    """Fresh trap cycle on 1-min bars (same real detector primitives, 1m
    granularity) -- fires IMMEDIATELY the moment price re-enters the
    first 1-min zone that confirms, no further checks."""
    zone = _fresh_zone()
    for bar in bars_1m:
        if zone.state == TrapZoneState.ARMED_WAIT_REENTRY and check_zone_reentry(zone, bar.close):
            return {"entry_price": bar.close, "entry_ts": bar.ts,
                     "zone_lo": zone.zone_lo, "zone_hi": zone.zone_hi}
        zone = on_bar_close(zone, bar)
    return None


def compute_performance_metrics(pnls: list[float]) -> dict:
    """Profit Factor, Expectancy, and Maximum Drawdown (peak-to-trough on
    CUMULATIVE P&L, in trade sequence order) for a list of per-trade net
    P&L figures -- direct user request before any live-deployment
    decision. `pnls` should already be in entry-time order."""
    gross_win = sum(p for p in pnls if p > 0)
    gross_loss = sum(-p for p in pnls if p < 0)
    win_count = sum(1 for p in pnls if p > 0)
    loss_count = sum(1 for p in pnls if p < 0)
    profit_factor = (gross_win / gross_loss) if gross_loss > 0 else (
        float("inf") if gross_win > 0 else 0.0)
    expectancy = (sum(pnls) / len(pnls)) if pnls else 0.0

    cum = peak = max_dd = 0.0
    for p in pnls:
        cum += p
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)

    return {
        "gross_win": gross_win, "gross_loss": gross_loss,
        "profit_factor": profit_factor, "expectancy": expectancy,
        "max_drawdown": max_dd, "total_pnl": sum(pnls),
        "win_count": win_count, "loss_count": loss_count,
        "trade_count": len(pnls),
    }


def apply_execution_costs(entry_price: float, lot1_exit_price: float,
                           lot2_exit_price: float, lot_qty_each: int,
                           cost_per_lot_leg: float = 30.0,
                           slippage_pct: float = 0.0005) -> dict:
    """Net-of-cost P&L for one 2-lot trade, direct user request before
    any live-deployment decision: slippage works AGAINST the trade on
    every fill (pay more to buy, receive less to sell); a flat
    brokerage/STT/exchange fee of `cost_per_lot_leg` is charged per lot
    per leg -- each lot has its own entry leg and its own exit leg (4
    lot-legs total across both lots, even though the physical entry order
    covers both lots at once -- a simplifying, disclosed assumption)."""
    eff_entry = entry_price * (1 + slippage_pct)
    eff_lot1_exit = lot1_exit_price * (1 - slippage_pct)
    eff_lot2_exit = lot2_exit_price * (1 - slippage_pct)

    lot1_net = (eff_lot1_exit - eff_entry) * lot_qty_each - 2 * cost_per_lot_leg
    lot2_net = (eff_lot2_exit - eff_entry) * lot_qty_each - 2 * cost_per_lot_leg

    return {
        "lot1_net_pnl": lot1_net, "lot2_net_pnl": lot2_net,
        "total_net_pnl": lot1_net + lot2_net,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--strike-step", type=int, default=50)
    parser.add_argument("--lot-qty", type=int, default=75)
    parser.add_argument("--cost-per-lot-leg", type=float, default=30.0,
                         dest="cost_per_lot_leg")
    parser.add_argument("--slippage-pct", type=float, default=0.0005,
                         dest="slippage_pct")
    args = parser.parse_args()

    from config.global_config import IST
    from data_layer.historical_candles import fetch_upstox_daily, fetch_upstox_range_1m
    from data_layer.instrument_registry import REGISTRY
    from strategies.bear_trap_oi.strike_selector import round_to_strike_step

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

    funnel_counts: Counter = Counter()
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

        funnel_counts["CE:" + diagnose_trap_funnel(ce_bars)] += 1
        funnel_counts["PE:" + diagnose_trap_funnel(pe_bars)] += 1

    print("=== Section 1 Funnel Diagnostic (where signals die before entry) ===")
    for stage in ("BREAKDOWN_WATCH", "TRAP_WATCH", "ARMED_WAIT_REENTRY", "ENTERED"):
        ce_n, pe_n = funnel_counts[f"CE:{stage}"], funnel_counts[f"PE:{stage}"]
        print(f"  {stage:<20} CE={ce_n:<3} PE={pe_n:<3} total={ce_n + pe_n}")
    total_days = len(daily_strikes)
    print(f"  (out of {total_days} trading day(s) x 2 sides = {total_days * 2} "
          f"side-days total)\n")

    _print_report("CE", [t for t in all_trades if t.side == "CE"])
    _print_report("PE", [t for t in all_trades if t.side == "PE"])
    print(f"\n=== Combined: {len(all_trades)} trade(s), "
          f"Total P&L: {sum(t.pnl for t in all_trades):.2f} ===")
    _print_mfe_mae_table(all_trades)

    print("\n" + "=" * 78)
    print("SECTION 1b: Section 1 signals re-evaluated with the real "
          "multi-strike OI confirmation filter (spec Section 5) -- "
          "2026-10-03 correction: Upstox DOES carry real OI on historical "
          "option candles for this contract, disproving the earlier "
          "un-backtestable assumption.")
    print("=" * 78)

    oi_depth = 5
    oi_surviving: list[BacktestTrade] = []
    oi_contexts: list[dict] = []  # reusable OI/ATM context per trade, for Section 1c
    for trade in all_trades:
        day = trade.entry_ts.date()
        spot_rows = await fetch_upstox_range_1m(index_key, access_token, day, day)
        spot_bars_oi = _truncate_to_eod(_resample_1m_to_5m(_candle_dicts_to_bars(spot_rows)))
        spot_by_ts = {b.ts: b for b in spot_bars_oi}
        entry_spot_bar = spot_by_ts.get(trade.entry_ts)
        if entry_spot_bar is None or not spot_bars_oi:
            print(f"[{trade.entry_ts}] {trade.side} {trade.strike}: SKIP OI check "
                  f"-- no spot bar at entry timestamp")
            continue
        atm = round_to_strike_step(entry_spot_bar.close, args.strike_step)
        band_strikes = ([atm - i * args.strike_step for i in range(oi_depth)]
                         if trade.side == "CE" else
                         [atm + i * args.strike_step for i in range(oi_depth)])

        expiry = REGISTRY.get_active_expiry_strict("NIFTY", from_date=day)
        oi_now = {"CE": {}, "PE": {}}
        oi_base = {"CE": {}, "PE": {}}
        ltp_now = {"CE": {}, "PE": {}}
        ltp_base = {"CE": {}, "PE": {}}
        for strike in band_strikes:
            for opt in ("CE", "PE"):
                key = REGISTRY.get_upstox_key("NIFTY", expiry, strike, opt)
                if not key:
                    continue
                rows = await fetch_upstox_range_1m(key, access_token, day, day)
                await asyncio.sleep(0.35)  # pacing, same budget as the roll-strike fetches
                if not rows:
                    continue
                oi_base[opt][strike] = rows[0].get("oi", 0)
                ltp_base[opt][strike] = rows[0].get("close", 0.0)
                nearest = _nearest_row_at_or_before(rows, trade.entry_ts)
                oi_now[opt][strike] = nearest.get("oi", 0)
                ltp_now[opt][strike] = nearest.get("close", 0.0)

        target_opt = trade.side
        opposing_opt = "PE" if trade.side == "CE" else "CE"
        target_now = sum_oi_band(oi_now[target_opt], atm, args.strike_step, trade.side, oi_depth)
        target_base = sum_oi_band(oi_base[target_opt], atm, args.strike_step, trade.side, oi_depth)
        opposing_now = sum_oi_band(oi_now[opposing_opt], atm, args.strike_step, trade.side, oi_depth)
        opposing_base = sum_oi_band(oi_base[opposing_opt], atm, args.strike_step, trade.side, oi_depth)
        result = check_oi_filter(target_now, target_base, opposing_now, opposing_base)

        verdict = "PASS" if result["passed"] else "REJECT"
        print(f"[{trade.entry_ts}] {trade.side} {trade.strike} (atm={atm}): "
              f"{verdict} -- {result['detail']} -- pnl={trade.pnl:.2f}")
        if result["passed"]:
            oi_surviving.append(trade)

        oi_contexts.append({
            "trade": trade, "atm": atm, "band_strikes": band_strikes,
            "oi_now": oi_now, "oi_base": oi_base, "ltp_now": ltp_now,
            "ltp_base": ltp_base, "expiry": expiry, "day": day,
            "oi_filter_result": result,
        })

    oi_metrics = compute_performance_metrics([t.pnl for t in oi_surviving])
    print(f"\n=== OI-Gated Section 1: {len(oi_surviving)}/{len(all_trades)} "
          f"trade(s) survived the OI filter ===")
    print(f"  Win/Loss: {oi_metrics.get('win_count', 0)}W/{oi_metrics.get('loss_count', 0)}L")
    print(f"  Total P&L: {oi_metrics.get('total_pnl', 0.0):.2f} "
          f"(unfiltered Section 1 was {sum(t.pnl for t in all_trades):.2f})")

    print("\n" + "=" * 78)
    print("SECTION 1c: Stage 3 institutional OI + Volume Profile confirmation "
          "matrix on top of Section 1b's survivors. KNOWN GAP: the rollover "
          "guard (spec guidelines 8-11) is NOT evaluated -- InstrumentRegistry "
          "only resolves the NEAR-month futures contract, with no next-month "
          "key resolution available, so a real current-vs-next-month OI "
          "comparison cannot be built without new registry work. Every other "
          "filter (noise, hard wall, directional matrix, price acceptance) "
          "runs for real.")
    print("=" * 78)

    futures_key = REGISTRY.get_futures_upstox("NIFTY")
    matrix_surviving: list[BacktestTrade] = []
    buyer_gate_results: list[tuple[BacktestTrade, bool]] = []
    velocity_gate_results: list[tuple[BacktestTrade, bool]] = []
    ltp_calc_results: list[tuple[BacktestTrade, bool]] = []
    for ctx in oi_contexts:
        if not ctx["oi_filter_result"]["passed"]:
            continue  # Section 1c only refines Section 1b's own survivors

        trade = ctx["trade"]
        day, atm, oi_now, oi_base = ctx["day"], ctx["atm"], ctx["oi_now"], ctx["oi_base"]

        # Noise + hard-wall: peak absolute OI across the fetched band
        # (rolling-since-09:15 proxy -- the max of whatever this trade's
        # entry-instant band fetch already pulled).
        all_abs_oi_now = list(oi_now["CE"].values()) + list(oi_now["PE"].values())
        peak_oi = max(all_abs_oi_now) if all_abs_oi_now else 0
        wall_strike = max({**oi_now["CE"], **oi_now["PE"]},
                           key=lambda s: oi_now["CE"].get(s, 0) + oi_now["PE"].get(s, 0),
                           default=atm)
        own_strike_abs_oi = oi_now[trade.side].get(trade.strike, 0)
        # Sensitivity pass, 2026-10-03: noise floor bypassed (0%) to test
        # whether CE 23100 was correctly rejected on OI concentration alone.
        significant = is_strike_oi_significant(own_strike_abs_oi, peak_oi, min_pct=0.0)

        wall_oi_base = oi_base["CE"].get(wall_strike, 0) + oi_base["PE"].get(wall_strike, 0)
        wall_oi_now = oi_now["CE"].get(wall_strike, 0) + oi_now["PE"].get(wall_strike, 0)
        wall_change_pct = (abs(wall_oi_now - wall_oi_base) / wall_oi_base) if wall_oi_base else 0.0
        # Sensitivity pass, 2026-10-03: wall proximity relaxed 50->25pts to
        # test whether PE 23550 clears a tighter threshold.
        blocked_by_wall = check_hard_wall(float(trade.strike), float(wall_strike),
                                           distance_pts=25, oi_change_pct=wall_change_pct,
                                           override_pct=0.30)

        # Directional matrix: needs a Futures OI trend too.
        fut_rows = await fetch_upstox_range_1m(futures_key, access_token, day, day) if futures_key else []
        await asyncio.sleep(0.35)
        fut_trend = "FLAT"
        if fut_rows:
            fut_base = fut_rows[0].get("oi", 0)
            fut_now = _nearest_row_at_or_before(fut_rows, trade.entry_ts).get("oi", 0)
            fut_trend = compute_oi_trend(fut_now, fut_base)

        put_now = sum_oi_band(oi_now["PE"], atm, args.strike_step, "PE", oi_depth)
        put_base = sum_oi_band(oi_base["PE"], atm, args.strike_step, "PE", oi_depth)
        call_now = sum_oi_band(oi_now["CE"], atm, args.strike_step, "CE", oi_depth)
        call_base = sum_oi_band(oi_base["CE"], atm, args.strike_step, "CE", oi_depth)
        put_trend = compute_oi_trend(put_now, put_base)
        call_trend = compute_oi_trend(call_now, call_base)
        matrix_ok = check_directional_matrix(fut_trend, put_trend, call_trend, trade.side)

        # Structural price acceptance: Volume Profile on NIFTY futures
        # price, built from the day's bars UP TO the entry bar.
        fut_bars_5m = _truncate_to_eod(_resample_1m_to_5m(_candle_dicts_to_bars(fut_rows))) if fut_rows else []
        fut_bars_vol = [VolBar(ts=b.ts, open=b.open, high=b.high, low=b.low,
                                close=b.close, volume=0) for b in fut_bars_5m]
        # volume comes from the raw 1-min rows, not the resampled Bar (which
        # drops it) -- approximate per-5m-bucket volume by summing the
        # underlying 1-min rows in that window.
        vol_by_5m_ts = {}
        for r in fut_rows:
            ts1 = datetime.fromisoformat(r["ts"])
            bucket = ts1.replace(minute=(ts1.minute // 5) * 5, second=0, microsecond=0)
            vol_by_5m_ts[bucket] = vol_by_5m_ts.get(bucket, 0) + r.get("volume", 0)
        fut_bars_vol = [VolBar(ts=b.ts, open=b.open, high=b.high, low=b.low,
                                close=b.close, volume=vol_by_5m_ts.get(b.ts, 0))
                        for b in fut_bars_5m]
        entry_idx_fut = next((i for i, b in enumerate(fut_bars_vol) if b.ts == trade.entry_ts), None)
        if not svp_ready(trade.entry_ts):
            # Calibration, 2026-10-03: before 09:45 the SVP hasn't had
            # enough bars to form a real Value Area -- don't penalize an
            # early entry for a profile that was never going to be ready.
            price_accepted = True
        else:
            price_accepted = False
            if entry_idx_fut is not None and entry_idx_fut > 0:
                profile = compute_volume_profile(fut_bars_vol[:entry_idx_fut])
                entry_bar = fut_bars_vol[entry_idx_fut]
                direction = "bullish" if trade.side == "CE" else "bearish"
                if profile["poc"] is not None:
                    price_accepted = check_price_acceptance(
                        close=entry_bar.close, val=profile["val"], vah=profile["vah"],
                        lvns=profile["lvns"], bar_low=entry_bar.low, bar_high=entry_bar.high,
                        direction=direction,
                    )

        passed_matrix = significant and not blocked_by_wall and matrix_ok and price_accepted
        verdict = "PASS" if passed_matrix else "REJECT"
        print(f"[{trade.entry_ts}] {trade.side} {trade.strike}: {verdict} -- "
              f"significant_oi={significant} blocked_by_wall={blocked_by_wall} "
              f"(wall={wall_strike}) fut={fut_trend} put={put_trend} call={call_trend} "
              f"matrix_ok={matrix_ok} price_accepted={price_accepted} -- pnl={trade.pnl:.2f}")
        if passed_matrix:
            matrix_surviving.append(trade)

        # --- Section 1d: option-buyer momentum gate (no wall/noise/LVN
        # maturation wait -- see check_buyer_gate's own docstring). Reuses
        # this iteration's already-fetched futures bars/OI trends. ---
        gate_open = False
        if entry_idx_fut is not None and entry_idx_fut > 0:
            pre_entry = fut_bars_vol[:entry_idx_fut]
            buyer_profile = compute_volume_profile(pre_entry)
            entry_bar = fut_bars_vol[entry_idx_fut]
            avg_volume = sum(b.volume for b in pre_entry) / len(pre_entry)
            closes = [b.close for b in pre_entry] + [entry_bar.close]
            ema = compute_ema(closes, period=9)
            if buyer_profile["poc"] is not None:
                gate_open = check_buyer_gate(
                    fut_trend, put_trend, call_trend, trade.side,
                    close=entry_bar.close, val=buyer_profile["val"],
                    vah=buyer_profile["vah"], bar_volume=entry_bar.volume,
                    avg_volume=avg_volume, ema_prev=ema[-2], ema_curr=ema[-1],
                )
        buyer_gate_results.append((trade, gate_open))
        print(f"[{trade.entry_ts}] {trade.side} {trade.strike}: "
              f"BUYER GATE {'OPEN' if gate_open else 'CLOSED'} -- pnl={trade.pnl:.2f}")

        # --- Section 1e: velocity gate (no Volume Profile at all). Reuses
        # the same pre_entry/avg_volume/ema/trends this iteration already
        # built for Section 1d above. ---
        velocity_open = False
        if entry_idx_fut is not None and entry_idx_fut > 0:
            roc = compute_roc(closes, lookback=min(3, len(pre_entry)))
            velocity_open = check_velocity_gate(
                fut_trend, put_trend, call_trend, trade.side, roc_pct=roc,
                roc_threshold=0.003, bar_volume=entry_bar.volume,
                avg_volume=avg_volume, ema_prev=ema[-2], ema_curr=ema[-1],
            )
        velocity_gate_results.append((trade, velocity_open))
        print(f"[{trade.entry_ts}] {trade.side} {trade.strike}: "
              f"VELOCITY GATE {'OPEN' if velocity_open else 'CLOSED'} -- pnl={trade.pnl:.2f}")

        # --- Section 1f: LTP-Calculator style dynamic S/R shift + writer
        # panic gate. Reuses this iteration's already-fetched OI band
        # (now also carrying LTP) -- no new fetches. ---
        ltp_now_ctx, ltp_base_ctx = ctx["ltp_now"], ctx["ltp_base"]
        resistance_now = max(oi_now["CE"], key=oi_now["CE"].get, default=atm)
        resistance_base = max(oi_base["CE"], key=oi_base["CE"].get, default=atm)
        support_now = max(oi_now["PE"], key=oi_now["PE"].get, default=atm)
        support_base = max(oi_base["PE"], key=oi_base["PE"].get, default=atm)

        if trade.side == "CE":
            shift = peak_strike_shift(resistance_base, resistance_now, args.strike_step)
            sr_favorable = shift == "DOWN"  # resistance caving -- bullish fuel
            panic_strike, panic_opt = resistance_now, "CE"
        else:
            shift = peak_strike_shift(support_base, support_now, args.strike_step)
            sr_favorable = shift == "DOWN"  # support caving -- bearish fuel
            panic_strike, panic_opt = support_now, "PE"

        writer_panic = detect_writer_panic(
            ltp_now=ltp_now_ctx[panic_opt].get(panic_strike, 0.0),
            ltp_base=ltp_base_ctx[panic_opt].get(panic_strike, 0.0),
            oi_now=oi_now[panic_opt].get(panic_strike, 0),
            oi_base=oi_base[panic_opt].get(panic_strike, 0),
        )
        price_tests_level = price_accepted  # reuse Section 1c's own breakout-level test
        ltp_calc_open = check_ltp_calc_gate(price_tests_level, sr_favorable, writer_panic)
        ltp_calc_results.append((trade, ltp_calc_open))
        print(f"[{trade.entry_ts}] {trade.side} {trade.strike}: "
              f"LTP-CALC GATE {'OPEN' if ltp_calc_open else 'CLOSED'} -- "
              f"shift={shift} sr_favorable={sr_favorable} writer_panic={writer_panic} "
              f"(panic_strike={panic_strike}) -- pnl={trade.pnl:.2f}")

    matrix_metrics = compute_performance_metrics([t.pnl for t in matrix_surviving])
    print(f"\n=== Institutional Matrix: {len(matrix_surviving)}/{len(oi_surviving)} "
          f"of Section 1b's survivors also pass Section 1c ===")
    print(f"  Win/Loss: {matrix_metrics.get('win_count', 0)}W/{matrix_metrics.get('loss_count', 0)}L")
    print(f"  Total P&L: {matrix_metrics.get('total_pnl', 0.0):.2f}")

    buyer_surviving = [t for t, open_ in buyer_gate_results if open_]
    buyer_metrics = compute_performance_metrics([t.pnl for t in buyer_surviving])
    print(f"\n=== SECTION 1d (Option-Buyer Momentum Gate): "
          f"{len(buyer_surviving)}/{len(oi_surviving)} of Section 1b's "
          f"survivors clear the fast gate (no wall/noise/LVN-maturation) ===")
    print(f"  Win/Loss: {buyer_metrics.get('win_count', 0)}W/{buyer_metrics.get('loss_count', 0)}L")
    print(f"  Total P&L: {buyer_metrics.get('total_pnl', 0.0):.2f}")

    velocity_surviving = [t for t, open_ in velocity_gate_results if open_]
    velocity_metrics = compute_performance_metrics([t.pnl for t in velocity_surviving])
    print(f"\n=== SECTION 1e (Velocity Gate -- no Volume Profile at all): "
          f"{len(velocity_surviving)}/{len(oi_surviving)} of Section 1b's "
          f"survivors clear OI-matrix + underlying ROC velocity + "
          f"volume/EMA confirmation ===")
    print(f"  Win/Loss: {velocity_metrics.get('win_count', 0)}W/{velocity_metrics.get('loss_count', 0)}L")
    print(f"  Total P&L: {velocity_metrics.get('total_pnl', 0.0):.2f}")

    ltp_calc_surviving = [t for t, open_ in ltp_calc_results if open_]
    ltp_calc_metrics = compute_performance_metrics([t.pnl for t in ltp_calc_surviving])
    print(f"\n=== SECTION 1f (LTP-Calculator S/R-Shift + Writer-Panic Gate): "
          f"{len(ltp_calc_surviving)}/{len(oi_surviving)} of Section 1b's "
          f"survivors clear price-tests-level + S/R-shift-favorable + "
          f"writer-panic ===")
    print(f"  Win/Loss: {ltp_calc_metrics.get('win_count', 0)}W/{ltp_calc_metrics.get('loss_count', 0)}L")
    print(f"  Total P&L: {ltp_calc_metrics.get('total_pnl', 0.0):.2f}")

    print("\n" + "=" * 78)
    print("SECTION 2 (EXPERIMENTAL): simultaneous CE+PE, dynamic 100-pt ITM "
          "strike rolling, Fib 1.618/2.618 dual-lot management (2 lots in, "
          "1 lot @1.618, remaining lot: Variant A = TSL trails 3-bar low, "
          "Variant B = no TSL, runs to 2.618/EOD)")
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
               f"{'Mode':<17} {'Target1':<9} {'Target2':<9} "
               f"{'VarA Exit':<10} {'VarA Reason':<20} {'VarA PnL':<10} "
               f"{'VarB Exit':<10} {'VarB Reason':<20} {'VarB PnL':<10}")
    print(header)
    print("-" * len(header))
    total_a = total_b = 0.0
    for t in sorted(all_dynamic_trades, key=lambda t: t["entry_ts"]):
        va, vb = t["variant_a"], t["variant_b"]
        total_a += va["total_pnl"]
        total_b += vb["total_pnl"]
        print(f"{t['side']:<4} {t['entry_strike']:<11} {t['entry_price']:<8} "
              f"{str(t['entry_ts']):<22} {str(t['mode']):<17} "
              f"{str(t['target1']):<9} {str(t['target2']):<9} "
              f"{va['lot2_exit_price']:<10} {va['lot2_exit_reason']:<20} "
              f"{va['total_pnl']:<10.2f} {vb['lot2_exit_price']:<10} "
              f"{vb['lot2_exit_reason']:<20} {vb['total_pnl']:<10.2f}")
    print(f"\n=== Dynamic/Fib Combined: {len(all_dynamic_trades)} trade(s) -- "
          f"Variant A total P&L: {total_a:.2f}  |  "
          f"Variant B total P&L: {total_b:.2f} ===")

    # --- Risk metrics + net-of-cost simulation for Variant A (direct user
    # request: validation before any live-deployment decision). ---
    sorted_trades = sorted(all_dynamic_trades, key=lambda t: t["entry_ts"])
    gross_a_pnls = [t["variant_a"]["total_pnl"] for t in sorted_trades]
    gross_metrics = compute_performance_metrics(gross_a_pnls)

    net_a_pnls = []
    for t in sorted_trades:
        costed = apply_execution_costs(
            entry_price=t["entry_price"], lot1_exit_price=t["lot1_exit_price"],
            lot2_exit_price=t["variant_a"]["lot2_exit_price"],
            lot_qty_each=args.lot_qty, cost_per_lot_leg=args.cost_per_lot_leg,
            slippage_pct=args.slippage_pct,
        )
        net_a_pnls.append(costed["total_net_pnl"])
    net_metrics = compute_performance_metrics(net_a_pnls)

    print("\n=== Variant A Risk Metrics (GROSS, before costs) ===")
    print(f"  Trades: {gross_metrics['trade_count']}  "
          f"Win/Loss: {gross_metrics['win_count']}W/{gross_metrics['loss_count']}L")
    print(f"  Total P&L: {gross_metrics['total_pnl']:.2f}")
    print(f"  Profit Factor: {gross_metrics['profit_factor']:.3f}")
    print(f"  Expectancy/Trade: {gross_metrics['expectancy']:.2f}")
    print(f"  Max Drawdown (peak-to-trough on cumulative P&L): "
          f"{gross_metrics['max_drawdown']:.2f}")

    print("\n=== Variant A Risk Metrics (NET, after slippage + fees) ===")
    print(f"  Cost model: {args.cost_per_lot_leg} per lot per leg, "
          f"{args.slippage_pct * 100:.3f}% slippage on every fill")
    print(f"  Trades: {net_metrics['trade_count']}  "
          f"Win/Loss: {net_metrics['win_count']}W/{net_metrics['loss_count']}L")
    print(f"  Total P&L: {net_metrics['total_pnl']:.2f}")
    print(f"  Profit Factor: {net_metrics['profit_factor']:.3f}")
    print(f"  Expectancy/Trade: {net_metrics['expectancy']:.2f}")
    print(f"  Max Drawdown (peak-to-trough on cumulative P&L): "
          f"{net_metrics['max_drawdown']:.2f}")
    print(f"  Total cost drag vs gross: "
          f"{gross_metrics['total_pnl'] - net_metrics['total_pnl']:.2f}")

    audit_path = "data/bear_trap_oi_audit_trail.txt"
    with open(audit_path, "w", encoding="utf-8") as f:
        f.write(f"Bear Trap OI -- Complete Trade & Bar Audit Report\n")
        f.write(f"{len(all_dynamic_trades)} triggered trade(s) across all "
                f"tested historical days\n")
        f.write("=" * 78 + "\n\n")
        for t in sorted(all_dynamic_trades, key=lambda t: t["entry_ts"]):
            f.write(format_trade_audit(t, t["post_entry_bars"]))
            f.write("\n\n" + "=" * 78 + "\n\n")
    print(f"\nFull candle-by-candle audit trail written to {audit_path}")


if __name__ == "__main__":
    asyncio.run(main())
