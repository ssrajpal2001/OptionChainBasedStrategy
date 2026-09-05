"""
scripts/pdh_pdl_backtest.py — one-off backtest for the "PDH-PDL" concept
(2026-08-28, direct user spec, real NIFTY spot data via Upstox).

Mechanic (as confirmed with the user):
  1. Each trading day, PDH/PDL = the PREVIOUS trading day's own daily high/low.
  2. LONG side: once price has traded ABOVE PDH at some point intraday
     ("breached"), the first later 1-min bar whose LOW touches back down to
     PDH ("retest") is the trigger to move forward.
  3. From the retest bar onward, build a FRESH SupportResistanceCalculator
     (same ping-pong R1/R2/S1/S2 ladder D1Trap-SR/CAG Straddle already use --
     reused directly, not reimplemented) on 3-MIN NIFTY spot bars, with no
     pre-retest history fed in (mirrors CAG Straddle's own "fresh calculator,
     no pre-window history" precedent).
  4. ENTRY fires on the "R2 breaches R1" phase transition
     (R2_TRACKING -> R1_TRACKING) -- a bought CE, spot-proxied. Direct user
     spec: the breach that actually fires must itself land ABOVE PDH (not
     just that the earlier retest touched PDH once) -- a ladder that
     wandered back below PDH during a pullback and then breaches there does
     NOT count.
  5. SL = the CURRENT S1's own low minus a 5-point buffer. As the ladder
     promotes to new S1 levels while the trade runs, the stop TRAILS to each
     new S1's low minus 5pts (never loosens) -- exit the instant a later
     3-min bar's LOW breaches it. No fixed target.
  6. SHORT side is the exact mirror: PDL breach -> retest -> fresh ladder ->
     entry on S2_TRACKING -> S1_TRACKING (a bought PE, spot-proxied), the
     firing breach must land BELOW PDL -> SL = current R1's high + 5pts,
     trailing to each new R1.
  7. Session 09:15-15:15 IST. Multiple trades/day: after any exit (SL/TSL or
     EOD), the day's retest-arming state resets and scanning resumes on BOTH
     sides immediately (a fresh PDH/PDL breach+retest is required to arm
     again -- a single retest is "spent" once it starts a tracking session,
     whether or not that session ever actually fires an entry).
  8. Only ONE position (or one pending, not-yet-triggered tracking session)
     open at a time -- matches every other single-position strategy in this
     codebase.

HONEST CAVEAT, stated up front and in every result: there is no 2-year
option-premium history available (Upstox's historical-candle API has no such
depth for individual option contracts), so P&L here is computed in NIFTY
SPOT POINTS x lot size (65), NOT real option premium. This is an
approximation -- delta, theta, and IV all mean an option's real premium does
not move 1:1 with spot -- exactly the same honesty standard every other
backtest in this codebase already applies to its own limitations.

Everything here is a fresh, self-contained implementation for this one
backtest script -- not wired into the live app, no new strategy class, same
precedent as scripts/liquidity_trap_backtest.py and
scripts/nifty_1500_sr_breakout_backtest.py.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer.historical_candles import fetch_upstox_range_1m
from strategies.core.support_resistance import SupportResistanceCalculator

TOKEN = sys.argv[1] if len(sys.argv) > 1 else ""
INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
LOT_SIZE = 65
SL_BUFFER_PTS = 5.0
SESSION_START = time(9, 15)
SESSION_END = time(15, 15)
CACHE_PATH = "scratch_pdh_pdl_2y_1m_cache.json"
YEARS_BACK = 2

# 2026-08-28, corrected after direct user chart review: unlike CAG Straddle
# (which only counts a breach into R1_TRACKING from S2_TRACKING/R2_TRACKING --
# the ping-pong pullback-promotion path), PDH-PDL's own spec is broader: "when
# S1 or R1 is breached, then only trade will start" -- ANY breach of the
# CURRENT live S1/R1 counts, including a straight "Directional Flip" through
# it with no intermediate S2/R2 pullback at all (support_resistance.py's own
# R1_TRACKING -> S1_TRACKING / S1_TRACKING -> R1_TRACKING transitions). Only
# the very first INITIAL_TREND_ESTABLISHMENT -> R1_TRACKING/S1_TRACKING
# transition stays excluded (a fresh base-candle breakout, not a real
# re-breach of an already-established level).
_LONG_BREACH_FROM_PHASES = ("S1_TRACKING", "S2_TRACKING", "R2_TRACKING")
_SHORT_BREACH_FROM_PHASES = ("R1_TRACKING", "S2_TRACKING", "R2_TRACKING")


@dataclass
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float


@dataclass
class Trade:
    direction: str          # "LONG" or "SHORT"
    entry_ts: datetime
    entry_price: float
    initial_sl: float
    exit_ts: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: str = ""
    final_sl: float = 0.0
    pnl_pts: float = 0.0
    pnl_rs: float = 0.0

    def finalize(self):
        if self.exit_price is None:
            return
        self.pnl_pts = ((self.exit_price - self.entry_price) if self.direction == "LONG"
                         else (self.entry_price - self.exit_price))
        self.pnl_rs = self.pnl_pts * LOT_SIZE


# ── data loading ─────────────────────────────────────────────────────────

async def load_1m_bars() -> List[Bar]:
    if os.path.exists(CACHE_PATH):
        print(f"Loading cached 1-min bars from {CACHE_PATH} ...", flush=True)
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            rows = json.load(f)
        print(f"Loaded {len(rows)} cached 1-min candles.", flush=True)
    else:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=365 * YEARS_BACK)
        print(f"Fetching 1-min NIFTY spot ({INSTRUMENT_KEY}) from {start} to {end} "
              f"({YEARS_BACK} years) -- this is ~{(end - start).days} calendar days, "
              f"one Upstox call per weekday, will take a while ...", flush=True)
        rows = await fetch_upstox_range_1m(INSTRUMENT_KEY, TOKEN, start, end)
        print(f"Fetched {len(rows)} 1-min candles.", flush=True)
        if rows:
            with open(CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(rows, f)
            print(f"Cached to {CACHE_PATH} for reuse.", flush=True)
    if not rows:
        return []
    bars = [Bar(ts=datetime.fromisoformat(r["ts"]), open=r["open"], high=r["high"],
                low=r["low"], close=r["close"]) for r in rows]
    bars.sort(key=lambda b: b.ts)
    return bars


def group_by_day(bars: List[Bar]) -> Dict[date, List[Bar]]:
    days: Dict[date, List[Bar]] = {}
    for b in bars:
        days.setdefault(b.ts.date(), []).append(b)
    return days


def daily_high_low(day_bars: List[Bar]) -> tuple:
    return max(b.high for b in day_bars), min(b.low for b in day_bars)


def to_3min_bars(day_bars: List[Bar]) -> List[Bar]:
    """Aggregate a day's 1-min bars into 3-min buckets, floored to the
    session start (09:15) so buckets align 09:15-09:17, 09:18-09:20, ..."""
    buckets: Dict[int, List[Bar]] = {}
    base_minutes = SESSION_START.hour * 60 + SESSION_START.minute
    for b in day_bars:
        mins = b.ts.hour * 60 + b.ts.minute
        bucket_idx = (mins - base_minutes) // 3
        buckets.setdefault(bucket_idx, []).append(b)
    out = []
    for idx in sorted(buckets.keys()):
        group = sorted(buckets[idx], key=lambda x: x.ts)
        out.append(Bar(
            ts=group[0].ts, open=group[0].open, high=max(g.high for g in group),
            low=min(g.low for g in group), close=group[-1].close,
        ))
    return out


# ── per-day mechanic ─────────────────────────────────────────────────────

def run_day(prev_pdh: float, prev_pdl: float, day_1m: List[Bar], day_3m: List[Bar],
            inst_key: str, retest_tol_pts: float = 0.0, sl_buffer_pts: float = SL_BUFFER_PTS,
            long_breach_phases=_LONG_BREACH_FROM_PHASES,
            short_breach_phases=_SHORT_BREACH_FROM_PHASES) -> List[Trade]:
    trades: List[Trade] = []
    breached_high = False   # PDH has been traded through at some point since the last retest
    breached_low = False    # PDL has been traded through at some point since the last retest

    position: Optional[Trade] = None
    calc: Optional[SupportResistanceCalculator] = None
    session_direction: Optional[str] = None   # "LONG" or "SHORT" -- which side the active calc is armed for
    session_active = False   # a retest has fired and a ladder is building, entry not yet confirmed
    live_sl: Optional[float] = None

    m1_idx = 0
    n1 = len(day_1m)

    for i, bar3 in enumerate(day_3m):
        bar_start = bar3.ts
        bar_end = day_3m[i + 1].ts if i + 1 < len(day_3m) else None

        # Advance the 1-min cursor through all 1-min bars inside this 3-min
        # window FIRST, to detect PDH/PDL breach/retest at 1-min resolution
        # (a real trader watches every tick, not just the 3-min close).
        while m1_idx < n1 and day_1m[m1_idx].ts < bar_start:
            m1_idx += 1
        cursor = m1_idx
        while cursor < n1 and (bar_end is None or day_1m[cursor].ts < bar_end):
            b1 = day_1m[cursor]
            if not (SESSION_START <= b1.ts.time() < SESSION_END):
                cursor += 1
                continue

            if not session_active and position is None:
                if not breached_high and b1.high > prev_pdh:
                    breached_high = True
                if not breached_low and b1.low < prev_pdl:
                    breached_low = True

                if breached_high and b1.low <= prev_pdh + retest_tol_pts:
                    session_active = True
                    session_direction = "LONG"
                    breached_high = False
                    calc = SupportResistanceCalculator()
                elif breached_low and b1.high >= prev_pdl - retest_tol_pts:
                    session_active = True
                    session_direction = "SHORT"
                    breached_low = False
                    calc = SupportResistanceCalculator()
            cursor += 1
        m1_idx = cursor

        # Feed the SAME 3-min ladder forward once a session (pending or live) exists.
        if session_active or position is not None:
            phase_before = calc.get_calculated_sr_state(inst_key).get("current_phase")
            calc.process_straddle_candle(inst_key, {
                "timestamp": bar3.ts, "high": bar3.high, "low": bar3.low, "duration": 3,
            })
            state = calc.get_calculated_sr_state(inst_key)
            phase_after = state.get("current_phase")
            sr = state.get("sr_levels", {})

            if position is None and session_active:
                fired = False
                # 2026-08-28 direct user spec: the breach that actually fires
                # entry must itself sit on the correct side of PDH/PDL -- a
                # retest that touched PDH once does NOT license an R1 breach
                # that happens to occur back below PDH (e.g. after the ladder
                # wandered back down through it during a pullback).
                if (session_direction == "LONG" and phase_before in long_breach_phases
                        and phase_after == "R1_TRACKING"
                        and sr.get("R1", {}).get("high", 0) > prev_pdh):
                    fired = True
                elif (session_direction == "SHORT" and phase_before in short_breach_phases
                        and phase_after == "S1_TRACKING"
                        and sr.get("S1", {}).get("low", 0) < prev_pdl):
                    fired = True
                if fired:
                    entry_price = bar3.close
                    if session_direction == "LONG":
                        s1 = sr.get("S1")
                        live_sl = (s1["low"] - sl_buffer_pts) if s1 else entry_price - sl_buffer_pts
                    else:
                        r1 = sr.get("R1")
                        live_sl = (r1["high"] + sl_buffer_pts) if r1 else entry_price + sl_buffer_pts
                    position = Trade(direction=session_direction, entry_ts=bar3.ts,
                                      entry_price=entry_price, initial_sl=live_sl)

            if position is not None:
                # Ratchet the TSL to whichever S1 (long) / R1 (short) is now live --
                # never loosens (only accept a tighter/better level).
                if position.direction == "LONG":
                    s1 = sr.get("S1")
                    if s1 is not None:
                        candidate = s1["low"] - sl_buffer_pts
                        if live_sl is None or candidate > live_sl:
                            live_sl = candidate
                    if live_sl is not None and bar3.low <= live_sl:
                        position.exit_ts = bar3.ts
                        position.exit_price = live_sl
                        position.exit_reason = "tsl"
                        position.final_sl = live_sl
                        position.finalize()
                        trades.append(position)
                        position = None
                        session_active = False
                        calc = None
                        live_sl = None
                else:
                    r1 = sr.get("R1")
                    if r1 is not None:
                        candidate = r1["high"] + sl_buffer_pts
                        if live_sl is None or candidate < live_sl:
                            live_sl = candidate
                    if live_sl is not None and bar3.high >= live_sl:
                        position.exit_ts = bar3.ts
                        position.exit_price = live_sl
                        position.exit_reason = "tsl"
                        position.final_sl = live_sl
                        position.finalize()
                        trades.append(position)
                        position = None
                        session_active = False
                        calc = None
                        live_sl = None

    # EOD force-close.
    if position is not None:
        last_bar = day_3m[-1]
        position.exit_ts = last_bar.ts
        position.exit_price = last_bar.close
        position.exit_reason = "eod"
        position.final_sl = live_sl or position.initial_sl
        position.finalize()
        trades.append(position)

    return trades


# ── main ─────────────────────────────────────────────────────────────────

async def main():
    bars_1m = await load_1m_bars()
    if not bars_1m:
        print("No data fetched -- aborting.")
        return

    days = group_by_day(bars_1m)
    sorted_dates = sorted(days.keys())
    print(f"{len(sorted_dates)} trading days loaded, "
          f"{sorted_dates[0]} .. {sorted_dates[-1]}.", flush=True)

    all_trades: List[Trade] = []
    prev_high = prev_low = None
    for d in sorted_dates:
        day_bars = sorted(days[d], key=lambda b: b.ts)
        if prev_high is not None:
            day_3m = to_3min_bars(day_bars)
            trades = run_day(prev_high, prev_low, day_bars, day_3m, inst_key="NIFTY")
            all_trades.extend(trades)
        prev_high, prev_low = daily_high_low(day_bars)

    print(f"\n{len(all_trades)} trades total.\n", flush=True)

    wins = [t for t in all_trades if t.pnl_pts > 0]
    losses = [t for t in all_trades if t.pnl_pts <= 0]
    gross_profit = sum(t.pnl_rs for t in wins)
    gross_loss = -sum(t.pnl_rs for t in losses)
    net = sum(t.pnl_rs for t in all_trades)
    win_pct = (len(wins) / len(all_trades) * 100.0) if all_trades else 0.0
    pf = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")

    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for t in all_trades:
        equity += t.pnl_rs
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)

    print(f"Win% = {win_pct:.1f}  PF = {pf:.2f}  Net = Rs{net:,.2f}  "
          f"MaxDD = Rs{max_dd:,.2f}  n={len(all_trades)}", flush=True)

    out_path = "scratch_pdh_pdl_trades.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump([{
            "direction": t.direction,
            "entry_ts": t.entry_ts.isoformat(),
            "entry_price": t.entry_price,
            "initial_sl": t.initial_sl,
            "exit_ts": t.exit_ts.isoformat() if t.exit_ts else None,
            "exit_price": t.exit_price,
            "final_sl": t.final_sl,
            "exit_reason": t.exit_reason,
            "pnl_pts": t.pnl_pts,
            "pnl_rs": t.pnl_rs,
        } for t in all_trades], f, indent=2)
    print(f"Trade log written to {out_path}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
