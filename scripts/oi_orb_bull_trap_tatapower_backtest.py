"""
scripts/oi_orb_bull_trap_tatapower_backtest.py — SHORT/PE side mirror of the
now-confirmed bear-trap mechanic, applied to TATAPOWER's real spot data for
2026-08-27 (the day it was shortlisted BEARISH by OI-ORB's own screener).

Exact mirror of the validated long-side mechanic:
  - Zone: bull-trap (buyers trapped) via strategies.liquidity_trap.detector.
    find_all_setups (direction="BULL") -- strict immediate-adjacency: the
    very NEXT 3-min candle after ref must break ref's HIGH (buyers denied
    on the very next bar). Zone = [ref.close, nxt.high].
  - Bulls' SL = ref's own LOW. Confirmed (locked) the first later candle
    whose LOW breaks below it -- unbounded scan, no time cap (matches the
    real, validated find_sl_hit's own design, and the direct user
    correction on the long side).
  - Zone merge: same real _collapse_nearby_zones (threshold=20pts,
    max_ref_gap=2 bars) -- direction-agnostic, reused as-is.
  - Retest: a later 3-min bar's HIGH touches back down into the zone
    (>= zone_lo).
  - Entry: fresh 1-min SupportResistanceCalculator from the retest bar;
    fires on S2_TRACKING/R2_TRACKING -> S1_TRACKING (the mirror of the
    long side's R2/S2 -> R1_TRACKING check).
  - TSL: a SEPARATE, parallel 3-min ladder's live R1 -- ratchets DOWN only
    (direct user spec: "for short trade R1 can only move down"), exit
    fires the instant a close breaches back above it.
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_range_1m
from strategies.core.support_resistance import SupportResistanceCalculator
from strategies.d1_trap_option.bear_only_book import _collapse_nearby_zones
from strategies.liquidity_trap.detector import find_all_setups

TOKEN = sys.argv[1] if len(sys.argv) > 1 else ""
STOCK_KEY = "NSE_EQ|INE205A01025"   # VEDL
STOCK_NAME = "VEDL"
TRADE_DATE = date(2026, 8, 27)
QTY = 1150   # VEDL F&O lot size
ZONE_START_CUTOFF = "09:24"


@dataclass
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float


def to_bars(rows):
    bars = [Bar(ts=datetime.fromisoformat(r["ts"]), open=r["open"], high=r["high"],
                low=r["low"], close=r["close"]) for r in rows]
    bars.sort(key=lambda b: b.ts)
    return bars


def to_n_min_bars(bars_1m, n):
    buckets = {}
    for b in bars_1m:
        floored = (b.ts.minute // n) * n
        key = (b.ts.hour, floored)
        buckets.setdefault(key, []).append(b)
    out = []
    for key in sorted(buckets.keys()):
        g = sorted(buckets[key], key=lambda x: x.ts)
        out.append(Bar(ts=g[0].ts, open=g[0].open, high=max(x.high for x in g),
                        low=min(x.low for x in g), close=g[-1].close))
    return out


def bull_trap_zones(bars_3m) -> list:
    """Mirror of the long side's sharp_bear_zones -- strict adjacency,
    unbounded SL-hit scan."""
    setups = [s for s in find_all_setups(bars_3m) if s.direction == "BULL"]
    out = []
    for s in setups:
        ref = bars_3m[s.ref_idx]
        nxt = bars_3m[s.locked_idx]
        lock_ts = None
        for k in range(s.locked_idx + 1, len(bars_3m)):
            if bars_3m[k].low < ref.low:
                lock_ts = bars_3m[k].ts
                break
        if lock_ts is None:
            continue
        out.append(dict(
            zone_lo=ref.close, zone_hi=nxt.high, entry_line=ref.high,
            lock_ts=lock_ts, ref_ts=ref.ts, ref_idx=s.ref_idx,
        ))
    return out


async def main():
    start = end = TRADE_DATE
    rows = await fetch_upstox_range_1m(STOCK_KEY, TOKEN, start, end)
    if not rows:
        print("No data returned -- aborting.")
        return
    bars_1m = to_bars(rows)
    bars_3m = to_n_min_bars(bars_1m, 3)
    print(f"{STOCK_NAME} {TRADE_DATE} {bars_1m[0].ts.strftime('%H:%M')}-{bars_1m[-1].ts.strftime('%H:%M')}, "
          f"{len(bars_1m)} 1-min bars, {len(bars_3m)} 3-min bars\n")

    zones = [z for z in bull_trap_zones(bars_3m) if z["ref_ts"].strftime("%H:%M") >= ZONE_START_CUTOFF]
    raw_count = len(zones)
    zones = _collapse_nearby_zones(zones)
    zones.sort(key=lambda z: z["lock_ts"])
    print(f"{len(zones)} bull-trap zones ({raw_count} raw -> {len(zones)} after time-gated merge)\n")

    trades = []
    position = None

    for z in zones:
        ref_bar = bars_3m[z["ref_idx"]]
        ref_end = z["ref_ts"] + timedelta(minutes=3)
        print(f"ZONE ref=[{z['ref_ts'].strftime('%H:%M')}-{ref_end.strftime('%H:%M')}) "
              f"O={ref_bar.open} H={ref_bar.high} L={ref_bar.low} C={ref_bar.close}  "
              f"zone=[{z['zone_lo']:.2f},{z['zone_hi']:.2f}]  bullsSL={ref_bar.low:.2f}  "
              f"locked@{z['lock_ts'].strftime('%H:%M')}")

        after_lock = [b for b in bars_3m if b.ts > z["lock_ts"]]
        retest_bar = next((b for b in after_lock if b.high >= z["zone_lo"]), None)
        if retest_bar is None:
            print("  no retest yet -- no trade from this zone.\n")
            continue

        if position is not None and retest_bar.ts < position["exit_ts"]:
            print(f"  retest at {retest_bar.ts.strftime('%H:%M')} -- SKIPPED, a position was already open then.\n")
            continue

        print(f"  RETEST at {retest_bar.ts.strftime('%H:%M')} (high={retest_bar.high:.2f} "
              f"touched zone_lo={z['zone_lo']:.2f})")

        entry_bars = [b for b in bars_1m if b.ts >= retest_bar.ts]
        tsl_bars_3m = [b for b in bars_3m if b.ts >= retest_bar.ts]
        calc = SupportResistanceCalculator()
        calc3 = SupportResistanceCalculator()
        tsl_idx = 0
        pos = None
        for b in entry_bars:
            phase_before = calc.get_calculated_sr_state(STOCK_NAME).get("current_phase")
            calc.process_straddle_candle(STOCK_NAME, {
                "timestamp": b.ts, "high": b.high, "low": b.low, "duration": 1,
            })
            state = calc.get_calculated_sr_state(STOCK_NAME)
            phase_after = state.get("current_phase")

            while tsl_idx < len(tsl_bars_3m) and tsl_bars_3m[tsl_idx].ts + timedelta(minutes=3) <= b.ts:
                b3 = tsl_bars_3m[tsl_idx]
                calc3.process_straddle_candle(f"{STOCK_NAME}_3M", {
                    "timestamp": b3.ts, "high": b3.high, "low": b3.low, "duration": 3,
                })
                tsl_idx += 1
            sr3 = calc3.get_calculated_sr_state(f"{STOCK_NAME}_3M").get("sr_levels", {})

            if pos is None:
                if phase_before in ("S2_TRACKING", "R2_TRACKING") and phase_after == "S1_TRACKING":
                    r1_3m = sr3.get("R1")
                    entry_price = b.close
                    live_sl = r1_3m["high"] if r1_3m else entry_price
                    pos = {"entry_ts": b.ts, "entry_price": entry_price, "sl": live_sl}
                    print(f"  ENTRY at {b.ts.strftime('%H:%M')} @ {entry_price:.2f} sl(3m R1)={live_sl:.2f}")
            else:
                r1_3m = sr3.get("R1")
                if r1_3m is not None and r1_3m["high"] < pos["sl"]:
                    pos["sl"] = r1_3m["high"]   # ratchets DOWN only, never loosens
                if b.close >= pos["sl"]:
                    pnl = (pos["entry_price"] - b.close) * QTY   # SHORT: profit when price falls
                    print(f"  EXIT (3m R1 TSL) at {b.ts.strftime('%H:%M')} @ {b.close:.2f} "
                          f"sl={pos['sl']:.2f} pnl=Rs{pnl:,.2f}\n")
                    trades.append({"entry_ts": pos["entry_ts"], "entry_price": pos["entry_price"],
                                    "exit_ts": b.ts, "exit_price": b.close, "sl": pos["sl"], "pnl": pnl})
                    position = trades[-1]
                    pos = None
                    break
        if pos is not None:
            last = entry_bars[-1]
            pnl = (pos["entry_price"] - last.close) * QTY
            print(f"  still open at end of data ({last.ts.strftime('%H:%M')} @ {last.close:.2f}) "
                  f"unrealized=Rs{pnl:,.2f}\n")
            trades.append({"entry_ts": pos["entry_ts"], "entry_price": pos["entry_price"],
                            "exit_ts": last.ts, "exit_price": last.close, "sl": pos["sl"], "pnl": pnl,
                            "open": True})
            position = trades[-1]

    seen = set()
    distinct = []
    for t in sorted(trades, key=lambda x: x["entry_ts"]):
        if t["entry_ts"] in seen:
            continue
        seen.add(t["entry_ts"])
        distinct.append(t)

    print("=" * 78)
    print("DISTINCT TRADES FOR THE DAY (deduplicated, one position at a time):")
    print("=" * 78)
    total = 0.0
    for t in distinct:
        tag = " (still open)" if t.get("open") else ""
        print(f"  {t['entry_ts'].strftime('%H:%M')} @ {t['entry_price']:.2f}  ->  "
              f"{t['exit_ts'].strftime('%H:%M')} @ {t['exit_price']:.2f}  "
              f"pnl=Rs{t['pnl']:>10,.2f}{tag}")
        total += t["pnl"]
    print(f"\nTOTAL for TATAPOWER {TRADE_DATE} (short/bull-trap mechanic): "
          f"Rs{total:,.2f}  ({len(distinct)} trades)")


if __name__ == "__main__":
    asyncio.run(main())
