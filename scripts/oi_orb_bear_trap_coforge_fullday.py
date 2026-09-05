"""
scripts/oi_orb_bear_trap_coforge_fullday.py — complete-day trace for COFORGE
under the confirmed bear-trap mechanic, corrected per direct user feedback:
  - 3-min grid stays anchored to market open (09:15) -- standard candles,
    never re-anchored to any later "start" time.
  - Zone ref candles are only considered from [09:24-09:27) onward (the
    candle containing 09:26, the user's own confirmed starting point for
    this stock) -- anything before that is excluded.
  - 2026-08-28 SHARPNESS FIX (real finding: the [09:24-09:27) zone took
    nearly an HOUR to sweep-then-reclaim -- ordinary chop, not a real trap,
    confirmed by direct chart review): a genuine bear trap needs the
    sweep-then-reclaim to happen within a TIGHT bar count, not drawn out.
    find_all_bear_zones itself is deliberately uncapped (v4_cascade's own
    docstring: "NO bar-count cap" -- correct for ITS OWN use case) -- this
    script layers its own MAX_BARS_SINCE_SWEEP filter on top, applied here
    only, not by editing the shared live function.
  - "Ignore zones way above the anchor" filter is still NOT applied
    (explicit user note: that threshold is a separate, later optimization
    item).
  - Only ONE position open at a time (matches every other strategy's own
    rule) -- once a trade is open, later zones' retests are ignored until
    that position exits.
"""
from __future__ import annotations

import asyncio
import sys
from datetime import timedelta

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_intraday_1m
from strategies.core.support_resistance import SupportResistanceCalculator
from scripts.oi_orb_bear_trap_coforge_backtest import to_bars, to_n_min_bars, COFORGE_STOCK_KEY
from strategies.liquidity_trap.detector import find_all_setups
from strategies.d1_trap_option.bear_only_book import _collapse_nearby_zones

TOKEN = sys.argv[1] if len(sys.argv) > 1 else ""
QTY = 475
ZONE_START_CUTOFF = "09:24"   # exclude ref candles from before the [09:24-09:27) window
APPLY_MERGE = True            # 2026-08-28: merge zones using the real, already-validated
                               # _collapse_nearby_zones (threshold_pts=20, max_ref_gap=2 bars)
                               # -- price-overlap AND ref-candle time proximity both required.


def sharp_bear_zones(bars_3m) -> list:
    """2026-08-28, corrected per direct user feedback: reuses the REAL,
    already-validated strategies.liquidity_trap.detector functions instead
    of a hand-rolled reimplementation.

    Stage 1 (find_all_setups): strict immediate-adjacency sweep -- only the
    very next 3-min candle counts for "sellers denied". This is the fix for
    the earlier wrong zone (09:27-09:30, whose own next candle never broke
    its low at all).

    Stage 2 (SL-hit, inlined here the same way find_sl_hit works): NO time
    cap -- an earlier attempt added an artificial max-bars-since-sweep
    limit that doesn't exist in the real validated function, and it wrongly
    excluded a real zone (sweep confirmed at 09:33's next candle, but SL
    didn't break until 11:00 -- 27 bars later, past that wrong cap). "Any
    time of day the SL gets hit confirms bears are trapped" -- direct user
    correction. Scans forward with no limit, exactly like find_sl_hit."""
    setups = [s for s in find_all_setups(bars_3m) if s.direction == "BEAR"]
    out = []
    for s in setups:
        ref = bars_3m[s.ref_idx]
        nxt = bars_3m[s.locked_idx]
        lock_ts = None
        for k in range(s.locked_idx + 1, len(bars_3m)):
            if bars_3m[k].high > ref.high:
                lock_ts = bars_3m[k].ts
                break
        if lock_ts is None:
            continue   # SL hasn't broken yet today -- not a confirmed trap
        out.append(dict(
            zone_lo=nxt.low, zone_hi=ref.close, entry_line=ref.low,
            lock_ts=lock_ts, ref_ts=ref.ts, ref_idx=s.ref_idx,
        ))
    return out


async def main():
    rows = await fetch_upstox_intraday_1m(COFORGE_STOCK_KEY, TOKEN)
    bars_1m = to_bars(rows)
    bars_3m = to_n_min_bars(bars_1m, 3)

    zones = [z for z in sharp_bear_zones(bars_3m)
             if z["ref_ts"].strftime("%H:%M") >= ZONE_START_CUTOFF]
    raw_count = len(zones)
    if APPLY_MERGE:
        zones = _collapse_nearby_zones(zones)
    zones.sort(key=lambda z: z["lock_ts"])
    merge_note = f" ({raw_count} raw -> {len(zones)} after time-gated merge)" if APPLY_MERGE else ""
    print(f"COFORGE {bars_1m[0].ts.strftime('%H:%M')}-{bars_1m[-1].ts.strftime('%H:%M')}, "
          f"{len(bars_3m)} 3-min bars, {len(zones)} zones{merge_note} "
          f"(ref >= {ZONE_START_CUTOFF}, strict-adjacency sweep, uncapped SL-hit).\n")

    trades = []
    position = None

    for z in zones:
        ref_bar = bars_3m[z["ref_idx"]]
        ref_end = z["ref_ts"] + timedelta(minutes=3)
        print(f"ZONE ref=[{z['ref_ts'].strftime('%H:%M')}-{ref_end.strftime('%H:%M')}) "
              f"O={ref_bar.open} H={ref_bar.high} L={ref_bar.low} C={ref_bar.close}  "
              f"zone=[{z['zone_lo']:.2f},{z['zone_hi']:.2f}]  bearsSL={ref_bar.high:.2f}  "
              f"locked@{z['lock_ts'].strftime('%H:%M')}")

        after_lock = [b for b in bars_3m if b.ts > z["lock_ts"]]
        retest_bar = next((b for b in after_lock if b.low <= z["zone_hi"]), None)
        if retest_bar is None:
            print("  no retest yet -- no trade from this zone.\n")
            continue

        if position is not None and retest_bar.ts < position["exit_ts"]:
            print(f"  retest at {retest_bar.ts.strftime('%H:%M')} -- SKIPPED, a position was already open then.\n")
            continue

        print(f"  RETEST at {retest_bar.ts.strftime('%H:%M')} (low={retest_bar.low:.2f} "
              f"touched zone_hi={z['zone_hi']:.2f})")

        # 2026-08-28, direct user spec: entry ladder stays on 1-min (R2->R1
        # breach, unchanged), but the TSL now tracks S1 from a SEPARATE,
        # PARALLEL 3-min ladder instead of the 1-min one -- a 3-min S1 is a
        # slower, more stable support level, less prone to being clipped by
        # ordinary 1-min noise (the exact failure mode behind COFORGE's own
        # earlier fast whipsaw stops). Both ladders start fresh from the
        # SAME retest bar.
        entry_bars = [b for b in bars_1m if b.ts >= retest_bar.ts]
        tsl_bars_3m = [b for b in bars_3m if b.ts >= retest_bar.ts]
        calc = SupportResistanceCalculator()
        calc3 = SupportResistanceCalculator()
        tsl_idx = 0   # how many 3-min bars have been fed to calc3 so far
        pos = None
        for b in entry_bars:
            phase_before = calc.get_calculated_sr_state("COFORGE").get("current_phase")
            calc.process_straddle_candle("COFORGE", {
                "timestamp": b.ts, "high": b.high, "low": b.low, "duration": 1,
            })
            state = calc.get_calculated_sr_state("COFORGE")
            phase_after = state.get("current_phase")
            sr = state.get("sr_levels", {})

            # Feed calc3 every 3-min bar that has fully closed as of this 1-min tick.
            while tsl_idx < len(tsl_bars_3m) and tsl_bars_3m[tsl_idx].ts + timedelta(minutes=3) <= b.ts:
                b3 = tsl_bars_3m[tsl_idx]
                calc3.process_straddle_candle("COFORGE_3M", {
                    "timestamp": b3.ts, "high": b3.high, "low": b3.low, "duration": 3,
                })
                tsl_idx += 1
            sr3 = calc3.get_calculated_sr_state("COFORGE_3M").get("sr_levels", {})

            if pos is None:
                if phase_before in ("S2_TRACKING", "R2_TRACKING") and phase_after == "R1_TRACKING":
                    s1_3m = sr3.get("S1")
                    entry_price = b.close
                    live_sl = s1_3m["low"] if s1_3m else entry_price
                    pos = {"entry_ts": b.ts, "entry_price": entry_price, "sl": live_sl}
                    print(f"  ENTRY at {b.ts.strftime('%H:%M')} @ {entry_price:.2f} sl(3m S1)={live_sl:.2f}")
            else:
                s1_3m = sr3.get("S1")
                if s1_3m is not None and s1_3m["low"] > pos["sl"]:
                    pos["sl"] = s1_3m["low"]
                if b.close <= pos["sl"]:
                    pnl = (b.close - pos["entry_price"]) * QTY
                    print(f"  EXIT (3m S1 TSL) at {b.ts.strftime('%H:%M')} @ {b.close:.2f} "
                          f"sl={pos['sl']:.2f} pnl=Rs{pnl:,.2f}\n")
                    trades.append({"entry_ts": pos["entry_ts"], "entry_price": pos["entry_price"],
                                    "exit_ts": b.ts, "exit_price": b.close, "sl": pos["sl"], "pnl": pnl})
                    position = trades[-1]
                    pos = None
                    break
        if pos is not None:
            last = entry_bars[-1]
            pnl = (last.close - pos["entry_price"]) * QTY
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
    print(f"\nTOTAL for COFORGE today (this mechanic): Rs{total:,.2f}  ({len(distinct)} trades)")


if __name__ == "__main__":
    asyncio.run(main())
