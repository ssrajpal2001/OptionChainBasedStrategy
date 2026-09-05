"""
scripts/oi_orb_bear_trap_coforge_backtest.py — 2026-08-28, direct user spec:
replay today's real COFORGE stock spot data through the confirmed OI-ORB
bear-trap mechanic (3-min zone -> SL-break already baked into the zone ->
retest -> drop to 1-min -> fresh R1/R2 ladder -> entry on R2-breaches-R1),
using REAL, ALREADY-VALIDATED detector code:

  - strategies.v4_cascade.rolling_base / d1_trap_option.bear_only_book's
    _detect_bear_zones -- the exact zone detector D1TrapBearOnlyBook and
    D1TrapSRBook already run live (sweep+reclaim, zone=[sellers_in.low,
    ref.close], sl_level baked in at ref.high -- by the time a zone appears
    in this function's output, the "bears' SL already broke" event has
    ALREADY happened, at zone['lock_ts']).
  - strategies.core.support_resistance.SupportResistanceCalculator
    -- the exact ping-pong R1/R2/S1/S2 ladder every other strategy in this
    codebase's own trap/SR mechanic already uses.

Fresh, self-contained backtest wiring (zone-touch/retest gating, the R1
ladder feed, and the trailing-S1 exit) -- not yet a live strategy class, per
this session's "validate first" discipline. Runs on COFORGE's own STOCK
SPOT price (matches D1TrapSRBook's design: zone/SR logic on spot, execution
translated to the option's live LTP only at entry/exit).
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from datetime import datetime, time as dtime
from typing import List, Optional

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_intraday_1m
from strategies.d1_trap_option.bear_only_book import _detect_bear_zones
from strategies.core.support_resistance import SupportResistanceCalculator

TOKEN = sys.argv[1] if len(sys.argv) > 1 else ""
COFORGE_STOCK_KEY = "NSE_EQ|INE591G01025"
QTY = 475   # same lot as today's real COFORGE trade, for an apples-to-apples P&L comparison


@dataclass
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    timestamp: datetime = None   # alias for _detect_bear_zones' own bar.timestamp access

    def __post_init__(self):
        self.timestamp = self.ts


def to_bars(rows: List[dict]) -> List[Bar]:
    bars = [Bar(ts=datetime.fromisoformat(r["ts"]), open=r["open"], high=r["high"],
                low=r["low"], close=r["close"]) for r in rows]
    bars.sort(key=lambda b: b.ts)
    return bars


def to_n_min_bars(bars_1m: List[Bar], n: int) -> List[Bar]:
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


async def main():
    rows = await fetch_upstox_intraday_1m(COFORGE_STOCK_KEY, TOKEN)
    if not rows:
        print("No data returned -- aborting.")
        return
    bars_1m = to_bars(rows)
    bars_3m = to_n_min_bars(bars_1m, 3)
    print(f"{len(bars_1m)} 1-min bars -> {len(bars_3m)} 3-min bars, "
          f"{bars_1m[0].ts.strftime('%H:%M')} - {bars_1m[-1].ts.strftime('%H:%M')}")

    zones = _detect_bear_zones(bars_3m)
    print(f"\n{len(zones)} bear-trap zone(s) found on the 3-min chart (SL-break already baked in):")
    # NOTE: entry_line in _detect_bear_zones' own dict is ref.low -- the
    # bears' SL (ref.high, what actually broke to lock the zone) isn't
    # separately stored in this dict, so recompute it directly from the bar.
    for z in zones:
        ref_bar = bars_3m[z["ref_idx"]]
        print(f"  ref={z['ref_ts'].strftime('%H:%M')} zone=[{z['zone_lo']:.2f}, {z['zone_hi']:.2f}] "
              f"bears'_SL(ref.high)={ref_bar.high:.2f} broke(lock_ts)={z['lock_ts'].strftime('%H:%M')}")

    if not zones:
        print("\nNo zones -- nothing to backtest today.")
        return

    anchor = min(zones, key=lambda z: z["lock_ts"])
    print(f"\nAnchor zone (first to lock): ref={anchor['ref_ts'].strftime('%H:%M')} "
          f"zone=[{anchor['zone_lo']:.2f}, {anchor['zone_hi']:.2f}]")
    print("(distance-from-anchor filter not applied yet -- explicit user note: "
          "threshold is a future optimization item, not defined yet. Evaluating ALL zones.)")

    for z in zones:
        print(f"\n{'=' * 70}")
        print(f"ZONE ref={z['ref_ts'].strftime('%H:%M')} zone=[{z['zone_lo']:.2f}, {z['zone_hi']:.2f}] "
              f"locked(SL broke)@{z['lock_ts'].strftime('%H:%M')}")
        print(f"{'=' * 70}")

        # Retest: first 3-min bar AFTER lock_ts whose low touches back into the zone.
        after_lock = [b for b in bars_3m if b.ts > z["lock_ts"]]
        retest_bar = next((b for b in after_lock if b.low <= z["zone_hi"]), None)
        if retest_bar is None:
            print("  never retested the zone after the SL-break -- no trade from this zone today.")
            continue
        print(f"  RETEST at {retest_bar.ts.strftime('%H:%M')} (low={retest_bar.low:.2f} "
              f"touched zone_hi={z['zone_hi']:.2f})")

        # Drop to 1-min, fresh ladder from the retest bar onward.
        entry_bars = [b for b in bars_1m if b.ts >= retest_bar.ts]
        calc = SupportResistanceCalculator()
        position = None
        for b in entry_bars:
            phase_before = calc.get_calculated_sr_state("COFORGE").get("current_phase")
            calc.process_straddle_candle("COFORGE", {
                "timestamp": b.ts, "high": b.high, "low": b.low, "duration": 1,
            })
            state = calc.get_calculated_sr_state("COFORGE")
            phase_after = state.get("current_phase")
            sr = state.get("sr_levels", {})

            if position is None:
                if phase_before in ("S2_TRACKING", "R2_TRACKING") and phase_after == "R1_TRACKING":
                    s1 = sr.get("S1")
                    entry_price = b.close
                    live_sl = s1["low"] if s1 else entry_price
                    position = {"entry_ts": b.ts, "entry_price": entry_price, "sl": live_sl}
                    print(f"  ENTRY at {b.ts.strftime('%H:%M')} @ {entry_price:.2f} "
                          f"(R2->R1 breach) sl={live_sl:.2f}")
            else:
                s1 = sr.get("S1")
                if s1 is not None and s1["low"] > position["sl"]:
                    position["sl"] = s1["low"]   # ratchet only, never loosens
                if b.close <= position["sl"]:
                    pnl = (b.close - position["entry_price"]) * QTY
                    print(f"  EXIT (S1 TSL) at {b.ts.strftime('%H:%M')} @ {b.close:.2f} "
                          f"sl={position['sl']:.2f} pnl=Rs{pnl:,.2f}")
                    position = None
                    break

        if position is not None:
            last = entry_bars[-1]
            pnl = (last.close - position["entry_price"]) * QTY
            print(f"  still open at end of available data -- last close={last.close:.2f} "
                  f"unrealized pnl=Rs{pnl:,.2f}")


if __name__ == "__main__":
    asyncio.run(main())
