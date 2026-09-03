"""One-off: print every stage of the Liquidity Trap mechanic for a single
real day, using the already-cached 1-min SENSEX bars, for a fully worked,
numbers-attached explanation. Reuses the exact functions from
scripts/liquidity_trap_backtest.py -- not a re-implementation."""
import asyncio
import sys
from datetime import date

sys.path.insert(0, ".")
sys.path.insert(0, "scripts")
from liquidity_trap_backtest import (
    Bar, load_1m_bars, resample, by_day, swing_points,
    find_ref_and_bias, find_all_bear_zones, find_all_bull_zones,
)

TARGET_DATE = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date(2025, 9, 8)


async def main():
    bars_1m = await load_1m_bars()
    bars_15m = resample(bars_1m, 15)
    bars_5m = resample(bars_1m, 5)
    days_1m = by_day(bars_1m)

    bars_15m_day = [b for b in bars_15m if b.ts.date() == TARGET_DATE]
    bars_1m_day = days_1m.get(TARGET_DATE, [])
    print(f"=== {TARGET_DATE} — {len(bars_15m_day)} 15m bars, {len(bars_1m_day)} 1m bars ===\n")

    print("-- 15m bars (first 8) --")
    for b in bars_15m_day[:8]:
        print(f"  {b.ts.strftime('%H:%M')}  O={b.open:.2f} H={b.high:.2f} L={b.low:.2f} C={b.close:.2f}")

    res = find_ref_and_bias(bars_15m_day)
    if res is None:
        print("No bias locked today.")
        return
    bias, ref_idx, lock_idx = res
    ref = bars_15m_day[ref_idx]
    lock_bar = bars_15m_day[lock_idx]
    print(f"\nSTAGE 1 — REF CANDLE ROLLING:")
    print(f"  ref candle @ {ref.ts.strftime('%H:%M')}  H={ref.high:.2f} L={ref.low:.2f}"
          f" (index {ref_idx} in today's 15m series)")
    print(f"  LOCK candle @ {lock_bar.ts.strftime('%H:%M')}  H={lock_bar.high:.2f} L={lock_bar.low:.2f}"
          f"  -> breached ref's {'HIGH' if bias=='BULL' else 'LOW'} only -> bias={bias} LOCKED, ref stays fixed")

    watch_level = ref.low if bias == "BULL" else ref.high
    print(f"\nSTAGE 2 — WATCH FOR REF CANDLE'S OWN {'LOW' if bias=='BULL' else 'HIGH'} TO BE HIT:")
    print(f"  watch_level = {watch_level:.2f}")
    sl_hit_ts = None
    for j in range(lock_idx + 1, len(bars_15m_day)):
        cj = bars_15m_day[j]
        if bias == "BULL" and cj.low <= watch_level:
            sl_hit_ts = cj.ts
            print(f"  HIT @ {cj.ts.strftime('%H:%M')}  L={cj.low:.2f} <= {watch_level:.2f}")
            break
        if bias == "BEAR" and cj.high >= watch_level:
            sl_hit_ts = cj.ts
            print(f"  HIT @ {cj.ts.strftime('%H:%M')}  H={cj.high:.2f} >= {watch_level:.2f}")
            break
    if sl_hit_ts is None:
        print("  never hit today.")
        return

    bars_5m_day = [b for b in bars_5m if b.ts.date() == TARGET_DATE and b.ts >= sl_hit_ts]
    print(f"\nSTAGE 3 — 5m REF-ROLLING CONFIRMATION (from {sl_hit_ts.strftime('%H:%M')}):")
    ref5_idx = 0
    confirm_ts = None
    sweep_extreme = bars_5m_day[0].low if bias == "BULL" else bars_5m_day[0].high
    for k in range(1, len(bars_5m_day)):
        ref5 = bars_5m_day[ref5_idx]
        cur5 = bars_5m_day[k]
        sweep_extreme = (min(sweep_extreme, cur5.low) if bias == "BULL" else max(sweep_extreme, cur5.high))
        broke_high = cur5.high > ref5.high
        broke_low = cur5.low < ref5.low
        print(f"  5m@{cur5.ts.strftime('%H:%M')} H={cur5.high:.2f} L={cur5.low:.2f} vs "
              f"5m-ref@{ref5.ts.strftime('%H:%M')} H={ref5.high:.2f} L={ref5.low:.2f}"
              f"  broke_high={broke_high} broke_low={broke_low}"
              f"  running_sweep_extreme={sweep_extreme:.2f}")
        if bias == "BULL" and broke_high and not broke_low:
            confirm_ts = cur5.ts
            print(f"  -> CONFIRMED @ {confirm_ts.strftime('%H:%M')} (5m ref's own HIGH breached)")
            break
        if bias == "BEAR" and broke_low and not broke_high:
            confirm_ts = cur5.ts
            print(f"  -> CONFIRMED @ {confirm_ts.strftime('%H:%M')} (5m ref's own LOW breached)")
            break
        ref5_idx = k
    if confirm_ts is None:
        print("  never confirmed today.")
        return
    sweep_sl = sweep_extreme
    print(f"  SL (deepest 5m sweep point) = {sweep_sl:.2f}")

    bars_1m_after = [b for b in bars_1m_day if b.ts >= confirm_ts]
    print(f"\nSTAGE 4 — 1m CHoCH (from {confirm_ts.strftime('%H:%M')}):")
    sw = swing_points(bars_1m_after, pivot=2)
    highs = sorted((s for s in sw if s[1] == "HIGH"), key=lambda s: s[0])
    lows = sorted((s for s in sw if s[1] == "LOW"), key=lambda s: s[0])
    entry = None
    if bias == "BULL":
        hi_ptr, active_high = 0, None
        for i, bar in enumerate(bars_1m_after):
            while hi_ptr < len(highs) and highs[hi_ptr][0] + 2 <= i:
                active_high = highs[hi_ptr]
                hi_ptr += 1
            if active_high is not None and bar.close > active_high[2]:
                entry = (bar.ts, bar.close)
                print(f"  CHoCH @ {bar.ts.strftime('%H:%M')}  close={bar.close:.2f} > "
                      f"active swing high {active_high[2]:.2f} (formed @ 1m idx {active_high[0]})")
                break
    else:
        lo_ptr, active_low = 0, None
        for i, bar in enumerate(bars_1m_after):
            while lo_ptr < len(lows) and lows[lo_ptr][0] + 2 <= i:
                active_low = lows[lo_ptr]
                lo_ptr += 1
            if active_low is not None and bar.close < active_low[2]:
                entry = (bar.ts, bar.close)
                print(f"  CHoCH @ {bar.ts.strftime('%H:%M')}  close={bar.close:.2f} < "
                      f"active swing low {active_low[2]:.2f} (formed @ 1m idx {active_low[0]})")
                break
    if entry is None:
        print("  no CHoCH found today.")
        return
    entry_ts, entry_price = entry

    risk = abs(entry_price - sweep_sl)
    target = entry_price + 2 * risk if bias == "BULL" else entry_price - 2 * risk
    print(f"\nSTAGE 5 — ENTRY + 1:2 RR:")
    print(f"  ENTRY (2 lots) @ {entry_ts.strftime('%H:%M')} price={entry_price:.2f}")
    print(f"  SL={sweep_sl:.2f}  risk={risk:.2f}pts  TARGET(1:2)={target:.2f}")

    print(f"\nSTAGE 6 — BEAR/BULL-TRAP SCALE-IN ZONE SEARCH (1m, from entry):")
    bars_after_entry = [b for b in bars_1m_day if b.ts > entry_ts]
    zone_bars = []
    for b in bars_after_entry:
        zone_bars.append(b)
        if len(zone_bars) < 3:
            continue
        zones = find_all_bear_zones(zone_bars) if bias == "BULL" else find_all_bull_zones(zone_bars)
        if not zones:
            continue
        zone = max(zones, key=lambda z: z.lock_ts)
        lo, hi = sorted((zone.entry_line, zone.sweep_low))
        size = hi - lo
        add_on_level = lo + size / 3.0 if bias == "BULL" else hi - size / 3.0
        hit = (b.low <= add_on_level) if bias == "BULL" else (b.high >= add_on_level)
        print(f"  1m@{b.ts.strftime('%H:%M')} zone=[{lo:.2f},{hi:.2f}] (locked {zone.lock_ts.strftime('%H:%M')}) "
              f"add_on_level={add_on_level:.2f} bar_L={b.low:.2f} bar_H={b.high:.2f} hit={hit}")
        if hit:
            print(f"  -> ADD-ON (2 more lots, 4 total) @ {b.ts.strftime('%H:%M')} price={add_on_level:.2f}")
            break

if __name__ == "__main__":
    asyncio.run(main())
