"""
scripts/liquidity_trap_backtest.py — one-off backtest for the "Liquidity Trap"
concept (2026-08-20, user spec, real SENSEX spot data via Upstox).

Mechanic (as confirmed with the user against real TradingView charts):
  1. 15m REF CANDLE: candle 1 of the day is the initial ref. Compare each
     next candle to the current ref candle:
       - breaches ONLY the ref's high  -> bullish bias LOCKED, ref stays same
       - breaches ONLY the ref's low   -> bearish bias LOCKED, ref stays same
       - breaches BOTH high and low    -> this candle becomes the new ref
       - breaches NEITHER (inside bar) -> this candle becomes the new ref
     Keep rolling until a clean one-sided breach locks the bias.
  2. Once locked, watch for the LOCKED ref candle's own OPPOSITE level to be
     hit on a later 15m candle (its low for bullish, its high for bearish) --
     this is the "SL hit" / liquidity-triggered moment.
  3. From that point, drop to 5m and run the SAME ref-candle-rolling rule
     starting fresh (5m candle at/after the SL-hit bar is the initial 5m ref),
     until a 5m candle breaches ONLY the opposite side (confirming the sweep
     direction) -- i.e. "5 min candle breached its own ref candle high"
     (bull) / low (bear). This is liquidity CONFIRMED.
  4. Drop to 1m and watch for a CHoCH: a 1m close breaking the most recent
     confirmed opposing 1m swing point (2-bar fractal pivot). Entry fires
     immediately at CHoCH confirmation (user confirmed: no retest wait).
  5. SL = the deepest wick reached during the 5m sweep (stage 3), i.e. the
     lowest 5m low between the 15m SL-hit bar and the 5m confirmation bar
     (bull) / highest 5m high (bear).
  6. Target = the nearest already-confirmed 15m swing-high pivot above entry
     (bull) / swing-low pivot below entry (bear) that existed BEFORE entry
     time (no lookahead) -- "next liquidity sweep of sellers/buyers in 15m".

Everything here is intentionally a fresh, self-contained implementation for
this one backtest script -- not wired into the live app, no new strategy
class, per the user's own "check the concept first" request.
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import List, Optional

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer.historical_candles import fetch_upstox_range_1m
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones

TOKEN = sys.argv[1] if len(sys.argv) > 1 else ""
INSTRUMENT_KEY = "BSE_INDEX|SENSEX"


@dataclass
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float

    @property
    def timestamp(self) -> datetime:
        """find_all_bear_zones/find_all_bull_zones (strategies/v4_cascade/
        rolling_base.py) are duck-typed against .timestamp, not .ts."""
        return self.ts


def resample(bars_1m: List[Bar], tf_min: int) -> List[Bar]:
    """Bucket 1m bars into tf_min bars, calendar-aligned (minute // tf_min),
    per real trading day (never merges across a day boundary)."""
    out: List[Bar] = []
    cur_key = None
    cur: Optional[Bar] = None
    for b in bars_1m:
        bucket_min = (b.ts.minute // tf_min) * tf_min
        key = (b.ts.date(), b.ts.hour, bucket_min)
        if key != cur_key:
            if cur is not None:
                out.append(cur)
            cur_key = key
            cur = Bar(ts=b.ts.replace(minute=bucket_min, second=0, microsecond=0),
                      open=b.open, high=b.high, low=b.low, close=b.close)
        else:
            cur.high = max(cur.high, b.high)
            cur.low = min(cur.low, b.low)
            cur.close = b.close
    if cur is not None:
        out.append(cur)
    return out


def by_day(bars: List[Bar]) -> dict:
    days: dict = {}
    for b in bars:
        days.setdefault(b.ts.date(), []).append(b)
    return days


def swing_points(bars: List[Bar], pivot: int = 2):
    """(index, kind, price) for confirmed fractal swing highs/lows."""
    out = []
    n = len(bars)
    for i in range(pivot, n - pivot):
        bar = bars[i]
        before = bars[i - pivot:i]
        after = bars[i + 1:i + 1 + pivot]
        if all(bar.high > b.high for b in before) and all(bar.high > b.high for b in after):
            out.append((i, "HIGH", bar.high))
        if all(bar.low < b.low for b in before) and all(bar.low < b.low for b in after):
            out.append((i, "LOW", bar.low))
    return out


@dataclass
class Trade:
    date: date
    direction: str
    entry_ts: datetime
    entry_price: float
    sl: float
    target: float
    exit_ts: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: str = ""
    pnl_pts: float = 0.0
    # Scale-in (2026-08-20 user spec): enter with 2 lots (half), track a fresh
    # bear-trap/bull-trap 3-candle zone (ref/sweep/reclaim, the SAME real
    # find_all_bear_zones/find_all_bull_zones used by D1TrapBearOnlyBook) on
    # the 1m chart from entry onward; add the other 2 lots (full = 4 lots) the
    # first time price trades back into the lowest third (bull) / highest
    # third (bear) of that zone. SL/target stay fixed at the original entry's
    # levels regardless of the add-on (confirmed by user).
    add_on_ts: Optional[datetime] = None
    add_on_price: Optional[float] = None
    lots_final: int = 2   # 2 (never added) or 4 (added)
    pnl_lotpts: float = 0.0   # points * lots, i.e. the real scaled P&L


def find_ref_and_bias(bars_15m_day: List[Bar]):
    """Returns (bias, ref_idx, lock_idx) or None if no bias locked today.
    bias: 'BULL'|'BEAR'. ref_idx: index (within the day's 15m list) of the
    LOCKED ref candle. lock_idx: index of the candle that did the locking
    breach (== the first candle checked against ref_idx that broke one side
    only)."""
    if len(bars_15m_day) < 2:
        return None
    ref_idx = 0
    for i in range(1, len(bars_15m_day)):
        ref = bars_15m_day[ref_idx]
        cur = bars_15m_day[i]
        broke_high = cur.high > ref.high
        broke_low = cur.low < ref.low
        if broke_high and not broke_low:
            return ("BULL", ref_idx, i)
        if broke_low and not broke_high:
            return ("BEAR", ref_idx, i)
        # both or neither -> this candle becomes the new ref
        ref_idx = i
    return None


def run_day(day: date, bars_1m_day: List[Bar], bars_15m_all: List[Bar],
            bars_5m_all: List[Bar], swings_15m_all: List, day15_start_idx: int,
            target_mode: str = "liquidity") -> Optional[Trade]:
    bars_15m_day = [b for b in bars_15m_all if b.ts.date() == day]
    if len(bars_15m_day) < 2:
        return None

    res = find_ref_and_bias(bars_15m_day)
    if res is None:
        return None
    bias, ref_idx, lock_idx = res
    ref_candle = bars_15m_day[ref_idx]

    # Stage 2: watch subsequent 15m candles (from lock_idx+1) for the ref
    # candle's OWN opposite level to be hit.
    sl_hit_ts = None
    watch_level = ref_candle.low if bias == "BULL" else ref_candle.high
    for j in range(lock_idx + 1, len(bars_15m_day)):
        cj = bars_15m_day[j]
        if bias == "BULL" and cj.low <= watch_level:
            sl_hit_ts = cj.ts
            break
        if bias == "BEAR" and cj.high >= watch_level:
            sl_hit_ts = cj.ts
            break
    if sl_hit_ts is None:
        return None

    # Stage 3 (2026-08-21 simplified, user spec -- "no need to check same
    # [ref-rolling] thing as 15m, if 5m high is breached it means triggered"):
    # the FIRST 5m bar at/after sl_hit_ts is a single FIXED reference (no
    # rolling) -- the first LATER 5m candle whose high (bull) / low (bear)
    # breaks that one reference confirms, straight to Stage 4.
    bars_5m_day = [b for b in bars_5m_all if b.ts.date() == day and b.ts >= sl_hit_ts]
    if len(bars_5m_day) < 2:
        return None
    ref5 = bars_5m_day[0]
    confirm_ts = None
    sweep_extreme = ref5.low if bias == "BULL" else ref5.high
    for k in range(1, len(bars_5m_day)):
        cur5 = bars_5m_day[k]
        sweep_extreme = (min(sweep_extreme, cur5.low) if bias == "BULL"
                         else max(sweep_extreme, cur5.high))
        if bias == "BULL" and cur5.high > ref5.high:
            confirm_ts = cur5.ts
            break
        if bias == "BEAR" and cur5.low < ref5.low:
            confirm_ts = cur5.ts
            break
    if confirm_ts is None:
        return None
    sweep_sl = sweep_extreme

    # Stage 4: 1m CHoCH from confirm_ts onward.
    bars_1m_after = [b for b in bars_1m_day if b.ts >= confirm_ts]
    if len(bars_1m_after) < 6:
        return None
    sw = swing_points(bars_1m_after, pivot=2)
    highs = sorted((s for s in sw if s[1] == "HIGH"), key=lambda s: s[0])
    lows = sorted((s for s in sw if s[1] == "LOW"), key=lambda s: s[0])
    entry = None
    if bias == "BULL":
        # CHoCH = close breaks the most recent confirmed swing HIGH so far.
        hi_ptr = 0
        active_high = None
        for i, bar in enumerate(bars_1m_after):
            while hi_ptr < len(highs) and highs[hi_ptr][0] + 2 <= i:
                active_high = highs[hi_ptr]
                hi_ptr += 1
            if active_high is not None and bar.close > active_high[2]:
                entry = (bar.ts, bar.close)
                break
    else:
        lo_ptr = 0
        active_low = None
        for i, bar in enumerate(bars_1m_after):
            while lo_ptr < len(lows) and lows[lo_ptr][0] + 2 <= i:
                active_low = lows[lo_ptr]
                lo_ptr += 1
            if active_low is not None and bar.close < active_low[2]:
                entry = (bar.ts, bar.close)
                break
    if entry is None:
        return None
    entry_ts, entry_price = entry

    # Target: two modes, compared per the user's request --
    #   "liquidity": nearest already-confirmed 15m swing pivot (before entry_ts)
    #                in the trade's favor (no lookahead).
    #   "rr2":       fixed 1:2 risk-reward off the SAME SL (risk = |entry-sl|).
    if target_mode == "rr2":
        risk = abs(entry_price - sweep_sl)
        if risk <= 0:
            return None
        target = entry_price + 2 * risk if bias == "BULL" else entry_price - 2 * risk
    else:
        prior_swings = [s for s in swings_15m_all if bars_15m_all[s[0]].ts < entry_ts]
        target = None
        if bias == "BULL":
            cands = [s[2] for s in prior_swings if s[1] == "HIGH" and s[2] > entry_price]
            if cands:
                target = min(cands)
        else:
            cands = [s[2] for s in prior_swings if s[1] == "LOW" and s[2] < entry_price]
            if cands:
                target = max(cands)
        if target is None:
            return None

    return Trade(date=day, direction=bias, entry_ts=entry_ts, entry_price=entry_price,
                 sl=sweep_sl, target=target)


def _check_scale_in(trade: Trade, zone_bars: List[Bar], cur: Bar) -> bool:
    """Fresh bear-trap (BULL trade) / bull-trap (BEAR trade) 3-candle zone
    search (ref/sweep/reclaim -- the real find_all_bear_zones/find_all_bull_
    zones from strategies/v4_cascade/rolling_base.py) on the 1m bars since
    entry. Takes the FRESHEST confirmed zone (max lock_ts). Add-on level =
    1/3 above the zone low (BULL) / 1/3 below the zone high (BEAR) -- user
    spec. Returns True the first time `cur` trades into that level."""
    zones = (find_all_bear_zones(zone_bars) if trade.direction == "BULL"
             else find_all_bull_zones(zone_bars))
    if not zones:
        return False
    zone = max(zones, key=lambda z: z.lock_ts)
    lo, hi = sorted((zone.entry_line, zone.sweep_low))
    size = hi - lo
    if size <= 0:
        return False
    if trade.direction == "BULL":
        add_on_level = lo + size / 3.0
        if cur.low <= add_on_level:
            trade.add_on_ts, trade.add_on_price = cur.ts, add_on_level
            return True
    else:
        add_on_level = hi - size / 3.0
        if cur.high >= add_on_level:
            trade.add_on_ts, trade.add_on_price = cur.ts, add_on_level
            return True
    return False


def simulate_exit(trade: Trade, bars_1m_all: List[Bar]) -> None:
    """Walk 1m bars forward from entry, intrabar low/high vs SL/target,
    EOD close at 15:15 if neither hit. Also tracks the scale-in (2 lots at
    entry -> 4 lots on the zone add-on trigger) in the same pass, since both
    need the same bar-by-bar walk."""
    after = [b for b in bars_1m_all if b.ts > trade.entry_ts and b.ts.date() == trade.date]
    zone_bars: List[Bar] = []
    added = False
    for b in after:
        if not added:
            zone_bars.append(b)
            if len(zone_bars) >= 3 and _check_scale_in(trade, zone_bars, b):
                added = True
                trade.lots_final = 4
        if b.ts.time() >= datetime(2000, 1, 1, 15, 15).time():
            trade.exit_ts, trade.exit_price, trade.exit_reason = b.ts, b.close, "EOD"
            break
        if trade.direction == "BULL":
            if b.low <= trade.sl:
                trade.exit_ts, trade.exit_price, trade.exit_reason = b.ts, trade.sl, "SL"
                break
            if b.high >= trade.target:
                trade.exit_ts, trade.exit_price, trade.exit_reason = b.ts, trade.target, "TARGET"
                break
        else:
            if b.high >= trade.sl:
                trade.exit_ts, trade.exit_price, trade.exit_reason = b.ts, trade.sl, "SL"
                break
            if b.low <= trade.target:
                trade.exit_ts, trade.exit_price, trade.exit_reason = b.ts, trade.target, "TARGET"
                break
    if trade.exit_ts is None and after:
        last = after[-1]
        trade.exit_ts, trade.exit_price, trade.exit_reason = last.ts, last.close, "EOD(data-end)"
    if trade.exit_price is not None:
        trade.pnl_pts = ((trade.exit_price - trade.entry_price) if trade.direction == "BULL"
                         else (trade.entry_price - trade.exit_price))
        # 2 lots always at entry_price; +2 more at add_on_price if triggered
        # (SL/target -- and therefore exit_price -- stay fixed at the
        # original entry's levels regardless, per user spec).
        per_pt_first_2 = trade.pnl_pts
        if trade.add_on_price is not None:
            per_pt_add_2 = ((trade.exit_price - trade.add_on_price) if trade.direction == "BULL"
                            else (trade.add_on_price - trade.exit_price))
            trade.pnl_lotpts = 2 * per_pt_first_2 + 2 * per_pt_add_2
        else:
            trade.pnl_lotpts = 2 * per_pt_first_2


CACHE_PATH = "scratch_liquidity_trap_1m_cache.json"


async def load_1m_bars() -> List[Bar]:
    import json, os
    if os.path.exists(CACHE_PATH):
        print(f"Loading cached 1-min bars from {CACHE_PATH} ...", flush=True)
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            rows = json.load(f)
        print(f"Loaded {len(rows)} cached 1-min candles.", flush=True)
    else:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=365)
        print(f"Fetching 1-min SENSEX spot ({INSTRUMENT_KEY}) from {start} to {end} ...", flush=True)
        rows = await fetch_upstox_range_1m(INSTRUMENT_KEY, TOKEN, start, end)
        print(f"Fetched {len(rows)} 1-min candles.", flush=True)
        if rows:
            with open(CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(rows, f)
            print(f"Cached to {CACHE_PATH} for reuse.", flush=True)
    if not rows:
        return []
    bars_1m = [Bar(ts=datetime.fromisoformat(r["ts"]), open=r["open"], high=r["high"],
                   low=r["low"], close=r["close"]) for r in rows]
    bars_1m.sort(key=lambda b: b.ts)
    return bars_1m


async def main():
    target_mode = sys.argv[2] if len(sys.argv) > 2 else "liquidity"
    bars_1m = await load_1m_bars()
    if not bars_1m:
        print("NO DATA RETURNED -- aborting.")
        return
    first_day, last_day = bars_1m[0].ts.date(), bars_1m[-1].ts.date()
    print(f"Real data range covers {first_day} .. {last_day} "
          f"({len(set(b.ts.date() for b in bars_1m))} trading days). target_mode={target_mode}", flush=True)

    bars_15m = resample(bars_1m, 15)
    bars_5m = resample(bars_1m, 5)
    swings_15m = swing_points(bars_15m, pivot=2)

    days_1m = by_day(bars_1m)
    trades: List[Trade] = []
    for day in sorted(days_1m.keys()):
        t = run_day(day, days_1m[day], bars_15m, bars_5m, swings_15m, 0, target_mode=target_mode)
        if t is not None:
            simulate_exit(t, bars_1m)
            trades.append(t)

    print(f"\n{len(trades)} trades found across {len(days_1m)} trading days.\n")
    header = (f"{'DATE':10} {'DIR':4} {'ENTRY TIME':16} {'ENTRY':>9} {'SL':>9} {'TARGET':>9} "
              f"{'ADD-ON TIME':16} {'ADD@':>9} {'LOTS':>5} {'EXIT TIME':16} {'EXIT':>9} {'REASON':10} "
              f"{'PNL(pts)':>9} {'PNL(lotpts)':>11}")
    print(header)
    print("-" * len(header))
    total_pnl = 0.0
    total_lotpnl = 0.0
    wins = 0
    added_count = 0
    for t in trades:
        total_pnl += t.pnl_pts
        total_lotpnl += t.pnl_lotpts
        if t.pnl_pts > 0:
            wins += 1
        if t.add_on_price is not None:
            added_count += 1
        print(f"{str(t.date):10} {t.direction:4} {t.entry_ts.strftime('%Y-%m-%d %H:%M'):16} "
              f"{t.entry_price:9.2f} {t.sl:9.2f} {t.target:9.2f} "
              f"{(t.add_on_ts.strftime('%Y-%m-%d %H:%M') if t.add_on_ts else '-'):16} "
              f"{(t.add_on_price if t.add_on_price is not None else 0):9.2f} {t.lots_final:5d} "
              f"{(t.exit_ts.strftime('%Y-%m-%d %H:%M') if t.exit_ts else '-'):16} "
              f"{(t.exit_price if t.exit_price is not None else 0):9.2f} {t.exit_reason:10} "
              f"{t.pnl_pts:9.2f} {t.pnl_lotpts:11.2f}")
    n = len(trades)
    win_pct = (wins / n * 100.0) if n else 0.0
    gross_win = sum(t.pnl_pts for t in trades if t.pnl_pts > 0)
    gross_loss = -sum(t.pnl_pts for t in trades if t.pnl_pts < 0)
    pf = (gross_win / gross_loss) if gross_loss > 0 else float("inf")
    gross_win_lot = sum(t.pnl_lotpts for t in trades if t.pnl_lotpts > 0)
    gross_loss_lot = -sum(t.pnl_lotpts for t in trades if t.pnl_lotpts < 0)
    pf_lot = (gross_win_lot / gross_loss_lot) if gross_loss_lot > 0 else float("inf")
    print("-" * len(header))
    print(f"TOTAL trades={n} win%={win_pct:.1f} scaled_in={added_count}/{n} "
          f"net_pts(per-2lot-unit)={total_pnl:.2f} PF(per-2lot-unit)={pf:.2f} | "
          f"net_lotpts(real, lot=20)={total_lotpnl:.2f} PF(lot-weighted)={pf_lot:.2f} "
          f"net_rupees(lotsize20)={total_lotpnl * 20:.2f}")


if __name__ == "__main__":
    asyncio.run(main())
