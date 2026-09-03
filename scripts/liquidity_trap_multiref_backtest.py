"""
scripts/liquidity_trap_multiref_backtest.py -- "multi-ref" variant of the
Liquidity Trap concept (2026-08-21, user spec), backtested against the SAME
real SENSEX spot data as scripts/liquidity_trap_backtest.py for a direct,
apples-to-apples comparison against the single-lock baseline.

Mechanic difference from the single-lock baseline (confirmed with the user
via a worked example): the baseline's find_ref_and_bias() locks ONE bias for
the whole day, rolling the ref candle forward on every ambiguous (both/
neither breached) candle until a clean one-sided breach locks it -- after
that, no other candle can ever start a new setup that day.

Multi-ref instead treats EVERY consecutive 15m candle pair independently:
  - candle[i] vs candle[i-1]: if candle[i] breaches ONLY candle[i-1]'s high
    -> a new BULL setup locks immediately, ref = candle[i-1].
  - if candle[i] breaches ONLY candle[i-1]'s low -> a new BEAR setup locks
    immediately, ref = candle[i-1].
  - (both breached or neither -> no setup spawns from this pair; this is NOT
    the same as the baseline's "roll the ref forward" -- there is no single
    rolling ref anymore, just independent pairwise checks.)
Any number of setups can be locked, in either direction, across the day.
Each locked setup independently watches for its OWN Stage 2 (a later candle
breaching its own ref's opposite level -- SAME find_sl_hit rule as the
baseline, just evaluated per-setup instead of once globally). Once a given
setup's SL-hit fires, it proceeds to Stage 3 (5m single-fixed-reference
confirm) -> Stage 4 (1m CHoCH entry), using the EXACT SAME stage3/4 logic as
the baseline (extracted into _stage3_and_4() below, not reimplemented).

Explicitly confirmed by the user: setups run FULLY IN PARALLEL, not
mutually exclusive -- multiple setups can independently reach SL-hit and
fire their own real trades the same day, even simultaneously/overlapping.
No "one trade per day" cap here; every setup that completes the full
Stage1-4 pipeline produces its own independent Trade, simulated exactly like
any other (own SL/target/EOD walk, own scale-in check) -- exactly as if a
trader were tracking several parallel structures and taking every one that
confirms.

Self-contained per this codebase's own backtest-script convention (each
sweep/backtest script is independent, not imported into another -- same
isolation reasoning already applied to every strategy module). Reuses the
SAME cached 1-min data file as the baseline script
(scratch_liquidity_trap_1m_cache.json) so no re-fetch is needed if that
cache already exists.
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
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
        return self.ts


def resample(bars_1m: List[Bar], tf_min: int) -> List[Bar]:
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
    ref_ts: datetime          # multi-ref only: which candle was this trade's own ref (for audit)
    entry_ts: datetime
    entry_price: float
    sl: float
    target: float
    exit_ts: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: str = ""
    pnl_pts: float = 0.0
    add_on_ts: Optional[datetime] = None
    add_on_price: Optional[float] = None
    lots_final: int = 2
    pnl_lotpts: float = 0.0


@dataclass
class Setup:
    ref_idx: int
    direction: str
    locked_idx: int
    sl_hit_ts: Optional[datetime] = None


def find_all_setups(bars_15m_day: List[Bar]) -> List[Setup]:
    """Every consecutive pair (candle[i-1] as ref, candle[i] as the breach
    check) independently spawns its own setup on a clean one-sided breach.
    Not a rolling single ref -- every pair is checked, regardless of what
    any other pair produced."""
    setups: List[Setup] = []
    for i in range(1, len(bars_15m_day)):
        ref = bars_15m_day[i - 1]
        cur = bars_15m_day[i]
        broke_high = cur.high > ref.high
        broke_low = cur.low < ref.low
        if broke_high and not broke_low:
            setups.append(Setup(ref_idx=i - 1, direction="BULL", locked_idx=i))
        elif broke_low and not broke_high:
            setups.append(Setup(ref_idx=i - 1, direction="BEAR", locked_idx=i))
    return setups


def find_sl_hit_for_setup(setup: Setup, bars_15m_day: List[Bar]) -> Optional[datetime]:
    ref = bars_15m_day[setup.ref_idx]
    watch_level = ref.low if setup.direction == "BULL" else ref.high
    for j in range(setup.locked_idx + 1, len(bars_15m_day)):
        cj = bars_15m_day[j]
        if setup.direction == "BULL" and cj.low <= watch_level:
            return cj.ts
        if setup.direction == "BEAR" and cj.high >= watch_level:
            return cj.ts
    return None


def _stage3_and_4(day: date, bias: str, ref_ts: datetime, sl_hit_ts: datetime,
                   bars_1m_day: List[Bar], bars_5m_all: List[Bar], bars_15m_all: List[Bar],
                   swings_15m_all: List, target_mode: str) -> Optional[Trade]:
    """Stage 3 (5m single-fixed-reference confirm) + Stage 4 (1m CHoCH entry)
    -- byte-identical logic to scripts/liquidity_trap_backtest.py's run_day(),
    extracted so both the single-lock baseline and this multi-ref variant
    exercise the exact same validated stage3/4 mechanic, only Stage 1/2
    (how a setup gets locked and SL-hit) differs."""
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

    bars_1m_after = [b for b in bars_1m_day if b.ts >= confirm_ts]
    if len(bars_1m_after) < 6:
        return None
    sw = swing_points(bars_1m_after, pivot=2)
    highs = sorted((s for s in sw if s[1] == "HIGH"), key=lambda s: s[0])
    lows = sorted((s for s in sw if s[1] == "LOW"), key=lambda s: s[0])
    entry = None
    if bias == "BULL":
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

    return Trade(date=day, direction=bias, ref_ts=ref_ts, entry_ts=entry_ts,
                 entry_price=entry_price, sl=sweep_sl, target=target)


def _simulate_exit_with_flip(trade: Trade, day_1m: List[Bar], next_opp: Optional[Trade]) -> None:
    """Like simulate_exit(), plus a THIRD exit trigger (user spec, 2026-08-21):
    if the opposite direction's own next setup fires its entry while this
    position is still open, that CLOSES this position immediately (reason
    "FLIP", at the opposite setup's own entry_ts/entry_price) -- checked
    ahead of SL/target on every bar, so a flip on the exact same bar as an
    SL/target touch still counts as a flip (an opposing structural signal is
    a stronger reason to exit than the raw SL/target level, per spec: "long
    trade running and short trade comes, long trade will be closed"). A
    same-direction later setup is simply never passed in as `next_opp` (the
    caller only ever looks for the first OPPOSITE-direction candidate), so
    it can never trigger an exit here -- already-same-direction signals stay
    silently ignored while this position runs, per spec."""
    after = [b for b in day_1m if b.ts > trade.entry_ts]
    zone_bars: List[Bar] = []
    added = False
    for b in after:
        if next_opp is not None and b.ts >= next_opp.entry_ts:
            trade.exit_ts, trade.exit_price, trade.exit_reason = next_opp.entry_ts, next_opp.entry_price, "FLIP"
            break
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
    if trade.exit_ts is None:
        if after:
            last = after[-1]
            trade.exit_ts, trade.exit_price, trade.exit_reason = last.ts, last.close, "EOD(data-end)"
        else:
            # Entered on the very last available 1m bar of the day -- no future
            # bars to walk at all. Exit immediately at entry (zero P&L) rather
            # than leave exit_ts as None, which the caller's sequencing compare
            # (candidates[idx].entry_ts <= cur.exit_ts) can't handle.
            trade.exit_ts, trade.exit_price, trade.exit_reason = trade.entry_ts, trade.entry_price, "EOD(no-data)"
    if trade.exit_price is not None:
        trade.pnl_pts = ((trade.exit_price - trade.entry_price) if trade.direction == "BULL"
                         else (trade.entry_price - trade.exit_price))
        per_pt_first_2 = trade.pnl_pts
        if trade.add_on_price is not None:
            per_pt_add_2 = ((trade.exit_price - trade.add_on_price) if trade.direction == "BULL"
                            else (trade.add_on_price - trade.exit_price))
            trade.pnl_lotpts = 2 * per_pt_first_2 + 2 * per_pt_add_2
        else:
            trade.pnl_lotpts = 2 * per_pt_first_2


def run_day_multiref(day: date, bars_1m_day: List[Bar], bars_1m_all: List[Bar],
                      bars_15m_all: List[Bar], bars_5m_all: List[Bar],
                      swings_15m_all: List, target_mode: str = "liquidity") -> List[Trade]:
    """Only ONE position open at a time (user spec, 2026-08-21). Setups are
    tracked/watched fully in parallel (Stage 1-3) regardless of position
    state. Once a position is open, THREE things can end it: SL, target, or
    an OPPOSITE-direction setup firing its own entry ("if long trade running
    and short trade comes, long trade will be closed" -- a flip/reversal, not
    a queued/missed signal). A SAME-direction setup firing while already in
    that direction is simply ignored (never even considered as a trigger).
    After any exit, the next candidate (any direction) whose own entry_ts is
    still in the future relative to that exit becomes the new open position;
    anything that would have fired WHILE the just-closed position was open is
    a genuinely missed opportunity (its trigger moment already passed), same
    principle as the earlier one-at-a-time version."""
    bars_15m_day = [b for b in bars_15m_all if b.ts.date() == day]
    if len(bars_15m_day) < 2:
        return []
    setups = find_all_setups(bars_15m_day)
    candidates: List[Trade] = []
    for s in setups:
        s.sl_hit_ts = find_sl_hit_for_setup(s, bars_15m_day)
        if s.sl_hit_ts is None:
            continue
        ref_ts = bars_15m_day[s.ref_idx].ts
        t = _stage3_and_4(day, s.direction, ref_ts, s.sl_hit_ts, bars_1m_day,
                           bars_5m_all, bars_15m_all, swings_15m_all, target_mode)
        if t is not None:
            candidates.append(t)
    candidates.sort(key=lambda t: t.entry_ts)

    day_1m = [b for b in bars_1m_all if b.ts.date() == day]
    accepted: List[Trade] = []
    idx = 0
    while idx < len(candidates):
        cur = candidates[idx]
        next_opp = None
        next_opp_pos = None
        for j in range(idx + 1, len(candidates)):
            if candidates[j].direction != cur.direction:
                next_opp = candidates[j]
                next_opp_pos = j
                break
        _simulate_exit_with_flip(cur, day_1m, next_opp)
        accepted.append(cur)
        if cur.exit_reason == "FLIP":
            idx = next_opp_pos   # the flip candidate IS the new open position, no re-check needed
        else:
            idx += 1
            while idx < len(candidates) and candidates[idx].entry_ts <= cur.exit_ts:
                idx += 1   # missed -- its own trigger moment fell while cur was still open
    return accepted


def _check_scale_in(trade: Trade, zone_bars: List[Bar], cur: Bar) -> bool:
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
          f"({len(set(b.ts.date() for b in bars_1m))} trading days). "
          f"MULTI-REF variant. target_mode={target_mode}", flush=True)

    bars_15m = resample(bars_1m, 15)
    bars_5m = resample(bars_1m, 5)
    swings_15m = swing_points(bars_15m, pivot=2)

    days_1m = by_day(bars_1m)
    trades: List[Trade] = []
    per_day_counts: dict = {}
    for day in sorted(days_1m.keys()):
        day_trades = run_day_multiref(day, days_1m[day], bars_1m, bars_15m, bars_5m,
                                       swings_15m, target_mode=target_mode)
        trades.extend(day_trades)
        if day_trades:
            per_day_counts[day] = len(day_trades)

    print(f"\n{len(trades)} trades found across {len(days_1m)} trading days "
          f"({len(per_day_counts)} days had >=1 trade, "
          f"max trades in a single day = {max(per_day_counts.values()) if per_day_counts else 0}).\n")
    header = (f"{'DATE':10} {'DIR':4} {'REF':16} {'ENTRY TIME':16} {'ENTRY':>9} {'SL':>9} {'TARGET':>9} "
              f"{'ADD-ON TIME':16} {'ADD@':>9} {'LOTS':>5} {'EXIT TIME':16} {'EXIT':>9} {'REASON':10} "
              f"{'PNL(pts)':>9} {'PNL(lotpts)':>11}")
    print(header)
    print("-" * len(header))
    total_pnl = 0.0
    total_lotpnl = 0.0
    wins = 0
    added_count = 0
    for t in sorted(trades, key=lambda t: t.entry_ts):
        total_pnl += t.pnl_pts
        total_lotpnl += t.pnl_lotpts
        if t.pnl_pts > 0:
            wins += 1
        if t.add_on_price is not None:
            added_count += 1
        print(f"{str(t.date):10} {t.direction:4} {t.ref_ts.strftime('%Y-%m-%d %H:%M'):16} "
              f"{t.entry_ts.strftime('%Y-%m-%d %H:%M'):16} "
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
