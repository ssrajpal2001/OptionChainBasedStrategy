"""backtest/v4_cascade/htf_ltf_backtest.py -- HTF(75m)-gated LTF(15m/5m)
cascade strategy, per the user's 2026-07-22 spec.

LONG (bearish trap):
  1. HTF zone (75m): a genuine 3-candle sweep+reclaim, same shape as
     production's find_bear_zone (ref, a SEPARATE later sweep candle, a
     STILL LATER separate reclaim candle -- never collapsed to 2 candles).
     Zone = [sweep_low, entry_line=ref.low]; T2's target = zone.sl_level
     (ref.high). Multiple candidate zones are tracked CONCURRENTLY in a
     pool per side (2026-07-22: confirmed live -- a real, valid zone
     matching a manually-verified chart was being missed entirely because
     an earlier-adopted single zone was still occupying the only slot).
     Each zone in the pool ages out of consideration after
     HTF_ZONE_MAX_AGE_DAYS (matches real option-contract liquidity: ~10-14
     days of usable history around the current point) -- removed from the
     pool for good, not merely superseded, whether it fired or not.
  2. Each pool zone independently waits for a later 75m candle to re-enter
     its own [zone_low, zone_high].
  3. Once a zone is re-entered, IT starts tracking 15m + 5m independently
     of every other zone in the pool.
  4. 15m nested zone: same 3-candle finder, fed 15m bars from THAT zone's
     own re-entry point onward. T1's target = ltf_zone.sl_level.
  5. 5m trigger: a candle closes above the immediately preceding 5m
     candle's high, checked independently per tracking zone.
  6. On trigger: limit entry at that zone's zone_low + offset, SL at
     zone_low - offset. Limit fills the first time a later 5m bar's low
     pierces down to it (persists across bars until filled or invalidated
     -- never phantom-fills against a stale price the market has long
     since left behind).
  7. The instant ANY zone in the pool fills, the whole pool is discarded --
     only one position per side at a time, matching the engine's existing
     single-CascadePosition constraint. T1 exits at ltf_zone.sl_level or
     the shared SL; T2 exits at htf_zone.sl_level or the shared SL, with
     the existing breakeven-then-trail ratchet once T1 hits.

SHORT (bullish trap) is the exact mirror, using the bull-side finder and
swapping every high/low comparison."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

from strategies.v4_cascade.book import _Bar, _bucket_key, _to_5m_bars
from strategies.v4_cascade.dataclasses import RollingBaseZone, TrancheLeg
from strategies.v4_cascade.exits import ExitCheck, TrailingBaseTracker, check_t1
from strategies.v4_cascade.rolling_base import (
    _is_mitigated_bear, _is_mitigated_bull, resample_bars,
)

SESSION_OPEN: Tuple[int, int] = (9, 15)
EOD_HOUR_MIN: Tuple[int, int] = (15, 15)
# 2026-07-22: real option contracts only have ~10-14 days of usable liquid
# history behind the current point (prev week + current week) -- the zone
# pool is bounded to match. A zone older than this ages out of the pool for
# good, whether it ever fired or not.
HTF_ZONE_MAX_AGE_DAYS = 10


def find_all_bear_zones(bars: List["_Bar"], known_ref_ts: Optional[Set[datetime]] = None) -> List[RollingBaseZone]:
    """Enumerate EVERY confirmed bear-side (sweep+reclaim) zone in ``bars``,
    not just the newest one -- the multi-zone-pool counterpart to
    rolling_base.find_bear_zone (which returns on the first newest-first
    match, by design, for the single-zone HTF use case elsewhere). Same
    3-candle rule (ref/sweep/reclaim strictly distinct candles) and the same
    mitigation check, reused directly from rolling_base.py. ``known_ref_ts``:
    ref timestamps already added to the pool (or already removed from it) --
    never re-considered, so a zone that ages out or breaks is gone for
    good, not rediscovered next bar."""
    known_ref_ts = known_ref_ts or set()
    n = len(bars)
    found: List[RollingBaseZone] = []
    for i in range(n - 2, -1, -1):
        ref = bars[i]
        if ref.timestamp in known_ref_ts:
            continue
        sellers_in_idx: Optional[int] = None
        for j in range(i + 1, n):
            if bars[j].low < ref.low:
                sellers_in_idx = j
                break
        if sellers_in_idx is None:
            continue
        trapped_idx: Optional[int] = None
        sweep_low = bars[sellers_in_idx].low
        sweep_started_ts = bars[sellers_in_idx].timestamp
        for k in range(sellers_in_idx + 1, n):
            sweep_low = min(sweep_low, bars[k].low)
            if bars[k].high > ref.high:
                trapped_idx = k
                break
        if trapped_idx is None:
            continue
        trapped_ts = bars[trapped_idx].timestamp
        entry_line = ref.low
        if _is_mitigated_bear(bars, entry_line, sweep_low, trapped_idx=trapped_idx):
            continue
        found.append(RollingBaseZone(
            reference_low=ref.low, reference_low_ts=ref.timestamp, prev_close=ref.close,
            swept=True, sweep_low=sweep_low, sweep_started_ts=sweep_started_ts,
            bars_since_sweep=trapped_idx - sellers_in_idx,
            locked=True, lock_ts=trapped_ts,
            entry_line=entry_line, sl_level=ref.high,
        ))
    return found


def find_all_bull_zones(bars: List["_Bar"], known_ref_ts: Optional[Set[datetime]] = None) -> List[RollingBaseZone]:
    """Symmetric to find_all_bear_zones -- buyers trapped (bearish read)."""
    known_ref_ts = known_ref_ts or set()
    n = len(bars)
    found: List[RollingBaseZone] = []
    for i in range(n - 2, -1, -1):
        ref = bars[i]
        if ref.timestamp in known_ref_ts:
            continue
        buyers_in_idx: Optional[int] = None
        for j in range(i + 1, n):
            if bars[j].high > ref.high:
                buyers_in_idx = j
                break
        if buyers_in_idx is None:
            continue
        trapped_idx: Optional[int] = None
        sweep_high = bars[buyers_in_idx].high
        sweep_started_ts = bars[buyers_in_idx].timestamp
        for k in range(buyers_in_idx + 1, n):
            sweep_high = max(sweep_high, bars[k].high)
            if bars[k].low < ref.low:
                trapped_idx = k
                break
        if trapped_idx is None:
            continue
        trapped_ts = bars[trapped_idx].timestamp
        entry_line = ref.high
        if _is_mitigated_bull(bars, entry_line, sweep_high, trapped_idx=trapped_idx):
            continue
        found.append(RollingBaseZone(
            reference_low=ref.high, reference_low_ts=ref.timestamp, prev_close=ref.close,
            swept=True, sweep_low=sweep_high, sweep_started_ts=sweep_started_ts,
            bars_since_sweep=trapped_idx - buyers_in_idx,
            locked=True, lock_ts=trapped_ts,
            entry_line=entry_line, sl_level=ref.low,
        ))
    return found


def _zone_bounds(z: RollingBaseZone) -> Tuple[float, float]:
    return min(z.entry_line, z.sweep_low), max(z.entry_line, z.sweep_low)


def _overlaps(bar_low: float, bar_high: float, lo: float, hi: float) -> bool:
    return bar_low <= hi and bar_high >= lo


class _ZoneSlot:
    """One candidate HTF zone's independent tracking state, living inside a
    side's pool. Everything that used to be single-slot state on _SideState
    (tracking/ltf_zone/bars_15m/prev_5m_bar/pending_entry/trigger_ts) now
    lives per-zone, since multiple zones progress concurrently."""

    def __init__(self, zone: RollingBaseZone) -> None:
        self.zone = zone
        self.zone_low, self.zone_high = _zone_bounds(zone)
        self.tracking = False
        self.reentry_ts: Optional[datetime] = None
        self.ltf_zone: Optional[RollingBaseZone] = None
        self.bars_15m: List["_Bar"] = []
        self.prev_5m_bar: Optional["_Bar"] = None
        self.pending_entry = False
        self.trigger_ts: Optional[datetime] = None


class _SideState:
    """Per-side (CE=long/bear-trap, PE=short/bull-trap) state: a POOL of
    concurrently-tracked candidate zones, plus the (at most one) open
    position for this side."""

    def __init__(self, bear: bool) -> None:
        self.bear = bear  # True=CE/long, False=PE/short
        self.pool: List[_ZoneSlot] = []
        self.known_ref_ts: Set[datetime] = set()
        self.t1: Optional[TrancheLeg] = None
        self.t2: Optional[TrancheLeg] = None
        self.trail: Optional[TrailingBaseTracker] = None

    def is_open(self) -> bool:
        return (self.t1 is not None and self.t1.status == "open") or \
               (self.t2 is not None and self.t2.status == "open")


def run_backtest(bars_5m: List["_Bar"], entry_offset: float = 10.0, qty: int = 130,
                  sl_buffer: float = 0.0) -> List[dict]:
    """entry_offset: the +-offset from a zone's own zone_low/high for both
    the limit entry and the SL (grid-search parameter). qty: combined
    T1+T2 quantity; T1/T2 each get qty//2. sl_buffer: unused placeholder --
    the spec's SL IS the offset itself, no separate buffer layered on top."""
    ce = _SideState(bear=True)
    pe = _SideState(bear=False)
    legs: List[dict] = []
    tranche_qty = qty // 2

    bars_75m_by_key = {_bucket_key(b.timestamp, 75, SESSION_OPEN): b
                        for b in resample_bars(bars_5m, 75, SESSION_OPEN)}
    bars_15m_by_key = {_bucket_key(b.timestamp, 15, SESSION_OPEN): b
                        for b in resample_bars(bars_5m, 15, SESSION_OPEN)}

    all_75m: List["_Bar"] = []

    def finalize(state: _SideState, tranche: str, leg: TrancheLeg, reason: str,
                 price: float, ts, slot: Optional[_ZoneSlot]) -> None:
        leg.status = "closed"
        leg.close_price = price
        leg.close_reason = reason
        leg.close_time = ts
        is_short = not state.bear
        pnl_points = (leg.entry_price - price) if is_short else (price - leg.entry_price)
        htf = slot.zone if slot else None
        ltf = slot.ltf_zone if slot else None
        htf_ref_high = htf.sl_level if state.bear else htf.entry_line if htf else None
        htf_ref_low = htf.entry_line if state.bear else htf.sl_level if htf else None
        ltf_ref_high = ltf.sl_level if state.bear else ltf.entry_line if ltf else None
        ltf_ref_low = ltf.entry_line if state.bear else ltf.sl_level if ltf else None
        legs.append({
            "side": "CE" if state.bear else "PE", "tranche": tranche,
            "htf_ref_ts": htf.reference_low_ts if htf else None,
            "htf_ref_high": htf_ref_high, "htf_ref_low": htf_ref_low,
            "htf_lock_ts": htf.lock_ts if htf else None,
            "reentry_ts": slot.reentry_ts if slot else None,
            "ltf_ref_ts": ltf.reference_low_ts if ltf else None,
            "ltf_ref_high": ltf_ref_high, "ltf_ref_low": ltf_ref_low,
            "trigger_ts": slot.trigger_ts if slot else None,
            "entry_ts": leg.entry_time, "entry_price": leg.entry_price,
            "sl_price": leg.sl_price, "target_price": leg.target_price,
            "close_ts": ts, "close_price": price, "close_reason": reason,
            "qty": leg.qty, "pnl_points": pnl_points,
        })

    # the zone slot each open leg was filled from -- needed by finalize()'s
    # audit trail once the pool has already moved on/been cleared.
    open_slot: Dict[str, Optional[_ZoneSlot]] = {"CE": None, "PE": None}

    def check_exits(state: _SideState, bar: "_Bar") -> None:
        is_short = not state.bear
        slot = open_slot["CE" if state.bear else "PE"]
        if state.t1 is not None and state.t1.status == "open":
            r: ExitCheck = check_t1(state.t1, bar, is_short=is_short)
            if r.hit:
                finalize(state, "T1", state.t1, r.reason, r.price, bar.timestamp, slot)
                if r.reason == "t1_target_2r" and state.t2 is not None and \
                        state.t2.status == "open" and state.trail is not None:
                    state.trail.move_to_breakeven(state.t1.entry_price, buffer=0.0)
        if state.t2 is not None and state.t2.status == "open" and state.trail is not None:
            state.trail.on_5m_bar(bar)
            r = state.trail.check_hit(bar)
            if r.hit:
                finalize(state, "T2", state.t2, r.reason, r.price, bar.timestamp, slot)

    def try_open(state: _SideState, slot: _ZoneSlot, fill_price: float, ts) -> None:
        htf, ltf = slot.zone, slot.ltf_zone
        if htf is None or ltf is None:
            return
        sl_price = slot.zone_low - entry_offset if state.bear else slot.zone_high + entry_offset
        t1_target = ltf.sl_level
        t2_target = htf.sl_level
        side = "CE" if state.bear else "PE"
        state.t1 = TrancheLeg(tranche="T1", option_type=side, strike=0.0, qty=tranche_qty,
                               entry_price=fill_price, entry_time=ts, entry_reason="htf_ltf_cascade",
                               sl_price=sl_price, target_price=t1_target)
        state.t2 = TrancheLeg(tranche="T2", option_type=side, strike=0.0, qty=tranche_qty,
                               entry_price=fill_price, entry_time=ts, entry_reason="htf_ltf_cascade",
                               sl_price=sl_price, target_price=t2_target)
        state.trail = TrailingBaseTracker(bear=state.bear, initial_stop=sl_price)
        open_slot[side] = slot
        # A position just opened -- only one at a time per side, matching
        # the engine's single-CascadePosition constraint. Discard the whole
        # pool; a fresh one builds up again once this position closes.
        state.pool = []

    def process_5m(state: _SideState, bar: "_Bar") -> None:
        if state.is_open():
            check_exits(state, bar)
            return
        for slot in list(state.pool):
            if not slot.tracking:
                continue
            broken = bar.close < slot.zone_low if state.bear else bar.close > slot.zone_high
            aged_out = (bar.timestamp - slot.zone.reference_low_ts) >= timedelta(days=HTF_ZONE_MAX_AGE_DAYS)
            if broken or aged_out:
                state.pool.remove(slot)
                continue
            if slot.ltf_zone is None:
                slot.prev_5m_bar = bar
                continue
            prev = slot.prev_5m_bar
            slot.prev_5m_bar = bar
            if prev is None:
                continue
            if not slot.pending_entry:
                triggered = bar.close > prev.high if state.bear else bar.close < prev.low
                if triggered:
                    slot.pending_entry = True
                    slot.trigger_ts = bar.timestamp
            if not slot.pending_entry:
                continue
            limit_price = slot.zone_low + entry_offset if state.bear else slot.zone_high - entry_offset
            pierced = bar.low <= limit_price if state.bear else bar.high >= limit_price
            if pierced:
                try_open(state, slot, limit_price, bar.timestamp)
                return  # pool just got cleared; nothing else to process this tick

    for idx, bar in enumerate(bars_5m):
        for state in (ce, pe):
            process_5m(state, bar)

        cur_key = _bucket_key(bar.timestamp, 15, SESSION_OPEN)
        bucket_closing_15 = (idx + 1 < len(bars_5m)
                              and _bucket_key(bars_5m[idx + 1].timestamp, 15, SESSION_OPEN) != cur_key)
        if bucket_closing_15:
            src = bars_15m_by_key.get(cur_key)
            if src is not None:
                b15 = _Bar(src.timestamp, src.close, src.high, src.low, src.close, tf=15)
                for state, finder in ((ce, find_all_bear_zones), (pe, find_all_bull_zones)):
                    if state.is_open():
                        continue
                    single_finder = _single_zone_finder(finder)
                    for slot in state.pool:
                        if slot.tracking:
                            slot.bars_15m.append(b15)
                            z = single_finder(slot.bars_15m)
                            if z is not None:
                                slot.ltf_zone = z

        key75 = _bucket_key(bar.timestamp, 75, SESSION_OPEN)
        bucket_closing_75 = (idx + 1 < len(bars_5m)
                              and _bucket_key(bars_5m[idx + 1].timestamp, 75, SESSION_OPEN) != key75)
        if bucket_closing_75:
            src = bars_75m_by_key.get(key75)
            if src is not None:
                b75 = _Bar(src.timestamp, src.close, src.high, src.low, src.close, tf=75)
                all_75m.append(b75)
                for state, all_finder in ((ce, find_all_bear_zones), (pe, find_all_bull_zones)):
                    if state.is_open():
                        continue
                    # Age out / invalidate pre-tracking pool members -- never
                    # rediscovered (known_ref_ts keeps them out for good).
                    for slot in list(state.pool):
                        if slot.tracking:
                            continue
                        broken = (b75.close < slot.zone_low if state.bear
                                  else b75.close > slot.zone_high)
                        aged_out = (b75.timestamp - slot.zone.reference_low_ts) >= timedelta(
                            days=HTF_ZONE_MAX_AGE_DAYS)
                        if broken or aged_out:
                            state.pool.remove(slot)
                    # Discover every NEW zone within the 10-day lookback --
                    # not just the newest -- and add each as its own pool slot.
                    lookback_start = b75.timestamp - timedelta(days=HTF_ZONE_MAX_AGE_DAYS)
                    search_bars = [b for b in all_75m if b.timestamp >= lookback_start]
                    for z in all_finder(search_bars, known_ref_ts=state.known_ref_ts):
                        state.known_ref_ts.add(z.reference_low_ts)
                        state.pool.append(_ZoneSlot(z))
                    # Each pool zone independently checks re-entry.
                    for slot in state.pool:
                        if slot.tracking:
                            continue
                        if _overlaps(b75.low, b75.high, slot.zone_low, slot.zone_high):
                            slot.tracking = True
                            slot.reentry_ts = b75.timestamp

        if (bar.timestamp.hour, bar.timestamp.minute) == EOD_HOUR_MIN:
            for state in (ce, pe):
                slot = open_slot["CE" if state.bear else "PE"]
                for tranche, leg in (("T1", state.t1), ("T2", state.t2)):
                    if leg is not None and leg.status == "open":
                        finalize(state, tranche, leg, "eod_force_close", bar.close, bar.timestamp, slot)

    return legs


def _single_zone_finder(all_finder):
    """Adapts a find_all_*_zones function into a find_bear_zone/find_bull_zone-
    shaped "newest one" call, for the LTF (15m) search -- a zone slot only
    ever needs its OWN single newest nested pattern, never a nested pool."""
    def _find(bars):
        zones = all_finder(bars)
        return zones[0] if zones else None
    return _find


def build_5m_bars(rows_1m: List[dict]) -> List["_Bar"]:
    """Index/spot data has no meaningful traded volume -- never filter on it
    (unlike option-premium bars, which _to_5m_bars normally filters)."""
    return _to_5m_bars(rows_1m, filter_zero_volume=False)
