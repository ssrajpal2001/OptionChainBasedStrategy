"""backtest/v4_cascade/htf_ltf_backtest.py -- HTF(75m)-gated LTF(15m/5m)
cascade strategy, per the user's 2026-07-22 spec (supersedes run_backtest.py,
which replayed the CURRENT production Gate1/Gate2/Gate3 model and was found
to over-trigger relative to the user's intended design).

LONG (bearish trap):
  1. HTF zone (75m): SpotConfirmTracker.find_bear_zone -- completely
     UNCHANGED, real production code/class. Zone = [sweep_low,
     entry_line=ref.low]; T2's target = zone.sl_level (ref.high). Carries
     across days if not re-entered same day (SpotConfirmTracker's own
     persistence, never day-reset -- confirmed correct by the user,
     untouched).
  2. Wait for a later 75m candle to re-enter [zone_low, zone_high].
  3. Once re-entered, start tracking 15m + 5m for this armed zone.
  4. 15m nested zone: find_bear_zone again (same function), fed 15m bars
     from the HTF ref candle's own timestamp onward. Re-searched on every
     15m close until found (or until the HTF zone itself is invalidated/
     replaced). T1's target = ltf_zone.sl_level.
  5. 5m trigger: a candle closes above the immediately preceding 5m
     candle's high. Only meaningful once an ltf_zone exists (T1's target
     must be known before a trade can open). Re-checked every 5m candle.
  6. On trigger: limit entry at htf_zone_low + offset, SL at htf_zone_low -
     offset (both T1 and T2 fill at the same price). Limit fills the first
     time a later 5m bar's low pierces down to it (checked starting with
     the trigger bar itself).
  7. T1 exits at ltf_zone.sl_level (target) or the shared SL. T2 exits at
     htf_zone.sl_level (target) or the shared SL, with the existing
     breakeven-then-trail ratchet once T1's target hits (exits.py's
     TrailingBaseTracker, unmodified).

SHORT (bullish trap) is the exact mirror, using find_bull_zone and swapping
every high/low comparison."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from strategies.v4_cascade.book import _Bar, _bucket_key, _to_5m_bars
from strategies.v4_cascade.dataclasses import RollingBaseZone, TrancheLeg
from strategies.v4_cascade.exits import ExitCheck, TrailingBaseTracker, check_t1
from strategies.v4_cascade.rolling_base import find_bear_zone, find_bull_zone, resample_bars

SESSION_OPEN: Tuple[int, int] = (9, 15)
EOD_HOUR_MIN: Tuple[int, int] = (15, 15)
# 2026-07-22: real option contracts only have ~10-14 days of usable liquid
# history behind the current point (prev week + current week) -- the zone
# search is bounded to match, both because that's what phase 2 (real option
# data) will actually have available, and because it fixes a real bug found
# in this NIFTY-spot phase: a valid-but-unconfirmed zone with no expiry was
# blocking the system from ever considering a fresher, more relevant zone
# for six-plus weeks (NIFTY simply never came back to revisit it). A zone
# older than this ages out of consideration entirely, win or lose.
HTF_ZONE_MAX_AGE_DAYS = 10


class _SideState:
    """Per-side (CE=long/bear-trap, PE=short/bull-trap) tracking state."""

    def __init__(self, bear: bool) -> None:
        self.bear = bear  # True=CE/long, False=PE/short
        self.htf_zone: Optional[RollingBaseZone] = None
        self.tracking = False        # price has re-entered the HTF zone
        self.ltf_zone: Optional[RollingBaseZone] = None
        self.bars_15m: List["_Bar"] = []   # accumulated since HTF zone was found (for the 15m scan)
        self.prev_5m_bar: Optional["_Bar"] = None
        # 2026-07-22 fix: the 5m trigger and the limit-order fill are now two
        # SEPARATE, persistent steps -- pending_entry stays armed across bars
        # once the trigger fires, until the limit genuinely fills OR the zone
        # is invalidated. Previously trigger+fill were only checked together
        # on the SAME bar and discarded otherwise, which meant a trigger that
        # fired long after price had crashed far away from the zone could
        # still "pierce" a limit price that was, by then, hundreds of points
        # from the real market -- a phantom fill that never happened in
        # reality (confirmed live: a logged 24172.10 fill on a bar where
        # NIFTY was actually trading near 23960).
        self.pending_entry = False
        self.t1: Optional[TrancheLeg] = None
        self.t2: Optional[TrancheLeg] = None
        self.trail: Optional[TrailingBaseTracker] = None
        self.htf_zone_low = 0.0
        self.htf_zone_high = 0.0
        # Audit trail, for a full per-trade "why did this fire" table:
        self.reentry_ts = None   # when a later 75m candle first came back inside the HTF zone
        self.trigger_ts = None   # when the 5m break-of-structure candle closed (armed the limit)

    def is_open(self) -> bool:
        return (self.t1 is not None and self.t1.status == "open") or \
               (self.t2 is not None and self.t2.status == "open")

    def reset_tracking(self) -> None:
        self.tracking = False
        self.ltf_zone = None
        self.bars_15m = []
        self.prev_5m_bar = None
        self.pending_entry = False
        self.reentry_ts = None
        self.trigger_ts = None


def _zone_bounds(z: RollingBaseZone) -> Tuple[float, float]:
    return min(z.entry_line, z.sweep_low), max(z.entry_line, z.sweep_low)


def _overlaps(bar_low: float, bar_high: float, lo: float, hi: float) -> bool:
    return bar_low <= hi and bar_high >= lo


def run_backtest(bars_5m: List["_Bar"], entry_offset: float = 10.0, qty: int = 130,
                  sl_buffer: float = 0.0) -> List[dict]:
    """entry_offset: the +-10pt offset from htf_zone_low/high for both the
    limit entry and the SL (this run's grid-search parameter). qty: combined
    T1+T2 quantity (matches V4CascadeConfig.tranche_qty*2 by convention;
    T1/T2 each get qty//2). sl_buffer: unused placeholder kept at 0.0 -- the
    spec's SL IS the offset itself (zone_low - entry_offset), no separate
    buffer layered on top."""
    ce = _SideState(bear=True)
    pe = _SideState(bear=False)
    legs: List[dict] = []
    tranche_qty = qty // 2

    bars_75m_by_key = {_bucket_key(b.timestamp, 75, SESSION_OPEN): b
                        for b in resample_bars(bars_5m, 75, SESSION_OPEN)}
    bars_15m_by_key = {_bucket_key(b.timestamp, 15, SESSION_OPEN): b
                        for b in resample_bars(bars_5m, 15, SESSION_OPEN)}

    all_75m: List["_Bar"] = []  # growing list fed to find_bear_zone/find_bull_zone each 75m close

    def finalize(state: _SideState, tranche: str, leg: TrancheLeg, reason: str,
                 price: float, ts) -> None:
        leg.status = "closed"
        leg.close_price = price
        leg.close_reason = reason
        leg.close_time = ts
        is_short = not state.bear
        pnl_points = (leg.entry_price - price) if is_short else (price - leg.entry_price)
        htf, ltf = state.htf_zone, state.ltf_zone
        # ref_high/ref_low are the ORIGINAL reference candle's own high/low --
        # entry_line/sl_level hold them, just swapped depending on bear/bull
        # (see find_bear_zone/find_bull_zone's field mapping).
        htf_ref_high = htf.sl_level if state.bear else htf.entry_line if htf else None
        htf_ref_low = htf.entry_line if state.bear else htf.sl_level if htf else None
        ltf_ref_high = ltf.sl_level if state.bear else ltf.entry_line if ltf else None
        ltf_ref_low = ltf.entry_line if state.bear else ltf.sl_level if ltf else None
        legs.append({
            "side": "CE" if state.bear else "PE", "tranche": tranche,
            "htf_ref_ts": htf.reference_low_ts if htf else None,
            "htf_ref_high": htf_ref_high, "htf_ref_low": htf_ref_low,
            "htf_lock_ts": htf.lock_ts if htf else None,          # 75m sweep+reclaim confirmed ("trapped")
            "reentry_ts": state.reentry_ts,                        # price came back inside the HTF zone
            "ltf_ref_ts": ltf.reference_low_ts if ltf else None,
            "ltf_ref_high": ltf_ref_high, "ltf_ref_low": ltf_ref_low,
            "trigger_ts": state.trigger_ts,                        # 5m break-of-structure candle closed
            "entry_ts": leg.entry_time, "entry_price": leg.entry_price,  # limit actually filled
            "sl_price": leg.sl_price, "target_price": leg.target_price,
            "close_ts": ts, "close_price": price, "close_reason": reason,
            "qty": leg.qty, "pnl_points": pnl_points,
        })

    def check_exits(state: _SideState, bar: "_Bar") -> None:
        is_short = not state.bear
        if state.t1 is not None and state.t1.status == "open":
            r: ExitCheck = check_t1(state.t1, bar, is_short=is_short)
            if r.hit:
                finalize(state, "T1", state.t1, r.reason, r.price, bar.timestamp)
                if r.reason == "t1_target_2r" and state.t2 is not None and \
                        state.t2.status == "open" and state.trail is not None:
                    state.trail.move_to_breakeven(state.t1.entry_price, buffer=0.0)
        if state.t2 is not None and state.t2.status == "open" and state.trail is not None:
            state.trail.on_5m_bar(bar)
            r = state.trail.check_hit(bar)
            if r.hit:
                finalize(state, "T2", state.t2, r.reason, r.price, bar.timestamp)

    def try_open(state: _SideState, fill_price: float, ts) -> None:
        htf = state.htf_zone
        ltf = state.ltf_zone
        if htf is None or ltf is None:
            return
        sl_price = state.htf_zone_low - entry_offset if state.bear else state.htf_zone_high + entry_offset
        t1_target = ltf.sl_level
        t2_target = htf.sl_level
        state.t1 = TrancheLeg(tranche="T1", option_type="CE" if state.bear else "PE",
                               strike=0.0, qty=tranche_qty, entry_price=fill_price,
                               entry_time=ts, entry_reason="htf_ltf_cascade",
                               sl_price=sl_price, target_price=t1_target)
        state.t2 = TrancheLeg(tranche="T2", option_type="CE" if state.bear else "PE",
                               strike=0.0, qty=tranche_qty, entry_price=fill_price,
                               entry_time=ts, entry_reason="htf_ltf_cascade",
                               sl_price=sl_price, target_price=t2_target)
        state.trail = TrailingBaseTracker(bear=state.bear, initial_stop=sl_price)
        # Position open -- LTF tracking for this zone is done; reset so a
        # FUTURE zone (after this one closes) starts clean.
        state.tracking = False
        state.pending_entry = False

    def process_5m(state: _SideState, bar: "_Bar") -> None:
        if state.is_open():
            check_exits(state, bar)
            return
        if state.tracking:
            # Invalidation: price has decisively broken back through the
            # HTF zone's own core level (zone_low for CE, zone_high for PE)
            # -- the "bears/buyers trapped, expect a reclaim" thesis this
            # whole setup depends on is now void. Cancels tracking AND any
            # armed-but-unfilled pending_entry, so a stray later trigger
            # can't fill a limit price the market has long since left behind
            # (the phantom-fill bug this fixes: a real crash from ~24160 to
            # ~23960 overnight left a stale armed limit at 24172.10, which a
            # much-later, unrelated noise-level trigger then "filled" at a
            # price NIFTY hadn't traded near in 20+ hours). A genuinely NEW
            # HTF zone can still be found fresh on a later 75m close.
            #
            # Also ages out here (same HTF_ZONE_MAX_AGE_DAYS rule as the
            # pre-tracking case) -- confirmed live: a zone that DID get
            # re-entered but then never produced an LTF pattern + 5m trigger
            # for weeks was blocking the system from ever picking up a much
            # fresher, already-confirmed zone sitting right there
            # unexamined. Dropping htf_zone entirely (not just tracking) so
            # the next 75m close searches fresh rather than immediately
            # re-adopting the same stale zone.
            broken = bar.close < state.htf_zone_low if state.bear else bar.close > state.htf_zone_high
            aged_out = (bar.timestamp - state.htf_zone.reference_low_ts) >= timedelta(days=HTF_ZONE_MAX_AGE_DAYS)
            if broken or aged_out:
                state.htf_zone = None
                state.reset_tracking()
                state.prev_5m_bar = bar
                return
        if not state.tracking or state.ltf_zone is None:
            state.prev_5m_bar = bar
            return
        prev = state.prev_5m_bar
        state.prev_5m_bar = bar
        if prev is None:
            return
        if not state.pending_entry:
            triggered = bar.close > prev.high if state.bear else bar.close < prev.low
            if triggered:
                state.pending_entry = True
                state.trigger_ts = bar.timestamp
        if not state.pending_entry:
            return
        limit_price = state.htf_zone_low + entry_offset if state.bear else state.htf_zone_high - entry_offset
        pierced = bar.low <= limit_price if state.bear else bar.high >= limit_price
        if pierced:
            try_open(state, limit_price, bar.timestamp)

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
                for state in (ce, pe):
                    if state.tracking and not state.is_open():
                        state.bars_15m.append(b15)
                        finder = find_bear_zone if state.bear else find_bull_zone
                        z = finder(state.bars_15m)
                        if z is not None:
                            state.ltf_zone = z

        key75 = _bucket_key(bar.timestamp, 75, SESSION_OPEN)
        bucket_closing_75 = (idx + 1 < len(bars_5m)
                              and _bucket_key(bars_5m[idx + 1].timestamp, 75, SESSION_OPEN) != key75)
        if bucket_closing_75:
            src = bars_75m_by_key.get(key75)
            if src is not None:
                b75 = _Bar(src.timestamp, src.close, src.high, src.low, src.close, tf=75)
                all_75m.append(b75)
                for state, finder in ((ce, find_bear_zone), (pe, find_bull_zone)):
                    if state.is_open():
                        continue
                    # 2026-07-22 fix: a valid, not-yet-re-entered HTF zone
                    # must NOT be silently replaced just because a newer
                    # candidate also happens to confirm -- confirmed live: a
                    # real zone matching a manually-verified chart (ref
                    # 04-24 11:45, confirmed 04-27 09:15) kept getting
                    # displaced by newer candidates on later 75m closes,
                    # every single time, before price ever came back to
                    # re-enter it -- so the trade never happened even though
                    # the zone itself was completely valid. A zone is only
                    # ever abandoned pre-tracking now if price has
                    # decisively closed back through its OWN zone_low/high
                    # (genuinely invalidated), or if it has simply aged out
                    # (see HTF_ZONE_MAX_AGE_DAYS) -- never merely superseded
                    # by a fresher candidate on its own.
                    if state.htf_zone is not None and not state.tracking:
                        broken = (b75.close < state.htf_zone_low if state.bear
                                  else b75.close > state.htf_zone_high)
                        aged_out = (b75.timestamp - state.htf_zone.reference_low_ts) >= timedelta(
                            days=HTF_ZONE_MAX_AGE_DAYS)
                        if broken or aged_out:
                            state.htf_zone = None
                    if state.htf_zone is None:
                        lookback_start = b75.timestamp - timedelta(days=HTF_ZONE_MAX_AGE_DAYS)
                        search_bars = [b for b in all_75m if b.timestamp >= lookback_start]
                        z = finder(search_bars)
                        if z is not None:
                            state.htf_zone = z
                            lo, hi = _zone_bounds(z)
                            state.htf_zone_low, state.htf_zone_high = lo, hi
                            state.reset_tracking()
                    if state.htf_zone is not None and not state.tracking:
                        lo, hi = state.htf_zone_low, state.htf_zone_high
                        if _overlaps(b75.low, b75.high, lo, hi):
                            state.tracking = True
                            state.reentry_ts = b75.timestamp
                            state.bars_15m = []
                            state.prev_5m_bar = None

        if (bar.timestamp.hour, bar.timestamp.minute) == EOD_HOUR_MIN:
            for state in (ce, pe):
                for tranche, leg in (("T1", state.t1), ("T2", state.t2)):
                    if leg is not None and leg.status == "open":
                        finalize(state, tranche, leg, "eod_force_close", bar.close, bar.timestamp)

    return legs


def build_5m_bars(rows_1m: List[dict]) -> List["_Bar"]:
    return _to_5m_bars(rows_1m, filter_zero_volume=False)
