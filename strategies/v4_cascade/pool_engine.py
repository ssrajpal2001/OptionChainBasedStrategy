"""strategies/v4_cascade/pool_engine.py -- HTF-gated LTF cascade engine,
live-incremental adaptation of backtest/v4_cascade/htf_ltf_backtest.py's
multi-zone pool (validated over 90 days of real NIFTY data). Scans the
TRACKING contract's own premium directly (not spot) and trades it directly
(no execution-strike split) -- see
docs/superpowers/specs/2026-07-23-v4-cascade-htf-ltf-live-design.md.

2026-07-23 correction: this engine is ALWAYS a buyer -- long CE or long PE,
never a short. Both sides look for the SAME pattern on their OWN premium
chart: a bear trap (price sweeps below a demand-zone floor, sellers who
shorted/sold into the sweep get trapped, price reclaims back up) -> buy.
There is no bull-trap branch and no CE/PE sign-flip anywhere in this file
-- that distinction only made sense for the OLD Index/spot-based Gate 1
(where CE needed an Index bear trap and PE needed an Index bull trap,
since the index itself only moves one direction at a time). Scanning each
option's OWN premium directly removes that asymmetry entirely: a demand-
zone reclaim on the CE premium chart and a demand-zone reclaim on the PE
premium chart mean exactly the same thing (buy that option), independently.

2026-07-24: widened to track several candidate strikes per side (was one
fixed ATM-offset strike per side) -- see
docs/superpowers/specs/2026-07-24-v4-cascade-multi-strike-scan-design.md.
The pool dict is now keyed (side, strike) instead of side alone; every
public method gains a strike parameter. Same-side candidates are mutually
exclusive (only one can be open at a time, others frozen while one is
open) but cross-side structural flip is unchanged.

2026-07-23 correction: the 15m LTF trap is NOT a precondition for the 5m
entry trigger to arm or fire -- entry is driven by the 75m zone re-entry
plus the 5m break-of-structure alone. The 15m trap (if one has locked
inside the zone by fill time) is consulted ONLY inside _open_position, to
set T1's target; if none has locked yet, T1 falls back to the same target
as T2 (the 75m zone's own opposite extreme).

Fed incrementally via on_75m_bar/on_15m_bar/on_5m_bar (mirrors
SpotConfirmTracker/IndexGatedPremiumScanner's existing shape), unlike the
backtest's whole-array replay loop -- the SAME class serves both the boot-
time history replay (call the three methods in chronological order over
fetched bars) and live tick-driven operation (call them as each bucket
closes)."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

from strategies.v4_cascade.config import V4CascadeConfig
from strategies.v4_cascade.dataclasses import (
    CascadeEvent, CascadeEventType, CascadePosition, RollingBaseZone, TrancheLeg,
)
from strategies.v4_cascade.exits import ExitCheck, TrailingBaseTracker, check_t1
from strategies.v4_cascade.rolling_base import find_all_bear_zones

HTF_ZONE_MAX_AGE_DAYS = 10


def _zone_bounds(z: RollingBaseZone) -> Tuple[float, float]:
    return min(z.entry_line, z.sweep_low), max(z.entry_line, z.sweep_low)


def _overlaps(bar_low: float, bar_high: float, lo: float, hi: float) -> bool:
    return bar_low <= hi and bar_high >= lo


class _ZoneSlot:
    """One candidate HTF zone's independent tracking state, living inside a
    (side, strike) candidate's pool -- multiple zones progress concurrently,
    each with its own re-entry/LTF/5m-trigger state."""

    def __init__(self, zone: RollingBaseZone, strike: float = 0.0) -> None:
        self.zone = zone
        self.strike = strike
        self.zone_low, self.zone_high = _zone_bounds(zone)
        self.tracking = False
        self.reentry_ts: Optional[datetime] = None
        self.ltf_zone: Optional[RollingBaseZone] = None
        self.bars_15m: List = []
        self.prev_5m_bar: Optional[object] = None
        self.pending_entry = False
        self.trigger_ts: Optional[datetime] = None
        # 2026-07-23: has price come back inside this slot's 15m sub-zone
        # since it was found -- used ONLY for counter-side trailing (see
        # PoolCascadeEngine.on_15m_bar), never for this slot's own entry
        # (entry fires off the 75m zone + 5m trigger alone, see on_5m_bar).
        self.ltf_reentered = False
        self.ltf_reentry_ts: Optional[datetime] = None


class PoolCascadeEngine:
    """One instance per book (NIFTY only). .position mirrors
    V4CascadeEngine's own .position attribute exactly, so book.py's
    persistence/dashboard/EOD code reads it unchanged regardless of which
    engine produced it.

    2026-07-24: widened from a side-keyed pool ("CE"/"PE") to a
    (side, strike)-composite-keyed pool so book.py can track several
    candidate strikes per side (see
    docs/superpowers/specs/2026-07-24-v4-cascade-multi-strike-scan-design.md)
    instead of committing blindly to one fixed ATM-offset. Kept as ONE
    shared engine instance (not one per candidate) specifically so the
    existing whole-engine "only one position ever" invariant
    (self.position/is_open()) and cross-side structural-flip logic in
    _close_for_structural_flip keep working with zero new coordination
    code -- they already operate at the whole-engine level, unaware of how
    many candidates exist per side.

    Same-side exclusivity (new): while a position is open on side X, EVERY
    other candidate on side X (not just the traded one) is frozen -- no
    zone-discovery (on_75m_bar/on_15m_bar already returned unconditionally
    for the whole side before this change, which generalizes correctly:
    "the side is open" is true regardless of which exact candidate holds
    the trade) and no new trigger-arming/pierce-checking (on_5m_bar, which
    DOES need an explicit strike comparison, since unlike 75m/15m it takes
    an active branch -- feed check_exits -- for the exact open candidate).
    Cross-side flip is UNCHANGED: a pierce on the opposite side still force-
    closes whatever's open and flips into the new position, confirmed with
    the user as a preserved behavior, not a regression, from the commit
    immediately before this change."""

    def __init__(self, cfg: V4CascadeConfig, entry_offset: float,
                 session_open: Tuple[int, int] = (9, 15)) -> None:
        self._cfg = cfg
        self._entry_offset = entry_offset
        self._session_open = session_open
        # Per-candidate state -- keyed (side, strike), populated lazily via
        # .setdefault() as new candidates are first fed a 75m bar. A
        # candidate with no zones yet simply has no key (equivalent to an
        # empty pool), matching today's side-keyed dict's own "empty list"
        # semantics.
        self._pool: Dict[Tuple[str, float], List[_ZoneSlot]] = {}
        self._known_ref_ts: Dict[Tuple[str, float], Set[datetime]] = {}
        self._all_75m: Dict[Tuple[str, float], List] = {}
        self._last_5m_date: Dict[Tuple[str, float], Optional[date]] = {}
        # Side-keyed (NOT per-candidate): only the SINGLE traded candidate
        # on a side ever has an open position, so there is never a need to
        # track more than one trailing tracker / "last bar seen while open"
        # per side at once. See _open_position/_close_for_structural_flip.
        self._trail: Dict[str, Optional[TrailingBaseTracker]] = {"CE": None, "PE": None}
        # 2026-07-23: last 5m bar seen per side. While FLAT, updated by
        # WHICHEVER candidate on that side ticks most recently (last-write-
        # wins across all candidates -- mirrors book.py's own _live_price
        # convention). While a side is OPEN, only the exact traded
        # candidate's bars reach the line that updates this (siblings
        # return early in on_5m_bar before touching it) -- so it always
        # reflects the traded contract's own price once a position exists,
        # never a sibling's unrelated premium scale.
        self._last_5m_bar: Dict[str, Optional[object]] = {"CE": None, "PE": None}
        self.position: Optional[CascadePosition] = None

    def is_open(self) -> bool:
        return self.position is not None and self.position.is_open

    def reset_candidate(self, side: str, strike: float) -> None:
        """Clear ONE candidate's zone pool, HTF (75m) bar history, and
        known-ref dedup set -- used by book.py's per-strike recenter diff
        (only strikes that actually left/entered the ATM window are reset,
        not the whole side) so a candidate that stays in-window across a
        recenter keeps its in-progress zone pool instead of re-warming from
        scratch."""
        key = (side, strike)
        self._pool[key] = []
        self._known_ref_ts[key] = set()
        self._all_75m[key] = []
        self._last_5m_date[key] = None

    def reset_side(self, side: str) -> None:
        """Clear every candidate currently tracked on this side. Mirrors
        the old single-candidate reset_side exactly when there is only one
        candidate; with multiple candidates this is a full-side wipe (book.py's
        recenter uses reset_candidate directly per changed strike instead,
        to avoid re-warming candidates that didn't move -- this stays
        available for any full-side-wipe caller)."""
        for (s, k) in [key for key in self._pool.keys() if key[0] == side]:
            self.reset_candidate(s, k)

    # ── HTF (75m) ────────────────────────────────────────────────────────
    def on_75m_bar(self, side: str, strike: float, bar) -> None:
        if self.is_open() and self.position.side == side:
            return
        key = (side, strike)
        self._all_75m.setdefault(key, []).append(bar)
        pool = self._pool.setdefault(key, [])
        known = self._known_ref_ts.setdefault(key, set())

        for slot in list(pool):
            if slot.tracking:
                continue
            broken = bar.close < slot.zone_low
            aged_out = (bar.timestamp - slot.zone.reference_low_ts) >= timedelta(days=HTF_ZONE_MAX_AGE_DAYS)
            if broken or aged_out:
                pool.remove(slot)

        # Re-entry check runs BEFORE this bar's newly-discovered zones are
        # appended -- a zone whose reclaim candle IS this very bar must not
        # treat that same candle as a later "re-entry"; tracking can only
        # arm on a bar strictly after the zone already existed in the pool.
        for slot in pool:
            if slot.tracking:
                continue
            if _overlaps(bar.low, bar.high, slot.zone_low, slot.zone_high):
                slot.tracking = True
                slot.reentry_ts = bar.timestamp

        lookback_start = bar.timestamp - timedelta(days=HTF_ZONE_MAX_AGE_DAYS)
        search_bars = [b for b in self._all_75m[key] if b.timestamp >= lookback_start]
        for z in find_all_bear_zones(search_bars, known_ref_ts=known):
            known.add(z.reference_low_ts)
            pool.append(_ZoneSlot(z, strike))

    # ── LTF (15m) ────────────────────────────────────────────────────────
    def on_15m_bar(self, side: str, strike: float, bar) -> None:
        if self.is_open() and self.position.side == side:
            return
        # 2026-07-23: while the OTHER side holds an open position, this
        # side's own 15m sub-zone re-entry feeds that position's T2 trail
        # (see exits.TrailingBaseTracker.consider_external_level) -- real
        # structural evidence the counter side may be turning, ratcheting
        # the open side's stop tighter without forcing an early exit on
        # mere proximity (only a genuine re-entry counts, and the stop only
        # ever moves in the favorable direction).
        is_counter_side = self.is_open() and self.position.side != side
        open_side = self.position.side if is_counter_side else None

        for slot in self._pool.get((side, strike), []):
            if not slot.tracking:
                continue
            slot.bars_15m.append(bar)
            zones = find_all_bear_zones(slot.bars_15m)
            if zones:
                new_ltf = zones[0]
                if slot.ltf_zone is None or new_ltf.reference_low_ts != slot.ltf_zone.reference_low_ts:
                    slot.ltf_zone = new_ltf
                    slot.ltf_reentered = False

            ltf = slot.ltf_zone
            if ltf is not None and not slot.ltf_reentered and bar.timestamp > ltf.lock_ts:
                lo, hi = _zone_bounds(ltf)
                if _overlaps(bar.low, bar.high, lo, hi):
                    slot.ltf_reentered = True
                    slot.ltf_reentry_ts = bar.timestamp
                    continue

            if is_counter_side and slot.ltf_reentered and open_side is not None:
                trail = self._trail.get(open_side)
                if trail is not None:
                    trail.consider_external_level(bar.low)

    # ── 5m trigger + limit fill + exits ─────────────────────────────────
    def on_5m_bar(self, side: str, strike: float, bar) -> List[CascadeEvent]:
        if self.is_open() and self.position.side == side:
            if self.position.tracking_strike == strike:
                self._last_5m_bar[side] = bar
                return self._check_exits(side, bar)
            # A DIFFERENT candidate on the same side as the open position --
            # frozen (no new scanning/triggering) until that position
            # closes, per the confirmed same-side-exclusivity rule.
            return []

        self._last_5m_bar[side] = bar
        key = (side, strike)
        pool = self._pool.setdefault(key, [])

        # 2026-07-23: intraday-only trigger -- the first 5m candle of a new
        # session has no legitimate "previous candle". Only this
        # candidate's own trigger prev-candle pointer resets here -- the
        # HTF pool and any zone's mid-tracking LTF/pending state both carry
        # across days completely unchanged.
        bar_date = bar.timestamp.date()
        if self._last_5m_date.get(key) != bar_date:
            for slot in pool:
                slot.prev_5m_bar = None
            self._last_5m_date[key] = bar_date

        events: List[CascadeEvent] = []
        for slot in list(pool):
            if not slot.tracking:
                continue
            broken = bar.close < slot.zone_low
            aged_out = (bar.timestamp - slot.zone.reference_low_ts) >= timedelta(days=HTF_ZONE_MAX_AGE_DAYS)
            if broken or aged_out:
                pool.remove(slot)
                continue

            if not slot.pending_entry:
                prev = slot.prev_5m_bar
                slot.prev_5m_bar = bar
                if prev is None:
                    continue
                triggered = bar.close > prev.high
                if triggered:
                    slot.pending_entry = True
                    slot.trigger_ts = bar.timestamp
                continue

            slot.prev_5m_bar = bar
            limit_price = slot.zone_low + self._entry_offset
            pierced = bar.low <= limit_price
            if pierced:
                # A pierce firing HERE means if a position is open at all,
                # it's on the OPPOSITE side (we already returned early above
                # if `side` were the currently-open side) -- force-close it
                # (structural flip), preserved exactly as today, then open
                # this candidate's position.
                if self.is_open():
                    events.extend(self._close_for_structural_flip(bar.timestamp))
                events.append(self._open_position(side, slot, limit_price, bar.timestamp))
                return events
        return events

    def _open_position(self, side: str, slot: _ZoneSlot, fill_price: float, ts) -> CascadeEvent:
        htf, ltf = slot.zone, slot.ltf_zone
        sl_price = slot.zone_low - self._entry_offset
        t2_target = htf.sl_level
        t1_target = ltf.sl_level if ltf is not None else t2_target
        qty = self._cfg.tranche_qty
        t1 = TrancheLeg(tranche="T1", option_type=side, strike=slot.strike, qty=qty,
                         entry_price=fill_price, entry_time=ts, entry_reason="htf_ltf_pool_cascade",
                         sl_price=sl_price, target_price=t1_target)
        t2 = TrancheLeg(tranche="T2", option_type=side, strike=slot.strike, qty=qty,
                         entry_price=fill_price, entry_time=ts, entry_reason="htf_ltf_pool_cascade",
                         sl_price=sl_price, target_price=None, tracking_current_stop=sl_price)
        self.position = CascadePosition(
            underlying=self._cfg.underlying, side=side,
            tracking_strike=slot.strike, execution_strike=slot.strike,
            atm_at_trigger=0.0, entry_spot=0.0,
            t1=t1, t2=t2, open_time=ts,
            tracking_entry_price=fill_price,
        )
        self._trail[side] = TrailingBaseTracker(bear=True, initial_stop=sl_price)
        # A position just opened -- only one at a time, engine-wide. Discard
        # EVERY candidate's pool on this side (was self._pool[side] = []
        # under the old single-candidate model; now spans every strike).
        for key in [k for k in self._pool.keys() if k[0] == side]:
            self._pool[key] = []
        event_type = CascadeEventType.OPEN_LONG_CE if side == "CE" else CascadeEventType.OPEN_LONG_PE
        audit = {
            "htf_ref_ts": htf.reference_low_ts.isoformat() if htf.reference_low_ts else None,
            "htf_lock_ts": htf.lock_ts.isoformat() if htf.lock_ts else None,
            "reentry_ts": slot.reentry_ts.isoformat() if slot.reentry_ts else None,
            "ltf_ref_ts": ltf.reference_low_ts.isoformat() if ltf is not None and ltf.reference_low_ts else None,
            "ltf_found_at_fill": ltf is not None,
            "trigger_ts": slot.trigger_ts.isoformat() if slot.trigger_ts else None,
            "zone_low": slot.zone_low, "zone_high": slot.zone_high,
            "strike": slot.strike,
            "computed_sl_price": sl_price, "t1_target": t1_target, "t2_target": t2_target,
        }
        return CascadeEvent(event_type=event_type, side=side, execution_strike=slot.strike,
                             price_hint=fill_price, reason="gate3_bear_trap_reclaim",
                             sl_price=sl_price, target_price=t1_target, timestamp=ts, audit=audit)

    def _close_for_structural_flip(self, ts) -> List[CascadeEvent]:
        """A pierce just fired on the side OPPOSITE the currently open
        position -- close both legs (whichever are still open) at the open
        side's own last known 5m close (the best price information
        available inside this engine; book.py's live tick feed has a more
        current price but this engine has no visibility into it)."""
        pos = self.position
        open_side = pos.side
        last_bar = self._last_5m_bar.get(open_side)
        if last_bar is not None:
            price = last_bar.close
        elif pos.t1 is not None:
            price = pos.t1.entry_price
        else:
            price = pos.tracking_entry_price or 0.0
        events: List[CascadeEvent] = []
        for tranche, leg in (("T1", pos.t1), ("T2", pos.t2)):
            if leg is None or leg.status != "open":
                continue
            leg.status = "closed"
            leg.close_price = price
            leg.close_reason = "structural_flip"
            leg.close_time = ts
            events.append(self._close_event(open_side, tranche, "structural_flip", price, ts))
        pos.status = "closed"
        pos.close_time = ts
        self._trail[open_side] = None
        return events

    def _check_exits(self, side: str, bar) -> List[CascadeEvent]:
        events: List[CascadeEvent] = []
        pos = self.position
        if pos is None or pos.side != side or not pos.is_open:
            return events
        # Always a long buyer -- never short, on either side (see module
        # docstring). check_t1's is_short branch (SL on the bar's high,
        # target on the bar's low) never applies here.
        t1, t2 = pos.t1, pos.t2
        trail = self._trail[side]

        if t1 is not None and t1.status == "open":
            r: ExitCheck = check_t1(t1, bar, is_short=False)
            if r.hit:
                t1.status = "closed"
                t1.close_price = r.price
                t1.close_reason = r.reason
                t1.close_time = bar.timestamp
                events.append(self._close_event(side, "T1", r.reason, r.price, bar.timestamp))
                if r.reason == "t1_target_2r" and t2 is not None and t2.status == "open" and trail is not None:
                    trail.move_to_breakeven(t1.entry_price, buffer=0.0)
                    if trail.current_stop is not None:
                        t2.trail_stop_price = trail.current_stop
                        t2.tracking_current_stop = trail.current_stop
        if t2 is not None and t2.status == "open" and trail is not None:
            trail.on_5m_bar(bar)
            r = trail.check_hit(bar)
            if r.hit:
                t2.status = "closed"
                t2.close_price = r.price
                t2.close_reason = r.reason
                t2.close_time = bar.timestamp
                events.append(self._close_event(side, "T2", r.reason, r.price, bar.timestamp))

        if (t1 is None or t1.status == "closed") and (t2 is None or t2.status == "closed"):
            pos.status = "closed"
            pos.close_time = bar.timestamp
            self._trail[side] = None
        return events

    @staticmethod
    def _close_event(side: str, tranche: str, reason: str, price: float, ts) -> CascadeEvent:
        event_type = CascadeEventType.CLOSE_LONG_CE if side == "CE" else CascadeEventType.CLOSE_LONG_PE
        return CascadeEvent(event_type=event_type, side=side, tranche=tranche,
                             reason=reason, price_hint=price, timestamp=ts)

    def force_eod_close(self, side: str, ts, price: float) -> List[CascadeEvent]:
        """Called by book.py's existing EOD square-off path."""
        events: List[CascadeEvent] = []
        pos = self.position
        if pos is None or pos.side != side or not pos.is_open:
            return events
        for tranche, leg in (("T1", pos.t1), ("T2", pos.t2)):
            if leg is None or leg.status != "open":
                continue
            leg.status = "closed"
            leg.close_price = price
            leg.close_reason = "eod_force_close"
            leg.close_time = ts
            events.append(self._close_event(side, tranche, "eod_force_close", price, ts))
        pos.status = "closed"
        pos.close_time = ts
        self._trail[side] = None
        return events
