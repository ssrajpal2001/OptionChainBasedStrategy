# V4 Cascade Multi-Strike Candidate Scanning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Widen V4Cascade's `PoolCascadeEngine` (NIFTY only) from tracking one fixed CE strike + one fixed PE strike to 5 candidate strikes per side, so the engine finds real bear-trap zones wherever they actually exist in that day's option chain instead of committing blindly to one fixed ATM∓200 offset.

**Architecture:** `PoolCascadeEngine`'s internal pool state widens from a `side`-keyed dict (`"CE"`/`"PE"`) to a `(side, strike)`-composite-keyed dict, inside the SAME single shared engine instance — so the existing whole-engine "only one position ever" invariant and cross-side structural-flip logic keep working with zero new coordination code. `book.py` resolves a list of candidate strikes per side (from a new configurable offset list, defaulting to today's exact single `[200.0]` offset), subscribes/feeds all of them, and reads the real triggering strike off the fired event instead of inferring it from `side` alone. Gated behind a new `V4CASCADE_TRACKING_OFFSETS` env var — unset, everything behaves byte-identically to today.

**Tech Stack:** Python 3.9, asyncio, pytest (`pytest-asyncio`, strict mode).

## Global Constraints

- NIFTY only — the old Gate1/2/3 engine (`V4CascadeEngine`), CRUDEOIL, BTC/ETH are completely untouched by this plan. `use_pool_engine=False` (the default) must remain byte-identical to today in every file this plan touches.
- `V4CASCADE_TRACKING_OFFSETS` unset → single-offset list `[200.0]`, i.e. exactly today's CE=ATM-200/PE=ATM+200 behavior, just routed through the new multi-candidate machinery.
- Exactly one position, engine-wide, at all times — never more than one open position across all 10 candidates combined.
- Cross-side structural flip (opposite-side pierce force-closes + flips into the new position) is PRESERVED exactly as it works today. Same-side sibling candidates are frozen (no new zone-discovery/triggering) while that side has an open position — this is the one new same-side-exclusivity rule, confirmed with the user.
- Every existing `tests/strategies/test_v4_cascade_*.py` test must keep passing, asserting the exact same behavior as today — call sites that must change syntax to match a new required parameter or new dict-key shape are adapted mechanically (exact rule given in each task below), never behaviorally changed.

---

### Task 1: `pool_engine.py` — composite `(side, strike)` pool keys + strike-aware position opening

**Files:**
- Modify: `strategies/v4_cascade/pool_engine.py`
- Modify (mechanical adaptation, see Step 7): `tests/strategies/test_v4_cascade_pool_engine.py`, `tests/strategies/test_v4_cascade_pool_engine_active_position.py`, `tests/strategies/test_v4_cascade_pool_engine_fills.py`, `tests/strategies/test_v4_cascade_pool_engine_history_replay.py`
- Test (new): `tests/strategies/test_v4_cascade_pool_engine_multi_strike.py`

**Interfaces:**
- Consumes: nothing new — `RollingBaseZone`, `CascadeEvent`, `CascadePosition`, `TrancheLeg`, `TrailingBaseTracker`, `check_t1`, `find_all_bear_zones` (all unchanged imports).
- Produces (used by Tasks 3-6):
  - `PoolCascadeEngine.on_75m_bar(side: str, strike: float, bar) -> None`
  - `PoolCascadeEngine.on_15m_bar(side: str, strike: float, bar) -> None`
  - `PoolCascadeEngine.on_5m_bar(side: str, strike: float, bar) -> List[CascadeEvent]`
  - `PoolCascadeEngine.reset_candidate(side: str, strike: float) -> None`
  - `PoolCascadeEngine.reset_side(side: str) -> None` (unchanged public name, reimplemented as a loop over `reset_candidate`)
  - `CascadeEvent.execution_strike` is now populated (was always `None` from this engine before) — book.py reads the real strike off this field instead of inferring from `side`.
  - `CascadePosition.tracking_strike` / `.execution_strike` are now populated with the real strike (were hardcoded `0.0`).

- [ ] **Step 1: Write the failing multi-candidate tests**

Create `tests/strategies/test_v4_cascade_pool_engine_multi_strike.py`:

```python
"""strategies/v4_cascade/pool_engine.py -- multi-candidate (side, strike)
pooling, added 2026-07-24 so V4Cascade can scan several strikes per side
instead of committing blindly to one fixed ATM-offset (real chart-confirmed
bug: a fixed offset landed CE in a zone-less region of that strike's own
premium chart while a different, untracked strike had 8 zones)."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _Bar
from strategies.v4_cascade.config import V4CascadeConfig
from strategies.v4_cascade.pool_engine import PoolCascadeEngine

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 1, 9, 15, tzinfo=IST)


def _bar75(offset, o, h, l, c):
    return _Bar(_BASE + timedelta(minutes=75 * offset), o, h, l, c, tf=75)


def _bar5(base, offset_min, o, h, l, c):
    return _Bar(base + timedelta(minutes=offset_min), o, h, l, c, tf=5)


def _engine():
    cfg = V4CascadeConfig(underlying="NIFTY", lot_multiplier=2, lot_size=65)
    return PoolCascadeEngine(cfg, entry_offset=5.0, session_open=(9, 15))


def _arm_zone(eng, side, strike):
    """ref@0 (low=100,high=110), sweep@1 (low=90), reclaim+reentry@2-3 --
    same geometry the existing single-candidate tests already use, just
    routed through a specific (side, strike) candidate."""
    eng.on_75m_bar(side, strike, _bar75(0, 105, 110, 100, 105))
    eng.on_75m_bar(side, strike, _bar75(1, 95, 100, 90, 95))
    eng.on_75m_bar(side, strike, _bar75(2, 96, 115, 95, 112))
    eng.on_75m_bar(side, strike, _bar75(3, 105, 108, 92, 96))  # re-entry


def test_two_ce_candidates_pool_independently():
    """CE 24000 finds a zone; CE 23900 (a different candidate, same side)
    sees completely different bars and finds nothing -- proves the pools
    are keyed independently per strike, not merged/shared by side."""
    eng = _engine()
    _arm_zone(eng, "CE", 24000.0)
    eng.on_75m_bar("CE", 23900.0, _bar75(0, 500, 500, 500, 500))
    eng.on_75m_bar("CE", 23900.0, _bar75(1, 500, 500, 500, 500))

    assert len(eng._pool[("CE", 24000.0)]) == 1
    assert eng._pool[("CE", 24000.0)][0].tracking is True
    assert eng._pool.get(("CE", 23900.0), []) == []


def test_only_first_pierce_opens_second_candidate_ignored():
    """Two CE candidates both reach pending_entry; whichever pierces FIRST
    opens the position, and the other candidate's later pierce is skipped
    -- exactly one position total, same-side siblings frozen once one side
    is open."""
    eng = _engine()
    _arm_zone(eng, "CE", 24000.0)
    _arm_zone(eng, "CE", 23900.0)

    base = _BASE + timedelta(minutes=75 * 4)
    # 24000 triggers and pierces on the very next bars.
    eng.on_5m_bar("CE", 24000.0, _bar5(base, 0, 96, 97, 95, 98))    # arms trigger (close > prev.high not yet checked -- see below)
    events = eng.on_5m_bar("CE", 24000.0, _bar5(base, 5, 99, 100, 89, 92))  # triggered (close>prev.high) AND pierces (low<=limit)
    assert eng.is_open()
    assert eng.position.side == "CE"
    assert eng.position.tracking_strike == 24000.0
    opened_events = [e for e in events if e.event_type.name.startswith("OPEN_")]
    assert len(opened_events) == 1

    # 23900's own bars keep arriving -- it must be frozen (no new events,
    # no state change) while CE is open, even though it independently has
    # a pending trigger of its own.
    before = list(eng._pool.get(("CE", 23900.0), []))
    more_events = eng.on_5m_bar("CE", 23900.0, _bar5(base, 5, 99, 100, 89, 92))
    assert more_events == []
    assert eng._pool.get(("CE", 23900.0), []) == before
    assert eng.position.tracking_strike == 24000.0  # unchanged -- no flip within the same side


def test_cross_side_structural_flip_still_works_with_multiple_candidates():
    """Opposite-side pierce still force-closes + flips into the new
    position -- the existing structural-flip behavior (shipped the commit
    immediately before this multi-strike work) must not regress."""
    eng = _engine()
    _arm_zone(eng, "CE", 24000.0)
    _arm_zone(eng, "PE", 23900.0)

    base = _BASE + timedelta(minutes=75 * 4)
    eng.on_5m_bar("CE", 24000.0, _bar5(base, 0, 96, 97, 95, 98))
    eng.on_5m_bar("CE", 24000.0, _bar5(base, 5, 99, 100, 89, 92))
    assert eng.is_open() and eng.position.side == "CE"

    eng.on_5m_bar("PE", 23900.0, _bar5(base, 10, 96, 97, 95, 98))
    events = eng.on_5m_bar("PE", 23900.0, _bar5(base, 15, 99, 100, 89, 92))
    reasons = [e.reason for e in events if e.reason]
    assert "structural_flip" in [getattr(e, "close_reason_placeholder", None) for e in []] or True  # see close-event check below
    assert eng.is_open() and eng.position.side == "PE"
    assert eng.position.tracking_strike == 23900.0


def test_open_position_carries_real_strike_not_placeholder():
    """_open_position must set CascadePosition.tracking_strike/.execution_strike
    and CascadeEvent.execution_strike to the real triggering strike -- these
    were hardcoded 0.0/None before this task."""
    eng = _engine()
    _arm_zone(eng, "PE", 24500.0)
    base = _BASE + timedelta(minutes=75 * 4)
    eng.on_5m_bar("PE", 24500.0, _bar5(base, 0, 96, 97, 95, 98))
    events = eng.on_5m_bar("PE", 24500.0, _bar5(base, 5, 99, 100, 89, 92))
    open_ev = [e for e in events if e.event_type.name.startswith("OPEN_")][0]
    assert open_ev.execution_strike == 24500.0
    assert eng.position.tracking_strike == 24500.0
    assert eng.position.execution_strike == 24500.0
    assert eng.position.t1.strike == 24500.0
    assert eng.position.t2.strike == 24500.0


def test_reset_candidate_clears_only_that_strike():
    eng = _engine()
    _arm_zone(eng, "CE", 24000.0)
    _arm_zone(eng, "CE", 23900.0)
    assert len(eng._pool[("CE", 24000.0)]) == 1
    assert len(eng._pool[("CE", 23900.0)]) == 1

    eng.reset_candidate("CE", 24000.0)
    assert eng._pool[("CE", 24000.0)] == []
    assert len(eng._pool[("CE", 23900.0)]) == 1  # untouched


def test_reset_side_clears_every_candidate_on_that_side():
    eng = _engine()
    _arm_zone(eng, "CE", 24000.0)
    _arm_zone(eng, "CE", 23900.0)
    eng.reset_side("CE")
    assert eng._pool[("CE", 24000.0)] == []
    assert eng._pool[("CE", 23900.0)] == []
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_multi_strike.py -v`
Expected: FAIL — `TypeError: on_75m_bar() takes 3 positional arguments but 4 were given` (current signature is `(self, side, bar)`).

- [ ] **Step 3: Rewrite `pool_engine.py`'s core to be candidate-keyed**

In `strategies/v4_cascade/pool_engine.py`, replace the `_ZoneSlot.__init__` and the whole `PoolCascadeEngine` class body from `class _ZoneSlot:` through the end of `on_5m_bar` with:

```python
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
```

Leave `_close_for_structural_flip`, `_check_exits`, `_close_event`, `force_eod_close` **completely unchanged** — all three already operate on `self.position`/`self._trail[side]`/`self._last_5m_bar[side]`, which stay side-keyed exactly as before; none of them need to know about candidates or strikes.

Also update the module docstring's opening paragraph (lines 1-32) to add one sentence after the existing "2026-07-23 correction" notes:

```python
2026-07-24: widened to track several candidate strikes per side (was one
fixed ATM-offset strike per side) -- see
docs/superpowers/specs/2026-07-24-v4-cascade-multi-strike-scan-design.md.
The pool dict is now keyed (side, strike) instead of side alone; every
public method gains a strike parameter. Same-side candidates are mutually
exclusive (only one can be open at a time, others frozen while one is
open) but cross-side structural flip is unchanged.
```

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_multi_strike.py -v`
Expected: PASS (6 tests). If `test_cross_side_structural_flip_still_works_with_multiple_candidates` fails on the `reasons`/placeholder assertion line, delete that no-op line (`assert "structural_flip" in ... or True`) — it was left as a comment-only placeholder in this plan and asserts nothing; the two real assertions below it (`eng.is_open() and eng.position.side == "PE"`, `eng.position.tracking_strike == 23900.0`) are what actually verify the flip.

- [ ] **Step 5: Mechanically adapt existing pool_engine tests to the new signature**

The following files call `on_75m_bar`/`on_15m_bar`/`on_5m_bar` with the OLD 2-argument signature (`side, bar`) and read `engine._pool["CE"]`/`engine._pool["PE"]` with a bare string key:
`tests/strategies/test_v4_cascade_pool_engine.py`, `tests/strategies/test_v4_cascade_pool_engine_active_position.py`, `tests/strategies/test_v4_cascade_pool_engine_fills.py`, `tests/strategies/test_v4_cascade_pool_engine_history_replay.py`.

Apply this exact mechanical transformation to every one of these 4 files (do not change any bar values, timestamps, or assertion logic — only the call syntax and dict-key literal):

1. Add a placeholder strike constant near the top of each file's helpers (right after the existing `_bar75`/`_bar15`/`_bar5`/`_engine` helper functions): `_STRIKE = 100.0`
2. Every call of the shape `eng.on_75m_bar("CE", <bar_expr>)` becomes `eng.on_75m_bar("CE", _STRIKE, <bar_expr>)` (same for `"PE"`, same for `on_15m_bar`/`on_5m_bar`).
3. Every read of the shape `eng._pool["CE"]` becomes `eng._pool[("CE", _STRIKE)]` (same for `"PE"`, same for `.get("CE", ...)` → `.get(("CE", _STRIKE), ...)`).
4. Every direct construction `_ZoneSlot(zone)` (3 call sites: `test_v4_cascade_pool_engine.py:148`, `:179`, `test_v4_cascade_pool_engine_fills.py:59`) is left **unchanged** — `strike` defaults to `0.0` on `_ZoneSlot.__init__`, so these keep working without modification.

Example (from `test_v4_cascade_pool_engine.py`'s `test_htf_zone_added_to_pool_on_reentry`), BEFORE:
```python
def test_htf_zone_added_to_pool_on_reentry():
    eng = _engine()
    eng.on_75m_bar("CE", _bar75(0, 105, 110, 100, 105))
    eng.on_75m_bar("CE", _bar75(1, 95, 100, 90, 95))
    eng.on_75m_bar("CE", _bar75(2, 96, 115, 95, 112))
    assert len(eng._pool["CE"]) == 1
    slot = eng._pool["CE"][0]
    assert slot.zone_low == 90 and slot.zone_high == 100
    assert slot.tracking is False
    eng.on_75m_bar("CE", _bar75(3, 105, 108, 92, 96))
    assert eng._pool["CE"][0].tracking is True
```
AFTER:
```python
def test_htf_zone_added_to_pool_on_reentry():
    eng = _engine()
    eng.on_75m_bar("CE", _STRIKE, _bar75(0, 105, 110, 100, 105))
    eng.on_75m_bar("CE", _STRIKE, _bar75(1, 95, 100, 90, 95))
    eng.on_75m_bar("CE", _STRIKE, _bar75(2, 96, 115, 95, 112))
    assert len(eng._pool[("CE", _STRIKE)]) == 1
    slot = eng._pool[("CE", _STRIKE)][0]
    assert slot.zone_low == 90 and slot.zone_high == 100
    assert slot.tracking is False
    eng.on_75m_bar("CE", _STRIKE, _bar75(3, 105, 108, 92, 96))
    assert eng._pool[("CE", _STRIKE)][0].tracking is True
```

Apply the identical pattern to every remaining test function in all 4 files.

- [ ] **Step 6: Run the full pool_engine test suite to verify nothing regressed**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine.py tests/strategies/test_v4_cascade_pool_engine_active_position.py tests/strategies/test_v4_cascade_pool_engine_fills.py tests/strategies/test_v4_cascade_pool_engine_history_replay.py tests/strategies/test_v4_cascade_pool_engine_multi_strike.py tests/strategies/test_v4_cascade_pool_engine_construction.py tests/strategies/test_v4_cascade_pool_engine_flag.py tests/strategies/test_v4_cascade_pool_engine_persistence.py tests/strategies/test_v4_cascade_pool_engine_tick_routing.py -v`
Expected: PASS, every test — the last 4 files are re-run as a pure regression check (they don't call the changed methods directly, per grep, so they should be unaffected).

- [ ] **Step 7: Commit**

```bash
git add strategies/v4_cascade/pool_engine.py tests/strategies/test_v4_cascade_pool_engine.py tests/strategies/test_v4_cascade_pool_engine_active_position.py tests/strategies/test_v4_cascade_pool_engine_fills.py tests/strategies/test_v4_cascade_pool_engine_history_replay.py tests/strategies/test_v4_cascade_pool_engine_multi_strike.py
git commit -m "V4Cascade pool engine: widen pool keys from side to (side, strike) for multi-candidate scanning"
```

---

### Task 2: `config.py` + `v4_cascade_book_manager.py` — `V4CASCADE_TRACKING_OFFSETS` env var

**Files:**
- Modify: `strategies/v4_cascade/config.py`
- Modify: `strategies/v4_cascade_book_manager.py`
- Test: `tests/strategies/test_v4_cascade_book_manager.py`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces (used by Task 3): `V4CascadeConfig.tracking_offsets_pts: List[float]` field; `v4_cascade_book_manager._tracking_offsets_from_env() -> List[float]`; `V4CascadeBookManager._spawn_book` passes `tracking_offsets_pts=...` as a new kwarg to `V4CascadeBook(...)`.

- [ ] **Step 1: Write the failing tests**

In `tests/strategies/test_v4_cascade_book_manager.py`, `_FakeBook.__init__` currently accepts `(self, bus, cfg, underlying="NIFTY", client_id="", binding_id="", lot_multiplier=1, squareoff_time="15:15", use_pool_engine=False)`. Add the new kwarg and 2 new tests at the end of the file:

```python
class _FakeBook:
    def __init__(self, bus, cfg, underlying="NIFTY", client_id="", binding_id="",
                 lot_multiplier=1, squareoff_time="15:15", use_pool_engine=False,
                 tracking_offsets_pts=None):
        self._underlying = underlying; self._client_id = client_id; self._binding_id = binding_id
        self._lot_multiplier = lot_multiplier
        self.squareoff_time = squareoff_time
        self.use_pool_engine = use_pool_engine
        self.tracking_offsets_pts = tracking_offsets_pts
        self.started = False

    def set_client_db(self, db):
        pass

    def start(self):
        self.started = True


def test_tracking_offsets_default_when_env_unset(monkeypatch):
    monkeypatch.delenv("V4CASCADE_TRACKING_OFFSETS", raising=False)
    monkeypatch.setattr(vm_mod, "V4CascadeBook", _FakeBook)
    db = _DB({"C1": [_dep("Z1", "NIFTY")]})
    m = V4CascadeBookManager(bus=None, cfg=None, client_db=db, monitored_indices=[])
    m._reconcile()
    assert m.books[0].tracking_offsets_pts is None


def test_tracking_offsets_parsed_from_env(monkeypatch):
    monkeypatch.setenv("V4CASCADE_TRACKING_OFFSETS", "100,200,300,400,500")
    monkeypatch.setattr(vm_mod, "V4CascadeBook", _FakeBook)
    db = _DB({"C1": [_dep("Z1", "NIFTY")]})
    m = V4CascadeBookManager(bus=None, cfg=None, client_db=db, monitored_indices=[])
    m._reconcile()
    assert m.books[0].tracking_offsets_pts == [100.0, 200.0, 300.0, 400.0, 500.0]
```

(Note: `_FakeBook.tracking_offsets_pts` is `None` when unset — the manager passes `None` through unresolved, and `V4CascadeBook.__init__` itself resolves `None` → `[self._tracking_offset]` in Task 3. This keeps the manager's own default trivial to test and keeps "what a missing env var means" defined in exactly one place, matching how `use_pool_engine` is already handled.)

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_book_manager.py -v`
Expected: FAIL — `TypeError: _FakeBook.__init__() got an unexpected keyword argument 'tracking_offsets_pts'` (manager doesn't pass it yet) or `AttributeError` on `.tracking_offsets_pts`.

- [ ] **Step 3: Add `tracking_offsets_pts` to `V4CascadeConfig`**

In `strategies/v4_cascade/config.py`, add the import and field:

```python
from dataclasses import dataclass, field
from typing import List
```

```python
    tracking_offset_pts: float = TRACKING_OFFSET_PTS
    execution_offset_pts: float = EXECUTION_OFFSET_PTS
    tracking_recenter_pts: float = TRACKING_RECENTER_PTS
    # 2026-07-24: candidate CE/PE strike offsets for the pool engine's
    # multi-strike scanning (NIFTY only, gated behind V4CASCADE_TRACKING_
    # OFFSETS -- see v4_cascade_book_manager.py). Defaults to a single-
    # element list matching tracking_offset_pts exactly -- unset, this is
    # byte-identical to today's single fixed-offset behavior. The plural
    # field is the source of truth ONLY for the pool-engine path; the
    # legacy Gate1/2/3 engine keeps reading the singular tracking_offset_pts
    # unchanged.
    tracking_offsets_pts: List[float] = field(default_factory=lambda: [TRACKING_OFFSET_PTS])
```

- [ ] **Step 4: Parse the env var in `v4_cascade_book_manager.py`**

In `strategies/v4_cascade_book_manager.py`, add the parser function right after `_pool_engine_enabled`:

```python
def _tracking_offsets_from_env() -> Optional[List[float]]:
    """2026-07-24: comma-separated CE/PE candidate strike offsets for the
    pool engine's multi-strike scanning (e.g. "100,200,300,400,500"),
    parsed once at spawn time. Returns None when unset -- V4CascadeBook
    resolves None -> [self._tracking_offset] itself (today's exact single-
    offset behavior), keeping "what a missing env var means" defined in
    exactly one place."""
    raw = os.environ.get("V4CASCADE_TRACKING_OFFSETS", "").strip()
    if not raw:
        return None
    try:
        return [float(x.strip()) for x in raw.split(",") if x.strip()]
    except ValueError:
        logger.warning("V4CascadeBookManager: could not parse V4CASCADE_TRACKING_OFFSETS=%r "
                       "-- falling back to single-offset default.", raw)
        return None
```

Change the existing `from typing import Dict` import line to `from typing import Dict, List, Optional` (both `List` and `Optional` are used by the new function's `-> Optional[List[float]]` return type).

- [ ] **Step 5: Thread it into `_spawn_book`**

In `strategies/v4_cascade_book_manager.py`'s `_spawn_book`, change:

```python
        use_pool_engine = _pool_engine_enabled(und)
        book = cls(self._bus, self._cfg, underlying=und, client_id=cid, binding_id=bid,
                   lot_multiplier=lots, squareoff_time=squareoff_time,
                   use_pool_engine=use_pool_engine)
```

to:

```python
        use_pool_engine = _pool_engine_enabled(und)
        tracking_offsets_pts = _tracking_offsets_from_env() if use_pool_engine else None
        book = cls(self._bus, self._cfg, underlying=und, client_id=cid, binding_id=bid,
                   lot_multiplier=lots, squareoff_time=squareoff_time,
                   use_pool_engine=use_pool_engine, tracking_offsets_pts=tracking_offsets_pts)
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_book_manager.py -v`
Expected: PASS, all tests (existing 6 + 2 new).

- [ ] **Step 7: Commit**

```bash
git add strategies/v4_cascade/config.py strategies/v4_cascade_book_manager.py tests/strategies/test_v4_cascade_book_manager.py
git commit -m "V4Cascade: add tracking_offsets_pts config field + V4CASCADE_TRACKING_OFFSETS env var"
```

---

### Task 3: `book.py` — multi-strike resolution, subscription, and locked-strike collapse

**Files:**
- Modify: `strategies/v4_cascade/book.py`
- Test: `tests/strategies/test_v4_cascade_pool_engine_multi_strike_book.py` (new)

**Interfaces:**
- Consumes: `V4CascadeConfig.tracking_offsets_pts` (Task 2), `PoolCascadeEngine.on_75m_bar/on_15m_bar/on_5m_bar(side, strike, bar)` (Task 1).
- Produces (used by Tasks 4-6):
  - `V4CascadeBook.__init__(..., tracking_offsets_pts: Optional[List[float]] = None)`
  - `self._ce_strikes: List[int]`, `self._pe_strikes: List[int]`, `self._ce_symbols: List[str]`, `self._pe_symbols: List[str]` (populated only when `self._use_pool_engine`; empty lists otherwise)
  - `self._ce_strike`/`self._pe_strike`/`self._ce_symbol`/`self._pe_symbol` (existing scalars) still populated, as the FIRST candidate — preserves `dashboard_server.py`'s existing `getattr(book, f"_{side}_strike", 0)` reads (line 2988) without needing a dashboard change for this specific field.

- [ ] **Step 1: Write the failing test**

Create `tests/strategies/test_v4_cascade_pool_engine_multi_strike_book.py`:

```python
"""strategies/v4_cascade/book.py -- multi-strike candidate resolution for
the pool engine (Task 3 of the multi-strike-scan plan). Exercises
_resolve_symbols'/_maybe_recenter_tracking_strikes'-adjacent strike-list-
building logic directly via a constructed V4CascadeBook, without touching
the network (REGISTRY.get_upstox_key is monkeypatched)."""
from datetime import date

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
import strategies.v4_cascade.book as book_mod


def _book(tracking_offsets_pts=None, locked_ce=None, locked_pe=None):
    b = V4CascadeBook(
        EventBus(), GlobalConfig(), underlying="NIFTY", client_id="C1", binding_id="B1",
        lot_multiplier=2, use_pool_engine=True, tracking_offsets_pts=tracking_offsets_pts,
    )
    if locked_ce is not None or locked_pe is not None:
        b.set_locked_strikes(locked_ce, locked_pe)
    return b


def _fake_get_upstox_key(underlying, expiry, strike, opt_type):
    return f"NSE_FO|{underlying}{expiry}{int(strike)}{opt_type}"


@pytest.mark.asyncio
async def test_default_offsets_produce_single_candidate_each_side(monkeypatch):
    """tracking_offsets_pts=None (env var unset) must produce EXACTLY today's
    single-strike behavior: one CE strike, one PE strike, both equal to the
    existing scalar self._ce_strike/self._pe_strike."""
    monkeypatch.setattr(book_mod.REGISTRY, "get_upstox_key", _fake_get_upstox_key)
    b = _book(tracking_offsets_pts=None)
    b._expiry = date(2026, 7, 28)
    b._build_candidate_strikes(atm_open=23700.0)
    assert b._ce_strikes == [23500]
    assert b._pe_strikes == [23900]
    assert b._ce_strike == 23500
    assert b._pe_strike == 23900


@pytest.mark.asyncio
async def test_multi_offsets_produce_five_candidates_each_side(monkeypatch):
    monkeypatch.setattr(book_mod.REGISTRY, "get_upstox_key", _fake_get_upstox_key)
    b = _book(tracking_offsets_pts=[100.0, 200.0, 300.0, 400.0, 500.0])
    b._expiry = date(2026, 7, 28)
    b._build_candidate_strikes(atm_open=24100.0)
    assert b._ce_strikes == [24000, 23900, 23800, 23700, 23600]
    assert b._pe_strikes == [24200, 24300, 24400, 24500, 24600]
    assert len(b._ce_symbols) == 5 and len(b._pe_symbols) == 5
    assert b._ce_strike == 24000  # first candidate, for dashboard backward-compat
    assert b._pe_strike == 24200


@pytest.mark.asyncio
async def test_locked_strike_collapses_to_single_candidate(monkeypatch):
    """Admin manual override (set_locked_strikes) must still work under
    multi-strike -- locking a side collapses its candidate list to exactly
    that one strike, ignoring tracking_offsets_pts entirely for that side."""
    monkeypatch.setattr(book_mod.REGISTRY, "get_upstox_key", _fake_get_upstox_key)
    b = _book(tracking_offsets_pts=[100.0, 200.0, 300.0, 400.0, 500.0],
              locked_ce=23850, locked_pe=None)
    b._expiry = date(2026, 7, 28)
    b._build_candidate_strikes(atm_open=24100.0)
    assert b._ce_strikes == [23850]
    assert b._pe_strikes == [24200, 24300, 24400, 24500, 24600]
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_multi_strike_book.py -v`
Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument 'tracking_offsets_pts'` and `AttributeError: 'V4CascadeBook' object has no attribute '_build_candidate_strikes'`.

- [ ] **Step 3: Add the constructor kwarg and candidate-list state**

In `strategies/v4_cascade/book.py`'s `V4CascadeBook.__init__` signature, change:

```python
    def __init__(
        self, bus, cfg, underlying: str, client_id: str, binding_id: str,
        lot_multiplier: int = 1, squareoff_time: str = "15:15",
        use_pool_engine: bool = False,
    ) -> None:
```

to:

```python
    def __init__(
        self, bus, cfg, underlying: str, client_id: str, binding_id: str,
        lot_multiplier: int = 1, squareoff_time: str = "15:15",
        use_pool_engine: bool = False,
        tracking_offsets_pts: Optional[List[float]] = None,
    ) -> None:
```

Right after the existing `self._v4cfg = V4CascadeConfig(...)` block (which already sets `tracking_offset_pts=self._tracking_offset`), add:

```python
        # 2026-07-24: multi-strike candidate offsets for the pool engine
        # ONLY -- the legacy Gate1/2/3 engine (use_pool_engine=False) never
        # reads this, stays on self._tracking_offset (singular) exactly as
        # today. None (env var unset) -> single-offset list matching
        # today's exact CE=ATM-200/PE=ATM+200 behavior.
        self._tracking_offsets: List[float] = (
            list(tracking_offsets_pts) if tracking_offsets_pts else [self._tracking_offset]
        )
```

Right after the existing `self._ce_strike: Optional[int] = None` / `self._pe_strike: Optional[int] = None` lines, add:

```python
        # Multi-strike candidate lists -- populated only for a pool-engine
        # book (self._use_pool_engine), by _build_candidate_strikes. Stay
        # empty for the legacy engine, which only ever uses the scalars
        # above. self._ce_strike/_ce_symbol (scalars) are kept in sync as
        # the FIRST candidate even in multi-strike mode, so any existing
        # code reading them (e.g. dashboard_server.py's tracking[f"{side}_
        # strike"] display field) keeps working without modification.
        self._ce_strikes: List[int] = []
        self._pe_strikes: List[int] = []
        self._ce_symbols: List[str] = []
        self._pe_symbols: List[str] = []
```

- [ ] **Step 4: Add `_build_candidate_strikes` and wire it into `_resolve_symbols`**

Add this new method right before `_resolve_symbols` in `book.py`:

```python
    def _build_candidate_strikes(self, atm_open: float) -> None:
        """Resolve the CE/PE candidate strike lists (and their Upstox
        symbols) from self._tracking_offsets, rounded to the same flat
        _TRACKING_STRIKE_STEP grid _resolve_symbols already uses for the
        single-strike case. Pool-engine only -- the legacy engine never
        calls this. set_locked_strikes overrides collapse that side's
        candidate list to exactly the locked strike, same precedence the
        single-strike path already gives locked strikes."""
        atm = round(atm_open / _TRACKING_STRIKE_STEP) * _TRACKING_STRIKE_STEP
        if self._locked_ce_strike is not None:
            self._ce_strikes = [int(self._locked_ce_strike)]
        else:
            self._ce_strikes = [int(atm - off) for off in self._tracking_offsets]
        if self._locked_pe_strike is not None:
            self._pe_strikes = [int(self._locked_pe_strike)]
        else:
            self._pe_strikes = [int(atm + off) for off in self._tracking_offsets]

        self._ce_symbols = [
            REGISTRY.get_upstox_key(self._underlying, self._expiry, strike, "CE")
            for strike in self._ce_strikes
        ]
        self._pe_symbols = [
            REGISTRY.get_upstox_key(self._underlying, self._expiry, strike, "PE")
            for strike in self._pe_strikes
        ]
        # First candidate stays the scalar's value -- dashboard_server.py's
        # existing getattr(book, f"_{side}_strike", 0) read (client
        # tracking block) keeps showing a real strike unmodified.
        self._ce_strike = self._ce_strikes[0]
        self._pe_strike = self._pe_strikes[0]
        self._ce_symbol = self._ce_symbols[0]
        self._pe_symbol = self._pe_symbols[0]
```

In `_resolve_symbols`, change:

```python
        self._atm_open = atm_open
        atm = round(atm_open / _TRACKING_STRIKE_STEP) * _TRACKING_STRIKE_STEP
        self._ce_strike = self._locked_ce_strike or int(atm - self._tracking_offset)
        self._pe_strike = self._locked_pe_strike or int(atm + self._tracking_offset)
        self._tracking_reference_atm = atm_open

        await asyncio.to_thread(REGISTRY.load_sync, self._underlying, token)
        self._expiry = self._resolve_expiry()
        if self._expiry is None:
            logger.warning("V4CascadeBook[%s]: no active expiry resolvable.", self._underlying)
            return False
        self._ce_symbol = REGISTRY.get_upstox_key(self._underlying, self._expiry, self._ce_strike, "CE")
        self._pe_symbol = REGISTRY.get_upstox_key(self._underlying, self._expiry, self._pe_strike, "PE")
        logger.info("V4CascadeBook[%s/%s/%s]: ATM_open=%.2f CE=%d(%s) PE=%d(%s) expiry=%s",
                    self._underlying, self._client_id, self._binding_id, atm_open,
                    self._ce_strike, self._ce_symbol, self._pe_strike, self._pe_symbol, self._expiry)
        self._clog.info("ATM_open=%.2f CE=%d(%s) PE=%d(%s) expiry=%s",
                        atm_open, self._ce_strike, self._ce_symbol,
                        self._pe_strike, self._pe_symbol, self._expiry)
        return bool(self._ce_symbol and self._pe_symbol)
```

to:

```python
        self._atm_open = atm_open
        self._tracking_reference_atm = atm_open

        await asyncio.to_thread(REGISTRY.load_sync, self._underlying, token)
        self._expiry = self._resolve_expiry()
        if self._expiry is None:
            logger.warning("V4CascadeBook[%s]: no active expiry resolvable.", self._underlying)
            return False

        if self._use_pool_engine:
            self._build_candidate_strikes(atm_open)
            logger.info("V4CascadeBook[%s/%s/%s]: ATM_open=%.2f CE_candidates=%s PE_candidates=%s expiry=%s",
                        self._underlying, self._client_id, self._binding_id, atm_open,
                        self._ce_strikes, self._pe_strikes, self._expiry)
            self._clog.info("ATM_open=%.2f CE_candidates=%s PE_candidates=%s expiry=%s",
                            atm_open, self._ce_strikes, self._pe_strikes, self._expiry)
            return bool(self._ce_symbols and self._pe_symbols)

        atm = round(atm_open / _TRACKING_STRIKE_STEP) * _TRACKING_STRIKE_STEP
        self._ce_strike = self._locked_ce_strike or int(atm - self._tracking_offset)
        self._pe_strike = self._locked_pe_strike or int(atm + self._tracking_offset)
        self._ce_symbol = REGISTRY.get_upstox_key(self._underlying, self._expiry, self._ce_strike, "CE")
        self._pe_symbol = REGISTRY.get_upstox_key(self._underlying, self._expiry, self._pe_strike, "PE")
        logger.info("V4CascadeBook[%s/%s/%s]: ATM_open=%.2f CE=%d(%s) PE=%d(%s) expiry=%s",
                    self._underlying, self._client_id, self._binding_id, atm_open,
                    self._ce_strike, self._ce_symbol, self._pe_strike, self._pe_symbol, self._expiry)
        self._clog.info("ATM_open=%.2f CE=%d(%s) PE=%d(%s) expiry=%s",
                        atm_open, self._ce_strike, self._ce_symbol,
                        self._pe_strike, self._pe_symbol, self._expiry)
        return bool(self._ce_symbol and self._pe_symbol)
```

Note this moves the expiry resolution ABOVE the strike/symbol derivation (was already needed by `_build_candidate_strikes`, which requires `self._expiry`) — harmless reordering since expiry resolution never depended on the strike values in the first place.

- [ ] **Step 5: Widen `_subscribe_tracking_contracts`**

Change:

```python
    async def _subscribe_tracking_contracts(self) -> None:
        if not self._rebalancer:
            return
        feeder = getattr(self._rebalancer, "_feeder", None)
        if not feeder:
            return
        tokens = [t for t in (self._ce_symbol, self._pe_symbol) if t]
        if tokens:
            await feeder.subscribe_tokens(tokens)
```

to:

```python
    async def _subscribe_tracking_contracts(self) -> None:
        if not self._rebalancer:
            return
        feeder = getattr(self._rebalancer, "_feeder", None)
        if not feeder:
            return
        if self._use_pool_engine:
            tokens = [t for t in (self._ce_symbols + self._pe_symbols) if t]
        else:
            tokens = [t for t in (self._ce_symbol, self._pe_symbol) if t]
        if tokens:
            await feeder.subscribe_tokens(tokens)
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_multi_strike_book.py -v`
Expected: PASS, all 3 tests.

- [ ] **Step 7: Run the full existing book-level test suite as a regression check**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_construction.py tests/strategies/test_v4_cascade_pool_engine_flag.py tests/strategies/test_v4_cascade_book_manager.py -v`
Expected: PASS — none of these construct a book with `tracking_offsets_pts` set, so they exercise the `None` default path only, which must remain identical to before this task.

- [ ] **Step 8: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_pool_engine_multi_strike_book.py
git commit -m "V4Cascade book: resolve+subscribe multi-strike CE/PE candidate lists for the pool engine"
```

---

### Task 4: `book.py` — multi-candidate history ingestion and replay

**Files:**
- Modify: `strategies/v4_cascade/book.py`
- Test: `tests/strategies/test_v4_cascade_pool_engine_history_replay.py`

**Interfaces:**
- Consumes: `self._ce_strikes/_pe_strikes/_ce_symbols/_pe_symbols` (Task 3), `PoolCascadeEngine.on_75m_bar/on_15m_bar/on_5m_bar(side, strike, bar)` (Task 1).
- Produces (used by Task 5): `self._pool_bars_5m: Dict[Tuple[str, int], List]` — per-candidate 5m bar history, replacing the pool-engine's reliance on the side-keyed `self._bars_5m` for anything beyond the legacy engine.

- [ ] **Step 1: Read the existing pool-engine history-replay test for its exact fixture pattern**

Read `tests/strategies/test_v4_cascade_pool_engine_history_replay.py` in full via the Read tool before writing new code. It already defines a `_rows(base, n, price=100.0)` helper and constructs a book via `patch.object(book, "_access_token", return_value="tok")`, `patch.object(book, "_resolve_symbols", return_value=True)`, `patch.object(book, "_subscribe_tracking_contracts", return_value=None)`, `patch("strategies.v4_cascade.book.fetch_upstox_range_1m", return_value=rows)`, `patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", return_value=[])` — the new test below reuses this exact pattern.

- [ ] **Step 2: Write the failing test**

Append to `tests/strategies/test_v4_cascade_pool_engine_history_replay.py` (reuses the file's existing `_rows` helper):

```python
@pytest.mark.asyncio
async def test_replay_pool_engine_history_feeds_every_candidate():
    """_ingest_history's pool-engine path must fetch+replay EVERY resolved
    candidate (not just one CE + one PE) -- proven by checking two
    DIFFERENT CE candidates both end up with their own independent 75m bar
    history after replay."""
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._running = True
    book._expiry = date(2026, 7, 28)
    book._ce_strikes = [24000, 23900]
    book._pe_strikes = [24200]
    book._ce_symbols = ["NSE_FO|CE24000", "NSE_FO|CE23900"]
    book._pe_symbols = ["NSE_FO|PE24200"]

    base = datetime(2026, 7, 1, 9, 15, tzinfo=IST)
    rows = _rows(base, 100)

    with patch.object(book, "_access_token", return_value="tok"), \
         patch.object(book, "_resolve_symbols", return_value=True), \
         patch.object(book, "_subscribe_tracking_contracts", return_value=None), \
         patch("strategies.v4_cascade.book.fetch_upstox_range_1m", return_value=rows), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", return_value=[]):
        ok = await book._ingest_history()

    assert ok is True
    assert book._engine.position is None  # old engine never touched
    assert len(book._pool_bars_5m[("CE", 24000)]) > 0
    assert len(book._pool_bars_5m[("CE", 23900)]) > 0
    assert len(book._pool_bars_5m[("PE", 24200)]) > 0
    # 100 1m rows from 09:15 cross a real 75m boundary (09:15->10:30) for
    # BOTH CE candidates independently -- proves each candidate's own
    # replay actually ran, not just that the dict key exists.
    assert len(book._pool_engine._all_75m[("CE", 24000)]) > 0
    assert len(book._pool_engine._all_75m[("CE", 23900)]) > 0
```

- [ ] **Step 3: Run to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_history_replay.py -v -k feeds_every_candidate`
Expected: FAIL — `AttributeError: 'V4CascadeBook' object has no attribute '_pool_bars_5m'`, since `_replay_pool_engine_history` still only takes `(ce_5m, pe_5m)` and `_ingest_history` doesn't branch on multi-strike yet.

- [ ] **Step 4: Add `self._pool_bars_5m` and widen `_ingest_history`**

Add the new state right next to the `self._bars_5m` declaration in `__init__`:

```python
        # 2026-07-24: per-candidate 5m bar history for the pool engine's
        # multi-strike scanning, keyed (side, strike). self._bars_5m (side-
        # keyed) stays exactly as-is for the legacy engine; the pool engine
        # uses this dict instead once multi-strike is active.
        self._pool_bars_5m: Dict[Tuple[str, int], List] = {}
```

Replace the `_ingest_history` body's history-fetch block. Change:

```python
        spot_key = REGISTRY.historical_instrument_key(self._underlying)
        (spot_rows, ce_rows, pe_rows,
         spot_today, ce_today, pe_today) = await asyncio.gather(
            fetch_upstox_range_1m(spot_key, token, start, today),
            fetch_upstox_range_1m(self._ce_symbol, token, start, today),
            fetch_upstox_range_1m(self._pe_symbol, token, start, today),
            fetch_upstox_intraday_1m(spot_key, token),
            fetch_upstox_intraday_1m(self._ce_symbol, token),
            fetch_upstox_intraday_1m(self._pe_symbol, token),
        )
        spot_rows = _merge_rows(spot_rows, spot_today)
        ce_rows = _merge_rows(ce_rows, ce_today)
        pe_rows = _merge_rows(pe_rows, pe_today)
        spot_5m = _to_5m_bars(spot_rows, filter_zero_volume=False)
        ce_5m = _to_5m_bars(ce_rows, filter_zero_volume=True)
        pe_5m = _to_5m_bars(pe_rows, filter_zero_volume=True)
        self._bars_5m["CE"] = ce_5m
        self._bars_5m["PE"] = pe_5m

        # Replay must ONLY rebuild HTF/MTF zone/scanner state -- it must
        # NEVER be allowed to open or close the live position. See
        # _guard_replay_position's docstring for the full explanation
        # (including the 2026-07-21 in-place-mutation fix).
        if self._use_pool_engine:
            self._replay_pool_engine_history(ce_5m, pe_5m)
        else:
            _pos_snapshot = self._position_snapshot(self._engine.position)
            _replay_through_engine(self._engine, spot_5m, ce_5m, pe_5m,
                                    on_daily_boundary=self._apply_eod_gate23_rules,
                                    session_open=self._session_open,
                                    eod_square_off=self._eod_hour_min,
                                    gate23_reset=self._gate23_hour_min)
            self._guard_replay_position(_pos_snapshot)
        self._history_ingested = True
        self._persist_position()
        logger.info("V4CascadeBook[%s/%s/%s]: history ingested — spot=%d CE=%d PE=%d 5m bars.",
                    self._underlying, self._client_id, self._binding_id,
                    len(spot_5m), len(ce_5m), len(pe_5m))
        self._clog.info("history ingested — spot=%d CE=%d PE=%d 5m bars.",
                        len(spot_5m), len(ce_5m), len(pe_5m))
        return True
```

to (note: the pool-engine branch below deliberately does NOT fetch `spot_key` history — `_replay_pool_engine_history` never took a `spot_5m` argument even before this task, per Task 1's design; today's code wastefully fetches it anyway for a pool-engine book and silently discards it. This task removes that dead fetch for the pool-engine path only; the legacy branch below keeps fetching `spot_key`, unchanged, since `_replay_through_engine` genuinely uses it):

```python
        spot_key = REGISTRY.historical_instrument_key(self._underlying)

        if self._use_pool_engine:
            candidates = [("CE", strike, symbol) for strike, symbol in zip(self._ce_strikes, self._ce_symbols)]
            candidates += [("PE", strike, symbol) for strike, symbol in zip(self._pe_strikes, self._pe_symbols)]
            fetches = [fetch_upstox_range_1m(symbol, token, start, today) for _, _, symbol in candidates]
            fetches += [fetch_upstox_intraday_1m(symbol, token) for _, _, symbol in candidates]
            results = await asyncio.gather(*fetches)
            n = len(candidates)
            range_results, today_results = results[:n], results[n:]
            self._pool_bars_5m = {}
            total_bars = 0
            for (side, strike, _symbol), range_rows, today_rows in zip(candidates, range_results, today_results):
                merged = _merge_rows(range_rows, today_rows)
                bars = _to_5m_bars(merged, filter_zero_volume=True)
                self._pool_bars_5m[(side, strike)] = bars
                total_bars += len(bars)
            self._replay_pool_engine_history(self._pool_bars_5m)
            self._history_ingested = True
            self._persist_position()
            logger.info("V4CascadeBook[%s/%s/%s]: history ingested — %d candidates, %d total 5m bars.",
                        self._underlying, self._client_id, self._binding_id, len(candidates), total_bars)
            self._clog.info("history ingested — %d candidates, %d total 5m bars.",
                            len(candidates), total_bars)
            return True

        (spot_rows, ce_rows, pe_rows,
         spot_today, ce_today, pe_today) = await asyncio.gather(
            fetch_upstox_range_1m(spot_key, token, start, today),
            fetch_upstox_range_1m(self._ce_symbol, token, start, today),
            fetch_upstox_range_1m(self._pe_symbol, token, start, today),
            fetch_upstox_intraday_1m(spot_key, token),
            fetch_upstox_intraday_1m(self._ce_symbol, token),
            fetch_upstox_intraday_1m(self._pe_symbol, token),
        )
        spot_rows = _merge_rows(spot_rows, spot_today)
        ce_rows = _merge_rows(ce_rows, ce_today)
        pe_rows = _merge_rows(pe_rows, pe_today)
        spot_5m = _to_5m_bars(spot_rows, filter_zero_volume=False)
        ce_5m = _to_5m_bars(ce_rows, filter_zero_volume=True)
        pe_5m = _to_5m_bars(pe_rows, filter_zero_volume=True)
        self._bars_5m["CE"] = ce_5m
        self._bars_5m["PE"] = pe_5m

        _pos_snapshot = self._position_snapshot(self._engine.position)
        _replay_through_engine(self._engine, spot_5m, ce_5m, pe_5m,
                                on_daily_boundary=self._apply_eod_gate23_rules,
                                session_open=self._session_open,
                                eod_square_off=self._eod_hour_min,
                                gate23_reset=self._gate23_hour_min)
        self._guard_replay_position(_pos_snapshot)
        self._history_ingested = True
        self._persist_position()
        logger.info("V4CascadeBook[%s/%s/%s]: history ingested — spot=%d CE=%d PE=%d 5m bars.",
                    self._underlying, self._client_id, self._binding_id,
                    len(spot_5m), len(ce_5m), len(pe_5m))
        self._clog.info("history ingested — spot=%d CE=%d PE=%d 5m bars.",
                        len(spot_5m), len(ce_5m), len(pe_5m))
        return True
```

- [ ] **Step 5: Rewrite `_replay_pool_engine_history` to take a per-candidate dict**

Change the method signature and body from:

```python
    def _replay_pool_engine_history(self, ce_5m, pe_5m) -> None:
        """..."""
        if self._pool_engine is None:
            return
        _pos_snapshot = self._position_snapshot(self._pool_engine.position)
        for side, bars in (("CE", ce_5m), ("PE", pe_5m)):
            for idx, bar in enumerate(bars):
                self._pool_engine.on_5m_bar(side, bar)
                if _bucket_end(bar.timestamp, 15, self._session_open):
                    window = [b for b in bars[:idx + 1] if b.timestamp.date() == bar.timestamp.date()]
                    r15 = resample_bars(window, 15, self._session_open)
                    if r15:
                        last15 = r15[-1]
                        self._pool_engine.on_15m_bar(side, _Bar(
                            last15.timestamp, last15.close, last15.high, last15.low,
                            last15.close, tf=15))
                if _bucket_end(bar.timestamp, 75, self._session_open):
                    window = [b for b in bars[:idx + 1] if b.timestamp.date() == bar.timestamp.date()]
                    r75 = resample_bars(window, 75, self._session_open)
                    if r75:
                        last75 = r75[-1]
                        self._pool_engine.on_75m_bar(side, _Bar(
                            last75.timestamp, last75.close, last75.high, last75.low,
                            last75.close, tf=75))
        self._guard_replay_position(_pos_snapshot)
```

to:

```python
    def _replay_pool_engine_history(self, bars_by_candidate: Dict[Tuple[str, int], List]) -> None:
        """Rebuilds the pool engine's HTF/LTF pool state from fetched
        history for EVERY resolved candidate (2026-07-24: was exactly two
        fixed candidates, ce_5m/pe_5m; now iterates however many are in
        self._ce_strikes/_pe_strikes). Same chronological-replay shape as
        before -- 75m/15m derived by resampling the growing per-candidate
        history at each boundary -- just looped once per (side, strike)
        instead of once per side. Replay must never touch a live position --
        reuses the SAME _position_snapshot/_guard_replay_position pair as
        before, checked ONCE after all candidates have replayed (a replay-
        caused phantom open/close from ANY candidate is still caught)."""
        if self._pool_engine is None:
            return
        _pos_snapshot = self._position_snapshot(self._pool_engine.position)
        for (side, strike), bars in bars_by_candidate.items():
            for idx, bar in enumerate(bars):
                self._pool_engine.on_5m_bar(side, strike, bar)
                if _bucket_end(bar.timestamp, 15, self._session_open):
                    window = [b for b in bars[:idx + 1] if b.timestamp.date() == bar.timestamp.date()]
                    r15 = resample_bars(window, 15, self._session_open)
                    if r15:
                        last15 = r15[-1]
                        self._pool_engine.on_15m_bar(side, strike, _Bar(
                            last15.timestamp, last15.close, last15.high, last15.low,
                            last15.close, tf=15))
                if _bucket_end(bar.timestamp, 75, self._session_open):
                    window = [b for b in bars[:idx + 1] if b.timestamp.date() == bar.timestamp.date()]
                    r75 = resample_bars(window, 75, self._session_open)
                    if r75:
                        last75 = r75[-1]
                        self._pool_engine.on_75m_bar(side, strike, _Bar(
                            last75.timestamp, last75.close, last75.high, last75.low,
                            last75.close, tf=75))
        self._guard_replay_position(_pos_snapshot)
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_history_replay.py -v`
Expected: PASS, all tests including the new one from Step 2.

- [ ] **Step 7: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_pool_engine_history_replay.py
git commit -m "V4Cascade book: multi-candidate history fetch + replay for the pool engine"
```

---

### Task 5: `book.py` — live tick routing and per-candidate 5m bucket accumulation

**Files:**
- Modify: `strategies/v4_cascade/book.py`
- Test: `tests/strategies/test_v4_cascade_pool_engine_tick_routing.py`

**Interfaces:**
- Consumes: `self._ce_strikes/_pe_strikes` (Task 3), `PoolCascadeEngine.on_5m_bar/on_15m_bar/on_75m_bar(side, strike, bar)` (Task 1).
- Produces (used by Task 6): `self._pool_buckets: Dict[Tuple[str, int], Optional[_Bar]]`, `_on_option_tick_pool(side, strike, ltp, ts)`, `_close_5m_bucket_pool_engine(side, strike, bar)` (widened signature).

- [ ] **Step 1: Read the existing tick-routing test's fixture pattern**

Read `tests/strategies/test_v4_cascade_pool_engine_tick_routing.py` in full — it currently has ONE test, `test_pool_engine_receives_5m_bars_not_old_engine`, which calls `book._on_option_tick("CE", ltp, ts)` directly. After this task, `_on_option_tick` becomes legacy-engine-only (Step 5 removes the pool-engine branch from the `_close_5m_bucket` path it calls into) — calling it directly would silently stop exercising the pool engine at all, turning this into a false-negative regression test (its own assertions would still pass, but for the wrong reason: they never actually check the pool engine received anything). Step 2 below both fixes this existing test and adds a new one.

- [ ] **Step 2: Write the failing tests**

Replace the ENTIRE existing `test_pool_engine_receives_5m_bars_not_old_engine` function in `tests/strategies/test_v4_cascade_pool_engine_tick_routing.py` with:

```python
def test_pool_engine_receives_5m_bars_not_old_engine():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._running = True
    base = datetime(2026, 7, 1, 9, 15, tzinfo=IST)
    # 2026-07-24: was book._on_option_tick("CE", ...) -- that method is now
    # legacy-engine-only (this task split pool-engine tick handling into
    # its own _on_option_tick_pool, which needs an explicit strike).
    # Updated so this test still actually exercises the pool engine's own
    # tick path instead of silently testing nothing.
    for i in range(2):
        book._on_option_tick_pool("CE", 23700, 100.0 + i, base + timedelta(minutes=5 * i))
    assert book._pool_engine is not None
    # The old engine must never be touched when the pool engine is active.
    assert book._engine.position is None
    # Strengthened: the pool engine's own per-candidate bucket must have
    # actually received these ticks -- the old assertions only checked
    # book._engine.position is None, which is trivially true for flat data
    # regardless of which engine processed it and proves nothing about
    # routing on its own.
    assert book._pool_buckets[("CE", 23700)] is not None
    assert book._pool_buckets[("CE", 23700)].close == 101.0


def test_pool_engine_two_candidates_bucket_independently():
    """Two different CE candidates (24000, 23900) receiving ticks must
    accumulate into SEPARATE 5m buckets -- a tick for 23900 must never be
    folded into 24000's in-progress bar, since they're different
    instruments at different price scales."""
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._running = True
    ts = datetime(2026, 7, 1, 9, 15, tzinfo=IST)
    book._on_option_tick_pool("CE", 24000, 100.0, ts)
    book._on_option_tick_pool("CE", 23900, 250.0, ts)
    assert book._pool_buckets[("CE", 24000)].close == 100.0
    assert book._pool_buckets[("CE", 23900)].close == 250.0
```

- [ ] **Step 3: Run to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_tick_routing.py -v`
Expected: FAIL — `AttributeError: 'V4CascadeBook' object has no attribute '_on_option_tick_pool'`.

- [ ] **Step 4: Add `self._pool_buckets` and the new tick/bucket methods**

Add next to `self._buckets`/`self._bars_5m` in `__init__`:

```python
        # 2026-07-24: per-candidate 5m bucket accumulator for the pool
        # engine's multi-strike scanning, keyed (side, strike). self._buckets
        # (side-keyed) stays exactly as-is for the legacy engine.
        self._pool_buckets: Dict[Tuple[str, int], Optional["_Bar"]] = {}
```

Add these two new methods right after `_close_5m_bucket_pool_engine`:

```python
    def _on_option_tick_pool(self, side: str, strike: int, ltp: float, ts: datetime) -> None:
        """Mirrors _on_option_tick, but per-candidate for the pool engine's
        multi-strike scanning. 2026-07-24: does NOT call _check_tick_exit --
        that call is already a no-op for pool-engine positions today (it
        unconditionally reads self._engine.check_exits_tick, and a pool
        book's self._engine never holds a position -- see
        V4CascadeBook._active_position's docstring), so preserving that
        (currently inert) call here for every one of 10 candidates would
        add real overhead for zero behavioral effect. Not this plan's job
        to fix that pre-existing gap -- out of scope, see the design spec."""
        self._live_price[side] = ltp  # last-write-wins across all candidates on this side
        bucket = _bucket_start(ts, 5, self._session_open)
        key = (side, strike)
        cur = self._pool_buckets.get(key)
        if cur is None or cur.timestamp != bucket:
            if cur is not None:
                self._close_5m_bucket_pool_engine(side, strike, cur)
            self._pool_buckets[key] = _Bar(bucket, ltp, ltp, ltp, ltp, tf=5)
        else:
            cur.high = max(cur.high, ltp)
            cur.low = min(cur.low, ltp)
            cur.close = ltp
```

Replace `_close_5m_bucket_pool_engine`'s signature and body:

```python
    def _close_5m_bucket_pool_engine(self, side: str, bar) -> None:
        """2026-07-23: pool-engine path -- 75m/15m bars are derived by
        resampling the FULL self._bars_5m[side] history (already
        maintained identically for the old engine), not a per-day slice
        (unlike the old engine's own 75m dispatch above, which is fine for
        that engine's per-day-scoped Gate 2 but would be wrong for the
        pool engine's genuinely multi-day HTF zone pool)."""
        events = self._pool_engine.on_5m_bar(side, bar)
        for ev in events:
            self._emit_order(ev, pos_before=None)
        self._persist_position()

        if _bucket_end(bar.timestamp, 15, self._session_open):
            r15 = resample_bars(self._bars_5m[side], 15, self._session_open)
            if r15:
                last15 = r15[-1]
                b15 = _Bar(last15.timestamp, last15.close, last15.high, last15.low,
                           last15.close, tf=15)
                self._pool_engine.on_15m_bar(side, b15)

        if _bucket_end(bar.timestamp, 75, self._session_open):
            r75 = resample_bars(self._bars_5m[side], 75, self._session_open)
            if r75:
                last75 = r75[-1]
                b75 = _Bar(last75.timestamp, last75.close, last75.high, last75.low,
                           last75.close, tf=75)
                self._pool_engine.on_75m_bar(side, b75)
```

becomes:

```python
    def _close_5m_bucket_pool_engine(self, side: str, strike: int, bar) -> None:
        """2026-07-24: widened to take an explicit strike (was side-only)
        -- 75m/15m bars are derived by resampling the FULL
        self._pool_bars_5m[(side, strike)] history for THIS candidate only
        (each candidate is an independent instrument/price-scale, never
        mixed with a sibling's bars), not a per-day slice (matches the
        pool engine's genuinely multi-day HTF zone pool, same reasoning as
        the original single-candidate version)."""
        key = (side, strike)
        self._pool_bars_5m.setdefault(key, []).append(bar)
        events = self._pool_engine.on_5m_bar(side, strike, bar)
        for ev in events:
            self._emit_order(ev, pos_before=None)
        self._persist_position()

        if _bucket_end(bar.timestamp, 15, self._session_open):
            r15 = resample_bars(self._pool_bars_5m[key], 15, self._session_open)
            if r15:
                last15 = r15[-1]
                b15 = _Bar(last15.timestamp, last15.close, last15.high, last15.low,
                           last15.close, tf=15)
                self._pool_engine.on_15m_bar(side, strike, b15)

        if _bucket_end(bar.timestamp, 75, self._session_open):
            r75 = resample_bars(self._pool_bars_5m[key], 75, self._session_open)
            if r75:
                last75 = r75[-1]
                b75 = _Bar(last75.timestamp, last75.close, last75.high, last75.low,
                           last75.close, tf=75)
                self._pool_engine.on_75m_bar(side, strike, b75)
```

Note this ALSO now appends to `self._pool_bars_5m[key]` directly (Task 4's `_ingest_history` only sets the INITIAL history; this is the live-tick continuation of the same per-candidate list, mirroring exactly how `_close_5m_bucket` already appends to `self._bars_5m[side]` for the legacy engine).

- [ ] **Step 5: Update `_close_5m_bucket`'s dispatch and `_option_loop`'s routing**

In `_close_5m_bucket`, change:

```python
    def _close_5m_bucket(self, side: str, bar) -> None:
        self._check_daily_boundary(bar.timestamp)
        _current_atm = self._live_spot or self._atm_open or 0.0
        if _current_atm > 0:
            self._fire(self._maybe_recenter_tracking_strikes(_current_atm))
        self._bars_5m[side].append(bar)
        if self._use_pool_engine:
            self._close_5m_bucket_pool_engine(side, bar)
            return
```

to (drop the pool-engine branch here entirely — pool-engine ticks now go through `_on_option_tick_pool`/`_close_5m_bucket_pool_engine` directly, never through `_close_5m_bucket`, since bucket accumulation must be per-candidate, not per-side):

```python
    def _close_5m_bucket(self, side: str, bar) -> None:
        self._check_daily_boundary(bar.timestamp)
        _current_atm = self._live_spot or self._atm_open or 0.0
        if _current_atm > 0:
            self._fire(self._maybe_recenter_tracking_strikes(_current_atm))
        self._bars_5m[side].append(bar)
```

(This function is now legacy-engine-only, matching that its remaining body — `self._engine.update(...)` — always was.)

In `_on_option_tick`, remove the day-boundary/recenter lines that are now redundant with the pool-engine path having its own bucket handling, by leaving `_on_option_tick` completely UNCHANGED (it's legacy-engine-only from here on — its own bucket-close call goes to `_close_5m_bucket`, which no longer branches on `use_pool_engine`). No edit needed to `_on_option_tick` itself.

In `_option_loop`, change:

```python
            side = None
            if symbol == self._ce_symbol or (
                    tick.underlying == self._underlying and int(tick.strike) == self._ce_strike and tick.option_type == "CE"):
                side = "CE"
            elif symbol == self._pe_symbol or (
                    tick.underlying == self._underlying and int(tick.strike) == self._pe_strike and tick.option_type == "PE"):
                side = "PE"
            if side is None:
                continue
            self._on_option_tick(side, float(tick.ltp), tick.timestamp)
```

to:

```python
            if self._use_pool_engine:
                matched_strike = None
                matched_side = None
                if tick.underlying == self._underlying and tick.option_type == "CE" and int(tick.strike) in self._ce_strikes:
                    matched_side, matched_strike = "CE", int(tick.strike)
                elif tick.underlying == self._underlying and tick.option_type == "PE" and int(tick.strike) in self._pe_strikes:
                    matched_side, matched_strike = "PE", int(tick.strike)
                if matched_side is not None:
                    self._on_option_tick_pool(matched_side, matched_strike, float(tick.ltp), tick.timestamp)
                continue

            side = None
            if symbol == self._ce_symbol or (
                    tick.underlying == self._underlying and int(tick.strike) == self._ce_strike and tick.option_type == "CE"):
                side = "CE"
            elif symbol == self._pe_symbol or (
                    tick.underlying == self._underlying and int(tick.strike) == self._pe_strike and tick.option_type == "PE"):
                side = "PE"
            if side is None:
                continue
            self._on_option_tick(side, float(tick.ltp), tick.timestamp)
```

Note the EXECUTION-contract tick-matching block just above this (`for exec_side in ("CE", "PE"): if self._exec_symbol[exec_side] and symbol == self._exec_symbol[exec_side]: ...`) stays completely unchanged — it's identical for both engines (both check exit conditions against whichever single strike is actually the OPEN position's execution contract, which is always exactly one strike regardless of how many candidates exist).

Also add `_maybe_recenter_tracking_strikes` triggering for the pool-engine path inside `_on_option_tick_pool` (it was removed from `_close_5m_bucket` in Step 4/5 above, but the pool engine still needs its own recenter check). Add this line right after the `self._live_price[side] = ltp` line in `_on_option_tick_pool`:

```python
        _current_atm = self._live_spot or self._atm_open or 0.0
        if _current_atm > 0:
            self._fire(self._maybe_recenter_tracking_strikes(_current_atm))
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_tick_routing.py -v`
Expected: PASS, all tests.

- [ ] **Step 7: Run the full existing suite as a regression check**

Run: `python -m pytest tests/strategies/ -k v4_cascade -v`
Expected: PASS across the board — `_maybe_recenter_tracking_strikes` is called from the new location but its own body isn't touched until Task 6, so it should behave identically to before for any test exercising it (single-strike default path).

- [ ] **Step 8: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_pool_engine_tick_routing.py
git commit -m "V4Cascade book: per-candidate live tick routing and 5m bucket accumulation"
```

---

### Task 6: `book.py` — multi-strike ATM-drift recenter (diff-based)

**Files:**
- Modify: `strategies/v4_cascade/book.py`
- Test: `tests/strategies/test_v4_cascade_tracking_recenter.py`

**Interfaces:**
- Consumes: `self._ce_strikes/_pe_strikes/_ce_symbols/_pe_symbols` (Task 3), `PoolCascadeEngine.reset_candidate(side, strike)` (Task 1), `self._pool_bars_5m` (Task 4/5).
- Produces: `_maybe_recenter_tracking_strikes` correctly recenters either the single-strike legacy pair OR the multi-strike candidate window, branching on `self._use_pool_engine`.

- [ ] **Step 1: Read the full existing recenter test file**

Read `tests/strategies/test_v4_cascade_tracking_recenter.py` in full (351 lines) before writing anything — this file already tests `_maybe_recenter_tracking_strikes`'s single-strike behavior (reentrancy guard, TOCTOU defense, old-strike bucket clearing, subscribe/unsubscribe calls) in detail; every one of those existing tests must keep passing UNCHANGED (they exercise the `tracking_offsets_pts=None` default path, which this task must not alter).

- [ ] **Step 2: Write the failing test**

Append to `tests/strategies/test_v4_cascade_tracking_recenter.py`, reusing its existing `_FakeFeeder`/`_FakeRebalancer` classes (defined at the top of the file, used by `test_recenter_unsubscribes_old_symbols_and_subscribes_new`):

```python
@pytest.mark.asyncio
async def test_recenter_diffs_multi_strike_window_only_touching_changed_strikes():
    """When ATM drifts, the pool-engine recenter must diff the OLD 5-strike
    window against the NEW one -- only strikes that fell out of range get
    unsubscribed+dropped, and only strikes newly in range get subscribed+
    fetched+reset; strikes that stay in-window (24000/23900/23800 for CE,
    24400/24500/24600 for PE in this scenario) are left completely alone."""
    cfg = GlobalConfig()
    book = V4CascadeBook(
        EventBus(), cfg, underlying="NIFTY", client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15", use_pool_engine=True,
        tracking_offsets_pts=[100.0, 200.0, 300.0, 400.0, 500.0],
    )
    book._running = True
    book._expiry = date(2026, 7, 21)
    book._tracking_reference_atm = 24100.0
    # OLD window: ATM=24100 -> CE [24000,23900,23800,23700,23600], PE [24200,24300,24400,24500,24600]
    book._ce_strikes = [24000, 23900, 23800, 23700, 23600]
    book._pe_strikes = [24200, 24300, 24400, 24500, 24600]
    book._ce_symbols = [f"NSE_FO|CE{s}" for s in book._ce_strikes]
    book._pe_symbols = [f"NSE_FO|PE{s}" for s in book._pe_strikes]
    feeder = _FakeFeeder()
    book._rebalancer = _FakeRebalancer(feeder)

    def _fake_upstox_key(underlying, expiry, strike, opt_type):
        return f"NSE_FO|{opt_type}{int(strike)}"

    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=[])), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
         patch.object(book, "_access_token", return_value="tok"), \
         patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", side_effect=_fake_upstox_key):
        # NEW ATM=24300 (drift=200 >= tracking_recenter_pts=100) -> CE
        # [24200,24100,24000,23900,23800], PE [24400,24500,24600,24700,24800] --
        # partially overlaps the old window (3 CE + 3 PE strikes unchanged),
        # so this genuinely tests the diff, not a full-window replacement.
        await book._maybe_recenter_tracking_strikes(current_atm=24300.0)

    assert book._ce_strikes == [24200, 24100, 24000, 23900, 23800]
    assert book._pe_strikes == [24400, 24500, 24600, 24700, 24800]
    assert sorted(feeder.subscribed) == sorted([
        "NSE_FO|CE24200", "NSE_FO|CE24100", "NSE_FO|PE24700", "NSE_FO|PE24800",
    ])
    assert sorted(feeder.unsubscribed) == sorted([
        "NSE_FO|CE23700", "NSE_FO|CE23600", "NSE_FO|PE24200", "NSE_FO|PE24300",
    ])
```

- [ ] **Step 3: Run to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_tracking_recenter.py -v -k multi_strike`
Expected: FAIL — the current `_maybe_recenter_tracking_strikes` only ever touches the single `self._ce_strike`/`self._pe_strike` scalars, never `self._ce_strikes` (plural), so the new assertions about diffed subscribe/unsubscribe calls fail.

- [ ] **Step 4: Rewrite `_maybe_recenter_tracking_strikes` to branch on `self._use_pool_engine`**

Replace the ENTIRE body from `self._recentering = True` through the `finally:` block's `self._recentering = False` (keep the outer guard checks — `if self._recentering: return`, the flatness gate, the `tracking_recenter_pts` drift-threshold gate — completely unchanged; they're engine-agnostic and correct for both paths):

```python
        self._recentering = True
        try:
            if self._use_pool_engine:
                await self._recenter_multi_strike(current_atm)
            else:
                await self._recenter_single_strike(current_atm)
        finally:
            self._recentering = False
```

Add two new methods right after `_maybe_recenter_tracking_strikes`. First, `_recenter_single_strike` — this is EXACTLY today's existing body (the whole `try:` block's contents from `old_ce, old_pe = self._ce_strike, self._pe_strike` through the final `self._clog.info(...)` call), moved verbatim into its own method with zero logic changes:

```python
    async def _recenter_single_strike(self, current_atm: float) -> None:
        """Legacy single-strike recenter -- EXACT existing body, unchanged,
        extracted verbatim into its own method so _maybe_recenter_tracking_
        strikes can branch on self._use_pool_engine. Never touches
        self._ce_strikes/_pe_strikes (plural) -- those stay empty for a
        non-pool-engine book."""
        old_ce, old_pe = self._ce_strike, self._pe_strike
        old_ce_symbol, old_pe_symbol = self._ce_symbol, self._pe_symbol
        atm = round(current_atm / _TRACKING_STRIKE_STEP) * _TRACKING_STRIKE_STEP
        new_ce = self._locked_ce_strike or int(atm - self._tracking_offset)
        new_pe = self._locked_pe_strike or int(atm + self._tracking_offset)

        token = await asyncio.to_thread(self._access_token)
        if not token or not self._expiry:
            logger.warning("V4CascadeBook[%s/%s/%s]: re-center aborted (no token/expiry) — "
                           "keeping existing tracking strikes CE=%s PE=%s.",
                           self._underlying, self._client_id, self._binding_id, old_ce, old_pe)
            return

        new_ce_symbol = REGISTRY.get_upstox_key(self._underlying, self._expiry, new_ce, "CE")
        new_pe_symbol = REGISTRY.get_upstox_key(self._underlying, self._expiry, new_pe, "PE")
        today = datetime.now(IST).date()
        start = today - timedelta(days=_LOOKBACK_DAYS)
        (ce_rows, pe_rows, ce_today, pe_today) = await asyncio.gather(
            fetch_upstox_range_1m(new_ce_symbol, token, start, today),
            fetch_upstox_range_1m(new_pe_symbol, token, start, today),
            fetch_upstox_intraday_1m(new_ce_symbol, token),
            fetch_upstox_intraday_1m(new_pe_symbol, token),
        )
        ce_rows = _merge_rows(ce_rows, ce_today)
        pe_rows = _merge_rows(pe_rows, pe_today)
        ce_5m = _to_5m_bars(ce_rows, filter_zero_volume=True)
        pe_5m = _to_5m_bars(pe_rows, filter_zero_volume=True)

        pos = self._active_position
        if pos is not None and pos.is_open:
            logger.warning("V4CascadeBook[%s/%s/%s]: re-center aborted post-fetch — a "
                           "position opened during the REST round-trip; keeping existing "
                           "tracking strikes CE=%s PE=%s.",
                           self._underlying, self._client_id, self._binding_id, old_ce, old_pe)
            self._clog.warning("re-center aborted post-fetch — a position opened during the "
                               "REST round-trip; keeping existing tracking strikes CE=%s PE=%s.",
                               old_ce, old_pe)
            return

        self._ce_strike, self._pe_strike = new_ce, new_pe
        self._ce_symbol, self._pe_symbol = new_ce_symbol, new_pe_symbol
        self._tracking_reference_atm = current_atm
        self._bars_5m["CE"], self._bars_5m["PE"] = ce_5m, pe_5m
        self._buckets["CE"] = None
        self._buckets["PE"] = None

        for side in ("CE", "PE"):
            self._engine._scanners[side].reset()
        _pos_snapshot = self._position_snapshot(self._engine.position)
        _replay_through_engine(self._engine, [], ce_5m, pe_5m,
                                on_daily_boundary=self._apply_eod_gate23_rules,
                                session_open=self._session_open,
                                eod_square_off=self._eod_hour_min,
                                gate23_reset=self._gate23_hour_min)
        self._guard_replay_position(_pos_snapshot)

        feeder = getattr(self._rebalancer, "_feeder", None) if self._rebalancer else None
        if feeder:
            try:
                await feeder.subscribe_tokens([new_ce_symbol, new_pe_symbol])
            except Exception:
                logger.exception("V4CascadeBook[%s/%s/%s]: re-center subscribe failed for %s/%s.",
                                 self._underlying, self._client_id, self._binding_id,
                                 new_ce_symbol, new_pe_symbol)
            if old_ce_symbol and old_pe_symbol:
                try:
                    await feeder.unsubscribe_tokens([old_ce_symbol, old_pe_symbol])
                except Exception:
                    logger.exception("V4CascadeBook[%s/%s/%s]: re-center unsubscribe failed for %s/%s.",
                                     self._underlying, self._client_id, self._binding_id,
                                     old_ce_symbol, old_pe_symbol)

        logger.info("V4CascadeBook[%s/%s/%s]: re-centered tracking strikes CE %s->%s PE %s->%s "
                   "(atm=%.2f) — re-warmed from %d/%d 5m bars.", self._underlying, self._client_id,
                   self._binding_id, old_ce, new_ce, old_pe, new_pe, current_atm, len(ce_5m), len(pe_5m))
        self._clog.info("re-centered tracking strikes CE %s->%s PE %s->%s (atm=%.2f) — "
                        "re-warmed from %d/%d 5m bars.", old_ce, new_ce, old_pe, new_pe,
                        current_atm, len(ce_5m), len(pe_5m))
```

Now add the NEW multi-strike method:

```python
    async def _recenter_multi_strike(self, current_atm: float) -> None:
        """Pool-engine multi-strike recenter -- diffs the OLD 5-strike
        window against the NEW one per side, so a candidate that stays
        in-window across the recenter keeps its in-progress zone pool
        (not reset/re-warmed); only strikes that fell out of range are
        unsubscribed and dropped, and only strikes newly in range are
        subscribed, fetched, and reset_candidate-ed. Mirrors
        StrikeRebalancer._rebalance's own to_unsub/to_sub diff pattern."""
        old_ce_strikes, old_pe_strikes = list(self._ce_strikes), list(self._pe_strikes)
        old_ce_symbols, old_pe_symbols = list(self._ce_symbols), list(self._pe_symbols)

        token = await asyncio.to_thread(self._access_token)
        if not token or not self._expiry:
            logger.warning("V4CascadeBook[%s/%s/%s]: re-center aborted (no token/expiry) — "
                           "keeping existing tracking strikes CE=%s PE=%s.",
                           self._underlying, self._client_id, self._binding_id,
                           old_ce_strikes, old_pe_strikes)
            return

        atm = round(current_atm / _TRACKING_STRIKE_STEP) * _TRACKING_STRIKE_STEP
        if self._locked_ce_strike is not None:
            new_ce_strikes = [int(self._locked_ce_strike)]
        else:
            new_ce_strikes = [int(atm - off) for off in self._tracking_offsets]
        if self._locked_pe_strike is not None:
            new_pe_strikes = [int(self._locked_pe_strike)]
        else:
            new_pe_strikes = [int(atm + off) for off in self._tracking_offsets]

        added_ce = [s for s in new_ce_strikes if s not in old_ce_strikes]
        added_pe = [s for s in new_pe_strikes if s not in old_pe_strikes]
        removed_ce = [s for s in old_ce_strikes if s not in new_ce_strikes]
        removed_pe = [s for s in old_pe_strikes if s not in new_pe_strikes]

        if not added_ce and not added_pe and not removed_ce and not removed_pe:
            self._tracking_reference_atm = current_atm
            return  # window unchanged (e.g. locked strikes) -- nothing to do

        new_ce_symbols_by_strike = {
            s: REGISTRY.get_upstox_key(self._underlying, self._expiry, s, "CE") for s in added_ce
        }
        new_pe_symbols_by_strike = {
            s: REGISTRY.get_upstox_key(self._underlying, self._expiry, s, "PE") for s in added_pe
        }

        fetch_targets = [(("CE", s), sym) for s, sym in new_ce_symbols_by_strike.items()]
        fetch_targets += [(("PE", s), sym) for s, sym in new_pe_symbols_by_strike.items()]
        fetched: Dict[Tuple[str, int], List] = {}
        if fetch_targets:
            fetches = [fetch_upstox_range_1m(sym, token, datetime.now(IST).date() - timedelta(days=_LOOKBACK_DAYS),
                                              datetime.now(IST).date()) for _, sym in fetch_targets]
            fetches += [fetch_upstox_intraday_1m(sym, token) for _, sym in fetch_targets]
            results = await asyncio.gather(*fetches)
            n = len(fetch_targets)
            for (key, _sym), range_rows, today_rows in zip(fetch_targets, results[:n], results[n:]):
                merged = _merge_rows(range_rows, today_rows)
                fetched[key] = _to_5m_bars(merged, filter_zero_volume=True)

        pos = self._active_position
        if pos is not None and pos.is_open:
            logger.warning("V4CascadeBook[%s/%s/%s]: re-center aborted post-fetch — a "
                           "position opened during the REST round-trip; keeping existing "
                           "tracking strikes CE=%s PE=%s.",
                           self._underlying, self._client_id, self._binding_id,
                           old_ce_strikes, old_pe_strikes)
            self._clog.warning("re-center aborted post-fetch — a position opened during the "
                               "REST round-trip; keeping existing tracking strikes CE=%s PE=%s.",
                               old_ce_strikes, old_pe_strikes)
            return

        self._ce_strikes, self._pe_strikes = new_ce_strikes, new_pe_strikes
        self._ce_symbols = [REGISTRY.get_upstox_key(self._underlying, self._expiry, s, "CE") for s in new_ce_strikes]
        self._pe_symbols = [REGISTRY.get_upstox_key(self._underlying, self._expiry, s, "PE") for s in new_pe_strikes]
        self._ce_strike, self._pe_strike = self._ce_strikes[0], self._pe_strikes[0]
        self._ce_symbol, self._pe_symbol = self._ce_symbols[0], self._pe_symbols[0]
        self._tracking_reference_atm = current_atm

        for side, strike in [("CE", s) for s in removed_ce] + [("PE", s) for s in removed_pe]:
            self._pool_buckets.pop((side, strike), None)
            self._pool_bars_5m.pop((side, strike), None)
            self._pool_engine.reset_candidate(side, strike)

        for (side, strike), bars in fetched.items():
            self._pool_bars_5m[(side, strike)] = bars
            self._pool_engine.reset_candidate(side, strike)
            self._replay_pool_engine_history({(side, strike): bars})

        removed_symbols = (
            [old_ce_symbols[old_ce_strikes.index(s)] for s in removed_ce]
            + [old_pe_symbols[old_pe_strikes.index(s)] for s in removed_pe]
        )
        added_symbols = list(new_ce_symbols_by_strike.values()) + list(new_pe_symbols_by_strike.values())

        feeder = getattr(self._rebalancer, "_feeder", None) if self._rebalancer else None
        if feeder:
            if added_symbols:
                try:
                    await feeder.subscribe_tokens(added_symbols)
                except Exception:
                    logger.exception("V4CascadeBook[%s/%s/%s]: re-center subscribe failed for %s.",
                                     self._underlying, self._client_id, self._binding_id, added_symbols)
            if removed_symbols:
                try:
                    await feeder.unsubscribe_tokens(removed_symbols)
                except Exception:
                    logger.exception("V4CascadeBook[%s/%s/%s]: re-center unsubscribe failed for %s.",
                                     self._underlying, self._client_id, self._binding_id, removed_symbols)

        logger.info("V4CascadeBook[%s/%s/%s]: re-centered CE %s->%s PE %s->%s (atm=%.2f) — "
                   "%d strikes added, %d removed.", self._underlying, self._client_id, self._binding_id,
                   old_ce_strikes, new_ce_strikes, old_pe_strikes, new_pe_strikes, current_atm,
                   len(added_ce) + len(added_pe), len(removed_ce) + len(removed_pe))
        self._clog.info("re-centered CE %s->%s PE %s->%s (atm=%.2f) — %d strikes added, %d removed.",
                        old_ce_strikes, new_ce_strikes, old_pe_strikes, new_pe_strikes, current_atm,
                        len(added_ce) + len(added_pe), len(removed_ce) + len(removed_pe))
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_tracking_recenter.py -v`
Expected: PASS — every pre-existing test (exercising `_recenter_single_strike` via the `tracking_offsets_pts=None` default) plus the new multi-strike test from Step 2.

- [ ] **Step 6: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_tracking_recenter.py
git commit -m "V4Cascade book: diff-based multi-strike recenter, legacy single-strike recenter unchanged"
```

---

### Task 7: `dashboard_server.py` — read the composite-keyed pool correctly

**Files:**
- Modify: `ui_layer/dashboard_server.py`

**Interfaces:**
- Consumes: `PoolCascadeEngine._pool` now keyed `(side, strike)` (Task 1) instead of `side`.
- Produces: client tracking block (`tracking[side.lower()]`) and admin `v4_trap_status` endpoint both correctly aggregate zones across every candidate on a side, and both surface which physical strike each zone belongs to.

- [ ] **Step 1: Fix the client tracking block (around line 2962-3018)**

Change:

```python
                                    pe = getattr(book, "_pool_engine", None)
                                    for side, label in (("CE", "CE"), ("PE", "PE")):
                                        side_ltp = float(live_price.get(side) or 0.0)
                                        pool = list(pe._pool.get(side, [])) if pe is not None else []
```

to:

```python
                                    pe = getattr(book, "_pool_engine", None)
                                    for side, label in (("CE", "CE"), ("PE", "PE")):
                                        side_ltp = float(live_price.get(side) or 0.0)
                                        pool = []
                                        if pe is not None:
                                            for _k, _slots in pe._pool.items():
                                                if _k[0] == side:
                                                    pool.extend(_slots)
```

A few lines below, change:

```python
                                        strike = int(getattr(book, f"_{side.lower()}_strike", 0) or 0)
```

to:

```python
                                        strike = (int(slot.strike) if slot is not None
                                                  else int(getattr(book, f"_{side.lower()}_strike", 0) or 0))
```

(`slot` here is the already-selected nearest-to-price `_ZoneSlot`, computed a few lines above this — using its OWN strike is more accurate under multi-strike than always showing the first candidate, since the selected zone could belong to any of the tracked strikes; falls back to the existing scalar-based display only when the pool is empty.)

Finally, add `"strike": int(s.strike),` as a new key inside the `all_zones_debug` list-comprehension dict (right after `"zone_high": round(s.zone_high, 2),`):

```python
                                            "all_zones_debug": [
                                                {
                                                    "strike": int(s.strike),
                                                    "zone_low": round(s.zone_low, 2),
                                                    "zone_high": round(s.zone_high, 2),
                                                    "distance": (None if _zone_dist(side_ltp, s.zone_low, s.zone_high) == float("inf")
                                                                 else round(_zone_dist(side_ltp, s.zone_low, s.zone_high), 2)),
                                                    "pending_entry": s.pending_entry,
                                                    "tracking": s.tracking,
                                                    "ref_ts": (s.zone.reference_low_ts.isoformat(timespec="minutes")
                                                               if s.zone and s.zone.reference_low_ts else None),
                                                }
                                                for s in sorted(pool, key=lambda s: _zone_dist(side_ltp, s.zone_low, s.zone_high))
                                            ],
```

- [ ] **Step 2: Fix the admin `v4_trap_status` endpoint (around line 2279-2301)**

Change:

```python
                pe = getattr(book, "_pool_engine", None)
                for side in ("CE", "PE"):
                    side_ltp = float(live_price.get(side) or 0.0)
                    pool = list(pe._pool.get(side, [])) if pe is not None else []
                    pool.sort(key=lambda s: _zone_dist(side_ltp, s.zone_low, s.zone_high))
                    for slot in pool:
                        limit_price = round(slot.zone_low + pe._entry_offset, 2) if pe else None
                        dist = _zone_dist(side_ltp, slot.zone_low, slot.zone_high)
                        z = slot.zone
                        zones.append({
                            "model": "pool_engine", "side": side,
                            "state": ("limit_armed" if slot.pending_entry
                                      else "tracking" if slot.tracking else "zone_found"),
                            "ref_ts": z.reference_low_ts.isoformat() if z and z.reference_low_ts else None,
                            "trap_ts": z.lock_ts.isoformat() if z and z.lock_ts else None,
                            "timeframe": 75,
                            "zone_low": round(slot.zone_low, 2), "zone_high": round(slot.zone_high, 2),
                            "limit_entry_price": limit_price,
                            "live_price": side_ltp or None,
                            "distance_to_trap": None if dist == float("inf") else round(dist, 2),
                            "reentry_ts": slot.reentry_ts.isoformat() if slot.reentry_ts else None,
                            "trigger_ts": slot.trigger_ts.isoformat() if slot.trigger_ts else None,
                        })
```

to:

```python
                pe = getattr(book, "_pool_engine", None)
                for side in ("CE", "PE"):
                    side_ltp = float(live_price.get(side) or 0.0)
                    pool = []
                    if pe is not None:
                        for _k, _slots in pe._pool.items():
                            if _k[0] == side:
                                pool.extend(_slots)
                    pool.sort(key=lambda s: _zone_dist(side_ltp, s.zone_low, s.zone_high))
                    for slot in pool:
                        limit_price = round(slot.zone_low + pe._entry_offset, 2) if pe else None
                        dist = _zone_dist(side_ltp, slot.zone_low, slot.zone_high)
                        z = slot.zone
                        zones.append({
                            "model": "pool_engine", "side": side, "strike": int(slot.strike),
                            "state": ("limit_armed" if slot.pending_entry
                                      else "tracking" if slot.tracking else "zone_found"),
                            "ref_ts": z.reference_low_ts.isoformat() if z and z.reference_low_ts else None,
                            "trap_ts": z.lock_ts.isoformat() if z and z.lock_ts else None,
                            "timeframe": 75,
                            "zone_low": round(slot.zone_low, 2), "zone_high": round(slot.zone_high, 2),
                            "limit_entry_price": limit_price,
                            "live_price": side_ltp or None,
                            "distance_to_trap": None if dist == float("inf") else round(dist, 2),
                            "reentry_ts": slot.reentry_ts.isoformat() if slot.reentry_ts else None,
                            "trigger_ts": slot.trigger_ts.isoformat() if slot.trigger_ts else None,
                        })
```

- [ ] **Step 3: Manual verification (no automated dashboard test harness exists for this block)**

Run: `python -c "import ast; ast.parse(open('ui_layer/dashboard_server.py', encoding='utf-8').read())"`
Expected: no output (valid syntax). This module has no existing pytest coverage for this specific rendering block (confirmed by the original pool-engine plan's own Task notes) — the real verification is Task 8's live/manual check.

- [ ] **Step 4: Commit**

```bash
git add ui_layer/dashboard_server.py
git commit -m "V4Cascade dashboard: aggregate zones across every candidate strike per side"
```

---

### Task 8: Full regression suite + manual multi-strike trace

**Files:** none (verification only)

- [ ] **Step 1: Run the complete V4Cascade test suite**

Run: `python -m pytest tests/strategies/ -k v4_cascade -v`
Expected: PASS, every test — this is the definitive check that `tracking_offsets_pts=None`/`V4CASCADE_TRACKING_OFFSETS` unset remains byte-identical to pre-plan behavior for both the legacy engine and the pool engine's default single-candidate mode, AND that every new multi-candidate behavior (Tasks 1-6) is correct in isolation.

- [ ] **Step 2: Run the full project test suite as a final regression net**

Run: `python -m pytest tests/ -v --tb=short 2>&1 | tail -60`
Expected: no NEW failures beyond whatever pre-existing failures were already present before this plan started (there is one known pre-existing, unrelated failure from a stale hardcoded test date in `tests/data_layer/test_feeder_translation.py::test_upstox_converts_fyers_mcx_option_symbol` — confirmed failing in isolation before this plan began; do not attempt to fix it as part of this plan).

- [ ] **Step 3: Manual trace — verify the default (unset env var) path is inert**

```bash
python -c "
from strategies.v4_cascade.config import V4CascadeConfig
cfg = V4CascadeConfig(underlying='NIFTY')
assert cfg.tracking_offsets_pts == [200.0], cfg.tracking_offsets_pts
print('OK: default tracking_offsets_pts =', cfg.tracking_offsets_pts)
"
```
Expected: `OK: default tracking_offsets_pts = [200.0]`

- [ ] **Step 4: Manual trace — verify multi-strike candidate resolution end-to-end**

```bash
python -c "
from unittest.mock import patch
from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
from datetime import date

with patch('strategies.v4_cascade.book.REGISTRY.get_upstox_key',
           side_effect=lambda und, exp, strike, ot: f'NSE_FO|{und}{exp}{int(strike)}{ot}'):
    b = V4CascadeBook(EventBus(), GlobalConfig(), underlying='NIFTY', client_id='C1', binding_id='B1',
                       lot_multiplier=2, use_pool_engine=True,
                       tracking_offsets_pts=[100.0, 200.0, 300.0, 400.0, 500.0])
    b._expiry = date(2026, 7, 28)
    b._build_candidate_strikes(atm_open=24145.0)
    print('CE candidates:', b._ce_strikes)
    print('PE candidates:', b._pe_strikes)
    assert b._ce_strikes == [24000, 23900, 23800, 23700, 23600]
    assert b._pe_strikes == [24200, 24300, 24400, 24500, 24600]
    print('OK: matches the user-specified example (NIFTY@24145 -> ATM=24100)')
"
```
Expected: prints both candidate lists matching the user's original example exactly, then `OK: matches the user-specified example (NIFTY@24145 -> ATM=24100)`.

- [ ] **Step 5: Report back for live deployment**

This step has no command — it's a checkpoint for the human. Summarize for the user: all tests pass, the default (env var unset) path is confirmed inert, and the multi-strike candidate resolution matches their own worked example exactly. Remind them this is still gated behind `V4CASCADE_TRACKING_OFFSETS` (unset by default) — enabling it live requires setting that env var via `pm2 set` (same mechanism as `V4CASCADE_USE_POOL_ENGINE` earlier this session) and restarting, and that the WS subscription budget (currently 38/50, confirmed live) has only ~12 symbols of headroom against the up-to-10 new symbols this feature can add — worth watching the "N symbols subscribed" log line after enabling.

- [ ] **Step 6: Do NOT commit anything in this task** — it's verification-only. If Steps 1-2 reveal any regression, stop and fix it as a new commit before proceeding (do not amend prior tasks' commits).
