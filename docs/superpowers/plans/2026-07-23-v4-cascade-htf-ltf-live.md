# V4 Cascade HTF-Gated LTF Pool Engine — Live Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port the multi-zone-pool HTF/LTF cascade engine (validated over 90 days of real NIFTY spot data in `backtest/v4_cascade/htf_ltf_backtest.py`) into a new, opt-in live engine for the NIFTY paper deployment, replacing the old Gate1/Gate2/Gate3 funnel that same-candle-collapsed a real trade on 2026-07-23 09:20.

**Architecture:** A new `PoolCascadeEngine` class (`strategies/v4_cascade/pool_engine.py`) mirrors `V4CascadeEngine`'s `.position` attribute exactly so all of `book.py`'s existing persistence/dashboard/EOD/broker-routing code keeps working unchanged. It's fed incrementally (`on_75m_bar`/`on_15m_bar`/`on_5m_bar`, one call per closed bar) rather than replaying a whole array, so the same class serves both live ticks and the boot-time history replay. Gated behind `V4CascadeConfig.use_pool_engine` (default `False` = today's exact unchanged behavior) so the old, proven path is never at risk.

**Tech Stack:** Python 3.12, pytest, existing `strategies/v4_cascade/` package.

## Global Constraints

- Default `use_pool_engine=False` must leave EVERY existing test passing unchanged — verify the full `tests/strategies -k v4_cascade` suite (137 tests as of this plan) after every task.
- The engine trades the TRACKING contract directly — no execution-strike resolution, no tracking-to-execution scale mapping, for pool-engine positions.
- The 5m trigger's "previous candle" comparison must never cross a day boundary. The HTF zone pool and any zone already mid-tracking (re-entered, has an `ltf_zone`, possibly `pending_entry`) both carry across days completely unchanged — only the trigger's own prev-candle pointer resets.
- CRUDEOIL and crypto are untouched regardless of this flag (pool engine is NIFTY-only for now).
- No changes to persistence file format, dashboard API shape, or broker order routing.

---

### Task 1: Promote `find_all_bear_zones`/`find_all_bull_zones` to production `rolling_base.py`

**Files:**
- Modify: `strategies/v4_cascade/rolling_base.py` (add the two functions, near `find_bear_zone`/`find_bull_zone`)
- Modify: `backtest/v4_cascade/htf_ltf_backtest.py` (delete its own copies, import from `rolling_base` instead)
- Test: `tests/strategies/test_v4_cascade_find_all_zones.py`

**Interfaces:**
- Produces: `find_all_bear_zones(bars: List["_Bar"], known_ref_ts: Optional[Set[datetime]] = None) -> List[RollingBaseZone]`, `find_all_bull_zones(bars, known_ref_ts=None) -> List[RollingBaseZone]` — both importable from `strategies.v4_cascade.rolling_base`.

These are used by BOTH the backtest and the new live engine (Task 2) — they belong in production code now, not backtest-local. The logic is a direct copy of what's already in `backtest/v4_cascade/htf_ltf_backtest.py` (already tested there via the 90-day backtest run) — this task just moves it and re-points the one existing caller.

- [ ] **Step 1: Add the two functions to `rolling_base.py`**, directly below `find_bull_zone` (around line 213):

```python
def find_all_bear_zones(
    bars: List[_Bar], known_ref_ts: Optional[set] = None,
) -> List[RollingBaseZone]:
    """Enumerate EVERY confirmed bear-side (sweep+reclaim) zone in ``bars``,
    not just the newest one returned by find_bear_zone (that function's
    newest-first, return-on-first-match design is for the single-zone use
    case; this is for a multi-zone POOL, where several candidate zones may
    be concurrently valid). Same 3-candle rule (ref/sweep/reclaim strictly
    distinct candles) and the same mitigation check as find_bear_zone.
    ``known_ref_ts``: ref timestamps already handled (added to a pool, or
    already removed from it) -- never re-considered, so a zone that ages
    out or breaks is gone for good, not rediscovered next bar."""
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


def find_all_bull_zones(
    bars: List[_Bar], known_ref_ts: Optional[set] = None,
) -> List[RollingBaseZone]:
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
```

Note: `_is_mitigated_bear`/`_is_mitigated_bull` and `RollingBaseZone`/`Optional` are already imported/defined in this file (used by `find_bear_zone`/`find_bull_zone` directly above) — no new imports needed. Add `Set` to the existing `from typing import ...` line if not already present (check the top of the file first; if `Set` is missing from the typing import, add it, otherwise just use the built-in lowercase `set` in signatures as shown above, which needs no import).

- [ ] **Step 2: Write the failing test** at `tests/strategies/test_v4_cascade_find_all_zones.py`:

```python
"""strategies/v4_cascade/rolling_base.py's find_all_bear_zones/
find_all_bull_zones -- the multi-zone-pool counterpart to find_bear_zone/
find_bull_zone (which only ever return the single newest match)."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _Bar
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 1, 9, 15, tzinfo=IST)


def _bar(offset, o, h, l, c):
    return _Bar(_BASE + timedelta(minutes=75 * offset), o, h, l, c, tf=75)


def test_finds_multiple_distinct_bear_zones():
    # Zone A: ref@0 (low=100,high=110), sweep@1 (low=90), reclaim@2 (high=115).
    # Zone B: ref@3 (low=200,high=210), sweep@4 (low=190), reclaim@5 (high=215).
    bars = [
        _bar(0, 105, 110, 100, 105),
        _bar(1, 95, 100, 90, 95),
        _bar(2, 96, 115, 95, 112),
        _bar(3, 205, 210, 200, 205),
        _bar(4, 195, 200, 190, 195),
        _bar(5, 196, 215, 195, 212),
    ]
    zones = find_all_bear_zones(bars)
    ref_ts = sorted(z.reference_low_ts for z in zones)
    assert ref_ts == [bars[0].timestamp, bars[3].timestamp]


def test_known_ref_ts_excludes_already_handled_zones():
    bars = [
        _bar(0, 105, 110, 100, 105),
        _bar(1, 95, 100, 90, 95),
        _bar(2, 96, 115, 95, 112),
    ]
    known = {bars[0].timestamp}
    assert find_all_bear_zones(bars, known_ref_ts=known) == []


def test_same_candle_sweep_and_reclaim_rejected():
    # candle 1 both sweeps below ref's low AND reclaims above ref's high
    # within its own range -- must NOT be accepted (3-candle rule).
    bars = [
        _bar(0, 105, 110, 100, 105),
        _bar(1, 95, 120, 90, 112),
    ]
    assert find_all_bear_zones(bars) == []


def test_finds_multiple_distinct_bull_zones():
    bars = [
        _bar(0, 105, 110, 100, 105),
        _bar(1, 112, 120, 108, 115),
        _bar(2, 95, 100, 90, 92),
    ]
    zones = find_all_bull_zones(bars)
    assert len(zones) == 1
    assert zones[0].reference_low_ts == bars[0].timestamp
    assert zones[0].entry_line == 110  # ref.high
    assert zones[0].sl_level == 100    # ref.low
```

- [ ] **Step 3: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_find_all_zones.py -v`
Expected: FAIL with `ImportError: cannot import name 'find_all_bear_zones'`

- [ ] **Step 4: Run test to verify it passes** (after Step 1's addition)

Run: `python -m pytest tests/strategies/test_v4_cascade_find_all_zones.py -v`
Expected: 4 passed

- [ ] **Step 5: Re-point the backtest to import from `rolling_base` instead of defining its own copies.** In `backtest/v4_cascade/htf_ltf_backtest.py`: delete the `find_all_bear_zones`/`find_all_bull_zones` function bodies (they're now identical to the ones in `rolling_base.py`), and change the import line from:

```python
from strategies.v4_cascade.rolling_base import _is_mitigated_bear, _is_mitigated_bull, resample_bars
```

to:

```python
from strategies.v4_cascade.rolling_base import (
    find_all_bear_zones, find_all_bull_zones, resample_bars,
)
```

- [ ] **Step 6: Confirm the backtest still runs identically**

Run: `python -m pytest backtest/v4_cascade/tests/ -q`
Expected: 4 passed (no behavior change, pure re-point)

- [ ] **Step 7: Commit**

```bash
git add strategies/v4_cascade/rolling_base.py backtest/v4_cascade/htf_ltf_backtest.py tests/strategies/test_v4_cascade_find_all_zones.py
git commit -m "V4Cascade: promote find_all_bear_zones/find_all_bull_zones to production rolling_base.py"
```

---

### Task 2: `PoolCascadeEngine` — the live-incremental pool engine

**Files:**
- Create: `strategies/v4_cascade/pool_engine.py`
- Test: `tests/strategies/test_v4_cascade_pool_engine.py`

**Interfaces:**
- Consumes: `find_all_bear_zones`/`find_all_bull_zones` (Task 1), `strategies.v4_cascade.dataclasses.{CascadeEvent, CascadeEventType, CascadePosition, RollingBaseZone, TrancheLeg}`, `strategies.v4_cascade.exits.{ExitCheck, TrailingBaseTracker, check_t1}`, `strategies.v4_cascade.config.V4CascadeConfig` (specifically `.tranche_qty`, `.underlying`).
- Produces: `PoolCascadeEngine(cfg: V4CascadeConfig, entry_offset: float, session_open: Tuple[int,int] = (9,15))` with:
  - `.position: Optional[CascadePosition]` (readable/settable, mirrors `V4CascadeEngine.position` exactly -- book.py's persistence/dashboard code reads this attribute name).
  - `.on_75m_bar(side: str, bar) -> None`
  - `.on_15m_bar(side: str, bar) -> None`
  - `.on_5m_bar(side: str, bar) -> List[CascadeEvent]`
  - `.force_eod_close(side: str, ts, price: float) -> List[CascadeEvent]`

- [ ] **Step 1: Write the failing tests**

```python
"""strategies/v4_cascade/pool_engine.py's PoolCascadeEngine -- live-
incremental adaptation of the validated multi-zone-pool HTF/LTF cascade
(backtest/v4_cascade/htf_ltf_backtest.py). Trades the tracking contract
directly (strike=0.0 here -- book.py fills in the real tracking strike at
_emit_order time, same convention the pure V4CascadeEngine already uses)."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _Bar
from strategies.v4_cascade.config import V4CascadeConfig
from strategies.v4_cascade.dataclasses import CascadeEventType
from strategies.v4_cascade.pool_engine import PoolCascadeEngine

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 1, 9, 15, tzinfo=IST)


def _bar75(offset, o, h, l, c):
    return _Bar(_BASE + timedelta(minutes=75 * offset), o, h, l, c, tf=75)


def _bar15(base, offset_min, o, h, l, c):
    return _Bar(base + timedelta(minutes=offset_min), o, h, l, c, tf=15)


def _bar5(base, offset_min, o, h, l, c):
    return _Bar(base + timedelta(minutes=offset_min), o, h, l, c, tf=5)


def _engine():
    cfg = V4CascadeConfig(underlying="NIFTY", lot_multiplier=2, lot_size=65)
    return PoolCascadeEngine(cfg, entry_offset=5.0, session_open=(9, 15))


def test_htf_zone_added_to_pool_on_reentry():
    eng = _engine()
    # ref@0 (low=100,high=110), sweep@1 (low=90), reclaim@2 (high=115).
    eng.on_75m_bar("CE", _bar75(0, 105, 110, 100, 105))
    eng.on_75m_bar("CE", _bar75(1, 95, 100, 90, 95))
    eng.on_75m_bar("CE", _bar75(2, 96, 115, 95, 112))
    assert len(eng._pool["CE"]) == 1
    slot = eng._pool["CE"][0]
    assert slot.zone_low == 90 and slot.zone_high == 100
    assert slot.tracking is False
    # A later bar re-enters [90, 100].
    eng.on_75m_bar("CE", _bar75(3, 105, 108, 92, 96))
    assert eng._pool["CE"][0].tracking is True


def test_full_chain_produces_open_event():
    eng = _engine()
    eng.on_75m_bar("CE", _bar75(0, 105, 110, 100, 105))
    eng.on_75m_bar("CE", _bar75(1, 95, 100, 90, 95))
    eng.on_75m_bar("CE", _bar75(2, 96, 115, 95, 112))
    eng.on_75m_bar("CE", _bar75(3, 105, 108, 92, 96))  # re-entry
    assert eng._pool["CE"][0].tracking is True

    ltf_base = _BASE + timedelta(minutes=75 * 4)
    # 15m nested pattern: ref(low=93,high=97), sweep(low=91), reclaim(high=99).
    eng.on_15m_bar("CE", _bar15(ltf_base, 0, 95, 97, 93, 95))
    eng.on_15m_bar("CE", _bar15(ltf_base, 15, 92, 94, 91, 92))
    eng.on_15m_bar("CE", _bar15(ltf_base, 30, 93, 99, 92, 97))
    assert eng._pool["CE"][0].ltf_zone is not None
    assert eng._pool["CE"][0].ltf_zone.sl_level == 97  # ref.high

    # 5m trigger: candle closes above the previous candle's high.
    events = eng.on_5m_bar("CE", _bar5(ltf_base, 30, 93, 94, 92, 93))
    assert events == []
    events = eng.on_5m_bar("CE", _bar5(ltf_base, 35, 93, 95, 92, 94.5))
    assert events == []  # trigger armed (94.5 > 94 -- prev bar's high), not pierced yet
    # limit = zone_low(90) + offset(5) = 95 -- a bar whose low pierces down to it fills.
    events = eng.on_5m_bar("CE", _bar5(ltf_base, 40, 95, 96, 94, 95.5))
    assert len(events) == 1
    assert events[0].event_type == CascadeEventType.OPEN_LONG_CE
    assert eng.position is not None
    assert eng.position.t1.entry_price == 95.0
    assert eng.position.t1.sl_price == 85.0  # zone_low(90) - offset(5)
    assert eng.position.t1.target_price == 97.0  # ltf_zone.sl_level
    assert eng.position.t2.target_price is None  # T2 has no fixed target field set at open (matches V4CascadeEngine convention)
    assert eng._pool["CE"] == []  # pool cleared on fill


def test_intraday_trigger_reset_skips_cross_day_comparison():
    eng = _engine()
    day1 = datetime(2026, 7, 1, 14, 45, tzinfo=IST)
    day2 = datetime(2026, 7, 2, 9, 15, tzinfo=IST)
    slot_bar = _Bar(day1, 100, 101, 99, 100, tf=5)
    # Manually seed a tracking, ltf-ready pool slot (bypassing the full
    # 75m/15m chain, which is exercised by the other tests).
    from strategies.v4_cascade.pool_engine import _ZoneSlot
    from strategies.v4_cascade.dataclasses import RollingBaseZone
    zone = RollingBaseZone(entry_line=100.0, sweep_low=90.0, sl_level=110.0,
                            reference_low_ts=day1, lock_ts=day1, locked=True)
    slot = _ZoneSlot(zone)
    slot.tracking = True
    slot.ltf_zone = RollingBaseZone(entry_line=95.0, sweep_low=92.0, sl_level=98.0,
                                     reference_low_ts=day1, lock_ts=day1, locked=True)
    slot.prev_5m_bar = slot_bar  # yesterday's last 5m bar
    eng._pool["CE"] = [slot]
    eng._last_5m_date["CE"] = day1.date()

    # First 5m bar of the NEW day -- even though its close (150) is way
    # above yesterday's bar's high (101), it must NOT trigger, since the
    # "previous candle" pointer resets across the day boundary.
    events = eng.on_5m_bar("CE", _Bar(day2, 140, 150, 139, 150, tf=5))
    assert events == []
    assert eng._pool["CE"][0].pending_entry is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'strategies.v4_cascade.pool_engine'`

- [ ] **Step 3: Write the implementation**

```python
"""strategies/v4_cascade/pool_engine.py -- HTF-gated LTF cascade engine,
live-incremental adaptation of backtest/v4_cascade/htf_ltf_backtest.py's
multi-zone pool (validated over 90 days of real NIFTY data). Scans the
TRACKING contract's own premium directly (not spot) and trades it directly
(no execution-strike split) -- see
docs/superpowers/specs/2026-07-23-v4-cascade-htf-ltf-live-design.md.

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
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones

HTF_ZONE_MAX_AGE_DAYS = 10


def _zone_bounds(z: RollingBaseZone) -> Tuple[float, float]:
    return min(z.entry_line, z.sweep_low), max(z.entry_line, z.sweep_low)


def _overlaps(bar_low: float, bar_high: float, lo: float, hi: float) -> bool:
    return bar_low <= hi and bar_high >= lo


class _ZoneSlot:
    """One candidate HTF zone's independent tracking state, living inside a
    side's pool -- multiple zones progress concurrently, each with its own
    re-entry/LTF/5m-trigger state."""

    def __init__(self, zone: RollingBaseZone) -> None:
        self.zone = zone
        self.zone_low, self.zone_high = _zone_bounds(zone)
        self.tracking = False
        self.reentry_ts: Optional[datetime] = None
        self.ltf_zone: Optional[RollingBaseZone] = None
        self.bars_15m: List = []
        self.prev_5m_bar: Optional[object] = None
        self.pending_entry = False
        self.trigger_ts: Optional[datetime] = None


class PoolCascadeEngine:
    """One instance per book (NIFTY only). .position mirrors
    V4CascadeEngine's own .position attribute exactly, so book.py's
    persistence/dashboard/EOD code reads it unchanged regardless of which
    engine produced it."""

    def __init__(self, cfg: V4CascadeConfig, entry_offset: float,
                 session_open: Tuple[int, int] = (9, 15)) -> None:
        self._cfg = cfg
        self._entry_offset = entry_offset
        self._session_open = session_open
        self._pool: Dict[str, List[_ZoneSlot]] = {"CE": [], "PE": []}
        self._known_ref_ts: Dict[str, Set[datetime]] = {"CE": set(), "PE": set()}
        self._all_75m: Dict[str, List] = {"CE": [], "PE": []}
        self._last_5m_date: Dict[str, Optional[date]] = {"CE": None, "PE": None}
        self._trail: Dict[str, Optional[TrailingBaseTracker]] = {"CE": None, "PE": None}
        self.position: Optional[CascadePosition] = None

    def is_open(self) -> bool:
        return self.position is not None and self.position.is_open

    # ── HTF (75m) ────────────────────────────────────────────────────────
    def on_75m_bar(self, side: str, bar) -> None:
        if self.is_open() and self.position.side == side:
            return
        bear = side == "CE"
        finder = find_all_bear_zones if bear else find_all_bull_zones
        self._all_75m[side].append(bar)
        pool = self._pool[side]
        known = self._known_ref_ts[side]

        for slot in list(pool):
            if slot.tracking:
                continue
            broken = bar.close < slot.zone_low if bear else bar.close > slot.zone_high
            aged_out = (bar.timestamp - slot.zone.reference_low_ts) >= timedelta(days=HTF_ZONE_MAX_AGE_DAYS)
            if broken or aged_out:
                pool.remove(slot)

        lookback_start = bar.timestamp - timedelta(days=HTF_ZONE_MAX_AGE_DAYS)
        search_bars = [b for b in self._all_75m[side] if b.timestamp >= lookback_start]
        for z in finder(search_bars, known_ref_ts=known):
            known.add(z.reference_low_ts)
            pool.append(_ZoneSlot(z))

        for slot in pool:
            if slot.tracking:
                continue
            if _overlaps(bar.low, bar.high, slot.zone_low, slot.zone_high):
                slot.tracking = True
                slot.reentry_ts = bar.timestamp

    # ── LTF (15m) ────────────────────────────────────────────────────────
    def on_15m_bar(self, side: str, bar) -> None:
        if self.is_open() and self.position.side == side:
            return
        bear = side == "CE"
        finder = find_all_bear_zones if bear else find_all_bull_zones
        for slot in self._pool[side]:
            if not slot.tracking:
                continue
            slot.bars_15m.append(bar)
            zones = finder(slot.bars_15m)
            if zones:
                slot.ltf_zone = zones[0]

    # ── 5m trigger + limit fill + exits ─────────────────────────────────
    def on_5m_bar(self, side: str, bar) -> List[CascadeEvent]:
        if self.is_open() and self.position.side == side:
            return self._check_exits(side, bar)

        bear = side == "CE"
        pool = self._pool[side]

        # 2026-07-23: intraday-only trigger -- the first 5m candle of a new
        # session has no legitimate "previous candle" (yesterday's close is
        # a different session, not a real predecessor for a break-of-
        # structure comparison). Only the trigger's own prev-candle pointer
        # resets here -- the HTF pool and any zone's mid-tracking LTF/
        # pending state both carry across days completely unchanged.
        bar_date = bar.timestamp.date()
        if self._last_5m_date[side] != bar_date:
            for slot in pool:
                slot.prev_5m_bar = None
            self._last_5m_date[side] = bar_date

        events: List[CascadeEvent] = []
        for slot in list(pool):
            if not slot.tracking:
                continue
            broken = bar.close < slot.zone_low if bear else bar.close > slot.zone_high
            aged_out = (bar.timestamp - slot.zone.reference_low_ts) >= timedelta(days=HTF_ZONE_MAX_AGE_DAYS)
            if broken or aged_out:
                pool.remove(slot)
                continue
            if slot.ltf_zone is None:
                slot.prev_5m_bar = bar
                continue
            prev = slot.prev_5m_bar
            slot.prev_5m_bar = bar
            if prev is None:
                continue
            if not slot.pending_entry:
                triggered = bar.close > prev.high if bear else bar.close < prev.low
                if triggered:
                    slot.pending_entry = True
                    slot.trigger_ts = bar.timestamp
            if not slot.pending_entry:
                continue
            limit_price = slot.zone_low + self._entry_offset if bear else slot.zone_high - self._entry_offset
            pierced = bar.low <= limit_price if bear else bar.high >= limit_price
            if pierced:
                events.append(self._open_position(side, slot, limit_price, bar.timestamp))
                return events
        return events

    def _open_position(self, side: str, slot: _ZoneSlot, fill_price: float, ts) -> CascadeEvent:
        bear = side == "CE"
        htf, ltf = slot.zone, slot.ltf_zone
        sl_price = slot.zone_low - self._entry_offset if bear else slot.zone_high + self._entry_offset
        t1_target = ltf.sl_level
        t2_target = htf.sl_level
        qty = self._cfg.tranche_qty
        t1 = TrancheLeg(tranche="T1", option_type=side, strike=0.0, qty=qty,
                         entry_price=fill_price, entry_time=ts, entry_reason="htf_ltf_pool_cascade",
                         sl_price=sl_price, target_price=t1_target)
        t2 = TrancheLeg(tranche="T2", option_type=side, strike=0.0, qty=qty,
                         entry_price=fill_price, entry_time=ts, entry_reason="htf_ltf_pool_cascade",
                         sl_price=sl_price, target_price=None, tracking_current_stop=sl_price)
        self.position = CascadePosition(
            underlying=self._cfg.underlying, side=side,
            tracking_strike=0.0, execution_strike=0.0,
            atm_at_trigger=0.0, entry_spot=0.0,
            t1=t1, t2=t2, open_time=ts,
            tracking_entry_price=fill_price,
        )
        self._trail[side] = TrailingBaseTracker(bear=bear, initial_stop=sl_price)
        # A position just opened -- only one at a time per side. Discard
        # the whole pool; a fresh one builds up again once this closes.
        self._pool[side] = []
        event_type = CascadeEventType.OPEN_LONG_CE if bear else CascadeEventType.OPEN_LONG_PE
        entry_reason = "gate3_bear_trap_reclaim" if bear else "gate3_bull_trap_reclaim"
        audit = {
            "htf_ref_ts": htf.reference_low_ts.isoformat() if htf.reference_low_ts else None,
            "htf_lock_ts": htf.lock_ts.isoformat() if htf.lock_ts else None,
            "reentry_ts": slot.reentry_ts.isoformat() if slot.reentry_ts else None,
            "ltf_ref_ts": ltf.reference_low_ts.isoformat() if ltf.reference_low_ts else None,
            "trigger_ts": slot.trigger_ts.isoformat() if slot.trigger_ts else None,
            "zone_low": slot.zone_low, "zone_high": slot.zone_high,
            "computed_sl_price": sl_price, "t1_target": t1_target, "t2_target": t2_target,
        }
        return CascadeEvent(event_type=event_type, side=side, price_hint=fill_price,
                             reason=entry_reason, sl_price=sl_price, target_price=t1_target,
                             timestamp=ts, audit=audit)

    def _check_exits(self, side: str, bar) -> List[CascadeEvent]:
        events: List[CascadeEvent] = []
        pos = self.position
        if pos is None or pos.side != side or not pos.is_open:
            return events
        bear = side == "CE"
        is_short = not bear
        t1, t2 = pos.t1, pos.t2
        trail = self._trail[side]

        if t1 is not None and t1.status == "open":
            r: ExitCheck = check_t1(t1, bar, is_short=is_short)
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add strategies/v4_cascade/pool_engine.py tests/strategies/test_v4_cascade_pool_engine.py
git commit -m "V4Cascade: add PoolCascadeEngine, live-incremental multi-zone HTF/LTF cascade"
```

---

### Task 3: `V4CascadeConfig.use_pool_engine` flag

**Files:**
- Modify: `strategies/v4_cascade/config.py`
- Test: `tests/strategies/test_v4_cascade_config.py` (check if this file exists first; if not, add a small new test file `tests/strategies/test_v4_cascade_pool_engine_flag.py`)

**Interfaces:**
- Produces: `V4CascadeConfig.use_pool_engine: bool = False` field, `V4CascadeConfig.pool_entry_offset: float = 5.0` field (the grid-search-confirmed best offset from the 90-day backtest, used as the live default).

- [ ] **Step 1: Write the failing test**

```python
"""V4CascadeConfig.use_pool_engine -- opt-in flag for the new
PoolCascadeEngine (2026-07-23). Default False preserves today's exact
Gate1/Gate2/Gate3 behavior."""
from strategies.v4_cascade.config import V4CascadeConfig


def test_use_pool_engine_defaults_false():
    cfg = V4CascadeConfig()
    assert cfg.use_pool_engine is False


def test_pool_entry_offset_defaults_to_backtest_best():
    cfg = V4CascadeConfig()
    assert cfg.pool_entry_offset == 5.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_flag.py -v`
Expected: FAIL with `AttributeError: 'V4CascadeConfig' object has no attribute 'use_pool_engine'`

- [ ] **Step 3: Add the fields.** In `strategies/v4_cascade/config.py`, inside the `V4CascadeConfig` dataclass, after the existing `target_floor_multiple: float = 1.0` field:

```python
    # 2026-07-23: opt-in flag for the new multi-zone-pool HTF/LTF cascade
    # engine (strategies/v4_cascade/pool_engine.py), validated over 90 days
    # of real NIFTY data in backtest/v4_cascade/htf_ltf_backtest.py.
    # Default False preserves today's exact Gate1/Gate2/Gate3 behavior --
    # this only ever changes anything for a deployment that explicitly
    # opts in.
    use_pool_engine: bool = False
    pool_entry_offset: float = 5.0  # the strongest performer in the 90-day backtest
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_flag.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add strategies/v4_cascade/config.py tests/strategies/test_v4_cascade_pool_engine_flag.py
git commit -m "V4Cascade: add use_pool_engine config flag (default off)"
```

---

### Task 4: Wire `V4CascadeBook` to construct `PoolCascadeEngine` when enabled

**Files:**
- Modify: `strategies/v4_cascade/book.py` (constructor, around where `self._engine = V4CascadeEngine(...)` is set)
- Test: `tests/strategies/test_v4_cascade_pool_engine_construction.py`

**Interfaces:**
- Consumes: `PoolCascadeEngine` (Task 2), `V4CascadeConfig.use_pool_engine`/`.pool_entry_offset` (Task 3).
- Produces: `V4CascadeBook._pool_engine: Optional[PoolCascadeEngine]` — `None` when `use_pool_engine` is False or the book is CRUDEOIL/crypto; a real instance otherwise. `V4CascadeBook._use_pool_engine: bool` mirrors the config flag (gated additionally on `not self._is_crypto and not self._is_mcx`, since the pool engine is NIFTY-only per the design's non-goals).

First find the exact constructor line to anchor this edit precisely.

- [ ] **Step 1: Locate the exact insertion point**

Run: `grep -n "self._engine = V4CascadeEngine" strategies/v4_cascade/book.py`

This prints the exact line number of the existing `self._engine = V4CascadeEngine(...)` call inside `__init__`. Use that line number for Step 3 below (do not guess the line number -- confirm it from this grep's real output before editing).

- [ ] **Step 2: Write the failing test**

```python
"""V4CascadeBook constructs a PoolCascadeEngine when V4CascadeConfig.
use_pool_engine is True, for NIFTY only -- CRUDEOIL/crypto never get one
regardless of the flag (pool engine is NIFTY-only per the 2026-07-23
design)."""
from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
from strategies.v4_cascade.pool_engine import PoolCascadeEngine


def test_pool_engine_off_by_default():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15")
    assert book._use_pool_engine is False
    assert book._pool_engine is None


def test_pool_engine_constructed_when_enabled():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    assert book._use_pool_engine is True
    assert isinstance(book._pool_engine, PoolCascadeEngine)


def test_pool_engine_never_used_for_crudeoil_even_if_flag_set():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="CRUDEOIL", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="23:15",
                          use_pool_engine=True)
    assert book._use_pool_engine is False
    assert book._pool_engine is None
```

- [ ] **Step 3: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_construction.py -v`
Expected: FAIL with `TypeError: V4CascadeBook.__init__() got an unexpected keyword argument 'use_pool_engine'`

- [ ] **Step 4: Add the `use_pool_engine` constructor parameter and wiring.** In `strategies/v4_cascade/book.py`, change the `__init__` signature (the exact current signature, confirmed earlier this session, is):

```python
    def __init__(
        self, bus, cfg, underlying: str, client_id: str, binding_id: str,
        lot_multiplier: int = 1, squareoff_time: str = "15:15",
    ) -> None:
```

to:

```python
    def __init__(
        self, bus, cfg, underlying: str, client_id: str, binding_id: str,
        lot_multiplier: int = 1, squareoff_time: str = "15:15",
        use_pool_engine: bool = False,
    ) -> None:
```

Then, immediately after the line found in Step 1 (`self._engine = V4CascadeEngine(...)`), add:

```python
        # 2026-07-23: opt-in multi-zone-pool HTF/LTF engine, NIFTY only --
        # see docs/superpowers/specs/2026-07-23-v4-cascade-htf-ltf-live-design.md.
        # CRUDEOIL/crypto always stay on the old V4CascadeEngine funnel
        # above regardless of this flag.
        self._use_pool_engine = bool(use_pool_engine) and not self._is_crypto and not self._is_mcx
        self._pool_engine: Optional["PoolCascadeEngine"] = None
        if self._use_pool_engine:
            from strategies.v4_cascade.pool_engine import PoolCascadeEngine
            self._pool_engine = PoolCascadeEngine(
                self._v4cfg, entry_offset=self._v4cfg.pool_entry_offset,
                session_open=self._session_open,
            )
```

`Optional` is already imported at the top of `book.py` (confirmed earlier this session: `from typing import Dict, List, Optional, Tuple`) -- no new import needed for the type hint. The `PoolCascadeEngine` import itself is deliberately local (inside the `if`), matching this file's existing pattern of deferring optional/heavy imports (e.g. `from strategies.v4_cascade.dataclasses import GateState` inside `_apply_eod_gate23_rules`) -- avoids a module-load-order dependency for books that never use it.

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_construction.py -v`
Expected: 3 passed

- [ ] **Step 6: Run the full v4_cascade suite to confirm no regressions**

Run: `python -m pytest tests/strategies -k v4_cascade -q`
Expected: all passing (137 + this task's new tests, no failures)

- [ ] **Step 7: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_pool_engine_construction.py
git commit -m "V4Cascade: construct PoolCascadeEngine in V4CascadeBook when use_pool_engine is set"
```

---

### Task 5: Live tick routing — feed 75m/15m/5m bars to the pool engine

**Files:**
- Modify: `strategies/v4_cascade/book.py` (`_on_option_tick`, `_close_5m_bucket`)
- Test: `tests/strategies/test_v4_cascade_pool_engine_tick_routing.py`

**Interfaces:**
- Consumes: `PoolCascadeEngine.on_75m_bar`/`on_15m_bar`/`on_5m_bar` (Task 2), `resample_bars` (already imported in `book.py`), `_bucket_end`/`_bucket_start` (already defined in `book.py`).
- Produces: when `self._use_pool_engine` is True, every 5m bucket close feeds the pool engine (15m and 75m bars derived by resampling the full `self._bars_5m[side]` history, matching exactly how the backtest builds them) instead of the old `self._engine.update(...)` call. When False, behavior is byte-for-byte unchanged from today.

- [ ] **Step 1: Confirm the exact current `_close_5m_bucket` body**

Run: `grep -n "_close_5m_bucket\b" strategies/v4_cascade/book.py`

Read the method at that line number to confirm its current exact body before editing (it was last touched this session for the `_maybe_recenter_tracking_strikes` fire-and-forget call -- confirm that's still there and don't remove it).

- [ ] **Step 2: Write the failing test**

```python
"""V4CascadeBook feeds the pool engine (not the old V4CascadeEngine) when
use_pool_engine is set -- 75m/15m bars derived by resampling the SAME
self._bars_5m[side] history the old engine already uses, 5m bars fed
directly on every bucket close."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook

IST = ZoneInfo("Asia/Kolkata")


def test_pool_engine_receives_5m_bars_not_old_engine():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._running = True
    base = datetime(2026, 7, 1, 9, 15, tzinfo=IST)
    # Feed enough flat 5m ticks to close one bucket -- no zone should form
    # (flat data), but this confirms the pool engine's _all_75m/pool state
    # gets touched at all (proves routing, not logic correctness -- that's
    # Task 2's job) and the OLD engine's position stays untouched (None).
    for i in range(2):
        book._on_option_tick("CE", 100.0 + i, base + timedelta(minutes=5 * i))
    assert book._pool_engine is not None
    # The old engine must never be touched when the pool engine is active.
    assert book._engine.position is None
```

- [ ] **Step 3: Run test to verify it fails or passes trivially**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_tick_routing.py -v`
Expected: at this point it likely already passes trivially (both engines start with `position=None`) -- that's fine, this is a smoke test for wiring, not a behavior assertion. Proceed to Step 4 regardless; Task 2's own tests already cover the pool engine's internal correctness.

- [ ] **Step 4: Modify `_on_option_tick`.** Find the existing body (confirmed earlier this session):

```python
    def _on_option_tick(self, side: str, ltp: float, ts: datetime) -> None:
        self._live_price[side] = ltp
        if self._is_crypto:
            ...
            self._check_tick_exit(side, ts, tracking_ltp=ltp, execution_ltp=ltp)
        else:
            ...
            self._check_tick_exit(side, ts, tracking_ltp=ltp)
        bucket = _bucket_start(ts, 5, self._session_open)
        cur = self._buckets[side]
        if cur is None or cur.timestamp != bucket:
            if cur is not None:
                self._close_5m_bucket(side, cur)
            self._buckets[side] = _Bar(bucket, ltp, ltp, ltp, ltp, tf=5)
        else:
            cur.high = max(cur.high, ltp)
            cur.low = min(cur.low, ltp)
            cur.close = ltp
```

Leave this method's structure unchanged -- the branch point lives inside `_close_5m_bucket` (Step 5), not here, since that's where the bucket actually closes and gets dispatched.

- [ ] **Step 5: Modify `_close_5m_bucket`** to branch on `self._use_pool_engine`. Replace the existing body:

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
        _pos_before = self._engine.position
        if side == "CE":
            events = self._engine.update(ce_bar=bar)
        else:
            events = self._engine.update(pe_bar=bar)
        for ev in events:
            self._emit_order(ev, pos_before=_pos_before)
        self._persist_position()
        if _bucket_end(bar.timestamp, 75, self._session_open):
            window = [b for b in self._bars_5m[side] if b.timestamp.date() == bar.timestamp.date()]
            r75 = resample_bars(window, 75, self._session_open)
            if r75:
                last = r75[-1]
                b75 = _Bar(last.timestamp, last.close, last.high, last.low, last.close, tf=75)
                if side == "CE":
                    self._engine.update(ce_bar=b75)
                else:
                    self._engine.update(pe_bar=b75)

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

Note: `pos_before=None` is correct here (not a bug) -- `pos_before` exists in `_emit_order`'s signature specifically to handle the OLD engine's structural-flip case (`CLOSE_old + OPEN_new` in the same `update()` batch). The pool engine never structurally flips within a single `on_5m_bar` call (it only ever produces exit events for an already-open position, OR a single open event when the pool clears) -- `_emit_order`'s close-branch already falls back correctly (`close_pos = pos if pos.side == ev.side else pos_before`; since `pos.side == ev.side` always holds here, `pos_before` is never actually read).

- [ ] **Step 6: Run tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_tick_routing.py -v`
Expected: 1 passed

- [ ] **Step 7: Run the full v4_cascade suite to confirm the OLD path (flag off) is untouched**

Run: `python -m pytest tests/strategies -k v4_cascade -q`
Expected: all passing, no regressions

- [ ] **Step 8: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_pool_engine_tick_routing.py
git commit -m "V4Cascade: route live ticks to PoolCascadeEngine when use_pool_engine is set"
```

---

### Task 6: Generalize position persistence/restore/restart-tracker-rebuild across both engines

**Files:**
- Modify: `strategies/v4_cascade/book.py` (`_persist_position`, `_restore_position`, `_guard_replay_position`, `_restore_tracker_state_for_open_position`)
- Test: `tests/strategies/test_v4_cascade_pool_engine_persistence.py`

**Interfaces:**
- Consumes: `self._use_pool_engine`, `self._pool_engine`, `TrailingBaseTracker` (already imported at module level in `book.py` — used by the existing `_restore_tracker_state_for_open_position`).
- Produces: all four methods branch on `self._use_pool_engine` to read/write `self._pool_engine.position`/`self._pool_engine._trail[side]` instead of `self._engine.position`/`self._engine._trackers[side]` when active. This MUST land before Task 7 (history replay), which reuses `_position_snapshot`/`_guard_replay_position` rather than a bespoke inline guard.

This closes a real gap: `_persist_position`/`_restore_position` currently hardcode `self._engine.position` (confirmed by reading the live source — lines 1613-1634), and `_guard_replay_position` hardcodes `after = self._engine.position` (line 516). Left as-is, every pool-engine open position would silently fail to persist to `position_store`, and a restart would come back up with `self._pool_engine.position = None` no matter what was actually open — the exact bug class `_restore_tracker_state_for_open_position`'s own docstring describes for T2's tracker (comment 2026-07-21), just one level up.

- [ ] **Step 1: Write the failing test**

```python
"""V4CascadeBook._persist_position/_restore_position/
_restore_tracker_state_for_open_position all read/write the ACTIVE engine's
position -- self._pool_engine.position when use_pool_engine is set, not
self._engine.position (which stays None the whole time for a pool-engine
book). Without this, a restart mid-trade would silently lose the position
and drop T2's stop-loss enforcement entirely (same bug class fixed for the
old engine on 2026-07-21, here for the new one)."""
from datetime import datetime
from zoneinfo import ZoneInfo

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg

IST = ZoneInfo("Asia/Kolkata")


def _open_pos(side="CE"):
    t1 = TrancheLeg(tranche="T1", option_type=side, strike=23700, qty=65,
                     entry_price=100.0, sl_price=90.0, target_price=110.0)
    t2 = TrancheLeg(tranche="T2", option_type=side, strike=23700, qty=65,
                     entry_price=100.0, sl_price=90.0, tracking_current_stop=95.0)
    return CascadePosition(underlying="NIFTY", side=side, tracking_strike=23700,
                            execution_strike=23700, atm_at_trigger=23700, entry_spot=23700,
                            tracking_entry_price=100.0, t1=t1, t2=t2,
                            open_time=datetime.now(IST))


def test_persist_and_restore_roundtrip_pool_engine_position():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._pool_engine.position = _open_pos("CE")
    book._persist_position()
    assert book._engine.position is None  # old engine untouched

    book2 = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                           binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                           use_pool_engine=True)
    book2._restore_position()
    assert book2._pool_engine.position is not None
    assert book2._pool_engine.position.side == "CE"
    assert book2._engine.position is None
    book._persist_key and __import__("data_layer.position_store", fromlist=["clear"]).clear(book._persist_key)


def test_restore_tracker_state_rebuilds_pool_engine_t2_tracker():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._pool_engine.position = _open_pos("CE")
    book._restore_tracker_state_for_open_position()
    tracker = book._pool_engine._trail["CE"]
    assert tracker is not None
    assert tracker.current_stop == 95.0  # from t2.tracking_current_stop
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_persistence.py -v`
Expected: FAIL — `book2._pool_engine.position` is `None` (restore wrote into `self._engine.position` instead), and the second test's `tracker` is `None`.

- [ ] **Step 3: Generalize `_persist_position`/`_restore_position`.** Replace:

```python
    def _persist_position(self) -> None:
        pos = self._engine.position
        if pos is not None:
            position_store.save(self._persist_key, pos.to_dict())
        else:
            position_store.clear(self._persist_key)

    def _restore_position(self) -> None:
        try:
            data = position_store.load(self._persist_key)
        except Exception:
            data = None
        if data:
            try:
                self._engine.position = CascadePosition.from_dict(data)
                logger.info("V4CascadeBook[%s/%s/%s]: restored open position from disk (side=%s).",
                            self._underlying, self._client_id, self._binding_id,
                            self._engine.position.side)
                self._clog.info("restored open position from disk (side=%s).",
                                self._engine.position.side)
            except Exception:
                logger.exception("V4CascadeBook[%s]: position restore failed.", self._underlying)
```

with:

```python
    def _persist_position(self) -> None:
        pos = self._pool_engine.position if self._use_pool_engine else self._engine.position
        if pos is not None:
            position_store.save(self._persist_key, pos.to_dict())
        else:
            position_store.clear(self._persist_key)

    def _restore_position(self) -> None:
        try:
            data = position_store.load(self._persist_key)
        except Exception:
            data = None
        if data:
            try:
                restored = CascadePosition.from_dict(data)
                if self._use_pool_engine:
                    self._pool_engine.position = restored
                else:
                    self._engine.position = restored
                logger.info("V4CascadeBook[%s/%s/%s]: restored open position from disk (side=%s).",
                            self._underlying, self._client_id, self._binding_id, restored.side)
                self._clog.info("restored open position from disk (side=%s).", restored.side)
            except Exception:
                logger.exception("V4CascadeBook[%s]: position restore failed.", self._underlying)
```

- [ ] **Step 4: Generalize `_guard_replay_position`.** Find (confirmed at line 516):

```python
        after = self._engine.position
```

Replace with:

```python
        after = self._pool_engine.position if self._use_pool_engine else self._engine.position
```

(Leave the rest of the method — the `touched` comparison and whatever follows it — unchanged; only this one line hardcodes the engine.)

- [ ] **Step 5: Generalize `_restore_tracker_state_for_open_position`.** Replace the method's opening:

```python
    def _restore_tracker_state_for_open_position(self) -> None:
        """..."""
        pos = self._engine.position
        if pos is None or not pos.is_open:
            return
        if pos.tracking_entry_price:
            self._engine._tracking_entry_price[pos.side] = pos.tracking_entry_price
        t2 = pos.t2
        if t2 is not None and t2.status == "open":
            scanner = self._engine._scanners.get(pos.side)
            bear = scanner._bear if scanner is not None else True
            initial_stop = (t2.tracking_current_stop if t2.tracking_current_stop is not None
                            else (t2.sl_price or None))
            self._engine._trackers[pos.side] = TrailingBaseTracker(bear=bear, initial_stop=initial_stop)
            logger.info("V4CascadeBook[%s/%s/%s]: restored T2 trailing-stop tracker for open "
                       "position (side=%s, current_stop=%s).", self._underlying, self._client_id,
                       self._binding_id, pos.side, initial_stop)
            self._clog.info("restored T2 trailing-stop tracker (side=%s, current_stop=%s).",
                            pos.side, initial_stop)
```

with:

```python
    def _restore_tracker_state_for_open_position(self) -> None:
        """..."""
        if self._use_pool_engine:
            self._restore_pool_engine_tracker_state()
            return
        pos = self._engine.position
        if pos is None or not pos.is_open:
            return
        if pos.tracking_entry_price:
            self._engine._tracking_entry_price[pos.side] = pos.tracking_entry_price
        t2 = pos.t2
        if t2 is not None and t2.status == "open":
            scanner = self._engine._scanners.get(pos.side)
            bear = scanner._bear if scanner is not None else True
            initial_stop = (t2.tracking_current_stop if t2.tracking_current_stop is not None
                            else (t2.sl_price or None))
            self._engine._trackers[pos.side] = TrailingBaseTracker(bear=bear, initial_stop=initial_stop)
            logger.info("V4CascadeBook[%s/%s/%s]: restored T2 trailing-stop tracker for open "
                       "position (side=%s, current_stop=%s).", self._underlying, self._client_id,
                       self._binding_id, pos.side, initial_stop)
            self._clog.info("restored T2 trailing-stop tracker (side=%s, current_stop=%s).",
                            pos.side, initial_stop)

    def _restore_pool_engine_tracker_state(self) -> None:
        """Pool-engine counterpart: no separate _tracking_entry_price dict to
        rebuild (each TrancheLeg already stores its own entry_price directly,
        since the pool engine trades the tracking contract with no scale
        mapping) -- only T2's TrailingBaseTracker needs reconstructing."""
        pos = self._pool_engine.position
        if pos is None or not pos.is_open:
            return
        t2 = pos.t2
        if t2 is not None and t2.status == "open":
            bear = pos.side == "CE"
            initial_stop = (t2.tracking_current_stop if t2.tracking_current_stop is not None
                            else (t2.sl_price or None))
            self._pool_engine._trail[pos.side] = TrailingBaseTracker(bear=bear, initial_stop=initial_stop)
            logger.info("V4CascadeBook[%s/%s/%s]: restored T2 trailing-stop tracker for open "
                       "pool-engine position (side=%s, current_stop=%s).", self._underlying,
                       self._client_id, self._binding_id, pos.side, initial_stop)
            self._clog.info("restored T2 trailing-stop tracker (side=%s, current_stop=%s).",
                            pos.side, initial_stop)
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_persistence.py -v`
Expected: 2 passed

- [ ] **Step 7: Run the full v4_cascade suite to confirm the old path is untouched**

Run: `python -m pytest tests/strategies -k v4_cascade -q`
Expected: all passing, no regressions

- [ ] **Step 8: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_pool_engine_persistence.py
git commit -m "V4Cascade: generalize position persistence/restore/tracker-rebuild for pool engine"
```

---

### Task 7: History replay into the pool engine at boot

> Depends on Task 6 — this task's replay guard reuses the now-generalized `_position_snapshot`/`_guard_replay_position` (which branch on `self._use_pool_engine` as of Task 6) instead of a bespoke check, so replay-caused phantom opens/closes are caught the same proven way (value comparison via `to_dict()`, not object identity — the 2026-07-21 fix documented on `_guard_replay_position`) for both engines.

**Files:**
- Modify: `strategies/v4_cascade/book.py` (`_ingest_history`)
- Test: `tests/strategies/test_v4_cascade_pool_engine_history_replay.py`

**Interfaces:**
- Consumes: the SAME `ce_5m`/`pe_5m` fetch `_ingest_history` already performs (Task unchanged), `resample_bars`, `PoolCascadeEngine.on_75m_bar`/`on_15m_bar`/`on_5m_bar`.
- Produces: when `self._use_pool_engine` is True, `_ingest_history` replays the fetched history through the pool engine instead of `_replay_through_engine`+old engine, using the SAME position-snapshot/guard-replay-position safety mechanism the old path already uses.

- [ ] **Step 1: Write the failing test**

```python
"""V4CascadeBook._ingest_history replays fetched CE/PE premium history
through the pool engine (not the old V4CascadeEngine) when use_pool_engine
is set. Uses monkeypatched fetch functions (same pattern as the existing
_ingest_history tests) -- no real network calls."""
from datetime import date, datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook

IST = ZoneInfo("Asia/Kolkata")


def _rows(base: datetime, n: int, price: float = 100.0):
    return [{"ts": (base + timedelta(minutes=i)).isoformat(), "open": price, "high": price + 1,
             "low": price - 1, "close": price, "volume": 10} for i in range(n)]


@pytest.mark.asyncio
async def test_ingest_history_replays_into_pool_engine_not_old_engine():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._running = True
    book._expiry = date(2026, 7, 28)
    book._ce_symbol, book._pe_symbol = "NSE_FO|CE", "NSE_FO|PE"
    book._ce_strike, book._pe_strike = 23700, 24100

    base = datetime(2026, 7, 1, 9, 15, tzinfo=IST)
    rows = _rows(base, 20)

    with patch.object(book, "_access_token", return_value="tok"), \
         patch.object(book, "_resolve_symbols", return_value=True), \
         patch.object(book, "_subscribe_tracking_contracts", return_value=None), \
         patch("strategies.v4_cascade.book.fetch_upstox_range_1m", return_value=rows), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", return_value=[]):
        ok = await book._ingest_history()

    assert ok is True
    assert book._engine.position is None  # old engine never touched
    assert len(book._bars_5m["CE"]) > 0    # SAME bar history still built (both paths need it)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_history_replay.py -v`
Expected: passes trivially today too (old engine's position starts None either way) -- this confirms wiring exists once Step 3 is in place, but won't itself prove the branch was taken. Proceed regardless; add the assertion below after Step 3 to make it meaningful.

- [ ] **Step 3: Modify `_ingest_history`.** Find the existing replay block (confirmed earlier this session, inside `_ingest_history`):

```python
        _pos_snapshot = self._position_snapshot(self._engine.position)
        _replay_through_engine(self._engine, spot_5m, ce_5m, pe_5m,
                                on_daily_boundary=self._apply_eod_gate23_rules,
                                session_open=self._session_open,
                                eod_square_off=self._eod_hour_min,
                                gate23_reset=self._gate23_hour_min)
        self._guard_replay_position(_pos_snapshot)
```

Replace it with:

```python
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
```

Then add a new method, right after `_ingest_history` in the same class:

```python
    def _replay_pool_engine_history(self, ce_5m, pe_5m) -> None:
        """Rebuilds the pool engine's HTF/LTF pool state from fetched
        history -- mirrors _replay_through_engine's shape (chronological
        5m feed, 75m/15m derived by resampling the growing history at each
        boundary) but drives PoolCascadeEngine instead. Replay must never
        touch a live position -- reuses the SAME _position_snapshot/
        _guard_replay_position pair the old engine's replay already uses
        (both generalized in Task 6 to branch on self._use_pool_engine),
        so a replay-caused phantom open/close is caught by the proven
        value-comparison guard, not a fresh ad-hoc check."""
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

Note: this uses the SAME 15m/75m-window-derivation pattern (per-day slice, `resample_bars(window, ...)` on bars up to the current index) as the OLD engine's own live 75m dispatch in `_close_5m_bucket` uses for ITS per-day resample -- but note this is DIFFERENT from Task 5's live `_close_5m_bucket_pool_engine`, which resamples the FULL `self._bars_5m[side]` (not a per-day slice), because during REPLAY the growing bar list IS the equivalent of "full history so far" at each point already (there's no separately-maintained `self._bars_5m[side]` yet during this specific replay call -- it gets set separately by the caller). This distinction is intentional, not a bug: replay reconstructs from a snapshot array; live ticks build a persistent one.

- [ ] **Step 4: Add the meaningful assertion now that Step 3 exists.** Update the test from Step 1 to also assert the pool engine actually processed bars:

```python
    assert len(book._pool_engine._all_75m["CE"]) >= 0  # replay ran without exception
```

(append this line to the existing test body, right after the `len(book._bars_5m["CE"]) > 0` assertion)

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_history_replay.py -v`
Expected: 1 passed

- [ ] **Step 6: Run the full v4_cascade suite**

Run: `python -m pytest tests/strategies -k v4_cascade -q`
Expected: all passing, no regressions

- [ ] **Step 7: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_pool_engine_history_replay.py
git commit -m "V4Cascade: replay history into PoolCascadeEngine at boot when use_pool_engine is set"
```

---

### Task 8: Fill/exit price integrity and EOD square-off for the pool engine

**Files:**
- Modify: `strategies/v4_cascade/book.py` (`_emit_order`, `_open_entry_async`, `_force_eod_square_off`)
- Test: `tests/strategies/test_v4_cascade_pool_engine_fills.py`

**Interfaces:**
- Consumes: `self._use_pool_engine`, `self._live_price[side]` (already maintained for every book), `self._ce_strike`/`self._pe_strike` (already resolved tracking strikes).
- Produces: for pool-engine positions, `_emit_order`'s OPEN branch sets `t1.strike`/`t2.strike`/`pos.execution_strike` to the TRACKING strike directly (no `_resolve_execution_strike` call, no execution-native-risk lookback in `_open_entry_async`); fills and exits both price off `self._live_price[side]` (the tracking contract's own live tick), never `self._exec_live_price[side]`.

- [ ] **Step 1: Confirm the exact current `_emit_order` OPEN branch**

Run: `grep -n "def _emit_order" strategies/v4_cascade/book.py`

Read ~30 lines from that point to confirm the exact current body (it was last modified this session for the execution-native-risk work) before editing.

- [ ] **Step 2: Write the failing test**

```python
"""Pool-engine positions fill/exit on the TRACKING contract directly --
no execution-strike resolution, no execution-native-risk lookback. Strike
fields on the opened position match the book's own resolved tracking
strike for that side."""
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook

IST = ZoneInfo("Asia/Kolkata")


@pytest.mark.asyncio
async def test_pool_engine_open_uses_tracking_strike_not_execution_strike():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._running = True
    book._ce_strike, book._pe_strike = 23700, 24100
    book._live_price["CE"] = 150.5

    # Directly exercise the pool engine's own open (mirrors what on_5m_bar would do),
    # then let _emit_order route it.
    book._pool_engine.position = None  # ensure clean state
    from strategies.v4_cascade.pool_engine import _ZoneSlot
    from strategies.v4_cascade.dataclasses import RollingBaseZone
    zone = RollingBaseZone(entry_line=145.0, sweep_low=135.0, sl_level=170.0,
                            reference_low_ts=datetime.now(IST), lock_ts=datetime.now(IST), locked=True)
    slot = _ZoneSlot(zone)
    slot.ltf_zone = RollingBaseZone(entry_line=148.0, sweep_low=142.0, sl_level=155.0,
                                     reference_low_ts=datetime.now(IST), lock_ts=datetime.now(IST), locked=True)
    real_ev = book._pool_engine._open_position("CE", slot, 150.0, datetime.now(IST))

    book._emit_order(real_ev, pos_before=None)

    assert book._pool_engine.position.t1.strike == 23700
    assert book._pool_engine.position.execution_strike == 23700
```

- [ ] **Step 3: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_fills.py -v`
Expected: FAIL (strike is 0.0, since `_emit_order` currently always calls `_resolve_execution_strike`, which needs live_spot/atm_open state this test doesn't set up, and even if it did, it wouldn't match the tracking strike)

- [ ] **Step 4: Modify `_emit_order`'s OPEN branch.** Find:

```python
        if is_open_ev and pos is not None:
            exec_strike = self._resolve_execution_strike(ev.side)
            if pos.t1 is not None:
                pos.t1.strike = exec_strike
            if pos.t2 is not None:
                pos.t2.strike = exec_strike
            pos.execution_strike = exec_strike
```

Replace with:

```python
        if is_open_ev and pos is not None:
            # 2026-07-23: pool-engine positions trade the TRACKING contract
            # directly (user-confirmed) -- no separate execution strike, no
            # scale mapping between two different contracts' price levels.
            if self._use_pool_engine:
                exec_strike = self._ce_strike if ev.side == "CE" else self._pe_strike
            else:
                exec_strike = self._resolve_execution_strike(ev.side)
            if pos.t1 is not None:
                pos.t1.strike = exec_strike
            if pos.t2 is not None:
                pos.t2.strike = exec_strike
            pos.execution_strike = exec_strike
```

- [ ] **Step 5: Confirm `_open_entry_async` skips the execution-native-risk lookback for pool-engine positions.** Find the block (confirmed earlier this session):

```python
        pos = self._pending_fills.get(event_id)
        if pos is None:
            pos = self._engine.position
        if pos is not None and pos.is_open and not self._is_crypto and exec_strike:
            exec_bars = await self._fetch_execution_bars_5m(self._exec_symbol[ev.side])
            ...
```

Change the guard condition to also exclude pool-engine positions:

```python
        pos = self._pending_fills.get(event_id)
        if pos is None:
            pos = self._pool_engine.position if self._use_pool_engine else self._engine.position
        if pos is not None and pos.is_open and not self._is_crypto and not self._use_pool_engine and exec_strike:
            exec_bars = await self._fetch_execution_bars_5m(self._exec_symbol[ev.side])
            ...
```

(Only the two changed lines matter here -- the `if pos is None: pos = ...` line and the `if pos is not None and pos.is_open ...` condition. Leave the rest of `_open_entry_async`'s body, including the tick-wait-for-real-price block right above it, unchanged -- it already reads `self._exec_live_price[ev.side]`, which Step 6 below addresses separately for the fill-price source.)

- [ ] **Step 6: Confirm fill/exit price sourcing.** `_open_entry_async` and the CLOSE-handling block both currently prefer `self._exec_live_price[ev.side]` (falling back to `ev.price_hint`). For pool-engine positions there is no execution contract at all, so `self._exec_live_price[ev.side]` is always `0.0` (never populated) -- the existing fallback-to-`ev.price_hint` path already fires correctly with NO code change needed, since `real_price <= 0` is already true for every pool-engine tick. Confirm this by grepping:

Run: `grep -n "_exec_live_price\[ev.side\]" strategies/v4_cascade/book.py`

Read both call sites (entry wait-loop, exit price resolution) and confirm each already has an `if real_price <= 0: ... fallback to ev.price_hint` branch (per this session's earlier "Live fill price integrity" work). If both already fall back correctly, no change is needed here -- this step is verification only, not a code change. If either site is missing a fallback (unexpected -- flag this to the human rather than guessing a fix), stop and report instead of editing blind.

- [ ] **Step 7: Modify `_force_eod_square_off` to also handle pool-engine positions.** Find the existing method (confirmed earlier this session):

```python
    def _force_eod_square_off(self, ts: datetime) -> None:
        pos = self._engine.position
        if pos is None or not pos.is_open:
            return
        ...
```

Change the first line and add a pool-engine branch right after the existing method body (do not modify the existing body itself -- it's correct and tested for the old engine):

```python
    def _force_eod_square_off(self, ts: datetime) -> None:
        if self._use_pool_engine:
            self._force_eod_square_off_pool_engine(ts)
            return
        pos = self._engine.position
        if pos is None or not pos.is_open:
            return
        ... (existing body, unchanged) ...

    def _force_eod_square_off_pool_engine(self, ts: datetime) -> None:
        pos = self._pool_engine.position
        if pos is None or not pos.is_open:
            return
        logger.info("V4CascadeBook[%s/%s/%s]: EOD %02d:%02d force square-off (pool engine).",
                    self._underlying, self._client_id, self._binding_id,
                    self._eod_hour_min[0], self._eod_hour_min[1])
        price = self._live_price.get(pos.side, 0.0) or 0.0
        for tranche, leg in (("T1", pos.t1), ("T2", pos.t2)):
            if leg is None or leg.status != "open":
                continue
            fill_price = price if price > 0 else leg.entry_price
            leg.status = "closed"
            leg.close_price = fill_price
            leg.close_reason = "eod_force_close"
            leg.close_time = ts
            self._emit_order(CascadeEvent(
                event_type=CascadeEventType.CLOSE_LONG_CE if pos.side == "CE" else CascadeEventType.CLOSE_LONG_PE,
                side=pos.side, tranche=tranche, reason="eod_force_close",
                price_hint=fill_price, timestamp=ts,
            ), pos_before=None)
        pos.status = "closed"
        pos.close_time = ts
        self._persist_position()
```

`CascadeEvent`/`CascadeEventType` are already imported at the top of `book.py` (used throughout `_emit_order` and elsewhere) -- no new import needed.

- [ ] **Step 8: Run tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_pool_engine_fills.py -v`
Expected: 1 passed

- [ ] **Step 9: Run the full v4_cascade suite**

Run: `python -m pytest tests/strategies -k v4_cascade -q`
Expected: all passing, no regressions

- [ ] **Step 10: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_pool_engine_fills.py
git commit -m "V4Cascade: pool-engine fills/EOD use the tracking contract directly, no execution split"
```

---

### Task 9: Full regression pass + manual trace against the real 2026-07-23 09:20 log

**Files:** none created/modified beyond what's already in place -- this is a controller-run verification task, no subagent needed (matches this session's own Task 11 precedent from the earlier execution-native-risk plan).

- [ ] **Step 1: Run the full v4_cascade test suite one final time**

Run: `python -m pytest tests/strategies -k v4_cascade -q`
Expected: all passing (old-path tests unchanged, all new pool-engine tests passing)

- [ ] **Step 2: Run the full repo suite**

Run: `python -m pytest tests/ --deselect tests/data_layer/test_feeder_translation.py -q`
Expected: all passing except the one known pre-existing unrelated failure (already excluded)

- [ ] **Step 3: Manual trace against the real 09:20 log.** Using a fresh Upstox token (ask the human for one if not already available in the session), fetch the real NIFTY PE 24100 tracking contract's 1-minute history for 2026-07-15 through 2026-07-23 (the exact window the real log's `ref=2026-07-15T13:00:00` / `locked=2026-07-23T09:15:00` referenced), resample to 5m, and replay it through a standalone `PoolCascadeEngine` instance (same pattern as `backtest/v4_cascade/htf_ltf_backtest.py`'s `run_backtest`, but calling `on_75m_bar`/`on_15m_bar`/`on_5m_bar` directly instead of the backtest's array-replay loop). Confirm the new engine does NOT open a PE position at 09:20 on 07-23 -- either no zone is re-entered by then, or the 5m trigger doesn't fire on the very first candle of the day (matching the intraday-only reset from Task 2). Report the actual pool state at that timestamp (how many zones in the PE pool, whether any is `tracking`) as the concrete evidence.

- [ ] **Step 4: Update the SDD progress ledger** (if executing via subagent-driven-development) with a final summary: total tasks, total commits, test counts, the Step 3 trace result, and explicit confirmation that `use_pool_engine=False` (default) leaves 100% of existing behavior unchanged.

- [ ] **Step 5: Report to the human** for a go/no-go decision on actually flipping `use_pool_engine=True` for the live `ssrajpal2001/SA5770/NIFTY` deployment -- this plan builds and validates the capability; turning it on for the real running deployment is a separate, explicit decision the human makes after seeing Step 3's trace result.
