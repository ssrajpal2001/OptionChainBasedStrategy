# Trap v2 Multi-Level Concurrent Registry — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the LIFO seller-trap stack with a concurrent multi-level registry so macro trap levels survive while micro levels form inside them, and reconnect the orphaned v2 position lifecycle (void shield, persistence, time cutoffs, telemetry, re-arming).

**Architecture:** Phase 1 rewrites the pure detector (`trap_seller_detection.py`) into a keyed `TrapLevelRegistry` with per-level state. Phase 2 rewires `trap_trading_engine.py` to drive per-zone MTF detectors, the 1-min void shield, structural-floor stop, and re-arming. Phase 3 wires v2 persistence, the 15:15/evening squareoff, and registry telemetry.

**Tech Stack:** Python 3, asyncio, dataclasses/Enum, pytest. No new deps.

**Spec:** `docs/superpowers/specs/2026-06-06-trap-v2-multilevel-registry-design.md`

---

## File Structure

- `strategies/trap_seller_detection.py` — **rewrite**. `LevelState`, `TrapLevel`, `TrapLevelRegistry` (pure, no I/O). Replaces `State`/`Level`/`SellerTrapDetector` LIFO.
- `strategies/trap_trading_engine.py` — **modify**. Per-leg `TrapLevelRegistry` (HTF) + per-zone MTF registries; `_feed_leg_tick` rewrite; void shield; 14:45 entry-block; structural-floor + two-tier trail; v2 EOD squareoff; persistence; telemetry; re-arming.
- `data_layer/position_store.py` — **no change** (existing `save`/`load`/`clear` are sufficient; v2 stores its dict under key `<UND>_trap_v2`).
- `config/global_config.py` — **modify**. Add `_RETEST_BAND_PCT` (buffer band, % of entry_l) and `_V2_SQUAREOFF_TIME` ("15:15") to the trap-engine config with getters, mirroring existing `_RETEST_ZONE_PERCENT`.
- `tests/test_trap_seller_detection.py` — **create**. Pure registry unit tests (Phase 1).
- `tests/test_trap_engine_v2.py` — **create**. Engine integration tests (Phases 2-3).

**Compatibility note:** `SellerTrapDetector` and `State` are imported by `trap_trading_engine.py` (`from strategies.trap_seller_detection import SellerTrapDetector, State as _DetState`). The engine is rewritten in Phase 2, so the old classes may be removed only after Phase 2 lands. Phase 1 ADDS the new classes alongside the old ones (no removal) so the existing import and the 150-test suite keep passing. Old classes are deleted in Phase 2 Task 13.

---

## Phase 1 — Pure Detection Registry

### Task 1: LevelState enum + TrapLevel dataclass

**Files:**
- Modify: `strategies/trap_seller_detection.py`
- Test: `tests/test_trap_seller_detection.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_trap_seller_detection.py
from strategies.trap_seller_detection import LevelState, TrapLevel


def test_trap_level_defaults_to_watch_and_is_active():
    lv = TrapLevel(anchor_id="c1", entry_l=363.30, sl_h=420.35, struct_low=363.30)
    assert lv.state is LevelState.WATCH
    assert lv.active is True


def test_trap_level_mitigated_and_invalidated_are_inactive():
    lv = TrapLevel(anchor_id="c1", entry_l=363.30, sl_h=420.35, struct_low=363.30)
    lv.state = LevelState.MITIGATED
    assert lv.active is False
    lv.state = LevelState.INVALIDATED
    assert lv.active is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_trap_seller_detection.py -q`
Expected: FAIL with `ImportError: cannot import name 'LevelState'`

- [ ] **Step 3: Write minimal implementation**

Append to `strategies/trap_seller_detection.py` (keep the existing `State`/`Level`/`SellerTrapDetector` for now):

```python
from enum import Enum, auto
from dataclasses import dataclass


class LevelState(Enum):
    WATCH = auto()
    SELLERS_IN = auto()
    TRAPPED = auto()
    ENTRY_READY = auto()
    MITIGATED = auto()
    INVALIDATED = auto()


@dataclass
class TrapLevel:
    anchor_id: object
    entry_l: float
    sl_h: float
    struct_low: float
    state: LevelState = LevelState.WATCH

    @property
    def active(self) -> bool:
        return self.state not in (LevelState.MITIGATED, LevelState.INVALIDATED)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_trap_seller_detection.py -q`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add strategies/trap_seller_detection.py tests/test_trap_seller_detection.py
git commit -m "feat(trap): add LevelState + TrapLevel for multi-level registry"
```

---

### Task 2: Registry keeps macro level while micro forms inside it

**Files:**
- Modify: `strategies/trap_seller_detection.py`
- Test: `tests/test_trap_seller_detection.py`

- [ ] **Step 1: Write the failing test**

```python
from strategies.trap_seller_detection import TrapLevelRegistry


def test_on_candle_keeps_all_levels_concurrently():
    reg = TrapLevelRegistry(band=2.0)
    reg.on_candle({"low": 363.30, "high": 420.35}, anchor_id="c1")  # macro
    reg.on_candle({"low": 380.00, "high": 410.00}, anchor_id="c2")  # micro inside
    active = reg.active_levels()
    ids = {lv.anchor_id for lv in active}
    assert ids == {"c1", "c2"}          # macro NOT evicted by micro


def test_on_candle_updates_struct_low_of_existing_levels():
    reg = TrapLevelRegistry(band=2.0)
    reg.on_candle({"low": 363.30, "high": 420.35}, anchor_id="c1")
    reg.on_candle({"low": 340.00, "high": 410.00}, anchor_id="c2")  # lower low
    c1 = reg.get("c1")
    assert c1.struct_low == 340.00       # macro's struct_low tracks the lower low
    assert c1.entry_l == 363.30          # entry reference unchanged
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_trap_seller_detection.py -q`
Expected: FAIL with `ImportError: cannot import name 'TrapLevelRegistry'`

- [ ] **Step 3: Write minimal implementation**

Append to `strategies/trap_seller_detection.py`:

```python
from typing import Dict, List, Optional, Set


class TrapLevelRegistry:
    """Concurrent registry of seller-trap levels. Macro levels persist while
    micro levels form inside them; each level has an independent state machine.
    Pure / side-effect-free. `band` is an absolute price buffer supplied by the
    engine (derived from a % of entry_l)."""

    def __init__(self, band: float = 0.0) -> None:
        self._levels: Dict[object, TrapLevel] = {}
        self.band = float(band)
        self._auto = 0

    def on_candle(self, c: dict, anchor_id: object = None) -> object:
        """Add a new level (keyed by anchor_id, else an auto id). Update the
        running struct_low of every active level with this candle's low.
        Never pops existing levels."""
        low = float(c["low"])
        for lv in self._levels.values():
            if lv.active:
                lv.struct_low = min(lv.struct_low, low)
        if anchor_id is None:
            anchor_id = f"_auto{self._auto}"
            self._auto += 1
        self._levels[anchor_id] = TrapLevel(
            anchor_id=anchor_id, entry_l=low, sl_h=float(c["high"]), struct_low=low)
        return anchor_id

    def get(self, anchor_id: object) -> Optional[TrapLevel]:
        return self._levels.get(anchor_id)

    def active_levels(self) -> List[TrapLevel]:
        return [lv for lv in self._levels.values() if lv.active]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_trap_seller_detection.py -q`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add strategies/trap_seller_detection.py tests/test_trap_seller_detection.py
git commit -m "feat(trap): TrapLevelRegistry keeps levels concurrently + tracks struct_low"
```

---

### Task 3: on_tick transitions — the 363.30 retest fires ENTRY_READY

**Files:**
- Modify: `strategies/trap_seller_detection.py`
- Test: `tests/test_trap_seller_detection.py`

- [ ] **Step 1: Write the failing test**

```python
from strategies.trap_seller_detection import LevelState


def test_retest_to_macro_low_fires_entry_ready():
    # Mirrors Images/a.png: 1st candle low=363.30 high=420.35; sellers enter
    # below, get trapped above, then the 8th candle returns to 363.30.
    reg = TrapLevelRegistry(band=2.0)
    reg.on_candle({"low": 363.30, "high": 420.35}, anchor_id="c1")
    reg.on_tick(360.00)                       # below low -> SELLERS_IN
    assert reg.get("c1").state is LevelState.SELLERS_IN
    reg.on_tick(430.00)                        # above high -> TRAPPED
    assert reg.get("c1").state is LevelState.TRAPPED
    newly = reg.on_tick(364.00)                # return into [363.30, 365.30] -> ENTRY_READY
    assert reg.get("c1").state is LevelState.ENTRY_READY
    assert "c1" in newly


def test_entry_ready_only_returned_once():
    reg = TrapLevelRegistry(band=2.0)
    reg.on_candle({"low": 363.30, "high": 420.35}, anchor_id="c1")
    reg.on_tick(360.00); reg.on_tick(430.00)
    assert "c1" in reg.on_tick(364.00)         # transition tick
    assert "c1" not in reg.on_tick(364.50)     # already ENTRY_READY, not re-emitted
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_trap_seller_detection.py -q`
Expected: FAIL with `AttributeError: 'TrapLevelRegistry' object has no attribute 'on_tick'`

- [ ] **Step 3: Write minimal implementation**

Add to `TrapLevelRegistry`:

```python
    def on_tick(self, price: float) -> Set[object]:
        """Evaluate price against ALL active levels. Returns the set of anchor_ids
        that NEWLY became ENTRY_READY this tick."""
        price = float(price)
        newly: Set[object] = set()
        for aid, lv in self._levels.items():
            if not lv.active:
                continue
            if lv.state is LevelState.WATCH and price < lv.entry_l:
                lv.state = LevelState.SELLERS_IN
            if lv.state is LevelState.SELLERS_IN and price > lv.sl_h:
                lv.state = LevelState.TRAPPED
            if lv.state is LevelState.TRAPPED and lv.entry_l <= price <= lv.entry_l + self.band:
                lv.state = LevelState.ENTRY_READY
                newly.add(aid)
            if lv.active and price < lv.struct_low - self.band:
                lv.state = LevelState.INVALIDATED
        return newly
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_trap_seller_detection.py -q`
Expected: PASS (6 passed)

- [ ] **Step 5: Commit**

```bash
git add strategies/trap_seller_detection.py tests/test_trap_seller_detection.py
git commit -m "feat(trap): on_tick concurrent transitions (363.30 retest -> ENTRY_READY)"
```

---

### Task 4: Hard structural breakdown invalidates only the breached level

**Files:**
- Modify: `strategies/trap_seller_detection.py` (no new code expected — verifies Task 3 logic)
- Test: `tests/test_trap_seller_detection.py`

- [ ] **Step 1: Write the failing test**

```python
def test_tick_below_struct_low_minus_band_invalidates():
    reg = TrapLevelRegistry(band=2.0)
    reg.on_candle({"low": 363.30, "high": 420.35}, anchor_id="c1")
    reg.on_tick(309.00)                         # < struct_low(363.30) - band(2) = 361.30
    assert reg.get("c1").state is LevelState.INVALIDATED
    assert reg.get("c1").active is False


def test_micro_invalidation_does_not_kill_macro():
    reg = TrapLevelRegistry(band=2.0)
    reg.on_candle({"low": 309.50, "high": 420.35}, anchor_id="macro")  # deep struct_low
    reg.on_candle({"low": 380.00, "high": 410.00}, anchor_id="micro")  # shallow
    # macro.struct_low updated to min(309.50, 380.00) = 309.50 on the 2nd candle
    reg.on_tick(370.00)                          # < micro.struct_low(380)-2=378 -> micro dead
    assert reg.get("micro").state is LevelState.INVALIDATED
    assert reg.get("macro").active is True       # macro survives (370 > 307.5)
```

- [ ] **Step 2: Run test to verify it fails or passes**

Run: `python -m pytest tests/test_trap_seller_detection.py -q`
Expected: PASS if Task 3 logic is correct (these assert the invalidation branch). If FAIL, fix the `price < lv.struct_low - self.band` branch in `on_tick`.

- [ ] **Step 3: (only if Step 2 failed) Fix implementation**

No change expected. If failing, ensure the invalidation check runs for every active level independently and uses each level's own `struct_low`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_trap_seller_detection.py -q`
Expected: PASS (8 passed)

- [ ] **Step 5: Commit**

```bash
git add tests/test_trap_seller_detection.py
git commit -m "test(trap): structural breakdown invalidates only breached level"
```

---

### Task 5: Buffer-band edge behavior

**Files:**
- Test: `tests/test_trap_seller_detection.py`

- [ ] **Step 1: Write the failing test**

```python
def test_retest_at_band_edge_arms_but_beyond_does_not():
    reg = TrapLevelRegistry(band=2.0)
    reg.on_candle({"low": 363.30, "high": 420.35}, anchor_id="c1")
    reg.on_tick(360.00); reg.on_tick(430.00)     # SELLERS_IN -> TRAPPED
    # price above the band top (365.30) must NOT arm yet
    assert "c1" not in reg.on_tick(366.00)
    assert reg.get("c1").state is LevelState.TRAPPED
    # exactly at band top arms
    assert "c1" in reg.on_tick(365.30)
    assert reg.get("c1").state is LevelState.ENTRY_READY


def test_dip_to_entry_low_within_band_does_not_invalidate():
    reg = TrapLevelRegistry(band=2.0)
    reg.on_candle({"low": 363.30, "high": 420.35}, anchor_id="c1")
    reg.on_tick(360.00); reg.on_tick(430.00)
    reg.on_tick(363.30)                          # at entry_l, well above struct_low-band
    assert reg.get("c1").active is True
    assert reg.get("c1").state is LevelState.ENTRY_READY
```

- [ ] **Step 2: Run test to verify it fails or passes**

Run: `python -m pytest tests/test_trap_seller_detection.py -q`
Expected: PASS (band logic from Task 3). If FAIL, check the inclusive `<=` bounds.

- [ ] **Step 3: (only if failed) Fix implementation** — adjust band comparisons in `on_tick`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_trap_seller_detection.py -q`
Expected: PASS (10 passed)

- [ ] **Step 5: Commit**

```bash
git add tests/test_trap_seller_detection.py
git commit -m "test(trap): buffer-band retest edge cases"
```

---

### Task 6: Accessors — mitigate, entry_ready_levels, lowest_struct_low, snapshot

**Files:**
- Modify: `strategies/trap_seller_detection.py`
- Test: `tests/test_trap_seller_detection.py`

- [ ] **Step 1: Write the failing test**

```python
def test_mitigate_and_accessors():
    reg = TrapLevelRegistry(band=2.0)
    reg.on_candle({"low": 363.30, "high": 420.35}, anchor_id="c1")
    reg.on_tick(360.00); reg.on_tick(430.00); reg.on_tick(364.00)
    assert [lv.anchor_id for lv in reg.entry_ready_levels()] == ["c1"]
    reg.mitigate("c1")
    assert reg.get("c1").state is LevelState.MITIGATED
    assert reg.entry_ready_levels() == []
    assert reg.active_levels() == []


def test_lowest_struct_low_and_snapshot():
    reg = TrapLevelRegistry(band=2.0)
    reg.on_candle({"low": 363.30, "high": 420.35}, anchor_id="c1")
    reg.on_candle({"low": 309.50, "high": 410.00}, anchor_id="c2")
    assert reg.lowest_struct_low() == 309.50
    snap = reg.snapshot()
    assert isinstance(snap, list) and len(snap) == 2
    assert {"anchor_id", "entry_l", "sl_h", "struct_low", "state"} <= set(snap[0].keys())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_trap_seller_detection.py -q`
Expected: FAIL with `AttributeError: ... 'mitigate'`

- [ ] **Step 3: Write minimal implementation**

Add to `TrapLevelRegistry`:

```python
    def entry_ready_levels(self) -> List[TrapLevel]:
        return [lv for lv in self._levels.values() if lv.state is LevelState.ENTRY_READY]

    def mitigate(self, anchor_id: object) -> None:
        lv = self._levels.get(anchor_id)
        if lv is not None:
            lv.state = LevelState.MITIGATED

    def lowest_struct_low(self) -> float:
        active = [lv.struct_low for lv in self._levels.values() if lv.active]
        return min(active) if active else 0.0

    def snapshot(self) -> List[dict]:
        return [{"anchor_id": lv.anchor_id, "entry_l": lv.entry_l, "sl_h": lv.sl_h,
                 "struct_low": lv.struct_low, "state": lv.state.name}
                for lv in self._levels.values()]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_trap_seller_detection.py -q`
Expected: PASS (12 passed)

- [ ] **Step 5: Commit**

```bash
git add strategies/trap_seller_detection.py tests/test_trap_seller_detection.py
git commit -m "feat(trap): registry accessors (mitigate, entry_ready, struct_low, snapshot)"
```

**Phase 1 gate:** Run full suite `python -m pytest tests/ -q` — Expected: existing 150 + 12 new = 162 passed (old `SellerTrapDetector` still present, so the engine import is intact).

---

## Phase 2 — Engine Orchestration & Void Shield

### Task 7: Config — retest band % and v2 squareoff time

**Files:**
- Modify: `config/global_config.py:217-226` (add fields) and the getter block after `RETEST_ZONE_PERCENT` (~line 251)
- Test: `tests/test_trap_engine_v2.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_trap_engine_v2.py
from config.global_config import GlobalConfig


def test_trap_engine_has_retest_band_and_v2_squareoff():
    cfg = GlobalConfig()
    tc = cfg.trap_engine
    assert tc.RETEST_BAND_PCT >= 0.0
    assert tc.V2_SQUAREOFF_TIME == "15:15"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_trap_engine_v2.py -q`
Expected: FAIL with `AttributeError: ... 'RETEST_BAND_PCT'`

- [ ] **Step 3: Write minimal implementation**

Add to the mutable-fields block (near line 226):

```python
    _RETEST_BAND_PCT:     float = field(default=0.5,         init=False, repr=False)  # buffer band as % of entry_l (retest arm / invalidation)
    _V2_SQUAREOFF_TIME:   str   = field(default="15:15",     init=False, repr=False)  # NSE intraday v2 force-squareoff (HH:MM IST)
```

Add getters after the `RETEST_ZONE_PERCENT` property:

```python
    @property
    def RETEST_BAND_PCT(self) -> float:
        with self._lock:  # type: ignore[attr-defined]
            return self._RETEST_BAND_PCT

    @property
    def V2_SQUAREOFF_TIME(self) -> str:
        with self._lock:  # type: ignore[attr-defined]
            return self._V2_SQUAREOFF_TIME
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_trap_engine_v2.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add config/global_config.py tests/test_trap_engine_v2.py
git commit -m "feat(trap): config RETEST_BAND_PCT + V2_SQUAREOFF_TIME"
```

---

### Task 8: Engine uses per-leg TrapLevelRegistry (HTF) driven by candles

**Files:**
- Modify: `strategies/trap_trading_engine.py` — `__init__` (add `self._htf_reg`), new helper `_reg(leg_key)`, and `_feed_leg_tick` candle-close branch.
- Test: `tests/test_trap_engine_v2.py`

- [ ] **Step 1: Write the failing test**

```python
import asyncio
from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.trap_trading_engine import TrapTradingEngine
from strategies.trap_seller_detection import LevelState


def _engine():
    return TrapTradingEngine(EventBus(), GlobalConfig(), client_db=None)


def test_engine_builds_concurrent_htf_levels_per_leg():
    eng = _engine()
    lk = "NIFTY:23000:CE"
    reg = eng._reg(lk)                       # per-leg HTF registry
    reg.on_candle({"low": 363.30, "high": 420.35}, anchor_id="c1")
    reg.on_candle({"low": 380.00, "high": 410.00}, anchor_id="c2")
    assert len(reg.active_levels()) == 2     # both live concurrently
    assert eng._reg(lk) is reg               # stable per-leg instance
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_trap_engine_v2.py::test_engine_builds_concurrent_htf_levels_per_leg -q`
Expected: FAIL with `AttributeError: ... '_reg'`

- [ ] **Step 3: Write minimal implementation**

In `__init__` (alongside `self._htf_det`):

```python
        from strategies.trap_seller_detection import TrapLevelRegistry as _TLR  # noqa: F401
        # v2 multi-level: per-leg HTF registry + per-zone MTF registries.
        self._htf_reg: Dict[str, "TrapLevelRegistry"] = {}
        self._zone_mtf: Dict[tuple, "TrapLevelRegistry"] = {}   # (leg_key, anchor_id) -> MTF reg
        self._void_zones: set = set()                            # (leg_key, anchor_id) voided
```

Add method:

```python
    def _band_for(self, entry_l: float) -> float:
        pct = float(self._cfg.trap_engine.RETEST_BAND_PCT)
        return abs(float(entry_l)) * pct / 100.0

    def _reg(self, leg_key: str):
        from strategies.trap_seller_detection import TrapLevelRegistry
        reg = self._htf_reg.get(leg_key)
        if reg is None:
            # band keyed off a nominal price; recomputed per level via _band_for at use sites
            reg = TrapLevelRegistry(band=0.0)
            self._htf_reg[leg_key] = reg
        return reg
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_trap_engine_v2.py::test_engine_builds_concurrent_htf_levels_per_leg -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add strategies/trap_trading_engine.py tests/test_trap_engine_v2.py
git commit -m "feat(trap): per-leg TrapLevelRegistry on the engine"
```

> **Note on band:** the registry stores one `band`, but the band scales with each level's price. Task 9 sets `reg.band = self._band_for(entry_l)` when a candle closes (using that candle's low), which is accurate enough since levels in one leg are close in price. If per-level bands are later required, move `band` onto `TrapLevel` — out of scope here.

---

### Task 9: `_feed_leg_tick` rewrite — registry tick + per-zone MTF + entry signal

**Files:**
- Modify: `strategies/trap_trading_engine.py` — replace `_feed_leg_tick` body; update `_on_mtf_entry_signal` signature to take `anchor_id`.
- Test: `tests/test_trap_engine_v2.py`

- [ ] **Step 1: Write the failing test**

```python
def test_macro_retest_then_mtf_confirm_fires_entry(monkeypatch):
    eng = _engine()
    fired = []
    monkeypatch.setattr(eng, "_on_mtf_entry_signal",
                        lambda leg, anchor, ltp: fired.append((leg, anchor, ltp)))
    lk = "NIFTY:23000:CE"
    reg = eng._reg(lk)
    reg.band = eng._band_for(363.30)
    reg.on_candle({"low": 363.30, "high": 420.35}, anchor_id="c1")
    # HTF lifecycle via ticks
    eng._feed_leg_tick(lk, _ts("09:20"), 360.0)   # SELLERS_IN
    eng._feed_leg_tick(lk, _ts("10:00"), 430.0)   # TRAPPED
    eng._feed_leg_tick(lk, _ts("11:00"), 364.0)   # ENTRY_READY (macro) -> spins zone MTF
    # zone MTF must itself complete a trap before entry fires
    mtf = eng._zone_mtf[(lk, "c1")]
    mtf.band = eng._band_for(364.0)
    mtf.on_candle({"low": 362.0, "high": 366.0}, anchor_id="m1")
    eng._feed_leg_tick(lk, _ts("11:01"), 361.0)   # mtf SELLERS_IN
    eng._feed_leg_tick(lk, _ts("11:02"), 368.0)   # mtf TRAPPED
    eng._feed_leg_tick(lk, _ts("11:03"), 362.5)   # mtf ENTRY_READY -> FIRE
    assert fired and fired[-1][1] == "c1"


def _ts(hhmm):
    from datetime import datetime
    from config.global_config import IST
    h, m = map(int, hhmm.split(":"))
    return datetime.now(IST).replace(hour=h, minute=m, second=0, microsecond=0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_trap_engine_v2.py::test_macro_retest_then_mtf_confirm_fires_entry -q`
Expected: FAIL (old `_feed_leg_tick` uses `SellerTrapDetector`, no `_zone_mtf`).

- [ ] **Step 3: Write minimal implementation**

Replace `_feed_leg_tick` with:

```python
    def _feed_leg_tick(self, leg_key: str, ts, ltp: float) -> None:
        """v2 multi-level: advance the per-leg HTF registry on every tick; for each
        zone that is ENTRY_READY, advance its own MTF registry; on MTF entry-ready
        for a non-void zone, fire the entry."""
        from strategies.trap_seller_detection import TrapLevelRegistry, LevelState
        tc = self._cfg.trap_engine
        try:
            base = ts.replace(second=0, microsecond=0)
        except Exception:
            base = datetime.now(IST).replace(second=0, microsecond=0)

        reg = self._reg(leg_key)
        newly_ready = reg.on_tick(ltp)
        for aid in newly_ready:
            lv = reg.get(aid)
            self._log_zone(leg_key, "HTF", aid, lv)
            if (leg_key, aid) not in self._zone_mtf and (leg_key, aid) not in self._void_zones:
                self._zone_mtf[(leg_key, aid)] = TrapLevelRegistry(band=self._band_for(lv.entry_l))

        # advance each armed zone's MTF registry; fire on MTF entry-ready
        for (lk, aid), mtf in list(self._zone_mtf.items()):
            if lk != leg_key:
                continue
            if (lk, aid) in self._void_zones:
                continue
            mready = mtf.on_tick(ltp)
            if mready:
                self._on_mtf_entry_signal(leg_key, aid, float(ltp))

        # build HTF + per-zone-MTF candles from the premium tick
        self._build_leg_candles(leg_key, base, ltp)

    def _build_leg_candles(self, leg_key: str, base, ltp: float) -> None:
        tc = self._cfg.trap_engine
        # HTF (75m) candle -> reg.on_candle
        self._roll_candle(leg_key, tc.HTF_MINUTES, base, ltp, target="htf")
        # MTF (5m) candle -> each armed zone's MTF reg
        self._roll_candle(leg_key, tc.MTF_MINUTES, base, ltp, target="mtf")

    def _roll_candle(self, leg_key: str, tf: int, base, ltp: float, target: str) -> None:
        minute = base.hour * 60 + base.minute
        floored = minute - (minute % tf)
        bstart = base.replace(hour=floored // 60, minute=floored % 60)
        key = (leg_key, tf)
        cur = self._leg_bars.get(key)
        if cur is None or cur["start"] != bstart:
            if cur is not None:
                candle = {"open": cur["o"], "high": cur["h"], "low": cur["l"], "close": cur["c"]}
                if target == "htf":
                    reg = self._reg(leg_key)
                    reg.band = self._band_for(candle["low"])
                    reg.on_candle(candle, anchor_id=cur["start"])
                else:
                    for (lk, aid), mtf in self._zone_mtf.items():
                        if lk == leg_key and (lk, aid) not in self._void_zones:
                            mtf.band = self._band_for(candle["low"])
                            mtf.on_candle(candle, anchor_id=(cur["start"], aid))
            self._leg_bars[key] = {"start": bstart, "o": ltp, "h": ltp, "l": ltp, "c": ltp}
        else:
            cur["h"] = max(cur["h"], ltp); cur["l"] = min(cur["l"], ltp); cur["c"] = ltp
```

Add `_log_zone` (replaces `_log_leg_transition` usage for zones):

```python
    def _log_zone(self, leg_key: str, tf_label: str, anchor_id, lv) -> None:
        parts = leg_key.split(":")
        underlying = parts[0] if parts else leg_key
        opt = parts[2] if len(parts) > 2 else "?"
        self._tlog(underlying).info(
            "%s %s zone=%s %s: entry_l=%.2f sl_h=%.2f struct_low=%.2f",
            opt, tf_label, anchor_id, lv.state.name, lv.entry_l, lv.sl_h, lv.struct_low)
```

Update `_on_mtf_entry_signal` signature:

```python
    def _on_mtf_entry_signal(self, leg_key: str, anchor_id, ltp: float) -> None:
        parts = leg_key.split(":")
        underlying = parts[0] if parts else leg_key
        opt = parts[2] if len(parts) > 2 else "?"
        self._tlog(underlying).info(
            "MTF ENTRY SIGNAL leg=%s zone=%s type=%s ltp=%.2f", leg_key, anchor_id, opt, ltp)
        try:
            asyncio.create_task(self._fire_entry_v2(underlying, opt, float(ltp), leg_key, anchor_id))
        except RuntimeError:
            pass
```

(The extra `_fire_entry_v2` args are added in Task 11; for this test `_on_mtf_entry_signal` is monkeypatched so the signature mismatch is not exercised yet.)

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_trap_engine_v2.py::test_macro_retest_then_mtf_confirm_fires_entry -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add strategies/trap_trading_engine.py tests/test_trap_engine_v2.py
git commit -m "feat(trap): _feed_leg_tick drives registry + per-zone MTF entry"
```

---

### Task 10: 1-minute void shield

**Files:**
- Modify: `strategies/trap_trading_engine.py` — add `_arm_void`, `_check_void_sweep`, `_clear_void_on_fresh_structure`; call from exec-tick + `_roll_candle`.
- Test: `tests/test_trap_engine_v2.py`

- [ ] **Step 1: Write the failing test**

```python
def test_void_shield_blocks_until_fresh_structure():
    eng = _engine()
    lk = "NIFTY:23000:CE"
    # arm void from a 1-min entry candle low of 362.0
    eng._arm_void(lk, "c1", entry_candle_low=362.0)
    assert (lk, "c1") in eng._void_zones
    # a later tick sweeping that low keeps/confirms void (no re-entry allowed)
    assert eng._check_void_sweep(lk, ltp=361.5) is True
    # a fresh 2nd-candle structure above the voided low lifts the void
    eng._clear_void_on_fresh_structure(lk, new_low=380.0)
    assert (lk, "c1") not in eng._void_zones
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_trap_engine_v2.py::test_void_shield_blocks_until_fresh_structure -q`
Expected: FAIL with `AttributeError: ... '_arm_void'`

- [ ] **Step 3: Write minimal implementation**

```python
    def _arm_void(self, leg_key: str, anchor_id, entry_candle_low: float) -> None:
        """Record the 1-min entry candle low for void detection and mark the zone void
        (blocks duplicate entries until a fresh structure forms)."""
        self._void_lows = getattr(self, "_void_lows", {})
        self._void_lows[(leg_key, anchor_id)] = float(entry_candle_low)
        self._void_zones.add((leg_key, anchor_id))

    def _check_void_sweep(self, leg_key: str, ltp: float) -> bool:
        """True if any voided zone's entry-candle low has been swept by this leg's ltp."""
        self._void_lows = getattr(self, "_void_lows", {})
        for (lk, aid), low in self._void_lows.items():
            if lk == leg_key and float(ltp) <= low:
                return True
        return False

    def _clear_void_on_fresh_structure(self, leg_key: str, new_low: float) -> None:
        """A fresh separate structure (new candle low ABOVE the voided low) lifts the void."""
        self._void_lows = getattr(self, "_void_lows", {})
        for (lk, aid), low in list(self._void_lows.items()):
            if lk == leg_key and float(new_low) > low:
                self._void_zones.discard((lk, aid))
                self._void_lows.pop((lk, aid), None)
```

Wire `_clear_void_on_fresh_structure` into `_roll_candle`'s HTF branch (after `reg.on_candle(...)`): `self._clear_void_on_fresh_structure(leg_key, candle["low"])`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_trap_engine_v2.py::test_void_shield_blocks_until_fresh_structure -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add strategies/trap_trading_engine.py tests/test_trap_engine_v2.py
git commit -m "feat(trap): 1-minute void shield (arm/sweep/lift)"
```

---

### Task 11: `_fire_entry_v2` — 14:45 block, void block, structural floor

**Files:**
- Modify: `strategies/trap_trading_engine.py` — extend `_fire_entry_v2(underlying, opt_type, entry_premium, leg_key, anchor_id)`; add cutoff + void guard; store `struct_floor`.
- Test: `tests/test_trap_engine_v2.py`

- [ ] **Step 1: Write the failing test**

```python
import asyncio
from datetime import time as _t
from unittest.mock import patch


def test_entry_blocked_after_1445(monkeypatch):
    eng = _engine()
    monkeypatch.setattr(eng, "_now_ist_time", lambda: _t(14, 46))
    eng._reg("NIFTY:23000:CE")  # ensure leg exists
    asyncio.get_event_loop().run_until_complete(
        eng._fire_entry_v2("NIFTY", "CE", 364.0, "NIFTY:23000:CE", "c1"))
    assert eng._v2_position is None


def test_entry_blocked_when_zone_void(monkeypatch):
    eng = _engine()
    monkeypatch.setattr(eng, "_now_ist_time", lambda: _t(11, 0))
    eng._void_zones.add(("NIFTY:23000:CE", "c1"))
    asyncio.get_event_loop().run_until_complete(
        eng._fire_entry_v2("NIFTY", "CE", 364.0, "NIFTY:23000:CE", "c1"))
    assert eng._v2_position is None


def test_entry_sets_structural_floor(monkeypatch):
    eng = _engine()
    monkeypatch.setattr(eng, "_now_ist_time", lambda: _t(11, 0))
    monkeypatch.setattr(eng, "_spot_cache", {"NIFTY": 23000.0})
    reg = eng._reg("NIFTY:23000:CE")
    reg.band = eng._band_for(363.30)
    reg.on_candle({"low": 309.50, "high": 420.35}, anchor_id="c1")  # struct_low 309.50
    asyncio.get_event_loop().run_until_complete(
        eng._fire_entry_v2("NIFTY", "CE", 364.0, "NIFTY:23000:CE", "c1"))
    assert eng._v2_position is not None
    assert eng._v2_position["struct_floor"] == 309.50
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_trap_engine_v2.py -k "blocked or structural_floor" -q`
Expected: FAIL (`_now_ist_time` missing / signature mismatch / no `struct_floor`).

- [ ] **Step 3: Write minimal implementation**

Add helper + extend `_fire_entry_v2`:

```python
    def _now_ist_time(self):
        return datetime.now(IST).time()

    def _entry_cutoff(self, underlying: str):
        # MCX trades the evening session -> no 14:45 cutoff; NSE blocks at ENTRY_CUTOFF_TIME.
        if self._market_close_for(underlying) == _MCX_MARKET_CLOSE:
            return time(22, 55, 0)
        s = self._cfg.trap_engine.ENTRY_CUTOFF_TIME
        try:
            return time(int(s[:2]), int(s[3:5]), 0)
        except Exception:
            return time(14, 45, 0)
```

At the TOP of `_fire_entry_v2`, before any work (new signature):

```python
    async def _fire_entry_v2(self, underlying: str, opt_type: str, entry_premium: float,
                             leg_key: str = "", anchor_id=None) -> None:
        # Hard time cutoff (NSE 14:45 / MCX 22:55).
        if self._now_ist_time() >= self._entry_cutoff(underlying):
            self._tlog(underlying).info("V2 ENTRY blocked: past entry cutoff")
            return
        # Void shield: never re-enter a voided zone.
        if leg_key and anchor_id is not None and (leg_key, anchor_id) in self._void_zones:
            self._tlog(underlying).info("V2 ENTRY blocked: zone %s void", anchor_id)
            return
        ...
```

When building `self._v2_position`, add the structural floor + zone identity:

```python
        struct_floor = 0.0
        if leg_key:
            reg = self._htf_reg.get(leg_key)
            lv = reg.get(anchor_id) if reg is not None else None
            if lv is not None:
                struct_floor = float(lv.struct_low)
        self._v2_position = {
            **payload, "entry_premium": float(entry_premium), "expiry": expiry,
            "ts": datetime.now(IST), "leg_key": leg_key, "anchor_id": anchor_id,
            "sl_5m": float(entry_premium), "sl_active": None,
            "entry_bucket": None, "_m1": None, "struct_floor": struct_floor,
        }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_trap_engine_v2.py -k "blocked or structural_floor" -q`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add strategies/trap_trading_engine.py tests/test_trap_engine_v2.py
git commit -m "feat(trap): v2 entry cutoff (14:45/22:55), void block, structural floor"
```

---

### Task 12: Structural-floor clamp on the two-tier trailing SL + re-arming

**Files:**
- Modify: `strategies/trap_trading_engine.py` — clamp in `_v2_track_exec_tick`; re-arm in `_v2_maybe_stop`/`_v2_publish_exit` path.
- Test: `tests/test_trap_engine_v2.py`

- [ ] **Step 1: Write the failing test**

```python
def test_trailing_sl_never_below_structural_floor():
    eng = _engine()
    eng._v2_position = {
        "underlying": "NIFTY", "strike": 22800, "opt_type": "CE",
        "sl_5m": 364.0, "sl_active": None, "entry_bucket": None, "_m1": None,
        "struct_floor": 309.50, "leg_key": "NIFTY:23000:CE", "anchor_id": "c1",
    }
    # simulate a 1m close far below the floor: clamp must hold the floor
    eng._v2_update_sl_on_1m_close(low=300.0, close=305.0)
    assert eng._v2_position["sl_active"] >= 309.50


def test_exit_rearms_leg_zone():
    eng = _engine()
    reg = eng._reg("NIFTY:23000:CE")
    reg.on_candle({"low": 363.30, "high": 420.35}, anchor_id="c1")
    eng._zone_mtf[("NIFTY:23000:CE", "c1")] = object()
    eng._rearm_after_exit("NIFTY:23000:CE", "c1")
    assert reg.get("c1").state.name == "MITIGATED"
    assert ("NIFTY:23000:CE", "c1") not in eng._zone_mtf
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_trap_engine_v2.py -k "floor or rearms" -q`
Expected: FAIL (`_rearm_after_exit` missing; clamp absent).

- [ ] **Step 3: Write minimal implementation**

Clamp in `_v2_update_sl_on_1m_close`:

```python
    def _v2_update_sl_on_1m_close(self, low: float, close: float) -> None:
        pos = self._v2_position
        if pos is None:
            return
        if float(close) < float(pos.get("sl_5m", 0.0)):
            floor = float(pos.get("struct_floor", 0.0) or 0.0)
            pos["sl_active"] = max(float(low), floor)   # never below the structural floor
```

Add re-arm + call it from `_v2_maybe_stop` (after clearing position) and the rotation path in `_fire_entry_v2`:

```python
    def _rearm_after_exit(self, leg_key: str, anchor_id) -> None:
        """After an exit, mitigate the traded zone and drop its MTF + void so the
        leg keeps scanning for the next trap sequence."""
        reg = self._htf_reg.get(leg_key)
        if reg is not None and anchor_id is not None:
            reg.mitigate(anchor_id)
        self._zone_mtf.pop((leg_key, anchor_id), None)
        self._void_zones.discard((leg_key, anchor_id))
        self._void_lows = getattr(self, "_void_lows", {})
        self._void_lows.pop((leg_key, anchor_id), None)
```

In `_v2_maybe_stop`, capture `leg_key`/`anchor_id` from `pos` before clearing, then after `self._v2_position = None` call `self._rearm_after_exit(lk, aid)`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_trap_engine_v2.py -k "floor or rearms" -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add strategies/trap_trading_engine.py tests/test_trap_engine_v2.py
git commit -m "feat(trap): structural-floor clamp on trailing SL + post-exit re-arming"
```

---

### Task 13: Remove dead LIFO `SellerTrapDetector` + fix imports/seeding

**Files:**
- Modify: `strategies/trap_seller_detection.py` (delete old `State`/`Level`/`SellerTrapDetector`).
- Modify: `strategies/trap_trading_engine.py` — remove the `SellerTrapDetector, State as _DetState` import; rewrite `_seed_leg_detection` to seed the registry via `reg.on_candle`; remove `_det`/`_log_leg_transition` if unused; update `_heartbeat` to read `self._htf_reg[lk].snapshot()`.
- Test: `tests/test_trap_engine_v2.py` + full suite.

- [ ] **Step 1: Write the failing test**

```python
def test_old_seller_trap_detector_is_gone():
    import strategies.trap_seller_detection as d
    assert not hasattr(d, "SellerTrapDetector")
    assert not hasattr(d, "State")


def test_seed_populates_registry(monkeypatch):
    eng = _engine()
    lk = "NIFTY:23000:CE"
    reg = eng._reg(lk)
    reg.band = eng._band_for(363.30)
    # seeding helper feeds historical HTF candles into the registry
    eng._seed_registry_candles(lk, [
        {"low": 363.30, "high": 420.35}, {"low": 380.0, "high": 410.0}])
    assert len(reg.active_levels()) == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_trap_engine_v2.py -k "gone or seed_populates" -q`
Expected: FAIL (old classes still present; `_seed_registry_candles` missing).

- [ ] **Step 3: Write minimal implementation**

Delete the original `State`, `Level`, `SellerTrapDetector` from `trap_seller_detection.py`. In `trap_trading_engine.py`:
- Remove `from strategies.trap_seller_detection import SellerTrapDetector, State as _DetState`.
- Remove `self._htf_det`/`self._mtf_det` dicts and the `_det` method.
- Add a seeding helper and call it from `_seed_leg_detection`:

```python
    def _seed_registry_candles(self, leg_key: str, candles: list) -> None:
        reg = self._reg(leg_key)
        for c in candles:
            reg.band = self._band_for(float(c["low"]))
            reg.on_candle({"low": float(c["low"]), "high": float(c["high"])})
```

In `_seed_leg_detection`, replace the `htf = self._det(...)` block: build the resampled HTF rows as before, then call `self._seed_registry_candles(leg_key, [{"low": r.low, "high": r.high} for ...])`.
- In `_heartbeat`, replace `self._htf_det.get(lk)` usage with `self._htf_reg.get(lk)` and render `reg.snapshot()` (zone count + states).

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/ -q`
Expected: PASS (full suite green; no import errors).

- [ ] **Step 5: Commit**

```bash
git add strategies/trap_seller_detection.py strategies/trap_trading_engine.py tests/test_trap_engine_v2.py
git commit -m "refactor(trap): remove dead LIFO SellerTrapDetector; seed registry; heartbeat reads registry"
```

**Phase 2 gate:** `python -m pytest tests/ -q` all green.

---

## Phase 3 — Telemetry & State Persistence

### Task 14: Persist + restore `_v2_position`

**Files:**
- Modify: `strategies/trap_trading_engine.py` — add `_persist_v2`, `_restore_v2`; call `_persist_v2` after entry/SL-update, `clear` on exit; call `_restore_v2` in `warm_start`.
- Test: `tests/test_trap_engine_v2.py`

- [ ] **Step 1: Write the failing test**

```python
def test_v2_position_persist_and_restore(tmp_path, monkeypatch):
    import data_layer.position_store as ps
    monkeypatch.setattr(ps, "_DIR", str(tmp_path))
    eng = _engine()
    eng._v2_position = {
        "underlying": "NIFTY", "strike": 22800, "opt_type": "CE", "qty": 65,
        "entry_premium": 364.0, "sl_5m": 364.0, "sl_active": 320.0,
        "entry_bucket": None, "_m1": None, "struct_floor": 309.50,
        "leg_key": "NIFTY:23000:CE", "anchor_id": "c1", "spot": 23000.0,
    }
    eng._persist_v2("NIFTY")
    eng2 = _engine()
    eng2._restore_v2("NIFTY")
    assert eng2._v2_position is not None
    assert eng2._v2_position["strike"] == 22800
    assert eng2._v2_position["struct_floor"] == 309.50
    assert eng2._v2_position["sl_active"] == 320.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_trap_engine_v2.py::test_v2_position_persist_and_restore -q`
Expected: FAIL (`_persist_v2` missing).

- [ ] **Step 3: Write minimal implementation**

```python
    @staticmethod
    def _persist_key_v2(symbol: str) -> str:
        return f"{symbol}_trap_v2"

    def _persist_v2(self, underlying: str) -> None:
        try:
            from data_layer import position_store as _ps
            pos = self._v2_position
            if pos is not None and pos.get("underlying") == underlying:
                store = {k: v for k, v in pos.items() if k != "_m1"}  # _m1 is a live builder
                store["ts"] = str(pos.get("ts", ""))
                _ps.save(self._persist_key_v2(underlying), store, product_type="MIS")
            else:
                _ps.clear(self._persist_key_v2(underlying))
        except Exception as exc:
            logger.warning("TrapEngine: v2 persist failed for %s: %s", underlying, exc)

    def _restore_v2(self, underlying: str) -> None:
        try:
            from data_layer import position_store as _ps
            saved = _ps.load(self._persist_key_v2(underlying))
            if not saved:
                return
            saved.setdefault("_m1", None)
            self._v2_position = saved
            self._tlog(underlying).info(
                "restored v2 position %s %s @ %.2f floor=%.2f sl_active=%s",
                saved.get("opt_type"), saved.get("strike"),
                float(saved.get("entry_premium", 0.0)),
                float(saved.get("struct_floor", 0.0)), saved.get("sl_active"))
        except Exception as exc:
            logger.warning("TrapEngine: v2 restore failed for %s: %s", underlying, exc)
```

Call `self._persist_v2(underlying)` at the end of `_fire_entry_v2` (on success) and inside `_v2_track_exec_tick` after `_v2_update_sl_on_1m_close`. Call `_ps.clear(...)` via `self._persist_v2(underlying)` after the position is cleared in `_v2_maybe_stop`/rotation. In `warm_start`, after `self._restore_trade(sym)`, add `self._restore_v2(sym)`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_trap_engine_v2.py::test_v2_position_persist_and_restore -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add strategies/trap_trading_engine.py tests/test_trap_engine_v2.py
git commit -m "feat(trap): persist + restore v2 position (key <UND>_trap_v2)"
```

---

### Task 15: 15:15 NSE / evening MCX v2 squareoff

**Files:**
- Modify: `strategies/trap_trading_engine.py` — add `_v2_squareoff_time`, a v2-aware force-exit, and call it from the EOD guard in `_on_candle`/`_check_touch_trigger`.
- Test: `tests/test_trap_engine_v2.py`

- [ ] **Step 1: Write the failing test**

```python
import asyncio
from datetime import time as _t


def test_v2_squareoff_closes_position_at_1515(monkeypatch):
    eng = _engine()
    eng._v2_position = {
        "underlying": "NIFTY", "strike": 22800, "opt_type": "CE", "qty": 65,
        "entry_premium": 364.0, "sl_5m": 364.0, "sl_active": None,
        "leg_key": "NIFTY:23000:CE", "anchor_id": "c1", "spot": 23000.0,
    }
    eng._leg_prem[("NIFTY", 22800, "CE")] = 400.0
    monkeypatch.setattr(eng, "_now_ist_time", lambda: _t(15, 16))
    asyncio.get_event_loop().run_until_complete(eng._v2_force_exit_if_eod("NIFTY"))
    assert eng._v2_position is None


def test_v2_squareoff_not_before_1515(monkeypatch):
    eng = _engine()
    eng._v2_position = {"underlying": "NIFTY", "strike": 22800, "opt_type": "CE",
                        "leg_key": "NIFTY:23000:CE", "anchor_id": "c1"}
    monkeypatch.setattr(eng, "_now_ist_time", lambda: _t(15, 0))
    asyncio.get_event_loop().run_until_complete(eng._v2_force_exit_if_eod("NIFTY"))
    assert eng._v2_position is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_trap_engine_v2.py -k "squareoff" -q`
Expected: FAIL (`_v2_force_exit_if_eod` missing).

- [ ] **Step 3: Write minimal implementation**

```python
    def _v2_squareoff_time(self, underlying: str):
        # MCX: evening session -> reuse the MCX market close (~23:30). NSE: V2_SQUAREOFF_TIME.
        if self._market_close_for(underlying) == _MCX_MARKET_CLOSE:
            return _MCX_MARKET_CLOSE
        s = self._cfg.trap_engine.V2_SQUAREOFF_TIME
        try:
            return time(int(s[:2]), int(s[3:5]), 0)
        except Exception:
            return time(15, 15, 0)

    async def _v2_force_exit_if_eod(self, underlying: str) -> None:
        pos = self._v2_position
        if pos is None or pos.get("underlying") != underlying:
            return
        if self._now_ist_time() < self._v2_squareoff_time(underlying):
            return
        ltp = float(self._leg_prem.get(
            (underlying, pos["strike"], pos["opt_type"]), pos.get("entry_premium", 0.0)))
        lk, aid = pos.get("leg_key", ""), pos.get("anchor_id")
        self._tlog(underlying).info("V2 EOD squareoff %s %d @ %.2f",
                                    pos["opt_type"], pos["strike"], ltp)
        self._v2_position = None
        await self._v2_publish_exit(pos, ltp, "eod")
        self._rearm_after_exit(lk, aid)
        self._persist_v2(underlying)   # clears the persisted file
```

Call it from the EOD guards: in `_on_candle` and `_check_touch_trigger`, before/instead of only `_force_exit_all`, add `await self._v2_force_exit_if_eod(c.symbol)` (resp. `tick.underlying`). Keep the existing `_force_exit_all` for v1.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_trap_engine_v2.py -k "squareoff" -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add strategies/trap_trading_engine.py tests/test_trap_engine_v2.py
git commit -m "feat(trap): v2 EOD squareoff 15:15 NSE / evening MCX (fixes orphaned _v2_position)"
```

---

### Task 16: v2 registry telemetry snapshot

**Files:**
- Modify: `strategies/trap_trading_engine.py` — add `v2_telemetry_snapshot()`.
- Test: `tests/test_trap_engine_v2.py`

- [ ] **Step 1: Write the failing test**

```python
def test_v2_telemetry_streams_registry_and_position():
    eng = _engine()
    lk = "NIFTY:23000:CE"
    reg = eng._reg(lk)
    reg.band = eng._band_for(363.30)
    reg.on_candle({"low": 363.30, "high": 420.35}, anchor_id="c1")
    reg.on_candle({"low": 380.00, "high": 410.00}, anchor_id="c2")
    snap = eng.v2_telemetry_snapshot()
    assert lk in snap["legs"]
    assert snap["legs"][lk]["zone_count"] == 2
    assert {"entry_l", "sl_h", "struct_low", "state"} <= set(snap["legs"][lk]["zones"][0].keys())
    assert "position" in snap
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_trap_engine_v2.py::test_v2_telemetry_streams_registry_and_position -q`
Expected: FAIL (`v2_telemetry_snapshot` missing).

- [ ] **Step 3: Write minimal implementation**

```python
    def v2_telemetry_snapshot(self) -> dict:
        """Concurrent multi-level registry + live v2 position, for the dashboard."""
        legs = {}
        for lk, reg in self._htf_reg.items():
            zones = reg.snapshot()
            legs[lk] = {
                "zone_count": len([z for z in zones if z["state"] not in ("MITIGATED", "INVALIDATED")]),
                "zones": zones,
                "lowest_struct_low": round(reg.lowest_struct_low(), 2),
                "void": sorted(aid for (l, aid) in self._void_zones if l == lk),
            }
        pos = self._v2_position
        position = None
        if pos is not None:
            position = {k: (str(v) if k == "ts" else v)
                        for k, v in pos.items() if k != "_m1"}
        return {"legs": legs, "position": position}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_trap_engine_v2.py::test_v2_telemetry_streams_registry_and_position -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add strategies/trap_trading_engine.py tests/test_trap_engine_v2.py
git commit -m "feat(trap): v2 registry telemetry snapshot (zones, states, position)"
```

---

### Task 17: Wire telemetry into the dashboard endpoint

**Files:**
- Modify: `ui_layer/dashboard_server.py` — find the existing trap telemetry route (search `telemetry_snapshot`) and add the v2 snapshot to the JSON payload.
- Test: manual (UI) + `python -m pytest tests/ -q` regression.

- [ ] **Step 1: Locate the route**

Run: `python -m pytest tests/ -q` first (baseline green). Then `grep -n "telemetry_snapshot\|trap" ui_layer/dashboard_server.py`.

- [ ] **Step 2: Write minimal implementation**

In the trap telemetry endpoint, add (guarded so a missing method never 500s — per the dashboard error convention):

```python
        try:
            v2 = trap_engine.v2_telemetry_snapshot() if hasattr(trap_engine, "v2_telemetry_snapshot") else {}
        except Exception as exc:
            v2 = {"error": str(exc)}
        payload["v2"] = v2     # existing payload dict for the trap telemetry response
```

- [ ] **Step 3: Run regression**

Run: `python -m pytest tests/ -q`
Expected: PASS (no test depends on the route shape; this is additive).

- [ ] **Step 4: Manual check**

Run the app (`python run_system.py --mode demo --ui`), open the dashboard, confirm the trap telemetry response includes a `v2` object with `legs`/`position`.

- [ ] **Step 5: Commit**

```bash
git add ui_layer/dashboard_server.py
git commit -m "feat(trap): expose v2 registry telemetry on the dashboard endpoint"
```

---

## Final Gate

- [ ] Run the full suite: `python -m pytest tests/ -q` — Expected: existing 150 + Phase-1 (12) + Phase-2/3 new tests all PASS.
- [ ] Update `CLAUDE.md` TrapTrading section: replace the "Known gaps" list with the delivered multi-level registry behavior, void shield, structural-floor SL, 15:15 squareoff, v2 persistence + telemetry.
- [ ] Update memory `project_trap_engine_current_state.md` to reflect the refactor (gaps resolved).

```bash
git add CLAUDE.md
git commit -m "docs(CLAUDE.md): Trap v2 multi-level registry shipped — gaps resolved"
```

---

## Self-Review Notes

- **Spec coverage:** §Goals 1 (registry → Tasks 2-6), 2 (concurrent on_tick → Task 3-4), 3 (void shield → Tasks 10-11), 4 (persistence Task 14, telemetry Tasks 16-17, cutoffs Tasks 11+15), 5 (re-arming → Task 12). Decisions 1-5 → Tasks 9 (per-zone MTF), 3+7 (band), 14 (persist+reseed), 11+15 (cutoffs), 11+12 (structural floor + trail).
- **Type consistency:** `LevelState`, `TrapLevel`, `TrapLevelRegistry`, `on_candle(c, anchor_id)`, `on_tick(price)->set`, `_reg`, `_band_for`, `_zone_mtf`, `_void_zones`, `_void_lows`, `_fire_entry_v2(..., leg_key, anchor_id)`, `_rearm_after_exit`, `_persist_v2`/`_restore_v2`, `_v2_force_exit_if_eod`, `v2_telemetry_snapshot` are used consistently across tasks.
- **Risk:** registry growth bounded by MITIGATED/INVALIDATED + re-arm pruning; a max-size cap can be added if intraday zone count grows large (out of scope, noted in spec §8).
