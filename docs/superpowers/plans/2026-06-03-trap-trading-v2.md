# Trap Trading v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild Trap Trading detection to the seller-trap model — nested HTF→MTF Below→Above→Return per CE/PE leg, fresh ATM±N execution strike, two-tier 1-min-sweep SL, PE↔CE rotation, per-instrument settings.

**Architecture:** Keep the working plumbing (day-strike selection, Upstox historical seeding, subscribe+pin, per-leg candle build, telemetry/UI, MCX EOD). Add a pure, unit-tested detection module `strategies/trap_seller_detection.py` and wire the engine to it, replacing the old `_process_htf/_process_mtf` state machine. Settings live in `strategy_config.json` and feed the engine.

**Tech Stack:** Python 3.12, asyncio, pytest, FastAPI + Alpine.js (UI), curl_cffi (already used for historical).

**Spec:** `docs/superpowers/specs/2026-06-03-trap-trading-v2-design.md`

---

## File Structure

- Create: `strategies/trap_seller_detection.py` — pure detection (state machine + LIFO levels). No I/O.
- Create: `tests/strategies/test_trap_seller_detection.py` — unit tests for the module.
- Modify: `data_layer/runtime_config.py` — trap_trading per-index defaults (settings).
- Modify: `strategies/trap_trading_engine.py` — orchestration B, execution C, SL D, rotation E; remove old HTF/MTF machine.
- Modify: `ui_layer/dashboard_server.py` — trap settings GET/POST in per-index config; telemetry fields.
- Modify: `ui_layer/templates/monitor.html` — trap settings form + panel fields.
- Test: `tests/strategies/test_trap_v2_engine.py` — orchestration/SL/rotation (using fakes).

---

## Task 1: Pure detection module — Below→Above→Return state machine

**Files:**
- Create: `strategies/trap_seller_detection.py`
- Test: `tests/strategies/test_trap_seller_detection.py`

- [ ] **Step 1: Write the failing test (core sequence)**

```python
# tests/strategies/test_trap_seller_detection.py
from strategies.trap_seller_detection import SellerTrapDetector, State

def _c(o, h, l, c):
    return {"open": o, "high": h, "low": l, "close": c}

def test_below_above_return_fires_entry():
    d = SellerTrapDetector()
    d.on_candle(_c(950, 1000, 900, 980))   # reference candle [L=900, H=1000]
    assert d.state == State.WATCH
    d.on_tick(880)                          # break BELOW 900 -> sellers in
    assert d.state == State.SELLERS_IN
    assert d.active_level.entry_l == 900 and d.active_level.sl_h == 1000
    d.on_tick(1010)                         # break ABOVE 1000 -> trapped
    assert d.state == State.TRAPPED
    assert d.entry_ready is False
    d.on_tick(900)                          # return DOWN to 900 -> entry
    assert d.state == State.ENTRY_READY
    assert d.entry_ready is True
```

- [ ] **Step 2: Run it to verify it fails**

Run: `python -m pytest tests/strategies/test_trap_seller_detection.py -q`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement the module**

```python
# strategies/trap_seller_detection.py
"""Pure seller-trap detection: Below -> Above -> Return, with a LIFO stack of levels.
No I/O, no engine state. One instance per leg+timeframe."""
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional


class State(Enum):
    WATCH = auto()        # tracking reference candle(s); no sellers in yet
    SELLERS_IN = auto()   # price broke below a reference low; sellers short, sl = high
    TRAPPED = auto()      # price broke above the high; sellers' sl hit
    ENTRY_READY = auto()  # price returned down to the entry low; BUY signal


@dataclass
class Level:
    entry_l: float        # reference candle low (seller entry / our trigger)
    sl_h: float           # reference candle high (seller sl)
    trapped: bool = False


class SellerTrapDetector:
    """Feed closed candles via on_candle(); feed live price via on_tick().
    Most-recent reference is active (LIFO); if a TRAPPED level's structure invalidates
    (price runs far past), it pops back to the prior valid level."""

    def __init__(self) -> None:
        self._levels: List[Level] = []     # LIFO stack; last = most recent/active
        self.state: State = State.WATCH
        self.entry_ready: bool = False

    @property
    def active_level(self) -> Optional[Level]:
        return self._levels[-1] if self._levels else None

    def on_candle(self, c: dict) -> None:
        """Record a new reference candle [low, high] as the most-recent level."""
        self._levels.append(Level(entry_l=float(c["low"]), sl_h=float(c["high"])))
        if self.state == State.WATCH:
            # newest reference becomes the one we watch for a break-below
            pass

    def on_tick(self, price: float) -> None:
        lv = self.active_level
        if lv is None:
            return
        if self.state in (State.WATCH, State.SELLERS_IN) and price < lv.entry_l and not lv.trapped:
            # break below the low -> sellers entered (or re-affirm)
            self.state = State.SELLERS_IN
        if self.state == State.SELLERS_IN and price > lv.sl_h:
            lv.trapped = True
            self.state = State.TRAPPED
        if self.state == State.TRAPPED and price <= lv.entry_l:
            self.state = State.ENTRY_READY
            self.entry_ready = True

    def consume_entry(self) -> None:
        """Caller acknowledges the entry; clear the flag (level stays for SL ref)."""
        self.entry_ready = False

    def invalidate_active(self) -> None:
        """SL hit on the active level -> pop it; fall back to the previous valid level."""
        if self._levels:
            self._levels.pop()
        self.state = State.WATCH if not self._levels else State.SELLERS_IN
        self.entry_ready = False
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python -m pytest tests/strategies/test_trap_seller_detection.py -q`
Expected: PASS.

- [ ] **Step 5: Add edge-case tests**

```python
def test_below_without_above_no_entry():
    d = SellerTrapDetector(); d.on_candle(_c(950,1000,900,980))
    d.on_tick(880)                     # below only
    assert d.state == State.SELLERS_IN and d.entry_ready is False

def test_above_without_return_trapped_no_entry():
    d = SellerTrapDetector(); d.on_candle(_c(950,1000,900,980))
    d.on_tick(880); d.on_tick(1010)
    assert d.state == State.TRAPPED and d.entry_ready is False

def test_return_before_above_stays_sellers_in():
    d = SellerTrapDetector(); d.on_candle(_c(950,1000,900,980))
    d.on_tick(880); d.on_tick(900)     # back to low but never trapped
    assert d.state == State.SELLERS_IN and d.entry_ready is False

def test_invalidate_pops_to_prior_level():
    d = SellerTrapDetector()
    d.on_candle(_c(950,1000,900,980))  # level A
    d.on_candle(_c(850,920,800,860))   # level B (most recent)
    assert d.active_level.entry_l == 800
    d.invalidate_active()              # B's SL hit -> pop
    assert d.active_level.entry_l == 900
```

- [ ] **Step 6: Run all module tests**

Run: `python -m pytest tests/strategies/test_trap_seller_detection.py -q`
Expected: 5 passed.

- [ ] **Step 7: Commit**

```bash
git add strategies/trap_seller_detection.py tests/strategies/test_trap_seller_detection.py
git commit -m "feat(trap-v2): pure seller-trap detection (Below->Above->Return, LIFO levels)"
```

---

## Task 2: Settings schema + defaults

**Files:**
- Modify: `data_layer/runtime_config.py` (the `trap_trading` defaults block, ~line 212)

- [ ] **Step 1: Add the v2 settings to the trap_trading default dict**

In `data_layer/runtime_config.py`, extend the `"trap_trading"` section default with:

```python
        "roundoff_step":       0,            # 0 = use ExchangeConfig step for the instrument
        "dte_offset_ladder":   {"5": 5, "4": 4, "3": 3, "2": 2, "1": 1},  # DTE>k -> v steps ITM
        "lookback_days":       2,            # >=2 prior days of history to seed detection
        "buy_depth":           0,            # execution strike = ATM +/- buy_depth steps
        "htf_minutes":         75,
        "mtf_minutes":         5,
        "sl_min_minutes":      1,
```

- [ ] **Step 2: Mirror them per-index** — ensure `_build_index_defaults()` includes a `trap_trading` section per index (add if missing) using the same dict, with `roundoff_step` defaulting to the instrument's `ExchangeConfig.strike_steps` value.

- [ ] **Step 3: Verify config loads**

Run: `python -c "from data_layer.runtime_config import RuntimeConfig; print(RuntimeConfig.index_section('CRUDEOIL','trap_trading'))"`
Expected: prints a dict containing `dte_offset_ladder`, `lookback_days`, `buy_depth`, `htf_minutes`, `mtf_minutes`.

- [ ] **Step 4: Commit**

```bash
git add data_layer/runtime_config.py
git commit -m "feat(trap-v2): per-index trap settings (dte ladder, lookback, buy_depth, tf)"
```

---

## Task 3: Engine orchestration — nested HTF→MTF per leg

**Files:**
- Modify: `strategies/trap_trading_engine.py`
- Test: `tests/strategies/test_trap_v2_engine.py`

- [ ] **Step 1: Write a failing orchestration test (fake candles)**

```python
# tests/strategies/test_trap_v2_engine.py
from strategies.trap_seller_detection import SellerTrapDetector, State

def test_mtf_only_fires_after_htf_ready():
    htf = SellerTrapDetector(); mtf = SellerTrapDetector()
    # HTF completes Below->Above->Return
    htf.on_candle({"open":950,"high":1000,"low":900,"close":980})
    htf.on_tick(880); htf.on_tick(1010); htf.on_tick(900)
    assert htf.state == State.ENTRY_READY
    # MTF must ALSO complete before we enter
    mtf.on_candle({"open":520,"high":540,"low":500,"close":530})
    mtf.on_tick(495); mtf.on_tick(545); mtf.on_tick(500)
    assert mtf.entry_ready is True   # entry only when BOTH ready
```

- [ ] **Step 2: Run it** — `python -m pytest tests/strategies/test_trap_v2_engine.py -q` → PASS (pure-module composition; this locks the orchestration contract).

- [ ] **Step 3: Replace the per-leg detection in the engine**

In `strategies/trap_trading_engine.py`:
- Add per-leg detectors: `self._htf_det: Dict[str, SellerTrapDetector]` and `self._mtf_det: Dict[str, SellerTrapDetector]`, keyed by leg_key.
- In `_feed_leg_tick`, on each closed HTF candle call `htf_det.on_candle(candle)`; feed every premium tick to `htf_det.on_tick(ltp)`.
- Only when `htf_det.state == State.ENTRY_READY`, feed MTF candles + ticks to `mtf_det`.
- When `mtf_det.entry_ready`, call `await self._fire_entry_v2(underlying, leg_side)` then `mtf_det.consume_entry()`.
- Delete the old `_process_htf` / `_process_mtf` state-machine bodies and the `_TrapState`/`_Phase` retest/armed logic they drove (keep `_TrapState` only if still referenced by telemetry; otherwise remove). Use `lookback_days` from settings in `_seed_leg_detection` (replace hardcoded `3`).

- [ ] **Step 4: Run the full strategy test suite** — `python -m pytest tests/strategies/ -q` → all pass.

- [ ] **Step 5: Commit**

```bash
git add strategies/trap_trading_engine.py tests/strategies/test_trap_v2_engine.py
git commit -m "feat(trap-v2): engine nested HTF->MTF detection per leg (replaces old state machine)"
```

---

## Task 4: Execution — fresh ATM±buy_depth strike

**Files:**
- Modify: `strategies/trap_trading_engine.py`

- [ ] **Step 1: Add a pure helper + test**

```python
# in tests/strategies/test_trap_v2_engine.py
from strategies.trap_trading_engine import exec_strike

def test_exec_strike_ce_itm_below_spot():
    assert exec_strike(8765, "CE", buy_depth=1, step=100) == 8700   # round 8765->8800 ATM, 1 step ITM = 8700
def test_exec_strike_pe_itm_above_spot():
    assert exec_strike(8765, "PE", buy_depth=1, step=100) == 8900   # ATM 8800, PE 1 step ITM above = 8900
def test_exec_strike_atm_when_depth_zero():
    assert exec_strike(8765, "CE", buy_depth=0, step=100) == 8800
```

- [ ] **Step 2: Run** → FAIL (exec_strike not defined).

- [ ] **Step 3: Implement `exec_strike` (module-level in trap_trading_engine.py)**

```python
def exec_strike(spot: float, opt_type: str, buy_depth: int, step: float) -> int:
    atm = round(spot / step) * step
    off = int(buy_depth) * int(step)
    return int(atm - off) if opt_type == "CE" else int(atm + off)
```

- [ ] **Step 4: Wire `_fire_entry_v2`** — resolve `spot` from `_spot_cache[underlying]`, compute `exec_strike(...)`, get active expiry, subscribe+pin the exec strike, place a BUY (long) via the existing order path, record the open position (leg side, exec strike, entry premium, qty). Run `python -m pytest tests/strategies/ -q` → pass.

- [ ] **Step 5: Commit**

```bash
git add strategies/trap_trading_engine.py tests/strategies/test_trap_v2_engine.py
git commit -m "feat(trap-v2): execution buys fresh ATM+/-buy_depth strike from live spot"
```

---

## Task 5: Two-tier 1-min-sweep stop loss

**Files:**
- Modify: `strategies/trap_trading_engine.py`

- [ ] **Step 1: Add a pure SL helper + tests**

```python
# tests/strategies/test_trap_v2_engine.py
from strategies.trap_trading_engine import sl_triggered

def test_sl_breaks_5m_low_on_tick():
    # before any 1m close below, the 5m low is the stop
    assert sl_triggered(ltp=498, sl_5m=500, sl_active=None) is True
def test_sl_uses_1m_low_after_close_below():
    # a 1m candle closed below 5m low; its low (495) is now the stop
    assert sl_triggered(ltp=494, sl_5m=500, sl_active=495) is True
    assert sl_triggered(ltp=496, sl_5m=500, sl_active=495) is False
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement `sl_triggered`**

```python
def sl_triggered(ltp: float, sl_5m: float, sl_active) -> bool:
    ref = sl_active if sl_active is not None else sl_5m
    return ltp < ref
```

- [ ] **Step 4: Wire SL into the position manager** — on entry set `pos.sl_5m = mtf_entry_candle_low`, `pos.sl_active = None`. On each 1-min close for the executed leg: if `close < pos.sl_5m` then `pos.sl_active = one_min_low`. On each tick: if `sl_triggered(ltp, pos.sl_5m, pos.sl_active)` → exit at market. Run `python -m pytest tests/strategies/ -q` → pass.

- [ ] **Step 5: Commit**

```bash
git add strategies/trap_trading_engine.py tests/strategies/test_trap_v2_engine.py
git commit -m "feat(trap-v2): two-tier 1-min-sweep stop loss"
```

---

## Task 6: PE↔CE immediate rotation

**Files:**
- Modify: `strategies/trap_trading_engine.py`

- [ ] **Step 1: Test the rotation decision (pure)**

```python
# tests/strategies/test_trap_v2_engine.py
from strategies.trap_trading_engine import should_rotate

def test_rotate_when_other_leg_entry_and_position_open():
    assert should_rotate(running_side="PE", signal_side="CE", has_position=True) is True
def test_no_rotate_same_side():
    assert should_rotate(running_side="PE", signal_side="PE", has_position=True) is False
def test_no_rotate_when_flat():
    assert should_rotate(running_side=None, signal_side="CE", has_position=False) is False
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement `should_rotate`**

```python
def should_rotate(running_side, signal_side, has_position: bool) -> bool:
    return bool(has_position and running_side is not None and signal_side != running_side)
```

- [ ] **Step 4: Wire rotation** — when a leg reaches MTF entry and `should_rotate(...)` is True, `await self._close_position("rotation")` for the runner, then `await self._fire_entry_v2(underlying, signal_side)`. Run `python -m pytest tests/strategies/ -q` → pass.

- [ ] **Step 5: Commit**

```bash
git add strategies/trap_trading_engine.py tests/strategies/test_trap_v2_engine.py
git commit -m "feat(trap-v2): immediate PE<->CE rotation on opposite-leg entry"
```

---

## Task 7: Settings UI + telemetry

**Files:**
- Modify: `ui_layer/dashboard_server.py` (per-index config GET/POST: include `trap_trading`; telemetry already has per-leg fields)
- Modify: `ui_layer/templates/monitor.html` (trap settings form in the admin strategy editor; panel shows detector state)

- [ ] **Step 1: Backend — return + accept trap_trading in `/api/admin/strategy/config/{index}`**

In `api_index_config_get`, add `"trap_trading": RuntimeConfig.index_section(idx, "trap_trading")`. In `api_index_config_save`, if `"trap_trading" in body`, call `RuntimeConfig.set_index_section(idx, "trap_trading", body["trap_trading"])` and reconfigure the trap engine for that underlying.

- [ ] **Step 2: Telemetry — expose detector state per leg** — in the trap `tracking` block, add `htf_state`/`mtf_state` from `eng._htf_det[leg].state.name` / `eng._mtf_det[leg].state.name`.

- [ ] **Step 3: UI — add a trap settings tab** under the admin strategy editor (mirror the sell_straddle tab) binding the new fields (`dte_offset_ladder` rows, `lookback_days`, `buy_depth`, `htf_minutes`, `mtf_minutes`), and show `HTF/MTF state` in the tracking panel.

- [ ] **Step 4: Verify** — `python -c "import ast; ast.parse(open('ui_layer/dashboard_server.py').read())"` and load the dashboard; open Admin → Strategy → CRUDEOIL → Trap, confirm fields populate and save.

- [ ] **Step 5: Commit**

```bash
git add ui_layer/dashboard_server.py ui_layer/templates/monitor.html
git commit -m "feat(trap-v2): trap settings UI + per-leg detector-state telemetry"
```

---

## Self-Review notes (coverage)
- Spec A → Task 1. Spec B → Task 3. Spec C → Task 4. Spec D → Task 5. Spec E → Task 6. Spec F → Tasks 2 + 7.
- Resolved decisions: no target (Tasks 4-6 never add a target exit); LIFO multi-level (Task 1 `invalidate_active`); HTF until-structure-breaks (Task 3 keeps HTF armed; only `invalidate_active` pops it).
- Reused plumbing untouched: `trap_strike_selection.py`, `_lock_day_strikes`/`_upstox_candles` (Task 3 only swaps the hardcoded lookback for the setting), `_ensure_subscribed_legs`+pin, `_seed_leg_detection` (now feeds the new detectors), MCX EOD.
