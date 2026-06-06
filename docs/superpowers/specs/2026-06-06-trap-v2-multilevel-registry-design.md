# Trap Trading Engine v2 — Multi-Level Concurrent Registry Refactor

**Date:** 2026-06-06
**Status:** Approved design (brainstorming complete)
**Affected files:** `strategies/trap_seller_detection.py`, `strategies/trap_trading_engine.py`, `data_layer/position_store.py`, `ui_layer/` (telemetry consumer), `tests/`

---

## 1. Background & Motivation

The live Trap path is **v2**: per-leg `SellerTrapDetector` instances driven off the **option premium** (not the index candle). The legacy v1 5-stage `_Phase` machine no longer trades (`_on_candle`/`_check_touch_trigger` are gutted to an EOD guard).

A deep-dive against two live structures — `Images/a.png` (1h HTF, NIFTY 09-JUN-2026 23000 CE premium) and `Images/b.png` (5m MTF, same contract) — exposes a fundamental flaw in the current detector: the **LIFO stack** model keeps only the *newest* reference candle as the active level (`active_level = self._levels[-1]`). When a newer candle forms or a high is swept, valid macro levels are prematurely discarded.

### What the charts show
- The **1st candle** (large red, premium drop) defines a seller-entry zone. The **2nd candle** forms inside/after it, but the **1st candle's zone must remain valid**.
- Price runs **above** the 2nd candle's high → sellers who entered at the 2nd candle are trapped (their SL is hit).
- The **8th candle returns precisely to the 1st candle's low (363.30)** — "the same point where the 1st candle entry happened" — and bounces. **This retest is the entry** ("when market came back we took the trade"), confirmed on the 5m timeframe ("mtf bearish trade started").
- **309.50** is annotated "lowest low is the stoploss" — the lowest low of the whole anchor structure, well below the 363.30 entry. It is both the **hard structural-breakdown / invalidation** level and the catastrophic stop floor.

The current LIFO model would have evicted the 1st-candle level the moment the 2nd candle formed, so the engine would **completely miss** the 8th-candle retest expansion.

---

## 2. Goals

1. Retain **all valid HTF traps concurrently** (ditch the LIFO stack; use a keyed registry).
2. **Multi-level concurrent monitoring** — every tick evaluated against all active, un-mitigated zones simultaneously.
3. **1-minute LTF void-lift shield** — block rapid-fire duplicate entries in choppy zones.
4. **V2 plumbing reconnection** — persistence, telemetry, and hard time cutoffs wired to `self._v2_position` (currently orphaned).
5. **Continuous re-arming lifecycle** — leg detectors reset cleanly after each trade to scan the next sequence.

### Non-goals (YAGNI)
- No revival of the legacy v1 5-stage machine; it stays as-is (EOD guard only).
- No UI chart rendering changes beyond consuming the new telemetry snapshot (backend-first; visual rendering optional follow-up).
- No new broker integrations or strike-selection changes (`trap_strike_selection.py` unchanged).

---

## 3. Locked Design Decisions

| # | Decision | Choice |
|---|----------|--------|
| 1 | MTF binding to zones | **Per-zone MTF detector** — each registry level owns its own MTF confirmation sub-detector, keyed `(leg, anchor_id)`. |
| 2 | Retest vs invalidation | **Buffer band** — retest touch within `[entry_l, entry_l + band]` arms `ENTRY_READY`; a tick below `struct_low − band` retires the level. `band` is configurable. |
| 3 | Restart persistence depth | **Position + SL state persisted; detectors re-seeded** from historical + live ticks on restart. |
| 4 | Time cutoffs across exchanges | **15:15 NSE, MCX evening** — NSE/BSE: entry-block 14:45, squareoff 15:15 IST. MCX: keep evening-session timing (~22:55 block / ~23:25 squareoff) via existing `_market_close_for` logic. |
| 5 | Executed trade stop | **Structural floor + two-tier trail** — initial hard stop = zone `struct_low`; the existing two-tier trailing SL (`sl_5m → sl_active`) trails up with profit but can never sit below the structural floor. |

---

## 4. Architecture

### 4.1 `trap_seller_detection.py` — registry replaces LIFO stack (pure, side-effect-free)

**`TrapLevel`** (one per zone):
- `anchor_id` — candle start timestamp (registry key / identity).
- `entry_l` — anchor candle low (retest entry reference).
- `sl_h` — anchor candle high (trapped-seller SL reference).
- `struct_low` — running lowest low of the structure from this anchor forward (updated on each `on_candle`). Drives invalidation and the structural stop floor.
- `state` — per-level enum: `WATCH → SELLERS_IN → TRAPPED → ENTRY_READY → MITIGATED` / `INVALIDATED`.

**`TrapLevelRegistry`** holds `Dict[anchor_id → TrapLevel]` — all levels live concurrently.
- `on_candle(c)`: append a **new** level keyed by its timestamp; **never pops** existing levels. For every active level, update `struct_low = min(struct_low, c.low)`.
- `on_tick(price) -> set[anchor_id]`: iterate **all** active levels and apply transitions:
  - `WATCH → SELLERS_IN` when `price < entry_l`
  - `SELLERS_IN → TRAPPED` when `price > sl_h`
  - `TRAPPED → ENTRY_READY` when `entry_l <= price <= entry_l + band` (retest touch)
  - any active state `→ INVALIDATED` when `price < struct_low − band` (hard structural breakdown)
  - Returns the set of `anchor_id`s that **newly** became `ENTRY_READY` this tick (so the engine fires per-zone MTF confirmation).
- Helpers: `active_levels()`, `mitigate(anchor_id)`, `lowest_struct_low()` (for telemetry / structural floor), snapshot accessor for telemetry.

`band` is supplied by the engine from config (no I/O inside the detector).

### 4.2 `trap_trading_engine.py` — per-zone MTF, void shield, lifecycle

- **Detection**: per leg, one HTF `TrapLevelRegistry`. For each zone that newly reaches `ENTRY_READY`, lazily create a **per-zone MTF sub-detector** keyed `(leg_key, anchor_id)`. Advance only the relevant zones' MTF detectors on tick. MTF confirmation → `_on_mtf_entry_signal(leg_key, anchor_id, ltp)`.
- **1-min Void Shield**: on entry, record the **1-minute entry candle's low**. If a later tick sweeps that low, mark `(leg_key, anchor_id)` **VOID** and block duplicate entries at that zone. The void lifts only when a **fresh separate 2nd-candle structure** forms — a new HTF level whose low is above the voided low.
- **Entry** (`_fire_entry_v2`): unchanged execution-strike logic (`exec_strike` = ATM ± `buy_depth` ITM from live spot, distinct from the detection strike). Adds the 14:45 entry-block guard. Stores `_v2_position` including the structural floor for the traded zone.
- **Exit / SL**: `struct_low` of the traded zone becomes the **initial structural stop floor**; the existing two-tier trailing SL (`sl_5m → sl_active`) trails up with profit but is clamped so it never drops below the floor. Rotation (opposite-leg) unchanged.
- **Persistence**: `_v2_position` (incl. `sl_5m`, `sl_active`, `entry_bucket`, `struct_floor`, void state) saved via `data_layer/position_store`; restored on start. Detection registry is **re-seeded** from historical + live ticks (not persisted).
- **Time cutoffs**: a v2-aware EOD path squares off `_v2_position` at 15:15 IST (NSE) / evening (MCX), fixing the current bug where `_force_exit_all` ignores `_v2_position`.
- **Telemetry**: extend the snapshot to stream, per leg, the registry — list of zones `{entry_l, sl_h, struct_low, state, mtf_state, void}`, zone count, active boundaries, and the live position. UI consumes this (rendering optional follow-up).
- **Re-arming**: after exit (two-tier SL or rotation), mark the traded zone `MITIGATED`, reset/clear its MTF detector + void flag, and let the registry keep scanning for the next sequence.

---

## 5. Data Flow

```
OPTION_TICK (tracked leg premium)
    │
    ▼
_feed_leg_tick(leg_key, ts, ltp)
    │  HTF registry.on_tick(ltp) ──► {newly ENTRY_READY anchor_ids}
    │      │
    │      ▼  (per zone)
    │  per-zone MTF detector.on_tick(ltp)
    │      │  mtf entry_ready?
    │      ▼
    │  _on_mtf_entry_signal(leg_key, anchor_id, ltp)
    │      │  void? blocked. 14:45? blocked.
    │      ▼
    │  _fire_entry_v2  ──► SignalPackage(LONG) ──► Topic.SIGNAL ──► execution_router
    │      │  store _v2_position (struct_floor = zone.struct_low)
    │      ▼
    │  persist _v2_position
    │
    ▼ (on executed-contract ticks)
_v2_track_exec_tick ──► two-tier trail clamped to struct_floor ──► _v2_maybe_stop
                                                                       │
15:15 IST / evening MCX ──► v2 EOD squareoff ──► _v2_publish_exit ──► re-arm leg
```

---

## 6. Testing Strategy (strict TDD)

**Red phase first** — write failing tests for `TrapLevelRegistry` and concurrent tracking before any engine code.

### Detection unit (`tests/test_trap_seller_detection.py`)
- Macro level holds while a micro level forms inside it (both present in the registry concurrently).
- Return-to-macro-low after a trap → that specific zone `ENTRY_READY` (the 363.30 retest).
- Tick below `struct_low − band` → `INVALIDATED`.
- Buffer-band edges: touch at `entry_l + band` arms; `entry_l − band` does **not** kill; `struct_low − band` does.
- Micro-level invalidation never touches the macro level.

### Engine integration (`tests/test_trap_engine_v2.py`)
- Replay the **363.30 scenario** (1st-candle trap holds, 8th candle returns to its low) → asserts a `LONG` `SignalPackage` fires.
- **Void shield** blocks a duplicate entry after the 1-min entry-low is swept, then re-arms on a fresh structure.
- **14:45** blocks new entries; **15:15** squareoff closes `_v2_position`.
- **Restart** restores position + SL and re-seeds detectors.
- **Structural floor** clamps the trailing stop (never drops below `struct_low`).

**Gate:** full existing suite (150) + new tests pass before the review pause.

---

## 7. Phased Implementation Roadmap

- **Phase 1 — Pure Detection Registry**: `TrapLevel` + `TrapLevelRegistry` replacing LIFO; concurrent multi-level transitions + buffer-band invalidation. TDD: detection unit suite (Red → Green).
- **Phase 2 — Engine Orchestration & Void Shield**: per-zone MTF map, `_feed_leg_tick` rewrite, 1-min void shield, 14:45 entry-block, structural-floor + two-tier trail, re-arming. TDD: engine integration suite.
- **Phase 3 — Telemetry & State Persistence**: `_v2_position` persistence (+ restore + re-seed), 15:15/evening v2 squareoff wiring, registry telemetry snapshot, UI consumption. TDD: persistence + squareoff + telemetry tests.

---

## 8. Risks & Open Items

- **Structure grouping**: `struct_low` is defined as the running minimum from each anchor forward; if real structures need explicit swing grouping, revisit in Phase 1 against the chart.
- **Registry growth**: levels accumulate intraday; bound via `MITIGATED`/`INVALIDATED` pruning and a max-age/size cap (config) to avoid unbounded memory.
- **Per-zone MTF cost**: N zones × MTF detectors — pruning retired zones keeps this bounded.
