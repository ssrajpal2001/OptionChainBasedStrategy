# Trap Trading v2 — Design Spec

**Date:** 2026-06-03
**Goal:** Rebuild the Trap Trading detection to the user's exact "seller-trap" model — a nested
HTF→MTF sequence detected per CE/PE leg on the option premium, executed by buying a fresh
ATM/ITM strike, managed with a two-tier 1-min-sweep stop loss and PE↔CE rotation, all driven
by per-instrument UI settings stored in `strategy_config.json`.

**Approach:** Keep the working plumbing built this session (day-strike selection, Upstox
historical seeding via curl_cffi, subscribe+pin tracked strikes, per-leg candle building,
telemetry/UI, MCX-aware EOD). Replace ONLY the detection state machine with a pure,
unit-tested module. Add execution-strike resolution, two-tier SL, rotation, and settings.

---

## Confirmed decisions (user, 2026-06-03)

1. **Trap candle:** ANY candle (irrespective of bullish/bearish). Its low = seller entry, high = seller SL.
2. **Trap sequence (exact, ordered):** **Below → Above → Return**
   - Price breaks **below** the reference candle's low (L) → sellers entered (sold), SL = high (H).
   - Price then breaks **above** H → sellers' SL hit → **trapped**.
   - Price **returns down to** L (sellers' breakeven) → entry condition met.
3. **Nested HTF→MTF:** the full Below→Above→Return runs on **HTF** first (we do NOT enter on HTF).
   When HTF completes, run the same on **MTF**; the **MTF** completion is the actual BUY trigger.
4. **Detection strikes:** the DTE-offset CE and PE (e.g. 8300CE, 9300PE), scanned **independently** per leg.
5. **Execution (buy) strike:** a **fresh ATM±N steps** strike resolved from the **current spot/future at
   entry** (`buy_depth` setting; N=0 ATM, N=1 one step ITM, …; CE ITM = below spot, PE ITM = above spot).
   Independent of the detection offset; detection and execution strikes may differ.
6. **Rotation:** **always rotate immediately** — when the opposite leg hits its MTF entry, close the
   running leg at market and open the new leg. One position at a time.
7. **Stop loss (two-tier):** SL = the **MTF entry candle's low**. If a **1-min candle closes below** that
   low → the **1-min candle's low** becomes the SL; if LTP **sweeps that 1-min low** → exit immediately.
8. **Underlying source:** SPOT for NSE/BSE indices, FUTURE for MCX commodities.

---

## Components

### A. Detection core — pure module `strategies/trap_seller_detection.py`
A deterministic, side-effect-free state machine over a stream of premium candles for ONE leg on
ONE timeframe. No I/O, no engine state — unit-tested in isolation.

States and transitions (per reference candle window):
```
WATCH        : track the latest candle as a reference [L, H].
SELLERS_IN   : entered when a later candle's price < L (break below). Record entry=L, sl=H.
TRAPPED      : entered when, while SELLERS_IN, price > H (break above). Sellers' SL hit.
ENTRY_READY  : entered when, while TRAPPED, price returns down to <= L. This is the signal.
```
- Input: ordered candles (open/high/low/close/timestamp) for the leg+timeframe.
- Output: current state + the active level `{entry_L, sl_H, trapped:bool}` and an
  `entry_ready` flag when ENTRY_READY is reached.
- Multiple reference candles may be tracked; resolve highest-priority per the existing
  "scan active levels" approach. (Exact multi-level policy: most-recent reference wins; documented in plan.)
- Reset rule: if price never breaks below L within the lookback/structure, the reference rolls
  forward to newer candles (no stale levels).

**Tests (TDD):** the 900/1000 example end-to-end; below-without-above (no trap); above-without-return
(trapped, no entry yet); return-before-above (invalid, stays SELLERS_IN); roll-forward of reference.

### B. Nested HTF→MTF orchestration (in the engine, per leg)
- Maintain two detector instances per leg: `htf_det` (htf_minutes) and `mtf_det` (mtf_minutes).
- Feed HTF candles to `htf_det`. Feed MTF candles to `mtf_det` ONLY after `htf_det` is ENTRY_READY.
- When `mtf_det` reaches ENTRY_READY → fire entry for that leg.
- After an entry (or invalidation), reset the MTF detector; HTF stays until its structure invalidates.

### C. Execution — fresh ATM/ITM strike
- On MTF entry for a leg (CE or PE):
  - `spot = current underlying spot/future` (SPOT for NSE/BSE, FUTURE for MCX).
  - `atm = round(spot / step) * step`.
  - CE buy strike = `atm - buy_depth*step` (ITM call below spot); PE buy strike = `atm + buy_depth*step` (ITM put above spot).
  - Subscribe+resolve that strike's broker symbol, place a BUY (long option), qty = lot * lot_multiplier.
- Record the open position (leg side, executed strike, entry premium, qty).

### D. Stop loss — two-tier 1-min sweep
- On entry, `sl_5m = low of the MTF entry candle`.
- Build/track the leg's 1-min candles. On each 1-min close:
  - if `1m_close < sl_5m` → set `sl_active = 1m_low` (the breaching 1-min candle's low).
- On each tick, if `sl_active` is set and `ltp < sl_active` → **exit immediately** (sweep).
- Until a 1-min closes below `sl_5m`, the effective stop is `sl_5m` (tick break below it also exits).
- Target: (open question — see below); else EOD force-exit (MCX-aware close already implemented).

### E. Rotation
- Both legs' detectors keep running while a position is open.
- If the OTHER leg reaches MTF ENTRY_READY while a position is live → close the running leg at
  market, then open the other leg via C. Strictly one position at a time.

### F. Settings — UI + `strategy_config.json` (per instrument, `indices.<SYM>.trap_trading`)
| Setting | Meaning | Default |
|---|---|---|
| `roundoff_step` | ATM rounding step | per-instrument (CRUDEOIL 100) |
| `dte_offset_ladder` | DTE→ITM steps for DETECTION strikes | `{">5":5,">4":4,">3":3,">2":2,">1":1}` |
| `lookback_days` | prior days of history to seed detection (min 2) | 2 |
| `buy_depth` | ATM±N steps for the EXECUTION strike | 0 (ATM) |
| `htf_minutes` | HTF timeframe | 75 |
| `mtf_minutes` | MTF timeframe | 5 |
| `sl_min_minutes` | the fine SL timeframe | 1 |
Underlying source (SPOT vs FUTURE) derived from instrument class (MCX → future), not a setting.
UI: a Trap section in the admin strategy editor mirroring SS/IC; values persist to JSON and feed the engine.

---

## Reused (unchanged) vs new
- **Reused:** `trap_strike_selection.py` (prev-day ATM + DTE), `_lock_day_strikes` / `_upstox_candles`
  (historical, parameterize lookback_days), `_ensure_subscribed_legs` + pin, `_feed_leg_tick`
  (per-leg candle build), telemetry/UI panel, MCX EOD.
- **New:** `trap_seller_detection.py` (pure), engine orchestration B/C/D/E, settings F + UI,
  replace the old `_process_htf/_process_mtf` state machine.

## Out of scope (this spec)
- Backtesting harness changes; per-broker MCX master fixes (Dhan/Angel) — tracked separately.

## Open questions to resolve during planning
1. **Target/profit exit:** is there a profit target for the long option, or exit only on SL / rotation / EOD?
2. **Multi-level policy** when several reference candles qualify (most-recent vs nearest-to-price).
3. **HTF invalidation:** when does an HTF ENTRY_READY expire if MTF never triggers (time/structure)?
