# Nifty Option Premium Bear Trap & OI Confirmation Engine — Design Spec

Status: approved design, ready for implementation planning.
Date: 2026-10-03 (revised same day — concurrent CE/PE execution + cross-side
MTM telemetry added, superseding the original one-side-at-a-time
constraint; see Sections 2, 4, 6, 7, 8, 8a, 10, 11)

## 1. Purpose

A new, standalone, intraday option-**buyer** strategy for NIFTY. It trades the
option's own 5-minute **premium chart** (CE and PE independently) looking for
a "bear trap" price-action pattern — premium sellers (writers) who sold short
below a reference candle's low get stopped out when premium reverses back
above that candle's high; when premium pulls back into the resulting zone, a
multi-strike Open Interest (OI) filter confirms writers are genuinely exiting
before a buy is armed and fired. Exit is EOD square-off only (3:15 PM IST),
by explicit choice — no stop-loss.

This is a **9th standalone strategy package** inside the existing
OptionChainBasedStrategy platform (see root `CLAUDE.md`), alongside
SellStraddle / OI-Bias-RSI-Exit / CAG Long Straddle / Iron Fly (live) and the
historical removed/retired ones. It follows the same "zero shared runtime"
mandate every standalone strategy here already follows (OI-Flow, Liquidity
Sweep, Liquidity Trap, CAG Straddle, Iron Fly) — own package, own order
events, own Topics, own execution bridge, own book manager, own DB file —
while reusing only genuine generic platform infrastructure (feed ticks,
instrument registry, base broker interface, base strategy book/gate classes).

## 2. Confirmed Mechanic (from direct user clarification)

- **"Bear trap" is a pure premium-chart pattern**, applied identically and
  independently to the CE premium chart and the PE premium chart — not a
  spot/underlying short-squeeze signal. In both cases we are, as option
  buyers, looking for the *option sellers'* stop-loss to get hit: when
  premium breaks below a reference candle's low then reverses back above its
  high, sellers who shorted premium in that breakdown are trapped. When
  premium later comes back inside the resulting zone, the remaining sellers
  are flushed out — that is the arming trigger for our buy.
- CE and PE each run this pattern **independently** on their own strike's
  premium chart.
- **Both sides may hold open positions concurrently** (updated 2026-10-03,
  direct user decision — supersedes the original "one side at a time"
  constraint). CE and PE run fully independent state machines, order
  events, and position tracking, with zero blocking between them. If PE
  arms and fires while CE is already `IN_POSITION`, PE opens anyway — see
  Section 8a for the cross-side MTM observability this requires.
- After a side's position closes (always via EOD — no SL), that side's state
  machine resets and re-arms, looking for a fresh Candle-1/Candle-2/trap
  sequence again the same day. Multiple sequential trades per side per day
  are expected and allowed.
- Exit is **EOD square-off at 3:15 PM IST only** — explicitly no stop-loss,
  per direct user decision. (Every other option-buyer strategy in this
  codebase carries a ₹2000/lot hard risk-cap backstop; this one deliberately
  does not, by explicit instruction.)

## 3. Pre-Market / Strike Selection

- Asset: NIFTY 50 index, **current-week** expiry only (resolved via the
  existing `InstrumentRegistry.get_active_expiry_strict("NIFTY")` — same
  function every other live strategy in this codebase already uses).
- Fetch previous trading day's High (PDH) and Low (PDL) from NIFTY spot (REST
  daily candle, reusing `data_layer/historical_candles.py`'s existing daily
  endpoint).
- Strike mapping (fixed for the day, computed once at pre-market):
  - `ce_strike` = strike price closest to PDL (rounded to NIFTY's strike
    step via the existing `ExchangeConfig.strike_steps`).
  - `pe_strike` = strike price closest to PDH.
- Timeframe: 5-minute bars for the premium pattern, built from the existing
  live 1-minute tick stream (`Topic.OPTION_TICK`) via an in-process bar
  accumulator — same bucket-accumulation pattern CAG Straddle/Liquidity
  Sweep/FVG already use (no dependency on `GlobalConfig.candle_timeframes`
  already including 5, though it typically does).

## 4. State Machine (per side, CE and PE independently)

States: `WAITING → BREAKDOWN_WATCH → TRAP_WATCH → ARMED_WAIT_REENTRY → IN_POSITION → (EOD close) → WAITING`

- **WAITING**: the next completed 5-min bar becomes `Candle1Ref` (`c1`).
  → `BREAKDOWN_WATCH`.
- **BREAKDOWN_WATCH**: on each new completed bar, if `bar.low < c1.low`, that
  bar becomes `c2`; `zone_hi = c1.close`, `zone_lo = c2.low` →
  `TRAP_WATCH`. If no breakdown bar ever appears, `c1` keeps rolling forward
  to the latest completed bar (a literal 1-bar-lookback reference — matches
  the spec's sequential "Candle 1 / Candle 2" reading, not a fixed anchor
  that waits forever for one specific candle).
- **TRAP_WATCH**: on each new completed bar after `c2`, if
  `bar.close > c1.high`, the trap is confirmed → `ARMED_WAIT_REENTRY`.
- **ARMED_WAIT_REENTRY**: checked on **every live tick** (not just bar
  close, for faster reaction) — if the current premium price is back inside
  `[zone_lo, zone_hi]`, run the OI filter (section 5) at that instant.
  - OI filter passes → fire a BUY at the current live premium → `IN_POSITION`.
  - OI filter fails → remain armed; re-check again on every subsequent
    zone touch (not a one-shot check) until EOD or this side's session reset.
- **IN_POSITION**: held until 3:15 PM IST EOD square-off. On close, this
  side's state resets to `WAITING` with a fresh `c1` taken from the next
  completed bar (re-arm, per user decision).

Each side's state machine is fully independent — a reset/arm/fire on CE
never touches PE's own state, and vice versa. There is no cross-side
blocking at all (updated 2026-10-03): both sides may be `IN_POSITION`
simultaneously. The only cross-side coupling left is observational, not a
gate — the instant a NEW entry fires on one side while the other side is
already `IN_POSITION`, the engine computes and logs a cross-side MTM
snapshot of the already-open side before/alongside firing the new entry
(see Section 8a).

## 5. Multi-Strike OI Filter

Run only at the arming-instant (zone re-entry), not continuously:

- Center strike = the **live current ATM** strike at the moment of the
  check (spot-derived, moves intraday) — **not** the fixed PDL/PDH-based
  `ce_strike`/`pe_strike` used for the premium pattern above. These are two
  deliberately separate concepts: the traded strike is fixed for the day
  from PDH/PDL; the OI-filter center floats with spot.
- For a **CE-side** arming check: aggregate OI across 5 strikes — the live
  ATM CE strike plus 4 strikes ITM (i.e. 4 strikes below ATM for calls) —
  sum = `call_oi_total`. Also sum the **PUT** OI across that same 5-strike
  band = `put_oi_total_same_band`.
  - Compare each sum's current value against its own value captured at
    today's session start (09:15 IST, first available snapshot).
  - **Pass condition**: `call_oi_total` is FALLING (lower now than at
    09:15) **AND** `put_oi_total_same_band` is RISING (higher now than at
    09:15).
- For a **PE-side** arming check: mirror exactly — 5-strike band = live ATM
  PE strike + 4 strikes ITM (4 strikes above ATM for puts); pass condition
  is PUT OI total FALLING and the same band's CALL OI total RISING.
- Every evaluation (pass or fail, with the actual OI numbers) is logged to a
  structured telemetry file for forward review (section 8) — this is the
  substitute for a backtest, since OI cannot be backtested (section 9).
- Live OI data source: the platform's existing `matrix_engine/option_matrix.py`
  `ChainSnapshot` (already self-initializing off the first real `INDEX_TICK`,
  per the platform bug fixed during OI-Flow's build) — read-only, same
  infra OI-Flow and D1TrapBearOnly already reuse. No new OI subscription
  mechanism needed.

## 6. Execution

- **Both sides may hold open positions concurrently** (updated 2026-10-03 —
  no platform-wide single-position gate). Each side's entry/exit/sizing
  logic is otherwise unchanged and fully independent of the other:
  - Entry: BUY at the live option premium LTP, at the moment that side's
    own OI filter passes.
  - Quantity: `lot_size × lot_multiplier` (per-deployment override, same
    pattern as every other strategy in this codebase) — sized independently
    per side.
  - Exit: EOD square-off at 3:15 PM IST, independently per side. No
    stop-loss on either side (explicit user decision — see section 2).
- Modes: `paper` / `paper_route` / `live`, same contract as every other
  execution bridge in this codebase (paper_route genuinely routes the order
  to the real broker to verify routing/permissions, with a locally
  simulated fill) — applies independently to whichever side(s) are
  currently open.

## 7. Module Breakdown

```
strategies/bear_trap_oi/
├── __init__.py
├── models.py          # Pure data classes: Bar, Candle1Ref, TrapZone, OiSnapshot, Position
├── candle_tracker.py  # BarAccumulator: 1-min ticks -> 5-min bars, per strike+side
├── trap_detector.py   # Pure functions: Candle1/Candle2/trap-confirm/zone/re-entry state machine
├── oi_filter.py        # Pure functions: multi-strike OI aggregation + rising/falling comparison
├── strike_selector.py # PDH/PDL fetch + CE-near-PDL / PE-near-PDH strike mapping
├── engine.py           # BearTrapOiStrategy: one book per (client,binding,underlying);
│                       #   owns 2 fully independent, concurrently-runnable side-trackers
│                       #   (CE, PE) with no cross-side blocking; wires ticks->bars->
│                       #   detector->OI filter->orders per side; computes and logs a
│                       #   cross-side MTM snapshot whenever a new entry fires while the
│                       #   other side is already open (see spec Section 8a)
├── book_manager.py     # BearTrapOiBookManager: spawn/stop books on deploy (reconcile loop,
│                       #   same pattern as every other *BookManager in this codebase)
├── events.py           # BearTrapOrderEvent / BearTrapFillEvent,
│                       #   Topic.BEAR_TRAP_OI_ORDER_REQUEST / BEAR_TRAP_OI_ORDER_FILL
├── store.py            # SQLite (data/bear_trap_oi.db): zones, OI audit trail, positions;
│                       #   restart-safe restore (re-resolve contract, re-subscribe feed,
│                       #   restore state-machine phase) — same precedent as OI-ORB Screener's
│                       #   store.py after its real position-loss-on-restart incident
└── telemetry.py        # Structured per-evaluation JSONL log (arm/reject/fire + the real OI
                          #   numbers), PLUS concurrent_entry_mtm_snapshot + periodic MTM
                          #   events while both sides are open (see Section 8a)
                          #   -> logs/bear_trap_oi/{underlying}_{date}.jsonl

execution_bridge/
└── bear_trap_oi_bridge.py   # confirm-then-finalize order routing (paper/paper_route/live),
                              #   own _record_history() call into data_layer.trade_history

scripts/
└── bear_trap_oi_backtest.py # Drives the REAL trap_detector.py classes (not a reimplementation,
                              #   per this codebase's own "backtest must drive the real class"
                              #   rule) against real historical 5-min option premium. OI filter
                              #   is NOT backtestable (section 9) — the script validates the
                              #   candle/trap/zone logic only, and reports candidate arming
                              #   events for manual/telemetry cross-reference once live.
```

Reused platform infrastructure (not reimplemented): `data_layer.base_feeder`
(live ticks), `data_layer.instrument_registry.REGISTRY` (strike/expiry
resolution), `matrix_engine.option_matrix` (live `ChainSnapshot` OI data),
`execution_bridge.base_broker` (`OrderRequest`/`OrderSide`/`OrderType`),
`strategies.core.base_book` / `book_manager` / `gate.can_trade`,
`data_layer.position_store`, `data_layer.trade_history`.

## 8. Data Dictionary

| Object | Fields | Notes |
|---|---|---|
| `Bar` | `ts: datetime`, `open, high, low, close: float` | 5-min, per (strike, side) |
| `Candle1Ref` | `bar: Bar` | the current reference candle |
| `TrapZone` | `state: WAITING\|BREAKDOWN_WATCH\|TRAP_WATCH\|ARMED_WAIT_REENTRY\|IN_POSITION`, `c1: Bar`, `c2: Bar \| None`, `zone_lo: float`, `zone_hi: float`, `confirmed_ts: datetime \| None` | one per side (CE, PE) |
| `OiSnapshot` | `strike: int`, `side: "CE"\|"PE"`, `oi: int`, `ts: datetime` | refreshed on every `MATRIX_SNAPSHOT` |
| `OiFilterResult` | `call_oi_total, put_oi_total_same_band: int`, `call_oi_trend, put_oi_trend: "RISING"\|"FALLING"\|"FLAT"`, `passed: bool`, `detail: str` | logged every evaluation, pass or fail |
| `Position` | `side: "CE"\|"PE"`, `strike: int`, `qty: int`, `entry_price: float`, `entry_ts: datetime`, `status: "open"\|"closed"` | **updated 2026-10-03**: two `Position` rows (one CE, one PE) may now coexist in `open` status simultaneously — no longer mutually exclusive |
| `MtmSnapshot` | `side: "CE"\|"PE"` (the side being observed), `observed_entry_price, observed_qty, observed_strike: as above`, `current_ltp: float`, `running_mtm: float`, `elapsed_sec: int`, `trigger: "concurrent_entry"\|"periodic"`, `ts: datetime`, `other_side_event: str \| None` | new — see Section 8a |

## 8a. Cross-Side MTM Snapshot on Concurrent Entry

Since both sides can now be open at once and neither carries a stop-loss,
the engine must give visibility into how far a still-open leg is running
against the user whenever a second leg opens — this is the adverse-
excursion data the user will review to pick an empirical SL for a future
1-lot live deployment. This is purely observability: **no action is taken
on this number, there is no auto-exit** — it exists only to be logged and
reviewed later.

- **Trigger**: the instant side B's OI filter passes and it is about to
  fire a BUY, if side A currently has `status == "open"`, the engine
  computes side A's live running MTM:
  `running_mtm = (side_A.current_ltp - side_A.entry_price) * side_A.qty`.
- This is logged as a `concurrent_entry_mtm_snapshot` telemetry event,
  capturing side A's `entry_price`, `entry_ts`, `strike`, elapsed holding
  time, and `running_mtm`, alongside side B's own about-to-fire details
  (strike, entry price) in `other_side_event` (e.g. `"PE entry fired @
  123.45"`). This is a distinct telemetry event type from the existing
  per-evaluation arm/reject/fire rows already specified in Section 5/9.
- **Periodic tracking, not just a single snapshot**: while both sides are
  concurrently open, the engine also logs a `MtmSnapshot` with
  `trigger="periodic"` for the already-running side on every completed
  5-min bar (same bar-driven cadence the price-action state machine already
  uses elsewhere in this spec) — so the user can review the full drawdown
  path over time, not just the instant the second leg opened.
- **Dashboard/terminal observability**: the live monitoring panel (Section
  11) must render both sides' open positions simultaneously, each with its
  own live MTM, whenever both are open — not a single "current position"
  slot.

## 9. Known Limitation — OI Cannot Be Backtested (honest disclosure)

Upstox's historical intraday candle API returns `oi=0` on every row for
option contracts — the same structural gap already documented for OI-Flow
and Liquidity Trap elsewhere in this codebase. This means:

- The **premium-chart trap/zone state machine** (sections 3-4) is fully
  backtestable against real historical 5-min option premium data, and
  `scripts/bear_trap_oi_backtest.py` will do so.
- The **multi-strike OI filter** (section 5) **cannot** be validated against
  historical data at all. It must be deployed paper/paper_route-only and
  validated via forward telemetry review (the `telemetry.py` JSONL log),
  same graduation discipline already established for OI-Flow and Liquidity
  Sweep in this codebase — watch a target number of real evaluations (both
  fired and rejected) before trusting the filter's thresholds or scaling to
  live capital.

This should be stated plainly to the client: the price-action half of the
strategy is backtest-validated before going live; the OI-confirmation half
is correct-by-construction (unit-tested logic) but forward-validated only.

## 10. Testing Plan

- `tests/bear_trap_oi/test_trap_detector.py` — state transitions (WAITING→
  BREAKDOWN_WATCH→TRAP_WATCH→ARMED→IN_POSITION), rolling-c1-forward when no
  breakdown occurs, re-arm after EOD close.
- `tests/bear_trap_oi/test_oi_filter.py` — aggregation across the correct
  5-strike ITM band for each side, RISING/FALLING classification vs 09:15
  baseline, pass/fail boundary cases.
- `tests/bear_trap_oi/test_strike_selector.py` — PDL/PDH-to-strike rounding.
- `tests/execution/test_bear_trap_oi_bridge.py` — paper/paper_route/live
  fill paths, `_record_history()` calls, and a test confirming CE and PE
  can both be `IN_POSITION` at the same time with no mutual blocking
  (replaces any prior implicit single-position assumption).
- `tests/bear_trap_oi/test_engine.py` — a test confirming a
  `concurrent_entry_mtm_snapshot` telemetry event is emitted with the
  correct running-MTM number when side B enters while side A is open, plus
  the periodic MTM logging cadence while both sides are concurrently open.
- `tests/bear_trap_oi/test_store.py` — restart-safe restore of zone state
  and open positions (now potentially two concurrently-open positions).

## 11. Deployment

- Strategy name in DB: `bear_trap_oi`. Registered in `strategies/registry.py`;
  run via `--strategies bear_trap_oi` (additive to whatever else is already
  running, same as every other strategy here).
- Dashboard: deploy-form entry in `monitor.html` (underlying fixed to
  NIFTY), live panel via `GET /api/beartrapoi/status` showing both sides'
  current zone state and the OI filter's last evaluation. **Updated
  2026-10-03**: the panel must render both sides' open positions
  concurrently, each with its own live MTM, whenever both are open — not a
  single "current position" slot (see Section 8a).
- First deployment: paper/paper_route only, per this codebase's established
  graduation discipline for every new strategy — no live capital until a
  real forward sample of OI-filter evaluations has been reviewed.
