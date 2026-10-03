# Bear Trap OI Confirmation Strategy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Bear Trap OI Confirmation Engine as a standalone strategy
package (`strategies/bear_trap_oi/`) in the OptionChainBasedStrategy
platform, with a real historical backtest script usable immediately (OI
filter bypassed, per the spec's documented limitation), plus the live
engine, execution bridge, and persistence needed for paper/live deployment.

**Architecture:** Pure-function detector/filter modules (`trap_detector.py`,
`oi_filter.py`, `strike_selector.py`) operating on simple dataclasses
(`models.py`), driven first by a historical-data backtest script and later
by the live `engine.py` (one book per client/binding, two independent
concurrent side-trackers for CE/PE, no cross-side blocking, with cross-side
MTM telemetry). The backtest script drives the *same* `TrapZone`/detector
classes the live engine uses — never a reimplementation — per this
codebase's established rule.

**Tech Stack:** Python 3 / asyncio (matches the rest of the platform),
`pytest`, SQLite (`store.py`), existing platform modules:
`data_layer.historical_candles`, `data_layer.instrument_registry.REGISTRY`,
`matrix_engine.option_matrix`, `execution_bridge.base_broker`,
`strategies.core.base_book`/`book_manager`/`gate`.

**Spec:** `docs/superpowers/specs/2026-10-03-bear-trap-oi-design.md`

## Global Constraints

- Strategy name in DB: `bear_trap_oi` (Section 11).
- NIFTY only, current-week expiry via `REGISTRY.get_active_expiry_strict("NIFTY")` (Section 3).
- 5-minute bars for the premium pattern (Section 3).
- No stop-loss on either side — EOD square-off at 3:15 PM IST only (Sections 2, 6).
- Both CE and PE sides run fully independently and may be open concurrently — no cross-side position gate (Sections 2, 4, 6).
- OI filter center = live current ATM strike, not the fixed PDL/PDH-based traded strike (Section 5).
- OI filter cannot be backtested (Upstox historical candles report `oi=0`) — backtest validates only the price-action trap/zone logic, OI filter bypassed/always-pass in backtest mode (Section 9).
- Every OI-filter evaluation (pass/fail) and every cross-side MTM event must be logged to `logs/bear_trap_oi/{underlying}_{date}.jsonl` (Sections 5, 8a).
- Zero shared runtime with other strategies — own Topics, own execution bridge, own DB file `data/bear_trap_oi.db` (Section 1, 7).

## Review Focus

- **No breakdown bar ever appears for a side all day** — `c1` must keep rolling forward to the latest completed bar forever, not get stuck or crash when the day ends with the state machine still in `WAITING`/`BREAKDOWN_WATCH`.
- **Trap confirms but price never re-enters the zone** — side stays `ARMED_WAIT_REENTRY` all day, never fires, and EOD-closes nothing (no open position) — must not be mistaken for an error.
- **Zone re-entry happens but OI filter is bypassed in backtest** — the backtest must count this as a trade anyway (per the user's explicit ask to evaluate the price-action engine with OI bypassed), while the live engine must never silently bypass it.
- **Both sides fire in the same 5-min bar** — CE and PE could both complete their own independent sequences on the same bar; the engine must open both without one blocking or corrupting the other's state.
- **A side's position is still open when the historical data window ends (end of the 1-week sample)** — the backtest must force-close at the data's own last bar (simulating EOD) rather than leaving a trade open with no exit, which would corrupt P&L totals.

---

## Task 1: Core data models

**Files:**
- Create: `strategies/bear_trap_oi/__init__.py`
- Create: `strategies/bear_trap_oi/models.py`
- Test: `tests/bear_trap_oi/test_models.py`
- Test: `tests/bear_trap_oi/__init__.py`

**Interfaces:**
- Produces: `Bar(ts, open, high, low, close)`, `TrapZoneState` (str enum:
  `WAITING`, `BREAKDOWN_WATCH`, `TRAP_WATCH`, `ARMED_WAIT_REENTRY`,
  `IN_POSITION`), `TrapZone(state, c1, c2, zone_lo, zone_hi, confirmed_ts)`,
  `OiSnapshot(strike, side, oi, ts)`, `OiFilterResult(call_oi_total,
  put_oi_total_same_band, call_oi_trend, put_oi_trend, passed, detail)`,
  `Position(side, strike, qty, entry_price, entry_ts, status)`,
  `MtmSnapshot(side, observed_entry_price, observed_qty, observed_strike,
  current_ltp, running_mtm, elapsed_sec, trigger, ts, other_side_event)`.

- [ ] **Step 1: Write the failing test**

```python
# tests/bear_trap_oi/test_models.py
from datetime import datetime, timezone
from strategies.bear_trap_oi.models import (
    Bar, TrapZoneState, TrapZone, OiSnapshot, OiFilterResult, Position,
    MtmSnapshot,
)


def _ts(minute):
    return datetime(2026, 10, 1, 9, minute, tzinfo=timezone.utc)


def test_bar_is_a_plain_dataclass():
    bar = Bar(ts=_ts(15), open=100.0, high=105.0, low=98.0, close=102.0)
    assert bar.high == 105.0
    assert bar.low == 98.0


def test_trap_zone_defaults_to_waiting_with_no_candles():
    zone = TrapZone(state=TrapZoneState.WAITING, c1=None, c2=None,
                     zone_lo=None, zone_hi=None, confirmed_ts=None)
    assert zone.state == TrapZoneState.WAITING
    assert zone.c1 is None


def test_oi_filter_result_carries_pass_and_detail():
    result = OiFilterResult(call_oi_total=120000, put_oi_total_same_band=90000,
                             call_oi_trend="FALLING", put_oi_trend="RISING",
                             passed=True, detail="call falling, put rising")
    assert result.passed is True
    assert "falling" in result.detail


def test_position_tracks_side_and_status():
    pos = Position(side="CE", strike=24500, qty=75, entry_price=120.5,
                    entry_ts=_ts(20), status="open")
    assert pos.status == "open"
    assert pos.side == "CE"


def test_mtm_snapshot_carries_running_mtm_and_trigger():
    snap = MtmSnapshot(side="CE", observed_entry_price=120.5, observed_qty=75,
                        observed_strike=24500, current_ltp=95.0,
                        running_mtm=(95.0 - 120.5) * 75, elapsed_sec=1800,
                        trigger="concurrent_entry", ts=_ts(45),
                        other_side_event="PE entry fired @ 60.25")
    assert snap.running_mtm == -1912.5
    assert snap.trigger == "concurrent_entry"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/bear_trap_oi/test_models.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'strategies.bear_trap_oi'`

- [ ] **Step 3: Write minimal implementation**

```python
# strategies/bear_trap_oi/__init__.py
```
(empty file)

```python
# strategies/bear_trap_oi/models.py
"""Pure data classes for the Bear Trap OI Confirmation strategy.

See docs/superpowers/specs/2026-10-03-bear-trap-oi-design.md for the full
mechanic. No behavior lives here — only shapes.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Literal, Optional

Side = Literal["CE", "PE"]
Trend = Literal["RISING", "FALLING", "FLAT"]
PositionStatus = Literal["open", "closed"]
MtmTrigger = Literal["concurrent_entry", "periodic"]


@dataclass(frozen=True)
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float


class TrapZoneState(str, Enum):
    WAITING = "WAITING"
    BREAKDOWN_WATCH = "BREAKDOWN_WATCH"
    TRAP_WATCH = "TRAP_WATCH"
    ARMED_WAIT_REENTRY = "ARMED_WAIT_REENTRY"
    IN_POSITION = "IN_POSITION"


@dataclass
class TrapZone:
    state: TrapZoneState
    c1: Optional[Bar]
    c2: Optional[Bar]
    zone_lo: Optional[float]
    zone_hi: Optional[float]
    confirmed_ts: Optional[datetime]


@dataclass(frozen=True)
class OiSnapshot:
    strike: int
    side: Side
    oi: int
    ts: datetime


@dataclass(frozen=True)
class OiFilterResult:
    call_oi_total: int
    put_oi_total_same_band: int
    call_oi_trend: Trend
    put_oi_trend: Trend
    passed: bool
    detail: str


@dataclass
class Position:
    side: Side
    strike: int
    qty: int
    entry_price: float
    entry_ts: datetime
    status: PositionStatus


@dataclass(frozen=True)
class MtmSnapshot:
    side: Side
    observed_entry_price: float
    observed_qty: int
    observed_strike: int
    current_ltp: float
    running_mtm: float
    elapsed_sec: int
    trigger: MtmTrigger
    ts: datetime
    other_side_event: Optional[str]
```

```python
# tests/bear_trap_oi/__init__.py
```
(empty file)

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/bear_trap_oi/test_models.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add strategies/bear_trap_oi/__init__.py strategies/bear_trap_oi/models.py tests/bear_trap_oi/__init__.py tests/bear_trap_oi/test_models.py
git commit -m "feat(bear-trap-oi): add core data models"
```

---

## Task 2: 5-minute bar accumulator

**Files:**
- Create: `strategies/bear_trap_oi/candle_tracker.py`
- Test: `tests/bear_trap_oi/test_candle_tracker.py`

**Interfaces:**
- Consumes: `Bar` from Task 1 (`strategies.bear_trap_oi.models`).
- Produces: `BarAccumulator(bucket_minutes: int = 5)` with
  `on_tick(ts: datetime, ltp: float) -> Optional[Bar]` — returns a completed
  `Bar` the instant a tick crosses into a new bucket (the *previous*
  bucket's bar), else `None`. `current_partial() -> Optional[Bar]` exposes
  the in-progress bar (used by `ARMED_WAIT_REENTRY`'s every-tick zone check,
  which needs the live, not-yet-closed price).

- [ ] **Step 1: Write the failing test**

```python
# tests/bear_trap_oi/test_candle_tracker.py
from datetime import datetime, timezone
from strategies.bear_trap_oi.candle_tracker import BarAccumulator


def _ts(minute, second=0):
    return datetime(2026, 10, 1, 9, minute, second, tzinfo=timezone.utc)


def test_ticks_within_one_bucket_produce_no_completed_bar_yet():
    acc = BarAccumulator(bucket_minutes=5)
    assert acc.on_tick(_ts(15, 0), 100.0) is None
    assert acc.on_tick(_ts(16, 30), 105.0) is None
    assert acc.on_tick(_ts(19, 59), 98.0) is None


def test_crossing_into_a_new_bucket_emits_the_completed_prior_bar():
    acc = BarAccumulator(bucket_minutes=5)
    acc.on_tick(_ts(15, 0), 100.0)
    acc.on_tick(_ts(16, 30), 105.0)
    acc.on_tick(_ts(19, 59), 98.0)
    completed = acc.on_tick(_ts(20, 0), 102.0)
    assert completed is not None
    assert completed.open == 100.0
    assert completed.high == 105.0
    assert completed.low == 98.0
    assert completed.close == 98.0


def test_current_partial_reflects_the_in_progress_bar():
    acc = BarAccumulator(bucket_minutes=5)
    acc.on_tick(_ts(15, 0), 100.0)
    acc.on_tick(_ts(16, 30), 93.0)
    partial = acc.current_partial()
    assert partial.low == 93.0
    assert partial.close == 93.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/bear_trap_oi/test_candle_tracker.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

```python
# strategies/bear_trap_oi/candle_tracker.py
"""5-minute (configurable) bar accumulator fed by raw live ticks.

Mirrors the bucket-accumulation pattern already used by
strategies/cag_straddle and the (removed) FVG strategy in this codebase —
built fresh here per this strategy's standalone mandate, no import.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from strategies.bear_trap_oi.models import Bar


def _bucket_start(ts: datetime, bucket_minutes: int) -> datetime:
    floored_minute = (ts.minute // bucket_minutes) * bucket_minutes
    return ts.replace(minute=floored_minute, second=0, microsecond=0)


class BarAccumulator:
    def __init__(self, bucket_minutes: int = 5):
        self._bucket_minutes = bucket_minutes
        self._bucket_ts: Optional[datetime] = None
        self._open: Optional[float] = None
        self._high: Optional[float] = None
        self._low: Optional[float] = None
        self._close: Optional[float] = None

    def on_tick(self, ts: datetime, ltp: float) -> Optional[Bar]:
        bucket_ts = _bucket_start(ts, self._bucket_minutes)
        completed: Optional[Bar] = None

        if self._bucket_ts is None:
            self._bucket_ts = bucket_ts
            self._open = self._high = self._low = self._close = ltp
            return None

        if bucket_ts != self._bucket_ts:
            completed = Bar(ts=self._bucket_ts, open=self._open,
                             high=self._high, low=self._low, close=self._close)
            self._bucket_ts = bucket_ts
            self._open = self._high = self._low = self._close = ltp
            return completed

        self._high = max(self._high, ltp)
        self._low = min(self._low, ltp)
        self._close = ltp
        return None

    def current_partial(self) -> Optional[Bar]:
        if self._bucket_ts is None:
            return None
        return Bar(ts=self._bucket_ts, open=self._open, high=self._high,
                    low=self._low, close=self._close)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/bear_trap_oi/test_candle_tracker.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add strategies/bear_trap_oi/candle_tracker.py tests/bear_trap_oi/test_candle_tracker.py
git commit -m "feat(bear-trap-oi): add 5-minute bar accumulator"
```

---

## Task 3: Trap detector state machine (the core logic)

**Files:**
- Create: `strategies/bear_trap_oi/trap_detector.py`
- Test: `tests/bear_trap_oi/test_trap_detector.py`

**Interfaces:**
- Consumes: `Bar`, `TrapZone`, `TrapZoneState` from Task 1.
- Produces: `on_bar_close(zone: TrapZone, bar: Bar) -> TrapZone` (advances
  `WAITING`→`BREAKDOWN_WATCH`→`TRAP_WATCH`→`ARMED_WAIT_REENTRY`, and rolls
  `c1` forward when no breakdown ever appears — pure function, returns a
  new `TrapZone`, never mutates in place). `check_zone_reentry(zone:
  TrapZone, live_price: float) -> bool` (True iff `zone.state ==
  ARMED_WAIT_REENTRY` and `zone.zone_lo <= live_price <= zone.zone_hi`).
  `close_position(zone: TrapZone) -> TrapZone` (returns a fresh `WAITING`
  zone with all candle refs cleared, per the "re-arm" decision).

- [ ] **Step 1: Write the failing test**

```python
# tests/bear_trap_oi/test_trap_detector.py
from datetime import datetime, timezone
from strategies.bear_trap_oi.models import Bar, TrapZone, TrapZoneState
from strategies.bear_trap_oi.trap_detector import (
    on_bar_close, check_zone_reentry, close_position,
)


def _bar(minute, o, h, l, c):
    return Bar(ts=datetime(2026, 10, 1, 9, minute, tzinfo=timezone.utc),
                open=o, high=h, low=l, close=c)


def _fresh_zone():
    return TrapZone(state=TrapZoneState.WAITING, c1=None, c2=None,
                     zone_lo=None, zone_hi=None, confirmed_ts=None)


def test_first_bar_becomes_c1_and_moves_to_breakdown_watch():
    zone = on_bar_close(_fresh_zone(), _bar(15, 100, 105, 98, 102))
    assert zone.state == TrapZoneState.BREAKDOWN_WATCH
    assert zone.c1.close == 102


def test_no_breakdown_rolls_c1_forward_to_latest_bar():
    zone = on_bar_close(_fresh_zone(), _bar(15, 100, 105, 98, 102))
    # next bar does NOT break c1's low (98) -> c1 rolls forward, stays in BREAKDOWN_WATCH
    zone = on_bar_close(zone, _bar(20, 102, 108, 100, 106))
    assert zone.state == TrapZoneState.BREAKDOWN_WATCH
    assert zone.c1.close == 106
    assert zone.c1.low == 100


def test_breakdown_bar_sets_zone_and_moves_to_trap_watch():
    zone = on_bar_close(_fresh_zone(), _bar(15, 100, 105, 98, 102))
    # breaks c1's low of 98
    zone = on_bar_close(zone, _bar(20, 97, 99, 90, 94))
    assert zone.state == TrapZoneState.TRAP_WATCH
    assert zone.c2.low == 90
    assert zone.zone_hi == 102  # c1.close
    assert zone.zone_lo == 90   # c2.low


def test_close_above_c1_high_confirms_trap_and_arms():
    zone = on_bar_close(_fresh_zone(), _bar(15, 100, 105, 98, 102))
    zone = on_bar_close(zone, _bar(20, 97, 99, 90, 94))
    # closes above c1.high (105)
    zone = on_bar_close(zone, _bar(25, 95, 110, 95, 108))
    assert zone.state == TrapZoneState.ARMED_WAIT_REENTRY
    assert zone.confirmed_ts is not None


def test_zone_reentry_detects_price_back_inside_zone():
    zone = on_bar_close(_fresh_zone(), _bar(15, 100, 105, 98, 102))
    zone = on_bar_close(zone, _bar(20, 97, 99, 90, 94))
    zone = on_bar_close(zone, _bar(25, 95, 110, 95, 108))
    assert zone.state == TrapZoneState.ARMED_WAIT_REENTRY
    assert check_zone_reentry(zone, live_price=96.0) is True   # inside [90,102]
    assert check_zone_reentry(zone, live_price=130.0) is False  # above zone


def test_zone_reentry_is_false_when_not_armed():
    zone = _fresh_zone()
    assert check_zone_reentry(zone, live_price=50.0) is False


def test_close_position_resets_to_fresh_waiting_zone():
    zone = on_bar_close(_fresh_zone(), _bar(15, 100, 105, 98, 102))
    zone = on_bar_close(zone, _bar(20, 97, 99, 90, 94))
    zone = on_bar_close(zone, _bar(25, 95, 110, 95, 108))
    closed = close_position(zone)
    assert closed.state == TrapZoneState.WAITING
    assert closed.c1 is None
    assert closed.c2 is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/bear_trap_oi/test_trap_detector.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

```python
# strategies/bear_trap_oi/trap_detector.py
"""Pure functions implementing the Bear Trap price-action state machine.

See docs/superpowers/specs/2026-10-03-bear-trap-oi-design.md Section 4.
These functions never mutate their TrapZone argument — each call returns
a new TrapZone, so the same functions can drive both the live engine and
the historical backtest without any behavioral drift between them.
"""
from __future__ import annotations

from dataclasses import replace

from strategies.bear_trap_oi.models import Bar, TrapZone, TrapZoneState


def on_bar_close(zone: TrapZone, bar: Bar) -> TrapZone:
    if zone.state == TrapZoneState.WAITING:
        return replace(zone, state=TrapZoneState.BREAKDOWN_WATCH, c1=bar)

    if zone.state == TrapZoneState.BREAKDOWN_WATCH:
        if bar.low < zone.c1.low:
            return replace(
                zone,
                state=TrapZoneState.TRAP_WATCH,
                c2=bar,
                zone_hi=zone.c1.close,
                zone_lo=bar.low,
            )
        # no breakdown yet: roll c1 forward to the latest bar
        return replace(zone, c1=bar)

    if zone.state == TrapZoneState.TRAP_WATCH:
        if bar.close > zone.c1.high:
            return replace(
                zone,
                state=TrapZoneState.ARMED_WAIT_REENTRY,
                confirmed_ts=bar.ts,
            )
        return zone

    # ARMED_WAIT_REENTRY / IN_POSITION: bar closes don't change state here
    # (re-entry is checked tick-by-tick via check_zone_reentry; IN_POSITION
    # transitions out only via close_position, called by the engine on EOD).
    return zone


def check_zone_reentry(zone: TrapZone, live_price: float) -> bool:
    if zone.state != TrapZoneState.ARMED_WAIT_REENTRY:
        return False
    return zone.zone_lo <= live_price <= zone.zone_hi


def close_position(zone: TrapZone) -> TrapZone:
    return TrapZone(state=TrapZoneState.WAITING, c1=None, c2=None,
                     zone_lo=None, zone_hi=None, confirmed_ts=None)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/bear_trap_oi/test_trap_detector.py -v`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add strategies/bear_trap_oi/trap_detector.py tests/bear_trap_oi/test_trap_detector.py
git commit -m "feat(bear-trap-oi): add trap detector state machine"
```

---

## Task 4: Strike selector (PDH/PDL → CE/PE strike mapping)

**Files:**
- Create: `strategies/bear_trap_oi/strike_selector.py`
- Test: `tests/bear_trap_oi/test_strike_selector.py`

**Interfaces:**
- Produces: `round_to_strike_step(price: float, step: int) -> int`,
  `map_strikes(pdh: float, pdl: float, step: int) -> tuple[int, int]`
  returning `(ce_strike, pe_strike)` where `ce_strike` is closest to `pdl`
  and `pe_strike` is closest to `pdh`.

- [ ] **Step 1: Write the failing test**

```python
# tests/bear_trap_oi/test_strike_selector.py
from strategies.bear_trap_oi.strike_selector import (
    round_to_strike_step, map_strikes,
)


def test_round_to_strike_step_rounds_to_nearest():
    assert round_to_strike_step(24513.0, 50) == 24500
    assert round_to_strike_step(24538.0, 50) == 24550
    assert round_to_strike_step(24525.0, 50) == 24550  # round-half-up


def test_map_strikes_ce_near_pdl_pe_near_pdh():
    ce_strike, pe_strike = map_strikes(pdh=24680.0, pdl=24510.0, step=50)
    assert ce_strike == 24500  # closest to PDL
    assert pe_strike == 24700  # closest to PDH
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/bear_trap_oi/test_strike_selector.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

```python
# strategies/bear_trap_oi/strike_selector.py
"""PDH/PDL -> traded-strike mapping. See spec Section 3.

CE strike = strike closest to the previous day's LOW.
PE strike = strike closest to the previous day's HIGH.
"""
from __future__ import annotations


def round_to_strike_step(price: float, step: int) -> int:
    return int(round(price / step) * step)


def map_strikes(pdh: float, pdl: float, step: int) -> tuple[int, int]:
    ce_strike = round_to_strike_step(pdl, step)
    pe_strike = round_to_strike_step(pdh, step)
    return ce_strike, pe_strike
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/bear_trap_oi/test_strike_selector.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add strategies/bear_trap_oi/strike_selector.py tests/bear_trap_oi/test_strike_selector.py
git commit -m "feat(bear-trap-oi): add PDH/PDL strike selector"
```

---

## Task 5: Backtest runner (drives the real detector against historical data)

**Files:**
- Create: `scripts/bear_trap_oi_backtest.py`
- Test: `tests/bear_trap_oi/test_backtest_runner.py`

**Interfaces:**
- Consumes: `Bar`, `TrapZone`, `TrapZoneState`, `Position` (Task 1);
  `on_bar_close`, `check_zone_reentry`, `close_position` (Task 3).
- Produces: `BacktestTrade(side, strike, entry_price, entry_ts, exit_price,
  exit_ts, pnl, c1, c2)` dataclass; `run_side_backtest(bars: list[Bar],
  side: Side, strike: int, lot_qty: int) -> list[BacktestTrade]` — pure,
  testable core that the CLI `main()` wraps with real Upstox data fetching.
  Entry fires on zone re-entry **unconditionally** (OI filter bypassed, per
  the spec's documented backtest limitation — this is what the user
  explicitly asked to evaluate). Exit is EOD: the LAST bar in the supplied
  `bars` list for a trading day forces a close at that bar's `close` price
  if a position is still open (Review Focus item 5) — bars must be grouped
  by calendar day by the caller before being handed to `run_side_backtest`
  (one call per trading day; the script's `main()` does this grouping).

- [ ] **Step 1: Write the failing test**

```python
# tests/bear_trap_oi/test_backtest_runner.py
from datetime import datetime, timezone
from strategies.bear_trap_oi.models import Bar
from scripts.bear_trap_oi_backtest import run_side_backtest


def _bar(minute, o, h, l, c):
    return Bar(ts=datetime(2026, 10, 1, 9, minute, tzinfo=timezone.utc),
                open=o, high=h, low=l, close=c)


def test_full_sequence_produces_one_winning_trade_closed_at_eod():
    bars = [
        _bar(15, 100, 105, 98, 102),   # c1
        _bar(20, 97, 99, 90, 94),      # c2 (breaks c1.low=98) -> zone [90,102]
        _bar(25, 95, 110, 95, 108),    # closes above c1.high=105 -> trap confirmed, armed
        _bar(30, 108, 112, 96, 97),    # dips back inside [90,102] -> entry @ 97 (bar close)
        _bar(315, 97, 150, 97, 145),   # last bar of day -> forced EOD close @ 145
    ]
    trades = run_side_backtest(bars, side="CE", strike=24500, lot_qty=75)
    assert len(trades) == 1
    trade = trades[0]
    assert trade.entry_price == 97.0
    assert trade.exit_price == 145.0
    assert trade.pnl == (145.0 - 97.0) * 75


def test_no_trap_ever_confirmed_produces_zero_trades():
    bars = [
        _bar(15, 100, 105, 98, 102),
        _bar(20, 102, 107, 101, 106),  # never breaks c1.low -> c1 rolls forward
        _bar(25, 106, 109, 104, 108),
    ]
    trades = run_side_backtest(bars, side="PE", strike=24700, lot_qty=75)
    assert trades == []


def test_armed_but_never_reentered_produces_zero_trades():
    bars = [
        _bar(15, 100, 105, 98, 102),   # c1
        _bar(20, 97, 99, 90, 94),      # c2 -> zone [90,102]
        _bar(25, 95, 110, 95, 108),    # trap confirmed, armed, zone [90,102]
        _bar(30, 108, 160, 108, 155),  # never comes back down into [90,102]
    ]
    trades = run_side_backtest(bars, side="CE", strike=24500, lot_qty=75)
    assert trades == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/bear_trap_oi/test_backtest_runner.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'scripts.bear_trap_oi_backtest'`

- [ ] **Step 3: Write minimal implementation**

```python
# scripts/bear_trap_oi_backtest.py
"""Historical backtest for the Bear Trap OI Confirmation strategy.

Drives the REAL strategies/bear_trap_oi/trap_detector.py state-machine
functions against historical 5-minute option premium bars (never a
reimplementation, per this codebase's "backtest must drive the real
class" rule).

LIMITATION (spec Section 9): Upstox's historical intraday candle API
returns oi=0 on every row for option contracts, so the multi-strike OI
filter CANNOT be backtested. This script evaluates ONLY the price-action
trap/zone engine, with the OI filter bypassed (every zone re-entry fires
unconditionally). Do not read these results as a validation of the full
live strategy -- only of its price-action half. See the spec for the
forward-telemetry plan that validates the OI half once live.

Usage:
    python scripts/bear_trap_oi_backtest.py --days 7 --strike-step 50
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import groupby
from typing import Literal

from strategies.bear_trap_oi.models import Bar, TrapZone, TrapZoneState
from strategies.bear_trap_oi.trap_detector import (
    on_bar_close, check_zone_reentry, close_position,
)

Side = Literal["CE", "PE"]


@dataclass
class BacktestTrade:
    side: Side
    strike: int
    entry_price: float
    entry_ts: datetime
    exit_price: float
    exit_ts: datetime
    pnl: float
    c1: Bar
    c2: Bar


def _fresh_zone() -> TrapZone:
    return TrapZone(state=TrapZoneState.WAITING, c1=None, c2=None,
                     zone_lo=None, zone_hi=None, confirmed_ts=None)


def run_side_backtest(bars: list[Bar], side: Side, strike: int,
                       lot_qty: int) -> list[BacktestTrade]:
    """Replay one trading day's bars for one side through the real
    detector. OI filter is bypassed (see module docstring)."""
    trades: list[BacktestTrade] = []
    zone = _fresh_zone()
    open_entry: tuple[float, datetime, Bar, Bar] | None = None  # price, ts, c1, c2

    for i, bar in enumerate(bars):
        is_last_bar = i == len(bars) - 1

        if zone.state == TrapZoneState.ARMED_WAIT_REENTRY and open_entry is None:
            # Check re-entry using this bar's own OHLC range (backtest has
            # no sub-bar ticks) -- re-entry fires if the bar's range ever
            # touched the zone.
            touched = (zone.zone_lo <= bar.high and bar.low <= zone.zone_hi)
            if touched:
                entry_price = bar.close
                open_entry = (entry_price, bar.ts, zone.c1, zone.c2)
                zone = TrapZone(state=TrapZoneState.IN_POSITION, c1=zone.c1,
                                 c2=zone.c2, zone_lo=zone.zone_lo,
                                 zone_hi=zone.zone_hi,
                                 confirmed_ts=zone.confirmed_ts)
                continue  # don't also run on_bar_close on the entry bar

        if zone.state not in (TrapZoneState.IN_POSITION,):
            zone = on_bar_close(zone, bar)

        if is_last_bar and open_entry is not None:
            entry_price, entry_ts, c1, c2 = open_entry
            exit_price = bar.close
            pnl = (exit_price - entry_price) * lot_qty
            trades.append(BacktestTrade(
                side=side, strike=strike, entry_price=entry_price,
                entry_ts=entry_ts, exit_price=exit_price, exit_ts=bar.ts,
                pnl=pnl, c1=c1, c2=c2,
            ))
            open_entry = None
            zone = close_position(zone)

    return trades


def _group_by_trading_day(bars: list[Bar]) -> list[list[Bar]]:
    keyfunc = lambda b: b.ts.date()
    return [list(g) for _, g in groupby(bars, key=keyfunc)]


async def _fetch_week_of_premium(underlying: str, ce_key: str, pe_key: str,
                                  days: int):
    """Fetches real 5-min premium history via the platform's existing
    Upstox intraday fetcher. Imported lazily so unit tests (which only
    exercise run_side_backtest) never need network/broker config."""
    from data_layer.historical_candles import fetch_upstox_intraday_1m

    end = datetime.now()
    start = end - timedelta(days=days)
    ce_1m = await fetch_upstox_intraday_1m(ce_key, start, end)
    pe_1m = await fetch_upstox_intraday_1m(pe_key, start, end)
    return ce_1m, pe_1m


def _resample_1m_to_5m(bars_1m: list[Bar]) -> list[Bar]:
    from strategies.bear_trap_oi.candle_tracker import BarAccumulator
    acc = BarAccumulator(bucket_minutes=5)
    out: list[Bar] = []
    for b in bars_1m:
        completed = acc.on_tick(b.ts, b.close)
        if completed is not None:
            out.append(completed)
    partial = acc.current_partial()
    if partial is not None:
        out.append(partial)
    return out


def _print_report(side: str, trades: list[BacktestTrade]) -> None:
    print(f"\n=== {side} side: {len(trades)} trade(s) ===")
    wins = [t for t in trades if t.pnl > 0]
    losses = [t for t in trades if t.pnl <= 0]
    total_pnl = sum(t.pnl for t in trades)
    print(f"Win/Loss: {len(wins)}W / {len(losses)}L")
    if trades:
        win_rate = len(wins) / len(trades) * 100
        avg_win = sum(t.pnl for t in wins) / len(wins) if wins else 0.0
        avg_loss = sum(t.pnl for t in losses) / len(losses) if losses else 0.0
        print(f"Win rate: {win_rate:.1f}%  Avg win: {avg_win:.2f}  Avg loss: {avg_loss:.2f}")
        print(f"Total P&L: {total_pnl:.2f}")
    for t in trades:
        print(f"  [{t.entry_ts}] strike={t.strike} C1(close={t.c1.close}, "
              f"high={t.c1.high}, low={t.c1.low}) C2(low={t.c2.low}) "
              f"entry={t.entry_price} -> exit({t.exit_ts})={t.exit_price} "
              f"pnl={t.pnl:.2f}")


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--strike-step", type=int, default=50)
    parser.add_argument("--lot-qty", type=int, default=75)
    args = parser.parse_args()

    from data_layer.historical_candles import fetch_upstox_daily
    from data_layer.instrument_registry import REGISTRY
    from strategies.bear_trap_oi.strike_selector import map_strikes

    today = datetime.now()
    daily = await fetch_upstox_daily("NSE_INDEX|Nifty 50", lookback_days=args.days + 2)
    pdh, pdl = daily[-2].high, daily[-2].low  # previous trading day

    ce_strike, pe_strike = map_strikes(pdh, pdl, args.strike_step)
    expiry = REGISTRY.get_active_expiry_strict("NIFTY", from_date=today)
    ce_key = REGISTRY.get_option_key("NIFTY", expiry, ce_strike, "CE")
    pe_key = REGISTRY.get_option_key("NIFTY", expiry, pe_strike, "PE")

    ce_1m, pe_1m = await _fetch_week_of_premium("NIFTY", ce_key, pe_key, args.days)
    ce_5m = _resample_1m_to_5m(ce_1m)
    pe_5m = _resample_1m_to_5m(pe_1m)

    all_trades: list[BacktestTrade] = []
    for day_bars in _group_by_trading_day(ce_5m):
        all_trades += run_side_backtest(day_bars, "CE", ce_strike, args.lot_qty)
    for day_bars in _group_by_trading_day(pe_5m):
        all_trades += run_side_backtest(day_bars, "PE", pe_strike, args.lot_qty)

    _print_report("CE", [t for t in all_trades if t.side == "CE"])
    _print_report("PE", [t for t in all_trades if t.side == "PE"])
    print(f"\n=== Combined: {len(all_trades)} trade(s), "
          f"Total P&L: {sum(t.pnl for t in all_trades):.2f} ===")


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/bear_trap_oi/test_backtest_runner.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add scripts/bear_trap_oi_backtest.py tests/bear_trap_oi/test_backtest_runner.py
git commit -m "feat(bear-trap-oi): add backtest runner driving the real detector"
```

---

## Task 6: Run the 1-week backtest and capture results

**Files:** none created — this task executes Task 5's script against real
data and records the output for the user.

- [ ] **Step 1: Confirm Upstox credentials are available**

Run: check `data/clients.db` has at least one client with a valid Upstox
access token (the script's `fetch_upstox_daily`/`fetch_upstox_intraday_1m`
calls need one). If none is available in this environment, this task
cannot execute and must be reported to the user as blocked (no synthetic
data substitution — this is a real-data backtest per the spec).

- [ ] **Step 2: Run the backtest for the most recent available week**

Run: `python scripts/bear_trap_oi_backtest.py --days 7 --strike-step 50`

- [ ] **Step 3: Capture full console output verbatim**

Save the full stdout to `data/bear_trap_oi_week1_backtest_report.txt` for
reference (matches this codebase's existing `data/*_report.json`
convention for backtest outputs).

```bash
python scripts/bear_trap_oi_backtest.py --days 7 --strike-step 50 \
  | tee data/bear_trap_oi_week1_backtest_report.txt
```

- [ ] **Step 4: Summarize for the user**

Report back (not committed to git — this is a results summary, not code):
total trade count and win/loss split per side, total P&L / win rate / avg
win / avg loss, the sample trade log lines (each shows C1 close/high/low,
C2 low, entry price+time, exit price+time, pnl) printed by `_print_report`,
and any observations — e.g. if `--days 7` only returns fewer actual trading
days due to a weekend/holiday, or if the resampled 5-min series has gaps
from illiquid off-peak ticks.

- [ ] **Step 5: Commit the saved report file**

```bash
git add data/bear_trap_oi_week1_backtest_report.txt
git commit -m "docs(bear-trap-oi): record 1-week price-action backtest results"
```

---

*(Remaining tasks for live deployment — OI filter, engine, book manager,
execution bridge, persistence, telemetry, dashboard wiring — are deferred
to a follow-up plan once the price-action backtest results above are
reviewed, per the spec's own graduation discipline. Task 6's results are
the gating checkpoint before that follow-up plan is written.)*
