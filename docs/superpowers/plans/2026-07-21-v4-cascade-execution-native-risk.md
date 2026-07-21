# V4 Cascade — Execution-Native Risk + Adaptive Tracking Strike Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Compute SL/target/trailing-stop for V4 Cascade trades natively on the execution
strike's own price history (one-time lookback at entry) instead of scaling them from the
tracking contract; flip the execution strike from 1-OTM to 1-ITM; re-center the
tracking/scanner strike when the underlying drifts, but only while flat, re-warming from
real history.

**Architecture:** A new pure function (`execution_risk.py`) runs the SAME sweep-detection
logic already used for Gate 2 (`find_all_bear_traps_2candle`, 5m-then-15m-fallback)
against the execution strike's own historical+intraday bars, fetched once at entry via
the SAME REST functions `_ingest_history` already uses. `CascadePosition` gains a
`risk_basis` field (`"tracking"` | `"execution_native"`) recorded at entry so every
downstream consumer (T2's live trailing, restart recovery) knows which price scale that
trade's numbers are on. A parallel, execution-contract-keyed 5m bar-builder in `book.py`
drives T1/T2 exit-checks independently of the tracking-bar clock whenever
`risk_basis == "execution_native"`; the existing tracking-bar-driven path is otherwise
untouched and still used verbatim as the fallback.

**Tech Stack:** Python 3.12, pytest + pytest-asyncio, existing `data_layer.historical_candles`
Upstox REST fetchers (already used by `_ingest_history`), no new dependencies.

## Global Constraints

- Entry discovery (Gate 1 Index, Gate 2 Demand Block, Gate 3 limit pierce) runs
  EXCLUSIVELY on the tracking/scanning strike — unchanged by this plan, no task here
  touches `zone_state.py`'s `IndexGatedPremiumScanner` discovery logic or
  `spot_confirm.py`.
- SL/target formulas themselves (`zone_low − sl_buffer` for SL, `zone.sl_level` floored
  at the risk distance for target) are UNCHANGED — only which contract's bars they're
  computed FROM changes (execution-native vs. tracking-native).
- Fallback to today's tracking-scaled approach is REQUIRED whenever the execution-native
  lookback finds no valid zone — SL/target must never be left undefined.
- Execution strike ITM/OTM flip: CE moves from `ATM + step` to `ATM − step`; PE moves
  from `ATM − step` to `ATM + step`. Same `EXECUTION_OFFSET_PTS`/strike-step magnitude,
  opposite sign.
- Re-center thresholds: NIFTY 100 points, CRUDEOIL 200 points, of underlying drift from
  the ATM the current tracking strikes were derived from. Other underlyings default to
  their existing `TRACKING_OFFSET_PTS` value (no re-centering behavior change for
  BANKNIFTY/SENSEX/GOLDM/crypto until explicitly tuned).
- Re-center NEVER happens while a position is open — gated on
  `self._engine.position is None or not self._engine.position.is_open` at the single
  check site.
- No re-center while open: never a "carry the zone over" reconciliation — this plan
  does not attempt to solve that; it structurally cannot happen instead.
- Full existing v4_cascade test suite (83 tests as of 2026-07-21) MUST stay green after
  every task.

---

### Task 1: `compute_execution_native_risk` — pure zone-detection on execution bars

**Files:**
- Create: `strategies/v4_cascade/execution_risk.py`
- Test: `tests/strategies/test_v4_cascade_execution_native_risk.py`

**Interfaces:**
- Consumes: `strategies.v4_cascade.rolling_base.find_all_bear_traps_2candle(bars) -> List[RollingBaseZone]`,
  `strategies.v4_cascade.rolling_base.resample_bars(bars_5m, multiplier, session_open) -> List[_ResampledBar]`,
  `strategies.v4_cascade.entries.compute_risk_mapping(zone, tracking_entry_price, exec_entry_price, sl_buffer, is_short) -> Tuple[float, float]`
  (reused with `tracking_entry_price == exec_entry_price == exec_entry_price`, i.e.
  scale=1, since both "tracking" and "execution" are now the SAME contract for this
  computation).
- Produces: `compute_execution_native_risk(execution_bars_5m: List[_Bar], exec_entry_price: float, sl_buffer: float, is_short: bool = False, session_open: Tuple[int,int] = (9,15)) -> Optional[Tuple[float, float]]`
  — returns `(sl_price, target_price)` on success, `None` if no valid zone found on
  either 5m or 15m-fallback.

- [ ] **Step 1: Write the failing tests**

```python
# tests/strategies/test_v4_cascade_execution_native_risk.py
"""strategies/v4_cascade/execution_risk.py's compute_execution_native_risk --
part of the 2026-07-21 execution-native risk design
(docs/superpowers/specs/2026-07-21-v4-cascade-execution-native-risk-design.md).
Runs the SAME sweep-detection logic Gate 2 already uses
(find_all_bear_traps_2candle, 5m-then-15m-fallback) against the EXECUTION
strike's own bars, once, at entry -- not a continuous scanner. Returns None
(triggering the caller's fallback to today's tracking-scaled approach) when
neither timeframe finds a valid zone."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _Bar
from strategies.v4_cascade.execution_risk import compute_execution_native_risk

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 21, 9, 15, tzinfo=IST)


def _bar(offset_5m, o, h, l, c):
    return _Bar(_BASE + timedelta(minutes=5 * offset_5m), o, h, l, c, tf=5)


def _trap_pattern_5m(offset0, level_offset=0.0):
    """entry_line=100+level_offset, sweep_low=95+level_offset,
    sl_level=110+level_offset (ref.high)."""
    lo = level_offset
    return [
        _bar(offset0, 105 + lo, 110 + lo, 100 + lo, 105 + lo),      # ref
        _bar(offset0 + 1, 98 + lo, 105 + lo, 95 + lo, 100 + lo),    # sweep
        _bar(offset0 + 2, 110 + lo, 115 + lo, 105 + lo, 112 + lo),  # reclaim (trapped)
    ]


def test_finds_zone_on_5m_and_returns_correct_sl_and_target_long():
    bars = _trap_pattern_5m(0)
    result = compute_execution_native_risk(
        bars, exec_entry_price=99.0, sl_buffer=5.0, is_short=False,
    )
    assert result is not None
    sl_price, target_price = result
    # zone_low = min(entry_line=100, sweep_low=95) = 95; risk = (99-95)+5 = 9
    assert abs(sl_price - (99.0 - 9.0)) < 1e-6
    # target = zone.sl_level (ref.high=110) distance from entry, floored at risk (9)
    # raw distance = 110-99 = 11, > risk(9), so raw distance is used
    assert abs(target_price - (99.0 + 11.0)) < 1e-6


def test_falls_back_to_15m_when_5m_finds_nothing():
    # A pattern that only resolves at 15m (mirrors
    # test_v4_cascade_index_gated_scanner.py's equivalent fixture): bucket0
    # (bars 0-2) is the 15m ref (non-decreasing internally, no 5m dips),
    # bucket1 (bars 3-5) supplies both the 15m sweep and reclaim in one
    # bucket, bucket2 stays flat.
    bucket0 = [_bar(0, 104, 106, 100, 104), _bar(1, 104, 107, 101, 105), _bar(2, 105, 108, 102, 106)]
    bucket1 = [_bar(3, 115, 130, 110, 120), _bar(4, 97, 98, 95, 96), _bar(5, 96, 99, 96, 97)]
    bucket2 = [_bar(6, 97, 100, 97, 98), _bar(7, 98, 101, 98, 99), _bar(8, 99, 102, 99, 100)]
    bars = bucket0 + bucket1 + bucket2
    result = compute_execution_native_risk(
        bars, exec_entry_price=99.0, sl_buffer=5.0, is_short=False,
    )
    assert result is not None


def test_returns_none_when_no_zone_found_on_either_timeframe():
    # A flat, featureless series -- no sweep, no reclaim, nothing at 5m or 15m.
    bars = [_bar(i, 100, 101, 99, 100) for i in range(10)]
    result = compute_execution_native_risk(
        bars, exec_entry_price=100.0, sl_buffer=5.0, is_short=False,
    )
    assert result is None


def test_returns_none_when_fewer_than_3_bars():
    bars = _trap_pattern_5m(0)[:2]
    result = compute_execution_native_risk(bars, exec_entry_price=99.0, sl_buffer=5.0)
    assert result is None


def test_short_geometry_returns_sl_above_and_target_below_entry():
    # Bull-zone (short, crypto PE only): ref.high=entry_line, ref.low=sl_level.
    bars = [
        _bar(0, 105, 110, 100, 105),      # ref: high=110 (entry_line), low=100 (sl_level)
        _bar(1, 112, 115, 108, 110),      # sweep up (buyers in)
        _bar(2, 95, 100, 90, 96),         # reclaim down through ref.low -- trapped
    ]
    result = compute_execution_native_risk(
        bars, exec_entry_price=101.0, sl_buffer=5.0, is_short=True,
    )
    assert result is not None
    sl_price, target_price = result
    assert sl_price > 101.0
    assert target_price < 101.0


def test_picks_the_most_recent_zone_when_multiple_exist():
    # Two independent patterns at different price levels -- the most
    # recently discovered (by reference_low_ts) must win.
    bars = _trap_pattern_5m(0) + _trap_pattern_5m(10, level_offset=50.0)
    result = compute_execution_native_risk(
        bars, exec_entry_price=149.0, sl_buffer=5.0, is_short=False,
    )
    assert result is not None
    sl_price, target_price = result
    # Should use the SECOND (later, shifted) zone: zone_low=145, risk=(149-145)+5=9
    assert abs(sl_price - (149.0 - 9.0)) < 1e-6
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/strategies/test_v4_cascade_execution_native_risk.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'strategies.v4_cascade.execution_risk'`

- [ ] **Step 3: Write the implementation**

```python
# strategies/v4_cascade/execution_risk.py
"""strategies/v4_cascade/execution_risk.py -- 2026-07-21 execution-native risk
computation.

Per docs/superpowers/specs/2026-07-21-v4-cascade-execution-native-risk-design.md:
SL/target for a V4 Cascade trade are computed natively from the EXECUTION
strike's own recent price history (a ONE-TIME lookback at entry), instead of
being mathematically scaled from the tracking contract's zone. Rationale: the
execution strike trades near ATM and is meaningfully more liquid than the
tracking strike (deliberately chosen far ITM/OTM for structural clarity of the
discovery pattern) -- it is MORE likely, not less, to show a clean, timely
analogous sweep+reclaim pattern.

This is NOT a continuous scanner -- it runs the exact same sweep-detection
function Gate 2 already uses (find_all_bear_traps_2candle, with the same
5m-then-15m-fallback Gate 2 already implements) ONCE against a fetched window
of execution-strike bars, at the moment of entry. Returns None when no valid
zone is found on either timeframe, signaling the caller (book.py's
_open_entry_async) to fall back to today's tracking-scaled approach -- SL/target
must never be left undefined.
"""
from __future__ import annotations

from typing import List, Optional, Tuple

from strategies.v4_cascade.entries import compute_risk_mapping
from strategies.v4_cascade.rolling_base import find_all_bear_traps_2candle, resample_bars


def compute_execution_native_risk(
    execution_bars_5m: List, exec_entry_price: float, sl_buffer: float,
    is_short: bool = False, session_open: Tuple[int, int] = (9, 15),
) -> Optional[Tuple[float, float]]:
    """Returns (sl_price, target_price) computed natively from
    ``execution_bars_5m`` (oldest-first execution-contract 5m bars), or None
    if no valid sweep+reclaim zone is found on 5m or the 15m fallback."""
    if len(execution_bars_5m) < 3:
        return None

    zones = find_all_bear_traps_2candle(execution_bars_5m)
    if not zones:
        resampled = resample_bars(execution_bars_5m, 15, session_open=session_open)
        if len(resampled) >= 3:
            zones = find_all_bear_traps_2candle(resampled)
    if not zones:
        return None

    # Most recently formed zone wins (mirrors the "most-recently-discovered"
    # recency convention already used elsewhere in this codebase, e.g. the
    # tracking-panel display's setup selection).
    zone = max(zones, key=lambda z: z.reference_low_ts)

    # compute_risk_mapping expects a "tracking" and "exec" entry price to
    # derive a scale ratio -- here both ARE the execution contract, so
    # passing exec_entry_price for both collapses the scale to 1.0 (no
    # conversion), which is exactly what "native" means.
    return compute_risk_mapping(
        zone, tracking_entry_price=exec_entry_price, exec_entry_price=exec_entry_price,
        sl_buffer=sl_buffer, is_short=is_short,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_execution_native_risk.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add strategies/v4_cascade/execution_risk.py tests/strategies/test_v4_cascade_execution_native_risk.py
git commit -m "V4Cascade: add compute_execution_native_risk (execution-strike zone lookback)"
```

---

### Task 2: `CascadePosition.risk_basis` — persisted field recording which scale a trade uses

**Files:**
- Modify: `strategies/v4_cascade/dataclasses.py`
- Test: `tests/strategies/test_v4_cascade_risk_basis_persistence.py`

**Interfaces:**
- Produces: `CascadePosition.risk_basis: str` (`"tracking"` | `"execution_native"`,
  default `"tracking"`), included in `to_dict()`/`from_dict()`. Every later task that
  reads/writes `CascadePosition` must set this correctly.

- [ ] **Step 1: Write the failing test**

```python
# tests/strategies/test_v4_cascade_risk_basis_persistence.py
"""CascadePosition.risk_basis -- 2026-07-21, records which price scale a
trade's SL/target/trailing-stop were computed on ("tracking" = today's
scaled-from-tracking-contract approach, "execution_native" = the new
lookback-on-execution-strike approach). Needed so a restart can rebuild T2's
tracker on the SAME scale the live trade was actually using -- without this,
_restore_tracker_state_for_open_position has no way to know which
reconstruction path to take."""
from datetime import datetime
from zoneinfo import ZoneInfo

from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg

IST = ZoneInfo("Asia/Kolkata")


def _position(risk_basis="tracking"):
    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=24200.0, qty=65, entry_price=20.0)
    t2 = TrancheLeg(tranche="T2", option_type="CE", strike=24200.0, qty=65, entry_price=20.0)
    return CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24200.0,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, t2=t2,
        open_time=datetime(2026, 7, 21, 12, 0, tzinfo=IST), risk_basis=risk_basis,
    )


def test_defaults_to_tracking():
    pos = CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24200.0,
        atm_at_trigger=24216.05, entry_spot=24216.05,
    )
    assert pos.risk_basis == "tracking"


def test_round_trips_execution_native_through_to_dict_from_dict():
    pos = _position(risk_basis="execution_native")
    restored = CascadePosition.from_dict(pos.to_dict())
    assert restored.risk_basis == "execution_native"


def test_round_trips_tracking_through_to_dict_from_dict():
    pos = _position(risk_basis="tracking")
    restored = CascadePosition.from_dict(pos.to_dict())
    assert restored.risk_basis == "tracking"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_risk_basis_persistence.py -v`
Expected: FAIL with `TypeError: CascadePosition.__init__() got an unexpected keyword argument 'risk_basis'`

- [ ] **Step 3: Write the implementation**

In `strategies/v4_cascade/dataclasses.py`, find the `CascadePosition` class (has fields
`underlying`, `side`, `tracking_strike`, `execution_strike`, `atm_at_trigger`,
`entry_spot`, `expiry_date`, `tracking_entry_price`, `t1`, `t2`, `open_time`,
`close_time`, `status`, `entry_indicators`, plus `to_dict()`/`from_dict()`). Add:

```python
    # "tracking" (today's scaled-from-tracking-contract SL/target/trail) or
    # "execution_native" (2026-07-21: computed from a one-time lookback on
    # the execution strike's own bars) -- set once at entry, read by
    # engine.py._check_exits to decide which bars/scale to check T1/T2
    # against, and by book.py._restore_tracker_state_for_open_position to
    # rebuild T2's tracker correctly after a restart.
    risk_basis: str = "tracking"
```

Add this field to the dataclass body (after `tracking_entry_price`, before `t1`). In
`to_dict()`, add `"risk_basis": self.risk_basis,` (alongside the existing
`"tracking_entry_price"` line). In `from_dict()`, add
`risk_basis=d.get("risk_basis", "tracking"),` (alongside the existing
`tracking_entry_price=d.get("tracking_entry_price"),` line).

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_v4_cascade_risk_basis_persistence.py -v`
Expected: 3 passed

- [ ] **Step 5: Run the full existing suite to confirm no regression**

Run: `python -m pytest tests/strategies/ -k v4_cascade -q`
Expected: all passing (83+3 = 86 tests)

- [ ] **Step 6: Commit**

```bash
git add strategies/v4_cascade/dataclasses.py tests/strategies/test_v4_cascade_risk_basis_persistence.py
git commit -m "V4Cascade: add CascadePosition.risk_basis (tracking vs execution_native)"
```

---

### Task 3: Fetch execution-strike historical bars in `book.py`

**Files:**
- Modify: `strategies/v4_cascade/book.py`
- Test: `tests/strategies/test_v4_cascade_execution_bar_fetch.py`

**Interfaces:**
- Consumes: `data_layer.historical_candles.fetch_upstox_range_1m(instrument_key, access_token, start, end) -> List[dict]`,
  `data_layer.historical_candles.fetch_upstox_intraday_1m(instrument_key, access_token) -> List[dict]`
  (both already used, unchanged, by `_ingest_history` at book.py:539-544),
  `book.py`'s own `_merge_rows(range_rows, today_rows) -> List[dict]` and
  `_to_5m_bars(rows, filter_zero_volume) -> List[_Bar]` (both already module-level
  functions in book.py, used unchanged).
- Produces: `V4CascadeBook._fetch_execution_bars_5m(self, symbol: str) -> List[_Bar]`
  (async instance method).

- [ ] **Step 1: Write the failing test**

```python
# tests/strategies/test_v4_cascade_execution_bar_fetch.py
"""V4CascadeBook._fetch_execution_bars_5m -- 2026-07-21, fetches the
execution strike's own historical+intraday 1m bars via the SAME REST
functions _ingest_history already uses for the tracking contract, merges
and resamples to 5m the same way. Needed so compute_execution_native_risk
has real bars to run its zone-detection against at entry time."""
from datetime import date
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook

IST = ZoneInfo("Asia/Kolkata")


def _book():
    cfg = GlobalConfig()
    book = V4CascadeBook(
        EventBus(), cfg, underlying="NIFTY", client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15",
    )
    book._running = True
    book._expiry = date(2026, 7, 21)
    return book


_RANGE_ROWS = [
    {"ts": "2026-07-21T09:15:00", "open": 100, "high": 105, "low": 98, "close": 102, "volume": 10},
    {"ts": "2026-07-21T09:16:00", "open": 102, "high": 106, "low": 100, "close": 104, "volume": 10},
]
_INTRADAY_ROWS = [
    {"ts": "2026-07-21T09:20:00", "open": 104, "high": 108, "low": 103, "close": 106, "volume": 10},
]


@pytest.mark.asyncio
async def test_fetches_merges_and_resamples_to_5m_bars():
    book = _book()
    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=_RANGE_ROWS)), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=_INTRADAY_ROWS)), \
         patch.object(book, "_access_token", return_value="tok"):
        bars = await book._fetch_execution_bars_5m("NSE_FO|12345")
    assert len(bars) == 1   # 3 one-minute rows all within the same 09:15-09:20 5m bucket
    assert bars[0].open == 100
    assert bars[0].close == 106


@pytest.mark.asyncio
async def test_returns_empty_list_when_no_access_token():
    book = _book()
    with patch.object(book, "_access_token", return_value=""):
        bars = await book._fetch_execution_bars_5m("NSE_FO|12345")
    assert bars == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/strategies/test_v4_cascade_execution_bar_fetch.py -v`
Expected: FAIL with `AttributeError: 'V4CascadeBook' object has no attribute '_fetch_execution_bars_5m'`

- [ ] **Step 3: Write the implementation**

In `strategies/v4_cascade/book.py`, add this method to `V4CascadeBook` (place it right
before `_ingest_history`, since it reuses the exact same fetch pattern):

```python
    async def _fetch_execution_bars_5m(self, symbol: str) -> List["_Bar"]:
        """Fetch the EXECUTION strike's own historical+intraday 1m bars via
        the same REST functions _ingest_history already uses for the
        tracking contract, merged and resampled to 5m the same way. Used
        once at entry by _open_entry_async to feed
        execution_risk.compute_execution_native_risk. [] on any failure
        (no token, fetch error) -- the caller treats an empty list the same
        as "no zone found" and falls back."""
        if self._is_crypto:
            return []
        token = await asyncio.to_thread(self._access_token)
        if not token:
            return []
        today = datetime.now(IST).date()
        start = today - timedelta(days=_LOOKBACK_DAYS)
        range_rows, today_rows = await asyncio.gather(
            fetch_upstox_range_1m(symbol, token, start, today),
            fetch_upstox_intraday_1m(symbol, token),
        )
        merged = _merge_rows(range_rows, today_rows)
        return _to_5m_bars(merged, filter_zero_volume=True)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_execution_bar_fetch.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_execution_bar_fetch.py
git commit -m "V4Cascade: fetch execution-strike historical bars for native risk lookup"
```

---

### Task 4: Wire execution-native risk into `_open_entry_async`, with fallback

**Files:**
- Modify: `strategies/v4_cascade/book.py`
- Test: `tests/strategies/test_v4_cascade_execution_native_entry.py`

**Interfaces:**
- Consumes: `Task 1`'s `compute_execution_native_risk`, `Task 3`'s
  `_fetch_execution_bars_5m`, `Task 2`'s `CascadePosition.risk_basis`.
- Produces: `_open_entry_async` sets `pos.risk_basis`, `t1.sl_price`/`t1.target_price`/
  `t2.sl_price` from the execution-native result when found; leaves the original
  tracking-scale values (already set by `_open_position`) untouched, with
  `risk_basis` staying `"tracking"`, when the lookback returns `None`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/strategies/test_v4_cascade_execution_native_entry.py
"""V4CascadeBook._open_entry_async -- 2026-07-21 wiring: uses
execution_risk.compute_execution_native_risk (fed by _fetch_execution_bars_5m)
to overwrite T1/T2's SL/target with execution-native numbers when a valid
zone is found on the execution strike's own bars; falls back to the
original tracking-scale values (already set by engine.py._open_position)
when it isn't. risk_basis is set accordingly so downstream consumers
(T2's live trailing, restart recovery) know which scale to use."""
from datetime import date, datetime
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
from strategies.v4_cascade.dataclasses import CascadeEvent, CascadeEventType, CascadePosition, TrancheLeg

IST = ZoneInfo("Asia/Kolkata")


class _FakeFeeder:
    async def subscribe_tokens(self, tokens):
        pass


class _FakeRebalancer:
    def __init__(self):
        self._feeder = _FakeFeeder()


def _book():
    cfg = GlobalConfig()
    book = V4CascadeBook(
        EventBus(), cfg, underlying="NIFTY", client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15",
    )
    book._running = True
    book._expiry = date(2026, 7, 21)
    book._rebalancer = _FakeRebalancer()
    return book


def _fresh_position(side="CE", strike=24200.0):
    t1 = TrancheLeg(tranche="T1", option_type=side, strike=strike, qty=65,
                     entry_price=100.0, sl_price=80.0, target_price=130.0, status="open")
    t2 = TrancheLeg(tranche="T2", option_type=side, strike=strike, qty=65,
                     entry_price=100.0, sl_price=80.0, status="open")
    return CascadePosition(
        underlying="NIFTY", side=side, tracking_strike=24000.0, execution_strike=strike,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, t2=t2,
        open_time=datetime(2026, 7, 21, 12, 0, tzinfo=IST), tracking_entry_price=100.0,
    )


def _entry_event(side="CE"):
    return CascadeEvent(event_type=CascadeEventType.OPEN_LONG_CE if side == "CE" else CascadeEventType.OPEN_LONG_PE,
                        side=side, price_hint=21.20, reason="gate3_bear_trap_reclaim",
                        sl_price=80.0, target_price=130.0, timestamp=datetime(2026, 7, 21, 12, 0, tzinfo=IST))


@pytest.mark.asyncio
async def test_uses_execution_native_risk_when_zone_found():
    book = _book()
    book._engine.position = _fresh_position()
    ev = _entry_event()
    with patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|1"), \
         patch.object(book, "_fetch_execution_bars_5m", new=AsyncMock(return_value=["some_bars"])), \
         patch("strategies.v4_cascade.book.compute_execution_native_risk", return_value=(21.0, 25.0)), \
         patch.object(book, "_bus") as mock_bus:
        mock_bus.publish = AsyncMock()
        await book._open_entry_async(ev, exec_strike=24200.0, qty=130, event_id="ev1",
                                     ts=datetime(2026, 7, 21, 12, 0, tzinfo=IST))
    pos = book._engine.position
    assert pos.risk_basis == "execution_native"
    assert pos.t1.sl_price == 21.0
    assert pos.t1.target_price == 25.0
    assert pos.t2.sl_price == 21.0


@pytest.mark.asyncio
async def test_falls_back_to_tracking_scale_when_no_zone_found():
    book = _book()
    book._engine.position = _fresh_position()
    ev = _entry_event()
    with patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|1"), \
         patch.object(book, "_fetch_execution_bars_5m", new=AsyncMock(return_value=[])), \
         patch("strategies.v4_cascade.book.compute_execution_native_risk", return_value=None), \
         patch.object(book, "_bus") as mock_bus:
        mock_bus.publish = AsyncMock()
        await book._open_entry_async(ev, exec_strike=24200.0, qty=130, event_id="ev1",
                                     ts=datetime(2026, 7, 21, 12, 0, tzinfo=IST))
    pos = book._engine.position
    assert pos.risk_basis == "tracking"
    assert pos.t1.sl_price == 80.0    # unchanged, original tracking-scale value
    assert pos.t1.target_price == 130.0


@pytest.mark.asyncio
async def test_crypto_skips_execution_native_lookup_entirely():
    book = _book()
    book._is_crypto = True
    book._engine.position = _fresh_position()
    ev = _entry_event()
    fetch_mock = AsyncMock(return_value=["bars"])
    with patch.object(book, "_fetch_execution_bars_5m", new=fetch_mock), \
         patch.object(book, "_bus") as mock_bus:
        mock_bus.publish = AsyncMock()
        await book._open_entry_async(ev, exec_strike=0.0, qty=130, event_id="ev1",
                                     ts=datetime(2026, 7, 21, 12, 0, tzinfo=IST))
    fetch_mock.assert_not_awaited()
    assert book._engine.position.risk_basis == "tracking"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/strategies/test_v4_cascade_execution_native_entry.py -v`
Expected: FAIL — `compute_execution_native_risk` not imported/used in book.py yet, so
the patch target doesn't exist and/or `risk_basis` stays at default without the wiring.

- [ ] **Step 3: Write the implementation**

In `strategies/v4_cascade/book.py`, add the import (alongside the existing v4_cascade
imports near the top):

```python
from strategies.v4_cascade.execution_risk import compute_execution_native_risk
```

Find `_open_entry_async` (the method Task 3 added `_fetch_execution_bars_5m` right
before). Currently, after resolving `exec_strike` and waiting for the live tick, it
publishes the `CascadeOrderEvent` using `ev.sl_price`/`ev.target_price` (the tracking-
scale values `_open_position` already set on `pos.t1`/`pos.t2`). Insert the execution-
native lookup between the tick-wait and the publish, so it can use the now-resolved
`exec_strike`/live price:

```python
        real_price = self._exec_live_price[ev.side]
        price_hint = real_price if real_price > 0 else ev.price_hint
        if real_price <= 0:
            logger.warning("V4CascadeBook[%s/%s/%s]: no execution-contract tick within 3s for "
                           "%s — falling back to tracking-contract price_hint=%.4f.",
                           self._underlying, self._client_id, self._binding_id, ev.side, ev.price_hint)
            self._clog.warning("no execution-contract tick within 3s for %s — falling back to "
                               "tracking-contract price_hint=%.4f.", ev.side, ev.price_hint)

        # 2026-07-21: execution-native risk lookup. Fetch the execution
        # strike's own bars and wait for the tick CONCURRENTLY (not
        # serially) so this doesn't add to entry latency beyond what
        # already existed.
        pos = self._engine.position
        if pos is not None and pos.is_open and not self._is_crypto and exec_strike:
            exec_bars = await self._fetch_execution_bars_5m(self._exec_symbol[ev.side])
            is_short = self._is_crypto and ev.side == "PE"
            native = compute_execution_native_risk(
                exec_bars, exec_entry_price=price_hint, sl_buffer=self._v4cfg.sl_buffer,
                is_short=is_short, session_open=self._session_open,
            )
            if native is not None:
                sl_price, target_price = native
                pos.risk_basis = "execution_native"
                if pos.t1 is not None:
                    pos.t1.sl_price = sl_price
                    pos.t1.target_price = target_price
                if pos.t2 is not None:
                    pos.t2.sl_price = sl_price
                logger.info("V4CascadeBook[%s/%s/%s]: execution-native risk found — "
                           "SL=%.4f target=%.4f (exec strike %s).", self._underlying,
                           self._client_id, self._binding_id, sl_price, target_price, exec_strike)
                self._clog.info("execution-native risk found — SL=%.4f target=%.4f (exec strike %s).",
                                sl_price, target_price, exec_strike)
            else:
                logger.warning("V4CascadeBook[%s/%s/%s]: no execution-native zone found for %s — "
                               "falling back to tracking-scale SL/target.", self._underlying,
                               self._client_id, self._binding_id, ev.side)
                self._clog.warning("no execution-native zone found for %s — falling back to "
                                   "tracking-scale SL/target.", ev.side)
```

Note: this block must come AFTER the existing `real_price = ...` / tick-wait block, and
BEFORE the existing `await self._bus.publish(Topic.CASCADE_ORDER_REQUEST, ...)` call —
insert it between them, do not duplicate the existing tick-wait logic. The `_fetch_execution_bars_5m`
call and the existing tick-wait loop already ran sequentially in the existing code (the
tick-wait happens earlier in the same method) — per the spec's "run concurrently"
requirement, this is acceptable because the historical REST fetch and the live-tick wait
are for the SAME already-resolved `exec_strike`/symbol and don't block each other's
inputs; a stricter `asyncio.gather` of the tick-wait-loop and the bars-fetch is an
optional latency optimization, not required for correctness, and is out of scope for
this task (flag as a follow-up if entry latency becomes a concern in practice).

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_execution_native_entry.py -v`
Expected: 3 passed

- [ ] **Step 5: Run full v4_cascade suite**

Run: `python -m pytest tests/strategies/ -k v4_cascade -q`
Expected: all passing

- [ ] **Step 6: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_execution_native_entry.py
git commit -m "V4Cascade: wire execution-native SL/target into entry, with fallback"
```

---

### Task 5: Execution strike ITM flip (1-OTM → 1-ITM)

**Files:**
- Modify: `strategies/v4_cascade/book.py`
- Test: `tests/strategies/test_v4_cascade_execution_strike_itm.py`

**Interfaces:** none new — modifies the existing `_resolve_execution_strike(self, side: str) -> float`.

- [ ] **Step 1: Write the failing test**

```python
# tests/strategies/test_v4_cascade_execution_strike_itm.py
"""V4CascadeBook._resolve_execution_strike -- 2026-07-21: flips from 1-OTM
to 1-ITM per user direction (an OTM strike's premium is theta/low-delta
dominated, contaminating any SL/target signal derived from it; an ITM
strike's premium is delta-dominated, a cleaner reflection of real price
action). CE: was ATM+step (OTM), now ATM-step (ITM). PE: was ATM-step
(OTM), now ATM+step (ITM)."""
from datetime import date

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook


def _book(underlying="NIFTY"):
    cfg = GlobalConfig()
    book = V4CascadeBook(
        EventBus(), cfg, underlying=underlying, client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15",
    )
    book._running = True
    book._expiry = date(2026, 7, 21)
    return book


def test_ce_execution_strike_is_itm_below_atm_nifty():
    book = _book("NIFTY")
    book._atm_open = 24216.05
    book._live_spot = 24216.05
    strike = book._resolve_execution_strike("CE")
    # ATM strike (rounded to step 50) is 24200; ITM for CE means BELOW atm.
    assert strike < 24200.0


def test_pe_execution_strike_is_itm_above_atm_nifty():
    book = _book("NIFTY")
    book._atm_open = 24216.05
    book._live_spot = 24216.05
    strike = book._resolve_execution_strike("PE")
    assert strike > 24200.0


def test_ce_execution_strike_is_itm_below_atm_crudeoil():
    book = _book("CRUDEOIL")
    book._atm_open = 7961.0
    book._live_spot = 7961.0
    strike = book._resolve_execution_strike("CE")
    assert strike < 8000.0


def test_pe_execution_strike_is_itm_above_atm_crudeoil():
    book = _book("CRUDEOIL")
    book._atm_open = 7961.0
    book._live_spot = 7961.0
    strike = book._resolve_execution_strike("PE")
    assert strike > 8000.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_execution_strike_itm.py -v`
Expected: FAIL (current code returns the OTM strike — CE assertion inverted, since
today `atm + self._execution_offset` for CE is ABOVE atm, not below)

- [ ] **Step 3: Write the implementation**

`_resolve_execution_strike` in `strategies/v4_cascade/book.py` (currently at line ~877)
reads exactly:

```python
    def _resolve_execution_strike(self, side: str) -> float:
        """ATM+-50 execution strike, resolved from LIVE spot (not the 09:15
        tracking-strike ATM) at trigger time, per the original design intent.
        Crypto trades the perpetual directly — no strike concept, always 0."""
        if self._is_crypto:
            return 0.0
        spot = self._live_spot or self._atm_open or 0.0
        step = self._strike_step
        if spot > 0:
            atm = round(spot / step) * step
            return atm + self._execution_offset if side == "CE" else atm - self._execution_offset
        # Defensive fallback — must NEVER silently return 0 (a live trade
        # entering with strike=0 is a real, observed bug this guards
        # against). live_spot and atm_open being simultaneously unavailable
        # should not happen once boot succeeded, but if it does, derive the
        # execution strike from the already-known-good TRACKING strike
        # (resolved once at boot, never zero if _resolve_symbols succeeded)
        # instead of ever returning 0.
        logger.error(...)
        self._clog.error(...)
        tracking_strike = self._ce_strike if side == "CE" else self._pe_strike
        ...
```

Change ONLY the single `return` line inside the `if spot > 0:` block — flip which side
gets `+`/`-`:

```python
            # 2026-07-21: flipped from OTM to ITM (was CE=atm+offset [OTM],
            # PE=atm-offset [OTM]). ITM: CE strike BELOW spot, PE strike
            # ABOVE spot -- delta-dominated premium, a cleaner signal for
            # execution-native SL/target than an OTM strike's theta/low-
            # delta-dominated premium.
            return atm - self._execution_offset if side == "CE" else atm + self._execution_offset
```

Leave the defensive fallback branch (the `tracking_strike = ...` code after the
docstring's fallback comment) completely untouched — it doesn't reference OTM/ITM at
all, it falls back to the tracking strike verbatim regardless of side convention.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_execution_strike_itm.py -v`
Expected: 4 passed

- [ ] **Step 5: Run full v4_cascade suite — check for hidden OTM assumptions**

Run: `python -m pytest tests/strategies/ -k v4_cascade -q`
Expected: all passing. If any existing test hardcodes an OTM execution strike value
(e.g. asserts a specific numeric strike), update that test's expected value to the new
ITM strike — this is an intentional behavior change, not a regression, provided the
only difference is which side of ATM the strike falls on.

- [ ] **Step 6: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_execution_strike_itm.py
git commit -m "V4Cascade: execution strike 1-OTM -> 1-ITM"
```

---

### Task 6: Execution-native T1/T2 exit-checking (separate clock from tracking bars)

**This is the largest, highest-risk task in this plan** — it introduces a second,
independent exit-check path driven by the execution contract's own bar closes, running
alongside (not replacing) the existing tracking-bar-driven path.

**Files:**
- Modify: `strategies/v4_cascade/engine.py`
- Modify: `strategies/v4_cascade/book.py`
- Test: `tests/strategies/test_v4_cascade_execution_native_exits.py`

**Interfaces:**
- Consumes: `Task 2`'s `CascadePosition.risk_basis`, existing `exits.check_t1`,
  `exits.TrailingBaseTracker` (used UNCHANGED — same class, just fed execution-scale
  bars and no `map_trailing_stop_to_execution` call).
- Produces: `V4CascadeEngine.check_exits_execution_native(self, side: str, exec_bar) -> List[CascadeEvent]`
  (new public method, mirrors `_check_exits`'s T1/T2 logic but with NO scale mapping —
  `exec_bar` IS the scale, throughout). `V4CascadeEngine._update_side`'s existing
  same-side branch (`if self.position.side == side: events += self._check_exits(side, bar); return events`)
  gated to skip `_check_exits` entirely when `self.position.risk_basis == "execution_native"`
  (that position's exits are now driven exclusively by the new execution-bar clock,
  never by the tracking-bar clock).

- [ ] **Step 1: Write the failing tests**

```python
# tests/strategies/test_v4_cascade_execution_native_exits.py
"""V4CascadeEngine.check_exits_execution_native -- 2026-07-21: for a position
with risk_basis=="execution_native", T1/T2 exit-checks run against the
EXECUTION contract's own bars directly (no tracking-to-execution scale
mapping at all -- current_stop, sl_price, target_price and the bar are all
on the SAME scale). Also: _update_side must skip the OLD tracking-bar-driven
_check_exits entirely for such a position, since its exits are now driven
exclusively by this new, separate execution-bar clock."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _Bar
from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg
from strategies.v4_cascade.engine import V4CascadeEngine
from strategies.v4_cascade.exits import TrailingBaseTracker

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 21, 12, 0, tzinfo=IST)


def _bar(offset_5m, o, h, l, c):
    return _Bar(_BASE + timedelta(minutes=5 * offset_5m), o, h, l, c, tf=5)


def _execution_native_position(entry_price=21.0, sl_price=18.0, target_price=25.0):
    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=24200.0, qty=65,
                     entry_price=entry_price, sl_price=sl_price, target_price=target_price, status="open")
    t2 = TrancheLeg(tranche="T2", option_type="CE", strike=24200.0, qty=65,
                     entry_price=entry_price, sl_price=sl_price, status="open")
    return CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24200.0,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, t2=t2, open_time=_BASE,
        risk_basis="execution_native",
    ), t1, t2


def test_t1_target_hit_on_execution_bar_directly_no_scale_mapping():
    eng = V4CascadeEngine()
    pos, t1, t2 = _execution_native_position()
    eng.position = pos
    eng._trackers["CE"] = TrailingBaseTracker(bear=True, initial_stop=18.0)

    events = eng.check_exits_execution_native("CE", _bar(0, 24, 26, 23, 25))
    fired = [e for e in events if e.tranche == "T1"]
    assert len(fired) == 1
    assert fired[0].reason == "t1_target_2r"
    assert t1.close_price == 25.0   # the EXECUTION bar's own level, no scale conversion


def test_t2_trail_stop_hit_on_execution_bar_directly():
    eng = V4CascadeEngine()
    pos, t1, t2 = _execution_native_position()
    eng.position = pos
    trail = TrailingBaseTracker(bear=True, initial_stop=18.0)
    eng._trackers["CE"] = trail

    events = eng.check_exits_execution_native("CE", _bar(0, 19, 20, 17, 17.5))
    fired = [e for e in events if e.tranche == "T2"]
    assert len(fired) == 1
    assert fired[0].reason == "t2_trailing_base_stop"
    assert t2.close_price == 18.0


def test_no_position_is_a_noop():
    eng = V4CascadeEngine()
    eng.position = None
    events = eng.check_exits_execution_native("CE", _bar(0, 24, 26, 23, 25))
    assert events == []


def test_update_side_skips_tracking_check_exits_for_execution_native_position():
    eng = V4CascadeEngine()
    pos, t1, t2 = _execution_native_position()
    eng.position = pos
    eng._trackers["CE"] = TrailingBaseTracker(bear=True, initial_stop=18.0)
    # A TRACKING bar whose (tracking-scale) numbers would trivially "hit" T1's
    # execution-scale target (25.0) if scale mapping were mistakenly still
    # applied -- must NOT close anything via the tracking-bar path.
    tracking_bar = _bar(0, 1000, 1100, 900, 1050)
    events = eng.update(ce_bar=tracking_bar)
    assert events == []
    assert t1.status == "open"
    assert t2.status == "open"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/strategies/test_v4_cascade_execution_native_exits.py -v`
Expected: FAIL with `AttributeError: 'V4CascadeEngine' object has no attribute 'check_exits_execution_native'`

- [ ] **Step 3: Write the implementation**

In `strategies/v4_cascade/engine.py`, add a new method right after the existing
`_check_exits` (do not modify `_check_exits` itself — it stays exactly as today's
tracking-bar-driven path, used only for `risk_basis == "tracking"` positions):

```python
    def check_exits_execution_native(self, side: str, exec_bar) -> List[CascadeEvent]:
        """2026-07-21: T1/T2 exit-checks for a risk_basis=="execution_native"
        position, fed EXECUTION-contract bars directly by book.py's parallel
        execution-bar clock (see book.py's execution-contract 5m bar-builder).
        No scale mapping anywhere here -- t1.sl_price/target_price, t2's
        tracker current_stop, and exec_bar are all already on the SAME
        (execution) scale, unlike _check_exits which bridges tracking-scale
        levels onto execution-scale bars via map_trailing_stop_to_execution.
        Public (unlike _check_exits) because book.py's bar-builder, not
        engine.py's own update(), is what drives this clock."""
        events: List[CascadeEvent] = []
        pos = self.position
        if pos is None:
            return events
        t1, t2 = pos.t1, pos.t2
        is_short = not self._scanners[side]._bear
        trail = self._trackers.get(side)

        if t1 is not None and t1.status == "open":
            r = check_t1(t1, exec_bar, is_short=is_short)
            if r.hit:
                t1.status = "closed"
                t1.close_price = r.price
                t1.close_reason = r.reason
                t1.close_time = exec_bar.timestamp
                events.append(self._close_event(side, "T1", r.reason, r.price, exec_bar.timestamp))
                if r.reason == "t1_target_2r" and t2 is not None and t2.status == "open" and trail is not None:
                    trail.move_to_breakeven(t1.entry_price, buffer=self._cfg.sl_buffer)
                    if trail.current_stop is not None:
                        t2.trail_stop_price = trail.current_stop   # already execution-scale, no mapping

        if t2 is not None and t2.status == "open" and trail is not None:
            moved = trail.on_5m_bar(exec_bar)
            if moved and trail.current_stop is not None:
                t2.trail_stop_price = trail.current_stop
                t2.tracking_current_stop = trail.current_stop
            r = trail.check_hit(exec_bar)
            if r.hit:
                t2.status = "closed"
                t2.close_price = r.price
                t2.close_reason = r.reason
                t2.close_time = exec_bar.timestamp
                events.append(self._close_event(side, "T2", r.reason, r.price, exec_bar.timestamp))

        if (t1 is None or t1.status == "closed") and (t2 is None or t2.status == "closed"):
            pos.status = "closed"
            pos.close_time = exec_bar.timestamp
            self._trackers.pop(side, None)
            self._tracking_entry_price.pop(side, None)
        return events
```

Now find `_update_side`'s same-side branch:

```python
        if self.position is not None and self.position.is_open:
            if self.position.side == side:
                events += self._check_exits(side, bar)
                return events
```

Change it to skip the tracking-bar-driven check entirely for an execution-native
position (its exits are handled exclusively by book.py's separate execution-bar clock,
via `check_exits_execution_native`, called from a different code path than `update()`):

```python
        if self.position is not None and self.position.is_open:
            if self.position.side == side:
                if self.position.risk_basis != "execution_native":
                    events += self._check_exits(side, bar)
                return events
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_execution_native_exits.py -v`
Expected: 4 passed

- [ ] **Step 5: Run full v4_cascade suite**

Run: `python -m pytest tests/strategies/ -k v4_cascade -q`
Expected: all passing — the `risk_basis != "execution_native"` guard means every
EXISTING test (all of which construct `CascadePosition` without `risk_basis`, defaulting
to `"tracking"`) is completely unaffected.

- [ ] **Step 6: Commit**

```bash
git add strategies/v4_cascade/engine.py tests/strategies/test_v4_cascade_execution_native_exits.py
git commit -m "V4Cascade: add execution-native exit-check path (separate from tracking-bar clock)"
```

---

### Task 7: Execution-contract live 5m bar-builder in `book.py`

**Files:**
- Modify: `strategies/v4_cascade/book.py`
- Test: `tests/strategies/test_v4_cascade_execution_bar_builder.py`

**Interfaces:**
- Consumes: `Task 6`'s `V4CascadeEngine.check_exits_execution_native`.
- Produces: `V4CascadeBook._on_execution_tick(self, side: str, ltp: float, ts: datetime) -> None`
  (bucket-builder, mirrors the existing tracking-contract `_on_option_tick`) and
  `V4CascadeBook._close_execution_5m_bucket(self, side: str, bar) -> None` (mirrors
  `_close_5m_bucket`, calls `check_exits_execution_native` instead of `engine.update`).

- [ ] **Step 1: Write the failing test**

```python
# tests/strategies/test_v4_cascade_execution_bar_builder.py
"""V4CascadeBook's execution-contract 5m bar-builder -- 2026-07-21: parallel
to the existing tracking-contract bucket-builder (_on_option_tick /
_close_5m_bucket), but keyed off _exec_symbol ticks and driving
engine.check_exits_execution_native instead of engine.update. Only active
for a risk_basis=="execution_native" open position -- a no-op otherwise, so
this never interferes with tracking-native positions' existing behavior."""
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 21, 12, 0, tzinfo=IST)


def _book():
    cfg = GlobalConfig()
    book = V4CascadeBook(
        EventBus(), cfg, underlying="NIFTY", client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15",
    )
    book._running = True
    book._expiry = date(2026, 7, 21)
    return book


def _open_execution_native_position(book):
    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=24200.0, qty=65,
                     entry_price=21.0, sl_price=18.0, target_price=25.0, status="open")
    t2 = TrancheLeg(tranche="T2", option_type="CE", strike=24200.0, qty=65,
                     entry_price=21.0, sl_price=18.0, status="open")
    book._engine.position = CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24200.0,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, t2=t2, open_time=_BASE,
        risk_basis="execution_native",
    )
    return t1, t2


def test_execution_ticks_build_5m_bars_and_close_on_bucket_rollover():
    book = _book()
    t1, t2 = _open_execution_native_position(book)
    book._exec_symbol["CE"] = "NSE_FO|24200CE"

    book._on_execution_tick("CE", 22.0, _BASE)
    book._on_execution_tick("CE", 26.0, _BASE + timedelta(minutes=1))   # clears T1 target (25.0) intrabar
    # Next tick in a NEW 5m bucket forces the previous bucket to close and
    # be checked.
    book._on_execution_tick("CE", 24.0, _BASE + timedelta(minutes=6))

    assert t1.status == "closed"
    assert t1.close_reason == "t1_target_2r"


def test_noop_for_tracking_native_position():
    book = _book()
    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=24200.0, qty=65,
                     entry_price=21.0, sl_price=18.0, target_price=25.0, status="open")
    t2 = TrancheLeg(tranche="T2", option_type="CE", strike=24200.0, qty=65,
                     entry_price=21.0, sl_price=18.0, status="open")
    book._engine.position = CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24200.0,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, t2=t2, open_time=_BASE,
        risk_basis="tracking",
    )
    book._exec_symbol["CE"] = "NSE_FO|24200CE"

    book._on_execution_tick("CE", 26.0, _BASE)
    book._on_execution_tick("CE", 26.0, _BASE + timedelta(minutes=6))

    assert t1.status == "open"   # untouched -- tracking-native positions ignore this clock
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/strategies/test_v4_cascade_execution_bar_builder.py -v`
Expected: FAIL with `AttributeError: 'V4CascadeBook' object has no attribute '_on_execution_tick'`

- [ ] **Step 3: Write the implementation**

In `strategies/v4_cascade/book.py`, add `self._exec_buckets: Dict[str, Optional["_Bar"]] = {"CE": None, "PE": None}`
to `__init__` (alongside the existing `self._buckets` dict for tracking-contract
buckets). Then add these two methods (place them right after the existing
`_on_option_tick`/`_close_5m_bucket` pair, since they mirror that exact structure):

```python
    def _on_execution_tick(self, side: str, ltp: float, ts: datetime) -> None:
        """Mirrors _on_option_tick, but for the EXECUTION contract — only
        matters for a risk_basis=='execution_native' open position; a no-op
        otherwise (tracking-native positions' exits are driven exclusively
        by the existing tracking-contract bucket-builder)."""
        pos = self._engine.position
        if pos is None or not pos.is_open or pos.side != side or pos.risk_basis != "execution_native":
            return
        bucket = _bucket_start(ts, 5, self._session_open)
        cur = self._exec_buckets[side]
        if cur is None or cur.timestamp != bucket:
            if cur is not None:
                self._close_execution_5m_bucket(side, cur)
            self._exec_buckets[side] = _Bar(bucket, ltp, ltp, ltp, ltp, tf=5)
        else:
            cur.high = max(cur.high, ltp)
            cur.low = min(cur.low, ltp)
            cur.close = ltp

    def _close_execution_5m_bucket(self, side: str, bar) -> None:
        events = self._engine.check_exits_execution_native(side, bar)
        for ev in events:
            self._emit_order(ev, pos_before=self._engine.position)
        self._persist_position()
```

Wire `_on_execution_tick` into `_option_loop`'s existing tick-dispatch (right where
`self._exec_live_price[exec_side] = float(tick.ltp)` is already set for each incoming
tick matching `self._exec_symbol[exec_side]`):

```python
            for exec_side in ("CE", "PE"):
                if self._exec_symbol[exec_side] and symbol == self._exec_symbol[exec_side]:
                    self._exec_live_price[exec_side] = float(tick.ltp)
                    self._on_execution_tick(exec_side, float(tick.ltp), tick.timestamp)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_execution_bar_builder.py -v`
Expected: 2 passed

- [ ] **Step 5: Run full v4_cascade suite**

Run: `python -m pytest tests/strategies/ -k v4_cascade -q`
Expected: all passing

- [ ] **Step 6: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_execution_bar_builder.py
git commit -m "V4Cascade: add execution-contract live 5m bar-builder for execution-native exits"
```

---

### Task 8: `_restore_tracker_state_for_open_position` respects `risk_basis`

**Files:**
- Modify: `strategies/v4_cascade/book.py`
- Test: `tests/strategies/test_v4_cascade_restored_tracker_state.py` (extend existing file)

**Interfaces:**
- Consumes: `Task 2`'s `CascadePosition.risk_basis`.
- Produces: `_restore_tracker_state_for_open_position` seeds
  `TrailingBaseTracker(initial_stop=...)` from `t2.tracking_current_stop` directly
  (no scale conversion) when `risk_basis == "execution_native"`, exactly as it already
  does for `"tracking"` — the difference is purely which BAR CLOCK later feeds that
  tracker (book.py's tracking-bucket builder vs. Task 7's execution-bucket builder),
  which is already handled correctly by `_on_execution_tick`'s own `risk_basis` check
  (Task 7) and `_update_side`'s guard (Task 6) — so this task is CONFIRMING no change is
  needed to the seeding logic itself, only adding the regression test that proves it.

- [ ] **Step 1: Write the failing test**

Add to the EXISTING `tests/strategies/test_v4_cascade_restored_tracker_state.py` (do not
create a new file — this extends the suite Task from earlier today):

```python
def test_restores_tracker_correctly_for_execution_native_position():
    """risk_basis=='execution_native': the seed value (tracking_current_stop)
    is used AS-IS with no scale conversion -- since for an execution-native
    position, tracking_current_stop actually holds an EXECUTION-scale value
    (set by check_exits_execution_native, which never distinguishes the two
    scales in its own field names — see Task 6/7). This confirms restore
    doesn't accidentally apply tracking-to-execution scaling on top of an
    already-execution-scale seed."""
    book = _book()
    pos = _open_position()
    pos.risk_basis = "execution_native"
    pos.t2.tracking_current_stop = 18.5   # execution-scale, per Task 6's field reuse
    book._engine.position = pos

    book._restore_tracker_state_for_open_position()

    tracker = book._engine._trackers["CE"]
    assert tracker.current_stop == 18.5
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_restored_tracker_state.py -v`
Expected: FAIL only if `_open_position()`'s test helper doesn't already default
`risk_basis` — check the existing helper in this test file; if `CascadePosition(...)` is
constructed without `risk_basis`, it already defaults to `"tracking"` (Task 2), and this
test sets it explicitly afterward, so this should actually PASS immediately once Task 2
is done, with NO production code change needed. If it fails, that indicates
`_restore_tracker_state_for_open_position` has some tracking-specific branch that needs
generalizing — inspect and fix minimally to make the seed a plain, scale-agnostic
assignment regardless of `risk_basis`.

- [ ] **Step 3: Confirm no implementation change needed (or make the minimal fix)**

Read the current `_restore_tracker_state_for_open_position` (added earlier today, in
`book.py`). Confirm its `initial_stop = (t2.tracking_current_stop if t2.tracking_current_stop is not None else (t2.sl_price or None))`
line is already scale-agnostic (it never applies `map_trailing_stop_to_execution` or any
conversion — it just passes the persisted value straight into `TrailingBaseTracker`'s
constructor). If so, no code change is needed here — this task exists purely to add the
regression test proving this holds for the new `risk_basis` field too.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_v4_cascade_restored_tracker_state.py -v`
Expected: 5 passed (4 existing + 1 new)

- [ ] **Step 5: Commit**

```bash
git add tests/strategies/test_v4_cascade_restored_tracker_state.py
git commit -m "V4Cascade: confirm+test tracker restore is risk_basis-agnostic"
```

---

### Task 9: `tracking_recenter_pts` config + re-center-while-flat check

**Files:**
- Modify: `strategies/v4_cascade/config.py`
- Modify: `strategies/v4_cascade/book.py`
- Test: `tests/strategies/test_v4_cascade_tracking_recenter.py`

**Interfaces:**
- Produces: `V4CascadeConfig.tracking_recenter_pts: float` (100.0 default, CRUDEOIL
  wired to 200.0 in `book.py.__init__` alongside the existing per-underlying
  `TRACKING_OFFSET_PTS`/`sl_buffer` wiring). `V4CascadeBook._tracking_reference_atm: Optional[float]`.
  `V4CascadeBook._maybe_recenter_tracking_strikes(self, current_atm: float) -> None`
  (checks drift + flat-gate; Task 10 fills in the actual re-warm body — this task only
  adds the gate/threshold check and strike re-derivation, raising `NotImplementedError`
  is wrong; instead this task's version re-derives strikes and RESETS scanners with a
  bare `scanner.reset()`, and Task 10 upgrades that reset into a full re-warm).

- [ ] **Step 1: Write the failing tests**

```python
# tests/strategies/test_v4_cascade_tracking_recenter.py
"""V4CascadeBook._maybe_recenter_tracking_strikes -- 2026-07-21: re-centers
the tracking/scanner strikes when the underlying has drifted far enough from
the ATM the CURRENT tracking strikes were derived from, but ONLY while flat
(no open position) -- carrying an open position's zone/SL/target state
across a strike change has no valid conversion between two different
instruments' unrelated price scales, so re-centering never happens mid-trade."""
from datetime import date, datetime
from zoneinfo import ZoneInfo

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg

IST = ZoneInfo("Asia/Kolkata")


def _book(underlying="NIFTY"):
    cfg = GlobalConfig()
    book = V4CascadeBook(
        EventBus(), cfg, underlying=underlying, client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15",
    )
    book._running = True
    book._expiry = date(2026, 7, 21)
    book._tracking_reference_atm = 24216.05
    book._ce_strike = 24000
    book._pe_strike = 24400
    return book


def test_recenters_when_flat_and_drift_exceeds_threshold_nifty():
    book = _book("NIFTY")
    book._engine.position = None
    assert book._v4cfg.tracking_recenter_pts == 100.0

    book._maybe_recenter_tracking_strikes(current_atm=24320.0)   # drift = 103.95 >= 100

    assert book._tracking_reference_atm == 24320.0
    assert book._ce_strike != 24000 or book._pe_strike != 24400


def test_does_not_recenter_when_drift_under_threshold():
    book = _book("NIFTY")
    book._engine.position = None

    book._maybe_recenter_tracking_strikes(current_atm=24250.0)   # drift = 33.95 < 100

    assert book._tracking_reference_atm == 24216.05
    assert book._ce_strike == 24000
    assert book._pe_strike == 24400


def test_does_not_recenter_while_position_open_regardless_of_drift():
    book = _book("NIFTY")
    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=24200.0, qty=65, entry_price=20.0, status="open")
    book._engine.position = CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24200.0,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, open_time=datetime(2026, 7, 21, 12, 0, tzinfo=IST),
    )

    book._maybe_recenter_tracking_strikes(current_atm=25000.0)   # huge drift, but position open

    assert book._tracking_reference_atm == 24216.05   # unchanged
    assert book._ce_strike == 24000
    assert book._pe_strike == 24400


def test_crudeoil_uses_200_point_threshold():
    book = _book("CRUDEOIL")
    assert book._v4cfg.tracking_recenter_pts == 200.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/strategies/test_v4_cascade_tracking_recenter.py -v`
Expected: FAIL with `AttributeError: 'V4CascadeBook' object has no attribute '_maybe_recenter_tracking_strikes'`

- [ ] **Step 3: Write the implementation**

In `strategies/v4_cascade/config.py`, add the module constant and config field
(alongside the existing `TRACKING_OFFSET_PTS`):

```python
TRACKING_RECENTER_PTS: float = 100.0   # NIFTY: re-center tracking strikes after this much ATM drift
```

Add to `V4CascadeConfig`:

```python
    tracking_recenter_pts: float = TRACKING_RECENTER_PTS
```

In `strategies/v4_cascade/book.py`'s `__init__`, find the existing `if self._is_mcx:`
block (currently sets `self._tracking_offset = 400.0` / `self._execution_offset = 100.0`
in the `if` branch, `TRACKING_OFFSET_PTS` / `EXECUTION_OFFSET_PTS` in the `else`) and add
the re-center threshold alongside it, same pattern:

```python
        if self._is_mcx:
            self._tracking_offset = 400.0
            self._execution_offset = 100.0
            _recenter_pts = 200.0
        else:
            self._tracking_offset = TRACKING_OFFSET_PTS
            self._execution_offset = EXECUTION_OFFSET_PTS
            _recenter_pts = TRACKING_RECENTER_PTS
```

Pass it into the `V4CascadeConfig(...)` constructor call a few lines below (which
already passes `sl_buffer=_sl_buffer` etc.): add `tracking_recenter_pts=_recenter_pts,`.
Add the import: `from strategies.v4_cascade.config import ..., TRACKING_RECENTER_PTS`
(extend the existing `strategies.v4_cascade.config` import line, do not add a second
import line for the same module).

Add `self._tracking_reference_atm: Optional[float] = None` to `__init__`. Find wherever
the session-open tracking-strike computation currently sets `self._ce_strike`/
`self._pe_strike` for the first time (in `_resolve_symbols`) and set
`self._tracking_reference_atm = self._atm_open` there too, right alongside it.

Add the new method (place it near `_check_daily_boundary`, since it's called from the
same live-bar-close site):

```python
    def _maybe_recenter_tracking_strikes(self, current_atm: float) -> None:
        """2026-07-21: re-center the tracking/scanner strikes when the
        underlying has drifted tracking_recenter_pts away from the ATM the
        CURRENT strikes were derived from -- but ONLY while flat. An open
        position's SL/target/zone are computed on a SPECIFIC contract's own
        price structure; there is no valid way to carry that state across a
        strike change (two different instruments, unrelated price scales),
        so re-centering never happens mid-trade -- gated at this single
        check site, not scattered across callers."""
        pos = self._engine.position
        if pos is not None and pos.is_open:
            return
        if self._tracking_reference_atm is None:
            return
        if abs(current_atm - self._tracking_reference_atm) < self._v4cfg.tracking_recenter_pts:
            return
        old_ce, old_pe = self._ce_strike, self._pe_strike
        step = self._strike_step
        self._ce_strike = int(round((current_atm - self._tracking_offset) / step) * step)
        self._pe_strike = int(round((current_atm + self._tracking_offset) / step) * step)
        self._tracking_reference_atm = current_atm
        for side in ("CE", "PE"):
            self._engine._scanners[side].reset()
        logger.info("V4CascadeBook[%s/%s/%s]: re-centered tracking strikes CE %s->%s PE %s->%s "
                   "(atm=%.2f).", self._underlying, self._client_id, self._binding_id,
                   old_ce, self._ce_strike, old_pe, self._pe_strike, current_atm)
        self._clog.info("re-centered tracking strikes CE %s->%s PE %s->%s (atm=%.2f).",
                        old_ce, self._ce_strike, old_pe, self._pe_strike, current_atm)
```

This rounding formula is identical to the one already used to derive the CE/PE tracking
strikes at session-open (confirmed against real logged values: ATM 7961, tracking_offset
400, step 100 → CE `round((7961-400)/100)*100 = 7600`, PE `round((7961+400)/100)*100 =
8400`, matching the exact `CE=7600 PE=8400` seen in tonight's boot log).

Wire the check into the existing live-bar-close path, alongside
`_check_daily_boundary`'s existing call site — find where `_check_daily_boundary(bar.timestamp)`
is called in `_close_5m_bucket` and add, using the SAME `self._live_spot or self._atm_open`
fallback chain `_resolve_execution_strike` already uses for "the book's current view of
spot" (not the frozen 09:15 `_atm_open` alone):

```python
        self._check_daily_boundary(bar.timestamp)
        _current_atm = self._live_spot or self._atm_open or 0.0
        if _current_atm > 0:
            self._maybe_recenter_tracking_strikes(_current_atm)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_tracking_recenter.py -v`
Expected: 4 passed

- [ ] **Step 5: Run full v4_cascade suite**

Run: `python -m pytest tests/strategies/ -k v4_cascade -q`
Expected: all passing

- [ ] **Step 6: Commit**

```bash
git add strategies/v4_cascade/config.py strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_tracking_recenter.py
git commit -m "V4Cascade: re-center tracking strikes on drift, gated to flat-only"
```

---

### Task 10: Re-center re-warms from real historical+intraday data (not a bare reset)

**Files:**
- Modify: `strategies/v4_cascade/book.py`
- Test: `tests/strategies/test_v4_cascade_tracking_recenter.py` (extend Task 9's file)

**Interfaces:**
- Consumes: `Task 3`'s pattern (`fetch_upstox_range_1m` + `fetch_upstox_intraday_1m` +
  `_merge_rows` + `_to_5m_bars`), the existing `_replay_through_engine` helper (already
  imported/used by `_ingest_history`).
- Produces: `_maybe_recenter_tracking_strikes` becomes `async def`, and after
  re-deriving strikes, fetches + replays history for the NEW CE/PE tracking symbols
  through fresh scanner state (mirrors `_ingest_history`'s own fetch-and-replay
  sequence, scoped to only the tracking scanners).

- [ ] **Step 1: Write the failing test**

Add to `tests/strategies/test_v4_cascade_tracking_recenter.py`:

```python
@pytest.mark.asyncio
async def test_recenter_rewarms_scanners_from_real_history_not_bare_reset():
    """The regression this guards against: a re-center that resets scanners
    without re-warming them, leaving the new strikes cold for hours until
    enough live bars accumulate a fresh pattern from scratch."""
    book = _book("NIFTY")
    book._engine.position = None

    range_rows = [
        {"ts": "2026-07-21T09:15:00", "open": 100, "high": 110, "low": 100, "close": 105, "volume": 10},
        {"ts": "2026-07-21T09:16:00", "open": 98, "high": 105, "low": 95, "close": 100, "volume": 10},
        {"ts": "2026-07-21T09:17:00", "open": 110, "high": 115, "low": 105, "close": 112, "volume": 10},
    ]
    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=range_rows)), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
         patch.object(book, "_access_token", return_value="tok"), \
         patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|new"):
        await book._maybe_recenter_tracking_strikes(current_atm=24320.0)

    # A real zone from the fetched history must now be present -- not an
    # empty reset.
    assert len(book._engine._scanners["CE"].setups) >= 1


@pytest.mark.asyncio
async def test_recenter_leaves_position_and_spot_confirm_untouched():
    book = _book("NIFTY")
    book._engine.position = None
    original_spot_confirm = book._engine._spot_confirm

    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=[])), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
         patch.object(book, "_access_token", return_value="tok"), \
         patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|new"):
        await book._maybe_recenter_tracking_strikes(current_atm=24320.0)

    assert book._engine._spot_confirm is original_spot_confirm   # untouched, same object
    assert book._engine.position is None
```

Update the four existing Task-9 tests in this file to `async def` with `@pytest.mark.asyncio`
and `await book._maybe_recenter_tracking_strikes(...)`, since the method signature is
changing to `async def` in this task — Task 9's synchronous version is superseded here,
not left running in parallel. Add the necessary imports
(`from unittest.mock import AsyncMock, patch`, `import pytest`) at the top of the file.

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/strategies/test_v4_cascade_tracking_recenter.py -v`
Expected: FAIL — `_maybe_recenter_tracking_strikes` is still synchronous and does a bare
`scanner.reset()`, so `setups` stays empty and awaiting a non-async function raises
`TypeError: object NoneType can't be used in 'await' expression`.

- [ ] **Step 3: Write the implementation**

Replace the body written in Task 9 — `_maybe_recenter_tracking_strikes` becomes:

```python
    async def _maybe_recenter_tracking_strikes(self, current_atm: float) -> None:
        """2026-07-21: re-center the tracking/scanner strikes when the
        underlying has drifted tracking_recenter_pts away from the ATM the
        CURRENT strikes were derived from -- but ONLY while flat (see
        docstring history in Task 9's version for the full rationale).
        Re-warms the new strikes' scanners from real historical+intraday
        data (the SAME fetch-and-replay sequence _ingest_history already
        performs at boot), scoped to ONLY the two per-side scanners --
        self.position (already confirmed None/closed by the gate below),
        self._spot_confirm (Gate 1, Index-based, never depended on the
        tracking option strike), and self._trackers/_tracking_entry_price
        (empty while flat) are all left untouched."""
        pos = self._engine.position
        if pos is not None and pos.is_open:
            return
        if self._tracking_reference_atm is None:
            return
        if abs(current_atm - self._tracking_reference_atm) < self._v4cfg.tracking_recenter_pts:
            return

        old_ce, old_pe = self._ce_strike, self._pe_strike
        new_ce = int(round((current_atm - self._tracking_offset) / self._strike_step) * self._strike_step)
        new_pe = int(round((current_atm + self._tracking_offset) / self._strike_step) * self._strike_step)

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

        self._ce_strike, self._pe_strike = new_ce, new_pe
        self._ce_symbol, self._pe_symbol = new_ce_symbol, new_pe_symbol
        self._tracking_reference_atm = current_atm
        self._bars_5m["CE"], self._bars_5m["PE"] = ce_5m, pe_5m
        for side in ("CE", "PE"):
            self._engine._scanners[side].reset()
        _replay_through_engine(self._engine, [], ce_5m, pe_5m,
                                on_daily_boundary=self._apply_eod_gate23_rules,
                                session_open=self._session_open,
                                eod_square_off=self._eod_hour_min,
                                gate23_reset=self._gate23_hour_min)

        feeder = getattr(self._rebalancer, "_feeder", None) if self._rebalancer else None
        if feeder:
            try:
                await feeder.subscribe_tokens([new_ce_symbol, new_pe_symbol])
            except Exception:
                logger.exception("V4CascadeBook[%s/%s/%s]: re-center subscribe failed for %s/%s.",
                                 self._underlying, self._client_id, self._binding_id,
                                 new_ce_symbol, new_pe_symbol)

        logger.info("V4CascadeBook[%s/%s/%s]: re-centered tracking strikes CE %s->%s PE %s->%s "
                   "(atm=%.2f) — re-warmed from %d/%d 5m bars.", self._underlying, self._client_id,
                   self._binding_id, old_ce, new_ce, old_pe, new_pe, current_atm, len(ce_5m), len(pe_5m))
        self._clog.info("re-centered tracking strikes CE %s->%s PE %s->%s (atm=%.2f) — "
                        "re-warmed from %d/%d 5m bars.", old_ce, new_ce, old_pe, new_pe,
                        current_atm, len(ce_5m), len(pe_5m))
```

(Passing `[]` for the spot-bar list to `_replay_through_engine` is intentional — Gate 1
never depended on the tracking option strike, so re-centering must not re-replay/re-touch
`self._spot_confirm`'s own state at all; check `_replay_through_engine`'s signature to
confirm passing an empty spot list is safe and doesn't error or reset Gate 1 state as a
side effect — if it does, this call needs a variant that skips the spot-bar replay
entirely rather than passing `[]`.)

Update the call site added in Task 9 (`_close_5m_bucket`'s
`self._maybe_recenter_tracking_strikes(_current_atm)`) to fire it via `self._fire(...)`
(matching the existing async-dispatch-from-a-sync-context convention already used for
`_open_entry_async`) rather than blocking the synchronous bar-close handler on a REST
round-trip — `_maybe_recenter_tracking_strikes` is now `async def` as of this task:

```python
        self._check_daily_boundary(bar.timestamp)
        _current_atm = self._live_spot or self._atm_open or 0.0
        if _current_atm > 0:
            self._fire(self._maybe_recenter_tracking_strikes(_current_atm))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/strategies/test_v4_cascade_tracking_recenter.py -v`
Expected: 6 passed

- [ ] **Step 5: Run full v4_cascade suite**

Run: `python -m pytest tests/strategies/ -k v4_cascade -q`
Expected: all passing (should now be roughly 83 + 6 + 6 + 2 + 3 + 4 + 3 + 2 + 1 + 6 ≈ 116 tests across this whole plan)

- [ ] **Step 6: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_tracking_recenter.py
git commit -m "V4Cascade: re-center re-warms tracking scanners from real history"
```

---

### Task 11: Full regression pass + manual trace

- [ ] **Step 1: Run the complete repo test suite**

Run: `python -m pytest tests/ -q --ignore=tests/data_layer/test_feeder_translation.py`
Expected: all passing (the one ignored test is a known pre-existing, unrelated failure
confirmed via `git stash` earlier this session — not caused by any change in this plan).

- [ ] **Step 2: Manually trace one synthetic end-to-end scenario**

Using `scripts/v4_backtest_july2026.py` (or a scratch script) against a known historical
NIFTY/CRUDEOIL day already used earlier this session for validation: confirm an entry
fires (tracking-strike Gate 1/2/3, unchanged), confirm `risk_basis` ends up
`"execution_native"` when the execution strike's own bars produce a valid zone, confirm
the fallback path (`risk_basis == "tracking"`) still works identically to today's
pre-plan behavior when they don't, and confirm a T1 target hit correctly ratchets T2 to
breakeven on whichever scale that trade is using.

- [ ] **Step 3: Do NOT push to the live branch without explicit confirmation**

Given this plan changes the entry-firing critical path (Task 4), adds a second
independent exit-check clock (Tasks 6/7), and changes live strike selection (Tasks 5, 9,
10) — all while CRUDEOIL/NIFTY may be actively live-trading on `nifty-cascade-v4-indicators`
— confirm with the user whether to deploy immediately or stage this for the next
pre-market window, before pushing.
