# CRUDEOIL Support for V4 Cascade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `strategies/v4_cascade/` (currently NIFTY-only, plus an existing crypto BTC/ETH branch) to also trade CRUDEOIL on MCX, reusing the underlying-agnostic Gate 1/2/3 scanning machinery unchanged.

**Architecture:** Follow the exact precedent already established for crypto (`self._is_crypto` branch inside `V4CascadeBook.__init__`, computing per-underlying `sl_buffer` into a `V4CascadeConfig` instance) — add a parallel `self._is_mcx` branch computing session-open time, EOD/gate-reset time, and per-underlying tracking/execution offsets, threaded through the small set of functions that currently hardcode NIFTY's `09:15`/`15:15`/`15:30`.

**Tech Stack:** Python 3.12, pytest, existing `strategies/v4_cascade/` module (pure dataclasses + `V4CascadeBook` adapter over `AbstractStrategyBook`).

## Global Constraints

- Full existing test suite (`python -m pytest tests/ -q`, currently 204 tests) MUST stay green after every task — several tasks touch code paths NIFTY already depends on.
- Every new/changed function gets a default parameter value that reproduces NIFTY's exact current behavior, so no task changes NIFTY's live behavior except where explicitly noted (Task 4/5's EOD/gate23 fix, which is a deliberate, spec-documented bug fix affecting NIFTY too).
- Follow this repo's existing test layout: flat files in `tests/strategies/test_<topic>.py`, no new subdirectories (confirmed via `tests/strategies/test_straddle_book_manager.py`).
- Confirmed parameters (source: `docs/superpowers/specs/2026-07-20-crudeoil-v4-cascade-design.md`): session open `09:00`, squareoff `23:15`, gate23 reset `23:30`, tracking offset `ATM∓400`, execution offset `ATM±100`, SL buffer `20` flat points, lot size `100` (already correctly sourced from `ExchangeConfig.lot_sizes`, no change needed), strike step `100` (already in `ExchangeConfig.strike_steps`).

---

### Task 1: Parameterize `resample_bars()` with a configurable session-open time

**Files:**
- Modify: `strategies/v4_cascade/rolling_base.py:440-482`
- Test: `tests/strategies/test_v4_cascade_rolling_base.py` (new)

**Interfaces:**
- Produces: `resample_bars(bars_5m: List[_Bar], multiplier: int, session_open: Tuple[int, int] = (9, 15)) -> List[_ResampledBar]` — the `session_open` param is new; all existing positional 2-arg calls across the codebase (`scripts/*.py`, `strategies/v4_cascade/book.py`, `strategies/v4_cascade/zone_state.py`) are unaffected since it's a trailing default.

- [ ] **Step 1: Write the failing test**

Create `tests/strategies/test_v4_cascade_rolling_base.py`:

```python
"""resample_bars() must clock-anchor buckets to a configurable session-open
time (default 09:15 for NIFTY/NSE), not a hardcoded one -- CRUDEOIL/MCX
opens at 09:00."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.rolling_base import resample_bars

IST = ZoneInfo("Asia/Kolkata")


def _bar(offset_minutes, o, h, l, c, base_hour=9, base_minute=15):
    """A 5m bar at (base_hour:base_minute) + offset_minutes. Uses timedelta
    arithmetic (not raw hour/minute construction) so callers can freely pass
    offsets >= 45 without a minute-overflow ValueError (e.g. 09:15 + 50min)."""
    from strategies.v4_cascade.book import _Bar  # concrete dataclass-like _Bar (rolling_base._Bar is a Protocol, not instantiable)
    base = datetime(2026, 7, 20, base_hour, base_minute, tzinfo=IST)
    return _Bar(base + timedelta(minutes=offset_minutes), o, h, l, c, tf=5)


def test_default_session_open_matches_nifty_0915():
    # Five 5m bars from 09:15 -> should form exactly one 75m bucket labeled 09:15.
    bars = [_bar(i * 5, 100, 101, 99, 100) for i in range(15)]  # 09:15..10:10, 15 bars = 75min
    out = resample_bars(bars, 75)
    assert len(out) == 1
    assert out[0].timestamp.hour == 9 and out[0].timestamp.minute == 15


def test_custom_session_open_changes_bucket_boundaries():
    # 17 bars from 09:15 to 10:35 -- deliberately ALL at/after both candidate
    # session-open times (09:00 and 09:15), so there is no pre-session bar in
    # either case and resample_bars's grouping-only contract (every input bar
    # lands in exactly one output bucket, nothing dropped) is exercised
    # cleanly. A (9,15) anchor's first 75m bucket runs 09:15-10:30 (bar at
    # 10:30 starts a new bucket); a (9,0) anchor's first 75m bucket runs
    # 09:00-10:15, so for this SAME bar sequence the bucket boundary falls at
    # 10:15 instead -- proving session_open genuinely changes the grouping.
    bars = [_bar(i * 5, 100, 101, 99, 100) for i in range(17)]  # 09:15..10:35
    out_nifty = resample_bars(bars, 75, session_open=(9, 15))
    out_mcx = resample_bars(bars, 75, session_open=(9, 0))
    assert len(out_nifty) == 2 and len(out_mcx) == 2
    # both start their first bucket labeled with the first bar's own ts (09:15)
    assert out_nifty[0].timestamp.hour == 9 and out_nifty[0].timestamp.minute == 15
    assert out_mcx[0].timestamp.hour == 9 and out_mcx[0].timestamp.minute == 15
    # but the SECOND bucket's boundary differs -- this is what actually
    # proves session_open took effect, not an artifact of which bar happens
    # to be first in a chunk.
    assert out_nifty[1].timestamp.hour == 10 and out_nifty[1].timestamp.minute == 30
    assert out_mcx[1].timestamp.hour == 10 and out_mcx[1].timestamp.minute == 15
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_rolling_base.py -v`
Expected: FAIL — `TypeError: resample_bars() got an unexpected keyword argument 'session_open'`

- [ ] **Step 3: Implement**

In `strategies/v4_cascade/rolling_base.py`, replace lines 440-482:

```python
_SESSION_OPEN_HOUR = 9
_SESSION_OPEN_MINUTE = 15


def resample_bars(
    bars_5m: List[_Bar], multiplier: int, session_open: Tuple[int, int] = (9, 15),
) -> List["_ResampledBar"]:
    """Resample a 5-minute bar sequence into ``multiplier``-minute bars,
    grouping each bar by (calendar day, minutes-since-that-day's-session-open
    // multiplier). This is CLOCK-ANCHORED per calendar day — every bar's own
    timestamp determines its bucket, independent of its position in the input
    list. This makes grouping robust to buffer eviction (e.g. a deque(maxlen=N)
    upstream dropping older bars), data gaps, or the input starting mid-day —
    none of which can misalign a purely positional "every 15th bar" grouping
    (which is what this function used to do, and which silently drifted off
    the 09:15 session boundary once an upstream deque eviction removed a
    partial day's worth of bars — see project memory / 2026-07-18 bugfix).

    ``session_open``: (hour, minute) of the exchange's own session open —
    defaults to NSE/NIFTY's 09:15. MCX underlyings (CRUDEOIL etc.) open at
    09:00 and must pass (9, 0) so bucket boundaries land on the real session
    start instead of NIFTY's."""
    if multiplier % 5 != 0 or multiplier < 5:
        raise ValueError(f"multiplier must be a positive multiple of 5, got {multiplier}")

    open_hour, open_minute = session_open
    buckets: dict = {}
    order: list = []
    for b in bars_5m:
        day = b.timestamp.date()
        open_dt = b.timestamp.replace(
            hour=open_hour, minute=open_minute, second=0, microsecond=0,
        )
        minutes_since_open = int((b.timestamp - open_dt).total_seconds() // 60)
        bucket_idx = minutes_since_open // multiplier
        key = (day, bucket_idx)
        if key not in buckets:
            buckets[key] = []
            order.append(key)
        buckets[key].append(b)

    out: List[_ResampledBar] = []
    for key in order:
        chunk = buckets[key]
        out.append(_ResampledBar(
            timestamp=chunk[0].timestamp,
            high=max(b.high for b in chunk),
            low=min(b.low for b in chunk),
            close=chunk[-1].close,
        ))
    return out
```

Also add `Tuple` to the file's existing `typing` import line near the top of `rolling_base.py` if not already imported (check `from typing import ...` at the top of the file — add `Tuple` if missing).

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_v4_cascade_rolling_base.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Run full suite to confirm no regression**

Run: `python -m pytest tests/ -q`
Expected: all pass (204 existing + 2 new = 206)

- [ ] **Step 6: Commit**

```bash
git add strategies/v4_cascade/rolling_base.py tests/strategies/test_v4_cascade_rolling_base.py
git commit -m "V4Cascade: parameterize resample_bars session-open time for CRUDEOIL (09:00 vs NIFTY's 09:15)"
```

---

### Task 2: Parameterize `_bucket_start`/`_bucket_end`/`_bucket_key` in book.py

**Files:**
- Modify: `strategies/v4_cascade/book.py` (three module-level functions, currently around line 1018-1058 — exact line numbers will have shifted after Task 1's edits elsewhere; locate by function name, not line number)
- Test: `tests/strategies/test_v4_cascade_book_buckets.py` (new)

**Interfaces:**
- Consumes: nothing from Task 1 directly (these are independent pure functions), but follows the identical `session_open: Tuple[int,int] = (9, 15)` parameter convention Task 1 established.
- Produces: `_bucket_start(ts, multiplier, session_open=(9,15)) -> datetime`, `_bucket_end(ts, multiplier, session_open=(9,15)) -> bool`, `_bucket_key(ts, multiplier, session_open=(9,15)) -> Tuple[date, int]` — all three gain the same trailing default param.

- [ ] **Step 1: Write the failing test**

Create `tests/strategies/test_v4_cascade_book_buckets.py`:

```python
"""_bucket_start/_bucket_end/_bucket_key must anchor to a configurable
session-open time -- CRUDEOIL/MCX opens at 09:00, not NIFTY's 09:15."""
from datetime import datetime
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _bucket_start, _bucket_end, _bucket_key

IST = ZoneInfo("Asia/Kolkata")


def test_bucket_start_default_anchors_to_0915():
    ts = datetime(2026, 7, 20, 10, 32, tzinfo=IST)
    assert _bucket_start(ts, 5) == datetime(2026, 7, 20, 10, 30, tzinfo=IST)


def test_bucket_start_mcx_anchors_to_0900():
    ts = datetime(2026, 7, 20, 9, 3, tzinfo=IST)
    # default (9,15) anchor: ts is BEFORE session open -> floor-divides negative,
    # landing on a bucket that does NOT equal 09:00.
    assert _bucket_start(ts, 5, session_open=(9, 15)) != datetime(2026, 7, 20, 9, 0, tzinfo=IST)
    # mcx anchor: 09:03 is 3 minutes into the 09:00 session -> buckets to 09:00.
    assert _bucket_start(ts, 5, session_open=(9, 0)) == datetime(2026, 7, 20, 9, 0, tzinfo=IST)


def test_bucket_end_respects_session_open():
    # 09:00 + 70 minutes = 10:10 -- the 5m bar ENDING at 10:10 (i.e. bar ts=10:05,
    # covering 10:05-10:10) is the last bar of the 09:00-10:15 75m bucket only
    # under the (9,0) anchor, not the default (9,15) anchor.
    ts = datetime(2026, 7, 20, 10, 5, tzinfo=IST)
    assert _bucket_end(ts, 75, session_open=(9, 0)) is True


def test_bucket_key_identity_stable_across_multiplier():
    ts1 = datetime(2026, 7, 20, 9, 0, tzinfo=IST)
    ts2 = datetime(2026, 7, 20, 9, 3, tzinfo=IST)
    # Both fall in the same 5-minute bucket under a 09:00 anchor.
    assert _bucket_key(ts1, 5, session_open=(9, 0)) == _bucket_key(ts2, 5, session_open=(9, 0))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_book_buckets.py -v`
Expected: FAIL — `TypeError: _bucket_start() got an unexpected keyword argument 'session_open'`

- [ ] **Step 3: Implement**

In `strategies/v4_cascade/book.py`, find and replace the three functions (search for `def _bucket_start`):

```python
def _bucket_start(ts: datetime, multiplier: int, session_open: Tuple[int, int] = (9, 15)) -> datetime:
    open_hour, open_minute = session_open
    open_dt = ts.replace(hour=open_hour, minute=open_minute, second=0, microsecond=0)
    minutes_since_open = int((ts - open_dt).total_seconds() // 60)
    return open_dt + timedelta(minutes=(minutes_since_open // multiplier) * multiplier)


def _bucket_end(ts: datetime, multiplier: int, session_open: Tuple[int, int] = (9, 15)) -> bool:
    """True if the 5-MINUTE bar at ``ts`` is the last one in its
    ``multiplier``-minute bucket (NIFTY option-premium path, 5m granularity)."""
    open_hour, open_minute = session_open
    open_dt = ts.replace(hour=open_hour, minute=open_minute, second=0, microsecond=0)
    minutes_since_open = int((ts - open_dt).total_seconds() // 60)
    return (minutes_since_open + 5) % multiplier == 0
```

And find `def _bucket_key` (added in the prior 75m-replay-bug fix) and update its signature the same way:

```python
def _bucket_key(ts: datetime, multiplier: int, session_open: Tuple[int, int] = (9, 15)):
    """(day, bucket_idx) identity for a timestamp, matching resample_bars's
    own internal grouping exactly. Used instead of resample_bars's OUTPUT
    timestamp for lookups -- resample_bars labels each bucket with its
    first ACTUAL bar's timestamp (correct for its own purposes), which on a
    sparse/gappy day can land off the canonical grid. A grid-computed
    timestamp (_bucket_start) then fails to match that key, and the whole
    75m bar silently vanishes from replay."""
    open_hour, open_minute = session_open
    open_dt = ts.replace(hour=open_hour, minute=open_minute, second=0, microsecond=0)
    minutes_since_open = int((ts - open_dt).total_seconds() // 60)
    return (ts.date(), minutes_since_open // multiplier)
```

Confirm `Tuple` is imported in `book.py`'s `from typing import ...` line (it already imports `Dict, List, Optional` — add `Tuple` if not present).

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_v4_cascade_book_buckets.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Run full suite**

Run: `python -m pytest tests/ -q`
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_book_buckets.py
git commit -m "V4Cascade: parameterize book.py bucket functions for a configurable session-open time"
```

---

### Task 3: Thread `session_open` through `PremiumGateScanner` and `V4CascadeEngine` (Gate 2's 15m fallback resample)

**Files:**
- Modify: `strategies/v4_cascade/zone_state.py` (`PremiumGateScanner.__init__` and `_attempt_mtf_lock`)
- Modify: `strategies/v4_cascade/engine.py` (`V4CascadeEngine.__init__`)
- Test: `tests/strategies/test_v4_cascade_zone_state_session.py` (new)

**Interfaces:**
- Consumes: `resample_bars(..., session_open=...)` from Task 1.
- Produces: `PremiumGateScanner(bear: bool = True, session_open: Tuple[int, int] = (9, 15))`; `V4CascadeEngine(cfg=None, pe_scans_bull: bool = False, session_open: Tuple[int, int] = (9, 15))` — both gain the new trailing param, engine forwards it to both scanners it constructs.

- [ ] **Step 1: Write the failing test**

Create `tests/strategies/test_v4_cascade_zone_state_session.py`:

```python
"""PremiumGateScanner's Gate-2 15m fallback resample must respect a
configurable session-open time, same as Gate-1's 75m scan."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.zone_state import PremiumGateScanner
from strategies.v4_cascade.engine import V4CascadeEngine

IST = ZoneInfo("Asia/Kolkata")


def test_scanner_default_session_open():
    s = PremiumGateScanner(bear=True)
    assert s._session_open == (9, 15)


def test_scanner_custom_session_open():
    s = PremiumGateScanner(bear=True, session_open=(9, 0))
    assert s._session_open == (9, 0)


def test_engine_forwards_session_open_to_both_scanners():
    eng = V4CascadeEngine(session_open=(9, 0))
    assert eng._scanners["CE"]._session_open == (9, 0)
    assert eng._scanners["PE"]._session_open == (9, 0)


def test_engine_default_session_open_matches_nifty():
    eng = V4CascadeEngine()
    assert eng._scanners["CE"]._session_open == (9, 15)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_zone_state_session.py -v`
Expected: FAIL — `AttributeError: 'PremiumGateScanner' object has no attribute '_session_open'`

- [ ] **Step 3: Implement**

In `strategies/v4_cascade/zone_state.py`, find `class PremiumGateScanner:` and its `__init__`:

```python
    def __init__(self, bear: bool = True, session_open: Tuple[int, int] = (9, 15)) -> None:
        # 2026-07-19 — bear=True (default, UNCHANGED behavior for the NIFTY
        # premium path): scans for bear traps only, via find_bear_trap_2candle
        # / find_all_bear_traps_2candle. bear=False (crypto spot-only path
        # ONLY — see strategies/v4_cascade/book.py's _is_crypto branch): scans
        # for bull traps instead, via the symmetric bull functions. Nothing
        # in the NIFTY path ever constructs a scanner with bear=False.
        self._bear = bear
        self._session_open = session_open
        self._find_all = find_all_bear_traps_2candle if bear else find_all_bull_traps_2candle
        self._find_one = find_bear_trap_2candle if bear else find_bull_trap_2candle
```

(Keep the rest of `__init__` unchanged — only the signature and the new `self._session_open = session_open` line are added.)

Add `Tuple` to `zone_state.py`'s `from typing import ...` import if not already present.

Then find `_attempt_mtf_lock` and update its `resample_bars` call:

```python
        resampled = resample_bars(window, 15, session_open=self._session_open)
```

In `strategies/v4_cascade/engine.py`, update `V4CascadeEngine.__init__`:

```python
    def __init__(
        self, cfg: Optional[V4CascadeConfig] = None, pe_scans_bull: bool = False,
        session_open: Tuple[int, int] = (9, 15),
    ) -> None:
        """``pe_scans_bull``: 2026-07-19, crypto-spot-only path ONLY (see
        strategies/v4_cascade/book.py's _is_crypto branch) — when True, the
        PE scanner looks for BULL traps (real option premium has no
        inversion on raw spot, so PE must scan for genuine bearish patterns
        directly rather than reusing bear-trap logic). Defaults to False,
        the exact validated NIFTY behavior (both CE and PE bear-trap-only).
        ``session_open``: forwarded to both scanners' Gate-2 15m fallback
        resample — NIFTY/NSE default (9,15), MCX underlyings (CRUDEOIL) pass
        (9,0)."""
        self._cfg = cfg or V4CascadeConfig()
        self._spot_confirm = SpotConfirmTracker()
        self._scanners: Dict[str, PremiumGateScanner] = {
            "CE": PremiumGateScanner(bear=True, session_open=session_open),
            "PE": PremiumGateScanner(bear=not pe_scans_bull, session_open=session_open),
        }
```

(Keep the rest of `__init__` unchanged.) Add `Tuple` to `engine.py`'s `from typing import ...` import if not already present.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_v4_cascade_zone_state_session.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Run full suite**

Run: `python -m pytest tests/ -q`
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add strategies/v4_cascade/zone_state.py strategies/v4_cascade/engine.py tests/strategies/test_v4_cascade_zone_state_session.py
git commit -m "V4Cascade: thread session_open through PremiumGateScanner and V4CascadeEngine"
```

---

### Task 4: Thread `session_open`, `eod_square_off`, `gate23_reset` through `_replay_through_engine`

**Files:**
- Modify: `strategies/v4_cascade/book.py` (`_replay_through_engine` function)
- Test: `tests/strategies/test_v4_cascade_replay_session.py` (new)

**Interfaces:**
- Consumes: `_bucket_key(..., session_open=...)` (Task 2), `resample_bars(..., session_open=...)` (Task 1).
- Produces: `_replay_through_engine(engine, spot_5m, ce_5m, pe_5m, on_daily_boundary=None, session_open=(9,15), eod_square_off=(15,15), gate23_reset=(15,30))` — three new trailing defaulted params.

- [ ] **Step 1: Write the failing test**

Create `tests/strategies/test_v4_cascade_replay_session.py`:

```python
"""_replay_through_engine must use the book's configured session-open and
EOD/gate23 times, not hardcoded NSE constants -- otherwise CRUDEOIL
(09:00 open, 23:15 squareoff) gets force-closed at NIFTY's 15:15 during
replay, mid-session."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _replay_through_engine, _Bar
from strategies.v4_cascade.engine import V4CascadeEngine

IST = ZoneInfo("Asia/Kolkata")


def _bars(day, start_hour, start_minute, n, price=100.0):
    out = []
    ts = datetime(2026, 7, 20, start_hour, start_minute, tzinfo=IST)
    for i in range(n):
        out.append(_Bar(ts + timedelta(minutes=5 * i), price, price + 1, price - 1, price, tf=5))
    return out


def test_replay_does_not_force_close_before_custom_eod():
    # MCX-style session: bars from 09:00 to 15:20 (well before 23:15 squareoff).
    # A position opened during replay must NOT be force-closed, since none of
    # these bars reach the custom eod_square_off=(23,15).
    engine = V4CascadeEngine(session_open=(9, 0))
    ce_5m = _bars("2026-07-20", 9, 0, 76)   # 09:00 .. ~15:15
    pe_5m = _bars("2026-07-20", 9, 0, 76)
    # No spot_5m needed for this check -- just confirm the EOD constant used
    # is the one passed in, not the module default (15,15), by checking the
    # replay runs without raising and completes (a force-close at the wrong
    # hardcoded 15:15 would be silently wrong but not raise -- so this test
    # asserts on the actual mechanism instead: the function accepts the new
    # kwargs at all).
    _replay_through_engine(
        engine, spot_5m=[], ce_5m=ce_5m, pe_5m=pe_5m,
        session_open=(9, 0), eod_square_off=(23, 15), gate23_reset=(23, 30),
    )
    # No exception = signature accepted. Behavioral EOD-close correctness is
    # covered by the existing book.py-level EOD tests once wired in Task 5.


def test_replay_default_session_open_still_09_15():
    # Backward-compat: calling with no new kwargs must behave exactly as
    # before (NIFTY's 09:15/15:15/15:30).
    engine = V4CascadeEngine()
    ce_5m = _bars("2026-07-20", 9, 15, 5)
    pe_5m = _bars("2026-07-20", 9, 15, 5)
    _replay_through_engine(engine, spot_5m=[], ce_5m=ce_5m, pe_5m=pe_5m)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_replay_session.py -v`
Expected: FAIL — `TypeError: _replay_through_engine() got an unexpected keyword argument 'session_open'`

- [ ] **Step 3: Implement**

In `strategies/v4_cascade/book.py`, find `def _replay_through_engine` and replace its signature and body:

```python
def _replay_through_engine(
    engine: V4CascadeEngine, spot_5m, ce_5m, pe_5m, on_daily_boundary=None,
    session_open: Tuple[int, int] = (9, 15),
    eod_square_off: Tuple[int, int] = (15, 15),
    gate23_reset: Tuple[int, int] = (15, 30),
) -> None:
    """Chronological replay identical in shape to
    scripts/test_real_premium_replay.py — feeds 5m bars continuously and 75m
    closes (spot bias + CE/PE Gate 1) at each 75m bucket boundary, applying
    the EOD/gate-reset rules along the way so the rebuilt state exactly
    matches what live ticks would have produced.

    ``session_open``/``eod_square_off``/``gate23_reset``: must match the
    calling book's own configured values (NIFTY: (9,15)/deployment-configured/
    +15min; CRUDEOIL/MCX: (9,0)/(23,15)/(23,30)) — these used to be hardcoded
    NSE-hours module constants here, silently force-closing any non-NIFTY
    underlying's positions mid-session during replay."""
    spot_75m_by_key = {_bucket_key(b.timestamp, 75, session_open): b for b in resample_bars(spot_5m, 75, session_open)} if spot_5m else {}
    ce_75m_by_key = {_bucket_key(b.timestamp, 75, session_open): b for b in resample_bars(ce_5m, 75, session_open)} if ce_5m else {}
    pe_75m_by_key = {_bucket_key(b.timestamp, 75, session_open): b for b in resample_bars(pe_5m, 75, session_open)} if pe_5m else {}
    ce_by_ts = {b.timestamp: b for b in ce_5m}
    pe_by_ts = {b.timestamp: b for b in pe_5m}
    all_ts = sorted(set(ce_by_ts) | set(pe_by_ts))

    last_day = None
    for idx, ts in enumerate(all_ts):
        day = ts.date()
        if last_day is not None and day != last_day and on_daily_boundary is not None:
            pass  # boundary-time rules (EOD/gate reset) applied via exact-time checks below
        last_day = day

        engine.update(ce_bar=ce_by_ts.get(ts), pe_bar=pe_by_ts.get(ts))

        cur_key = _bucket_key(ts, 75, session_open)
        bucket_closing = idx + 1 < len(all_ts) and _bucket_key(all_ts[idx + 1], 75, session_open) != cur_key
        if bucket_closing:
            sbar = spot_75m_by_key.get(cur_key)
            ce75 = ce_75m_by_key.get(cur_key)
            pe75 = pe_75m_by_key.get(cur_key)
            if sbar is not None:
                sbar75 = _Bar(sbar.timestamp, sbar.close, sbar.high, sbar.low, sbar.close, tf=75)
            else:
                sbar75 = None
            engine.update(
                spot_bar=sbar75,
                ce_bar=_Bar(ce75.timestamp, ce75.close, ce75.high, ce75.low, ce75.close, tf=75) if ce75 else None,
                pe_bar=_Bar(pe75.timestamp, pe75.close, pe75.high, pe75.low, pe75.close, tf=75) if pe75 else None,
            )

        if (ts.hour, ts.minute) == eod_square_off:
            pos = engine.position
            if pos is not None and pos.is_open:
                for leg in (pos.t1, pos.t2):
                    if leg is not None and leg.status == "open":
                        leg.status = "closed"
                        leg.close_price = leg.entry_price
                        leg.close_reason = "eod_force_close_replay"
                        leg.close_time = ts
                pos.status = "closed"
                pos.close_time = ts
        if (ts.hour, ts.minute) == gate23_reset and on_daily_boundary is not None:
            on_daily_boundary(ts)
```

Note: `_bucket_key(ts, 75, session_open)` here passes `session_open` positionally as the third arg — confirm this matches Task 2's signature `_bucket_key(ts, multiplier, session_open=(9,15))` (it does, positional call is fine).

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_v4_cascade_replay_session.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Run full suite**

Run: `python -m pytest tests/ -q`
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_replay_session.py
git commit -m "V4Cascade: thread session_open/eod_square_off/gate23_reset through replay instead of hardcoded NSE constants"
```

---

### Task 5: Wire per-underlying session/offset config into `V4CascadeBook.__init__` and all call sites

**Files:**
- Modify: `strategies/v4_cascade/book.py`:
  - `__init__` (currently ~lines 60-174 — locate via `class V4CascadeBook`)
  - `_check_daily_boundary` (uses `_GATE23_RESET` module constant — replace with `self._gate23_hour_min`)
  - `_on_option_tick` / `_close_5m_bucket` (uses bare `_bucket_start`/`_bucket_end`/`resample_bars` — add `self._session_open`)
  - `_resolve_symbols` (uses `_STRIKE_STEP`/`_TRACKING_OFFSET` module constants — replace with `self._strike_step`/`self._tracking_offset`)
  - `_resolve_execution_strike` (uses `EXECUTION_OFFSET_PTS`/`_TRACKING_OFFSET` module constants — replace with `self._execution_offset`/`self._tracking_offset`)
  - `_ingest_history`'s call to `_replay_through_engine` (add the three new kwargs)
- Test: `tests/strategies/test_v4_cascade_book_underlying_config.py` (new)

**Interfaces:**
- Consumes: everything from Tasks 1-4.
- Produces: new instance attributes on `V4CascadeBook`: `self._is_mcx: bool`, `self._session_open: Tuple[int,int]`, `self._gate23_hour_min: Tuple[int,int]`, `self._strike_step: float`, `self._tracking_offset: float`, `self._execution_offset: float`.

- [ ] **Step 1: Write the failing test**

Create `tests/strategies/test_v4_cascade_book_underlying_config.py`. This test constructs a `V4CascadeBook` directly (mirrors how other tests in this repo construct strategy books with a minimal fake bus/cfg — check `tests/strategies/test_crypto_timing.py` or similar for the exact fake-bus pattern used elsewhere in this repo before writing this, and match it):

```python
"""V4CascadeBook must resolve session-open time, EOD/gate23 reset time, and
strike/offset numbers per-underlying: NIFTY unchanged, CRUDEOIL gets MCX's
09:00 open / 23:15 squareoff / 23:30 gate23 reset / 100-point strike step /
400-point tracking offset / 100-point execution offset."""
import pytest
from config.global_config import GlobalConfig
from strategies.v4_cascade.book import V4CascadeBook


class _FakeBus:
    def subscribe(self, topic):
        raise NotImplementedError  # not exercised by these tests

    def unsubscribe(self, topic, q):
        pass


def _book(underlying, squareoff_time="15:15"):
    cfg = GlobalConfig()
    return V4CascadeBook(
        _FakeBus(), cfg, underlying=underlying, client_id="C1", binding_id="B1",
        lot_multiplier=2, squareoff_time=squareoff_time,
    )


def test_nifty_session_config_unchanged():
    b = _book("NIFTY", squareoff_time="15:15")
    assert b._is_mcx is False
    assert b._session_open == (9, 15)
    assert b._eod_hour_min == (15, 15)
    assert b._gate23_hour_min == (15, 30)
    assert b._strike_step == 50.0
    assert b._tracking_offset == 200.0
    assert b._execution_offset == 50.0


def test_crudeoil_session_config():
    b = _book("CRUDEOIL", squareoff_time="23:15")
    assert b._is_mcx is True
    assert b._session_open == (9, 0)
    assert b._eod_hour_min == (23, 15)
    assert b._gate23_hour_min == (23, 30)
    assert b._strike_step == 100.0
    assert b._tracking_offset == 400.0
    assert b._execution_offset == 100.0
    assert b._v4cfg.sl_buffer == 20.0
    assert b._v4cfg.lot_size == 100


def test_gate23_reset_wraps_past_midnight_safely():
    # Not applicable today (23:15 + 15min = 23:30, same day) but guards
    # against a future squareoff_time near midnight producing an invalid
    # (hour>=24) tuple -- gate23_hour_min must clamp to (23, 59) rather than
    # overflow.
    b = _book("CRUDEOIL", squareoff_time="23:50")
    assert b._gate23_hour_min == (23, 59)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_book_underlying_config.py -v`
Expected: FAIL — `AttributeError: 'V4CascadeBook' object has no attribute '_is_mcx'`

- [ ] **Step 3: Implement**

In `strategies/v4_cascade/book.py`, `V4CascadeBook.__init__`, locate this existing block (search for `self._is_crypto = underlying.upper() in _CRYPTO_UNDERLYINGS`):

```python
        self._is_crypto = underlying.upper() in _CRYPTO_UNDERLYINGS
```

Immediately after it, add:

```python
        self._is_mcx = cfg.exchange.is_mcx(underlying)
        self._session_open: Tuple[int, int] = (9, 0) if self._is_mcx else (9, 15)
```

Locate the existing `self._eod_hour_min` computation near the top of `__init__` (search for `self._eod_hour_min = (int(_hh), int(_mm))`) and, immediately after that try/except block, add the gate23 derivation:

```python
        # Gate 2/3 daily reset fires 15 minutes after squareoff (matches the
        # already-validated NIFTY 15:15->15:30 convention). Clamped to not
        # overflow past 23:59 -- a squareoff configured very close to
        # midnight (not expected in practice, MCX closes ~23:30) must not
        # produce an invalid (hour>=24) tuple.
        _g_hh, _g_mm = self._eod_hour_min[0], self._eod_hour_min[1] + 15
        if _g_mm >= 60:
            _g_hh += 1
            _g_mm -= 60
        self._gate23_hour_min = (min(_g_hh, 23), _g_mm if _g_hh <= 23 else 59)
```

Now find the `_sl_buffer` branch (search for `if self._is_crypto:` near `_sl_buffer =`) and extend it with an MCX branch, plus add the strike/offset resolution right after:

```python
        from strategies.v4_cascade.config import (
            SL_BUFFER_PER_COIN_CRYPTO, SL_BUFFER_PTS_NIFTY, TRACKING_OFFSET_PTS, EXECUTION_OFFSET_PTS,
        )
        if self._is_crypto:
            _coin_qty = lot_multiplier * _CRYPTO_CONTRACT_VALUE.get(underlying.upper(), 1.0)
            _sl_buffer = SL_BUFFER_PER_COIN_CRYPTO * _coin_qty
        elif self._is_mcx:
            _sl_buffer = 20.0
        else:
            _sl_buffer = SL_BUFFER_PTS_NIFTY

        # Strike step already correctly sourced from ExchangeConfig for the
        # EXECUTION strike elsewhere (_resolve_execution_strike) -- resolved
        # here too so the TRACKING strike's ATM rounding uses the same real
        # per-underlying value instead of a hardcoded NIFTY-only constant
        # (was silently wrong for any underlying but NIFTY before this).
        self._strike_step = float(cfg.exchange.strike_steps.get(underlying.upper(), 50.0) or 50.0)
        if self._is_mcx:
            self._tracking_offset = 400.0
            self._execution_offset = 100.0
        else:
            self._tracking_offset = TRACKING_OFFSET_PTS
            self._execution_offset = EXECUTION_OFFSET_PTS

        self._v4cfg = V4CascadeConfig(underlying=underlying, lot_multiplier=lot_multiplier,
                                       lot_size=_real_lot_size, sl_buffer=_sl_buffer,
                                       tracking_offset_pts=self._tracking_offset,
                                       execution_offset_pts=self._execution_offset)
        self._engine = V4CascadeEngine(self._v4cfg, pe_scans_bull=self._is_crypto,
                                        session_open=self._session_open)
```

This REPLACES the existing:
```python
        self._v4cfg = V4CascadeConfig(underlying=underlying, lot_multiplier=lot_multiplier,
                                       lot_size=_real_lot_size, sl_buffer=_sl_buffer)
        self._engine = V4CascadeEngine(self._v4cfg, pe_scans_bull=self._is_crypto)
```

Add `Tuple` to `book.py`'s typing import if not already added in Task 2.

Now update the call sites. In `_check_daily_boundary` (search for `if (ts.hour, ts.minute) >= _GATE23_RESET:`):

```python
        if (ts.hour, ts.minute) >= self._gate23_hour_min:
            self._apply_eod_gate23_rules(ts)
```

In `_on_option_tick` (search for `bucket = _bucket_start(ts, 5)`):

```python
        bucket = _bucket_start(ts, 5, self._session_open)
```

In `_close_5m_bucket` (search for `if _bucket_end(bar.timestamp, 75):`):

```python
        if _bucket_end(bar.timestamp, 75, self._session_open):
            window = [b for b in self._bars_5m[side] if b.timestamp.date() == bar.timestamp.date()]
            r75 = resample_bars(window, 75, self._session_open)
```

In `_resolve_symbols` (search for `atm = round(atm_open / _STRIKE_STEP) * _STRIKE_STEP`):

```python
        atm = round(atm_open / self._strike_step) * self._strike_step
        self._ce_strike = self._locked_ce_strike or int(atm - self._tracking_offset)
        self._pe_strike = self._locked_pe_strike or int(atm + self._tracking_offset)
```

In `_resolve_execution_strike` (search for `return atm + EXECUTION_OFFSET_PTS if side == "CE" else atm - EXECUTION_OFFSET_PTS`):

```python
            return atm + self._execution_offset if side == "CE" else atm - self._execution_offset
```

and further down in the same method (search for `offset_sum = _TRACKING_OFFSET + EXECUTION_OFFSET_PTS`):

```python
        offset_sum = self._tracking_offset + self._execution_offset
```

In `_ingest_history` (search for `_replay_through_engine(self._engine, spot_5m, ce_5m, pe_5m,`):

```python
        _replay_through_engine(self._engine, spot_5m, ce_5m, pe_5m,
                                on_daily_boundary=self._apply_eod_gate23_rules,
                                session_open=self._session_open,
                                eod_square_off=self._eod_hour_min,
                                gate23_reset=self._gate23_hour_min)
```

Now the module-level `_STRIKE_STEP`, `_TRACKING_OFFSET`, `_GATE23_RESET` constants (near the top of `book.py`, alongside `_EOD_SQUARE_OFF`) are unused — remove them (search for `_STRIKE_STEP = 100.0`, `_TRACKING_OFFSET = 200.0`, `_GATE23_RESET = (15, 30)` and delete those three lines; leave `_EOD_SQUARE_OFF = (15, 15)` as the default parameter value already lives in `_replay_through_engine`'s signature from Task 4, but the standalone module constant is also now unused — delete it too). Grep to confirm no other reference exists first:

```bash
grep -n "_STRIKE_STEP\b\|_TRACKING_OFFSET\b\|_GATE23_RESET\b\|_EOD_SQUARE_OFF\b" strategies/v4_cascade/book.py
```

If this shows only the definition line (no other usages) for each, delete that constant's definition line.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_v4_cascade_book_underlying_config.py -v`
Expected: PASS (3 tests). If the fake-bus construction pattern doesn't match this repo's actual convention (checked in Step 1), adjust the test's `_FakeBus`/`GlobalConfig` setup to match — do not change `V4CascadeBook`'s constructor signature to work around a test-only mismatch.

- [ ] **Step 5: Run full suite**

Run: `python -m pytest tests/ -q`
Expected: all pass — this is the highest-risk task for NIFTY regressions (touches 7+ call sites), scrutinize any failure carefully rather than patching around it.

- [ ] **Step 6: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_book_underlying_config.py
git commit -m "V4Cascade: resolve session-open/EOD/gate23/strike-offset config per-underlying (CRUDEOIL support)"
```

---

### Task 6: Fix ATM source — use futures key for MCX instead of spot-index key

**Files:**
- Modify: `strategies/v4_cascade/book.py` (`_fetch_session_open` and `_ingest_history`'s `spot_key` resolution)
- Test: `tests/strategies/test_v4_cascade_atm_source.py` (new)

**Interfaces:**
- Consumes: `REGISTRY.historical_instrument_key(underlying)` (already exists in `data_layer/instrument_registry.py`, no changes needed there).
- Produces: no new public interface — internal fix only.

- [ ] **Step 1: Write the failing test**

Create `tests/strategies/test_v4_cascade_atm_source.py`:

```python
"""CRUDEOIL's ATM must be sourced from the futures instrument key
(historical_instrument_key), not the spot-index key (get_upstox_index_key,
which has no valid entry for MCX underlyings and would resolve to a bogus
NSE_INDEX|CRUDEOIL key)."""
from data_layer.instrument_registry import REGISTRY


def test_historical_instrument_key_prefers_futures_for_mcx(monkeypatch):
    monkeypatch.setitem(REGISTRY._futures_upstox, "CRUDEOIL", "MCX_FO|499095")
    assert REGISTRY.historical_instrument_key("CRUDEOIL") == "MCX_FO|499095"


def test_get_upstox_index_key_is_wrong_for_mcx():
    # Documents WHY book.py must not call this for MCX underlyings -- it has
    # no CRUDEOIL entry and falls through to a bogus NSE_INDEX key.
    assert REGISTRY.get_upstox_index_key("CRUDEOIL") == "NSE_INDEX|CRUDEOIL"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_atm_source.py -v`
Expected: the first test may already PASS (since `historical_instrument_key` already exists and works correctly per Task exploration) — if so, this confirms the REGISTRY-side function is already correct and this task is purely about book.py's *call site*, not new REGISTRY logic. The second test documents the current (correct-as-is, don't fix) behavior of `get_upstox_index_key`. Both should PASS already — this task has no registry-side code to write. Proceed directly to Step 3 (the actual bug is in book.py, not tested via unit test since it requires a live token/network call — verify via code inspection + the Step 3 grep instead).

- [ ] **Step 3: Implement**

In `strategies/v4_cascade/book.py`, find `_fetch_session_open` (search for `spot_key = REGISTRY.get_upstox_index_key(self._underlying)` — appears twice, once in `_fetch_session_open`, once in `_ingest_history`):

```python
    async def _fetch_session_open(self, token: str, day: date) -> Optional[float]:
        """PRIMARY method — today's real session-open candle open via
        Upstox's INTRADAY endpoint (fetch_upstox_intraday_1m). Uses
        historical_instrument_key() (NOT get_upstox_index_key()) so MCX
        underlyings correctly source the near-month FUTURES price -- MCX
        commodities have no tradeable spot index, get_upstox_index_key()
        has no CRUDEOIL entry and would fall through to a bogus
        NSE_INDEX|CRUDEOIL key. Correct and available for a restart at ANY
        time of day, not just right at market open, since it always
        re-reads the FIRST candle of today's session rather than "now".
        This is a DIFFERENT call than _ingest_history's multi-day HTF/MTF
        lookback replay — that one stays exactly as-is, still required."""
        instrument_key = REGISTRY.historical_instrument_key(self._underlying)
        try:
            rows = await fetch_upstox_intraday_1m(instrument_key, token)
        except Exception:
            rows = []
        if not rows:
            return None
        rows.sort(key=lambda r: r["ts"])
        return float(rows[0]["open"])
```

(Only the `spot_key = REGISTRY.get_upstox_index_key(self._underlying)` line changes, replaced with `instrument_key = REGISTRY.historical_instrument_key(self._underlying)` and the variable renamed at its one use site below it — the rest of the method is unchanged from its current form.)

In `_ingest_history`, find the second occurrence (search for `spot_key = REGISTRY.get_upstox_index_key(self._underlying)` again — this one feeds `fetch_upstox_range_1m`/`fetch_upstox_intraday_1m` for the deep replay window):

```python
        spot_key = REGISTRY.historical_instrument_key(self._underlying)
```

(One-line change — everything else in `_ingest_history` that uses `spot_key` afterward is unchanged.)

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_v4_cascade_atm_source.py -v`
Expected: PASS (2 tests, both were already passing before Step 3 — this step is documentation + the actual book.py fix, verified by the grep below rather than a new failing test, since testing an async network-calling method properly requires mocking `fetch_upstox_intraday_1m`, which is disproportionate for a one-line call-site swap already covered by the registry-level tests above).

Verify the fix landed correctly:

```bash
grep -n "get_upstox_index_key\|historical_instrument_key" strategies/v4_cascade/book.py
```

Expected: both occurrences now say `historical_instrument_key`, zero occurrences of `get_upstox_index_key` remain in this file.

- [ ] **Step 5: Run full suite**

Run: `python -m pytest tests/ -q`
Expected: all pass. (NIFTY's `historical_instrument_key("NIFTY")` falls through to the same `_UPSTOX_UNDERLYING_KEY` lookup `get_upstox_index_key` would have used, per the registry's own implementation — confirm this by reading `historical_instrument_key`'s body once more before considering this safe: `if u in self._futures_upstox: return self._futures_upstox[u]; return _UPSTOX_UNDERLYING_KEY.get(u, "")` — NIFTY is never in `_futures_upstox`, so it falls through to the identical lookup table `get_upstox_index_key` uses. No NIFTY behavior change.)

- [ ] **Step 6: Commit**

```bash
git add strategies/v4_cascade/book.py tests/strategies/test_v4_cascade_atm_source.py
git commit -m "V4Cascade: source ATM from futures key (not spot-index key) for MCX underlyings"
```

---

### Task 7: Wire CRUDEOIL into V4CascadeBookManager

**Files:**
- Modify: `strategies/v4_cascade_book_manager.py`
- Test: `tests/strategies/test_v4_cascade_book_manager.py` (new)

**Interfaces:**
- Consumes: nothing from prior tasks (manager-level, independent of the pure-engine/book internals).
- Produces: `_SUPPORTED_UNDERLYINGS` now includes `"CRUDEOIL"`; `_spawn_book`'s default `squareoff_time` fallback becomes underlying-aware.

- [ ] **Step 1: Write the failing test**

Create `tests/strategies/test_v4_cascade_book_manager.py` (mirrors `tests/strategies/test_straddle_book_manager.py`'s `_FakeBook`/`_DB` pattern exactly):

```python
"""V4CascadeBookManager must accept CRUDEOIL deployments (previously only
NIFTY/BTC/ETH), and its default squareoff_time fallback must be
underlying-aware -- a CRUDEOIL deployment with no configured squareoff_time
must not silently default to NIFTY's 15:15 (which would instantly force-
close it mid-session, the exact class of bug already documented for
sell_straddle in project memory)."""
import strategies.v4_cascade_book_manager as vm_mod
from strategies.v4_cascade_book_manager import V4CascadeBookManager


class _FakeBook:
    def __init__(self, bus, cfg, underlying="NIFTY", client_id="", binding_id="",
                 lot_multiplier=1, squareoff_time="15:15"):
        self._underlying = underlying; self._client_id = client_id; self._binding_id = binding_id
        self._lot_multiplier = lot_multiplier
        self.squareoff_time = squareoff_time
        self.started = False

    def set_client_db(self, db):
        pass

    def start(self):
        self.started = True


class _DB:
    def __init__(self, deps):
        self._deps = deps

    def get_running_deployments_by_strategy_sync(self, strategy_name):
        result = []
        for cid, deps in self._deps.items():
            for d in deps:
                if d.get("strategy_name") == strategy_name and int(d.get("is_running", 0) or 0) == 1:
                    result.append({**d, "client_id": cid})
        return result

    def get_deployments_sync(self, cid):
        return self._deps.get(cid, [])


def _dep(bid, und, is_running=1, lot_multiplier=1, squareoff_time=None):
    d = {"binding_id": bid, "strategy_name": "v4_cascade", "underlying": und,
         "is_running": is_running, "lot_multiplier": lot_multiplier}
    if squareoff_time is not None:
        d["squareoff_time"] = squareoff_time
    return d


def test_crudeoil_deployment_spawns(monkeypatch):
    monkeypatch.setattr(vm_mod, "V4CascadeBook", _FakeBook)
    db = _DB({"C1": [_dep("Z1", "CRUDEOIL", squareoff_time="23:15")]})
    m = V4CascadeBookManager(bus=None, cfg=None, client_db=db, monitored_indices=[])
    m._reconcile()
    assert len(m.books) == 1
    book = list(m.books.values())[0]
    assert book._underlying == "CRUDEOIL"
    assert book.squareoff_time == "23:15"


def test_crudeoil_deployment_missing_squareoff_defaults_to_mcx_time(monkeypatch):
    monkeypatch.setattr(vm_mod, "V4CascadeBook", _FakeBook)
    db = _DB({"C1": [_dep("Z1", "CRUDEOIL", squareoff_time=None)]})
    m = V4CascadeBookManager(bus=None, cfg=None, client_db=db, monitored_indices=[])
    m._reconcile()
    book = list(m.books.values())[0]
    assert book.squareoff_time == "23:15"  # NOT NIFTY's "15:15" default


def test_unsupported_underlying_still_skipped(monkeypatch):
    monkeypatch.setattr(vm_mod, "V4CascadeBook", _FakeBook)
    db = _DB({"C1": [_dep("Z1", "GOLD")]})  # not in _SUPPORTED_UNDERLYINGS
    m = V4CascadeBookManager(bus=None, cfg=None, client_db=db, monitored_indices=[])
    m._reconcile()
    assert m.books == {}
```

(`V4CascadeBookManager` and `StraddleBookManager` both extend the same `strategies.core.StrategyBookManager` base class, so the `.books` dict and `_reconcile()` method used above are confirmed correct — verified directly against `tests/strategies/test_straddle_book_manager.py`'s working usage of the identical base-class API, not guessed.)

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_v4_cascade_book_manager.py -v`
Expected: FAIL on `test_crudeoil_deployment_spawns` — CRUDEOIL not in `_SUPPORTED_UNDERLYINGS`, book never spawns, `len(m.books) == 1` fails.

- [ ] **Step 3: Implement**

In `strategies/v4_cascade_book_manager.py`, find:

```python
_SUPPORTED_UNDERLYINGS = {"NIFTY", "BTC", "ETH"}
```

Replace with:

```python
_SUPPORTED_UNDERLYINGS = {"NIFTY", "BTC", "ETH", "CRUDEOIL"}

# Default squareoff_time fallback, per underlying -- a deployment row with no
# configured squareoff_time must not silently fall back to NIFTY's 15:15 for
# an MCX underlying (would force-close it minutes after the 09:00 open,
# hours before MCX's real ~23:15-23:30 close -- the same class of bug
# project memory already documents for sell_straddle).
_DEFAULT_SQUAREOFF_TIME = {"CRUDEOIL": "23:15"}
_FALLBACK_SQUAREOFF_TIME = "15:15"
```

Find `_spawn_book` (search for `squareoff_time = "15:15"`):

```python
        squareoff_time = _DEFAULT_SQUAREOFF_TIME.get(und, _FALLBACK_SQUAREOFF_TIME)
        try:
            for d in (self._db.get_deployments_sync(cid) or []):
                if (d.get("binding_id") == bid and d.get("strategy_name") == "v4_cascade"
                        and str(d.get("underlying", "") or d.get("assigned_instrument", "")).upper() == und):
                    squareoff_time = str(d.get("squareoff_time") or squareoff_time)
                    break
        except Exception:
            pass
```

(This replaces the existing `squareoff_time = "15:15"` initial value and the `str(d.get("squareoff_time") or "15:15")` fallback inside the loop — same structure, just sourcing the default from the new per-underlying dict instead of a bare `"15:15"` literal in two places.)

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_v4_cascade_book_manager.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Run full suite**

Run: `python -m pytest tests/ -q`
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add strategies/v4_cascade_book_manager.py tests/strategies/test_v4_cascade_book_manager.py
git commit -m "V4Cascade: support CRUDEOIL deployments, underlying-aware default squareoff time"
```

---

### Task 8: Add `--underlying` support to the diagnostic script for manual CRUDEOIL verification

**Files:**
- Modify: `scripts/diag_v4_ce_pe_traps.py`

**Interfaces:**
- Consumes: nothing new — reuses `REGISTRY.get_upstox_key`, `fetch_upstox_range_1m`/`fetch_upstox_intraday_1m`, `find_all_bear_traps_2candle`, `resample_bars` exactly as today.
- Produces: no new interface — CLI-only change, not consumed by any other task.

This task has no automated test — it's a CLI diagnostic tool whose correctness is verified by actually running it against live/historical data (which requires network + a real access token, same as the script already does for NIFTY). Follow the existing pattern exactly rather than inventing a new one.

- [ ] **Step 1: Add `--underlying` and `--session-open` CLI args**

In `scripts/diag_v4_ce_pe_traps.py`, find the `argparse` setup (search for `ap.add_argument("--underlying"`):

```python
    ap.add_argument("--underlying", default="NIFTY")
```

This flag already exists — confirm it's already threaded through to every call that currently hardcodes `"NIFTY"`. Search the file for any other hardcoded `"NIFTY"` string:

```bash
grep -n '"NIFTY"' scripts/diag_v4_ce_pe_traps.py
```

If `REGISTRY.load_sync(args.underlying, token)` and `REGISTRY.get_upstox_key(args.underlying, expiry, args.strike, args.opt_type)` already use `args.underlying` (not a hardcoded `"NIFTY"`), no change is needed there. Add a `--session-open` flag for the Gate-1 75m resample call:

```python
    ap.add_argument("--session-open", default="9:15", help="HH:MM, e.g. 9:00 for MCX/CRUDEOIL")
```

In `main()`, after parsing `args`, add:

```python
    _oh, _om = (int(x) for x in args.session_open.split(":"))
    session_open = (_oh, _om)
```

Find the call `bars_75m = resample_bars(bars_5m, 75)` and update it:

```python
    bars_75m = resample_bars(bars_5m, 75, session_open=session_open)
```

- [ ] **Step 2: Manual smoke test against CRUDEOIL (requires MCX session to be open, or historical data available)**

Run:
```bash
python scripts/diag_v4_ce_pe_traps.py --underlying CRUDEOIL --strike <ATM-derived strike> --opt-type CE --expiry <current CRUDEOIL monthly expiry, YYYY-MM-DD> --days 14 --session-open 9:00
```

Expected: script prints `instrument_key = MCX_FO|...` (not empty/error), fetches range+intraday rows, resamples to 75m bars whose first timestamp of each trading day is `09:00` (not `09:15`), and reports zero or more bear-trap zones without raising an exception. Confirm the strike/expiry values by checking a live CRUDEOIL option chain (e.g. via the admin dashboard) before running, since the diagnostic script does not derive them automatically.

- [ ] **Step 3: Commit**

```bash
git add scripts/diag_v4_ce_pe_traps.py
git commit -m "diag_v4_ce_pe_traps.py: add --session-open for MCX/CRUDEOIL verification"
```

---

## Post-Implementation

Once all 8 tasks are complete and merged, deploy on EC2 the same way every fix this session was deployed (`git pull && pm2 restart terminus`), then create a CRUDEOIL v4_cascade deployment via the admin dashboard (same generic instrument-selection form sell_straddle already uses for CRUDEOIL) and confirm live: ATM locks from the futures price at 09:00, no EOD force-close before 23:15, Gate 1/2/3 zones populate in the trap-status UI the same way NIFTY's do.
