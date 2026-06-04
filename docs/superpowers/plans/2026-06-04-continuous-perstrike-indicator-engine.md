# Continuous Per-Strike Indicator Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Maintain VWAP/SLOPE/RSI/ROC continuously per pool strike so any pair (entry/re-entry/roll) reads WARM indicators, eliminating the false `vwap_rise_sl` / `SLOPE=-146` / `RSI=1.69` mis-fires caused by the per-position series resetting on re-entry.

**Architecture:** A new pure `PoolIndicatorEngine` keeps a rolling per-strike 1-min `(ltp, atp)` series, seeded from prev-day historical for RSI/ROC. `pair_indicators(ce,pe)` reconstructs a pair's combined series on demand. The sell-straddle reads this engine instead of its own active-legs series; a subscription manager keeps the pool subscribed and pins the running legs.

**Tech Stack:** Python/asyncio, numpy, existing `matrix_engine.indicators` (rsi/ema/vwap), `strategies.straddle_selection.pair_indicators`, trap engine's `_upstox_candles` (curl_cffi), `data_layer.strike_rebalancer.pin_strike`.

---

## File Structure

- **Create** `strategies/pool_indicator_engine.py` — `PoolIndicatorEngine`: per-strike series, `pair_indicators`, `is_warm`. Pure (no EventBus/IO except an injected async historical fetcher).
- **Create** `tests/strategies/test_pool_indicator_engine.py` — unit tests.
- **Modify** `strategies/trap_trading_engine.py` — extract its `_upstox_candles` into a reusable helper (or expose it) for seeding. (Minimal: add a module-level `fetch_upstox_candles`.)
- **Create** `data_layer/historical_candles.py` — shared prev-day 1-min candle fetch + holiday step-back (moved out of trap so both use it).
- **Modify** `strategies/sell_straddle.py` — instantiate the engine, feed it ticks/candles, read `pair_indicators` for exit-eval + selection, remove `_prem_closes` reset-on-entry.
- **Modify** `data_layer/runtime_config.py` — add `pool_itm_depth` / `pool_otm_depth` defaults to the sell_straddle section.

---

## Task 1: PoolIndicatorEngine — per-strike series + pair indicators (no seed yet)

**Files:**
- Create: `strategies/pool_indicator_engine.py`
- Test: `tests/strategies/test_pool_indicator_engine.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/strategies/test_pool_indicator_engine.py
import numpy as np
from strategies.pool_indicator_engine import PoolIndicatorEngine

def _feed_bars(eng, strike, side, closes, atp=None):
    atp = atp if atp is not None else closes
    for c, a in zip(closes, atp):
        eng.update_tick(strike, side, ltp=c, atp=a)
        eng.commit_bar()

def test_pair_indicators_combined_close_and_vwap():
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    _feed_bars(eng, 100, "CE", [50, 51, 52], atp=[49, 50, 51])
    _feed_bars(eng, 100, "PE", [40, 41, 42], atp=[39, 40, 41])
    ind = eng.pair_indicators(100, 100)
    assert ind["close"] == 52 + 42          # latest combined ltp
    assert ind["vwap"]  == 51 + 41          # combined atp
    # slope = Δ combined vwap = (51+41) - (50+40)
    assert round(ind["slope"], 6) == round((51 + 41) - (50 + 40), 6)

def test_pair_rsi_roc_present_when_enough_bars():
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    closes = list(range(50, 70))            # 20 ascending bars
    _feed_bars(eng, 100, "CE", closes)
    _feed_bars(eng, 100, "PE", [10] * len(closes))  # flat PE
    ind = eng.pair_indicators(100, 100)
    assert "rsi" in ind and "roc" in ind
    assert ind["rsi"] > 50                  # ascending combined → RSI high
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_pool_indicator_engine.py -v`
Expected: FAIL with `ModuleNotFoundError: strategies.pool_indicator_engine`.

- [ ] **Step 3: Write minimal implementation**

```python
# strategies/pool_indicator_engine.py
"""Continuous per-strike indicator engine for the sell-straddle. Maintains a rolling 1-min
(ltp, atp) series per (strike, side) so any pair's combined VWAP/SLOPE/RSI/ROC can be computed
on demand — independent of the active position. No EventBus/IO here (pure + testable); the
strategy feeds it ticks/bars and an optional async seeder fills prev-day history (Task 2)."""
from __future__ import annotations

from collections import deque
from typing import Dict, Optional, Tuple

import numpy as np

from matrix_engine.indicators import rsi as _rsi

Key = Tuple[int, str]  # (strike, "CE"/"PE")


class PoolIndicatorEngine:
    def __init__(self, rsi_len: int = 14, roc_len: int = 10, maxlen: int = 240) -> None:
        self._rsi_len = rsi_len
        self._roc_len = roc_len
        self._maxlen = maxlen
        self._latest: Dict[Key, Tuple[float, float]] = {}          # (strike,side) -> (ltp, atp)
        self._closes: Dict[Key, deque] = {}                         # (strike,side) -> deque[ltp]
        self._atps:   Dict[Key, deque] = {}                         # (strike,side) -> deque[atp]

    def _key(self, strike: int, side: str) -> Key:
        return (int(strike), side.upper())

    def update_tick(self, strike: int, side: str, ltp: float, atp: float) -> None:
        self._latest[self._key(strike, side)] = (float(ltp), float(atp))

    def commit_bar(self) -> None:
        """Push the latest (ltp, atp) as a 1-min close for every tracked strike."""
        for k, (ltp, atp) in self._latest.items():
            self._closes.setdefault(k, deque(maxlen=self._maxlen)).append(ltp)
            self._atps.setdefault(k, deque(maxlen=self._maxlen)).append(atp)

    def is_warm(self, strike: int, side: str) -> bool:
        k = self._key(strike, side)
        return len(self._closes.get(k, ())) >= max(self._rsi_len + 1, self._roc_len + 1)

    def pair_indicators(self, ce_strike: int, pe_strike: int) -> Optional[Dict[str, float]]:
        ce, pe = self._key(ce_strike, "CE"), self._key(pe_strike, "PE")
        if ce not in self._latest or pe not in self._latest:
            return None
        ce_ltp, ce_atp = self._latest[ce]
        pe_ltp, pe_atp = self._latest[pe]
        if min(ce_ltp, pe_ltp, ce_atp, pe_atp) <= 0:
            return None
        ind: Dict[str, float] = {"close": ce_ltp + pe_ltp, "vwap": ce_atp + pe_atp}
        ca, pa = self._atps.get(ce), self._atps.get(pe)
        if ca and pa and len(ca) >= 2 and len(pa) >= 2:
            ind["slope"] = (ca[-1] + pa[-1]) - (ca[-2] + pa[-2])
        cc, pc = self._closes.get(ce), self._closes.get(pe)
        if cc and pc:
            n = min(len(cc), len(pc))
            combined = np.array([cc[-n + i] + pc[-n + i] for i in range(n)], dtype=np.float64)
            if n >= self._rsi_len + 1:
                ind["rsi"] = float(_rsi(combined))
            if n >= self._roc_len + 1 and combined[-self._roc_len - 1] != 0:
                ref = combined[-self._roc_len - 1]
                ind["roc"] = float((combined[-1] - ref) / ref * 100.0)
        return ind
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_pool_indicator_engine.py -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add strategies/pool_indicator_engine.py tests/strategies/test_pool_indicator_engine.py
git commit -m "feat(straddle): PoolIndicatorEngine — per-strike series + on-demand pair indicators"
```

---

## Task 2: Shared prev-day historical fetch (holiday step-back) + seed RSI/ROC

**Files:**
- Create: `data_layer/historical_candles.py`
- Modify: `strategies/pool_indicator_engine.py` (add `seed_strike`)
- Test: `tests/strategies/test_pool_indicator_engine.py` (add seed test)

- [ ] **Step 1: Write the failing test**

```python
# add to tests/strategies/test_pool_indicator_engine.py
def test_seed_prefills_series_for_rsi():
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    # seed 20 bars of (ltp, atp) for CE and PE directly
    eng.seed_strike(100, "CE", closes=list(range(50, 70)), atps=list(range(49, 69)))
    eng.seed_strike(100, "PE", closes=[10] * 20, atps=[10] * 20)
    # push one live tick so 'latest' exists
    eng.update_tick(100, "CE", 70, 69); eng.update_tick(100, "PE", 10, 10)
    assert eng.is_warm(100, "CE")
    ind = eng.pair_indicators(100, 100)
    assert "rsi" in ind and "roc" in ind
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_pool_indicator_engine.py::test_seed_prefills_series_for_rsi -v`
Expected: FAIL with `AttributeError: 'PoolIndicatorEngine' object has no attribute 'seed_strike'`.

- [ ] **Step 3: Write minimal implementation**

Add to `strategies/pool_indicator_engine.py`:

```python
    def seed_strike(self, strike: int, side: str, closes: list, atps: list) -> None:
        """Prefill the rolling series from historical bars (oldest-first) so RSI/ROC are valid
        immediately. VWAP/ATP are intraday-fresh so seeding atps is only to keep lengths aligned."""
        k = self._key(strike, side)
        cd = self._closes.setdefault(k, deque(maxlen=self._maxlen))
        ad = self._atps.setdefault(k, deque(maxlen=self._maxlen))
        for c, a in zip(closes, atps):
            cd.append(float(c)); ad.append(float(a))
```

Create `data_layer/historical_candles.py`:

```python
"""Shared prev-day 1-min historical candle fetch with holiday step-back. Used to seed RSI/ROC
warm-up (sell-straddle pool engine) and the trap engine. Uses curl_cffi Chrome impersonation
(Upstox edge 403s plain urllib)."""
from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta
from typing import List, Optional

logger = logging.getLogger(__name__)


async def fetch_upstox_1m(instrument_key: str, access_token: str, max_step_back: int = 7) -> List[dict]:
    """Return the most recent available day's 1-min candles (oldest-first) for an Upstox
    instrument_key, stepping back day-by-day over holidays/empties up to max_step_back days.
    Each candle dict: {'ts','open','high','low','close','volume'}. [] if none found."""
    def _get(d: date):
        from curl_cffi import requests as _cc
        url = (f"https://api.upstox.com/v2/historical-candle/{instrument_key}/1minute/"
               f"{d.isoformat()}/{d.isoformat()}")
        headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}
        try:
            r = _cc.get(url, headers=headers, impersonate="chrome131", timeout=8).json()
            rows = (r.get("data", {}) or {}).get("candles", []) or []
            # Upstox returns newest-first; normalise to oldest-first dicts.
            out = [{"ts": c[0], "open": c[1], "high": c[2], "low": c[3], "close": c[4],
                    "volume": c[5]} for c in reversed(rows)]
            return out
        except Exception as exc:
            logger.debug("fetch_upstox_1m %s %s: %s", instrument_key, d, exc)
            return []

    d = date.today() - timedelta(days=1)
    for _ in range(max_step_back):
        rows = await asyncio.to_thread(_get, d)
        if rows:
            return rows
        d -= timedelta(days=1)   # holiday/empty → step back
    return []
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_pool_indicator_engine.py -v`
Expected: PASS (all three tests). `historical_candles.py` is exercised in Task 4 integration, not unit-mocked here.

- [ ] **Step 5: Commit**

```bash
git add data_layer/historical_candles.py strategies/pool_indicator_engine.py tests/strategies/test_pool_indicator_engine.py
git commit -m "feat(straddle): seed_strike + shared prev-day 1m fetch with holiday step-back"
```

---

## Task 3: Pool subscription manager — pin running legs

**Files:**
- Modify: `strategies/sell_straddle.py` (add `_pool_strikes(atm)` + pin/unpin around open/close)
- Test: `tests/strategies/test_straddle_pool_subscription.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/strategies/test_straddle_pool_subscription.py
from strategies.sell_straddle import pool_strike_set

def test_pool_set_covers_itm_atm_otm():
    # ATM 100, step 5, itm_depth 2, otm_depth 3 → 100-2*5 .. 100+3*5
    s = pool_strike_set(atm=100, step=5, itm_depth=2, otm_depth=3)
    assert min(s) == 90 and max(s) == 115
    assert 100 in s and len(s) == (2 + 3 + 1)

def test_pool_set_keeps_running_legs():
    s = pool_strike_set(atm=100, step=5, itm_depth=1, otm_depth=1, pinned={80, 130})
    assert 80 in s and 130 in s        # running legs pinned even if outside range
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_straddle_pool_subscription.py -v`
Expected: FAIL with `ImportError: cannot import name 'pool_strike_set'`.

- [ ] **Step 3: Write minimal implementation**

Add a module-level helper to `strategies/sell_straddle.py` (near the other module helpers, e.g. after `_parse_time`):

```python
def pool_strike_set(atm: float, step: float, itm_depth: int, otm_depth: int,
                    pinned: set | None = None) -> set:
    """Strikes to keep subscribed: ATM-itm_depth*step .. ATM+otm_depth*step (inclusive),
    PLUS any pinned strikes (the running position's legs — never dropped even if out of range)."""
    atm_r = round(atm / step) * step
    out = {int(atm_r + i * step) for i in range(-itm_depth, otm_depth + 1)}
    if pinned:
        out |= {int(p) for p in pinned}
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_straddle_pool_subscription.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add strategies/sell_straddle.py tests/strategies/test_straddle_pool_subscription.py
git commit -m "feat(straddle): pool_strike_set — pool range + pinned running legs"
```

---

## Task 4: Wire the engine into sell_straddle (feed it, read it, stop the reset)

**Files:**
- Modify: `strategies/sell_straddle.py:251` (add `self._pool_engine = PoolIndicatorEngine(...)` in `__init__`)
- Modify: `strategies/sell_straddle.py` `_on_option_tick`/option loop (call `update_tick`)
- Modify: `strategies/sell_straddle.py` `_on_candle` (call `commit_bar` once per 1-min close)
- Modify: `strategies/sell_straddle.py:764` `_pair_indicators` (read pool engine; fall back to old `pair_indicators`)
- Modify: `strategies/sell_straddle.py:717` `_recompute_indicators` (use `pair_indicators(active_ce, active_pe)` from the engine for rsi/roc/slope; keep VWAP/ATP path)
- Modify: `strategies/sell_straddle.py:488` — REMOVE `self._prem_closes.clear()` on entry/roll (the reset that caused false exits)

- [ ] **Step 1: Write the failing test**

```python
# tests/strategies/test_straddle_engine_wiring.py
from strategies.sell_straddle import SellStraddleStrategy
from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus

def test_pair_indicators_use_pool_engine():
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    eng = ss._pool_engine
    # feed warm bars for a pair
    for c in range(50, 70):
        eng.update_tick(23450, "CE", c, c); eng.update_tick(23400, "PE", 10, 10)
        eng.commit_bar()
    eng.update_tick(23450, "CE", 70, 70); eng.update_tick(23400, "PE", 10, 10)
    ind = ss._pair_indicators(23450, 23400)
    assert ind is not None and "rsi" in ind and "roc" in ind   # warm from the engine
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_straddle_engine_wiring.py -v`
Expected: FAIL with `AttributeError: 'SellStraddleStrategy' object has no attribute '_pool_engine'`.

- [ ] **Step 3: Write minimal implementation**

In `strategies/sell_straddle.py` `__init__` (near `self._prem_closes` at line 251) add:

```python
        from strategies.pool_indicator_engine import PoolIndicatorEngine
        self._pool_engine = PoolIndicatorEngine(rsi_len=14, roc_len=10)
```

In `_pair_indicators` (line 764), prefer the engine:

```python
    def _pair_indicators(self, ce_strike: int, pe_strike: int) -> Optional[Dict[str, float]]:
        ind = self._pool_engine.pair_indicators(int(ce_strike), int(pe_strike))
        if ind is not None and "rsi" in ind:
            return ind
        # fall back to feed-only pair_indicators (pre-warm / missing strike)
        from strategies.straddle_selection import pair_indicators
        return pair_indicators(self._strike_prem, self._prev_atp_closed, ce_strike, pe_strike)
```

In the option-tick handler (where `self._strike_prem[(strike, side)]` is updated), also feed the engine:

```python
        self._pool_engine.update_tick(int(strike), opt_type, ltp=ltp, atp=atp)
```

In `_on_candle` (1-min branch), after building the active series, commit a pool bar once:

```python
        if getattr(ev, "timeframe", 1) == 1:
            self._pool_engine.commit_bar()
```

In `_recompute_indicators`, replace the active-only rsi/roc/slope with the engine's pair values when a position is open:

```python
        if self._position and self._position.status == "open":
            _pe = self._pool_engine.pair_indicators(
                int(self._position.ce_leg.strike), int(self._position.pe_leg.strike))
            if _pe:
                for k in ("rsi", "roc", "slope", "vwap", "close"):
                    if k in _pe:
                        self._ind[k] = _pe[k]
                return   # engine is the source of truth for the active pair
        # (fall through to the legacy ATP/closes path only when no warm engine data)
```

Remove the reset at line 488: delete `self._prem_closes.clear()` (and any sibling series clears on entry/roll) — the engine is continuous, no reset needed.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_straddle_engine_wiring.py tests/strategies/ -v`
Expected: PASS (new test + all existing 112+ still green).

- [ ] **Step 5: Commit**

```bash
git add strategies/sell_straddle.py tests/strategies/test_straddle_engine_wiring.py
git commit -m "feat(straddle): read warm pool-engine indicators for exits/selection; stop per-position series reset"
```

---

## Task 5: Seed-on-subscribe + pool config + integration

**Files:**
- Modify: `data_layer/runtime_config.py` (`_ss_index_default`: add `pool_itm_depth`, `pool_otm_depth`)
- Modify: `strategies/sell_straddle.py` `start()` — seed the pool engine from prev-day history for the initial pool strikes
- Test: `tests/strategies/test_straddle_pool_config.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/strategies/test_straddle_pool_config.py
from data_layer.runtime_config import RuntimeConfig

def test_pool_depths_present_in_default():
    ss = RuntimeConfig.index_section("NIFTY", "sell_straddle")
    assert "pool_itm_depth" in ss and "pool_otm_depth" in ss
    assert int(ss["pool_itm_depth"]) >= 0 and int(ss["pool_otm_depth"]) >= 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/strategies/test_straddle_pool_config.py -v`
Expected: FAIL with `KeyError`/assertion (`pool_itm_depth` missing).

- [ ] **Step 3: Write minimal implementation**

In `data_layer/runtime_config.py`, in the sell-straddle default dict (`_SS_INDEX_DEFAULT` / `_ss_index_default`), add:

```python
        "pool_itm_depth": 4,
        "pool_otm_depth": 4,
```

In `strategies/sell_straddle.py` `start()` (after restore, before the loops), add a background seed:

```python
        async def _seed_pool():
            try:
                from data_layer.historical_candles import fetch_upstox_1m
                from data_layer.instrument_registry import REGISTRY
                from data_layer.client_db import ClientDB
                creds = await asyncio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
                token = (creds or {}).get("access_token", "")
                if not token or self._spot <= 0:
                    return
                step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0)
                ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
                itm = int(ss.get("pool_itm_depth", 4)); otm = int(ss.get("pool_otm_depth", 4))
                strikes = pool_strike_set(self._spot, step, itm, otm)
                exp = REGISTRY.get_active_expiry(self._underlying, datetime.now(IST).date())
                for stk in strikes:
                    for side in ("CE", "PE"):
                        ikey = REGISTRY.get_broker_symbol(self._underlying, exp, int(stk), side, "upstox")
                        if not ikey:
                            continue
                        bars = await fetch_upstox_1m(ikey, token)
                        if bars:
                            closes = [b["close"] for b in bars]
                            self._pool_engine.seed_strike(int(stk), side, closes, closes)
                logger.info("SellStraddle[%s]: pool engine seeded %d strikes (prev-day RSI/ROC warm).",
                            self._underlying, len(strikes))
            except Exception as exc:
                logger.warning("SellStraddle[%s]: pool seed failed: %s", self._underlying, exc)
        asyncio.create_task(_seed_pool())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/strategies/test_straddle_pool_config.py tests/ -q`
Expected: PASS (new test + full suite green).

- [ ] **Step 5: Commit**

```bash
git add data_layer/runtime_config.py strategies/sell_straddle.py tests/strategies/test_straddle_pool_config.py
git commit -m "feat(straddle): pool_itm/otm_depth config + prev-day seed of pool engine on start"
```

---

## Task 6: Refactor trap to use the shared fetcher (DRY)

**Files:**
- Modify: `strategies/trap_trading_engine.py` (`_upstox_candles` delegates to `data_layer.historical_candles.fetch_upstox_1m`)

- [ ] **Step 1:** Run the existing trap tests to capture the baseline: `python -m pytest tests/strategies/test_trap_v2_* -q` → note PASS count.
- [ ] **Step 2:** Replace the body of `_upstox_candles` to call `fetch_upstox_1m` (keep its signature; adapt the return shape if the trap expects newest-first — wrap with `list(reversed(...))` if needed).
- [ ] **Step 3:** Run `python -m pytest tests/strategies/ -q` → expect the same PASS count (no regressions).
- [ ] **Step 4: Commit**

```bash
git add strategies/trap_trading_engine.py
git commit -m "refactor(trap): use shared data_layer.historical_candles fetcher (DRY)"
```

---

## Self-Review

**Spec coverage:**
- Per-strike 1-min series → Task 1 ✓
- pair_indicators on-demand (combined close/vwap/slope/rsi/roc) → Task 1 ✓
- Prev-day seed for RSI/ROC + holiday step-back → Task 2 + Task 5 ✓
- VWAP intraday (no seed) → seed only fills lengths; live atp drives vwap → Task 1/4 ✓
- pool_itm_depth/otm_depth config → Task 5 ✓
- Pin running legs, never unsubscribe → Task 3 (`pool_strike_set` with `pinned`); wiring the actual sub/unsub uses the existing rebalancer `pin_strike` — **NOTE for executor:** call `self._rebalancer.pin_strike` on open and `unpin_strike` on full close (the rebalancer already honours pins). ✓
- Full exit → cooldown / roll → immediate → already implemented (cooldown on `_close_position`, rolls keep running leg); the engine just removes the false-exit cause. ✓
- DRY trap fetcher → Task 6 ✓

**Placeholder scan:** none — every step has concrete code/commands.

**Type consistency:** `PoolIndicatorEngine.update_tick/commit_bar/seed_strike/pair_indicators/is_warm`, `pool_strike_set`, `fetch_upstox_1m` used consistently across tasks.

**Gap noted for executor:** Task 3 defines `pool_strike_set` but the live sub/unsub loop (call it on ATM change, diff against current subs, pin running legs) is wired via the existing `StrikeRebalancer`; if the rebalancer's pool range must widen to `pool_itm/otm_depth`, extend its range calc to read those config keys (small follow-up, same pattern as `chain_depth`).
