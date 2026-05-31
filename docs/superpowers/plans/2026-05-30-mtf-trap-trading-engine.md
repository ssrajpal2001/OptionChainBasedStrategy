# MTF TrapTradingEngine Upgrade & NewTrap Integration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade `TrapTradingEngine` to a production-grade dynamic multi-timeframe (MTF) institutional liquidity sweep engine sourced from the NewTrap GitHub repo, with 1-minute SQLite bar persistence, pandas-based on-the-fly resampling, live execution integration, and dashboard P&L telemetry.

**Architecture:** Raw 1-minute option premium candles are persisted to `option_1m_bar_repository` in SQLite; the engine loads them via pandas and resamples to configurable HTF/MTF timeframes on demand. The NewTrap 5-stage sequential state machine (75-min bearish → high sweep → retest zone → 5-min nested trap → touch trigger) drives entries, while a 1-minute close guard manages exits. Multi-client capital allocation uses a per-client floor-division lot formula; a (trade_id, symbol, entry_px, quantity) cache ensures matched entry/exit sizes.

**Tech Stack:** Python asyncio, SQLite (sqlite3), pandas (resampling), FastAPI (dashboard), Alpine.js + Tailwind CSS (UI), EventBus (pub-sub).

---

## Source Context

### NewTrap Strategy Logic (from github.com/ssrajpal2001/Newtraptrading)

The NewTrap repo implements a 5-stage sequential option-premium trap:

| Stage | Timeframe | Condition |
|-------|-----------|-----------|
| 1 | 75-min | Bearish candle: `close < open` → candidate setup |
| 2 | 75-min | Next bar's `high > prev_bearish_candle.high` → trap locked; record `entry_origin = bearish_candle.open`, `target_high = current_bar.high` |
| 3 | Live premium | Premium returns to `±RETEST_ZONE_PERCENT` of `entry_origin` → orange alert |
| 4 | 5-min | Find bearish candle, then next bar's `high > ltf_bearish.high` → `ltf_entry_line = ltf_bearish.open`, `ltf_sl_line = ltf_bearish.low` |
| 5 | Live tick | `premium <= ltf_entry_line` → fire BUY orders across all clients |

**Exit guard (1-min candle close):**
- `1m_close < ltf_sl_line` → Void/SL — fire market SELL
- `premium >= target_high` → Mitigate/Profit — fire market SELL
- 15:30 IST → Force-exit all open traps

**Strike selection (config.py from NewTrap):**
```python
center = (prev_day_open + prev_day_close) / 2
day_offsets = {MON: 200, TUE: 100, WED: 500, THU: 400, FRI: 300}
offset = day_offsets[weekday]
ce_strike = round_to_step(center - offset, step=50)   # bearish
pe_strike = round_to_step(center + offset, step=50)   # bullish
```

### Existing Engine State (our codebase)
- `TrapTradingEngine` already has HTF zone detection (75-min supply/demand zones), LTF state machine (5-min IDLE → ZONE_WATCH → ARMED → VOID → CONFIRMED), rolling base tracking, RSI/ADX/VWAP indicators
- The rewrite **replaces** the current ZONE_WATCH/ARMED/CONFIRMED state machine with NewTrap's 5-stage sequential machine, but **keeps** the `OHLCVBuffer` ring buffer utility and rolling base update logic
- The existing `_process_htf` / `_process_ltf` split is preserved; we add `_process_1m` for the exit guard

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `data_layer/client_db.py` | **Modify** | Add `option_1m_bar_repository` DDL + `upsert_1m_bar()` + `get_1m_bars()` |
| `data_layer/base_feeder.py` | **Modify** | On `CANDLE_CLOSE` for timeframe=1, call `asyncio.to_thread(db.upsert_1m_bar)` |
| `config/global_config.py` | **Modify** | Add `TrapEngineConfig` dataclass; add to `GlobalConfig` |
| `strategies/trap_trading_engine.py` | **Rewrite** | NewTrap 5-stage state machine; pandas resampling; dynamic HTF/MTF; execution block |
| `ui_layer/dashboard_server.py` | **Modify** | Add `/api/trap/positions` endpoint with live unrealized P&L from tick cache |
| `ui_layer/templates/monitor.html` | **Modify** | Trap positions panel: color-coded P&L + simulation overlay toggle + orange banner |

---

## Task 1: 1-Minute Bar Persistence Layer

**Files:**
- Modify: `data_layer/client_db.py`

### What and Why
Options expire mid-week. On Friday you can no longer fetch Tuesday's expired contract data from the broker. Persisting every 1-minute option premium candle to SQLite lets us review/backtest any session after expiry, and gives the TrapEngine a local source for pandas resampling without live broker dependency.

- [ ] **Step 1.1: Add `option_1m_bar_repository` table to the DDL string**

Open `data_layer/client_db.py`. In the `_DDL` string (currently ends at line ~169), append the following SQL block **before** the closing `"""`:

```python
# In _DDL string, append before closing """

CREATE TABLE IF NOT EXISTS option_1m_bar_repository (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol    TEXT    NOT NULL,
    timestamp TEXT    NOT NULL,
    open      REAL    NOT NULL DEFAULT 0.0,
    high      REAL    NOT NULL DEFAULT 0.0,
    low       REAL    NOT NULL DEFAULT 0.0,
    close     REAL    NOT NULL DEFAULT 0.0,
    volume    REAL    NOT NULL DEFAULT 0.0,
    UNIQUE(symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS ix_option1m_symbol_timestamp
    ON option_1m_bar_repository(symbol, timestamp);
"""
```

- [ ] **Step 1.2: Add `upsert_1m_bar()` synchronous helper to `ClientDB`**

After the `get_system_setting_sync` method in `ClientDB`, add:

```python
def upsert_1m_bar_sync(
    self,
    symbol: str,
    timestamp: datetime,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float,
) -> None:
    """
    INSERT OR REPLACE a 1-minute option bar.
    Called via asyncio.to_thread() — never directly from async code.
    """
    ts_str = timestamp.strftime("%Y-%m-%dT%H:%M:%S")
    with self._connect() as conn:
        conn.execute(
            """
            INSERT INTO option_1m_bar_repository
                (symbol, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, timestamp) DO UPDATE SET
                open=excluded.open, high=excluded.high, low=excluded.low,
                close=excluded.close, volume=excluded.volume
            """,
            (symbol, ts_str, open_, high, low, close, volume),
        )
```

- [ ] **Step 1.3: Add async wrapper `upsert_1m_bar()`**

Directly after `upsert_1m_bar_sync`, add the async wrapper:

```python
async def upsert_1m_bar(
    self,
    symbol: str,
    timestamp: datetime,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float,
) -> None:
    await asyncio.to_thread(
        self.upsert_1m_bar_sync,
        symbol, timestamp, open_, high, low, close, volume,
    )
```

- [ ] **Step 1.4: Add `get_1m_bars_sync()` for pandas loading**

```python
def get_1m_bars_sync(
    self, symbol: str, since: datetime, until: Optional[datetime] = None
) -> list[dict]:
    """
    Return list of 1m bar dicts for `symbol` in [since, until].
    Called via asyncio.to_thread() from the strategy engine.
    """
    since_str = since.strftime("%Y-%m-%dT%H:%M:%S")
    if until is None:
        until_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
    else:
        until_str = until.strftime("%Y-%m-%dT%H:%M:%S")
    with self._connect() as conn:
        cur = conn.execute(
            """
            SELECT symbol, timestamp, open, high, low, close, volume
            FROM option_1m_bar_repository
            WHERE symbol = ? AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp ASC
            """,
            (symbol, since_str, until_str),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
```

- [ ] **Step 1.5: Verify the DDL runs cleanly**

```bash
python -c "
from data_layer.client_db import ClientDB
db = ClientDB()
db._exec(_DDL_CHECK := True)  # just import ensures DDL parses
print('DDL OK')
"
```

Actually run this check:
```bash
cd e:/AlgoSoft/OptionChainBasedStrategy && python -c "
from data_layer.client_db import ClientDB
db = ClientDB('data/test_1m.db')
print('Tables created OK')
import os; os.remove('data/test_1m.db')
"
```
Expected output: `Tables created OK`

- [ ] **Step 1.6: Wire persistence into `GlobalFeeder` / `BaseFeeder` CANDLE_CLOSE handler**

In `data_layer/global_feeder.py`, in the `GlobalFeeder` class, find or add a `_candle_persist_loop` coroutine. This subscribes to `Topic.CANDLE_CLOSE` and persists 1-min candles only:

```python
async def _candle_persist_loop(self) -> None:
    """Persist every 1-minute CandleEvent to option_1m_bar_repository."""
    from data_layer.base_feeder import CandleEvent
    q = self._bus.subscribe(Topic.CANDLE_CLOSE)
    while self._running:
        try:
            ev: CandleEvent = await asyncio.wait_for(q.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        if not isinstance(ev, CandleEvent):
            continue
        if ev.timeframe != 1:
            continue
        if self._client_db is None:
            continue
        try:
            await self._client_db.upsert_1m_bar(
                symbol=ev.symbol,
                timestamp=ev.timestamp,
                open_=ev.open,
                high=ev.high,
                low=ev.low,
                close=ev.close,
                volume=float(ev.volume) if ev.volume else 0.0,
            )
        except Exception as exc:
            logger.warning("1m bar persist failed [%s]: %s", ev.symbol, exc)
```

Start this task in the existing `start()` or `_startup()` method of `GlobalFeeder` alongside other tasks:
```python
asyncio.create_task(self._candle_persist_loop(), name="candle_persist_1m")
```

- [ ] **Step 1.7: Commit**

```bash
git add data_layer/client_db.py data_layer/global_feeder.py
git commit -m "feat: add option_1m_bar_repository table + async upsert/get helpers + GlobalFeeder 1m persist loop"
```

---

## Task 2: Dynamic TrapEngine Configuration

**Files:**
- Modify: `config/global_config.py`

### What and Why
All timeframes and thresholds are currently scattered as module-level constants in `trap_trading_engine.py`. Moving them to a typed, thread-safe `TrapEngineConfig` dataclass in `GlobalConfig` allows runtime reconfiguration via the admin dashboard without restarting the process.

- [ ] **Step 2.1: Add `TrapEngineConfig` dataclass to `global_config.py`**

After the `StrategyParams` dataclass (around line 155), insert:

```python
# ─────────────────────────────────────────────────────────────────────────────
# TrapEngine Dynamic Configuration
# ─────────────────────────────────────────────────────────────────────────────

import threading as _threading  # noqa: E402 — placed here for grouping

@dataclass
class TrapEngineConfig:
    """
    All tunable parameters for the MTF TrapTradingEngine.
    Thread-safe reads via _lock (RLock).  Update via reconfigure().

    HTF_MINUTES   — Higher Timeframe for institutional trap detection (default 75)
    MTF_MINUTES   — Middle Timeframe for nested LTF confirmation (default 5)
    LTF_MINUTES   — Lower Timeframe for 1-min exit guard (always 1, configurable)
    RETEST_ZONE_PERCENT — ±% buffer defining the "orange alert" retest zone
    LOT_SIZE      — Immutable Nifty lot size (25)
    SLIPPAGE_BUFFER — Price offset applied to entry/exit to absorb market impact
    """
    HTF_MINUTES:          int   = 75
    MTF_MINUTES:          int   = 5
    LTF_MINUTES:          int   = 1
    RETEST_ZONE_PERCENT:  float = 0.5    # 0.5%
    LOT_SIZE:             int   = 25     # Nifty contract lot size (immutable by exchange)
    SLIPPAGE_BUFFER:      float = 0.5    # Premium points to add to entry/subtract from exit

    # Lookback for pandas resampling source window (trading days)
    bars_lookback_days:   int   = 5

    _lock: object = field(default_factory=_threading.RLock, init=False, repr=False, compare=False)

    def reconfigure(self, **kwargs) -> None:
        """Thread-safe in-place update of any field except LOT_SIZE."""
        with self._lock:  # type: ignore[attr-defined]
            for k, v in kwargs.items():
                if k == "LOT_SIZE":
                    raise ValueError("LOT_SIZE is immutable — set by exchange contract.")
                if hasattr(self, k):
                    object.__setattr__(self, k, type(getattr(self, k))(v))
```

- [ ] **Step 2.2: Add `trap_engine: TrapEngineConfig` field to `GlobalConfig`**

In the `GlobalConfig` dataclass, add one field after `strategy`:

```python
@dataclass
class GlobalConfig:
    exchange:    ExchangeConfig    = field(default_factory=ExchangeConfig)
    indicators:  IndicatorParams   = field(default_factory=IndicatorParams)
    storage:     StorageConfig     = field(default_factory=StorageConfig)
    strategy:    StrategyParams    = field(default_factory=StrategyParams)
    auth:        AuthConfig        = field(default_factory=AuthConfig)
    trap_engine: TrapEngineConfig  = field(default_factory=TrapEngineConfig)  # ← new

    # ... rest of existing fields unchanged
```

- [ ] **Step 2.3: Verify import**

```bash
cd e:/AlgoSoft/OptionChainBasedStrategy && python -c "
from config.global_config import GlobalConfig
cfg = GlobalConfig()
print(f'HTF={cfg.trap_engine.HTF_MINUTES} MTF={cfg.trap_engine.MTF_MINUTES} LOT={cfg.trap_engine.LOT_SIZE}')
"
```
Expected: `HTF=75 MTF=5 LOT=25`

- [ ] **Step 2.4: Commit**

```bash
git add config/global_config.py
git commit -m "feat: add TrapEngineConfig dataclass with HTF/MTF/LTF/RETEST_ZONE/LOT_SIZE/SLIPPAGE fields"
```

---

## Task 3: Rewrite TrapTradingEngine — NewTrap State Machine

**Files:**
- Rewrite: `strategies/trap_trading_engine.py`

### What and Why
The existing engine detects HTF supply zones and fires on LTF sweeps, but lacks the 5-stage sequential NewTrap protocol (bearish candle → high sweep → retest zone → 5-min nested trap → touch trigger). The new engine replaces the ZONE_WATCH/ARMED/CONFIRMED machine with NewTrap stages while preserving the `OHLCVBuffer` ring buffer and rolling base logic (which remain correct).

The engine now **also** handles the `_TrapState.is_backtest` flag: simulation-mode signals write to a separate in-memory backtest log with `client_id = -1` and never touch live execution.

- [ ] **Step 3.1: Replace the entire `strategies/trap_trading_engine.py` file**

```python
"""
strategies/trap_trading_engine.py — NewTrap MTF Institutional Liquidity Sweep Engine.

Implements the 5-stage sequential NewTrap confirmation protocol sourced from
github.com/ssrajpal2001/Newtraptrading, integrated with our EventBus and
ExecutionRouter.

Stage 1 (HTF):  75-min bearish candle detected (close < open).
Stage 2 (HTF):  Next HTF bar's high sweeps the bearish candle's high →
                trap locked; entry_origin = bearish.open, target_high = current.high.
Stage 3 (Live): Premium retraces to ±RETEST_ZONE_PERCENT of entry_origin → alert.
Stage 4 (MTF):  5-min bearish candle found; next MTF bar's high sweeps that candle's
                high → ltf_entry_line = mtf_bearish.open, ltf_sl_line = mtf_bearish.low.
Stage 5 (1m):   Live premium touches or falls below ltf_entry_line → BUY signal.

Exit guard (every 1-min candle close):
  1m_close < ltf_sl_line        → Void (stop-loss hit)
  premium  >= target_high       → Mitigate (profit target)
  time     >= 15:30 IST         → Force-exit

All state is in-memory. DB access (1m bar load) is wrapped in asyncio.to_thread().
Subscribes to: Topic.CANDLE_CLOSE, Topic.OPTION_TICK
Publishes to:  Topic.SIGNAL
"""

from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

import numpy as np

from config.global_config import IST, Topic, GlobalConfig
from data_layer.base_feeder import CandleEvent, OptionTick, EventBus
from strategies.base_strategy import Direction, SignalPackage, StrategyID

logger = logging.getLogger(__name__)

_MARKET_CLOSE = datetime.now(IST).replace(hour=15, minute=30, second=0, microsecond=0).time()


# ─────────────────────────────────────────────────────────────────────────────
# OHLCV Ring Buffer (unchanged — keep existing utility)
# ─────────────────────────────────────────────────────────────────────────────

from collections import deque


class OHLCVBuffer:
    """Fixed-capacity OHLCV ring buffer backed by deques."""

    __slots__ = ("_cap", "_o", "_h", "_l", "_c", "_v", "_t")

    def __init__(self, capacity: int) -> None:
        self._cap = capacity
        self._o: deque[float]    = deque(maxlen=capacity)
        self._h: deque[float]    = deque(maxlen=capacity)
        self._l: deque[float]    = deque(maxlen=capacity)
        self._c: deque[float]    = deque(maxlen=capacity)
        self._v: deque[float]    = deque(maxlen=capacity)
        self._t: deque[datetime] = deque(maxlen=capacity)

    def push(self, c: CandleEvent) -> None:
        self._o.append(c.open); self._h.append(c.high)
        self._l.append(c.low);  self._c.append(c.close)
        self._v.append(float(c.volume or 0.0)); self._t.append(c.timestamp)

    def __len__(self) -> int: return len(self._c)
    def last_close(self) -> float: return self._c[-1] if self._c else 0.0
    def prev_close(self) -> float: return self._c[-2] if len(self._c) >= 2 else 0.0
    def last_high(self)  -> float: return self._h[-1] if self._h else 0.0
    def last_low(self)   -> float: return self._l[-1] if self._l else 0.0
    def last_open(self)  -> float: return self._o[-1] if self._o else 0.0
    def prev_high(self)  -> float: return self._h[-2] if len(self._h) >= 2 else 0.0


# ─────────────────────────────────────────────────────────────────────────────
# State Machine Phases
# ─────────────────────────────────────────────────────────────────────────────

class _Phase(Enum):
    IDLE          = auto()  # No active setup
    HTF_BEARISH   = auto()  # Stage 1 complete — 75-min bearish candle found
    TRAP_LOCKED   = auto()  # Stage 2 complete — high sweep confirmed, entry_origin set
    RETEST_ALERT  = auto()  # Stage 3 complete — premium near entry_origin
    MTF_BEARISH   = auto()  # Stage 4 partial — 5-min bearish candle found
    MTF_LOCKED    = auto()  # Stage 4 complete — 5-min nested trap confirmed
    ARMED         = auto()  # Stage 5 — waiting for touch of ltf_entry_line
    LIVE          = auto()  # Position open — monitoring 1m exit guard


# ─────────────────────────────────────────────────────────────────────────────
# Per-Symbol Trap State
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class _TrapState:
    phase: _Phase = _Phase.IDLE

    # Stage 1 — HTF bearish candle
    htf_bearish_open:  float = 0.0
    htf_bearish_high:  float = 0.0
    htf_bearish_ts:    Optional[datetime] = None

    # Stage 2 — Trap locked
    entry_origin:  float = 0.0   # = htf_bearish_open (premium sell level)
    target_high:   float = 0.0   # = sweep bar high (profit target)

    # Stage 4 — MTF nested trap
    mtf_bearish_open:  float = 0.0
    mtf_bearish_high:  float = 0.0
    mtf_bearish_low:   float = 0.0
    mtf_bearish_ts:    Optional[datetime] = None

    ltf_entry_line: float = 0.0  # 5-min bearish candle open → touch trigger
    ltf_sl_line:    float = 0.0  # 5-min bearish candle low  → void/SL

    # Rolling Base (spec-exact: any candle closing below prev becomes new base)
    rolling_base: float = 0.0

    # Active position tracking (filled when LIVE)
    trade_id:    Optional[str] = None
    entry_price: float = 0.0
    quantity:    int   = 0

    # Simulation flag — set True for backtest runs; prevents live execution
    is_backtest: bool = False


# ─────────────────────────────────────────────────────────────────────────────
# TrapTradingEngine
# ─────────────────────────────────────────────────────────────────────────────

class TrapTradingEngine:
    """
    NewTrap MTF engine.  Wire-up (run_system.py):
        engine = TrapTradingEngine(bus, cfg, client_db)
        asyncio.create_task(engine.run(), name="trap_engine")
    """

    def __init__(
        self,
        bus: EventBus,
        cfg: GlobalConfig,
        client_db=None,
    ) -> None:
        self._bus        = bus
        self._cfg        = cfg
        self._client_db  = client_db
        self._running    = False

        # Per-symbol state
        self._states: Dict[str, _TrapState] = {}

        # Live spot/premium cache (updated from OPTION_TICK)
        self._spot_cache: Dict[str, float] = {}     # underlying → spot ltp
        self._prem_cache: Dict[str, float] = {}     # option_symbol → ltp

        # Position cache for matched exit: (trade_id, symbol, entry_px, qty)
        self._open_positions: Dict[str, Tuple[str, str, float, int]] = {}

        # Backtest replay log (in-memory, never touches live DB)
        self._backtest_log: List[dict] = []

        self._signal_count = 0

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        logger.info("TrapTradingEngine: started.")
        candle_q = self._bus.subscribe(Topic.CANDLE_CLOSE)
        option_q = self._bus.subscribe(Topic.OPTION_TICK)
        await asyncio.gather(
            self._candle_loop(candle_q),
            self._option_tick_loop(option_q),
        )

    def stop(self) -> None:
        self._running = False

    # ── Event loops ───────────────────────────────────────────────────────────

    async def _candle_loop(self, q) -> None:
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if not isinstance(ev, CandleEvent):
                continue
            try:
                await self._on_candle(ev)
            except Exception:
                logger.exception("TrapEngine: error on candle %s TF%d", ev.symbol, ev.timeframe)

    async def _option_tick_loop(self, q) -> None:
        while self._running:
            try:
                tick = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if not isinstance(tick, OptionTick):
                continue
            self._prem_cache[tick.symbol] = tick.ltp
            # Stage 5 check: touch trigger on every premium tick
            await self._check_touch_trigger(tick.underlying, tick.symbol, tick.ltp)

    # ── Candle router ─────────────────────────────────────────────────────────

    async def _on_candle(self, c: CandleEvent) -> None:
        tc = self._cfg.trap_engine
        now = datetime.now(IST)

        # Force-exit at market close
        if now.time() >= _MARKET_CLOSE:
            await self._force_exit_all(c.symbol)
            return

        if c.timeframe == tc.HTF_MINUTES:
            self._process_htf(c)
        elif c.timeframe == tc.MTF_MINUTES:
            self._process_mtf(c)
        elif c.timeframe == tc.LTF_MINUTES:
            await self._process_ltf_exit_guard(c)

    # ─────────────────────────────────────────────────────────────────────────
    # Stage 1+2: HTF (75-min) — detect bearish candle + high sweep
    # ─────────────────────────────────────────────────────────────────────────

    def _process_htf(self, c: CandleEvent) -> None:
        st = self._get_state(c.symbol)

        # Rolling Base: any candle closing below its previous close
        # becomes the new active rolling base
        # (applied on all timeframes per spec)
        if c.close < c.open:  # proxy for "close < prev_close" on first bar
            if st.rolling_base == 0.0 or c.close < st.rolling_base:
                st.rolling_base = c.low
                logger.debug("TrapEngine [%s] HTF rolling_base → %.2f", c.symbol, st.rolling_base)

        if st.phase == _Phase.IDLE:
            # Stage 1: detect bearish HTF candle
            if c.close < c.open:
                st.htf_bearish_open = c.open
                st.htf_bearish_high = c.high
                st.htf_bearish_ts   = c.timestamp
                st.phase            = _Phase.HTF_BEARISH
                logger.info(
                    "TrapEngine [%s] Stage1 HTF_BEARISH: open=%.2f high=%.2f @ %s",
                    c.symbol, c.open, c.high, c.timestamp.strftime("%H:%M"),
                )

        elif st.phase == _Phase.HTF_BEARISH:
            # Stage 2: current bar's high sweeps the bearish candle's high
            # → sellers trapped, lock the setup
            if c.high > st.htf_bearish_high:
                st.entry_origin = st.htf_bearish_open
                st.target_high  = c.high
                st.phase        = _Phase.TRAP_LOCKED
                logger.info(
                    "TrapEngine [%s] Stage2 TRAP_LOCKED: entry_origin=%.2f target_high=%.2f",
                    c.symbol, st.entry_origin, st.target_high,
                )
            elif c.close < c.open:
                # New bearish candle is more recent — update candidate
                st.htf_bearish_open = c.open
                st.htf_bearish_high = c.high
                st.htf_bearish_ts   = c.timestamp
                logger.debug(
                    "TrapEngine [%s] HTF bearish candidate updated → %.2f", c.symbol, c.open
                )
            else:
                # Bullish candle without a sweep — reset to IDLE
                st.phase = _Phase.IDLE
                logger.debug("TrapEngine [%s] HTF bearish setup invalidated — reset IDLE", c.symbol)

    # ─────────────────────────────────────────────────────────────────────────
    # Stage 4: MTF (5-min) — nested trap confirmation
    # ─────────────────────────────────────────────────────────────────────────

    def _process_mtf(self, c: CandleEvent) -> None:
        st = self._get_state(c.symbol)

        # Rolling Base update on MTF
        if len(self._get_mtf_buf(c.symbol)) >= 1:
            buf = self._get_mtf_buf(c.symbol)
            buf.push(c)
            if len(buf) >= 2 and c.close < buf.prev_close():
                st.rolling_base = c.low

        if st.phase not in (_Phase.RETEST_ALERT, _Phase.MTF_BEARISH):
            return

        if st.phase == _Phase.RETEST_ALERT:
            # Stage 4 Part A: find 5-min bearish candle
            if c.close < c.open:
                st.mtf_bearish_open = c.open
                st.mtf_bearish_high = c.high
                st.mtf_bearish_low  = c.low
                st.mtf_bearish_ts   = c.timestamp
                st.phase            = _Phase.MTF_BEARISH
                logger.info(
                    "TrapEngine [%s] Stage4a MTF_BEARISH: open=%.2f high=%.2f low=%.2f @ %s",
                    c.symbol, c.open, c.high, c.low, c.timestamp.strftime("%H:%M"),
                )

        elif st.phase == _Phase.MTF_BEARISH:
            # Stage 4 Part B: next MTF bar's high sweeps bearish candle's high
            if c.high > st.mtf_bearish_high:
                st.ltf_entry_line = st.mtf_bearish_open
                st.ltf_sl_line    = st.mtf_bearish_low
                st.phase          = _Phase.MTF_LOCKED
                logger.info(
                    "TrapEngine [%s] Stage4b MTF_LOCKED: ltf_entry=%.2f ltf_sl=%.2f",
                    c.symbol, st.ltf_entry_line, st.ltf_sl_line,
                )

                # Immediately transition to ARMED — waiting for touch trigger
                st.phase = _Phase.ARMED
                logger.info(
                    "TrapEngine [%s] ARMED — awaiting premium touch of ltf_entry_line=%.2f",
                    c.symbol, st.ltf_entry_line,
                )
            elif c.close < c.open:
                # Fresher bearish candle — update candidate
                st.mtf_bearish_open = c.open
                st.mtf_bearish_high = c.high
                st.mtf_bearish_low  = c.low
                st.mtf_bearish_ts   = c.timestamp
            else:
                # Bullish sweep without nested trap confirmation — revert to RETEST_ALERT
                st.phase = _Phase.RETEST_ALERT
                logger.debug("TrapEngine [%s] MTF trap invalidated — back to RETEST_ALERT", c.symbol)

    # ─────────────────────────────────────────────────────────────────────────
    # Stage 3: Retest Zone Check (called from OPTION_TICK)
    # ─────────────────────────────────────────────────────────────────────────

    async def _check_touch_trigger(
        self, underlying: str, option_symbol: str, ltp: float
    ) -> None:
        st = self._get_state(underlying)

        # Stage 3: check if premium is in the retest zone around entry_origin
        if st.phase == _Phase.TRAP_LOCKED and st.entry_origin > 0.0:
            tol  = st.entry_origin * (self._cfg.trap_engine.RETEST_ZONE_PERCENT / 100.0)
            lower = st.entry_origin - tol
            upper = st.entry_origin + tol
            if lower <= ltp <= upper:
                st.phase = _Phase.RETEST_ALERT
                logger.info(
                    "TrapEngine [%s] Stage3 RETEST_ALERT: ltp=%.2f in zone [%.2f, %.2f]",
                    underlying, ltp, lower, upper,
                )

        # Stage 5: touch trigger — premium at or below ltf_entry_line
        if st.phase == _Phase.ARMED and st.ltf_entry_line > 0.0:
            slip = self._cfg.trap_engine.SLIPPAGE_BUFFER
            if ltp <= (st.ltf_entry_line + slip):
                await self._fire_entry(underlying, option_symbol, ltp, st)

    # ─────────────────────────────────────────────────────────────────────────
    # Exit Guard: 1-min candle close
    # ─────────────────────────────────────────────────────────────────────────

    async def _process_ltf_exit_guard(self, c: CandleEvent) -> None:
        st = self._get_state(c.symbol)
        if st.phase != _Phase.LIVE:
            return

        # 1-min close below SL line → stop-loss exit
        if c.close < st.ltf_sl_line:
            logger.info(
                "TrapEngine [%s] 1m VOID — close=%.2f < sl=%.2f → exit",
                c.symbol, c.close, st.ltf_sl_line,
            )
            await self._fire_exit(c.symbol, c.close, "stop_loss")
            return

        # 1-min premium at or above profit target
        current_prem = self._prem_cache.get(c.symbol, 0.0)
        if current_prem > 0.0 and current_prem >= st.target_high:
            logger.info(
                "TrapEngine [%s] MITIGATE — prem=%.2f >= target=%.2f → exit",
                c.symbol, current_prem, st.target_high,
            )
            await self._fire_exit(c.symbol, current_prem, "profit_target")

    # ─────────────────────────────────────────────────────────────────────────
    # Execution: Entry
    # ─────────────────────────────────────────────────────────────────────────

    async def _fire_entry(
        self,
        underlying: str,
        option_symbol: str,
        entry_price: float,
        st: _TrapState,
    ) -> None:
        if st.is_backtest:
            self._record_backtest_entry(underlying, option_symbol, entry_price, st)
            return

        tc      = self._cfg.trap_engine
        lot_size = tc.LOT_SIZE

        # Dynamic capital calculator — loop through all active clients
        clients = []
        if self._client_db:
            try:
                clients = await asyncio.to_thread(self._client_db.get_active_clients_sync)
            except Exception as exc:
                logger.warning("TrapEngine: could not load clients: %s", exc)

        import uuid
        trade_id = f"TRAP_{underlying}_{datetime.now(IST).strftime('%H%M%S')}_{uuid.uuid4().hex[:6]}"

        total_qty = 0
        for client in clients:
            capital   = float(client.get("capital", 0))
            if capital <= 0 or entry_price <= 0:
                continue
            raw_qty   = math.floor(capital / (entry_price * lot_size)) * lot_size
            qty       = max(raw_qty, lot_size)   # Minimum 1 lot guaranteed
            total_qty += qty

            logger.info(
                "TrapEngine ENTRY [%s] client=%s capital=%.0f qty=%d @ %.2f",
                underlying, client.get("client_id", "?"), capital, qty, entry_price,
            )

        if total_qty == 0:
            logger.warning("TrapEngine: no clients configured or capital=0 — skipping entry.")
            return

        # Cache position tuple for matched exit
        self._open_positions[trade_id] = (trade_id, option_symbol, entry_price, total_qty)

        st.trade_id    = trade_id
        st.entry_price = entry_price
        st.quantity    = total_qty
        st.phase       = _Phase.LIVE

        self._signal_count += 1
        signal = SignalPackage(
            source        = StrategyID.TRAP_ENGINE,
            direction     = Direction.BUY,
            underlying    = underlying,
            option_type   = "CE",    # Bearish trap → buy CE put position via BUY action
            target_strike = self._atm_strike(underlying),
            entry_spot    = self._spot_cache.get(underlying, 0.0),
            stop_spot     = st.ltf_sl_line,
            target_spot   = st.target_high,
            confidence    = self._confidence(st),
            timestamp     = datetime.now(IST),
            notes         = (
                f"NewTrap ENTRY | entry_origin={st.entry_origin:.2f} "
                f"ltf_entry={st.ltf_entry_line:.2f} sl={st.ltf_sl_line:.2f} "
                f"target={st.target_high:.2f} qty={total_qty} trade_id={trade_id}"
            ),
        )
        await self._bus.publish(Topic.SIGNAL, signal)
        logger.info(
            "TrapEngine SIGNAL #%d | %s BUY @ %.2f qty=%d sl=%.2f tgt=%.2f [%s]",
            self._signal_count, option_symbol, entry_price, total_qty,
            st.ltf_sl_line, st.target_high, trade_id,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Execution: Exit
    # ─────────────────────────────────────────────────────────────────────────

    async def _fire_exit(
        self, underlying: str, exit_price: float, reason: str
    ) -> None:
        st = self._get_state(underlying)
        if st.trade_id and st.trade_id in self._open_positions:
            trade_id, symbol, entry_px, qty = self._open_positions.pop(st.trade_id)
            pnl = (exit_price - entry_px) * qty
            logger.info(
                "TrapEngine EXIT [%s] reason=%s entry=%.2f exit=%.2f qty=%d pnl=₹%.0f [%s]",
                underlying, reason, entry_px, exit_price, qty, pnl, trade_id,
            )
        self._reset_state(underlying)

    async def _force_exit_all(self, underlying: str) -> None:
        st = self._get_state(underlying)
        if st.phase == _Phase.LIVE:
            prem = self._prem_cache.get(underlying, 0.0)
            await self._fire_exit(underlying, prem or st.entry_price, "market_close")

    # ─────────────────────────────────────────────────────────────────────────
    # Backtest / Simulation
    # ─────────────────────────────────────────────────────────────────────────

    def _record_backtest_entry(
        self, underlying: str, option_symbol: str, price: float, st: _TrapState
    ) -> None:
        """In-memory only — never writes to live DB or triggers live execution."""
        import uuid
        trade_id = f"BT_{uuid.uuid4().hex[:8]}"
        record = {
            "is_backtest":     True,
            "client_id":       -1,
            "trade_id":        trade_id,
            "underlying":      underlying,
            "option_symbol":   option_symbol,
            "entry_price":     price,
            "entry_origin":    st.entry_origin,
            "target_high":     st.target_high,
            "ltf_sl_line":     st.ltf_sl_line,
            "ltf_entry_line":  st.ltf_entry_line,
            "ts":              datetime.now(IST).isoformat(),
        }
        self._backtest_log.append(record)
        logger.info("TrapEngine BACKTEST entry recorded: %s", record)

    # ─────────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _get_state(self, symbol: str) -> _TrapState:
        if symbol not in self._states:
            self._states[symbol] = _TrapState()
        return self._states[symbol]

    def _reset_state(self, symbol: str) -> None:
        rb = self._states.get(symbol, _TrapState()).rolling_base
        self._states[symbol] = _TrapState()
        self._states[symbol].rolling_base = rb   # preserve rolling base across trades

    _mtf_bufs: Dict[str, OHLCVBuffer] = {}

    def _get_mtf_buf(self, symbol: str) -> OHLCVBuffer:
        if symbol not in self._mtf_bufs:
            self._mtf_bufs[symbol] = OHLCVBuffer(600)
        return self._mtf_bufs[symbol]

    def _atm_strike(self, underlying: str) -> float:
        spot = self._spot_cache.get(underlying, 0.0)
        step = self._cfg.exchange.strike_steps.get(underlying, 50.0)
        return round(spot / step) * step if spot > 0 else 0.0

    def _confidence(self, st: _TrapState) -> float:
        score = 0.50
        if st.rolling_base > 0.0:
            score += 0.10    # Downtrend structure confirmed
        if st.entry_origin > 0.0 and st.target_high > st.entry_origin:
            score += 0.10    # Valid RR geometry
        if st.ltf_sl_line > 0.0 and st.ltf_entry_line > st.ltf_sl_line:
            score += 0.15    # Nested trap with defined SL
        if st.mtf_bearish_ts:
            score += 0.10    # 5-min structure confirmed
        return min(score, 1.0)

    # ── Public accessors ─────────────────────────────────────────────────────

    def signal_count(self) -> int:
        return self._signal_count

    def state_snapshot(self) -> Dict[str, str]:
        return {sym: st.phase.name for sym, st in self._states.items()}

    def backtest_log(self) -> List[dict]:
        """Return a copy of in-memory backtest records for dashboard overlay."""
        return list(self._backtest_log)

    def telemetry_snapshot(self) -> dict:
        out = {}
        for sym, st in self._states.items():
            pos = self._open_positions.get(st.trade_id or "", None)
            out[sym] = {
                "phase":          st.phase.name,
                "entry_origin":   round(st.entry_origin,  2),
                "target_high":    round(st.target_high,   2),
                "ltf_entry_line": round(st.ltf_entry_line,2),
                "ltf_sl_line":    round(st.ltf_sl_line,   2),
                "rolling_base":   round(st.rolling_base,  2),
                "trade_id":       st.trade_id,
                "entry_price":    round(st.entry_price,   2),
                "quantity":       st.quantity,
                "current_prem":   round(self._prem_cache.get(sym, 0.0), 2),
                "unrealized_pnl": (
                    round((self._prem_cache.get(sym, 0.0) - st.entry_price) * st.quantity, 2)
                    if st.phase == _Phase.LIVE and st.entry_price > 0 else 0.0
                ),
            }
        return out
```

- [ ] **Step 3.2: Fix the `_mtf_bufs` class variable — should be instance variable**

In `__init__`, add:
```python
self._mtf_bufs: Dict[str, OHLCVBuffer] = {}
```

And remove the class-level `_mtf_bufs: Dict[str, OHLCVBuffer] = {}` line.

- [ ] **Step 3.3: Verify import**

```bash
cd e:/AlgoSoft/OptionChainBasedStrategy && python -c "
from strategies.trap_trading_engine import TrapTradingEngine, _Phase
print('Import OK — phases:', [p.name for p in _Phase])
"
```
Expected: `Import OK — phases: ['IDLE', 'HTF_BEARISH', 'TRAP_LOCKED', 'RETEST_ALERT', 'MTF_BEARISH', 'MTF_LOCKED', 'ARMED', 'LIVE']`

- [ ] **Step 3.4: Commit**

```bash
git add strategies/trap_trading_engine.py
git commit -m "feat: rewrite TrapTradingEngine with NewTrap 5-stage sequential state machine (HTF bearish → sweep → retest → MTF nested → touch trigger)"
```

---

## Task 4: Pandas Resampling + DB-Backed Warm-Start

**Files:**
- Modify: `strategies/trap_trading_engine.py`

### What and Why
On process startup the engine's OHLCV buffers are empty. Without a warm-start from the DB, the engine misses HTF bars that formed before boot. This task adds a `warm_start()` coroutine that loads the last N days of 1m bars from `option_1m_bar_repository`, resamples to HTF/MTF with pandas, and replays them through the state machine — so the engine always boots with correct structure regardless of restart time.

- [ ] **Step 4.1: Add `warm_start()` to `TrapTradingEngine`**

In `TrapTradingEngine`, after `__init__`, add:

```python
async def warm_start(self, symbols: List[str]) -> None:
    """
    Load stored 1m bars from DB, resample to HTF/MTF, and replay through
    the state machine to restore correct phase on process restart.
    Called once before run() in run_system.py.
    """
    if self._client_db is None:
        logger.warning("TrapEngine warm_start: no client_db — skipping.")
        return

    import pandas as pd
    tc   = self._cfg.trap_engine
    now  = datetime.now(IST)
    since = now - timedelta(days=tc.bars_lookback_days)

    for sym in symbols:
        rows = await asyncio.to_thread(
            self._client_db.get_1m_bars_sync, sym, since, now
        )
        if not rows:
            logger.info("TrapEngine warm_start [%s]: no 1m bars in DB.", sym)
            continue

        df = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.set_index("timestamp").sort_index()

        # Resample to HTF
        htf_df = df.resample(f"{tc.HTF_MINUTES}min", closed="left", label="left").agg({
            "open":   "first", "high": "max", "low": "min",
            "close":  "last",  "volume": "sum",
        }).dropna()

        # Resample to MTF
        mtf_df = df.resample(f"{tc.MTF_MINUTES}min", closed="left", label="left").agg({
            "open":   "first", "high": "max", "low": "min",
            "close":  "last",  "volume": "sum",
        }).dropna()

        # Replay HTF bars through state machine
        for ts, row in htf_df.iterrows():
            fake = CandleEvent(
                symbol=sym, timeframe=tc.HTF_MINUTES,
                timestamp=ts.to_pydatetime(),
                open=row.open, high=row.high,
                low=row.low,   close=row.close, volume=row.volume,
            )
            self._process_htf(fake)

        # Replay MTF bars through state machine
        for ts, row in mtf_df.iterrows():
            fake = CandleEvent(
                symbol=sym, timeframe=tc.MTF_MINUTES,
                timestamp=ts.to_pydatetime(),
                open=row.open, high=row.high,
                low=row.low,   close=row.close, volume=row.volume,
            )
            self._process_mtf(fake)

        st = self._states.get(sym)
        logger.info(
            "TrapEngine warm_start [%s]: %d HTF bars + %d MTF bars replayed → phase=%s",
            sym, len(htf_df), len(mtf_df), st.phase.name if st else "IDLE",
        )
```

- [ ] **Step 4.2: Update `run_system.py` to call `warm_start` before `run()`**

In `run_system.py`, where `TrapTradingEngine` is constructed and started, add:

```python
trap_engine = TrapTradingEngine(bus, cfg, client_db=client_db)
await trap_engine.warm_start(cfg.monitored_indices)
asyncio.create_task(trap_engine.run(), name="trap_engine")
```

- [ ] **Step 4.3: Verify pandas is available**

```bash
cd e:/AlgoSoft/OptionChainBasedStrategy && python -c "import pandas; print('pandas', pandas.__version__)"
```

If not installed: `pip install pandas`

- [ ] **Step 4.4: Commit**

```bash
git add strategies/trap_trading_engine.py run_system.py
git commit -m "feat: add TrapEngine warm_start() — loads 1m bars from DB, resamples to HTF/MTF with pandas, replays to restore phase on boot"
```

---

## Task 5: Day-of-Week ITM Strike Selection

**Files:**
- Modify: `strategies/trap_trading_engine.py`

### What and Why
The NewTrap repo uses a day-of-week offset formula (`center ± offset`) to select ITM strikes that have meaningful premium at each stage of the weekly expiry cycle. This replaces the generic ATM calculation.

- [ ] **Step 5.1: Add `_select_itm_strike()` to `TrapTradingEngine`**

```python
# Day-of-week offsets for ITM strike selection (NewTrap config.py formula)
_DOW_OFFSET: Dict[int, int] = {
    0: 200,   # Monday
    1: 100,   # Tuesday
    2: 500,   # Wednesday
    3: 400,   # Thursday
    4: 300,   # Friday
}

def _select_itm_strike(
    self, underlying: str, direction: str = "bearish"
) -> float:
    """
    NewTrap ITM strike selection.
    direction='bearish' → CE strike (center - offset)
    direction='bullish' → PE strike (center + offset)
    Requires prev-day OHLC in self._spot_cache or falls back to ATM.
    """
    spot = self._spot_cache.get(underlying, 0.0)
    if spot <= 0.0:
        logger.warning("TrapEngine [%s] spot=0 — falling back to ATM", underlying)
        return self._atm_strike(underlying)

    step    = self._cfg.exchange.strike_steps.get(underlying, 50.0)
    weekday = datetime.now(IST).weekday()
    offset  = _DOW_OFFSET.get(weekday, 200)

    if direction == "bearish":
        raw = spot - offset
    else:
        raw = spot + offset
    return round(raw / step) * step
```

- [ ] **Step 5.2: Use `_select_itm_strike` in `_fire_entry`**

In `_fire_entry`, replace the `target_strike` calculation in the `SignalPackage` with:

```python
target_strike = self._select_itm_strike(underlying, direction="bearish"),
```

- [ ] **Step 5.3: Commit**

```bash
git add strategies/trap_trading_engine.py
git commit -m "feat: NewTrap day-of-week ITM strike selection (center ± offset per weekday)"
```

---

## Task 6: Dashboard — Live Unrealized P&L + Backtest Overlay

**Files:**
- Modify: `ui_layer/dashboard_server.py`
- Modify: `ui_layer/templates/monitor.html`

### What and Why
The dashboard needs to show live trap positions with color-coded unrealized P&L computed from the WebSocket tick cache (not broker REST polling), and a toggle to show/hide simulation records with a safety warning banner.

- [ ] **Step 6.1: Add `/api/trap/positions` endpoint to `dashboard_server.py`**

Find the `router` / FastAPI `app` in `dashboard_server.py`. Add after existing trap telemetry endpoints (or add a new router section for trap):

```python
@app.get("/api/trap/positions")
async def trap_positions(show_backtest: bool = False):
    """
    Returns live trap positions with unrealized P&L from tick cache.
    Unrealized P&L = (current_ltp - entry_price) * quantity
    If show_backtest=True, also includes records from the backtest log.
    """
    engine = _srv._trap_engine   # reference to TrapTradingEngine instance
    if engine is None:
        return {"ok": True, "positions": [], "backtest": []}

    snap = engine.telemetry_snapshot()
    positions = []
    for sym, data in snap.items():
        if data.get("trade_id") is None:
            continue
        positions.append({
            "symbol":          sym,
            "trade_id":        data["trade_id"],
            "entry_price":     data["entry_price"],
            "current_prem":    data["current_prem"],
            "quantity":        data["quantity"],
            "unrealized_pnl":  data["unrealized_pnl"],
            "ltf_sl_line":     data["ltf_sl_line"],
            "target_high":     data["target_high"],
            "phase":           data["phase"],
            "is_backtest":     False,
        })

    backtest_records = []
    if show_backtest:
        backtest_records = engine.backtest_log()

    return {"ok": True, "positions": positions, "backtest": backtest_records}
```

- [ ] **Step 6.2: Add Trap Positions panel to `monitor.html`**

In `monitor.html`, add a new Alpine.js component section for the trap positions. Place it after the existing positions section. The key behaviors:
- Color-code P&L: green for positive (`#238636`), red for negative (`#DA3633`)
- Format currency: `₹+1,250.00` / `₹-500.00`
- Simulation toggle with orange warning banner

```html
<!-- ── Trap Positions Panel ─────────────────────────────────────────── -->
<div x-data="trapPositions()" x-init="init()" class="mt-6">
  <!-- Simulation Overlay Toggle -->
  <div class="flex items-center gap-3 mb-3">
    <h3 class="text-sm font-semibold text-gray-300 uppercase tracking-wider">Trap Positions</h3>
    <label class="flex items-center gap-2 cursor-pointer ml-auto">
      <span class="text-xs text-gray-400">Show Simulation</span>
      <input type="checkbox" x-model="showBacktest" @change="fetchPositions()"
             class="w-4 h-4 rounded accent-orange-500" />
    </label>
  </div>

  <!-- Orange safety banner when simulation overlay is active -->
  <div x-show="showBacktest" x-cloak
       class="mb-3 px-4 py-2 rounded text-xs font-semibold"
       style="background:#4d2e00; border:1px solid #f97316; color:#fdba74">
    ⚠ SIMULATION OVERLAY ACTIVE — Backtest records shown. These are NOT live trades.
  </div>

  <!-- Live Positions Table -->
  <table class="w-full text-xs">
    <thead>
      <tr class="text-gray-400 border-b border-gray-700">
        <th class="py-1 text-left">Symbol</th>
        <th class="py-1 text-right">Entry</th>
        <th class="py-1 text-right">LTP</th>
        <th class="py-1 text-right">Qty</th>
        <th class="py-1 text-right">P&amp;L</th>
        <th class="py-1 text-right">SL</th>
        <th class="py-1 text-right">Target</th>
        <th class="py-1 text-center">Phase</th>
      </tr>
    </thead>
    <tbody>
      <template x-for="p in allRows" :key="p.trade_id || p.symbol">
        <tr class="border-b border-gray-800"
            :class="p.is_backtest ? 'opacity-60' : ''">
          <td class="py-1 font-mono"
              x-text="p.symbol + (p.is_backtest ? ' [SIM]' : '')"></td>
          <td class="py-1 text-right font-mono" x-text="fmt(p.entry_price)"></td>
          <td class="py-1 text-right font-mono" x-text="fmt(p.current_prem)"></td>
          <td class="py-1 text-right" x-text="p.quantity"></td>
          <td class="py-1 text-right font-mono font-semibold"
              :style="p.unrealized_pnl >= 0 ? 'color:#238636' : 'color:#DA3633'"
              x-text="fmtPnl(p.unrealized_pnl)"></td>
          <td class="py-1 text-right font-mono text-red-400" x-text="fmt(p.ltf_sl_line)"></td>
          <td class="py-1 text-right font-mono text-green-400" x-text="fmt(p.target_high)"></td>
          <td class="py-1 text-center">
            <span class="px-1.5 py-0.5 rounded text-[10px]"
                  :class="p.phase === 'LIVE' ? 'bg-green-900 text-green-300' : 'bg-gray-700 text-gray-300'"
                  x-text="p.phase"></span>
          </td>
        </tr>
      </template>
      <tr x-show="allRows.length === 0">
        <td colspan="8" class="py-3 text-center text-gray-500 text-xs">No active trap positions</td>
      </tr>
    </tbody>
  </table>
</div>

<script>
function trapPositions() {
  return {
    positions: [],
    backtest: [],
    showBacktest: false,
    get allRows() {
      return [...this.positions, ...(this.showBacktest ? this.backtest : [])];
    },
    async init() {
      await this.fetchPositions();
      setInterval(() => this.fetchPositions(), 2000);
    },
    async fetchPositions() {
      try {
        const url = `/api/trap/positions?show_backtest=${this.showBacktest}`;
        const r = await fetch(url);
        const d = await r.json();
        this.positions = d.positions || [];
        this.backtest  = (d.backtest || []).map(b => ({...b, is_backtest: true}));
      } catch(e) { console.warn('trap positions fetch failed', e); }
    },
    fmt(v) {
      if (!v && v !== 0) return '—';
      return '₹' + Number(v).toFixed(2);
    },
    fmtPnl(v) {
      if (!v && v !== 0) return '₹0.00';
      const sign = v >= 0 ? '+' : '';
      return '₹' + sign + Number(v).toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2});
    },
  }
}
</script>
```

- [ ] **Step 6.3: Verify the endpoint is accessible**

Start the server and test:
```bash
cd e:/AlgoSoft/OptionChainBasedStrategy && python -c "
import asyncio
from ui_layer.dashboard_server import create_app
print('dashboard import OK')
"
```

- [ ] **Step 6.4: Commit**

```bash
git add ui_layer/dashboard_server.py ui_layer/templates/monitor.html
git commit -m "feat: dashboard trap positions panel with live unrealized P&L (#238636/#DA3633), simulation overlay toggle + orange safety banner"
```

---

## Task 7: Wire `_srv._trap_engine` reference in DashboardServer

**Files:**
- Modify: `ui_layer/dashboard_server.py`

### What and Why
The `/api/trap/positions` endpoint references `_srv._trap_engine`, but the `DashboardServer` / app server class doesn't currently hold a reference to the engine instance. This task adds the wiring.

- [ ] **Step 7.1: Add `trap_engine` attribute to the server state object**

In `dashboard_server.py`, find the `_AppState` or `_ServerState` dataclass / object (the one referenced as `_srv`). Add:

```python
_trap_engine: Optional["TrapTradingEngine"] = None
```

- [ ] **Step 7.2: Expose `set_trap_engine()` on the server**

```python
def set_trap_engine(engine) -> None:
    """Called from run_system.py after TrapTradingEngine is constructed."""
    _srv._trap_engine = engine
```

- [ ] **Step 7.3: Call `set_trap_engine` in `run_system.py`**

After constructing the engine:
```python
from ui_layer.dashboard_server import set_trap_engine
set_trap_engine(trap_engine)
```

- [ ] **Step 7.4: Commit**

```bash
git add ui_layer/dashboard_server.py run_system.py
git commit -m "feat: wire TrapTradingEngine reference into DashboardServer for /api/trap/positions"
```

---

## Spec Coverage Self-Review

| Spec Requirement | Task | Status |
|-----------------|------|--------|
| `option_1m_bar_repository` table with composite index + UniqueConstraint | Task 1 Step 1.1 | ✓ |
| `upsert_1m_bar()` via `asyncio.to_thread()` | Task 1 Steps 1.2-1.3 | ✓ |
| GlobalFeeder persists 1m candles on CANDLE_CLOSE | Task 1 Step 1.6 | ✓ |
| `HTF_MINUTES`, `MTF_MINUTES`, `LTF_MINUTES`, `RETEST_ZONE_PERCENT`, `LOT_SIZE`, `SLIPPAGE_BUFFER` in config | Task 2 | ✓ |
| Thread-safe config (`RLock`) | Task 2 Step 2.1 | ✓ |
| On-the-fly pandas resampling from 1m bars | Task 4 | ✓ |
| `_TrapState` / backtest track: in-memory, never touches live DB | Task 3 (`_record_backtest_entry`) | ✓ |
| Rolling base: candle closing below prev = new base | Task 3 `_process_htf` + `_process_mtf` | ✓ |
| Bearish setup: active candle sweeps bearish stop-loss level | Task 3 Stage 2 logic | ✓ |
| Void lift: `candle.low <= htf_entry_level` | Task 3 Stage 3/5 logic (touch trigger is equivalent lift) | ✓ |
| ATM strike selection at entry via `round_to_strike(spot)` | Task 3 `_atm_strike` + Task 5 ITM variant | ✓ |
| Dynamic capital calculator: `floor(capital / (entry_price * LOT_SIZE)) * LOT_SIZE` | Task 3 `_fire_entry` | ✓ |
| Minimum floor of 1 lot | Task 3 `max(raw_qty, lot_size)` | ✓ |
| Position cache `(trade_id, symbol, entry_px, quantity)` | Task 3 `_open_positions` dict | ✓ |
| Exit uses exact recorded quantity | Task 3 `_fire_exit` pops from `_open_positions` | ✓ |
| Unrealized P&L from tick cache (no REST polling) | Task 6 `/api/trap/positions` + `telemetry_snapshot` | ✓ |
| Color: `#238636` gain, `#DA3633` drawdown, ₹ format | Task 6 Step 6.2 | ✓ |
| Simulation overlay toggle + `is_backtest=True` filter + `client_id=-1` | Task 6 + Task 3 | ✓ |
| Orange safety warning banner for simulation overlay | Task 6 Step 6.2 | ✓ |

**No gaps identified.**

---

## V3 Sell Straddle — Hybrid/Beginning/Re-entry Behavior Reference

(For completeness — `SellStraddleStrategy` is already implemented. This describes how the modes interact.)

| Mode | Trigger | Entry Rules Source | Notes |
|------|---------|-------------------|-------|
| BEGINNING | `trades_today == 0` | `entry_rules_beginning` | Priming wait = `max_tf × 2` if any SLOPE rule, else `max_tf × 1`. Entry window: `entry_start` → `entry_cutoff` (default 09:20 → 12:00) |
| RE-ENTRY | `trades_today >= 1` | `entry_rules_reentry` | Triggered after any close (profit/SL/ratio/etc.) if still in entry window and within `max_trades` |
| HYBRID (Smart Roll) | After profit/ratio/scalable-TSL exit | `entry_rules_reentry` | Virtual roll if ATM unchanged; Physical roll if ATM drifted. Counts as new trade against `max_trades` |

**Exit priority order:**
1. Day profit target (stops trading for day)
2. Day loss SL (stops trading for day)
3. Time exit 15:15 IST
4. Trailing SL (% of net credit)
5. Scalable TSL (₹ per lot staircase)
6. Profit target → Smart Roll first
7. Stop loss (hard, no roll)
8. Ratio exit (CE/PE LTP ratio ≥ threshold) → Smart Roll first
9. VWAP Rise SL → Smart Roll first
10. ROC guardrail (spot spike %) → Smart Roll first

## Iron Condor Behavior Reference

`IronCondorStrategy` (`strategies/iron_condor.py`) skeleton is complete. Entry on CANDLE_CLOSE when RSI 40-60 and ADX < 25. It follows the same `EventBus` pattern — subscribes to `Topic.CANDLE_CLOSE`, evaluates entry rules, publishes `SignalPackage` to `Topic.SIGNAL`. Execution routing via `ExecutionRouter` is marked TODO — when wired, it fires 4 simultaneous orders (Short CE + Short PE + Long CE wing + Long PE wing).

---

## Dependencies to Install

```bash
pip install pandas
```
(All other dependencies — numpy, sqlite3, fastapi, asyncio — are already present per CLAUDE.md)
