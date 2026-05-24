# Developer Architecture Guide

## System Overview

Enterprise-grade, event-driven, multi-tenant option-chain algo trading system for NSE/BSE.
Built on pure asyncio — no `time.sleep` anywhere. All cross-module communication flows
through an in-memory Pub-Sub `EventBus`.

---

## Directory Structure

```
OptionChainBasedStrategy/
├── config/
│   ├── global_config.py         System constants, session times, indicator params
│   └── client_profiles.py       Multi-tenant client & broker binding registry
│
├── data_layer/
│   ├── base_feeder.py           EventBus, IndexTick, OptionTick, CandleEvent (pub-sub backbone)
│   ├── global_feeder.py         WebSocket adapter: two-stage pipeline (_ws_loop + _parse_worker)
│   ├── symbol_translator.py     InternalSymbol <-> broker-specific symbol format conversion
│   ├── tick_recorder.py         Async Parquet + ZStandard tick logger (non-blocking I/O)
│   ├── strike_rebalancer.py     Dynamic ATM strike monitor: resubscribes on 3-interval spot drift
│   └── strike_cleanup.py        Post-exit stream GC: unsubscribes dead strikes after position close
│
├── matrix_engine/
│   ├── candle_cache.py          Tick -> OHLCV aggregation per timeframe; publishes CandleEvent
│   ├── indicators.py            Vectorized NumPy: RSI(14), VWAP(500), ADX(20), EMA, ATR
│   ├── option_matrix.py         Live option chain: per-strike OI/DOI/IV, max-pain, PCR
│   ├── state_persistence.py     SQLite snapshot on CANDLE_CLOSE; hot-reload on mid-day reboot
│   └── gap_handler.py           Gap-open detector: captures pre-open ref, triggers reset on >1% drift
│
├── strategies/
│   ├── base_strategy.py         SignalPackage, ConfluenceEngine (meta-aggregator), BaseStrategy
│   ├── strategy_a_oi.py         OI Zone Breakout / Rejection
│   ├── strategy_b_trap.py       Liquidity Trap + Rolling Base + Void/Lift mechanism
│   └── strategy_c_panic.py      Panic Selling / Put Unwind reversal
│
├── execution_bridge/
│   ├── base_broker.py           BaseBroker ABC, MockBroker, BROKER_REGISTRY, OrderRequest/Fill
│   ├── rate_limiter.py          Token-bucket rate limiter: 10 req/s per broker binding
│   ├── parallel_worker_pool.py  ClientExecutionWorker (per-client asyncio.Queue + circuit breaker)
│   ├── execution_router.py      Thin signal dispatcher: pool.dispatch() -> all workers simultaneously
│   ├── broker_shoonya.py        Shoonya/Finvasia (NorenRestApiPy) - self-registers
│   ├── broker_fyers.py          Fyers API v3 (fyers-apiv3) - self-registers
│   ├── broker_angel.py          Angel One SmartAPI (smartapi-python + pyotp) - self-registers
│   ├── broker_dhan.py           Dhan HQ (dhanhq) - self-registers
│   └── broker_upstox.py         Upstox API v2 (upstox-python-sdk) - self-registers
│
├── management/
│   ├── client_manager.py        Subscribes ORDER_FILL; enforces drawdown limits; auto-halts
│   └── admin_console.py         Async REPL with state monitoring and rebalancing commands
│
├── backtester/
│   ├── historical_core.py       Event-driven backtester on recorded Parquet data
│   └── unified_iterator.py      Zero-copy Parquet pump through production engine stack
│
├── main.py                      Bootstrap: argument parsing, mode selection, task launch
├── DEVELOPER_ARCHITECTURE.md    This file
└── CLIENT_USER_GUIDE.md         Operator setup guide
```

---

## Event Bus

```
EventBus (data_layer/base_feeder.py)
  subscribe(topic) -> asyncio.Queue   # Each subscriber gets its own Queue
  publish(topic, event) -> None       # put_nowait() to all subscriber queues (non-blocking)
  drop_stats() -> Dict[str, int]      # Count of dropped messages per topic
```

### Topic Constants (`config/global_config.py :: Topic`)

| Topic            | Publisher              | Subscribers                              | Payload Type     |
|------------------|------------------------|------------------------------------------|------------------|
| `index_tick`     | GlobalFeeder           | CandleCache, TickRecorder, StrikeRebalancer | `IndexTick`   |
| `option_tick`    | GlobalFeeder           | OptionMatrixEngine, Recorder             | `OptionTick`     |
| `candle_close`   | CandleCache            | StatePersistence, diagnostic             | `CandleEvent`    |
| `matrix_snapshot`| CandleCache / OptionMatrix | ConfluenceEngine                     | `TechSnapshot` / `ChainSnapshot` |
| `signal`         | ConfluenceEngine       | ExecutionRouter                          | `SignalPackage`  |
| `order_fill`     | ClientExecutionWorker  | ClientManager                            | `OrderFill`      |
| `system_event`   | Any                    | AdminConsole, logging                    | `dict`           |

**Backpressure policy**: if a subscriber queue is full (maxsize=20,000), the event is
DROPPED (never blocked). The drop count is tracked per topic and visible via the
AdminConsole `drop_counts` command.

---

## Data Flow — Live Tick to Order

```
WebSocket (broker)
       |
       v
GlobalFeeder._ws_loop()                    [data_layer/global_feeder.py]
  receive raw frames → _enqueue_raw()
  (WebSocket thread NEVER parses — stays unblocked)
       |
       v   [asyncio Queue — RAW_QUEUE_SIZE = 50,000]
       |
GlobalFeeder._parse_worker()               [independent asyncio Task]
  _parse_frame(raw) → IndexTick / OptionTick
  bus.publish("index_tick" / "option_tick", tick)
       |
       +----> TickRecorder                 [data_layer/tick_recorder.py]
       |       asyncio.to_thread -> pyarrow.parquet write
       |
       +----> StrikeRebalancer.run()       [data_layer/strike_rebalancer.py]
       |       monitors spot drift from open-ATM
       |       on drift >= 3 × strike_step:
       |         feeder.unsubscribe_tokens(old_strikes - pinned)
       |         feeder.subscribe_tokens(new_strikes)
       |
       v
CandleCache.run()                          [matrix_engine/candle_cache.py]
  bucket ticks into OHLCV per timeframe (5m, 15m, 75m)
  on bucket close:
    compute TechSnapshot (RSI, VWAP, ADX, EMA, ATR) via indicators.py
    bus.publish("candle_close", CandleEvent)
    bus.publish("matrix_snapshot", TechSnapshot)
       |
       +----> StatePersistence.run()       [matrix_engine/state_persistence.py]
               on CANDLE_CLOSE:
                 snapshot TechSnapshot + Strategy B state -> SQLite (asyncio.to_thread)
       |
       v
OptionMatrixEngine.run()                   [matrix_engine/option_matrix.py]
  update ChainRow (OI, doi, IV, LTP) per option tick
  recompute PCR, max-pain, max-OI strikes
  bus.publish("matrix_snapshot", ChainSnapshot)
       |
       v
ConfluenceEngine.run()                     [strategies/base_strategy.py]
  hold latest TechSnapshot + ChainSnapshot per underlying
  when BOTH are fresh:
    for each strategy: evaluate(tech, chain) -> Optional[SignalPackage]
    directional conflict check (LONG + SHORT -> discard all)
    min_rr >= 2.0, min_confidence >= 0.50
    bus.publish("signal", best_signal)
       |
       v
ExecutionRouter._dispatch()                [execution_bridge/execution_router.py]
  signal.is_valid() gate
  pool.dispatch(signal)                    ← O(N) put_nowait(), returns in microseconds
       |
       v   [per-client asyncio.Queue(maxsize=100)]
       |
ClientExecutionWorker.run()  [N workers]   [execution_bridge/parallel_worker_pool.py]
  each worker runs in its own asyncio Task
  translate InternalSymbol -> broker symbol (SymbolTranslator)
  compute lot size (risk-based)
  await broker.place_order(req)
  await broker.get_order_status()
  bus.publish("order_fill", fill)
       |
       v
ClientManager.run()                        [management/client_manager.py]
  update daily P&L
  check drawdown limit
  auto-halt breached clients
  bus.publish("system_event", HALT) if needed
```

---

## Parallel Execution — ClientExecutionWorker Architecture

```
                    SignalPackage
                         │
              ExecutionRouter._dispatch()
                         │
              pool.dispatch(signal)  ← single O(N) loop, no await
                    │   │   │
          ┌─────────┘   │   └─────────┐
          ▼             ▼             ▼
  Worker[C001]    Worker[C002]    Worker[C003]
  Queue[100]      Queue[100]      Queue[100]
  asyncio.Task    asyncio.Task    asyncio.Task
       │               │               │
  Shoonya broker  Fyers broker    Upstox broker
  await place()   await place()   await place()
```

**Key invariant**: Client A's network round-trip to Shoonya is completely isolated from
Client B's call to Fyers. Both run simultaneously in their own asyncio Tasks.

If a worker's queue fills (e.g., client's broker is down), that client's signal is
dropped with a warning — all other clients are unaffected.

---

## Dynamic Strike Rebalancing

```
StrikeRebalancer (data_layer/strike_rebalancer.py)
  Subscribes to: INDEX_TICK

State per underlying:
  open_atm       — ATM recorded on first tick at/after 09:15 IST
  current_atm    — Most recent baseline (updated after each rebalance)
  active_strikes — Currently subscribed set
  pinned_strikes — Open-position strikes (NEVER unsubscribed)

Rebalance trigger:
  abs(new_atm - current_atm) >= 3 × strike_step

On trigger:
  new_window = ATM ± chain_depth strikes
  to_unsub = active - new_window - pinned    # safe to drop
  to_sub   = new_window - active            # need to add
  await feeder.unsubscribe_tokens(to_unsub)
  await feeder.subscribe_tokens(to_sub)
  bus.publish(SYSTEM_EVENT, rebalance notice)
```

---

## State Persistence & Mid-Day Reboot Recovery

```
StatePersistence (matrix_engine/state_persistence.py)
  Subscribes to: CANDLE_CLOSE
  Database:      data/state_snapshots.db (SQLite)

Tables:
  candle_snapshots   — RSI, VWAP, ADX, EMA, ATR per symbol×timeframe
  strategy_b_state   — rolling_base, htf_entry_level, void_phase, void_since
  order_tickets      — client_id, symbol, side, qty, order_id, avg_price (open/closed)
  risk_params        — capital, daily_pnl, trade_count, is_halted

Write path:
  CANDLE_CLOSE → provider callbacks → batch dict → asyncio.to_thread(SQLite write)
  Non-blocking: event loop is never stalled by disk I/O.

Boot recovery:
  on reboot: persist.restore_state() reads latest row per key
  admin command: state_restore
```

---

## Multi-Broker Architecture

### BROKER_REGISTRY (execution_bridge/base_broker.py)

Self-registration pattern — each `broker_*.py` module adds its factory at import time:

```python
# broker_shoonya.py (last line)
BROKER_REGISTRY["shoonya"] = lambda b, cid: ShoonyaBroker(b, cid)
```

The `execution_bridge/__init__.py` imports all five broker modules, triggering registration.
`create_broker(binding, client_id)` looks up the factory by `binding.provider`.

### Symbol Translation (data_layer/symbol_translator.py)

All strategies produce `InternalSymbol(underlying, strike, option_type, expiry)`.
Worker `_translate()` calls the appropriate static method per binding provider:

| Provider  | Example Output                     | Format                                   |
|-----------|------------------------------------|------------------------------------------|
| Shoonya   | `NIFTY28MAY26C22000`               | `{SYMBOL}{DD}{MON}{YY}{C/P}{STRIKE}`     |
| Fyers     | `NSE:NIFTY2652822000CE`            | `{EX}:{SYMBOL}{YY}{M_CODE}{DD}{STRIKE}{CE/PE}` |
| Angel One | `NIFTY28MAY2422000CE`              | `{SYMBOL}{DD}{MON}{YY}{STRIKE}{CE/PE}`   |
| Dhan      | security_id lookup key             | Pre-fetched from instrument CSV          |
| Upstox    | `NSE_FO\|NIFTY2526522000CE`        | `{SEGMENT}\|{SYMBOL}{YY}{DD}{MM}{STRIKE}{CE/PE}` |

---

## Multi-Tenant Client Model

```
ClientRegistry (config/client_profiles.py)
  clients: {client_id: ClientProfile}
  tradeable_clients() -> [ClientProfile]   # active + not halted + within daily limits

ClientProfile
  risk: RiskProfile                        # capital, daily loss %, max trades
  broker_bindings: [BrokerBinding]         # N brokers per client
  enabled_strategies: ["A", "B", "C"]
  _halted: bool                            # runtime, not persisted
  _daily_pnl: float                        # runtime, reset at 09:15

BrokerBinding
  provider: "shoonya" | "fyers" | "angelone" | "dhan" | "upstox" | "mock"
  lot_multiplier: float                    # Scale signal lots (e.g. 0.5x)
  credentials: user_id, api_key, totp_secret, ...
```

**Credentials** are never written to the JSON persistence file (`config/client_profiles.json`).
They are injected at runtime via `registry.inject_credentials(client_id, binding_id, **kwargs)`
or loaded from environment variables via `BrokerBinding.from_env(...)`.

---

## Strategy Engine

### BaseStrategy.evaluate(tech, chain, all_tf) -> Optional[SignalPackage]

Each strategy is a pure, synchronous function — no I/O, no async, no state mutation
beyond its own internal `_state` dict. The `ConfluenceEngine` orchestrates evaluation.

### SignalPackage (frozen dataclass)

```python
SignalPackage(
    source=StrategyID.B_TRAP,
    direction=Direction.LONG,
    underlying="NIFTY",
    option_type="CE",
    target_strike=22000.0,
    entry_spot=21980.0,
    stop_spot=21900.0,       # SL on underlying
    target_spot=22160.0,     # Target on underlying
    confidence=0.72,
    notes="Rolling base confirm + call OI spike",
)
```

`rr_ratio = reward / risk = (22160 - 21980) / (21980 - 21900) = 180/80 = 2.25`

### Strategy B — Rolling Base + Void/Lift

```
State Machine:

IDLE ──(OI spike + vol spike, price near resistance/support)──> BEARISH_TRAP / BULLISH_TRAP
TRAP ──(stall_count >= N AND OI unwinding)──> CONFIRMED  ──> SIGNAL
TRAP ──(price runs 2×ATR past trap level)──> VOID
VOID ──(candle.low <= htf_entry_level + tolerance)──> CONFIRMED  ──> SIGNAL (Void Lift)
       (void_lift is INVALID until this exact retest condition is met)

Rolling Base:
  Any candle closing BELOW previous candle: rolling_base = min(rolling_base, c_low)
  This ensures the trap level tracks the weakest low dynamically.

Void Lift Guard:
  htf_entry_level must be non-zero (trap must have been detected first).
  Retest condition: tech.c_low <= htf_entry_level + tol
  tol = htf_entry_level × void_lift_retest_tolerance / 100   (default 0.10%)
```

---

## Indicator Spec (Hard-Pinned)

| Indicator | Period | Source                        |
|-----------|--------|-------------------------------|
| RSI       | 14     | Wilder's smoothing             |
| VWAP      | 500    | Rolling 500-candle window       |
| ADX       | 20     | Returns (ADX, +DI, -DI)        |
| EMA fast  | 9      | Standard exponential smoothing |
| EMA slow  | 21     | Standard exponential smoothing |
| ATR       | 14     | True Range Wilder's avg        |

Period constants are module-level in `matrix_engine/indicators.py`:
`RSI_PERIOD=14`, `VWAP_WINDOW=500`, `ADX_PERIOD=20`.
The public functions `rsi()`, `vwap()`, `adx()` accept **no period arguments** —
impossible to accidentally pass a wrong value at any call site.

---

## Backtester

### Historical Core (backtester/historical_core.py)

```
HistoricalBacktester.run(underlying, start, end, capital)

Data source hierarchy:
  1. data/recorded/{underlying}/spot/YYYY-MM-DD.parquet  (TickRecorder output)
  2. Synthetic fallback: NumPy random-walk intraday data (for demo/testing)

Pipeline:
  tick stream -> _LightCandleBuilder (per tf) -> _IndicatorState (deque buffers)
  -> TechSnapshot -> _build_chain (synthetic ChainSnapshot from Black-Scholes)
  -> ConfluenceEngine.force_evaluate() [synchronous path]
  -> TradeRecord (entry, SL/target tracking, exit)
  -> BacktestReport (win rate, profit factor, max drawdown, per-trade log)
```

### Unified Iterator (backtester/unified_iterator.py)

Zero-copy Parquet event pump that routes historical ticks through the **production**
engine stack without modifying any strategy code.

```
ParquetEventPump.merged_stream(sources)   ← heap-merge across symbol files
       │
       v   (IndexTick, time-ordered)
       │
UnifiedBacktestIterator._async_run()
  bus.publish("index_tick", tick)         ← same path as live
  CandleCache processes normally
  CANDLE_CLOSE → ConfluenceEngine.evaluate_from_snapshot()
  SignalPackage collected in ReplayResult
```

**Zero-copy contract**: Parquet columns read once as pyarrow arrays. Rows iterated
as scalars — no secondary DataFrame buffer held in memory during replay.

---

## Session Lifecycle

```
09:00 IST  pre_open_connect   GlobalFeeder: connect WebSocket, download instrument masters
09:15 IST  market_open        CandleCache, OptionMatrix, Strategies all start processing
                               StrikeRebalancer: record open-ATM for each underlying
           ClientManager       reset_all_daily() — clear P&L, trade counters, unhalt
15:25 IST  near_close         Backtester: force EOD-exit all open positions
15:30 IST  market_close       GlobalFeeder: unsubscribe, flush all buffers
15:45 IST  eod_cleanup        TickRecorder: rotate Parquet files, rename with date stamp
```

---

## Adding a New Broker

1. Create `execution_bridge/broker_{name}.py`
2. Subclass `BaseBroker`, implement all 7 abstract methods:
   `authenticate`, `logout`, `place_order`, `cancel_order`,
   `get_order_status`, `get_positions`, `get_funds`
3. Add self-registration at the bottom:
   ```python
   BROKER_REGISTRY["newbroker"] = lambda b, cid: NewBroker(b, cid)
   ```
4. Add the import in `execution_bridge/__init__.py`:
   ```python
   import execution_bridge.broker_newbroker  # noqa: F401
   ```
5. Add the provider literal to `BrokerBinding.provider` in `config/client_profiles.py`
6. Add symbol translation in `data_layer/symbol_translator.py`
7. Handle the new provider case in `execution_bridge/parallel_worker_pool.py::_translate()`

---

## Adding a New Strategy

1. Create `strategies/strategy_d_{name}.py`
2. Subclass `BaseStrategy`, implement `evaluate(tech, chain, all_tf)`
3. Return `None` (no signal) or a fully-populated `SignalPackage`
4. Add strategy letter to `_SOURCE_TO_LETTER` in `execution_bridge/parallel_worker_pool.py`
5. Register in `main.py`:
   ```python
   from strategies.strategy_d_name import StrategyD_Name
   strategies = [..., StrategyD_Name(cfg)]
   ```
6. Add `"D"` to `ClientProfile.enabled_strategies` for clients that should trade it

---

## Phase 3 Operational Hardening

### A. Stream Cleanup — `data_layer/strike_cleanup.py`

Prevents bandwidth waste from dead option subscriptions after a position is closed.

```
StrikeCleanup
  Subscribes to: ORDER_FILL, SYSTEM_EVENT

  Dual cleanup condition (both must be true to unsubscribe):
    1. open_count[(underlying, strike)] == 0   # no remaining live positions
    2. strike NOT IN rebalancer.active_strikes  # outside current ATM window

  On ORDER_FILL BUY  → increment open_count
  On ORDER_FILL SELL → decrement open_count, check cleanup eligibility
  On SYSTEM_EVENT POSITION_CLOSED (format: "UNDERLYING:STRIKE:OPTTYPE")
                      → decrement open_count, check cleanup eligibility

  If eligible: await feeder.unsubscribe_tokens([CE_token, PE_token])
               rebalancer.unpin_strike(underlying, strike)

  If strike is still in ATM window: leave subscribed (overhead is minimal,
  may be needed for next signal without waiting for a new subscription round-trip)

Public API:
  notify_position_opened(underlying, strike)    # sync-safe increment
  notify_position_closed(underlying, strike)    # sync-safe; schedules async cleanup
  cleanup_stats() -> dict                       # cleanups_performed, skipped, open_positions
```

### B. Gap-Open Detection — `matrix_engine/gap_handler.py`

Prevents poisoned indicator readings on gap-open days where overnight news moves
the index > 1% away from the pre-market reference price.

```
GapHandler
  Subscribes to: INDEX_TICK
  Threshold: GAP_THRESHOLD = 0.01 (1%)

  Phase 1 — Pre-open capture (09:08:00 – 09:14:59 IST):
    First tick for each underlying at/after 09:08 IST records pre_open_ref price.
    Source: NSE call-auction equilibrium price during pre-open session.

  Phase 2 — Opening validation (first tick >= 09:15:01 IST):
    drift = |opening_spot - pre_open_ref| / pre_open_ref
    If drift > 1%: publish GAP_OPEN system event + fire all reset callbacks

  Reset cascade (all callbacks run concurrently via asyncio.gather):
    - candle_cache.reset_symbol(underlying)    # clear RSI/VWAP/ADX buffers
    - rebalancer: set current_atm = None       # forces new ATM from opening price
    - strategy state machines: reset to IDLE

Registration in main.py or admin_console.py:
  gap_handler.register_reset_callback(candle_cache.reset_symbol_async)
  gap_handler.register_reset_callback(rebalancer.reset_atm)
  gap_handler.register_reset_callback(strategy.reset_state_async)

  Call daily_reset() at 15:45 IST to clear state for next session.

SysEvent.GAP_OPEN message format:
  "UNDERLYING:ref=PRICE:open=PRICE:drift=N.NNpct"
```

### C. Execution Rate Limiting — `execution_bridge/rate_limiter.py`

Hard ceiling of 10 req/s per broker binding to prevent HTTP 429 errors.

```
TokenBucket (per binding, per client)
  Algorithm: continuous token refill at rate tokens/second; burst capacity
  acquire():
    if tokens available: consume immediately, return (O(1), no yield)
    else: sleep(deficit / rate) exactly, then return
    (no busy-wait, no time.sleep — purely asyncio.sleep with computed duration)

BrokerRateProfile per provider:
  shoonya / fyers / angelone / dhan / upstox: rate=10.0/s, burst=10
  mock: rate=1000.0/s, burst=1000  (effectively unlimited for testing)

ClientRateLimiterRegistry (per ClientExecutionWorker)
  get_limiter(binding_id, provider) -> TokenBucket   # lazy-creates on first call
  override_rate(binding_id, rate, burst)             # runtime admin adjustment
  all_stats() -> dict                                # token counts and wait stats

Integration in ClientExecutionWorker._place():
  bucket = self._rate_limiter.get_limiter(broker.binding_id, provider)
  await bucket.acquire()           # before place_order()
  order_id = await broker.place_order(req)
  await bucket.acquire()           # before get_order_status()
  fill = await broker.get_order_status(order_id)
```

Rate limiter stats appear in `WorkerPool.stats()` under `rate_limiter` key per client.

---

## Key Design Invariants

- **No `time.sleep` anywhere** — all yielding via `asyncio.wait_for(..., timeout=1.0)`
- **All datetimes are IST-aware** — `datetime.now(IST)`, never `datetime.utcnow()`
- **Strategy layer is sync** — `evaluate()` must return fast; no I/O, no network calls
- **Broker layer is async** — all broker methods are `async def`, wrapped with `asyncio.to_thread`
- **EventBus drops over blocks** — slow consumers cause drops, not backpressure on publishers
- **Credentials never logged or persisted** — only `binding.mask()` dict is safe to log
- **SignalPackage is frozen** — strategies cannot mutate a signal after creation
- **Indicator periods are hard-pinned** — no period arguments accepted at call sites
- **Worker isolation** — one asyncio Task per client; no shared mutable state between workers
- **State survives reboots** — SQLite snapshots on every CANDLE_CLOSE; `state_restore` on boot
- **Rate-limited broker calls** — `await bucket.acquire()` before every `place_order()` and `get_order_status()` call; never floods broker APIs
- **Gap-safe indicator windows** — GapHandler clears CandleCache buffers on gap-open so RSI/VWAP/ADX never compute on cross-session data
