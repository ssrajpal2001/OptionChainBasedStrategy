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
│   ├── global_feeder.py         Websocket adapter: connects broker feed, normalizes & publishes ticks
│   ├── symbol_translator.py     InternalSymbol <-> broker-specific symbol format conversion
│   └── tick_recorder.py         Async Parquet + ZStandard tick logger (non-blocking I/O)
│
├── matrix_engine/
│   ├── candle_cache.py          Tick -> OHLCV aggregation per timeframe; publishes CandleEvent
│   ├── indicators.py            Vectorized NumPy: RSI(14), VWAP(500), ADX(20), EMA, ATR
│   └── option_matrix.py         Live option chain: per-strike OI/DOI/IV, max-pain, PCR
│
├── strategies/
│   ├── base_strategy.py         SignalPackage, ConfluenceEngine (meta-aggregator), BaseStrategy
│   ├── strategy_a_oi.py         OI Zone Breakout / Rejection
│   ├── strategy_b_trap.py       Liquidity Trap + Rolling Base + Void/Lift mechanism
│   └── strategy_c_panic.py      Panic Selling / Put Unwind reversal
│
├── execution_bridge/
│   ├── base_broker.py           BaseBroker ABC, MockBroker, BROKER_REGISTRY, OrderRequest/Fill
│   ├── execution_router.py      Fan-out signal -> all clients x brokers (asyncio.gather)
│   ├── broker_shoonya.py        Shoonya/Finvasia (NorenRestApiPy) - self-registers
│   ├── broker_fyers.py          Fyers API v3 (fyers-apiv3) - self-registers
│   ├── broker_angel.py          Angel One SmartAPI (smartapi-python + pyotp) - self-registers
│   └── broker_dhan.py           Dhan HQ (dhanhq) - self-registers
│
├── management/
│   ├── client_manager.py        Subscribes ORDER_FILL; enforces drawdown limits; auto-halts
│   └── admin_console.py         Async REPL (asyncio.to_thread stdin) for runtime ops
│
├── backtester/
│   └── historical_core.py       Event-driven backtester on recorded Parquet data
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

| Topic            | Publisher              | Subscribers                    | Payload Type     |
|------------------|------------------------|--------------------------------|------------------|
| `index_tick`     | GlobalFeeder           | CandleCache, TickRecorder      | `IndexTick`      |
| `option_tick`    | GlobalFeeder           | OptionMatrixEngine, Recorder   | `OptionTick`     |
| `candle_close`   | CandleCache            | (diagnostic/debug only)        | `CandleEvent`    |
| `matrix_snapshot`| CandleCache / OptionMatrix | ConfluenceEngine           | `TechSnapshot` / `ChainSnapshot` |
| `signal`         | ConfluenceEngine       | ExecutionRouter                | `SignalPackage`  |
| `order_fill`     | ExecutionRouter        | ClientManager                  | `OrderFill`      |
| `system_event`   | Any                    | AdminConsole, logging           | `dict`           |

**Backpressure policy**: if a subscriber queue is full (maxsize=20,000), the event is
DROPPED (never blocked). The drop count is tracked per topic and visible via the
AdminConsole `drop_counts` command.

---

## Data Flow — Live Tick to Order

```
WebSocket (broker)
       |
       v
GlobalFeeder.run()                        [data_layer/global_feeder.py]
  normalise raw frame -> IndexTick / OptionTick
  bus.publish("index_tick" / "option_tick", tick)
       |
       +----> TickRecorder                [data_layer/tick_recorder.py]
       |       asyncio.to_thread -> pyarrow.parquet write
       |
       v
CandleCache.run()                         [matrix_engine/candle_cache.py]
  bucket ticks into OHLCV per timeframe (5m, 15m, 75m)
  on bucket close:
    compute TechSnapshot (RSI, VWAP, ADX, EMA, ATR) via indicators.py
    bus.publish("matrix_snapshot", TechSnapshot)
       |
       v
OptionMatrixEngine.run()                  [matrix_engine/option_matrix.py]
  update ChainRow (OI, doi, IV, LTP) per option tick
  recompute PCR, max-pain, max-OI strikes
  bus.publish("matrix_snapshot", ChainSnapshot)
       |
       v
ConfluenceEngine.run()                    [strategies/base_strategy.py]
  hold latest TechSnapshot + ChainSnapshot per underlying
  when BOTH are fresh:
    for each strategy: evaluate(tech, chain) -> Optional[SignalPackage]
    directional conflict check (LONG + SHORT -> discard all)
    min_rr >= 2.0, min_confidence >= 0.50
    bus.publish("signal", best_signal)
       |
       v
ExecutionRouter._dispatch()               [execution_bridge/execution_router.py]
  for each tradeable client x enabled broker:
    translate InternalSymbol -> broker symbol (SymbolTranslator)
    compute lot size (risk-based)
    await broker.place_order(req)  ─┐ asyncio.gather
    await broker.get_order_status()─┘ concurrent per client
    bus.publish("order_fill", fill)
       |
       v
ClientManager.run()                       [management/client_manager.py]
  update daily P&L
  check drawdown limit
  auto-halt breached clients
  bus.publish("system_event", HALT) if needed
```

---

## Multi-Broker Architecture

### BROKER_REGISTRY (execution_bridge/base_broker.py)

Self-registration pattern — each `broker_*.py` module adds its factory at import time:

```python
# broker_shoonya.py (last line)
BROKER_REGISTRY["shoonya"] = lambda b, cid: ShoonyaBroker(b, cid)
```

The `execution_bridge/__init__.py` imports all four broker modules, triggering registration.
`create_broker(binding, client_id)` looks up the factory by `binding.provider`.

### Symbol Translation (data_layer/symbol_translator.py)

All strategies produce `InternalSymbol(underlying, strike, option_type, expiry)`.
`ExecutionRouter._translate()` calls the appropriate static method per binding provider:

| Provider  | Example Output             | Format                               |
|-----------|---------------------------|--------------------------------------|
| Shoonya   | `NIFTY28MAY26C22000`       | `{SYMBOL}{DD}{MON}{YY}{C/P}{STRIKE}` |
| Fyers     | `NSE:NIFTY2652822000CE`    | `{EX}:{SYMBOL}{YY}{M_CODE}{DD}{STRIKE}{CE/PE}` |
| Angel One | `NIFTY28MAY2422000CE`      | `{SYMBOL}{DD}{MON}{YY}{STRIKE}{CE/PE}` |
| Dhan      | security_id lookup key     | Pre-fetched from instrument CSV      |

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
  provider: "shoonya" | "fyers" | "angelone" | "dhan" | "mock"
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

IDLE ──(OI spike + price near base)──> SETUP
SETUP ──(reversal candle confirmed)──> CONFIRMED  ──> SIGNAL
SETUP ──(price runs 2×ATR past level)──> VOID
VOID ──(price retraces to entry_level + tolerance)──> CONFIRMED  ──> SIGNAL (Void Lift)
Any ──(EOD or opposing signal)──> IDLE

Rolling Base:
  Any candle closing BELOW previous candle: rolling_base = min(rolling_base, c_low)
  This ensures the trap level tracks the weakest low dynamically.
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

---

## Backtester

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

**Cost model** (mirrors live execution): STT 0.0625% (sell side), exchange 0.035%,
SEBI 0.0001%, GST 18% on brokerage+exchange, flat ₹20 brokerage per leg.

---

## Session Lifecycle

```
09:00 IST  pre_open_connect  GlobalFeeder: connect WebSocket, download instrument masters
09:15 IST  market_open       CandleCache, OptionMatrix, Strategies all start processing
           ClientManager      reset_all_daily() — clear P&L, trade counters, unhalt
15:25 IST  near_close        Backtester: force EOD-exit all open positions
15:30 IST  market_close      GlobalFeeder: unsubscribe, flush all buffers
15:45 IST  eod_cleanup       TickRecorder: rotate Parquet files, rename with date stamp
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
7. Handle the new provider case in `execution_bridge/execution_router.py::_translate()`

---

## Adding a New Strategy

1. Create `strategies/strategy_d_{name}.py`
2. Subclass `BaseStrategy`, implement `evaluate(tech, chain, all_tf)`
3. Return `None` (no signal) or a fully-populated `SignalPackage`
4. Register in `main.py`:
   ```python
   from strategies.strategy_d_name import StrategyD_Name
   strategies = [..., StrategyD_Name(cfg)]
   ```
5. Add `"D"` to `ClientProfile.enabled_strategies` for clients that should trade it

---

## Key Design Invariants

- **No `time.sleep` anywhere** — all yielding via `asyncio.wait_for(..., timeout=1.0)`
- **All datetimes are IST-aware** — `datetime.now(IST)`, never `datetime.utcnow()`
- **Strategy layer is sync** — `evaluate()` must return fast; no I/O, no network calls
- **Broker layer is async** — all broker methods are `async def`, wrapped with `asyncio.to_thread`
- **EventBus drops over blocks** — slow consumers cause drops, not backpressure on publishers
- **Credentials never logged or persisted** — only `binding.mask()` dict is safe to log
- **SignalPackage is frozen** — strategies cannot mutate a signal after creation
