# Delta Exchange (Crypto) Plug-and-Play Integration — Plan

Branch: `feat/delta-crypto-integration` (off `master`). Goal: add Delta Exchange BTC/ETH **daily**
options as a first-class market alongside NSE/BSE, keeping the **strategy layer 100% market-agnostic**
(it already only consumes neutral `IndexTick`/`OptionTick` + `OrderRequest`/`OrderFill` and never
imports a broker SDK). We extend the EXISTING abstractions — we do NOT add a parallel set.

## Core rule (already true, must stay true)
Strategies interact only with: `data_layer/base_feeder.py` events (EventBus) and
`execution_bridge/base_broker.OrderRequest/OrderFill`. Adapters translate to/from exchange strings via
`UniversalOptionMapper`. No `delta-exchange`/`upstox`/`fyers` import may appear under `strategies/`.

## Key market differences handled
| | NSE/BSE | Delta (crypto) |
|---|---|---|
| Suffix | `CE`/`PE` | `C`/`P` |
| Symbol | `NIFTY2661822000CE` | `BTC-12JUN26-70000-C` |
| Expiry | weekly/monthly, 15:30 IST | **DAILY 24/7/365, 17:30 IST (12:00 UTC)** |
| Rollover | calendar-driven | **hot-swap at 17:30 IST every day** |
| Product | MIS/NRML | margin + **leverage** |

---

## Stages

### ✅ Stage 1 — pure normalization + rollover engine (DONE, tested)
- `data_layer/universal_option_mapper.py` — `UniversalOptionMapper`: `to_short_type`/`to_internal_type`
  (CE/PE↔C/P), `to_delta_symbol`/`parse_delta_symbol` (round-trips `BTC-12JUN26-70000-C`),
  `active_daily_expiry`/`next_rollover_at`/`seconds_to_next_rollover` (17:30 IST), `build_internal`.
- `tests/data_layer/test_universal_option_mapper.py` — 8 tests, timezone-aware. Non-breaking.

### Stage 2 — Delta feeder + broker adapters (needs Delta API docs/key)
- `data_layer/delta_feeder.py` — `DeltaFeeder(BaseFeeder)` using `aiohttp` WS (`v2_ticker` / l2),
  ping-pong heartbeat, parse → publish neutral `OptionTick`/`IndexTick` on the EventBus (same Topics
  the strategies already drain). Reuse the dedup/forward-fill patterns from `global_feeder.py`.
- `execution_bridge/broker_delta.py` — `DeltaBroker(BaseBroker)` registered in `BROKER_REGISTRY`
  (`BROKER_REGISTRY["delta"]`), HMAC-SHA256 signed REST (`api-key`/`signature`/`timestamp`),
  `authenticate`/`place_order`/`cancel_order`/`get_order_status`/`get_positions`/`get_funds`.
  Crypto-specific error handling (rate-limit 429, margin/leverage rejects).
- **Profile + funds + leverage on connect** (`get_profile()`, `get_funds()`, `get_leverage()`,
  `set_leverage()`), surfaced to the UI. Generalize `get_funds()`/`get_profile()` across ALL brokers
  where the broker exposes them (per user note).
- Same API key for data + orders (user: testing with small funds).

### Stage 3 — daily rollover hot-swap worker
- A timezone-aware task: at `seconds_to_next_rollover()==0` (17:30 IST), `unsubscribe_symbols(dead)`
  → compute next front-day strikes via `UniversalOptionMapper` → `subscribe_symbols(new)` — no app
  restart, indicator state preserved. Wire into the feeder lifecycle (not a busy 0.5s loop — sleep on
  `seconds_to_next_rollover`).

### Stage 4 — config, factory/DI, UI
- `config/global_config.py` — `ExchangeConfig` entry for DELTA (strike step, lot/contract size,
  rollover rule); `EXCHANGE`/per-binding exchange selector.
- Factory: `create_broker(provider)` already does registry lookup; add `delta`. Feeder factory selects
  Delta vs Dual by config.
- UI: Delta as a broker option in the client portal; **leverage control** (read current + set if Delta
  allows); **funds/profile panel** populated on connect; admin exchange config. Reuse the broker-card
  + source-IP patterns.

## Verification
- Stage 1: unit tests (done). Stage 2: mock-server unit tests for parse/auth-signature + a live
  smoke test against the Delta testnet/small-fund key. Stage 3: simulate a 17:30 boundary, assert
  unsubscribe→subscribe swap. Stage 4: connect a Delta key in the UI → profile/funds/leverage render;
  a mock strategy places a trade unchanged when EXCHANGE flips Fyers→Delta (decoupling proof).

## What I need to start Stage 2
Delta Exchange API docs (REST base, auth signing spec, WS channel + symbol/product format, leverage
endpoint) and a small-fund test key. Until then, Stage 1 stands alone and breaks nothing.
