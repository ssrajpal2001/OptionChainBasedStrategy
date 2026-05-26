# OptionChain AlgoTrader — CLAUDE.md

Complete codebase reference for Claude Code. Updated after each major phase.

---

## Project Overview

NSE/BSE options algorithmic trading system with:
- Multi-tenant client lifecycle management
- Real-time option chain ingestion (Upstox + Fyers dual-feed)
- Shared global data feed server (TCP broadcast hub)
- Multiple strategy engines (TrapTrading, IronCondor, SellStraddle)
- Risk management with circuit breakers
- Live FastAPI dashboard with WebSocket telemetry
- Headless TOTP authentication for all supported brokers

---

## Launch Commands

```bash
# Strategy bot + dashboard (live mode)
python run_system.py --mode live --ui --port 8080 --index NIFTY

# Paper trading
python run_system.py --mode paper --ui --port 8080

# Demo mode (synthetic ticks, no broker)
python run_system.py --mode demo

# Shared feed server only (for EC2 multi-app setup)
python run_feed_server.py              # mock mode (synthetic)
python run_feed_server.py --dual       # live Upstox + Fyers (reads creds from DB)

# Connect this app to a running FeedServer (instead of own broker connection)
# Set primary_feeder_provider = "shared" in GlobalConfig, or pass --provider shared
```

---

## Module Map

```
OptionChainBasedStrategy/
├── run_system.py              CLI launcher — starts all subsystems
├── run_feed_server.py         Standalone shared data feed server (TCP port 15765)
│
├── config/
│   └── global_config.py       IST, Topic, SysEvent, GlobalConfig, ExchangeConfig
│
├── data_layer/
│   ├── base_feeder.py         EventBus, BaseFeeder ABC, IndexTick, OptionTick, CandleEvent
│   ├── global_feeder.py       GlobalFeeder lifecycle wrapper; DualFeeder; MockFeeder
│   │                          UpstoxFeeder (stub); FyersFeeder (stub)
│   ├── feed_server.py         TCP broadcast hub — fans EventBus ticks to all clients
│   ├── shared_feed_client.py  BaseFeeder subclass — connects to FeedServer over TCP
│   ├── client_db.py           SQLite client/credentials store (XOR-obfuscated secrets)
│   ├── symbol_translator.py   InternalSymbol ↔ broker-format conversion (Upstox, Fyers, etc.)
│   ├── strike_rebalancer.py   ATM tracking; auto-subscribe ±N strikes around ATM
│   ├── strike_cleanup.py      Unsubscribe stale strikes after ATM drift
│   └── tick_recorder.py       Parquet recording of live ticks
│
├── matrix_engine/
│   ├── __init__.py
│   └── gap_handler.py         Gap-open detector; publishes GAP_EVENT to EventBus
│
├── strategies/
│   ├── trap_trading_engine.py TrapTradingEngine — dual-timeframe institutional trap detection
│   ├── iron_condor.py         IronCondorStrategy — OTM 4-leg credit spread
│   └── sell_straddle.py       SellStraddleStrategy — ATM straddle/strangle premium decay
│
├── management/
│   ├── __init__.py            Exports ClientManager, AdminConsole, RiskManager
│   ├── client_manager.py      Multi-tenant client lifecycle (spawn/halt worker per client)
│   ├── admin_console.py       CLI REPL for system control
│   └── risk_manager.py        Portfolio risk engine — drawdown, position limits, circuit breakers
│
├── broker_auth/
│   └── headless_auth.py       HeadlessAuthEngine — TOTP auth for all brokers
│                              Uses curl_cffi (Chrome TLS fingerprint) for Upstox
│
├── execution_bridge/
│   └── execution_router.py    Multi-broker order routing and fill tracking
│
└── ui_layer/
    ├── dashboard_server.py    FastAPI app — REST API + WebSocket broadcast
    ├── ws_bridge.py           EventBus → WebSocket bridge
    └── templates/
        └── monitor.html       Live trading dashboard (Alpine.js + Tailwind CSS)
```

---

## Data Flow

```
                    ┌─────────────────────────────────┐
                    │  FeedServer (run_feed_server.py) │
                    │  port 15765 (TCP)                │
                    │  DualFeeder: Upstox + Fyers      │
                    └──────────────┬──────────────────┘
                                   │  JSON ticks (newline-delimited)
                    ┌──────────────▼──────────────────┐
                    │  SharedFeedClient (BaseFeeder)   │
                    │  OR: UpstoxFeeder / FyersFeeder  │
                    │  OR: MockFeeder (demo/paper)     │
                    └──────────────┬──────────────────┘
                                   │
                    ┌──────────────▼──────────────────┐
                    │  GlobalFeeder (lifecycle wrapper)│
                    └──────────────┬──────────────────┘
                                   │  EventBus.publish(INDEX_TICK / OPTION_TICK)
                    ┌──────────────▼──────────────────┐
                    │  EventBus (asyncio.Queue pub-sub)│
                    └──────┬───────┬────────┬─────────┘
                           │       │        │
               ┌───────────▼─┐ ┌──▼─────┐ ┌▼──────────────────┐
               │ StrikeRebal │ │ Matrix │ │ Strategies         │
               │ (ATM track) │ │ Engine │ │ TrapTradingEngine  │
               └─────────────┘ └──┬─────┘ │ IronCondorStrategy │
                                   │       │ SellStraddleStrat. │
                                   │       └──────────┬─────────┘
                                   │                  │
                    ┌──────────────▼──────────────────▼─────────┐
                    │  ExecutionRouter  (multi-broker orders)    │
                    └──────────────────────────────────────────-─┘
                                   │
                    ┌──────────────▼──────────────────────────────┐
                    │  RiskManager (drawdown / position limits)    │
                    └────────────────────────────────────────────-┘
```

---

## Shared Feed Server Architecture

The `FeedServer` enables a single Upstox + Fyers WebSocket session to serve
multiple strategy processes running on the same EC2 instance (or LAN).

```
EC2 instance
├── run_feed_server.py (one process, always-on)
│   ├── DualFeeder: Upstox WebSocket + Fyers WebSocket
│   ├── Publishes INDEX_TICK + OPTION_TICK to local EventBus
│   └── FeedServer: TCP hub on 0.0.0.0:15765
│       ├── fans ticks to all connected clients
│       └── handles subscribe/unsubscribe/ping/status commands
│
├── run_system.py (provider="shared")
│   └── SharedFeedClient → connects to FeedServer:15765
│       └── converts JSON ticks → IndexTick → local EventBus
│
└── Option_Selling_May_2026/bot (FeedClient → port 15765)
    └── also connects to the same FeedServer
```

**TCP protocol** (identical to Option_Selling_May_2026 FeedServer for interoperability):
- Client → `{"cmd": "subscribe", "instruments": ["NIFTY", "BANKNIFTY"]}`
- Client → `{"cmd": "ping"}`
- Server → `{"type": "tick", "symbol": "NIFTY", "ltp": 24500.0, "ts": 1714486539.0, ...}`
- Server → `{"type": "opt_tick", "symbol": "NIFTY24500CE", "ltp": 150.0, ...}`
- Server → `{"type": "keepalive"}`

---

## Strategy Reference

### TrapTradingEngine (`strategies/trap_trading_engine.py`)
Dual-timeframe institutional trap detection.
- **Trigger**: CANDLE_CLOSE events
- **Logic**: Detects liquidity sweeps (false breakouts above/below recent highs/lows)
  followed by sharp reversal — the "institutional trap" pattern.
- **Timeframes**: Fast (5m) + Slow (15m) confluence required.
- **Indicators used**: EMA crossover, ADX, volume spike.
- **Entry**: On trap confirmation candle close.
- **Exit**: Opposite trap signal, time-based, or stop loss.

### IronCondorStrategy (`strategies/iron_condor.py`)
Neutral market premium collection via 4-leg spread.
- **Trigger**: CANDLE_CLOSE (checks once per candle)
- **Setup**: Short OTM CE + Short OTM PE + Long wing CE + Long wing PE
- **Entry conditions**: RSI 40–60, ADX < 25 (low-volatility range-bound market)
- **Strike selection**: Short strikes at ±1 SD OTM (≈0.20 delta)
  - NIFTY: ±200 pts short, ±200 pts wing; BANKNIFTY: ±400 pts short, ±500 pts wing
- **Max profit**: Net credit received (all legs expire worthless)
- **Max loss**: Wing width − credit (capped by long wings)
- **Exit triggers**:
  1. Profit target: 50% of credit captured
  2. Stop loss: unrealized loss exceeds 200% of credit
  3. Breach: spot crosses short strike → exit that side
  4. Time: 15:15 IST force-exit
- **Status**: Strategy skeleton complete; order routing via ExecutionRouter TODO

### SellStraddleStrategy (`strategies/sell_straddle.py`)
ATM straddle/strangle selling for theta decay. Ported from Option_Selling_May_2026.
- **Trigger**: CANDLE_CLOSE (entry window 09:20–12:00 IST)
- **Setup**: Sell ATM CE + Sell ATM PE (straddle)
- **Entry conditions**: RSI 35–65, ADX < 30
- **Net credit**: CE entry price + PE entry price
- **Exit triggers**:
  1. Profit target: 30% of credit captured
  2. Hard stop: loss = 200% of credit (total debit = 3× original credit)
  3. Trailing SL: activates after 20% profit captured; trails at 10% floor below peak
  4. ROC guardrail: exit if spot moves > 1.5% in a single tick
  5. Time: 15:15 IST force-exit
- **Daily trade limit**: max 1 re-entry per session (configurable)
- **Status**: Strategy skeleton complete; order routing via ExecutionRouter TODO

---

## Key Design Decisions

### EventBus (not callbacks)
The internal EventBus uses `asyncio.Queue` per topic (not async callbacks). This means:
- `bus.subscribe(topic)` returns a `Queue` the consumer drains in its own task
- `bus.publish(topic, event)` is non-blocking (`put_nowait`)
- Slow consumers drop events silently (logged every 1000 drops)

### Headless Authentication (`broker_auth/headless_auth.py`)
- **Upstox**: Uses `curl_cffi` with `impersonate="chrome131"` TLS fingerprint + Chrome 140 headers.
  6-step flow: dialog → OTP generate → TOTP verify → PIN (base64) → OAuth approve → token exchange.
  All HTTP to `service.upstox.com` (not `api.upstox.com` or `login.upstox.com`).
- **Fyers**: `fyers_apiv3.FyersAuthCode.authCodeModel` (requires fyers-apiv3 >= 3.1.0).
- **Others**: Shoonya (NorenAPI), AngelOne (SmartConnect), Dhan (token validation only).
- TOTP secrets are sanitized: stripped of spaces/hyphens, uppercased before `pyotp.TOTP()`.

### Credentials Storage (`data_layer/client_db.py`)
- SQLite at `data/clients.db`
- All secrets XOR-obfuscated via PBKDF2 (`_encode_cred` / `_decode_cred`)
- Two tables: `clients` (trading config) and `feeder_creds` (broker API keys)
- All writes via `asyncio.to_thread()` for non-blocking I/O

### Dashboard (`ui_layer/dashboard_server.py` + `monitor.html`)
- FastAPI + uvicorn (no build step)
- Alpine.js v3 CDN + Tailwind CSS CDN (CDN-only, no npm/webpack)
- Pydantic schemas must be at MODULE LEVEL (not inside functions) due to `from __future__ import annotations`
- All backend errors return `{"ok": false, "error": "..."}` JSON — never raw 500 exceptions
- Kill switch requires 2-click confirm with 5-second window

---

## Required Packages

```bash
# Core
pip install numpy pyarrow zstandard

# Dashboard
pip install fastapi uvicorn[standard]

# Broker auth
pip install pyotp curl_cffi

# Optional broker SDKs
pip install fyers-apiv3 upstox-client dhanhq smartapi-python NorenRestApiPy
```

---

## Environment / Config

All runtime config lives in `config/global_config.py`:
- `GlobalConfig.primary_feeder_provider`: `"mock"` | `"upstox"` | `"fyers"` | `"shared"`
- `GlobalConfig.monitored_indices`: list of index names
- `ExchangeConfig.strike_steps`: per-index strike granularity
- `ExchangeConfig.lot_sizes`: standard lot sizes

Credentials are stored in `data/clients.db` via the dashboard — never in config files or env vars.

---

## Development Notes

- All async I/O: `asyncio` only. No `threading`, no `time.sleep` (use `asyncio.sleep`).
- Blocking operations (SQLite, curl_cffi): wrapped with `asyncio.to_thread()`.
- The `SharedFeedClient` falls back to `FEEDER_DOWN` system event after 3 failed reconnect
  rounds — GlobalFeeder heartbeat will then attempt a provider switch.
- FeedServer broadcasts to all clients unless they send a `subscribe` command;
  after subscribe, only matching symbols are forwarded.
- The `Option_Selling_May_2026` FeedClient (TCP, port 15765) is protocol-compatible with
  this project's FeedServer — both projects can share the same broadcast stream.
