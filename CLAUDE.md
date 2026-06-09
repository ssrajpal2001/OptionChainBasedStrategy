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
python run_system.py --mode live --ui --port 5000 --index NIFTY

# Paper trading
python run_system.py --mode paper --ui --port 5000

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

> ⚠️ The bullet list above is the original **v1** skeleton (5-stage `_Phase` machine, index-candle driven). **Live behaviour is v2 — per-leg seller-trap detectors driven off the OPTION PREMIUM.** v1's `_on_candle`/`_check_touch_trigger` are gutted to an EOD guard only. See below.

### TrapTrading — v2 current behavior, knowledge graph & workflow (AUTHORITATIVE)

**Two codebases in one file.** v1 (`_TrapState`/`_Phase`, `_fire_entry`, `_force_exit_all`, `telemetry_snapshot`, rolling-base, trap-levels) is the legacy 5-stage machine and is *no longer the trading path*. **v2** (per-leg `SellerTrapDetector` HTF+MTF on the premium) is what trades.

**Detector (`strategies/trap_seller_detection.py`, pure/side-effect-free):** models a seller trap on a premium series against the **newest** reference candle (LIFO; `active_level` = last `on_candle`). `WATCH →(price < candle low)→ SELLERS_IN →(price > candle high)→ TRAPPED →(price returns ≤ low)→ ENTRY_READY`. `consume_entry()` clears only the `entry_ready` flag (state stays); `invalidate_active()` pops the level (**never called in v2** — see gaps).

**v2 workflow (NIFTY etc., per instrument):**
1. **Day-strike lock** (`_lock_day_strikes`→`_compute_day_strikes`, warm-start bg task): `ATM=round((prev_open_day_high+prev_low)/2, step)`; DTE→ITM offset via `dte_offset_ladder` config (fallback `min(max(dte-1,0),5)` steps). Track **CE=ATM−offset**, **PE=ATM+offset** (ITM, day-fixed). Source: DB 1m bars → else Upstox historical daily (`_fetch_prev_day_hl_upstox`). Strike math in pure `strategies/trap_strike_selection.py`.
2. **Subscribe+pin** (`_ensure_subscribed_legs`): tracked CE/PE are deep-ITM (outside rebalancer ATM window) → engine subscribes + **pins** them; re-asserted every 60s on index tick (survives feeder swap/reconnect → no frozen LTP). `ticks/min=0` in the heartbeat = subscription/feed problem.
3. **Seed** (`_seed_leg_detection`): replays each leg's intraday 1m Upstox history → HTF candles into the HTF detector so trap state is warm at startup. `_seed_legs_from_history` seeds LTP for the panel.
4. **Detect** (`_feed_leg_tick`, per option tick of a tracked leg): advances HTF detector every tick + builds HTF(75m)/MTF(5m) premium candles; **MTF detector only advances while HTF is `ENTRY_READY`** (nested gate). MTF `entry_ready` → `_on_mtf_entry_signal`.
5. **Entry** (`_fire_entry_v2`): execution strike resolved from **LIVE spot** = `exec_strike(spot, ATM±buy_depth ITM)` — *distinct from the detection strike*. Publishes `LONG` `SignalPackage`; subscribes/pins exec contract; stores `self._v2_position`. **One position at a time**: opposite-leg signal **rotates** (`should_rotate` → close runner, open new); same-leg ignored.
6. **Exit — two-tier SL only** (`_v2_track_exec_tick`/`_v2_maybe_stop`): `sl_5m` starts at entry premium, trails **down** to the entry 5m candle low while in-bucket then freezes; once a **1m closes below sl_5m**, that 1m low becomes `sl_active`. Exit `ltp < ref`. Rotation/SL exit → `_v2_publish_exit` (SHORT to close).

**Config** (`config/global_config.py` `trap_engine`): `HTF_MINUTES=75`, `MTF_MINUTES=5`, `SL_MODE` (dynamic|structural), `SL_PCT=2.0`, `SL_BUFFER_PCT=0.3`, `ENTRY_CUTOFF_TIME=14:45`. Per-index UI overrides via `RuntimeConfig.index_section(idx,"trap_trading")`: `dte_offset_ladder`, `buy_depth`, `lookback_days`. EOD: NSE 15:30 / MCX 23:30 (`_market_close_for`).

**Knowledge graph (file connections):**
`run_system.py` → `TrapTradingEngine(bus,cfg,client_db)` + `set_feeder`/`set_rebalancer` + `warm_start`. Engine ← `Topic.INDEX_TICK` (spot cache, lazy day-lock, 60s re-subscribe, heartbeat), `Topic.OPTION_TICK` (premium cache, `_feed_leg_tick`, exec-tick SL), `Topic.CANDLE_CLOSE` (EOD guard only). Engine → `Topic.SIGNAL` (`SignalPackage` LONG/SHORT) → `execution_router`. Pure helpers: `trap_seller_detection.py` (state machine), `trap_strike_selection.py` (strikes). Persistence: `data_layer/position_store` (v1 only). Logs: `logs/clients/tt_{UND}_{date}.log` (per-symbol heartbeat + Below→Above→Return transitions).

**⚠️ Known gaps in v2 (the lifecycle was never re-wired to `self._v2_position`):**
1. **EOD does NOT square off a v2 trade** — `_force_exit_all` iterates v1 `_states`/`_open_positions` only; `_v2_position` rides through 15:30. *(critical)*
2. **No profit-target/MITIGATE exit** — v2 exits only on SL / rotation / (intended) EOD.
3. **No v2 persistence** — `_v2_position` is in-memory; a restart loses the live trade (no SL, no square-off).
4. **`ENTRY_CUTOFF_TIME` not enforced in v2** — `_fire_entry_v2` has no cutoff (v1 `_fire_entry` did).
5. **Dashboard `telemetry_snapshot()` shows v1 only** — live v2 position is invisible.
6. **Detector level lifecycle incomplete** — `invalidate_active()` never called; HTF stays `ENTRY_READY` forever, no v2 equivalent of v1 `_reset_to_next_level` re-arming.

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

> ⚠️ The bullet list above is the original skeleton. **Current behavior is rule-builder driven & dynamic** — see the section below.

### SellStraddle — Current behavior & ops (2026-06, AUTHORITATIVE)
- **Everything is dynamic** — entry/exit conditions, indicators (`CLOSE`/`VWAP`/`SLOPE`/`RSI`/`ROC`), operators, values, and each rule's **timeframe** are set per deployment in the UI rule-builder before the terminal starts. Numbers above are illustrative only. Client guide: `docs/STRATEGY_CLIENT_GUIDE.md`.
- **VWAP = broker ATP** (never computed). The `PoolIndicatorEngine` (`strategies/pool_indicator_engine.py`) keeps a continuous per-(strike,side) 1-min (ltp,atp) series for every subscribed pool strike; VWAP/SLOPE use LIVE bars only (seeds warm RSI/ROC). `pair_indicators_tf` resamples clock-aligned.
- **Per-rule timeframe**: each rule read at its own tf; the rule SET is evaluated once per its MAX-tf boundary **+5s**, tick-driven (no `time.sleep`). Tick-based exits (TSL, vwap-rise%, LTP-decay, ratio, day%, EOD) stay every-tick.
- **Entry**: hybrid — BEGINNING (`select_balanced_pair`) is first-trade-of-day; on a warm block flips to RE-ENTRY (`scan_pool`, balanced N×N) for the day. Gated on Terminal ON + Trade ON.
- **vwap_rise = single-side ROLL** of the less-burning leg (not full exit). Its VWAP is read STRICTLY from the pool engine for the exact open pair (+sanity bound, skip if a leg isn't warm) so a post-roll stale/half ATP can't poison `session_min_vwap`. **Staleness guard: the whole vwap_rise step is SKIPPED — no `session_min_vwap` read/update — if either leg's broker ATP hasn't ticked within `vwap_stale_sec` (default 90s, per-index overridable), so a frozen illiquid leg (e.g. CRUDEOIL PE forward-filled) can't set a low baseline that later normal reads "rise" above → kills false vwap_rise churn. `PoolIndicatorEngine.pair_atp_fresh(ce,pe,max_sec)` (stamps `_last_atp_ts` only on a real atp>0 tick); EXIT-EVAL `exit_ind_by_tf[1]['stale']` + VWAPrise crit show STALE-skip.** Every roll re-baselines `session_min_vwap=inf` + scalable-TSL anchor; single-side roll skips same-strike no-op rolls. **Rollover partner rule (`select_partner_for`, all single-side rolls — ltp_decay/ratio/vwap_rise): keep the losing/expensive leg, roll the cheap/decayed leg; the new partner must be in ATM±offset, ≥ ltp_target, pass the re-entry rule, and STRICTLY ≤ the kept leg's LTP (never roll into a richer leg), then most-balanced (closest to kept LTP) among those; none → close both → fresh.** This cap governs ALL single-side rolls incl. the **scalable-TSL partial** roll (routed through `_single_side_roll`). The only non-capped case is the scalable-TSL **physical** roll (BOTH legs change → no single kept leg → it's a fresh `scan_pool` balanced pair; `virtual` = re-mark, no trade).
- **Live fill price integrity (2026-06-10 fix)**: `broker.place_order()` returns the broker ORDER-ID **string** (not a fill object). `straddle_bridge` was doing `fill.avg_price` on that string → `'str' object has no attribute 'avg_price'` on EVERY live leg → it fell back to recording the STRATEGY LTP as the fill, so dashboard/history P&L diverged from the real Zerodha order book. Fixed to `order_id = place_order(req); order_fill = get_order_status(order_id); avg = order_fill.avg_price` (mirrors `ic_bridge`), guard `avg>0 else fallback_ltp`. Also passes the live LTP as `OrderRequest.price` so MockBroker (paper/demo) fills the real premium, not its 100.0 default. (Live brokers ignore price on MARKET orders.)
- **Entry-price integrity (c729394)**: `_on_fill` never overwrites a leg's `entry_price` with a 0/missing fill (and re-persists the confirmed entry so restarts keep it); `_close_leg` books `pnl=0` (not a phantom `(0-ltp)*qty`) if a leg's entry is ever lost — so a 0-entry can't pollute history or falsely trip `day_loss_sl`. (Root of the old −32360 ghost loss after mid-position restarts.)
- **Multi-tenant**: per-underlying strategy, one logical position mirrored to all engine-active brokers. **Trade/Terminal OFF squares off only that client-broker's legs** (`bridge.square_off_binding`). Product type (MIS/NRML/carry) is client-selected per deployment (binding) and overrides the strategy default.
- **History**: recorded on EXIT per leg, filtered to `ev.legs` (so a single-side roll records ONLY the rolled leg — no dupes), with per-leg `open_time`/`close_time`/`open_reason` threaded via the order event into `trade_history`. UI History (`monitor.html`) is an **order-book event ledger**: each leg → a `SELL` (open: time+price) row + a `BUY` (close: time+price+P&L) row, **strictly time-sorted newest-first**, **paginated 10/page**, junk `0.00`-price rows hidden, and a `reasonLabel()` maps codes to human text (roll-out / roll-in / "no pair → closed all (fresh)" / beginning / re-entry / EOD …). Tools: `scripts/dedupe_history.py` (collapse legacy duplicate records), `scripts/backfill_entry_ts.py` (fill open/close times into pre-fix records from `logs/trades/`).
  - **Data caveat:** records written before commit `c2eae5d` logged BOTH legs on every exit, so old ledger rows can show a *kept* leg as closed+reopened at a roll (artifact). New records are clean: a kept leg shows ONE open (its true entry) + ONE close (its true exit); only the rolled leg changes at a roll.
- **Phase 2 add-ons (2026-06-10, SHIPPED):**
  - **2a — day-wise exit basis**: per-weekday `ss["per_day"][weekday]["exit_basis"]` = `"ltp"` (legacy) | `"theta"`. Theta = simple intrinsic time-value decay (`strategies/theta_calc.py`, NOT Black-Scholes). Read into `self._day_exit_basis`; the day-% guardrail uses `pos.theta_decay_pct(spot)` when theta, else `(realized+running)/credit`. UI: per-day **Exit Basis dropdown** in the PER-DAY admin grid (`monitor.html`), round-trips via `set_index_section`.
  - **2b — granular tick-by-tick exit audit**: admin per-client toggle `broker_bindings.show_granular_ticks` (DB col + `set_show_granular_ticks` + `get_bindings_safe_sync`). Admin endpoint `POST /api/admin/client/{cid}/binding/{bid}/granular_ticks`; admin UI button in client-profiles ACTIONS (`toggleGranular`). Strategy publishes `Topic.EXIT_AUDIT` (the `_crit` criteria list + `exit_ind_by_tf` dump) ONLY when `_granular_audit_clients()` is non-empty (gate). `ws_bridge._exit_audit_loop` forwards verbatim; `monitor.html` `_handle` filters `exit_audit` → `exitAudit{}`, shown in a collapsible panel on the live straddle card (`_auditFor`).
  - **2c — client 1-min premium chart**: `self._chart_series` deque (ts, combined, ce/pe_ltp, vwap, rsi, slope) appended per 1-min close, cleared on `reset_session`, exposed via `get_premium_series()`. Endpoint `GET /api/client/strategy/{deploy_id}/premium_series` (underlying = last `_`-token). UI: Chart.js CDN; collapsible chart in the client deployment card (`togglePremiumChart`/`_renderChart`): combined+VWAP main panel, RSI + SLOPE subpanels, 30s live refresh.
- **Ops**: `python run_system.py --mode live --ui --index <IDX> --strategies sell_straddle`. `scripts/fresh_start.sh <IDX>` pulls + WIPES positions/history/logs + restarts (skip if preserving data; plain `git reset --hard` never touches gitignored `data/`). `pm2 restart` reuses old args — use fresh_start / explicit `pm2 start` to change `--index`/`--strategies`. HTTPS broker callbacks on a raw EC2 IP: `scripts/setup_https.sh` (Caddy + sslip.io). **Footguns**: MCX `squareoff_time` must be ~23:25 (15:15 default instantly EOD-exits MCX); NIFTY lot=75 (65 rejected); MCX needs Zerodha single-ledger activation.

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
