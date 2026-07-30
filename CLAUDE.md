# OptionChain AlgoTrader — CLAUDE.md

Complete codebase reference for Claude Code. Updated after each major phase.

> **CURRENT FOCUS (2026-07-30):** This project is **ONLY** working on two strategies:
> 1. **SellStraddle** — theta-decay option seller (mature, live in production)
> 2. **D1 Trap FnO/Index** — zone-based option buyer (active development, next session continues here)
>
> Do NOT suggest, implement, or discuss any other strategies. All new work belongs to
> one of these two. When starting a new session, read the D1 Trap section below first.

---

## Project Overview

NSE/BSE options algorithmic trading system with:
- Multi-tenant client lifecycle management
- Real-time option chain ingestion (Upstox + Fyers dual-feed)
- Shared global data feed server (TCP broadcast hub)
- SellStraddle strategy engine + D1 Trap FnO/Index option buyer (both live)
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
│   ├── sell_straddle.py       SellStraddleStrategy — ATM straddle/strangle premium decay
│   └── d1_trap_option/
│       ├── book.py            D1TrapOptionBook — per-(client,binding,underlying) zone engine
│       └── book_manager.py    D1TrapOptionBookManager — spawns books; WATCHLIST + ALL_FNO sentinels
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
               │ (ATM track) │ │ Engine │ │ SellStraddleStrat. │
               └─────────────┘ └──┬─────┘ └──────────┬─────────┘
                                   │                  │
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

> **2026-07-18:** TrapTradingEngine, TrapScanner (`strategies/trap_scanner/`), and
> IronCondorStrategy were removed entirely — code, tests, backtest/research scripts,
> registry entries, and all `run_system.py`/dashboard wiring. Iron Condor and the old
> TrapScanner are gone; do not resurrect deleted v1/v2 trap code as a starting point
> (recoverable via `git log` pre-2026-07-18 if ever needed).
>
> **2026-07-23+:** D1 Trap (`d1_trap_fno` / `d1_trap_index`) rebuilt from scratch as a
> zone-based option BUYER strategy in `strategies/d1_trap_option/`. This is NOT related
> to the deleted TrapScanner. Both **SellStraddle and D1 Trap are now live**.

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
- **vwap_rise = single-side ROLL** of the less-burning leg (not full exit). Its VWAP is read STRICTLY from the pool engine for the exact open pair (+sanity bound, skip if a leg isn't warm) so a post-roll stale/half ATP can't poison `session_min_vwap`. **Staleness guard: the whole vwap_rise step is SKIPPED — no `session_min_vwap` read/update — if either leg's broker ATP hasn't ticked within `vwap_stale_sec` (default 90s, per-index overridable), so a frozen illiquid leg (e.g. CRUDEOIL PE forward-filled) can't set a low baseline that later normal reads "rise" above → kills false vwap_rise churn. `PoolIndicatorEngine.pair_atp_fresh(ce,pe,max_sec)` (stamps `_last_atp_ts` only on a real atp>0 tick); EXIT-EVAL `exit_ind_by_tf[1]['stale']` + VWAPrise crit show STALE-skip.** Every roll re-baselines `session_min_vwap=inf` + scalable-TSL anchor; single-side roll skips same-strike no-op rolls. **Rollover partner rule (`select_partner_for`, all single-side rolls — ltp_decay/ratio/vwap_rise): keep the losing/expensive leg, roll the cheap/decayed leg; the new partner must be in ATM±offset, ≥ ltp_target, pass the re-entry rule, and STRICTLY ≤ the kept leg's LTP (never roll into a richer leg), then most-balanced (closest to kept LTP) among those; none → close both → fresh.** This cap governs ALL single-side rolls. **scalable-TSL roll (2026-06-11 fix): now ALSO a single-side roll** — keeps the losing/expensive leg, rolls only the decayed/cheaper leg via `_single_side_roll` (was a `_try_smart_roll` physical roll that closed BOTH legs into a fresh deep-ITM `scan_pool` pair; `_try_smart_roll`/`classify_roll` are now dead code). Re-entry strikes are capped near ATM by `roll_max_itm_steps` (config, default 2) in `select_partner_for` so a roll can't sell a deep-ITM strike. **Cooldown** (`sl_cooldown_minutes`, direct minutes) fires on every FULL exit incl. roll-with-no-partner; a single-side roll keeps a leg → no cooldown.
- **Live fill price integrity (2026-06-10 fix)**: `broker.place_order()` returns the broker ORDER-ID **string** (not a fill object). `straddle_bridge` was doing `fill.avg_price` on that string → `'str' object has no attribute 'avg_price'` on EVERY live leg → it fell back to recording the STRATEGY LTP as the fill, so dashboard/history P&L diverged from the real Zerodha order book. Fixed to `order_id = place_order(req); order_fill = get_order_status(order_id); avg = order_fill.avg_price` (mirrors `ic_bridge`), guard `avg>0 else fallback_ltp`. Also passes the live LTP as `OrderRequest.price` so MockBroker (paper/demo) fills the real premium, not its 100.0 default. (Live brokers ignore price on MARKET orders.)
- **Entry-price integrity (c729394)**: `_on_fill` never overwrites a leg's `entry_price` with a 0/missing fill (and re-persists the confirmed entry so restarts keep it); `_close_leg` books `pnl=0` (not a phantom `(0-ltp)*qty`) if a leg's entry is ever lost — so a 0-entry can't pollute history or falsely trip `day_loss_sl`. (Root of the old −32360 ghost loss after mid-position restarts.)
- **Multi-tenant — PER-BINDING (2026-06-11, branch `feat/per-binding-straddle`):** SellStraddle is now **one independent book per `(client, binding, index)` deployment** (`strategies/straddle_book_manager.py` `StraddleBookManager`), NOT one shared per-index position. Each book has its OWN beginning entry (anchored to when THAT terminal turns ON), strikes, rolls, exits, position, persistence key (`{client}_{binding}_{und}_sell_straddle`), log file (`ss_{UND}_{client}_{binding}_{date}.log`), and gating (only its binding's Terminal+Trade). Orders carry `client_id`/`binding_id`; the bridge routes to **ONLY that broker** (no mirror) — `StraddleOrderEvent` tags + `_emit_order`. Manager reconciles every 5s → **auto-spawns a book on deploy**, stops it on un-deploy. `run_system` runs the manager as a task; dashboard `_find_ss_book(cid,bid,und)` + `_sell_straddles` property read live books; `square_off_binding` filters to the matching binding. **Feed is shared per index** (books read the same EventBus ticks; each keeps its own pool engine — duplicated indicator CPU, zero extra feeder load; shared-per-index context extraction deferred until >~20–30 bindings). Plan/status: `docs/PER_BINDING_REFACTOR_PLAN.md`. **(Pre-refactor master: one shared position mirrored to all brokers — a client joining mid-position inherited it; that's the bug this fixes.)** Product type (MIS/NRML/carry) is client-selected per deployment.
- **History**: recorded on EXIT per leg, filtered to `ev.legs` (so a single-side roll records ONLY the rolled leg — no dupes), with per-leg `open_time`/`close_time`/`open_reason` threaded via the order event into `trade_history`. UI History (`monitor.html`) is an **order-book event ledger**: each leg → a `SELL` (open: time+price) row + a `BUY` (close: time+price+P&L) row, **strictly time-sorted newest-first**, **paginated 10/page**, junk `0.00`-price rows hidden, and a `reasonLabel()` maps codes to human text (roll-out / roll-in / "no pair → closed all (fresh)" / beginning / re-entry / EOD …). Tools: `scripts/dedupe_history.py` (collapse legacy duplicate records), `scripts/backfill_entry_ts.py` (fill open/close times into pre-fix records from `logs/trades/`).
  - **Data caveat:** records written before commit `c2eae5d` logged BOTH legs on every exit, so old ledger rows can show a *kept* leg as closed+reopened at a roll (artifact). New records are clean: a kept leg shows ONE open (its true entry) + ONE close (its true exit); only the rolled leg changes at a roll.
- **Phase 2 add-ons (2026-06-10, SHIPPED):**
  - **2a — day-wise exit basis**: per-weekday `ss["per_day"][weekday]["exit_basis"]` = `"ltp"` (legacy) | `"theta"`. Theta = simple intrinsic time-value decay (`strategies/theta_calc.py`, NOT Black-Scholes). Read into `self._day_exit_basis`; the day-% guardrail uses `pos.theta_decay_pct(spot)` when theta, else `(realized+running)/credit`. UI: per-day **Exit Basis dropdown** in the PER-DAY admin grid (`monitor.html`), round-trips via `set_index_section`.
  - **2b — granular tick-by-tick exit audit**: admin per-client toggle `broker_bindings.show_granular_ticks` (DB col + `set_show_granular_ticks` + `get_bindings_safe_sync`). Admin endpoint `POST /api/admin/client/{cid}/binding/{bid}/granular_ticks`; admin UI button in client-profiles ACTIONS (`toggleGranular`). Strategy publishes `Topic.EXIT_AUDIT` (the `_crit` criteria list + `exit_ind_by_tf` dump) ONLY when `_granular_audit_clients()` is non-empty (gate). `ws_bridge._exit_audit_loop` forwards verbatim; `monitor.html` `_handle` filters `exit_audit` → `exitAudit{}`, shown in a collapsible panel on the live straddle card (`_auditFor`).
  - **2c — client 1-min premium chart**: `self._chart_series` deque (ts, combined, ce/pe_ltp, vwap, rsi, slope) appended per 1-min close, cleared on `reset_session`, exposed via `get_premium_series()`. Endpoint `GET /api/client/strategy/{deploy_id}/premium_series` (underlying = last `_`-token). UI: Chart.js CDN; collapsible chart in the client deployment card (`togglePremiumChart`/`_renderChart`): combined+VWAP main panel, RSI + SLOPE subpanels, 30s live refresh.
- **v2 multi-client hardening (2026-06-12, branch `feat/per-binding-straddle`):**
  - **Reconcile gates on `is_running`**: `StraddleBookManager` only spawns a book when its deployment's per-strategy Run toggle is ON (`is_running=1`); toggling OFF stops+removes it; a `lot_multiplier` change re-spawns (only while flat). Fixes "re-selected/already-ticked deployment never trades" / "no trade on deploy."
  - **`lot_multiplier` propagated** deployment→book → fixes BOTH "lot not changing" (qty = lot_size × multiplier) and "scalable-TSL not scaling >1 lot" (same root — staircase already multiplied, never got the real multiplier).
  - **STRICT per-binding identity** in `square_off_binding` (no legacy empty-identity fallback) — terminal-off / per-broker / kill / global square-off each touch ONLY their own `(client,binding)` book → fixes "terminal off flattened ssrajpal2001 but not gurmeet" + cross-client bleed.
  - **Kill Broker** squares off own legs + stops deployments BEFORE halting.
  - **Paper = REAL order + local sim fill**: paper sends the order to the actual client broker (verifies routing / SEBI source-IP whitelist) then books a LOCAL sim-fill at strategy LTP (no-fund reject expected). LIVE books the real fill. Bridge tracks `order_id`s per leg (`self._order_ids`). **Closing places opposite orders for the book's OWN strikes/qty only — never a broker net-flatten** (shared-account safe). Mode change **hot-swaps** the live broker (no restart).
  - **`upsert_binding`** preserves `source_ip` when an edit omits it (was wiped); fixed asymmetric `assigned_instrument` CASE.
  - **Theta ENTRY basis** (`entry_basis`=ltp|theta + `theta_target`): MIN floor on raw LTP or per-leg TIME VALUE (`straddle_selection.leg_entry_value`); threaded into beginning/re-entry/roll; balance stays on LTP; `ltp` basis byte-identical to legacy. **Theta TSL basis** (`tsl_scalable.basis`=ltp|theta): staircase trails time-value decay vs LTP P&L. Theta = `theta_calc.py` intrinsic/time-value, never Black-Scholes.
  - **UI**: PRODUCT TYPE → MIS/NRML toggle (native `<select>` was dark-on-dark); rule rows wrap below `xl` (was `md`) → no 100%-zoom overflow, ✕ reachable; broker-specific **⊗ Square Off** + `/api/client/broker/{id}/squareoff`; ENTRY/TSL basis selectors; positions panel = broker-style ledger (Instrument·Type·Qty·Sell·Buy·LTP·P&L·MTM + TOTAL).
- **Ops**: `python run_system.py --mode live --ui --index <IDX> --strategies sell_straddle`. `scripts/fresh_start.sh <IDX>` pulls + WIPES positions/history/logs + restarts (skip if preserving data; plain `git reset --hard` never touches gitignored `data/`). `pm2 restart` reuses old args — use fresh_start / explicit `pm2 start` to change `--index`/`--strategies`. HTTPS broker callbacks on a raw EC2 IP: `scripts/setup_https.sh` (Caddy + sslip.io). **Footguns**: MCX `squareoff_time` must be ~23:25 (15:15 default instantly EOD-exits MCX); NIFTY lot=75 (65 rejected); MCX needs Zerodha single-ledger activation.

### D1 Trap FnO / Index (`strategies/d1_trap_option/`)

Option **buyer** strategy — detects D1 supply/demand zones on the UNDERLYING spot price,
enters intraday option (CE for LONG, PE for SHORT) on a multi-timeframe cascade:
HTF zone → MTF C2 confirmation → LTF 5M trigger entry.

**Files:** `strategies/d1_trap_option/book.py` (engine), `book_manager.py` (lifecycle)

**Strategy names in DB:** `d1_trap_fno` (FnO stocks, positional NRML) | `d1_trap_index` (indices, intraday MIS)

**Default timeframes per strategy:**
- `d1_trap_fno`: HTF=D1, MTF=75min, LTF=5min — overridable via `strategy_params` JSON
- `d1_trap_index`: HTF=75min, MTF=15min, LTF=5min

**Zone state machine (per `_ZoneMonitor`):**
- `WAITING` — zone detected on HTF; watching for price to enter zone
- `MONITORING` — price bar touched zone (`LONG: bar.low ≤ zone_hi`; `SHORT: bar.high ≥ zone_lo`); first MTF bar after contact becomes `ref_bar`
- C2 trigger — next MTF bar breaks ref bar (`LONG: bar.high > ref.high`; `SHORT: bar.low < ref.low`) → sets `_pending_5m`; LTF 5M entry fires on next 5M confirmation
- `invalid` — MTF bar closes THROUGH zone (`LONG: close < zone_lo`; `SHORT: close > zone_hi`) → zone failed; TWEAK counter-direction setup queued if zone had been MONITORING
- `done` — position entered; monitor consumed

**TWEAK setup:** after zone failure, counter-direction 5M breach of the failure bar triggers entry (SHORT zone failure → LONG TWEAK if next bar's `high > failure_bar.high`).

**Intraday warmup on startup (`_warmup_intraday`):**
Fetches today's 1M bars via Upstox, replays through `_check_zone_contact` + `_on_mtf_close`
so zone states (MONITORING / invalid) are correct on mid-day restart. Gate: `self._warming_up = True`
suppresses C2 triggers and order placement during replay — only zone contact + invalidation run.
- ⚠️ **Upstox intraday API for NSE_EQ stocks only accepts `1minute` or `30minute`** — NOT `5minute` (error UDAPI1076). The warmup fetches `1minute` bars and aggregates into MTF buckets internally.

**WATCHLIST sentinel** (`underlying=WATCHLIST`, `strategy_name=d1_trap_fno`):
- Reads `data/fno_watchlist.json` written by nightly scan
- Nightly command (run after 15:30 IST): `python backtest/fno_scanner/scan_live.py --save --top-n 5`
- Takes `stocks[:top_n]` from pre-sorted file (sorted by `btst_rr` descending) — hard cap of 5 books
- Each JSON entry carries `upstox_key`, `lot`, `step` so stocks not in `FNO_STOCK_CONFIG` work correctly
- Old individual stock deployments in `strategy_deployments` (e.g. AXISBANK with `is_running=1`) will spawn extra books — delete them, keep only the WATCHLIST row

**Dashboard endpoint:** `GET /api/d1trap/zones` → all books' `monitoring_zones()` dict
Fields per book: `underlying`, `client_id`, `binding_id`, `spot`, `zones[]` (each: `direction`,
`zone_lo`, `zone_hi`, `state`, `dist_pct`, `ref_ts`), `pending`, `position`, `total_zones`

**WATCHLIST TRACKER UI (`monitor.html`):**
- Fetches `/api/d1trap/zones` on init + every 30s; filters by `bk.binding_id === b.binding_id`
- Real-time spot via WS: `window.dispatchEvent(new CustomEvent('spot-tick', {detail:{sym,ltp}}))` in `_handle()`; component listens via `window.addEventListener('spot-tick', ...)` → `_liveSpots{}` dict
- Phase derived from `bk.position` / `bk.pending` / `bk.zones[0].state`; Dist% column (amber ≤ 2%)

**Ops / deployment:**
- One `strategy_deployments` row per (client, binding): `underlying=WATCHLIST`, `strategy_name=d1_trap_fno`, `product_type=NRML`, `strategy_params={"htf":"D1","mtf":"75min","top_n":5}`
- `lot_multiplier` sets quantity; book auto-spawns on reconcile when `is_running=1`
- `pm2 restart terminus` after code changes; `git pull` before restart on EC2

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
