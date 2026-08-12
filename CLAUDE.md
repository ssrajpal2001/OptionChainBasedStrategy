# OptionChain AlgoTrader — CLAUDE.md

Complete codebase reference for Claude Code. Updated after each major phase.

> **CURRENT FOCUS (2026-08-12):** This project is **ONLY** working on four strategies:
> 1. **SellStraddle** — theta-decay option seller (mature, live in production)
> 2. **D1 Trap FnO/Index** — zone-based option buyer (active development)
> 3. **FVG (Fair Value Gap)** — Smart Money Concepts option buyer (new 2026-08-01/03, entering paper trading; see "FVG Strategy" section below)
> 4. **OI-Flow Pre-Breakout** — OI-divergence option buyer (new 2026-08-12, built as a **fully standalone 4th strategy pipeline** — own package, own Topics, own execution bridge, own book manager; shares zero runtime infrastructure with strategies 1-3. Not yet deployed even in paper mode — see "OI-Flow Pre-Breakout Strategy" section below.)
>
> Do NOT suggest, implement, or discuss any other strategies. All new work belongs to
> one of these four. When starting a new session, read the D1 Trap, FVG, and OI-Flow sections below first.

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

# All three strategies together (SellStraddle + D1 Trap + FVG) --
# per-client paper vs live is controlled by each broker binding's own
# "Trading Mode" toggle in the dashboard (data_layer/client_db.py), NOT
# by this flag -- --mode/--strategies just decide what's constructed and
# started at the process level.
python run_system.py --mode live --ui --port 5000 --index NIFTY --strategies sell_straddle,d1_trap_option,fvg

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

### D1 Trap BearTrap — `D1TrapBearOnlyBook` (`strategies/d1_trap_option/bear_only_book.py`)

The **actively-developed, live-traded** D1 Trap engine (`strategy_name="d1_trap_bear_only"`)
— distinct from `D1TrapOptionBook` (`book.py`) above, which is frozen/legacy. Runs zone
detection on the OPTION'S OWN premium chart (not spot), buyer-only (CE or PE, never
shorts), tranche T1 (fast breach)/T2 (confirmed retracement) entries, per-lot ₹2000 hard
risk cap + staircase TSL. See the module docstring in `bear_only_book.py` for the full
mechanic (zone contact → 15m ref-candle → breach → 5m subzone → arm → swing-breach, flip
concept on zone invalidation).

**2026-08-02 optimization pass** (real premium, month window 06-29..07-31, single
consistent weekly contract per underlying — both NIFTY and SENSEX's currently-active
weekly happened to have real history back through the whole prior month, so no rollover
contamination):

- **Zone boundary**: `_detect_bear_zones` changed from `[min(ref.low,sellers_in.low),
  max(...)]` to `[sellers_in.low, ref.close]` — matches the user's own real manual
  trading method exactly (verified candle-by-candle against a live SENSEX 78000CE chart:
  zone_hi=ref candle's CLOSE, zone_lo=next/wick candle's LOW). Backtested via
  `scripts/d1trap_zone_definition_sweep.py`: ties or beats the old `ref.low` boundary
  everywhere it mattered (NIFTY 60m: PF 1.93 vs 1.86; SENSEX 15m: PF 1.71 vs the old
  live default's 1.60, ~2x the trade count).
- **Zone timeframe now per-underlying** (`_HTF_MINUTES_DEFAULT_BY_UNDERLYING`): NIFTY
  stays **60m** (15m collapses to PF 0.89 — far too noisy), SENSEX moves to **15m**
  (60m only gets PF 1.17; SENSEX needs the finer HTF to catch its faster structure).
  Overridable per-deployment via `strategy_params.htf_minutes`.
- **Strike depth now 3-ITM by default** (`_ITM_OFFSET_DEFAULT_BY_UNDERLYING`: NIFTY
  150pts, SENSEX 300pts) — replaces the old flat 200pt/500pt offsets. Backtested via
  `scripts/d1trap_strike_ladder_backtest.py` across 0/1/2/3-ITM on real premium: 3-ITM
  was the best PF on BOTH indices (NIFTY 1.86, SENSEX 1.60) of the tested range; 2-ITM
  was a real dead zone on both (PF 0.87 / 0.98). **Not tested beyond 3-ITM** — the old
  200/500pt defaults were ~4/5-ITM, so whether depth 4+ beats 3-ITM is still open.
  Overridable via `strategy_params.itm_offset_pts`.
- **ATM rounding stays 100** (`_ATM_ROUND_STEP`) — tested round-to-500 as an alternative
  anchor (`scripts/d1trap_round500_test.py`) and it was clearly worse on both indices
  (NIFTY PF 1.93→1.20, SENSEX PF 1.71→1.12). Not adopted.
- **OI-wall strike selection** (`_oi_wall_strikes`, `_OI_WALL_STRIKE_SELECTION_ENABLED
  = True` by default): daily strike choice reads the live `OptionMatrixEngine`
  `ChainSnapshot` (`Topic.MATRIX_SNAPSHOT`, already running via `run_system.py`,
  previously only fed the dashboard) and picks `CE = max_put_oi_strike` (the strike PE
  writers are defending = support), `PE = max_call_oi_strike` (the strike CE writers are
  defending = resistance) — the exact wall-swap the user described, verified against
  their worked example (spot 77000 → CE@76500/PE@77500). Falls back to the fixed-offset
  strike if no snapshot has published yet or a wall would land the traded strike OTM.
  **Cannot be backtested** — Upstox's historical candle API has no OI field
  (`data_layer/historical_candles.py` only parses OHLCV) — this is live/paper-only until
  proven forward.
- **Structure-gated SL** (`_STRUCTURE_GATED_SL_ENABLED = False` by default): built but
  NOT adopted — backtested worse on the NIFTY month baseline (PF 0.98 vs 1.03, net
  -₹1,256 vs +₹1,431) via `scripts/d1trap_structure_sl_test.py`. The idea (don't let the
  soft/zone SL fire on a tick touch while the zone is still structurally intact, only
  once a 15m candle actually closes below `zone_lo`) sounded right but on this sample the
  zone almost always went genuinely invalid within the same/next 15m candle as the tick
  SL touch anyway — waiting just let losses run to the harder ₹2000/lot cap without
  rescuing any trades. Left in as an opt-in toggle for further tuning, not live.
- Real SENSEX week backtest (07-27..07-31, pre-optimization mechanic): n=14, win% 21.4,
  PF 0.42, net -₹7,231 — confirmed the live pain point empirically; also surfaced a
  same-strike-re-entered-3x-in-1-minute churn pattern on 07-30 not yet investigated.

**Verification scripts** (all `scripts/d1trap_*`, real premium, no synthetic data):
`d1trap_strike_ladder_fetch.py`/`_backtest.py` (Stage 1), `d1trap_zone_definition_sweep.py`
(Stage 2), `d1trap_round500_test.py`, `d1trap_verify_live_defaults.py` (confirms the wired
live `bb._detect_bear_zones` reproduces the sweep scripts' numbers exactly),
`d1trap_structure_sl_test.py`, `d1trap_sensex_week_fetch.py`/`_backtest.py`.

---

### FVG — Fair Value Gap (`strategies/fvg/`)

Smart Money Concepts option **buyer** strategy. Detection runs on the underlying
**spot/index chart** (matches D1TrapOptionBook's design, not D1TrapBearOnlyBook's
option-native one) — then buys CE on a bullish setup, PE on a bearish one.
**No indicators** (no RSI/VWAP/ADX/ATR) — pure price action only, per direct spec.
Intraday only: MIS, EOD square-off at 15:15 IST, no overnight carry.

**Files:** `strategies/fvg/detector.py` (pure, unit-tested functions — swing points,
liquidity sweep, MSS, FVG detection, CE, state machine), `strategies/fvg/engine.py`
(`FVGStrategy` — the live per-binding book), `strategies/fvg/book_manager.py`
(`FVGBookManager`). Execution: `execution_bridge/fvg_bridge.py` (modeled directly on
`d1_trap_bridge.py`), `Topic.FVG_ORDER_REQUEST`/`FVG_ORDER_FILL`.

**Strategy name in DB:** `fvg`. Registered in `strategies/registry.py`; run with
`--strategies fvg`. `strategy_params` JSON (all keys optional, shown with their
validated-baseline defaults): `{"itm_offset_pts": 50, "htf_tf": 10, "ltf_tf": 3,
"direction_mode": "BOTH", "initial_sl_pct": 0.20, "trail_trigger_pct": 0.15,
"first_lock_pct": 0.08, "step_pct": 0.10, "step_lock_pct": 0.05}`. `direction_mode`
∈ `{"BOTH", "CE_ONLY", "PE_ONLY"}` restricts entries to one side (filtered in
`_check_retest_entry`, called from the `_candle_loop`→`_process_ltf_bar` chain).

**Timeframes (validated baseline, `scripts/fvg_tf_sweep.py`):** HTF = 10min (swing
structure, liquidity sweep, MSS), LTF = 3min (FVG detection + retest entry) — both
fully configurable per deployment. The engine builds BOTH from the always-available
1-minute `CANDLE_CLOSE` stream via its own bucket accumulation (`_on_candle`), not by
relying on `CandleCache` already publishing the exact configured timeframe —
`GlobalConfig.candle_timeframes` defaults to `[1, 2, 5, 15, 75]`, which does not
include arbitrary values like 3 or 10.

**Mechanic:**
1. **Swing points** — 5-bar fractal pivot on HTF bars (`find_swing_points`).
2. **Liquidity sweep** — a wick beyond PDH/PDL or an equal-highs/lows pool
   (`group_equal_levels`, 5pt NIFTY-scale tolerance) that closes back inside
   (`detect_liquidity_sweep`).
3. **MSS (Market Structure Shift)** — a later HTF bar's CLOSE breaks the most recent
   opposing swing point (`detect_mss`).
4. **FVG (Fair Value Gap)** — classic 3-candle imbalance on LTF bars
   (`detect_fvg`): bullish `candle1.high < candle3.low`, bearish
   `candle1.low > candle3.high`. candle2 must be a displacement candle (body ≥ 50% of
   its own H-L range — pure price action, not an indicator).
5. **High-liquidity tagging** (`tag_high_liquidity`) — an FVG is only tradeable if it
   forms after a confirmed HTF liquidity sweep followed by an MSS in the FVG's own
   direction; isolated/internal FVGs are recorded but never traded.
5b. **Intraday-only pool (2026-08-03 correction)** — the FVG pool is wiped at the
    start of every trading day (`reset_session()`) and `_rebuild_fvg_pool`/
    `detect_fvg` only ever scan **today's** LTF bars, never the multi-day history.
    A gap that formed on an earlier day and never got retested is NOT still
    tradeable today. This was a real, confirmed bug: a backtest trade fired off an
    FVG whose reference candles were 24 days old, using today's ATM strike against a
    price structure from three weeks earlier. HTF structure (`self._htf_bars`/
    `_htf_swings`, PDH/PDL) legitimately stays multi-day — swing points and
    yesterday's high/low are supposed to carry over, same as any real SMC read.
    Only the FVG gap itself and its retest are same-session-only, unlike
    D1TrapOptionBook's zones which are deliberately multi-day (D1Trap watches zones
    for up to 14 days by design — FVG does not).
6. **State machine** (`update_fvg_state`): `UNMITIGATED` → `PARTIALLY_FILLED` (price
   touched the gap but not the 50% Consequent Encroachment level) → `MITIGATED`
   (reached CE — tradeable retest) / `INVALIDATED` (closed all the way through the far
   boundary instead of retesting). Mitigation-touch is always checked BEFORE
   invalidation-close on a given bar so a gap-through candle is never misread as a
   valid retest.
7. **Entry**: a `high_liquidity` + `MITIGATED` FVG triggers a retest entry — CE for
   bullish, PE for bearish, at ATM ± `itm_offset_pts` (default 50 = 1-strike ITM on
   NIFTY's 50pt grid). `direction_mode` can restrict this to CE-only or PE-only.
   **Expiry = NEXT-WEEK, not current-week** (`_next_week_expiry()` in engine.py):
   `current_week = REGISTRY.get_active_expiry("NIFTY", from_date=today)`, then
   `next_week = REGISTRY.get_active_expiry("NIFTY", from_date=current_week+1day)` —
   both resolved through the registry function, never a hardcoded calendar date. A
   real-premium comparison on the identical 13 signals (only the contract changed)
   showed next-week clearly outperforms current-week: PF 1.50 vs 1.43, Net +Rs2,762
   vs +Rs1,979, smaller Max DD -Rs3,010 vs -Rs3,520 — slower theta decay per minute
   of holding time is the mechanism (`scripts/fvg_next_week_expiry_test.py`).
8. **Option-native exits (2026-08-03 rewrite)** — a real-premium backtest of the
   original spot-based SL/TP showed it desynchronizes from actual option P&L (theta
   decay, delta/IV shifts let a spot "stop" fire with premium unmoved, or premium bleed
   while spot sat inside its band; PF 0.75, net -Rs5,723 real vs -Rs907 naive-estimate
   over the same 28 trades). SL/exit now trigger off the position's OWN live premium
   (`Topic.OPTION_TICK`, tracked per-strike in `self._option_ltp` regardless of whether
   a position is open yet, so a freshly-computed entry strike already has a usable
   premium at the moment of entry):
   - **SL** = whichever is TIGHTER of `entry_premium * (1 - initial_sl_pct)` and the
     hard Rs/lot risk cap (`_MAX_RISK_RS_PER_LOT` = Rs2000, same constant as
     `bear_only_book.py`).
   - **No fixed take-profit.** A step-locked trailing stop takes over: once profit
     reaches `trail_trigger_pct`, the stop locks to `first_lock_pct`; every further
     `step_pct` of additional gain locks another `step_lock_pct` (repeating) — lets a
     strong move keep running instead of capping it at a fixed R:R.
   - **Stagnation exit**: a position whose TSL has never activated within
     `~40 real minutes` (bar count = `max(1, 40 // ltf_mins)`) is closed at market to
     cap theta bleed on a rangebound spot. Once the TSL DOES activate, stagnation no
     longer applies — the trade runs under trailing-stop management instead.

**LOCKED baseline (2026-08-03, real option premium, NIFTY, last-7-trading-days
window 2026-07-23..07-31 — the literally-requested 07-25/08-01 bookends are both
Saturdays) — FINAL, after intraday-only fix + TSL re-tune + next-week-expiry switch:**
- Timeframe: **HTF=10m / LTF=3m** (`scripts/fvg_tf_sweep.py`, 8-combo sweep — PF 1.79,
  win% 56.2%, balanced 8 CE / 8 PE on the current-week contract; faster combos like
  5m/1m or 3m/1m picked up noise and underperformed, both PF<0.9).
- Execution: 1-strike ITM option (`itm_offset_pts = 50`), **NEXT-WEEK expiry** (see
  mechanic 7 above — this was the last optimization pass and the single biggest
  improvement of the whole tuning series).
- FVG pool: **intraday-only** (see mechanic 5b above) — this alone removed 3 of the
  original 16 backtest trades that had fired off stale, prior-day (in one case
  24-day-old) FVGs.
- Risk: step-locked TSL, re-tuned tier — `initial_sl_pct=0.20`,
  `trail_trigger_pct=0.15`, `first_lock_pct=0.08`, `step_pct=0.10`,
  `step_lock_pct=0.05`. The originally-optimized "Wider Runner" tier (trigger 25%)
  only ever activated on 1 of 16 trades — 14 exited via the 40min stagnation timer
  before the premium ever swung 25%. Lowering the trigger to 15% roughly doubled TSL
  engagement without materially hurting the SL side.
- Stagnation exit: **~40 minutes** (`max(1, 40 // ltf_mins)` bars — 13 bars at the
  default 3m LTF). Confirmed load-bearing, not just upside-capping — tested and
  rejected 3 separate times this session (removing it entirely, shortening it to
  21min, and an MFE-driven fixed-TP/hybrid sweep): every alternative underperformed
  because most trades' true peak gain never reaches a fixed target, so removing the
  time cutoff just leaves losers exposed to the hard SL for longer, not "letting
  winners run." Entry-side filters (time-of-day, absolute displacement, ADX>20) were
  also tested and rejected — none improved on the no-filter baseline on this sample.
- **Final result: n=13, win% 53.8% (7W/6L), PF 1.50, Net +Rs2,762, Max DD -Rs3,010.**
  GO for paper trading (PF>1.3, win%>45%). Improved from the pre-next-week-expiry
  result (PF 1.43, Net +Rs1,979, Max DD -Rs3,520) purely by trading a farther-dated
  contract with slower theta decay — same signals, same exit rules.

**Files:** `scripts/fvg_backtest.py` (3-phase walk-forward backtest against real spot +
real option premium — signal discovery → fetch only the strikes needed → resolve
option-native exits; also the intraday-only FVG-pool fix), `scripts/fvg_tf_sweep.py`
(timeframe optimization), `scripts/fvg_tsl_sweep.py` (TSL parameter optimization),
`scripts/fvg_mfe_exit_sweep.py` (MFE analysis + exit-style sweep, all rejected),
`scripts/fvg_next_week_expiry_test.py` (the winning next-week-expiry comparison).

**Status:** implemented 2026-08-01/03, unit-tested (`tests/strategies/test_fvg_detector.py`,
19 tests), backtested against real NIFTY spot + real option premium history —
including an intraday-only FVG bug found and fixed via manual trade-by-trade review
against real chart data, and a full exit/entry/expiry optimization pass (timeframe,
TSL tiers, stagnation window, entry filters, expiry selection) before paper
deployment. Dashboard deploy form (`monitor.html`) exposes HTF/LTF/direction_mode;
TSL/expiry params are code-defaulted (override via raw `strategy_params` JSON if
needed). Ready for paper trading — see "Launch Commands" at the top of this file.

---

### OI-Flow Pre-Breakout Strategy (`strategies/oi_flow/`)

Smart-money-flow option **buyer** strategy for BANKNIFTY. Built 2026-08-12 from a
direct user request to integrate Open Interest (OI) analysis with the price-action
skill the user already trades with (option premium chart, not spot). Core insight
from the design discussion: raw/absolute OI is a **lagging** indicator — by the time
total OI confirms a breakout, the move already happened. The edge is in OI
**divergence and rate-of-change** while price is still consolidating at a wall,
catching writers unwinding *before* the breakout candle closes.

**Fully standalone by explicit user direction** — own package (`strategies/oi_flow/`),
own order/fill events (`events.py`), own execution bridge
(`execution_bridge/oi_flow_bridge.py`), own book manager (`book_manager.py`), own
`Topic.OI_FLOW_ORDER_REQUEST`/`OI_FLOW_ORDER_FILL`. Shares **zero runtime
infrastructure** with SellStraddle/D1Trap/FVG — no shared Topic, no shared event
class, no shared bridge instance, no shared book manager. Even the swing-point/
market-structure-shift detector (`detector.py`) is a fresh, independent
implementation, not imported from `strategies/fvg/detector.py`'s equivalent, despite
the conceptual overlap — a deliberate exception to this codebase's normal reuse
discipline, made so this strategy stays fully removable/auditable in isolation with
zero blast radius onto anything else.

What genuinely IS reused (platform/base infrastructure every strategy in this
codebase already sits on, not another strategy's own logic): `strategies.core.
base_book.AbstractStrategyBook`, `strategies.core.book_manager.StrategyBookManager`,
`execution_bridge.base_broker.{OrderRequest,OrderSide,OrderType}`,
`execution_bridge.broker_resolve.resolve_broker_or_alert`,
`strategies.core.gate.can_trade`, `data_layer.instrument_registry.REGISTRY`,
`data_layer.position_store`, `matrix_engine.option_matrix` (`ChainSnapshot`/
`Topic.MATRIX_SNAPSHOT` — read-only, the platform's own shared OI/PCR aggregation).

**⚠️ Cannot be backtested against history.** Confirmed by direct inspection:
`data_layer/historical_candles.py`'s intraday endpoints (`fetch_upstox_1m`,
`fetch_upstox_intraday_1m`, `fetch_upstox_range_1m`) hardcode OI to 0 — Upstox's
historical-candle API simply has no intraday OI field. (The unrelated day-level
`fetch_upstox_daily` endpoint does have an OI column, but daily granularity is
useless for a 3-5 minute divergence signal.) Every other strategy in this codebase
was validated against real historical data before going live; this one structurally
cannot be. It is correct-by-construction (unit-tested logic on hand-built synthetic
sequences) and validated **forward**, in paper mode, via the structured telemetry
described below — matching the same honesty pattern already established for
`D1TrapBearOnlyBook`'s `_oi_wall_strikes` feature (also live/paper-only, same root
cause).

**Mechanic:**
1. `strategies/oi_flow/tracker.py` (`OIFlowTracker`) — the missing piece: a rolling,
   wall-clock-anchored per-(strike, side) OI time series, computed from absolute `oi`
   levels (`oi_roc()`), **never** from a broker-supplied delta field — confirmed
   Upstox's live WebSocket feed hardcodes `change_oi=0` (`data_layer/global_feeder.py`;
   only Fyers populates a genuine per-tick delta). Returns `None` (never 0) during
   warm-up so a data gap can never silently read as "confirmed flat."
2. `strategies/oi_flow/detector.py` — two separate gates:
   - `detect_pre_breakout_signal()` (**spot** chart + OI wall): is spot still within
     `proximity_pct` of the OI wall (`ChainSnapshot.max_call_oi_strike`/
     `max_put_oi_strike`), with no confirmed market-structure break yet
     (`has_recent_structure_break` — absence of a break is exactly "still
     consolidating"), while the opposing side's OI is flattening/dropping
     (`max_opposing_roc_pct`) and the supporting side is building
     (`min_supporting_roc_pct`), gated by a PCR band (`min_pcr_bias`/`max_pcr_bias`,
     off `ChainSnapshot.pcr_smooth()` — real rolling history, zero new infra needed).
   - `confirm_option_price_action()` (**option premium** chart — the actual tradeable
     instrument): premium holding above its own rolling VWAP with no active
     rejection wick; the stop-loss is the option chart's **own** recent confirmed
     swing low (`swing_pivot=2` default — needs 5+ bars to confirm anything, by
     design), never a spot-derived offset.
   Both gates must pass before `strategies/oi_flow/engine.py`'s `OIFlowStrategy`
   emits an entry — spot-only firing without the option-side confirmation was
   explicitly rejected during design.
3. Entry: BUY at the option's current live LTP (from the book's own option-tick
   loop). Exit: SL hit (checked every option tick against the position's own
   strike), the universal hard ₹2000/lot risk-cap backstop, or EOD squareoff.
4. `strategies/oi_flow/telemetry.py` — since there's no backtest, **every** signal
   evaluation (fired or not, and exactly why not) gets logged to
   `logs/oi_flow/{underlying}_{date}.jsonl`. This is the substitute for a backtest
   report: it lets a rejection be reviewed after the fact just as easily as a real
   trade ("was the spot gate right to reject this, in hindsight?").

**Volume/absorption confirmation (2026-08-13, soft/logged only, not a hard gate):**
`Bar` now carries `volume` (a per-bar delta) and `BarAccumulator.on_tick()` accepts an
optional cumulative-volume arg — Upstox (`vtt`)/Fyers (`vol_traded_today`) both report
real, live CUMULATIVE SESSION volume on every option tick (unlike OI's `change_oi=0`
gotcha), so a bar's own volume is the delta between its first and last tick's cumulative
reading, same shape `OIFlowTracker` already handles for OI. `detect_volume_spike()`
(`detector.py`) flags when the option's own latest 1-min bar volume is ≥1.5× its
trailing 20-bar average — the "high volume + OI dropping while price consolidates =
writers being absorbed, not defending" read. Wired into `confirm_option_price_action()`
as two additive `OptionConfirmation` fields (`volume_spike`, `volume_ratio`), populated
on **every** return path including blocked ones, but **never gates `ok`** — deliberately
kept soft/telemetry-only (mirrors how PCR was already treated) rather than stacking a
4th untested hard AND-gate on top of the existing spot+option gates before a single
forward day has run. Promote to a real gate only once `logs/oi_flow/*.jsonl` shows it
earns its keep. The SL-anchoring and partial-profit-booking refinements from the same
discussion (anchor SL to the absorption candle's own low; lock partial profit once the
breakout candle closes) were deliberately deferred — they change money-tracking/exit
mechanics, higher regression risk, better done after a few days of the simpler
version's telemetry exist to review, not blind on day one.

**Status (2026-08-13):** Phases 1-5 built and unit-tested (tracker, detector, bridge,
engine/book manager, telemetry) plus the volume/absorption addition above — **not yet
deployed, not even in paper mode.**
Registered in `strategies/registry.py` as `"oi_flow"`; deploy via a direct
`strategy_deployments` DB row (`strategy_name="oi_flow"`, `underlying="BANKNIFTY"`,
`product_type="MIS"`), same pre-UI-form pattern every other strategy in this
codebase used before its own dashboard deploy form existed — no UI form built yet.
**Before any live-capital conversation**, an explicit graduation criterion needs
agreement: target ~20-30 real signal evaluations with a reviewable win/loss split in
paper mode first — there is no backtest number to compare against, so this must be
agreed up front, not decided after the fact once real numbers start coming in.

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
