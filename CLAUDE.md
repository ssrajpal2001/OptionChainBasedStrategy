# OptionChain AlgoTrader — CLAUDE.md

Complete codebase reference for Claude Code. Updated after each major phase.

> **CURRENT FOCUS (2026-08-24):** This project's core work is six strategies, **plus one
> explicit exception added 2026-08-24 (see #7 below)**. **Actually live-deployed and
> running right now: SellStraddle, OI-Flow, and Liquidity Trap.** D1 Trap FnO/Index, FVG,
> and Liquidity Sweep are built but not part of the current live rotation (see each one's
> own status below). OI-ORB Screener (#7) is built, tested, NOT yet deployed — see its
> own section.
> 1. **SellStraddle** — theta-decay option seller (mature, live in production)
> 2. **D1 Trap FnO/Index** — zone-based option buyer (built, not in the current live rotation — see "D1 Trap FnO / Index" section below)
> 3. **FVG (Fair Value Gap)** — Smart Money Concepts option buyer (new 2026-08-01/03, built for paper trading, not in the current live rotation; see "FVG Strategy" section below)
> 4. **OI-Flow Pre-Breakout** — OI-divergence option buyer (new 2026-08-12, built as a **fully standalone 4th strategy pipeline** — own package, own Topics, own execution bridge, own book manager; shares zero runtime infrastructure with strategies 1-3. **Live-deployed** — see "OI-Flow Pre-Breakout Strategy" section below.)
> 5. **Liquidity Sweep** — SMC/ICT sweep+displacement+FVG+retest option buyer (new 2026-08-19, built as a **fully standalone 5th strategy pipeline**, same zero-shared-infrastructure mandate as OI-Flow. Iteratively built and validated as a Pine Script indicator against real NIFTY chart data in TradingView BEFORE being ported to Python, per direct user instruction — not backtested in Python first. Not yet deployed even in paper mode — see "Liquidity Sweep Strategy" section below.)
> 6. **Liquidity Trap** — ref-candle sweep/CHoCH option buyer (new 2026-08-20/21, built as a **fully standalone 6th strategy pipeline**, same zero-shared-infrastructure mandate. **Live-deployed** on NIFTY/SENSEX — see "Liquidity Trap Strategy" section below.)
> 7. **OI-ORB Screener** — OI-Spurt + ORB breakout option buyer on individual F&O **STOCKS**, not an index (new 2026-08-24, built as a **fully standalone 7th strategy pipeline**, same zero-shared-infrastructure mandate. **Explicit exception to the "only six strategies" rule** — direct user instruction 2026-08-24 to port an already-working standalone Colab screener into the live app as a paper_route connectivity proof; EOD-square-off only, no SL/target yet. See "OI-ORB Screener Strategy" section below.)
>
> Do NOT suggest, implement, or discuss any other strategies beyond these seven. When
> starting a new session, read the D1 Trap, FVG, OI-Flow, Liquidity Sweep, Liquidity
> Trap, and OI-ORB Screener sections below first.

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
- **ITM PAIR GATE + "70% roll-protect"** (`strategies/sell_straddle/rolling.py`, toggle `itm_pair_gate_enabled`): when both legs are ITM with strike gap > `itm_pair_gate_min_strike_gap` (default 100pts) and cumulative P&L ≥ `itm_pair_gate_profit_inr` (default ₹500), `_check_itm_pair_gate` attempts a single-side roll (`itm_pair_gate_profit_rollover`, keeps the losing leg, rolls the profitable one), falling back to closing the whole position if no roll partner exists. Immediately after a successful rollover, `_single_side_roll`'s tail arms a protective stop on the *freshly-rolled* leg worth 70% of the ₹ profit just booked by that specific leg's close (`booked_pnl_rs = realized_pnl` of the closed leg, **not** the cumulative position P&L that triggered the gate) — per direct user spec: if the closing leg itself isn't profitable, no protection arms and no rollover-funded budget exists (rollover not happening at all when both legs are net negative is already naturally enforced by the cumulative-P&L gate above; this is specifically about the arm/no-arm choice on the leg that does close). `_check_itm_roll_protection_side` then watches that leg's running loss every cycle; once it reaches the budget, closes and either restores the original pre-roll strike (if it re-passes re-entry rules), pool-searches a balanced replacement, or closes the whole position if neither is available.
  - **⚠️ CRITICAL FIX — priority-ordering starvation (2026-08-18, real incident from 2026-08-17: a genuinely-breached 70% budget never triggered a close)**: `_check_exits()` runs a fixed, early-return exit-check ladder (EOD → day%% guardrails → LTP-decay roll → ratio-exit roll → scalable TSL → exit_rules roll → vwap_rise roll → …). The ITM-pair-gate + 70%-protection checks used to sit **last** in that ladder. Any earlier check that fires `return`s immediately without ever reaching them. An itm-pair-gate pair is, by definition, two ITM legs with a wide strike gap — exactly the shape most likely to keep the CE/PE premium ratio persistently above `ratio_threshold`, so `ratio_exit` could keep re-rolling every single cycle and starve the protection check indefinitely, even while its running loss kept growing past the budget. **Fixed**: `_check_itm_pair_gate` + `_check_itm_roll_protection` now run FIRST, immediately after the day-level %% guardrails, before LTP-decay/ratio/TSL/exit_rules/vwap_rise — a hard ₹-loss cap on an already-profit-funded leg always gets first look every cycle. `_check_exits` re-checks `self._position.status == "open"` and refreshes its local `pos` reference right after these two calls, since either can close the position outright or swap a leg's strike out from under the stale reference the rest of the ladder was about to use. Regression test: `tests/strategies/test_itm_roll_protection_priority.py` (persistently-true ratio_exit condition + a breached protection budget on the same tick — proves the stop fires and ratio_exit does not preempt it).
- **DAY-LOW REVERSAL EXIT** (2026-08-18/19, direct user spec, opt-in `day_low_exit_enabled`, default **OFF**): the straddle's combined premium (`pos.current_value` = CE_ltp+PE_ltp) typically decays to its lowest point of the day somewhere in the 09:15-15:00 window as theta drains, then tends to reverse upward into the close, eating back into profit already banked on paper. `_check_exits()` tracks the CURRENTLY RUNNING pair's own running-min combined premium since IT started running (`self._session_min_straddle_value`), freezes it ONCE — the first tick at/after `day_low_freeze_time` (default **15:00** IST) — at whatever this pair's own low is at that instant (`self._session_min_straddle_frozen`). From the freeze tick onward — **including the freeze tick itself**: if the freeze-time reading turns out to BE this pair's low so far, that already-equals-frozen value fires the exit immediately, same tick, no separate retest required (explicit user clarification: "if for instance the lowest value is the [freeze-time] value then also we exit") — the moment the current combined premium reaches or undercuts the frozen value, the whole position closes (`day_low_reversal_exit`) and the book stops for the day, same as `day_profit_target`/`day_loss_sl`. Placed right after the ITM-pair-gate/70%-protection block in the priority ladder (`EOD→Day%→ITMgate→DayLow→LTPdecay→Ratio→ScalableTSL→exit_rules→VWAPrise`) — a hard risk-cap still gets first look, then this profit-lock, then the generic rollover checks.
  - **Scoped to the running pair, not the whole day (2026-08-19 correction)**: an earlier version tracked a whole-day/whole-book low that survived rollovers untouched. User explicitly corrected this: "exit will fire from the current running pair not the pair which was there prev when the roll happened... when roll happened prev data is of no use... will focus only on running legs and its own lowest point of the day." `self._day_low_tracked_pair` (a `(ce_strike, pe_strike)` tuple) identifies which pair the tracker is currently following; the instant the running position's own strikes differ from it (a rollover OR a fresh re-entry), tracking resets — the prior pair's low is discarded, not inherited.
  - **"From scratch" seeds from REAL history, not a blank slate (second 2026-08-19 correction)**: "from scratch" does not mean literally starting the count at whatever tick the book happens to notice the new pair on — the new pair was genuinely trading on the real market since 09:15 even though this book only just started holding it. Direct user spec: "get the rest api intraday historical data of 1 min for the pair and get the min values and then find out what was the min low of the day and then add that to the aggregator and move forward with current value added to it." `_seed_day_low_for_pair()` (`exits.py`) REST-fetches today's 1-min intraday history (`data_layer.historical_candles.fetch_upstox_intraday_1m`) for both legs via `REGISTRY.get_broker_symbol(...)`/`ClientDB` (same credential/lookup pattern as the pre-existing `_seed_exec_legs` RSI/ROC warm-up), combines CE+PE **CLOSE** prices minute-by-minute aligned by timestamp (deliberately NOT each leg's own independent `low` — summing two legs' separate lows would combine two price extremes that almost certainly never occurred at the same instant, a floor the real combined premium may never have actually touched; matches how the live tracker itself works off `ce_ltp+pe_ltp`, both "last traded"), and returns the min of that aligned series. On a pair-change, `self._session_min_straddle_value = min(seed, current_tick_value)` — the seed wins whenever it found a genuinely lower historical point; the live tick wins if the seed is unavailable (`float('inf')` on ANY failure: crypto, no token, no broker symbol, no data, no overlapping minutes, network error — always degrades safely, never blocks or sets a wrong floor). **Consequence**: a pair that starts running after freeze time (e.g. a roll at 15:10 with freeze=15:00) is no longer a degenerate self-fire-on-first-tick case the way an unseeded reset would be — it gets seeded with its real 09:15–15:10 low just like any other pair, so it only fires once the live rate actually retests that real historical low. Tests: `tests/strategies/test_day_low_seed_rest_fetch.py` (minute-alignment correctness, rejects the independent-low miscalculation, graceful `inf` fallback for crypto/no-token/no-symbol/no-overlap/fetch-exception) + the seed-integration tests in `test_day_low_reversal_exit.py`.
  - Both tracking fields + `day_low_tracked_pair` persist across a restart via the existing `_persist_session()`/`_restore_session()` session file (same one `session_realized_pnl_pts` already uses; the pair tuple round-trips through JSON as a list) and reset to fresh (`inf`/`None`/`None`) on `reset_session()` for a new trading day.
  - New config keys (per-index `sell_straddle` section, both admin + per-client-override capable like `entry_start`/`force_exit`; also seeded in `data_layer/runtime_config.py`'s `_SS_INDEX_DEFAULT`): `day_low_exit_enabled` (bool, default False), `day_low_freeze_time` (`"HH:MM"`, default `"15:00"`). **UI**: new "DAY-LOW REVERSAL EXIT" card in the admin Guardrails tab (`monitor.html`, right after ITM Pair Gate) — ON/OFF toggle + a `<input type=time>` for the freeze time; `reasonLabel()` maps `day_low_reversal_exit` to "Day-low reversal exit (retest of frozen day-low)" for the dashboard History ledger.
  - Brand new and structurally unvalidated (like every other SellStraddle feature, this one can't be backtested against real intraday premium either without a dedicated pass) — deploy opt-in per binding, watch a few real sessions before trusting it broadly. Tests: `tests/strategies/test_day_low_reversal_exit.py` (disabled-by-default no-op, running-min tracking before freeze, freeze-tick-is-itself-the-low fires same-tick, freeze-then-later-retest, undercut-also-exits, tracking-resets-on-a-mid-day-rollover, pair-starting-after-freeze-time-fires-on-its-own-first-tick-when-unseeded, pair-change-seeds-from-rest-history-not-a-blank-slate, pair-starting-after-freeze-time-uses-seeded-low-instead-of-self-firing) + `tests/strategies/test_day_low_seed_rest_fetch.py` (the REST fetch/minute-alignment logic in isolation).
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

**Full dashboard UI integration + paper_route (2026-08-13):** `oi_flow` is now a
selectable option in the deploy form (`monitor.html` ADD STRATEGY dropdown, default
underlying NIFTY) — no more raw SQL needed to deploy. `OIFlowExecutionBridge` gained
`paper_route` handling (previously only pure `paper`/full `live` existed): the order
genuinely reaches the real broker (verifies routing/whitelist from a no-fund account,
mirrors `StraddleExecutionBridge`'s own `paper_route` contract), and the strategy's
own fill always finalizes — the broker's real `avg_price` if it happened to confirm
one, else a local simulated fill at the strategy's own price; a genuinely unresolvable
broker still aborts loudly. New `OIFlowStrategy.monitoring_state()` + `GET
/api/oiflow/status` + a live panel on the deployment card (mirrors the FVG/D1Trap zone
panels) show the OI wall + buildup per strike, the open position with running P&L, and
an in-memory "recent remarks" trail (last 30 signal evaluations, human-readable) — the
live-UI counterpart to `telemetry.py`'s JSONL log.

**⚠️ CRITICAL FIX — PE-side confirmation/SL was backwards (2026-08-13):**
`confirm_option_price_action()` incorrectly mirrored CE/PE the way `detect_pre_
breakout_signal()` correctly does for the SPOT-side gate (where CE/PE genuinely point
opposite directions). But `confirm_option_price_action` runs on the **option's own
premium chart** — a bought PE is still LONG its own premium, exactly like a bought CE
(profit = premium up, loss = premium down). The old PE branch required `close < VWAP`
(entering into weakness) and anchored SL to a swing **HIGH** — both backwards for a
long position. Zero tests ever exercised the PE branch, so this shipped unnoticed
until caught during a target/SL review ahead of the first live deployment.
`OIFlowStrategy._check_exit()` had the matching bug (PE fired on `ltp >= sl_price`, a
RISE, inconsistent with its own `"sl_option_swing_low"` label). Both fixed so `side`
no longer changes the decision logic in either function — only labeling. 4 regression
tests added (2 detector-level proving PE now matches CE byte-for-byte, 2 engine-level
proving `_check_exit` fires on a fall for PE).

**Step-locked trailing profit-lock — the "target" concept (2026-08-13):** before this,
exits were only SL + the hard ₹2000/lot risk cap + EOD — no take-profit or trailing
mechanism at all. `OIFlowStrategy._check_exit()` now runs the same ratchet FVG's own
validated `_check_exit_premium()` uses (written fresh here, no import, per the
standalone mandate): once `profit_pct >= trail_trigger_pct`, lock `first_lock_pct`;
every further `step_pct` of gain locks another `step_lock_pct` (repeating, never
un-ratchets). No fixed take-profit ceiling — a strong move keeps running until the
rising floor catches it. **The mechanic is proven (same formula as FVG); the specific
default numbers (`trail_trigger_pct=0.15, first_lock_pct=0.08, step_pct=0.10,
step_lock_pct=0.05`) are FVG's own tuned baseline, borrowed as a starting point — NOT
independently validated for OI-Flow**, since this strategy still can't be backtested
at all. Review against real forward telemetry before trusting them. Configurable per
deployment via `strategy_params` (same JSON pattern as every other param).

**S1 trailing stop — user's own framing, "S1 will act as TSL" (2026-08-13):** a second,
structural trailing floor alongside the percentage ratchet above. `_maybe_promote_s1()`
reuses `detector.swing_low()` (this strategy's own already-built utility, not another
strategy's S&R code) against the position's own option-premium bars: as a new CONFIRMED
swing low prints above the current `s1_floor`, the floor promotes to it (ratchets only,
never lowers) — checked on every option-bar CLOSE for the position's own strike.
`_check_exit()`'s effective stop is `max(percentage_floor, s1_floor)` — whichever is
tighter binds, so S1 can pull the stop in tighter than the flat percentage alone would
(a real confirmed price-action level, not an arbitrary step) without ever loosening
protection the percentage ratchet already earned. Exit reason distinguishes `s1_hit`
from `tsl_hit` from the plain `sl_option_swing_low` depending on which one actually
promoted past the original anchor. Deliberately a **combination**, not a replacement of
the percentage TSL — hedges against either single mechanism underperforming, since
neither can be backtested.

**⚠️ Related fix found while building S1 — option-strike lock during an open position:**
`_option_acc[side]`/`_live_option_ltp[side]` used to ALWAYS follow whatever the
*current* OI wall was (`snap.max_call_oi_strike`/`max_put_oi_strike`, re-derived on
every `MATRIX_SNAPSHOT` regardless of position state). Correct while scanning/flat, but
if the wall drifted to a *different* strike while a position was open, these would
silently start tracking the new wall's premium — a different instrument's price series
entirely — corrupting the EOD exit-price fallback, the dashboard's live P&L, and (had
this shipped first) the S1 swing-low calculation above, while the actual held position
sat at the old strike. `_option_tick_loop()` now locks onto the position's own strike
the moment one opens, immune to wall drift, and reverts to following the current wall
once flat again. 2 regression tests drive the real loop with a position open at one
strike while the snapshot's wall has already moved to another.

**⚠️ Twin bug found on the SCANNING side — accumulator never reset on wall drift while
flat (2026-08-13):** `BarAccumulator` has zero concept of "which instrument" it's
bucketing — it just buckets whatever `ltp` arrives by timestamp. The position-side fix
above only handles wall drift *during an open trade*; while flat/scanning (the far more
common case, since the wall can drift many times a day with no position open at all),
`_option_acc[side]`/`_live_option_ltp[side]` kept following the current wall correctly
in *principle*, but the accumulator itself was never reset when the tracked strike
changed — silently mixing two different option contracts' OHLC into one continuous bar
series, corrupting `confirm_option_price_action()`'s VWAP/swing-low for every entry
evaluated afterward. Fixed with one unified mechanism: `self._tracked_option_strike`
records whichever strike is currently feeding the accumulator (wall while flat,
position's own strike while open); `_option_tick_loop()` resets
`self._option_acc[side]`/pops `_live_option_ltp[side]` the instant that tracked value
changes for ANY reason (wall drift, position opening, position closing and reverting to
a possibly-new wall).

**⚠️ Market-hours risk audit (2026-08-13), prompted by direct request to "think like a
trader" about what can go wrong during a live position — found and fixed 3 more real
gaps beyond the wall-drift ones above:**
1. **Partial fills never reconciled.** `OrderFill.qty` (the broker's actual filled
   quantity) was never read — only `avg_price`. A broker that fills PART of the
   requested lots (real, not uncommon on a moderately-liquid strike for a MARKET order)
   would have been silently treated as a full fill: on ENTRY, `self._position["qty"]`
   would stay at the full requested amount while only some lots were genuinely held,
   corrupting P&L/risk-cap sizing; on EXIT, the position would be cleared entirely
   (believed flat) while some lots were still actually open and completely unprotected.
   `OIFlowFillEvent` gained a `filled_qty` field (defaults to `qty` for paper/paper_route's
   always-full simulated fills); the bridge now detects `0 < filled_qty < requested` and
   logs CRITICAL; the engine reconciles `position["qty"]` down on a partial ENTRY fill
   and, on a partial EXIT fill, does NOT clear the position — reduces `qty` to what's
   genuinely still open and lets the next SL/TSL/S1 tick (or EOD) retry closing the
   remainder, exactly like an unconfirmed/aborted exit already does.
2. **No feed-staleness protection for an open position.** SL/TSL/S1 only ever
   re-evaluate when a fresh `OPTION_TICK` arrives for the position's own strike — a
   WebSocket outage (or the feed silently dropping just that one strike) would leave a
   position with zero protection and zero warning, since nothing else ever re-checks the
   exit condition. `_eod_loop`'s existing 5s cycle now also calls
   `_check_tick_staleness()`: if no tick has landed for the position's own strike within
   60s, logs CRITICAL once per staleness episode (cleared the moment a fresh tick
   arrives) — an alert, not an auto-close, since a REST-poll fallback to keep the
   position genuinely protected during an outage is a bigger lift than this pass covers.
3. Two of the design-decision items flagged above were subsequently approved and
   **built** (see next section): the re-entry cooldown and the corrupt-tick date guard.
   Still deferred: a REST-poll fallback for genuinely stale feeds (needs a real endpoint
   wired in); CE-priority tie-break when both CE and PE signals would fire on the exact
   same bar (arbitrary but defensible, not wrong).

Also considered and explicitly rejected: pegging an exit **target** to a new/shifted OI
wall level. The wall lives on spot; translating "spot distance to the wall" into an
option premium target reintroduces the same spot-premium desync problem the PE bug and
FVG's own history already proved out (theta/IV/delta mean spot distance doesn't map
cleanly to premium distance). If revisited, it should be a soft/logged signal (same
tier as PCR and the volume spike), not a hard exit gate.

**Re-entry cooldown + corrupt-tick date guard, approved and built (2026-08-13):**
- `sl_cooldown_minutes` (default 15, `strategy_params`-configurable): after ANY stop-out
  exit (SL/TSL/S1/hard risk cap — EOD explicitly excluded, since it isn't a loss signal
  and `_day_done` already blocks further entries that day), `_on_spot_bar_close()` skips
  entry evaluation entirely (not just blocks the resulting order) until
  `self._cooldown_until` passes — book-wide, not per-side, since the book only ever
  holds one position at a time anyway. Logged to `self._clog` both when it starts and on
  every bar it's still active, so it's visible, not silent. **Not persisted across a
  restart** — a restart mid-cooldown resets it; accepted as a low-probability gap, not
  worth the added persistence complexity this pass.
- Corrupt-tick date guard: `_index_tick_loop()` now validates a tick's own reported date
  against the real wall-clock date (`datetime.now(IST).date()`) before trusting it for
  anything — a single malformed/corrupt tick reporting an implausible date (more than 1
  day off) is REJECTED outright (not bucketed, not used to decide a session reset),
  rather than potentially triggering `reset_session()` and wiping all in-progress state
  (bars, tracked strike, cooldown, remarks) for a day that hasn't actually changed.
- 6 new tests: cooldown blocks/resumes evaluation, cooldown starts on a stop-out exit
  but not on EOD, corrupt-date tick rejected without resetting session, plausible-date
  tick still triggers a genuine first-of-day reset.

**Status (2026-08-13):** Phases 1-5 + the volume/absorption addition + the paper_route/
dashboard integration + the critical PE fix + the target/TSL mechanic above are all
built, unit-tested, and pushed. First real deployment (BOTH live and paper_route) is
scheduled for the next trading session — **NIFTY** (and optionally **SENSEX**, one
deployment row per underlying — the strategy is underlying-agnostic by design; the
client's own choice for the first real run, not BANKNIFTY), one binding on
`trading_mode=live` (real funds) and one on `trading_mode=paper_route` (order verified
against the real broker, fill simulated) running side-by-side with `sell_straddle`.
Deploy via the dashboard form, not a raw SQL row.
**Before any FURTHER live-capital scale-up beyond this first deployment**, an explicit
graduation criterion needs agreement: target ~20-30 real signal evaluations with a
reviewable win/loss split first — there is no backtest number to compare against, so
this must be agreed up front, not decided after the fact once real numbers start
coming in.

**⚠️ CRITICAL PLATFORM BUG found and fixed during first live deployment (2026-08-13):**
`matrix_engine/option_matrix.py`'s `OptionMatrixEngine.initialize()` had **zero callers
anywhere in the codebase** — confirmed via a repo-wide grep, only its own definition.
`OptionMatrix.on_option_tick()`'s very first line is `if self._snap is None: return
False`, and `self._snap` is ONLY ever set by `initialize()` — so `Topic.MATRIX_SNAPSHOT`
had **never been published, for any underlying, ever**, regardless of real tick volume.
Confirmed live: NIFTY was receiving ~1600 real option ticks/min (via SellStraddle's own
independent consumption of the same `Topic.OPTION_TICK` stream — SellStraddle does not
use `OptionMatrixEngine` at all, confirmed zero references) while OI-Flow sat on `WAIT`
for over an hour on its first live day, because it depends entirely on this snapshot.
No test existed for `option_matrix.py` at all before this — that's how it went
unnoticed. This is a **platform bug, not an OI-Flow bug** — `D1TrapBearOnlyBook`'s
`_oi_wall_strikes` feature also reads this same snapshot but was silently protected by
its own documented fallback ("falls back to the fixed-offset strike if no snapshot has
published yet"), so it never surfaced as a visible failure there.

**Fix**: `OptionMatrixEngine._consume_index()` now self-initializes a matrix the moment
it sees the first real `INDEX_TICK` for that underlying (resolving the active expiry via
the same `REGISTRY.get_active_expiry()` every other live consumer already uses), instead
of waiting for an external `initialize()` call that was never coming.
`tests/matrix_engine/test_option_matrix.py` (new — first test coverage this file has
ever had) covers the exact bug (`on_option_tick` returns `False` before init), the fix
(lazy self-init on first tick, only once, gracefully no-ops if the registry isn't loaded
yet or spot≤0), and an end-to-end regression driving both consumer loops together to
confirm a real `MATRIX_SNAPSHOT` publish. **SellStraddle (including Gurmeet's live
capital) has zero dependency on this component and was completely unaffected, before or
after this fix** — confirmed via direct code inspection before touching anything.

**Remark specificity (2026-08-13):** found live, same first trading day — NIFTY PE's
dashboard remark said "not consolidating at the wall yet" while spot was genuinely only
0.19% from the PE wall (well inside the 0.5% proximity threshold); the true blocker was
PCR sitting neutral at 1.00 (needs <0.7 for PE), but the old generic
`skip_reason="spot_gate_no_signal"` couldn't distinguish a proximity miss from a
structure-break, OI-ROC, or PCR rejection. Added `detector.explain_no_signal()` — a
diagnostic-only twin of `detect_pre_breakout_signal()` that re-runs the exact same checks
in the exact same order purely to report which one is actually blocking, in plain
English (deliberately duplicated rather than refactoring the real decision function to
return a reason code, keeping it exactly as simple/pure as its own docstring already
commits to). `SignalTelemetryRow` gained `spot_gate_detail`; remarks now say e.g. "PCR
1.00 not bearish enough for PE (needs <0.7)" instead of the misleading generic line.

**⚠️ CRITICAL — closed trades never appeared in the dashboard History tab (2026-08-13):**
found when a real NIFTY CE trade (entered 12:03, S1-hit exit 12:16, both confirmed in the
per-underlying log) showed nowhere in the dashboard. Root cause: `execution_bridge/
option_buyer_bridge_base.py` (the shared base class D1Trap/FVG's bridges inherit) calls
`data_layer.trade_history.record()` on every confirmed SELL fill — but `oi_flow_bridge.py`,
written fresh per the standalone mandate, never got the equivalent call at all. Fixed:
added `OIFlowExecutionBridge._record_history()` (own implementation, no import from the
base class — `data_layer.trade_history` is platform infrastructure, not another
strategy's logic, same category as `position_store`/`instrument_registry` already reused
elsewhere in this bridge), called from all three SELL-fill paths (paper, paper_route
simulated, live confirmed). Also threads `pos["entry_ts"]` into the exit's
`OIFlowOrderEvent` (engine.py) — it was never being passed before, so history records
would have had no entry timestamp even once recording started. 4 new tests confirm each
fill path calls `trade_history.record()` with the correct P&L, and that a BUY (entry)
fill never does. Separately noted, lower priority: the dashboard's "History" tab only
shows *in-progress* (still-open) trades for `sell_straddle`/`v4_cascade`
(`_open_history_rows()` in `dashboard_server.py`) — `oi_flow` (and FVG/D1Trap) never got
that branch either; a currently-open position won't appear there until closed, though it
already shows correctly in OI-Flow's own live monitoring panel. Not fixed this pass.

**⚠️ CRITICAL — zero trades possible for days straight: OI-wall jitter permanently starved
the OI tracker's warm-up (2026-08-19):** user ran OI-Flow live on 2026-08-18 and it took
zero trades all day. Root-caused via `logs/oi_flow/NIFTY_20260818.jsonl` — every single
evaluation across the whole session showed `opposing_roc`/`supporting_roc` as `null` with
`skip_reason="spot_gate_no_signal"`, most commonly `"insufficient OI history yet (tracker
still warming up)"`, and the logged `wall_strike` was visibly flipping between 3-4 nearby
strikes (24000/24100/24150/24200/24250/24400) almost every single one-minute evaluation
cycle. Traced to `matrix_engine/option_matrix.py`'s `ChainSnapshot.max_call_oi_strike`/
`max_put_oi_strike` (`recompute()`) — a raw, **unsmoothed** `max()` over per-strike OI,
recomputed on every `OPTION_TICK`. When 2-3 strikes carry genuinely near-tied OI (common,
normal market condition, not a data bug), that argmax can flip every few seconds as
individual OI updates land for different strikes. `OIFlowStrategy._rewatch_oi_strikes()`
(engine.py) blindly followed this raw wall on every `MATRIX_SNAPSHOT` and called
`OIFlowTracker.watch_strikes()` with whatever the CURRENT wall was — and `watch_strikes()`
**drops history for any (strike,side) no longer watched** (by design, so memory doesn't
accumulate for strikes that stopped mattering). Net effect: the tracker's `oi_roc()` needs
180 continuous seconds (`window_sec`) of history on one specific strike to return anything
but `None` — and the wall kept reassigning to a different strike faster than that, so the
180s clock reset before it ever completed, **forever**, on any real trading day where the
wall wasn't a single clean dominant strike (i.e. most days). This alone fully explains zero
entries: `detect_pre_breakout_signal()`'s OI-ROC check can never even evaluate a real
number, only `None`, which always fails the gate.
**Fix — wall-selection debounce, standalone to OI-Flow only** (does NOT touch
`OptionMatrixEngine`'s own raw computation, which other consumers like the dashboard's live
wall display may legitimately want instantaneous): new `_debounced_wall()` in engine.py —
a new wall candidate must be the *consistently reported* argmax for `wall_debounce_sec`
seconds (config, default **90s**) before `OIFlowTracker`'s watch list actually switches to
it; a candidate that flips away before that never resets anything, and `_rewatch_oi_strikes`
now also skips the `watch_strikes()` call entirely when the debounced result is unchanged
from last time (steady-state days never touch the tracker at all). First-ever pick each
session is adopted immediately (nothing to debounce against). `wall_debounce_sec<=0`
disables debouncing entirely (reverts to the old instantaneous-follow behavior) for anyone
who wants to opt back out. New `strategy_params` key `wall_debounce_sec` (default 90.0,
wired through `book_manager.py` same as every other tunable). **Known, deliberately
untouched parallel gap**: `_option_tick_loop`'s own wall-following (for the option premium
bar accumulator feeding `confirm_option_price_action`'s VWAP/wick/swing-low checks) reads
`snap.max_call_oi_strike`/`max_put_oi_strike` directly, un-debounced, by design (the
2026-08-13 twin-bug fix specifically wants an accumulator reset on ANY wall change while
flat, to prevent two different option contracts' OHLC mixing into one series) — so once the
spot-side gate above starts passing more often thanks to this fix, the option-side gate
could still be starved by the same underlying wall-jitter in a *different* way (never
enough same-strike bars for a valid swing-low/VWAP). No evidence yet that this has actually
blocked anything (the spot gate never got that far to see), so it's flagged for future
telemetry review, not blind-fixed alongside this. 3 new regression tests
(`tests/oi_flow/test_engine.py`, `test_debounced_wall_*`) drive `_rewatch_oi_strikes` the
same way `_matrix_snapshot_loop` does: a brief flip is ignored (and the still-tracked wall's
accumulated OI history survives untouched), a candidate sustained past the debounce window
does switch, and `wall_debounce_sec=0` reproduces the old instant-follow behavior exactly.

**⚠️ Real production incident: a SENSEX PE entry stopped out 13 seconds after entry —
root-caused, fixed, and the fix itself verified against real market data before shipping
(2026-08-19):** first real live signal (paper_route, BFO-segment-restricted account, so
simulated fill only — no real capital at risk) fired correctly (spot 0.08% from the PE
wall, opposing OI dropping, supporting OI building, PCR 0.52) at 10:27:00, entered PE
77000 @197.65, and hit `sl_option_swing_low@194.15` at 10:27:13 — confirmed via a real
TradingView 1-min chart review to be ordinary candle noise, not a genuine reversal. Root
cause: the SL anchor (`swing_low()`, a bare single-touch `pivot=2` pivot) had no minimum-
distance floor — it just took whatever the most recent confirmed pivot happened to be,
which this time sat only 1.77% from entry.

Fix: `pool_swing_low()` (`strategies/oi_flow/detector.py`) — the SAME "equal lows /
liquidity pool" multi-touch concept already validated (in Pine, against real chart data)
for the Liquidity Sweep strategy, independently reimplemented here (fresh code, no
import, per this strategy's standalone mandate). Requires 2+ confirmed swing lows
clustering within `tol_pts` (default ₹2) of each other before counting as a real anchor
— a lone pivot no longer qualifies. Used by both `confirm_option_price_action()`'s
entry-time SL and `_maybe_promote_s1()`'s trailing-stop promotion.

**A first design (wider `pivot=5` + a separate, slower 3-min option-bar accumulator
feeding only the SL) was built, then REJECTED after checking it against the real SENSEX
77000 PE 1-min data for the actual incident day** (fetched live via Upstox's intraday
API — `data_layer/historical_candles.py`'s existing `fetch_upstox_intraday_1m`, same
function the app itself uses): that combination would have BLOCKED the real 10:27 entry
entirely — no valid 2-touch cluster existed until 12:42, over 2 hours later. A second,
simpler variant (**Config F**, what actually shipped) — keep the plain `pivot=2` on the
SAME 1-min bars already used for VWAP/wick-rejection, just add `min_touches=2` — found a
real, valid anchor (160.85, vs. the original 194.15) WITHOUT blocking the entry.
Simulated forward against the same real data with a faithful intrabar (not close-only)
tick model: the wider SL would have survived the early noise, ridden a genuine spike to
280, and the EXISTING percentage trailing-stop (unchanged) would have locked and exited
at 233.23 (11:07) for **+₹711.54**, vs. the real **−₹71** loss. Separately tested
tightening/loosening the existing TSL step parameters against this same real trade: the
current settings (`trail_trigger_pct=0.15, first_lock_pct=0.08, step_pct=0.10,
step_lock_pct=0.05`) outperformed every tested variant — tightening caused premature
exits that missed the spike entirely; loosening gave back more on the pullback. Left
unchanged; a single real trade isn't enough evidence to retune this, and the point was to
confirm the initial "exiting too early" impression wasn't itself a TSL problem (it
mostly was the too-tight SL cutting the trade off before the TSL ever got a chance to
work).

5 new regression tests (`pool_swing_low` multi-touch/clustering behavior, `confirm_
option_price_action` blocked-vs-confirmed with the new default). Still unvalidated
beyond this one real incident — this strategy cannot be backtested at all (see this
section's own opening note) — watch real forward telemetry before trusting these
specific numbers (₹2 tolerance, 2-touch minimum) any further.

---

### Liquidity Sweep Strategy (`strategies/liquidity_sweep/`)

SMC/ICT option **buyer** strategy: sweep (stop-hunt) → structure bias → displacement →
Fair Value Gap (FVG) → retest entry. Detection runs on the underlying **spot/index
chart** (same design as `D1TrapOptionBook`/FVG, not `D1TrapBearOnlyBook`'s option-native
one). **Fully standalone** — own package, own order/fill events, own Topics
(`Topic.LIQUIDITY_SWEEP_ORDER_REQUEST`/`LIQUIDITY_SWEEP_ORDER_FILL`), own execution
bridge (`execution_bridge/liquidity_sweep_bridge.py`), own book manager — shares zero
runtime infrastructure with SellStraddle/D1Trap/FVG/OI-Flow, same mandate as
`strategies/oi_flow/`. Intraday only: EOD squareoff (default 15:15 IST), no overnight
carry, full session-state reset every trading day.

**Build process (2026-08-19, explicit direct user instruction — do NOT repeat the
skipped step for any future strategy unless told to): built and iteratively tuned as a
Pine Script v5 indicator in TradingView FIRST, against real NIFTY 5-minute chart data,
BEFORE any Python code was written** — the user explicitly said not to do a Python
backtest first for this one. The Pine files live in `pinescript/` (informational/
reference only, not part of the running application):
`pinescript/liquidity_sweep_indicator.pine` (first pure-visual version),
`pinescript/liquidity_sweep_strategy.pine` (adds SL/Target1/Target2 for TradingView's
own Strategy Tester), `pinescript/liquidity_sweep_indicator_with_risk.pine` (the
**validated source of truth** — funnel diagnostics, scoreboard, all final tuned
defaults). Tuning was funnel-diagnostic driven throughout (raw sweeps → passed bias →
displaced → FVG confirmed → signals, with explicit counters at each stage) — every
default below came from real evidence on that funnel, not a guess, and every version
change was confirmed actually-applied on the user's chart via version tags
(`[v7]`...`[v10]`) after two rounds of the user reporting stale/unrefreshed scripts.
`strategies/liquidity_sweep/detector.py` is a direct, faithful Python port of the final
validated script's logic — see that file's own module docstring for the full mechanic
breakdown (rolling-base/swing-pivot/liquidity-pool sources, BoS/CHoCH structure replay,
wick-vs-body sweep definition, displacement, multi-candle FVG confirmation window,
retest).

**Validated/default parameters** (all overridable per-deployment via `strategy_params`
JSON, same pattern as every other strategy): `ltf_min=5` (single timeframe drives the
whole pipeline — pivots/structure/sweep/displacement/FVG/retest all read the SAME bars;
`htf_min=75` only matters for the non-default `rolling_base` liq_source),
`liq_source="liquidity_pool"` (classic ICT equal-highs/equal-lows clustering — the
user's own synthesized answer to "what counts as real liquidity" after cross-
referencing multiple independent reference indicators plus this codebase's own FVG
`group_equal_levels` precedent; `"swing_pivots"`/`"rolling_base"` kept for parity),
`pivot_left=pivot_right=5`, `pool_tol_pts=5.0`, `pool_min_touches=2`,
`use_struct_bias=True` (BoS/CHoCH replay gates sweep direction), `atr_len=14`,
`atr_mult=0.7` (evidence-tuned DOWN from 1.5→1.0→0.7 — a live funnel diagnostic isolated
the ATR ratio as the binding constraint at higher thresholds), `disp_window=6`,
`swing_len=3`, `fvg_confirm_window=3` (multi-candle retry, not one-shot — real evidence
this was silently killing the only displacement candidate that got through during
tuning), `stale_bars=12`, `tgt1_rr=1.5`, `use_liquidity_target2=True` (Target2 = the
opposing side's own currently-active liquidity level, confirmed formula from the source
GitHub repo's `execution/risk_engine.py`; falls back to `tgt2_rr=3.0` R-multiple only
when no valid correctly-sided opposing level exists), `itm_offset_pts=0.0` (ATM by
default — deliberately NOT copying FVG's 1-strike-ITM default, since this strategy's
option-side execution has zero validation of its own, unlike FVG's dedicated
optimization pass).

**SL/Target1/Target2 are SPOT-INDEX levels, not option premium — a deliberate, honestly-
flagged design choice** (see `engine.py`'s own module docstring): the validated Pine
script's whole risk pipeline is spot-based throughout (SL = the swept candle's own
extreme, Target1/2 = R-multiples or opposing spot liquidity off spot risk), and
translating that into option-premium terms would need a live delta/greeks model this
codebase doesn't build elsewhere (`OptionTick.delta` exists as a field but its
reliability across both Upstox/Fyers feeders was not verified under the time pressure
of this build). `LiquiditySweepStrategy._check_exit_on_spot()` compares live SPOT ticks
(checked on every tick, MORE often than the validated Pine script's own bar-close-only
check) against these levels directly; the option's own live LTP is simply the fill price
whenever a spot-level entry/exit condition fires — never itself a premium-based
SL/target. On Target1 hit, SL moves to breakeven (spot terms) and the position keeps
running toward Target2 — no partial booking at T1, matching the final tuned Pine
version (an earlier `liquidity_sweep_strategy.pine` draft had partial-booking; the
validated `_with_risk.pine` simplified it away). An independent hard ₹2000/lot
option-premium risk-cap backstop (same constant every other option-buyer strategy in
this codebase uses) runs alongside the spot-based SL as a safety net against IV
crush/bid-ask blowouts the spot read alone wouldn't catch.

**Strike selection**: ATM ± `itm_offset_pts`, rounded to the underlying's own
`strike_step`; **current-week expiry** (`REGISTRY.get_active_expiry_strict`) — no
optimization pass exists yet to justify FVG's next-week-expiry choice for this
strategy, so it stays on the platform's plain default rather than copying an
unvalidated assumption from a different strategy.

**⚠️ Bug found and fixed during construction (2026-08-19), before any test run saw
it**: `_try_enter()`'s opposing-liquidity Target2 selection was initially backwards —
`opp_level = level_high if direction == -1 else level_low` picked the resistance level
ABOVE for a BEARISH (PE) trade and the support level BELOW for a BULLISH (CE) trade,
exactly inverted from "target the next liquidity in the trade's own profit direction."
`compute_trade_plan()`'s own side-validity check (`opposing_liquidity` must sit on the
correct side of entry) meant this would never have corrupted a live trade — it would
just have silently disabled `use_liquidity_target2` in precisely the scenario where it
should have fired, always falling back to the R-multiple instead. Caught while writing
`tests/liquidity_sweep/test_engine.py::test_try_enter_uses_liquidity_target2_on_correct_side`
before the code ever ran against real data. Fixed to `level_high if direction == 1 else
level_low`.

**Files**: `strategies/liquidity_sweep/detector.py` (pure functions — `Bar`/
`BarAccumulator`, swing points, liquidity pool clustering, BoS/CHoCH structure,
sweep/displacement/FVG/retest, `compute_trade_plan`), `engine.py`
(`LiquiditySweepStrategy` — bar-by-bar pipeline state machine mirroring the Pine
script's own var-state cascading EXACTLY, including same-bar cascading where a later
stage can fire in the same closure as the stage that just unlocked it — e.g. FVG
confirmation and its own retest check can both fire on the very same bar, since the
FVG's own boundary is literally defined as that bar's own high/low; see
`tests/liquidity_sweep/test_engine.py`'s `test_full_pipeline_sweep_to_retest_entry` for
a fully-traced worked example and its own detailed comments on this), `book_manager.py`,
`__init__.py`. Execution: `execution_bridge/liquidity_sweep_bridge.py` (modeled
directly on `oi_flow_bridge.py` — same confirm-then-finalize contract, own
`_LiquiditySweepTradeLogger`/`_record_history()`). Dedicated per-(underlying,client,
binding,day) rotating log via `utils.logging_utils.make_strategy_logger` (own `_clog`,
same pattern as every other strategy) — explicit user requirement ("it shodul haev its
won log to knwo what exactly happend").

**Strategy name in DB**: `liquidity_sweep`. Registered in `strategies/registry.py`; run
with `--strategies liquidity_sweep` (add to the existing `--strategies` CLI flag
alongside whatever else is already running — NOT automatic, the operator must include
it explicitly at launch, same as every other strategy). Deploy form in `monitor.html`'s
ADD STRATEGY dropdown (default underlying NIFTY, `strategy_params='{}'` lets
`LiquiditySweepBookManager._parse_params()` fill in all the validated defaults above,
same pattern as OI-Flow's own deploy-form entry). Live panel on the deployment card
(pipeline stage, BoS/CHoCH bias, active liquidity levels, open position with spot SL/
T1/T2 + running P&L, recent remarks trail) via `GET /api/liqsweep/status` →
`LiquiditySweepStrategy.monitoring_state()`.

**Status (2026-08-19)**: built, unit-tested (`tests/liquidity_sweep/` — 21 detector
tests, 14 engine tests including a full traced sweep→displacement→FVG→retest→entry
pipeline test), wired into registry/run_system/dashboard/UI. **Not yet deployed even in
paper mode** — the user's own plan (stated before going offline) is to run this
tomorrow on NIFTY in paper mode on one client and review results. Like OI-Flow, this
strategy's option-side execution (strike selection, spot-to-premium translation) has
**zero validation** beyond what's documented above — only the underlying spot-signal
logic was validated (in Pine, on TradingView, against real chart data). Watch the first
few real paper sessions closely before trusting default parameters broadly, and revisit
`itm_offset_pts`/expiry choice once real forward data exists to tune them, same
graduation discipline already established for OI-Flow.

---

### Liquidity Trap Strategy (`strategies/liquidity_trap/`)

Option **buyer** strategy, distinct package from Liquidity Sweep above (different
mechanic, different files, same zero-shared-runtime mandate as OI-Flow/Liquidity
Sweep — own events, own Topics, own execution bridge, own book manager). **Live
deployed** on NIFTY/SENSEX — one of the three strategies actually running in
production right now, alongside SellStraddle and OI-Flow.

**Mechanic**: ref-candle (default 20m) rolling bias → SL-watch (ref candle's own
opposite level swept on a later ref-tf candle) → confirm-tf (default 3m)
single-fixed-reference confirmation → 1m CHoCH entry (half size, 2 lots) → 1:2
risk-reward SL/target off the confirm-tf sweep extreme → a 3-candle
(ref/sweep/reclaim) scale-in zone on 1m adds the other half (2 more lots, 4 total)
on a retrace into the lowest/highest third of that zone, SL/target unchanged by
the add-on. SL/Target are **spot-index levels**, not option premium — same
honest, deliberate design choice as Liquidity Sweep (no validated delta/greeks
model exists in this codebase); checked every spot tick. Entry executes 1-strike
ITM (`itm_offset_pts`, live-configured per-deployment — code default is `0.0`
(ATM); this was corrected live on 2026-08-20 to `50` (NIFTY) / `100` (SENSEX)
after a review of that day's live trades showed ATM was actually being used).

**MULTI-REF mechanic** (2026-08-21, superseded the original single-lock design):
every consecutive ref-tf candle pair independently spawns its own setup on a
clean one-sided breach, tracked fully in parallel (`self._setups` in
`engine.py`) — Stage 1-3 all run per-setup simultaneously regardless of what any
other setup is doing. Only ONE option position open at a time: a setup reaching
Stage 4 (CHoCH) while flat enters; while already in a position, both a
same-direction AND an opposite-direction CHoCH are skipped (**skip-if-blocked**,
not a flip-exit) — backtested as the higher-PF of the two variants on SENSEX
(1.81 vs 1.67 lot-weighted PF); flip was re-tested against real NIFTY data on
2026-08-22 and ties-or-beats skip on every NIFTY combo tested, but the edge is
marginal (PF +0.04 to +0.15) and the opposite of the SENSEX result — kept skip
everywhere rather than add asymmetric per-underlying exit-style logic to a live
engine for a marginal, underlying-inconsistent gain.

**Mid-day restart intraday warmup** (`_warmup_intraday()`): REST-fetches today's
1-min history and replays it through the exact same live pipeline before the
first live tick, gated by `self._warming_up` so replay never places real orders.
`self._today` is set **before** replay (not after) — the same critical-fix
pattern as every other strategy's warmup in this codebase — so the first live
tick's own new-day check can't silently wipe the just-replayed state via
`reset_session()`.

**Real-data-validated timeframe/trend-filter optimization** (2026-08-21,
`scripts/liquidity_trap_tf_and_trend_sweep.py`, 1-year real SENSEX spot via
Upstox): `ref_tf_min=20` / `confirm_tf_min=3` (was 15m/5m) + a `trend_tf_min=60`
/ `trend_sma_len=10` higher-timeframe trend filter (only enter WITH the trend —
current close above/below a simple SMA on the coarser series) together took the
backtest from 930 trades/77.8% win/PF 1.81 to 392 trades/82.7% win/PF 2.67 —
fewer trades, higher win rate, AND higher PF simultaneously, the only tested
config that hit all three. VWAP / change-in-OI / max-pain / open-interest
filters were explicitly considered and are **confirmed impossible to backtest**
— Upstox's historical index-candle API returns `volume=0`/`oi=0` on every row
for spot indices, same root limitation OI-Flow already hit. These are now the
LIVE defaults (`strategies/liquidity_trap/book_manager.py`'s `_DEFAULT_PARAMS`),
fully overridable per-deployment via `strategy_params`. Deploy dropdown label
(`monitor.html`) reflects this: "Liquidity Trap (Multi-Ref 20m/3m + 60m Trend
Filter)".

**NIFTY-specific optimization pass** (2026-08-22, `scripts/liquidity_trap_
nifty_tf_and_trend_sweep.py` + `scripts/liquidity_trap_nifty_full_optimization.py`,
1-year real NIFTY spot, 247 trading days / 92,310 1m bars): the prior pass only
ever ran against SENSEX data, so the same tf/trend sweep was repeated on NIFTY,
plus two more axes (target_mode `liquidity` vs `rr2`, exit-style `skip` vs
`flip`) that hadn't been swept for either underlying before. Result: **the
existing SENSEX-derived global default (20m/3m/liquidity-target/skip/60m-SMA10
trend) holds up well on NIFTY too** — PF 2.67, win% 81.3%, n=380 over the year —
confirming it wasn't accidentally a SENSEX-only tuning blindly applied
elsewhere. `target_mode=rr2` (fixed 2R target instead of the opposing-side
liquidity level) was confirmed worse on NIFTY too — win% collapses to 52-61%
despite a higher raw ₹ net, consistent with the existing design choice. SL
concept itself was not varied in either optimization pass — it is always the
swept reference candle's own extreme (a structural, price-action-anchored
stop) in every mode ever tested for this strategy; no alternate SL concept
(ATR-based, fixed-points, etc.) has been built or requested.

**Known live incident (2026-08-20, diagnosed only, not a codebase bug)**: a
live order was rejected by Dhan with `Expecting value: line 1 column 1 (char
0)` — root-caused to `dhanhq`'s `_parse_response()` calling `json_loads()` on
an empty HTTP body from Dhan's `/v2/orders` endpoint. Confirmed NOT a symbol-
resolution or codebase bug (verified correct via direct inspection); recurred
3 times same day across different underlyings/times — recommend checking the
Dhan account's own order-placement permissions if it recurs.

**Files**: `strategies/liquidity_trap/detector.py` (pure functions —
`find_all_setups`, `find_sl_hit`, `find_5m_confirmation`/configurable-tf
confirm, `find_choch_entry`, `compute_sl_target`, `find_scale_in_level`,
`compute_trend`), `engine.py` (`LiquidityTrapStrategy` — re-scans growing
per-day bar lists on every new bar close, exactly mirroring the backtest
script so the live engine can never behaviorally drift from what was actually
validated), `book_manager.py`, `events.py`. Backtest/optimization scripts:
`scripts/liquidity_trap_backtest.py` (original single-lock baseline, source of
truth the live engine was first ported from), `scripts/liquidity_trap_
multiref_backtest.py` (multi-ref + flip-vs-skip comparison), `scripts/
liquidity_trap_tf_and_trend_sweep.py` / `liquidity_trap_nifty_tf_and_trend_
sweep.py` (per-underlying tf/trend optimization), `scripts/liquidity_trap_
nifty_full_optimization.py` (target-mode + exit-style sweep), `scripts/
liquidity_trap_today_whatif.py` (what-if P&L reconstruction for a real trading
day using REST history, e.g. to sanity-check a broker-rejected order).

**Status (2026-08-22)**: live-deployed on NIFTY/SENSEX, actively running
alongside SellStraddle and OI-Flow (D1 Trap FnO/Index, FVG, and Liquidity
Sweep are built but not part of the current live rotation — see each
section above for their own status).

---

### OI-ORB Screener Strategy (`strategies/oi_orb_screener/`)

Option **buyer** strategy trading individual F&O **STOCKS** (not an index) — a
different composite signal from every other strategy above: NSE OI-Spurt list
∩ F&O price-move filter → NIFTY-regime-gated Opening Range Breakout (ORB) entry.
Ported 2026-08-24 from an already-working, independently-run **standalone Google
Colab script** (`colab/oi_orb_screener/screener_nse_direct.py`, confirmed live
against real NSE data the same day — see that file's own module docstring for
the full pipeline/regime-table spec and connectivity history) into the live EC2
app, per direct user instruction, as a **connectivity/plumbing proof**: does a
fired signal genuinely place a real (`paper_route`) broker order and subscribe
to that option's live LTP? Explicitly **NOT** the point of this pass to build
SL/target/trailing/risk-cap logic — **EOD square-off is the ONLY exit** this
pass. That comes in a follow-up before any real live capital sits behind this.

**Fully standalone**, same zero-shared-runtime mandate as OI-Flow/Liquidity
Sweep/Liquidity Trap — own package (`strategies/oi_orb_screener/`), own
order/fill events, own Topics (`Topic.OI_ORB_ORDER_REQUEST`/`OI_ORB_ORDER_FILL`),
own execution bridge (`execution_bridge/oi_orb_bridge.py`, modeled directly on
`oi_flow_bridge.py`'s paper_route contract), own book manager.

**First strategy in this codebase's live pipeline to trade a dynamically-chosen
underlying** — every other strategy's underlying is fixed at deployment time
(NIFTY/SENSEX/BANKNIFTY, or a stock list from D1 Trap FnO's WATCHLIST sentinel).
Here, `screener.build_shortlist()` picks a fresh set of F&O stocks every trading
day, so one book (per client/binding) can hold several concurrent positions —
one per shortlisted stock, keyed by stock symbol, not capped to 1 (direct user
answer, 2026-08-24). The deployment row itself stores the sentinel underlying
`"SCREENER"` (mirrors D1 Trap FnO's own `WATCHLIST` sentinel precedent) —
**`strategies.core.gate.can_trade()` is therefore gated on that sentinel, not
the real stock symbol**, inside `oi_orb_bridge.py` (`_GATE_UNDERLYING =
"SCREENER"`) — gating on the real per-order `underlying` would never match the
one real deployment row and would silently block every entry; found and fixed
before this ever ran for real, via the bridge's own test suite.

**Mechanic** (`strategies/oi_orb_screener/screener.py`, a faithful synchronous
port of the Colab script's pure logic — every NSE HTTP call wrapped in
`asyncio.to_thread()` at the call sites in `engine.py`, never on the event
loop): NSE OI-Spurt list (`avgInOI` field) ∩ F&O price-move filter (`NextApi/
apiClient/marketWatchApi?functionName=getIndicesData&symbol=SECURITIES+IN+F%26O`
— the correct, DevTools-confirmed endpoint; an earlier `equity-stock-indices`
guess was confirmed dead 2026-08-24, see the Colab script's own module
docstring for the full incident) → classify bullish/bearish/neutral off NIFTY's
own 09:15→09:30 move → 15-min Opening Range per shortlisted stock → 09:30–10:30
IST entry window, regime table gates which ORB breakout side is tradeable
(Bullish day: ORB-High→CALL, ORB-Low→PUT | Bearish day: ORB-High ignored,
ORB-Low→PUT | Neutral day: no trade). `REGISTRY.load_sync(stock)` (called only
if not already loaded — this is the first book in this codebase to call it
itself, since its underlyings aren't known at deployment time; confirmed via
direct code inspection to work for arbitrary NSE F&O stocks even without an
access_token, via the master-JSON fallback) resolves the real expiry/strike
contract (`strategies/oi_orb_screener/stock_resolve.py`) — lot size/strike step
come from the curated `FNO_STOCK_CONFIG` fast path first, else a small
independent Upstox-instrument-master lot lookup + the same price-band strike-
step heuristic the Colab script already carries (flagged there as unverified
against a real broker chain — fine for a `paper_route` pass, not real capital).
The option feed is subscribed **before** the order is placed (unlike every
other strategy here, which subscribes only after a fill) — `engine.py` waits up
to 5s for a real live tick so `entry_price` is a genuine LTP, not a guess; that
same subscription is what fulfills the "track the LTP" requirement afterward.

**Files**: `screener.py` (pure pipeline logic — NSESession, shortlist build, ORB
bars, regime, `evaluate_breakout`), `stock_resolve.py` (contract/lot/step
resolution), `events.py`, `engine.py` (`OiOrbScreenerStrategy` — one book per
client/binding, `_daily_loop`/`_run_today_pipeline` mirrors the Colab script's
`run_screener_and_monitor()` adapted to a non-blocking asyncio loop with day-
rollover instead of a single run-then-exit script), `book_manager.py`.
Execution: `execution_bridge/oi_orb_bridge.py`.

**Strategy name in DB**: `oi_orb_screener`. Run with `--strategies
oi_orb_screener`. No dashboard deploy form this pass (matches how OI-Flow/
Liquidity Sweep both first shipped) — seed one `strategy_deployments` row via
`scripts/seed_oi_orb_screener_deployment.py`, run **on the EC2 server itself**
after `git pull` (`data/*.db` is gitignored — this repo's local dev copy of
`data/clients.db` is a separate file from EC2's real one, confirmed via direct
inspection during this build; a row inserted locally never reaches production).

**Status (2026-08-24)**: built, unit-tested (`tests/oi_orb_screener/`,
`tests/execution/test_oi_orb_bridge.py`), registered, wired into
`run_system.py`. **Not yet deployed** — awaiting the EC2-side deployment-row
seed + a `pm2 restart` (the same restart the user plans to also start
`sell_straddle` in, per direct instruction not to restart the process
repeatedly in one day). First live check: client `ssrajpal2001`, binding
`SA5770` (Zerodha, already `trading_mode='paper_route'`). Before any further
scale-up or SL/target work: confirm from real logs that (1) a signal actually
resolves a contract and places a real paper_route order, and (2) the option's
live LTP ticks are genuinely arriving — the exact two things this pass exists
to prove.

**Dashboard UI (2026-08-24)**: now selectable in the ADD STRATEGY deploy form
(`monitor.html`) — underlying is a fixed, disabled `SCREENER` selector (the
screener itself picks real stocks daily, not a per-deployment choice), same
pattern as FnO Positional's `FNO_STOCKS` sentinel. A live panel on the
deployment card (`GET /api/oiorb/status` → `OiOrbScreenerStrategy.
monitoring_state()`) shows today's shortlist, NIFTY regime, frozen ORB levels
per stock, and every currently OPEN position with its live LTP — this book can
show several open positions at once (one per shortlisted stock), unlike every
other strategy's single-position panel.

**Temporary connectivity-test toggle (`strategy_params.ignore_time_windows`,
default `False`)**: bypasses the real 09:10 start-gate, the 09:30 ORB-freeze
time, and the 09:30–10:30 entry window entirely — ORB freezes immediately off
whatever bars exist (the Yahoo backfill still covers the real elapsed 09:15–
09:30 session regardless of what time the book actually starts), so the whole
pipeline can be verified end-to-end outside the real window (e.g. right after
an afternoon deploy) without waiting for the next morning. **Must be turned
back to `False`** (or simply left unset) once connectivity is confirmed — real
trading days should always run under the genuine ORB/regime timing this
strategy was actually designed around.

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
