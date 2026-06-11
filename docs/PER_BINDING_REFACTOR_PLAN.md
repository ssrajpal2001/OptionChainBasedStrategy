# Per-Binding Independent Straddle — Refactor Plan

**Goal (user, 2026-06-11):** every `(client, broker-binding, index)` deployment runs its OWN
independent SellStraddle — its own beginning entry anchored to when *that* terminal turns ON, its
own strikes, rolls, exits and P&L. A client starting at 09:20 and one starting at 11:00 each get
their own entry for their own market moment. **Nothing is shared between clients except the
admin-configured generic strategy rules.** Market data/indicators are shared per index.

## Root cause being fixed
Today: ONE `SellStraddleStrategy` per index, ONE `_position`, mirrored to all engine-active
brokers. So a client who turns ON mid-position **inherits** the running position (wrong) and can
place naked exit orders for a trade it never entered (dangerous).

## Target architecture
- One strategy **book** per `(client, binding, index)` deployment with its own position/lifecycle.
- **Shared per index:** `PoolIndicatorEngine` + strike subscription + spot/option tick cache
  (compute once, all books on that index read it).
- **Per binding:** position, entry timing, strikes, rolls, exits, persistence key, product type,
  lot multiplier, gating (own Terminal+Trade), P&L.
- **Shared (read-only):** admin generic rules via `RuntimeConfig.index_section(index)`.
- Orders tagged with `client_id`/`binding_id`; bridge routes to **only that broker** (no mirror).
- Books spawn at startup from deployments AND on-deploy (auto-start), stop on un-deploy/Trade-OFF.

## Decision: implementation shape
Per-binding **instances** of `SellStraddleStrategy` (reuse existing single-position logic) keyed by
`(client, binding, index)`, sharing a per-index market-data context. (Chosen over a per-book dict
inside one engine to minimise changes to the heavily `self._position`-centric internals.)

## Phases
1. **Order tagging + per-binding routing.** Add `client_id`/`binding_id` to `StraddleOrderEvent`;
   strategy stamps them; bridge routes a tagged event to ONLY that binding (drop mirror-to-all).
2. **Shared per-index market context.** Extract pool engine + strike subscription + tick caches
   into a shared object injected into every book for that index.
3. **Per-binding spawn at startup.** run_system builds one book per existing sell_straddle
   deployment; per-binding persistence keys + gating (own terminal/trade only).
4. **Dynamic spawn/stop on deploy / Trade-OFF** (delivers auto-start-on-deploy too).
5. **UI/P&L** read each binding's own book (panel already per-deployment); per-binding history.

## Data-feeder load (CRITICAL — the core of the system)
**The per-binding refactor adds ZERO extra load to the data feeder**, because market data is
**shared per index**, not per client:
- ONE feeder session (DualFeeder Upstox+Fyers / FeedServer) subscribes index ticks + ATM±N option
  strikes **per index** — independent of client count.
- The **shared per-index context** drains the EventBus ONCE per index, updates the pool engine +
  tick caches once; books READ that shared state (books do NOT each subscribe to the feed).
- 1 client or 100 clients on NIFTY → identical NIFTY subscription + identical tick processing.
- Per-binding books only add cheap CPU for position decisions on already-computed indicators.

Feeder load = (indices monitored) × (ATM±N strikes) × (tick rate) — a function of INDICES, never
of clients. To protect the core: keep the shared session, bound the strike window (rebalancer),
keep EventBus drop-on-slow-consumer (no back-pressure to the feed), run `run_feed_server.py` as a
separate always-on process so strategy restarts never drop the feed, and keep Upstox+Fyers
active-active so one provider lagging doesn't stall the app.

## Invariants to preserve
- VWAP = broker ATP (never computed); shared pool engine keeps live-bars-only semantics.
- Booked P&L from History ledger (already per client/binding).
- Restart restores each book's own position (per-binding persistence key).
- Cooldown on full exit, single-side roll keeps losing leg, near-ATM cap — all unchanged, now
  evaluated per book.

## Status
- [x] **P1 order tagging + routing** — `StraddleOrderEvent.client_id/binding_id`; bridge routes to
  ONLY the stamped binding (empty = legacy mirror). Commit `d7742ab`, tests in
  `tests/execution/test_straddle_targeted_routing.py`.
- [ ] P2 shared per-index context
- [x] **P3a per-binding book identity** — `SellStraddleStrategy(client_id, binding_id)`: own
  persist key, `_emit_order` stamps tags, `_any_active_terminal` gates on own binding only.
  Commit `9dd4853`, tests `tests/strategies/test_per_binding_identity.py`.
- [ ] **P3b spawn per-binding instances in run_system** ← NEXT (the make-it-work piece)
- [ ] P4 dynamic spawn/stop on deploy
- [ ] P5 UI/P&L per binding

## P3b — spawn per-binding instances (next focused step, the make-it-work piece)
`run_system.py` lines ~395-399 currently: one `SellStraddleStrategy` per `cfg.monitored_indices`.
Change to: **one instance per (client, binding) sell_straddle deployment**, each constructed with
`client_id=`/`binding_id=`, wired (`set_feeder`/`set_rebalancer`/`set_client_db`) and started like
today. Key edits:
- Read deployments at startup (`client_db.get_all_clients_sync` → `get_deployments_sync`) and build
  the instance list keyed `(client, binding, underlying)`.
- `_sell_straddles` becomes that list; everything that iterates it (bridge square-off, UI `_find`)
  must match on `(client_id, binding_id, underlying)` not just underlying.
- UI `api_client_positions` `_find`: locate the instance for THIS deployment's `(cid, bid, und)`.
- Dry-test MVP keeps each instance's OWN pool engine (duplicated) — fine for ~10 clients; the
  shared-per-index context (P2) is a later optimisation, not needed to dry-test.
- P4 (dynamic spawn on deploy) and P5 (UI) follow.
**Risk note:** this is the piece that turns the tested primitives into a running system; do it with
full focus + a paper/dry run before any funded live use.

## Phase 2 — precise starting point (handoff)
**Goal:** one shared market context per index; books read from it (books do NOT subscribe to the feed).
1. New class e.g. `strategies/straddle_market_context.py` `StraddleIndexContext(underlying)` owning:
   - the `PoolIndicatorEngine` (today `SellStraddleStrategy._pool_engine`),
   - the per-strike `_strike_prem` cache + `_spot` + `_ce_ltp/_pe_ltp` ATM caches,
   - the option/index tick draining (one subscriber per index) + `commit_bar` per 1-min,
   - the strike subscription/pin/rebalance hooks.
2. It exposes read methods books call: `pair_indicators_tf(ce,pe,tf)`, `strike_prem`, `spot`,
   `atm`, `active_premium(ce,pe)`, warm/seed helpers.
3. `SellStraddleStrategy` keeps ONLY per-book state (position, entry timing, rolls, exits, day
   targets, cooldown, chart series) and takes a `ctx: StraddleIndexContext` instead of owning the
   pool engine / tick loops. Its `_tick_loop`/option handling delegates market reads to `ctx`.
4. Keep behaviour identical for a single book first (one ctx + one book per index) → run full
   suite → only THEN move to many books per index (P3).

**Invariant check during P2:** VWAP=broker ATP, live-bars-only SLOPE, seed-warm RSI/ROC must be
byte-for-byte the same (the pool-engine moves wholesale into ctx; do not change its logic).
