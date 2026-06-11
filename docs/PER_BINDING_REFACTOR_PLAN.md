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
- [ ] P1 order tagging + routing
- [ ] P2 shared per-index context
- [ ] P3 per-binding spawn at startup
- [ ] P4 dynamic spawn/stop on deploy
- [ ] P5 UI/P&L per binding
