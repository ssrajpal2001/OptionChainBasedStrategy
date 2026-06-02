# Per-Broker Position Tracking & Display (Design)

**Date:** 2026-06-02
**Problem:** The dashboard shows a *strategy's* position under **every** broker
assigned to that strategy — regardless of whether that broker actually filled it.
E.g. "Zerodha shows a CRUDEOIL straddle it never placed" (the global
`SellStraddle[CRUDEOIL]` entered in memory; no order routed to Zerodha, yet the UI
shows the position under Zerodha). Positions are tracked at the **strategy** level
(one global instance per index), not per **broker/binding**.

## Goal
Show, per **(client, binding)**, the positions that **that broker actually holds**
— driven by real fills — so each broker card reflects reality.

## Root cause
- Strategies are global (one `SellStraddle[NIFTY]`, `IronCondor[NIFTY]`, …).
- They publish ORDER_REQUEST → bridges fan out to engine-active bindings → fills.
- `StraddleFillEvent` / `ICFillEvent` already carry `client_id`, `binding_id`,
  `action` (ENTRY/EXIT), strikes and fill prices.
- The dashboard `/api/client/positions` reads the **global strategy** position, not
  the per-binding fills → the conflation.

## Design

### 1. `data_layer/broker_positions.py` — per-binding open-position store
In-memory dict (optionally JSON-persisted, MIS new-day discard) keyed by
`(client_id, binding_id, strategy, underlying)` → record:
```
{ strategy, underlying, legs: [ {side, option_type, strike, entry_price, ltp} ],
  open_time, paper_mode }
```
API:
- `open(client_id, binding_id, strategy, underlying, legs, paper_mode)`
- `update_ltp(underlying, strike, option_type, ltp)` — bump matching legs' ltp
- `close(client_id, binding_id, strategy, underlying)`
- `for_client(client_id) -> list[record]`

### 2. Feed it from the fills (the bridges)
The bridges are the single truth point — they know what **actually** filled and for
**which binding**:
- `StraddleExecutionBridge` on ENTRY fill → `broker_positions.open(...)` with the
  CE/PE legs; on EXIT fill → `broker_positions.close(...)`.
- `ICExecutionBridge` likewise with the 4 legs.
- (Trap, when it routes orders, the same.)
Only **real routed fills** create a record → a broker that didn't fill has none.

### 3. Live LTP for P&L
A single subscriber to `Topic.OPTION_TICK` (in the bridge or a small task) calls
`broker_positions.update_ltp(underlying, strike, type, ltp)` so each binding's legs
price live. P&L per leg = `(entry−ltp)×qty` for shorts, `(ltp−entry)×qty` for longs,
`qty = lot_size × lot_multiplier` from the binding.

### 4. Dashboard endpoint
Rewrite `/api/client/positions` to read `broker_positions.for_client(cid)` and group
by `binding_id` → each broker card shows **only its own** filled legs + P&L. The
current "read global strategy instance" path is removed (or kept as an admin-only
"strategy view").

## Data flow
```
strategy ENTRY → bridge routes to engine-active bindings → fill per binding
   → broker_positions.open(client, binding, strategy, underlying, legs)
OPTION_TICK → broker_positions.update_ltp(...)
strategy EXIT → bridge → broker_positions.close(client, binding, ...)
dashboard /api/client/positions → broker_positions.for_client(cid) grouped by binding
```

## Testing
- Unit: `open`→`for_client` returns the record; `close` removes it; `update_ltp`
  bumps the right leg; new-day MIS discard.
- Unit: two bindings, fill on only one → only that binding has a position.
- Integration: simulate a straddle ENTRY fill on binding A (not B) → A shows the
  position, B shows none (fixes the "Zerodha showing a trade it didn't place").

## Migration / safety
- Additive store; the dashboard switch is the only behavioural change.
- Paper fills also create records (so paper brokers still show their sim positions).
- Keep the strategy-level view available to admins for debugging.

## Out of scope
- Reconciliation against the broker's *actual* positions API (future).
- Cross-binding netting.
