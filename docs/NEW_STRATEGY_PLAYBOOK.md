# New-Strategy Playbook — Plug-and-Play on the Shared Feed

How to build a **new strategy** (in this session or a fresh one) that **reuses the data
feed, broker routing, and per-binding lifecycle** already built here. Goal: write only the
*strategy brain*; inherit everything else.

---

## The big idea

This app is already split into reusable layers. A new strategy plugs into them — it does
**not** re-implement feeds, auth, or order routing.

```
 ┌─────────────────────────────────────────────────────────────┐
 │ FEED LAYER  (one broker session, shared)                     │
 │   run_feed_server.py --dual   → FeedServer TCP hub :15765    │
 │   Upstox + Fyers → JSON ticks fanned to ALL clients          │
 └───────────────┬─────────────────────────────────────────────┘
                 │  (TCP 15765, newline-JSON)
 ┌───────────────▼─────────────────────────────────────────────┐
 │ FEEDER ADAPTER   data_layer/shared_feed_client.py            │
 │   SharedFeedClient → publishes INDEX_TICK / OPTION_TICK      │
 │   onto a local EventBus (config.primary_feeder_provider=     │
 │   "shared")                                                  │
 └───────────────┬─────────────────────────────────────────────┘
                 │  EventBus (asyncio pub/sub)
 ┌───────────────▼─────────────────────────────────────────────┐
 │ YOUR STRATEGY  (the only new code)                           │
 │   <Name>Engine  — one book per (client, binding, index)      │
 │   <Name>BookManager — reconciles DB deployments → spawns     │
 │   emits *OrderEvent → execution_bridge                       │
 └───────────────┬─────────────────────────────────────────────┘
                 │  Topic.*_ORDER_REQUEST
 ┌───────────────▼─────────────────────────────────────────────┐
 │ EXECUTION LAYER  execution_bridge/ (reuse as-is)             │
 │   routes to ONLY the (client,binding) broker; fills tracked  │
 └─────────────────────────────────────────────────────────────┘
```

---

## Two ways to share the feed

**Mode A — same process (simplest, recommended for strategies that live in THIS repo).**
Your strategy is a module here and reads the same in-process `EventBus` that the feeder
publishes to. Start the system normally; add `--strategies <yourname>`. No TCP needed.

**Mode B — separate process / separate repo (true plug-and-play, multi-app).**
Run the feed once: `python run_feed_server.py --dual` (TCP :15765). Your strategy is its
own process/app that embeds `SharedFeedClient(bus, cfg, host, 15765)` → `await client.connect()`
→ it gets `INDEX_TICK`/`OPTION_TICK` on its own local EventBus. This is how a strategy built
in a *different session/codebase* shares this machine's single broker feed. The TCP protocol
is documented in `CLAUDE.md` and is wire-compatible with `Option_Selling_May_2026`.

> Data feed = Fyers/Upstox (read-only). Order execution = client brokers (Zerodha/AngelOne/…).
> Never route data through a client broker or orders through the feeder.

---

## The strategy contract (what you actually write)

Copy the **per-binding book pattern** already used by `sell_straddle` + `trap_scanner`:

1. **`<Name>Engine`** (`strategies/<name>_engine.py`) — one independent book per
   `(client_id, binding_id, underlying)`. It:
   - subscribes to `Topic.INDEX_TICK` / `Topic.OPTION_TICK` / `Topic.CANDLE_CLOSE` on the bus
   - holds its own state + position; logs to `logs/clients/<name>_{UND}_{cid}_{bid}_{date}.log`
   - gates entries with a `_can_trade()` check (THIS binding's `terminal_connected=1 AND
     is_trade_enabled=1`) — copy `TrapScannerEngine._can_trade()`
   - emits a strategy `OrderEvent` tagged with `client_id`/`binding_id` so the bridge routes
     to **only that broker** (no mirror)
2. **`<Name>BookManager`** (`strategies/<name>_book_manager.py`) — copy
   `strategies/trap_book_manager.py` almost verbatim. Reconciles DB deployments every 5s,
   spawns a book when a deployment is `is_running=1`, stops it on un-deploy, re-spawns on
   lot/expiry change.
3. **Register it in `strategies/registry.py`** — add an entry to `STRATEGY_REGISTRY`
   instead of editing `run_system.py`:

   ```python
   from strategies.my_strategy import MyBookManager

   STRATEGY_REGISTRY = {
       # ... existing entries ...
       "my_strategy": {
           "manager_class": MyBookManager,
           "per_binding": True,
       },
   }
   ```

   `run_system.py` will automatically construct the manager when `--strategies my_strategy`
   is enabled, wire the rebalancer/feeder, start its `run()` task, and stop it on shutdown.
4. **Execution** — reuse `execution_bridge`. If your order shape matches straddle/IC, reuse
   their bridge; otherwise add a thin bridge that listens on a new `Topic.*_ORDER_REQUEST` and
   calls `router`/broker `place_order` → `get_order_status` (mirror `ic_bridge`).

That's it — feed, auth, registry, broker routing, persistence, dashboard plumbing are inherited.

---

## Adding a 4th strategy — minimal skeleton

Create a new sub-package (this is documentation/example only; it is **not** registered by
default):

`strategies/my_strategy/__init__.py`:
```python
from strategies.my_strategy.book_manager import MyBookManager
from strategies.my_strategy.engine import MyStrategy

__all__ = ["MyBookManager", "MyStrategy"]
```

`strategies/my_strategy/engine.py`:
```python
from strategies.core.base_book import AbstractStrategyBook

class MyStrategy(AbstractStrategyBook):
    """One independent trading book per (client, binding, underlying)."""

    def reset_session(self) -> None:
        pass

    async def _tick_loop(self):
        # consume Topic.INDEX_TICK via self._subscribe(Topic.INDEX_TICK)
        pass
```

`strategies/my_strategy/book_manager.py`:
```python
from strategies.core.book_manager import StrategyBookManager
from strategies.my_strategy.engine import MyStrategy

class MyBookManager(StrategyBookManager):
    def _wanted(self):
        # Query client_db for running deployments of this strategy
        return {}

    def _spawn_book(self, key, value):
        client_id, binding_id, underlying = key
        return MyStrategy(self._bus, self._cfg,
                          underlying=underlying,
                          client_id=client_id,
                          binding_id=binding_id)
```

Then register it in `strategies/registry.py` as shown above and run with
`--strategies my_strategy`.

> For a concrete, importable example see `strategies/sample_strategy/`.

---

## Reuse catalog (import, don't rewrite)

| Need | Reuse |
|------|-------|
| Live ticks (shared) | `data_layer/shared_feed_client.py` → `SharedFeedClient`; or in-proc `EventBus` |
| Event bus / tick types | `data_layer/base_feeder.py` (`EventBus`, `IndexTick`, `OptionTick`, `CandleEvent`) |
| Topics | `config/global_config.py` → `Topic.*` |
| Strike ↔ broker symbol | `data_layer/symbol_translator.py`, `data_layer/instrument_registry.py` (`REGISTRY`) |
| ATM tracking / auto-subscribe | `data_layer/strike_rebalancer.py` |
| Per-strike VWAP/SLOPE/RSI/ROC (broker ATP) | `strategies/pool_indicator_engine.py` |
| Candles / indicators | `matrix_engine/candle_cache.py`, `matrix_engine/indicators.py` |
| Multi-broker order routing + fills | `execution_bridge/` (`ExecutionRouter`, `*_bridge`) |
| Clients / bindings / deployments / creds | `data_layer/client_db.py` |
| Per-binding lifecycle template | `strategies/trap_book_manager.py` |
| Per-index lifecycle wrapper | `strategies/core/per_index_manager.py` |
| Terminal/Trade gate template | `TrapScannerEngine._can_trade()` |
| Backtest data fetch (Upstox 1m) | `scripts/nifty_backtest.py` (`_fetch_1m`, `_option_key`, `_resample`, `_mkt_hours`) |

---

## Prompt to paste into a NEW Claude session

> I'm building a new options strategy called **`<NAME>`** in this repo
> (`OptionChainBasedStrategy`). Read `CLAUDE.md` and `docs/NEW_STRATEGY_PLAYBOOK.md` first.
>
> Constraints / reuse (do NOT rewrite these): share the existing data feed
> (`SharedFeedClient`/`EventBus`, never a client broker for data); reuse `instrument_registry`,
> `symbol_translator`, `strike_rebalancer`, `execution_bridge` (orders routed per
> `(client,binding)` only), `client_db`, and — if I use VWAP/SLOPE/RSI/ROC —
> `pool_indicator_engine`. Follow the **per-binding book pattern**: one `<NAME>Engine` per
> `(client,binding,index)` + a `<NAME>BookManager` copied from `trap_book_manager.py`, gated by
> `_can_trade()` (terminal+trade ON). Register the new strategy in `strategies/registry.py`; do
> not hard-code it in `run_system.py`. Only the existing strategies are live by default — add
> mine as an optional registered strategy.
>
> The strategy logic is: **<describe entry / exit / strike selection / timeframe here>**.
>
> Deliver: `strategies/<name>/__init__.py`, `strategies/<name>/engine.py`,
> `strategies/<name>/book_manager.py`, registration in `strategies/registry.py`, a thin
> execution bridge if needed, and unit tests. Then a backtest script under `scripts/` reusing
> `nifty_backtest`'s fetch helpers.

Fill in `<NAME>` and the logic line; everything else is boilerplate the session inherits.

---

## Footguns (carried from live ops)

- VWAP = **broker ATP** from the live feed — it is NOT in Upstox historical candles, so any
  VWAP/SLOPE strategy can only be *approximated* in backtest (use TrueData tick data for a
  faithful run).
- Lot sizes are exchange-fixed (NIFTY 65, SENSEX 20, CRUDEOIL 100). MCX squareoff ≈ 23:25.
- Orders must carry `client_id`/`binding_id`; the bridge must route to that broker ONLY
  (shared-account safe — never net-flatten a broker).
- A new strategy needs an entry in the dashboard `allowed_strategies` sets + a deployment row
  to spawn a book.
