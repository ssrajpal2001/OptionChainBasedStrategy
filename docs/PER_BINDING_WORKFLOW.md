# Per-Binding SellStraddle — Runtime Workflow

**What happens when N brokers run the SAME strategy at DIFFERENT times.**

## Spawn / lifecycle
1. Launch: `run_system.py --strategies sell_straddle --index NIFTY,...`. The **feeder** subscribes
   index ticks + ATM±N option strikes **per index** (shared, client-agnostic). `StraddleBookManager`
   starts and reconciles the DB every 5 s.
2. A client **deploys** sell_straddle on a broker binding (UI). Within ~5 s the manager **spawns a
   book** for `(client, binding, index)` → log line `StraddleBookManager: spawned book C/B/UND`.
3. A client **un-deploys** → its book is stopped and dropped. Other books are untouched.

## What each book does (independently)
- Reads the **shared** market feed (same EventBus ticks) and the **shared admin generic rules**
  (`RuntimeConfig.index_section(index)`); computes its own indicators in its own pool engine.
- **Gates only on ITS binding's Terminal ON + Trade ON.** Other clients' state is irrelevant.
- Takes its **own beginning entry** the first time it is gated-ON and the rules pass — so the
  **strikes depend on the market at THAT moment**.
- Runs its own rolls / exits / re-entries / day-targets / cooldown on its own position.
- Emits orders tagged with its `client_id`/`binding_id` → bridge routes to **ONLY that broker**.
- Persists its own position (`{client}_{binding}_{und}_sell_straddle`) → restart restores per book.

## Example — N brokers, different start times (the answer to "what exactly happens")
- **Broker A turns ON at 09:20.** Book A evaluates the rules at 09:20; if they pass it sells, say,
  CE 23150 / PE 23100 (the balanced ATM pair for 09:20's spot/IV). Book A now manages that.
- **Broker B turns ON at 11:00** (market moved, spot higher). Book B starts **fresh** — it does NOT
  see or inherit Book A's position. It evaluates the rules at 11:00 and sells, say, CE 23300 /
  PE 23250 (the ATM pair for 11:00). Different time → **different strikes**, fully independent.
- **Broker C turns ON at 13:30**, etc. — same story, its own entry for 13:30's market.
- Each book rolls/exits on its **own** triggers; a roll/exit on A never touches B or C.
- Orders fire to **each broker separately**. With no funds (dry test) they reject per broker —
  proving routing/independence with zero money risk.

## Different clients, different SCRIPTS (indices)
A book is per `(client, binding, **index**)`. So a client can deploy sell_straddle on NIFTY on one
binding and CRUDEOIL on another — two independent books. Two clients on different indices are
naturally independent. Two clients on the **same** index are independent too (separate books).
Requirement: the process must be launched with each index in `--index` so the feeder subscribes it.

## Feeder load (the core)
**Unchanged as clients grow.** Books read the same shared per-index tick stream; only indicator CPU
duplicates per book. Feeder load = indices × strikes × tick-rate, never × clients. The EventBus
drops on slow consumers, so a slow book can never back-pressure the feed.

## Fresh start (wipe all trades/history/positions)
```bash
cd ~/OptionChainBasedStrategy
pm2 stop algo
rm -f data/positions/*.json data/straddle_participants.json
rm -f data/history/*.json
rm -f logs/clients/ss_*.log logs/trades/*.log
pm2 start algo   # or the explicit pm2 start ... --index ... --strategies sell_straddle
```
This clears persisted positions, the History ledger, and per-binding/trade logs so the app starts
clean. (`data/` is gitignored — these are runtime files only.)
