# Continuous Per-Strike Indicator Engine (Sell-Straddle) — Design

**Goal:** Maintain VWAP/SLOPE/RSI/ROC continuously per pool strike (independent of the active
position) so that entries, re-entries, and rollovers always read **warm** indicators — eliminating
the false `vwap_rise_sl` / `SLOPE=-146` / `RSI=1.69` mis-fires caused today by the series resetting
on every re-entry.

**Architecture:** A pool-scoped indicator engine subscribes the full strike range around ATM and
keeps a rolling 1-min series of `(ltp, atp)` per strike. Any candidate pair's combined indicators
are computed on-demand by summing the two strikes' aligned series. The straddle reads this engine
for both selection and exit-evaluation instead of computing on the active legs only.

**Tech:** Python/asyncio, existing EventBus (OPTION_TICK / CANDLE_CLOSE), existing instrument
registry + curl_cffi historical fetch (reused from the trap engine).

---

## Confirmed design decisions (from the user)

1. **Per-strike** series (not per-pair) — any pair reconstructs its combined series from the two
   strikes. Most flexible; matches "subscribe all pool strikes".
2. **1-min candle closes** are the series granularity; indicators resample to each rule's `tf` (1/2/5).
3. **Pool range** = explicit config `pool_itm_depth` + `pool_otm_depth` (the "missing setting"),
   covering ITM + ATM + OTM. Replaces the implicit `pool_offset`/`chain_depth` for the straddle.
4. **Never unsubscribe the running position's two legs** — they are pinned even if ATM drifts and
   they leave the pool range. Only non-position strikes that leave the range get unsubscribed.
5. **VWAP/ATP is intraday-fresh** (Upstox/Fyers ATP field, resets daily) — needs NO historical seed.
6. **RSI/ROC seed from prev-day historical 1-min candles** — fetch prev trading day's 1-min bars per
   pool strike; if prev day is a holiday/empty, **step back one day at a time until candle data is
   found** (reuse the trap engine's holiday step-back). Save the fetched history.
7. **Full exit → cooldown**, then fresh entry on warm data. **Rollover → immediate** (keep running
   leg, pick partner from warm pool data, re-enter same tick).

---

## Components

### 1. `PoolIndicatorEngine` (new, pure-ish module)
- **State:** `series[(strike, side)] -> deque[(ts, ltp, atp)]` (1-min closes, maxlen ≈ enough for
  RSI/ROC length + margin).
- **Inputs:** OPTION_TICK (latest ltp/atp per strike) + CANDLE_CLOSE (commit a 1-min bar per strike).
- **Seed:** on first subscribe of a strike, fetch prev-day 1-min bars (holiday step-back) and prefill
  the deque so RSI/ROC are valid immediately. VWAP/ATP come live (intraday).
- **API:**
  - `update_tick(strike, side, ltp, atp)` — track latest.
  - `commit_bar(now)` — push a 1-min close per tracked strike.
  - `pair_indicators(ce_strike, pe_strike) -> {close, vwap, slope, rsi, roc}` — combined series:
    `close = ce_ltp+pe_ltp`, `vwap = ce_atp+pe_atp`, `slope = Δvwap`, `rsi/roc` on combined-close series.
  - `is_warm(strike) -> bool` — enough bars (seeded or live) for RSI/ROC.

### 2. Pool subscription manager (extend StrikeRebalancer or a thin wrapper)
- Compute pool set = `ATM-pool_itm_depth … ATM+pool_otm_depth` (both CE & PE).
- On ATM change: subscribe new, unsubscribe leavers **minus the pinned running legs**.
- Pin/unpin the active position's CE/PE strikes on open/close.

### 3. Sell-straddle wiring (modify `strategies/sell_straddle.py`)
- Replace `_recompute_indicators`/`_active_premium`-derived series with reads from
  `PoolIndicatorEngine.pair_indicators(active_ce, active_pe)` for exit checks.
- Selection (`select_balanced_pair`/`scan_pool`/`select_partner_for`) rule-eval reads
  `pair_indicators` for candidate pairs (warm) instead of the active-only series.
- Remove the per-position series reset on entry/roll (the source of the false exits).

---

## Data flow

```
OPTION_TICK / CANDLE_CLOSE
        │
        ▼
PoolIndicatorEngine  ── seeds prev-day RSI/ROC (holiday step-back), tracks per-strike 1-min series
        │  pair_indicators(ce,pe)  (warm, on-demand)
        ▼
SellStraddle:  entry-gate / re-entry-gate / roll-partner / exit-eval  ──►  orders
        │
        ▼
Subscription mgr: keep pool ± depth subscribed; PIN running legs (never unsubscribe)
```

---

## Behaviour matrix

| Event | Indicator source | Cooldown? |
|---|---|---|
| First entry | warm pool data (prev-day seed + live VWAP) | n/a |
| Rollover (ratio/decay) | warm pool data — running leg never unsubscribed | **No** (immediate) |
| Full exit (vwap_rise/roc/exit_rules) | — | **Yes** (existing cooldown) |
| Re-entry after cooldown | warm pool data | n/a |

---

## Error handling
- Historical seed fails (network/holiday-exhausted) → fall back to live-only warm-up, gate RSI/ROC
  with `is_warm()` until enough live bars exist; VWAP/ATP still usable immediately.
- A pool strike has no ticks (illiquid) → its series is stale; pair_indicators flags it; selection
  skips pairs with a stale leg.
- Never let an engine exception break the strategy loop (defensive try/except, log once).

## Testing
- `PoolIndicatorEngine` is unit-testable in isolation: feed synthetic ticks/bars, assert
  `pair_indicators` matches hand-computed VWAP/SLOPE/RSI/ROC; assert `is_warm` after seed.
- Holiday step-back: mock the historical fetcher to return empty for N days, assert it walks back.
- Pin invariant: simulate ATM drift, assert running legs stay subscribed.

## Open / deferred
- Reconcile with the trap engine's own historical fetcher (share one helper).
- Per-strike series memory: pool ~ (itm+otm+1)×2 strikes × deque — bounded, fine.
- The broker-feed (Approach B) is orthogonal — this engine is the data-feeder (entry brain) side.
