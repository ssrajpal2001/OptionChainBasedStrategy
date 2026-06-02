# Iron Condor — min-LTP Expiry Shift (Design)

**Date:** 2026-06-02
**Goal:** When the IC's short legs are too cheap on the current weekly expiry
(near-expiry far-OTM premium ≈ 0, e.g. live `net_credit=1.70`), automatically
**shift to the next weekly expiry** (more time value) until both short premiums
meet a per-index `min_ltp` floor.

## Confirmed decisions
- **Forward pricing:** subscribe the next-week candidate strikes (live LTP), same
  as the current week — no REST.
- **Look-forward:** up to **2 expiries** (current + next). If neither qualifies → skip entry.
- **`min_ltp`:** per-index config (`indices.<IDX>.iron_condor.min_ltp`).
- Weekly indices (NIFTY, SENSEX): "next week". Monthly (BANKNIFTY, FINNIFTY):
  "next month" — both are just "the next entry in `all_expiries`".

## Current state (what exists)
- `InstrumentRegistry.all_expiries(underlying)` → sorted `List[date]` (exists).
- `InstrumentRegistry.get_active_expiry(underlying, from_date)` → current expiry (exists).
- IC prices via `self._prem_cache[f"{underlying}{strike}{type}"]` — **NOT expiry-aware**
  (two expiries with the same strike collide). `_min_ltp` currently only **blocks** entry.
- StrikeRebalancer subscribes ATM±`chain_depth` for the **active expiry only**.

## Components

### 1. Registry — candidate expiry list
Add `IronCondorStrategy` helper using existing registry:
`_candidate_expiries() -> List[date]` = the first **2** entries of
`registry.all_expiries(underlying)` that are ≥ today (current + next).

### 2. Expiry-aware premium cache (IC)
Change `_prem_cache` key to include expiry:
`key = f"{underlying}:{expiry.isoformat()}:{int(strike)}{type}"`.
`_option_loop` already receives `OptionTick.expiry` — use it to build the key.
A new helper `_prem(expiry, strike, opt_type) -> float` reads the expiry-aware cache.
(All current single-expiry reads switch to `_prem(active_expiry, …)`.)

### 3. Subscribe next-week short strikes
The IC must ensure the candidate short/hedge strikes for the **next** expiry are
streamed. Approach (feed-side, no REST):
- On each `_try_entry`, compute the candidate strikes (short CE/PE + hedges) for
  the next expiry and request their subscription via the existing feeder
  subscription path (resolve broker keys through
  `registry.get_broker_symbol(underlying, expiry, strike, type, provider)` and call
  `feeder.subscribe_tokens([...])`, deduped so we only subscribe once).
- These are few keys (4 per expiry) → negligible feed load.
- Guard: only subscribe next-expiry keys when the current-week min_ltp check fails
  (lazy — avoid streaming forward strikes we never use).

### 4. Entry loop over expiries (`_try_entry`)
Replace the single-expiry premium block with:
```
for expiry in _candidate_expiries():          # current, then next (max 2)
    atm = round(spot/step)*step
    short_ce, short_pe = atm+short_otm, atm-short_otm
    ce_ltp = _prem(expiry, short_ce, "CE"); pe_ltp = _prem(expiry, short_pe, "PE")
    if ce_ltp <= 0 or pe_ltp <= 0:            # not streamed yet → ensure subscribe, continue
        _ensure_subscribed(expiry, [short_ce, short_pe, hedges]); continue
    if min_ltp > 0 and (ce_ltp < min_ltp or pe_ltp < min_ltp):
        log "shift expiry"; continue          # too cheap → try next expiry
    chosen_expiry = expiry; break
else:
    log WAIT (no expiry met min_ltp); return
# … proceed to build legs / net_credit / route order on chosen_expiry
```
The chosen expiry flows into the order event and `IronCondorPosition` (so exits /
adjustments use the same expiry).

### 5. Position + order carry expiry
`ICOrderEvent` and `IronCondorPosition` must carry the chosen `expiry` (today the
bridge resolves expiry via `next_expiry` — change to use the event's expiry) so
the four legs are placed on the **selected** expiry, and exits price the same one.

## Data flow
```
_try_entry → for expiry in [current, next]:
    premiums from expiry-aware cache
    if both shorts >= min_ltp → choose expiry, enter
    else ensure next-expiry strikes subscribed, try next
order/position tagged with chosen expiry → bridge places 4 legs on that expiry
```

## Testing
- **Expiry-aware cache** (unit): two ticks same strike, different expiry → distinct
  `_prem` values.
- **Expiry-shift decision** (pure helper `choose_expiry(premiums_by_expiry, min_ltp)`
  → returns the first expiry whose both shorts ≥ min_ltp, else None): table tests
  (current passes; current fails→next passes; both fail→None).
- **Subscribe-once** (unit): `_ensure_subscribed` dedupes repeat calls.
- Regression: with current-week premiums ≥ min_ltp, behaviour unchanged (picks current).

## Risks / notes
- **Feed must actually carry the next-expiry keys** — if the broker/symbol mapping
  for a forward expiry is wrong, those LTPs stay 0 and the IC correctly waits
  (logged). Symbol resolution reuses the proven `get_broker_symbol`.
- **Adjustments/rolls** must use the position's stored expiry, not `next_expiry`.
- Build on a branch; merge after a no-funds dry run shows the IC choosing the
  correct expiry and a reject reason of margin/freeze (not invalid symbol).
- Out of scope: >2 expiries, calendar spreads, per-leg different expiries.
