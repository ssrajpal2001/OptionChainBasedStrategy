# V4 Cascade HTF-Gated LTF Cascade — Live Deployment Design

## Context

Over 2026-07-22, extensive chart-by-chart verification against real NIFTY
data (in `backtest/v4_cascade/htf_ltf_backtest.py`) validated a new entry
model that replaces the funnel currently live in `book.py`/`engine.py`/
`zone_state.py`/`spot_confirm.py`. That old funnel (Gate 1 Index 75m arms a
side immediately, Gate 2 scans a 2-candle Demand Block continuously and
unconditionally, Gate 3 can lock+pierce on the very same candle) was
confirmed live on 2026-07-23 09:20 to open a trade fully formed within one
5-minute candle at market open — a real, observed weakness: same-candle
collapse in Gate 2/Gate 3, and no "wait for a genuine re-entry" concept at
all. The user wants this replaced end-to-end with the validated model,
deployed live in paper mode today.

## What's already validated (backtest, do not re-litigate)

- **3-candle rule**: ref, sweep, and reclaim must be three separate candles
  (already fixed at the source in `rolling_base.py`'s `find_bear_zone`/
  `find_bull_zone` — this fix is already live).
- **HTF (75m) zone pool**: multiple candidate zones tracked concurrently per
  side (`find_all_bear_zones`/`find_all_bull_zones`, backtest-local in
  `htf_ltf_backtest.py`), each independently waiting for its own re-entry.
  A zone ages out of the pool after `HTF_ZONE_MAX_AGE_DAYS` (10) — matches
  real option-liquidity constraints (prev week + current week).
- **Re-entry gate**: a zone only starts LTF/5m tracking once a later 75m
  candle genuinely re-enters `[zone_low, zone_high]`.
- **LTF (15m) nested zone**: same 3-candle finder, fed 15m bars from that
  zone's own re-entry point onward. T1's target = its `sl_level`.
- **5m trigger**: a candle closes above (long) / below (short) the
  immediately preceding 5m candle. Persistent `pending_entry` — armed once,
  stays armed across bars until genuinely filled or the zone is
  invalidated (fixes the old system's phantom/same-candle fills).
- **Entry/SL**: `zone_low ± offset` (flat, grid-search parameter, offset=5
  was the strongest performer in the 90-day NIFTY-spot backtest).
- **T2 target** = the HTF zone's own `sl_level` (original ref candle's
  opposite extreme), with the existing breakeven-then-trail ratchet once
  T1 hits (`TrailingBaseTracker`, unmodified, reused as-is).
- **Pool clears entirely** the instant any zone in it fills — one position
  per side at a time, matching the engine's existing constraint.

## What changes for live deployment (new decisions, this spec)

1. **Scan the tracking contract's own premium, not raw NIFTY spot.** The
   90-day backtest used raw spot as a phase-1 stand-in (option history for
   arbitrary past dates isn't resolvable). Live, real per-strike premium
   history IS available (production already fetches it for the tracking
   CE/PE contracts via `_ingest_history`'s `fetch_upstox_range_1m`/
   `fetch_upstox_intraday_1m`, same as today). NIFTY spot is used ONLY to
   resolve which tracking strikes to scan (ATM∓200, 100-pt rounding,
   `_TRACKING_STRIKE_STEP` — unchanged from today's `_resolve_symbols`).
   HTF/LTF/5m all run on each side's OWN premium bars, independently.
2. **Trade fills on the tracking contract directly** (user-confirmed) — no
   separate ITM execution contract, no tracking-to-execution scale mapping.
   The strike being scanned is the strike being traded. This removes the
   entire execution-strike-resolution/scale-mapping subsystem for this new
   engine path (dead weight now, not touched/removed from the old path).
3. **5m trigger is intraday-only** (user-confirmed): the "previous 5m
   candle" comparison must never cross a day boundary — the first 5m
   candle of a new session has no "previous candle" to compare against
   (not yesterday's last candle). Only the HTF zone pool itself persists
   across days; the 5m/15m tracking state within an already-re-entered
   zone does NOT reset daily (only the trigger's own day-boundary check
   does) -- open question resolved below.
4. **New engine module**, not a patch to the old `V4CascadeEngine`/
   `IndexGatedPremiumScanner`/`SpotConfirmTracker`: those stay completely
   untouched (still used by CRUDEOIL and by anything not yet migrated).
   Adapts `htf_ltf_backtest.py`'s `_ZoneSlot`/`_SideState`/
   `find_all_bear_zones`/`find_all_bull_zones` into a live, incrementally-fed
   class (`on_75m_bar`/`on_15m_bar`/`on_5m_bar` methods, mirroring
   `SpotConfirmTracker`'s existing shape) instead of the backtest's
   whole-array replay loop.
5. **`V4CascadeBook` integration**: reuse ALL existing plumbing unchanged --
   persistence (`position_store`), EOD force square-off, the admin/dashboard
   API, `StraddleBookManager`-style multi-tenant spawning, broker order
   routing via `cascade_bridge`. Only the internal decision engine (what
   currently drives Gate1/Gate2/Gate3 via `V4CascadeEngine`) gets swapped
   for the new pool engine, for NIFTY only (CRUDEOIL untouched, matching
   today's config-gated behavior elsewhere in this codebase).
6. **A new opt-in flag** selects which engine a deployment uses (e.g.
   `V4CascadeConfig.use_pool_engine: bool = False`, default False = today's
   exact unchanged behavior). The user's own NIFTY paper deployment gets
   this flag turned on; nothing else changes behavior. This makes the
   rollout reversible with a single config flip if something's wrong,
   without touching the old, still-proven code path at all.

## Open question resolved here

**Does 15m/5m tracking reset daily once a zone is already re-entered?** No
— only the 5m TRIGGER's day-boundary check resets (a candle at session-open
never compares against yesterday's close). If a zone is mid-tracking
(re-entered, LTF zone found, waiting for the 5m trigger) when the session
ends, it resumes exactly where it left off the next morning — this matches
the HTF pool's own "carries across days" philosophy (user-confirmed
earlier: "CARRY DON'T ASK THIS QUESTION... IT IS WORKING PERFECTLY").

## Data flow

```
NIFTY spot (live ticks) -> ATM -> tracking CE/PE strikes (unchanged
  _resolve_symbols logic)
tracking CE premium ticks -> 5m/15m/75m bar builders (NEW, per-side,
  mirrors book.py's existing bucket-builder pattern) -> pool engine
tracking PE premium ticks -> same, independently
pool engine (CE side) -> CascadeEvent (OPEN_LONG_CE / CLOSE_LONG_CE) ->
  _emit_order -> cascade_bridge -> broker (paper) -> _on_fill reconciliation
  (all UNCHANGED from today)
```

## Testing

Unit tests for the new live-incremental pool engine class (mirroring the
existing `test_v4_cascade_index_gated_scanner.py` style: bear/bull
discovery, pool concurrency, aging, re-entry, intraday-only trigger reset,
persistent pending-entry). Integration test wiring it into `V4CascadeBook`
behind the new config flag, confirming the OLD path (flag off) is
byte-for-byte unchanged (full existing 137-test suite must stay green).
Manual trace against today's actual EC2 log (the 09:20 PE trade) as the
end-to-end sanity check: replay the same real historical data through the
new engine and confirm it does NOT fire at 09:20 the same way.

## Non-goals

CRUDEOIL is untouched. The old `V4CascadeEngine`/`IndexGatedPremiumScanner`/
`SpotConfirmTracker` path is untouched and remains the default (flag off).
No changes to persistence file format, dashboard API shape, or broker
routing -- the new engine only changes WHEN/WHY a CascadeEvent gets
produced, not what happens after.
