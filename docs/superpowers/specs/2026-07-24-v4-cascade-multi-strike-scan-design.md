# V4 Cascade — Multi-Strike Candidate Scanning (NIFTY, pool engine)

## Context

`PoolCascadeEngine` (`strategies/v4_cascade/pool_engine.py`), shipped earlier this
session, replaced the old Gate1/2/3 Index-driven funnel for NIFTY with a
75m-HTF-zone-pool → 5m-break-of-structure model that scans each option's OWN
premium chart directly (both CE and PE look for the same bear-trap pattern —
this engine is always a long buyer, never short). It runs behind
`V4CascadeConfig.use_pool_engine`, itself gated behind the `V4CASCADE_USE_POOL_ENGINE`
env var (NIFTY-only, dev-phase, opt-in — see `strategies/v4_cascade_book_manager.py`).

Today `book.py` resolves exactly **one** CE tracking strike and **one** PE
tracking strike per book, fixed at `atm ∓ tracking_offset_pts` (default 200.0,
on the flat `_TRACKING_STRIKE_STEP = 100.0` grid — see
`strategies/v4_cascade/config.py`), and feeds only those two symbols' bars into
the engine. Live production data (2026-07-24) showed a real, chart-confirmed
asymmetry: CE's fixed tracking strike (23500, i.e. ATM-200 with ATM=23700)
found **zero** bear-trap zones over the tracked window, while PE's fixed
tracking strike (23900, ATM+200) found **8**. This isn't a bug — it's the
fixed single-offset choice happening to land CE in a structurally quiet region
of that particular strike's own premium chart on that particular day. The fix
is to track several candidate strikes per side instead of committing blindly
to one fixed offset.

This was explicitly deferred until the WS subscription budget was understood.
That investigation (same session) found and fixed a real bug in
`UpstoxFeeder.subscribe_tokens` (`data_layer/global_feeder.py`) that was
double-subscribing every option leg — real usage is 38 of ~50 symbols on the
shared Upstox connection (NIFTY StrikeRebalancer chain 18 + SENSEX chain 18 +
2 index), leaving **~12 symbols of real headroom**, shared with SENSEX.

## Decisions (confirmed with user during brainstorming)

1. **Track all 5 candidates per side live, persistently** — not a
   periodic REST scan that only subscribes a winner. Simpler mental model,
   accepted trade-off: costs up to 10 new WS symbols against the ~12-symbol
   headroom (see Cost section — this is tight and worth stating plainly, not
   hiding).
2. **Exactly one position total, engine-wide** — unchanged from today. Whichever
   candidate (any strike, either side) is first to pierce its limit opens the
   trade; every other candidate's pending trigger, on both sides, is simply
   left pending/ignored until that position closes. No new risk/quantity
   model, no multiple concurrent positions.
3. **Recenter the whole 5-strike window on ATM drift** — same drift-threshold
   mechanism as today's single-strike recenter
   (`V4CascadeBook._maybe_recenter_tracking_strikes`), just diffing a 5-strike
   set instead of a single strike.
4. **Approach for internal representation: widen `PoolCascadeEngine`'s pool key
   from `side` to `(side, strike)`, single shared engine instance.** Chosen
   over (a) 10 separate engine instances with book-level cross-instance
   coordination, and (b) 2 engines (one per side, each internally
   multi-strike) — both alternatives require re-implementing the "only one
   position ever" invariant that the single-shared-instance approach gets for
   free from `self.position`/`is_open()` already living at the whole-engine
   level.

## Architecture

### `strategies/v4_cascade/pool_engine.py`

- `_pool: Dict[str, List[_ZoneSlot]]` keyed today by `"CE"`/`"PE"` becomes keyed
  by a composite candidate key, e.g. `"CE_24000"`, `"CE_23900"`, `"PE_24200"`
  — 10 entries instead of 2 when multi-strike is active (naturally degrades to
  today's exact 2-entry behavior when only one offset is configured — see
  Config section). Every dict currently keyed by side alone
  (`_known_ref_ts`, `_all_75m`, `_last_5m_date`, `_trail`, `_last_5m_bar`)
  widens the same way.
- `on_75m_bar` / `on_15m_bar` / `on_5m_bar` gain a `strike: float` parameter
  alongside the existing `side: str`, used only to build/look up the
  composite pool key — no change to any of the zone-discovery, re-entry,
  trigger, or pierce logic itself, all of which already operates per-slot.
- `_ZoneSlot` gains a `strike: float` field, set from the constructor call
  site (`on_75m_bar`'s zone-discovery loop) so `_open_position` can read it
  without needing the composite key parsed back apart.
- `_open_position`: sets `CascadePosition.tracking_strike` and
  `.execution_strike` to the SAME real value (`slot.strike`) instead of the
  current `0.0` placeholders — the pool engine's "tracking IS execution, no
  split" model (already true today, just never populated). Also sets the
  existing `CascadeEvent.execution_strike` field (already defined on the
  dataclass, just never set by this engine today) so `book.py` no longer
  needs to infer the strike from `side` alone.
  - Note: `CascadePosition.tracking_strike`/`.execution_strike`'s existing
    field comments ("ATM∓200 tracking contract" / "ATM±50 execution
    contract") describe the OLD Gate1/2/3 execution-offset model. Update
    those comments alongside this change to avoid confusion — both fields
    hold the identical value under the pool engine.
- `is_open()` / `_close_for_structural_flip` / `_check_exits`: **no changes**
  — these already operate on `self.position` at the whole-engine level, so
  the single-position-ever invariant and the CE↔PE structural-flip logic
  keep working unchanged across 10 candidates exactly as they do across 2.
- New `reset_candidate(side, strike)` clears just that one candidate's zone
  pool, HTF bar history, and known-ref dedup set (the per-candidate
  granularity today's single-strike `reset_side(side)` doesn't need, since
  today there's only one candidate per side to reset). `reset_side(side)`
  stays, reimplemented as a thin loop calling `reset_candidate` for every
  strike currently tracked on that side — kept for any full-side wipe, but
  the recenter path (book.py) calls `reset_candidate` directly, only for the
  specific strikes that actually changed.

### `strategies/v4_cascade/config.py`

- New field `tracking_offsets_pts: List[float]`, defaulting to
  `[TRACKING_OFFSET_PTS]` (i.e. `[200.0]`) — preserves today's exact
  single-strike behavior when unset. `tracking_offset_pts` (singular) stays
  as-is for any code that still reads it directly; `tracking_offsets_pts`
  (plural) is the new source of truth book.py's symbol resolution reads from.
- `v4_cascade_book_manager.py` gains a new env var,
  `V4CASCADE_TRACKING_OFFSETS` (comma-separated points, e.g.
  `"100,200,300,400,500"`), parsed into `tracking_offsets_pts` alongside the
  existing `V4CASCADE_USE_POOL_ENGINE` parsing. Unset → single-element list
  `[200.0]`, i.e. today's exact behavior. This makes multi-strike purely
  additive and revertible by unsetting one env var, matching how
  `use_pool_engine` itself was staged. Offsets apply symmetrically: CE
  candidates at `atm - offset` for each configured offset, PE at
  `atm + offset`.

### `strategies/v4_cascade/book.py`

- `_resolve_symbols`: builds `self._ce_strikes: List[int]` /
  `self._pe_strikes: List[int]` (renamed+pluralized from today's
  `self._ce_strike`/`self._pe_strike` scalars) from `cfg.tracking_offsets_pts`,
  and `self._ce_symbols` / `self._pe_symbols` (Upstox keys) alongside them.
  Subscribes all resolved symbols at boot via `_subscribe_tracking_contracts`
  (same call, wider symbol list).
- `_ingest_history`: fetches history for every resolved symbol
  (`asyncio.gather` over all CE+PE symbols, same pattern as today's 2-call
  gather, just wider) and replays each into the engine via
  `on_75m_bar(side, strike, bar)` / etc. with the correct strike threaded
  through.
- Live tick routing (today: `int(tick.strike) == self._ce_strike`) becomes
  `int(tick.strike) in self._ce_strikes` (and same for PE), dispatching each
  matching tick's bar to `on_Nm_bar(side, tick.strike, bar)`.
- `_maybe_recenter_tracking_strikes`: recomputes the full 5-strike window from
  the new ATM, diffs old-set vs new-set per side (mirroring
  `StrikeRebalancer._rebalance`'s existing to_unsub/to_sub diff pattern),
  unsubscribes only strikes that fell out of range, subscribes only newly
  in-range ones, and calls `reset_side` only for candidates that actually
  changed (a strike that stays in-window across a recenter keeps its
  in-progress zone pool — no unnecessary re-warm).
- Execution stays "tracking IS execution, no split" — same invariant as
  today. `exec_strike = self._ce_strike if ev.side == "CE" else
  self._pe_strike` (today's inference) is replaced by reading
  `ev.execution_strike` directly off the event, since with 5 CE candidates
  there is no longer a single `self._ce_strike` to infer from.
- Persistence / restore-from-disk: no new logic needed —
  `CascadePosition.tracking_strike`/`.execution_strike` are already real
  persisted fields; they simply get correctly populated now instead of
  staying `0.0`.

### Dashboard (`ui_layer/dashboard_server.py` + `monitor.html`)

Reuses this session's existing `all_zones_debug` + nearest-to-price
(`_zone_dist`) selection logic unchanged in spirit: `_zone_dist` already
operates on zone bounds alone, not strike, so "nearest zone across the whole
side" naturally extends across all 5 strikes' pools without new selection
code. Each entry in `all_zones_debug` gains a `strike` field so the rendered
UI list (already built this session) can show which physical contract each
zone belongs to. No new per-strike card — the existing single CE
card + single PE card, now backed by a wider pool, stays the UI shape.

## Cost / trade-offs (stated plainly, not glossed over)

- Boot-time: up to 10 REST history-fetch calls (`asyncio.gather`, bounded,
  one-time) instead of today's 2.
- Steady-state WS: up to 10 new symbols (5 CE + 5 PE, some strikes may
  already overlap the existing SellStraddle/StrikeRebalancer NIFTY chain and
  not need a NEW subscription — `UpstoxFeeder.subscribe_tokens`'s dedup,
  fixed this session, already handles this correctly with no new code
  needed: subscribing an already-subscribed key is a safe no-op) against the
  ~12-symbol headroom confirmed live this session. This is tight — it leaves little to no margin for
  SENSEX or a third underlying to grow afterward without revisiting the
  budget again. Not a blocker given the decision to track-all-5, but a known
  constraint the implementation should surface (e.g. keep the existing "N
  symbols subscribed — EXCEEDS ~50" warning log intact as the early signal
  if this or a future change pushes past budget).

## Testing

- `pool_engine.py`: extend the existing zone/trigger/pierce test suite with
  multi-candidate cases — two candidates on the same side, one finds a zone
  and triggers, the other never does (mirrors the real CE-23500-vs-PE-23900
  asymmetry that motivated this); confirm only one opens even if two
  candidates on the SAME side both reach `pending_entry` (first pierce wins,
  the other's `pending_entry` state is simply abandoned, not double-fired).
- `book.py`: strike-set resolution from `tracking_offsets_pts` (single vs
  multi-offset), tick routing to the correct composite candidate on a
  matching strike, recenter diffing (only changed strikes get
  unsubscribed/resubscribed/reset).
- `v4_cascade_book_manager.py`: `V4CASCADE_TRACKING_OFFSETS` parsing
  (unset → `[200.0]`, set → parsed list), mirroring the existing
  `test_pool_engine_on_for_nifty_when_env_var_set`-style tests already in
  `tests/strategies/test_v4_cascade_book_manager.py`.
- Existing single-offset (today's default) behavior must remain a byte-for-byte
  regression case — every existing `test_v4_cascade_pool_engine*.py` test
  should keep passing unchanged with the default single-element offset list.

## Out of scope

- Any change to `execution_offset_pts` / the OLD Gate1/2/3 engine's separate
  tracking/execution split — that engine is untouched, this design only
  touches the pool-engine path (`use_pool_engine=True`).
- SENSEX / CRUDEOIL / crypto — `use_pool_engine` and this multi-strike
  extension both stay NIFTY-only (existing `_POOL_ENGINE_UNDERLYINGS = {"NIFTY"}`
  guard in `v4_cascade_book_manager.py` is unchanged).
- Resolving the WS budget tightness beyond what's already confirmed (38/50) —
  if the 10-symbol addition is later found to actually exceed budget once
  overlap with the existing chain is accounted for precisely, that's a
  follow-up, not something this design pre-solves speculatively.
