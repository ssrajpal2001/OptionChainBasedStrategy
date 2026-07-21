# V4 Cascade — Execution-Native Risk + Adaptive Tracking Strike (2026-07-21)

## Context

Live paper-trading CRUDEOIL and NIFTY through today's V4 Cascade session surfaced a
structural problem, not a bug: the tracking contract (deliberately chosen far ITM/OTM
from ATM — ∓400 for CRUDEOIL, ∓200 for NIFTY — so its own candles show cleaner
liquidity-sweep structure) can be genuinely illiquid, especially deep into the evening
CRUDEOIL session. A real trade tonight showed the tracking contract (PE 8400) moving
only 1.1 points over 40 minutes while the execution contract actually traded (PE 8000)
moved 17.5 points against the position — the trailing-stop *decision* was correctly
triggered on the tracking chart, but by the time it fired, the real, held instrument had
already moved far more than the tracking chart showed. Two root causes, addressed
separately below:

1. **SL/target/trailing-stop are computed on the tracking contract and mathematically
   scaled onto the execution contract** (`entries.compute_risk_mapping`,
   `exits.map_trailing_stop_to_execution`). If the tracking contract's price lags due to
   illiquidity, every scaled number derived from it is wrong, regardless of how correct
   the scaling math itself is.
2. **The tracking contract is fixed once at session-open and never revisited.** As the
   underlying drifts far from where the day started (CRUDEOIL moved from ~7961 to over
   8100+ tonight), the original offset increasingly overshoots into illiquid territory.

Both were discussed at length directly with the user this session; this spec captures
the four agreed changes. **Entry discovery (Gate 1 Index, Gate 2 Demand Block, Gate 3
limit pierce) is explicitly OUT OF SCOPE and unchanged** — it continues to run
exclusively on the tracking contract, which is correctly chosen for structural clarity
of the liquidity-sweep pattern. Only what happens *after* entry fires, plus the
tracking-strike *selection* itself, are in scope.

## User-confirmed decisions (do not re-litigate)

1. **Entry trigger stays on the tracking/scanning strike** — Gate 1/2/3 mechanics,
   discovery, and the pierce that fires an entry are completely unchanged.
2. **SL and target, once a trade is open, are derived natively from the EXECUTION
   strike's own recent price history** (a one-time lookback at entry, not a continuous
   parallel scanner) — not scaled/mapped from the tracking contract's zone. Rationale
   (user's, confirmed sound): the execution strike trades near ATM and is meaningfully
   more liquid than the far-ITM/OTM tracking strike, so it is *more* likely — not
   less — to show a clean, timely analogous sweep+reclaim pattern; the tracking
   strike's illiquidity is what produces stale/laggy candles, not a property of
   distance-from-ATM patterns in general.
3. **Execution strike moves from 1-OTM to 1-ITM.** Rationale (user's): an OTM option's
   premium is dominated by theta decay and lower delta — a SL/target computed from its
   raw premium can trigger falsely from time decay alone, unrelated to real price
   movement. An ITM strike's premium is delta-dominated (more intrinsic value), making
   SL/target derived from it a cleaner reflection of genuine price action.
4. **A fallback to today's scaled-mapping approach is required** for the (expected to be
   rare, per point 2's liquidity argument, but not impossible) case where no valid zone
   is found on the execution strike's own recent bars — SL/target must never be left
   undefined.
5. **The tracking/scanning strike re-centers, but ONLY while flat** (no open position).
   Never re-centers mid-trade — carrying an open position's SL/target/zone state across
   a strike change has no valid conversion (two different instruments, unrelated price
   scales) and was explicitly rejected as unsafe.
6. **Re-center thresholds** (user-specified): NIFTY re-centers when the underlying has
   moved **100 points** from the ATM last used to derive the current tracking strikes;
   CRUDEOIL re-centers at **200 points**. Other underlyings not explicitly tuned here
   default to their existing `TRACKING_OFFSET_PTS`-equivalent value (no behavior change
   for BANKNIFTY/SENSEX/GOLDM/crypto until explicitly tuned).
7. **Structural flip (opposite side reaching its own Gate 3 closes the current
   position) is unrelated to this work and stays exactly as-is.**

## Design

### 1. Execution-native SL/target at entry (`strategies/v4_cascade/`)

**New function** `entries.compute_execution_native_risk(execution_bars_5m, side,
exec_entry_price, sl_buffer, is_short=False) -> Optional[Tuple[float, float]]`:
runs `find_all_bear_traps_2candle` (bear geometry, matching Gate 2's own convention —
NIFTY/CRUDEOIL only ever scan bear-trap patterns on both CE and PE) against the
EXECUTION strike's own 5m bars (15m fallback if 5m finds nothing, mirroring Gate 2's
existing fallback), picks the most-recently-locked zone whose `entry_line` is on the
correct side of `exec_entry_price` (below it for a long, matching how Gate 3's pierce
itself works), and returns `(sl_price, target_price)` computed the same way
`compute_risk_mapping` already does — `zone_low − sl_buffer` for SL, `zone.sl_level`
floored at the risk distance for target (both formulas unchanged from today, just fed
execution-native inputs instead of tracking-native + scale) — **or returns `None`** if no
valid zone is found on either timeframe, signaling the caller to fall back.

**Data fetch**: `book.py`'s `_open_entry_async` (already the async, pre-publish step
that resolves the execution symbol and waits for its live tick) gains a REST historical
fetch for the execution symbol's own recent 5m bars — reusing
`data_layer.historical_candles.fetch_upstox_warm_1m`-equivalent machinery already used
elsewhere in this codebase for warming indicators (`sell_straddle._seed_exec_legs` is
the existing precedent), resampled to 5m the same way `_ingest_history` already resamples
tracking bars. Lookback window: same span `_ingest_history` already fetches for the
tracking contract (today, session-to-date) — reuses the existing fetch range, not a new
constant.

**Sequencing**: this fetch happens BEFORE the order is published, in the same
`_open_entry_async` step that already waits up to 3s for the execution contract's first
live tick — the two run concurrently (`asyncio.gather`), not serially, so this does not
meaningfully add to entry latency beyond what already exists. Once both resolve,
`compute_execution_native_risk` is called; on success, its `(sl_price, target_price)`
**overwrite** `t1.sl_price`/`t1.target_price`/`t2.sl_price` (originally set by the pure
engine using tracking-scale values) before the order is published. On `None` (no valid
execution-native zone, or the historical fetch failed/timed out), the original
tracking-scale-computed values from `_open_position` are left untouched — this is the
fallback, logged clearly (`WARNING`, same convention as the existing
"falling back to tracking-contract price_hint" logs) so it's auditable which trades
used which path.

### 2. T2 trailing stop moves to execution-native bars

Consequence of decision 2: T2's trailing-stop mechanism (`TrailingBaseTracker`) should
also trail using NEW bases forming on the EXECUTION strike's own live price action, not
the tracking contract's — otherwise T2's *entry* SL is execution-native but its
*ongoing* trail would still be tracking-native, reintroducing the same scale-mapping
problem this whole spec exists to remove.

**New bar-builder**: `book.py` currently builds tracking-contract 5m bars from ticks
matching `_ce_symbol`/`_pe_symbol` (`_on_option_tick`/`_close_5m_bucket`); execution-
contract ticks currently only update the scalar `_exec_live_price[side]`. This gains a
parallel, execution-contract 5m bar-builder (same bucketing logic as
`_close_5m_bucket`, keyed off `_exec_symbol[side]` instead) that feeds
`TrailingBaseTracker.on_5m_bar`/`check_hit` directly with execution-scale bars once a
position is open. `map_trailing_stop_to_execution`'s scale-conversion call is removed
entirely for a position using this path — `t2.trail_stop_price` becomes a direct
assignment of `trail.current_stop` (already execution-scale, no ratio to apply).
`move_to_breakeven`'s buffer and the "ratchet, never regress" logic are unchanged, just
operating on execution-scale numbers throughout instead of tracking-scale.

**Fallback interaction**: if `compute_execution_native_risk` fell back (case 1's
`None` path), T2's tracker stays on the tracking contract exactly as it works today
(seeded from the tracking-scale SL, scaled via `map_trailing_stop_to_execution` as
now) — the two mechanisms (SL/target source, and which bars T2 trails on) are coupled:
whichever price scale SL/target ended up computed on is the same scale T2 trails on.

### 3. Execution strike: 1-OTM → 1-ITM

`book.py`'s `_resolve_execution_strike(side)` currently resolves ATM ± one strike step
in the OTM direction (CE: ATM+step, PE: ATM−step). Flips to the ITM direction (CE:
ATM−step, PE: ATM+step) — a one-line sign change, no new config needed
(`EXECUTION_OFFSET_PTS`/strike-step values are unchanged, only which side of ATM they're
applied to). Verify this doesn't collide with `REGISTRY.get_upstox_key` assumptions
elsewhere (none currently branch on ITM-vs-OTM, only on the numeric strike itself).

### 4. Tracking/scanner strike re-centering (flat-only)

`V4CascadeConfig` gains `tracking_recenter_pts: float` (100.0 default for NIFTY, 200.0
for CRUDEOIL, matching `TRACKING_OFFSET_PTS`'s existing per-underlying wiring pattern in
`book.py`'s `__init__`).

`book.py` gains `self._tracking_reference_atm: Optional[float]` (the ATM value the
CURRENT tracking strikes were derived from — set whenever tracking strikes are
(re)computed, including the existing session-open computation). A new check, run
alongside the existing daily-boundary check (`_check_daily_boundary`, called from every
live bar close): if `self._engine.position is None or not self._engine.position.is_open`
AND `abs(current_atm - self._tracking_reference_atm) >= self._v4cfg.tracking_recenter_pts`,
re-derive `_ce_strike`/`_pe_strike` from the CURRENT atm (same offset math already used
at session-open), update `self._tracking_reference_atm` to the new anchor, and
unsubscribe the old tracking symbols / subscribe the new ones (mirrors the existing
strike-resolution/subscribe code path at boot).

**Re-center must re-warm the scanners from real history, not just reset them empty**
(explicit user requirement) — a bare `scanner.reset()` leaves the new strikes' scanners
with zero context until enough live bars happen to accumulate a fresh pattern from
scratch, which could take hours. Instead, re-centering re-runs the SAME fetch-plus-
replay sequence `_ingest_history` already performs at boot: fetch the new CE/PE tracking
symbols' historical + today's intraday 5m bars via REST (same dated-range-fetch +
intraday-fetch-and-merge pattern `_ingest_history`/`_merge_rows` already implement), then
replay them through fresh `IndexGatedPremiumScanner` instances for both sides (`bear`
geometry, `session_open` unchanged) via the same `_replay_through_engine` helper —
**but only the two per-side scanners are replaced/rebuilt this way, not the whole
engine**: `self.position` (must be `None` at this point, already guaranteed by the gate
above), `self._spot_confirm` (Gate 1, Index-based, untouched — never depended on the
tracking option strike), and `self._trackers`/`_tracking_entry_price` (empty when flat)
are all left alone. This makes a re-center behave like a mid-day mini-boot scoped to
just the tracking-strike scanners, reusing proven machinery rather than inventing a new
warm-up path.

**No re-center while a position is open, ever** — the check above is itself gated on
`position is None or not is_open`, so this is enforced at the single check site, not
scattered across call sites.

## Testing

- `test_v4_cascade_execution_native_risk.py`: `compute_execution_native_risk` finds a
  valid zone on 5m execution bars and returns correctly-signed SL/target; falls back to
  15m when 5m finds nothing; returns `None` (triggering the caller's fallback) when
  neither timeframe finds a zone; long and short (crypto PE) geometry both covered.
- `test_v4_cascade_execution_native_entry_fallback.py`: `_open_entry_async` uses the
  execution-native SL/target when the lookback succeeds; falls back to the original
  tracking-scale `t1.sl_price`/`target_price` (already set by `_open_position`) when it
  returns `None`, logging the fallback clearly; the historical fetch and the tick-wait
  run concurrently, not serially (assert on elapsed time or call ordering, not just the
  end result).
- `test_v4_cascade_t2_execution_native_trail.py`: T2's tracker ratchets and checks hits
  against EXECUTION-scale bars end-to-end when the entry used the execution-native path;
  `map_trailing_stop_to_execution` is NOT invoked in this path (assert via mock/spy);
  the fallback path (tracking-native SL) still uses the existing tracking-bar trailing
  mechanism unchanged — a regression guard that this spec doesn't silently break today's
  fallback behavior.
- `test_v4_cascade_execution_strike_itm.py`: `_resolve_execution_strike` returns the
  ITM-side strike for both CE and PE, both NIFTY and CRUDEOIL strike steps.
- `test_v4_cascade_tracking_recenter.py`: re-centers when flat and drift exceeds
  threshold; does NOT re-center when a position is open regardless of drift; does NOT
  re-center when drift is under threshold; updates `_tracking_reference_atm` to the new
  anchor (not the old one) after re-centering, so a subsequent re-center measures drift
  from the NEW anchor, not the original session-open one; re-center fetches + replays
  historical/intraday bars for the NEW strikes (assert the new scanners end up
  reflecting a genuine locked setup from the fetched history, not an empty
  `scanner.setups == []` reset) — the regression this specifically guards against is a
  re-center that resets scanners without re-warming them, leaving the new strikes cold
  for hours; `self.position`/`self._spot_confirm` are untouched by a re-center (assert
  identity unchanged, not just value).
- Full existing v4_cascade suite (83 tests as of this session) must stay green —
  particularly the entry-cutoff, replay-guard, and restored-tracker-state tests, since
  this spec's changes touch the same `_open_position`/`_check_exits`/`_boot` call paths.

## Open questions for plan-writing (not blocking this spec, but must be resolved before
tasks are cut)

- Exact REST fetch function/module to reuse for the execution strike's historical 5m
  bars (candidate: `data_layer.historical_candles.fetch_upstox_warm_1m` resampled to 5m,
  matching `_seed_exec_legs`'s existing pattern) — needs an Explore pass over
  `data_layer/historical_candles.py`'s actual signature before the plan can cite exact
  function names/parameters.
- Whether `_restore_tracker_state_for_open_position` (today's restart-recovery fix) also
  needs to know which price-scale (tracking vs. execution-native) a restored position's
  T2 tracker was using, so a restart correctly rebuilds the tracker on the SAME scale the
  live trade was actually using — likely a new persisted field on `CascadePosition`
  (e.g., `risk_basis: Literal["tracking", "execution_native"]`) alongside the existing
  `tracking_current_stop`/`tracking_entry_price` fields from today's persistence fix.
