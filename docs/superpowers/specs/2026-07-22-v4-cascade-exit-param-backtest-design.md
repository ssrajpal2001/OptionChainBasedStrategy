# V4 Cascade Exit-Parameter Backtest + Optimizer — Design

## Context

V4 Cascade (NIFTY, paper mode) needs a go/no-go decision before live deployment.
Today's live paper session (2026-07-22) surfaced a real question the team can't
answer from 3 trades alone: is the SL buffer (flat 10 pts), the T1 target floor
(fixed 1R), and T2's trailing-stop config (4 bases / 5m) actually good values,
and how often does the `risk_basis=="tracking"` fallback (no execution-native
zone found at entry) underperform the `"execution_native"` path? A prior
backtest script (`scripts/v4_backtest_july2026.py`) tested a completely
different, superseded risk model (pre index/premium-gate-decoupling, pre
execution-native-risk) and cannot answer this. A separate, unrelated
backtest+optimizer (`backtest/nifty_liquidity_sweep/`) exists for a different
strategy entirely (PDH/PDL sweep on futures) but its `optimizer.py`/
`reporting.py` output shape (report.md + trades.csv + best_params.json, ranked
grid) is a useful structural precedent.

## Goal

Build a reusable backtest + grid-search optimizer, scoped to NIFTY, that:
1. Reuses REAL production entry-signal code unchanged (no drift risk between
   backtest and live behavior).
2. Grid-searches exit parameters only (SL buffer, T1 target floor multiple,
   T2 TSL lookback/timeframe) since entry timing/price never depends on them.
3. Reports execution-native vs tracking-fallback performance separately.
4. Produces a ranked report the user can read to decide go-live parameters.

## Architecture — two passes

**Why two passes:** Gate 1 (`SpotConfirmTracker`, 75m Index sweep+reclaim),
Gate 2 (`IndexGatedPremiumScanner`, 5m Demand Block), and Gate 3 (limit-pierce
trigger, `entries.check_limit_pierce`) never consult SL buffer, target floor,
or TSL settings — those only affect what happens once qty is already
notionally filled. So entry discovery runs once; exit simulation re-runs
cheaply per grid cell against already-discovered entries and already-fetched
bars.

### Pass 1 — Entry discovery (`entry_replay.py`)

Replays ~65 trading days of real NIFTY 1-minute history (index/spot for Gate 1,
tracking CE/PE for Gate 2) through the **actual, unmodified** production
classes: `spot_confirm.SpotConfirmTracker`, `zone_state.IndexGatedPremiumScanner`,
`engine.V4CascadeEngine.update()` — the same call pattern book.py's
`_ingest_history`/`_replay_through_engine` already uses at boot. Tracking
strikes are re-derived per day from that day's real 09:15 open (mirroring
`_resolve_symbols`'s `_TRACKING_STRIKE_STEP`-rounding formula) and re-centered
mid-day using the real `tracking_recenter_pts` drift rule (mirroring
`_maybe_recenter_tracking_strikes`, flat-only equivalent — trivially true here
since this pass never opens a live position). EOD Gate 2/3 setup-reset fires
daily, matching `_apply_eod_gate23_rules`.

Output: a list of discovered entries, each `(timestamp, side, zone,
entry_price, execution_strike_at_that_moment)`, persisted to
`backtest/v4_cascade/data_cache/entries.json` so Pass 2 never needs to re-run
Pass 1 unless the date range changes.

### Pass 2 — Exit simulation (`exit_simulator.py`, `optimizer.py`)

For each grid combination × each discovered entry:
1. Recompute `(sl_price, target_price)` via the real, unmodified
   `entries.compute_risk_mapping(zone, tracking_entry_price=entry_price,
   exec_entry_price=entry_price, sl_buffer=<grid value>, ...)`, with the
   target-floor multiple applied as a thin wrapper around the same function
   (floor = `<grid multiple> * tracking_risk` instead of the hardcoded `1 *
   tracking_risk`).
2. Also run the real, unmodified `execution_risk.compute_execution_native_risk`
   against that entry's execution-strike bars (fetched once per entry, cached,
   reused across every grid cell) to determine whether `risk_basis` would
   have been `"execution_native"` or the tracking fallback — recorded per
   trade, not grid-varied.
3. Walk forward through the entry's already-fetched execution-strike 5m bars
   using the real, unmodified `exits.check_t1` and `exits.TrailingBaseTracker`
   (constructed with `<grid value>` lookback_bases/tf_minutes) to produce a
   simulated close: time, price, reason, P&L.

`optimizer.py` aggregates every grid cell's simulated trades into profit
factor / max drawdown / win rate / net P&L, plus the same metrics split by
`risk_basis`, and ranks cells by profit factor (max-drawdown tie-break).

### Data layer (`data_fetch.py`)

Reuses `data_layer.historical_candles.fetch_upstox_range_1m`/
`fetch_upstox_intraday_1m` (the same functions book.py already calls) for:
index/spot bars, each day's tracking CE/PE bars (strike resolved per-day),
and each discovered entry's execution-strike bars (strike resolved per-entry,
21-day lookback ending at that entry's date, matching production's
`_LOOKBACK_DAYS`). Raw rows cached to `backtest/v4_cascade/data_cache/` as
JSON keyed by `(instrument_key, date)`; a re-run only fetches missing keys.
Token is read from `UPSTOX_TOKEN` env var at run time — never written to any
cached file, never logged, never committed.

### Reporting (`reporting.py`)

`backtest/v4_cascade/results/report.md` (ranked grid table, best-set
interpretation, execution-native vs tracking-fallback breakdown, deep-dive on
losing trades — same four-section shape as `nifty_liquidity_sweep`'s report),
`trades.csv` (every simulated trade under the best parameter set),
`best_params.json`.

## Grid (defaults; overridable via `main.py` args)

- `sl_buffer`: 5.0 / 10.0 / 15.0 / 20.0 (points)
- target floor multiple: 1.0 / 1.5 / 2.0 / 2.5 (× SL distance)
- TSL `lookback_bases`: 2 / 4 / 6
- TSL `tf_minutes`: 5 / 15

4×4×3×2 = 96 combinations. Pass 1 (expensive) runs once; Pass 2 (cheap) runs
96 times against already-fetched, already-cached bars.

## Testing

Unit tests for `data_fetch.py`'s cache-hit/miss logic (mocked HTTP), for the
target-floor-multiple wrapper around `compute_risk_mapping` (verify it
collapses to identical output as production when multiple=1.0), and for
`optimizer.py`'s ranking/aggregation math (profit factor, drawdown, tie-break)
against synthetic trade lists. Entry discovery and exit simulation are NOT
independently unit-tested beyond that — they call real, already-tested
production functions; the integration itself is validated by the report's own
output being sane (spot-checked against a few known-live trades from today's
session, e.g. the 10:05 CE 24000 execution-native entry).

## Non-goals

CRUDEOIL is out of scope (different session timing/offsets; NIFTY-only per
user decision). This does not change any production code — `book.py`,
`engine.py`, `entries.py`, `exits.py`, `execution_risk.py` are read-only
imports, never modified by this work.
