# V4 Cascade — CRUDEOIL Support (2026-07-20)

## Context

The V4 Premium Trap Cascade Engine (`strategies/v4_cascade/`) is currently NIFTY-only
(plus a separately-added crypto BTC/ETH spot-only path, handled as an `_is_crypto`
branch inside the same `V4CascadeBook` class). With the NSE market closed for the day,
the user wants to extend it to also trade CRUDEOIL (MCX commodity), following the same
architectural precedent as the crypto path: reuse the existing Gate 1/2/3 scanning
machinery (`PremiumGateScanner`, `find_all_bear_traps_2candle`, invalidation, entries/
exits — all pure premium-chart pattern detection, fully underlying-agnostic) and only
parameterize the underlying-specific surrounding logic (session timing, ATM source,
strike/offset numbers, EOD timing).

Explored first (per project convention — reuse existing infrastructure rather than
rebuild): `config/global_config.py`'s `ExchangeConfig` already has `mcx_market_open =
9:00`, `mcx_market_close = 23:30`, `is_mcx(underlying)`, and CRUDEOIL entries in both
`strike_steps` (100.0) and `lot_sizes` (100). `data_layer/instrument_registry.py`
already loads CRUDEOIL's option chain and near-month futures key from the MCX master
JSON, and `historical_instrument_key(underlying)` already routes MCX underlyings to
their futures key (the correct "ATM from futures, not spot" source). An Explore agent
confirmed the live tick path needs zero changes: the feeder layer already relabels
CRUDEOIL futures ticks as `IndexTick(symbol="CRUDEOIL", ltp=...)` before publishing to
`Topic.INDEX_TICK`, identical to how NIFTY spot ticks are published — so
`_await_first_tick()` (which subscribes that topic and filters by `tick.symbol`) works
unchanged for CRUDEOIL.

The gap is entirely in `strategies/v4_cascade/book.py` (four places hardcode NSE's
`09:15` session-open assumption) and `rolling_base.py` (`resample_bars`'s own hardcoded
`_SESSION_OPEN_HOUR/MINUTE`), plus a handful of NIFTY-only module constants
(`_STRIKE_STEP`, `_TRACKING_OFFSET`, `EXECUTION_OFFSET_PTS`, SL buffer, `_EOD_SQUARE_OFF`,
`_GATE23_RESET`) that need to become per-underlying instead of fixed values.

All parameters below were confirmed directly with the user (2026-07-20): lot size 100,
session start 09:00, squareoff 23:15, ATM from futures price, tracking/execution
offsets scaled by strike-step ratio, SL buffer scaled the same way.

## Confirmed Parameters

| Parameter | NIFTY (existing) | CRUDEOIL (new) |
|---|---|---|
| Session open | 09:15 | 09:00 |
| Squareoff time | deployment-configured (e.g. 15:15/15:20) | 23:15 |
| Gate 2/3 daily reset | squareoff + 15 min | squareoff + 15 min (23:30) |
| ATM source | spot index tick/REST | futures tick/REST (`historical_instrument_key`) |
| Strike step | 50 | 100 |
| Lot size | 65 | 100 |
| Tracking offset (wide contract) | ATM∓200 (4 strikes) | ATM∓400 (4 strikes) |
| Execution offset (real contract) | ATM±50 (1 strike) | ATM±100 (1 strike) |
| SL buffer | 10 flat points | 20 flat points |

Strike/offset/lot numbers scale by the strike-step ratio (100/50 = 2x) to preserve the
same *number of strikes* away from ATM as the validated NIFTY setup — not a re-derivation
from CRUDEOIL-specific volatility research, which is out of scope for this pass.

## Design

### 1. Session timing (clock-anchoring)

`V4CascadeBook.__init__` gains `self._session_open: Tuple[int, int]`, resolved once at
construction from `self._cfg.exchange.is_mcx(self._underlying)`: `(9, 0)` if MCX, else
`(9, 15)` (matches existing NSE/crypto behavior — crypto's own bucketing is 1-minute-
based via `_bucket_end_1m`, unaffected by this since it doesn't anchor to a 09:15/09:00
session open at all, it's 24/7).

This propagates as a new parameter (default `(9, 15)` for backward compatibility with
every existing non-MCX caller) to:
- `rolling_base.py::resample_bars(bars_5m, multiplier, session_open=(9, 15))` — replaces
  the hardcoded module-level `_SESSION_OPEN_HOUR`/`_SESSION_OPEN_MINUTE` constants with a
  parameter, used in the `open_dt = b.timestamp.replace(hour=..., minute=...)` line.
- `book.py`'s module-level `_bucket_start(ts, multiplier, session_open=(9, 15))`,
  `_bucket_end(ts, multiplier, session_open=(9, 15))`, and the new `_bucket_key(ts,
  multiplier, session_open=(9, 15))` (added in the prior 75m-replay-bug fix) — same
  parameterization.
- `_replay_through_engine(engine, spot_5m, ce_5m, pe_5m, on_daily_boundary=None,
  session_open=(9, 15))` — passes `session_open` through to every `resample_bars`/
  `_bucket_key` call inside it.
- The live tick bucket builder (`_on_option_tick`, currently `bucket = _bucket_start(ts,
  5)`) — passes `self._session_open`.

All call sites inside `V4CascadeBook` pass `self._session_open` explicitly; the bare
module-level default `(9, 15)` only matters for any other caller (tests, other scripts)
that doesn't pass it.

### 2. ATM source (futures vs. spot)

`_fetch_session_open` and `_ingest_history`'s `spot_key` resolution switch from
`REGISTRY.get_upstox_index_key(self._underlying)` to
`REGISTRY.historical_instrument_key(self._underlying)` — the latter already branches on
`is_mcx`-equivalent internally (`if u in self._futures_upstox: return
self._futures_upstox[u]`), so this one change correctly sources both NIFTY's spot index
key and CRUDEOIL's futures key from the same call, no branching needed in book.py itself.

Live tick path (`_await_first_tick`, `_spot_loop`): no changes — confirmed the feeder
already publishes CRUDEOIL futures ticks as `IndexTick(symbol="CRUDEOIL")` on
`Topic.INDEX_TICK`, and both methods already filter by `tick.symbol == self._underlying`,
which is underlying-name-based, not exchange-based.

### 3. Strike/offset/lot numbers

Replace the NIFTY-only module constants with a small per-underlying lookup (mirrors the
existing crypto pattern in the same file, e.g. `_CRYPTO_CONTRACT_VALUE`):

```python
_TRACKING_OFFSET_BY_UNDERLYING = {"NIFTY": 200.0, "CRUDEOIL": 400.0}
_EXECUTION_OFFSET_BY_UNDERLYING = {"NIFTY": 50.0, "CRUDEOIL": 100.0}
_SL_BUFFER_BY_UNDERLYING = {"NIFTY": 10.0, "CRUDEOIL": 20.0}
```

resolved in `__init__` with a fallback to the NIFTY values (preserves current behavior
for any underlying not in the table, though only NIFTY/CRUDEOIL are supported by the
manager). `EXECUTION_OFFSET_PTS` in `config.py` (currently a single shared constant
read into `V4CascadeConfig.execution_offset_pts`) needs the same per-underlying
treatment — `V4CascadeBook` resolves its own value from the table above rather than
reading the shared config constant, since the config dataclass is not itself
underlying-aware.

Tracking-strike ATM rounding (`_resolve_symbols`) currently uses a hardcoded
`_STRIKE_STEP = 100.0` module constant — coincidentally correct for CRUDEOIL but wrong
for NIFTY (whose real step is 50, already correctly sourced from
`ExchangeConfig.strike_steps` elsewhere in `_resolve_execution_strike`). Fixed to read
`self._cfg.exchange.strike_steps.get(self._underlying, 50.0)` in both places consistently,
removing the `_STRIKE_STEP` module constant entirely — a latent NIFTY bug (tracking ATM
rounding to the wrong step) fixed as a side effect of doing this properly for CRUDEOIL,
not a new NIFTY behavior change to itself round-trip test separately.

Lot size: already generic — `V4CascadeBookManager` reads `lot_multiplier` from the
deployment row and the book multiplies tranche `qty` by it; the *base* lot size comes
from `ExchangeConfig.lot_sizes[underlying]` via the same shared config path NIFTY
already uses, no CRUDEOIL-specific code needed here.

### 4. EOD / Gate 2-3 reset timing (bug fix, not just CRUDEOIL support)

Current state: `_EOD_SQUARE_OFF = (15, 15)` and `_GATE23_RESET = (15, 30)` are hardcoded
module-level constants in `book.py`. `_GATE23_RESET` is read in *both* the live path
(`_check_daily_boundary`) and inside `_replay_through_engine`; `_EOD_SQUARE_OFF` is read
only inside `_replay_through_engine`. Neither derives from the book's actual configured
`squareoff_time` (`self._eod_hour_min`, already correctly threaded through the
constructor and used for the *live* force-square-off check).

This is a real, already-known class of bug — project memory records `sell_straddle`
being hit by exactly this pattern once already ("MCX squareoff_time must be ~23:25;
15:15 default instantly EOD-exits MCX"). Left as-is, deploying CRUDEOIL would force-close
positions and wipe in-flight Gate 2/3 progress at 15:15/15:30 IST — the middle of the
MCX trading session — every single day.

Fix: compute `self._gate23_hour_min` in `__init__` as 15 minutes after
`self._eod_hour_min` (matches the existing NIFTY 15:15→15:30 convention exactly; for
CRUDEOIL's 23:15 squareoff this yields 23:30, which also happens to coincide with MCX's
actual session close). Replace every use of the module constants:
- `_check_daily_boundary`: `_GATE23_RESET` → `self._gate23_hour_min` (already uses
  `self._eod_hour_min` for the squareoff check — no change needed there).
- `_replay_through_engine`: add `eod_square_off` and `gate23_reset` parameters (both
  `Tuple[int, int]`), passed in from `_ingest_history` as `self._eod_hour_min` /
  `self._gate23_hour_min`, replacing the hardcoded module constants inside the function.

This changes NIFTY's replay-path EOD simulation too (from a hardcoded 15:15/15:30 to
whatever the deployment actually configured, e.g. the "SQ 15:20" seen live today) —
correct and consistent with what the live path already does, closing a gap that existed
before this session even for NIFTY.

### 5. Manager / registry wiring

`strategies/v4_cascade_book_manager.py`: add `"CRUDEOIL"` to `_SUPPORTED_UNDERLYINGS`
(currently `{"NIFTY", "BTC", "ETH"}`).

No admin UI changes expected — the deployment form's underlying/instrument selector is
already generic across strategies (sell_straddle already deploys to CRUDEOIL through the
same form), so this should only need the backend acceptance-list change. Verify during
implementation rather than assume.

### 6. Explicitly unchanged

- `PremiumGateScanner`, `find_all_bear_traps_2candle`/`find_bear_trap_2candle`,
  `_invalidate_broken_setups`, `entries.py`, `exits.py` — fully underlying-agnostic, no
  changes.
- Expiry resolution (`REGISTRY.get_active_expiry`) — already correctly per-underlying via
  the loaded MCX expiry list (monthly format), matching how it already resolves NIFTY's
  weekly expiries.
- Live tick ingestion, order routing, position persistence — no underlying-specific
  assumptions found.

## Testing / Verification

- Compile-check + full existing test suite (`python -m pytest tests/ -q`) after each
  change, per this session's established pattern — must stay green throughout, since
  several of these changes (default `session_open=(9,15)`, the `_STRIKE_STEP` fix, the
  EOD/gate23 fix) touch NIFTY's existing behavior and must not regress it.
- `scripts/diag_v4_ce_pe_traps.py` (built earlier this session) needs a `--underlying`
  flag added so it can run against CRUDEOIL's own tracking contracts — reuse for a
  sanity check that Gate 1 zone discovery produces sane results on real CRUDEOIL premium
  data before considering this ready for live/paper testing.
- Manual: deploy a CRUDEOIL v4_cascade binding after market reopens (CRUDEOIL's own MCX
  session starts independently of NSE hours, so this could in principle be tested before
  NIFTY's next session), confirm ATM locks from the futures price at 09:00, confirm no
  EOD force-close before 23:15, confirm Gate 1/2/3 zones populate in the UI same as NIFTY.

## Open Items (deferred, not blocking this pass)

- CRUDEOIL-specific SL buffer / offset tuning based on the instrument's own historical
  volatility (current numbers are a straight strike-step scaling from NIFTY, not
  independently derived) — candidate for the same after-hours backtest pass already
  queued for NIFTY (bias-gating, RSI/VWAP/ATR, mitigation-rule strictness).
- Whether other MCX underlyings already in `ExchangeConfig.mcx_underlyings`
  (CRUDEOILM, NATURALGAS, GOLD, GOLDM, SILVER) should get the same treatment — out of
  scope, CRUDEOIL only per the user's explicit request.
