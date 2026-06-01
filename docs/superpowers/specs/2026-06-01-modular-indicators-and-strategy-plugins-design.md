# Design: Modular Indicators + Shared Per-Timeframe Indicator Engine + Plug-and-Play Strategy Framework

Date: 2026-06-01
Status: Approved (design phase)

## 1. Goal

Refactor the OptionChain trading system so that:

1. **Indicators are modular** — one file per indicator, each independently
   verifiable against the reference system `E:\Option_Selling\Option_Selling_May_2026`.
2. **Indicators are computed once** in a shared per-`(symbol, timeframe)` engine
   (single source of truth), not duplicated inside each strategy.
3. **VWAP comes from the broker feed (ATP)**, never self-computed.
4. **Rules evaluate on their own timeframe** (`tf=1/2/5`) with occurrence
   counting — fixing the two known correctness gaps.
5. **Strategies are plug-and-play** — each a self-contained plugin, enabled or
   disabled via config, applied on restart (no runtime hot-swap).

Non-goals: runtime hot-swap of strategies; changing the feed/EventBus/execution
bridges/Zerodha order path (these stay as-is); MCX support (separate effort).

## 2. Key decisions (from brainstorming)

- Plug-and-play = **config enable/disable + restart** (not live hot-swap).
- **Shared IndicatorEngine** (one computation, all strategies read it).
- **Full refactor now.**
- **VWAP = broker ATP per instrument**, read live from each tick's
  `avg_trade_price` (Fyers) / `atp` (Upstox full mode), snapshotted at minute
  close. NOT computed.
- **Combined VWAP is leg-dependent**: each strategy combines the ATP of *its own*
  legs (straddle = ATM CE ATP + ATM PE ATP; IC combines its 4 legs).

## 3. Architecture

```
Feed → EventBus (INDEX_TICK / OPTION_TICK / CANDLE_CLOSE)   [unchanged]
                       │
              IndicatorEngine
                 - per (symbol, timeframe) closed-candle series
                 - per-instrument ATP captured from OPTION_TICK
                 - computes RSI/ROC/ADX/EMA/ATR on closed candles
                 - caches closed-minute values (the "+5s reads the closed minute")
                       │  query API + RuleEvaluator (tf-aware)
        ┌──────────────┼──────────────┐
   IronCondorPlugin StraddlePlugin TrapPlugin   (loader starts only enabled)
        └──────────────┴──────────────┘
                       │
              ExecutionRouter / bridges / Zerodha   [unchanged]
```

### 3.1 Module layout

```
matrix_engine/
  indicators/
    __init__.py        re-exports (backward compat for the 7 current importers)
    constants.py       RSI_PERIOD=14, VWAP_WINDOW=500, ADX_PERIOD=20
    vwap.py            feed-ATP helpers (combine legs' ATP); NOT a math function
    slope.py           VWAP slope from ATP: v_curr vs v_prev (tf apart) + occurrences
    rsi.py             Wilder's RSI(14)            [computed]
    roc.py             100*(src-src[length])/src[length]  [computed, ported]
    adx.py  ema.py  atr.py  volume.py              [computed, kept]
    snapshot.py        TechSnapshot dataclass
  indicator_engine.py  shared engine
  rule_evaluator.py    tf-aware rule evaluation

strategies/
  plugin.py            StrategyPlugin ABC + @register_strategy + StrategyRegistry
  loader.py            reads config, instantiates enabled plugins
  iron_condor.py       -> plugin; reads indicators from engine
  sell_straddle.py     -> plugin
  trap_trading_engine.py -> plugin
```

## 4. Components

### 4.1 Indicators package
Each indicator is a pure function/helper in its own file, ported from the
reference where it exists:
- **vwap.py** — `leg_atp(tick)` returns the instrument ATP from the tick;
  `combined_vwap(atps: list[float]) -> float` sums the relevant legs' ATP.
  No price math — VWAP is exchange ATP.
- **slope.py** — `vwap_slope(vwaps: list[float], occurrences: int)` →
  `(is_rising, is_falling, v_curr, v_prev, cons_rising, cons_falling)`, matching
  reference `get_vwap_slope_status`. `vwaps` are closed-minute combined-VWAP
  values one tf-boundary apart.
- **rsi.py** — Wilder's RSI(14) (current impl already matches; moved here).
- **roc.py** — `100*(src - src[length])/src[length]` (ported from reference).
- **adx.py / ema.py / atr.py / volume.py** — moved from current indicators.py.
- **__init__.py** re-exports `rsi, vwap, adx, ema, atr, volume_spike, roc,
  vwap_slope, TechSnapshot` and the constants so existing imports keep working.

### 4.2 OptionTick ATP field
`data_layer/base_feeder.py`: add `atp: float = 0.0` to `OptionTick`.
- `FyersFeeder._parse_frame`: set `atp = raw.get("avg_trade_price") or 0.0`.
- `UpstoxFeeder._parse_frame`: set `atp` from full-mode `marketFF.atp`
  (the eFeedDetails / atp field). Verify exact path against a live decoded frame.

### 4.3 IndicatorEngine (`indicator_engine.py`)
- Subscribes to `CANDLE_CLOSE` and `OPTION_TICK`.
- State per `(symbol, timeframe)`: rolling closed-candle OHLCV + combined-premium
  close series (for RSI/ROC). State per instrument: latest ATP + closed-minute
  ATP keyed by minute timestamp.
- On `CANDLE_CLOSE(tf)`: compute that tf's computed indicators once; store the
  snapshot under the candle's **closed-minute timestamp**.
- API:
  - `get(symbol, tf, name) -> float | None`
  - `snapshot(symbol, tf) -> dict`
  - `atp(instrument_symbol) -> float` (latest) and `atp_at_close(instrument, minute)`
  - `vwap_slope(vwap_series, occurrences)` via slope.py
- The engine does NOT know strategy legs; strategies pass their leg ATPs to get
  combined VWAP/slope.

### 4.4 RuleEvaluator (`rule_evaluator.py`)
- `eval(rules, symbol, ctx) -> (passed, reason)` where `ctx` supplies
  strategy-specific values (e.g. combined VWAP/SLOPE for the strategy's legs).
- For each rule: read `tf`, fetch the operands from that **tf's** engine snapshot
  (or from `ctx` for combined-leg values), apply `operator_sym`, honor
  brackets/AND/OR and `occurrences` for SLOPE.
- Returns the rich `OPERAND(value)op OPERAND(value)=✓/✗` reason string already in
  use for logging — now per-tf-correct.

### 4.5 Strategy plugin framework
- **plugin.py**: `StrategyPlugin` ABC — `name`, classmethod `enabled(cfg, underlying)`,
  `__init__(bus, cfg, underlying, engine, evaluator, registry)`, `async start()`,
  `async stop()`. `@register_strategy(name)` registers into `StrategyRegistry`.
- **loader.py**: `load_enabled(...)` iterates monitored indices × registered
  plugins; starts those whose `enabled()` is true. Returns started instances for
  lifecycle management.
- **run_system.py**: replace hardcoded strategy construction with the loader.

### 4.6 Strategy migration
Each of IC / Straddle / Trap:
- Becomes a `StrategyPlugin` subclass with `@register_strategy`.
- Keeps its entry/exit decision logic and order-event publishing (unchanged
  downstream).
- Removes private indicator computation; reads indicators from the engine and
  builds its VWAP/SLOPE by combining its own legs' feed ATP.
- IC keeps immediate (tick-driven) entry; straddle keeps rule-gated entry.

## 5. Error handling
- Missing ATP (instrument hasn't ticked yet) → VWAP/SLOPE return None; rule using
  them evaluates ✗ with reason `VWAP(N/A)`; strategy logs WAIT, does not enter.
- Engine never raises into the bus loop; per-callback try/except with logged
  warnings.
- Loader: a plugin that fails to start is logged and skipped; others continue.

## 6. Testing
- Unit tests per indicator file vs reference values (rsi, roc, slope, vwap-combine).
- IndicatorEngine: feed synthetic CANDLE_CLOSE + OPTION_TICK, assert closed-minute
  snapshot values and that the +5s read returns the closed-minute value.
- RuleEvaluator: a rule with tf=2 reads the 2m snapshot, not 1m; occurrence count.
- Loader: enabled=false plugin never starts; enabled=true does.
- Regression: NIFTY IC still produces non-zero net_credit and routes orders.

## 7. Rollout
Full refactor, but staged commits to keep the tree runnable:
1. Indicators package (split + re-export) — no behavior change.
2. OptionTick ATP capture in feeders.
3. IndicatorEngine + RuleEvaluator (added, not yet consumed).
4. StrategyPlugin + registry + loader; wire run_system.
5. Migrate straddle to plugin + engine (most affected by VWAP/SLOPE fix).
6. Migrate IC and Trap.
7. Remove dead per-strategy indicator code.

## 8. Open items to verify during implementation
- Exact Upstox full-mode ATP field path (confirm from a live decoded frame).
- Whether combined VWAP should use both legs' ATP at the SAME closed minute
  (yes — snapshot both at the minute boundary).
- Trap engine's premium cache keying (separate known issue) folds into engine ATP.
