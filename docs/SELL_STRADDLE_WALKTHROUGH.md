# Sell Straddle — Complete Start-to-Stop Walkthrough

Every bit and piece joined together, in execution order, so nothing is left behind.
File anchors point to `strategies/sell_straddle.py` and `strategies/straddle_selection.py`.

Reference parity target: `E:\Option_Selling\Option_Selling_May_2026` sell_v3.

---

## 0. Cast of components

| Piece | File | Role |
|---|---|---|
| `SellStraddleStrategy` | `strategies/sell_straddle.py` | The engine. Subscribes to the bus, decides entries/exits, emits orders. |
| Selection math | `strategies/straddle_selection.py` | Pure functions: `strip_intrinsic`, `pair_indicators`, `select_balanced_pair`, `scan_pool`. |
| EventBus | `data_layer/base_feeder.py` | asyncio pub-sub. Topics: INDEX_TICK, OPTION_TICK, CANDLE_CLOSE, ORDER_REQUEST, ORDER_FILL. |
| StraddleExecutionBridge | `execution_bridge/straddle_bridge.py` | Turns ORDER_REQUEST into broker orders per-leg; publishes ORDER_FILL. |
| PositionStore | `data_layer/position_store.py` | JSON persistence; survives restarts; discards prior-day MIS. |
| RuntimeConfig | `data_layer/runtime_config.py` | Live read of `data/strategy_config.json` per index. |

Four async loops run concurrently, started in `start()` (sell_straddle.py:349):
`_candle_loop`, `_tick_loop`, `_option_loop`, `_fill_loop`.

---

## 1. STARTUP

`start()` (sell_straddle.py:349):
1. Sets `_running = True`.
2. **Restore**: `position_store.load("NIFTY_sell_straddle")`. If a position is returned
   (NRML carryover, or same-day MIS), rebuild it via `StraddlePosition.from_dict` and set
   `_trades_today = 1`. Prior-day MIS is discarded by the store (broker auto-squared at EOD).
3. Spawns the four loops.

State that matters across the session (set in `__init__`, wiped by `reset_session`):
- `_strike_prem: {(strike, side): {ltp, atp}}` — per-strike feed cache (ALL subscribed strikes).
- `_prev_atp_closed: {(strike, side): atp}` — previous **closed-candle** ATP per leg (for slope).
- `_beginning_failed` — hybrid flag (beginning gate failed → use pool scan next pulse).
- `_trades_today`, `_position`, `_sl_cooldown_until`, `_stop_for_day`, `_initial_net_credit`.

---

## 2. THE FEED FILLS THE CACHE  (continuous)

**Index ticks** → `_tick_loop` (sell_straddle.py:414): sets `self._spot = tick.ltp`. If a
position is open, calls `_check_exits()` on every tick (exits are tick-driven, not candle-driven).

**Option ticks** → `_option_loop` (sell_straddle.py:471). For every option tick whose
`underlying == "NIFTY"`:
1. **Per-strike cache write** (the foundation of balanced-pair selection): for any `ltp > 0`,
   `_strike_prem[(int strike, side)] = {ltp, atp}` where `atp` = broker Average Traded Price =
   the exchange VWAP for that contract. VWAP is **never computed** — it is read from the feed.
2. **ATM capture**: if the tick is the ATM strike, also store `_ce_ltp/_pe_ltp` and
   `_ce_atp/_pe_atp` (used for the OPEN position's combined indicators and exits).
3. **Open-position leg pricing** (per-leg strike — supports asymmetric CE/PE):
   a CE tick updates `ce_leg.ltp` only if `tick.strike == ce_leg.strike`; PE likewise.

Which strikes arrive? The StrikeRebalancer subscribes ATM ± `chain_depth` (10), so the pool
(ATM ± `offset`, default 4) is always a subset already in the cache.

---

## 3. EVERY 1-MINUTE CANDLE  (the heartbeat)

`_candle_loop` (sell_straddle.py:397) receives CANDLE_CLOSE, filters to `symbol == "NIFTY"`
and `timeframe == 1` (one clean base series), then `_on_candle` (sell_straddle.py:519):

1. **New-day reset**: if the stored market-open date ≠ today → `reset_session()` (wipes all
   intraday state incl. caches and `_beginning_failed`).
2. **Market-open anchor**: first candle of the day fixes `_market_open_dt` at 09:15.
3. **Buffers**: append index H/L/C and combined ATM premium to ring buffers.
4. `_recompute_indicators()` — computes the OPEN position's combined indicators (RSI, combined
   VWAP from ATM ATP, slope, ROC) used by exit rules. (Entry uses per-pair indicators instead;
   see §4.)
5. **Previous-closed-ATP snapshot** (enables per-pair slope): for every leg in `_strike_prem`
   with a valid ATP, `_prev_atp_closed[leg] = atp`. Only overwrites on a valid ATP, so a missing
   tick never corrupts the next slope.
6. **EOD force-exit**: if `now >= squareoff_time` and a position is open → `_close_position
   ("time_exit_eod")` and return.
7. **Entry**: if no open position → `_try_entry(now)`.

---

## 4. ENTRY — `_try_entry`  (sell_straddle.py:696)

### 4.1 Pre-checks (all must pass, else return)
`_stop_for_day` off · inside `[entry_start, entry_end)` · `_trades_today < max_trades` ·
not in SL cooldown · not `_order_pending` · spot/ATM-CE/ATM-PE all `> 0` (need ticks; selection
reads ATM for anchor/bias). The ltp_target floor is **not** pre-gated here — it is enforced
inside selection (reference parity).

### 4.2 Dynamic rule set + workflow mode
- `is_beginning = (trades_today == 0)`.
- `workflow_mode = entry_workflow_mode` (default `hybrid`):
  - `beginning_only` → always beginning concept
  - `reentry_only` → always pool scan
  - `hybrid` → beginning concept on first trade, UNLESS `_beginning_failed` (then pool scan)
- `rule_key = entry_rules_beginning | entry_rules_reentry`; `rules = ss.get(rule_key, [])`.
  **These rules are read live from config and are fully dynamic** — nothing hardcoded.
  Live NIFTY rules: `CLOSE < VWAP` and `SLOPE < 0` (both VWAP-derived, from broker ATP).
- **Priming wait**: `_is_primed(now, rules)` — no entry until `market_open + wait_minutes`
  (wait = max_rule_tf × 2 if any SLOPE rule, else ×1). Mirrors reference `_is_in_priming_wait`.

### 4.3 Strike selection (the new heart)
`step`, `offset = v_slope_pool_offset|reentry_offset|4`, `ltp_target = _ltp_target|50`.

**Beginning concept** → `select_balanced_pair(_strike_prem, spot, step, offset, ltp_target)`
(straddle_selection.py). Exact port of reference `_get_strictly_lower_balanced_pair`:
1. ATM both sides; need both LTP > 0.
2. **Intrinsic-stripped (time-value) LTP**: `CE_corr = ce_ltp − max(0, spot−atm)`,
   `PE_corr = pe_ltp − max(0, atm−spot)`.
3. **Anchor** = side with the LOWER time value; **partner** = the other side.
4. Anchor raw LTP must be ≥ `ltp_target`.
5. **Partner search** over ATM ± offset: keep strikes with `ltp_target ≤ ltp < anchor_ltp`;
   pick the **highest** such LTP (closest below the anchor). May be a DIFFERENT strike than ATM.
6. Returns `(ce_strike, pe_strike, ce_ltp, pe_ltp)`.

**Re-entry concept** → `scan_pool(...)`. Exact port of reference `_scan_v_slope_pool`
(balanced_premium metric):
1. Strikes = ATM ± offset (**ATM included**, matching reference).
2. ATM **bias** from corrected ATM LTP: CE stronger if `CE_corr > PE_corr`.
3. N×N over (s_ce, s_pe): both LTP ≥ `ltp_target`; **bias filter** (CE stronger → require
   `ce_ltp < pe_ltp`; else `pe_ltp < ce_ltp`).
4. **Per-pair technical gate** — `rule_pass(s_ce, s_pe)` = `_eval_rules(rules,
   _pair_indicators(s_ce, s_pe))`. This is the SAME dynamic evaluator, applied to each pair's
   own `{close, vwap, slope}`.
5. `balanced_score = |ce−pe| / (ce+pe)`; pick the **minimum** score.

`_pair_indicators(ce_strike, pe_strike)` (sell_straddle.py, delegates to
`straddle_selection.pair_indicators`): `close = ce_ltp+pe_ltp`, `vwap = ce_atp+pe_atp`,
`slope = (ce_atp+pe_atp) − (ce_prev+pe_prev)` (only when both legs have a previous closed ATP).
All from the feed — no REST priming needed for the live VWAP/SLOPE rules.

### 4.4 Final gate + log
With the selected pair, build `ind = _pair_indicators(...)` and run
`passed, reason = _eval_rules(rules, ind)`. The `EVAL` line in
`logs/clients/ss_NIFTY_*.log` shows the exact CE/PE strikes, each rule's live value (✓/✗),
PASS/BLOCK, and the full indicator dict.
- **BLOCK** + hybrid beginning → set `_beginning_failed = True` (next pulse uses pool scan); return.
- **PASS** → `_open_position(now, ce_strike, pe_strike, ce_ltp, pe_ltp, rule_key, reason)`.

---

## 5. OPENING THE POSITION — `_open_position`  (sell_straddle.py:794)

1. Build `StraddlePosition`:
   - `ce_leg = StraddleLeg("CE", ce_strike, ce_ltp, ce_ltp)`,
     `pe_leg = StraddleLeg("PE", pe_strike, pe_ltp, pe_ltp)` — **independent strikes**.
   - `atm_at_entry = round(spot/step)*step` (reference for logging / physical-roll re-anchor).
   - `net_credit = ce_ltp + pe_ltp`; `entry_indicators = _pair_indicators(...)`;
     `session_min_vwap` seeded for VWAP-rise SL.
2. `_persist()` → PositionStore JSON (survives restart).
3. `_trades_today += 1`; `_beginning_failed = False`; `_order_pending = True`.
4. Lock `_initial_net_credit` (denominator for day-% guardrails) on the first trade.
5. Publish a `StraddleOrderEvent(action="ENTRY", ce_strike, pe_strike, ce_ltp, pe_ltp, ...)`
   on `Topic.ORDER_REQUEST`.

**Bridge** (`execution_bridge/straddle_bridge.py`): receives the event, places **two orders**
(SELL CE @ ce_strike, SELL PE @ pe_strike) via the routed broker (paper or live Zerodha),
logs an `ENTRY` line, and publishes a `StraddleFillEvent(action="ENTRY")` on `Topic.ORDER_FILL`.

**Fill** → `_fill_loop` (sell_straddle.py:430) → `_on_fill`: stamps actual fill prices onto both
legs and `net_credit`, clears `_order_pending`. Now the position is fully live.

---

## 6. MANAGING THE OPEN POSITION — `_check_exits`  (sell_straddle.py, tick-driven)

Runs on **every index tick** while a position is open. Priority order (first match wins):

1. **EOD force square-off** (`now >= squareoff_time`) → `_close_position("eod_squareoff")`,
   `_stop_for_day = True`.
2. **Day-level % guardrails**: `(session_realized + running_pnl)/initial_credit ×100` vs
   `day_profit_target_pct` / `−day_loss_sl_pct` → close + stop for day.
3. **guardrail_pnl**: cumulative session premium points vs target/SL.
4. **LTP decay**: a leg's LTP below `ltp_exit_min` → try smart roll, else close.
5. **Trailing SL**: activates once profit ≥ `net_credit × trail_lock_pct`; exits if profit falls
   below `peak − net_credit × trail_floor_pct`.
6. **Scalable TSL** (`tsl_scalable`): ₹ staircase lock (`base_lock + N×step_lock`); breach →
   try smart roll, else close.
7. **Ratio exit**: `max(ce,pe)/min(ce,pe) ≥ ratio_threshold` → try smart roll, else close.
8. **VWAP rise SL**: combined VWAP risen ≥ `threshold%` above session low → try smart roll, else close.
9. **guardrail_roc**: TF-boundary ROC of combined premium vs target/SL → try smart roll, else close.
10. **exit_rules** (dynamic, config): evaluated once per TF bucket via `_eval_rules` → try smart roll, else close.

> Leg LTPs feeding all of the above now update per-leg-strike (§2.3), so exit math is correct
> even when CE strike ≠ PE strike.

---

## 7. ROLLOVER — `_try_smart_roll`  (sell_straddle.py:1022 region)  — UNCHANGED

Triggered by the "try smart roll" exits (decay, scalable TSL, ratio, VWAP-rise, ROC, exit_rules).
Re-evaluates `entry_rules_reentry` against current indicators:
- **FAIL** → returns False → caller performs a **full exit** (§8).
- **PASS**, branch on current ATM vs `pos.atm_at_entry`:
  - **Virtual roll** (same ATM): keep the position; refresh entry prices to current LTP; reset
    `net_credit`, TSL lock, peak, trailing, `open_time`, `session_min_vwap`. "Reset the clock."
  - **Physical roll** (ATM moved): book the old P&L (`status=closed`), open a NEW position at the
    new (symmetric) ATM immediately, no cooldown.

> Note (unchanged behavior): rolls update state and log `ORDER INTENT` but do **not** currently
> publish ORDER_REQUEST — i.e. a roll is virtual/logged, not a broker round-trip. Tracked as a
> separate future item; intentionally out of scope here.

---

## 8. FULL EXIT — `_close_position(reason)`  (sell_straddle.py:1111 region)  — UNCHANGED

1. `realized_pnl = net_credit − current combined LTP`; `status = "closed"`.
2. Publish `StraddleOrderEvent(action="EXIT", ce_strike, pe_strike, ...)` → bridge BUYs back both
   legs at the broker → logs `EXIT` → publishes ORDER_FILL(EXIT).
3. Accumulate `session_realized_pnl_pts`; `_position = None`; `_persist()` clears the JSON.
4. If reason was a stop-loss → `_apply_sl_cooldown()` (blocks re-entry for ~1 candle of max TF).

After a full exit (not stop-for-day), the next candle's `_try_entry` runs with
`is_beginning = False` → **re-entry concept (pool scan)** with `entry_rules_reentry`.

---

## 9. PERSISTENCE & LIFECYCLE EDGES

- **Restart mid-position**: `start()` restores from JSON; `_option_loop` re-prices legs from
  live ticks (per-leg strike); exits resume seamlessly.
- **MIS new day**: PositionStore discards a prior-day MIS file on load → not restored (broker
  squared it at EOD). NRML carries forward.
- **New trading day while running**: `_on_candle` detects date change → `reset_session()` wipes
  caches, trade count, day-P&L, and `_beginning_failed`.
- **stop()**: cancels the four loops.

---

## 10. END-TO-END TRACE (one session, NIFTY, hybrid)

```
09:15  market open; _market_open_dt set; priming wait begins
09:15+ option ticks fill _strike_prem (ATM±10); _spot from index ticks
each 1m candle: prev_atp_closed snapshot; _recompute_indicators
~09:17 priming done (max_rule_tf×2). _try_entry, is_beginning=True
        → select_balanced_pair → anchor=lower-time-value side, partner=closest-lower LTP
        → _eval_rules(entry_rules_beginning, pair_ind): CLOSE<VWAP ✓ & SLOPE<0 ✓ → PASS
        → _open_position → SELL CE@strikeA + SELL PE@strikeB (may differ) → bridge → fill
... ticks → _check_exits each tick (profit/TSL/ratio/VWAP/ROC/exit_rules)
        → ratio exit fires → _try_smart_roll(reentry rules) → virtual or physical roll
... later → full exit (e.g. day profit target) → BUY back both legs → _stop_for_day
15:15  any open position force-squared (eod_squareoff)
next day → reset_session → fresh start
```

---

## 11. WHAT CHANGED vs THE OLD APP (this branch)

| Area | Before | Now (reference parity) |
|---|---|---|
| Strike selection | spot-ATM, SAME strike both legs | anchor/partner balanced pair (beginning) + N×N pool scan (re-entry); CE/PE may differ |
| Entry indicators | combined ATM premium only | **per-pair** `{close, vwap, slope}` from each pair's broker ATP |
| Intrinsic handling | none | time-value stripping for anchor/bias (CE `max(0,spot−K)`, PE `max(0,K−spot)`) |
| Workflow | beginning/re-entry by trade count | hybrid mode + `_beginning_failed` transition (reference) |
| Leg pricing | both legs vs single ATM | per-leg-strike routing (asymmetric-safe) |
| Exits / rollover | (as built) | **UNCHANGED** |
| Rules | dynamic via `_eval_rules` | **still dynamic** — only the evaluated strikes changed |

Tests: `tests/strategies/test_straddle_selection.py` (13),
`tests/strategies/test_sell_straddle_legs.py` (1),
`tests/strategies/test_straddle_entry_integration.py` (1). Hot-path runtime smoke verified.
