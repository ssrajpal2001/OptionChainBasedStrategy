# Sell Straddle — Balanced-Pair Entry Selection (Design)

**Date:** 2026-06-02
**Scope:** Replace the current spot-ATM same-strike entry selection in
`strategies/sell_straddle.py` with an exact replica of the reference
(`E:\Option_Selling\Option_Selling_May_2026\bot\hub\sell_v3\entry_logic.py`)
anchor/partner + pool-scan selection.

**Out of scope (DO NOT CHANGE):** `_check_exits`, `_close_position`,
`_try_smart_roll` exit/rollover *decision logic*. These stay exactly as today.
The only exit-path touch is mechanical leg-LTP plumbing required to support a
CE strike that differs from the PE strike (see §6).

---

## 1. Why this is feasible without REST priming

The live NIFTY entry rules (`data/strategy_config.json`
→ `indices.NIFTY.sell_straddle.entry_rules_beginning / entry_rules_reentry`)
use **only two gates**, both VWAP-derived:

- `CLOSE < VWAP`  (combined premium below combined VWAP)
- `SLOPE < 0`     (combined VWAP slope negative)

No RSI, no ROC. VWAP = broker ATP per strike, delivered on every option tick.
Therefore the reference's per-strike technical gates are reproducible **from
feed data alone** — the reference's `_prime_indicators_for_strikes`,
`calculate_combined_rsi`, `calculate_combined_roc` are **not required** for this
config. (If a future config adds RSI/ROC rules, per-strike OHLC history /
priming becomes a separate follow-up — explicitly out of scope here.)

VWAP is never computed (per project rule); it is the broker ATP. SLOPE is the
delta of two *closed* combined-VWAP values, with the previous value updated only
on a valid (non-zero) VWAP — identical to today's ATM logic, generalised to any
pair.

---

## 2. Components

### 2.1 Per-strike feed cache  (`_option_loop`)
Maintain `self._strike_prem: Dict[Tuple[int,str], dict]` keyed by
`(int(strike), "CE"|"PE")` with `{"ltp": float, "atp": float}`, updated for
**every** subscribed strike tick where `ltp > 0` (not just ATM). The existing
ATM-only `_ce_ltp/_pe_ltp/_ce_atp/_pe_atp` capture stays (used by the open
position's combined indicators and exits). Cleared in `reset_session()`.

Subscription depth: pool scan reads ATM ± `pool_offset` (default 4). The
StrikeRebalancer subscribes ATM ± `chain_depth` (default 10), so the pool is
always a subset of subscribed strikes. No new subscription work.

### 2.2 Per-pair closed-VWAP tracking  (`_on_candle`, 1-minute base)
Maintain `self._prev_atp_closed: Dict[Tuple[int,str], float]` — the previous
*closed-candle* ATP per (strike, side). On each 1-minute candle close, for every
key currently in `_strike_prem` with `atp > 0`, set
`_prev_atp_closed[key] = current atp`. (Same "update prev only on valid VWAP"
discipline as the current ATM path.)

For any pair `(ce_strike, pe_strike)` the per-pair indicators are:
```
close = ce_ltp + pe_ltp                                  # current
vwap  = ce_atp + pe_atp                                  # current combined VWAP
slope = (ce_atp + pe_atp) - (ce_prev_atp + pe_prev_atp)  # combined VWAP slope
```
`slope` is available only once both legs have a prior closed ATP (mirrors the
reference "V-Slope data not ready" gate).

### 2.3 Per-pair rule evaluation
New helper `_pair_indicators(ce_strike, pe_strike) -> Optional[dict]` returns the
`{close, vwap, slope}` dict above (None if either leg's LTP/ATP missing). The
existing `_eval_rules(rules, ind)` is reused **unchanged** — it already maps
`CLOSE`, `VWAP`, `SLOPE` operands. This guarantees identical gate semantics to
today; only the *input strikes* change.

### 2.4 Beginning concept — `_select_balanced_pair(now)`
Exact port of reference `_get_strictly_lower_balanced_pair`:
1. `atm = round(spot/step)*step`. Read ATM CE/PE LTP from `_strike_prem`.
   If either is 0 → return None (wait).
2. Intrinsic-stripped (time-value) LTP: `ce_corr = ce_ltp − max(0, spot−atm)`,
   `pe_corr = pe_ltp − max(0, atm−spot)`.
3. Anchor = side with **lower** corrected LTP; partner = other side.
4. Anchor raw LTP must be ≥ `ltp_target`, else None.
5. Scan partner side over `atm + i*step` for `i ∈ [−offset, +offset]`; keep
   strikes with `ltp_target ≤ ltp < anchor_ltp`; pick the **highest** such LTP
   (closest below the anchor).
6. Return `(ce_strike, pe_strike, ce_ltp, pe_ltp)` assembled from anchor+partner.

### 2.5 Re-entry concept — `_scan_pool(now)`
Exact port of reference `_scan_v_slope_pool` (VWAP/slope metric only):
1. Strikes = `atm + i*step`, `i ∈ [−offset, +offset]`.
2. ATM bias from corrected ATM LTP (`ce_corr > pe_corr` → CE stronger).
3. N×N over (s_ce, s_pe): require both LTP ≥ `ltp_target`; apply bias filter
   (CE stronger → require `ce_ltp < pe_ltp`; else `pe_ltp < ce_ltp`).
4. Per-pair gate via `_eval_rules(rules, _pair_indicators(...))`.
5. `balanced_score = abs(ce−pe)/(ce+pe)`; keep passing pairs; pick **min**
   balanced_score (config `reentry_best_metric` default `balanced_premium`;
   `vwap_pct` variant optional, matching reference).

### 2.6 Hybrid workflow  (`_try_entry`)
- `is_beginning = (trades_today == 0)`; rule key as today.
- `workflow_mode = ss.get("entry_workflow_mode", "hybrid")`.
- beginning_only → always beginning concept; reentry_only → always pool scan;
  hybrid → beginning concept when `is_beginning`, else pool scan.
- Hybrid transition: if beginning concept's selected pair **fails** the gate,
  fall through to pool scan on the next pulse (reference sets
  `workflow_phase='CONTINUE'`; our equivalent: a `self._beginning_failed` flag
  that routes the next `_try_entry` to the pool scan even while `trades_today==0`).

All existing pre-checks stay: entry window, `max_trades`, SL cooldown,
`_order_pending`, priming wait, day/stop-for-day guards.

---

## 3. Entry orchestration (replaces the body of `_try_entry` after gates)

```
pair = _select_balanced_pair(now)         # or _scan_pool(now) per workflow mode
if pair is None: log WAIT/BLOCK; return
ce_strike, pe_strike, ce_ltp, pe_ltp = pair
ind = _pair_indicators(ce_strike, pe_strike)
passed, reason = _eval_rules(rules, ind)
log EVAL (which strikes, rule values, PASS/BLOCK)
if not passed: (hybrid → set _beginning_failed) ; return
await _open_position(now, ce_strike, pe_strike, ce_ltp, pe_ltp, reason)
```

`ltp_target` floor: enforced inside selection (both legs ≥ target) — replaces the
current ATM-only `ltp_target` block, preserving "neither leg below floor".

---

## 4. `_open_position` changes
Accept explicit `ce_strike, pe_strike, ce_ltp, pe_ltp`. Build:
- `ce_leg = StraddleLeg("CE", ce_strike, ce_ltp, ce_ltp)`
- `pe_leg = StraddleLeg("PE", pe_strike, pe_ltp, pe_ltp)`
- `atm_at_entry = round(spot/step)*step` (kept for logging/roll reference)
- `StraddleOrderEvent`: `ce_strike=ce_strike`, `pe_strike=pe_strike`
  (today both are passed `atm`). Bridge already accepts separate ce/pe strikes.

`entry_indicators` stores the per-pair `ind`.

---

## 5. Indicator/session bookkeeping
- `reset_session()` clears `_strike_prem` and `_prev_atp_closed`.
- The ATM combined buffers (`_prem_closes`, `_recompute_indicators`) remain for
  the **open position's** exit indicators (VWAP rise SL, guardrail_roc,
  exit_rules) — unchanged.

---

## 6. Required exit-path plumbing (mechanical only — no rule change)

Because `ce_leg.strike` may differ from `pe_leg.strike`:

1. **`_option_loop` open-position LTP update** — today matches both legs against
   `position.atm_at_entry`. Change to update **each leg from its own strike**:
   `if tick.strike == ce_leg.strike and type==CE → ce_leg.ltp`; likewise PE.
2. **Physical roll** (`_try_smart_roll`) — the new position is opened at the new
   ATM. Keep the existing roll decision logic; just construct the new legs with
   their own strikes (new ATM symmetric is fine — rolls re-anchor to ATM). The
   `new_atm == pos.atm_at_entry` virtual/physical branch is unchanged.

No exit *thresholds*, *triggers*, or *ordering* change. `unrealized_pnl`,
`current_value`, ratio/TSL/VWAP-rise/decay all read `ce_leg.ltp`/`pe_leg.ltp`,
which now update correctly per asymmetric strike.

---

## 7. Testing / verification
- Unit: `_select_balanced_pair` and `_scan_pool` against hand-built tick maps —
  assert anchor=less-time-value side, partner=highest LTP strictly below anchor,
  pool min-balanced-score winner, `ltp_target` floor, bias filter. Mirror the
  reference's documented examples.
- Unit: `_pair_indicators` slope = combined-VWAP delta; None until both legs have
  a prior closed ATP.
- Unit: asymmetric leg LTP update routes ticks to the correct leg.
- Regression: with a symmetric pair (ce_strike==pe_strike) behaviour matches the
  current app (no change for the common ATM case).
- Live dry-run (no funds): confirm EVAL log shows the selected CE/PE strikes,
  the gate values, and that ENTRY + final EXIT orders reach the bridge.

---

## 8. Risks / notes
- Slope readiness: a pair whose legs lack two closed ATP values gates out
  (correct — matches reference "data not ready"). Early-session entries wait,
  same as today's priming.
- `_beginning_failed` must reset at `reset_session()` and after a successful
  entry so a later re-entry uses the pool scan path intentionally, not by
  leftover state.
- Out of scope, tracked separately: rollover currently does **not** publish
  ORDER_REQUEST events (virtual/physical roll only log intent). Unchanged here.
