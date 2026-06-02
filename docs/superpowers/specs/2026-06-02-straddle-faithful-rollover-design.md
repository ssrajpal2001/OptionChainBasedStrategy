# Sell Straddle — Faithful sell_v3 Rollover/Exit Mirror (Design)

**Date:** 2026-06-02
**Branch:** `feat/straddle-faithful-rollover`
**Reference:** `E:\Option_Selling\Option_Selling_May_2026\bot\hub\sell_v3\exit_logic.py`
and `bot\hub\sell_manager_v3.py` (`_single_side_roll`, `_single_side_roll_to`,
`_execute_full_exit`, `perform_smart_roll`).

## Goal
Mirror the reference sell_v3 **rollover and exit** behavior in
`strategies/sell_straddle.py` so our straddle matches it: real single-leg orders,
the 0-or-2 leg invariant, single-side LTP-decay rolls, and a 4-outcome smart roll.
Entry selection (balanced pair) and exit *ordering* are already done on master.

## Already done (master, not in this spec)
- Balanced-pair entry selection (`straddle_selection.py`).
- Exit priority order: EOD → Day% → guardrail_pnl → LTP-decay → Ratio → Scalable
  TSL → ROC → VWAP-rise → exit_rules; pct-trailing-SL removed.
- Startup settings banner.

## Out of scope
- Entry selection internals (done).
- Telegram notifications, dashboard enrichment, rust-core (reference extras not in our app).

---

## Component 1 — Bridge: single-leg order capability

`execution_bridge/straddle_bridge.py`.

- `StraddleOrderEvent` gains `legs: List[str] = field(default_factory=lambda: ["CE","PE"])`.
- `StraddleFillEvent` gains `legs: List[str]` (echoes which legs were acted on).
- `_live_fill` / `_paper_fill` iterate only the legs in `ev.legs`. For an EXIT with
  `legs=["CE"]`, BUY only CE; for an ENTRY with `legs=["PE"]`, SELL only the new PE
  strike. Fill prices for non-acted legs are reported as 0.0 / omitted.
- Default `["CE","PE"]` → **identical** to today's whole-straddle behavior (regression-safe).

`SellStraddleStrategy._on_fill` updates only the legs present in `fill.legs`.

## Component 2 — Position invariant: always 0 or 2 legs

Hard rule from the reference: never leave a single open leg. Any single-side or
partial roll that fails its re-entry/placement **closes the surviving leg too**
(`_execute_full_exit(sides=[other_side])`). Enforced inside the roll helpers below.
A helper `_close_leg(side, reason)` publishes an EXIT with `legs=[side]`, books that
leg's P&L into `_session_realized_pnl_pts`, and removes the leg from the position
(marks `position=None` only when both legs are gone).

## Component 3 — LTP Decay → single-side roll

New `_single_side_roll(side, now, reason)` mirrors `sell_manager_v3._single_side_roll`:
1. Close the decayed leg: `_close_leg(side, reason)` (EXIT `legs=[side]`, cooldown off).
2. Re-scan: `scan_pool(strike_prem, spot, step, offset, ltp_target, rule_pass=reentry
   gate, metric)` → best pair; take the candidate for `side`.
3. Validate candidate LTP ≥ `ltp_target`; resolve the new strike.
4. Open it: ENTRY `legs=[side]` at the new strike; set `position.<side>_leg` to the new
   strike/price; refresh `open_time`/`session_min_vwap`; reset TSL lock.
5. **0-or-2 invariant:** if step 2–4 yields no candidate or fails → `_close_leg(
   other_side)` so no single leg remains; `position=None`.

`_check_exits` LTP-decay branch loops both sides (CE then PE) like the reference,
calling `_single_side_roll` per decayed side; returns after any roll.

## Component 4 — Smart roll: 4 outcomes

Rewrite `_try_smart_roll(now, trigger)` to mirror `exit_logic.perform_smart_roll`:
1. `cands = scan_pool(... reentry rules ...)`. If none → **Full exit**
   (`_close_position(f"full_exit_{trigger}")`), return True (handled).
2. Candidate `(ce_strike, pe_strike, ce_ltp, pe_ltp)`. Compare to current legs:
   `ce_same = (ce_strike == pos.ce_leg.strike)`, `pe_same = (pe_strike == pos.pe_leg.strike)`.
3. **Virtual** (both same): refresh both legs' entry prices to current LTP, reset
   `net_credit`, TSL lock, peak, trailing, `open_time`, `session_min_vwap`. No orders.
   (Matches today's virtual roll — kept.)
4. **Partial** (one same): `_single_side_roll_to(changed_side, candidate, now, trigger)`
   — close only the changed side (EXIT `legs=[changed]`), open the candidate strike
   (ENTRY `legs=[changed]`), update that leg; 0-or-2 invariant on failure.
5. **Physical** (both different): full close (EXIT both) then open the new pair
   (ENTRY both) — re-anchored to the scanned candidate strikes (not forced ATM).
6. Returns True in all cases (caller does not also close).

`_single_side_roll_to(side, candidate, now, reason)` mirrors the reference: close
`side`, place candidate strike on `side`, update leg; on failure close the other side.

## Component 5 — guardrail_pnl ordering + partial P&L

- Move the `guardrail_pnl` block **before** the Day-% block in `_check_exits`
  (reference runs PnL guardrail as the first mandatory guardrail).
- Session P&L: `_close_leg` books the **single leg's** realized P&L
  (`entry_price − current_ltp`) into `_session_realized_pnl_pts`; `_close_position`
  continues to book the full pair. This keeps day-% math correct across single-side
  rolls (reference accumulates `session_points_pnl` on every `_execute_full_exit`,
  including single-`sides` closes).

---

## Data flow (single-side decay roll)

```
tick → _check_exits → LTP decay (CE ltp < min)
   → _single_side_roll("CE")
       → _close_leg("CE")            → EXIT legs=["CE"]  → bridge BUY CE → fill
       → scan_pool(reentry gates)    → candidate CE strike (gated)
       → ENTRY legs=["CE"] new strike → bridge SELL CE  → fill
       → position.ce_leg = new strike/price
   (if no candidate) → _close_leg("PE") → position=None   [0-or-2 invariant]
```

## Error handling
- Any broker/order failure in a roll → treat as re-entry failure → enforce 0-or-2
  (close surviving leg). Never leave one leg open.
- Missing candidate LTP / contract → same path.
- All roll helpers are crash-guarded (the `_tick_loop`/`_candle_loop` already wrap
  exits; roll helpers additionally catch and fall back to full close).

## Testing
- **Bridge `legs` filter** (unit): EXIT `legs=["CE"]` acts on CE only; default acts on both.
- **Smart-roll outcome decision** (pure helper `classify_roll(ce_same, pe_same, has_cands)`
  → "virtual"|"partial_ce"|"partial_pe"|"physical"|"full_exit"): table-tested.
- **0-or-2 invariant** (unit): single-side roll with no candidate → both legs closed.
- **guardrail_pnl precedence** (unit/logic): with both day% and pnl-guardrail breached,
  pnl-guardrail reason wins (matches reference order).
- **Async smoke**: feed ticks → force CE decay → assert one EXIT(legs=CE) + one
  ENTRY(legs=CE) (or both-closed if no candidate).
- Regression: existing 15 selection tests stay green; whole-straddle ENTRY/EXIT
  (default legs) unchanged.

## Risks
- **Single-leg P&L accounting**: must book the closed leg's P&L exactly once; covered by
  `_close_leg` being the only single-leg close path.
- **Bridge regression**: default `legs` must preserve current behavior — guarded by a
  default-args test.
- Live deploy: lands on the branch; merged to master only after the dry run + tests
  validate. master stays deployable meanwhile.
