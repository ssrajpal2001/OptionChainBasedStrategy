# Iron Condor Strategy — Design Specification

**Date**: 2026-08-10
**Status**: Approved by user, ready for implementation planning
**Audience**: This document is written to be shown to the client as-is. The
"Technical Notes" section at the end is for the engineering record.

---

## What This Strategy Does

Iron Condor is a new premium-selling strategy, in the same family as the
existing Sell Straddle strategy, but built with defined, capped risk from
the moment the trade is placed — never a naked, unlimited-risk position.

### The trade, step by step

1. **Find the trade.** Scan the live option chain for a call (CE) and a put
   (PE) trading at a healthy premium — by default, ₹50 or more. These are
   the options to **sell**.
2. **Find the insurance.** Scan for a cheaper call and put, further away
   from the current market price — by default, around ₹20 premium. These
   are bought as **hedges**, not for profit — purely to cap the maximum
   possible loss on the trade. This is what turns a naked short straddle
   into a proper Iron Condor: four legs, defined risk, known worst case.
3. **Wait for the right entry price.** Add up the net value of all four
   legs together (premium collected from the two sold legs, minus premium
   paid for the two bought hedges). This net number is compared against a
   **fixed reference price**: the VWAP (volume-weighted average price) of
   this exact same four-leg combination from the **previous trading day**.
   That reference number is calculated once, from yesterday, and does not
   move during today's session.
   The strategy waits, tick by tick, until today's live net value **touches**
   that fixed reference price. That touch is the entry signal — all four
   legs go in together, at the same time.
4. **Manage the trade with a fixed target and stop-loss.** Once in the
   trade, there is no rolling and no adjustment. The position is held until
   either:
   - a **profit target** is hit (a fixed number of points of gain on the
     net four-leg position), or
   - a **stop-loss** is hit (a fixed number of points of loss on the net
     four-leg position).
   Whichever comes first closes **all four legs together**, as one
   transaction. The trade is then flat until the next entry signal.

### What's dynamic vs. fixed

| Parameter | Default | Configurable? |
|---|---|---|
| Minimum LTP to sell (CE/PE) | ₹50 | Yes, per deployment |
| Minimum LTP for hedge (CE/PE) | ₹20 | Yes, per deployment |
| Profit target (points, net 4-leg) | — | Yes, per deployment |
| Stop-loss (points, net 4-leg) | — | Yes, per deployment |
| Entry trigger reference | Previous day's VWAP of the 4-leg combo | Fixed methodology, recalculated daily |

### A worked example (illustrative numbers)

- NIFTY option chain scan finds: Sell 24,500 CE @ ₹55, Sell 24,300 PE @ ₹52
  → combined sell credit ₹107.
- Hedge scan finds: Buy 24,700 CE @ ₹22, Buy 24,100 PE @ ₹19 → combined
  hedge cost ₹41.
- Net credit if entered = ₹107 − ₹41 = **₹66**.
- Yesterday, this same four-leg combination's VWAP worked out to **₹64**.
- The strategy watches live ticks. The moment the net value of these four
  legs touches ₹64 (whether it approaches from above or below), it enters
  all four legs at once.
- From there: if the net position gains, say, 25 points (net value moves
  to ₹39, since this is a net credit position — profit as the structure
  cheapens), the target closes it. If it loses 15 points instead (net value
  rises to ₹79), the stop-loss closes it. Both threshold numbers are set
  per deployment, not hardcoded to this example.

### What this strategy deliberately does NOT do

- No rolling a losing leg to a better strike (unlike Sell Straddle).
- No indicator-based exits (no RSI, VWAP-rise, ADX, etc.) — the only
  reference value used anywhere is the fixed previous-day VWAP for entry
  timing.
- No per-leg management once in the trade — entry and exit are always all
  four legs together, as a single atomic unit.

---

## Technical Notes (engineering record)

**Reuses from the existing system:**
- Option chain / OI data: `OptionMatrixEngine` chain snapshots (already
  running, feeds the dashboard today — see `bear_only_book.py`'s OI-wall
  selection for the existing pattern of reading `Topic.MATRIX_SNAPSHOT`).
- Historical candle fetch for the previous-day VWAP calculation:
  `data_layer/historical_candles.py` (already used throughout the D1Trap
  family for daily/warmup bars).
- Order placement / multi-leg atomic entry: `ExecutionRouter` +
  `OrderRequest` pipeline, same pattern as Sell Straddle's 2-leg entry,
  extended to 4 legs.
- Per-(client, binding) deployment lifecycle: same `StrategyBookManager`
  base class pattern as every other strategy in this system.

**New work required (not a reuse of anything existing):**
- The strike-selection scan itself (find CE/PE crossing the sell/hedge LTP
  thresholds) — no existing strategy scans the chain this way.
- The previous-day VWAP calculation for a **dynamically-selected**
  four-leg combination. Working assumption (needs confirmation before
  implementation, see Open Question below): today's strikes are selected
  first (via the LTP-threshold scan, using live/current prices), and then
  *those specific four contracts'* own historical intraday data from the
  previous trading day is fetched to compute their combined VWAP. This
  mirrors how "previous day's high/low" is normally used as a reference —
  the contract's own prior session, not a separately re-run selection
  process for "yesterday."
- The 4-leg atomic entry/exit sequencing and the points-based net-position
  SL/target tracking — no existing strategy in this codebase manages a
  4-leg structure as a single unit; Sell Straddle manages 2 legs
  independently (each can roll on its own), which does not apply here.
- No rollover engine is needed — this is simpler than Sell Straddle in that
  specific respect, not a reuse of Sell Straddle's rolling logic at all
  (an earlier draft of this design assumed rollover reuse; that was
  corrected during design review — there is no rollover concept in this
  strategy).

**Open question carried into implementation planning:**
Confirm the previous-day VWAP interpretation above (contracts selected by
today's live prices, VWAP pulled from their own price history yesterday)
is correct before implementation — this is the one part of the spec that
required an engineering assumption rather than being stated directly.
