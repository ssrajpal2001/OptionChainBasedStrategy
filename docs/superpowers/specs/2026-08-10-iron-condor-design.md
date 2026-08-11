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
3. **Wait for the right entry price.** In every version of this check, one
   side is always the same: the **combined CLOSE** of all four legs — each
   leg's own close price, on whatever candle you're evaluating, added
   together (never netted/subtracted — sell-leg and hedge-leg closes are
   summed the same way). What that combined close is compared *against*
   has two independent modes, each switchable on/off:

   - **Trigger A — Previous-day VWAP mode**: compare today's combined
     close against **yesterday's fixed combined VWAP** (each of the four
     legs' own VWAP from the previous trading day, added together — one
     number, calculated once, that does not move during today's session).
     Enter when combined close ≥ yesterday's combined VWAP.
   - **Trigger B — Intraday VWAP mode**: compare today's combined close
     against **today's own combined VWAP so far** (each leg's own running
     VWAP, live, added together, continuously updating as today's session
     progresses). Enter when combined close ≥ today's own combined VWAP.

   **Both triggers can be turned on or off independently.** Only Trigger A
   on, only Trigger B on, or both on together (whichever fires first
   enters the trade) are all valid configurations. **For paper trading,
   both are enabled together** — running both at once is how their
   real-world behavior gets compared before deciding which to run live.
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
| Trigger A: prev-day VWAP mode | ON | Independently enable/disable |
| Trigger B: intraday VWAP mode | ON | Independently enable/disable |
| Combined-value method (both triggers, both sides) | Sum of all 4 legs' own close/VWAP (never netted) | Fixed methodology |

### A worked example (illustrative numbers)

- NIFTY option chain scan finds: Sell 24,500 CE @ ₹55, Sell 24,300 PE @ ₹52
  (both above the ₹50 sell threshold). Hedge scan finds: Buy 24,700 CE @
  ₹22, Buy 24,100 PE @ ₹19 (both above the ₹20 hedge threshold).
- **Entry timing — both triggers enabled (paper-trading setup)**:
  - Yesterday, each leg's own full-day VWAP was — sell CE ₹54, sell PE
    ₹50, hedge CE ₹21, hedge PE ₹18. Added together: **₹143** — yesterday's
    fixed reference number (used by Trigger A).
  - Today, at some candle close, the four legs' closes are ₹56 / ₹53 /
    ₹22.50 / ₹19.50 → combined close **₹151**. Today's own running VWAP so
    far (each leg's live VWAP, summed) is **₹148** (used by Trigger B).
  - Combined close (₹151) ≥ yesterday's VWAP (₹143) → **Trigger A fires.**
    Combined close (₹151) ≥ today's own running VWAP (₹148) → **Trigger B
    also fires.** Either one alone would have been enough; here both
    agree, and the trade enters — all four legs together, at their live
    prices at that moment.
- **Actual position economics (net, from the real fills)**: suppose the
  four legs actually fill at ₹56 / ₹53 / ₹22.50 / ₹19.50 — sell credit
  ₹109, hedge cost ₹42, net credit received = **₹67**. This is the number
  the target and stop-loss track from here, not the ₹143/₹148/₹151
  combined figures used only to time the entry.
- From there: if the net position gains, say, 25 points (net value moves
  to ₹42, since this is a net credit position — profit as the structure
  cheapens), the target closes it. If it loses 15 points instead (net value
  rises to ₹82), the stop-loss closes it. Both threshold numbers are set
  per deployment, not hardcoded to this example.

### What this strategy deliberately does NOT do

- No rolling a losing leg to a better strike (unlike Sell Straddle).
- No indicator-based exits (no RSI, ADX, etc.) — the only reference values
  used anywhere are the two VWAP-based entry triggers (previous-day and/or
  today's own intraday, per which are enabled).
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
- **Combined close** (2026-08-10 correction: the live/moving side of both
  triggers is the four legs' own close prices summed together, evaluated
  per candle — not a VWAP on the live side). Needs a chosen candle
  timeframe (e.g. 1-minute, matching Sell Straddle's typical granularity)
  to define what "close" means moment to moment; not specified by the
  user yet, defaulting to 1-minute unless told otherwise — flagged in the
  Open Question below.
- **Trigger A — yesterday's combined VWAP**: today's strikes are selected
  first (via the LTP-threshold scan, using live/current prices), then
  *those specific four contracts'* own historical intraday data from the
  previous trading day is fetched, each leg's own VWAP is calculated
  separately, and the four VWAPs are **added together** (summed, never
  netted/subtracted) to produce one fixed reference number for yesterday.
  Working assumption (needs confirmation, see Open Question below): this
  mirrors how "previous day's high/low" is normally used as a reference —
  the contract's own prior session, not a separately re-run selection
  process for "yesterday."
- **Trigger B — today's own combined intraday VWAP**: each of the same
  four legs' own running VWAP, live, summed together into one running
  figure that updates continuously through today's session. This is a
  genuinely new piece of tracking, not a reuse of Sell Straddle's existing
  per-strike VWAP (`PoolIndicatorEngine` tracks VWAP per individual strike
  already, which is the closest existing pattern to build the per-leg
  piece from — but the four-way sum-and-compare logic on top of it is
  new).
- **Both triggers need independent enable/disable flags** in the
  deployment config, with both defaulting ON for paper trading per the
  user's explicit instruction (compare real-world behavior of both before
  deciding which to run live).
- **Separately**, the actual net credit/debit from the real entry fills
  (sell proceeds minus hedge cost) is what the points-based target/stop-
  loss track in step 4 — a different, simpler "combined" number than the
  summed entry-timing figures above.
- The 4-leg atomic entry/exit sequencing and the points-based net-position
  SL/target tracking — no existing strategy in this codebase manages a
  4-leg structure as a single unit; Sell Straddle manages 2 legs
  independently (each can roll on its own), which does not apply here.
- No rollover engine is needed — this is simpler than Sell Straddle in that
  specific respect, not a reuse of Sell Straddle's rolling logic at all
  (an earlier draft of this design assumed rollover reuse; that was
  corrected during design review — there is no rollover concept in this
  strategy).

**Open questions carried into implementation planning:**
1. Confirm the previous-day VWAP interpretation above (contracts selected
   by today's live prices, VWAP pulled from their own price history
   yesterday) is correct before implementation.
2. What candle timeframe should "combined close" use for both triggers
   (1-minute default assumed, not yet confirmed)?
