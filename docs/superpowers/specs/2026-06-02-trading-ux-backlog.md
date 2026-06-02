# Trading UX / Execution Backlog (Design + Priorities)

**Date:** 2026-06-02
Captures the requirements raised during the live dry run. Each builds as its own
spec→plan→TDD piece. Ordered by value × safety.

---

## P1 — Lot size vs quantity (correct + separate)  ← build first
**Problem:** UI shows lot size **50** for NIFTY (should be **65**). It conflates the
exchange lot size with the number of lots.
**Fix:**
- Strategies derive the **per-lot exchange lot size** from `ExchangeConfig.lot_sizes`
  (NIFTY 65, BANKNIFTY 30, FINNIFTY 60, SENSEX, CRUDEOIL 100…), **not** a config
  default of 50.
- Position carries **`lot_size`** (per-lot, e.g. 65) and **`lots`** (= `lot_multiplier`,
  the *number of lots*) **separately**. Total contracts = `lot_size × lots`.
- Order qty sent to broker = `lot_size × lots`.
- Dashboard shows **Lot size**, **Qty (lots)**, and **Total contracts** distinctly;
  P&L = `(entry − ltp) × lot_size × lots`.
**Touch:** sell_straddle.py, iron_condor.py (lot size source + position fields),
straddle_bridge/ic_bridge (qty), dashboard positions render.

## P2 — Per-broker positions (already spec'd)
See `2026-06-02-per-broker-positions-design.md`. Each broker card shows only what
**it** filled (fixes "Zerodha shows a trade it didn't place").

## P3 — Executed / Rejected status + reason
**Problem:** No visibility whether an order **filled or was rejected** and why.
**Fix:**
- Broker `place_order` already returns an order id or raises; capture the broker's
  reject reason (e.g. "insufficient margin", "quantity freeze", "invalid symbol").
- Bridges publish an order-result event (status: FILLED/REJECTED + reason +
  client/binding/strategy/legs).
- Surface in the dashboard (a per-binding "last order" line) **and** the trade
  History (record rejects too, not just fills).
- Classify: margin/freeze = path OK; invalid symbol/exchange/product = integration bug.

## P4 — Margin: show used / available
**Problem:** Don't surface broker margin.
**Fix:** brokers already have `get_funds()` (available/used). Poll per binding
(throttled), expose via `/api/client/margin`, render on each broker card.

## P5 — Margin-gated / margin-sized execution  ← most complex, build carefully
**Problem:** Orders fire regardless of margin; want either (a) block if insufficient,
or (b) size lots to use ≤ 80% of available margin.
**Fix:**
- Estimate **required margin per lot** for the structure (sell straddle/strangle =
  SPAN+exposure per lot; IC = net of hedges). Prefer the broker's **margin
  calculator API** where available (Zerodha `/margins/basket`, etc.); fallback to a
  configurable per-lot estimate.
- Mode A (gate): if `required > available` → skip + log "insufficient margin".
- Mode B (size): `lots = floor(0.80 × available / required_per_lot)`; cap by config
  max; if `lots < 1` → skip.
- Per-binding (each broker has its own margin). Config flag selects A vs B.
**Risk:** changes order sizing with real money — needs careful tests + a dry-run gate.

## P6 — Square-off button (manual)
Per-binding (and/or per-position) **Square Off** → publishes an EXIT for that
strategy/underlying on that binding → bridge closes the legs. Endpoint
`/api/client/squareoff/{binding}` + button on the broker card.

## P7 — Pause / Resume button
Distinct from Trade-off/Kill: **Pause** stops *new entries* but **keeps** the open
position managed (exits still run); **Resume** re-enables entries. Per binding (or
global). A `paused` flag the strategy/bridge checks before entries (not exits).
Endpoint `/api/client/pause/{binding}` + button.

---

## Build order
1. **P1 lot size/qty** (concrete, bounded) — starting now.
2. **P6 Square-off** + **P7 Pause** (small UI+endpoint each, high operator value).
3. **P3 Executed/Rejected status** (visibility, ties into History).
4. **P2 per-broker positions** (architectural).
5. **P4 margin display** → **P5 margin-gated/sized execution** (most complex; do last, with a dry-run gate).

Each ships independently, tested, pushed.
