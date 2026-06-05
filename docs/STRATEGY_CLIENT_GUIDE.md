# Sell-Straddle — Client Guide

A plain-language explanation of how the sell-straddle strategy works, with the exact entry and
exit rules and a flowchart you can share with clients.

---

## What the strategy does (one sentence)

We **sell** an at-the-money Call and Put together (a "straddle"), collect the premium, and profit
when the market stays calm and those options lose value (**time decay**). We **adjust** (roll) one
leg when the market drifts, and **fully exit** on a strong adverse move, a profit/loss target, or
end of day.

> ⚙️ **Everything in the strategy is dynamic.** The entry conditions, the exit conditions, the
> indicators, the operators, the comparison values, **and the timeframe of each rule** are all
> chosen from dropdowns in the dashboard **before you start the terminal**. The specific rules,
> timeframes, and percentages shown in this guide are **one example configuration** — they are not
> hard-coded. Read every "1-min", "2-min", "< 0", "+20%", "4×", etc. below as *"whatever you
> configured for that rule."*

---

## Before anything — the gate

Trading only starts when **all** of these are true:

- **Terminal ON + Trade ON** for the client-broker
- Inside the **entry window** (NIFTY 09:15–15:00 IST)
- Not already **stopped for the day** (a day target/limit was hit)
- Not in a **cooldown** after a recent exit
- Live option prices are flowing

---

## ENTRY — when do we sell?

The strategy evaluates a set of **entry conditions you build from dropdowns**. Each condition picks:
an **indicator** (the combined premium's `CLOSE`, `VWAP`, `SLOPE`, `RSI`, or `ROC`), an **operator**
(`<`, `>`, …), a **value/operand**, and a **timeframe** (1m / 2m / 5m / …). You can use one condition
or several combined with AND. There are two independent rule sets — one for the **first trade of the
day** and one for **re-entries** — each with its own conditions and timeframes.

**Example configuration** (what you might pick — all of this is editable in the UI):

| Stage | Checked every | Example condition (configurable) | Pair chosen |
|-------|---------------|----------------------------------|-------------|
| **Beginning** (first trade of the day) | its max timeframe | e.g. `CLOSE < VWAP (1-min)` **AND** `SLOPE < 0 (2-min)` | balanced near-ATM Call + Put |
| **Re-entry** (every trade after) | its max timeframe | e.g. `CLOSE < VWAP (5-min)` **AND** `SLOPE < 0 (2-min)` | most-balanced pair from ATM ± offset strikes |

When all configured conditions are true → **SELL** the pair → position is open.

> **Timing:** each condition is read on **its own** configured timeframe, at that candle's close
> **+5 seconds** (so the broker's data for that candle has arrived). The whole set is evaluated
> together once per the **slowest** condition's timeframe — so a 1-min and a 5-min condition are
> only judged true at the same moment when the 5-min candle closes.

---

## MANAGING the position — exit ladder (top wins)

Checked continuously while a position is open. The first matching rule acts. **Each exit is
individually enabled/disabled and its thresholds, timeframes, and the rule-builder conditions are
all set from the dashboard** — the values below are an example, not fixed.

| # | Trigger (example values — configurable) | Plain meaning | Action |
|---|---------|---------------|--------|
| 1 | **3:15 PM** | end of day | **Close all** |
| 2 | **Day +20%** | day profit target | **Close + stop for the day** |
| 3 | **Day −30%** | day loss limit | **Close + stop for the day** |
| 4 | **A leg < ₹20** | one side decayed to near-zero | **Roll** that leg closer (collect fresh premium) |
| 5 | **One leg ≥ 4× the other** | position gone lopsided | **Roll** the cheap leg back toward balance |
| 6 | **Trailing stop (TSL)** | in profit, then profit pulls back | **Roll** to lock gains |
| 7 | **Premium rises +2% off its low** | starting to go against us | **Roll** the *winning* (less-burning) leg, keep the other |
| 8 | **Strong adverse move** — `CLOSE>VWAP AND SLOPE>0 AND RSI>55 AND ROC>10` | premium decisively rising against us | **Full exit** (both legs) |

- **Roll** = close one leg, re-sell a better strike → position stays alive, re-centered.
- **Full exit** = close both legs → after a short cooldown, the bot looks to re-enter.

> The percentages, ratios, timeframes, and thresholds above are the configured defaults and are
> set per deployment in the dashboard.

---

## Flowchart

```
        ┌──────────────────────────┐
        │ Terminal ON & Trade ON?  │──No──► wait
        └────────────┬─────────────┘
                    Yes
        ┌────────────▼─────────────┐
        │ Inside entry window?     │──No──► wait / (3:15 → close)
        └────────────┬─────────────┘
                    Yes
        ┌────────────▼─────────────┐
        │ Position already open?   │
        └─────┬───────────────┬────┘
             No               Yes
   ┌──────────▼─────┐   ┌──────▼──────────────────────────────┐
   │ ENTRY CHECK    │   │ EXIT LADDER (checked continuously)  │
   │ (2m beginning  │   │ 1 EOD?          → CLOSE ALL          │
   │  / 5m re-entry)│   │ 2 Day +20%?     → CLOSE + STOP       │
   │ CLOSE<VWAP AND │   │ 3 Day −30%?     → CLOSE + STOP       │
   │ SLOPE<0 ?      │   │ 4 Leg < ₹20?    → ROLL that leg      │
   │   ├─Yes→ SELL  │   │ 5 Ratio ≥ 4×?   → ROLL cheap leg     │
   │   │     pair → ─┼──►│ 6 TSL hit?      → ROLL               │
   │   └─No → wait  │   │ 7 VWAP +2%?     → ROLL winning leg   │
   └────────────────┘   │ 8 All-4 signal? → FULL EXIT          │
                        │ else            → HOLD               │
                        └──────────┬──────────────────────────┘
                          full exit → cooldown → back to ENTRY
```

---

## Worked example

1. **09:34** — combined premium 271 dips below its average and is falling → **SELL** CE 23550 +
   PE 23450 (collect 271).
2. Market stays calm → premium decays → profit grows.
3. If the day's profit hits **+20%** → **book it and stop** for the day.
4. If the market jumps and premium climbs **+2%** off its low → **roll the winning side** to
   re-balance (the losing leg is kept).
5. If it keeps rising hard (all 4 exit signals) → **full exit**.
6. **3:15 PM** → square off whatever is still open.

---

## Notes for the operator

- **Trade OFF** (or **Terminal OFF**) now **squares off that client-broker's open legs only** — other
  clients on the same strategy keep running.
- Each client-broker has its **own** trade log at `logs/trades/{client}-{binding}-{date}.log`
  (entries, exits, order placements, rejections, square-offs).
- For real live trading, set the instrument **lot size** correctly (NIFTY = 75) or orders are
  rejected on quantity, and ensure the broker account has margin.
