# Strategy Condition & Indicator Reference Guide

How each indicator is calculated, what it measures, and example rule conditions.

---

## Indicators Available in Rule Builder

### 1. VWAP — Volume Weighted Average Price

**What it is:**  
VWAP is the average price of the combined straddle premium (CE + PE) weighted by volume over the trading session.

**Calculation:**
```
VWAP = Σ(price × volume) / Σ(volume)

Combined LTP      = CE_ltp + PE_ltp
Combined VWAP     = CE_vwap + PE_vwap
```
Session resets each morning at 09:15 IST.

**What it tells you:**  
Whether the straddle is currently trading above or below the session average. A premium above VWAP means it is elevated — potential SL territory. Below VWAP means theta decay is working in your favour.

**Example rule:**
```
VWAP  CLOSE < VWAP   (Combined Close below Combined VWAP → premium decaying normally)
VWAP  CLOSE > VWAP   (Combined Close above VWAP → premium rising, potential stress)
```

---

### 2. Slope (VWAP Slope)

**What it is:**  
The rate of change of VWAP over a configurable look-back period.

**Calculation:**
```
VWAP_Slope = (VWAP_current - VWAP_N_bars_ago) / N_bars_ago

V-Slope Current (SLOPE_CURR) = slope of most recent bar
V-Slope Previous (SLOPE_PREV) = slope of the bar before that
```

**Period field:** Number of bars to look back (default 9).

**What it tells you:**  
- Slope > 0 and rising: premium accelerating upward — bearish for seller.
- Slope < 0 and falling: premium dropping — good for seller, theta working.
- Slope crossing zero from negative to positive: potential reversal signal.

**Example rule:**
```
Slope (period=9, TF=5)  SLOPE_CURR < 0   (VWAP slope negative → momentum in seller's favour)
Slope (period=9, TF=5)  SLOPE_CURR < SLOPE_PREV  (slope declining → deceleration in premium)
```

---

### 3. RSI — Relative Strength Index

**What it is:**  
RSI measures the speed and magnitude of recent price changes on a scale of 0–100. Used here on the combined straddle premium or the underlying index.

**Calculation:**
```
RS  = Average Gain over N periods / Average Loss over N periods
RSI = 100 - (100 / (1 + RS))
```

**Period field:** Look-back window, typically 14.

**What it tells you for options selling:**  
- RSI 40–60 on the index: market is range-bound → ideal for straddle selling.
- RSI > 70: market overbought → directional move likely, avoid entry.
- RSI < 30: market oversold → directional bounce likely, avoid entry.

**Example rules:**
```
RSI (period=14, TF=5)  Combined RSI > 40    (index not oversold)
RSI (period=14, TF=5)  Combined RSI < 60    (index not overbought)
```
Combined in sequence with AND these two rules create a 40–60 band.

---

### 4. ROC — Rate of Change

**What it is:**  
Percentage change in price over the last N bars.

**Calculation:**
```
ROC = ((Close_current - Close_N_periods_ago) / Close_N_periods_ago) × 100
```

**Period field:** Number of bars, typically 9–14.

**What it tells you:**  
ROC measures momentum. A large positive ROC on the combined premium means premium surging — SL risk. A large negative ROC means premium collapsing fast — could be profit-target approach.

**Example rule:**
```
ROC (period=9, TF=5)  Combined ROC < 2    (premium not surging more than 2% per bar)
ROC (period=9, TF=5)  Combined ROC > -5  (premium not collapsing — avoid chasing exit)
```

---

### 5. Advanced — Custom Cross-Indicator Conditions

The **Advanced** indicator lets you write free-form comparisons between any two values.

**Available Operands:**

| Operand | Description |
|---|---|
| `Combined LTP` | CE_ltp + PE_ltp — current live combined premium |
| `Combined Close` | CE_close + PE_close — last confirmed bar close |
| `Combined VWAP` | CE_vwap + PE_vwap — session VWAP of combined premium |
| `Combined RSI` | RSI of combined premium (uses Period field for look-back) |
| `Combined ROC` | ROC of combined premium (uses Period field for look-back) |
| `V-Slope (curr)` | VWAP slope of the current bar |
| `V-Slope Current` | Explicit alias for current bar slope |
| `V-Slope Previous` | VWAP slope of the previous bar |
| `Fixed Value` | A constant number you type in |

**How to build a condition:**

```
[Operand 1]  [Operator: > < >= <= =]  [Operand 2 or Fixed Value]
```

**Example: premium below VWAP and slope negative**
```
Advanced  CLOSE < VWAP       (combined close is below combined VWAP)
Advanced  SLOPE_CURR < VALUE  0   (current slope is negative)
```

**Example: combined LTP has not doubled from open**
```
Advanced  LTP < VALUE  300   (combined live premium below ₹300 — reject if premium too high)
```

---

## Rule Builder Fields Reference

| Field | Description | Typical Values |
|---|---|---|
| `AND / OR` | Logic connector to previous rule | AND = all must match; OR = any one |
| `( )` | Grouping brackets for complex logic | e.g. `(A AND B) OR C` |
| `Indicator` | Which signal to evaluate | VWAP, Slope, RSI, ROC, Advanced |
| `Period` | Look-back bars for RSI / ROC / Slope | 9 for ROC/Slope, 14 for RSI |
| `TF (min)` | Timeframe of candle data (minutes) | 1, 3, 5, 15 |
| `Operator` | Comparison symbol | `>`, `<`, `>=`, `<=`, `=` |
| `Operand 2` | Right-hand side of comparison | Any operand or `Fixed Value` |

---

## Exit & Risk Controls Reference

### Profit Target (% of Premium)
Exit when: `unrealized P&L ≥ net_credit × profit_pct / 100`

Example: net credit ₹200, profit_pct = 30 → exit at ₹60 profit.

### Capital-Based Profit Target (Guardrails → Capital/Margin ₹)
When `capital_deployed_inr > 0`, profit target is calculated as:
```
Profit Target = capital_deployed_inr × profit_target_pct / 100
```
Per-day override: set a day's `Profit Target %` in the **PER DAY** tab.  
Global fallback: uses the global `profit_pct` value when per-day is 0.

Example: capital = ₹5,00,000, global profit_pct = 2 → target = ₹10,000 regardless of premium received.

### Stop Loss (% of Premium)
Exit when: `unrealized loss ≥ net_credit × sl_pct / 100`

Example: net credit ₹200, sl_pct = 200 → hard stop when combined premium reaches ₹600 (you've lost ₹400).

### Trailing SL
Activates after `trail_lock_pct`% profit is captured. Once active, exit if profit drops more than `trail_floor_pct`% below the peak profit.

Example: lock at 20%, floor 10%. Net credit ₹200.
- Trail activates when profit ≥ ₹40 (20%).
- If profit peaks at ₹80, trail floor = ₹80 - ₹20 = ₹60.
- Exit if profit drops below ₹60.

### Guardrail ROC
Exit when the rate of change of the combined premium exceeds the configured bound within a single candle.  
Fields: `TF` (candle size in min), `Length` (look-back bars), `Target` (exit at +N pts ROC), `Stoploss` (exit at -N pts ROC).

### Guardrail P&L (Session)
Exit entire session when the cumulative session P&L (in points) hits target or stoploss.  
Fields: `Target pts` (positive number), `Stoploss pts` (negative number, e.g. -60).

### Ratio Exit
Exit when one short leg LTP is `threshold × ` the other:
```
max(CE_ltp, PE_ltp) / min(CE_ltp, PE_ltp) >= threshold
```
Typically set to 3.0 — means one side has tripled relative to the other, indicating strong directional move.

### LTP Decay Exit
Exit when either leg's live price drops below `ltp_exit_min` points. Useful when a leg becomes near-worthless and there is no more premium to decay — avoids holding a position with tail risk for minimal remaining theta.

### VWAP Rise SL
Exit when the combined VWAP rises `threshold`% above the session-low VWAP:
```
(current_combined_VWAP - min_combined_VWAP_today) / min_combined_VWAP_today × 100 >= threshold
```
Catches sustained premium expansion.

---

## SL Cooldown by TF

After a stop loss closes a trade, the strategy waits before allowing re-entry:

```
Cooldown = max(TF across all active entry rules) × sl_cooldown_tf_multiplier
```

Example: entry rules use TF=5 min and TF=15 min. Multiplier=1.  
→ Cooldown = 15 × 1 = 15 minutes.

Set multiplier to 0 to disable the cooldown entirely.

---

## Per-Day Overrides (PER DAY tab)

Each weekday can override:

| Field | Description |
|---|---|
| `Profit Target %` | Overrides profit_pct for capital-based target on this day only (0 = use global) |
| `Trade Target (pts)` | Points-based profit target for a single trade on this day |
| `Trade SL (pts)` | Points-based stop loss for a single trade on this day |
| `Session Target (pts)` | Day-specific guardrail_pnl target override |
| `Session SL (pts)` | Day-specific guardrail_pnl stoploss override |

Leave all at 0 to inherit global settings.

---

## Iron Condor Reference

The Iron Condor is **entry time-gated only** — no RSI or ADX filter.

**Structure:**
```
SELL CE at ATM + short_leg_otm_pts
BUY  CE at ATM + long_leg_otm_pts
SELL PE at ATM - short_leg_otm_pts
BUY  PE at ATM - long_leg_otm_pts
```

Both OTM distances are measured from ATM independently.

| Index | Short OTM | Long OTM |
|---|---|---|
| NIFTY | ±200 pts | ±300 pts |
| BANKNIFTY | ±400 pts | ±600 pts |
| FINNIFTY | ±200 pts | ±300 pts |
| SENSEX | ±500 pts | ±750 pts |
| MIDCPNIFTY | ±150 pts | ±250 pts |

**Exit triggers:**
1. **Profit target (₹):** Exit all 4 legs when total P&L ≥ `profit_target_inr`.
2. **Stop loss (₹):** Exit all 4 legs when total loss ≥ `stoploss_inr`.
3. **Ratio breach:** Roll one side when `short_call_ltp / short_put_ltp >= ratio_exit_threshold` (or inverse). Up to `max_adjustments_per_side` rolls, each rolling by `roll_step_pts`.
4. **Time exit:** Force close all legs at `squareoff_time` IST.
