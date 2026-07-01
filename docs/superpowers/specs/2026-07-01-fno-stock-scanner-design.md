# FnO Stock Trap Scanner — Phase 1 Design
**Date:** 2026-07-01  
**Status:** Approved  
**Phase:** 1 — Alert-only (no auto-trade)

---

## Goal

Scan all ~200 NSE FnO stocks every evening after market close. Identify stocks that have a Daily (D1) trap zone formed, are near that zone, and align with NIFTY's directional bias for the next session. Present a shortlist on the morning dashboard so the trader can manually decide which stock's option to buy.

---

## Stage Overview

| Stage | When | What |
|---|---|---|
| **Stage 1 — Nightly Scan** | After 3:30 PM daily | Scan all 200 FnO stocks → D1 zones → NIFTY alignment → save shortlist |
| **Stage 2 — Intraday Monitor** | 9:15 AM onwards (future) | Live HTF→MTF→LTF cascade on shortlisted stocks only |

**Phase 1 covers Stage 1 only.** Stage 2 is deferred.

---

## Stage 1 — Nightly Scanner Detail

### Script
`scripts/fno_stock_scanner.py` — standalone, runs after market close.

### Step 1: NIFTY Direction Bias

Fetch NIFTY D1 bars (last 30 days) from Upstox historical REST API.  
Run `scanner.scan_htf()` on the daily bars to find TRAPPED zones.

**Bias rule:**
- NIFTY last close is within **0.8%** of a **bearish zone** high/low → bias = **BULLISH** (look for CE buys on stocks)
- NIFTY last close is within **0.8%** of a **bullish zone** high/low → bias = **BEARISH** (look for PE buys on stocks)
- No clear bias (no zone within 0.8%) → **skip scan entirely** — no alert day

The 0.8% proximity threshold is a config constant (adjustable).

NIFTY doesn't need to be inside the zone — approaching it is enough to set the bias.

### Step 2: Load FnO Stock List

Load the NSE FnO stock list from Upstox instruments file (or a bundled CSV in `data/fno_stocks.csv`).  
~200 stocks. Includes: symbol, Upstox instrument key, lot size, strike step.

### Step 3: Per-Stock Scan (parallel, 10 at a time)

For each stock:
1. Fetch D1 bars (last 30 days) from Upstox historical REST API
2. Run `scanner.scan_htf()` on the daily bars
3. Check if any zone is **TRAPPED** in the direction matching NIFTY bias:
   - Bias = BULLISH → need a **bearish** TRAPPED zone (bears got trapped → stock will squeeze up → buy CE)
   - Bias = BEARISH → need a **bullish** TRAPPED zone (bulls got trapped → stock will squeeze down → buy PE)
4. Check proximity: last close is **inside or within 1%** of the zone high/low
5. If passes → compute output fields (see below)

**Output fields per qualifying stock:**
| Field | Description |
|---|---|
| `symbol` | Stock ticker (RELIANCE, TCS, etc.) |
| `direction` | CE or PE |
| `zone_high` | D1 zone upper boundary (stock price) |
| `zone_low` | D1 zone lower boundary (stock price) |
| `last_close` | Stock's last closing price |
| `zone_distance_pct` | How far last_close is from zone edge (%) |
| `stock_sl` | SL level = zone_low − small buffer (CE) or zone_high + buffer (PE) |
| `suggested_strike` | ATM ± 1 step in the right direction |
| `zone_age_days` | How many days since zone first formed |
| `zone_tests` | How many times price has tested the zone |
| `nifty_bias` | "Near bearish zone" or "Near bullish zone" |

### Step 4: Sort and Save

Sort qualifying stocks by `zone_distance_pct` ascending (closest to zone = highest conviction = top).  
Save to `data/fno_scan_YYYY-MM-DD.json`.

---

## SL Logic

**SL is defined on the STOCK's spot price, not the option premium.**

- **CE trade:** SL fires when stock price drops **below D1 zone low** (− small buffer, default 0.2%)
- **PE trade:** SL fires when stock price rises **above D1 zone high** (+ small buffer, default 0.2%)

The zone boundary IS the SL. No option premium monitoring needed for SL in Phase 1 (alert-only; trader monitors manually).

---

## Dashboard — Morning Card

New **"Stocks" tab** in `monitor.html`. Loads once on open, refreshes on tab click.

**Card format per stock:**

```
RELIANCE          ▲ BUY CE          [NEAR ZONE]
────────────────────────────────────────────────────
D1 Zone    :  ₹1,280 – ₹1,310
Last Close :  ₹1,295   (inside zone, 2.3% from low)
Stock SL   :  ₹1,278   (zone low − 0.2% buffer)
Strike     :  1300 CE  (ATM − 1 step)
Zone Age   :  3 days   |   Tests: 2
NIFTY Bias :  Near bearish zone ✓
```

Cards sorted: closest-to-zone first (highest conviction at top).

---

## API Endpoints

Added to `dashboard_server.py`:

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/scanner/fno` | Returns today's scan JSON (or latest available) |
| `POST` | `/api/scanner/run` | Admin: triggers a manual re-scan immediately |

---

## Detection Mode

Trap detection for stocks uses **stock SPOT price bars** (not option premium bars).  
This maps to `htf_source = "spot"` equivalent in scanner logic:
- Zone is detected on the stock's daily price chart
- Option order is placed on the stock's CE/PE contract  
- SL is monitored against stock spot price (not option LTP)

This is the correct approach for stocks: option premiums for individual FnO stocks are often illiquid and gappy — unsuitable for reliable zone detection. Stock price bars are clean.

---

## Configuration Constants

All in `scripts/fno_stock_scanner.py` (top of file, easy to tune):

```python
NIFTY_BIAS_PROXIMITY_PCT = 0.8    # NIFTY must be within 0.8% of zone
STOCK_ZONE_PROXIMITY_PCT = 1.0    # Stock must be within 1% of zone
SL_BUFFER_PCT            = 0.2    # SL = zone boundary ± 0.2%
D1_LOOKBACK_DAYS         = 30     # Days of daily bars for zone scan
PARALLEL_WORKERS         = 10     # Concurrent stocks scanned at once
FNO_LIST_PATH            = "data/fno_stocks.csv"
SCAN_OUTPUT_DIR          = "data/"
```

---

## What Phase 1 Does NOT Include

- Live intraday monitoring (Stage 2) — deferred
- Auto-trade execution — deferred to Phase 2
- Option premium tracking — Phase 1 is alert-only; trader places orders manually
- IV rank / Greeks display — deferred
- Backtesting the stock scanner — separate work item

---

## Files Created / Modified

| File | Action |
|---|---|
| `scripts/fno_stock_scanner.py` | **New** — nightly scan script |
| `data/fno_stocks.csv` | **New** — FnO stock list with Upstox keys |
| `dashboard_server.py` | **Modified** — add 2 scanner endpoints |
| `ui_layer/templates/monitor.html` | **Modified** — add Stocks tab + card component |
