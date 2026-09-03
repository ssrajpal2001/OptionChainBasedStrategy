# New Spot Data Format / Naming / Path Requirements

**Date:** 2026-07-17  
**Purpose:** Append updated 1-minute spot parquet data so we can run a broader multi-month NIFTY and FNO-stock trap-scanner validation sweep.

---

## 1. Target Folder

All spot parquet files must be placed in:

```
E:/AlgoSoft/OptionChainBasedStrategy/data/nse_option_cache/
```

(Resolved at runtime from `ROOT/data/nse_option_cache/` where `ROOT` is the repository root.)

---

## 2. File Naming Convention

Each file must be named exactly as:

```
spot_{SYMBOL}_1m_{START_DATE}_{END_DATE}.parquet
```

Where:
- `{SYMBOL}` — uppercase trading symbol as it appears in `data/fno_stocks.csv` or `NIFTY` for the index.
- `{START_DATE}` — first trading date in the file, format `YYYY-MM-DD`.
- `{END_DATE}` — last trading date in the file, format `YYYY-MM-DD`.

### Examples

```
spot_NIFTY_1m_2026-05-25_2026-06-30.parquet
spot_NIFTY_1m_2026-06-29_2026-07-14.parquet
spot_RELIANCE_1m_2026-06-01_2026-07-17.parquet
spot_TATASTEEL_1m_2026-06-01_2026-07-17.parquet
```

### Notes
- Multiple files for the same symbol are allowed and will be merged chronologically; duplicates are dropped by `datetime`.
- Gaps between files are fine as long as the desired backtest range is covered.
- For the 500-period VWAP to be fully primed from day 1 of the backtest, include at least **25 trading days** of history before the backtest start date (e.g., start data from mid-April for a June 1 backtest).

---

## 3. Required Columns

Each parquet file must contain at least these columns:

| Column | Dtype | Description |
|--------|-------|-------------|
| `datetime` | `datetime64[ns]` or timezone-aware `datetime64[ns, tz]` | Bar close timestamp in **Asia/Kolkata** (IST). Must be 1-minute granularity. |
| `open` | `float64` | Opening price of the 1-minute bar. |
| `high` | `float64` | Highest price of the 1-minute bar. |
| `low` | `float64` | Lowest price of the 1-minute bar. |
| `close` | `float64` | Closing price of the 1-minute bar. |
| `volume` | `int64` or `float64` | Volume traded in the 1-minute bar. Used for VWAP; if missing or zero, volume is defaulted to 1. |

### Optional columns
- Any extra columns are ignored by the backtest loaders.

---

## 4. Timezone & Hours

- Timestamps must be **Asia/Kolkata** local time.
- Only bars between **09:15 and 15:30 IST** are used by the backtests. Pre-market or post-market bars are filtered out automatically, but it is cleaner to exclude them at source.
- If `datetime` is timezone-naive, the loader assumes it is already IST and localizes it.
- If `datetime` is UTC or another timezone, the loader converts it to Asia/Kolkata.

---

## 5. Frequency & Gaps

- **1-minute bars only.** Higher-timeframe bars (5m, 15m, 75m) are resampled from these 1m bars by the backtest scripts.
- Bars do **not** need to be perfectly contiguous; missing bars are handled as empty resampling buckets.
- Each trading day should have roughly **375 bars** (09:15 to 15:30 inclusive, 1-minute closes).

---

## 6. Symbols Currently Used

### NIFTY (index)
Existing cache covers approximately **2026-05-25 to 2026-07-03**. Need extension to at least **2026-07-14** (current file name claims 2026-07-14 but data ends 2026-07-03) and preferably through **2026-08-31** for multi-month validation.

### FNO Stocks (25 liquid names)
The 2-TF backtest currently uses these symbols, all of which already have cache files starting 2026-06-01:

```
ASIANPAINT, AXISBANK, BAJFINANCE, BHARTIARTL, COALINDIA, HDFCBANK, HINDALCO,
ICICIBANK, INFY, JSWSTEEL, KOTAKBANK, LT, MARUTI, NESTLEIND, NTPC, ONGC,
POWERGRID, RELIANCE, SBIN, SUNPHARMA, TATASTEEL, TCS, TITAN, ULTRACEMCO, WIPRO
```

To extend the validation, append new files for each of these symbols following the naming convention above.

---

## 7. Backtest Scripts That Consume This Data

| Script | Symbols | Date range default | Notes |
|--------|---------|---------------------|-------|
| `scripts/nifty_cascade_v4_sweep.py` | NIFTY | `--start 2026-06-01 --end 2026-07-03` | Needs 500-period VWAP primed; include pre-June history. |
| `scripts/fno_2tf_trap_spot_backtest.py` | `--symbols ALL` or comma-separated | `--start 2026-06-01 --end 2026-07-14` | Uses lot sizes from `data/fno_stocks.csv`. |

---

## 8. Example: Appending New NIFTY Data

If you download NIFTY 1m spot for 2026-07-04 to 2026-08-31, save it as:

```
data/nse_option_cache/spot_NIFTY_1m_2026-07-04_2026-08-31.parquet
```

The next run of:

```bash
python scripts/nifty_cascade_v4_sweep.py --sweep --start 2026-06-01 --end 2026-08-31
```

will automatically pick up the new file and merge it with existing cache.

---

## 9. Validation Checklist

Before declaring new data ready, verify:

- [ ] File name matches `spot_{SYMBOL}_1m_{START_DATE}_{END_DATE}.parquet` exactly.
- [ ] File is in `data/nse_option_cache/`.
- [ ] Columns include `datetime`, `open`, `high`, `low`, `close`, `volume`.
- [ ] `datetime` values are in Asia/Kolkata local time.
- [ ] No duplicate `datetime` rows per symbol.
- [ ] First bar of each day is at or before 09:15 IST; last bar at or after 15:29 IST.
- [ ] For NIFTY: at least 25 trading days before the desired backtest start so 500-period VWAP is available.
- [ ] For stocks: at least 25 trading days before the desired backtest start, or accept that VWAP-based variants will be sparse until the lookback window is filled.
