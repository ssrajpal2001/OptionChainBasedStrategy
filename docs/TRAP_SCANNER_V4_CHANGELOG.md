# Trap Scanner V4 Changelog

**Date:** 2026-07-17  
**Scope:** Backtest single source of truth (`scripts/nifty_cascade_v4_sweep.py`) vs. live engine/helpers (`strategies/trap_scanner/`)

This document lists the structural differences between the V4 backtest script and the remaining live trap-scanner engine so the live implementation can be aligned with the verified mathematical single source of truth.

---

## 1. Timeframe Cascade

| Aspect | V4 Backtest (single source of truth) | Live Engine (`strategies/trap_scanner/`)
|--------|----------------------------------------|------------------------------------------|
| HTF | 75-minute spot bars | Configurable (`self._htf_min`, often 180m or 75m depending on underlying) |
| MTF | 15-minute spot bars (mandatory filter) | 15-minute spot/option bars used in some paths |
| LTF | 5-minute spot bars | 5-minute option bars or spot bars depending on `htf_source` |
| Execution | 1-minute spot bars | Live tick stream (option/futures LTP) |
| Data source | Spot-only for backtest | Spot, futures, or option-mode depending on `htf_source` (`option`/`futures`/`spot`) |

**Action required:** Live engine should support a pure spot-mode 75m→15m→5m→1m cascade matching V4 for NIFTY validation, separate from option/futures paths.

---

## 2. Entry Trigger

| Aspect | V4 Backtest | Live Engine
|--------|-------------|-------------|
| Trigger price | 1/3 retracement of the LTF trap zone: <br>Long: `zone_low + (zone_high - zone_low)/3` <br>Short: `zone_high - (zone_high - zone_low)/3` | Entry reference is `zone_high` for bear-trap zones (`entry.get("zone_high", entry.get("zone_trigger", 0))`). This is effectively the full extreme, not the 1/3 retracement. |
| Execution | Wait for 1m price to cross trigger | Market order placed immediately when zone is triggered; fallback to ATM/dynamic strike if scan strike not liquid |

**Action required:** Add a configurable entry mode to live engine: `zone_trigger` (1/3) vs. `zone_high` (full breach). V4 backtest shows the 1/3 entry is materially more profitable.

---

## 3. Stop Loss & Target

| Aspect | V4 Backtest | Live Engine
|--------|-------------|-------------|
| SL | LTF zone extreme ± fixed buffer (10 points for NIFTY, stocks) | LTF zone extreme ± `self._sl_buf` (per-underlying config) |
| Target | HTF reference level (prev high for bear trap, prev low for bull trap) | T1 = MTF target, T2 = HTF target; partial exits at T1 (50%) and runner at T2 |
| Retest/void-lift | After entry, target becomes active only after price revisits the HTF entry level (`htf_entry_level`). Until then, only SL is watched. | No explicit pre-entry retest gate. Spot-mode does check `t1_price` and `t2_price` directly after entry. |

**Action required:** Decide whether to adopt the V4 post-entry void-lift policy in live engine. V4 backtest used it for NIFTY; the FNO 2-TF test without it was worse, suggesting it adds value.

---

## 4. Technical Filters

| Aspect | V4 Backtest | Live Engine
|--------|-------------|-------------|
| VWAP | 500-period rolling VWAP on LTF; directional filter (long above VWAP, short below) | Not present as a hard LTF filter in the live engine |
| ADX | 20-period ADX; require `ADX < max` (default 20) | Not present as a hard LTF filter |
| RSI | 14-period RSI; directional (long > threshold, short < threshold) | Not present as a hard LTF filter |
| RSI symmetry | V4 sweep uses `RSI_long > 40` and `RSI_short < 60` (relaxed) | N/A |

**Action required:** Port the ADX/RSI/VWAP filter block into the live engine as an optional gate, with the same relaxed thresholds that scored best in the NIFTY sweep (`ADX < 20`, `RSI_long > 40`, `RSI_short < 60`).

---

## 5. Position & Exit Management

| Aspect | V4 Backtest | Live Engine
|--------|-------------|-------------|
| Max positions | 1 per day | Multiple legs (CE1/CE2/PE1/PE2), scale-ins, probe orders |
| Partial exits | None — full exit at SL/target/EOD | T1 (50% at MTF target), T2 (runner at HTF target), trailing SL, profit floor |
| Trailing SL | None | 5m trap-based trail SL after T1; futures-mode TSL before T1 |
| Sweep re-entry | None | After plain SL, watches 2 candles for price recovery above SL → re-enter |
| EOD | Hard square-off at 15:30 IST | Configurable `cutoff_str`; `_eod_square_off` at cutoff |
| Slippage/fills | Assumes trigger price fill; no slippage | Real market fills, paper fallback on rejection |

**Action required:** For parity with V4 backtest, add a "simple mode" to live engine that disables scaling, T1/T2, trailing SL, and sweep re-entry, using only SL/target/EOD with a single position per day.

---

## 6. Instrument & Lot Handling

| Aspect | V4 Backtest | Live Engine
|--------|-------------|-------------|
| Underlying | NIFTY spot (or FNO stock spot in 2-TF test) | NIFTY, SENSEX, BANKNIFTY, FINNIFTY, CRUDEOIL, BTC, ETH, FNO stocks |
| Lot size | Fixed per underlying / read from `data/fno_stocks.csv` | Per-binding `lot_size × lot_multiplier` |
| Execution instrument | Spot P&L (points × lot size) | Option/futures/perp contracts; real premium P&L |

---

## 7. Verified V4 Parameters (from NIFTY sweep, 2026-06-01 to 2026-07-03)

| Parameter | Best value |
|-----------|------------|
| HTF | 75m |
| MTF | 15m |
| LTF | 5m |
| ADX max | 20.0 |
| RSI long min | 40.0 |
| RSI short max | 60.0 |
| VWAP filter | Test both On and Off; Off gave more trades, On gave higher PF |
| Entry mode | 1/3 retracement (`zone_trigger`) |
| SL buffer | 10 points (NIFTY) / 10 points (stocks) |

---

## 8. Recommendations for Live Alignment

1. **Add a pure spot-mode V4 path** in the live engine that mirrors the backtest exactly: 75m→15m→5m→1m on spot, no option/futures mixing.
2. **Use the 1/3 retracement entry** as the default, with full-breach as an optional toggle.
3. **Implement the ADX/RSI/VWAP filter block** with the relaxed thresholds above, configurable per underlying.
4. **Add the post-entry retest void-lift policy** as a configurable flag.
5. **Provide a simple mode** that disables scaling, trailing SL, profit floor, and sweep re-entry for clean A/B testing against the V4 backtest.
6. **Log the source of every live entry** (HTF zone UID, MTF zone, LTF zone, filter values) so trades can be reconciled with backtest output.

---

## 9. Files Referenced

- **V4 backtest:** `scripts/nifty_cascade_v4_sweep.py`
- **FNO 2-TF validation:** `scripts/fno_2tf_trap_spot_backtest.py`
- **Live engine:** `strategies/trap_scanner/engine.py`
- **Live entries:** `strategies/trap_scanner/entries.py`
- **Live exits:** `strategies/trap_scanner/exits.py`
- **Live zones:** `strategies/trap_scanner/zones.py`
- **Live scanner math:** `strategies/trap_scanner/scanner.py`
- **Universe/lots:** `data/fno_stocks.csv`
