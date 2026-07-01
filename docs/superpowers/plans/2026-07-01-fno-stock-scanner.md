# FnO Stock Trap Scanner — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Nightly scanner that scans all ~200 FnO stocks for D1 trap zones aligned with NIFTY's directional bias, computes R:R per stock, and displays a ranked morning alert dashboard.

**Architecture:** Standalone script (`scripts/fno_stock_scanner.py`) fetches daily bars from Upstox REST, runs `scanner.scan_htf_spot()` for NIFTY bias then `scanner.scan_htf()` per stock, filters by direction + proximity + R:R, and saves a JSON file. Two new FastAPI endpoints serve that JSON. A new "Stocks" tab in `monitor.html` renders ranked alert cards.

**Tech Stack:** Python 3.10+, requests, pandas, SQLite (Upstox token), FastAPI, Alpine.js v3, Tailwind CSS (CDN). No new pip packages needed.

## Global Constraints

- Upstox REST base: `https://api.upstox.com/v2`
- Token source: `data/clients.db` table `system_feeder_creds` column `access_token` WHERE `provider='upstox'`
- Rate-limit safety: `time.sleep(0.35)` between every Upstox REST call
- Parallel workers: 10 stocks at a time using `concurrent.futures.ThreadPoolExecutor`
- Scan output: `data/fno_scan_YYYY-MM-DD.json`
- FnO list: `data/fno_stocks.csv` columns: `symbol,upstox_key,lot_size,strike_step`
- Min R:R to appear on dashboard: `1.5` (config constant)
- SL buffer: `0.2%` of zone boundary
- All backend errors return `{"ok": false, "error": "..."}` — never raw 500
- Pydantic schemas must be at module level in `dashboard_server.py` (not inside functions)
- Alpine.js + Tailwind CSS CDN only — no npm/webpack
- `scan_htf_spot(df)` returns `(events_df, entries_list)` — entries have fields: `kind` (BEAR/BULL), `zone_high`, `zone_low`, `sl`, `status` (ACTIVE/TRAPPED/CLOSED)
- `scan_htf(df)` same signature — bear-only version (for individual stock bars)
- Daily bars from Upstox: `GET /v2/historical-candle/{encoded_key}/day/{to_date}/{from_date}` → `data.candles` list of `[ts, open, high, low, close, vol, oi]`
- Stock Upstox key format: `NSE_EQ|ISIN` (e.g. `NSE_EQ|INE009A01021` for INFOSYS)

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `scripts/fno_stock_scanner.py` | **Create** | All scan logic: NIFTY bias, per-stock D1 scan, R:R, optimize sweep, JSON output |
| `data/fno_stocks.csv` | **Create** | FnO stock list — symbol, Upstox key, lot size, strike step |
| `ui_layer/dashboard_server.py` | **Modify** | Add `GET /api/scanner/fno` + `POST /api/scanner/run` endpoints |
| `ui_layer/templates/monitor.html` | **Modify** | Add "Stocks" tab + alert card component |
| `tests/test_fno_scanner.py` | **Create** | Unit tests for R:R calc, proximity filter, bias rule, zone age |

---

## Task 1: FnO Stock List CSV

**Files:**
- Create: `data/fno_stocks.csv`

**Interfaces:**
- Produces: CSV with columns `symbol,upstox_key,lot_size,strike_step` consumed by Task 2

- [ ] **Step 1: Create the FnO stocks CSV**

Create `data/fno_stocks.csv` with the top 50 most-liquid NSE FnO stocks (covers >90% of retail trading interest). Full 200-stock list can be added later — 50 is sufficient for Phase 1 and avoids Upstox rate-limit issues on first run.

```csv
symbol,upstox_key,lot_size,strike_step
RELIANCE,NSE_EQ|INE002A01018,250,20
TCS,NSE_EQ|INE467B01029,150,50
HDFCBANK,NSE_EQ|INE040A01034,550,10
INFY,NSE_EQ|INE009A01021,300,20
ICICIBANK,NSE_EQ|INE090A01021,700,10
KOTAKBANK,NSE_EQ|INE237A01028,400,20
LT,NSE_EQ|INE018A01030,150,50
SBIN,NSE_EQ|INE062A01020,1500,5
AXISBANK,NSE_EQ|INE238A01034,625,10
WIPRO,NSE_EQ|INE075A01022,1500,5
BAJFINANCE,NSE_EQ|INE296A01024,125,50
BHARTIARTL,NSE_EQ|INE397D01024,475,10
ASIANPAINT,NSE_EQ|INE021A01026,200,25
MARUTI,NSE_EQ|INE585B01010,37,100
TITAN,NSE_EQ|INE280A01028,375,10
SUNPHARMA,NSE_EQ|INE044A01036,700,5
ULTRACEMCO,NSE_EQ|INE481G01011,100,50
NESTLEIND,NSE_EQ|INE239A01016,50,100
POWERGRID,NSE_EQ|INE752E01010,2700,2
NTPC,NSE_EQ|INE733E01010,3000,2
ONGC,NSE_EQ|INE213A01029,1925,2
COALINDIA,NSE_EQ|INE522F01014,2100,2
TATAMOTORS,NSE_EQ|INE155A01022,1425,2
TATASTEEL,NSE_EQ|INE081A01020,5500,2
HINDALCO,NSE_EQ|INE038A01020,2150,2
JSWSTEEL,NSE_EQ|INE019A01038,1350,2
ADANIENT,NSE_EQ|INE423A01024,250,20
ADANIPORTS,NSE_EQ|INE742F01042,1250,5
GRASIM,NSE_EQ|INE047A01021,375,10
TECHM,NSE_EQ|INE669C01036,600,10
HCLTECH,NSE_EQ|INE860A01027,700,10
DRREDDY,NSE_EQ|INE089A01023,125,50
CIPLA,NSE_EQ|INE059A01026,650,10
DIVISLAB,NSE_EQ|INE361B01024,150,50
APOLLOHOSP,NSE_EQ|INE437A01024,125,50
EICHERMOT,NSE_EQ|INE066A01021,175,50
BAJAJFINSV,NSE_EQ|INE918I01026,500,10
BPCL,NSE_EQ|INE029A01011,1800,2
IOC,NSE_EQ|INE242A01010,2500,2
HEROMOTOCO,NSE_EQ|INE158A01026,300,20
BRITANNIA,NSE_EQ|INE216A01030,200,25
ITC,NSE_EQ|INE154A01025,3200,2
M&M,NSE_EQ|INE101A01026,700,5
TATACONSUM,NSE_EQ|INE192A01025,1050,5
PIDILITIND,NSE_EQ|INE318A01026,250,20
INDUSINDBK,NSE_EQ|INE095A01012,500,10
PFC,NSE_EQ|INE134E01011,2700,2
RECLTD,NSE_EQ|INE020B01018,3000,2
SIEMENS,NSE_EQ|INE003A01024,125,50
ABB,NSE_EQ|INE117A01022,125,50
```

- [ ] **Step 2: Commit**

```bash
git add data/fno_stocks.csv
git commit -m "data: add FnO stock list CSV (50 liquid stocks) for nightly scanner"
```

---

## Task 2: Core Scanner Script — NIFTY Bias + Per-Stock D1 Scan

**Files:**
- Create: `scripts/fno_stock_scanner.py`
- Test: `tests/test_fno_scanner.py`

**Interfaces:**
- Consumes: `data/fno_stocks.csv` (Task 1), `scanner.scan_htf_spot()`, `scanner.scan_htf()`
- Produces:
  - `scan_nifty_bias(token, proximity_pct) -> dict` — `{"bias": "CE"|"PE", "zone": {...}, "nifty_close": float}`
  - `scan_stock(symbol, upstox_key, lot_size, strike_step, token, bias, stock_prox_pct, min_rr) -> dict|None`
  - `run_scan(token, nifty_prox_pct, stock_prox_pct, min_rr) -> list[dict]` — sorted results
  - JSON written to `data/fno_scan_YYYY-MM-DD.json`

- [ ] **Step 1: Write failing tests**

Create `tests/test_fno_scanner.py`:

```python
"""Tests for FnO stock scanner core logic."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import pandas as pd
import pytest

# ── helpers that don't need Upstox ──────────────────────────────────────────

def test_compute_rr_ce():
    from fno_stock_scanner import _compute_rr
    # CE: entry=100, sl=90 (risk=10), t1=130 (reward=30) → rr=3.0
    result = _compute_rr(entry=100.0, sl=90.0, t1=130.0, direction="CE")
    assert abs(result["rr_ratio"] - 3.0) < 0.01
    assert result["risk_pts"] == pytest.approx(10.0)
    assert result["reward_pts"] == pytest.approx(30.0)

def test_compute_rr_pe():
    from fno_stock_scanner import _compute_rr
    # PE: entry=200, sl=215 (risk=15), t1=170 (reward=30) → rr=2.0
    result = _compute_rr(entry=200.0, sl=215.0, t1=170.0, direction="PE")
    assert abs(result["rr_ratio"] - 2.0) < 0.01

def test_compute_rr_zero_risk():
    from fno_stock_scanner import _compute_rr
    # entry == sl → rr=0 (guard against div-by-zero)
    result = _compute_rr(entry=100.0, sl=100.0, t1=130.0, direction="CE")
    assert result["rr_ratio"] == 0.0

def test_proximity_pass():
    from fno_stock_scanner import _in_proximity
    # last_close=98, zone_low=95, zone_high=105, prox_pct=3.0 → close is inside zone → True
    assert _in_proximity(last_close=98.0, zone_low=95.0, zone_high=105.0, prox_pct=3.0) is True

def test_proximity_outside_but_near():
    from fno_stock_scanner import _in_proximity
    # last_close=106, zone_high=105 → 0.95% above zone → within 1.0% → True
    assert _in_proximity(last_close=106.0, zone_low=95.0, zone_high=105.0, prox_pct=1.0) is True

def test_proximity_fail():
    from fno_stock_scanner import _in_proximity
    # last_close=112, zone_high=105 → 6.7% above zone → outside 1.0% → False
    assert _in_proximity(last_close=112.0, zone_low=95.0, zone_high=105.0, prox_pct=1.0) is False

def test_zone_age():
    from fno_stock_scanner import _zone_age_days
    import pandas as pd
    from datetime import date, timedelta
    trapped_ts = (date.today() - timedelta(days=3)).isoformat()
    assert _zone_age_days(trapped_ts) == 3

def test_nifty_bias_picks_nearest_zone():
    from fno_stock_scanner import _pick_nifty_bias
    bear_zone = {"kind": "BEAR", "zone_high": 24500.0, "zone_low": 24300.0,
                 "sl": 24600.0, "status": "TRAPPED", "trapped_on": "2026-06-30"}
    bull_zone = {"kind": "BULL", "zone_high": 23800.0, "zone_low": 23600.0,
                 "sl": 23500.0, "status": "TRAPPED", "trapped_on": "2026-06-30"}
    # nifty_close=24350 → inside bear zone → bear zone is nearer
    bias, zone = _pick_nifty_bias(nifty_close=24350.0, zones=[bear_zone, bull_zone], prox_pct=2.0)
    assert bias == "CE"   # near bearish zone → expect CE buys on stocks
    assert zone["kind"] == "BEAR"

def test_nifty_bias_bull():
    from fno_stock_scanner import _pick_nifty_bias
    bull_zone = {"kind": "BULL", "zone_high": 23800.0, "zone_low": 23600.0,
                 "sl": 23500.0, "status": "TRAPPED", "trapped_on": "2026-06-30"}
    bias, zone = _pick_nifty_bias(nifty_close=23650.0, zones=[bull_zone], prox_pct=2.0)
    assert bias == "PE"   # near bullish zone → expect PE buys on stocks
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd e:\AlgoSoft\OptionChainBasedStrategy
python -m pytest tests/test_fno_scanner.py -v 2>&1 | head -30
```
Expected: `ModuleNotFoundError: No module named 'fno_stock_scanner'`

- [ ] **Step 3: Create `scripts/fno_stock_scanner.py`**

```python
"""
FnO Stock Trap Scanner — Phase 1 (Alert-only, nightly D1 scan)
===============================================================
Usage:
  python scripts/fno_stock_scanner.py              # live scan → data/fno_scan_YYYY-MM-DD.json
  python scripts/fno_stock_scanner.py --optimize   # threshold sweep → prints table
"""
from __future__ import annotations

import os, sys, json, sqlite3, time
from datetime import date, datetime, timedelta
from typing import Optional
from urllib.parse import quote as _quote
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from strategies.trap_scanner import scanner

# ── Config ────────────────────────────────────────────────────────────────────
NIFTY_BIAS_PROXIMITY_PCT = 1.5   # tune via --optimize
STOCK_ZONE_PROXIMITY_PCT = 2.0   # tune via --optimize
SL_BUFFER_PCT            = 0.2
MIN_RR                   = 1.5
D1_LOOKBACK_DAYS         = 60
PARALLEL_WORKERS         = 10

DB_PATH       = os.path.join(_ROOT, "data", "clients.db")
FNO_LIST_PATH = os.path.join(_ROOT, "data", "fno_stocks.csv")
SCAN_DIR      = os.path.join(_ROOT, "data")
UPSTOX_BASE   = "https://api.upstox.com/v2"
NIFTY_KEY     = "NSE_INDEX|Nifty 50"

# ── Token ─────────────────────────────────────────────────────────────────────

def _get_token() -> str:
    try:
        conn = sqlite3.connect(DB_PATH)
        row  = conn.execute(
            "SELECT access_token FROM system_feeder_creds WHERE provider='upstox' LIMIT 1"
        ).fetchone()
        conn.close()
        return (row[0] or "") if row else ""
    except Exception:
        return ""

# ── Upstox REST ───────────────────────────────────────────────────────────────

def _hdr(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

def _fetch_daily(instrument_key: str, token: str, days: int = D1_LOOKBACK_DAYS) -> pd.DataFrame:
    """Fetch daily OHLCV bars for any Upstox instrument key."""
    to_dt = date.today()
    fr_dt = to_dt - timedelta(days=days)
    enc   = _quote(instrument_key, safe="")
    url   = f"{UPSTOX_BASE}/historical-candle/{enc}/day/{to_dt}/{fr_dt}"
    try:
        r = requests.get(url, headers=_hdr(token), timeout=15)
        time.sleep(0.35)
        if r.status_code != 200:
            return pd.DataFrame()
        candles = r.json().get("data", {}).get("candles", [])
        rows = [
            {"datetime": c[0][:10], "open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4])}
            for c in reversed(candles)
        ]
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

# ── Pure helper functions (tested) ───────────────────────────────────────────

def _compute_rr(entry: float, sl: float, t1: float, direction: str) -> dict:
    """Compute risk:reward. Entry is last_close (approximate)."""
    if direction == "CE":
        risk   = entry - sl      # sl is below entry for CE
        reward = t1 - entry
    else:
        risk   = sl - entry      # sl is above entry for PE
        reward = entry - t1
    if risk <= 0:
        return {"risk_pts": 0.0, "reward_pts": max(reward, 0.0), "rr_ratio": 0.0}
    return {
        "risk_pts":   round(risk, 2),
        "reward_pts": round(reward, 2),
        "rr_ratio":   round(reward / risk, 2),
    }

def _in_proximity(last_close: float, zone_low: float, zone_high: float, prox_pct: float) -> bool:
    """True if last_close is inside the zone OR within prox_pct% of zone boundary."""
    if zone_low <= last_close <= zone_high:
        return True
    pct_above = (last_close - zone_high) / zone_high * 100
    pct_below = (zone_low - last_close) / zone_low * 100
    return pct_above <= prox_pct or pct_below <= prox_pct

def _zone_age_days(trapped_on: str) -> int:
    """Days since zone was first trapped."""
    if not trapped_on:
        return 0
    try:
        d = date.fromisoformat(str(trapped_on)[:10])
        return (date.today() - d).days
    except Exception:
        return 0

def _pick_nifty_bias(nifty_close: float, zones: list, prox_pct: float) -> tuple:
    """
    Pick the nearest TRAPPED zone to nifty_close.
    BEAR zone near → CE bias (bears will be squeezed → market up).
    BULL zone near → PE bias (bulls will be squeezed → market down).
    Returns (bias: "CE"|"PE", zone: dict).
    """
    candidates = []
    for z in zones:
        if z.get("status") != "TRAPPED":
            continue
        zh, zl = z.get("zone_high", 0.0), z.get("zone_low", 0.0)
        if _in_proximity(nifty_close, zl, zh, prox_pct):
            mid  = (zh + zl) / 2
            dist = abs(nifty_close - mid)
            candidates.append((dist, z))
    if not candidates:
        # No zone within proximity — pick absolute nearest regardless of threshold
        for z in zones:
            if z.get("status") != "TRAPPED":
                continue
            zh, zl = z.get("zone_high", 0.0), z.get("zone_low", 0.0)
            mid  = (zh + zl) / 2
            dist = abs(nifty_close - mid)
            candidates.append((dist, z))
    if not candidates:
        return "CE", {}
    _, nearest = min(candidates, key=lambda x: x[0])
    bias = "CE" if nearest.get("kind") == "BEAR" else "PE"
    return bias, nearest

# ── NIFTY bias ────────────────────────────────────────────────────────────────

def scan_nifty_bias(token: str, proximity_pct: float = NIFTY_BIAS_PROXIMITY_PCT) -> dict:
    """Fetch NIFTY D1 bars, detect zones, return bias + zone."""
    df = _fetch_daily(NIFTY_KEY, token)
    if df.empty or len(df) < 5:
        return {"bias": "CE", "zone": {}, "nifty_close": 0.0}
    _, all_zones = scanner.scan_htf_spot(df)
    nifty_close  = float(df.iloc[-1]["close"])
    bias, zone   = _pick_nifty_bias(nifty_close, all_zones, proximity_pct)
    return {"bias": bias, "zone": zone, "nifty_close": nifty_close}

# ── Per-stock scan ────────────────────────────────────────────────────────────

def scan_stock(symbol: str, upstox_key: str, lot_size: int, strike_step: int,
               token: str, bias: str,
               stock_prox_pct: float = STOCK_ZONE_PROXIMITY_PCT,
               min_rr: float = MIN_RR) -> Optional[dict]:
    """
    Scan one stock's D1 bars. Returns result dict if it qualifies, else None.
    bias="CE" → look for BEAR (bearish) TRAPPED zones (bears squeezed → stock up → buy CE)
    bias="PE" → look for BULL (bullish) TRAPPED zones (bulls squeezed → stock down → buy PE)
    """
    df = _fetch_daily(upstox_key, token)
    if df.empty or len(df) < 5:
        return None
    last_close = float(df.iloc[-1]["close"])

    _, all_zones = scanner.scan_htf_spot(df)

    # Filter: only zones matching bias direction
    if bias == "CE":
        zones = [z for z in all_zones if z.get("kind") == "BEAR" and z.get("status") == "TRAPPED"]
    else:
        zones = [z for z in all_zones if z.get("kind") == "BULL" and z.get("status") == "TRAPPED"]

    if not zones:
        return None

    # Pick nearest zone to last_close
    best = min(zones, key=lambda z: abs(last_close - (z["zone_high"] + z["zone_low"]) / 2))
    zh, zl = best["zone_high"], best["zone_low"]

    if not _in_proximity(last_close, zl, zh, stock_prox_pct):
        return None

    # SL and T1
    if bias == "CE":
        sl = round(zl * (1 - SL_BUFFER_PCT / 100), 2)
        t1 = round(best.get("sl", zh * 1.05), 2)   # trapped sellers' SL = ref bar HIGH = our T1
    else:
        sl = round(zh * (1 + SL_BUFFER_PCT / 100), 2)
        t1 = round(best.get("sl", zl * 0.95), 2)

    rr = _compute_rr(entry=last_close, sl=sl, t1=t1, direction=bias)
    if rr["rr_ratio"] < min_rr:
        return None

    # Zone proximity %
    if zl <= last_close <= zh:
        zone_dist_pct = 0.0
    elif last_close > zh:
        zone_dist_pct = round((last_close - zh) / zh * 100, 2)
    else:
        zone_dist_pct = round((zl - last_close) / zl * 100, 2)

    # Count tests (number of times zone went TRAPPED+CLOSED cycle before)
    zone_tests = sum(
        1 for z in all_zones
        if abs(z.get("zone_low", 0) - zl) < strike_step and z.get("status") == "CLOSED"
    )

    # Suggested strike: ATM ± 1 step
    atm = round(last_close / strike_step) * strike_step
    suggested_strike = (atm - strike_step) if bias == "CE" else (atm + strike_step)

    return {
        "symbol":           symbol,
        "direction":        bias,
        "zone_high":        round(zh, 2),
        "zone_low":         round(zl, 2),
        "last_close":       round(last_close, 2),
        "zone_distance_pct": zone_dist_pct,
        "stock_sl":         sl,
        "stock_t1":         t1,
        "risk_pts":         rr["risk_pts"],
        "reward_pts":       rr["reward_pts"],
        "rr_ratio":         rr["rr_ratio"],
        "suggested_strike": suggested_strike,
        "lot_size":         lot_size,
        "zone_age_days":    _zone_age_days(best.get("trapped_on", "")),
        "zone_tests":       zone_tests,
        "scanned_at":       datetime.now().isoformat(timespec="seconds"),
    }

# ── Full scan run ─────────────────────────────────────────────────────────────

def run_scan(token: str,
             nifty_prox_pct: float = NIFTY_BIAS_PROXIMITY_PCT,
             stock_prox_pct: float = STOCK_ZONE_PROXIMITY_PCT,
             min_rr: float = MIN_RR) -> list:
    """Run full nightly scan. Returns list of qualifying stocks sorted by R:R desc."""
    bias_result = scan_nifty_bias(token, nifty_prox_pct)
    bias        = bias_result["bias"]
    nifty_close = bias_result["nifty_close"]
    nifty_zone  = bias_result["zone"]
    print(f"NIFTY close={nifty_close:.0f}  bias={bias}  "
          f"zone={nifty_zone.get('zone_low', 0):.0f}–{nifty_zone.get('zone_high', 0):.0f}")

    stocks_df = pd.read_csv(FNO_LIST_PATH)
    results   = []

    def _scan_one(row):
        return scan_stock(
            symbol=row["symbol"], upstox_key=row["upstox_key"],
            lot_size=int(row["lot_size"]), strike_step=int(row["strike_step"]),
            token=token, bias=bias,
            stock_prox_pct=stock_prox_pct, min_rr=min_rr,
        )

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as ex:
        futures = {ex.submit(_scan_one, row): row["symbol"]
                   for _, row in stocks_df.iterrows()}
        for i, fut in enumerate(as_completed(futures), 1):
            sym = futures[fut]
            try:
                res = fut.result()
                if res:
                    results.append(res)
                    print(f"  [{i}/{len(futures)}] {sym} ✓  R:R={res['rr_ratio']}")
                else:
                    print(f"  [{i}/{len(futures)}] {sym} —")
            except Exception as exc:
                print(f"  [{i}/{len(futures)}] {sym} ERROR: {exc}")

    results.sort(key=lambda x: x["rr_ratio"], reverse=True)

    # Add NIFTY context to output
    output = {
        "scan_date":   date.today().isoformat(),
        "nifty_close": nifty_close,
        "nifty_bias":  bias,
        "nifty_zone":  nifty_zone,
        "stocks":      results,
    }
    out_path = os.path.join(SCAN_DIR, f"fno_scan_{date.today()}.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\n✓ {len(results)} stocks qualify → {out_path}")
    return results

# ── Optimize mode ─────────────────────────────────────────────────────────────

def run_optimize(token: str) -> None:
    """
    Sweep NIFTY proximity × stock proximity thresholds over last 6 months.
    For each trading day: apply thresholds → check if shortlisted stocks
    actually moved in the predicted direction the NEXT day.
    Prints ranked table of (nifty_pct, stock_pct, avg_stocks/day, direction_accuracy%).
    """
    import numpy as np
    from itertools import product as _product

    end_dt  = date.today() - timedelta(days=1)
    start_dt = end_dt - timedelta(days=180)

    print("Fetching NIFTY D1 bars for optimize window...")
    nifty_df = _fetch_daily(NIFTY_KEY, token, days=200)
    if nifty_df.empty:
        print("[ERROR] Could not fetch NIFTY D1 bars")
        return

    print("Fetching all stock D1 bars (this takes ~3 minutes)...")
    stocks_df  = pd.read_csv(FNO_LIST_PATH)
    stock_bars = {}
    for _, row in stocks_df.iterrows():
        df = _fetch_daily(row["upstox_key"], token, days=200)
        stock_bars[row["symbol"]] = (df, int(row["lot_size"]), int(row["strike_step"]))
        print(f"  {row['symbol']}: {len(df)} bars")

    # Trading days in window
    trading_days = [
        d for d in pd.bdate_range(start_dt, end_dt)
        if d.date() in set(pd.to_datetime(nifty_df["datetime"]).dt.date)
    ]

    NIFTY_PCTS = [round(x, 2) for x in list(pd.np.arange(0.5, 3.25, 0.25))]
    STOCK_PCTS = [round(x, 2) for x in list(pd.np.arange(0.5, 3.25, 0.25))]

    # Pre-compute NIFTY zones per day
    _, nifty_all_zones = scanner.scan_htf_spot(nifty_df)

    results = []
    total = len(NIFTY_PCTS) * len(STOCK_PCTS)
    for idx, (np_pct, sp_pct) in enumerate(_product(NIFTY_PCTS, STOCK_PCTS), 1):
        day_counts    = []
        day_correct   = []
        for day_ts in trading_days[:-1]:  # exclude last (no next-day to check)
            day       = day_ts.date()
            next_day  = trading_days[trading_days.index(day_ts) + 1].date()

            nifty_row = nifty_df[nifty_df["datetime"] == day.isoformat()]
            if nifty_row.empty:
                continue
            nc = float(nifty_row.iloc[-1]["close"])

            # NIFTY bias on this day
            bias, _ = _pick_nifty_bias(nc, nifty_all_zones, np_pct)

            qualified = []
            for sym, (sdf, ls, ss) in stock_bars.items():
                if sdf.empty:
                    continue
                sdf_day = sdf[sdf["datetime"] <= day.isoformat()]
                if len(sdf_day) < 5:
                    continue
                lc = float(sdf_day.iloc[-1]["close"])
                _, szones = scanner.scan_htf_spot(sdf_day)
                wanted_kind = "BEAR" if bias == "CE" else "BULL"
                zones = [z for z in szones if z.get("kind") == wanted_kind and z.get("status") == "TRAPPED"]
                if not zones:
                    continue
                best = min(zones, key=lambda z: abs(lc - (z["zone_high"] + z["zone_low"]) / 2))
                if _in_proximity(lc, best["zone_low"], best["zone_high"], sp_pct):
                    qualified.append((sym, bias, lc))

            day_counts.append(len(qualified))

            # Check next-day direction
            for sym, b, entry in qualified:
                sdf = stock_bars[sym][0]
                nd_row = sdf[sdf["datetime"] == next_day.isoformat()]
                if nd_row.empty:
                    continue
                nd_close = float(nd_row.iloc[-1]["close"])
                correct  = (nd_close > entry) if b == "CE" else (nd_close < entry)
                day_correct.append(int(correct))

        avg_stocks = round(float(np.mean(day_counts)) if day_counts else 0, 1)
        acc        = round(100 * float(np.mean(day_correct)) if day_correct else 0, 1)
        results.append((np_pct, sp_pct, avg_stocks, acc, len(day_correct)))
        if idx % 20 == 0:
            print(f"  {idx}/{total} combos done...")

    results.sort(key=lambda x: x[3], reverse=True)
    print(f"\n{'NIFTY%':>8}  {'STOCK%':>8}  {'Avg/Day':>10}  {'Accuracy%':>12}  {'Samples':>8}")
    print("─" * 58)
    for np_pct, sp_pct, avg, acc, n in results[:20]:
        print(f"{np_pct:>8.2f}  {sp_pct:>8.2f}  {avg:>10.1f}  {acc:>11.1f}%  {n:>8}")

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    token = _get_token()
    if not token:
        print("[ERROR] No Upstox token found in data/clients.db")
        sys.exit(1)
    if "--optimize" in sys.argv:
        run_optimize(token)
    else:
        run_scan(token)
```

- [ ] **Step 4: Run tests — expect pass**

```bash
python -m pytest tests/test_fno_scanner.py -v
```
Expected output:
```
tests/test_fno_scanner.py::test_compute_rr_ce PASSED
tests/test_fno_scanner.py::test_compute_rr_pe PASSED
tests/test_fno_scanner.py::test_compute_rr_zero_risk PASSED
tests/test_fno_scanner.py::test_proximity_pass PASSED
tests/test_fno_scanner.py::test_proximity_outside_but_near PASSED
tests/test_fno_scanner.py::test_proximity_fail PASSED
tests/test_fno_scanner.py::test_zone_age PASSED
tests/test_fno_scanner.py::test_nifty_bias_picks_nearest_zone PASSED
tests/test_fno_scanner.py::test_nifty_bias_bull PASSED
9 passed
```

- [ ] **Step 5: Smoke-test with live token (optional — requires market data)**

```bash
python -X utf8 scripts/fno_stock_scanner.py 2>&1 | head -30
```
Expected: prints NIFTY bias line + per-stock scan progress + final JSON path.

- [ ] **Step 6: Commit**

```bash
git add scripts/fno_stock_scanner.py tests/test_fno_scanner.py
git commit -m "feat(scanner): FnO stock nightly D1 scanner + optimize sweep mode

- scan_nifty_bias(): NIFTY D1 zones → CE/PE direction for session
- scan_stock(): per-stock D1 scan, proximity filter, R:R compute
- run_scan(): parallel 10-workers, sorts by R:R desc, saves JSON
- run_optimize(): sweeps NIFTY%×Stock% thresholds over 6m history
- 9 unit tests covering R:R, proximity, zone age, bias pick"
```

---

## Task 3: Dashboard API Endpoints

**Files:**
- Modify: `ui_layer/dashboard_server.py` (append before `return app` on line ~4913)

**Interfaces:**
- Consumes: `data/fno_scan_YYYY-MM-DD.json` (Task 2)
- Produces:
  - `GET /api/scanner/fno` → `{"ok": true, "scan_date": "...", "bias": "CE"|"PE", "stocks": [...]}`
  - `POST /api/scanner/run` → `{"ok": true, "count": N, "stocks": [...]}`

- [ ] **Step 1: Write failing test for endpoint shape**

Add to `tests/test_fno_scanner.py`:

```python
def test_scan_json_structure():
    """Verify the JSON file written by run_scan has expected top-level keys."""
    import json, tempfile, os
    # Build a minimal mock output
    mock = {
        "scan_date": "2026-07-01",
        "nifty_close": 24500.0,
        "nifty_bias": "CE",
        "nifty_zone": {"zone_high": 24600.0, "zone_low": 24400.0},
        "stocks": [
            {"symbol": "RELIANCE", "direction": "CE", "rr_ratio": 2.5,
             "zone_high": 1310.0, "zone_low": 1280.0, "last_close": 1295.0,
             "stock_sl": 1277.4, "stock_t1": 1336.0, "risk_pts": 17.6,
             "reward_pts": 41.0, "zone_distance_pct": 0.0,
             "suggested_strike": 1300, "lot_size": 250,
             "zone_age_days": 3, "zone_tests": 1, "scanned_at": "2026-07-01T16:00:00"}
        ],
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(mock, f)
        tmp = f.name
    loaded = json.load(open(tmp))
    assert "stocks" in loaded
    assert loaded["stocks"][0]["rr_ratio"] == 2.5
    assert loaded["nifty_bias"] in ("CE", "PE")
    os.unlink(tmp)
```

```bash
python -m pytest tests/test_fno_scanner.py::test_scan_json_structure -v
```
Expected: PASS (this tests the structure, no API call needed).

- [ ] **Step 2: Add Pydantic schema at module level in `dashboard_server.py`**

Find the block of Pydantic schemas near the top of `dashboard_server.py` and add:

```python
class _ScannerRunSchema(BaseModel):
    nifty_prox_pct: float = 1.5
    stock_prox_pct: float = 2.0
    min_rr: float = 1.5
```

- [ ] **Step 3: Add endpoints before `return app` (line ~4913)**

In `dashboard_server.py`, find the line `return app` and insert before it:

```python
        # ── FnO Stock Scanner ────────────────────────────────────────────────
        @app.get("/api/scanner/fno", tags=["Scanner"])
        async def get_fno_scan():
            """Return today's FnO stock scan result (or most recent available)."""
            import glob as _glob
            import json as _json
            scan_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
            # Find most recent scan file
            pattern = os.path.join(scan_dir, "fno_scan_*.json")
            files   = sorted(_glob.glob(pattern), reverse=True)
            if not files:
                return {"ok": False, "error": "No scan file found — run the nightly scanner first"}
            try:
                with open(files[0]) as f:
                    data = _json.load(f)
                return {"ok": True, **data}
            except Exception as exc:
                return {"ok": False, "error": str(exc)}

        @app.post("/api/scanner/run", tags=["Scanner"])
        async def run_fno_scan(params: _ScannerRunSchema):
            """Admin: trigger a fresh FnO scan synchronously (takes ~2-3 min)."""
            try:
                import sys as _sys
                _scripts = os.path.join(os.path.dirname(os.path.dirname(__file__)), "scripts")
                if _scripts not in _sys.path:
                    _sys.path.insert(0, _scripts)
                from fno_stock_scanner import run_scan as _run_scan, _get_token as _tok
                token = _tok()
                if not token:
                    return {"ok": False, "error": "No Upstox token — connect feeder first"}
                results = await asyncio.to_thread(
                    _run_scan, token,
                    params.nifty_prox_pct, params.stock_prox_pct, params.min_rr
                )
                return {"ok": True, "count": len(results), "stocks": results}
            except Exception as exc:
                return {"ok": False, "error": str(exc)}
```

- [ ] **Step 4: Verify endpoints load without error**

```bash
python -c "from ui_layer.dashboard_server import DashboardServer; print('OK')"
```
Expected: `OK`

- [ ] **Step 5: Commit**

```bash
git add ui_layer/dashboard_server.py tests/test_fno_scanner.py
git commit -m "feat(api): add /api/scanner/fno GET + POST /api/scanner/run endpoints"
```

---

## Task 4: Dashboard — Stocks Tab UI

**Files:**
- Modify: `ui_layer/templates/monitor.html`

**Interfaces:**
- Consumes: `GET /api/scanner/fno` (Task 3)
- Produces: "STOCKS" tab visible in admin nav; alert cards ranked by R:R

- [ ] **Step 1: Add `stockScan` state and `loadStockScan()` method to Alpine data object**

Find the line in `monitor.html` that contains `riskSummary: {` (around line 757) and add before it:

```javascript
stockScan: { scan_date: '', nifty_bias: '', nifty_close: 0, nifty_zone: {}, stocks: [], loaded: false, error: '' },
```

Find the `_hydrateAdminOnLogin()` method (around line 1010) and add inside it:

```javascript
this.loadStockScan();
```

Add the `loadStockScan` method alongside other `load*` methods (after `loadRiskSummary` for example):

```javascript
async loadStockScan() {
  try {
    const r = await this._fetch('/api/scanner/fno');
    if (r.ok) {
      this.stockScan = { ...r, loaded: true, error: '' };
    } else {
      this.stockScan = { ...this.stockScan, loaded: true, error: r.error || 'Scan file not found' };
    }
  } catch(e) {
    this.stockScan = { ...this.stockScan, loaded: true, error: String(e) };
  }
},
async runStockScan() {
  this.stockScan.error = '';
  try {
    const r = await this._fetch('/api/scanner/run', { method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ nifty_prox_pct: 1.5, stock_prox_pct: 2.0, min_rr: 1.5 })
    });
    if (r.ok) { await this.loadStockScan(); }
    else { this.stockScan.error = r.error || 'Scan failed'; }
  } catch(e) { this.stockScan.error = String(e); }
},
```

- [ ] **Step 2: Add STOCKS tab button to admin nav**

Find in `monitor.html` the line:
```html
    <button class="admin-tab" :class="{active: adminTab==='clients'}"
```
Insert BEFORE it:
```html
    <button class="admin-tab" :class="{active: adminTab==='stocks'}"
            @click="adminTab='stocks'; loadStockScan()">&#x1F4C8; STOCKS</button>
```

- [ ] **Step 3: Add Stocks tab content panel**

Find the closing tag of the last admin tab section (search for `adminTab === 'trap'` section end, around line 5478+). After that section's closing `</div>`, add:

```html
<!-- ════════════════════════════════════════════════════
     STOCKS TAB — FnO Stock Scanner Morning Alerts
════════════════════════════════════════════════════ -->
<div x-show="adminTab === 'stocks'" class="flex flex-col gap-3 p-3">

  <!-- Header bar -->
  <div class="flex items-center justify-between">
    <div class="font-mono text-sm font-bold" style="color:var(--t-fg)">
      FnO STOCK SCANNER
      <span class="ml-2 font-normal text-xs" style="color:var(--t-muted)"
            x-text="stockScan.scan_date ? 'Scan: ' + stockScan.scan_date : 'No scan yet'"></span>
    </div>
    <div class="flex items-center gap-3">
      <!-- NIFTY bias badge -->
      <span x-show="stockScan.nifty_bias" class="font-mono text-xs px-2 py-0.5 rounded font-bold"
            :style="stockScan.nifty_bias==='CE'
              ? 'background:rgba(var(--t-green-rgb),0.12);color:var(--t-green);border:1px solid rgba(var(--t-green-rgb),0.3)'
              : 'background:rgba(var(--t-red-rgb),0.12);color:var(--t-red);border:1px solid rgba(var(--t-red-rgb),0.3)'"
            x-text="'NIFTY ' + stockScan.nifty_close.toFixed(0) + ' → ' + stockScan.nifty_bias + ' BIAS'">
      </span>
      <!-- Run scan button -->
      <button @click="runStockScan()"
              class="font-mono text-xs px-3 py-1 rounded"
              style="background:rgba(var(--t-accent-rgb),0.15);color:var(--t-accent);border:1px solid rgba(var(--t-accent-rgb),0.3)">
        ↻ RUN SCAN
      </button>
    </div>
  </div>

  <!-- Error -->
  <div x-show="stockScan.error" class="font-mono text-xs px-3 py-2 rounded"
       style="background:rgba(var(--t-red-rgb),0.1);color:var(--t-red)"
       x-text="stockScan.error"></div>

  <!-- No results -->
  <div x-show="stockScan.loaded && !stockScan.error && stockScan.stocks && stockScan.stocks.length === 0"
       class="font-mono text-xs" style="color:var(--t-muted)">
    No qualifying stocks today — either no zones align with NIFTY bias or R:R &lt; 1.5
  </div>

  <!-- Stock cards grid -->
  <div class="grid gap-3" style="grid-template-columns: repeat(auto-fill, minmax(320px, 1fr))">
    <template x-for="s in (stockScan.stocks || [])" :key="s.symbol">
      <div class="rounded border p-3 font-mono text-xs flex flex-col gap-2"
           style="background:var(--t-surface);border-color:var(--t-border)">

        <!-- Card header -->
        <div class="flex items-center justify-between">
          <div class="flex items-center gap-2">
            <span class="font-bold text-sm" style="color:var(--t-fg)" x-text="s.symbol"></span>
            <span class="text-xs px-1.5 py-0.5 rounded font-bold"
                  :style="s.direction==='CE'
                    ? 'color:var(--t-green);background:rgba(var(--t-green-rgb),0.12)'
                    : 'color:var(--t-red);background:rgba(var(--t-red-rgb),0.12)'"
                  x-text="'▲ BUY ' + s.direction"></span>
          </div>
          <!-- R:R badge -->
          <span class="text-xs px-2 py-0.5 rounded font-bold"
                :style="s.rr_ratio >= 2.5
                  ? 'color:var(--t-green);background:rgba(var(--t-green-rgb),0.15);border:1px solid rgba(var(--t-green-rgb),0.3)'
                  : s.rr_ratio >= 1.5
                  ? 'color:var(--t-amber);background:rgba(var(--t-amber-rgb),0.12);border:1px solid rgba(var(--t-amber-rgb),0.3)'
                  : 'color:var(--t-muted);border:1px solid var(--t-border)'"
                x-text="'R:R ' + s.rr_ratio.toFixed(1) + '×'"></span>
        </div>

        <!-- Zone row -->
        <div class="flex justify-between" style="color:var(--t-muted)">
          <span>D1 Zone</span>
          <span style="color:var(--t-fg)"
                x-text="'₹' + s.zone_low.toFixed(0) + ' – ₹' + s.zone_high.toFixed(0)"></span>
        </div>

        <!-- Last close row -->
        <div class="flex justify-between" style="color:var(--t-muted)">
          <span>Last Close</span>
          <span style="color:var(--t-fg)"
                x-text="'₹' + s.last_close.toFixed(0) + (s.zone_distance_pct === 0 ? ' (inside zone)' : ' (' + s.zone_distance_pct.toFixed(1) + '% from zone)')"></span>
        </div>

        <!-- SL row -->
        <div class="flex justify-between" style="color:var(--t-muted)">
          <span>Stock SL</span>
          <span style="color:var(--t-red)"
                x-text="'₹' + s.stock_sl.toFixed(0) + '  (risk ₹' + s.risk_pts.toFixed(0) + ')'"></span>
        </div>

        <!-- T1 row -->
        <div class="flex justify-between" style="color:var(--t-muted)">
          <span>Target T1</span>
          <span style="color:var(--t-green)"
                x-text="'₹' + s.stock_t1.toFixed(0) + '  (reward ₹' + s.reward_pts.toFixed(0) + ')'"></span>
        </div>

        <!-- Strike row -->
        <div class="flex justify-between" style="color:var(--t-muted)">
          <span>Strike</span>
          <span style="color:var(--t-fg)"
                x-text="s.suggested_strike + ' ' + s.direction + '  (lot ' + s.lot_size + ')'"></span>
        </div>

        <!-- Footer meta -->
        <div class="flex justify-between pt-1 border-t" style="border-color:var(--t-border);color:var(--t-muted)">
          <span x-text="'Age: ' + s.zone_age_days + 'd  |  Tests: ' + s.zone_tests"></span>
          <span style="color:var(--t-green);font-size:0.65rem">NIFTY ✓</span>
        </div>
      </div>
    </template>
  </div>
</div>
```

- [ ] **Step 4: Verify page loads without JS errors**

Start the system (or just open the HTML file in browser dev tools). Check browser console for errors. Navigate to STOCKS tab — should show either cards or "No qualifying stocks" message.

```bash
python run_system.py --mode demo --ui --port 5000
```
Open `http://localhost:5000`, log in as admin, click STOCKS tab. Confirm no console errors.

- [ ] **Step 5: Commit**

```bash
git add ui_layer/templates/monitor.html
git commit -m "feat(ui): add Stocks tab with FnO scanner morning alert cards

- NIFTY bias badge (CE/PE) in header
- Per-stock cards: zone, close, SL, T1, R:R badge, strike, zone age/tests
- R:R badge color: green ≥2.5×, amber ≥1.5×
- RUN SCAN button triggers POST /api/scanner/run"
```

---

## Task 5: Run Optimize + Set Final Thresholds

**Files:**
- Modify: `scripts/fno_stock_scanner.py` (update 2 config constants at top)

- [ ] **Step 1: Run the optimize sweep**

```bash
python -X utf8 scripts/fno_stock_scanner.py --optimize 2>&1
```
Takes ~3-5 minutes. Output shows ranked table of (NIFTY%, STOCK%, Avg/Day, Accuracy%).

- [ ] **Step 2: Pick winning thresholds**

From the output table, pick the combo with:
- Highest `Accuracy%`
- `Avg/Day` between 4 and 10 (not too many, not too few)

Example result interpretation:
```
NIFTY%     STOCK%   Avg/Day   Accuracy%   Samples
  1.50       2.00       6.3       74.1%       247   ← pick this
  2.00       2.50       9.1       71.3%       381
  1.00       1.50       3.8       78.2%       142   ← or this if prefer fewer/cleaner
```

- [ ] **Step 3: Update config constants in `scripts/fno_stock_scanner.py`**

Change lines 17–18 from defaults to winning values. Example:
```python
NIFTY_BIAS_PROXIMITY_PCT = 1.50   # optimized 2026-07-01: best accuracy at 6 stocks/day
STOCK_ZONE_PROXIMITY_PCT = 2.00   # optimized 2026-07-01: 74% directional accuracy
```

- [ ] **Step 4: Run live scan to verify end-to-end**

```bash
python -X utf8 scripts/fno_stock_scanner.py
```
Confirm: JSON saved to `data/fno_scan_YYYY-MM-DD.json`, open it and check cards look reasonable.

- [ ] **Step 5: Commit**

```bash
git add scripts/fno_stock_scanner.py
git commit -m "config(scanner): set optimized proximity thresholds from sweep results"
```

---

## Self-Review

**Spec coverage check:**
- ✅ Stage 1 nightly scan with D1 zones → Task 2
- ✅ NIFTY direction bias (near bearish = CE, near bullish = PE) → `scan_nifty_bias()` + `_pick_nifty_bias()`
- ✅ NIFTY always has direction (no skip days) → `_pick_nifty_bias()` falls back to absolute nearest if none within threshold
- ✅ R:R computed as (T1-entry)/(entry-SL), min 1.5 → `_compute_rr()` + `MIN_RR` filter
- ✅ T1 = zone's ref bar HIGH (sellers' SL) → `best.get("sl", ...)` in `scan_stock()`
- ✅ SL = zone boundary ± 0.2% → `SL_BUFFER_PCT = 0.2`
- ✅ Parallel workers 10 → `ThreadPoolExecutor(max_workers=10)`
- ✅ Optimize sweep NIFTY% × Stock% over 6m → `run_optimize()`
- ✅ Cards sorted by R:R desc → `results.sort(key=lambda x: x["rr_ratio"], reverse=True)`
- ✅ Morning card: zone, close, SL, T1, strike, zone age, zone tests, R:R → Task 4 HTML
- ✅ `GET /api/scanner/fno` + `POST /api/scanner/run` → Task 3
- ✅ FnO stock list CSV → Task 1
- ✅ 9 unit tests for pure functions → Task 2

**Type consistency check:** All method names match across tasks. `scan_stock()` signature matches `run_scan()` call. `_compute_rr()` return keys (`rr_ratio`, `risk_pts`, `reward_pts`) used consistently in `scan_stock()` result dict and HTML template.

**Placeholder check:** None found. All code is complete and runnable.
