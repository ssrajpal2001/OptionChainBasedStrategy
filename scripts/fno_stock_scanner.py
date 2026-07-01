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
STOCK_ZONE_PROXIMITY_PCT = 3.0   # tune via --optimize
SL_BUFFER_PCT            = 0.2
MIN_RR                   = 1.5
D1_LOOKBACK_DAYS         = 365
MAX_ZONE_AGE_DAYS        = 15    # ignore zones older than this
TOP_N_PER_DIRECTION      = 5     # max CE results + max PE results
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
    if last_close > zone_high:
        pct_above = (last_close - zone_high) / zone_high * 100
        return pct_above <= prox_pct
    else:
        pct_below = (zone_low - last_close) / zone_low * 100
        return pct_below <= prox_pct

def _zone_age_days(trapped_on: str) -> int:
    """Days since zone was first trapped."""
    if not trapped_on:
        return 0
    try:
        d = date.fromisoformat(str(trapped_on)[:10])
        return (date.today() - d).days
    except Exception:
        return 0

def _zone_date_str(trapped_on: str) -> str:
    """Return human date string e.g. 'Jun 25'."""
    if not trapped_on:
        return ""
    try:
        d = date.fromisoformat(str(trapped_on)[:10])
        return d.strftime("%b %d").lstrip("0")
    except Exception:
        return ""

def _approaching(last_close: float, zone_low: float, zone_high: float,
                 direction: str, prox_pct: float) -> bool:
    """
    True only when price is approaching from the CORRECT side.
    CE (bear zone = support, bears got trapped, price will go UP):
        Price is ABOVE zone or inside it (approaching support from above).
        Invalid if price already broke BELOW zone_low.
    PE (bull zone = resistance, bulls got trapped, price will go DOWN):
        Price is BELOW zone or inside it (approaching resistance from below).
        Invalid if price already broke ABOVE zone_high.
    """
    if zone_low <= last_close <= zone_high:
        return True                          # inside zone — always valid
    if direction == "CE":
        # Support zone. Price must be above zone (not broken below)
        if last_close < zone_low:
            return False                     # broke below support — invalid
        pct = (last_close - zone_high) / zone_high * 100
        return pct <= prox_pct
    else:  # PE
        # Resistance zone. Price must be below zone (not broken above)
        if last_close > zone_high:
            return False                     # broke above resistance — invalid
        pct = (zone_low - last_close) / zone_low * 100
        return pct <= prox_pct

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

def _merge_cluster(best: dict, zones: list) -> tuple:
    """
    Merge TRAPPED zones that OVERLAP with `best` into one cluster block.
    Overlap = zone boundaries intersect (zone_low of one <= zone_high of other).
    Only merges same-kind zones (BEAR with BEAR, BULL with BULL).
    Avoids the % threshold problem where 3% on ₹3000 = 90 pts and collapses R:R.
    """
    kind = best.get("kind")

    def _overlaps(z: dict) -> bool:
        return (z.get("kind") == kind
                and z.get("status") == "TRAPPED"
                and z["zone_low"]  <= best["zone_high"]
                and best["zone_low"] <= z["zone_high"])

    cluster = [z for z in zones if _overlaps(z)]
    if not cluster:
        cluster = [best]

    zh = max(z["zone_high"] for z in cluster)
    zl = min(z["zone_low"]  for z in cluster)

    # Use the most recently trapped zone as representative (freshest SL/target)
    rep = dict(sorted(cluster, key=lambda z: z.get("trapped_on") or "")[-1])
    rep["zone_high"] = zh
    rep["zone_low"]  = zl
    return zh, zl, rep


def _build_result(symbol: str, lot_size: int, strike_step: int,
                   last_close: float, direction: str, best: dict,
                   all_zones: list, stock_prox_pct: float, min_rr: float) -> Optional[dict]:
    """Build a result dict for one zone+direction. Returns None if doesn't qualify."""
    # Merge nearby zones into a cluster for accurate zone boundaries
    zh, zl, best = _merge_cluster(best, all_zones)

    # Age filter (use the most recent trapped_on in the cluster)
    age = _zone_age_days(best.get("trapped_on", ""))
    if age > MAX_ZONE_AGE_DAYS:
        return None

    # Directional proximity — price must be approaching from the correct side
    if not _approaching(last_close, zl, zh, direction, stock_prox_pct):
        return None

    # SL and T1
    if direction == "CE":
        sl = round(zl * (1 - SL_BUFFER_PCT / 100), 2)
        t1 = round(best.get("sl", zh * 1.05), 2)
        if t1 <= last_close:   # zone already played out — T1 already reached
            return None
    else:
        sl = round(zh * (1 + SL_BUFFER_PCT / 100), 2)
        t1 = round(best.get("sl", zl * 0.95), 2)
        if t1 >= last_close:   # zone already played out — T1 already reached
            return None

    rr = _compute_rr(entry=last_close, sl=sl, t1=t1, direction=direction)
    if rr["rr_ratio"] < min_rr:
        return None

    # Zone distance %
    if zl <= last_close <= zh:
        zone_dist_pct = 0.0
    elif last_close > zh:
        zone_dist_pct = round((last_close - zh) / zh * 100, 2)
    else:
        zone_dist_pct = round((zl - last_close) / zl * 100, 2)

    zone_tests = sum(
        1 for z in all_zones
        if abs(z.get("zone_low", 0) - zl) < strike_step and z.get("status") == "CLOSED"
    )

    atm = round(last_close / strike_step) * strike_step
    suggested_strike = (atm - strike_step) if direction == "CE" else (atm + strike_step)

    trapped_on = best.get("trapped_on", "")
    return {
        "symbol":            symbol,
        "direction":         direction,
        "zone_high":         round(zh, 2),
        "zone_low":          round(zl, 2),
        "last_close":        round(last_close, 2),
        "zone_distance_pct": zone_dist_pct,
        "stock_sl":          sl,
        "stock_t1":          t1,
        "risk_pts":          rr["risk_pts"],
        "reward_pts":        rr["reward_pts"],
        "rr_ratio":          rr["rr_ratio"],
        "suggested_strike":  suggested_strike,
        "lot_size":          lot_size,
        "zone_age_days":     age,
        "zone_date":         _zone_date_str(trapped_on),
        "zone_tests":        zone_tests,
        "nifty_bias":        "",   # filled by run_scan
        "scanned_at":        datetime.now().isoformat(timespec="seconds"),
    }


def scan_stock(symbol: str, upstox_key: str, lot_size: int, strike_step: int,
               token: str, bias: Optional[str] = None,
               stock_prox_pct: float = STOCK_ZONE_PROXIMITY_PCT,
               min_rr: float = MIN_RR) -> list:
    """
    Scan one stock's D1 bars.
    If bias is given (CE/PE), returns only matching direction (list of 0-1 items).
    If bias is None, returns best CE result + best PE result (list of 0-2 items).
    """
    df = _fetch_daily(upstox_key, token)
    if df.empty or len(df) < 5:
        return []
    last_close = float(df.iloc[-1]["close"])
    _, all_zones = scanner.scan_htf_spot(df)

    directions = [bias] if bias else ["CE", "PE"]
    results = []
    for direction in directions:
        kind = "BEAR" if direction == "CE" else "BULL"
        zones = [z for z in all_zones if z.get("kind") == kind and z.get("status") == "TRAPPED"]
        if not zones:
            continue
        # Pick nearest zone to last_close
        best = min(zones, key=lambda z: abs(last_close - (z["zone_high"] + z["zone_low"]) / 2))
        r = _build_result(symbol, lot_size, strike_step, last_close,
                          direction, best, zones, stock_prox_pct, min_rr)
        if r:
            results.append(r)
    return results

# ── Full scan run ─────────────────────────────────────────────────────────────

def run_scan(token: str,
             nifty_prox_pct: float = NIFTY_BIAS_PROXIMITY_PCT,
             stock_prox_pct: float = STOCK_ZONE_PROXIMITY_PCT,
             min_rr: float = MIN_RR,
             use_nifty_bias: bool = False) -> dict:
    """
    Run full nightly scan.
    use_nifty_bias=False (default): scan CE+PE independently, top 5 each.
    use_nifty_bias=True: filter stocks to NIFTY bias direction only.
    Returns output dict (also saved to JSON).
    """
    bias_result = scan_nifty_bias(token, nifty_prox_pct)
    nifty_bias  = bias_result["bias"]
    nifty_close = bias_result["nifty_close"]
    nifty_zone  = bias_result["zone"]
    print(f"NIFTY close={nifty_close:.0f}  bias={nifty_bias}  "
          f"zone={nifty_zone.get('zone_low', 0):.0f}–{nifty_zone.get('zone_high', 0):.0f}  "
          f"bias_filter={'ON' if use_nifty_bias else 'OFF'}")

    stocks_df = pd.read_csv(FNO_LIST_PATH)
    all_results: list[dict] = []

    def _scan_one(row):
        b = nifty_bias if use_nifty_bias else None
        return scan_stock(
            symbol=row["symbol"], upstox_key=row["upstox_key"],
            lot_size=int(row["lot_size"]), strike_step=int(row["strike_step"]),
            token=token, bias=b,
            stock_prox_pct=stock_prox_pct, min_rr=min_rr,
        )

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as ex:
        futures = {ex.submit(_scan_one, row): row["symbol"]
                   for _, row in stocks_df.iterrows()}
        for i, fut in enumerate(as_completed(futures), 1):
            sym = futures[fut]
            try:
                res_list = fut.result()
                if res_list:
                    for r in res_list:
                        r["nifty_bias"] = nifty_bias
                    all_results.extend(res_list)
                    tags = " ".join(f"{r['direction']} R:R={r['rr_ratio']}" for r in res_list)
                    print(f"  [{i}/{len(futures)}] {sym} ✓  {tags}")
                else:
                    print(f"  [{i}/{len(futures)}] {sym} —")
            except Exception as exc:
                print(f"  [{i}/{len(futures)}] {sym} ERROR: {exc}")

    # Split into CE and PE, top N each by R:R
    ce = sorted([r for r in all_results if r["direction"] == "CE"],
                key=lambda x: x["rr_ratio"], reverse=True)[:TOP_N_PER_DIRECTION]
    pe = sorted([r for r in all_results if r["direction"] == "PE"],
                key=lambda x: x["rr_ratio"], reverse=True)[:TOP_N_PER_DIRECTION]

    output = {
        "scan_date":      date.today().isoformat(),
        "nifty_close":    nifty_close,
        "nifty_bias":     nifty_bias,
        "nifty_zone":     nifty_zone,
        "use_nifty_bias": use_nifty_bias,
        "ce_stocks":      ce,
        "pe_stocks":      pe,
        "stocks":         ce + pe,   # backwards compat for UI
    }
    out_path = os.path.join(SCAN_DIR, f"fno_scan_{date.today()}.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\n✓ CE:{len(ce)}  PE:{len(pe)} → {out_path}")
    return output

# ── Optimize mode ─────────────────────────────────────────────────────────────

def run_optimize(token: str) -> None:
    """
    Sweep NIFTY proximity x stock proximity thresholds over last 6 months.
    For each trading day: apply thresholds -> check if shortlisted stocks
    actually moved in the predicted direction the NEXT day.
    Prints ranked table of (nifty_pct, stock_pct, avg_stocks/day, direction_accuracy%).
    """
    import numpy as np
    from itertools import product as _product

    end_dt   = date.today() - timedelta(days=1)
    start_dt = end_dt - timedelta(days=365)

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

    NIFTY_PCTS = [round(x, 2) for x in list(np.arange(0.5, 3.25, 0.25))]
    STOCK_PCTS = [round(x, 2) for x in list(np.arange(0.5, 3.25, 0.25))]

    # Pre-compute NIFTY zones per day
    _, nifty_all_zones = scanner.scan_htf_spot(nifty_df)

    results = []
    total = len(NIFTY_PCTS) * len(STOCK_PCTS)
    for idx, (np_pct, sp_pct) in enumerate(_product(NIFTY_PCTS, STOCK_PCTS), 1):
        day_counts    = []
        day_correct   = []
        for day_ts in trading_days[:-1]:  # exclude last (no next-day to check)
            day      = day_ts.date()
            next_day = trading_days[trading_days.index(day_ts) + 1].date()

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
