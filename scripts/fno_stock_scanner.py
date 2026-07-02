"""
FnO Stock Trap Scanner — Phase 1 (Alert-only, nightly D1 scan)
===============================================================
Usage:
  python scripts/fno_stock_scanner.py              # live scan → data/fno_scan_YYYY-MM-DD.json
  python scripts/fno_stock_scanner.py --optimize   # threshold sweep → prints table
"""
from __future__ import annotations

import os, sys, json, sqlite3, time
from datetime import date, datetime, timedelta, time as dtime
from typing import Optional
import pytz
_IST = pytz.timezone("Asia/Kolkata")
def _market_closed() -> bool:
    """True if NSE market has closed for today (after 15:30 IST)."""
    now = datetime.now(_IST)
    return now.time() >= dtime(15, 30) and now.weekday() < 5
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
STOCK_ZONE_PROXIMITY_PCT = 5.0   # tune via --optimize
SL_BUFFER_PCT            = 0.2
MIN_RR                   = 1.5
D1_LOOKBACK_DAYS         = 365
MAX_ZONE_AGE_DAYS        = 30    # ignore zones older than this
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

def _fetch_intraday_as_d1(instrument_key: str, token: str) -> Optional[dict]:
    """Fetch today's intraday 1m bars and collapse to a single D1 OHLCV row."""
    enc = _quote(instrument_key, safe="")
    url = f"{UPSTOX_BASE}/historical-candle/intraday/{enc}/1minute"
    try:
        r = requests.get(url, headers=_hdr(token), timeout=15)
        time.sleep(0.1)
        if r.status_code != 200:
            return None
        candles = r.json().get("data", {}).get("candles", [])
        if not candles:
            return None
        opens  = [float(c[1]) for c in candles]
        highs  = [float(c[2]) for c in candles]
        lows   = [float(c[3]) for c in candles]
        closes = [float(c[4]) for c in candles]
        return {
            "datetime": date.today().isoformat(),
            "open":  opens[0],
            "high":  max(highs),
            "low":   min(lows),
            "close": closes[-1],
        }
    except Exception:
        return None

def _fetch_daily(instrument_key: str, token: str, days: int = D1_LOOKBACK_DAYS) -> pd.DataFrame:
    """Fetch daily OHLCV bars. Appends today's bar from intraday API if D1 API lags."""
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
        df = pd.DataFrame(rows) if rows else pd.DataFrame()
        # If market has closed today and D1 API still shows yesterday, patch from intraday
        today_str = to_dt.isoformat()
        if _market_closed() and not df.empty and df.iloc[-1]["datetime"] < today_str:
            today_bar = _fetch_intraday_as_d1(instrument_key, token)
            if today_bar:
                df = pd.concat([df, pd.DataFrame([today_bar])], ignore_index=True)
        return df
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

def _classify_today_touch(last_close: float, today_high: float, today_low: float,
                          zl: float, zh: float, direction: str) -> dict:
    """
    Classify how today's session interacted with the zone.

    For tomorrow's option trade we care whether the zone is:
      - untouched        : price stayed above (CE) / below (PE) — classic pending retest
      - tested+bounced   : price entered the zone today but closed favourably — strong confirmation
      - inside           : today's close is inside the zone — active, watch first 15 min
      - broken           : price broke through the wrong side and closed there — invalidated

    Returns dict with keys: tested_today, bounced_today, broken_today, inside_today,
                            today_wick_pct, touch_status.
    """
    if direction == "CE":
        tested   = today_low <= zh
        inside   = zl <= last_close <= zh
        broken   = last_close < zl
        bounced  = tested and not broken and last_close >= zh
        # How deep did today's low wick into the zone? (0% = touched top, 100% = touched bottom)
        if zh > zl and today_low <= zh:
            today_wick_pct = min(100.0, max(0.0, round((zh - today_low) / (zh - zl) * 100, 1)))
        else:
            today_wick_pct = 0.0
    else:  # PE
        tested   = today_high >= zl
        inside   = zl <= last_close <= zh
        broken   = last_close > zh
        bounced  = tested and not broken and last_close <= zl
        if zh > zl and today_high >= zl:
            today_wick_pct = min(100.0, max(0.0, round((today_high - zl) / (zh - zl) * 100, 1)))
        else:
            today_wick_pct = 0.0

    if broken:
        touch_status = "BROKEN"
    elif bounced:
        touch_status = "BOUNCED"
    elif inside:
        touch_status = "INSIDE"
    elif tested:
        touch_status = "TESTED"
    else:
        touch_status = "UNTOUCHED"

    return {
        "tested_today": tested,
        "bounced_today": bounced,
        "broken_today": broken,
        "inside_today": inside,
        "today_wick_pct": today_wick_pct,
        "touch_status": touch_status,
    }

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
                  last_close: float, today_high: float, today_low: float,
                  direction: str, best: dict,
                  all_zones: list, stock_prox_pct: float, min_rr: float) -> Optional[dict]:
    """Build a result dict for one zone+direction. Returns None if doesn't qualify."""
    # Merge nearby zones into a cluster for accurate zone boundaries
    zh, zl, best = _merge_cluster(best, all_zones)

    # Age filter (use the most recent trapped_on in the cluster)
    age = _zone_age_days(best.get("trapped_on", ""))
    if age > MAX_ZONE_AGE_DAYS:
        return None

    # Minimum zone width filter — reject noise zones narrower than 0.3% of price
    min_width = last_close * 0.003
    if zh - zl < min_width:
        return None

    # Directional proximity — price must be approaching from the correct side
    if not _approaching(last_close, zl, zh, direction, stock_prox_pct):
        return None

    # Classify today's interaction with the zone
    touch = _classify_today_touch(last_close, today_high, today_low, zl, zh, direction)
    if touch["broken_today"]:
        return None

    # SL and T1
    if direction == "CE":
        sl = round(zl * (1 - SL_BUFFER_PCT / 100), 2)
        t1 = round(best.get("sl", zh * 1.05), 2)
        # Target already reached if today's close is above T1
        if last_close >= t1:
            return None
        # Realistic tomorrow entry = retest of zone_high (sellers' entry level)
        plan_entry = zh
    else:
        sl = round(zh * (1 + SL_BUFFER_PCT / 100), 2)
        t1 = round(best.get("sl", zl * 0.95), 2)
        if last_close <= t1:
            return None
        plan_entry = zl

    rr = _compute_rr(entry=plan_entry, sl=sl, t1=t1, direction=direction)
    if rr["rr_ratio"] < min_rr:
        return None

    # Zone distance % (from last_close)
    if zl <= last_close <= zh:
        zone_dist_pct = 0.0
    elif last_close > zh:
        zone_dist_pct = round((last_close - zh) / zh * 100, 2)
    else:
        zone_dist_pct = round((zl - last_close) / zl * 100, 2)

    # Distance from plan entry to last close
    plan_entry_dist_pct = round(abs(last_close - plan_entry) / plan_entry * 100, 2) if plan_entry else 0.0

    zone_tests = sum(
        1 for z in all_zones
        if abs(z.get("zone_low", 0) - zl) < strike_step and z.get("status") == "CLOSED"
    )

    atm = round(last_close / strike_step) * strike_step
    suggested_strike = (atm - strike_step) if direction == "CE" else (atm + strike_step)

    trapped_on = best.get("trapped_on", "")

    # Forward-looking plan text
    if touch["bounced_today"]:
        plan = "Zone tested & bounced today. Enter on a 5m/15m pullback to zone."
    elif touch["inside_today"]:
        plan = "Price inside zone at close. Watch first 15 min for hold/reversal."
    elif zone_dist_pct == 0.0:
        plan = "Price at zone. Enter on confirmed reversal candle."
    else:
        plan = f"Wait for price to retest {plan_entry:.1f}–{zl:.1f} zone."

    return {
        "symbol":               symbol,
        "direction":            direction,
        "zone_high":            round(zh, 2),
        "zone_low":             round(zl, 2),
        "last_close":           round(last_close, 2),
        "entry_plan_price":     round(plan_entry, 2),
        "zone_distance_pct":    zone_dist_pct,
        "plan_entry_dist_pct":  plan_entry_dist_pct,
        "stock_sl":             sl,
        "stock_t1":             t1,
        "risk_pts":             rr["risk_pts"],
        "reward_pts":           rr["reward_pts"],
        "rr_ratio":             rr["rr_ratio"],
        "suggested_strike":     suggested_strike,
        "lot_size":             lot_size,
        "zone_age_days":        age,
        "zone_date":            _zone_date_str(trapped_on),
        "zone_tests":           zone_tests,
        "touch_status":         touch["touch_status"],
        "tested_today":         touch["tested_today"],
        "bounced_today":        touch["bounced_today"],
        "inside_today":         touch["inside_today"],
        "today_wick_pct":       touch["today_wick_pct"],
        "tomorrow_plan":        plan,
        "nifty_bias":           "",   # filled by run_scan
        "scanned_at":           datetime.now().isoformat(timespec="seconds"),
    }


def _score_zone(r: dict) -> float:
    """
    Score a qualifying zone for tomorrow's trade.
    Prefer:
      1. Tested-and-bounced today (strong confirmation)
      2. Freshness (younger age)
      3. Higher R:R
      4. Closer to zone (smaller plan_entry_dist_pct)
    Returns a higher-is-better score.
    """
    score = 0.0
    if r.get("bounced_today"):
        score += 200.0
    elif r.get("inside_today"):
        score += 80.0
    elif r.get("tested_today"):
        score += 40.0
    # Freshness: 0-day = +30, 30-day = +0
    score += max(0.0, 30.0 - r.get("zone_age_days", 0))
    # R:R
    score += r.get("rr_ratio", 0.0) * 20.0
    # Proximity to entry: closer is better, cap benefit
    score += max(0.0, 10.0 - r.get("plan_entry_dist_pct", 0.0))
    return score


def scan_stock(symbol: str, upstox_key: str, lot_size: int, strike_step: int,
               token: str, bias: Optional[str] = None,
               stock_prox_pct: float = STOCK_ZONE_PROXIMITY_PCT,
               min_rr: float = MIN_RR) -> list:
    """
    Scan one stock's D1 bars.
    If bias is given (CE/PE), returns only matching direction (list of 0-1 items).
    If bias is None, returns best CE result + best PE result (list of 0-2 items).

    Forward-looking logic for tomorrow's option trade:
      - Uses zone-boundary entry for realistic R:R
      - Classifies today's interaction (bounced/inside/broken/untouched)
      - Skips only broken zones; tested-and-bounced zones are preferred
      - Applies clean-old-zones filter
    """
    df = _fetch_daily(upstox_key, token)
    if df.empty or len(df) < 5:
        return []
    last_close = float(df.iloc[-1]["close"])
    today_high = float(df.iloc[-1]["high"])
    today_low  = float(df.iloc[-1]["low"])
    _, all_zones = scanner.scan_htf_spot(df)

    directions = [bias] if bias else ["CE", "PE"]
    results = []
    for direction in directions:
        kind = "BEAR" if direction == "CE" else "BULL"
        zones = [z for z in all_zones if z.get("kind") == kind and z.get("status") == "TRAPPED"]
        if not zones:
            continue
        # Evaluate all trapped zones; score and pick the best for tomorrow
        candidates = []
        for candidate in zones:
            r = _build_result(symbol, lot_size, strike_step, last_close,
                              today_high, today_low,
                              direction, candidate, all_zones, stock_prox_pct, min_rr)
            if not r:
                continue
            r["_score"] = _score_zone(r)
            candidates.append(r)
        if not candidates:
            continue
        best_r = max(candidates, key=lambda x: x["_score"])
        del best_r["_score"]
        results.append(best_r)
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

# ── Debug mode ────────────────────────────────────────────────────────────────

def debug_stock(symbol: str, token: str) -> None:
    """Print all detected zones for a single stock. Use --debug SYMBOL."""
    stocks_df = pd.read_csv(FNO_LIST_PATH)
    row = stocks_df[stocks_df["symbol"].str.upper() == symbol.upper()]
    if row.empty:
        print(f"[ERROR] {symbol} not found in {FNO_LIST_PATH}")
        return
    row = row.iloc[0]
    df = _fetch_daily(row["upstox_key"], token)
    if df.empty:
        print(f"[ERROR] Could not fetch bars for {symbol}")
        return
    last_close = float(df.iloc[-1]["close"])
    today_high = float(df.iloc[-1]["high"])
    today_low  = float(df.iloc[-1]["low"])
    print(f"\n{symbol}  last_close={last_close}  today_low={today_low}  today_high={today_high}")
    print(f"Total bars: {len(df)}  ({df.iloc[0]['datetime']} → {df.iloc[-1]['datetime']})")

    _, all_zones = scanner.scan_htf_spot(df)
    bear = [z for z in all_zones if z["kind"] == "BEAR"]
    bull = [z for z in all_zones if z["kind"] == "BULL"]
    print(f"\nBEAR zones ({len(bear)} total, {sum(1 for z in bear if z['status']=='TRAPPED')} TRAPPED):")
    for z in sorted(bear, key=lambda z: str(z.get("trapped_on") or ""), reverse=True)[:10]:
        app = _approaching(last_close, z["zone_low"], z["zone_high"], "CE", STOCK_ZONE_PROXIMITY_PCT)
        touch = _classify_today_touch(last_close, today_high, today_low,
                                      z["zone_low"], z["zone_high"], "CE")
        print(f"  [{z['status']:8}] zone={z['zone_low']:.1f}–{z['zone_high']:.1f}  "
              f"sl={z['sl']:.1f}  trapped={z.get('trapped_on','')}  "
              f"approaching={app}  touch={touch['touch_status']:8} "
              f"wick={touch['today_wick_pct']:.0f}%")
    print(f"\nBULL zones ({len(bull)} total, {sum(1 for z in bull if z['status']=='TRAPPED')} TRAPPED):")
    for z in sorted(bull, key=lambda z: str(z.get("trapped_on") or ""), reverse=True)[:10]:
        app = _approaching(last_close, z["zone_low"], z["zone_high"], "PE", STOCK_ZONE_PROXIMITY_PCT)
        touch = _classify_today_touch(last_close, today_high, today_low,
                                      z["zone_low"], z["zone_high"], "PE")
        print(f"  [{z['status']:8}] zone={z['zone_low']:.1f}–{z['zone_high']:.1f}  "
              f"sl={z['sl']:.1f}  trapped={z.get('trapped_on','')}  "
              f"approaching={app}  touch={touch['touch_status']:8} "
              f"wick={touch['today_wick_pct']:.0f}%")

    # Run actual scan logic to show real verdict (includes width filter, R:R, T1 check)
    lot_size   = int(row["lot_size"])
    strike_step = int(row["strike_step"])
    print(f"\n── Full scan verdict (prox={STOCK_ZONE_PROXIMITY_PCT}%, min_rr={MIN_RR}, max_age={MAX_ZONE_AGE_DAYS}d) ──")
    results = scan_stock(symbol, row["upstox_key"], lot_size, strike_step, token,
                         stock_prox_pct=STOCK_ZONE_PROXIMITY_PCT, min_rr=MIN_RR)
    if results:
        for r in results:
            print(f"  ✓ {r['direction']} zone={r['zone_low']}–{r['zone_high']}  "
                  f"entry_plan={r['entry_plan_price']}  sl={r['stock_sl']}  t1={r['stock_t1']}  "
                  f"R:R={r['rr_ratio']}  age={r['zone_age_days']}d  touch={r['touch_status']}")
            print(f"    → {r['tomorrow_plan']}")
    else:
        print("  — no qualifying setup today")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    token = _get_token()
    if not token:
        print("[ERROR] No Upstox token found in data/clients.db")
        sys.exit(1)
    if "--optimize" in sys.argv:
        run_optimize(token)
    elif "--debug" in sys.argv:
        idx = sys.argv.index("--debug")
        sym = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else ""
        if not sym:
            print("[ERROR] Usage: python scripts/fno_stock_scanner.py --debug SYMBOL")
        else:
            debug_stock(sym, token)
    else:
        run_scan(token)
