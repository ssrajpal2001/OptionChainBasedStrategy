"""
NSE/BANKNIFTY 4-Tier Cascade Backtest — Option Premium Chart
=============================================================
Period  : June 1 – June 30, 2026  (configurable)
Symbol  : NIFTY or BANKNIFTY      (configurable)

Trap detected on the OPTION PREMIUM 1m chart (not spot).
  BEAR trap in CE premium → BUY CE (bears in CE squeezed out as premium rises)
  BEAR trap in PE premium → BUY PE (bears in PE squeezed out as premium rises)

Cascade (4 tiers):
  HTF zone on option premium → MTF overlap → LTF overlap
  → exec-TF candle inside LTF zone → entry on break of exec candle HIGH

Zone logic (same as BTC):
  BEAR: zone=[zone_low, zone_high], T1=sl (trapped bears' stop = premium squeeze target)
  Entry when premium returns to zone → BUY
  SL: trailing SL in premium points

Data source: Upstox historical candle REST API (reads token from data/clients.db)

Usage:
  python3 scripts/nse_cascade_backtest.py [NIFTY|BANKNIFTY] [CE|PE|BOTH]

Requirements:
  pip install requests pandas numpy pyarrow
"""
from __future__ import annotations

import os, sys, time, sqlite3, json, base64
from datetime import date, datetime, timedelta
from typing import Optional, Tuple, List, Dict
from urllib.parse import quote as _quote
import numpy as np
import pandas as pd
import requests

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from strategies.trap_scanner import scanner   # scan_htf_spot

# ── Config ────────────────────────────────────────────────────────────────────

SYMBOL    = sys.argv[1].upper() if len(sys.argv) > 1 else "NIFTY"
OPT_SIDE  = sys.argv[2].upper() if len(sys.argv) > 2 else "BOTH"   # CE | PE | BOTH

START_DATE = date(2026, 6,  1)
END_DATE   = date(2026, 6, 30)
LOOKBACK   = 5          # extra days before START_DATE for zone warmup

DB_PATH    = os.path.join(_ROOT, "data", "clients.db")
CACHE_DIR  = os.path.join(_ROOT, "data", "nse_option_cache")
OUT_CSV    = os.path.join(_ROOT, "data", f"{SYMBOL.lower()}_cascade_results.csv")

# Monthly expiries  {(year, month, symbol): expiry_str}
MONTHLY_EXP = {
    ("NIFTY",     2026, 6): ("25JUN26", date(2026, 6, 25)),
    ("NIFTY",     2026, 7): ("30JUL26", date(2026, 7, 30)),
    ("BANKNIFTY", 2026, 6): ("24JUN26", date(2026, 6, 24)),
    ("BANKNIFTY", 2026, 7): ("29JUL26", date(2026, 7, 29)),
}

LOT_SIZES  = {"NIFTY": 25, "BANKNIFTY": 15, "FINNIFTY": 40, "SENSEX": 10}
STRIKE_STEPS = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50, "SENSEX": 100}
INDEX_KEY  = {
    "NIFTY":     "NSE_INDEX%7CNifty%2050",
    "BANKNIFTY": "NSE_INDEX%7CNifty%20Bank",
    "FINNIFTY":  "NSE_INDEX%7CNifty%20Fin%20Service",
    "SENSEX":    "BSE_INDEX%7CSENSEX",
}

LOT   = LOT_SIZES.get(SYMBOL, 25)
STEP  = STRIKE_STEPS.get(SYMBOL, 50)

# Grid
HTF_GRID  = [60, 120, 180, 240]
MTF_GRID  = [15, 30]
LTF_GRID  = [3, 5]
EXEC_GRID = [1, 3]
SL_GRID   = [5, 10, 20, 30]    # premium points
CAP_GRID  = [0, 30, 50, 100]   # premium points (0 = trailing SL only)

# How many strikes above/below ATM to scan (covers ATM, 1-OTM, 1-ITM etc.)
STRIKES_OFFSET = [-2, -1, 0, 1, 2]   # × STEP from ATM

UPSTOX_BASE = "https://api.upstox.com/v2"

# ── Token from DB ─────────────────────────────────────────────────────────────

def _get_upstox_token() -> str:
    if not os.path.exists(DB_PATH):
        print(f"[ERROR] DB not found: {DB_PATH}", flush=True)
        return ""
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT creds FROM feeder_creds WHERE provider='upstox' LIMIT 1"
        ).fetchone()
        conn.close()
        if not row:
            return ""
        raw = row[0]
        # XOR-decode (same as client_db._decode_cred)
        try:
            key   = b"AlgoSoft2024"
            data  = base64.b64decode(raw.encode())
            dec   = bytes(b ^ key[i % len(key)] for i, b in enumerate(data))
            creds = json.loads(dec.decode())
        except Exception:
            creds = json.loads(raw) if isinstance(raw, str) else {}
        return creds.get("access_token", "")
    except Exception as exc:
        print(f"[WARN] Could not read token from DB: {exc}", flush=True)
        return ""

# ── Upstox REST ───────────────────────────────────────────────────────────────

def _upstox_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

def _fetch_index_daily(symbol: str, token: str, fr: date, to: date) -> pd.DataFrame:
    """Fetch daily OHLC for index (to compute ATM per day)."""
    enc_key = INDEX_KEY.get(symbol, f"NSE_INDEX%7C{symbol}")
    url = f"{UPSTOX_BASE}/historical-candle/{enc_key}/day/{to}/{fr}"
    r = requests.get(url, headers=_upstox_headers(token), timeout=15)
    if r.status_code != 200:
        print(f"[WARN] index daily HTTP {r.status_code}: {r.text[:200]}", flush=True)
        return pd.DataFrame()
    candles = r.json().get("data", {}).get("candles", [])
    rows = [{"date": c[0][:10], "open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4])} for c in reversed(candles)]
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def _fetch_option_1m(symbol: str, exp_str: str, strike: int, opt_type: str,
                     token: str, fr: date, to: date) -> pd.DataFrame:
    """Fetch 1m historical bars for an option strike from Upstox."""
    exc_pfx = "BSE_FO" if symbol == "SENSEX" else "NSE_FO"
    raw_key = f"{exc_pfx}|{symbol}{exp_str}{strike}{opt_type}"
    enc_key = _quote(raw_key, safe="")
    cache_f = os.path.join(CACHE_DIR, f"{symbol}_{exp_str}_{strike}{opt_type}_{fr}_{to}.parquet")
    os.makedirs(CACHE_DIR, exist_ok=True)
    if os.path.exists(cache_f):
        return pd.read_parquet(cache_f)
    url = f"{UPSTOX_BASE}/historical-candle/{enc_key}/1minute/{to + timedelta(days=1)}/{fr}"
    r = requests.get(url, headers=_upstox_headers(token), timeout=20)
    time.sleep(0.3)   # rate limit
    if r.status_code != 200:
        print(f"  [WARN] {raw_key} HTTP {r.status_code}", flush=True)
        return pd.DataFrame()
    candles = r.json().get("data", {}).get("candles", [])
    if not candles:
        return pd.DataFrame()
    rows = [{"datetime": c[0], "open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4]), "volume": int(c[5])}
            for c in reversed(candles)]
    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df.to_parquet(cache_f, index=False)
    return df

# ── Resample + zones ──────────────────────────────────────────────────────────

def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty or len(df) < 2:
        return pd.DataFrame()
    if df["datetime"].dt.tz is not None:
        df = df.copy()
        df["datetime"] = df["datetime"].dt.tz_localize(None)
    agg = {"open": "first", "high": "max", "low": "min", "close": "last"}
    out = df.set_index("datetime").resample(f"{minutes}min", closed="left", label="left")[list(agg)].agg(agg)
    out = out.dropna(subset=["open"]).reset_index()
    out.rename(columns={"datetime": "datetime"}, inplace=True)
    return out

def _get_zones(bars: pd.DataFrame) -> list:
    if len(bars) < 3:
        return []
    _, ents = scanner.scan_htf_spot(bars)
    return [e for e in (ents or []) if e.get("status") in ("CLOSED", "TRAPPED")]

def _eff_zone(z: dict) -> Tuple[float, float]:
    return float(z["zone_low"]), float(z["zone_high"])

def _zones_overlap(parent: dict, child: dict, tol: float = 0.15) -> bool:
    pl, ph = _eff_zone(parent)
    cl, ch = _eff_zone(child)
    buf = max((ph - pl) * tol, 1.0)
    return cl <= ph + buf and ch >= pl - buf

# ── numpy simulation ──────────────────────────────────────────────────────────

def _simulate_numpy(H, L, C, is_long, entry, init_sl, t1, sl_buf, cap_pts, size) -> dict:
    active_sl = init_sl
    for i in range(len(H)):
        h, l, c = float(H[i]), float(L[i]), float(C[i])
        run = (c - entry) if is_long else (entry - c)
        new_trail = (h - sl_buf) if is_long else (l + sl_buf)
        if is_long and new_trail > active_sl:
            active_sl = new_trail
        elif not is_long and new_trail < active_sl:
            active_sl = new_trail
        if cap_pts > 0 and run >= cap_pts:
            return {"pnl": round(run * size, 2), "exit_reason": "CAP", "exit_price": c}
        if (is_long and l <= active_sl) or (not is_long and h >= active_sl):
            pnl = (active_sl - entry if is_long else entry - active_sl) * size
            return {"pnl": round(pnl, 2), "exit_reason": "SL", "exit_price": active_sl}
        if (is_long and h >= t1) or (not is_long and l <= t1):
            pnl = (t1 - entry if is_long else entry - t1) * size
            return {"pnl": round(pnl, 2), "exit_reason": "T1", "exit_price": t1}
    ep = float(C[-1]) if len(C) > 0 else entry
    pnl = (ep - entry if is_long else entry - ep) * size
    return {"pnl": round(pnl, 2), "exit_reason": "EOD", "exit_price": ep}

def _find_exec_entry(exec_arr, ltf_zone, htf_zone, kind, sl_buf, cap_pts, lot) -> Optional[dict]:
    is_long = True   # option premium bear trap → always BUY (premium will rise)
    ltf_l, ltf_h = _eff_zone(ltf_zone)
    buf = max((ltf_h - ltf_l) * 0.15, 1.0)
    t1  = float(htf_zone.get("sl", 0))
    size = lot
    if t1 <= 0:
        return None
    H = exec_arr["high"]
    L = exec_arr["low"]
    C = exec_arr["close"]
    n = len(H)
    if n < 2:
        return None
    in_zone = (C >= ltf_l - buf) & (C <= ltf_h + buf)
    idxs    = np.where(in_zone)[0]
    idxs    = idxs[idxs < n - 1]
    for i in idxs:
        trig     = float(H[i])          # always BUY: break of exec candle HIGH
        entry_sl = float(L[i]) - sl_buf
        if t1 <= trig or entry_sl >= trig:
            continue
        hit = np.where(H[i+1:] >= trig)[0]
        if len(hit) == 0:
            continue
        entry_idx   = hit[0]
        sim_h = H[i+1:][entry_idx:]
        sim_l = L[i+1:][entry_idx:]
        sim_c = C[i+1:][entry_idx:]
        if len(sim_h) == 0:
            continue
        res = _simulate_numpy(sim_h, sim_l, sim_c, True, trig, entry_sl,
                              t1, sl_buf, cap_pts, size)
        res["entry_price"] = round(trig, 2)
        res["t1"]          = round(t1, 2)
        res["exec_sl"]     = round(entry_sl, 2)
        return res
    return None

# ── Per-day cascade ───────────────────────────────────────────────────────────

def _run_cascade_day(day_str, exec_arr, htf_zones, mtf_zones, ltf_zones,
                     sl_buf, cap_pts, sl_hist, lot) -> Optional[dict]:
    for htf_z in htf_zones:
        kind = htf_z.get("kind", "BEAR")
        if kind != "BEAR":   # option premium: only BEAR trap → BUY (premium squeezes up)
            continue
        hl, hh   = _eff_zone(htf_z)
        zone_key = f"{hl:.1f}-{hh:.1f}"
        t1 = float(htf_z.get("sl", 0))
        if t1 <= hh:   # T1 must be ABOVE zone_high
            continue
        if zone_key in sl_hist:
            if (date.fromisoformat(day_str) - date.fromisoformat(sl_hist[zone_key])).days <= 1:
                continue
        mtf_m = next((z for z in mtf_zones if z.get("kind") == kind and _zones_overlap(htf_z, z)), None)
        if not mtf_m:
            continue
        ltf_m = next((z for z in ltf_zones if z.get("kind") == kind and _zones_overlap(mtf_m, z)), None)
        if not ltf_m:
            continue
        res = _find_exec_entry(exec_arr, ltf_m, htf_z, kind, sl_buf, cap_pts, lot)
        if res:
            res.update({"date": day_str, "zone_key": zone_key})
            if res["exit_reason"] == "SL":
                sl_hist[zone_key] = day_str
            return res
    return None

# ── Summary ───────────────────────────────────────────────────────────────────

def _summarize(trades, params) -> dict:
    wins   = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    gp = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    pf = round(gp / gl, 3) if gl > 0 else (9999.0 if gp > 0 else 0.0)
    s  = {
        "total"        : len(trades),
        "wins"         : len(wins),
        "losses"       : len(losses),
        "win_rate_pct" : round(len(wins)/len(trades)*100, 1) if trades else 0.0,
        "profit_factor": pf,
        "net_pnl_inr"  : round(gp - gl, 2),
        "avg_win_inr"  : round(gp/len(wins),   2) if wins   else 0.0,
        "avg_loss_inr" : round(gl/len(losses), 2) if losses else 0.0,
        "exits_sl"     : sum(1 for t in trades if t.get("exit_reason") == "SL"),
        "exits_t1"     : sum(1 for t in trades if t.get("exit_reason") == "T1"),
        "exits_cap"    : sum(1 for t in trades if t.get("exit_reason") == "CAP"),
        "exits_eod"    : sum(1 for t in trades if t.get("exit_reason") == "EOD"),
    }
    s.update(params)
    return s

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"=== {SYMBOL} Cascade Backtest on Option Premium Chart ===", flush=True)
    print(f"    Period: {START_DATE} to {END_DATE}  |  Side: {OPT_SIDE}  |  Lot: {LOT}", flush=True)

    token = _get_upstox_token()
    if not token:
        print("[ERROR] No Upstox token found in DB. Run the live system first to cache token.", flush=True)
        sys.exit(1)
    print(f"[OK] Upstox token loaded", flush=True)

    # Fetch index daily bars to get ATM per day
    fetch_fr = START_DATE - timedelta(days=LOOKBACK + 5)
    print(f"[{SYMBOL}] Fetching daily index bars {fetch_fr} -> {END_DATE} ...", flush=True)
    daily_df = _fetch_index_daily(SYMBOL, token, fetch_fr, END_DATE)
    if daily_df.empty:
        print("[ERROR] Could not fetch index daily data. Check token.", flush=True)
        sys.exit(1)
    daily_df["date"] = daily_df["date"].astype(str)
    daily_map = {r["date"]: r for _, r in daily_df.iterrows()}
    print(f"[{SYMBOL}] {len(daily_df)} daily bars", flush=True)

    # Build list of trading days in our period
    all_days = []
    d = START_DATE
    while d <= END_DATE:
        if d.isoformat() in daily_map:
            all_days.append(d.isoformat())
        d += timedelta(days=1)
    print(f"[{SYMBOL}] {len(all_days)} trading days", flush=True)

    # Determine expiry per day and ATM per day
    def _get_expiry_for_day(d_str: str):
        d = date.fromisoformat(d_str)
        exp_str, exp_date = MONTHLY_EXP.get((SYMBOL, d.year, d.month),
                                             (None, None))
        if exp_date and d > exp_date:
            # After expiry → use next month
            nm = d.month + 1 if d.month < 12 else 1
            ny = d.year if d.month < 12 else d.year + 1
            exp_str, exp_date = MONTHLY_EXP.get((SYMBOL, ny, nm), (None, None))
        return exp_str, exp_date

    def _get_atm(d_str: str) -> int:
        # Use previous day's close to compute ATM
        prev_row = None
        prev_d   = (date.fromisoformat(d_str) - timedelta(days=1)).isoformat()
        for _ in range(7):
            if prev_d in daily_map:
                prev_row = daily_map[prev_d]
                break
            prev_d = (date.fromisoformat(prev_d) - timedelta(days=1)).isoformat()
        if prev_row is None:
            return 0
        close = float(prev_row["close"])
        return int(round(close / STEP) * STEP)

    # Build strike list per day
    opt_sides = ["CE", "PE"] if OPT_SIDE == "BOTH" else [OPT_SIDE]
    download_tasks: list = []   # (day_str, exp_str, strike, opt_type)
    for d_str in all_days:
        exp_str, exp_date = _get_expiry_for_day(d_str)
        if not exp_str:
            continue
        atm = _get_atm(d_str)
        if not atm:
            continue
        for offset in STRIKES_OFFSET:
            strike = atm + offset * STEP
            for ot in opt_sides:
                download_tasks.append((d_str, exp_str, strike, ot))

    # Deduplicate downloads (same strike/expiry across multiple days → one fetch)
    fetch_set: set = set()
    for _, exp_str, strike, ot in download_tasks:
        fetch_set.add((exp_str, strike, ot))

    print(f"[{SYMBOL}] Downloading {len(fetch_set)} option series ...", flush=True)
    fetch_fr2 = START_DATE - timedelta(days=LOOKBACK + 1)
    option_bars: Dict[tuple, pd.DataFrame] = {}
    for i, (exp_str, strike, ot) in enumerate(sorted(fetch_set)):
        df = _fetch_option_1m(SYMBOL, exp_str, strike, ot, token, fetch_fr2, END_DATE)
        option_bars[(exp_str, strike, ot)] = df
        if df.empty:
            print(f"  [{i+1}/{len(fetch_set)}] {SYMBOL}{exp_str}{strike}{ot} — NO DATA", flush=True)
        else:
            print(f"  [{i+1}/{len(fetch_set)}] {SYMBOL}{exp_str}{strike}{ot} — {len(df)} bars", flush=True)

    # Precompute zones for each (exp_str, strike, ot, tf, day)
    print(f"\n[{SYMBOL}] Precomputing zones ...", flush=True)
    all_tfs = sorted(set(HTF_GRID) | set(MTF_GRID) | set(LTF_GRID))
    zones_cache: Dict[tuple, list] = {}   # (exp_str, strike, ot, tf, day_str) -> zones
    exec_cache:  Dict[tuple, Optional[dict]] = {}  # (exp_str, strike, ot, exec_min, day_str) -> arrays

    for (exp_str, strike, ot), df_full in option_bars.items():
        if df_full.empty:
            continue
        if df_full["datetime"].dt.tz is not None:
            df_full = df_full.copy()
            df_full["datetime"] = df_full["datetime"].dt.tz_localize(None)

        for d_str in all_days:
            d_start = pd.Timestamp(f"{d_str}T09:15:00")
            d_end   = pd.Timestamp(f"{d_str}T15:30:00")
            lb_start = d_start - pd.Timedelta(days=LOOKBACK)

            df_day = df_full[(df_full["datetime"] >= d_start) & (df_full["datetime"] <= d_end)].copy()
            df_lb  = df_full[(df_full["datetime"] >= lb_start) & (df_full["datetime"] < d_start)].copy()

            if len(df_day) < 30:
                continue

            combined = pd.concat([df_lb, df_day], ignore_index=True)

            for tf in all_tfs:
                bars  = _resample(combined, tf)
                zones = _get_zones(bars)
                if not zones:
                    zones = _get_zones(_resample(df_day, tf))
                zones_cache[(exp_str, strike, ot, tf, d_str)] = zones

            for exc in EXEC_GRID:
                df_ex = _resample(df_day, exc)
                if df_ex.empty:
                    exec_cache[(exp_str, strike, ot, exc, d_str)] = None
                    continue
                exec_cache[(exp_str, strike, ot, exc, d_str)] = {
                    "high":  df_ex["high"].to_numpy(dtype=np.float64),
                    "low":   df_ex["low"].to_numpy(dtype=np.float64),
                    "close": df_ex["close"].to_numpy(dtype=np.float64),
                }

    print(f"[{SYMBOL}] Precompute done", flush=True)

    # Build combos
    combos = [
        (htf, mtf, ltf, exc, sl, cap)
        for htf in HTF_GRID
        for mtf in MTF_GRID  if mtf < htf
        for ltf in LTF_GRID  if ltf < mtf
        for exc in EXEC_GRID if exc <= ltf
        for sl  in SL_GRID
        for cap in CAP_GRID
    ]
    total = len(combos)
    print(f"[{SYMBOL}] {total} combos ...", flush=True)

    results = []
    t0 = time.time()

    for idx, (htf_min, mtf_min, ltf_min, exec_min, sl_buf, cap_pts) in enumerate(combos):
        all_trades = []
        sl_hist: Dict[str, str] = {}

        for d_str in all_days:
            exp_str, _ = _get_expiry_for_day(d_str)
            if not exp_str:
                continue
            atm = _get_atm(d_str)
            if not atm:
                continue

            for offset in STRIKES_OFFSET:
                strike = atm + offset * STEP
                for ot in opt_sides:
                    key = (exp_str, strike, ot)
                    if key not in option_bars or option_bars[key].empty:
                        continue
                    htf_z  = zones_cache.get((exp_str, strike, ot, htf_min, d_str), [])
                    mtf_z  = zones_cache.get((exp_str, strike, ot, mtf_min, d_str), [])
                    ltf_z  = zones_cache.get((exp_str, strike, ot, ltf_min, d_str), [])
                    ex_arr = exec_cache.get((exp_str, strike, ot, exec_min, d_str))
                    if not htf_z or ex_arr is None:
                        continue
                    res = _run_cascade_day(d_str, ex_arr, htf_z, mtf_z, ltf_z,
                                           sl_buf, cap_pts, sl_hist, LOT)
                    if res:
                        res["strike"] = strike
                        res["opt"]    = ot
                        all_trades.append(res)
                        break   # one trade per day
                else:
                    continue
                break

        params = {"htf_min": htf_min, "mtf_min": mtf_min, "ltf_min": ltf_min,
                  "exec_min": exec_min, "sl_buf": sl_buf, "cap_pts": cap_pts,
                  "symbol": SYMBOL, "lot": LOT}
        results.append(_summarize(all_trades, params))

        if (idx + 1) % 30 == 0:
            el = time.time() - t0
            print(f"  {idx+1}/{total}  elapsed={el:.1f}s  ETA={el/(idx+1)*(total-idx-1):.1f}s",
                  flush=True)

    results.sort(key=lambda r: r["profit_factor"] if r["total"] >= 3 else -1, reverse=True)
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    pd.DataFrame(results).to_csv(OUT_CSV, index=False)
    print(f"\n[{SYMBOL}] Results -> {OUT_CSV}", flush=True)

    print(f"\n{'='*120}")
    print(f"  {SYMBOL} Option Chart Cascade — Top 25  ({START_DATE} to {END_DATE})")
    print(f"  BEAR trap in option premium -> BUY option  |  Lot={LOT}  Side={OPT_SIDE}")
    print(f"{'='*120}")
    print(f"{'Rank':>4}  {'HTF':>5}  {'MTF':>5}  {'LTF':>4}  {'Exc':>4}  {'SL':>4}  {'Cap':>4}  "
          f"{'#':>4}  {'Win%':>5}  {'PF':>7}  {'Net INR':>10}  {'AvgW':>8}  {'AvgL':>8}  "
          f"{'SLs':>4}  {'T1s':>4}  {'Cap':>4}  {'EOD':>4}")
    print(f"{'-'*120}")
    for rank, r in enumerate(results[:25], 1):
        print(f"{rank:>4}  {r['htf_min']:>4}m  {r['mtf_min']:>4}m  {r['ltf_min']:>3}m  "
              f"{r['exec_min']:>3}m  {r['sl_buf']:>4.0f}  {r['cap_pts']:>4.0f}  "
              f"{r['total']:>4}  {r['win_rate_pct']:>4.0f}%  {r['profit_factor']:>7.3f}  "
              f"{r['net_pnl_inr']:>10.2f}  {r['avg_win_inr']:>8.2f}  {r['avg_loss_inr']:>8.2f}  "
              f"{r['exits_sl']:>4}  {r['exits_t1']:>4}  {r['exits_cap']:>4}  {r['exits_eod']:>4}")

    print(f"\n[{SYMBOL}] Done in {time.time()-t0:.1f}s  |  Full CSV: {OUT_CSV}")
