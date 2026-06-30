"""
NSE/BANKNIFTY 4-Tier Cascade Backtest — Option Premium Chart + Sector Confirmation
====================================================================================
Period  : June 1 – June 30, 2026  (configurable)
Symbol  : NIFTY or BANKNIFTY      (configurable via argv[1])
Side    : CE | PE | BOTH           (configurable via argv[2])

Trap logic:
  1. OPTION PREMIUM chart  → 4-tier cascade (HTF -> MTF -> LTF -> Exec)
     BEAR trap in CE/PE premium -> BUY that option (premium squeezes up)
  2. SECTOR SPOT chart (bias filter) → HTF zone must agree in same direction
     NIFTY   : BANKNIFTY spot + NIFTYIT spot must show same BEAR/BULL bias
     BANKNIFTY: NIFTY50 spot must show same bias

Sector confirmation modes (compared in results):
  0 = no sector filter (baseline)
  1 = primary sector only (BANKNIFTY for NIFTY / NIFTY for BANKNIFTY)
  2 = primary + secondary sector (e.g. BANKNIFTY + NIFTYIT for NIFTY)

Data source: Upstox historical candle REST API (reads token from data/clients.db)

Usage:
  python3 scripts/nse_cascade_backtest.py [NIFTY|BANKNIFTY] [CE|PE|BOTH]
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
from strategies.trap_scanner import scanner

# ── Config ────────────────────────────────────────────────────────────────────

SYMBOL   = sys.argv[1].upper() if len(sys.argv) > 1 else "NIFTY"
OPT_SIDE = sys.argv[2].upper() if len(sys.argv) > 2 else "BOTH"

START_DATE = date(2026, 4,  1)
END_DATE   = date(2026, 6, 30)
LOOKBACK   = 5

DB_PATH   = os.path.join(_ROOT, "data", "clients.db")
CACHE_DIR = os.path.join(_ROOT, "data", "nse_option_cache")
OUT_CSV   = os.path.join(_ROOT, "data", f"{SYMBOL.lower()}_cascade_results.csv")

import calendar as _calendar

def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    """Return the last occurrence of weekday (0=Mon..6=Sun) in given month."""
    last = _calendar.monthrange(year, month)[1]
    d = date(year, month, last)
    while d.weekday() != weekday:
        d -= timedelta(days=1)
    return d

# Weekly expiry weekday per underlying (matches REGISTRY)
_EXPIRY_WEEKDAY = {
    "NIFTY": 1, "BANKNIFTY": 2, "FINNIFTY": 1,
    "MIDCPNIFTY": 0, "SENSEX": 1,
}

def _get_monthly_expiry(symbol: str, year: int, month: int) -> date:
    """Last weekly-expiry-weekday of the month = monthly expiry."""
    wd = _EXPIRY_WEEKDAY.get(symbol, 3)
    return _last_weekday_of_month(year, month, wd)

LOT_SIZES    = {"NIFTY": 25, "BANKNIFTY": 15, "FINNIFTY": 40, "SENSEX": 10}
STRIKE_STEPS = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50, "SENSEX": 100}

# Upstox raw instrument keys for spot indices (NOT URL-encoded — encoded at call site)
INDEX_KEY = {
    "NIFTY":     "NSE_INDEX|Nifty 50",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
    "NIFTYIT":   "NSE_INDEX|Nifty IT",
    "FINNIFTY":  "NSE_INDEX|Nifty Fin Service",
    "SENSEX":    "BSE_INDEX|SENSEX",
}

# Upstox underlying keys for option/contract API
UNDERLYING_KEY = {
    "NIFTY":     "NSE_INDEX|Nifty 50",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
    "FINNIFTY":  "NSE_INDEX|Nifty Fin Service",
    "SENSEX":    "BSE_INDEX|SENSEX",
}

# Sector confirmation indices per symbol
#   key: underlying → [primary_sector, secondary_sector]
#   Confirmation = same HTF BEAR zone exists in sector spot chart on that day
SECTORS = {
    "NIFTY":     ["BANKNIFTY", "NIFTYIT"],   # BankNifty 30% + IT 15%
    "BANKNIFTY": ["NIFTY"],                  # Parent index
    "FINNIFTY":  ["BANKNIFTY", "NIFTY"],
    "SENSEX":    ["NIFTY"],
}

LOT  = LOT_SIZES.get(SYMBOL, 25)
STEP = STRIKE_STEPS.get(SYMBOL, 50)

# Optimization grids
# SL_GRID / CAP_GRID = OPTION PREMIUM points (e.g. 20 = ₹20 move in ATM option).
# Internally converted to spot points: spot_sl = option_pts / ATM_delta (0.5) → ×2.
# So SL=20 → 40 spot pts, SL=100 → 200 spot pts (reasonable BANKNIFTY SL).
HTF_GRID  = [60, 120, 180, 240]
MTF_GRID  = [15, 30]
LTF_GRID  = [3, 5]
EXEC_GRID = [1, 3]
SL_GRID   = [20, 50, 100, 200]    # option premium points
CAP_GRID  = [0, 100, 200, 500]    # option premium points (0 = trailing only)

STRIKES_OFFSET = [-2, -1, 0, 1, 2]   # x STEP from ATM

UPSTOX_BASE = "https://api.upstox.com/v2"

# ── Token ─────────────────────────────────────────────────────────────────────

def _get_upstox_token() -> str:
    if not os.path.exists(DB_PATH):
        return ""
    try:
        conn = sqlite3.connect(DB_PATH)
        # system_feeder_creds has individual columns (not a JSON blob)
        row = conn.execute(
            "SELECT access_token FROM system_feeder_creds WHERE provider='upstox' LIMIT 1"
        ).fetchone()
        conn.close()
        if not row:
            return ""
        return row[0] or ""
    except Exception as exc:
        print(f"[WARN] token read error: {exc}", flush=True)
        return ""

# ── Upstox REST ───────────────────────────────────────────────────────────────

def _hdr(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

def _fetch_index_daily(sym: str, token: str, fr: date, to: date) -> pd.DataFrame:
    raw_key = INDEX_KEY.get(sym, f"NSE_INDEX|{sym}")
    enc     = _quote(raw_key, safe="")
    url = f"{UPSTOX_BASE}/historical-candle/{enc}/day/{to}/{fr}"
    r   = requests.get(url, headers=_hdr(token), timeout=15)
    if r.status_code != 200:
        return pd.DataFrame()
    candles = r.json().get("data", {}).get("candles", [])
    rows = [{"date": c[0][:10], "open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4])} for c in reversed(candles)]
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def _fetch_index_1m(sym: str, token: str, fr: date, to: date) -> pd.DataFrame:
    """Fetch 1m bars for a spot index — chunked in 28-day windows (Upstox limit)."""
    cache_f = os.path.join(CACHE_DIR, f"idx_{sym}_{fr}_{to}.parquet")
    os.makedirs(CACHE_DIR, exist_ok=True)
    if os.path.exists(cache_f):
        return pd.read_parquet(cache_f)
    raw_key = INDEX_KEY.get(sym, f"NSE_INDEX|{sym}")
    enc     = _quote(raw_key, safe="")
    all_rows = []
    chunk_fr = fr
    CHUNK = 28
    while chunk_fr <= to:
        chunk_to = min(chunk_fr + timedelta(days=CHUNK - 1), to, date.today())
        url = f"{UPSTOX_BASE}/historical-candle/{enc}/1minute/{chunk_to}/{chunk_fr}"
        r   = requests.get(url, headers=_hdr(token), timeout=20)
        time.sleep(0.35)
        if r.status_code != 200:
            print(f"  [WARN] {sym} 1m {chunk_fr}→{chunk_to} HTTP {r.status_code}: {r.text[:120]}",
                  flush=True)
        else:
            candles = r.json().get("data", {}).get("candles", [])
            all_rows.extend(
                {"datetime": c[0], "open": float(c[1]), "high": float(c[2]),
                 "low": float(c[3]), "close": float(c[4])}
                for c in reversed(candles)
            )
        chunk_fr = chunk_to + timedelta(days=1)
    if not all_rows:
        return pd.DataFrame()
    df = pd.DataFrame(all_rows).drop_duplicates("datetime")
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)
    df.to_parquet(cache_f, index=False)
    return df

_NSE_MASTER_CACHE: Optional[list] = None   # loaded once per run

def _load_nse_master() -> list:
    """Download Upstox public NSE master JSON (no auth). Cached in memory per run."""
    global _NSE_MASTER_CACHE
    if _NSE_MASTER_CACHE is not None:
        return _NSE_MASTER_CACHE
    cache_f = os.path.join(CACHE_DIR, f"nse_master_{date.today()}.json")
    os.makedirs(CACHE_DIR, exist_ok=True)
    if os.path.exists(cache_f):
        with open(cache_f) as f:
            _NSE_MASTER_CACHE = json.load(f)
        print(f"  [master] loaded {len(_NSE_MASTER_CACHE):,} instruments from cache", flush=True)
        return _NSE_MASTER_CACHE
    import gzip, io
    print("  [master] Downloading NSE instruments master JSON ...", flush=True)
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    r   = requests.get(url, timeout=30)
    if r.status_code != 200:
        print(f"  [WARN] NSE master HTTP {r.status_code}", flush=True)
        _NSE_MASTER_CACHE = []
        return []
    data = json.loads(gzip.decompress(r.content))
    with open(cache_f, "w") as f:
        json.dump(data, f)
    _NSE_MASTER_CACHE = data
    print(f"  [master] {len(data):,} instruments downloaded", flush=True)
    return data

def _get_option_contracts(symbol: str, expiry_date: date) -> Dict[Tuple[int, str], str]:
    """
    Parse the NSE master instruments JSON to get REAL instrument keys
    for (strike, CE/PE) for a given symbol and expiry date.
    Returns {(strike, 'CE'|'PE'): instrument_key}.
    Works for both current and recently-expired contracts.
    """
    cache_f = os.path.join(CACHE_DIR, f"contracts_{symbol}_{expiry_date}.json")
    os.makedirs(CACHE_DIR, exist_ok=True)
    if os.path.exists(cache_f):
        with open(cache_f) as f:
            raw = json.load(f)
        if raw:   # empty cache = no contracts found, re-try
            return {(int(k.split("|")[0]), k.split("|")[1]): v for k, v in raw.items()}

    master = _load_nse_master()
    result: Dict[Tuple[int, str], str] = {}
    exp_str = expiry_date.isoformat()   # "2026-06-24"

    for inst in master:
        # Filter: underlying symbol + option type + matching expiry
        name = str(inst.get("trading_symbol", "") or inst.get("name", "")).upper()
        itype = str(inst.get("instrument_type", "")).upper()
        if symbol not in name:
            continue
        if itype not in ("CE", "PE", "CALL", "PUT"):
            continue
        # Expiry field varies: epoch ms, ISO string, or DDMONYY
        exp_raw = inst.get("expiry") or inst.get("expiry_date") or ""
        if not exp_raw:
            continue
        # Normalise to ISO date
        try:
            if isinstance(exp_raw, (int, float)):
                exp_d = datetime.utcfromtimestamp(int(exp_raw) / 1000).date()
            elif len(str(exp_raw)) == 10 and "-" in str(exp_raw):
                exp_d = date.fromisoformat(str(exp_raw))
            elif len(str(exp_raw)) == 7:   # epoch seconds
                exp_d = datetime.utcfromtimestamp(int(exp_raw)).date()
            else:
                continue
        except Exception:
            continue
        if exp_d != expiry_date:
            continue
        ikey   = inst.get("instrument_key", "")
        strike = inst.get("strike_price") or inst.get("strike") or 0
        try:
            strike = int(float(strike))
        except Exception:
            continue
        if not ikey or not strike:
            continue
        ot = "CE" if "CE" in itype or "CALL" in itype else "PE"
        result[(strike, ot)] = ikey

    with open(cache_f, "w") as f:
        json.dump({f"{k[0]}|{k[1]}": v for k, v in result.items()}, f)
    return result

def _fetch_option_1m(instrument_key: str, label: str,
                     token: str, fr: date, to: date) -> pd.DataFrame:
    """Fetch 1m historical bars using the REAL Upstox instrument key."""
    enc_key = _quote(instrument_key, safe="")
    cache_f = os.path.join(CACHE_DIR, f"bars_{label}_{fr}_{to}.parquet")
    os.makedirs(CACHE_DIR, exist_ok=True)
    if os.path.exists(cache_f):
        return pd.read_parquet(cache_f)
    to_use = min(to, date.today())
    url = f"{UPSTOX_BASE}/historical-candle/{enc_key}/1minute/{to_use}/{fr}"
    r   = requests.get(url, headers=_hdr(token), timeout=20)
    time.sleep(0.3)
    if r.status_code != 200:
        print(f"  [WARN] {label} HTTP {r.status_code}: {r.text[:100]}", flush=True)
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
    df = df.copy()
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_localize(None)
    agg = {"open": "first", "high": "max", "low": "min", "close": "last"}
    out = (df.set_index("datetime")
             .resample(f"{minutes}min", closed="left", label="left")[list(agg)]
             .agg(agg)
             .dropna(subset=["open"])
             .reset_index())
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

def _simulate_numpy(H, L, C, entry, init_sl, t1, sl_buf, cap_pts, size) -> dict:
    active_sl = init_sl
    for i in range(len(H)):
        h, l, c = float(H[i]), float(L[i]), float(C[i])
        run       = c - entry
        new_trail = h - sl_buf
        if new_trail > active_sl:
            active_sl = new_trail
        if cap_pts > 0 and run >= cap_pts:
            return {"pnl": round(run * size, 2), "exit_reason": "CAP"}
        if l <= active_sl:
            pnl = (active_sl - entry) * size
            return {"pnl": round(pnl, 2), "exit_reason": "SL"}
        if h >= t1:
            pnl = (t1 - entry) * size
            return {"pnl": round(pnl, 2), "exit_reason": "T1"}
    ep  = float(C[-1]) if len(C) > 0 else entry
    return {"pnl": round((ep - entry) * size, 2), "exit_reason": "EOD"}

def _find_exec_entry(exec_arr, ltf_zone, htf_zone, sl_buf, cap_pts, lot) -> Optional[dict]:
    ltf_l, ltf_h = _eff_zone(ltf_zone)
    buf  = max((ltf_h - ltf_l) * 0.15, 1.0)
    t1   = float(htf_zone.get("sl", 0))
    if t1 <= 0:
        return None
    H, L, C = exec_arr["high"], exec_arr["low"], exec_arr["close"]
    n = len(H)
    if n < 2:
        return None
    in_zone = (C >= ltf_l - buf) & (C <= ltf_h + buf)
    idxs    = np.where(in_zone)[0]
    idxs    = idxs[idxs < n - 1]
    for i in idxs:
        trig     = float(H[i])
        entry_sl = float(L[i]) - sl_buf
        if t1 <= trig or entry_sl >= trig:
            continue
        hit = np.where(H[i+1:] >= trig)[0]
        if not len(hit):
            continue
        j    = hit[0]
        res  = _simulate_numpy(H[i+1:][j:], L[i+1:][j:], C[i+1:][j:],
                               trig, entry_sl, t1, sl_buf, cap_pts, lot)
        res["entry_price"] = round(trig, 2)
        res["t1"]          = round(t1, 2)
        return res
    return None

# ── Sector confirmation check ─────────────────────────────────────────────────

def _sector_confirms(sector_zones_day: Dict[str, list], htf_min: int,
                     kind: str, n_sectors: int) -> bool:
    """
    Check if n_sectors of the sector indices show a zone of the same kind (BEAR/BULL)
    on the same HTF timeframe on that day.
    n_sectors=0 → always True (no filter).
    """
    if n_sectors == 0:
        return True
    sectors = SECTORS.get(SYMBOL, [])
    confirmed = 0
    for sec in sectors[:n_sectors]:
        zones = sector_zones_day.get((sec, htf_min), [])
        if any(z.get("kind") == kind for z in zones):
            confirmed += 1
    return confirmed >= n_sectors

# ── Per-day cascade ───────────────────────────────────────────────────────────

def _run_cascade_day(d_str, exec_arr, htf_zones, mtf_zones, ltf_zones,
                     sl_buf, cap_pts, sl_hist, lot,
                     sector_zones_day, htf_min, n_sectors) -> Optional[dict]:
    for htf_z in htf_zones:
        kind = htf_z.get("kind", "BEAR")
        if kind != "BEAR":   # option chart: only BEAR trap -> BUY
            continue
        hl, hh   = _eff_zone(htf_z)
        zone_key = f"{hl:.1f}-{hh:.1f}"
        t1       = float(htf_z.get("sl", 0))
        if t1 <= hh:
            continue
        if zone_key in sl_hist:
            if (date.fromisoformat(d_str) - date.fromisoformat(sl_hist[zone_key])).days <= 1:
                continue
        # ── Sector confirmation ──
        if not _sector_confirms(sector_zones_day, htf_min, kind, n_sectors):
            continue
        mtf_m = next((z for z in mtf_zones if z.get("kind") == kind and _zones_overlap(htf_z, z)), None)
        if not mtf_m:
            continue
        ltf_m = next((z for z in ltf_zones if z.get("kind") == kind and _zones_overlap(mtf_m, z)), None)
        if not ltf_m:
            continue
        res = _find_exec_entry(exec_arr, ltf_m, htf_z, sl_buf, cap_pts, lot)
        if res:
            res.update({"date": d_str, "zone_key": zone_key})
            if res["exit_reason"] == "SL":
                sl_hist[zone_key] = d_str
            return res
    return None

# ── Summary ───────────────────────────────────────────────────────────────────

def _summarize(trades, params) -> dict:
    wins   = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    gp = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    pf = round(gp / gl, 3) if gl > 0 else (9999.0 if gp > 0 else 0.0)
    s = {
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

def _get_expiry_for_day(d_str: str) -> date:
    """Monthly expiry that covers this trading day."""
    d = date.fromisoformat(d_str)
    exp = _get_monthly_expiry(SYMBOL, d.year, d.month)
    if d > exp:
        nm = d.month % 12 + 1
        ny = d.year + (1 if d.month == 12 else 0)
        exp = _get_monthly_expiry(SYMBOL, ny, nm)
    return exp


def _build_option_bars_for_day(d_str: str, expiry: date,
                                contracts: Dict[Tuple[int, str], str],
                                daily_map: dict, token: str,
                                fetch_fr: date, fetch_to: date,
                                option_bar_cache: dict) -> Optional[pd.DataFrame]:
    """
    Return combined CE+PE ATM option 1m bar series for this day.
    Tries ATM, then ATM±1 step (mirror of trap scanner strike selection).
    Returns the first strike pair that has data, or None.
    P&L unit: raw option premium (no delta scaling needed — already priced).
    """
    atm = _get_atm_close(d_str, daily_map)
    if not atm:
        return None
    for offset in [0, -1, 1]:    # ATM → 1 ITM → 1 OTM
        ce_strike = atm + offset * STEP
        pe_strike = atm - offset * STEP
        ce_key = contracts.get((ce_strike, "CE"))
        pe_key = contracts.get((pe_strike, "PE"))
        if not ce_key or not pe_key:
            continue
        for key, label in [(ce_key, f"{SYMBOL}CE{ce_strike}"), (pe_key, f"{SYMBOL}PE{pe_strike}")]:
            if (key, d_str) not in option_bar_cache:
                cache_f = os.path.join(CACHE_DIR, f"bars_{label}_{fetch_fr}_{fetch_to}.parquet")
                if os.path.exists(cache_f):
                    df_c = pd.read_parquet(cache_f)
                else:
                    df_c = _fetch_option_1m(key, label, token, fetch_fr, fetch_to)
                    if not df_c.empty:
                        df_c.to_parquet(cache_f, index=False)
                option_bar_cache[(key, d_str)] = df_c
        ce_full = option_bar_cache.get((ce_key, d_str), pd.DataFrame())
        pe_full = option_bar_cache.get((pe_key, d_str), pd.DataFrame())
        d_s  = pd.Timestamp(f"{d_str}T09:15:00")
        d_e  = pd.Timestamp(f"{d_str}T15:30:00")
        ce_day = ce_full[(ce_full["datetime"] >= d_s) & (ce_full["datetime"] <= d_e)] if not ce_full.empty else pd.DataFrame()
        pe_day = pe_full[(pe_full["datetime"] >= d_s) & (pe_full["datetime"] <= d_e)] if not pe_full.empty else pd.DataFrame()
        if len(ce_day) < 30 and len(pe_day) < 30:
            continue
        # Use whichever leg has more data; prefer CE for BEAR trap (buying CE premium)
        return ce_day if len(ce_day) >= len(pe_day) else pe_day
    return None


def _get_atm_close(d_str: str, daily_map: dict) -> int:
    """ATM = prev-day close rounded to STEP."""
    prev_d = (date.fromisoformat(d_str) - timedelta(days=1)).isoformat()
    for _ in range(7):
        if prev_d in daily_map:
            return int(round(float(daily_map[prev_d]["close"]) / STEP) * STEP)
        prev_d = (date.fromisoformat(prev_d) - timedelta(days=1)).isoformat()
    return 0


if __name__ == "__main__":
    print(f"=== {SYMBOL} Cascade Backtest (hybrid: spot proxy + real July options) ===", flush=True)
    print(f"    Period: {START_DATE} -> {END_DATE}  |  Lot: {LOT}  |  ATM delta: 0.5 (spot proxy)", flush=True)

    token = _get_upstox_token()
    if not token:
        print("[ERROR] No Upstox token in DB. Ensure live system ran at least once today.", flush=True)
        sys.exit(1)
    print("[OK] Upstox token loaded", flush=True)

    fetch_fr  = START_DATE - timedelta(days=LOOKBACK + 5)
    fetch_fr2 = START_DATE - timedelta(days=LOOKBACK + 1)

    # ── Daily bars — trading-day calendar + ATM computation ───────────────────
    print(f"[{SYMBOL}] Fetching daily index bars ...", flush=True)
    daily_df = _fetch_index_daily(SYMBOL, token, fetch_fr, END_DATE)
    if daily_df.empty:
        print("[ERROR] Could not fetch daily index data. Check token.", flush=True)
        sys.exit(1)
    daily_df["date"] = daily_df["date"].astype(str)
    daily_map        = {r["date"]: r for _, r in daily_df.iterrows()}
    trading_day_set  = set(daily_map.keys())
    all_days = [d.isoformat() for d in
                (START_DATE + timedelta(i) for i in range((END_DATE - START_DATE).days + 1))
                if d.isoformat() in trading_day_set]
    print(f"[{SYMBOL}] {len(all_days)} trading days in window", flush=True)

    # ── July option contracts (active expiry → real instrument keys available) ─
    FUTURE_CUTOFF = date.today()   # days >= today use real option bars
    july_contracts: Dict[date, Dict[Tuple[int, str], str]] = {}
    july_expiries_needed = set(
        _get_expiry_for_day(d) for d in all_days
        if date.fromisoformat(d) >= FUTURE_CUTOFF
    )
    if july_expiries_needed:
        print(f"[{SYMBOL}] Loading July option contracts for: {sorted(july_expiries_needed)} ...", flush=True)
        # Delete stale NSE master cache so we get today's active contracts
        for f in os.listdir(CACHE_DIR) if os.path.isdir(CACHE_DIR) else []:
            if f.startswith("nse_master_") and not f.endswith(f"{date.today()}.json"):
                try: os.remove(os.path.join(CACHE_DIR, f))
                except Exception: pass
        for exp in sorted(july_expiries_needed):
            c = _get_option_contracts(SYMBOL, exp)
            july_contracts[exp] = c
            print(f"  {exp}: {len(c)} contracts", flush=True)

    option_bar_cache: dict = {}   # (instrument_key, d_str) -> DataFrame

    # ── SPOT 1m bars (proxy for historical April-June days) ───────────────────
    print(f"[{SYMBOL}] Downloading SPOT 1m bars (proxy for historical days) ...", flush=True)
    df_spot = _fetch_index_1m(SYMBOL, token, fetch_fr2, END_DATE)
    if df_spot.empty:
        print("[ERROR] No spot 1m data", flush=True)
        sys.exit(1)
    if df_spot["datetime"].dt.tz is not None:
        df_spot["datetime"] = df_spot["datetime"].dt.tz_localize(None)
    print(f"  {SYMBOL} spot: {len(df_spot)} bars", flush=True)

    # ── Sector index 1m bars ──────────────────────────────────────────────────
    sector_list = SECTORS.get(SYMBOL, [])
    sector_df: Dict[str, pd.DataFrame] = {}
    if sector_list:
        print(f"[{SYMBOL}] Downloading sector indices: {sector_list} ...", flush=True)
        for sec in sector_list:
            df_sec = _fetch_index_1m(sec, token, fetch_fr2, END_DATE)
            if df_sec.empty:
                print(f"  [WARN] No data for sector {sec}", flush=True)
            else:
                if df_sec["datetime"].dt.tz is not None:
                    df_sec["datetime"] = df_sec["datetime"].dt.tz_localize(None)
                sector_df[sec] = df_sec
                print(f"  {sec}: {len(df_sec)} bars", flush=True)

    # ── Precompute zones + exec arrays per (tf, day) ──────────────────────────
    # For July days with real option bars: use option premium series.
    # For Apr–Jun days: use SPOT as proxy (P&L scaled by ATM_delta later).
    print(f"\n[{SYMBOL}] Precomputing zones ...", flush=True)
    all_tfs   = sorted(set(HTF_GRID) | set(MTF_GRID) | set(LTF_GRID))
    ATM_DELTA = 0.50

    zones_cache: Dict[tuple, list] = {}
    exec_cache:  Dict[tuple, Optional[dict]] = {}
    day_mode:    Dict[str, str] = {}   # d_str -> "option" | "spot"

    for d_str in all_days:
        d_s  = pd.Timestamp(f"{d_str}T09:15:00")
        d_e  = pd.Timestamp(f"{d_str}T15:30:00")
        lb_s = d_s - pd.Timedelta(days=LOOKBACK)

        # Try real option bars first (July days)
        df_src = None
        if date.fromisoformat(d_str) >= FUTURE_CUTOFF:
            expiry = _get_expiry_for_day(d_str)
            contracts = july_contracts.get(expiry, {})
            if contracts:
                df_opt = _build_option_bars_for_day(
                    d_str, expiry, contracts, daily_map,
                    token, fetch_fr2, END_DATE, option_bar_cache)
                if df_opt is not None and not df_opt.empty:
                    if df_opt["datetime"].dt.tz is not None:
                        df_opt["datetime"] = df_opt["datetime"].dt.tz_localize(None)
                    df_src = df_opt
                    day_mode[d_str] = "option"

        # Fall back to spot proxy
        if df_src is None:
            df_day_spot = df_spot[(df_spot["datetime"] >= d_s) & (df_spot["datetime"] <= d_e)].copy()
            df_lb_spot  = df_spot[(df_spot["datetime"] >= lb_s) & (df_spot["datetime"] < d_s)].copy()
            if len(df_day_spot) < 30:
                continue
            df_src = pd.concat([df_lb_spot, df_day_spot], ignore_index=True)
            day_mode[d_str] = "spot"

        # Build zones for all TFs
        if day_mode.get(d_str) == "option":
            df_day_src = df_src
            df_lb_src  = pd.DataFrame()   # no lookback for live option bars
        else:
            d_s2 = pd.Timestamp(f"{d_str}T09:15:00")
            df_day_src = df_src[(df_src["datetime"] >= d_s2)] if "datetime" in df_src else df_src
            df_lb_src  = df_src[(df_src["datetime"] < d_s2)]  if "datetime" in df_src else pd.DataFrame()

        combined = df_src   # already has lookback if spot; just today if option
        for tf in all_tfs:
            bars  = _resample(combined, tf)
            zones = _get_zones(bars)
            if not zones and day_mode.get(d_str) == "option":
                zones = _get_zones(_resample(df_src, tf))
            zones_cache[(tf, d_str)] = zones

        # Exec arrays — always from today's session only
        if day_mode.get(d_str) == "option":
            df_exec_src = df_src
        else:
            d_s2 = pd.Timestamp(f"{d_str}T09:15:00")
            df_exec_src = df_src[(df_src["datetime"] >= d_s2)] if "datetime" in df_src else df_src

        for exc in EXEC_GRID:
            df_ex = _resample(df_exec_src, exc)
            if df_ex.empty:
                exec_cache[(exc, d_str)] = None
                continue
            exec_cache[(exc, d_str)] = {
                "high":  df_ex["high"].to_numpy(dtype=np.float64),
                "low":   df_ex["low"].to_numpy(dtype=np.float64),
                "close": df_ex["close"].to_numpy(dtype=np.float64),
            }

    opt_days  = sum(1 for v in day_mode.values() if v == "option")
    spot_days = sum(1 for v in day_mode.values() if v == "spot")

    # ── Precompute sector zones ─────────────────────────────────────────────────
    sector_zones: Dict[tuple, list] = {}
    if sector_df:
        print(f"[{SYMBOL}] Precomputing sector confirmation zones ...", flush=True)
        for sec, df_sec in sector_df.items():
            for d_str in all_days:
                d_s  = pd.Timestamp(f"{d_str}T09:15:00")
                d_e  = pd.Timestamp(f"{d_str}T15:30:00")
                lb_s = d_s - pd.Timedelta(days=LOOKBACK)
                df_day = df_sec[(df_sec["datetime"] >= d_s) & (df_sec["datetime"] <= d_e)].copy()
                df_lb  = df_sec[(df_sec["datetime"] >= lb_s) & (df_sec["datetime"] < d_s)].copy()
                if len(df_day) < 30:
                    continue
                combined = pd.concat([df_lb, df_day], ignore_index=True)
                for tf in HTF_GRID:
                    bars  = _resample(combined, tf)
                    zones = _get_zones(bars)
                    if not zones:
                        zones = _get_zones(_resample(df_day, tf))
                    sector_zones[(sec, tf, d_str)] = zones

    print(f"[{SYMBOL}] Precompute done — {len(all_days)} days "
          f"({spot_days} spot-proxy, {opt_days} real-option)", flush=True)

    # ── Combos ─────────────────────────────────────────────────────────────────
    combos = [
        (htf, mtf, ltf, exc, sl, cap, nsec)
        for htf  in HTF_GRID
        for mtf  in MTF_GRID   if mtf  < htf
        for ltf  in LTF_GRID   if ltf  < mtf
        for exc  in EXEC_GRID  if exc  <= ltf
        for sl   in SL_GRID
        for cap  in CAP_GRID
        for nsec in [0, 1, 2]
    ]
    total = len(combos)
    print(f"[{SYMBOL}] {total} combos ...", flush=True)

    results = []
    t0 = time.time()

    for idx, (htf_min, mtf_min, ltf_min, exec_min, sl_buf, cap_pts, n_sec) in enumerate(combos):
        all_trades = []
        sl_hist: Dict[str, str] = {}

        for d_str in all_days:
            # Build sector_zones_day for this day + htf
            sector_zones_day: Dict[tuple, list] = {}
            for sec in sector_list:
                sector_zones_day[(sec, htf_min)] = sector_zones.get((sec, htf_min, d_str), [])

            htf_z  = zones_cache.get((htf_min, d_str), [])
            mtf_z  = zones_cache.get((mtf_min, d_str), [])
            ltf_z  = zones_cache.get((ltf_min, d_str), [])
            ex_arr = exec_cache.get((exec_min, d_str))
            if not htf_z or ex_arr is None:
                continue

            # sl_buf / cap_pts are in option premium points.
            # Spot-proxy days: convert to spot units (÷ delta); PnL scaled by delta×lot.
            # Real option days: use as-is; PnL = premium_move × lot.
            if day_mode.get(d_str) == "option":
                sim_sl  = sl_buf
                sim_cap = cap_pts if cap_pts > 0 else 0
                sim_lot = float(LOT)
            else:
                sim_sl  = sl_buf  / ATM_DELTA
                sim_cap = cap_pts / ATM_DELTA if cap_pts > 0 else 0
                sim_lot = ATM_DELTA * LOT
            res = _run_cascade_day(d_str, ex_arr, htf_z, mtf_z, ltf_z,
                                   sim_sl, sim_cap, sl_hist, sim_lot,
                                   sector_zones_day, htf_min, n_sec)
            if res:
                all_trades.append(res)

        params = {"htf_min": htf_min, "mtf_min": mtf_min, "ltf_min": ltf_min,
                  "exec_min": exec_min, "sl_buf": sl_buf, "cap_pts": cap_pts,
                  "sector_confirm": n_sec, "symbol": SYMBOL, "lot": LOT}
        results.append(_summarize(all_trades, params))

        if (idx + 1) % 50 == 0:
            el = time.time() - t0
            print(f"  {idx+1}/{total}  elapsed={el:.1f}s  ETA={el/(idx+1)*(total-idx-1):.1f}s",
                  flush=True)

    results.sort(key=lambda r: r["profit_factor"] if r["total"] >= 3 else -1, reverse=True)
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    pd.DataFrame(results).to_csv(OUT_CSV, index=False)

    print(f"\n{'='*130}")
    print(f"  {SYMBOL} Option Chart Cascade — Top 30  ({START_DATE} to {END_DATE})")
    print(f"  Sector confirm: 0=none, 1=primary({SECTORS.get(SYMBOL,['?'])[0] if SECTORS.get(SYMBOL) else '?'}), "
          f"2=primary+secondary")
    print(f"{'='*130}")
    print(f"{'Rank':>4}  {'HTF':>5}  {'MTF':>5}  {'LTF':>4}  {'Exc':>4}  "
          f"{'SL':>4}  {'Cap':>4}  {'Sec':>3}  "
          f"{'#':>4}  {'Win%':>5}  {'PF':>7}  {'Net INR':>10}  "
          f"{'AvgW':>8}  {'AvgL':>8}  {'SLs':>4}  {'T1s':>4}  {'EOD':>4}")
    print(f"{'-'*130}")
    for rank, r in enumerate(results[:30], 1):
        print(f"{rank:>4}  {r['htf_min']:>4}m  {r['mtf_min']:>4}m  {r['ltf_min']:>3}m  "
              f"{r['exec_min']:>3}m  {r['sl_buf']:>4.0f}  {r['cap_pts']:>4.0f}  "
              f"{r['sector_confirm']:>3}  "
              f"{r['total']:>4}  {r['win_rate_pct']:>4.0f}%  {r['profit_factor']:>7.3f}  "
              f"{r['net_pnl_inr']:>10.2f}  {r['avg_win_inr']:>8.2f}  {r['avg_loss_inr']:>8.2f}  "
              f"{r['exits_sl']:>4}  {r['exits_t1']:>4}  {r['exits_eod']:>4}")

    print(f"\n[{SYMBOL}] Done in {time.time()-t0:.1f}s  |  CSV: {OUT_CSV}")
