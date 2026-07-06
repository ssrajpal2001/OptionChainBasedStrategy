"""
scripts/nse_gap_split_backtest.py
==================================
Splits every trade day into GAP vs NORMAL and compares:
  - Win rate, PF, net P&L for each category per index
  - GAP = |today_open - prev_close| / prev_close >= gap_thresh (%)
  - NORMAL = all other days

Uses the same June 2026 option bars already cached from nse_ob_option_backtest.py.
HTF=75m / MTF=15m / LTF=5m (no OB gate — isolate gap effect only).

Run: python scripts/nse_gap_split_backtest.py
"""
from __future__ import annotations

import os, sys, sqlite3
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from strategies.trap_scanner import scanner

START_DATE = date(2026, 6, 1)
END_DATE   = date(2026, 6, 30)

HTF_MIN  = 75
MTF_MIN  = 15
LTF_MIN  = 5
EXEC_MIN = 5
SL_BUF   = 10.0
CAP_PTS  = 200

# Gap thresholds to test
GAP_THRESHOLDS = [0.3, 0.5, 0.8, 1.0]

SYMBOLS      = ["NIFTY", "BANKNIFTY", "SENSEX"]
LOT_SIZES    = {"NIFTY": 25, "BANKNIFTY": 15, "SENSEX": 10}
STRIKE_STEPS = {"NIFTY": 50, "BANKNIFTY": 100, "SENSEX": 100}
JULY_EXPIRY  = {
    "NIFTY":     date(2026, 7, 28),
    "BANKNIFTY": date(2026, 7, 28),
    "SENSEX":    date(2026, 7, 30),
}
ZONE_CUTOFF = "15:14"
CACHE_DIR   = os.path.join(ROOT, "data", "nse_option_cache")

# ── helpers ───────────────────────────────────────────────────────────────────
def _get_token() -> str:
    try:
        conn = sqlite3.connect(os.path.join(ROOT, "data", "clients.db"))
        row = conn.execute(
            "SELECT access_token FROM system_feeder_creds WHERE provider='upstox' LIMIT 1"
        ).fetchone()
        conn.close()
        return (row[0] or "") if row else ""
    except Exception:
        return ""

def _zone_kind(z: dict) -> str:
    sl, zh, zl = z.get("sl",0), z.get("zone_high",0), z.get("zone_low",0)
    if sl > zh: return "BEAR"
    if sl < zl: return "BULL"
    return "UNKNOWN"

def _get_zones(df: pd.DataFrame) -> list:
    if len(df) < 3:
        return []
    try:
        _, zones = scanner.scan_htf(df)
        return zones or []
    except Exception:
        return []

def _zones_overlap(a: dict, b: dict) -> bool:
    al, ah = float(a.get("zone_low",0)), float(a.get("zone_high",0))
    bl, bh = float(b.get("zone_low",0)), float(b.get("zone_high",0))
    buf = max((ah-al)*0.15, 0.5)
    return ah+buf >= bl and bh+buf >= al

def _resample(df1m: pd.DataFrame, minutes: int, cutoff: str = None) -> pd.DataFrame:
    if df1m.empty or len(df1m) < 2:
        return pd.DataFrame()
    df = df1m.copy()
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_localize(None)
    if cutoff:
        try:
            h, m = cutoff.split(":")
            from datetime import time as dtime
            t_cut = dtime(int(h), int(m))
            df = df[df["datetime"].dt.time <= t_cut]
        except Exception:
            pass
    r = (df.set_index("datetime")[["open","high","low","close"]]
          .resample(f"{minutes}min", closed="left", label="left")
          .agg({"open":"first","high":"max","low":"min","close":"last"})
          .dropna(subset=["close"]).reset_index())
    return r

def _simulate(df_exec: pd.DataFrame, ltf_zone: dict, htf_zone: dict,
              sl_buf: float, cap_pts: int, lot: int) -> Optional[dict]:
    ltf_l = float(ltf_zone.get("zone_low", 0))
    ltf_h = float(ltf_zone.get("zone_high", 0))
    htf_l = float(htf_zone.get("zone_low", 0))
    t1    = float(htf_zone.get("sl", 0))
    sl    = htf_l - sl_buf
    if t1 <= 0 or sl <= 0 or t1 <= ltf_h:
        return None
    H = df_exec["high"].values.astype(float)
    L = df_exec["low"].values.astype(float)
    C = df_exec["close"].values.astype(float)
    buf = max((ltf_h-ltf_l)*0.15, 0.5)
    in_zone = (C >= ltf_l-buf) & (C <= ltf_h+buf)
    idxs = np.where(in_zone)[0]
    idxs = idxs[idxs < len(H)-1]
    for i in idxs:
        trig = float(H[i])
        if t1 <= trig or sl >= trig:
            continue
        hit = np.where(H[i+1:] >= trig)[0]
        if not len(hit):
            continue
        j = hit[0]
        Hs = H[i+1+j:]; Ls = L[i+1+j:]; Cs = C[i+1+j:]
        for k in range(len(Hs)):
            if Ls[k] <= sl:
                return {"pnl": round((sl-trig)*lot, 2), "exit": "SL"}
            target = trig+cap_pts if cap_pts > 0 else t1
            if Hs[k] >= min(t1, target):
                return {"pnl": round((min(t1,target)-trig)*lot, 2), "exit": "T1"}
        return {"pnl": round((float(Cs[-1])-trig)*lot, 2), "exit": "EOD"}
    return None

def _load_opt_cache(sym: str) -> Dict[str, pd.DataFrame]:
    """Load all cached option bar parquets for this symbol into memory."""
    import glob
    result = {}
    pattern = os.path.join(CACHE_DIR, f"opt_{sym}*.parquet")
    for f in glob.glob(pattern):
        try:
            df = pd.read_parquet(f)
            df["datetime"] = pd.to_datetime(df["datetime"])
            if df["datetime"].dt.tz is not None:
                df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
            label = os.path.basename(f).replace("opt_","").split("_2026")[0]
            result[label] = df
        except Exception:
            pass
    return result

def _get_day_bars(opt_cache: Dict[str, pd.DataFrame], sym: str,
                  atm: int, otype: str, step: int, day: date) -> pd.DataFrame:
    d_s = pd.Timestamp(f"{day}T09:15:00")
    d_e = pd.Timestamp(f"{day}T15:30:00")
    for offset in [0, 1, -1]:
        strike = atm + offset*step if otype == "CE" else atm - offset*step
        label  = f"{sym}{otype}{strike}"
        full   = opt_cache.get(label, pd.DataFrame())
        if full.empty:
            continue
        day_df = full[(full["datetime"] >= d_s) & (full["datetime"] <= d_e)].copy()
        if len(day_df) >= 30:
            return day_df
    return pd.DataFrame()

def _get_spot_daily(sym: str, token: str) -> Dict[str, float]:
    """Fetch D1 spot bars — open and close per day."""
    import requests, json
    from urllib.parse import quote as _quote
    cache_f = os.path.join(CACHE_DIR, f"daily_{sym}.json")
    if os.path.exists(cache_f):
        with open(cache_f) as f:
            data = json.load(f)
        if data:
            return data
    index_keys = {
        "NIFTY":     "NSE_INDEX|Nifty 50",
        "BANKNIFTY": "NSE_INDEX|Nifty Bank",
        "SENSEX":    "BSE_INDEX|SENSEX",
    }
    key = index_keys[sym]
    enc = _quote(key, safe="")
    url = f"https://api.upstox.com/v2/historical-candle/{enc}/day/{END_DATE}/{START_DATE - timedelta(days=10)}"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}, timeout=15)
    if r.status_code != 200:
        return {}
    candles = r.json().get("data", {}).get("candles", [])
    result = {}
    for c in reversed(candles):
        try:
            dt = pd.to_datetime(c[0])
            d_str = str(dt.date())
            result[d_str] = {"open": float(c[1]), "close": float(c[4])}
        except Exception:
            pass
    with open(cache_f, "w") as f:
        json.dump(result, f)
    return result

# ── main per-symbol analysis ──────────────────────────────────────────────────
def run_symbol(sym: str, token: str, gap_thresh: float) -> dict:
    lot  = LOT_SIZES[sym]
    step = STRIKE_STEPS[sym]

    # Load OHLC daily cache (open + close for gap detection)
    ohlc_f = os.path.join(CACHE_DIR, f"daily_ohlc_{sym}.json")
    import json
    with open(ohlc_f) as f:
        daily_spot = json.load(f)
    opt_cache  = _load_opt_cache(sym)

    if not opt_cache:
        print(f"  {sym}: no option cache found — run nse_ob_option_backtest.py first")
        return {}

    days = []
    d = START_DATE
    while d <= END_DATE:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)

    gap_trades: List[dict] = []
    normal_trades: List[dict] = []
    sl_hist: Dict[str, date] = {}
    gap_days, normal_days = 0, 0

    for day in days:
        d_str = str(day)

        # Get today open + prev close for gap computation
        today_data = daily_spot.get(d_str, {})
        today_open = today_data.get("open") if today_data else None

        prev_d = day - timedelta(days=1)
        prev_close = None
        for _ in range(5):
            prev_data = daily_spot.get(str(prev_d), {})
            if prev_data:
                prev_close = prev_data.get("close")
                break
            prev_d -= timedelta(days=1)

        is_gap = False
        if today_open and prev_close and prev_close > 0:
            gap_pct = abs(today_open - prev_close) / prev_close * 100
            is_gap  = gap_pct >= gap_thresh

        if is_gap:
            gap_days += 1
        else:
            normal_days += 1

        # ATM from prev close
        atm = int(round(float(prev_close or 0) / step) * step) if prev_close else 0
        if not atm:
            continue

        for opt_type in ("CE", "PE"):
            opt_df = _get_day_bars(opt_cache, sym, atm, opt_type, step, day)
            if len(opt_df) < 30:
                continue

            htf = _resample(opt_df, HTF_MIN, ZONE_CUTOFF)
            mtf = _resample(opt_df, MTF_MIN, ZONE_CUTOFF)
            ltf = _resample(opt_df, LTF_MIN, ZONE_CUTOFF)
            exc = _resample(opt_df, EXEC_MIN)

            if len(htf) < 2 or len(mtf) < 2 or len(ltf) < 2 or len(exc) < 2:
                continue

            htf_zones = _get_zones(htf)
            mtf_zones = _get_zones(mtf)
            ltf_zones = _get_zones(ltf)

            for htf_z in htf_zones:
                zk = _zone_kind(htf_z)
                if zk == "UNKNOWN":
                    continue
                if (zk == "BEAR" and opt_type != "CE") or (zk == "BULL" and opt_type != "PE"):
                    continue

                zone_key = f"{sym}_{opt_type}_{htf_z.get('zone_low',0):.0f}"
                if zone_key in sl_hist and (day - sl_hist[zone_key]).days <= 1:
                    continue

                mtf_m = next((z for z in mtf_zones if _zones_overlap(htf_z, z)), None)
                if not mtf_m:
                    continue
                ltf_m = next((z for z in ltf_zones if _zones_overlap(mtf_m, z)), None)
                if not ltf_m:
                    continue

                res = _simulate(exc, ltf_m, htf_z, SL_BUF, CAP_PTS, lot)
                if res:
                    res.update({"date": d_str, "opt": opt_type, "is_gap": is_gap})
                    if is_gap:
                        gap_trades.append(res)
                    else:
                        normal_trades.append(res)
                    if res["exit"] == "SL":
                        sl_hist[zone_key] = day
                    break

    def _stats(tlist):
        if not tlist:
            return dict(n=0, wins=0, wr=0.0, pf=0.0, net=0.0)
        wins = [t for t in tlist if t["pnl"] > 0]
        losses = [t for t in tlist if t["pnl"] <= 0]
        gw = sum(t["pnl"] for t in wins)
        gl = abs(sum(t["pnl"] for t in losses))
        return dict(
            n=len(tlist), wins=len(wins),
            wr=round(len(wins)/len(tlist)*100, 1),
            pf=round(gw/gl, 2) if gl > 0 else 9999.0,
            net=round(sum(t["pnl"] for t in tlist), 2),
        )

    return {
        "sym": sym,
        "gap_days": gap_days,
        "normal_days": normal_days,
        "gap": _stats(gap_trades),
        "normal": _stats(normal_trades),
        "gap_trades": gap_trades,
        "normal_trades": normal_trades,
    }

# ── main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    token = _get_token()
    if not token:
        print("ERROR: No token in clients.db")
        sys.exit(1)

    print(f"Gap split backtest — June 2026 | HTF={HTF_MIN}m MTF={MTF_MIN}m LTF={LTF_MIN}m")
    print(f"Symbols: {SYMBOLS}")

    for gap_thresh in GAP_THRESHOLDS:
        print(f"\n{'='*70}")
        print(f"  GAP THRESHOLD: {gap_thresh}%")
        print(f"{'='*70}")
        print(f"  {'Symbol':10} {'Type':8} {'Days':>5} {'Trades':>7} {'Win%':>7} {'PF':>7} {'Net Rs':>10}")
        print(f"  {'-'*58}")

        for sym in SYMBOLS:
            r = run_symbol(sym, token, gap_thresh)
            if not r:
                continue
            for label, key in [("GAP", "gap"), ("NORMAL", "normal")]:
                s = r[key]
                days_count = r["gap_days"] if key == "gap" else r["normal_days"]
                print(f"  {sym:10} {label:8} {days_count:>5} {s['n']:>7} "
                      f"{s['wr']:>6.1f}% {s['pf']:>7.2f} {s['net']:>10.0f}")
            print(f"  {'-'*58}")

    # Detailed breakdown at 0.5% threshold (SEBI standard gap definition)
    print(f"\n\n{'='*70}")
    print(f"  DETAILED TRADE LOG — GAP>=0.5% — June 2026")
    print(f"{'='*70}")
    for sym in SYMBOLS:
        r = run_symbol(sym, token, 0.5)
        if not r:
            continue
        print(f"\n  {sym} — GAP days ({r['gap_days']}):")
        for t in r["gap_trades"]:
            print(f"    {t['date']}  {t['opt']}  pnl={t['pnl']:+.0f}  exit={t['exit']}")
        print(f"  {sym} — NORMAL days ({r['normal_days']}):")
        for t in r["normal_trades"][:10]:
            print(f"    {t['date']}  {t['opt']}  pnl={t['pnl']:+.0f}  exit={t['exit']}")
