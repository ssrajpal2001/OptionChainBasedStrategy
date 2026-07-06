"""
scripts/nse_ob_backtest.py
==========================
Targeted backtest: HTF=75m / MTF=15m / LTF=5m
Compares WITHOUT vs WITH OB+CHoCH gate.

Instruments: NIFTY, BANKNIFTY, SENSEX
Period     : Apr 1 – Jun 30 2026 (3 months)

Usage: python scripts/nse_ob_backtest.py
"""
from __future__ import annotations

import os, sys, time, sqlite3
from datetime import date, datetime, timedelta
from typing import Optional, Dict, List
import numpy as np
import pandas as pd
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from strategies.trap_scanner import scanner

# ── config ────────────────────────────────────────────────────────────────────
START_DATE = date(2026, 6, 1)
END_DATE   = date(2026, 6, 30)

HTF_MIN  = 75
MTF_MIN  = 15
LTF_MIN  = 5
EXEC_MIN = 5
SL_BUF   = 10        # pts below zone_low (option premium units)
CAP_PTS  = 200       # profit cap 0=hold to T1
ZIGZAG   = 9

SYMBOLS  = ["NIFTY", "BANKNIFTY", "SENSEX"]
LOT_SIZES    = {"NIFTY": 25, "BANKNIFTY": 15, "SENSEX": 10, "FINNIFTY": 40}
STRIKE_STEPS = {"NIFTY": 50, "BANKNIFTY": 100, "SENSEX": 100}
ZONE_CUTOFF  = "15:14"

DB_PATH   = os.path.join(ROOT, "data", "clients.db")
CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")
UPSTOX_BASE = "https://api.upstox.com/v2"

INDEX_KEY = {
    "NIFTY":     "NSE_INDEX|Nifty 50",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
    "SENSEX":    "BSE_INDEX|SENSEX",
}
UNDERLYING_KEY = {
    "NIFTY":     "NSE_INDEX|Nifty 50",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
    "SENSEX":    "BSE_INDEX|SENSEX",
}

# ── token ─────────────────────────────────────────────────────────────────────
def _get_token() -> str:
    if not os.path.exists(DB_PATH):
        return ""
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT access_token FROM system_feeder_creds WHERE provider='upstox' LIMIT 1"
        ).fetchone()
        conn.close()
        return (row[0] or "") if row else ""
    except Exception:
        return ""

def _hdr(token):
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

# ── fetch helpers ─────────────────────────────────────────────────────────────
from urllib.parse import quote as _quote

def _fetch_index_1m(sym: str, token: str, day: date) -> pd.DataFrame:
    """Load from cache or fetch 1m index bars for a day."""
    from pathlib import Path
    cache_f = Path(CACHE_DIR) / f"idx_{sym}_{day}.parquet"
    if cache_f.exists():
        df = pd.read_parquet(cache_f)
        df["datetime"] = pd.to_datetime(df["datetime"])
        return df.sort_values("datetime").reset_index(drop=True)

    raw_key = INDEX_KEY.get(sym, f"NSE_INDEX|{sym}")
    enc = _quote(raw_key, safe="")
    url = f"{UPSTOX_BASE}/historical-candle/intraday/{enc}/1minute"
    r = requests.get(url, headers=_hdr(token), timeout=15)
    if r.status_code != 200:
        return pd.DataFrame()
    candles = r.json().get("data", {}).get("candles", [])
    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame(candles, columns=["datetime","open","high","low","close","volume","oi"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df[df["datetime"].dt.date == day].sort_values("datetime").reset_index(drop=True)
    if "volume" not in df.columns:
        df["volume"] = 0
    return df[["datetime","open","high","low","close","volume"]]

def _load_cached_idx(sym: str) -> Optional[pd.DataFrame]:
    """Load the largest cached index parquet for the symbol."""
    import glob
    from pathlib import Path
    files = sorted(glob.glob(str(Path(CACHE_DIR) / f"idx_{sym}_*.parquet")), reverse=True)
    if not files:
        return None
    frames = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
            frames.append(df)
        except Exception:
            pass
    if not frames:
        return None
    combined = pd.concat(frames).sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)
    # Filter to backtest period
    combined = combined[combined["datetime"].dt.date >= START_DATE]
    combined = combined[combined["datetime"].dt.date <= END_DATE]
    return combined

def _resample(df1m: pd.DataFrame, minutes: int, cutoff: str = None) -> pd.DataFrame:
    df = df1m.set_index("datetime")[["open","high","low","close","volume"]]
    if cutoff:
        t_cut = pd.to_datetime(cutoff).time() if len(cutoff) <= 5 else None
        if not t_cut:
            try:
                h, m = cutoff.split(":")
                t_cut = __import__("datetime").time(int(h), int(m))
            except Exception:
                t_cut = None
        if t_cut:
            df = df[df.index.time <= t_cut]
    r = df.resample(f"{minutes}min").agg(
        {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}
    ).dropna(subset=["close"]).reset_index()
    r = r.rename(columns={"datetime": "datetime"})
    return r

def _bars_to_dicts(df: pd.DataFrame) -> List[dict]:
    return [{"datetime": str(r.datetime), "open": r.open, "high": r.high,
             "low": r.low, "close": r.close, "volume": r.volume}
            for r in df.itertuples()]

# ── OB+CHoCH check on a bar list ──────────────────────────────────────────────
def _ob_choch_clear(bars_dicts: list, opt_type: str, ltp: float, zz: int = ZIGZAG) -> bool:
    """Return True if OB+CHoCH gate clears for this opt_type on the given bars."""
    min_bars = max(zz * 3 + 14, 30)
    if len(bars_dicts) < min_bars:
        return False
    try:
        df = pd.DataFrame(bars_dicts[-500:])
        df["datetime"] = pd.to_datetime(df["datetime"])
        bull_obs, bear_obs = scanner.active_order_blocks(df, zigzag_len=zz)
        sigs     = scanner.detect_choch_bos(df, zigzag_len=zz)
        choch    = scanner.last_choch_direction(sigs)
        if opt_type == "CE":
            # CE = we're buying a call = BEAR trap in option → expect reversal UP
            return choch == "UP" and bool(scanner.price_in_order_block(ltp, bull_obs))
        else:
            # PE = buying a put = BULL trap in option → expect reversal DOWN
            return choch == "DOWN" and bool(scanner.price_in_order_block(ltp, bear_obs))
    except Exception:
        return False

# ── scan helpers ──────────────────────────────────────────────────────────────
def _zone_kind(z: dict) -> str:
    """Infer BEAR/BULL from SL position: BEAR if sl > zone_high (SL above zone)."""
    sl = z.get("sl", 0)
    zh = z.get("zone_high", 0)
    zl = z.get("zone_low", 0)
    if sl > zh:
        return "BEAR"
    if sl < zl:
        return "BULL"
    return "UNKNOWN"

def _get_zones(df_tf: pd.DataFrame, kind: str = "BEAR") -> list:
    if len(df_tf) < 3:
        return []
    try:
        _, zones = scanner.scan_htf(df_tf)
        return [z for z in zones if _zone_kind(z) == kind]
    except Exception:
        return []

def _zones_overlap(a: dict, b: dict) -> bool:
    al, ah = a.get("zone_low", 0), a.get("zone_high", 0)
    bl, bh = b.get("zone_low", 0), b.get("zone_high", 0)
    return ah > bl and bh > al

# ── simulate one trade ────────────────────────────────────────────────────────
def _simulate(exec_arr, ltf_zone, htf_zone, sl_buf, cap_pts, lot) -> Optional[dict]:
    ltf_l = ltf_zone.get("zone_low", 0)
    ltf_h = ltf_zone.get("zone_high", 0)
    htf_l = htf_zone.get("zone_low", 0)
    t1    = float(htf_zone.get("sl", 0))
    sl    = htf_l - sl_buf
    if t1 <= 0 or sl <= 0 or t1 <= ltf_h:
        return None
    buf = max((ltf_h - ltf_l) * 0.15, 1.0)
    H, L, C = exec_arr["high"], exec_arr["low"], exec_arr["close"]
    n = len(H)
    in_zone = (C >= ltf_l - buf) & (C <= ltf_h + buf)
    idxs = np.where(in_zone)[0]
    idxs = idxs[idxs < n - 1]
    for i in idxs:
        trig = float(H[i])
        if t1 <= trig or sl >= trig:
            continue
        hit = np.where(H[i+1:] >= trig)[0]
        if not len(hit):
            continue
        j = hit[0]
        Hs = H[i+1:][j:]; Ls = L[i+1:][j:]; Cs = C[i+1:][j:]
        # simple sim: exit at t1, SL, cap, or EOD
        for k in range(len(Hs)):
            if Ls[k] <= sl:
                pnl = (sl - trig) * lot
                return {"pnl": round(pnl, 2), "exit": "SL", "entry": round(trig, 2)}
            if Hs[k] >= t1:
                target = min(t1, trig + cap_pts) if cap_pts > 0 else t1
                pnl = (target - trig) * lot
                return {"pnl": round(pnl, 2), "exit": "T1", "entry": round(trig, 2)}
        pnl = (float(Cs[-1]) - trig) * lot
        return {"pnl": round(pnl, 2), "exit": "EOD", "entry": round(trig, 2)}
    return None

# ── run one symbol ────────────────────────────────────────────────────────────
def run_symbol(sym: str, token: str) -> dict:
    print(f"\n{'='*60}")
    print(f"  {sym}  HTF={HTF_MIN}m  MTF={MTF_MIN}m  LTF={LTF_MIN}m")
    print(f"{'='*60}")

    lot  = LOT_SIZES.get(sym, 25)
    step = STRIKE_STEPS.get(sym, 50)

    # Load combined cache
    df_all = _load_cached_idx(sym)
    if df_all is None or len(df_all) < 100:
        print(f"  No cache data for {sym}. Skipping.")
        return {}

    if "volume" not in df_all.columns:
        df_all["volume"] = 0
    df_all = df_all[["datetime","open","high","low","close","volume"]].copy()
    print(f"  Cache: {len(df_all)} bars  {df_all['datetime'].min().date()}–{df_all['datetime'].max().date()}")

    # Trade days
    days = sorted({r.datetime.date() for r in df_all.itertuples()
                   if START_DATE <= r.datetime.date() <= END_DATE})
    print(f"  Trade days: {len(days)}")

    trades_no_ob, trades_ob = [], []
    sl_hist_no: Dict[str, str] = {}
    sl_hist_ob: Dict[str, str] = {}

    for day in days:
        day_df = df_all[df_all["datetime"].dt.date == day].copy()
        if len(day_df) < 30:
            continue

        # Resample to each TF with cutoff
        htf = _resample(day_df, HTF_MIN, ZONE_CUTOFF)
        mtf = _resample(day_df, MTF_MIN, ZONE_CUTOFF)
        ltf = _resample(day_df, LTF_MIN, ZONE_CUTOFF)
        exc = _resample(day_df, EXEC_MIN)

        if len(htf) < 2 or len(mtf) < 2 or len(ltf) < 2 or len(exc) < 2:
            continue

        # Check both BEAR (CE buys) and BULL (PE buys)
        htf_z = _get_zones(htf, "BEAR") + _get_zones(htf, "BULL")
        mtf_z = _get_zones(mtf, "BEAR") + _get_zones(mtf, "BULL")
        ltf_z = _get_zones(ltf, "BEAR") + _get_zones(ltf, "BULL")

        # Build numpy arrays for exec simulation
        exc_arr = {
            "high":  exc["high"].values.astype(float),
            "low":   exc["low"].values.astype(float),
            "close": exc["close"].values.astype(float),
        }

        # Both strategies attempt the same cascade
        d_str = str(day)

        for use_ob in (False, True):
            sl_hist = sl_hist_ob if use_ob else sl_hist_no
            trades  = trades_ob  if use_ob else trades_no_ob

            for htf_zone in htf_z:
                zone_key = f"{htf_zone.get('zone_low',0):.1f}-{htf_zone.get('zone_high',0):.1f}"
                if zone_key in sl_hist:
                    if (day - date.fromisoformat(sl_hist[zone_key])).days <= 1:
                        continue

                mtf_m = next((z for z in mtf_z if _zones_overlap(htf_zone, z)), None)
                if not mtf_m:
                    continue

                ltf_m = next((z for z in ltf_z if _zones_overlap(mtf_m, z)), None)
                if not ltf_m:
                    continue

                # OB+CHoCH gate using LTF option bars
                # BEAR zone → CE (expect reversal up) → CHoCH UP + bullish OB
                # BULL zone → PE (expect reversal down) → CHoCH DOWN + bearish OB
                zone_kind_str = _zone_kind(htf_zone)
                opt_type = "CE" if zone_kind_str == "BEAR" else "PE"
                if use_ob:
                    ltf_bars = _bars_to_dicts(ltf)
                    ltp = float(ltf["close"].iloc[-1])
                    if not _ob_choch_clear(ltf_bars, opt_type, ltp):
                        continue

                res = _simulate(exc_arr, ltf_m, htf_zone, SL_BUF, CAP_PTS, lot)
                if res:
                    res.update({"date": d_str, "zone": zone_key, "sym": sym})
                    trades.append(res)
                    if res["exit"] == "SL":
                        sl_hist[zone_key] = d_str
                    break  # one trade per day per approach

    def _stats(trades):
        if not trades:
            return {"trades": 0, "wins": 0, "losses": 0, "win_pct": 0,
                    "pf": 0, "net": 0, "avg_win": 0, "avg_loss": 0}
        wins   = [t for t in trades if t["pnl"] > 0]
        losses = [t for t in trades if t["pnl"] <= 0]
        gross_w = sum(t["pnl"] for t in wins)
        gross_l = abs(sum(t["pnl"] for t in losses))
        pf = gross_w / gross_l if gross_l > 0 else 9999.0
        return {
            "trades":   len(trades),
            "wins":     len(wins),
            "losses":   len(losses),
            "win_pct":  round(len(wins) / len(trades) * 100, 1),
            "pf":       round(pf, 2),
            "net":      round(sum(t["pnl"] for t in trades), 2),
            "avg_win":  round(gross_w / max(len(wins), 1), 2),
            "avg_loss": round(-gross_l / max(len(losses), 1), 2),
        }

    s_no = _stats(trades_no_ob)
    s_ob = _stats(trades_ob)

    print(f"\n  {'':25} {'WITHOUT OB':>12} {'WITH OB':>12}")
    print(f"  {'-'*49}")
    for k in ["trades","wins","losses","win_pct","pf","net","avg_win","avg_loss"]:
        print(f"  {k:25} {str(s_no[k]):>12} {str(s_ob[k]):>12}")

    # Print individual OB trades for detail
    if trades_ob:
        print(f"\n  Trades WITH OB ({len(trades_ob)}):")
        for t in trades_ob:
            print(f"    {t['date']}  zone={t['zone']}  entry={t['entry']}  "
                  f"pnl={t['pnl']:+.2f}  exit={t['exit']}")

    return {"sym": sym, "no_ob": s_no, "ob": s_ob}

# ── main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    token = _get_token()
    if not token:
        print("ERROR: No Upstox token found in data/clients.db")
        sys.exit(1)
    print(f"Token OK. Period: {START_DATE} to {END_DATE}")
    print(f"Config: HTF={HTF_MIN}m  MTF={MTF_MIN}m  LTF={LTF_MIN}m  SL={SL_BUF}pts  CAP={CAP_PTS}pts")

    all_results = []
    for sym in SYMBOLS:
        r = run_symbol(sym, token)
        if r:
            all_results.append(r)

    # ── Final comparison table ────────────────────────────────────────────────
    print(f"\n\n{'='*70}")
    print(f"  FINAL SUMMARY  HTF={HTF_MIN}m / MTF={MTF_MIN}m / LTF={LTF_MIN}m")
    print(f"{'='*70}")
    print(f"  {'Symbol':10} {'':8} {'Trades':>8} {'Win%':>8} {'PF':>8} {'Net Rs':>10} {'AvgWin':>8} {'AvgLoss':>8}")
    print(f"  {'-'*68}")
    for r in all_results:
        sym = r["sym"]
        for label, s in [("NO OB", r["no_ob"]), ("OB+CHoCH", r["ob"])]:
            print(f"  {sym:10} {label:8} {s['trades']:>8} {s['win_pct']:>7.1f}% "
                  f"{s['pf']:>8.2f} {s['net']:>10.0f} {s['avg_win']:>8.0f} {s['avg_loss']:>8.0f}")
        print(f"  {'-'*68}")
