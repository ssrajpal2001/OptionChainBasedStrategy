"""
scripts/nifty_registry_expiry_backtest.py
=========================================
Uses the project's InstrumentRegistry to get the latest (current-week)
and next-week NIFTY expiry, fetches the option bars for those expiries,
and runs the trailing-SL backtest.

Risk rules:
  - Lot = 65 (NIFTY)
  - Max risk = Rs 2,000 per lot -> fixed SL = 2000/65 option pts
  - When floating profit >= +40 option pts, move SL to cost-to-cost (CTC)
  - After CTC, trail SL at running_high - 20 option pts

Run: python scripts/nifty_registry_expiry_backtest.py
"""
from __future__ import annotations
import os, sys, time
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from data_layer.instrument_registry import REGISTRY, _calc_next_expiry
from strategies.trap_scanner import scanner

CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")
DB_PATH = os.path.join(ROOT, "data", "clients.db")
UPSTOX_BASE = "https://api.upstox.com/v2"

SYMBOL = "NIFTY"
LOT = 65
STEP = 50
MAX_RISK_RS = 2000.0
FIXED_SL_POINTS = MAX_RISK_RS / LOT          # ~30.77 option pts
CTC_PROFIT_POINTS = 40.0
TRAIL_BUFFER_POINTS = 20.0

HTF_MIN, MTF_MIN, LTF_MIN, EXEC_MIN = 75, 15, 5, 5
ZONE_CUTOFF = "15:14"
STRIKE_RANGE = 10                            # ATM +/- 10 strikes to fetch


def _token() -> str:
    import sqlite3
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT access_token FROM system_feeder_creds WHERE provider='upstox' LIMIT 1"
        ).fetchone()
        conn.close()
        return (row[0] or "") if row else ""
    except Exception:
        return ""


def _hdr(t):
    return {"Authorization": f"Bearer {t}", "Accept": "application/json"}


def _spot_daily() -> Dict[str, float]:
    cache_f = os.path.join(CACHE_DIR, "daily_NIFTY.json")
    if os.path.exists(cache_f):
        data = json.load(open(cache_f))
        return {k: float(v) if isinstance(v, (int, float)) else float(v.get("close", 0))
                for k, v in data.items()}
    return {}


# ── option bar fetcher ────────────────────────────────────────────────────────
def _fetch_option_1m(ikey: str, label: str, fr: date, to: date, token: str) -> pd.DataFrame:
    cache_f = os.path.join(CACHE_DIR, f"opt_{label}_{fr}_{to}.parquet")
    if os.path.exists(cache_f):
        df = pd.read_parquet(cache_f)
        if not df.empty:
            return df
    from urllib.parse import quote as _q
    enc = _q(ikey, safe="")
    url = f"{UPSTOX_BASE}/historical-candle/{enc}/1minute/{to}/{fr}"
    r = requests.get(url, headers=_hdr(token), timeout=20)
    time.sleep(0.25)
    if r.status_code != 200:
        return pd.DataFrame()
    candles = r.json().get("data", {}).get("candles", [])
    if not candles:
        return pd.DataFrame()
    rows = [{"datetime": c[0], "open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4]), "volume": int(c[5])}
            for c in reversed(candles)]
    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["datetime"])
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    df.to_parquet(cache_f, index=False)
    return df


def _load_or_fetch_expiry(expiry: date, atm: int, token: str) -> Dict[str, pd.DataFrame]:
    """Return {label: df} for all CE/PE strikes around atm for this expiry."""
    cache: Dict[str, pd.DataFrame] = {}
    for st in range(atm - STRIKE_RANGE * STEP, atm + (STRIKE_RANGE + 1) * STEP, STEP):
        for otype in ("CE", "PE"):
            ikey = REGISTRY.get_upstox_key(SYMBOL, expiry, st, otype)
            if not ikey:
                continue
            label = f"NIFTY{otype}{st}_W{expiry}"
            df = _fetch_option_1m(ikey, label, date(2026, 7, 1), expiry, token)
            if not df.empty:
                cache[label] = df
    return cache


# ── zone helpers ──────────────────────────────────────────────────────────────
def _zone_kind(z):
    sl, zh, zl = z.get("sl", 0), z.get("zone_high", 0), z.get("zone_low", 0)
    if sl > zh:
        return "BEAR"
    if sl < zl:
        return "BULL"
    return "?"


def _zones(df):
    if len(df) < 3:
        return []
    try:
        _, z = scanner.scan_htf(df)
        return z or []
    except Exception:
        return []


def _overlap(a, b):
    al, ah = float(a.get("zone_low", 0)), float(a.get("zone_high", 0))
    bl, bh = float(b.get("zone_low", 0)), float(b.get("zone_high", 0))
    buf = max((ah - al) * 0.15, 0.5)
    return ah + buf >= bl and bh + buf >= al


def _resamp(df1m, m, cut=None):
    if df1m.empty or len(df1m) < 2:
        return pd.DataFrame()
    df = df1m.copy()
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_localize(None)
    if cut:
        h, mn = cut.split(":")
        from datetime import time as _t
        df = df[df["datetime"].dt.time <= _t(int(h), int(mn))]
    return (
        df.set_index("datetime")[["open", "high", "low", "close"]]
        .resample(f"{m}min", closed="left", label="left")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
        .dropna(subset=["close"])
        .reset_index()
    )


def _get_day_bars(opt_cache: Dict[str, pd.DataFrame], atm: int, otype: str, day: date,
                  expiry: date) -> Tuple[pd.DataFrame, int]:
    d_s = pd.Timestamp(f"{day}T09:15:00")
    d_e = pd.Timestamp(f"{day}T15:30:00")
    for off in [0, 1, -1, 2, -2]:
        st = atm + off * STEP if otype == "CE" else atm - off * STEP
        label = f"NIFTY{otype}{st}_W{expiry}"
        full = opt_cache.get(label)
        if full is None or full.empty:
            continue
        day_df = full[(full["datetime"] >= d_s) & (full["datetime"] <= d_e)].copy()
        if len(day_df) >= 30:
            return day_df, st
    return pd.DataFrame(), atm


# ── simulation with fixed SL / CTC / trailing SL ──────────────────────────────
def _simulate(exec_df: pd.DataFrame, entry_idx: int, entry_price: float) -> dict:
    H = exec_df["high"].values[entry_idx + 1 :].astype(float)
    L = exec_df["low"].values[entry_idx + 1 :].astype(float)
    C = exec_df["close"].values[entry_idx + 1 :].astype(float)
    if len(H) == 0:
        return None

    sl = entry_price - FIXED_SL_POINTS
    running_high = entry_price
    ctc_active = False

    for k in range(len(H)):
        if L[k] <= sl:
            return {
                "pnl": round((sl - entry_price) * LOT, 2),
                "exit": "SL",
                "entry": round(entry_price, 2),
                "sl": round(sl, 2),
                "max_high": round(running_high, 2),
            }
        running_high = max(running_high, H[k])
        if running_high - entry_price >= CTC_PROFIT_POINTS:
            sl = max(sl, entry_price)
            ctc_active = True
        if ctc_active:
            sl = max(sl, running_high - TRAIL_BUFFER_POINTS)

    return {
        "pnl": round((float(C[-1]) - entry_price) * LOT, 2),
        "exit": "EOD",
        "entry": round(entry_price, 2),
        "sl": round(sl, 2),
        "max_high": round(running_high, 2),
    }


def _find_entry(exec_df: pd.DataFrame, ltf_zone: dict, htf_zone: dict):
    ltf_l = float(ltf_zone.get("zone_low", 0))
    ltf_h = float(ltf_zone.get("zone_high", 0))
    htf_l = float(htf_zone.get("zone_low", 0))
    t1 = float(htf_zone.get("sl", 0))
    if t1 <= 0 or htf_l <= 0 or t1 <= ltf_h:
        return None

    H = exec_df["high"].values.astype(float)
    L = exec_df["low"].values.astype(float)
    C = exec_df["close"].values.astype(float)
    buf = max((ltf_h - ltf_l) * 0.15, 0.5)
    in_zone = (C >= ltf_l - buf) & (C <= ltf_h + buf)
    idxs = np.where(in_zone)[0]
    idxs = idxs[idxs < len(H) - 1]

    for i in idxs:
        trig = float(H[i])
        if t1 <= trig or (htf_l - FIXED_SL_POINTS) >= trig:
            continue
        hit = np.where(H[i + 1 :] >= trig)[0]
        if not len(hit):
            continue
        return i, trig
    return None


# ── per-day runner ────────────────────────────────────────────────────────────
def _run_day(day: date, atm: int, opt_cache: Dict[str, pd.DataFrame],
             expiry: date) -> List[dict]:
    trades: List[dict] = []
    for otype in ("CE", "PE"):
        opt_df, used_st = _get_day_bars(opt_cache, atm, otype, day, expiry)
        if opt_df.empty:
            continue

        htf = _resamp(opt_df, HTF_MIN, ZONE_CUTOFF)
        mtf = _resamp(opt_df, MTF_MIN, ZONE_CUTOFF)
        ltf = _resamp(opt_df, LTF_MIN, ZONE_CUTOFF)
        exc = _resamp(opt_df, EXEC_MIN)
        if any(len(x) < 2 for x in [htf, mtf, ltf, exc]):
            continue

        hzs = _zones(htf)
        mzs = _zones(mtf)
        lzs = _zones(ltf)

        for hz in hzs:
            if _zone_kind(hz) != "BEAR":
                continue
            mm = next((z for z in mzs if _overlap(hz, z)), None)
            if not mm:
                continue
            lm = next((z for z in lzs if _overlap(mm, z)), None)
            if not lm:
                continue
            entry = _find_entry(exc, lm, hz)
            if not entry:
                continue
            idx, price = entry
            res = _simulate(exc, idx, price)
            if not res:
                continue
            res.update({
                "date": str(day),
                "opt": otype,
                "atm": atm,
                "strike": used_st,
                "expiry": str(expiry),
            })
            trades.append(res)
            break  # one trade per side per day
    return trades


def _stats(tlist: List[dict]) -> dict:
    if not tlist:
        return dict(n=0, w=0, wr=0.0, pf=0.0, net=0.0)
    wins = [t for t in tlist if t["pnl"] > 0]
    losses = [t for t in tlist if t["pnl"] <= 0]
    gw = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    return dict(
        n=len(tlist),
        w=len(wins),
        wr=round(len(wins) / len(tlist) * 100, 1),
        pf=round(gw / gl, 2) if gl > 0 else 9999.0,
        net=round(sum(t["pnl"] for t in tlist), 2),
    )


# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import json
    token = _token()
    if not token:
        print("ERROR: No Upstox token")
        sys.exit(1)

    spot = _spot_daily()
    if not spot:
        print("ERROR: No daily NIFTY spot cache")
        sys.exit(1)

    # Load registry for NIFTY
    REGISTRY.load_sync(SYMBOL, token)
    current_exp = REGISTRY.get_active_expiry(SYMBOL, date(2026, 7, 1))
    if not current_exp:
        print("ERROR: Registry did not return active expiry")
        sys.exit(1)
    all_exps = REGISTRY.all_expiries(SYMBOL)
    next_exp = None
    try:
        idx = all_exps.index(current_exp)
        next_exp = all_exps[idx + 1] if idx + 1 < len(all_exps) else None
    except ValueError:
        next_exp = _calc_next_expiry(SYMBOL, current_exp + timedelta(days=1))

    print("=" * 70)
    print("NIFTY registry-expiry trailing-SL backtest")
    print(f"Lot = {LOT} | Fixed SL = {FIXED_SL_POINTS:.2f} pts (Rs {MAX_RISK_RS:.0f}) "
          f"| CTC at +{CTC_PROFIT_POINTS:.0f} | Trail buffer {TRAIL_BUFFER_POINTS:.0f}")
    print(f"Current-week expiry from registry: {current_exp}")
    print(f"Next-week expiry from registry:    {next_exp}")
    print("=" * 70)

    start = date(2026, 7, 1)
    end = min(current_exp, date.today())
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    print(f"Trade days: {[str(x) for x in days]}\n")

    all_trades: List[dict] = []
    for exp in [e for e in (current_exp, next_exp) if e]:
        # Use a representative ATM to decide strike range for fetching
        prev_c = spot.get(str(days[0] - timedelta(days=1))) if days else None
        for _ in range(5):
            if prev_c:
                break
        atm_fetch = int(round(float(prev_c or 23850) / STEP) * STEP) if prev_c else 23850
        print(f"Fetching/loading {exp} option bars around ATM {atm_fetch}...")
        opt_cache = _load_or_fetch_expiry(exp, atm_fetch, token)
        print(f"  {len(opt_cache)} option series ready")

        exp_trades: List[dict] = []
        for day in days:
            prev_d = day - timedelta(days=1)
            prev_c = None
            for _ in range(5):
                prev_c = spot.get(str(prev_d))
                if prev_c:
                    break
                prev_d -= timedelta(days=1)
            if not prev_c:
                continue
            atm = int(round(float(prev_c) / STEP) * STEP)
            exp_trades.extend(_run_day(day, atm, opt_cache, exp))

        s = _stats(exp_trades)
        print(f"\nExpiry {exp}: Trades={s['n']} Wins={s['w']} WR={s['wr']:.1f}% "
              f"PF={s['pf']:.2f} Net=Rs {s['net']:.0f}")
        if exp_trades:
            print(f"  {'Date':12} {'OPT':4} {'Strike':7} {'Entry':8} {'SL':8} {'MaxH':8} {'PnL':>8} {'Exit'}")
            print("  " + "-" * 70)
            for t in exp_trades:
                print(f"  {t['date']:12} {t['opt']:4} {t['strike']:7} {t['entry']:8.1f} "
                      f"{t['sl']:8.1f} {t['max_high']:8.1f} {t['pnl']:+8.0f}  {t['exit']}")
        all_trades.extend(exp_trades)

    s = _stats(all_trades)
    print("\n" + "=" * 70)
    print(f"Combined: Trades={s['n']} Wins={s['w']} WR={s['wr']:.1f}% "
          f"PF={s['pf']:.2f} Net=Rs {s['net']:.0f}")
    print("=" * 70)
