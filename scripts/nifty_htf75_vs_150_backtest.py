"""
scripts/nifty_htf75_vs_150_backtest.py
======================================
Compare two NIFTY trap-scanner HTF settings using the same option data:

  - PROPOSED config:  HTF=150m / MTF=15m / LTF=5m  (current trap_scanner config)
  - REQUESTED config: HTF=75m  / MTF=15m / LTF=5m  (user request)

Rules:
  - Strike selection from prev-day NIFTY spot close.
  - For each day, trade:
      * current-week expiry
      * next-week expiry
      * monthly expiry (July 28)
  - Non-gap days: HTF -> MTF -> LTF cascade.
  - Gap days (|open - prev_close|/prev_close >= GAP_PCT): skip HTF, use MTF -> LTF cascade.
  - Fixed SL = Rs 2,000/lot, CTC at +40 pts, trail at running_high - 20 pts.

Because June 2026 weekly option data is expired/unavailable, June days will only
produce trades for the monthly leg. July days will also show current/next week legs.

Run: python scripts/nifty_htf75_vs_150_backtest.py
"""
from __future__ import annotations
import glob, json, os, sys, time
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from data_layer.instrument_registry import _calc_next_expiry
from strategies.trap_scanner import scanner

CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")
DB_PATH = os.path.join(ROOT, "data", "clients.db")
UPSTOX_BASE = "https://api.upstox.com/v2"

SYMBOL = "NIFTY"
LOT = 65
STEP = 50
MAX_RISK_RS = 2000.0
FIXED_SL_POINTS = MAX_RISK_RS / LOT
CTC_PROFIT_POINTS = 40.0
TRAIL_BUFFER_POINTS = 20.0
GAP_PCT = 0.5

MTF_MIN, LTF_MIN, EXEC_MIN = 15, 5, 5
ZONE_CUTOFF = "15:14"

MONTHLY_EXPIRY = date(2026, 7, 28)   # July monthly (only monthly data we have)


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


# ── NIFTY daily OHLC for gap detection ────────────────────────────────────────
def _spot_daily_ohlc(token: str) -> Dict[str, dict]:
    cache_f = os.path.join(CACHE_DIR, "daily_ohlc_NIFTY.json")
    if os.path.exists(cache_f):
        return json.load(open(cache_f))

    from urllib.parse import quote as _q
    key = "NSE_INDEX|Nifty 50"
    enc = _q(key, safe="")
    url = f"{UPSTOX_BASE}/historical-candle/{enc}/day/2026-07-05/2026-05-25"
    r = requests.get(url, headers=_hdr(token), timeout=15)
    if r.status_code != 200:
        return {}
    candles = r.json().get("data", {}).get("candles", [])
    result = {}
    for c in reversed(candles):
        dt = pd.to_datetime(c[0])
        d = str(dt.date())
        result[d] = {"open": float(c[1]), "high": float(c[2]),
                     "low": float(c[3]), "close": float(c[4])}
    with open(cache_f, "w") as f:
        json.dump(result, f)
    return result


# ── option bar cache loaders ──────────────────────────────────────────────────
def _load_weekly_cache(expiry: date) -> Dict[str, pd.DataFrame]:
    exp_label = f"_W{expiry}"
    cache: Dict[str, pd.DataFrame] = {}
    for f in glob.glob(os.path.join(CACHE_DIR, f"opt_NIFTY*{exp_label}_*.parquet")):
        try:
            df = pd.read_parquet(f)
            df["datetime"] = pd.to_datetime(df["datetime"])
            if df["datetime"].dt.tz is not None:
                df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
            label = os.path.basename(f).replace("opt_", "").split("_2026")[0]
            cache[label] = df
        except Exception:
            pass
    return cache


def _load_monthly_cache() -> Dict[str, pd.DataFrame]:
    cache: Dict[str, pd.DataFrame] = {}
    for f in glob.glob(os.path.join(CACHE_DIR, "opt_NIFTY*.parquet")):
        if "_W" in os.path.basename(f):
            continue
        try:
            df = pd.read_parquet(f)
            df["datetime"] = pd.to_datetime(df["datetime"])
            if df["datetime"].dt.tz is not None:
                df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
            label = os.path.basename(f).replace("opt_", "").split("_2026")[0]
            cache[label] = df
        except Exception:
            pass
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
                  expiry: date, is_monthly: bool = False) -> Tuple[pd.DataFrame, int]:
    d_s = pd.Timestamp(f"{day}T09:15:00")
    d_e = pd.Timestamp(f"{day}T15:30:00")
    for off in [0, 1, -1, 2, -2]:
        st = atm + off * STEP if otype == "CE" else atm - off * STEP
        label = f"NIFTY{otype}{st}"
        if not is_monthly:
            label += f"_W{expiry}"
        keys = [k for k in opt_cache if k.startswith(label)]
        for key in keys:
            full = opt_cache[key]
            if full.empty:
                continue
            day_df = full[(full["datetime"] >= d_s) & (full["datetime"] <= d_e)].copy()
            if len(day_df) >= 30:
                return day_df, st
    return pd.DataFrame(), atm


# ── simulation with fixed SL / CTC / trailing SL ─────────────────────────────-
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


def _find_entry(exec_df: pd.DataFrame, ltf_zone: dict, anchor_zone: dict):
    ltf_l = float(ltf_zone.get("zone_low", 0))
    ltf_h = float(ltf_zone.get("zone_high", 0))
    anchor_l = float(anchor_zone.get("zone_low", 0))
    t1 = float(anchor_zone.get("sl", 0))
    if t1 <= 0 or anchor_l <= 0 or t1 <= ltf_h:
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
        if t1 <= trig or (anchor_l - FIXED_SL_POINTS) >= trig:
            continue
        hit = np.where(H[i + 1 :] >= trig)[0]
        if not len(hit):
            continue
        return i, trig
    return None


# ── per-day runner ────────────────────────────────────────────────────────────
def _run_day(day: date, atm: int, opt_cache: Dict[str, pd.DataFrame],
             expiry: date, expiry_label: str, htf_min: int,
             is_gap: bool) -> List[dict]:
    trades: List[dict] = []
    is_monthly = (expiry == MONTHLY_EXPIRY)

    for otype in ("CE", "PE"):
        opt_df, used_st = _get_day_bars(opt_cache, atm, otype, day, expiry, is_monthly)
        if opt_df.empty:
            continue

        mtf = _resamp(opt_df, MTF_MIN, ZONE_CUTOFF)
        ltf = _resamp(opt_df, LTF_MIN, ZONE_CUTOFF)
        exc = _resamp(opt_df, EXEC_MIN)
        if any(len(x) < 2 for x in [mtf, ltf, exc]):
            continue

        mtf_zones = _zones(mtf)
        ltf_zones = _zones(ltf)

        if is_gap:
            # Intraday cascade: MTF -> LTF, no HTF
            anchors = [(z, "gap_mtf") for z in mtf_zones if _zone_kind(z) == "BEAR"]
        else:
            # Full cascade: HTF -> MTF -> LTF
            htf = _resamp(opt_df, htf_min, ZONE_CUTOFF)
            if len(htf) < 2:
                continue
            htf_zones = _zones(htf)
            anchors = [(z, "htf") for z in htf_zones if _zone_kind(z) == "BEAR"]

        for anchor, mode in anchors:
            if mode == "htf":
                mm = next((z for z in mtf_zones if _overlap(anchor, z)), None)
                if not mm:
                    continue
                lm = next((z for z in ltf_zones if _overlap(mm, z)), None)
                if not lm:
                    continue
            else:
                mm = anchor
                lm = next((z for z in ltf_zones if _overlap(mm, z)), None)
                if not lm:
                    continue

            entry = _find_entry(exc, lm, anchor)
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
                "expiry_label": expiry_label,
                "htf": htf_min,
                "mode": mode,
                "is_gap": is_gap,
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
    token = _token()
    if not token:
        print("ERROR: No Upstox token")
        sys.exit(1)

    ohlc = _spot_daily_ohlc(token)
    if not ohlc:
        print("ERROR: Could not load NIFTY daily OHLC")
        sys.exit(1)

    start = date(2026, 6, 1)
    end = date(2026, 7, 5)
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)

    monthly_cache = _load_monthly_cache()
    weekly_caches: Dict[date, Dict[str, pd.DataFrame]] = {}

    print("=" * 80)
    print("NIFTY HTF comparison backtest")
    print(f"Lot={LOT} | SL={FIXED_SL_POINTS:.2f}pts | CTC={CTC_PROFIT_POINTS:.0f} | Trail={TRAIL_BUFFER_POINTS:.0f} | Gap thr={GAP_PCT}%")
    print(f"Period: {start} to {end}")
    print("=" * 80)

    all_trades: Dict[int, List[dict]] = {75: [], 150: []}

    for day in days:
        d_str = str(day)

        # Spot close for ATM
        prev_d = day - timedelta(days=1)
        prev_c = None
        for _ in range(5):
            prev_data = ohlc.get(str(prev_d))
            if prev_data:
                prev_c = prev_data.get("close")
                break
            prev_d -= timedelta(days=1)
        if not prev_c:
            continue
        atm = int(round(float(prev_c) / STEP) * STEP)

        # Gap detection
        today_data = ohlc.get(d_str, {})
        prev_close = prev_c
        today_open = today_data.get("open")
        is_gap = False
        if today_open and prev_close:
            gap_pct = abs(today_open - prev_close) / prev_close * 100
            is_gap = gap_pct >= GAP_PCT

        # Expiries for this day
        cur_exp = _calc_next_expiry(SYMBOL, day)
        nxt_exp = cur_exp + timedelta(days=7)

        expiries = [
            ("monthly", MONTHLY_EXPIRY, monthly_cache),
            ("current_week", cur_exp, None),
            ("next_week", nxt_exp, None),
        ]

        for label, exp, cache in expiries:
            if cache is None:
                if exp not in weekly_caches:
                    weekly_caches[exp] = _load_weekly_cache(exp)
                cache = weekly_caches[exp]
            if not cache:
                continue
            for htf in (75, 150):
                trades = _run_day(day, atm, cache, exp, label, htf, is_gap)
                all_trades[htf].extend(trades)

    # ── Summary ───────────────────────────────────────────────────────────────
    for htf in (75, 150):
        trades = all_trades[htf]
        s = _stats(trades)
        print(f"\n{'='*80}")
        print(f"HTF = {htf}m | TOTAL")
        print(f"{'='*80}")
        print(f"  Trades={s['n']}  Wins={s['w']}  WR={s['wr']:.1f}%  PF={s['pf']:.2f}  Net=Rs {s['net']:.0f}")

        # By expiry type
        print(f"\n  By expiry type:")
        print(f"  {'Expiry':14} {'Trades':>7} {'Wins':>6} {'WR%':>7} {'PF':>7} {'Net Rs':>10}")
        print("  " + "-" * 60)
        for lbl in ("monthly", "current_week", "next_week"):
            sub = [t for t in trades if t["expiry_label"] == lbl]
            ss = _stats(sub)
            print(f"  {lbl:14} {ss['n']:>7} {ss['w']:>6} {ss['wr']:>6.1f}% {ss['pf']:>7.2f} {ss['net']:>10.0f}")

        # By gap/normal
        gap_trades = [t for t in trades if t["is_gap"]]
        normal_trades = [t for t in trades if not t["is_gap"]]
        sg = _stats(gap_trades)
        sn = _stats(normal_trades)
        print(f"\n  Normal days: Trades={sn['n']} Wins={sn['w']} WR={sn['wr']:.1f}% PF={sn['pf']:.2f} Net={sn['net']:.0f}")
        print(f"  Gap days:    Trades={sg['n']} Wins={sg['w']} WR={sg['wr']:.1f}% PF={sg['pf']:.2f} Net={sg['net']:.0f}")

    # Combined comparison table
    print(f"\n\n{'='*80}")
    print("SIDE-BY-SIDE COMPARISON")
    print(f"{'='*80}")
    print(f"{'HTF':6} {'Trades':>7} {'Wins':>6} {'WR%':>7} {'PF':>7} {'Net Rs':>10}")
    print("-" * 50)
    for htf in (75, 150):
        s = _stats(all_trades[htf])
        print(f"{htf}m{5*' '} {s['n']:>7} {s['w']:>6} {s['wr']:>6.1f}% {s['pf']:>7.2f} {s['net']:>10.0f}")

    # Show gap days for context
    print(f"\n\nGap days in period:")
    for day in days:
        td = ohlc.get(str(day), {})
        pd = ohlc.get(str(day - timedelta(days=1)), {})
        if td and pd:
            gp = abs(td['open'] - pd['close']) / pd['close'] * 100
            print(f"  {day}: open={td['open']:.1f} prev_close={pd['close']:.1f} gap={gp:.2f}% {'GAP' if gp>=GAP_PCT else ''}")
