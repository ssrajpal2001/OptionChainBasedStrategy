"""
BANKNIFTY Risk-Filter Backtest — HTF(75m) → MTF(5m) Zone Trigger Entry
=======================================================================
Strategy variant: NO LTF. Entry directly at MTF zone_trigger (1/3 level).
Risk filter: skip trade if (zone_trigger − SL) × lot > max_loss_rs.

Cascade:
  1. HTF (75m) BEAR trap zone ACTIVE on option premium chart
  2. Price enters HTF zone (close <= htf zone_high)
  3. MTF (5m) BEAR trap zone forms (bears trapped)
  4. Price comes to MTF zone_trigger = zone_low + (zone_high − zone_low) × frac
  5. FILTER: SL_distance × lot_size ≤ max_loss_rs → ENTER (else SKIP)
  6. Exit: T1 = MTF zone sl (ref bar HIGH), SL = zone_low − sl_buf

Optimisation grid:
  max_loss_rs : ₹500 .. ₹3000 (risk cap per trade)
  trig_frac   : 0.20 .. 0.50  (where in the MTF zone to enter)

Comparison vs previous best:
  Previous best from nse_cascade_backtest.py:
    HTF=180m / MTF=30m / LTF=3m / SLbuf=30 / DTE≤10
    PF=6.28, 78% WR

Usage:
  python scripts/banknifty_risk_filter_backtest.py
"""
from __future__ import annotations

import os, sys, sqlite3, time
from datetime import date, datetime, timedelta
from typing import Optional, Dict, Tuple
import numpy as np
import pandas as pd
import requests
from urllib.parse import quote as _quote

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from strategies.trap_scanner import scanner
from strategies.trap_scanner.zones import _bars_to_df, _resample_htf

# ── Constants ─────────────────────────────────────────────────────────────────
SYMBOL     = "BANKNIFTY"
LOT        = 15           # lot size as user stated
STEP       = 100
HTF_MIN    = 75           # fixed per user spec
MTF_MIN    = 5            # fixed per user spec
SL_BUF     = 30.0         # pts below MTF zone_low (same as current optimal)
START_DATE = date(2026, 4, 1)
END_DATE   = date(2026, 6, 30)
LOOKBACK   = 5            # days of lookback for zone history

UPSTOX_BASE = "https://api.upstox.com/v2"
DB_PATH     = os.path.join(_ROOT, "data", "clients.db")
CACHE_DIR   = os.path.join(_ROOT, "data", "nse_option_cache")

INDEX_KEY = {
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
    "NIFTY":     "NSE_INDEX|Nifty 50",
}

# ── Optimisation grids ────────────────────────────────────────────────────────
# max_loss_rs: max rupee loss allowed per trade (risk filter)
MAX_LOSS_GRID  = [500, 750, 1000, 1500, 2000, 2500, 3000]
# trig_frac: MTF zone trigger level = zone_low + (zone_high − zone_low) × frac
TRIG_FRAC_GRID = [0.20, 0.25, 0.33, 0.40, 0.50]
# Also run with NO risk filter (baseline for this HTF75/MTF5 variant)
NO_FILTER      = 999_999  # sentinel = no cap

# Previous best params (from nse_cascade_backtest.py results for comparison)
PREV_BEST = {
    "htf_min": 180, "mtf_min": 30, "ltf_min": 3,
    "sl_buf": 30, "dte_filter": 10,
    "profit_factor": 6.28, "win_rate_pct": 78.0,
    "note": "HTF=180m/MTF=30m/LTF=3m/SLbuf=30/DTE≤10 (4-tier cascade)",
}

# ── Token + API ───────────────────────────────────────────────────────────────

def _get_token() -> str:
    if not os.path.exists(DB_PATH):
        return ""
    try:
        conn = sqlite3.connect(DB_PATH)
        row  = conn.execute(
            "SELECT access_token FROM system_feeder_creds WHERE provider='upstox' LIMIT 1"
        ).fetchone()
        conn.close()
        return (row[0] or "") if row else ""
    except Exception as e:
        print(f"[WARN] token: {e}"); return ""

def _hdr(token): return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

def _fetch_1m(sym: str, token: str, fr: date, to: date) -> pd.DataFrame:
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_f = os.path.join(CACHE_DIR, f"idx_{sym}_{fr}_{to}.parquet")
    if os.path.exists(cache_f):
        return pd.read_parquet(cache_f)
    raw_key = INDEX_KEY.get(sym, f"NSE_INDEX|{sym}")
    enc     = _quote(raw_key, safe="")
    rows    = []
    chunk   = fr
    while chunk <= to:
        chunk_to = min(chunk + timedelta(days=27), to, date.today())
        url = f"{UPSTOX_BASE}/historical-candle/{enc}/1minute/{chunk_to}/{chunk}"
        r   = requests.get(url, headers=_hdr(token), timeout=20)
        time.sleep(0.35)
        if r.status_code == 200:
            rows.extend(
                {"datetime": c[0], "open": float(c[1]), "high": float(c[2]),
                 "low": float(c[3]), "close": float(c[4])}
                for c in reversed(r.json().get("data", {}).get("candles", []))
            )
        else:
            print(f"  [WARN] {sym} 1m {chunk}→{chunk_to}: HTTP {r.status_code}", flush=True)
        chunk = chunk_to + timedelta(days=1)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).drop_duplicates("datetime")
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)
    df.to_parquet(cache_f, index=False)
    return df

def _fetch_daily(sym: str, token: str, fr: date, to: date) -> pd.DataFrame:
    enc = _quote(INDEX_KEY.get(sym, f"NSE_INDEX|{sym}"), safe="")
    url = f"{UPSTOX_BASE}/historical-candle/{enc}/day/{to}/{fr}"
    r   = requests.get(url, headers=_hdr(token), timeout=15)
    if r.status_code != 200:
        return pd.DataFrame()
    rows = [{"date": c[0][:10], "open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4])}
            for c in reversed(r.json().get("data", {}).get("candles", []))]
    return pd.DataFrame(rows) if rows else pd.DataFrame()

# ── Zone computation helpers ───────────────────────────────────────────────────

def _zones_for_day(df_1m: pd.DataFrame, d_str: str, tf_min: int,
                   lookback_days: int = 5) -> list:
    """Return BEAR trap zones for one day on the given timeframe."""
    d_s   = pd.Timestamp(f"{d_str}T09:15:00")
    d_e   = pd.Timestamp(f"{d_str}T15:14:00")   # strip last stub
    lb_s  = d_s - pd.Timedelta(days=lookback_days)
    day   = df_1m[(df_1m["datetime"] >= lb_s) & (df_1m["datetime"] <= d_e)].copy()
    if "volume" not in day.columns:
        day["volume"] = 0
    if len(day) < tf_min * 2:
        return []
    htf = _resample_htf(day, tf_min)
    if len(htf) < 2:
        return []
    _, entries = scanner.scan_htf(htf)
    return [e for e in entries if e.get("kind", "BEAR") == "BEAR"]


def _day_exec_array(df_1m: pd.DataFrame, d_str: str) -> Optional[dict]:
    """1-min OHLC arrays for the trading day (09:15–15:29)."""
    d_s = pd.Timestamp(f"{d_str}T09:15:00")
    d_e = pd.Timestamp(f"{d_str}T15:29:00")
    day = df_1m[(df_1m["datetime"] >= d_s) & (df_1m["datetime"] <= d_e)].copy()
    if len(day) < 10:
        return None
    return {
        "high":  day["high"].values.astype(float),
        "low":   day["low"].values.astype(float),
        "close": day["close"].values.astype(float),
    }

# ── Core simulation ───────────────────────────────────────────────────────────

def _simulate(H, L, C, entry, sl, t1, lot) -> dict:
    """
    Single-target sim: entry at zone_trigger, SL=zone_low−buf, T1=MTF zone sl.
    P&L in INR = pts × lot.
    """
    size = float(lot)
    for i in range(len(H)):
        h, l, c = float(H[i]), float(L[i]), float(C[i])
        if l <= sl:
            return {"pnl": round((sl - entry) * size, 2),
                    "exit_reason": "SL", "exit_price": sl}
        if h >= t1:
            return {"pnl": round((t1 - entry) * size, 2),
                    "exit_reason": "T1", "exit_price": t1}
    ep = float(C[-1]) if len(C) else entry
    return {"pnl": round((ep - entry) * size, 2),
            "exit_reason": "EOD", "exit_price": ep}


def _run_day(d_str: str, exec_arr: dict,
             htf_zones: list, mtf_zones: list,
             sl_buf: float, lot: int,
             max_loss_rs: float, trig_frac: float,
             sl_hist: dict) -> Optional[dict]:
    """
    For one day, try to find a valid HTF→MTF entry.

    HTF zone: BEAR, active (price enters zone bounds today).
    MTF zone: BEAR, overlaps with HTF zone.
    Entry: MTF zone_trigger = zone_low + (zone_high − zone_low) × trig_frac
    Filter: (zone_trigger − SL) × lot ≤ max_loss_rs
    T1: MTF zone sl (ref bar HIGH = bears' stop level)
    SL: MTF zone_low − sl_buf
    """
    H = exec_arr["high"]
    L = exec_arr["low"]
    C = exec_arr["close"]

    for htf_z in htf_zones:
        if htf_z.get("kind", "BEAR") != "BEAR":
            continue

        htf_low  = float(htf_z.get("zone_low",  0))
        htf_high = float(htf_z.get("zone_high", 0))
        if htf_low <= 0 or htf_high <= 0:
            continue

        zone_key = f"{htf_low:.1f}-{htf_high:.1f}"
        # Dedup: skip if this zone SL'd yesterday
        if zone_key in sl_hist:
            if (date.fromisoformat(d_str) - date.fromisoformat(sl_hist[zone_key])).days <= 1:
                continue

        # HTF zone must be touched today (price enters zone)
        htf_entered = bool(np.any((C >= htf_low) & (C <= htf_high)))
        if not htf_entered:
            continue

        t1_htf = float(htf_z.get("sl", 0))   # HTF ref bar HIGH

        for mtf_z in mtf_zones:
            if mtf_z.get("kind", "BEAR") != "BEAR":
                continue

            mtf_low  = float(mtf_z.get("zone_low",  0))
            mtf_high = float(mtf_z.get("zone_high", 0))
            mtf_sl   = float(mtf_z.get("sl", 0))   # MTF ref bar HIGH = T1

            if mtf_low <= 0 or mtf_high <= 0 or mtf_sl <= 0:
                continue

            # MTF zone must be inside or overlapping HTF zone
            if mtf_high < htf_low or mtf_low > htf_high:
                continue

            # MTF T1 must be above entry zone (valid trap: squeeze target above zone)
            if mtf_sl <= mtf_high:
                continue

            # Compute entry trigger level
            zone_range  = mtf_high - mtf_low
            if zone_range <= 0:
                continue
            trig_price  = mtf_low + zone_range * trig_frac

            # SL = MTF zone_low − sl_buf
            sl_price    = mtf_low - sl_buf
            if sl_price <= 0:
                continue

            # SL distance and risk filter
            sl_dist     = trig_price - sl_price
            if sl_dist <= 0:
                continue
            max_loss_this = sl_dist * lot
            if max_loss_rs < NO_FILTER and max_loss_this > max_loss_rs:
                continue   # ← RISK FILTER: skip if max loss exceeds threshold

            # T1: use MTF sl (bears' stop on 5m chart)
            # If MTF sl is below our trigger price (can't happen for valid bear zone),
            # fall back to HTF sl.
            t1 = mtf_sl if mtf_sl > trig_price else t1_htf
            if t1 <= trig_price:
                continue

            # Find first 1m bar where price comes to trig_price
            entry_idx = None
            for i in range(len(L) - 1):
                if L[i] <= trig_price <= H[i] or C[i] <= trig_price:
                    entry_idx = i + 1   # enter on next bar open
                    break

            if entry_idx is None or entry_idx >= len(H):
                continue

            Hs = H[entry_idx:]
            Ls = L[entry_idx:]
            Cs = C[entry_idx:]

            res = _simulate(Hs, Ls, Cs, trig_price, sl_price, t1, lot)
            res.update({
                "date":        d_str,
                "zone_key":    zone_key,
                "entry_price": round(trig_price, 2),
                "sl_level":    round(sl_price, 2),
                "sl_dist":     round(sl_dist, 2),
                "max_loss_rs": round(max_loss_this, 0),
                "t1":          round(t1, 2),
                "htf_low":     htf_low,
                "htf_high":    htf_high,
                "mtf_low":     mtf_low,
                "mtf_high":    mtf_high,
                "trig_frac":   trig_frac,
            })
            if res["exit_reason"] == "SL":
                sl_hist[zone_key] = d_str
            return res   # one trade per day

    return None

# ── Summary ───────────────────────────────────────────────────────────────────

def _summarize(trades: list, params: dict) -> dict:
    wins   = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    gp = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    pf = round(gp / gl, 3) if gl > 0 else (9999.0 if gp > 0 else 0.0)
    return {
        "total":         len(trades),
        "wins":          len(wins),
        "losses":        len(losses),
        "win_rate_pct":  round(len(wins) / len(trades) * 100, 1) if trades else 0.0,
        "profit_factor": pf,
        "net_pnl_inr":   round(gp - gl, 0),
        "avg_win_inr":   round(gp / len(wins),    0) if wins   else 0.0,
        "avg_loss_inr":  round(gl / len(losses),  0) if losses else 0.0,
        "exits_sl":      sum(1 for t in trades if t.get("exit_reason") == "SL"),
        "exits_t1":      sum(1 for t in trades if t.get("exit_reason") == "T1"),
        "exits_eod":     sum(1 for t in trades if t.get("exit_reason") == "EOD"),
        "avg_sl_dist":   round(sum(t.get("sl_dist", 0) for t in trades) / len(trades), 1) if trades else 0.0,
        "avg_max_loss":  round(sum(t.get("max_loss_rs", 0) for t in trades) / len(trades), 0) if trades else 0.0,
        **params,
    }

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"=== BANKNIFTY Risk-Filter Backtest (HTF={HTF_MIN}m → MTF={MTF_MIN}m, No LTF) ===", flush=True)
    print(f"    Period: {START_DATE} → {END_DATE}  |  Lot: {LOT}  |  SL_buf: {SL_BUF}", flush=True)
    print(f"    Grid: max_loss={MAX_LOSS_GRID}  trig_frac={TRIG_FRAC_GRID}", flush=True)

    token = _get_token()
    if not token:
        print("[ERROR] No Upstox token in DB.", flush=True)
        sys.exit(1)
    print("[OK] token loaded", flush=True)

    fetch_fr = START_DATE - timedelta(days=LOOKBACK + 5)

    # Daily bars for trading day calendar
    print(f"\n[DATA] Fetching daily bars ...", flush=True)
    daily_df = _fetch_daily(SYMBOL, token, fetch_fr, END_DATE)
    if daily_df.empty:
        print("[ERROR] No daily data"); sys.exit(1)
    daily_map = {r["date"]: r for _, r in daily_df.iterrows()}
    all_days  = sorted(d.isoformat()
                       for d in (START_DATE + timedelta(i)
                                 for i in range((END_DATE - START_DATE).days + 1))
                       if d.isoformat() in daily_map)
    print(f"  {len(all_days)} trading days", flush=True)

    # 1m spot bars
    print(f"[DATA] Fetching 1m spot bars ...", flush=True)
    df1m = _fetch_1m(SYMBOL, token, fetch_fr, END_DATE)
    if df1m.empty:
        print("[ERROR] No 1m data"); sys.exit(1)
    if df1m["datetime"].dt.tz is not None:
        df1m["datetime"] = df1m["datetime"].dt.tz_localize(None)
    print(f"  {len(df1m):,} bars", flush=True)

    # Precompute zones and exec arrays for all days
    print(f"\n[ZONES] Precomputing HTF({HTF_MIN}m) + MTF({MTF_MIN}m) zones ...", flush=True)
    htf_cache: Dict[str, list] = {}
    mtf_cache: Dict[str, list] = {}
    exec_cache: Dict[str, Optional[dict]] = {}

    for d_str in all_days:
        htf_cache[d_str]  = _zones_for_day(df1m, d_str, HTF_MIN, LOOKBACK)
        mtf_cache[d_str]  = _zones_for_day(df1m, d_str, MTF_MIN, LOOKBACK)
        exec_cache[d_str] = _day_exec_array(df1m, d_str)

    days_with_htf = sum(1 for d in all_days if htf_cache[d])
    days_with_mtf = sum(1 for d in all_days if mtf_cache[d])
    print(f"  Days with HTF zones: {days_with_htf}/{len(all_days)}", flush=True)
    print(f"  Days with MTF zones: {days_with_mtf}/{len(all_days)}", flush=True)

    # ── Optimisation sweep ────────────────────────────────────────────────────
    print(f"\n[SWEEP] Running {len(MAX_LOSS_GRID) * len(TRIG_FRAC_GRID) + 1} combinations ...\n", flush=True)

    results = []

    # First run baseline: no risk filter (to isolate effect of HTF75/MTF5 alone)
    for trig_frac in TRIG_FRAC_GRID:
        sl_hist: dict = {}
        trades = []
        for d_str in all_days:
            ea = exec_cache.get(d_str)
            if ea is None:
                continue
            hz = htf_cache[d_str]
            mz = mtf_cache[d_str]
            if not hz or not mz:
                continue
            r = _run_day(d_str, ea, hz, mz, SL_BUF, LOT,
                         NO_FILTER, trig_frac, sl_hist)
            if r:
                trades.append(r)
        s = _summarize(trades, {
            "max_loss_rs": "NO_FILTER",
            "trig_frac":   trig_frac,
            "sl_buf":      SL_BUF,
            "lot":         LOT,
        })
        results.append(s)

    # Risk filter sweep
    for max_loss in MAX_LOSS_GRID:
        for trig_frac in TRIG_FRAC_GRID:
            sl_hist: dict = {}
            trades = []
            for d_str in all_days:
                ea = exec_cache.get(d_str)
                if ea is None:
                    continue
                hz = htf_cache[d_str]
                mz = mtf_cache[d_str]
                if not hz or not mz:
                    continue
                r = _run_day(d_str, ea, hz, mz, SL_BUF, LOT,
                             float(max_loss), trig_frac, sl_hist)
                if r:
                    trades.append(r)
            s = _summarize(trades, {
                "max_loss_rs": max_loss,
                "trig_frac":   trig_frac,
                "sl_buf":      SL_BUF,
                "lot":         LOT,
            })
            results.append(s)

    # ── Print results table ───────────────────────────────────────────────────
    print(f"\n{'='*110}")
    print(f"  BANKNIFTY Risk-Filter Backtest Results  ({START_DATE} → {END_DATE})")
    print(f"  HTF={HTF_MIN}m  MTF={MTF_MIN}m  No LTF  SL_buf={SL_BUF}  Lot={LOT}")
    print(f"{'='*110}")
    hdr = (f"  {'max_loss_rs':>12} {'trig_frac':>10} {'trades':>7} {'wr%':>6} "
           f"{'PF':>7} {'net_inr':>9} {'avgW':>8} {'avgL':>8} "
           f"{'SLs':>5} {'T1s':>5} {'EODs':>5} {'avg_sl_dist':>12}")
    print(hdr)
    print(f"  {'─'*106}")

    # Sort by profit factor descending
    results.sort(key=lambda x: x["profit_factor"], reverse=True)
    for s in results:
        print(
            f"  {str(s['max_loss_rs']):>12} {s['trig_frac']:>10.2f} "
            f"{s['total']:>7} {s['win_rate_pct']:>6.1f} "
            f"{s['profit_factor']:>7.3f} {s['net_pnl_inr']:>9,.0f} "
            f"{s['avg_win_inr']:>8,.0f} {s['avg_loss_inr']:>8,.0f} "
            f"{s['exits_sl']:>5} {s['exits_t1']:>5} {s['exits_eod']:>5} "
            f"{s['avg_sl_dist']:>12.1f}"
        )

    # ── Top 5 ─────────────────────────────────────────────────────────────────
    print(f"\n{'─'*110}")
    print(f"  TOP 5 BY PROFIT FACTOR:")
    print(f"{'─'*110}")
    for s in results[:5]:
        print(f"  max_loss=₹{s['max_loss_rs']}  frac={s['trig_frac']:.2f}  "
              f"trades={s['total']}  WR={s['win_rate_pct']}%  "
              f"PF={s['profit_factor']}  net=₹{s['net_pnl_inr']:,.0f}  "
              f"avg_SL_dist={s['avg_sl_dist']}pts")

    # ── Comparison with previous best ─────────────────────────────────────────
    best = results[0]
    print(f"\n{'='*110}")
    print(f"  COMPARISON WITH PREVIOUS BEST (4-tier cascade)")
    print(f"{'='*110}")
    print(f"  {'METRIC':<22} {'PREVIOUS BEST (4-tier)':>30} {'THIS VARIANT (best combo)':>30}")
    print(f"  {'─'*82}")
    prev = PREV_BEST
    metrics = [
        ("Strategy",        f"HTF={prev['htf_min']}m/MTF={prev['mtf_min']}m/LTF={prev['ltf_min']}m",
                            f"HTF={HTF_MIN}m/MTF={MTF_MIN}m/NoLTF"),
        ("Max_loss filter", "None (fixed lot)",
                            f"₹{best['max_loss_rs']}"),
        ("Trig fraction",   "LTF zone_high re-test",
                            f"{best['trig_frac']:.2f} of MTF zone"),
        ("DTE filter",      f"≤{prev['dte_filter']} days",
                            "None (all DTE)"),
        ("Win Rate",        f"{prev['win_rate_pct']}%",
                            f"{best['win_rate_pct']}%"),
        ("Profit Factor",   f"{prev['profit_factor']}",
                            f"{best['profit_factor']}"),
        ("Trades",          "—",
                            f"{best['total']}"),
        ("Net P&L",         "—",
                            f"₹{best['net_pnl_inr']:,.0f}"),
        ("Avg SL dist",     "—",
                            f"{best['avg_sl_dist']} pts"),
    ]
    for name, prev_v, this_v in metrics:
        print(f"  {name:<22} {prev_v:>30} {this_v:>30}")

    pf_diff = round(best["profit_factor"] - prev["profit_factor"], 3)
    wr_diff = round(best["win_rate_pct"]  - prev["win_rate_pct"],  1)
    print(f"\n  PF difference   : {pf_diff:+.3f}")
    print(f"  WR difference   : {wr_diff:+.1f}%")
    if best["profit_factor"] > prev["profit_factor"]:
        print(f"\n  VERDICT: ✓ This variant BEATS the previous 4-tier cascade on PF.")
    elif best["profit_factor"] > prev["profit_factor"] * 0.9:
        print(f"\n  VERDICT: ~ Within 10% of previous best. Simpler (no LTF) with risk control.")
    else:
        print(f"\n  VERDICT: ✗ Previous 4-tier cascade still superior on PF.")

    print(f"\n{'='*110}", flush=True)

    # ── Day-by-day trades for best combo ──────────────────────────────────────
    print(f"\n  DAY-BY-DAY TRADES (best combo: max_loss=₹{best['max_loss_rs']} frac={best['trig_frac']:.2f}):")
    print(f"  {'─'*90}")
    print(f"  {'DATE':<12} {'ENTRY':>8} {'SL':>8} {'T1':>8} {'SL_DIST':>8} "
          f"{'MAX_LOSS':>9} {'PNL_INR':>10} {'EXIT':>8}")
    print(f"  {'─'*90}")

    # Re-run best combo to collect day-by-day detail
    best_max_loss = best["max_loss_rs"]
    best_frac     = best["trig_frac"]
    sl_hist_final: dict = {}
    trades_final  = []
    for d_str in all_days:
        ea = exec_cache.get(d_str)
        if ea is None:
            continue
        hz = htf_cache[d_str]
        mz = mtf_cache[d_str]
        if not hz or not mz:
            continue
        ml = best_max_loss if best_max_loss != "NO_FILTER" else NO_FILTER
        r = _run_day(d_str, ea, hz, mz, SL_BUF, LOT,
                     float(ml), best_frac, sl_hist_final)
        if r:
            trades_final.append(r)
            pnl_s = f"₹{r['pnl']:>+8,.0f}"
            print(f"  {d_str:<12} {r['entry_price']:>8.1f} {r['sl_level']:>8.1f} "
                  f"{r['t1']:>8.1f} {r['sl_dist']:>8.1f} "
                  f"{r.get('max_loss_rs',0):>9,.0f} {pnl_s:>10} {r['exit_reason']:>8}")

    print(f"  {'─'*90}")
    print(f"  Total: {len(trades_final)} trades  |  "
          f"Wins: {sum(1 for t in trades_final if t['pnl']>0)}  |  "
          f"Net: ₹{sum(t['pnl'] for t in trades_final):,.0f}")
