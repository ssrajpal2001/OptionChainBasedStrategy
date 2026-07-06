"""
scripts/nifty_weekly_trailing_sl_backtest.py
============================================
NIFTY option backtest with:
  - Strike selection from NIFTY spot close (prev day -> ATM)
  - Current-week + next-week expiry scanning
  - Fixed SL = Rs 2,000 per lot (80 pts on a 25-lot NIFTY option)
  - When floating profit >= 40 pts, move SL to cost-to-cost (CTC)
  - Then trail SL at running_high - 20 pts

Because Upstox does not keep expired weekly contracts in the public master,
true June 2026 weekly bars are not downloadable.  The script therefore runs:

  1. A "June proxy" using the existing July-monthly option bars for June days
     (same premium logic, just a wider expiry).
  2. A real current/next-week expiry run on the available July weekly bars
     (only 3 market days downloaded so far).

Run: python scripts/nifty_weekly_trailing_sl_backtest.py
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
from strategies.trap_scanner import scanner

CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")
DB_PATH = os.path.join(ROOT, "data", "clients.db")
UPSTOX_BASE = "https://api.upstox.com/v2"

SYMBOL = "NIFTY"
LOT = 65
STEP = 50
MAX_RISK_RS = 2000.0
FIXED_SL_POINTS = MAX_RISK_RS / LOT          # ~30.77 option premium points
CTC_PROFIT_POINTS = 40.0
TRAIL_BUFFER_POINTS = 20.0
WEEKDAY_EXPIRY = 3                            # Thursday for NIFTY weekly

HTF_MIN, MTF_MIN, LTF_MIN, EXEC_MIN = 75, 15, 5, 5
ZONE_CUTOFF = "15:14"


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


# ── NIFTY daily spot closes ───────────────────────────────────────────────────
def _spot_daily() -> Dict[str, float]:
    cache_f = os.path.join(CACHE_DIR, "daily_NIFTY.json")
    if os.path.exists(cache_f):
        data = json.load(open(cache_f))
        return {k: float(v) if isinstance(v, (int, float)) else float(v.get("close", 0))
                for k, v in data.items()}
    return {}


# ── expiry helpers ────────────────────────────────────────────────────────────
def _current_week_expiry(d: date) -> date:
    """Next Thursday on or after d."""
    dd = d
    while dd.weekday() != WEEKDAY_EXPIRY:
        dd += timedelta(days=1)
    return dd


def _next_week_expiry(d: date) -> date:
    return _current_week_expiry(d) + timedelta(days=7)


# ── contract loading from Upstox master ───────────────────────────────────────
_master_cache: Optional[List[dict]] = None


def _load_master() -> List[dict]:
    global _master_cache
    if _master_cache is not None:
        return _master_cache
    import gzip
    cache_f = os.path.join(CACHE_DIR, f"nse_master_{date.today()}.json")
    if os.path.exists(cache_f):
        with open(cache_f) as f:
            _master_cache = json.load(f)
        return _master_cache
    print("  Downloading NSE master...", flush=True)
    r = requests.get(
        "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
        timeout=30,
    )
    data = json.loads(gzip.decompress(r.content))
    with open(cache_f, "w") as f:
        json.dump(data, f)
    _master_cache = data
    return data


def _get_contracts(expiry: date) -> Dict[Tuple[int, str], str]:
    """Return {(strike, 'CE'|'PE'): instrument_key} for NIFTY on expiry."""
    cache_f = os.path.join(CACHE_DIR, f"contracts_NIFTY_{expiry}.json")
    if os.path.exists(cache_f):
        raw = json.load(open(cache_f))
        if raw:
            return {(int(k.split("|")[0]), k.split("|")[1]): v for k, v in raw.items()}

    master = _load_master()
    result: Dict[Tuple[int, str], str] = {}
    for inst in master:
        name = str(inst.get("trading_symbol", "") or inst.get("name", "")).upper()
        itype = str(inst.get("instrument_type", "")).upper()
        if "NIFTY" not in name or itype not in ("CE", "PE", "CALL", "PUT"):
            continue
        exp_raw = inst.get("expiry") or inst.get("expiry_date") or ""
        try:
            if isinstance(exp_raw, (int, float)):
                exp_d = __import__("datetime").datetime.utcfromtimestamp(int(exp_raw) / 1000).date()
            elif len(str(exp_raw)) == 10 and "-" in str(exp_raw):
                exp_d = date.fromisoformat(str(exp_raw))
            else:
                continue
        except Exception:
            continue
        if exp_d != expiry:
            continue
        ikey = inst.get("instrument_key", "")
        strike = inst.get("strike_price") or inst.get("strike") or 0
        try:
            strike = int(float(strike))
        except Exception:
            continue
        otype = "CE" if itype in ("CE", "CALL") else "PE"
        if ikey and strike > 0:
            result[(strike, otype)] = ikey
    if result:
        with open(cache_f, "w") as f:
            json.dump({f"{k[0]}|{k[1]}": v for k, v in result.items()}, f)
    return result


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
    """Load the existing non-weekly NIFTY option bars (June/July monthly)."""
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


# ── day-bar selector ──────────────────────────────────────────────────────────
def _get_day_bars(opt_cache: Dict[str, pd.DataFrame], atm: int, otype: str, day: date,
                  expiry: Optional[date] = None) -> Tuple[pd.DataFrame, int]:
    d_s = pd.Timestamp(f"{day}T09:15:00")
    d_e = pd.Timestamp(f"{day}T15:30:00")
    for off in [0, 1, -1, 2, -2]:
        st = atm + off * STEP if otype == "CE" else atm - off * STEP
        label = f"NIFTY{otype}{st}"
        keys = [k for k in opt_cache if k.startswith(label + (f"_W{expiry}" if expiry else ""))]
        for key in keys:
            full = opt_cache[key]
            if full.empty:
                continue
            day_df = full[(full["datetime"] >= d_s) & (full["datetime"] <= d_e)].copy()
            if len(day_df) >= 30:
                return day_df, st
    return pd.DataFrame(), atm


# ── simulation with fixed SL / CTC / trailing SL ──────────────────────────────
def _simulate_risk(exec_df: pd.DataFrame, entry_idx: int, entry_price: float, lot: int) -> dict:
    """
    - SL fixed at entry - FIXED_SL_POINTS (80 pts -> Rs 2,000 on 25 lot)
    - When running_high - entry >= CTC_PROFIT_POINTS (40 pts), move SL to entry
    - After CTC, trail SL at running_high - TRAIL_BUFFER_POINTS (20 pts)
    """
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
                "pnl": round((sl - entry_price) * lot, 2),
                "exit": "SL",
                "entry": round(entry_price, 2),
                "sl": round(sl, 2),
                "max_high": round(running_high, 2),
            }
        running_high = max(running_high, H[k])
        if running_high - entry_price >= CTC_PROFIT_POINTS:
            sl = max(sl, entry_price)          # CTC
            ctc_active = True
        if ctc_active:
            sl = max(sl, running_high - TRAIL_BUFFER_POINTS)

    return {
        "pnl": round((float(C[-1]) - entry_price) * lot, 2),
        "exit": "EOD",
        "entry": round(entry_price, 2),
        "sl": round(sl, 2),
        "max_high": round(running_high, 2),
    }


def _find_entry_and_trade(exec_df: pd.DataFrame, ltf_zone: dict, htf_zone: dict):
    """Return (entry_idx, entry_price) if a trigger is found, else None."""
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
             expiry: Optional[date], label: str) -> List[dict]:
    trades: List[dict] = []
    sl_hist: Dict[str, bool] = {}

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
            zk = _zone_kind(hz)
            if zk != "BEAR":
                continue
            zkey = f"{label}_{otype}_{hz.get('zone_low', 0):.0f}"
            if zkey in sl_hist:
                continue
            mm = next((z for z in mzs if _overlap(hz, z)), None)
            if not mm:
                continue
            lm = next((z for z in lzs if _overlap(mm, z)), None)
            if not lm:
                continue
            entry = _find_entry_and_trade(exc, lm, hz)
            if not entry:
                continue
            idx, price = entry
            res = _simulate_risk(exc, idx, price, LOT)
            if not res:
                continue
            res.update({
                "date": str(day),
                "opt": otype,
                "atm": atm,
                "strike": used_st,
                "expiry": str(expiry) if expiry else "monthly_proxy",
                "mode": label,
                "zone_kind": zk,
            })
            trades.append(res)
            if res["exit"] == "SL":
                sl_hist[zkey] = True
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
def run_june_proxy(spot: Dict[str, float]) -> List[dict]:
    print("\n" + "=" * 70)
    print("RUN 1 — June 2026 proxy using July-monthly option bars")
    print("(True June weekly bars are not available via Upstox master)")
    print("=" * 70)

    opt_cache = _load_monthly_cache()
    print(f"  Monthly option cache loaded: {len(opt_cache)} files")

    start, end = date(2026, 6, 1), date(2026, 6, 30)
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)
            if (start + timedelta(days=i)).weekday() < 5]

    all_trades: List[dict] = []
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
        all_trades.extend(_run_day(day, atm, opt_cache, None, "june_proxy"))

    s = _stats(all_trades)
    print(f"\n  Trades={s['n']}  Wins={s['w']}  WR={s['wr']:.1f}%  PF={s['pf']:.2f}  Net=Rs {s['net']:.0f}")
    if all_trades:
        print(f"\n  {'Date':12} {'OPT':4} {'Strike':7} {'Entry':8} {'SL':8} {'MaxH':8} {'PnL':>8} {'Exit'}")
        print("  " + "-" * 70)
        for t in all_trades:
            print(f"  {t['date']:12} {t['opt']:4} {t['strike']:7} {t['entry']:8.1f} "
                  f"{t['sl']:8.1f} {t['max_high']:8.1f} {t['pnl']:+8.0f}  {t['exit']}")
    return all_trades


# ═══════════════════════════════════════════════════════════════════════════════
def _available_weekly_expiries() -> List[date]:
    """Find which weekly expiries already have cached option bars."""
    exps = set()
    for f in glob.glob(os.path.join(CACHE_DIR, "opt_NIFTY*_W*.parquet")):
        name = os.path.basename(f)
        # opt_NIFTYCE23050_W2026-07-07_...
        if "_W" in name:
            part = name.split("_W")[1].split("_")[0]
            try:
                exps.add(date.fromisoformat(part))
            except Exception:
                pass
    return sorted(exps)


def run_july_weekly(spot: Dict[str, float]) -> List[dict]:
    print("\n" + "=" * 70)
    print("RUN 2 — Real weekly expiry bars currently in cache")
    print("(These are the only weekly expiries downloaded so far)")
    print("=" * 70)

    weekly_expiries = _available_weekly_expiries()
    if not weekly_expiries:
        print("  No weekly option bars cached yet.")
        return []
    print(f"  Cached weekly expiries: {[str(e) for e in weekly_expiries]}")

    start = date(2026, 7, 1)
    end = min(date(2026, 7, 5), date.today())
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    print(f"  Available July trade days: {[str(x) for x in days]}")

    all_trades: List[dict] = []
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

        for exp in weekly_expiries:
            opt_cache = _load_weekly_cache(exp)
            label = f"jul_{exp}"
            all_trades.extend(_run_day(day, atm, opt_cache, exp, label))

    # Print by expiry
    for exp in sorted({t["expiry"] for t in all_trades}):
        sub = [t for t in all_trades if t["expiry"] == exp]
        s = _stats(sub)
        print(f"\n  Expiry {exp}: Trades={s['n']} Wins={s['w']} WR={s['wr']:.1f}% "
              f"PF={s['pf']:.2f} Net=Rs {s['net']:.0f}")
        for t in sub:
            print(f"    {t['date']} {t['opt']} strike={t['strike']} entry={t['entry']:.1f} "
                  f"pnl={t['pnl']:+7.0f} exit={t['exit']}")

    s = _stats(all_trades)
    print(f"\n  Combined July weekly: Trades={s['n']} Wins={s['w']} WR={s['wr']:.1f}% "
          f"PF={s['pf']:.2f} Net=Rs {s['net']:.0f}")
    return all_trades


# ═══════════════════════════════════════════════════════════════════════════════
# RUN 3 — Max-OI strike selection, trade 1 ITM, SL on scanner strike
# ═══════════════════════════════════════════════════════════════════════════════
def _build_oi_table(strikes: List[int]) -> Dict[str, Dict[int, Dict[str, int]]]:
    """Prev-day EOD OI table from daily option bars (July monthly contracts)."""
    contracts = _get_contracts(date(2026, 7, 28))
    oi_table: Dict[str, Dict[int, Dict[str, int]]] = {}
    for st in strikes:
        for otype in ("CE", "PE"):
            ikey = contracts.get((st, otype))
            if not ikey:
                continue
            f = os.path.join(
                CACHE_DIR,
                f"daily_opt_{ikey.replace('|', '_')}_2026-05-27_2026-06-30.parquet",
            )
            if not os.path.exists(f):
                continue
            df = pd.read_parquet(f)
            for row in df.itertuples():
                d = str(row.date)
                if d not in oi_table:
                    oi_table[d] = {}
                if st not in oi_table[d]:
                    oi_table[d][st] = {"CE": 0, "PE": 0}
                oi_table[d][st][otype] = int(row.oi)
    return oi_table


def _oi_strikes(oi_day: Dict[int, Dict[str, int]]) -> dict:
    if not oi_day:
        return {}
    ce = {s: oi_day[s].get("CE", 0) for s in oi_day}
    pe = {s: oi_day[s].get("PE", 0) for s in oi_day}
    max_ce = max(ce, key=ce.get) if ce else None
    max_pe = max(pe, key=pe.get) if pe else None
    return {"max_ce_strike": max_ce, "max_pe_strike": max_pe}


def _simulate_pair(merged: pd.DataFrame, entry_idx: int,
                   scanner_entry: float, trade_entry: float, lot: int) -> dict:
    sl = scanner_entry - FIXED_SL_POINTS
    running_high = scanner_entry
    ctc_active = False

    for k in range(entry_idx + 1, len(merged)):
        row = merged.iloc[k]
        scanner_low = float(row["low_s"])
        scanner_high = float(row["high_s"])
        trade_low = float(row["low_t"])
        trade_close = float(row["close_t"])

        if scanner_low <= sl:
            return {
                "pnl": round((trade_low - trade_entry) * lot, 2),
                "exit": "SL",
                "scanner_entry": round(scanner_entry, 2),
                "trade_entry": round(trade_entry, 2),
                "sl": round(sl, 2),
                "max_high": round(running_high, 2),
            }
        running_high = max(running_high, scanner_high)
        if running_high - scanner_entry >= CTC_PROFIT_POINTS:
            sl = max(sl, scanner_entry)
            ctc_active = True
        if ctc_active:
            sl = max(sl, running_high - TRAIL_BUFFER_POINTS)

    return {
        "pnl": round((float(merged.iloc[-1]["close_t"]) - trade_entry) * lot, 2),
        "exit": "EOD",
        "scanner_entry": round(scanner_entry, 2),
        "trade_entry": round(trade_entry, 2),
        "sl": round(sl, 2),
        "max_high": round(running_high, 2),
    }


def _run_max_oi_day(day: date, oi_day: dict, opt_cache: Dict[str, pd.DataFrame]) -> Optional[dict]:
    info = _oi_strikes(oi_day)
    if not info:
        return None

    d_s = pd.Timestamp(f"{day}T09:15:00")
    d_e = pd.Timestamp(f"{day}T15:30:00")

    candidates = []
    # CE: scanner = max CE OI strike, trade = 1 ITM CE (strike - STEP)
    if info.get("max_ce_strike"):
        scanner_st = info["max_ce_strike"]
        trade_st = scanner_st - STEP
        candidates.append(("CE", scanner_st, trade_st))
    # PE: scanner = max PE OI strike, trade = 1 ITM PE (strike + STEP)
    if info.get("max_pe_strike"):
        scanner_st = info["max_pe_strike"]
        trade_st = scanner_st + STEP
        candidates.append(("PE", scanner_st, trade_st))

    trades = []
    for otype, scanner_st, trade_st in candidates:
        scanner_df = _day_bars_for_strike(opt_cache, scanner_st, otype, day)
        trade_df = _day_bars_for_strike(opt_cache, trade_st, otype, day)
        if scanner_df.empty or trade_df.empty:
            continue

        htf = _resamp(scanner_df, HTF_MIN, ZONE_CUTOFF)
        mtf = _resamp(scanner_df, MTF_MIN, ZONE_CUTOFF)
        ltf = _resamp(scanner_df, LTF_MIN, ZONE_CUTOFF)
        scanner_exec = _resamp(scanner_df, EXEC_MIN)
        trade_exec = _resamp(trade_df, EXEC_MIN)
        if any(len(x) < 2 for x in [htf, mtf, ltf, scanner_exec, trade_exec]):
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
            entry = _find_entry_and_trade(scanner_exec, lm, hz)
            if not entry:
                continue
            idx, scanner_entry_price = entry
            trigger_dt = scanner_exec["datetime"].iloc[idx]

            merged = pd.merge(
                scanner_exec.rename(columns={"open": "open_s", "high": "high_s",
                                             "low": "low_s", "close": "close_s"}),
                trade_exec.rename(columns={"open": "open_t", "high": "high_t",
                                           "low": "low_t", "close": "close_t"}),
                on="datetime",
                how="inner",
            )
            merged_pos = merged[merged["datetime"] == trigger_dt]
            if merged_pos.empty:
                continue
            m_idx = merged_pos.index[0]
            trade_entry_price = float(merged_pos.iloc[0]["high_t"])

            res = _simulate_pair(merged, m_idx, scanner_entry_price, trade_entry_price, LOT)
            res.update({
                "date": str(day),
                "opt": otype,
                "scanner_strike": scanner_st,
                "trade_strike": trade_st,
                "mode": "max_oi",
                "trigger_dt": str(trigger_dt),
            })
            trades.append(res)
            break  # one trade per side

    # Return the earliest-triggered trade of the day (one ITM CE or PE)
    if not trades:
        return None
    trades.sort(key=lambda x: x["trigger_dt"])
    return trades[0]


def _day_bars_for_strike(opt_cache: Dict[str, pd.DataFrame], strike: int, otype: str, day: date) -> pd.DataFrame:
    d_s = pd.Timestamp(f"{day}T09:15:00")
    d_e = pd.Timestamp(f"{day}T15:30:00")
    label = f"NIFTY{otype}{strike}"
    for key in [k for k in opt_cache if k.startswith(label)]:
        full = opt_cache[key]
        if full.empty:
            continue
        return full[(full["datetime"] >= d_s) & (full["datetime"] <= d_e)].copy()
    return pd.DataFrame()


def run_max_oi(spot: Dict[str, float]) -> List[dict]:
    print("\n" + "=" * 70)
    print("RUN 3 — Max-OI strike selection, trade 1 ITM, SL on scanner strike")
    print("Scanner strike = max OI strike | Trade strike = 1 ITM from scanner")
    print("=" * 70)

    opt_cache = _load_monthly_cache()

    # Scan strikes around the June range
    atm_values = [v for v in spot.values() if isinstance(v, (int, float)) and v > 0]
    mid = int(round(sum(atm_values) / len(atm_values) / STEP) * STEP) if atm_values else 23850
    strikes = list(range(mid - 20 * STEP, mid + 21 * STEP, STEP))
    print(f"  OI scan range: {strikes[0]} – {strikes[-1]}")

    oi_table = _build_oi_table(strikes)
    print(f"  OI table built for {len(oi_table)} days")

    start, end = date(2026, 6, 1), date(2026, 6, 30)
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)
            if (start + timedelta(days=i)).weekday() < 5]

    trades: List[dict] = []
    for day in days:
        prev_d = day - timedelta(days=1)
        oi_day = None
        for _ in range(5):
            oi_day = oi_table.get(str(prev_d))
            if oi_day:
                break
            prev_d -= timedelta(days=1)
        if not oi_day:
            continue
        t = _run_max_oi_day(day, oi_day, opt_cache)
        if t:
            trades.append(t)

    s = _stats(trades)
    print(f"\n  Trades={s['n']}  Wins={s['w']}  WR={s['wr']:.1f}%  PF={s['pf']:.2f}  Net=Rs {s['net']:.0f}")
    if trades:
        print(f"\n  {'Date':12} {'OPT':4} {'Scan':7} {'Trade':7} {'ScEntry':9} {'TrEntry':9} {'PnL':>8} {'Exit'}")
        print("  " + "-" * 75)
        for t in trades:
            print(f"  {t['date']:12} {t['opt']:4} {t['scanner_strike']:7} {t['trade_strike']:7} "
                  f"{t['scanner_entry']:9.1f} {t['trade_entry']:9.1f} {t['pnl']:+8.0f}  {t['exit']}")
    return trades


# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    spot = _spot_daily()
    if not spot:
        print("ERROR: No daily NIFTY spot cache. Run nse_ob_option_backtest.py first.")
        sys.exit(1)

    print("NIFTY trailing-SL backtest")
    print(f"  Fixed risk = Rs {MAX_RISK_RS:.0f}/lot -> SL = {FIXED_SL_POINTS:.0f} option pts")
    print(f"  CTC trigger = +{CTC_PROFIT_POINTS:.0f} pts | Trail buffer = {TRAIL_BUFFER_POINTS:.0f} pts")

    run_june_proxy(spot)
    run_july_weekly(spot)
    run_max_oi(spot)

    print("\nDone.")
