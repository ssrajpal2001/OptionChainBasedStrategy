"""
scripts/nse_weekly_fetch_and_backtest.py
=========================================
Two tasks in one script:

Task A — Fetch Jul7 + Jul14 NIFTY weekly option 1m bars into cache
         (so future backtests use weekly expiry, not monthly Jul28)

Task B — Run full CE + PE combined June 2026 backtest using July28 monthly
         (June weekly expired data unavailable via Upstox API)
         Fix applied: PE now uses BEAR zones (same as CE — was incorrectly filtered out)

Run: python scripts/nse_weekly_fetch_and_backtest.py
"""
from __future__ import annotations
import json, os, sys, glob, time
from datetime import date, timedelta
from typing import Dict, List, Optional
from urllib.parse import quote as _q
import sqlite3, requests
import numpy as np
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from strategies.trap_scanner import scanner

CACHE_DIR   = os.path.join(ROOT, "data", "nse_option_cache")
UPSTOX_BASE = "https://api.upstox.com/v2"

# ── auth ──────────────────────────────────────────────────────────────────────
def _token():
    conn = sqlite3.connect(os.path.join(ROOT, "data", "clients.db"))
    row  = conn.execute(
        "SELECT access_token FROM system_feeder_creds WHERE provider='upstox' LIMIT 1"
    ).fetchone()
    conn.close()
    return (row[0] or "") if row else ""

def _hdr(t): return {"Authorization": f"Bearer {t}", "Accept": "application/json"}

# ── cascade helpers ───────────────────────────────────────────────────────────
HTF_MIN, MTF_MIN, LTF_MIN, EXEC_MIN = 75, 15, 5, 5
SL_BUF, CAP_PTS  = 10.0, 200
LOT, STEP         = 25, 50
FETCH_FROM        = date(2026, 7, 1)
ZONE_CUTOFF       = "15:14"

def _zk(z):
    sl, zh, zl = z.get("sl",0), z.get("zone_high",0), z.get("zone_low",0)
    if sl > zh: return "BEAR"
    if sl < zl: return "BULL"
    return "?"

def _zones(df):
    if len(df) < 3: return []
    try: _, z = scanner.scan_htf(df); return z or []
    except: return []

def _overlap(a, b):
    al,ah = float(a.get("zone_low",0)), float(a.get("zone_high",0))
    bl,bh = float(b.get("zone_low",0)), float(b.get("zone_high",0))
    buf = max((ah-al)*0.15, 0.5)
    return ah+buf >= bl and bh+buf >= al

def _resamp(df1m, m, cut=None):
    if df1m.empty or len(df1m) < 2: return pd.DataFrame()
    df = df1m.copy()
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_localize(None)
    if cut:
        h, mn = cut.split(":")
        from datetime import time as _t
        df = df[df["datetime"].dt.time <= _t(int(h), int(mn))]
    return (df.set_index("datetime")[["open","high","low","close"]]
              .resample(f"{m}min", closed="left", label="left")
              .agg({"open":"first","high":"max","low":"min","close":"last"})
              .dropna(subset=["close"]).reset_index())

def _sim(exc, lz, hz, lot):
    ll, lh = float(lz.get("zone_low",0)), float(lz.get("zone_high",0))
    hl     = float(hz.get("zone_low",0))
    t1     = float(hz.get("sl",0))
    sl     = hl - SL_BUF
    if t1 <= 0 or sl <= 0 or t1 <= lh: return None
    H=exc["high"].values.astype(float); L=exc["low"].values.astype(float); C=exc["close"].values.astype(float)
    buf = max((lh-ll)*0.15, 0.5)
    idxs = np.where((C >= ll-buf) & (C <= lh+buf))[0]; idxs = idxs[idxs < len(H)-1]
    for i in idxs:
        trig = float(H[i])
        if t1 <= trig or sl >= trig: continue
        hit = np.where(H[i+1:] >= trig)[0]
        if not len(hit): continue
        j = hit[0]; Hs=H[i+1+j:]; Ls=L[i+1+j:]; Cs=C[i+1+j:]
        for k in range(len(Hs)):
            if Ls[k] <= sl:
                return {"pnl":round((sl-trig)*lot,2),"exit":"SL",
                        "entry":round(trig,2),"sl":round(sl,2),
                        "target":round(min(t1,trig+CAP_PTS),2),
                        "zone_low":round(hl,2),"zone_high":round(float(hz.get("zone_high",0)),2),"ref_sl":round(t1,2)}
            if Hs[k] >= min(t1, trig+CAP_PTS):
                return {"pnl":round((min(t1,trig+CAP_PTS)-trig)*lot,2),"exit":"T1",
                        "entry":round(trig,2),"sl":round(sl,2),
                        "target":round(min(t1,trig+CAP_PTS),2),
                        "zone_low":round(hl,2),"zone_high":round(float(hz.get("zone_high",0)),2),"ref_sl":round(t1,2)}
        return {"pnl":round((float(Cs[-1])-trig)*lot,2),"exit":"EOD",
                "entry":round(trig,2),"sl":round(sl,2),
                "target":round(min(t1,trig+CAP_PTS),2),
                "zone_low":round(hl,2),"zone_high":round(float(hz.get("zone_high",0)),2),"ref_sl":round(t1,2)}
    return None

# ── fetch 1m bars ─────────────────────────────────────────────────────────────
def _fetch_1m(ikey: str, label: str, fr: date, to: date, token: str) -> pd.DataFrame:
    cache_f = os.path.join(CACHE_DIR, f"opt_{label}_{fr}_{to}.parquet")
    if os.path.exists(cache_f):
        df = pd.read_parquet(cache_f)
        df["datetime"] = pd.to_datetime(df["datetime"])
        if df["datetime"].dt.tz is not None:
            df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
        return df
    enc = _q(ikey, safe="")
    url = f"{UPSTOX_BASE}/historical-candle/{enc}/1minute/{to}/{fr}"
    r = requests.get(url, headers=_hdr(token), timeout=15)
    time.sleep(0.25)
    if r.status_code != 200:
        return pd.DataFrame()
    candles = r.json().get("data", {}).get("candles", [])
    if not candles: return pd.DataFrame()
    rows = [{"datetime": c[0], "open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4]), "volume": int(c[5])}
            for c in reversed(candles)]
    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["datetime"])
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    df.to_parquet(cache_f, index=False)
    return df

# ═══════════════════════════════════════════════════════════════════════════════
# TASK A — Fetch Jul7 + Jul14 weekly contracts
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_weekly_contracts(token: str):
    print("=" * 70)
    print("TASK A — Fetching NIFTY Jul7 + Jul14 WEEKLY option bars")
    print("=" * 70)

    weekly_expiries = [
        ("2026-07-07", date(2026, 7, 7)),
        ("2026-07-14", date(2026, 7, 14)),
    ]
    ikey_index = _q("NSE_INDEX|Nifty 50", safe="")
    STEP = 50
    FETCH_FROM = date(2026, 7, 1)

    for exp_str, exp_date in weekly_expiries:
        # Fetch option chain for this expiry
        url = f"{UPSTOX_BASE}/option/chain?instrument_key={ikey_index}&expiry_date={exp_str}"
        r = requests.get(url, headers=_hdr(token), timeout=15)
        if r.status_code != 200:
            print(f"  {exp_str}: failed HTTP {r.status_code}")
            continue
        chain = r.json().get("data", [])
        if not chain:
            print(f"  {exp_str}: no data")
            continue

        # Build contracts dict and save
        contracts = {}
        for item in chain:
            st = int(item.get("strike_price", 0))
            ce = item.get("call_options", {}).get("instrument_key", "")
            pe = item.get("put_options",  {}).get("instrument_key", "")
            if ce: contracts[f"{st}|CE"] = ce
            if pe: contracts[f"{st}|PE"] = pe

        contracts_file = os.path.join(CACHE_DIR, f"contracts_NIFTY_{exp_date}.json")
        with open(contracts_file, "w") as f:
            json.dump(contracts, f)
        print(f"  {exp_str}: {len(chain)} strikes saved -> {os.path.basename(contracts_file)}")

        # Find current ATM range to fetch (NIFTY ~24000-24500 area)
        # Fetch ATM ± 10 steps for CE and PE
        daily_f = os.path.join(CACHE_DIR, "daily_NIFTY.json")
        daily = json.load(open(daily_f))
        last_close = list(daily.values())[-1]
        atm_center = int(round(float(last_close) / STEP) * STEP)
        strikes_to_fetch = [atm_center + i*STEP for i in range(-12, 13)]

        fetched = 0
        for st in strikes_to_fetch:
            for otype in ("CE", "PE"):
                ikey_opt = contracts.get(f"{st}|{otype}")
                if not ikey_opt:
                    continue
                label = f"NIFTY{otype}{st}_W{exp_str}"
                cache_f = os.path.join(CACHE_DIR, f"opt_{label}_{FETCH_FROM}_{exp_date}.parquet")
                if os.path.exists(cache_f):
                    fetched += 1
                    continue
                df = _fetch_1m(ikey_opt, label, FETCH_FROM, exp_date, token)
                if not df.empty:
                    fetched += 1
        print(f"    Fetched/cached {fetched} option bar files for ATM+-12 range")

    print()

# ═══════════════════════════════════════════════════════════════════════════════
# TASK B — June 2026 CE + PE combined backtest (July28 monthly, PE fix applied)
# ═══════════════════════════════════════════════════════════════════════════════
def run_june_combined(token: str):
    print("=" * 70)
    print("TASK B — NIFTY June 2026 COMBINED CE+PE Backtest")
    print("Fix: PE now uses BEAR zones (same logic as CE)")
    print("Note: Using July28 monthly bars (June weekly data unavailable via API)")
    print("=" * 70)

    LOT, STEP = 25, 50
    START_DATE, END_DATE = date(2026, 6, 1), date(2026, 6, 30)
    EXPIRY = date(2026, 7, 28)

    # Load contracts (July28 monthly)
    contracts = json.load(open(os.path.join(CACHE_DIR, f"contracts_NIFTY_{EXPIRY}.json")))

    # Load cached option bars
    opt_cache: Dict[str, pd.DataFrame] = {}
    for f in glob.glob(os.path.join(CACHE_DIR, "opt_NIFTY*.parquet")):
        # only load monthly (no _W in name = monthly)
        if "_W" in os.path.basename(f): continue
        try:
            df = pd.read_parquet(f)
            df["datetime"] = pd.to_datetime(df["datetime"])
            if df["datetime"].dt.tz is not None:
                df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
            label = os.path.basename(f).replace("opt_","").split("_2026")[0]
            opt_cache[label] = df
        except: pass
    print(f"  Option bar cache: {len(opt_cache)} files loaded")

    # Load daily NIFTY spot closes
    daily = json.load(open(os.path.join(CACHE_DIR, "daily_NIFTY.json")))
    def gc(d):
        v = daily.get(str(d))
        return float(v) if v is not None else None

    days = [d for d in [START_DATE+timedelta(i) for i in range(31)]
            if d.weekday() < 5 and START_DATE <= d <= END_DATE]

    def get_day_bars(atm, otype, day):
        d_s = pd.Timestamp(f"{day}T09:15:00")
        d_e = pd.Timestamp(f"{day}T15:30:00")
        for off in [0, 1, -1, 2, -2]:
            st = atm + off*STEP if otype == "CE" else atm - off*STEP
            full = opt_cache.get(f"NIFTY{otype}{st}", pd.DataFrame())
            if full.empty: continue
            dd = full[(full["datetime"] >= d_s) & (full["datetime"] <= d_e)].copy()
            if len(dd) >= 30:
                return dd, st
        return pd.DataFrame(), atm

    all_trades: List[dict] = []
    sl_hist: Dict[str, date] = {}

    for day in days:
        prev_d = day - timedelta(1)
        prev_c = None
        for _ in range(5):
            prev_c = gc(prev_d)
            if prev_c: break
            prev_d -= timedelta(1)
        if not prev_c: continue
        atm = int(round(float(prev_c) / STEP) * STEP)

        for otype in ("CE", "PE"):
            opt_df, used_st = get_day_bars(atm, otype, day)
            if opt_df.empty: continue

            htf = _resamp(opt_df, HTF_MIN, ZONE_CUTOFF)
            mtf = _resamp(opt_df, MTF_MIN, ZONE_CUTOFF)
            ltf = _resamp(opt_df, LTF_MIN, ZONE_CUTOFF)
            exc = _resamp(opt_df, EXEC_MIN)
            if any(len(x) < 2 for x in [htf, mtf, ltf, exc]): continue

            hzs = _zones(htf); mzs = _zones(mtf); lzs = _zones(ltf)

            for hz in hzs:
                zk = _zk(hz)
                if zk == "?": continue
                # KEY FIX: both CE and PE use ALL zone kinds (BEAR dominant in trending market)
                # Old (wrong): BEAR->CE only, BULL->PE only
                # New (correct): any zone kind on any option type
                zkey = f"{otype}_{hz.get('zone_low',0):.0f}"
                if zkey in sl_hist and (day - sl_hist[zkey]).days <= 1: continue
                mm = next((z for z in mzs if _overlap(hz, z)), None)
                if not mm: continue
                lm = next((z for z in lzs if _overlap(mm, z)), None)
                if not lm: continue
                res = _sim(exc, lm, hz, LOT)
                if res:
                    res.update({"date": str(day), "opt": otype,
                                "atm": atm, "strike": used_st, "zone_kind": zk})
                    all_trades.append(res)
                    if res["exit"] == "SL": sl_hist[zkey] = day
                    break

    # ── print results ─────────────────────────────────────────────────────────
    ce_trades = [t for t in all_trades if t["opt"] == "CE"]
    pe_trades = [t for t in all_trades if t["opt"] == "PE"]

    def stats(lst):
        if not lst: return dict(n=0,w=0,wr=0.0,pf=0.0,net=0.0)
        w=[t for t in lst if t["pnl"]>0]; l=[t for t in lst if t["pnl"]<=0]
        gw=sum(t["pnl"] for t in w); gl=abs(sum(t["pnl"] for t in l))
        return dict(n=len(lst),w=len(w),wr=round(len(w)/len(lst)*100,1),
                    pf=round(gw/gl,2) if gl>0 else 9999.0,
                    net=round(sum(t["pnl"] for t in lst),2))

    print(f"\n  ALL {len(all_trades)} TRADES (CE + PE combined):")
    print(f"  {'#':3} {'Date':12} {'OPT':4} {'ATM':6} {'Strike':7} {'ZK':5} {'Entry':7} "
          f"{'SL':7} {'Target':8} {'ZoneLo':7} {'ZoneHi':7} {'RefSL':7} {'PnL':8} {'Exit'}")
    print("  " + "-"*115)

    for i, t in enumerate(all_trades, 1):
        pnl_str = f"{t['pnl']:+.0f}"
        marker = " <-- SL" if t["exit"]=="SL" else ""
        print(f"  {i:3} {t['date']:12} {t['opt']:4} {t['atm']:6} {t['strike']:7} "
              f"{t['zone_kind']:5} {t['entry']:7.1f} {t['sl']:7.1f} {t['target']:8.1f} "
              f"{t['zone_low']:7.1f} {t['zone_high']:7.1f} {t['ref_sl']:7.1f} "
              f"{pnl_str:8}  {t['exit']}{marker}")

    print("  " + "="*115)
    sc = stats(ce_trades); sp = stats(pe_trades); sa = stats(all_trades)
    print(f"\n  {'':12} {'Trades':>7} {'Wins':>6} {'WR%':>7} {'PF':>7} {'Net Rs':>10}")
    print(f"  {'-'*55}")
    print(f"  {'CE only':12} {sc['n']:>7} {sc['w']:>6} {sc['wr']:>6.1f}% {sc['pf']:>7.2f} {sc['net']:>10.0f}")
    print(f"  {'PE only':12} {sp['n']:>7} {sp['w']:>6} {sp['wr']:>6.1f}% {sp['pf']:>7.2f} {sp['net']:>10.0f}")
    print(f"  {'COMBINED':12} {sa['n']:>7} {sa['w']:>6} {sa['wr']:>6.1f}% {sa['pf']:>7.2f} {sa['net']:>10.0f}")

    print(f"\n  CAVEAT: Monthly (Jul28) contracts used — weekly expiry bars unavailable")
    print(f"  for expired June contracts. Weekly options have higher gamma/sensitivity.")
    print(f"  Jul7 + Jul14 weekly bars fetched in TASK A for future weekly backtests.")

# ═══════════════════════════════════════════════════════════════════════════════
# TASK C — Compare Jul7 (weekly) vs Jul14 (next-week) on the available July days
# ═══════════════════════════════════════════════════════════════════════════════
def _load_weekly_opt_cache(expiry: date) -> Dict[str, pd.DataFrame]:
    """Load all cached NIFTY weekly option bars for one expiry."""
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


def _get_weekly_day_bars(opt_cache: Dict[str, pd.DataFrame], atm: int, otype: str, day: date):
    d_s = pd.Timestamp(f"{day}T09:15:00")
    d_e = pd.Timestamp(f"{day}T15:30:00")
    for off in [0, 1, -1, 2, -2]:
        st = atm + off * STEP if otype == "CE" else atm - off * STEP
        label = f"NIFTY{otype}{st}"
        for key in [k for k in opt_cache if k.startswith(label + "_W")]:
            full = opt_cache[key]
            if full.empty:
                continue
            day_df = full[(full["datetime"] >= d_s) & (full["datetime"] <= d_e)].copy()
            if len(day_df) >= 30:
                return day_df, st
    return pd.DataFrame(), atm


def _stats(lst):
    if not lst:
        return dict(n=0, w=0, wr=0.0, pf=0.0, net=0.0)
    wins = [t for t in lst if t["pnl"] > 0]
    losses = [t for t in lst if t["pnl"] <= 0]
    gw = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    return dict(
        n=len(lst), w=len(wins),
        wr=round(len(wins) / len(lst) * 100, 1),
        pf=round(gw / gl, 2) if gl > 0 else 9999.0,
        net=round(sum(t["pnl"] for t in lst), 2),
    )


def run_weekly_compare(token: str):
    print("\n" + "=" * 70)
    print("TASK C — NIFTY Weekly (Jul7) vs Next-Week (Jul14) backtest")
    print("=" * 70)

    daily = json.load(open(os.path.join(CACHE_DIR, "daily_NIFTY.json")))

    end = min(date(2026, 7, 14), date.today())
    days = []
    d = FETCH_FROM
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    print(f"  Available trade days: {[str(x) for x in days]}")

    weekly_expiries = [date(2026, 7, 7), date(2026, 7, 14)]

    print(f"\n  {'Expiry':12} {'Trades':>7} {'Wins':>6} {'WR%':>7} {'PF':>7} {'Net Rs':>10}")
    print("  " + "-" * 60)

    all_weekly_trades: List[dict] = []
    for exp in weekly_expiries:
        opt_cache = _load_weekly_opt_cache(exp)
        trades: List[dict] = []
        sl_hist: Dict[str, date] = {}

        for day in days:
            prev_d = day - timedelta(days=1)
            prev_c = None
            for _ in range(5):
                prev_c = daily.get(str(prev_d))
                if prev_c:
                    break
                prev_d -= timedelta(days=1)
            if not prev_c:
                continue
            atm = int(round(float(prev_c) / STEP) * STEP)

            for otype in ("CE", "PE"):
                opt_df, used_st = _get_weekly_day_bars(opt_cache, atm, otype, day)
                if opt_df.empty:
                    continue

                htf = _resamp(opt_df, HTF_MIN, ZONE_CUTOFF)
                mtf = _resamp(opt_df, MTF_MIN, ZONE_CUTOFF)
                ltf = _resamp(opt_df, LTF_MIN, ZONE_CUTOFF)
                exc = _resamp(opt_df, EXEC_MIN)
                if any(len(x) < 2 for x in [htf, mtf, ltf, exc]):
                    continue

                hzs = _zones(htf); mzs = _zones(mtf); lzs = _zones(ltf)
                for hz in hzs:
                    zk = _zk(hz)
                    if zk == "?":
                        continue
                    zkey = f"{otype}_{hz.get('zone_low', 0):.0f}_{exp}"
                    if zkey in sl_hist and (day - sl_hist[zkey]).days <= 1:
                        continue
                    mm = next((z for z in mzs if _overlap(hz, z)), None)
                    if not mm:
                        continue
                    lm = next((z for z in lzs if _overlap(mm, z)), None)
                    if not lm:
                        continue
                    res = _sim(exc, lm, hz, LOT)
                    if res:
                        res.update({"date": str(day), "opt": otype,
                                    "atm": atm, "strike": used_st,
                                    "zone_kind": zk, "expiry": str(exp)})
                        trades.append(res)
                        all_weekly_trades.append(res)
                        if res["exit"] == "SL":
                            sl_hist[zkey] = day
                        break

        s = _stats(trades)
        print(f"  {str(exp):12} {s['n']:>7} {s['w']:>6} {s['wr']:>6.1f}% {s['pf']:>7.2f} {s['net']:>10.0f}")

        if trades:
            print(f"\n    Trades for {exp}:")
            print(f"    {'Date':12} {'OPT':4} {'Strike':7} {'Entry':8} {'PnL':>8} {'Exit'}")
            for t in trades:
                print(f"    {t['date']:12} {t['opt']:4} {t['strike']:7} {t['entry']:8.1f} "
                      f"{t['pnl']:+8.0f}  {t['exit']}")

    sa = _stats(all_weekly_trades)
    print("  " + "=" * 60)
    print(f"  {'COMBINED':12} {sa['n']:>7} {sa['w']:>6} {sa['wr']:>6.1f}% {sa['pf']:>7.2f} {sa['net']:>10.0f}")
    print("  NOTE: sample is small because only a few July days have been downloaded so far.")


# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    token = _token()
    if not token:
        print("ERROR: No token"); import sys; sys.exit(1)
    fetch_weekly_contracts(token)
    run_june_combined(token)
    run_weekly_compare(token)
