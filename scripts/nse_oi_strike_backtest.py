"""
scripts/nse_oi_strike_backtest.py
==================================
OI-based strike selection vs ATM baseline backtest.

Logic:
  - For each trade day D, look at prev day (D-1) EOD OI for all strikes
  - Max CALL OI strike  → where call writers are positioned → CE scan
  - Max PUT  OI strike  → where put writers are positioned → PE scan
  - Max Pain strike     → where total buyer loss is highest (market gravity)
  - Run 75m/15m/5m cascade on those strikes vs plain ATM±offset

Period  : June 2026 (1 month)
Expiry  : July monthly (NIFTY/BANKNIFTY=Jul28, SENSEX=Jul30)
Symbols : NIFTY, BANKNIFTY, SENSEX

Run: python scripts/nse_oi_strike_backtest.py
"""
from __future__ import annotations

import json, os, sys, time, sqlite3
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote as _quote

import numpy as np
import pandas as pd
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from strategies.trap_scanner import scanner

# ── config ────────────────────────────────────────────────────────────────────
START_DATE   = date(2026, 6, 1)
END_DATE     = date(2026, 6, 30)
HTF_MIN, MTF_MIN, LTF_MIN, EXEC_MIN = 75, 15, 5, 5
SL_BUF, CAP_PTS = 10.0, 200
ZONE_CUTOFF  = "15:14"

SYMBOLS      = ["NIFTY", "BANKNIFTY", "SENSEX"]
LOT_SIZES    = {"NIFTY": 25, "BANKNIFTY": 15, "SENSEX": 10}
STRIKE_STEPS = {"NIFTY": 50, "BANKNIFTY": 100, "SENSEX": 100}
OI_SCAN_STEPS = 20          # scan ATM ± this many steps for OI structure
JULY_EXPIRY  = {
    "NIFTY":     date(2026, 7, 28),
    "BANKNIFTY": date(2026, 7, 28),
    "SENSEX":    date(2026, 7, 30),
}

UPSTOX_BASE = "https://api.upstox.com/v2"
CACHE_DIR   = os.path.join(ROOT, "data", "nse_option_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# ── auth ──────────────────────────────────────────────────────────────────────
def _get_token() -> str:
    try:
        conn = sqlite3.connect(os.path.join(ROOT, "data", "clients.db"))
        row  = conn.execute(
            "SELECT access_token FROM system_feeder_creds WHERE provider='upstox' LIMIT 1"
        ).fetchone()
        conn.close()
        return (row[0] or "") if row else ""
    except Exception:
        return ""

def _hdr(token): return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

# ── contracts ─────────────────────────────────────────────────────────────────
def _load_contracts(sym: str, expiry: date) -> Dict[Tuple[int,str], str]:
    cache_f = os.path.join(CACHE_DIR, f"contracts_{sym}_{expiry}.json")
    if not os.path.exists(cache_f):
        print(f"  ERROR: contracts cache missing for {sym} {expiry}.")
        print(f"         Run nse_ob_option_backtest.py first to build it.")
        return {}
    with open(cache_f) as f:
        raw = json.load(f)
    return {(int(k.split("|")[0]), k.split("|")[1]): v for k, v in raw.items()}

# ── fetch daily bars (OI in last field) ───────────────────────────────────────
def _fetch_daily(instrument_key: str, token: str,
                 fr: date, to: date) -> pd.DataFrame:
    """Fetch day bars for one option contract. Returns df with [date, oi]."""
    cache_f = os.path.join(CACHE_DIR, f"daily_opt_{instrument_key.replace('|','_')}_{fr}_{to}.parquet")
    if os.path.exists(cache_f):
        return pd.read_parquet(cache_f)
    enc = _quote(instrument_key, safe="")
    url = f"{UPSTOX_BASE}/historical-candle/{enc}/day/{to}/{fr}"
    r   = requests.get(url, headers=_hdr(token), timeout=15)
    time.sleep(0.2)
    if r.status_code != 200:
        return pd.DataFrame()
    candles = r.json().get("data", {}).get("candles", [])
    if not candles:
        return pd.DataFrame()
    rows = []
    for c in reversed(candles):
        try:
            rows.append({
                "date":   str(pd.to_datetime(c[0]).date()),
                "open":   float(c[1]),
                "close":  float(c[4]),
                "volume": int(c[5]),
                "oi":     int(c[6]) if len(c) > 6 else 0,
            })
        except Exception:
            pass
    df = pd.DataFrame(rows)
    if not df.empty:
        df.to_parquet(cache_f, index=False)
    return df

# ── build OI table for a symbol ───────────────────────────────────────────────
def _build_oi_table(sym: str, contracts: Dict[Tuple[int,str], str],
                    token: str, atm_range: range) -> Dict[str, Dict[int, Dict[str,int]]]:
    """
    Returns {date_str: {strike: {"ce_oi": X, "pe_oi": Y}}}
    Fetches daily bars for all strikes in atm_range for CE and PE.
    """
    print(f"  Building OI table for {sym} ({len(atm_range)*2} contracts)...", flush=True)
    # {date_str → {strike → {ce_oi, pe_oi}}}
    oi_table: Dict[str, Dict[int, Dict[str,int]]] = {}

    fetched = 0
    for strike in atm_range:
        for otype in ("CE", "PE"):
            ikey = contracts.get((strike, otype))
            if not ikey:
                continue
            df = _fetch_daily(ikey, token,
                              START_DATE - timedelta(days=5), END_DATE)
            fetched += 1
            if df.empty:
                continue
            for row in df.itertuples():
                d_str = str(row.date)
                if d_str not in oi_table:
                    oi_table[d_str] = {}
                if strike not in oi_table[d_str]:
                    oi_table[d_str][strike] = {"ce_oi": 0, "pe_oi": 0}
                key = "ce_oi" if otype == "CE" else "pe_oi"
                oi_table[d_str][strike][key] = int(row.oi)

    print(f"    Fetched {fetched} contracts, {len(oi_table)} days with OI data", flush=True)
    return oi_table

# ── OI analysis ───────────────────────────────────────────────────────────────
def _oi_strikes(oi_day: Dict[int, Dict[str,int]]) -> dict:
    """
    Given OI for one day, compute:
      - max_ce_oi_strike : highest call OI → call writers' wall → CE trap target
      - max_pe_oi_strike : highest put OI  → put writers' wall  → PE trap target
      - max_pain         : strike minimising total option buyer loss
    """
    if not oi_day:
        return {}
    strikes   = sorted(oi_day.keys())
    ce_ois    = {s: oi_day[s].get("ce_oi", 0) for s in strikes}
    pe_ois    = {s: oi_day[s].get("pe_oi", 0) for s in strikes}

    max_ce_strike = max(ce_ois, key=ce_ois.get) if ce_ois else None
    max_pe_strike = max(pe_ois, key=pe_ois.get) if pe_ois else None

    # Max pain: for each candidate strike K, compute total loss of all option buyers
    pain = {}
    for K in strikes:
        call_loss = sum((s - K) * ce_ois.get(s, 0) for s in strikes if s > K)
        put_loss  = sum((K - s) * pe_ois.get(s, 0) for s in strikes if s < K)
        pain[K]   = call_loss + put_loss
    max_pain = min(pain, key=pain.get) if pain else None

    return {
        "max_ce_oi_strike": max_ce_strike,
        "max_pe_oi_strike": max_pe_strike,
        "max_pain":          max_pain,
        "ce_ois":            ce_ois,
        "pe_ois":            pe_ois,
    }

# ── cascade helpers ───────────────────────────────────────────────────────────
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
            from datetime import time as _t
            t_cut = _t(int(h), int(m))
            df = df[df["datetime"].dt.time <= t_cut]
        except Exception:
            pass
    r = (df.set_index("datetime")[["open","high","low","close"]]
          .resample(f"{minutes}min", closed="left", label="left")
          .agg({"open":"first","high":"max","low":"min","close":"last"})
          .dropna(subset=["close"]).reset_index())
    return r

def _simulate(df_exec: pd.DataFrame, ltf_z: dict, htf_z: dict,
              sl_buf: float, cap: int, lot: int) -> Optional[dict]:
    ll, lh = float(ltf_z.get("zone_low",0)), float(ltf_z.get("zone_high",0))
    hl     = float(htf_z.get("zone_low",0))
    t1     = float(htf_z.get("sl",0))
    sl     = hl - sl_buf
    if t1 <= 0 or sl <= 0 or t1 <= lh:
        return None
    H = df_exec["high"].values.astype(float)
    L = df_exec["low"].values.astype(float)
    C = df_exec["close"].values.astype(float)
    buf = max((lh-ll)*0.15, 0.5)
    idxs = np.where((C >= ll-buf) & (C <= lh+buf))[0]
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
                return {"pnl": round((sl-trig)*lot,2), "exit":"SL"}
            tgt = trig+cap if cap>0 else t1
            if Hs[k] >= min(t1,tgt):
                return {"pnl": round((min(t1,tgt)-trig)*lot,2), "exit":"T1"}
        return {"pnl": round((float(Cs[-1])-trig)*lot,2), "exit":"EOD"}
    return None

# ── load cached 1m option bars ────────────────────────────────────────────────
def _load_opt_1m(sym: str, strike: int, otype: str) -> pd.DataFrame:
    label   = f"{sym}{otype}{strike}"
    pattern = os.path.join(CACHE_DIR, f"opt_{label}_*.parquet")
    import glob
    files = glob.glob(pattern)
    if not files:
        return pd.DataFrame()
    try:
        df = pd.read_parquet(files[0])
        df["datetime"] = pd.to_datetime(df["datetime"])
        if df["datetime"].dt.tz is not None:
            df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
        return df
    except Exception:
        return pd.DataFrame()

def _day_bars(df_full: pd.DataFrame, day: date) -> pd.DataFrame:
    d_s = pd.Timestamp(f"{day}T09:15:00")
    d_e = pd.Timestamp(f"{day}T15:30:00")
    return df_full[(df_full["datetime"] >= d_s) & (df_full["datetime"] <= d_e)].copy()

# ── run cascade on one (strike, opt_type, day) ────────────────────────────────
_bar_cache: Dict[str, pd.DataFrame] = {}

def _cascade(sym: str, strike: int, otype: str, day: date, lot: int) -> Optional[dict]:
    label = f"{sym}{otype}{strike}"
    if label not in _bar_cache:
        _bar_cache[label] = _load_opt_1m(sym, strike, otype)
    full = _bar_cache[label]
    if full.empty:
        return None
    opt_df = _day_bars(full, day)
    if len(opt_df) < 30:
        return None

    htf = _resample(opt_df, HTF_MIN, ZONE_CUTOFF)
    mtf = _resample(opt_df, MTF_MIN, ZONE_CUTOFF)
    ltf = _resample(opt_df, LTF_MIN, ZONE_CUTOFF)
    exc = _resample(opt_df, EXEC_MIN)
    if any(len(x) < 2 for x in [htf, mtf, ltf, exc]):
        return None

    htf_zones = _get_zones(htf)
    mtf_zones = _get_zones(mtf)
    ltf_zones = _get_zones(ltf)

    for htf_z in htf_zones:
        zk = _zone_kind(htf_z)
        if zk == "UNKNOWN":
            continue
        if (zk=="BEAR" and otype!="CE") or (zk=="BULL" and otype!="PE"):
            continue
        mtf_m = next((z for z in mtf_zones if _zones_overlap(htf_z,z)), None)
        if not mtf_m: continue
        ltf_m = next((z for z in ltf_zones if _zones_overlap(mtf_m,z)), None)
        if not ltf_m: continue
        res = _simulate(exc, ltf_m, htf_z, SL_BUF, CAP_PTS, lot)
        if res:
            return res
    return None

# ── daily spot for ATM ────────────────────────────────────────────────────────
def _load_daily_close(sym: str) -> Dict[str, float]:
    cache_f = os.path.join(CACHE_DIR, f"daily_{sym}.json")
    if os.path.exists(cache_f):
        with open(cache_f) as f:
            data = json.load(f)
        # Could be {str: float} or {str: {open,close}}
        result = {}
        for k, v in data.items():
            result[k] = v if isinstance(v, float) else v.get("close", 0)
        return result
    return {}

# ── per-symbol runner ─────────────────────────────────────────────────────────
def run_symbol(sym: str, token: str) -> dict:
    print(f"\n{'='*64}")
    print(f"  {sym}  —  OI-based vs ATM strike selection  |  June 2026")
    print(f"{'='*64}")

    lot     = LOT_SIZES[sym]
    step    = STRIKE_STEPS[sym]
    expiry  = JULY_EXPIRY[sym]

    contracts = _load_contracts(sym, expiry)
    if not contracts:
        return {}

    daily_close = _load_daily_close(sym)

    # Determine ATM range to scan for OI (all strikes across the month)
    all_strikes = sorted({s for (s,_) in contracts})
    # Filter to a reasonable range (ATM of period ± OI_SCAN_STEPS)
    closes = [v for v in daily_close.values() if isinstance(v,(int,float)) and v > 0]
    mid_atm = int(round(sum(closes)/len(closes)/step)*step) if closes else 0
    lo_strike = mid_atm - OI_SCAN_STEPS * step
    hi_strike = mid_atm + OI_SCAN_STEPS * step
    scan_strikes = [s for s in all_strikes if lo_strike <= s <= hi_strike]
    print(f"  OI scan range: {lo_strike}–{hi_strike}  ({len(scan_strikes)} strikes)")

    # Build OI table: prev-day EOD OI per strike
    oi_table = _build_oi_table(sym, contracts, token, scan_strikes)

    # Trade days
    days = []
    d = START_DATE
    while d <= END_DATE:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)

    atm_trades: List[dict] = []
    oi_trades:  List[dict] = []
    sl_hist_atm: Dict[str, date] = {}
    sl_hist_oi:  Dict[str, date] = {}

    for day in days:
        d_str = str(day)

        # ATM from prev day close
        prev_d = day - timedelta(days=1)
        prev_close = None
        for _ in range(5):
            prev_close = daily_close.get(str(prev_d))
            if prev_close: break
            prev_d -= timedelta(days=1)
        if not prev_close: continue
        atm = int(round(float(prev_close) / step) * step)

        # Prev day OI structure (use D-1 data)
        prev_d2 = day - timedelta(days=1)
        oi_day = None
        for _ in range(5):
            oi_day = oi_table.get(str(prev_d2))
            if oi_day: break
            prev_d2 -= timedelta(days=1)

        oi_info = _oi_strikes(oi_day) if oi_day else {}
        max_ce_strike = oi_info.get("max_ce_oi_strike")
        max_pe_strike = oi_info.get("max_pe_oi_strike")
        max_pain      = oi_info.get("max_pain")

        # ── Strategy A: ATM baseline ─────────────────────────────────────────
        for otype, target_strike in [("CE", atm), ("PE", atm)]:
            zone_key = f"ATM_{sym}_{otype}_{target_strike}"
            if zone_key in sl_hist_atm and (day - sl_hist_atm[zone_key]).days <= 1:
                continue
            res = _cascade(sym, target_strike, otype, day, lot)
            if res:
                res.update({"date": d_str, "mode": "ATM", "strike": target_strike,
                            "opt": otype, "atm": atm})
                atm_trades.append(res)
                if res["exit"] == "SL":
                    sl_hist_atm[zone_key] = day
                break

        # ── Strategy B: OI-based strike selection ────────────────────────────
        if not oi_info:
            continue

        # CE: scan max call OI strike (where call writers are → they'll defend)
        # PE: scan max put OI strike  (where put writers are → they'll defend)
        # Also try max pain as a secondary target
        ce_targets = [s for s in [max_ce_strike, max_pain] if s is not None]
        pe_targets = [s for s in [max_pe_strike, max_pain] if s is not None]

        for otype, targets in [("CE", ce_targets), ("PE", pe_targets)]:
            for target_strike in targets:
                if target_strike not in [s for (s,_) in contracts]:
                    continue
                zone_key = f"OI_{sym}_{otype}_{target_strike}"
                if zone_key in sl_hist_oi and (day - sl_hist_oi[zone_key]).days <= 1:
                    continue
                res = _cascade(sym, target_strike, otype, day, lot)
                if res:
                    res.update({"date": d_str, "mode": "OI", "strike": target_strike,
                                "opt": otype, "atm": atm,
                                "max_ce": max_ce_strike, "max_pe": max_pe_strike,
                                "pain": max_pain})
                    oi_trades.append(res)
                    if res["exit"] == "SL":
                        sl_hist_oi[zone_key] = day
                    break

    def _stats(tlist: list) -> dict:
        if not tlist:
            return dict(n=0, wr=0.0, pf=0.0, net=0.0, wins=0)
        wins = [t for t in tlist if t["pnl"] > 0]
        losses = [t for t in tlist if t["pnl"] <= 0]
        gw = sum(t["pnl"] for t in wins)
        gl = abs(sum(t["pnl"] for t in losses))
        return dict(
            n=len(tlist), wins=len(wins),
            wr=round(len(wins)/len(tlist)*100,1),
            pf=round(gw/gl,2) if gl>0 else 9999.0,
            net=round(sum(t["pnl"] for t in tlist),2),
        )

    sa = _stats(atm_trades)
    sb = _stats(oi_trades)

    print(f"\n  {'':22} {'ATM baseline':>14} {'OI-based':>14}")
    print(f"  {'-'*50}")
    for k in ["n","wins","wr","pf","net"]:
        print(f"  {k:22} {str(sa[k]):>14} {str(sb[k]):>14}")

    # OI trade detail
    if oi_trades:
        print(f"\n  OI trades ({len(oi_trades)}):")
        for t in oi_trades:
            print(f"    {t['date']}  {t['opt']}  strike={t['strike']}  "
                  f"atm={t['atm']}  maxCE={t.get('max_ce')}  maxPE={t.get('max_pe')}  "
                  f"pain={t.get('pain')}  pnl={t['pnl']:+.0f}  exit={t['exit']}")

    # Show OI structure for a sample day
    sample = str(days[5]) if len(days) > 5 else str(days[0])
    prev_s = str(date.fromisoformat(sample) - timedelta(days=1))
    oi_s   = oi_table.get(prev_s, {})
    info_s = _oi_strikes(oi_s)
    if info_s:
        print(f"\n  Sample OI structure (prev day of {sample}):")
        print(f"    Max Call OI strike : {info_s['max_ce_oi_strike']}  "
              f"OI={info_s['ce_ois'].get(info_s['max_ce_oi_strike'],0):,}")
        print(f"    Max Put  OI strike : {info_s['max_pe_oi_strike']}  "
              f"OI={info_s['pe_ois'].get(info_s['max_pe_oi_strike'],0):,}")
        print(f"    Max Pain strike    : {info_s['max_pain']}")

    return {"sym": sym, "atm": sa, "oi": sb,
            "atm_trades": atm_trades, "oi_trades": oi_trades}

# ── main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    token = _get_token()
    if not token:
        print("ERROR: No Upstox token in data/clients.db")
        sys.exit(1)

    print("OI-based strike selection backtest — June 2026")
    print(f"HTF={HTF_MIN}m / MTF={MTF_MIN}m / LTF={LTF_MIN}m  |  SL={SL_BUF}  CAP={CAP_PTS}")
    print(f"OI logic: prev-day EOD OI -> max call OI strike (CE) + max put OI strike (PE)")

    all_results = []
    for sym in SYMBOLS:
        _bar_cache.clear()
        r = run_symbol(sym, token)
        if r:
            all_results.append(r)

    print(f"\n\n{'='*70}")
    print(f"  FINAL COMPARISON  —  ATM baseline vs OI-based strike selection")
    print(f"{'='*70}")
    print(f"  {'Symbol':10} {'Mode':12} {'Trades':>7} {'Win%':>7} {'PF':>7} {'Net Rs':>10}")
    print(f"  {'-'*58}")
    for r in all_results:
        for label, s in [("ATM", r["atm"]), ("OI-based", r["oi"])]:
            print(f"  {r['sym']:10} {label:12} {s['n']:>7} "
                  f"{s['wr']:>6.1f}% {s['pf']:>7.2f} {s['net']:>10.0f}")
        print(f"  {'-'*58}")
