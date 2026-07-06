"""
scripts/nse_ob_option_backtest.py
==================================
Backtest HTF=75m / MTF=15m / LTF=5m  WITHOUT vs WITH OB+CHoCH gate.
Uses REAL July monthly expiry option premium bars (not spot proxy).

Instruments : NIFTY, BANKNIFTY, SENSEX
Period      : June 2026 (1 month)
Expiry used : July monthly (last Tuesday/Wednesday of July 2026)
              NIFTY/SENSEX → Jul 28 2026, BANKNIFTY → Jul 29 2026

Usage: python scripts/nse_ob_option_backtest.py
"""
from __future__ import annotations

import gzip, io, json, os, sys, time, sqlite3
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote as _quote

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
ZIGZAG   = 9
SL_BUF   = 10.0   # pts below zone_low
CAP_PTS  = 200    # 0 = hold to T1

SYMBOLS  = ["NIFTY", "BANKNIFTY", "SENSEX"]
LOT_SIZES    = {"NIFTY": 25, "BANKNIFTY": 15, "SENSEX": 10}
STRIKE_STEPS = {"NIFTY": 50, "BANKNIFTY": 100, "SENSEX": 100}

# July monthly expiry: last Tuesday (wd=1) for NIFTY/SENSEX, last Wednesday (wd=2) for BANKNIFTY
# July 2026: Tuesdays → 7,14,21,28; Wednesdays → 1,8,15,22,29
JULY_EXPIRY = {
    "NIFTY":     date(2026, 7, 28),   # last Tuesday Jul 2026 (from NSE master)
    "BANKNIFTY": date(2026, 7, 28),   # also Jul 28 (from NSE master)
    "SENSEX":    date(2026, 7, 30),   # last Thursday Jul 2026 (from BSE master)
}
ZONE_CUTOFF = "15:14"

UPSTOX_BASE = "https://api.upstox.com/v2"
NSE_MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
BSE_MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz"

DB_PATH   = os.path.join(ROOT, "data", "clients.db")
CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# ── helpers ───────────────────────────────────────────────────────────────────
def _get_token() -> str:
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

_master_cache: Dict[str, list] = {}

def _load_master(exchange: str = "NSE") -> list:
    if exchange in _master_cache:
        return _master_cache[exchange]
    url = BSE_MASTER_URL if exchange == "BSE" else NSE_MASTER_URL
    cache_f = os.path.join(CACHE_DIR, f"master_{exchange}.json")
    if os.path.exists(cache_f) and (time.time() - os.path.getmtime(cache_f)) < 3600 * 12:
        with open(cache_f) as f:
            data = json.load(f)
        _master_cache[exchange] = data
        return data
    print(f"  Downloading {exchange} master...", flush=True)
    r = requests.get(url, timeout=30)
    raw = gzip.decompress(r.content)
    data = json.loads(raw.decode("utf-8"))
    with open(cache_f, "w") as f:
        json.dump(data, f)
    _master_cache[exchange] = data
    return data

def _get_contracts(symbol: str, expiry: date) -> Dict[Tuple[int, str], str]:
    """Return {(strike, 'CE'|'PE'): instrument_key} for the given expiry."""
    cache_f = os.path.join(CACHE_DIR, f"contracts_{symbol}_{expiry}.json")
    if os.path.exists(cache_f):
        with open(cache_f) as f:
            raw = json.load(f)
        if raw:
            return {(int(k.split("|")[0]), k.split("|")[1]): v for k, v in raw.items()}

    exchange = "BSE" if symbol == "SENSEX" else "NSE"
    master = _load_master(exchange)
    result: Dict[Tuple[int, str], str] = {}
    for inst in master:
        name  = str(inst.get("trading_symbol", "") or inst.get("name", "")).upper()
        itype = str(inst.get("instrument_type", "")).upper()
        if symbol not in name:
            continue
        if itype not in ("CE", "PE", "CALL", "PUT"):
            continue
        exp_raw = inst.get("expiry") or inst.get("expiry_date") or ""
        if not exp_raw:
            continue
        try:
            if isinstance(exp_raw, (int, float)):
                exp_d = datetime.utcfromtimestamp(int(exp_raw) / 1000).date()
            elif len(str(exp_raw)) == 10 and "-" in str(exp_raw):
                exp_d = date.fromisoformat(str(exp_raw))
            else:
                continue
        except Exception:
            continue
        if exp_d != expiry:
            continue
        ikey   = inst.get("instrument_key", "")
        strike = inst.get("strike_price") or inst.get("strike") or 0
        try:
            strike = int(float(strike))
        except Exception:
            continue
        otype = "CE" if itype in ("CE", "CALL") else "PE"
        if ikey and strike > 0:
            result[(strike, otype)] = ikey

    serialisable = {f"{k[0]}|{k[1]}": v for k, v in result.items()}
    with open(cache_f, "w") as f:
        json.dump(serialisable, f)
    print(f"  {symbol} {expiry} contracts: {len(result)}", flush=True)
    return result

def _fetch_1m(instrument_key: str, label: str, token: str,
              fr: date, to: date) -> pd.DataFrame:
    cache_f = os.path.join(CACHE_DIR, f"opt_{label}_{fr}_{to}.parquet")
    if os.path.exists(cache_f):
        df = pd.read_parquet(cache_f)
        if not df.empty:
            return df
    enc = _quote(instrument_key, safe="")
    url = f"{UPSTOX_BASE}/historical-candle/{enc}/1minute/{to}/{fr}"
    r   = requests.get(url, headers=_hdr(token), timeout=20)
    time.sleep(0.25)
    if r.status_code != 200:
        print(f"    [WARN] {label} HTTP {r.status_code}", flush=True)
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

def _get_atm(contracts: Dict[Tuple[int, str], str], spot: float, step: int) -> int:
    """Round spot to nearest strike step."""
    return int(round(spot / step) * step)

def _get_spot_daily(symbol: str, token: str) -> Dict[str, float]:
    """Fetch D1 index bars for prev-close ATM computation. Returns {date_str: close}."""
    cache_f = os.path.join(CACHE_DIR, f"daily_{symbol}.json")
    if os.path.exists(cache_f) and (time.time() - os.path.getmtime(cache_f)) < 3600 * 6:
        with open(cache_f) as f:
            return json.load(f)
    index_keys = {
        "NIFTY":     "NSE_INDEX|Nifty 50",
        "BANKNIFTY": "NSE_INDEX|Nifty Bank",
        "SENSEX":    "BSE_INDEX|SENSEX",
    }
    key = index_keys[symbol]
    enc = _quote(key, safe="")
    url = f"{UPSTOX_BASE}/historical-candle/{enc}/day/{END_DATE}/{START_DATE - timedelta(days=5)}"
    r   = requests.get(url, headers=_hdr(token), timeout=15)
    if r.status_code != 200:
        return {}
    candles = r.json().get("data", {}).get("candles", [])
    result = {}
    for c in reversed(candles):
        try:
            dt = pd.to_datetime(c[0])
            d_str = str(dt.date())
            result[d_str] = float(c[4])  # close
        except Exception:
            pass
    with open(cache_f, "w") as f:
        json.dump(result, f)
    return result

# ── zone helpers ──────────────────────────────────────────────────────────────
def _zone_kind(z: dict) -> str:
    sl = z.get("sl", 0)
    zh = z.get("zone_high", 0)
    zl = z.get("zone_low", 0)
    if sl > zh:
        return "BEAR"
    if sl < zl:
        return "BULL"
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
    al, ah = float(a.get("zone_low", 0)), float(a.get("zone_high", 0))
    bl, bh = float(b.get("zone_low", 0)), float(b.get("zone_high", 0))
    buf = max((ah - al) * 0.15, 0.5)
    return ah + buf >= bl and bh + buf >= al

def _resample(df1m: pd.DataFrame, minutes: int, cutoff: str = None) -> pd.DataFrame:
    if df1m.empty or len(df1m) < 2:
        return pd.DataFrame()
    df = df1m.copy()
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_localize(None)
    if cutoff:
        try:
            h, m = cutoff.split(":")
            t_cut = datetime.strptime(f"{h}:{m}", "%H:%M").time()
            df = df[df["datetime"].dt.time <= t_cut]
        except Exception:
            pass
    r = (df.set_index("datetime")[["open","high","low","close"]]
          .resample(f"{minutes}min", closed="left", label="left")
          .agg({"open":"first","high":"max","low":"min","close":"last"})
          .dropna(subset=["close"])
          .reset_index())
    return r

# ── OB + CHoCH gate ───────────────────────────────────────────────────────────
def _ob_choch_clear(bars_df: pd.DataFrame, opt_type: str, ltp: float,
                    zz: int = ZIGZAG) -> bool:
    min_bars = max(zz * 3 + 14, 30)
    if len(bars_df) < min_bars:
        return False
    try:
        df = bars_df.tail(500).copy()
        bull_obs, bear_obs = scanner.active_order_blocks(df, zigzag_len=zz)
        sigs  = scanner.detect_choch_bos(df, zigzag_len=zz)
        choch = scanner.last_choch_direction(sigs)
        if opt_type == "CE":   # BEAR trap: expect reversal up
            return choch == "UP" and bool(scanner.price_in_order_block(ltp, bull_obs))
        else:                  # BULL trap: expect reversal down
            return choch == "DOWN" and bool(scanner.price_in_order_block(ltp, bear_obs))
    except Exception:
        return False

# ── simulate ──────────────────────────────────────────────────────────────────
def _simulate(df_exec: pd.DataFrame, ltf_zone: dict, htf_zone: dict,
              sl_buf: float, cap_pts: int, lot: int,
              use_ob: bool = False, opt_type: str = "CE") -> Optional[dict]:
    """
    Find entry trigger bar, optionally check OB+CHoCH at that bar, then exit.
    OB is evaluated on the exec (5m) bars UP TO the trigger bar — matching live behaviour.
    """
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
    buf = max((ltf_h - ltf_l) * 0.15, 0.5)
    in_zone = (C >= ltf_l - buf) & (C <= ltf_h + buf)
    idxs = np.where(in_zone)[0]
    idxs = idxs[idxs < len(H) - 1]
    for i in idxs:
        trig = float(H[i])
        if t1 <= trig or sl >= trig:
            continue
        # OB+CHoCH gate: evaluate on bars up to this trigger bar (inclusive)
        if use_ob:
            bars_so_far = df_exec.iloc[:i+1].copy()
            ltp = float(C[i])
            if not _ob_choch_clear(bars_so_far, opt_type, ltp):
                continue
        hit = np.where(H[i+1:] >= trig)[0]
        if not len(hit):
            continue
        j = hit[0]
        Hs = H[i+1+j:]; Ls = L[i+1+j:]; Cs = C[i+1+j:]
        for k in range(len(Hs)):
            if Ls[k] <= sl:
                return {"pnl": round((sl - trig) * lot, 2), "exit": "SL", "entry": round(trig, 2)}
            target = trig + cap_pts if cap_pts > 0 else t1
            if Hs[k] >= min(t1, target):
                return {"pnl": round((min(t1, target) - trig) * lot, 2), "exit": "T1", "entry": round(trig, 2)}
        return {"pnl": round((float(Cs[-1]) - trig) * lot, 2), "exit": "EOD", "entry": round(trig, 2)}
    return None

# ── per-symbol runner ─────────────────────────────────────────────────────────
def run_symbol(sym: str, token: str) -> dict:
    print(f"\n{'='*62}")
    print(f"  {sym}  |  HTF={HTF_MIN}m MTF={MTF_MIN}m LTF={LTF_MIN}m  |  July monthly expiry")
    print(f"{'='*62}")

    lot    = LOT_SIZES.get(sym, 25)
    step   = STRIKE_STEPS.get(sym, 50)
    expiry = JULY_EXPIRY[sym]
    print(f"  Expiry: {expiry}   SL={SL_BUF}pts  CAP={CAP_PTS}pts  lot={lot}")

    # Load contracts
    contracts = _get_contracts(sym, expiry)
    if not contracts:
        print(f"  No contracts found for {sym} {expiry}. Skipping.")
        return {}

    # Daily spot for ATM computation
    daily_spot = _get_spot_daily(sym, token)

    # Trade days
    days = []
    d = START_DATE
    while d <= END_DATE:
        if d.weekday() < 5:  # Mon-Fri
            days.append(d)
        d += timedelta(days=1)
    print(f"  Trading days in June: {len(days)}")

    # Pre-fetch option bars for all days in one batch per strike
    # We'll fetch per-day lazily and cache
    opt_cache: Dict[str, pd.DataFrame] = {}  # label → full month bars

    def _get_day_bars(strike: int, otype: str, day: date) -> pd.DataFrame:
        key_t = contracts.get((strike, otype))
        if not key_t:
            return pd.DataFrame()
        label = f"{sym}{otype}{strike}"
        if label not in opt_cache:
            print(f"    Fetching {label} (Jul expiry)...", flush=True)
            df = _fetch_1m(key_t, label, token, START_DATE, END_DATE)
            opt_cache[label] = df
        full = opt_cache[label]
        if full.empty:
            return pd.DataFrame()
        d_s = pd.Timestamp(f"{day}T09:15:00")
        d_e = pd.Timestamp(f"{day}T15:30:00")
        day_df = full[(full["datetime"] >= d_s) & (full["datetime"] <= d_e)].copy()
        return day_df

    trades_no: List[dict] = []
    trades_ob: List[dict] = []
    sl_hist_no: Dict[str, date] = {}
    sl_hist_ob: Dict[str, date] = {}

    for day in days:
        # Get ATM from prev day close
        prev_d = day - timedelta(days=1)
        for _ in range(5):
            spot = daily_spot.get(str(prev_d))
            if spot:
                break
            prev_d -= timedelta(days=1)
        if not spot:
            continue
        atm = int(round(float(spot) / step) * step)

        # Try ATM, then ATM±1 — pick the strike with more bars
        best_ce_df = pd.DataFrame()
        best_pe_df = pd.DataFrame()
        for offset in [0, 1, -1]:
            ce_df = _get_day_bars(atm + offset * step, "CE", day)
            pe_df = _get_day_bars(atm - offset * step, "PE", day)
            if len(ce_df) > len(best_ce_df):
                best_ce_df = ce_df
            if len(pe_df) > len(best_pe_df):
                best_pe_df = pe_df

        # Use whichever has more data; try CE+PE separately
        for opt_type, opt_df in [("CE", best_ce_df), ("PE", best_pe_df)]:
            if len(opt_df) < 30:
                continue

            # Resample to HTF/MTF/LTF with cutoff
            htf = _resample(opt_df, HTF_MIN, ZONE_CUTOFF)
            mtf = _resample(opt_df, MTF_MIN, ZONE_CUTOFF)
            ltf = _resample(opt_df, LTF_MIN, ZONE_CUTOFF)
            exc = _resample(opt_df, EXEC_MIN)

            if len(htf) < 2 or len(mtf) < 2 or len(ltf) < 2 or len(exc) < 2:
                continue

            htf_zones = _get_zones(htf)
            mtf_zones = _get_zones(mtf)
            ltf_zones = _get_zones(ltf)

            for use_ob in (False, True):
                sl_hist = sl_hist_ob if use_ob else sl_hist_no
                trades  = trades_ob  if use_ob else trades_no

                for htf_z in htf_zones:
                    zk = _zone_kind(htf_z)
                    if zk == "UNKNOWN":
                        continue
                    # BEAR zone → CE (buy call on reversal up)
                    # BULL zone → PE (buy put on reversal down)
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

                    res = _simulate(exc, ltf_m, htf_z, SL_BUF, CAP_PTS, lot,
                                   use_ob=use_ob, opt_type=opt_type)
                    if res:
                        res.update({"date": str(day), "zone": zone_key,
                                    "opt": opt_type, "atm": atm})
                        trades.append(res)
                        if res["exit"] == "SL":
                            sl_hist[zone_key] = day
                        break  # one trade per leg per day

    def _stats(tlist: list) -> dict:
        if not tlist:
            return dict(trades=0, wins=0, losses=0, win_pct=0.0, pf=0.0,
                        net=0.0, avg_win=0.0, avg_loss=0.0)
        wins   = [t for t in tlist if t["pnl"] > 0]
        losses = [t for t in tlist if t["pnl"] <= 0]
        gw = sum(t["pnl"] for t in wins)
        gl = abs(sum(t["pnl"] for t in losses))
        return dict(
            trades=len(tlist),
            wins=len(wins), losses=len(losses),
            win_pct=round(len(wins)/len(tlist)*100, 1),
            pf=round(gw/gl, 2) if gl > 0 else 9999.0,
            net=round(sum(t["pnl"] for t in tlist), 2),
            avg_win=round(gw/max(len(wins),1), 2),
            avg_loss=round(-gl/max(len(losses),1), 2),
        )

    sn = _stats(trades_no)
    so = _stats(trades_ob)

    print(f"\n  {'':22} {'NO OB':>12} {'OB+CHoCH':>12}")
    print(f"  {'-'*46}")
    for k in ["trades","wins","losses","win_pct","pf","net","avg_win","avg_loss"]:
        print(f"  {k:22} {str(sn[k]):>12} {str(so[k]):>12}")

    if trades_ob:
        print(f"\n  OB+CHoCH trades ({len(trades_ob)}):")
        for t in trades_ob:
            print(f"    {t['date']}  {t['opt']}  atm={t['atm']}  "
                  f"entry={t['entry']}  pnl={t['pnl']:+.0f}  exit={t['exit']}")
    if trades_no:
        print(f"\n  No-OB trades ({len(trades_no)}) — first 10:")
        for t in trades_no[:10]:
            print(f"    {t['date']}  {t['opt']}  atm={t['atm']}  "
                  f"entry={t['entry']}  pnl={t['pnl']:+.0f}  exit={t['exit']}")

    return {"sym": sym, "no_ob": sn, "ob": so}

# ── main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    token = _get_token()
    if not token:
        print("ERROR: No Upstox token in data/clients.db")
        sys.exit(1)
    print(f"Token OK")
    print(f"Period : {START_DATE} to {END_DATE}")
    print(f"Expiry : July monthly  (NIFTY/SENSEX Jul 28, BANKNIFTY Jul 29)")
    print(f"Config : HTF={HTF_MIN}m / MTF={MTF_MIN}m / LTF={LTF_MIN}m  SL={SL_BUF}  CAP={CAP_PTS}")

    all_results = []
    for sym in SYMBOLS:
        r = run_symbol(sym, token)
        if r:
            all_results.append(r)

    print(f"\n\n{'='*70}")
    print(f"  FINAL — June 2026 | HTF={HTF_MIN}m / MTF={MTF_MIN}m / LTF={LTF_MIN}m | July expiry")
    print(f"{'='*70}")
    print(f"  {'Symbol':10} {'Mode':10} {'Trades':>7} {'Win%':>7} {'PF':>7} {'Net Rs':>10} {'AvgW':>7} {'AvgL':>7}")
    print(f"  {'-'*68}")
    for r in all_results:
        for label, s in [("NO OB", r["no_ob"]), ("OB+CHoCH", r["ob"])]:
            print(f"  {r['sym']:10} {label:10} {s['trades']:>7} {s['win_pct']:>6.1f}% "
                  f"{s['pf']:>7.2f} {s['net']:>10.0f} {s['avg_win']:>7.0f} {s['avg_loss']:>7.0f}")
        print(f"  {'-'*68}")
