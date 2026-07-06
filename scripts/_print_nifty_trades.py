"""Print all NIFTY ATM trades with full entry/SL/target/zone details."""
import json, os, sys, glob
from datetime import date, timedelta
import numpy as np
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from strategies.trap_scanner import scanner

CACHE_DIR   = os.path.join(ROOT, "data", "nse_option_cache")
START_DATE  = date(2026, 6, 1)
END_DATE    = date(2026, 6, 30)
HTF_MIN, MTF_MIN, LTF_MIN, EXEC_MIN = 75, 15, 5, 5
SL_BUF, CAP_PTS, LOT, STEP = 10.0, 200, 25, 50
ZONE_CUTOFF = "15:14"

def _zone_kind(z):
    sl, zh, zl = z.get("sl",0), z.get("zone_high",0), z.get("zone_low",0)
    if sl > zh: return "BEAR"
    if sl < zl: return "BULL"
    return "UNKNOWN"

def _get_zones(df):
    if len(df) < 3: return []
    try:
        _, zones = scanner.scan_htf(df)
        return zones or []
    except Exception:
        return []

def _zones_overlap(a, b):
    al, ah = float(a.get("zone_low",0)), float(a.get("zone_high",0))
    bl, bh = float(b.get("zone_low",0)), float(b.get("zone_high",0))
    buf = max((ah-al)*0.15, 0.5)
    return ah+buf >= bl and bh+buf >= al

def _resample(df1m, minutes, cutoff=None):
    if df1m.empty or len(df1m) < 2: return pd.DataFrame()
    df = df1m.copy()
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_localize(None)
    if cutoff:
        h, m = cutoff.split(":")
        from datetime import time as _t
        t_cut = _t(int(h), int(m))
        df = df[df["datetime"].dt.time <= t_cut]
    return (df.set_index("datetime")[["open","high","low","close"]]
             .resample(f"{minutes}min", closed="left", label="left")
             .agg({"open":"first","high":"max","low":"min","close":"last"})
             .dropna(subset=["close"]).reset_index())

def _simulate(df_exec, ltf_z, htf_z, sl_buf, cap, lot):
    ll, lh = float(ltf_z.get("zone_low",0)), float(ltf_z.get("zone_high",0))
    hl     = float(htf_z.get("zone_low",0))
    zh     = float(htf_z.get("zone_high",0))
    t1     = float(htf_z.get("sl",0))
    sl     = hl - sl_buf
    if t1<=0 or sl<=0 or t1<=lh: return None
    H = df_exec["high"].values.astype(float)
    L = df_exec["low"].values.astype(float)
    C = df_exec["close"].values.astype(float)
    buf = max((lh-ll)*0.15, 0.5)
    idxs = np.where((C >= ll-buf) & (C <= lh+buf))[0]
    idxs = idxs[idxs < len(H)-1]
    for i in idxs:
        trig = float(H[i])
        if t1<=trig or sl>=trig: continue
        hit = np.where(H[i+1:] >= trig)[0]
        if not len(hit): continue
        j = hit[0]
        Hs=H[i+1+j:]; Ls=L[i+1+j:]; Cs=C[i+1+j:]
        for k in range(len(Hs)):
            if Ls[k] <= sl:
                return {"pnl":round((sl-trig)*lot,2), "exit":"SL",
                        "entry":round(trig,2), "sl":round(sl,2),
                        "target":round(min(t1,trig+cap),2),
                        "zone_low":round(hl,2), "zone_high":round(zh,2), "ref_sl":round(t1,2)}
            tgt = trig+cap if cap>0 else t1
            if Hs[k] >= min(t1,tgt):
                return {"pnl":round((min(t1,tgt)-trig)*lot,2), "exit":"T1",
                        "entry":round(trig,2), "sl":round(sl,2),
                        "target":round(min(t1,trig+cap),2),
                        "zone_low":round(hl,2), "zone_high":round(zh,2), "ref_sl":round(t1,2)}
        return {"pnl":round((float(Cs[-1])-trig)*lot,2), "exit":"EOD",
                "entry":round(trig,2), "sl":round(sl,2),
                "target":round(min(t1,trig+cap),2),
                "zone_low":round(hl,2), "zone_high":round(zh,2), "ref_sl":round(t1,2)}
    return None

# ── load opt cache ────────────────────────────────────────────────────────────
opt_cache = {}
for f in glob.glob(os.path.join(CACHE_DIR, "opt_NIFTY*.parquet")):
    try:
        df = pd.read_parquet(f)
        df["datetime"] = pd.to_datetime(df["datetime"])
        if df["datetime"].dt.tz is not None:
            df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
        label = os.path.basename(f).replace("opt_","").split("_2026")[0]
        opt_cache[label] = df
    except Exception:
        pass

daily = json.load(open(os.path.join(CACHE_DIR, "daily_NIFTY.json")))
def get_close(d):
    v = daily.get(str(d))
    if v is None: return None
    return float(v) if isinstance(v, float) else float(v.get("close", 0))

days = []
d = START_DATE
while d <= END_DATE:
    if d.weekday() < 5: days.append(d)
    d += timedelta(days=1)

print()
print("NIFTY -- ALL ATM CASCADE TRADES -- June 2026")
print("HTF=75m / MTF=15m / LTF=5m / SL_BUF=10 / CAP=200pts / LOT=25")
print("="*115)
print(f"  {'Date':12} {'OPT':4} {'ATM':6} {'Strike':7} {'Entry':7} {'SL':7} "
      f"{'Target':8} {'ZoneLo':8} {'ZoneHi':8} {'RefSL':8} {'PnL':8} {'Exit'}")
print("-"*115)

trades = []
sl_hist = {}

for day in days:
    prev_d = day - timedelta(days=1)
    prev_close = None
    for _ in range(5):
        prev_close = get_close(prev_d)
        if prev_close: break
        prev_d -= timedelta(days=1)
    if not prev_close: continue
    atm = int(round(float(prev_close) / STEP) * STEP)

    for opt_type in ("CE", "PE"):
        d_s = pd.Timestamp(f"{day}T09:15:00")
        d_e = pd.Timestamp(f"{day}T15:30:00")
        opt_df = pd.DataFrame()
        used_strike = atm
        for offset in [0,1,-1,2,-2]:
            st = atm + offset*STEP if opt_type == "CE" else atm - offset*STEP
            label = f"NIFTY{opt_type}{st}"
            full  = opt_cache.get(label, pd.DataFrame())
            if full.empty: continue
            day_df = full[(full["datetime"] >= d_s) & (full["datetime"] <= d_e)].copy()
            if len(day_df) >= 30:
                opt_df = day_df
                used_strike = st
                break
        if opt_df.empty: continue

        htf = _resample(opt_df, HTF_MIN, ZONE_CUTOFF)
        mtf = _resample(opt_df, MTF_MIN, ZONE_CUTOFF)
        ltf = _resample(opt_df, LTF_MIN, ZONE_CUTOFF)
        exc = _resample(opt_df, EXEC_MIN)
        if any(len(x) < 2 for x in [htf, mtf, ltf, exc]): continue

        htf_zones = _get_zones(htf)
        mtf_zones = _get_zones(mtf)
        ltf_zones = _get_zones(ltf)

        for htf_z in htf_zones:
            zk = _zone_kind(htf_z)
            if zk == "UNKNOWN": continue
            if (zk=="BEAR" and opt_type!="CE") or (zk=="BULL" and opt_type!="PE"): continue
            zkey = f"{opt_type}_{htf_z.get('zone_low',0):.0f}"
            if zkey in sl_hist and (day - sl_hist[zkey]).days <= 1: continue
            mtf_m = next((z for z in mtf_zones if _zones_overlap(htf_z, z)), None)
            if not mtf_m: continue
            ltf_m = next((z for z in ltf_zones if _zones_overlap(mtf_m, z)), None)
            if not ltf_m: continue
            res = _simulate(exc, ltf_m, htf_z, SL_BUF, CAP_PTS, LOT)
            if res:
                res.update({"date": str(day), "opt": opt_type,
                            "atm": atm, "strike": used_strike})
                trades.append(res)
                print(f"  {str(day):12} {opt_type:4} {atm:6} {used_strike:7} "
                      f"{res['entry']:7.1f} {res['sl']:7.1f} {res['target']:8.1f} "
                      f"{res['zone_low']:8.1f} {res['zone_high']:8.1f} {res['ref_sl']:8.1f} "
                      f"{res['pnl']:8.1f}  {res['exit']}")
                if res["exit"] == "SL": sl_hist[zkey] = day
                break

print("="*115)
wins   = [t for t in trades if t["pnl"] > 0]
losses = [t for t in trades if t["pnl"] <= 0]
gw     = sum(t["pnl"] for t in wins)
gl     = abs(sum(t["pnl"] for t in losses))
print(f"  Total={len(trades)}  Wins={len(wins)}  Losses={len(losses)}  "
      f"WR={len(wins)/len(trades)*100:.1f}%  PF={gw/gl:.2f}  "
      f"Net=Rs {sum(t['pnl'] for t in trades):.0f}")
print()
print("COLUMNS: Date | OPT=CE/PE | ATM=prev-day-close-round | Strike=actual bar used |")
print("         Entry=HTF trigger | SL=zone_low-10 | Target=min(RefSL,entry+200) |")
print("         ZoneLo/ZoneHi=HTF zone | RefSL=zone ref bar high |")
print("         PnL=points x 25 lots | Exit=T1/SL/EOD")
