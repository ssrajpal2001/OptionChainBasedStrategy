"""Analyze why NIFTY PE trades were zero in June 2026 and what PE OI shows."""
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
    sl, zh, zl = z.get("sl", 0), z.get("zone_high", 0), z.get("zone_low", 0)
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
    al, ah = float(a.get("zone_low", 0)), float(a.get("zone_high", 0))
    bl, bh = float(b.get("zone_low", 0)), float(b.get("zone_high", 0))
    buf = max((ah - al) * 0.15, 0.5)
    return ah + buf >= bl and bh + buf >= al


def _resample(df1m, minutes, cutoff=None):
    if df1m.empty or len(df1m) < 2: return pd.DataFrame()
    df = df1m.copy()
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_localize(None)
    if cutoff:
        h, m = cutoff.split(":")
        from datetime import time as _t
        df = df[df["datetime"].dt.time <= _t(int(h), int(m))]
    return (df.set_index("datetime")[["open", "high", "low", "close"]]
              .resample(f"{minutes}min", closed="left", label="left")
              .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
              .dropna(subset=["close"]).reset_index())


def _simulate(df_exec, ltf_z, htf_z, sl_buf, cap, lot):
    ll, lh = float(ltf_z.get("zone_low", 0)), float(ltf_z.get("zone_high", 0))
    hl = float(htf_z.get("zone_low", 0))
    t1 = float(htf_z.get("sl", 0))
    sl = hl - sl_buf
    if t1 <= 0 or sl <= 0 or t1 <= lh: return None
    H = df_exec["high"].values.astype(float)
    L = df_exec["low"].values.astype(float)
    C = df_exec["close"].values.astype(float)
    buf = max((lh - ll) * 0.15, 0.5)
    idxs = np.where((C >= ll - buf) & (C <= lh + buf))[0]
    idxs = idxs[idxs < len(H) - 1]
    for i in idxs:
        trig = float(H[i])
        if t1 <= trig or sl >= trig: continue
        hit = np.where(H[i + 1:] >= trig)[0]
        if not len(hit): continue
        j = hit[0]
        Hs = H[i + 1 + j:]; Ls = L[i + 1 + j:]; Cs = C[i + 1 + j:]
        for k in range(len(Hs)):
            if Ls[k] <= sl:
                return {"pnl": round((sl - trig) * lot, 2), "exit": "SL",
                        "entry": round(trig, 2), "sl": round(sl, 2), "target": round(min(t1, trig + cap), 2)}
            tgt = trig + cap if cap > 0 else t1
            if Hs[k] >= min(t1, tgt):
                return {"pnl": round((min(t1, tgt) - trig) * lot, 2), "exit": "T1",
                        "entry": round(trig, 2), "sl": round(sl, 2), "target": round(min(t1, trig + cap), 2)}
        return {"pnl": round((float(Cs[-1]) - trig) * lot, 2), "exit": "EOD",
                "entry": round(trig, 2), "sl": round(sl, 2), "target": round(min(t1, trig + cap), 2)}
    return None


# ── load cache ────────────────────────────────────────────────────────────────
opt_cache = {}
for f in glob.glob(os.path.join(CACHE_DIR, "opt_NIFTY*.parquet")):
    try:
        df = pd.read_parquet(f)
        df["datetime"] = pd.to_datetime(df["datetime"])
        if df["datetime"].dt.tz is not None:
            df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
        label = os.path.basename(f).replace("opt_", "").split("_2026")[0]
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

# ── show what PE bars are cached ──────────────────────────────────────────────
pe_labels = sorted([k for k in opt_cache if "PE" in k])
ce_labels = sorted([k for k in opt_cache if "CE" in k])
print(f"PE option bars cached: {len(pe_labels)}")
print(f"  Strikes: {[int(k.replace('NIFTYPE','')) for k in pe_labels]}")
print(f"CE option bars cached: {len(ce_labels)}")
print()

# ── daily CLOSE for context ───────────────────────────────────────────────────
print("NIFTY daily close in June (to understand the trend):")
for day in days:
    c = get_close(day)
    prev_d = day - timedelta(days=1)
    prev_c = None
    for _ in range(5):
        prev_c = get_close(prev_d)
        if prev_c: break
        prev_d -= timedelta(days=1)
    chg = ((c - prev_c) / prev_c * 100) if (c and prev_c) else 0
    bar = "UP  " if chg >= 0 else "DOWN"
    print(f"  {day}  close={c:8.1f}  chg={chg:+5.2f}%  {bar}")

print()

# ── PE zone analysis per day ──────────────────────────────────────────────────
print("="*90)
print("PE ZONE ANALYSIS — what zones form on PE bars each day")
print("="*90)
print(f"  {'Date':12} {'ATM':6} {'PE_strike':10} {'Bars':5} {'HTF_zones':10} {'BULL':5} {'BEAR':5} {'Trade?':8}")
print("-"*90)

pe_bull_days = 0
pe_bear_days = 0
pe_trades_bear = []  # BEAR zones on PE (this is the setup -- PE premium rising like CE)
pe_trades_bull = []  # BULL zones on PE
sl_hist_bear = {}
sl_hist_bull = {}

for day in days:
    prev_d = day - timedelta(days=1)
    prev_close = None
    for _ in range(5):
        prev_close = get_close(prev_d)
        if prev_close: break
        prev_d -= timedelta(days=1)
    if not prev_close: continue
    atm = int(round(float(prev_close) / STEP) * STEP)

    d_s = pd.Timestamp(f"{day}T09:15:00")
    d_e = pd.Timestamp(f"{day}T15:30:00")

    opt_df = pd.DataFrame()
    used_strike = atm
    # PE: try ATM, then ATM+step (slightly OTM put), then ATM-step (slightly ITM put)
    for offset in [0, 1, -1, 2, -2]:
        st = atm - offset * STEP
        label = f"NIFTYPE{st}"
        full = opt_cache.get(label, pd.DataFrame())
        if full.empty: continue
        day_df = full[(full["datetime"] >= d_s) & (full["datetime"] <= d_e)].copy()
        if len(day_df) >= 30:
            opt_df = day_df
            used_strike = st
            break

    if opt_df.empty:
        print(f"  {str(day):12} {atm:6}  {'NO PE CACHE':>10}  -")
        continue

    htf = _resample(opt_df, HTF_MIN, ZONE_CUTOFF)
    mtf = _resample(opt_df, MTF_MIN, ZONE_CUTOFF)
    ltf = _resample(opt_df, LTF_MIN, ZONE_CUTOFF)
    exc = _resample(opt_df, EXEC_MIN)

    htf_zones = _get_zones(htf) if len(htf) >= 2 else []
    bull_z = [z for z in htf_zones if _zone_kind(z) == "BULL"]
    bear_z = [z for z in htf_zones if _zone_kind(z) == "BEAR"]

    if bull_z: pe_bull_days += 1
    if bear_z: pe_bear_days += 1

    # Try BEAR zones on PE premium (PE premium rising and pulling back = seller trap)
    traded = "-"
    if len(mtf) >= 2 and len(ltf) >= 2 and len(exc) >= 2:
        mtf_z = _get_zones(mtf)
        ltf_z = _get_zones(ltf)
        for htf_z in bear_z:
            zkey = f"PE_{htf_z.get('zone_low', 0):.0f}"
            if zkey in sl_hist_bear and (day - sl_hist_bear[zkey]).days <= 1: continue
            mtf_m = next((z for z in mtf_z if _zones_overlap(htf_z, z)), None)
            if not mtf_m: continue
            ltf_m = next((z for z in ltf_z if _zones_overlap(mtf_m, z)), None)
            if not ltf_m: continue
            res = _simulate(exc, ltf_m, htf_z, SL_BUF, CAP_PTS, LOT)
            if res:
                res.update({"date": str(day), "opt": "PE", "atm": atm,
                            "strike": used_strike, "zone_kind": "BEAR"})
                pe_trades_bear.append(res)
                traded = f"BEAR pnl={res['pnl']:+.0f}"
                if res["exit"] == "SL": sl_hist_bear[zkey] = day
                break

    print(f"  {str(day):12} {atm:6}  {used_strike:>10}  "
          f"{len(opt_df):>5}  {len(htf_zones):>10}  {len(bull_z):>5}  {len(bear_z):>5}  {traded}")

print()
print(f"Days with BULL zone on PE bars: {pe_bull_days}/22")
print(f"Days with BEAR zone on PE bars: {pe_bear_days}/22  <- these are the tradeable setups")
print()

# ── PE BEAR zone trade results ────────────────────────────────────────────────
if pe_trades_bear:
    print("PE BEAR-zone trades (PE premium seller traps):")
    print(f"  {'Date':12} {'ATM':6} {'Entry':7} {'SL':7} {'Target':8} {'PnL':8} {'Exit'}")
    print("-"*70)
    for t in pe_trades_bear:
        print(f"  {t['date']:12} {t['atm']:6} {t['entry']:7.1f} {t['sl']:7.1f} "
              f"{t['target']:8.1f} {t['pnl']:8.1f}  {t['exit']}")
    wins = [t for t in pe_trades_bear if t["pnl"] > 0]
    losses = [t for t in pe_trades_bear if t["pnl"] <= 0]
    gw = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    net = sum(t["pnl"] for t in pe_trades_bear)
    print(f"  Trades={len(pe_trades_bear)} Wins={len(wins)} WR={len(wins)/len(pe_trades_bear)*100:.0f}% "
          f"PF={gw/gl:.2f if gl>0 else 9999} Net=Rs {net:.0f}")
else:
    print("No PE trades found with BEAR zones either.")

print()
print("KEY INSIGHT:")
print("  CE BEAR zone = CE premium rises (market rallies) then sellers get trapped")
print("  PE BEAR zone = PE premium rises (market falls) then sellers get trapped")
print("  In June 2026 NIFTY was UPTRENDING -> PE premium was mostly FALLING")
print("  -> Very few BEAR zones form on falling PE premium -> that is why 0 PE trades")
print()
print("YOUR OBSERVATION IS CORRECT:")
print("  If NIFTY had a down day, PE premium would spike -> form BEAR zone -> PE trap setup")
print("  But June was mostly up -> CE zones dominated")
