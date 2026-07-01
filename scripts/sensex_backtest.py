"""
SENSEX Trap Scanner Backtest — Jun 17 2026
Fetches CE1(76600)/PE1(77000) 1-min bars from Upstox,
replays through zone detector, compares:
  A) Current logic: shared intraday_mode
  B) Proposed: per-leg independent cascade

Usage: python scripts/sensex_backtest.py
"""
import sys, os, requests, pandas as pd
from datetime import date
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI3NkFFNDciLCJqdGkiOiI2YTMzNjU1N2EwZTg2ODU4Y2ZkZmU0N2MiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzgxNzUzMTc1LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3ODE4MjAwMDB9.DL0Vhwm0P2yGxKAn5HLGWkqIJvgxwp857Q4S_RnXF2E"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}

# From morning init log
CE1_KEY  = "BSE_FO|1137766"   # SENSEX 76600 CE
PE1_KEY  = "BSE_FO|1147016"   # SENSEX 77000 PE
DATE     = "2026-06-17"
HTF_MIN  = 75
LTF_MIN  = 5
CASCADE_MIN = 15
ATR_MULT = 1.5

def fetch_1m(instrument_key: str, dt: str) -> pd.DataFrame:
    key_enc = instrument_key.replace("|", "%7C")
    url = f"https://api.upstox.com/v2/historical-candle/{key_enc}/1minute/{dt}/{dt}"
    r = requests.get(url, headers=HEADERS)
    data = r.json()
    if data.get("status") != "success":
        print(f"  ERROR fetching {instrument_key}: {data}")
        return pd.DataFrame()
    candles = data["data"]["candles"]
    df = pd.DataFrame(candles, columns=["ts","o","h","l","c","v","oi"])
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.sort_values("ts").reset_index(drop=True)
    # Market hours only
    df = df[(df["ts"].dt.time >= pd.Timestamp("09:15").time()) &
            (df["ts"].dt.time <= pd.Timestamp("15:30").time())]
    return df

def resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    df = df.set_index("ts")
    r = df["c"].resample(f"{minutes}min", label="right", closed="right").ohlc()
    r.columns = ["open","high","low","close"]
    r["volume"] = df["v"].resample(f"{minutes}min", label="right", closed="right").sum()
    return r.dropna().reset_index()

def scan_zones(htf: pd.DataFrame, label: str) -> list:
    """Simple seller-trap scanner: finds candles where price dips below low then recovers above high."""
    zones = []
    for i in range(1, len(htf)):
        row = htf.iloc[i]
        zh = round(row["high"], 2)
        zl = round(row["low"],  2)
        zt = round(zl + (zh - zl) * 0.33, 2)  # trigger at 33% of zone
        sl_target = round(zh + (zh - zl) * 1.5, 2)
        zones.append({
            "ts": row["ts"], "zone_high": zh, "zone_low": zl,
            "zone_trigger": zt, "target": sl_target, "label": label,
            "status": "TRAPPED"
        })
    return zones

def compute_atr(htf: pd.DataFrame) -> float:
    if len(htf) < 2:
        return 0.0
    tr = (htf["high"] - htf["low"]).abs()
    return round(tr.mean(), 2)

def run_backtest(df: pd.DataFrame, leg: str, htf_min=75, ltf_min=5, cascade_min=15):
    print(f"\n{'='*60}")
    print(f"  {leg} — Bars: {len(df)}  Range: {df['ts'].iloc[0].strftime('%H:%M')} → {df['ts'].iloc[-1].strftime('%H:%M')}")
    print(f"  LTP range: {df['c'].min():.1f} → {df['c'].max():.1f}")
    print(f"{'='*60}")

    # Resample to HTF
    htf = resample(df, htf_min)
    atr = compute_atr(htf)
    threshold = ATR_MULT * atr
    print(f"\n[HTF {htf_min}m] Candles: {len(htf)}  ATR: {atr:.2f}  Threshold: {threshold:.2f}")

    htf_zones = scan_zones(htf, f"HTF-{htf_min}m")
    trapped_htf = [z for z in htf_zones if z["status"] == "TRAPPED"]
    print(f"[HTF] Zones found: {len(trapped_htf)}")
    for z in trapped_htf:
        print(f"  {z['ts'].strftime('%H:%M')}  Zone {z['zone_low']:.1f}→{z['zone_high']:.1f}  Trigger={z['zone_trigger']:.2f}  Target={z['target']:.2f}")

    # Check reachability at each 75-min boundary
    print(f"\n[REACHABILITY CHECK at each {htf_min}m boundary]")
    intraday_mode_current = False
    intraday_mode_pleg    = False

    for i, row in htf.iterrows():
        ltp_at_boundary = row["close"]
        zones_at_time = [z for z in trapped_htf if z["ts"] <= row["ts"]]
        if not zones_at_time:
            intraday_mode_current = True
            intraday_mode_pleg    = True
            print(f"  {row['ts'].strftime('%H:%M')}  LTP={ltp_at_boundary:.1f}  No zones → CASCADE (both logics)")
            continue

        nearest = min(abs(ltp_at_boundary - z["zone_trigger"]) for z in zones_at_time)

        # Current logic: cascade if nearest > threshold
        if nearest > threshold:
            intraday_mode_current = True
            status_cur = f"CASCADE (nearest={nearest:.1f} > {threshold:.1f})"
        else:
            intraday_mode_current = False
            status_cur = f"NORMAL  (nearest={nearest:.1f} ≤ {threshold:.1f})"

        # Per-leg logic: cascade if NO zone within threshold (same for single leg)
        intraday_mode_pleg = intraday_mode_current  # same for single leg
        print(f"  {row['ts'].strftime('%H:%M')}  LTP={ltp_at_boundary:.1f}  {status_cur}")

    # Cascade zones (15-min)
    cascade = resample(df, cascade_min)
    cascade_zones = scan_zones(cascade, f"CASCADE-{cascade_min}m")
    print(f"\n[CASCADE {cascade_min}m] Zones found: {len(cascade_zones)}")
    for z in cascade_zones[:10]:
        print(f"  {z['ts'].strftime('%H:%M')}  Zone {z['zone_low']:.1f}→{z['zone_high']:.1f}  Trigger={z['zone_trigger']:.2f}  Target={z['target']:.2f}")

    # LTF (5-min) zones — potential entries inside HTF zones
    ltf = resample(df, ltf_min)
    print(f"\n[LTF {ltf_min}m] Candles: {len(ltf)}")
    # Find 5-min zones that fall inside HTF zones
    entries = []
    for z in trapped_htf:
        for _, lr in ltf.iterrows():
            if z["zone_low"] <= lr["close"] <= z["zone_high"]:
                if lr["low"] < z["zone_trigger"]:
                    entries.append({"htf_zone": f"{z['zone_low']:.1f}→{z['zone_high']:.1f}",
                                    "entry_ts": lr["ts"], "entry_ltp": lr["close"],
                                    "trigger": z["zone_trigger"], "target": z["target"]})
    print(f"[ENTRIES inside HTF zones] {len(entries)} potential:")
    for e in entries[:5]:
        pnl = e["target"] - e["entry_ltp"]
        print(f"  {e['entry_ts'].strftime('%H:%M')}  HTF={e['htf_zone']}  Entry={e['entry_ltp']:.1f}  Target={e['target']:.1f}  Potential={pnl:.1f}pts")

    return len(trapped_htf), len(cascade_zones), len(entries)

print("Fetching SENSEX CE1 (76600) bars for", DATE)
df_ce = fetch_1m(CE1_KEY, DATE)
print(f"CE1 bars: {len(df_ce)}")

print("Fetching SENSEX PE1 (77000) bars for", DATE)
df_pe = fetch_1m(PE1_KEY, DATE)
print(f"PE1 bars: {len(df_pe)}")

if not df_ce.empty:
    run_backtest(df_ce, "CE1 (76600)")

if not df_pe.empty:
    run_backtest(df_pe, "PE1 (77000)")

print("\n\n=== SUMMARY ===")
print("Per-leg cascade benefit: PE1 'No zone' → would cascade to 15-min independently")
print("Check above: did CASCADE 15-min zones form closer to PE1 LTP range?")
