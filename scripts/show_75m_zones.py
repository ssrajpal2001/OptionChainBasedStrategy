"""
show_75m_zones.py — Print 75-minute candles and zones for SENSEX CE 77000.

Zone rule (user-defined):
  For each pair of consecutive 75m candles:
  IF candle[N+1].low < candle[N].low  →  ZONE detected
  Zone HIGH = candle[N].low
  Zone LOW  = candle[N+1].low

Usage:
  python scripts/show_75m_zones.py --token <upstox_token>
"""
import argparse
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import pandas as pd
from datetime import date, timedelta

HEADERS = lambda t: {"Authorization": f"Bearer {t}", "Accept": "application/json"}

def fetch_1m(key: str, dt: date, token: str) -> pd.DataFrame:
    ds = dt.strftime("%Y-%m-%d")
    url = f"https://api.upstox.com/v2/historical-candle/{key}/1minute/{ds}/{ds}"
    r = requests.get(url, headers=HEADERS(token), timeout=15)
    d = r.json()
    candles = d.get("data", {}).get("candles", [])
    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame(candles, columns=["ts","open","high","low","close","vol","oi"])
    df["ts"] = pd.to_datetime(df["ts"]).dt.tz_localize(None)
    df = df.sort_values("ts").reset_index(drop=True)
    # Keep only market hours 9:15 - 15:30
    df = df[(df["ts"].dt.time >= pd.Timestamp("09:15").time()) &
            (df["ts"].dt.time <= pd.Timestamp("15:30").time())]
    return df

def resample_75m(df1m: pd.DataFrame) -> pd.DataFrame:
    df = df1m.set_index("ts")
    rule = "75min"
    out = df.resample(rule, origin=pd.Timestamp("2000-01-01 09:15:00")).agg(
        open=("open","first"), high=("high","max"),
        low=("low","min"), close=("close","last")
    ).dropna(subset=["open"])
    out = out.reset_index()
    return out

def find_zones(htf: pd.DataFrame) -> list:
    zones = []
    for i in range(len(htf) - 1):
        c0 = htf.iloc[i]
        c1 = htf.iloc[i+1]
        if float(c1["low"]) < float(c0["low"]):
            zones.append({
                "candle_0_ts":  c0["ts"],
                "candle_0_low": float(c0["low"]),
                "candle_1_ts":  c1["ts"],
                "candle_1_low": float(c1["low"]),
                "zone_high":    float(c0["low"]),   # top of zone
                "zone_low":     float(c1["low"]),   # bottom of zone
                "range":        round(float(c0["low"]) - float(c1["low"]), 1),
            })
    return zones

def get_key_for_strike(token: str, strike: int, side: str, trade_date: date) -> tuple:
    """Find instrument key by trying expiry dates from trade_date to +14 days."""
    from datetime import timedelta
    for delta in range(15):
        candidate = trade_date + timedelta(days=delta)
        try:
            r = requests.get(
                "https://api.upstox.com/v2/instruments/search",
                params={"exchange":"BSE_FO","segment":"BSE_FO",
                        "query": f"SENSEX {strike} {side} {candidate.strftime('%d%b%y').upper()}"},
                headers=HEADERS(token), timeout=10
            )
            items = r.json().get("data", [])
            if items:
                key = items[0].get("instrument_key","")
                if key:
                    return key, candidate
        except Exception:
            pass
    return "", None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", required=True)
    parser.add_argument("--strike", type=int, default=77000)
    parser.add_argument("--side", default="CE")
    args = parser.parse_args()

    token = args.token
    strike = args.strike
    side = args.side

    # Dates: previous week Mon-Fri + current week so far
    today = date.today()
    # Go back to find last Monday
    days_back = today.weekday() + 7   # previous week's Monday
    prev_mon = today - timedelta(days=days_back)
    dates = []
    for i in range(14):
        d = prev_mon + timedelta(days=i)
        if d.weekday() < 5 and d <= today:   # Mon-Fri only
            dates.append(d)

    print(f"\n{'='*60}")
    print(f"SENSEX {strike} {side} — 75m Zone Detection")
    print(f"{'='*60}")

    for dt in dates:
        # Find key (use the first date's search result, reuse across week)
        key, expiry = get_key_for_strike(token, strike, side, dt)
        if not key:
            print(f"\n{dt} — could not find instrument key, skip")
            continue

        df1m = fetch_1m(key, dt, token)
        if df1m.empty:
            print(f"\n{dt} — no 1m data (holiday or no trades)")
            continue

        htf = resample_75m(df1m)
        zones = find_zones(htf)

        print(f"\n{'─'*60}")
        print(f"Date: {dt}  |  Key: {key}  |  Expiry: {expiry}")
        print(f"75m Candles ({len(htf)} total):")
        for _, row in htf.iterrows():
            ts = row["ts"].strftime("%H:%M")
            print(f"  {ts}  O={row['open']:.1f}  H={row['high']:.1f}  L={row['low']:.1f}  C={row['close']:.1f}")

        if zones:
            print(f"\nZONES DETECTED ({len(zones)}):")
            for z in zones:
                t0 = z["candle_0_ts"].strftime("%H:%M")
                t1 = z["candle_1_ts"].strftime("%H:%M")
                print(f"  [{t0}→{t1}]  zone_high={z['zone_high']:.1f}  zone_low={z['zone_low']:.1f}  range={z['range']:.1f}pts")
        else:
            print(f"\n  No zones detected (no candle breached previous low)")

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
