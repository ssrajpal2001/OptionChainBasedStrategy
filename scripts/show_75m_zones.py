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
    # Strict market hours: 09:15:00 to 15:29:59 only
    market_open  = pd.Timestamp("09:15:00").time()
    market_close = pd.Timestamp("15:29:00").time()
    df = df[(df["ts"].dt.time >= market_open) & (df["ts"].dt.time <= market_close)]
    return df.reset_index(drop=True)

def resample_75m(df1m: pd.DataFrame) -> pd.DataFrame:
    """Resample 1m bars to 75m, anchored at 09:15 each day."""
    if df1m.empty:
        return pd.DataFrame()
    # Compute minutes-since-0915 and bucket into 75m blocks
    base = df1m["ts"].iloc[0].normalize() + pd.Timedelta("9h15m")
    df1m = df1m.copy()
    df1m["bucket"] = ((df1m["ts"] - base).dt.total_seconds() // (75 * 60)).astype(int)
    out = df1m.groupby("bucket").agg(
        ts=("ts", "first"),
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
    ).reset_index(drop=True)
    return out

def find_zones(htf: pd.DataFrame) -> list:
    """
    Find zones where:
    1. candle[N+1].low < candle[N].low  (breach = zone formed)
    2. A subsequent candle's HIGH >= zone_high (sellers' SL triggered = valid zone)
    """
    zones = []
    for i in range(len(htf) - 1):
        c0 = htf.iloc[i]
        c1 = htf.iloc[i+1]
        if float(c1["low"]) >= float(c0["low"]):
            continue
        zone_high = float(c0["low"])
        zone_low  = float(c1["low"])
        # Check subsequent candles for SL hit (high >= zone_high)
        sl_hit_ts = None
        for j in range(i + 2, len(htf)):
            if float(htf.iloc[j]["high"]) >= zone_high:
                sl_hit_ts = htf.iloc[j]["ts"]
                break
        if sl_hit_ts is None:
            continue   # SL never hit — zone not valid
        zones.append({
            "candle_0_ts":  c0["ts"],
            "candle_1_ts":  c1["ts"],
            "zone_high":    zone_high,
            "zone_low":     zone_low,
            "range":        round(zone_high - zone_low, 1),
            "sl_hit_ts":    sl_hit_ts,
        })
    return zones


def resample_15m(df1m: pd.DataFrame) -> pd.DataFrame:
    """Resample 1m bars to 15m, anchored at 09:15."""
    if df1m.empty:
        return pd.DataFrame()
    base = df1m["ts"].iloc[0].normalize() + pd.Timedelta("9h15m")
    df1m = df1m.copy()
    df1m["bucket"] = ((df1m["ts"] - base).dt.total_seconds() // (15 * 60)).astype(int)
    out = df1m.groupby("bucket").agg(
        ts=("ts", "first"),
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
    ).reset_index(drop=True)
    return out


def find_15m_zones_inside(df1m: pd.DataFrame, z75: dict) -> list:
    """
    Find 15m zones inside a given 75m zone.
    Time window: candle_0_ts to candle_1_ts + 75min
    Price window: zone_low to zone_high
    """
    t_start = z75["candle_0_ts"]
    t_end   = z75["candle_1_ts"] + pd.Timedelta("75min")
    window  = df1m[(df1m["ts"] >= t_start) & (df1m["ts"] < t_end)].copy()
    if window.empty:
        return []

    mtf = resample_15m(window)
    zones_15m = []
    for i in range(len(mtf) - 1):
        c0 = mtf.iloc[i]
        c1 = mtf.iloc[i+1]
        if float(c1["low"]) >= float(c0["low"]):
            continue
        zh = float(c0["low"])
        zl = float(c1["low"])
        # Must be inside the 75m zone price range
        if zl < z75["zone_low"] or zh > z75["zone_high"]:
            continue
        # SL must be hit: subsequent 15m candle high >= zone_high
        sl_hit_ts = None
        for j in range(i + 2, len(mtf)):
            if float(mtf.iloc[j]["high"]) >= zh:
                sl_hit_ts = mtf.iloc[j]["ts"]
                break
        if sl_hit_ts is None:
            continue
        zones_15m.append({
            "ts":         c0["ts"],
            "zone_high":  zh,
            "zone_low":   zl,
            "range":      round(zh - zl, 1),
            "sl_hit_ts":  sl_hit_ts,
        })
    return zones_15m

_KEY_CACHE: dict = {}   # (strike, side, expiry_str) → key

def get_key_for_strike(token: str, strike: int, side: str, trade_date: date) -> tuple:
    """
    Find instrument key by trying expiry dates from trade_date to +14 days.
    Caches results so the same contract key is reused across the week.
    """
    for delta in range(15):
        candidate = trade_date + timedelta(days=delta)
        cache_k = (strike, side, candidate.strftime("%d%b%y").upper())
        if cache_k in _KEY_CACHE:
            return _KEY_CACHE[cache_k], candidate
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
                    _KEY_CACHE[cache_k] = key
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
            print(f"\n75m ZONES ({len(zones)}):")
            for z in zones:
                t0 = z["candle_0_ts"].strftime("%H:%M")
                t1 = z["candle_1_ts"].strftime("%H:%M")
                sl_t = z["sl_hit_ts"].strftime("%H:%M")
                print(f"\n  75m [{t0}→{t1}]  zone_high={z['zone_high']:.1f}  zone_low={z['zone_low']:.1f}  range={z['range']:.1f}pts  SL_hit={sl_t}")
                zones_15 = find_15m_zones_inside(df1m, z)
                if zones_15:
                    for z15 in zones_15:
                        t15    = z15["ts"].strftime("%H:%M")
                        sl15_t = z15["sl_hit_ts"].strftime("%H:%M")
                        print(f"    └─ 15m [{t15}]  zone_high={z15['zone_high']:.1f}  zone_low={z15['zone_low']:.1f}  range={z15['range']:.1f}pts  SL_hit={sl15_t}")
                else:
                    print(f"    └─ No valid 15m zone (SL not hit) inside this 75m zone")
        else:
            print(f"\n  No 75m zones detected")

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
