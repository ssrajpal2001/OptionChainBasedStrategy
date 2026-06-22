from __future__ import annotations
"""
show_75m_zones.py — 3-level seller trap zone detection for SENSEX CE/PE.

Hierarchy:
  75m zone valid (c0.high SL hit)
    -> 1m candle enters 75m zone
      -> 15m zones inside (c0.high SL hit)
        -> price returns to 15m zone_high
          -> 5m zones inside (c0.high SL hit)
            -> price returns to 5m zone_high = ENTRY

Usage:
  python scripts/show_75m_zones.py --token <upstox_token>
"""
import argparse, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import pandas as pd
from datetime import date, timedelta
from typing import Optional

HEADERS = lambda t: {"Authorization": f"Bearer {t}", "Accept": "application/json"}

# ── Data fetch ────────────────────────────────────────────────────────────────

def fetch_1m(key: str, dt: date, token: str) -> pd.DataFrame:
    ds  = dt.strftime("%Y-%m-%d")
    url = f"https://api.upstox.com/v2/historical-candle/{key}/1minute/{ds}/{ds}"
    r   = requests.get(url, headers=HEADERS(token), timeout=15)
    candles = r.json().get("data", {}).get("candles", [])
    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame(candles, columns=["ts","open","high","low","close","vol","oi"])
    df["ts"] = pd.to_datetime(df["ts"]).dt.tz_localize(None)
    df = df.sort_values("ts").reset_index(drop=True)
    market_open  = pd.Timestamp("09:15:00").time()
    market_close = pd.Timestamp("15:29:00").time()
    df = df[(df["ts"].dt.time >= market_open) & (df["ts"].dt.time <= market_close)]
    return df.reset_index(drop=True)

# ── Resampling anchored at 09:15 ──────────────────────────────────────────────

def resample(df1m: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df1m.empty:
        return pd.DataFrame()
    base = df1m["ts"].iloc[0].normalize() + pd.Timedelta("9h15m")
    df   = df1m.copy()
    df["bucket"] = ((df["ts"] - base).dt.total_seconds() // (minutes * 60)).astype(int)
    out = df.groupby("bucket").agg(
        ts=("ts","first"), open=("open","first"),
        high=("high","max"), low=("low","min"), close=("close","last"),
    ).reset_index(drop=True)
    return out

# ── Core zone detection ───────────────────────────────────────────────────────

def detect_zones(candles: pd.DataFrame) -> list:
    """
    Find valid seller-trap zones.
    Zone forms: candle[N+1].low < candle[N].low
    Valid only if a later candle HIGH >= candle[N].HIGH (sellers SL hit)
    """
    zones = []
    for i in range(len(candles) - 1):
        c0 = candles.iloc[i]
        c1 = candles.iloc[i + 1]
        if float(c1["low"]) >= float(c0["low"]):
            continue
        zone_high = float(c0["low"])
        zone_low  = float(c1["low"])
        sl_level  = float(c0["high"])
        sl_hit_ts = None
        for j in range(i + 2, len(candles)):
            if float(candles.iloc[j]["high"]) >= sl_level:
                sl_hit_ts = candles.iloc[j]["ts"]
                break
        if sl_hit_ts is None:
            continue
        zones.append({
            "formed_ts": c0["ts"],
            "c1_ts":     c1["ts"],
            "zone_high": zone_high,
            "zone_low":  zone_low,
            "sl_level":  sl_level,
            "sl_hit_ts": sl_hit_ts,
        })
    return zones

# ── Helpers ───────────────────────────────────────────────────────────────────

def first_1m_entry_into_zone(df1m: pd.DataFrame, zone: dict,
                              after_ts: pd.Timestamp) -> Optional[pd.Timestamp]:
    window = df1m[df1m["ts"] > after_ts]
    for _, bar in window.iterrows():
        if float(bar["low"]) <= zone["zone_high"] and float(bar["high"]) >= zone["zone_low"]:
            return bar["ts"]
    return None

def first_return_to_zone_high(df1m: pd.DataFrame, zone: dict,
                               after_ts: pd.Timestamp) -> Optional[dict]:
    window = df1m[df1m["ts"] > after_ts]
    for _, bar in window.iterrows():
        if float(bar["low"]) <= zone["zone_high"]:
            return {
                "entry_ts":    bar["ts"],
                "entry_price": zone["zone_high"],
                "bar_open":    float(bar["open"]),
            }
    return None

# ── Main per-day logic ────────────────────────────────────────────────────────

def analyse_day(df1m: pd.DataFrame, dt: date) -> None:
    if df1m.empty:
        return

    htf_75 = resample(df1m, 75)
    mtf_15 = resample(df1m, 15)
    ltf_5  = resample(df1m,  5)

    zones_75 = detect_zones(htf_75)
    if not zones_75:
        print("  No valid 75m zones")
        return

    for z75 in zones_75:
        t0   = z75["formed_ts"].strftime("%H:%M")
        t1   = z75["c1_ts"].strftime("%H:%M")
        sl_t = z75["sl_hit_ts"].strftime("%H:%M")
        print(f"\n  >> 75m zone [{t0}->{t1}]  zone_high={z75['zone_high']:.1f}  "
              f"zone_low={z75['zone_low']:.1f}  sl_level={z75['sl_level']:.1f}  SL_hit@{sl_t}")

        entry_1m_ts = first_1m_entry_into_zone(df1m, z75, z75["c1_ts"])
        if entry_1m_ts is None:
            print("    x 1m never entered 75m zone")
            continue
        print(f"    + 1m enters zone at {entry_1m_ts.strftime('%H:%M')} -> track 15m zones")

        zones_15 = [z for z in detect_zones(mtf_15)
                    if z["zone_low"] >= z75["zone_low"]
                    and z["zone_high"] <= z75["zone_high"]
                    and z["formed_ts"] >= entry_1m_ts]

        if not zones_15:
            print("    x No valid 15m zones inside 75m zone")
            continue

        for z15 in zones_15:
            t15   = z15["formed_ts"].strftime("%H:%M")
            sl15t = z15["sl_hit_ts"].strftime("%H:%M")
            print(f"\n      >> 15m zone [{t15}]  zone_high={z15['zone_high']:.1f}  "
                  f"zone_low={z15['zone_low']:.1f}  sl_level={z15['sl_level']:.1f}  SL_hit@{sl15t}")

            ret15 = first_return_to_zone_high(df1m, z15, z15["sl_hit_ts"])
            if ret15 is None:
                print("        x Price never returned to 15m zone_high")
                continue
            ret15_t = ret15["entry_ts"].strftime("%H:%M")
            print(f"        + Price returns to {z15['zone_high']:.1f} at {ret15_t} -> track 5m zones")

            zones_5 = [z for z in detect_zones(ltf_5)
                       if z["zone_low"] >= z15["zone_low"]
                       and z["zone_high"] <= z15["zone_high"]
                       and z["formed_ts"] >= ret15["entry_ts"]]

            if not zones_5:
                print("        x No valid 5m zones inside 15m zone")
                continue

            for z5 in zones_5:
                t5   = z5["formed_ts"].strftime("%H:%M")
                sl5t = z5["sl_hit_ts"].strftime("%H:%M")
                print(f"\n          >> 5m zone [{t5}]  zone_high={z5['zone_high']:.1f}  "
                      f"zone_low={z5['zone_low']:.1f}  sl_level={z5['sl_level']:.1f}  SL_hit@{sl5t}")

                ret5 = first_return_to_zone_high(df1m, z5, z5["sl_hit_ts"])
                if ret5 is None:
                    print("            x Price never returned to 5m zone_high — no entry")
                else:
                    ret5_t = ret5["entry_ts"].strftime("%H:%M")
                    print(f"            * ENTRY  time={ret5_t}  price={ret5['entry_price']:.1f}  "
                          f"bar_open={ret5['bar_open']:.1f}")

# ── Instrument key lookup ─────────────────────────────────────────────────────

_KEY_CACHE: dict = {}

def get_key_for_strike(token: str, strike: int, side: str, trade_date: date) -> tuple:
    for delta in range(15):
        candidate = trade_date + timedelta(days=delta)
        ck = (strike, side, candidate.strftime("%d%b%y").upper())
        if ck in _KEY_CACHE:
            return _KEY_CACHE[ck], candidate
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
                    _KEY_CACHE[ck] = key
                    return key, candidate
        except Exception:
            pass
    return "", None

# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token",  required=True)
    parser.add_argument("--strike", type=int, default=77000)
    parser.add_argument("--side",   default="CE")
    args = parser.parse_args()

    today     = date.today()
    days_back = today.weekday() + 7
    prev_mon  = today - timedelta(days=days_back)
    dates = [prev_mon + timedelta(days=i)
             for i in range(14)
             if (prev_mon + timedelta(days=i)).weekday() < 5
             and (prev_mon + timedelta(days=i)) <= today]

    print(f"\n{'='*65}")
    print(f"SENSEX {args.strike} {args.side} — 75m/15m/5m Seller Trap Zones")
    print(f"{'='*65}")

    for dt in dates:
        key, _ = get_key_for_strike(args.token, args.strike, args.side, dt)
        if not key:
            print(f"\n{dt} — key not found, skip")
            continue
        df1m = fetch_1m(key, dt, args.token)
        if df1m.empty:
            print(f"\n{dt} — no 1m data")
            continue
        print(f"\n{'─'*65}")
        print(f"Date: {dt}  |  Key: {key}")
        analyse_day(df1m, dt)

    print(f"\n{'='*65}\n")

if __name__ == "__main__":
    main()
