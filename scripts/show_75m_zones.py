from __future__ import annotations
"""
show_75m_zones.py — 3-level seller trap zone detection for SENSEX CE/PE.

Zone validity periods:
  75m zones : valid for 10 trading days (multi-day pool)
  15m zones : intraday only (same day they form)
  5m zones  : intraday only (same day they form)

Hierarchy:
  Build 75m zone pool (last 10 days)
  Each day: if 1m price enters ANY pooled 75m zone
    -> track 15m zones inside that zone (intraday)
    -> 15m SL hit + price returns to 15m zone_high
      -> track 5m zones inside (intraday)
      -> 5m SL hit + price returns to 5m zone_high = ENTRY

Usage:
  python scripts/show_75m_zones.py --token <upstox_token> [--strike 77000] [--side CE]
"""
import argparse, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import pandas as pd
from datetime import date, timedelta
from typing import Optional

HEADERS = lambda t: {"Authorization": f"Bearer {t}", "Accept": "application/json"}
ZONE_VALID_DAYS = 10

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
    t_open  = pd.Timestamp("09:15:00").time()
    t_close = pd.Timestamp("15:29:00").time()
    df = df[(df["ts"].dt.time >= t_open) & (df["ts"].dt.time <= t_close)]
    return df.reset_index(drop=True)

# ── Resampling anchored at 09:15 ──────────────────────────────────────────────

def resample(df1m: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df1m.empty:
        return pd.DataFrame()
    base = df1m["ts"].iloc[0].normalize() + pd.Timedelta("9h15m")
    df   = df1m.copy()
    df["bucket"] = ((df["ts"] - base).dt.total_seconds() // (minutes * 60)).astype(int)
    return df.groupby("bucket").agg(
        ts=("ts","first"), open=("open","first"),
        high=("high","max"), low=("low","min"), close=("close","last"),
    ).reset_index(drop=True)

# ── Core zone detection ───────────────────────────────────────────────────────

def detect_zones(candles: pd.DataFrame) -> list:
    """
    Seller-trap zones: c1.low < c0.low -> zone forms.
    Valid only if a later candle HIGH >= c0.HIGH (sellers SL hit).
    zone_high = c0.low  (where sellers entered)
    zone_low  = c1.low  (how far sellers pushed)
    sl_level  = c0.high (sellers stop loss)
    """
    zones = []
    for i in range(len(candles) - 1):
        c0, c1 = candles.iloc[i], candles.iloc[i + 1]
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

def first_1m_entry(df1m: pd.DataFrame, zone: dict,
                   after_ts: pd.Timestamp) -> Optional[pd.Timestamp]:
    """First 1m bar after after_ts whose price overlaps [zone_low, zone_high]."""
    for _, bar in df1m[df1m["ts"] > after_ts].iterrows():
        if float(bar["low"]) <= zone["zone_high"] and float(bar["high"]) >= zone["zone_low"]:
            return bar["ts"]
    return None

def first_return_to_zone_high(df1m: pd.DataFrame, zone: dict,
                               after_ts: pd.Timestamp) -> Optional[dict]:
    """First 1m bar after after_ts whose LOW <= zone_high (price returns to entry level)."""
    for _, bar in df1m[df1m["ts"] > after_ts].iterrows():
        if float(bar["low"]) <= zone["zone_high"]:
            return {
                "entry_ts":    bar["ts"],
                "entry_price": zone["zone_high"],
                "bar_open":    float(bar["open"]),
            }
    return None

# ── Per-day analysis ──────────────────────────────────────────────────────────

def analyse_day(dt: date, df1m: pd.DataFrame, z75_pool: list) -> None:
    """
    For one trading day:
      - Check if 1m price enters ANY active 75m zone from the pool
      - If yes, find 15m zones (intraday) -> 5m zones (intraday) -> ENTRY
    """
    print(f"\n{'─'*65}")
    print(f"Day: {dt}")

    if df1m.empty:
        print("  no 1m data")
        return

    mtf_15 = resample(df1m, 15)
    ltf_5  = resample(df1m,  5)
    day_start = df1m["ts"].iloc[0] - pd.Timedelta(minutes=1)

    # Only zones whose SL was already hit BEFORE today are "active"
    today_start = pd.Timestamp(dt)
    active_75 = [z for z in z75_pool if z["sl_hit_ts"] < today_start]

    if not active_75:
        print("  No active 75m zones from pool for this day")
        return

    hit_any = False
    for z75 in active_75:
        formed = z75["formed_ts"].strftime("%Y-%m-%d %H:%M")
        entry_1m_ts = first_1m_entry(df1m, z75, day_start)
        if entry_1m_ts is None:
            continue
        hit_any = True
        print(f"\n  >> 75m zone (formed {formed})  "
              f"zone_high={z75['zone_high']:.1f}  zone_low={z75['zone_low']:.1f}  "
              f"sl_level={z75['sl_level']:.1f}")
        print(f"     1m enters zone at {entry_1m_ts.strftime('%H:%M')} -> tracking 15m zones")

        zones_15 = [z for z in detect_zones(mtf_15)
                    if z["zone_low"] >= z75["zone_low"]
                    and z["zone_high"] <= z75["zone_high"]
                    and z["formed_ts"] >= entry_1m_ts]

        if not zones_15:
            print("     x No valid 15m zones inside 75m zone today")
            continue

        for z15 in zones_15:
            t15   = z15["formed_ts"].strftime("%H:%M")
            sl15t = z15["sl_hit_ts"].strftime("%H:%M")
            print(f"\n       >> 15m zone [{t15}]  "
                  f"zone_high={z15['zone_high']:.1f}  zone_low={z15['zone_low']:.1f}  "
                  f"sl_level={z15['sl_level']:.1f}  SL_hit@{sl15t}")

            ret15 = first_return_to_zone_high(df1m, z15, z15["sl_hit_ts"])
            if ret15 is None:
                print("         x Price never returned to 15m zone_high today")
                continue
            ret15_t = ret15["entry_ts"].strftime("%H:%M")
            print(f"         + Returns to {z15['zone_high']:.1f} at {ret15_t} -> tracking 5m zones")

            zones_5 = [z for z in detect_zones(ltf_5)
                       if z["zone_low"] >= z15["zone_low"]
                       and z["zone_high"] <= z15["zone_high"]
                       and z["formed_ts"] >= ret15["entry_ts"]]

            if not zones_5:
                print("         x No valid 5m zones inside 15m zone")
                continue

            for z5 in zones_5:
                t5   = z5["formed_ts"].strftime("%H:%M")
                sl5t = z5["sl_hit_ts"].strftime("%H:%M")
                print(f"\n           >> 5m zone [{t5}]  "
                      f"zone_high={z5['zone_high']:.1f}  zone_low={z5['zone_low']:.1f}  "
                      f"sl_level={z5['sl_level']:.1f}  SL_hit@{sl5t}")

                ret5 = first_return_to_zone_high(df1m, z5, z5["sl_hit_ts"])
                if ret5 is None:
                    print("             x Price never returned to 5m zone_high — no entry")
                else:
                    ret5_t = ret5["entry_ts"].strftime("%H:%M")
                    print(f"             * ENTRY  time={ret5_t}  "
                          f"price={ret5['entry_price']:.1f}  "
                          f"bar_open={ret5['bar_open']:.1f}")

    if not hit_any:
        print("  Price never entered any active 75m zone today")

# ── Instrument key lookup ─────────────────────────────────────────────────────

_KEY_CACHE: dict = {}

def get_key(token: str, strike: int, side: str, trade_date: date) -> tuple:
    for delta in range(15):
        exp = trade_date + timedelta(days=delta)
        ck  = (strike, side, exp.strftime("%d%b%y").upper())
        if ck in _KEY_CACHE:
            return _KEY_CACHE[ck], exp
        try:
            r = requests.get(
                "https://api.upstox.com/v2/instruments/search",
                params={"exchange":"BSE_FO","segment":"BSE_FO",
                        "query": f"SENSEX {strike} {side} {exp.strftime('%d%b%y').upper()}"},
                headers=HEADERS(token), timeout=10,
            )
            items = r.json().get("data", [])
            if items:
                key = items[0].get("instrument_key","")
                if key:
                    _KEY_CACHE[ck] = key
                    return key, exp
        except Exception:
            pass
    return "", None

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_trading_days(n: int) -> list:
    days, d = [], date.today()
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    return sorted(days)

# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token",  required=True)
    parser.add_argument("--strike", type=int, default=77000)
    parser.add_argument("--side",   default="CE")
    parser.add_argument("--days",   type=int, default=10,
                        help="Number of trading days to analyse")
    args = parser.parse_args()

    # Fetch data for pool window (extra 5 days buffer for zone building)
    pool_days  = get_trading_days(args.days + 5)
    trade_days = get_trading_days(args.days)

    print(f"\n{'='*65}")
    print(f"SENSEX {args.strike} {args.side} — Multi-day Zone Analysis")
    print(f"75m zones valid {ZONE_VALID_DAYS} days | 15m+5m zones intraday only")
    print(f"{'='*65}")

    # Step 1: build 75m zone pool and cache 1m data
    print("\nBuilding 75m zone pool...")
    z75_pool: list = []
    day_data: dict = {}

    for dt in pool_days:
        key, _ = get_key(args.token, args.strike, args.side, dt)
        if not key:
            continue
        df1m = fetch_1m(key, dt, args.token)
        if df1m.empty:
            continue
        day_data[dt] = df1m
        for z in detect_zones(resample(df1m, 75)):
            sl_d = z["sl_hit_ts"].strftime("%Y-%m-%d %H:%M")
            print(f"  {dt}  zone_high={z['zone_high']:.1f}  zone_low={z['zone_low']:.1f}  "
                  f"sl_level={z['sl_level']:.1f}  SL_hit={sl_d}")
            z75_pool.append(z)

    if not z75_pool:
        print("No valid 75m zones found — nothing to analyse.")
        return

    print(f"\nTotal pooled 75m zones: {len(z75_pool)}")

    # Step 2: analyse each trade day against the pool
    for dt in trade_days:
        df1m = day_data.get(dt)
        if df1m is None or df1m.empty:
            key, _ = get_key(args.token, args.strike, args.side, dt)
            if key:
                df1m = fetch_1m(key, dt, args.token)
            if df1m is None or df1m.empty:
                print(f"\n{'─'*65}")
                print(f"Day: {dt}  — no data")
                continue
        analyse_day(dt, df1m, z75_pool)

    print(f"\n{'='*65}\n")

if __name__ == "__main__":
    main()
