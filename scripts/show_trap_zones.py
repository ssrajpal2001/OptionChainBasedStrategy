"""
scripts/show_trap_zones.py
--------------------------
Diagnostic: show ALL zones found by TrapScanner at HTF (75m) and inside each
HTF zone at LTF (5m) for a given date and option instrument.

Usage:
  python scripts/show_trap_zones.py \
      --token <upstox_token> \
      --date  2026-06-18 \
      --key   NSE_FO|57202    \   # CE or PE instrument key
      --side  CE              \   # CE (bear trap) or PE (bull trap)
      --htf   75                  # HTF minutes (default 75)

Output:
  Prints a zone tree:
    [HTF 75m] zone_high=520 zone_low=480 sl=540 | status=TRAPPED at 10:30
      └─[LTF 5m] zone_high=505 zone_low=492 sl=512 | status=CLOSED at 11:18 ← ENTRY HERE
                 Entry price=497.0  SL=490  T1=512
"""

import argparse
import sys
import os
from datetime import date, timedelta
from urllib.parse import quote

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from strategies.trap_scanner.scanner import scan_htf, scan_ltf, scan_htf_spot

# ── Upstox fetch ──────────────────────────────────────────────────────────────

def _fetch_1m_single(key: str, dt: str, token: str) -> pd.DataFrame:
    """Fetch 1m bars for a single date. Returns empty df on non-trading days (400)."""
    enc = quote(key, safe="")
    url = f"https://api.upstox.com/v2/historical-candle/{enc}/1minute/{dt}/{dt}"
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {token}",
                                        "Accept": "application/json"}, timeout=30)
        if r.status_code == 400:
            return pd.DataFrame()   # non-trading day
        r.raise_for_status()
    except requests.exceptions.HTTPError:
        return pd.DataFrame()
    candles = r.json().get("data", {}).get("candles", [])
    if not candles:
        return pd.DataFrame()
    rows = [{"datetime": pd.to_datetime(c[0]),
             "open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4]),
             "volume": int(c[5])}
            for c in reversed(candles)]
    df = pd.DataFrame(rows)
    df["datetime"] = df["datetime"].dt.tz_localize(None)
    return df


def _fetch_1m(key: str, dt: str, token: str, lookback_days: int = 5) -> pd.DataFrame:
    """Fetch 1m bars for dt and up to lookback_days prior trading days for HTF pool."""
    frames = []
    target = date.fromisoformat(dt)
    d = target - timedelta(days=lookback_days + 4)  # go back extra to cover weekends
    collected = 0
    while d <= target:
        if d.weekday() < 5:  # Mon-Fri only
            df = _fetch_1m_single(key, d.isoformat(), token)
            if not df.empty:
                frames.append(df)
                collected += 1
        d += timedelta(days=1)
        if d > target and collected >= lookback_days:
            break
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames).drop_duplicates("datetime").sort_values("datetime").reset_index(drop=True)


def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    df2 = df.set_index("datetime").resample(f"{minutes}min", label="left", closed="left").agg(
        open=("open", "first"), high=("high", "max"),
        low=("low", "min"),   close=("close", "last"), volume=("volume", "sum")
    ).dropna(subset=["open"]).reset_index()
    return df2


def _hhmm(ts) -> str:
    if pd.isnull(ts):
        return "-"
    return pd.Timestamp(ts).strftime("%H:%M")


# ── Main ──────────────────────────────────────────────────────────────────────

def show_zones(token: str, dt: str, key: str, side: str, htf_min: int = 75):
    side = side.upper()
    print(f"\n{'='*70}")
    print(f"  TrapScanner Zone Diagnostic")
    print(f"  Date : {dt}  |  Key : {key}  |  Side : {side}  |  HTF : {htf_min}m")
    print(f"{'='*70}")

    # ── Fetch bars ────────────────────────────────────────────────────────────
    print(f"\n[1] Fetching 1m bars for {dt} + prior 5 trading days for HTF pool...")

    # All bars (prev 5 trading days + today) for HTF zone pool
    df_pool = _fetch_1m(key, dt, token, lookback_days=5)

    if df_pool.empty:
        print(f"  ERROR: No 1m data found. Check token or key.")
        return

    # Today-only bars for LTF scan
    today_str = pd.Timestamp(dt).date()
    df1m_today = df_pool[df_pool["datetime"].dt.date == today_str].copy().reset_index(drop=True)

    print(f"  1m bars (pool)  : {len(df_pool)} bars  "
          f"[{df_pool['datetime'].iloc[0].date()} → {df_pool['datetime'].iloc[-1].date()}]")
    print(f"  1m bars (today) : {len(df1m_today)} bars  ", end="")
    if df1m_today.empty:
        print(f"\n  ERROR: No bars for {dt} specifically. Check token or key.")
        return
    print(f"[{_hhmm(df1m_today['datetime'].iloc[0])} → {_hhmm(df1m_today['datetime'].iloc[-1])}]")

    # ── HTF zones ─────────────────────────────────────────────────────────────
    df_htf = _resample(df_pool, htf_min)
    print(f"\n[2] HTF {htf_min}m bars : {len(df_htf)} bars  "
          f"[{_hhmm(df_htf['datetime'].iloc[0])} → {_hhmm(df_htf['datetime'].iloc[-1])}]")

    _, htf_entries = scan_htf(df_htf)

    # Filter to zones relevant to today
    today_start = pd.Timestamp(dt + " 00:00:00")
    today_end   = pd.Timestamp(dt + " 23:59:59")

    htf_zones = [e for e in htf_entries
                 if e["status"] in ("TRAPPED", "CLOSED", "ACTIVE")]

    # Separate: zones that CLOSED before today (pool zones) vs zones active today
    pool_zones   = [e for e in htf_zones
                    if e.get("closed_on") is None or pd.Timestamp(str(e["closed_on"])) < today_start]
    active_zones = [e for e in htf_zones
                    if e.get("trapped_on") is not None
                    and pd.Timestamp(str(e["trapped_on"])) >= today_start]

    if not htf_entries:
        print(f"  No HTF zones found at all.")
    else:
        print(f"  Total HTF entries scanned : {len(htf_entries)}")
        print(f"  TRAPPED/CLOSED zones      : {len(htf_zones)}")

    # ── Print all HTF zones with LTF inside ───────────────────────────────────
    print(f"\n{'─'*70}")
    print(f"  ZONE TREE  (HTF {htf_min}m → LTF 5m)")
    print(f"{'─'*70}")

    if not htf_zones:
        print("  No TRAPPED or CLOSED HTF zones found.")
    else:
        for i, e in enumerate(htf_zones):
            status     = e["status"]
            trapped_at = _hhmm(e.get("trapped_on"))
            closed_at  = _hhmm(e.get("closed_on"))
            status_str = f"ACTIVE" if status == "ACTIVE" else \
                         f"TRAPPED at {trapped_at}" if status == "TRAPPED" else \
                         f"CLOSED at {closed_at}"

            # Colour markers
            marker = "🟡" if status == "ACTIVE" else \
                     "🟠" if status == "TRAPPED" else "🟢"

            print(f"\n  {marker} [HTF {htf_min}m] Zone #{i+1}")
            print(f"     zone_high  = {e['zone_high']:.2f}   ← bears shorted here (your entry level)")
            print(f"     zone_low   = {e['zone_low']:.2f}   ← bottom of zone")
            print(f"     zone_trig  = {e['zone_trigger']:.2f}   ← HTF entry trigger (lower 1/3)")
            print(f"     sl_level   = {e['sl']:.2f}   ← bears' SL = your T1")
            print(f"     ref_bar    = {_hhmm(e['ref_ts'])}")
            print(f"     status     = {status_str}")

            # ── LTF 5m scan inside this HTF zone ──────────────────────────────
            # Use today's bars only for LTF
            df5m = _resample(df1m_today, 5)

            # Filter to bars that started AFTER this HTF zone was formed
            if e.get("trapped_on") is not None:
                trap_ts = pd.Timestamp(str(e["trapped_on"]))
                df5m_after = df5m[df5m["datetime"] >= trap_ts].copy().reset_index(drop=True)
            else:
                df5m_after = df5m.copy()

            _, ltf_entries = scan_ltf(
                df5m_after,
                htf_zone_high=e["zone_high"],
                htf_zone_low=e["zone_low"],
                htf_ref_bar=str(e.get("ref_ts", "")),
                htf_trap_bar=str(e.get("trapped_on", "")),
                htf_target=e["sl"],
            )

            ltf_closed = [l for l in ltf_entries if l["status"] == "CLOSED"]
            ltf_trapped = [l for l in ltf_entries if l["status"] == "TRAPPED"]
            ltf_active  = [l for l in ltf_entries if l["status"] == "ACTIVE"]

            if not ltf_entries:
                print(f"     └─ [LTF 5m] No 5m zones found inside this HTF zone")
            else:
                print(f"     └─ [LTF 5m] {len(ltf_entries)} zones found  "
                      f"({len(ltf_closed)} CLOSED, {len(ltf_trapped)} TRAPPED, {len(ltf_active)} ACTIVE)")

                for j, l in enumerate(ltf_entries):
                    l_status = l["status"]
                    l_marker = "🟡" if l_status == "ACTIVE" else \
                               "🟠" if l_status == "TRAPPED" else "✅"
                    l_trapped = _hhmm(l.get("trapped_on"))
                    l_closed  = _hhmm(l.get("closed_on"))
                    l_status_str = f"ACTIVE" if l_status == "ACTIVE" else \
                                   f"TRAPPED at {l_trapped}" if l_status == "TRAPPED" else \
                                   f"CLOSED at {l_closed}"

                    print(f"        {l_marker} [LTF 5m] Zone #{j+1}")
                    print(f"           zone_high = {l['zone_high']:.2f}   ← LTF sellers' entry")
                    print(f"           zone_low  = {l['zone_low']:.2f}   ← LTF zone bottom")
                    print(f"           zone_trig = {l['zone_trigger']:.2f}   ← LTF entry trigger")
                    print(f"           sl_level  = {l['sl']:.2f}   ← LTF T1 target")
                    print(f"           ref_bar   = {_hhmm(l.get('ref_ts'))}")
                    print(f"           status    = {l_status_str}")

                    if l_status == "CLOSED":
                        entry_px = round(l["zone_trigger"], 2)
                        sl_px    = round(l["zone_low"] - 2.0, 2)
                        t1_px    = round(l["sl"], 2)
                        rr       = round((t1_px - entry_px) / max(entry_px - sl_px, 0.01), 2)
                        print(f"")
                        print(f"           ⚡ ENTRY SIGNAL")
                        print(f"              Entry  = {entry_px}  (zone_trigger)")
                        print(f"              SL     = {sl_px}  (zone_low − 2pts)")
                        print(f"              T1     = {t1_px}  (LTF sl_level)")
                        print(f"              R:R    = {rr}")
                        print(f"              HTF T1 = {e['sl']:.2f}  (HTF sl_level = bears' SL)")

    print(f"\n{'='*70}\n")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="TrapScanner zone diagnostic viewer")
    ap.add_argument("--token", required=True,  help="Upstox access token")
    ap.add_argument("--date",  required=True,  help="Date YYYY-MM-DD")
    ap.add_argument("--key",   required=True,  help="Instrument key e.g. NSE_FO|57202")
    ap.add_argument("--side",  default="CE",   help="CE or PE")
    ap.add_argument("--htf",   default=75, type=int, help="HTF minutes (default 75)")
    args = ap.parse_args()

    show_zones(
        token   = args.token,
        dt      = args.date,
        key     = args.key,
        side    = args.side,
        htf_min = args.htf,
    )
