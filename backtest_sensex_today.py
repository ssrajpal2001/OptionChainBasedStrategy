"""
Backtest TrapScanner on SENSEX for today (Jun 17, 2026).

Keys confirmed from check_sensex_expiry.py:
  Expiry: 2026-06-18
  CE1=76800 → BSE_FO|1145316
  CE2=76500 → BSE_FO|1148517
  PE1=77400 → BSE_FO|1140561
  PE2=77700 → BSE_FO|1142448

Gap UP 1.1% → CE1=76800 (near ITM call), PE1=77400 (near ITM put)
HTF=75min on CE1 bars for BEAR zones, PE1 bars for BULL zones
LTF=5min entry inside TRAPPED HTF zone
2 lots × 20 = 40 qty
T1 = 50% qty at HTF zone target
Trail = 5-min ratchet on remaining qty
"""
import sys, os, asyncio, json
sys.path.insert(0, os.getcwd())

import aiohttp
import pandas as pd
from datetime import date, timedelta
from strategies.trap_scanner import scanner

TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI3NkFFNDciLCJqdGkiOiI2YTMyMTk4ZTRmNWFmZDdkOTUyM2YxMzgiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzgxNjY4MjM4LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3ODE3MzM2MDB9.Ypfp_KNM6CZoZyOf3MRASdrGdOHEJJioAGYqunIAVYY"

KEYS = {
    "CE1": "BSE_FO|1145316",   # 76800 CE
    "CE2": "BSE_FO|1148517",   # 76500 CE
    "PE1": "BSE_FO|1140561",   # 77400 PE
    "PE2": "BSE_FO|1142448",   # 77700 PE
}

LOT_SIZE   = 20
LOTS       = 2
QTY        = LOT_SIZE * LOTS   # 40
HTF_MIN    = 75
LTF_MIN    = 5
SL_BUF_PCT = 2.0


async def fetch_bars(key: str, days: int = 8) -> list:
    today   = date.today()
    to_date = today + timedelta(days=1)
    fr_date = today - timedelta(days=days)
    url = (f"https://api.upstox.com/v2/historical-candle/"
           f"{key}/1minute/{to_date}/{fr_date}")
    headers = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}
    async with aiohttp.ClientSession() as s:
        async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status != 200:
                body = await r.text()
                print(f"  HTTP {r.status} for {key}: {body[:200]}")
                return []
            data = await r.json()
    candles = data.get("data", {}).get("candles", [])
    bars = [
        {"datetime": c[0], "open": float(c[1]), "high": float(c[2]),
         "low": float(c[3]), "close": float(c[4]), "volume": int(c[5])}
        for c in reversed(candles)
    ]
    print(f"  {key}: {len(bars)} bars  "
          f"({bars[0]['datetime'][:10] if bars else '—'} → {bars[-1]['datetime'][:10] if bars else '—'})")
    return bars


def to_df(bars):
    if not bars:
        return pd.DataFrame()
    df = pd.DataFrame(bars)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df


def resample_htf(df, minutes):
    if df.empty:
        return df
    dfc = df.set_index("datetime")
    htf = dfc.resample(f"{minutes}min").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return htf


def simulate_trade(entry_bar, zones, bars_5m_today, side, label):
    """Simulate entry + T1 + trail exit on 5-min bars after entry_bar."""
    if not zones:
        return None
    zone = zones[-1]
    t1_target  = zone.get("t1_target", 0)
    zone_high  = zone.get("zone_high", 0)
    zone_low   = zone.get("zone_low", 0)

    entry_time  = pd.to_datetime(entry_bar["datetime"])
    entry_price = entry_bar["close"]
    sl_price    = entry_price * (1 - SL_BUF_PCT / 100)

    qty_remaining = QTY
    qty_t1        = QTY // 2
    t1_done       = False
    trail_high    = entry_price
    trail_sl      = sl_price
    pnl           = 0.0
    exit_price    = None
    exit_time     = None
    exit_reason   = None

    future_bars = [b for b in bars_5m_today
                   if pd.to_datetime(b["datetime"]) > entry_time]

    print(f"\n  [{label}] ENTRY @ {entry_time.strftime('%H:%M')} price={entry_price:.1f} "
          f"SL={sl_price:.1f} T1_target={t1_target:.1f} zone=[{zone_low:.1f}–{zone_high:.1f}]")

    for bar in future_bars:
        bt = pd.to_datetime(bar["datetime"])
        hi = bar["high"]
        lo = bar["low"]
        cl = bar["close"]

        # T1: 50% at zone target
        if not t1_done and t1_target > 0 and hi >= t1_target:
            t1_pnl = (t1_target - entry_price) * qty_t1
            pnl += t1_pnl
            qty_remaining -= qty_t1
            t1_done = True
            trail_high = t1_target
            trail_sl   = max(trail_sl, entry_price)  # move SL to entry after T1
            print(f"    T1 @ {bt.strftime('%H:%M')} price={t1_target:.1f}  "
                  f"qty={qty_t1}  pnl_so_far={pnl:.0f}")

        # Trail: ratchet trail_sl up as price makes new highs
        if t1_done and hi > trail_high:
            trail_high = hi
            trail_sl   = max(trail_sl, trail_high - (trail_high - entry_price) * 0.5)

        # SL hit
        if lo <= (sl_price if not t1_done else trail_sl):
            hit_sl = sl_price if not t1_done else trail_sl
            sl_pnl = (hit_sl - entry_price) * qty_remaining
            pnl += sl_pnl
            exit_price  = hit_sl
            exit_time   = bt
            exit_reason = "SL"
            break

        # EOD 15:20
        if bt.hour > 15 or (bt.hour == 15 and bt.minute >= 20):
            eod_pnl = (cl - entry_price) * qty_remaining
            pnl += eod_pnl
            exit_price  = cl
            exit_time   = bt
            exit_reason = "EOD"
            break

    if exit_time is None:
        exit_reason = "OPEN"

    print(f"    EXIT @ {exit_time.strftime('%H:%M') if exit_time else '—'}  "
          f"reason={exit_reason}  exit_px={exit_price:.1f if exit_price else 0:.1f}  "
          f"NET P&L = ₹{pnl:.0f}")
    return {"label": label, "entry": entry_price, "entry_time": str(entry_time),
            "exit": exit_price, "exit_time": str(exit_time), "reason": exit_reason, "pnl": pnl}


async def main():
    print("=" * 60)
    print("SENSEX TrapScanner Backtest — 17 Jun 2026")
    print(f"Lots={LOTS}  Qty={QTY}  HTF={HTF_MIN}m  LTF={LTF_MIN}m")
    print("=" * 60)

    print("\nFetching bars...")
    ce1_bars = await fetch_bars(KEYS["CE1"])
    pe1_bars = await fetch_bars(KEYS["PE1"])

    if not ce1_bars or not pe1_bars:
        print("No bars — check token / keys")
        return

    today = date.today().isoformat()

    # Split into seed (HTF) and today (LTF)
    ce1_all = to_df(ce1_bars)
    pe1_all = to_df(pe1_bars)

    ce1_today_bars = [b for b in ce1_bars if b["datetime"][:10] == today]
    pe1_today_bars = [b for b in pe1_bars if b["datetime"][:10] == today]
    print(f"\nToday bars: CE1={len(ce1_today_bars)}  PE1={len(pe1_today_bars)}")

    # ── HTF scan (75-min on ALL bars for historical zones) ──────────────────
    print("\n── HTF Scan (75-min) ──")
    htf_ce1 = resample_htf(ce1_all, HTF_MIN)
    htf_pe1 = resample_htf(pe1_all, HTF_MIN)
    print(f"HTF CE1 bars: {len(htf_ce1)}  HTF PE1 bars: {len(htf_pe1)}")

    _, bear_zones = scanner.scan_htf(htf_ce1) if len(htf_ce1) >= 2 else ([], [])
    _, bull_zones = scanner.scan_htf(htf_pe1) if len(htf_pe1) >= 2 else ([], [])

    trapped_bear = [z for z in bear_zones if z["status"] == "TRAPPED"]
    trapped_bull = [z for z in bull_zones if z["status"] == "TRAPPED"]
    print(f"Bear zones (TRAPPED): {len(trapped_bear)}")
    for z in trapped_bear:
        print(f"  zone [{z['zone_low']:.1f}–{z['zone_high']:.1f}]  trigger={z.get('zone_trigger',0):.1f}  t1={z.get('t1_target',0):.1f}")
    print(f"Bull zones (TRAPPED): {len(trapped_bull)}")
    for z in trapped_bull:
        print(f"  zone [{z['zone_low']:.1f}–{z['zone_high']:.1f}]  trigger={z.get('zone_trigger',0):.1f}  t1={z.get('t1_target',0):.1f}")

    # ── Cascade: 15-min scan on today's bars ────────────────────────────────
    print("\n── Cascade Scan (15-min on today) ──")
    ce1_today_df = to_df(ce1_today_bars)
    pe1_today_df = to_df(pe1_today_bars)
    htf15_ce1 = resample_htf(ce1_today_df, 15) if not ce1_today_df.empty else pd.DataFrame()
    htf15_pe1 = resample_htf(pe1_today_df, 15) if not pe1_today_df.empty else pd.DataFrame()
    print(f"15-min CE1 bars today: {len(htf15_ce1)}  PE1: {len(htf15_pe1)}")

    _, casc_bear = scanner.scan_htf(htf15_ce1) if len(htf15_ce1) >= 2 else ([], [])
    _, casc_bull = scanner.scan_htf(htf15_pe1) if len(htf15_pe1) >= 2 else ([], [])
    casc_bear_t = [z for z in casc_bear if z["status"] == "TRAPPED"]
    casc_bull_t = [z for z in casc_bull if z["status"] == "TRAPPED"]
    print(f"15-min bear TRAPPED: {len(casc_bear_t)}  bull TRAPPED: {len(casc_bull_t)}")

    # ── LTF scan (5-min on today's bars) ────────────────────────────────────
    print("\n── LTF Scan (5-min entries) ──")
    all_results = []

    for label, opt_bars_today, zones, side in [
        ("CE1-BEAR", ce1_today_bars, trapped_bear or casc_bear_t, "CE"),
        ("PE1-BULL", pe1_today_bars, trapped_bull or casc_bull_t, "PE"),
    ]:
        if not zones:
            print(f"  {label}: no zones → skip")
            continue
        df5 = resample_htf(to_df(opt_bars_today), LTF_MIN)
        if df5.empty or len(df5) < 3:
            print(f"  {label}: not enough 5-min bars ({len(df5)})")
            continue
        entries, _ = scanner.scan_ltf(df5, zones)
        print(f"  {label}: LTF entries found = {len(entries)}")
        for e in entries:
            bars_list = [b for b in df5.to_dict("records")]
            result = simulate_trade(e, zones, bars_list, side, label)
            if result:
                all_results.append(result)

    # ── Summary ─────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    if not all_results:
        print("No trades fired today.")
    else:
        total_pnl = sum(r["pnl"] for r in all_results)
        for r in all_results:
            print(f"  {r['label']:12s}  entry={r['entry']:.1f}  exit={r['exit'] or 0:.1f}  "
                  f"reason={r['reason']:4s}  P&L=₹{r['pnl']:,.0f}")
        print(f"\n  TOTAL NET P&L: ₹{total_pnl:,.0f}")
        print(f"  (Qty={QTY}, {LOTS} lots × {LOT_SIZE} lot_size)")


asyncio.run(main())
