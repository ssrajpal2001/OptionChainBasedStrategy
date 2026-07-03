#!/usr/bin/env python3
"""
FnO Stock Scanner — 1 Month Backtest with 20-DMA Trend Filter
--------------------------------------------------------------
Only take CE when stock close > 20DMA (uptrend)
Only take PE when stock close < 20DMA (downtrend)

Shows per-trade timeline: ref candle → trap → entry → SL/T1 → result
Plus active waiting zones (DMA-aligned only)

Run: python3 scripts/fno_backtest_dma.py
"""
import os, sys, sqlite3, time
from datetime import date, timedelta

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, '.')

import requests
import pandas as pd
from strategies.trap_scanner import scanner

# ── Config ────────────────────────────────────────────────────────────────────
STOCKS = [
    ("RELIANCE",    "NSE_EQ|INE002A01018"),
    ("HDFCBANK",    "NSE_EQ|INE040A01034"),
    ("SBIN",        "NSE_EQ|INE062A01020"),
    ("AXISBANK",    "NSE_EQ|INE238A01034"),
    ("TCS",         "NSE_EQ|INE467B01029"),
    ("INFY",        "NSE_EQ|INE009A01021"),
    ("WIPRO",       "NSE_EQ|INE075A01022"),
    ("BHARTIARTL",  "NSE_EQ|INE397D01024"),
    ("ITC",         "NSE_EQ|INE154A01025"),
    ("TATAMOTORS",  "NSE_EQ|INE155L01010"),
]

PROX_PCT     = 5.0   # % proximity to zone boundary
MIN_RR       = 1.5   # minimum reward:risk
SL_BUF_PCT   = 0.2   # SL buffer beyond zone edge
MAX_AGE_DAYS = 30    # max zone age (days)
MAX_FWD      = 15    # max forward bars to simulate
DMA          = 20    # trend filter: 20-day moving average


# ── Token ─────────────────────────────────────────────────────────────────────
def get_token():
    conn = sqlite3.connect('data/clients.db')
    row = conn.execute(
        "SELECT access_token FROM system_feeder_creds WHERE provider='upstox' LIMIT 1"
    ).fetchone()
    conn.close()
    return row[0] if row else ""


# ── Bars ──────────────────────────────────────────────────────────────────────
def fetch_bars(key, token, days=90):
    from urllib.parse import quote as q
    to_dt = date.today()
    fr_dt = to_dt - timedelta(days=days)
    enc   = q(key, safe="")
    url   = f"https://api.upstox.com/v2/historical-candle/{enc}/day/{to_dt}/{fr_dt}"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}",
                                   "Accept": "application/json"}, timeout=15)
    time.sleep(0.4)
    if r.status_code != 200:
        return pd.DataFrame()
    candles = r.json().get("data", {}).get("candles", [])
    rows = [{"datetime": c[0][:10], "open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4])} for c in reversed(candles)]
    return pd.DataFrame(rows)


# ── Zone helpers ──────────────────────────────────────────────────────────────
def approaching(close, zl, zh, direction):
    if zl <= close <= zh:
        return True
    if direction == "CE":
        if close < zl:
            return False
        return (close - zh) / zh * 100 <= PROX_PCT
    else:
        if close > zh:
            return False
        return (zl - close) / zl * 100 <= PROX_PCT


def build_levels(close, zl, zh, zone, direction):
    if direction == "CE":
        entry  = zh
        sl     = round(zl * (1 - SL_BUF_PCT / 100), 2)
        t1     = round(zone.get("sl", zh * 1.03), 2)
        risk   = entry - sl
        reward = t1 - entry
    else:
        entry  = zl
        sl     = round(zh * (1 + SL_BUF_PCT / 100), 2)
        t1     = round(zone.get("sl", zl * 0.97), 2)
        risk   = sl - entry
        reward = entry - t1
    rr = round(reward / risk, 2) if risk > 0 else 0.0
    return entry, sl, t1, rr


# ── Forward simulation ────────────────────────────────────────────────────────
def simulate(direction, entry, sl, t1, fwd):
    """
    Simulate entry at zone boundary across forward bars.
    Includes signal-day itself in fwd (1-day-lag fix).
    """
    entered    = False
    entry_date = None

    for b in fwd:
        d, hi, lo = b["datetime"], b["high"], b["low"]
        if not entered:
            if direction == "CE" and lo <= entry:
                entered = True; entry_date = d
            elif direction == "PE" and hi >= entry:
                entered = True; entry_date = d
            else:
                continue
        if direction == "CE":
            if lo <= sl:  return "LOSS", sl, d, entry_date
            if hi >= t1:  return "WIN",  t1, d, entry_date
        else:
            if hi >= sl:  return "LOSS", sl, d, entry_date
            if lo <= t1:  return "WIN",  t1, d, entry_date

    if not entered:
        return "NO-ENTRY", entry, fwd[-1]["datetime"] if fwd else "?", None
    last = fwd[-1]
    return "OPEN", last["close"], last["datetime"], entry_date


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    token = get_token()
    if not token:
        print("ERROR: No Upstox token in DB"); return
    print(f"Token loaded: {token[:20]}...\n")
    print(f"Filter: CE only when price > 20DMA (uptrend) | PE only when price < 20DMA (downtrend)\n")

    all_trades  = []
    all_waiting = []
    skipped_dma = 0

    for sym, key in STOCKS:
        print(f"\n{'='*72}")
        print(f"  {sym}")
        print(f"{'='*72}")

        df = fetch_bars(key, token, days=90)
        if df.empty or len(df) < DMA + 5:
            print(f"  Not enough bars"); continue

        df["dma20"] = df["close"].rolling(DMA).mean()
        print(f"  {len(df)} D1 bars  ({df.iloc[0]['datetime']} → {df.iloc[-1]['datetime']})")

        sim_start    = max(DMA, len(df) - 25)
        seen_zones   = set()
        stock_trades = []
        stock_waiting = []

        for i in range(sim_start, len(df)):
            scan_bars = df.iloc[:i]
            today_bar = df.iloc[i]
            scan_date = today_bar["datetime"]
            close     = today_bar["close"]
            dma_val   = today_bar["dma20"]
            if pd.isna(dma_val):
                continue
            above_dma = (close > dma_val)
            below_dma = (close < dma_val)

            _, all_zones = scanner.scan_htf_spot(scan_bars)

            for direction in ["CE", "PE"]:
                # ── 20 DMA TREND FILTER ───────────────────────────────
                if direction == "CE" and not above_dma:
                    skipped_dma += 1; continue
                if direction == "PE" and not below_dma:
                    skipped_dma += 1; continue
                # ─────────────────────────────────────────────────────

                kind    = "BEAR" if direction == "CE" else "BULL"
                trapped = [z for z in all_zones
                           if z.get("kind") == kind and z.get("status") == "TRAPPED"]

                for z in trapped:
                    zh         = z["zone_high"]
                    zl         = z["zone_low"]
                    trapped_on = str(z.get("trapped_on", "") or "")[:10]
                    ref_ts     = str(z.get("ref_ts",     "") or "")[:10]
                    if not trapped_on:
                        continue

                    zone_key = (direction, trapped_on, round(zl, 0))
                    if zone_key in seen_zones:
                        continue

                    try:
                        age = (date.fromisoformat(scan_date) - date.fromisoformat(trapped_on)).days
                    except Exception:
                        continue
                    if age > MAX_AGE_DAYS:
                        continue
                    if (zh - zl) / close < 0.003:
                        continue
                    if not approaching(close, zl, zh, direction):
                        continue

                    entry, sl, t1, rr = build_levels(close, zl, zh, z, direction)
                    if rr < MIN_RR:
                        continue

                    # Include signal day itself (1-day-lag fix)
                    fwd_raw = df.iloc[i: i + MAX_FWD]
                    fwd     = [{"datetime": b["datetime"], "high": b["high"],
                                "low": b["low"], "close": b["close"]}
                               for _, b in fwd_raw.iterrows()]

                    outcome, exit_px, exit_dt, entry_dt = simulate(direction, entry, sl, t1, fwd)
                    seen_zones.add(zone_key)

                    rec = {
                        "sym":        sym,
                        "direction":  direction,
                        "ref_date":   ref_ts,
                        "trap_date":  trapped_on,
                        "signal_date": scan_date,
                        "zone_low":   round(zl, 2),
                        "zone_high":  round(zh, 2),
                        "entry":      entry,
                        "sl":         sl,
                        "t1":         t1,
                        "rr":         rr,
                        "outcome":    outcome,
                        "exit_px":    round(exit_px, 2),
                        "exit_dt":    exit_dt,
                        "entry_dt":   entry_dt,
                        "close_sig":  round(close, 2),
                        "dma":        round(dma_val, 2),
                    }
                    stock_trades.append(rec)
                    all_trades.append(rec)
                    break

        # ── Active waiting zones (DMA-filtered) ───────────────────────
        last_bar   = df.iloc[-1]
        last_close = float(last_bar["close"])
        last_dma   = float(last_bar["dma20"]) if not pd.isna(last_bar["dma20"]) else 0
        last_date  = last_bar["datetime"]
        _, all_zones_now = scanner.scan_htf_spot(df)

        for direction in ["CE", "PE"]:
            if direction == "CE" and last_close <= last_dma: continue
            if direction == "PE" and last_close >= last_dma: continue
            kind    = "BEAR" if direction == "CE" else "BULL"
            trapped = [z for z in all_zones_now
                       if z.get("kind") == kind and z.get("status") == "TRAPPED"]
            for z in trapped:
                zh         = z["zone_high"]
                zl         = z["zone_low"]
                trapped_on = str(z.get("trapped_on", "") or "")[:10]
                if not trapped_on: continue
                try:
                    age = (date.fromisoformat(last_date) - date.fromisoformat(trapped_on)).days
                except Exception:
                    continue
                if age > MAX_AGE_DAYS: continue
                if (zh - zl) / last_close < 0.003: continue

                if direction == "CE":
                    if last_close < zl: continue
                    dist_pct = round((last_close - zh) / zh * 100, 1) if last_close > zh else 0.0
                else:
                    if last_close > zh: continue
                    dist_pct = round((zl - last_close) / zl * 100, 1) if last_close < zl else 0.0

                if dist_pct > PROX_PCT: continue
                entry, sl, t1, rr = build_levels(last_close, zl, zh, z, direction)
                if rr < MIN_RR: continue

                w = {
                    "sym":       sym,
                    "direction": direction,
                    "trap_date": trapped_on,
                    "zone_low":  round(zl, 2),
                    "zone_high": round(zh, 2),
                    "entry":     entry,
                    "sl":        sl,
                    "t1":        t1,
                    "rr":        rr,
                    "dist_pct":  dist_pct,
                    "current":   round(last_close, 2),
                    "dma":       round(last_dma, 2),
                }
                stock_waiting.append(w)
                all_waiting.append(w)

        # ── Print per-stock trades ─────────────────────────────────────
        if not stock_trades:
            print("  No qualifying signals (after 20DMA filter)")
        else:
            wins   = [t for t in stock_trades if t["outcome"] == "WIN"]
            losses = [t for t in stock_trades if t["outcome"] == "LOSS"]
            no_e   = [t for t in stock_trades if t["outcome"] == "NO-ENTRY"]
            print(f"\n  TRADES (W={len(wins)} L={len(losses)} NO-ENTRY={len(no_e)})  [20DMA filter ON]")
            hdr = f"  {'#':<3} {'Dir':<4} {'Ref':<10} {'Trap':<10} {'Entry Dt':<10} {'Entry':>8} {'SL':>8} {'T1':>8} {'R:R':>5} {'20DMA':>8} {'Result':<10} {'Exit Dt':<10} {'P&L':>8}"
            print(hdr)
            print(f"  {'-'*len(hdr)}")
            for n, t in enumerate(stock_trades, 1):
                pnl     = round(t["entry"] - t["exit_px"], 2) if t["direction"] == "PE" else round(t["exit_px"] - t["entry"], 2)
                pnl_str = f"{pnl:+.2f}" if t["outcome"] in ("WIN", "LOSS") else "-"
                edt     = t["entry_dt"] or "no-touch"
                res     = {"WIN": "WIN ✓", "LOSS": "LOSS ✗", "NO-ENTRY": "NO-ENTRY", "OPEN": "OPEN ~"}.get(t["outcome"], t["outcome"])
                arrow   = "↑" if t["direction"] == "CE" else "↓"
                print(f"  {n:<3} {t['direction']:<4} {t['ref_date']:<10} {t['trap_date']:<10} {edt:<10} {t['entry']:>8.2f} {t['sl']:>8.2f} {t['t1']:>8.2f} {t['rr']:>5.1f}x {t['dma']:>7.2f}{arrow} {res:<10} {t['exit_dt'] or '-':<10} {pnl_str:>8}")

        if stock_waiting:
            print(f"\n  WAITING ZONES (20DMA aligned — price not there yet)")
            print(f"  {'Dir':<4} {'Zone':<18} {'Entry':>8} {'SL':>8} {'T1':>8} {'R:R':>5} {'Dist%':>6} {'20DMA':>9}  Action")
            print(f"  {'-'*92}")
            for w in sorted(stock_waiting, key=lambda x: x["dist_pct"]):
                zone_str = f"{w['zone_low']:.1f}-{w['zone_high']:.1f}"
                action   = "Rally→sell PE" if w["direction"] == "PE" else "Pullback→buy CE"
                print(f"  {w['direction']:<4} {zone_str:<18} {w['entry']:>8.2f} {w['sl']:>8.2f} {w['t1']:>8.2f} {w['rr']:>5.1f}x {w['dist_pct']:>5.1f}% {w['dma']:>9.2f}  {action}")

    # ── Grand summary ─────────────────────────────────────────────────────────
    wins   = [t for t in all_trades if t["outcome"] == "WIN"]
    losses = [t for t in all_trades if t["outcome"] == "LOSS"]
    no_e   = [t for t in all_trades if t["outcome"] == "NO-ENTRY"]
    closed = len(wins) + len(losses)

    print(f"\n{'='*72}")
    print(f"  GRAND SUMMARY — 20DMA filter applied")
    print(f"  Total signals : {len(all_trades)}  ({skipped_dma} skipped by DMA filter, was 78 without)")
    print(f"  WIN={len(wins)}  LOSS={len(losses)}  NO-ENTRY={len(no_e)}")
    if closed:
        print(f"  Win rate      : {len(wins)/closed*100:.0f}%  ({len(wins)}/{closed})  [was 51% without filter]")

    print(f"\n  ALL ACTIVE WAITING ZONES — 20DMA ALIGNED (nearest first)")
    print(f"  {'Stock':<12} {'Dir':<4} {'Zone':<20} {'Entry':>8} {'SL':>8} {'T1':>8} {'R:R':>5} {'Dist%':>6} {'20DMA':>9}  Trap Date   Action")
    print(f"  {'-'*110}")
    for w in sorted(all_waiting, key=lambda x: x["dist_pct"]):
        zone_str = f"{w['zone_low']:.1f}-{w['zone_high']:.1f}"
        action   = "Rally→PE" if w["direction"] == "PE" else "Pullback→CE"
        print(f"  {w['sym']:<12} {w['direction']:<4} {zone_str:<20} {w['entry']:>8.2f} {w['sl']:>8.2f} {w['t1']:>8.2f} {w['rr']:>5.1f}x {w['dist_pct']:>5.1f}% {w['dma']:>9.2f}  {w['trap_date']}  {action}")

    print(f"\n  Interpretation:")
    print(f"  CE zone = bears trapped below → price expected UP  → buy CE option")
    print(f"  PE zone = bulls trapped above → price expected DOWN → buy PE option")
    print(f"  20DMA filter: CE only in uptrend (close>DMA), PE only in downtrend (close<DMA)")


if __name__ == "__main__":
    main()
