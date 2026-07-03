#!/usr/bin/env python3
"""
FnO Backtest Parameter Optimizer
----------------------------------
Fetches 10-stock D1 data once, then sweeps 4 parameters:
  - MIN_ZONE_PCT : minimum zone width as % of price
  - MIN_RR       : minimum reward:risk ratio
  - MAX_AGE_DAYS : max days since trap fired
  - PROX_PCT     : max % from price to zone boundary

Scores each combo by: win_rate * log(1 + n_closed_trades)
  - win_rate alone ignores quantity (100% on 1 trade is useless)
  - trade count alone ignores quality (50% on 100 trades = gambling)
  - composite score rewards BOTH

Extrapolates daily signal count to 200 FnO stocks.

Run: python3 scripts/fno_optimize.py
"""
import os, sys, sqlite3, time, math, itertools
from datetime import date, timedelta

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, '.')

import requests
import pandas as pd
from strategies.trap_scanner import scanner

# ── Stocks ────────────────────────────────────────────────────────────────────
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

# ── Parameter grid to sweep ───────────────────────────────────────────────────
PARAM_GRID = {
    "MIN_ZONE_PCT":  [0.3, 0.5, 0.7, 1.0, 1.3, 1.5],
    "MIN_RR":        [1.5, 1.8, 2.0, 2.5, 3.0],
    "MAX_AGE_DAYS":  [10, 15, 20, 30],
    "PROX_PCT":      [3.0, 5.0, 7.0],
}

# Fixed params (not swept)
SL_BUF_PCT   = 0.2
MAX_FWD      = 15
BACKTEST_DAYS = 25   # ~1 calendar month of trading days


# ── Token ─────────────────────────────────────────────────────────────────────
def get_token():
    conn = sqlite3.connect('data/clients.db')
    row = conn.execute(
        "SELECT access_token FROM system_feeder_creds WHERE provider='upstox' LIMIT 1"
    ).fetchone()
    conn.close()
    return row[0] if row else ""


# ── Fetch bars ────────────────────────────────────────────────────────────────
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
        print(f"    HTTP {r.status_code} for {key}")
        return pd.DataFrame()
    candles = r.json().get("data", {}).get("candles", [])
    rows = [{"datetime": c[0][:10], "open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4])} for c in reversed(candles)]
    return pd.DataFrame(rows)


# ── Pre-compute all zones per day per stock ───────────────────────────────────
def precompute_zones(df):
    """
    For each simulation day i, pre-run scanner once and cache zones.
    This avoids re-running scanner.scan_htf_spot() for every param combo.
    Returns list of (date, close, zones) tuples.
    """
    results = []
    sim_start = max(10, len(df) - BACKTEST_DAYS)
    for i in range(sim_start, len(df)):
        scan_bars = df.iloc[:i]
        today_bar = df.iloc[i]
        scan_date = today_bar["datetime"]
        close     = today_bar["close"]
        _, zones  = scanner.scan_htf_spot(scan_bars)
        results.append((scan_date, close, zones))
    return results


# ── Build entry/sl/t1 ─────────────────────────────────────────────────────────
def build_levels(zl, zh, zone, direction):
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


# ── Simulate forward ──────────────────────────────────────────────────────────
def simulate(direction, entry, sl, t1, fwd_rows):
    entered = False
    for hi, lo, close in fwd_rows:
        if not entered:
            if direction == "CE" and lo <= entry:
                entered = True
            elif direction == "PE" and hi >= entry:
                entered = True
            else:
                continue
        if direction == "CE":
            if lo <= sl:  return "LOSS"
            if hi >= t1:  return "WIN"
        else:
            if hi >= sl:  return "LOSS"
            if lo <= t1:  return "WIN"
    return "NO-ENTRY" if not entered else "OPEN"


# ── Run backtest for ONE stock with given params ──────────────────────────────
def run_stock(cached_days, df, params):
    MIN_ZONE_PCT = params["MIN_ZONE_PCT"]
    MIN_RR       = params["MIN_RR"]
    MAX_AGE_DAYS = params["MAX_AGE_DAYS"]
    PROX_PCT     = params["PROX_PCT"]

    wins = losses = no_entry = 0
    seen = set()

    for idx, (scan_date, close, all_zones) in enumerate(cached_days):
        for direction in ["CE", "PE"]:
            kind    = "BEAR" if direction == "CE" else "BULL"
            trapped = [z for z in all_zones
                       if z.get("kind") == kind and z.get("status") == "TRAPPED"]
            for z in trapped:
                zh         = z["zone_high"]
                zl         = z["zone_low"]
                trapped_on = str(z.get("trapped_on", "") or "")[:10]
                if not trapped_on: continue

                zone_key = (direction, trapped_on, round(zl, 0))
                if zone_key in seen: continue

                try:
                    age = (date.fromisoformat(scan_date) - date.fromisoformat(trapped_on)).days
                except Exception:
                    continue
                if age > MAX_AGE_DAYS: continue
                if (zh - zl) / close * 100 < MIN_ZONE_PCT: continue

                # Directional proximity
                in_zone = (zl <= close <= zh)
                if direction == "CE":
                    if close < zl: continue
                    prox_ok = in_zone or ((close - zh) / zh * 100 <= PROX_PCT)
                else:
                    if close > zh: continue
                    prox_ok = in_zone or ((zl - close) / zl * 100 <= PROX_PCT)
                if not prox_ok: continue

                entry, sl, t1, rr = build_levels(zl, zh, z, direction)
                if rr < MIN_RR: continue

                # Forward bars (include signal day itself)
                sim_start_global = max(10, len(df) - BACKTEST_DAYS)
                i = sim_start_global + idx
                fwd_raw  = df.iloc[i: i + MAX_FWD]
                fwd_rows = [(row["high"], row["low"], row["close"])
                            for _, row in fwd_raw.iterrows()]

                outcome = simulate(direction, entry, sl, t1, fwd_rows)
                seen.add(zone_key)

                if outcome == "WIN":    wins += 1
                elif outcome == "LOSS": losses += 1
                else:                   no_entry += 1
                break  # one zone per direction per day

    return wins, losses, no_entry


# ── Score function ────────────────────────────────────────────────────────────
def score(wins, losses):
    closed = wins + losses
    if closed < 3:
        return 0.0   # not enough trades to be meaningful
    wr = wins / closed
    # Composite: win_rate * log(1 + closed_trades)
    # Rewards both high win rate AND enough trade volume
    return round(wr * math.log(1 + closed), 4)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    token = get_token()
    if not token:
        print("ERROR: No Upstox token in DB"); return

    print("=" * 70)
    print("  FnO Optimizer — fetching D1 data for all stocks...")
    print("=" * 70)

    # Step 1: Fetch all bars + pre-compute zones (done ONCE, reused per combo)
    stock_data = []
    for sym, key in STOCKS:
        print(f"  Fetching {sym}...", end="", flush=True)
        df = fetch_bars(key, token, days=90)
        if df.empty or len(df) < 15:
            print(f" skip (only {len(df)} bars)")
            continue
        print(f" {len(df)} bars", end="", flush=True)
        cached = precompute_zones(df)
        stock_data.append((sym, df, cached))
        print(f" → {len(cached)} scan days cached")

    print(f"\n  {len(stock_data)} stocks loaded. Pre-computing zones done.\n")

    # Step 2: Build all parameter combinations
    keys   = list(PARAM_GRID.keys())
    values = list(PARAM_GRID.values())
    combos = list(itertools.product(*values))
    total  = len(combos)
    print(f"  Sweeping {total} parameter combinations...\n")

    results = []
    for ci, combo in enumerate(combos):
        params = dict(zip(keys, combo))
        total_wins = total_losses = total_no_entry = 0

        for sym, df, cached in stock_data:
            w, l, ne = run_stock(cached, df, params)
            total_wins    += w
            total_losses  += l
            total_no_entry += ne

        sc = score(total_wins, total_losses)
        closed = total_wins + total_losses
        wr = total_wins / closed * 100 if closed > 0 else 0
        results.append({
            "params":   params,
            "wins":     total_wins,
            "losses":   total_losses,
            "no_entry": total_no_entry,
            "closed":   closed,
            "wr":       round(wr, 1),
            "score":    sc,
        })

        if (ci + 1) % 60 == 0 or ci + 1 == total:
            print(f"  [{ci+1}/{total}] done...", flush=True)

    # Step 3: Sort and print top results
    results.sort(key=lambda x: x["score"], reverse=True)
    top = results[:25]

    # Extrapolation: 10 stocks over 25 trading days → per-day rate → scale to 200
    trading_days = 25

    print(f"\n{'='*90}")
    print(f"  TOP 25 PARAMETER COMBINATIONS  (score = win_rate × log(1+closed_trades))")
    print(f"{'='*90}")
    print(f"  {'#':<3} {'W%':>5} {'W':>4} {'L':>4} {'NE':>4} {'Score':>7} │ "
          f"{'ZoneW%':>7} {'MinRR':>6} {'Age':>5} {'Prox%':>6} │ "
          f"{'Sig/day×200':>12}")
    print(f"  {'-'*90}")

    for rank, r in enumerate(top, 1):
        p = r["params"]
        # signals per trading day across 200 stocks (scale from 10 stocks)
        total_signals = r["wins"] + r["losses"] + r["no_entry"]
        sig_per_day_200 = round(total_signals / trading_days * (200 / len(stock_data)), 1)
        print(f"  {rank:<3} {r['wr']:>4.0f}% {r['wins']:>4} {r['losses']:>4} {r['no_entry']:>4} {r['score']:>7.4f} │ "
              f"{p['MIN_ZONE_PCT']:>6.1f}% {p['MIN_RR']:>6.1f}x {p['MAX_AGE_DAYS']:>5}d {p['PROX_PCT']:>5.1f}% │ "
              f"{sig_per_day_200:>12.0f}")

    # Also show: highest win rate combos with >= 10 closed trades
    print(f"\n{'='*90}")
    print(f"  HIGHEST WIN RATE (min 10 closed trades)")
    print(f"{'='*90}")
    high_wr = [r for r in results if r["closed"] >= 10]
    high_wr.sort(key=lambda x: x["wr"], reverse=True)
    print(f"  {'#':<3} {'W%':>5} {'W':>4} {'L':>4} {'Score':>7} │ "
          f"{'ZoneW%':>7} {'MinRR':>6} {'Age':>5} {'Prox%':>6} │ "
          f"{'Sig/day×200':>12}")
    print(f"  {'-'*90}")
    for rank, r in enumerate(high_wr[:15], 1):
        p = r["params"]
        total_signals = r["wins"] + r["losses"] + r["no_entry"]
        sig_per_day_200 = round(total_signals / trading_days * (200 / len(stock_data)), 1)
        print(f"  {rank:<3} {r['wr']:>4.0f}% {r['wins']:>4} {r['losses']:>4} {r['score']:>7.4f} │ "
              f"{p['MIN_ZONE_PCT']:>6.1f}% {p['MIN_RR']:>6.1f}x {p['MAX_AGE_DAYS']:>5}d {p['PROX_PCT']:>5.1f}% │ "
              f"{sig_per_day_200:>12.0f}")

    # Best combo recommendation
    best = top[0]
    p = best["params"]
    print(f"\n{'='*90}")
    print(f"  RECOMMENDED PARAMETERS (best composite score)")
    print(f"{'='*90}")
    print(f"  MIN_ZONE_PCT  = {p['MIN_ZONE_PCT']}   (zone must be >= {p['MIN_ZONE_PCT']}% of stock price)")
    print(f"  MIN_RR        = {p['MIN_RR']}   (min reward:risk ratio)")
    print(f"  MAX_AGE_DAYS  = {p['MAX_AGE_DAYS']}   (zone must have trapped within {p['MAX_AGE_DAYS']} days)")
    print(f"  PROX_PCT      = {p['PROX_PCT']}   (price must be within {p['PROX_PCT']}% of zone)")
    print(f"  Win rate      = {best['wr']}%  ({best['wins']}W / {best['losses']}L on 10 stocks / 1 month)")
    total_signals = best["wins"] + best["losses"] + best["no_entry"]
    sig_per_day_200 = round(total_signals / trading_days * (200 / len(stock_data)), 1)
    print(f"  Signals/day × 200 stocks ≈ {sig_per_day_200}")
    print(f"  Score         = {best['score']}")


if __name__ == "__main__":
    main()
