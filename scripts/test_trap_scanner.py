"""
scripts/test_trap_scanner.py - Trap Scanner backtest on yesterday's data.

Instruments:
  NIFTY   - NSE spot 1m bars (for HTF scan) + CE/PE option bars would be used live;
             spot used here since we don't know which option strike was selected
  SENSEX  - same as NIFTY approach
  CRUDEOIL- near-month futures 1m bars (MCX_FO|499095 = CRUDEOIL26JUNFUT)

Exit logic (matches live_tracker.py exactly):
  T1 = 50% of qty at zone_high (bears SL = your target)
  Remaining 50%: SL trails 5m bar lows upward (ratchet, never down)
  Initial trail_sl = zone_low - sl_buffer
  After each 5m close: trail_sl = max(trail_sl, prev_5m_bar_low - sl_buffer)
  EOD: square off remaining at close

Run: python scripts/test_trap_scanner.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import io, sys
# Force UTF-8 output on Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import requests
import pandas as pd
from datetime import date, timedelta
from strategies.trap_scanner import scanner

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI0SkNIRDciLCJqdGkiOiI2YTMxOGFmYjMyNTdiYzE2ZTA1MTllNDciLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzgxNjMxNzM5LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3ODE2NDcyMDB9.C3eJij616XXpMbn9SWwiSknLGzg6j8jEmkxilTuN0R4"
HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Accept": "application/json"}

# Correct instrument keys
INSTRUMENTS = {
    "NIFTY":    {"key": "NSE_INDEX|Nifty 50", "lot": 75,  "step": 100, "gap_near": 200, "htf_src": "spot"},
    "SENSEX":   {"key": "BSE_INDEX|SENSEX",   "lot": 20,  "step": 100, "gap_near": 300, "htf_src": "spot"},
    "CRUDEOIL": {"key": "MCX_FO|499095",      "lot": 100, "step": 100, "gap_near": 200, "htf_src": "futures"},
}

HTF_MIN  = 75
SL_BUF   = 2.0


def fetch_1m(key: str, from_dt: str, to_dt: str) -> pd.DataFrame:
    from urllib.parse import quote
    enc = quote(key, safe="")
    url = f"https://api.upstox.com/v2/historical-candle/{enc}/1minute/{to_dt}/{from_dt}"
    r = requests.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    candles = r.json().get("data", {}).get("candles", [])
    if not candles:
        return pd.DataFrame()
    rows = [{"datetime": pd.to_datetime(c[0]), "open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4]), "volume": int(c[5])}
            for c in reversed(candles)]
    return pd.DataFrame(rows)


def resample_htf(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    dfc = df.set_index("datetime")
    htf = dfc.resample(f"{minutes}min").agg(
        {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}
    ).dropna().reset_index()
    return htf


def pivot_levels(H, L, C):
    P = (H+L+C)/3
    return {"P": P, "R1": 2*P-L, "R2": P+(H-L), "S1": 2*P-H, "S2": P-(H-L)}


def get_zone_trigger(e):
    """
    scan_htf_spot entries don't include zone_trigger; compute it.
    BEAR trap: entry is at zone_low + 1/3 of range (price returns down after trap)
    BULL trap: entry is at zone_high - 1/3 of range (price returns up after trap)
    """
    if "zone_trigger" in e:
        return e["zone_trigger"]
    zh, zl = e["zone_high"], e["zone_low"]
    if e.get("kind") == "BULL":
        return round(zh - (zh - zl) / 3, 2)
    return round(zl + (zh - zl) / 3, 2)


def get_t1_price(e):
    """T1 target: for BEAR = zone_high (bears' SL); for BULL = zone_low (bulls' SL)."""
    if e.get("kind") == "BULL":
        return round(e["zone_low"], 2)
    return round(e.get("sl", e["zone_high"]), 2)


def get_init_sl(e, sl_buf):
    """Initial SL: for BEAR = zone_low - buf; for BULL = zone_high + buf."""
    if e.get("kind") == "BULL":
        return round(e["zone_high"] + sl_buf, 2)
    return round(e["zone_low"] - sl_buf, 2)


def simulate_with_5m_trail(entries, df1m, lot, lots, sl_buf=2.0, name=""):
    """
    Two-tier exit:
      T1 = 50% at zone_high (bears' SL)
      Remaining 50%: trail_sl starts at zone_low-buf, trails UP on each 5m bar close
                     trail_sl = max(trail_sl, prev_5m_bar_low - sl_buf)  [ratchet only up]
      EOD: exit remaining at close
    """
    total_qty = lot * lots
    t1_qty    = total_qty // 2
    rem_qty   = total_qty - t1_qty
    results   = []

    trapped = [e for e in entries if e.get("status") in ("TRAPPED","CLOSED")]
    if not trapped:
        return results

    for e in trapped:
        entry_price = get_zone_trigger(e)
        t1_price    = get_t1_price(e)
        trail_sl    = get_init_sl(e, sl_buf)
        is_bull     = e.get("kind") == "BULL"

        trap_ts = pd.to_datetime(e.get("trapped_on") or e.get("closed_on"))
        future  = df1m[df1m["datetime"] > trap_ts] if trap_ts is not pd.NaT else df1m.copy()

        if future.empty:
            continue

        t1_hit     = False
        t1_pnl     = 0.0
        exit_price = None
        exit_reason= "OPEN"
        exit_ts    = None
        last_5m_ts = None

        for _, row in future.iterrows():
            bar_ts = row["datetime"]

            # --- 5m trail update (only after T1 hit) ---
            if t1_hit:
                bucket = bar_ts.floor("5min")
                if last_5m_ts is None or bucket > last_5m_ts:
                    last_5m_ts = bucket
                    prev_start = bucket - pd.Timedelta(minutes=5)
                    prev_bars  = df1m[(df1m["datetime"] >= prev_start) & (df1m["datetime"] < bucket)]
                    if not prev_bars.empty:
                        if is_bull:
                            # BULL trap: SL trails DOWN from zone_high (ratchet only moves DOWN)
                            prev_high = prev_bars["high"].max()
                            cand = round(prev_high + sl_buf, 2)
                            if cand < trail_sl:
                                trail_sl = cand
                        else:
                            # BEAR trap: SL trails UP from zone_low (ratchet only moves UP)
                            prev_low = prev_bars["low"].min()
                            cand = round(prev_low - sl_buf, 2)
                            if cand > trail_sl:
                                trail_sl = cand

            # --- SL check ---
            active_sl = trail_sl if t1_hit else get_init_sl(e, sl_buf)
            if is_bull:
                hit_sl = row["high"] >= active_sl
            else:
                hit_sl = row["low"] <= active_sl
            if hit_sl:
                exit_price  = active_sl
                exit_reason = "TRAIL_SL" if t1_hit else "SL"
                exit_ts     = bar_ts
                break

            # --- T1 check ---
            if not t1_hit:
                if is_bull:
                    t1_hit = row["low"] <= t1_price
                else:
                    t1_hit = row["high"] >= t1_price
                if t1_hit:
                    t1_pnl = round(abs(t1_price - entry_price) * t1_qty, 2)

        # EOD exit
        if exit_price is None:
            last = future.iloc[-1]
            exit_price  = round(last["close"], 2)
            exit_reason = "EOD"
            exit_ts     = last["datetime"]

        if is_bull:
            rem_pnl = round((entry_price - exit_price) * rem_qty, 2)  # profit when price falls
        else:
            rem_pnl = round((exit_price - entry_price) * rem_qty, 2)  # profit when price rises
        total_pnl = round(t1_pnl + rem_pnl, 2)

        results.append({
            "index":    name,
            "entry":    entry_price,
            "t1":       t1_price,
            "init_sl":  round(e["zone_low"] - sl_buf, 2),
            "trail_sl_final": trail_sl,
            "t1_hit":   t1_hit,
            "exit":     exit_price,
            "reason":   exit_reason,
            "exit_ts":  exit_ts,
            "t1_pnl":   t1_pnl,
            "rem_pnl":  rem_pnl,
            "total_pnl":total_pnl,
            "total_qty":total_qty,
        })
    return results


def run_index(name, cfg, prev_date, fetch_to, split_on=None):
    test_date = split_on or fetch_to
    print(f"\n{'='*65}")
    print(f"  {name}  |  source: {cfg['htf_src'].upper()}")
    print(f"{'='*65}")

    # Fetch data
    print(f"  Fetching 1m bars {prev_date} to {fetch_to}...")
    try:
        df_all = fetch_1m(cfg["key"], prev_date, fetch_to)
    except Exception as e:
        print(f"  ERROR fetching: {e}")
        return None

    if df_all.empty:
        print("  No data returned.")
        return None

    test_dt = pd.to_datetime(test_date).date()
    df_prev = df_all[df_all["datetime"].dt.date < test_dt].copy()
    df_today= df_all[df_all["datetime"].dt.date == test_dt].copy()

    print(f"  Prev bars={len(df_prev)}  Today bars={len(df_today)}")
    if df_prev.empty or df_today.empty:
        print("  SKIP: missing prev or today data")
        return None

    # Prev-day OHLC for pivot + gap
    prev_H = df_prev["high"].max()
    prev_L = df_prev["low"].min()
    prev_C = df_prev["close"].iloc[-1]
    piv    = pivot_levels(prev_H, prev_L, prev_C)

    print(f"  Prev day: H={prev_H:.0f}  L={prev_L:.0f}  C={prev_C:.0f}")
    print(f"  Pivots:   P={piv['P']:.0f}  R1={piv['R1']:.0f}  R2={piv['R2']:.0f}  S1={piv['S1']:.0f}  S2={piv['S2']:.0f}")

    today_open = df_today["open"].iloc[0]
    gap_pct    = abs(today_open - prev_C) / prev_C * 100
    gap_dir    = "UP" if today_open > prev_C else "DOWN"
    gap_fired  = gap_pct >= 1.0
    step       = cfg["step"]
    near       = cfg["gap_near"]

    def rs(v): return int(round(v/step)*step)

    if gap_fired:
        atm = rs(today_open)
        ce_strike = atm - near if gap_dir=="UP" else atm + near
        pe_strike = atm + near if gap_dir=="UP" else atm - near
        print(f"\n  GAP {gap_dir} = {gap_pct:.2f}%  ->  CE={ce_strike}  PE={pe_strike}  (ITM+/-{near})")
    else:
        # CE at S1/S2 (support, bears short here); PE at R1/R2 (resistance, bulls buy here)
        ce_strike = rs(piv["S1"])
        pe_strike = rs(piv["R1"])
        print(f"\n  No gap ({gap_pct:.2f}%)  ->  Pivot  CE={ce_strike} (S1={piv['S1']:.0f})  PE={pe_strike} (R1={piv['R1']:.0f})")

    # HTF scan on FULL data (prev + today) — ref candle can be from prev day!
    # Then filter to zones that became TRAPPED/CLOSED DURING today's session only.
    htf_all = resample_htf(df_all, HTF_MIN)
    htf_today_only = resample_htf(df_today, HTF_MIN)
    print(f"\n  HTF bars ({HTF_MIN}m): total={len(htf_all)}  today-only={len(htf_today_only)}")
    if len(htf_all) < 2:
        print("  SKIP: not enough HTF bars")
        return None

    # Use scan_htf_spot for NIFTY/SENSEX (catches BOTH bull and bear traps)
    # Use scan_htf for CRUDEOIL futures (bearish traps only on futures chart)
    if cfg["htf_src"] == "spot":
        _, htf_entries = scanner.scan_htf_spot(htf_all)
    else:
        _, htf_entries = scanner.scan_htf(htf_all)

    # Filter to zones that TRIGGERED (TRAPPED/CLOSED) during today's session
    def is_today(ts_str):
        if not ts_str:
            return False
        try:
            return pd.to_datetime(ts_str).date() == test_dt
        except Exception:
            return False

    all_done  = [e for e in htf_entries if e["status"] in ("TRAPPED","CLOSED")]
    trapped   = [e for e in all_done if is_today(e.get("trapped_on") or e.get("closed_on"))]
    active    = [e for e in htf_entries if e["status"] == "ACTIVE"]
    print(f"  HTF scan: total={len(htf_entries)}  TRAPPED/CLOSED(all)={len(all_done)}  TRAPPED/CLOSED(today)={len(trapped)}  ACTIVE={len(active)}")

    if active:
        print(f"  Active (not yet triggered):")
        for e in active[:4]:
            print(f"    zone_high={e.get('zone_high','?'):.2f}  zone_low={e.get('zone_low','?'):.2f}  target={e.get('sl','?'):.2f}  kind={e.get('kind','BEAR')}")

    print(f"\n  {'ZONE_HIGH':>10} {'ZONE_LOW':>10} {'TRIGGER':>10} {'TARGET':>10} {'STATUS':>8} {'KIND':>6}")
    for e in trapped:
        print(f"  {e['zone_high']:10.2f} {e['zone_low']:10.2f} {get_zone_trigger(e):10.2f} {get_t1_price(e):10.2f} {e['status']:>8} {e.get('kind','BEAR'):>6}")

    if not trapped:
        print("  No zones to trade.")
        return None

    # Simulate trades with 5m trailing SL
    results = simulate_with_5m_trail(trapped, df_today, cfg["lot"], lots=1,
                                     sl_buf=SL_BUF, name=name)
    if not results:
        print("  No trade entries found in 1m bars.")
        return None

    print(f"\n  {'ENTRY':>8} {'T1':>8} {'SL_INIT':>8} {'TRAIL_SL':>9} {'T1?':>4} {'EXIT':>8} {'REASON':<15} {'PNL_PTS':>8} {'PNL_RS':>10}")
    print(f"  {'-'*8} {'-'*8} {'-'*8} {'-'*9} {'-'*4} {'-'*8} {'-'*15} {'-'*8} {'-'*10}")
    total_pts = 0
    for t in results:
        pnl_pts = t["total_pnl"] / t["total_qty"] if t["total_qty"] else 0
        print(f"  {t['entry']:8.2f} {t['t1']:8.2f} {t['init_sl']:8.2f} {t['trail_sl_final']:9.2f} "
              f"{'Y' if t['t1_hit'] else 'N':>4} {t['exit']:8.2f} {t['reason']:<15} "
              f"{pnl_pts:8.2f} {t['total_pnl']:10.2f}")
        total_pts += pnl_pts

    print(f"\n  NET PNL: {total_pts:+.2f} pts x {cfg['lot']} lot = RS {total_pts*cfg['lot']:+.0f}")
    return results


if __name__ == "__main__":
    # Run two days for fuller picture
    # Upstox excludes to_date when recent; use next_day as fetch_to
    TEST_RUNS = [
        {"label": "2026-06-16 (Mon/yesterday)", "test_date":"2026-06-16", "prev_date":"2026-06-13", "fetch_to":"2026-06-17"},
        {"label": "2026-06-13 (Fri)",            "test_date":"2026-06-13", "prev_date":"2026-06-12", "fetch_to":"2026-06-16"},
    ]

    print(f"HTF={HTF_MIN}m  SL_BUF={SL_BUF}pts  Trail: 5m bar low - buffer (ratchet up = new bears entry)")

    grand_pnl = 0
    for run in TEST_RUNS:
        print(f"\n\n{'#'*65}")
        print(f"  TEST DAY: {run['label']}")
        print(f"  prev={run['prev_date']}  test={run['test_date']}")
        print(f"{'#'*65}")
        day_pnl = 0
        for name, cfg in INSTRUMENTS.items():
            try:
                r = run_index(name, cfg, run["prev_date"], run["fetch_to"], split_on=run["test_date"])
                if r:
                    for t in r:
                        day_pnl += t["total_pnl"]
                        grand_pnl += t["total_pnl"]
            except Exception as exc:
                import traceback
                print(f"\nERROR [{name}]: {exc}")
                traceback.print_exc()
        print(f"\n  DAY PNL ({run['label']}): RS={day_pnl:+.2f}")

    print(f"\n\n{'='*65}")
    print(f"  GRAND TOTAL 2 DAYS: RS={grand_pnl:+.2f}")
    print(f"{'='*65}")
