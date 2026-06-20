"""
backtest_crude_compare.py — CrudeOil Two-Approach Comparison Backtest
========================================================================
Compares two entry approaches on CrudeOil for the last N trading days.

APPROACH A — INTRADAY ONLY (no HTF pre-existing zones)
  1. At market open (09:00 MCX), compute ATM from first futures bar
  2. CE1 = ATM - 1 step (1-ITM call),  PE1 = ATM + 1 step (1-ITM put)
  3. Bias from 15-min FUTURES bars (bear trap → CE trade, bull trap → PE trade)
  4. Trigger: futures price enters the 15-min trap zone
  5. LTF confirmation: 5-min OPTION bars (CE1/PE1 premium) must show all sellers cleared
     (i.e., scan_htf finds only CLOSED zones, no TRAPPED — sellers exhausted → enter)
  6. SL: futures zone_low (CE) / zone_high (PE) ± buffer
  7. T1: zone SL level (HTF bears' stop = our target)

APPROACH B — HTF 30-MIN + LTF 5-MIN (current live logic)
  1. Strike = 200-pts ITM (CE1 = ATM-200, PE1 = ATM+200) — fixed at open
  2. Bias from 30-min FUTURES HTF zones (BEAR → CE, BULL → PE)
  3. LTF: 5-min option bars, all-sellers-cleared trigger
  4. Same SL/T1 logic

Both approaches:
  - Entry only between 09:30 and 22:30 MCX
  - EOD square-off at 23:00
  - 1 position at a time per side (CE or PE)
  - P&L in OPTION PREMIUM points (not futures points)
    (premium entry/exit tracked on the option bar at the time of signal)

Usage:
  python scripts/backtest_crude_compare.py --token YOUR_UPSTOX_TOKEN
  python scripts/backtest_crude_compare.py --token YOUR_TOKEN --days 7 --lots 2
"""

from __future__ import annotations
import argparse, gzip, json, sys, os, time
from datetime import date, timedelta
from typing import Optional
import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from strategies.trap_scanner import scanner

# ── Config ────────────────────────────────────────────────────────────────────
CRUDE_STEP   = 100        # strike step (Rs)
CRUDE_LOT    = 100        # 1 lot = 100 barrels
SL_BUF       = 20.0      # futures SL buffer in Rs
ENTRY_OPEN   = "09:30"   # no entries before this
SQ_OFF       = "23:00"   # EOD square-off
MKT_OPEN     = "09:00"
MKT_CLOSE    = "23:30"

# HTF / LTF timeframes
HTF_A        = 15         # Approach A: 15-min futures (intraday only)
HTF_B        = 30         # Approach B: 30-min futures (live engine)
LTF_MIN      = 5          # 5-min option bars for both
ITM_STEPS_A  = 1          # Approach A: 1-ITM strike (ATM ± 1 step)
ITM_STEPS_B  = 2          # Approach B: 2-ITM (ATM ± 200 = 2 steps of 100)

HEADERS: dict = {}        # set from --token arg

# ── API helpers ───────────────────────────────────────────────────────────────
def _get(url: str, retries: int = 3) -> dict:
    for _ in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 429:
                time.sleep(2); continue
            return r.json() if r.status_code == 200 else {}
        except Exception:
            time.sleep(1)
    return {}

def fetch_1m(key: str, dt: str) -> pd.DataFrame:
    enc  = key.replace("|", "%7C")
    url  = f"https://api.upstox.com/v2/historical-candle/{enc}/1minute/{dt}/{dt}"
    data = _get(url)
    cands = data.get("data", {}).get("candles", [])
    if not cands:
        return pd.DataFrame()
    df = pd.DataFrame(cands, columns=["datetime","open","high","low","close","volume","oi"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)
    df = df[(df["datetime"].dt.strftime("%H:%M") >= MKT_OPEN) &
            (df["datetime"].dt.strftime("%H:%M") <= MKT_CLOSE)]
    return df

def resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty:
        return df
    dfc = df.set_index("datetime")
    r = dfc.resample(f"{minutes}min", label="right", closed="right").agg(
        {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}
    ).dropna().reset_index()
    return r

def load_mcx_master() -> list:
    url = "https://assets.upstox.com/market-quote/instruments/exchange/MCX.json.gz"
    try:
        r = requests.get(url, timeout=30)
        return json.loads(gzip.decompress(r.content))
    except Exception as e:
        print(f"  [master] MCX load failed: {e}"); return []

_MCX_MASTER: list = []

def find_crude_option(strike: int, otype: str, min_expiry: date) -> str:
    global _MCX_MASTER
    if not _MCX_MASTER:
        _MCX_MASTER = load_mcx_master()
        print(f"  [master] MCX: {len(_MCX_MASTER)} instruments")
    ot = otype.upper()
    candidates = []
    for row in _MCX_MASTER:
        itype = str(row.get("instrument_type","")).upper()
        row_otype = itype if itype in ("CE","PE") else str(row.get("option_type","")).upper()
        if row_otype != ot:
            continue
        if abs(float(row.get("strike", 0) or 0) - strike) > 0.5:
            continue
        sym = str(row.get("tradingsymbol","") or row.get("name","")).upper()
        und = str(row.get("underlying_symbol","") or "").upper()
        if "CRUDE" not in sym and "CRUDE" not in und:
            continue
        exp_str = str(row.get("expiry","") or "")
        try:
            exp_dt = date.fromisoformat(exp_str[:10])
        except Exception:
            continue
        if exp_dt < min_expiry:
            continue
        key = str(row.get("instrument_key",""))
        if key:
            candidates.append((exp_dt, key))
    if not candidates:
        return ""
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]

def get_atm(fut_df: pd.DataFrame, step: int) -> int:
    """ATM = first bar's open, rounded to nearest step."""
    if fut_df.empty:
        return 0
    first_open = float(fut_df.iloc[0]["open"])
    return int(round(first_open / step) * step)

def get_trading_days(n: int) -> list[str]:
    days = []
    d = date.today()
    while len(days) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            days.append(d.isoformat())
    return list(reversed(days))

# ── Core LTF scan: all-sellers-cleared on option bars ─────────────────────────
def all_sellers_cleared(opt_df_today: pd.DataFrame) -> bool:
    """
    Scan today's 5-min option bars. Return True when:
      - At least 1 bear trap is CLOSED (sellers entered and got squeezed out)
      - 0 bear traps currently TRAPPED (no active squeeze)
    This = all option sellers are exhausted → enter immediately.
    """
    if opt_df_today.empty or len(opt_df_today) < 3:
        return False
    _, entries = scanner.scan_htf(opt_df_today)
    trapped = [e for e in entries if e["status"] == "TRAPPED"]
    closed  = [e for e in entries if e["status"] == "CLOSED"]
    return len(closed) > 0 and len(trapped) == 0

# ── P&L recording ─────────────────────────────────────────────────────────────
def record_exit(trade: dict, exit_price: float, exit_ts, reason: str, out: list):
    ep  = trade["entry_price"]
    opt = trade["opt_type"]
    # option P&L: we BOUGHT the option premium
    pnl_pts = (exit_price - ep)   # positive = profit (premium rose)
    out.append({
        "approach":   trade["approach"],
        "opt_type":   opt,
        "strike":     trade["strike"],
        "entry_ts":   str(entry_ts_fmt(trade["entry_ts"])),
        "exit_ts":    str(entry_ts_fmt(exit_ts)),
        "entry":      round(ep, 2),
        "exit":       round(exit_price, 2),
        "sl":         round(trade["sl_price"], 2),
        "t1":         round(trade["t1_price"], 2),
        "t1_hit":     trade["t1_hit"],
        "result":     reason,
        "pnl_pts":    round(pnl_pts, 2),
        "fut_entry":  round(trade.get("fut_entry", 0), 2),
        "bias_zone":  trade.get("bias_zone", ""),
    })

def entry_ts_fmt(ts) -> str:
    if hasattr(ts, "strftime"):
        return ts.strftime("%H:%M")
    return str(ts)[:16]

# ── One-day backtest for one approach ─────────────────────────────────────────
def run_day(
    trade_date: str,
    fut_df: pd.DataFrame,
    ce_df: pd.DataFrame,
    pe_df: pd.DataFrame,
    ce_strike: int,
    pe_strike: int,
    htf_min: int,
    approach: str,
) -> list[dict]:
    """
    Both approaches share the same structure:
      - futures HTF zones give BEAR/BULL bias
      - option LTF (5-min) all_sellers_cleared → entry
      - SL = futures zone boundary ± buffer (mapped to option exit when SL futures hit)
      - T1 = zone target (HTF bears' SL level) on futures
      - Exit: when futures hits SL or T1, record option premium exit at that bar
    """
    sq_time    = pd.Timestamp(f"{trade_date} {SQ_OFF}")
    entry_open = pd.Timestamp(f"{trade_date} {ENTRY_OPEN}")
    trades: list[dict] = []

    def _run_side(opt_type: str, opt_df: pd.DataFrame, strike: int):
        if fut_df.empty or opt_df.empty:
            return
        in_trade  = None
        notified  = set()
        fut5 = resample(fut_df, htf_min)

        for idx, row in fut5.iterrows():
            bar_ts  = row["datetime"]
            cur_fut = float(row["close"])

            if bar_ts < entry_open:
                continue
            if bar_ts >= sq_time:
                if in_trade:
                    # EOD — find option price at this bar
                    opt_bar = opt_df[opt_df["datetime"] <= bar_ts]
                    exit_p  = float(opt_bar.iloc[-1]["close"]) if not opt_bar.empty else in_trade["entry_price"]
                    record_exit(in_trade, exit_p, bar_ts, "EOD", trades)
                    in_trade = None
                break

            # ── Exit check ─────────────────────────────────────────────────
            if in_trade:
                # Check futures bars since entry for SL or T1
                fut_fwd = fut_df[(fut_df["datetime"] > in_trade["entry_ts"]) &
                                 (fut_df["datetime"] <= bar_ts)]
                result = None
                for _, fb in fut_fwd.iterrows():
                    flo, fhi, fts = float(fb["low"]), float(fb["high"]), fb["datetime"]
                    if fts >= sq_time:
                        opt_bar = opt_df[opt_df["datetime"] <= fts]
                        ep = float(opt_bar.iloc[-1]["close"]) if not opt_bar.empty else in_trade["entry_price"]
                        result = ("EOD", ep, fts); break
                    # T1 check: CE → futures rises to zone target; PE → futures falls to target
                    if not in_trade["t1_hit"]:
                        t1_hit = (fhi >= in_trade["t1_price"]) if opt_type == "CE" \
                                 else (flo <= in_trade["t1_price"])
                        if t1_hit:
                            in_trade["t1_hit"] = True
                            opt_bar = opt_df[opt_df["datetime"] <= fts]
                            exit_p  = float(opt_bar.iloc[-1]["close"]) if not opt_bar.empty else in_trade["entry_price"]
                            result  = ("T1", exit_p, fts); break
                    # SL check: CE → futures drops below SL; PE → rises above SL
                    sl_hit = (flo <= in_trade["sl_price"]) if opt_type == "CE" \
                             else (fhi >= in_trade["sl_price"])
                    if sl_hit:
                        opt_bar = opt_df[opt_df["datetime"] <= fts]
                        exit_p  = float(opt_bar.iloc[-1]["close"]) if not opt_bar.empty else in_trade["entry_price"]
                        result  = ("SL", exit_p, fts); break
                if result:
                    record_exit(in_trade, result[1], result[2], result[0], trades)
                    in_trade = None
                continue

            # ── Find futures HTF zone ──────────────────────────────────────
            hist_fut = fut_df[fut_df["datetime"] <= bar_ts]
            htf = resample(hist_fut, htf_min)
            if len(htf) < 2:
                continue

            _, all_zones = scanner.scan_htf_spot(htf)
            trapped = [e for e in all_zones if e["status"] == "TRAPPED"]

            # Filter by direction: CE=BEAR zone, PE=BULL zone
            kind_want = "BEAR" if opt_type == "CE" else "BULL"
            bias_zones = [e for e in trapped if e.get("kind") == kind_want
                          and e.get("zone_low", 0) <= cur_fut]
            # For BEAR: spot must be inside zone; for BULL: spot at or above zone_low
            if opt_type == "CE":
                bias_zones = [e for e in bias_zones
                              if e.get("zone_low",0) <= cur_fut <= e.get("zone_high",0)]

            if not bias_zones:
                continue

            # Pick nearest zone
            zone = min(bias_zones, key=lambda e: abs(cur_fut - e.get("zone_low", cur_fut)))
            uid  = f"{zone.get('ref_ts','')}_{zone.get('zone_high',0):.1f}_{kind_want}"
            if uid in notified:
                continue

            # ── LTF: all option sellers cleared? ──────────────────────────
            opt_today = opt_df[
                (opt_df["datetime"].dt.date == pd.Timestamp(trade_date).date()) &
                (opt_df["datetime"] <= bar_ts)
            ]
            opt_5m = resample(opt_today, LTF_MIN)
            if not all_sellers_cleared(opt_5m):
                continue

            # Entry: record current option premium as entry price
            opt_bar = opt_df[opt_df["datetime"] <= bar_ts]
            if opt_bar.empty:
                continue
            entry_opt_p = float(opt_bar.iloc[-1]["close"])
            if entry_opt_p <= 0:
                continue

            # Futures SL and T1
            if opt_type == "CE":
                sl_p  = round(zone["zone_low"] - SL_BUF, 1)
                t1_p  = zone.get("sl", zone["zone_high"] + 50)
            else:
                sl_p  = round(zone["zone_high"] + SL_BUF, 1)
                t1_p  = zone.get("sl", zone["zone_low"] - 50)

            notified.add(uid)
            in_trade = {
                "approach":   approach,
                "opt_type":   opt_type,
                "strike":     strike,
                "entry_price": entry_opt_p,
                "sl_price":    sl_p,
                "t1_price":    t1_p,
                "t1_hit":      False,
                "entry_ts":    bar_ts,
                "fut_entry":   cur_fut,
                "bias_zone":   f"{zone.get('zone_low',0):.0f}→{zone.get('zone_high',0):.0f}",
            }

        # EOD safety
        if in_trade and not fut_df.empty:
            opt_bar = opt_df[opt_df["datetime"] <= sq_time]
            exit_p  = float(opt_bar.iloc[-1]["close"]) if not opt_bar.empty else in_trade["entry_price"]
            record_exit(in_trade, exit_p, sq_time, "EOD", trades)

    _run_side("CE", ce_df, ce_strike)
    _run_side("PE", pe_df, pe_strike)
    return trades

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="CrudeOil two-approach comparison backtest")
    parser.add_argument("--token", required=True, help="Upstox access token")
    parser.add_argument("--days",  type=int, default=7, help="Trading days to backtest")
    parser.add_argument("--lots",  type=int, default=2, help="Number of lots")
    parser.add_argument("--fut-key", default="MCX_FO|520702", help="Futures instrument key")
    args = parser.parse_args()

    global HEADERS
    HEADERS = {"Authorization": f"Bearer {args.token}", "Accept": "application/json"}

    trading_days = get_trading_days(args.days)
    lot_size     = CRUDE_LOT * args.lots

    print(f"\n{'='*70}")
    print(f"  CrudeOil Comparison Backtest")
    print(f"  Period : {trading_days[0]} → {trading_days[-1]}  ({len(trading_days)} days)")
    print(f"  Lots   : {args.lots}  (lot_size={lot_size})")
    print(f"  Approach A : 15-min FUT bias + 5-min OPT LTF + 1-ITM strike")
    print(f"  Approach B : 30-min FUT bias + 5-min OPT LTF + 2-ITM strike")
    print(f"{'='*70}\n")

    all_results: list[dict] = []

    for trade_date in trading_days:
        print(f"\n{'─'*60}")
        print(f"  DATE: {trade_date}")
        print(f"{'─'*60}")

        # Fetch futures 1m bars
        print(f"  Fetching futures ({args.fut_key})... ", end="", flush=True)
        fut_df = fetch_1m(args.fut_key, trade_date)
        print(f"{len(fut_df)} bars")
        time.sleep(0.5)

        if fut_df.empty:
            print("  No futures data — skip"); continue

        # ATM from first bar
        atm = get_atm(fut_df, CRUDE_STEP)
        if atm <= 0:
            print("  Could not determine ATM — skip"); continue

        # Approach A strikes (1-ITM)
        ce_strike_a = atm - ITM_STEPS_A * CRUDE_STEP
        pe_strike_a = atm + ITM_STEPS_A * CRUDE_STEP

        # Approach B strikes (2-ITM)
        ce_strike_b = atm - ITM_STEPS_B * CRUDE_STEP
        pe_strike_b = atm + ITM_STEPS_B * CRUDE_STEP

        print(f"  ATM={atm}  A: CE={ce_strike_a} PE={pe_strike_a}  "
              f"B: CE={ce_strike_b} PE={pe_strike_b}")

        dt_obj = date.fromisoformat(trade_date)

        # Find option instrument keys
        print(f"  Finding option keys... ", end="", flush=True)
        ce_key_a = find_crude_option(ce_strike_a, "CE", dt_obj)
        pe_key_a = find_crude_option(pe_strike_a, "PE", dt_obj)
        ce_key_b = find_crude_option(ce_strike_b, "CE", dt_obj)
        pe_key_b = find_crude_option(pe_strike_b, "PE", dt_obj)
        print(f"CE_A={bool(ce_key_a)} PE_A={bool(pe_key_a)} CE_B={bool(ce_key_b)} PE_B={bool(pe_key_b)}")

        # Fetch option bars
        def _fetch_opt(key: str, label: str) -> pd.DataFrame:
            if not key:
                print(f"    {label}: no key")
                return pd.DataFrame()
            print(f"  Fetching {label}... ", end="", flush=True)
            df = fetch_1m(key, trade_date)
            print(f"{len(df)} bars")
            time.sleep(0.4)
            return df

        ce_df_a = _fetch_opt(ce_key_a, f"CE{ce_strike_a}(A)")
        pe_df_a = _fetch_opt(pe_key_a, f"PE{pe_strike_a}(A)")
        ce_df_b = _fetch_opt(ce_key_b, f"CE{ce_strike_b}(B)") if ce_strike_b != ce_strike_a else ce_df_a
        pe_df_b = _fetch_opt(pe_key_b, f"PE{pe_strike_b}(B)") if pe_strike_b != pe_strike_a else pe_df_a

        # Run both approaches
        trades_a = run_day(trade_date, fut_df, ce_df_a, pe_df_a,
                           ce_strike_a, pe_strike_a, HTF_A, "A-15m")
        trades_b = run_day(trade_date, fut_df, ce_df_b, pe_df_b,
                           ce_strike_b, pe_strike_b, HTF_B, "B-30m")

        # Print results
        for approach, trades in [("A (15-min intraday)", trades_a),
                                   ("B (30-min HTF)",      trades_b)]:
            day_pnl = sum(t["pnl_pts"] * lot_size for t in trades)
            print(f"\n  Approach {approach}:")
            if not trades:
                print("    No trades")
            for t in trades:
                pnl_rs = round(t["pnl_pts"] * lot_size, 0)
                tag    = "T1  " if t["t1_hit"] else "    "
                win    = "WIN " if t["pnl_pts"] > 0 else "LOSS"
                print(f"    {t['entry_ts']}→{t['exit_ts']}  {t['opt_type']}"
                      f"  strike={t['strike']}  "
                      f"opt_in={t['entry']:.1f}  opt_out={t['exit']:.1f}  "
                      f"fut@entry={t['fut_entry']:.0f}  zone={t['bias_zone']}  "
                      f"sl={t['sl']:.0f}  t1={t['t1']:.0f}  "
                      f"{tag} {t['result']:<8}  "
                      f"{t['pnl_pts']:+.1f}pts  Rs{pnl_rs:+.0f}  {win}")
            print(f"    Day P&L: Rs{day_pnl:+.0f}")

            for t in trades:
                t["date"] = trade_date
            all_results.extend(trades)

    # ── Summary ───────────────────────────────────────────────────────────────
    if not all_results:
        print("\nNo trades found in the period.")
        return

    df_res = pd.DataFrame(all_results)
    print(f"\n\n{'='*70}")
    print("  SUMMARY")
    print(f"{'='*70}")

    for approach in ["A-15m", "B-30m"]:
        sub = df_res[df_res["approach"] == approach]
        label = "Approach A (15-min intraday)" if approach == "A-15m" else "Approach B (30-min HTF)"
        if sub.empty:
            print(f"\n  {label}: No trades")
            continue
        total_pnl = sub["pnl_pts"].sum() * lot_size
        wins      = sub[sub["pnl_pts"] > 0]
        losses    = sub[sub["pnl_pts"] <= 0]
        win_rate  = len(wins) / len(sub) * 100 if len(sub) else 0
        avg_win   = wins["pnl_pts"].mean() * lot_size if len(wins) else 0
        avg_loss  = losses["pnl_pts"].mean() * lot_size if len(losses) else 0
        print(f"\n  {label}")
        print(f"    Trades    : {len(sub)}  (W={len(wins)} L={len(losses)})  win%={win_rate:.0f}%")
        print(f"    Total P&L : Rs{total_pnl:+,.0f}")
        print(f"    Avg Win   : Rs{avg_win:+,.0f}    Avg Loss : Rs{avg_loss:+,.0f}")
        print(f"    By day:")
        for dt, grp in sub.groupby("date"):
            dp = grp["pnl_pts"].sum() * lot_size
            print(f"      {dt}  {len(grp)} trades  Rs{dp:+,.0f}")

    print(f"\n{'='*70}")

if __name__ == "__main__":
    main()
