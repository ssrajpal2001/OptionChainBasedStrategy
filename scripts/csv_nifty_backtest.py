"""
scripts/csv_nifty_backtest.py — NIFTY backtest from local CSV option data.

Reads GFDL/broker CSV files (one per day or one per month).

CSV format expected:
  Ticker,Date,Time,Open,High,Low,Close,Volume,Open Interest
  NIFTY31JUL2525500CE.NFO,01-07-2025,09:15:59,450.0,...

Ticker format: NIFTY + DDMONYY + strike + CE/PE + .NFO
Date format  : DD-MM-YYYY
Time format  : HH:MM:SS  (each row = 1-minute bar, time = bar end)

Strategy (identical to nifty_backtest.py):
  - Monthly expiry (last Thursday of month)
  - ATM from nearest CE≈PE at 09:30
  - CE = ATM − 200 (near ITM),  PE = ATM + 200
  - Pure intraday cascade: 15m → 3m → 1m HIGH breakout
  - Zone detection: scanner.scan_htf on option premium bars
  - SL: intrabar — exits at zone_low − sl_buf
  - T1: zone_high; 5m ratchet TSL after T1
  - EOD: 15:25 IST

Usage:
  # Single day or single file (all dates in it):
  python3 scripts/csv_nifty_backtest.py --file GFDLNFO_OPTIONS_01072025.csv

  # Directory of daily files (auto-loads all matching files):
  python3 scripts/csv_nifty_backtest.py --dir /path/to/csv_files/

  # Override params:
  python3 scripts/csv_nifty_backtest.py --file data.csv --sl-buf 5 --max-ltf 10 --lots 1
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, ".")
from strategies.trap_scanner import scanner

# ── Constants (same as nifty_backtest.py) ─────────────────────────────────────
LOT_SIZE  = 75     # NIFTY lot (June 2026 onward)
LOT_SIZE_OLD = 50  # pre-2024
STEP      = 50     # NIFTY strike step
NEAR_OFFSET = 200  # CE = ATM-200, PE = ATM+200
EOD_TIME  = "15:25"
ENTRY_OPEN = "09:30"

TICKER_PAT = re.compile(r'^NIFTY(\d{2}[A-Z]{3}\d{2})(\d+)(CE|PE)\.NFO$')

# ── Ticker parsing ─────────────────────────────────────────────────────────────
def _parse_ticker(t: str):
    """Return (expiry_str, strike_int, otype_str) or (None, None, None)."""
    m = TICKER_PAT.match(t)
    if m:
        return m.group(1), int(m.group(2)), m.group(3)
    return None, None, None


def _expiry_date(exp_str: str) -> date | None:
    """Convert '31JUL25' → date(2025, 7, 31)."""
    try:
        return datetime.strptime(exp_str, "%d%b%y").date()
    except Exception:
        return None


def _monthly_expiry(trade_date: date) -> str:
    """
    Return expiry_str (DDMONYY) for the MONTHLY NIFTY contract
    active on trade_date — last Thursday of the same month, or next
    month's if past that Thursday.
    """
    # Last Thursday of trade_date's month
    y, m = trade_date.year, trade_date.month
    # Find last Thursday
    last_day = date(y, m + 1, 1) - timedelta(days=1) if m < 12 else date(y + 1, 1, 1) - timedelta(days=1)
    while last_day.weekday() != 3:  # Thursday=3
        last_day -= timedelta(days=1)
    if trade_date > last_day:
        # Roll to next month
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
        last_day = date(y, m + 1, 1) - timedelta(days=1) if m < 12 else date(y + 1, 1, 1) - timedelta(days=1)
        while last_day.weekday() != 3:
            last_day -= timedelta(days=1)
    return last_day.strftime("%d%b%y").upper()


# ── Load + normalize CSV ───────────────────────────────────────────────────────
def _load_csv(path: str) -> pd.DataFrame:
    """Load one CSV file, return raw DataFrame."""
    print(f"  Loading {os.path.basename(path)}... ", end="", flush=True)
    df = pd.read_csv(path, dtype={"Ticker": str, "Date": str, "Time": str})
    print(f"{len(df):,} rows")
    return df


def load_nifty_monthly(paths: list[str]) -> pd.DataFrame:
    """
    Load all CSVs, filter to NIFTY monthly options, parse tickers,
    build datetime column, return 1m bar DataFrame.
    """
    frames = []
    for p in paths:
        df = _load_csv(p)
        # Filter NIFTY only
        df = df[df["Ticker"].str.startswith("NIFTY", na=False)].copy()
        if df.empty:
            continue
        # Parse tickers
        parsed = df["Ticker"].apply(_parse_ticker)
        df["expiry"] = parsed.apply(lambda x: x[0])
        df["strike"] = parsed.apply(lambda x: x[1])
        df["otype"]  = parsed.apply(lambda x: x[2])
        df = df.dropna(subset=["expiry", "strike", "otype"])
        df["strike"] = df["strike"].astype(int)

        # Parse datetime (DD-MM-YYYY HH:MM:SS)
        df["datetime"] = pd.to_datetime(
            df["Date"] + " " + df["Time"],
            format="%d-%m-%Y %H:%M:%S",
            errors="coerce"
        )
        df = df.dropna(subset=["datetime"])
        df["trade_date"] = df["datetime"].dt.date
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values("datetime").reset_index(drop=True)
    return combined


# ── ATM detection ─────────────────────────────────────────────────────────────
def _find_atm(df_day: pd.DataFrame, target_time: str = "09:30") -> int:
    """
    Find ATM strike = strike where |CE_close - PE_close| is minimum
    at the target time bar.
    Falls back to 09:15 then 09:16 if no exact match.
    """
    tgt = pd.Timestamp(f"1970-01-01 {target_time}:59").time()
    bar = df_day[df_day["datetime"].dt.time == tgt]
    if bar.empty:
        # Try nearby minute
        for m in range(9*60+15, 9*60+45):
            t = datetime.strptime(f"{m//60:02d}:{m%60:02d}:59", "%H:%M:%S").time()
            bar = df_day[df_day["datetime"].dt.time == t]
            if not bar.empty:
                break
    if bar.empty:
        return 0

    pivot = bar.pivot_table(index="strike", columns="otype", values="Close", aggfunc="mean")
    if "CE" not in pivot.columns or "PE" not in pivot.columns:
        return 0

    pivot["diff"] = (pivot["CE"] - pivot["PE"]).abs()
    atm_raw = int(pivot["diff"].idxmin())
    # Round to nearest 100 (NIFTY strike convention for ITM selection)
    return int(round(atm_raw / 100) * 100)


# ── Build 1m bars for one strike ──────────────────────────────────────────────
def _opt_bars(df_day: pd.DataFrame, strike: int, otype: str,
              expiry: str) -> pd.DataFrame:
    """Extract 1m OHLCV bars for a specific option on a given day."""
    sub = df_day[
        (df_day["strike"] == strike) &
        (df_day["otype"]  == otype)  &
        (df_day["expiry"] == expiry)
    ].copy()
    if sub.empty:
        return pd.DataFrame()
    # Keep market hours 09:15–15:30
    t0 = pd.Timestamp("1970-01-01 09:15:00").time()
    t1 = pd.Timestamp("1970-01-01 15:30:00").time()
    sub = sub[(sub["datetime"].dt.time >= t0) &
              (sub["datetime"].dt.time <= t1)]
    sub = sub[["datetime","Open","High","Low","Close","Volume"]].copy()
    sub.columns = ["datetime","open","high","low","close","volume"]
    sub = sub.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)
    return sub


# ── Resample + zone scan (same as nifty_backtest.py) ─────────────────────────
def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty:
        return df
    dfc = df.set_index("datetime")
    r = dfc.resample(f"{minutes}min", label="right", closed="right").agg(
        {"open": "first", "high": "max", "low": "min",
         "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return r


# ── Simulate exit (same logic as nifty_backtest.py) ───────────────────────────
def _simulate_exit(
    opt_1m: pd.DataFrame,
    entry_ts: pd.Timestamp,
    entry_price: float,
    zone_low: float,
    zone_high: float,
    sl_buf: float,
    trade_date: date,
    total_qty: int,
    opp_signal_ts: pd.Timestamp | None = None,
) -> dict:
    """
    T1 = zone_high, SL = zone_low − sl_buf (intrabar).
    After T1: ratchet TSL on 5m zone lows.
    EOD: 15:25.
    OPP_SIGNAL: if opposite side fires, close at that timestamp.
    """
    sq_ts    = pd.Timestamp(f"{trade_date} {EOD_TIME}")
    init_sl  = zone_low          # sl_trigger = init_sl - sl_buf (applied below)
    t1_price = zone_high

    future_bars = opt_1m[opt_1m["datetime"] > entry_ts].copy()
    if future_bars.empty:
        return {"exit": entry_price, "exit_ts": entry_ts,
                "reason": "EOD", "t1_hit": False, "pnl_rs": 0}

    # 5m ratchet levels
    df_5m = _resample(opt_1m[opt_1m["datetime"] <= sq_ts], 5)
    _, ltf5_all = scanner.scan_htf(df_5m) if len(df_5m) >= 2 else (None, [])
    ratchet = sorted(
        [float(e.get("zone_low", 0))
         for e in (ltf5_all or [])
         if e.get("status") in ("TRAPPED", "CLOSED") and float(e.get("zone_low", 0)) > 0]
    )

    trail_sl    = zone_low
    t1_hit      = False
    t1_qty      = total_qty // 2
    rem_qty     = total_qty - t1_qty
    t1_pnl      = 0.0
    exit_price  = None
    exit_reason = "OPEN"
    exit_ts_out = None

    for _, row in future_bars.iterrows():
        bar_ts    = row["datetime"]
        bar_high  = float(row["high"])
        bar_low   = float(row["low"])
        bar_close = float(row["close"])

        # OPP_SIGNAL
        if opp_signal_ts is not None and bar_ts >= opp_signal_ts:
            exit_price  = bar_close
            exit_reason = "OPP_SIGNAL"
            exit_ts_out = bar_ts
            break

        # EOD
        if bar_ts >= sq_ts:
            exit_price  = bar_close
            exit_reason = "EOD"
            exit_ts_out = bar_ts
            break

        # T1
        if not t1_hit and bar_high >= t1_price:
            t1_hit  = True
            t1_pnl  = (t1_price - entry_price) * t1_qty
            trail_sl = zone_low

        # Ratchet TSL
        if t1_hit:
            new_fl = max(
                (f for f in ratchet if f > trail_sl and f < bar_close),
                default=trail_sl
            )
            if new_fl > trail_sl:
                trail_sl = new_fl

        # SL check (intrabar)
        sl_trigger = (trail_sl if t1_hit else init_sl) - sl_buf
        if bar_low < sl_trigger:
            exit_price  = sl_trigger
            exit_reason = "TRAIL_SL" if t1_hit else "SL"
            exit_ts_out = bar_ts
            break

    if exit_price is None:
        last        = future_bars.iloc[-1]
        exit_price  = float(last["close"])
        exit_reason = "EOD"
        exit_ts_out = last["datetime"]

    exit_qty  = rem_qty if t1_hit else total_qty
    rem_pnl   = (exit_price - entry_price) * exit_qty
    total_pnl = int(round(t1_pnl + rem_pnl, 0))

    return {
        "exit":    round(exit_price, 2),
        "exit_ts": exit_ts_out,
        "reason":  exit_reason,
        "t1_hit":  t1_hit,
        "pnl_rs":  total_pnl,
    }


# ── Per-day backtest (pure intraday 15m→3m→1m) ────────────────────────────────
def _run_day_csv(
    trade_date: date,
    df_all: pd.DataFrame,
    sl_buf: float,
    lots: int,
    max_ltf: int,
    zcache: dict,
) -> list[dict]:
    """Run one trading day from CSV data."""
    expiry   = _monthly_expiry(trade_date)
    total_qty = lots * LOT_SIZE
    entry_open_ts = pd.Timestamp(f"{trade_date} {ENTRY_OPEN}")

    df_day = df_all[df_all["trade_date"] == trade_date].copy()
    if df_day.empty:
        return []

    # Find ATM
    atm = _find_atm(df_day, "09:30")
    if atm <= 0:
        print(f"  {trade_date}: ATM not found — skip")
        return []

    ce_strike = atm - NEAR_OFFSET
    pe_strike = atm + NEAR_OFFSET
    print(f"  {trade_date}  expiry={expiry}  ATM={atm}  CE={ce_strike}  PE={pe_strike}")

    all_trades: list[dict] = []
    # opp_ce_ts = first CE entry time → used as OPP_SIGNAL for PE
    # opp_pe_ts = first PE entry time → used as OPP_SIGNAL for CE (sequential: CE runs first so unused)
    opp_ce_ts: pd.Timestamp | None = None
    opp_pe_ts: pd.Timestamp | None = None

    for opt_type, strike in [("CE", ce_strike), ("PE", pe_strike)]:
        opt_1m = _opt_bars(df_day, strike, opt_type, expiry)
        if opt_1m.empty:
            print(f"  {trade_date} {opt_type} {strike}: no 1m bars for expiry {expiry}")
            continue

        # Zone scan cache
        ck_cas = (trade_date, strike, opt_type, 15, "cas")
        if ck_cas in zcache:
            cas_zones = zcache[ck_cas]
        else:
            df_15 = _resample(opt_1m, 15)
            _, cas_raw = scanner.scan_htf(df_15) if len(df_15) >= 2 else (None, [])
            cas_zones = sorted(
                [e for e in (cas_raw or []) if e.get("status") in ("TRAPPED", "CLOSED")],
                key=lambda z: float(z.get("zone_low", 9999))
            )
            zcache[ck_cas] = cas_zones

        if not cas_zones:
            print(f"  {trade_date} {opt_type} {strike}: no 15m zones")
            continue

        ck_sub = (trade_date, strike, opt_type, 3, "sub")
        if ck_sub in zcache:
            sub_all = zcache[ck_sub]
        else:
            df_3 = _resample(opt_1m, 3)
            _, sub_all = scanner.scan_htf(df_3) if len(df_3) >= 2 else (None, [])
            zcache[ck_sub] = sub_all or []

        open_trade = None

        for cz in cas_zones:
            zh = float(cz["zone_high"])
            zl = float(cz["zone_low"])
            if (zh - zl) < sl_buf:
                continue

            ltf_in = [
                e for e in (sub_all or [])
                if e.get("status") in ("TRAPPED", "CLOSED")
                and float(e.get("zone_high", 0)) <= zh * 1.02
                and float(e.get("zone_low",  0)) >= zl * 0.98
            ]
            if not ltf_in:
                print(f"  {trade_date} {opt_type} {strike}: 15m {zl:.0f}-{zh:.0f} → no 3m sub-zone")
                continue

            ltf_in.sort(key=lambda e: float(e.get("zone_low", 9999)))
            max_idx = max_ltf if max_ltf > 0 else len(ltf_in)
            added   = 0

            for idx, sz in enumerate(ltf_in[:max_idx]):
                sz_low  = float(sz["zone_low"])
                sz_high = float(sz["zone_high"])
                if (sz_high - sz_low) < sl_buf:
                    continue

                trap_ts = pd.to_datetime(
                    sz.get("closed_on") or sz.get("trapped_on") or sz.get("ref_ts") or "NaT"
                )
                if trap_ts is pd.NaT:
                    continue
                if hasattr(trap_ts, "tzinfo") and trap_ts.tzinfo:
                    trap_ts = trap_ts.tz_localize(None)

                search = opt_1m[
                    (opt_1m["datetime"] > trap_ts) &
                    (opt_1m["datetime"] >= entry_open_ts)
                ]
                breakout = search[search["high"] > sz_high]
                if breakout.empty:
                    continue

                entry_ts_bar = breakout.iloc[0]["datetime"]
                entry_price  = float(breakout.iloc[0]["close"])
                if entry_price <= 0:
                    continue
                if open_trade is not None and entry_ts_bar <= open_trade["exit_ts"]:
                    continue

                trap_pos = f"LTF-{idx+1}"
                ts_str   = entry_ts_bar.strftime("%H:%M")
                print(f"  {trade_date} {opt_type} {strike}: 15m→3m {sz_low:.0f}-{sz_high:.0f} "
                      f"→ 1m breakout @ {ts_str}  entry={entry_price:.1f}  [{trap_pos}]")

                # OPP_SIGNAL: close PE when first CE fired; CE runs first so opp_pe_ts stays None
                opp_ts = opp_ce_ts if opt_type == "PE" else opp_pe_ts

                exit_info = _simulate_exit(
                    opt_1m         = opt_1m,
                    entry_ts       = entry_ts_bar,
                    entry_price    = entry_price,
                    zone_low       = sz_low,   # sub-zone low → SL anchor
                    zone_high      = zh,        # PARENT 15m zone high → T1 target (above entry)
                    sl_buf         = sl_buf,
                    trade_date     = trade_date,
                    total_qty      = total_qty,
                    opp_signal_ts  = opp_ts,
                )

                trade = {
                    "date":      str(trade_date),
                    "opt_type":  opt_type,
                    "strike":    strike,
                    "expiry":    expiry,
                    "trap_pos":  trap_pos,
                    "entry_ts":  str(entry_ts_bar)[:16],
                    "entry":     round(entry_price, 2),
                    "sub_zone":  f"{sz_low:.0f}-{sz_high:.0f}",
                    "t1":        round(zh, 2),        # parent 15m zone high
                    "sl":        round(sz_low - sl_buf, 2),
                    "exit":      exit_info["exit"],
                    "exit_ts":   str(exit_info["exit_ts"])[:16],
                    "reason":    exit_info["reason"],
                    "t1_hit":    exit_info["t1_hit"],
                    "pnl_rs":    exit_info["pnl_rs"],
                }
                all_trades.append(trade)
                open_trade = exit_info

                # CE entry → mark opp_ce_ts so PE iteration closes on it
                if opt_type == "CE" and opp_ce_ts is None:
                    opp_ce_ts = entry_ts_bar
                elif opt_type == "PE" and opp_pe_ts is None:
                    opp_pe_ts = entry_ts_bar

                added += 1

            if added:
                print(f"  {trade_date} {opt_type} {strike}: ×{added}/{len(ltf_in[:max_idx])} sub-zones")

    return all_trades


# ── Main backtest ──────────────────────────────────────────────────────────────
def run_csv_backtest(
    paths:   list[str],
    sl_buf:  float = 5.0,
    lots:    int   = 1,
    max_ltf: int   = 10,
) -> dict:
    print(f"\nLoading CSV data...")
    df_all = load_nifty_monthly(paths)
    if df_all.empty:
        return {"ok": False, "error": "no NIFTY data found in CSV(s)"}

    trading_days = sorted(df_all["trade_date"].unique())
    print(f"  {len(trading_days)} trading days: {trading_days[0]} → {trading_days[-1]}")

    print(f"\n{'='*65}")
    print(f"  NIFTY CSV Backtest — Pure Intraday 15m→3m→1m")
    print(f"{'='*65}")
    print(f"  Days       : {len(trading_days)}")
    print(f"  Lots       : {lots}  (qty={lots * LOT_SIZE})")
    print(f"  SL Buffer  : {sl_buf} pts")
    print(f"  Max LTF    : {max_ltf}")
    print(f"  Expiry     : Monthly (last Thursday of month)")
    print(f"  Strike     : ATM±200 ITM (Near)")
    print(f"{'='*65}\n")

    zcache: dict  = {}
    all_trades: list[dict] = []

    for td in trading_days:
        day_trades = _run_day_csv(
            trade_date  = td,
            df_all      = df_all,
            sl_buf      = sl_buf,
            lots        = lots,
            max_ltf     = max_ltf,
            zcache      = zcache,
        )
        all_trades.extend(day_trades)

    return {"ok": True, "trades": all_trades}


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="NIFTY backtest from local CSV option data (GFDL format)"
    )
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--file",  help="Single CSV file path")
    grp.add_argument("--dir",   help="Directory containing daily CSV files")
    ap.add_argument("--sl-buf",  type=float, default=5.0,  help="SL buffer pts (default: 5)")
    ap.add_argument("--lots",    type=int,   default=1,     help="Lots (default: 1)")
    ap.add_argument("--max-ltf", type=int,   default=10,    help="Max LTF sub-zone index (default: 10)")
    args = ap.parse_args()

    if args.file:
        paths = [args.file]
    else:
        d = Path(args.dir)
        paths = sorted(str(p) for p in d.glob("*.csv"))
        if not paths:
            print(f"No CSV files found in {args.dir}")
            sys.exit(1)
        print(f"Found {len(paths)} CSV files in {args.dir}")

    result = run_csv_backtest(
        paths   = paths,
        sl_buf  = args.sl_buf,
        lots    = args.lots,
        max_ltf = args.max_ltf,
    )

    if not result["ok"]:
        print(f"\nERROR: {result['error']}")
        sys.exit(1)

    trades = result.get("trades", [])
    if not trades:
        print("\nNo trades found.")
        sys.exit(0)

    wins   = [t for t in trades if t["pnl_rs"] > 0]
    losses = [t for t in trades if t["pnl_rs"] <= 0]
    gw     = sum(t["pnl_rs"] for t in wins)
    gl     = abs(sum(t["pnl_rs"] for t in losses))
    total  = gw - gl
    pf     = round(gw / gl, 2) if gl > 0 else (99.0 if gw > 0 else 0.0)
    wr     = round(100 * len(wins) / len(trades), 1) if trades else 0

    print(f"\n{'─'*90}")
    print(f"NIFTY CSV  Trades={len(trades)}  Win={wr}%  Rs {total:+,}  PF={pf}")
    print(f"{'─'*90}")
    print(f"  {'Date':<12} {'Opt':<4} {'Strike':<7} {'LTF':<7} {'Time':<6} "
          f"{'Entry':>7} {'SubZone':>11} {'T1':>6} {'SL':>6} {'Exit':>7} {'Reason':<12} {'P&L':>9}")
    print(f"  {'─'*96}")
    for t in trades:
        ts = t["entry_ts"][11:16] if len(t["entry_ts"]) > 10 else t["entry_ts"]
        print(f"  {t['date']:<12} {t['opt_type']:<4} {t['strike']:<7} "
              f"{t['trap_pos']:<7} {ts:<6} "
              f"{t['entry']:>7.1f} {t['sub_zone']:>11} {t['t1']:>6.0f} {t['sl']:>6.0f} "
              f"{t['exit']:>7.1f} {t['reason']:<12} ₹{t['pnl_rs']:>+8,}")
    print(f"\n{'='*65}")
    print(f"  Total P&L    : ₹{total:+,}")
    print(f"  Win Rate     : {len(wins)}/{len(trades)}  ({wr}%)")
    print(f"  Profit Factor: {pf}")
    if wins:   print(f"  Avg Win      : ₹{round(gw/len(wins)):+,}")
    if losses: print(f"  Avg Loss     : ₹{-round(gl/len(losses)):,}")
    print(f"  Lot size used: {LOT_SIZE}")
    print(f"{'='*65}\n")
