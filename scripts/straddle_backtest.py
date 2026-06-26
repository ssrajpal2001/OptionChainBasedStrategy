#!/usr/bin/env python3
"""
Sell Straddle backtest -- NIFTY with monthly expiry (default 30JUN26).

Simulates the exact NIFTY config (indices.NIFTY.sell_straddle):
  Entry-beginning : SLOPE(2m) < 0
  Entry-reentry   : CLOSE(2m) < VWAP(2m) AND SLOPE(2m) < 0
  Exit rules      : SLOPE(1m)>0 AND RSI(3m)>55 AND ROC(2m)>10 AND CLOSE(1m)>VWAP(1m)
  Position exits  : profit_pct=50, sl_pct=30, trailing_sl(12.5%/20%), scalable TSL
  Day guardrails  : Mon 50%/30%, Tue 75%/30%, Wed 12.5%/30%, Thu 15%/30%, Fri 20%/30%

Usage:
  python scripts/straddle_backtest.py --token TOKEN [--start 2026-05-27] [--end 2026-06-26] [--fixed-expiry 30JUN26]
"""
import sys, os, argparse, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date, timedelta
from urllib.parse import quote

import numpy as np
import pandas as pd

# ── NIFTY config (from the provided JSON, indices.NIFTY.sell_straddle) ────────
LOT_SIZE        = 65
LOT_MULTI       = 1.0
LTP_TARGET      = 50.0      # min LTP per leg
STEP            = 50        # strike step
ENTRY_START_T   = "09:15"
ENTRY_END_T     = "15:00"
SQOFF_T         = "15:15"
PROFIT_PCT      = 50.0      # position profit target (premium decayed 50%)
SL_PCT          = 30.0      # position hard SL (premium rose 30%)
TRAIL_LOCK_PCT  = 12.5      # trailing SL arms at this profit %
TRAIL_FLOOR_PCT = 20.0      # trailing SL: exit if profit drops 20pts% from peak
COOLDOWN_MIN    = 5
MAX_TRADES      = 10
INDEX           = "NIFTY"
SPOT_KEY        = "NSE_INDEX|Nifty 50"

# Scalable TSL thresholds (Rs)
TSL_BASE_PROFIT = 1_000.0
TSL_BASE_LOCK   = 250.0
TSL_STEP_PROFIT = 250.0
TSL_STEP_LOCK   = 250.0

# Per-day day-level guardrails (target%, sl%)
PER_DAY = {
    "monday":    (50.0,   30.0),
    "tuesday":   (75.0,   30.0),
    "wednesday": (12.5,   30.0),
    "thursday":  (15.0,   30.0),
    "friday":    (20.0,   30.0),
}

_HEADERS: dict = {}


# ── Fetch helpers ─────────────────────────────────────────────────────────────
def _fetch_chunk(key: str, from_dt: str, to_dt: str) -> pd.DataFrame:
    import requests
    enc = quote(key, safe="")
    url = f"https://api.upstox.com/v2/historical-candle/{enc}/1minute/{to_dt}/{from_dt}"
    r = requests.get(url, headers=_HEADERS, timeout=30)
    r.raise_for_status()
    cs = r.json().get("data", {}).get("candles", [])
    if not cs:
        return pd.DataFrame()
    rows = [{"datetime": pd.to_datetime(c[0]),
             "open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4]),
             "volume": int(c[5])} for c in reversed(cs)]
    df = pd.DataFrame(rows)
    df["datetime"] = df["datetime"].dt.tz_localize(None)
    return df


def _fetch(key: str, start: date, end: date) -> pd.DataFrame:
    chunks, cur = [], start
    while cur <= end:
        nxt = min(cur + timedelta(days=28), end)
        try:
            ch = _fetch_chunk(key, cur.isoformat(), nxt.isoformat())
            if not ch.empty:
                chunks.append(ch)
            time.sleep(0.15)
        except Exception as e:
            print(f"  WARN {key[:50]} {cur}: {e}")
        cur = nxt + timedelta(days=1)
    if not chunks:
        return pd.DataFrame()
    return (pd.concat(chunks)
            .sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True))


def _mkt(df: pd.DataFrame) -> pd.DataFrame:
    return df[(df["datetime"].dt.time >= pd.Timestamp("09:15").time()) &
              (df["datetime"].dt.time <= pd.Timestamp("15:30").time())]


# ── Option key lookup via REGISTRY ───────────────────────────────────────────
def _opt_key(fixed_expiry: str, strike: int, opt_type: str) -> str:
    _M = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
          "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
    fe = fixed_expiry.strip().upper()
    try:
        exp = date(2000 + int(fe[5:7]), _M[fe[2:5]], int(fe[:2]))
        from data_layer.instrument_registry import REGISTRY
        if REGISTRY.is_loaded(INDEX):
            k = REGISTRY.get_upstox_key(INDEX, exp, strike, opt_type)
            if k:
                return k
    except Exception:
        pass
    return f"NSE_FO|{INDEX}{fe}{strike}{opt_type}"


# ── Indicator computation ─────────────────────────────────────────────────────
def _vwap(df: pd.DataFrame) -> pd.Series:
    """Cumulative intraday VWAP (typical price weighted by volume)."""
    tp  = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    vol = df["volume"].clip(lower=1)
    return (tp * vol).cumsum() / vol.cumsum()


def _rsi(s: pd.Series, period: int = 14) -> pd.Series:
    d = s.diff()
    g = d.clip(lower=0).ewm(com=period-1, min_periods=period).mean()
    l = (-d).clip(lower=0).ewm(com=period-1, min_periods=period).mean()
    rs = g / l.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(50)


def _build_indicators(df1m: pd.DataFrame) -> pd.DataFrame:
    """Compute 1m/2m/3m indicators for the combined (CE+PE) premium series."""
    df = df1m.copy().set_index("datetime")

    # 1-min VWAP and SLOPE
    df["vwap_1m"]  = _vwap(df.reset_index()).values
    df["slope_1m"] = df["vwap_1m"].diff()

    # 2-min resampled: VWAP, SLOPE, ROC(10), CLOSE
    df2 = df.resample("2min").agg(
        {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}
    ).dropna(subset=["close"])
    df2["vwap_2m"]  = _vwap(df2.reset_index()).values
    df2["slope_2m"] = df2["vwap_2m"].diff()
    df2["roc_2m"]   = df2["close"].pct_change(10) * 100

    # 3-min RSI(14)
    df3 = df.resample("3min").agg(
        {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}
    ).dropna(subset=["close"])
    df3["rsi_3m"] = _rsi(df3["close"])

    # Forward-fill tf indicators back to 1-min index
    df["slope_2m"] = df2["slope_2m"].reindex(df.index, method="ffill")
    df["vwap_2m"]  = df2["vwap_2m"].reindex(df.index, method="ffill")
    df["rsi_3m"]   = df3["rsi_3m"].reindex(df.index, method="ffill")
    df["roc_2m"]   = df2["roc_2m"].reindex(df.index, method="ffill")

    return df.reset_index()


# ── Per-day simulation ────────────────────────────────────────────────────────
def _sim_day(dt: date, ce_df: pd.DataFrame, pe_df: pd.DataFrame) -> list:
    """Simulate one trading day. Returns list of trade event dicts."""
    if ce_df.empty or pe_df.empty:
        return []

    # Align on common timestamps
    ce_d = ce_df.set_index("datetime")
    pe_d = pe_df.set_index("datetime")
    idx  = ce_d.index.intersection(pe_d.index)
    if len(idx) < 10:
        return []
    ce_d = ce_d.reindex(idx)
    pe_d = pe_d.reindex(idx)

    # Combined premium series
    combo = ce_d.copy()
    for col in ("open", "high", "low", "close"):
        combo[col] = ce_d[col] + pe_d[col]
    combo["volume"] = ce_d["volume"] + pe_d["volume"]
    combo = combo.reset_index()

    ind = _build_indicators(combo).set_index("datetime")

    weekday  = dt.strftime("%A").lower()
    day_tgt, day_sl_pct = PER_DAY.get(weekday, (30.0, 30.0))
    qty = int(LOT_SIZE * LOT_MULTI)

    pos            = None
    trades         = []
    day_pnl_pts    = 0.0
    first_credit   = None
    trades_today   = 0
    stop_for_day   = False
    cooldown_until = None
    last_2m_bucket = ""

    for ts, row in ind.iterrows():
        t = ts.strftime("%H:%M")

        # EOD force exit
        if t >= SQOFF_T and pos:
            pnl = pos["entry"] - row["close"]
            day_pnl_pts += pnl
            trades.append({"date": dt, "entry_ts": pos["ts"], "exit_ts": ts,
                           "entry": pos["entry"], "exit": row["close"],
                           "pnl_pts": pnl, "pnl_rs": pnl * qty, "reason": "eod"})
            pos = None
            stop_for_day = True

        if stop_for_day or t >= SQOFF_T:
            continue

        combined = row["close"]
        ce_ltp   = float(ce_d["close"].get(ts, 0) or 0)
        pe_ltp   = float(pe_d["close"].get(ts, 0) or 0)

        # ── EXIT CHECKS ──────────────────────────────────────────────────────
        if pos:
            pnl_pts = pos["entry"] - combined
            pct     = pnl_pts / pos["entry"] * 100 if pos["entry"] > 0 else 0
            pnl_rs  = pnl_pts * qty
            reason  = None

            # 1. Hard SL: premium rose 30% above entry
            if combined > pos["entry"] * (1 + SL_PCT / 100):
                reason = "hard_sl"

            # 2. Profit target: premium decayed 50%
            elif pct >= PROFIT_PCT:
                reason = "profit_target"

            else:
                # 3. Trailing SL (12.5% lock, 20% floor)
                if pct > pos["peak_pct"]:
                    pos["peak_pct"] = pct
                if (pos["peak_pct"] >= TRAIL_LOCK_PCT and
                        pct <= pos["peak_pct"] - TRAIL_FLOOR_PCT):
                    reason = "trailing_sl"

                # 4. Scalable TSL (Rs staircase)
                if not reason:
                    if pnl_rs >= TSL_BASE_PROFIT:
                        lock = (TSL_BASE_LOCK
                                + int((pnl_rs - TSL_BASE_PROFIT) / TSL_STEP_PROFIT)
                                * TSL_STEP_LOCK)
                        if lock > pos["tsl_lock"]:
                            pos["tsl_lock"] = lock
                    if pos["tsl_lock"] > 0 and pnl_rs < pos["tsl_lock"]:
                        reason = "scalable_tsl"

                # 5. Day-level guardrails
                if not reason and first_credit and first_credit > 0:
                    total_day_pct = (day_pnl_pts + pnl_pts) / first_credit * 100
                    if day_tgt > 0 and total_day_pct >= day_tgt:
                        reason = "day_profit_target"
                    elif day_sl_pct > 0 and total_day_pct <= -day_sl_pct:
                        reason = "day_loss_sl"

                # 6. Exit rules: SLOPE(1m)>0 AND RSI(3m)>55 AND ROC(2m)>10 AND CLOSE>VWAP(1m)
                if not reason:
                    s1  = float(row.get("slope_1m") or 0)
                    r3  = float(row.get("rsi_3m")   or 50)
                    rc2 = float(row.get("roc_2m")   or 0)
                    v1  = float(row.get("vwap_1m")  or combined)
                    if s1 > 0 and r3 > 55 and rc2 > 10 and combined > v1:
                        reason = "exit_rules"

            if reason:
                day_pnl_pts += pnl_pts
                trades.append({"date": dt, "entry_ts": pos["ts"], "exit_ts": ts,
                               "entry": pos["entry"], "exit": combined,
                               "pnl_pts": pnl_pts, "pnl_rs": pnl_rs, "reason": reason})
                pos = None
                if reason != "day_loss_sl":
                    cooldown_until = ts + pd.Timedelta(minutes=COOLDOWN_MIN)
                if reason in ("day_profit_target", "day_loss_sl"):
                    stop_for_day = True
                continue

        # ── ENTRY CHECKS ─────────────────────────────────────────────────────
        if not pos:
            if stop_for_day or t < ENTRY_START_T or t > ENTRY_END_T:
                continue
            if cooldown_until and ts < cooldown_until:
                continue
            if trades_today >= MAX_TRADES:
                continue
            if ce_ltp < LTP_TARGET or pe_ltp < LTP_TARGET:
                continue

            # Gate to 2-minute boundary
            _bkt = f"{ts.strftime('%Y%m%d_%H')}{(ts.minute // 2) * 2:02d}"
            if _bkt == last_2m_bucket:
                continue
            last_2m_bucket = _bkt

            s2 = float(row.get("slope_2m") or 0)
            v2 = float(row.get("vwap_2m")  or 0)

            if trades_today == 0:
                # Beginning entry: SLOPE(2m) < 0
                if s2 >= 0:
                    continue
            else:
                # Re-entry: CLOSE < VWAP AND SLOPE(2m) < 0
                if s2 >= 0 or (v2 > 0 and combined >= v2):
                    continue

            trades_today += 1
            if first_credit is None:
                first_credit = combined
            pos = {"ts": ts, "entry": combined, "peak_pct": 0.0, "tsl_lock": 0.0}
            trades.append({"date": dt, "entry_ts": ts, "exit_ts": None,
                           "entry": combined, "exit": None,
                           "pnl_pts": None, "pnl_rs": None, "reason": "entry"})

    return trades


# ── Main backtest runner ──────────────────────────────────────────────────────
def run_straddle_backtest(token: str, start: date, end: date, fixed_expiry: str) -> dict:
    global _HEADERS
    _HEADERS = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    # Load REGISTRY for numeric option keys
    try:
        from data_layer.instrument_registry import REGISTRY
        import asyncio
        if not REGISTRY.is_loaded(INDEX):
            print("Loading REGISTRY...")
            asyncio.run(REGISTRY.load_all(token))
        exps = sorted(REGISTRY.all_expiries(INDEX))
        print(f"REGISTRY OK — {INDEX} expiries: {[e.strftime('%d%b%y').upper() for e in exps[:8]]}")
    except Exception as e:
        print(f"WARN: REGISTRY not loaded ({e})")

    # Fetch spot to get trading days
    print(f"Fetching spot {SPOT_KEY}...")
    spot_df = _fetch(SPOT_KEY, start, end)
    if spot_df.empty:
        return {"error": "no spot data"}

    trading_days = sorted(set(_mkt(spot_df)["datetime"].dt.date.tolist()))
    print(f"Trading days: {len(trading_days)}")

    # Compute ATM at 09:15 open for each day
    spot_by_day = _mkt(spot_df).groupby(spot_df["datetime"].dt.date)
    atm_by_day  = {}
    for d in trading_days:
        if d not in spot_by_day.groups:
            continue
        day_spot = spot_by_day.get_group(d)
        first_bar = day_spot[day_spot["datetime"].dt.time >= pd.Timestamp("09:15").time()].head(1)
        if first_bar.empty:
            continue
        spot_open = float(first_bar.iloc[0]["open"])
        atm_by_day[d] = round(spot_open / STEP) * STEP

    unique_atms = sorted(set(atm_by_day.values()))
    print(f"Unique ATMs: {unique_atms}")

    # Fetch all option series needed
    print(f"Fetching {len(unique_atms) * 2} option series (expiry={fixed_expiry})...")
    opt_cache: dict = {}
    for atm in unique_atms:
        for ot in ("CE", "PE"):
            key = _opt_key(fixed_expiry, atm, ot)
            print(f"  {key[:65]}...")
            df = _fetch(key, start, end)
            opt_cache[(atm, ot)] = df
            print(f"    -> {len(df)} bars")
            time.sleep(0.2)

    # Simulate each trading day
    print("\nSimulating...")
    all_trades: list = []
    for d in trading_days:
        atm = atm_by_day.get(d)
        if atm is None:
            continue
        ce_full = opt_cache.get((atm, "CE"), pd.DataFrame())
        pe_full = opt_cache.get((atm, "PE"), pd.DataFrame())

        def _day_slice(df):
            if df.empty:
                return pd.DataFrame()
            return _mkt(df[df["datetime"].dt.date == d].copy())

        ce_day = _day_slice(ce_full)
        pe_day = _day_slice(pe_full)

        trades = _sim_day(d, ce_day, pe_day)
        all_trades.extend(trades)

        closed   = [t for t in trades if t["reason"] != "entry" and t["pnl_rs"] is not None]
        day_rs   = sum(t["pnl_rs"] for t in closed)
        reasons  = [t["reason"] for t in closed]
        print(f"  {d} ({d.strftime('%a')}) ATM={atm}  trades={len(closed):2d}  "
              f"Rs {day_rs:+8,.0f}  exits={reasons}")

    # Summary
    closed = [t for t in all_trades if t["reason"] != "entry" and t["pnl_rs"] is not None]
    if not closed:
        return {"error": "no closed trades", "all_trades": all_trades}

    wins   = [t for t in closed if t["pnl_rs"] > 0]
    losses = [t for t in closed if t["pnl_rs"] <= 0]
    total  = sum(t["pnl_rs"] for t in closed)

    return {
        "total_trades": len(closed),
        "wins":         len(wins),
        "losses":       len(losses),
        "win_pct":      len(wins) / len(closed) * 100 if closed else 0,
        "total_pnl_rs": total,
        "avg_win_rs":   sum(t["pnl_rs"] for t in wins)   / len(wins)   if wins   else 0,
        "avg_loss_rs":  sum(t["pnl_rs"] for t in losses) / len(losses) if losses else 0,
        "profit_factor": (abs(sum(t["pnl_rs"] for t in wins))
                          / abs(sum(t["pnl_rs"] for t in losses) or 1)),
        "all_trades": all_trades,
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--token",        required=True)
    ap.add_argument("--start",        default="2026-05-27")
    ap.add_argument("--end",          default="2026-06-26")
    ap.add_argument("--fixed-expiry", default="30JUN26")
    args = ap.parse_args()

    result = run_straddle_backtest(
        token=args.token,
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        fixed_expiry=args.fixed_expiry,
    )

    print("\n" + "=" * 65)
    print(f"SELL STRADDLE BACKTEST  NIFTY {args.fixed_expiry}")
    print(f"  Period     : {args.start} -> {args.end}")
    print(f"  Total trades: {result.get('total_trades', 0)}")
    print(f"  Wins        : {result.get('wins', 0)}  ({result.get('win_pct', 0):.1f}%)")
    print(f"  Losses      : {result.get('losses', 0)}")
    print(f"  Total P&L   : Rs {result.get('total_pnl_rs', 0):+,.0f}")
    print(f"  Avg Win     : Rs {result.get('avg_win_rs', 0):+,.0f}")
    print(f"  Avg Loss    : Rs {result.get('avg_loss_rs', 0):+,.0f}")
    print(f"  Profit Factor: {result.get('profit_factor', 0):.2f}")
    print("=" * 65)

    print("\nPer-trade log:")
    print(f"  {'Date':<12} {'In':>5} {'Out':>5} {'Entry':>7} {'Exit':>7} {'P&L Rs':>10}  Reason")
    print("  " + "-" * 62)
    for t in result.get("all_trades", []):
        if t["reason"] == "entry" or t["pnl_rs"] is None:
            continue
        e_t = t["entry_ts"].strftime("%H:%M") if t["entry_ts"] else "?"
        x_t = t["exit_ts"].strftime("%H:%M")  if t["exit_ts"]  else "?"
        sign = "+" if t["pnl_rs"] >= 0 else ""
        print(f"  {t['date']}  {e_t}  {x_t}  {t['entry']:7.1f}  {t['exit']:7.1f}  "
              f"{sign}{t['pnl_rs']:9,.0f}  {t['reason']}")
