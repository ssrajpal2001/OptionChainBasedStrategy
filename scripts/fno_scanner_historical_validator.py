"""
FnO Stock Trap Scanner — Historical validator (daily TF)
=========================================================
For a given set of past dates, run the same D1 trap scan that the nightly
scanner runs, then check what happened on the next trading day.

Usage:
  python scripts/fno_scanner_historical_validator.py
  python scripts/fno_scanner_historical_validator.py --n-days 15
  python scripts/fno_scanner_historical_validator.py --dates 2026-07-01,2026-07-02,2026-07-03
  python scripts/fno_scanner_historical_validator.py --from-date 2026-06-01 --to-date 2026-07-10
"""
from __future__ import annotations

import os, sys, json, sqlite3, argparse, time
from datetime import date, datetime, timedelta
from urllib.parse import quote as _quote
from typing import Optional

import pandas as pd
import requests

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from strategies.trap_scanner import scanner
from scripts.fno_stock_scanner import (
    FNO_LIST_PATH, DB_PATH, UPSTOX_BASE, NIFTY_KEY,
    NIFTY_BIAS_PROXIMITY_PCT, STOCK_ZONE_PROXIMITY_PCT,
    SL_BUFFER_PCT, MIN_RR, MAX_ZONE_AGE_DAYS, TOP_N_PER_DIRECTION,
    _compute_rr, _in_proximity, _classify_today_touch,
    _approaching, _pick_nifty_bias, _merge_cluster, _score_zone,
)

# ── Token ─────────────────────────────────────────────────────────────────────

def _get_token() -> str:
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT access_token FROM system_feeder_creds WHERE provider='upstox' LIMIT 1"
        ).fetchone()
        conn.close()
        return (row[0] or "") if row else ""
    except Exception:
        return ""

# ── History fetch (works for any date range) ──────────────────────────────────

def _hdr(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def _fetch_history(instrument_key: str, token: str,
                   from_dt: date, to_dt: date) -> pd.DataFrame:
    """Fetch daily OHLCV bars from from_dt to to_dt inclusive, sorted ascending."""
    enc = _quote(instrument_key, safe="")
    url = f"{UPSTOX_BASE}/historical-candle/{enc}/day/{to_dt}/{from_dt}"
    try:
        r = requests.get(url, headers=_hdr(token), timeout=15)
        time.sleep(0.1)
        if r.status_code != 200:
            # print(f"  [WARN] {instrument_key} HTTP {r.status_code}")
            return pd.DataFrame()
        candles = r.json().get("data", {}).get("candles", [])
        rows = [
            {"datetime": c[0][:10], "open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4])}
            for c in reversed(candles)
        ]
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception as exc:
        # print(f"  [WARN] {instrument_key} fetch error: {exc}")
        return pd.DataFrame()


# ── Historical variants of build_result ───────────────────────────────────────

def _zone_age_days_as_of(trapped_on: str, as_of: date) -> int:
    if not trapped_on:
        return 0
    try:
        d = date.fromisoformat(str(trapped_on)[:10])
        return (as_of - d).days
    except Exception:
        return 0


def _build_result_as_of(symbol: str, lot_size: int, strike_step: int,
                        last_close: float, today_high: float, today_low: float,
                        direction: str, best: dict,
                        all_zones: list, stock_prox_pct: float,
                        min_rr: float, as_of: date) -> Optional[dict]:
    """Same as _build_result but ages zones relative to as_of."""
    zh, zl, best = _merge_cluster(best, all_zones)

    age = _zone_age_days_as_of(best.get("trapped_on", ""), as_of)
    if age > MAX_ZONE_AGE_DAYS:
        return None

    min_width = last_close * 0.005
    if zh - zl < min_width:
        return None

    if not _approaching(last_close, zl, zh, direction, stock_prox_pct):
        return None

    touch = _classify_today_touch(last_close, today_high, today_low, zl, zh, direction)
    if touch["broken_today"]:
        return None

    if direction == "CE":
        sl = round(zl * (1 - SL_BUFFER_PCT / 100), 2)
        t1 = round(best.get("sl", zh * 1.05), 2)
        if last_close >= t1:
            return None
        plan_entry = zh
    else:
        sl = round(zh * (1 + SL_BUFFER_PCT / 100), 2)
        t1 = round(best.get("sl", zl * 0.95), 2)
        if last_close <= t1:
            return None
        plan_entry = zl

    rr = _compute_rr(entry=plan_entry, sl=sl, t1=t1, direction=direction)
    if rr["rr_ratio"] < min_rr:
        return None

    if zl <= last_close <= zh:
        zone_dist_pct = 0.0
    elif last_close > zh:
        zone_dist_pct = round((last_close - zh) / zh * 100, 2)
    else:
        zone_dist_pct = round((zl - last_close) / zl * 100, 2)

    plan_entry_dist_pct = round(abs(last_close - plan_entry) / plan_entry * 100, 2) if plan_entry else 0.0

    zone_tests = sum(
        1 for z in all_zones
        if abs(z.get("zone_low", 0) - zl) < strike_step and z.get("status") == "CLOSED"
    )

    atm = round(last_close / strike_step) * strike_step
    suggested_strike = (atm - strike_step) if direction == "CE" else (atm + strike_step)

    if touch["bounced_today"]:
        plan = "Zone tested & bounced. Enter on 5m/15m pullback to zone."
    elif touch["inside_today"]:
        plan = "Price inside zone at close. Watch first 15 min for hold/reversal."
    elif zone_dist_pct == 0.0:
        plan = "Price at zone. Enter on confirmed reversal candle."
    else:
        plan = f"Wait for price to retest {plan_entry:.1f}-{zl:.1f} zone."

    return {
        "symbol": symbol,
        "direction": direction,
        "zone_high": round(zh, 2),
        "zone_low": round(zl, 2),
        "last_close": round(last_close, 2),
        "entry_plan_price": round(plan_entry, 2),
        "zone_distance_pct": zone_dist_pct,
        "plan_entry_dist_pct": plan_entry_dist_pct,
        "stock_sl": sl,
        "stock_t1": t1,
        "risk_pts": rr["risk_pts"],
        "reward_pts": rr["reward_pts"],
        "rr_ratio": rr["rr_ratio"],
        "suggested_strike": suggested_strike,
        "strike_step": strike_step,
        "lot_size": lot_size,
        "zone_age_days": age,
        "zone_date": best.get("trapped_on", ""),
        "zone_tests": zone_tests,
        "touch_status": touch["touch_status"],
        "tested_today": touch["tested_today"],
        "bounced_today": touch["bounced_today"],
        "inside_today": touch["inside_today"],
        "tomorrow_plan": plan,
    }


def _scan_stock_as_of(symbol: str, upstox_key: str, lot_size: int,
                      strike_step: int, full_df: pd.DataFrame,
                      as_of: date, bias: Optional[str] = None,
                      stock_prox_pct: float = STOCK_ZONE_PROXIMITY_PCT,
                      min_rr: float = MIN_RR) -> list:
    """Run the nightly scan logic for one stock as-of a historical date."""
    df = full_df[full_df["datetime"] <= as_of.isoformat()].copy()
    if df.empty or len(df) < 5:
        return []
    last_close = float(df.iloc[-1]["close"])
    today_high = float(df.iloc[-1]["high"])
    today_low = float(df.iloc[-1]["low"])
    _, all_zones = scanner.scan_htf_spot(df)

    directions = [bias] if bias else ["CE", "PE"]
    results = []
    for direction in directions:
        kind = "BEAR" if direction == "CE" else "BULL"
        zones = [z for z in all_zones if z.get("kind") == kind and z.get("status") == "TRAPPED"]
        if not zones:
            continue
        candidates = []
        for candidate in zones:
            r = _build_result_as_of(symbol, lot_size, strike_step,
                                    last_close, today_high, today_low,
                                    direction, candidate, all_zones,
                                    stock_prox_pct, min_rr, as_of)
            if not r:
                continue
            r["_score"] = _score_zone(r)
            candidates.append(r)
        if not candidates:
            continue
        best_r = max(candidates, key=lambda x: x["_score"])
        del best_r["_score"]
        results.append(best_r)
    return results


# ── Next-day evaluation ───────────────────────────────────────────────────────

def _evaluate_next_day(r: dict, full_df: pd.DataFrame, as_of: date) -> Optional[dict]:
    """Check how the selected stock performed on the next trading day."""
    next_df = full_df[full_df["datetime"] > as_of.isoformat()]
    if next_df.empty:
        return None
    next_row = next_df.iloc[0]
    next_date = next_row["datetime"]
    o = float(next_row["open"])
    h = float(next_row["high"])
    l = float(next_row["low"])
    c = float(next_row["close"])

    direction = r["direction"]
    entry = r["entry_plan_price"]
    sl = r["stock_sl"]
    t1 = r["stock_t1"]

    if direction == "CE":
        correct_dir = int(c > entry)
        sl_hit = int(l <= sl)
        t1_hit = int(h >= t1)
        mfe = round(h - entry, 2)   # max favourable excursion
        mae = round(entry - l, 2)   # max adverse excursion
        close_pnl = round(c - entry, 2)
    else:
        correct_dir = int(c < entry)
        sl_hit = int(h >= sl)
        t1_hit = int(l <= t1)
        mfe = round(entry - l, 2)
        mae = round(h - entry, 2)
        close_pnl = round(entry - c, 2)

    return {
        "next_date": next_date,
        "next_open": o,
        "next_high": h,
        "next_low": l,
        "next_close": c,
        "correct_dir": correct_dir,
        "sl_hit": sl_hit,
        "t1_hit": t1_hit,
        "mfe_pts": mfe,
        "mae_pts": mae,
        "close_pnl_pts": close_pnl,
    }


# ── Main validation runner ────────────────────────────────────────────────────

def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def run_validation(token: str,
                   dates: Optional[list[date]] = None,
                   n_days: int = 10,
                   use_nifty_bias: bool = False,
                   output_dir: str = "data") -> pd.DataFrame:
    """Run historical validation."""

    if not dates:
        # Need enough buffer to have a next trading day after the last scanned date.
        buffer_days = 5
        # Fetch recent NIFTY history to discover trading days.
        to_dt = date.today()
        from_dt = to_dt - timedelta(days=n_days + buffer_days + 30)
        print("Fetching NIFTY history to discover trading days...")
        nifty_df = _fetch_history(NIFTY_KEY, token, from_dt, to_dt)
        if nifty_df.empty:
            raise RuntimeError("Could not fetch NIFTY history")
        all_trading_days = [date.fromisoformat(d) for d in nifty_df["datetime"].tolist()]
        # Use last n_days that also have a next day in the data.
        dates = all_trading_days[-(n_days + 1):-1]
        print(f"Validating on dates: {[d.isoformat() for d in dates]}")

    if not dates:
        raise RuntimeError("No dates to validate")

    from_dt = min(dates) - timedelta(days=45)
    to_dt = max(dates) + timedelta(days=5)

    print("Fetching NIFTY history...")
    nifty_df = _fetch_history(NIFTY_KEY, token, from_dt, to_dt)
    if nifty_df.empty or len(nifty_df) < 5:
        raise RuntimeError("Could not fetch NIFTY history")

    stocks_df = pd.read_csv(FNO_LIST_PATH)
    stock_histories = {}
    print(f"Fetching {len(stocks_df)} stock histories...")
    for idx, row in stocks_df.iterrows():
        sym = row["symbol"]
        df = _fetch_history(row["upstox_key"], token, from_dt, to_dt)
        if not df.empty:
            stock_histories[sym] = df
        if (idx + 1) % 50 == 0:
            print(f"  {idx + 1}/{len(stocks_df)} fetched")

    records = []
    for as_of in dates:
        print(f"\n--- as-of {as_of} ---")
        nifty_row = nifty_df[nifty_df["datetime"] == as_of.isoformat()]
        if nifty_row.empty:
            print(f"  NIFTY data missing for {as_of}, skipping")
            continue
        nifty_close = float(nifty_row.iloc[-1]["close"])
        _, nifty_all_zones = scanner.scan_htf_spot(nifty_df[nifty_df["datetime"] <= as_of.isoformat()])
        nifty_bias, nifty_zone = _pick_nifty_bias(nifty_close, nifty_all_zones, NIFTY_BIAS_PROXIMITY_PCT)
        print(f"  NIFTY close={nifty_close:.2f} bias={nifty_bias} "
              f"zone={nifty_zone.get('zone_low', 0):.2f}-{nifty_zone.get('zone_high', 0):.2f}")

        all_results = []
        for _, row in stocks_df.iterrows():
            sym = row["symbol"]
            if sym not in stock_histories:
                continue
            df = stock_histories[sym]
            b = nifty_bias if use_nifty_bias else None
            res_list = _scan_stock_as_of(
                symbol=sym, upstox_key=row["upstox_key"],
                lot_size=int(row["lot_size"]), strike_step=int(row["strike_step"]),
                full_df=df, as_of=as_of, bias=b,
                stock_prox_pct=STOCK_ZONE_PROXIMITY_PCT, min_rr=MIN_RR,
            )
            for r in res_list:
                r["nifty_bias"] = nifty_bias
                r["nifty_close"] = nifty_close
            all_results.extend(res_list)

        ce = sorted([r for r in all_results if r["direction"] == "CE"],
                    key=lambda x: x["rr_ratio"], reverse=True)[:TOP_N_PER_DIRECTION]
        pe = sorted([r for r in all_results if r["direction"] == "PE"],
                    key=lambda x: x["rr_ratio"], reverse=True)[:TOP_N_PER_DIRECTION]

        for r in ce + pe:
            sym = r["symbol"]
            df = stock_histories.get(sym)
            ev = _evaluate_next_day(r, df, as_of) if df is not None else None
            if ev is None:
                continue
            records.append({
                "as_of_date": as_of.isoformat(),
                "symbol": sym,
                "direction": r["direction"],
                "ltp": r["last_close"],
                "entry_plan": r["entry_plan_price"],
                "sl": r["stock_sl"],
                "t1": r["stock_t1"],
                "rr_ratio": r["rr_ratio"],
                "zone_low": r["zone_low"],
                "zone_high": r["zone_high"],
                "touch_status": r["touch_status"],
                "zone_age_days": r["zone_age_days"],
                "zone_distance_pct": r["zone_distance_pct"],
                "plan_entry_dist_pct": r["plan_entry_dist_pct"],
                "suggested_strike": r["suggested_strike"],
                "lot_size": r["lot_size"],
                "nifty_bias": r["nifty_bias"],
                **ev,
            })

    df = pd.DataFrame(records)
    if df.empty:
        print("No records generated")
        return df

    suffix = "_niftybias" if use_nifty_bias else ""
    out_csv = os.path.join(output_dir, f"fno_scanner_validation_{min(dates)}_to_{max(dates)}{suffix}.csv")
    df.to_csv(out_csv, index=False)
    print(f"\nDetailed report written: {out_csv}")

    # Aggregate stats
    print("\n=== Aggregate next-day stats ===")
    for direction in ["CE", "PE", "ALL"]:
        sub = df if direction == "ALL" else df[df["direction"] == direction]
        if sub.empty:
            continue
        n = len(sub)
        correct = sub["correct_dir"].sum()
        sl = sub["sl_hit"].sum()
        t1 = sub["t1_hit"].sum()
        avg_mfe = sub["mfe_pts"].mean()
        avg_mae = sub["mae_pts"].mean()
        avg_pnl = sub["close_pnl_pts"].mean()
        print(f"{direction:4} | n={n:3} | correct={correct}/{n} ({100*correct/n:.1f}%) | "
              f"SL hit={sl}/{n} ({100*sl/n:.1f}%) | T1 hit={t1}/{n} ({100*t1/n:.1f}%) | "
              f"avg MFE={avg_mfe:.2f} | avg MAE={avg_mae:.2f} | avg close P/L={avg_pnl:.2f}")

    # Per-date stats
    print("\n=== Per-date correct-direction rate ===")
    for d, sub in df.groupby("as_of_date"):
        n = len(sub)
        correct = sub["correct_dir"].sum()
        print(f"{d} | n={n:2} | correct={correct}/{n} ({100*correct/n:.1f}%)")

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Historical validator for FnO stock trap scanner")
    parser.add_argument("--n-days", type=int, default=10, help="Number of recent trading days to validate")
    parser.add_argument("--dates", type=str, default=None, help="Comma-separated list of YYYY-MM-DD dates")
    parser.add_argument("--from-date", type=str, default=None, help="Start date (used with --to-date)")
    parser.add_argument("--to-date", type=str, default=None, help="End date (used with --from-date)")
    parser.add_argument("--use-nifty-bias", action="store_true", help="Filter to NIFTY bias direction only")
    args = parser.parse_args()

    token = _get_token()
    if not token:
        print("[ERROR] No Upstox token found in data/clients.db")
        sys.exit(1)

    dates = None
    if args.dates:
        dates = [_parse_date(d.strip()) for d in args.dates.split(",")]
    elif args.from_date and args.to_date:
        from_d = _parse_date(args.from_date)
        to_d = _parse_date(args.to_date)
        dates = [from_d + timedelta(days=i) for i in range((to_d - from_d).days + 1)]

    run_validation(token, dates=dates, n_days=args.n_days, use_nifty_bias=args.use_nifty_bias)
