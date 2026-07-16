"""
scripts/nifty_nested_trap_option_backtest.py
============================================
NIFTY intraday nested-fractal trap backtest using **actual option premiums**.

Logic (same as CRUDEOIL evening version, adapted to NIFTY regular hours):
  1. HTF = 1h spot candle. If the next 1h candle breaches the prior 1h high/low,
     the traders inside that prior hour are trapped.
        - prior 1h LOW breached  -> bull trap -> SHORT -> sell CE
        - prior 1h HIGH breached -> bear trap -> LONG  -> sell PE
  2. Inside the breach 1h candle, find 15m traps in the same direction.
  3. Inside the relevant 15m candle(s), find 5m traps in the same direction.
  4. Enter when a 1m candle breaks the 5m zone trigger.
  5. SL = 5m zone extreme ± buffer, target = the breached 1h level.
  6. Square off at 15:30 if still open.

P&L is computed from real cached option premiums (nearest weekly expiry, ATM strike).
"""
from __future__ import annotations

import glob
import io
import os
import re
import sys
from datetime import date, datetime, time as dt_time, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pytz

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from strategies.trap_scanner import scanner

IST = pytz.timezone("Asia/Kolkata")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")

UNDERLYING = "NIFTY"
STRIKE_STEP = 50
LOT_SIZE = 50              # current NIFTY lot size; change if your broker uses 65
HTF_MIN = 60
MTF_MIN = 15
LTF_MIN = 5
SL_BUF = 10.0              # NIFTY is ~1/4 the point value of crude; keep tight
ENTRY_START = dt_time(9, 15)
ENTRY_END = dt_time(15, 30)
RISK_FREE_RATE = 0.06      # annual risk-free rate for Black-Scholes


def _d1(S: float, K: float, T: float, r: float, sigma: float) -> float:
    from math import log, sqrt
    return (log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt(T))


def _d2(d1: float, T: float, sigma: float) -> float:
    from math import sqrt
    return d1 - sigma * sqrt(T)


def bs_price(S: float, K: float, T: float, r: float, sigma: float, opt_type: str) -> float:
    """Black-Scholes price for European option."""
    from math import exp, sqrt
    from scipy.stats import norm
    if T <= 0 or sigma <= 0:
        return max(0.0, (S - K) if opt_type == "CE" else (K - S))
    d1 = _d1(S, K, T, r, sigma)
    d2 = _d2(d1, T, sigma)
    if opt_type == "CE":
        return S * norm.cdf(d1) - K * exp(-r * T) * norm.cdf(d2)
    return K * exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


def implied_vol(S: float, K: float, T: float, r: float, market_price: float, opt_type: str,
                tol: float = 1e-6, max_iter: int = 100) -> Optional[float]:
    """Back out implied volatility from market price using Newton-Raphson."""
    from math import sqrt
    from scipy.stats import norm
    if T <= 0 or market_price <= 0:
        return None
    sigma = 0.2  # initial guess
    for _ in range(max_iter):
        price = bs_price(S, K, T, r, sigma, opt_type)
        if price is None:
            return None
        diff = price - market_price
        if abs(diff) < tol:
            return sigma
        # vega
        d1 = _d1(S, K, T, r, sigma)
        vega = S * norm.pdf(d1) * sqrt(T)
        if vega <= 0:
            return None
        sigma -= diff / vega
        if sigma <= 1e-4:
            sigma = 1e-4
        if sigma > 2.0:
            sigma = 2.0
    return sigma


def years_to_expiry(expiry: date, ts: datetime) -> float:
    """Years from ts to expiry 15:30 IST, floored at 1 minute."""
    expiry_dt = IST.localize(datetime.combine(expiry, dt_time(15, 30)))
    if ts.tzinfo is None:
        ts = IST.localize(ts)
    secs = max((expiry_dt - ts).total_seconds(), 60.0)
    return secs / (365.25 * 24 * 3600)


def _list_option_files() -> Dict[Tuple[date, str, int], str]:
    """Map (expiry, opt_type, strike) -> parquet path by parsing filenames."""
    cache: Dict[Tuple[date, str, int], str] = {}
    weekly_re = re.compile(r"opt_NIFTY(CE|PE)(\d+)_W(\d{4}-\d{2}-\d{2})_\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}\.parquet$")
    monthly_re = re.compile(r"opt_NIFTY(CE|PE)(\d+)_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.parquet$")

    for name in os.listdir(CACHE_DIR):
        if not name.startswith("opt_NIFTY") or not name.endswith(".parquet"):
            continue
        m = weekly_re.match(name)
        if m:
            opt_type, strike, expiry = m.group(1), int(m.group(2)), date.fromisoformat(m.group(3))
            cache[(expiry, opt_type, strike)] = os.path.join(CACHE_DIR, name)
            continue
        m = monthly_re.match(name)
        if m:
            opt_type, strike, start = m.group(1), int(m.group(2)), date.fromisoformat(m.group(3))
            # Monthly files span Jun 1-30 -> assume July monthly expiry 2026-07-28
            # (matches nifty_spot_option_v2_backtest.py convention)
            if start == date(2026, 6, 1):
                expiry = date(2026, 7, 28)
            else:
                expiry = start
            cache[(expiry, opt_type, strike)] = os.path.join(CACHE_DIR, name)
    return cache


def _select_expiry(trade_date: date, opt_cache: Dict[Tuple[date, str, int], pd.DataFrame]) -> Optional[date]:
    """Pick the nearest expiry whose cached data actually covers trade_date."""
    covered = set()
    for (expiry, _, _), df in opt_cache.items():
        dates = df["datetime"].dt.date
        if dates.min() <= trade_date <= dates.max():
            covered.add(expiry)
    future = [e for e in sorted(covered) if e >= trade_date]
    return min(future) if future else None


def _load_option_cache(paths: Dict[Tuple[date, str, int], str]) -> Dict[Tuple[date, str, int], pd.DataFrame]:
    cache: Dict[Tuple[date, str, int], pd.DataFrame] = {}
    for key, path in paths.items():
        try:
            df = pd.read_parquet(path)
            df["datetime"] = pd.to_datetime(df["datetime"])
            if df["datetime"].dt.tz is not None:
                df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata")
            else:
                df["datetime"] = df["datetime"].dt.tz_localize("Asia/Kolkata")
            cache[key] = df
        except Exception as exc:
            print(f"  WARN: could not load {path}: {exc}")
    return cache


def _opt_price(df: pd.DataFrame, ts: pd.Timestamp, field: str = "close") -> Optional[float]:
    row = df[df["datetime"] == ts]
    if not row.empty:
        return float(row.iloc[0][field])
    later = df[df["datetime"] >= ts]
    if not later.empty:
        return float(later.iloc[0][field])
    return None


def _atm_strike(spot: float) -> int:
    return int(round(spot / STRIKE_STEP) * STRIKE_STEP)


def _load_spot() -> pd.DataFrame:
    files = [
        os.path.join(CACHE_DIR, "spot_NIFTY_1m_2026-06-29_2026-07-14.parquet"),
        os.path.join(CACHE_DIR, "spot_NIFTY_1m_2026-06-01_2026-06-30.parquet"),
        os.path.join(CACHE_DIR, "spot_NIFTY_1m_2026-05-25_2026-06-30.parquet"),
    ]
    frames = []
    for f in files:
        if os.path.exists(f):
            df = pd.read_parquet(f)
            df["datetime"] = pd.to_datetime(df["datetime"])
            if df["datetime"].dt.tz is not None:
                df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata")
            else:
                df["datetime"] = df["datetime"].dt.tz_localize("Asia/Kolkata")
            frames.append(df)
    if not frames:
        raise RuntimeError("No NIFTY spot cache files found")
    df = pd.concat(frames, ignore_index=True).drop_duplicates("datetime").sort_values("datetime").reset_index(drop=True)
    return df


def resample_bars(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    dfc = df.set_index("datetime")
    res = dfc.resample(f"{minutes}min").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return res


def find_htf_traps(df_htf: pd.DataFrame) -> List[dict]:
    traps = []
    for i in range(1, len(df_htf)):
        prev = df_htf.iloc[i - 1]
        curr = df_htf.iloc[i]
        if curr["high"] > prev["high"]:
            traps.append({
                "kind": "BEAR",
                "ref_ts": prev["datetime"],
                "ref_high": float(prev["high"]),
                "ref_low": float(prev["low"]),
                "breach_ts": curr["datetime"],
                "target": float(prev["high"]),
            })
        if curr["low"] < prev["low"]:
            traps.append({
                "kind": "BULL",
                "ref_ts": prev["datetime"],
                "ref_high": float(prev["high"]),
                "ref_low": float(prev["low"]),
                "breach_ts": curr["datetime"],
                "target": float(prev["low"]),
            })
    return traps


def scan_traps_in_window(df: pd.DataFrame, kind: str) -> List[dict]:
    _, entries = scanner.scan_htf_spot(df)
    return [e for e in entries if e.get("kind") == kind and e.get("status") in ("TRAPPED", "CLOSED")]


def mtf_trigger(entry: dict) -> float:
    zh = float(entry["zone_high"])
    zl = float(entry["zone_low"])
    if entry.get("kind") == "BULL":
        return round(zh - (zh - zl) / 3, 2)
    return round(zl + (zh - zl) / 3, 2)


def mtf_sl(entry: dict, buf: float = SL_BUF) -> float:
    if entry.get("kind") == "BULL":
        return round(float(entry["zone_high"]) + buf, 2)
    return round(float(entry["zone_low"]) - buf, 2)


def simulate_trade(
    kind: str,
    entry_price: float,
    sl_price: float,
    target_price: float,
    trigger_ts: datetime,
    df_1m: pd.DataFrame,
    window_end: datetime,
    opt_entry_premium: float,
    opt_df: pd.DataFrame,
    expiry: date,
    strike: int,
    opt_type: str,
) -> Optional[dict]:
    future = df_1m[df_1m["datetime"] > trigger_ts].copy()
    if future.empty:
        return None

    entry_ts = None
    entry_spot = None
    for _, row in future.iterrows():
        if row["datetime"] > window_end:
            return None
        if kind == "BULL":
            if row["low"] <= entry_price:
                entry_ts = row["datetime"]
                entry_spot = row["close"]
                break
        else:
            if row["high"] >= entry_price:
                entry_ts = row["datetime"]
                entry_spot = row["close"]
                break

    if entry_ts is None:
        return None

    # Back out implied vol from entry market premium.
    T_entry = years_to_expiry(expiry, entry_ts)
    iv = implied_vol(entry_spot, strike, T_entry, RISK_FREE_RATE, opt_entry_premium, opt_type)
    if iv is None:
        return None

    after_entry = future[future["datetime"] >= entry_ts].copy()
    exit_reason = "OPEN"
    exit_ts = None
    exit_spot = None
    exit_opt_market = None

    for _, row in after_entry.iterrows():
        bar_end = row["datetime"] + timedelta(minutes=1)
        opt_px = _opt_price(opt_df, row["datetime"], "close")

        if kind == "BULL":
            if row["high"] >= sl_price:
                exit_spot = row["close"]
                exit_opt_market = opt_px
                exit_reason = "SL"
                exit_ts = min(bar_end, window_end)
                break
            if row["low"] <= target_price:
                exit_spot = row["close"]
                exit_opt_market = opt_px
                exit_reason = "TARGET"
                exit_ts = min(bar_end, window_end)
                break
        else:
            if row["low"] <= sl_price:
                exit_spot = row["close"]
                exit_opt_market = opt_px
                exit_reason = "SL"
                exit_ts = min(bar_end, window_end)
                break
            if row["high"] >= target_price:
                exit_spot = row["close"]
                exit_opt_market = opt_px
                exit_reason = "TARGET"
                exit_ts = min(bar_end, window_end)
                break

        if bar_end >= window_end:
            exit_spot = row["close"]
            exit_opt_market = opt_px
            exit_reason = "WINDOW_END"
            exit_ts = window_end
            break

    if exit_ts is None or exit_spot is None:
        return None

    # Black-Scholes re-price at exit as a sanity-check / stale-data fallback.
    T_exit = years_to_expiry(expiry, exit_ts)
    exit_opt_bs = bs_price(exit_spot, strike, T_exit, RISK_FREE_RATE, iv, opt_type)

    # Use actual market exit premium when available; fall back to BS only when market tick is stale.
    if exit_opt_market is not None and exit_opt_market != opt_entry_premium:
        exit_opt_premium = exit_opt_market
    else:
        exit_opt_premium = exit_opt_bs

    # Selling options: profit if premium decays
    pnl_per_lot = opt_entry_premium - exit_opt_premium
    pnl_rs = pnl_per_lot * LOT_SIZE

    return {
        "entry_ts": entry_ts.isoformat(),
        "entry_spot": entry_spot,
        "entry_trigger": entry_price,
        "entry_opt_premium": opt_entry_premium,
        "entry_iv": round(iv * 100, 2),
        "sl": sl_price,
        "target": target_price,
        "exit_ts": exit_ts.isoformat(),
        "exit_spot": exit_spot,
        "exit_opt_market": exit_opt_market,
        "exit_opt_bs": round(exit_opt_bs, 2),
        "exit_opt_premium": round(exit_opt_premium, 2),
        "exit_reason": exit_reason,
        "pnl_per_lot": round(pnl_per_lot, 2),
        "pnl_rs": round(pnl_rs, 2),
    }


def process_day(
    df_1m: pd.DataFrame,
    df_5m: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_60m: pd.DataFrame,
    day: date,
    opt_cache: Dict[Tuple[date, str, int], pd.DataFrame],
) -> List[dict]:
    trades = []
    window_start = IST.localize(datetime.combine(day, ENTRY_START))
    window_end = IST.localize(datetime.combine(day, ENTRY_END))

    htf_traps = find_htf_traps(df_60m)

    expiry = _select_expiry(day, opt_cache)
    if expiry is None:
        print(f"{day}: no option expiry covers this date")
        return []

    for htf in htf_traps:
        breach_start = htf["breach_ts"]
        breach_end = breach_start + timedelta(minutes=HTF_MIN)
        if not (window_start <= breach_start and breach_end <= window_end):
            continue

        mtf_slice = df_15m[
            (df_15m["datetime"] >= breach_start - timedelta(minutes=MTF_MIN)) &
            (df_15m["datetime"] < breach_end)
        ].copy()
        if len(mtf_slice) < 2:
            continue

        mtf_traps = scan_traps_in_window(mtf_slice, htf["kind"])
        mtf_traps = [
            e for e in mtf_traps
            if window_start <= pd.to_datetime(e.get("trapped_on")) < breach_end
        ]

        for mtf in mtf_traps:
            mtf_breach_ts = pd.to_datetime(mtf.get("trapped_on"))
            mtf_breach_start = mtf_breach_ts
            mtf_breach_end = mtf_breach_start + timedelta(minutes=MTF_MIN)

            ltf_slice = df_5m[
                (df_5m["datetime"] >= mtf_breach_start - timedelta(minutes=2 * LTF_MIN)) &
                (df_5m["datetime"] < mtf_breach_end)
            ].copy()
            if len(ltf_slice) < 2:
                continue

            ltf_traps = scan_traps_in_window(ltf_slice, htf["kind"])
            ltf_traps = [
                e for e in ltf_traps
                if mtf_breach_start <= pd.to_datetime(e.get("trapped_on")) < mtf_breach_end
            ]

            for ltf in ltf_traps:
                trigger = mtf_trigger(ltf)
                sl = mtf_sl(ltf)
                target = htf["target"]
                trigger_ts = pd.to_datetime(ltf.get("trapped_on"))

                # Determine option to sell
                opt_type = "CE" if htf["kind"] == "BULL" else "PE"
                # Estimate spot at trigger to pick ATM strike
                spot_row = df_1m[df_1m["datetime"] <= trigger_ts]
                spot_at_trigger = float(spot_row.iloc[-1]["close"]) if not spot_row.empty else trigger
                strike = _atm_strike(spot_at_trigger)

                opt_df = opt_cache.get((expiry, opt_type, strike))
                if opt_df is None:
                    # Try neighbouring strikes
                    for off in [0, 1, -1, 2, -2]:
                        opt_df = opt_cache.get((expiry, opt_type, strike + off * STRIKE_STEP))
                        if opt_df is not None:
                            strike = strike + off * STRIKE_STEP
                            break
                if opt_df is None:
                    print(f"  {day}: no option data for {opt_type}{strike} exp {expiry}")
                    continue

                # Require option data to cover the anticipated trade window.
                opt_min_ts = opt_df["datetime"].min()
                opt_max_ts = opt_df["datetime"].max()
                if not (opt_min_ts <= trigger_ts <= opt_max_ts):
                    continue

                opt_entry_premium = _opt_price(opt_df, trigger_ts, "close")
                if opt_entry_premium is None:
                    continue

                result = simulate_trade(
                    htf["kind"], trigger, sl, target,
                    trigger_ts, df_1m, window_end,
                    opt_entry_premium, opt_df,
                    expiry, strike, opt_type,
                )
                if result:
                    trades.append({
                        "date": day.isoformat(),
                        "expiry": expiry.isoformat(),
                        "kind": htf["kind"],
                        "opt_type": opt_type,
                        "strike": strike,
                        "htf_ref": htf["ref_ts"].isoformat(),
                        "htf_breach": htf["breach_ts"].isoformat(),
                        "mtf_breach": mtf.get("trapped_on"),
                        "ltf_breach": ltf.get("trapped_on"),
                        **result,
                    })

    return trades


def run_backtest(start_date: date, end_date: date) -> None:
    print(f"NIFTY nested-trap option backtest: {start_date} to {end_date}")
    print(f"Window: {ENTRY_START}-{ENTRY_END} IST | HTF={HTF_MIN}m MTF={MTF_MIN}m LTF={LTF_MIN}m")
    print(f"SL buffer={SL_BUF}pts | LOT={LOT_SIZE} | option P&L from cached premiums\n")

    spot_1m = _load_spot()
    spot_5m = resample_bars(spot_1m, LTF_MIN)
    spot_15m = resample_bars(spot_1m, MTF_MIN)
    spot_60m = resample_bars(spot_1m, HTF_MIN)

    opt_paths = _list_option_files()
    opt_cache = _load_option_cache(opt_paths)
    cover_expiries = sorted({
        k[0] for k, df in opt_cache.items()
        if df["datetime"].dt.date.min() <= start_date <= df["datetime"].dt.date.max()
        or df["datetime"].dt.date.min() <= end_date <= df["datetime"].dt.date.max()
    })
    print(f"Loaded {len(opt_cache)} option series | expiries covering range: {', '.join(str(e) for e in cover_expiries)}")

    all_trades: List[dict] = []
    current = start_date
    while current <= end_date:
        day1m = spot_1m[spot_1m["datetime"].dt.date == current]
        day5m = spot_5m[spot_5m["datetime"].dt.date == current]
        day15m = spot_15m[spot_15m["datetime"].dt.date == current]
        day60m = spot_60m[spot_60m["datetime"].dt.date == current]
        if day1m.empty or day5m.empty or day15m.empty or day60m.empty:
            current += timedelta(days=1)
            continue

        trades = process_day(day1m, day5m, day15m, day60m, current, opt_cache)
        if trades:
            print(f"{current} -> {len(trades)} trade(s)")
            all_trades.extend(trades)
        current += timedelta(days=1)

    if not all_trades:
        print("\nNo nested-trap trades generated in the configured window.")
        return

    df_trades = pd.DataFrame(all_trades)
    out_path = os.path.join("data", f"nifty_nested_trap_option_{start_date}_{end_date}.csv")
    df_trades.to_csv(out_path, index=False)

    wins = df_trades[df_trades["pnl_rs"] > 0]
    losses = df_trades[df_trades["pnl_rs"] < 0]
    win_rate = len(wins) / len(df_trades) * 100 if len(df_trades) else 0.0
    gross_profit = wins["pnl_rs"].sum() if not wins.empty else 0.0
    gross_loss = abs(losses["pnl_rs"].sum()) if not losses.empty else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")
    net_pnl = df_trades["pnl_rs"].sum()
    avg_win = wins["pnl_rs"].mean() if not wins.empty else 0.0
    avg_loss = abs(losses["pnl_rs"].mean()) if not losses.empty else 0.0
    rr = avg_win / avg_loss if avg_loss > 0 else 0.0
    cummax = df_trades["pnl_rs"].cumsum().cummax()
    drawdown = (df_trades["pnl_rs"].cumsum() - cummax).min()

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total trades       : {len(df_trades)}")
    print(f"Wins               : {len(wins)} ({win_rate:.1f}%)")
    print(f"Losses             : {len(losses)}")
    print(f"Gross profit       : ₹{gross_profit:,.2f}")
    print(f"Gross loss         : ₹{gross_loss:,.2f}")
    print(f"Net P&L            : ₹{net_pnl:,.2f}")
    print(f"Profit factor      : {profit_factor:.2f}")
    print(f"Avg win / avg loss : ₹{avg_win:,.2f} / ₹{avg_loss:,.2f}  (R:R = {rr:.2f})")
    print(f"Max drawdown       : ₹{drawdown:,.2f}")
    print(f"Per-trade CSV      : {out_path}")
    print("=" * 70)
    print("\nNOTE: P&L uses actual cached option premiums (selling ATM option).")
    print("Slippage, bid-ask spread, and execution delays are NOT included.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=date.fromisoformat, help="YYYY-MM-DD")
    parser.add_argument("--end", type=date.fromisoformat, help="YYYY-MM-DD")
    args = parser.parse_args()

    end = args.end or date(2026, 7, 14)
    start = args.start or date(2026, 7, 1)
    run_backtest(start, end)
