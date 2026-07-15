"""
scripts/crudeoil_evening_trap_backtest.py

1-month backtest for CRUDEOIL evening window (18:00-21:30 IST) trap scanner.

Signal:
  - HTF = 60m on near-month MCX futures
  - MTF = 15m used for entry/exit simulation (bars resampled from 1m)
  - Bull trap -> PE trade (short direction)
  - Bear trap -> CE trade (long direction)

Trade logic:
  - Entry at HTF zone_trigger, filled on the first 15m bar whose range touches it.
  - SL = zone_low - sl_buf  for CE (bear trap)
         zone_high + sl_buf for PE (bull trap)
  - Target = HTF 1h extreme = e["sl"] (ref bar high for CE, ref bar low for PE)
  - Square-off any open position at 21:30 IST if SL/Target not hit.
  - Only traps triggered between 18:00 and 21:30 IST are considered.

Output: per-trade CSV + summary stats.
"""
from __future__ import annotations

import io
import os
import sys
from datetime import date, datetime, time as dt_time, timedelta
from typing import List, Optional
from urllib.parse import quote

import time

import pandas as pd
import pytz

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from data_layer.instrument_registry import REGISTRY
from strategies.trap_scanner import scanner

# curl_cffi avoids Upstox TLS/403 blocks on plain requests.
try:
    from curl_cffi import requests as _requests
except Exception as _exc:
    import requests as _requests
    print(f"WARN: curl_cffi not available ({_exc}); falling back to plain requests.")

IST = pytz.timezone("Asia/Kolkata")

UNDERLYING = "CRUDEOIL"
HTF_MIN = 60
MTF_MIN = 15
STEP = 100
LOT_SIZE = 100
SL_BUF = 20.0
ENTRY_START = dt_time(18, 0)
ENTRY_END = dt_time(21, 30)


def _get_token() -> str:
    """Try env var first, then DB."""
    token = os.environ.get("UPSTOX_TOKEN", "")
    if token:
        return token
    try:
        from data_layer.client_db import ClientDB
        creds = ClientDB().get_feeder_creds_sync("upstox")
        token = (creds or {}).get("access_token", "")
    except Exception as exc:
        print(f"WARN: could not read Upstox token from DB: {exc}")
    return token or ""


def _http_get(url: str, headers: dict) -> dict:
    """GET JSON using curl_cffi if available, else plain requests."""
    impersonate = getattr(_requests, "get", None) is not None and "curl_cffi" in str(_requests)
    try:
        if impersonate:
            r = _requests.get(url, headers=headers, impersonate="chrome131", timeout=60)
        else:
            r = _requests.get(url, headers=headers, timeout=60)
    except Exception as exc:
        raise RuntimeError(f"Upstox fetch failed: {exc}")
    r.raise_for_status()
    return r.json()


def fetch_1m_single_day(instrument_key: str, day: date, token: str) -> pd.DataFrame:
    """Fetch 1-minute candles for a single day. Upstox dated endpoint requires from==to."""
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    enc = quote(instrument_key, safe="")
    url = (
        f"https://api.upstox.com/v2/historical-candle/{enc}/1minute/"
        f"{day.isoformat()}/{day.isoformat()}"
    )
    data = _http_get(url, headers)
    if data.get("status") != "success":
        raise RuntimeError(f"Upstox non-success for {day}: {data}")
    candles = data.get("data", {}).get("candles", [])
    if not candles:
        return pd.DataFrame()
    rows = [
        {
            "datetime": pd.to_datetime(c[0]),
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4]),
            "volume": int(c[5]),
        }
        for c in reversed(candles)
    ]
    df = pd.DataFrame(rows)
    if df["datetime"].dt.tz is None:
        df["datetime"] = df["datetime"].dt.tz_localize(IST)
    else:
        df["datetime"] = df["datetime"].dt.tz_convert(IST)
    return df


def fetch_1m(instrument_key: str, from_dt: date, to_dt: date, token: str) -> pd.DataFrame:
    """Fetch 1-minute candles day-by-day (Upstox dated endpoint only supports one day)."""
    frames = []
    d = from_dt
    while d <= to_dt:
        try:
            df = fetch_1m_single_day(instrument_key, d, token)
            if not df.empty:
                frames.append(df)
        except Exception as exc:
            print(f"  WARN: fetch failed for {d}: {exc}")
        d += timedelta(days=1)
        time.sleep(0.15)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values("datetime").reset_index(drop=True)


def resample_bars(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    dfc = df.set_index("datetime")
    res = dfc.resample(f"{minutes}min").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return res


def zone_trigger(e: dict) -> float:
    zh = float(e["zone_high"])
    zl = float(e["zone_low"])
    if e.get("kind") == "BULL":
        return round(zh - (zh - zl) / 3, 2)
    return round(zl + (zh - zl) / 3, 2)


def trade_target(e: dict) -> float:
    """Target = 1h extreme (ref bar high for CE/bear, ref bar low for PE/bull)."""
    return round(float(e["sl"]), 2)


def trade_sl(e: dict, sl_buf: float = SL_BUF) -> float:
    """Initial SL in futures price units."""
    if e.get("kind") == "BULL":  # PE short
        return round(float(e["zone_high"]) + sl_buf, 2)
    # BEAR -> CE long
    return round(float(e["zone_low"]) - sl_buf, 2)


def simulate_day(
    htf_entries: List[dict],
    df_15m: pd.DataFrame,
    day: date,
    lot_size: int = LOT_SIZE,
) -> List[dict]:
    """Simulate trades for one day in the evening window."""
    results: List[dict] = []
    window_start = IST.localize(datetime.combine(day, ENTRY_START))
    window_end = IST.localize(datetime.combine(day, ENTRY_END))

    for e in htf_entries:
        if e.get("status") not in ("TRAPPED", "CLOSED"):
            continue
        trap_ts = pd.to_datetime(e.get("trapped_on") or e.get("closed_on"))
        if trap_ts is pd.NaT:
            continue
        # Trap must trigger inside the evening window.
        if not (window_start <= trap_ts < window_end):
            continue

        is_bull = e.get("kind") == "BULL"  # PE short
        entry_price = zone_trigger(e)
        sl_price = trade_sl(e)
        tgt_price = trade_target(e)

        # Find first 15m bar after trap that touches the entry trigger.
        future = df_15m[df_15m["datetime"] >= trap_ts].copy()
        if future.empty:
            continue

        entry_ts = None
        for _, row in future.iterrows():
            bar_end = row["datetime"] + timedelta(minutes=MTF_MIN)
            if bar_end > window_end:
                break
            if is_bull and row["high"] >= entry_price:
                entry_ts = row["datetime"]
                break
            if (not is_bull) and row["low"] <= entry_price:
                entry_ts = row["datetime"]
                break

        if entry_ts is None:
            continue

        # Walk forward 15m bars from entry to check SL / target / window end.
        after_entry = future[future["datetime"] >= entry_ts].copy()
        exit_price = None
        exit_reason = "OPEN"
        exit_ts = None

        for _, row in after_entry.iterrows():
            bar_end = row["datetime"] + timedelta(minutes=MTF_MIN)
            # SL hit?
            if is_bull:
                if row["high"] >= sl_price:
                    exit_price = sl_price
                    exit_reason = "SL"
                    exit_ts = min(bar_end, window_end)
                    break
            else:
                if row["low"] <= sl_price:
                    exit_price = sl_price
                    exit_reason = "SL"
                    exit_ts = min(bar_end, window_end)
                    break

            # Target hit?
            if is_bull:
                if row["low"] <= tgt_price:
                    exit_price = tgt_price
                    exit_reason = "TARGET"
                    exit_ts = min(bar_end, window_end)
                    break
            else:
                if row["high"] >= tgt_price:
                    exit_price = tgt_price
                    exit_reason = "TARGET"
                    exit_ts = min(bar_end, window_end)
                    break

            # Window end square-off
            if bar_end >= window_end:
                exit_price = round(row["close"], 2)
                exit_reason = "WINDOW_END"
                exit_ts = window_end
                break

        if exit_price is None:
            continue

        if is_bull:
            pnl_pts = entry_price - exit_price
        else:
            pnl_pts = exit_price - entry_price

        pnl_rs = round(pnl_pts * lot_size, 2)
        rr = round(abs(tgt_price - entry_price) / abs(entry_price - sl_price), 2) if abs(entry_price - sl_price) > 0 else 0.0

        results.append({
            "date": day.isoformat(),
            "kind": e.get("kind"),
            "trap_ts": trap_ts.isoformat(),
            "entry_ts": entry_ts.isoformat(),
            "exit_ts": exit_ts.isoformat() if exit_ts else None,
            "entry": entry_price,
            "sl": sl_price,
            "target": tgt_price,
            "exit": exit_price,
            "exit_reason": exit_reason,
            "pnl_pts": round(pnl_pts, 2),
            "pnl_rs": pnl_rs,
            "rr_potential": rr,
        })

    return results


def run_backtest(start_date: date, end_date: date, token: str) -> None:
    REGISTRY.load_sync(UNDERLYING)
    instrument_key = REGISTRY.historical_instrument_key(UNDERLYING)
    print(f"Using instrument key: {instrument_key}")
    print(f"Backtest window: {start_date} to {end_date} | Evening entries {ENTRY_START}-{ENTRY_END} IST")
    print(f"HTF={HTF_MIN}m MTF={MTF_MIN}m SL_BUF={SL_BUF}pts LOT={LOT_SIZE}\n")

    # Fetch 1m data for the whole range plus a few days of warm-up for HTF context.
    fetch_from = start_date - timedelta(days=5)
    fetch_to = end_date + timedelta(days=1)
    df1m = fetch_1m(instrument_key, fetch_from, fetch_to, token)
    if df1m.empty:
        print("ERROR: no 1m data returned")
        return

    df1m = df1m.sort_values("datetime").reset_index(drop=True)
    htf_all = resample_bars(df1m, HTF_MIN)
    mtf_all = resample_bars(df1m, MTF_MIN)

    # HTF trap scan on full history so context is available.
    _, htf_entries = scanner.scan_htf_spot(htf_all)
    print(f"Total HTF traps in range: {len([e for e in htf_entries if e.get('status') in ('TRAPPED','CLOSED')])}")

    all_trades: List[dict] = []
    current = start_date
    while current <= end_date:
        day_htf = htf_all[htf_all["datetime"].dt.date == current]
        day_mtf = mtf_all[mtf_all["datetime"].dt.date == current]
        if day_htf.empty or day_mtf.empty:
            current += timedelta(days=1)
            continue

        # Re-scan only today's HTF bars (but detection uses prev bar from prior day if present).
        _, day_entries = scanner.scan_htf_spot(day_htf)
        trades = simulate_day(day_entries, day_mtf, current)
        all_trades.extend(trades)
        if trades:
            print(f"{current} -> {len(trades)} trade(s)")
        current += timedelta(days=1)

    if not all_trades:
        print("\nNo trades generated in the configured window.")
        return

    df_trades = pd.DataFrame(all_trades)
    out_path = os.path.join("data", f"crudeoil_trap_evening_{start_date}_{end_date}.csv")
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

    # Running max drawdown
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
    print("\nNOTE: P&L is in futures-point units (1 point = ₹100 for 1 lot).")
    print("Actual option P&L will be lower depending on option delta/time-value.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=date.fromisoformat, help="YYYY-MM-DD")
    parser.add_argument("--end", type=date.fromisoformat, help="YYYY-MM-DD")
    args = parser.parse_args()

    token = _get_token()
    if not token:
        print("ERROR: No Upstox access token found.")
        print("Set UPSTOX_TOKEN env var or update data/clients.db system_feeder_creds.")
        sys.exit(1)

    end = args.end or date.today()
    start = args.start or (end - timedelta(days=30))
    run_backtest(start, end, token)
