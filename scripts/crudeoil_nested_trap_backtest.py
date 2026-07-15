"""
scripts/crudeoil_nested_trap_backtest.py

CRUDEOIL nested-fractal trap backtest for the 18:00-21:30 IST window.

Logic (user-specified):
  1. HTF = 1h futures candle. If the next 1h candle breaches the prior 1h high/low,
     the traders inside that prior hour are trapped.
        - prior 1h LOW breached  -> bull trap -> SHORT (PE)
        - prior 1h HIGH breached -> bear trap -> LONG  (CE)
  2. Inside the breach 1h candle, find 15m traps in the same direction.
  3. Inside the relevant 15m candle(s), find 5m traps in the same direction.
  4. Enter when a 1m candle breaks the 5m zone trigger:
        - long  : 1m high >= 5m trigger
        - short : 1m low  <= 5m trigger
  5. SL = 5m zone extreme ± 10 pts, target = the breached 1h level.
  6. Square off at 21:30 if still open.

Output: per-trade CSV + summary stats.
"""
from __future__ import annotations

import io
import os
import sys
import time
from datetime import date, datetime, time as dt_time, timedelta
from typing import List, Optional
from urllib.parse import quote

import pandas as pd
import pytz

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from data_layer.instrument_registry import REGISTRY
from strategies.trap_scanner import scanner

IST = pytz.timezone("Asia/Kolkata")

UNDERLYING = "CRUDEOIL"
HTF_MIN = 60
MTF_MIN = 15
LTF_MIN = 5
LOT_SIZE = 100
SL_BUF = 10.0
ENTRY_START = dt_time(18, 0)
ENTRY_END = dt_time(21, 30)

# curl_cffi avoids Upstox TLS/403 blocks on plain requests.
try:
    from curl_cffi import requests as _requests
except Exception as _exc:
    import requests as _requests
    print(f"WARN: curl_cffi not available ({_exc}); falling back to plain requests.")


def _get_token() -> str:
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


def find_htf_traps(df_htf: pd.DataFrame) -> List[dict]:
    """Find 1h candles whose high/low is breached by the next 1h candle."""
    traps = []
    for i in range(1, len(df_htf)):
        prev = df_htf.iloc[i - 1]
        curr = df_htf.iloc[i]
        # Bear trap: high breached -> bears trapped -> LONG (CE)
        if curr["high"] > prev["high"]:
            traps.append({
                "kind": "BEAR",
                "ref_ts": prev["datetime"],
                "ref_high": float(prev["high"]),
                "ref_low": float(prev["low"]),
                "breach_ts": curr["datetime"],
                "target": float(prev["high"]),
            })
        # Bull trap: low breached -> bulls trapped -> SHORT (PE)
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
    """Run scanner on a sliced df and return traps of the requested kind."""
    _, entries = scanner.scan_htf_spot(df)
    return [e for e in entries if e.get("kind") == kind and e.get("status") in ("TRAPPED", "CLOSED")]


def mtf_trigger(entry: dict) -> float:
    """Zone trigger for a scanner entry dict."""
    zh = float(entry["zone_high"])
    zl = float(entry["zone_low"])
    if entry.get("kind") == "BULL":
        return round(zh - (zh - zl) / 3, 2)
    return round(zl + (zh - zl) / 3, 2)


def mtf_sl(entry: dict, buf: float = SL_BUF) -> float:
    """Initial SL for the LTF zone."""
    if entry.get("kind") == "BULL":  # short trade
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
) -> Optional[dict]:
    """Walk 1m bars after trigger_ts for entry, then SL/target/window-end."""
    future = df_1m[df_1m["datetime"] > trigger_ts].copy()
    if future.empty:
        return None

    # Entry: first 1m bar whose extreme breaches the trigger level.
    entry_ts = None
    for _, row in future.iterrows():
        if row["datetime"] > window_end:
            return None
        if kind == "BULL":  # short
            if row["low"] <= entry_price:
                entry_ts = row["datetime"]
                break
        else:  # BEAR -> long
            if row["high"] >= entry_price:
                entry_ts = row["datetime"]
                break

    if entry_ts is None:
        return None

    after_entry = future[future["datetime"] >= entry_ts].copy()
    exit_price = None
    exit_reason = "OPEN"
    exit_ts = None

    for _, row in after_entry.iterrows():
        bar_end = row["datetime"] + timedelta(minutes=1)
        if kind == "BULL":  # short
            if row["high"] >= sl_price:
                exit_price = sl_price
                exit_reason = "SL"
                exit_ts = min(bar_end, window_end)
                break
            if row["low"] <= target_price:
                exit_price = target_price
                exit_reason = "TARGET"
                exit_ts = min(bar_end, window_end)
                break
        else:  # long
            if row["low"] <= sl_price:
                exit_price = sl_price
                exit_reason = "SL"
                exit_ts = min(bar_end, window_end)
                break
            if row["high"] >= target_price:
                exit_price = target_price
                exit_reason = "TARGET"
                exit_ts = min(bar_end, window_end)
                break

        if bar_end >= window_end:
            exit_price = round(row["close"], 2)
            exit_reason = "WINDOW_END"
            exit_ts = window_end
            break

    if exit_price is None:
        return None

    if kind == "BULL":
        pnl_pts = entry_price - exit_price
    else:
        pnl_pts = exit_price - entry_price

    return {
        "entry_ts": entry_ts.isoformat(),
        "entry": entry_price,
        "sl": sl_price,
        "target": target_price,
        "exit_ts": exit_ts.isoformat() if exit_ts else None,
        "exit": exit_price,
        "exit_reason": exit_reason,
        "pnl_pts": round(pnl_pts, 2),
        "pnl_rs": round(pnl_pts * LOT_SIZE, 2),
    }


def process_day(
    df_1m: pd.DataFrame,
    df_5m: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_60m: pd.DataFrame,
    day: date,
) -> List[dict]:
    """Find nested traps and simulate trades for one day."""
    trades = []
    window_start = IST.localize(datetime.combine(day, ENTRY_START))
    window_end = IST.localize(datetime.combine(day, ENTRY_END))

    htf_traps = find_htf_traps(df_60m)

    for htf in htf_traps:
        # HTF breach candle must be fully inside the evening window.
        breach_start = htf["breach_ts"]
        breach_end = breach_start + timedelta(minutes=HTF_MIN)
        if not (window_start <= breach_start and breach_end <= window_end):
            continue

        # 15m slice: include the prior 15m so a trap can form at the boundary.
        mtf_slice = df_15m[
            (df_15m["datetime"] >= breach_start - timedelta(minutes=MTF_MIN)) &
            (df_15m["datetime"] < breach_end)
        ].copy()
        if len(mtf_slice) < 2:
            continue

        mtf_traps = scan_traps_in_window(mtf_slice, htf["kind"])
        # Keep only 15m traps whose breach bar is inside the HTF breach candle.
        mtf_traps = [
            e for e in mtf_traps
            if window_start <= pd.to_datetime(e.get("trapped_on")) < breach_end
        ]

        for mtf in mtf_traps:
            mtf_breach_ts = pd.to_datetime(mtf.get("trapped_on"))
            mtf_breach_start = mtf_breach_ts
            mtf_breach_end = mtf_breach_start + timedelta(minutes=MTF_MIN)

            # 5m slice: include prior 5m bars for boundary trap detection.
            ltf_slice = df_5m[
                (df_5m["datetime"] >= mtf_breach_start - timedelta(minutes=2 * LTF_MIN)) &
                (df_5m["datetime"] < mtf_breach_end)
            ].copy()
            if len(ltf_slice) < 2:
                continue

            ltf_traps = scan_traps_in_window(ltf_slice, htf["kind"])
            # Keep only 5m traps whose breach bar is inside the 15m breach candle.
            ltf_traps = [
                e for e in ltf_traps
                if mtf_breach_start <= pd.to_datetime(e.get("trapped_on")) < mtf_breach_end
            ]

            for ltf in ltf_traps:
                trigger = mtf_trigger(ltf)
                sl = mtf_sl(ltf)
                target = htf["target"]
                result = simulate_trade(
                    htf["kind"], trigger, sl, target,
                    pd.to_datetime(ltf.get("trapped_on")), df_1m, window_end,
                )
                if result:
                    trades.append({
                        "date": day.isoformat(),
                        "kind": htf["kind"],
                        "htf_ref": htf["ref_ts"].isoformat(),
                        "htf_breach": htf["breach_ts"].isoformat(),
                        "mtf_breach": mtf.get("trapped_on"),
                        "ltf_breach": ltf.get("trapped_on"),
                        **result,
                    })

    return trades


def run_backtest(start_date: date, end_date: date, token: str) -> None:
    REGISTRY.load_sync(UNDERLYING)
    instrument_key = REGISTRY.historical_instrument_key(UNDERLYING)
    print(f"Using instrument key: {instrument_key}")
    print(f"Backtest window: {start_date} to {end_date} | Evening entries {ENTRY_START}-{ENTRY_END} IST")
    print(f"HTF={HTF_MIN}m MTF={MTF_MIN}m LTF={LTF_MIN}m SL_BUF={SL_BUF}pts LOT={LOT_SIZE}\n")

    # Fetch data day-by-day for the whole range plus warm-up.
    fetch_from = start_date - timedelta(days=5)
    fetch_to = end_date
    df1m = fetch_1m(instrument_key, fetch_from, fetch_to, token)
    if df1m.empty:
        print("ERROR: no 1m data returned")
        return

    df1m = df1m.sort_values("datetime").reset_index(drop=True)
    df5m = resample_bars(df1m, LTF_MIN)
    df15m = resample_bars(df1m, MTF_MIN)
    df60m = resample_bars(df1m, HTF_MIN)

    all_trades: List[dict] = []
    current = start_date
    while current <= end_date:
        day1m = df1m[df1m["datetime"].dt.date == current]
        day5m = df5m[df5m["datetime"].dt.date == current]
        day15m = df15m[df15m["datetime"].dt.date == current]
        day60m = df60m[df60m["datetime"].dt.date == current]
        if day1m.empty or day5m.empty or day15m.empty or day60m.empty:
            current += timedelta(days=1)
            continue

        trades = process_day(day1m, day5m, day15m, day60m, current)
        if trades:
            print(f"{current} -> {len(trades)} trade(s)")
            all_trades.extend(trades)
        current += timedelta(days=1)

    if not all_trades:
        print("\nNo nested-trap trades generated in the configured window.")
        return

    df_trades = pd.DataFrame(all_trades)
    out_path = os.path.join("data", f"crudeoil_nested_trap_evening_{start_date}_{end_date}.csv")
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
