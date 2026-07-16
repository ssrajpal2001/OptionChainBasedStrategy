"""
scripts/nifty_nested_trap_spot_backtest.py
==========================================
NIFTY intraday nested-fractal trap backtest using **NIFTY spot directly**.

Logic:
  1. HTF = 1h spot candle. If the next 1h candle breaches the prior 1h high/low,
     traders inside that prior hour are trapped.
        - prior 1h LOW breached  -> bull trap -> SHORT NIFTY
        - prior 1h HIGH breached -> bear trap -> LONG NIFTY
  2. Inside the breach 1h candle, find 15m traps in the same direction.
  3. Inside the relevant 15m candle(s), find 5m traps in the same direction.
  4. Enter when a 1m candle breaks the 5m zone trigger.
  5. SL = 5m zone extreme ± buffer, target = the breached 1h level.
  6. Square off at 15:30 if still open.

P&L is computed as if trading NIFTY futures / spot at a fixed lot size
(1 point move = LOT_SIZE rupees).
"""
from __future__ import annotations

import io
import os
import sys
from datetime import date, datetime, time as dt_time, timedelta
from typing import List, Optional

import pandas as pd
import pytz

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from strategies.trap_scanner import scanner

IST = pytz.timezone("Asia/Kolkata")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")

UNDERLYING = "NIFTY"
LOT_SIZE = 65               # user-requested fixed lot size for NIFTY spot P&L
HTF_MIN = 60
MTF_MIN = 15
LTF_MIN = 5
SL_BUF = 10.0               # NIFTY points
ENTRY_START = dt_time(9, 15)
ENTRY_END = dt_time(15, 30)


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
) -> Optional[dict]:
    """
    Simulate a NIFTY spot trade.
      BULL trap -> short NIFTY (profit if spot falls)
      BEAR trap -> long NIFTY  (profit if spot rises)
    """
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

    after_entry = future[future["datetime"] >= entry_ts].copy()
    exit_reason = "OPEN"
    exit_ts = None
    exit_spot = None

    for _, row in after_entry.iterrows():
        bar_end = row["datetime"] + timedelta(minutes=1)

        if kind == "BULL":
            if row["high"] >= sl_price:
                exit_spot = sl_price
                exit_reason = "SL"
                exit_ts = min(bar_end, window_end)
                break
            if row["low"] <= target_price:
                exit_spot = target_price
                exit_reason = "TARGET"
                exit_ts = min(bar_end, window_end)
                break
        else:
            if row["low"] <= sl_price:
                exit_spot = sl_price
                exit_reason = "SL"
                exit_ts = min(bar_end, window_end)
                break
            if row["high"] >= target_price:
                exit_spot = target_price
                exit_reason = "TARGET"
                exit_ts = min(bar_end, window_end)
                break

        if bar_end >= window_end:
            exit_spot = row["close"]
            exit_reason = "WINDOW_END"
            exit_ts = window_end
            break

    if exit_ts is None or exit_spot is None:
        return None

    # NIFTY spot P&L
    if kind == "BULL":
        # short NIFTY: profit when entry_spot > exit_spot
        pnl_rs = (entry_spot - exit_spot) * LOT_SIZE
    else:
        # long NIFTY: profit when exit_spot > entry_spot
        pnl_rs = (exit_spot - entry_spot) * LOT_SIZE

    return {
        "entry_ts": entry_ts.isoformat(),
        "entry_spot": entry_spot,
        "entry_trigger": entry_price,
        "sl": sl_price,
        "target": target_price,
        "exit_ts": exit_ts.isoformat(),
        "exit_spot": exit_spot,
        "exit_reason": exit_reason,
        "pnl_rs": round(pnl_rs, 2),
    }


def process_day(
    df_1m: pd.DataFrame,
    df_5m: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_60m: pd.DataFrame,
    day: date,
) -> List[dict]:
    trades = []
    window_start = IST.localize(datetime.combine(day, ENTRY_START))
    window_end = IST.localize(datetime.combine(day, ENTRY_END))

    htf_traps = find_htf_traps(df_60m)

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

                result = simulate_trade(
                    htf["kind"], trigger, sl, target,
                    trigger_ts, df_1m, window_end,
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


def run_backtest(start_date: date, end_date: date) -> None:
    print(f"NIFTY nested-trap SPOT backtest: {start_date} to {end_date}")
    print(f"Window: {ENTRY_START}-{ENTRY_END} IST | HTF={HTF_MIN}m MTF={MTF_MIN}m LTF={LTF_MIN}m")
    print(f"SL buffer={SL_BUF}pts | LOT={LOT_SIZE} | P&L = NIFTY points × {LOT_SIZE}\n")

    spot_1m = _load_spot()
    spot_5m = resample_bars(spot_1m, LTF_MIN)
    spot_15m = resample_bars(spot_1m, MTF_MIN)
    spot_60m = resample_bars(spot_1m, HTF_MIN)

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

        trades = process_day(day1m, day5m, day15m, day60m, current)
        if trades:
            print(f"{current} -> {len(trades)} trade(s)")
            all_trades.extend(trades)
        current += timedelta(days=1)

    if not all_trades:
        print("\nNo nested-trap trades generated in the configured window.")
        return

    df_trades = pd.DataFrame(all_trades)
    out_path = os.path.join("data", f"nifty_nested_trap_spot_{start_date}_{end_date}.csv")
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
    print("\nNOTE: P&L uses NIFTY spot points × fixed lot size (no option premium, no slippage).")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=date.fromisoformat, help="YYYY-MM-DD")
    parser.add_argument("--end", type=date.fromisoformat, help="YYYY-MM-DD")
    args = parser.parse_args()

    end = args.end or date(2026, 7, 14)
    start = args.start or date(2026, 7, 1)
    run_backtest(start, end)
