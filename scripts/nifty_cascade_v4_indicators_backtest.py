"""
scripts/nifty_cascade_v4_indicators_backtest.py
================================================
Production-grade NIFTY Spot Multi-Timeframe Cascade Trap backtest.

Architecture:
  HTF 60m  -> 2-candle liquidity sweep (bull/bear trap)
  MTF 15m  -> matching 15m trap inside the 60m breach window
  LTF 5m   -> 5m sweep zone + 1/3 entry trigger + indicator filters
  Exec 1m  -> first 1m candle crossing the 5m trigger price

Indicators on 5m (at LTF setup bar):
  - VWAP 500-period
  - ADX 20-period  (must be < 20)
  - RSI 14-period

Risk:
  - Lot size = 65
  - SL = zone extreme ± 10 NIFTY points
  - Target = HTF 60m reference level
  - Max 1 position at a time
  - EOD square-off at 15:30 IST

Outputs:
  - Per-trade CSV
  - Summary with win rate, profit factor, R:R, max drawdown
"""
from __future__ import annotations

import glob
import os
import sys
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

IST = pytz.timezone("Asia/Kolkata")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")
OUTPUT_DIR = os.path.join(ROOT, "data")

LOT_SIZE = 65
SL_BUF = 10.0
HTF_MIN = 60
MTF_MIN = 15
LTF_MIN = 5

ENTRY_START = time(9, 15)
ENTRY_END = time(15, 30)
SQUAREOFF = time(15, 30)

VWAP_PERIOD = 500
ADX_PERIOD = 20
RSI_PERIOD = 14

# Runtime filter parameters (overridden by CLI args)
ADX_MAX = 20.0
RSI_LONG_MIN = 50.0
RSI_SHORT_MAX = 50.0
SKIP_INDICATORS = False
VERBOSE = False


# ──────────────────────────────────────────────────────────────────────────────
# Data loading & resampling
# ──────────────────────────────────────────────────────────────────────────────

def load_1m_spot() -> pd.DataFrame:
    """Load all cached NIFTY 1m spot parquet files and merge chronologically."""
    files = sorted(glob.glob(os.path.join(CACHE_DIR, "spot_NIFTY_1m_*.parquet")))
    if not files:
        raise RuntimeError(f"No NIFTY 1m spot parquet files found in {CACHE_DIR}")

    frames = []
    for f in files:
        df = pd.read_parquet(f)
        df["datetime"] = pd.to_datetime(df["datetime"])
        if df["datetime"].dt.tz is not None:
            df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata")
        else:
            df["datetime"] = df["datetime"].dt.tz_localize("Asia/Kolkata")
        frames.append(df)

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates("datetime").sort_values("datetime").reset_index(drop=True)
    df = df[(df["datetime"].dt.time >= ENTRY_START) & (df["datetime"].dt.time <= ENTRY_END)]
    return df


def resample_intraday(df_1m: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """Resample 1m to target TF, anchored at 09:15 IST each trading day."""
    frames = []
    for day, g in df_1m.groupby(df_1m["datetime"].dt.date):
        g = g.set_index("datetime").sort_index()
        origin = pd.Timestamp(f"{day} 09:15:00", tz="Asia/Kolkata")
        g = g[g.index >= origin] if g.index.min() < origin else g
        r = g.resample(f"{minutes}min", origin=origin).agg(
            {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
        ).dropna().reset_index()
        frames.append(r)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ──────────────────────────────────────────────────────────────────────────────
# Indicators
# ──────────────────────────────────────────────────────────────────────────────

def compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Wilder RSI."""
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def compute_adx(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """Wilder ADX. Returns DataFrame with +DI, -DI, ADX."""
    df = df.copy()
    high = df["high"]
    low = df["low"]
    close = df["close"]

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = ((up_move > down_move) & (up_move > 0)) * up_move
    minus_dm = ((down_move > up_move) & (down_move > 0)) * down_move

    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = tr.ewm(alpha=1.0 / period, min_periods=period).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1.0 / period, min_periods=period).mean() / atr
    minus_di = 100 * minus_dm.ewm(alpha=1.0 / period, min_periods=period).mean() / atr

    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    adx = dx.ewm(alpha=1.0 / period, min_periods=period).mean()

    df["plus_di"] = plus_di
    df["minus_di"] = minus_di
    df["adx"] = adx
    return df


def compute_rolling_vwap(df: pd.DataFrame, period: int = 500) -> pd.DataFrame:
    """Rolling volume-weighted average price over the last `period` bars.
    If volume data is missing or all zero, falls back to unit-weighted typical price."""
    df = df.copy()
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    vol = df["volume"].fillna(0)
    if vol.sum() == 0:
        weight = pd.Series(1, index=df.index)
    else:
        weight = vol
    pv = typical * weight
    df["vwap"] = pv.rolling(window=period, min_periods=period).sum() / weight.rolling(window=period, min_periods=period).sum()
    return df


def prepare_5m_with_indicators(df_1m: pd.DataFrame) -> pd.DataFrame:
    """Build 5m dataframe and attach VWAP500, ADX20, RSI14."""
    df = resample_intraday(df_1m, LTF_MIN)
    df = df.sort_values("datetime").reset_index(drop=True)
    df = compute_rolling_vwap(df, VWAP_PERIOD)
    df = compute_adx(df, ADX_PERIOD)
    df["rsi"] = compute_rsi(df["close"], RSI_PERIOD)
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Trap detection
# ──────────────────────────────────────────────────────────────────────────────

def find_htf_60m_traps(df_60m: pd.DataFrame) -> List[Dict]:
    """
    2-candle liquidity sweep on 60m.

    NOTE: The spec text describes the trigger as a breakout/breakdown; here we use
    the standard trap-scanner convention so that target and direction align:
      - Bear Trap (Bullish / Long): current low sweeps below previous low,
        then we look for long entries. Target = previous high.
      - Bull Trap (Bearish / Short): current high sweeps above previous high,
        then we look for short entries. Target = previous low.
    """
    traps = []
    for i in range(1, len(df_60m)):
        prev = df_60m.iloc[i - 1]
        curr = df_60m.iloc[i]

        if curr["low"] < prev["low"]:
            traps.append({
                "kind": "BEAR",
                "direction": "LONG",
                "ref_ts": prev["datetime"],
                "breach_ts": curr["datetime"],
                "breach_end": curr["datetime"] + timedelta(minutes=HTF_MIN),
                "target": float(prev["high"]),
                "ref_high": float(prev["high"]),
                "ref_low": float(prev["low"]),
            })

        if curr["high"] > prev["high"]:
            traps.append({
                "kind": "BULL",
                "direction": "SHORT",
                "ref_ts": prev["datetime"],
                "breach_ts": curr["datetime"],
                "breach_end": curr["datetime"] + timedelta(minutes=HTF_MIN),
                "target": float(prev["low"]),
                "ref_high": float(prev["high"]),
                "ref_low": float(prev["low"]),
            })
    return traps


def find_mtf_15m_traps(df_15m: pd.DataFrame, htf_trap: Dict) -> List[Dict]:
    """
    Matching 15m trap inside the 60m breach window.
    Bear trap (long): curr 15m high > prev 15m high AND close inside prev range.
    Bull trap (short): curr 15m low < prev 15m low AND close inside prev range.
    """
    window_start = htf_trap["breach_ts"]
    window_end = htf_trap["breach_end"]
    kind = htf_trap["kind"]

    bars = df_15m[(df_15m["datetime"] >= window_start) & (df_15m["datetime"] < window_end)].copy()
    if len(bars) < 2:
        return []

    traps = []
    for i in range(1, len(bars)):
        prev = bars.iloc[i - 1]
        curr = bars.iloc[i]
        curr_end = curr["datetime"] + timedelta(minutes=MTF_MIN)

        if kind == "BEAR":
            swept = curr["high"] > prev["high"]
            closed_inside = prev["low"] <= curr["close"] <= prev["high"]
            if swept and closed_inside:
                traps.append({
                    "kind": "BEAR",
                    "direction": "LONG",
                    "ref_ts": prev["datetime"],
                    "breach_ts": curr["datetime"],
                    "breach_end": curr_end,
                    "target": htf_trap["target"],
                })
        else:
            swept = curr["low"] < prev["low"]
            closed_inside = prev["low"] <= curr["close"] <= prev["high"]
            if swept and closed_inside:
                traps.append({
                    "kind": "BULL",
                    "direction": "SHORT",
                    "ref_ts": prev["datetime"],
                    "breach_ts": curr["datetime"],
                    "breach_end": curr_end,
                    "target": htf_trap["target"],
                })
    return traps


def find_ltf_5m_traps(df_5m: pd.DataFrame, mtf_trap: Dict) -> List[Dict]:
    """
    5m sweep zone + 1/3 entry trigger inside the 15m trap window.
    Bear trap (long): zone = [curr low, prev low], trigger = prev low - wick/3.
    Bull trap (short): zone = [prev high, curr high], trigger = prev high + wick/3.
    """
    window_start = mtf_trap["breach_ts"]
    window_end = mtf_trap["breach_end"]
    kind = mtf_trap["kind"]

    bars = df_5m[(df_5m["datetime"] >= window_start) & (df_5m["datetime"] < window_end)].copy()
    if len(bars) < 2:
        return []

    bars = bars.sort_values("datetime").reset_index(drop=True)
    traps = []
    for i in range(1, len(bars)):
        prev = bars.iloc[i - 1]
        curr = bars.iloc[i]

        if kind == "BEAR":
            if curr["low"] >= prev["low"]:
                continue
            zone_high = float(prev["low"])
            zone_low = float(curr["low"])
            trigger = zone_high - (zone_high - zone_low) / 3.0
            sl = zone_low - SL_BUF
            traps.append({
                "kind": "BEAR",
                "direction": "LONG",
                "setup_ts": curr["datetime"],
                "zone_high": zone_high,
                "zone_low": zone_low,
                "trigger": round(trigger, 2),
                "sl": round(sl, 2),
                "target": mtf_trap["target"],
                "bar_close": float(curr["close"]),
                "bar_volume": int(curr["volume"]) if not pd.isna(curr["volume"]) else 0,
            })
        else:
            if curr["high"] <= prev["high"]:
                continue
            zone_high = float(curr["high"])
            zone_low = float(prev["high"])
            trigger = zone_low + (zone_high - zone_low) / 3.0
            sl = zone_high + SL_BUF
            traps.append({
                "kind": "BULL",
                "direction": "SHORT",
                "setup_ts": curr["datetime"],
                "zone_high": zone_high,
                "zone_low": zone_low,
                "trigger": round(trigger, 2),
                "sl": round(sl, 2),
                "target": mtf_trap["target"],
                "bar_close": float(curr["close"]),
                "bar_volume": int(curr["volume"]) if not pd.isna(curr["volume"]) else 0,
            })
    return traps


# ──────────────────────────────────────────────────────────────────────────────
# Indicator filters
# ──────────────────────────────────────────────────────────────────────────────

def check_5m_filters(ltf_trap: Dict, df_5m: pd.DataFrame) -> bool:
    """
    Apply 5m indicator filters at the LTF setup bar.
      VWAP500: long above, short below.
      ADX: below ADX_MAX (consolidation squeeze).
      RSI: long above RSI_LONG_MIN, short below RSI_SHORT_MAX.
    Use --skip-indicators to bypass all filters for testing the raw trap logic.
    """
    if SKIP_INDICATORS:
        return True

    setup_ts = ltf_trap["setup_ts"]
    row = df_5m[df_5m["datetime"] == setup_ts]
    if row.empty:
        return False
    row = row.iloc[0]

    vwap = row.get("vwap")
    adx = row.get("adx")
    rsi = row.get("rsi")
    close = row["close"]

    if pd.isna(vwap) or pd.isna(adx) or pd.isna(rsi):
        return False

    if ltf_trap["kind"] == "BEAR":
        if close <= vwap:
            return False
        if adx >= ADX_MAX:
            return False
        if rsi <= RSI_LONG_MIN:
            return False
    else:
        if close >= vwap:
            return False
        if adx >= ADX_MAX:
            return False
        if rsi >= RSI_SHORT_MAX:
            return False

    return True


# ──────────────────────────────────────────────────────────────────────────────
# Trade simulation
# ──────────────────────────────────────────────────────────────────────────────

def simulate_trade(
    ltf_trap: Dict,
    df_1m: pd.DataFrame,
    eod: datetime,
) -> Optional[Dict]:
    """
    Execute on first 1m candle crossing the 5m trigger, then walk to SL/target/EOD.
    Returns trade dict or None if not executed.
    """
    setup_ts = ltf_trap["setup_ts"]
    trigger = ltf_trap["trigger"]
    sl = ltf_trap["sl"]
    target = ltf_trap["target"]
    kind = ltf_trap["kind"]

    future = df_1m[df_1m["datetime"] > setup_ts].copy()
    if future.empty:
        return None

    entry_ts = None
    for _, row in future.iterrows():
        if row["datetime"] > eod:
            return None
        if kind == "BEAR":
            if row["high"] >= trigger:
                entry_ts = row["datetime"]
                break
        else:
            if row["low"] <= trigger:
                entry_ts = row["datetime"]
                break

    if entry_ts is None:
        return None

    after_entry = df_1m[df_1m["datetime"] >= entry_ts].copy()
    exit_ts = None
    exit_spot = None
    exit_reason = "OPEN"

    for _, row in after_entry.iterrows():
        bar_end = row["datetime"] + timedelta(minutes=1)
        if kind == "BEAR":
            if row["low"] <= sl:
                exit_spot = sl
                exit_reason = "SL"
                exit_ts = min(bar_end, eod)
                break
            if row["high"] >= target:
                exit_spot = target
                exit_reason = "TARGET"
                exit_ts = min(bar_end, eod)
                break
        else:
            if row["high"] >= sl:
                exit_spot = sl
                exit_reason = "SL"
                exit_ts = min(bar_end, eod)
                break
            if row["low"] <= target:
                exit_spot = target
                exit_reason = "TARGET"
                exit_ts = min(bar_end, eod)
                break

        if bar_end >= eod:
            exit_spot = float(row["close"])
            exit_reason = "EOD"
            exit_ts = eod
            break

    if exit_ts is None or exit_spot is None:
        return None

    if kind == "BEAR":
        pts = exit_spot - trigger
    else:
        pts = trigger - exit_spot

    pnl_rs = pts * LOT_SIZE

    return {
        "kind": kind,
        "direction": ltf_trap["direction"],
        "setup_ts": setup_ts,
        "entry_ts": entry_ts,
        "entry_price": trigger,
        "sl": sl,
        "target": target,
        "exit_ts": exit_ts,
        "exit_price": exit_spot,
        "exit_reason": exit_reason,
        "pts": round(pts, 2),
        "pnl_rs": round(pnl_rs, 2),
        "zone_high": ltf_trap["zone_high"],
        "zone_low": ltf_trap["zone_low"],
    }


# ──────────────────────────────────────────────────────────────────────────────
# Day processing
# ──────────────────────────────────────────────────────────────────────────────

def process_day(
    day: date,
    df_1m_day: pd.DataFrame,
    df_5m_day: pd.DataFrame,
    df_15m_day: pd.DataFrame,
    df_60m_day: pd.DataFrame,
    df_5m_full: pd.DataFrame,
) -> List[Dict]:
    """Process one trading day. Max 1 active trade at a time."""
    trades = []
    in_position = False
    stats = {"htf": 0, "mtf": 0, "ltf": 0, "filtered": 0, "executed": 0}

    eod = IST.localize(datetime.combine(day, SQUAREOFF))

    htf_traps = find_htf_60m_traps(df_60m_day)
    stats["htf"] = len(htf_traps)
    for htf in htf_traps:
        if in_position:
            break

        mtf_traps = find_mtf_15m_traps(df_15m_day, htf)
        stats["mtf"] += len(mtf_traps)
        for mtf in mtf_traps:
            if in_position:
                break

            ltf_traps = find_ltf_5m_traps(df_5m_day, mtf)
            stats["ltf"] += len(ltf_traps)
            for ltf in ltf_traps:
                if in_position:
                    break

                # skip if setup is after entry cutoff
                if ltf["setup_ts"].time() > ENTRY_END:
                    continue

                if not check_5m_filters(ltf, df_5m_full):
                    stats["filtered"] += 1
                    continue

                trade = simulate_trade(ltf, df_1m_day, eod)
                if trade:
                    trades.append(trade)
                    stats["executed"] += 1
                    in_position = True

    if VERBOSE and (stats["htf"] > 0 or stats["executed"] > 0):
        print(f"  {day}: htf={stats['htf']} mtf={stats['mtf']} ltf={stats['ltf']} filtered={stats['filtered']} exec={stats['executed']}")

    return trades


# ──────────────────────────────────────────────────────────────────────────────
# Summary & main
# ──────────────────────────────────────────────────────────────────────────────

def summarize(trades: List[Dict]) -> Dict:
    if not trades:
        return {
            "total": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
            "gross_profit": 0.0, "gross_loss": 0.0, "net_pnl": 0.0,
            "profit_factor": 0.0, "avg_win": 0.0, "avg_loss": 0.0, "rr": 0.0,
            "max_dd": 0.0,
        }

    pnls = np.array([t["pnl_rs"] for t in trades])
    wins = pnls[pnls > 0]
    losses = pnls[pnls < 0]
    gross_profit = float(wins.sum()) if len(wins) else 0.0
    gross_loss = float(abs(losses.sum())) if len(losses) else 0.0
    net_pnl = float(pnls.sum())
    pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")
    avg_win = float(wins.mean()) if len(wins) else 0.0
    avg_loss = float(abs(losses.mean())) if len(losses) else 0.0
    rr = avg_win / avg_loss if avg_loss > 0 else 0.0

    cum = pnls.cumsum()
    cummax = np.maximum.accumulate(cum)
    max_dd = float((cummax - cum).max())

    return {
        "total": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": 100 * len(wins) / len(trades),
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "net_pnl": net_pnl,
        "profit_factor": pf,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "rr": rr,
        "max_dd": max_dd,
    }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="NIFTY MTF Cascade Trap V4 Backtest")
    parser.add_argument("--start", type=date.fromisoformat, default=None, help="YYYY-MM-DD")
    parser.add_argument("--end", type=date.fromisoformat, default=None, help="YYYY-MM-DD")
    parser.add_argument("--adx-max", type=float, default=20.0, help="Max ADX allowed on 5m setup bar")
    parser.add_argument("--rsi-long-min", type=float, default=50.0, help="Min RSI for long setups")
    parser.add_argument("--rsi-short-max", type=float, default=50.0, help="Max RSI for short setups")
    parser.add_argument("--skip-indicators", action="store_true", help="Skip VWAP/ADX/RSI filters for testing")
    parser.add_argument("--verbose", action="store_true", help="Print per-stage trap counts")
    args = parser.parse_args()

    # Bind filter parameters to module-level globals so helper functions can see them
    global ADX_MAX, RSI_LONG_MIN, RSI_SHORT_MAX, SKIP_INDICATORS, VERBOSE
    ADX_MAX = args.adx_max
    RSI_LONG_MIN = args.rsi_long_min
    RSI_SHORT_MAX = args.rsi_short_max
    SKIP_INDICATORS = args.skip_indicators
    VERBOSE = args.verbose

    print("Loading NIFTY 1m spot data...")
    df_1m_all = load_1m_spot()
    if df_1m_all.empty:
        print("No 1m data loaded.")
        sys.exit(1)
    print(f"Loaded {len(df_1m_all)} 1m bars from {df_1m_all['datetime'].min()} to {df_1m_all['datetime'].max()}")

    start = args.start or df_1m_all["datetime"].dt.date.min()
    end = args.end or df_1m_all["datetime"].dt.date.max()
    print(f"Backtest window: {start} to {end}")

    print("Building higher timeframe bars and indicators...")
    df_5m_all = prepare_5m_with_indicators(df_1m_all)
    df_15m_all = resample_intraday(df_1m_all, MTF_MIN)
    df_60m_all = resample_intraday(df_1m_all, HTF_MIN)
    print(f"5m bars: {len(df_5m_all)} | 15m bars: {len(df_15m_all)} | 60m bars: {len(df_60m_all)}")

    all_dates = sorted(df_1m_all["datetime"].dt.date.unique())
    all_trades = []

    for day in all_dates:
        if day < start or day > end:
            continue

        day_1m = df_1m_all[df_1m_all["datetime"].dt.date == day]
        day_5m = df_5m_all[df_5m_all["datetime"].dt.date == day]
        day_15m = df_15m_all[df_15m_all["datetime"].dt.date == day]
        day_60m = df_60m_all[df_60m_all["datetime"].dt.date == day]

        if day_1m.empty or day_5m.empty or day_15m.empty or day_60m.empty:
            continue

        trades = process_day(day, day_1m, day_5m, day_15m, day_60m, df_5m_all)
        if trades:
            print(f"{day}: {len(trades)} trade(s)")
            all_trades.extend(trades)

    if not all_trades:
        print("\nNo trades generated.")
        return

    df_trades = pd.DataFrame(all_trades)
    out_path = os.path.join(OUTPUT_DIR, f"nifty_cascade_v4_trades_{start}_{end}.csv")
    df_trades.to_csv(out_path, index=False)
    print(f"\nSaved {len(df_trades)} trades to {out_path}")

    s = summarize(all_trades)
    print("\n" + "=" * 70)
    print("NIFTY CASCADE V4 BACKTEST SUMMARY")
    print("=" * 70)
    print(f"Total trades        : {s['total']}")
    print(f"Wins                : {s['wins']} ({s['win_rate']:.1f}%)")
    print(f"Losses              : {s['losses']}")
    print(f"Gross profit        : Rs. {s['gross_profit']:,.2f}")
    print(f"Gross loss          : Rs. {s['gross_loss']:,.2f}")
    print(f"Net P&L             : Rs. {s['net_pnl']:,.2f}")
    print(f"Profit factor       : {s['profit_factor']:.2f}")
    print(f"Avg win / avg loss  : Rs. {s['avg_win']:,.2f} / Rs. {s['avg_loss']:,.2f}  (R:R = {s['rr']:.2f})")
    print(f"Max drawdown        : Rs. {s['max_dd']:,.2f}")
    print("=" * 70)

    print("\nExit reason breakdown:")
    print(df_trades["exit_reason"].value_counts().to_string())


if __name__ == "__main__":
    main()
