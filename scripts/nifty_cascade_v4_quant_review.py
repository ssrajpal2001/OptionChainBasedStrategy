"""
scripts/nifty_cascade_v4_quant_review.py
=========================================
Clean, reviewed implementation of the NIFTY MTF Cascade Trap strategy.

Key design choices to prevent cross-day leakage:
  1. All timeframe resampling is anchored at 09:15 IST per trading day.
  2. Indicator calculations (VWAP, ADX, RSI) are run once on the full
     chronological 5m series, so today's values only use past data.
  3. The backtest loop processes one day at a time and only looks at
     today's higher-timeframe bars for trap detection.
  4. The first bar of each day has no "previous" bar in the same day, so
     no traps can fire at the 09:15 open bar.
"""
from __future__ import annotations

import glob
import os
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pytz

IST = pytz.timezone("Asia/Kolkata")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")
OUTPUT_DIR = os.path.join(ROOT, "data")

LOT_SIZE = 65
SL_BUFFER = 10.0
ENTRY_START = time(9, 15)
ENTRY_END = time(15, 30)
INTRADAY_EXIT = time(15, 30)

HTF_MIN = 60
MTF_MIN = 15
LTF_MIN = 5

VWAP_PERIOD = 500
ADX_PERIOD = 20
RSI_PERIOD = 14

DEFAULT_MAX_ADX = 20.0
DEFAULT_RSI_LONG_MIN = 50.0
DEFAULT_RSI_SHORT_MAX = 50.0


# ──────────────────────────────────────────────────────────────────────────────
# Indicators (no lookahead, only past data)
# ──────────────────────────────────────────────────────────────────────────────

def compute_adx(df: pd.DataFrame, period: int = ADX_PERIOD) -> pd.Series:
    """Wilder ADX; uses only past data via ewm smoothing."""
    high = df["high"]
    low = df["low"]
    close = df["close"]

    prev_high = high.shift(1)
    prev_low = low.shift(1)
    prev_close = close.shift(1)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    up_move = high - prev_high
    down_move = prev_low - low

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    # Wilder's smoothing
    atr = tr.ewm(alpha=1.0 / period, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1.0 / period, adjust=False).mean() / atr
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1.0 / period, adjust=False).mean() / atr

    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    adx = dx.ewm(alpha=1.0 / period, adjust=False).mean()
    return adx


def compute_rsi(df: pd.DataFrame, period: int = RSI_PERIOD) -> pd.Series:
    """Wilder RSI; uses only past data."""
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_rolling_vwap(df: pd.DataFrame, period: int = VWAP_PERIOD) -> pd.Series:
    """Rolling VWAP; if volume is missing, falls back to unit-weighted typical price."""
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    vol = df["volume"].fillna(0)
    weight = vol if vol.sum() > 0 else pd.Series(1, index=df.index)
    pv = typical * weight
    return pv.rolling(window=period, min_periods=period).sum() / weight.rolling(window=period, min_periods=period).sum()


# ──────────────────────────────────────────────────────────────────────────────
# Data loading & per-day resampling (no cross-day leakage)
# ──────────────────────────────────────────────────────────────────────────────

def load_1m_spot() -> pd.DataFrame:
    """Load all cached NIFTY 1m spot parquet files."""
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
    # Keep only market hours
    df = df[(df["datetime"].dt.time >= ENTRY_START) & (df["datetime"].dt.time <= ENTRY_END)]
    return df


def resample_per_day(df_1m: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """
    Resample 1m to `minutes` TF, resetting the origin to 09:15 each day.
    This guarantees no 60m/15m/5m bar spans across two trading days.
    """
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


def prepare_5m_with_indicators(df_1m: pd.DataFrame) -> pd.DataFrame:
    """Build 5m bars and attach indicators."""
    df = resample_per_day(df_1m, LTF_MIN)
    df = df.sort_values("datetime").reset_index(drop=True)
    df["vwap"] = compute_rolling_vwap(df, VWAP_PERIOD)
    df["adx"] = compute_adx(df, ADX_PERIOD)
    df["rsi"] = compute_rsi(df, RSI_PERIOD)
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Trap detection (same-day only)
# ──────────────────────────────────────────────────────────────────────────────

def find_htf_60m_traps(df_60m: pd.DataFrame) -> List[Dict]:
    """60m 2-candle sweep."""
    traps = []
    for i in range(1, len(df_60m)):
        prev = df_60m.iloc[i - 1]
        curr = df_60m.iloc[i]
        if curr["low"] < prev["low"]:
            traps.append({
                "kind": "BEAR",
                "direction": "LONG",
                "breach_ts": curr["datetime"],
                "breach_end": curr["datetime"] + timedelta(minutes=HTF_MIN),
                "target": float(prev["high"]),
            })
        if curr["high"] > prev["high"]:
            traps.append({
                "kind": "BULL",
                "direction": "SHORT",
                "breach_ts": curr["datetime"],
                "breach_end": curr["datetime"] + timedelta(minutes=HTF_MIN),
                "target": float(prev["low"]),
            })
    return traps


def find_mtf_15m_traps(df_15m: pd.DataFrame, htf_trap: Dict) -> List[Dict]:
    """15m matching trap inside the 60m breach window."""
    bars = df_15m[(df_15m["datetime"] >= htf_trap["breach_ts"]) &
                  (df_15m["datetime"] < htf_trap["breach_end"])].copy()
    if len(bars) < 2:
        return []

    traps = []
    for i in range(1, len(bars)):
        prev = bars.iloc[i - 1]
        curr = bars.iloc[i]
        if htf_trap["kind"] == "BEAR":
            if curr["high"] > prev["high"] and prev["low"] <= curr["close"] <= prev["high"]:
                traps.append({
                    "kind": "BEAR",
                    "breach_ts": curr["datetime"],
                    "breach_end": curr["datetime"] + timedelta(minutes=MTF_MIN),
                    "target": htf_trap["target"],
                })
        else:
            if curr["low"] < prev["low"] and prev["low"] <= curr["close"] <= prev["high"]:
                traps.append({
                    "kind": "BULL",
                    "breach_ts": curr["datetime"],
                    "breach_end": curr["datetime"] + timedelta(minutes=MTF_MIN),
                    "target": htf_trap["target"],
                })
    return traps


def find_ltf_5m_traps(df_5m: pd.DataFrame, mtf_trap: Dict) -> List[Dict]:
    """5m sweep zone + 1/3 entry trigger inside the 15m window."""
    bars = df_5m[(df_5m["datetime"] >= mtf_trap["breach_ts"]) &
                 (df_5m["datetime"] < mtf_trap["breach_end"])].copy()
    if len(bars) < 2:
        return []
    bars = bars.sort_values("datetime").reset_index(drop=True)

    traps = []
    for i in range(1, len(bars)):
        prev = bars.iloc[i - 1]
        curr = bars.iloc[i]
        if mtf_trap["kind"] == "BEAR":
            if curr["low"] >= prev["low"]:
                continue
            zone_high = float(prev["low"])
            zone_low = float(curr["low"])
            trigger = zone_high - (zone_high - zone_low) / 3.0
            sl = zone_low - SL_BUFFER
            traps.append({
                "kind": "BEAR",
                "direction": "LONG",
                "setup_ts": curr["datetime"],
                "zone_high": zone_high,
                "zone_low": zone_low,
                "trigger": round(trigger, 2),
                "sl": round(sl, 2),
                "target": mtf_trap["target"],
            })
        else:
            if curr["high"] <= prev["high"]:
                continue
            zone_high = float(curr["high"])
            zone_low = float(prev["high"])
            trigger = zone_low + (zone_high - zone_low) / 3.0
            sl = zone_high + SL_BUFFER
            traps.append({
                "kind": "BULL",
                "direction": "SHORT",
                "setup_ts": curr["datetime"],
                "zone_high": zone_high,
                "zone_low": zone_low,
                "trigger": round(trigger, 2),
                "sl": round(sl, 2),
                "target": mtf_trap["target"],
            })
    return traps


# ──────────────────────────────────────────────────────────────────────────────
# Filters & execution
# ──────────────────────────────────────────────────────────────────────────────

def check_5m_filters(ltf_trap: Dict, df_5m_full: pd.DataFrame,
                     max_adx: float, rsi_long_min: float, rsi_short_max: float,
                     use_filters: bool) -> bool:
    if not use_filters:
        return True

    row = df_5m_full[df_5m_full["datetime"] == ltf_trap["setup_ts"]]
    if row.empty:
        return False
    row = row.iloc[0]
    vwap, adx, rsi, close = row["vwap"], row["adx"], row["rsi"], row["close"]
    if pd.isna(vwap) or pd.isna(adx) or pd.isna(rsi):
        return False

    if ltf_trap["kind"] == "BEAR":
        return close > vwap and adx < max_adx and rsi > rsi_long_min
    else:
        return close < vwap and adx < max_adx and rsi < rsi_short_max


def simulate_trade(ltf_trap: Dict, df_1m_day: pd.DataFrame, eod: datetime) -> Optional[Dict]:
    """Execute on first 1m cross of trigger, then walk to SL/target/EOD."""
    trigger, sl, target = ltf_trap["trigger"], ltf_trap["sl"], ltf_trap["target"]
    kind = ltf_trap["kind"]

    future = df_1m_day[df_1m_day["datetime"] > ltf_trap["setup_ts"]].copy()
    if future.empty:
        return None

    entry_ts = None
    for _, row in future.iterrows():
        if row["datetime"] > eod:
            return None
        if kind == "BEAR" and row["high"] >= trigger:
            entry_ts = row["datetime"]
            break
        if kind == "BULL" and row["low"] <= trigger:
            entry_ts = row["datetime"]
            break
    if entry_ts is None:
        return None

    after = df_1m_day[df_1m_day["datetime"] >= entry_ts].copy()
    exit_ts = None
    exit_spot = None
    exit_reason = "OPEN"

    for _, row in after.iterrows():
        bar_end = row["datetime"] + timedelta(minutes=1)
        if kind == "BEAR":
            if row["low"] <= sl:
                exit_spot, exit_reason, exit_ts = sl, "SL", min(bar_end, eod)
                break
            if row["high"] >= target:
                exit_spot, exit_reason, exit_ts = target, "TARGET", min(bar_end, eod)
                break
        else:
            if row["high"] >= sl:
                exit_spot, exit_reason, exit_ts = sl, "SL", min(bar_end, eod)
                break
            if row["low"] <= target:
                exit_spot, exit_reason, exit_ts = target, "TARGET", min(bar_end, eod)
                break
        if bar_end >= eod:
            exit_spot, exit_reason, exit_ts = float(row["close"]), "EOD", eod
            break

    if exit_ts is None or exit_spot is None:
        return None

    pts = exit_spot - trigger if kind == "BEAR" else trigger - exit_spot
    return {
        "kind": kind,
        "direction": ltf_trap["direction"],
        "setup_ts": ltf_trap["setup_ts"],
        "entry_ts": entry_ts,
        "entry_price": trigger,
        "sl": sl,
        "target": target,
        "exit_ts": exit_ts,
        "exit_price": exit_spot,
        "exit_reason": exit_reason,
        "pts": round(pts, 2),
        "pnl_rs": round(pts * LOT_SIZE, 2),
        "zone_high": ltf_trap["zone_high"],
        "zone_low": ltf_trap["zone_low"],
    }


# ──────────────────────────────────────────────────────────────────────────────
# Day-by-day backtest loop
# ──────────────────────────────────────────────────────────────────────────────

def run_backtest(df_1m: pd.DataFrame, use_filters: bool = True,
                 max_adx: float = DEFAULT_MAX_ADX,
                 rsi_long_min: float = DEFAULT_RSI_LONG_MIN,
                 rsi_short_max: float = DEFAULT_RSI_SHORT_MAX) -> pd.DataFrame:
    """Process day-by-day without cross-day leakage."""
    df_5m_full = prepare_5m_with_indicators(df_1m)
    df_15m_full = resample_per_day(df_1m, MTF_MIN)
    df_60m_full = resample_per_day(df_1m, HTF_MIN)

    all_trades: List[Dict] = []
    all_dates = sorted(df_1m["datetime"].dt.date.unique())

    for day in all_dates:
        day_1m = df_1m[df_1m["datetime"].dt.date == day]
        day_5m = df_5m_full[df_5m_full["datetime"].dt.date == day]
        day_15m = df_15m_full[df_15m_full["datetime"].dt.date == day]
        day_60m = df_60m_full[df_60m_full["datetime"].dt.date == day]

        if day_1m.empty or day_5m.empty or day_15m.empty or day_60m.empty:
            continue

        eod = IST.localize(datetime.combine(day, INTRADAY_EXIT))
        in_position = False

        for htf in find_htf_60m_traps(day_60m):
            if in_position:
                break
            for mtf in find_mtf_15m_traps(day_15m, htf):
                if in_position:
                    break
                for ltf in find_ltf_5m_traps(day_5m, mtf):
                    if in_position:
                        break
                    if ltf["setup_ts"].time() > ENTRY_END:
                        continue
                    if not check_5m_filters(ltf, df_5m_full, max_adx, rsi_long_min, rsi_short_max, use_filters):
                        continue
                    trade = simulate_trade(ltf, day_1m, eod)
                    if trade:
                        all_trades.append(trade)
                        in_position = True

    print("Backtest processing complete.")
    return pd.DataFrame(all_trades)


# ──────────────────────────────────────────────────────────────────────────────
# Summary & CLI
# ──────────────────────────────────────────────────────────────────────────────

def summarize(trades: pd.DataFrame) -> Dict:
    if trades.empty:
        return {
            "total": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
            "gross_profit": 0.0, "gross_loss": 0.0, "net_pnl": 0.0,
            "profit_factor": 0.0, "avg_win": 0.0, "avg_loss": 0.0, "rr": 0.0,
            "max_dd": 0.0,
        }

    pnls = trades["pnl_rs"].values
    wins = pnls[pnls > 0]
    losses = pnls[pnls < 0]
    gp = wins.sum() if len(wins) else 0.0
    gl = abs(losses.sum()) if len(losses) else 0.0
    net = pnls.sum()
    pf = gp / gl if gl > 0 else float("inf")
    avg_win = wins.mean() if len(wins) else 0.0
    avg_loss = abs(losses.mean()) if len(losses) else 0.0
    rr = avg_win / avg_loss if avg_loss > 0 else 0.0

    cum = pnls.cumsum()
    cummax = np.maximum.accumulate(cum)
    max_dd = (cummax - cum).max()

    return {
        "total": len(trades), "wins": len(wins), "losses": len(losses),
        "win_rate": 100 * len(wins) / len(trades), "gross_profit": gp,
        "gross_loss": gl, "net_pnl": net, "profit_factor": pf,
        "avg_win": avg_win, "avg_loss": avg_loss, "rr": rr, "max_dd": max_dd,
    }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="NIFTY MTF Cascade Trap V4 Reviewed")
    parser.add_argument("--start", type=date.fromisoformat, default=None)
    parser.add_argument("--end", type=date.fromisoformat, default=None)
    parser.add_argument("--no-filters", action="store_true", help="Skip VWAP/ADX/RSI filters")
    parser.add_argument("--max-adx", type=float, default=DEFAULT_MAX_ADX)
    parser.add_argument("--rsi-long-min", type=float, default=DEFAULT_RSI_LONG_MIN)
    parser.add_argument("--rsi-short-max", type=float, default=DEFAULT_RSI_SHORT_MAX)
    args = parser.parse_args()

    df_1m = load_1m_spot()
    start = args.start or df_1m["datetime"].dt.date.min()
    end = args.end or df_1m["datetime"].dt.date.max()
    df_1m = df_1m[(df_1m["datetime"].dt.date >= start) & (df_1m["datetime"].dt.date <= end)]

    print(f"Backtest: {start} to {end} | filters={not args.no_filters}")
    trades = run_backtest(df_1m, use_filters=not args.no_filters,
                          max_adx=args.max_adx,
                          rsi_long_min=args.rsi_long_min,
                          rsi_short_max=args.rsi_short_max)

    if trades.empty:
        print("No trades generated.")
        return

    out = os.path.join(OUTPUT_DIR, f"nifty_cascade_v4_reviewed_trades_{start}_{end}.csv")
    trades.to_csv(out, index=False)
    print(f"Saved {len(trades)} trades to {out}")

    s = summarize(trades)
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total trades        : {s['total']}")
    print(f"Wins                : {s['wins']} ({s['win_rate']:.1f}%)")
    print(f"Losses              : {s['losses']}")
    print(f"Gross profit        : Rs. {s['gross_profit']:,.2f}")
    print(f"Gross loss          : Rs. {s['gross_loss']:,.2f}")
    print(f"Net P&L             : Rs. {s['net_pnl']:,.2f}")
    print(f"Profit factor       : {s['profit_factor']:.2f}")
    print(f"Avg win / avg loss  : Rs. {s['avg_win']:,.2f} / Rs. {s['avg_loss']:,.2f} (R:R = {s['rr']:.2f})")
    print(f"Max drawdown        : Rs. {s['max_dd']:,.2f}")
    print("=" * 70)
    print("\nExit reasons:")
    print(trades["exit_reason"].value_counts().to_string())


if __name__ == "__main__":
    main()
