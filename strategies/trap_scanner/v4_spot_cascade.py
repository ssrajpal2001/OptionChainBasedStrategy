"""
strategies/trap_scanner/v4_spot_cascade.py
============================================
Pure V4 spot cascade logic for NIFTY.

Mirrors the backtest in scripts/nifty_cascade_v4_sweep.py:
  - 75m HTF spot -> 15m MTF spot -> 5m LTF spot -> 1m execution
  - 1/3 retracement entry trigger on 5m LTF
  - ADX < max, RSI directional, VWAP directional filters on 5m LTF
  - Retest void-lift: after entry, target only becomes active after price
    revisits the HTF entry level (prev low for bear, prev high for bull).

This module is stateless and works on pandas DataFrames so it can be unit-tested
and reused by both the backtest and the live engine.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# Default V4 parameters (match the backtest script)
HTF_MIN = 75
MTF_MIN = 15
LTF_MIN = 5
SL_BUFFER = 10.0
VWAP_PERIOD = 500
ADX_PERIOD = 20
RSI_PERIOD = 14


def _resample_per_day(df_1m: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """Resample 1m bars to higher TF anchored at 09:15 IST per day."""
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


def compute_adx(df: pd.DataFrame, period: int = ADX_PERIOD) -> pd.Series:
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

    atr = tr.ewm(alpha=1.0 / period, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1.0 / period, adjust=False).mean() / atr
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1.0 / period, adjust=False).mean() / atr

    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    adx = dx.ewm(alpha=1.0 / period, adjust=False).mean()
    return adx


def compute_rsi(df: pd.DataFrame, period: int = RSI_PERIOD) -> pd.Series:
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_rolling_vwap(df: pd.DataFrame, period: int = VWAP_PERIOD) -> pd.Series:
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    vol = df["volume"].fillna(0)
    weight = vol if vol.sum() > 0 else pd.Series(1, index=df.index)
    pv = typical * weight
    return pv.rolling(window=period, min_periods=period).sum() / weight.rolling(window=period, min_periods=period).sum()


def prepare_5m_with_indicators(df_1m: pd.DataFrame) -> pd.DataFrame:
    df = _resample_per_day(df_1m, LTF_MIN)
    df = df.sort_values("datetime").reset_index(drop=True)
    df["vwap"] = compute_rolling_vwap(df, VWAP_PERIOD)
    df["adx"] = compute_adx(df, ADX_PERIOD)
    df["rsi"] = compute_rsi(df, RSI_PERIOD)
    return df


def find_htf_75m_traps(df_75m: pd.DataFrame) -> List[Dict]:
    """Detect structural traps on 75m HTF spot bars."""
    traps = []
    for i in range(1, len(df_75m)):
        prev = df_75m.iloc[i - 1]
        curr = df_75m.iloc[i]

        if curr["low"] < prev["low"]:
            traps.append({
                "kind": "BEAR",
                "direction": "LONG",
                "breach_ts": curr["datetime"],
                "breach_end": curr["datetime"] + timedelta(minutes=HTF_MIN),
                "target": float(prev["high"]),
                "htf_entry_level": float(prev["low"]),
                "zone_high": float(prev["low"]),
                "zone_low": float(curr["low"]),
                "htf_breach_ts": curr["datetime"],
            })
        if curr["high"] > prev["high"]:
            traps.append({
                "kind": "BULL",
                "direction": "SHORT",
                "breach_ts": curr["datetime"],
                "breach_end": curr["datetime"] + timedelta(minutes=HTF_MIN),
                "target": float(prev["low"]),
                "htf_entry_level": float(prev["high"]),
                "zone_high": float(curr["high"]),
                "zone_low": float(prev["high"]),
                "htf_breach_ts": curr["datetime"],
            })
    return traps


def find_mtf_15m_traps(df_15m: pd.DataFrame, htf_trap: Dict) -> List[Dict]:
    """Find matching 15m MTF traps inside the HTF breach window."""
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
                    "htf_entry_level": htf_trap["htf_entry_level"],
                    "htf_breach_ts": htf_trap["htf_breach_ts"],
                })
        else:
            if curr["low"] < prev["low"] and prev["low"] <= curr["close"] <= prev["high"]:
                traps.append({
                    "kind": "BULL",
                    "breach_ts": curr["datetime"],
                    "breach_end": curr["datetime"] + timedelta(minutes=MTF_MIN),
                    "target": htf_trap["target"],
                    "htf_entry_level": htf_trap["htf_entry_level"],
                    "htf_breach_ts": htf_trap["htf_breach_ts"],
                })
    return traps


def find_ltf_5m_traps(df_5m: pd.DataFrame, mtf_trap: Dict) -> List[Dict]:
    """Find 5m LTF traps with 1/3 retracement entry inside the MTF breach window."""
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
            trigger = zone_low + (zone_high - zone_low) / 3.0
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
                "htf_entry_level": mtf_trap["htf_entry_level"],
                "htf_breach_ts": mtf_trap["htf_breach_ts"],
            })
        else:
            if curr["high"] <= prev["high"]:
                continue
            zone_high = float(curr["high"])
            zone_low = float(prev["high"])
            trigger = zone_high - (zone_high - zone_low) / 3.0
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
                "htf_entry_level": mtf_trap["htf_entry_level"],
                "htf_breach_ts": mtf_trap["htf_breach_ts"],
            })
    return traps


def check_ltf_filters(
    ltf_trap: Dict,
    df_5m_full: pd.DataFrame,
    max_adx: float,
    rsi_long_min: float,
    rsi_short_max: float,
    use_vwap: bool,
    use_adx: bool,
    use_rsi: bool,
) -> bool:
    """ADX/RSI/VWAP filter gate on the 5m LTF setup bar."""
    row = df_5m_full[df_5m_full["datetime"] == ltf_trap["setup_ts"]]
    if row.empty:
        return False
    row = row.iloc[0]
    vwap, adx, rsi, close = row["vwap"], row["adx"], row["rsi"], row["close"]

    if use_vwap and not pd.isna(vwap):
        if ltf_trap["kind"] == "BEAR" and close <= vwap:
            return False
        if ltf_trap["kind"] == "BULL" and close >= vwap:
            return False

    if use_adx and not pd.isna(adx):
        if adx >= max_adx:
            return False

    if use_rsi and not pd.isna(rsi):
        if ltf_trap["kind"] == "BEAR" and rsi <= rsi_long_min:
            return False
        if ltf_trap["kind"] == "BULL" and rsi >= rsi_short_max:
            return False

    return True


def find_v4_setups(
    df_1m: pd.DataFrame,
    max_adx: float = 20.0,
    rsi_long_min: float = 40.0,
    rsi_short_max: float = 60.0,
    use_vwap: bool = True,
    use_adx: bool = True,
    use_rsi: bool = True,
) -> List[Dict]:
    """
    Return all V4 LTF setups that pass filters for the current 1m data.
    Each setup dict contains the trigger, SL, target, htf_entry_level, and direction.
    """
    df_5m_full = prepare_5m_with_indicators(df_1m)
    df_15m_full = _resample_per_day(df_1m, MTF_MIN)
    df_75m_full = _resample_per_day(df_1m, HTF_MIN)

    setups = []
    all_dates = sorted(df_1m["datetime"].dt.date.unique())

    for day in all_dates:
        day_5m = df_5m_full[df_5m_full["datetime"].dt.date == day]
        day_15m = df_15m_full[df_15m_full["datetime"].dt.date == day]
        day_75m = df_75m_full[df_75m_full["datetime"].dt.date == day]

        if day_5m.empty or day_15m.empty or day_75m.empty:
            continue

        for htf in find_htf_75m_traps(day_75m):
            for mtf in find_mtf_15m_traps(day_15m, htf):
                for ltf in find_ltf_5m_traps(day_5m, mtf):
                    if not check_ltf_filters(
                        ltf, df_5m_full, max_adx, rsi_long_min, rsi_short_max,
                        use_vwap, use_adx, use_rsi,
                    ):
                        continue
                    setups.append(ltf)

    return setups



def detect_macro_htf_traps(df_htf: pd.DataFrame, lookback: int = 3) -> List[Dict]:
    """
    Detect macro structural bull/bear traps on an HTF DataFrame.

    Bull trap (short setup):
      - Resistance = highest high of the previous `lookback` candles.
      - Breakout candle closes above resistance. Anchor SL = low of the breakout candle.
      - Confirmed when a later candle trades below the anchor SL.
      - Trap zone = [resistance, highest high from breakout to confirmation].

    Bear trap (long setup):
      - Support = lowest low of the previous `lookback` candles.
      - Breakdown candle closes below support. Anchor SL = high of the breakdown candle.
      - Confirmed when a later candle trades above the anchor SL.
      - Trap zone = [lowest low from breakdown to confirmation, support].
    """
    traps: List[Dict] = []
    n = len(df_htf)
    if n < lookback + 2:
        return traps

    i = lookback
    while i < n:
        # --- Bull trap (breakout above resistance, then fail below anchor low) ---
        resistance = float(df_htf["high"].iloc[i - lookback:i].max())
        if df_htf["close"].iloc[i] > resistance:
            anchor_sl = float(df_htf["low"].iloc[i])
            peak_high = float(df_htf["high"].iloc[i])
            j = i + 1
            while j < n:
                peak_high = max(peak_high, float(df_htf["high"].iloc[j]))
                if df_htf["low"].iloc[j] < anchor_sl:
                    traps.append({
                        "trap_ts": df_htf["datetime"].iloc[j],
                        "type": "Bull",
                        "multiplier": None,
                        "breakout_line": resistance,
                        "anchor_sl": anchor_sl,
                        "peak": peak_high,
                        "zone_low": resistance,
                        "zone_high": peak_high,
                    })
                    i = j + 1
                    break
                j += 1
            else:
                i += 1
            continue

        # --- Bear trap (breakdown below support, then fail above anchor high) ---
        support = float(df_htf["low"].iloc[i - lookback:i].min())
        if df_htf["close"].iloc[i] < support:
            anchor_sl = float(df_htf["high"].iloc[i])
            peak_low = float(df_htf["low"].iloc[i])
            j = i + 1
            while j < n:
                peak_low = min(peak_low, float(df_htf["low"].iloc[j]))
                if df_htf["high"].iloc[j] > anchor_sl:
                    traps.append({
                        "trap_ts": df_htf["datetime"].iloc[j],
                        "type": "Bear",
                        "multiplier": None,
                        "breakout_line": support,
                        "anchor_sl": anchor_sl,
                        "peak": peak_low,
                        "zone_low": peak_low,
                        "zone_high": support,
                    })
                    i = j + 1
                    break
                j += 1
            else:
                i += 1
            continue

        i += 1

    return traps


def find_latest_v4_setup(
    df_1m: pd.DataFrame,
    max_adx: float = 20.0,
    rsi_long_min: float = 40.0,
    rsi_short_max: float = 60.0,
    use_vwap: bool = True,
    use_adx: bool = True,
    use_rsi: bool = True,
) -> Optional[Dict]:
    """Return the most recent V4 setup that is still active (not yet triggered)."""
    setups = find_v4_setups(df_1m, max_adx, rsi_long_min, rsi_short_max, use_vwap, use_adx, use_rsi)
    if not setups:
        return None
    return setups[-1]
