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

from datetime import datetime, time, timedelta
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

# Per-index lot configuration: each tranche = 1 lot, total position = 2 lots.
# Values are the number of units per tranche (1 lot).
INDEX_LOT_CONFIG: Dict[str, int] = {
    "NIFTY": 65,
    "SENSEX": 20,
    "BANKNIFTY": 35,
}


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
      - Base = previous `lookback` candles. Breakout line = highest high of the base.
      - Breakout candle closes above the breakout line.
      - Anchor SL = lowest low of the same base (the swing low of the breakout move).
      - Confirmed when a later candle trades below the anchor SL.
      - Trap zone = [breakout line, highest high from breakout to confirmation].

    Bear trap (long setup):
      - Base = previous `lookback` candles. Breakout line = lowest low of the base.
      - Breakdown candle closes below the breakout line.
      - Anchor SL = highest high of the same base (the swing high of the breakdown move).
      - Confirmed when a later candle trades above the anchor SL.
      - Trap zone = [lowest low from breakdown to confirmation, breakout line].

    A state machine is used so only one pending trap per direction is tracked at a time,
    preventing the noisy overlapping duplicates produced by the previous implementation.

    Returns one dict per trap with ref/breakout/confirmation timestamps for audit.
    """
    traps: List[Dict] = []
    n = len(df_htf)
    if n < lookback + 2:
        return traps

    pending_bull: Optional[Dict] = None
    pending_bear: Optional[Dict] = None

    for i in range(lookback, n):
        base = df_htf.iloc[i - lookback:i]
        cur_low = float(df_htf["low"].iloc[i])
        cur_high = float(df_htf["high"].iloc[i])
        cur_close = float(df_htf["close"].iloc[i])
        cur_ts = df_htf["datetime"].iloc[i]

        # Update pending bull trap
        if pending_bull is not None:
            pending_bull["peak"] = max(pending_bull["peak"], cur_high)
            if cur_low < pending_bull["anchor_sl"]:
                pending_bull["confirm_ts"] = cur_ts
                pending_bull["trap_ts"] = cur_ts
                pending_bull["zone_high"] = pending_bull["peak"]
                traps.append(pending_bull.copy())
                pending_bull = None

        # Update pending bear trap
        if pending_bear is not None:
            pending_bear["peak"] = min(pending_bear["peak"], cur_low)
            if cur_high > pending_bear["anchor_sl"]:
                pending_bear["confirm_ts"] = cur_ts
                pending_bear["trap_ts"] = cur_ts
                pending_bear["zone_low"] = pending_bear["peak"]
                traps.append(pending_bear.copy())
                pending_bear = None

        # Start a new bull trap only if none is pending, or replace it with a stronger one
        if pending_bull is None or float(base["high"].max()) > pending_bull["breakout_line"]:
            resistance = float(base["high"].max())
            ref_idx = base["high"].idxmax()
            ref_ts = df_htf.loc[ref_idx, "datetime"]
            base_low = float(base["low"].min())
            if cur_close > resistance:
                pending_bull = {
                    "trap_ts": None,
                    "ref_ts": ref_ts,
                    "breakout_ts": cur_ts,
                    "confirm_ts": None,
                    "type": "Bull",
                    "multiplier": None,
                    "breakout_line": resistance,
                    "anchor_sl": base_low,
                    "peak": cur_high,
                    "zone_low": resistance,
                    "zone_high": cur_high,
                }

        # Start a new bear trap only if none is pending, or replace it with a stronger one
        if pending_bear is None or float(base["low"].min()) < pending_bear["breakout_line"]:
            support = float(base["low"].min())
            ref_idx = base["low"].idxmin()
            ref_ts = df_htf.loc[ref_idx, "datetime"]
            base_high = float(base["high"].max())
            if cur_close < support:
                pending_bear = {
                    "trap_ts": None,
                    "ref_ts": ref_ts,
                    "breakout_ts": cur_ts,
                    "confirm_ts": None,
                    "type": "Bear",
                    "multiplier": None,
                    "breakout_line": support,
                    "anchor_sl": base_high,
                    "peak": cur_low,
                    "zone_low": cur_low,
                    "zone_high": support,
                }

    # Unconfirmed pending traps are discarded — no structural trap until confirmed.
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


# ---------------------------------------------------------------------------
# Macro-to-micro trap execution engine
# ---------------------------------------------------------------------------

def _macro_to_htf_trap(macro: Dict) -> Dict:
    """Map a macro trap (Bull/Bear) to the V4 htf_trap shape used by MTF/LTF."""
    kind = "BULL" if macro["type"] == "Bull" else "BEAR"
    return {
        "kind": kind,
        "direction": "SHORT" if kind == "BULL" else "LONG",
        "breach_ts": macro["confirm_ts"],
        "breach_end": macro["confirm_ts"] + timedelta(minutes=macro.get("multiplier_min", HTF_MIN)),
        "target": macro["anchor_sl"],
        "htf_entry_level": macro["breakout_line"],
        "htf_breach_ts": macro["confirm_ts"],
    }


def _find_zone_reentry_ts(macro: Dict, df_1m: pd.DataFrame) -> Optional[datetime]:
    """First 1m timestamp after confirmation where price is back inside the validated zone."""
    after = df_1m[df_1m["datetime"] > macro["confirm_ts"]].copy()
    if after.empty:
        return None
    after = after.sort_values("datetime").reset_index(drop=True)
    if macro["type"] == "Bull":
        inside = after[(after["close"] >= macro["breakout_line"]) & (after["close"] <= macro["peak"])]
    else:
        inside = after[(after["close"] >= macro["peak"]) & (after["close"] <= macro["breakout_line"])]
    return inside["datetime"].iloc[0] if not inside.empty else None


def _has_rejection_bars(df: pd.DataFrame, macro: Dict) -> bool:
    """
    Detect a rejection candle: price pushes into the macro trap zone but the
    candle closes back inside the body range of its own preceding candle.

    Bull trap (short setup): a candle pushes up into the zone (high >= breakout_line)
    but closes inside the previous candle's body.
    Bear trap (long setup): a candle pushes down into the zone (low <= breakout_line)
    but closes inside the previous candle's body.
    """
    if len(df) < 2:
        return False
    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]
        prev_body_low = min(prev["open"], prev["close"])
        prev_body_high = max(prev["open"], prev["close"])
        if macro["type"] == "Bull":
            if curr["high"] >= macro["breakout_line"] and prev_body_low <= curr["close"] <= prev_body_high:
                return True
        else:
            if curr["low"] <= macro["breakout_line"] and prev_body_low <= curr["close"] <= prev_body_high:
                return True
    return False


def _check_mtf_ltf_rejection(
    macro: Dict,
    df_5m_full: pd.DataFrame,
    df_15m_full: pd.DataFrame,
    start_ts: datetime,
    end_ts: datetime,
) -> bool:
    """
    Verify structural rejection on 15m OR 5m between start_ts and end_ts.
    Returns True as soon as one valid rejection candle is found on either TF.
    """
    bars15 = df_15m_full[
        (df_15m_full["datetime"] >= start_ts) & (df_15m_full["datetime"] <= end_ts)
    ].copy()
    if _has_rejection_bars(bars15, macro):
        return True

    bars5 = df_5m_full[
        (df_5m_full["datetime"] >= start_ts) & (df_5m_full["datetime"] <= end_ts)
    ].copy()
    if _has_rejection_bars(bars5, macro):
        return True

    return False


def _simulate_micro_trade(
    ltf: Dict,
    df_1m: pd.DataFrame,
    eod: datetime,
    entry_mode: str = "close",
    index_name: str = "NIFTY",
) -> Optional[Dict]:
    """
    Execute a V4 LTF setup on the 1m stream.

    entry_mode:
      - "close": 1m candle must close above/below the prior 1m extreme
                 (long: close > prev_high; short: close < prev_low) and cross
                 the V4 1/3 retracement trigger.
      - "limit": pure limit order at the V4 1/3 retracement trigger; entry
                 fires as soon as price touches the trigger line.
      - "wick": 1m candle must wick past the prior 1m extreme
                (long: high > prev_high; short: low < prev_low) and cross the
                V4 1/3 retracement trigger.

    Entry price is always the V4 1/3 retracement trigger for consistent R:R
    bookkeeping.
    """
    trigger = ltf["trigger"]
    sl = ltf["sl"]
    target = ltf["target"]
    kind = ltf["kind"]
    htf_entry = ltf["htf_entry_level"]

    future = df_1m[df_1m["datetime"] > ltf["setup_ts"]].copy()
    if future.empty:
        return None
    future = future.sort_values("datetime").reset_index(drop=True)

    entry_ts = None
    for i in range(1, len(future)):
        prev = future.iloc[i - 1]
        curr = future.iloc[i]
        if curr["datetime"] > eod:
            return None

        if kind == "BEAR":  # long
            if entry_mode == "close":
                if curr["close"] > prev["high"] and curr["high"] >= trigger:
                    entry_ts = curr["datetime"]
                    break
            elif entry_mode == "limit":
                if curr["high"] >= trigger:
                    entry_ts = curr["datetime"]
                    break
            elif entry_mode == "wick":
                if curr["high"] > prev["high"] and curr["high"] >= trigger:
                    entry_ts = curr["datetime"]
                    break
            else:
                raise ValueError(f"Unknown entry_mode: {entry_mode}")
        else:  # short
            if entry_mode == "close":
                if curr["close"] < prev["low"] and curr["low"] <= trigger:
                    entry_ts = curr["datetime"]
                    break
            elif entry_mode == "limit":
                if curr["low"] <= trigger:
                    entry_ts = curr["datetime"]
                    break
            elif entry_mode == "wick":
                if curr["low"] < prev["low"] and curr["low"] <= trigger:
                    entry_ts = curr["datetime"]
                    break
            else:
                raise ValueError(f"Unknown entry_mode: {entry_mode}")

    if entry_ts is None:
        return None

    after = df_1m[df_1m["datetime"] >= entry_ts].copy()
    if after.empty:
        return None
    after = after.sort_values("datetime").reset_index(drop=True)

    exit_ts = None
    exit_spot = None
    exit_reason = "OPEN"
    void_lifted = False

    for _, row in after.iterrows():
        bar_end = row["datetime"] + timedelta(minutes=1)

        if kind == "BEAR":  # long
            if not void_lifted and row["low"] <= htf_entry:
                void_lifted = True
            if void_lifted:
                if row["low"] <= sl:
                    exit_spot, exit_reason, exit_ts = sl, "SL", min(bar_end, eod)
                    break
                if row["high"] >= target:
                    exit_spot, exit_reason, exit_ts = target, "TARGET", min(bar_end, eod)
                    break
            else:
                if row["low"] <= sl:
                    exit_spot, exit_reason, exit_ts = sl, "SL", min(bar_end, eod)
                    break
        else:  # short
            if not void_lifted and row["high"] >= htf_entry:
                void_lifted = True
            if void_lifted:
                if row["high"] >= sl:
                    exit_spot, exit_reason, exit_ts = sl, "SL", min(bar_end, eod)
                    break
                if row["low"] <= target:
                    exit_spot, exit_reason, exit_ts = target, "TARGET", min(bar_end, eod)
                    break
            else:
                if row["high"] >= sl:
                    exit_spot, exit_reason, exit_ts = sl, "SL", min(bar_end, eod)
                    break

        if bar_end >= eod:
            exit_spot, exit_reason, exit_ts = float(row["close"]), "EOD", eod
            break

    if exit_ts is None or exit_spot is None:
        return None

    tranche_units = INDEX_LOT_CONFIG.get(index_name, 75)
    pts = exit_spot - trigger if kind == "BEAR" else trigger - exit_spot
    return {
        "kind": kind,
        "direction": ltf["direction"],
        "macro_type": "Bull" if kind == "BULL" else "Bear",
        "index_name": index_name,
        "setup_ts": ltf["setup_ts"],
        "entry_ts": entry_ts,
        "entry_price": trigger,
        "entry_mode": entry_mode,
        "sl": sl,
        "target": target,
        "exit_ts": exit_ts,
        "exit_price": exit_spot,
        "exit_reason": exit_reason,
        "pts": round(pts, 2),
        "pnl_rs": round(pts * tranche_units, 2),
        "zone_high": ltf["zone_high"],
        "zone_low": ltf["zone_low"],
        "htf_entry_level": htf_entry,
        "void_lifted": void_lifted,
    }


def _compute_trailing_sl(
    kind: str,
    current_1m_ts: pd.Timestamp,
    df_5m_full: pd.DataFrame,
    df_15m_full: pd.DataFrame,
    current_sl: float,
    entry: float,
    initial_risk: float,
    running_best: float,
    trailing_activation_r: float = 1.5,
    trailing_tf: str = "5m",
    trailing_lookback: int = 2,
) -> float:
    """
    Dynamic structural trailing stop for Tranche 2.

    Parameters:
      trailing_activation_r: profit multiple (in R) at which trailing starts.
      trailing_tf: "5m" or "15m" — which timeframe to use for the trailing anchor.
      trailing_lookback: number of completed candles of trailing_tf to look back.

    For longs: trail at the lowest low of the lookback candles (only tightens).
    For shorts: trail at the highest high of the lookback candles (only tightens).
    """
    if kind == "BEAR":  # long
        if running_best < entry + trailing_activation_r * initial_risk:
            return current_sl
        if trailing_tf == "5m":
            current_tf_start = current_1m_ts.floor("5min")
            df_tf = df_5m_full
        elif trailing_tf == "15m":
            current_tf_start = current_1m_ts.floor("15min")
            df_tf = df_15m_full
        else:
            raise ValueError(f"Unknown trailing_tf: {trailing_tf}")
        prev_bars = df_tf[
            (df_tf["datetime"] < current_tf_start)
            & (df_tf["datetime"].dt.date == current_1m_ts.date())
        ].tail(trailing_lookback)
        if len(prev_bars) < trailing_lookback:
            return current_sl
        trail = float(prev_bars["low"].min())
        return max(current_sl, trail)
    else:  # short
        if running_best > entry - trailing_activation_r * initial_risk:
            return current_sl
        if trailing_tf == "5m":
            current_tf_start = current_1m_ts.floor("5min")
            df_tf = df_5m_full
        elif trailing_tf == "15m":
            current_tf_start = current_1m_ts.floor("15min")
            df_tf = df_15m_full
        else:
            raise ValueError(f"Unknown trailing_tf: {trailing_tf}")
        prev_bars = df_tf[
            (df_tf["datetime"] < current_tf_start)
            & (df_tf["datetime"].dt.date == current_1m_ts.date())
        ].tail(trailing_lookback)
        if len(prev_bars) < trailing_lookback:
            return current_sl
        trail = float(prev_bars["high"].max())
        return min(current_sl, trail)


def _simulate_micro_trade_tranches(
    ltf: Dict,
    df_1m: pd.DataFrame,
    df_5m_full: pd.DataFrame,
    df_15m_full: pd.DataFrame,
    eod: datetime,
    entry_mode: str = "close",
    trailing_activation_r: float = 1.5,
    trailing_tf: str = "5m",
    trailing_lookback: int = 2,
    index_name: str = "NIFTY",
) -> List[Dict]:
    """
    Dual-tranche risk-managed execution of a V4 LTF setup.

    Tranche 1 (50%): exits at 2R target or SL (initial or break-even).
    Tranche 2 (50%): exits via trailing stop after trailing_activation_r,
                     or break-even SL, or initial SL, or EOD.

    Break-even: once price reaches 1R profit, both tranches move SL to entry.
    Trailing: configurable via trailing_activation_r, trailing_tf ("5m" or "15m"),
    and trailing_lookback. Once the profit threshold is hit, Tranche 2 trails at the
    lowest low (long) or highest high (short) of the last `trailing_lookback` completed
    candles of `trailing_tf`.

    Returns a list of two tranche records (one per tranche). If no valid entry
    is found, returns an empty list.
    """
    trigger = ltf["trigger"]
    sl = ltf["sl"]
    target = ltf["target"]
    kind = ltf["kind"]
    htf_entry = ltf["htf_entry_level"]

    initial_risk = trigger - sl if kind == "BEAR" else sl - trigger
    if initial_risk <= 0:
        return []

    # --- 1m entry gate (same as single mode) ---
    future = df_1m[df_1m["datetime"] > ltf["setup_ts"]].copy()
    if future.empty:
        return []
    future = future.sort_values("datetime").reset_index(drop=True)

    entry_ts = None
    for i in range(1, len(future)):
        prev = future.iloc[i - 1]
        curr = future.iloc[i]
        if curr["datetime"] > eod:
            return []

        if kind == "BEAR":  # long
            if entry_mode == "close":
                if curr["close"] > prev["high"] and curr["high"] >= trigger:
                    entry_ts = curr["datetime"]
                    break
            elif entry_mode == "limit":
                if curr["high"] >= trigger:
                    entry_ts = curr["datetime"]
                    break
            elif entry_mode == "wick":
                if curr["high"] > prev["high"] and curr["high"] >= trigger:
                    entry_ts = curr["datetime"]
                    break
            else:
                raise ValueError(f"Unknown entry_mode: {entry_mode}")
        else:  # short
            if entry_mode == "close":
                if curr["close"] < prev["low"] and curr["low"] <= trigger:
                    entry_ts = curr["datetime"]
                    break
            elif entry_mode == "limit":
                if curr["low"] <= trigger:
                    entry_ts = curr["datetime"]
                    break
            elif entry_mode == "wick":
                if curr["low"] < prev["low"] and curr["low"] <= trigger:
                    entry_ts = curr["datetime"]
                    break
            else:
                raise ValueError(f"Unknown entry_mode: {entry_mode}")

    if entry_ts is None:
        return []

    after = df_1m[df_1m["datetime"] >= entry_ts].copy()
    if after.empty:
        return []
    after = after.sort_values("datetime").reset_index(drop=True)

    # Tranche 1 target = 2R
    t1_target = trigger + 2 * initial_risk if kind == "BEAR" else trigger - 2 * initial_risk

    t1_active, t2_active = True, True
    t1_sl, t2_sl = sl, sl
    t1_exit_ts = t2_exit_ts = None
    t1_exit_price = t2_exit_price = None
    t1_exit_reason = t2_exit_reason = None
    running_best = trigger

    for _, row in after.iterrows():
        bar_end = row["datetime"] + timedelta(minutes=1)

        # Update running best price (intrabar) for 1R / trailing thresholds
        if kind == "BEAR":
            running_best = max(running_best, row["high"])
        else:
            running_best = min(running_best, row["low"])

        # Break-even: at 1R, move both SLs to entry
        if kind == "BEAR":
            if running_best >= trigger + initial_risk:
                t1_sl = max(t1_sl, trigger)
                t2_sl = max(t2_sl, trigger)
        else:
            if running_best <= trigger - initial_risk:
                t1_sl = min(t1_sl, trigger)
                t2_sl = min(t2_sl, trigger)

        # Tranche 2 trailing stop (configurable activation and anchor)
        t2_sl = _compute_trailing_sl(
            kind, row["datetime"], df_5m_full, df_15m_full, t2_sl, trigger,
            initial_risk, running_best, trailing_activation_r, trailing_tf, trailing_lookback
        )

        # Tranche 1 exit: 2R target or SL
        if t1_active:
            if kind == "BEAR":
                if row["high"] >= t1_target:
                    t1_exit_price, t1_exit_reason, t1_exit_ts = t1_target, "TARGET_2R", min(bar_end, eod)
                    t1_active = False
                elif row["low"] <= t1_sl:
                    t1_exit_price, t1_exit_reason, t1_exit_ts = t1_sl, "SL", min(bar_end, eod)
                    t1_active = False
            else:
                if row["low"] <= t1_target:
                    t1_exit_price, t1_exit_reason, t1_exit_ts = t1_target, "TARGET_2R", min(bar_end, eod)
                    t1_active = False
                elif row["high"] >= t1_sl:
                    t1_exit_price, t1_exit_reason, t1_exit_ts = t1_sl, "SL", min(bar_end, eod)
                    t1_active = False

        # Tranche 2 exit: SL or EOD
        if t2_active:
            if kind == "BEAR":
                if row["low"] <= t2_sl:
                    t2_exit_price, t2_exit_reason, t2_exit_ts = t2_sl, "SL", min(bar_end, eod)
                    t2_active = False
            else:
                if row["high"] >= t2_sl:
                    t2_exit_price, t2_exit_reason, t2_exit_ts = t2_sl, "SL", min(bar_end, eod)
                    t2_active = False

        # EOD square-off for any remaining active tranche
        if bar_end >= eod:
            if t1_active:
                t1_exit_price, t1_exit_reason, t1_exit_ts = float(row["close"]), "EOD", eod
                t1_active = False
            if t2_active:
                t2_exit_price, t2_exit_reason, t2_exit_ts = float(row["close"]), "EOD", eod
                t2_active = False
            break

        if not t1_active and not t2_active:
            break

    if t1_exit_ts is None or t2_exit_ts is None:
        return []

    t1_pts = t1_exit_price - trigger if kind == "BEAR" else trigger - t1_exit_price
    t2_pts = t2_exit_price - trigger if kind == "BEAR" else trigger - t2_exit_price

    # 1 lot per tranche (2 lots total), index-specific lot size
    tranche_units = INDEX_LOT_CONFIG.get(index_name, 75)
    t1_pnl = t1_pts * tranche_units
    t2_pnl = t2_pts * tranche_units

    base = {
        "kind": kind,
        "direction": ltf["direction"],
        "macro_type": "Bull" if kind == "BULL" else "Bear",
        "index_name": index_name,
        "setup_ts": ltf["setup_ts"],
        "entry_ts": entry_ts,
        "entry_price": trigger,
        "entry_mode": entry_mode,
        "sl": sl,
        "target": target,
        "zone_high": ltf["zone_high"],
        "zone_low": ltf["zone_low"],
        "htf_entry_level": htf_entry,
        "initial_risk": round(initial_risk, 2),
    }

    return [
        {
            **base,
            "tranche": 1,
            "exit_ts": t1_exit_ts,
            "exit_price": t1_exit_price,
            "exit_reason": t1_exit_reason,
            "pts": round(t1_pts, 2),
            "pnl_rs": round(t1_pnl, 2),
        },
        {
            **base,
            "tranche": 2,
            "exit_ts": t2_exit_ts,
            "exit_price": t2_exit_price,
            "exit_reason": t2_exit_reason,
            "pts": round(t2_pts, 2),
            "pnl_rs": round(t2_pnl, 2),
        },
    ]


def simulate_macro_to_micro_trade(
    macro: Dict,
    df_1m: pd.DataFrame,
    df_5m_full: pd.DataFrame,
    df_15m_full: pd.DataFrame,
    max_adx: float = 22.5,
    rsi_long_min: float = 40.0,
    rsi_short_max: float = 60.0,
    use_vwap: bool = True,
    use_adx: bool = True,
    use_rsi: bool = True,
    entry_end: time = time(15, 15),
    intraday_exit: time = time(15, 30),
    require_zone_reentry: bool = False,
    entry_mode: str = "close",
    use_filters: bool = True,
    require_mtf_ltf_rejection: bool = False,
    dual_tranche: bool = False,
    trailing_activation_r: float = 1.5,
    trailing_tf: str = "5m",
    trailing_lookback: int = 2,
    index_name: str = "NIFTY",
) -> List[Dict]:
    """
    For a single confirmed macro trap, wait for price to re-enter the validated
    trap zone (unless require_zone_reentry=False, in which case we start at
    confirmation), then run the V4 15m -> 5m cascade and 1m entry gate.
    Returns a list of completed trade/tranche records (max one setup per macro trap).

    Set use_filters=False to run pure price action: ADX/RSI/VWAP gates are bypassed.
    Set require_mtf_ltf_rejection=True to require a 15m or 5m rejection candle
    inside the macro trap zone before the 1m entry is allowed.
    Set dual_tranche=True to split each position into 50/50 tranches with 1R
    break-even, 2R target for tranche 1, and configurable structural trailing stop
    for tranche 2 (controlled by trailing_activation_r, trailing_tf, trailing_lookback).
    """
    reentry_ts = _find_zone_reentry_ts(macro, df_1m)
    cascade_ts = reentry_ts if reentry_ts is not None else macro["confirm_ts"]
    if require_zone_reentry and reentry_ts is None:
        return []

    htf_trap = _macro_to_htf_trap(macro)
    htf_trap["breach_ts"] = cascade_ts
    htf_trap["breach_end"] = cascade_ts + timedelta(minutes=macro.get("multiplier_min", HTF_MIN))
    htf_trap["htf_breach_ts"] = cascade_ts

    day = cascade_ts.date()
    day_5m = df_5m_full[df_5m_full["datetime"].dt.date == day]
    day_15m = df_15m_full[df_15m_full["datetime"].dt.date == day]
    if day_5m.empty or day_15m.empty:
        return []

    eod = pd.Timestamp(f"{day} {intraday_exit}", tz="Asia/Kolkata")

    for mtf in find_mtf_15m_traps(day_15m, htf_trap):
        for ltf in find_ltf_5m_traps(day_5m, mtf):
            if ltf["setup_ts"].time() > entry_end:
                continue
            if use_filters and not check_ltf_filters(
                ltf, df_5m_full, max_adx, rsi_long_min, rsi_short_max,
                use_vwap, use_adx, use_rsi,
            ):
                continue
            if require_mtf_ltf_rejection:
                if not _check_mtf_ltf_rejection(
                    macro, df_5m_full, df_15m_full, cascade_ts, ltf["setup_ts"]
                ):
                    continue
            if dual_tranche:
                tranches = _simulate_micro_trade_tranches(
                    ltf, df_1m, df_5m_full, df_15m_full, eod, entry_mode=entry_mode,
                    trailing_activation_r=trailing_activation_r,
                    trailing_tf=trailing_tf,
                    trailing_lookback=trailing_lookback,
                    index_name=index_name,
                )
                if tranches:
                    for tr in tranches:
                        tr["macro_confirm_ts"] = macro["confirm_ts"]
                        tr["macro_reentry_ts"] = reentry_ts
                        tr["multiplier"] = macro.get("multiplier", f"{HTF_MIN}m")
                    return tranches
            else:
                trade = _simulate_micro_trade(ltf, df_1m, eod, entry_mode=entry_mode, index_name=index_name)
                if trade:
                    trade["macro_confirm_ts"] = macro["confirm_ts"]
                    trade["macro_reentry_ts"] = reentry_ts
                    trade["multiplier"] = macro.get("multiplier", f"{HTF_MIN}m")
                    return [trade]
    return []


def backtest_macro_to_micro(
    df_1m: pd.DataFrame,
    multipliers: List[int] = (75, 150, 225),
    lookback: int = 3,
    max_adx: float = 22.5,
    rsi_long_min: float = 40.0,
    rsi_short_max: float = 60.0,
    use_vwap: bool = True,
    use_adx: bool = True,
    use_rsi: bool = True,
    entry_start: time = time(9, 15),
    entry_end: time = time(15, 15),
    intraday_exit: time = time(15, 30),
    require_zone_reentry: bool = False,
    entry_mode: str = "close",
    use_filters: bool = True,
    require_mtf_ltf_rejection: bool = False,
    dual_tranche: bool = False,
    trailing_activation_r: float = 1.5,
    trailing_tf: str = "5m",
    trailing_lookback: int = 2,
    index_name: str = "NIFTY",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Run the macro-to-micro trap engine on NIFTY spot data.
    Returns (macro_table, trade_log).
    """
    df_5m_full = prepare_5m_with_indicators(df_1m)
    df_15m_full = _resample_per_day(df_1m, MTF_MIN)

    macro_records: List[Dict] = []
    trades: List[Dict] = []

    for mult in multipliers:
        df_htf = _resample_per_day(df_1m, mult)
        if df_htf.empty or len(df_htf) < lookback + 2:
            continue
        traps = detect_macro_htf_traps(df_htf, lookback=lookback)
        for t in traps:
            t["multiplier_min"] = mult
            t["multiplier"] = f"{mult}m"
            macro_records.append(t)
            tranches = simulate_macro_to_micro_trade(
                t, df_1m, df_5m_full, df_15m_full,
                max_adx=max_adx, rsi_long_min=rsi_long_min, rsi_short_max=rsi_short_max,
                use_vwap=use_vwap, use_adx=use_adx, use_rsi=use_rsi,
                entry_end=entry_end, intraday_exit=intraday_exit,
                require_zone_reentry=require_zone_reentry,
                entry_mode=entry_mode,
                use_filters=use_filters,
                require_mtf_ltf_rejection=require_mtf_ltf_rejection,
                dual_tranche=dual_tranche,
                trailing_activation_r=trailing_activation_r,
                trailing_tf=trailing_tf,
                trailing_lookback=trailing_lookback,
                index_name=index_name,
            )
            if tranches:
                trades.extend(tranches)

    macro_df = pd.DataFrame(macro_records)
    trades_df = pd.DataFrame(trades)
    return macro_df, trades_df


def summarize_macro_to_micro_trades(trades: pd.DataFrame) -> Dict:
    """Standard performance summary for the macro-to-micro trade log."""
    if trades.empty:
        return {
            "total": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
            "gross_profit": 0.0, "gross_loss": 0.0, "net_pnl": 0.0,
            "profit_factor": 0.0, "avg_win": 0.0, "avg_loss": 0.0, "rr": 0.0,
            "max_dd": 0.0, "setup_count": 0,
        }

    pnls = trades["pnl_rs"].values
    wins = pnls[pnls > 0]
    losses = pnls[pnls < 0]
    gp = float(wins.sum()) if len(wins) else 0.0
    gl = abs(float(losses.sum())) if len(losses) else 0.0
    net = float(pnls.sum())
    pf = gp / gl if gl > 0 else float("inf")
    avg_win = float(wins.mean()) if len(wins) else 0.0
    avg_loss = abs(float(losses.mean())) if len(losses) else 0.0
    rr = avg_win / avg_loss if avg_loss > 0 else 0.0

    cum = pnls.cumsum()
    cummax = np.maximum.accumulate(cum)
    max_dd = float((cummax - cum).max())

    setup_count = trades["setup_ts"].nunique() if "setup_ts" in trades.columns else len(trades)

    return {
        "total": len(trades), "wins": len(wins), "losses": len(losses),
        "win_rate": 100 * len(wins) / len(trades), "gross_profit": gp,
        "gross_loss": gl, "net_pnl": net, "profit_factor": pf,
        "avg_win": avg_win, "avg_loss": avg_loss, "rr": rr, "max_dd": max_dd,
        "setup_count": setup_count,
    }
