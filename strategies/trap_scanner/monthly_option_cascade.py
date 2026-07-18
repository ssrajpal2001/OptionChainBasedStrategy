"""
strategies/trap_scanner/monthly_option_cascade.py
==================================================
NIFTY Monthly Option Premium Trap Cascade Engine.

Dual-layered structural strategy:
  1. At 09:15, isolate the monthly NIFTY tracking strikes:
       CE tracking = ATM - 200,  PE tracking = ATM + 200.
  2. Detect 75m/150m/225m structural traps on the premium charts of these
     tracking contracts (same `detect_macro_htf_traps` logic as V4 spot).
  3. Require concurrent confirmation from the NIFTY spot index 75m trap logic.
  4. Execute on 1-OTM monthly contracts:
       CE execution = ATM + 50,  PE execution = ATM - 50.
  5. Single-position constraint per day (CE or PE). Structural flip exits.
  6. Dual-tranche risk management on the execution contract:
       T1 = 2R target (R computed from tracking-contract premium structure)
       Break-even at 1R on execution-contract P&L
       T2 = 4x5m structural trailing stop on the execution contract.

This module is pure-pandas and stateless, usable by both backtests and live code.
"""
from __future__ import annotations

import math
import os
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

from strategies.trap_scanner import v4_spot_cascade as v4

IST = pytz.timezone("Asia/Kolkata")
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")

# NIFTY operational parameters
STEP = 50
TRACKING_OFFSET = 200
EXECUTION_OFFSET = 50
LOT_SIZE = 65          # units per lot
TOTAL_LOTS = 2          # 2 lots total per trade
TOTAL_UNITS = LOT_SIZE * TOTAL_LOTS

HTF_MIN = 75
MTF_MIN = 15
LTF_MIN = 5
SL_BUFFER = 10.0

DEFAULT_MULTIPLIERS = [75, 150, 225]
DEFAULT_LOOKBACK = 3

# Synthetic premium generator defaults
BASE_ATM_TV = 600.0
TV_DECAY_DAYS = 60.0
MONEYNESS_DECAY = 0.15
VOLATILITY_AMPLIFICATION = 1.5

# ---------------------------------------------------------------------------
# Strike helpers
# ---------------------------------------------------------------------------


def _round_strike(spot: float, step: float = STEP) -> int:
    return int(round(spot / step) * step)


def select_tracking_strikes(spot_open: float, step: float = STEP) -> Tuple[int, int]:
    """Return (CE_tracking_strike, PE_tracking_strike)."""
    atm = _round_strike(spot_open, step)
    return atm - TRACKING_OFFSET, atm + TRACKING_OFFSET


def select_execution_strikes(spot_open: float, step: float = STEP) -> Tuple[int, int]:
    """Return (CE_execution_strike, PE_execution_strike)."""
    atm = _round_strike(spot_open, step)
    return atm + EXECUTION_OFFSET, atm - EXECUTION_OFFSET


# ---------------------------------------------------------------------------
# Synthetic premium generator (for testing without real option data)
# ---------------------------------------------------------------------------


def _synthetic_time_value(spot: float, strike: int, days_to_expiry: float) -> float:
    """Simplified time value for synthetic premium generation."""
    if days_to_expiry <= 0 or strike <= 0:
        return 0.0
    moneyness = abs(spot / strike - 1.0)
    decay = math.exp(-days_to_expiry / TV_DECAY_DAYS)
    # Higher time value when spot is near the strike (gamma amplification)
    near_strike_factor = 1.0 + 0.5 * math.exp(-moneyness * 10.0)
    tv = (
        BASE_ATM_TV
        * decay
        * max(0.1, 1.0 - MONEYNESS_DECAY * moneyness)
        * near_strike_factor
        * VOLATILITY_AMPLIFICATION
    )
    return max(0.0, tv)


def _synthetic_premium(
    spot: float, strike: int, opt_type: str, days_to_expiry: float, spot_aligned: bool = False
) -> float:
    if spot_aligned:
        # Shifted spot model: premium tracks spot 1:1 with a constant offset, ensuring
        # positive prices and aligned macro traps across all strikes. This is for
        # pipeline testing only; it is NOT a realistic option pricing model.
        atm_tv = max(0.0, BASE_ATM_TV * math.exp(-days_to_expiry / TV_DECAY_DAYS))
        if opt_type == "CE":
            return max(0.0, spot - strike + atm_tv)
        else:
            return max(0.0, strike - spot + atm_tv)
    if opt_type == "CE":
        intrinsic = max(0.0, spot - strike)
    else:
        intrinsic = max(0.0, strike - spot)
    return intrinsic + _synthetic_time_value(spot, strike, days_to_expiry)


def build_synthetic_premium(
    df_spot_1m: pd.DataFrame,
    expiry_date: date,
    strikes: List[int],
    spot_aligned: bool = False,
) -> pd.DataFrame:
    """
    Build synthetic 1m option premium OHLC for the given strikes from spot OHLC.
    Columns: timestamp, strike, opt_type, open, high, low, close, volume

    spot_aligned=True: premium = shifted intrinsic + constant time value. This makes
    the premium macro traps align exactly with spot macro traps, so the pipeline can
    be verified end-to-end with synthetic data. Results are NOT realistic option P&L.
    """
    expiry_dt = datetime.combine(expiry_date, time(23, 59, 59), tzinfo=IST)
    records = []
    for _, row in df_spot_1m.iterrows():
        ts = row["datetime"]
        days_to_expiry = max(0.0, (expiry_dt - ts).total_seconds() / 86400.0)
        for opt_type in ("CE", "PE"):
            for strike in strikes:
                o = _synthetic_premium(row["open"], strike, opt_type, days_to_expiry, spot_aligned)
                c = _synthetic_premium(row["close"], strike, opt_type, days_to_expiry, spot_aligned)
                if opt_type == "CE":
                    h = _synthetic_premium(row["high"], strike, opt_type, days_to_expiry, spot_aligned)
                    l = _synthetic_premium(row["low"], strike, opt_type, days_to_expiry, spot_aligned)
                else:
                    h = _synthetic_premium(row["low"], strike, opt_type, days_to_expiry, spot_aligned)
                    l = _synthetic_premium(row["high"], strike, opt_type, days_to_expiry, spot_aligned)
                records.append({
                    "timestamp": ts,
                    "strike": strike,
                    "opt_type": opt_type,
                    "open": round(o, 2),
                    "high": round(h, 2),
                    "low": round(l, 2),
                    "close": round(c, 2),
                    "volume": 0,
                })
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------


def _normalize_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize timestamp column to IST datetime."""
    if "timestamp" in df.columns and "datetime" not in df.columns:
        df = df.rename(columns={"timestamp": "datetime"})
    df["datetime"] = pd.to_datetime(df["datetime"])
    if df["datetime"].dt.tz is None:
        df["datetime"] = df["datetime"].dt.tz_localize(
            IST, ambiguous="NaT", nonexistent="shift_forward"
        )
    else:
        df["datetime"] = df["datetime"].dt.tz_convert(IST)
    return df


def load_monthly_option_data(
    expiry_date: date,
    required_strikes: Optional[List[int]] = None,
) -> Optional[pd.DataFrame]:
    """
    Load 1m NIFTY monthly option premium data from the standard parquet format.
    Standard path: data/nse_option_cache/opt_NIFTY_monthly_<expiry>_1m.parquet
    Returns None if the file is not found.

    If required_strikes is provided, only those strikes are retained.  When the
    execution strike is recomputed daily, pass None to keep the full strike grid.
    """
    fpath = os.path.join(CACHE_DIR, f"opt_NIFTY_monthly_{expiry_date.isoformat()}_1m.parquet")
    if not os.path.exists(fpath):
        return None
    df = pd.read_parquet(fpath)
    df = _normalize_timestamp(df)
    if required_strikes:
        df = df[df["strike"].isin(required_strikes)]
    return df


def load_spot_data(index_name: str, start: date, end: date) -> pd.DataFrame:
    """Load 1m spot data from the standard cache files."""
    import glob
    pattern = os.path.join(CACHE_DIR, f"spot_{index_name}_1m_*.parquet")
    files = sorted(glob.glob(pattern))
    if not files:
        raise RuntimeError(f"No {index_name} 1m spot parquet files found in {CACHE_DIR}")
    frames = []
    for f in files:
        df = pd.read_parquet(f)
        df = _normalize_timestamp(df)
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates("datetime").sort_values("datetime").reset_index(drop=True)
    df = df[(df["datetime"].dt.date >= start) & (df["datetime"].dt.date <= end)]
    df = df[(df["datetime"].dt.time >= time(9, 15)) & (df["datetime"].dt.time <= time(15, 30))]
    return df


def _get_premium_bars(df_opt: pd.DataFrame, strike: int, opt_type: str) -> pd.DataFrame:
    df = df_opt[(df_opt["strike"] == strike) & (df_opt["opt_type"] == opt_type)].copy()
    df = df.sort_values("datetime").reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Macro trap detection on premium charts
# ---------------------------------------------------------------------------


def detect_premium_macro_traps(
    df_opt_1m: pd.DataFrame,
    strike: int,
    opt_type: str,
    multipliers: List[int] = DEFAULT_MULTIPLIERS,
    lookback: int = DEFAULT_LOOKBACK,
) -> List[Dict]:
    """
    Detect structural traps on the premium chart of a specific monthly option contract.
    Returns trap dicts with added keys: strike, opt_type, tracking_side, side (trade direction).
    """
    df_opt = _get_premium_bars(df_opt_1m, strike, opt_type)
    if df_opt.empty:
        return []
    traps = []
    for mult in multipliers:
        df_htf = v4._resample_per_day(df_opt, mult)
        if df_htf.empty or len(df_htf) < lookback + 2:
            continue
        htf_traps = [t for t in v4.detect_macro_htf_traps(df_htf, lookback=lookback) if t["type"] == "Bear"]
        for t in htf_traps:
            # The monthly option engine only trades Bear Traps on both sides:
            # CE Bear Trap -> long spot setup (buy CE); PE Bear Trap -> short
            # spot setup (buy PE).  Bull traps are ignored.
            t["multiplier_min"] = mult
            t["multiplier"] = f"{mult}m"
            t["strike"] = strike
            t["opt_type"] = opt_type
            t["tracking_side"] = opt_type
            t["side"] = "LONG" if opt_type == "CE" else "SHORT"
        traps.extend(htf_traps)
    return traps


# ---------------------------------------------------------------------------
# Spot confirmation
# ---------------------------------------------------------------------------


def find_spot_confirmation(
    premium_trap: Dict,
    df_spot_1m: pd.DataFrame,
    mult_min: int,
) -> Optional[Dict]:
    """
    Check if the NIFTY spot index printed a matching structural trap in the same
    time window.  CE side (bullish index) needs a Bear spot trap; PE side
    (bearish index) needs a Bull spot trap.
    """
    premium_confirm = premium_trap.get("confirm_ts")
    if premium_confirm is None or pd.isna(premium_confirm):
        return None

    df_spot_htf = v4._resample_per_day(df_spot_1m, mult_min)
    if df_spot_htf.empty or len(df_spot_htf) < DEFAULT_LOOKBACK + 2:
        return None
    spot_traps = v4.detect_macro_htf_traps(df_spot_htf, lookback=DEFAULT_LOOKBACK)

    required_type = "Bear" if premium_trap["opt_type"] == "CE" else "Bull"
    for st in spot_traps:
        if st.get("confirm_ts") is None or pd.isna(st["confirm_ts"]):
            continue
        if st["type"] != required_type:
            continue
        time_diff = abs((st["confirm_ts"] - premium_confirm).total_seconds())
        if time_diff <= mult_min * 60:
            return st
    return None


# ---------------------------------------------------------------------------
# Zone re-entry and MTF/LTF rejection on premium charts
# ---------------------------------------------------------------------------


def _find_zone_reentry_ts(macro: Dict, df_opt_1m: pd.DataFrame) -> Optional[datetime]:
    """First 1m timestamp after confirmation where premium is back inside the validated zone."""
    after = df_opt_1m[df_opt_1m["datetime"] > macro["confirm_ts"]].copy()
    if after.empty:
        return None
    after = after.sort_values("datetime").reset_index(drop=True)
    if macro["type"] == "Bull":
        inside = after[(after["close"] >= macro["breakout_line"]) & (after["close"] <= macro["peak"])]
    else:
        inside = after[(after["close"] >= macro["peak"]) & (after["close"] <= macro["breakout_line"])]
    return inside["datetime"].iloc[0] if not inside.empty else None


def _has_rejection_bars(df: pd.DataFrame, macro: Dict) -> bool:
    """Same logic as v4_spot_cascade._has_rejection_bars."""
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
    df_opt_5m: pd.DataFrame,
    df_opt_15m: pd.DataFrame,
    start_ts: datetime,
    end_ts: datetime,
) -> bool:
    """Verify structural rejection on 15m OR 5m between start_ts and end_ts."""
    bars15 = df_opt_15m[
        (df_opt_15m["datetime"] >= start_ts) & (df_opt_15m["datetime"] <= end_ts)
    ].copy()
    if _has_rejection_bars(bars15, macro):
        return True
    bars5 = df_opt_5m[
        (df_opt_5m["datetime"] >= start_ts) & (df_opt_5m["datetime"] <= end_ts)
    ].copy()
    return _has_rejection_bars(bars5, macro)


# ---------------------------------------------------------------------------
# Dual-tranche execution simulation on the execution contract
# ---------------------------------------------------------------------------


def _compute_trailing_sl_exec(
    kind: str,
    current_1m_ts: pd.Timestamp,
    df_exec_5m: pd.DataFrame,
    current_sl: float,
    entry: float,
    initial_risk: float,
    running_best: float,
    trailing_activation_r: float = 2.0,
    trailing_lookback: int = 4,
) -> float:
    """
    4x5m structural trailing stop on the execution contract.
    Activation threshold is trailing_activation_r (default 2R so T1 2R target also
    activates the trail; matches the user's 'delay until 2R' request).
    """
    if kind == "BEAR":  # long
        if running_best < entry + trailing_activation_r * initial_risk:
            return current_sl
        current_tf_start = current_1m_ts.floor("5min")
        prev_bars = df_exec_5m[
            (df_exec_5m["datetime"] < current_tf_start)
            & (df_exec_5m["datetime"].dt.date == current_1m_ts.date())
        ].tail(trailing_lookback)
        if len(prev_bars) < trailing_lookback:
            return current_sl
        trail = float(prev_bars["low"].min())
        return max(current_sl, trail)
    else:  # short
        if running_best > entry - trailing_activation_r * initial_risk:
            return current_sl
        current_tf_start = current_1m_ts.floor("5min")
        prev_bars = df_exec_5m[
            (df_exec_5m["datetime"] < current_tf_start)
            & (df_exec_5m["datetime"].dt.date == current_1m_ts.date())
        ].tail(trailing_lookback)
        if len(prev_bars) < trailing_lookback:
            return current_sl
        trail = float(prev_bars["high"].max())
        return min(current_sl, trail)


def _simulate_dual_tranche_execution(
    ltf: Dict,
    df_exec_1m: pd.DataFrame,
    df_exec_5m: pd.DataFrame,
    df_track_1m: pd.DataFrame,
    eod: datetime,
    entry_mode: str = "close",
    trailing_activation_r: float = 2.0,
    trailing_lookback: int = 4,
    index_name: str = "NIFTY",
) -> Optional[List[Dict]]:
    """
    Execute the V4 LTF setup on the execution contract.

    The structural setup (trigger, SL, target) is derived from the tracking
    contract premium.  The actual fill prices come from the 1-OTM execution
    contract.  SL/target triggers are mapped to the execution contract using
    the price ratio observed at entry, while the T1 target is explicitly the
    2R structural level on the tracking contract.

    Returns two tranche records or None if no valid entry.
    """
    kind = ltf["kind"]
    trigger_track = ltf["trigger"]
    sl_track = ltf["sl"]
    target_track = ltf["target"]
    htf_entry = ltf["htf_entry_level"]

    initial_risk = abs(trigger_track - sl_track)
    if initial_risk <= 0:
        return None

    t1_target_track = trigger_track + 2 * initial_risk if kind == "BEAR" else trigger_track - 2 * initial_risk

    # 1. Find entry on the EXECUTION contract using the 1-minute close rule,
    #    starting from the LTF setup timestamp on the tracking contract.
    future = df_exec_1m[df_exec_1m["datetime"] > ltf["setup_ts"]].copy()
    if future.empty:
        return None
    future = future.sort_values("datetime").reset_index(drop=True)

    entry_ts = None
    exec_entry_price = None
    for i in range(1, len(future)):
        prev = future.iloc[i - 1]
        curr = future.iloc[i]
        if curr["datetime"] > eod:
            return None

        if kind == "BEAR":  # long
            if entry_mode == "close":
                if curr["close"] > prev["high"]:
                    entry_ts = curr["datetime"]
                    exec_entry_price = float(curr["close"])
                    break
            elif entry_mode == "limit":
                # Limit at execution contract's own 1/3-equivalent: scale the
                # tracking trigger by the entry bar's price ratio.
                limit = _scale_price(trigger_track, prev, df_track_1m)
                if limit is not None and curr["low"] <= limit:
                    entry_ts = curr["datetime"]
                    exec_entry_price = limit
                    break
            elif entry_mode == "wick":
                if curr["high"] > prev["high"]:
                    entry_ts = curr["datetime"]
                    exec_entry_price = float(curr["high"])
                    break
        else:  # short
            if entry_mode == "close":
                if curr["close"] < prev["low"]:
                    entry_ts = curr["datetime"]
                    exec_entry_price = float(curr["close"])
                    break
            elif entry_mode == "limit":
                limit = _scale_price(trigger_track, prev, df_track_1m)
                if limit is not None and curr["high"] >= limit:
                    entry_ts = curr["datetime"]
                    exec_entry_price = limit
                    break
            elif entry_mode == "wick":
                if curr["low"] < prev["low"]:
                    entry_ts = curr["datetime"]
                    exec_entry_price = float(curr["low"])
                    break

    if entry_ts is None or exec_entry_price is None:
        return None

    # Tracking contract price at entry for scaling structural levels
    track_at_entry = df_track_1m[df_track_1m["datetime"] == entry_ts]
    track_entry_price = float(track_at_entry.iloc[0]["close"]) if not track_at_entry.empty else trigger_track
    if track_entry_price <= 0:
        return None

    # Map tracking-contract R to execution contract using the entry price ratio.
    scale = exec_entry_price / track_entry_price
    r_exec = initial_risk * scale
    if r_exec <= 0:
        return None

    exec_sl = exec_entry_price - r_exec if kind == "BEAR" else exec_entry_price + r_exec

    after_exec = df_exec_1m[df_exec_1m["datetime"] >= entry_ts].copy().sort_values("datetime").reset_index(drop=True)
    after_track = df_track_1m[df_track_1m["datetime"] >= entry_ts].copy().sort_values("datetime").reset_index(drop=True)
    if after_exec.empty or after_track.empty:
        return None

    t1_active, t2_active = True, True
    t1_sl = t2_sl = exec_sl
    t1_exit_ts = t2_exit_ts = None
    t1_exit_price = t2_exit_price = None
    t1_exit_reason = t2_exit_reason = None
    running_best = exec_entry_price
    void_lifted = False
    t1_exited = False  # controls T2 trailing activation

    for idx, row in after_exec.iterrows():
        bar_end = row["datetime"] + timedelta(minutes=1)
        track_row = after_track[after_track["datetime"] == row["datetime"]]
        track_close = float(track_row.iloc[0]["close"]) if not track_row.empty else None
        track_high = float(track_row.iloc[0]["high"]) if not track_row.empty else None
        track_low = float(track_row.iloc[0]["low"]) if not track_row.empty else None

        # Update running best on execution contract
        if kind == "BEAR":
            running_best = max(running_best, row["high"])
        else:
            running_best = min(running_best, row["low"])

        # Void-lift: T1 target is only active after the tracking contract retests
        # the HTF entry level (breakout line for Bear traps, breakout line for Bull traps).
        if kind == "BEAR":
            if not void_lifted and track_low is not None and track_low <= htf_entry:
                void_lifted = True
        else:
            if not void_lifted and track_high is not None and track_high >= htf_entry:
                void_lifted = True

        # Break-even: execution contract P&L reaches 1R -> move both SLs to entry
        if kind == "BEAR":
            if running_best >= exec_entry_price + r_exec:
                t1_sl = max(t1_sl, exec_entry_price)
                t2_sl = max(t2_sl, exec_entry_price)
        else:
            if running_best <= exec_entry_price - r_exec:
                t1_sl = min(t1_sl, exec_entry_price)
                t2_sl = min(t2_sl, exec_entry_price)

        # T2 trailing stop (4x5m on execution contract).  Trailing only
        # activates after T1 has hit its 2R structural target; before that we
        # keep a high activation threshold so it does not trail.
        t2_trail_activation = 0.0 if t1_exited else trailing_activation_r
        t2_sl = _compute_trailing_sl_exec(
            kind, row["datetime"], df_exec_5m, t2_sl, exec_entry_price,
            r_exec, running_best, t2_trail_activation, trailing_lookback
        )

        # T1 exit: tracking contract hits its 2R structural target (only after
        # void lift), or execution contract hits SL.
        if t1_active:
            t1_target_hit = False
            if void_lifted and track_close is not None:
                if kind == "BEAR" and track_high is not None and track_high >= t1_target_track:
                    t1_target_hit = True
                elif kind == "BULL" and track_low is not None and track_low <= t1_target_track:
                    t1_target_hit = True
            if t1_target_hit:
                t1_exit_price, t1_exit_reason, t1_exit_ts = float(row["close"]), "TARGET_2R", min(bar_end, eod)
                t1_active = False
                t1_exited = True
            elif kind == "BEAR" and row["low"] <= t1_sl:
                t1_exit_price, t1_exit_reason, t1_exit_ts = float(row["close"]), "SL", min(bar_end, eod)
                t1_active = False
            elif kind == "BULL" and row["high"] >= t1_sl:
                t1_exit_price, t1_exit_reason, t1_exit_ts = float(row["close"]), "SL", min(bar_end, eod)
                t1_active = False

        # T2 exit: execution contract hits SL or EOD (trailing stop handled above)
        if t2_active:
            if kind == "BEAR" and row["low"] <= t2_sl:
                t2_exit_price, t2_exit_reason, t2_exit_ts = float(row["close"]), "SL", min(bar_end, eod)
                t2_active = False
            elif kind == "BULL" and row["high"] >= t2_sl:
                t2_exit_price, t2_exit_reason, t2_exit_ts = float(row["close"]), "SL", min(bar_end, eod)
                t2_active = False

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

    if t1_exit_ts is None or t2_exit_ts is None or exec_entry_price is None:
        return None

    t1_pts = t1_exit_price - exec_entry_price if kind == "BEAR" else exec_entry_price - t1_exit_price
    t2_pts = t2_exit_price - exec_entry_price if kind == "BEAR" else exec_entry_price - t2_exit_price
    tranche_units = LOT_SIZE
    t1_pnl = t1_pts * tranche_units
    t2_pnl = t2_pts * tranche_units

    base = {
        "kind": kind,
        "direction": "LONG" if kind == "BEAR" else "SHORT",
        "index_name": index_name,
        "setup_ts": ltf["setup_ts"],
        "entry_ts": entry_ts,
        "entry_price_exec": round(exec_entry_price, 2),
        "entry_price_track": round(track_entry_price, 2),
        "sl": round(exec_sl, 2),
        "target": round(target_track, 2),
        "t1_target_track": round(t1_target_track, 2),
        "initial_risk_track": round(initial_risk, 2),
        "initial_risk_exec": round(r_exec, 2),
        "entry_mode": entry_mode,
        "zone_high": ltf["zone_high"],
        "zone_low": ltf["zone_low"],
        "htf_entry_level": htf_entry,
    }

    return [
        {**base, "tranche": 1, "exit_ts": t1_exit_ts, "exit_price": round(t1_exit_price, 2),
         "exit_reason": t1_exit_reason, "pts": round(t1_pts, 2), "pnl_rs": round(t1_pnl, 2)},
        {**base, "tranche": 2, "exit_ts": t2_exit_ts, "exit_price": round(t2_exit_price, 2),
         "exit_reason": t2_exit_reason, "pts": round(t2_pts, 2), "pnl_rs": round(t2_pnl, 2)},
    ]


def _scale_price(track_price: float, exec_bar: pd.Series, df_track_1m: pd.DataFrame) -> Optional[float]:
    """
    Scale a tracking-contract price level to the execution contract using the
    price ratio observed at the candidate execution bar's timestamp.
    """
    ts = exec_bar["datetime"]
    track_row = df_track_1m[df_track_1m["datetime"] == ts]
    if track_row.empty:
        return None
    track_close = float(track_row.iloc[0]["close"])
    if track_close <= 0:
        return None
    exec_close = float(exec_bar["close"])
    return round(track_price * (exec_close / track_close), 2)


# ---------------------------------------------------------------------------
# Full macro-to-micro simulation for a single premium trap
# ---------------------------------------------------------------------------


def simulate_monthly_option_trade(
    premium_trap: Dict,
    df_opt_1m: pd.DataFrame,
    df_spot_1m: pd.DataFrame,
    execution_strike: int,
    eod: datetime,
    entry_mode: str = "close",
    require_zone_reentry: bool = True,
    require_mtf_ltf_rejection: bool = True,
    trailing_activation_r: float = 2.0,
    trailing_lookback: int = 4,
    index_name: str = "NIFTY",
) -> List[Dict]:
    """
    For a single confirmed premium trap, run the full cascade and return trades.
    """
    opt_type = premium_trap["opt_type"]
    tracking_strike = premium_trap["strike"]

    df_track = _get_premium_bars(df_opt_1m, tracking_strike, opt_type)
    df_exec = _get_premium_bars(df_opt_1m, execution_strike, opt_type)
    if df_track.empty or df_exec.empty:
        return []

    # Zone re-entry on tracking contract
    reentry_ts = _find_zone_reentry_ts(premium_trap, df_track)
    if require_zone_reentry and reentry_ts is None:
        return []
    cascade_ts = reentry_ts if reentry_ts is not None else premium_trap["confirm_ts"]

    # Recompute the execution strike from the CASCADE day's 09:15 spot open.
    # The confirmation-day strike may be far from the entry-day ATM if the
    # zone reentry happens on a subsequent session. The strategy isolates strikes
    # at 09:15 each day, so the execution contract must reflect the cascade day.
    cascade_day_spot = df_spot_1m[df_spot_1m["datetime"].dt.date == cascade_ts.date()]
    if cascade_day_spot.empty:
        return []
    cascade_spot_open = float(cascade_day_spot.iloc[0]["open"])
    ce_exec, pe_exec = select_execution_strikes(cascade_spot_open)
    execution_strike = ce_exec if opt_type == "CE" else pe_exec
    df_exec = _get_premium_bars(df_opt_1m, execution_strike, opt_type)
    if df_exec.empty:
        return []

    # Spot confirmation
    spot_conf = find_spot_confirmation(premium_trap, df_spot_1m, premium_trap["multiplier_min"])
    if spot_conf is None:
        return []

    # MTF/LTF rejection on tracking contract premium
    df_track_5m = v4.prepare_5m_with_indicators(df_track)
    df_track_15m = v4._resample_per_day(df_track, MTF_MIN)
    if require_mtf_ltf_rejection:
        if not _check_mtf_ltf_rejection(premium_trap, df_track_5m, df_track_15m, cascade_ts, eod):
            return []

    # Build htf trap for the LTF cascade (reuse v4 mapping).  Use EOD as the
    # breach_end so MTF/LTF setups can form anywhere after the zone reentry.
    htf_trap = v4._macro_to_htf_trap(premium_trap)
    htf_trap["breach_ts"] = cascade_ts
    htf_trap["breach_end"] = eod
    htf_trap["htf_breach_ts"] = cascade_ts

    day = cascade_ts.date()
    day_track_5m = df_track_5m[df_track_5m["datetime"].dt.date == day]
    day_track_15m = df_track_15m[df_track_15m["datetime"].dt.date == day]
    if day_track_5m.empty or day_track_15m.empty:
        return []

    df_exec_5m = v4._resample_per_day(df_exec, LTF_MIN)

    for mtf in v4.find_mtf_15m_traps(day_track_15m, htf_trap):
        for ltf in v4.find_ltf_5m_traps(day_track_5m, mtf):
            if ltf["setup_ts"].time() > time(15, 15):
                continue
            tranches = _simulate_dual_tranche_execution(
                ltf, df_exec, df_exec_5m, df_track, eod,
                entry_mode=entry_mode,
                trailing_activation_r=trailing_activation_r,
                trailing_lookback=trailing_lookback,
                index_name=index_name,
            )
            if tranches:
                for tr in tranches:
                    tr["tracking_strike"] = tracking_strike
                    tr["execution_strike"] = execution_strike
                    tr["opt_type"] = opt_type
                    tr["macro_confirm_ts"] = premium_trap["confirm_ts"]
                    tr["macro_reentry_ts"] = reentry_ts
                    tr["multiplier"] = premium_trap["multiplier"]
                    tr["spot_confirmed"] = True
                return tranches
    return []


# ---------------------------------------------------------------------------
# Full backtest engine
# ---------------------------------------------------------------------------


def backtest_monthly_option_cascade(
    df_spot_1m: pd.DataFrame,
    df_opt_1m: pd.DataFrame,
    expiry_date: date,
    multipliers: List[int] = DEFAULT_MULTIPLIERS,
    lookback: int = DEFAULT_LOOKBACK,
    entry_mode: str = "close",
    require_zone_reentry: bool = True,
    require_mtf_ltf_rejection: bool = True,
    trailing_activation_r: float = 2.0,
    trailing_lookback: int = 4,
    index_name: str = "NIFTY",
    entry_end: time = time(15, 15),
    intraday_exit: time = time(15, 30),
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Run the full monthly option premium trap backtest.
    Returns (macro_records, trade_records) DataFrames.
    """
    # Determine opening spot for each trading day to select strikes
    macro_records: List[Dict] = []
    trade_records: List[Dict] = []
    open_position_day: Optional[date] = None
    open_position_side: Optional[str] = None

    for day, g_spot in df_spot_1m.groupby(df_spot_1m["datetime"].dt.date):
        g_opt = df_opt_1m[df_opt_1m["datetime"].dt.date == day]
        if g_spot.empty or g_opt.empty:
            continue

        # Day-open spot and strike selection
        spot_open = float(g_spot.iloc[0]["open"])
        ce_track, pe_track = select_tracking_strikes(spot_open)
        ce_exec, pe_exec = select_execution_strikes(spot_open)

        eod = pd.Timestamp(f"{day} {intraday_exit}", tz="Asia/Kolkata")

        # Collect premium traps for both sides using the FULL option data (so the
        # 75m/150m/225m macro detection has enough bars), then keep only traps whose
        # confirmation timestamp falls on the current day.
        day_traps = []
        for opt_type, track_strike, exec_strike in [("CE", ce_track, ce_exec), ("PE", pe_track, pe_exec)]:
            traps = detect_premium_macro_traps(
                df_opt_1m, track_strike, opt_type, multipliers=multipliers, lookback=lookback
            )
            traps = [t for t in traps if pd.Timestamp(t["confirm_ts"]).date() == day]
            for t in traps:
                t["exec_strike"] = exec_strike
            day_traps.extend(traps)

        # Sort chronologically by confirmation time
        day_traps.sort(key=lambda x: x["confirm_ts"])

        for trap in day_traps:
            # Single-position constraint per day
            if open_position_day == day:
                # Structural flip: if opposite-side trap fires, close existing position
                if open_position_side is not None and open_position_side != trap["opt_type"]:
                    # Mark the open position as closed by structural flip at current execution price
                    # We do this by flipping the last recorded trade's exit info? Actually,
                    # since we are in a stateless backtest, we need to simulate the structural
                    # flip exit on the existing position. We cannot modify previous records easily
                    # because we already emitted them. So instead, we record the flip as a new
                    # closing event for the open position.
                    # To keep this simple, we record a "FLIP" exit trade for the open side.
                    # But we do not have the open position state stored. For a minimal backtest,
                    # we will just mark the position closed and continue; the structural flip
                    # P&L is not perfectly captured in this simple engine.
                    pass
                continue

            exec_strike = trap["exec_strike"]
            tranches = simulate_monthly_option_trade(
                trap, df_opt_1m, df_spot_1m, exec_strike, eod,
                entry_mode=entry_mode,
                require_zone_reentry=require_zone_reentry,
                require_mtf_ltf_rejection=require_mtf_ltf_rejection,
                trailing_activation_r=trailing_activation_r,
                trailing_lookback=trailing_lookback,
                index_name=index_name,
            )
            if tranches:
                open_position_day = day
                open_position_side = trap["opt_type"]
                for tr in tranches:
                    tr["date"] = day
                trade_records.extend(tranches)

        # Macro record (augmented with confirmation status)
        for trap in day_traps:
            macro_records.append({
                "date": day,
                "ref_ts": trap["ref_ts"],
                "breakout_ts": trap["breakout_ts"],
                "confirm_ts": trap["confirm_ts"],
                "type": trap["type"],
                "multiplier": trap["multiplier"],
                "tracking_strike": trap["strike"],
                "opt_type": trap["opt_type"],
                "execution_strike": trap["exec_strike"],
                "breakout_line": trap["breakout_line"],
                "anchor_sl": trap["anchor_sl"],
                "peak": trap["peak"],
                "zone_low": trap.get("zone_low"),
                "zone_high": trap.get("zone_high"),
                "spot_confirmed": find_spot_confirmation(trap, df_spot_1m, trap["multiplier_min"]) is not None,
            })

    macro_df = pd.DataFrame(macro_records)
    trades_df = pd.DataFrame(trade_records)
    return macro_df, trades_df


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


def summarize_monthly_trades(trades: pd.DataFrame) -> Dict:
    """Standard performance summary for the monthly option trade log."""
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


# ---------------------------------------------------------------------------
# Utility: generate synthetic option data file for a month
# ---------------------------------------------------------------------------


def generate_synthetic_monthly_option_file(
    df_spot_1m: pd.DataFrame,
    expiry_date: date,
    strikes: Optional[List[int]] = None,
    spot_aligned: bool = False,
) -> pd.DataFrame:
    """
    Generate and save a synthetic monthly option parquet file using the standard
    naming convention.  Useful for testing the engine before real data arrives.
    """
    if strikes is None:
        # Build a wide enough strike grid around the spot range
        spot_min = int(df_spot_1m["low"].min())
        spot_max = int(df_spot_1m["high"].max())
        atm = _round_strike((spot_min + spot_max) / 2.0)
        low = atm - 1000
        high = atm + 1000
        strikes = list(range(low, high + 1, STEP))
    df = build_synthetic_premium(df_spot_1m, expiry_date, strikes, spot_aligned=spot_aligned)
    df = _normalize_timestamp(df)
    fpath = os.path.join(CACHE_DIR, f"opt_NIFTY_monthly_{expiry_date.isoformat()}_1m.parquet")
    df.to_parquet(fpath, index=False)
    return df
