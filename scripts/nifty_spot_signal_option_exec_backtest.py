"""
scripts/nifty_spot_signal_option_exec_backtest.py
==================================================
NIFTY backtest: signal / SL / target on NIFTY SPOT chart, execution via July-monthly option.

- Period: 1 Jun 2026 → 30 Jun 2026 (all trading days).
- Spot data: NIFTY 1m spot bars (cached from Upstox).
- Option data: July 28 monthly option 1m bars (already cached).
- Entry: HTF=75m spot trap → MTF=15m spot zone → LTF=5m spot zone retest.
- SL & target are on the spot chart; P&L is computed from the option premium.
- Lot = 65 (current NIFTY lot size).

Run: python scripts/nifty_spot_signal_option_exec_backtest.py
"""
from __future__ import annotations
import glob
import json
import os
import sys
from datetime import date, timedelta, time as dt_time
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from strategies.trap_scanner import scanner

CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")
DAILY_OHLC_FILE = os.path.join(CACHE_DIR, "daily_ohlc_NIFTY.json")
SPOT_FILE = os.path.join(CACHE_DIR, "spot_NIFTY_1m_2026-05-25_2026-06-30.parquet")

# ── Config ───────────────────────────────────────────────────────────────────
SYMBOL = "NIFTY"
STEP = 50
LOT = 65
SL_BUF = 10.0          # spot points below/above the zone
GAP_PCT = 0.5
HTF_MIN = 75
MTF_MIN = 15
LTF_MIN = 5
ENTRY_TOL_PCT = 0.001  # 0.1% tolerance for spot retest of LTF zone level
START_DATE = date(2026, 6, 1)
END_DATE = date(2026, 6, 30)


# ── Data loaders ─────────────────────────────────────────────────────────────
def _load_spot() -> pd.DataFrame:
    df = pd.read_parquet(SPOT_FILE)
    df["datetime"] = pd.to_datetime(df["datetime"])
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    return df.sort_values("datetime").reset_index(drop=True)


def _load_daily_ohlc() -> Dict[str, dict]:
    return json.load(open(DAILY_OHLC_FILE))


def _load_option_cache() -> Dict[str, pd.DataFrame]:
    cache: Dict[str, pd.DataFrame] = {}
    for f in glob.glob(os.path.join(CACHE_DIR, "opt_NIFTY*.parquet")):
        if "_W" in os.path.basename(f):
            continue
        try:
            df = pd.read_parquet(f)
            df["datetime"] = pd.to_datetime(df["datetime"])
            if df["datetime"].dt.tz is not None:
                df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
            label = os.path.basename(f).replace("opt_", "").split("_2026")[0]
            cache[label] = df
        except Exception:
            pass
    return cache


def _opt_label(strike: int, opt_type: str) -> str:
    return f"NIFTY{opt_type}{strike}"


def _get_option_df(cache: Dict[str, pd.DataFrame], strike: int, opt_type: str
                   ) -> Tuple[Optional[pd.DataFrame], int]:
    """Return option df and the strike actually used (falls back to nearby strikes)."""
    for off in [0, 1, -1, 2, -2, 3, -3]:
        st = strike + off * STEP
        label = _opt_label(st, opt_type)
        df = cache.get(label)
        if df is not None and len(df) >= 30:
            return df, st
    return None, strike


def _opt_price(df: pd.DataFrame, ts: pd.Timestamp, field: str = "close") -> Optional[float]:
    row = df[df["datetime"] == ts]
    if not row.empty:
        return float(row.iloc[0][field])
    later = df[df["datetime"] >= ts]
    if not later.empty:
        return float(later.iloc[0][field])
    return None


# ── Resampling / zone helpers ────────────────────────────────────────────────
def _resamp(df1m: pd.DataFrame, minutes: int, cut: Optional[str] = None) -> pd.DataFrame:
    if df1m.empty or len(df1m) < 2:
        return pd.DataFrame()
    df = df1m.copy()
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_localize(None)
    if cut:
        h, m = cut.split(":")
        df = df[df["datetime"].dt.time <= dt_time(int(h), int(m))]
    return (
        df.set_index("datetime")[["open", "high", "low", "close"]]
        .resample(f"{minutes}min", closed="left", label="left")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
        .dropna(subset=["close"])
        .reset_index()
    )


def _zones_spot(df: pd.DataFrame) -> List[dict]:
    if len(df) < 3:
        return []
    try:
        _, entries = scanner.scan_htf_spot(df)
        return entries or []
    except Exception:
        return []


def _zones_bear(df: pd.DataFrame) -> List[dict]:
    """Bear-only zones (for compatibility with scan_ltf)."""
    if len(df) < 3:
        return []
    try:
        _, entries = scanner.scan_htf(df)
        return entries or []
    except Exception:
        return []


def _ltf_zones(df: pd.DataFrame, mtf_zone: dict, kind: str) -> List[dict]:
    """5-min spot LTF zones inside the given MTF zone."""
    if df.empty or len(df) < 2:
        return []
    z_high = float(mtf_zone.get("zone_high", 0))
    z_low = float(mtf_zone.get("zone_low", 0))
    if z_high <= 0 or z_low <= 0 or z_high < z_low:
        return []
    try:
        if kind == "BEAR":
            _, entries = scanner.scan_ltf(df, htf_zone_high=z_high, htf_zone_low=z_low)
        else:
            _, entries = scanner.scan_ltf_bull(df, htf_zone_high=z_high, htf_zone_low=z_low)
        return entries or []
    except Exception:
        return []


def _overlap(a: dict, b: dict, buf_pct: float = 0.15) -> bool:
    al, ah = float(a.get("zone_low", 0)), float(a.get("zone_high", 0))
    bl, bh = float(b.get("zone_low", 0)), float(b.get("zone_high", 0))
    buf = max((ah - al) * buf_pct, 0.5)
    return ah + buf >= bl and bh + buf >= al


def _zone_uid(z: dict) -> str:
    return f"{z.get('zone_low', 0):.1f}_{z.get('zone_high', 0):.1f}_{z.get('kind', '?')}"


# ── Simulation ───────────────────────────────────────────────────────────────
def _simulate(day_spot: pd.DataFrame, opt_df: pd.DataFrame,
              entry_idx: int, side: str, sl_spot: float, target_spot: float) -> Optional[dict]:
    entry_ts = day_spot.iloc[entry_idx]["datetime"]
    entry_opt = _opt_price(opt_df, entry_ts, "close")
    if entry_opt is None:
        return None

    H = day_spot["high"].values
    L = day_spot["low"].values
    T = day_spot["datetime"].values

    exit_idx = None
    exit_reason = None
    for k in range(entry_idx + 1, len(day_spot)):
        if side == "CE":
            if L[k] <= sl_spot:
                exit_idx = k
                exit_reason = "SL"
                break
            if H[k] >= target_spot:
                exit_idx = k
                exit_reason = "TARGET"
                break
        else:  # PE
            if H[k] >= sl_spot:
                exit_idx = k
                exit_reason = "SL"
                break
            if L[k] <= target_spot:
                exit_idx = k
                exit_reason = "TARGET"
                break

    if exit_idx is None:
        exit_idx = len(day_spot) - 1
        exit_reason = "EOD"

    exit_ts = T[exit_idx]
    exit_opt = _opt_price(opt_df, exit_ts, "close")
    if exit_opt is None:
        return None

    pnl = (exit_opt - entry_opt) * LOT
    entry_spot = float(day_spot.iloc[entry_idx]["close"])
    exit_spot = float(day_spot.iloc[exit_idx]["close"])
    return {
        "entry_ts": entry_ts,
        "exit_ts": exit_ts,
        "entry_spot": round(entry_spot, 2),
        "exit_spot": round(exit_spot, 2),
        "entry_opt": round(entry_opt, 2),
        "exit_opt": round(exit_opt, 2),
        "sl_spot": round(sl_spot, 2),
        "target_spot": round(target_spot, 2),
        "exit_reason": exit_reason,
        "pnl": round(pnl, 2),
        "side": side,
    }


def _find_entry_bar(day_spot: pd.DataFrame, ltf_zone: dict, side: str,
                    after_ts: pd.Timestamp) -> Optional[int]:
    """First 1m bar after after_ts where spot retests the LTF entry level."""
    if side == "CE":
        level = float(ltf_zone.get("zone_high", 0))
    else:
        level = float(ltf_zone.get("zone_low", 0))
    if level <= 0:
        return None
    tol = max(level * ENTRY_TOL_PCT, 1.0)
    for idx, row in day_spot.iterrows():
        if row["datetime"] <= after_ts:
            continue
        close = float(row["close"])
        if side == "CE" and close <= level + tol:
            return int(idx)
        if side == "PE" and close >= level - tol:
            return int(idx)
    return None


def _process_anchor(day_spot: pd.DataFrame, spot_1m_full: pd.DataFrame,
                    anchor: dict, kind: str, is_gap: bool,
                    opt_cache: Dict[str, pd.DataFrame]) -> Optional[dict]:
    """Process one HTF (or MTF on gap days) zone and return a trade if one fires."""
    side = "CE" if kind == "BEAR" else "PE"

    # SL / target on the spot chart
    if side == "CE":
        sl_spot = float(anchor.get("zone_low", 0)) - SL_BUF
        target_spot = float(anchor.get("sl", 0))
    else:
        sl_spot = float(anchor.get("zone_high", 0)) + SL_BUF
        target_spot = float(anchor.get("sl", 0))

    if sl_spot <= 0 or target_spot <= 0:
        return None

    # Stage 1: spot must enter the anchor zone
    anchor_low = float(anchor.get("zone_low", 0))
    anchor_high = float(anchor.get("zone_high", 0))
    arm_idx = None
    for idx, row in day_spot.iterrows():
        if anchor_low <= float(row["close"]) <= anchor_high:
            arm_idx = int(idx)
            break
    if arm_idx is None:
        return None
    arm_ts = day_spot.iloc[arm_idx]["datetime"]
    anchor_ref_ts = pd.Timestamp(anchor.get("ref_ts")) if anchor.get("ref_ts") else arm_ts

    # Stage 2: find a matching MTF zone formed at/after anchor ref_ts, spot inside it
    mtf_df = _resamp(day_spot, MTF_MIN)
    mtf_zones = _zones_spot(mtf_df)
    valid_mtf = []
    for z in mtf_zones:
        if z.get("kind") != kind:
            continue
        z_ref = pd.Timestamp(z.get("ref_ts")) if z.get("ref_ts") else None
        if z_ref is not None and z_ref < anchor_ref_ts:
            continue
        if not _overlap(anchor, z):
            continue
        valid_mtf.append(z)
    if not valid_mtf:
        return None

    # Pick the first MTF zone whose bounds spot entered after arm_ts
    chosen_mtf = None
    mtf_enter_ts = None
    for z in valid_mtf:
        z_low = float(z.get("zone_low", 0))
        z_high = float(z.get("zone_high", 0))
        for idx, row in day_spot.iterrows():
            if row["datetime"] <= arm_ts:
                continue
            if z_low <= float(row["close"]) <= z_high:
                chosen_mtf = z
                mtf_enter_ts = row["datetime"]
                break
        if chosen_mtf is not None:
            break
    if chosen_mtf is None:
        return None

    # Stage 3: LTF zone inside MTF zone, then spot retest
    ltf_df = _resamp(day_spot, LTF_MIN)
    ltf_zones = _ltf_zones(ltf_df, chosen_mtf, kind)
    # Use CLOSED LTF zones only
    closed_ltf = [z for z in ltf_zones if z.get("status") == "CLOSED"]
    if not closed_ltf:
        return None
    # Pick the most recently closed LTF zone that closed after we entered the MTF zone
    closed_ltf.sort(key=lambda z: str(z.get("closed_on", "")))
    chosen_ltf = None
    for z in closed_ltf:
        closed_ts = pd.Timestamp(z.get("closed_on")) if z.get("closed_on") else None
        if closed_ts is not None and closed_ts <= mtf_enter_ts:
            continue
        chosen_ltf = z
        break
    if chosen_ltf is None:
        chosen_ltf = closed_ltf[-1]

    entry_idx = _find_entry_bar(day_spot, chosen_ltf, side, mtf_enter_ts)
    if entry_idx is None:
        return None

    entry_spot = float(day_spot.iloc[entry_idx]["close"])
    # Sanity: target must be on the profitable side of entry
    if side == "CE" and (target_spot <= entry_spot or sl_spot >= entry_spot):
        return None
    if side == "PE" and (target_spot >= entry_spot or sl_spot <= entry_spot):
        return None

    # Select option strike from entry spot
    atm = int(round(entry_spot / STEP) * STEP)
    if side == "CE":
        strike = atm if atm <= entry_spot else atm - STEP
    else:
        strike = atm if atm >= entry_spot else atm + STEP
    opt_df, used_strike = _get_option_df(opt_cache, strike, side)
    if opt_df is None:
        return None

    trade = _simulate(day_spot, opt_df, entry_idx, side, sl_spot, target_spot)
    if trade is None:
        return None
    trade.update({
        "date": str(day_spot.iloc[0]["datetime"].date()),
        "side": side,
        "used_strike": used_strike,
        "anchor": "gap_mtf" if is_gap else "htf",
        "htf_zone_low": round(anchor_low, 2),
        "htf_zone_high": round(anchor_high, 2),
    })
    return trade


def _run_day(day: date, spot_1m_full: pd.DataFrame, opt_cache: Dict[str, pd.DataFrame],
             ohlc: Dict[str, dict]) -> List[dict]:
    trades: List[dict] = []
    day_start = pd.Timestamp(f"{day} 09:15:00")
    day_end = pd.Timestamp(f"{day} 15:30:00")
    day_spot = spot_1m_full[(spot_1m_full["datetime"] >= day_start) &
                            (spot_1m_full["datetime"] <= day_end)].copy().reset_index(drop=True)
    if len(day_spot) < 30:
        return trades

    # Gap detection
    prev_c = None
    d = day - timedelta(days=1)
    for _ in range(5):
        data = ohlc.get(str(d))
        if data:
            prev_c = float(data.get("close", 0))
            break
        d -= timedelta(days=1)
    today_data = ohlc.get(str(day), {})
    today_open = float(today_data.get("open", 0)) if today_data else 0.0
    is_gap = False
    if prev_c and today_open:
        is_gap = abs(today_open - prev_c) / prev_c * 100 >= GAP_PCT

    htf_df = _resamp(day_spot, HTF_MIN)
    htf_zones = _zones_spot(htf_df)

    anchors: List[Tuple[dict, bool]] = []
    if not is_gap:
        for z in htf_zones:
            if z.get("status") in ("TRAPPED", "CLOSED"):
                anchors.append((z, False))
    else:
        # Gap mode: MTF zones become anchors
        mtf_df = _resamp(day_spot, MTF_MIN)
        for z in _zones_spot(mtf_df):
            if z.get("status") in ("TRAPPED", "CLOSED"):
                anchors.append((z, True))

    used_uids: set = set()
    used_entries: set = set()
    last_exit_idx = -1
    # Sort anchors by ref_ts so processing follows market chronology
    anchors_sorted = sorted(
        [(a, g) for a, g in anchors],
        key=lambda ag: str(ag[0].get("ref_ts", ""))
    )
    for anchor, gap_flag in anchors_sorted:
        uid = _zone_uid(anchor)
        if uid in used_uids:
            continue
        kind = anchor.get("kind", "BEAR")
        trade = _process_anchor(day_spot, spot_1m_full, anchor, kind, gap_flag, opt_cache)
        if not trade:
            continue
        entry_idx = day_spot[day_spot["datetime"] == trade["entry_ts"]].index
        if len(entry_idx) == 0:
            continue
        entry_idx = int(entry_idx[0])
        exit_idx = day_spot[day_spot["datetime"] == trade["exit_ts"]].index[0]
        entry_key = (trade["date"], trade["side"], trade["entry_ts"])
        if entry_key in used_entries:
            continue
        # No overlapping positions: engine holds only one trade at a time
        if entry_idx <= last_exit_idx:
            continue
        trades.append(trade)
        used_entries.add(entry_key)
        used_uids.add(uid)
        last_exit_idx = int(exit_idx)
    return trades


def _stats(tlist: List[dict]) -> dict:
    if not tlist:
        return dict(n=0, w=0, wr=0.0, pf=0.0, net=0.0)
    wins = [t for t in tlist if t["pnl"] > 0]
    losses = [t for t in tlist if t["pnl"] <= 0]
    gw = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    return dict(
        n=len(tlist),
        w=len(wins),
        wr=round(len(wins) / len(tlist) * 100, 1),
        pf=round(gw / gl, 2) if gl > 0 else 9999.0,
        net=round(sum(t["pnl"] for t in tlist), 2),
    )


# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("Loading spot 1m ...")
    spot_1m = _load_spot()
    print(f"  {len(spot_1m)} spot bars  {spot_1m['datetime'].iloc[0]} -> {spot_1m['datetime'].iloc[-1]}")

    print("Loading option cache ...")
    opt_cache = _load_option_cache()
    print(f"  {len(opt_cache)} monthly option files")

    print("Loading daily OHLC ...")
    ohlc = _load_daily_ohlc()

    days = [START_DATE + timedelta(days=i) for i in range((END_DATE - START_DATE).days + 1)
            if (START_DATE + timedelta(days=i)).weekday() < 5]

    all_trades: List[dict] = []
    for day in days:
        trades = _run_day(day, spot_1m, opt_cache, ohlc)
        all_trades.extend(trades)

    print("\n" + "=" * 90)
    print(f"NIFTY spot-signal / option-exec backtest  |  {START_DATE} to {END_DATE}")
    print(f"HTF={HTF_MIN}m  MTF={MTF_MIN}m  LTF={LTF_MIN}m  SLbuf={SL_BUF}pts  Lot={LOT}")
    print("=" * 90)

    print(f"\n{'#':4} {'Date':12} {'Side':4} {'Strike':7} {'EntrySpot':10} {'ExitSpot':10} "
          f"{'EntryOpt':9} {'ExitOpt':9} {'SL':8} {'Target':8} {'Exit':8} {'PnL':10}")
    print("-" * 110)
    for i, t in enumerate(all_trades, 1):
        print(f"{i:4} {t['date']:12} {t['side']:4} {t['used_strike']:7} "
              f"{t['entry_spot']:10.1f} {t['exit_spot']:10.1f} "
              f"{t['entry_opt']:9.1f} {t['exit_opt']:9.1f} "
              f"{t['sl_spot']:8.1f} {t['target_spot']:8.1f} {t['exit_reason']:8} {t['pnl']:+10.0f}")

    s = _stats(all_trades)
    print("\n" + "=" * 90)
    print(f"TOTAL: Trades={s['n']}  Wins={s['w']}  WR={s['wr']:.1f}%  PF={s['pf']:.2f}  Net=Rs {s['net']:.0f}")
    print("=" * 90)

    ce = _stats([t for t in all_trades if t["side"] == "CE"])
    pe = _stats([t for t in all_trades if t["side"] == "PE"])
    print(f"\nCE: Trades={ce['n']}  Wins={ce['w']}  WR={ce['wr']:.1f}%  PF={ce['pf']:.2f}  Net=Rs {ce['net']:.0f}")
    print(f"PE: Trades={pe['n']}  Wins={pe['w']}  WR={pe['wr']:.1f}%  PF={pe['pf']:.2f}  Net=Rs {pe['net']:.0f}")
