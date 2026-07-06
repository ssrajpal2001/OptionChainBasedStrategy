"""
scripts/nifty_spot_option_sweep_backtest.py
============================================
NIFTY backtest sweep:
  - signal / target on NIFTY SPOT chart
  - execution via options (monthly proxy for June, real weekly for July)
  - partial exit: 2 lots; 1 lot at MTF target, 1 lot at HTF target / SL / EOD
  - compares zone-based SL vs fixed spot SL distances

Run: python scripts/nifty_spot_option_sweep_backtest.py
"""
from __future__ import annotations
import glob
import json
import os
import re
import sys
from datetime import date, timedelta, time as dt_time
from typing import Dict, List, Optional, Tuple

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from data_layer.instrument_registry import _calc_next_expiry
from strategies.trap_scanner import scanner

CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")
DAILY_OHLC_FILE = os.path.join(CACHE_DIR, "daily_ohlc_NIFTY.json")
SPOT_FILES = [
    os.path.join(CACHE_DIR, "spot_NIFTY_1m_2026-05-25_2026-06-30.parquet"),
    os.path.join(CACHE_DIR, "spot_NIFTY_1m_2026-06-29_2026-07-14.parquet"),
]

# ── Config ───────────────────────────────────────────────────────────────────
SYMBOL = "NIFTY"
STEP = 50
LOT = 65
TOTAL_LOTS = 2
GAP_PCT = 0.5
HTF_MIN = 75
MTF_MIN = 15
LTF_MIN = 5
ENTRY_TOL_PCT = 0.001
SL_MODES = {"mtf_zone": None, "htf_zone": -1, "fixed30": 30.0, "fixed40": 40.0, "fixed50": 50.0}

JUNE_START, JUNE_END = date(2026, 6, 1), date(2026, 6, 30)
JULY_START, JULY_END = date(2026, 7, 1), date(2026, 7, 3)
MONTHLY_EXPIRY = date(2026, 7, 28)


# ── Data loaders ─────────────────────────────────────────────────────────────
def _load_spot() -> pd.DataFrame:
    frames = []
    for f in SPOT_FILES:
        if not os.path.exists(f):
            continue
        df = pd.read_parquet(f)
        df["datetime"] = pd.to_datetime(df["datetime"])
        if df["datetime"].dt.tz is not None:
            df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    return df.drop_duplicates("datetime").sort_values("datetime").reset_index(drop=True)


def _load_daily_ohlc() -> Dict[str, dict]:
    return json.load(open(DAILY_OHLC_FILE))


def _load_option_cache() -> Dict[Tuple[date, str, int], pd.DataFrame]:
    cache: Dict[Tuple[date, str, int], pd.DataFrame] = {}
    monthly_re = re.compile(r"opt_(NIFTY(CE|PE)(\d+))_\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}\.parquet$")
    weekly_re = re.compile(r"opt_(NIFTY(CE|PE)(\d+))_W(\d{4}-\d{2}-\d{2})_\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}\.parquet$")
    for f in glob.glob(os.path.join(CACHE_DIR, "opt_NIFTY*.parquet")):
        name = os.path.basename(f)
        m = weekly_re.match(name)
        expiry = MONTHLY_EXPIRY
        if m:
            expiry = date.fromisoformat(m.group(4))
        else:
            m = monthly_re.match(name)
            if not m:
                continue
        opt_type = m.group(2)
        strike = int(m.group(3))
        try:
            df = pd.read_parquet(f)
            df["datetime"] = pd.to_datetime(df["datetime"])
            if df["datetime"].dt.tz is not None:
                df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
            cache[(expiry, opt_type, strike)] = df
        except Exception:
            pass
    return cache


def _get_option_df(cache: Dict[Tuple[date, str, int], pd.DataFrame],
                   expiry: date, strike: int, opt_type: str
                   ) -> Tuple[Optional[pd.DataFrame], int]:
    for off in [0, 1, -1, 2, -2, 3, -3]:
        st = strike + off * STEP
        df = cache.get((expiry, opt_type, st))
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


def _resamp(df1m: pd.DataFrame, minutes: int, cut: Optional[str] = "15:14") -> pd.DataFrame:
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


def _ltf_zones(df: pd.DataFrame, mtf_zone: dict, kind: str) -> List[dict]:
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


def _advance_zone_statuses(zones: List[dict], day_spot: pd.DataFrame) -> None:
    for z in zones:
        kind = z.get("kind", "BEAR")
        sl = float(z.get("sl", 0.0))
        entry = float(z.get("entry", 0.0))
        if sl == 0.0 or entry == 0.0:
            continue
        if kind == "BEAR":
            for _, row in day_spot.iterrows():
                st = z.get("status")
                ts = row["datetime"]
                if st == "ACTIVE" and float(row["high"]) > sl:
                    z["status"] = "TRAPPED"
                    z["trapped_on"] = ts
                elif st == "TRAPPED" and float(row["low"]) <= entry and ts != z.get("trapped_on"):
                    z["status"] = "CLOSED"
                    z["closed_on"] = ts
                    break
        else:
            for _, row in day_spot.iterrows():
                st = z.get("status")
                ts = row["datetime"]
                if st == "ACTIVE" and float(row["low"]) < sl:
                    z["status"] = "TRAPPED"
                    z["trapped_on"] = ts
                elif st == "TRAPPED" and float(row["high"]) >= entry and ts != z.get("trapped_on"):
                    z["status"] = "CLOSED"
                    z["closed_on"] = ts
                    break


def _simulate_partial(day_spot: pd.DataFrame, opt_df: pd.DataFrame, entry_idx: int,
                      side: str, sl_spot: float, mtf_target: float, htf_target: float) -> Optional[dict]:
    entry_ts = day_spot.iloc[entry_idx]["datetime"]
    entry_opt = _opt_price(opt_df, entry_ts, "close")
    if entry_opt is None:
        return None

    H = day_spot["high"].values
    L = day_spot["low"].values
    T = day_spot["datetime"].values

    total_qty = LOT * TOTAL_LOTS
    half_qty = LOT
    remaining = total_qty
    pnl = 0.0
    mtf_hit = False
    exit_reason = "EOD"
    exit_idx = len(day_spot) - 1

    for k in range(entry_idx + 1, len(day_spot)):
        opt_k = _opt_price(opt_df, T[k], "close")
        if opt_k is None:
            continue

        if side == "CE":
            if L[k] <= sl_spot and remaining > 0:
                pnl += (opt_k - entry_opt) * remaining
                remaining = 0
                exit_idx = k
                exit_reason = "SL"
                break
            if H[k] >= mtf_target and not mtf_hit and remaining > 0:
                pnl += (opt_k - entry_opt) * half_qty
                remaining -= half_qty
                mtf_hit = True
                if remaining <= 0:
                    exit_idx = k
                    exit_reason = "MTF_TARGET_FULL"
                    break
            if H[k] >= htf_target and remaining > 0:
                pnl += (opt_k - entry_opt) * remaining
                remaining = 0
                exit_idx = k
                exit_reason = "HTF_TARGET"
                break
        else:
            if H[k] >= sl_spot and remaining > 0:
                pnl += (opt_k - entry_opt) * remaining
                remaining = 0
                exit_idx = k
                exit_reason = "SL"
                break
            if L[k] <= mtf_target and not mtf_hit and remaining > 0:
                pnl += (opt_k - entry_opt) * half_qty
                remaining -= half_qty
                mtf_hit = True
                if remaining <= 0:
                    exit_idx = k
                    exit_reason = "MTF_TARGET_FULL"
                    break
            if L[k] <= htf_target and remaining > 0:
                pnl += (opt_k - entry_opt) * remaining
                remaining = 0
                exit_idx = k
                exit_reason = "HTF_TARGET"
                break

    if remaining > 0:
        exit_idx = len(day_spot) - 1
        exit_opt = _opt_price(opt_df, T[exit_idx], "close") or entry_opt
        pnl += (exit_opt - entry_opt) * remaining
        exit_reason = "MTF_PARTIAL+EOD" if mtf_hit else "EOD"

    entry_spot = float(day_spot.iloc[entry_idx]["close"])
    exit_spot = float(day_spot.iloc[exit_idx]["close"])
    return {
        "entry_ts": entry_ts,
        "exit_ts": T[exit_idx],
        "entry_spot": round(entry_spot, 2),
        "exit_spot": round(exit_spot, 2),
        "entry_opt": round(entry_opt, 2),
        "sl_spot": round(sl_spot, 2),
        "mtf_target": round(mtf_target, 2),
        "htf_target": round(htf_target, 2),
        "exit_reason": exit_reason,
        "mtf_hit": mtf_hit,
        "pnl": round(pnl, 2),
        "side": side,
    }


def _find_entry_bar(day_spot: pd.DataFrame, ltf_zone: dict, side: str,
                    after_ts: pd.Timestamp) -> Optional[int]:
    level = float(ltf_zone.get("zone_high" if side == "CE" else "zone_low", 0))
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


def _process_anchor(day_spot: pd.DataFrame, anchor: dict, kind: str,
                    expiry: date, opt_cache: Dict[Tuple[date, str, int], pd.DataFrame],
                    sl_mode: Optional[float]) -> Optional[dict]:
    side = "CE" if kind == "BEAR" else "PE"
    anchor_low = float(anchor.get("zone_low", 0))
    anchor_high = float(anchor.get("zone_high", 0))
    if anchor_low <= 0 or anchor_high <= 0:
        return None

    arm_idx = None
    for idx, row in day_spot.iterrows():
        if anchor_low <= float(row["close"]) <= anchor_high:
            arm_idx = int(idx)
            break
    if arm_idx is None:
        return None
    arm_ts = day_spot.iloc[arm_idx]["datetime"]
    anchor_ref_ts = pd.Timestamp(anchor.get("ref_ts")) if anchor.get("ref_ts") else arm_ts

    mtf_df = _resamp(day_spot, MTF_MIN)
    mtf_zones = _zones_spot(mtf_df)
    valid_mtf = [z for z in mtf_zones if z.get("kind") == kind
                 and (not z.get("ref_ts") or pd.Timestamp(z["ref_ts"]) >= anchor_ref_ts)
                 and _overlap(anchor, z)]
    if not valid_mtf:
        return None

    chosen_mtf = None
    mtf_enter_ts = None
    for z in valid_mtf:
        z_low, z_high = float(z.get("zone_low", 0)), float(z.get("zone_high", 0))
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

    ltf_df = _resamp(day_spot, LTF_MIN)
    ltf_zones = _ltf_zones(ltf_df, chosen_mtf, kind)
    closed_ltf = [z for z in ltf_zones if z.get("status") == "CLOSED"]
    if not closed_ltf:
        return None
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

    htf_target = float(anchor.get("sl", 0))
    mtf_target = float(chosen_mtf.get("sl", 0))
    mtf_low = float(chosen_mtf.get("zone_low", 0))
    mtf_high = float(chosen_mtf.get("zone_high", 0))

    if side == "CE":
        if sl_mode is None:                       # mtf_zone mode
            sl_spot = mtf_low - 10.0
        elif sl_mode == -1:                       # htf_zone mode
            sl_spot = anchor_low - 10.0
        else:                                     # fixed mode
            sl_spot = entry_spot - sl_mode
    else:
        if sl_mode is None:
            sl_spot = mtf_high + 10.0
        elif sl_mode == -1:
            sl_spot = anchor_high + 10.0
        else:
            sl_spot = entry_spot + sl_mode

    if sl_spot <= 0 or htf_target <= 0 or mtf_target <= 0:
        return None
    if side == "CE" and (htf_target <= entry_spot or mtf_target <= entry_spot or sl_spot >= entry_spot):
        return None
    if side == "PE" and (htf_target >= entry_spot or mtf_target >= entry_spot or sl_spot <= entry_spot):
        return None

    atm = int(round(entry_spot / STEP) * STEP)
    if side == "CE":
        strike = atm if atm <= entry_spot else atm - STEP
    else:
        strike = atm if atm >= entry_spot else atm + STEP
    opt_df, used_strike = _get_option_df(opt_cache, expiry, strike, side)
    if opt_df is None:
        return None

    trade = _simulate_partial(day_spot, opt_df, entry_idx, side, sl_spot, mtf_target, htf_target)
    if trade is None:
        return None
    trade.update({
        "date": str(day_spot.iloc[0]["datetime"].date()),
        "used_strike": used_strike,
        "anchor_kind": kind,
        "sl_mode": "zone" if sl_mode is None else f"fixed{int(sl_mode)}",
    })
    return trade


def _run_day(day: date, expiry_mode: str, sl_mode: Optional[float],
             spot_1m: pd.DataFrame, opt_cache: Dict[Tuple[date, str, int], pd.DataFrame],
             ohlc: Dict[str, dict]) -> List[dict]:
    trades: List[dict] = []
    day_start = pd.Timestamp(f"{day} 09:15:00")
    day_end = pd.Timestamp(f"{day} 15:30:00")
    day_spot = spot_1m[(spot_1m["datetime"] >= day_start) & (spot_1m["datetime"] <= day_end)].copy().reset_index(drop=True)
    if len(day_spot) < 30:
        return trades

    if expiry_mode == "monthly":
        expiry = MONTHLY_EXPIRY
    elif expiry_mode == "current_week":
        expiry = _calc_next_expiry(SYMBOL, day)
    elif expiry_mode == "next_week":
        expiry = _calc_next_expiry(SYMBOL, day) + timedelta(days=7)
    else:
        return trades

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
    _advance_zone_statuses(htf_zones, day_spot)

    anchors: List[Tuple[dict, bool]] = []
    if not is_gap:
        for z in htf_zones:
            if z.get("status") in ("TRAPPED", "CLOSED"):
                anchors.append((z, False))
    else:
        mtf_df = _resamp(day_spot, MTF_MIN)
        mtf_zones = _zones_spot(mtf_df)
        _advance_zone_statuses(mtf_zones, day_spot)
        for z in mtf_zones:
            if z.get("status") in ("TRAPPED", "CLOSED"):
                anchors.append((z, True))

    anchors_sorted = sorted(anchors, key=lambda ag: str(ag[0].get("ref_ts", "")))
    used_uids: set = set()
    used_entries: set = set()
    last_exit_idx = -1

    for anchor, gap_flag in anchors_sorted:
        uid = _zone_uid(anchor)
        if uid in used_uids:
            continue
        kind = anchor.get("kind", "BEAR")
        trade = _process_anchor(day_spot, anchor, kind, expiry, opt_cache, sl_mode)
        if not trade:
            continue
        entry_idx = int(day_spot[day_spot["datetime"] == trade["entry_ts"]].index[0])
        exit_idx = int(day_spot[day_spot["datetime"] == trade["exit_ts"]].index[0])
        entry_key = (trade["date"], trade["side"], trade["entry_ts"])
        if entry_key in used_entries or entry_idx <= last_exit_idx:
            continue
        trades.append(trade)
        used_entries.add(entry_key)
        used_uids.add(uid)
        last_exit_idx = exit_idx
    return trades


def _run_period(start: date, end: date, expiry_mode: str, sl_mode: Optional[float],
                spot_1m: pd.DataFrame, opt_cache: Dict[Tuple[date, str, int], pd.DataFrame],
                ohlc: Dict[str, dict]) -> List[dict]:
    trades: List[dict] = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            trades.extend(_run_day(d, expiry_mode, sl_mode, spot_1m, opt_cache, ohlc))
        d += timedelta(days=1)
    return trades


def _stats(tlist: List[dict]) -> dict:
    if not tlist:
        return dict(n=0, w=0, wr=0.0, pf=0.0, net=0.0)
    wins = [t for t in tlist if t["pnl"] > 0]
    losses = [t for t in tlist if t["pnl"] <= 0]
    gw = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    return dict(
        n=len(tlist), w=len(wins), wr=round(len(wins) / len(tlist) * 100, 1),
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
    print(f"  {len(opt_cache)} option files loaded")
    print("Loading daily OHLC ...")
    ohlc = _load_daily_ohlc()

    print("\n" + "=" * 95)
    print("NIFTY spot-signal / option-exec sweep")
    print("Partial exit: 2 lots; 1 lot at MTF target, 1 lot at HTF target / SL / EOD")
    print("=" * 95)

    for sl_label, sl_val in SL_MODES.items():
        print(f"\n{'='*95}")
        desc = {None: 'MTF zone SL', -1: 'HTF zone SL'}
        print(f"SL MODE = {sl_label}  ({desc.get(sl_val, f'fixed {sl_val} spot points')})")
        print(f"{'='*95}")

        june = _run_period(JUNE_START, JUNE_END, "monthly", sl_val, spot_1m, opt_cache, ohlc)
        july_cur = _run_period(JULY_START, JULY_END, "current_week", sl_val, spot_1m, opt_cache, ohlc)
        july_nxt = _run_period(JULY_START, JULY_END, "next_week", sl_val, spot_1m, opt_cache, ohlc)

        for label, trades in [
            ("June (monthly proxy)", june),
            ("July current-week", july_cur),
            ("July next-week", july_nxt),
            ("July combined", july_cur + july_nxt),
        ]:
            s = _stats(trades)
            print(f"  {label:22}: Trades={s['n']:3}  Wins={s['w']:3}  WR={s['wr']:5.1f}%  PF={s['pf']:5.2f}  Net=Rs {s['net']:8.0f}")
