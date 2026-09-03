"""
scripts/d1trap_fractal_backtest.py — backtest of the NEW D1-trap fractal
mechanic designed in the "liquidity sweep / trap flip-flop" discussion
(2026-07-30). This is NOT the currently-live d1_trap_index mechanic in
strategies/d1_trap_option/book.py — it is a from-scratch simulator of the
new design:

  D1 zone (sweep+reclaim, reused from strategies.v4_cascade.rolling_base)
    -> tick(1m)-wise zone contact
    -> 75-min ref candle (must CLOSE), then tick(1m)-wise breach of its high/low
    -> decompose that 75-min span into 5-min bars, find bear/bull-trap
       sub-zone(s), collapse to one zone_high=max(highs)/zone_low=min(lows)
    -> arm on a retracement scaled by zone size vs a %-of-spot threshold
       (large zone -> shallow 1/3 retrace on the near side; small zone ->
       deep 1/3 retrace on the far side)
    -> entry on a break of the collapsed zone's own high/low (the swing),
       SL = 75-min ref candle's low/high

  Invalidation (flip trigger) = a 75-min candle CLOSING beyond the D1 zone.
  On invalidation, TWO pool candidates are queued (whichever fires first
  wins, the other retires; the un-flipped zone retires too once either
  fires -- one flip per zone, no re-flip back):
    - continuation: tick-wise break of the running low/high since invalidation,
      SL = the invalidating 75-min bar's opposite extreme
    - retest: same fractal pipeline (75-min ref -> breach -> 5-min sub-zone
      -> arm -> swing break) re-anchored to price re-touching the ORIGINAL
      D1 zone level, whenever that happens (bounded only by zone max-age)

Approximations (stated explicitly, not hidden):
  - "tick-wise" is simulated at 1-minute-bar resolution -- the finest
    granularity Upstox historical data provides. A breach/arm/entry level
    is considered hit the moment a 1-min bar's high/low reaches it.
  - No historical option-chain data exists before this month, so P&L is
    computed in SPOT POINTS (validated against the live book's own exit
    logic, which is already spot-close-based, not premium-based). This
    measures setup quality, not real option-account P&L (no premium decay/
    IV/spread modeled).
  - Exit checks use 5-min close (matches live d1_trap_option._check_exit);
    TSL ratchets on 75-min close (matches mtf_tf=75min cadence); EOD force
    exit 15:15 IST (MIS, matches d1_trap_index).
  - Zone becomes "known" the day after its D1 lock/reclaim (no lookahead),
    expires after MAX_ZONE_AGE_DAYS unmatched.
  - Global "one position at a time" is approximated as a post-hoc
    chronological filter over all zones' candidate entries (see run_backtest).

Usage:
  python scripts/d1trap_fractal_backtest.py --tag 2025-07-30_2026-07-29 --sweep
  python scripts/d1trap_fractal_backtest.py --tag 2025-07-30_2026-07-29 --single
"""
from __future__ import annotations

import argparse
import itertools
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pytz

sys.path.insert(0, ".")
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones

IST = pytz.timezone("Asia/Kolkata")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(ROOT, "data", "d1trap_fractal_cache")

SESSION_OPEN = time(9, 15)
EOD_TIME = time(15, 15)
MAX_ZONE_AGE_DAYS = 20
LOT_SIZE = 65


# ── data loading / resampling ──────────────────────────────────────────────

def load_data(tag: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    d1 = pd.read_parquet(os.path.join(CACHE_DIR, f"nifty_d1_{tag}.parquet"))
    m1 = pd.read_parquet(os.path.join(CACHE_DIR, f"nifty_1m_{tag}.parquet"))
    d1["datetime"] = pd.to_datetime(d1["datetime"])
    m1["datetime"] = pd.to_datetime(m1["datetime"])
    return d1.sort_values("datetime").reset_index(drop=True), m1.sort_values("datetime").reset_index(drop=True)


def resample(df_1m: pd.DataFrame, minutes: int) -> pd.DataFrame:
    frames = []
    for day, g in df_1m.groupby(df_1m["datetime"].dt.date):
        g = g.set_index("datetime").sort_index()
        origin = pd.Timestamp(f"{day} 09:15:00", tz=IST)
        r = g.resample(f"{minutes}min", origin=origin).agg(
            {"open": "first", "high": "max", "low": "min", "close": "last"}
        ).dropna().reset_index()
        r = r.rename(columns={"datetime": "timestamp"})
        frames.append(r)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def to_bars(df: pd.DataFrame):
    cols = df.rename(columns={"datetime": "timestamp"}) if "timestamp" not in df.columns else df
    return list(cols[["timestamp", "open", "high", "low", "close"]].itertuples(index=False, name="Bar"))


# ── D1 zone detection (reuse rolling_base sweep+reclaim) ──────────────────

def detect_d1_zones(d1_bars) -> List[dict]:
    zones = []
    for z in find_all_bear_zones(d1_bars):      # LONG (sellers/bears trapped)
        lo, hi = min(z.entry_line, z.sweep_low), max(z.entry_line, z.sweep_low)
        zones.append(dict(direction="LONG", zone_lo=lo, zone_hi=hi,
                           entry_line=z.entry_line, lock_ts=z.lock_ts))
    for z in find_all_bull_zones(d1_bars):       # SHORT (buyers trapped)
        lo, hi = min(z.entry_line, z.sweep_low), max(z.entry_line, z.sweep_low)
        zones.append(dict(direction="SHORT", zone_lo=lo, zone_hi=hi,
                           entry_line=z.entry_line, lock_ts=z.lock_ts))
    zones.sort(key=lambda z: z["lock_ts"])
    return zones


# ── config ──────────────────────────────────────────────────────────────────

@dataclass
class Config:
    zone_size_threshold_pct: float = 0.15   # % of spot, splits large/small sub-zone
    enable_continuation: bool = True
    enable_retest: bool = True
    fallback_on_no_subzone: str = "skip"    # "skip" | "raw_breakout"
    violent_move_filter_pct: Optional[float] = None  # suppress entries off moves > this % of spot
    htf_minutes: int = 0     # 0 = D1 (day bars); else resampled intraday minutes
    ref_minutes: int = 75    # ref-candle / MTF timeframe
    sub_minutes: int = 5     # sub-zone decomposition timeframe
    entry_mode: str = "swing_breach"   # "mtf_breach" | "subzone_reached" | "swing_breach"


# ── fractal pipeline (shared by original-zone path and retest path) ───────

def collapse_subzones(bars_5m_window, direction) -> Optional[Tuple[float, float]]:
    if len(bars_5m_window) < 3:
        return None
    found = find_all_bear_zones(bars_5m_window) if direction == "LONG" else find_all_bull_zones(bars_5m_window)
    if not found:
        return None
    los, his = [], []
    for z in found:
        lo, hi = min(z.entry_line, z.sweep_low), max(z.entry_line, z.sweep_low)
        los.append(lo)
        his.append(hi)
    return min(los), max(his)


def arm_level(zone_lo: float, zone_hi: float, direction: str, threshold_pts: float) -> float:
    size = zone_hi - zone_lo
    large = size > threshold_pts
    if direction == "LONG":     # approach from above
        return zone_hi - size / 3.0 if large else zone_lo + size / 3.0
    else:                       # SHORT, approach from below
        return zone_lo + size / 3.0 if large else zone_hi - size / 3.0


def find_ref_bar(anchor_ts, m_ref: pd.DataFrame, ref_minutes: int):
    for row in m_ref.itertuples(index=False):
        bar_open = row.timestamp
        bar_close = bar_open + timedelta(minutes=ref_minutes)
        if bar_open <= anchor_ts < bar_close or bar_open >= anchor_ts:
            return row
    return None


def run_fractal_from(anchor_ts, direction: str, m1: pd.DataFrame, m_ref: pd.DataFrame,
                      m_sub: pd.DataFrame, cfg: Config, deadline_ts) -> Optional[dict]:
    ref = find_ref_bar(anchor_ts, m_ref, cfg.ref_minutes)
    if ref is None:
        return None
    ref_open, ref_close_time = ref.timestamp, ref.timestamp + timedelta(minutes=cfg.ref_minutes)
    ref_high, ref_low = ref.high, ref.low
    if ref_close_time > deadline_ts:
        return None

    m1_after = m1[(m1["datetime"] >= ref_close_time) & (m1["datetime"] <= deadline_ts)]
    if m1_after.empty:
        return None

    if direction == "LONG":
        breach = m1_after[m1_after["high"] >= ref_high]
    else:
        breach = m1_after[m1_after["low"] <= ref_low]
    if breach.empty:
        return None
    breach_ts = breach.iloc[0]["datetime"]

    # Mode 1: enter right at the MTF/ref-candle breach -- no sub-zone required at all.
    if cfg.entry_mode == "mtf_breach":
        entry_price = ref_high if direction == "LONG" else ref_low
        sl = ref_low if direction == "LONG" else ref_high
        return dict(entry_ts=breach_ts, entry_price=entry_price, sl=sl,
                    ref_open=ref_open, subzone=None, move_ref_close=ref.close)

    window_sub = m_sub[(m_sub["timestamp"] >= ref_open) & (m_sub["timestamp"] < ref_close_time)]
    collapse = collapse_subzones(to_bars(window_sub), direction)

    if collapse is None:
        if cfg.fallback_on_no_subzone != "raw_breakout":
            return None
        entry_price = ref_high if direction == "LONG" else ref_low
        sl = ref_low if direction == "LONG" else ref_high
        return dict(entry_ts=breach_ts, entry_price=entry_price, sl=sl,
                    ref_open=ref_open, subzone=None, move_ref_close=ref.close)

    zone_lo, zone_hi = collapse

    # Mode 2: enter the instant the sub-zone is confirmed -- no arm/retracement wait,
    # no requirement to break the sub-zone's own high/low again.
    if cfg.entry_mode == "subzone_reached":
        entry_price = zone_hi if direction == "LONG" else zone_lo
        sl = ref_low if direction == "LONG" else ref_high
        return dict(entry_ts=breach_ts, entry_price=entry_price, sl=sl,
                    ref_open=ref_open, subzone=(zone_lo, zone_hi), move_ref_close=ref.close)

    # Mode 3 (default): full pipeline -- arm on retracement, then wait for a break
    # of the sub-zone's own high/low (the swing).
    threshold_pts = cfg.zone_size_threshold_pct / 100.0 * ref.close
    lvl = arm_level(zone_lo, zone_hi, direction, threshold_pts)

    m1_arm = m1[(m1["datetime"] > breach_ts) & (m1["datetime"] <= deadline_ts)]
    if direction == "LONG":
        armed = m1_arm[(m1_arm["low"] <= lvl) & (m1_arm["low"] >= zone_lo)]
    else:
        armed = m1_arm[(m1_arm["high"] >= lvl) & (m1_arm["high"] <= zone_hi)]
    if armed.empty:
        return None
    armed_ts = armed.iloc[0]["datetime"]

    m1_trig = m1[(m1["datetime"] > armed_ts) & (m1["datetime"] <= deadline_ts)]
    if direction == "LONG":
        trig = m1_trig[m1_trig["high"] >= zone_hi]
        if trig.empty:
            return None
        return dict(entry_ts=trig.iloc[0]["datetime"], entry_price=zone_hi, sl=ref_low,
                    ref_open=ref_open, subzone=(zone_lo, zone_hi), move_ref_close=ref.close)
    else:
        trig = m1_trig[m1_trig["low"] <= zone_lo]
        if trig.empty:
            return None
        return dict(entry_ts=trig.iloc[0]["datetime"], entry_price=zone_lo, sl=ref_high,
                    ref_open=ref_open, subzone=(zone_lo, zone_hi), move_ref_close=ref.close)


def simulate_continuation(inv_row, flip_dir: str, m1: pd.DataFrame, deadline_ts,
                           ref_minutes: int) -> Optional[dict]:
    start_ts = inv_row.timestamp + timedelta(minutes=ref_minutes)
    window = m1[(m1["datetime"] >= start_ts) & (m1["datetime"] <= deadline_ts)]
    if window.empty:
        return None
    if flip_dir == "SHORT":
        running = inv_row.low
        for bar in window.itertuples(index=False):
            if bar.low < running:
                return dict(entry_ts=bar.datetime, entry_price=running, sl=inv_row.high,
                            move_ref_close=inv_row.close)
            running = min(running, bar.low)
    else:
        running = inv_row.high
        for bar in window.itertuples(index=False):
            if bar.high > running:
                return dict(entry_ts=bar.datetime, entry_price=running, sl=inv_row.low,
                            move_ref_close=inv_row.close)
            running = max(running, bar.high)
    return None


# ── per-zone driver (HTF zone is D1 when cfg.htf_minutes==0, else intraday) ─

def simulate_d1_zone(zone: dict, m1: pd.DataFrame, m_ref: pd.DataFrame, m_sub: pd.DataFrame,
                      cfg: Config) -> Optional[dict]:
    direction = zone["direction"]
    zone_lo, zone_hi = zone["zone_lo"], zone["zone_hi"]
    entry_line = zone["entry_line"]
    lock_ts = zone["lock_ts"]

    if cfg.htf_minutes == 0:
        known_from = (lock_ts.normalize() + timedelta(days=1)).replace(hour=9, minute=15)
        if known_from.tzinfo is None:
            known_from = IST.localize(known_from)
    else:
        known_from = lock_ts + timedelta(minutes=cfg.htf_minutes)
    deadline = known_from + timedelta(days=MAX_ZONE_AGE_DAYS)

    m1_window = m1[(m1["datetime"] >= known_from) & (m1["datetime"] <= deadline)]
    if m1_window.empty:
        return None

    if direction == "LONG":
        touch = m1_window[m1_window["low"] <= zone_hi]
    else:
        touch = m1_window[m1_window["high"] >= zone_lo]
    if touch.empty:
        return None
    contact_ts = touch.iloc[0]["datetime"]

    orig = run_fractal_from(contact_ts, direction, m1, m_ref, m_sub, cfg, deadline)

    m_ref_after = m_ref[m_ref["timestamp"] >= contact_ts]
    invalidation_ts, inv_row = None, None
    for row in m_ref_after.itertuples(index=False):
        bar_close_time = row.timestamp + timedelta(minutes=cfg.ref_minutes)
        if bar_close_time > deadline:
            break
        if orig is not None and orig["entry_ts"] <= bar_close_time:
            break
        if direction == "LONG" and row.close < entry_line:
            invalidation_ts, inv_row = bar_close_time, row
            break
        if direction == "SHORT" and row.close > entry_line:
            invalidation_ts, inv_row = bar_close_time, row
            break

    if orig is not None and (invalidation_ts is None or orig["entry_ts"] <= invalidation_ts):
        return dict(direction=direction, origin="original", zone_entry_line=entry_line, **orig)

    if invalidation_ts is None:
        return None

    if cfg.violent_move_filter_pct is not None:
        move_pct = abs(inv_row.close - inv_row.open) / inv_row.open * 100.0
        if move_pct > cfg.violent_move_filter_pct:
            return None

    flip_dir = "SHORT" if direction == "LONG" else "LONG"
    candidates = []

    if cfg.enable_continuation:
        cont = simulate_continuation(inv_row, flip_dir, m1, deadline, cfg.ref_minutes)
        if cont:
            candidates.append(("continuation", cont))

    if cfg.enable_retest:
        rt_window = m1[(m1["datetime"] > invalidation_ts) & (m1["datetime"] <= deadline)]
        if flip_dir == "SHORT":
            rt = rt_window[rt_window["high"] >= entry_line]
        else:
            rt = rt_window[rt_window["low"] <= entry_line]
        if not rt.empty:
            retest_anchor_ts = rt.iloc[0]["datetime"]
            rt_res = run_fractal_from(retest_anchor_ts, flip_dir, m1, m_ref, m_sub, cfg, deadline)
            if rt_res:
                candidates.append(("retest", rt_res))

    if not candidates:
        return None

    candidates.sort(key=lambda c: c[1]["entry_ts"])
    origin, res = candidates[0]
    return dict(direction=flip_dir, origin=origin, zone_entry_line=entry_line, **res)


# ── exit simulation (5m close checks, ref-tf TSL ratchet, 15:15 EOD) ──────

def simulate_exit(direction: str, entry_ts, entry_price: float, sl: float,
                   m5: pd.DataFrame, m_ref: pd.DataFrame, ref_minutes: int) -> dict:
    entry_day = entry_ts.date()
    eod_ts = entry_ts.replace(hour=EOD_TIME.hour, minute=EOD_TIME.minute, second=0, microsecond=0)

    m5_after = m5[(m5["timestamp"] > entry_ts) & (m5["timestamp"].dt.date == entry_day)]
    m75_day = m_ref[(m_ref["timestamp"] >= entry_ts) & (m_ref["timestamp"].dt.date == entry_day)]
    tsl = sl
    m75_ratchets = {r.timestamp + timedelta(minutes=ref_minutes): r for r in m75_day.itertuples(index=False)}

    for bar in m5_after.itertuples(index=False):
        ts = bar.timestamp
        if ts >= eod_ts:
            break
        if direction == "LONG":
            if bar.close <= tsl:
                reason = "sl_hit" if tsl == sl else "tsl_hit"
                return dict(exit_ts=ts, exit_price=tsl, reason=reason)
        else:
            if bar.close >= tsl:
                reason = "sl_hit" if tsl == sl else "tsl_hit"
                return dict(exit_ts=ts, exit_price=tsl, reason=reason)
        if ts in m75_ratchets:
            r = m75_ratchets[ts]
            tsl = max(tsl, r.low) if direction == "LONG" else min(tsl, r.high)

    eod_rows = m5[(m5["timestamp"] >= eod_ts) & (m5["timestamp"].dt.date == entry_day)]
    if not eod_rows.empty:
        return dict(exit_ts=eod_ts, exit_price=eod_rows.iloc[0]["close"], reason="eod")
    last = m5[(m5["timestamp"].dt.date == entry_day) & (m5["timestamp"] > entry_ts)]
    if not last.empty:
        row = last.iloc[-1]
        return dict(exit_ts=row["timestamp"], exit_price=row["close"], reason="data_end")
    return dict(exit_ts=entry_ts, exit_price=entry_price, reason="no_data")


# ── full backtest for one config ───────────────────────────────────────────

def run_backtest(zones: List[dict], m1: pd.DataFrame, m_ref: pd.DataFrame, m_sub: pd.DataFrame,
                  m5: pd.DataFrame, cfg: Config) -> List[dict]:
    raw_candidates = []
    for zone in zones:
        res = simulate_d1_zone(zone, m1, m_ref, m_sub, cfg)
        if res:
            raw_candidates.append(res)

    raw_candidates.sort(key=lambda c: c["entry_ts"])

    trades = []
    flat_until = None
    for c in raw_candidates:
        if flat_until is not None and c["entry_ts"] < flat_until:
            continue  # global one-position-at-a-time gate
        exitr = simulate_exit(c["direction"], c["entry_ts"], c["entry_price"], c["sl"], m5, m_ref,
                               cfg.ref_minutes)
        pnl_pts = (exitr["exit_price"] - c["entry_price"]) if c["direction"] == "LONG" \
            else (c["entry_price"] - exitr["exit_price"])
        trades.append(dict(
            direction=c["direction"], origin=c["origin"],
            entry_ts=c["entry_ts"], entry_price=c["entry_price"], sl=c["sl"],
            zone_entry_line=c.get("zone_entry_line"),
            exit_ts=exitr["exit_ts"], exit_price=exitr["exit_price"], reason=exitr["reason"],
            pnl_pts=pnl_pts, pnl_rs=pnl_pts * LOT_SIZE,
        ))
        flat_until = exitr["exit_ts"]
    return trades


def stats(trades: List[dict]) -> dict:
    if not trades:
        return dict(count=0, win_pct=0.0, total_rs=0.0, pf=0.0, max_dd=0.0, avg_r=0.0)
    wins = [t for t in trades if t["pnl_pts"] > 0]
    losses = [t for t in trades if t["pnl_pts"] <= 0]
    gw = sum(t["pnl_pts"] for t in wins)
    gl = abs(sum(t["pnl_pts"] for t in losses))
    pf = gw / gl if gl > 0 else (99.0 if gw > 0 else 0.0)
    eq, peak, dd = 0.0, 0.0, 0.0
    r_sum = 0.0
    for t in trades:
        eq += t["pnl_pts"]
        peak = max(peak, eq)
        dd = max(dd, peak - eq)
        risk = abs(t["entry_price"] - t["sl"])
        if risk > 0:
            r_sum += t["pnl_pts"] / risk
    return dict(count=len(trades), win_pct=100.0 * len(wins) / len(trades),
                total_rs=sum(t["pnl_rs"] for t in trades), pf=round(pf, 2),
                max_dd=round(dd * LOT_SIZE, 0), avg_r=round(r_sum / len(trades), 2))


def stats_risk_normalized(trades: List[dict], risk_rs: float = 5000.0) -> dict:
    """Size each trade so it risks the same rupees regardless of SL width --
    the fair way to compare entry modes whose SL distance differs structurally."""
    if not trades:
        return dict(count=0, win_pct=0.0, total_rs=0.0, pf=0.0, max_dd=0.0, avg_qty_lots=0.0)
    wins_rs, losses_rs, eq, peak, dd, total = [], [], 0.0, 0.0, 0.0, 0.0
    qtys = []
    for t in trades:
        risk_pts = abs(t["entry_price"] - t["sl"])
        if risk_pts <= 0:
            continue
        qty = risk_rs / risk_pts
        qtys.append(qty / LOT_SIZE)
        pnl_rs = t["pnl_pts"] * qty
        total += pnl_rs
        eq += pnl_rs
        peak = max(peak, eq)
        dd = max(dd, peak - eq)
        (wins_rs if pnl_rs > 0 else losses_rs).append(pnl_rs)
    gw, gl = sum(wins_rs), abs(sum(losses_rs))
    pf = gw / gl if gl > 0 else (99.0 if gw > 0 else 0.0)
    n = len(wins_rs) + len(losses_rs)
    return dict(count=n, win_pct=100.0 * len(wins_rs) / n if n else 0.0,
                total_rs=round(total, 0), pf=round(pf, 2), max_dd=round(dd, 0),
                avg_qty_lots=round(sum(qtys) / len(qtys), 2) if qtys else 0.0)


def stats_by_origin(trades: List[dict]) -> Dict[str, dict]:
    out = {}
    for o in ("original", "continuation", "retest"):
        out[o] = stats([t for t in trades if t["origin"] == o])
    return out


# ── pure intraday liquidity-sweep strategy (PDH/PDL/opening-range, single day) ─
#
# Not the fractal HTF-zone mechanic -- this is the "textbook SMC" version:
# map PDH/PDL + opening-range extremes for the day, wait for a sweep+rejection
# on 5-min bars, require a displacement bar (range > threshold, breaks recent
# structure) in the reversal direction, enter at the displacement bar's close,
# SL beyond the sweep wick, target = the opposing liquidity level for the day.
# One trade per day (first valid sweep wins), MIS EOD exit 15:15.

def compute_daily_levels(d1: pd.DataFrame, m1: pd.DataFrame) -> Dict[date, dict]:
    out = {}
    d1s = d1.sort_values("datetime").reset_index(drop=True)
    for i in range(1, len(d1s)):
        day = d1s.iloc[i]["datetime"].date()
        pdh, pdl = d1s.iloc[i - 1]["high"], d1s.iloc[i - 1]["low"]
        or_bars = m1[(m1["datetime"].dt.date == day) & (m1["datetime"].dt.time >= time(9, 15)) &
                     (m1["datetime"].dt.time < time(9, 30))]
        if or_bars.empty:
            continue
        out[day] = dict(pdh=pdh, pdl=pdl, orh=or_bars["high"].max(), orl=or_bars["low"].min())
    return out


def simulate_intraday_sweep_day(day: date, levels: dict, m5_day: pd.DataFrame,
                                 disp_threshold_pct: float = 0.10,
                                 sl_buffer_pct: float = 0.03) -> Optional[dict]:
    m5_day = m5_day[m5_day["timestamp"].dt.time >= time(9, 30)].reset_index(drop=True)
    if len(m5_day) < 5:
        return None
    pool = [("PDH", levels["pdh"], "res", levels["pdl"]),
            ("PDL", levels["pdl"], "sup", levels["pdh"]),
            ("ORH", levels["orh"], "res", levels["pdl"]),
            ("ORL", levels["orl"], "sup", levels["pdh"])]

    for i in range(len(m5_day) - 2):
        bar = m5_day.iloc[i]
        for name, lvl, kind, target in pool:
            swept = (kind == "res" and bar["high"] > lvl) or (kind == "sup" and bar["low"] < lvl)
            if not swept:
                continue
            rejected = (kind == "res" and bar["close"] < lvl) or (kind == "sup" and bar["close"] > lvl)
            reject_bar = bar
            if not rejected:
                if i + 1 >= len(m5_day):
                    continue
                nb = m5_day.iloc[i + 1]
                rejected = (kind == "res" and nb["close"] < lvl) or (kind == "sup" and nb["close"] > lvl)
                if not rejected:
                    continue
                reject_bar = nb
                disp_idx = i + 2
            else:
                disp_idx = i + 1
            if disp_idx >= len(m5_day):
                continue
            disp = m5_day.iloc[disp_idx]
            rng = disp["high"] - disp["low"]
            threshold_pts = disp_threshold_pct / 100.0 * disp["close"]
            if rng < threshold_pts:
                continue
            lookback = m5_day.iloc[max(0, disp_idx - 3):disp_idx]
            if kind == "res":  # swept resistance -> expect bearish reversal (SHORT)
                structure_break = disp["close"] < lookback["low"].min() if not lookback.empty else True
                if not (disp["close"] < reject_bar["open"] and structure_break):
                    continue
                direction = "SHORT"
                entry_price = disp["close"]
                sl = bar["high"] * (1 + sl_buffer_pct / 100.0)
            else:  # swept support -> expect bullish reversal (LONG)
                structure_break = disp["close"] > lookback["high"].max() if not lookback.empty else True
                if not (disp["close"] > reject_bar["open"] and structure_break):
                    continue
                direction = "LONG"
                entry_price = disp["close"]
                sl = bar["low"] * (1 - sl_buffer_pct / 100.0)
            return dict(direction=direction, entry_ts=disp["timestamp"], entry_price=entry_price,
                        sl=sl, target=target, swept_level=name, origin="intraday_sweep")
    return None


def simulate_intraday_sweep_exit(direction: str, entry_ts, entry_price: float, sl: float,
                                  target: float, m5_day: pd.DataFrame) -> dict:
    entry_day = entry_ts.date()
    eod_ts = entry_ts.replace(hour=EOD_TIME.hour, minute=EOD_TIME.minute, second=0, microsecond=0)
    after = m5_day[(m5_day["timestamp"] > entry_ts) & (m5_day["timestamp"].dt.date == entry_day)]
    for bar in after.itertuples(index=False):
        if bar.timestamp >= eod_ts:
            break
        if direction == "LONG":
            if bar.low <= sl:
                return dict(exit_ts=bar.timestamp, exit_price=sl, reason="sl_hit")
            if bar.high >= target:
                return dict(exit_ts=bar.timestamp, exit_price=target, reason="target_hit")
        else:
            if bar.high >= sl:
                return dict(exit_ts=bar.timestamp, exit_price=sl, reason="sl_hit")
            if bar.low <= target:
                return dict(exit_ts=bar.timestamp, exit_price=target, reason="target_hit")
    eod_rows = m5_day[(m5_day["timestamp"] >= eod_ts) & (m5_day["timestamp"].dt.date == entry_day)]
    if not eod_rows.empty:
        return dict(exit_ts=eod_ts, exit_price=eod_rows.iloc[0]["close"], reason="eod")
    return dict(exit_ts=entry_ts, exit_price=entry_price, reason="no_data")


def run_intraday_sweep_backtest(d1: pd.DataFrame, m1: pd.DataFrame, m5: pd.DataFrame) -> List[dict]:
    daily_levels = compute_daily_levels(d1, m1)
    trades = []
    for day, levels in sorted(daily_levels.items()):
        m5_day = m5[m5["timestamp"].dt.date == day]
        if m5_day.empty:
            continue
        c = simulate_intraday_sweep_day(day, levels, m5_day)
        if c is None:
            continue
        exitr = simulate_intraday_sweep_exit(c["direction"], c["entry_ts"], c["entry_price"],
                                              c["sl"], c["target"], m5_day)
        pnl_pts = (exitr["exit_price"] - c["entry_price"]) if c["direction"] == "LONG" \
            else (c["entry_price"] - exitr["exit_price"])
        trades.append(dict(direction=c["direction"], origin=c["origin"], swept_level=c["swept_level"],
                            entry_ts=c["entry_ts"], entry_price=c["entry_price"], sl=c["sl"],
                            exit_ts=exitr["exit_ts"], exit_price=exitr["exit_price"],
                            reason=exitr["reason"], pnl_pts=pnl_pts, pnl_rs=pnl_pts * LOT_SIZE))
    return trades


# ── main ────────────────────────────────────────────────────────────────────

def build_htf_bars(htf_minutes: int, d1_bars, m1: pd.DataFrame, resamples: dict):
    if htf_minutes == 0:
        return d1_bars
    if htf_minutes not in resamples:
        resamples[htf_minutes] = resample(m1, htf_minutes)
    return to_bars(resamples[htf_minutes])


def get_resample(minutes: int, m1: pd.DataFrame, resamples: dict) -> pd.DataFrame:
    if minutes not in resamples:
        resamples[minutes] = resample(m1, minutes)
    return resamples[minutes]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", required=True, help="cache tag e.g. 2025-07-30_2026-07-29")
    ap.add_argument("--sweep", action="store_true")
    ap.add_argument("--single", action="store_true")
    ap.add_argument("--sweep-tf", action="store_true")
    ap.add_argument("--sweep-entry", action="store_true")
    ap.add_argument("--liquidity-compare", action="store_true")
    args = ap.parse_args()

    d1, m1 = load_data(args.tag)
    print(f"D1 bars: {len(d1)}   1m bars: {len(m1)}   "
          f"range {m1['datetime'].min()} -> {m1['datetime'].max()}", flush=True)

    resamples: dict = {}
    m75 = get_resample(75, m1, resamples)
    m5 = get_resample(5, m1, resamples)
    print(f"75m bars: {len(m75)}   5m bars: {len(m5)}", flush=True)

    d1_bars = to_bars(d1)
    zones = detect_d1_zones(d1_bars)
    print(f"D1 zones detected: {len(zones)} "
          f"({sum(1 for z in zones if z['direction']=='LONG')} LONG / "
          f"{sum(1 for z in zones if z['direction']=='SHORT')} SHORT)", flush=True)

    if args.single:
        cfg = Config()
        trades = run_backtest(zones, m1, m75, m5, m5, cfg)
        s = stats(trades)
        print(f"\nBaseline config: {cfg}")
        print(f"Trades={s['count']}  Win%={s['win_pct']:.1f}  "
              f"Rs{s['total_rs']:+,.0f}  PF={s['pf']}  DD=Rs{s['max_dd']:,.0f}  avgR={s['avg_r']}")
        for o, so in stats_by_origin(trades).items():
            print(f"  {o:>12}: n={so['count']:>3}  win%={so['win_pct']:.1f}  "
                  f"Rs{so['total_rs']:+,.0f}  PF={so['pf']}")
        for t in trades:
            print(f"  {t['entry_ts']}  {t['direction']:>5}  {t['origin']:>12}  "
                  f"entry={t['entry_price']:.1f} sl={t['sl']:.1f} exit={t['exit_price']:.1f} "
                  f"({t['reason']})  Rs{t['pnl_rs']:+,.0f}")
        return

    if args.sweep:
        thresholds = [0.10, 0.15, 0.20, 0.30]
        flip_modes = [(False, False), (True, False), (False, True), (True, True)]
        fallbacks = ["skip", "raw_breakout"]

        results = []
        combo_n = 0
        total = len(thresholds) * len(flip_modes) * len(fallbacks)
        for th, (cont, retest), fb in itertools.product(thresholds, flip_modes, fallbacks):
            combo_n += 1
            cfg = Config(zone_size_threshold_pct=th, enable_continuation=cont,
                         enable_retest=retest, fallback_on_no_subzone=fb)
            trades = run_backtest(zones, m1, m75, m5, m5, cfg)
            s = stats(trades)
            print(f"  {combo_n}/{total}  th={th:>4.2f}%  cont={cont!s:>5}  retest={retest!s:>5}  "
                  f"fb={fb:>12}  n={s['count']:>3}  win%={s['win_pct']:>5.1f}  "
                  f"Rs{s['total_rs']:>+9,.0f}  PF={s['pf']:>5.2f}  DD=Rs{s['max_dd']:>7,.0f}  "
                  f"avgR={s['avg_r']:>5.2f}", flush=True)
            results.append(dict(threshold=th, continuation=cont, retest=retest,
                                 fallback=fb, **s))

        results.sort(key=lambda r: (r["count"] >= 5, r["pf"]), reverse=True)
        print("\n" + "=" * 100)
        print("TOP 15 BY PROFIT FACTOR (min 5 trades):")
        print("=" * 100)
        shown = [r for r in results if r["count"] >= 5][:15]
        for rank, r in enumerate(shown, 1):
            print(f"{rank:>2}. th={r['threshold']:.2f}%  cont={r['continuation']!s:>5}  "
                  f"retest={r['retest']!s:>5}  fb={r['fallback']:>12}  "
                  f"n={r['count']:>3}  win%={r['win_pct']:>5.1f}  "
                  f"Rs{r['total_rs']:>+9,.0f}  PF={r['pf']:>5.2f}  DD=Rs{r['max_dd']:>7,.0f}  "
                  f"avgR={r['avg_r']:>5.2f}")

    if args.liquidity_compare:
        # 1) Our fractal strategy's winning config: 60m/15m/5m, swing_breach, continuation.
        htf_m, ref_m = 60, 15
        htf_bars = build_htf_bars(htf_m, d1_bars, m1, resamples)
        m_ref = get_resample(ref_m, m1, resamples)
        zones_tf = detect_d1_zones(htf_bars)
        cfg = Config(zone_size_threshold_pct=0.20, enable_continuation=True, enable_retest=False,
                     fallback_on_no_subzone="raw_breakout", htf_minutes=htf_m, ref_minutes=ref_m,
                     sub_minutes=5, entry_mode="swing_breach")
        fractal_trades = run_backtest(zones_tf, m1, m_ref, m5, m5, cfg)

        # 2) Tag each fractal trade's origin zone by proximity to a genuine PDH/PDL level.
        daily_levels = compute_daily_levels(d1, m1)
        shelf = []
        for lv in daily_levels.values():
            shelf.extend([lv["pdh"], lv["pdl"]])
        tol_pct = 0.10
        tagged, untagged = [], []
        for t in fractal_trades:
            el = t.get("zone_entry_line")
            if el is None:
                untagged.append(t)
                continue
            tol_pts = tol_pct / 100.0 * el
            near = any(abs(el - lv) <= tol_pts for lv in shelf)
            (tagged if near else untagged).append(t)

        # 3) Pure intraday liquidity-sweep strategy (PDH/PDL/opening-range, single-day).
        sweep_trades = run_intraday_sweep_backtest(d1, m1, m5)

        print("=" * 100)
        print("COMPARISON: fractal HTF-zone strategy vs pure intraday liquidity-sweep strategy")
        print("=" * 100)

        print("\n[A] Fractal strategy (60m/15m/5m, swing_breach, continuation) -- fixed 1-lot:")
        s = stats(fractal_trades)
        print(f"    n={s['count']}  win%={s['win_pct']:.1f}  Rs{s['total_rs']:+,.0f}  "
              f"PF={s['pf']}  DD=Rs{s['max_dd']:,.0f}  avgR={s['avg_r']}")
        rs = stats_risk_normalized(fractal_trades)
        print(f"    risk-normalized (Rs5000/trade): n={rs['count']}  win%={rs['win_pct']:.1f}  "
              f"Rs{rs['total_rs']:+,.0f}  PF={rs['pf']}  DD=Rs{rs['max_dd']:,.0f}")

        print(f"\n[A-tagged] Fractal trades whose zone level is within {tol_pct}% of a genuine PDH/PDL "
              f"(n={len(tagged)}):")
        rs_t = stats_risk_normalized(tagged)
        print(f"    win%={rs_t['win_pct']:.1f}  Rs{rs_t['total_rs']:+,.0f}  PF={rs_t['pf']}  "
              f"DD=Rs{rs_t['max_dd']:,.0f}")
        print(f"[A-untagged] Fractal trades NOT near a genuine PDH/PDL (n={len(untagged)}):")
        rs_u = stats_risk_normalized(untagged)
        print(f"    win%={rs_u['win_pct']:.1f}  Rs{rs_u['total_rs']:+,.0f}  PF={rs_u['pf']}  "
              f"DD=Rs{rs_u['max_dd']:,.0f}")

        print(f"\n[B] Pure intraday liquidity-sweep strategy (PDH/PDL/opening-range, single-day):")
        s2 = stats(sweep_trades)
        print(f"    n={s2['count']}  win%={s2['win_pct']:.1f}  Rs{s2['total_rs']:+,.0f}  "
              f"PF={s2['pf']}  DD=Rs{s2['max_dd']:,.0f}  avgR={s2['avg_r']}")
        rs2 = stats_risk_normalized(sweep_trades)
        print(f"    risk-normalized (Rs5000/trade): n={rs2['count']}  win%={rs2['win_pct']:.1f}  "
              f"Rs{rs2['total_rs']:+,.0f}  PF={rs2['pf']}  DD=Rs{rs2['max_dd']:,.0f}")
        if sweep_trades:
            from collections import Counter
            lvl_counts = Counter(t["swept_level"] for t in sweep_trades)
            print(f"    swept-level breakdown: {dict(lvl_counts)}")
            for t in sweep_trades[:15]:
                print(f"      {t['entry_ts']}  {t['direction']:>5}  swept={t['swept_level']:>4}  "
                      f"entry={t['entry_price']:.1f} sl={t['sl']:.1f} exit={t['exit_price']:.1f} "
                      f"({t['reason']})  Rs{t['pnl_rs']:+,.0f}")
        return

    if args.sweep_entry:
        # Winning stack from --sweep-tf: 60m HTF zone -> 15m ref-candle -> 5m sub-zone.
        htf_m, ref_m = 60, 15
        htf_bars = build_htf_bars(htf_m, d1_bars, m1, resamples)
        m_ref = get_resample(ref_m, m1, resamples)
        zones_tf = detect_d1_zones(htf_bars)
        print(f"60m/15m/5m zones: {len(zones_tf)}\n", flush=True)

        modes = ["mtf_breach", "swing_breach"]
        th = 0.20   # not sensitive per earlier sweep
        print("Fixed 1-lot sizing (what was reported before -- masks SL-width differences):")
        for mode in modes:
            cfg = Config(zone_size_threshold_pct=th, enable_continuation=True, enable_retest=False,
                         fallback_on_no_subzone="raw_breakout", htf_minutes=htf_m, ref_minutes=ref_m,
                         sub_minutes=5, entry_mode=mode)
            trades = run_backtest(zones_tf, m1, m_ref, m5, m5, cfg)
            s = stats(trades)
            avg_sl_pts = sum(abs(t["entry_price"] - t["sl"]) for t in trades) / len(trades)
            print(f"  mode={mode:>14}  n={s['count']:>4}  win%={s['win_pct']:>5.1f}  "
                  f"Rs{s['total_rs']:>+10,.0f}  PF={s['pf']:>5.2f}  DD=Rs{s['max_dd']:>7,.0f}  "
                  f"avgR={s['avg_r']:>5.2f}  avgSL={avg_sl_pts:>6.1f}pts", flush=True)

        print("\nRisk-normalized sizing (Rs5,000 risked per trade, both modes) -- the fair comparison:")
        for mode in modes:
            cfg = Config(zone_size_threshold_pct=th, enable_continuation=True, enable_retest=False,
                         fallback_on_no_subzone="raw_breakout", htf_minutes=htf_m, ref_minutes=ref_m,
                         sub_minutes=5, entry_mode=mode)
            trades = run_backtest(zones_tf, m1, m_ref, m5, m5, cfg)
            rs = stats_risk_normalized(trades, risk_rs=5000.0)
            print(f"  mode={mode:>14}  n={rs['count']:>4}  win%={rs['win_pct']:>5.1f}  "
                  f"Rs{rs['total_rs']:>+10,.0f}  PF={rs['pf']:>5.2f}  DD=Rs{rs['max_dd']:>7,.0f}  "
                  f"avgQty={rs['avg_qty_lots']:>5.2f}lots", flush=True)
        return

    if args.sweep_tf:
        # HTF stacks: (label, htf_minutes, ref_minutes)  -- sub stays 5min throughout
        stacks = [
            ("D1/75/5 (current)", 0, 75),
            ("75m/15m/5m", 75, 15),
            ("60m/15m/5m", 60, 15),
        ]
        thresholds = [0.10, 0.20, 0.30]
        flip_modes = [(True, False), (True, True)]
        fallback = "raw_breakout"

        results = []
        combo_n = 0
        total = len(stacks) * len(thresholds) * len(flip_modes)
        for (label, htf_m, ref_m), th, (cont, retest) in itertools.product(stacks, thresholds, flip_modes):
            combo_n += 1
            htf_bars = build_htf_bars(htf_m, d1_bars, m1, resamples)
            m_ref = get_resample(ref_m, m1, resamples) if ref_m != 75 else m75
            zones_tf = detect_d1_zones(htf_bars)
            cfg = Config(zone_size_threshold_pct=th, enable_continuation=cont, enable_retest=retest,
                         fallback_on_no_subzone=fallback, htf_minutes=htf_m, ref_minutes=ref_m,
                         sub_minutes=5)
            trades = run_backtest(zones_tf, m1, m_ref, m5, m5, cfg)
            s = stats(trades)
            print(f"  {combo_n}/{total}  stack={label:>18}  th={th:>4.2f}%  cont={cont!s:>5}  "
                  f"retest={retest!s:>5}  zones={len(zones_tf):>4}  n={s['count']:>3}  "
                  f"win%={s['win_pct']:>5.1f}  Rs{s['total_rs']:>+9,.0f}  PF={s['pf']:>5.2f}  "
                  f"DD=Rs{s['max_dd']:>7,.0f}  avgR={s['avg_r']:>5.2f}", flush=True)
            results.append(dict(stack=label, threshold=th, continuation=cont, retest=retest,
                                 zones=len(zones_tf), **s))

        results.sort(key=lambda r: (r["count"] >= 8, r["pf"]), reverse=True)
        print("\n" + "=" * 105)
        print("TOP 15 BY PROFIT FACTOR (min 8 trades):")
        print("=" * 105)
        shown = [r for r in results if r["count"] >= 8][:15]
        for rank, r in enumerate(shown, 1):
            print(f"{rank:>2}. stack={r['stack']:>18}  th={r['threshold']:.2f}%  "
                  f"cont={r['continuation']!s:>5}  retest={r['retest']!s:>5}  "
                  f"zones={r['zones']:>4}  n={r['count']:>3}  win%={r['win_pct']:>5.1f}  "
                  f"Rs{r['total_rs']:>+9,.0f}  PF={r['pf']:>5.2f}  DD=Rs{r['max_dd']:>7,.0f}  "
                  f"avgR={r['avg_r']:>5.2f}")


if __name__ == "__main__":
    main()
