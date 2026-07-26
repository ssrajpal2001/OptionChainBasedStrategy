"""backtest/v4_cascade/july_trade_diagnostic.py -- loser vs. winner audit for
July 2026 Run-5 trades. Computes NIFTY spot context at each entry (intraday VWAP,
day trend, spot vs. round level) and zone-quality metrics (depth, lock-to-entry
speed, ITM distance) from the cached 1-minute spot data + known Run-5 audit data.
No new API calls required."""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

# ── Known Run-5 trade audit data (from backtest output) ─────────────────────

TRADES = [
    {
        "id":         "CE-24300-Jul07",
        "side":       "CE",
        "strike":     24300,
        "entry_ts":   datetime(2026, 7, 7, 15, 5, tzinfo=IST),
        "entry_px":   333.00,
        "zone_low":   301.0,
        "zone_high":  349.0,
        "sl":         281.0,
        "target":     368.90,
        "htf_lock_ts": datetime(2026, 7, 7, 9, 15, tzinfo=IST),
        "reentry_ts": datetime(2026, 7, 7, 13, 0, tzinfo=IST),
        "trigger_ts": datetime(2026, 7, 7, 14, 35, tzinfo=IST),
        "rsi":        25.1,
        "t1_pnl":     -1950,
        "t2_pnl":     -1950,
        "t1_reason":  "structural_flip",
        "t2_reason":  "structural_flip",
    },
    {
        "id":         "PE-24200-Jul09",
        "side":       "PE",
        "strike":     24200,
        "entry_ts":   datetime(2026, 7, 9, 12, 15, tzinfo=IST),
        "entry_px":   331.30,
        "zone_low":   257.55,
        "zone_high":  361.3,
        "sl":         237.55,
        "target":     396.05,
        "htf_lock_ts": datetime(2026, 7, 9, 9, 15, tzinfo=IST),
        "reentry_ts": datetime(2026, 7, 9, 10, 30, tzinfo=IST),
        "trigger_ts": datetime(2026, 7, 9, 12, 10, tzinfo=IST),
        "rsi":        46.4,
        "t1_pnl":     4856,
        "t2_pnl":     -570,
        "t1_reason":  "t1_target_2r",
        "t2_reason":  "t2_trailing_base_stop",
    },
    {
        "id":         "PE-24300-Jul10",
        "side":       "PE",
        "strike":     24300,
        "entry_ts":   datetime(2026, 7, 10, 11, 55, tzinfo=IST),
        "entry_px":   303.30,
        "zone_low":   270.55,
        "zone_high":  333.3,
        "sl":         250.55,
        "target":     351.95,
        "htf_lock_ts": datetime(2026, 7, 10, 9, 15, tzinfo=IST),
        "reentry_ts": datetime(2026, 7, 10, 10, 30, tzinfo=IST),
        "trigger_ts": datetime(2026, 7, 10, 11, 50, tzinfo=IST),
        "rsi":        50.5,
        "t1_pnl":     3649,
        "t2_pnl":     -1748,
        "t1_reason":  "t1_target_2r",
        "t2_reason":  "t2_trailing_base_stop",
    },
    {
        "id":         "CE-23800-Jul14",
        "side":       "CE",
        "strike":     23800,
        "entry_ts":   datetime(2026, 7, 14, 10, 50, tzinfo=IST),
        "entry_px":   450.00,
        "zone_low":   379.05,
        "zone_high":  480.0,
        "sl":         359.05,
        "target":     551.45,
        "htf_lock_ts": datetime(2026, 7, 13, 11, 45, tzinfo=IST),
        "reentry_ts": datetime(2026, 7, 14, 9, 15, tzinfo=IST),
        "trigger_ts": datetime(2026, 7, 14, 10, 45, tzinfo=IST),
        "rsi":        43.1,
        "t1_pnl":     -3878,
        "t2_pnl":     -458,
        "t1_reason":  "structural_flip",
        "t2_reason":  "t2_trailing_base_stop",
    },
    {
        "id":         "PE-24200-Jul15",
        "side":       "PE",
        "strike":     24200,
        "entry_ts":   datetime(2026, 7, 15, 14, 25, tzinfo=IST),
        "entry_px":   295.00,
        "zone_low":   215.5,
        "zone_high":  325.0,
        "sl":         195.50,
        "target":     353.70,
        "htf_lock_ts": datetime(2026, 7, 15, 11, 45, tzinfo=IST),
        "reentry_ts": datetime(2026, 7, 15, 13, 0, tzinfo=IST),
        "trigger_ts": datetime(2026, 7, 15, 14, 20, tzinfo=IST),
        "rsi":        53.0,
        "t1_pnl":     -38,
        "t2_pnl":     -150,
        "t1_reason":  "structural_flip",
        "t2_reason":  "t2_trailing_base_stop",
    },
    {
        "id":         "CE-24000-Jul16",
        "side":       "CE",
        "strike":     24000,
        "entry_ts":   datetime(2026, 7, 16, 13, 25, tzinfo=IST),
        "entry_px":   278.00,
        "zone_low":   253.0,
        "zone_high":  290.5,
        "sl":         233.0,
        "target":     324.00,
        "htf_lock_ts": datetime(2026, 7, 16, 10, 30, tzinfo=IST),
        "reentry_ts": datetime(2026, 7, 16, 11, 45, tzinfo=IST),
        "trigger_ts": datetime(2026, 7, 16, 13, 5, tzinfo=IST),
        "rsi":        31.0,
        "t1_pnl":     3450,
        "t2_pnl":     -1305,
        "t1_reason":  "t1_target_2r",
        "t2_reason":  "t2_trailing_base_stop",
    },
    {
        "id":         "CE-24000-Jul17",
        "side":       "CE",
        "strike":     24000,
        "entry_ts":   datetime(2026, 7, 17, 13, 50, tzinfo=IST),
        "entry_px":   327.75,
        "zone_low":   253.0,
        "zone_high":  357.75,
        "sl":         233.0,
        "target":     378.90,
        "htf_lock_ts": datetime(2026, 7, 17, 9, 15, tzinfo=IST),
        "reentry_ts": datetime(2026, 7, 17, 10, 30, tzinfo=IST),
        "trigger_ts": datetime(2026, 7, 17, 11, 50, tzinfo=IST),
        "rsi":        33.3,
        "t1_pnl":     3836,
        "t2_pnl":     5494,
        "t1_reason":  "t1_target_2r",
        "t2_reason":  "t2_trailing_base_stop",
    },
    {
        "id":         "PE-24200-Jul17",
        "side":       "PE",
        "strike":     24200,
        "entry_ts":   datetime(2026, 7, 17, 9, 30, tzinfo=IST),
        "entry_px":   214.55,
        "zone_low":   204.45,
        "zone_high":  219.6,
        "sl":         184.45,
        "target":     265.55,
        "htf_lock_ts": datetime(2026, 7, 16, 10, 30, tzinfo=IST),
        "reentry_ts": datetime(2026, 7, 16, 11, 45, tzinfo=IST),
        "trigger_ts": datetime(2026, 7, 16, 13, 10, tzinfo=IST),
        "rsi":        30.7,
        "t1_pnl":     -2258,
        "t2_pnl":     -379,
        "t1_reason":  "t1_sl_structural_floor",
        "t2_reason":  "t2_trailing_base_stop",
    },
    {
        "id":         "CE-24000-Jul20",
        "side":       "CE",
        "strike":     24000,
        "entry_ts":   datetime(2026, 7, 20, 12, 15, tzinfo=IST),
        "entry_px":   336.15,
        "zone_low":   253.0,
        "zone_high":  366.15,
        "sl":         233.0,
        "target":     408.00,
        "htf_lock_ts": datetime(2026, 7, 17, 14, 15, tzinfo=IST),
        "reentry_ts": datetime(2026, 7, 20, 9, 15, tzinfo=IST),
        "trigger_ts": datetime(2026, 7, 20, 12, 10, tzinfo=IST),
        "rsi":        42.6,
        "t1_pnl":     1110,
        "t2_pnl":     -3626,
        "t1_reason":  "structural_flip",
        "t2_reason":  "t2_trailing_base_stop",
    },
    {
        "id":         "PE-24300-Jul20",
        "side":       "PE",
        "strike":     24300,
        "entry_ts":   datetime(2026, 7, 20, 14, 35, tzinfo=IST),
        "entry_px":   218.47,
        "zone_low":   191.0,
        "zone_high":  232.2,
        "sl":         171.0,
        "target":     271.50,
        "htf_lock_ts": datetime(2026, 7, 20, 9, 15, tzinfo=IST),
        "reentry_ts": datetime(2026, 7, 20, 13, 0, tzinfo=IST),
        "trigger_ts": datetime(2026, 7, 20, 14, 30, tzinfo=IST),
        "rsi":        41.6,
        "t1_pnl":     3978,
        "t2_pnl":     -1138,
        "t1_reason":  "t1_target_2r",
        "t2_reason":  "t2_trailing_base_stop",
    },
]

# Pool-clearing impact (from event log: which trade was open when another would have fired)
POOL_CLEARING_IMPACT = {
    "CE-24300-Jul07": "Held CE pool Jul07→Jul09 for 2 days. Jul09 PE winner had to wait for structural flip.",
    "PE-24200-Jul09": "No blocking — PE pool was free; CE 24300 structural flip released it.",
    "PE-24300-Jul10": "No blocking — entered clean.",
    "CE-23800-Jul14": "Held CE pool Jul14→Jul15; CE 24000 Jul16 winner delayed by one day.",
    "PE-24200-Jul15": "Quick exit (structural flip Jul16); released CE pool for Jul16 winner.",
    "CE-24000-Jul16": "Structural flip at 13:25 directly enabled Jul17 CE 24000 via flip mechanism.",
    "CE-24000-Jul17": "Best winner. PE 24200 Jul17 LOSS was open same day — CE flip closed it.",
    "PE-24200-Jul17": "Open from 09:30, SL floor hit at 14:15 by CE structural flip. Blocked NO new PE.",
    "CE-24000-Jul20": "Structural flip at 15:20 enabled PE 24300 Jul20 same day.",
    "PE-24300-Jul20": "No subsequent trade blocked — end of dataset.",
}

# ── Load cached 1-minute NIFTY spot data ─────────────────────────────────────

def _load_spot_1m(path: str) -> List[dict]:
    with open(path) as f:
        rows = json.load(f)
    for r in rows:
        r["_ts"] = datetime.fromisoformat(r["ts"])
        if r["_ts"].tzinfo is None:
            r["_ts"] = r["_ts"].replace(tzinfo=IST)
    return rows


def _spot_at(rows: List[dict], ts: datetime) -> Optional[float]:
    """Closest 1m bar close at or before ts."""
    best = None
    for r in rows:
        if r["_ts"] <= ts:
            best = r
        elif best is not None:
            break
    return best["close"] if best else None


def _day_ohlc(rows: List[dict], d) -> Optional[dict]:
    day_rows = [r for r in rows if r["_ts"].date() == d
                and r["_ts"].hour >= 9 and r["_ts"].hour < 16]
    if not day_rows:
        return None
    return {
        "open": day_rows[0]["open"],
        "high": max(r["high"] for r in day_rows),
        "low": min(r["low"] for r in day_rows),
        "close": day_rows[-1]["close"],
    }


def _vwap_at(rows: List[dict], d, ts: datetime) -> Optional[float]:
    """Cumulative VWAP from day open (09:15) up to ts."""
    day_rows = [r for r in rows
                if r["_ts"].date() == d and r["_ts"] <= ts
                and r["_ts"].hour >= 9]
    if not day_rows:
        return None
    cum_tp_vol = sum(((r["high"] + r["low"] + r["close"]) / 3) * max(r.get("volume", 1), 1)
                     for r in day_rows)
    cum_vol = sum(max(r.get("volume", 1), 1) for r in day_rows)
    return round(cum_tp_vol / cum_vol, 2)


def _spot_at_entry_range_pct(day: dict, spot: float) -> float:
    """Where spot sits within the day's range at entry: 0 = day low, 1 = day high."""
    rng = day["high"] - day["low"]
    if rng < 1:
        return 0.5
    return round((spot - day["low"]) / rng, 2)


def _nearest_round(spot: float, step: float = 100.0) -> float:
    return round(round(spot / step) * step, 2)


def _spot_trend_at_entry(rows: List[dict], d, ts: datetime) -> str:
    """Simple intraday trend: compare spot at ts to day open. Return UP/DOWN/FLAT."""
    day_open_rows = [r for r in rows if r["_ts"].date() == d and r["_ts"].hour == 9]
    if not day_open_rows:
        return "UNKNOWN"
    open_px = day_open_rows[0]["open"]
    spot = _spot_at(rows, ts)
    if spot is None:
        return "UNKNOWN"
    delta = spot - open_px
    if delta > 50:
        return f"UP  ({delta:+.0f})"
    elif delta < -50:
        return f"DOWN({delta:+.0f})"
    else:
        return f"FLAT({delta:+.0f})"


def _spot_day_trend(rows: List[dict], d) -> str:
    """Full-day trend: open vs close."""
    day = _day_ohlc(rows, d)
    if not day:
        return "UNKNOWN"
    delta = day["close"] - day["open"]
    pct = delta / day["open"] * 100
    if delta > 60:
        return f"BULL +{pct:.1f}%  (O={day['open']:.0f} C={day['close']:.0f})"
    elif delta < -60:
        return f"BEAR {pct:.1f}%  (O={day['open']:.0f} C={day['close']:.0f})"
    else:
        return f"CHOP {pct:+.1f}%  (O={day['open']:.0f} C={day['close']:.0f})"


# ── CE/PE alignment rule ─────────────────────────────────────────────────────

def _side_aligned_with_spot_trend(side: str, spot: float, vwap: Optional[float],
                                   day_trend: str) -> Tuple[bool, str]:
    """Returns (aligned, reason).
    CE = buying calls = expecting premium to rebound = needs spot to be low/rising.
    PE = buying puts = expecting put premium to rebound = needs spot to be high/falling.
    Primary: spot vs VWAP. Secondary: intraday trend direction.
    """
    if vwap is None:
        return True, "VWAP N/A"
    diff = spot - vwap
    if side == "CE":
        aligned = diff <= 0  # spot at or below VWAP → oversold → CE trap valid
        label = f"Spot {'below' if diff<=0 else 'ABOVE'} VWAP by {abs(diff):.0f}pt"
    else:  # PE
        aligned = diff >= 0  # spot at or above VWAP → overbought → PE trap valid
        label = f"Spot {'above' if diff>=0 else 'BELOW'} VWAP by {abs(diff):.0f}pt"
    return aligned, label


# ── Zone quality metrics ─────────────────────────────────────────────────────

def _zone_quality(t: dict) -> dict:
    depth = t["zone_high"] - t["zone_low"]
    entry_px = t["entry_px"]
    depth_pct = depth / entry_px * 100  # depth as % of entry price
    # Wick quality proxy: how much of the zone depth was filled at entry
    # (entry = zone_high - depth/3, so fill_depth = zone_high - entry_px)
    fill_depth = t["zone_high"] - entry_px
    fill_ratio = fill_depth / depth if depth > 0 else 0  # should be ~0.33 for 1/3 formula
    # Time from lock to entry
    lock_to_entry_mins = (t["entry_ts"] - t["htf_lock_ts"]).total_seconds() / 60
    # Time from reentry to entry
    reentry_to_entry_mins = (t["entry_ts"] - t["reentry_ts"]).total_seconds() / 60
    return {
        "depth": round(depth, 2),
        "depth_pct": round(depth_pct, 1),
        "fill_ratio": round(fill_ratio, 2),
        "lock_to_entry_h": round(lock_to_entry_mins / 60, 1),
        "reentry_to_entry_h": round(reentry_to_entry_mins / 60, 1),
    }


# ── Spot-to-Strike distance ──────────────────────────────────────────────────

def _strike_distance(spot: float, strike: int, side: str) -> str:
    """How ITM/OTM is the strike vs. spot at entry."""
    diff = spot - strike
    if side == "CE":
        # CE: strike < spot → ITM CE (buying ITM call is expensive, high absolute premium)
        # CE: strike > spot → OTM CE
        itm = diff > 0
        label = f"{'ITM' if itm else 'OTM'} by {abs(diff):.0f}pt"
    else:
        # PE: strike > spot → ITM PE
        itm = diff < 0
        label = f"{'ITM' if itm else 'OTM'} by {abs(diff):.0f}pt"
    return label


# ── Main report ──────────────────────────────────────────────────────────────

def run_diagnostic():
    spot_rows = _load_spot_1m(
        "backtest/v4_cascade/data_cache/NSE_INDEX_Nifty_50_2026-04-23_2026-07-22.json"
    )

    SPOT_SESSION_OPENS = {
        # from Run-5 output
        "2026-07-07": 24464.45, "2026-07-09": 23928.95, "2026-07-10": 24124.70,
        "2026-07-13": 24039.40, "2026-07-14": 24068.00, "2026-07-15": 24085.85,
        "2026-07-16": 24142.10, "2026-07-17": 24127.60, "2026-07-20": 24190.05,
    }

    print("=" * 110)
    print("LOSER vs WINNER DIAGNOSTIC — July 2026 Run-5 (10 trades)")
    print("=" * 110)

    for t in TRADES:
        net_pnl = t["t1_pnl"] + t["t2_pnl"]
        verdict = "WIN " if net_pnl > 0 else "LOSS"
        d = t["entry_ts"].date()
        entry_ts = t["entry_ts"]
        zq = _zone_quality(t)

        # Spot at entry
        spot = _spot_at(spot_rows, entry_ts)
        if spot is None:
            spot = SPOT_SESSION_OPENS.get(str(d), 0.0)

        vwap = _vwap_at(spot_rows, d, entry_ts)
        day = _day_ohlc(spot_rows, d)
        day_trend = _spot_day_trend(spot_rows, d)
        range_pos = _spot_at_entry_range_pct(day, spot) if day else 0.5
        nearest_round = _nearest_round(spot)
        round_dist = abs(spot - nearest_round)
        intraday_trend = _spot_trend_at_entry(spot_rows, d, entry_ts)
        aligned, alignment_note = _side_aligned_with_spot_trend(
            t["side"], spot, vwap, day_trend)
        strike_dist = _strike_distance(spot, t["strike"], t["side"])

        symbol = "✓" if aligned else "✗"
        print(f"\n{'-'*110}")
        print(f"  {verdict}  {t['id']:<22}  Net P&L: Rs {net_pnl:+,}")
        print(f"  Entry: {entry_ts.strftime('%H:%M')} | RSI: {t['rsi']:.1f} | "
              f"T1:{t['t1_reason']}  T2:{t['t2_reason']}")
        print(f"\n  ── ZONE QUALITY ──")
        print(f"     Depth: {zq['depth']:.1f}pts  ({zq['depth_pct']:.1f}% of premium)  "
              f"| Fill ratio: {zq['fill_ratio']:.2f} (expected ~0.33)")
        print(f"     Lock→Entry: {zq['lock_to_entry_h']:.1f}h  "
              f"| Reentry→Entry: {zq['reentry_to_entry_h']:.1f}h")
        print(f"     Zone: [{t['zone_low']:.1f}, {t['zone_high']:.1f}]  "
              f"SL: {t['sl']:.1f}  Target: {t['target']:.2f}")

        print(f"\n  ── SPOT CONTEXT AT ENTRY ──")
        print(f"     NIFTY spot: {spot:.1f}  |  Strike: {t['strike']}  |  {strike_dist}")
        print(f"     Nearest round: {nearest_round:.0f}  ({round_dist:.0f}pt away)")
        if vwap:
            print(f"     VWAP at entry: {vwap:.1f}  |  Spot vs VWAP: {spot-vwap:+.1f}pt")
        if day:
            print(f"     Day OHLC: O={day['open']:.0f} H={day['high']:.0f} "
                  f"L={day['low']:.0f} C={day['close']:.0f}  "
                  f"(spot at {range_pos:.0%} of day range)")
        print(f"     Intraday trend at entry: {intraday_trend}")
        print(f"     Full-day trend: {day_trend}")
        print(f"\n  [{symbol}] SIDE-SPOT ALIGNMENT: {alignment_note}")
        print(f"\n  ── POOL CLEARING ──")
        print(f"     {POOL_CLEARING_IMPACT.get(t['id'], 'N/A')}")

    # ── Summary table ────────────────────────────────────────────────────────
    print(f"\n\n{'='*110}")
    print("COMPARISON SUMMARY TABLE")
    print(f"{'='*110}")
    print(f"{'#':<3} {'Trade':<24} {'Net P&L':>9} {'Depth':>6} {'RSI':>5} "
          f"{'Spot vs VWAP':>14} {'Range%':>7} {'Day':>5} {'Aligned':>8}")
    print(f"{'─'*110}")

    for t in TRADES:
        net_pnl = t["t1_pnl"] + t["t2_pnl"]
        verdict = "WIN " if net_pnl > 0 else "LOSS"
        d = t["entry_ts"].date()
        zq = _zone_quality(t)
        spot = _spot_at(spot_rows, t["entry_ts"]) or 0.0
        vwap = _vwap_at(spot_rows, d, t["entry_ts"]) or spot
        range_pos = 0.5
        if day := _day_ohlc(spot_rows, d):
            range_pos = _spot_at_entry_range_pct(day, spot)
        aligned, _ = _side_aligned_with_spot_trend(t["side"], spot, vwap, "")
        vs_vwap = spot - vwap
        day_close = _day_ohlc(spot_rows, d)
        day_dir = ""
        if day_close:
            day_delta = day_close["close"] - day_close["open"]
            day_dir = "BULL" if day_delta > 60 else ("BEAR" if day_delta < -60 else "CHOP")
        print(f"{verdict:<4} {t['id']:<24} {net_pnl:>+9,} {zq['depth']:>6.0f} "
              f"{t['rsi']:>5.1f} {vs_vwap:>+14.0f} {range_pos:>6.0%} "
              f"{day_dir:>5} {'YES' if aligned else 'NO':>8}")

    print(f"\n{'-'*110}")
    # Pattern analysis
    wins = [t for t in TRADES if t["t1_pnl"] + t["t2_pnl"] > 0]
    losses = [t for t in TRADES if t["t1_pnl"] + t["t2_pnl"] <= 0]

    def avg(lst, key):
        vals = [key(t) for t in lst if key(t) is not None]
        return sum(vals) / len(vals) if vals else float("nan")

    def spot_vwap_diff(t):
        d = t["entry_ts"].date()
        spot = _spot_at(spot_rows, t["entry_ts"]) or 0.0
        vwap = _vwap_at(spot_rows, d, t["entry_ts"])
        return (spot - vwap) if vwap else None

    def zone_depth(t):
        return t["zone_high"] - t["zone_low"]

    def reentry_speed(t):
        return (t["entry_ts"] - t["reentry_ts"]).total_seconds() / 3600

    print(f"\n  AVERAGES           {'WINNERS ('+str(len(wins))+')':>20}  {'LOSERS ('+str(len(losses))+')':>20}")
    print(f"  Zone depth         {avg(wins, zone_depth):>20.1f}  {avg(losses, zone_depth):>20.1f}")
    print(f"  RSI at entry       {avg(wins, lambda t: t['rsi']):>20.1f}  {avg(losses, lambda t: t['rsi']):>20.1f}")
    print(f"  Spot vs VWAP (pt)  {avg(wins, spot_vwap_diff):>+20.0f}  {avg(losses, spot_vwap_diff):>+20.0f}")
    print(f"  Reentry→Entry (h)  {avg(wins, reentry_speed):>20.1f}  {avg(losses, reentry_speed):>20.1f}")

    aligned_wins = sum(1 for t in wins
                       if _side_aligned_with_spot_trend(t["side"],
                                                        _spot_at(spot_rows, t["entry_ts"]) or 0,
                                                        _vwap_at(spot_rows, t["entry_ts"].date(), t["entry_ts"]),
                                                        "")[0])
    aligned_losses = sum(1 for t in losses
                         if _side_aligned_with_spot_trend(t["side"],
                                                           _spot_at(spot_rows, t["entry_ts"]) or 0,
                                                           _vwap_at(spot_rows, t["entry_ts"].date(), t["entry_ts"]),
                                                           "")[0])
    print(f"  Spot-VWAP aligned  {aligned_wins:>20}/{len(wins)}  {aligned_losses:>20}/{len(losses)}")


if __name__ == "__main__":
    import os, sys
    sys.path.insert(0, ".")
    run_diagnostic()
