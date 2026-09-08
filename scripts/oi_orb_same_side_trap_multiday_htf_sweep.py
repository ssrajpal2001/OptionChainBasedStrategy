"""
scripts/oi_orb_same_side_trap_multiday_htf_sweep.py -- 2026-09-08, direct
user follow-up: the same-side trap + S1/R1 sweep (oi_orb_trap_target_full_
htf_ltf_sweep.py) only ever fed the zone detector TODAY's own single-day
1-min bars (a deliberate 2026-09-06 correction, made because coarser HTFs
had almost no real candles to form a zone on within one ~6.25h session).
Direct user instruction: feed the zone detector >=3 real prior trading
days of history so genuinely coarse HTFs (Daily, 4H, 2H, 1H, 75min) get a
fair test -- this is an intentional reversal of that 2026-09-06 restriction,
not a re-litigation of it.

Fallback layering (direct user spec): if no HTF-based zone ever locks and
gets touched for a given trade, fall back to the ALREADY-VALIDATED intraday
zone result (HTF=15min/LTF=3min, single-day bars, oi_orb_trap_target_full_
htf_ltf_sweep.py's own trap_target_exit) for that trade -- never worse off
than the existing baseline, only ever a potential improvement.

CRITICAL: strategies.core.candle_indicators.to_n_min_bars is explicitly
NOT date-aware ("Non-date-aware (hour, floored-minute) bucketing -- correct
for a single intraday session") -- using it directly on a multi-day bar
list would silently merge same-hour bars from DIFFERENT days into one
bucket. A fresh, date-aware bucketer (_to_n_min_bars_dateaware) is used
here instead for every HTF option, including "Daily" (one real bar per
real trading day).

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_same_side_trap_multiday_htf_sweep.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List, Optional

sys.path.insert(0, ".")

from strategies.core.trap_zone_utils import Bar
from strategies.core.support_resistance import SupportResistanceCalculator
from scripts.oi_orb_entry_mode_backtest import ROWS, SIDE, resolve_eq_key, to_bars, volume_by_ts, compute_orb
from scripts.oi_orb_trap_target_full_htf_ltf_sweep import find_entry, trap_target_exit
from scripts.oi_orb_atr_chandelier_backtest import fetch_all as fetch_all_singleday
from strategies.oi_orb_screener import screener
from data_layer.historical_candles import fetch_upstox_range_1m

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
LOOKBACK_CALENDAR_DAYS = 15   # ~10 real trading days -- gives "Daily" HTF a fair number of bars,
                              # well above the user's own >=3-trading-day floor.

HTF_OPTIONS = [("1D", "daily"), ("4h", 240), ("2h", 120), ("1h", 60), ("75min", 75)]
LTF_OPTIONS = [5, 3, 2, 1]

SAME_HTF_MIN_FALLBACK = 15
SAME_LTF_MIN_FALLBACK = 3


def _to_n_min_bars_dateaware(bars: List[Bar], n: int) -> List[Bar]:
    """Date-aware version of candle_indicators.to_n_min_bars -- buckets by
    (calendar date, minutes-since-midnight // n), so bars from different
    real trading days never collapse into the same bucket the way the
    hour-only bucketing in the shared helper would for a multi-day series."""
    buckets: Dict[tuple, list] = {}
    for b in bars:
        mins = b.ts.hour * 60 + b.ts.minute
        key = (b.ts.date(), mins // n)
        buckets.setdefault(key, []).append(b)
    out = []
    for key in sorted(buckets.keys()):
        g = sorted(buckets[key], key=lambda x: x.ts)
        out.append(Bar(ts=g[0].ts, open=g[0].open, high=max(x.high for x in g),
                        low=min(x.low for x in g), close=g[-1].close))
    return out


def _to_daily_bars(bars: List[Bar]) -> List[Bar]:
    by_day: Dict[date, list] = {}
    for b in bars:
        by_day.setdefault(b.ts.date(), []).append(b)
    out = []
    for day in sorted(by_day):
        g = sorted(by_day[day], key=lambda x: x.ts)
        out.append(Bar(ts=g[0].ts, open=g[0].open, high=max(x.high for x in g),
                        low=min(x.low for x in g), close=g[-1].close))
    return out


def _prev_weekday(d: date) -> date:
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    return prev


@dataclass
class Diag:
    date: str
    symbol: str
    side: str
    entry_ts: object
    entry_price: float
    htf_label: str
    ltf_min: int
    used_htf_trades_back: int
    zone_source: str   # "multiday_htf" | "intraday_fallback"
    zone_lo: Optional[float] = None
    zone_hi: Optional[float] = None
    zone_lock_ts: object = None
    zone_touch_ts: object = None
    level_name: str = ""
    level_price: Optional[float] = None
    level_established_ts: object = None
    exit_ts: object = None
    exit_price: Optional[float] = None
    reason: str = ""

    @property
    def points(self):
        if self.entry_price is None or self.exit_price is None:
            return None
        raw = self.exit_price - self.entry_price
        return raw if self.side == "CALL" else -raw


def trap_target_exit_diag_multiday(entry_ts, entry_price, side, bars_1m_today, htf_bars_multiday, ltf_bars_today,
                                    ltf_min) -> Diag:
    """Same mechanic as trap_target_exit's own inner logic, instrumented for
    diagnostics AND fed a multi-day htf_bars series -- LTF (the S&R ladder
    that reads the actual exit) still only ever needs TODAY's own bars once
    the zone has been touched, matching the original design (the ladder
    starts fresh from the touch instant, not from multiple days back)."""
    zones_fn = screener.bull_trap_zones if side == "CALL" else screener.sharp_bear_zones
    zones = zones_fn(htf_bars_multiday)
    post_entry_1m = [b for b in bars_1m_today if b.ts >= entry_ts]

    diag = Diag(date=None, symbol=None, side=side, entry_ts=entry_ts, entry_price=entry_price,
                htf_label="", ltf_min=ltf_min, used_htf_trades_back=0, zone_source="multiday_htf")

    zone_touched_ts = None
    calc = None
    ltf_fed = 0

    for b in post_entry_1m:
        if zone_touched_ts is None:
            # Direct user correction: only ever check the MOST RECENTLY locked
            # zone as of this bar, not every zone confirmed anywhere across the
            # whole multi-day lookback -- a stale zone from days ago shouldn't
            # win a touch race against a fresher, more relevant one just because
            # it happens to be earlier in the list.
            locked_so_far = [z for z in zones if z["lock_ts"] is not None and z["lock_ts"] <= b.ts]
            if locked_so_far:
                z = max(locked_so_far, key=lambda zz: zz["lock_ts"])
                touched = (b.low <= z["zone_hi"]) and (b.high >= z["zone_lo"])
                if touched:
                    zone_touched_ts = b.ts
                    calc = SupportResistanceCalculator()
                    ltf_fed = 0
                    diag.zone_lo, diag.zone_hi = z["zone_lo"], z["zone_hi"]
                    diag.zone_lock_ts = z["lock_ts"]
                    diag.zone_touch_ts = b.ts

        if calc is not None:
            avail_ltf = [x for x in ltf_bars_today if entry_ts <= x.ts <= b.ts]
            for nb in avail_ltf[ltf_fed:]:
                calc.process_straddle_candle("SYM", {"timestamp": nb.ts, "high": nb.high,
                                                       "low": nb.low, "duration": ltf_min})
            ltf_fed = len(avail_ltf)
            sr = calc.get_calculated_sr_state("SYM").get("sr_levels", {})
            level = sr.get("S1") if side == "CALL" else sr.get("R1")
            if level is not None and level.get("is_established"):
                if diag.level_established_ts is None:
                    diag.level_name = "S1" if side == "CALL" else "R1"
                    diag.level_established_ts = b.ts
                lvl = level["low"] if side == "CALL" else level["high"]
                diag.level_price = lvl
                breach = (b.low <= lvl) if side == "CALL" else (b.high >= lvl)
                if breach:
                    diag.exit_ts, diag.exit_price, diag.reason = b.ts, lvl, "trap_target_hit"
                    return diag

    # No multi-day-HTF zone ever touched (or touched but never established+breached) -- signal "no fire"
    diag.exit_ts = None
    return diag


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching single-day cache (for entries + intraday fallback)...")
    singleday_cache = await fetch_all_singleday()

    entries = {}
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = singleday_cache.get((trade_date, symbol))
        if cached is None:
            entries[(trade_date, symbol)] = None
            continue
        bars_1m, vol_by_ts, orb_h, orb_l = cached
        entries[(trade_date, symbol)] = find_entry(bars_1m, side, orb_h, orb_l, vol_by_ts)

    print("Fetching multi-day history (>=3 trading days back, real Upstox 1-min NSE_EQ)...")
    multiday_bars: Dict[tuple, List[Bar]] = {}
    fetch_status = {}
    for trade_date, symbol, side_bias in ROWS:
        key = (trade_date, symbol)
        if key in multiday_bars:
            continue
        eq_key = resolve_eq_key(symbol)
        if eq_key is None:
            multiday_bars[key] = None
            fetch_status[key] = "no_eq_key"
            continue
        d = date.fromisoformat(trade_date)
        start = d - timedelta(days=LOOKBACK_CALENDAR_DAYS)
        rows = await fetch_upstox_range_1m(eq_key, TOKEN, start, d)
        if not rows:
            multiday_bars[key] = None
            fetch_status[key] = "no_data"
            continue
        bars = to_bars(rows)
        days_present = len({b.ts.date() for b in bars if b.ts.date() <= d})
        multiday_bars[key] = bars
        fetch_status[key] = f"ok_{days_present}_days"

    n_ok = sum(1 for v in multiday_bars.values() if v)
    n_short = sum(1 for v in fetch_status.values() if v.startswith("ok_") and int(v.split("_")[1]) < 4)
    print(f"Multi-day fetch: {n_ok}/{len(multiday_bars)} usable, {n_short} came back with <4 real trading days "
          f"(thin history -- flagged, not silently trusted).")

    # ---- Sweep HTF x LTF, multi-day zones, fallback to intraday 15min/3min ----
    all_results = {}
    fallback_used_by_combo = {}
    for htf_label, htf_spec in HTF_OPTIONS:
        for ltf_min in LTF_OPTIONS:
            diags = []
            fired_multiday = 0
            fell_back = 0
            for trade_date, symbol, side_bias in ROWS:
                side = SIDE[side_bias]
                cached = singleday_cache.get((trade_date, symbol))
                entry = entries.get((trade_date, symbol))
                mbars = multiday_bars.get((trade_date, symbol))
                if cached is None or entry is None:
                    continue
                bars_1m_today, vol_by_ts, orb_h, orb_l = cached
                entry_ts, entry_price = entry
                ltf_bars_today = _to_n_min_bars_dateaware(bars_1m_today, ltf_min)

                d = None
                htf_bars_multiday = []
                if mbars:
                    d = date.fromisoformat(trade_date)
                    hist = [b for b in mbars if b.ts.date() <= d]
                    htf_bars_multiday = (_to_daily_bars(hist) if htf_spec == "daily"
                                          else _to_n_min_bars_dateaware(hist, htf_spec))

                diag = None
                if len(htf_bars_multiday) >= 3:
                    diag = trap_target_exit_diag_multiday(entry_ts, entry_price, side, bars_1m_today,
                                                           htf_bars_multiday, ltf_bars_today, ltf_min)
                if diag is None or diag.exit_ts is None:
                    # Fall back to the already-validated intraday result (15min/3min, single-day bars).
                    htf_bars_intraday = _to_n_min_bars_dateaware(bars_1m_today, SAME_HTF_MIN_FALLBACK)
                    ltf_bars_intraday = _to_n_min_bars_dateaware(bars_1m_today, SAME_LTF_MIN_FALLBACK)
                    exit_ts, exit_price, reason = trap_target_exit(
                        entry_ts, entry_price, side, bars_1m_today, htf_bars_intraday, ltf_bars_intraday,
                        SAME_LTF_MIN_FALLBACK)
                    diag = Diag(date=trade_date, symbol=symbol, side=side, entry_ts=entry_ts,
                                entry_price=entry_price, htf_label=htf_label, ltf_min=ltf_min,
                                used_htf_trades_back=0, zone_source="intraday_fallback",
                                exit_ts=exit_ts, exit_price=exit_price, reason=reason)
                    fell_back += 1
                else:
                    diag.date, diag.symbol, diag.htf_label = trade_date, symbol, htf_label
                    fired_multiday += 1
                diags.append(diag)

            entered = [dg for dg in diags if dg.points is not None]
            wins = [dg.points for dg in entered if dg.points > 0]
            losses = [dg.points for dg in entered if dg.points <= 0]
            total = sum(dg.points for dg in entered)
            loss_sum = sum(losses)
            pf = (sum(wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
            win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
            key = f"{htf_label}_{ltf_min}m"
            all_results[key] = {
                "htf": htf_label, "ltf": ltf_min, "entered": len(entered), "win_pct": win_pct,
                "pf": (pf if pf != float("inf") else 9999.0), "total": total,
                "fired_multiday": fired_multiday, "fell_back": fell_back,
                "trades": [
                    {"date": dg.date, "symbol": dg.symbol, "side": dg.side,
                     "entry_ts": dg.entry_ts.strftime("%H:%M"), "entry_price": dg.entry_price,
                     "zone_source": dg.zone_source,
                     "zone_lo": dg.zone_lo, "zone_hi": dg.zone_hi,
                     "zone_lock_ts": dg.zone_lock_ts.strftime("%Y-%m-%d %H:%M") if dg.zone_lock_ts else None,
                     "zone_touch_ts": dg.zone_touch_ts.strftime("%H:%M") if dg.zone_touch_ts else None,
                     "level_name": dg.level_name, "level_price": dg.level_price,
                     "level_established_ts": dg.level_established_ts.strftime("%H:%M") if dg.level_established_ts else None,
                     "exit_ts": dg.exit_ts.strftime("%H:%M") if dg.exit_ts else None,
                     "exit_price": dg.exit_price, "reason": dg.reason, "points": dg.points}
                    for dg in sorted(diags, key=lambda x: (x.date, x.symbol))
                ],
            }
            print(f"  {key}: entered={len(entered)} win%={win_pct:.1f} PF={all_results[key]['pf']:.2f} "
                  f"total={total:+.2f}  (fired via multi-day HTF: {fired_multiday}, fell back to intraday: {fell_back})")

    with open("data/oi_orb_same_side_trap_multiday_htf_report.json", "w") as f:
        json.dump(all_results, f)
    print("\nWrote data/oi_orb_same_side_trap_multiday_htf_report.json")


if __name__ == "__main__":
    asyncio.run(main())
