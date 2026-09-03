"""
scripts/nifty_sensex_intraday_cross_backtest.py — 2026-08-11.

Backtests IndexIntradayCrossTracker (strategies/d1_trap_option/
index_intraday_cross.py) for NIFTY and SENSEX. Weekly-expiry aware:
resolves expiry via REGISTRY.get_active_expiry_strict per day (never a
single fixed expiry across the window -- that exact bug already
contaminated earlier NIFTY/SENSEX results this session, see
project_sr_pingpong_live_2026_08_09 memory).

LIMITATION (real, severe, not a bug): a weekly contract only stays
resolvable back to the PREVIOUS weekly cycle before get_active_expiry_strict
correctly refuses to substitute an expired/delisted one -- confirmed
empirically ~7-10 calendar days, i.e. roughly one prior week's worth of
trading days. Entry-signal detection only, no exit rule specified yet --
this reports signals fired and a simple N-bar/EOD spot follow-through as a
directional-quality proxy, not a full P&L.

Cross-day zone carryover (2026-08-11 addition, MAX_ZONE_AGE_SWEEP): zones
are detected PER STRIKE across the whole available window, not reset each
day -- a zone only carries forward to a later day if that later day happens
to select the SAME ATM strike again (option premium decays day to day, so
carryover only makes sense same-strike, not same-underlying).

Usage:
    python3 scripts/nifty_sensex_intraday_cross_backtest.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.global_config import IST  # noqa: E402
from data_layer.client_db import ClientDB  # noqa: E402
from data_layer.instrument_registry import REGISTRY  # noqa: E402
from strategies.d1_trap_option.book import _fetch_bars, _upstox_key_for  # noqa: E402
from strategies.d1_trap_option.index_intraday_cross import (  # noqa: E402
    IndexIntradayCrossTracker, resample_bars, _Bar,
)

ATM_ROUND_STEP = {"NIFTY": 50, "SENSEX": 100}
STRICT_MAX_DAYS_OUT = 10   # weekly names -- tight bound, not the 35 used for monthly
NO_ENTRY_AFTER = time(14, 30)


def _to_bar_list(bars) -> List[_Bar]:
    return [_Bar(timestamp=b.timestamp, open=b.open, high=b.high, low=b.low, close=b.close) for b in bars]


async def backtest_underlying(underlying: str, token: str, max_zone_age_days: int = None) -> dict:
    await asyncio.to_thread(REGISTRY.load_sync, underlying, token)
    today = datetime.now(IST).date()

    spot_key = _upstox_key_for(underlying)
    spot_start = today - timedelta(days=20)
    spot_bars_raw = await asyncio.to_thread(_fetch_bars, spot_key, "1minute", spot_start, today, token)
    if not spot_bars_raw:
        return dict(underlying=underlying, error="no spot data")
    spot_bars = _to_bar_list(spot_bars_raw)
    trading_days = sorted({b.timestamp.date() for b in spot_bars})

    # Pass 1: figure out each day's ATM (and confirm its contract is still
    # resolvable) WITHOUT fetching option data yet, so we know the distinct
    # set of strikes to fetch multi-day history for.
    day_plan = {}   # date -> (expiry, atm)
    for day in trading_days:
        expiry = REGISTRY.get_active_expiry_strict(underlying, day, max_days_out=STRICT_MAX_DAYS_OUT)
        if expiry is None:
            continue
        day_spot_bars = [b for b in spot_bars if b.timestamp.date() == day]
        if not day_spot_bars:
            continue
        spot_open = day_spot_bars[0].open
        step = ATM_ROUND_STEP.get(underlying, 50)
        atm = int(round(spot_open / step) * step)
        day_plan[day] = (expiry, atm, spot_open)

    # Pass 2: fetch multi-day 1-min CE/PE bars ONCE per distinct (expiry, strike)
    # (purely to avoid re-fetching the same strike's history once per day it
    # recurs as ATM), resample to 15-min. ONE persistent tracker per strike
    # covering the whole window -- but zone DETECTION stays per-day (see Pass
    # 3): find_all_bear_zones is a same-day-only algorithm in practice. Fed a
    # concatenated multi-day series, its forward sweep/reclaim search runs
    # across day boundaries, which changes which candidate zones survive
    # `_is_mitigated_bear` (a same-day-valid reclaim gets invalidated by an
    # intervening close from a LATER day that a per-day run would never even
    # see) -- confirmed empirically: 08-03's 6 real same-day zones vanish
    # entirely when 08-03 is fed as part of a 08-01..08-11 concatenated
    # series. So detection must still run per-day, on that day's own bars
    # only; only the TRACKING/touch pool (self.zones) persists across days.
    distinct_strikes = sorted({(exp, atm) for exp, atm, _ in day_plan.values()})
    trackers: Dict[tuple, IndexIntradayCrossTracker] = {}
    ce_bars_by_strike: Dict[tuple, List[_Bar]] = {}
    pe_by_ts_by_strike: Dict[tuple, dict] = {}
    fetch_start = min(day_plan.keys()) if day_plan else today
    for expiry, atm in distinct_strikes:
        ce_key = REGISTRY.get_upstox_key(underlying, expiry, atm, "CE")
        pe_key = REGISTRY.get_upstox_key(underlying, expiry, atm, "PE")
        if not ce_key or not pe_key:
            continue
        ce_raw = await asyncio.to_thread(_fetch_bars, ce_key, "1minute", fetch_start, today, token)
        pe_raw = await asyncio.to_thread(_fetch_bars, pe_key, "1minute", fetch_start, today, token)
        if not ce_raw or not pe_raw:
            continue
        ce_15m = resample_bars(_to_bar_list(ce_raw), 15)
        pe_15m = resample_bars(_to_bar_list(pe_raw), 15)
        ce_bars_by_strike[(expiry, atm)] = ce_15m
        pe_by_ts_by_strike[(expiry, atm)] = {b.timestamp: b for b in pe_15m}
        trackers[(expiry, atm)] = IndexIntradayCrossTracker(
            no_entry_after=NO_ENTRY_AFTER, max_zone_age_days=max_zone_age_days)

    # Pass 3: walk chronologically. Each day, first seed that strike's tracker
    # with ONLY that day's own 15-min bars (matching the validated same-day
    # detection exactly -- known_ref_ts already dedupes ref candles already
    # seen on an earlier day for the same strike), THEN feed that day's bars
    # through the (possibly multi-day-old) persistent zone pool.
    all_signals = []
    all_zone_logs = []
    days_processed = []
    for day in sorted(day_plan.keys()):
        expiry, atm, spot_open = day_plan[day]
        key = (expiry, atm)
        if key not in trackers:
            continue
        tracker = trackers[key]
        day_spot_bars = [b for b in spot_bars if b.timestamp.date() == day]
        ce_15m_day = [b for b in ce_bars_by_strike[key] if b.timestamp.date() == day]
        pe_by_ts = pe_by_ts_by_strike[key]
        tracker.seed_zones(ce_15m_day)
        for bar in ce_15m_day:
            pe_bar = pe_by_ts.get(bar.timestamp)
            sig = tracker.on_ce_15m_bar(bar, pe_bar)
            if sig is not None:
                sig["underlying"] = underlying
                sig["date"] = str(day)
                sig["atm"] = atm
                sig["expiry"] = str(expiry)
                sig_ts = sig["entry_ts"]
                spot_at_sig = next((b.close for b in reversed(day_spot_bars) if b.timestamp <= sig_ts), None)
                spot_1h_later = next((b.close for b in day_spot_bars if b.timestamp >= sig_ts + timedelta(hours=1)), None)
                spot_eod = day_spot_bars[-1].close if day_spot_bars else None
                sig["spot_at_signal"] = spot_at_sig
                sig["spot_1h_later"] = spot_1h_later
                sig["spot_eod"] = spot_eod
                if spot_at_sig:
                    sig["spot_move_1h_pct"] = (round(100 * (spot_1h_later - spot_at_sig) / spot_at_sig, 3)
                                                if spot_1h_later else None)
                    sig["spot_move_eod_pct"] = (round(100 * (spot_eod - spot_at_sig) / spot_at_sig, 3)
                                                 if spot_eod else None)
                all_signals.append(sig)
        # Zones whose lock_ts falls on THIS day, for the per-day log view.
        day_zone_log = [z for z in tracker.zone_log() if z["zone_lock_ts"] and
                         datetime.fromisoformat(str(z["zone_lock_ts"])).date() == day]
        for z in day_zone_log:
            z["date"] = str(day)
        all_zone_logs.append(dict(date=str(day), atm=atm, expiry=str(expiry),
                                   spot_open=spot_open, zones=day_zone_log))
        days_processed.append(str(day))

    return dict(underlying=underlying, days_processed=days_processed, n_signals=len(all_signals),
                signals=all_signals, zone_logs=all_zone_logs,
                distinct_strikes=[f"{e}/{a}" for e, a in distinct_strikes])


async def main() -> int:
    db = ClientDB()
    token = db.get_feeder_creds_sync("upstox")["access_token"]

    ZONE_AGE_SWEEP = (0, 3, 7, 14)   # 0 = same-day only (old behavior)
    results = {}
    for underlying in ("NIFTY", "SENSEX"):
        print(f"\n{'='*70}\n{underlying}")
        for age in ZONE_AGE_SWEEP:
            tag = f"{underlying}_age{age}"
            r = await backtest_underlying(underlying, token, max_zone_age_days=age)
            results[tag] = r
            if r.get("error"):
                print(f"  age={age}: ERROR: {r['error']}")
                continue
            print(f"  age={age:>2}d: days={len(r['days_processed'])} signals={r['n_signals']} "
                  f"strikes={r.get('distinct_strikes')}")
            for sig in r["signals"]:
                move1h = sig.get("spot_move_1h_pct")
                moveeod = sig.get("spot_move_eod_pct")
                print(f"      {sig['date']} {sig['entry_ts']} ATM={sig['atm']} "
                      f"entry_premium={sig['entry_price']:.2f} zone=({sig['zone_lo']:.1f}-{sig['zone_hi']:.1f}) "
                      f"spot_move_1h={move1h}% spot_move_eod={moveeod}%")

    out_path = Path(__file__).resolve().parents[1] / "data" / "sweeps" / "nifty_sensex_intraday_cross_age_sweep.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    print(f"\n-> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
