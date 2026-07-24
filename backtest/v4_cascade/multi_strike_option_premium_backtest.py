"""backtest/v4_cascade/multi_strike_option_premium_backtest.py -- real
option PREMIUM backtest for the CURRENT active NIFTY weekly expiry,
replaying PoolCascadeEngine day-by-day exactly as book.py's multi-strike
scanning would run it live (2026-07-24 plan): each day, resolve that day's
session-open ATM -> 5 CE + 5 PE candidate strikes (ATM-100/-200/-300/-400/
-500 and ATM+100/+200/+300/+400/+500, matching
strategies/v4_cascade/book.py's _build_candidate_strikes exactly, 100-pt
grid) -> fetch each candidate's own 1m premium -> feed into ONE persistent
PoolCascadeEngine, keyed (side, strike) per Task 1's composite-pool design
-- resetting a candidate's pool only when it actually leaves the tracked
window (mirrors book.py's _recenter_multi_strike diff logic), not on every
day's re-resolution.

Adapted from the single-strike predecessor (july_option_premium_backtest.py)
to exercise the new multi-strike candidate scanning end-to-end against
real historical data, not synthetic test bars.

DATA CONSTRAINT (same as the single-strike script): Upstox's
InstrumentRegistry only resolves instrument_keys for CURRENTLY ACTIVE
(unexpired) contracts, so this can only backtest whatever real trading
history exists for the CURRENT weekly expiry, not arbitrary past weeks
whose contracts have already expired. The actual available window is
printed at the top of the run, not assumed -- for a weekly-expiry
instrument like NIFTY, a "full month" backtest is naturally bounded by
however far back the CURRENT contract's own trading history goes (usually
1-2 weeks), not a full calendar month.

Usage:
    UPSTOX_TOKEN=<token> python backtest/v4_cascade/multi_strike_option_premium_backtest.py \
        --start 2026-06-24 --end 2026-07-24 --dump-json results/multi_strike_july.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer.historical_candles import fetch_upstox_intraday_1m, fetch_upstox_range_1m
from data_layer.instrument_registry import REGISTRY
from strategies.v4_cascade.book import _Bar, _bucket_end, _merge_rows, _to_5m_bars
from strategies.v4_cascade.config import V4CascadeConfig
from strategies.v4_cascade.pool_engine import PoolCascadeEngine
from strategies.v4_cascade.rolling_base import resample_bars

_TRACKING_STRIKE_STEP = 100.0
_TRACKING_OFFSETS_PTS = [100.0, 200.0, 300.0, 400.0, 500.0]
_SESSION_OPEN = (9, 15)
_ENTRY_OFFSET = 5.0


def _fetch_spot_1m(token: str, start: date, end: date) -> List[dict]:
    key = REGISTRY.get_upstox_index_key("NIFTY")
    range_rows = asyncio.run(fetch_upstox_range_1m(key, token, start, end - timedelta(days=1)))
    today_rows = asyncio.run(fetch_upstox_intraday_1m(key, token)) if end >= date.today() else []
    return _merge_rows(range_rows, today_rows)


def _daily_session_opens(spot_rows: List[dict]) -> Dict[date, float]:
    """First real tick at/after 09:15 IST for each trading day present in
    the fetched spot data."""
    by_day: Dict[date, List[dict]] = {}
    for r in spot_rows:
        ts = datetime.fromisoformat(r["ts"])
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=IST)
        by_day.setdefault(ts.date(), []).append((ts, r["open"]))
    opens: Dict[date, float] = {}
    for day, rows in by_day.items():
        rows.sort(key=lambda x: x[0])
        opens[day] = rows[0][1]
    return opens


def _resolve_candidate_strikes(atm_open: float) -> Tuple[List[int], List[int]]:
    """Matches strategies/v4_cascade/book.py's _build_candidate_strikes
    exactly: CE = atm - offset for each offset, PE = atm + offset."""
    atm = round(atm_open / _TRACKING_STRIKE_STEP) * _TRACKING_STRIKE_STEP
    ce_strikes = [int(atm - off) for off in _TRACKING_OFFSETS_PTS]
    pe_strikes = [int(atm + off) for off in _TRACKING_OFFSETS_PTS]
    return ce_strikes, pe_strikes


def _fetch_option_1m(token: str, key: str, start: date, end: date) -> List[dict]:
    if not key:
        return []
    range_rows = asyncio.run(fetch_upstox_range_1m(key, token, start, end - timedelta(days=1)))
    today_rows = asyncio.run(fetch_upstox_intraday_1m(key, token)) if end >= date.today() else []
    return _merge_rows(range_rows, today_rows)


def run(start: date, end: date, dump_json: Optional[str] = None) -> None:
    token = os.environ["UPSTOX_TOKEN"]
    REGISTRY.load_sync("NIFTY", token)
    expiry = REGISTRY.get_active_expiry("NIFTY")
    print(f"Active expiry: {expiry}  (all loaded: {REGISTRY.all_expiries('NIFTY')})")

    print(f"\nFetching NIFTY spot {start}..{end} to derive daily session-open ATM...")
    spot_rows = _fetch_spot_1m(token, start, end)
    opens = _daily_session_opens(spot_rows)
    trading_days = sorted(d for d in opens if start <= d <= end)
    print(f"Found {len(trading_days)} trading days with spot data: "
          f"{trading_days[0] if trading_days else None} .. {trading_days[-1] if trading_days else None}")

    # Resolve each day's 5 CE + 5 PE candidate strikes.
    day_candidates: Dict[date, Tuple[List[int], List[int]]] = {}
    for d in trading_days:
        day_candidates[d] = _resolve_candidate_strikes(opens[d])
        ce, pe = day_candidates[d]
        print(f"  {d}  spot_open={opens[d]:.2f}  CE={ce}  PE={pe}")

    all_strikes_seen: Dict[str, set] = {"CE": set(), "PE": set()}
    for d in trading_days:
        ce, pe = day_candidates[d]
        all_strikes_seen["CE"].update(ce)
        all_strikes_seen["PE"].update(pe)
    for side in ("CE", "PE"):
        print(f"\n{side} strikes used across the whole window: {sorted(all_strikes_seen[side])}")

    eng = PoolCascadeEngine(V4CascadeConfig(underlying="NIFTY", lot_multiplier=2, lot_size=75),
                             entry_offset=_ENTRY_OFFSET, session_open=_SESSION_OPEN)
    all_events: List[dict] = []
    # Per-(side,strike) 5m bar accumulator -- mirrors book.py's
    # self._pool_bars_5m, needed for this candidate's own 15m/75m resample.
    bars_5m: Dict[Tuple[str, int], List] = {}
    prev_candidates: Dict[str, set] = {"CE": set(), "PE": set()}
    fetched_cache: Dict[Tuple[str, int], List["_Bar"]] = {}

    # (side, strike, ref_ts_iso) -> zone record, for the full multi-strike
    # ledger dump -- matches EXACTLY what the day-hopping multi-candidate
    # engine (with per-candidate resets on window exit) actually tracked.
    zones: Dict[Tuple[str, int, str], dict] = {}

    def _snapshot_zones(side: str, strike: int, at_ts: datetime) -> None:
        for slot in eng._pool.get((side, strike), []):
            key = (side, strike, slot.zone.reference_low_ts.isoformat())
            if key not in zones:
                zones[key] = {
                    "side": side, "strike": strike,
                    "ref_ts": slot.zone.reference_low_ts.isoformat(),
                    "discovered_at": at_ts.isoformat(),
                    "ref_low": slot.zone.entry_line, "ref_high": slot.zone.sl_level,
                    "sweep_low": slot.zone.sweep_low,
                    "lock_ts": slot.zone.lock_ts.isoformat() if slot.zone.lock_ts else None,
                    "zone_low": round(slot.zone_low, 2), "zone_high": round(slot.zone_high, 2),
                    "reentry_ts": None, "pending_entry": False, "trigger_ts": None,
                    "removed_at": None, "removed_reason": None,
                }
            z = zones[key]
            if slot.reentry_ts and not z["reentry_ts"]:
                z["reentry_ts"] = slot.reentry_ts.isoformat()
            if slot.pending_entry and not z["pending_entry"]:
                z["pending_entry"] = True
                z["trigger_ts"] = slot.trigger_ts.isoformat() if slot.trigger_ts else None

    def _mark_removed(side: str, strike: int, at_ts: datetime, reason: str) -> None:
        for (s, k, ref) in list(zones):
            if s == side and k == strike and zones[(s, k, ref)]["removed_at"] is None:
                zones[(s, k, ref)]["removed_at"] = at_ts.isoformat()
                zones[(s, k, ref)]["removed_reason"] = reason

    def _fetch_and_cache(side: str, strike: int) -> List["_Bar"]:
        cache_key = (side, strike)
        if cache_key in fetched_cache:
            return fetched_cache[cache_key]
        key = REGISTRY.get_upstox_key("NIFTY", expiry, strike, side)
        rows = _fetch_option_1m(token, key, start, end)
        b5 = _to_5m_bars(rows, filter_zero_volume=True)
        fetched_cache[cache_key] = b5
        real_start = b5[0].timestamp.date() if b5 else None
        real_end = b5[-1].timestamp.date() if b5 else None
        print(f"  fetched {side} {strike} (key={key or 'UNRESOLVED'}): {len(b5)} 5m bars, "
              f"real data range {real_start}..{real_end}")
        return b5

    for d in trading_days:
        ce_strikes, pe_strikes = day_candidates[d]
        day_start_ts = datetime.combine(d, datetime.min.time()).replace(
            hour=_SESSION_OPEN[0], minute=_SESSION_OPEN[1], tzinfo=IST)

        for side, today_set in (("CE", set(ce_strikes)), ("PE", set(pe_strikes))):
            left_window = prev_candidates[side] - today_set
            for strike in left_window:
                print(f"  [{d}] {side} {strike} left the tracked window -- resetting candidate")
                _mark_removed(side, strike, day_start_ts, "strike_left_window")
                eng.reset_candidate(side, strike)
            prev_candidates[side] = today_set

            for strike in sorted(today_set):
                all_bars = _fetch_and_cache(side, strike)
                day_bars = [b for b in all_bars if b.timestamp.date() == d]
                key = (side, strike)
                for bar in day_bars:
                    bars_5m.setdefault(key, []).append(bar)
                    prior_refs = {s.zone.reference_low_ts for s in eng._pool.get(key, [])}
                    events = eng.on_5m_bar(side, strike, bar)
                    for ev in events:
                        all_events.append({
                            "ts": bar.timestamp, "side": side, "strike": strike,
                            "event": ev.event_type.value,
                            "tranche": ev.tranche, "reason": ev.reason, "price": ev.price_hint,
                            "sl": ev.sl_price, "target": ev.target_price,
                            "execution_strike": ev.execution_strike, "audit": ev.audit,
                        })
                    after_refs = {s.zone.reference_low_ts for s in eng._pool.get(key, [])}
                    fired_here = any(ev.event_type.value == f"open_long_{side.lower()}" for ev in events)
                    for ref in prior_refs - after_refs:
                        zkey = (side, strike, ref.isoformat())
                        if zkey in zones and zones[zkey]["removed_at"] is None:
                            zones[zkey]["removed_at"] = bar.timestamp.isoformat()
                            zones[zkey]["removed_reason"] = "pool_cleared_on_fire" if fired_here else "broken_or_aged"
                    if _bucket_end(bar.timestamp, 15, _SESSION_OPEN):
                        r15 = resample_bars(bars_5m[key], 15, _SESSION_OPEN)
                        if r15:
                            last15 = r15[-1]
                            eng.on_15m_bar(side, strike, _Bar(last15.timestamp, last15.close, last15.high,
                                                               last15.low, last15.close, tf=15))
                    if _bucket_end(bar.timestamp, 75, _SESSION_OPEN):
                        r75 = resample_bars(bars_5m[key], 75, _SESSION_OPEN)
                        if r75:
                            last75 = r75[-1]
                            eng.on_75m_bar(side, strike, _Bar(last75.timestamp, last75.close, last75.high,
                                                               last75.low, last75.close, tf=75))
                            _snapshot_zones(side, strike, bar.timestamp)

    print(f"\n{'='*110}\nTRADE / EVENT LOG ({len(all_events)} events)\n{'='*110}")
    header = (f"{'ts':<20} {'side':<5} {'strike':>7} {'event':<16} {'tranche':<4} "
              f"{'reason':<28} {'price':>8} {'sl':>8} {'target':>8}")
    print(header)
    print("-" * len(header))
    for e in all_events:
        print(f"{e['ts'].strftime('%Y-%m-%d %H:%M'):<20} {e['side']:<5} {e['strike']:>7} "
              f"{e['event']:<16} {e['tranche'] or '-':<4} {e['reason']:<28} "
              f"{e['price'] or 0:>8.2f} {e['sl'] or 0:>8.2f} {e['target'] or 0:>8.2f}")

    print(f"\n{'='*110}\nOPEN EVENTS -- FULL AUDIT TRAIL (for chart cross-check)\n{'='*110}")
    for e in all_events:
        if not e["event"].startswith("open_long"):
            continue
        a = e["audit"] or {}
        print(f"\n{e['side']} {e['strike']} entry at {e['ts']} price={e['price']:.2f} "
              f"sl={e['sl']:.2f} target={e['target']:.2f} "
              f"(execution_strike on event={e['execution_strike']})")
        print(f"  75m ref/lock:   {a.get('htf_ref_ts')}  (lock={a.get('htf_lock_ts')})")
        print(f"  re-entry:       {a.get('reentry_ts')}")
        print(f"  15m ref:        {a.get('ltf_ref_ts')}  (found_at_fill={a.get('ltf_found_at_fill')})")
        print(f"  5m trigger:     {a.get('trigger_ts')}")
        print(f"  zone:           [{a.get('zone_low')}, {a.get('zone_high')}]")
        print(f"  t1_target/t2_target: {a.get('t1_target')} / {a.get('t2_target')}")

    n_zones_by_strike: Dict[Tuple[str, int], int] = {}
    for (s, k, _ref) in zones:
        n_zones_by_strike[(s, k)] = n_zones_by_strike.get((s, k), 0) + 1
    print(f"\n{'='*110}\nZONE COUNT PER CANDIDATE (the whole point of multi-strike: which strikes\n"
          f"actually found structure vs. which found nothing)\n{'='*110}")
    for side in ("CE", "PE"):
        for strike in sorted({k for (s, k) in n_zones_by_strike if s == side}):
            print(f"  {side} {strike}: {n_zones_by_strike[(side, strike)]} zones")

    if dump_json:
        out = {
            "start": str(start), "end": str(end), "expiry": str(expiry),
            "day_candidates": {str(d): {"spot_open": opens[d], "ce": day_candidates[d][0],
                                         "pe": day_candidates[d][1]} for d in trading_days},
            "zones": [dict(v, ref_ts=v["ref_ts"]) for v in zones.values()],
            "events": [{"ts": e["ts"].isoformat(), "side": e["side"], "strike": e["strike"],
                        "event": e["event"], "tranche": e["tranche"], "reason": e["reason"],
                        "price": e["price"], "sl": e["sl"], "target": e["target"],
                        "execution_strike": e["execution_strike"]} for e in all_events],
        }
        os.makedirs(os.path.dirname(dump_json) or ".", exist_ok=True)
        with open(dump_json, "w") as f:
            json.dump(out, f, indent=2)
        print(f"\nWrote full zone/event ledger to {dump_json} ({len(zones)} zones, {len(all_events)} events)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, default="2026-06-24")
    ap.add_argument("--end", type=str, default="2026-07-24")
    ap.add_argument("--dump-json", type=str, default=None)
    args = ap.parse_args()
    run(date.fromisoformat(args.start), date.fromisoformat(args.end), dump_json=args.dump_json)
