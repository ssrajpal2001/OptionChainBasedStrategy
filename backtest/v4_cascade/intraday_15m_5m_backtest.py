"""backtest/v4_cascade/intraday_15m_5m_backtest.py -- EXPLORATORY variant
of multi_strike_option_premium_backtest.py testing the user's 2026-07-24
idea: instead of 75m HTF zone-discovery + 5m entry, use 15m as the SOLE
"HTF" zone-discovery timeframe (fed into PoolCascadeEngine.on_75m_bar --
that method has no hardcoded assumption about real-world 75-minute
spacing, it's purely a label; confirmed via direct grep of
pool_engine.py) + 5m entry, unchanged. No separate LTF step -- the user's
own framing names only two timeframes, and _open_position already falls
back cleanly to T2's target when no LTF has locked (ltf=None), so simply
never calling on_15m_bar produces exactly that degenerate case correctly.

This is a THROWAWAY comparison script, not a production change -- it
exists to answer "what would this idea's results look like" with real
data before deciding whether to actually build it into pool_engine.py/
book.py (which would need its own brainstorm/design/plan given it's a
real behavior change, not just a backtest parameter).

Usage:
    UPSTOX_TOKEN=<token> python backtest/v4_cascade/intraday_15m_5m_backtest.py \
        --start 2026-06-24 --end 2026-07-24 --dump-json results/intraday_15m5m_july.json
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
_HTF_MINUTES = 15  # the only change vs. multi_strike_option_premium_backtest.py: 15m HTF, not 75m


def _fetch_spot_1m(token: str, start: date, end: date) -> List[dict]:
    key = REGISTRY.get_upstox_index_key("NIFTY")
    range_rows = asyncio.run(fetch_upstox_range_1m(key, token, start, end - timedelta(days=1)))
    today_rows = asyncio.run(fetch_upstox_intraday_1m(key, token)) if end >= date.today() else []
    return _merge_rows(range_rows, today_rows)


def _daily_session_opens(spot_rows: List[dict]) -> Dict[date, float]:
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
    print(f"Active expiry: {expiry}  |  HTF timeframe under test: {_HTF_MINUTES}m (vs. production's 75m)")

    print(f"\nFetching NIFTY spot {start}..{end} to derive daily session-open ATM...")
    spot_rows = _fetch_spot_1m(token, start, end)
    opens = _daily_session_opens(spot_rows)
    trading_days = sorted(d for d in opens if start <= d <= end)
    print(f"Found {len(trading_days)} trading days: {trading_days[0] if trading_days else None} .. "
          f"{trading_days[-1] if trading_days else None}")

    day_candidates: Dict[date, Tuple[List[int], List[int]]] = {}
    for d in trading_days:
        day_candidates[d] = _resolve_candidate_strikes(opens[d])

    eng = PoolCascadeEngine(V4CascadeConfig(underlying="NIFTY", lot_multiplier=2, lot_size=75),
                             entry_offset=_ENTRY_OFFSET, session_open=_SESSION_OPEN)
    all_events: List[dict] = []
    bars_5m: Dict[Tuple[str, int], List] = {}
    prev_candidates: Dict[str, set] = {"CE": set(), "PE": set()}
    fetched_cache: Dict[Tuple[str, int], List["_Bar"]] = {}

    def _fetch_and_cache(side: str, strike: int) -> List["_Bar"]:
        cache_key = (side, strike)
        if cache_key in fetched_cache:
            return fetched_cache[cache_key]
        key = REGISTRY.get_upstox_key("NIFTY", expiry, strike, side)
        rows = _fetch_option_1m(token, key, start, end)
        b5 = _to_5m_bars(rows, filter_zero_volume=True)
        fetched_cache[cache_key] = b5
        return b5

    for d in trading_days:
        ce_strikes, pe_strikes = day_candidates[d]
        for side, today_set in (("CE", set(ce_strikes)), ("PE", set(pe_strikes))):
            for strike in prev_candidates[side] - today_set:
                eng.reset_candidate(side, strike)
            prev_candidates[side] = today_set

            for strike in sorted(today_set):
                all_bars = _fetch_and_cache(side, strike)
                day_bars = [b for b in all_bars if b.timestamp.date() == d]
                key = (side, strike)
                for bar in day_bars:
                    bars_5m.setdefault(key, []).append(bar)
                    events = eng.on_5m_bar(side, strike, bar)
                    for ev in events:
                        all_events.append({
                            "ts": bar.timestamp, "side": side, "strike": strike,
                            "event": ev.event_type.value, "tranche": ev.tranche, "reason": ev.reason,
                            "price": ev.price_hint, "sl": ev.sl_price, "target": ev.target_price,
                            "execution_strike": ev.execution_strike,
                        })
                    # ONLY difference from the 75m production script: HTF bars
                    # come from a _HTF_MINUTES (15m) resample instead of 75m.
                    # No on_15m_bar call at all -- see module docstring.
                    if _bucket_end(bar.timestamp, _HTF_MINUTES, _SESSION_OPEN):
                        rhtf = resample_bars(bars_5m[key], _HTF_MINUTES, _SESSION_OPEN)
                        if rhtf:
                            last = rhtf[-1]
                            eng.on_75m_bar(side, strike, _Bar(last.timestamp, last.close, last.high,
                                                               last.low, last.close, tf=_HTF_MINUTES))

    print(f"\n{'='*110}\nTRADE / EVENT LOG ({len(all_events)} events) -- {_HTF_MINUTES}m HTF / 5m entry\n{'='*110}")
    header = (f"{'ts':<20} {'side':<5} {'strike':>7} {'event':<16} {'tranche':<4} "
              f"{'reason':<28} {'price':>8} {'sl':>8} {'target':>8}")
    print(header)
    print("-" * len(header))
    for e in all_events:
        print(f"{e['ts'].strftime('%Y-%m-%d %H:%M'):<20} {e['side']:<5} {e['strike']:>7} "
              f"{e['event']:<16} {e['tranche'] or '-':<4} {e['reason']:<28} "
              f"{e['price'] or 0:>8.2f} {e['sl'] or 0:>8.2f} {e['target'] or 0:>8.2f}")

    if dump_json:
        out = {
            "start": str(start), "end": str(end), "expiry": str(expiry), "htf_minutes": _HTF_MINUTES,
            "events": [{"ts": e["ts"].isoformat(), "side": e["side"], "strike": e["strike"],
                        "event": e["event"], "tranche": e["tranche"], "reason": e["reason"],
                        "price": e["price"], "sl": e["sl"], "target": e["target"],
                        "execution_strike": e["execution_strike"]} for e in all_events],
        }
        os.makedirs(os.path.dirname(dump_json) or ".", exist_ok=True)
        with open(dump_json, "w") as f:
            json.dump(out, f, indent=2)
        print(f"\nWrote event ledger to {dump_json} ({len(all_events)} events)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, default="2026-06-24")
    ap.add_argument("--end", type=str, default="2026-07-24")
    ap.add_argument("--dump-json", type=str, default=None)
    args = ap.parse_args()
    run(date.fromisoformat(args.start), date.fromisoformat(args.end), dump_json=args.dump_json)
