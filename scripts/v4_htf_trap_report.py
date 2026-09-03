"""
scripts/v4_htf_trap_report.py — standalone HTF trap-zone verification report.

Fetches real NIFTY spot 1-minute historical candles from Upstox (using the
access token stored in data/clients.db), resamples to 5-minute bars, and runs
the SAME multiplier-ladder 3-candle sweep+reclaim scanner used by the V4
Premium Trap Cascade Engine (strategies/v4_cascade/rolling_base.py +
zone_state.py) over the whole range. Prints every zone it locks (bear=floor
sweep/reclaim, bull=structural-high sweep/reclaim) with the exact bars and
price levels involved, so it can be checked by eye against a real chart for
the same date range.

This exercises ONLY the pure HTF trap-detection core — no option premiums,
no order routing, no bus/broker — by design, so the detector itself can be
validated in isolation first.

Usage:
    python scripts/v4_htf_trap_report.py --days 10
    python scripts/v4_htf_trap_report.py --start 2026-07-01 --end 2026-07-18
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List

import pandas as pd

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer.client_db import ClientDB
from data_layer.historical_candles import _http_get_json, _parse_candles
from data_layer.instrument_registry import REGISTRY
from strategies.v4_cascade.rolling_base import build_ladder, scan_ladder, LadderMatch
from strategies.v4_cascade.zone_state import _LADDER_STEP, _MAX_LADDER_MINUTES


@dataclass(frozen=True)
class Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int = 0


def _fetch_day_1m(instrument_key: str, access_token: str, day: date) -> List[dict]:
    from urllib.parse import quote as _q
    url = (f"https://api.upstox.com/v2/historical-candle/{_q(instrument_key, safe='')}/1minute/"
           f"{day.isoformat()}/{day.isoformat()}")
    return _parse_candles(_http_get_json(url, access_token))


def fetch_range_1m(instrument_key: str, access_token: str, start: date, end: date) -> pd.DataFrame:
    rows: List[dict] = []
    d = start
    while d <= end:
        if d.weekday() < 5:  # Mon-Fri only
            day_rows = _fetch_day_1m(instrument_key, access_token, d)
            rows.extend(day_rows)
            print(f"  fetched {d.isoformat()}: {len(day_rows)} 1m candles")
        d += timedelta(days=1)
    if not rows:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.sort_values("ts").reset_index(drop=True)
    return df


def resample_to_5m(df: pd.DataFrame) -> List[Bar]:
    if df.empty:
        return []
    df = df.set_index("ts")
    ohlc = df.resample("5min", label="left", closed="left").agg({
        "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
    }).dropna()
    bars: List[Bar] = []
    for ts, row in ohlc.iterrows():
        py_ts = ts.to_pydatetime()
        if py_ts.tzinfo is None:
            py_ts = py_ts.replace(tzinfo=IST)
        bars.append(Bar(
            timestamp=py_ts, open=float(row["open"]), high=float(row["high"]),
            low=float(row["low"]), close=float(row["close"]), volume=int(row["volume"]),
        ))
    return bars


def replay_and_report(bars_5m: List[Bar], bear: bool, ladder: List[int]) -> None:
    """Feed bars one at a time (as the live scanner would), print every NEWLY
    locked zone the moment it locks (i.e. as of the bar that just closed)."""
    kind = "BEAR (floor sweep/reclaim)" if bear else "BULL (ceiling sweep/reclaim)"
    print(f"\n=== {kind} — replay over {len(bars_5m)} x 5m bars ===")
    consumed_before = None
    zone_count = 0
    for i in range(2, len(bars_5m) + 1):
        window = bars_5m[:i]
        match: LadderMatch | None = scan_ladder(window, ladder, bear=bear, skip_before_ts=consumed_before)
        if match is None:
            continue
        z = match.zone
        # Only print once — the instant this exact reclaim first becomes visible.
        if z.lock_ts == bars_5m[i - 1].timestamp:
            zone_count += 1
            print(
                f"  [{zone_count}] multiplier={match.multiplier}m  "
                f"ref_ts={z.reference_low_ts}  entry_line={z.entry_line:.2f}  sl_level={z.sl_level:.2f}  "
                f"sweep_ts={z.sweep_started_ts}  sweep_low={z.sweep_low:.2f}  "
                f"TRAPPED_ts={z.lock_ts}"
            )
    if zone_count == 0:
        print("  (no zones locked in this range)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--underlying", default="NIFTY")
    ap.add_argument("--start", default=None, help="YYYY-MM-DD")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD (default: today)")
    ap.add_argument("--days", type=int, default=10, help="if --start omitted, look back N calendar days from --end")
    ap.add_argument("--max-ladder-minutes", type=int, default=_MAX_LADDER_MINUTES)
    args = ap.parse_args()

    end = date.fromisoformat(args.end) if args.end else date.today()
    start = date.fromisoformat(args.start) if args.start else (end - timedelta(days=args.days))

    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    if not creds or not creds.get("access_token"):
        print("FATAL: no Upstox access_token found in data/clients.db. "
              "Authenticate the Upstox feeder via the dashboard first.")
        sys.exit(1)
    access_token = creds["access_token"]

    instrument_key = REGISTRY.get_upstox_index_key(args.underlying)
    print(f"Fetching {args.underlying} ({instrument_key}) 1m candles {start} -> {end} ...")
    df_1m = fetch_range_1m(instrument_key, access_token, start, end)
    if df_1m.empty:
        print("FATAL: no 1m candles returned — check token validity / date range / market holidays.")
        sys.exit(1)
    print(f"Total 1m candles fetched: {len(df_1m)}")

    bars_5m = resample_to_5m(df_1m)
    print(f"Resampled to {len(bars_5m)} x 5m bars.")

    ladder = build_ladder(_LADDER_STEP, args.max_ladder_minutes)
    print(f"Ladder: {ladder}")

    replay_and_report(bars_5m, bear=True, ladder=ladder)
    replay_and_report(bars_5m, bear=False, ladder=ladder)


if __name__ == "__main__":
    main()
