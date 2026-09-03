"""
scripts/test_real_premium_replay.py — historical reality cross-check for the
V4CascadeEngine.

**2026-07-20 Index/Premium decoupling**: Gate 1 (structural sweep+reclaim) now
lives entirely on the 75m NIFTY spot chart (spot_confirm.py's
SpotConfirmTracker, unchanged) — it arms CE/PE, it is never itself the thing
that "locks" a tradeable zone. Gate 2 (the only chart-scanning gate left on
the CE/PE tracking premium contracts) looks exclusively for a 2-candle
bear-trap Demand Block on 5m (15m fallback), and only starts once Gate 1 has
armed that side (IndexGatedPremiumScanner, zone_state.py). Gate 3 (1/3-depth
limit + pierce) is unchanged.

Loads real NIFTY spot + CE 23900 / PE 24300 (21-JUL-2026 monthly-only expiry —
this is the only expiry currently trading, per user directive) 1-minute
history from Upstox, replays it bar-by-bar (chronologically interleaved 5m +
75m closes, exactly as a live feed would deliver them) through the real
V4CascadeEngine, and prints every gate transition + trigger with its exact
timestamp so it can be cross-checked against TradingView by eye — the same
validation method already used manually earlier this session.

No mocking of engine internals — this drives strategies/v4_cascade/engine.py
directly, unmodified.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List
from urllib.parse import quote as _q

import pandas as pd

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer.client_db import ClientDB
from data_layer.historical_candles import _http_get_json, _parse_candles
from data_layer.instrument_registry import REGISTRY
from strategies.v4_cascade.engine import V4CascadeEngine
from strategies.v4_cascade.rolling_base import resample_bars

EXPIRY = date(2026, 7, 21)
CE_STRIKE = 23900
PE_STRIKE = 24300
START = date(2026, 7, 4)
END = date(2026, 7, 17)


@dataclass
class Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    timeframe: int
    volume: int = 0


def fetch_1m(instrument_key: str, token: str, start: date, end: date) -> pd.DataFrame:
    rows: List[dict] = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            url = f"https://api.upstox.com/v2/historical-candle/{_q(instrument_key, safe='')}/1minute/{d.isoformat()}/{d.isoformat()}"
            try:
                rows.extend(_parse_candles(_http_get_json(url, token)))
            except Exception:
                pass
        d += timedelta(days=1)
    if not rows:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["ts"])
    return df.sort_values("ts").reset_index(drop=True)


def to_5m_bars(df: pd.DataFrame, filter_zero_volume: bool) -> List[Bar]:
    """``filter_zero_volume=True`` excludes Upstox's zero-volume forward-filled
    minutes before aggregating (2026-07-19 bugfix — a stale carried-forward
    price otherwise corrupts the bucket's real high/low). ONLY valid for
    OPTION premium data — NIFTY spot/index candles report volume=0 on every
    real tick (indices aren't traded instruments), so applying this filter to
    spot data would silently discard the entire series."""
    if df.empty:
        return []
    if filter_zero_volume:
        df = df[df["volume"] > 0]
        if df.empty:
            return []
    df = df.set_index("ts")
    ohlc = df.resample("5min", label="left", closed="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna()
    bars: List[Bar] = []
    for ts, row in ohlc.iterrows():
        py = ts.to_pydatetime()
        if py.tzinfo is None:
            py = py.replace(tzinfo=IST)
        bars.append(Bar(py, float(row["open"]), float(row["high"]), float(row["low"]),
                         float(row["close"]), timeframe=5, volume=int(row["volume"])))
    return bars


def to_resampled_bar(bars_5m: List[Bar], multiplier: int) -> List[Bar]:
    rb = resample_bars(bars_5m, multiplier)
    return [Bar(b.timestamp, b.close, b.high, b.low, b.close, timeframe=multiplier) for b in rb]


def bucket_start(ts: datetime, multiplier: int) -> datetime:
    """The start timestamp of the ``multiplier``-minute bucket containing ``ts``."""
    open_dt = ts.replace(hour=9, minute=15, second=0, microsecond=0)
    minutes_since_open = int((ts - open_dt).total_seconds() // 60)
    bucket_idx = minutes_since_open // multiplier
    return open_dt + timedelta(minutes=bucket_idx * multiplier)


def bucket_end_minute(ts: datetime, multiplier: int) -> bool:
    """True if ``ts`` (a 5m bar's own timestamp / bucket start) is the LAST 5m
    bar inside its ``multiplier``-minute bucket (so the bucket is now closed)."""
    open_dt = ts.replace(hour=9, minute=15, second=0, microsecond=0)
    minutes_since_open = int((ts - open_dt).total_seconds() // 60)
    return (minutes_since_open + 5) % multiplier == 0


def replay(engine: V4CascadeEngine, spot_5m: List[Bar], ce_5m: List[Bar], pe_5m: List[Bar]) -> None:
    spot_75m_all = {b.timestamp: b for b in to_resampled_bar(spot_5m, 75)}

    ce_by_ts = {b.timestamp: b for b in ce_5m}
    pe_by_ts = {b.timestamp: b for b in pe_5m}
    all_ts = sorted(set(ce_by_ts) | set(pe_by_ts))

    prev_scanner_state = {"CE": engine._scanners["CE"].state, "PE": engine._scanners["PE"].state}
    prev_index_kind = engine._spot_confirm.current_kind
    last_zones: dict = {"CE": {}, "PE": {}}   # side -> {id(setup): zone_info}, captured before pop_setup() wipes it
    trade_log: List[dict] = []

    for ts in all_ts:
        events = []
        ce_bar = ce_by_ts.get(ts)
        pe_bar = pe_by_ts.get(ts)

        # Snapshot zone info for every in-flight setup, keyed by OBJECT
        # IDENTITY (not price -- multiple independent setups can coincidentally
        # compute the same limit price), captured BEFORE the same/a later
        # update() call fires+pops it. Also snapshot which setup ids are
        # in-flight on each side pre-call so we can diff post-call to find
        # exactly which setup(s) fired this step.
        pre_ids = {side: {id(s): s for s in engine._scanners[side].setups} for side in ("CE", "PE")}
        for side in ("CE", "PE"):
            for setup in engine._scanners[side].setups:
                if setup.zone is not None:
                    last_zones.setdefault(side, {})[id(setup)] = {
                        "ref_ts": setup.ref_ts, "entry": setup.zone.entry_line,
                        "sl": setup.zone.sl_level, "lock_ts": setup.zone.lock_ts,
                        "tf": setup.timeframe, "limit_price": setup.limit_entry_price,
                    }

        events += engine.update(ce_bar=ce_bar, pe_bar=pe_bar)
        if bucket_end_minute(ts, 75):
            bstart = bucket_start(ts, 75)
            spot_bar = spot_75m_all.get(bstart)
            if spot_bar is not None:
                events += engine.update(spot_bar=spot_bar)

        if engine._spot_confirm.current_kind != prev_index_kind:
            print(f"  [{ts}] INDEX gate -> {engine._spot_confirm.current_kind.value}")
            prev_index_kind = engine._spot_confirm.current_kind

        for side in ("CE", "PE"):
            st = engine._scanners[side].state
            if st != prev_scanner_state[side]:
                print(f"  [{ts}] {side} gate -> {st.value}")
                prev_scanner_state[side] = st

        for ev in events:
            if ev.event_type.value.startswith("open_long"):
                post_ids = {id(s) for s in engine._scanners[ev.side].setups}
                fired_ids = [sid for sid in pre_ids[ev.side] if sid not in post_ids]
                z = {}
                for sid in fired_ids:
                    cand = last_zones.get(ev.side, {}).get(sid)
                    if cand is not None:
                        z = cand
                        break
                trade_log.append({"side": ev.side, "entry_ts": ts, "entry_price": ev.price_hint, **z})
            print(f"  [{ts}] *** EVENT *** {ev.event_type.value} side={ev.side} tranche={ev.tranche} "
                  f"price_hint={ev.price_hint} sl={ev.sl_price} target={ev.target_price} reason={ev.reason}")

    print("\n=== ENTRY REASON TABLE (why each trade fired) ===")
    header = (f"{'Side':4} {'Entry TS':20} {'Entry Px':>9} | {'Premium Ref TS':20} {'Entry':>10} "
              f"{'SL':>8} {'Lock TS':20} {'TF':4} | {'LimitPx':>9}")
    print(header)
    for r in trade_log:
        print(f"{r['side']:4} {str(r['entry_ts']):20} {r['entry_price']:9.2f} | "
              f"{str(r.get('ref_ts')):20} {(r.get('entry') or 0):10.2f} "
              f"{(r.get('sl') or 0):8.2f} {str(r.get('lock_ts')):20} {str(r.get('tf')):4} | "
              f"{(r.get('limit_price') or 0):9.2f}")


def main() -> None:
    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    if not creds or not creds.get("access_token"):
        print("FATAL: no Upstox access_token in data/clients.db.")
        sys.exit(1)
    token = creds["access_token"]
    REGISTRY.load_sync("NIFTY", token)

    spot_key = REGISTRY.get_upstox_index_key("NIFTY")
    ce_key = REGISTRY.get_upstox_key("NIFTY", EXPIRY, CE_STRIKE, "CE")
    pe_key = REGISTRY.get_upstox_key("NIFTY", EXPIRY, PE_STRIKE, "PE")
    print(f"spot={spot_key}  CE({CE_STRIKE})={ce_key}  PE({PE_STRIKE})={pe_key}")

    print(f"Fetching {START} -> {END} ...")
    spot_5m = to_5m_bars(fetch_1m(spot_key, token, START, END), filter_zero_volume=False)
    ce_5m = to_5m_bars(fetch_1m(ce_key, token, START, END), filter_zero_volume=True)
    pe_5m = to_5m_bars(fetch_1m(pe_key, token, START, END), filter_zero_volume=True)
    print(f"spot 5m bars={len(spot_5m)}  CE 5m bars={len(ce_5m)}  PE 5m bars={len(pe_5m)}")

    engine = V4CascadeEngine()
    print("\n=== replay (gate transitions + fired events) ===")
    replay(engine, spot_5m, ce_5m, pe_5m)

    print("\n=== INDEX GATE (final state) ===")
    print(f"kind={engine._spot_confirm.current_kind.value}  "
          f"CE armed={engine._spot_confirm.confirms('CE')}  PE armed={engine._spot_confirm.confirms('PE')}")

    print("\n=== STILL-OPEN SETUPS (no trade fired yet) — prospective entry ===")
    header2 = (f"{'Side':4} {'State':16} {'TF':4} {'Ref TS':20} {'Entry':>10} {'SL':>8} | "
               f"{'Prospective Entry':>17}")
    print(header2)
    for side in ("CE", "PE"):
        s = engine._scanners[side]
        for setup in sorted(s.setups, key=lambda x: x.ref_ts):
            z = setup.zone
            prospective = ""
            lp = setup.limit_entry_price
            if lp is None and z is not None and z.entry_line is not None and z.sweep_low is not None:
                lp = z.entry_line - (z.entry_line - z.sweep_low) / 3.0
            if lp is not None:
                prospective = f"{lp:.2f}"
            print(f"{side:4} {setup.state.value:16} {setup.timeframe:4} {str(setup.ref_ts):20} "
                  f"{(z.entry_line if z else 0):10.2f} {(z.sl_level if z else 0):8.2f} | {prospective:>17}")
    print("\nposition:", engine.position)


if __name__ == "__main__":
    main()
