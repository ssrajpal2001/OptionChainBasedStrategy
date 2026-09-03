"""
scripts/v4_btc_weekend_test.py — 2026-07-19 WEEKEND SANITY TEST ONLY.

Stress-tests the real, UNMODIFIED V4CascadeEngine / PremiumGateScanner state
machine against live BTC perpetual (BTCUSD on Delta Exchange) price action,
since NIFTY is closed. Not part of the production app, not wired into
run_system.py, not committed to git — a standalone throwaway harness.

WHAT THIS ACTUALLY VALIDATES:
  - The real engine.py/zone_state.py/rolling_base.py/entries.py/exits.py
    code paths, unmodified, under real-time-like conditions: historical
    warm-up -> live catch-up -> gate transitions -> Gate 3 fire -> T1/T2 exit.
  - Multi-day lookback ingestion (48h BTC history) + de-duped re-ingestion.
  - Verbose terminal audit logging of every gate transition and fired event.

WHAT THIS DOES NOT VALIDATE (flagged explicitly, not silently skipped):
  - The admin/client dashboard UI (Force Ingest Zones button, Live Trap
    Status grid, global Run/Stop toggle) — those only work through the REAL
    V4CascadeBookManager/book.py wired to a live deployment, and that
    manager deliberately rejects any underlying other than NIFTY (by
    design — the ATM+-200 tracking offsets are NIFTY-specific). Testing
    the actual UI/admin wiring requires either a real NIFTY deployment
    tomorrow at market open, or a separate, explicit decision to relax
    the NIFTY-only restriction — not done here since that's a real
    production behavior change, not a "temporary, uncommitted" one.

HOW TIMEFRAMES ARE REMAPPED (no production code touched):
  V4CascadeEngine._update_side() dispatches purely on bar.timeframe==75
  (HTF) / ==5 (MTF) — it never checks the bar's REAL duration. So 15-minute
  BTC bars are fed in tagged .timeframe=75, and 1-minute BTC bars tagged
  .timeframe=5. The gate logic inside zone_state.py is likewise timeframe-
  value-agnostic (it just processes whatever bars arrive via
  on_75m_bar()/on_5m_bar()) -- zero engine/zone_state changes needed.

  CAVEAT: rolling_base.resample_bars() anchors 75m-style bucket boundaries
  to a 09:15 "session open" (NSE convention) — cosmetically odd for a 24/7
  asset like BTC (buckets won't align to a meaningful "market open"), but
  functionally harmless for this test (buckets are still consistently
  15-minute-aligned to clock time either way).

  Spot bias: the SAME BTC bar stream is fed as spot_bar (for bear/bull
  classification -> CE/PE arming) AND as both ce_bar/pe_bar (the "premium"
  the 3 gates scan) -- there's no real option premium for a spot-only
  stress test, so CE and PE both track raw BTC price, maximizing
  observable gate activity on both sides at once.
"""
from __future__ import annotations

import asyncio
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

sys.path.insert(0, ".")

from strategies.v4_cascade.dataclasses import GateState
from strategies.v4_cascade.engine import V4CascadeEngine
from strategies.v4_cascade.rolling_base import resample_bars

logging.basicConfig(level=logging.WARNING)  # quiet 3rd-party noise; we print our own audit lines
IST = timezone(timedelta(hours=5, minutes=30))

DELTA_BASE = "https://api.india.delta.exchange"
SYMBOL = "BTCUSD"           # perpetual — serves as "spot" for crypto, same as production DeltaChainManager
LOOKBACK_HOURS = 48
POLL_SECONDS = 5.0          # live ticker poll interval
HTF_MINUTES_REAL = 60       # real duration of the "HTF" bars (tagged .timeframe=75) — multiples of 60m for crypto
MTF_MINUTES_REAL = 1        # real duration of the "MTF" bars (tagged .timeframe=5)


@dataclass
class Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    timeframe: int
    volume: int = 0


def _audit(msg: str) -> None:
    print(f"[AUDIT {datetime.now(IST).strftime('%H:%M:%S')}] {msg}", flush=True)


def _get_json(url: str, params: dict) -> dict:
    """Blocking curl_cffi GET (same pattern as data_layer/historical_candles.py's
    _http_get_json) -- public Delta market-data endpoints, no auth needed."""
    from curl_cffi import requests as _cc
    try:
        return _cc.get(url, params=params, impersonate="chrome131", timeout=15).json()
    except Exception as exc:
        print(f"  [http error] {url}: {exc}")
        return {}


async def fetch_history(hours: int) -> List[dict]:
    end = int(time.time())
    start = end - hours * 3600
    url = f"{DELTA_BASE}/v2/history/candles"
    params = {"resolution": "1m", "symbol": SYMBOL, "start": start, "end": end}
    data = await asyncio.to_thread(_get_json, url, params)
    rows = data.get("result", []) or []
    # Delta returns newest-first or oldest-first depending on version -- normalize by time.
    rows.sort(key=lambda c: c["time"])
    return rows


async def fetch_ticker() -> dict:
    data = await asyncio.to_thread(_get_json, f"{DELTA_BASE}/v2/tickers/{SYMBOL}", {})
    return data.get("result", {}) or {}


def rows_to_1m_bars(rows: List[dict]) -> List[Bar]:
    bars = []
    for c in rows:
        ts = datetime.fromtimestamp(int(c["time"]), tz=timezone.utc).astimezone(IST)
        bars.append(Bar(ts, float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"]),
                         timeframe=5, volume=int(c.get("volume", 0) or 0)))
    return bars


def resample_to_htf(bars_1m: List[Bar], real_minutes: int) -> List[Bar]:
    rb = resample_bars(bars_1m, real_minutes)
    return [Bar(b.timestamp, b.close, b.high, b.low, b.close, timeframe=75) for b in rb]


def _log_gate_states(engine: V4CascadeEngine, prev: Dict[str, Dict]) -> None:
    for side in ("CE", "PE"):
        scanner = engine._scanners[side]
        seen_ids = set()
        for setup in scanner.setups:
            key = id(setup)
            seen_ids.add(key)
            prev_state = prev.get(side, {}).get(key)
            if prev_state != setup.state:
                _audit(f"BTC[{side}] setup(htf_ref={setup.htf_ref_ts}) {prev_state} -> {setup.state.value}"
                       + (f"  limit_price={setup.limit_entry_price:.2f}" if setup.limit_entry_price else ""))
                prev.setdefault(side, {})[key] = setup.state
        # drop stale ids (setup popped/consumed)
        for key in list(prev.get(side, {})):
            if key not in seen_ids:
                del prev[side][key]


async def warm_historical_zones(engine: V4CascadeEngine) -> List[Bar]:
    _audit(f"Fetching {LOOKBACK_HOURS}h of {SYMBOL} 1m history from Delta ...")
    rows = await fetch_history(LOOKBACK_HOURS)
    bars_1m = rows_to_1m_bars(rows)
    bars_htf = resample_to_htf(bars_1m, HTF_MINUTES_REAL)
    _audit(f"Fetched {len(bars_1m)} x 1m bars -> {len(bars_htf)} x {HTF_MINUTES_REAL}m HTF bars. Replaying ...")

    htf_by_ts = {b.timestamp: b for b in bars_htf}
    n_events = 0
    for i, bar in enumerate(bars_1m):
        events = engine.update(ce_bar=bar, pe_bar=bar)
        n_events += len(events)
        # feed HTF bars + spot bias at each real HTF_MINUTES_REAL boundary
        open_dt = bar.timestamp.replace(minute=0, second=0, microsecond=0)
        minutes_since = int((bar.timestamp - open_dt).total_seconds() // 60)
        if minutes_since % HTF_MINUTES_REAL == (HTF_MINUTES_REAL - 1):
            bucket_start = bar.timestamp.replace(
                minute=(bar.timestamp.minute // HTF_MINUTES_REAL) * HTF_MINUTES_REAL, second=0, microsecond=0)
            htf_bar = htf_by_ts.get(bucket_start)
            if htf_bar is not None:
                engine.update(spot_bar=htf_bar, ce_bar=htf_bar, pe_bar=htf_bar)
        for ev in events:
            _audit(f"*** HISTORICAL EVENT *** {ev.event_type.value} side={ev.side} tranche={ev.tranche} "
                   f"price={ev.price_hint} reason={ev.reason} @ {ev.timestamp}")
    _audit(f"Historical warm-up complete: {n_events} synthetic events replayed. "
           f"CE setups={len(engine._scanners['CE'].setups)} PE setups={len(engine._scanners['PE'].setups)}")
    return bars_1m


async def live_loop(engine: V4CascadeEngine, seed_bars: List[Bar]) -> None:
    _audit(f"Entering live poll loop (every {POLL_SECONDS}s) — Ctrl+C to stop.")
    prev_states: Dict[str, Dict] = {}
    _log_gate_states(engine, prev_states)  # snapshot post-warm-up state as baseline

    cur_bucket: Optional[datetime] = None
    cur_bar: Optional[Bar] = None
    all_1m: List[Bar] = list(seed_bars)

    while True:
        try:
            d = await fetch_ticker()
            ltp = float(d.get("spot_price") or d.get("mark_price") or d.get("close") or 0)
        except Exception as exc:
            _audit(f"ticker poll failed: {exc}")
            await asyncio.sleep(POLL_SECONDS)
            continue

        if ltp <= 0:
            await asyncio.sleep(POLL_SECONDS)
            continue

        now = datetime.now(IST)
        bucket = now.replace(second=0, microsecond=0)
        if cur_bucket is None:
            cur_bucket = bucket
            cur_bar = Bar(bucket, ltp, ltp, ltp, ltp, timeframe=5)
        elif bucket != cur_bucket:
            # 1m bar closed -> feed MTF
            all_1m.append(cur_bar)
            events = engine.update(ce_bar=cur_bar, pe_bar=cur_bar)
            for ev in events:
                _audit(f"*** LIVE EVENT (Gate 3 fire / exit) *** {ev.event_type.value} side={ev.side} "
                       f"tranche={ev.tranche} price={ev.price_hint} sl={ev.sl_price} "
                       f"target={ev.target_price} reason={ev.reason}")
            # HTF boundary check on the just-closed bar
            minutes_since_hour = cur_bar.timestamp.minute
            if minutes_since_hour % HTF_MINUTES_REAL == (HTF_MINUTES_REAL - 1) % HTF_MINUTES_REAL or True:
                recent = [b for b in all_1m if b.timestamp >= cur_bar.timestamp - timedelta(minutes=HTF_MINUTES_REAL * 3)]
                htf_bars = resample_to_htf(recent, HTF_MINUTES_REAL)
                if htf_bars:
                    last_htf = htf_bars[-1]
                    if last_htf.timestamp + timedelta(minutes=HTF_MINUTES_REAL) <= cur_bar.timestamp + timedelta(minutes=1):
                        engine.update(spot_bar=last_htf, ce_bar=last_htf, pe_bar=last_htf)
            _log_gate_states(engine, prev_states)
            pos = engine.position
            if pos is not None and pos.is_open and pos.t1 is not None:
                _audit(f"LIVE POSITION side={pos.side} entry={pos.t1.entry_price:.2f} "
                       f"ltp={ltp:.2f} sl={pos.t1.sl_price:.2f} target={(pos.t1.target_price or 0):.2f}")
            cur_bucket = bucket
            cur_bar = Bar(bucket, ltp, ltp, ltp, ltp, timeframe=5)
        else:
            cur_bar.high = max(cur_bar.high, ltp)
            cur_bar.low = min(cur_bar.low, ltp)
            cur_bar.close = ltp

        await asyncio.sleep(POLL_SECONDS)


async def main() -> None:
    engine = V4CascadeEngine()
    seed_bars = await warm_historical_zones(engine)
    # CE/PE bias check right after warm-up
    for side in ("CE", "PE"):
        s = engine._scanners[side]
        _audit(f"post-warmup {side}: armed={s.armed} state={s.state.value} setups={len(s.setups)}")
    try:
        await live_loop(engine, seed_bars)
    except KeyboardInterrupt:
        _audit("Stopped by user.")


if __name__ == "__main__":
    asyncio.run(main())
