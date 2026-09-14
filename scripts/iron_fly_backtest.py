"""
scripts/iron_fly_backtest.py -- Phase 1 backtest for the NIFTY Weekly Iron
Condor -> Iron Fly strategy (approved plan, 2026-09-13), driving the REAL
`strategies.iron_fly.engine.IronFlyEngine` against real Upstox historical
data -- never a parallel reimplementation of the rules (per this repo's own
`feedback_backtest_drive_real_class` convention).

SECURITY: pass a fresh Upstox access token as argv[1] at run time. NEVER
hardcode a token in this file or any other committed file -- a token pasted
into a chat/terminal history earlier in this project was treated as
compromised and rotated; this script must never reintroduce that mistake.

Data window: fixed to 2026-09-08 .. today, per the user's own confirmation
that this is the only real historical window available for the currently-
active ("next week", at the time this was requested) NIFTY weekly contract.

Approach (a practical variant of the plan's "3-phase walk-forward"):
  1. Fetch real NIFTY spot 1-min candles for the whole window (cheap, one
     instrument key). This alone requires no option data at all, since
     every entry/roll/conversion TRIGGER in this strategy (+/-100pt moves,
     ATM == a short strike) is fully spot-derived -- only the exact STRIKE
     chosen at each trigger, and the profit-target check, need real premium.
  2. Derive a strike band from the window's OWN observed spot min/max (never
     an arbitrary full chain) padded by STRIKE_BAND_PADDING points to give
     the >Rs20/<Rs20 threshold search room to find real short/long strikes
     on both sides. Fetch real 1-min premium history for every strike in
     that band, both CE and PE, across every trading day in the window --
     bounded, rate-limit-paced (chunked concurrent fetches + inter-chunk
     sleep, same precaution `scripts/nifty_1500_sr_breakout_backtest.py`
     needed after a real rate-limit incident).
     NOTE: this is a deliberate simplification of the plan's literal
     "discover exactly which strikes get used, then fetch only those"
     adaptive design -- doing that precisely is circular here (which
     strikes get used depends on premiums we don't have yet, unlike a
     spot-only entry signal strategy where the strike is a fixed ATM
     offset). A window-wide band fetched once is honest, bounded by real
     observed price action, and avoids that circularity.
  3. Replay every spot 1-min bar (market hours only) through ONE continuous
     `IronFlyEngine` instance in chronological order across the whole
     window (this strategy carries positions across days -- no daily
     reset, per the approved plan's "no EOD square-off" decision), backed
     by a premium lookup keyed off each bar's own real historical premium.
     Print the engine's own trade_log verbatim for manual review against
     the doc's worked examples.

Known, honestly-flagged limitations (same category as every other
scripts/*_backtest.py in this repo):
  - Small sample -- this window is ~4-6 trading days, same caveat CAG
    Straddle's own 8-day/n=10 backtest carries. This validates mechanics
    and surfaces bugs; it does not statistically prove the strategy.
  - 1-min bar CLOSE is fed as "the tick" -- true intra-minute tick paths
    (e.g. a gap that both crosses and returns to a trigger within one
    minute) are not resolvable from 1-min OHLC alone; only what the close
    sequence itself shows is replayed.
  - Brokerage + govt charges: direct user spec (2026-09-14) -- flat Rs60
    per order (any buy OR sell, whether an open or a close), applied via
    `IronFlyEngine.order_count` (a lifetime counter across cycles -- real
    charges don't reset at a cycle boundary). This is a flat per-order fee
    model only -- no STT/turnover-based slippage beyond that flat figure.
  - qty is a placeholder 1-lot (LOT_SIZE), not a real deployed size.
  - Upstox's historical option-candle endpoint may not have data for every
    requested strike/day (illiquid strikes, or the strike simply never
    traded that day) -- those series come back empty and the engine's own
    "skip and retry" behavior (see detector.py) takes over exactly as it
    would live.
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, time as dtime
from typing import Dict, List, Optional
from urllib.parse import quote as _q

sys.path.insert(0, ".")

from data_layer.historical_candles import (
    fetch_upstox_range_1m,
    fetch_upstox_intraday_1m,
    _http_get_json,
    _parse_candles,
)
from data_layer.instrument_registry import REGISTRY
from strategies.iron_fly.engine import IronFlyEngine

TOKEN = sys.argv[1] if len(sys.argv) > 1 else ""
SPOT_KEY = "NSE_INDEX|Nifty 50"
STRIKE_STEP = 50
LOT_SIZE = 75
BACKTEST_START = date(2026, 9, 8)
BROKERAGE_PER_ORDER = 60.0  # direct user spec, 2026-09-14: brokerage + govt
                            # charges combined, flat per buy or sell order
STRIKE_BAND_PADDING = 1000.0  # points beyond the window's own observed spot min/max
FETCH_CHUNK = 10              # concurrent option-series fetches per batch
FETCH_CHUNK_PAUSE_SEC = 1.5   # pause between batches -- real rate-limit incident precedent


@dataclass
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float


def _rows_to_bars(rows: List[dict]) -> List[Bar]:
    out = [
        Bar(ts=datetime.fromisoformat(r["ts"]), open=r["open"], high=r["high"], low=r["low"], close=r["close"])
        for r in rows
    ]
    out.sort(key=lambda b: b.ts)
    return out


async def _fetch_dated(key: str, day: date, token: str) -> List[dict]:
    url = (f"https://api.upstox.com/v2/historical-candle/{_q(key, safe='')}/1minute/"
           f"{day.isoformat()}/{day.isoformat()}")
    return await asyncio.to_thread(lambda: _parse_candles(_http_get_json(url, token)))


async def _token_is_valid(token: str) -> bool:
    """Same up-front probe as scripts/nifty_1500_sr_breakout_backtest.py --
    _http_get_json swallows HTTP errors into {}, so an expired token and
    genuinely-no-data look identical without this check."""
    try:
        resp = await asyncio.to_thread(_http_get_json, "https://api.upstox.com/v2/user/profile", token)
    except Exception:
        return False
    return bool(resp) and resp.get("status") == "success"


class PremiumLookup:
    """Wraps pre-fetched {(strike, side): [Bar,...]} series into an
    engine-compatible get_premium(strike, side) callable. Call advance_to(ts)
    once per spot tick before invoking the engine, so lookups reflect the
    most recent premium bar at or before that timestamp -- forward-filled
    WITHIN a trading day only; a strike with no more bars today returns None
    rather than silently answering with yesterday's stale close."""

    def __init__(self, series: Dict[tuple, List[Bar]]) -> None:
        self._series = series
        self._idx: Dict[tuple, int] = {k: -1 for k in series}
        self._now: Optional[datetime] = None

    def advance_to(self, ts: datetime) -> None:
        self._now = ts
        for key, bars in self._series.items():
            idx = self._idx[key]
            while idx + 1 < len(bars) and bars[idx + 1].ts <= ts:
                idx += 1
            self._idx[key] = idx

    def get(self, strike: int, side: str) -> Optional[float]:
        bars = self._series.get((strike, side))
        if not bars:
            return None
        idx = self._idx.get((strike, side), -1)
        if idx < 0:
            return None
        bar = bars[idx]
        if self._now is None or bar.ts.date() != self._now.date():
            return None
        return bar.close


async def main() -> None:
    if not TOKEN:
        print("Usage: python scripts/iron_fly_backtest.py <upstox_access_token>")
        print("Pass a FRESH token -- never commit one to this file.")
        return
    if not await _token_is_valid(TOKEN):
        print("ERROR: Upstox token appears INVALID or EXPIRED (checked via /v2/user/profile).")
        return

    end = date.today()
    start = BACKTEST_START
    print(f"Fetching NIFTY spot 1-min candles {start} .. {end} ...")

    spot_rows = await fetch_upstox_range_1m(SPOT_KEY, TOKEN, start, end - timedelta(days=1))
    spot_bars = _rows_to_bars(spot_rows)
    today_rows = await fetch_upstox_intraday_1m(SPOT_KEY, TOKEN)
    if not today_rows:
        today_rows = await fetch_upstox_range_1m(SPOT_KEY, TOKEN, end, end)
    spot_bars += _rows_to_bars(today_rows)
    spot_bars.sort(key=lambda b: b.ts)
    spot_bars = [b for b in spot_bars if start <= b.ts.date() <= end]

    if not spot_bars:
        print("No spot data returned -- check token / date range / market holidays.")
        return

    trading_days = sorted({b.ts.date() for b in spot_bars})
    print(f"{len(spot_bars)} spot bars across {len(trading_days)} trading day(s): {trading_days}")

    REGISTRY.load_sync("NIFTY", TOKEN)
    expiry = REGISTRY.get_active_expiry_strict("NIFTY", from_date=trading_days[0])
    if expiry is None:
        print("Could not resolve an active NIFTY weekly expiry for this window -- abort.")
        return
    print(f"Using expiry: {expiry}")

    min_spot = min(b.low for b in spot_bars)
    max_spot = max(b.high for b in spot_bars)
    band_lo = int((min_spot - STRIKE_BAND_PADDING) // STRIKE_STEP * STRIKE_STEP)
    band_hi = int(-(-(max_spot + STRIKE_BAND_PADDING) // STRIKE_STEP) * STRIKE_STEP)
    strikes = list(range(band_lo, band_hi + STRIKE_STEP, STRIKE_STEP))
    print(f"Observed spot range [{min_spot:.1f}, {max_spot:.1f}] -> strike band "
          f"[{band_lo}, {band_hi}] ({len(strikes)} strikes x 2 sides x {len(trading_days)} days)")

    series: Dict[tuple, List[Bar]] = {}

    async def _fetch_one(strike: int, side: str) -> None:
        key = REGISTRY.get_upstox_key("NIFTY", expiry, strike, side)
        if not key:
            series[(strike, side)] = []
            return
        bars: List[Bar] = []
        for day in trading_days:
            if day == date.today():
                rows = await fetch_upstox_intraday_1m(key, TOKEN)
                if not rows:
                    rows = await _fetch_dated(key, day, TOKEN)
            else:
                rows = await _fetch_dated(key, day, TOKEN)
            bars.extend(_rows_to_bars(rows))
        bars.sort(key=lambda b: b.ts)
        series[(strike, side)] = bars

    tasks = [_fetch_one(s, side) for s in strikes for side in ("CE", "PE")]
    for i in range(0, len(tasks), FETCH_CHUNK):
        await asyncio.gather(*tasks[i:i + FETCH_CHUNK])
        if i + FETCH_CHUNK < len(tasks):
            await asyncio.sleep(FETCH_CHUNK_PAUSE_SEC)

    n_with_data = sum(1 for v in series.values() if v)
    print(f"Fetched {n_with_data}/{len(series)} strike/side series with real data.\n")

    lookup = PremiumLookup(series)
    engine = IronFlyEngine(qty=LOT_SIZE, strike_step=float(STRIKE_STEP))

    for bar in spot_bars:
        if not (dtime(9, 15) <= bar.ts.time() <= dtime(15, 30)):
            continue
        lookup.advance_to(bar.ts)
        engine.on_spot_tick(bar.close, lookup.get, ts=bar.ts)

    print(f"=== TRADE LOG ({len(engine.trade_log)} event(s), {engine.cycle_number} cycle(s) started) ===")
    for ev in engine.trade_log:
        print(f"  {ev}")

    print(f"\n=== COST-ADJUSTED LEDGER (Rs{BROKERAGE_PER_ORDER:.0f}/order, every buy or sell) ===")
    print(f"{'when':<20} {'event':<24} {'orders':>7} {'charges':>10} {'realized':>12} {'net after chg':>14}")
    for ev in engine.trade_log:
        when = ev["ts"].strftime("%m-%d %H:%M") if ev.get("ts") else "?"
        oc = ev.get("order_count", 0)
        charges = oc * BROKERAGE_PER_ORDER
        realized = ev.get("lifetime_realized_pnl", 0.0)
        net = realized - charges
        print(f"{when:<20} {ev['event']:<24} {oc:>7} {charges:>10.2f} {realized:>12.2f} {net:>14.2f}")

    total_realized = engine.lifetime_realized_pnl
    total_charges_so_far = engine.order_count * BROKERAGE_PER_ORDER
    net_realized_after_charges = total_realized - total_charges_so_far
    print(f"\nTotal realized P&L (all closed legs, all cycles): Rs{total_realized:+.2f}")
    print(f"Total orders placed so far: {engine.order_count}  ->  charges: Rs{total_charges_so_far:.2f}")
    print(f"Net realized P&L after charges: Rs{net_realized_after_charges:+.2f}")

    if not engine.is_flat():
        final_ts = spot_bars[-1].ts
        lookup.advance_to(final_ts)
        open_legs = [(l, s) for l, s in engine._leg_sides() if l is not None]
        print(f"\nPosition still OPEN at end of window ({final_ts}): {engine._legs_dict()}")
        unrealized = 0.0
        have_all_live = True
        for leg, side in open_legs:
            live = lookup.get(leg.strike, side)
            if live is None:
                have_all_live = False
                print(f"  {side} strike={leg.strike} entry={leg.entry_price:.2f} live=None (stopped ticking)")
                continue
            leg_unrealized = (leg.entry_price - live) * leg.qty if leg.is_short else (live - leg.entry_price) * leg.qty
            unrealized += leg_unrealized
            print(f"  {side} strike={leg.strike} entry={leg.entry_price:.2f} live={live:.2f} unrealized=Rs{leg_unrealized:+.2f}")
        if have_all_live:
            projected_closing_charges = len(open_legs) * BROKERAGE_PER_ORDER
            print(f"\nUnrealized P&L on open legs: Rs{unrealized:+.2f}")
            print(f"Projected closing charges for these {len(open_legs)} open legs: Rs{projected_closing_charges:.2f}")
            print(f"'If closed right now' net P&L after ALL charges (realized+unrealized-all charges): "
                  f"Rs{(total_realized + unrealized - total_charges_so_far - projected_closing_charges):+.2f}")
        else:
            print("(Some open legs have no live premium at the final tick -- 'if closed now' total skipped.)")
    else:
        print("\nFlat at end of window.")


if __name__ == "__main__":
    asyncio.run(main())
