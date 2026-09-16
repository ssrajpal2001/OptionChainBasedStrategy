"""
scripts/oi_orb_screener_20260916_full_threshold_sweep.py

Direct user spec, 2026-09-16: "optimise all values which r used in
threshold" -- sweeps the 3 real thresholds the NEW mechanic (today's
price-trigger -> continuous futures-OI-confirm -> VWAP-retest) actually
uses:
  PRICE_TRIGGER_PCT  in [1.5, 2.0, 2.5, 3.0]
  OI_CONFIRM_THRESHOLD_PCT in [2.0, 3.0, 4.0, 5.0]
  VWAP_WINDOW_MIN    in [10, 15, 20, 30]
= 64 combos, run against the SAME real 20-stock universe and real 1-min
data every other 2026-09-16 script used.

Two-phase, same discipline as the earlier oi_orb_screener_20260916_
threshold_sweep.py: real per-symbol equity bars, prev-close, and futures-
OI series are fetched ONCE and cached (not re-fetched per combo -- 64x
fewer real API calls); each combo is then evaluated purely in-memory
against that cache for SPEED (trade count + a simple point-based proxy
P&L: the underlying's own price delta between entry and EOD/15:15,
signed for side -- NOT real option premium, deliberately, so this whole
64-combo grid can run in one pass without 64x the real option-premium
fetches every other script needed). Reports the full grid, then flags
the combo(s) that look most promising by proxy P&L for a real-premium
follow-up validation (same "approximate first, validate the winner with
real premium" pattern already established in this codebase, e.g. D1Trap
zone-definition tuning).

The already-known baseline (2.0%/3.0%/15min) full REAL-option result is
+110.15 pts (5 trades) -- shown here for reference, not recomputed.

MUST run on EC2 (real Upstox2 access token + real intraday history).

Usage: python scripts/oi_orb_screener_20260916_full_threshold_sweep.py
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer import historical_candles as hc
from data_layer.client_db import ClientDB
from strategies.core.trap_zone_utils import Bar
from strategies.oi_orb_screener import stock_resolve
from strategies.oi_orb_screener.engine import OiOrbScreenerStrategy
from strategies.oi_orb_screener.screener import VwapState, RollingVwapRetestTracker

TRADE_DATE = "2026-09-16"
TODAY = date.fromisoformat(TRADE_DATE)
EOD_TIME = "15:15"

PRICE_GRID = [1.5, 2.0, 2.5, 3.0]
OI_GRID = [2.0, 3.0, 4.0, 5.0]
WINDOW_GRID = [10.0, 15.0, 20.0, 30.0]

BASELINE_REAL_PNL = 110.15   # already-validated real-option result at 2.0/3.0/15
BASELINE_TRADES = 5

CLIENT_ID = "ssrajpal2001"
BINDING_ID = "UPSTOX"

UNIVERSE = [
    "PATANJALI", "ASTRAL", "POLICYBZR", "PREMIERENE", "PAYTM", "BLUESTARCO",
    "OFSS", "SOLARINDS", "BAJAJHLDNG", "WAAREEENER", "YESBANK", "LAURUSLABS",
    "NESTLEIND", "BSE", "TCS", "NYKAA", "MFSL", "BOSCHLTD", "SHRIRAMFIN", "COFORGE",
]


class _NullBus:
    def subscribe(self, topic):
        return None

    def unsubscribe(self, topic, q):
        pass

    async def publish(self, topic, event):
        pass


def _access_token() -> str:
    creds = ClientDB().get_feeder_creds_sync("upstox2")
    if creds and creds.get("access_token"):
        return creds["access_token"]
    raise RuntimeError("No upstox2 feeder access_token found -- run this on EC2.")


def _to_bars(rows):
    out = []
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        out.append(Bar(ts=ts.astimezone(IST), open=float(r["open"]), high=float(r["high"]),
                        low=float(r["low"]), close=float(r["close"])))
    return out


async def _fetch_symbol_cache(book, sym, token):
    """One real fetch pass per symbol -- reused across all 64 combos."""
    eq_key = stock_resolve.resolve_eq_instrument_key(sym)
    if not eq_key:
        return None
    daily = await hc.fetch_upstox_daily(eq_key, token, lookback_days=5)
    prev_close = None
    for row in reversed(daily or []):
        row_date = row["ts"][:10] if isinstance(row["ts"], str) else row["ts"].date().isoformat()
        if row_date < TRADE_DATE:
            prev_close = float(row["close"])
            break
    if not prev_close:
        return None
    eq_rows = await hc.fetch_upstox_intraday_1m(eq_key, token)
    eq_bars = _to_bars(eq_rows)
    if not eq_bars:
        return None

    if sym not in book._today_0915_oi:
        await book._compute_oi_regime_side(sym)
    yday_oi = book._prev_day_last_tick_oi.get(sym)
    if not yday_oi:
        return None
    resolved = await book._resolve_futures_key_and_token(sym)
    if resolved is None:
        return None
    fut_key, _tok = resolved
    fut_rows = await hc.fetch_upstox_intraday_1m(fut_key, token)
    oi_readings = []
    for r in fut_rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        ts = ts.astimezone(IST)
        oi = r.get("oi")
        if not oi:
            continue
        change_pct = (float(oi) - yday_oi) / yday_oi * 100.0
        oi_readings.append((ts.replace(second=0, microsecond=0), change_pct))
    if not oi_readings:
        return None

    return {"eq_bars": eq_bars, "prev_close": prev_close, "oi_readings": oi_readings}


def _find_trigger(cache, price_pct):
    for b in cache["eq_bars"]:
        pchange = (b.close - cache["prev_close"]) / cache["prev_close"] * 100.0
        if pchange >= price_pct or pchange <= -price_pct:
            return ("CALL" if pchange > 0 else "PUT"), b.ts
    return None, None


def _find_oi_confirm(cache, trigger_ts, oi_pct):
    trig_floor = trigger_ts.replace(second=0, microsecond=0)
    for ts, change_pct in cache["oi_readings"]:
        if ts < trig_floor:
            continue
        if change_pct > oi_pct:
            return ts
    return None


def _find_vwap_retest(sym, cache, side, after_ts, window_min):
    tracker = RollingVwapRetestTracker(window_min=window_min)
    vwap_state = VwapState()
    for b in cache["eq_bars"]:
        typical = (b.high + b.low + b.close) / 3.0
        vwap_state.update(sym, typical, 1.0)
        vwap = vwap_state.current(sym)
        if vwap is None:
            continue
        bar_ts = b.ts.replace(second=0, microsecond=0)
        fired = tracker.check(side, bar_ts, b.close, vwap)
        if fired and bar_ts >= after_ts.replace(second=0, microsecond=0):
            return bar_ts, b.close
    return None, None


def _proxy_pnl(cache, side, entry_ts, entry_spot):
    """Underlying spot-price delta entry->EOD, signed for side -- a FAST
    proxy for whether the direction worked, NOT real option premium P&L.
    Used only to rank combos for a follow-up real-premium validation."""
    eod = None
    for b in cache["eq_bars"]:
        if b.ts.time() <= datetime.strptime(EOD_TIME, "%H:%M").time():
            eod = b.close
    if eod is None:
        return None
    raw = eod - entry_spot
    return raw if side == "CALL" else -raw


async def main():
    token = _access_token()
    bus = _NullBus()
    book = OiOrbScreenerStrategy(
        bus, cfg=None, client_id=CLIENT_ID, binding_id=BINDING_ID,
        lot_multiplier=1, product_type="MIS", squareoff_time="15:15",
        strategy_name="oi_orb_screener_top20",
    )

    print("=" * 130)
    print("OI-ORB Screener -- 2026-09-16 FULL THRESHOLD SWEEP (price x OI x VWAP-window)")
    print(f"Baseline (already real-option-validated): 2.0%/3.0%/15min -> {BASELINE_TRADES} trades, "
          f"{BASELINE_REAL_PNL:+.2f} REAL pts")
    print("Grid uses a fast spot-delta PROXY P&L (entry->EOD, signed for side), NOT real option "
          "premium -- for ranking only. Any promising combo needs its own real-premium validation "
          "pass before being trusted, same as every other tuning result in this codebase.")
    print("=" * 130)
    print("\nFetching real per-symbol data (cached once, reused across all 64 combos)...")

    cache = {}
    for sym in UNIVERSE:
        c = await _fetch_symbol_cache(book, sym, token)
        if c is not None:
            cache[sym] = c
    print(f"Cached {len(cache)}/{len(UNIVERSE)} symbols with usable real data.\n")

    grid = {}
    for price_pct in PRICE_GRID:
        for oi_pct in OI_GRID:
            for window_min in WINDOW_GRID:
                trades = 0
                proxy_total = 0.0
                for sym, c in cache.items():
                    side, trig_ts = _find_trigger(c, price_pct)
                    if side is None:
                        continue
                    confirm_ts = _find_oi_confirm(c, trig_ts, oi_pct)
                    if confirm_ts is None:
                        continue
                    fire_ts, fire_price = _find_vwap_retest(sym, c, side, confirm_ts, window_min)
                    if fire_ts is None:
                        continue
                    pnl = _proxy_pnl(c, side, fire_ts, fire_price)
                    if pnl is None:
                        continue
                    trades += 1
                    proxy_total += pnl
                grid[(price_pct, oi_pct, window_min)] = (trades, proxy_total)

    print(f"{'price%':>8} {'OI%':>6} {'window':>7} {'trades':>7} {'proxy pnl (spot pts)':>22}")
    print("-" * 60)
    for (price_pct, oi_pct, window_min), (trades, proxy_total) in sorted(
            grid.items(), key=lambda kv: kv[0]):
        marker = "  <- baseline" if (price_pct, oi_pct, window_min) == (2.0, 3.0, 15.0) else ""
        print(f"{price_pct:8.1f} {oi_pct:6.1f} {window_min:7.0f} {trades:7d} {proxy_total:22.2f}{marker}")

    best = max(grid.items(), key=lambda kv: kv[1][1])
    print("\n" + "=" * 130)
    (bp, bo, bw), (bt, bpnl) = best
    print(f"BEST BY PROXY PNL: price={bp}%  OI={bo}%  window={bw}min  -> {bt} trades, "
          f"proxy={bpnl:+.2f} spot-pts")
    print("This is a RANKING signal only (spot points, not real option premium) -- if this combo "
          "differs materially from the 2.0/3.0/15 baseline, it needs its own real-premium backtest "
          "(like the already-validated baseline got) before being trusted or adopted.")
    print("CAVEAT: single real day (n=1) for every cell -- a 64-combo grid on one day will always "
          "have noisy winners; don't over-fit to today's specific data.")
    print("=" * 130)


if __name__ == "__main__":
    asyncio.run(main())
