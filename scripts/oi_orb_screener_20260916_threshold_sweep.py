"""
scripts/oi_orb_screener_20260916_threshold_sweep.py

Real-data parameter sweep, 2026-09-16, direct user spec: "2 things to
optimise the future oi percentage value and 2 percentage" -- tests a grid
of (PRICE_MOVE_MIN_PCT, OI_REGIME_DECREASE_MAX_PCT) combinations against
the REAL full 20-stock top20 OI-spurt universe for today (not just the 9
stocks that already passed today's live 2% filter), reusing the REAL
OiOrbScreenerStrategy._compute_oi_regime_side() (only its threshold config
varied per grid cell, never reimplemented) and the REAL historical VWAP-
retest + HA/StochRSI exit simulation from oi_orb_screener_20260916_
oi_regime_whatif.py.

Universe source: data/oi_orb_screener.db's oi_spurt_history table, the
LAST real poll of the day (2026-09-16T15:29:23+05:30) -- the only table
that was actually populated with all 20 ranked candidates + real
price_change_pct throughout today (oi_orb_top20_daily_scan, which would
normally hold this, only ever writes during the single morning scan and
today's many restarts all happened after that window, so it's empty).

Grid:
  PRICE_MOVE_MIN_PCT (step 1, the shortlist filter): 1.0, 1.5, 2.0, 2.5, 3.0
  OI_REGIME_DECREASE_MAX_PCT (step 2, the DECREASING/INCREASING cutoff,
  today's live "<=X% -> DECREASING, >X% -> INCREASING" rule with the
  NEUTRAL band already removed): -3, -4, -5 (today's live value), -6, -7, -8

For each grid cell: filter the 20-symbol universe by the price-move
threshold, gate the survivors through the REAL _compute_oi_regime_side()
(cfg["OI_REGIME_DECREASE_MAX_PCT"] swapped per cell, OI data + yesterday's
candle direction fetched ONCE per symbol and cached/reused across every
cell -- never re-fetched), then reconstruct REAL P&L per resulting
(symbol, side) pair via the real historical_rolling_retest_check + HA/
StochRSI exit replay (also cached per (symbol, side) so a pair reused
across multiple cells is only backtested once).

MUST run on EC2 (real Upstox2 access token + real intraday history needed).

Usage: python scripts/oi_orb_screener_20260916_threshold_sweep.py
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer import historical_candles as hc
from data_layer.client_db import ClientDB
from data_layer.instrument_registry import REGISTRY
from strategies.core.trap_zone_utils import Bar
from strategies.core.candle_indicators import (
    to_heikin_ashi, to_n_min_bars, compute_stoch_rsi, ha_stoch_shape_exit_signal,
)
from strategies.oi_orb_screener import stock_resolve, screener
from strategies.oi_orb_screener.engine import OiOrbScreenerStrategy

_MIN_WARM_1M_BARS = 400
CLIENT_ID = "ssrajpal2001"
BINDING_ID = "UPSTOX"
TRADE_DATE = "2026-09-16"
EOD_TIME = "15:15"
RSI_PERIOD, STOCH_PERIOD, SMOOTH = 9, 9, 3

PRICE_MOVE_GRID = [1.0, 1.5, 2.0, 2.5, 3.0]
OI_REGIME_GRID = [-3.0, -4.0, -5.0, -6.0, -7.0, -8.0]

# Real 20-stock universe, last poll of today (2026-09-16T15:29:23+05:30),
# from data/oi_orb_screener.db's oi_spurt_history table (see module
# docstring for why this table, not oi_orb_top20_daily_scan).
UNIVERSE = [
    ("PATANJALI", 7.43), ("ASTRAL", 0.28), ("POLICYBZR", 5.47),
    ("PREMIERENE", -4.90), ("PAYTM", 3.64), ("BLUESTARCO", 1.56),
    ("OFSS", -2.27), ("SOLARINDS", -1.82), ("BAJAJHLDNG", -1.01),
    ("WAAREEENER", -1.99), ("YESBANK", 1.21), ("LAURUSLABS", -1.31),
    ("NESTLEIND", 1.63), ("BSE", -2.18), ("TCS", -3.02), ("NYKAA", -2.14),
    ("MFSL", 3.83), ("BOSCHLTD", 1.25), ("SHRIRAMFIN", -0.08), ("COFORGE", -0.37),
]


class _NullBus:
    def subscribe(self, topic):
        return None

    def unsubscribe(self, topic, q):
        pass

    async def publish(self, topic, event):
        pass


def _access_token() -> str:
    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox2")
    if creds and creds.get("access_token"):
        return creds["access_token"]
    raise RuntimeError("No upstox2 feeder access_token found -- run this on EC2.")


def _to_bars(rows: List[dict]) -> List[Bar]:
    out = []
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        out.append(Bar(ts=ts.astimezone(IST), open=float(r["open"]), high=float(r["high"]),
                        low=float(r["low"]), close=float(r["close"])))
    return out


@dataclass
class WhatIfResult:
    symbol: str
    side: str
    entry_ts: Optional[str]
    entry_price: Optional[float]
    exit_ts: Optional[str]
    exit_price: Optional[float]
    reason: str
    pnl: Optional[float]


async def _simulate_one(symbol: str, side: str, token: str) -> WhatIfResult:
    try:
        results = await asyncio.to_thread(
            screener.historical_rolling_retest_check, {symbol: side}, screener.CONFIG, 15.0)
    except Exception:
        results = {}
    r = results.get(symbol)
    if not r or not r.get("fired"):
        return WhatIfResult(symbol, side, None, None, None, None,
                             "no genuine VWAP-retest completed today (real intraday data)", None)
    entry_ts = datetime.combine(date.fromisoformat(TRADE_DATE),
                                 datetime.strptime(r["fire_ts"], "%H:%M").time(), tzinfo=IST)
    entry_price_spot = r["fire_price"]

    eq_key = stock_resolve.resolve_eq_instrument_key(symbol)
    if not eq_key:
        return WhatIfResult(symbol, side, entry_ts.strftime("%H:%M:%S"), None, None, None,
                             "NO_EQ_KEY -- could not resolve spot instrument key", None)
    warm_rows = await hc.fetch_upstox_warm_1m(eq_key, token, min_bars=_MIN_WARM_1M_BARS)
    all_bars = _to_bars(warm_rows)
    entry_floor = entry_ts.replace(second=0, microsecond=0)
    post_entry_bars = [b for b in all_bars if b.ts >= entry_floor]
    if len(all_bars) < 15 or not post_entry_bars:
        return WhatIfResult(symbol, side, entry_ts.strftime("%H:%M:%S"), None, None, None,
                             f"INSUFFICIENT_DATA -- only {len(all_bars)} seeded bars, "
                             f"{len(post_entry_bars)} at/after entry", None)

    exit_signal_ts = None
    last_evaluated = None
    for i in range(15, len(all_bars) + 1):
        window = all_bars[:i]
        cur_ts = window[-1].ts
        ha_1m = to_heikin_ashi(window)
        ha_15m = to_n_min_bars(ha_1m, 15)
        if not ha_15m:
            continue
        last_bar = ha_15m[-1]
        if cur_ts < last_bar.ts + timedelta(minutes=15):
            ha_15m = ha_15m[:-1]
        if not ha_15m:
            continue
        latest = ha_15m[-1]
        if latest.ts < entry_floor:
            continue
        if last_evaluated == latest.ts:
            continue
        last_evaluated = latest.ts
        closes = [b.close for b in ha_15m]
        k, d = compute_stoch_rsi(closes, RSI_PERIOD, STOCH_PERIOD, SMOOTH)
        if ha_stoch_shape_exit_signal(latest, k[-1], d[-1], side, inclusive=True):
            exit_signal_ts = cur_ts
            break

    if exit_signal_ts is None:
        exit_signal_ts = datetime.combine(date.fromisoformat(TRADE_DATE),
                                           datetime.strptime(EOD_TIME, "%H:%M").time(), tzinfo=IST)
        reason = "eod_squareoff (HA+StochRSI never fired)"
    else:
        reason = "ha_stoch_exit"

    entry_price = None
    exit_price = None
    try:
        opt_type = "CE" if side == "CALL" else "PE"
        contract = await stock_resolve.resolve_contract_async(symbol, entry_price_spot, opt_type)
        if contract is None:
            reason += " -- could not resolve a real tradable contract"
        else:
            opt_rows = await hc.fetch_upstox_intraday_1m(contract.upstox_key, token)
            opt_bars = _to_bars(opt_rows)
            entry_candidates = [b for b in opt_bars if b.ts <= entry_ts]
            if entry_candidates:
                entry_price = entry_candidates[-1].close
            exit_candidates = [b for b in opt_bars if b.ts <= exit_signal_ts]
            if exit_candidates:
                exit_price = exit_candidates[-1].close
    except Exception as exc:
        reason += f" -- option resolution failed ({exc})"

    pnl_pts = None
    if entry_price is not None and exit_price is not None:
        raw = exit_price - entry_price
        pnl_pts = round(raw if side == "CALL" else -raw, 2)
    else:
        reason += " -- option premium history unavailable, P&L not reconstructed"

    return WhatIfResult(symbol, side, entry_ts.strftime("%H:%M:%S"), entry_price,
                         exit_signal_ts.strftime("%H:%M:%S"), exit_price, reason, pnl_pts)


async def main():
    token = _access_token()
    print("=" * 118)
    print(f"OI-ORB Screener (top20) -- {TRADE_DATE} THRESHOLD SWEEP: real 20-stock universe, "
          f"PRICE_MOVE_MIN_PCT x OI_REGIME_DECREASE_MAX_PCT")
    print(f"Universe ({len(UNIVERSE)}): {[s for s, _ in UNIVERSE]}")
    print("=" * 118)

    bus = _NullBus()
    book = OiOrbScreenerStrategy(
        bus, cfg=None, client_id=CLIENT_ID, binding_id=BINDING_ID,
        lot_multiplier=1, product_type="MIS", squareoff_time="15:15",
        strategy_name="oi_orb_screener_top20",
    )
    book._screener_cfg["OI_REGIME_GATE_ENABLED"] = True
    book._screener_cfg["IGNORE_TIME_WINDOWS"] = True
    for sym, pch in UNIVERSE:
        book._shortlist_pchange[sym] = pch

    # ── Step 1: warm the REAL futures-OI + yesterday's-candle-direction
    # caches ONCE per symbol (regardless of grid), by calling the real
    # _compute_oi_regime_side() with today's live threshold (-5%) first --
    # its own internal caching (self._today_0915_oi/_prev_day_last_tick_oi)
    # already prevents re-fetching OI on later calls. Yesterday's candle
    # direction is NOT cached by the real class (fetched fresh every call),
    # so we memoize it ourselves here to avoid 20 x 6 redundant REST calls.
    _candle_dir_cache: Dict[str, Optional[str]] = {}
    _real_yesterday_candle_direction = book._yesterday_candle_direction

    async def _memoized_yesterday_candle_direction(fut_key, tok):
        key = fut_key
        if key not in _candle_dir_cache:
            _candle_dir_cache[key] = await _real_yesterday_candle_direction(fut_key, tok)
        return _candle_dir_cache[key]
    book._yesterday_candle_direction = _memoized_yesterday_candle_direction

    print("\nWarming real futures-OI data for all 20 symbols (one-time fetch)...")
    for sym, _ in UNIVERSE:
        await book._compute_oi_regime_side(sym)
        oi = book._today_0915_oi.get(sym)
        yoi = book._prev_day_last_tick_oi.get(sym)
        chg = round((oi - yoi) / yoi * 100.0, 2) if (oi is not None and yoi) else None
        print(f"  {sym:12s} today_0915={oi} yday_1539={yoi} change={chg}")

    # ── Step 2: sweep the grid, reusing cached OI/candle data every time ──
    backtest_cache: Dict[Tuple[str, str], WhatIfResult] = {}
    grid_results = {}

    for price_thr in PRICE_MOVE_GRID:
        for oi_thr in OI_REGIME_GRID:
            book._screener_cfg["OI_REGIME_DECREASE_MAX_PCT"] = oi_thr
            book._oi_regime_computed = set()
            book._oi_regime_side = {}

            survivors = [(sym, pch) for sym, pch in UNIVERSE if abs(pch) >= price_thr]
            cell_trades: List[WhatIfResult] = []
            for sym, _ in survivors:
                side = await book._compute_oi_regime_side(sym)
                if side is None:
                    continue
                key = (sym, side)
                if key not in backtest_cache:
                    backtest_cache[key] = await _simulate_one(sym, side, token)
                cell_trades.append(backtest_cache[key])

            total_pnl = sum(t.pnl for t in cell_trades if t.pnl is not None)
            fired = sum(1 for t in cell_trades if t.entry_ts is not None)
            reconstructed = sum(1 for t in cell_trades if t.pnl is not None)
            grid_results[(price_thr, oi_thr)] = {
                "universe_size": len(survivors), "passed_gate": len(cell_trades),
                "fired": fired, "reconstructed": reconstructed, "total_pnl": total_pnl,
            }

    # ── Report ──
    print("\n" + "=" * 118)
    print(f"{'price%':>7} {'oi%':>6} {'univ':>5} {'gate-pass':>10} {'fired':>6} {'recon':>6} {'pnl(pts)':>10}")
    print("-" * 118)
    best_cell = None
    for price_thr in PRICE_MOVE_GRID:
        for oi_thr in OI_REGIME_GRID:
            g = grid_results[(price_thr, oi_thr)]
            marker = " <== today's live config" if (price_thr == 2.0 and oi_thr == -5.0) else ""
            print(f"{price_thr:7.1f} {oi_thr:6.1f} {g['universe_size']:5d} {g['passed_gate']:10d} "
                  f"{g['fired']:6d} {g['reconstructed']:6d} {g['total_pnl']:10.2f}{marker}")
            if best_cell is None or g["total_pnl"] > grid_results[best_cell]["total_pnl"]:
                best_cell = (price_thr, oi_thr)
    print("-" * 118)
    bp, bo = best_cell
    print(f"BEST cell today: price_move>={bp}% oi_regime_cutoff={bo}% -> "
          f"pnl={grid_results[best_cell]['total_pnl']:.2f} pts "
          f"({grid_results[best_cell]['reconstructed']} trades reconstructed)")
    print("=" * 118)
    print("CAVEAT: this is a SINGLE real trading day (n=1) -- directionally informative, "
          "NOT a statistically validated optimum. Do not treat the 'best' cell above as a "
          "final parameter choice without repeating this sweep across many more real days.")
    print("=" * 118)


if __name__ == "__main__":
    asyncio.run(main())
