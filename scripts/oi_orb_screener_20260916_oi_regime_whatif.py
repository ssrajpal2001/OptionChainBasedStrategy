"""
scripts/oi_orb_screener_20260916_oi_regime_whatif.py

Real-data "what-if" backtest, 2026-09-16, direct user spec: "do a backtest
with all stocks which came today in scanner from DB" -- run right after the
fix (commit 49022e5) that closed a real live gap where a symbol could trade
via the historical-retest catch-up replay (_apply_historical_rolling_retest)
WITHOUT ever passing the futures-OI-regime gate (see that commit's own
message for the COFORGE incident this closes).

This answers: "if the FIXED gate had been enforced everywhere, all day, what
would have happened to every stock that made it into today's real shortlist?"

Never reimplements the live strategy's own decision logic (per this repo's
feedback_backtest_drive_real_class discipline) -- constructs a real, headless
OiOrbScreenerStrategy instance and calls its actual _compute_oi_regime_side()
for the gate verdict, and reuses screener.historical_rolling_retest_check()
(the exact top20-mode entry replay the live engine itself uses) for any
symbol the gate allows through. Exit simulation reuses the SAME HA+StochRSI
functions (strategies.core.candle_indicators) the live _ha_stoch_check_exit
uses, same pattern as scripts/oi_orb_screener_today_whatif_ha_stoch.py
(2026-09-07's equivalent "today whatif" script for this strategy).

Symbol source: today's real shortlist as recorded to data/oi_orb_screener.db
(client=ssrajpal2001, binding=UPSTOX, trade_date=2026-09-16, strategy=
oi_orb_screener_top20) -- queried directly from that DB when available. Since
this deployment's DB gets wiped as part of applying today's fix (same
fresh-start pattern used throughout this session), a hardcoded fallback list
mirrors the EXACT real row recorded to that table earlier today (see the
live log line this was sourced from, quoted below) so the backtest still
uses genuine today's-DB data even after the wipe.

Real shortlist log line (2026-09-16 12:16:16, oiorb_ssrajpal2001_UPSTOX_
20260916.log): "shortlist ready (9): SOLARINDS(px=-2.36%,oi_spurt=15.63%),
PREMIERENE(px=-5.37%,oi_spurt=11.91%), PATANJALI(px=+4.30%,oi_spurt=8.60%),
NESTLEIND(px=+2.02%,oi_spurt=7.34%), MARICO(px=+3.27%,oi_spurt=5.93%),
LAURUSLABS(px=-2.75%,oi_spurt=5.61%), COFORGE(px=-2.07%,oi_spurt=5.35%),
COLPAL(px=+3.80%,oi_spurt=4.01%), NYKAA(px=-3.49%,oi_spurt=4.01%)"

MUST run on EC2 (or anywhere with a real, valid Upstox2 access token) --
pulls a real access token from data/clients.db (the "upstox2" feeder, the
SAME dedicated credential the live gate itself uses -- see
_resolve_futures_key_and_token in engine.py). Cannot run standalone on a dev
machine with no live credentials.

Usage: python scripts/oi_orb_screener_20260916_oi_regime_whatif.py
"""
from __future__ import annotations

import asyncio
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional

sys.path.insert(0, ".")

from config.global_config import IST, Topic
from data_layer import historical_candles as hc
from data_layer.client_db import ClientDB
from data_layer.instrument_registry import REGISTRY
from strategies.core.trap_zone_utils import Bar
from strategies.core.candle_indicators import (
    to_heikin_ashi, to_n_min_bars, compute_stoch_rsi, ha_stoch_shape_exit_signal,
)
from strategies.oi_orb_screener import stock_resolve, screener, store
from strategies.oi_orb_screener.engine import OiOrbScreenerStrategy

_MIN_WARM_1M_BARS = 400
CLIENT_ID = "ssrajpal2001"
BINDING_ID = "UPSTOX"
TRADE_DATE = "2026-09-16"
EOD_TIME = "15:15"
RSI_PERIOD, STOCH_PERIOD, SMOOTH = 9, 9, 3

# Fallback -- the exact real row this session's own live log recorded to
# data/oi_orb_screener.db's shortlist table before the fix-deploy wipe (see
# module docstring for the source log line). Used only if the DB query below
# finds nothing for today (e.g. run after a wipe, before a fresh same-day
# re-scan has repopulated it).
_FALLBACK_SHORTLIST = [
    ("SOLARINDS", -2.36), ("PREMIERENE", -5.37), ("PATANJALI", 4.30),
    ("NESTLEIND", 2.02), ("MARICO", 3.27), ("LAURUSLABS", -2.75),
    ("COFORGE", -2.07), ("COLPAL", 3.80), ("NYKAA", -3.49),
]


class _NullBus:
    """Just enough of the real EventBus interface for a headless book that
    never calls start()/subscribes to anything -- only _compute_oi_regime_side
    is ever invoked, which touches no bus/topic at all."""

    def subscribe(self, topic):
        return None

    def unsubscribe(self, topic, q):
        pass

    async def publish(self, topic, event):
        pass


def _todays_real_shortlist() -> List[tuple]:
    try:
        con = sqlite3.connect(store._DB_PATH)
        rows = con.execute(
            "SELECT symbol, price_change_pct FROM shortlist "
            "WHERE client_id=? AND binding_id=? AND trade_date=? ORDER BY id",
            (CLIENT_ID, BINDING_ID, TRADE_DATE),
        ).fetchall()
        con.close()
    except Exception:
        rows = []
    if rows:
        return [(sym, pch if pch is not None else 0.0) for sym, pch in rows]
    print("(no rows in data/oi_orb_screener.db for today -- using the fallback "
          "list sourced from this session's own real shortlist log line)")
    return _FALLBACK_SHORTLIST


def _access_token() -> str:
    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox2")
    if creds and creds.get("access_token"):
        return creds["access_token"]
    raise RuntimeError("No upstox2 feeder access_token found -- run this on EC2 "
                        "after today's feeder re-authentication.")


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
class RegimeResult:
    symbol: str
    pchange: float
    today_0915_oi: Optional[float]
    yday_1539_oi: Optional[float]
    oi_change_pct: Optional[float]
    side: Optional[str]   # None == blocked/removed from pool


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


async def _gate_one(book: OiOrbScreenerStrategy, symbol: str, pchange: float) -> RegimeResult:
    book._shortlist_pchange[symbol] = pchange
    side = await book._compute_oi_regime_side(symbol)
    return RegimeResult(
        symbol=symbol, pchange=pchange,
        today_0915_oi=book._today_0915_oi.get(symbol),
        yday_1539_oi=book._prev_day_last_tick_oi.get(symbol),
        oi_change_pct=(
            round((book._today_0915_oi[symbol] - book._prev_day_last_tick_oi[symbol])
                  / book._prev_day_last_tick_oi[symbol] * 100.0, 2)
            if symbol in book._today_0915_oi and book._prev_day_last_tick_oi.get(symbol)
            else None
        ),
        side=side,
    )


async def _simulate_one(symbol: str, side: str, token: str) -> WhatIfResult:
    # 1. True retest moment via the REAL top20 replay function (real Upstox
    #    intraday history, same mechanic the live engine's own historical
    #    catch-up path uses -- now correctly gated, this call represents
    #    what it's allowed to fire once the gate has already passed).
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

    # 2. Real spot 1-min data, pre-warmed for RSI(9)+Stoch(9)+smooth(3).
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

    # 3. Replay HA+StochRSI(9,9,3) on 15-min bars, exactly like the live
    #    _ha_stoch_check_exit -- only counting a signal at/after entry.
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

    # 4. Real option premium at entry/exit minutes -- strike = ATM (raw
    #    strike is the live spot trigger price, per STRIKE_OTM_PCT=0.0),
    #    resolved via the SAME real stock_resolve.resolve_contract_async
    #    the live engine itself calls, never a hand-rolled strike/expiry
    #    guess.
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
        # 2026-09-16 CRITICAL bug fix: a long option (CE or PE) profits
        # purely on its own premium direction, no side-based sign flip --
        # see oi_orb_screener_20260916_live_oi_confirm_backtest.py's
        # _simulate_exit for the full reasoning.
        pnl_pts = round(exit_price - entry_price, 2)
    else:
        reason += " -- option premium history unavailable, P&L not reconstructed (points-only)"

    return WhatIfResult(symbol, side, entry_ts.strftime("%H:%M:%S"), entry_price,
                         exit_signal_ts.strftime("%H:%M:%S"), exit_price, reason, pnl_pts)


async def main():
    token = _access_token()
    shortlist = _todays_real_shortlist()

    print("=" * 110)
    print(f"OI-ORB Screener (top20) -- {TRADE_DATE} WHAT-IF: fixed OI-regime gate applied to "
          f"every real shortlisted stock, all day")
    print(f"Symbols ({len(shortlist)}): {[s for s, _ in shortlist]}")
    print("=" * 110)

    bus = _NullBus()
    book = OiOrbScreenerStrategy(
        bus, cfg=None, client_id=CLIENT_ID, binding_id=BINDING_ID,
        lot_multiplier=1, product_type="MIS", squareoff_time="15:15",
        strategy_name="oi_orb_screener_top20",
    )
    book._screener_cfg["OI_REGIME_GATE_ENABLED"] = True
    book._screener_cfg["IGNORE_TIME_WINDOWS"] = True   # allow the gate to compute regardless of wall clock

    regime_results: List[RegimeResult] = []
    for sym, pch in shortlist:
        rr = await _gate_one(book, sym, pch)
        regime_results.append(rr)
        oi_str = (f"today_0915={rr.today_0915_oi:.0f} yday_1539={rr.yday_1539_oi:.0f} "
                  f"change={rr.oi_change_pct:+.2f}%"
                  if rr.oi_change_pct is not None else "OI data unavailable")
        print(f"\n{sym:12s} pChange={rr.pchange:+.2f}%  {oi_str}  -> "
              f"{'side=' + rr.side if rr.side else 'BLOCKED (removed from pool)'}")

    passed = [rr for rr in regime_results if rr.side is not None]
    blocked = [rr for rr in regime_results if rr.side is None]

    print("\n" + "-" * 110)
    print(f"Gate result: {len(passed)}/{len(regime_results)} stocks pass the FIXED OI-regime gate "
          f"today; {len(blocked)} blocked.")
    print("-" * 110)

    whatif_results: List[WhatIfResult] = []
    for rr in passed:
        wr = await _simulate_one(rr.symbol, rr.side, token)
        whatif_results.append(wr)
        ep = f"{wr.entry_price:.2f}" if wr.entry_price is not None else "n/a"
        xp = f"{wr.exit_price:.2f}" if wr.exit_price is not None else "n/a"
        pnl = f"{wr.pnl:+.2f}pts" if wr.pnl is not None else "n/a"
        print(f"\n{wr.symbol} {wr.side}: entry {wr.entry_ts or 'n/a'} @ {ep}  ->  "
              f"exit {wr.exit_ts or 'n/a'} @ {xp}  reason={wr.reason}  pnl={pnl}")

    total_pnl_pts = sum(w.pnl for w in whatif_results if w.pnl is not None)
    reconstructed = sum(1 for w in whatif_results if w.pnl is not None)

    print("\n" + "=" * 110)
    print(f"Stocks that fired a trade under the FIXED gate: {len(whatif_results)}")
    print(f"P&L reconstructed for {reconstructed}/{len(whatif_results)} of them")
    print(f"TOTAL what-if P&L today (option points, summed across stocks -- NOT rupees, "
          f"lot sizes differ per stock): {total_pnl_pts:+.2f} pts")
    print("=" * 110)


if __name__ == "__main__":
    asyncio.run(main())
