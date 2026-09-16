"""
scripts/oi_orb_screener_today_whatif_ha_stoch.py

Real-data "what-if" backtest, 2026-09-07, direct user spec: "do a backtest
for today trades which happened in oi scanner" -- specifically, what would
have happened to today's REAL 10 OI-ORB Screener trades if the exit logic
had been HA+StochRSI-ONLY the whole day (today's fix, commit 44fcc78),
instead of the mix of hard_risk_cap/option_target/option_sl/eod_squareoff
that actually fired (some trades closed BEFORE the fix deployed at ~15:04).

2026-09-07 CORRECTION, direct user spec: "issue is we started the
application after 09:47, assume it started at 09:25 and all stocks came
at that time -- after that show backtest." Today's real entry times
(10:42 onward) were themselves artifacts of this session's own repeated
restarts, not genuine market timing -- the real VWAP retest for most of
these stocks happened much earlier, before this book ever started
watching. Each trade's entry point is now reconstructed via
screener.historical_vwap_retest_check() -- the SAME real function the
live app's own "check history for an already-completed retest" feature
uses (real Yahoo Finance intraday data, no token needed) -- to find each
stock's TRUE real-market retest moment, as if the app had genuinely
started fresh at market open. HA+StochRSI is then replayed from THAT
earlier point using real Upstox spot data, giving the indicator its full
intended runway instead of the restart-truncated one.

Uses the EXACT same functions the live engine imports (strategies.core.
candle_indicators: to_heikin_ashi, to_n_min_bars, compute_stoch_rsi,
ha_stoch_shape_exit_signal) against REAL 1-min intraday spot data fetched
from Upstox -- never a reimplementation, per this repo's own
feedback_backtest_drive_real_class discipline. Entry/exit signal is
evaluated on the STOCK'S OWN SPOT price (never option premium, matching
_ha_stoch_check_exit's own real behavior); P&L is then reconstructed using
the OPTION's own real intraday premium at the reconstructed entry/exit
minutes.

MUST run on EC2 (or anywhere with a real, valid Upstox access token) --
pulls today's real trade list from data/oi_orb_screener.db and a real
access token from data/clients.db (ssrajpal2001's UPSTOX binding). Cannot
run standalone on a dev machine with no live credentials.

CAVEAT: historical_vwap_retest_check() calls yfinance with period="1d",
which always returns the CURRENT day's data, not a specific historical
date -- this script's "true retest time" reconstruction is only accurate
if run on 2026-09-07 itself (today). Running it on a later date would
silently reconstruct against the WRONG day's data.

Usage: python scripts/oi_orb_screener_today_whatif_ha_stoch.py
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional

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

# 21 fifteen-min bars (~315 one-min bars) covers RSI(9)+Stoch(9)+smooth(3)'s
# real warm-up need with margin -- forces fetch_upstox_warm_1m to backfill
# with the previous trading day's tail whenever today alone (from market
# open to now) doesn't have that many 1-min bars yet.
_MIN_WARM_1M_BARS = 400

CLIENT_ID = "ssrajpal2001"
TRADE_DATE = "2026-09-07"
EOD_TIME = "15:15"
RSI_PERIOD, STOCH_PERIOD, SMOOTH = 9, 9, 3

# Today's real trades -- pulled directly from data/oi_orb_screener.db's
# positions table (see the SQL query run alongside this script).
REAL_TRADES = [
    # (symbol, side, strike, expiry, qty, entry_ts, entry_price, real_exit_ts, real_exit_price, real_reason, real_pnl)
    ("SOLARINDS",  "CALL", 21500, "2026-09-29", 50,   "10:42:37", 1118.30, "11:09:40", 1061.80, "hard_risk_cap", -2825.00),
    ("MANAPPURAM", "PUT",  330,   "2026-09-29", 3000, "10:42:37", 11.50,   "12:26:48", 10.40,   "hard_risk_cap", -3300.00),
    ("KEI",        "PUT",  4800,  "2026-09-29", 175,  "10:54:25", 176.85,  "15:15:03", 187.00,  "eod_squareoff", 1776.25),
    ("ICICIPRULI", "PUT",  485,   "2026-09-29", 925,  "11:09:03", 12.55,   "15:15:03", 15.90,   "eod_squareoff", 3098.75),
    ("LTM",        "PUT",  4450,  "2026-09-29", 150,  "12:13:35", 153.55,  "12:25:00", 155.05,  "option_sl", 225.00),
    ("MANAPPURAM", "PUT",  330,   "2026-09-29", 3000, "12:27:04", 10.40,   "14:25:13", 10.90,   "option_target", 1500.00),
    ("INFY",       "PUT",  1100,  "2026-09-29", 400,  "13:46:38", 31.40,   "15:15:03", 32.20,   "eod_squareoff", 320.00),
    ("WIPRO",      "PUT",  175,   "2026-09-29", 3000, "14:06:46", 5.75,    "15:15:03", 5.73,    "eod_squareoff", -60.00),
    ("MANAPPURAM", "PUT",  330,   "2026-09-29", 3000, "14:54:06", 11.45,   "15:05:56", 10.75,   "hard_risk_cap", -2100.00),
    ("VMM",        "PUT",  105,   "2026-09-29", 4850, "14:59:09", 3.78,    "15:15:03", 3.78,    "eod_squareoff", 0.00),
]


def _access_token() -> str:
    db = ClientDB()
    for b in db.get_bindings_safe_sync(CLIENT_ID):
        if (b.get("provider") or "").lower() == "upstox" and b.get("access_token"):
            return b["access_token"]
    raise RuntimeError("No Upstox access_token found for ssrajpal2001 -- run this on EC2 "
                        "after the day's Upstox re-authentication.")


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
    entry_ts: str
    entry_price: float
    real_exit_ts: str
    real_exit_price: float
    real_reason: str
    real_pnl: float
    whatif_entry_ts: Optional[str]
    whatif_entry_price: Optional[float]
    whatif_exit_ts: Optional[str]
    whatif_exit_price: Optional[float]
    whatif_reason: str
    whatif_pnl: Optional[float]


async def _true_entry_ts(symbol: str, side: str) -> Optional[str]:
    """Real historical VWAP-retest moment for this stock (Yahoo intraday,
    same function the live app's own historical-retest feature uses) --
    the "if the app had started fresh at market open" true entry point,
    independent of when today's restarts happened to notice it."""
    cfg_side = "CALL" if side == "CALL" else "PUT"
    try:
        results = await asyncio.to_thread(screener.historical_vwap_retest_check,
                                           {symbol: cfg_side})
    except Exception:
        return None
    r = results.get(symbol)
    if not r or not r.get("fired"):
        return None
    return r["fire_ts"]   # "HH:MM"


async def _reconstruct_one(symbol, side, strike, expiry_s, qty, entry_ts_s, entry_price,
                            real_exit_ts_s, real_exit_price, real_reason, real_pnl,
                            token: str) -> WhatIfResult:
    # 1. Find the TRUE real-market retest moment (assume-fresh-start-at-
    #    market-open scenario) -- fall back to today's actual (restart-
    #    delayed) entry time if Yahoo has no usable data for this stock.
    true_entry_hhmm = await _true_entry_ts(symbol, side)
    if true_entry_hhmm:
        entry_ts = datetime.combine(date.fromisoformat(TRADE_DATE),
                                     datetime.strptime(true_entry_hhmm, "%H:%M").time(), tzinfo=IST)
        entry_note = "true retest time (assume-fresh-start)"
    else:
        entry_ts = datetime.combine(date.fromisoformat(TRADE_DATE),
                                     datetime.strptime(entry_ts_s, "%H:%M:%S").time(), tzinfo=IST)
        entry_note = "no earlier retest found in real history -- using today's actual entry time"

    # 2. Real spot 1-min data, PRE-WARMED with the previous trading day's
    #    tail when today alone (from market open to now) isn't enough for
    #    RSI(9)+Stoch(9)+smooth(3) on 15-min bars to warm up -- same
    #    prior-day-seeding pattern this codebase already uses for RSI/ROC
    #    elsewhere. NOTE: the live _ha_stoch_check_exit does NOT currently
    #    do this seeding (starts cold from each position's own entry) --
    #    this variant tests a better-seeded version of the mechanic, not
    #    exactly today's live behavior.
    eq_key = stock_resolve.resolve_eq_instrument_key(symbol)
    if not eq_key:
        return WhatIfResult(symbol, side, entry_ts_s, entry_price, real_exit_ts_s,
                             real_exit_price, real_reason, real_pnl, None, None, None, None,
                             "NO_EQ_KEY -- could not resolve spot instrument key", None)
    warm_rows = await hc.fetch_upstox_warm_1m(eq_key, token, min_bars=_MIN_WARM_1M_BARS)
    all_bars = _to_bars(warm_rows)
    # Indicator warms up on the FULL seeded series; the exit-signal EVALUATION
    # loop only starts once we reach the (possibly-earlier) reconstructed entry.
    post_entry_bars = [b for b in all_bars if b.ts >= entry_ts.replace(second=0, microsecond=0)]
    if len(all_bars) < 15 or not post_entry_bars:
        return WhatIfResult(symbol, side, entry_ts_s, entry_price, real_exit_ts_s,
                             real_exit_price, real_reason, real_pnl, None, None, None, None,
                             f"INSUFFICIENT_DATA -- only {len(all_bars)} seeded bars, "
                             f"{len(post_entry_bars)} at/after reconstructed entry", None)

    # 3. Replay the EXACT live signal on the seeded series: HA on 1-min ->
    #    resample 15-min -> StochRSI(9,9,3) -> ha_stoch_shape_exit_signal
    #    (inclusive=True), only on fully-closed 15-min bars, walking forward
    #    exactly like _ha_stoch_check_exit does live -- but only counting a
    #    signal as a real EXIT once its bar closes at/after the reconstructed
    #    entry (seeded history warms the indicator, never triggers an exit
    #    on a position that didn't exist yet).
    entry_floor = entry_ts.replace(second=0, microsecond=0)
    whatif_exit_bar_ts = None
    exit_signal_ts = None
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
            continue   # bar closed before this position existed -- warm-up only
        if whatif_exit_bar_ts == latest.ts:
            continue   # already evaluated this exact closed bar
        whatif_exit_bar_ts = latest.ts
        closes = [b.close for b in ha_15m]
        k, d = compute_stoch_rsi(closes, RSI_PERIOD, STOCH_PERIOD, SMOOTH)
        if ha_stoch_shape_exit_signal(latest, k[-1], d[-1], side, inclusive=True):
            exit_signal_ts = cur_ts
            break

    if exit_signal_ts is None:
        exit_signal_ts = datetime.combine(date.fromisoformat(TRADE_DATE),
                                           datetime.strptime(EOD_TIME, "%H:%M").time(), tzinfo=IST)
        whatif_reason = f"eod_squareoff (HA+StochRSI never fired; entry={entry_note})"
    else:
        whatif_reason = f"ha_stoch_exit (entry={entry_note})"

    # 4. Real option premium at BOTH the reconstructed entry and exit minutes.
    if not REGISTRY.is_loaded(symbol):
        await asyncio.to_thread(REGISTRY.load_sync, symbol)
    opt_key = REGISTRY.get_upstox_key(symbol, date.fromisoformat(expiry_s), strike, "CE" if side == "CALL" else "PE")
    whatif_entry_price = None
    whatif_exit_price = None
    if opt_key:
        opt_rows = await hc.fetch_upstox_intraday_1m(opt_key, token)
        opt_bars = _to_bars(opt_rows)
        entry_candidates = [b for b in opt_bars if b.ts <= entry_ts]
        if entry_candidates:
            whatif_entry_price = entry_candidates[-1].close
        exit_candidates = [b for b in opt_bars if b.ts <= exit_signal_ts]
        if exit_candidates:
            whatif_exit_price = exit_candidates[-1].close

    whatif_pnl = None
    if whatif_entry_price is not None and whatif_exit_price is not None:
        # 2026-09-16 CRITICAL bug fix: a long option (CE or PE) profits
        # purely on its own premium direction, no side-based sign flip --
        # see oi_orb_screener_20260916_live_oi_confirm_backtest.py's
        # _simulate_exit for the full reasoning.
        whatif_pnl = round((whatif_exit_price - whatif_entry_price) * qty, 2)
    else:
        whatif_reason += " -- option premium history unavailable, P&L not reconstructed"

    return WhatIfResult(
        symbol, side, entry_ts_s, entry_price, real_exit_ts_s, real_exit_price, real_reason, real_pnl,
        entry_ts.strftime("%H:%M:%S"), whatif_entry_price,
        exit_signal_ts.strftime("%H:%M:%S"), whatif_exit_price, whatif_reason, whatif_pnl,
    )


async def main():
    token = _access_token()
    print("=" * 110)
    print(f"OI-ORB Screener -- {TRADE_DATE} WHAT-IF backtest: HA+StochRSI-only exit vs. what actually happened")
    print("=" * 110)

    results: List[WhatIfResult] = []
    for symbol, side, strike, expiry, qty, entry_ts, entry_price, exit_ts, exit_price, reason, pnl in REAL_TRADES:
        r = await _reconstruct_one(symbol, side, strike, expiry, qty, entry_ts, entry_price,
                                    exit_ts, exit_price, reason, pnl, token)
        results.append(r)
        print(f"\n{r.symbol} {r.side} qty~real")
        print(f"  REAL:    entry {r.entry_ts} @ {r.entry_price:.2f}  ->  exit {r.real_exit_ts} @ "
              f"{r.real_exit_price:.2f}  reason={r.real_reason}  pnl={r.real_pnl:+.2f}")
        wep = f"{r.whatif_entry_price:.2f}" if r.whatif_entry_price is not None else "n/a"
        wp = f"{r.whatif_exit_price:.2f}" if r.whatif_exit_price is not None else "n/a"
        wpnl = f"{r.whatif_pnl:+.2f}" if r.whatif_pnl is not None else "n/a"
        print(f"  WHAT-IF: entry {r.whatif_entry_ts} @ {wep}  ->  exit {r.whatif_exit_ts} @ "
              f"{wp}  reason={r.whatif_reason}  pnl={wpnl}")

    real_total = sum(r.real_pnl for r in results)
    whatif_total = sum(r.whatif_pnl for r in results if r.whatif_pnl is not None)
    missing = [r.symbol for r in results if r.whatif_pnl is None]

    print("\n" + "=" * 110)
    print(f"REAL total P&L today:     Rs{real_total:+.2f}")
    print(f"WHAT-IF total P&L (HA+StochRSI-only, {len(results) - len(missing)}/{len(results)} trades reconstructed): "
          f"Rs{whatif_total:+.2f}")
    if missing:
        print(f"NOT reconstructed (missing option history): {missing}")
    print("=" * 110)


if __name__ == "__main__":
    asyncio.run(main())
