"""
scripts/oi_orb_screener_today_whatif_ha_stoch.py

Real-data "what-if" backtest, 2026-09-07, direct user spec: "do a backtest
for today trades which happened in oi scanner" -- specifically, what would
have happened to today's REAL 10 OI-ORB Screener trades if the exit logic
had been HA+StochRSI-ONLY the whole day (today's fix, commit 44fcc78),
instead of the mix of hard_risk_cap/option_target/option_sl/eod_squareoff
that actually fired (some trades closed BEFORE the fix deployed at ~15:04).

Uses the EXACT same functions the live engine imports (strategies.core.
candle_indicators: to_heikin_ashi, to_n_min_bars, compute_stoch_rsi,
ha_stoch_shape_exit_signal) against REAL 1-min intraday spot data fetched
from Upstox -- never a reimplementation, per this repo's own
feedback_backtest_drive_real_class discipline. Entry/exit signal is
evaluated on the STOCK'S OWN SPOT price (never option premium, matching
_ha_stoch_check_exit's own real behavior); P&L is then reconstructed using
the OPTION's own real intraday premium at the reconstructed exit minute.

MUST run on EC2 (or anywhere with a real, valid Upstox access token) --
pulls today's real trade list from data/oi_orb_screener.db and a real
access token from data/clients.db (ssrajpal2001's UPSTOX binding). Cannot
run standalone on a dev machine with no live credentials.

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
from strategies.oi_orb_screener import stock_resolve

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
    whatif_exit_ts: Optional[str]
    whatif_exit_price: Optional[float]
    whatif_reason: str
    whatif_pnl: Optional[float]


async def _reconstruct_one(symbol, side, strike, expiry_s, qty, entry_ts_s, entry_price,
                            real_exit_ts_s, real_exit_price, real_reason, real_pnl,
                            token: str) -> WhatIfResult:
    entry_ts = datetime.combine(date.fromisoformat(TRADE_DATE),
                                 datetime.strptime(entry_ts_s, "%H:%M:%S").time(), tzinfo=IST)

    # 1. Real spot 1-min intraday for the underlying stock.
    eq_key = stock_resolve.resolve_eq_instrument_key(symbol)
    if not eq_key:
        return WhatIfResult(symbol, side, entry_ts_s, entry_price, real_exit_ts_s,
                             real_exit_price, real_reason, real_pnl, None, None,
                             "NO_EQ_KEY -- could not resolve spot instrument key", None)
    spot_rows = await hc.fetch_upstox_intraday_1m(eq_key, token)
    spot_bars = [b for b in _to_bars(spot_rows) if b.ts >= entry_ts.replace(second=0, microsecond=0)]
    if len(spot_bars) < 15:
        return WhatIfResult(symbol, side, entry_ts_s, entry_price, real_exit_ts_s,
                             real_exit_price, real_reason, real_pnl, None, None,
                             f"INSUFFICIENT_DATA -- only {len(spot_bars)} 1-min bars post-entry", None)

    # 2. Replay the EXACT live signal: HA on 1-min -> resample 15-min -> StochRSI(9,9,3)
    #    -> ha_stoch_shape_exit_signal(inclusive=True), only on fully-closed 15-min bars,
    #    walking forward bar-by-bar exactly like _ha_stoch_check_exit does live.
    whatif_exit_bar_ts = None
    for i in range(15, len(spot_bars) + 1):
        window = spot_bars[:i]
        ha_1m = to_heikin_ashi(window)
        ha_15m = to_n_min_bars(ha_1m, 15)
        if not ha_15m:
            continue
        last_bar = ha_15m[-1]
        cur_ts = window[-1].ts
        if cur_ts < last_bar.ts + timedelta(minutes=15):
            ha_15m = ha_15m[:-1]
        if not ha_15m:
            continue
        latest = ha_15m[-1]
        if whatif_exit_bar_ts == latest.ts:
            continue   # already evaluated this exact closed bar
        whatif_exit_bar_ts = latest.ts
        closes = [b.close for b in ha_15m]
        k, d = compute_stoch_rsi(closes, RSI_PERIOD, STOCH_PERIOD, SMOOTH)
        if ha_stoch_shape_exit_signal(latest, k[-1], d[-1], side, inclusive=True):
            exit_signal_ts = cur_ts   # the 1-min tick moment the 15-min bar closed and confirmed
            break
    else:
        exit_signal_ts = None   # never fired -- falls back to EOD

    if exit_signal_ts is None:
        exit_signal_ts = datetime.combine(date.fromisoformat(TRADE_DATE),
                                           datetime.strptime(EOD_TIME, "%H:%M").time(), tzinfo=IST)
        whatif_reason = "eod_squareoff (HA+StochRSI never fired)"
    else:
        whatif_reason = "ha_stoch_exit"

    # 3. Real option premium at the reconstructed exit minute, for P&L.
    if not REGISTRY.is_loaded(symbol):
        await asyncio.to_thread(REGISTRY.load_sync, symbol)
    opt_key = REGISTRY.get_upstox_key(symbol, date.fromisoformat(expiry_s), strike, "CE" if side == "CALL" else "PE")
    whatif_exit_price = None
    if opt_key:
        opt_rows = await hc.fetch_upstox_intraday_1m(opt_key, token)
        opt_bars = _to_bars(opt_rows)
        candidates = [b for b in opt_bars if b.ts <= exit_signal_ts]
        if candidates:
            whatif_exit_price = candidates[-1].close

    whatif_pnl = None
    if whatif_exit_price is not None:
        raw = whatif_exit_price - entry_price
        whatif_pnl = round((raw if side == "CALL" else -raw) * qty, 2)
    else:
        whatif_reason += " -- option premium history unavailable, P&L not reconstructed"

    return WhatIfResult(
        symbol, side, entry_ts_s, entry_price, real_exit_ts_s, real_exit_price, real_reason, real_pnl,
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
        wp = f"{r.whatif_exit_price:.2f}" if r.whatif_exit_price is not None else "n/a"
        wpnl = f"{r.whatif_pnl:+.2f}" if r.whatif_pnl is not None else "n/a"
        print(f"  WHAT-IF: entry {r.entry_ts} @ {r.entry_price:.2f}  ->  exit {r.whatif_exit_ts} @ "
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
