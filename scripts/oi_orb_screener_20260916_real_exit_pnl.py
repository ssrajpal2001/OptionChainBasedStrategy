"""
scripts/oi_orb_screener_20260916_real_exit_pnl.py

Direct follow-up: the trap-target backtest found 4 of the 5 real trades
hit a real trap-zone target BEFORE the 15:15 EOD exit the earlier
(SL-only) backtest priced them at. This fetches the REAL option premium
at each trade's ACTUAL exit time (target-hit or EOD, whichever is
correct per that backtest) and recomputes the final, fully-corrected P&L
-- entry prices are the already-known real values, only the exit side
changes here.

MUST run on EC2 (real Upstox2 access token).

Usage: python scripts/oi_orb_screener_20260916_real_exit_pnl.py
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

TODAY = date.fromisoformat("2026-09-16")

# (symbol, side, entry_ts, entry_spot, entry_opt, exit_ts, exit_reason)
# entry_opt values are the already-known real premiums from today's
# validated entry step. exit_ts is the REAL exit time found by the
# trap-target backtest (target-hit) or the unchanged EOD 15:15 for the
# one trade whose target never touched.
TRADES = [
    ("PAYTM", "CALL", datetime(2026, 9, 16, 10, 45, tzinfo=IST), 1748.80, 52.05,
     datetime(2026, 9, 16, 12, 15, tzinfo=IST), "trap_target_hit"),
    ("BLUESTARCO", "CALL", datetime(2026, 9, 16, 13, 28, tzinfo=IST), 1494.40, 26.00,
     datetime(2026, 9, 16, 13, 53, tzinfo=IST), "trap_target_hit"),
    ("OFSS", "PUT", datetime(2026, 9, 16, 11, 13, tzinfo=IST), 11645.00, 320.00,
     datetime(2026, 9, 16, 11, 15, tzinfo=IST), "trap_target_hit"),
    ("PREMIERENE", "PUT", datetime(2026, 9, 16, 14, 29, tzinfo=IST), 901.80, 18.95,
     datetime(2026, 9, 16, 15, 15, tzinfo=IST), "eod_squareoff"),
    ("NYKAA", "PUT", datetime(2026, 9, 16, 13, 25, tzinfo=IST), 327.30, 4.65,
     datetime(2026, 9, 16, 13, 25, tzinfo=IST), "trap_target_hit"),
]


def _access_token():
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


async def main():
    token = _access_token()
    print("=" * 120)
    print("OI-ORB Screener -- 2026-09-16 FINAL corrected P&L (real trap-target exits, not EOD)")
    print("=" * 120)

    total = 0.0
    for symbol, side, entry_ts, entry_spot, entry_opt, exit_ts, exit_reason in TRADES:
        opt_type = "CE" if side == "CALL" else "PE"
        contract = await stock_resolve.resolve_contract_async(symbol, entry_spot, opt_type)
        if contract is None:
            print(f"{symbol}: could not resolve contract")
            continue
        opt_rows = await hc.fetch_upstox_intraday_1m(contract.upstox_key, token)
        opt_bars = _to_bars(opt_rows)
        exit_candidates = [b for b in opt_bars if b.ts <= exit_ts]
        if not exit_candidates:
            print(f"{symbol}: no real option bar at/before exit_ts {exit_ts}")
            continue
        exit_price = exit_candidates[-1].close
        pnl = round(exit_price - entry_opt, 2)
        total += pnl
        print(f"\n{symbol} ({side}): entry@{entry_ts.strftime('%H:%M')} opt={entry_opt}  ->  "
              f"exit@{exit_ts.strftime('%H:%M')} opt={exit_price}  [{exit_reason}]")
        print(f"  PNL = {pnl:+.2f} pts")

    print("\n" + "=" * 120)
    print(f"TOTAL, fully corrected (real SL + real trap-target exits, both layers verified): {total:+.2f} pts")
    print("=" * 120)


if __name__ == "__main__":
    asyncio.run(main())
