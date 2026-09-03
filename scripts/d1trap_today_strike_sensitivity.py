"""
scripts/d1trap_today_strike_sensitivity.py — for each of TODAY's real
BearTrap entries (taken directly from the real EC2 log), sweep ITM depth
0..5 strikes around the real spot at that exact entry moment, and replay
REAL option premium for each depth against the same hard Rs2000/lot risk
cap the live book actually uses.

Directly answers the question: was today's loss a STRIKE-SELECTION problem
(a shallower or deeper strike would have avoided/reduced it), or a
DIRECTIONAL problem (the underlying moved against the trade regardless of
which strike was traded, so no strike choice would have saved it)?

Trade list below is transcribed directly from today's real
~/.pm2/logs/terminus-out.log BearTrap entries (2026-08-07) -- the ones that
actually closed via a confirmed SL hit. The 2 positions still open at
market close (SENSEX PE78900, NIFTY PE24750) are excluded -- their final
outcome isn't known yet, so a sensitivity sweep on them would be
speculative in a different way than what's being asked here.

Run on the box with a real Upstox access_token (data/clients.db):
    python3 scripts/d1trap_today_strike_sensitivity.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date, datetime, time, timedelta
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.global_config import GlobalConfig, IST  # noqa: E402
from data_layer.client_db import ClientDB  # noqa: E402
from data_layer.historical_candles import fetch_upstox_intraday_1m  # noqa: E402
from data_layer.instrument_registry import REGISTRY  # noqa: E402
from strategies.d1_trap_option.book import _upstox_key_for  # noqa: E402

_MAX_RISK_RS_PER_LOT = 2000.0
_EOD_TIME = time(15, 15)
_STRIKE_STEP = {"NIFTY": 50, "SENSEX": 100}
_ITM_DEPTHS = (0, 1, 2, 3, 4, 5)   # 0 = ATM; 3 = today's live default for both underlyings

# (underlying, direction, entry_ts, actual_strike_traded) -- real trades that
# actually closed via a confirmed SL today, transcribed from the real log.
TODAY_TRADES = [
    ("SENSEX", "PE", datetime(2026, 8, 7, 10, 46, 1, tzinfo=IST), 78800),
    ("SENSEX", "PE", datetime(2026, 8, 7, 11, 47, 42, tzinfo=IST), 78800),
    ("SENSEX", "CE", datetime(2026, 8, 7, 13, 54, 1, tzinfo=IST), 78200),
    ("SENSEX", "PE", datetime(2026, 8, 7, 14, 6, 58, tzinfo=IST), 78800),
    ("NIFTY", "CE", datetime(2026, 8, 7, 10, 48, 0, tzinfo=IST), 24350),
    ("NIFTY", "PE", datetime(2026, 8, 7, 13, 34, 34, tzinfo=IST), 24650),
]


async def _real_spot_at(underlying: str, ts: datetime, token: str) -> Optional[float]:
    key = _upstox_key_for(underlying)
    candles = await fetch_upstox_intraday_1m(key, token)
    if not candles:
        return None
    ts_min = ts.replace(second=0, microsecond=0)
    exact = [c for c in candles if datetime.fromisoformat(c["ts"]).replace(second=0, microsecond=0) == ts_min]
    if exact:
        return float(exact[0]["close"])
    before = [c for c in candles if datetime.fromisoformat(c["ts"]) <= ts]
    return float(before[-1]["close"]) if before else None


def _replay_pnl(entry_ts: datetime, premium_candles: list, lot_size: int) -> dict:
    candles = [c for c in premium_candles if datetime.fromisoformat(c["ts"]) >= entry_ts]
    if not candles:
        return {"outcome": "no premium data at/after entry", "pnl": None}
    entry_premium = float(candles[0]["close"])
    if entry_premium <= 0:
        return {"outcome": f"bad entry premium ({entry_premium})", "pnl": None}
    cap_sl = entry_premium - (_MAX_RISK_RS_PER_LOT / lot_size)

    for c in candles[1:]:
        ts = datetime.fromisoformat(c["ts"])
        premium = float(c["close"])
        if ts.time() >= _EOD_TIME:
            pnl = (premium - entry_premium) * lot_size
            return {"outcome": f"eod @ {premium:.2f}", "pnl": pnl, "entry_premium": entry_premium}
        if premium <= cap_sl:
            pnl = (premium - entry_premium) * lot_size
            return {"outcome": f"SL hit @ {premium:.2f}", "pnl": pnl, "entry_premium": entry_premium}
    last = float(candles[-1]["close"])
    pnl = (last - entry_premium) * lot_size
    return {"outcome": f"still running @ {last:.2f}", "pnl": pnl, "entry_premium": entry_premium}


async def check_trade(underlying: str, direction: str, entry_ts: datetime, actual_strike: int,
                       token: str, cfg: GlobalConfig) -> None:
    lot_size = int(cfg.exchange.lot_sizes.get(underlying, 75))
    step = _STRIKE_STEP[underlying]

    spot = await _real_spot_at(underlying, entry_ts, token)
    if spot is None:
        print(f"\n{underlying} {direction} @ {entry_ts.time()} (actual strike {actual_strike}): "
              f"SKIP -- could not read real spot at entry.")
        return
    atm = round(spot / step) * step
    print(f"\n{'='*70}\n{underlying} {direction} @ {entry_ts.time()}  real spot={spot:.2f}  ATM={atm}  "
          f"(actual strike traded today: {actual_strike})")

    today = entry_ts.date()
    expiry = REGISTRY.get_active_expiry(underlying, today)
    try:
        await asyncio.to_thread(REGISTRY.load_sync, underlying, token)
        expiry = REGISTRY.get_active_expiry(underlying, today)
    except Exception:
        pass
    if not expiry:
        print("  SKIP: no active expiry resolved.")
        return

    print(f"  {'Depth':<10}{'Strike':>9}{'Entry prem':>12}{'Outcome':>28}{'P&L/lot':>12}")
    for n in _ITM_DEPTHS:
        strike = int(atm - n * step) if direction == "CE" else int(atm + n * step)
        opt_key = REGISTRY.get_upstox_key(underlying, expiry, strike, direction)
        if not opt_key:
            print(f"  {n}-ITM{'':<5}{strike:>9}{'':>12}{'no instrument key':>28}")
            continue
        premium_candles = await fetch_upstox_intraday_1m(opt_key, token)
        if not premium_candles:
            print(f"  {n}-ITM{'':<5}{strike:>9}{'':>12}{'no premium data':>28}")
            continue
        result = _replay_pnl(entry_ts, premium_candles, lot_size)
        if result["pnl"] is None:
            print(f"  {n}-ITM{'':<5}{strike:>9}{'':>12}{result['outcome']:>28}")
            continue
        tag = " (actual)" if strike == actual_strike else ""
        sign = "+" if result["pnl"] >= 0 else ""
        print(f"  {n}-ITM{tag:<10}{strike:>9}{result['entry_premium']:>12.2f}{result['outcome']:>28}"
              f"{sign}Rs{result['pnl']:>9.2f}")


async def main() -> int:
    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    token = (creds or {}).get("access_token", "")
    if not token:
        print("FATAL: no Upstox access_token in data/clients.db.")
        return 1
    cfg = GlobalConfig()
    try:
        cfg.exchange.apply_db_overrides(db)
    except Exception:
        pass

    for underlying, direction, entry_ts, actual_strike in TODAY_TRADES:
        await check_trade(underlying, direction, entry_ts, actual_strike, token, cfg)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
