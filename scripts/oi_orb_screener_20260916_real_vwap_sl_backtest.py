"""
scripts/oi_orb_screener_20260916_real_vwap_sl_backtest.py

CRITICAL correction, 2026-09-16, direct user follow-up ("check what TF was
for VWAP SL, I think it was 20 min... check the live what is implemented").
Checking this surfaced a much bigger problem: every backtest run TODAY
(oi_orb_screener_20260916_live_oi_confirm_backtest.py,
oi_orb_screener_20260916_4scenario_backtest.py, and this session's earlier
oi_orb_screener_20260916_sltarget_backtest.py) simulated the exit using
ha_stoch_shape_exit_signal (15-min HA+StochRSI) -- which is DEAD CODE in
the live engine, superseded 2026-09-08 (comment at engine.py:1783-1794:
"the universal exit for EVERY open position... is now replaced"; confirmed
via grep that _ha_stoch_check_exit is defined but never called from the
live tick loop any more).

The REAL, currently-live exit (strategies/oi_orb_screener/engine.py) checked
in this priority order, every cycle, for every open position:
  1. HARD SL -- _vwap_close_sl_check: a _VWAP_SL_TF_MIN-minute (=20,
     re-tuned 30->20 on 2026-09-09 via real backtest) Heikin-Ashi candle,
     market-anchored to 09:15 (not midnight), whose CLOSE sits on the wrong
     side of the underlying's own running SESSION VWAP by at least
     _VWAP_SL_MIN_GAP_PCT (=0.2%) -- AND (as of TODAY, 2026-09-16, the
     latest revision) the candle must also have the matching HA-shape (no
     opposing wick: CALL adverse needs ha_high==ha_open, PUT needs
     ha_low==ha_open). Both conditions required together
     (_ha_vwap_close_sl_adverse, reused directly here, not reimplemented).
  2. Multi-day 75-min HTF trap + 3-min S&R ladder (_trap_multiday_exit_check)
     -- NOT replicated in this backtest (see caveat below).
  3. Intraday 15-min/3-min trap fallback (_trap_intraday_exit_check) --
     NOT replicated either.
  4. EOD square-off (15:15), unchanged.

This script replicates ONLY layer 1 (the hard SL, checked first every
cycle so it's the dominant near-term protection) faithfully, reusing the
REAL static method OiOrbScreenerStrategy._ha_vwap_close_sl_adverse and the
REAL to_n_min_bars_market_anchored bucketing (strategies/core/
candle_indicators.py) -- not reimplemented. Layers 2+3 (the multi-day/
intraday trap ladder) are a much larger, separate mechanic (15-day HTF
zone lookback, 3-min S&R) and are NOT built here -- flagged honestly, this
backtest is a lower bound on how much protection the real live exit
provides (the real system has MORE ways to exit early than modeled here).

Entry stays the already-validated VWAP-retest mechanic (confirmed today's
best-performing entry vs. an ORB-breakout alternative) -- unchanged, the 5
known real strong-quadrant trades and their real entry timestamps/prices
are taken as given.

MUST run on EC2 (real Upstox2 access token + real intraday history).

Usage: python scripts/oi_orb_screener_20260916_real_vwap_sl_backtest.py
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer import historical_candles as hc
from data_layer.client_db import ClientDB
from strategies.core.trap_zone_utils import Bar
from strategies.core.candle_indicators import to_heikin_ashi, to_n_min_bars_market_anchored
from strategies.oi_orb_screener import stock_resolve
from strategies.oi_orb_screener.engine import (
    OiOrbScreenerStrategy, _VWAP_SL_TF_MIN, _VWAP_SL_MIN_GAP_PCT,
)
from strategies.oi_orb_screener.screener import VwapState

TRADE_DATE = "2026-09-16"
TODAY = date.fromisoformat(TRADE_DATE)
EOD_TIME = "15:15"

KNOWN_TRADES = [
    ("PAYTM", "CALL", datetime(2026, 9, 16, 10, 45, tzinfo=IST), 1748.80),
    ("BLUESTARCO", "CALL", datetime(2026, 9, 16, 13, 28, tzinfo=IST), 1494.40),
    ("OFSS", "PUT", datetime(2026, 9, 16, 11, 13, tzinfo=IST), 11645.00),
    ("PREMIERENE", "PUT", datetime(2026, 9, 16, 14, 29, tzinfo=IST), 901.80),
    ("NYKAA", "PUT", datetime(2026, 9, 16, 13, 25, tzinfo=IST), 327.30),
]
BASELINE_PNL = {"PAYTM": 21.75, "BLUESTARCO": 1.95, "OFSS": 86.45, "PREMIERENE": -0.10, "NYKAA": 0.10}


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


async def simulate(symbol, side, entry_ts, entry_price_spot, token):
    eq_key = stock_resolve.resolve_eq_instrument_key(symbol)
    if not eq_key:
        return {"reason": "NO_EQ_KEY"}
    warm_rows = await hc.fetch_upstox_warm_1m(eq_key, token, min_bars=400)
    all_eq_bars = _to_bars(warm_rows)
    if not all_eq_bars:
        return {"reason": "no real equity bars"}

    entry_floor = entry_ts.replace(second=0, microsecond=0)

    # Real market-anchored HA bucketing -- exact live class methods, not
    # reimplemented.
    ha_1m = to_heikin_ashi(all_eq_bars)
    ha_tf = to_n_min_bars_market_anchored(ha_1m, _VWAP_SL_TF_MIN)

    # Running session VWAP (spot), same class the live engine's own
    # self._vwap uses -- track vwap "as of" each minute so a bucket is
    # judged against VWAP as it genuinely stood at that bucket's own
    # close, not the final end-of-day value (mirrors the real 2026-09-10
    # bugfix in _replay_vwap_close_sl).
    vwap_state = VwapState()
    vwap_at_minute = {}
    for b in all_eq_bars:
        typical = (b.high + b.low + b.close) / 3.0
        vwap_state.update(symbol, typical, 1.0)   # unweighted-typical-price proxy, same known caveat as every other script today
        v = vwap_state.current(symbol)
        if v is not None:
            vwap_at_minute[b.ts.replace(second=0, microsecond=0)] = v
    sorted_minutes = sorted(vwap_at_minute.keys())

    def _vwap_as_of(bucket_end):
        eligible = [ts for ts in sorted_minutes if ts < bucket_end]
        return vwap_at_minute[eligible[-1]] if eligible else None

    sl_fire_ts = None
    for hb in ha_tf:
        bucket_end = hb.ts + timedelta(minutes=_VWAP_SL_TF_MIN)
        if bucket_end <= entry_floor:
            continue   # bucket closed before this position existed -- not a real SL (real guard)
        vwap_now = _vwap_as_of(bucket_end)
        if vwap_now is None or vwap_now <= 0:
            continue
        if OiOrbScreenerStrategy._ha_vwap_close_sl_adverse(hb, vwap_now, side):
            sl_fire_ts = bucket_end
            break

    opt_type = "CE" if side == "CALL" else "PE"
    contract = await stock_resolve.resolve_contract_async(symbol, entry_price_spot, opt_type)
    if contract is None:
        return {"reason": "could not resolve a real tradable contract"}
    opt_rows = await hc.fetch_upstox_intraday_1m(contract.upstox_key, token)
    opt_bars = _to_bars(opt_rows)
    entry_candidates = [b for b in opt_bars if b.ts <= entry_ts]
    if not entry_candidates:
        return {"reason": "no option bar at/before entry"}
    entry_price = entry_candidates[-1].close

    if sl_fire_ts is not None:
        exit_ts, exit_reason = sl_fire_ts, "vwap_close_sl (REAL live mechanic)"
    else:
        exit_ts = datetime.combine(TODAY, datetime.strptime(EOD_TIME, "%H:%M").time(), tzinfo=IST)
        exit_reason = "eod_squareoff (real VWAP-close SL never fired -- layers 2/3 trap ladder not modeled)"
    exit_candidates = [b for b in opt_bars if b.ts <= exit_ts]
    exit_price = exit_candidates[-1].close if exit_candidates else None

    pnl = round(exit_price - entry_price, 2) if (entry_price is not None and exit_price is not None) else None
    return {"entry_price": entry_price, "exit_price": exit_price, "exit_ts": exit_ts,
            "exit_reason": exit_reason, "pnl": pnl}


async def main():
    token = _access_token()
    print("=" * 130)
    print("OI-ORB Screener -- 2026-09-16 REAL LIVE EXIT re-run (VWAP-close hard SL, tf="
          f"{_VWAP_SL_TF_MIN}min, min_gap={_VWAP_SL_MIN_GAP_PCT*100:.1f}%, HA-shape+VWAP-gap combined)")
    print("CORRECTS all earlier scripts today, which simulated 15-min HA+StochRSI -- confirmed DEAD CODE, "
          "superseded 2026-09-08 by this mechanic. Multi-day/intraday TRAP LADDER (layers 2/3 of the real "
          "priority order) is NOT modeled here -- this is a lower bound on real exit protection.")
    print("=" * 130)

    total_baseline = total_new = 0.0
    for symbol, side, entry_ts, entry_spot in KNOWN_TRADES:
        print(f"\n{'-' * 130}\n{symbol} ({side}), entry@{entry_ts.strftime('%H:%M')} spot={entry_spot}\n{'-' * 130}")
        r = await simulate(symbol, side, entry_ts, entry_spot, token)
        if r.get("pnl") is None:
            print(f"  FAILED: {r.get('reason')}")
            continue
        print(f"  entry opt={r['entry_price']}")
        print(f"  EXIT: {r['exit_reason']} @ {r['exit_ts'].strftime('%H:%M:%S')} opt={r['exit_price']}")
        baseline = BASELINE_PNL[symbol]
        print(f"  PNL: old-script-baseline(dead HA+StochRSI, WRONG)={baseline:+.2f}  ->  "
              f"REAL-live-SL={r['pnl']:+.2f}  (delta {r['pnl'] - baseline:+.2f})")
        total_baseline += baseline
        total_new += r["pnl"]

    print("\n" + "=" * 130)
    print(f"TOTAL, old (WRONG, dead-code) baseline: {total_baseline:+.2f} pts")
    print(f"TOTAL, REAL live hard-SL only (layer 1, no trap ladder):   {total_new:+.2f} pts")
    print("CAVEAT: n=5 trades, single real day. Spot VWAP here uses the same unweighted-typical-price "
          "session-VWAP proxy as every other script today (no real tick volume available historically). "
          "The multi-day/intraday trap ladder (layers 2/3) is NOT modeled -- the real live system may exit "
          "some of these EARLIER than shown here via that ladder, so this total is a lower bound on "
          "protection, not the final real-mechanic number.")
    print("=" * 130)


if __name__ == "__main__":
    asyncio.run(main())
