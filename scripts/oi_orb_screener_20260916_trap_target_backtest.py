"""
scripts/oi_orb_screener_20260916_trap_target_backtest.py

Direct user follow-up, 2026-09-16: "also did u checked the target part or
not" -- no, the earlier real_vwap_sl_backtest.py explicitly only modeled
layer 1 (the hard SL) of the real live exit priority order. This script
checks layer 2/3 -- the TARGET side, which is NOT a fixed risk-reward
ratio (unlike the disabled option-premium ratchet from
oi_orb_screener_20260916_sltarget_backtest.py). The REAL live target is a
TRAP-ZONE TOUCH:
  - Multi-day tier: 180-min HTF bars built from 15 real calendar days of
    history (today's REAL 15-day lookback, same as the live engine's own
    _seed_trap_exit_state), zones from screener.bull_trap_zones (CALL) /
    sharp_bear_zones (PUT), already-touched-on-a-prior-day zones dropped
    (_drop_already_touched_zones), the MOST RECENTLY LOCKED zone as of
    each moment is the active target (_latest_locked_zone) -- reaching
    that zone's [zone_lo, zone_hi] range is a target-hit exit.
  - Intraday tier: same idea, 15-min HTF bars from TODAY's bars only, used
    as a fallback when the multi-day tier has too few bars (<3) or hasn't
    locked/touched anything yet.
  - Hard SL (already verified -- never fired on any of these 5 today)
    still takes priority if it fires first.

All zone-detection functions (screener.bull_trap_zones/sharp_bear_zones)
and the static helpers (_drop_already_touched_zones, _latest_locked_zone)
are called DIRECTLY from the real OiOrbScreenerStrategy class -- not
reimplemented. This script is deliberately READ-ONLY: it never calls
_emit_close/_replay_trap_state/_fire_vwap_close_sl or anything else that
writes to the real data/oi_orb_screener.db -- only pure zone functions
and a plain range-overlap check against real bars.

MUST run on EC2 (real Upstox2 access token + real 15-day history).

Usage: python scripts/oi_orb_screener_20260916_trap_target_backtest.py
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
from strategies.core.candle_indicators import to_n_min_bars_dateaware
from strategies.oi_orb_screener import stock_resolve, screener
from strategies.oi_orb_screener.engine import (
    OiOrbScreenerStrategy, _TRAP_EXIT_HTF_MULTIDAY_MIN, _TRAP_EXIT_HTF_INTRADAY_MIN,
    _TRAP_EXIT_LOOKBACK_CALENDAR_DAYS,
)

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


def _find_target_touch(zones, bars, entry_ts):
    """Read-only replay: at each real bar at/after entry_ts, find the most
    recently locked zone as of that bar's own timestamp (real static
    helper), and check for a genuine price overlap with its range."""
    for b in bars:
        if b.ts < entry_ts:
            continue
        zone = OiOrbScreenerStrategy._latest_locked_zone(zones, b.ts)
        if zone is None:
            continue
        if b.low <= zone["zone_hi"] and b.high >= zone["zone_lo"]:
            return b.ts, zone
    return None, None


async def check_symbol(symbol, side, entry_ts, token):
    eq_key = stock_resolve.resolve_eq_instrument_key(symbol)
    if not eq_key:
        return {"reason": "NO_EQ_KEY"}
    today = TODAY
    start = today - timedelta(days=_TRAP_EXIT_LOOKBACK_CALENDAR_DAYS)
    prior_rows = await hc.fetch_upstox_range_1m(eq_key, token, start, today)
    today_rows = await hc.fetch_upstox_intraday_1m(eq_key, token)
    seen, all_rows = set(), []
    for r in sorted(list(prior_rows or []) + list(today_rows or []), key=lambda r: r["ts"]):
        if r["ts"] in seen:
            continue
        seen.add(r["ts"])
        all_rows.append(r)
    bars = _to_bars(all_rows)
    if not bars:
        return {"reason": "no real bars (multi-day + today)"}
    today_bars = [b for b in bars if b.ts.date() == today]
    if not today_bars:
        return {"reason": "no real bars for today"}

    zones_fn = screener.bull_trap_zones if side == "CALL" else screener.sharp_bear_zones
    eod_ts = datetime.combine(TODAY, datetime.strptime(EOD_TIME, "%H:%M").time(), tzinfo=IST)

    # -- multi-day tier --
    htf_multiday = to_n_min_bars_dateaware(bars, _TRAP_EXIT_HTF_MULTIDAY_MIN)
    multiday_result = None
    if len(htf_multiday) >= 3:
        zones_all = zones_fn(htf_multiday)
        zones = OiOrbScreenerStrategy._drop_already_touched_zones(zones_all, bars, today)
        touch_ts, zone = _find_target_touch(zones, today_bars, entry_ts)
        multiday_result = (touch_ts, zone, len(zones_all), len(zones), len(htf_multiday))

    # -- intraday tier (only matters if multiday found nothing) --
    intraday_result = None
    if multiday_result is None or multiday_result[0] is None:
        htf_intraday = to_n_min_bars_dateaware(today_bars, _TRAP_EXIT_HTF_INTRADAY_MIN)
        if len(htf_intraday) >= 3:
            zones = zones_fn(htf_intraday)
            touch_ts, zone = _find_target_touch(zones, today_bars, entry_ts)
            intraday_result = (touch_ts, zone, len(zones), len(htf_intraday))

    target_ts = target_zone = tier = None
    if multiday_result and multiday_result[0] is not None:
        target_ts, target_zone, tier = multiday_result[0], multiday_result[1], "multiday(180min)"
    elif intraday_result and intraday_result[0] is not None:
        target_ts, target_zone, tier = intraday_result[0], intraday_result[1], "intraday(15min)"

    exit_ts = target_ts if target_ts is not None else eod_ts
    exit_reason = f"trap_target_hit ({tier})" if target_ts is not None else \
        "eod_squareoff (no trap target ever touched -- real SL already confirmed not to fire either)"

    return {
        "multiday_zone_count": multiday_result[3] if multiday_result else 0,
        "multiday_zone_count_before_drop": multiday_result[2] if multiday_result else 0,
        "multiday_htf_bars": multiday_result[4] if multiday_result else 0,
        "intraday_zone_count": intraday_result[2] if intraday_result else 0,
        "target_ts": target_ts, "target_zone": target_zone, "tier": tier,
        "exit_ts": exit_ts, "exit_reason": exit_reason,
    }


async def main():
    token = _access_token()
    print("=" * 130)
    print("OI-ORB Screener -- 2026-09-16 TRAP-TARGET check (layer 2/3 of the real live exit priority order)")
    print(f"Multi-day tier: {_TRAP_EXIT_HTF_MULTIDAY_MIN}min HTF, {_TRAP_EXIT_LOOKBACK_CALENDAR_DAYS}-calendar-day "
          f"real lookback. Intraday fallback: {_TRAP_EXIT_HTF_INTRADAY_MIN}min HTF, today only.")
    print("Real SL (layer 1) already confirmed to NEVER fire on any of these 5 trades -- this checks whether "
          "the TARGET (trap-zone touch) would have closed any of them EARLIER than 15:15 EOD.")
    print("=" * 130)

    for symbol, side, entry_ts, _entry_spot in KNOWN_TRADES:
        print(f"\n{'-' * 130}\n{symbol} ({side}), entry@{entry_ts.strftime('%H:%M')}\n{'-' * 130}")
        r = await check_symbol(symbol, side, entry_ts, token)
        if "reason" in r:
            print(f"  FAILED: {r['reason']}")
            continue
        print(f"  multi-day: {r['multiday_htf_bars']} real 180min bars, "
              f"{r['multiday_zone_count_before_drop']} zone(s) found, {r['multiday_zone_count']} "
              f"still valid after dropping already-touched-on-a-prior-day ones")
        print(f"  intraday fallback: {r['intraday_zone_count']} zone(s) (only checked if multi-day found no touch)")
        if r["target_ts"] is not None:
            z = r["target_zone"]
            print(f"  TARGET TOUCHED at {r['target_ts'].strftime('%H:%M')} via {r['tier']} tier -- "
                  f"zone=[{z['zone_lo']:.2f}, {z['zone_hi']:.2f}] locked={z['lock_ts']}")
        else:
            print(f"  target: NEVER touched by real price after entry")
        print(f"  REAL exit: {r['exit_reason']} @ {r['exit_ts'].strftime('%H:%M:%S')}")
        baseline = BASELINE_PNL[symbol]
        if r["target_ts"] is not None:
            print(f"  NOTE: this exits EARLIER than the already-known EOD result (baseline pnl={baseline:+.2f} "
                  f"was priced at 15:15) -- real P&L would differ, needs its own real-option-premium fetch "
                  f"at {r['exit_ts'].strftime('%H:%M')} to compute exactly.")
        else:
            print(f"  Confirms baseline pnl={baseline:+.2f} still holds (target never touched, SL never fired, "
                  f"falls through to the same EOD exit already validated).")

    print("\n" + "=" * 130)
    print("CAVEAT: n=5, single real day. The multi-day tier depends on real 15-calendar-day history genuinely "
          "existing for each stock (some newly-listed/thin-history stocks may have fewer than 3 real 180min "
          "bars, forcing the intraday-only fallback -- shown per-stock above).")
    print("=" * 130)


if __name__ == "__main__":
    asyncio.run(main())
