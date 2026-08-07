"""
scripts/d1trap_nested_15m_5m_backtest.py — backtest a NEW nested-trap concept
for today, NIFTY + SENSEX, per direct user spec (2026-08-07):

  1. A 15-minute trap (sweep + reclaim) is detected using prev-day + today's
     intraday spot data -- same zone-boundary definition D1TrapBearOnlyBook
     itself uses ([sellers_in.low, ref.close] for a bullish/CE reclaim,
     mirrored [ref.close, buyers_in.high] for a bearish/PE reclaim).
  2. Once price re-enters (touches) that 15m zone, switch to watching
     5-MINUTE bars for a SECOND, independent trap (same sweep+reclaim
     structure, finer granularity) forming after the touch.
  3. Entry fires when price breaks through the 5-MINUTE sub-trap's OWN
     reference candle -- confirmed with the user this is the intended
     trigger, NOT the coarser 15m level.

This is a NEW, standalone concept for evaluation only -- it does NOT change
strategies/d1_trap_option/bear_only_book.py or any live code. Reuses the same
proven building blocks the live strategy uses (find_all_bear_zones /
find_all_bull_zones from strategies/v4_cascade/rolling_base.py, the same
zone-boundary construction, real spot history via
strategies/d1_trap_option/book.py's fetch helpers, real option premium via
data_layer/historical_candles.py) rather than reimplementing detection logic
from scratch or trading on synthetic data.

SCOPE / CAVEATS (read before trusting the numbers):
  - This is a FIRST-PASS evaluation, not validated the way the live BearTrap
    zone-boundary/strike-depth/HTF choices were (those went through
    multi-week sweeps in scripts/d1trap_*.py before being adopted). Treat
    today's single-day result as "does this concept even fire real signals,"
    not "should this replace BearTrap."
  - Exit simulation uses a simple hard Rs2000/lot risk cap (mirrors
    bear_only_book.py's _MAX_RISK_RS_PER_LOT) and a 2x-risk profit target,
    checked against real 1-min OPTION premium closes -- not tick-level, so a
    real intra-minute wick through either level wouldn't be caught (same
    disclosed limitation as every other 1-min-candle backtest built this
    session).
  - ITM offset / strike selection mirrors BearTrap's own defaults
    (NIFTY 150pts, SENSEX 300pts) purely for a like-for-like comparison
    against today's real BearTrap trades -- not re-optimized for this new
    mechanic.

Run on the box with a real Upstox access_token (data/clients.db):
    python3 scripts/d1trap_nested_15m_5m_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date, datetime, time, timedelta
from typing import List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.global_config import GlobalConfig, IST  # noqa: E402
from data_layer.client_db import ClientDB  # noqa: E402
from data_layer.historical_candles import fetch_upstox_intraday_1m  # noqa: E402
from data_layer.instrument_registry import REGISTRY  # noqa: E402
from strategies.d1_trap_option.bear_only_book import (  # noqa: E402
    _Bar,
    _collapse_nearby_zones,
    _resample,
    _to_bars,
)
from strategies.d1_trap_option.book import _fetch_1m_bars, _fetch_intraday_5m, _upstox_key_for  # noqa: E402
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones  # noqa: E402

_HIST_WARMUP_DAYS = 14
_SESSION_OPEN = time(9, 15)
_ENTRY_CUTOFF = time(14, 30)
_EOD_TIME = time(15, 15)
_MAX_RISK_RS_PER_LOT = 2000.0
_ATM_ROUND_STEP = 100
_ITM_OFFSET_BY_UNDERLYING = {"NIFTY": 150, "SENSEX": 300}
_ZONE_MERGE_THRESHOLD_PTS = 30.0
_ZONE_MAX_REF_GAP_BARS = 2


def _detect_zones(bars_15m: List[_Bar], direction: str) -> List[dict]:
    """direction='CE' -> bear-zone detection (sellers trapped, bullish
    reclaim), boundary [sellers_in.low, ref.close] -- exact same construction
    as bear_only_book.py's _detect_bear_zones.
    direction='PE' -> bull-zone detection (buyers trapped, bearish reclaim),
    mirrored boundary [ref.close, buyers_in.high]."""
    n = len(bars_15m)
    idx_by_ts = {b.timestamp: i for i, b in enumerate(bars_15m)}
    out: List[dict] = []
    zones = find_all_bear_zones(bars_15m) if direction == "CE" else find_all_bull_zones(bars_15m)
    for z in zones:
        ref_i = idx_by_ts[z.reference_low_ts]
        ref = bars_15m[ref_i]
        if direction == "CE":
            other = None
            for j in range(ref_i + 1, n):
                if bars_15m[j].low < ref.low:
                    other = bars_15m[j]
                    break
            if other is None:
                continue
            lo, hi = other.low, ref.close
        else:
            other = None
            for j in range(ref_i + 1, n):
                if bars_15m[j].high > ref.high:
                    other = bars_15m[j]
                    break
            if other is None:
                continue
            lo, hi = ref.close, other.high
        out.append(dict(
            zone_lo=lo, zone_hi=hi, entry_line=z.entry_line, lock_ts=z.lock_ts,
            ref_ts=ref.timestamp, ref_idx=ref_i, ref_high=ref.high, ref_low=ref.low,
            direction=direction,
        ))

    collapsed = _collapse_nearby_zones(out, threshold_pts=_ZONE_MERGE_THRESHOLD_PTS,
                                        max_ref_gap=_ZONE_MAX_REF_GAP_BARS)
    # _collapse_nearby_zones (reused from bear_only_book.py) builds a fresh dict
    # per merged zone containing only the fields IT knows about -- ref_high/
    # ref_low/direction (added above) get silently dropped on any merge. It does
    # preserve ref_idx though, which still indexes into this same bars_15m list,
    # so re-derive the dropped fields from there rather than losing them.
    for z in collapsed:
        ref_bar = bars_15m[z["ref_idx"]]
        z["ref_high"] = ref_bar.high
        z["ref_low"] = ref_bar.low
        z["direction"] = direction
    return collapsed


def _find_subtrap(bars_5m: List[_Bar], direction: str) -> Optional[dict]:
    """Newest confirmed 5m trap in bars_5m (same detection, one level down).
    Returns the sub-trap's own ref candle info -- entry fires on ITS breach,
    not the outer 15m zone's."""
    zones = _detect_zones(bars_5m, direction)
    if not zones:
        return None
    return max(zones, key=lambda z: z["lock_ts"])


async def check_underlying(underlying: str, token: str, cfg: GlobalConfig) -> None:
    print(f"\n{'='*70}\n{underlying}  (15m outer trap -> 5m sub-trap -> "
          f"entry on sub-trap's own ref-candle breach)")

    key = _upstox_key_for(underlying)
    lot_size = int(cfg.exchange.lot_sizes.get(underlying, 75))
    itm_offset = _ITM_OFFSET_BY_UNDERLYING.get(underlying, 200)
    today = datetime.now(IST).date()

    hist_start = today - timedelta(days=_HIST_WARMUP_DAYS)
    hist_end = today - timedelta(days=1)
    bars_1m_hist = await asyncio.to_thread(_fetch_1m_bars, key, hist_start, hist_end, token)
    if not bars_1m_hist:
        print("  SKIP: no historical 1-min bars fetched.")
        return

    bars_today_1m = await asyncio.to_thread(_fetch_intraday_5m, key, token)
    if not bars_today_1m:
        print("  RESULT: no intraday data for today -- cannot verify.")
        return
    bars_today_1m = [b for b in bars_today_1m if b.timestamp.time() >= _SESSION_OPEN]
    print(f"  Real today's intraday: {len(bars_today_1m)} 1m bars "
          f"({bars_today_1m[0].timestamp if bars_today_1m else '?'} -> "
          f"{bars_today_1m[-1].timestamp if bars_today_1m else '?'})")

    all_1m = bars_1m_hist + bars_today_1m
    bars_15m_full = _to_bars(_resample(_bars_to_df(all_1m), 15))

    outer_zones = {
        "CE": _detect_zones(bars_15m_full, "CE"),
        "PE": _detect_zones(bars_15m_full, "PE"),
    }
    for d in ("CE", "PE"):
        print(f"  15m outer {d} zones detected (full history): {len(outer_zones[d])}")

    # Replay today's bars minute-by-minute, tracking 15m zone contact -> 5m
    # sub-trap search (in the bars since contact) -> sub-trap ref breach = entry.
    entries: List[dict] = []
    contacted: dict = {"CE": set(), "PE": set()}   # lock_ts already touched today
    subtrap_armed: dict = {"CE": {}, "PE": {}}      # lock_ts -> subtrap dict once found
    running_1m: List[_Bar] = list(bars_1m_hist)

    for i, bar in enumerate(bars_today_1m):
        running_1m.append(bar)
        if bar.timestamp.time() >= _ENTRY_CUTOFF:
            break
        if entries:
            break   # one signal at a time for this evaluation, matches BearTrap's own invariant

        for direction in ("CE", "PE"):
            for zone in outer_zones[direction]:
                if zone["lock_ts"] > bar.timestamp:
                    continue   # zone not even confirmed yet as of this bar
                touched = (bar.low <= zone["zone_hi"]) if direction == "CE" else (bar.high >= zone["zone_lo"])
                if not touched:
                    continue
                if zone["lock_ts"] not in contacted[direction]:
                    contacted[direction].add(zone["lock_ts"])

                if zone["lock_ts"] in subtrap_armed[direction]:
                    sub = subtrap_armed[direction][zone["lock_ts"]]
                else:
                    since_contact = [b for b in running_1m if b.timestamp >= zone["lock_ts"]]
                    bars_5m = _to_bars(_resample(_bars_to_df(since_contact), 5))
                    sub = _find_subtrap(bars_5m, direction)
                    if sub is None:
                        continue
                    subtrap_armed[direction][zone["lock_ts"]] = sub
                    print(f"  {direction} 5m sub-trap ARMED inside 15m zone [{zone['zone_lo']:.2f},"
                          f"{zone['zone_hi']:.2f}] @ {bar.timestamp} -- sub ref_high={sub['ref_high']:.2f} "
                          f"ref_low={sub['ref_low']:.2f} (locked {sub['lock_ts']})")

                breached = (bar.high >= sub["ref_high"]) if direction == "CE" else (bar.low <= sub["ref_low"])
                if breached and bar.timestamp > sub["lock_ts"]:
                    entries.append(dict(
                        direction=direction, ts=bar.timestamp, spot=bar.close,
                        sl_spot=sub["ref_low"] if direction == "CE" else sub["ref_high"],
                        outer_zone=zone, subtrap=sub,
                    ))
                    print(f"  RESULT: {direction} ENTRY -- 5m sub-trap ref breached @ {bar.timestamp} "
                          f"(spot={bar.close:.2f})")
                    break
            if entries:
                break

    if not entries:
        print("  RESULT: NO ENTRY today -- no 15m zone ever got both touched AND "
              "produced a 5m sub-trap whose own ref candle then broke.")
        return

    entry = entries[0]
    direction = entry["direction"]
    atm = round(entry["spot"] / _ATM_ROUND_STEP) * _ATM_ROUND_STEP
    strike = int(atm - itm_offset) if direction == "CE" else int(atm + itm_offset)
    opt_type = direction
    try:
        await asyncio.to_thread(REGISTRY.load_sync, underlying, token)
    except Exception:
        pass
    expiry = REGISTRY.get_active_expiry(underlying, today)
    print(f"    Strike={strike}{opt_type} expiry={expiry} spot_sl={entry['sl_spot']:.2f}")

    if not expiry:
        print("    P&L: SKIPPED -- no active expiry resolved.")
        return
    opt_key = REGISTRY.get_upstox_key(underlying, expiry, strike, opt_type)
    if not opt_key:
        print(f"    P&L: SKIPPED -- no Upstox instrument key for {underlying} {strike}{opt_type}.")
        return
    premium_candles = await fetch_upstox_intraday_1m(opt_key, token)
    if not premium_candles:
        print("    P&L: SKIPPED -- no real premium candles for this strike today.")
        return

    _replay_pnl(entry, premium_candles, lot_size)


def _bars_to_df(bars: List[_Bar]):
    import pandas as pd
    return pd.DataFrame([
        {"datetime": b.timestamp, "open": b.open, "high": b.high, "low": b.low, "close": b.close}
        for b in bars
    ])


def _replay_pnl(entry: dict, premium_candles: list, lot_size: int) -> None:
    """Simple hard-cap SL / 2x-risk target replay against real 1-min option
    closes -- same disclosed 1-min-candle-close limitation as every other
    intraday premium replay built this session (not tick-level)."""
    candles = [c for c in premium_candles if datetime.fromisoformat(c["ts"]) >= entry["ts"]]
    if not candles:
        print("    P&L: SKIPPED -- no premium data at/after the entry moment.")
        return
    entry_premium = float(candles[0]["close"])
    if entry_premium <= 0:
        print(f"    P&L: SKIPPED -- entry-moment premium is {entry_premium}.")
        return
    cap_sl = entry_premium - (_MAX_RISK_RS_PER_LOT / lot_size)
    target = entry_premium + 2 * (entry_premium - cap_sl)

    exit_reason, exit_price, exit_ts = None, None, None
    for c in candles[1:]:
        ts = datetime.fromisoformat(c["ts"])
        premium = float(c["close"])
        if ts.time() >= _EOD_TIME:
            exit_reason, exit_price, exit_ts = "eod", premium, c["ts"]
            break
        if premium <= cap_sl:
            exit_reason, exit_price, exit_ts = "sl_hit_hard_cap", premium, c["ts"]
            break
        if premium >= target:
            exit_reason, exit_price, exit_ts = "target_2x_risk", premium, c["ts"]
            break
    if exit_reason is None:
        exit_reason = "still running (data ends before an exit trigger)"
        exit_price, exit_ts = float(candles[-1]["close"]), candles[-1]["ts"]

    pnl = (exit_price - entry_premium) * lot_size
    sign = "+" if pnl >= 0 else ""
    print(f"    REAL PREMIUM REPLAY: entry_premium={entry_premium:.2f} -> exit={exit_reason} "
          f"@ {exit_price:.2f} (at {exit_ts})")
    print(f"    P&L per lot (qty={lot_size}): {sign}Rs{pnl:.2f}")


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

    for underlying in ("NIFTY", "SENSEX"):
        await check_underlying(underlying, token, cfg)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
