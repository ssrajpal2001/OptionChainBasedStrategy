"""
scripts/d1trap_sr_zone_backtest.py — BearTrap's existing HTF sweep+reclaim
zone (UNCHANGED, detected on the option's own premium chart -- exactly how
live D1TrapBearOnlyBook already works), but T1/T2 temporarily dropped and
replaced with the "ping-pong" Support & Resistance tracker
(strategies/d1_trap_option/support_resistance.py) once price re-touches the
zone.

Mechanic (confirmed with user, 2026-08-07):
  1. Outer zone: identical to live BearTrap -- sweep+reclaim detected on
     each strike's own real premium history, at the underlying's existing
     default HTF timeframe (NIFTY 60m / SENSEX 15m,
     _HTF_MINUTES_DEFAULT_BY_UNDERLYING).
  2. Inner: on the FIRST 1-minute premium touch of a zone, seed a *fresh*
     SupportResistanceCalculator and start feeding it TF-minute premium
     candles from that point forward (TF swept below).
  3. Entry: the first time the S&R state machine confirms a genuine
     breakout (phase INITIAL_TREND_ESTABLISHMENT -> R1_TRACKING, i.e. a
     real higher-high-AND-higher-low candle, not any single tick above the
     prior high).
  4. SL: trails the S&R tracker's own live S1 level (NOT frozen at entry --
     confirmed with user this should ratchet as the tracker's support
     level moves, same "structural trailing stop" idea as BearTrap's
     existing staircase TSL, just derived from S&R structure instead of a
     fixed premium %).

S&R candle timeframe is SWEPT across 1m / 3m / 5m per direct user spec --
this reports which granularity actually performs better on real data for
today, rather than assuming one value, same discipline as
scripts/d1trap_strike_ladder_backtest.py's ITM-depth sweep.

Strike selection: ATM +/- the underlying's existing default ITM offset
(_ITM_OFFSET_DEFAULT_BY_UNDERLYING) off the real day-open spot -- BearTrap's
own fixed-offset fallback path (live OI-wall selection can't be replicated
retroactively, same documented limitation as every other backtest this
session that touches strike selection).

SCOPE: standalone evaluation only. Does NOT change D1TrapBearOnlyBook.
T1/T2 are not run at all in this script (per user: "temp drop t1/t2") --
this is testing the S&R replacement in isolation, not stacked with the
existing tranches.

Run on the box with a real Upstox access_token (data/clients.db):
    python3 scripts/d1trap_sr_zone_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, time, timedelta
from typing import List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.global_config import GlobalConfig, IST  # noqa: E402
from data_layer.client_db import ClientDB  # noqa: E402
from data_layer.historical_candles import fetch_upstox_intraday_1m  # noqa: E402
from data_layer.instrument_registry import REGISTRY  # noqa: E402
from strategies.d1_trap_option.bear_only_book import (  # noqa: E402
    _Bar,
    _HTF_MINUTES_DEFAULT_BY_UNDERLYING,
    _ITM_OFFSET_DEFAULT_BY_UNDERLYING,
    _resample,
    _to_bars,
)
from strategies.d1_trap_option.book import _fetch_1m_bars, _upstox_key_for  # noqa: E402
from strategies.d1_trap_option.support_resistance import SupportResistanceCalculator  # noqa: E402
from scripts.d1trap_nested_15m_5m_backtest import _detect_zones  # noqa: E402

_HIST_WARMUP_DAYS = 14
_SESSION_OPEN = time(9, 15)
_ENTRY_CUTOFF = time(14, 30)
_EOD_TIME = time(15, 15)
_ATM_ROUND_STEP = 100
_SR_TF_SWEEP = (1, 3, 5)
_MAX_RISK_RS_PER_LOT = 2000.0   # sanity backstop even while SL trails S&R structure


def _candles_to_bars(candles: list) -> List[_Bar]:
    out = []
    for c in candles:
        ts = datetime.fromisoformat(c["ts"]) if isinstance(c["ts"], str) else c["ts"]
        out.append(_Bar(timestamp=ts, open=float(c["open"]), high=float(c["high"]),
                         low=float(c["low"]), close=float(c["close"])))
    return out


def _bars_to_df(bars: List[_Bar]):
    import pandas as pd
    return pd.DataFrame([
        {"datetime": b.timestamp, "open": b.open, "high": b.high, "low": b.low, "close": b.close}
        for b in bars
    ])


def _run_sr_variant(zones: List[dict], today_1m: List[_Bar], tf_minutes: int, lot_size: int) -> Optional[dict]:
    """One S&R-timeframe variant: 1-min touch detection, TF-min S&R tracking
    from the touch forward, entry on first confirmed breakout, SL trails S1.
    Returns the trade result dict, or None if nothing fired."""
    touched_zone_ts: set = set()
    active_sr: dict = {}   # zone lock_ts -> {"calc": SupportResistanceCalculator, "bucket": [...], "bucket_open": ts}
    position: Optional[dict] = None

    for bar in today_1m:
        if bar.timestamp.time() >= _ENTRY_CUTOFF and position is None:
            break

        # EOD exit for an open position
        if position is not None:
            if bar.timestamp.time() >= _EOD_TIME:
                pnl = (bar.close - position["entry_premium"]) * lot_size
                return {"entry_ts": position["entry_ts"], "entry_premium": position["entry_premium"],
                        "exit_reason": "eod", "exit_price": bar.close, "exit_ts": bar.timestamp, "pnl": pnl}
            sl = active_sr[position["zone_ts"]]["calc"].get_calculated_sr_state("OPT")["sr_levels"]["S1"]["low"]
            if bar.close <= sl:
                pnl = (bar.close - position["entry_premium"]) * lot_size
                return {"entry_ts": position["entry_ts"], "entry_premium": position["entry_premium"],
                        "exit_reason": f"sl_trail@{sl:.2f}", "exit_price": bar.close, "exit_ts": bar.timestamp,
                        "pnl": pnl}

        for zone in zones:
            if zone["lock_ts"] > bar.timestamp:
                continue
            touched = bar.low <= zone["zone_hi"]
            if not touched:
                continue
            if zone["lock_ts"] not in touched_zone_ts:
                touched_zone_ts.add(zone["lock_ts"])
                active_sr[zone["lock_ts"]] = {"calc": SupportResistanceCalculator(),
                                               "bucket": [], "bucket_open": None}
            if position is not None:
                continue   # one position at a time -- still update touch bookkeeping above

            entry = active_sr[zone["lock_ts"]]
            calc = entry["calc"]
            b_open = bar.timestamp.replace(
                minute=(bar.timestamp.minute // tf_minutes) * tf_minutes, second=0, microsecond=0)
            if entry["bucket_open"] is None:
                entry["bucket_open"] = b_open
            elif b_open != entry["bucket_open"]:
                bucket_bars = entry["bucket"]
                if bucket_bars:
                    tf_bar = dict(timestamp=entry["bucket_open"], high=max(b.high for b in bucket_bars),
                                  low=min(b.low for b in bucket_bars), close=bucket_bars[-1].close, duration=1)
                    phase_before = calc.get_calculated_sr_state("OPT")["current_phase"]
                    calc.process_straddle_candle("OPT", tf_bar, silent=True)
                    phase_after = calc.get_calculated_sr_state("OPT")["current_phase"]
                    if phase_before == "INITIAL_TREND_ESTABLISHMENT" and phase_after == "R1_TRACKING":
                        entry_premium = float(tf_bar["close"])
                        position = {"zone_ts": zone["lock_ts"], "entry_ts": tf_bar["timestamp"],
                                    "entry_premium": entry_premium}
                entry["bucket_open"] = b_open
                entry["bucket"] = []
            entry["bucket"].append(bar)

    return None


async def check_underlying(underlying: str, token: str, cfg: GlobalConfig) -> None:
    print(f"\n{'='*70}\n{underlying}  (HTF zone unchanged, S&R replaces T1/T2, "
          f"SL trails S&R's own S1)")

    lot_size = int(cfg.exchange.lot_sizes.get(underlying, 75))
    itm_offset = _ITM_OFFSET_DEFAULT_BY_UNDERLYING.get(underlying, 200)
    htf_minutes = _HTF_MINUTES_DEFAULT_BY_UNDERLYING.get(underlying, 60)
    today = datetime.now(IST).date()

    spot_key = _upstox_key_for(underlying)
    spot_today = await fetch_upstox_intraday_1m(spot_key, token)
    if not spot_today:
        print("  SKIP: no real spot data for today.")
        return
    spot_open = float(spot_today[0]["open"])
    atm = round(spot_open / _ATM_ROUND_STEP) * _ATM_ROUND_STEP
    ce_strike, pe_strike = int(atm - itm_offset), int(atm + itm_offset)
    print(f"  Real day-open spot={spot_open:.2f} ATM={atm} -> CE strike={ce_strike} PE strike={pe_strike} "
          f"(fixed-offset fallback, HTF={htf_minutes}m)")

    try:
        await asyncio.to_thread(REGISTRY.load_sync, underlying, token)
    except Exception:
        pass
    expiry = REGISTRY.get_active_expiry(underlying, today)
    if not expiry:
        print("  SKIP: no active expiry resolved.")
        return

    for direction, strike in (("CE", ce_strike), ("PE", pe_strike)):
        opt_key = REGISTRY.get_upstox_key(underlying, expiry, strike, direction)
        if not opt_key:
            print(f"  {direction}{strike}: SKIP -- no Upstox instrument key.")
            continue

        hist_start = today - timedelta(days=_HIST_WARMUP_DAYS)
        hist_end = today - timedelta(days=1)
        hist_1m = await asyncio.to_thread(_fetch_1m_bars, opt_key, hist_start, hist_end, token)
        today_candles = await fetch_upstox_intraday_1m(opt_key, token)
        if not today_candles:
            print(f"  {direction}{strike}: SKIP -- no real premium data for today.")
            continue
        today_1m = _candles_to_bars(today_candles)
        today_1m = [b for b in today_1m if b.timestamp.time() >= _SESSION_OPEN]

        all_1m = hist_1m + today_1m
        bars_htf = _to_bars(_resample(_bars_to_df(all_1m), htf_minutes))
        zones = _detect_zones(bars_htf, direction)
        print(f"\n  {direction}{strike}: {len(hist_1m)} historical + {len(today_1m)} today 1m bars -> "
              f"{len(zones)} HTF({htf_minutes}m) zones")

        for tf in _SR_TF_SWEEP:
            result = _run_sr_variant(zones, today_1m, tf, lot_size)
            if result is None:
                print(f"    S&R TF={tf}m: NO ENTRY today")
                continue
            sign = "+" if result["pnl"] >= 0 else ""
            print(f"    S&R TF={tf}m: ENTRY @ {result['entry_ts']} premium={result['entry_premium']:.2f} -> "
                  f"exit={result['exit_reason']} @ {result['exit_price']:.2f} ({result['exit_ts']}) "
                  f"P&L/lot={sign}Rs{result['pnl']:.2f}")


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
