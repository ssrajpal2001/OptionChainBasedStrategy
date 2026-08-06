"""
scripts/fvg_today_check.py — did NIFTY or SENSEX actually fire a real FVG
retest entry today, using REAL Upstox market data?

FVG (strategies/fvg/) is in paper-trading readiness per CLAUDE.md but was not
confirmed deployed/running today -- this answers the retrospective question:
if it HAD been running (default strategy_params: htf=10m/ltf=3m,
direction_mode=BOTH, itm_offset=50pts), would today's real price action have
produced a retest entry on NIFTY and/or SENSEX?

Reuses the exact same pure/shared functions the live engine
(strategies/fvg/engine.py) itself calls -- no reimplementation of the
detection logic:
  - strategies/d1_trap_option/book.py: _fetch_1m_bars (historical, cached),
    _fetch_intraday_5m (today's real 1-minute bars, despite the name),
    _resample/_mtf_bucket/_build_bar (bar bucketing), _upstox_key_for.
  - strategies/fvg/detector.py: find_swing_points, detect_fvg,
    tag_high_liquidity, update_fvg_state (the actual FVG state machine).
  - strategies/fvg/engine.py: _next_week_expiry + the module's own
    HTF/LTF/ITM-offset/risk-cap/entry-cutoff constants, so this script's
    replay uses the SAME validated-baseline defaults as the live book,
    not independently guessed numbers.

Scope: this checks the SPOT-SIDE entry trigger only (does a high-liquidity
MITIGATED FVG retest actually fire within the entry window, same gating the
live engine applies). It does NOT replay post-entry option-premium P&L --
the live engine's SL/TSL trigger off the position's own real option premium
(Topic.OPTION_TICK), which requires per-strike historical option candle data
this script does not fetch. It also does NOT verify a live premium tick
existed for the computed strike at the trigger instant (a real, separate
gate in _open_position) -- flagged explicitly in the output as a known
scope limit, not silently assumed away.

Run on the box with a real Upstox access_token (data/clients.db) -- e.g. EC2:
    python3 scripts/fvg_today_check.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.global_config import GlobalConfig, IST  # noqa: E402
from data_layer.client_db import ClientDB  # noqa: E402
from data_layer.instrument_registry import REGISTRY  # noqa: E402
from strategies.d1_trap_option.book import (  # noqa: E402
    _build_bar,
    _fetch_1m_bars,
    _fetch_intraday_5m,
    _mtf_bucket,
    _resample,
    _upstox_key_for,
)
from strategies.fvg.detector import (  # noqa: E402
    detect_fvg,
    find_swing_points,
    tag_high_liquidity,
    update_fvg_state,
)
from strategies.fvg.engine import (  # noqa: E402
    _ATM_ROUND_STEP,
    _DEFAULT_HTF_MINS,
    _DEFAULT_ITM_OFFSET_PTS,
    _DEFAULT_LTF_MINS,
    _ENTRY_CUTOFF,
    _HIST_WARMUP_DAYS,
    _MAX_RISK_RS_PER_LOT,
    _OPTION_DELTA_APPROX,
    _SESSION_OPEN,
    _next_week_expiry,
)

UNDERLYINGS = ["NIFTY", "SENSEX"]


def _risk_within_cap(spot_sl_distance: float, lot_size: int) -> bool:
    est_premium_distance = spot_sl_distance * _OPTION_DELTA_APPROX
    est_risk_rs = est_premium_distance * lot_size
    return est_risk_rs <= _MAX_RISK_RS_PER_LOT


async def check_one(underlying: str, token: str, cfg: GlobalConfig) -> None:
    print(f"\n{'='*70}\n{underlying}  (HTF={_DEFAULT_HTF_MINS}m LTF={_DEFAULT_LTF_MINS}m "
          f"itm_offset={_DEFAULT_ITM_OFFSET_PTS}pts direction_mode=BOTH -- validated defaults)")

    key = _upstox_key_for(underlying)
    lot_size = int(cfg.exchange.lot_sizes.get(underlying, 75))
    today = datetime.now(IST).date()

    # Multi-day baseline (excludes today) -- same role as _startup_load's
    # historical fetch: HTF structure/swings + PDH/PDL, computed BEFORE
    # today's own bars are folded in one at a time below.
    hist_start = today - timedelta(days=_HIST_WARMUP_DAYS)
    hist_end = today - timedelta(days=1)
    bars_1m_hist = await asyncio.to_thread(_fetch_1m_bars, key, hist_start, hist_end, token)
    if not bars_1m_hist:
        print("  SKIP: no historical 1-min bars fetched (fetch failed/empty).")
        return

    htf_bars = _resample(bars_1m_hist, _DEFAULT_HTF_MINS)
    ltf_bars = _resample(bars_1m_hist, _DEFAULT_LTF_MINS)
    htf_swings = find_swing_points(htf_bars)

    pdh = pdl = None
    if htf_bars:
        last_day = htf_bars[-1].timestamp.date()
        prior_day_bars = [b for b in htf_bars if b.timestamp.date() == last_day]
        if prior_day_bars:
            pdh = max(b.high for b in prior_day_bars)
            pdl = min(b.low for b in prior_day_bars)
    print(f"  Baseline: {len(htf_bars)} HTF / {len(ltf_bars)} LTF bars over "
          f"{hist_start}..{hist_end}. PDH={pdh} PDL={pdl}")

    # Today's real 1-minute bars (despite the misleading function name --
    # confirmed in strategies/fvg/engine.py's own _warmup_intraday docstring).
    bars_today_1m = await asyncio.to_thread(_fetch_intraday_5m, key, token)
    if not bars_today_1m:
        print("  RESULT: no intraday data for today (market holiday, not yet "
              "open, or fetch failed) -- cannot verify.")
        return

    # Bucket today's 1-min bars into closed LTF bars (same accumulation loop
    # as _warmup_intraday).
    closed_ltf_today = []
    bucket_open = None
    bucket_1m = []
    for bar in bars_today_1m:
        if bar.timestamp.time() < _SESSION_OPEN:
            continue
        b_open = _mtf_bucket(bar.timestamp, _DEFAULT_LTF_MINS)
        if bucket_open is None:
            bucket_open = b_open
        elif b_open != bucket_open:
            if bucket_1m:
                closed_ltf_today.append(_build_bar(bucket_open, bucket_1m))
            bucket_open = b_open
            bucket_1m = []
        bucket_1m.append(bar)
    if bucket_1m:
        closed_ltf_today.append(_build_bar(bucket_open, bucket_1m))
    print(f"  Real today's intraday: {len(bars_today_1m)} 1m bars -> "
          f"{len(closed_ltf_today)} closed {_DEFAULT_LTF_MINS}m LTF bars "
          f"({closed_ltf_today[0].timestamp if closed_ltf_today else '?'} -> "
          f"{closed_ltf_today[-1].timestamp if closed_ltf_today else '?'})")

    fvgs: list = []
    known_fvg_ts: set = set()
    current_htf_open = None
    current_htf_sub: list = []
    entry = None

    for bar in closed_ltf_today:
        ltf_bars.append(bar)

        htf_open = _mtf_bucket(bar.timestamp, _DEFAULT_HTF_MINS)
        if current_htf_open is None:
            current_htf_open = htf_open
        elif htf_open != current_htf_open:
            if current_htf_sub:
                closed_htf = _build_bar(current_htf_open, current_htf_sub)
                htf_bars.append(closed_htf)
                htf_swings = find_swing_points(htf_bars)
            current_htf_open = htf_open
            current_htf_sub = []
        current_htf_sub.append(bar)

        todays_ltf = [b for b in ltf_bars if b.timestamp.date() == today]
        found = detect_fvg(todays_ltf)
        for fvg in found:
            if fvg["candle3_ts"] in known_fvg_ts:
                continue
            tag_high_liquidity(fvg, htf_bars, htf_swings, pdh=pdh, pdl=pdl)
            fvgs.append(fvg)
            known_fvg_ts.add(fvg["candle3_ts"])

        for fvg in fvgs:
            update_fvg_state(fvg, bar)

        if entry is None and bar.timestamp.time() < _ENTRY_CUTOFF:
            for fvg in fvgs:
                if not fvg["high_liquidity"] or fvg["state"] != "MITIGATED":
                    continue
                direction = "LONG" if fvg["direction"] == "BULLISH" else "SHORT"
                sl_price = fvg["candle1_low"] if direction == "LONG" else fvg["candle1_high"]
                entry_price = bar.close
                sl_distance = abs(entry_price - sl_price)
                if sl_distance <= 0:
                    continue
                if not _risk_within_cap(sl_distance, lot_size):
                    fvg["state"] = "INVALIDATED"
                    continue
                atm = round(entry_price / _ATM_ROUND_STEP) * _ATM_ROUND_STEP
                if direction == "LONG":
                    strike, opt_type = int(atm - _DEFAULT_ITM_OFFSET_PTS), "CE"
                else:
                    strike, opt_type = int(atm + _DEFAULT_ITM_OFFSET_PTS), "PE"
                entry = dict(ts=bar.timestamp, direction=direction, opt_type=opt_type,
                             strike=strike, spot=entry_price, sl_spot=sl_price,
                             zone=(fvg["zone_lo"], fvg["zone_hi"]))
                fvg["state"] = "INVALIDATED"
                break

    hl_fvgs = [f for f in fvgs if f["high_liquidity"]]
    print(f"  Today's FVG pool: {len(fvgs)} total, {len(hl_fvgs)} high-liquidity "
          f"(states: {[f['state'] for f in hl_fvgs]})")

    if entry is None:
        print("  RESULT: NO ENTRY today -- no high-liquidity FVG reached a "
              "MITIGATED retest within the entry window (09:15-14:30).")
        return

    try:
        await asyncio.to_thread(REGISTRY.load_sync, underlying, token)
    except Exception:
        pass
    expiry = _next_week_expiry(underlying, entry["ts"].date())

    print(f"  RESULT: WOULD HAVE ENTERED {entry['direction']} ({entry['opt_type']}) "
          f"at {entry['ts']} spot={entry['spot']:.2f}")
    print(f"    Strike={entry['strike']} expiry={expiry or 'UNRESOLVED'} "
          f"spot_sl={entry['sl_spot']:.2f} fvg_zone={entry['zone']}")
    print("    CAVEAT: spot-side trigger only. Live entry also requires a real "
          "option-premium tick for this exact strike/expiry to exist at the "
          "trigger instant (_open_position's 'no live premium -> skip' gate) -- "
          "not verified here. Post-entry option-premium SL/TSL P&L is not "
          "replayed by this script.")


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

    for underlying in UNDERLYINGS:
        await check_one(underlying, token, cfg)

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
