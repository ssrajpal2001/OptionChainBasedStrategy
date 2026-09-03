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

Scope: this checks the SPOT-SIDE entry trigger (does a high-liquidity
MITIGATED FVG retest actually fire within the entry window, same gating the
live engine applies), AND, if an entry fires, fetches the REAL option
premium history for that exact strike/expiry (Upstox intraday 1-min candles)
and replays the live engine's own option-native SL/step-locked-TSL/
stagnation/EOD exit logic (_check_exit_premium/_check_stagnation_exit in
strategies/fvg/engine.py) against those real closes to report an actual
profit/loss. This is a 1-MINUTE CANDLE-CLOSE approximation of the live
engine's per-TICK evaluation -- a real intra-minute wick through the SL/TSL
level between candle closes would not be caught here, so a reported "still
running"/near-miss result is a slight optimistic bias vs what tick-level
execution would show. It also does NOT verify a live premium tick existed
for the computed strike at the exact trigger instant (a real, separate gate
in _open_position) -- flagged explicitly in the output as a known scope
limit, not silently assumed away.

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
from data_layer.historical_candles import fetch_upstox_intraday_1m  # noqa: E402
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
    _DEFAULT_FIRST_LOCK_PCT,
    _DEFAULT_HTF_MINS,
    _DEFAULT_INITIAL_SL_PCT,
    _DEFAULT_ITM_OFFSET_PTS,
    _DEFAULT_LTF_MINS,
    _DEFAULT_STEP_LOCK_PCT,
    _DEFAULT_STEP_PCT,
    _DEFAULT_TRAIL_TRIGGER_PCT,
    _ENTRY_CUTOFF,
    _EOD_TIME,
    _HIST_WARMUP_DAYS,
    _MAX_RISK_RS_PER_LOT,
    _OPTION_DELTA_APPROX,
    _SESSION_OPEN,
    _STAGNATION_MINUTES,
    _next_week_expiry,
)

UNDERLYINGS = ["NIFTY", "SENSEX"]


def _risk_within_cap(spot_sl_distance: float, lot_size: int) -> bool:
    est_premium_distance = spot_sl_distance * _OPTION_DELTA_APPROX
    est_risk_rs = est_premium_distance * lot_size
    return est_risk_rs <= _MAX_RISK_RS_PER_LOT


def _replay_option_pnl(entry: dict, premium_candles: list, lot_size: int) -> dict:
    """Mirrors FVGStrategy._check_exit_premium / _check_stagnation_exit / EOD
    square-off EXACTLY (same formulas, same defaults), replayed against real
    1-min option candle closes instead of live ticks. Returns a result dict
    or {'error': ...} if there's no usable premium data."""
    decision_ts = entry["ts"] + timedelta(minutes=_DEFAULT_LTF_MINS)
    candles = [c for c in premium_candles if datetime.fromisoformat(c["ts"]) >= decision_ts]
    if not candles:
        return {"error": "no real premium data at/after the entry moment"}

    entry_premium = float(candles[0]["close"])
    entry_actual_ts = candles[0]["ts"]
    if entry_premium <= 0:
        return {"error": f"entry-moment premium is {entry_premium} (bad/zero data)"}

    pct_sl = entry_premium * (1 - _DEFAULT_INITIAL_SL_PCT)
    cap_sl = entry_premium - (_MAX_RISK_RS_PER_LOT / lot_size)
    premium_sl = max(pct_sl, cap_sl)
    high_lock_pct = 0.0

    exit_reason, exit_price, exit_ts = None, None, None
    for c in candles[1:]:
        ts = datetime.fromisoformat(c["ts"])
        premium = float(c["close"])
        if ts.time() >= _EOD_TIME:
            exit_reason, exit_price, exit_ts = "eod", premium, c["ts"]
            break
        profit_pct = (premium - entry_premium) / entry_premium
        if profit_pct >= _DEFAULT_TRAIL_TRIGGER_PCT:
            steps = int((profit_pct - _DEFAULT_TRAIL_TRIGGER_PCT) // _DEFAULT_STEP_PCT)
            calc_lock = _DEFAULT_FIRST_LOCK_PCT + steps * _DEFAULT_STEP_LOCK_PCT
            high_lock_pct = max(high_lock_pct, calc_lock)
        stop_price = entry_premium * (1 + high_lock_pct) if high_lock_pct > 0 else premium_sl
        if premium <= stop_price:
            exit_reason = "tsl_hit" if high_lock_pct > 0 else "sl_hit"
            exit_price, exit_ts = premium, c["ts"]
            break
        if high_lock_pct == 0 and (ts - decision_ts) >= timedelta(minutes=_STAGNATION_MINUTES):
            exit_reason, exit_price, exit_ts = "stagnation_exit", premium, c["ts"]
            break

    if exit_reason is None:
        exit_reason = "still running (real data ends before an exit trigger)"
        exit_price, exit_ts = float(candles[-1]["close"]), candles[-1]["ts"]

    pnl_per_lot = (exit_price - entry_premium) * lot_size
    return {
        "entry_premium": entry_premium, "entry_actual_ts": entry_actual_ts,
        "exit_reason": exit_reason, "exit_price": exit_price, "exit_ts": exit_ts,
        "high_lock_pct": high_lock_pct, "pnl_per_lot": pnl_per_lot,
        "pnl_pct": (exit_price - entry_premium) / entry_premium * 100,
    }


async def check_one(underlying: str, token: str, cfg: GlobalConfig) -> None:
    key = _upstox_key_for(underlying)
    lot_size = int(cfg.exchange.lot_sizes.get(underlying, 75))
    # 2026-08-06 fix: matches strategies/fvg/engine.py's own default resolution --
    # "1-strike ITM" means one real strike on THIS underlying's own grid (e.g. 100pts
    # for SENSEX/BANKNIFTY), not a hardcoded 50pts (NIFTY's grid) applied everywhere.
    # A flat 50pt offset previously computed an off-grid, never-listed SENSEX strike.
    itm_offset_pts = int(cfg.exchange.strike_steps.get(underlying, _DEFAULT_ITM_OFFSET_PTS))
    today = datetime.now(IST).date()

    print(f"\n{'='*70}\n{underlying}  (HTF={_DEFAULT_HTF_MINS}m LTF={_DEFAULT_LTF_MINS}m "
          f"itm_offset={itm_offset_pts}pts direction_mode=BOTH -- validated defaults)")

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
                    strike, opt_type = int(atm - itm_offset_pts), "CE"
                else:
                    strike, opt_type = int(atm + itm_offset_pts), "PE"
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
    print("    NOTE: live entry also requires a real option-premium tick for this "
          "exact strike/expiry to exist at the trigger instant "
          "(_open_position's 'no live premium -> skip' gate) -- not verified here.")

    if not expiry:
        print("    P&L: SKIPPED -- could not resolve expiry, cannot fetch real premium.")
        return
    opt_key = REGISTRY.get_upstox_key(underlying, expiry, entry["strike"], entry["opt_type"])
    if not opt_key:
        print(f"    P&L: SKIPPED -- no Upstox instrument key resolved for "
              f"{underlying} {entry['strike']}{entry['opt_type']} exp={expiry}.")
        return
    premium_candles = await fetch_upstox_intraday_1m(opt_key, token)
    if not premium_candles:
        print("    P&L: SKIPPED -- no real premium candles returned for this strike today.")
        return

    result = _replay_option_pnl(entry, premium_candles, lot_size)
    if "error" in result:
        print(f"    P&L: SKIPPED -- {result['error']}.")
        return
    sign = "+" if result["pnl_per_lot"] >= 0 else ""
    print(f"    REAL PREMIUM REPLAY: entry_premium={result['entry_premium']:.2f} "
          f"(at {result['entry_actual_ts']}) -> exit={result['exit_reason']} "
          f"@ {result['exit_price']:.2f} (at {result['exit_ts']})")
    print(f"    P&L per lot (qty={lot_size}): {sign}Rs{result['pnl_per_lot']:.2f} "
          f"({sign}{result['pnl_pct']:.1f}%)"
          + (f", TSL locked at {result['high_lock_pct']*100:.1f}%" if result["high_lock_pct"] > 0 else ""))
    print("    (1-min candle-close approximation of tick-level exit evaluation -- "
          "a real intra-minute wick through SL/TSL between candle closes wouldn't "
          "be caught here, so this is a slight optimistic bias.)")


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
