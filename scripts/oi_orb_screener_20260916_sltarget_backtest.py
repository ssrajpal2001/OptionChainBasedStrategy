"""
scripts/oi_orb_screener_20260916_sltarget_backtest.py

Direct user follow-up, 2026-09-16: "run the same with the entry we just
saw and the SL which is there along with target." Takes the 5 REAL
trades already found+validated by today's strong-quadrant OI-confirm
backtest (oi_orb_screener_20260916_live_oi_confirm_backtest.py, sign-bug
fixed) -- their real (symbol, side, entry_ts, entry_spot_price) exactly
as already confirmed -- and re-runs the exit side ONLY, adding the
previously-live (DISABLED 2026-09-07, "only exit is HA+StochRSI... remove
other exit condition") option-premium SL/target ratchet racing the
already-validated HA+StochRSI(15m) exit -- whichever fires FIRST
chronologically wins, else EOD fallback. Reuses the REAL screener.py
primitives (is_adverse_bar_close, pool_sl_from_adverse_lows,
compute_option_premium_target, check_option_premium_exit) directly, not
reimplemented -- same live formula: 5-min option bars, own session VWAP
of the option's own typical price as an ATP proxy (KNOWN approximation,
same unweighted-typical-price class already flagged in every other
script today -- no real tick volume/broker-ATP history available),
rr_multiple=2.0 target, pool SL requiring 2+ clustered adverse lows
(tol=1%).

MUST run on EC2 (real Upstox2 access token + real intraday history).

Usage: python scripts/oi_orb_screener_20260916_sltarget_backtest.py
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
from strategies.core.candle_indicators import (
    to_heikin_ashi, to_n_min_bars, compute_stoch_rsi, ha_stoch_shape_exit_signal,
)
from strategies.oi_orb_screener import stock_resolve
from strategies.oi_orb_screener.screener import (
    VwapState, is_adverse_bar_close, pool_sl_from_adverse_lows,
    compute_option_premium_target, check_option_premium_exit,
)

TRADE_DATE = "2026-09-16"
TODAY = date.fromisoformat(TRADE_DATE)
EOD_TIME = "15:15"
RSI_PERIOD, STOCH_PERIOD, SMOOTH = 9, 9, 3
VWAP_SL_TF_MIN = 5     # live default (book_manager.py _DEFAULT_PARAMS)
RR_MULTIPLE = 2.0      # live default

# The 5 real strong-quadrant trades already validated today (symbol, side,
# entry_ts, entry_spot_price) -- taken as-given, not recomputed here.
KNOWN_TRADES = [
    ("PAYTM", "CALL", datetime(2026, 9, 16, 10, 45, tzinfo=IST), 1748.80),
    ("BLUESTARCO", "CALL", datetime(2026, 9, 16, 13, 28, tzinfo=IST), 1494.40),
    ("OFSS", "PUT", datetime(2026, 9, 16, 11, 13, tzinfo=IST), 11645.00),
    ("PREMIERENE", "PUT", datetime(2026, 9, 16, 14, 29, tzinfo=IST), 901.80),
    ("NYKAA", "PUT", datetime(2026, 9, 16, 13, 25, tzinfo=IST), 327.30),
]
# Baseline (pure HA+StochRSI, EOD-only) PnL, from the already-validated run.
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


def _floor_key(ts, tf_min):
    floored = (ts.minute // tf_min) * tf_min
    return f"{ts.hour:02d}:{floored:02d}"


async def simulate(symbol, side, entry_ts, entry_price_spot, token):
    eq_key = stock_resolve.resolve_eq_instrument_key(symbol)
    if not eq_key:
        return {"reason": "NO_EQ_KEY"}
    opt_type = "CE" if side == "CALL" else "PE"
    contract = await stock_resolve.resolve_contract_async(symbol, entry_price_spot, opt_type)
    if contract is None:
        return {"reason": "could not resolve a real tradable contract"}
    opt_rows = await hc.fetch_upstox_intraday_1m(contract.upstox_key, token)
    opt_bars = _to_bars(opt_rows)
    if not opt_bars:
        return {"reason": "no real option premium history"}

    entry_floor = entry_ts.replace(second=0, microsecond=0)
    entry_candidates = [b for b in opt_bars if b.ts <= entry_ts]
    if not entry_candidates:
        return {"reason": "no option bar at/before entry"}
    entry_price = entry_candidates[-1].close

    warm_rows = await hc.fetch_upstox_warm_1m(eq_key, token, min_bars=400)
    all_eq_bars = _to_bars(warm_rows)

    # -- HA+StochRSI(15m) leg, identical to the already-validated baseline --
    ha_exit_ts = None
    last_evaluated = None
    for i in range(15, len(all_eq_bars) + 1):
        window = all_eq_bars[:i]
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
            continue
        if last_evaluated == latest.ts:
            continue
        last_evaluated = latest.ts
        closes = [b.close for b in ha_15m]
        k, d = compute_stoch_rsi(closes, RSI_PERIOD, STOCH_PERIOD, SMOOTH)
        if ha_stoch_shape_exit_signal(latest, k[-1], d[-1], side, inclusive=True):
            ha_exit_ts = cur_ts
            break

    # -- SL/target ratchet leg, on the OPTION's own premium, racing in parallel --
    vwap_state = VwapState()
    for b in opt_bars:
        if b.ts < entry_floor:
            typical = (b.high + b.low + b.close) / 3.0
            vwap_state.update(symbol, typical, 1.0)

    sl_target_exit_ts = sl_target_exit_price = sl_target_reason = None
    adverse_lows, events = [], []
    live_sl = live_target = None
    bar_key = bar_acc = None

    for b in opt_bars:
        typical = (b.high + b.low + b.close) / 3.0
        vwap_state.update(symbol, typical, 1.0)
        if b.ts < entry_floor:
            continue
        vwap_now = vwap_state.current(symbol)

        if live_sl is not None or live_target is not None:
            hit = check_option_premium_exit(live_sl, live_target, b.close)
            if hit is not None:
                sl_target_exit_ts, sl_target_exit_price, sl_target_reason = b.ts, b.close, hit
                break

        key = _floor_key(b.ts, VWAP_SL_TF_MIN)
        if bar_key is None:
            bar_key, bar_acc = key, {"h": b.close, "l": b.close, "c": b.close, "ts": b.ts}
        elif key != bar_key:
            closed = bar_acc
            if vwap_now is not None and is_adverse_bar_close(closed["c"], vwap_now):
                adverse_lows.append(closed["l"])
                new_sl = pool_sl_from_adverse_lows(adverse_lows)
                if new_sl is not None and new_sl != live_sl:
                    live_sl = new_sl
                    new_target = compute_option_premium_target(entry_price, live_sl, RR_MULTIPLE)
                    if new_target is not None:
                        live_target = new_target
                    events.append((closed["ts"], live_sl, live_target))
            bar_key, bar_acc = key, {"h": b.close, "l": b.close, "c": b.close, "ts": b.ts}
        else:
            bar_acc["h"] = max(bar_acc["h"], b.close)
            bar_acc["l"] = min(bar_acc["l"], b.close)
            bar_acc["c"] = b.close

    candidates = []
    if ha_exit_ts is not None:
        candidates.append(("ha_stoch_exit", ha_exit_ts))
    if sl_target_exit_ts is not None:
        candidates.append((f"{sl_target_reason}_hit", sl_target_exit_ts))
    if candidates:
        candidates.sort(key=lambda x: x[1])
        exit_reason, exit_ts = candidates[0]
    else:
        exit_ts = datetime.combine(TODAY, datetime.strptime(EOD_TIME, "%H:%M").time(), tzinfo=IST)
        exit_reason = "eod_squareoff (neither HA+StochRSI nor SL/target fired)"

    if exit_reason in ("sl_hit", "target_hit"):
        exit_price = sl_target_exit_price
    else:
        exit_candidates = [b for b in opt_bars if b.ts <= exit_ts]
        exit_price = exit_candidates[-1].close if exit_candidates else None

    pnl = round(exit_price - entry_price, 2) if (entry_price is not None and exit_price is not None) else None

    return {"entry_price": entry_price, "exit_price": exit_price, "exit_ts": exit_ts,
            "exit_reason": exit_reason, "pnl": pnl, "sl_target_events": events,
            "final_sl": live_sl, "final_target": live_target}


async def main():
    token = _access_token()
    print("=" * 130)
    print("OI-ORB Screener -- 2026-09-16 SL/TARGET-AUGMENTED re-run of the 5 known strong-quadrant trades")
    print(f"SL/target mechanic (previously live, disabled 2026-09-07): {VWAP_SL_TF_MIN}-min option-premium "
          f"bars, own session VWAP (typical-price proxy) adverse-close re-arm, pool SL (2+ clustered lows), "
          f"rr_multiple={RR_MULTIPLE} target. Races the already-validated HA+StochRSI(15m) exit -- first to "
          f"fire chronologically wins, else EOD.")
    print("=" * 130)

    total_baseline = total_new = 0.0
    for symbol, side, entry_ts, entry_spot in KNOWN_TRADES:
        print(f"\n{'-' * 130}\n{symbol} ({side}), entry@{entry_ts.strftime('%H:%M')} spot={entry_spot}\n{'-' * 130}")
        r = await simulate(symbol, side, entry_ts, entry_spot, token)
        if r.get("pnl") is None:
            print(f"  FAILED: {r.get('reason')}")
            continue
        print(f"  entry opt={r['entry_price']}")
        if r["sl_target_events"]:
            for ts, sl, tgt in r["sl_target_events"]:
                print(f"  SL RE-ARMED @ {ts.strftime('%H:%M')}: sl={sl:.2f} target={f'{tgt:.2f}' if tgt else 'n/a'}")
        else:
            print(f"  SL/target: never armed (no 2+ clustered adverse {VWAP_SL_TF_MIN}-min bar lows)")
        print(f"  EXIT: {r['exit_reason']} @ {r['exit_ts'].strftime('%H:%M:%S')} opt={r['exit_price']}")
        baseline = BASELINE_PNL[symbol]
        print(f"  PNL: baseline(HA+StochRSI only)={baseline:+.2f}  ->  with-SL/target={r['pnl']:+.2f}  "
              f"(delta {r['pnl'] - baseline:+.2f})")
        total_baseline += baseline
        total_new += r["pnl"]

    print("\n" + "=" * 130)
    print(f"TOTAL baseline (HA+StochRSI only, already validated): {total_baseline:+.2f} pts")
    print(f"TOTAL with SL/target racing HA+StochRSI:               {total_new:+.2f} pts")
    print("CAVEAT: n=5 trades, single real day. SL/target's ATP proxy uses unweighted typical-price session "
          "VWAP (no real broker-ATP tick history available historically) -- directionally informative only.")
    print("=" * 130)


if __name__ == "__main__":
    asyncio.run(main())
