"""
scripts/fvg_backtest.py — walk-forward backtest for the FVG strategy against
real NIFTY spot 1-minute history AND real option premium history, using the
SAME pure functions as the live engine (strategies/fvg/detector.py) so the
backtest is a faithful regression of production logic, not a re-implementation.

2026-08-03 rewrite, per explicit spec:
  - Window: last 7 TRADING days of data available (2026-07-23..07-31). The
    literally-requested 07-25/08-01 bookends are both Saturdays (non-trading
    days) -- substituted with the nearest real 7-session window ending at
    the last date this dataset has (07-31).
  - Contract mapping: 1-strike ITM (NIFTY strike step = 50pts), not the
    earlier 200pt/4-strike offset.
  - Option-native exits: SL = tighter of a flat premium % and the hard
    Rs/lot risk cap; step-locked TSL (no fixed TP) once profit crosses the
    trigger; ~40min stagnation exit (bar count scaled to LTF_MINS) if the
    TSL never activates -- same shape as strategies/fvg/engine.py's
    option-native rewrite, run here against real historical premium instead
    of live ticks. Defaults below are the validated baseline
    (HTF=10m/LTF=3m, "Wider Runner" TSL tier from scripts/fvg_tsl_sweep.py).

Three-phase design (entries only need spot; exits need real premium, which
depends on knowing which strikes entries actually picked -- so signal
discovery runs first, then the exact strikes needed are fetched, then exits
are resolved against real premium):
  Phase 1 -- signal discovery: walk the spot data (unchanged FVG/MSS/sweep
    detection) to find every retest-entry signal within the 7-day window.
    A provisional spot-based exit is used ONLY to gate "one position at a
    time" the same way the live book does -- it is discarded afterward.
  Phase 2 -- fetch real 1-min premium for every (strike, option_type) any
    signal in Phase 1 needs (skips strikes already cached on disk).
  Phase 3 -- resolve the REAL option-native exit for every signal by
    independently walking forward through real premium bars from its own
    entry_ts, applying the SL/TP/stagnation/EOD rules above.
"""
import sys
sys.path.insert(0, ".")
from datetime import date, timedelta, time as dtime
from urllib.parse import quote as _q

import pandas as pd

from data_layer.historical_candles import _http_get_json, _parse_candles
from data_layer.instrument_registry import REGISTRY
from strategies.d1_trap_option.bear_only_book import _resample as _df_resample, _to_bars
from strategies.d1_trap_option.book import _Bar
from strategies.fvg.detector import (
    detect_fvg,
    find_swing_points,
    tag_high_liquidity,
    update_fvg_state,
)

SPOT_PATH = "data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet"
PREMIUM_DIR = "data/d1trap_fractal_cache/fvg_7day_strikes"
TOKEN_PATH = ("C:/Users/SERVER/AppData/Local/Temp/claude/e--AlgoSoft-OptionChainBasedStrategy/"
              "3f952902-2e64-455f-be1c-fac0a7378cbc/scratchpad/upstox_token.txt")

HTF_MINS, LTF_MINS = 10, 3     # 2026-08-03 validated baseline (scripts/fvg_tf_sweep.py)
HIST_WARMUP_DAYS = 14
ENTRY_CUTOFF = dtime(14, 30)
EOD_TIME = dtime(15, 15)
MAX_FVG_AGE_DAYS = 14
LOT_SIZE = 65
ATM_ROUND_STEP = 100
ITM_OFFSET_PTS = 50            # 1-strike ITM (NIFTY step = 50pts)

# provisional (Phase-1 gating only, discarded) spot-based numbers
GATE_OPTION_DELTA_APPROX = 0.5
GATE_MAX_RISK_RS_PER_LOT = 2000.0
GATE_MIN_RR = 2.5

# REAL option-native exit rules (Phase 3, per explicit spec)
PREMIUM_SL_PCT = 0.20          # initial 20% max loss on premium (also capped by
                                # MAX_RISK_RS_PER_LOT below, whichever is tighter)
MAX_RISK_RS_PER_LOT = 2000.0   # same hard cap as bear_only_book.py / engine.py
STAGNATION_LTF_BARS = 8        # 8 x 5m = 40 min (default LTF_MINS=5; scaled to
                                # `max(1, 40 // ltf_mins)` when a different LTF is passed)

# step-locked TSL defaults -- 2026-08-03 re-tune: the "Wider Runner" 25%
# trigger rarely fired before the 40min stagnation timer expired (14/16
# trades exited via stagnation, only 1 via TSL) -- lowered the trigger to
# 15% so the TSL actually engages on realistic 10m/3m option premium swings
# instead of being dead weight. Replaces "Wider Runner"/"Baseline"/fixed
# 1:2 R:R, all tested earlier in scripts/fvg_tsl_sweep.py.
TSL_BASE_PCT = 0.15            # profit_pct trigger for the first lock
TSL_BASE_LOCK_PCT = 0.08       # locked-in fraction once triggered
TSL_STEP_PCT = 0.10            # every further this much profit...
TSL_STEP_LOCK_PCT = 0.05       # ...locks another this much (repeating)

BACKTEST_START = date(2026, 7, 23)   # last 7 TRADING days ending at the
BACKTEST_END = date(2026, 7, 31)     # latest date this dataset has (07-25/08-01
                                      # requested are both Saturdays)


def load_spot():
    df = pd.read_parquet(SPOT_PATH)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.sort_values("datetime").reset_index(drop=True)


def roll_pdh_pdl(htf_bars, before_ts):
    prior = [b for b in htf_bars if b.timestamp < before_ts]
    if not prior:
        return None, None
    last_day = prior[-1].timestamp.date()
    day_bars = [b for b in prior if b.timestamp.date() == last_day]
    return max(b.high for b in day_bars), min(b.low for b in day_bars)


def rebuild_fvg_pool(fvgs, known_ts, ltf_bars, htf_bars, htf_swings, pdh, pdl, today=None):
    """Intraday-only (2026-08-03 fix): only scan TODAY's ltf_bars for FVGs --
    a gap that formed on an earlier day and never got retested has no
    business still being tradeable today (confirmed live: a real backtest
    trade fired off a 24-day-old FVG before this fix). HTF structure
    (htf_bars/htf_swings/pdh/pdl) legitimately stays multi-day; only the FVG
    gap + its retest are same-session-only. Caller is responsible for
    clearing `fvgs`/`known_ts` at the start of each new day (see
    discover_signals)."""
    scan_bars = [b for b in ltf_bars if b.timestamp.date() == today] if today is not None else ltf_bars
    found = detect_fvg(scan_bars)
    for fvg in found:
        if fvg["candle3_ts"] in known_ts:
            continue
        tag_high_liquidity(fvg, htf_bars, htf_swings, pdh=pdh, pdl=pdl)
        fvgs.append(fvg)
        known_ts.add(fvg["candle3_ts"])


def gate_risk_within_cap(spot_sl_distance):
    est_premium_distance = spot_sl_distance * GATE_OPTION_DELTA_APPROX
    return est_premium_distance * LOT_SIZE <= GATE_MAX_RISK_RS_PER_LOT


def _bucket(ts, mins):
    open_dt = ts.replace(hour=9, minute=15, second=0, microsecond=0)
    elapsed = max(0, int((ts - open_dt).total_seconds() // 60))
    return open_dt + timedelta(minutes=(elapsed // mins) * mins)


def _close_ltf_bar(bucket_open, rows):
    return _Bar(timestamp=bucket_open, open=rows[0]["open"], high=max(r["high"] for r in rows),
                low=min(r["low"] for r in rows), close=rows[-1]["close"])


# ══════════════════════════════════════════════════════════════════════════
# Phase 1 — signal discovery (spot-only, provisional gating exit)
# ══════════════════════════════════════════════════════════════════════════

def discover_signals(spot, htf_mins=HTF_MINS, ltf_mins=LTF_MINS, itm_offset_pts=ITM_OFFSET_PTS):
    days = sorted(spot["datetime"].dt.date.unique())

    htf_bars, ltf_bars, htf_swings = [], [], []
    fvgs, known_fvg_ts = [], set()
    pdh = pdl = None
    position = None
    signals = []

    def close_gate(exit_price, exit_ts):
        nonlocal position
        position = None

    for day in days:
        # Intraday-only (2026-08-03 fix): the FVG pool is wiped at the start
        # of every new day -- a gap from an earlier day (however recently)
        # can never trigger an entry today. HTF structure (htf_bars/
        # htf_swings/pdh/pdl) legitimately stays multi-day.
        fvgs.clear()
        known_fvg_ts.clear()

        start = day - timedelta(days=HIST_WARMUP_DAYS)
        hist = spot[(spot["datetime"].dt.date >= start) & (spot["datetime"].dt.date < day)]
        if len(hist) >= 30:
            htf_bars = _to_bars(_df_resample(hist, htf_mins))
            ltf_bars = _to_bars(_df_resample(hist, ltf_mins))
            htf_swings = find_swing_points(htf_bars)
            pdh, pdl = roll_pdh_pdl(htf_bars, pd.Timestamp(day, tz=htf_bars[0].timestamp.tz) if htf_bars else None)
            rebuild_fvg_pool(fvgs, known_fvg_ts, ltf_bars, htf_bars, htf_swings, pdh, pdl, today=day)

        day_bars = spot[spot["datetime"].dt.date == day].reset_index(drop=True)
        if day_bars.empty:
            continue

        current_htf_open = current_ltf_open = None
        current_htf_5m, current_ltf_1m = [], []
        day_done = False
        in_window = BACKTEST_START <= day <= BACKTEST_END

        for _, row in day_bars.iterrows():
            ts = row["datetime"]
            if ts.time() < dtime(9, 15):
                continue
            spot_px = row["close"]

            if position is not None:
                if position["direction"] == "LONG":
                    if spot_px <= position["sl"]:
                        close_gate(position["sl"], ts)
                    elif spot_px >= position["tp"]:
                        close_gate(position["tp"], ts)
                else:
                    if spot_px >= position["sl"]:
                        close_gate(position["sl"], ts)
                    elif spot_px <= position["tp"]:
                        close_gate(position["tp"], ts)

            if not day_done and ts.time() >= EOD_TIME and position is not None:
                close_gate(spot_px, ts)
                day_done = True

            htf_open = _bucket(ts, htf_mins)
            if current_htf_open is None:
                current_htf_open = htf_open
            elif htf_open != current_htf_open:
                if current_htf_5m:
                    htf_bars.append(_close_ltf_bar(current_htf_open, current_htf_5m))
                    htf_swings = find_swing_points(htf_bars)
                current_htf_open, current_htf_5m = htf_open, []
            current_htf_5m.append(row)

            ltf_open = _bucket(ts, ltf_mins)
            if current_ltf_open is None:
                current_ltf_open = ltf_open
            elif ltf_open != current_ltf_open:
                if current_ltf_1m:
                    bar = _close_ltf_bar(current_ltf_open, current_ltf_1m)
                    ltf_bars.append(bar)
                    rebuild_fvg_pool(fvgs, known_fvg_ts, ltf_bars, htf_bars, htf_swings, pdh, pdl, today=day)
                    for fvg in fvgs:
                        update_fvg_state(fvg, bar)
                    if position is None and bar.timestamp.time() < ENTRY_CUTOFF:
                        for fvg in fvgs:
                            if not fvg["high_liquidity"] or fvg["state"] != "MITIGATED":
                                continue
                            direction = "LONG" if fvg["direction"] == "BULLISH" else "SHORT"
                            sl_price = fvg["candle1_low"] if direction == "LONG" else fvg["candle1_high"]
                            entry_price = bar.close
                            sl_distance = abs(entry_price - sl_price)
                            if sl_distance <= 0 or not gate_risk_within_cap(sl_distance):
                                fvg["state"] = "INVALIDATED"
                                continue
                            tp_price = (entry_price + sl_distance * GATE_MIN_RR if direction == "LONG"
                                        else entry_price - sl_distance * GATE_MIN_RR)
                            atm = round(entry_price / ATM_ROUND_STEP) * ATM_ROUND_STEP
                            if direction == "LONG":
                                strike, opt_type = int(atm - itm_offset_pts), "CE"
                            else:
                                strike, opt_type = int(atm + itm_offset_pts), "PE"
                            position = dict(direction=direction, entry=entry_price, sl=sl_price, tp=tp_price,
                                             entry_ts=bar.timestamp, strike=strike, option_type=opt_type)
                            if in_window:
                                signals.append(dict(
                                    direction=direction, entry_ts=bar.timestamp,
                                    spot_entry=entry_price, strike=strike, option_type=opt_type, day=day,
                                    spot_sl=sl_price, fvg_zone_lo=fvg["zone_lo"], fvg_zone_hi=fvg["zone_hi"],
                                    fvg_ce=fvg["ce"], candle1_ts=fvg["candle1_ts"], candle3_ts=fvg["candle3_ts"],
                                    candle1_low=fvg["candle1_low"], candle1_high=fvg["candle1_high"],
                                ))
                            fvg["state"] = "INVALIDATED"
                            break
                current_ltf_open, current_ltf_1m = ltf_open, []
            current_ltf_1m.append(row)

    return signals


# ══════════════════════════════════════════════════════════════════════════
# Phase 2 — fetch real premium for exactly the strikes Phase 1 needs
# ══════════════════════════════════════════════════════════════════════════

def fetch_premium(strike, side, token, start, end):
    REGISTRY.load_sync("NIFTY", token)
    expiry = REGISTRY.get_active_expiry("NIFTY", from_date=end)
    key = REGISTRY.get_upstox_key("NIFTY", expiry, strike, side)
    if not key:
        return pd.DataFrame(columns=["datetime", "open", "high", "low", "close"])
    url = f"https://api.upstox.com/v2/historical-candle/{_q(key, safe='')}/1minute/{end.isoformat()}/{start.isoformat()}"
    r = _http_get_json(url, token)
    rows = _parse_candles(r)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.rename(columns={"ts": "datetime"})
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)


def ensure_premium_data(signals):
    import os
    os.makedirs(PREMIUM_DIR, exist_ok=True)
    token = open(TOKEN_PATH).read().strip()
    needed = sorted({(s["strike"], s["option_type"]) for s in signals})
    cache = {}
    for strike, side in needed:
        path = f"{PREMIUM_DIR}/{strike}_{side}.parquet"
        if os.path.exists(path):
            df = pd.read_parquet(path)
            df["datetime"] = pd.to_datetime(df["datetime"])
        else:
            df = fetch_premium(strike, side, token, BACKTEST_START - timedelta(days=1),
                                BACKTEST_END + timedelta(days=1))
            df.to_parquet(path)
        cache[(strike, side)] = df
        print(f"  premium {side}{strike}: {len(df)} rows"
              + (f"  {df['datetime'].min()} .. {df['datetime'].max()}" if not df.empty else "  NO DATA"))
    return cache


# ══════════════════════════════════════════════════════════════════════════
# Phase 3 — resolve option-native exits against real premium
# ══════════════════════════════════════════════════════════════════════════

def price_at(df, ts):
    before = df[df["datetime"] <= ts]
    if not before.empty:
        return float(before.iloc[-1]["close"])
    after = df[df["datetime"] >= ts]
    if not after.empty:
        return float(after.iloc[0]["close"])
    return None


def resolve_trades(signals, premium_cache, spot, ltf_mins=LTF_MINS, stagnation_bars=STAGNATION_LTF_BARS,
                    sl_pct=PREMIUM_SL_PCT, max_risk_rs_per_lot=MAX_RISK_RS_PER_LOT,
                    tsl_base_pct=TSL_BASE_PCT, tsl_base_lock_pct=TSL_BASE_LOCK_PCT,
                    tsl_step_pct=TSL_STEP_PCT, tsl_step_lock_pct=TSL_STEP_LOCK_PCT,
                    eod_time=EOD_TIME):
    """Option-native exit resolution, matching strategies/fvg/engine.py's
    _check_exit_premium/_check_stagnation_exit exactly: SL = tighter of a
    flat sl_pct stop and the hard Rs/lot risk cap; no fixed TP -- a
    staircase TSL locks in profit as it's gained (tsl_base_pct triggers the
    first lock, then every further tsl_step_pct locks another
    tsl_step_lock_pct); stagnation exit only applies while the TSL has never
    activated. Pass stagnation_bars=None to disable the stagnation exit
    entirely (position then only closes on SL/TSL/EOD)."""
    trades = []
    for sig in signals:
        df = premium_cache.get((sig["strike"], sig["option_type"]))
        if df is None or df.empty:
            print(f"  SKIP {sig['option_type']}{sig['strike']} @ {sig['entry_ts']} -- no premium data")
            continue
        entry_premium = price_at(df, sig["entry_ts"])
        if entry_premium is None or entry_premium <= 0:
            print(f"  SKIP {sig['option_type']}{sig['strike']} @ {sig['entry_ts']} -- no entry premium")
            continue

        pct_sl = entry_premium * (1 - sl_pct)
        cap_sl = entry_premium - (max_risk_rs_per_lot / LOT_SIZE)
        sl_premium = max(pct_sl, cap_sl)

        # walk forward through this option's OWN ltf_mins-bucketed bars after entry
        day_spot = spot[spot["datetime"].dt.date == sig["day"]]
        future = day_spot[day_spot["datetime"] > sig["entry_ts"]]
        seen_buckets = set()
        exit_premium, exit_ts, reason = None, None, None
        bars_elapsed = 0
        high_lock_pct = 0.0
        for _, row in future.iterrows():
            ts = row["datetime"]
            bucket = _bucket(ts, ltf_mins)
            if bucket in seen_buckets:
                continue
            seen_buckets.add(bucket)
            bars_elapsed += 1

            premium_now = price_at(df, ts)
            if premium_now is None:
                continue

            profit_pct = (premium_now - entry_premium) / entry_premium
            if profit_pct >= tsl_base_pct:
                steps = int((profit_pct - tsl_base_pct) // tsl_step_pct)
                calc_lock = tsl_base_lock_pct + steps * tsl_step_lock_pct
                high_lock_pct = max(high_lock_pct, calc_lock)
            stop_price = entry_premium * (1 + high_lock_pct) if high_lock_pct > 0 else sl_premium

            if premium_now <= stop_price:
                exit_premium, exit_ts = premium_now, ts
                reason = "tsl_hit" if high_lock_pct > 0 else "sl_hit"
                break
            if stagnation_bars is not None and high_lock_pct <= 0 and bars_elapsed >= stagnation_bars:
                exit_premium, exit_ts, reason = premium_now, ts, "stagnation_exit"
                break
            if ts.time() >= eod_time:
                exit_premium, exit_ts, reason = premium_now, ts, "eod"
                break

        if exit_premium is None:
            last_row = future.iloc[-1] if not future.empty else None
            exit_ts = last_row["datetime"] if last_row is not None else sig["entry_ts"]
            exit_premium = price_at(df, exit_ts) or entry_premium
            reason = "eod_final"

        pnl_rs = (exit_premium - entry_premium) * LOT_SIZE
        trades.append(dict(
            direction=sig["direction"], entry_ts=sig["entry_ts"], spot_entry=sig["spot_entry"],
            strike=sig["strike"], option_type=sig["option_type"],
            entry_premium=entry_premium, sl_premium=sl_premium, tsl_locked_pct=high_lock_pct,
            exit_premium=exit_premium, exit_ts=exit_ts, reason=reason, pnl_rs=pnl_rs,
        ))
    return trades


# ══════════════════════════════════════════════════════════════════════════
# Report
# ══════════════════════════════════════════════════════════════════════════

def max_drawdown(trades_sorted):
    equity, peak, mdd = 0.0, 0.0, 0.0
    for t in trades_sorted:
        equity += t["pnl_rs"]
        peak = max(peak, equity)
        mdd = min(mdd, equity - peak)
    return mdd


def main():
    spot = load_spot()
    stagnation_bars = max(1, 40 // LTF_MINS)
    print(f"Phase 1 -- discovering entry signals in {BACKTEST_START}..{BACKTEST_END} "
          f"(HTF={HTF_MINS}m/LTF={LTF_MINS}m, 1-strike ITM, {ITM_OFFSET_PTS}pt offset)...")
    signals = discover_signals(spot)
    print(f"  {len(signals)} signals found.\n")

    print("Phase 2 -- fetching/loading real option premium...")
    premium_cache = ensure_premium_data(signals)
    print()

    print(f"Phase 3 -- resolving option-native exits (SL={PREMIUM_SL_PCT*100:.0f}% premium | "
          f"step-locked TSL: trigger={TSL_BASE_PCT*100:.0f}% lock={TSL_BASE_LOCK_PCT*100:.1f}% "
          f"step={TSL_STEP_PCT*100:.0f}%/{TSL_STEP_LOCK_PCT*100:.1f}% | "
          f"{stagnation_bars}x{LTF_MINS}m-candle (~40min) stagnation)...")
    trades = resolve_trades(signals, premium_cache, spot, stagnation_bars=stagnation_bars)
    trades.sort(key=lambda t: t["exit_ts"])
    print()

    n = len(trades)
    wins = [t for t in trades if t["pnl_rs"] > 0]
    losses = [t for t in trades if t["pnl_rs"] <= 0]
    gross_win = sum(t["pnl_rs"] for t in wins)
    gross_loss = abs(sum(t["pnl_rs"] for t in losses))
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")
    net = sum(t["pnl_rs"] for t in trades)
    win_pct = 100 * len(wins) / n if n else 0
    mdd = max_drawdown(trades)

    print("=" * 116)
    print(f"FVG OPTION-NATIVE BACKTEST -- NIFTY, {BACKTEST_START} .. {BACKTEST_END} (last 7 trading sessions)")
    print(f"HTF={HTF_MINS}m/LTF={LTF_MINS}m | 1-strike ITM ({ITM_OFFSET_PTS}pt) | SL={PREMIUM_SL_PCT*100:.0f}% premium "
          f"(Rs/lot-capped) | step-locked TSL {TSL_BASE_PCT*100:.0f}%->{TSL_BASE_LOCK_PCT*100:.1f}%, "
          f"then +{TSL_STEP_PCT*100:.0f}%->+{TSL_STEP_LOCK_PCT*100:.1f}% | {stagnation_bars}-candle stagnation exit")
    print("=" * 116)
    print(f"\n{'Entry TS':<22}{'Dir':<7}{'Option':<9}{'Spot Entry':>11}{'Entry Prem':>11}"
          f"{'SL Prem':>9}{'TSL Lock':>9}{'Exit Prem':>10}{'Exit TS':<22}{'Reason':<16}{'PnL(Rs)':>10}")
    for t in trades:
        print(f"{str(t['entry_ts']):<22}{t['direction']:<7}{t['option_type']+str(t['strike']):<9}"
              f"{t['spot_entry']:>11.2f}{t['entry_premium']:>11.2f}{t['sl_premium']:>9.2f}"
              f"{t['tsl_locked_pct']*100:>8.1f}%{t['exit_premium']:>10.2f}{str(t['exit_ts']):<22}{t['reason']:<16}"
              f"{t['pnl_rs']:>+10,.0f}")

    print(f"\n{'='*116}\nSUMMARY\n{'='*116}")
    print(f"Total Trades      : {n}")
    print(f"Win Rate          : {win_pct:.1f}%  ({len(wins)}W / {len(losses)}L)")
    print(f"Gross Profit      : Rs{gross_win:+,.0f}")
    print(f"Gross Loss        : -Rs{gross_loss:,.0f}")
    print(f"Profit Factor     : {pf:.2f}")
    print(f"Net PnL           : Rs{net:+,.0f}")
    print(f"Max Drawdown      : Rs{mdd:,.0f}")

    print(f"\n{'='*116}\nGO / NO-GO RECOMMENDATION\n{'='*116}")
    if pf > 1.3 and win_pct > 45:
        print(f"GO -- PF={pf:.2f} > 1.3 and Win%={win_pct:.1f}% > 45%. "
              f"Proceed to the 2-week paper trading phase on live market feed.")
    elif pf < 1.0:
        print(f"NO-GO -- PF={pf:.2f} < 1.0. Tweak entry filters (HTF trend bias, stricter FVG gap "
              f"threshold) before paper trading.")
    else:
        print(f"HOLD -- PF={pf:.2f} and Win%={win_pct:.1f}% are between the GO and NO-GO thresholds. "
              f"Neither condition is met; treat as inconclusive on this sample size (n={n}) rather than "
              f"a clean signal either way.")
    return trades


if __name__ == "__main__":
    main()
