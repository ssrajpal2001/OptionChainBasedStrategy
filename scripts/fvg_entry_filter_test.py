"""
scripts/fvg_entry_filter_test.py — structural entry-filter grid test on the
clean intraday-only 7-day dataset (2026-07-23..07-31, HTF=10m/LTF=3m), per
explicit request. Tests whether tightening ENTRY quality (not exits) fixes
the "10 of 13 trades stall into stagnation" symptom from the validated
baseline (PF 1.43, Net +Rs1,979).

Filters (each independently, then combined):
  1. Time-of-day ("no-chop zone"): only allow entries 09:20-11:15 or
     13:45-15:00 IST -- discard the 11:15-13:45 midday chop window.
  2. Displacement/impulse: candle2's absolute body must exceed
     MIN_BODY_PTS NIFTY points (not just the existing 50%-of-range
     relative test) -- rejects micro-gaps from low-momentum candles.
  3. ADX(20) on 3m spot bars > 20 at the entry candle -- confirms an
     active trend/expansion phase. NOTE: this contradicts the project's
     explicit "no indicators" design constraint for this strategy
     (CLAUDE.md, direct user spec earlier this session) -- included here
     ONLY as a one-off comparison test per this request, not adopted into
     the live engine's indicator-free design without further sign-off.

All variants reuse the validated TSL (trigger=15%/lock=8%/step=10%/
step_lock=5%, 40min/13-candle stagnation) and the same intraday-only FVG
pool fix already in scripts/fvg_backtest.py -- ONLY the entry gate changes.
"""
import sys
sys.path.insert(0, ".")
from datetime import time as dtime

import numpy as np
import pandas as pd

import scripts.fvg_backtest as fb
from matrix_engine.indicators import adx as _adx_fn
from strategies.fvg.detector import (
    detect_fvg,
    find_swing_points,
    tag_high_liquidity,
    update_fvg_state,
)

MORNING = (dtime(9, 20), dtime(11, 15))
AFTERNOON = (dtime(13, 45), dtime(15, 0))
MIN_BODY_PTS = 18.0     # midpoint of the requested 15-20pt range
ADX_THRESHOLD = 20.0
ADX_MIN_BARS = 2 * 20 + 2   # matrix_engine.indicators.ADX_PERIOD=20 -> needs 42 bars


def in_no_chop_zone(ts) -> bool:
    t = ts.time()
    return (MORNING[0] <= t <= MORNING[1]) or (AFTERNOON[0] <= t <= AFTERNOON[1])


def build_adx_lookup(spot: pd.DataFrame, ltf_mins: int) -> dict:
    """Precompute ADX(20) at every closed ltf_mins spot bar across the whole
    dataset (rolling, causal -- only bars up to and including each point)."""
    m = fb._df_resample(spot, ltf_mins) if hasattr(fb, "_df_resample") else None
    # fb doesn't export _df_resample by that name at module scope; use the
    # same resample helper fb.py itself imports.
    from strategies.d1_trap_option.bear_only_book import _resample as _df_resample
    bars_df = _df_resample(spot, ltf_mins)
    highs, lows, closes = bars_df["high"].to_numpy(), bars_df["low"].to_numpy(), bars_df["close"].to_numpy()
    lookup = {}
    for i in range(len(bars_df)):
        if i + 1 < ADX_MIN_BARS:
            val = 0.0
        else:
            val, _, _ = _adx_fn(highs[:i + 1], lows[:i + 1], closes[:i + 1])
        lookup[bars_df.iloc[i]["timestamp"]] = val
    return lookup


def discover_signals_filtered(spot, htf_mins, ltf_mins, itm_offset_pts,
                               use_tod=False, use_displacement=False, use_adx=False,
                               adx_lookup=None):
    days = sorted(spot["datetime"].dt.date.unique())
    htf_bars, ltf_bars, htf_swings = [], [], []
    fvgs, known_fvg_ts = [], set()
    pdh = pdl = None
    position = None
    signals = []

    def close_gate(exit_price, exit_ts):
        nonlocal position
        position = None

    min_body_pts = MIN_BODY_PTS if use_displacement else 0.0

    for day in days:
        fvgs.clear()
        known_fvg_ts.clear()

        start = day - fb.timedelta(days=fb.HIST_WARMUP_DAYS)
        hist = spot[(spot["datetime"].dt.date >= start) & (spot["datetime"].dt.date < day)]
        if len(hist) >= 30:
            htf_bars = fb._to_bars(fb._df_resample(hist, htf_mins))
            ltf_bars = fb._to_bars(fb._df_resample(hist, ltf_mins))
            htf_swings = find_swing_points(htf_bars)
            pdh, pdl = fb.roll_pdh_pdl(htf_bars, pd.Timestamp(day, tz=htf_bars[0].timestamp.tz) if htf_bars else None)
            found = detect_fvg([b for b in ltf_bars if b.timestamp.date() == day], min_body_pts=min_body_pts)
            for fv in found:
                if fv["candle3_ts"] in known_fvg_ts:
                    continue
                tag_high_liquidity(fv, htf_bars, htf_swings, pdh=pdh, pdl=pdl)
                fvgs.append(fv)
                known_fvg_ts.add(fv["candle3_ts"])

        day_bars = spot[spot["datetime"].dt.date == day].reset_index(drop=True)
        if day_bars.empty:
            continue

        current_htf_open = current_ltf_open = None
        current_htf_5m, current_ltf_1m = [], []
        day_done = False
        in_window = fb.BACKTEST_START <= day <= fb.BACKTEST_END

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
            if not day_done and ts.time() >= fb.EOD_TIME and position is not None:
                close_gate(spot_px, ts)
                day_done = True

            htf_open = fb._bucket(ts, htf_mins)
            if current_htf_open is None:
                current_htf_open = htf_open
            elif htf_open != current_htf_open:
                if current_htf_5m:
                    htf_bars.append(fb._close_ltf_bar(current_htf_open, current_htf_5m))
                    htf_swings = find_swing_points(htf_bars)
                current_htf_open, current_htf_5m = htf_open, []
            current_htf_5m.append(row)

            ltf_open = fb._bucket(ts, ltf_mins)
            if current_ltf_open is None:
                current_ltf_open = ltf_open
            elif ltf_open != current_ltf_open:
                if current_ltf_1m:
                    bar = fb._close_ltf_bar(current_ltf_open, current_ltf_1m)
                    ltf_bars.append(bar)
                    today_bars = [b for b in ltf_bars if b.timestamp.date() == day]
                    found = detect_fvg(today_bars, min_body_pts=min_body_pts)
                    for fv in found:
                        if fv["candle3_ts"] in known_fvg_ts:
                            continue
                        tag_high_liquidity(fv, htf_bars, htf_swings, pdh=pdh, pdl=pdl)
                        fvgs.append(fv)
                        known_fvg_ts.add(fv["candle3_ts"])
                    for fv in fvgs:
                        update_fvg_state(fv, bar)

                    if position is None and bar.timestamp.time() < fb.ENTRY_CUTOFF:
                        if not use_tod or in_no_chop_zone(bar.timestamp):
                            adx_ok = True
                            if use_adx:
                                adx_val = (adx_lookup or {}).get(fb._bucket(bar.timestamp, ltf_mins), 0.0)
                                adx_ok = adx_val > ADX_THRESHOLD
                            if adx_ok:
                                for fv in fvgs:
                                    if not fv["high_liquidity"] or fv["state"] != "MITIGATED":
                                        continue
                                    direction = "LONG" if fv["direction"] == "BULLISH" else "SHORT"
                                    sl_price = fv["candle1_low"] if direction == "LONG" else fv["candle1_high"]
                                    entry_price = bar.close
                                    sl_distance = abs(entry_price - sl_price)
                                    if sl_distance <= 0 or not fb.gate_risk_within_cap(sl_distance):
                                        fv["state"] = "INVALIDATED"
                                        continue
                                    tp_price = (entry_price + sl_distance * fb.GATE_MIN_RR if direction == "LONG"
                                                else entry_price - sl_distance * fb.GATE_MIN_RR)
                                    atm = round(entry_price / fb.ATM_ROUND_STEP) * fb.ATM_ROUND_STEP
                                    if direction == "LONG":
                                        strike, opt_type = int(atm - itm_offset_pts), "CE"
                                    else:
                                        strike, opt_type = int(atm + itm_offset_pts), "PE"
                                    position = dict(direction=direction, entry=entry_price, sl=sl_price,
                                                     tp=tp_price, entry_ts=bar.timestamp, strike=strike,
                                                     option_type=opt_type)
                                    if in_window:
                                        signals.append(dict(direction=direction, entry_ts=bar.timestamp,
                                                             spot_entry=entry_price, strike=strike,
                                                             option_type=opt_type, day=day))
                                    fv["state"] = "INVALIDATED"
                                    break
                    # even when the entry gate is closed, still consume MITIGATED
                    # FVGs so they don't re-fire once the gate reopens on a stale setup
                    if position is None and (use_tod and not in_no_chop_zone(bar.timestamp)):
                        for fv in fvgs:
                            if fv["high_liquidity"] and fv["state"] == "MITIGATED":
                                fv["state"] = "INVALIDATED"
                current_ltf_open, current_ltf_1m = ltf_open, []
            current_ltf_1m.append(row)

    return signals


TSL_PARAMS = dict(tsl_base_pct=0.15, tsl_base_lock_pct=0.08, tsl_step_pct=0.10, tsl_step_lock_pct=0.05)
STAGNATION_BARS = 13   # 40min at 3m LTF -- validated baseline, unchanged (entry-only test)


def run_variant(spot, name, use_tod, use_displacement, use_adx, adx_lookup):
    signals = discover_signals_filtered(spot, htf_mins=10, ltf_mins=3, itm_offset_pts=fb.ITM_OFFSET_PTS,
                                         use_tod=use_tod, use_displacement=use_displacement, use_adx=use_adx,
                                         adx_lookup=adx_lookup)
    premium_cache = fb.ensure_premium_data(signals)
    trades = fb.resolve_trades(signals, premium_cache, spot, ltf_mins=3, stagnation_bars=STAGNATION_BARS,
                                **TSL_PARAMS)
    trades.sort(key=lambda t: t["exit_ts"])
    n = len(trades)
    wins = [t for t in trades if t["pnl_rs"] > 0]
    losses = [t for t in trades if t["pnl_rs"] <= 0]
    gross_win = sum(t["pnl_rs"] for t in wins)
    gross_loss = abs(sum(t["pnl_rs"] for t in losses))
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")
    net = sum(t["pnl_rs"] for t in trades)
    win_pct = 100 * len(wins) / n if n else 0
    mdd = fb.max_drawdown(trades)
    tsl_n = sum(1 for t in trades if t["reason"] == "tsl_hit")
    stag_n = sum(1 for t in trades if t["reason"] == "stagnation_exit")
    sl_n = sum(1 for t in trades if t["reason"] == "sl_hit")
    print(f"{name:<24} n={n:>3}  win%={win_pct:>5.1f}  PF={pf:>5.2f}  NET=Rs{net:>+7,.0f}  "
          f"maxDD=Rs{mdd:>7,.0f}  TSL={tsl_n} Stag={stag_n} SL={sl_n}")
    return dict(name=name, n=n, win_pct=win_pct, pf=pf, net=net, mdd=mdd, tsl_n=tsl_n, stag_n=stag_n, sl_n=sl_n)


def main():
    spot = fb.load_spot()
    print("Precomputing ADX(20) on 3m spot bars...")
    adx_lookup = build_adx_lookup(spot, 3)
    print("done.\n")

    results = []
    results.append(run_variant(spot, "Baseline (no filters)", False, False, False, adx_lookup))
    results.append(run_variant(spot, "1. Time-of-Day only", True, False, False, adx_lookup))
    results.append(run_variant(spot, "2. Displacement only", False, True, False, adx_lookup))
    results.append(run_variant(spot, "3. ADX>20 only", False, False, True, adx_lookup))
    results.append(run_variant(spot, "All 3 combined", True, True, True, adx_lookup))

    print(f"\n{'='*100}\nENTRY FILTER GRID -- NIFTY, {fb.BACKTEST_START}..{fb.BACKTEST_END}, HTF=10m/LTF=3m, "
          f"TSL trigger=15%/lock=8%/step=10%/5%, 40min stagnation\n{'='*100}")
    print(f"{'Filter':<24}{'Trades':>8}{'Win%':>8}{'PF':>7}{'NetPnL':>11}{'MaxDD':>10}{'TSL':>6}{'Stag':>6}{'SL':>5}")
    for r in results:
        print(f"{r['name']:<24}{r['n']:>8}{r['win_pct']:>7.1f}%{r['pf']:>7.2f}Rs{r['net']:>+8,.0f}"
              f"Rs{r['mdd']:>8,.0f}{r['tsl_n']:>6}{r['stag_n']:>6}{r['sl_n']:>5}")


if __name__ == "__main__":
    main()
