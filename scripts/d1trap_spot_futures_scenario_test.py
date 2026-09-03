"""
scripts/d1trap_spot_futures_scenario_test.py -- compare 4 ways of deciding
WHEN to fire a D1Trap entry, keeping the option side execution-only:

  S1: signal computed on SPOT alone -> buy option
  S2: signal must independently confirm on BOTH spot AND futures within a
      15-min window -> buy option
  S3: signal computed on FUTURES alone -> buy option
  S4: (not rebuilt here -- already measured in d1trap_spot_bias_test.py /
      d1trap_breakeven_bos_test.py "baseline") = today's live design: zones
      detected on the OPTION'S OWN premium, gated by a spot HTF bias filter.

Reuses bb._detect_bear_zones (bear/PE-side geometry) UNCHANGED. For the
CE/bullish side there is no separate "bull zone" detector in this codebase
-- rather than inventing new zone geometry, a bull zone is mathematically a
bear zone on the PRICE-NEGATED series (mirror bars: high<->-low, low<->-high,
open/close negated), then translate the found zone/trigger levels back by
negating again. This is the same trick, not new logic.

Entry timing (HTF zone -> 15m invalidation -> T1 fast-breach / T2 swing
retest via the flip path) reuses scripts/d1trap_month_rolling_backtest.py's
day-loop functions unchanged (on_new_15m_close, check_fast_t1,
process_zones_tick, process_flip_entry_t2) -- only the BARS fed in change
(spot/futures instead of option premium).

Once a spot/futures signal fires (ts, side), execution + SL/TSL/P&L are
still fully option-native: pick the 1-ITM strike for that day, pull the
REAL cached option premium at and after that timestamp, and run the same
tranche TSL/hard-cap exit mechanic used everywhere else in this session's
D1Trap validation scripts (mrb.open_leg / mrb.check_exit / mrb._close).
"""
import sys
sys.path.insert(0, ".")
from datetime import timedelta, date
import pandas as pd
import strategies.d1_trap_option.bear_only_book as bb
import scripts.d1trap_month_rolling_backtest as mrb

STRIKE_DIR = "data/d1trap_fractal_cache/strike_ladder"
DAY_MIN, DAY_MAX = mrb.DAY_MIN, mrb.DAY_MAX

CONFIGS = {
    "NIFTY":  dict(fname_prefix="niftyladder", offset=150, round_step=100, lot=65, htf=60,
                   spot_path="data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet",
                   fut_path="data/d1trap_fractal_cache/nifty_fut_1m_month_backtest.parquet"),
    "SENSEX": dict(fname_prefix="sensexladder", offset=300, round_step=100, lot=20, htf=15,
                   spot_path="data/d1trap_fractal_cache/sensex_1m_fullmonth_spot.parquet",
                   fut_path="data/d1trap_fractal_cache/sensex_fut_1m_month_backtest.parquet"),
}


def _mirror(df):
    """Negate-and-swap H/L so a bear-zone detector run on this finds bull
    structure on the original series (translate back by negating again)."""
    m = df.copy()
    m["open"], m["close"] = -df["open"], -df["close"]
    m["high"], m["low"] = -df["low"], -df["high"]
    return m


def _unmirror_price(p):
    return -p


def collect_signals(price_df, htf_minutes, side):
    """Run the exact live zone/T1/T2/flip cascade on `price_df` (a plain
    spot/futures OHLC series), returning raw (ts, side, trigger_price)
    signals -- no option execution yet. `side` is 'PE' (bear, native
    geometry) or 'CE' (bull, mirrored geometry)."""
    df = price_df if side == "PE" else _mirror(price_df)
    days = sorted(d for d in df["datetime"].dt.date.unique() if DAY_MIN <= d <= DAY_MAX)
    state = mrb.SideState()
    state.side = side
    signals = []

    # Monkey-patch _close to record signals instead of running SL/TSL (we
    # only want ENTRY timing here; open_leg is called by check_fast_t1 /
    # process_zones_tick / process_flip_entry_t2, so intercept there instead).
    orig_open_leg = mrb.open_leg
    def spy_open_leg(st, lot_size, tranche, entry_price, sl, zone_lock_ts, audit=None, entry_ts=None):
        px = entry_price if side == "PE" else _unmirror_price(entry_price)
        signals.append((entry_ts, side, px))
        orig_open_leg(st, lot_size, tranche, entry_price, sl, zone_lock_ts, audit=audit, entry_ts=entry_ts)
        st.positions.clear()  # one signal per trigger; don't track a "position" on the index itself
    mrb.open_leg = spy_open_leg
    try:
        for day in days:
            day_df = df[df["datetime"].dt.date == day]
            if day_df.empty:
                continue
            start = day - timedelta(days=mrb.HIST_WARMUP_DAYS)
            hist = df[(df["datetime"].dt.date >= start) & (df["datetime"].dt.date < day)]
            if len(hist) < 30:
                continue
            m_htf_hist, m15_hist = bb._resample(hist, htf_minutes), bb._resample(hist, 15)
            state.zones = mrb._prevalidate(bb._detect_bear_zones(bb._to_bars(m_htf_hist)), m15_hist)
            state.flip_candidates, state.positions = [], []

            today_df = day_df.reset_index(drop=True)
            m5, m15, mhtf = bb._resample(df[df["datetime"].dt.date <= day], 5), \
                             bb._resample(df[df["datetime"].dt.date <= day], 15), \
                             bb._resample(df[df["datetime"].dt.date <= day], htf_minutes)
            m15_today = m15[m15["timestamp"].dt.date == day].reset_index(drop=True)
            mhtf_today = mhtf[mhtf["timestamp"].dt.date == day].reset_index(drop=True)

            i15 = ihtf = 0
            dummy_other = mrb.SideState()
            for i in range(len(today_df)):
                bar = today_df.iloc[i]; ts = bar["datetime"]
                while ihtf < len(mhtf_today) and mhtf_today.iloc[ihtf]["timestamp"] + timedelta(minutes=htf_minutes) <= ts:
                    window = df[(df["datetime"].dt.date >= start) & (df["datetime"] < mhtf_today.iloc[ihtf]["timestamp"] + timedelta(minutes=htf_minutes))]
                    if len(window) >= 30:
                        m_htf2, m15_2 = bb._resample(window, htf_minutes), bb._resample(window, 15)
                        existing_refs = {z["ref_ts"] for z in state.zones}
                        new_zones = [z for z in bb._detect_bear_zones(bb._to_bars(m_htf2)) if z["ref_ts"] not in existing_refs]
                        state.zones.extend(mrb._prevalidate(new_zones, m15_2))
                    ihtf += 1
                while i15 < len(m15_today) and m15_today.iloc[i15]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = m15_today.iloc[i15]
                    state.prev15_high, state.prev15_low = m15row["high"], m15row["low"]
                    mrb.on_new_15m_close(state, m15row)
                    mrb.process_flip_entry_t2(state, 1, m15row, m15, m5, dummy_other)
                    i15 += 1
                mrb.check_fast_t1(state, 1, bar["high"], ts, dummy_other)
                mrb.process_zones_tick(state, 1, ts, bar["low"], bar["high"], m15, m5, dummy_other)
    finally:
        mrb.open_leg = orig_open_leg
    return signals


def execute_signals(signals, cfg, spot_df):
    """Translate raw (ts, side, index_price) signals into real option trades:
    1-ITM strike for that day, real cached premium at entry, then the
    standard tranche TSL/hard-cap exit walked forward on that strike's own
    real premium path."""
    trades = []
    strike_cache = {}
    for ts, side, _px in signals:
        day = ts.date()
        day_spot = spot_df[spot_df["datetime"].dt.date == day]
        if day_spot.empty:
            continue
        o = day_spot.iloc[0]["open"]
        atm = round(o / cfg["round_step"]) * cfg["round_step"]
        strike = int(atm - cfg["offset"]) if side == "CE" else int(atm + cfg["offset"])
        key = (strike, side)
        if key not in strike_cache:
            strike_cache[key] = mrb.load_bars(cfg["fname_prefix"], strike, side) if False else None
        # load_bars uses mrb.MONTH_DIR; point it at the real strike ladder cache
        path = f"{STRIKE_DIR}/{cfg['fname_prefix']}_{strike}_{side}.parquet"
        try:
            odf = pd.read_parquet(path)
        except FileNotFoundError:
            continue
        odf["datetime"] = pd.to_datetime(odf["datetime"])
        odf = odf.sort_values("datetime").reset_index(drop=True)
        window = odf[odf["datetime"] >= ts]
        if window.empty:
            continue
        entry_row = window.iloc[0]
        entry_price = entry_row["open"] if entry_row["open"] > 0 else entry_row["close"]
        if entry_price <= 0:
            continue
        state = mrb.SideState()
        state.side, state.strike = side, strike
        # Recent premium low (last 15 bars before entry) as the SL reference,
        # same shape as the live code's zone-low reference -- open_leg applies
        # the standard buffer + hard-cap floor on top of this.
        pre = odf[odf["datetime"] < ts].tail(15)
        ref_low = pre["low"].min() if not pre.empty else entry_price * 0.9
        mrb.open_leg(state, cfg["lot"], "single", entry_price, ref_low, ts, entry_ts=ts)
        for _, bar in window.iterrows():
            mrb.check_exit(state, cfg["lot"], bar["close"], bar["datetime"])
            if not state.positions:
                break
        if state.positions:  # ran off the end of data -- force EOD close at last price
            mrb.check_exit(state, cfg["lot"], window.iloc[-1]["close"], window.iloc[-1]["datetime"], force=True)
        trades.extend(state.trades)
    return sorted(trades, key=lambda t: t["exit_ts"])


def confirm_both(spot_sig, fut_sig, tol_min=15):
    fut_by_side = {"CE": [t for t, s, p in fut_sig if s == "CE"], "PE": [t for t, s, p in fut_sig if s == "PE"]}
    out = []
    for ts, side, px in spot_sig:
        pool = fut_by_side[side]
        if any(abs((ts - ft).total_seconds()) <= tol_min * 60 for ft in pool):
            out.append((ts, side, px))
    return out


if __name__ == "__main__":
    for underlying, cfg in CONFIGS.items():
        print(f"\n{'#'*100}\n{underlying}\n{'#'*100}")
        spot_df = pd.read_parquet(cfg["spot_path"]); spot_df["datetime"] = pd.to_datetime(spot_df["datetime"])
        fut_df = pd.read_parquet(cfg["fut_path"]); fut_df["datetime"] = pd.to_datetime(fut_df["datetime"])

        spot_ce = collect_signals(spot_df, cfg["htf"], "CE")
        spot_pe = collect_signals(spot_df, cfg["htf"], "PE")
        spot_sig = sorted(spot_ce + spot_pe, key=lambda x: x[0])
        print(f"  spot signals: {len(spot_sig)} ({len(spot_ce)} CE / {len(spot_pe)} PE)")

        fut_ce = collect_signals(fut_df, cfg["htf"], "CE")
        fut_pe = collect_signals(fut_df, cfg["htf"], "PE")
        fut_sig = sorted(fut_ce + fut_pe, key=lambda x: x[0])
        print(f"  futures signals: {len(fut_sig)} ({len(fut_ce)} CE / {len(fut_pe)} PE)")

        both_sig = confirm_both(spot_sig, fut_sig)
        print(f"  spot+futures agree (within 15min): {len(both_sig)}")

        t_spot = execute_signals(spot_sig, cfg, spot_df)
        mrb.summarize(f"{underlying} S1: spot-only signal", t_spot)

        t_both = execute_signals(both_sig, cfg, spot_df)
        mrb.summarize(f"{underlying} S2: spot+futures confirm", t_both)

        t_fut = execute_signals(fut_sig, cfg, spot_df)
        mrb.summarize(f"{underlying} S3: futures-only signal", t_fut)
