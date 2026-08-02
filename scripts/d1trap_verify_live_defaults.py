"""Sanity check: run the month backtest using the ACTUAL live bear_only_book.py
functions (_detect_bear_zones, its new defaults) rather than the standalone
copies used in scripts/d1trap_zone_definition_sweep.py, to confirm the wired
live code reproduces the same backtested numbers (NIFTY 60m/ref.close PF~1.93,
SENSEX 15m/ref.close PF~1.71) before pushing to live/paper tomorrow.
"""
import sys
sys.path.insert(0, ".")
from datetime import timedelta

import pandas as pd
import strategies.d1_trap_option.bear_only_book as bb
import scripts.d1trap_month_rolling_backtest as mrb

LADDER_DIR = "data/d1trap_fractal_cache/strike_ladder"


def run_month_live(underlying, fname_prefix, spot_path, offset, round_step, lot_size, htf_minutes):
    """Same day-loop as d1trap_zone_definition_sweep.run_month_htf, but calling
    the LIVE bb._detect_bear_zones directly (no local re-implementation)."""
    spot = pd.read_parquet(spot_path)
    spot["datetime"] = pd.to_datetime(spot["datetime"])
    days = sorted(d for d in spot["datetime"].dt.date.unique() if mrb.DAY_MIN <= d <= mrb.DAY_MAX)

    ce_state, pe_state = mrb.SideState(), mrb.SideState()
    ce_cache, pe_cache = {}, {}

    def get_bars(strike, side):
        cache = ce_cache if side == "CE" else pe_cache
        if strike not in cache:
            cache[strike] = mrb.load_bars(fname_prefix, strike, side)
        return cache[strike]

    def warmup_live(state, df1m, strike, side, day):
        state.strike, state.side = strike, side
        state.zones, state.flip_candidates, state.positions = [], [], []
        start = day - timedelta(days=mrb.HIST_WARMUP_DAYS)
        hist = df1m[(df1m["datetime"].dt.date >= start) & (df1m["datetime"].dt.date < day)]
        m_htf_hist, m15_hist = bb._resample(hist, htf_minutes), bb._resample(hist, 15)
        state.zones = mrb._prevalidate(bb._detect_bear_zones(bb._to_bars(m_htf_hist)), m15_hist)

    def refresh_live(state, ts, df1m, day):
        start = day - timedelta(days=mrb.HIST_WARMUP_DAYS)
        window = df1m[(df1m["datetime"].dt.date >= start) & (df1m["datetime"] < ts)]
        if len(window) < 30:
            return
        m_htf, m15 = bb._resample(window, htf_minutes), bb._resample(window, 15)
        existing_refs = {z["ref_ts"] for z in state.zones}
        new_zones = [z for z in bb._detect_bear_zones(bb._to_bars(m_htf)) if z["ref_ts"] not in existing_refs]
        state.zones.extend(mrb._prevalidate(new_zones, m15))

    for day in days:
        day_spot = spot[spot["datetime"].dt.date == day]
        if day_spot.empty:
            continue
        o = day_spot.iloc[0]["open"]
        atm = round(o / round_step) * round_step
        ce_strike, pe_strike = int(atm - offset), int(atm + offset)

        ce_df = get_bars(ce_strike, "CE")
        pe_df = get_bars(pe_strike, "PE")
        if ce_df.empty or pe_df.empty:
            continue

        if ce_state.strike != ce_strike:
            if ce_state.positions:
                last_row = ce_cache[ce_state.strike]
                last_today = last_row[last_row["datetime"].dt.date == day]
                px = last_today.iloc[0]["open"] if not last_today.empty else ce_state.positions[0]["entry_price"]
                mrb.check_exit(ce_state, lot_size, px, day_spot.iloc[0]["datetime"], force=True)
            warmup_live(ce_state, ce_df, ce_strike, "CE", day)
        if pe_state.strike != pe_strike:
            if pe_state.positions:
                last_row = pe_cache[pe_state.strike]
                last_today = last_row[last_row["datetime"].dt.date == day]
                px = last_today.iloc[0]["open"] if not last_today.empty else pe_state.positions[0]["entry_price"]
                mrb.check_exit(pe_state, lot_size, px, day_spot.iloc[0]["datetime"], force=True)
            warmup_live(pe_state, pe_df, pe_strike, "PE", day)

        ce_today = ce_df[ce_df["datetime"].dt.date == day].reset_index(drop=True)
        pe_today = pe_df[pe_df["datetime"].dt.date == day].reset_index(drop=True)
        ce_m5, ce_m15, ce_mhtf = bb._resample(ce_df, 5), bb._resample(ce_df, 15), bb._resample(ce_df, htf_minutes)
        pe_m5, pe_m15, pe_mhtf = bb._resample(pe_df, 5), bb._resample(pe_df, 15), bb._resample(pe_df, htf_minutes)
        ce_m15_today = ce_m15[ce_m15["timestamp"].dt.date == day].reset_index(drop=True)
        pe_m15_today = pe_m15[pe_m15["timestamp"].dt.date == day].reset_index(drop=True)
        ce_mhtf_today = ce_mhtf[ce_mhtf["timestamp"].dt.date == day].reset_index(drop=True)
        pe_mhtf_today = pe_mhtf[pe_mhtf["timestamp"].dt.date == day].reset_index(drop=True)

        if ce_today.empty and pe_today.empty:
            continue

        max_len = max(len(ce_today), len(pe_today))
        ce15 = pe15 = cehtf = pehtf = 0
        for i in range(max_len):
            if i < len(ce_today):
                bar = ce_today.iloc[i]; ts = bar["datetime"]
                while cehtf < len(ce_mhtf_today) and ce_mhtf_today.iloc[cehtf]["timestamp"] + timedelta(minutes=htf_minutes) <= ts:
                    refresh_live(ce_state, ce_mhtf_today.iloc[cehtf]["timestamp"] + timedelta(minutes=htf_minutes), ce_df, day)
                    cehtf += 1
                while ce15 < len(ce_m15_today) and ce_m15_today.iloc[ce15]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = ce_m15_today.iloc[ce15]
                    ce_state.prev15_high, ce_state.prev15_low = m15row["high"], m15row["low"]
                    mrb.on_new_15m_close(ce_state, m15row)
                    mrb.process_flip_entry_t2(ce_state, lot_size, m15row, ce_m15, ce_m5, pe_state)
                    ce15 += 1
                mrb.check_exit(ce_state, lot_size, bar["close"], ts)
                mrb.check_fast_t1(ce_state, lot_size, bar["high"], ts, pe_state)
                mrb.process_zones_tick(ce_state, lot_size, ts, bar["low"], bar["high"], ce_m15, ce_m5, pe_state)
            if i < len(pe_today):
                bar = pe_today.iloc[i]; ts = bar["datetime"]
                while pehtf < len(pe_mhtf_today) and pe_mhtf_today.iloc[pehtf]["timestamp"] + timedelta(minutes=htf_minutes) <= ts:
                    refresh_live(pe_state, pe_mhtf_today.iloc[pehtf]["timestamp"] + timedelta(minutes=htf_minutes), pe_df, day)
                    pehtf += 1
                while pe15 < len(pe_m15_today) and pe_m15_today.iloc[pe15]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = pe_m15_today.iloc[pe15]
                    pe_state.prev15_high, pe_state.prev15_low = m15row["high"], m15row["low"]
                    mrb.on_new_15m_close(pe_state, m15row)
                    mrb.process_flip_entry_t2(pe_state, lot_size, m15row, pe_m15, pe_m5, ce_state)
                    pe15 += 1
                mrb.check_exit(pe_state, lot_size, bar["close"], ts)
                mrb.check_fast_t1(pe_state, lot_size, bar["high"], ts, ce_state)
                mrb.process_zones_tick(pe_state, lot_size, ts, bar["low"], bar["high"], pe_m15, pe_m5, ce_state)

    return sorted(ce_state.trades + pe_state.trades, key=lambda t: t["exit_ts"])


if __name__ == "__main__":
    mrb.MONTH_DIR = LADDER_DIR
    trades_nifty = run_month_live("NIFTY", "niftyladder", "data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet",
                                   150, 100, 65, 60)
    mrb.summarize("NIFTY live-code (150pt/60m, new default)", trades_nifty)
    trades_sensex = run_month_live("SENSEX", "sensexladder", "data/d1trap_fractal_cache/sensex_1m_fullmonth_spot.parquet",
                                    300, 100, 20, 15)
    mrb.summarize("SENSEX live-code (300pt/15m, new default)", trades_sensex)
