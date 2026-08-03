"""
scripts/d1trap_htf_bigger_tf_sweep.py -- live finding (2026-08-03, gap-up day):
SENSEX's 15m zones carried over from 07-31 (days-old, decayed premium) and
were trivially "contacted" the instant today's gapped-up open sat below
their stale ranges. User's proposed fix: bigger HTF zones (which
legitimately persist across days) for SENSEX, matching NIFTY's already-
working 60m approach; only fall back to a SAME-DAY-ONLY 15m zone pool if
the bigger TF finds nothing that day (same principle as FVG's proven
same-session-only pool fix).

This script tests each candidate TIMEFRAME STANDALONE first (to see which
wins outright before committing to the bigger fallback-cascade
architecture): 75m, 120m, 170m, 240m, 1440m(=D1), all WITH prev-day
carryover (matching current 60m behavior) -- vs a corrected 15m
SAME-DAY-ONLY variant, for both SENSEX and NIFTY, against real month-
window premium data already cached.
"""
import sys
sys.path.insert(0, ".")
from datetime import timedelta, date
import pandas as pd
import strategies.d1_trap_option.bear_only_book as bb
import scripts.d1trap_month_rolling_backtest as mrb

LADDER_DIR = "data/d1trap_fractal_cache/strike_ladder"

CONFIGS = {
    "NIFTY":  dict(fname_prefix="niftyladder", step=50, round_step=100, lot=65, offset=150,
                   spot_path="data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet"),
    "SENSEX": dict(fname_prefix="sensexladder", step=100, round_step=100, lot=20, offset=300,
                   spot_path="data/d1trap_fractal_cache/sensex_1m_fullmonth_spot.parquet"),
}

CARRYOVER_TFS = [75, 120, 170, 240, 1440]   # minutes; 1440 = D1


def run_month_htf_carryover(underlying, fname_prefix, spot_path, offset, round_step, lot_size, htf_minutes):
    """Same as d1trap_verify_live_defaults.run_month_live but generic HTF
    minutes with prev-day carryover (today's live ref.close boundary,
    unchanged) -- reuses bb._detect_bear_zones directly."""
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

    def warmup(state, df1m, strike, side, day):
        state.strike, state.side = strike, side
        state.zones, state.flip_candidates, state.positions = [], [], []
        start = day - timedelta(days=mrb.HIST_WARMUP_DAYS)
        hist = df1m[(df1m["datetime"].dt.date >= start) & (df1m["datetime"].dt.date < day)]
        m_htf_hist = bb._resample(hist, htf_minutes)
        m15_hist = bb._resample(hist, 15)
        state.zones = mrb._prevalidate(bb._detect_bear_zones(bb._to_bars(m_htf_hist)), m15_hist)

    def refresh_intraday(state, ts, df1m, day):
        start = day - timedelta(days=mrb.HIST_WARMUP_DAYS)
        window = df1m[(df1m["datetime"].dt.date >= start) & (df1m["datetime"] < ts)]
        if len(window) < 30:
            return
        m_htf = bb._resample(window, htf_minutes)
        m15 = bb._resample(window, 15)
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
            warmup(ce_state, ce_df, ce_strike, "CE", day)
        if pe_state.strike != pe_strike:
            if pe_state.positions:
                last_row = pe_cache[pe_state.strike]
                last_today = last_row[last_row["datetime"].dt.date == day]
                px = last_today.iloc[0]["open"] if not last_today.empty else pe_state.positions[0]["entry_price"]
                mrb.check_exit(pe_state, lot_size, px, day_spot.iloc[0]["datetime"], force=True)
            warmup(pe_state, pe_df, pe_strike, "PE", day)

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
                    refresh_intraday(ce_state, ce_mhtf_today.iloc[cehtf]["timestamp"] + timedelta(minutes=htf_minutes), ce_df, day)
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
                    refresh_intraday(pe_state, pe_mhtf_today.iloc[pehtf]["timestamp"] + timedelta(minutes=htf_minutes), pe_df, day)
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

    all_trades = sorted(ce_state.trades + pe_state.trades, key=lambda t: t["exit_ts"])
    return all_trades


def run_month_15m_sameday(underlying, fname_prefix, spot_path, offset, round_step, lot_size):
    """15m HTF, but zones are SAME-DAY-ONLY -- rebuilt fresh from scratch each
    day using only that day's own 1m bars, never carried from prior days."""
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

        if ce_state.positions:
            last_today = ce_cache[ce_state.strike][ce_cache[ce_state.strike]["datetime"].dt.date == day]
            px = last_today.iloc[0]["open"] if not last_today.empty else ce_state.positions[0]["entry_price"]
            mrb.check_exit(ce_state, lot_size, px, day_spot.iloc[0]["datetime"], force=True)
        if pe_state.positions:
            last_today = pe_cache[pe_state.strike][pe_cache[pe_state.strike]["datetime"].dt.date == day]
            px = last_today.iloc[0]["open"] if not last_today.empty else pe_state.positions[0]["entry_price"]
            mrb.check_exit(pe_state, lot_size, px, day_spot.iloc[0]["datetime"], force=True)

        # Fresh, same-day-only zones -- no prior-day carryover at all.
        ce_state.strike, ce_state.side = ce_strike, "CE"
        pe_state.strike, pe_state.side = pe_strike, "PE"
        ce_state.zones, ce_state.flip_candidates, ce_state.positions = [], [], []
        pe_state.zones, pe_state.flip_candidates, pe_state.positions = [], [], []

        ce_today = ce_df[ce_df["datetime"].dt.date == day].reset_index(drop=True)
        pe_today = pe_df[pe_df["datetime"].dt.date == day].reset_index(drop=True)
        if ce_today.empty and pe_today.empty:
            continue
        ce_m5_today, ce_m15_today = bb._resample(ce_today, 5), bb._resample(ce_today, 15)
        pe_m5_today, pe_m15_today = bb._resample(pe_today, 5), bb._resample(pe_today, 15)

        max_len = max(len(ce_today), len(pe_today))
        ce15 = pe15 = 0
        for i in range(max_len):
            if i < len(ce_today):
                bar = ce_today.iloc[i]; ts = bar["datetime"]
                while ce15 < len(ce_m15_today) and ce_m15_today.iloc[ce15]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = ce_m15_today.iloc[ce15]
                    ce_state.prev15_high, ce_state.prev15_low = m15row["high"], m15row["low"]
                    # SAME-DAY zone discovery: detect from today's own 15m bars up to now.
                    window15 = ce_m15_today[ce_m15_today["timestamp"] <= m15row["timestamp"]]
                    existing_refs = {z["ref_ts"] for z in ce_state.zones}
                    new_zones = [z for z in bb._detect_bear_zones(bb._to_bars(window15)) if z["ref_ts"] not in existing_refs]
                    ce_state.zones.extend(mrb._prevalidate(new_zones, window15))
                    mrb.on_new_15m_close(ce_state, m15row)
                    mrb.process_flip_entry_t2(ce_state, lot_size, m15row, ce_m15_today, ce_m5_today, pe_state)
                    ce15 += 1
                mrb.check_exit(ce_state, lot_size, bar["close"], ts)
                mrb.check_fast_t1(ce_state, lot_size, bar["high"], ts, pe_state)
                mrb.process_zones_tick(ce_state, lot_size, ts, bar["low"], bar["high"], ce_m15_today, ce_m5_today, pe_state)
            if i < len(pe_today):
                bar = pe_today.iloc[i]; ts = bar["datetime"]
                while pe15 < len(pe_m15_today) and pe_m15_today.iloc[pe15]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = pe_m15_today.iloc[pe15]
                    pe_state.prev15_high, pe_state.prev15_low = m15row["high"], m15row["low"]
                    window15 = pe_m15_today[pe_m15_today["timestamp"] <= m15row["timestamp"]]
                    existing_refs = {z["ref_ts"] for z in pe_state.zones}
                    new_zones = [z for z in bb._detect_bear_zones(bb._to_bars(window15)) if z["ref_ts"] not in existing_refs]
                    pe_state.zones.extend(mrb._prevalidate(new_zones, window15))
                    mrb.on_new_15m_close(pe_state, m15row)
                    mrb.process_flip_entry_t2(pe_state, lot_size, m15row, pe_m15_today, pe_m5_today, ce_state)
                    pe15 += 1
                mrb.check_exit(pe_state, lot_size, bar["close"], ts)
                mrb.check_fast_t1(pe_state, lot_size, bar["high"], ts, ce_state)
                mrb.process_zones_tick(pe_state, lot_size, ts, bar["low"], bar["high"], pe_m15_today, pe_m5_today, ce_state)

    all_trades = sorted(ce_state.trades + pe_state.trades, key=lambda t: t["exit_ts"])
    return all_trades


if __name__ == "__main__":
    mrb.MONTH_DIR = LADDER_DIR
    for underlying, cfg in CONFIGS.items():
        print(f"\n{'#'*100}\n{underlying}\n{'#'*100}")
        for tf in CARRYOVER_TFS:
            try:
                trades = run_month_htf_carryover(underlying, cfg["fname_prefix"], cfg["spot_path"],
                                                  cfg["offset"], cfg["round_step"], cfg["lot"], tf)
                mrb.summarize(f"{underlying} {tf}m (carryover)", trades)
            except Exception as exc:
                print(f"{underlying} {tf}m FAILED: {exc}")
        trades_15_sameday = run_month_15m_sameday(underlying, cfg["fname_prefix"], cfg["spot_path"],
                                                    cfg["offset"], cfg["round_step"], cfg["lot"])
        mrb.summarize(f"{underlying} 15m (SAME-DAY-ONLY)", trades_15_sameday)
