"""
scripts/d1trap_zone_definition_sweep.py -- Stage 2 of the intraday D1-Trap
optimization: zone TIMEFRAME (60m current default vs 15m, matching how the
user actually reads charts manually) x zone BOUNDARY (ref.low current
default vs ref.close, matching the user's manual method exactly -- see the
07-30/07-31 SENSEX 78000CE chart walkthrough: zone_hi=412.35=ref CLOSE,
zone_lo=204.35=next-candle LOW).

Everything else (entry cascade -- contact/MONITORING/ref-candle
assignment/breach/T1, 5m subzone/arm/swing-breach/T2, flip concept, SL/TSL)
is UNCHANGED and reused directly from scripts/d1trap_month_rolling_backtest
-- only zone construction (_detect_zones_corrected + the HTF resample used
to seed/refresh it) is swapped per variant.

Runs at the Stage-1 winning strike depth (3-ITM, see
scripts/d1trap_strike_ladder_backtest.py output) for both NIFTY and SENSEX,
same real month window, same cached data (data/d1trap_fractal_cache/
strike_ladder/, already fetched for the ladder test).
"""
import sys
sys.path.insert(0, ".")
from datetime import timedelta, date

import pandas as pd
import strategies.d1_trap_option.bear_only_book as bb
from strategies.v4_cascade.rolling_base import find_all_bear_zones
import scripts.d1trap_month_rolling_backtest as mrb

LADDER_DIR = "data/d1trap_fractal_cache/strike_ladder"
CONFIGS = {
    "NIFTY":  dict(fname_prefix="niftyladder", step=50, round_step=100, lot=65, itm_n=3,
                   spot_path="data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet"),
    "SENSEX": dict(fname_prefix="sensexladder", step=100, round_step=100, lot=20, itm_n=3,
                   spot_path="data/d1trap_fractal_cache/sensex_1m_fullmonth_spot.parquet"),
}
VARIANTS = [
    ("60m / ref.low  (current default)", 60, "ref_low"),
    ("60m / ref.close", 60, "ref_close"),
    ("15m / ref.low", 15, "ref_low"),
    ("15m / ref.close (user's manual method)", 15, "ref_close"),
]


def make_detect_zones(boundary_mode):
    def _detect(bars):
        n = len(bars)
        idx_by_ts = {b.timestamp: i for i, b in enumerate(bars)}
        raw = []
        for z in find_all_bear_zones(bars):
            ref_i = idx_by_ts[z.reference_low_ts]
            ref = bars[ref_i]
            sellers_in = None
            for j in range(ref_i + 1, n):
                if bars[j].low < ref.low:
                    sellers_in = bars[j]
                    break
            if sellers_in is None:
                continue
            if boundary_mode == "ref_close":
                lo, hi = sellers_in.low, ref.close
            else:
                lo, hi = min(ref.low, sellers_in.low), max(ref.low, sellers_in.low)
            raw.append(dict(zone_lo=lo, zone_hi=hi, entry_line=ref.low, lock_ts=z.lock_ts,
                             ref_ts=ref.timestamp, ref_idx=ref_i, ref_high=ref.high, ref_low=ref.low,
                             sellers_in_ts=sellers_in.timestamp, sellers_in_low=sellers_in.low,
                             reclaim_ts=z.lock_ts, reclaim_high=bars[idx_by_ts[z.lock_ts]].high,
                             state="WAITING", ref_bar=None, done=False, invalid=False,
                             contact_ts=None, ref_open=None, ref_close_time=None,
                             breach_ts=None, sub_lo=None, sub_hi=None))
        if not raw:
            return []
        ordered = sorted(raw, key=lambda z: (z["zone_lo"], z["zone_hi"]))
        groups = [[ordered[0]]]
        for z in ordered[1:]:
            grp = groups[-1]
            group_hi = max(g["zone_hi"] for g in grp)
            near_in_time = any(abs(z["ref_idx"] - g["ref_idx"]) <= mrb.MAX_REF_GAP for g in grp)
            truly_overlaps = z["zone_lo"] <= group_hi
            if near_in_time and (truly_overlaps or z["zone_lo"] <= group_hi + mrb.MERGE_THRESHOLD_PTS):
                grp.append(z)
            else:
                groups.append([z])
        merged = []
        for group in groups:
            newest = max(group, key=lambda g: g["lock_ts"])
            merged.append(dict(zone_lo=min(g["zone_lo"] for g in group), zone_hi=max(g["zone_hi"] for g in group),
                                entry_line=newest["entry_line"], lock_ts=newest["lock_ts"],
                                ref_ts=newest["ref_ts"], ref_idx=newest["ref_idx"],
                                ref_high=newest["ref_high"], ref_low=newest["ref_low"],
                                sellers_in_ts=newest["sellers_in_ts"], sellers_in_low=newest["sellers_in_low"],
                                reclaim_ts=newest["reclaim_ts"], reclaim_high=newest["reclaim_high"],
                                state="WAITING", ref_bar=None, done=False, invalid=False,
                                contact_ts=None, ref_open=None, ref_close_time=None,
                                breach_ts=None, sub_lo=None, sub_hi=None))
        return merged
    return _detect


def run_month_htf(underlying, fname_prefix, spot_path, offset, round_step, lot_size,
                   htf_minutes, boundary_mode):
    detect_fn = make_detect_zones(boundary_mode)
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

    def warmup_htf(state, df1m, strike, side, day):
        state.strike, state.side = strike, side
        state.zones, state.flip_candidates, state.positions = [], [], []
        start = day - timedelta(days=mrb.HIST_WARMUP_DAYS)
        hist = df1m[(df1m["datetime"].dt.date >= start) & (df1m["datetime"].dt.date < day)]
        m_htf_hist, m15_hist = bb._resample(hist, htf_minutes), bb._resample(hist, 15)
        state.zones = mrb._prevalidate(detect_fn(bb._to_bars(m_htf_hist)), m15_hist)

    def refresh_intraday_htf(state, ts, df1m, day):
        start = day - timedelta(days=mrb.HIST_WARMUP_DAYS)
        window = df1m[(df1m["datetime"].dt.date >= start) & (df1m["datetime"] < ts)]
        if len(window) < 30:
            return
        m_htf, m15 = bb._resample(window, htf_minutes), bb._resample(window, 15)
        existing_refs = {z["ref_ts"] for z in state.zones}
        new_zones = [z for z in detect_fn(bb._to_bars(m_htf)) if z["ref_ts"] not in existing_refs]
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
            warmup_htf(ce_state, ce_df, ce_strike, "CE", day)
        if pe_state.strike != pe_strike:
            if pe_state.positions:
                last_row = pe_cache[pe_state.strike]
                last_today = last_row[last_row["datetime"].dt.date == day]
                px = last_today.iloc[0]["open"] if not last_today.empty else pe_state.positions[0]["entry_price"]
                mrb.check_exit(pe_state, lot_size, px, day_spot.iloc[0]["datetime"], force=True)
            warmup_htf(pe_state, pe_df, pe_strike, "PE", day)

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
                    refresh_intraday_htf(ce_state, ce_mhtf_today.iloc[cehtf]["timestamp"] + timedelta(minutes=htf_minutes), ce_df, day)
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
                    refresh_intraday_htf(pe_state, pe_mhtf_today.iloc[pehtf]["timestamp"] + timedelta(minutes=htf_minutes), pe_df, day)
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


if __name__ == "__main__":
    mrb.MONTH_DIR = LADDER_DIR
    results = {}
    for underlying, cfg in CONFIGS.items():
        print(f"\n{'#'*100}\n{underlying} (offset={cfg['itm_n']}-ITM = {cfg['itm_n']*cfg['step']}pts, Stage-1 winner)\n{'#'*100}")
        results[underlying] = {}
        offset = cfg["itm_n"] * cfg["step"]
        for label, htf_minutes, boundary_mode in VARIANTS:
            trades = run_month_htf(underlying, cfg["fname_prefix"], cfg["spot_path"], offset,
                                    cfg["round_step"], cfg["lot"], htf_minutes, boundary_mode)
            stats = mrb.summarize(f"{underlying} {label}", trades)
            results[underlying][label] = stats

    print(f"\n{'='*100}\nSTAGE 2 SUMMARY -- ZONE TIMEFRAME x BOUNDARY\n{'='*100}")
    for underlying, by_label in results.items():
        print(f"\n{underlying}")
        print(f"{'Variant':<42}{'Trades':>8}{'Win%':>8}{'PF':>8}{'NetPnL':>12}")
        for label, _, _ in VARIANTS:
            s = by_label[label]
            print(f"{label:<42}{s['n']:>8}{s['win_pct']:>7.1f}%{s['pf']:>8.2f}{s['total']:>+12,.0f}")
