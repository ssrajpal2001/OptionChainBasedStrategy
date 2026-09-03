"""
scripts/d1trap_zone_reentry_test.py -- test re-entry into a zone after an
SL-hit, IF the zone is still structurally valid (never closed a 15m candle
below zone_lo), per direct user spec (2026-08-04): "if SL gets hit but zone
is valid, trade can be initiated again when criteria are fulfilled."

Design decision (stated explicitly, not hidden): today, as soon as T1 fires
and no 5m subzone forms in that ref candle's own window (the common case),
the zone is marked done=True almost immediately -- independent of whether
T1 later wins or loses. Re-entry requires NOT setting done=True in that
case; instead, on an SL-hit closing a T1 leg, if the zone hasn't gone
invalid, re-arm it (reset breach_ts/sub_lo/sub_hi/armed so it re-enters the
ref-candle-breach stage fresh) -- capped at MAX_REENTRIES additional
attempts per zone so it can't churn indefinitely on a choppy day.

Runs on the settings just adopted live (SENSEX 7.5%/7.5% step + 8% capital
cap; NIFTY 5%/5% step + flat Rs2000 cap), full month window, both indices.
"""
import sys
sys.path.insert(0, ".")
from datetime import timedelta, date
import pandas as pd
import strategies.d1_trap_option.bear_only_book as bb
import scripts.d1trap_month_rolling_backtest as mrb
import scripts.d1trap_verify_live_defaults as verify

LADDER_DIR = "data/d1trap_fractal_cache/strike_ladder"
MAX_REENTRIES = 2

CONFIGS = {
    "SENSEX": dict(spot_path="data/d1trap_fractal_cache/sensex_1m_fullmonth_spot.parquet",
                   offset=300, round_step=100, lot=20, htf=15, step_pct=0.075, step_lock=0.075, pct_cap=0.08),
    "NIFTY":  dict(spot_path="data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet",
                   offset=150, round_step=100, lot=65, htf=60, step_pct=0.05, step_lock=0.05, pct_cap=None),
}


def make_open_leg(cfg):
    def patched(state, lot_size, tranche, entry_price, sl, zone_lock_ts, audit=None, entry_ts=None):
        sl_buffered = sl - mrb.SL_BUFFER_PTS
        if cfg["pct_cap"] is not None:
            floor = entry_price * (1 - cfg["pct_cap"])
        else:
            floor = entry_price - mrb.MAX_RISK_RS_PER_LOT / lot_size
        sl_final = max(sl_buffered, floor)
        state.positions.append(dict(side=state.side, strike=state.strike, entry_price=entry_price, sl=sl_final,
                                     high_lock_pct=0.0, tranche=tranche, zone_lock_ts=zone_lock_ts,
                                     tsl_base_pct=bb._TSL_TRANCHE_BASE_PCT, tsl_base_lock_pct=bb._TSL_TRANCHE_BASE_LOCK_PCT,
                                     tsl_step_pct=cfg["step_pct"], tsl_step_lock_pct=cfg["step_lock"],
                                     audit=audit or {}, entry_ts=entry_ts))
    return patched


def process_zones_tick_reentry(state, lot_size, last_ts, last_low, last_high, m15, m5, other_state):
    """Copy of mrb.process_zones_tick's T1 entry branch, with SL-hit re-arm
    added -- everything else (T2 subzone/arm/swing-breach) unchanged."""
    if last_ts.time() >= mrb.ENTRY_CUTOFF:
        return
    active_locks = {p["zone_lock_ts"] for p in state.positions}
    for zone in state.zones:
        if zone["done"] or zone["invalid"]:
            continue
        if (state.positions and zone["ref_ts"] not in active_locks) or (not state.positions and other_state.positions):
            continue
        if zone["state"] == "WAITING":
            if last_low <= zone["zone_hi"]:
                zone["state"] = "MONITORING"
                zone["contact_ts"] = last_ts
            continue
        if zone["state"] != "MONITORING":
            continue
        if zone["ref_open"] is None:
            if last_ts.time() < mrb.EARLY_CUTOFF:
                continue
            ref = bb.D1TrapBearOnlyBook._find_latest_closed_ref_bar(m15, last_ts)
            if ref is not None:
                zone["ref_open"], zone["ref_close_time"] = ref.timestamp, ref.timestamp + timedelta(minutes=15)
                zone["ref15_high"], zone["ref15_low"] = ref.high, ref.low
            continue
        if zone["breach_ts"] is None:
            if last_ts >= zone["ref_close_time"] and last_high >= zone["ref15_high"]:
                zone["breach_ts"] = last_ts
                zone["_reentry_ref_ts"] = zone["ref_ts"]  # tag this T1 attempt for re-arm bookkeeping
                mrb.open_leg(state, lot_size, "T1", zone["ref15_high"], zone["ref15_low"], zone["ref_ts"], entry_ts=last_ts)
                active_locks.add(zone["ref_ts"])
                continue
            new_ref = bb.D1TrapBearOnlyBook._find_latest_closed_ref_bar(m15, last_ts)
            if new_ref is not None and new_ref.timestamp > zone["ref_open"]:
                zone["ref_open"], zone["ref_close_time"] = new_ref.timestamp, new_ref.timestamp + timedelta(minutes=15)
                zone["ref15_high"], zone["ref15_low"] = new_ref.high, new_ref.low
            continue
        if zone["sub_lo"] is None:
            window_5m = m5[(m5["timestamp"] >= zone["ref_open"]) & (m5["timestamp"] < zone["ref_close_time"])]
            collapse = bb._collapse_subzones(bb._to_bars(window_5m))
            if collapse is None:
                # 2026-08-04: was `zone["done"] = True; return` unconditionally here --
                # now, only finalize the zone if it can't still be re-armed after a
                # future SL-hit (tracked via zone["reentries_used"]).
                if zone.get("reentries_used", 0) >= MAX_REENTRIES:
                    zone["done"] = True
                return
            zone["sub_lo"], zone["sub_hi"] = collapse
            threshold_pts = bb._ZONE_SIZE_THRESHOLD_PCT / 100.0 * zone["ref15_high"]
            zone["arm_level"] = bb._arm_level(zone["sub_lo"], zone["sub_hi"], threshold_pts)
            zone["armed"] = False
            continue
        if not zone.get("armed"):
            if zone["sub_lo"] <= last_low <= zone["arm_level"]:
                zone["armed"] = True
            continue
        if last_high >= zone["sub_hi"]:
            if not any(p["zone_lock_ts"] == zone["ref_ts"] and p["tranche"] == "T2" for p in state.positions):
                mrb.open_leg(state, lot_size, "T2", zone["sub_hi"], zone["ref15_low"], zone["ref_ts"], entry_ts=last_ts)
            zone["done"] = True
            return


def check_exit_reentry(state, lot_size, ltp, ts, force=False, force_reason="day_switch"):
    """Copy of mrb.check_exit, with: on an sl_hit closing a T1 leg whose
    zone is still valid and under the re-entry cap, re-arm the zone
    (reset breach_ts/sub_lo/sub_hi/armed/arm_level) instead of leaving it
    permanently spent."""
    now_t = ts.time()
    for pos in list(state.positions):
        entry = pos["entry_price"]
        if force:
            mrb._close(state, lot_size, pos, force_reason, ltp, ts)
            continue
        profit_pct = (ltp - entry) / entry
        if profit_pct >= pos["tsl_base_pct"]:
            steps = int((profit_pct - pos["tsl_base_pct"]) // pos["tsl_step_pct"])
            calc_lock = pos["tsl_base_lock_pct"] + steps * pos["tsl_step_lock_pct"]
            pos["high_lock_pct"] = max(pos["high_lock_pct"], calc_lock)
        stop_price = entry * (1 + pos["high_lock_pct"]) if pos["high_lock_pct"] > 0 else pos["sl"]
        if ltp <= stop_price:
            reason = "tsl_hit" if pos["high_lock_pct"] > 0 else "sl_hit"
            mrb._close(state, lot_size, pos, reason, stop_price, ts)
            if reason == "sl_hit" and pos["tranche"] == "T1":
                zone = next((z for z in state.zones if z["ref_ts"] == pos["zone_lock_ts"]), None)
                if zone is not None and not zone["invalid"] and not zone["done"] and zone.get("reentries_used", 0) < MAX_REENTRIES:
                    zone["reentries_used"] = zone.get("reentries_used", 0) + 1
                    zone["breach_ts"] = None
                    zone["sub_lo"] = zone["sub_hi"] = zone["arm_level"] = None
                    zone["armed"] = False
        elif now_t >= mrb.EOD_TIME:
            mrb._close(state, lot_size, pos, "eod", ltp, ts)


def run_month(underlying, cfg, use_reentry):
    spot = pd.read_parquet(cfg["spot_path"])
    spot["datetime"] = pd.to_datetime(spot["datetime"])
    days = sorted(d for d in spot["datetime"].dt.date.unique() if mrb.DAY_MIN <= d <= mrb.DAY_MAX)
    fname_prefix = "sensexladder" if underlying == "SENSEX" else "niftyladder"

    orig_open_leg = mrb.open_leg
    mrb.open_leg = make_open_leg(cfg)
    process_zones = process_zones_tick_reentry if use_reentry else mrb.process_zones_tick
    check_exit = check_exit_reentry if use_reentry else mrb.check_exit

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
        m_htf_hist, m15_hist = bb._resample(hist, cfg["htf"]), bb._resample(hist, 15)
        state.zones = mrb._prevalidate(bb._detect_bear_zones(bb._to_bars(m_htf_hist)), m15_hist)

    def refresh(state, ts, df1m, day):
        start = day - timedelta(days=mrb.HIST_WARMUP_DAYS)
        window = df1m[(df1m["datetime"].dt.date >= start) & (df1m["datetime"] < ts)]
        if len(window) < 30:
            return
        m_htf, m15 = bb._resample(window, cfg["htf"]), bb._resample(window, 15)
        existing_refs = {z["ref_ts"] for z in state.zones}
        new_zones = [z for z in bb._detect_bear_zones(bb._to_bars(m_htf)) if z["ref_ts"] not in existing_refs]
        state.zones.extend(mrb._prevalidate(new_zones, m15))

    try:
        for day in days:
            day_spot = spot[spot["datetime"].dt.date == day]
            if day_spot.empty:
                continue
            o = day_spot.iloc[0]["open"]
            atm = round(o / cfg["round_step"]) * cfg["round_step"]
            ce_strike, pe_strike = int(atm - cfg["offset"]), int(atm + cfg["offset"])
            ce_df, pe_df = get_bars(ce_strike, "CE"), get_bars(pe_strike, "PE")
            if ce_df.empty or pe_df.empty:
                continue

            if ce_state.strike != ce_strike:
                if ce_state.positions:
                    last_row = ce_cache[ce_state.strike]
                    last_today = last_row[last_row["datetime"].dt.date == day]
                    px = last_today.iloc[0]["open"] if not last_today.empty else ce_state.positions[0]["entry_price"]
                    check_exit(ce_state, cfg["lot"], px, day_spot.iloc[0]["datetime"], force=True)
                warmup(ce_state, ce_df, ce_strike, "CE", day)
            if pe_state.strike != pe_strike:
                if pe_state.positions:
                    last_row = pe_cache[pe_state.strike]
                    last_today = last_row[last_row["datetime"].dt.date == day]
                    px = last_today.iloc[0]["open"] if not last_today.empty else pe_state.positions[0]["entry_price"]
                    check_exit(pe_state, cfg["lot"], px, day_spot.iloc[0]["datetime"], force=True)
                warmup(pe_state, pe_df, pe_strike, "PE", day)

            ce_today = ce_df[ce_df["datetime"].dt.date == day].reset_index(drop=True)
            pe_today = pe_df[pe_df["datetime"].dt.date == day].reset_index(drop=True)
            ce_m5, ce_m15, ce_mhtf = bb._resample(ce_df, 5), bb._resample(ce_df, 15), bb._resample(ce_df, cfg["htf"])
            pe_m5, pe_m15, pe_mhtf = bb._resample(pe_df, 5), bb._resample(pe_df, 15), bb._resample(pe_df, cfg["htf"])
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
                    while cehtf < len(ce_mhtf_today) and ce_mhtf_today.iloc[cehtf]["timestamp"] + timedelta(minutes=cfg["htf"]) <= ts:
                        refresh(ce_state, ce_mhtf_today.iloc[cehtf]["timestamp"] + timedelta(minutes=cfg["htf"]), ce_df, day)
                        cehtf += 1
                    while ce15 < len(ce_m15_today) and ce_m15_today.iloc[ce15]["timestamp"] + timedelta(minutes=15) <= ts:
                        m15row = ce_m15_today.iloc[ce15]
                        ce_state.prev15_high, ce_state.prev15_low = m15row["high"], m15row["low"]
                        mrb.on_new_15m_close(ce_state, m15row)
                        mrb.process_flip_entry_t2(ce_state, cfg["lot"], m15row, ce_m15, ce_m5, pe_state)
                        ce15 += 1
                    check_exit(ce_state, cfg["lot"], bar["close"], ts)
                    mrb.check_fast_t1(ce_state, cfg["lot"], bar["high"], ts, pe_state)
                    process_zones(ce_state, cfg["lot"], ts, bar["low"], bar["high"], ce_m15, ce_m5, pe_state)
                if i < len(pe_today):
                    bar = pe_today.iloc[i]; ts = bar["datetime"]
                    while pehtf < len(pe_mhtf_today) and pe_mhtf_today.iloc[pehtf]["timestamp"] + timedelta(minutes=cfg["htf"]) <= ts:
                        refresh(pe_state, pe_mhtf_today.iloc[pehtf]["timestamp"] + timedelta(minutes=cfg["htf"]), pe_df, day)
                        pehtf += 1
                    while pe15 < len(pe_m15_today) and pe_m15_today.iloc[pe15]["timestamp"] + timedelta(minutes=15) <= ts:
                        m15row = pe_m15_today.iloc[pe15]
                        pe_state.prev15_high, pe_state.prev15_low = m15row["high"], m15row["low"]
                        mrb.on_new_15m_close(pe_state, m15row)
                        mrb.process_flip_entry_t2(pe_state, cfg["lot"], m15row, pe_m15, pe_m5, ce_state)
                        pe15 += 1
                    check_exit(pe_state, cfg["lot"], bar["close"], ts)
                    mrb.check_fast_t1(pe_state, cfg["lot"], bar["high"], ts, ce_state)
                    process_zones(pe_state, cfg["lot"], ts, bar["low"], bar["high"], pe_m15, pe_m5, ce_state)
    finally:
        mrb.open_leg = orig_open_leg

    return sorted(ce_state.trades + pe_state.trades, key=lambda t: t["exit_ts"])


if __name__ == "__main__":
    mrb.MONTH_DIR = LADDER_DIR
    for underlying, cfg in CONFIGS.items():
        print(f"\n{'#'*90}\n{underlying}\n{'#'*90}")
        baseline = run_month(underlying, cfg, use_reentry=False)
        mrb.summarize(f"{underlying} baseline (no zone re-entry, current live)", baseline)
        reentry = run_month(underlying, cfg, use_reentry=True)
        mrb.summarize(f"{underlying} WITH zone re-entry (max {MAX_REENTRIES} extra T1 attempts)", reentry)
