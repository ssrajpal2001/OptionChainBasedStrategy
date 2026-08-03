"""
scripts/d1trap_bos_premature_exit_check.py -- direct user question (2026-08-04):
of the trades exited via breakeven_bos (premium-level BOS firing before 20%
profit), how many times was the OWNING ZONE still structurally valid (never
closed a 15m candle through zone_lo) at the moment of that exit, AND price
went on to run further / reach the original day_t1 target afterward -- i.e.
a premature exit that gave up a trade the zone itself never invalidated.

Reuses the exact same simulation as d1trap_tsl_finestep_bos_test.py
(7.5%/7.5% fine-step + pre-20% BOS->breakeven, the best-net config), with
added instrumentation: at each breakeven_bos exit, record whether the
owning zone was invalid yet, and walk the rest of that day's premium to see
the post-exit MFE and whether day_t1 was eventually reached.
"""
import sys
sys.path.insert(0, ".")
from datetime import timedelta, date
import pandas as pd
import strategies.d1_trap_option.bear_only_book as bb
import scripts.d1trap_month_rolling_backtest as mrb
from strategies.fvg.detector import find_swing_points
import scripts.d1trap_tsl_finestep_bos_test as ft

STEP_PCT, STEP_LOCK = 0.075, 0.075


def run_with_diagnostics():
    spot = pd.read_parquet(ft.SPOT_PATH)
    spot["datetime"] = pd.to_datetime(spot["datetime"])
    days = sorted(d for d in spot["datetime"].dt.date.unique() if mrb.DAY_MIN <= d <= mrb.DAY_MAX)

    diagnostics = []  # one dict per breakeven_bos exit

    def check_exit_diag(state, lot_size, ltp, low_px, ts, tracker, side_df, day_df, force=False, force_reason="day_switch"):
        now_t = ts.time()
        for pos in list(state.positions):
            entry = pos["entry_price"]
            if force:
                mrb._close(state, lot_size, pos, force_reason, ltp, ts)
                continue

            if not pos.get("bos_fired"):
                eh, el = pos.get("entry_swing_high"), pos.get("entry_swing_low")
                cur_h, cur_l = tracker.last_high, tracker.last_low
                lower_high = eh is not None and cur_h is not None and cur_h.ts > eh.ts and cur_h.price < eh.price
                low_broken = cur_l is not None and low_px < cur_l.price
                if lower_high or low_broken:
                    pos["bos_fired"] = True

            profit_pct = (ltp - entry) / entry
            if profit_pct >= ft.BASE_PCT:
                steps = int((profit_pct - ft.BASE_PCT) // STEP_PCT)
                calc_lock = ft.BASE_LOCK + steps * STEP_LOCK
                pos["high_lock_pct"] = max(pos["high_lock_pct"], calc_lock)

            if pos["high_lock_pct"] > 0:
                stop_price, reason = entry * (1 + pos["high_lock_pct"]), "tsl_hit"
            else:
                stop_price, reason = pos["sl"], "sl_hit"
                if pos.get("bos_fired") and entry > stop_price:
                    stop_price, reason = entry, "breakeven_bos"

            if ltp <= stop_price:
                if reason == "breakeven_bos":
                    zone = next((z for z in state.zones if z["ref_ts"] == pos["zone_lock_ts"]), None)
                    zone_invalid = bool(zone and zone.get("invalid"))
                    day_t1 = zone.get("ref15_high") if zone else None
                    future = side_df[(side_df["datetime"] > ts) & (side_df["datetime"].dt.date == ts.date())]
                    post_mfe = future["high"].max() if not future.empty else stop_price
                    reached_t1 = bool(day_t1 and post_mfe >= day_t1)
                    diagnostics.append(dict(
                        side=state.side, strike=state.strike, entry_ts=pos.get("entry_ts"), exit_ts=ts,
                        entry=entry, exit=stop_price, zone_still_valid=not zone_invalid,
                        post_exit_mfe=post_mfe, post_exit_run_pct=(post_mfe - stop_price) / stop_price * 100,
                        day_t1=day_t1, reached_day_t1_after=reached_t1,
                    ))
                mrb._close(state, lot_size, pos, reason, stop_price, ts)
            elif now_t >= mrb.EOD_TIME:
                mrb._close(state, lot_size, pos, "eod", ltp, ts)

    ce_state, pe_state = mrb.SideState(), mrb.SideState()
    ce_tracker, pe_tracker = ft.SwingTracker(ft.BOS_TF, ft.BOS_PIVOT), ft.SwingTracker(ft.BOS_TF, ft.BOS_PIVOT)
    ce_cache, pe_cache = {}, {}

    def get_bars(strike, side):
        cache = ce_cache if side == "CE" else pe_cache
        if strike not in cache:
            cache[strike] = mrb.load_bars("sensexladder", strike, side)
        return cache[strike]

    def warmup(state, df1m, strike, side, day, tracker):
        state.strike, state.side = strike, side
        state.zones, state.flip_candidates, state.positions = [], [], []
        tracker.reset()
        start = day - timedelta(days=mrb.HIST_WARMUP_DAYS)
        hist = df1m[(df1m["datetime"].dt.date >= start) & (df1m["datetime"].dt.date < day)]
        m_htf_hist, m15_hist = bb._resample(hist, ft.HTF if hasattr(ft, "HTF") else 15), bb._resample(hist, 15)
        state.zones = mrb._prevalidate(bb._detect_bear_zones(bb._to_bars(m_htf_hist)), m15_hist)

    def refresh(state, ts, df1m, day):
        start = day - timedelta(days=mrb.HIST_WARMUP_DAYS)
        window = df1m[(df1m["datetime"].dt.date >= start) & (df1m["datetime"] < ts)]
        if len(window) < 30:
            return
        m_htf, m15 = bb._resample(window, 15), bb._resample(window, 15)
        existing_refs = {z["ref_ts"] for z in state.zones}
        new_zones = [z for z in bb._detect_bear_zones(bb._to_bars(m_htf)) if z["ref_ts"] not in existing_refs]
        state.zones.extend(mrb._prevalidate(new_zones, m15))

    for day in days:
        day_spot = spot[spot["datetime"].dt.date == day]
        if day_spot.empty:
            continue
        o = day_spot.iloc[0]["open"]
        atm = round(o / ft.ROUND_STEP) * ft.ROUND_STEP
        ce_strike, pe_strike = int(atm - ft.OFFSET), int(atm + ft.OFFSET)
        ce_df, pe_df = get_bars(ce_strike, "CE"), get_bars(pe_strike, "PE")
        if ce_df.empty or pe_df.empty:
            continue

        if ce_state.strike != ce_strike:
            if ce_state.positions:
                last_row = ce_cache[ce_state.strike]
                last_today = last_row[last_row["datetime"].dt.date == day]
                px = last_today.iloc[0]["open"] if not last_today.empty else ce_state.positions[0]["entry_price"]
                check_exit_diag(ce_state, ft.LOT, px, px, day_spot.iloc[0]["datetime"], ce_tracker, ce_df, day_spot, force=True)
            warmup(ce_state, ce_df, ce_strike, "CE", day, ce_tracker)
        if pe_state.strike != pe_strike:
            if pe_state.positions:
                last_row = pe_cache[pe_state.strike]
                last_today = last_row[last_row["datetime"].dt.date == day]
                px = last_today.iloc[0]["open"] if not last_today.empty else pe_state.positions[0]["entry_price"]
                check_exit_diag(pe_state, ft.LOT, px, px, day_spot.iloc[0]["datetime"], pe_tracker, pe_df, day_spot, force=True)
            warmup(pe_state, pe_df, pe_strike, "PE", day, pe_tracker)

        ce_today = ce_df[ce_df["datetime"].dt.date == day].reset_index(drop=True)
        pe_today = pe_df[pe_df["datetime"].dt.date == day].reset_index(drop=True)
        ce_m5, ce_m15, ce_mhtf = bb._resample(ce_df, 5), bb._resample(ce_df, 15), bb._resample(ce_df, 15)
        pe_m5, pe_m15, pe_mhtf = bb._resample(pe_df, 5), bb._resample(pe_df, 15), bb._resample(pe_df, 15)
        ce_mswing, pe_mswing = bb._resample(ce_df, ft.BOS_TF), bb._resample(pe_df, ft.BOS_TF)
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
                while cehtf < len(ce_mhtf_today) and ce_mhtf_today.iloc[cehtf]["timestamp"] + timedelta(minutes=15) <= ts:
                    refresh(ce_state, ce_mhtf_today.iloc[cehtf]["timestamp"] + timedelta(minutes=15), ce_df, day)
                    cehtf += 1
                while ce15 < len(ce_m15_today) and ce_m15_today.iloc[ce15]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = ce_m15_today.iloc[ce15]
                    ce_state.prev15_high, ce_state.prev15_low = m15row["high"], m15row["low"]
                    mrb.on_new_15m_close(ce_state, m15row)
                    mrb.process_flip_entry_t2(ce_state, ft.LOT, m15row, ce_m15, ce_m5, pe_state)
                    ce15 += 1
                ce_tracker.advance(ce_mswing, ts)
                check_exit_diag(ce_state, ft.LOT, bar["close"], bar["low"], ts, ce_tracker, ce_df, day_spot)
                mrb.check_fast_t1(ce_state, ft.LOT, bar["high"], ts, pe_state)
                mrb.process_zones_tick(ce_state, ft.LOT, ts, bar["low"], bar["high"], ce_m15, ce_m5, pe_state)
                ft._snapshot(ce_state, ce_tracker)
            if i < len(pe_today):
                bar = pe_today.iloc[i]; ts = bar["datetime"]
                while pehtf < len(pe_mhtf_today) and pe_mhtf_today.iloc[pehtf]["timestamp"] + timedelta(minutes=15) <= ts:
                    refresh(pe_state, pe_mhtf_today.iloc[pehtf]["timestamp"] + timedelta(minutes=15), pe_df, day)
                    pehtf += 1
                while pe15 < len(pe_m15_today) and pe_m15_today.iloc[pe15]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = pe_m15_today.iloc[pe15]
                    pe_state.prev15_high, pe_state.prev15_low = m15row["high"], m15row["low"]
                    mrb.on_new_15m_close(pe_state, m15row)
                    mrb.process_flip_entry_t2(pe_state, ft.LOT, m15row, pe_m15, pe_m5, ce_state)
                    pe15 += 1
                pe_tracker.advance(pe_mswing, ts)
                check_exit_diag(pe_state, ft.LOT, bar["close"], bar["low"], ts, pe_tracker, pe_df, day_spot)
                mrb.check_fast_t1(pe_state, ft.LOT, bar["high"], ts, ce_state)
                mrb.process_zones_tick(pe_state, ft.LOT, ts, bar["low"], bar["high"], pe_m15, pe_m5, ce_state)
                ft._snapshot(pe_state, pe_tracker)

    return diagnostics


if __name__ == "__main__":
    mrb.MONTH_DIR = ft.LADDER_DIR
    diag = run_with_diagnostics()
    print(f"Total breakeven_bos exits: {len(diag)}\n")
    still_valid = [d for d in diag if d["zone_still_valid"]]
    reached_after = [d for d in diag if d["reached_day_t1_after"]]
    ran_up_after = [d for d in diag if d["post_exit_run_pct"] > 5]
    print(f"Zone still structurally valid at time of BOS exit: {len(still_valid)}/{len(diag)}")
    print(f"Of those, price later reached the zone's day_t1 target anyway: "
          f"{sum(1 for d in still_valid if d['reached_day_t1_after'])}/{len(still_valid)}")
    print(f"Premium ran >5% higher AFTER the breakeven exit (any zone state): {len(ran_up_after)}/{len(diag)}")
    print()
    for d in diag:
        print(f"{d['entry_ts'].strftime('%m-%d %H:%M')} {d['side']}{d['strike']}  entry={d['entry']:.2f} "
              f"exit={d['exit']:.2f}  zone_valid={d['zone_still_valid']}  "
              f"post_exit_MFE={d['post_exit_mfe']:.2f} (+{d['post_exit_run_pct']:.1f}%)  "
              f"day_t1={d['day_t1']}  reached_t1_after={d['reached_day_t1_after']}")
