"""
scripts/d1trap_tsl_finestep_bos_test.py -- user-specified exit redesign
(2026-08-04), tested against real SENSEX option premium, full month window
(06-29..07-31), on the WINNING strike selection from the first-15m-candle
comparison (baseline: 300pt offset from ATM open -- the first-15m idea
underperformed and was not adopted):

1. Fine-step trailing SL: base trigger 20% -> lock 12.5% (unchanged), but
   AFTER that every further step_pct% of additional profit locks another
   step_lock_pct% (default 5%/5%, swept against 7.5%/7.5% and 10%/10%) --
   replaces the current tranche ladder's 20%-gap next tier (effectively
   40% before the lock ever moves past 12.5%), which is why every real
   trade that ran 28-34% gave back to a flat 12.5% lock.
2. Breakeven-on-BOS, ONLY while profit has NOT yet reached the 20% base
   trigger: if a break of structure fires on the option's OWN premium (5m
   swing timeframe, pivot=5 -- the setting that held up best, not the
   overfit-looking tight ones, from the earlier BOS sweep), move the SL up
   to breakeven (entry price) as a floor. Once profit crosses 20%, the
   staircase takes over and this floor no longer applies (superseded).

Baseline for comparison: current live tranche defaults (20%/12.5%, next
tier at 40%), no BOS.
"""
import sys
sys.path.insert(0, ".")
from datetime import timedelta, date
import pandas as pd
import strategies.d1_trap_option.bear_only_book as bb
import scripts.d1trap_month_rolling_backtest as mrb
import scripts.d1trap_verify_live_defaults as verify
from strategies.fvg.detector import find_swing_points

LADDER_DIR = "data/d1trap_fractal_cache/strike_ladder"
SPOT_PATH = "data/d1trap_fractal_cache/sensex_1m_fullmonth_spot.parquet"
OFFSET, ROUND_STEP, LOT, HTF = 300, 100, 20, 15

BASE_PCT, BASE_LOCK = 0.20, 0.125
BOS_TF, BOS_PIVOT = 5, 5


class _Swing:
    __slots__ = ("price", "ts")
    def __init__(self, price, ts):
        self.price, self.ts = price, ts


class SwingTracker:
    def __init__(self, tf_minutes, pivot):
        self.tf, self.pivot = tf_minutes, pivot
        self.bars, self.last_high, self.last_low, self._ridx = [], None, None, 0

    def reset(self):
        self.bars, self.last_high, self.last_low, self._ridx = [], None, None, 0

    def advance(self, resampled, ts):
        while self._ridx < len(resampled) and resampled.iloc[self._ridx]["timestamp"] + timedelta(minutes=self.tf) <= ts:
            row = resampled.iloc[self._ridx]
            self.bars.append(bb._Bar(row["timestamp"], row["open"], row["high"], row["low"], row["close"]))
            self._ridx += 1
            if len(self.bars) >= 2 * self.pivot + 1:
                window = self.bars[-(10 * self.pivot + 5):]
                swings = find_swing_points(window, pivot=self.pivot)
                highs = [s for s in swings if s.kind == "HIGH"]
                lows = [s for s in swings if s.kind == "LOW"]
                if highs:
                    s = highs[-1]; self.last_high = _Swing(s.price, window[s.index].timestamp)
                if lows:
                    s = lows[-1]; self.last_low = _Swing(s.price, window[s.index].timestamp)


def make_check_exit(step_pct, step_lock, use_bos):
    def check_exit_new(state, lot_size, ltp, low_px, ts, tracker, force=False, force_reason="day_switch"):
        now_t = ts.time()
        for pos in list(state.positions):
            entry = pos["entry_price"]
            if force:
                mrb._close(state, lot_size, pos, force_reason, ltp, ts)
                continue

            if use_bos and not pos.get("bos_fired"):
                eh, el = pos.get("entry_swing_high"), pos.get("entry_swing_low")
                cur_h, cur_l = tracker.last_high, tracker.last_low
                lower_high = eh is not None and cur_h is not None and cur_h.ts > eh.ts and cur_h.price < eh.price
                low_broken = cur_l is not None and low_px < cur_l.price
                if lower_high or low_broken:
                    pos["bos_fired"] = True

            profit_pct = (ltp - entry) / entry
            if profit_pct >= BASE_PCT:
                steps = int((profit_pct - BASE_PCT) // step_pct)
                calc_lock = BASE_LOCK + steps * step_lock
                pos["high_lock_pct"] = max(pos["high_lock_pct"], calc_lock)

            if pos["high_lock_pct"] > 0:
                stop_price, reason = entry * (1 + pos["high_lock_pct"]), "tsl_hit"
            else:
                stop_price, reason = pos["sl"], "sl_hit"
                if use_bos and pos.get("bos_fired") and entry > stop_price:
                    stop_price, reason = entry, "breakeven_bos"

            if ltp <= stop_price:
                mrb._close(state, lot_size, pos, reason, stop_price, ts)
            elif now_t >= mrb.EOD_TIME:
                mrb._close(state, lot_size, pos, "eod", ltp, ts)
    return check_exit_new


def _snapshot(state, tracker):
    for pos in state.positions:
        if "entry_swing_high" not in pos:
            pos["entry_swing_high"] = tracker.last_high
            pos["entry_swing_low"] = tracker.last_low
            pos["bos_fired"] = False


def run_month(step_pct, step_lock, use_bos):
    spot = pd.read_parquet(SPOT_PATH)
    spot["datetime"] = pd.to_datetime(spot["datetime"])
    days = sorted(d for d in spot["datetime"].dt.date.unique() if mrb.DAY_MIN <= d <= mrb.DAY_MAX)
    check_exit_new = make_check_exit(step_pct, step_lock, use_bos)

    ce_state, pe_state = mrb.SideState(), mrb.SideState()
    ce_tracker, pe_tracker = SwingTracker(BOS_TF, BOS_PIVOT), SwingTracker(BOS_TF, BOS_PIVOT)
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
        m_htf_hist, m15_hist = bb._resample(hist, HTF), bb._resample(hist, 15)
        state.zones = mrb._prevalidate(bb._detect_bear_zones(bb._to_bars(m_htf_hist)), m15_hist)

    def refresh(state, ts, df1m, day):
        start = day - timedelta(days=mrb.HIST_WARMUP_DAYS)
        window = df1m[(df1m["datetime"].dt.date >= start) & (df1m["datetime"] < ts)]
        if len(window) < 30:
            return
        m_htf, m15 = bb._resample(window, HTF), bb._resample(window, 15)
        existing_refs = {z["ref_ts"] for z in state.zones}
        new_zones = [z for z in bb._detect_bear_zones(bb._to_bars(m_htf)) if z["ref_ts"] not in existing_refs]
        state.zones.extend(mrb._prevalidate(new_zones, m15))

    for day in days:
        day_spot = spot[spot["datetime"].dt.date == day]
        if day_spot.empty:
            continue
        o = day_spot.iloc[0]["open"]
        atm = round(o / ROUND_STEP) * ROUND_STEP
        ce_strike, pe_strike = int(atm - OFFSET), int(atm + OFFSET)
        ce_df, pe_df = get_bars(ce_strike, "CE"), get_bars(pe_strike, "PE")
        if ce_df.empty or pe_df.empty:
            continue

        if ce_state.strike != ce_strike:
            if ce_state.positions:
                last_row = ce_cache[ce_state.strike]
                last_today = last_row[last_row["datetime"].dt.date == day]
                px = last_today.iloc[0]["open"] if not last_today.empty else ce_state.positions[0]["entry_price"]
                check_exit_new(ce_state, LOT, px, px, day_spot.iloc[0]["datetime"], ce_tracker, force=True)
            warmup(ce_state, ce_df, ce_strike, "CE", day, ce_tracker)
        if pe_state.strike != pe_strike:
            if pe_state.positions:
                last_row = pe_cache[pe_state.strike]
                last_today = last_row[last_row["datetime"].dt.date == day]
                px = last_today.iloc[0]["open"] if not last_today.empty else pe_state.positions[0]["entry_price"]
                check_exit_new(pe_state, LOT, px, px, day_spot.iloc[0]["datetime"], pe_tracker, force=True)
            warmup(pe_state, pe_df, pe_strike, "PE", day, pe_tracker)

        ce_today = ce_df[ce_df["datetime"].dt.date == day].reset_index(drop=True)
        pe_today = pe_df[pe_df["datetime"].dt.date == day].reset_index(drop=True)
        ce_m5, ce_m15, ce_mhtf = bb._resample(ce_df, 5), bb._resample(ce_df, 15), bb._resample(ce_df, HTF)
        pe_m5, pe_m15, pe_mhtf = bb._resample(pe_df, 5), bb._resample(pe_df, 15), bb._resample(pe_df, HTF)
        ce_mswing, pe_mswing = bb._resample(ce_df, BOS_TF), bb._resample(pe_df, BOS_TF)
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
                while cehtf < len(ce_mhtf_today) and ce_mhtf_today.iloc[cehtf]["timestamp"] + timedelta(minutes=HTF) <= ts:
                    refresh(ce_state, ce_mhtf_today.iloc[cehtf]["timestamp"] + timedelta(minutes=HTF), ce_df, day)
                    cehtf += 1
                while ce15 < len(ce_m15_today) and ce_m15_today.iloc[ce15]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = ce_m15_today.iloc[ce15]
                    ce_state.prev15_high, ce_state.prev15_low = m15row["high"], m15row["low"]
                    mrb.on_new_15m_close(ce_state, m15row)
                    mrb.process_flip_entry_t2(ce_state, LOT, m15row, ce_m15, ce_m5, pe_state)
                    ce15 += 1
                ce_tracker.advance(ce_mswing, ts)
                check_exit_new(ce_state, LOT, bar["close"], bar["low"], ts, ce_tracker)
                mrb.check_fast_t1(ce_state, LOT, bar["high"], ts, pe_state)
                mrb.process_zones_tick(ce_state, LOT, ts, bar["low"], bar["high"], ce_m15, ce_m5, pe_state)
                _snapshot(ce_state, ce_tracker)
            if i < len(pe_today):
                bar = pe_today.iloc[i]; ts = bar["datetime"]
                while pehtf < len(pe_mhtf_today) and pe_mhtf_today.iloc[pehtf]["timestamp"] + timedelta(minutes=HTF) <= ts:
                    refresh(pe_state, pe_mhtf_today.iloc[pehtf]["timestamp"] + timedelta(minutes=HTF), pe_df, day)
                    pehtf += 1
                while pe15 < len(pe_m15_today) and pe_m15_today.iloc[pe15]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = pe_m15_today.iloc[pe15]
                    pe_state.prev15_high, pe_state.prev15_low = m15row["high"], m15row["low"]
                    mrb.on_new_15m_close(pe_state, m15row)
                    mrb.process_flip_entry_t2(pe_state, LOT, m15row, pe_m15, pe_m5, ce_state)
                    pe15 += 1
                pe_tracker.advance(pe_mswing, ts)
                check_exit_new(pe_state, LOT, bar["close"], bar["low"], ts, pe_tracker)
                mrb.check_fast_t1(pe_state, LOT, bar["high"], ts, ce_state)
                mrb.process_zones_tick(pe_state, LOT, ts, bar["low"], bar["high"], pe_m15, pe_m5, ce_state)
                _snapshot(pe_state, pe_tracker)

    return sorted(ce_state.trades + pe_state.trades, key=lambda t: t["exit_ts"])


if __name__ == "__main__":
    mrb.MONTH_DIR = LADDER_DIR
    baseline = verify.run_month_live("SENSEX", "sensexladder", SPOT_PATH, OFFSET, ROUND_STEP, LOT, HTF)
    mrb.summarize("BASELINE: live tranche 20%/12.5%, next tier at 40%, no BOS", baseline)

    for step_pct, step_lock in [(0.05, 0.05), (0.075, 0.075), (0.10, 0.10)]:
        trades_nobos = run_month(step_pct, step_lock, use_bos=False)
        mrb.summarize(f"Fine-step {step_pct*100:.1f}%/{step_lock*100:.1f}%, NO BOS", trades_nobos)

        trades_bos = run_month(step_pct, step_lock, use_bos=True)
        bos_n = sum(1 for t in trades_bos if t["reason"] == "breakeven_bos")
        mrb.summarize(f"Fine-step {step_pct*100:.1f}%/{step_lock*100:.1f}%, WITH pre-20% BOS->breakeven ({bos_n} exits)", trades_bos)
