"""
scripts/d1trap_breakeven_bos_test.py -- test breakeven-on-BOS: once an open
position's OWN option premium prints a break of structure (a new confirmed
swing high that's LOWER than the prior one, or price breaking below the
most recent confirmed swing low), tighten the SL to breakeven (entry price)
-- never loosen an existing tighter stop (TSL lock always wins once active).
Unlike a full exit-on-BOS or a tighter %-TSL, this never caps upside: the
trade stays open and can still ride into the normal TSL ladder.

Grid: swing timeframe (1m/3m/5m) x pivot depth (2/3/5 bars -> 5/7/11-bar
fractal), run against the SAME live day-loop as d1trap_verify_live_defaults
/ d1trap_spot_bias_test (today's validated config: per-index offset/HTF/
zone-boundary), with the spot bias filter applied post-hoc same as before.
Reuses strategies.fvg.detector.find_swing_points -- no reinvention.
"""
import sys
sys.path.insert(0, ".")
from datetime import timedelta, date
import pandas as pd
import strategies.d1_trap_option.bear_only_book as bb
import scripts.d1trap_month_rolling_backtest as mrb
import scripts.d1trap_spot_bias_test as sbt
from strategies.fvg.detector import find_swing_points

LADDER_DIR = "data/d1trap_fractal_cache/strike_ladder"

CONFIGS = {
    "NIFTY":  dict(fname_prefix="niftyladder", offset=150, round_step=100, lot=65, htf=60,
                   spot_path="data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet"),
    "SENSEX": dict(fname_prefix="sensexladder", offset=300, round_step=100, lot=20, htf=15,
                   spot_path="data/d1trap_fractal_cache/sensex_1m_fullmonth_spot.parquet"),
}

TF_GRID = [1, 3, 5]
PIVOT_GRID = [2, 3, 5]


class _Swing:
    __slots__ = ("price", "ts")
    def __init__(self, price, ts):
        self.price, self.ts = price, ts


class SwingTracker:
    """Rolling swing-point tracker on top of a resampled bar stream, shared
    across a strike's whole life (reset when the traded strike changes)."""
    def __init__(self, tf_minutes, pivot):
        self.tf = tf_minutes
        self.pivot = pivot
        self.bars = []
        self.last_high = None
        self.last_low = None
        self._ridx = 0

    def reset(self):
        self.bars, self.last_high, self.last_low, self._ridx = [], None, None, 0

    def advance(self, resampled_today_or_full, ts):
        while self._ridx < len(resampled_today_or_full) and \
                resampled_today_or_full.iloc[self._ridx]["timestamp"] + timedelta(minutes=self.tf) <= ts:
            row = resampled_today_or_full.iloc[self._ridx]
            self.bars.append(bb._Bar(row["timestamp"], row["open"], row["high"], row["low"], row["close"]))
            self._ridx += 1
            if len(self.bars) >= 2 * self.pivot + 1:
                window = self.bars[-(10 * self.pivot + 5):]
                swings = find_swing_points(window, pivot=self.pivot)
                highs = [s for s in swings if s.kind == "HIGH"]
                lows = [s for s in swings if s.kind == "LOW"]
                if highs:
                    s = highs[-1]
                    self.last_high = _Swing(price=s.price, ts=window[s.index].timestamp)
                if lows:
                    s = lows[-1]
                    self.last_low = _Swing(price=s.price, ts=window[s.index].timestamp)


def check_exit_be(state, lot_size, ltp, low_px, ts, tracker, force=False, force_reason="day_switch"):
    now_t = ts.time()
    for pos in list(state.positions):
        entry = pos["entry_price"]
        if force:
            mrb._close(state, lot_size, pos, force_reason, ltp, ts)
            continue

        # BOS check: fires once, then sticks for this position's life.
        if not pos.get("bos_fired"):
            eh, el = pos.get("entry_swing_high"), pos.get("entry_swing_low")
            cur_h, cur_l = tracker.last_high, tracker.last_low
            lower_high = eh is not None and cur_h is not None and cur_h.ts > eh.ts and cur_h.price < eh.price
            low_broken = cur_l is not None and low_px < cur_l.price
            if lower_high or low_broken:
                pos["bos_fired"] = True

        profit_pct = (ltp - entry) / entry
        if profit_pct >= pos["tsl_base_pct"]:
            steps = int((profit_pct - pos["tsl_base_pct"]) // pos["tsl_step_pct"])
            calc_lock = pos["tsl_base_lock_pct"] + steps * pos["tsl_step_lock_pct"]
            pos["high_lock_pct"] = max(pos["high_lock_pct"], calc_lock)

        if pos["high_lock_pct"] > 0:
            stop_price, reason = entry * (1 + pos["high_lock_pct"]), "tsl_hit"
        else:
            stop_price, reason = pos["sl"], "sl_hit"
            if pos.get("bos_fired") and entry > stop_price:
                stop_price, reason = entry, "breakeven_bos"

        if ltp <= stop_price:
            mrb._close(state, lot_size, pos, reason, stop_price, ts)
        elif now_t >= mrb.EOD_TIME:
            mrb._close(state, lot_size, pos, "eod", ltp, ts)


def _snapshot_new_positions(state, tracker):
    for pos in state.positions:
        if "entry_swing_high" not in pos:
            pos["entry_swing_high"] = tracker.last_high
            pos["entry_swing_low"] = tracker.last_low
            pos["bos_fired"] = False


def run_month_with_be(underlying, fname_prefix, spot_path, offset, round_step, lot_size, htf_minutes, tf, pivot):
    spot = pd.read_parquet(spot_path)
    spot["datetime"] = pd.to_datetime(spot["datetime"])
    days = sorted(d for d in spot["datetime"].dt.date.unique() if mrb.DAY_MIN <= d <= mrb.DAY_MAX)

    ce_state, pe_state = mrb.SideState(), mrb.SideState()
    ce_tracker, pe_tracker = SwingTracker(tf, pivot), SwingTracker(tf, pivot)
    ce_cache, pe_cache = {}, {}

    def get_bars(strike, side):
        cache = ce_cache if side == "CE" else pe_cache
        if strike not in cache:
            cache[strike] = mrb.load_bars(fname_prefix, strike, side)
        return cache[strike]

    def warmup_live(state, df1m, strike, side, day, tracker):
        state.strike, state.side = strike, side
        state.zones, state.flip_candidates, state.positions = [], [], []
        tracker.reset()
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
                check_exit_be(ce_state, lot_size, px, px, day_spot.iloc[0]["datetime"], ce_tracker, force=True)
            warmup_live(ce_state, ce_df, ce_strike, "CE", day, ce_tracker)
        if pe_state.strike != pe_strike:
            if pe_state.positions:
                last_row = pe_cache[pe_state.strike]
                last_today = last_row[last_row["datetime"].dt.date == day]
                px = last_today.iloc[0]["open"] if not last_today.empty else pe_state.positions[0]["entry_price"]
                check_exit_be(pe_state, lot_size, px, px, day_spot.iloc[0]["datetime"], pe_tracker, force=True)
            warmup_live(pe_state, pe_df, pe_strike, "PE", day, pe_tracker)

        ce_today = ce_df[ce_df["datetime"].dt.date == day].reset_index(drop=True)
        pe_today = pe_df[pe_df["datetime"].dt.date == day].reset_index(drop=True)
        ce_m5, ce_m15, ce_mhtf = bb._resample(ce_df, 5), bb._resample(ce_df, 15), bb._resample(ce_df, htf_minutes)
        pe_m5, pe_m15, pe_mhtf = bb._resample(pe_df, 5), bb._resample(pe_df, 15), bb._resample(pe_df, htf_minutes)
        ce_mswing, pe_mswing = bb._resample(ce_df, tf), bb._resample(pe_df, tf)
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
                ce_tracker.advance(ce_mswing, ts)
                check_exit_be(ce_state, lot_size, bar["close"], bar["low"], ts, ce_tracker)
                mrb.check_fast_t1(ce_state, lot_size, bar["high"], ts, pe_state)
                mrb.process_zones_tick(ce_state, lot_size, ts, bar["low"], bar["high"], ce_m15, ce_m5, pe_state)
                _snapshot_new_positions(ce_state, ce_tracker)
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
                pe_tracker.advance(pe_mswing, ts)
                check_exit_be(pe_state, lot_size, bar["close"], bar["low"], ts, pe_tracker)
                mrb.check_fast_t1(pe_state, lot_size, bar["high"], ts, ce_state)
                mrb.process_zones_tick(pe_state, lot_size, ts, bar["low"], bar["high"], pe_m15, pe_m5, ce_state)
                _snapshot_new_positions(pe_state, pe_tracker)

    return sorted(ce_state.trades + pe_state.trades, key=lambda t: t["exit_ts"])


if __name__ == "__main__":
    mrb.MONTH_DIR = LADDER_DIR
    for underlying, cfg in CONFIGS.items():
        print(f"\n{'#'*100}\n{underlying}\n{'#'*100}")
        spot_df = pd.read_parquet(cfg["spot_path"])
        spot_df["datetime"] = pd.to_datetime(spot_df["datetime"])
        bias_by_day = sbt.daily_bias_series(spot_df)

        import scripts.d1trap_verify_live_defaults as verify
        baseline = verify.run_month_live(underlying, cfg["fname_prefix"], cfg["spot_path"],
                                          cfg["offset"], cfg["round_step"], cfg["lot"], cfg["htf"])
        baseline_biased = sbt.apply_bias_filter(baseline, bias_by_day)
        mrb.summarize(f"{underlying} baseline (current live: no breakeven-BOS)", baseline_biased)

        for tf in TF_GRID:
            for pivot in PIVOT_GRID:
                trades = run_month_with_be(underlying, cfg["fname_prefix"], cfg["spot_path"], cfg["offset"],
                                            cfg["round_step"], cfg["lot"], cfg["htf"], tf, pivot)
                trades_biased = sbt.apply_bias_filter(trades, bias_by_day)
                bos_n = sum(1 for t in trades_biased if t["reason"] == "breakeven_bos")
                mrb.summarize(f"{underlying} BE-BOS swing_tf={tf}m pivot={pivot} ({bos_n} exited via breakeven)", trades_biased)
