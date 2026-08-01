"""
Width-threshold sensitivity sweep: how does the raw-zone discard cutoff
(currently 30 absolute points in bear_only_book.py's _ZONE_MAX_RAW_WIDTH_PTS)
affect trade count/quality across the full month? Tests both absolute-point
thresholds and a percentage-of-entry_line threshold (since a fixed point cutoff
doesn't scale -- 30pts is huge relative to a Rs100 option but tiny relative to
a Rs800 one). Same mechanic as d1trap_zone_mode_comparison.py (T1/T2 tranches
everywhere, flip concept, corrected merge-with-true-overlap-priority) -- only
the raw-zone discard rule varies between variants.
"""
import sys
sys.path.insert(0, ".")
import strategies.d1_trap_option.bear_only_book as bb
from strategies.v4_cascade.rolling_base import find_all_bear_zones
import pandas as pd
from datetime import timedelta

OPT_DIR = "data/d1trap_fractal_cache/aug4_options"
SPOT_PATH = "data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet"
LOT_SIZE = 65
ITM_OFFSET_PTS = 200
ATM_ROUND_STEP = 100
HIST_WARMUP_DAYS = bb._HIST_WARMUP_DAYS
EARLY_CUTOFF = bb._EARLY_SESSION_CUTOFF
ENTRY_CUTOFF = bb._ENTRY_CUTOFF
EOD_TIME = bb._EOD_TIME
SL_BUFFER_PTS = bb._SL_BUFFER_PTS
MAX_RISK_RS_PER_LOT = bb._MAX_RISK_RS_PER_LOT

# variant name -> (kind, value)
VARIANTS = {
    "abs_30pt (current)": ("abs", 30.0),
    "abs_40pt": ("abs", 40.0),
    "abs_50pt": ("abs", 50.0),
    "abs_60pt": ("abs", 60.0),
    "pct_15": ("pct", 0.15),
    "pct_20": ("pct", 0.20),
    "pct_25": ("pct", 0.25),
}

_opt_cache = {}


def load_option(strike, side):
    key = (strike, side)
    if key not in _opt_cache:
        df = pd.read_parquet(f"{OPT_DIR}/{strike}_{side}_month.parquet")
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime").reset_index(drop=True)
        _opt_cache[key] = dict(m1=df, m5=bb._resample(df, 5), m15=bb._resample(df, 15), m60=bb._resample(df, 60))
    return _opt_cache[key]


def raw_zone_dicts(bars_60m):
    out = []
    for z in find_all_bear_zones(bars_60m):
        lo, hi = min(z.entry_line, z.sweep_low), max(z.entry_line, z.sweep_low)
        out.append(dict(zone_lo=lo, zone_hi=hi, entry_line=z.entry_line, lock_ts=z.lock_ts,
                         state="WAITING", ref_bar=None, done=False, invalid=False,
                         contact_ts=None, ref_open=None, ref_close_time=None,
                         breach_ts=None, sub_lo=None, sub_hi=None))
    return out


def detect_zones(bars_60m, variant):
    kind, value = VARIANTS[variant]
    out = raw_zone_dicts(bars_60m)
    if kind == "abs":
        out = [z for z in out if (z["zone_hi"] - z["zone_lo"]) <= value]
    else:  # pct of entry_line (the zone's own reference price level)
        out = [z for z in out if z["entry_line"] > 0 and (z["zone_hi"] - z["zone_lo"]) / z["entry_line"] <= value]
    return bb._collapse_nearby_zones(out)


class SideState:
    def __init__(self, strike, side):
        self.strike, self.side = strike, side
        self.zones, self.flip_candidates = [], []
        self.positions, self.trades = [], []
        self.prev15_high, self.prev15_low = None, None


def warmup_zones(state, data, day, variant):
    start = day - timedelta(days=HIST_WARMUP_DAYS)
    hist = data["m1"][(data["m1"]["datetime"].dt.date >= start) & (data["m1"]["datetime"].dt.date < day)]
    if len(hist) < 30:
        state.zones = []
        return
    m60_hist, m15_hist = bb._resample(hist, 60), bb._resample(hist, 15)
    state.zones = bb._prevalidate_zones(detect_zones(bb._to_bars(m60_hist), variant), m15_hist)


def refresh_intraday_zones(state, ts, data, day, variant):
    start = day - timedelta(days=HIST_WARMUP_DAYS)
    window = data["m1"][(data["m1"]["datetime"].dt.date >= start) & (data["m1"]["datetime"] < ts)]
    if len(window) < 30:
        return
    m60, m15 = bb._resample(window, 60), bb._resample(window, 15)
    existing = {z["lock_ts"] for z in state.zones}

    def already_known(z):
        if z["lock_ts"] in existing:
            return True
        return any(abs(z["zone_lo"] - e["zone_lo"]) <= bb._ZONE_MERGE_THRESHOLD_PTS
                   and abs(z["zone_hi"] - e["zone_hi"]) <= bb._ZONE_MERGE_THRESHOLD_PTS
                   for e in state.zones)
    new_zones = [z for z in detect_zones(bb._to_bars(m60), variant) if not already_known(z)]
    if new_zones:
        state.zones.extend(bb._prevalidate_zones(new_zones, m15))


def open_leg(state, tranche, entry_price, sl, zone_lock_ts, use_tranche_tsl):
    sl_final = max(sl - SL_BUFFER_PTS, entry_price - MAX_RISK_RS_PER_LOT / LOT_SIZE)
    if use_tranche_tsl:
        b, bl, s, sl_ = bb._TSL_TRANCHE_BASE_PCT, bb._TSL_TRANCHE_BASE_LOCK_PCT, bb._TSL_TRANCHE_STEP_PCT, bb._TSL_TRANCHE_STEP_LOCK_PCT
    else:
        b, bl, s, sl_ = bb._TSL_BASE_PCT, bb._TSL_BASE_LOCK_PCT, bb._TSL_STEP_PCT, bb._TSL_STEP_LOCK_PCT
    state.positions.append(dict(side=state.side, strike=state.strike, entry_price=entry_price, sl=sl_final,
                                 high_lock_pct=0.0, tranche=tranche, zone_lock_ts=zone_lock_ts,
                                 tsl_base_pct=b, tsl_base_lock_pct=bl, tsl_step_pct=s, tsl_step_lock_pct=sl_))


def check_exit(state, ltp, ts):
    now_t = ts.time()
    for pos in list(state.positions):
        entry = pos["entry_price"]
        profit_pct = (ltp - entry) / entry
        if profit_pct >= pos["tsl_base_pct"]:
            steps = int((profit_pct - pos["tsl_base_pct"]) // pos["tsl_step_pct"])
            calc_lock = pos["tsl_base_lock_pct"] + steps * pos["tsl_step_lock_pct"]
            pos["high_lock_pct"] = max(pos["high_lock_pct"], calc_lock)
        stop_price = entry * (1 + pos["high_lock_pct"]) if pos["high_lock_pct"] > 0 else pos["sl"]
        if ltp <= stop_price:
            _close(state, pos, "tsl_hit" if pos["high_lock_pct"] > 0 else "sl_hit", stop_price, ts)
        elif now_t >= EOD_TIME:
            _close(state, pos, "eod", ltp, ts)


def _close(state, pos, reason, exit_price, ts):
    state.positions = [p for p in state.positions if p is not pos]
    pnl = (exit_price - pos["entry_price"]) * LOT_SIZE
    state.trades.append(dict(side=pos["side"], strike=pos["strike"], tranche=pos["tranche"],
                              entry=pos["entry_price"], exit=exit_price, reason=reason, exit_ts=ts, pnl=pnl))


def on_new_15m_close(state, m15_bar):
    for z in state.zones:
        if not z["done"] and not z["invalid"] and m15_bar.close < z["zone_lo"]:
            z["invalid"] = True
            z["invalid_ts"] = m15_bar.timestamp + timedelta(minutes=15)
            state.flip_candidates.append(dict(
                candleA_low=m15_bar.low, candleA_high=m15_bar.high, candleA_ts=m15_bar.timestamp,
                zone_lo=z["zone_lo"], zone_hi=z["zone_hi"], zone_lock_ts=z["lock_ts"],
                parent_zone=z, confirmed=False, cancelled=False, t1_taken=False,
            ))
    for fc in state.flip_candidates:
        if fc["confirmed"] or fc["cancelled"] or m15_bar.timestamp <= fc["candleA_ts"]:
            continue
        if m15_bar.low < fc["candleA_low"]:
            continue
        if fc["zone_lo"] <= m15_bar.close <= fc["zone_hi"]:
            fc["cancelled"] = True
            fc["parent_zone"]["invalid"] = False


def process_flip_entry_t2(state, m15_bar, m15, m5, flip_source):
    if any(p["tranche"] == "T2" for p in state.positions):
        return
    if not state.positions and flip_source.positions:
        return
    if not flip_source.flip_candidates:
        return
    idx = m15.index[m15["timestamp"] == m15_bar.timestamp]
    if not len(idx) or idx[0] == 0:
        return
    prev15 = m15.iloc[idx[0] - 1]
    if m15_bar.high <= prev15["high"]:
        return
    for fc in flip_source.flip_candidates:
        if fc["confirmed"] or fc["cancelled"] or m15_bar.timestamp <= fc["candleA_ts"]:
            continue
        window_5m = m5[(m5["timestamp"] >= m15_bar.timestamp) & (m5["timestamp"] < m15_bar.timestamp + timedelta(minutes=15))]
        collapse = bb._collapse_subzones(bb._to_bars(window_5m))
        if collapse is None:
            continue
        fc["confirmed"] = True
        open_leg(state, "T2", m15_bar.high, m15_bar.low, fc["zone_lock_ts"], use_tranche_tsl=True)
        return


def check_fast_t1(state, ltp, ts, flip_source):
    if any(p["side"] == state.side for p in state.positions) or flip_source.positions:
        return
    if state.prev15_high is None:
        return
    for fc in flip_source.flip_candidates:
        if fc["confirmed"] or fc["cancelled"] or fc["t1_taken"]:
            continue
        if ts <= fc["candleA_ts"] + timedelta(minutes=15):
            continue
        if ltp <= state.prev15_high:
            continue
        fc["t1_taken"] = True
        open_leg(state, "T1", ltp, state.prev15_low, fc["zone_lock_ts"], use_tranche_tsl=True)
        return


def process_zones_tick(state, last_ts, last_low, last_high, m15, m5, other_state):
    if last_ts.time() >= ENTRY_CUTOFF:
        return
    active_locks = {p["zone_lock_ts"] for p in state.positions}
    for zone in state.zones:
        if zone["done"] or zone["invalid"]:
            continue
        if (state.positions and zone["lock_ts"] not in active_locks) or (not state.positions and other_state.positions):
            continue
        if zone["state"] == "WAITING":
            if last_low <= zone["zone_hi"]:
                zone["state"] = "MONITORING"
                zone["contact_ts"] = last_ts
            continue
        if zone["state"] != "MONITORING":
            continue
        if zone["ref_open"] is None:
            if last_ts.time() < EARLY_CUTOFF:
                continue
            ref = bb.D1TrapBearOnlyBook._find_latest_closed_ref_bar(m15, last_ts)
            if ref is not None:
                zone["ref_open"], zone["ref_close_time"] = ref.timestamp, ref.timestamp + timedelta(minutes=15)
                zone["ref_high"], zone["ref_low"] = ref.high, ref.low
            continue
        if zone["breach_ts"] is None:
            if last_ts >= zone["ref_close_time"] and last_high >= zone["ref_high"]:
                zone["breach_ts"] = last_ts
                open_leg(state, "T1", zone["ref_high"], zone["ref_low"], zone["lock_ts"], use_tranche_tsl=True)
                active_locks.add(zone["lock_ts"])
                continue
            new_ref = bb.D1TrapBearOnlyBook._find_latest_closed_ref_bar(m15, last_ts)
            if new_ref is not None and new_ref.timestamp > zone["ref_open"]:
                zone["ref_open"], zone["ref_close_time"] = new_ref.timestamp, new_ref.timestamp + timedelta(minutes=15)
                zone["ref_high"], zone["ref_low"] = new_ref.high, new_ref.low
            continue
        if zone["sub_lo"] is None:
            window_5m = m5[(m5["timestamp"] >= zone["ref_open"]) & (m5["timestamp"] < zone["ref_close_time"])]
            collapse = bb._collapse_subzones(bb._to_bars(window_5m))
            if collapse is None:
                zone["done"] = True
                return
            zone["sub_lo"], zone["sub_hi"] = collapse
            threshold_pts = bb._ZONE_SIZE_THRESHOLD_PCT / 100.0 * zone["ref_high"]
            zone["arm_level"] = bb._arm_level(zone["sub_lo"], zone["sub_hi"], threshold_pts)
            zone["armed"] = False
            continue
        if not zone.get("armed"):
            if zone["sub_lo"] <= last_low <= zone["arm_level"]:
                zone["armed"] = True
            continue
        if last_high >= zone["sub_hi"]:
            if not any(p["zone_lock_ts"] == zone["lock_ts"] and p["tranche"] == "T2" for p in state.positions):
                open_leg(state, "T2", zone["sub_hi"], zone["ref_low"], zone["lock_ts"], use_tranche_tsl=True)
            zone["done"] = True
            return


def run_variant(variant, day_opens):
    all_trades = []
    for day, row in day_opens.iterrows():
        atm = round(row["open"] / ATM_ROUND_STEP) * ATM_ROUND_STEP
        ce_strike, pe_strike = int(atm - ITM_OFFSET_PTS), int(atm + ITM_OFFSET_PTS)
        try:
            ce_data, pe_data = load_option(ce_strike, "CE"), load_option(pe_strike, "PE")
        except FileNotFoundError:
            continue

        ce_state, pe_state = SideState(ce_strike, "CE"), SideState(pe_strike, "PE")
        warmup_zones(ce_state, ce_data, day, variant)
        warmup_zones(pe_state, pe_data, day, variant)

        ce_today = ce_data["m1"][ce_data["m1"]["datetime"].dt.date == day].reset_index(drop=True)
        pe_today = pe_data["m1"][pe_data["m1"]["datetime"].dt.date == day].reset_index(drop=True)
        ce_m15_today = ce_data["m15"][ce_data["m15"]["timestamp"].dt.date == day].reset_index(drop=True)
        pe_m15_today = pe_data["m15"][pe_data["m15"]["timestamp"].dt.date == day].reset_index(drop=True)
        ce_m60_today = ce_data["m60"][ce_data["m60"]["timestamp"].dt.date == day].reset_index(drop=True)
        pe_m60_today = pe_data["m60"][pe_data["m60"]["timestamp"].dt.date == day].reset_index(drop=True)

        max_len = max(len(ce_today), len(pe_today))
        ce15, pe15, ce60, pe60 = 0, 0, 0, 0
        for i in range(max_len):
            if i < len(ce_today):
                bar = ce_today.iloc[i]; ts = bar["datetime"]
                while ce60 < len(ce_m60_today) and ce_m60_today.iloc[ce60]["timestamp"] + timedelta(minutes=60) <= ts:
                    refresh_intraday_zones(ce_state, ce_m60_today.iloc[ce60]["timestamp"] + timedelta(minutes=60), ce_data, day, variant)
                    ce60 += 1
                while ce15 < len(ce_m15_today) and ce_m15_today.iloc[ce15]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = ce_m15_today.iloc[ce15]
                    ce_state.prev15_high, ce_state.prev15_low = m15row["high"], m15row["low"]
                    on_new_15m_close(ce_state, m15row)
                    process_flip_entry_t2(ce_state, m15row, ce_data["m15"], ce_data["m5"], pe_state)
                    ce15 += 1
                check_exit(ce_state, bar["close"], ts)
                check_fast_t1(ce_state, bar["high"], ts, pe_state)
                process_zones_tick(ce_state, ts, bar["low"], bar["high"], ce_data["m15"], ce_data["m5"], pe_state)
            if i < len(pe_today):
                bar = pe_today.iloc[i]; ts = bar["datetime"]
                while pe60 < len(pe_m60_today) and pe_m60_today.iloc[pe60]["timestamp"] + timedelta(minutes=60) <= ts:
                    refresh_intraday_zones(pe_state, pe_m60_today.iloc[pe60]["timestamp"] + timedelta(minutes=60), pe_data, day, variant)
                    pe60 += 1
                while pe15 < len(pe_m15_today) and pe_m15_today.iloc[pe15]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = pe_m15_today.iloc[pe15]
                    pe_state.prev15_high, pe_state.prev15_low = m15row["high"], m15row["low"]
                    on_new_15m_close(pe_state, m15row)
                    process_flip_entry_t2(pe_state, m15row, pe_data["m15"], pe_data["m5"], ce_state)
                    pe15 += 1
                check_exit(pe_state, bar["close"], ts)
                check_fast_t1(pe_state, bar["high"], ts, ce_state)
                process_zones_tick(pe_state, ts, bar["low"], bar["high"], pe_data["m15"], pe_data["m5"], ce_state)

        if ce_state.positions and not ce_today.empty:
            last = ce_today.iloc[-1]
            for pos in list(ce_state.positions):
                _close(ce_state, pos, "eod", last["close"], last["datetime"])
        if pe_state.positions and not pe_today.empty:
            last = pe_today.iloc[-1]
            for pos in list(pe_state.positions):
                _close(pe_state, pos, "eod", last["close"], last["datetime"])

        all_trades.extend(ce_state.trades)
        all_trades.extend(pe_state.trades)
    return all_trades


def stats(trades):
    n = len(trades)
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    gw, gl = sum(t["pnl"] for t in wins), abs(sum(t["pnl"] for t in losses))
    pf = gw / gl if gl > 0 else (99 if gw > 0 else 0)
    total = sum(t["pnl"] for t in trades)
    return dict(n=n, win_pct=100 * len(wins) / n if n else 0, pf=pf, total=total)


def main():
    spot = pd.read_parquet(SPOT_PATH)
    spot["datetime"] = pd.to_datetime(spot["datetime"])
    day_opens = spot.groupby(spot["datetime"].dt.date).first()

    print(f"{'Variant':<20}{'Trades':>8}{'Win%':>8}{'PF':>7}{'TotalPnL':>12}")
    print("-" * 60)
    for variant in VARIANTS:
        trades = run_variant(variant, day_opens)
        s = stats(trades)
        print(f"{variant:<20}{s['n']:>8}{s['win_pct']:>7.1f}%{s['pf']:>7.2f}{s['total']:>+12,.0f}")


if __name__ == "__main__":
    main()
