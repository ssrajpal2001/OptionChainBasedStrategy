"""
True daily-ATM-rolling 1-month backtest, corrected zone algorithm
(tight [ref.low, sellers_in.low] boundary + reclaim-gated validity +
2-candle-neighbor price+time merge), for both NIFTY and SENSEX, so the two
indices can be compared under identical strategy logic.

Each trading day gets its own ATM+/-offset CE/PE pair (matching live
`_select_strikes_for_today`); when the pair differs from the previous
day's, that side's zone/flip-candidate state is rebuilt fresh from ITS OWN
14-day-prior 1-min history (a genuinely different option instrument has no
relationship to the old one's zones). Positions open on the outgoing pair
are force-closed at that day's price before switching.
"""
import sys
sys.path.insert(0, ".")
import strategies.d1_trap_option.bear_only_book as bb
from strategies.v4_cascade.rolling_base import find_all_bear_zones
import pandas as pd
from datetime import timedelta, date

MONTH_DIR = "data/d1trap_fractal_cache/month_roll"
LOT_NIFTY, OFFSET_NIFTY, STEP_NIFTY = 65, 200, 100
LOT_SENSEX, OFFSET_SENSEX, STEP_SENSEX = 20, 500, 100
HIST_WARMUP_DAYS = bb._HIST_WARMUP_DAYS
EARLY_CUTOFF = bb._EARLY_SESSION_CUTOFF
ENTRY_CUTOFF = bb._ENTRY_CUTOFF
EOD_TIME = bb._EOD_TIME
SL_BUFFER_PTS = bb._SL_BUFFER_PTS
MAX_RISK_RS_PER_LOT = bb._MAX_RISK_RS_PER_LOT
MERGE_THRESHOLD_PTS = bb._ZONE_MERGE_THRESHOLD_PTS
MAX_REF_GAP = 2
DAY_MIN, DAY_MAX = date(2026, 6, 29), date(2026, 7, 31)


def _detect_zones_corrected(bars):
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
        near_in_time = any(abs(z["ref_idx"] - g["ref_idx"]) <= MAX_REF_GAP for g in grp)
        truly_overlaps = z["zone_lo"] <= group_hi
        if near_in_time and (truly_overlaps or z["zone_lo"] <= group_hi + MERGE_THRESHOLD_PTS):
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


def _prevalidate(zones, m15):
    for z in zones:
        later = m15[(m15["timestamp"] > z["lock_ts"]) & (m15["close"] < z["zone_lo"])]
        if not later.empty:
            z["invalid"] = True
    return zones


def load_bars(fname_prefix, strike, side):
    path = f"{MONTH_DIR}/{fname_prefix}_{strike}_{side}.parquet"
    df = pd.read_parquet(path)
    if df.empty or "datetime" not in df.columns:
        return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.sort_values("datetime").reset_index(drop=True)


class SideState:
    def __init__(self):
        self.strike = None
        self.zones, self.flip_candidates = [], []
        self.positions, self.trades = [], []
        self.prev15_high, self.prev15_low = None, None
        self.side = None


def warmup(state, df1m, strike, side, day):
    state.strike, state.side = strike, side
    state.zones, state.flip_candidates, state.positions = [], [], []
    start = day - timedelta(days=HIST_WARMUP_DAYS)
    hist = df1m[(df1m["datetime"].dt.date >= start) & (df1m["datetime"].dt.date < day)]
    m60_hist, m15_hist = bb._resample(hist, 60), bb._resample(hist, 15)
    state.zones = _prevalidate(_detect_zones_corrected(bb._to_bars(m60_hist)), m15_hist)


def refresh_intraday(state, ts, df1m, day):
    start = day - timedelta(days=HIST_WARMUP_DAYS)
    window = df1m[(df1m["datetime"].dt.date >= start) & (df1m["datetime"] < ts)]
    if len(window) < 30:
        return
    m60, m15 = bb._resample(window, 60), bb._resample(window, 15)
    existing_refs = {z["ref_ts"] for z in state.zones}
    new_zones = [z for z in _detect_zones_corrected(bb._to_bars(m60)) if z["ref_ts"] not in existing_refs]
    state.zones.extend(_prevalidate(new_zones, m15))


def open_leg(state, lot_size, tranche, entry_price, sl, zone_lock_ts, audit=None, entry_ts=None):
    sl_final = max(sl - SL_BUFFER_PTS, entry_price - MAX_RISK_RS_PER_LOT / lot_size)
    b, bl, s, sl_ = bb._TSL_TRANCHE_BASE_PCT, bb._TSL_TRANCHE_BASE_LOCK_PCT, bb._TSL_TRANCHE_STEP_PCT, bb._TSL_TRANCHE_STEP_LOCK_PCT
    state.positions.append(dict(side=state.side, strike=state.strike, entry_price=entry_price, sl=sl_final,
                                 high_lock_pct=0.0, tranche=tranche, zone_lock_ts=zone_lock_ts,
                                 tsl_base_pct=b, tsl_base_lock_pct=bl, tsl_step_pct=s, tsl_step_lock_pct=sl_,
                                 audit=audit or {}, entry_ts=entry_ts))


def check_exit(state, lot_size, ltp, ts, force=False, force_reason="day_switch"):
    now_t = ts.time()
    for pos in list(state.positions):
        entry = pos["entry_price"]
        if force:
            _close(state, lot_size, pos, force_reason, ltp, ts)
            continue
        profit_pct = (ltp - entry) / entry
        if profit_pct >= pos["tsl_base_pct"]:
            steps = int((profit_pct - pos["tsl_base_pct"]) // pos["tsl_step_pct"])
            calc_lock = pos["tsl_base_lock_pct"] + steps * pos["tsl_step_lock_pct"]
            pos["high_lock_pct"] = max(pos["high_lock_pct"], calc_lock)
        stop_price = entry * (1 + pos["high_lock_pct"]) if pos["high_lock_pct"] > 0 else pos["sl"]
        if ltp <= stop_price:
            _close(state, lot_size, pos, "tsl_hit" if pos["high_lock_pct"] > 0 else "sl_hit", stop_price, ts)
        elif now_t >= EOD_TIME:
            _close(state, lot_size, pos, "eod", ltp, ts)


def _close(state, lot_size, pos, reason, exit_price, ts):
    state.positions = [p for p in state.positions if p is not pos]
    pnl = (exit_price - pos["entry_price"]) * lot_size
    state.trades.append(dict(side=pos["side"], strike=pos["strike"], tranche=pos["tranche"],
                              entry=pos["entry_price"], entry_ts=pos.get("entry_ts"),
                              exit=exit_price, reason=reason, exit_ts=ts, pnl=pnl))


def _create_flip_candidate(state, zone, candleA):
    state.flip_candidates.append(dict(candleA_low=candleA.low, candleA_high=candleA.high, candleA_ts=candleA.timestamp,
                                       zone_lo=zone["zone_lo"], zone_hi=zone["zone_hi"], zone_lock_ts=zone["ref_ts"],
                                       confirmed=False, cancelled=False, t1_taken=False))


def on_new_15m_close(state, m15_bar):
    for z in state.zones:
        if not z["done"] and not z["invalid"] and m15_bar.close < z["zone_lo"]:
            z["invalid"] = True
            _create_flip_candidate(state, z, m15_bar)
    for fc in state.flip_candidates:
        if fc["confirmed"] or fc["cancelled"] or m15_bar.timestamp <= fc["candleA_ts"]:
            continue
        if m15_bar.low < fc["candleA_low"]:
            continue
        if fc["zone_lo"] <= m15_bar.close <= fc["zone_hi"]:
            fc["cancelled"] = True


def process_flip_entry_t2(state, lot_size, m15_bar, m15, m5, flip_source):
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
        open_leg(state, lot_size, "T2", m15_bar.high, m15_bar.low, fc["zone_lock_ts"],
                 entry_ts=m15_bar.timestamp + timedelta(minutes=15))
        return


def check_fast_t1(state, lot_size, ltp, ts, flip_source):
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
        open_leg(state, lot_size, "T1", ltp, state.prev15_low, fc["zone_lock_ts"], entry_ts=ts)
        return


def process_zones_tick(state, lot_size, last_ts, last_low, last_high, m15, m5, other_state):
    if last_ts.time() >= ENTRY_CUTOFF:
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
            if last_ts.time() < EARLY_CUTOFF:
                continue
            ref = bb.D1TrapBearOnlyBook._find_latest_closed_ref_bar(m15, last_ts)
            if ref is not None:
                zone["ref_open"], zone["ref_close_time"] = ref.timestamp, ref.timestamp + timedelta(minutes=15)
                zone["ref15_high"], zone["ref15_low"] = ref.high, ref.low
            continue
        if zone["breach_ts"] is None:
            if last_ts >= zone["ref_close_time"] and last_high >= zone["ref15_high"]:
                zone["breach_ts"] = last_ts
                open_leg(state, lot_size, "T1", zone["ref15_high"], zone["ref15_low"], zone["ref_ts"], entry_ts=last_ts)
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
                open_leg(state, lot_size, "T2", zone["sub_hi"], zone["ref15_low"], zone["ref_ts"], entry_ts=last_ts)
            zone["done"] = True
            return


def run_month(underlying, fname_prefix, spot_path, offset, round_step, lot_size):
    spot = pd.read_parquet(spot_path)
    spot["datetime"] = pd.to_datetime(spot["datetime"])
    days = sorted(d for d in spot["datetime"].dt.date.unique() if DAY_MIN <= d <= DAY_MAX)

    ce_state, pe_state = SideState(), SideState()
    ce_cache, pe_cache = {}, {}

    def get_bars(strike, side):
        cache = ce_cache if side == "CE" else pe_cache
        if strike not in cache:
            cache[strike] = load_bars(fname_prefix, strike, side)
        return cache[strike]

    print(f"\n{'='*100}\n{underlying} -- {len(days)} trading days\n{'='*100}")
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
            print(f"  {day}: SKIP (missing data for CE{ce_strike}/PE{pe_strike})")
            continue

        if ce_state.strike != ce_strike:
            if ce_state.positions:
                last_row = ce_cache[ce_state.strike]
                last_today = last_row[last_row["datetime"].dt.date == day]
                px = last_today.iloc[0]["open"] if not last_today.empty else ce_state.positions[0]["entry_price"]
                check_exit(ce_state, lot_size, px, day_spot.iloc[0]["datetime"], force=True)
            warmup(ce_state, ce_df, ce_strike, "CE", day)
        if pe_state.strike != pe_strike:
            if pe_state.positions:
                last_row = pe_cache[pe_state.strike]
                last_today = last_row[last_row["datetime"].dt.date == day]
                px = last_today.iloc[0]["open"] if not last_today.empty else pe_state.positions[0]["entry_price"]
                check_exit(pe_state, lot_size, px, day_spot.iloc[0]["datetime"], force=True)
            warmup(pe_state, pe_df, pe_strike, "PE", day)

        ce_today = ce_df[ce_df["datetime"].dt.date == day].reset_index(drop=True)
        pe_today = pe_df[pe_df["datetime"].dt.date == day].reset_index(drop=True)
        ce_m5, ce_m15, ce_m60 = bb._resample(ce_df, 5), bb._resample(ce_df, 15), bb._resample(ce_df, 60)
        pe_m5, pe_m15, pe_m60 = bb._resample(pe_df, 5), bb._resample(pe_df, 15), bb._resample(pe_df, 60)
        ce_m15_today = ce_m15[ce_m15["timestamp"].dt.date == day].reset_index(drop=True)
        pe_m15_today = pe_m15[pe_m15["timestamp"].dt.date == day].reset_index(drop=True)
        ce_m60_today = ce_m60[ce_m60["timestamp"].dt.date == day].reset_index(drop=True)
        pe_m60_today = pe_m60[pe_m60["timestamp"].dt.date == day].reset_index(drop=True)

        if ce_today.empty and pe_today.empty:
            continue

        max_len = max(len(ce_today), len(pe_today))
        ce15 = pe15 = ce60 = pe60 = 0
        for i in range(max_len):
            if i < len(ce_today):
                bar = ce_today.iloc[i]; ts = bar["datetime"]
                while ce60 < len(ce_m60_today) and ce_m60_today.iloc[ce60]["timestamp"] + timedelta(minutes=60) <= ts:
                    refresh_intraday(ce_state, ce_m60_today.iloc[ce60]["timestamp"] + timedelta(minutes=60), ce_df, day)
                    ce60 += 1
                while ce15 < len(ce_m15_today) and ce_m15_today.iloc[ce15]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = ce_m15_today.iloc[ce15]
                    ce_state.prev15_high, ce_state.prev15_low = m15row["high"], m15row["low"]
                    on_new_15m_close(ce_state, m15row)
                    process_flip_entry_t2(ce_state, lot_size, m15row, ce_m15, ce_m5, pe_state)
                    ce15 += 1
                check_exit(ce_state, lot_size, bar["close"], ts)
                check_fast_t1(ce_state, lot_size, bar["high"], ts, pe_state)
                process_zones_tick(ce_state, lot_size, ts, bar["low"], bar["high"], ce_m15, ce_m5, pe_state)
            if i < len(pe_today):
                bar = pe_today.iloc[i]; ts = bar["datetime"]
                while pe60 < len(pe_m60_today) and pe_m60_today.iloc[pe60]["timestamp"] + timedelta(minutes=60) <= ts:
                    refresh_intraday(pe_state, pe_m60_today.iloc[pe60]["timestamp"] + timedelta(minutes=60), pe_df, day)
                    pe60 += 1
                while pe15 < len(pe_m15_today) and pe_m15_today.iloc[pe15]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = pe_m15_today.iloc[pe15]
                    pe_state.prev15_high, pe_state.prev15_low = m15row["high"], m15row["low"]
                    on_new_15m_close(pe_state, m15row)
                    process_flip_entry_t2(pe_state, lot_size, m15row, pe_m15, pe_m5, ce_state)
                    pe15 += 1
                check_exit(pe_state, lot_size, bar["close"], ts)
                check_fast_t1(pe_state, lot_size, bar["high"], ts, ce_state)
                process_zones_tick(pe_state, lot_size, ts, bar["low"], bar["high"], pe_m15, pe_m5, ce_state)

        day_trades = [t for t in ce_state.trades + pe_state.trades if t["exit_ts"].date() == day]
        if day_trades:
            print(f"  {day}  ATM={atm} CE={ce_strike} PE={pe_strike}  trades={len(day_trades)}  "
                  f"pnl={sum(t['pnl'] for t in day_trades):+,.0f}")

    all_trades = sorted(ce_state.trades + pe_state.trades, key=lambda t: t["exit_ts"])
    return all_trades


def summarize(name, trades):
    n = len(trades)
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    gross_win = sum(t["pnl"] for t in wins)
    gross_loss = abs(sum(t["pnl"] for t in losses))
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")
    total = sum(t["pnl"] for t in trades)
    win_pct = 100 * len(wins) / n if n else 0
    print(f"\n{name}: n={n}  win%={win_pct:.1f}  PF={pf:.2f}  gross_win={gross_win:+,.0f}  gross_loss=-{gross_loss:,.0f}  NET={total:+,.0f}")
    return dict(n=n, win_pct=win_pct, pf=pf, total=total)


if __name__ == "__main__":
    nifty_trades = run_month("NIFTY", "nifty", "data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet",
                              OFFSET_NIFTY, STEP_NIFTY, LOT_NIFTY)
    sensex_trades = run_month("SENSEX", "sensex", "data/d1trap_fractal_cache/sensex_1m_fullmonth_spot.parquet",
                               OFFSET_SENSEX, STEP_SENSEX, LOT_SENSEX)

    print(f"\n{'='*100}\nMONTH SUMMARY (2026-06-29 .. 2026-07-31)\n{'='*100}")
    n_stats = summarize("NIFTY ", nifty_trades)
    s_stats = summarize("SENSEX", sensex_trades)

    print(f"\n--- NIFTY trade list ---")
    for t in nifty_trades:
        print(f"  {t['exit_ts'].date()}  {t['side']}{t['strike']} [{t['tranche']}]  entry={t['entry']:.2f}  exit={t['exit']:.2f}  "
              f"({t['reason']})  pnl={t['pnl']:+,.0f}")
    print(f"\n--- SENSEX trade list ---")
    for t in sensex_trades:
        print(f"  {t['exit_ts'].date()}  {t['side']}{t['strike']} [{t['tranche']}]  entry={t['entry']:.2f}  exit={t['exit']:.2f}  "
              f"({t['reason']})  pnl={t['pnl']:+,.0f}")
