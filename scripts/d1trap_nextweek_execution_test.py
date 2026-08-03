"""
scripts/d1trap_nextweek_execution_test.py -- cross-contract experiment:
scan zones/entries on THIS-WEEK's contract (2026-08-04, the live default),
same mechanic as today (60m HTF, ref.close boundary, 3-ITM scanner
strikes -- already the live default) -- but EXECUTE the actual trade on the
NEXT-WEEK contract (2026-08-11) instead, at ATM or 1-ITM (two variants),
computed from that day's own spot open. Window: 07-23..07-31 (2026), real
premium on both contracts, no synthetic data.

Mechanic:
  - Zone construction / contact / MONITORING / ref-candle / breach (T1) /
    5m subzone-arm-swing-breach (T2) / flip concept -- ALL unchanged, still
    driven entirely by the scanner (this-week) contract's own 1m bars.
  - The moment a signal fires (T1 or T2), the ACTUAL entry price is looked
    up on the EXECUTION (next-week) contract at that same timestamp, not
    the scanner contract's own price.
  - SL: the scanner's structural SL (ref_low) is a price level in the
    SCANNER contract's premium units -- not directly transferable to a
    different contract's premium scale. Translated as a PERCENTAGE move
    (sl_pct = (ref_low-ref_high)/ref_high on the scanner side) and applied
    to the execution leg's own entry price, same shape as the existing
    hard-cap-vs-buffer max() the live book already does.
  - TSL is already percentage-based off entry -- transfers natively, no
    translation needed.
  - Exit price checks (SL/TSL/EOD) all read the EXECUTION contract's own
    live price at each scanner-clock tick (forward-filled if a given
    minute is missing on the execution side).

Reuses bb._detect_bear_zones / _collapse_subzones / _arm_level /
_resample / _to_bars directly -- no re-implementation of the zone engine.
"""
import sys
sys.path.insert(0, ".")
from datetime import timedelta, date

import pandas as pd
import strategies.d1_trap_option.bear_only_book as bb
import scripts.d1trap_month_rolling_backtest as mrb

LADDER_DIR = "data/d1trap_fractal_cache/strike_ladder"
EXEC_DIR = "data/d1trap_fractal_cache/nextweek_exec"
SPOT_PATH = "data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet"
LOT_SIZE = 65
SCANNER_ITM_OFFSET = 150   # matches live 3-ITM default
DAY_MIN, DAY_MAX = date(2026, 7, 23), date(2026, 7, 31)
MAX_RISK_RS_PER_LOT = bb._MAX_RISK_RS_PER_LOT
SL_BUFFER_PTS = bb._SL_BUFFER_PTS


def load_scanner(strike, side):
    df = pd.read_parquet(f"{LADDER_DIR}/niftyladder_{strike}_{side}.parquet")
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.sort_values("datetime").reset_index(drop=True)


def load_exec(strike, side):
    path = f"{EXEC_DIR}/nextweek_{strike}_{side}.parquet"
    df = pd.read_parquet(path)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.sort_values("datetime").reset_index(drop=True)


class SideState:
    def __init__(self):
        self.strike = None
        self.zones, self.flip_candidates = [], []
        self.positions, self.trades = [], []
        self.prev15_high, self.prev15_low = None, None
        self.side = None


def exec_price_at(exec_series_map, side, ts):
    """Nearest available execution price at/just-before ts (forward-fill)."""
    df = exec_series_map.get(side)
    if df is None or df.empty:
        return None
    sub = df[df["datetime"] <= ts]
    if sub.empty:
        return None
    return float(sub.iloc[-1]["close"])


def open_leg(state, tranche, scanner_entry, scanner_sl, zone_lock_ts, exec_entry_price, entry_ts=None):
    sl_pct = (scanner_sl - SL_BUFFER_PTS - scanner_entry) / scanner_entry
    sl_buffered_exec = exec_entry_price * (1 + sl_pct)
    hard_sl_exec = exec_entry_price - MAX_RISK_RS_PER_LOT / LOT_SIZE
    sl_final = max(sl_buffered_exec, hard_sl_exec)
    b, bl, s, sl_ = bb._TSL_TRANCHE_BASE_PCT, bb._TSL_TRANCHE_BASE_LOCK_PCT, bb._TSL_TRANCHE_STEP_PCT, bb._TSL_TRANCHE_STEP_LOCK_PCT
    state.positions.append(dict(side=state.side, strike=state.strike, entry_price=exec_entry_price, sl=sl_final,
                                 high_lock_pct=0.0, tranche=tranche, zone_lock_ts=zone_lock_ts,
                                 tsl_base_pct=b, tsl_base_lock_pct=bl, tsl_step_pct=s, tsl_step_lock_pct=sl_,
                                 entry_ts=entry_ts))


def check_exit(state, exec_ltp, ts, force=False, force_reason="eod"):
    now_t = ts.time()
    for pos in list(state.positions):
        entry = pos["entry_price"]
        if force:
            _close(state, pos, force_reason, exec_ltp, ts)
            continue
        profit_pct = (exec_ltp - entry) / entry
        if profit_pct >= pos["tsl_base_pct"]:
            steps = int((profit_pct - pos["tsl_base_pct"]) // pos["tsl_step_pct"])
            calc_lock = pos["tsl_base_lock_pct"] + steps * pos["tsl_step_lock_pct"]
            pos["high_lock_pct"] = max(pos["high_lock_pct"], calc_lock)
        stop_price = entry * (1 + pos["high_lock_pct"]) if pos["high_lock_pct"] > 0 else pos["sl"]
        if exec_ltp <= stop_price:
            _close(state, pos, "tsl_hit" if pos["high_lock_pct"] > 0 else "sl_hit", stop_price, ts)
        elif now_t >= mrb.EOD_TIME:
            _close(state, pos, "eod", exec_ltp, ts)


def _close(state, pos, reason, exit_price, ts):
    state.positions = [p for p in state.positions if p is not pos]
    pnl = (exit_price - pos["entry_price"]) * LOT_SIZE
    state.trades.append(dict(side=pos["side"], strike=pos["strike"], tranche=pos["tranche"],
                              entry=pos["entry_price"], entry_ts=pos.get("entry_ts"),
                              exit=exit_price, reason=reason, exit_ts=ts, pnl=pnl))


def run_variant(exec_strike_fn, label):
    """exec_strike_fn(day, side) -> execution strike for that day/side (ATM or 1-ITM)."""
    spot = pd.read_parquet(SPOT_PATH)
    spot["datetime"] = pd.to_datetime(spot["datetime"])
    days = sorted(d for d in spot["datetime"].dt.date.unique() if DAY_MIN <= d <= DAY_MAX)

    ce_state, pe_state = SideState(), SideState()
    scanner_cache, exec_cache = {}, {}

    def get_scanner(strike, side):
        if (strike, side) not in scanner_cache:
            scanner_cache[(strike, side)] = load_scanner(strike, side)
        return scanner_cache[(strike, side)]

    def get_exec(strike, side):
        if (strike, side) not in exec_cache:
            exec_cache[(strike, side)] = load_exec(strike, side)
        return exec_cache[(strike, side)]

    def warmup(state, df1m, strike, side, day):
        state.strike, state.side = strike, side
        state.zones, state.flip_candidates, state.positions = [], [], []
        start = day - timedelta(days=mrb.HIST_WARMUP_DAYS)
        hist = df1m[(df1m["datetime"].dt.date >= start) & (df1m["datetime"].dt.date < day)]
        m60_hist, m15_hist = bb._resample(hist, 60), bb._resample(hist, 15)
        state.zones = mrb._prevalidate(bb._detect_bear_zones(bb._to_bars(m60_hist)), m15_hist)

    def refresh_intraday(state, ts, df1m, day):
        start = day - timedelta(days=mrb.HIST_WARMUP_DAYS)
        window = df1m[(df1m["datetime"].dt.date >= start) & (df1m["datetime"] < ts)]
        if len(window) < 30:
            return
        m60, m15 = bb._resample(window, 60), bb._resample(window, 15)
        existing_refs = {z["ref_ts"] for z in state.zones}
        new_zones = [z for z in bb._detect_bear_zones(bb._to_bars(m60)) if z["ref_ts"] not in existing_refs]
        state.zones.extend(mrb._prevalidate(new_zones, m15))

    def exec_map_for_day(day):
        return {
            "CE": get_exec(exec_strike_fn(day, "CE"), "CE"),
            "PE": get_exec(exec_strike_fn(day, "PE"), "PE"),
        }

    for day in days:
        day_spot = spot[spot["datetime"].dt.date == day]
        if day_spot.empty:
            continue
        o = day_spot.iloc[0]["open"]
        atm = round(o / 100) * 100
        ce_strike, pe_strike = int(atm - SCANNER_ITM_OFFSET), int(atm + SCANNER_ITM_OFFSET)

        ce_df = get_scanner(ce_strike, "CE")
        pe_df = get_scanner(pe_strike, "PE")
        if ce_df.empty or pe_df.empty:
            continue
        exec_today = exec_map_for_day(day)

        if ce_state.strike != ce_strike:
            if ce_state.positions:
                px = exec_price_at(exec_today, "CE", day_spot.iloc[0]["datetime"]) or ce_state.positions[0]["entry_price"]
                check_exit(ce_state, px, day_spot.iloc[0]["datetime"], force=True, force_reason="day_switch")
            warmup(ce_state, ce_df, ce_strike, "CE", day)
        if pe_state.strike != pe_strike:
            if pe_state.positions:
                px = exec_price_at(exec_today, "PE", day_spot.iloc[0]["datetime"]) or pe_state.positions[0]["entry_price"]
                check_exit(pe_state, px, day_spot.iloc[0]["datetime"], force=True, force_reason="day_switch")
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
                    mrb.on_new_15m_close(ce_state, m15row)
                    _process_flip_entry_t2(ce_state, pe_state, m15row, ce_m15, ce_m5, exec_today)
                    ce15 += 1
                exec_ltp = exec_price_at(exec_today, "CE", ts)
                if exec_ltp is not None:
                    check_exit(ce_state, exec_ltp, ts)
                _check_fast_t1(ce_state, pe_state, bar["high"], ts, exec_today)
                _process_zones_tick(ce_state, ts, bar["low"], bar["high"], ce_m15, ce_m5, pe_state, exec_today)
            if i < len(pe_today):
                bar = pe_today.iloc[i]; ts = bar["datetime"]
                while pe60 < len(pe_m60_today) and pe_m60_today.iloc[pe60]["timestamp"] + timedelta(minutes=60) <= ts:
                    refresh_intraday(pe_state, pe_m60_today.iloc[pe60]["timestamp"] + timedelta(minutes=60), pe_df, day)
                    pe60 += 1
                while pe15 < len(pe_m15_today) and pe_m15_today.iloc[pe15]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = pe_m15_today.iloc[pe15]
                    pe_state.prev15_high, pe_state.prev15_low = m15row["high"], m15row["low"]
                    mrb.on_new_15m_close(pe_state, m15row)
                    _process_flip_entry_t2(pe_state, ce_state, m15row, pe_m15, pe_m5, exec_today)
                    pe15 += 1
                exec_ltp = exec_price_at(exec_today, "PE", ts)
                if exec_ltp is not None:
                    check_exit(pe_state, exec_ltp, ts)
                _check_fast_t1(pe_state, ce_state, bar["high"], ts, exec_today)
                _process_zones_tick(pe_state, ts, bar["low"], bar["high"], pe_m15, pe_m5, ce_state, exec_today)

    all_trades = sorted(ce_state.trades + pe_state.trades, key=lambda t: t["exit_ts"])
    return all_trades


def _process_flip_entry_t2(state, flip_source, m15_bar, m15, m5, exec_today):
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
        entry_ts = m15_bar.timestamp + timedelta(minutes=15)
        exec_entry = exec_price_at(exec_today, state.side, entry_ts)
        if exec_entry is None:
            return
        open_leg(state, "T2", m15_bar.high, m15_bar.low, fc["zone_lock_ts"], exec_entry, entry_ts=entry_ts)
        return


def _check_fast_t1(state, flip_source, ltp_scanner, ts, exec_today):
    if any(p["side"] == state.side for p in state.positions) or flip_source.positions:
        return
    if state.prev15_high is None:
        return
    for fc in flip_source.flip_candidates:
        if fc["confirmed"] or fc["cancelled"] or fc["t1_taken"]:
            continue
        if ts <= fc["candleA_ts"] + timedelta(minutes=15):
            continue
        if ltp_scanner <= state.prev15_high:
            continue
        fc["t1_taken"] = True
        exec_entry = exec_price_at(exec_today, state.side, ts)
        if exec_entry is None:
            return
        open_leg(state, "T1", ltp_scanner, state.prev15_low, fc["zone_lock_ts"], exec_entry, entry_ts=ts)
        return


def _process_zones_tick(state, last_ts, last_low, last_high, m15, m5, other_state, exec_today):
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
                exec_entry = exec_price_at(exec_today, state.side, last_ts)
                if exec_entry is not None:
                    open_leg(state, "T1", zone["ref15_high"], zone["ref15_low"], zone["ref_ts"], exec_entry, entry_ts=last_ts)
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
                exec_entry = exec_price_at(exec_today, state.side, last_ts)
                if exec_entry is not None:
                    open_leg(state, "T2", zone["sub_hi"], zone["ref15_low"], zone["ref_ts"], exec_entry, entry_ts=last_ts)
            zone["done"] = True
            return


def atm_strike_fn(day, side):
    spot = pd.read_parquet(SPOT_PATH)
    spot["datetime"] = pd.to_datetime(spot["datetime"])
    o = spot[spot["datetime"].dt.date == day].iloc[0]["open"]
    return int(round(o / 100) * 100)


def one_itm_strike_fn(day, side):
    atm = atm_strike_fn(day, side)
    return atm - 50 if side == "CE" else atm + 50


if __name__ == "__main__":
    print("Baseline: same-week execution (scanner contract itself), 07-23..07-31 window")
    import scripts.d1trap_verify_live_defaults as verify
    mrb.MONTH_DIR = LADDER_DIR
    mrb.DAY_MIN, mrb.DAY_MAX = DAY_MIN, DAY_MAX
    trades_baseline = verify.run_month_live("NIFTY", "niftyladder", SPOT_PATH, SCANNER_ITM_OFFSET, 100, LOT_SIZE, 60)
    mrb.summarize("Same-week execution (baseline)", trades_baseline)

    print("\nNext-week execution @ ATM")
    trades_atm = run_variant(atm_strike_fn, "next-week ATM")
    mrb.summarize("Next-week execution @ ATM", trades_atm)

    print("\nNext-week execution @ 1-ITM")
    trades_1itm = run_variant(one_itm_strike_fn, "next-week 1-ITM")
    mrb.summarize("Next-week execution @ 1-ITM", trades_1itm)

    print(f"\n{'='*100}\nDETAIL -- Next-week @ ATM\n{'='*100}")
    for t in trades_atm:
        print(f"  {t['side']}{t['strike']} [{t['tranche']}] entry={t['entry']:.2f}@{t.get('entry_ts')} "
              f"exit={t['exit']:.2f}@{t['exit_ts']} ({t['reason']}) pnl={t['pnl']:+,.0f}")

    print(f"\n{'='*100}\nDETAIL -- Next-week @ 1-ITM\n{'='*100}")
    for t in trades_1itm:
        print(f"  {t['side']}{t['strike']} [{t['tranche']}] entry={t['entry']:.2f}@{t.get('entry_ts')} "
              f"exit={t['exit']:.2f}@{t['exit_ts']} ({t['reason']}) pnl={t['pnl']:+,.0f}")
