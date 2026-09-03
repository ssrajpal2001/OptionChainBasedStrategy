"""
2026-07-31 scenario: what would have happened if the app had started cleanly
at 09:15 with TODAY'S final code (no restarts, no mid-day fixes) -- the exact
mechanic now live in strategies/d1_trap_option/bear_only_book.py: zone
invalidation, the corrected flip concept, fast tick-level T1 tranche + T2
confirmation tranche, independent per-leg 20%/12.5% staircase TSL.

Reuses the pure functions/constants directly from bear_only_book.py so this
is a faithful replay of the actual deployed logic, not an approximation.
"""
import sys
sys.path.insert(0, ".")
import strategies.d1_trap_option.bear_only_book as bb
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
DAY = pd.Timestamp("2026-07-31").date()


def load_option(strike, side):
    df = pd.read_parquet(f"{OPT_DIR}/{strike}_{side}_month.parquet")
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)
    return dict(m1=df, m5=bb._resample(df, 5), m15=bb._resample(df, 15), m60=bb._resample(df, 60))


class SideState:
    def __init__(self, strike, side):
        self.strike, self.side = strike, side
        self.zones, self.flip_candidates = [], []
        self.positions = []   # list of legs -- mirrors live self._positions
        self.trades = []
        self.prev15_high, self.prev15_low = None, None


def warmup_zones(state, data):
    start = DAY - timedelta(days=HIST_WARMUP_DAYS)
    hist = data["m1"][(data["m1"]["datetime"].dt.date >= start) & (data["m1"]["datetime"].dt.date < DAY)]
    m60_hist, m15_hist = bb._resample(hist, 60), bb._resample(hist, 15)
    state.zones = bb._prevalidate_zones(bb._detect_bear_zones(bb._to_bars(m60_hist)), m15_hist)


def refresh_intraday_zones(state, ts, data):
    start = DAY - timedelta(days=HIST_WARMUP_DAYS)
    window = data["m1"][(data["m1"]["datetime"].dt.date >= start) & (data["m1"]["datetime"] < ts)]
    if len(window) < 30:
        return
    m60, m15 = bb._resample(window, 60), bb._resample(window, 15)
    existing = {z["lock_ts"] for z in state.zones}

    def overlaps(z):
        return any(abs(z["zone_lo"] - e["zone_lo"]) <= 5.0 and abs(z["zone_hi"] - e["zone_hi"]) <= 5.0
                   for e in state.zones)
    new_zones = [z for z in bb._detect_bear_zones(bb._to_bars(m60))
                 if z["lock_ts"] not in existing and not overlaps(z)]
    if new_zones:
        state.zones.extend(bb._prevalidate_zones(new_zones, m15))


def open_leg(state, tranche, entry_price, sl, zone_lock_ts, use_tranche_tsl, audit=None, entry_ts=None):
    sl_final = max(sl - SL_BUFFER_PTS, entry_price - MAX_RISK_RS_PER_LOT / LOT_SIZE)
    if use_tranche_tsl:
        b, bl, s, sl_ = bb._TSL_TRANCHE_BASE_PCT, bb._TSL_TRANCHE_BASE_LOCK_PCT, bb._TSL_TRANCHE_STEP_PCT, bb._TSL_TRANCHE_STEP_LOCK_PCT
    else:
        b, bl, s, sl_ = bb._TSL_BASE_PCT, bb._TSL_BASE_LOCK_PCT, bb._TSL_STEP_PCT, bb._TSL_STEP_LOCK_PCT
    state.positions.append(dict(side=state.side, strike=state.strike, entry_price=entry_price, sl=sl_final,
                                 high_lock_pct=0.0, tranche=tranche, zone_lock_ts=zone_lock_ts,
                                 tsl_base_pct=b, tsl_base_lock_pct=bl, tsl_step_pct=s, tsl_step_lock_pct=sl_,
                                 audit=audit or {}, entry_ts=entry_ts))


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
            reason = "tsl_hit" if pos["high_lock_pct"] > 0 else "sl_hit"
            _close(state, pos, reason, stop_price, ts)
        elif now_t >= EOD_TIME:
            _close(state, pos, "eod", ltp, ts)


def _close(state, pos, reason, exit_price, ts):
    state.positions = [p for p in state.positions if p is not pos]
    pnl = (exit_price - pos["entry_price"]) * LOT_SIZE
    trade = dict(side=pos["side"], strike=pos["strike"], tranche=pos["tranche"],
                 entry=pos["entry_price"], entry_ts=pos.get("entry_ts"),
                 sl_initial=pos["sl"], locked_pct=pos["high_lock_pct"],
                 exit=exit_price, reason=reason, exit_ts=ts, pnl=pnl)
    trade.update(pos.get("audit") or {})
    state.trades.append(trade)


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
        if fc["confirmed"] or fc["cancelled"]:
            continue
        if m15_bar.timestamp <= fc["candleA_ts"]:
            continue
        if m15_bar.low < fc["candleA_low"]:
            continue
        if fc["zone_lo"] <= m15_bar.close <= fc["zone_hi"]:
            fc["cancelled"] = True
            fc["parent_zone"]["invalid"] = False


def process_flip_entry_t2(state, m15_bar, m15, m5, flip_source):
    if any(p["tranche"] == "T2" for p in state.positions):
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
        if fc["confirmed"] or fc["cancelled"]:
            continue
        if m15_bar.timestamp <= fc["candleA_ts"]:
            continue
        window_5m = m5[(m5["timestamp"] >= m15_bar.timestamp) & (m5["timestamp"] < m15_bar.timestamp + timedelta(minutes=15))]
        collapse = bb._collapse_subzones(bb._to_bars(window_5m))
        if collapse is None:
            continue
        fc["confirmed"] = True
        audit = dict(flip_source_side=flip_source.side, flip_zone_lock_ts=fc["zone_lock_ts"],
                     flip_candleA_ts=fc["candleA_ts"], flip_candleA_low=fc["candleA_low"],
                     flip_invalid_ts=fc["parent_zone"].get("invalid_ts"),
                     breakout_15m_ts=m15_bar.timestamp, prev15_high=prev15["high"],
                     sub5m_lo=collapse[0], sub5m_hi=collapse[1])
        open_leg(state, "T2", m15_bar.high, m15_bar.low, fc["zone_lock_ts"], use_tranche_tsl=True,
                 audit=audit, entry_ts=m15_bar.timestamp + timedelta(minutes=15))
        return


def check_fast_t1(state, ltp, ts, flip_source):
    if any(p["side"] == state.side for p in state.positions):
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
        audit = dict(flip_source_side=flip_source.side, flip_zone_lock_ts=fc["zone_lock_ts"],
                     flip_candleA_ts=fc["candleA_ts"], flip_candleA_low=fc["candleA_low"],
                     flip_invalid_ts=fc["parent_zone"].get("invalid_ts"),
                     prev15_high=state.prev15_high)
        open_leg(state, "T1", ltp, state.prev15_low, fc["zone_lock_ts"], use_tranche_tsl=True,
                 audit=audit, entry_ts=ts)
        return


def process_zones_tick(state, last_ts, last_low, last_high, m15, m5):
    if last_ts.time() >= ENTRY_CUTOFF:
        return
    active_locks = {p["zone_lock_ts"] for p in state.positions}
    for zone in state.zones:
        if zone["done"] or zone["invalid"]:
            continue
        if state.positions and zone["lock_ts"] not in active_locks:
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
                audit = dict(zone_lock_ts=zone["lock_ts"], zone_lo=zone["zone_lo"], zone_hi=zone["zone_hi"],
                             contact_ts=zone["contact_ts"], ref_open=zone["ref_open"], ref_close_time=zone["ref_close_time"],
                             ref_high=zone["ref_high"], ref_low=zone["ref_low"], breach_ts=zone["breach_ts"],
                             sub5m_lo=None, sub5m_hi=None, armed_ts=None)
                open_leg(state, "T1", zone["ref_high"], zone["ref_low"], zone["lock_ts"], use_tranche_tsl=True,
                         audit=audit, entry_ts=last_ts)
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
                zone["done"] = True   # no subzone -> no T2 possible, T1 alone stands
                return
            zone["sub_lo"], zone["sub_hi"] = collapse
            threshold_pts = bb._ZONE_SIZE_THRESHOLD_PCT / 100.0 * zone["ref_high"]
            zone["arm_level"] = bb._arm_level(zone["sub_lo"], zone["sub_hi"], threshold_pts)
            zone["armed"] = False
            continue
        if not zone.get("armed"):
            if zone["sub_lo"] <= last_low <= zone["arm_level"]:
                zone["armed"] = True
                zone["armed_ts"] = last_ts
            continue
        if last_high >= zone["sub_hi"]:
            if not any(p["zone_lock_ts"] == zone["lock_ts"] and p["tranche"] == "T2" for p in state.positions):
                audit = dict(zone_lock_ts=zone["lock_ts"], zone_lo=zone["zone_lo"], zone_hi=zone["zone_hi"],
                             contact_ts=zone["contact_ts"], ref_open=zone["ref_open"], ref_close_time=zone["ref_close_time"],
                             ref_high=zone["ref_high"], ref_low=zone["ref_low"], breach_ts=zone["breach_ts"],
                             sub5m_lo=zone["sub_lo"], sub5m_hi=zone["sub_hi"], armed_ts=zone.get("armed_ts"))
                open_leg(state, "T2", zone["sub_hi"], zone["ref_low"], zone["lock_ts"], use_tranche_tsl=True,
                         audit=audit, entry_ts=last_ts)
            zone["done"] = True
            return


def main():
    spot = pd.read_parquet(SPOT_PATH)
    spot["datetime"] = pd.to_datetime(spot["datetime"])
    day_open = spot[spot["datetime"].dt.date == DAY].iloc[0]["open"]
    atm = round(day_open / ATM_ROUND_STEP) * ATM_ROUND_STEP
    ce_strike, pe_strike = int(atm - ITM_OFFSET_PTS), int(atm + ITM_OFFSET_PTS)
    print(f"spot_open={day_open:.2f} ATM={atm} -> CE={ce_strike} PE={pe_strike}\n")

    ce_data, pe_data = load_option(ce_strike, "CE"), load_option(pe_strike, "PE")
    ce_state, pe_state = SideState(ce_strike, "CE"), SideState(pe_strike, "PE")
    warmup_zones(ce_state, ce_data)
    warmup_zones(pe_state, pe_data)
    print(f"CE warmed -> {len(ce_state.zones)} zones")
    print(f"PE warmed -> {len(pe_state.zones)} zones\n")

    ce_today = ce_data["m1"][ce_data["m1"]["datetime"].dt.date == DAY].reset_index(drop=True)
    pe_today = pe_data["m1"][pe_data["m1"]["datetime"].dt.date == DAY].reset_index(drop=True)
    ce_m15_today = ce_data["m15"][ce_data["m15"]["timestamp"].dt.date == DAY].reset_index(drop=True)
    pe_m15_today = pe_data["m15"][pe_data["m15"]["timestamp"].dt.date == DAY].reset_index(drop=True)
    ce_m60_today = ce_data["m60"][ce_data["m60"]["timestamp"].dt.date == DAY].reset_index(drop=True)
    pe_m60_today = pe_data["m60"][pe_data["m60"]["timestamp"].dt.date == DAY].reset_index(drop=True)

    max_len = max(len(ce_today), len(pe_today))
    ce15, pe15, ce60, pe60 = 0, 0, 0, 0
    for i in range(max_len):
        if i < len(ce_today):
            bar = ce_today.iloc[i]; ts = bar["datetime"]
            while ce60 < len(ce_m60_today) and ce_m60_today.iloc[ce60]["timestamp"] + timedelta(minutes=60) <= ts:
                refresh_intraday_zones(ce_state, ce_m60_today.iloc[ce60]["timestamp"] + timedelta(minutes=60), ce_data)
                ce60 += 1
            while ce15 < len(ce_m15_today) and ce_m15_today.iloc[ce15]["timestamp"] + timedelta(minutes=15) <= ts:
                m15row = ce_m15_today.iloc[ce15]
                ce_state.prev15_high, ce_state.prev15_low = m15row["high"], m15row["low"]
                on_new_15m_close(ce_state, m15row)
                process_flip_entry_t2(ce_state, m15row, ce_data["m15"], ce_data["m5"], pe_state)
                ce15 += 1
            check_exit(ce_state, bar["close"], ts)
            check_fast_t1(ce_state, bar["high"], ts, pe_state)
            process_zones_tick(ce_state, ts, bar["low"], bar["high"], ce_data["m15"], ce_data["m5"])
        if i < len(pe_today):
            bar = pe_today.iloc[i]; ts = bar["datetime"]
            while pe60 < len(pe_m60_today) and pe_m60_today.iloc[pe60]["timestamp"] + timedelta(minutes=60) <= ts:
                refresh_intraday_zones(pe_state, pe_m60_today.iloc[pe60]["timestamp"] + timedelta(minutes=60), pe_data)
                pe60 += 1
            while pe15 < len(pe_m15_today) and pe_m15_today.iloc[pe15]["timestamp"] + timedelta(minutes=15) <= ts:
                m15row = pe_m15_today.iloc[pe15]
                pe_state.prev15_high, pe_state.prev15_low = m15row["high"], m15row["low"]
                on_new_15m_close(pe_state, m15row)
                process_flip_entry_t2(pe_state, m15row, pe_data["m15"], pe_data["m5"], ce_state)
                pe15 += 1
            check_exit(pe_state, bar["close"], ts)
            check_fast_t1(pe_state, bar["high"], ts, ce_state)
            process_zones_tick(pe_state, ts, bar["low"], bar["high"], pe_data["m15"], pe_data["m5"])

    if ce_state.positions and not ce_today.empty:
        last = ce_today.iloc[-1]
        for pos in list(ce_state.positions):
            _close(ce_state, pos, "eod", last["close"], last["datetime"])
    if pe_state.positions and not pe_today.empty:
        last = pe_today.iloc[-1]
        for pos in list(pe_state.positions):
            _close(pe_state, pos, "eod", last["close"], last["datetime"])

    all_trades = sorted(ce_state.trades + pe_state.trades, key=lambda t: t["exit_ts"])

    def fmt(ts):
        return ts.strftime("%m-%d %H:%M:%S") if ts is not None and pd.notna(ts) else "-"

    print(f"\nCE warmed zones (as of 09:15 today):")
    for z in ce_state.zones:
        print(f"  [{z['zone_lo']:.2f},{z['zone_hi']:.2f}] lock={z['lock_ts']} invalid={z['invalid']} state={z['state']}")
    print(f"PE warmed zones (as of 09:15 today):")
    for z in pe_state.zones:
        print(f"  [{z['zone_lo']:.2f},{z['zone_hi']:.2f}] lock={z['lock_ts']} invalid={z['invalid']} state={z['state']}")
    print(f"\nCE all flip candidates: {len(ce_state.flip_candidates)}")
    for fc in ce_state.flip_candidates:
        print(f"  candleA_ts={fc['candleA_ts']} low={fc['candleA_low']:.2f} zone=[{fc['zone_lo']:.2f},{fc['zone_hi']:.2f}] "
              f"confirmed={fc['confirmed']} cancelled={fc['cancelled']} t1_taken={fc['t1_taken']}")
    print(f"PE all flip candidates: {len(pe_state.flip_candidates)}")
    for fc in pe_state.flip_candidates:
        print(f"  candleA_ts={fc['candleA_ts']} low={fc['candleA_low']:.2f} zone=[{fc['zone_lo']:.2f},{fc['zone_hi']:.2f}] "
              f"confirmed={fc['confirmed']} cancelled={fc['cancelled']} t1_taken={fc['t1_taken']}")

    print(f"\n{'='*130}\nFULL AUDIT -- clean 09:15 start, today's final code, 2026-07-31\n{'='*130}")
    total = 0
    for t in all_trades:
        total += t["pnl"]
        print(f"\n### {t['side']}{t['strike']}  [{t['tranche'].upper()}]  entry={t['entry']:.2f}@{fmt(t.get('entry_ts'))}  "
              f"exit={t['exit']:.2f}@{fmt(t['exit_ts'])}  ({t['reason']})  PnL=Rs{t['pnl']:+,.0f}")
        if "flip_source_side" in t:
            print(f"    FLIP TRADE -- triggered by {t.get('flip_source_side')} invalidation:")
            print(f"      {t.get('flip_source_side')} 60m zone locked (trap confirmed) : {fmt(t.get('flip_zone_lock_ts'))}")
            print(f"      {t.get('flip_source_side')} candle A (invalidating 15m close): {fmt(t.get('flip_candleA_ts'))}  "
                  f"low={t.get('flip_candleA_low'):.2f}  (zone invalid @ {fmt(t.get('flip_invalid_ts'))})")
            if t["tranche"] == "T1":
                print(f"      T1 fast trigger: {t['side']} tick > prev15_high={t.get('prev15_high'):.2f}  "
                      f"-> immediate entry @ {t['entry']:.2f} ({fmt(t.get('entry_ts'))})")
                print(f"      SL = prev-15m candle's own low (buffered/capped) = {t['sl_initial']:.2f}")
            else:
                print(f"      T2 confirm: {t['side']} breakout 15m {fmt(t.get('breakout_15m_ts'))} "
                      f"high={t['entry']:.2f} > prev15_high={t.get('prev15_high'):.2f}")
                print(f"      5m subzone found in breakout candle: [{t.get('sub5m_lo'):.2f}, {t.get('sub5m_hi'):.2f}]")
                print(f"      SL = breakout candle's own low (buffered/capped) = {t['sl_initial']:.2f}")
        else:
            print(f"      60m zone locked (trap ref candle)  : {fmt(t.get('zone_lock_ts'))}  "
                  f"zone=[{t.get('zone_lo'):.2f}, {t.get('zone_hi'):.2f}]")
            print(f"      price entered zone (contact)       : {fmt(t.get('contact_ts'))}")
            print(f"      15m ref-candle assigned            : {fmt(t.get('ref_open'))}  "
                  f"(closes {fmt(t.get('ref_close_time'))})  ref_high={t.get('ref_high'):.2f} ref_low={t.get('ref_low'):.2f}")
            print(f"      ref-candle high breached (trigger) : {fmt(t.get('breach_ts'))}")
            if t.get("sub5m_lo") is not None:
                print(f"      5m subzone found -> armed          : sub=[{t.get('sub5m_lo'):.2f}, {t.get('sub5m_hi'):.2f}]  "
                      f"armed_ts={fmt(t.get('armed_ts'))}")
                print(f"      swing-break entry                  : entry={t['entry']:.2f} @ {fmt(t.get('entry_ts'))}")
            else:
                print(f"      no 5m subzone -> RAW BREAKOUT fallback entry @ ref_high, immediately on ref breach")
        if t["locked_pct"] > 0:
            print(f"      TSL locked @ {t['locked_pct']*100:.1f}% of entry -> exit stop = "
                  f"{t['entry']*(1+t['locked_pct']):.2f}")
        else:
            print(f"      never reached TSL-activation profit -> exited at initial SL = {t['sl_initial']:.2f}")

    print(f"\n{'='*130}")
    print(f"{'Side':<8}{'Tranche':>8}{'Entry':>9}{'EntryTS':>11}{'Exit':>9}{'ExitTS':>11}{'Reason':>9}{'SL':>9}{'TSL%':>6}{'PnL':>9}")
    print("-" * 130)
    for t in all_trades:
        print(f"{t['side']+str(t['strike']):<8}{t['tranche']:>8}{t['entry']:>9.2f}{fmt(t.get('entry_ts')):>11}"
              f"{t['exit']:>9.2f}{fmt(t['exit_ts']):>11}{t['reason']:>9}{t['sl_initial']:>9.2f}"
              f"{t['locked_pct']*100:>5.1f}%{t['pnl']:>+9.0f}")
    print(f"\nn={len(all_trades)}  total PnL = Rs{total:+,.0f}")
    print(f"\nCE zones remaining: {[{'state':z['state'],'invalid':z['invalid'],'done':z['done']} for z in ce_state.zones if not z['done']]}")
    print(f"PE zones remaining: {[{'state':z['state'],'invalid':z['invalid'],'done':z['done']} for z in pe_state.zones if not z['done']]}")


if __name__ == "__main__":
    main()
