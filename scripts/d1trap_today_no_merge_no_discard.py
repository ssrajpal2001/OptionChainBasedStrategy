"""
2026-08-01: NEW continuous-chain zone detection, per the corrected design:
a "fresh trap" = ref candle + an UNBROKEN run of strictly-lower-lows
immediately after it. The chain locks one of two ways:
  - RECLAIM (within the still-continuing chain): some candle's high beats
    ref.high while lows are still making fresh lows -> a normal, tradeable
    zone forms (same downstream 15m/5m/T1/T2 pipeline as before).
  - FAILURE (chain breaks -- a candle prints a HIGHER low than the previous
    chain candle -- before any reclaim happened): no tradeable zone forms.
    The chain's own low becomes a watch level; the first LATER 15m close
    below that level seeds a flip candidate directly (candle A = that
    invalidating close) -- exactly mirroring the existing flip-candidate
    shape, just seeded from a failed fresh trap instead of a from a zone
    that first had to go through full MONITORING.

This directly replaces find_all_bear_zones/_collapse_nearby_zones for THIS
backtest -- everything downstream (T1/T2 tranches, TSL, flip entry checks)
is reused verbatim from bear_only_book.py.
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


def _detect_fresh_traps(bars):
    """Returns (valid_zones, failed_traps). bars = list of Bar namedtuples
    (timestamp, open, high, low, close), oldest..newest."""
    n = len(bars)
    valid_zones, failed_traps = [], []
    known_ref_ts = set()
    for i in range(n - 2, -1, -1):
        ref = bars[i]
        if ref.timestamp in known_ref_ts:
            continue
        # first seller-in candle: first candle after ref whose low < ref.low
        sellers_in_idx = None
        for j in range(i + 1, n):
            if bars[j].low < ref.low:
                sellers_in_idx = j
                break
        if sellers_in_idx is None:
            continue

        # walk the continuous lower-lows chain
        chain_end_idx = sellers_in_idx
        prev_low = bars[sellers_in_idx].low
        reclaim_idx = None
        if bars[sellers_in_idx].high > ref.high:
            reclaim_idx = sellers_in_idx
        else:
            for k in range(sellers_in_idx + 1, n):
                if bars[k].low < prev_low:
                    chain_end_idx = k
                    prev_low = bars[k].low
                    if bars[k].high > ref.high:
                        reclaim_idx = k
                        break
                else:
                    break  # chain breaks -- higher low

        sweep_low = prev_low
        entry_line = ref.low
        zone_lo, zone_hi = sweep_low, entry_line

        if reclaim_idx is not None:
            valid_zones.append(dict(
                zone_lo=zone_lo, zone_hi=zone_hi, entry_line=entry_line, lock_ts=bars[reclaim_idx].timestamp,
                ref_ts=ref.timestamp, ref_high=ref.high, ref_low=ref.low,
                sellers_in_ts=bars[sellers_in_idx].timestamp, sellers_in_low=bars[sellers_in_idx].low,
                reclaim_ts=bars[reclaim_idx].timestamp, reclaim_high=bars[reclaim_idx].high,
                chain_end_ts=bars[chain_end_idx].timestamp, sweep_low=sweep_low,
                state="WAITING", ref_bar=None, done=False, invalid=False,
                contact_ts=None, ref_open=None, ref_close_time=None,
                breach_ts=None, sub_lo=None, sub_hi=None,
            ))
        else:
            failed_traps.append(dict(
                zone_lo=zone_lo, zone_hi=zone_hi, entry_line=entry_line,
                ref_ts=ref.timestamp, ref_high=ref.high, ref_low=ref.low,
                sellers_in_ts=bars[sellers_in_idx].timestamp, sellers_in_low=bars[sellers_in_idx].low,
                chain_end_ts=bars[chain_end_idx].timestamp, chain_end_low=bars[chain_end_idx].low,
                sweep_low=sweep_low, watch_level=sweep_low,
            ))
        known_ref_ts.add(ref.timestamp)
    return valid_zones, failed_traps


def load_option(strike, side):
    df = pd.read_parquet(f"{OPT_DIR}/{strike}_{side}_month.parquet")
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)
    return dict(m1=df, m5=bb._resample(df, 5), m15=bb._resample(df, 15), m60=bb._resample(df, 60))


class SideState:
    def __init__(self, strike, side):
        self.strike, self.side = strike, side
        self.zones, self.flip_candidates, self.failed_traps = [], [], []
        self.positions, self.trades = [], []
        self.prev15_high, self.prev15_low = None, None


def warmup(state, data):
    start = DAY - timedelta(days=HIST_WARMUP_DAYS)
    hist = data["m1"][(data["m1"]["datetime"].dt.date >= start) & (data["m1"]["datetime"].dt.date < DAY)]
    m60_hist = bb._resample(hist, 60)
    valid, failed = _detect_fresh_traps(bb._to_bars(m60_hist))
    state.zones = valid
    state.failed_traps = failed
    # seed flip candidates for failed traps whose watch level has ALREADY
    # been breached (15m close < watch_level) sometime before today
    m15_hist = bb._resample(hist, 15)
    for ft in failed:
        later = m15_hist[(m15_hist["timestamp"] > ft["chain_end_ts"]) & (m15_hist["close"] < ft["watch_level"])]
        if not later.empty:
            first = later.iloc[0]
            state.flip_candidates.append(dict(
                candleA_low=first["low"], candleA_high=first["high"], candleA_ts=first["timestamp"],
                zone_lo=ft["zone_lo"], zone_hi=ft["zone_hi"], zone_lock_ts=ft["ref_ts"],
                parent_zone=None, confirmed=False, cancelled=False, t1_taken=False,
                source="failed_trap", ref_ts=ft["ref_ts"], sellers_in_ts=ft["sellers_in_ts"],
                chain_end_ts=ft["chain_end_ts"], watch_level=ft["watch_level"],
            ))


def refresh_intraday(state, ts, data):
    start = DAY - timedelta(days=HIST_WARMUP_DAYS)
    window = data["m1"][(data["m1"]["datetime"].dt.date >= start) & (data["m1"]["datetime"] < ts)]
    if len(window) < 30:
        return
    m60 = bb._resample(window, 60)
    m15 = bb._resample(window, 15)
    valid, failed = _detect_fresh_traps(bb._to_bars(m60))

    existing_zone_refs = {z["ref_ts"] for z in state.zones}
    for z in valid:
        if z["ref_ts"] not in existing_zone_refs:
            state.zones.append(z)
            existing_zone_refs.add(z["ref_ts"])

    existing_trap_refs = {ft["ref_ts"] for ft in state.failed_traps}
    known_fc_refs = {fc.get("ref_ts") for fc in state.flip_candidates}
    for ft in failed:
        if ft["ref_ts"] not in existing_trap_refs:
            state.failed_traps.append(ft)
            existing_trap_refs.add(ft["ref_ts"])
        if ft["ref_ts"] in known_fc_refs:
            continue
        later = m15[(m15["timestamp"] > ft["chain_end_ts"]) & (m15["close"] < ft["watch_level"])]
        if not later.empty:
            first = later.iloc[0]
            state.flip_candidates.append(dict(
                candleA_low=first["low"], candleA_high=first["high"], candleA_ts=first["timestamp"],
                zone_lo=ft["zone_lo"], zone_hi=ft["zone_hi"], zone_lock_ts=ft["ref_ts"],
                parent_zone=None, confirmed=False, cancelled=False, t1_taken=False,
                source="failed_trap", ref_ts=ft["ref_ts"], sellers_in_ts=ft["sellers_in_ts"],
                chain_end_ts=ft["chain_end_ts"], watch_level=ft["watch_level"],
            ))
            known_fc_refs.add(ft["ref_ts"])


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
            _close(state, pos, "tsl_hit" if pos["high_lock_pct"] > 0 else "sl_hit", stop_price, ts)
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
    # existing zones can still invalidate the OLD way too (belt & suspenders)
    for z in state.zones:
        if not z["done"] and not z["invalid"] and m15_bar.close < z["zone_lo"]:
            z["invalid"] = True
    for fc in state.flip_candidates:
        if fc["confirmed"] or fc["cancelled"] or m15_bar.timestamp <= fc["candleA_ts"]:
            continue
        if m15_bar.low < fc["candleA_low"]:
            continue
        if fc["zone_lo"] <= m15_bar.close <= fc["zone_hi"]:
            fc["cancelled"] = True


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
        audit = dict(flip_source_side=flip_source.side, flip_ref_ts=fc.get("ref_ts"),
                     flip_sellers_in_ts=fc.get("sellers_in_ts"), flip_chain_end_ts=fc.get("chain_end_ts"),
                     flip_watch_level=fc.get("watch_level"), flip_candleA_ts=fc["candleA_ts"],
                     flip_candleA_low=fc["candleA_low"], breakout_15m_ts=m15_bar.timestamp,
                     prev15_high=prev15["high"], sub5m_lo=collapse[0], sub5m_hi=collapse[1])
        open_leg(state, "T2", m15_bar.high, m15_bar.low, fc["zone_lock_ts"], use_tranche_tsl=True,
                 audit=audit, entry_ts=m15_bar.timestamp + timedelta(minutes=15))
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
        audit = dict(flip_source_side=flip_source.side, flip_ref_ts=fc.get("ref_ts"),
                     flip_sellers_in_ts=fc.get("sellers_in_ts"), flip_chain_end_ts=fc.get("chain_end_ts"),
                     flip_watch_level=fc.get("watch_level"), flip_candleA_ts=fc["candleA_ts"],
                     flip_candleA_low=fc["candleA_low"], prev15_high=state.prev15_high)
        open_leg(state, "T1", ltp, state.prev15_low, fc["zone_lock_ts"], use_tranche_tsl=True,
                 audit=audit, entry_ts=ts)
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
                audit = dict(zone_ref_ts=zone["ref_ts"], zone_sellers_in_ts=zone["sellers_in_ts"],
                             zone_reclaim_ts=zone["reclaim_ts"], zone_sweep_low=zone["sweep_low"],
                             zone_lo=zone["zone_lo"], zone_hi=zone["zone_hi"],
                             contact_ts=zone["contact_ts"], ref15_open=zone["ref_open"], ref15_close=zone["ref_close_time"],
                             ref15_high=zone["ref_high"], ref15_low=zone["ref_low"], breach_ts=zone["breach_ts"],
                             sub5m_lo=None, sub5m_hi=None, armed_ts=None)
                open_leg(state, "T1", zone["ref_high"], zone["ref_low"], zone["ref_ts"], use_tranche_tsl=True,
                         audit=audit, entry_ts=last_ts)
                active_locks.add(zone["ref_ts"])
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
                zone["armed_ts"] = last_ts
            continue
        if last_high >= zone["sub_hi"]:
            if not any(p["zone_lock_ts"] == zone["ref_ts"] and p["tranche"] == "T2" for p in state.positions):
                audit = dict(zone_ref_ts=zone["ref_ts"], zone_sellers_in_ts=zone["sellers_in_ts"],
                             zone_reclaim_ts=zone["reclaim_ts"], zone_sweep_low=zone["sweep_low"],
                             zone_lo=zone["zone_lo"], zone_hi=zone["zone_hi"],
                             contact_ts=zone["contact_ts"], ref15_open=zone["ref_open"], ref15_close=zone["ref_close_time"],
                             ref15_high=zone["ref_high"], ref15_low=zone["ref_low"], breach_ts=zone["breach_ts"],
                             sub5m_lo=zone["sub_lo"], sub5m_hi=zone["sub_hi"], armed_ts=zone.get("armed_ts"))
                open_leg(state, "T2", zone["sub_hi"], zone["ref_low"], zone["ref_ts"], use_tranche_tsl=True,
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
    warmup(ce_state, ce_data)
    warmup(pe_state, pe_data)

    def fmt(ts):
        return ts.strftime("%m-%d %H:%M") if ts is not None and pd.notna(ts) else "-"

    print(f"CE valid (reclaimed) zones: {len(ce_state.zones)}")
    for z in ce_state.zones:
        print(f"  ref={fmt(z['ref_ts'])}(L={z['ref_low']:.2f}) sellers_in={fmt(z['sellers_in_ts'])}(L={z['sellers_in_low']:.2f}) "
              f"reclaim={fmt(z['reclaim_ts'])}(H={z['reclaim_high']:.2f}) sweep_low={z['sweep_low']:.2f}  zone=[{z['zone_lo']:.2f},{z['zone_hi']:.2f}]")
    print(f"CE failed traps (no reclaim, chain broke): {len(ce_state.failed_traps)}")
    for ft in ce_state.failed_traps:
        print(f"  ref={fmt(ft['ref_ts'])}(L={ft['ref_low']:.2f}) sellers_in={fmt(ft['sellers_in_ts'])}(L={ft['sellers_in_low']:.2f}) "
              f"chain_end={fmt(ft['chain_end_ts'])}(L={ft['chain_end_low']:.2f})  watch_level={ft['watch_level']:.2f}")
    print(f"\nPE valid (reclaimed) zones: {len(pe_state.zones)}")
    for z in pe_state.zones:
        print(f"  ref={fmt(z['ref_ts'])}(L={z['ref_low']:.2f}) sellers_in={fmt(z['sellers_in_ts'])}(L={z['sellers_in_low']:.2f}) "
              f"reclaim={fmt(z['reclaim_ts'])}(H={z['reclaim_high']:.2f}) sweep_low={z['sweep_low']:.2f}  zone=[{z['zone_lo']:.2f},{z['zone_hi']:.2f}]")
    print(f"PE failed traps (no reclaim, chain broke): {len(pe_state.failed_traps)}")
    for ft in pe_state.failed_traps:
        print(f"  ref={fmt(ft['ref_ts'])}(L={ft['ref_low']:.2f}) sellers_in={fmt(ft['sellers_in_ts'])}(L={ft['sellers_in_low']:.2f}) "
              f"chain_end={fmt(ft['chain_end_ts'])}(L={ft['chain_end_low']:.2f})  watch_level={ft['watch_level']:.2f}")
    print(f"\nCE flip candidates seeded pre-market: {len(ce_state.flip_candidates)}")
    print(f"PE flip candidates seeded pre-market: {len(pe_state.flip_candidates)}")

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
                refresh_intraday(ce_state, ce_m60_today.iloc[ce60]["timestamp"] + timedelta(minutes=60), ce_data)
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
                refresh_intraday(pe_state, pe_m60_today.iloc[pe60]["timestamp"] + timedelta(minutes=60), pe_data)
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

    all_trades = sorted(ce_state.trades + pe_state.trades, key=lambda t: t["exit_ts"])
    print(f"\n{'='*140}\nFULL AUDIT -- continuous-chain zones, clean 09:15 start, 2026-07-31\n{'='*140}")
    total = 0
    for t in all_trades:
        total += t["pnl"]
        print(f"\n### {t['side']}{t['strike']}  [{t['tranche'].upper()}]  entry={t['entry']:.2f}@{fmt(t.get('entry_ts'))}  "
              f"exit={t['exit']:.2f}@{fmt(t['exit_ts'])}  ({t['reason']})  PnL=Rs{t['pnl']:+,.0f}")
        if "flip_source_side" in t:
            print(f"    FLIP TRADE -- triggered by {t.get('flip_source_side')} failed trap:")
            print(f"      {t.get('flip_source_side')} ref candle          : {fmt(t.get('flip_ref_ts'))}")
            print(f"      {t.get('flip_source_side')} sellers-in candle   : {fmt(t.get('flip_sellers_in_ts'))}")
            print(f"      {t.get('flip_source_side')} chain-end (fail) ts : {fmt(t.get('flip_chain_end_ts'))}  watch_level={t.get('flip_watch_level'):.2f}")
            print(f"      {t.get('flip_source_side')} candle A (breach)   : {fmt(t.get('flip_candleA_ts'))}  low={t.get('flip_candleA_low'):.2f}")
            if t["tranche"] == "T1":
                print(f"      T1 (15m concept): fast trigger, {t['side']} tick > prev15_high={t.get('prev15_high'):.2f} -> entry @ {t['entry']:.2f}")
            else:
                print(f"      T2 (15m concept): breakout 15m {fmt(t.get('breakout_15m_ts'))} high={t['entry']:.2f} > prev15_high={t.get('prev15_high'):.2f}")
                print(f"      T2 (5m concept) : subzone found [{t.get('sub5m_lo'):.2f}, {t.get('sub5m_hi'):.2f}]")
        else:
            print(f"      zone ref candle       : {fmt(t.get('zone_ref_ts'))}")
            print(f"      zone sellers-in candle: {fmt(t.get('zone_sellers_in_ts'))}")
            print(f"      zone reclaim candle   : {fmt(t.get('zone_reclaim_ts'))}  sweep_low={t.get('zone_sweep_low'):.2f}  zone=[{t.get('zone_lo'):.2f},{t.get('zone_hi'):.2f}]")
            print(f"      contact               : {fmt(t.get('contact_ts'))}")
            print(f"      15m concept: ref-candle {fmt(t.get('ref15_open'))} (closes {fmt(t.get('ref15_close'))}) H={t.get('ref15_high'):.2f} L={t.get('ref15_low'):.2f}, breach={fmt(t.get('breach_ts'))}")
            if t.get("sub5m_lo") is not None:
                print(f"      5m concept : subzone [{t.get('sub5m_lo'):.2f},{t.get('sub5m_hi'):.2f}] armed_ts={fmt(t.get('armed_ts'))}")
            else:
                print(f"      5m concept : no subzone -> raw breakout")
    print(f"\nn={len(all_trades)}  total PnL = Rs{total:+,.0f}")


if __name__ == "__main__":
    main()
