"""
2026-08-01: backtest with the CORRECTED zone algorithm, per user's direct
correction of the previous two attempts:
  Step 1: for each ref candle whose own high eventually gets reclaimed
          (find_all_bear_zones decides reclaim/validity), the zone's PRICE
          RANGE is the tight two-candle footprint [ref.low, sellers_in.low]
          -- NOT the multi-day running sweep_low. If a ref candle's high
          never gets reclaimed within the 14-day window, NO ZONE forms at
          all for it (not even a watch level) -- scrapped the "failed trap
          seeds a flip" idea entirely per explicit user pushback.
  Step 2: merge zones that are close in PRICE **and** close in TIME (their
          ref candles within 2 60m-bars of each other) -- "2 candle
          neighbour will be merged". This stops unrelated weeks-apart
          trades from chaining into one mega-zone via pure price proximity.
Everything downstream (contact/15m-ref/breach/5m-subzone/T1+T2 tranches/
flip concept from a zone that was VALID and then got invalidated live/SL/
TSL) is unchanged from the live bear_only_book.py design -- reused as-is.
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
MERGE_THRESHOLD_PTS = bb._ZONE_MERGE_THRESHOLD_PTS
MAX_REF_GAP = 2   # "2 candle neighbour"
DAY = pd.Timestamp("2026-07-31").date()


def _detect_zones_corrected(bars):
    """Step 1 (tight [ref.low, sellers_in.low] boundary, reclaim-gated
    validity) + Step 2 (price-AND-time-bound neighbor merge)."""
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
        raw.append(dict(
            zone_lo=lo, zone_hi=hi, entry_line=ref.low, lock_ts=z.lock_ts,
            ref_ts=ref.timestamp, ref_idx=ref_i, ref_high=ref.high, ref_low=ref.low,
            sellers_in_ts=sellers_in.timestamp, sellers_in_low=sellers_in.low,
            reclaim_ts=z.lock_ts, reclaim_high=bars[idx_by_ts[z.lock_ts]].high,
            state="WAITING", ref_bar=None, done=False, invalid=False,
            contact_ts=None, ref_open=None, ref_close_time=None,
            breach_ts=None, sub_lo=None, sub_hi=None,
        ))
    if not raw:
        return []
    ordered = sorted(raw, key=lambda z: (z["zone_lo"], z["zone_hi"]))
    groups = [[ordered[0]]]
    for z in ordered[1:]:
        grp = groups[-1]
        group_lo = min(g["zone_lo"] for g in grp)
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
        merged.append(dict(
            zone_lo=min(g["zone_lo"] for g in group), zone_hi=max(g["zone_hi"] for g in group),
            entry_line=newest["entry_line"], lock_ts=newest["lock_ts"],
            ref_ts=newest["ref_ts"], ref_idx=newest["ref_idx"],
            ref_high=newest["ref_high"], ref_low=newest["ref_low"],
            sellers_in_ts=newest["sellers_in_ts"], sellers_in_low=newest["sellers_in_low"],
            reclaim_ts=newest["reclaim_ts"], reclaim_high=newest["reclaim_high"],
            state="WAITING", ref_bar=None, done=False, invalid=False,
            contact_ts=None, ref_open=None, ref_close_time=None,
            breach_ts=None, sub_lo=None, sub_hi=None,
        ))
    return merged


def _prevalidate(zones, m15):
    for z in zones:
        later = m15[(m15["timestamp"] > z["lock_ts"]) & (m15["close"] < z["zone_lo"])]
        if not later.empty:
            z["invalid"] = True
    return zones


def load_option(strike, side):
    df = pd.read_parquet(f"{OPT_DIR}/{strike}_{side}_month.parquet")
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)
    return dict(m1=df, m5=bb._resample(df, 5), m15=bb._resample(df, 15), m60=bb._resample(df, 60))


class SideState:
    def __init__(self, strike, side):
        self.strike, self.side = strike, side
        self.zones, self.flip_candidates = [], []
        self.positions, self.trades = [], []
        self.prev15_high, self.prev15_low = None, None


def warmup(state, data):
    start = DAY - timedelta(days=HIST_WARMUP_DAYS)
    hist = data["m1"][(data["m1"]["datetime"].dt.date >= start) & (data["m1"]["datetime"].dt.date < DAY)]
    m60_hist, m15_hist = bb._resample(hist, 60), bb._resample(hist, 15)
    state.zones = _prevalidate(_detect_zones_corrected(bb._to_bars(m60_hist)), m15_hist)


def refresh_intraday(state, ts, data):
    start = DAY - timedelta(days=HIST_WARMUP_DAYS)
    window = data["m1"][(data["m1"]["datetime"].dt.date >= start) & (data["m1"]["datetime"] < ts)]
    if len(window) < 30:
        return
    m60, m15 = bb._resample(window, 60), bb._resample(window, 15)
    existing_refs = {z["ref_ts"] for z in state.zones}
    new_zones = [z for z in _detect_zones_corrected(bb._to_bars(m60)) if z["ref_ts"] not in existing_refs]
    state.zones.extend(_prevalidate(new_zones, m15))


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


def _create_flip_candidate(state, zone, candleA):
    state.flip_candidates.append(dict(
        candleA_low=candleA.low, candleA_high=candleA.high, candleA_ts=candleA.timestamp,
        zone_lo=zone["zone_lo"], zone_hi=zone["zone_hi"], zone_lock_ts=zone["ref_ts"],
        confirmed=False, cancelled=False, t1_taken=False,
        ref_ts=zone["ref_ts"], sellers_in_ts=zone["sellers_in_ts"], reclaim_ts=zone["reclaim_ts"],
    ))


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
                     flip_sellers_in_ts=fc.get("sellers_in_ts"), flip_reclaim_ts=fc.get("reclaim_ts"),
                     flip_zone_lo=fc["zone_lo"], flip_zone_hi=fc["zone_hi"],
                     flip_candleA_ts=fc["candleA_ts"], flip_candleA_low=fc["candleA_low"],
                     breakout_15m_ts=m15_bar.timestamp, prev15_high=prev15["high"],
                     sub5m_lo=collapse[0], sub5m_hi=collapse[1])
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
                     flip_sellers_in_ts=fc.get("sellers_in_ts"), flip_reclaim_ts=fc.get("reclaim_ts"),
                     flip_zone_lo=fc["zone_lo"], flip_zone_hi=fc["zone_hi"],
                     flip_candleA_ts=fc["candleA_ts"], flip_candleA_low=fc["candleA_low"],
                     prev15_high=state.prev15_high)
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
                audit = dict(zone_ref_ts=zone["ref_ts"], zone_sellers_in_ts=zone["sellers_in_ts"],
                             zone_reclaim_ts=zone["reclaim_ts"], zone_lo=zone["zone_lo"], zone_hi=zone["zone_hi"],
                             contact_ts=zone["contact_ts"], ref15_open=zone["ref_open"], ref15_close=zone["ref_close_time"],
                             ref15_high=zone["ref15_high"], ref15_low=zone["ref15_low"], breach_ts=zone["breach_ts"],
                             sub5m_lo=None, sub5m_hi=None, armed_ts=None)
                open_leg(state, "T1", zone["ref15_high"], zone["ref15_low"], zone["ref_ts"], use_tranche_tsl=True,
                         audit=audit, entry_ts=last_ts)
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
                zone["armed_ts"] = last_ts
            continue
        if last_high >= zone["sub_hi"]:
            if not any(p["zone_lock_ts"] == zone["ref_ts"] and p["tranche"] == "T2" for p in state.positions):
                audit = dict(zone_ref_ts=zone["ref_ts"], zone_sellers_in_ts=zone["sellers_in_ts"],
                             zone_reclaim_ts=zone["reclaim_ts"], zone_lo=zone["zone_lo"], zone_hi=zone["zone_hi"],
                             contact_ts=zone["contact_ts"], ref15_open=zone["ref_open"], ref15_close=zone["ref_close_time"],
                             ref15_high=zone["ref15_high"], ref15_low=zone["ref15_low"], breach_ts=zone["breach_ts"],
                             sub5m_lo=zone["sub_lo"], sub5m_hi=zone["sub_hi"], armed_ts=zone.get("armed_ts"))
                open_leg(state, "T2", zone["sub_hi"], zone["ref15_low"], zone["ref_ts"], use_tranche_tsl=True,
                         audit=audit, entry_ts=last_ts)
            zone["done"] = True
            return


def fmt(ts):
    return ts.strftime("%m-%d %H:%M") if ts is not None and pd.notna(ts) else "-"


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

    print(f"CE valid zones after warmup: {len(ce_state.zones)}  (invalid={sum(1 for z in ce_state.zones if z['invalid'])})")
    for z in sorted(ce_state.zones, key=lambda z: z["zone_lo"]):
        print(f"  [{z['zone_lo']:.2f},{z['zone_hi']:.2f}]  ref={fmt(z['ref_ts'])} sellers_in={fmt(z['sellers_in_ts'])} "
              f"reclaim={fmt(z['reclaim_ts'])}  invalid={z['invalid']}")
    print(f"PE valid zones after warmup: {len(pe_state.zones)}  (invalid={sum(1 for z in pe_state.zones if z['invalid'])})")
    for z in sorted(pe_state.zones, key=lambda z: z["zone_lo"]):
        print(f"  [{z['zone_lo']:.2f},{z['zone_hi']:.2f}]  ref={fmt(z['ref_ts'])} sellers_in={fmt(z['sellers_in_ts'])} "
              f"reclaim={fmt(z['reclaim_ts'])}  invalid={z['invalid']}")

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
    print(f"\n{'='*140}\nFULL AUDIT -- corrected [ref.low,sellers_in.low] boundary + 2-candle-neighbor merge, 2026-07-31\n{'='*140}")
    total = 0
    for t in all_trades:
        total += t["pnl"]
        print(f"\n### {t['side']}{t['strike']}  [{t['tranche'].upper()}]  entry={t['entry']:.2f}@{fmt(t.get('entry_ts'))}  "
              f"exit={t['exit']:.2f}@{fmt(t['exit_ts'])}  ({t['reason']})  PnL=Rs{t['pnl']:+,.0f}")
        if "flip_source_side" in t:
            print(f"    FLIP TRADE -- triggered by {t.get('flip_source_side')} zone invalidation:")
            print(f"      {t.get('flip_source_side')} zone ref candle      : {fmt(t.get('flip_ref_ts'))}")
            print(f"      {t.get('flip_source_side')} zone sellers-in      : {fmt(t.get('flip_sellers_in_ts'))}")
            print(f"      {t.get('flip_source_side')} zone reclaim (valid) : {fmt(t.get('flip_reclaim_ts'))}  zone=[{t.get('flip_zone_lo'):.2f},{t.get('flip_zone_hi'):.2f}]")
            print(f"      {t.get('flip_source_side')} candle A (invalidating 15m close): {fmt(t.get('flip_candleA_ts'))}  low={t.get('flip_candleA_low'):.2f}")
            if t["tranche"] == "T1":
                print(f"      T1 (15m concept): fast trigger, {t['side']} tick > prev15_high={t.get('prev15_high'):.2f} -> entry @ {t['entry']:.2f}")
            else:
                print(f"      T2 (15m concept): breakout 15m {fmt(t.get('breakout_15m_ts'))} high={t['entry']:.2f} > prev15_high={t.get('prev15_high'):.2f}")
                print(f"      T2 (5m concept) : subzone found [{t.get('sub5m_lo'):.2f}, {t.get('sub5m_hi'):.2f}]")
        else:
            print(f"      zone ref candle       : {fmt(t.get('zone_ref_ts'))}")
            print(f"      zone sellers-in candle: {fmt(t.get('zone_sellers_in_ts'))}")
            print(f"      zone reclaim candle   : {fmt(t.get('zone_reclaim_ts'))}  zone=[{t.get('zone_lo'):.2f},{t.get('zone_hi'):.2f}]")
            print(f"      contact               : {fmt(t.get('contact_ts'))}")
            print(f"      15m concept: ref-candle {fmt(t.get('ref15_open'))} (closes {fmt(t.get('ref15_close'))}) H={t.get('ref15_high'):.2f} L={t.get('ref15_low'):.2f}, breach={fmt(t.get('breach_ts'))}")
            if t.get("sub5m_lo") is not None:
                print(f"      5m concept : subzone [{t.get('sub5m_lo'):.2f},{t.get('sub5m_hi'):.2f}] armed_ts={fmt(t.get('armed_ts'))}")
            else:
                print(f"      5m concept : no subzone -> raw breakout")
    print(f"\nn={len(all_trades)}  total PnL = Rs{total:+,.0f}")


if __name__ == "__main__":
    main()
