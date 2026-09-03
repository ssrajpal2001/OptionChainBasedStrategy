"""
1-month backtest of the FULL current mechanic in strategies/d1_trap_option/bear_only_book.py
(as of 2026-07-31: zone merge/collapse, zone_lo prevalidation+ongoing invalidation,
staircase TSL 10%/7%, Rs2000 SL cap, 09:35 early-session guard, fixed-reference
ref-candle roll-forward, and the new flip concept with fixed candle-A reference
+ ordered checks).

Reuses the EXACT pure functions from bear_only_book.py. Daily 200-pt-ITM CE/PE
strikes recomputed each day (ATM round-100); state persists across days for a
strike that repeats, resets fresh for a newly selected one.

PERF: resamples (5m/15m/60m) are computed ONCE per strike over its full history
(month + today), then sliced by timestamp -- not recomputed every 1-min bar.
"""
import sys, os
sys.path.insert(0, ".")
import strategies.d1_trap_option.bear_only_book as bb
import pandas as pd
from datetime import time, timedelta

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
TSL_BASE_PCT = bb._TSL_BASE_PCT
TSL_BASE_LOCK_PCT = bb._TSL_BASE_LOCK_PCT
TSL_STEP_PCT = bb._TSL_STEP_PCT
TSL_STEP_LOCK_PCT = bb._TSL_STEP_LOCK_PCT

_opt_cache = {}   # (strike,side) -> dict(m1, m5, m15, m60, bars60)


def load_option(strike: int, side: str) -> dict:
    key = (strike, side)
    if key not in _opt_cache:
        df = pd.read_parquet(f"{OPT_DIR}/{strike}_{side}_month.parquet")
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime").reset_index(drop=True)
        m5 = bb._resample(df, 5)
        m15 = bb._resample(df, 15)
        m60 = bb._resample(df, 60)
        _opt_cache[key] = dict(m1=df, m5=m5, m15=m15, m60=m60)
    return _opt_cache[key]


class SideState:
    def __init__(self, strike, side):
        self.strike = strike
        self.side = side
        self.zones = []
        self.flip_candidates = []
        self.position = None
        self.trades = []
        self.seen_lock_ts = set()
        self.last_15m_seen_ts = None


def refresh_intraday_zones(state: SideState, ts, warmup_start, data: dict):
    """Mirrors _process_new_bar's new-zone detection: rescan the warmup-window+today
    60m bars for newly-locked zones not already known, merge (don't replace)."""
    m1_window = data["m1"][(data["m1"]["datetime"].dt.date >= warmup_start) & (data["m1"]["datetime"] < ts)]
    if len(m1_window) < 30:
        return
    m60 = bb._resample(m1_window, 60)
    m15 = bb._resample(m1_window, 15)
    existing_lock_ts = {z["lock_ts"] for z in state.zones}

    def _overlaps_existing(z):
        return any(abs(z["zone_lo"] - e["zone_lo"]) <= 5.0 and abs(z["zone_hi"] - e["zone_hi"]) <= 5.0
                   for e in state.zones)

    new_zones = [z for z in bb._detect_bear_zones(bb._to_bars(m60))
                 if z["lock_ts"] not in existing_lock_ts and not _overlaps_existing(z)]
    if new_zones:
        new_zones = bb._prevalidate_zones(new_zones, m15)
        state.zones.extend(new_zones)


def warmup_zones(state: SideState, as_of_day, data: dict):
    """Mirrors _select_strikes_for_today: fresh rolling _HIST_WARMUP_DAYS-day
    window fetched/rebuilt every day, regardless of whether the strike repeats."""
    start = as_of_day - timedelta(days=HIST_WARMUP_DAYS)
    m1_hist = data["m1"][(data["m1"]["datetime"].dt.date >= start) & (data["m1"]["datetime"].dt.date < as_of_day)]
    if len(m1_hist) < 30:
        state.zones = []
        return
    m60_hist = bb._resample(m1_hist, 60)
    m15_hist = bb._resample(m1_hist, 15)
    state.zones = bb._prevalidate_zones(bb._detect_bear_zones(bb._to_bars(m60_hist)), m15_hist)


def on_new_15m_close(state: SideState, m15_bar, data: dict, other_state: "SideState"):
    """Called once per 15m bar close: zone invalidation + flip candidate creation.
    Candidate is created directly off the invalidating close -- the 5m-subzone
    check belongs to the OTHER side's entry trigger (process_flip_breakout),
    not a precondition for candle A itself."""
    for z in state.zones:
        if not z["done"] and not z["invalid"] and m15_bar.close < z["zone_lo"]:
            z["invalid"] = True
            z["invalid_ts"] = m15_bar.timestamp + timedelta(minutes=15)
            state.flip_candidates.append(dict(
                candleA_low=m15_bar.low, candleA_high=m15_bar.high, candleA_ts=m15_bar.timestamp,
                zone_lo=z["zone_lo"], zone_hi=z["zone_hi"], zone_lock_ts=z["lock_ts"],
                parent_zone=z, confirmed=False, cancelled=False,
                cancelled_ts=None, cancelled_reason=None,
            ))

    for fc in state.flip_candidates:
        if fc["confirmed"] or fc["cancelled"]:
            continue
        if m15_bar.timestamp <= fc["candleA_ts"]:
            continue
        if m15_bar.low < fc["candleA_low"]:
            continue   # Check 1: new low below fixed candle A -- no cancellation, keep waiting
        if fc["zone_lo"] <= m15_bar.close <= fc["zone_hi"]:   # Check 2 (only if Check 1 false)
            fc["cancelled"] = True
            fc["cancelled_ts"] = m15_bar.timestamp + timedelta(minutes=15)
            fc["cancelled_reason"] = f"closed back inside zone @ {m15_bar.close:.2f}"
            fc["parent_zone"]["invalid"] = False
            fc["parent_zone"]["revalidated_ts"] = fc["cancelled_ts"]


def process_flip_breakout(state: SideState, m15_bar, own_data: dict, flip_source: SideState):
    """state = the OTHER side (e.g. CE) being fast-tracked by flip_source's (PE) invalidation.
    Skips flip_source's own 60m-zone requirement; simplified trigger only:
    current 15m high > previous 15m high, + 5m subzone found in that breakout candle."""
    for fc in flip_source.flip_candidates:
        if fc["confirmed"] or fc["cancelled"]:
            continue
        if m15_bar.timestamp <= fc["candleA_ts"]:
            continue
        if m15_bar.low < fc["candleA_low"]:
            continue
        if fc["zone_lo"] <= m15_bar.close <= fc["zone_hi"]:
            continue
        idx = own_data["m15"].index[own_data["m15"]["timestamp"] == m15_bar.timestamp]
        if not len(idx) or idx[0] == 0:
            continue
        prev15 = own_data["m15"].iloc[idx[0] - 1]
        if m15_bar.high <= prev15["high"]:
            continue
        window_5m_o = own_data["m5"][(own_data["m5"]["timestamp"] >= m15_bar.timestamp) &
                                      (own_data["m5"]["timestamp"] < m15_bar.timestamp + timedelta(minutes=15))]
        collapse_o = bb._collapse_subzones(bb._to_bars(window_5m_o))
        if collapse_o is not None and state.position is None:
            fc["confirmed"] = True
            fc["confirmed_ts"] = m15_bar.timestamp + timedelta(minutes=15)
            audit = dict(
                flip_source_side=flip_source.side, flip_source_strike=flip_source.strike,
                flip_zone_lock_ts=fc["zone_lock_ts"], flip_zone_lo=fc["zone_lo"], flip_zone_hi=fc["zone_hi"],
                flip_candleA_ts=fc["candleA_ts"], flip_candleA_low=fc["candleA_low"],
                flip_invalid_ts=fc["parent_zone"].get("invalid_ts"),
                breakout_15m_ts=m15_bar.timestamp, prev15_high=prev15["high"],
                sub5m_lo=collapse_o[0], sub5m_hi=collapse_o[1],
            )
            open_position(state, m15_bar.high, m15_bar.low,
                           m15_bar.timestamp + timedelta(minutes=15), origin="flip", audit=audit)


def process_zones_tick(state: SideState, last_ts, last_low, last_high, data: dict):
    if last_ts.time() >= ENTRY_CUTOFF:
        return
    m15, m5 = data["m15"], data["m5"]
    for zone in state.zones:
        if zone["done"] or zone["invalid"]:
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
                zone["ref_open"] = ref.timestamp
                zone["ref_close_time"] = ref.timestamp + timedelta(minutes=15)
                zone["ref_high"], zone["ref_low"] = ref.high, ref.low
            continue
        if zone["breach_ts"] is None:
            if last_ts >= zone["ref_close_time"] and last_high >= zone["ref_high"]:
                zone["breach_ts"] = last_ts
                continue
            new_ref = bb.D1TrapBearOnlyBook._find_latest_closed_ref_bar(m15, last_ts)
            if new_ref is not None and new_ref.timestamp > zone["ref_open"]:
                zone["ref_open"] = new_ref.timestamp
                zone["ref_close_time"] = new_ref.timestamp + timedelta(minutes=15)
                zone["ref_high"], zone["ref_low"] = new_ref.high, new_ref.low
            continue
        if zone["sub_lo"] is None:
            window_5m = m5[(m5["timestamp"] >= zone["ref_open"]) & (m5["timestamp"] < zone["ref_close_time"])]
            collapse = bb._collapse_subzones(bb._to_bars(window_5m))
            if collapse is None:
                if state.position is None:
                    audit = dict(zone_lock_ts=zone["lock_ts"], zone_lo=zone["zone_lo"], zone_hi=zone["zone_hi"],
                                 contact_ts=zone["contact_ts"], ref_open=zone["ref_open"],
                                 ref_close_time=zone["ref_close_time"], ref_high=zone["ref_high"],
                                 ref_low=zone["ref_low"], breach_ts=zone["breach_ts"],
                                 sub5m_lo=None, sub5m_hi=None, armed_ts=None)
                    open_position(state, zone["ref_high"], zone["ref_low"], last_ts, origin="raw_breakout", audit=audit)
                zone["done"] = True
                return
            zone["sub_lo"], zone["sub_hi"] = collapse
            zone["sub_assigned_ts"] = last_ts
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
            if state.position is None:
                audit = dict(zone_lock_ts=zone["lock_ts"], zone_lo=zone["zone_lo"], zone_hi=zone["zone_hi"],
                             contact_ts=zone["contact_ts"], ref_open=zone["ref_open"],
                             ref_close_time=zone["ref_close_time"], ref_high=zone["ref_high"],
                             ref_low=zone["ref_low"], breach_ts=zone["breach_ts"],
                             sub5m_lo=zone["sub_lo"], sub5m_hi=zone["sub_hi"], armed_ts=zone.get("armed_ts"))
                open_position(state, zone["sub_hi"], zone["ref_low"], last_ts, origin="swing_breach", audit=audit)
            zone["done"] = True
            return


def open_position(state: SideState, entry_price, raw_sl, entry_ts, origin, audit=None):
    sl_buffered = raw_sl - SL_BUFFER_PTS
    max_risk_pts = MAX_RISK_RS_PER_LOT / LOT_SIZE
    sl_final = max(sl_buffered, entry_price - max_risk_pts)
    state.position = dict(entry_price=entry_price, sl=sl_final, entry_ts=entry_ts,
                           high_lock_pct=0.0, origin=origin, strike=state.strike, side=state.side,
                           audit=audit or {})


def check_exit(state: SideState, ltp: float, ts) -> None:
    pos = state.position
    if pos is None:
        return
    entry = pos["entry_price"]
    profit_pct = (ltp - entry) / entry
    if profit_pct >= TSL_BASE_PCT:
        num_steps = int((profit_pct - TSL_BASE_PCT) // TSL_STEP_PCT)
        calc_lock = TSL_BASE_LOCK_PCT + num_steps * TSL_STEP_LOCK_PCT
        pos["high_lock_pct"] = max(pos["high_lock_pct"], calc_lock)
    stop_price = entry * (1 + pos["high_lock_pct"]) if pos["high_lock_pct"] > 0 else pos["sl"]
    hit = ltp <= stop_price
    eod = ts.time() >= EOD_TIME
    if hit or eod:
        exit_price = stop_price if hit else ltp
        reason = ("tsl_hit" if pos["high_lock_pct"] > 0 else "sl_hit") if hit else "eod"
        pnl = (exit_price - entry) * LOT_SIZE
        trade = dict(strike=pos["strike"], side=pos["side"], origin=pos["origin"],
                      entry_ts=pos["entry_ts"], entry=entry, sl_initial=pos["sl"],
                      locked_pct=pos["high_lock_pct"], exit_ts=ts,
                      exit=exit_price, reason=reason, pnl=pnl)
        trade.update(pos["audit"])
        state.trades.append(trade)
        state.position = None


def main():
    spot = pd.read_parquet(SPOT_PATH)
    spot["datetime"] = pd.to_datetime(spot["datetime"])
    day_opens = spot.groupby(spot["datetime"].dt.date).first()

    all_trades = []

    for day, row in day_opens.iterrows():
        atm = round(row["open"] / ATM_ROUND_STEP) * ATM_ROUND_STEP
        ce_strike, pe_strike = int(atm - ITM_OFFSET_PTS), int(atm + ITM_OFFSET_PTS)

        warmup_start = day - timedelta(days=HIST_WARMUP_DAYS)
        ce_state = SideState(ce_strike, "CE")
        warmup_zones(ce_state, day, load_option(ce_strike, "CE"))
        pe_state = SideState(pe_strike, "PE")
        warmup_zones(pe_state, day, load_option(pe_strike, "PE"))

        ce_data = load_option(ce_strike, "CE")
        pe_data = load_option(pe_strike, "PE")
        ce_today = ce_data["m1"][ce_data["m1"]["datetime"].dt.date == day]
        pe_today = pe_data["m1"][pe_data["m1"]["datetime"].dt.date == day]
        ce_m15_today = ce_data["m15"][ce_data["m15"]["timestamp"].dt.date == day]
        pe_m15_today = pe_data["m15"][pe_data["m15"]["timestamp"].dt.date == day]
        ce_m60_today = ce_data["m60"][ce_data["m60"]["timestamp"].dt.date == day]
        pe_m60_today = pe_data["m60"][pe_data["m60"]["timestamp"].dt.date == day]

        max_len = max(len(ce_today), len(pe_today))
        ce_15_ptr, pe_15_ptr, ce_60_ptr, pe_60_ptr = 0, 0, 0, 0
        for i in range(max_len):
            if i < len(ce_today):
                bar = ce_today.iloc[i]
                ts = bar["datetime"]
                while ce_60_ptr < len(ce_m60_today) and ce_m60_today.iloc[ce_60_ptr]["timestamp"] + timedelta(minutes=60) <= ts:
                    refresh_intraday_zones(ce_state, ce_m60_today.iloc[ce_60_ptr]["timestamp"] + timedelta(minutes=60), warmup_start, ce_data)
                    ce_60_ptr += 1
                while ce_15_ptr < len(ce_m15_today) and ce_m15_today.iloc[ce_15_ptr]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = ce_m15_today.iloc[ce_15_ptr]
                    on_new_15m_close(ce_state, m15row, ce_data, pe_state)
                    process_flip_breakout(ce_state, m15row, ce_data, pe_state)
                    ce_15_ptr += 1
                if ce_state.position is not None:
                    check_exit(ce_state, bar["close"], ts)
                else:
                    process_zones_tick(ce_state, ts, bar["low"], bar["high"], ce_data)
            if i < len(pe_today):
                bar = pe_today.iloc[i]
                ts = bar["datetime"]
                while pe_60_ptr < len(pe_m60_today) and pe_m60_today.iloc[pe_60_ptr]["timestamp"] + timedelta(minutes=60) <= ts:
                    refresh_intraday_zones(pe_state, pe_m60_today.iloc[pe_60_ptr]["timestamp"] + timedelta(minutes=60), warmup_start, pe_data)
                    pe_60_ptr += 1
                while pe_15_ptr < len(pe_m15_today) and pe_m15_today.iloc[pe_15_ptr]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = pe_m15_today.iloc[pe_15_ptr]
                    on_new_15m_close(pe_state, m15row, pe_data, ce_state)
                    process_flip_breakout(pe_state, m15row, pe_data, ce_state)
                    pe_15_ptr += 1
                if pe_state.position is not None:
                    check_exit(pe_state, bar["close"], ts)
                else:
                    process_zones_tick(pe_state, ts, bar["low"], bar["high"], pe_data)

        if ce_state.position is not None and not ce_today.empty:
            last = ce_today.iloc[-1]
            check_exit(ce_state, last["close"], last["datetime"].replace(hour=15, minute=15))
        if pe_state.position is not None and not pe_today.empty:
            last = pe_today.iloc[-1]
            check_exit(pe_state, last["close"], last["datetime"].replace(hour=15, minute=15))

        all_trades.extend(ce_state.trades)
        all_trades.extend(pe_state.trades)
        for s in (ce_state, pe_state):
            n_inv = sum(1 for z in s.zones if z["invalid"])
            n_fc = len(s.flip_candidates)
            n_conf = sum(1 for fc in s.flip_candidates if fc["confirmed"])
            n_cxl = sum(1 for fc in s.flip_candidates if fc["cancelled"])
            if n_inv or n_fc:
                print(f"[diag] {day} {s.strike}{s.side}: zones={len(s.zones)} invalid={n_inv} "
                      f"flip_candidates={n_fc} confirmed={n_conf} cancelled={n_cxl}")

    all_trades.sort(key=lambda t: t["entry_ts"])

    def fmt(ts):
        return ts.strftime("%m-%d %H:%M") if ts is not None and pd.notna(ts) else "-"

    print("\n" + "=" * 220)
    print("FULL AUDIT TRAIL -- one row per trade, in chronological order")
    print("=" * 220)
    for t in all_trades:
        print(f"\n### {t['strike']}{t['side']}  [{t['origin'].upper()}]  entry={t['entry']:.1f}@{fmt(t['entry_ts'])}"
              f"  exit={t['exit']:.1f}@{fmt(t['exit_ts'])}  ({t['reason']})  PnL=Rs{t['pnl']:+,.0f}")
        if t["origin"] == "flip":
            print(f"    FLIP CONCEPT TRADE -- triggered by {t.get('flip_source_strike')}{t.get('flip_source_side')} invalidation:")
            print(f"      parent-zone lock_ts (60m trap confirmed) : {fmt(t.get('flip_zone_lock_ts'))}  "
                  f"zone=[{t.get('flip_zone_lo'):.1f}, {t.get('flip_zone_hi'):.1f}]")
            print(f"      candle A (invalidating 15m close)        : {fmt(t.get('flip_candleA_ts'))}  "
                  f"low={t.get('flip_candleA_low'):.1f}  (zone invalid @ {fmt(t.get('flip_invalid_ts'))})")
            print(f"      this side's breakout 15m candle          : {fmt(t.get('breakout_15m_ts'))}  "
                  f"(prev15 high={t.get('prev15_high'):.1f})")
            print(f"      5m subzone in breakout candle            : [{t.get('sub5m_lo'):.1f}, {t.get('sub5m_hi'):.1f}]")
            print(f"      entry = breakout candle high, SL = breakout candle low")
        else:
            print(f"      60m zone locked (trap ref candle)  : {fmt(t.get('zone_lock_ts'))}  "
                  f"zone=[{t.get('zone_lo'):.1f}, {t.get('zone_hi'):.1f}]")
            print(f"      price entered zone (contact)       : {fmt(t.get('contact_ts'))}")
            print(f"      15m ref-candle assigned            : {fmt(t.get('ref_open'))}  "
                  f"(closes {fmt(t.get('ref_close_time'))})  ref_high={t.get('ref_high'):.1f} ref_low={t.get('ref_low'):.1f}")
            print(f"      ref-candle high breached (trigger) : {fmt(t.get('breach_ts'))}")
            if t.get("sub5m_lo") is not None:
                print(f"      5m subzone found -> armed          : sub=[{t.get('sub5m_lo'):.1f}, {t.get('sub5m_hi'):.1f}]  "
                      f"armed_ts={fmt(t.get('armed_ts'))}")
                print(f"      swing-break entry (sub_hi cleared) : entry={t['entry']:.1f} @ {fmt(t['entry_ts'])}")
            else:
                print(f"      no 5m subzone found -> RAW BREAKOUT fallback entry @ ref_high, immediately on ref breach")
        print(f"      SL (initial, buffered -{SL_BUFFER_PTS}pts, capped Rs{MAX_RISK_RS_PER_LOT}/lot) : {t['sl_initial']:.1f}")
        if t["locked_pct"] > 0:
            print(f"      staircase TSL locked in @ {t['locked_pct']*100:.1f}% of entry -> exit stop = "
                  f"{t['entry']*(1+t['locked_pct']):.1f}")
        print(f"      exit reason: {t['reason']}" + (f"  (forced EOD 15:15)" if t['reason'] == "eod" else ""))

    print("\n" + "=" * 130)
    print(f"{'Date':<8}{'Strike':>7}{'Side':>5}{'Origin':>13}{'Entry':>8}{'EntryTS':>13}"
          f"{'Exit':>8}{'ExitTS':>13}{'Reason':>9}{'SL':>8}{'TSL%':>6}{'PnL':>9}")
    print("-" * 130)
    total = 0
    for t in all_trades:
        total += t["pnl"]
        print(f"{t['entry_ts'].strftime('%m-%d'):<8}{t['strike']:>7}{t['side']:>5}{t['origin']:>13}"
              f"{t['entry']:>8.1f}{fmt(t['entry_ts']):>13}{t['exit']:>8.1f}{fmt(t['exit_ts']):>13}"
              f"{t['reason']:>9}{t['sl_initial']:>8.1f}{t['locked_pct']*100:>5.1f}%{t['pnl']:>+9.0f}")
    wins = [t for t in all_trades if t["pnl"] > 0]
    losses = [t for t in all_trades if t["pnl"] <= 0]
    gw = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    pf = gw / gl if gl > 0 else (99 if gw > 0 else 0)
    print(f"\nn={len(all_trades)}  win%={100*len(wins)/len(all_trades) if all_trades else 0:.1f}  "
          f"Rs{total:+,.0f}  PF={pf:.2f}")
    flip_trades = [t for t in all_trades if t["origin"] == "flip"]
    print(f"flip-origin trades: {len(flip_trades)}  PnL from flips: Rs{sum(t['pnl'] for t in flip_trades):+,.0f}")
    raw_trades = [t for t in all_trades if t["origin"] != "flip"]
    print(f"non-flip trades: {len(raw_trades)}  PnL: Rs{sum(t['pnl'] for t in raw_trades):+,.0f}")


if __name__ == "__main__":
    main()
