"""Replay the ACTUAL live bear_only_book mechanic for TODAY's real SENSEX
strikes (CE78600, PE79200 -- confirmed from the live strike-selection log),
using real 1-min data for both sides together (interleaved), so flip
concept cross-references work exactly as they do live. Reports every
zone/contact/ref/breach/flip/entry event for BOTH sides, to give a
definitive, code-verified account of what did/should have happened today."""
import sys
sys.path.insert(0, ".")
from datetime import timedelta, date
import pandas as pd
import strategies.d1_trap_option.bear_only_book as bb

SCRATCH = r"C:\Users\SERVER\AppData\Local\Temp\claude\e--AlgoSoft-OptionChainBasedStrategy\3f952902-2e64-455f-be1c-fac0a7378cbc\scratchpad"
HTF_MIN = 15
DAY = date(2026, 8, 3)
EARLY_CUTOFF = bb._EARLY_SESSION_CUTOFF
ENTRY_CUTOFF = bb._ENTRY_CUTOFF
SL_BUFFER = bb._SL_BUFFER_PTS
MAX_RISK_RS = bb._MAX_RISK_RS_PER_LOT
LOT_SIZE = 20


def load(strike, side):
    hist = pd.read_parquet(f"{SCRATCH}/sensex{strike}{side.lower()}_hist.parquet").rename(columns={"ts": "datetime"})
    today = pd.read_parquet(f"{SCRATCH}/sensex{strike}{side.lower()}_today.parquet").rename(columns={"ts": "datetime"})
    df = pd.concat([hist, today]).sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)
    return df


class State:
    def __init__(self, side, strike):
        self.side, self.strike = side, strike
        self.zones = []
        self.flip_candidates = []
        self.positions = []
        self.prev15_high = None
        self.prev15_low = None


def warmup(state, df1m):
    hist = df1m[df1m["datetime"].dt.date < DAY]
    m_htf_hist = bb._resample(hist, HTF_MIN)
    m15_hist = bb._resample(hist, 15)
    state.zones = bb._prevalidate_zones(bb._detect_bear_zones(bb._to_bars(m_htf_hist)), m15_hist)
    print(f"{state.side}{state.strike}: warmup -> {len(state.zones)} zones")


def on_new_15m_close(state, m15_bar):
    for z in state.zones:
        if not z["done"] and not z["invalid"] and m15_bar.close < z["zone_lo"]:
            z["invalid"] = True
            print(f"{m15_bar.timestamp}  {state.side}{state.strike} zone [{z['zone_lo']:.2f},{z['zone_hi']:.2f}] "
                  f"INVALIDATED -- 15m close {m15_bar.close:.2f}")
            state.flip_candidates.append(dict(
                candleA_low=m15_bar.low, candleA_high=m15_bar.high, candleA_ts=m15_bar.timestamp,
                zone_lo=z["zone_lo"], zone_hi=z["zone_hi"], lock_ts=z["lock_ts"],
                confirmed=False, cancelled=False, t1_taken=False,
            ))
    for fc in state.flip_candidates:
        if fc["confirmed"] or fc["cancelled"] or m15_bar.timestamp <= fc["candleA_ts"]:
            continue
        if m15_bar.low < fc["candleA_low"]:
            continue
        if fc["zone_lo"] <= m15_bar.close <= fc["zone_hi"]:
            fc["cancelled"] = True
            print(f"{m15_bar.timestamp}  {state.side}{state.strike} FLIP CANDIDATE cancelled (re-validated)")


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
        sl_buffered = state.prev15_low - SL_BUFFER
        hard_sl = ltp - MAX_RISK_RS / LOT_SIZE
        sl_final = max(sl_buffered, hard_sl)
        print(f"{ts}  *** {state.side}{state.strike} FLIP T1 (fast) *** entry={ltp:.2f} > prev15_high={state.prev15_high:.2f} "
              f"sl={sl_final:.2f} (buffered={sl_buffered:.2f} hard_cap={hard_sl:.2f}) "
              f"(triggered by {flip_source.side} candle-A @ {fc['candleA_ts']})")
        state.positions.append(dict(side=state.side, entry=ltp, sl=sl_final, ts=ts, high_lock_pct=0.0,
                                     tsl_base_pct=bb._TSL_TRANCHE_BASE_PCT, tsl_base_lock_pct=bb._TSL_TRANCHE_BASE_LOCK_PCT,
                                     tsl_step_pct=bb._TSL_TRANCHE_STEP_PCT, tsl_step_lock_pct=bb._TSL_TRANCHE_STEP_LOCK_PCT))
        return


def process_zones_tick(state, last_ts, last_low, last_high, m15, m5, other_state):
    if last_ts.time() >= ENTRY_CUTOFF:
        return
    active_locks = {p.get("lock_ts") for p in state.positions}
    for zone in state.zones:
        if zone["done"] or zone["invalid"]:
            continue
        if (state.positions and zone["lock_ts"] not in active_locks) or (not state.positions and other_state.positions):
            continue
        if zone["state"] == "WAITING":
            if last_low <= zone["zone_hi"]:
                zone["state"] = "MONITORING"
                print(f"{last_ts}  {state.side}{state.strike} CONTACT -> MONITORING zone=[{zone['zone_lo']:.2f},{zone['zone_hi']:.2f}]")
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
                print(f"{last_ts}  {state.side}{state.strike} REF ASSIGNED {ref.timestamp} H={ref.high:.2f} L={ref.low:.2f}")
            continue
        if zone["breach_ts"] is None:
            if last_ts >= zone["ref_close_time"] and last_high >= zone["ref_high"]:
                zone["breach_ts"] = last_ts
                sl_final = max(zone["ref_low"] - SL_BUFFER, zone["ref_high"] - MAX_RISK_RS / LOT_SIZE)
                print(f"{last_ts}  *** {state.side}{state.strike} T1 BREACH/ENTRY *** entry={zone['ref_high']:.2f} sl={sl_final:.2f}")
                state.positions.append(dict(side=state.side, entry=zone["ref_high"], sl=sl_final, ts=last_ts, lock_ts=zone["lock_ts"]))
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
                continue
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
            sl_final = max(zone["ref_low"] - SL_BUFFER, zone["ref_high"] - MAX_RISK_RS / LOT_SIZE)
            print(f"{last_ts}  *** {state.side}{state.strike} T2 SWING-BREACH ENTRY *** entry={zone['sub_hi']:.2f} sl={sl_final:.2f}")
            state.positions.append(dict(side=state.side, entry=zone["sub_hi"], sl=sl_final, ts=last_ts))
            zone["done"] = True


ce_df = load(78600, "CE")
pe_df = load(79200, "PE")
ce_state, pe_state = State("CE", 78600), State("PE", 79200)
warmup(ce_state, ce_df)
warmup(pe_state, pe_df)

ce_today = ce_df[ce_df["datetime"].dt.date == DAY].reset_index(drop=True)
pe_today = pe_df[pe_df["datetime"].dt.date == DAY].reset_index(drop=True)
ce_m5, ce_m15 = bb._resample(ce_df, 5), bb._resample(ce_df, 15)
pe_m5, pe_m15 = bb._resample(pe_df, 5), bb._resample(pe_df, 15)
ce_m15_today = ce_m15[ce_m15["timestamp"].dt.date == DAY].reset_index(drop=True)
pe_m15_today = pe_m15[pe_m15["timestamp"].dt.date == DAY].reset_index(drop=True)

print(f"\nCE today bars: {len(ce_today)}  PE today bars: {len(pe_today)}\n")

max_len = max(len(ce_today), len(pe_today))
ce15 = pe15 = 0
for i in range(max_len):
    if i < len(ce_today):
        bar = ce_today.iloc[i]; ts = bar["datetime"]
        while ce15 < len(ce_m15_today) and ce_m15_today.iloc[ce15]["timestamp"] + timedelta(minutes=15) <= ts:
            row = ce_m15_today.iloc[ce15]
            ce_state.prev15_high, ce_state.prev15_low = row["high"], row["low"]
            on_new_15m_close(ce_state, row)
            ce15 += 1
        check_fast_t1(ce_state, bar["high"], ts, pe_state)
        process_zones_tick(ce_state, ts, bar["low"], bar["high"], ce_m15, ce_m5, pe_state)
        for pos in list(ce_state.positions):
            if pos.get("closed"):
                continue
            entry = pos["entry"]
            ltp = bar["close"]
            profit_pct = (ltp - entry) / entry
            if profit_pct >= pos["tsl_base_pct"]:
                steps = int((profit_pct - pos["tsl_base_pct"]) // pos["tsl_step_pct"])
                calc_lock = pos["tsl_base_lock_pct"] + steps * pos["tsl_step_lock_pct"]
                pos["high_lock_pct"] = max(pos["high_lock_pct"], calc_lock)
            stop = entry * (1 + pos["high_lock_pct"]) if pos["high_lock_pct"] > 0 else pos["sl"]
            if ltp <= stop:
                reason = "tsl_hit" if pos["high_lock_pct"] > 0 else "sl_hit"
                pos["closed"] = True
                print(f"{ts}  *** CE78600 EXIT ({reason}) *** ltp={ltp:.2f} stop={stop:.2f} "
                      f"pnl_pts={ltp-entry:+.2f}")
    if i < len(pe_today):
        bar = pe_today.iloc[i]; ts = bar["datetime"]
        while pe15 < len(pe_m15_today) and pe_m15_today.iloc[pe15]["timestamp"] + timedelta(minutes=15) <= ts:
            row = pe_m15_today.iloc[pe15]
            pe_state.prev15_high, pe_state.prev15_low = row["high"], row["low"]
            on_new_15m_close(pe_state, row)
            pe15 += 1
        check_fast_t1(pe_state, bar["high"], ts, ce_state)
        process_zones_tick(pe_state, ts, bar["low"], bar["high"], pe_m15, pe_m5, ce_state)

print(f"\nCE positions taken: {ce_state.positions}")
print(f"PE positions taken: {pe_state.positions}")
