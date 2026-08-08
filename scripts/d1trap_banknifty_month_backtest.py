"""
scripts/d1trap_banknifty_month_backtest.py — real-data BANKNIFTY backtest,
2026-07-01 through today, against a SINGLE fixed contract throughout: the
August monthly expiry (per direct user request) -- avoids mid-window
expiry-rollover contamination, same discipline as every other multi-week
real-data backtest this session (see e.g. FVG's next-week-expiry test).
Resolved live via REGISTRY.get_active_expiry("BANKNIFTY") -- whatever
monthly contract is currently active is printed at the top of the run so
you can visually confirm it's the one you expect before reading results.

Runs BOTH concepts on the SAME real data, SAME daily strikes, SAME outer
60m HTF zones (built once per day, shared by both):

1. LIVE T1/T2 MECHANIC — byte-identical reuse of the pure functions in
   strategies/d1_trap_option/bear_only_book.py (bb): 60m HTF zone
   detection (bb._detect_bear_zones), 15m ref-candle, 5m subzone, arm,
   swing-breach, staircase TSL, Rs2000/lot risk cap, flip concept on zone
   invalidation. Day-loop structure is the SAME as
   scripts/d1trap_month_backtest_v2.py (built for NIFTY off pre-cached
   parquet) -- copied here (not imported) because that script hardcodes
   NIFTY's lot size/ITM offset at module level; this version live-fetches
   BANKNIFTY data at runtime and uses BANKNIFTY's own strike step/lot size.

2. SUPPORT & RESISTANCE MECHANIC — the 2026-08-07 "ping-pong" S&R tracker
   (strategies/d1_trap_option/support_resistance.py), reusing
   _run_sr_variant + all 4 exit-mode variants (raw/bucket_close/buffered/
   profit_trigger, all risk-capped) directly from
   scripts/d1trap_sr_zone_backtest.py -- run once per day per side per
   (tf, exit_mode), fed the SAME zones the T1/T2 mechanic sees that day.
   Results are logged into the SAME data/d1trap_sr_exit_variant_log.jsonl
   used by the NIFTY/SENSEX daily runs (dated per actual trading day, not
   "today"), so the running track record printed by
   scripts/d1trap_sr_zone_backtest.py's _print_variant_track_record()
   becomes a real cross-underlying comparison once this has run.

Per-day ATM CE/PE strike selection uses the SAME "3-ITM-step" pattern
CLAUDE.md documents for NIFTY (150pts = 3x50 grid) and SENSEX (300pts =
3x100 grid) -- extrapolated to BANKNIFTY's own 100pt grid -> 300pts. This
is an ASSUMPTION by extrapolation, not a validated BANKNIFTY-specific
value (CLAUDE.md itself says the 3-ITM depth was "not tested beyond" the
two underlyings it was actually swept on).

Both mechanics rebuild their zones FRESH every day from a rolling
14-calendar-day window (bb._HIST_WARMUP_DAYS), even when a strike repeats
across days -- this is not a simplification, it's what the live book
itself does (D1TrapBearOnlyBook re-selects/re-scans per day; see
d1trap_month_backtest_v2.py's own docstring on this point).

Run on the box with a real Upstox access_token (data/clients.db) --
expect this to take a while: BANKNIFTY can range widely over 5+ weeks, so
this may need to fetch a real month+ of 1-minute data for 15-30+ distinct
strikes:
    python3 scripts/d1trap_banknifty_month_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402

from config.global_config import GlobalConfig, IST  # noqa: E402
from data_layer.client_db import ClientDB  # noqa: E402
from data_layer.instrument_registry import REGISTRY  # noqa: E402
import strategies.d1_trap_option.bear_only_book as bb  # noqa: E402
from strategies.d1_trap_option.book import _fetch_1m_bars, _upstox_key_for  # noqa: E402
from scripts.d1trap_sr_zone_backtest import (  # noqa: E402
    _run_sr_variant,
    _EXIT_MODES,
    _append_variant_log,
    _print_variant_track_record,
)

UNDERLYING = "BANKNIFTY"
START_DATE = date(2026, 7, 1)
LOT_SIZE = 30
ITM_OFFSET_PTS = 300   # extrapolated 3-ITM-step on BANKNIFTY's 100pt grid -- see docstring
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
SR_TF_SWEEP = (1, 3, 5)

_opt_cache: Dict[tuple, Optional[dict]] = {}


def _bars_to_df(bars: list) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame(columns=["datetime", "open", "high", "low", "close"])
    return pd.DataFrame([
        {"datetime": b.timestamp, "open": b.open, "high": b.high, "low": b.low, "close": b.close}
        for b in bars
    ])


async def load_option(strike: int, side: str, expiry: date, fetch_start: date, fetch_end: date,
                       token: str) -> Optional[dict]:
    """Fetch (or return cached) full-range 1m/5m/15m/60m data for one (strike, side)
    against the fixed expiry. Cached across the whole run -- a repeated ATM level
    across multiple days only ever triggers one real fetch."""
    key = (strike, side)
    if key in _opt_cache:
        return _opt_cache[key]
    opt_key = REGISTRY.get_upstox_key(UNDERLYING, expiry, strike, side)
    if not opt_key:
        print(f"    [skip] {strike}{side}: no Upstox instrument key for expiry {expiry}.")
        _opt_cache[key] = None
        return None
    bars = await asyncio.to_thread(_fetch_1m_bars, opt_key, fetch_start, fetch_end, token)
    if not bars:
        print(f"    [skip] {strike}{side}: no real premium data returned.")
        _opt_cache[key] = None
        return None
    df = _bars_to_df(bars)
    data = dict(m1=df, m5=bb._resample(df, 5), m15=bb._resample(df, 15), m60=bb._resample(df, 60))
    _opt_cache[key] = data
    print(f"    [fetched] {strike}{side}: {len(df)} real 1m bars ({fetch_start} .. {fetch_end}).")
    return data


class SideState:
    def __init__(self, strike, side):
        self.strike = strike
        self.side = side
        self.zones: List[dict] = []
        self.flip_candidates: List[dict] = []
        self.position: Optional[dict] = None
        self.trades: List[dict] = []
        # 2026-08-08 fix, mirroring bear_only_book.py's own 2026-08-07 live fix
        # (commit eb23992, "CRITICAL: fix BearTrap double-firing a real entry on
        # the same reference candle"): a DIFFERENT zone dict object (distinct per
        # the 60m zone-detection dedup key, e.g. one from warmup_zones and another
        # discovered later via refresh_intraday_zones) can independently reach its
        # own ref-candle assignment and get handed the SAME real 15m ref candle
        # (_find_latest_closed_ref_bar has no notion of "which zone is asking").
        # Confirmed in this script's own first real BANKNIFTY run: the SAME entry
        # price fired 2-7 times in a row, each closing (sl_hit) within 1-2 minutes
        # before the next near-duplicate zone re-fired -- ~Rs59,700 of pure
        # duplicate-churn loss embedded in a Rs43,286 net "loss" that first run
        # reported. Reset fresh every day (new SideState per day, same as live's
        # reset_session()).
        self.fired_ref_opens: set = set()


def warmup_zones(state: SideState, as_of_day: date, data: dict) -> None:
    """Fresh rolling HIST_WARMUP_DAYS-day window, rebuilt every day regardless of
    whether the strike repeats -- mirrors D1TrapBearOnlyBook's own daily strike
    (re)selection, same as d1trap_month_backtest_v2.py."""
    start = as_of_day - timedelta(days=HIST_WARMUP_DAYS)
    m1_hist = data["m1"][(data["m1"]["datetime"].dt.date >= start) & (data["m1"]["datetime"].dt.date < as_of_day)]
    if len(m1_hist) < 30:
        state.zones = []
        return
    m60_hist = bb._resample(m1_hist, 60)
    m15_hist = bb._resample(m1_hist, 15)
    state.zones = bb._prevalidate_zones(bb._detect_bear_zones(bb._to_bars(m60_hist)), m15_hist)


def refresh_intraday_zones(state: SideState, ts, warmup_start: date, data: dict) -> None:
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


def on_new_15m_close(state: SideState, m15_bar, other_state: "SideState") -> None:
    for z in state.zones:
        if not z["done"] and not z["invalid"] and m15_bar.close < z["zone_lo"]:
            z["invalid"] = True
            z["invalid_ts"] = m15_bar.timestamp + timedelta(minutes=15)
            state.flip_candidates.append(dict(
                candleA_low=m15_bar.low, candleA_high=m15_bar.high, candleA_ts=m15_bar.timestamp,
                zone_lo=z["zone_lo"], zone_hi=z["zone_hi"], zone_lock_ts=z["lock_ts"],
                parent_zone=z, confirmed=False, cancelled=False, cancelled_ts=None, cancelled_reason=None,
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
            fc["cancelled_ts"] = m15_bar.timestamp + timedelta(minutes=15)
            fc["parent_zone"]["invalid"] = False


def process_flip_breakout(state: SideState, m15_bar, own_data: dict, flip_source: SideState) -> None:
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
            audit = dict(origin_detail=f"flip from {flip_source.strike}{flip_source.side}")
            open_position(state, m15_bar.high, m15_bar.low,
                           m15_bar.timestamp + timedelta(minutes=15), origin="flip", audit=audit)


def process_zones_tick(state: SideState, last_ts, last_low, last_high, data: dict) -> None:
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
                # 2026-08-08 fix: refuse a second real entry off the same reference
                # candle, even from a different zone object -- see SideState.fired_ref_opens.
                if state.position is None and zone["ref_open"] not in state.fired_ref_opens:
                    audit = dict()
                    open_position(state, zone["ref_high"], zone["ref_low"], last_ts,
                                   origin="raw_breakout", audit=audit)
                    state.fired_ref_opens.add(zone["ref_open"])
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
            if state.position is None and zone["ref_open"] not in state.fired_ref_opens:
                open_position(state, zone["sub_hi"], zone["ref_low"], last_ts, origin="swing_breach", audit=dict())
                state.fired_ref_opens.add(zone["ref_open"])
            zone["done"] = True
            return


def open_position(state: SideState, entry_price, raw_sl, entry_ts, origin, audit=None) -> None:
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
        trade = dict(strike=pos["strike"], side=pos["side"], origin=pos["origin"], entry_ts=pos["entry_ts"],
                      entry=entry, sl_initial=pos["sl"], locked_pct=pos["high_lock_pct"], exit_ts=ts,
                      exit=exit_price, reason=reason, pnl=pnl)
        state.trades.append(trade)
        state.position = None


def run_t1t2_day(day: date, warmup_start: date, ce_state: SideState, pe_state: SideState,
                  ce_data: dict, pe_data: dict) -> None:
    ce_today = ce_data["m1"][ce_data["m1"]["datetime"].dt.date == day]
    pe_today = pe_data["m1"][pe_data["m1"]["datetime"].dt.date == day]
    ce_m15_today = ce_data["m15"][ce_data["m15"]["timestamp"].dt.date == day]
    pe_m15_today = pe_data["m15"][pe_data["m15"]["timestamp"].dt.date == day]
    ce_m60_today = ce_data["m60"][ce_data["m60"]["timestamp"].dt.date == day]
    pe_m60_today = pe_data["m60"][pe_data["m60"]["timestamp"].dt.date == day]

    max_len = max(len(ce_today), len(pe_today))
    ce_15_ptr = pe_15_ptr = ce_60_ptr = pe_60_ptr = 0
    for i in range(max_len):
        if i < len(ce_today):
            bar = ce_today.iloc[i]
            ts = bar["datetime"]
            while ce_60_ptr < len(ce_m60_today) and ce_m60_today.iloc[ce_60_ptr]["timestamp"] + timedelta(minutes=60) <= ts:
                refresh_intraday_zones(ce_state, ce_m60_today.iloc[ce_60_ptr]["timestamp"] + timedelta(minutes=60), warmup_start, ce_data)
                ce_60_ptr += 1
            while ce_15_ptr < len(ce_m15_today) and ce_m15_today.iloc[ce_15_ptr]["timestamp"] + timedelta(minutes=15) <= ts:
                m15row = ce_m15_today.iloc[ce_15_ptr]
                on_new_15m_close(ce_state, m15row, pe_state)
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
                on_new_15m_close(pe_state, m15row, ce_state)
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


def run_sr_day(day: date, zones: List[dict], side_label: str, day_1m: "pd.DataFrame", lot_size: int) -> None:
    """Runs the S&R mechanic once per (tf, exit_mode) using the SAME zones the
    T1/T2 mechanic saw that day, and logs results into the shared JSONL track
    record (data/d1trap_sr_exit_variant_log.jsonl) alongside NIFTY/SENSEX."""
    from strategies.d1_trap_option.bear_only_book import _Bar
    bars = [_Bar(timestamp=r["datetime"], open=r["open"], high=r["high"], low=r["low"], close=r["close"])
            for r in day_1m.to_dict("records")]
    if not bars:
        return
    log_records = []
    for tf in SR_TF_SWEEP:
        for mode in _EXIT_MODES:
            result = _run_sr_variant(zones, bars, tf, lot_size, exit_mode=mode)
            log_records.append({
                "date": day.isoformat(), "underlying": UNDERLYING, "side": side_label,
                "tf_minutes": tf, "exit_mode": mode,
                "pnl": result.get("pnl"), "entry_ts": str(result.get("entry_ts", "")),
                "exit_reason": result.get("exit_reason"), "no_entry": bool(result.get("no_entry")),
            })
    _append_variant_log(log_records)


async def main() -> int:
    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    token = (creds or {}).get("access_token", "")
    if not token:
        print("FATAL: no Upstox access_token in data/clients.db.")
        return 1

    today = datetime.now(IST).date()
    try:
        await asyncio.to_thread(REGISTRY.load_sync, UNDERLYING, token)
    except Exception as exc:
        print(f"FATAL: REGISTRY.load_sync({UNDERLYING}) failed: {exc}")
        return 1
    expiry = REGISTRY.get_active_expiry(UNDERLYING)
    if not expiry:
        print("FATAL: could not resolve an active BANKNIFTY expiry.")
        return 1
    print(f"Resolved expiry: {expiry}  (confirm this is the August monthly you expect)")

    spot_key = _upstox_key_for(UNDERLYING)
    fetch_start = START_DATE - timedelta(days=HIST_WARMUP_DAYS)
    print(f"Fetching real BANKNIFTY spot 1m bars {fetch_start} .. {today} ...")
    spot_bars = await asyncio.to_thread(_fetch_1m_bars, spot_key, fetch_start, today, token)
    if not spot_bars:
        print("FATAL: no real spot data returned.")
        return 1
    spot_df = _bars_to_df(spot_bars)
    trading_days = sorted(d for d in spot_df["datetime"].dt.date.unique() if START_DATE <= d <= today)
    print(f"{len(trading_days)} real trading day(s) in range: {trading_days[0]} .. {trading_days[-1]}")

    all_t1t2_trades: List[dict] = []
    ce_state: Optional[SideState] = None
    pe_state: Optional[SideState] = None

    for day in trading_days:
        day_opens = spot_df[spot_df["datetime"].dt.date == day]
        if day_opens.empty:
            continue
        spot_open = float(day_opens.iloc[0]["open"])
        atm = round(spot_open / ATM_ROUND_STEP) * ATM_ROUND_STEP
        ce_strike, pe_strike = int(atm - ITM_OFFSET_PTS), int(atm + ITM_OFFSET_PTS)
        print(f"\n{day}: real spot open={spot_open:.2f} ATM={atm} -> CE{ce_strike} / PE{pe_strike}")

        ce_data = await load_option(ce_strike, "CE", expiry, fetch_start, today, token)
        pe_data = await load_option(pe_strike, "PE", expiry, fetch_start, today, token)
        if ce_data is None or pe_data is None:
            print("  SKIP this day -- missing real premium data for one or both strikes.")
            continue

        warmup_start = day - timedelta(days=HIST_WARMUP_DAYS)
        ce_state = SideState(ce_strike, "CE")
        warmup_zones(ce_state, day, ce_data)
        pe_state = SideState(pe_strike, "PE")
        warmup_zones(pe_state, day, pe_data)
        print(f"  zones today: CE={len(ce_state.zones)} PE={len(pe_state.zones)}")

        run_t1t2_day(day, warmup_start, ce_state, pe_state, ce_data, pe_data)
        all_t1t2_trades.extend(ce_state.trades)
        all_t1t2_trades.extend(pe_state.trades)

        ce_today_1m = ce_data["m1"][ce_data["m1"]["datetime"].dt.date == day]
        pe_today_1m = pe_data["m1"][pe_data["m1"]["datetime"].dt.date == day]
        run_sr_day(day, ce_state.zones, f"CE{ce_strike}", ce_today_1m, LOT_SIZE)
        run_sr_day(day, pe_state.zones, f"PE{pe_strike}", pe_today_1m, LOT_SIZE)

    # ---- T1/T2 report ----
    all_t1t2_trades.sort(key=lambda t: t["entry_ts"])

    def fmt(ts):
        return ts.strftime("%m-%d %H:%M") if ts is not None and pd.notna(ts) else "-"

    print("\n" + "=" * 130)
    print("T1/T2 MECHANIC -- FULL TRADE LIST (chronological)")
    print("=" * 130)
    print(f"{'Date':<8}{'Strike':>7}{'Side':>5}{'Origin':>13}{'Entry':>8}{'EntryTS':>13}"
          f"{'Exit':>8}{'ExitTS':>13}{'Reason':>9}{'SL':>8}{'TSL%':>6}{'PnL':>9}")
    print("-" * 130)
    total = 0.0
    for t in all_t1t2_trades:
        total += t["pnl"]
        print(f"{t['entry_ts'].strftime('%m-%d'):<8}{t['strike']:>7}{t['side']:>5}{t['origin']:>13}"
              f"{t['entry']:>8.1f}{fmt(t['entry_ts']):>13}{t['exit']:>8.1f}{fmt(t['exit_ts']):>13}"
              f"{t['reason']:>9}{t['sl_initial']:>8.1f}{t['locked_pct']*100:>5.1f}%{t['pnl']:>+9.0f}")
    wins = [t for t in all_t1t2_trades if t["pnl"] > 0]
    losses = [t for t in all_t1t2_trades if t["pnl"] <= 0]
    gw = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    pf = gw / gl if gl > 0 else (99 if gw > 0 else 0)
    win_pct = 100 * len(wins) / len(all_t1t2_trades) if all_t1t2_trades else 0
    print(f"\nn={len(all_t1t2_trades)}  win%={win_pct:.1f}  Rs{total:+,.0f}  PF={pf:.2f}")
    flips = [t for t in all_t1t2_trades if t["origin"] == "flip"]
    print(f"flip-origin trades: {len(flips)}  PnL from flips: Rs{sum(t['pnl'] for t in flips):+,.0f}")

    # ---- S&R report (from the same run's freshly-logged rows) ----
    print("\n" + "=" * 70)
    print("S&R MECHANIC -- see aggregate track record below "
          "(now includes BANKNIFTY alongside any prior NIFTY/SENSEX days)")
    _print_variant_track_record()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
