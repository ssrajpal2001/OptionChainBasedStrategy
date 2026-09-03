"""
scripts/d1trap_sensex_first15_strike_test.py -- test a new SENSEX strike
selection idea (direct user spec, 2026-08-04): wait for the first 15-min
candle to close (09:15-09:30), then:
  PE strike = round(that candle's HIGH to the nearest 100)
  CE strike = round(that candle's LOW  to the nearest 100)
Everything else is UNCHANGED from the live bear_only_book mechanic --
15-min HTF zone detection, ref.close/sellers_in.low zone boundary, tranche
T1/T2 entries, tick-level TSL + hard-cap SL -- run on those two strikes' own
REAL option premium (option-chart-native, not spot), exactly like the
existing live design. Only the STRIKE SELECTION step changes.

Window: 2026-07-22 (when this month's SENSEX option premium became liquid)
through the latest available trading day.
"""
import sys, asyncio
sys.path.insert(0, ".")
from datetime import date, timedelta
import pandas as pd
import strategies.d1_trap_option.bear_only_book as bb
import scripts.d1trap_month_rolling_backtest as mrb
from data_layer.instrument_registry import REGISTRY
from data_layer.historical_candles import fetch_upstox_range_1m

TOKEN_PATH = r"C:\Users\SERVER\AppData\Local\Temp\claude\e--AlgoSoft-OptionChainBasedStrategy\3f952902-2e64-455f-be1c-fac0a7378cbc\scratchpad\upstox_token.txt"
SPOT_PATH = "data/d1trap_fractal_cache/sensex_1m_fullmonth_spot.parquet"
LADDER_DIR = "data/d1trap_fractal_cache/strike_ladder"
EXPIRY = date(2026, 8, 6)   # current active weekly, continuous history back through the window
ROUND_STEP = 100
LOT = 20
HTF_MINUTES = 15
DAY_MIN = date(2026, 7, 17)


def _install_trade_detail_patches():
    """Monkeypatch mrb.open_leg/check_exit/_close (process-local, this script
    only) to capture initial SL and max-favorable-excursion (peak premium
    reached before any pullback) per trade -- needed for the detailed
    entry/SL/TSL/MFE printout requested, not available in the base trade
    dict."""
    orig_open_leg, orig_check_exit, orig_close = mrb.open_leg, mrb.check_exit, mrb._close

    def patched_open_leg(state, lot_size, tranche, entry_price, sl, zone_lock_ts, audit=None, entry_ts=None):
        orig_open_leg(state, lot_size, tranche, entry_price, sl, zone_lock_ts, audit=audit, entry_ts=entry_ts)
        pos = state.positions[-1]
        pos["mfe"] = pos["entry_price"]
        pos["initial_sl"] = pos["sl"]

    def patched_check_exit(state, lot_size, ltp, ts, force=False, force_reason="day_switch"):
        for pos in state.positions:
            pos["mfe"] = max(pos.get("mfe", pos["entry_price"]), ltp)
        orig_check_exit(state, lot_size, ltp, ts, force=force, force_reason=force_reason)

    def patched_close(state, lot_size, pos, reason, exit_price, ts):
        mfe, initial_sl, lock_pct = pos.get("mfe", pos["entry_price"]), pos.get("initial_sl", pos["sl"]), pos.get("high_lock_pct", 0.0)
        orig_close(state, lot_size, pos, reason, exit_price, ts)
        state.trades[-1]["mfe"] = mfe
        state.trades[-1]["initial_sl"] = initial_sl
        state.trades[-1]["tsl_lock_pct"] = lock_pct

    mrb.open_leg, mrb.check_exit, mrb._close = patched_open_leg, patched_check_exit, patched_close


def print_trade_detail(name, trades):
    print(f"\n{'-'*110}\n{name} -- per-trade detail\n{'-'*110}")
    print(f"{'Entry TS':<17}{'Side':<6}{'Strike':<8}{'Entry':>8}{'InitSL':>8}{'MFE':>8}{'ExitTS':<17}{'Exit':>8}{'Reason':<14}{'TSLlock%':>9}{'PnL':>10}")
    for t in trades:
        ets = t["entry_ts"].strftime("%m-%d %H:%M") if t.get("entry_ts") is not None else "?"
        xts = t["exit_ts"].strftime("%m-%d %H:%M") if t.get("exit_ts") is not None else "?"
        mfe_run_pct = (t.get("mfe", t["entry"]) - t["entry"]) / t["entry"] * 100 if t["entry"] else 0
        print(f"{ets:<17}{t['side']:<6}{t['strike']:<8}{t['entry']:>8.2f}{t.get('initial_sl',0):>8.2f}"
              f"{t.get('mfe',t['entry']):>8.2f}{xts:<17}{t['exit']:>8.2f}{t['reason']:<14}"
              f"{t.get('tsl_lock_pct',0)*100:>8.1f}%{t['pnl']:>10.1f}"
              f"   (ran +{mfe_run_pct:.1f}% from entry before this exit)")


def first15_strikes(spot_df, day):
    day_df = spot_df[spot_df["datetime"].dt.date == day]
    first15 = day_df[day_df["datetime"].dt.time < pd.Timestamp("09:30").time()]
    if first15.empty:
        return None, None
    hi, lo = first15["high"].max(), first15["low"].min()
    pe_strike = int(round(hi / ROUND_STEP) * ROUND_STEP)
    ce_strike = int(round(lo / ROUND_STEP) * ROUND_STEP)
    return ce_strike, pe_strike


async def fetch_needed(token, spot_df, days):
    REGISTRY.load_sync("SENSEX", token)
    needed = set()
    for day in days:
        ce, pe = first15_strikes(spot_df, day)
        if ce and pe:
            needed.add((ce, "CE"))
            needed.add((pe, "PE"))
    print(f"{len(needed)} unique (strike,side) pairs needed")
    for strike, side in sorted(needed):
        fname = f"{LADDER_DIR}/sensexladder_{strike}_{side}.parquet"
        import os
        if os.path.exists(fname):
            continue
        key = REGISTRY.get_upstox_key("SENSEX", EXPIRY, strike, side)
        if not key:
            print(f"  SKIP {strike}{side}: no key")
            continue
        rows = await fetch_upstox_range_1m(key, token, DAY_MIN - timedelta(days=bb._HIST_WARMUP_DAYS), date.today())
        df = pd.DataFrame(rows)
        if df.empty:
            print(f"  {strike}{side}: 0 rows")
            continue
        df = df.rename(columns={"ts": "datetime"})
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)
        df.to_parquet(fname)
        print(f"  fetched {strike}{side}: {len(df)} rows")


def run_backtest(spot_df, days):
    ce_state, pe_state = mrb.SideState(), mrb.SideState()
    ce_cache, pe_cache = {}, {}

    def get_bars(strike, side):
        cache = ce_cache if side == "CE" else pe_cache
        if strike not in cache:
            cache[strike] = mrb.load_bars("sensexladder", strike, side)
        return cache[strike]

    def warmup(state, df1m, strike, side, day):
        state.strike, state.side = strike, side
        state.zones, state.flip_candidates, state.positions = [], [], []
        start = day - timedelta(days=bb._HIST_WARMUP_DAYS)
        hist = df1m[(df1m["datetime"].dt.date >= start) & (df1m["datetime"].dt.date < day)]
        m_htf_hist, m15_hist = bb._resample(hist, HTF_MINUTES), bb._resample(hist, 15)
        state.zones = mrb._prevalidate(bb._detect_bear_zones(bb._to_bars(m_htf_hist)), m15_hist)

    def refresh(state, ts, df1m, day):
        start = day - timedelta(days=bb._HIST_WARMUP_DAYS)
        window = df1m[(df1m["datetime"].dt.date >= start) & (df1m["datetime"] < ts)]
        if len(window) < 30:
            return
        m_htf, m15 = bb._resample(window, HTF_MINUTES), bb._resample(window, 15)
        existing_refs = {z["ref_ts"] for z in state.zones}
        new_zones = [z for z in bb._detect_bear_zones(bb._to_bars(m_htf)) if z["ref_ts"] not in existing_refs]
        state.zones.extend(mrb._prevalidate(new_zones, m15))

    for day in days:
        ce_strike, pe_strike = first15_strikes(spot_df, day)
        if not ce_strike or not pe_strike:
            continue
        ce_df, pe_df = get_bars(ce_strike, "CE"), get_bars(pe_strike, "PE")
        if ce_df.empty or pe_df.empty:
            print(f"  {day}: missing premium data for {ce_strike}CE/{pe_strike}PE — skip day")
            continue

        if ce_state.strike != ce_strike:
            if ce_state.positions:
                px = ce_df.iloc[0]["open"] if not ce_df[ce_df["datetime"].dt.date == day].empty else ce_state.positions[0]["entry_price"]
                mrb.check_exit(ce_state, LOT, px, pd.Timestamp(day), force=True)
            warmup(ce_state, ce_df, ce_strike, "CE", day)
        if pe_state.strike != pe_strike:
            if pe_state.positions:
                px = pe_df.iloc[0]["open"] if not pe_df[pe_df["datetime"].dt.date == day].empty else pe_state.positions[0]["entry_price"]
                mrb.check_exit(pe_state, LOT, px, pd.Timestamp(day), force=True)
            warmup(pe_state, pe_df, pe_strike, "PE", day)

        ce_today = ce_df[ce_df["datetime"].dt.date == day].reset_index(drop=True)
        pe_today = pe_df[pe_df["datetime"].dt.date == day].reset_index(drop=True)
        if ce_today.empty and pe_today.empty:
            continue
        ce_m5, ce_m15, ce_mhtf = bb._resample(ce_df, 5), bb._resample(ce_df, 15), bb._resample(ce_df, HTF_MINUTES)
        pe_m5, pe_m15, pe_mhtf = bb._resample(pe_df, 5), bb._resample(pe_df, 15), bb._resample(pe_df, HTF_MINUTES)
        ce_m15_today = ce_m15[ce_m15["timestamp"].dt.date == day].reset_index(drop=True)
        pe_m15_today = pe_m15[pe_m15["timestamp"].dt.date == day].reset_index(drop=True)
        ce_mhtf_today = ce_mhtf[ce_mhtf["timestamp"].dt.date == day].reset_index(drop=True)
        pe_mhtf_today = pe_mhtf[pe_mhtf["timestamp"].dt.date == day].reset_index(drop=True)

        max_len = max(len(ce_today), len(pe_today))
        ce15 = pe15 = cehtf = pehtf = 0
        for i in range(max_len):
            if i < len(ce_today):
                bar = ce_today.iloc[i]; ts = bar["datetime"]
                while cehtf < len(ce_mhtf_today) and ce_mhtf_today.iloc[cehtf]["timestamp"] + timedelta(minutes=HTF_MINUTES) <= ts:
                    refresh(ce_state, ce_mhtf_today.iloc[cehtf]["timestamp"] + timedelta(minutes=HTF_MINUTES), ce_df, day)
                    cehtf += 1
                while ce15 < len(ce_m15_today) and ce_m15_today.iloc[ce15]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = ce_m15_today.iloc[ce15]
                    ce_state.prev15_high, ce_state.prev15_low = m15row["high"], m15row["low"]
                    mrb.on_new_15m_close(ce_state, m15row)
                    mrb.process_flip_entry_t2(ce_state, LOT, m15row, ce_m15, ce_m5, pe_state)
                    ce15 += 1
                mrb.check_exit(ce_state, LOT, bar["close"], ts)
                mrb.check_fast_t1(ce_state, LOT, bar["high"], ts, pe_state)
                mrb.process_zones_tick(ce_state, LOT, ts, bar["low"], bar["high"], ce_m15, ce_m5, pe_state)
            if i < len(pe_today):
                bar = pe_today.iloc[i]; ts = bar["datetime"]
                while pehtf < len(pe_mhtf_today) and pe_mhtf_today.iloc[pehtf]["timestamp"] + timedelta(minutes=HTF_MINUTES) <= ts:
                    refresh(pe_state, pe_mhtf_today.iloc[pehtf]["timestamp"] + timedelta(minutes=HTF_MINUTES), pe_df, day)
                    pehtf += 1
                while pe15 < len(pe_m15_today) and pe_m15_today.iloc[pe15]["timestamp"] + timedelta(minutes=15) <= ts:
                    m15row = pe_m15_today.iloc[pe15]
                    pe_state.prev15_high, pe_state.prev15_low = m15row["high"], m15row["low"]
                    mrb.on_new_15m_close(pe_state, m15row)
                    mrb.process_flip_entry_t2(pe_state, LOT, m15row, pe_m15, pe_m5, ce_state)
                    pe15 += 1
                mrb.check_exit(pe_state, LOT, bar["close"], ts)
                mrb.check_fast_t1(pe_state, LOT, bar["high"], ts, ce_state)
                mrb.process_zones_tick(pe_state, LOT, ts, bar["low"], bar["high"], pe_m15, pe_m5, ce_state)

    return sorted(ce_state.trades + pe_state.trades, key=lambda t: t["exit_ts"])


async def main():
    token = open(TOKEN_PATH).read().strip()
    spot = pd.read_parquet(SPOT_PATH)
    spot["datetime"] = pd.to_datetime(spot["datetime"])
    days = sorted(d for d in spot["datetime"].dt.date.unique() if d >= DAY_MIN)
    print(f"{len(days)} trading days from {DAY_MIN}: {days}")

    await fetch_needed(token, spot, days)
    _install_trade_detail_patches()

    import scripts.d1trap_spot_bias_test as sbt
    bias_by_day = sbt.daily_bias_series(spot)

    mrb.MONTH_DIR = LADDER_DIR
    new_trades = run_backtest(spot, days)
    new_biased = sbt.apply_bias_filter(new_trades, bias_by_day)

    import scripts.d1trap_verify_live_defaults as verify
    orig_min, orig_max = mrb.DAY_MIN, mrb.DAY_MAX
    mrb.DAY_MIN, mrb.DAY_MAX = DAY_MIN, date.today()
    try:
        base_trades = verify.run_month_live("SENSEX", "sensexladder", SPOT_PATH, 300, 100, LOT, 15)
    finally:
        mrb.DAY_MIN, mrb.DAY_MAX = orig_min, orig_max
    base_biased = sbt.apply_bias_filter(base_trades, bias_by_day)

    print(f"\n{'='*110}\nSUMMARY -- window {DAY_MIN} .. {days[-1]}\n{'='*110}")
    mrb.summarize("NEW: first-15m-candle strikes, WITHOUT bias filter", new_trades)
    mrb.summarize("NEW: first-15m-candle strikes, WITH bias filter", new_biased)
    mrb.summarize("BASELINE: live 300pt offset, WITHOUT bias filter", base_trades)
    mrb.summarize("BASELINE: live 300pt offset, WITH bias filter", base_biased)

    print_trade_detail("NEW idea, WITHOUT bias filter", new_trades)
    print_trade_detail("NEW idea, WITH bias filter", new_biased)
    print_trade_detail("BASELINE, WITHOUT bias filter", base_trades)
    print_trade_detail("BASELINE, WITH bias filter", base_biased)

if __name__ == "__main__":
    asyncio.run(main())
