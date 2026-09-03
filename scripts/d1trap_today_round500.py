"""Same tweaked mechanic (09:35 early-session guard, 10%/7% staircase TSL,
Rs2000 SL cap) but for ATM rounded to 500 instead of 100: CE=23800, PE=24200
(from spot_open=24249.55 -> ATM500=24000). Full bear-trap-only pipeline,
including the 15m ref -> 5m subzone -> arm -> swing_breach stages (not just
raw_breakout), matching the live book exactly."""
import sys, os
sys.path.insert(0, ".")
import scripts.d1trap_fractal_backtest as bt
import pandas as pd
from datetime import time

OPT_DIR = os.path.join(bt.CACHE_DIR, "aug4_options")
EARLY_CUTOFF = time(9, 35)
LOT_SIZE = 65
SL_BUFFER_PTS = 20.0
MAX_RISK_RS_PER_LOT = 2000.0
TSL_BASE_PCT = 0.10
TSL_BASE_LOCK_PCT = 0.07
TSL_STEP_PCT = 0.10
TSL_STEP_LOCK_PCT = 0.07
EOD_TIME = time(15, 15)
TODAY = pd.Timestamp("2026-07-30").date()


def find_ref_bar_gated(anchor_ts, m15):
    for row in m15.itertuples(index=False):
        bar_open, bar_close = row.timestamp, row.timestamp + pd.Timedelta(minutes=15)
        if (bar_open <= anchor_ts < bar_close or bar_open >= anchor_ts) and bar_open.time() >= EARLY_CUTOFF:
            return row
    return None


def simulate_exit(entry_ts, entry_price, raw_sl, m1):
    sl_buffered = raw_sl - SL_BUFFER_PTS
    max_risk_pts = MAX_RISK_RS_PER_LOT / LOT_SIZE
    sl_final = max(sl_buffered, entry_price - max_risk_pts)
    entry_day = entry_ts.date()
    eod_ts = entry_ts.replace(hour=EOD_TIME.hour, minute=EOD_TIME.minute, second=0, microsecond=0)
    after = m1[(m1["datetime"] > entry_ts) & (m1["datetime"].dt.date == entry_day)]
    high_lock_pct, peak = 0.0, entry_price
    for bar in after.itertuples(index=False):
        if bar.datetime >= eod_ts:
            break
        stop_price = entry_price * (1 + high_lock_pct) if high_lock_pct > 0 else sl_final
        if bar.low <= stop_price:
            reason = "tsl_hit" if high_lock_pct > 0 else "sl_hit"
            return dict(exit_ts=bar.datetime, exit_price=stop_price, reason=reason,
                        sl_final=sl_final, locked_pct=high_lock_pct)
        peak = max(peak, bar.high)
        profit_pct = (peak - entry_price) / entry_price
        if profit_pct >= TSL_BASE_PCT:
            num_steps = int((profit_pct - TSL_BASE_PCT) // TSL_STEP_PCT)
            calc_lock = TSL_BASE_LOCK_PCT + num_steps * TSL_STEP_LOCK_PCT
            high_lock_pct = max(high_lock_pct, calc_lock)
    eod_rows = m1[(m1["datetime"] >= eod_ts) & (m1["datetime"].dt.date == entry_day)]
    if not eod_rows.empty:
        return dict(exit_ts=eod_ts, exit_price=eod_rows.iloc[0]["close"], reason="eod",
                    sl_final=sl_final, locked_pct=high_lock_pct)
    return dict(exit_ts=entry_ts, exit_price=entry_price, reason="no_data", sl_final=sl_final, locked_pct=0.0)


def run_side(strike, side, fname):
    print(f"\n{'='*80}\n{strike}{side}\n{'='*80}")
    df = pd.read_parquet(os.path.join(OPT_DIR, f"{fname}_with_today.parquet"))
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)
    df_before_today = df[df["datetime"].dt.date < TODAY]

    resamples = {}
    m60 = bt.get_resample(60, df_before_today, resamples)
    zones = bt.detect_d1_zones(bt.to_bars(m60))
    bear_zones = [z for z in zones if z["direction"] == "LONG"]
    print(f"Bear-trap zones from history: {len(bear_zones)}")

    m15 = bt.get_resample(15, df, {})
    m5 = bt.get_resample(5, df, {})
    today_bars = df[df["datetime"].dt.date == TODAY]
    if today_bars.empty:
        print("No data today.")
        return 0.0
    print(f"Today range: O={today_bars.iloc[0]['open']:.2f} "
          f"L={today_bars['low'].min():.2f} H={today_bars['high'].max():.2f} "
          f"C={today_bars.iloc[-1]['close']:.2f}")

    cfg = bt.Config(zone_size_threshold_pct=0.20, enable_continuation=False, enable_retest=False,
                     fallback_on_no_subzone="raw_breakout", htf_minutes=60, ref_minutes=15,
                     sub_minutes=5, entry_mode="swing_breach")

    total_pnl = 0.0
    trades = 0
    flat_until = None
    candidates = []
    for zone in bear_zones:
        touch = today_bars[today_bars["low"] <= zone["zone_hi"]]
        if touch.empty:
            continue
        contact_ts = touch.iloc[0]["datetime"]
        ref = find_ref_bar_gated(contact_ts, m15)
        if ref is None:
            continue
        ref_close_time = ref.timestamp + pd.Timedelta(minutes=15)
        m1_after = df[df["datetime"] >= ref_close_time]
        breach = m1_after[m1_after["high"] >= ref.high]
        if breach.empty:
            continue
        breach_ts = breach.iloc[0]["datetime"]

        window_5m = m5[(m5["timestamp"] >= ref.timestamp) & (m5["timestamp"] < ref_close_time)]
        collapse = bt.collapse_subzones(bt.to_bars(window_5m), "LONG")
        if collapse is None:
            candidates.append((breach_ts, ref.high, ref.low))
        else:
            zone_lo, zone_hi = collapse
            threshold_pts = cfg.zone_size_threshold_pct / 100.0 * ref.close
            lvl = bt.arm_level(zone_lo, zone_hi, "LONG", threshold_pts)
            m1_arm = df[(df["datetime"] > breach_ts)]
            armed = m1_arm[(m1_arm["low"] <= lvl) & (m1_arm["low"] >= zone_lo)]
            if armed.empty:
                continue
            armed_ts = armed.iloc[0]["datetime"]
            m1_trig = df[df["datetime"] > armed_ts]
            trig = m1_trig[m1_trig["high"] >= zone_hi]
            if trig.empty:
                continue
            candidates.append((trig.iloc[0]["datetime"], zone_hi, ref.low))

    candidates.sort(key=lambda c: c[0])
    for entry_ts, entry_price, raw_sl in candidates:
        if flat_until is not None and entry_ts < flat_until:
            continue
        result = simulate_exit(entry_ts, entry_price, raw_sl, df)
        pnl = (result["exit_price"] - entry_price) * LOT_SIZE
        total_pnl += pnl
        trades += 1
        print(f"  Trade: entry={entry_ts} @ {entry_price:.2f}  exit={result['exit_ts']} @ "
              f"{result['exit_price']:.2f} ({result['reason']}, locked={result['locked_pct']*100:.1f}%)  "
              f"PnL=Rs{pnl:+,.0f}")
        flat_until = result["exit_ts"]

    if trades == 0:
        print("  No trades today.")
    print(f"  Side total: Rs{total_pnl:+,.0f} ({trades} trades)")
    return total_pnl


ce_pnl = run_side(23800, "CE", "23800_CE")
pe_pnl = run_side(24200, "PE", "24200_PE")
print(f"\n{'='*80}\nROUND-500 DAY TOTAL: Rs{ce_pnl+pe_pnl:+,.0f}\n{'='*80}")
