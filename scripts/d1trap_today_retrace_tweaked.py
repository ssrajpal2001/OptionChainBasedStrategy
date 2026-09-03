"""Re-trace today (24000 CE) with the 2026-07-30 tweaks applied:
  - no ref-candle assignment before 09:35 IST
  - TSL staircase tightened to +10%/lock 7% (repeating)
Mirrors the live book's actual _process_new_bar/_check_exit logic exactly."""
import sys, os
sys.path.insert(0, ".")
import scripts.d1trap_fractal_backtest as bt
import pandas as pd
from datetime import time

EARLY_CUTOFF = time(9, 35)
LOT_SIZE = 65
SL_BUFFER_PTS = 20.0
MAX_RISK_RS_PER_LOT = 2000.0
TSL_BASE_PCT = 0.10
TSL_BASE_LOCK_PCT = 0.07
TSL_STEP_PCT = 0.10
TSL_STEP_LOCK_PCT = 0.07
EOD_TIME = time(15, 15)


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

    high_lock_pct = 0.0
    peak = entry_price
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


df = pd.read_parquet("data/d1trap_fractal_cache/aug4_options/24000_CE_with_today.parquet")
df["datetime"] = pd.to_datetime(df["datetime"])
m15 = bt.get_resample(15, df, {})
m1 = df

# Same 3 historical zones as before, sharing the same underlying band.
today = pd.Timestamp("2026-07-30").date()
today_bars = df[df["datetime"].dt.date == today]
contact_ts = today_bars.iloc[0]["datetime"]   # 09:15, unaffected by the tweak (contact still immediate)

ref = find_ref_bar_gated(contact_ts, m15)
print(f"New ref candle (>=09:35 gate): {ref.timestamp} -> {ref.timestamp+pd.Timedelta(minutes=15)}  "
      f"H={ref.high:.2f} L={ref.low:.2f}")

ref_close_time = ref.timestamp + pd.Timedelta(minutes=15)
m1_after = m1[m1["datetime"] >= ref_close_time]
breach = m1_after[m1_after["high"] >= ref.high]
if breach.empty:
    print("Never breached today.")
else:
    entry_ts = breach.iloc[0]["datetime"]
    entry_price = ref.high
    print(f"Entry: {entry_ts} @ {entry_price:.2f}")
    result = simulate_exit(entry_ts, entry_price, ref.low, m1)
    pnl = (result["exit_price"] - entry_price) * LOT_SIZE
    print(f"Exit: {result['exit_ts']} @ {result['exit_price']:.2f} ({result['reason']}, "
          f"locked={result['locked_pct']*100:.1f}%)  sl_final={result['sl_final']:.2f}")
    print(f"PnL: Rs{pnl:+,.0f}")

    # Check for a possible second entry after this one closes, same as before.
    after_exit = m1[m1["datetime"] > result["exit_ts"]]
    recross = after_exit[after_exit["high"] >= entry_price]
    if not recross.empty and result["exit_ts"].time() < time(15, 15):
        e2_ts = recross.iloc[0]["datetime"]
        print(f"\nRe-crosses {entry_price:.2f} again at {e2_ts} -- would fire a second entry there.")
        r2 = simulate_exit(e2_ts, entry_price, ref.low, m1)
        pnl2 = (r2["exit_price"] - entry_price) * LOT_SIZE
        print(f"Trade 2: exit {r2['exit_ts']} @ {r2['exit_price']:.2f} ({r2['reason']}, "
              f"locked={r2['locked_pct']*100:.1f}%)  PnL=Rs{pnl2:+,.0f}")
        print(f"\nDay total (tweaked): Rs{pnl+pnl2:+,.0f}")
    else:
        print(f"\nDay total (tweaked): Rs{pnl:+,.0f}")
