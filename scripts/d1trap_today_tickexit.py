"""Tick-approximate exit simulation (1-min bar high/low, not 5-min close) --
matches the live book's actual tick-by-tick _check_exit logic much more
closely than the close-based backtest simulator. Peak tracked via running
1-min high; stop checked against 1-min low (long) each bar."""
import sys, os
sys.path.insert(0, ".")
import pandas as pd
from datetime import time

LOT_SIZE = 65
EOD_TIME = time(15, 15)
SL_BUFFER_PTS = 20.0
MAX_RISK_RS_PER_LOT = 2000.0
TSL_BASE_PCT = 0.20
TSL_BASE_LOCK_PCT = 0.125
TSL_STEP_PCT = 0.20
TSL_STEP_LOCK_PCT = 0.125


def simulate(entry_ts, entry_price, raw_sl, m1: pd.DataFrame):
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
        # Check the LOW of this bar against the current stop first (tick-order-safe:
        # a bar's low could be reached before or after its high; checking the adverse
        # side first is the conservative assumption).
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
    return dict(exit_ts=entry_ts, exit_price=entry_price, reason="no_data",
                sl_final=sl_final, locked_pct=high_lock_pct)


if __name__ == "__main__":
    df = pd.read_parquet("data/d1trap_fractal_cache/aug4_options/24000_CE_with_today.parquet")
    df["datetime"] = pd.to_datetime(df["datetime"])

    trades = [
        ("Trade 1", pd.Timestamp("2026-07-30 09:31:00", tz="Asia/Kolkata"), 299.45, 236.60),
        ("Trade 2", pd.Timestamp("2026-07-30 12:31:00", tz="Asia/Kolkata"), 299.45, 236.60),
    ]
    total = 0.0
    for name, entry_ts, entry_price, raw_sl in trades:
        r = simulate(entry_ts, entry_price, raw_sl, df)
        pnl = (r["exit_price"] - entry_price) * LOT_SIZE
        total += pnl
        print(f"{name}: entry={entry_ts} @ {entry_price:.2f}  sl_final={r['sl_final']:.2f}  "
              f"exit={r['exit_ts']} @ {r['exit_price']:.2f} ({r['reason']}, "
              f"locked={r['locked_pct']*100:.1f}%)  PnL=Rs{pnl:+,.0f}")
    print(f"\nCE side total for today: Rs{total:+,.0f}")
    print("PE side: no trade (never breached its ref-candle high). PnL=Rs0")
    print(f"Day total (CE+PE): Rs{total:+,.0f}")
