"""Improved exit: SL = ref_low - 20pts buffer; TSL activates only after +25%
premium gain, then trails 20% below running peak (percentage-based, not
raw-candle-low ratchet). Re-checks the exact 07-13 23850CE trade and the
full month's daily-table trades with this exit instead of the old one."""
import sys, os
sys.path.insert(0, ".")
import scripts.d1trap_fractal_backtest as bt
import scripts.d1trap_daily_table as dt
import pandas as pd
from datetime import time

SL_BUFFER_PTS = 20.0
TSL_ACTIVATE_PCT = 0.25
TSL_TRAIL_PCT = 0.20
EOD_TIME = time(15, 15)


def simulate_exit_v2(direction, entry_ts, entry_price, sl, m5):
    entry_day = entry_ts.date()
    eod_ts = entry_ts.replace(hour=EOD_TIME.hour, minute=EOD_TIME.minute, second=0, microsecond=0)
    sl_final = sl - SL_BUFFER_PTS if direction == "LONG" else sl + SL_BUFFER_PTS
    after = m5[(m5["timestamp"] > entry_ts) & (m5["timestamp"].dt.date == entry_day)]

    peak = entry_price
    armed = False
    trail_level = None
    for bar in after.itertuples(index=False):
        if bar.timestamp >= eod_ts:
            break
        stop = trail_level if (armed and trail_level is not None) else sl_final
        if direction == "LONG":
            if bar.close <= stop:
                return dict(exit_ts=bar.timestamp, exit_price=stop,
                            reason="tsl_hit" if armed else "sl_hit")
            peak = max(peak, bar.high)
            if not armed and peak >= entry_price * (1 + TSL_ACTIVATE_PCT):
                armed = True
                trail_level = peak * (1 - TSL_TRAIL_PCT)
            elif armed:
                trail_level = max(trail_level, peak * (1 - TSL_TRAIL_PCT))
        else:
            if bar.close >= stop:
                return dict(exit_ts=bar.timestamp, exit_price=stop,
                            reason="tsl_hit" if armed else "sl_hit")
            peak = min(peak, bar.low) if peak != entry_price else min(entry_price, bar.low)

    eod_rows = m5[(m5["timestamp"] >= eod_ts) & (m5["timestamp"].dt.date == entry_day)]
    if not eod_rows.empty:
        return dict(exit_ts=eod_ts, exit_price=eod_rows.iloc[0]["close"], reason="eod")
    return dict(exit_ts=entry_ts, exit_price=entry_price, reason="no_data")


def main():
    # 1) The exact 07-13 23850CE trade
    df = pd.read_parquet(os.path.join(dt.OPT_DIR, "23850_CE.parquet"))
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)
    resamples = {}
    m5 = bt.get_resample(5, df, resamples)

    entry_ts = pd.Timestamp("2026-07-13 09:36:00", tz=bt.IST)
    entry_price, sl = 420.40, 412.25
    old = dict(exit_price=430.00, reason="tsl_hit")
    new = simulate_exit_v2("LONG", entry_ts, entry_price, sl, m5)
    print("07-13 23850CE, single trade comparison:")
    print(f"  OLD exit: {old['exit_price']:.2f} ({old['reason']})  "
          f"PnL=Rs{(old['exit_price']-entry_price)*65:+,.0f}")
    print(f"  NEW exit: {new['exit_ts']}  {new['exit_price']:.2f} ({new['reason']})  "
          f"PnL=Rs{(new['exit_price']-entry_price)*65:+,.0f}")

    # 2) Full month re-run with the new exit, reusing daily_table's signal generation
    print(f"\n{'='*90}\nFull month re-run with improved SL/TSL\n{'='*90}")
    d1, m1 = bt.load_data("2025-07-30_2026-07-29")
    window_spot = m1[(m1["datetime"] >= dt.WINDOW_START) & (m1["datetime"] <= dt.WINDOW_END)]

    old_total, new_total = 0.0, 0.0
    n = 0
    for day, g in window_spot.groupby(window_spot["datetime"].dt.date):
        open_px = g.sort_values("datetime").iloc[0]["open"]
        atm = round(open_px / dt.STRIKE_STEP) * dt.STRIKE_STEP
        ce_strike, pe_strike = int(atm - dt.ITM_OFFSET_PTS), int(atm + dt.ITM_OFFSET_PTS)
        for strike, side in [(ce_strike, "CE"), (pe_strike, "PE")]:
            trades = dt.trades_for_strike_up_to(strike, side, day)
            today_trades = [t for t in trades if t["entry_ts"].date() == day]
            if not today_trades:
                continue
            opt_df = dt.load_option_1m(strike, side)
            opt_m5 = bt.get_resample(5, opt_df, {})
            for t in today_trades:
                n += 1
                old_pnl = t["pnl_pts"] * 65
                new_exit = simulate_exit_v2(t["direction"], t["entry_ts"], t["entry_price"],
                                             t["sl"], opt_m5)
                new_pnl = (new_exit["exit_price"] - t["entry_price"]) * 65 if t["direction"] == "LONG" \
                    else (t["entry_price"] - new_exit["exit_price"]) * 65
                old_total += old_pnl
                new_total += new_pnl
                if abs(new_pnl - old_pnl) > 200:
                    print(f"  {t['entry_ts'].date()} {side} entry={t['entry_price']:.1f}  "
                          f"OLD={old_pnl:+,.0f} ({t['reason']})  NEW={new_pnl:+,.0f} ({new_exit['reason']})")

    print(f"\nn={n} trades   OLD total=Rs{old_total:+,.0f}   NEW total=Rs{new_total:+,.0f}")


if __name__ == "__main__":
    main()
