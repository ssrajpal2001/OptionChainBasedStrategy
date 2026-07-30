"""Staircase TSL, matching SellStraddle's tsl_scalable shape
(strategies/sell_straddle/rolling.py::_check_scalable_tsl) but in premium %
terms for this strategy: activate at +20% profit -> lock 12.5%; every further
+20% profit gained locks in another +12.5% (uniform repeat of the same
activate/lock ratio -- 'same concept' repeating). SL = ref_low - 20pts buffer,
same as before. Compares OLD (candle-ratchet) vs V2 (single activate+trail)
vs V3 (staircase) on the same 24-trade set."""
import sys, os
sys.path.insert(0, ".")
import scripts.d1trap_fractal_backtest as bt
import scripts.d1trap_daily_table as dt
import pandas as pd
from datetime import time

SL_BUFFER_PTS = 20.0
BASE_PCT = 0.20
BASE_LOCK_PCT = 0.125
STEP_PCT = 0.20
STEP_LOCK_PCT = 0.125
EOD_TIME = time(15, 15)
LOT_SIZE = 65
MAX_RISK_RS_PER_LOT = 2000.0
MAX_RISK_PTS = MAX_RISK_RS_PER_LOT / LOT_SIZE   # ~30.77 pts


def simulate_exit_staircase(direction, entry_ts, entry_price, sl, m5):
    entry_day = entry_ts.date()
    eod_ts = entry_ts.replace(hour=EOD_TIME.hour, minute=EOD_TIME.minute, second=0, microsecond=0)
    sl_buffered = sl - SL_BUFFER_PTS if direction == "LONG" else sl + SL_BUFFER_PTS
    # Cap max risk at Rs2000/lot regardless of how wide ref_low (the zone-derived SL) is.
    if direction == "LONG":
        sl_final = max(sl_buffered, entry_price - MAX_RISK_PTS)
    else:
        sl_final = min(sl_buffered, entry_price + MAX_RISK_PTS)
    after = m5[(m5["timestamp"] > entry_ts) & (m5["timestamp"].dt.date == entry_day)]

    high_lock_pct = 0.0
    for bar in after.itertuples(index=False):
        if bar.timestamp >= eod_ts:
            break
        px = bar.close
        profit_pct = (px - entry_price) / entry_price if direction == "LONG" \
            else (entry_price - px) / entry_price

        if profit_pct >= BASE_PCT:
            num_steps = int((profit_pct - BASE_PCT) // STEP_PCT)
            calc_lock = BASE_LOCK_PCT + num_steps * STEP_LOCK_PCT
            high_lock_pct = max(high_lock_pct, calc_lock)

        if high_lock_pct > 0:
            stop_price = entry_price * (1 + high_lock_pct) if direction == "LONG" \
                else entry_price * (1 - high_lock_pct)
        else:
            stop_price = sl_final

        hit = (px <= stop_price) if direction == "LONG" else (px >= stop_price)
        if hit:
            reason = "tsl_hit" if high_lock_pct > 0 else "sl_hit"
            return dict(exit_ts=bar.timestamp, exit_price=stop_price, reason=reason,
                        locked_pct=high_lock_pct)

    eod_rows = m5[(m5["timestamp"] >= eod_ts) & (m5["timestamp"].dt.date == entry_day)]
    if not eod_rows.empty:
        return dict(exit_ts=eod_ts, exit_price=eod_rows.iloc[0]["close"], reason="eod",
                    locked_pct=high_lock_pct)
    return dict(exit_ts=entry_ts, exit_price=entry_price, reason="no_data", locked_pct=0.0)


def main():
    d1, m1 = bt.load_data("2025-07-30_2026-07-29")
    window_spot = m1[(m1["datetime"] >= dt.WINDOW_START) & (m1["datetime"] <= dt.WINDOW_END)]

    old_total, staircase_total = 0.0, 0.0
    n = 0
    print(f"{'Date':<12} {'Side':>4} {'Entry':>8} {'OLD PnL':>10} {'OLD reason':>11} "
          f"{'STAIR PnL':>10} {'STAIR reason':>12} {'locked%':>8}")
    print("-" * 90)
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
                st = simulate_exit_staircase(t["direction"], t["entry_ts"], t["entry_price"],
                                              t["sl"], opt_m5)
                st_pnl = (st["exit_price"] - t["entry_price"]) * 65 if t["direction"] == "LONG" \
                    else (t["entry_price"] - st["exit_price"]) * 65
                old_total += old_pnl
                staircase_total += st_pnl
                print(f"{str(day):<12} {side:>4} {t['entry_price']:>8.1f} {old_pnl:>+10,.0f} "
                      f"{t['reason']:>11} {st_pnl:>+10,.0f} {st['reason']:>12} "
                      f"{st['locked_pct']*100:>7.1f}%")

    print(f"\nn={n}   OLD total=Rs{old_total:+,.0f}   STAIRCASE total=Rs{staircase_total:+,.0f}")


if __name__ == "__main__":
    main()
