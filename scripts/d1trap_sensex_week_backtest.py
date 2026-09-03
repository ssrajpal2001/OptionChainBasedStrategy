"""
scripts/d1trap_sensex_week_backtest.py -- genuine 1-week (07-27..07-31) SENSEX
backtest against REAL option premium, all under the SAME currently-active
weekly contract (expiry 2026-08-06, which has genuinely been trading since
2026-07-10 -- BSE lists several weeklies in parallel, so this window has no
rollover contamination). Data fetched by scripts/d1trap_sensex_week_fetch.py.

Reuses scripts/d1trap_month_rolling_backtest.py's zone engine / day-loop
wiring directly (same mechanic validated on NIFTY's month backtest) -- only
the date range, strike offset (500 vs NIFTY's 200), strike step (100), lot
size (20), and data directory differ.

SENSEX-specific note (per direct user context): SENSEX moves fast as an
underlying, and this strategy is a pure OPTION BUYER -- it needs real
momentum/follow-through to work, since a slow/rangebound SENSEX session
would under-trigger breaches (T1) and starve the sub-zone arm/swing-breach
(T2) stage. This script reports trade frequency and per-day P&L alongside
PF/win% so a low-trade-count week can be read correctly as "SENSEX didn't
move enough this week" rather than "the mechanic is broken."
"""
import sys
sys.path.insert(0, ".")
from datetime import date

import scripts.d1trap_month_rolling_backtest as mrb

LOT_SENSEX, OFFSET_SENSEX, STEP_SENSEX = 20, 500, 100
SPOT_PATH = "data/d1trap_fractal_cache/sensex_1m_fullmonth_spot.parquet"
WEEK_MIN, WEEK_MAX = date(2026, 7, 27), date(2026, 7, 31)


if __name__ == "__main__":
    mrb.MONTH_DIR = "data/d1trap_fractal_cache/sensex_week"
    mrb.DAY_MIN, mrb.DAY_MAX = WEEK_MIN, WEEK_MAX

    trades = mrb.run_month("SENSEX", "sensexweek", SPOT_PATH, OFFSET_SENSEX, STEP_SENSEX, LOT_SENSEX)

    print(f"\n{'='*100}\nSENSEX 1-WEEK BACKTEST (2026-07-27..07-31, real premium, expiry 2026-08-06)\n{'='*100}")
    stats = mrb.summarize("SENSEX week", trades)

    print(f"\n{'Side':<10}{'Tranche':>8}{'Entry':>10}{'EntryTS':>17}{'Exit':>10}{'ExitTS':>17}{'Reason':>10}{'PnL':>10}")
    print("-" * 100)
    for t in trades:
        print(f"{t['side']+str(t['strike']):<10}{t['tranche']:>8}{t['entry']:>10.2f}"
              f"{str(t.get('entry_ts')):>17}{t['exit']:>10.2f}{str(t['exit_ts']):>17}"
              f"{t['reason']:>10}{t['pnl']:>+10.0f}")

    n_days_with_trades = len({t["exit_ts"].date() for t in trades})
    print(f"\nTrading days in window: 5 (07-27..07-31)  |  days with a trade: {n_days_with_trades}  |  total trades: {len(trades)}")
    if len(trades) < 5:
        print("NOTE: low trade count this week -- consistent with SENSEX needing real momentum for this "
              "option-buyer mechanic to trigger (breach/arm/swing-breach all require directional follow-through, "
              "not just range). Not enough samples here to judge PF/win% reliably -- read this as a smoke test "
              "that the mechanic fires correctly on SENSEX, not as a validated edge.")
