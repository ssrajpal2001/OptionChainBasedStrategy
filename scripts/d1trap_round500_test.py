"""ATM rounding granularity test: round_step=500 vs the current round_step=100
for both NIFTY and SENSEX, holding everything else at the newly-wired live
defaults (3-ITM offset, ref.close boundary, per-index HTF: NIFTY 60m /
SENSEX 15m). Reuses scripts/d1trap_verify_live_defaults.py's run_month_live
(calls the ACTUAL live bb._detect_bear_zones), just varying round_step.
"""
import sys
sys.path.insert(0, ".")
import scripts.d1trap_month_rolling_backtest as mrb
import scripts.d1trap_verify_live_defaults as verify

LADDER_DIR = "data/d1trap_fractal_cache/strike_ladder"

if __name__ == "__main__":
    mrb.MONTH_DIR = LADDER_DIR

    print("NIFTY -- round_step=100 (current) vs round_step=500")
    t_100 = verify.run_month_live("NIFTY", "niftyladder", "data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet",
                                   150, 100, 65, 60)
    mrb.summarize("NIFTY round100 (150pt/60m)", t_100)
    t_500 = verify.run_month_live("NIFTY", "niftyladder", "data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet",
                                   150, 500, 65, 60)
    mrb.summarize("NIFTY round500 (150pt/60m)", t_500)

    print("\nSENSEX -- round_step=100 (current) vs round_step=500")
    s_100 = verify.run_month_live("SENSEX", "sensexladder", "data/d1trap_fractal_cache/sensex_1m_fullmonth_spot.parquet",
                                   300, 100, 20, 15)
    mrb.summarize("SENSEX round100 (300pt/15m)", s_100)
    s_500 = verify.run_month_live("SENSEX", "sensexladder", "data/d1trap_fractal_cache/sensex_1m_fullmonth_spot.parquet",
                                   300, 500, 20, 15)
    mrb.summarize("SENSEX round500 (300pt/15m)", s_500)
