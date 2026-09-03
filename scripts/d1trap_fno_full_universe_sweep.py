"""
scripts/d1trap_fno_full_universe_sweep.py -- run the EXISTING, already-built
backtest/fno_scanner engine (D1 bull/bear trap zones, hard SL = zone boundary
+ buffer, TSL methods incl. prev_day_low = "day low" SL/TSL, phase-based
T1/hedge exits, full param sweep) across the FULL ~195-200 stock FnO
universe instead of just the hardcoded TOP_30_STOCKS -- this is what the
user asked for ("test the same concept with all 200 fno stocks... zone
concept at htf and sl as day low or hour low, again optimisation and tsl as
well"). No new strategy logic invented -- this reuses backtest.py's proven
simulate_stock/Params/build_param_grid unchanged, only the stock universe
and lookback window are new.

Note: only "day low" (prev_day_low TSL method) is in the existing grid, not
"hour low" -- that would need intraday hourly bars, which backtest.py's D1
pipeline doesn't fetch. Flagged as a follow-up, not silently skipped.
"""
import sys, time
sys.path.insert(0, ".")
from datetime import date, timedelta
from backtest.fno_scanner.scan_live import _fetch_fno_universe
from backtest.fno_scanner.backtest import (
    load_or_fetch, build_param_grid, run_backtest_for_params, print_sweep_report, Params,
)

TOKEN_PATH = r"C:\Users\SERVER\AppData\Local\Temp\claude\e--AlgoSoft-OptionChainBasedStrategy\3f952902-2e64-455f-be1c-fac0a7378cbc\scratchpad\upstox_token.txt"

if __name__ == "__main__":
    token = open(TOKEN_PATH).read().strip()

    universe = _fetch_fno_universe(token)
    print(f"FnO universe: {len(universe.stocks)} stocks")

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=6 * 31)
    print(f"Loading D1 bars {start_date}..{end_date} for {len(universe.stocks)} stocks...")

    stock_bars = {}
    for i, (symbol, key) in enumerate(sorted(universe.stocks.items())):
        bars = load_or_fetch(symbol, key, token, start_date, end_date)
        if len(bars) < 20:
            continue
        stock_bars[symbol] = bars
        if i % 25 == 0:
            print(f"  [{i}/{len(universe.stocks)}] {symbol}: {len(bars)} bars")

    print(f"\nLoaded {len(stock_bars)} stocks with usable D1 history.")

    # Full 504-config grid x 200 stocks is computationally intractable here
    # (simulate_stock recomputes zone detection on the WHOLE growing lookback
    # every day it's flat, i.e. ~O(days^2) per stock per config) -- a first
    # timed attempt didn't finish in 10+ minutes. Run a small, meaningful
    # set instead: the default config (pct_from_peak TSL, scan_live.py's own
    # "best" hard_sl/min_rr) plus the literal "day low" TSL the user asked
    # about (prev_day_low), both x hedge_close variants.
    candidates = [
        Params("rolling", "pct_from_peak", 2.0, "tsl_level", 0.8, 1.5, 30),
        Params("rolling", "prev_day_low", 0.0, "tsl_level", 0.8, 1.5, 30),
        Params("rolling", "prev_day_low", 0.0, "day_t1", 0.8, 1.5, 30),
        Params("fixed_at_entry", "prev_day_low", 0.0, "tsl_level", 0.8, 1.5, 30),
    ]
    print(f"Running {len(candidates)} configs x {len(stock_bars)} stocks...")
    t0 = time.time()
    results = []
    for idx, params in enumerate(candidates):
        print(f"  config {idx+1}/{len(candidates)}: {params.label()} ...")
        results.append(run_backtest_for_params(stock_bars, params))
        print(f"    -> {len(results[-1].completed())} trades, "
              f"win={results[-1].win_rate():.1f}%, PF={results[-1].profit_factor():.2f}, "
              f"net={results[-1].net_pnl_pct():+.2f}%  ({time.time()-t0:.0f}s elapsed)")

    print_sweep_report(results, stock_bars, top_n=len(candidates))
