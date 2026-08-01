"""
scripts/fvg_tsl_sweep.py — step-locked TSL parameter optimization sweep for
the FVG strategy, per explicit request. Runs 4 TSL parameter combos across 2
timeframe setups (HTF=10m/LTF=3m and HTF=10m/LTF=1m -- the two leading
candidates from scripts/fvg_tf_sweep.py) against the same real NIFTY option
premium data (last 7 TRADING days available: 2026-07-23..07-31 -- the
literally-requested 07-25/08-01 window is both Saturdays, same substitution
already used and accepted in scripts/fvg_backtest.py).

Combos:
  Baseline        : trigger=20% lock=12.5% step=10% step_lock=5%
  A (Tight Lock)  : trigger=15% lock=10%   step=10% step_lock=5%
  B (Aggressive)  : trigger=20% lock=15%   step=10% step_lock=5%
  C (Wider Runner): trigger=25% lock=15%   step=15% step_lock=7.5%

Initial SL stays fixed at 20% premium (+ hard Rs/lot cap) across all combos
-- only the trailing-lock tiers vary, per the request.
"""
import sys
sys.path.insert(0, ".")

import scripts.fvg_backtest as fb

TF_COMBOS = [(10, 3), (10, 1)]

TSL_COMBOS = [
    ("Baseline",        dict(tsl_base_pct=0.20, tsl_base_lock_pct=0.125, tsl_step_pct=0.10, tsl_step_lock_pct=0.05)),
    ("A (Tight Lock)",  dict(tsl_base_pct=0.15, tsl_base_lock_pct=0.10,  tsl_step_pct=0.10, tsl_step_lock_pct=0.05)),
    ("B (Aggressive)",  dict(tsl_base_pct=0.20, tsl_base_lock_pct=0.15,  tsl_step_pct=0.10, tsl_step_lock_pct=0.05)),
    ("C (Wider Runner)", dict(tsl_base_pct=0.25, tsl_base_lock_pct=0.15, tsl_step_pct=0.15, tsl_step_lock_pct=0.075)),
]

STAGNATION_MINUTES = 40


def run(spot, signals_cache, premium_cache, htf_mins, ltf_mins, tsl_params):
    stagnation_bars = max(1, STAGNATION_MINUTES // ltf_mins)
    trades = fb.resolve_trades(signals_cache[(htf_mins, ltf_mins)], premium_cache[(htf_mins, ltf_mins)], spot,
                                ltf_mins=ltf_mins, stagnation_bars=stagnation_bars, **tsl_params)
    trades.sort(key=lambda t: t["exit_ts"])
    n = len(trades)
    wins = [t for t in trades if t["pnl_rs"] > 0]
    losses = [t for t in trades if t["pnl_rs"] <= 0]
    gross_win = sum(t["pnl_rs"] for t in wins)
    gross_loss = abs(sum(t["pnl_rs"] for t in losses))
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")
    net = sum(t["pnl_rs"] for t in trades)
    win_pct = 100 * len(wins) / n if n else 0
    mdd = fb.max_drawdown(trades)
    return dict(n=n, win_pct=win_pct, pf=pf, gross_win=gross_win, gross_loss=gross_loss,
                net=net, mdd=mdd, trades=trades)


def main():
    spot = fb.load_spot()

    # discover signals + fetch premium ONCE per timeframe combo (shared across
    # all 4 TSL variants -- entries don't depend on TSL params, only exits do)
    signals_cache, premium_cache = {}, {}
    for htf_mins, ltf_mins in TF_COMBOS:
        print(f"\n{'='*90}\nDiscovering signals + premium for HTF={htf_mins}m/LTF={ltf_mins}m...\n{'='*90}")
        signals = fb.discover_signals(spot, htf_mins=htf_mins, ltf_mins=ltf_mins)
        print(f"  {len(signals)} signals found.")
        premium_cache[(htf_mins, ltf_mins)] = fb.ensure_premium_data(signals)
        signals_cache[(htf_mins, ltf_mins)] = signals

    results = []
    for htf_mins, ltf_mins in TF_COMBOS:
        for combo_name, tsl_params in TSL_COMBOS:
            r = run(spot, signals_cache, premium_cache, htf_mins, ltf_mins, tsl_params)
            r.update(htf=htf_mins, ltf=ltf_mins, combo=combo_name, params=tsl_params)
            results.append(r)
            print(f"HTF={htf_mins}m/LTF={ltf_mins}m  {combo_name:<18} "
                  f"n={r['n']:>3}  win%={r['win_pct']:>5.1f}  PF={r['pf']:>5.2f}  "
                  f"NET=Rs{r['net']:>+8,.0f}  maxDD=Rs{r['mdd']:>8,.0f}")

    print(f"\n{'='*110}\nTSL OPTIMIZATION SWEEP -- NIFTY, {fb.BACKTEST_START}..{fb.BACKTEST_END}\n{'='*110}")
    print(f"{'Combo':<10}{'TF':<10}{'Trigger':>8}{'Lock':>7}{'Step':>7}{'StepLock':>9}"
          f"{'Trades':>8}{'Win%':>7}{'PF':>7}{'NetPnL':>11}{'MaxDD':>10}")
    for r in sorted(results, key=lambda r: r["pf"], reverse=True):
        p = r["params"]
        print(f"{r['combo']:<10}{str(r['htf'])+'m/'+str(r['ltf'])+'m':<10}"
              f"{p['tsl_base_pct']*100:>7.1f}%{p['tsl_base_lock_pct']*100:>6.1f}%"
              f"{p['tsl_step_pct']*100:>6.1f}%{p['tsl_step_lock_pct']*100:>8.1f}%"
              f"{r['n']:>8}{r['win_pct']:>6.1f}%{r['pf']:>7.2f}Rs{r['net']:>+8,.0f}Rs{r['mdd']:>8,.0f}")

    viable = [r for r in results if r["n"] >= 5]
    pool = viable if viable else results
    best = max(pool, key=lambda r: r["pf"])
    print(f"\nBest combo{'  (n>=5 only)' if viable else '  (NO combo reached n>=5 -- directional only)'}: "
          f"{best['combo']} on HTF={best['htf']}m/LTF={best['ltf']}m "
          f"-- PF={best['pf']:.2f}, win%={best['win_pct']:.1f}%, NET=Rs{best['net']:+,.0f}, n={best['n']}, "
          f"maxDD=Rs{best['mdd']:,.0f}")

    best_net = max(pool, key=lambda r: r["net"])
    if best_net is not best:
        print(f"Highest NET PnL alternative: {best_net['combo']} on HTF={best_net['htf']}m/LTF={best_net['ltf']}m "
              f"-- NET=Rs{best_net['net']:+,.0f}, PF={best_net['pf']:.2f}, n={best_net['n']}")


if __name__ == "__main__":
    main()
