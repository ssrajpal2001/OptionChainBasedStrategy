"""
scripts/fvg_tf_sweep.py — timeframe optimization sweep for the FVG strategy,
per explicit request to find which HTF/LTF combo suits intraday scalping
best under the option-native exit rules (20% premium SL, 1:2 R:R, stagnation
exit) already validated in scripts/fvg_backtest.py.

Same 3-phase design (signal discovery on spot -> fetch only the strikes
actually needed -> resolve option-native exits against real premium), run
once per (HTF, LTF) combo over the same 7-day window
(2026-07-23..2026-07-31) so every combo is judged on identical market
conditions. Stagnation window is held at a fixed ~40 REAL minutes across
combos (bars = max(1, 40 // ltf_mins)) so the "avoid theta bleed after 40
minutes rangebound" intent stays constant while the candle granularity
underneath it changes -- otherwise a faster LTF would get an unfairly
tighter (or slower LTF an unfairly looser) stagnation window in bar terms.
"""
import sys
sys.path.insert(0, ".")

import scripts.fvg_backtest as fb

# (htf_mins, ltf_mins) candidates: current baseline down to 1-min scalping.
# HTF stays >= LTF and >=3 (need enough bars for a meaningful 5-bar swing
# fractal); LTF is what actually drives FVG detection + entry timing.
COMBOS = [
    (15, 5),   # current baseline
    (15, 3),
    (15, 1),
    (10, 3),
    (10, 1),
    (5, 3),
    (5, 1),
    (3, 1),
]

STAGNATION_MINUTES = 40


def run_combo(spot, htf_mins, ltf_mins):
    stagnation_bars = max(1, STAGNATION_MINUTES // ltf_mins)
    signals = fb.discover_signals(spot, htf_mins=htf_mins, ltf_mins=ltf_mins)
    if not signals:
        return dict(htf=htf_mins, ltf=ltf_mins, n=0, win_pct=0, pf=0, gross_win=0,
                    gross_loss=0, net=0, mdd=0, trades=[])
    premium_cache = fb.ensure_premium_data(signals)
    trades = fb.resolve_trades(signals, premium_cache, spot, ltf_mins=ltf_mins,
                                stagnation_bars=stagnation_bars)
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
    return dict(htf=htf_mins, ltf=ltf_mins, n=n, win_pct=win_pct, pf=pf,
                gross_win=gross_win, gross_loss=gross_loss, net=net, mdd=mdd, trades=trades)


def main():
    spot = fb.load_spot()
    results = []
    for htf_mins, ltf_mins in COMBOS:
        print(f"\n{'='*90}\nHTF={htf_mins}m / LTF={ltf_mins}m  (stagnation={max(1, STAGNATION_MINUTES // ltf_mins)} "
              f"bars ~= {STAGNATION_MINUTES}min)\n{'='*90}")
        r = run_combo(spot, htf_mins, ltf_mins)
        results.append(r)
        print(f"  n={r['n']}  win%={r['win_pct']:.1f}  PF={r['pf']:.2f}  "
              f"gross_win=Rs{r['gross_win']:+,.0f}  gross_loss=-Rs{r['gross_loss']:,.0f}  "
              f"NET=Rs{r['net']:+,.0f}  maxDD=Rs{r['mdd']:,.0f}")

    print(f"\n{'='*90}\nTIMEFRAME SWEEP COMPARISON -- NIFTY, {fb.BACKTEST_START}..{fb.BACKTEST_END}, "
          f"1-strike ITM, 20% premium SL / 1:2 R:R, ~40min stagnation guard\n{'='*90}")
    print(f"{'HTF':>5}{'LTF':>5}{'Trades':>8}{'Win%':>8}{'PF':>8}{'GrossWin':>12}{'GrossLoss':>12}"
          f"{'NetPnL':>12}{'MaxDD':>10}")
    for r in sorted(results, key=lambda r: r["pf"], reverse=True):
        print(f"{r['htf']:>4}m{r['ltf']:>4}m{r['n']:>8}{r['win_pct']:>7.1f}%{r['pf']:>8.2f}"
              f"Rs{r['gross_win']:>+9,.0f}-Rs{r['gross_loss']:>9,.0f}Rs{r['net']:>+9,.0f}"
              f"Rs{r['mdd']:>8,.0f}")

    viable = [r for r in results if r["n"] >= 5]
    if viable:
        best = max(viable, key=lambda r: r["pf"])
        print(f"\nBest combo with a usable sample (n>=5): HTF={best['htf']}m/LTF={best['ltf']}m "
              f"-- PF={best['pf']:.2f}, win%={best['win_pct']:.1f}%, NET=Rs{best['net']:+,.0f}, n={best['n']}")
    else:
        best = max(results, key=lambda r: r["pf"])
        print(f"\nNo combo reached n>=5 trades in this 7-day window -- treat ALL results as "
              f"directional only. Highest PF: HTF={best['htf']}m/LTF={best['ltf']}m "
              f"(PF={best['pf']:.2f}, n={best['n']}).")


if __name__ == "__main__":
    main()
