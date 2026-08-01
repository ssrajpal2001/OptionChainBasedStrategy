"""
scripts/fvg_fast_exec_test.py — two fast-execution adjustment tests on the
CLEAN intraday-only dataset (2026-07-23..07-31), per explicit request:

Test Run 1: HTF=10m/LTF=1m, TSL trigger=15%/lock=8%/step=10%/step_lock=5%,
            stagnation=25 candles (~25 min at 1m LTF).
Test Run 2: HTF=10m/LTF=3m, TSL trigger=15%/lock=8%/step=10%/step_lock=5%,
            stagnation=7 candles (~21 min at 3m LTF, tightened from 13/40min).

Both reuse the already-fixed intraday-only discover_signals/resolve_trades
from scripts/fvg_backtest.py (FVG pool wiped daily, same-day-only scan).
"""
import sys
sys.path.insert(0, ".")

import scripts.fvg_backtest as fb

RUNS = [
    ("Test 1: 10m/1m",  dict(htf_mins=10, ltf_mins=1, stagnation_bars=25)),
    ("Test 2: 10m/3m",  dict(htf_mins=10, ltf_mins=3, stagnation_bars=7)),
]
TSL_PARAMS = dict(tsl_base_pct=0.15, tsl_base_lock_pct=0.08, tsl_step_pct=0.10, tsl_step_lock_pct=0.05)


def run(spot, htf_mins, ltf_mins, stagnation_bars):
    signals = fb.discover_signals(spot, htf_mins=htf_mins, ltf_mins=ltf_mins)
    premium_cache = fb.ensure_premium_data(signals)
    trades = fb.resolve_trades(signals, premium_cache, spot, ltf_mins=ltf_mins,
                                stagnation_bars=stagnation_bars, **TSL_PARAMS)
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
    tsl_n = sum(1 for t in trades if t["reason"] == "tsl_hit")
    stag_n = sum(1 for t in trades if t["reason"] == "stagnation_exit")
    sl_n = sum(1 for t in trades if t["reason"] == "sl_hit")
    eod_n = n - tsl_n - stag_n - sl_n
    return dict(n=n, win_pct=win_pct, pf=pf, gross_win=gross_win, gross_loss=gross_loss,
                net=net, mdd=mdd, tsl_n=tsl_n, stag_n=stag_n, sl_n=sl_n, eod_n=eod_n, trades=trades)


def main():
    spot = fb.load_spot()
    results = []
    for name, cfg in RUNS:
        print(f"\n{'='*100}\n{name}  (htf={cfg['htf_mins']}m ltf={cfg['ltf_mins']}m "
              f"stagnation={cfg['stagnation_bars']} candles)\n{'='*100}")
        r = run(spot, **cfg)
        r["name"] = name
        r["cfg"] = cfg
        results.append(r)
        print(f"  n={r['n']}  win%={r['win_pct']:.1f}  PF={r['pf']:.2f}  NET=Rs{r['net']:+,.0f}  "
              f"maxDD=Rs{r['mdd']:,.0f}  TSL={r['tsl_n']} Stagnation={r['stag_n']} SL={r['sl_n']} EOD={r['eod_n']}")
        for t in r["trades"]:
            print(f"    {t['entry_ts']}  {t['direction']:5s}  {t['option_type']}{t['strike']}  "
                  f"entry={t['entry_premium']:.2f}  exit={t['exit_premium']:.2f}@{t['exit_ts']}  "
                  f"({t['reason']})  PnL=Rs{t['pnl_rs']:+,.0f}")

    print(f"\n{'='*100}\nCOMPARISON -- NIFTY, {fb.BACKTEST_START}..{fb.BACKTEST_END}, intraday-only FVGs\n{'='*100}")
    print(f"{'Combo':<16}{'Trades':>8}{'Win%':>8}{'GrossWin':>12}{'GrossLoss':>12}{'PF':>7}"
          f"{'NetPnL':>11}{'MaxDD':>10}{'TSL':>6}{'Stagn':>7}{'SL':>5}{'EOD':>5}")
    for r in results:
        print(f"{r['name']:<16}{r['n']:>8}{r['win_pct']:>7.1f}%Rs{r['gross_win']:>+9,.0f}"
              f"-Rs{r['gross_loss']:>9,.0f}{r['pf']:>7.2f}Rs{r['net']:>+8,.0f}Rs{r['mdd']:>8,.0f}"
              f"{r['tsl_n']:>6}{r['stag_n']:>7}{r['sl_n']:>5}{r['eod_n']:>5}")


if __name__ == "__main__":
    main()
