"""
scripts/fvg_next_week_expiry_test.py — theta-decay hypothesis test: does
trading the NEXT-WEEK expiry contract (lower theta decay) instead of the
current-week one improve results on the same 13 intraday signals?

Same spot-based signal discovery (entries/strikes/timing unchanged --
entries only depend on spot price action) but exits are resolved against
NEXT-WEEK expiry premium (fetched via scripts/fvg_next_week_expiry_fetch.py,
expiry resolved through REGISTRY.get_active_expiry(), never a hardcoded
calendar date).

Variations:
  A. Next-week expiry, WITH 40-min stagnation exit.
  B. Next-week expiry, WITHOUT stagnation (EOD=15:20).
  C. Next-week expiry, Fixed TP +20% (no stagnation).
All share: hard SL = tighter of -20% premium and the Rs/lot risk cap;
TSL (A/B only) trigger=15%/lock=8%/step=10%/step_lock=5%.
"""
import sys
sys.path.insert(0, ".")
from datetime import time as dtime

import pandas as pd

import scripts.fvg_backtest as fb
import scripts.fvg_mfe_exit_sweep as mfe

NEXTWEEK_DIR = "data/d1trap_fractal_cache/fvg_nextweek_strikes"
EOD_TIME = dtime(15, 20)
TSL = dict(tsl_base_pct=0.15, tsl_base_lock_pct=0.08, tsl_step_pct=0.10, tsl_step_lock_pct=0.05)


def load_nextweek_cache(signals):
    cache = {}
    for sig in signals:
        key = (sig["strike"], sig["option_type"])
        if key in cache:
            continue
        path = f"{NEXTWEEK_DIR}/{sig['strike']}_{sig['option_type']}.parquet"
        try:
            df = pd.read_parquet(path)
            df["datetime"] = pd.to_datetime(df["datetime"])
            cache[key] = df.sort_values("datetime").reset_index(drop=True)
        except FileNotFoundError:
            print(f"  MISSING next-week premium file for {sig['option_type']}{sig['strike']}")
            cache[key] = pd.DataFrame(columns=["datetime", "open", "high", "low", "close"])
    return cache


def breakdown(trades):
    tsl_n = sum(1 for t in trades if t["reason"] == "tsl_hit")
    sl_n = sum(1 for t in trades if t["reason"] == "sl_hit")
    stag_n = sum(1 for t in trades if t["reason"] == "stagnation_exit")
    eod_n = sum(1 for t in trades if t["reason"] in ("eod", "eod_final"))
    tp_n = sum(1 for t in trades if t["reason"] == "tp_hit")
    return tsl_n, sl_n, stag_n, eod_n, tp_n


def summarize(trades, label):
    n = len(trades)
    wins = [t for t in trades if t["pnl_rs"] > 0]
    losses = [t for t in trades if t["pnl_rs"] <= 0]
    gw = sum(t["pnl_rs"] for t in wins)
    gl = abs(sum(t["pnl_rs"] for t in losses))
    pf = gw / gl if gl > 0 else float("inf")
    net = sum(t["pnl_rs"] for t in trades)
    win_pct = 100 * len(wins) / n if n else 0
    mdd = fb.max_drawdown(sorted(trades, key=lambda t: t["exit_ts"]))
    tsl_n, sl_n, stag_n, eod_n, tp_n = breakdown(trades)
    print(f"{label:<38} n={n:>3}  win%={win_pct:>5.1f}  PF={pf:>5.2f}  gross_win=Rs{gw:>+8,.0f}  "
          f"gross_loss=-Rs{gl:>8,.0f}  NET=Rs{net:>+8,.0f}  maxDD=Rs{mdd:>8,.0f}  "
          f"TSL={tsl_n} SL={sl_n} Stag={stag_n} EOD={eod_n} TP={tp_n}")
    return dict(label=label, n=n, win_pct=win_pct, pf=pf, gw=gw, gl=gl, net=net, mdd=mdd,
                tsl_n=tsl_n, sl_n=sl_n, stag_n=stag_n, eod_n=eod_n, tp_n=tp_n)


def main():
    spot = fb.load_spot()
    print("Discovering the same 13 baseline signals (spot-only, unchanged)...")
    signals = fb.discover_signals(spot)
    print(f"  {len(signals)} signals\n")

    print("Loading NEXT-WEEK expiry premium cache...")
    nextweek_cache = load_nextweek_cache(signals)
    print()

    print("=" * 110)
    print("NEXT-WEEK EXPIRY THETA-DECAY TEST")
    print("=" * 110)

    results = []

    # Variation A: next-week expiry, 40-min stagnation
    trades_a = fb.resolve_trades(signals, nextweek_cache, spot, stagnation_bars=13,
                                  eod_time=EOD_TIME, **TSL)
    results.append(summarize(trades_a, "A. Next-Week + 40min Stagnation"))

    # Variation B: next-week expiry, no stagnation, EOD 15:20
    trades_b = fb.resolve_trades(signals, nextweek_cache, spot, stagnation_bars=None,
                                  eod_time=EOD_TIME, **TSL)
    results.append(summarize(trades_b, "B. Next-Week + No Stagnation (EOD)"))

    # Variation C: next-week expiry, fixed TP +20%, no stagnation
    trades_c = []
    for sig in signals:
        df = nextweek_cache.get((sig["strike"], sig["option_type"]))
        if df is None or df.empty:
            continue
        entry_premium = fb.price_at(df, sig["entry_ts"])
        if entry_premium is None or entry_premium <= 0:
            continue
        exit_px, exit_ts, reason = mfe.resolve_fixed_tp(df, sig["entry_ts"], entry_premium, sig["day"], 0.20)
        trades_c.append(mfe.build_trade(sig, entry_premium, exit_px, exit_ts, reason))
    results.append(summarize(trades_c, "C. Next-Week + Fixed TP +20%"))

    # Reference: current-week baseline (already validated) for direct comparison
    current_cache = fb.ensure_premium_data(signals)
    trades_ref = fb.resolve_trades(signals, current_cache, spot, stagnation_bars=13, **TSL)
    results.append(summarize(trades_ref, "Reference: Current-Week + 40min Stag"))

    print(f"\n{'='*110}\nSUMMARY TABLE\n{'='*110}")
    print(f"{'Expiry / Exit Variation':<38}{'Trades':>7}{'Win%':>7}{'GrossWin':>11}{'GrossLoss':>11}{'PF':>6}"
          f"{'NetPnL':>10}{'MaxDD':>9}{'SL':>4}{'TSL':>5}{'Stag':>6}{'EOD':>5}{'TP':>4}")
    for r in results:
        print(f"{r['label']:<38}{r['n']:>7}{r['win_pct']:>6.1f}%Rs{r['gw']:>+8,.0f}-Rs{r['gl']:>8,.0f}"
              f"{r['pf']:>6.2f}Rs{r['net']:>+7,.0f}Rs{r['mdd']:>6,.0f}"
              f"{r['sl_n']:>4}{r['tsl_n']:>5}{r['stag_n']:>6}{r['eod_n']:>5}{r['tp_n']:>4}")


if __name__ == "__main__":
    main()
