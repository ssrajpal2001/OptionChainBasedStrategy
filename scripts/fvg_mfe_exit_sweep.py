"""
scripts/fvg_mfe_exit_sweep.py — MFE analysis + exit-style sweep on the clean
7-day intraday-only dataset (HTF=10m/LTF=3m), per explicit request.

Reuses the same 13 signals as the validated baseline (scripts/fvg_backtest.py
discover_signals) and their real fetched option premium. Walks the REAL
1-minute premium ticks (not LTF-bucketed) from entry to day-end for MFE
precision and for the fixed-TP/hybrid exit styles, since those need to catch
the exact tick a target is crossed rather than sampling every 3 minutes.

Exit styles compared (all share entry timing/strikes from Phase 1 signal
discovery -- only the EXIT rule differs):
  A. 40-min stagnation (validated baseline)              -- via fb.resolve_trades
  B. No stagnation, step-locked TSL only                  -- via fb.resolve_trades
  C. Fixed TP +25% (no trail)
  D. Fixed TP +30% (no trail)
  E. Hybrid: book 50% qty at +20%, trail remaining 50% with a 5%-off-peak
     trailing stop (tight stop) once triggered
All non-A/B styles share: hard SL = tighter of -20% premium and the Rs/lot
risk cap; EOD = 15:20 IST.
"""
import sys
sys.path.insert(0, ".")
from datetime import time as dtime

import pandas as pd

import scripts.fvg_backtest as fb

EOD_TIME = dtime(15, 20)
LOT_SIZE = fb.LOT_SIZE


def sl_premium_for(entry_premium):
    pct_sl = entry_premium * (1 - fb.PREMIUM_SL_PCT)
    cap_sl = entry_premium - (fb.MAX_RISK_RS_PER_LOT / LOT_SIZE)
    return max(pct_sl, cap_sl)


def real_ticks_after_entry(df, entry_ts, day):
    """Real (near-)1-minute premium ticks strictly after entry_ts, through
    EOD_TIME on the same day."""
    day_end = pd.Timestamp.combine(day, EOD_TIME).tz_localize(df["datetime"].dt.tz)
    return df[(df["datetime"] > entry_ts) & (df["datetime"] <= day_end)].reset_index(drop=True)


def compute_mfe(df, entry_ts, entry_premium, day):
    ticks = real_ticks_after_entry(df, entry_ts, day)
    if ticks.empty:
        return 0.0, entry_ts
    peak_idx = ticks["close"].idxmax()
    peak = ticks.loc[peak_idx, "close"]
    peak_ts = ticks.loc[peak_idx, "datetime"]
    mfe_pct = (peak - entry_premium) / entry_premium * 100
    return mfe_pct, peak_ts


def resolve_fixed_tp(df, entry_ts, entry_premium, day, tp_pct):
    sl = sl_premium_for(entry_premium)
    tp = entry_premium * (1 + tp_pct)
    ticks = real_ticks_after_entry(df, entry_ts, day)
    for _, row in ticks.iterrows():
        px = row["close"]
        if px <= sl:
            return sl, row["datetime"], "sl_hit"
        if px >= tp:
            return tp, row["datetime"], "tp_hit"
    if ticks.empty:
        return entry_premium, entry_ts, "eod_final"
    last = ticks.iloc[-1]
    return last["close"], last["datetime"], "eod"


def resolve_hybrid(df, entry_ts, entry_premium, day, book_pct=0.20, trail_pct=0.05):
    """Book 50% qty at +book_pct gain; remaining 50% trails at trail_pct
    below its running peak once the book trigger has fired. Before the
    trigger, the whole position shares the hard SL."""
    sl = sl_premium_for(entry_premium)
    book_price = entry_premium * (1 + book_pct)
    ticks = real_ticks_after_entry(df, entry_ts, day)

    booked = False
    peak = entry_premium
    half_a_exit = None   # (price, ts, reason)
    half_b_exit = None

    for _, row in ticks.iterrows():
        px, ts = row["close"], row["datetime"]
        if not booked:
            if px <= sl:
                half_a_exit = half_b_exit = (sl, ts, "sl_hit")
                break
            if px >= book_price:
                booked = True
                half_a_exit = (book_price, ts, "tp_book_50pct")
                peak = px
                continue
        else:
            peak = max(peak, px)
            trail_stop = peak * (1 - trail_pct)
            if px <= trail_stop:
                half_b_exit = (px, ts, "trail_stop_hit")
                break

    if half_a_exit is None:
        # SL never hit, TP never booked, ran to EOD unbooked
        if ticks.empty:
            half_a_exit = half_b_exit = (entry_premium, entry_ts, "eod_final")
        else:
            last = ticks.iloc[-1]
            half_a_exit = half_b_exit = (last["close"], last["datetime"], "eod")
    elif half_b_exit is None:
        # booked at +20%, but remaining half never hit trail stop -> EOD
        if ticks.empty:
            half_b_exit = (entry_premium, entry_ts, "eod_final")
        else:
            last = ticks.iloc[-1]
            half_b_exit = (last["close"], last["datetime"], "eod")

    return half_a_exit, half_b_exit


def build_trade(sig, entry_premium, exit_premium, exit_ts, reason, qty_fraction=1.0):
    pnl_rs = (exit_premium - entry_premium) * LOT_SIZE * qty_fraction
    return dict(direction=sig["direction"], entry_ts=sig["entry_ts"], strike=sig["strike"],
                option_type=sig["option_type"], entry_premium=entry_premium, exit_premium=exit_premium,
                exit_ts=exit_ts, reason=reason, pnl_rs=pnl_rs)


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
    print(f"{label:<28} n={n:>3}  win%={win_pct:>5.1f}  PF={pf:>5.2f}  gross_win=Rs{gw:>+8,.0f}  "
          f"gross_loss=-Rs{gl:>8,.0f}  NET=Rs{net:>+8,.0f}  maxDD=Rs{mdd:>8,.0f}")
    return dict(label=label, n=n, win_pct=win_pct, pf=pf, gw=gw, gl=gl, net=net, mdd=mdd)


def main():
    spot = fb.load_spot()
    print("Discovering baseline signals...")
    signals = fb.discover_signals(spot)
    premium_cache = fb.ensure_premium_data(signals)
    print(f"  {len(signals)} signals\n")

    TSL = dict(tsl_base_pct=0.15, tsl_base_lock_pct=0.08, tsl_step_pct=0.10, tsl_step_lock_pct=0.05)

    # ── MFE table ────────────────────────────────────────────────────────────
    print("=" * 100)
    print("MFE (Maximum Favorable Excursion) -- peak % gain reached during each trade's life")
    print("=" * 100)
    print(f"{'Entry TS':<22}{'Option':<10}{'Entry Prem':>11}{'MFE %':>9}{'MFE TS':<22}")
    mfe_rows = []
    for sig in signals:
        df = premium_cache.get((sig["strike"], sig["option_type"]))
        entry_premium = fb.price_at(df, sig["entry_ts"])
        mfe_pct, mfe_ts = compute_mfe(df, sig["entry_ts"], entry_premium, sig["day"])
        mfe_rows.append(dict(sig=sig, entry_premium=entry_premium, mfe_pct=mfe_pct, mfe_ts=mfe_ts))
        print(f"{str(sig['entry_ts']):<22}{sig['option_type']+str(sig['strike']):<10}"
              f"{entry_premium:>11.2f}{mfe_pct:>8.1f}%{str(mfe_ts):<22}")

    # ── Exit style sweep ────────────────────────────────────────────────────
    print(f"\n{'='*100}\nEXIT STYLE SWEEP (no stagnation on C/D/E)\n{'='*100}")

    results = []

    # A. baseline (40min stagnation)
    trades_a = fb.resolve_trades(signals, premium_cache, spot, stagnation_bars=13, **TSL)
    results.append(summarize(trades_a, "A. 40-min Stagnation (base)"))

    # B. no stagnation, TSL only
    trades_b = fb.resolve_trades(signals, premium_cache, spot, stagnation_bars=None, eod_time=EOD_TIME, **TSL)
    results.append(summarize(trades_b, "B. No Stagnation, TSL only"))

    # C. Fixed TP +25%
    trades_c = []
    for sig in signals:
        df = premium_cache.get((sig["strike"], sig["option_type"]))
        entry_premium = fb.price_at(df, sig["entry_ts"])
        exit_px, exit_ts, reason = resolve_fixed_tp(df, sig["entry_ts"], entry_premium, sig["day"], 0.25)
        trades_c.append(build_trade(sig, entry_premium, exit_px, exit_ts, reason))
    results.append(summarize(trades_c, "C. Fixed TP +25%"))

    # D. Fixed TP +30%
    trades_d = []
    for sig in signals:
        df = premium_cache.get((sig["strike"], sig["option_type"]))
        entry_premium = fb.price_at(df, sig["entry_ts"])
        exit_px, exit_ts, reason = resolve_fixed_tp(df, sig["entry_ts"], entry_premium, sig["day"], 0.30)
        trades_d.append(build_trade(sig, entry_premium, exit_px, exit_ts, reason))
    results.append(summarize(trades_d, "D. Fixed TP +30%"))

    # E. Hybrid 50% @ +20% + trail remaining 50% (5%% off peak)
    trades_e = []
    for sig in signals:
        df = premium_cache.get((sig["strike"], sig["option_type"]))
        entry_premium = fb.price_at(df, sig["entry_ts"])
        (px_a, ts_a, reason_a), (px_b, ts_b, reason_b) = resolve_hybrid(df, sig["entry_ts"], entry_premium, sig["day"])
        trades_e.append(build_trade(sig, entry_premium, px_a, ts_a, reason_a, qty_fraction=0.5))
        trades_e.append(build_trade(sig, entry_premium, px_b, ts_b, reason_b, qty_fraction=0.5))
    results.append(summarize(trades_e, "E. Hybrid 50%@+20% + trail"))

    print(f"\n{'='*100}\nSUMMARY TABLE\n{'='*100}")
    print(f"{'Exit Method':<28}{'Trades':>8}{'Win%':>8}{'GrossWin':>12}{'GrossLoss':>12}{'PF':>7}{'NetPnL':>11}{'MaxDD':>10}")
    for r in results:
        print(f"{r['label']:<28}{r['n']:>8}{r['win_pct']:>7.1f}%Rs{r['gw']:>+9,.0f}-Rs{r['gl']:>9,.0f}"
              f"{r['pf']:>7.2f}Rs{r['net']:>+8,.0f}Rs{r['mdd']:>8,.0f}")


if __name__ == "__main__":
    main()
