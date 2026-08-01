"""
scripts/fvg_backtest_real_premium.py — re-run the FVG backtest's trade list
(entries/exits are still spot-structure-based, exactly as
scripts/fvg_backtest.py and the live engine compute them) but replace the
illustrative 0.5-delta P&L with REAL option premium looked up at each
trade's actual entry_ts/exit_ts from real Upstox 1-min option data
(fetched via scripts/fvg_fetch_trade_strikes.py).
"""
import sys
sys.path.insert(0, ".")
import io
import contextlib

import pandas as pd

import scripts.fvg_backtest as fb

STRIKE_DIR = "data/d1trap_fractal_cache/fvg_trade_strikes"
LOT_SIZE = 65


def load_premium(strike, side):
    df = pd.read_parquet(f"{STRIKE_DIR}/{strike}_{side}.parquet")
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.sort_values("datetime").reset_index(drop=True)


def price_at(df, ts):
    """Nearest available candle's close AT OR BEFORE ts (a live fill can't see
    the future) -- falls back to the nearest available candle overall if ts
    is before the series starts (thin/illiquid strike)."""
    before = df[df["datetime"] <= ts]
    if not before.empty:
        return float(before.iloc[-1]["close"])
    after = df[df["datetime"] >= ts]
    if not after.empty:
        return float(after.iloc[0]["close"])
    return None


def main():
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        trades = fb.main()

    cache = {}
    rows = []
    missing = 0
    for t in trades:
        key = (t["strike"], t["option_type"])
        if key not in cache:
            cache[key] = load_premium(*key)
        df = cache[key]
        entry_prem = price_at(df, t["entry_ts"])
        exit_prem = price_at(df, t["exit_ts"])
        if entry_prem is None or exit_prem is None:
            missing += 1
            continue
        pnl_rs_real = (exit_prem - entry_prem) * LOT_SIZE
        rows.append(dict(t, entry_prem=entry_prem, exit_prem=exit_prem, pnl_rs_real=pnl_rs_real))

    n = len(rows)
    wins = [r for r in rows if r["pnl_rs_real"] > 0]
    losses = [r for r in rows if r["pnl_rs_real"] <= 0]
    gross_win = sum(r["pnl_rs_real"] for r in wins)
    gross_loss = abs(sum(r["pnl_rs_real"] for r in losses))
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")
    net = sum(r["pnl_rs_real"] for r in rows)
    win_pct = 100 * len(wins) / n if n else 0

    print(f"FVG backtest -- REAL option premium P&L, NIFTY, 2026-06-29..07-31 "
          f"({n} trades, {missing} skipped -- no premium data)\n")
    print(f"n={n}  win%={win_pct:.1f}  PF={pf:.2f}  gross_win=Rs{gross_win:+,.0f}  "
          f"gross_loss=-Rs{gross_loss:,.0f}  NET(real)=Rs{net:+,.0f}\n")

    print(f"{'Entry TS':<22}{'Dir':<6}{'Opt':<9}{'Spot Entry':>11}{'Spot Exit':>11}{'Reason':<9}"
          f"{'Prem Entry':>11}{'Prem Exit':>10}{'PnL(real)':>12}{'PnL(0.5d est)':>15}")
    for r in rows:
        print(f"{str(r['entry_ts']):<22}{r['direction']:<6}{r['option_type']+str(r['strike']):<9}"
              f"{r['entry']:>11.2f}{r['exit']:>11.2f}{r['reason']:<9}"
              f"{r['entry_prem']:>11.2f}{r['exit_prem']:>10.2f}"
              f"Rs{r['pnl_rs_real']:>+9,.0f}  Rs{r['pnl_rs_est']:>+9,.0f}")

    real_net = net
    est_net = sum(r["pnl_rs_est"] for r in rows)
    print(f"\nReal-premium NET = Rs{real_net:+,.0f}   vs   0.5-delta-estimate NET = Rs{est_net:+,.0f}"
          f"   (diff = Rs{real_net - est_net:+,.0f})")


if __name__ == "__main__":
    main()
