"""
Re-run the locked config (60m/15m/5m, swing_breach, continuation-only, raw_breakout,
threshold 0.20%) restricted to entries in the last month (2026-06-29 -> 2026-07-29),
but instead of spot-points P&L, look up the REAL 1-ITM CE/PE option premium (Aug-4 2026
expiry) at entry_ts and exit_ts and compute actual option P&L. Same signal timing
(spot-based, already validated) -- only the fill price and P&L basis change.
"""
import sys, os
sys.path.insert(0, ".")
import scripts.d1trap_fractal_backtest as bt
import pandas as pd

CACHE_DIR = bt.CACHE_DIR
OPT_DIR = os.path.join(CACHE_DIR, "aug4_options")
LOT_SIZE = 65
STRIKE_STEP = 50

_opt_cache = {}


def load_option(strike: int, side: str) -> pd.DataFrame:
    key = (strike, side)
    if key not in _opt_cache:
        path = os.path.join(OPT_DIR, f"{strike}_{side}.parquet")
        df = pd.read_parquet(path)
        df["datetime"] = pd.to_datetime(df["datetime"])
        _opt_cache[key] = df.sort_values("datetime").reset_index(drop=True)
    return _opt_cache[key]


def price_at(df: pd.DataFrame, ts) -> float:
    """Nearest available 1-min close at/after ts (falls back to nearest before if none after)."""
    after = df[df["datetime"] >= ts]
    if not after.empty:
        return float(after.iloc[0]["close"])
    before = df[df["datetime"] <= ts]
    if not before.empty:
        return float(before.iloc[-1]["close"])
    return None


def main():
    d1, m1 = bt.load_data("2025-07-30_2026-07-29")
    resamples = {}
    m5 = bt.get_resample(5, m1, resamples)
    d1_bars = bt.to_bars(d1)

    htf_m, ref_m = 60, 15
    htf_bars = bt.build_htf_bars(htf_m, d1_bars, m1, resamples)
    m_ref = bt.get_resample(ref_m, m1, resamples)
    zones_tf = bt.detect_d1_zones(htf_bars)

    cfg = bt.Config(zone_size_threshold_pct=0.20, enable_continuation=True, enable_retest=False,
                     fallback_on_no_subzone="raw_breakout", htf_minutes=htf_m, ref_minutes=ref_m,
                     sub_minutes=5, entry_mode="swing_breach")
    all_trades = bt.run_backtest(zones_tf, m1, m_ref, m5, m5, cfg)

    window_start = pd.Timestamp("2026-06-29", tz=bt.IST)
    window_end = pd.Timestamp("2026-07-29 23:59:59", tz=bt.IST)
    trades = [t for t in all_trades if window_start <= t["entry_ts"] <= window_end]
    print(f"Trades in window (spot-signal entries, last month): {len(trades)}\n")

    m1_idx = m1.set_index("datetime")

    results = []
    for t in trades:
        entry_ts, exit_ts = t["entry_ts"], t["exit_ts"]
        spot_row = m1[m1["datetime"] <= entry_ts]
        if spot_row.empty:
            continue
        spot_at_entry = spot_row.iloc[-1]["close"]
        atm = round(spot_at_entry / STRIKE_STEP) * STRIKE_STEP
        if t["direction"] == "LONG":
            strike, side = int(atm - STRIKE_STEP), "CE"
        else:
            strike, side = int(atm + STRIKE_STEP), "PE"

        opt_path = os.path.join(OPT_DIR, f"{strike}_{side}.parquet")
        if not os.path.exists(opt_path):
            print(f"  SKIP {entry_ts} strike={strike}{side}: no cached option data (out of fetched range)")
            continue
        opt_df = load_option(strike, side)
        entry_opt = price_at(opt_df, entry_ts)
        exit_opt = price_at(opt_df, exit_ts)
        if entry_opt is None or exit_opt is None:
            print(f"  SKIP {entry_ts} strike={strike}{side}: no option price available at entry/exit")
            continue

        pnl_rs = (exit_opt - entry_opt) * LOT_SIZE
        results.append(dict(
            entry_ts=entry_ts, direction=t["direction"], origin=t["origin"],
            strike=strike, side=side, spot_entry=spot_at_entry,
            spot_exit_reason=t["reason"], entry_opt=entry_opt, exit_opt=exit_opt,
            pnl_rs=pnl_rs,
        ))

    print(f"Trades with real option fills: {len(results)}\n")
    if not results:
        return

    wins = [r for r in results if r["pnl_rs"] > 0]
    losses = [r for r in results if r["pnl_rs"] <= 0]
    gw = sum(r["pnl_rs"] for r in wins)
    gl = abs(sum(r["pnl_rs"] for r in losses))
    pf = gw / gl if gl > 0 else (99.0 if gw > 0 else 0.0)
    total = sum(r["pnl_rs"] for r in results)
    print(f"n={len(results)}  win%={100*len(wins)/len(results):.1f}  "
          f"Rs{total:+,.0f}  PF={pf:.2f}  avg_win=Rs{gw/len(wins) if wins else 0:,.0f}  "
          f"avg_loss=Rs{-gl/len(losses) if losses else 0:,.0f}")
    print()
    for r in results:
        print(f"  {r['entry_ts']}  {r['direction']:>5}  {r['origin']:>12}  "
              f"{r['strike']}{r['side']}  spot={r['spot_entry']:.1f}  "
              f"opt_entry={r['entry_opt']:.2f} opt_exit={r['exit_opt']:.2f}  "
              f"({r['spot_exit_reason']})  Rs{r['pnl_rs']:+,.0f}")


if __name__ == "__main__":
    main()
