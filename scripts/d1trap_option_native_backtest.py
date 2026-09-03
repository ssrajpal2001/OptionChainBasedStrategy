"""
Same entry signals (spot-based fractal timing, already validated) as
d1trap_option_premium_backtest.py, but exits are now OPTION-NATIVE:
  - hard SL: premium falls sl_pct below entry premium
  - TSL: once premium rises tsl_activate_pct above entry, trail
    tsl_trail_pct below the running peak premium
  - EOD force exit 15:15 IST (MIS), same as before
No fixed target -- TSL manages upside, matching how the underlying
mechanic already works (trail, don't fix a target).
"""
import sys, os
sys.path.insert(0, ".")
import scripts.d1trap_fractal_backtest as bt
import pandas as pd
from datetime import time

CACHE_DIR = bt.CACHE_DIR
OPT_DIR = os.path.join(CACHE_DIR, "aug4_options")
LOT_SIZE = 65
STRIKE_STEP = 50
EOD_TIME = time(15, 15)

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
    after = df[df["datetime"] >= ts]
    if not after.empty:
        return float(after.iloc[0]["close"])
    before = df[df["datetime"] <= ts]
    if not before.empty:
        return float(before.iloc[-1]["close"])
    return None


def simulate_option_native_exit(opt_df: pd.DataFrame, entry_ts, entry_opt: float,
                                 sl_pct: float, tsl_activate_pct: float, tsl_trail_pct: float) -> dict:
    entry_day = entry_ts.date()
    eod_ts = entry_ts.replace(hour=EOD_TIME.hour, minute=EOD_TIME.minute, second=0, microsecond=0)
    after = opt_df[(opt_df["datetime"] > entry_ts) & (opt_df["datetime"].dt.date == entry_day)]

    sl_level = entry_opt * (1 - sl_pct)
    peak = entry_opt
    armed = False
    trail_level = None

    for bar in after.itertuples(index=False):
        if bar.datetime >= eod_ts:
            break
        px = bar.close
        if px <= sl_level:
            reason = "tsl_hit" if armed else "sl_hit"
            return dict(exit_ts=bar.datetime, exit_price=sl_level if not armed else trail_level,
                        reason=reason)
        peak = max(peak, px)
        if not armed and peak >= entry_opt * (1 + tsl_activate_pct):
            armed = True
            trail_level = peak * (1 - tsl_trail_pct)
        elif armed:
            trail_level = max(trail_level, peak * (1 - tsl_trail_pct))
            if px <= trail_level:
                return dict(exit_ts=bar.datetime, exit_price=trail_level, reason="tsl_hit")

    eod_rows = opt_df[(opt_df["datetime"] >= eod_ts) & (opt_df["datetime"].dt.date == entry_day)]
    if not eod_rows.empty:
        return dict(exit_ts=eod_ts, exit_price=eod_rows.iloc[0]["close"], reason="eod")
    last = opt_df[(opt_df["datetime"].dt.date == entry_day) & (opt_df["datetime"] > entry_ts)]
    if not last.empty:
        row = last.iloc[-1]
        return dict(exit_ts=row["datetime"], exit_price=row["close"], reason="data_end")
    return dict(exit_ts=entry_ts, exit_price=entry_opt, reason="no_data")


def run(sl_pct: float, tsl_activate_pct: float, tsl_trail_pct: float, verbose=True):
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

    results = []
    for t in trades:
        entry_ts = t["entry_ts"]
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
            continue
        opt_df = load_option(strike, side)
        entry_opt = price_at(opt_df, entry_ts)
        if entry_opt is None or entry_opt <= 0:
            continue

        exitr = simulate_option_native_exit(opt_df, entry_ts, entry_opt, sl_pct,
                                             tsl_activate_pct, tsl_trail_pct)
        pnl_rs = (exitr["exit_price"] - entry_opt) * LOT_SIZE
        results.append(dict(entry_ts=entry_ts, direction=t["direction"], origin=t["origin"],
                             strike=strike, side=side, entry_opt=entry_opt,
                             exit_opt=exitr["exit_price"], reason=exitr["reason"], pnl_rs=pnl_rs))

    if not results:
        print("No trades.")
        return

    wins = [r for r in results if r["pnl_rs"] > 0]
    losses = [r for r in results if r["pnl_rs"] <= 0]
    gw = sum(r["pnl_rs"] for r in wins)
    gl = abs(sum(r["pnl_rs"] for r in losses))
    pf = gw / gl if gl > 0 else (99.0 if gw > 0 else 0.0)
    total = sum(r["pnl_rs"] for r in results)
    print(f"sl={sl_pct:.0%} tsl_act={tsl_activate_pct:.0%} tsl_trail={tsl_trail_pct:.0%}  "
          f"n={len(results)}  win%={100*len(wins)/len(results):.1f}  "
          f"Rs{total:+,.0f}  PF={pf:.2f}  avg_win=Rs{gw/len(wins) if wins else 0:,.0f}  "
          f"avg_loss=Rs{-gl/len(losses) if losses else 0:,.0f}", flush=True)
    if verbose:
        for r in results:
            print(f"    {r['entry_ts']}  {r['direction']:>5}  {r['origin']:>12}  "
                  f"{r['strike']}{r['side']}  entry={r['entry_opt']:.2f} exit={r['exit_opt']:.2f}  "
                  f"({r['reason']})  Rs{r['pnl_rs']:+,.0f}")
    return dict(n=len(results), win_pct=100*len(wins)/len(results), total_rs=total, pf=pf)


if __name__ == "__main__":
    print("Default config (sl=30%, tsl activate=20%, trail=15%):")
    run(0.30, 0.20, 0.15)
