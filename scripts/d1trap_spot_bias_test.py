"""
scripts/d1trap_spot_bias_test.py -- test whether filtering D1Trap entries by
a higher-timeframe SPOT directional bias (only take CE when spot HTF bias
is bullish, only PE when bearish) improves results, vs no filter.

Bias computed via FVG's already-built, already-unit-tested swing/MSS
detector (strategies/fvg/detector.py) applied to DAILY spot bars -- reused
as-is, not reinvented: find_swing_points + detect_mss in both directions;
whichever MSS is most recent (higher bar index) sets that day's bias.
No bias (neither/ambiguous) = no filter that day.

Methodology: run the exact same live mechanic UNCHANGED (today's validated
defaults: NIFTY 60m/150pt, SENSEX 15m/300pt, ref.close boundary) to collect
every trade exactly as before, then compare NET/PF with vs without
excluding trades whose side disagrees with that day's spot bias --
equivalent to gating entries, without touching the entry pipeline itself.
"""
import sys
sys.path.insert(0, ".")
from datetime import timedelta, date
import pandas as pd
import strategies.d1_trap_option.bear_only_book as bb
import scripts.d1trap_month_rolling_backtest as mrb
import scripts.d1trap_verify_live_defaults as verify
from strategies.fvg.detector import find_swing_points, detect_mss

LADDER_DIR = "data/d1trap_fractal_cache/strike_ladder"

CONFIGS = {
    "NIFTY":  dict(fname_prefix="niftyladder", offset=150, round_step=100, lot=65, htf=60,
                   spot_path="data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet"),
    "SENSEX": dict(fname_prefix="sensexladder", offset=300, round_step=100, lot=20, htf=15,
                   spot_path="data/d1trap_fractal_cache/sensex_1m_fullmonth_spot.parquet"),
}

BIAS_LOOKBACK_DAYS = 20


def daily_bias_series(spot_df):
    """For each trading day in spot_df, compute that day's HTF bias from the
    prior BIAS_LOOKBACK_DAYS of DAILY bars (not including today) -- returns
    {day: "UP"|"DOWN"|None}."""
    df = spot_df.sort_values("datetime").reset_index(drop=True)
    daily = bb._resample(df, 1440)  # 1440min = D1
    daily_bars = bb._to_bars(daily)
    days = sorted(df["datetime"].dt.date.unique())
    bias = {}
    for day in days:
        # daily bars strictly before `day`
        hist_bars = [b for b in daily_bars if b.timestamp.date() < day]
        hist_bars = hist_bars[-BIAS_LOOKBACK_DAYS:]
        if len(hist_bars) < 6:  # need enough bars for a 5-bar pivot to exist at all
            bias[day] = None
            continue
        swings = find_swing_points(hist_bars, pivot=2)
        bull = detect_mss(hist_bars, swings, "BULLISH")
        bear = detect_mss(hist_bars, swings, "BEARISH")
        if bull and (not bear or bull.index > bear.index):
            bias[day] = "UP"
        elif bear and (not bull or bear.index > bull.index):
            bias[day] = "DOWN"
        else:
            bias[day] = None
    return bias


def apply_bias_filter(trades, bias_by_day):
    """CE trades only kept if that day's bias is UP (or None=unfiltered);
    PE trades only kept if DOWN (or None)."""
    kept = []
    for t in trades:
        day = t["exit_ts"].date() if hasattr(t["exit_ts"], "date") else t["exit_ts"]
        # Use entry day if available (more correct -- bias should gate the ENTRY, not the exit)
        entry_ts = t.get("entry_ts")
        day = entry_ts.date() if entry_ts is not None and hasattr(entry_ts, "date") else day
        b = bias_by_day.get(day)
        if b is None:
            kept.append(t)
            continue
        if t["side"] == "CE" and b == "UP":
            kept.append(t)
        elif t["side"] == "PE" and b == "DOWN":
            kept.append(t)
        # else: filtered out -- disagrees with the day's bias
    return kept


if __name__ == "__main__":
    mrb.MONTH_DIR = LADDER_DIR
    for underlying, cfg in CONFIGS.items():
        print(f"\n{'#'*100}\n{underlying}\n{'#'*100}")
        spot_df = pd.read_parquet(cfg["spot_path"])
        spot_df["datetime"] = pd.to_datetime(spot_df["datetime"])
        bias_by_day = daily_bias_series(spot_df)
        n_up = sum(1 for v in bias_by_day.values() if v == "UP")
        n_down = sum(1 for v in bias_by_day.values() if v == "DOWN")
        n_none = sum(1 for v in bias_by_day.values() if v is None)
        print(f"Bias days: UP={n_up} DOWN={n_down} NONE={n_none}")
        for day in sorted(bias_by_day):
            if mrb.DAY_MIN <= day <= mrb.DAY_MAX:
                print(f"  {day}: {bias_by_day[day]}")

        trades = verify.run_month_live(underlying, cfg["fname_prefix"], cfg["spot_path"],
                                        cfg["offset"], cfg["round_step"], cfg["lot"], cfg["htf"])
        mrb.summarize(f"{underlying} WITHOUT bias filter (current live)", trades)

        trades_biased = apply_bias_filter(trades, bias_by_day)
        mrb.summarize(f"{underlying} WITH spot HTF bias filter", trades_biased)

        removed = len(trades) - len(trades_biased)
        removed_pnl = sum(t["pnl"] for t in trades) - sum(t["pnl"] for t in trades_biased)
        print(f"  -> bias filter removed {removed} trades worth {removed_pnl:+,.0f} combined")
