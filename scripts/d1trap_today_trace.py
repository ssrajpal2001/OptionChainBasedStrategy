"""Trace today (2026-07-30) exactly as the live D1TrapBearOnlyBook would have seen it:
CE=24000, PE=24400 (ATM=24200 rounded to 100 from spot_open=24249.55), bear-trap-only
60m zones warmed from history, then walk forward through today's bars stage by stage."""
import sys, os
sys.path.insert(0, ".")
import scripts.d1trap_fractal_backtest as bt
import pandas as pd

OPT_DIR = os.path.join(bt.CACHE_DIR, "aug4_options")
TODAY = pd.Timestamp("2026-07-30").date()

def load(name):
    df = pd.read_parquet(os.path.join(OPT_DIR, f"{name}_with_today.parquet"))
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.sort_values("datetime").reset_index(drop=True)

for side, strike, fname in [("CE", 24000, "24000_CE"), ("PE", 24400, "24400_PE")]:
    print(f"\n{'='*90}\n{strike}{side}\n{'='*90}")
    df = load(fname)
    df_before_today = df[df["datetime"].dt.date < TODAY]
    df_full = df

    resamples = {}
    m60 = bt.get_resample(60, df_before_today, resamples)
    zones = bt.detect_d1_zones(bt.to_bars(m60))
    bear_zones = [z for z in zones if z["direction"] == "LONG"]
    print(f"Bear-trap zones from history (before today): {len(bear_zones)}")
    for z in bear_zones[-6:]:
        print(f"  zone_lo={z['zone_lo']:.2f} zone_hi={z['zone_hi']:.2f} "
              f"entry_line={z['entry_line']:.2f} lock_ts={z['lock_ts']}")

    m15_full = bt.get_resample(15, df_full, {})
    m5_full = bt.get_resample(5, df_full, {})

    today_bars = df_full[df_full["datetime"].dt.date == TODAY]
    print(f"\nToday's bars: {len(today_bars)}  spot-open-equivalent(open)={today_bars.iloc[0]['open']:.2f}  "
          f"day range: L={today_bars['low'].min():.2f} H={today_bars['high'].max():.2f} "
          f"close={today_bars.iloc[-1]['close']:.2f}")

    cfg = bt.Config(zone_size_threshold_pct=0.20, enable_continuation=False, enable_retest=False,
                     fallback_on_no_subzone="raw_breakout", htf_minutes=60, ref_minutes=15,
                     sub_minutes=5, entry_mode="swing_breach")

    any_event = False
    for zone in bear_zones:
        zone_lo, zone_hi, entry_line = zone["zone_lo"], zone["zone_hi"], zone["entry_line"]
        touch = today_bars[today_bars["low"] <= zone_hi]
        if touch.empty:
            continue
        contact_ts = touch.iloc[0]["datetime"]
        any_event = True
        print(f"\n-- Zone [{zone_lo:.2f},{zone_hi:.2f}] entry_line={entry_line:.2f} "
              f"(locked {zone['lock_ts']}) --")
        print(f"   CONTACT @ {contact_ts}")

        deadline = contact_ts + pd.Timedelta(days=20)
        result = bt.run_fractal_from(contact_ts, "LONG", df_full, m15_full, m5_full, cfg, deadline)
        if result:
            print(f"   -> WOULD ENTER @ {result['entry_ts']}  price={result['entry_price']:.2f} "
                  f"sl={result['sl']:.2f}  subzone={result.get('subzone')}")
        else:
            print(f"   -> contact registered, but no completed entry today (never reached "
                  f"ref-breach+subzone/fallback, or armed/swing stage never completed).")

    if not any_event:
        print("\nNo historical zone was ever touched by today's price action.")
