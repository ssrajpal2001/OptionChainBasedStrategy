"""Full stage-by-stage trace (with the 5-min window actually shown) for the
24000 CE zone that fired today, so every stage check is explicit -- not just
the final entry_ts."""
import sys, os
sys.path.insert(0, ".")
import scripts.d1trap_fractal_backtest as bt
import pandas as pd

OPT_DIR = os.path.join(bt.CACHE_DIR, "aug4_options")

df = pd.read_parquet(os.path.join(OPT_DIR, "24000_CE_with_today.parquet"))
df["datetime"] = pd.to_datetime(df["datetime"])
df = df.sort_values("datetime").reset_index(drop=True)

resamples = {}
m15 = bt.get_resample(15, df, resamples)
m5 = bt.get_resample(5, df, resamples)
m1 = df

contact_ts = pd.Timestamp("2026-07-30 09:15:00", tz=bt.IST)

ref = bt.find_ref_bar(contact_ts, m15, 15)
ref_open, ref_close_time = ref.timestamp, ref.timestamp + pd.Timedelta(minutes=15)
ref_high, ref_low = ref.high, ref.low
print(f"Stage 2 -- 15m REF CANDLE: {ref_open} -> {ref_close_time}  "
      f"O={ref.open:.2f} H={ref_high:.2f} L={ref_low:.2f} C={ref.close:.2f}")

m1_after = m1[(m1["datetime"] >= ref_close_time)]
breach = m1_after[m1_after["high"] >= ref_high]
breach_ts = breach.iloc[0]["datetime"] if not breach.empty else None
print(f"Stage 2 cont'd -- tick(1m)-wise BREACH of ref_high={ref_high:.2f}: "
      f"first 1m bar with high>=ref_high @ {breach_ts}")
breach_bar = m1[m1["datetime"] == breach_ts].iloc[0]
print(f"   that 1m bar: O={breach_bar.open:.2f} H={breach_bar.high:.2f} "
      f"L={breach_bar.low:.2f} C={breach_bar.close:.2f}")

print(f"\nStage 3 -- 5m SUB-ZONE decomposition of the ref candle's own span "
      f"[{ref_open} , {ref_close_time}):")
window_5m = m5[(m5["timestamp"] >= ref_open) & (m5["timestamp"] < ref_close_time)]
print(f"   5-min bars inside that 15-min window ({len(window_5m)} bars):")
for row in window_5m.itertuples(index=False):
    print(f"     {row.timestamp}  O={row.open:.2f} H={row.high:.2f} L={row.low:.2f} C={row.close:.2f}")

collapse = bt.collapse_subzones(bt.to_bars(window_5m), "LONG")
print(f"   collapse_subzones() result: {collapse}")
if collapse is None:
    print("   -> No valid ref/sweep/reclaim 3-candle pattern found in just "
          f"{len(window_5m)} five-minute bars (needs >= 3 distinct bars with a real "
          "sweep-then-reclaim structure). Stages 4/5 (arm, swing-break) are SKIPPED.")
    print(f"   -> raw_breakout FALLBACK fires immediately at breach_ts={breach_ts}, "
          f"entry_price=ref_high={ref_high:.2f}")
