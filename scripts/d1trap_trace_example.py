"""Stage-by-stage trace for one specific trade: 23850 CE, as-of 2026-07-13
(entry=420.40 sl=412.25 exit=430.00 tsl_hit), showing every timestamp the
pipeline used: zone lock (trap confirmed on 60m) -> contact -> 15m ref
candle -> tick-wise breach -> 5m sub-zone -> arm -> swing-break entry."""
import sys, os
sys.path.insert(0, ".")
import scripts.d1trap_fractal_backtest as bt
import pandas as pd

OPT_DIR = os.path.join(bt.CACHE_DIR, "aug4_options")
STRIKE, SIDE = 23850, "CE"
AS_OF_DAY = pd.Timestamp("2026-07-13").date()

df = pd.read_parquet(os.path.join(OPT_DIR, f"{STRIKE}_{SIDE}.parquet"))
df["datetime"] = pd.to_datetime(df["datetime"])
df = df.sort_values("datetime").reset_index(drop=True)
df = df[df["datetime"].dt.date <= AS_OF_DAY]

resamples = {}
m_htf = bt.get_resample(60, df, resamples)
m_ref = bt.get_resample(15, df, resamples)
m_sub = bt.get_resample(5, df, resamples)

zones = bt.detect_d1_zones(bt.to_bars(m_htf))
bear_zones = [z for z in zones if z["direction"] == "LONG"]
print(f"{len(bear_zones)} bear-trap (LONG) zones found on 23850 CE's own 60m chart, as of {AS_OF_DAY}\n")

cfg = bt.Config(zone_size_threshold_pct=0.20, enable_continuation=False, enable_retest=False,
                 fallback_on_no_subzone="raw_breakout", htf_minutes=60, ref_minutes=15,
                 sub_minutes=5, entry_mode="swing_breach")

MAX_ZONE_AGE_DAYS = bt.MAX_ZONE_AGE_DAYS
found_trace = None

for zone in bear_zones:
    zone_lo, zone_hi, entry_line, lock_ts = zone["zone_lo"], zone["zone_hi"], zone["entry_line"], zone["lock_ts"]
    known_from = (lock_ts.normalize() + pd.Timedelta(days=1)).replace(hour=9, minute=15)
    if known_from.tzinfo is None:
        known_from = bt.IST.localize(known_from)
    deadline = known_from + pd.Timedelta(days=MAX_ZONE_AGE_DAYS)

    m1_window = df[(df["datetime"] >= known_from) & (df["datetime"] <= deadline)]
    touch = m1_window[m1_window["low"] <= zone_hi]
    if touch.empty:
        continue
    contact_ts = touch.iloc[0]["datetime"]

    ref = bt.find_ref_bar(contact_ts, m_ref, cfg.ref_minutes)
    if ref is None:
        continue
    ref_open, ref_close_time = ref.timestamp, ref.timestamp + pd.Timedelta(minutes=cfg.ref_minutes)
    ref_high, ref_low = ref.high, ref.low
    if ref_close_time > deadline:
        continue

    m1_after = df[(df["datetime"] >= ref_close_time) & (df["datetime"] <= deadline)]
    breach = m1_after[m1_after["high"] >= ref_high]
    if breach.empty:
        continue
    breach_ts = breach.iloc[0]["datetime"]

    print(f"  candidate zone lock={lock_ts}  ref_open={ref_open}  ref_high={ref_high:.2f}  "
          f"ref_low(SL)={ref_low:.2f}  breach_ts={breach_ts}")

    window_sub = m_sub[(m_sub["timestamp"] >= ref_open) & (m_sub["timestamp"] < ref_close_time)]
    collapse = bt.collapse_subzones(bt.to_bars(window_sub), "LONG")
    if collapse is None:
        # raw_breakout fallback path (matches cfg.fallback_on_no_subzone="raw_breakout")
        if abs(ref_high - 420.40) < 2.0 and abs(ref_low - 412.25) < 2.0:
            found_trace = dict(lock_ts=lock_ts, zone_lo=zone_lo, zone_hi=zone_hi, entry_line=entry_line,
                                known_from=known_from, contact_ts=contact_ts,
                                ref_open=ref_open, ref_close_time=ref_close_time,
                                ref_high=ref_high, ref_low=ref_low, breach_ts=breach_ts,
                                sub_lo=None, sub_hi=None, threshold_pts=None,
                                arm_level=None, armed_ts=None, entry_ts=breach_ts, fallback=True)
            break
        continue
    sub_lo, sub_hi = collapse

    threshold_pts = cfg.zone_size_threshold_pct / 100.0 * ref.close
    lvl = bt.arm_level(sub_lo, sub_hi, "LONG", threshold_pts)
    m1_arm = df[(df["datetime"] > breach_ts) & (df["datetime"] <= deadline)]
    armed = m1_arm[(m1_arm["low"] <= lvl) & (m1_arm["low"] >= sub_lo)]
    if armed.empty:
        continue
    armed_ts = armed.iloc[0]["datetime"]

    m1_trig = df[(df["datetime"] > armed_ts) & (df["datetime"] <= deadline)]
    trig = m1_trig[m1_trig["high"] >= sub_hi]
    if trig.empty:
        continue
    entry_ts = trig.iloc[0]["datetime"]

    print(f"  candidate zone lock={lock_ts}  ref_low(SL)={ref_low:.2f}  sub_hi(entry)={sub_hi:.2f}  "
          f"entry_ts={entry_ts}")

    if abs(sub_hi - 420.40) < 2.0 and abs(ref_low - 412.25) < 2.0:
        found_trace = dict(lock_ts=lock_ts, zone_lo=zone_lo, zone_hi=zone_hi, entry_line=entry_line,
                            known_from=known_from, contact_ts=contact_ts,
                            ref_open=ref_open, ref_close_time=ref_close_time,
                            ref_high=ref_high, ref_low=ref_low, breach_ts=breach_ts,
                            sub_lo=sub_lo, sub_hi=sub_hi, threshold_pts=threshold_pts,
                            arm_level=lvl, armed_ts=armed_ts, entry_ts=entry_ts)
        break

if found_trace is None:
    print("Could not isolate the exact zone (entry price didn't match within tolerance) -- "
          "printing all candidate zones' final numbers instead for inspection.")
else:
    t = found_trace
    print("=" * 90)
    print("STAGE-BY-STAGE TRACE: 23850 CE, entry=420.40 SL=412.25 exit=430.00 (tsl_hit)")
    print("=" * 90)
    print(f"Stage 0 (60m TRAP CONFIRMED / zone locked):")
    print(f"    lock_ts      = {t['lock_ts']}")
    print(f"    entry_line   = {t['entry_line']:.2f}   (the swept-then-reclaimed 60m ref candle level)")
    print(f"    zone band    = [{t['zone_lo']:.2f}, {t['zone_hi']:.2f}]")
    print(f"    known_from   = {t['known_from']}  (zone becomes tradeable the day after lock)")
    print()
    print(f"Stage 1 (zone CONTACT -- price ticks into the zone band):")
    print(f"    contact_ts   = {t['contact_ts']}")
    print()
    print(f"Stage 2 (15m REF CANDLE assigned, then tick-wise BREACH of its high):")
    print(f"    ref_open     = {t['ref_open']}")
    print(f"    ref_close    = {t['ref_close_time']}  (ref candle must fully close first)")
    print(f"    ref_high/low = {t['ref_high']:.2f} / {t['ref_low']:.2f}   <- SL = ref_low = {t['ref_low']:.2f}")
    print(f"    breach_ts    = {t['breach_ts']}  (first tick where price >= ref_high)")
    print()
    if t.get("fallback"):
        print(f"Stage 3-5: NO 5m sub-zone found inside the ref candle -> raw_breakout FALLBACK used.")
        print(f"    (Stages 3/4/5 -- sub-zone/arm/swing-break -- were SKIPPED for this trade)")
        print(f"    ENTRY PRICE  = ref_high = {t['ref_high']:.2f}  (fired immediately at breach_ts)")
        print(f"    entry_ts     = {t['entry_ts']}   SL = {t['ref_low']:.2f}")
    else:
        print(f"Stage 3 (5m SUB-ZONE decomposition of the ref candle's own span):")
        print(f"    sub-zone     = [{t['sub_lo']:.2f}, {t['sub_hi']:.2f}]   <- entry level = sub_hi = {t['sub_hi']:.2f}")
        print()
        print(f"Stage 4 (ARM -- retracement into the sub-zone, size-scaled):")
        print(f"    threshold    = {t['threshold_pts']:.2f} pts (0.20% of ref candle close)")
        print(f"    arm_level    = {t['arm_level']:.2f}")
        print(f"    armed_ts     = {t['armed_ts']}")
        print()
        print(f"Stage 5 (SWING BREACH -- break of the sub-zone's own high = actual ENTRY):")
        print(f"    entry_ts     = {t['entry_ts']}")
        print(f"    ENTRY PRICE  = {t['sub_hi']:.2f}   SL = {t['ref_low']:.2f}")
