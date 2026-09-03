"""One-off: replay the ACTUAL live zone-detection mechanic (bb._detect_bear_zones,
_collapse_subzones, _arm_level, D1TrapBearOnlyBook._find_latest_closed_ref_bar)
against real 2026-08-03 1-min data for SENSEX 78500 PE -- the strike the user is
manually charting -- to report the exact entry price / SL the live code would
compute, instead of eyeballing the chart image."""
import sys
sys.path.insert(0, ".")
from datetime import timedelta, date
import pandas as pd
import strategies.d1_trap_option.bear_only_book as bb

SCRATCH = r"C:\Users\SERVER\AppData\Local\Temp\claude\e--AlgoSoft-OptionChainBasedStrategy\3f952902-2e64-455f-be1c-fac0a7378cbc\scratchpad"
HTF_MIN = 15  # SENSEX live default
DAY = date(2026, 8, 3)
EARLY_CUTOFF = bb._EARLY_SESSION_CUTOFF
SL_BUFFER = bb._SL_BUFFER_PTS
MAX_RISK_RS = bb._MAX_RISK_RS_PER_LOT
LOT_SIZE = 20

hist = pd.read_parquet(f"{SCRATCH}/sensex78500pe_hist.parquet").rename(columns={"ts": "datetime"})
today = pd.read_parquet(f"{SCRATCH}/sensex78500pe_today.parquet").rename(columns={"ts": "datetime"})
df1m = pd.concat([hist, today]).sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)

m_htf_hist = bb._resample(hist.sort_values("datetime"), HTF_MIN)
m15_hist = bb._resample(hist.sort_values("datetime"), 15)
zones = bb._prevalidate_zones(bb._detect_bear_zones(bb._to_bars(m_htf_hist)), m15_hist)
print(f"Warmup: {len(zones)} bear zones found (HTF={HTF_MIN}m)")
for z in zones:
    print(f"  zone_lo={z['zone_lo']:.2f} zone_hi={z['zone_hi']:.2f} lock_ts={z['lock_ts']}")

m5_full = bb._resample(df1m, 5)
m15_full = bb._resample(df1m, 15)

today_1m = df1m[df1m["datetime"].dt.date == DAY].reset_index(drop=True)
print(f"\nToday: {len(today_1m)} 1-min bars, {today_1m['datetime'].min()} .. {today_1m['datetime'].max()}")

active_locks = set()
for i in range(len(today_1m)):
    last_ts = today_1m.iloc[i]["datetime"]
    last_low = today_1m.iloc[i]["low"]
    last_high = today_1m.iloc[i]["high"]

    for zone in zones:
        if zone["done"] or zone["invalid"]:
            continue
        if zone["state"] == "WAITING":
            if last_low <= zone["zone_hi"]:
                zone["state"] = "MONITORING"
                zone["contact_ts"] = last_ts
                print(f"{last_ts}  CONTACT -> MONITORING  zone=[{zone['zone_lo']:.2f},{zone['zone_hi']:.2f}]")
            continue
        if zone["state"] != "MONITORING":
            continue
        if zone["ref_open"] is None:
            if last_ts.time() < EARLY_CUTOFF:
                continue
            ref = bb.D1TrapBearOnlyBook._find_latest_closed_ref_bar(m15_full, last_ts)
            if ref is not None:
                zone["ref_open"], zone["ref_close_time"] = ref.timestamp, ref.timestamp + timedelta(minutes=15)
                zone["ref_high"], zone["ref_low"] = ref.high, ref.low
                print(f"{last_ts}  REF CANDLE ASSIGNED {ref.timestamp}  H={ref.high:.2f} L={ref.low:.2f}")
            continue
        if zone["breach_ts"] is None:
            if last_ts >= zone["ref_close_time"] and last_high >= zone["ref_high"]:
                zone["breach_ts"] = last_ts
                sl_buffered = zone["ref_low"] - SL_BUFFER
                hard_sl = zone["ref_high"] - MAX_RISK_RS / LOT_SIZE
                sl_final = max(sl_buffered, hard_sl)
                print(f"{last_ts}  *** T1 BREACH/ENTRY *** entry={zone['ref_high']:.2f} "
                      f"sl={sl_final:.2f} (buffered={sl_buffered:.2f} hard_cap={hard_sl:.2f})")
                active_locks.add(zone["lock_ts"])
                continue
            new_ref = bb.D1TrapBearOnlyBook._find_latest_closed_ref_bar(m15_full, last_ts)
            if new_ref is not None and new_ref.timestamp > zone["ref_open"]:
                print(f"{last_ts}  ref rolled forward {zone['ref_open']} -> {new_ref.timestamp} "
                      f"(old H={zone['ref_high']:.2f} -> new H={new_ref.high:.2f}, no breach yet)")
                zone["ref_open"], zone["ref_close_time"] = new_ref.timestamp, new_ref.timestamp + timedelta(minutes=15)
                zone["ref_high"], zone["ref_low"] = new_ref.high, new_ref.low
            continue
        if zone["sub_lo"] is None:
            window_5m = m5_full[(m5_full["timestamp"] >= zone["ref_open"]) & (m5_full["timestamp"] < zone["ref_close_time"])]
            collapse = bb._collapse_subzones(bb._to_bars(window_5m))
            if collapse is None:
                zone["done"] = True
                print(f"{last_ts}  no 5m subzone -> T1 stands alone, zone done")
                continue
            zone["sub_lo"], zone["sub_hi"] = collapse
            threshold_pts = bb._ZONE_SIZE_THRESHOLD_PCT / 100.0 * zone["ref_high"]
            zone["arm_level"] = bb._arm_level(zone["sub_lo"], zone["sub_hi"], threshold_pts)
            zone["armed"] = False
            print(f"{last_ts}  5m subzone=[{zone['sub_lo']:.2f},{zone['sub_hi']:.2f}] arm_level={zone['arm_level']:.2f}")
            continue
        if not zone.get("armed"):
            if zone["sub_lo"] <= last_low <= zone["arm_level"]:
                zone["armed"] = True
                print(f"{last_ts}  ARMED (retrace into subzone)")
            continue
        if last_high >= zone["sub_hi"]:
            sl_final = max(zone["ref_low"] - SL_BUFFER, zone["ref_high"] - MAX_RISK_RS / LOT_SIZE)
            print(f"{last_ts}  *** T2 SWING-BREACH ENTRY *** entry={zone['sub_hi']:.2f} sl={sl_final:.2f}")
            zone["done"] = True

print("\nDONE replaying today's bars.")
