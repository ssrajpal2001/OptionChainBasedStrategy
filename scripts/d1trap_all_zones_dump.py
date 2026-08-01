"""
2026-08-01: dump every zone the algo builds for CE/PE over the 14-day
warmup window, per the user's corrected boundary rule:
  Step 1: for each ref candle whose own high eventually gets reclaimed
          (find_all_bear_zones decides VALIDITY/reclaim timing), the
          zone's PRICE RANGE is just the two-candle footprint
          [ref.low, sellers_in_candle.low] -- NOT the multi-day running
          sweep_low. sweep_low answers "how far did this trade eventually
          go before being stopped out" (a lifetime fact); the zone's
          boundary is only the immediate ref/seller-in pair.
  Step 2: _collapse_nearby_zones (merge zones close in price), applied to
          these corrected [ref.low, sellers_in.low] boundaries.
No width-cap discard (_ZONE_MAX_RAW_WIDTH_PTS) is applied here -- that's
a separate filter set aside while we validate steps 1 and 2.
"""
import sys
sys.path.insert(0, ".")
import strategies.d1_trap_option.bear_only_book as bb
from strategies.v4_cascade.rolling_base import find_all_bear_zones
import pandas as pd
from datetime import timedelta

OPT_DIR = "data/d1trap_fractal_cache/aug4_options"
SPOT_PATH = "data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet"
ATM_ROUND_STEP = 100
ITM_OFFSET_PTS = 200
HIST_WARMUP_DAYS = bb._HIST_WARMUP_DAYS
DAY = pd.Timestamp("2026-07-31").date()


def load_bars(strike, side):
    df = pd.read_parquet(f"{OPT_DIR}/{strike}_{side}_month.parquet")
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)
    start = DAY - timedelta(days=HIST_WARMUP_DAYS)
    hist = df[(df["datetime"].dt.date >= start) & (df["datetime"].dt.date < DAY)]
    m60 = bb._resample(hist, 60)
    return bb._to_bars(m60)


def _collapse_neighbor_zones(zones, threshold_pts=bb._ZONE_MERGE_THRESHOLD_PTS, max_ref_gap=2):
    """2026-08-01: merge zones close in PRICE *and* close in TIME -- only
    ref candles within `max_ref_gap` 60m bars of each other are allowed to
    chain, so a week-long price-proximity bridge (07-22 -> 07-30) can no
    longer collapse unrelated days into one mega-zone. Same true-overlap
    vs proximity-cap distinction as bb._collapse_nearby_zones, plus the
    added ref_idx gate."""
    if not zones:
        return []
    ordered = sorted(zones, key=lambda z: (z["zone_lo"], z["zone_hi"]))
    groups = [[ordered[0]]]
    for z in ordered[1:]:
        grp = groups[-1]
        group_lo = min(g["zone_lo"] for g in grp)
        group_hi = max(g["zone_hi"] for g in grp)
        near_in_time = any(abs(z["ref_idx"] - g["ref_idx"]) <= max_ref_gap for g in grp)
        truly_overlaps = z["zone_lo"] <= group_hi
        if near_in_time and (truly_overlaps or z["zone_lo"] <= group_hi + threshold_pts):
            grp.append(z)
        else:
            groups.append([z])
    collapsed = []
    for group in groups:
        newest = max(group, key=lambda g: g["lock_ts"])
        collapsed.append(dict(zone_lo=min(g["zone_lo"] for g in group), zone_hi=max(g["zone_hi"] for g in group),
                               entry_line=newest["entry_line"], lock_ts=newest["lock_ts"],
                               ref_ts=newest["ref_ts"], ref_idx=newest["ref_idx"],
                               state="WAITING", ref_bar=None, done=False, invalid=False,
                               contact_ts=None, ref_open=None, ref_close_time=None,
                               breach_ts=None, sub_lo=None, sub_hi=None))
    return collapsed


def fmt(ts):
    return ts.strftime("%m-%d %H:%M") if ts is not None and pd.notna(ts) else "-"


def dump_side(strike, side):
    bars = load_bars(strike, side)
    print(f"\n{'='*100}\n{side}{strike} -- 60m bars in 14-day window: {len(bars)}  ({fmt(bars[0].timestamp)} .. {fmt(bars[-1].timestamp)})\n{'='*100}")

    # ---- STEP 1: raw zones (every ref candle, independently checked) ----
    raw = find_all_bear_zones(bars)
    print(f"\nSTEP 1 -- raw zones (ref candle's own SL got hit, i.e. reclaimed): {len(raw)}")
    print(f"{'ref_ts':<14}{'ref_O':>8}{'ref_H':>8}{'ref_L':>8}{'ref_C':>8}   {'sellers_in_ts':<14}{'sellers_low':>12}   {'reclaim_ts':<14}{'reclaim_hi':>11}   zone (ref.low, sellers_in.low)")
    raw_rows = []
    n = len(bars)
    idx_by_ts = {b.timestamp: i for i, b in enumerate(bars)}
    for z in sorted(raw, key=lambda z: z.reference_low_ts):
        ref_i = idx_by_ts[z.reference_low_ts]
        ref = bars[ref_i]
        # recover seller-in candle (first later candle whose low < ref.low)
        sellers_in = None
        for j in range(ref_i + 1, n):
            if bars[j].low < ref.low:
                sellers_in = bars[j]
                break
        # CORRECTED boundary: [ref.low, sellers_in.low] -- NOT sweep_low
        lo, hi = min(ref.low, sellers_in.low), max(ref.low, sellers_in.low)
        reclaim_bar = bars[idx_by_ts[z.lock_ts]]
        print(f"{fmt(ref.timestamp):<14}{ref.open:>8.2f}{ref.high:>8.2f}{ref.low:>8.2f}{ref.close:>8.2f}   "
              f"{fmt(sellers_in.timestamp) if sellers_in else '-':<14}{sellers_in.low if sellers_in else 0:>12.2f}   "
              f"{fmt(z.lock_ts):<14}{reclaim_bar.high:>11.2f}   "
              f"[{lo:.2f},{hi:.2f}]")
        raw_rows.append(dict(zone_lo=lo, zone_hi=hi, entry_line=ref.low, lock_ts=z.lock_ts,
                              ref_ts=ref.timestamp, ref_idx=ref_i,
                              state="WAITING", ref_bar=None, done=False, invalid=False,
                              contact_ts=None, ref_open=None, ref_close_time=None,
                              breach_ts=None, sub_lo=None, sub_hi=None))

    # ---- STEP 2: merge nearby raw zones -- price-close AND 2-candle-neighbor only ----
    merged = _collapse_neighbor_zones(raw_rows)
    print(f"\nSTEP 2 -- after merging zones within {bb._ZONE_MERGE_THRESHOLD_PTS}pts price AND within 2 ref-candles in time: {len(merged)}")
    for z in sorted(merged, key=lambda z: z["zone_lo"]):
        width = z["zone_hi"] - z["zone_lo"]
        print(f"  [{z['zone_lo']:.2f}, {z['zone_hi']:.2f}]  width={width:.2f}  lock_ts(newest member)={fmt(z['lock_ts'])}")
    return raw_rows, merged


if __name__ == "__main__":
    spot = pd.read_parquet(SPOT_PATH)
    spot["datetime"] = pd.to_datetime(spot["datetime"])
    day_open = spot[spot["datetime"].dt.date == DAY].iloc[0]["open"]
    atm = round(day_open / ATM_ROUND_STEP) * ATM_ROUND_STEP
    ce_strike, pe_strike = int(atm - ITM_OFFSET_PTS), int(atm + ITM_OFFSET_PTS)
    print(f"spot_open={day_open:.2f}  ATM={atm}  ->  CE={ce_strike}  PE={pe_strike}  (as of {DAY})")

    dump_side(ce_strike, "CE")
    dump_side(pe_strike, "PE")
