"""
scripts/htf_followthrough_backtest.py
======================================
Compares two entry variants on the SAME frozen BANKNIFTY optimal params
(HTF=180m, MTF=15m, LTF=3m, Exec=3m, SLbuf=30, Cap=200, Gap=0.8%, DTE=0):

VARIANT A (current / strict):
  HTF TRAPPED → LTP must be inside HTF zone bounds [zone_low, zone_high]
  → MTF closed zone OVERLAPS HTF zone
  → LTF overlaps MTF
  → Entry. SL = HTF zone_low - SL_BUF

VARIANT B (HTF-trigger then follow-through):
  HTF TRAPPED → HTF zone is TRIGGERED (price reaches zone_trigger, i.e.
      exec bar touches or closes at/above zone_trigger = zone_low + SL_BUF)
  → AFTER trigger confirmed, MTF closed zone can be ANYWHERE above HTF zone_low
      (NOT restricted to inside HTF zone bounds)
  → LTF overlaps MTF
  → Entry. SL = MTF zone_low  (tighter — invalidation is the MTF setup)

Key difference: B allows the MTF/LTF trap to form ABOVE zone_high (squeeze
continuation), as long as the HTF zone was already triggered first. This
matches the live scanner behaviour where price crosses zone_trigger then
momentum carries it above zone_high before MTF sellers get trapped.

Backtest note: uses 1-min CLOSE as proxy for tick data (conservative — in
live market the trigger check fires on every WebSocket tick).

Usage:
    python scripts/htf_followthrough_backtest.py [BANKNIFTY|NIFTY]

Output: side-by-side table A vs B per day + totals, then trade-by-trade diff.
"""
from __future__ import annotations

import json
import sys
import time
from collections import defaultdict
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

# ── Reuse helpers from nse_cascade_backtest ──────────────────────────────────
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
from nse_cascade_backtest import (
    SYMBOL, LOT, STEP, UPSTOX_BASE,
    ATM_DELTA, _HEADERS, TOKEN,
    _eff_zone, _zones_overlap,
    _find_exec_entry, _run_cascade_day, _run_cascade_day_gap,
    _summarize, _load_bars, _resample_zones,
    sector_zones, zones_cache, exec_cache, day_mode,
    gap_pct_map, dte_map, all_days, sector_list,
)

# ── Frozen optimal params (BANKNIFTY, from 18k-combo sweep) ──────────────────
FX_HTF   = 180
FX_MTF   = 15
FX_LTF   = 3
FX_EXC   = 3
FX_SL    = 30.0
FX_CAP   = 200.0
FX_NSEC  = 1
FX_GAP   = 0.8
FX_DTE   = 0
T1_SRC   = "mtf"
TSL_TYPE = "bar_low"
TSL_BUF  = 10

# ─────────────────────────────────────────────────────────────────────────────

def _find_exec_entry_mtf_sl(exec_arr, ltf_zone, htf_zone, mtf_zone,
                             sl_buf, cap_pts, lot,
                             tsl_type="none", tsl_buf=10) -> Optional[dict]:
    """
    Variant B entry: SL = MTF zone_low (NOT HTF zone_low - buf).
    T1 = MTF sl (15m target). T2 = HTF sl (180m runner).
    Everything else identical to _find_exec_entry.
    """
    from nse_cascade_backtest import _simulate_numpy, _simulate_two_target
    ltf_l, ltf_h = _eff_zone(ltf_zone)
    mtf_l, _     = _eff_zone(mtf_zone)
    buf       = max((ltf_h - ltf_l) * 0.15, 1.0)
    htf_sl    = float(htf_zone.get("sl", 0))
    mtf_sl_t1 = float(mtf_zone.get("sl", 0))
    # SL is MTF zone_low (tighter than HTF zone_low - sl_buf)
    struct_sl = mtf_l

    if htf_sl > mtf_sl_t1 > 0:
        t1_price = mtf_sl_t1
        t2_price = htf_sl
    else:
        t1_price = htf_sl
        t2_price = 0.0

    if t1_price <= 0 or struct_sl <= 0:
        return None

    H, L, C = exec_arr["high"], exec_arr["low"], exec_arr["close"]
    n = len(H)
    if n < 2:
        return None
    in_zone = (C >= ltf_l - buf) & (C <= ltf_h + buf)
    idxs    = np.where(in_zone)[0]
    idxs    = idxs[idxs < n - 1]
    for i in idxs:
        trig = float(H[i])
        if t1_price <= trig or struct_sl >= trig:
            continue
        hit = np.where(H[i+1:] >= trig)[0]
        if not len(hit):
            continue
        j = hit[0]
        Hs = H[i+1:][j:]; Ls = L[i+1:][j:]; Cs = C[i+1:][j:]

        if t2_price > 0:
            res = _simulate_two_target(Hs, Ls, Cs, trig, struct_sl,
                                       t1_price, t2_price, tsl_type, tsl_buf, cap_pts, lot)
        else:
            res = _simulate_numpy(Hs, Ls, Cs, trig, struct_sl,
                                  t1_price, sl_buf, cap_pts, lot)
        res["entry_price"] = round(trig, 2)
        res["sl_level"]    = round(struct_sl, 2)
        res["t1"]          = round(t1_price, 2)
        res["t2"]          = round(t2_price, 2)
        res["sl_dist"]     = round(trig - struct_sl, 2)
        return res
    return None


def _htf_zone_triggered(htf_zone, exec_arr) -> bool:
    """
    Return True if the exec bars show price reaching htf zone_trigger
    (= zone_low + a small buffer, i.e. the 'Entry if OPT >' level).
    We use zone_low + 0.3 * (zone_high - zone_low) as the trigger proxy
    since the exact sl_buf isn't stored per zone in backtest.
    In live this is the moment HTF detector becomes ENTRY_READY.
    """
    z_low, z_high = _eff_zone(htf_zone)
    # trigger proxy = zone_low + 30% of zone range (matches live SL_BUF ≈ 30 pts on 150pt zone)
    trigger = z_low + 0.3 * (z_high - z_low)
    C = exec_arr["close"]
    # Any close AT or ABOVE the trigger (inside or above zone) = triggered
    return bool(np.any(C >= trigger))


def _run_variant_b_day(d_str, exec_arr, htf_zones, mtf_zones, ltf_zones,
                        sl_buf, cap_pts, sl_hist, lot,
                        sector_zones_day, n_sectors) -> Optional[dict]:
    """
    Variant B: HTF-trigger then follow-through.
    - HTF zone must be TRAPPED and TRIGGERED (price reaches zone_trigger in exec bars)
    - After trigger, MTF closed zone can be ANYWHERE above HTF zone_low
      (not restricted to overlap with HTF zone bounds)
    - LTF overlaps MTF
    - SL = MTF zone_low
    """
    from nse_cascade_backtest import _sector_confirms
    for htf_z in htf_zones:
        kind = htf_z.get("kind", "BEAR")
        if kind != "BEAR":
            continue
        hl, hh   = _eff_zone(htf_z)
        zone_key = f"B_{hl:.1f}-{hh:.1f}"
        t1       = float(htf_z.get("sl", 0))
        if t1 <= hh:
            continue
        if zone_key in sl_hist:
            if (date.fromisoformat(d_str) - date.fromisoformat(sl_hist[zone_key])).days <= 1:
                continue
        if not _sector_confirms(sector_zones_day, FX_HTF, kind, n_sectors):
            continue

        # Gate: HTF zone must have been TRIGGERED today
        if not _htf_zone_triggered(htf_z, exec_arr):
            continue

        # MTF: any BEAR zone anywhere ABOVE htf zone_low (not restricted to inside htf bounds)
        for mtf_z in mtf_zones:
            if mtf_z.get("kind") != kind:
                continue
            ml, mh = _eff_zone(mtf_z)
            # MTF zone must be above HTF zone_low (direction preserved, squeeze continuing)
            if ml < hl:
                continue
            mtf_t1 = float(mtf_z.get("sl", 0))
            if mtf_t1 <= mh:
                continue

            # LTF must overlap MTF (normal requirement)
            ltf_m = next((z for z in ltf_zones
                          if z.get("kind") == kind and _zones_overlap(mtf_z, z)), None)
            if not ltf_m:
                continue

            # Entry with SL = MTF zone_low (tighter)
            res = _find_exec_entry_mtf_sl(exec_arr, ltf_m, htf_z, mtf_z,
                                          sl_buf, cap_pts, lot,
                                          tsl_type=TSL_TYPE, tsl_buf=TSL_BUF)
            if res:
                res.update({"date": d_str, "zone_key": zone_key, "mode": "B_followthrough"})
                if res["exit_reason"] == "SL":
                    sl_hist[zone_key] = d_str
                return res
    return None


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*120}")
    print(f"  HTF FOLLOW-THROUGH COMPARISON  —  {SYMBOL}")
    print(f"  Frozen: HTF={FX_HTF}m MTF={FX_MTF}m LTF={FX_LTF}m Exec={FX_EXC}m "
          f"SLbuf={FX_SL:.0f} Cap={FX_CAP:.0f} Gap={FX_GAP}% DTE={FX_DTE} "
          f"T1={T1_SRC} TSL={TSL_TYPE}/{TSL_BUF}")
    print(f"{'='*120}\n")
    print(f"  VARIANT A (current):  HTF TRAPPED → LTP inside HTF bounds → MTF overlaps HTF → LTF → Entry. SL=HTF zone_low-buf")
    print(f"  VARIANT B (new):      HTF TRAPPED + TRIGGERED → MTF anywhere above HTF zone_low → LTF → Entry. SL=MTF zone_low")
    print(f"\n  Trading days: {len(all_days)}  ({all_days[0]} → {all_days[-1]})\n")

    trades_a: List[dict] = []
    trades_b: List[dict] = []
    sl_hist_a: Dict[str, str] = {}
    sl_hist_b: Dict[str, str] = {}

    for d_str in all_days:
        sec_day: Dict[tuple, list] = {}
        for sec in sector_list:
            sec_day[(sec, FX_HTF)] = sector_zones.get((sec, FX_HTF, d_str), [])
            sec_day[(sec, FX_MTF)] = sector_zones.get((sec, FX_MTF, d_str), [])

        htf_z  = zones_cache.get((FX_HTF, d_str), [])
        mtf_z  = zones_cache.get((FX_MTF, d_str), [])
        ltf_z  = zones_cache.get((FX_LTF, d_str), [])
        ex_arr = exec_cache.get((FX_EXC, d_str))
        if ex_arr is None:
            continue

        is_opt_day = day_mode.get(d_str) == "option"
        sim_lot    = float(LOT) if is_opt_day else ATM_DELTA * LOT
        sim_cap    = FX_CAP
        day_gap    = gap_pct_map.get(d_str, 0.0)
        day_dte    = dte_map.get(d_str, 99)

        if FX_DTE > 0 and day_dte > FX_DTE:
            continue

        use_gap = FX_GAP < 99.0 and day_gap > FX_GAP

        # ── Variant A ──
        if use_gap:
            res_a = _run_cascade_day_gap(d_str, ex_arr, mtf_z, ltf_z,
                                         FX_SL, sim_cap, sl_hist_a, sim_lot,
                                         sec_day, FX_MTF, FX_NSEC,
                                         t1_src=T1_SRC, tsl_type=TSL_TYPE, tsl_buf=TSL_BUF)
        else:
            res_a = _run_cascade_day(d_str, ex_arr, htf_z, mtf_z, ltf_z,
                                     FX_SL, sim_cap, sl_hist_a, sim_lot,
                                     sec_day, FX_HTF, FX_NSEC,
                                     t1_src=T1_SRC, tsl_type=TSL_TYPE, tsl_buf=TSL_BUF)
        if res_a:
            trades_a.append(res_a)

        # ── Variant B ──
        if use_gap:
            # Gap day: same as A (HTF unreachable, already 3-tier)
            res_b = _run_cascade_day_gap(d_str, ex_arr, mtf_z, ltf_z,
                                         FX_SL, sim_cap, sl_hist_b, sim_lot,
                                         sec_day, FX_MTF, FX_NSEC,
                                         t1_src=T1_SRC, tsl_type=TSL_TYPE, tsl_buf=TSL_BUF)
        else:
            res_b = _run_variant_b_day(d_str, ex_arr, htf_z, mtf_z, ltf_z,
                                       FX_SL, sim_cap, sl_hist_b, sim_lot,
                                       sec_day, FX_NSEC)
        if res_b:
            trades_b.append(res_b)

    # ── Summary ───────────────────────────────────────────────────────────────
    params = {"htf_min": FX_HTF, "mtf_min": FX_MTF, "ltf_min": FX_LTF,
              "exec_min": FX_EXC, "sl_buf": FX_SL, "cap_pts": FX_CAP,
              "sector_confirm": FX_NSEC, "gap_thr_pct": FX_GAP,
              "dte_filter": FX_DTE, "t1_src": T1_SRC,
              "tsl_type": TSL_TYPE, "tsl_buf": TSL_BUF, "symbol": SYMBOL}

    sa = _summarize(trades_a, params)
    sb = _summarize(trades_b, params)

    W = 55
    print(f"\n{'─'*120}")
    print(f"  {'METRIC':<30} {'VARIANT A (current strict)':>{W}} {'VARIANT B (HTF-trigger follow)':>{W}}")
    print(f"{'─'*120}")
    for k in ["total","wins","losses","win_rate_pct","profit_factor",
              "net_pnl_inr","avg_win_inr","avg_loss_inr",
              "exits_sl","exits_t1","exits_t2","exits_trail","exits_cap","exits_eod",
              "t1_hit_pct","t2_hit_pct"]:
        va = sa.get(k, "—")
        vb = sb.get(k, "—")
        print(f"  {k:<30} {str(va):>{W}} {str(vb):>{W}}")
    print(f"{'─'*120}")

    # ── Day-by-day diff ───────────────────────────────────────────────────────
    a_by_date = {t["date"]: t for t in trades_a}
    b_by_date = {t["date"]: t for t in trades_b}
    all_trade_dates = sorted(set(list(a_by_date) + list(b_by_date)))

    print(f"\n  {'DATE':<12} {'A PNL':>10} {'A EXIT':>10} {'B PNL':>10} {'B EXIT':>10}  DIFF  NOTE")
    print(f"  {'─'*80}")
    only_in_a = only_in_b = both_better_b = both_worse_b = 0
    for d in all_trade_dates:
        ta = a_by_date.get(d)
        tb = b_by_date.get(d)
        pa = ta["pnl"] if ta else None
        pb = tb["pnl"] if tb else None
        ea = ta.get("exit_reason","—") if ta else "—"
        eb = tb.get("exit_reason","—") if tb else "—"
        diff = round(pb - pa, 2) if pa is not None and pb is not None else "—"
        note = ""
        if ta and not tb:   note = "A only (B missed)";  only_in_a += 1
        elif tb and not ta: note = "B only (extra trade)"; only_in_b += 1
        elif diff != "—":
            if diff > 0:    note = "B BETTER"; both_better_b += 1
            elif diff < 0:  note = "B WORSE";  both_worse_b  += 1
            else:           note = "same"
        print(f"  {d:<12} {str(round(pa,1) if pa is not None else '—'):>10} "
              f"{ea:>10} {str(round(pb,1) if pb is not None else '—'):>10} "
              f"{eb:>10}  {str(diff):>7}  {note}")

    print(f"\n  DIFF SUMMARY:")
    print(f"    A only (trades B missed):    {only_in_a}")
    print(f"    B only (extra trades B got): {only_in_b}")
    print(f"    Both traded, B better:       {both_better_b}")
    print(f"    Both traded, B worse:        {both_worse_b}")
    print(f"\n  NET B advantage: {round(sb['net_pnl_inr'] - sa['net_pnl_inr'], 2)} INR  "
          f"  WR diff: {round(sb['win_rate_pct'] - sa['win_rate_pct'], 1)}%  "
          f"  PF diff: {round(sb['profit_factor'] - sa['profit_factor'], 3)}")
    print(f"\n  CONCLUSION:")
    if sb["profit_factor"] > sa["profit_factor"] and sb["win_rate_pct"] >= sa["win_rate_pct"] - 5:
        print(f"    ✓ Variant B is BETTER — higher PF with acceptable WR. Consider merging.")
    elif sb["win_rate_pct"] < sa["win_rate_pct"] - 10:
        print(f"    ✗ Variant B has LOWER win rate (>{sa['win_rate_pct']-sb['win_rate_pct']:.0f}% drop). Extra trades reduce quality.")
    else:
        print(f"    ~ Results mixed. Review day-by-day diff above before deciding.")


if __name__ == "__main__":
    main()
