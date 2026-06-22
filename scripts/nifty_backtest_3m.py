"""
scripts/nifty_backtest_3m.py — NIFTY 3m-confirmation trap backtest.

Entry logic (per leg):
  1. GAP bias:  UP → CE side only; DOWN → PE side only; no-gap → both.
  2. HTF 75m zone must be TRAPPED or CLOSED (confirmed bearish trap).
  3. Option price in lower 1/3 of HTF zone (ltp ≤ zone_trigger).
  4. 3m candle: curr HIGH > prev HIGH while prev close ≤ zone_trigger → ENTRY.
  5. R:R gate: reward / risk ≥ rr_min (uses opposite-side cross-price method).
  6. No entry after 14:45 IST.

Exit logic:
  SL  : CE bar low ≤ HTF zone_low → exit at zone_low (full position).
  T1  : OPP side bar close ≤ OPP zone_trigger → exit CE at current bar close
        (IMMEDIATE — no 1m confirmation, tick-driven check on every 1m bar).
  EOD : ≥ 15:20 IST → exit at bar open.

Rotation:
  After CE closes via T1 (OPP_ZONE_TRIGGER) → check PE entry conditions.
  PE entry requires the same 3m confirmation on PE bars.
  One live trade at a time.

Usage (CLI):
  python scripts/nifty_backtest_3m.py --token TOKEN
  python scripts/nifty_backtest_3m.py --token TOKEN --days 20 --lots 2 --rr-min 1.5
  python scripts/nifty_backtest_3m.py --token TOKEN --days 30 --no-bias
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta
from typing import Optional

import pandas as pd

sys.path.insert(0, ".")

# Reuse all data/fetch/expiry utilities from existing backtest
from scripts.nifty_backtest import (
    _fetch_1m,
    _mkt_hours,
    _option_key,
    _resample,
    _USE_MONTHLY,
    _USE_NEXT_WEEK,
    INDEX_CFG,
    _HEADERS,
)
import scripts.nifty_backtest as _nb
from strategies.trap_scanner.scanner import scan_htf
from data_layer.instrument_registry import REGISTRY

EOD_HM   = (15, 20)
CUTOFF_HM = (14, 45)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _zone_trigger_val(e: dict) -> float:
    """Lower 1/3 of zone band (bear zone: low + 1/3 range)."""
    if "zone_trigger" in e:
        return float(e["zone_trigger"])
    zh, zl = float(e["zone_high"]), float(e["zone_low"])
    return round(zl + (zh - zl) / 3, 2)


def _ts_hm(ts) -> tuple[int, int]:
    t = pd.Timestamp(ts)
    return t.hour, t.minute


def _before_cutoff(ts) -> bool:
    h, m = _ts_hm(ts)
    return (h, m) < CUTOFF_HM


def _is_eod(ts) -> bool:
    h, m = _ts_hm(ts)
    return (h, m) >= EOD_HM


# ── HTF zone scan ──────────────────────────────────────────────────────────────

def _htf_zones_for_today(df_all: pd.DataFrame, htf_min: int, today: date) -> list:
    """
    Run HTF scan on multi-day bars; return zones confirmed (TRAPPED or CLOSED)
    on or before today.
    """
    if df_all.empty:
        return []
    df_htf = _resample(df_all, htf_min)
    _, entries = scan_htf(df_htf)
    result = []
    for e in entries:
        if e.get("status") not in ("TRAPPED", "CLOSED"):
            continue
        confirm_ts = e.get("closed_on") or e.get("trapped_on")
        if confirm_ts is not None:
            try:
                c_date = pd.Timestamp(str(confirm_ts)).date()
                if c_date > today:
                    continue
            except Exception:
                pass
        e["zone_trigger"] = _zone_trigger_val(e)
        result.append(e)
    return result


# ── 3m entry detection ─────────────────────────────────────────────────────────

def _find_3m_entries(df_1m_today: pd.DataFrame, htf_zones: list,
                     sl_buf: float = 3.0, min_risk: float = 10.0) -> list:
    """
    Return list of entry dicts for TRAPPED/CLOSED zones.
    Entry condition:
      - Prev 3m bar: close ≤ zone_trigger (in lower 1/3)
      - Curr 3m bar: high > prev high → entry at prev high
    One entry per zone (first signal).
    Skip entries at or after CUTOFF.
    """
    if df_1m_today.empty or not htf_zones:
        return []

    df_3m = _resample(df_1m_today, 3)
    entries = []

    for zone in htf_zones:
        if zone.get("status") not in ("TRAPPED", "CLOSED"):
            continue

        zone_trigger = float(zone["zone_trigger"])
        zone_low     = float(zone["zone_low"])
        zone_high    = float(zone["zone_high"])

        # Only scan bars AFTER zone was confirmed
        valid_from = zone.get("closed_on") or zone.get("trapped_on")
        if valid_from is not None:
            valid_ts = pd.Timestamp(str(valid_from))
            sub = df_3m[df_3m["datetime"] > valid_ts].copy().reset_index(drop=True)
        else:
            sub = df_3m.copy().reset_index(drop=True)

        if len(sub) < 2:
            continue

        for i in range(1, len(sub)):
            prev = sub.iloc[i - 1]
            curr = sub.iloc[i]

            bar_ts = curr["datetime"]
            if _is_eod(bar_ts) or not _before_cutoff(bar_ts):
                continue

            # Prev candle close must be at or below zone_trigger (in lower 1/3)
            # Price can dip below zone_low (liquidity sweep) — allowed up to sl_buf below
            prev_close = float(prev["close"])
            if prev_close > zone_trigger:
                continue
            if prev_close < zone_low - sl_buf:   # runaway breakdown, skip
                continue

            # Confirmation: curr 3m high breaks prev 3m high
            if float(curr["high"]) > float(prev["high"]):
                entry_price = round(float(prev["high"]), 2)

                # SL rule:
                # Normal (close ≥ zone_low): SL = zone_low − sl_buf
                # Liquidity sweep (close < zone_low): SL = 3m candle low − sl_buf
                prev_low = float(prev["low"])
                if prev_close >= zone_low:
                    sl      = round(zone_low - sl_buf, 2)
                    sl_type = "ZONE_LOW"
                else:
                    sl      = round(prev_low - sl_buf, 2)
                    sl_type = "DIP_LOW"

                # Skip if SL is above entry (degenerate candle)
                if sl >= entry_price:
                    continue
                # Skip if risk < min_risk pts (spread noise would eat it)
                if (entry_price - sl) < min_risk:
                    continue

                entries.append({
                    "entry_price":  entry_price,
                    "entry_ts":     bar_ts,
                    "zone_low":     zone_low,
                    "zone_high":    zone_high,
                    "zone_trigger": zone_trigger,
                    "sl_price":     sl,
                    "sl_type":      sl_type,
                    "htf_zone":     zone,
                })
                break  # first signal per zone

    return entries


# ── R:R calculation ────────────────────────────────────────────────────────────

def _calc_rr(entry_price: float, sl_price: float,
             df_exec_today: pd.DataFrame,
             df_opp_today: pd.DataFrame,
             opp_zone_trigger: float) -> Optional[float]:
    """
    CE target = CE price at the moment PE was closest to PE zone_trigger.
    R:R = (target - entry) / (entry - sl)
    Uses cross-leg price correlation (same approach as nifty_backtest.py).
    """
    risk = entry_price - sl_price
    if risk <= 0:
        return None
    if df_opp_today.empty:
        return None

    # Find timestamp when PE was closest to its zone_trigger
    dists   = (df_opp_today["close"] - opp_zone_trigger).abs()
    hist_ts = df_opp_today.loc[dists.idxmin(), "datetime"]

    # CE price at that timestamp
    mask = df_exec_today["datetime"] <= hist_ts
    if not mask.any():
        return None

    ce_target = float(df_exec_today[mask].iloc[-1]["close"])
    reward    = ce_target - entry_price
    if reward <= 0:
        return None

    return round(reward / risk, 2)


# ── Trade simulation ───────────────────────────────────────────────────────────

def _simulate_trade(entry: dict,
                    df_exec_1m: pd.DataFrame,
                    df_opp_1m: pd.DataFrame,
                    opp_zone_trigger: Optional[float],
                    lot: int,
                    lot_size: int,
                    trail_pts: float = 5.0) -> Optional[dict]:
    """
    Two-stage exit simulation:

    Stage 1 — until T1 (OPP_ZONE_TRIGGER):
      SL   : bar low ≤ sl_price → full exit at sl_price
      T1   : OPP close ≤ opp_zone_trigger → book 50% at CE bar close
      EOD  : ≥ 15:20 → full exit at bar open

    Stage 2 — trailing remaining 50% after T1:
      TSL  : trail_sl starts at entry_price (breakeven), ratchets up when
             bar close > (trail_sl + trail_pts).  trail_sl = bar_close - trail_pts.
      SL   : bar low ≤ trail_sl → exit remaining at trail_sl
      EOD  : ≥ 15:20 → exit remaining at bar open
    """
    entry_ts    = pd.Timestamp(entry["entry_ts"])
    entry_price = entry["entry_price"]
    sl_price    = entry["sl_price"]
    total_qty   = lot * lot_size
    half_qty    = total_qty // 2
    rem_qty     = total_qty - half_qty   # handles odd lots

    df_after = df_exec_1m[df_exec_1m["datetime"] > entry_ts].copy().reset_index(drop=True)
    df_opp_after = (
        df_opp_1m[df_opp_1m["datetime"] > entry_ts].copy().reset_index(drop=True)
        if df_opp_1m is not None and not df_opp_1m.empty
        else pd.DataFrame()
    )

    if df_after.empty:
        return None

    # Safety: SL must be below entry for a long position
    if sl_price >= entry_price:
        return None

    # ── Stage 1: full position, watch for SL / T1 / EOD ──────────────────
    t1_hit      = False
    t1_price    = None
    t1_ts       = None
    t1_pnl_rs   = 0.0
    sl_hit      = False
    sl_exit_px  = None
    sl_exit_ts  = None
    eod_exit_px = None
    eod_exit_ts = None

    for i in range(len(df_after)):
        bar    = df_after.iloc[i]
        bar_ts = pd.Timestamp(bar["datetime"])

        if _is_eod(bar_ts):
            eod_exit_px = float(bar["open"])
            eod_exit_ts = bar_ts
            break

        if float(bar["low"]) <= sl_price:
            sl_hit     = True
            sl_exit_px = sl_price
            sl_exit_ts = bar_ts
            break

        if opp_zone_trigger is not None and not df_opp_after.empty:
            opp_bars = df_opp_after[df_opp_after["datetime"] <= bar["datetime"]]
            if not opp_bars.empty and float(opp_bars.iloc[-1]["close"]) <= opp_zone_trigger:
                t1_hit    = True
                t1_price  = round(float(bar["close"]), 2)
                t1_ts     = bar_ts
                t1_pnl_rs = round((t1_price - entry_price) * half_qty, 2)
                stage2_start_idx = i + 1
                break

    # Full exit (SL or EOD without T1) — single-stage result
    if sl_hit:
        pnl_rs = round((sl_exit_px - entry_price) * total_qty, 2)
        return _trade_result(entry, entry_ts, sl_exit_ts, sl_exit_px, sl_price,
                             total_qty, pnl_rs, "SL",
                             t1_hit=False, t1_price=None, t1_ts=None, t1_pnl_rs=0)

    if not t1_hit:
        px = eod_exit_px or float(df_after.iloc[-1]["close"])
        ts = eod_exit_ts or pd.Timestamp(df_after.iloc[-1]["datetime"])
        pnl_rs = round((px - entry_price) * total_qty, 2)
        return _trade_result(entry, entry_ts, ts, px, sl_price,
                             total_qty, pnl_rs, "EOD",
                             t1_hit=False, t1_price=None, t1_ts=None, t1_pnl_rs=0)

    # ── Stage 2: trail remaining 50% after T1 ────────────────────────────
    trail_sl      = entry_price          # start TSL at breakeven
    tsl_exit_px   = None
    tsl_exit_ts   = None
    tsl_reason    = "EOD"

    df_stage2 = df_after.iloc[stage2_start_idx:].reset_index(drop=True)

    for i in range(len(df_stage2)):
        bar    = df_stage2.iloc[i]
        bar_ts = pd.Timestamp(bar["datetime"])
        bar_close = float(bar["close"])
        bar_low   = float(bar["low"])

        if _is_eod(bar_ts):
            tsl_exit_px = float(bar["open"])
            tsl_exit_ts = bar_ts
            tsl_reason  = "EOD"
            break

        # Ratchet TSL up when price makes new highs
        if bar_close > trail_sl + trail_pts:
            trail_sl = round(bar_close - trail_pts, 2)

        # TSL hit
        if bar_low <= trail_sl:
            tsl_exit_px = trail_sl
            tsl_exit_ts = bar_ts
            tsl_reason  = "TSL"
            break

    if tsl_exit_px is None:
        last = df_stage2.iloc[-1] if not df_stage2.empty else df_after.iloc[-1]
        tsl_exit_px = float(last["close"])
        tsl_exit_ts = pd.Timestamp(last["datetime"])
        tsl_reason  = "EOD"

    trail_pnl_rs = round((tsl_exit_px - entry_price) * rem_qty, 2)
    total_pnl_rs = round(t1_pnl_rs + trail_pnl_rs, 2)

    return _trade_result(entry, entry_ts, tsl_exit_ts, tsl_exit_px, sl_price,
                         total_qty, total_pnl_rs, tsl_reason,
                         t1_hit=True, t1_price=t1_price, t1_ts=t1_ts,
                         t1_pnl_rs=t1_pnl_rs,
                         trail_exit_px=tsl_exit_px, trail_exit_ts=tsl_exit_ts,
                         trail_pnl_rs=trail_pnl_rs, trail_sl=trail_sl)


def _trade_result(entry, entry_ts, exit_ts, exit_price, sl_price,
                  total_qty, pnl_rs, reason,
                  t1_hit=False, t1_price=None, t1_ts=None, t1_pnl_rs=0,
                  trail_exit_px=None, trail_exit_ts=None,
                  trail_pnl_rs=0, trail_sl=None) -> dict:
    return {
        "date":           str(entry_ts.date()),
        "opt_type":       entry.get("opt_type", "?"),
        "sl_type":        entry.get("sl_type", "ZONE_LOW"),
        "entry_ts":       str(entry_ts),
        "exit_ts":        str(exit_ts),
        "entry_price":    round(entry["entry_price"], 2),
        "exit_price":     round(exit_price, 2),
        "sl_price":       round(sl_price, 2),
        "zone_low":       round(entry["zone_low"], 2),
        "zone_high":      round(entry["zone_high"], 2),
        "zone_trigger":   round(entry["zone_trigger"], 2),
        "qty":            total_qty,
        "pnl_rs":         pnl_rs,
        "reason":         reason,
        "win":            pnl_rs > 0,
        # T1 detail
        "t1_hit":         t1_hit,
        "t1_price":       t1_price,
        "t1_ts":          str(t1_ts) if t1_ts else None,
        "t1_pnl_rs":      t1_pnl_rs,
        # Trail detail
        "trail_exit_px":  trail_exit_px,
        "trail_exit_ts":  str(trail_exit_ts) if trail_exit_ts else None,
        "trail_pnl_rs":   trail_pnl_rs,
        "trail_sl":       trail_sl,
    }


# ── Per-day runner ─────────────────────────────────────────────────────────────

def _run_day(index: str, cfg: dict, td: date,
             df_spot_all: pd.DataFrame,
             use_bias: bool,
             lots: int,
             htf_min: int,
             rr_min: float,
             sl_buf: float,
             min_risk: float,
             cache: dict,
             fetch_from: str) -> list:
    """Process one trading day; return list of trade result dicts."""
    lot_size  = cfg["lot"]
    today_str = td.isoformat()

    # ── Gap detection ──────────────────────────────────────────────────────
    gap_fired = False
    gap_dir   = None
    gap_thresh = cfg.get("gap_thresh", 0.5)  # percent

    df_spot_today = df_spot_all[df_spot_all["datetime"].dt.date == td]
    if use_bias and not df_spot_today.empty:
        prev_days = df_spot_all[df_spot_all["datetime"].dt.date < td]
        if not prev_days.empty:
            prev_close = float(prev_days.iloc[-1]["close"])
            today_open = float(df_spot_today.iloc[0]["open"])
            gap_pct    = abs((today_open - prev_close) / prev_close) * 100
            if gap_pct >= gap_thresh:
                gap_fired = True
                gap_dir   = "UP" if today_open > prev_close else "DOWN"

    # ── ATM + ITM strikes (matches live engine: CE=ATM-gap_near, PE=ATM+gap_near) ──
    step     = cfg["step"]
    gap_near = cfg.get("gap_near", 200)   # ITM offset in points (NIFTY=200)
    atm      = 0
    ce_strike = 0
    pe_strike = 0
    if not df_spot_today.empty:
        atm       = int(round(float(df_spot_today.iloc[0]["open"]) / step) * step)
        ce_strike = atm - gap_near   # ITM call (below spot)
        pe_strike = atm + gap_near   # ITM put  (above spot)

    # ── Determine scan sides ───────────────────────────────────────────────
    all_sides = ["CE", "PE"]
    if use_bias and gap_fired:
        all_sides = ["CE"] if gap_dir == "UP" else ["PE"]

    # ── Strike map per side ───────────────────────────────────────────────
    strike_map = {"CE": ce_strike, "PE": pe_strike}

    # ── Fetch option bars (always both CE+PE for R:R cross-check) ─────────
    side_data: dict[str, dict] = {}

    for opt_type in ["CE", "PE"]:
        strike = strike_map[opt_type]
        if strike <= 0:
            continue
        key = _option_key(index, strike, opt_type, td)
        if not key:
            continue
        if key in cache:
            df_raw = cache[key]
        else:
            try:
                df_raw = _fetch_1m(key, fetch_from, today_str)
                time.sleep(0.25)
                cache[key] = df_raw
            except Exception as exc:
                print(f"  {today_str} {opt_type} fetch error: {exc}")
                continue

        if df_raw.empty or "datetime" not in df_raw.columns:
            print(f"  {today_str} {opt_type}: no bars for key={key}")
            continue

        df_all   = _mkt_hours(df_raw)
        df_today = df_all[df_all["datetime"].dt.date == td].copy().reset_index(drop=True)
        if df_today.empty:
            continue

        side_data[opt_type] = {
            "df_all":   df_all,
            "df_today": df_today,
        }

    if not side_data:
        return []

    trades = []

    for opt_type in all_sides:
        if opt_type not in side_data:
            continue

        opp_type = "PE" if opt_type == "CE" else "CE"
        df_all   = side_data[opt_type]["df_all"]
        df_today = side_data[opt_type]["df_today"]
        df_opp   = side_data.get(opp_type, {}).get("df_today", pd.DataFrame())

        # ── HTF zones for today ────────────────────────────────────────────
        zones = _htf_zones_for_today(df_all, htf_min, td)
        if not zones:
            print(f"  {opt_type}: no TRAPPED/CLOSED HTF zones for {td}")
            continue

        # ── OPP zone trigger (for R:R and exit) ───────────────────────────
        opp_zones = []
        if opp_type in side_data:
            opp_all   = side_data[opp_type]["df_all"]
            opp_zones = _htf_zones_for_today(opp_all, htf_min, td)

        opp_zone_trigger: Optional[float] = None
        if opp_zones:
            # Use the lowest zone_trigger among confirmed OPP zones
            opp_zone_trigger = min(float(z["zone_trigger"]) for z in opp_zones)

        # ── 3m entry signals ───────────────────────────────────────────────
        signals = _find_3m_entries(df_today, zones, sl_buf=sl_buf, min_risk=min_risk)
        if not signals:
            print(f"  {opt_type}: {len(zones)} zone(s) found but no 3m entry signal (price never in lower 1/3 or risk < {min_risk}pts)")
            continue

        last_exit_ts: Optional[pd.Timestamp] = None

        for sig in signals:
            entry_ts = pd.Timestamp(sig["entry_ts"])

            # No overlap with prior trade
            if last_exit_ts is not None and entry_ts <= last_exit_ts:
                continue
            # Hard cutoff at 14:45
            if not _before_cutoff(entry_ts):
                continue

            # ── R:R gate ──────────────────────────────────────────────────
            if rr_min > 0 and opp_zone_trigger is not None and not df_opp.empty:
                rr = _calc_rr(sig["entry_price"], sig["sl_price"],
                              df_today, df_opp, opp_zone_trigger)
                if rr is not None and rr < rr_min:
                    print(f"  SKIP {today_str} {opt_type}  entry={sig['entry_price']:.1f} "
                          f"sl={sig['sl_price']:.1f}  R:R={rr:.2f} < {rr_min}")
                    continue
                if rr is not None:
                    sig["rr"] = rr

            sig["opt_type"] = opt_type

            # ── Simulate ───────────────────────────────────────────────────
            result = _simulate_trade(
                sig, df_today, df_opp, opp_zone_trigger, lots, lot_size
            )
            if result is None:
                continue

            result["gap_fired"] = gap_fired
            result["gap_dir"]   = gap_dir or "-"
            result["atm"]       = atm
            result["strike"]    = strike_map[opt_type]
            result["rr"]        = sig.get("rr", 0)

            last_exit_ts = pd.Timestamp(result["exit_ts"])
            trades.append(result)

            sl_tag = result.get("sl_type", "ZONE_LOW")
            rr_tag = f"  RR={result['rr']:.2f}" if result.get("rr") else ""
            if result.get("t1_hit"):
                print(f"  {today_str} {opt_type} [{sl_tag}]{rr_tag}"
                      f"  entry={result['entry_price']:.1f} ({str(entry_ts)[11:16]})"
                      f"  sl={result['sl_price']:.1f}"
                      f"  T1={result['t1_price']:.1f} ({str(result['t1_ts'])[11:16]})"
                      f"  +Rs{result['t1_pnl_rs']:.0f}(50%)"
                      f"  trail_exit={result['trail_exit_px']:.1f} ({str(result['trail_exit_ts'])[11:16]})"
                      f"  Rs{result['trail_pnl_rs']:+.0f}(50%)"
                      f"  TOTAL=Rs{result['pnl_rs']:+.0f}  [{result['reason']}]")
            else:
                print(f"  {today_str} {opt_type} [{sl_tag}]{rr_tag}"
                      f"  entry={result['entry_price']:.1f} ({str(entry_ts)[11:16]})"
                      f"  sl={result['sl_price']:.1f}"
                      f"  exit={result['exit_price']:.1f} ({str(last_exit_ts)[11:16]})"
                      f"  Rs{result['pnl_rs']:+.0f}  [{result['reason']}]")

            # ── Rotation: if T1 hit, check OPP side ───────────────────────
            if result.get("t1_hit") and opp_type in side_data:
                _try_opp_entry(
                    opp_type, opp_zones,
                    side_data[opp_type]["df_today"],
                    side_data.get(opt_type, {}).get("df_today", pd.DataFrame()),
                    float(sig["zone_trigger"]),  # CE zone_trigger = PE's T1
                    last_exit_ts,
                    lots, lot_size, rr_min,
                    today_str, gap_fired, gap_dir, atm, trades,
                )

    return trades


def _try_opp_entry(opt_type: str,
                   opp_zones: list,
                   df_today: pd.DataFrame,
                   df_opp: pd.DataFrame,
                   opp_zone_trigger: Optional[float],
                   after_ts: pd.Timestamp,
                   lots: int,
                   lot_size: int,
                   rr_min: float,
                   today_str: str,
                   gap_fired: bool,
                   gap_dir: Optional[str],
                   atm: int,
                   trades: list) -> None:
    """
    After a rotation (T1 hit on prior leg), check if OPP side has an entry.
    Entry must be after after_ts and within 3 bars.
    """
    if not opp_zones:
        return

    # On a gap day where OPP is the exit-only side, no new OPP entry
    if gap_fired and gap_dir == "UP" and opt_type == "PE":
        return
    if gap_fired and gap_dir == "DOWN" and opt_type == "CE":
        return

    signals = _find_3m_entries(df_today, opp_zones)
    if not signals:
        return

    for sig in signals:
        entry_ts = pd.Timestamp(sig["entry_ts"])
        if entry_ts <= after_ts:
            continue
        if not _before_cutoff(entry_ts):
            break

        if rr_min > 0 and opp_zone_trigger is not None and not df_opp.empty:
            rr = _calc_rr(sig["entry_price"], sig["sl_price"],
                          df_today, df_opp, opp_zone_trigger)
            if rr is not None and rr < rr_min:
                continue
            sig["rr"] = rr or 0

        sig["opt_type"] = opt_type
        result = _simulate_trade(sig, df_today, df_opp, opp_zone_trigger, lots, lot_size)
        if result is None:
            continue

        result["gap_fired"] = gap_fired
        result["gap_dir"]   = gap_dir or "-"
        result["atm"]       = atm
        result["rr"]        = sig.get("rr", 0)
        result["rotation"]  = True

        trades.append(result)
        print(f"  ROTATION → {opt_type}  "
              f"entry={result['entry_price']:.1f}  "
              f"exit={result['exit_price']:.1f}  "
              f"pnl=₹{result['pnl_rs']:+.0f}  {result['reason']}")
        break  # one rotation trade


# ── Main backtest ──────────────────────────────────────────────────────────────

def run_backtest_3m(token: str,
                    index: str = "NIFTY",
                    days: int = 20,
                    lots: int = 2,
                    htf_min: int = 75,
                    rr_min: float = 1.5,
                    sl_buf: float = 10.0,
                    min_risk: float = 10.0,
                    use_bias: bool = True) -> dict:
    """
    Run 3m-confirmation backtest and return:
      {"ok": True, "trades": [...], "summary": {...}}
    """
    # Inject token into shared headers dict
    _nb._HEADERS["Authorization"] = f"Bearer {token}"
    _nb._HEADERS["Accept"]        = "application/json"

    if not REGISTRY.is_loaded(index):
        print(f"Loading REGISTRY for {index}...")
        REGISTRY.load_sync(index, token)

    cfg = INDEX_CFG.get(index, INDEX_CFG["NIFTY"])

    # Build trading day list (newest first → reverse for processing)
    today = date.today()
    trading_days: list[date] = []
    d = today - timedelta(days=1)
    while len(trading_days) < days:
        if d.weekday() < 5:
            trading_days.append(d)
        d -= timedelta(days=1)
    trading_days.reverse()

    fetch_from = (trading_days[0] - timedelta(days=30)).isoformat()

    # Spot bars for gap detection
    spot_key = cfg.get("spot_key", "NSE_INDEX|Nifty 50")
    print(f"Fetching spot ({spot_key}) from {fetch_from}...")
    try:
        df_spot_raw = _fetch_1m(spot_key, fetch_from, today.isoformat())
        df_spot_all = _mkt_hours(df_spot_raw)
    except Exception as exc:
        print(f"Spot fetch failed: {exc}")
        df_spot_all = pd.DataFrame()

    cache: dict = {}
    all_trades: list[dict] = []

    for td in trading_days:
        print(f"\n--- {td} {'(GAP bias)' if use_bias else ''} ---")
        day_trades = _run_day(
            index, cfg, td, df_spot_all,
            use_bias, lots, htf_min, rr_min, sl_buf, min_risk,
            cache, fetch_from
        )
        all_trades.extend(day_trades)

    return _summarise(all_trades, lots, cfg["lot"])


def _summarise(trades: list[dict], lots: int, lot_size: int) -> dict:
    if not trades:
        print("\nNo trades generated.")
        return {"ok": True, "trades": [], "summary": {"total": 0}}

    wins   = [t for t in trades if t["pnl_rs"] > 0]
    losses = [t for t in trades if t["pnl_rs"] <= 0]

    total_pnl  = sum(t["pnl_rs"] for t in trades)
    win_rate   = round(len(wins) / len(trades) * 100, 1)
    avg_win    = round(sum(t["pnl_rs"] for t in wins)   / len(wins),   0) if wins   else 0
    avg_loss   = round(sum(t["pnl_rs"] for t in losses) / len(losses), 0) if losses else 0
    gross_win  = sum(t["pnl_rs"] for t in wins)
    gross_loss = abs(sum(t["pnl_rs"] for t in losses))
    pf         = round(gross_win / gross_loss, 2) if gross_loss > 0 else 999

    by_reason: dict[str, dict] = {}
    for t in trades:
        r = t["reason"]
        if r not in by_reason:
            by_reason[r] = {"count": 0, "pnl": 0, "wins": 0}
        by_reason[r]["count"] += 1
        by_reason[r]["pnl"]   += t["pnl_rs"]
        by_reason[r]["wins"]  += 1 if t["win"] else 0

    summary = {
        "total":      len(trades),
        "wins":       len(wins),
        "losses":     len(losses),
        "win_rate":   win_rate,
        "total_pnl":  round(total_pnl, 0),
        "avg_win":    avg_win,
        "avg_loss":   avg_loss,
        "pf":         pf,
        "lots":       lots,
        "lot_size":   lot_size,
        "by_reason":  by_reason,
    }

    print(f"\n{'='*55}", flush=True)
    print(f"  Trades    : {len(trades)}   Wins: {len(wins)}   Losses: {len(losses)}")
    print(f"  Win Rate  : {win_rate}%")
    print(f"  Total P&L : ₹{total_pnl:+,.0f}")
    print(f"  Avg Win   : ₹{avg_win:+,.0f}   Avg Loss: ₹{avg_loss:+,.0f}")
    print(f"  Prof. Fac.: {pf}")
    print(f"\n  By Exit Reason:")
    for reason, stats in by_reason.items():
        wr = round(stats["wins"] / stats["count"] * 100, 0)
        print(f"    {reason:<22} {stats['count']:>3} trades  "
              f"₹{stats['pnl']:+,.0f}  win={wr:.0f}%")

    return {"ok": True, "trades": trades, "summary": summary}


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="NIFTY 3m confirmation backtest")
    ap.add_argument("--token",    required=True, help="Upstox access token")
    ap.add_argument("--index",    default="NIFTY")
    ap.add_argument("--days",     type=int,   default=20, help="Trading days to back-test")
    ap.add_argument("--lots",     type=int,   default=2)
    ap.add_argument("--htf",      type=int,   default=75, help="HTF timeframe minutes")
    ap.add_argument("--rr-min",   type=float, default=1.5, help="Min R:R ratio (0=skip check)")
    ap.add_argument("--sl-buf",   type=float, default=10.0, help="Buffer below zone_low/dip-low for SL (pts)")
    ap.add_argument("--min-risk", type=float, default=10.0, help="Min entry-to-SL distance (pts), skip if smaller")
    ap.add_argument("--no-bias",  action="store_true", help="Disable gap-direction bias")
    args = ap.parse_args()

    result = run_backtest_3m(
        token    = args.token,
        index    = args.index,
        days     = args.days,
        lots     = args.lots,
        htf_min  = args.htf,
        rr_min   = args.rr_min,
        sl_buf   = args.sl_buf,
        min_risk = args.min_risk,
        use_bias = not args.no_bias,
    )
