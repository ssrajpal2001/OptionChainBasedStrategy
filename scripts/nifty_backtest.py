"""
scripts/nifty_backtest.py — NIFTY / SENSEX options backtest (2-week rolling).

Strategy:
  Morning setup (spot only):
    Prev day H/L/C → pivot (P, R1, R2, S1, S2)
    Today open → gap check

  No gap  → CE = S1 strike, PE = R1 strike
             HTF 75min scan on BOTH option charts (prev week + today)
             First trap that fires → entry (no directional bias from spot)
             OR both fire → take both

  Gap     → CE = ATM-1ITM, PE = ATM+1ITM (gap direction gives bias)
             Intraday cascade 30min→5min on option bars

  All zone detection and LTF entries run on OPTION PREMIUM bars.
  Spot is used only at morning setup (gap / pivot / strike selection).

  Exit: T1 = 50% at zone_high (BEAR) / zone_low (BULL) of option zone.
        Remaining 50%: 5min ratchet trail on option bars until EOD.

Usage (CLI):
  python scripts/nifty_backtest.py --token TOKEN
  python scripts/nifty_backtest.py --token TOKEN --index SENSEX --weeks 2
  python scripts/nifty_backtest.py --token TOKEN --start 2026-06-10 --end 2026-06-21
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta
from typing import Optional
from urllib.parse import quote

import pandas as pd

sys.path.insert(0, ".")

from strategies.trap_scanner import scanner
from data_layer.instrument_registry import REGISTRY

# ── Config ─────────────────────────────────────────────────────────────────────
INDEX_CFG = {
    "NIFTY": {
        "spot_key":  "NSE_INDEX|Nifty 50",
        "step":      50,
        "lot":       75,
        "gap_near":  50,   # 1 ITM = 1 step
        "gap_thresh": 0.5, # % gap to classify as gap day
        "htf_min":   75,
        "ltf_min":   5,
        "sq_time":   "15:25",
    },
    "SENSEX": {
        "spot_key":  "BSE_INDEX|SENSEX",
        "step":      100,
        "lot":       20,
        "gap_near":  100,
        "gap_thresh": 0.5,
        "htf_min":   75,
        "ltf_min":   5,
        "sq_time":   "15:25",
    },
}

_HEADERS: dict = {}   # set by CLI / API caller


# ── Data fetch ─────────────────────────────────────────────────────────────────
def _fetch_1m(key: str, from_dt: str, to_dt: str) -> pd.DataFrame:
    enc = quote(key, safe="")
    url = f"https://api.upstox.com/v2/historical-candle/{enc}/1minute/{to_dt}/{from_dt}"
    import requests
    r = requests.get(url, headers=_HEADERS, timeout=30)
    r.raise_for_status()
    candles = r.json().get("data", {}).get("candles", [])
    if not candles:
        return pd.DataFrame()
    rows = [{"datetime": pd.to_datetime(c[0]),
             "open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4]),
             "volume": int(c[5])}
            for c in reversed(candles)]
    df = pd.DataFrame(rows)
    df["datetime"] = df["datetime"].dt.tz_localize(None)
    return df


def _mkt_hours(df: pd.DataFrame) -> pd.DataFrame:
    return df[(df["datetime"].dt.time >= pd.Timestamp("09:15").time()) &
              (df["datetime"].dt.time <= pd.Timestamp("15:30").time())]


def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    dfc = df.set_index("datetime")
    htf = dfc.resample(f"{minutes}min").agg(
        {"open": "first", "high": "max", "low": "min",
         "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return htf


# ── Zone dedup: merge zones within price_tol of each other ────────────────────
def _dedup_zones(zones: list, price_tol: float = 10.0) -> list:
    """
    If multiple zones have zone_high within price_tol of each other they are
    the same structure seen at different bar resolutions. Keep only the one
    whose entry signal (closed_on or trapped_on) came earliest.
    """
    if not zones:
        return zones

    def _entry_ts(z):
        ts = z.get("closed_on") or z.get("trapped_on") or z.get("ref_ts")
        return str(ts) if ts else ""

    # Sort by entry time ascending so earliest survives the cluster
    ordered = sorted(zones, key=_entry_ts)
    kept = []
    for z in ordered:
        zh = float(z.get("zone_high", 0))
        # Check if any already-kept zone is within price_tol
        is_dup = any(abs(float(k.get("zone_high", 0)) - zh) <= price_tol for k in kept)
        if not is_dup:
            kept.append(z)
    return kept


# ── Pivot / gap ────────────────────────────────────────────────────────────────
def _pivot(H: float, L: float, C: float) -> dict:
    P = (H + L + C) / 3
    return {"P": P, "R1": 2*P - L, "R2": P + (H - L),
            "S1": 2*P - H, "S2": P - (H - L)}


def _round_strike(v: float, step: int) -> int:
    return int(round(v / step) * step)


# ── Instrument key lookup ──────────────────────────────────────────────────────
def _get_expiry(index: str, from_date: date) -> tuple[date, str]:
    """Return (expiry_date, expiry_str) for the nearest weekly expiry >= from_date."""
    # Try REGISTRY first (most reliable for BSE numeric keys)
    if REGISTRY.is_loaded(index):
        exp = REGISTRY.get_active_expiry(index, from_date=from_date)
        if exp:
            return exp, exp.strftime("%d%b%y").upper()
    # Fallback: weekday math
    _DOW = {"NIFTY": 3, "BANKNIFTY": 2, "FINNIFTY": 1, "SENSEX": 3, "MIDCPNIFTY": 1}
    dow = _DOW.get(index, 3)
    d = from_date
    for _ in range(7):
        if d.weekday() == dow:
            return d, d.strftime("%d%b%y").upper()
        d += timedelta(days=1)
    return from_date, from_date.strftime("%d%b%y").upper()


def _option_key(index: str, strike: int, opt_type: str, trade_date: date) -> str:
    """Resolve Upstox instrument key for an option strike."""
    exp_date, exp_str = _get_expiry(index, trade_date)
    # REGISTRY lookup (required for BSE_FO numeric token)
    if REGISTRY.is_loaded(index):
        key = REGISTRY.get_upstox_key(index, exp_date, strike, opt_type)
        if key:
            return key
    # NSE fallback: symbol format works for historical API
    _PFX = {"NIFTY": "NSE_FO|", "BANKNIFTY": "NSE_FO|",
            "SENSEX": "BSE_FO|", "FINNIFTY": "NSE_FO|"}
    pfx = _PFX.get(index, "NSE_FO|")
    return f"{pfx}{index}{exp_str}{strike}{opt_type}"


# ── Exit simulation ────────────────────────────────────────────────────────────
def _zone_trigger(e: dict) -> float:
    if "zone_trigger" in e:
        return float(e["zone_trigger"])
    zh, zl = float(e["zone_high"]), float(e["zone_low"])
    if e.get("kind") == "BULL":
        return round(zh - (zh - zl) / 3, 2)
    return round(zl + (zh - zl) / 3, 2)


def _init_sl(e: dict, sl_buf: float) -> float:
    """Initial SL: just beyond the zone boundary."""
    if e.get("kind") == "BULL":
        return round(float(e["zone_high"]) + sl_buf, 2)
    return round(float(e["zone_low"]) - sl_buf, 2)


def _simulate_exit(e: dict, df1m: pd.DataFrame, lot: int, sl_buf: float,
                   opt_type: str, trade_date: str) -> Optional[dict]:
    """
    Exit rules:
      Entry : zone_trigger (1/3 into zone from inner boundary)
      SL    : just beyond outer zone boundary (zone_high+buf for BEAR, zone_low-buf for BULL)
      T1    : 50% qty booked when price moves 1× the zone width beyond entry
              BEAR: entry + (zone_high - zone_low)
              BULL: entry - (zone_high - zone_low)
      T2    : remaining 50% trailed on 5min bar lows/highs (ratchet) until SL hit or EOD
    """
    total_qty = lot * 2   # 2 lots
    t1_qty    = total_qty // 2
    rem_qty   = total_qty - t1_qty

    # scan_htf detects BEARISH traps on option premium.
    # We are BUYERS of the option → expect premium to RISE (bullish on premium).
    # BEAR trap: bears shorted, got trapped above zone_high.
    #   Entry  = zone_trigger (1/3 from zone_low into zone)
    #   T1     = zone_high (bears' SL = premium rises here = our 50% target)
    #   SL     = zone_low - sl_buf (premium drops further = trap failed)
    #   Trail  = ratchet SL UP as premium rises (lock profit on remainder)
    zh = float(e["zone_high"])
    zl = float(e["zone_low"])
    entry_price = _zone_trigger(e)
    t1_price    = round(zh, 2)              # T1 = zone_high (bears' SL)
    init_sl     = round(zl - sl_buf, 2)    # SL = below zone_low
    trail_sl    = init_sl

    # closed_on = price returned to zone = entry signal; trapped_on = trap sweep
    trap_ts = pd.to_datetime(e.get("closed_on") or e.get("trapped_on"))
    if trap_ts is pd.NaT:
        return None
    trap_ts = trap_ts.tz_localize(None) if trap_ts.tzinfo else trap_ts
    future = df1m[df1m["datetime"] > trap_ts]
    if future.empty:
        return None

    t1_hit     = False
    t1_pnl     = 0.0
    t1_exit_ts = None
    exit_price  = None
    exit_reason = "OPEN"
    exit_ts     = None
    last_5m_ts  = None

    for _, row in future.iterrows():
        bar_ts = row["datetime"]

        # After T1: ratchet trail SL UP on each new 5min bar (premium rises)
        if t1_hit:
            bucket = bar_ts.floor("5min")
            if last_5m_ts is None or bucket > last_5m_ts:
                last_5m_ts = bucket
                prev5 = df1m[(df1m["datetime"] >= bucket - pd.Timedelta(minutes=5)) &
                             (df1m["datetime"] < bucket)]
                if not prev5.empty:
                    # We are long premium: trail SL up using prev 5min LOW
                    cand = round(float(prev5["low"].min()) - sl_buf, 2)
                    if cand > trail_sl:
                        trail_sl = cand

        active_sl = trail_sl if t1_hit else init_sl

        # Check SL hit (premium drops to SL)
        if row["low"] <= active_sl:
            exit_price  = active_sl
            exit_reason = "TRAIL_SL" if t1_hit else "SL"
            exit_ts     = bar_ts
            break

        # Check T1 hit (premium rises to zone_high = 50% book)
        if not t1_hit and row["high"] >= t1_price:
            t1_hit     = True
            t1_exit_ts = bar_ts
            t1_pnl     = round((t1_price - entry_price) * t1_qty, 2)
            # Move trail SL to entry (break-even lock for remainder)
            trail_sl   = round(entry_price - sl_buf, 2)

        # EOD square-off
        if bar_ts.time() >= pd.Timestamp("15:25").time():
            exit_price  = round(float(row["close"]), 2)
            exit_reason = "EOD"
            exit_ts     = bar_ts
            break

    if exit_price is None:
        last = future.iloc[-1]
        exit_price  = round(float(last["close"]), 2)
        exit_reason = "EOD"
        exit_ts     = last["datetime"]

    # P&L — always long premium (buy CE/PE, exit when price rises or SL)
    rem_pnl   = round((exit_price - entry_price) * rem_qty, 2)
    total_pnl = round(t1_pnl + rem_pnl, 2)

    return {
        "date":       trade_date,
        "opt_type":   opt_type,
        "kind":       e.get("kind", "BEAR"),
        "entry":      round(entry_price, 2),
        "t1":         round(t1_price, 2),
        "sl":         round(init_sl, 2),
        "entry_ts":   str(trap_ts)[:16],
        "t1_ts":      str(t1_exit_ts)[:16] if t1_exit_ts else "",
        "exit":       round(exit_price, 2),
        "exit_ts":    str(exit_ts)[:16] if exit_ts is not None else "",
        "reason":     exit_reason,
        "t1_hit":     t1_hit,
        "t1_pnl":     t1_pnl,
        "rem_pnl":    rem_pnl,
        "pnl_pts":    round(total_pnl / total_qty, 2) if total_qty else 0,
        "pnl_rs":     int(total_pnl),
        "zone_low":   round(float(e["zone_low"]), 2),
        "zone_high":  round(float(e["zone_high"]), 2),
        "zone":       f"{e['zone_low']:.0f}-{e['zone_high']:.0f}",
        "mode":       e.get("_mode", ""),
        "trap_pos":   e.get("_trap_pos", ""),   # FIRST / MIDDLE / LAST among 5min traps
    }


# ── Per-day backtest ───────────────────────────────────────────────────────────
def _run_day(index: str, cfg: dict, trade_date: str,
             df_spot_all: pd.DataFrame,
             use_bias: bool, sl_buf: float) -> list[dict]:
    """
    Run one trading day. df_spot_all has spot 1m bars for prev week + today.
    Returns list of trade dicts (may be empty).
    """
    td = pd.to_datetime(trade_date).date()
    step       = cfg["step"]
    htf_min    = cfg["htf_min"]
    ltf_min    = cfg["ltf_min"]
    gap_thresh = cfg["gap_thresh"]
    lot        = cfg["lot"]

    df_prev  = df_spot_all[df_spot_all["datetime"].dt.date < td].copy()
    df_today = _mkt_hours(df_spot_all[df_spot_all["datetime"].dt.date == td].copy())
    if df_prev.empty or df_today.empty:
        return []

    # Prev day OHLC
    prev_H = float(df_prev["high"].max())
    prev_L = float(df_prev["low"].min())
    prev_C = float(df_prev["close"].iloc[-1])
    piv    = _pivot(prev_H, prev_L, prev_C)

    today_open = float(df_today["open"].iloc[0])
    gap_pct    = abs(today_open - prev_C) / prev_C * 100 if prev_C > 0 else 0.0
    gap_fired  = gap_pct >= gap_thresh
    gap_dir    = "UP" if today_open >= prev_C else "DOWN"

    # Strike selection
    trade_dt_obj = td
    if gap_fired:
        atm        = _round_strike(today_open, step)
        ce_strike  = atm - cfg["gap_near"]
        pe_strike  = atm + cfg["gap_near"]
        mode       = f"GAP {gap_dir} {gap_pct:.1f}%"
    else:
        ce_strike   = _round_strike(piv["S1"], step)
        pe_strike   = _round_strike(piv["R1"], step)
        mode        = f"NOGAP pivot P={piv['P']:.0f} S1={piv['S1']:.0f} R1={piv['R1']:.0f}"

    ce_key = _option_key(index, ce_strike, "CE", trade_dt_obj)
    pe_key = _option_key(index, pe_strike, "PE", trade_dt_obj)

    # Fetch option bars (prev week + today for HTF zone history)
    fetch_from = (td - timedelta(days=14)).isoformat()
    fetch_to   = (td + timedelta(days=1)).isoformat()

    trades = []

    for opt_type, strike, key in [("CE", ce_strike, ce_key), ("PE", pe_strike, pe_key)]:
        # Bias filter: on gap days, skip the leg that opposes gap direction
        if use_bias and gap_fired:
            if gap_dir == "UP" and opt_type == "PE":
                continue
            if gap_dir == "DOWN" and opt_type == "CE":
                continue

        if not key:
            print(f"  {trade_date} {opt_type} {strike}: no instrument key — skip")
            continue

        try:
            df_opt_raw = _fetch_1m(key, fetch_from, fetch_to)
            time.sleep(0.2)
        except Exception as exc:
            print(f"  {trade_date} {opt_type} {strike}: fetch error {exc}")
            continue

        if df_opt_raw.empty:
            print(f"  {trade_date} {opt_type} {strike}: no option data")
            continue

        df_opt_all   = _mkt_hours(df_opt_raw)
        df_opt_today = df_opt_all[df_opt_all["datetime"].dt.date == td].copy()

        if df_opt_today.empty:
            print(f"  {trade_date} {opt_type} {strike}: no today option bars")
            continue

        # ── Step 1: 75min HTF scan on full history (prev week + today) ──────
        htf_bars = _resample(df_opt_all, htf_min)
        _, htf_entries = scanner.scan_htf(htf_bars) if len(htf_bars) >= 2 else (None, [])

        def _closed_today(e):
            ts = e.get("closed_on")   # CLOSED = price returned to zone = entry ready
            if not ts:
                return False
            try:
                return pd.to_datetime(ts).date() == td
            except Exception:
                return False

        htf_zones = [e for e in htf_entries if e.get("status") == "CLOSED" and _closed_today(e)]

        entry_signals = []   # list of (entry_ts, entry_price, sl, t1, zone_low, zone_high, mode_tag)

        if htf_zones:
            # HTF zone fired today — use directly, entry at zone_trigger on 1min bars
            n = len(htf_zones)
            for idx, z in enumerate(htf_zones):
                z["_mode"] = f"HTF-{htf_min}m"
                z["_trap_pos"] = ("FIRST" if idx == 0
                                  else "LAST" if idx == n - 1
                                  else "MIDDLE")
                entry_signals.append(z)
            print(f"  {trade_date} {opt_type} {strike} [{mode}]: {len(htf_zones)} HTF zone(s)")
        else:
            # ── Step 2: No HTF zone (or zone is far) → 15min intraday cascade ──
            df_15 = _resample(df_opt_today, 15)
            _, cas15 = scanner.scan_htf(df_15) if len(df_15) >= 2 else (None, [])

            # TRAPPED or CLOSED on 15min intraday bars
            cas_zones = [e for e in cas15 if e.get("status") in ("TRAPPED", "CLOSED")]

            if not cas_zones:
                print(f"  {trade_date} {opt_type} {strike}: no zones (HTF or 15m)")
                continue

            # ── Step 3: For each 15min zone → scan 5min bars for LTF entry ──
            df_5 = _resample(df_opt_today, 5)

            for cz in cas_zones:
                zh = float(cz["zone_high"])
                zl = float(cz["zone_low"])
                trap_ts = pd.to_datetime(cz.get("trapped_on") or cz.get("ref_ts"))
                if trap_ts is not pd.NaT:
                    trap_ts = trap_ts.tz_localize(None) if trap_ts.tzinfo else trap_ts

                # 5min bars INSIDE the 15min zone, after it was detected
                df_5_in_zone = df_5[df_5["datetime"] >= trap_ts] if trap_ts is not pd.NaT else df_5

                # Bearish LTF trap inside the 15min zone
                _, ltf5 = scanner.scan_htf(df_5_in_zone) if len(df_5_in_zone) >= 2 else (None, [])
                ltf5_in = [e for e in ltf5
                           if e.get("status") in ("TRAPPED", "CLOSED")
                           and float(e["zone_high"]) <= zh * 1.02
                           and float(e["zone_low"])  >= zl * 0.98]

                if not ltf5_in:
                    # No 5min sub-trap found — use the 15min zone itself as entry
                    cz["_mode"] = "CASCADE-15m"
                    cz["_trap_pos"] = "ONLY"
                    entry_signals.append(cz)
                else:
                    n = len(ltf5_in)
                    for idx, le in enumerate(ltf5_in):
                        le["_mode"] = "CASCADE-15m->5m"
                        le["_trap_pos"] = ("FIRST" if idx == 0
                                           else "LAST" if idx == n - 1
                                           else "MIDDLE")
                        entry_signals.append(le)

            print(f"  {trade_date} {opt_type} {strike} [{mode}]: {len(entry_signals)} cascade entry(s)")

        if not entry_signals:
            continue

        # Merge zones that are at the same price level (within 10 pts) — take earliest
        entry_signals = _dedup_zones(entry_signals, price_tol=10.0)

        # ── Simulate exit on today's 1min bars for each entry signal ──
        for z in entry_signals:
            result = _simulate_exit(z, df_opt_today, lot, sl_buf, opt_type, trade_date)
            if result:
                result["strike"]    = strike
                result["index"]     = index
                result["gap_pct"]   = round(gap_pct, 2)
                result["gap_fired"] = gap_fired
                result["mode"]     += f" {mode}"
                trades.append(result)

    return trades


# ── Date helpers ───────────────────────────────────────────────────────────────
def _trading_days(start: date, end: date) -> list[str]:
    result, d = [], start
    while d <= end:
        if d.weekday() < 5:
            result.append(d.isoformat())
        d += timedelta(days=1)
    return result


# ── Public entry point (called by API + CLI) ───────────────────────────────────
def run_nifty_backtest(token: str, index: str = "NIFTY", weeks: int = 2,
                       start: str = "", end: str = "",
                       use_bias: bool = True, sl_buf: float = 2.0) -> dict:
    global _HEADERS
    _HEADERS = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    cfg = INDEX_CFG.get(index.upper())
    if not cfg:
        return {"ok": False, "error": f"Unknown index {index}"}

    # Load REGISTRY for instrument key lookup (BSE_FO needs numeric tokens)
    try:
        REGISTRY.load_sync(index.upper(), access_token=token)
    except Exception as exc:
        print(f"[REGISTRY] load failed ({exc}) — will use NSE symbol fallback")

    # Date range
    if start and end:
        s_date = date.fromisoformat(start)
        e_date = date.fromisoformat(end)
    else:
        e_date = date.today()
        s_date = e_date - timedelta(weeks=weeks)

    days = _trading_days(s_date, e_date)
    print(f"\n{index} backtest  {s_date} to {e_date}  ({len(days)} days)  "
          f"bias={'ON' if use_bias else 'OFF'}  sl_buf={sl_buf}")

    # Fetch spot bars for entire range (+ 1 extra week for prev-day pivot)
    spot_from = (s_date - timedelta(days=14)).isoformat()
    spot_to   = (e_date + timedelta(days=1)).isoformat()
    spot_key  = cfg["spot_key"]
    print(f"Fetching spot bars {spot_from} to {spot_to}...")
    df_spot_all = _fetch_1m(spot_key, spot_from, spot_to)
    if df_spot_all.empty:
        return {"ok": False, "error": "No spot data"}
    df_spot_all = _mkt_hours(df_spot_all)
    print(f"  {len(df_spot_all)} spot bars loaded\n")

    all_trades: list[dict] = []
    for td in days:
        day_trades = _run_day(index, cfg, td, df_spot_all, use_bias, sl_buf)
        all_trades.extend(day_trades)

    # Summary
    wins   = [t for t in all_trades if t["pnl_rs"] > 0]
    losses = [t for t in all_trades if t["pnl_rs"] <= 0]
    total  = sum(t["pnl_rs"] for t in all_trades)
    gw = sum(t["pnl_rs"] for t in wins)
    gl = abs(sum(t["pnl_rs"] for t in losses))
    pf = round(gw / gl, 2) if gl > 0 else 99.0

    summary = {
        "index": index, "start": str(s_date), "end": str(e_date),
        "days": len(days), "trades": len(all_trades),
        "wins": len(wins), "losses": len(losses),
        "win_pct": round(100 * len(wins) / len(all_trades), 1) if all_trades else 0.0,
        "total_rs": int(total), "profit_factor": pf,
        "avg_win":  round(gw / len(wins), 0) if wins else 0,
        "avg_loss": round(-gl / len(losses), 0) if losses else 0,
    }

    # Equity curve (daily cumulative)
    eq_map: dict[str, int] = {}
    running = 0
    for t in sorted(all_trades, key=lambda x: x["date"]):
        running += t["pnl_rs"]
        eq_map[t["date"]] = running

    print(f"\n{'─'*60}")
    print(f"{index}  {s_date} to {e_date}  Trades={len(all_trades)}  "
          f"Win={summary['win_pct']:.1f}%  Rs {total:+,.0f}  PF={pf}")
    print(f"{'─'*60}")
    for t in all_trades:
        print(f"  {t['date']}  {t['opt_type']} {t['strike']}  "
              f"{t['mode']:<30}  {t['entry_ts'][11:]} to {t['exit_ts'][11:]}  "
              f"entry={t['entry']}  exit={t['exit']}  {t['reason']:<10}  "
              f"Rs {t['pnl_rs']:+,.0f}")

    return {
        "ok": True,
        "summary": summary,
        "trades": all_trades,
        "equity": [{"date": d, "equity": v} for d, v in sorted(eq_map.items())],
    }


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--token",   required=True)
    ap.add_argument("--index",   default="NIFTY", choices=["NIFTY", "SENSEX"])
    ap.add_argument("--weeks",   type=int, default=2)
    ap.add_argument("--start",   default="")
    ap.add_argument("--end",     default="")
    ap.add_argument("--no-bias", action="store_true", help="Scan both CE+PE on gap days too")
    ap.add_argument("--sl-buf",  type=float, default=2.0)
    args = ap.parse_args()

    result = run_nifty_backtest(
        token    = args.token,
        index    = args.index,
        weeks    = args.weeks,
        start    = args.start,
        end      = args.end,
        use_bias = not args.no_bias,
        sl_buf   = args.sl_buf,
    )
    if not result["ok"]:
        print(f"ERROR: {result['error']}")
        sys.exit(1)
