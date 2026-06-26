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
        "step":      100,
        "lot":       65,
        "gap_near":  200,  # CE1=ATM-200, PE1=ATM+200  ← matches live scanner
        "gap_far":   400,  # CE2=ATM-400, PE2=ATM+400  ← matches live scanner
        "gap_thresh": 0.5, # % gap to classify as gap day
        "htf_min":   75,
        "ltf_min":   5,
        "sq_time":   "15:25",
    },
    "SENSEX": {
        "spot_key":  "BSE_INDEX|SENSEX",
        "step":      100,
        "lot":       20,
        "gap_near":  300,  # CE1=ATM-300, PE1=ATM+300
        "gap_far":   600,  # CE2=ATM-600, PE2=ATM+600
        "gap_thresh": 0.5,
        "htf_min":   75,
        "ltf_min":   5,
        "sq_time":   "15:25",
    },
    "BANKNIFTY": {
        "spot_key":  "NSE_INDEX|Nifty Bank",
        "step":      100,
        "lot":       15,
        "gap_near":  400,  # CE1=ATM-400, PE1=ATM+400 (wider — BNF moves 2-3x NIFTY)
        "gap_far":   800,
        "gap_thresh": 0.5,
        "htf_min":   75,
        "ltf_min":   5,
        "sq_time":   "15:25",
    },
    "FINNIFTY": {
        "spot_key":  "NSE_INDEX|Nifty Fin Service",
        "step":      50,
        "lot":       40,
        "gap_near":  200,
        "gap_far":   400,
        "gap_thresh": 0.5,
        "htf_min":   75,
        "ltf_min":   5,
        "sq_time":   "15:25",
    },
    "MIDCPNIFTY": {
        "spot_key":  "NSE_INDEX|Nifty Midcap Select",
        "step":      25,
        "lot":       75,
        "gap_near":  100,
        "gap_far":   200,
        "gap_thresh": 0.5,
        "htf_min":   75,
        "ltf_min":   5,
        "sq_time":   "15:25",
    },
}

_HEADERS:       dict = {}   # set by CLI / API caller
_FIXED_EXPIRY:  str  = ""   # when set, all option keys use this expiry (e.g. "31JUL26")


# ── Data fetch ─────────────────────────────────────────────────────────────────
def _fetch_1m_chunk(key: str, from_dt: str, to_dt: str) -> pd.DataFrame:
    """Fetch one chunk (≤30 days) of 1min bars from Upstox."""
    import requests
    enc = quote(key, safe="")
    url = f"https://api.upstox.com/v2/historical-candle/{enc}/1minute/{to_dt}/{from_dt}"
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


def _fetch_1m(key: str, from_dt: str, to_dt: str) -> pd.DataFrame:
    """Fetch 1min bars, splitting into 28-day chunks to stay within Upstox API limits."""
    f = date.fromisoformat(from_dt)
    t = date.fromisoformat(to_dt)
    chunks: list[pd.DataFrame] = []
    cur = f
    while cur <= t:
        nxt = min(cur + timedelta(days=28), t)
        try:
            chunk = _fetch_1m_chunk(key, cur.isoformat(), nxt.isoformat())
            if not chunk.empty:
                chunks.append(chunk)
            time.sleep(0.1)
        except Exception as exc:
            print(f"    [fetch] {key} {cur}→{nxt} failed: {exc}")
        cur = nxt + timedelta(days=1)
    if not chunks:
        return pd.DataFrame()
    df = pd.concat(chunks, ignore_index=True).sort_values("datetime").drop_duplicates("datetime")
    return df.reset_index(drop=True)


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

# Expiry weekday per index: weekly
_WEEKLY_DOW  = {"NIFTY": 3, "BANKNIFTY": 2, "FINNIFTY": 1,
                "SENSEX": 4, "MIDCPNIFTY": 1}   # 0=Mon … 4=Fri
# Expiry weekday per index: monthly (last occurrence in month)
_MONTHLY_DOW = {"NIFTY": 3, "BANKNIFTY": 2, "FINNIFTY": 1,
                "SENSEX": 4, "MIDCPNIFTY": 1}


def _last_weekday_of_month(year: int, month: int, dow: int) -> date:
    """Return the last occurrence of weekday `dow` (0=Mon) in the given month."""
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    d = date(year, month, last_day)
    while d.weekday() != dow:
        d -= timedelta(days=1)
    return d


def _monthly_expiry(index: str, from_date: date) -> tuple[date, str]:
    """
    Return the monthly expiry on or after from_date.

    Strategy: get all expiries from REGISTRY (sorted). A monthly expiry is
    the LAST expiry of its calendar month — detected by comparing consecutive
    expiries: if expiry[i].month != expiry[i+1].month, expiry[i] is monthly.
    If REGISTRY unavailable, fall back to last-weekday-of-month math.
    """
    if REGISTRY.is_loaded(index):
        all_exp = sorted(REGISTRY.all_expiries(index))
        # Filter to on or after from_date
        future = [e for e in all_exp if e >= from_date]
        # Walk pairs — if next expiry is a different month, this one is the monthly
        for i, exp in enumerate(future):
            if i + 1 >= len(future) or future[i + 1].month != exp.month:
                return exp, exp.strftime("%d%b%y").upper()

    # Fallback: last-weekday-of-month math
    dow = _MONTHLY_DOW.get(index, 3)
    for delta_m in range(3):
        month = from_date.month + delta_m
        year  = from_date.year + (month - 1) // 12
        month = ((month - 1) % 12) + 1
        exp = _last_weekday_of_month(year, month, dow)
        if exp >= from_date:
            return exp, exp.strftime("%d%b%y").upper()
    return from_date, from_date.strftime("%d%b%y").upper()


def _get_expiry(index: str, from_date: date,
                monthly: bool = False,
                next_week: bool = False) -> tuple[date, str]:
    """Return (expiry_date, expiry_str) for weekly / next-week / monthly expiry."""
    if monthly:
        return _monthly_expiry(index, from_date)
    # Weekly: try REGISTRY first (most reliable for BSE numeric keys)
    if REGISTRY.is_loaded(index):
        exp = REGISTRY.get_active_expiry(index, from_date=from_date)
        if exp:
            if next_week:
                # Skip current week — get the expiry AFTER this one
                exp2 = REGISTRY.get_active_expiry(index, from_date=exp + timedelta(days=1))
                if exp2:
                    return exp2, exp2.strftime("%d%b%y").upper()
            else:
                return exp, exp.strftime("%d%b%y").upper()
    # Fallback: weekday math (REGISTRY not loaded or no match).
    # next_week cannot be resolved accurately without real contract list —
    # calendar +7d can land on a holiday-shifted date; log warning.
    dow = _WEEKLY_DOW.get(index, 3)
    d = from_date
    for _ in range(7):
        if d.weekday() == dow:
            if next_week:
                print(f"  [WARN] next_week expiry: REGISTRY not loaded for {index}; "
                      f"calendar +7d fallback may be inaccurate — load REGISTRY first")
                d += timedelta(days=7)
            return d, d.strftime("%d%b%y").upper()
        d += timedelta(days=1)
    return from_date, from_date.strftime("%d%b%y").upper()


# Module-level flags — set by run_nifty_backtest before _run_day is called
_USE_MONTHLY:   bool = False
_USE_NEXT_WEEK: bool = False


def _option_key(index: str, strike: int, opt_type: str, trade_date: date) -> str:
    """Resolve Upstox instrument key for an option strike.

    When _FIXED_EXPIRY is set (e.g. '31JUL26'), that expiry is used for ALL dates
    instead of computing the nearest weekly/monthly — used for multi-expiry comparison
    where we want to test one specific July contract over the full Apr-Jun backtest period.
    REGISTRY lookup is skipped when fixed (NSE symbol format is always valid for NSE_FO).
    """
    if _FIXED_EXPIRY:
        exp_str = _FIXED_EXPIRY
        _PFX = {"NIFTY": "NSE_FO|", "BANKNIFTY": "NSE_FO|",
                "SENSEX": "BSE_FO|", "FINNIFTY": "NSE_FO|", "MIDCPNIFTY": "NSE_FO|"}
        pfx = _PFX.get(index.upper(), "NSE_FO|")
        return f"{pfx}{index}{exp_str}{strike}{opt_type}"

    exp_date, exp_str = _get_expiry(index, trade_date, monthly=_USE_MONTHLY, next_week=_USE_NEXT_WEEK)
    # REGISTRY lookup (required for BSE_FO numeric token; also gives accurate expiry dates)
    if REGISTRY.is_loaded(index):
        key = REGISTRY.get_upstox_key(index, exp_date, strike, opt_type)
        if key:
            return key
    # NSE fallback: symbol format works for historical API
    _PFX = {"NIFTY": "NSE_FO|", "BANKNIFTY": "NSE_FO|",
            "SENSEX": "BSE_FO|", "FINNIFTY": "NSE_FO|", "MIDCPNIFTY": "NSE_FO|"}
    pfx = _PFX.get(index.upper(), "NSE_FO|")
    return f"{pfx}{index}{exp_str}{strike}{opt_type}"


# ── Exit simulation ────────────────────────────────────────────────────────────
def _is_junk_day(df_today: pd.DataFrame, min_active: int = 10) -> bool:
    """True if the option bars lack volume — far-dated contract not yet liquid on this day."""
    if df_today.empty:
        return True
    cutoff = df_today["datetime"].iloc[0] + pd.Timedelta(minutes=60)
    first_hr = df_today[df_today["datetime"] <= cutoff]
    return int((first_hr["volume"] > 0).sum()) < min_active


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


def _spot_at_ts(df_spot: pd.DataFrame, ts: pd.Timestamp) -> float:
    """Return the spot close price at or just before ts."""
    prior = df_spot[df_spot["datetime"] <= ts]
    return float(prior["close"].iloc[-1]) if not prior.empty else 0.0


def _price_at_ts(df1m: pd.DataFrame, ts: pd.Timestamp) -> float:
    """Return close price from df1m at the bar that contains ts (first bar >= ts)."""
    if df1m.empty:
        return 0.0
    row = df1m[df1m["datetime"] >= ts]
    return float(row["close"].iloc[0]) if not row.empty else float(df1m["close"].iloc[-1])


def _simulate_exit(e: dict, df1m: pd.DataFrame, df5m: pd.DataFrame,
                   lot: int, sl_buf: float,
                   opt_type: str, trade_date: str, strike: int,
                   spot_at_entry: float,
                   profit_cap_per_lot: float = 0.0,
                   profit_floor_per_lot: float = 0.0,
                   df1m_scan: pd.DataFrame | None = None,
                   force_exit_ts: pd.Timestamp | None = None,
                   no_target_tsl: bool = False,
                   use_high_breakout: bool = True) -> Optional[dict]:
    """
    df1m       = exec strike bars (prices for entry/exit P&L)
    df1m_scan  = scan strike bars (SL/T1/TSL trigger logic); if None, uses df1m (no 1ITM)
    """
    # 1ITM: scan bars drive triggers (zone levels), exec bars drive entry/exit prices.
    # Only valid when scan and exec strikes are close (GAP trades: 150 pts apart).
    # For PIVOT trades df1m_scan is None → standard mode (scan=exec).
    use_1itm_mode = df1m_scan is not None and not df1m_scan.empty
    scan_bars = df1m_scan if use_1itm_mode else df1m
    exec_bars = df1m

    total_qty = lot * 2
    t1_qty    = total_qty // 2
    rem_qty   = total_qty - t1_qty

    zh = float(e["zone_high"])
    zl = float(e["zone_low"])
    scan_entry  = _zone_trigger(e)   # zone trigger level on scan strike
    t1_price    = round(float(e["_htf_t1"]) if "_htf_t1" in e else zh, 2)
    t2_price    = float(e["_t2"]) if e.get("_t2") else None
    # SL: if HTF zone_low override present (5min sub-trap inside HTF zone),
    # use HTF zone_low as absolute stop — not the tight 5min zone_low.
    init_sl     = float(e["_htf_sl"]) if "_htf_sl" in e else zl

    trap_ts = pd.to_datetime(e.get("closed_on") or e.get("trapped_on"))
    if trap_ts is pd.NaT:
        return None
    trap_ts = trap_ts.tz_localize(None) if trap_ts.tzinfo else trap_ts

    # ── 1min rejection-candle entry confirmation ─────────────────────────
    # At zone_low area, wait for a 1min setup candle, then:
    #   use_high_breakout=True  → enter only when next bar breaks ABOVE setup candle HIGH
    #                             (avoids entering on continued downtrends after bears cleared)
    #   use_high_breakout=False → enter at 50% midpoint (earlier entry, less confirmation)
    # If no confirmation within session → no trade.
    entry_price  = None
    actual_entry_ts = None
    future_1m_all = exec_bars[exec_bars["datetime"] > trap_ts]

    rejection_bar = None   # (high, low, ts) of first 1min bar that touched zone_low area

    for _, rb in future_1m_all.iterrows():
        rb_ts   = rb["datetime"]
        rb_high = float(rb["high"])
        rb_low  = float(rb["low"])

        if rejection_bar is None:
            # First 1min bar whose low touches zone_trigger (zone_low area)
            if rb_low <= scan_entry:
                rejection_bar = (rb_high, rb_low, rb_ts)
        else:
            rej_high, rej_low, _ = rejection_bar
            if use_high_breakout:
                # HIGH breakout: price must break ABOVE the setup candle's high
                if rb_high >= rej_high:
                    entry_price     = round(rej_high, 2)
                    actual_entry_ts = rb_ts
                    break
            else:
                # Midpoint entry: enter when price reaches 50% from rejection candle low
                midpoint = round(rej_low + (rej_high - rej_low) * 0.5, 2)
                if rb_high >= midpoint:
                    entry_price     = midpoint
                    actual_entry_ts = rb_ts
                    break

    if entry_price is None or entry_price <= 0:
        return None   # no 1min confirmation → skip trade

    # Update trap_ts to actual entry timestamp for simulation start
    trap_ts = actual_entry_ts

    future_scan = scan_bars[scan_bars["datetime"] > trap_ts]
    if future_scan.empty:
        return None

    # Pre-compute 5-min TSL trap events on scan strike bars
    trap_events: list[tuple[pd.Timestamp, float]] = []
    if not df5m.empty:
        df5m_post = df5m[df5m["datetime"] > trap_ts]
        if len(df5m_post) >= 2:
            _, zones5 = scanner.scan_htf(df5m_post)
            for z5 in zones5:
                if z5.get("status") not in ("TRAPPED", "CLOSED"):
                    continue
                z5_low = float(z5.get("zone_low", 0))
                if z5_low <= scan_entry:
                    continue
                ev_ts = pd.to_datetime(z5.get("trapped_on") or z5.get("closed_on"))
                if ev_ts is pd.NaT:
                    continue
                ev_ts = ev_ts.tz_localize(None) if ev_ts.tzinfo else ev_ts
                trap_events.append((ev_ts, z5_low))
    trap_events.sort(key=lambda x: x[0])

    t1_hit          = False
    t1_pnl          = 0.0
    t1_exit_ts      = None
    trail_sl        = init_sl
    exit_price      = None
    exit_reason     = "OPEN"
    exit_ts         = None
    trap_idx        = 0
    floor_locked    = False
    locked_floor_rs = 0.0

    for _, row in future_scan.iterrows():
        bar_ts    = row["datetime"]
        if not isinstance(bar_ts, pd.Timestamp):
            bar_ts = pd.Timestamp(bar_ts)
        bar_high  = float(row["high"])
        bar_low   = float(row["low"])
        bar_close = float(row["close"])
        # Exec price at this bar timestamp (for P&L calcs)
        exec_close = _price_at_ts(exec_bars, bar_ts) or bar_close

        # Force exit: opposite side signal fired — close at this bar's exec price
        if force_exit_ts is not None and bar_ts >= force_exit_ts:
            exit_price  = round(exec_close, 2)
            exit_reason = "OPP_SIGNAL"
            exit_ts     = bar_ts
            break

        if no_target_tsl:
            # ── No-Target-TSL mode: SL / OPP_SIGNAL / Floor / EOD only ──────
            # Floor locks directly from total P&L (no T1 prerequisite)
            if profit_floor_per_lot > 0:
                current_pnl = (exec_close - entry_price) * total_qty
                if not floor_locked and current_pnl >= profit_floor_per_lot:
                    floor_locked    = True
                    locked_floor_rs = profit_floor_per_lot
                if floor_locked and current_pnl < locked_floor_rs:
                    exit_price  = round(entry_price + locked_floor_rs / total_qty, 2)
                    exit_reason = "FLOOR_SL"
                    exit_ts     = bar_ts
                    break
        else:
            # 1. T1 trigger from scan bar (high crosses zone_high)
            if not t1_hit and bar_high >= t1_price:
                t1_hit       = True
                t1_exit_ts   = bar_ts
                exec_t1_px   = _price_at_ts(exec_bars, bar_ts) or exec_close
                t1_pnl       = round((exec_t1_px - entry_price) * t1_qty, 2)
                trail_sl     = init_sl   # TSL resets to zone_low (breakeven anchor)

            # Profit cap: triggered on exec_close P&L
            if t1_hit and profit_cap_per_lot > 0:
                running_rem = (exec_close - entry_price) * rem_qty
                if t1_pnl + running_rem >= profit_cap_per_lot:
                    rem_needed  = profit_cap_per_lot - t1_pnl
                    exit_price  = round(entry_price + rem_needed / rem_qty, 2)
                    exit_reason = "PROFIT_CAP"
                    exit_ts     = bar_ts
                    break

            # Profit floor: lock ₹floor after T1; exit if P&L drops below
            if t1_hit and profit_floor_per_lot > 0:
                running_rem = (exec_close - entry_price) * rem_qty
                current_pnl = t1_pnl + running_rem
                if not floor_locked and current_pnl >= profit_floor_per_lot:
                    floor_locked    = True
                    locked_floor_rs = profit_floor_per_lot
                if floor_locked and current_pnl < locked_floor_rs:
                    floor_rem_needed = locked_floor_rs - t1_pnl
                    exit_price  = round(entry_price + floor_rem_needed / rem_qty, 2)
                    exit_reason = "FLOOR_SL"
                    exit_ts     = bar_ts
                    break

            # 2. Ratchet TSL: scan-zone 5-min traps raise TSL
            if t1_hit:
                while trap_idx < len(trap_events):
                    ev_ts, z_low = trap_events[trap_idx]
                    if bar_ts < ev_ts:
                        break
                    if z_low > trail_sl:
                        trail_sl = z_low
                    trap_idx += 1

            # 3. T2: scan bar high crosses next zone → exit at exec price
            if t1_hit and t2_price and bar_high >= t2_price:
                exit_price  = _price_at_ts(exec_bars, bar_ts) or exec_close
                exit_reason = "T2"
                exit_ts     = bar_ts
                break

        # SL trigger: intrabar (bar_low crosses SL level) → exit AT SL price, not close
        active_sl = trail_sl if (t1_hit and not no_target_tsl) else init_sl
        sl_trigger = round(active_sl - sl_buf, 2)
        if bar_low < sl_trigger:
            exit_price  = sl_trigger   # exit at the SL price itself
            exit_reason = "TRAIL_SL" if t1_hit else "SL"
            exit_ts     = bar_ts
            break

        # 5. EOD
        if bar_ts.time() >= pd.Timestamp("15:25").time():
            exit_price  = exec_close
            exit_reason = "EOD"
            exit_ts     = bar_ts
            break

    if exit_price is None:
        last        = future_scan.iloc[-1]
        last_ts     = last["datetime"]
        exit_price  = _price_at_ts(exec_bars, last_ts) or float(last["close"])
        exit_reason = "EOD"
        exit_ts     = last_ts

    if no_target_tsl:
        exit_qty  = total_qty   # always full position (no half-exit at T1)
        rem_pnl   = round((exit_price - entry_price) * exit_qty, 2)
        total_pnl = rem_pnl
    else:
        exit_qty  = rem_qty if t1_hit else total_qty
        rem_pnl   = round((exit_price - entry_price) * exit_qty, 2)
        total_pnl = round(t1_pnl + rem_pnl, 2)
    capital_rs  = int(round(entry_price * total_qty, 0))   # premium paid to hold position
    roi_pct     = round(total_pnl / capital_rs * 100, 1) if capital_rs > 0 else 0.0

    return {
        "date":          trade_date,
        "opt_type":      opt_type,
        "strike":        strike,
        "spot_at_entry": round(spot_at_entry, 1),
        "trap_pos":      e.get("_trap_pos", ""),
        "mode":          e.get("_mode", ""),
        "entry":         round(entry_price, 2),
        "t1":            round(t1_price, 2),
        "t2":            round(t2_price, 2) if t2_price else None,
        "sl":            round(init_sl - sl_buf, 2),   # display the real exit level
        "entry_ts":      str(trap_ts)[:16],
        "t1_ts":         str(t1_exit_ts)[:16] if t1_exit_ts else "",
        "exit":          round(exit_price, 2),
        "exit_ts":       str(exit_ts)[:16] if exit_ts is not None else "",
        "reason":        exit_reason,
        "t1_hit":        t1_hit,
        "t1_pnl":        t1_pnl,
        "rem_pnl":       rem_pnl,
        "pnl_pts":       round(total_pnl / total_qty, 2) if total_qty else 0,
        "pnl_rs":        int(total_pnl),
        "capital_rs":    capital_rs,
        "roi_pct":       roi_pct,
        "zone_low":      round(zl, 2),
        "zone_high":     round(zh, 2),
        "zone":          f"{zl:.0f}-{zh:.0f}",
        "kind":          e.get("kind", "BEAR"),
    }


# ── Per-day backtest ───────────────────────────────────────────────────────────
def _run_day(index: str, cfg: dict, trade_date: str,
             df_spot_all: pd.DataFrame,
             use_bias: bool, sl_buf: float,
             opt_bar_cache: dict | None = None,
             strike_depth: str = "both",
             profit_cap_per_lot: float = 0.0,
             use_1itm: bool = False,
             profit_floor_per_lot: float = 0.0,
             no_target_tsl: bool = False,
             rr_filter: bool = False,
             rr_min_ratio: float = 1.0,
             use_high_breakout: bool = True,
             skip_open_spike: bool = True,
             open_spike_min: int = 30,
             pure_intraday: bool = False,
             max_ltf_index: int = 0,
             zone_scan_cache: dict | None = None) -> list[dict]:
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

    # Strike selection — mirrors live scanner CE1/CE2/PE1/PE2
    trade_dt_obj = td
    if gap_fired:
        atm        = _round_strike(today_open, step)
        ce_near    = atm - cfg["gap_near"]         # CE1 = ATM-200
        ce_far     = atm - cfg.get("gap_far", cfg["gap_near"] * 2)  # CE2 = ATM-400
        pe_near    = atm + cfg["gap_near"]         # PE1 = ATM+200
        pe_far     = atm + cfg.get("gap_far", cfg["gap_near"] * 2)  # PE2 = ATM+400
        base_mode  = f"GAP {gap_dir} {gap_pct:.1f}%"
        all_legs = [
            ("CE", ce_near, _option_key(index, ce_near, "CE", trade_dt_obj), "NEAR"),
            ("CE", ce_far,  _option_key(index, ce_far,  "CE", trade_dt_obj), "FAR"),
            ("PE", pe_near, _option_key(index, pe_near, "PE", trade_dt_obj), "NEAR"),
            ("PE", pe_far,  _option_key(index, pe_far,  "PE", trade_dt_obj), "FAR"),
        ]
        if strike_depth == "near":
            legs = [l for l in all_legs if l[3] == "NEAR"]
        elif strike_depth == "far":
            legs = [l for l in all_legs if l[3] == "FAR"]
        else:
            legs = all_legs
    else:
        ce_strike  = _round_strike(piv["S1"], step)
        pe_strike  = _round_strike(piv["R1"], step)
        base_mode  = f"NOGAP pivot P={piv['P']:.0f} S1={piv['S1']:.0f} R1={piv['R1']:.0f}"
        legs = [
            ("CE", ce_strike, _option_key(index, ce_strike, "CE", trade_dt_obj), "S1"),
            ("PE", pe_strike, _option_key(index, pe_strike, "PE", trade_dt_obj), "R1"),
        ]

    # Pure intraday mode: override strike selection to ATM±offset (both sides, no pivot/gap bias)
    if pure_intraday:
        atm       = _round_strike(today_open, step)
        ce_near   = atm - cfg["gap_near"]
        ce_far    = atm - cfg.get("gap_far", cfg["gap_near"] * 2)
        pe_near   = atm + cfg["gap_near"]
        pe_far    = atm + cfg.get("gap_far", cfg["gap_near"] * 2)
        base_mode = f"INTRADAY ATM={atm}"
        all_legs  = [
            ("CE", ce_near, _option_key(index, ce_near, "CE", trade_dt_obj), "NEAR"),
            ("CE", ce_far,  _option_key(index, ce_far,  "CE", trade_dt_obj), "FAR"),
            ("PE", pe_near, _option_key(index, pe_near, "PE", trade_dt_obj), "NEAR"),
            ("PE", pe_far,  _option_key(index, pe_far,  "PE", trade_dt_obj), "FAR"),
        ]
        if strike_depth == "near":
            legs = [l for l in all_legs if l[3] == "NEAR"]
        elif strike_depth == "far":
            legs = [l for l in all_legs if l[3] == "FAR"]
        else:
            legs = all_legs

    fetch_from = (td - timedelta(days=14)).isoformat()
    fetch_to   = (td + timedelta(days=1)).isoformat()
    cache  = opt_bar_cache if opt_bar_cache is not None else {}
    zcache = zone_scan_cache if zone_scan_cache is not None else {}

    trades = []
    # One-trade-at-a-time: persists across CE and PE legs
    # {exit_ts, opt_type, trade_idx} — trade_idx points to trades[] for re-simulation
    day_running_trade: dict | None = None

    # ── PASS 1: collect signals + bars for all legs (no simulation yet) ──────
    # This allows cross-leg R:R check (CE reward = PE LTP - PE zone_trigger)
    leg_coll: dict[tuple, dict] = {}   # (opt_type, strike, depth) → data

    for opt_type, strike, key, depth in legs:
        # Bias filter: on gap days, opposing leg scanned for EXIT ONLY (not entry)
        # Gap UP → CE = entry; PE = exit tracker only (close CE if PE trap fires)
        # Gap DOWN → PE = entry; CE = exit tracker only
        exit_only = False
        if use_bias and gap_fired and not pure_intraday:
            if gap_dir == "UP" and opt_type == "PE":
                exit_only = True   # scan PE but only for closing CE
            if gap_dir == "DOWN" and opt_type == "CE":
                exit_only = True   # scan CE but only for closing PE
        mode = f"{base_mode} {depth}"

        if not key:
            print(f"  {trade_date} {opt_type} {strike}: no instrument key — skip")
            continue

        # Use cached bars if available; otherwise fetch and cache
        if key in cache:
            df_opt_raw = cache[key]
        else:
            try:
                df_opt_raw = _fetch_1m(key, fetch_from, fetch_to)
                time.sleep(0.2)
                cache[key] = df_opt_raw
            except Exception as exc:
                print(f"  {trade_date} {opt_type} {strike}: fetch error {exc}")
                cache[key] = pd.DataFrame()
                continue

        if df_opt_raw.empty:
            print(f"  {trade_date} {opt_type} {strike}: no option data")
            continue

        _today_ck = (td, key, "opt_today")
        if _today_ck in zcache:
            df_opt_today = zcache[_today_ck]
            df_opt_all   = None  # not needed when cache hit (HTF path uses df_opt_all for htf_min>=60)
        else:
            df_opt_all   = _mkt_hours(df_opt_raw)
            df_opt_today = df_opt_all[df_opt_all["datetime"].dt.date == td].copy()
            zcache[_today_ck] = df_opt_today

        if df_opt_today.empty:
            print(f"  {trade_date} {opt_type} {strike}: no today option bars")
            continue

        # Junk day guard: far-dated contracts may have zero/near-zero volume in early months
        if _is_junk_day(df_opt_today):
            print(f"  {trade_date} {opt_type} {strike}: JUNK DAY — <10 active bars in first 60m, skip")
            continue

        # ── Step 1: HTF scan ────────────────────────────────────────────────
        # >= 60min: institutional memory → scan full prev-week + today history
        # <  60min: pure intraday concept → scan TODAY's bars only (no prev day reference)
        # (pure_intraday skips this entirely — htf_zones always set to [] below)
        if not pure_intraday:
            if df_opt_all is None:
                df_opt_all = _mkt_hours(df_opt_raw)
            htf_source = df_opt_all if htf_min >= 60 else df_opt_today
            htf_bars = _resample(htf_source, htf_min)
            _, htf_entries = scanner.scan_htf(htf_bars) if len(htf_bars) >= 2 else (None, [])
        else:
            htf_entries = []

        def _closed_today(e):
            ts = e.get("closed_on")   # CLOSED = price returned to zone = entry ready
            if not ts:
                return False
            try:
                return pd.to_datetime(ts).date() == td
            except Exception:
                return False

        # pure_intraday: skip all HTF prev-day zones — only intraday cascade
        htf_zones = [] if pure_intraday else [
            e for e in htf_entries if e.get("status") == "CLOSED" and _closed_today(e)
        ]

        entry_signals = []   # list of (entry_ts, entry_price, sl, t1, zone_low, zone_high, mode_tag)

        if htf_zones:
            # For small HTF (< 60min): multiple nearby zones → pick the LOWEST zone_low
            if htf_min < 60 and len(htf_zones) > 1:
                htf_zones = [min(htf_zones, key=lambda z: float(z.get("zone_low", 9999)))]

            df_5 = _resample(df_opt_today, 5)

            for htf_z in htf_zones:
                zh = float(htf_z.get("zone_high", 0))
                zl = float(htf_z.get("zone_low",  0))
                trap_ts = pd.to_datetime(htf_z.get("trapped_on") or htf_z.get("ref_ts") or "NaT")
                if trap_ts is pd.NaT:
                    trap_ts = None
                if trap_ts is not None and getattr(trap_ts, 'tzinfo', None):
                    trap_ts = trap_ts.tz_localize(None)

                # Skip zones trapped during opening spike (e.g. first 30 min of session).
                # A spike to 780 at 09:20 creates a fake HTF zone — real traps need time to form.
                if skip_open_spike and trap_ts is not None:
                    _spike_end = pd.Timestamp(f"{td} 09:14:00") + pd.Timedelta(minutes=open_spike_min)
                    if trap_ts.date() == td and trap_ts <= _spike_end:
                        print(f"  {trade_date} SPIKE-SKIP {opt_type} {strike}: "
                              f"zone trapped at {trap_ts.time()} (within {open_spike_min}m of open) "
                              f"zone={zl:.0f}-{zh:.0f}")
                        continue

                # Scan ALL of today's 5min bars within the HTF zone for a fresh trap.
                _, ltf5_all = scanner.scan_htf(df_5) if len(df_5) >= 2 else (None, [])
                ltf5_in = [e for e in (ltf5_all or [])
                           if e.get("status") in ("TRAPPED", "CLOSED")
                           and float(e.get("zone_high", 0)) <= zh * 1.02
                           and float(e.get("zone_low",  0)) >= zl * 0.98]

                if ltf5_in:
                    # Take ALL valid 5min sub-traps inside HTF zone (more trade opportunities)
                    ltf5_in.sort(key=lambda e: float(e.get("zone_low", 9999)))
                    added = 0
                    for idx, best in enumerate(ltf5_in):
                        if max_ltf_index > 0 and idx + 1 > max_ltf_index:
                            break  # sorted ascending; all remaining have higher index
                        best["_mode"]     = f"{'INTRADAY' if htf_min < 60 else 'HTF'}-{htf_min}m→5m"
                        best["_trap_pos"] = f"LTF-{idx+1}"
                        best["_htf_t1"]   = zh
                        best["_htf_sl"]   = zl
                        entry_signals.append(best)
                        added += 1
                    print(f"  {trade_date} {opt_type} {strike}: HTF {zl:.0f}-{zh:.0f} → {added}/{len(ltf5_in)} 5m sub-trap(s)")
                else:
                    # No fresh 5min trap found inside HTF zone — skip (no trade).
                    # Do not enter blindly at HTF trigger without LTF confirmation.
                    print(f"  {trade_date} {opt_type} {strike}: HTF {zl:.0f}-{zh:.0f} → no 5m sub-trap — SKIP")

            print(f"  {trade_date} {opt_type} {strike} [{mode}]: {len(entry_signals)} HTF zone(s)")
        else:
            # ── Step 2: No HTF zone → 15min intraday cascade ──────────────────
            # pure_intraday: scan ALL 15m zones + 3m sub-zones (15-3-1 mode)
            # normal cascade: pick lowest 15m zone + 5m sub-zones
            _cas_ck = (td, key, "cas15")
            if _cas_ck in zcache:
                cas_zones = zcache[_cas_ck]
            else:
                df_15 = _resample(df_opt_today, 15)
                _, cas15 = scanner.scan_htf(df_15) if len(df_15) >= 2 else (None, [])
                cas_zones = sorted(
                    [e for e in (cas15 or []) if e.get("status") in ("TRAPPED", "CLOSED")],
                    key=lambda z: float(z.get("zone_low", 9999))
                )
                zcache[_cas_ck] = cas_zones

            if not cas_zones:
                print(f"  {trade_date} {opt_type} {strike}: no zones (HTF or 15m)")
                continue

            # Sub-zone timeframe: 3m for pure_intraday, 5m for normal cascade
            sub_min  = 3 if pure_intraday else ltf_min

            _df5_ck = (td, key, "df5")
            if _df5_ck in zcache:
                df_5 = zcache[_df5_ck]
            else:
                df_5 = _resample(df_opt_today, 5)
                zcache[_df5_ck] = df_5

            _ltf_ck = (td, key, sub_min, "ltf")
            if _ltf_ck in zcache:
                ltf_sub_all = zcache[_ltf_ck]
            else:
                df_sub = _resample(df_opt_today, sub_min) if sub_min != 5 else df_5
                _, ltf_sub_all = scanner.scan_htf(df_sub) if len(df_sub) >= 2 else (None, [])
                zcache[_ltf_ck] = ltf_sub_all

            # pure_intraday: all 15m zones (complete day); normal: only the lowest one
            zones_to_scan = cas_zones if pure_intraday else cas_zones[:1]
            mode_tag      = f"INTRADAY-15m→{sub_min}m" if pure_intraday else f"CASCADE-15m→{sub_min}m"

            for cz in zones_to_scan:
                zh = float(cz["zone_high"])
                zl = float(cz["zone_low"])

                ltf_in = [e for e in (ltf_sub_all or [])
                          if e.get("status") in ("TRAPPED", "CLOSED")
                          and float(e.get("zone_high", 0)) <= zh * 1.02
                          and float(e.get("zone_low",  0)) >= zl * 0.98]

                if ltf_in:
                    ltf_in.sort(key=lambda e: float(e.get("zone_low", 9999)))
                    added = 0
                    for idx, best in enumerate(ltf_in):
                        if max_ltf_index > 0 and idx + 1 > max_ltf_index:
                            break  # sorted ascending; all remaining have higher index
                        best["_mode"]     = mode_tag
                        best["_trap_pos"] = f"LTF-{idx+1}"
                        best["_htf_t1"]   = zh
                        best["_htf_sl"]   = zl
                        entry_signals.append(best)
                        added += 1
                    print(f"  {trade_date} {opt_type} {strike}: {mode_tag} {zl:.0f}-{zh:.0f} → {sub_min}m ×{added}/{len(ltf_in)}")
                else:
                    print(f"  {trade_date} {opt_type} {strike}: {mode_tag} {zl:.0f}-{zh:.0f} → no {sub_min}m sub-trap — SKIP")

        if not entry_signals:
            continue

        # Merge zones at the same price level (within 10 pts) — keep earliest
        entry_signals = _dedup_zones(entry_signals, price_tol=10.0)

        # Minimum zone width filter: T1 must be at least sl_buf pts above entry.
        min_zone_width = sl_buf
        entry_signals = [z for z in entry_signals
                         if (z.get("zone_high", 0) - z.get("zone_low", 0)) >= min_zone_width]

        if not entry_signals:
            continue

        # 5-min bars (may already exist from cascade path)
        if "df_5" not in dir():
            df_5 = _resample(df_opt_today, 5)

        # Store for pass 2
        leg_coll[(opt_type, strike, depth)] = {
            "signals":    entry_signals,
            "df_today":   df_opt_today,
            "df_5":       df_5,
            "exit_only":  exit_only,
            "mode":       mode,
        }

    # ── PASS 2: R:R filter + simulate ────────────────────────────────────────

    # Sort signals by trap timestamp for chronological processing
    def _sig_ts(z):
        t = pd.to_datetime(z.get("closed_on") or z.get("trapped_on") or "NaT")
        return t.tz_localize(None) if (t is not pd.NaT and t.tzinfo) else t

    for (opt_type, strike, depth), ld in leg_coll.items():
        entry_signals = ld["signals"]
        df_opt_today  = ld["df_today"]
        df_5          = ld["df_5"]
        exit_only     = ld["exit_only"]
        mode          = ld["mode"]

        # Opposing leg data for R:R filter
        opp_type = "PE" if opt_type == "CE" else "CE"
        opp_ld   = next((v for (ot, s, d), v in leg_coll.items() if ot == opp_type), None)
        opp_df   = opp_ld["df_today"] if opp_ld else None
        opp_sigs = opp_ld["signals"]  if opp_ld else []
        # Lowest zone_low across all opposing signals = most conservative zone_trigger
        opp_zone_trigger = (min(float(s.get("zone_low", 9999)) for s in opp_sigs)
                            if opp_sigs else None)

        # 1 ITM exec bars (per signal, resolved below)
        exec_strike   = strike
        df_exec_today = df_opt_today
        df_exec_5m    = df_5
        _1itm_exec_bars: dict[int, pd.DataFrame] = {}

        entry_signals.sort(key=_sig_ts)

        for z in entry_signals:
            trap_ts = pd.to_datetime(z.get("closed_on") or z.get("trapped_on"))
            spot_val = 0.0
            if trap_ts is not pd.NaT:
                ts_naive = trap_ts.tz_localize(None) if trap_ts.tzinfo else trap_ts
                spot_val = _spot_at_ts(df_today, ts_naive)

            z_ts = _sig_ts(z)

            # One-trade-at-a-time: check day_running_trade (spans CE+PE legs)
            force_exit_ts_arg = None
            if day_running_trade is not None:
                rt = day_running_trade
                if z_ts is pd.NaT or z_ts >= rt["exit_ts"]:
                    day_running_trade = None   # previous trade already closed
                elif rt["opt_type"] == opt_type:
                    continue   # SAME side still running → skip
                else:
                    # OPPOSITE side fired while trade running → force-close running trade
                    # (applies even if this leg is exit_only — that's exactly what exit_only is for)
                    # Re-simulate the running trade with forced exit at z_ts
                    prev_result = trades[rt["trade_idx"]]
                    prev_z      = rt["z"]
                    re_result = _simulate_exit(
                        prev_z,
                        rt["df_exec"], rt["df_exec_5m"],
                        lot, sl_buf, rt["opt_type"], trade_date,
                        strike=rt["exec_strike"], spot_at_entry=rt["spot_val"],
                        profit_cap_per_lot=profit_cap_per_lot,
                        profit_floor_per_lot=profit_floor_per_lot,
                        df1m_scan=rt["scan_bars_arg"],
                        force_exit_ts=z_ts,
                        no_target_tsl=no_target_tsl,
                        use_high_breakout=use_high_breakout,
                    )
                    if re_result:
                        re_result["index"]       = index
                        re_result["gap_pct"]     = round(gap_pct, 2)
                        re_result["gap_fired"]   = gap_fired
                        re_result["depth"]       = rt["depth"]
                        re_result["mode"]        = re_result["mode"] + f" {rt['mode_tag']}"
                        re_result["scan_strike"] = rt["scan_strike"]
                        trades[rt["trade_idx"]]  = re_result   # replace with forced-exit version
                    day_running_trade = None   # now open opposite side

            # 1 ITM: resolve exec strike from LIVE spot at entry time
            # CE → live ATM − step (1 step ITM), PE → live ATM + step
            # SL/T1 triggers remain on scan strike (zone levels intact).
            # Only entry/exit prices are read from exec strike bars.
            exec_strike   = strike
            df_exec_today = df_opt_today
            df_exec_5m    = df_5
            if use_1itm and spot_val > 0:
                live_atm    = _round_strike(spot_val, step)
                z_exec      = live_atm - step if opt_type == "CE" else live_atm + step
                if z_exec != strike:
                    if z_exec not in _1itm_exec_bars:
                        exec_key = _option_key(index, z_exec, opt_type, td)
                        if exec_key not in cache:
                            try:
                                cache[exec_key] = _fetch_1m(exec_key, fetch_from, fetch_to)
                                time.sleep(0.2)
                            except Exception as exc:
                                print(f"  1ITM exec fetch {opt_type}{z_exec}: {exc}")
                                cache[exec_key] = pd.DataFrame()
                        raw = cache.get(exec_key, pd.DataFrame())
                        if not raw.empty:
                            _1itm_exec_bars[z_exec] = _mkt_hours(raw)[
                                _mkt_hours(raw)["datetime"].dt.date == td].copy()
                        else:
                            _1itm_exec_bars[z_exec] = pd.DataFrame()
                    df_exec = _1itm_exec_bars.get(z_exec, pd.DataFrame())
                    if not df_exec.empty:
                        exec_strike   = z_exec
                        df_exec_today = df_exec
                        df_exec_5m    = _resample(df_exec, 5)

            # exit_only leg: only used to close running opposite trade, never opens new entry
            if exit_only:
                continue

            # ── R:R filter ────────────────────────────────────────────────────
            # Logic: find the historical timestamp when opposing leg was at its
            # zone_low (entry point for opp bears). At that same timestamp, get
            # current leg's price → that is the current leg's TARGET (market
            # returning to that level = opp OPP_SIGNAL territory again).
            # Reward = target - entry_est.  Skip if reward < risk (< 1:1).
            if rr_filter and opp_zone_trigger is not None and opp_df is not None and not opp_df.empty:
                entry_est = float(z.get("zone_low", 0))
                my_sl     = float(z.get("_htf_sl", 0)) - sl_buf
                my_risk   = entry_est - my_sl
                if my_risk > 0:
                    # Find bar in opp bars where close was closest to opp zone_low
                    dists     = (opp_df["close"] - opp_zone_trigger).abs()
                    hist_ts   = opp_df.loc[dists.idxmin(), "datetime"]
                    # Get current-leg price at that historical timestamp
                    mask_hist = df_opt_today["datetime"] <= hist_ts
                    if mask_hist.any():
                        ce_at_hist = float(df_opt_today[mask_hist].iloc[-1]["close"])
                        ce_reward  = ce_at_hist - entry_est
                        if ce_reward < my_risk * rr_min_ratio:
                            print(f"  R:R SKIP {opt_type} {strike}: target={ce_at_hist:.1f} entry={entry_est:.1f} reward={ce_reward:.1f} < {rr_min_ratio}x risk={my_risk:.1f}")
                            continue

            # scan bars drive SL/T1 timing; exec bars (ATM-50) provide entry/exit prices
            scan_bars_arg = (df_opt_today
                             if (use_1itm and exec_strike != strike)
                             else None)
            result = _simulate_exit(
                z, df_exec_today, df_exec_5m,
                lot, sl_buf, opt_type, trade_date,
                strike=exec_strike, spot_at_entry=spot_val,
                profit_cap_per_lot=profit_cap_per_lot,
                profit_floor_per_lot=profit_floor_per_lot,
                df1m_scan=scan_bars_arg,
                no_target_tsl=no_target_tsl,
                use_high_breakout=use_high_breakout,
            )
            if result:
                result["index"]        = index
                result["gap_pct"]      = round(gap_pct, 2)
                result["gap_fired"]    = gap_fired
                result["depth"]        = depth
                result["mode"]        += f" {mode}"
                result["scan_strike"]  = strike   # the detection strike
                if use_1itm and exec_strike != strike:
                    result["exec_mode"] = "1ITM"
                trades.append(result)
                # Track for one-at-a-time + opposite-side force-close
                exit_ts_raw = result.get("exit_ts")
                if exit_ts_raw:
                    exit_ts_pd = pd.to_datetime(exit_ts_raw)
                    if exit_ts_pd is not pd.NaT:
                        if exit_ts_pd.tzinfo:
                            exit_ts_pd = exit_ts_pd.tz_localize(None)
                        day_running_trade = {
                            "exit_ts":     exit_ts_pd,
                            "opt_type":    opt_type,
                            "trade_idx":   len(trades) - 1,
                            "z":           z,
                            "df_exec":     df_exec_today,
                            "df_exec_5m":  df_exec_5m,
                            "exec_strike": exec_strike,
                            "spot_val":    spot_val,
                            "scan_bars_arg": scan_bars_arg,
                            "depth":       depth,
                            "mode_tag":    mode,
                            "scan_strike": strike,
                        }

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
                       use_bias: bool = True, sl_buf: float = 10.0,
                       monthly: bool = False,
                       strike_depth: str = "both",
                       profit_cap_per_lot: float = 0.0,
                       use_1itm: bool = False,
                       profit_floor_per_lot: float = 0.0,
                       htf_min: int = 0,
                       no_target_tsl: bool = False,
                       rr_filter: bool = False,
                       rr_min_ratio: float = 1.0,
                       next_week: bool = False,
                       use_high_breakout: bool = True,
                       skip_open_spike: bool = True,
                       open_spike_min: int = 30,
                       pure_intraday: bool = False,
                       max_ltf_index: int = 0,
                       fixed_expiry: str = "") -> dict:
    """fixed_expiry: e.g. '31JUL26' — pins ALL option lookups to this one contract
    regardless of the trade date. Used for expiry-comparison backtest (Apr-Jun on July contracts).
    When set, `monthly` / `next_week` are ignored. REGISTRY is also bypassed for key lookup.
    For BANKNIFTY/FINNIFTY/MIDCPNIFTY (monthly only): leave empty — REGISTRY returns
    the active June monthly automatically since May has expired."""
    global _HEADERS, _USE_MONTHLY, _USE_NEXT_WEEK, _FIXED_EXPIRY
    _HEADERS      = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    _USE_MONTHLY  = monthly
    _USE_NEXT_WEEK = next_week
    _FIXED_EXPIRY  = fixed_expiry.strip().upper()

    cfg = dict(INDEX_CFG.get(index.upper(), {}))
    if not cfg:
        return {"ok": False, "error": f"Unknown index {index}"}
    if htf_min > 0:
        cfg["htf_min"] = htf_min  # UI override

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

    expiry_label = "MONTHLY" if monthly else ("NEXT_WEEK" if next_week else "WEEKLY")
    days = _trading_days(s_date, e_date)
    print(f"\n{index} backtest  {s_date} to {e_date}  ({len(days)} days)  "
          f"expiry={expiry_label}  bias={'ON' if use_bias else 'OFF'}  sl_buf={sl_buf}")

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

    # Shared option bar cache.
    # For monthly mode: same contract key covers the whole period → pre-note the
    # full range so each key is fetched ONCE with complete history.
    # _run_day will use fetch_from=td-14d which may miss early bars on later days;
    # pre-seeding with the full range fixes that and eliminates duplicate fetches.
    opt_bar_cache: dict = {}
    if monthly:
        print("Monthly mode: option bars will be cached per strike key (fetch once).")

    all_trades: list[dict] = []
    for td in days:
        day_trades = _run_day(index, cfg, td, df_spot_all, use_bias, sl_buf,
                              opt_bar_cache, strike_depth, profit_cap_per_lot, use_1itm,
                              profit_floor_per_lot, no_target_tsl, rr_filter, rr_min_ratio,
                              use_high_breakout=use_high_breakout,
                              skip_open_spike=skip_open_spike,
                              open_spike_min=open_spike_min,
                              pure_intraday=pure_intraday,
                              max_ltf_index=max_ltf_index)
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

    print(f"\n{'─'*100}")
    print(f"{index}  {s_date} to {e_date}  Trades={len(all_trades)}  "
          f"Win={summary['win_pct']:.1f}%  Rs {total:+,.0f}  PF={pf}")
    print(f"{'─'*100}")
    print(f"  {'DATE':<10}  {'OPT':<3}  {'STRIKE':>6}  {'SPOT':>7}  "
          f"{'POS':<6}  {'MODE':<24}  {'TRAP@':5}  "
          f"{'ENTRY':>6}  {'T1':>6}  {'EXIT':>6}  {'REASON':<10}  {'T1?':3}  {'P&L Rs':>9}")
    print(f"  {'─'*98}")
    for t in all_trades:
        t1_flag = "Y" if t["t1_hit"] else "N"
        spot_s = f"{t['spot_at_entry']:.0f}" if t.get("spot_at_entry", 0) > 0 else "-"
        print(f"  {t['date']}  {t['opt_type']:<3}  "
              f"{t['strike']:>6}  {spot_s:>7}  "
              f"{t.get('trap_pos',''):<6}  {t['mode'][:24]:<24}  "
              f"{t['entry_ts'][11:16]:5}  "
              f"{t['entry']:>6.1f}  {t['t1']:>6.1f}  {t['exit']:>6.1f}  "
              f"{t['reason']:<10}  {t1_flag:<3}  Rs {t['pnl_rs']:>+8,.0f}")

    return {
        "ok": True,
        "summary": summary,
        "trades": all_trades,
        "equity": [{"date": d, "equity": v} for d, v in sorted(eq_map.items())],
    }


def run_nifty_backtest_optimize(
    token: str, index: str = "NIFTY",
    start: str = "", end: str = "",
    monthly: bool = True,
    htf_min: int = 0,
    use_high_breakout: bool = True,
    fixed_expiry: str = "",
) -> dict:
    """Sweep key backtest parameters and return all combinations ranked by Profit Factor.

    Fixed settings (not swept): pure_intraday=True, use_bias=False,
    skip_open_spike=True, use_high_breakout=use_high_breakout.

    Swept grid:
        max_ltf_index : [0, 5, 8, 10, 12, 15, 20]
        sl_buf        : [5, 8, 10, 12, 15]
        strike_depth  : ["near", "far", "both"]
        open_spike_min: [15, 30, 45]

    The option-bar cache is shared across all combinations so API calls happen
    only once (first time a strike key is seen).
    """
    global _HEADERS, _USE_MONTHLY, _FIXED_EXPIRY
    _HEADERS      = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    _USE_MONTHLY  = monthly
    _FIXED_EXPIRY = fixed_expiry.strip().upper()

    cfg = dict(INDEX_CFG.get(index.upper(), {}))
    if not cfg:
        return {"ok": False, "error": f"Unknown index {index}"}
    if htf_min > 0:
        cfg["htf_min"] = htf_min

    try:
        REGISTRY.load_sync(index.upper(), access_token=token)
    except Exception as exc:
        print(f"[REGISTRY] load failed ({exc}) — will use NSE symbol fallback")

    s_date = date.fromisoformat(start)
    e_date = date.fromisoformat(end)
    days   = _trading_days(s_date, e_date)

    # Fetch spot bars once
    spot_from  = (s_date - timedelta(days=14)).isoformat()
    spot_to    = (e_date + timedelta(days=1)).isoformat()
    df_spot_all = _fetch_1m(cfg["spot_key"], spot_from, spot_to)
    if df_spot_all.empty:
        return {"ok": False, "error": "No spot data"}
    df_spot_all = _mkt_hours(df_spot_all)

    # Shared caches — option bars fetched once, zone scans cached per (day, strike, tf)
    shared_bar_cache:  dict = {}
    shared_zone_cache: dict = {}

    # Per-index SL grids: scaled to typical ATM ITM premium range.
    # BANKNIFTY premiums are ~3-4× NIFTY → needs wider SL to survive normal noise.
    _SL_GRIDS = {
        "NIFTY":      [5, 8, 10, 15],
        "SENSEX":     [10, 15, 20, 30],
        "BANKNIFTY":  [15, 20, 25, 35],
        "FINNIFTY":   [5, 8, 10, 15],
        "MIDCPNIFTY": [5, 8, 10, 15],
    }
    max_ltf_grid = [0, 5, 8, 10, 15, 20]
    sl_buf_grid  = _SL_GRIDS.get(index.upper(), [5, 8, 10, 15])
    depth_grid   = ["near", "both"]

    total_combos = len(max_ltf_grid) * len(sl_buf_grid) * len(depth_grid)
    print(f"\n[OPTIMIZE] {index}  {s_date}→{e_date}  "
          f"{len(days)} days  {total_combos} combinations  monthly={monthly}")

    results = []
    combo_n = 0
    for sl_buf in sl_buf_grid:
        for depth in depth_grid:
            for max_ltf in max_ltf_grid:
                combo_n += 1
                trades: list[dict] = []
                for td in days:
                    day_trades = _run_day(
                        index, cfg, td, df_spot_all,
                        use_bias=False, sl_buf=sl_buf,
                        opt_bar_cache=shared_bar_cache,
                        strike_depth=depth,
                        profit_cap_per_lot=0.0,
                        use_1itm=False,
                        profit_floor_per_lot=0.0,
                        no_target_tsl=False,
                        rr_filter=False,
                        rr_min_ratio=1.0,
                        use_high_breakout=use_high_breakout,
                        skip_open_spike=True,
                        open_spike_min=30,
                        pure_intraday=True,
                        max_ltf_index=max_ltf,
                        zone_scan_cache=shared_zone_cache,
                    )
                    trades.extend(day_trades)

                n       = len(trades)
                wins    = [t for t in trades if t["pnl_rs"] > 0]
                losses  = [t for t in trades if t["pnl_rs"] <= 0]
                total   = sum(t["pnl_rs"] for t in trades)
                gw      = sum(t["pnl_rs"] for t in wins)
                gl      = abs(sum(t["pnl_rs"] for t in losses))
                pf      = round(gw / gl, 2) if gl > 0 else (99.0 if gw > 0 else 0.0)
                win_pct = round(100 * len(wins) / n, 1) if n else 0.0

                results.append({
                    "combo":         combo_n,
                    "sl_buf":        sl_buf,
                    "depth":         depth,
                    "spike_min":     30,
                    "max_ltf":       max_ltf,
                    "trades":        n,
                    "wins":          len(wins),
                    "losses":        len(losses),
                    "win_pct":       win_pct,
                    "total_rs":      int(total),
                    "profit_factor": pf,
                    "avg_win":       round(gw / len(wins), 0) if wins else 0,
                    "avg_loss":      round(-gl / len(losses), 0) if losses else 0,
                })
                if combo_n % 12 == 0 or combo_n == total_combos:
                    print(f"  [{combo_n}/{total_combos}] bar_cache={len(shared_bar_cache)} zone_cache={len(shared_zone_cache)}")

    # Rank: primary = Profit Factor (desc), secondary = total_rs (desc)
    results.sort(key=lambda r: (r["profit_factor"], r["total_rs"]), reverse=True)

    print(f"\n[OPTIMIZE] done — top combo: {results[0]}")
    return {"ok": True, "results": results, "total": len(results)}


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="NIFTY / SENSEX options backtest — pure intraday cascade mode",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    # Auth
    ap.add_argument("--token",   required=True, help="Upstox access token")
    # Index
    ap.add_argument("--index",   default="NIFTY", choices=["NIFTY", "SENSEX"],
                    help="Index to backtest (default: NIFTY)")
    # Date range
    ap.add_argument("--start",   default="", help="Start date YYYY-MM-DD (default: today-weeks*7)")
    ap.add_argument("--end",     default="", help="End date YYYY-MM-DD   (default: today)")
    ap.add_argument("--weeks",   type=int, default=4, help="Rolling weeks if --start/--end not given")
    # Expiry
    ap.add_argument("--monthly", action="store_true", default=True,
                    help="Monthly expiry (default ON — always use for this strategy)")
    ap.add_argument("--weekly",  action="store_true",
                    help="Override to weekly expiry")
    # Entry / zone params
    ap.add_argument("--sl-buf",        type=float, default=None,
                    help="SL buffer pts below zone_low  [NIFTY default: 5  SENSEX default: 20]")
    ap.add_argument("--strike-depth",  default="near", choices=["near", "far", "both"],
                    help="Strike depth: near=ATM±1ITM, both=near+far  (default: near)")
    ap.add_argument("--max-ltf",       type=int, default=None,
                    help="Max LTF sub-zone index  [NIFTY default: 10  SENSEX default: 8]")
    ap.add_argument("--pure-intraday", action="store_true", default=True,
                    help="Pure intraday cascade 15m→3m→1m (default ON)")
    ap.add_argument("--no-pure-intraday", dest="pure_intraday", action="store_false",
                    help="Disable pure intraday (use HTF prev-day zones)")
    ap.add_argument("--high-breakout", action="store_true", default=True,
                    help="1m HIGH breakout confirmation (default ON)")
    ap.add_argument("--no-high-breakout", dest="high_breakout", action="store_false",
                    help="Disable 1m HIGH breakout filter")
    # Bias / gap
    ap.add_argument("--no-bias", action="store_true",
                    help="Disable gap-bias filter (scan CE+PE both on gap days)")
    ap.add_argument("--gap-thresh", type=float, default=0.5,
                    help="Gap %% threshold to classify as gap day (default: 0.5)")
    ap.add_argument("--fixed-expiry", dest="fixed_expiry", default="",
                    help="Pin ALL option lookups to one contract (e.g. 31JUL26). "
                         "Use for Apr-Jun backtest on July contracts. "
                         "Overrides --monthly/--weekly. Leave empty for auto (REGISTRY).")
    args = ap.parse_args()

    # Index-specific defaults for params not explicitly passed
    _DEFAULTS = {
        "NIFTY":      {"sl_buf": 5.0,  "max_ltf": 10},
        "SENSEX":     {"sl_buf": 20.0, "max_ltf": 8},
        "BANKNIFTY":  {"sl_buf": 20.0, "max_ltf": 10},
        "FINNIFTY":   {"sl_buf": 8.0,  "max_ltf": 10},
        "MIDCPNIFTY": {"sl_buf": 8.0,  "max_ltf": 10},
    }
    _def = _DEFAULTS.get(args.index.upper(), _DEFAULTS["NIFTY"])
    sl_buf    = args.sl_buf  if args.sl_buf  is not None else _def["sl_buf"]
    max_ltf   = args.max_ltf if args.max_ltf is not None else _def["max_ltf"]
    use_monthly = not args.weekly  # --weekly overrides default monthly=True

    fixed_exp = getattr(args, "fixed_expiry", "") or ""
    expiry_label = (f"FIXED:{fixed_exp}" if fixed_exp
                    else ("MONTHLY" if use_monthly else "WEEKLY"))

    print(f"\n{'='*60}")
    print(f"  {args.index} Backtest — Pure Intraday Cascade (15m→3m→1m)")
    print(f"{'='*60}")
    print(f"  Date       : {args.start or 'rolling'} → {args.end or 'today'}  (weeks={args.weeks})")
    print(f"  Expiry     : {expiry_label}")
    print(f"  Depth      : {args.strike_depth.upper()}")
    print(f"  SL Buffer  : {sl_buf} pts")
    print(f"  Max LTF    : {max_ltf}  (sub-zones LTF-{max_ltf}+ filtered)")
    print(f"  High BrkOut: {'ON' if args.high_breakout else 'OFF'}")
    print(f"  Gap Bias   : {'OFF' if args.no_bias else 'ON'}")
    print(f"{'='*60}\n")

    result = run_nifty_backtest(
        token             = args.token,
        index             = args.index,
        weeks             = args.weeks,
        start             = args.start,
        end               = args.end,
        use_bias          = not args.no_bias,
        sl_buf            = sl_buf,
        monthly           = use_monthly,
        strike_depth      = args.strike_depth,
        use_high_breakout = args.high_breakout,
        skip_open_spike   = True,
        open_spike_min    = 30,
        pure_intraday     = args.pure_intraday,
        max_ltf_index     = max_ltf,
        fixed_expiry      = fixed_exp,
    )
    if not result["ok"]:
        print(f"ERROR: {result['error']}")
        sys.exit(1)

    trades = result.get("trades", [])
    wins   = [t for t in trades if t["pnl_rs"] > 0]
    losses = [t for t in trades if t["pnl_rs"] <= 0]
    total  = sum(t["pnl_rs"] for t in trades)
    gw     = sum(t["pnl_rs"] for t in wins)
    gl     = abs(sum(t["pnl_rs"] for t in losses))
    pf     = round(gw / gl, 2) if gl > 0 else (99.0 if gw > 0 else 0.0)

    print(f"\n{'='*60}")
    print(f"  RESULTS — {len(trades)} trades")
    print(f"{'='*60}")
    print(f"  Win Rate     : {len(wins)}/{len(trades)}  ({round(100*len(wins)/len(trades),1) if trades else 0}%)")
    print(f"  Total P&L    : ₹{total:+,.0f}")
    print(f"  Profit Factor: {pf}")
    print(f"  Avg Win      : ₹{round(gw/len(wins),0) if wins else 0:,.0f}")
    print(f"  Avg Loss     : ₹{-round(gl/len(losses),0) if losses else 0:,.0f}")
    print(f"\n  {'Date':<12} {'Opt':<4} {'Strike':<8} {'LTF':<8} {'Entry':>7} {'Exit':>7} {'Reason':<12} {'P&L':>9}")
    print(f"  {'-'*75}")
    for t in trades:
        print(f"  {t['date']:<12} {t['opt_type']:<4} {t['strike']:<8} "
              f"{t.get('trap_pos',''):<8} {t['entry']:>7.1f} {t['exit']:>7.1f} "
              f"{t['reason']:<12} ₹{t['pnl_rs']:>+8,}")
    print(f"{'='*60}\n")
