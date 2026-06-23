#!/usr/bin/env python3
"""
scripts/nifty_scalein_backtest.py - NIFTY Scale-In Entry Backtest

Entry logic:
  Lot 1 : HTF 75-min zone TRAPPED -> enter IMMEDIATELY on first 1-min bar
          that closes inside zone (no LTF 5-min wait)
  Lot 2 : CE drops to 1/3 of zone depth AND a fresh 5-min bearish trap
          forms at that level -> scale in (add 1 more lot)
          SL (both lots) = zone_low - SL_BUF (default 20 pts)

Exit logic:
  1 lot only (Lot 2 never fired):
    Full TSL - 5-min ratchet trap lows raise trail SL continuously
  2 lots (both entered):
    T1 = exit Lot 1 when price hits zone target (zone "sl" field, bears' stop)
    Remainder (Lot 2): 5-min ratchet TSL

  Hard SL = zone_low - SL_BUF (both modes)
  EOD squareoff = 15:20

Usage:
  python scripts/nifty_scalein_backtest.py --token TOKEN
  python scripts/nifty_scalein_backtest.py --token TOKEN --days 10 --expiry 2026-06-26
  python scripts/nifty_scalein_backtest.py --token TOKEN --start 2026-06-10 --end 2026-06-23 --expiry 2026-06-26
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time as _time
from datetime import date, timedelta
from typing import Optional
from urllib.parse import quote

import pandas as pd

sys.path.insert(0, ".")

from strategies.trap_scanner import scanner
from data_layer.instrument_registry import REGISTRY

# -- Module logger (file + console, configured per-run) -----------------------
_log: logging.Logger = logging.getLogger("scalein_backtest")
_log.setLevel(logging.DEBUG)
if not _log.handlers:
    _log.addHandler(logging.NullHandler())   # silent until _setup_log() wires the file


def _setup_log(log_path: str) -> None:
    """Wire a FileHandler + StreamHandler to _log for this backtest run."""
    for h in list(_log.handlers):
        _log.removeHandler(h)
    fmt = logging.Formatter("%(asctime)s  %(message)s", datefmt="%H:%M:%S")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    _log.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    _log.addHandler(ch)


# -- Shared config -------------------------------------------------------------
HTF_MIN  = 75
LOT      = 65           # NIFTY lot size
STEP     = 50           # strike step
SL_BUF   = 20.0         # fallback only — actual buffer = max(2, zone_height)
MIN_RR   = 1.5          # minimum reward:risk ratio (skip zone if below)
SQ_TIME  = pd.Timestamp("15:20").time()
IDX      = "NIFTY"
SPOT_KEY = "NSE_INDEX|Nifty 50"
IS_STOCK = False        # True for NSE stock F&O (monthly expiry, different lot/step)

_HEADERS: dict = {}

# -- NSE F&O stock config (lot size and strike step per stock) -----------------
# Lot sizes from SEBI/NSE; steps based on typical price ranges.
# Steps are approximate - adjust if your strike selection looks off.
_STOCK_CONFIG: dict = {
    "RELIANCE":    {"lot": 250,   "step": 50},
    "TCS":         {"lot": 150,   "step": 50},
    "INFY":        {"lot": 300,   "step": 20},
    "HDFCBANK":    {"lot": 550,   "step": 20},
    "ICICIBANK":   {"lot": 700,   "step": 20},
    "SBIN":        {"lot": 1500,  "step": 10},
    "WIPRO":       {"lot": 3000,  "step": 5},
    "BHARTIARTL":  {"lot": 950,   "step": 20},
    "HINDUNILVR":  {"lot": 300,   "step": 50},
    "BAJFINANCE":  {"lot": 125,   "step": 100},
    "KOTAKBANK":   {"lot": 400,   "step": 50},
    "AXISBANK":    {"lot": 1200,  "step": 20},
    "MARUTI":      {"lot": 25,    "step": 200},
    "TITAN":       {"lot": 375,   "step": 50},
    "SUNPHARMA":   {"lot": 700,   "step": 20},
    "HCLTECH":     {"lot": 700,   "step": 20},
    "ITC":         {"lot": 3200,  "step": 5},
    "M&M":         {"lot": 700,   "step": 50},
    "TATASTEEL":   {"lot": 7150,  "step": 5},
    "TATAMOTORS":  {"lot": 2750,  "step": 10},
    "ULTRACEMCO":  {"lot": 100,   "step": 200},
    "NTPC":        {"lot": 8000,  "step": 5},
    "TECHM":       {"lot": 600,   "step": 20},
    "ADANIPORTS":  {"lot": 1250,  "step": 20},
    "POWERGRID":   {"lot": 4700,  "step": 5},
    "ONGC":        {"lot": 3850,  "step": 5},
    "COALINDIA":   {"lot": 3000,  "step": 5},
    "JSWSTEEL":    {"lot": 2750,  "step": 10},
    "GRASIM":      {"lot": 475,   "step": 50},
    "ASIANPAINT":  {"lot": 200,   "step": 50},
    "LT":          {"lot": 450,   "step": 100},
    "BAJAJ-AUTO":  {"lot": 75,    "step": 200},
    "HEROMOTOCO":  {"lot": 300,   "step": 100},
    "DIVISLAB":    {"lot": 200,   "step": 100},
    "BPCL":        {"lot": 4800,  "step": 5},
    "CIPLA":       {"lot": 650,   "step": 20},
    "INDUSINDBANK":{"lot": 1000,  "step": 20},
    "DRREDDY":     {"lot": 125,   "step": 100},
    "BRITANNIA":   {"lot": 200,   "step": 100},
    "APOLLOHOSP":  {"lot": 125,   "step": 100},
    "PIDILITIND":  {"lot": 375,   "step": 50},
    "TATACONSUM":  {"lot": 3450,  "step": 10},
    "NESTLEIND":   {"lot": 800,   "step": 50},
    "EICHERMOT":   {"lot": 175,   "step": 100},
    "TRENT":       {"lot": 175,   "step": 50},
    "ZOMATO":      {"lot": 3300,  "step": 5},
    "ADANIENT":    {"lot": 125,   "step": 50},
    "VEDL":        {"lot": 4850,  "step": 5},
    "SAIL":        {"lot": 11200, "step": 2},
    "PNB":         {"lot": 16000, "step": 2},
    "BANKBARODA":  {"lot": 4700,  "step": 5},
    "CANBK":       {"lot": 6400,  "step": 5},
    "HAL":         {"lot": 200,   "step": 50},
    "BEL":         {"lot": 5700,  "step": 5},
    "BHEL":        {"lot": 4725,  "step": 2},
    "NMDC":        {"lot": 6450,  "step": 2},
    "IDFCFIRSTB":  {"lot": 13000, "step": 2},
    "YESBANK":     {"lot": 40000, "step": 1},
    "IRFC":        {"lot": 9800,  "step": 2},
    "IRCTC":       {"lot": 1875,  "step": 10},
    "DMART":       {"lot": 175,   "step": 50},
    "NYKAA":       {"lot": 4400,  "step": 5},
    "PAYTM":       {"lot": 1000,  "step": 10},
    "POLICYBZR":   {"lot": 850,   "step": 20},
    "DELHIVERY":   {"lot": 2100,  "step": 10},
    "MCDOWELL-N":  {"lot": 500,   "step": 20},
    "GODREJCP":    {"lot": 1000,  "step": 10},
    "MARICO":      {"lot": 1200,  "step": 10},
    "BERGEPAINT":  {"lot": 1100,  "step": 10},
    "COLPAL":      {"lot": 350,   "step": 50},
    "BIOCON":      {"lot": 2500,  "step": 5},
    "GLENMARK":    {"lot": 900,   "step": 10},
    "LUPIN":       {"lot": 850,   "step": 20},
    "AUROPHARMA":  {"lot": 1000,  "step": 20},
    "TORNTPHARM":  {"lot": 250,   "step": 50},
    "LICHSGFIN":   {"lot": 2000,  "step": 10},
    "RECLTD":      {"lot": 3000,  "step": 5},
    "PFC":         {"lot": 4200,  "step": 5},
    "HINDPETRO":   {"lot": 2900,  "step": 5},
    "IOC":         {"lot": 7500,  "step": 2},
    "GAIL":        {"lot": 5775,  "step": 2},
    "CONCOR":      {"lot": 600,   "step": 20},
    "IGL":         {"lot": 1375,  "step": 10},
    "MGL":         {"lot": 500,   "step": 20},
    "PETRONET":    {"lot": 3000,  "step": 5},
    "APOLLOTYRE":  {"lot": 3750,  "step": 5},
    "MRF":         {"lot": 10,    "step": 1000},
    "BALKRISIND":  {"lot": 300,   "step": 50},
    "TIINDIA":     {"lot": 250,   "step": 50},
    "EXIDEIND":    {"lot": 3000,  "step": 5},
    "DLF":         {"lot": 3300,  "step": 10},
    "GODREJPROP":  {"lot": 525,   "step": 20},
    "OBEROIRLTY":  {"lot": 400,   "step": 50},
    "PRESTIGE":    {"lot": 1250,  "step": 20},
    "ABBOTINDIA":  {"lot": 75,    "step": 200},
    "ALKEM":       {"lot": 175,   "step": 100},
    "PERSISTENT":  {"lot": 175,   "step": 100},
    "LTIM":        {"lot": 150,   "step": 100},
    "COFORGE":     {"lot": 125,   "step": 100},
    "MPHASIS":     {"lot": 225,   "step": 50},
    "OFSS":        {"lot": 50,    "step": 200},
    "KPITTECH":    {"lot": 1100,  "step": 10},
    "LTTS":        {"lot": 125,   "step": 100},
}


# -- Data fetch ----------------------------------------------------------------
def _fetch_1m_chunk(key: str, from_dt: str, to_dt: str) -> pd.DataFrame:
    import requests
    enc = quote(key, safe="")
    url = (f"https://api.upstox.com/v2/historical-candle/{enc}/1minute"
           f"/{to_dt}/{from_dt}")
    r = requests.get(url, headers=_HEADERS, timeout=30)
    if r.status_code == 400:
        return pd.DataFrame()   # contract not listed yet for that date range
    r.raise_for_status()
    candles = r.json().get("data", {}).get("candles", [])
    if not candles:
        return pd.DataFrame()
    rows = [{"datetime": pd.to_datetime(c[0]),
             "open": float(c[1]), "high": float(c[2]),
             "low":  float(c[3]), "close": float(c[4]),
             "volume": int(c[5])}
            for c in reversed(candles)]
    df = pd.DataFrame(rows)
    df["datetime"] = df["datetime"].dt.tz_localize(None)
    return df


def _fetch_1m(key: str, from_dt: str, to_dt: str) -> pd.DataFrame:
    f, t = date.fromisoformat(from_dt), date.fromisoformat(to_dt)
    chunks, cur = [], f
    while cur <= t:
        nxt = min(cur + timedelta(days=28), t)
        try:
            chunk = _fetch_1m_chunk(key, cur.isoformat(), nxt.isoformat())
            if not chunk.empty:
                chunks.append(chunk)
            _time.sleep(0.15)
        except Exception as exc:
            _log.info(f"  [fetch] {key} {cur}->{nxt}: {exc}")
        cur = nxt + timedelta(days=1)
    if not chunks:
        return pd.DataFrame()
    df = pd.concat(chunks, ignore_index=True).sort_values("datetime").drop_duplicates("datetime")
    return df.reset_index(drop=True)


def _mkt(df: pd.DataFrame) -> pd.DataFrame:
    return df[(df["datetime"].dt.time >= pd.Timestamp("09:15").time()) &
              (df["datetime"].dt.time <= pd.Timestamp("15:30").time())]


def _rs(df: pd.DataFrame, m: int) -> pd.DataFrame:
    if df.empty:
        return df
    htf = df.set_index("datetime").resample(f"{m}min").agg(
        {"open": "first", "high": "max", "low": "min",
         "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return htf


def _round(v: float, s: int) -> int:
    return int(round(v / s) * s)


def _pivot(H: float, L: float, C: float) -> dict:
    P = (H + L + C) / 3
    return {"S1": 2*P - H, "S2": P - (H - L),
            "R1": 2*P - L, "R2": P + (H - L)}


def _px(df1m: pd.DataFrame, ts: pd.Timestamp) -> float:
    row = df1m[df1m["datetime"] >= ts]
    return float(row["close"].iloc[0]) if not row.empty else float(df1m["close"].iloc[-1])


# -- Instrument key ------------------------------------------------------------
def _opt_key(expiry: date, strike: int, ot: str) -> str:
    if REGISTRY.is_loaded(IDX):
        k = REGISTRY.get_upstox_key(IDX, expiry, strike, ot)
        if k:
            return k
    # Compact symbol fallback (index only — stocks require master JSON key)
    if not IS_STOCK:
        _MC = {1:"1",2:"2",3:"3",4:"4",5:"5",6:"6",
               7:"7",8:"8",9:"9",10:"O",11:"N",12:"D"}
        yy = expiry.strftime("%y")
        mc = _MC[expiry.month]
        dd = expiry.strftime("%d")
        return f"NSE_FO|{IDX}{yy}{mc}{dd}{strike}{ot}"
    return ""  # stock without REGISTRY key — skip


def _current_expiry(trade_date: date) -> date:
    """Return the current active expiry >= trade_date (for monthly stock F&O)."""
    if REGISTRY.is_loaded(IDX):
        exp = REGISTRY.get_active_expiry(IDX, from_date=trade_date)
        if exp:
            return exp
    # Fallback: last Thursday of the month
    import calendar
    year, month = trade_date.year, trade_date.month
    last_day = calendar.monthrange(year, month)[1]
    d = date(year, month, last_day)
    while d.weekday() != 3:  # Thursday
        d -= timedelta(days=1)
    return d


def _next_week_expiry(trade_date: date) -> date:
    """Return the NEXT WEEK's real expiry for a given trade date via REGISTRY.

    Logic: get this week's active expiry, then get the expiry after that.
    REGISTRY handles weekly vs monthly format internally — no calendar math.
    Falls back to a simple +7d scan if REGISTRY unavailable.
    """
    if REGISTRY.is_loaded(IDX):
        this_week = REGISTRY.get_active_expiry(IDX, from_date=trade_date)
        if this_week:
            next_wk = REGISTRY.get_active_expiry(IDX, from_date=this_week + timedelta(days=1))
            if next_wk:
                return next_wk
    # Fallback: walk forward from trade_date + 7 days to find any real expiry
    all_exp = sorted(REGISTRY.all_expiries(IDX)) if REGISTRY.is_loaded(IDX) else []
    candidates = [e for e in all_exp if e >= trade_date + timedelta(days=7)]
    if candidates:
        return candidates[0]
    # Last resort: calendar Thursday +7
    d = trade_date + timedelta(days=7)
    while d.weekday() != 3:
        d += timedelta(days=1)
    return d


# -- Scale-in simulation -------------------------------------------------------
def _simulate(zone: dict, df1m: pd.DataFrame, df5m: pd.DataFrame,
              trade_date: str, strike: int, ot: str,
              spot_val: float, mode: str = "scalein") -> Optional[dict]:
    """
    Simulate one HTF TRAPPED zone.

    mode="scalein"    : Lot 1 on first bar inside zone; Lot 2 on 5-min sub-trap at 1/3 depth.
    mode="trapscanner": Single entry only AFTER full SELLERS_IN→ENTRY_READY cycle
                        (price sweeps below zone_low, then closes above zone_high).

    zone fields used:
      zone_high  = bears' entry (Lot 1 entry level / ENTRY_READY trigger)
      zone_low   = support (SL reference; SELLERS_IN trigger)
      sl         = bears' stop = our T1 target (above zone_high)
    """
    zh  = float(zone["zone_high"])
    zl  = float(zone["zone_low"])
    zt  = float(zone.get("sl", zh + (zh - zl)))   # T1 target = bears' stop above zone_high
    zht = zh - zl                                  # zone height
    _log.info(f"    ZONE  zh={zh}  zl={zl}  target={zt}  height={zht:.1f}  mode={mode}")
    if zht < 3.0:                                  # reject narrow/noise zones (< 3 pts)
        _log.info(f"    SKIP  zone too narrow ({zht:.1f} < 3.0 pts)")
        return None

    scale_in_lvl = round(zh - zht / 3.0, 2)       # 1/3 down from zone_high

    # SL buffer: fixed 2 pts for stock options (premiums are tiny, 2pt sweep is enough).
    # For index options use the user's SL_BUF input (default 10 pts).
    # Proportional (= zone_height) was tried but made R:R structurally ≤ 0.8 for all zones.
    sl_buf_eff = 2.0 if IS_STOCK else max(1.0, SL_BUF)
    sl_px      = round(zl - sl_buf_eff, 2)        # hard SL for both lots

    # R:R filter: reward (target - lot1 trigger) vs risk (trigger - sl).
    # Entry approximated at scale_in_lvl (zone_high - 1/3 zone_height).
    # For stocks: threshold = 1.0 (realistic given tiny premiums).
    # For indices: threshold = 1.5 (NIFTY/BN zones have larger swings).
    _rr_min    = 1.0 if IS_STOCK else MIN_RR
    _approx_entry = scale_in_lvl
    _reward       = zt - _approx_entry
    _risk         = _approx_entry - sl_px
    _log.info(f"    R:R   entry~={_approx_entry}  SL={sl_px}  target={zt}  "
              f"risk={_risk:.1f}  reward={_reward:.1f}  rr={_reward/_risk:.2f}  min={_rr_min}" if _risk > 0
              else f"    R:R   risk<=0 — skip")
    if _risk <= 0 or (_reward / _risk) < _rr_min:
        _log.info(f"    SKIP  R:R {_reward/_risk:.2f} < {_rr_min}" if _risk > 0 else "    SKIP  risk<=0")
        return None   # poor R:R — zone target not far enough above SL

    # -- Zone valid-from gate (common to both modes) ---------------------------
    raw_trapped_on = zone.get("trapped_on") or zone.get("closed_on")
    zone_valid_from: Optional[pd.Timestamp] = None
    if raw_trapped_on:
        try:
            _vf = pd.to_datetime(raw_trapped_on)
            zone_valid_from = _vf.tz_localize(None) if _vf.tzinfo else _vf
        except Exception:
            pass

    lot1_px: Optional[float] = None
    lot1_ts: Optional[pd.Timestamp] = None

    if mode == "trapscanner":
        # -- TrapScanner entry: wait for full SELLERS_IN → ENTRY_READY cycle --
        # Phase 1 (SELLERS_IN): any 1-min bar's LOW drops below zone_low
        # Phase 2 (ENTRY_READY): after phase 1, first 1-min close above zone_high
        # This mirrors the live TrapScanner which only fires after bears are fully trapped.
        sellers_in = False
        for _, r in df1m.iterrows():
            if r["datetime"].time() >= SQ_TIME:
                break
            if zone_valid_from is not None and r["datetime"] <= zone_valid_from:
                continue
            if not sellers_in:
                if float(r["low"]) < zl:          # bears entered below zone_low
                    sellers_in = True
            else:
                if float(r["close"]) > zh:         # bears' SL hit: close above zone_high
                    lot1_px = float(r["close"])
                    lot1_ts = r["datetime"]
                    break
    else:
        # -- Scale-in Lot 1: first 1-min close INSIDE zone --------------------
        for _, r in df1m.iterrows():
            if r["datetime"].time() >= SQ_TIME:
                break
            if zone_valid_from is not None and r["datetime"] <= zone_valid_from:
                continue
            if zl <= r["close"] <= zh:
                lot1_px = float(r["close"])
                lot1_ts = r["datetime"]
                break

    if lot1_px is None:
        _log.info(f"    NO ENTRY  entry condition never triggered")
        return None   # entry condition never triggered today

    _log.info(f"    LOT1 ENTRY  px={lot1_px}  ts={lot1_ts}  sl={sl_px}  target={zt}")

    # TrapScanner = 2 lots entered at same ENTRY_READY price, T1 exits Lot 1 (50%)
    if mode == "trapscanner":
        two_lots  = True
        lot2_px   = lot1_px   # both lots enter at ENTRY_READY close
        lot2_ts   = lot1_ts
        total_qty = LOT * 2
        t1_qty    = LOT
        rem_qty   = LOT
    else:
        pass   # scale-in Lot 2 detection follows below

    # -- Lot 2 scale-in detection (scale-in mode only) -------------------------
    post1m = df1m[df1m["datetime"] > lot1_ts].copy()
    post5m = df5m[df5m["datetime"] > lot1_ts].copy()

    if mode != "trapscanner":
        # lot2_px/ts declared here only — trapscanner already set them above
        lot2_px: Optional[float] = None
        lot2_ts: Optional[pd.Timestamp] = None
        # Conditions (both required):
        #   1. A 1-min bar closes at scale_in_lvl (price dipped to 1/3 zone depth)
        #   2. A 5-min bearish trap forms at or near scale_in_lvl
        reached_scalein = not post1m[post1m["close"] <= scale_in_lvl].empty
        if reached_scalein and len(post5m) >= 2:
            _, traps = scanner.scan_htf(post5m)
            for tz in traps:
                if tz.get("status") not in ("TRAPPED", "CLOSED"):
                    continue
                tz_zh = float(tz.get("zone_high", 0))
                tz_zl = float(tz.get("zone_low",  0))
                if abs(tz_zh - scale_in_lvl) > 10 or tz_zl < zl * 0.98:
                    continue
                ev_ts = pd.to_datetime(tz.get("trapped_on") or tz.get("closed_on"))
                if ev_ts is pd.NaT:
                    continue
                ev_ts = ev_ts.tz_localize(None) if ev_ts.tzinfo else ev_ts
                if ev_ts <= lot1_ts:
                    continue
                entry_row = post1m[post1m["datetime"] >= ev_ts]
                if not entry_row.empty:
                    lot2_px = float(entry_row.iloc[0]["close"])
                    lot2_ts = entry_row.iloc[0]["datetime"]
                    break
        two_lots  = lot2_px is not None
        total_qty = LOT * 2 if two_lots else LOT
    # t1_qty / rem_qty: both modes — two_lots already set correctly above
    t1_qty  = LOT   if two_lots else 0
    rem_qty = LOT

    # -- Build 5-min TSL events (ratchet: trap lows raise trail SL) ------------
    future5m = df5m[df5m["datetime"] > lot1_ts].copy()
    tsl_events: list[tuple[pd.Timestamp, float]] = []
    if len(future5m) >= 2:
        _, z5s = scanner.scan_htf(future5m)
        for z5 in z5s:
            if z5.get("status") not in ("TRAPPED", "CLOSED"):
                continue
            z5l = float(z5.get("zone_low", 0))
            if z5l <= sl_px:
                continue
            ev = pd.to_datetime(z5.get("trapped_on") or z5.get("closed_on"))
            if ev is pd.NaT:
                continue
            ev = ev.tz_localize(None) if ev.tzinfo else ev
            tsl_events.append((ev, z5l))
    tsl_events.sort(key=lambda x: x[0])

    # -- Bar-by-bar exit loop --------------------------------------------------
    future1m  = df1m[df1m["datetime"] > lot1_ts].copy()
    trail_sl  = sl_px
    t1_hit    = False
    t1_pnl    = 0.0
    t1_exit_ts: Optional[pd.Timestamp] = None
    exit_px   = None
    reason    = "OPEN"
    exit_ts   = None
    tsl_idx   = 0

    for _, row in future1m.iterrows():
        bts   = row["datetime"]
        bhi   = float(row["high"])
        blo   = float(row["low"])
        bcl   = float(row["close"])

        if bts.time() >= SQ_TIME:
            exit_px = bcl; reason = "EOD"; exit_ts = bts
            break

        # T1: only in 2-lot mode - bar_high touches or exceeds target
        if two_lots and not t1_hit and bhi >= zt:
            t1_hit    = True
            t1_exit_ts = bts
            t1_px     = _px(df1m, bts) or bcl
            t1_pnl    = round((t1_px - lot1_px) * t1_qty, 2)
            trail_sl  = sl_px   # reset TSL anchor after T1

        # Ratchet TSL: advance trail_sl using 5-min trap lows
        # Only active after T1 (per spec: "if both lots purchased + T1 hit → trail")
        # 1-lot trades use hard SL only throughout
        tsl_active = t1_hit
        if tsl_active:
            while tsl_idx < len(tsl_events):
                ev_ts, z_low = tsl_events[tsl_idx]
                if bts < ev_ts:
                    break
                if z_low > trail_sl:
                    trail_sl = z_low
                tsl_idx += 1

        # SL check — trigger on candle LOW (intrabar), exit at SL price (not close)
        # This avoids booking a worse close price when the bar just touched SL
        active_sl = trail_sl if tsl_active else sl_px
        if blo <= active_sl:
            exit_px = active_sl   # exit AT the SL level, not at close
            reason  = "TRAIL_SL" if (tsl_active and trail_sl > sl_px) else "SL"
            exit_ts = bts
            break

    if exit_px is None:
        last = future1m.iloc[-1] if not future1m.empty else None
        if last is not None:
            exit_px = float(last["close"]); reason = "EOD"; exit_ts = last["datetime"]
        else:
            return None

    _log.info(f"    EXIT  px={exit_px}  reason={reason}  ts={exit_ts}  t1_hit={t1_hit}")

    # -- P&L ------------------------------------------------------------------
    if two_lots:
        if t1_hit:
            # Lot 1 exited at T1; Lot 2 exits at exit_px
            rem_pnl   = round((exit_px - lot2_px) * LOT, 2)
            total_pnl = round(t1_pnl + rem_pnl, 2)
        else:
            # Both lots exit together (SL or EOD before T1)
            pnl1      = (exit_px - lot1_px) * LOT
            pnl2      = (exit_px - lot2_px) * LOT
            total_pnl = round(pnl1 + pnl2, 2)
            rem_pnl   = total_pnl
    else:
        total_pnl = round((exit_px - lot1_px) * LOT, 2)
        rem_pnl   = total_pnl

    _log.info(f"    TRADE  pnl=Rs {int(total_pnl):+,}  lots={total_qty//LOT}  "
              f"entry={lot1_px}  exit={exit_px}  reason={reason}")
    return {
        "date":           trade_date,
        "opt_type":       ot,
        "strike":         strike,
        "spot":           round(spot_val, 0),
        "zone":           f"{zl:.0f}->{zh:.0f}",
        "zone_high":      round(zh, 2),
        "zone_low":       round(zl, 2),
        "scale_in_lvl":   round(scale_in_lvl, 2),
        "lot1_entry":     round(lot1_px, 2),
        "lot1_ts":        str(lot1_ts)[:16],
        "lot2_entry":     round(lot2_px, 2) if lot2_px else None,
        "lot2_ts":        str(lot2_ts)[:16] if lot2_ts else "",
        "two_lots":       two_lots,
        "sl":             round(sl_px, 2),
        "t1_target":      round(zt, 2),
        "t1_hit":         t1_hit,
        "t1_pnl":         int(t1_pnl),
        "exit":           round(exit_px, 2),
        "exit_ts":        str(exit_ts)[:16] if exit_ts else "",
        "reason":         reason,
        "total_qty":      total_qty,
        "pnl_rs":         int(total_pnl),
        "pnl_pts":        round(total_pnl / total_qty, 2) if total_qty else 0,
        "kind":           zone.get("kind", "BEAR"),
        "mode":           mode,
    }


# -- Per-day backtest ----------------------------------------------------------
def _run_day(trade_date: str, df_spot_all: pd.DataFrame,
             _expiry_unused, cache: dict,
             csv_df: Optional[pd.DataFrame] = None,
             entry_mode: str = "scalein") -> list[dict]:
    td = pd.to_datetime(trade_date).date()

    df_prev  = df_spot_all[df_spot_all["datetime"].dt.date < td].copy()
    df_today = _mkt(df_spot_all[df_spot_all["datetime"].dt.date == td].copy())
    if df_prev.empty or df_today.empty:
        _log.info(f"  {trade_date}: no spot data - skip")
        return []

    prev_H = float(df_prev["high"].max())
    prev_L = float(df_prev["low"].min())
    prev_C = float(df_prev["close"].iloc[-1])
    piv    = _pivot(prev_H, prev_L, prev_C)
    today_open = float(df_today["open"].iloc[0])
    gap_pct    = abs(today_open - prev_C) / prev_C * 100

    gap_fired  = gap_pct >= 0.5
    gap_dir    = "UP" if today_open >= prev_C else "DOWN"

    # Spot bias: today_open vs prev_close with min gap threshold (0.3%)
    # A 0.1% open difference is noise — require meaningful gap before picking a side.
    # If gap < threshold in either direction, skip the day entirely.
    BIAS_GAP_PCT = 0.3
    bias_diff_pct = (today_open - prev_C) / prev_C * 100
    bias_bull = bias_diff_pct >= BIAS_GAP_PCT    # open meaningfully above prev_close → CE
    bias_bear = bias_diff_pct <= -BIAS_GAP_PCT   # open meaningfully below prev_close → PE

    # Strike selection
    # STOCKS: ATM rounded to nearest STEP, check BOTH CE and PE every day.
    #   Pivot/gap picks deep-OTM strikes for stocks → zero volume. ATM is always liquid.
    # INDICES: pivot-based (S1=CE, R1=PE) with gap override (4 steps from ATM).
    GAP_STEPS = 4
    if IS_STOCK:
        atm        = _round(today_open, STEP)
        ce_s       = atm
        pe_s       = atm
        strike_mode = f"ATM {atm} (stock, both sides)"
    elif gap_fired:
        atm  = _round(today_open, STEP)
        ce_s = max(STEP, atm - GAP_STEPS * STEP)
        pe_s = atm + GAP_STEPS * STEP
        strike_mode = f"GAP {gap_dir} {gap_pct:.1f}%"
    else:
        ce_s = _round(piv["S1"], STEP)
        pe_s = _round(piv["R1"], STEP)
        strike_mode = f"PIVOT S1={ce_s} R1={pe_s}"

    fetch_from = (td - timedelta(days=14)).isoformat()
    fetch_to   = (td + timedelta(days=1)).isoformat()

    # Resolve next-week expiry: CSV takes priority (historical data), else REGISTRY
    if csv_df is not None:
        all_csv_exp = sorted(csv_df["csv_expiry"].dropna().unique())
        this_week = next((e for e in all_csv_exp if e >= td and (e - td).days <= 8), None)
        if this_week:
            day_expiry = next((e for e in all_csv_exp if e > this_week), None)
        else:
            day_expiry = next((e for e in all_csv_exp if e > td), None)
        if day_expiry is None:
            _log.info(f"  {trade_date}: no next-week expiry in CSV - skip")
            return []
    elif IS_STOCK:
        day_expiry = _current_expiry(td)
        _log.info(f"  {trade_date}: monthly expiry = {day_expiry}")
    else:
        day_expiry = _next_week_expiry(td)
        _log.info(f"  {trade_date}: next-week expiry = {day_expiry}")

    # Leg selection:
    # STOCKS: always check both CE and PE at ATM (no bias filter — ATM is liquid both ways)
    # INDICES: bias filter (bullish day → CE only, bearish → PE only, ambiguous → skip)
    if IS_STOCK:
        legs = [("CE", ce_s), ("PE", pe_s)]
        bias_label = f"ATM {ce_s} both-sides"
    else:
        legs = []
        if bias_bull:
            legs.append(("CE", ce_s))
        elif bias_bear:
            legs.append(("PE", pe_s))
        else:
            _log.info(f"  {trade_date}: gap {bias_diff_pct:+.2f}% < {BIAS_GAP_PCT}% threshold - skip (ambiguous)")
            return []
        bias_label = f"BULL-bias(CE) +{bias_diff_pct:.2f}%" if bias_bull else f"BEAR-bias(PE) {bias_diff_pct:.2f}%"
    trades = []

    for ot, strike in legs:
        # Use a simple string key for CSV mode (no Upstox instrument lookup needed)
        key = f"{day_expiry}|{strike}|{ot}" if csv_df is not None else _opt_key(day_expiry, strike, ot)
        if not key:
            _log.info(f"  {trade_date} {ot}{strike}: no key")
            continue

        if key not in cache:
            # Try CSV first; fall back to Upstox API
            if csv_df is not None:
                day_expiry_obj = day_expiry  # date object
                cslice = csv_df[
                    (csv_df["csv_expiry"] == day_expiry_obj) &
                    (csv_df["csv_strike"] == strike) &
                    (csv_df["csv_ot"]     == ot)
                ].copy()
                if not cslice.empty:
                    cslice = cslice[["datetime","open","high","low","close","volume"]].sort_values("datetime").reset_index(drop=True)
                    cache[key] = cslice
                else:
                    cache[key] = pd.DataFrame()
            else:
                try:
                    cache[key] = _fetch_1m(key, fetch_from, fetch_to)
                except Exception as exc:
                    _log.info(f"  {trade_date} {ot}{strike}: fetch error {exc}")
                    cache[key] = pd.DataFrame()

        df_raw = cache[key]
        if df_raw.empty:
            _log.info(f"  {trade_date} {ot}{strike}: no option data")
            continue

        df_all   = _mkt(df_raw)
        df_today_opt = df_all[df_all["datetime"].dt.date == td].copy()
        if df_today_opt.empty:
            _log.info(f"  {trade_date} {ot}{strike}: no today bars")
            continue

        # HTF 75-min scan on PREVIOUS days only — today's bars must NOT be included.
        # Using today's spike to form a zone and then "entering" that same spike is circular.
        df_prev_opt = df_all[df_all["datetime"].dt.date < td].copy()
        htf = _rs(df_prev_opt, HTF_MIN)
        if len(htf) < 2:
            _log.info(f"  {trade_date} {ot}{strike}: not enough HTF bars")
            continue

        _, htf_zones = scanner.scan_htf(htf)

        # Only use zones trapped strictly BEFORE today (HTF scanned on prev-day bars only)
        def _trapped_before_today(z):
            ts = z.get("trapped_on")
            if not ts:
                return False
            try:
                return pd.to_datetime(ts).date() < td
            except Exception:
                return False

        valid = [z for z in htf_zones
                 if z.get("status") in ("TRAPPED", "CLOSED") and _trapped_before_today(z)]

        if not valid:
            # Intraday 15-min cascade: scan today's bars for zones that completed
            # (TRAPPED) earlier in the day. Entry is gated to bars AFTER trapped_on
            # in _simulate(), so the morning-spike circular problem is prevented.
            # Rule: zone valid only after C1 closes + C2 low < C1 low + C3 hits C1 high.
            df_15 = _rs(df_today_opt, 15)
            if len(df_15) >= 3:   # need at least 3 bars: C1, C2 (sellers), C3 (trap)
                _, cas = scanner.scan_htf(df_15)
                valid = [z for z in cas if z.get("status") in ("TRAPPED", "CLOSED")
                         and z.get("trapped_on")]
                if valid:
                    _log.info(f"  {trade_date} {ot}{strike}: no HTF zone -> cascade 15m ({len(valid)} zones)")
                else:
                    _log.info(f"  {trade_date} {ot}{strike}: no zones (HTF or 15m) - skip")
                    continue
            else:
                _log.info(f"  {trade_date} {ot}{strike}: no zones - skip")
                continue
        else:
            _log.info(f"  {trade_date} {ot}{strike} [{strike_mode} {bias_label}]: {len(valid)} HTF zone(s)")

        df5m = _rs(df_today_opt, 5)
        spot_val = float(df_today.iloc[0]["open"]) if not df_today.empty else 0.0

        for zone in valid:
            result = _simulate(zone, df_today_opt, df5m,
                               trade_date, strike, ot, spot_val,
                               mode=entry_mode)
            if result:
                result["strike_mode"] = strike_mode
                trades.append(result)
                # One-trade-at-a-time: stop after first successful sim per leg
                break

    return trades


# -- CSV option data loader ----------------------------------------------------
def _load_csv_options(csv_path: str, index: str) -> pd.DataFrame:
    """Load option data from a GFD-format CSV for a given underlying.

    CSV columns: Ticker, Date, Time, Open, High, Low, Close, Volume, Open Interest
    Ticker format: {UNDERLYING}{DD}{MON}{YY}{STRIKE}{CE|PE}.NFO
    Date: DD-MM-YYYY   Time: HH:MM:SS
    """
    import re
    df = pd.read_csv(csv_path, usecols=["Ticker", "Date", "Time", "Open", "High", "Low", "Close", "Volume"])
    # Keep only the requested underlying
    pat = re.compile(rf"^{re.escape(index)}\d{{2}}[A-Z]{{3}}\d{{2}}\d+[CP][EP]\.NFO$")
    df = df[df["Ticker"].apply(lambda t: bool(pat.match(t)))].copy()
    if df.empty:
        return pd.DataFrame()
    # Parse datetime: Date = DD-MM-YYYY, Time = HH:MM:SS
    df["datetime"] = pd.to_datetime(df["Date"] + " " + df["Time"], format="%d-%m-%Y %H:%M:%S")
    # Parse strike, expiry, opt_type from ticker
    def _parse(t):
        m = re.match(r"^([A-Z0-9]+)(\d{2})([A-Z]{3})(\d{2})(\d+)(CE|PE)\.NFO$", t)
        if not m:
            return None, None, None
        dd, mon, yy, strike, ot = m.group(2), m.group(3), m.group(4), m.group(5), m.group(6)
        _MONS = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
                 "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
        try:
            expiry = date(2000 + int(yy), _MONS[mon], int(dd))
        except Exception:
            return None, None, None
        return expiry, int(strike), ot
    parsed = df["Ticker"].apply(_parse)
    df["csv_expiry"] = parsed.apply(lambda x: x[0])
    df["csv_strike"] = parsed.apply(lambda x: x[1])
    df["csv_ot"]     = parsed.apply(lambda x: x[2])
    # Lowercase OHLCV columns
    df.rename(columns={"Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume"}, inplace=True)
    return df.dropna(subset=["csv_expiry"]).reset_index(drop=True)


# -- Main ----------------------------------------------------------------------
def run_scalein_backtest(token: str, days: int = 10,
                         start: str = "", end: str = "",
                         expiry_str: str = "",
                         index: str = "NIFTY",
                         sl_buf_override: float = 0.0,
                         csv_path: str = "",
                         entry_mode: str = "scalein") -> dict:
    global _HEADERS, IDX, SPOT_KEY, STEP, LOT, SL_BUF, IS_STOCK

    # Set up per-run log file
    import datetime as _dt
    _ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    _log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
    os.makedirs(_log_dir, exist_ok=True)
    _log_path = os.path.join(_log_dir, f"backtest_{index.upper()}_{entry_mode}_{_ts}.log")
    _setup_log(_log_path)
    _log.info(f"=== Backtest log: {_log_path}  mode={entry_mode} ===")

    # Override index-specific constants
    IDX = index.upper()
    _index_cfg = {
        "NIFTY":     {"step": 50,  "lot": 65,  "spot_key": "NSE_INDEX|Nifty 50",        "stock": False},
        "BANKNIFTY": {"step": 100, "lot": 15,  "spot_key": "NSE_INDEX|Nifty Bank",       "stock": False},
        "SENSEX":    {"step": 100, "lot": 10,  "spot_key": "BSE_INDEX|SENSEX",           "stock": False},
        "FINNIFTY":  {"step": 50,  "lot": 40,  "spot_key": "NSE_INDEX|Nifty Fin Service","stock": False},
    }
    if IDX in _index_cfg:
        cfg = _index_cfg[IDX]
        STEP     = cfg["step"]
        LOT      = cfg["lot"]
        SPOT_KEY = cfg["spot_key"]
        IS_STOCK = False
    elif IDX in _STOCK_CONFIG:
        scfg     = _STOCK_CONFIG[IDX]
        STEP     = scfg["step"]
        LOT      = scfg["lot"]
        SPOT_KEY = ""  # resolved below after REGISTRY load
        IS_STOCK = True
    else:
        # Unknown instrument — use sensible defaults
        STEP     = 50
        LOT      = 100
        SPOT_KEY = ""
        IS_STOCK = True

    if sl_buf_override > 0:
        SL_BUF = sl_buf_override

    _HEADERS = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    try:
        # For stocks, REGISTRY now falls through to master JSON automatically
        REGISTRY.load_sync(IDX, access_token=token)
        if IS_STOCK and not SPOT_KEY:
            # Use futures contract key as spot proxy for NSE stocks
            SPOT_KEY = REGISTRY.historical_instrument_key(IDX) or ""
            if SPOT_KEY:
                _log.info(f"[REGISTRY] {IDX} spot key (futures proxy): {SPOT_KEY}")
            else:
                _log.info(f"[REGISTRY] {IDX}: no futures key found — spot bars may fail")
    except Exception as exc:
        _log.info(f"[REGISTRY] {exc} - using symbol fallback")

    if expiry_str:
        _log.info(f"[INFO] --expiry {expiry_str} ignored; using per-day next-week expiry from REGISTRY")

    # Load CSV option data if provided
    csv_df: Optional[pd.DataFrame] = None
    if csv_path and os.path.exists(csv_path):
        _log.info(f"[CSV] Loading option data from {csv_path} ...")
        csv_df = _load_csv_options(csv_path, IDX)
        if csv_df is not None and not csv_df.empty:
            csv_dates = sorted(csv_df["datetime"].dt.date.unique())
            _log.info(f"[CSV] {len(csv_df)} rows | {len(csv_dates)} dates | {IDX}")
            # Use CSV date range if no explicit range
            if not (start and end):
                start = csv_dates[0].isoformat()
                end   = csv_dates[-1].isoformat()
        else:
            _log.info(f"[CSV] No {IDX} data found in CSV")
            csv_df = None

    # Date range
    if start and end:
        s_date = date.fromisoformat(start)
        e_date = date.fromisoformat(end)
    else:
        e_date = date.today()
        s_date = e_date - timedelta(days=days * 2)

    # Get trading days
    trading_days = []
    d = s_date
    while d <= e_date:
        if d.weekday() < 5:
            trading_days.append(d.isoformat())
        d += timedelta(days=1)
    if not (start and end):
        trading_days = trading_days[-days:]

    _log.info(f"\n{IDX} Scale-In Backtest  {trading_days[0]} to {trading_days[-1]}"
          f"  ({len(trading_days)} days)  Expiry=NEXT-WEEK(REGISTRY)  SL_BUF={SL_BUF}")

    # Fetch spot bars
    spot_from = (date.fromisoformat(trading_days[0]) - timedelta(days=7)).isoformat()
    spot_to   = (date.fromisoformat(trading_days[-1]) + timedelta(days=1)).isoformat()
    _log.info(f"Fetching {IDX} spot {spot_from} to {spot_to}...")
    df_spot = _fetch_1m(SPOT_KEY, spot_from, spot_to)
    if df_spot.empty:
        return {"ok": False, "error": "No spot data"}
    df_spot = _mkt(df_spot)
    _log.info(f"  {len(df_spot)} spot bars\n")

    cache: dict = {}
    all_trades: list[dict] = []
    for td in trading_days:
        day_trades = _run_day(td, df_spot, None, cache, csv_df=csv_df, entry_mode=entry_mode)
        all_trades.extend(day_trades)

    # -- Summary ---------------------------------------------------------------
    wins   = [t for t in all_trades if t["pnl_rs"] > 0]
    losses = [t for t in all_trades if t["pnl_rs"] <= 0]
    total  = sum(t["pnl_rs"] for t in all_trades)
    gw     = sum(t["pnl_rs"] for t in wins)
    gl     = abs(sum(t["pnl_rs"] for t in losses))
    pf     = round(gw / gl, 2) if gl > 0 else 99.0

    two_lot_trades = [t for t in all_trades if t["two_lots"]]
    one_lot_trades = [t for t in all_trades if not t["two_lots"]]

    _log.info(f"\n{'='*110}")
    _log.info(f"{IDX} Scale-In  {trading_days[0]} to {trading_days[-1]}  "
          f"Trades={len(all_trades)}  Win={round(100*len(wins)/len(all_trades),1) if all_trades else 0}%  "
          f"Rs {total:+,.0f}  PF={pf}  "
          f"[1-lot={len(one_lot_trades)}  2-lot={len(two_lot_trades)}]")
    _log.info(f"  {'DATE':<10}  {'OT':<3}  {'STK':>6}  {'SPOT':>6}  "
          f"{'ZONE':^12}  {'L1@':>6}  {'L2@':>6}  {'SL':>6}  {'T1':>6}  "
          f"{'EXIT':>6}  {'TIME':5}  {'REASON':<10}  {'T1?':<3}  {'LOTS':<4}  {'P&L Rs':>9}")
    _log.info(f"  {'-'*108}")
    running = 0
    for t in all_trades:
        running += t["pnl_rs"]
        l2s  = f"{t['lot2_entry']:.1f}" if t["lot2_entry"] else "  -  "
        t1f  = "Y" if t["t1_hit"] else "N"
        lots = "2" if t["two_lots"] else "1"
        _log.info(f"  {t['date']}  {t['opt_type']:<3}  {t['strike']:>6}  "
              f"{t['spot']:>6.0f}  {t['zone']:^12}  "
              f"{t['lot1_entry']:>6.1f}  {l2s:>6}  "
              f"{t['sl']:>6.1f}  {t['t1_target']:>6.1f}  "
              f"{t['exit']:>6.1f}  {t['exit_ts'][11:16]:5}  "
              f"{t['reason']:<10}  {t1f:<3}  {lots:<4}  Rs {t['pnl_rs']:>+8,.0f}  "
              f"(cum Rs {running:+,.0f})")

    _log.info(f"\n  Total: Rs {total:+,.0f}  Wins={len(wins)}  Losses={len(losses)}  PF={pf}"
          f"  AvgWin=Rs {round(gw/len(wins),0) if wins else 0:,.0f}"
          f"  AvgLoss=Rs {round(-gl/len(losses),0) if losses else 0:,.0f}")

    summary = {
        "days": len(trading_days), "trades": len(all_trades),
        "wins": len(wins), "losses": len(losses),
        "win_pct": round(100*len(wins)/len(all_trades), 1) if all_trades else 0,
        "total_rs": int(total), "profit_factor": pf,
        "avg_win":  int(round(gw/len(wins), 0)) if wins else 0,
        "avg_loss": int(round(-gl/len(losses), 0)) if losses else 0,
        "one_lot_trades": len(one_lot_trades),
        "two_lot_trades": len(two_lot_trades),
    }
    _log.info(f"\n=== Done. Log saved: {_log_path} ===")
    return {"ok": True, "summary": summary, "trades": all_trades,
            "log_path": _log_path, "lot_size": LOT, "days_run": len(trading_days)}


# -- CLI -----------------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="NIFTY Scale-In Backtest")
    ap.add_argument("--token",  required=True, help="Upstox access token")
    ap.add_argument("--days",   type=int, default=10, help="Last N trading days")
    ap.add_argument("--start",  default="", help="Start date YYYY-MM-DD")
    ap.add_argument("--end",    default="", help="End date YYYY-MM-DD")
    ap.add_argument("--expiry", default="2026-06-26",
                    help="Option expiry date (default: 2026-06-26 coming week)")
    ap.add_argument("--sl-buf", type=float, default=20.0,
                    help="SL buffer below zone_low (default 20 pts)")
    args = ap.parse_args()

    SL_BUF = args.sl_buf

    result = run_scalein_backtest(
        token      = args.token,
        days       = args.days,
        start      = args.start,
        end        = args.end,
        expiry_str = args.expiry,
    )
    if not result["ok"]:
        _log.info(f"ERROR: {result['error']}")
        sys.exit(1)
