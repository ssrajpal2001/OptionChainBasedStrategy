"""
SENSEX Intraday Backtest — 15m HTF / 5m Confirm / 1m Entry

Strategy:
  1. At market open (9:15) get SENSEX spot first bar open
  2. Round to nearest 500:
       CE strike = floor(spot / 500) * 500   (below spot)
       PE strike = ceil(spot  / 500) * 500   (above spot)
  3. 15-min HTF: detect seller-trap zones on OPTION premium bars
  4. When 15m zone is TRAPPED → wait for 5m candle to confirm
       (5m closes INSIDE zone: zone_low ≤ close ≤ zone_high)
  5. When 5m confirms → wait for 1m candle to show BUY signal
       (1m close > previous 1m high — momentum confirmation)
  6. Enter at next 1m open after signal
  7. Exit on: SL (close < zone_low) | Target | EOD 15:15

Usage:
  python scripts/sensex_intraday_backtest.py --token <upstox_token> [--days 10] [--lots 1]

Requires: pip install requests pandas tabulate
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ── Config ────────────────────────────────────────────────────────────────────
SENSEX_INDEX_KEY = "BSE_INDEX|SENSEX"
STRIKE_STEP      = 500          # round spot to nearest 500
HTF_MIN          = 15           # HTF timeframe
MTF_MIN          = 5            # entry confirmation timeframe
LTF_MIN          = 1            # trigger timeframe
EOD_TIME         = "14:00"      # no new entries after this time (square-off at 15:15)
SQ_OFF_TIME      = "15:15"      # hard square-off
SL_BUFFER        = 0.0          # pts below zone_low for SL (0 = zone_low itself)
TARGET_MULT      = 1.5          # target = zone_high + (zone_high-zone_low)*TARGET_MULT
LOT_SIZE         = 20           # SENSEX lot size
MIN_ZONE_RANGE   = 30           # skip zones smaller than this (noise filter)
MAX_TRADES_PER_SIDE = 2         # max entries per direction (CE/PE) per day
MIN_REWARD       = 20           # skip if target - entry < this (entry already overshot zone)

# BSE options URL uses BSE_FO — SENSEX weekly options
# Instrument keys are looked up from REGISTRY at runtime; fallback to Upstox search API
UPSTOX_HIST_URL  = "https://api.upstox.com/v2/historical-candle/{key}/1minute/{to}/{from_}"
UPSTOX_QUOTE_URL = "https://api.upstox.com/v2/market-quote/quotes"
UPSTOX_SEARCH_URL = "https://api.upstox.com/v2/instruments/search"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def _round_ce(spot: float) -> int:
    """Floor to nearest STRIKE_STEP — CE is below spot."""
    return int(spot // STRIKE_STEP) * STRIKE_STEP


def _round_pe(spot: float) -> int:
    """Ceil to nearest STRIKE_STEP — PE is above spot."""
    import math
    return int(math.ceil(spot / STRIKE_STEP)) * STRIKE_STEP


def _fetch_1m(key: str, dt: date, token: str) -> pd.DataFrame:
    """Fetch 1-min OHLCV bars from Upstox for a single day."""
    key_enc = key.replace("|", "%7C")
    ds = dt.strftime("%Y-%m-%d")
    url = f"https://api.upstox.com/v2/historical-candle/{key_enc}/1minute/{ds}/{ds}"
    try:
        r = requests.get(url, headers=_headers(token), timeout=10)
        d = r.json()
    except Exception as exc:
        print(f"  [FETCH ERROR] {key} {dt}: {exc}")
        return pd.DataFrame()
    if d.get("status") != "success":
        print(f"  [API ERROR] {key} {dt}: {d.get('message','?')}")
        return pd.DataFrame()
    candles = d.get("data", {}).get("candles", [])
    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "volume", "oi"])
    df["ts"] = pd.to_datetime(df["ts"]).dt.tz_localize(None)
    df = df.sort_values("ts").reset_index(drop=True)
    df = df[(df["ts"].dt.time >= pd.Timestamp("09:15").time()) &
            (df["ts"].dt.time <= pd.Timestamp("15:30").time())]
    return df.reset_index(drop=True)


def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """Resample 1m bars to N-min OHLCV (clock-aligned)."""
    if df.empty:
        return df
    idx = pd.DatetimeIndex(df["ts"])
    resampler = df.set_index(idx).resample(f"{minutes}min", label="right", closed="right")
    out = resampler.agg({"open": "first", "high": "max", "low": "min",
                         "close": "last", "volume": "sum"}).dropna()
    out = out[(out.index.time >= pd.Timestamp("09:15").time()) &
              (out.index.time <= pd.Timestamp("15:30").time())]
    return out.reset_index().rename(columns={"ts": "ts"}).rename(columns={"index": "ts"})


def _find_sensex_option_key(spot: float, expiry_date: date, side: str,
                             token: str) -> Optional[str]:
    """
    Look up the BSE_FO instrument key for a SENSEX option.
    side = 'CE' or 'PE'
    strike = _round_ce(spot) for CE, _round_pe(spot) for PE
    Uses Upstox instrument search API.
    """
    strike = _round_ce(spot) if side == "CE" else _round_pe(spot)
    expiry_str = expiry_date.strftime("%Y-%m-%d")

    # Try Upstox instrument search
    try:
        r = requests.get(
            "https://api.upstox.com/v2/instruments",
            params={"segment": "BSE_FO"},
            headers=_headers(token), timeout=15
        )
    except Exception:
        pass

    # Fall back: try known REGISTRY
    try:
        from data_layer.instrument_registry import REGISTRY
        if REGISTRY.is_loaded("SENSEX"):
            key = REGISTRY.get_upstox_key("SENSEX", expiry_date, strike, side)
            if key:
                return key
    except Exception:
        pass

    return None


def _get_sensex_spot_open(dt: date, token: str) -> float:
    """Fetch SENSEX index first-bar open for the day."""
    df = _fetch_1m(SENSEX_INDEX_KEY, dt, token)
    if df.empty:
        return 0.0
    return float(df.iloc[0]["open"])


# ── Zone detection ────────────────────────────────────────────────────────────

def _detect_seller_trap_zones(htf: pd.DataFrame) -> list[dict]:
    """
    Every completed 15m candle creates a zone (low → high).
    The zone is "armed" and we then watch 1m ticks to advance state.
    """
    zones = []
    ts_col = "ts" if "ts" in htf.columns else htf.index.name
    for i in range(len(htf)):
        row = htf.iloc[i]
        zone_low  = round(float(row["low"]),  2)
        zone_high = round(float(row["high"]), 2)
        if zone_high <= zone_low or (zone_high - zone_low) < 5:
            continue
        zone_range = zone_high - zone_low
        # Trigger = midpoint of zone (50%) — price recovering through mid = sellers losing
        trigger = round(zone_low + zone_range * 0.50, 2)
        # Target = zone_high + range × mult (sellers get squeezed above zone_high)
        target  = round(zone_high + zone_range * TARGET_MULT, 2)
        sl      = round(zone_low - SL_BUFFER, 2)
        zones.append({
            "zone_ts":      row["ts"] if ts_col == "ts" else htf.index[i],
            "zone_low":     zone_low,
            "zone_high":    zone_high,
            "zone_trigger": trigger,
            "target":       target,
            "sl":           sl,
            "state":        "WATCH",
            # Track lowest price seen while SELLERS_IN (confirms sellers really entered)
            "sellers_in_low": None,
        })
    return zones


def _advance_zone_state(zone: dict, price: float) -> dict:
    """
    State machine matching what the charts show:

    WATCH
      → price ≤ zone_low                    → SELLERS_IN   (sellers pushed it down)
    SELLERS_IN
      → price ≥ zone_trigger (midpoint)     → TRAPPED      (sellers starting to lose)
    TRAPPED
      → price ≥ zone_high                   → ENTRY_READY  (sellers fully trapped above zone)

    Entry is triggered from ENTRY_READY state via 5m + 1m confirmation.
    SL = zone_low (where sellers entered — if price goes back there, trap failed).
    """
    z = zone.copy()
    st = z["state"]
    if st == "WATCH":
        if price <= z["zone_low"]:
            z["state"] = "SELLERS_IN"
            z["sellers_in_low"] = price
    elif st == "SELLERS_IN":
        # Track how deep sellers pushed
        if z["sellers_in_low"] is None or price < z["sellers_in_low"]:
            z["sellers_in_low"] = price
        # Sellers are trapped when price recovers through the midpoint trigger
        if price >= z["zone_trigger"]:
            z["state"] = "TRAPPED"
    elif st == "TRAPPED":
        # Full trap confirmed when price breaks above zone_high
        if price >= z["zone_high"]:
            z["state"] = "ENTRY_READY"
        # If price falls back to zone_low, trap failed — reset to WATCH
        elif price <= z["zone_low"]:
            z["state"] = "WATCH"
            z["sellers_in_low"] = None
    # ENTRY_READY: stays until consumed by entry logic
    return z


# ── Main per-day simulation ───────────────────────────────────────────────────
#
# STRATEGY (matches user's June-22 SENSEX CE-77000 example):
#   Reference  = option's OPENING PRICE (first 1m open) = zone_high
#   Session low = lowest price seen from open onward      = zone_low
#   State machine (per 1m bar):
#     WATCH    → price drops MIN_DIP pts below ref       → SELLERS_IN
#     SELLERS_IN → price recovers to REF_PCT (90%) of ref → TRAPPED
#     TRAPPED  → 5m close above ref AND 1m breakout      → ENTRY
#   Entry at next 1m open.  SL = session_low.  Target = ref + (ref-session_low)*TARGET_MULT
#   Max 1 trade per side per day; no new entries after EOD_TIME.

MIN_DIP_PCT  = 0.015  # sellers must push option at least 1.5% below opening price

def _run_day(dt: date, token: str, lots: int,
             ce_key: str, pe_key: str,
             ce_strike: int, pe_strike: int) -> list[dict]:
    trades = []

    for side, key, strike in [("CE", ce_key, ce_strike), ("PE", pe_key, pe_strike)]:
        df1m = _fetch_1m(key, dt, token)
        if df1m.empty or len(df1m) < 20:
            print(f"    [{side} {strike}] insufficient bars ({len(df1m)}), skip")
            continue

        if df1m["ts"].dt.tz is not None:
            df1m["ts"] = df1m["ts"].dt.tz_localize(None)

        mtf = _resample(df1m, MTF_MIN)   # 5m candles for confirmation

        ref_price    = float(df1m.iloc[0]["open"])   # opening price = our reference
        session_low  = ref_price                      # track lowest price seen
        min_dip_pts  = ref_price * MIN_DIP_PCT        # 3% of opening = meaningful dip

        print(f"    [{side} {strike}] bars={len(df1m)} open={ref_price:.1f}  need_dip_below={ref_price - min_dip_pts:.1f}")

        # State: WATCH → SELLERS_IN → TRAPPED → (entry fired)
        state        = "WATCH"
        trap_low     = None    # lowest price seen while SELLERS_IN
        entered      = False   # only 1 trade per side per day
        position     = None

        no_new_entry = pd.Timestamp(f"{dt} {EOD_TIME}")
        sq_off       = pd.Timestamp(f"{dt} {SQ_OFF_TIME}")

        for _, bar1m in df1m.iterrows():
            ts     = bar1m["ts"]
            ltp    = float(bar1m["close"])
            high1m = float(bar1m["high"])
            low1m  = float(bar1m["low"])

            # Track session low (even while in position)
            if low1m < session_low:
                session_low = low1m

            if ts >= sq_off:
                if position:
                    pnl = (ltp - position["entry"]) * position["qty"]
                    trades.append({**position,
                                   "exit_ts": ts, "exit_price": ltp,
                                   "pnl_pts": ltp - position["entry"],
                                   "pnl_rs":  pnl, "reason": "EOD"})
                    position = None
                break

            # ── Manage open position ──────────────────────────────────────────
            if position:
                if low1m <= position["sl"]:
                    pnl = (position["sl"] - position["entry"]) * position["qty"]
                    trades.append({**position,
                                   "exit_ts": ts, "exit_price": position["sl"],
                                   "pnl_pts": position["sl"] - position["entry"],
                                   "pnl_rs":  pnl, "reason": "SL"})
                    position = None
                elif high1m >= position["target"]:
                    pnl = (position["target"] - position["entry"]) * position["qty"]
                    trades.append({**position,
                                   "exit_ts": ts, "exit_price": position["target"],
                                   "pnl_pts": position["target"] - position["entry"],
                                   "pnl_rs":  pnl, "reason": "TARGET"})
                    position = None
                continue

            if entered or ts >= no_new_entry:
                continue

            # ── State machine ─────────────────────────────────────────────────
            if state == "WATCH":
                # Sellers must push option at least MIN_DIP_PCT below opening price
                if ltp <= ref_price - min_dip_pts:
                    state    = "SELLERS_IN"
                    trap_low = ltp

            elif state == "SELLERS_IN":
                if ltp < trap_low:
                    trap_low = ltp
                # Sellers trapped when price recovers to 50% between trap_low and ref
                midpoint = (trap_low + ref_price) / 2
                if ltp >= midpoint:
                    state = "TRAPPED"

            elif state == "TRAPPED":
                # If price falls back to trap_low — sellers regained control, reset
                if ltp <= trap_low:
                    state    = "WATCH"
                    trap_low = None
                    continue

                # ── 5m confirmation: last completed 5m must close ABOVE ref ──
                mtf_ts   = mtf["ts"] if "ts" in mtf.columns else mtf.index
                mtf_past = mtf[mtf_ts < ts]
                if mtf_past.empty:
                    continue
                last_5m = mtf_past.iloc[-1]
                if float(last_5m["close"]) < ref_price:
                    continue   # 5m not yet above opening ref

                # ── 1m breakout: close > prev 1m high ─────────────────────────
                bar_idx = bar1m.name
                if bar_idx < 1:
                    continue
                prev1m = df1m.iloc[bar_idx - 1]
                if ltp <= float(prev1m["high"]):
                    continue

                # ── ENTRY ─────────────────────────────────────────────────────
                next_idx = bar_idx + 1
                if next_idx >= len(df1m):
                    continue
                entry_bar   = df1m.iloc[next_idx]
                entry_price = float(entry_bar["open"])
                entry_ts    = entry_bar["ts"]
                if entry_ts >= no_new_entry:
                    continue

                dip_range = ref_price - trap_low    # how far sellers pushed it
                sl        = round(trap_low - SL_BUFFER, 1)
                target    = round(ref_price + dip_range * TARGET_MULT, 1)

                # R:R check: must be at least 1.5:1
                risk   = entry_price - sl
                reward = target - entry_price
                if risk <= 0 or reward / risk < 1.5:
                    continue

                qty      = lots * LOT_SIZE
                position = {
                    "date":     dt.isoformat(),
                    "side":     side,
                    "strike":   strike,
                    "key":      key,
                    "entry_ts": entry_ts,
                    "entry":    entry_price,
                    "sl":       sl,
                    "target":   target,
                    "qty":      qty,
                    "htf_zone": f"{round(trap_low,1)}→{ref_price}",
                }
                entered = True
                _ets = entry_ts.strftime('%H:%M') if hasattr(entry_ts, 'strftime') else str(entry_ts)[11:16]
                print(f"      ENTRY {_ets} {side} {strike} @ {entry_price:.1f}"
                      f"  ref={ref_price:.1f}  trap_low={trap_low:.1f}"
                      f"  SL={sl:.1f}  T={target:.1f}  R:R={reward/risk:.1f}")

        # Hard EOD close if loop ended with open position
        if position and not df1m.empty:
            last = df1m.iloc[-1]
            ltp  = float(last["close"])
            pnl  = (ltp - position["entry"]) * position["qty"]
            trades.append({**position,
                           "exit_ts": last["ts"], "exit_price": ltp,
                           "pnl_pts": ltp - position["entry"],
                           "pnl_rs":  pnl, "reason": "EOD"})

    return trades


# ── Multi-day runner ──────────────────────────────────────────────────────────

def _get_trading_days(n: int) -> list[date]:
    """Return last N weekdays up to (and including) today."""
    days = []
    d = date.today()
    while len(days) < n:
        if d.weekday() < 5:   # Mon-Fri
            days.append(d)
        d -= timedelta(days=1)
    return list(reversed(days))


def _find_option_key_from_registry(underlying: str, expiry: date,
                                   strike: int, side: str) -> Optional[str]:
    """Try REGISTRY first, else return None."""
    try:
        from data_layer.instrument_registry import REGISTRY
        if REGISTRY.is_loaded(underlying):
            key = REGISTRY.get_upstox_key(underlying, expiry, strike, side)
            return key
    except Exception:
        pass
    return None


def _find_key_and_expiry(token: str, strike: int, side: str,
                         trade_date: date) -> tuple[Optional[str], Optional[date]]:
    """
    Find SENSEX option instrument key by trying candidate expiry dates
    (every day from trade_date to +14 days) via Upstox search API.
    Returns (instrument_key, expiry_date) of the first match found.
    """
    from datetime import timedelta
    for delta in range(15):
        candidate = trade_date + timedelta(days=delta)
        try:
            url = "https://api.upstox.com/v2/instruments/search"
            params = {
                "exchange": "BSE_FO",
                "segment":  "BSE_FO",
                "query":    f"SENSEX {strike} {side} {candidate.strftime('%d%b%y').upper()}"
            }
            r = requests.get(url, params=params, headers=_headers(token), timeout=10)
            d = r.json()
            items = d.get("data", [])
            if items:
                key = items[0].get("instrument_key")
                if key:
                    print(f"    [{side} {strike}] found expiry {candidate} key={key}")
                    return key, candidate
        except Exception:
            pass
    return None, None


def run(token: str, days: int = 10, lots: int = 1) -> None:
    print(f"\nSENSEX Intraday Backtest  |  {days} days  |  {lots} lot(s)")
    print(f"Strategy: 15m HTF zone → 5m confirm → 1m buy signal")
    print(f"Strike: nearest {STRIKE_STEP} (CE=below, PE=above market open)")
    print("=" * 70)

    trading_days = _get_trading_days(days)
    all_trades: list[dict] = []

    # Load REGISTRY with SENSEX BSE_FO contracts (real expiry dates, no calendar math)
    from data_layer.instrument_registry import REGISTRY
    try:
        REGISTRY.load_sync("SENSEX", token)
        print(f"REGISTRY loaded — SENSEX loaded: {REGISTRY.is_loaded('SENSEX')}, "
              f"expiries: {REGISTRY._expiries.get('SENSEX', [])[:5]}\n")
    except Exception as exc:
        print(f"REGISTRY load failed: {exc}\n")

    for dt in trading_days:
        print(f"\n── {dt} ──────────────────────────────────────────────────")

        spot = _get_sensex_spot_open(dt, token)
        if spot <= 0:
            print(f"  Could not fetch SENSEX spot for {dt}, skip")
            continue

        ce_strike = _round_ce(spot)
        pe_strike = _round_pe(spot)
        print(f"  Spot open={spot:.0f}  CE={ce_strike}  PE={pe_strike}")

        # Get expiry from REGISTRY (correct, no calendar math)
        expiry = REGISTRY.get_active_expiry("SENSEX", from_date=dt) if REGISTRY.is_loaded("SENSEX") else None
        if expiry is None:
            # Fallback: brute-force search via API
            _, expiry = _find_key_and_expiry(token, ce_strike, "CE", dt)
        if expiry is None:
            print(f"  Could not determine expiry for {dt}, skip")
            continue
        print(f"  Expiry: {expiry}")

        ce_key = REGISTRY.get_upstox_key("SENSEX", expiry, ce_strike, "CE") if REGISTRY.is_loaded("SENSEX") else None
        pe_key = REGISTRY.get_upstox_key("SENSEX", expiry, pe_strike, "PE") if REGISTRY.is_loaded("SENSEX") else None
        if not ce_key:
            ce_key, _ = _find_key_and_expiry(token, ce_strike, "CE", dt)
        if not pe_key:
            pe_key, _ = _find_key_and_expiry(token, pe_strike, "PE", dt)

        print(f"  CE={ce_key or 'NOT FOUND'}  PE={pe_key or 'NOT FOUND'}")
        if not ce_key and not pe_key:
            print(f"  No keys found, skip")
            continue

        day_trades = _run_day(
            dt=dt, token=token, lots=lots,
            ce_key=ce_key or "", pe_key=pe_key or "",
            ce_strike=ce_strike, pe_strike=pe_strike,
        )
        all_trades.extend(day_trades)
        time.sleep(0.3)   # be polite to API

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("BACKTEST RESULTS")
    print("=" * 70)

    if not all_trades:
        print("No trades generated.")
        return

    df = pd.DataFrame(all_trades)
    total = len(df)
    wins  = len(df[df["pnl_rs"] > 0])
    losses = len(df[df["pnl_rs"] <= 0])
    total_pnl = df["pnl_rs"].sum()
    avg_win  = df[df["pnl_rs"] > 0]["pnl_rs"].mean() if wins else 0
    avg_loss = df[df["pnl_rs"] <= 0]["pnl_rs"].mean() if losses else 0

    print(f"Total trades : {total}")
    print(f"Wins / Losses: {wins} / {losses}  ({wins/total*100:.0f}% win rate)")
    print(f"Total P&L    : ₹{total_pnl:,.0f}")
    print(f"Avg Win      : ₹{avg_win:,.0f}")
    print(f"Avg Loss     : ₹{avg_loss:,.0f}")
    if avg_loss != 0:
        print(f"Profit factor: {abs(avg_win * wins / (avg_loss * losses)):.2f}" if losses else "∞")

    print(f"\n{'Date':<12}{'Side':<5}{'Strike':<8}{'Entry':>7}{'Exit':>7}{'P&L pts':>9}{'P&L ₹':>10}  Reason")
    print("-" * 70)
    for _, t in df.iterrows():
        ets = t["entry_ts"]
        xts = t["exit_ts"]
        ets_s = ets.strftime("%H:%M") if hasattr(ets, "strftime") else str(ets)
        xts_s = xts.strftime("%H:%M") if hasattr(xts, "strftime") else str(xts)
        print(f"{t['date']:<12}{t['side']:<5}{t['strike']:<8}"
              f"{t['entry']:>7.1f}{t['exit_price']:>7.1f}"
              f"{t['pnl_pts']:>+9.1f}{t['pnl_rs']:>+10.0f}  {t['reason']} ({ets_s}→{xts_s})")

    # ── Equity curve ─────────────────────────────────────────────────────────
    print("\nEquity curve (cumulative P&L):")
    cum = 0
    for _, t in df.iterrows():
        cum += t["pnl_rs"]
        bar = "█" * int(abs(cum) / 500) if abs(cum) > 0 else ""
        sign = "+" if cum >= 0 else "-"
        print(f"  {t['date']}  {sign}  ₹{cum:>+8,.0f}  {bar}")


# ── UI-callable entry point ──────────────────────────────────────────────────

def run_backtest_ui(
    token: str,
    days: int             = 10,
    lots: int             = 1,
    htf_min: int          = 15,
    mtf_min: int          = 5,
    eod_time: str         = "14:00",
    sq_off_time: str      = "15:15",
    min_zone_range: float = 30.0,
    max_trades_side: int  = 2,
    min_reward: float     = 20.0,
    target_mult: float    = 1.5,
) -> dict:
    """Called by FastAPI /api/backtest/sensex — returns JSON-serialisable dict."""
    global HTF_MIN, MTF_MIN, EOD_TIME, SQ_OFF_TIME, MIN_ZONE_RANGE, MAX_TRADES_PER_SIDE, MIN_REWARD, TARGET_MULT
    HTF_MIN          = htf_min
    MTF_MIN          = mtf_min
    EOD_TIME         = eod_time
    SQ_OFF_TIME      = sq_off_time
    MIN_ZONE_RANGE   = min_zone_range
    MAX_TRADES_PER_SIDE = max_trades_side
    MIN_REWARD       = min_reward
    TARGET_MULT      = target_mult

    # Load REGISTRY with SENSEX BSE_FO contracts (real expiry dates)
    from data_layer.instrument_registry import REGISTRY
    reg_err = ""
    try:
        REGISTRY.load_sync("SENSEX", token)
    except Exception as e:
        reg_err = str(e)

    debug_log = [
        f"REGISTRY loaded={REGISTRY.is_loaded('SENSEX')} err={reg_err}",
        f"expiries={REGISTRY._expiries.get('SENSEX', [])[:6]}",
    ]

    all_trades: list = []
    trading_days = _get_trading_days(days)

    for dt in trading_days:
        spot = _get_sensex_spot_open(dt, token)
        if not spot or spot <= 0:
            debug_log.append(f"{dt}: spot fetch failed")
            continue
        ce_strike = _round_ce(spot)
        pe_strike = _round_pe(spot)

        expiry = REGISTRY.get_active_expiry("SENSEX", from_date=dt) if REGISTRY.is_loaded("SENSEX") else None
        if expiry is None:
            _, expiry = _find_key_and_expiry(token, ce_strike, "CE", dt)
        if expiry is None:
            debug_log.append(f"{dt}: no expiry found")
            continue

        ce_key = REGISTRY.get_upstox_key("SENSEX", expiry, ce_strike, "CE") if REGISTRY.is_loaded("SENSEX") else None
        pe_key = REGISTRY.get_upstox_key("SENSEX", expiry, pe_strike, "PE") if REGISTRY.is_loaded("SENSEX") else None
        if not ce_key:
            ce_key, _ = _find_key_and_expiry(token, ce_strike, "CE", dt)
        if not pe_key:
            pe_key, _ = _find_key_and_expiry(token, pe_strike, "PE", dt)

        debug_log.append(f"{dt}: spot={spot:.0f} expiry={expiry} CE={ce_strike}/{bool(ce_key)} PE={pe_strike}/{bool(pe_key)}")

        if not ce_key and not pe_key:
            continue

        day_trades = _run_day(
            dt=dt, token=token, lots=lots,
            ce_key=ce_key or "", pe_key=pe_key or "",
            ce_strike=ce_strike, pe_strike=pe_strike,
        )
        all_trades.extend(day_trades)
        time.sleep(0.3)

    if not all_trades:
        return {"ok": True, "trades": [], "summary": {}, "equity": []}

    df = pd.DataFrame(all_trades)
    # Normalise timestamps to HH:MM strings now — avoids mixed-type issues in iterrows
    def _hhmm(v) -> str:
        if hasattr(v, "strftime"):
            return v.strftime("%H:%M")
        s = str(v)
        return s[11:16] if len(s) > 15 else s

    df["entry_hm"] = df["entry_ts"].apply(_hhmm)
    df["exit_hm"]  = df["exit_ts"].apply(_hhmm)

    wins   = len(df[df["pnl_rs"] > 0])
    losses = len(df[df["pnl_rs"] <= 0])
    total_pnl = float(df["pnl_rs"].sum())
    avg_win   = float(df[df["pnl_rs"] > 0]["pnl_rs"].mean()) if wins else 0.0
    avg_loss  = float(df[df["pnl_rs"] <= 0]["pnl_rs"].mean()) if losses else 0.0
    pf = abs(avg_win * wins / (avg_loss * losses)) if losses and avg_loss else None

    trades_out = []
    cum = 0.0
    equity = []
    for _, t in df.iterrows():
        cum += float(t["pnl_rs"])
        equity.append({"date": str(t["date"]), "cum_pnl": round(cum, 0)})
        trades_out.append({
            "date":       t["date"],
            "side":       t["side"],
            "strike":     int(t["strike"]),
            "entry_time": str(t["entry_hm"]),
            "exit_time":  str(t["exit_hm"]),
            "entry":      round(float(t["entry"]), 1),
            "exit":       round(float(t["exit_price"]), 1),
            "pnl_pts":    round(float(t["pnl_pts"]), 1),
            "pnl_rs":     round(float(t["pnl_rs"]), 0),
            "reason":     t["reason"],
            "zone":       t.get("htf_zone", ""),
        })

    return {
        "ok": True,
        "summary": {
            "total":      len(df),
            "wins":       wins,
            "losses":     losses,
            "win_rate":   round(wins / len(df) * 100, 1),
            "total_pnl":  round(total_pnl, 0),
            "avg_win":    round(avg_win, 0),
            "avg_loss":   round(avg_loss, 0),
            "profit_factor": round(pf, 2) if pf else None,
        },
        "trades": trades_out,
        "equity": equity,
        "debug": debug_log,
    }


# ── 3-level hierarchy backtest (75m pool → 15m CLOSED → 5m CLOSED → ENTRY) ──

def _simulate_exit(df1m: pd.DataFrame, entry_ts, entry_price: float,
                   sl: float, t1: float, sq_off: str, lot_size: int, lots: int) -> dict:
    """
    Walk 1m bars from entry_ts.
    At T1: book 50% (lot1), trail remaining lot with SL = entry (breakeven), exit at EOD/SL.
    Returns combined PnL across both legs.
    """
    sq_h, sq_m = map(int, sq_off.split(":"))
    qty_total = lots * lot_size
    qty_half  = qty_total // 2      # 50% at T1
    qty_rest  = qty_total - qty_half

    future = df1m[df1m["ts"] > entry_ts]
    t1_hit = False
    t1_ts  = None
    t1_pnl_rs = 0.0
    trailing_sl = sl   # before T1 = zone_low SL; after T1 = entry (breakeven)

    for _, bar in future.iterrows():
        ts   = bar["ts"]
        lo   = float(bar["low"])
        hi   = float(bar["high"])
        eod  = (ts.hour, ts.minute) >= (sq_h, sq_m)

        if eod:
            ep = float(bar["open"])
            if not t1_hit:
                pnl_pts = ep - entry_price
                return {"exit_ts": ts, "exit_price": ep,
                        "pnl_pts": round(pnl_pts, 2),
                        "pnl_rs": round(pnl_pts * qty_total, 0), "reason": "EOD"}
            # T1 already booked: exit rest at EOD open
            rest_pnl_pts = ep - entry_price
            total_rs = t1_pnl_rs + rest_pnl_pts * qty_rest
            avg_pts  = total_rs / qty_total
            return {"exit_ts": ts, "exit_price": ep,
                    "pnl_pts": round(avg_pts, 2),
                    "pnl_rs": round(total_rs, 0), "reason": "T1+EOD"}

        if lo <= trailing_sl:
            if not t1_hit:
                pnl_pts = trailing_sl - entry_price
                return {"exit_ts": ts, "exit_price": trailing_sl,
                        "pnl_pts": round(pnl_pts, 2),
                        "pnl_rs": round(pnl_pts * qty_total, 0), "reason": "SL"}
            # Rest stopped out at breakeven
            rest_pnl_pts = trailing_sl - entry_price   # = 0 at breakeven
            total_rs = t1_pnl_rs + rest_pnl_pts * qty_rest
            avg_pts  = total_rs / qty_total
            return {"exit_ts": ts, "exit_price": trailing_sl,
                    "pnl_pts": round(avg_pts, 2),
                    "pnl_rs": round(total_rs, 0), "reason": "T1+SL"}

        if not t1_hit and hi >= t1:
            t1_hit = True
            t1_ts  = ts
            t1_pnl_rs   = (t1 - entry_price) * qty_half
            trailing_sl = entry_price   # move SL to breakeven for rest

    # Last bar — no sq_off hit
    last = df1m.iloc[-1]
    ep   = float(last["close"])
    if not t1_hit:
        pnl_pts = ep - entry_price
        return {"exit_ts": last["ts"], "exit_price": ep,
                "pnl_pts": round(pnl_pts, 2),
                "pnl_rs": round(pnl_pts * qty_total, 0), "reason": "EOD"}
    rest_pnl_pts = ep - entry_price
    total_rs = t1_pnl_rs + rest_pnl_pts * qty_rest
    avg_pts  = total_rs / qty_total
    return {"exit_ts": last["ts"], "exit_price": ep,
            "pnl_pts": round(avg_pts, 2),
            "pnl_rs": round(total_rs, 0), "reason": "T1+EOD"}


def _collect_entries_3level(dt, df1m: pd.DataFrame, z75_pool: list,
                             cutoff: str = "15:10") -> list:
    """
    Run 3-level hierarchy on one day's 1m bars.
    Returns list of entry dicts: {entry_ts, entry_price, sl, t1, zone_label, side}
    side = 'BUY' (we always BUY the option in a seller trap).
    """
    from scripts.show_75m_zones import (
        resample, detect_zones, first_1m_entry, first_return_to_zone_high,
    )
    cut_h, cut_m = map(int, cutoff.split(":"))
    today_start  = pd.Timestamp(dt)
    mtf_15  = resample(df1m, 15)
    ltf_5   = resample(df1m,  5)
    day_start = df1m["ts"].iloc[0] - pd.Timedelta(minutes=1)

    entries = []
    seen_5m_zones: set = set()

    MIN_ZONE_WIDTH = 5.0   # skip degenerate zones with near-zero range

    def _try_5m(z15, ret15_ts):
        """Find CLOSED 5m zones inside z15 after ret15_ts → record entries."""
        if (z15["zone_high"] - z15["zone_low"]) < MIN_ZONE_WIDTH:
            return   # degenerate zero-width zone — skip
        zones_5 = [z for z in detect_zones(ltf_5)
                   if z["zone_high"] >= z15["zone_low"]
                   and z["zone_high"] <= z15["zone_high"]
                   and z["formed_ts"] >= ret15_ts]
        for z5 in zones_5:
            uid = f"{z5['formed_ts']}_{z5['zone_high']:.1f}"
            if uid in seen_5m_zones:
                continue
            ret5 = first_return_to_zone_high(df1m, z5, z5["sl_hit_ts"])
            if ret5 is None:
                continue
            entry_ts = ret5["entry_ts"]
            if (entry_ts.hour, entry_ts.minute) >= (cut_h, cut_m):
                continue
            seen_5m_zones.add(uid)
            entries.append({
                "entry_ts":    entry_ts,
                "entry_price": z5["zone_high"],
                "sl":          round(z15["zone_low"], 2),       # 15m zone_low (wider SL)
                "t1":          round(z15["sl_level"], 2),      # c0.high of 15m = sellers' SL = T1
                "zone_label":  f"15m {z15['zone_high']:.0f}→{z15['zone_low']:.0f}(sl={z15['sl_level']:.0f}) / 5m {z5['zone_high']:.0f}",
                "side":        "BUY",
            })

    active_75 = [z for z in z75_pool if z["sl_hit_ts"] < today_start]
    used_75 = False

    for z75 in active_75:
        entry_1m_ts = first_1m_entry(df1m, z75, day_start)
        if entry_1m_ts is None:
            continue
        used_75 = True
        zones_15 = [z for z in detect_zones(mtf_15)
                    if z["zone_high"] >= z75["zone_low"]
                    and z["zone_high"] <= z75["zone_high"]
                    and z["formed_ts"] >= entry_1m_ts]
        for z15 in zones_15:
            ret15 = first_return_to_zone_high(df1m, z15, z15["sl_hit_ts"])
            if ret15 is None:
                continue
            _try_5m(z15, ret15["entry_ts"])

    if not used_75:
        # CASCADE: any CLOSED 15m zone today
        for z15 in detect_zones(mtf_15):
            ret15 = first_return_to_zone_high(df1m, z15, z15["sl_hit_ts"])
            if ret15 is None:
                continue
            _try_5m(z15, ret15["entry_ts"])

    # Take only the first entry (earliest) to avoid over-trading
    entries.sort(key=lambda e: e["entry_ts"])
    return entries[:1]


def _resolve_day_strikes(dt: date, token: str, und: str = "SENSEX",
                          gap_thresh: float = 0.5, gap_near: int = 500,
                          step: int = 100) -> dict:
    """
    Replicate TrapScannerEngine morning-init strike selection for a past date.
    Returns {ce1_strike, pe1_strike, ce1_key, pe1_key, expiry, gap_fired, label}
    or empty dict on failure.
    """
    from data_layer.instrument_registry import REGISTRY
    import requests as _req

    # 1. Prev-day OHLC: fetch 1-min bars for the calendar day before dt
    prev_dt = dt - timedelta(days=1)
    while prev_dt.weekday() >= 5:
        prev_dt -= timedelta(days=1)

    idx_key = "BSE_INDEX|SENSEX"
    ds = prev_dt.strftime("%Y-%m-%d")
    try:
        url = f"https://api.upstox.com/v2/historical-candle/{idx_key}/1minute/{ds}/{ds}"
        r = _req.get(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}, timeout=15)
        candles = r.json().get("data", {}).get("candles", [])
    except Exception:
        candles = []
    if not candles:
        return {}

    prev_df = pd.DataFrame(candles, columns=["ts","open","high","low","close","vol","oi"])
    prev_H = float(prev_df["high"].max())
    prev_L = float(prev_df["low"].min())
    prev_C = float(prev_df["close"].iloc[-1])

    # 2. Today open: first bar of dt
    ds_today = dt.strftime("%Y-%m-%d")
    today_open = 0.0
    try:
        if dt == date.today():
            url2 = f"https://api.upstox.com/v2/historical-candle/intraday/{idx_key}/1minute"
        else:
            url2 = f"https://api.upstox.com/v2/historical-candle/{idx_key}/1minute/{ds_today}/{ds_today}"
        r2 = _req.get(url2, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}, timeout=15)
        c2 = r2.json().get("data", {}).get("candles", [])
        if c2:
            today_open = float(sorted(c2, key=lambda x: x[0])[0][1])  # first bar open
    except Exception:
        pass
    if today_open <= 0:
        today_open = prev_C

    # 3. Gap check
    gap_pct = abs(today_open - prev_C) / prev_C * 100 if prev_C > 0 else 0.0
    gap_fired = gap_pct >= gap_thresh

    # 4. Strike selection (mirror TrapScannerEngine)
    P = (prev_H + prev_L + prev_C) / 3
    r1 = 2 * P - prev_L
    s1 = 2 * P - prev_H
    atm = int(round(today_open / step)) * step

    if gap_fired:
        ce1_strike = atm - gap_near
        pe1_strike = atm + gap_near
        label = f"GAP {gap_pct:.1f}% → CE={ce1_strike} PE={pe1_strike}"
    else:
        ce1_strike = int(round(s1 / step)) * step
        pe1_strike = int(round(r1 / step)) * step
        label = f"No-gap (S1={s1:.0f}→{ce1_strike}, R1={r1:.0f}→{pe1_strike})"

    # 5. Expiry + key lookup — REGISTRY only has current/future contracts.
    #    For past dates use _find_key_and_expiry (Upstox search by strike+date).
    if not REGISTRY.is_loaded(und):
        try:
            REGISTRY.load_sync(und, token)
        except Exception:
            pass

    expiry = REGISTRY.get_active_expiry(und, from_date=dt) if REGISTRY.is_loaded(und) else None

    # Try REGISTRY key first (works for current expiry), fall back to API search for past dates
    ce1_key = REGISTRY.get_upstox_key(und, expiry, ce1_strike, "CE") if (REGISTRY.is_loaded(und) and expiry) else ""
    pe1_key = REGISTRY.get_upstox_key(und, expiry, pe1_strike, "PE") if (REGISTRY.is_loaded(und) and expiry) else ""

    if not ce1_key:
        ce1_key, expiry = _find_key_and_expiry(token, ce1_strike, "CE", dt)
    if not pe1_key:
        pe1_key, _ = _find_key_and_expiry(token, pe1_strike, "PE", dt)

    if not ce1_key and not pe1_key:
        return {}   # genuinely no data for this date

    return {
        "ce1_strike": ce1_strike, "pe1_strike": pe1_strike,
        "ce1_key": ce1_key or "", "pe1_key": pe1_key or "",
        "expiry": expiry, "gap_fired": gap_fired, "label": label,
    }


def run_backtest_3level_ui(
    token: str,
    days: int         = 10,
    lots: int         = 1,
    sl_buf: float     = 5.0,
    sq_off: str       = "15:20",
    cutoff: str       = "15:10",
    pool_days: int    = 15,
    ce_key: str       = "",   # kept for compat but auto-resolved if empty
    pe_key: str       = "",
) -> dict:
    """
    3-level hierarchy backtest (75m pool → 15m CLOSED → 5m CLOSED → ENTRY).
    Strikes auto-resolved per day using same pivot logic as TrapScannerEngine.
    """
    from scripts.show_75m_zones import fetch_1m, resample, detect_zones

    LOT_SIZE = 20
    trading_days = _get_trading_days(days)
    all_pool_days = _get_trading_days(days + pool_days)
    debug_log: list = []
    all_trades: list = []

    # Cache per-key bars across days to avoid re-fetching
    key_bar_cache: dict = {}

    def _get_bars(key: str, dt: date) -> pd.DataFrame:
        if (key, dt) not in key_bar_cache:
            key_bar_cache[(key, dt)] = fetch_1m(key, dt, token)
        return key_bar_cache[(key, dt)]

    for dt in trading_days:
        # Resolve strikes for this day
        if ce_key or pe_key:
            # Manual override (legacy / testing)
            day_ce_key = ce_key
            day_pe_key = pe_key
            day_label  = "manual keys"
        else:
            strikes = _resolve_day_strikes(dt, token)
            if not strikes:
                debug_log.append(f"{dt}: strike resolution failed")
                continue
            day_ce_key = strikes["ce1_key"]
            day_pe_key = strikes["pe1_key"]
            day_label  = strikes["label"]
        debug_log.append(f"{dt}: {day_label} CE={day_ce_key} PE={day_pe_key}")

        for side, scan_key in [("CE", day_ce_key), ("PE", day_pe_key)]:
            if not scan_key:
                debug_log.append(f"  {side}: no key")
                continue

            # Build 75m zone pool using pool_days of bars for this key
            z75_pool: list = []
            for pdt in all_pool_days:
                df_p = _get_bars(scan_key, pdt)
                if not df_p.empty:
                    for z in detect_zones(resample(df_p, 75)):
                        z75_pool.append(z)

            df1m = _get_bars(scan_key, dt)
            if df1m.empty:
                debug_log.append(f"  {dt} {side}: no 1m data")
                continue

            entries = _collect_entries_3level(dt, df1m, z75_pool, cutoff=cutoff)
            for e in entries:
                ep   = e["entry_price"]
                sl   = round(e["sl"] - sl_buf, 2)  # zone_low - buffer (same as TrapScanner)
                t1   = e["t1"]
                exit_info = _simulate_exit(df1m, e["entry_ts"], ep, sl, t1,
                                           sq_off, LOT_SIZE, lots)
                all_trades.append({
                    "date":       dt.isoformat(),
                    "side":       side,
                    "strike":     scan_key.split("|")[-1],
                    "key":        scan_key,
                    "entry_ts":   e["entry_ts"],
                    "entry":      round(ep, 2),
                    "sl":         sl,
                    "t1":         t1,
                    "exit_price": exit_info["exit_price"],
                    "exit_ts":    exit_info["exit_ts"],
                    "pnl_pts":    exit_info["pnl_pts"],
                    "pnl_rs":     exit_info["pnl_rs"],
                    "reason":     exit_info["reason"],
                    "htf_zone":   e["zone_label"],
                })

    if not all_trades:
        return {"ok": True, "trades": [], "summary": {}, "equity": [], "debug": debug_log}

    all_trades.sort(key=lambda t: (t["date"], str(t["entry_ts"])))
    df = pd.DataFrame(all_trades)

    def _hhmm(v) -> str:
        if hasattr(v, "strftime"):
            return v.strftime("%H:%M")
        s = str(v); return s[11:16] if len(s) > 15 else s

    df["entry_hm"] = df["entry_ts"].apply(_hhmm)
    df["exit_hm"]  = df["exit_ts"].apply(_hhmm)

    wins      = len(df[df["pnl_rs"] > 0])
    losses    = len(df[df["pnl_rs"] <= 0])
    total_pnl = float(df["pnl_rs"].sum())
    avg_win   = float(df[df["pnl_rs"] > 0]["pnl_rs"].mean()) if wins else 0.0
    avg_loss  = float(df[df["pnl_rs"] <= 0]["pnl_rs"].mean()) if losses else 0.0
    pf        = abs(avg_win * wins / (avg_loss * losses)) if losses and avg_loss else None

    trades_out, equity, cum = [], [], 0.0
    for _, t in df.iterrows():
        cum += float(t["pnl_rs"])
        equity.append({"date": str(t["date"]), "cum_pnl": round(cum, 0)})
        trades_out.append({
            "date":       t["date"],
            "side":       t["side"],
            "strike":     str(t["strike"]),
            "entry_time": str(t["entry_hm"]),
            "exit_time":  str(t["exit_hm"]),
            "entry":      round(float(t["entry"]), 1),
            "exit":       round(float(t["exit_price"]), 1),
            "sl":         round(float(t["sl"]), 1),
            "t1":         round(float(t["t1"]), 1),
            "pnl_pts":    round(float(t["pnl_pts"]), 1),
            "pnl_rs":     round(float(t["pnl_rs"]), 0),
            "reason":     t["reason"],
            "zone":       t.get("htf_zone", ""),
        })

    return {
        "ok": True,
        "summary": {
            "total":         len(df),
            "wins":          wins,
            "losses":        losses,
            "win_rate":      round(wins / len(df) * 100, 1),
            "total_pnl":     round(total_pnl, 0),
            "avg_win":       round(avg_win, 0),
            "avg_loss":      round(avg_loss, 0),
            "profit_factor": round(pf, 2) if pf else None,
        },
        "trades":  trades_out,
        "equity":  equity,
        "debug":   debug_log,
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SENSEX Intraday Backtest 15m/5m/1m")
    parser.add_argument("--token", required=True, help="Upstox access token")
    parser.add_argument("--days",  type=int, default=10, help="Number of trading days to backtest")
    parser.add_argument("--lots",  type=int, default=1,  help="Number of lots per trade")
    parser.add_argument("--sl-buffer", type=float, default=0.0,
                        help="SL buffer pts below zone_low (default 0)")
    parser.add_argument("--target-mult", type=float, default=1.5,
                        help="Target = zone_high + range×mult (default 1.5)")
    args = parser.parse_args()

    SL_BUFFER   = args.sl_buffer
    TARGET_MULT = args.target_mult

    run(token=args.token, days=args.days, lots=args.lots)
