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
EOD_TIME         = "15:15"      # square-off time
SL_BUFFER        = 0.0          # pts below zone_low for SL (0 = zone_low itself)
TARGET_MULT      = 1.5          # target = zone_low + (zone_high-zone_low)*TARGET_MULT
LOT_SIZE         = 10           # SENSEX lot size

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
    df["ts"] = pd.to_datetime(df["ts"])
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
            key = REGISTRY.get_instrument_key("SENSEX", expiry_date, strike, side)
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
    Seller-trap on HTF (15m) bars:
    For each candle, a BEAR trap forms when:
      - Price dips into the candle range (low ≤ close ≤ high of reference candle)
      - Then recovers above candle high → sellers trapped
    Returns list of zones with zone_low, zone_high, zone_trigger, target.
    """
    zones = []
    for i in range(1, len(htf)):
        ref = htf.iloc[i - 1]   # reference candle (LIFO — previous candle)
        curr = htf.iloc[i]
        zone_low  = round(float(ref["low"]),  2)
        zone_high = round(float(ref["high"]), 2)
        if zone_high <= zone_low:
            continue
        zone_range = zone_high - zone_low
        # Trigger: 33% into the zone from low (similar to live engine)
        trigger = round(zone_low + zone_range * 0.33, 2)
        target  = round(zone_high + zone_range * TARGET_MULT, 2)
        sl      = round(zone_low - SL_BUFFER, 2)
        zones.append({
            "ref_ts":      ref["ts"] if "ts" in ref.index else htf.index[i - 1],
            "zone_ts":     curr["ts"] if "ts" in curr.index else htf.index[i],
            "zone_low":    zone_low,
            "zone_high":   zone_high,
            "zone_trigger":trigger,
            "target":      target,
            "sl":          sl,
            "state":       "WATCH",
        })
    return zones


def _advance_zone_state(zone: dict, price: float) -> dict:
    """
    WATCH → (price dips ≤ zone_low) → SELLERS_IN
          → (price rises ≥ zone_high) → TRAPPED
          → (price drops ≤ zone_low) → ENTRY_READY
    """
    z = zone.copy()
    st = z["state"]
    if st == "WATCH":
        if price <= z["zone_low"]:
            z["state"] = "SELLERS_IN"
    elif st == "SELLERS_IN":
        if price >= z["zone_high"]:
            z["state"] = "TRAPPED"
    elif st == "TRAPPED":
        if price <= z["zone_low"]:
            z["state"] = "ENTRY_READY"
    return z


# ── Main per-day simulation ───────────────────────────────────────────────────

def _run_day(dt: date, token: str, lots: int,
             ce_key: str, pe_key: str,
             ce_strike: int, pe_strike: int) -> list[dict]:
    """
    Run one trading day. Returns list of trade dicts (one per trade).
    """
    trades = []

    for side, key, strike in [("CE", ce_key, ce_strike), ("PE", pe_key, pe_strike)]:
        df1m = _fetch_1m(key, dt, token)
        if df1m.empty or len(df1m) < 20:
            print(f"    [{side} {strike}] insufficient bars ({len(df1m)}), skip")
            continue

        # Build HTF (15m) and MTF (5m) from 1m
        htf = _resample(df1m, HTF_MIN)
        mtf = _resample(df1m, MTF_MIN)

        if htf.empty or len(htf) < 2:
            print(f"    [{side} {strike}] not enough HTF candles, skip")
            continue

        opening_price = float(df1m.iloc[0]["open"])
        print(f"    [{side} {strike}] bars={len(df1m)} open={opening_price:.1f}")

        # Detect initial zones from HTF
        zones = _detect_seller_trap_zones(htf)

        # State machine
        position    = None   # dict when in trade
        htf_idx     = 0      # which HTF candle we're up to
        active_zone = None   # zone that passed HTF + 5m confirmation
        mtf_confirmed = False

        eod = pd.Timestamp(f"{dt} {EOD_TIME}")

        for _, bar1m in df1m.iterrows():
            ts   = bar1m["ts"]
            ltp  = float(bar1m["close"])
            high1m = float(bar1m["high"])
            low1m  = float(bar1m["low"])

            if ts >= eod:
                # EOD square-off
                if position:
                    pnl = (ltp - position["entry"]) * position["qty"]
                    trades.append({**position,
                                   "exit_ts": ts, "exit_price": ltp,
                                   "pnl_pts": ltp - position["entry"],
                                   "pnl_rs":  pnl, "reason": "EOD"})
                    position = None
                break

            # ── If in position: check SL / target ────────────────────────────
            if position:
                if low1m <= position["sl"]:
                    pnl = (position["sl"] - position["entry"]) * position["qty"]
                    trades.append({**position,
                                   "exit_ts": ts, "exit_price": position["sl"],
                                   "pnl_pts": position["sl"] - position["entry"],
                                   "pnl_rs":  pnl, "reason": "SL"})
                    position      = None
                    active_zone   = None
                    mtf_confirmed = False
                elif high1m >= position["target"]:
                    pnl = (position["target"] - position["entry"]) * position["qty"]
                    trades.append({**position,
                                   "exit_ts": ts, "exit_price": position["target"],
                                   "pnl_pts": position["target"] - position["entry"],
                                   "pnl_rs":  pnl, "reason": "TARGET"})
                    position      = None
                    active_zone   = None
                    mtf_confirmed = False
                continue   # don't look for new entry while in trade

            # ── Advance zone states on each 1m close ─────────────────────────
            zones = [_advance_zone_state(z, ltp) for z in zones]

            # ── Step 1: HTF gate — find a TRAPPED zone ────────────────────────
            # Update zones list with newly-completed HTF candles
            htf_ts_list = list(htf["ts"]) if "ts" in htf.columns else list(htf.index)
            while htf_idx < len(htf) and htf_ts_list[htf_idx] <= ts:
                htf_idx += 1
            zones_ready = [z for z in zones if z["state"] == "ENTRY_READY"]

            if not zones_ready:
                continue   # no HTF zone ready yet

            # Use the most recent ENTRY_READY zone
            best_zone = zones_ready[-1]

            # ── Step 2: 5m MTF confirmation ───────────────────────────────────
            # 5m candle containing current ts
            mtf_now = mtf[mtf["ts"] <= ts]
            if mtf_now.empty:
                continue
            last_5m = mtf_now.iloc[-1]
            last_5m_close = float(last_5m["close"])

            # 5m must close INSIDE the zone (zone_low ≤ 5m_close ≤ zone_high)
            in_zone = best_zone["zone_low"] <= last_5m_close <= best_zone["zone_high"]
            if not in_zone:
                continue

            # ── Step 3: 1m buy signal ─────────────────────────────────────────
            # 1m close > prev 1m high (bullish breakout on 1m)
            bar_idx = bar1m.name
            if bar_idx < 1:
                continue
            prev1m = df1m.iloc[bar_idx - 1]
            buy_signal = ltp > float(prev1m["high"])

            if not buy_signal:
                continue

            # All 3 conditions met — ENTER at next bar's open
            next_idx = bar_idx + 1
            if next_idx >= len(df1m):
                continue
            entry_bar  = df1m.iloc[next_idx]
            entry_price = float(entry_bar["open"])
            entry_ts    = entry_bar["ts"]

            if entry_ts >= eod:
                continue

            qty = lots * LOT_SIZE
            position = {
                "date":        dt.isoformat(),
                "side":        side,
                "strike":      strike,
                "key":         key,
                "entry_ts":    entry_ts,
                "entry":       entry_price,
                "sl":          best_zone["sl"],
                "target":      best_zone["target"],
                "qty":         qty,
                "htf_zone":    f"{best_zone['zone_low']}→{best_zone['zone_high']}",
            }
            print(f"      ENTRY {entry_ts.strftime('%H:%M')} {side} {strike} "
                  f"@ {entry_price:.1f}  SL={best_zone['sl']:.1f}  T={best_zone['target']:.1f}")

        # EOD close if still open
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
            key = REGISTRY.get_instrument_key(underlying, expiry, strike, side)
            return key
    except Exception:
        pass
    return None


def _find_option_key_upstox_search(token: str, strike: int, side: str,
                                   expiry: date) -> Optional[str]:
    """Search Upstox instruments API for SENSEX option key."""
    # Upstox instrument search for BSE_FO SENSEX options
    expiry_str = expiry.strftime("%d%b%y").upper()   # e.g. 23JUN26 → but API uses YYYY-MM-DD
    try:
        url = "https://api.upstox.com/v2/instruments/search"
        params = {
            "exchange": "BSE_FO",
            "segment":  "BSE_FO",
            "query":    f"SENSEX {strike} {side} {expiry.strftime('%d%b%y').upper()}"
        }
        r = requests.get(url, params=params, headers=_headers(token), timeout=10)
        d = r.json()
        items = d.get("data", [])
        if items:
            return items[0].get("instrument_key", None)
    except Exception:
        pass
    return None


def run(token: str, days: int = 10, lots: int = 1) -> None:
    print(f"\nSENSEX Intraday Backtest  |  {days} days  |  {lots} lot(s)")
    print(f"Strategy: 15m HTF zone → 5m confirm → 1m buy signal")
    print(f"Strike: nearest {STRIKE_STEP} (CE=below, PE=above market open)")
    print("=" * 70)

    trading_days = _get_trading_days(days)
    all_trades: list[dict] = []

    # Try to load REGISTRY for instrument key lookup
    try:
        from data_layer.instrument_registry import REGISTRY
        from config.global_config import GlobalConfig
        cfg = GlobalConfig()
        import asyncio
        asyncio.run(REGISTRY.load_all(cfg))
        print("REGISTRY loaded.\n")
    except Exception as exc:
        print(f"REGISTRY not available ({exc}) — will use Upstox search.\n")

    for dt in trading_days:
        print(f"\n── {dt} ──────────────────────────────────────────────────")

        # Get SENSEX spot open
        spot = _get_sensex_spot_open(dt, token)
        if spot <= 0:
            print(f"  Could not fetch SENSEX spot for {dt}, skip")
            continue

        ce_strike = _round_ce(spot)
        pe_strike = _round_pe(spot)
        print(f"  Spot open={spot:.0f}  CE={ce_strike}  PE={pe_strike}")

        # Find expiry (nearest Thursday for SENSEX weekly, or monthly)
        # SENSEX weekly = every Friday (BSE), or use REGISTRY
        expiry = None
        try:
            from data_layer.instrument_registry import REGISTRY
            if REGISTRY.is_loaded("SENSEX"):
                expiry = REGISTRY.get_active_expiry("SENSEX", from_date=dt)
        except Exception:
            pass
        if expiry is None:
            # Fallback: next Friday
            days_to_fri = (4 - dt.weekday()) % 7
            expiry = dt + timedelta(days=days_to_fri if days_to_fri else 7)
        print(f"  Expiry: {expiry}")

        # Get instrument keys
        ce_key = _find_option_key_from_registry("SENSEX", expiry, ce_strike, "CE")
        pe_key = _find_option_key_from_registry("SENSEX", expiry, pe_strike, "PE")

        if not ce_key:
            ce_key = _find_option_key_upstox_search(token, ce_strike, "CE", expiry)
        if not pe_key:
            pe_key = _find_option_key_upstox_search(token, pe_strike, "PE", expiry)

        if not ce_key and not pe_key:
            print(f"  Could not resolve instrument keys for {dt}, skip")
            print(f"  Hint: add BSE_FO keys manually below for testing")
            continue

        print(f"  CE key: {ce_key or 'NOT FOUND'}")
        print(f"  PE key: {pe_key or 'NOT FOUND'}")

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
