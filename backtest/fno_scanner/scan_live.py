"""
backtest/fno_scanner/scan_live.py  —  FnO Live Zone Scanner
=============================================================
Dynamically discovers all NSE FnO stocks (~200) from the Upstox instrument
master, fetches their D1 history, and scans for active trap zones.

Reports:
  - TRIGGERED  : last bar already retested the zone (enter at next open)
  - APPROACHING: zone entry_line within 1.5% of last close (watch next session)

Output: ranked list + suggested monthly expiry option entry for each signal.

Usage:
    python backtest/fno_scanner/scan_live.py [--save] [--top-n 5]
    (token auto-loaded from data/clients.db — no env var needed)
"""
from __future__ import annotations

import gzip, json, os, sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from config.global_config import IST
from backtest.fno_scanner.backtest import load_or_fetch, Bar
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones

# NSE index underlyings that appear in NSE_FO but are NOT stocks.
# We skip these — they have no NSE_EQ entry and we don't trade index options here.
_INDEX_UNDERLYINGS: Set[str] = {
    "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX",
    "BANKEX", "NIFTY50", "NIFTYBANK", "NIFTYFIN", "NIFTYMID",
    "NIFTYNXT50", "NIFTYNXT", "NIFTYIT",
}


@dataclass
class _FnoUniverse:
    stocks:   Dict[str, str]       # symbol → NSE_EQ instrument_key
    expiries: Dict[str, List[date]] # symbol → sorted future expiry dates
    lot_sizes: Dict[str, int]       # symbol → lot size


def _fetch_fno_universe(token: str) -> _FnoUniverse:
    """Download NSE instrument master once and extract the full FnO stock universe.

    Returns all stocks (not indices) that have active CE/PE contracts, together
    with their NSE_EQ instrument key (needed for D1 data), lot sizes, and expiry dates.
    Stocks with no NSE_EQ entry are skipped (they are indices or delisted).
    """
    from curl_cffi import requests as cc
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    try:
        r = cc.get(url, impersonate="chrome131", timeout=30)
        instruments = json.loads(gzip.decompress(r.content))
    except Exception as e:
        print(f"  [warn] Could not fetch instrument master: {e}")
        return _FnoUniverse({}, {}, {})

    today = date.today()

    # ── Step 1: build NSE_EQ map  symbol → instrument_key ──────────────────
    eq_map: Dict[str, str] = {}
    for inst in instruments:
        if inst.get("segment") != "NSE_EQ":
            continue
        key = inst.get("instrument_key", "")
        sym = (inst.get("trading_symbol") or inst.get("short_name") or "").strip().upper()
        if sym and key:
            eq_map[sym] = key

    # ── Step 2: scan NSE_FO CE/PE contracts for underlyings, lots, expiries ─
    fno_underlyings: Set[str] = set()
    lot_sizes: Dict[str, int] = {}
    expiries: Dict[str, List[date]] = {}

    for inst in instruments:
        if inst.get("segment") != "NSE_FO":
            continue
        if inst.get("instrument_type") not in ("CE", "PE"):
            continue
        ts = inst.get("trading_symbol", "")
        parts = ts.split()
        if len(parts) < 6:
            continue
        underlying = parts[0].upper()
        if underlying in _INDEX_UNDERLYINGS:
            continue

        # Lot size
        ls = int(inst.get("lot_size") or 0)
        if ls > 0:
            # Keep the minimum seen (all series for same underlying should match)
            if underlying not in lot_sizes or ls < lot_sizes[underlying]:
                lot_sizes[underlying] = ls

        # Expiry date
        try:
            exp_date = datetime.strptime(f"{parts[3]} {parts[4]} {parts[5]}", "%d %b %y").date()
        except ValueError:
            continue
        if exp_date < today:
            continue

        fno_underlyings.add(underlying)
        expiries.setdefault(underlying, [])
        if exp_date not in expiries[underlying]:
            expiries[underlying].append(exp_date)

    # ── Step 3: keep only stocks that have an NSE_EQ entry ─────────────────
    stocks: Dict[str, str] = {}
    for sym in fno_underlyings:
        eq_key = eq_map.get(sym)
        if eq_key:
            stocks[sym] = eq_key

    for sym in expiries:
        expiries[sym].sort()

    return _FnoUniverse(stocks=stocks, expiries=expiries, lot_sizes=lot_sizes)


def _next_monthly_expiry(expiries: List[date], from_month: int, from_year: int) -> Optional[date]:
    """Return the last (furthest) expiry in the given calendar month."""
    candidates = [d for d in expiries if d.month == from_month and d.year == from_year]
    return max(candidates) if candidates else None


# Best config from the parameter sweep
HARD_SL_BUF   = 0.8    # % beyond zone boundary
MIN_RR        = 1.5    # minimum R:R
APPROACH_PCT  = 1.5    # % distance to call a zone "approaching"
MAX_ZONE_AGE  = 30     # days — 1-month lookback; zones older than this are stale


EARNINGS_MOVE_PCT = 4.5   # single D1 bar move above this % = likely earnings event
EARNINGS_LOOKBACK = 5    # check last N bars for earnings spikes

@dataclass
class Signal:
    symbol:      str
    direction:   str        # "CE" (bear trap -> long) or "PE" (bull trap -> short)
    status:      str        # "TRIGGERED" | "APPROACHING" | "FLIP"
    entry_line:  float      # zone level to buy at
    current:     float      # last close
    dist_pct:    float      # % distance of close from entry_line
    hard_sl:     float      # spot SL level
    day_t1:      float      # Day-1 target
    zone_age:    int        # days since zone locked
    lock_date:   str        # date zone was locked e.g. "24 Jul"
    rr:          float      # zone R:R (entry_line → T1 / entry_line → SL)
    btst_rr:     float      # BTST R:R (current close → T1 / current close → SL)
    suggested_strike: int   # nearest round-step to entry_line
    expiry:      str = ""   # actual contract expiry from registry e.g. "28 AUG 26"
    upstox_key:  str = ""   # NSE_EQ instrument key for the underlying spot
    zone_lo:     float = 0.0   # zone boundary (0.0 if not populated -- see load_watchlist)
    zone_hi:     float = 0.0   # zone boundary


def _nearest_strike(price: float, step: int = 50) -> int:
    return round(price / step) * step


def _step_for(price: float) -> int:
    if price > 5000:
        return 100
    elif price > 2000:
        return 50
    elif price > 500:
        return 20
    return 10


def _itm_strike(price: float, direction: str) -> int:
    """1-strike ITM, matching the D1Trap/FVG convention: CE ITM = strike
    BELOW spot, PE ITM = strike ABOVE spot -- one step off the ATM strike,
    not plain ATM (2026-08-03, user-requested parity with the intraday
    option-buyer strategies)."""
    step = _step_for(price)
    atm = _nearest_strike(price, step)
    return atm - step if direction == "CE" else atm + step


EARNINGS_CUMULATIVE_PCT = 8.0  # 5-day cumulative move above this % = likely earnings rally

def _has_recent_earnings(bars: list, lookback: int = EARNINGS_LOOKBACK,
                         threshold_pct: float = EARNINGS_MOVE_PCT) -> bool:
    """True if recent bars show signs of an earnings-driven move:
    1. Any single D1 bar with close-to-close move > threshold_pct% (gap-style earnings), OR
    2. Cumulative 5-day close-to-close move > EARNINGS_CUMULATIVE_PCT% (gradual earnings rally).
    """
    tail = bars[-lookback:] if len(bars) >= lookback else bars
    if len(tail) < 2:
        return False
    # Check 1: single-bar spike
    for i in range(1, len(tail)):
        prev_c = tail[i - 1].close
        if prev_c <= 0:
            continue
        if abs((tail[i].close - prev_c) / prev_c * 100) >= threshold_pct:
            return True
    # Check 2: cumulative multi-day drift (earnings beat spread over several sessions)
    base_c = tail[0].close
    last_c = tail[-1].close
    if base_c > 0 and abs((last_c - base_c) / base_c * 100) >= EARNINGS_CUMULATIVE_PCT:
        return True
    return False


def _check_flip(
    symbol: str,
    last_bar,
    bear_zones: list,
    bull_zones: list,
    today,
    universe: "_FnoUniverse",
    target_month: int,
    target_year: int,
) -> "Optional[Signal]":
    """Check if any zone was broken in the wrong direction, generating a flip signal.

    Bear zone (CE) broken DOWNWARD → zone_lo now resistance → PE flip
    Bull zone (PE) broken UPWARD   → zone_hi now support    → CE flip

    Only the best flip (by btst_rr, must be >= MIN_RR) within APPROACH_PCT is returned.
    """
    best_flip: Optional[Signal] = None

    all_zones = [(z, "CE") for z in bear_zones] + [(z, "PE") for z in bull_zones]
    for zone, original_dir in all_zones:
        if zone.entry_line is None or zone.sweep_low is None or zone.lock_ts is None:
            continue
        age_days = (today - zone.lock_ts.date()).days
        if age_days > MAX_ZONE_AGE:
            continue

        entry_line = zone.entry_line
        sweep_ref  = zone.sweep_low
        zone_lo = min(entry_line, sweep_ref)
        zone_hi = max(entry_line, sweep_ref)
        if zone_lo <= 0 or zone_hi <= 0:
            continue

        lock_date_str = zone.lock_ts.strftime("%d %b").lstrip("0")

        if original_dir == "CE":
            # Bear zone (CE) broken downward: close < zone_lo → zone_lo = new resistance → PE flip
            if last_bar.close >= zone_lo:
                continue  # not broken
            flip_dir    = "PE"
            flip_entry  = zone_lo          # old support = new resistance
            flip_sl     = zone_lo * (1 + HARD_SL_BUF / 100)
            flip_t1     = last_bar.low     # intraday low as forward target
            dist_pct    = (flip_entry - last_bar.close) / flip_entry * 100  # positive = below resistance
            flip_reward = flip_entry - flip_t1
            flip_risk   = flip_sl - flip_entry
        else:
            # Bull zone (PE) broken upward: close > zone_hi → zone_hi = new support → CE flip
            if last_bar.close <= zone_hi:
                continue  # not broken
            flip_dir    = "CE"
            flip_entry  = zone_hi          # old resistance = new support
            flip_sl     = zone_hi * (1 - HARD_SL_BUF / 100)
            flip_t1     = last_bar.high    # intraday high as forward target
            dist_pct    = (last_bar.close - flip_entry) / flip_entry * 100  # positive = above support
            flip_reward = flip_t1 - flip_entry
            flip_risk   = flip_entry - flip_sl

        if flip_risk <= 0 or flip_reward <= 0:
            continue
        rr = flip_reward / flip_risk
        if rr < MIN_RR:
            continue

        # Only show as APPROACHING if within APPROACH_PCT of the flip level
        if dist_pct > APPROACH_PCT:
            continue

        btst_reward = (last_bar.close - flip_t1) if flip_dir == "PE" else (flip_t1 - last_bar.close)
        btst_risk   = (flip_sl - last_bar.close) if flip_dir == "PE" else (last_bar.close - flip_sl)
        btst_rr     = (btst_reward / btst_risk) if btst_risk > 0 else 0.0

        strike = _itm_strike(flip_entry, flip_dir)

        sym_expiries = universe.expiries.get(symbol, [])
        exp_date = _next_monthly_expiry(sym_expiries, target_month, target_year)
        expiry_str = (f"{exp_date.day} {exp_date.strftime('%b %y').upper()}"
                      if exp_date else f"? {target_month}/{target_year}")

        sig = Signal(
            symbol=symbol, direction=flip_dir, status="FLIP",
            entry_line=flip_entry, current=last_bar.close,
            dist_pct=dist_pct, hard_sl=flip_sl, day_t1=flip_t1,
            zone_age=age_days, lock_date=lock_date_str, rr=rr,
            btst_rr=btst_rr,
            suggested_strike=strike, expiry=expiry_str,
            upstox_key=universe.stocks.get(symbol, ""),
        )

        if best_flip is None or sig.btst_rr > best_flip.btst_rr:
            best_flip = sig

    return best_flip


def scan(token: str) -> Tuple[List[Signal], _FnoUniverse]:
    # Include today's bar if market has closed (after 15:31 IST), else use yesterday.
    from datetime import datetime as _dt
    _now_ist = _dt.now(IST)
    if _now_ist.hour > 15 or (_now_ist.hour == 15 and _now_ist.minute >= 31):
        end_date = date.today()
    else:
        end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=31)   # 1-month D1 lookback

    # Determine which month to target for positional trading.
    # If we're in the last week of current month (expiry within 7 days),
    # target NEXT month; otherwise target current month.
    today = date.today()
    # Find next month's expiry target: use 2 months ahead if within last week
    target_month = today.month + 1 if today.day >= 24 else today.month
    target_year  = today.year
    if target_month > 12:
        target_month = 1
        target_year += 1

    print(f"\nFetching NSE FnO universe from Upstox instrument master...")
    universe = _fetch_fno_universe(token)
    print(f"  {len(universe.stocks)} FnO stocks discovered (with NSE_EQ data key)")

    signals: List[Signal] = []

    print(f"\nFnO Live Scanner  --  data up to {end_date}  |  target expiry month: {target_month}/{target_year}")
    print(f"{'─'*70}")

    earnings_skipped: List[str] = []

    for symbol, key in sorted(universe.stocks.items()):
        bars = load_or_fetch(symbol, key, token, start_date, end_date)
        if len(bars) < 20:
            print(f"  {symbol:<14} insufficient data — skip")
            continue

        # Earnings filter: if any recent bar had a >4.5% single-day move, skip.
        # These stocks have zone logic invalidated by earnings surprises (e.g. INFY +4.5%).
        if _has_recent_earnings(bars):
            earnings_skipped.append(symbol)
            print(f"  {symbol:<14} SKIP — recent earnings move >={EARNINGS_MOVE_PCT}%")
            continue

        last_bar = bars[-1]
        today    = last_bar.timestamp.date()

        # All confirmed zones in full history
        bear_zones = find_all_bear_zones(bars)
        bull_zones = find_all_bull_zones(bars)
        all_zones  = [(z, "CE") for z in bear_zones] + [(z, "PE") for z in bull_zones]

        best: Optional[Signal] = None

        for zone, direction in all_zones:
            if zone.entry_line is None or zone.sweep_low is None:
                continue
            if zone.lock_ts is None:
                continue

            age_days = (today - zone.lock_ts.date()).days
            if age_days > MAX_ZONE_AGE:
                continue

            entry_line = zone.entry_line
            sweep_ref  = zone.sweep_low   # sweep_low stores sweep-extreme for both directions

            # zone bounds
            zone_lo = min(entry_line, sweep_ref)
            zone_hi = max(entry_line, sweep_ref)

            if zone_lo <= 0 or zone_hi <= 0:
                continue

            # SL and T1
            if direction == "CE":          # bear trap -> long
                hard_sl   = zone_lo * (1 - HARD_SL_BUF / 100)
                day_t1    = last_bar.high
                risk      = entry_line - hard_sl
                reward    = day_t1 - entry_line
                retest    = last_bar.low <= entry_line and last_bar.close >= zone_lo
                dist_pct  = (last_bar.close - entry_line) / entry_line * 100  # positive = above
                # BTST R:R: enter at current close, target same T1, SL same hard_sl
                btst_reward = day_t1 - last_bar.close
                btst_risk   = last_bar.close - hard_sl
            else:                          # bull trap -> short
                hard_sl   = zone_hi * (1 + HARD_SL_BUF / 100)
                day_t1    = last_bar.low
                risk      = hard_sl - entry_line
                reward    = entry_line - day_t1
                retest    = last_bar.high >= entry_line and last_bar.close <= zone_hi
                dist_pct  = (entry_line - last_bar.close) / entry_line * 100  # positive = below
                # BTST R:R: enter at current close, target same T1, SL same hard_sl
                btst_reward = last_bar.close - day_t1
                btst_risk   = hard_sl - last_bar.close

            if risk <= 0 or reward < 0:
                continue

            rr = reward / risk
            if rr < MIN_RR:
                continue

            # BTST R:R from current close (realistic next-day entry price, not zone entry)
            btst_rr = (btst_reward / btst_risk) if btst_risk > 0 else 0.0

            # Skip if last bar's close has already consumed >80% of entry→T1 headroom.
            # This prevents "stale T1" signals where the stock already ran most of the
            # trade in a previous session and has almost no upside left.
            if direction == "CE":
                t1_room_consumed = (last_bar.close - entry_line) / (day_t1 - entry_line) if (day_t1 - entry_line) > 0 else 1.0
            else:
                t1_room_consumed = (entry_line - last_bar.close) / (entry_line - day_t1) if (entry_line - day_t1) > 0 else 1.0
            if t1_room_consumed > 0.8:
                continue

            # Classify status
            # dist_pct is always POSITIVE when stock is approaching from the correct side:
            #   CE: dist_pct = (close - entry_line)/entry_line → positive = close ABOVE entry (bear zone support, approaching from above) ✓
            #   PE: dist_pct = (entry_line - close)/entry_line → positive = close BELOW entry (bull zone resistance, approaching from below) ✓
            # Negative dist_pct means stock already moved PAST the zone in the wrong direction → skip.
            if retest:
                status = "TRIGGERED"
            elif 0 < dist_pct <= APPROACH_PCT:
                status = "APPROACHING"
            else:
                continue

            # 1-strike ITM (matches D1Trap/FVG convention), not plain ATM.
            strike = _itm_strike(entry_line, direction)

            # Real expiry from instrument master
            sym_expiries = universe.expiries.get(symbol, [])
            exp_date = _next_monthly_expiry(sym_expiries, target_month, target_year)
            if exp_date:
                expiry_str = f"{exp_date.day} {exp_date.strftime('%b %y').upper()}"
            else:
                expiry_str = f"? {target_month}/{target_year}"

            lock_date_str = (zone.lock_ts.strftime("%d %b").lstrip("0") if zone.lock_ts else "?")

            sig = Signal(
                symbol=symbol, direction=direction, status=status,
                entry_line=entry_line, current=last_bar.close,
                dist_pct=dist_pct, hard_sl=hard_sl, day_t1=day_t1,
                zone_age=age_days, lock_date=lock_date_str, rr=rr,
                btst_rr=btst_rr,
                suggested_strike=strike, expiry=expiry_str,
                upstox_key=key,
            )

            # Keep the signal with best R:R per stock
            # Priority: TRIGGERED > APPROACHING > FLIP
            if best is None:
                best = sig
            elif sig.status == "TRIGGERED" and best.status != "TRIGGERED":
                best = sig
            elif sig.status == best.status and sig.rr > best.rr:
                best = sig

        # ── FLIP detection: zones broken in wrong direction → role-reverse ────
        # When a bear zone (CE setup) is broken DOWNWARD (close < zone_lo):
        #   zone_lo was support → now becomes RESISTANCE → flip to PE
        # When a bull zone (PE setup) is broken UPWARD (close > zone_hi):
        #   zone_hi was resistance → now becomes SUPPORT → flip to CE
        # Only generate a flip if no primary signal was found for this stock.
        if best is None:
            flip_sig = _check_flip(
                symbol, last_bar, bear_zones, bull_zones, today,
                universe, target_month, target_year,
            )
            if flip_sig:
                best = flip_sig

        if best:
            signals.append(best)

    if earnings_skipped:
        print(f"\n  Earnings-excluded (zone invalidated by recent >{EARNINGS_MOVE_PCT}% move): {', '.join(earnings_skipped)}")

    return signals, universe


def print_report(signals: List[Signal]) -> None:
    triggered   = [s for s in signals if s.status == "TRIGGERED"]
    approaching = [s for s in signals if s.status == "APPROACHING"]
    flipped     = [s for s in signals if s.status == "FLIP"]

    approaching.sort(key=lambda s: s.btst_rr, reverse=True)
    triggered.sort(key=lambda s: s.btst_rr, reverse=True)
    flipped.sort(key=lambda s: s.btst_rr, reverse=True)

    print(f"\n{'='*70}")
    print(f"  FnO LIVE SIGNALS  --  positional option picks")
    print(f"{'='*70}")

    # ── APPROACHING = primary BTST picks ──────────────────────────────────────
    # Entry price ≈ zone entry price → realistic R:R for next-day position.
    if approaching:
        print(f"\n  *** APPROACHING — BTST PICKS (zone within {APPROACH_PCT}%, enter at/near zone) ***\n")
        print(f"  {'Stock':<12} {'Dir':<4} {'Entry':>7} {'Close':>7} {'Dist%':>6} {'SL':>7} {'T1':>7} {'B-R:R':>6} {'ZoneLock':<9}  Contract")
        print(f"  {'-'*12} {'-'*4} {'-'*7} {'-'*7} {'-'*6} {'-'*7} {'-'*7} {'-'*6} {'-'*9}  {'-'*20}")
        for s in approaching:
            arrow = "v" if s.direction == "CE" else "^"
            print(f"  {s.symbol:<12} {s.direction:<4} {s.entry_line:>7.1f} {s.current:>7.1f} "
                  f"{arrow}{s.dist_pct:>5.2f}% {s.hard_sl:>7.1f} {s.day_t1:>7.1f} {s.btst_rr:>6.2f} {s.lock_date:<9}  "
                  f"{s.suggested_strike} {s.direction} {s.expiry}")
    else:
        print(f"\n  No APPROACHING signals (no stock within {APPROACH_PCT}% of a zone).")

    # ── TRIGGERED = already fired yesterday ────────────────────────────────────
    # Trap fired on last bar. Close ≠ zone entry → BTST R:R from close shown.
    # Good for momentum continuation if BTST R:R is still > 1.5.
    if triggered:
        print(f"\n  -- TRIGGERED yesterday (zone already retested — BTST R:R from close) --\n")
        print(f"  {'Stock':<12} {'Dir':<4} {'ZoneEntry':>9} {'Close':>7} {'SL':>7} {'T1':>7} {'ZnR:R':>6} {'B-R:R':>6} {'ZoneLock':<9}  Contract")
        print(f"  {'-'*12} {'-'*4} {'-'*9} {'-'*7} {'-'*7} {'-'*7} {'-'*6} {'-'*6} {'-'*9}  {'-'*20}")
        for s in triggered:
            btst_flag = "" if s.btst_rr >= MIN_RR else "  [low]"
            print(f"  {s.symbol:<12} {s.direction:<4} {s.entry_line:>9.1f} {s.current:>7.1f} "
                  f"{s.hard_sl:>7.1f} {s.day_t1:>7.1f} {s.rr:>6.2f} {s.btst_rr:>6.2f} {s.lock_date:<9}  "
                  f"{s.suggested_strike} {s.direction} {s.expiry}{btst_flag}")
    else:
        print("\n  No TRIGGERED signals on last bar.")

    # ── FLIP signals ──────────────────────────────────────────────────────────
    if flipped:
        print(f"\n  ~~ FLIP (zone broken → role-reversed, retest from opposite side) ~~\n")
        print(f"  {'Stock':<12} {'Dir':<4} {'FlipLevel':>9} {'Close':>7} {'Dist%':>6} {'SL':>7} {'T1':>7} {'B-R:R':>6} {'ZoneLock':<9}  Contract")
        print(f"  {'-'*12} {'-'*4} {'-'*9} {'-'*7} {'-'*6} {'-'*7} {'-'*7} {'-'*6} {'-'*9}  {'-'*20}")
        for s in flipped:
            arrow = "v" if s.direction == "CE" else "^"
            print(f"  {s.symbol:<12} {s.direction:<4} {s.entry_line:>9.1f} {s.current:>7.1f} "
                  f"{arrow}{s.dist_pct:>5.2f}% {s.hard_sl:>7.1f} {s.day_t1:>7.1f} {s.btst_rr:>6.2f} {s.lock_date:<9}  "
                  f"{s.suggested_strike} {s.direction} {s.expiry}")

    if not approaching and not triggered and not flipped:
        print("\n  No signals found. Check back after next session.")
        return

    print(f"\n{'='*70}")
    print(f"  TOP 2 PICKS FOR PAPER TRADING")
    print(f"{'='*70}")
    print(f"  NOTE: APPROACHING = enter at zone price (full R:R).")
    print(f"        FLIP        = broken zone role-reversed; wait for retest of flip level.")
    print(f"        TRIGGERED   = zone fired yesterday; only trade if BTST R:R >= {MIN_RR}.")

    # Pick top 2: APPROACHING > FLIP (btst_rr >= MIN_RR) > TRIGGERED (btst_rr >= MIN_RR)
    top_approaching = approaching[:2]
    top_flipped     = [s for s in flipped if s.btst_rr >= MIN_RR][:2]
    top_triggered   = [s for s in triggered if s.btst_rr >= MIN_RR][:2]
    top2 = (top_approaching + top_flipped + top_triggered)[:2]

    if not top2:
        print(f"\n  No picks meet quality bar (BTST R:R >= {MIN_RR}). Watch APPROACHING list for entries.")
        print(f"\n{'='*70}\n")
        return

    for i, s in enumerate(top2, 1):
        is_approaching = s.status in ("APPROACHING", "FLIP")
        print(f"\n  Pick {i}: {s.symbol} {s.direction}  [{s.status}]")
        print(f"    Contract : {s.suggested_strike} {s.direction} {s.expiry}")
        if s.status == "FLIP":
            opp = "support" if s.direction == "CE" else "resistance"
            print(f"    Entry    : wait for pullback to {s.entry_line:.1f} (old zone now {opp} — buy option on retest)")
        elif s.status == "APPROACHING":
            print(f"    Entry    : stock spot at {s.entry_line:.1f} (zone retest — buy option at that level)")
        else:
            print(f"    Entry    : momentum continuation from {s.current:.1f} (zone fired yesterday at {s.entry_line:.1f})")
        print(f"    Spot SL  : {s.hard_sl:.1f}  ({HARD_SL_BUF}% beyond zone boundary)")
        print(f"    Day T1   : {s.day_t1:.1f}")
        print(f"    Zone R:R : {s.rr:.2f}  |  BTST R:R (from close): {s.btst_rr:.2f}")
        print(f"    Zone age : {s.zone_age} days since lock ({s.lock_date})")

    print(f"\n{'='*70}\n")


def save_watchlist(
    signals: List[Signal],
    universe: Optional[_FnoUniverse] = None,
    top_n: int = 30,
    out_path: Optional[str] = None,
) -> str:
    """Save top-N signals (APPROACHING first, then TRIGGERED with good BTST R:R) to data/fno_watchlist.json.

    Returns the file path written.  Only TRIGGERED + APPROACHING signals are saved.
    Consumers (live trading system) read this at startup to know which stocks to subscribe.
    Lot sizes come from FNO_STOCK_CONFIG first (curated), then from the instrument master
    universe (for stocks outside the hardcoded 30).
    """
    from config.global_config import FNO_STOCK_CONFIG

    # Priority: APPROACHING (full R:R) → FLIP (role-reversed) → TRIGGERED (already fired)
    # Within each group, sort by btst_rr descending
    _order = {"APPROACHING": 0, "FLIP": 1, "TRIGGERED": 2}
    ranked = sorted(
        signals,
        key=lambda s: (_order.get(s.status, 3), -s.btst_rr),
    )[:top_n]

    records = []
    for s in ranked:
        cfg = FNO_STOCK_CONFIG.get(s.symbol, {})
        # Lot: prefer curated FNO_STOCK_CONFIG, fall back to instrument master
        lot = cfg.get("lot") or (universe.lot_sizes.get(s.symbol, 0) if universe else 0)
        upstox_key = cfg.get("upstox_key") or (universe.stocks.get(s.symbol, "") if universe else "")
        records.append({
            "symbol":           s.symbol,
            "upstox_key":       upstox_key,
            "fyers":            cfg.get("fyers", ""),
            "lot":              lot,
            "step":             cfg.get("step", 10),
            "direction":        s.direction,
            "status":           s.status,
            "entry_line":       round(s.entry_line, 2),
            # zone_lo / zone_hi: reconstruct from SL which already includes the buffer
            "zone_lo":          round(s.hard_sl / (1 - HARD_SL_BUF / 100) if s.direction == "CE" else s.entry_line, 2),
            "zone_hi":          round(s.entry_line if s.direction == "CE" else s.hard_sl / (1 + HARD_SL_BUF / 100), 2),
            "hard_sl":          round(s.hard_sl, 2),
            "day_t1":           round(s.day_t1, 2),
            "rr":               round(s.rr, 2),
            "btst_rr":          round(s.btst_rr, 2),
            "dist_pct":         round(s.dist_pct, 2),
            "zone_age":         s.zone_age,
            "lock_date":        s.lock_date,
            "suggested_strike": s.suggested_strike,
            "expiry":           s.expiry,
            "scanned_at":       datetime.now().strftime("%Y-%m-%d %H:%M"),
        })

    if out_path is None:
        out_path = str(ROOT / "data" / "fno_watchlist.json")

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump({"generated": datetime.now().strftime("%Y-%m-%d %H:%M"), "stocks": records}, f, indent=2)

    return out_path


def load_watchlist(path: Optional[str] = None) -> List[Signal]:
    """Read back a JSON file written by save_watchlist() (an offline/nightly
    scan) as Signal objects, for a live book to consume without ever
    scanning the full ~200-stock universe itself. 2026-08-03: replaces
    fno_positional's previous live 09:00 scan_live.scan() call, which (a)
    fetched D1 history for the whole FnO universe every trading day and
    (b) was silently crashing on every call (scan() returns a (signals,
    universe) tuple, but the caller assigned it straight to `signals` and
    iterated it as if it were the signal list -- AttributeError on the
    very first list comprehension, meaning fno_positional had never
    actually populated a pending signal in production)."""
    if path is None:
        path = str(ROOT / "data" / "fno_positional_watchlist.json")
    try:
        with open(path) as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []
    out = []
    for r in data.get("stocks", []):
        out.append(Signal(
            symbol=r["symbol"], direction=r["direction"], status=r["status"],
            entry_line=r["entry_line"], current=r.get("entry_line", 0.0),
            dist_pct=r.get("dist_pct", 0.0), hard_sl=r["hard_sl"], day_t1=r["day_t1"],
            zone_age=r.get("zone_age", 0), lock_date=r.get("lock_date", ""),
            rr=r.get("rr", 0.0), btst_rr=r.get("btst_rr", 0.0),
            suggested_strike=r["suggested_strike"], expiry=r.get("expiry", ""),
            upstox_key=r.get("upstox_key", ""),
            zone_lo=r.get("zone_lo", 0.0), zone_hi=r.get("zone_hi", 0.0),
        ))
    return out


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="FnO Live Zone Scanner")
    parser.add_argument("--save", action="store_true",
                        help="Save top signals to data/fno_watchlist.json after scan")
    parser.add_argument("--top-n", type=int, default=5,
                        help="Max stocks to save to watchlist (default 30)")
    parser.add_argument("--out", default=None,
                        help="Override watchlist output path")
    args = parser.parse_args()

    token = os.environ.get("UPSTOX_TOKEN", "").strip()
    if not token:
        # Try loading from DB
        try:
            import asyncio as _asyncio
            from data_layer.client_db import ClientDB as _CDB
            async def _get_token():
                db = _CDB(); await db.initialise()
                creds = db.get_feeder_creds_sync("upstox") or {}
                return creds.get("access_token", "")
            token = _asyncio.run(_get_token())
        except Exception:
            pass
    if not token:
        print("ERROR: Set UPSTOX_TOKEN environment variable (or ensure upstox creds in DB).")
        sys.exit(1)

    signals, universe = scan(token)
    print_report(signals)

    if args.save:
        # Two independent live consumers read this scan's output under two different
        # default filenames: D1TrapOptionBookManager's WATCHLIST sentinel reads
        # data/fno_watchlist.json (save_watchlist()'s own default), while
        # FnOPositionalBook's load_watchlist() reads data/fno_positional_watchlist.json.
        # The documented nightly command (`--save --top-n N`, no --out) used to only
        # ever write the first of those -- FnOPositionalBook silently kept trading a
        # stale file until someone happened to pass --out explicitly. 2026-08-05
        # incident: that staleness (scan hadn't refreshed fno_positional_watchlist.json
        # since 08-03) let an already-invalidated zone fire live. If the caller didn't
        # override --out, write both so one command actually refreshes both consumers.
        out_paths = [args.out] if args.out else [
            str(ROOT / "data" / "fno_watchlist.json"),
            str(ROOT / "data" / "fno_positional_watchlist.json"),
        ]
        written = [save_watchlist(signals, universe=universe, top_n=args.top_n, out_path=p)
                   for p in out_paths]
        triggered_n   = sum(1 for s in signals if s.status == "TRIGGERED")
        approaching_n = sum(1 for s in signals if s.status == "APPROACHING")
        for path in written:
            print(f"\n  Watchlist saved → {path}")
        print(f"  {triggered_n} TRIGGERED  +  {approaching_n} APPROACHING  →  {min(len(signals), args.top_n)} stocks written")
