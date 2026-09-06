"""
scripts/oi_orb_entry_mode_backtest.py -- 2026-09-04, direct user spec:
replay the REAL last-5-trading-days OI-ORB Screener shortlist (already
recorded in data/oi_orb_screener.db, exported by the user) through all
three entry mechanics this codebase has ever built for this strategy --
trap-retest (current live default), immediate-ORB-entry (currently
enabled), and VWAP-retest (dormant code, screener.check_vwap_retest_entry)
-- on REAL intraday 1-min stock price history for the EXACT day each stock
was actually shortlisted, per direct user correction ("check when this
stocks came so backtest can be done for that day only as intraday and not
for 2 years").

Reuses REAL, already-validated detector code throughout, per this
codebase's own feedback_backtest_drive_real_class discipline:
  - strategies.liquidity_trap.detector.find_all_setups (via screener.
    sharp_bear_zones / bull_trap_zones)
  - strategies.d1_trap_option.bear_only_book._collapse_nearby_zones
  - strategies.core.support_resistance.SupportResistanceCalculator
  - strategies.oi_orb_screener.screener.check_vwap_retest_entry / VwapState

Deliberately backtests on the STOCK'S OWN spot price throughout (direct
user instruction "run the backtest in stocks not in option") -- P&L is
reported in stock POINTS moved, not real option premium (this strategy's
actual option-side execution has never been backtested, same honesty
caveat as every other OI-ORB Screener design note).

All three entry modes share the IDENTICAL exit mechanic (the hybrid
ORB-extreme + 15-min S1/R1 ratchet TSL, exactly matching engine.py's
_immediate_update_tsl_and_check_exit) -- isolates ENTRY TIMING QUALITY as
the only variable between the three, since that's what's actually being
compared here.

Historical data: Upstox's DATED historical-candle endpoint (1-minute,
specific past date -- NOT the "today only" intraday endpoint), via
data_layer.historical_candles.fetch_upstox_range_1m. NSE_EQ instrument keys
resolved from Upstox's public NSE.json.gz master (no auth needed for that
part).

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_entry_mode_backtest.py
"""
from __future__ import annotations

import asyncio
import gzip
import json
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time as dtime
from typing import Dict, List, Optional

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_range_1m
from strategies.core.trap_zone_utils import _collapse_nearby_zones, Bar
from strategies.core.support_resistance import SupportResistanceCalculator
from strategies.oi_orb_screener import screener

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
NSE_EQ_MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

ORB_START, ORB_END = "09:15", "09:25"
ENTRY_WINDOW_END = "15:00"

# ── (trade_date, symbol, side_bias) rows, exported 2026-09-04 from
# data/oi_orb_screener.db's shortlist table (last 5 trading days) ──────────
ROWS = [
    ("2026-08-31", "ADANIENT", "bearish"), ("2026-08-31", "ATHERENERG", "bullish"),
    ("2026-08-31", "AUROPHARMA", "bullish"), ("2026-08-31", "BDL", "bearish"),
    ("2026-08-31", "CAMS", "bullish"), ("2026-08-31", "KAYNES", "bearish"),
    ("2026-08-31", "MUTHOOTFIN", "bearish"), ("2026-08-31", "PERSISTENT", "bearish"),
    ("2026-08-31", "SAGILITY", "bullish"), ("2026-08-31", "SBICARD", "bullish"),
    ("2026-09-01", "ADANIENSOL", "bearish"), ("2026-09-01", "ADANIPORTS", "bullish"),
    ("2026-09-01", "ADANIPOWER", "bullish"), ("2026-09-01", "ASHOKLEY", "bearish"),
    ("2026-09-01", "BAJAJ-AUTO", "bullish"), ("2026-09-01", "BHARTIARTL", "bullish"),
    ("2026-09-01", "FORCEMOT", "bullish"), ("2026-09-01", "HEROMOTOCO", "bullish"),
    ("2026-09-01", "ITC", "bullish"), ("2026-09-01", "KALYANKJIL", "bearish"),
    ("2026-09-01", "KEI", "bearish"), ("2026-09-01", "LTF", "bearish"),
    ("2026-09-01", "MARUTI", "bearish"), ("2026-09-01", "MPHASIS", "bullish"),
    ("2026-09-01", "PERSISTENT", "bullish"), ("2026-09-01", "POLYCAB", "bearish"),
    ("2026-09-01", "SHRIRAMFIN", "bearish"),
    ("2026-09-02", "BOSCHLTD", "bearish"), ("2026-09-02", "BSE", "bearish"),
    ("2026-09-02", "COALINDIA", "bullish"), ("2026-09-02", "EICHERMOT", "bearish"),
    ("2026-09-02", "HEROMOTOCO", "bearish"), ("2026-09-02", "KEI", "bearish"),
    ("2026-09-02", "RBLBANK", "bullish"), ("2026-09-02", "SWIGGY", "bearish"),
    ("2026-09-02", "VOLTAS", "bearish"),
    ("2026-09-03", "APLAPOLLO", "bearish"), ("2026-09-03", "FEDERALBNK", "bearish"),
    ("2026-09-03", "GODREJCP", "bearish"), ("2026-09-03", "KAYNES", "bearish"),
    ("2026-09-03", "MAHABANK", "bullish"), ("2026-09-03", "MCX", "bearish"),
    ("2026-09-03", "PHOENIXLTD", "bullish"), ("2026-09-03", "RBLBANK", "bullish"),
    ("2026-09-03", "SBICARD", "bullish"), ("2026-09-03", "SOLARINDS", "bullish"),
    ("2026-09-03", "SWIGGY", "bullish"),
    ("2026-09-04", "ANGELONE", "bullish"), ("2026-09-04", "HAVELLS", "bearish"),
    ("2026-09-04", "KEI", "bearish"), ("2026-09-04", "POLYCAB", "bearish"),
]
# side_bias "bullish" -> stock is a gainer -> CALL candidate; "bearish" -> PUT candidate
SIDE = {"bullish": "CALL", "bearish": "PUT"}


@dataclass
class Trade:
    date: str
    symbol: str
    side: str
    mode: str
    entry_ts: Optional[datetime]
    entry_price: Optional[float]
    exit_ts: Optional[datetime]
    exit_price: Optional[float]
    reason: str

    @property
    def points(self) -> Optional[float]:
        if self.entry_price is None or self.exit_price is None:
            return None
        raw = self.exit_price - self.entry_price
        return raw if self.side == "CALL" else -raw


def to_bars(rows: List[dict]) -> List[Bar]:
    bars = [Bar(ts=datetime.fromisoformat(r["ts"]), open=r["open"], high=r["high"],
                low=r["low"], close=r["close"]) for r in rows]
    bars.sort(key=lambda b: b.ts)
    return bars


def volume_by_ts(rows: List[dict]) -> Dict[datetime, float]:
    """Bar (strategies.liquidity_trap.detector) carries no volume field --
    keep it in a parallel map keyed by timestamp for VWAP-retest mode."""
    return {datetime.fromisoformat(r["ts"]): float(r.get("volume", 0.0) or 0.0) for r in rows}


# 2026-09-06: to_n_min_bars moved to strategies/core/candle_indicators.py
# (ported into the live oi_orb_screener engine) -- imported here (and
# re-exported for the many other scripts that already do
# `from scripts.oi_orb_entry_mode_backtest import to_n_min_bars`), not
# duplicated, so this backtest can never drift from the live version.
from strategies.core.candle_indicators import to_n_min_bars  # noqa: E402,F401


def _key_range(bars: List[Bar], start_hhmm: str, end_hhmm: str) -> List[Bar]:
    return [b for b in bars if start_hhmm <= b.ts.strftime("%H:%M") < end_hhmm]


def compute_orb(bars_1m: List[Bar]) -> Optional[tuple]:
    window = _key_range(bars_1m, ORB_START, ORB_END)
    if not window:
        return None
    return (max(b.high for b in window), min(b.low for b in window))


def compute_late_orb(bars_1m: List[Bar]) -> Optional[tuple]:
    """2026-09-04, direct user correction: VWAP-retest's entry-validity
    filter AND its fixed SL both reference the narrower 09:20-09:25 window's
    low/high, NOT the full 09:15-09:25 ORB range compute_orb() returns.
    Same exclusive-of-end convention as compute_orb (bars 09:20..09:24)."""
    window = _key_range(bars_1m, "09:20", ORB_END)
    if not window:
        return None
    return (max(b.high for b in window), min(b.low for b in window))


# ── Equity instrument-key resolution (Upstox public NSE master, no auth) ──

_EQ_MAP: Dict[str, str] = {}


def _load_eq_map() -> None:
    global _EQ_MAP
    if _EQ_MAP:
        return
    from curl_cffi import requests as cc
    print("Downloading NSE equity instrument master...")
    r = cc.get(NSE_EQ_MASTER_URL, impersonate="chrome131", timeout=30)
    instruments = json.loads(gzip.decompress(r.content))
    m = {}
    for inst in instruments:
        if inst.get("segment") != "NSE_EQ" or inst.get("instrument_type") != "EQ":
            continue
        ts = str(inst.get("trading_symbol", "")).upper()
        key = inst.get("instrument_key", "")
        if ts and key:
            m[ts] = key
    _EQ_MAP = m
    print(f"Loaded {len(m)} NSE equity instrument keys.")


def resolve_eq_key(symbol: str) -> Optional[str]:
    _load_eq_map()
    return _EQ_MAP.get(symbol.upper())


# ── Shared exit mechanic: hybrid ORB-extreme + 15-min S1/R1 ratchet ────────
# Mirrors engine.py's _immediate_update_tsl_and_check_exit EXACTLY.

def simulate_hybrid_exit(entry_ts: datetime, entry_price: float, side: str,
                          orb_h: float, orb_l: float, bars_1m: List[Bar],
                          bars_15m: List[Bar]) -> tuple:
    sl_level = orb_l if side == "CALL" else orb_h
    calc = SupportResistanceCalculator()
    fed = 0
    post_entry_1m = [b for b in bars_1m if b.ts >= entry_ts]
    for b in post_entry_1m:
        # Feed any newly-closed 15-min bars up to this point.
        avail_15m = [x for x in bars_15m if x.ts <= b.ts]
        for nb in avail_15m[fed:]:
            calc.process_straddle_candle("SYM", {"timestamp": nb.ts, "high": nb.high,
                                                   "low": nb.low, "duration": 15})
        fed = len(avail_15m)
        sr = calc.get_calculated_sr_state("SYM").get("sr_levels", {})
        level = sr.get("S1") if side == "CALL" else sr.get("R1")
        if level is not None and level.get("is_established"):
            ladder_level = level["low"] if side == "CALL" else level["high"]
            if side == "CALL" and ladder_level > sl_level:
                sl_level = ladder_level
            elif side == "PUT" and ladder_level < sl_level:
                sl_level = ladder_level
        breach = (b.close <= sl_level) if side == "CALL" else (b.close >= sl_level)
        if breach:
            return b.ts, b.close, "hybrid_sl"
    if post_entry_1m:
        last = post_entry_1m[-1]
        return last.ts, last.close, "eod_open"
    return entry_ts, entry_price, "no_data_after_entry"


def simulate_fixed_sl_exit(entry_ts: datetime, entry_price: float, side: str,
                            ref_h: float, ref_l: float, bars_1m: List[Bar]) -> tuple:
    """2026-09-04, direct user spec, VWAP-retest mode ONLY: SL is FIXED at
    the 09:20-09:25 reference window's low (CALL) / high (PUT) -- a direct
    correction, NOT the full 09:15-09:25 ORB range -- for the whole life of
    the trade -- never ratchets, no TSL at all. If never touched, the trade
    simply runs to EOD (last available bar of the day). Real-stop semantics:
    triggers on an intrabar TOUCH (bar low/high), not a close confirmation,
    matching how an actual resting SL order would fire."""
    sl_level = ref_l if side == "CALL" else ref_h
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]
    for b in post_entry:
        breach = (b.low <= sl_level) if side == "CALL" else (b.high >= sl_level)
        if breach:
            return b.ts, sl_level, "fixed_sl"
    if post_entry:
        last = post_entry[-1]
        return last.ts, last.close, "eod_close"
    return entry_ts, entry_price, "no_data_after_entry"


def simulate_pct_tsl_exit(entry_ts: datetime, entry_price: float, side: str,
                           ref_h: float, ref_l: float, bars_1m: List[Bar],
                           activate_pct: float) -> tuple:
    """2026-09-05, direct user spec: "threshold system where when stock
    moves certain % then we move SL above and trail SL accordingly."

    The ORB-extreme fixed SL (simulate_fixed_sl_exit) is the floor for the
    whole trade -- always active from entry, never loosened. Once the
    trade's own running favorable extreme (intrabar high for CALL / low for
    PUT, vs entry) reaches `activate_pct`% profit, a trailing stop switches
    on: it trails `activate_pct`% behind that same running extreme (single
    parameter drives both the activation threshold and the trail distance,
    per the user's own framing -- "certain %... trail SL accordingly"). The
    effective stop is whichever is TIGHTER of the original fixed SL and the
    trail -- the trail only ever tightens the stop, never loosens it past
    the original ORB-extreme floor. Once touched (intrabar), the trade
    exits at that stop level; if never touched, runs to EOD close, same as
    simulate_fixed_sl_exit.

    Known simplification (same category as the rest of this 1-min-bar
    backtest): the favorable-extreme update and the breach check both read
    the SAME bar's high/low without reconstructing tick order within that
    bar -- a bar that both sets a new extreme AND reverses to breach the
    freshly-tightened stop in the same 60 seconds is treated as breaching
    at the new level, which is mildly optimistic. Immaterial at 1-min
    granularity for the trade shapes seen in this dataset (BOSCHLTD's own
    reversal played out over ~50 minutes, not one bar)."""
    sl_orig = ref_l if side == "CALL" else ref_h
    stop_level = sl_orig
    extreme = entry_price
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]
    for b in post_entry:
        if side == "CALL":
            extreme = max(extreme, b.high)
            profit_pct = (extreme / entry_price - 1.0) * 100.0
            if profit_pct >= activate_pct:
                trail = extreme * (1 - activate_pct / 100.0)
                stop_level = max(stop_level, trail)
            breach = b.low <= stop_level
        else:
            extreme = min(extreme, b.low)
            profit_pct = (1.0 - extreme / entry_price) * 100.0
            if profit_pct >= activate_pct:
                trail = extreme * (1 + activate_pct / 100.0)
                stop_level = min(stop_level, trail)
            breach = b.high >= stop_level
        if breach:
            reason = "tsl_hit" if stop_level != sl_orig else "fixed_sl"
            return b.ts, stop_level, reason
    if post_entry:
        last = post_entry[-1]
        return last.ts, last.close, "eod_close"
    return entry_ts, entry_price, "no_data_after_entry"


def simulate_two_param_tsl_exit(entry_ts: datetime, entry_price: float, side: str,
                                 ref_h: float, ref_l: float, bars_1m: List[Bar],
                                 activate_pct: float, trail_pct: float) -> tuple:
    """2026-09-05, direct user spec follow-up: the single-% version
    (simulate_pct_tsl_exit) forces the SAME % to both gate activation AND
    set the trail distance -- confirmed via a sweep to be a real structural
    tradeoff, not a tuning miss: any % small enough to protect a modest
    swing like BOSCHLTD's (peak favorable move only 0.78%) is also small
    enough to clip nearly every other trade into a scratch seconds after
    entry (PF looked great, total points collapsed).

    Decouples the two roles, same two-knob shape as this codebase's own
    FVG/OI-Flow trail_trigger_pct + first_lock_pct: `activate_pct` (looser)
    gates the mechanism off entirely until the trade's own running
    favorable peak actually proves real momentum -- most of the noisy,
    near-breakeven trades never cross this bar and stay on the plain fixed
    SL the whole time, same as before. `trail_pct` (tighter, always
    <= activate_pct for the trail to mean anything) only takes over once
    activated, hugging the running peak more closely than the activation
    gate itself would."""
    sl_orig = ref_l if side == "CALL" else ref_h
    stop_level = sl_orig
    extreme = entry_price
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]
    for b in post_entry:
        if side == "CALL":
            extreme = max(extreme, b.high)
            profit_pct = (extreme / entry_price - 1.0) * 100.0
            if profit_pct >= activate_pct:
                trail = extreme * (1 - trail_pct / 100.0)
                stop_level = max(stop_level, trail)
            breach = b.low <= stop_level
        else:
            extreme = min(extreme, b.low)
            profit_pct = (1.0 - extreme / entry_price) * 100.0
            if profit_pct >= activate_pct:
                trail = extreme * (1 + trail_pct / 100.0)
                stop_level = min(stop_level, trail)
            breach = b.high >= stop_level
        if breach:
            reason = "tsl_hit" if stop_level != sl_orig else "fixed_sl"
            return b.ts, stop_level, reason
    if post_entry:
        last = post_entry[-1]
        return last.ts, last.close, "eod_close"
    return entry_ts, entry_price, "no_data_after_entry"


# ── Mode 1: trap-retest (current live default) ─────────────────────────────

def run_trap_retest(bars_1m: List[Bar], bars_3m: List[Bar], side: str) -> Optional[tuple]:
    zones_fn = screener.sharp_bear_zones if side == "CALL" else screener.bull_trap_zones
    zones = _collapse_nearby_zones(zones_fn(bars_3m))
    entry_window = _key_range(bars_1m, ORB_END, ENTRY_WINDOW_END)
    if not zones or not entry_window:
        return None
    calc = None
    zone_active = None
    for b in entry_window:
        if calc is None:
            for z in zones:
                if z["lock_ts"] >= b.ts:
                    continue
                touched = (b.low <= z["zone_hi"]) if side == "CALL" else (b.high >= z["zone_lo"])
                if touched:
                    zone_active = z
                    calc = SupportResistanceCalculator()
                    break
            continue
        phase_before = calc.get_calculated_sr_state("SYM").get("current_phase")
        calc.process_straddle_candle("SYM", {"timestamp": b.ts, "high": b.high, "low": b.low, "duration": 1})
        phase_after = calc.get_calculated_sr_state("SYM").get("current_phase")
        target = "R1_TRACKING" if side == "CALL" else "S1_TRACKING"
        if phase_before in ("S2_TRACKING", "R2_TRACKING") and phase_after == target:
            return b.ts, b.close
    return None


# ── Mode 2: immediate-ORB-entry (currently enabled live) ───────────────────

def run_immediate(bars_1m: List[Bar], orb_h: float, orb_l: float) -> Optional[tuple]:
    entry_window = _key_range(bars_1m, ORB_END, ENTRY_WINDOW_END)
    if not entry_window:
        return None
    b = entry_window[0]
    return b.ts, b.close


# ── Mode 3: VWAP-retest (dormant code) ──────────────────────────────────────

def run_vwap_retest(bars_1m: List[Bar], side: str, ref_h: float,
                     ref_l: float, vol_by_ts: Dict[datetime, float]) -> List[tuple]:
    """2026-09-04, direct user spec (real backtest review, AUROPHARMA CALL
    caught taking a long entry already below the day's own support):

    Rule 1 -- reference-extreme entry filter: even when the VWAP touch-back
    fires, reject the entry if price is already below the 09:20-09:25
    window's own LOW for a CALL, or already above that window's HIGH for a
    PUT -- that's a sign of a broken level underneath/above, not a genuine
    VWAP pullback long/short. NOTE: this is the narrower 09:20-09:25 window
    (`ref_h`/`ref_l`, compute_late_orb()), a direct 2026-09-04 correction --
    NOT the full 09:15-09:25 ORB range compute_orb() returns. A rejected
    fire still resets `armed` (see check_vwap_retest_entry's own docstring/
    return contract -- it keeps armed=True on fire and leaves "already
    fired" bookkeeping to the caller), so the setup can re-arm naturally on
    the next away-from-VWAP move.

    Rule 2 -- ONE re-entry per stock per day, SL-triggered only: if entry 1
    gets stopped out (fixed_sl, not eod_close) with time left in the entry
    window, a second VWAP touch-back (same Rule 1 filter) is allowed to fire
    -- but never a third time the same day, regardless of how many more
    times the condition re-fires.

    Exit mechanic (2026-09-04, direct user spec): SL is FIXED at the SAME
    09:20-09:25 reference extreme for the whole trade, never ratchets --
    see simulate_fixed_sl_exit(). No TSL of any kind for this mode.

    Returns up to 2 (entry_ts, entry_price, exit_ts, exit_price, reason)
    tuples, oldest first."""
    vwap_state = screener.VwapState()
    armed = False
    for b in _key_range(bars_1m, ORB_START, ORB_END):
        vol = vol_by_ts.get(b.ts, 0.0)
        if vol > 0:
            vwap_state.seed("SYM", (b.high + b.low + b.close) / 3.0 * vol, vol)

    entry_window = _key_range(bars_1m, ORB_END, ENTRY_WINDOW_END)
    trades: List[tuple] = []
    i = 0
    while i < len(entry_window) and len(trades) < 2:
        b = entry_window[i]
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        vwap = vwap_state.current("SYM")
        if vwap is None:
            i += 1
            continue
        armed, fire = screener.check_vwap_retest_entry(side, b.close, vwap, armed, 0.15)
        if not fire:
            i += 1
            continue
        armed = False   # touched back -- must move away from VWAP again before re-arming
        ref_ok = (b.close >= ref_l) if side == "CALL" else (b.close <= ref_h)
        if not ref_ok:
            i += 1
            continue   # Rule 1: rejected, structure already broken -- keep scanning, doesn't count against the cap
        entry_ts, entry_price = b.ts, b.close
        exit_ts, exit_price, reason = simulate_fixed_sl_exit(
            entry_ts, entry_price, side, ref_h, ref_l, bars_1m)
        trades.append((entry_ts, entry_price, exit_ts, exit_price, reason))
        if reason != "fixed_sl" or len(trades) >= 2:
            break   # Rule 2: only an SL-triggered exit earns a re-entry chance, capped at 2 total
        next_idx = next((k for k, bb in enumerate(entry_window) if bb.ts > exit_ts), None)
        if next_idx is None:
            break
        i = next_idx
    return trades


def run_vwap_retest_no_reentry_breach_cancel(bars_1m: List[Bar], side: str, orb_h: float,
                                              orb_l: float, vol_by_ts: Dict[datetime, float],
                                              exit_fn=simulate_fixed_sl_exit) -> List[tuple]:
    """2026-09-04, direct user spec, variant 2 -- three changes from
    run_vwap_retest() above:

    1. NO re-entry -- at most ONE entry attempt per stock per day, period.
       No second chance after a stop-out.

    2. Reference reverts to the FULL 09:15-09:25 ORB range (compute_orb),
       not the narrower 09:20-09:25 window run_vwap_retest() uses.

    3/4. Breach-cancel -- a genuinely different check from run_vwap_retest's
       Rule 1 (which only looked at price AT THE INSTANT of the touch-back).
       This tracks a running, ONE-WAY flag for the whole entry window: once
       price has EVER dipped below the ORB low (CALL) / risen above the ORB
       high (PUT) at any point after 09:25, every SUBSEQUENT VWAP touch-back
       for that side is cancelled for the rest of the day -- even if price
       fully recovers and legitimately touches VWAP again later. A stock
       that already broke its own opening-range support/resistance once
       can't be trusted for a pullback-continuation story on a later bounce.
       Practically: once breached, no valid entry can ever fire again that
       day for that side (every later touch-back happens "after" the
       breach by definition).

    Returns 0 or 1 (entry_ts, entry_price, exit_ts, exit_price, reason)
    tuples (never more -- no re-entry)."""
    vwap_state = screener.VwapState()
    armed = False
    for b in _key_range(bars_1m, ORB_START, ORB_END):
        vol = vol_by_ts.get(b.ts, 0.0)
        if vol > 0:
            vwap_state.seed("SYM", (b.high + b.low + b.close) / 3.0 * vol, vol)

    entry_window = _key_range(bars_1m, ORB_END, ENTRY_WINDOW_END)
    breached = False
    for b in entry_window:
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        if side == "CALL" and b.low <= orb_l:
            breached = True
        elif side == "PUT" and b.high >= orb_h:
            breached = True
        vwap = vwap_state.current("SYM")
        if vwap is None:
            continue
        armed, fire = screener.check_vwap_retest_entry(side, b.close, vwap, armed, 0.15)
        if not fire:
            continue
        armed = False
        if breached:
            continue   # Rule 3/4: cancelled -- structure already broken once today
        entry_ts, entry_price = b.ts, b.close
        exit_ts, exit_price, reason = exit_fn(
            entry_ts, entry_price, side, orb_h, orb_l, bars_1m)
        return [(entry_ts, entry_price, exit_ts, exit_price, reason)]
    return []


def run_vwap_retest_immediate_if_historically_fulfilled(bars_1m, side, orb_h, orb_l, vol_by_ts,
                                                          exit_fn=simulate_fixed_sl_exit):
    """2026-09-04, direct user spec, variant 3: "when the stocks are found
    at 9.25 we will fetch the historical data of that specific stock and if
    vwap condition is fulfilled then immediately take trade, dont wait for
    fresh vwap touch logic to happen."

    Interpretation: at the 09:25 shortlist-lock moment, replay the SAME
    arm+touch-back VWAP condition (check_vwap_retest_entry) bar-by-bar
    across the stock's own 09:15-09:25 opening-range history (the only
    "historical data" that exists at that exact instant). If the condition
    already resolved (armed, then touched back) WITHIN that opening window,
    enter IMMEDIATELY at 09:25 -- don't wait for it to happen again live.

    If it never resolved within 09:15-09:25, falls back unchanged to the
    normal post-09:25 live scan (variant 2: no re-entry, breach-cancel,
    full-ORB reference, fixed-SL/EOD-close exit) -- this variant only
    changes WHEN an already-historically-true condition gets acted on, it
    doesn't relax or replace any of variant 2's other rules.

    Note: the ORB extreme (orb_h/orb_l) is the min/max of the 09:15-09:25
    bars themselves, so a "breach" of it is structurally impossible inside
    that same window -- the breach-cancel rule has nothing to check yet
    during the historical replay and only matters once we fall through to
    the live post-09:25 path."""
    orb_bars = _key_range(bars_1m, ORB_START, ORB_END)
    vwap_state = screener.VwapState()
    armed = False
    historically_fulfilled = False
    for b in orb_bars:
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        vwap = vwap_state.current("SYM")
        if vwap is None:
            continue
        armed, fire = screener.check_vwap_retest_entry(side, b.close, vwap, armed, 0.15)
        if fire:
            historically_fulfilled = True
            break

    if not historically_fulfilled:
        return run_vwap_retest_no_reentry_breach_cancel(bars_1m, side, orb_h, orb_l, vol_by_ts, exit_fn=exit_fn)

    entry_window = _key_range(bars_1m, ORB_END, ENTRY_WINDOW_END)
    if not entry_window:
        return []
    b0 = entry_window[0]
    entry_ts, entry_price = b0.ts, b0.close
    exit_ts, exit_price, reason = exit_fn(
        entry_ts, entry_price, side, orb_h, orb_l, bars_1m)
    return [(entry_ts, entry_price, exit_ts, exit_price, "immediate_historical:" + reason)]


async def backtest_one(trade_date: str, symbol: str, side: str) -> List[Trade]:
    key = resolve_eq_key(symbol)
    if key is None:
        print(f"  [{trade_date} {symbol}] no NSE_EQ instrument key found -- skipping.")
        return []
    d = date.fromisoformat(trade_date)
    rows = await fetch_upstox_range_1m(key, TOKEN, d, d)
    if not rows:
        print(f"  [{trade_date} {symbol}] no candle data returned -- skipping.")
        return []
    bars_1m = to_bars(rows)
    vol_by_ts = volume_by_ts(rows)
    orb = compute_orb(bars_1m)
    if orb is None:
        print(f"  [{trade_date} {symbol}] no 09:15-09:25 ORB bars -- skipping.")
        return []
    orb_h, orb_l = orb

    # 2026-09-04, direct user spec, variant 3: if the VWAP arm+touch-back
    # condition already resolved within the stock's own 09:15-09:25 history,
    # enter immediately at 09:25 instead of waiting for a fresh live
    # touch-back -- falls back to variant 2 (no re-entry, breach-cancel,
    # full-ORB reference) unchanged when it did not already resolve.
    vwap_trades = run_vwap_retest_immediate_if_historically_fulfilled(bars_1m, side, orb_h, orb_l, vol_by_ts)
    trades = []
    if not vwap_trades:
        trades.append(Trade(trade_date, symbol, side, "vwap_retest",
                             None, None, None, None, "no_entry"))
    else:
        for (entry_ts, entry_price, exit_ts, exit_price, reason) in vwap_trades:
            trades.append(Trade(trade_date, symbol, side, "vwap_retest", entry_ts, entry_price,
                                 exit_ts, exit_price, reason))
    return trades


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    all_trades: List[Trade] = []
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        print(f"[{trade_date} {symbol} {side}] fetching + backtesting...")
        trades = await backtest_one(trade_date, symbol, side)
        all_trades.extend(trades)

    print("\n" + "=" * 100)
    print("PER-MODE SUMMARY")
    print("=" * 100)
    by_mode: Dict[str, List[Trade]] = defaultdict(list)
    for t in all_trades:
        by_mode[t.mode].append(t)

    for mode in ("trap_retest", "immediate", "vwap_retest"):
        rows = by_mode.get(mode, [])
        entered = [t for t in rows if t.points is not None]
        no_entry = len(rows) - len(entered)
        wins = [t.points for t in entered if t.points > 0]
        losses = [t.points for t in entered if t.points <= 0]
        total_pts = sum(t.points for t in entered)
        pf = (sum(wins) / abs(sum(losses))) if losses and sum(losses) != 0 else float("inf") if wins else 0.0
        win_pct = (len(wins) / len(entered) * 100.0) if entered else 0.0
        avg_pts = (total_pts / len(entered)) if entered else 0.0
        print(f"\n{mode.upper()}: {len(entered)} entered / {no_entry} no-entry (of {len(rows)} candidates)")
        print(f"  win%={win_pct:.1f}  PF={pf:.2f}  total_pts={total_pts:+.2f}  avg_pts/trade={avg_pts:+.2f}")

    print("\n" + "=" * 100)
    print("PER-TRADE DETAIL")
    print("=" * 100)
    for t in all_trades:
        if t.points is None:
            print(f"{t.date} {t.symbol:12s} {t.side:4s} {t.mode:12s} NO ENTRY")
        else:
            print(f"{t.date} {t.symbol:12s} {t.side:4s} {t.mode:12s} "
                  f"entry={t.entry_price:8.2f}@{t.entry_ts.strftime('%H:%M')} "
                  f"exit={t.exit_price:8.2f}@{t.exit_ts.strftime('%H:%M')} "
                  f"({t.reason}) pts={t.points:+7.2f}")


if __name__ == "__main__":
    asyncio.run(main())
