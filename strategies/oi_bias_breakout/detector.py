"""
strategies/oi_bias_breakout/detector.py -- pure, unit-testable logic for the
"9:25 AM OI + Price Action" strategy (frozen spec, 2026-09-25, direct user
spec after extensive back-and-forth clarification -- see the session's own
locked decisions, restated below per function).

Fully standalone, per this codebase's own zero-shared-runtime mandate for a
new strategy -- the ONE deliberate exception is Step 1 (stock selection),
which is explicitly NOT reimplemented here: the live engine calls
strategies.oi_orb_screener.screener.build_shortlist()/poll_oi_rank() and
strategies.core.trap_zone_utils / strategies.oi_orb_screener.screener.
bull_trap_zones() directly, unchanged, per direct user instruction not to
redesign or reinterpret those.

No asyncio, no network, no broker calls anywhere in this module -- every
function here takes plain data in and returns a plain, deterministic
answer, so the exact same functions can drive both a real backtest and the
live engine without behavioral drift (this codebase's own established
discipline -- see feedback_backtest_drive_real_class).

Frozen spec, step by step (2026-09-25):
  1. Selection (9:25:05) -- top 10 gainers + top 10 losers, cross-matched
     against NSE's OI-spurt list. NOT reimplemented here.
  2. Signal-strike freeze -- ATM/OTM Call/OTM Put fixed from the stock's
     9:15 opening price. Used ONLY for the Step 3-5 OI analysis, never for
     what actually gets traded.
  3. OI snapshots at 9:15, 9:20, 9:25 on those frozen strikes.
  4. 9:15->9:20 logged only, never gates anything. Only 9:20->9:25 decides.
  5. Directional bias: bullish = OTM Call OI falling + ATM Put OI rising;
     bearish = OTM Put OI falling + ATM Call OI rising; both true at once =
     CONFLICT = no trade; neither = no signal.
  6. Historical OI-pattern comparison -- research/logging only, not
     implemented as a trading gate in this module at all.
  7. Entry 1: reference is the underlying's own FIRST 1-minute candle of the
     day (9:15:00-9:15:59), its own high/low -- NOT a 15-minute window. A
     LATER 1-minute candle must CLOSE beyond that level (a wick doesn't
     count). Strike is ATM, resolved fresh from the underlying's price at
     this entry's own moment -- independent of the Step 2 signal strikes.
  8. Entry 2: VWAP tracked on a 1-minute basis. Bullish retest = a 1-minute
     candle whose open is above VWAP but whose low dips below it. Bearish
     = open below VWAP, high pokes above it. Strike is ATM again, resolved
     fresh at THIS entry's own moment -- independent of both Step 2 and
     Entry 1's strikes. Max 2 lots total, potentially two different
     strikes.
  9. Exit, first of three to fire: (a) a 20-minute candle genuinely CLOSES
     on the wrong side of VWAP: (b) no new high (bullish) / low (bearish)
     for 60 minutes, timer anchored at Entry 1 and never reset by Entry 2;
     (c) the 75-minute bull-trap target is hit -- the SAME bull-trap
     detection (a failed bullish breakout, reversing back through its own
     reference candle's low) run independently on whichever option is
     actually held (the Call's own premium chart for a bullish trade, the
     Put's own premium chart for a bearish trade) -- no bear-trap logic
     anywhere in this strategy, confirmed by direct user instruction.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

from strategies.core.trap_zone_utils import Bar, BarAccumulator  # noqa: F401 (re-exported for callers)

# ── Step 2: signal-strike freeze ────────────────────────────────────────────


@dataclass(frozen=True)
class SignalStrikes:
    atm: float
    otm_call: float
    otm_put: float


def freeze_signal_strikes(open_915_price: float, strike_step: float) -> SignalStrikes:
    """Decision 2 (frozen): ATM/OTM Call/OTM Put derived from the stock's
    9:15 opening price, rounded to the nearest real strike step. Example
    from the frozen spec: open=1247, step=10 -> atm=1250, otm_call=1260,
    otm_put=1240. Used ONLY for the Step 3-5 OI comparison -- never re-run,
    never used for what actually gets bought."""
    if strike_step <= 0:
        raise ValueError("strike_step must be positive")
    atm = round(open_915_price / strike_step) * strike_step
    return SignalStrikes(atm=atm, otm_call=atm + strike_step, otm_put=atm - strike_step)


def resolve_entry_atm(spot_price: float, strike_step: float) -> float:
    """Decisions 7/8: the ATM strike actually traded is resolved FRESH from
    the underlying's price at each entry's own moment -- deliberately NOT
    tied to freeze_signal_strikes' own 9:15-anchored result. Called once at
    Entry 1's own trigger moment, and again independently at Entry 2's own
    trigger moment -- the two entries can legitimately land on different
    strikes."""
    if strike_step <= 0:
        raise ValueError("strike_step must be positive")
    return round(spot_price / strike_step) * strike_step


# ── Step 5: directional OI bias ─────────────────────────────────────────────

BIAS_BULLISH = "bullish"
BIAS_BEARISH = "bearish"
BIAS_CONFLICT = "conflict"
BIAS_NONE = "none"


def classify_oi_bias(
    otm_call_oi_920: float, otm_call_oi_925: float,
    atm_put_oi_920: float, atm_put_oi_925: float,
    otm_put_oi_920: float, otm_put_oi_925: float,
    atm_call_oi_920: float, atm_call_oi_925: float,
) -> str:
    """Decision 5 (frozen), mechanical only -- no claim about who is behind
    the OI move, purely an observable-movement rule:

        bullish: OTM Call OI falls (9:25 < 9:20) AND ATM Put OI rises (9:25 > 9:20)
        bearish: OTM Put OI falls (9:25 < 9:20) AND ATM Call OI rises (9:25 > 9:20)
        both true at once -> CONFLICT -> no trade
        neither -> no signal

    Strictly '<' / '>' (not '<=' / '>=') -- an unchanged OI is neither a
    rise nor a fall, so it can never itself satisfy either condition."""
    bullish = (otm_call_oi_925 < otm_call_oi_920) and (atm_put_oi_925 > atm_put_oi_920)
    bearish = (otm_put_oi_925 < otm_put_oi_920) and (atm_call_oi_925 > atm_call_oi_920)
    if bullish and bearish:
        return BIAS_CONFLICT
    if bullish:
        return BIAS_BULLISH
    if bearish:
        return BIAS_BEARISH
    return BIAS_NONE


# ── Step 7: Entry 1, first-1-minute-candle breakout ─────────────────────────


def find_entry1_trigger(candle_915: Bar, later_bars: List[Bar], bias: str) -> Optional[Bar]:
    """Decision 7 (frozen): the reference is the underlying's own FIRST
    1-minute candle of the day (9:15:00-9:15:59) -- its own high/low, NOT a
    15-minute opening-range window. Scans later_bars (already chronological,
    already excluding candle_915 itself) for the first candle whose CLOSE
    genuinely beyond the reference level -- a wick through it does not
    count. Returns that triggering Bar, or None if it hasn't happened yet
    in the given bars."""
    if bias not in (BIAS_BULLISH, BIAS_BEARISH):
        return None
    for bar in later_bars:
        if bias == BIAS_BULLISH and bar.close > candle_915.high:
            return bar
        if bias == BIAS_BEARISH and bar.close < candle_915.low:
            return bar
    return None


# ── Step 8: Entry 2, 1-minute VWAP retest ───────────────────────────────────


def check_vwap_retest(candle: Bar, vwap: float, bias: str) -> bool:
    """Decision 8 (frozen): VWAP evaluated per 1-minute candle.
        bullish retest: candle.open > vwap AND candle.low < vwap
                        (approaches from above, dips to touch/pierce VWAP)
        bearish retest: candle.open < vwap AND candle.high > vwap
                        (approaches from below, pokes up through VWAP)"""
    if vwap <= 0:
        return False
    if bias == BIAS_BULLISH:
        return candle.open > vwap and candle.low < vwap
    if bias == BIAS_BEARISH:
        return candle.open < vwap and candle.high > vwap
    return False


# ── Step 9a: 20-minute VWAP-close exit ──────────────────────────────────────


def check_vwap_close_exit(candle_20m: Bar, vwap: float, bias: str) -> bool:
    """Decision 9a (frozen): a 20-minute candle must genuinely CLOSE on the
    wrong side of VWAP -- a wick through it intrabar does not trigger this.
    Bullish exits below VWAP; bearish exits above it."""
    if vwap <= 0:
        return False
    if bias == BIAS_BULLISH:
        return candle_20m.close < vwap
    if bias == BIAS_BEARISH:
        return candle_20m.close > vwap
    return False


# ── Step 9b: 60-minute stagnation exit (anchored at Entry 1, never reset) ──


def time_since_last_new_extreme(bars_since_entry1: List[Bar], bias: str) -> Optional[timedelta]:
    """Walks bars_since_entry1 (chronological, starting at/after Entry 1's
    own trigger bar) tracking the running peak high (bullish) or trough low
    (bearish) and the timestamp it was last set. Returns
    (last_bar.ts - last_new_extreme.ts), or None if bars_since_entry1 is
    empty. A pure re-scan (matches this codebase's own established
    'grow the bar list, re-scan every call' pattern), not a stateful
    incremental tracker -- deliberately simple and easy to verify."""
    if not bars_since_entry1:
        return None
    if bias == BIAS_BULLISH:
        extreme = bars_since_entry1[0].high
        extreme_ts = bars_since_entry1[0].ts
        for bar in bars_since_entry1[1:]:
            if bar.high > extreme:
                extreme = bar.high
                extreme_ts = bar.ts
    elif bias == BIAS_BEARISH:
        extreme = bars_since_entry1[0].low
        extreme_ts = bars_since_entry1[0].ts
        for bar in bars_since_entry1[1:]:
            if bar.low < extreme:
                extreme = bar.low
                extreme_ts = bar.ts
    else:
        return None
    return bars_since_entry1[-1].ts - extreme_ts


def check_stagnation_exit(bars_since_entry1: List[Bar], bias: str,
                           stagnation_minutes: float = 60.0) -> bool:
    """Decision 9b (frozen): exits once stagnation_minutes have passed with
    no new high (bullish) / no new low (bearish) since Entry 1. The clock
    is implicitly anchored at Entry 1 because bars_since_entry1 always
    starts there and is never reset by Entry 2 -- callers must pass the
    FULL bar list since Entry 1, not a window that restarts on the second
    entry."""
    gap = time_since_last_new_extreme(bars_since_entry1, bias)
    if gap is None:
        return False
    return gap >= timedelta(minutes=stagnation_minutes)


# ── Step 9c: 75-minute bull-trap target ─────────────────────────────────────
#
# Deliberately NOT reimplemented here -- reuses
# strategies.oi_orb_screener.screener.bull_trap_zones() (itself built on
# strategies.core.trap_zone_utils.find_all_setups()) directly, per direct
# user instruction: bull-trap detection ONLY, run independently on
# whichever option's own premium chart is actually held (the Call's own
# 75-min bars for a bullish trade, the Put's own 75-min bars for a bearish
# trade) -- never the mirrored bear-trap logic (screener.sharp_bear_zones),
# regardless of side. See strategies/oi_orb_screener/screener.py's own
# bull_trap_zones() for the exact mechanic:
#   a candle breaks above the PRIOR candle's high (a bull setup) is only a
#   confirmed trap once a LATER candle fully reverses back below that same
#   prior candle's own low; the zone spans [prior.close, breakout.high].


def check_trap_target_hit(zones: List[dict], current_price: float) -> bool:
    """Decision 9c (frozen): the target/exit fires the instant price
    genuinely re-enters ANY already-locked bull-trap zone -- same zone-touch
    check strategies/oi_orb_screener/engine.py's own _trap_ladder_check
    already uses (zone_lo <= price <= zone_hi). `zones` is whatever
    screener.bull_trap_zones() returned for the held option's own 75-min
    bar series so far."""
    return any(z["zone_lo"] <= current_price <= z["zone_hi"] for z in zones)
