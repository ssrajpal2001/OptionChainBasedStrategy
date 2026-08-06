"""Futures open-interest buildup classification -- price vs day-over-day OI
change. Pure functions only (no I/O), used by FnOPositionalBook as an
additive, non-blocking confirmation note on real entries -- see
FnOPositionalBook._check_oi_buildup. Never gates or delays a trade: OI
buildup can lag a zone touch by hours or days, so it is logged as context,
not required before entry.
"""
from __future__ import annotations

LONG_BUILDUP   = "LONG_BUILDUP"    # price up   + OI up   -- fresh longs entering
SHORT_BUILDUP  = "SHORT_BUILDUP"   # price down + OI up   -- fresh shorts entering
SHORT_COVERING = "SHORT_COVERING"  # price up   + OI down -- shorts exiting
LONG_UNWINDING = "LONG_UNWINDING"  # price down + OI down -- longs exiting
FLAT           = "FLAT"            # unusable/unchanged inputs

CONFIRMS    = "CONFIRMS"
CONTRADICTS = "CONTRADICTS"
NEUTRAL     = "NEUTRAL"

_BULLISH_CONFIRMS = {LONG_BUILDUP, SHORT_COVERING}
_BEARISH_CONFIRMS = {SHORT_BUILDUP, LONG_UNWINDING}


def classify_oi_buildup(prev_close: float, curr_close: float,
                         prev_oi: float, curr_oi: float) -> str:
    """Standard futures OI-buildup classification. Returns FLAT if the prior
    close/OI is unusable (<=0) or price/OI is unchanged -- avoids a
    misleading call on noise rather than guessing a direction."""
    if prev_close <= 0 or prev_oi <= 0:
        return FLAT
    price_up, price_down = curr_close > prev_close, curr_close < prev_close
    oi_up, oi_down = curr_oi > prev_oi, curr_oi < prev_oi
    if price_up and oi_up:
        return LONG_BUILDUP
    if price_down and oi_up:
        return SHORT_BUILDUP
    if price_up and oi_down:
        return SHORT_COVERING
    if price_down and oi_down:
        return LONG_UNWINDING
    return FLAT


def oi_agreement(direction: str, buildup: str) -> str:
    """direction: 'CE' (bullish thesis) or 'PE' (bearish thesis).
    CONFIRMS/CONTRADICTS/NEUTRAL relative to that thesis."""
    if buildup == FLAT:
        return NEUTRAL
    if direction == "CE":
        return CONFIRMS if buildup in _BULLISH_CONFIRMS else CONTRADICTS
    if direction == "PE":
        return CONFIRMS if buildup in _BEARISH_CONFIRMS else CONTRADICTS
    return NEUTRAL
