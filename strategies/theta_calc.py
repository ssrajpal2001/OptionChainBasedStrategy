"""Simple, feed-only 'theta' = option TIME VALUE (extrinsic), per the user's spec — NOT
Black-Scholes. Intrinsic = how deep in-the-money the strike is vs spot; time value = the part of
the premium that is NOT intrinsic, i.e. what decays. For a short straddle we PROFIT as the combined
time value decays toward 0, so the day-wise 'theta-based' exit measures combined-time-value decay %.

Pure functions — unit-testable, no I/O, no Greeks library.
"""
from __future__ import annotations


def intrinsic_value(option_type: str, strike: float, spot: float) -> float:
    """In-the-money amount. CE: max(0, spot-strike); PE: max(0, strike-spot)."""
    s, k = float(spot), float(strike)
    if str(option_type).upper().startswith("C"):
        return max(0.0, s - k)
    return max(0.0, k - s)


def time_value(option_type: str, strike: float, spot: float, premium: float) -> float:
    """Extrinsic (time) value = |premium - intrinsic|. abs() so a momentarily sub-intrinsic
    quote (stale/illiquid) can't produce a negative time value."""
    return abs(float(premium) - intrinsic_value(option_type, strike, spot))


def combined_time_value(ce_strike: float, pe_strike: float, spot: float,
                        ce_premium: float, pe_premium: float) -> float:
    """CE time value + PE time value for the straddle/strangle at a given spot."""
    return (time_value("CE", ce_strike, spot, ce_premium)
            + time_value("PE", pe_strike, spot, pe_premium))


def theta_decay_pct(entry_time_value: float, current_time_value: float) -> float:
    """Signed % the combined time value has decayed since entry.
    Positive = time value shrank = profit for a short straddle; negative = it expanded = loss.
    Returns 0 when the entry time value is non-positive (cannot form a ratio)."""
    etv = float(entry_time_value)
    if etv <= 0:
        return 0.0
    return (etv - float(current_time_value)) / etv * 100.0
