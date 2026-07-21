"""strategies/v4_cascade/execution_risk.py -- 2026-07-21 execution-native risk
computation.

Per docs/superpowers/specs/2026-07-21-v4-cascade-execution-native-risk-design.md:
SL/target for a V4 Cascade trade are computed natively from the EXECUTION
strike's own recent price history (a ONE-TIME lookback at entry), instead of
being mathematically scaled from the tracking contract's zone. Rationale: the
execution strike trades near ATM and is meaningfully more liquid than the
tracking strike (deliberately chosen far ITM/OTM for structural clarity of the
discovery pattern) -- it is MORE likely, not less, to show a clean, timely
analogous sweep+reclaim pattern.

This is NOT a continuous scanner -- it runs the exact same sweep-detection
function Gate 2 already uses (find_all_bear_traps_2candle, with the same
5m-then-15m-fallback Gate 2 already implements) ONCE against a fetched window
of execution-strike bars, at the moment of entry. Returns None when no valid
zone is found on either timeframe, signaling the caller (book.py's
_open_entry_async) to fall back to today's tracking-scaled approach -- SL/target
must never be left undefined.

``is_short`` selects find_all_bull_traps_2candle instead (mirroring
engine.py's Gate 3 `is_short = not scanner._bear` convention, which picks
which trap-shaped scanner produced a zone before feeding it into
compute_risk_mapping) -- unlike engine.py, this module has no scanner to
read that from, since it does its own one-time zone search, so it must
replicate the same bear/bull selection internally to stay consistent. In
current wiring this branch is unreachable in production: crypto (the only
caller that ever passes is_short=True) skips this whole module entirely, so
only the is_short=False/bear-trap path is exercised on real NIFTY/CRUDEOIL
trades today. Kept for interface symmetry with compute_risk_mapping, which
already supports is_short generally.
"""
from __future__ import annotations

from typing import List, Optional, Tuple

from strategies.v4_cascade.entries import compute_risk_mapping
from strategies.v4_cascade.rolling_base import (
    find_all_bear_traps_2candle, find_all_bull_traps_2candle, resample_bars,
)


def compute_execution_native_risk(
    execution_bars_5m: List, exec_entry_price: float, sl_buffer: float,
    is_short: bool = False, session_open: Tuple[int, int] = (9, 15),
) -> Optional[Tuple[float, float]]:
    """Returns (sl_price, target_price) computed natively from
    ``execution_bars_5m`` (oldest-first execution-contract 5m bars), or None
    if no valid sweep+reclaim zone is found on 5m or the 15m fallback."""
    if len(execution_bars_5m) < 3:
        return None

    # Bear/bull selection mirrors engine.py's Gate 3 is_short convention --
    # see module docstring for why this module must replicate it internally.
    finder = find_all_bull_traps_2candle if is_short else find_all_bear_traps_2candle
    zones = finder(execution_bars_5m)
    if not zones:
        resampled = resample_bars(execution_bars_5m, 15, session_open=session_open)
        if len(resampled) >= 3:
            zones = finder(resampled)
    if not zones:
        return None

    # Most recently formed zone wins (mirrors the "most-recently-discovered"
    # recency convention already used elsewhere in this codebase, e.g. the
    # tracking-panel display's setup selection).
    zone = max(zones, key=lambda z: z.reference_low_ts)

    # compute_risk_mapping expects a "tracking" and "exec" entry price to
    # derive a scale ratio -- here both ARE the execution contract, so
    # passing exec_entry_price for both collapses the scale to 1.0 (no
    # conversion), which is exactly what "native" means.
    return compute_risk_mapping(
        zone, tracking_entry_price=exec_entry_price, exec_entry_price=exec_entry_price,
        sl_buffer=sl_buffer, is_short=is_short,
    )
