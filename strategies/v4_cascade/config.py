"""
strategies/v4_cascade/config.py — V4CascadeConfig: tunable parameters for the
V4 Premium Trap Cascade Engine. Pure dataclass, no I/O.
"""
from __future__ import annotations

from dataclasses import dataclass

# Per-deployment default: 2 lots total (130 units) split into two 65-unit
# tranches for NIFTY (lot_size=65). ``lot_multiplier`` scales this the same
# way sell_straddle's lot_multiplier scales its lot_size — kept dynamic
# (configurable per deployment) rather than hard-locked, so a deployment can
# run more/fewer lots while defaulting to the spec's "2 lots" baseline.
DEFAULT_LOT_MULTIPLIER: int = 2

TRACKING_OFFSET_PTS: float = 200.0   # CE tracking = ATM-200, PE tracking = ATM+200
EXECUTION_OFFSET_PTS: float = 50.0   # CE execution = ATM+50, PE execution = ATM-50

T1_TARGET_R: float = 2.0             # Tranche 1 fixed target, in R-multiples of tracking-contract risk
T2_TRAIL_LOOKBACK_BASES: int = 4     # Tranche 2: last 4 locked 5m Rolling Bases
T2_TRAIL_TF_MINUTES: int = 5

# SL is anchored directly to the Inner (Gate 2/MTF) zone edge, not a scaled
# distance from entry: long = zone_low - sl_buffer, short = zone_high +
# sl_buffer, in TRACKING-contract price units (mirrors the same
# tracking->execution ratio scale already used for the rest of the risk
# distance). NIFTY = 10 premium points, BTC/ETH = $50 (crypto tracking ==
# execution price, so this ends up being the exact literal formula there).
SL_BUFFER_PTS_NIFTY: float = 10.0
SL_BUFFER_PTS_CRYPTO: float = 50.0

PIERCE_CHECK_TF_MINUTES: int = 5     # granularity of the "candle low pierces entry line" trigger
SPOT_CONFIRM_TF_MINUTES: int = 75    # spot-side concurrent-confirmation is fixed at 75m (no ladder)

# NOTE: there is intentionally NO bar-count cap on how long a sellers-in/
# buyers-in candle can wait for its SL to clear (TRAPPED confirmation) — the
# wait is bounded only by the multiplier's resampled lookback window itself.
# If a multiplier's whole history shows no confirmed trap, the ladder climbs
# to the next multiplier. See rolling_base.py's module docstring.


@dataclass
class V4CascadeConfig:
    underlying: str = "NIFTY"
    lot_multiplier: int = DEFAULT_LOT_MULTIPLIER
    lot_size: int = 65                # NIFTY lot size (cfg.exchange.lot_sizes["NIFTY"] at wiring time)

    tracking_offset_pts: float = TRACKING_OFFSET_PTS
    execution_offset_pts: float = EXECUTION_OFFSET_PTS

    t1_target_r: float = T1_TARGET_R
    t2_trail_lookback_bases: int = T2_TRAIL_LOOKBACK_BASES
    t2_trail_tf_minutes: int = T2_TRAIL_TF_MINUTES
    sl_buffer: float = SL_BUFFER_PTS_NIFTY

    pierce_check_tf_minutes: int = PIERCE_CHECK_TF_MINUTES
    spot_confirm_tf_minutes: int = SPOT_CONFIRM_TF_MINUTES

    @property
    def tranche_qty(self) -> int:
        """65 units per tranche at lot_multiplier=2 (2 lots / 2 tranches = 1 lot/tranche)."""
        return (self.lot_size * self.lot_multiplier) // 2
