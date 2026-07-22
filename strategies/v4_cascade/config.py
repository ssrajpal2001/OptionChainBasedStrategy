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
TRACKING_RECENTER_PTS: float = 100.0   # NIFTY: re-center tracking strikes after this much ATM drift

T2_TRAIL_LOOKBACK_BASES: int = 4     # Tranche 2: last 4 locked 5m Rolling Bases
T2_TRAIL_TF_MINUTES: int = 5

# SL is anchored directly to the Inner (Gate 2/MTF) zone edge, not a scaled
# distance from entry: long = zone_low - sl_buffer, short = zone_high +
# sl_buffer, in TRACKING-contract price units (mirrors the same
# tracking->execution ratio scale already used for the rest of the risk
# distance). NIFTY = flat 10 premium points, regardless of lot_multiplier.
SL_BUFFER_PTS_NIFTY: float = 10.0
# Crypto: buffer is $200 PER 1 FULL COIN (BTC/ETH) of position size, not a
# flat number -- at lot_multiplier=1000 (1000 x 0.001 BTC = exactly 1 BTC),
# the buffer is the full $200; a smaller/larger position gets a
# proportionally smaller/larger buffer. book.py computes the actual
# sl_buffer passed into V4CascadeConfig as
# SL_BUFFER_PER_COIN_CRYPTO * (lot_multiplier * contract_value).
SL_BUFFER_PER_COIN_CRYPTO: float = 200.0

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
    tracking_recenter_pts: float = TRACKING_RECENTER_PTS

    t2_trail_lookback_bases: int = T2_TRAIL_LOOKBACK_BASES
    t2_trail_tf_minutes: int = T2_TRAIL_TF_MINUTES
    sl_buffer: float = SL_BUFFER_PTS_NIFTY

    pierce_check_tf_minutes: int = PIERCE_CHECK_TF_MINUTES
    spot_confirm_tf_minutes: int = SPOT_CONFIRM_TF_MINUTES

    # 2026-07-22: T1 target-floor multiple (entries.compute_risk_mapping's
    # target_floor_multiple kwarg) -- default 1.0 preserves today's exact
    # live behavior (floor = 1R). Exists as a config field purely so a
    # backtest can grid-search other multiples via V4CascadeConfig alone,
    # without passing anything special through engine.py's call site.
    target_floor_multiple: float = 1.0

    @property
    def tranche_qty(self) -> int:
        """65 units per tranche at lot_multiplier=2 (2 lots / 2 tranches = 1 lot/tranche)."""
        return (self.lot_size * self.lot_multiplier) // 2
