"""
strategies/v4_cascade/entries.py — pure entry trigger evaluation.

Mechanical trigger: NOT the reclaim candle's close — a subsequent 5m candle's
low (bear/CE side) or high (bull/PE side) must pierce the frozen entry_line.
Gated by the concurrent spot-confirmation check for the same window.

Risk mapping: SL/target are computed on the TRACKING contract's structure
(entry_line vs. sweep_low = tracking risk), then scaled proportionally onto
the execution contract by the entry-price ratio. Both CE and PE are bought
(long options) — P&L direction is identical regardless of side (premium up =
profit), so the SL/target formula does not branch on side.

No bus/broker/DB dependency.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from strategies.v4_cascade.dataclasses import RollingBaseZone
from strategies.v4_cascade.spot_confirm import SpotConfirmTracker
from strategies.v4_cascade.zone_state import PremiumGateScanner, TrackingZoneScanner


@dataclass(frozen=True)
class TriggerResult:
    fired: bool
    discarded: bool          # pierced but spot did not confirm -> setup permanently discarded
    tracking_price: float = 0.0


def check_entry_trigger(
    scanner: TrackingZoneScanner, spot_confirm: SpotConfirmTracker,
    side: str, tracking_bar,
) -> TriggerResult:
    """Evaluate one 5m tracking-contract bar against the scanner's current
    RETEST_PENDING zone. Returns fired=True only if the pierce AND the spot
    concurrent-confirmation both hold; discarded=True if it pierced but spot
    did not confirm (permanently discards the setup instance, per spec)."""
    if not scanner.check_pierce(tracking_bar):
        return TriggerResult(fired=False, discarded=False)

    tracking_price = tracking_bar.low if side == "CE" else tracking_bar.high
    if spot_confirm.confirms(side):
        return TriggerResult(fired=True, discarded=False, tracking_price=tracking_price)
    return TriggerResult(fired=False, discarded=True, tracking_price=tracking_price)


@dataclass(frozen=True)
class LimitPierceResult:
    fired: bool
    setup: object = None            # the zone_state._HTFSetup that fired, if any
    tracking_price: float = 0.0


def check_limit_pierce(scanner: PremiumGateScanner, tracking_bar) -> LimitPierceResult:
    """2026-07-19 3-gate funnel — Gate 3 external trigger check, run across
    EVERY concurrently LIMIT_ARMED setup on this scanner (multi-zone, per the
    2026-07-19 same-day fix). Fires on the FIRST setup (oldest HTF ref first)
    whose ``limit_entry_price`` a 5m tracking-contract bar's low pierces down
    to or below. No spot-confirmation re-check here — spot bias was already
    applied once, at Gate 1's arming (see engine.py); it is not re-evaluated
    per-trigger under the new pure-premium model. The caller (engine.py) is
    responsible for calling ``scanner.pop_setup(setup, ts)`` once acted on —
    every OTHER in-flight setup keeps advancing untouched."""
    candidates = sorted(scanner.limit_armed_setups(), key=lambda s: s.htf_ref_ts)
    for setup in candidates:
        if setup.limit_entry_price is not None and tracking_bar.low <= setup.limit_entry_price:
            return LimitPierceResult(fired=True, setup=setup, tracking_price=setup.limit_entry_price)
    return LimitPierceResult(fired=False)


def compute_risk_mapping(
    zone: RollingBaseZone, tracking_entry_price: float, exec_entry_price: float,
    target_r: float,
) -> Tuple[float, float]:
    """Map the tracking contract's structural risk (entry_line vs sweep_low)
    proportionally onto the execution contract via the entry-price ratio.
    Returns (sl_price, target_price) for the execution contract."""
    if zone.entry_line is None or zone.sweep_low is None:
        # Defensive fallback — should not happen for a locked zone.
        return max(0.0, exec_entry_price * 0.5), exec_entry_price * (1.0 + target_r * 0.5)

    tracking_risk = abs(zone.entry_line - zone.sweep_low)
    scale = (exec_entry_price / tracking_entry_price) if tracking_entry_price > 0 else 1.0
    exec_risk = max(tracking_risk * scale, 0.01)

    sl_price = max(0.0, exec_entry_price - exec_risk)
    target_price = exec_entry_price + target_r * exec_risk
    return sl_price, target_price
