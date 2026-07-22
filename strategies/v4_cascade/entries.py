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
from typing import Optional, Tuple, Union

from strategies.v4_cascade.dataclasses import RollingBaseZone
from strategies.v4_cascade.spot_confirm import SpotConfirmTracker
from strategies.v4_cascade.zone_state import PremiumGateScanner, IndexGatedPremiumScanner, TrackingZoneScanner


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


def check_limit_pierce(
    scanner: Union[PremiumGateScanner, IndexGatedPremiumScanner], tracking_bar,
) -> LimitPierceResult:
    """3-gate funnel — Gate 3 external trigger check, run across EVERY
    concurrently LIMIT_ARMED setup on this scanner (multi-zone). Fires on the
    FIRST setup (oldest ref first) whose ``limit_entry_price`` a 5m
    tracking-contract bar pierces. Direction mirrors the scanner's own
    geometry: a bear-zone (long) setup fires when the bar's LOW drops down to
    or below limit_entry_price (retesting the zone from above); a bull-zone
    (short, crypto PE only) setup fires when the bar's HIGH rises up to or
    above it (retesting the zone from below) — NIFTY/CRUDEOIL only ever use
    bear-zone scanners, so this is unchanged there.
    No spot/Index re-check here — arming was already applied once, at Gate
    1/2's discovery gate (see engine.py); it is not re-evaluated per-trigger.
    The caller (engine.py) is responsible for calling
    ``scanner.pop_setup(setup, ts)`` once acted on — every OTHER in-flight
    setup keeps advancing untouched.
    Sort key is polymorphic: the new IndexGatedPremiumScanner's _PremiumSetup
    exposes ``.ref_ts``; the legacy (crypto) PremiumGateScanner's _HTFSetup
    exposes ``.htf_ref_ts``."""
    candidates = sorted(
        scanner.limit_armed_setups(),
        key=lambda s: getattr(s, "ref_ts", None) or getattr(s, "htf_ref_ts", None),
    )
    for setup in candidates:
        if setup.limit_entry_price is None:
            continue
        pierced = (tracking_bar.low <= setup.limit_entry_price if scanner._bear
                   else tracking_bar.high >= setup.limit_entry_price)
        if pierced:
            return LimitPierceResult(fired=True, setup=setup, tracking_price=setup.limit_entry_price)
    return LimitPierceResult(fired=False)


def compute_risk_mapping(
    zone: RollingBaseZone, tracking_entry_price: float, exec_entry_price: float,
    sl_buffer: float = 10.0, is_short: bool = False, target_floor_multiple: float = 1.0,
) -> Tuple[float, float]:
    """SL is anchored directly to the Inner Zone's edge plus a buffer (long:
    zone_low - sl_buffer, short: zone_high + sl_buffer), computed in
    TRACKING-contract terms and scaled onto the execution contract the same
    way the rest of the risk distance already was.

    2026-07-21: target is the zone's OWN ``sl_level`` (long: ref.high, the
    exact level where the sellers who sold into the sweep got stopped out;
    short: ref.low, the mirror for a bull-zone) -- a full round-trip back
    past the original trap-confirmation candle, mapped onto the execution
    contract by the same tracking-to-execution distance scale as the SL.
    Replaces the previous fixed target_r-multiple-of-risk formula per user
    direction: the target should be anchored to the zone's real structure,
    not an arbitrary R-multiple.

    Target distance is floored at ``tracking_risk * target_floor_multiple``
    (default multiple 1.0, i.e. the same distance used for SL -- unchanged
    default behavior; a backtest can grid-search other multiples without
    touching live callers, which never pass this kwarg) -- confirmed live: a
    flat/zero-range reference candle can put
    ``sl_level`` almost right on top of ``entry_line``, collapsing the raw
    target distance to nearly nothing (a real CRUDEOIL trade risked ~25
    tracking points to make ~2.4) while SL still measured out to the full
    sweep distance. Flooring at 1R guarantees the trade is never worse than
    1:1 reward:risk, and only ever changes the degenerate flat-candle case --
    a normal zone (sl_level genuinely far from entry) is unaffected since its
    natural distance already exceeds 1R.

    For crypto (tracking == execution price), this reduces to the literal
    zone_low-buffer SL / literal max(sl_level distance, risk) target.
    Returns (sl_price, target_price) for the execution contract."""
    if zone.entry_line is None or zone.sweep_low is None or zone.sl_level is None:
        # Defensive fallback — should not happen for a locked zone.
        if is_short:
            return exec_entry_price * 1.5, max(0.0, exec_entry_price * 0.75)
        return max(0.0, exec_entry_price * 0.5), exec_entry_price * 1.25

    zone_low = min(zone.entry_line, zone.sweep_low)
    zone_high = max(zone.entry_line, zone.sweep_low)
    scale = (exec_entry_price / tracking_entry_price) if tracking_entry_price > 0 else 1.0

    if is_short:
        tracking_risk = max((zone_high - tracking_entry_price) + sl_buffer, 0.01)
        exec_risk = tracking_risk * scale
        sl_price = exec_entry_price + exec_risk
        tracking_target_dist = max(tracking_entry_price - zone.sl_level, tracking_risk * target_floor_multiple)
        target_price = max(0.0, exec_entry_price - tracking_target_dist * scale)
    else:
        tracking_risk = max((tracking_entry_price - zone_low) + sl_buffer, 0.01)
        exec_risk = tracking_risk * scale
        sl_price = max(0.0, exec_entry_price - exec_risk)
        tracking_target_dist = max(zone.sl_level - tracking_entry_price, tracking_risk * target_floor_multiple)
        target_price = exec_entry_price + tracking_target_dist * scale
    return sl_price, target_price
