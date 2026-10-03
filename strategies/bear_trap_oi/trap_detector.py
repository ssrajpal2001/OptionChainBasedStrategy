"""Pure functions implementing the Bear Trap price-action state machine.

See docs/superpowers/specs/2026-10-03-bear-trap-oi-design.md Section 4.
These functions never mutate their TrapZone argument -- each call returns
a new TrapZone, so the same functions can drive both the live engine and
the historical backtest without any behavioral drift between them.
"""
from __future__ import annotations

from dataclasses import replace

from strategies.bear_trap_oi.models import Bar, TrapZone, TrapZoneState


def on_bar_close(zone: TrapZone, bar: Bar) -> TrapZone:
    if zone.state == TrapZoneState.WAITING:
        return replace(zone, state=TrapZoneState.BREAKDOWN_WATCH, c1=bar)

    if zone.state == TrapZoneState.BREAKDOWN_WATCH:
        if bar.low < zone.c1.low:
            return replace(
                zone,
                state=TrapZoneState.TRAP_WATCH,
                c2=bar,
                zone_hi=zone.c1.close,
                zone_lo=bar.low,
            )
        # no breakdown yet: roll c1 forward to the latest bar
        return replace(zone, c1=bar)

    if zone.state == TrapZoneState.TRAP_WATCH:
        if bar.close > zone.c1.high:
            return replace(
                zone,
                state=TrapZoneState.ARMED_WAIT_REENTRY,
                confirmed_ts=bar.ts,
            )
        return zone

    # ARMED_WAIT_REENTRY / IN_POSITION: bar closes don't change state here
    # (re-entry is checked tick-by-tick via check_zone_reentry; IN_POSITION
    # transitions out only via close_position, called by the engine on EOD).
    return zone


def check_zone_reentry(zone: TrapZone, live_price: float) -> bool:
    if zone.state != TrapZoneState.ARMED_WAIT_REENTRY:
        return False
    return zone.zone_lo <= live_price <= zone.zone_hi


def close_position(zone: TrapZone) -> TrapZone:
    return TrapZone(state=TrapZoneState.WAITING, c1=None, c2=None,
                     zone_lo=None, zone_hi=None, confirmed_ts=None)
