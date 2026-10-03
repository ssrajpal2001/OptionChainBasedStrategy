from datetime import datetime, timezone
from strategies.bear_trap_oi.models import Bar, TrapZone, TrapZoneState
from strategies.bear_trap_oi.trap_detector import (
    on_bar_close, check_zone_reentry, close_position,
)


def _bar(minute, o, h, l, c):
    return Bar(ts=datetime(2026, 10, 1, 9, minute, tzinfo=timezone.utc),
                open=o, high=h, low=l, close=c)


def _fresh_zone():
    return TrapZone(state=TrapZoneState.WAITING, c1=None, c2=None,
                     zone_lo=None, zone_hi=None, confirmed_ts=None)


def test_first_bar_becomes_c1_and_moves_to_breakdown_watch():
    zone = on_bar_close(_fresh_zone(), _bar(15, 100, 105, 98, 102))
    assert zone.state == TrapZoneState.BREAKDOWN_WATCH
    assert zone.c1.close == 102


def test_no_breakdown_rolls_c1_forward_to_latest_bar():
    zone = on_bar_close(_fresh_zone(), _bar(15, 100, 105, 98, 102))
    # next bar does NOT break c1's low (98) -> c1 rolls forward, stays in BREAKDOWN_WATCH
    zone = on_bar_close(zone, _bar(20, 102, 108, 100, 106))
    assert zone.state == TrapZoneState.BREAKDOWN_WATCH
    assert zone.c1.close == 106
    assert zone.c1.low == 100


def test_breakdown_bar_sets_zone_and_moves_to_trap_watch():
    zone = on_bar_close(_fresh_zone(), _bar(15, 100, 105, 98, 102))
    # breaks c1's low of 98
    zone = on_bar_close(zone, _bar(20, 97, 99, 90, 94))
    assert zone.state == TrapZoneState.TRAP_WATCH
    assert zone.c2.low == 90
    assert zone.zone_hi == 102  # c1.close
    assert zone.zone_lo == 90   # c2.low


def test_close_above_c1_high_confirms_trap_and_arms():
    zone = on_bar_close(_fresh_zone(), _bar(15, 100, 105, 98, 102))
    zone = on_bar_close(zone, _bar(20, 97, 99, 90, 94))
    # closes above c1.high (105)
    zone = on_bar_close(zone, _bar(25, 95, 110, 95, 108))
    assert zone.state == TrapZoneState.ARMED_WAIT_REENTRY
    assert zone.confirmed_ts is not None


def test_zone_reentry_detects_price_back_inside_zone():
    zone = on_bar_close(_fresh_zone(), _bar(15, 100, 105, 98, 102))
    zone = on_bar_close(zone, _bar(20, 97, 99, 90, 94))
    zone = on_bar_close(zone, _bar(25, 95, 110, 95, 108))
    assert zone.state == TrapZoneState.ARMED_WAIT_REENTRY
    assert check_zone_reentry(zone, live_price=96.0) is True   # inside [90,102]
    assert check_zone_reentry(zone, live_price=130.0) is False  # above zone


def test_zone_reentry_is_false_when_not_armed():
    zone = _fresh_zone()
    assert check_zone_reentry(zone, live_price=50.0) is False


def test_close_position_resets_to_fresh_waiting_zone():
    zone = on_bar_close(_fresh_zone(), _bar(15, 100, 105, 98, 102))
    zone = on_bar_close(zone, _bar(20, 97, 99, 90, 94))
    zone = on_bar_close(zone, _bar(25, 95, 110, 95, 108))
    closed = close_position(zone)
    assert closed.state == TrapZoneState.WAITING
    assert closed.c1 is None
    assert closed.c2 is None
