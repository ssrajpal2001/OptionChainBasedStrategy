"""strategies/v4_cascade/entries.py's compute_risk_mapping -- 2026-07-21:
target changed from a fixed 2R-multiple-of-risk to literally zone.sl_level
(the trap-confirmation candle's opposite extreme), per explicit user
direction: target should be anchored to the zone's real structure (a full
round-trip back past where the trapped sellers/buyers got stopped out), not
an arbitrary R-multiple. SL is unchanged: zone_low - buffer (long) /
zone_high + buffer (short), tracking-to-execution distance-scaled."""
from strategies.v4_cascade.dataclasses import RollingBaseZone
from strategies.v4_cascade.entries import compute_risk_mapping


def _zone(entry_line, sweep_low, sl_level):
    return RollingBaseZone(entry_line=entry_line, sweep_low=sweep_low, sl_level=sl_level, locked=True)


def test_long_sl_below_entry_target_equals_scaled_sl_level():
    # Mirrors the live CRUDEOIL example: entry_line=737.2, sweep_low=736.5
    # (zone_low), sl_level=745.6 (ref.high). tracking entry (limit price)
    # 736.9666..., execution entry 516.10 (same numbers from the real trade).
    zone = _zone(entry_line=737.2, sweep_low=736.5, sl_level=745.6)
    sl_price, target_price = compute_risk_mapping(
        zone, tracking_entry_price=736.9666666666667, exec_entry_price=516.10,
        sl_buffer=20.0, is_short=False,
    )
    # SL must be below entry (zone_low - buffer, scaled).
    assert sl_price < 516.10
    scale = 516.10 / 736.9666666666667
    expected_sl = 516.10 - ((736.9666666666667 - 736.5) + 20.0) * scale
    assert abs(sl_price - expected_sl) < 1e-6
    # Target = zone.sl_level distance from entry, scaled -- NOT a 2R multiple.
    expected_target = 516.10 + (745.6 - 736.9666666666667) * scale
    assert abs(target_price - expected_target) < 1e-6
    assert target_price > 516.10


def test_short_sl_above_entry_target_equals_scaled_sl_level():
    # Bull-zone (short, crypto PE only): entry_line=ref.high, sl_level=ref.low.
    zone = _zone(entry_line=2000.0, sweep_low=2500.0, sl_level=1500.0)
    sl_price, target_price = compute_risk_mapping(
        zone, tracking_entry_price=1900.0, exec_entry_price=100.0,
        sl_buffer=10.0, is_short=True,
    )
    assert sl_price > 100.0   # short SL is above entry
    assert target_price < 100.0   # short target is below entry
    scale = 100.0 / 1900.0
    zone_high = max(2000.0, 2500.0)
    expected_sl = 100.0 + ((zone_high - 1900.0) + 10.0) * scale
    assert abs(sl_price - expected_sl) < 1e-6
    expected_target = max(0.0, 100.0 - (1900.0 - 1500.0) * scale)
    assert abs(target_price - expected_target) < 1e-6


def test_target_never_negative_when_sl_level_beyond_entry_wrong_direction():
    # Defensive: if sl_level ends up on the "wrong" side of entry (shouldn't
    # happen for a real locked zone, but the formula must not go negative).
    zone = _zone(entry_line=100.0, sweep_low=95.0, sl_level=90.0)  # sl_level < entry
    sl_price, target_price = compute_risk_mapping(
        zone, tracking_entry_price=98.0, exec_entry_price=50.0,
        sl_buffer=5.0, is_short=False,
    )
    assert target_price == 50.0  # zero distance -> target collapses to entry, never negative


def test_defensive_fallback_when_zone_fields_missing():
    zone = RollingBaseZone()  # entry_line/sweep_low/sl_level all None
    sl_price, target_price = compute_risk_mapping(
        zone, tracking_entry_price=100.0, exec_entry_price=50.0, is_short=False,
    )
    assert sl_price == 25.0
    assert target_price == 62.5
