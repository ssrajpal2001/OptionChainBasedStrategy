"""strategies/v4_cascade/entries.py's compute_risk_mapping -- 2026-07-21:
target changed from a fixed 2R-multiple-of-risk to literally zone.sl_level
(the trap-confirmation candle's opposite extreme), per explicit user
direction: target should be anchored to the zone's real structure (a full
round-trip back past where the trapped sellers/buyers got stopped out), not
an arbitrary R-multiple. SL is unchanged: zone_low - buffer (long) /
zone_high + buffer (short), tracking-to-execution distance-scaled.

Same-day follow-up: target distance is floored at tracking_risk (the same
distance used for SL) -- confirmed live, a flat/zero-range reference candle
can put sl_level almost right on top of entry_line, collapsing the raw
target distance to nearly nothing while SL still measures out to the full
sweep distance. The floor guarantees the trade is never worse than 1:1
reward:risk."""
from strategies.v4_cascade.dataclasses import RollingBaseZone
from strategies.v4_cascade.entries import compute_risk_mapping


def _zone(entry_line, sweep_low, sl_level):
    return RollingBaseZone(entry_line=entry_line, sweep_low=sweep_low, sl_level=sl_level, locked=True)


def test_long_sl_below_entry_target_floored_at_1r_scaled_sl_level():
    # Mirrors the live CRUDEOIL example: entry_line=737.2, sweep_low=736.5
    # (zone_low), sl_level=745.6 (ref.high). tracking entry (limit price)
    # 736.9666..., execution entry 516.10 (same numbers from the real trade).
    # This IS the degenerate case the floor was added for: raw sl_level
    # distance (8.63) is smaller than the risk distance (20.47), so the
    # floor kicks in and target uses the risk distance instead.
    zone = _zone(entry_line=737.2, sweep_low=736.5, sl_level=745.6)
    sl_price, target_price = compute_risk_mapping(
        zone, tracking_entry_price=736.9666666666667, exec_entry_price=516.10,
        sl_buffer=20.0, is_short=False,
    )
    # SL must be below entry (zone_low - buffer, scaled).
    assert sl_price < 516.10
    scale = 516.10 / 736.9666666666667
    tracking_risk = (736.9666666666667 - 736.5) + 20.0
    expected_sl = 516.10 - tracking_risk * scale
    assert abs(sl_price - expected_sl) < 1e-6
    # Target = max(sl_level distance, tracking_risk), scaled -- floored at 1R
    # here since the raw sl_level distance (8.63) is smaller.
    raw_target_dist = 745.6 - 736.9666666666667
    assert raw_target_dist < tracking_risk   # confirms this is the floor case
    expected_target = 516.10 + tracking_risk * scale
    assert abs(target_price - expected_target) < 1e-6
    assert target_price > 516.10


def test_short_sl_above_entry_target_floored_at_1r_scaled_sl_level():
    # Bull-zone (short, crypto PE only): entry_line=ref.high, sl_level=ref.low.
    # Raw sl_level distance (400) is smaller than risk (610) here too, so
    # this also exercises the floor, not the raw sl_level distance.
    zone = _zone(entry_line=2000.0, sweep_low=2500.0, sl_level=1500.0)
    sl_price, target_price = compute_risk_mapping(
        zone, tracking_entry_price=1900.0, exec_entry_price=100.0,
        sl_buffer=10.0, is_short=True,
    )
    assert sl_price > 100.0   # short SL is above entry
    assert target_price < 100.0   # short target is below entry
    scale = 100.0 / 1900.0
    zone_high = max(2000.0, 2500.0)
    tracking_risk = (zone_high - 1900.0) + 10.0
    expected_sl = 100.0 + tracking_risk * scale
    assert abs(sl_price - expected_sl) < 1e-6
    raw_target_dist = 1900.0 - 1500.0
    assert raw_target_dist < tracking_risk   # confirms this is the floor case
    expected_target = max(0.0, 100.0 - tracking_risk * scale)
    assert abs(target_price - expected_target) < 1e-6


def test_target_floored_at_1r_when_sl_level_beyond_entry_wrong_direction():
    # Defensive: if sl_level ends up on the "wrong" side of entry (shouldn't
    # happen for a real locked zone), the floor still guarantees a sane,
    # positive 1R target rather than collapsing to entry or going negative.
    zone = _zone(entry_line=100.0, sweep_low=95.0, sl_level=90.0)  # sl_level < entry
    sl_price, target_price = compute_risk_mapping(
        zone, tracking_entry_price=98.0, exec_entry_price=50.0,
        sl_buffer=5.0, is_short=False,
    )
    scale = 50.0 / 98.0
    tracking_risk = (98.0 - 95.0) + 5.0
    expected_target = 50.0 + tracking_risk * scale
    assert abs(target_price - expected_target) < 1e-6
    assert target_price > 50.0   # never collapses to bare entry anymore


def test_defensive_fallback_when_zone_fields_missing():
    zone = RollingBaseZone()  # entry_line/sweep_low/sl_level all None
    sl_price, target_price = compute_risk_mapping(
        zone, tracking_entry_price=100.0, exec_entry_price=50.0, is_short=False,
    )
    assert sl_price == 25.0
    assert target_price == 62.5


# ── 2026-07-22: target_floor_multiple (backtest grid-search knob) ──────────

def test_target_floor_multiple_default_matches_prior_1r_behavior():
    """No target_floor_multiple passed -- must be byte-identical to today's
    live behavior (floor at exactly 1x tracking_risk)."""
    zone = _zone(entry_line=737.2, sweep_low=736.5, sl_level=745.6)
    kwargs = dict(tracking_entry_price=736.9666666666667, exec_entry_price=516.10,
                  sl_buffer=20.0, is_short=False)
    sl_a, target_a = compute_risk_mapping(zone, **kwargs)
    sl_b, target_b = compute_risk_mapping(zone, target_floor_multiple=1.0, **kwargs)
    assert sl_a == sl_b
    assert target_a == target_b


def test_target_floor_multiple_2x_widens_the_floored_target():
    # Same degenerate-zone fixture as test_long_sl_below_entry_target_floored_at_1r_scaled_sl_level
    # (raw sl_level distance 8.63 < tracking_risk 20.47, so the floor governs).
    zone = _zone(entry_line=737.2, sweep_low=736.5, sl_level=745.6)
    kwargs = dict(tracking_entry_price=736.9666666666667, exec_entry_price=516.10,
                  sl_buffer=20.0, is_short=False)
    sl_1x, target_1x = compute_risk_mapping(zone, target_floor_multiple=1.0, **kwargs)
    sl_2x, target_2x = compute_risk_mapping(zone, target_floor_multiple=2.0, **kwargs)
    assert sl_1x == sl_2x  # SL is never affected by the target floor multiple
    scale = 516.10 / 736.9666666666667
    tracking_risk = (736.9666666666667 - 736.5) + 20.0
    expected_target_2x = 516.10 + (tracking_risk * 2.0) * scale
    assert abs(target_2x - expected_target_2x) < 1e-6
    assert target_2x > target_1x


def test_target_floor_multiple_does_not_shrink_a_naturally_wider_target():
    # Non-degenerate zone: raw sl_level distance (32) already exceeds
    # tracking_risk (8) even at floor_multiple=3 (24), so the floor multiple
    # must be a no-op regardless of its value here.
    zone = _zone(entry_line=100.0, sweep_low=95.0, sl_level=130.0)
    kwargs = dict(tracking_entry_price=98.0, exec_entry_price=50.0, sl_buffer=5.0, is_short=False)
    _, target_1x = compute_risk_mapping(zone, target_floor_multiple=1.0, **kwargs)
    _, target_3x = compute_risk_mapping(zone, target_floor_multiple=3.0, **kwargs)
    assert target_1x == target_3x
