import pytest

from strategies.vp_oi_regime.volume_profile import SessionVolumeProfile


def test_empty_profile_returns_none():
    vp = SessionVolumeProfile(rows=80)
    assert vp.snapshot() is None


def test_rejects_out_of_spec_row_count():
    with pytest.raises(ValueError):
        SessionVolumeProfile(rows=50)
    with pytest.raises(ValueError):
        SessionVolumeProfile(rows=120)


def test_poc_is_highest_volume_row():
    vp = SessionVolumeProfile(rows=80, value_area_pct=0.70)
    # Heavy concentration around 100, light elsewhere across a 90-110 range.
    for _ in range(50):
        vp.add_bar(high=100.2, low=99.8, volume=1000.0)
    for p in (90.0, 95.0, 105.0, 110.0):
        vp.add_bar(high=p + 0.2, low=p - 0.2, volume=10.0)
    snap = vp.snapshot()
    assert snap is not None
    assert abs(snap.poc - 100.0) < 1.0


def test_value_area_contains_70_percent_of_volume():
    vp = SessionVolumeProfile(rows=80, value_area_pct=0.70)
    for _ in range(10):
        vp.add_bar(high=100.2, low=99.8, volume=500.0)
    for p in (90.0, 92.0, 94.0, 96.0, 98.0, 102.0, 104.0, 106.0, 108.0, 110.0):
        vp.add_bar(high=p + 0.2, low=p - 0.2, volume=50.0)
    snap = vp.snapshot()
    assert snap is not None
    va_volume = sum(v for row_low, v in snap.rows.items() if snap.val <= row_low < snap.vah)
    assert va_volume / snap.total_volume >= 0.65  # within rounding of the 70% target


def test_location_classification():
    vp = SessionVolumeProfile(rows=80)
    for _ in range(20):
        vp.add_bar(high=100.2, low=99.8, volume=100.0)
    for p in (95.0, 105.0):
        vp.add_bar(high=p + 0.2, low=p - 0.2, volume=20.0)
    snap = vp.snapshot()
    assert snap.location(snap.poc) == "inside_va"
    assert snap.location(snap.vah + 10.0) == "above_vah"
    assert snap.location(snap.val - 10.0) == "below_val"


def test_zero_width_session_returns_none():
    vp = SessionVolumeProfile(rows=80)
    for _ in range(5):
        vp.add_bar(high=100.0, low=100.0, volume=10.0)  # every bar at the same price -> hi==lo
    assert vp.snapshot() is None


def test_lvn_rows_are_low_volume_relative_to_poc():
    vp = SessionVolumeProfile(rows=80, lvn_percentile=0.30)
    for _ in range(100):
        vp.add_bar(high=100.2, low=99.8, volume=1000.0)
    vp.add_bar(high=130.2, low=129.8, volume=5.0)  # a single far-away bar -> definitely an LVN row
    snap = vp.snapshot()
    assert snap is not None
    # The row containing 130.0 should be classified LVN (far below hvn_percentile).
    assert any(abs(row - 130.0) < snap.row_size * 2 for row in snap.lvn_rows)


def test_rejects_high_below_low():
    vp = SessionVolumeProfile(rows=80)
    vp.add_bar(high=90.0, low=95.0, volume=100.0)  # invalid, high < low -> silently dropped
    assert vp.snapshot() is None


def test_volume_spread_across_full_bar_range_not_just_close():
    """2026-10-03 fix verification: a single wide bar's volume must land in
    EVERY row its [low, high] touches, not collapse onto one point (the
    close-only bug confirmed wrong against a real TradingView chart)."""
    vp = SessionVolumeProfile(rows=70, value_area_pct=0.70)
    vp.add_bar(high=110.0, low=100.0, volume=700.0)  # spans the whole 70-row range
    snap = vp.snapshot()
    assert snap is not None
    # Every one of the 70 rows should have gotten an equal share (10 each).
    assert len(snap.rows) == 70
    assert all(abs(v - 10.0) < 1e-6 for v in snap.rows.values())
