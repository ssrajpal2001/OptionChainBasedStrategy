from strategies.vp_oi_regime.futures_rollover import FuturesRolloverTracker


def test_standard_tracking_outside_expiry_week():
    t = FuturesRolloverTracker()
    t.update_near(0.0, 1_000_000, 50_000)
    state = t.classify(now_ts=0.0, is_expiry_week=False)
    assert state.is_expiry_week is False
    assert state.active_contract == "near"
    assert state.rollover_detected is False


def test_genuine_rollover_detected_comparable_magnitude():
    t = FuturesRolloverTracker(change_window_min=15, rollover_match_tol_pct=25.0, min_move_pct=10.0)
    t.update_near(0.0, 1_000_000, 50_000)
    t.update_next(0.0, 100_000, 5_000)
    # Near-month OI falls ~20%, next-month OI rises ~22% -- comparable magnitude.
    t.update_near(900.0, 800_000, 50_000)
    t.update_next(900.0, 122_000, 10_000)
    state = t.classify(now_ts=900.0, is_expiry_week=True)
    assert state.rollover_detected is True
    assert "ROLLOVER" in state.reason


def test_divergent_magnitude_not_classified_as_rollover():
    t = FuturesRolloverTracker(change_window_min=15, rollover_match_tol_pct=25.0, min_move_pct=10.0)
    t.update_near(0.0, 1_000_000, 50_000)
    t.update_next(0.0, 100_000, 5_000)
    # Near-month falls ~20%, next-month only rises ~11% -- not comparable, a
    # genuine directional (long-unwinding) signal should NOT be suppressed.
    t.update_near(900.0, 800_000, 50_000)
    t.update_next(900.0, 111_000, 6_000)
    state = t.classify(now_ts=900.0, is_expiry_week=True)
    assert state.rollover_detected is False


def test_contract_switch_rule_flips_to_next_month():
    t = FuturesRolloverTracker()
    t.update_near(0.0, 1_000_000, 50_000)
    t.update_next(0.0, 100_000, 5_000)
    state = t.classify(now_ts=0.0, is_expiry_week=True)
    assert state.active_contract == "near"
    # Next-month OI overtakes near-month -- switch.
    t.update_near(900.0, 300_000, 20_000)
    t.update_next(900.0, 1_200_000, 90_000)
    state = t.classify(now_ts=900.0, is_expiry_week=True)
    assert state.active_contract == "next"


def test_no_next_month_data_outside_expiry_week_never_detects_rollover():
    t = FuturesRolloverTracker()
    t.update_near(0.0, 1_000_000, 50_000)
    t.update_near(900.0, 500_000, 50_000)
    state = t.classify(now_ts=900.0, is_expiry_week=True)
    # is_expiry_week True but no next-month data at all -- falls back to
    # standard tracking rather than crashing or false-flagging.
    assert state.rollover_detected is False
