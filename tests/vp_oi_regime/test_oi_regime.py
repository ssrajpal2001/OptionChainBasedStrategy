import pytest

from strategies.vp_oi_regime.oi_regime import OiRegimeTracker


def _seed(tracker, strike, side, oi_start, oi_now, t0=0.0, t_now=300.0):
    tracker.update_tick(strike, side, oi_start, t0)
    tracker.update_tick(strike, side, oi_now, t_now)


def test_rejects_bad_window():
    with pytest.raises(ValueError):
        OiRegimeTracker(change_window_min=10)


def test_no_data_returns_none():
    t = OiRegimeTracker()
    assert t.classify(spot=22500, minute_ts=300.0) is None


def test_first_evaluation_establishes_anchor_as_no_change():
    """The very first classify() call for a side has nothing to compare
    against yet -- it must establish the anchor and report 'No Change',
    never crash or guess a direction from zero history."""
    t = OiRegimeTracker(change_window_min=5)
    atm = 22500
    for off in range(-5, 6):
        strike = atm + off * 50
        t.update_tick(strike, "CE", 100000, 0.0)
        t.update_tick(strike, "PE", 100000, 0.0)
    res = t.classify(spot=float(atm), minute_ts=0.0)
    assert res is not None
    assert res.call_trend == "No Change"
    assert res.put_trend == "No Change"
    assert res.call_anchor_oi == pytest.approx(res.call_total_now)


def test_basic_rise_trend_detected_on_second_evaluation():
    t = OiRegimeTracker(change_window_min=5, trend_pct=3.0)
    atm = 22500
    for off in range(-5, 6):
        strike = atm + off * 50
        t.update_tick(strike, "CE", 100000, 0.0)
        t.update_tick(strike, "PE", 100000, 0.0)
    t.classify(spot=float(atm), minute_ts=0.0)  # establishes anchor

    # 5 minutes later, PE band total rises sharply -- first real evaluation.
    for off in range(-5, 6):
        strike = atm + off * 50
        t.update_tick(strike, "PE", 150000, 300.0)
        t.update_tick(strike, "CE", 100000, 300.0)
    res = t.classify(spot=float(atm), minute_ts=300.0)
    assert res is not None
    assert res.put_trend == "Rise"
    assert res.call_trend == "No Change"


def test_sticky_latch_holds_rise_through_a_later_no_change_reading():
    """2026-10-03 direct user spec: once Rise is confirmed, a LATER reading
    that itself would compute as 'No Change' against the NEW anchor must
    still report 'Rise' -- not revert to 'No Change'. Only an opposite
    (Fall) confirmed trigger may change it."""
    t = OiRegimeTracker(change_window_min=5, trend_pct=3.0)
    atm = 22500
    strikes = [atm + off * 50 for off in range(-5, 6)]
    for strike in strikes:
        t.update_tick(strike, "PE", 100000, 0.0)
        t.update_tick(strike, "CE", 100000, 0.0)
    t.classify(spot=float(atm), minute_ts=0.0)  # anchor = 1,100,000 (11 strikes)

    # t=300: PE rises 50% -- confirmed Rise, anchor moves to the new total.
    for strike in strikes:
        t.update_tick(strike, "PE", 150000, 300.0)
        t.update_tick(strike, "CE", 100000, 300.0)
    res1 = t.classify(spot=float(atm), minute_ts=300.0)
    assert res1.put_trend == "Rise"
    anchor_after_rise = res1.put_anchor_oi

    # t=600: PE barely moves (+1%, well under 3% vs the NEW anchor) -- the
    # instantaneous calc alone would say "No Change", but the sticky latch
    # must keep reporting "Rise".
    for strike in strikes:
        t.update_tick(strike, "PE", 151500, 600.0)  # ~+1% vs anchor_after_rise
        t.update_tick(strike, "CE", 100000, 600.0)
    res2 = t.classify(spot=float(atm), minute_ts=600.0)
    assert res2.put_trend == "Rise", "sticky latch must hold Rise through a sub-threshold reading"
    assert res2.put_anchor_oi == pytest.approx(anchor_after_rise), "anchor must NOT move on a non-trigger reading"


def test_sticky_latch_flips_only_on_opposite_confirmed_trigger():
    t = OiRegimeTracker(change_window_min=5, trend_pct=3.0)
    atm = 22500
    strikes = [atm + off * 50 for off in range(-5, 6)]
    for strike in strikes:
        t.update_tick(strike, "PE", 100000, 0.0)
        t.update_tick(strike, "CE", 100000, 0.0)
    t.classify(spot=float(atm), minute_ts=0.0)

    for strike in strikes:
        t.update_tick(strike, "PE", 150000, 300.0)
        t.update_tick(strike, "CE", 100000, 300.0)
    res1 = t.classify(spot=float(atm), minute_ts=300.0)
    assert res1.put_trend == "Rise"

    # A later confirmed FALL (-10% vs the Rise-updated anchor) must flip it.
    for strike in strikes:
        t.update_tick(strike, "PE", 135000, 600.0)  # -10% vs 150000*11 anchor
        t.update_tick(strike, "CE", 100000, 600.0)
    res2 = t.classify(spot=float(atm), minute_ts=600.0)
    assert res2.put_trend == "Fall"


def test_evaluation_only_happens_once_per_change_window_cadence():
    """classify() may be called far more often than change_window_min (e.g.
    every real tick in production) -- the latch must only actually
    re-evaluate/move its anchor once that many minutes have genuinely
    elapsed since ITS OWN last evaluation, regardless of call frequency."""
    t = OiRegimeTracker(change_window_min=5, trend_pct=3.0)
    atm = 22500
    strikes = [atm + off * 50 for off in range(-5, 6)]
    for strike in strikes:
        t.update_tick(strike, "PE", 100000, 0.0)
        t.update_tick(strike, "CE", 100000, 0.0)
    t.classify(spot=float(atm), minute_ts=0.0)

    # A huge jump arrives at t=60 (only 1 minute later, inside the 5-min
    # cadence) -- must NOT be evaluated/latched yet.
    for strike in strikes:
        t.update_tick(strike, "PE", 200000, 60.0)
        t.update_tick(strike, "CE", 100000, 60.0)
    res_early = t.classify(spot=float(atm), minute_ts=60.0)
    assert res_early.put_trend == "No Change", "must not evaluate before change_window_min has elapsed"

    # At t=300 (5 real minutes since the t=0 anchor), the same data IS due
    # for evaluation and should now confirm Rise.
    res_due = t.classify(spot=float(atm), minute_ts=300.0)
    assert res_due.put_trend == "Rise"


def test_rule1_noise_floor_drops_small_strikes_before_summing():
    t = OiRegimeTracker(change_window_min=5, noise_floor_pct=15.0)
    atm = 22500
    t.update_tick(atm, "CE", 1_000_000, 0.0)
    t.update_tick(atm + 50, "CE", 1_000, 0.0)
    for off in range(-5, 6):
        if off in (0, 1):
            continue
        t.update_tick(atm + off * 50, "CE", 10, 0.0)
    for off in range(-5, 6):
        t.update_tick(atm + off * 50, "PE", 100000, 0.0)
    t.classify(spot=float(atm), minute_ts=0.0)

    # t=300: the tiny strike spikes +4900% (should be filtered, Rule 1),
    # the dominant strike is unchanged.
    t.update_tick(atm, "CE", 1_000_000, 300.0)
    t.update_tick(atm + 50, "CE", 50_000, 300.0)
    for off in range(-5, 6):
        if off in (0, 1):
            continue
        t.update_tick(atm + off * 50, "CE", 10, 300.0)
    for off in range(-5, 6):
        t.update_tick(atm + off * 50, "PE", 100000, 300.0)
    res = t.classify(spot=float(atm), minute_ts=300.0)
    assert res is not None
    assert res.call_trend == "No Change"


def test_rule2_hard_wall_suppresses_change_in_oi_near_wall():
    t = OiRegimeTracker(change_window_min=5, hard_wall_pts=50.0, override_pct=30.0)
    atm = 22500
    for off in range(-5, 6):
        t.update_tick(atm + off * 50, "PE", 100000, 0.0)
        t.update_tick(atm + off * 50, "CE", 500000 if off == 0 else 10000, 0.0)
    t.classify(spot=float(atm), minute_ts=0.0)  # establish anchor

    for off in range(-5, 6):
        t.update_tick(atm + off * 50, "PE", 100000, 300.0)
    t.update_tick(atm, "CE", 550000, 300.0)  # wall strike +10% -- < 30% override
    for off in range(-5, 6):
        if off == 0:
            continue
        t.update_tick(atm + off * 50, "CE", 10000, 300.0)
    res = t.classify(spot=float(atm), minute_ts=300.0)
    assert res is not None
    assert res.call_trend == "No Change"
    assert any("WALL HOLDS" in n for n in res.wall_notes)


def test_rule3_momentum_override_breaks_the_wall():
    t = OiRegimeTracker(change_window_min=5, hard_wall_pts=50.0, override_pct=30.0)
    atm = 22500
    for off in range(-5, 6):
        t.update_tick(atm + off * 50, "PE", 100000, 0.0)
        t.update_tick(atm + off * 50, "CE", 500000 if off == 0 else 10000, 0.0)
    t.classify(spot=float(atm), minute_ts=0.0)

    for off in range(-5, 6):
        t.update_tick(atm + off * 50, "PE", 100000, 300.0)
    t.update_tick(atm, "CE", 700000, 300.0)  # wall strike +40% -- >= 30% override
    for off in range(-5, 6):
        if off == 0:
            continue
        t.update_tick(atm + off * 50, "CE", 10000, 300.0)
    res = t.classify(spot=float(atm), minute_ts=300.0)
    assert res is not None
    assert any("WALL BREACH OVERRIDE" in n for n in res.wall_notes)


def test_rollover_active_suppresses_both_trends():
    t = OiRegimeTracker(change_window_min=5)
    atm = 22500
    for off in range(-5, 6):
        t.update_tick(atm + off * 50, "CE", 100000, 0.0)
        t.update_tick(atm + off * 50, "PE", 100000, 0.0)
    t.classify(spot=float(atm), minute_ts=0.0)  # establish anchor

    for off in range(-5, 6):
        t.update_tick(atm + off * 50, "CE", 150000, 300.0)  # would confirm Rise...
        t.update_tick(atm + off * 50, "PE", 150000, 300.0)
    res = t.classify(spot=float(atm), minute_ts=300.0, rollover_active=True)
    assert res is not None
    assert res.call_trend == "No Change", "rollover must force No Change even though a real Rise would confirm"
    assert res.put_trend == "No Change"
    assert res.rollover_suppressed is True
