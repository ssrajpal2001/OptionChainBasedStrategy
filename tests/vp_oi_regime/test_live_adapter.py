from strategies.vp_oi_regime.live_adapter import VpOiRegimeAdapter


def _warm(adapter, strikes, side, oi, ts):
    for s in strikes:
        adapter.on_option_oi_tick(s, side, oi, ts)


def test_evaluate_returns_none_until_oi_is_warm():
    a = VpOiRegimeAdapter()
    assert a.evaluate(spot=24000.0, now_ts=1000.0) is None


def test_evaluate_returns_decision_once_oi_fed_both_sides():
    a = VpOiRegimeAdapter()
    strikes = [24000 + i * 50 for i in range(-5, 6)]
    _warm(a, strikes, "CE", 1000.0, 0.0)
    _warm(a, strikes, "PE", 1000.0, 0.0)
    a.on_futures_bar(high=24010, low=23990, volume=100, oi=50000, ts=0.0)
    dr = a.evaluate(spot=24000.0, now_ts=0.0)
    assert dr is not None
    assert dr.regime == "Neutral"


def test_naked_leg_lifecycle_and_reentry():
    a = VpOiRegimeAdapter()
    a.mark_naked("NAKED_CE", {"CE": 100.0})
    assert a.naked_state == "NAKED_CE"

    for p in (18.0, 19.0, 20.0, 22.0, 24.0, 26.0, 28.0, 30.0, 32.0):
        a.on_naked_leg_price("CE", p)
    assert a.stop_hit("CE", 20.0) is True

    a.on_naked_stopped("CE")
    assert a.naked_state == "NONE"
    assert a.awaiting_reentry["CE"] is True

    a.last_poc = 24000.0
    assert a.check_reentry("CE", 24050.0) is False  # CE waits for close BELOW POC
    assert a.check_reentry("CE", 23950.0) is True
    assert a.awaiting_reentry["CE"] is False


def test_reset_clears_naked_and_reentry_state():
    a = VpOiRegimeAdapter()
    a.mark_naked("NAKED_BOTH", {"CE": 100.0, "PE": 90.0})
    a.on_naked_stopped("CE")
    a.reset()
    assert a.naked_state == "NONE"
    assert a.awaiting_reentry == {}
    assert a.naked_entry_premium == {}


def test_monitoring_state_shape():
    a = VpOiRegimeAdapter()
    state = a.monitoring_state()
    assert state["regime"] is None
    assert state["naked_state"] == "NONE"
