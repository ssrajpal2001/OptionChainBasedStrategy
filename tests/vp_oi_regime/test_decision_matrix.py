import pytest

from strategies.vp_oi_regime.decision_matrix import (
    decide,
    hedge_target_premium,
    pick_strike_by_premium_match,
    NineEmaTrailingStop,
)


def test_all_27_rows_resolve():
    trends = ("Rise", "No Change", "Fall")
    count = 0
    for f in trends:
        for p in trends:
            for c in trends:
                res = decide(f, p, c)
                assert res.regime
                count += 1
    assert count == 27


def test_highly_bullish_buildup_hedge_sizing_70_vs_nothing():
    """2026-10-04 fix, authoritative source (Obs 6): Future OI RISING
    'Long Buildup' uses 70% on the exited Call leg and NOTHING on the
    surviving Put leg -- not the 15-20%/5% split (that's the Falling-OI
    'Short Covering' variant instead, see the next test)."""
    res = decide("Rise", "Rise", "Fall")
    assert res.regime == "Highly Bullish (Long Buildup)"
    assert res.call_action == "Exit call leg"
    assert res.call_hedge.pct_of_premium == (0.70, 0.70)
    assert res.put_hedge.enabled is False


def test_highly_bullish_covering_hedge_sizing_matches_buildup():
    """2026-10-04 correction: Obs 24 ('Short Covering') must behave
    IDENTICALLY to Obs 6 ('Long Buildup') -- 70%/NOTHING, not 15-20%/5%."""
    res = decide("Fall", "Rise", "Fall")
    assert res.regime == "Highly Bullish (Short Covering)"
    assert res.call_hedge.pct_of_premium == (0.70, 0.70)
    assert res.put_hedge.enabled is False


def test_highly_bearish_buildup_hedge_sizing_70_vs_nothing():
    """Future OI RISING 'Short Buildup' (Obs 5) uses 70% on the exited Put
    leg and NOTHING on the surviving Call leg."""
    res = decide("Rise", "Fall", "Rise")
    assert res.regime == "Highly Bearish (Short Buildup)"
    assert res.put_action == "Exit put leg"
    assert res.put_hedge.pct_of_premium == (0.70, 0.70)
    assert res.call_hedge.enabled is False


def test_highly_bearish_unwinding_hedge_sizing_matches_buildup():
    """2026-10-04 correction: Obs 23 ('Long Unwinding') must behave
    IDENTICALLY to Obs 5 ('Short Buildup') -- 70%/NOTHING, not 15-20%/5%."""
    res = decide("Fall", "Fall", "Rise")
    assert res.regime == "Highly Bearish (Long Unwinding)"
    assert res.put_hedge.pct_of_premium == (0.70, 0.70)
    assert res.call_hedge.enabled is False


def test_volatile_row_hedge_sizing_is_uniform_50_50():
    """2026-10-04 correction: ALL THREE Volatile rows (Obs 9/18/27) use the
    SAME 50%/50% split regardless of Future OI direction -- not a flat
    15-20% for the Falling-Future-OI row as an earlier revision had it."""
    for future in ("Rise", "No Change", "Fall"):
        res = decide(future, "Fall", "Fall")
        assert res.regime == "Volatile"
        assert res.call_hedge.pct_of_premium == (0.50, 0.50)
        assert res.put_hedge.pct_of_premium == (0.50, 0.50)


def test_neutral_row_no_hedge():
    res = decide("No Change", "No Change", "No Change")
    assert res.regime == "Neutral"
    assert res.call_hedge.enabled is False
    assert res.put_hedge.enabled is False


def test_invalid_trend_raises():
    with pytest.raises(ValueError):
        decide("Rise", "Up", "Down")


def test_hedge_target_premium_is_fraction_of_entry_not_current_ltp():
    res = decide("Fall", "Rise", "Fall")  # Highly Bullish/Short Covering: CE exited, 70%
    target = hedge_target_premium(res.call_hedge, exited_leg_entry_premium=120.0)
    assert target == (84.0, 84.0)


def test_hedge_target_premium_none_when_hedge_is_do_nothing():
    res = decide("Rise", "Rise", "Fall")  # Highly Bullish/Long Buildup: surviving PE = DO NOTHING
    assert hedge_target_premium(res.put_hedge, exited_leg_entry_premium=100.0) is None


def test_pick_strike_by_premium_match_prefers_inside_band():
    chain = {22600: 78.0, 22650: 58.0, 22700: 41.0, 22750: 27.5, 22800: 18.0, 22850: 11.5}
    picked = pick_strike_by_premium_match(chain, target_lo=18.0, target_hi=24.0)
    assert picked == (22800, 18.0)


def test_pick_strike_by_premium_match_falls_back_below_target():
    chain = {22600: 78.0, 22650: 58.0}  # nothing anywhere near a tiny target
    picked = pick_strike_by_premium_match(chain, target_lo=1.0, target_hi=5.0)
    assert picked is None  # nothing <= target_hi exists in this chain


def test_nine_ema_trailing_stop_long_call_exits_on_fall_through_ema():
    ema = NineEmaTrailingStop(period=9)
    for p in (18.0, 19.0, 20.0, 22.0, 24.0, 26.0, 28.0, 30.0, 32.0):
        ema.update(p)
    assert ema.stop_hit(price=20.0, direction="up") is True  # fell well below EMA
    assert ema.stop_hit(price=33.0, direction="up") is False  # still trending up


def test_nine_ema_trailing_stop_long_put_exits_on_rise_through_ema():
    ema = NineEmaTrailingStop(period=9)
    for p in (30.0, 28.0, 26.0, 24.0, 22.0, 20.0, 18.0, 16.0, 14.0):
        ema.update(p)
    assert ema.stop_hit(price=25.0, direction="down") is True  # rose back above EMA
    assert ema.stop_hit(price=12.0, direction="down") is False  # still falling
