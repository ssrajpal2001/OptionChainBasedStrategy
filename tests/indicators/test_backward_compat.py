import numpy as np


def test_existing_imports_still_work():
    from matrix_engine.indicators import rsi, adx, ema, atr, volume_spike
    from matrix_engine.indicators import RSI_PERIOD, VWAP_WINDOW, ADX_PERIOD
    assert RSI_PERIOD == 14 and VWAP_WINDOW == 500 and ADX_PERIOD == 20


def test_rsi_neutral_when_insufficient():
    from matrix_engine.indicators import rsi
    assert rsi(np.array([100.0, 101.0])) == 50.0


def test_new_indicators_exported():
    from matrix_engine.indicators import roc, vwap_slope, combined_vwap, leg_atp  # noqa: F401


def test_roc_formula():
    from matrix_engine.indicators import roc
    # 100*(110-100)/100 = 10.0, length=1
    assert abs(roc(np.array([100.0, 110.0]), length=1) - 10.0) < 1e-9


def test_vwap_slope_falling():
    from matrix_engine.indicators import vwap_slope
    rising_ok, falling_ok, v_curr, v_prev, cr, cf = vwap_slope([100.0, 102.0, 104.0], occurrences=1)
    # newest first: 100 < 102 -> falling now
    assert falling_ok is True and v_curr == 100.0 and v_prev == 102.0


def test_combined_vwap_sums_legs():
    from matrix_engine.indicators import combined_vwap
    assert combined_vwap([50.0, 60.0]) == 110.0
    assert combined_vwap([50.0, 0.0]) is None  # missing leg
