"""TT v2 Task 5 — two-tier 1-min-sweep stop loss."""

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.trap_trading_engine import sl_triggered, TrapTradingEngine


# ── Pure predicate ───────────────────────────────────────────────────────────
def test_sl_triggered_uses_5m_low_before_1m_break():
    assert sl_triggered(ltp=498, sl_5m=500, sl_active=None) is True
    assert sl_triggered(ltp=502, sl_5m=500, sl_active=None) is False


def test_sl_triggered_uses_1m_low_after_break():
    # after a 1m close below the 5m low, the stop is the 1m low (495)
    assert sl_triggered(ltp=494, sl_5m=500, sl_active=495) is True
    assert sl_triggered(ltp=496, sl_5m=500, sl_active=495) is False  # between 495 and 500: no exit


# ── Engine position management ───────────────────────────────────────────────
def _pos_engine(sl_5m):
    eng = TrapTradingEngine(EventBus(), GlobalConfig())
    eng._v2_position = {
        "underlying": "CRUDEOIL", "opt_type": "CE", "strike": 8800, "qty": 100,
        "sl_5m": float(sl_5m), "sl_active": None, "entry_premium": 380.0,
    }
    return eng


def test_clean_trade_no_sl_break():
    eng = _pos_engine(500)
    assert eng._v2_maybe_stop(520) is False
    assert eng._v2_position is not None     # position intact


def test_direct_tick_breaks_5m_low_exits_and_clears():
    eng = _pos_engine(500)
    assert eng._v2_maybe_stop(498) is True
    assert eng._v2_position is None          # cleared cleanly


def test_1m_close_breach_activates_trailing_low():
    eng = _pos_engine(500)
    eng._v2_update_sl_on_1m_close(low=495, close=494)   # close < 5m low → activate tier 2
    assert eng._v2_position["sl_active"] == 495
    # between the 1m low (495) and the 5m low (500): no exit
    assert eng._v2_maybe_stop(496) is False
    assert eng._v2_position is not None


def test_1m_low_sweep_after_activation_exits():
    eng = _pos_engine(500)
    eng._v2_update_sl_on_1m_close(low=495, close=494)
    assert eng._v2_maybe_stop(494) is True    # sweeps the 1m low → exit
    assert eng._v2_position is None


def test_1m_close_above_5m_low_no_activation():
    eng = _pos_engine(500)
    eng._v2_update_sl_on_1m_close(low=505, close=510)   # close >= 5m low → no tier 2
    assert eng._v2_position["sl_active"] is None
