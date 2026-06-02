"""Trap position persistence: a LIVE trade must survive a restart (save on entry,
restore on warm_start). Mirrors the IC/Straddle persistence, per symbol."""
from data_layer.base_feeder import EventBus
from config.global_config import GlobalConfig
from strategies.trap_trading_engine import TrapTradingEngine, _Phase
from data_layer import position_store as ps

SYM = "TESTTRAPSYM"


def _cleanup():
    ps.clear(f"{SYM}_trap")


def _live_engine():
    eng = TrapTradingEngine(EventBus(), GlobalConfig())
    st = eng._get_state(SYM)
    st.phase = _Phase.LIVE
    st.trade_id = "abc123"
    st.entry_price = 50.0
    st.quantity = 75
    st.ltf_sl_line = 45.0
    st.target_high = 65.0
    st.entry_origin = 23000.0
    eng._open_positions["abc123"] = ("abc123", "NIFTY23500CE", 50.0, 75)
    return eng


def test_trap_persist_and_restore_round_trip():
    _cleanup()
    try:
        _live_engine()._persist_trade(SYM)

        eng2 = TrapTradingEngine(EventBus(), GlobalConfig())
        eng2._restore_trade(SYM)
        st2 = eng2._states[SYM]
        assert st2.phase == _Phase.LIVE
        assert st2.trade_id == "abc123"
        assert st2.entry_price == 50.0
        assert st2.quantity == 75
        assert st2.ltf_sl_line == 45.0
        assert st2.target_high == 65.0
        assert st2.entry_origin == 23000.0
        assert eng2._open_positions["abc123"] == ("abc123", "NIFTY23500CE", 50.0, 75)
    finally:
        _cleanup()


def test_trap_clear_removes_persisted_trade():
    _cleanup()
    try:
        eng = _live_engine()
        eng._persist_trade(SYM)
        eng._clear_trade(SYM)

        eng2 = TrapTradingEngine(EventBus(), GlobalConfig())
        eng2._restore_trade(SYM)
        # No saved trade → state not created / no live trade restored.
        assert SYM not in eng2._states
        assert "abc123" not in eng2._open_positions
    finally:
        _cleanup()


def test_trap_persist_skips_when_not_live():
    _cleanup()
    try:
        eng = TrapTradingEngine(EventBus(), GlobalConfig())
        st = eng._get_state(SYM)
        st.phase = _Phase.IDLE   # not live → nothing to persist
        eng._persist_trade(SYM)
        eng2 = TrapTradingEngine(EventBus(), GlobalConfig())
        eng2._restore_trade(SYM)
        assert SYM not in eng2._states
    finally:
        _cleanup()
