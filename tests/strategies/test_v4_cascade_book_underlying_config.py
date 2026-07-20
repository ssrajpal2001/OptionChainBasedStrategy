"""V4CascadeBook must resolve session-open time, EOD/gate23 reset time, and
strike/offset numbers per-underlying: NIFTY unchanged, CRUDEOIL gets MCX's
09:00 open / 23:15 squareoff / 23:30 gate23 reset / 100-point strike step /
400-point tracking offset / 100-point execution offset."""
from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook, _TRACKING_STRIKE_STEP


def _book(underlying, squareoff_time="15:15"):
    cfg = GlobalConfig()
    return V4CascadeBook(
        EventBus(), cfg, underlying=underlying, client_id="C1", binding_id="B1",
        lot_multiplier=2, squareoff_time=squareoff_time,
    )


def test_nifty_session_config_unchanged():
    b = _book("NIFTY", squareoff_time="15:15")
    assert b._is_mcx is False
    assert b._session_open == (9, 15)
    assert b._eod_hour_min == (15, 15)
    assert b._gate23_hour_min == (15, 30)
    assert b._strike_step == 50.0
    assert b._tracking_offset == 200.0
    assert b._execution_offset == 50.0


def test_crudeoil_session_config():
    b = _book("CRUDEOIL", squareoff_time="23:15")
    assert b._is_mcx is True
    assert b._session_open == (9, 0)
    assert b._eod_hour_min == (23, 15)
    assert b._gate23_hour_min == (23, 30)
    assert b._strike_step == 100.0
    assert b._tracking_offset == 400.0
    assert b._execution_offset == 100.0
    assert b._v4cfg.sl_buffer == 20.0
    assert b._v4cfg.lot_size == 100


def test_tracking_strike_step_is_flat_100_regardless_of_underlying():
    # Regression guard: the TRACKING contract's ATM rounding is deliberately
    # a flat 100 for every underlying -- it is NOT the same value as
    # self._strike_step (the real per-underlying execution grid, 50 for
    # NIFTY / 100 for CRUDEOIL). A prior pass incorrectly unified these,
    # which would have changed NIFTY's live tracking strikes; user caught
    # it and confirmed NIFTY's tracking rounding must stay 100.
    assert _TRACKING_STRIKE_STEP == 100.0


def test_gate23_reset_wraps_past_midnight_safely():
    # Not applicable today (23:15 + 15min = 23:30, same day) but guards
    # against a future squareoff_time near midnight producing an invalid
    # (hour>=24) tuple -- gate23_hour_min must clamp to (23, 59) rather than
    # overflow.
    b = _book("CRUDEOIL", squareoff_time="23:50")
    assert b._gate23_hour_min == (23, 59)
