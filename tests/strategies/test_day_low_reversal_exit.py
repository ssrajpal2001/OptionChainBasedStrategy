"""
Regression tests for the day-low reversal exit (2026-08-18, direct user spec):
the straddle's combined premium (CE_ltp+PE_ltp) typically bottoms out somewhere
in the 09:15-15:00 window then reverses upward into the close, eating back
into profit already banked on paper. Track the day's running-min combined
premium from position open; freeze it ONCE at `day_low_freeze_time` (default
15:00 IST) using whatever the running min is at that instant. From the freeze
tick onward -- INCLUDING the freeze tick itself, if that reading happens to BE
the day's low -- the moment the current combined premium reaches (or
undercuts) the frozen value, close the whole position and stop for the day.

Opt-in (`day_low_exit_enabled`, default OFF) -- brand new, unvalidated,
must never silently activate on an existing live deployment.

Tests use `_day_low_freeze_time = time(23, 59, 59)` to guarantee the freeze
condition is false regardless of real wall-clock time the suite runs at, and
`time(0, 0)` to guarantee it's true -- avoids mocking datetime.now() entirely.
"""
import asyncio
from datetime import time as dtime

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg


def _strategy():
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
    s._lot_size = 75
    s._lot_multiplier = 1
    s._spot = 24000.0
    s._force_exit = dtime(23, 59)
    s._itm_pair_gate_enabled = False
    s._ltp_decay_enabled = False
    s._tsl_enabled = False
    s._vwap_rise_enabled = False
    s._exit_rules = []
    s._day_profit_target_pct = 0.0
    s._day_loss_sl_pct = 0.0
    s._ratio_threshold = 999.0
    return s


def _position(ce_ltp: float, pe_ltp: float) -> StraddlePosition:
    return StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 24000, ce_ltp, ce_ltp),
        pe_leg=StraddleLeg("PE", 24000, pe_ltp, pe_ltp),
        net_credit=ce_ltp + pe_ltp, status="open",
    )


def _spy_close(s):
    calls = []

    async def _fake(reason):
        calls.append(reason)
        s._position.status = "closed"
        s._position = None
    s._close_position = _fake
    return calls


def test_disabled_by_default_no_tracking_no_exit():
    s = _strategy()
    assert s._day_low_exit_enabled is False   # wiring sanity check
    assert s._day_low_freeze_time == dtime(15, 0), "default freeze time must be 15:00 per spec"
    s._position = _position(30.0, 20.0)   # current_value = 50
    asyncio.run(s._check_exits())
    assert s._session_min_straddle_value == float("inf"), "must not track when the toggle is off"
    assert s._position is not None and s._position.status == "open"


def test_tracks_running_min_before_freeze_time():
    s = _strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = dtime(23, 59, 59)   # never reached during a normal test run

    s._position = _position(30.0, 20.0)   # current_value = 50
    asyncio.run(s._check_exits())
    s._position = _position(25.0, 15.0)   # current_value = 40 -- new low
    asyncio.run(s._check_exits())
    s._position = _position(28.0, 19.0)   # current_value = 47 -- bounced, not a new low
    asyncio.run(s._check_exits())

    assert s._session_min_straddle_value == 40.0
    assert s._session_min_straddle_frozen is None
    assert s._position is not None and s._position.status == "open", "must not exit before freeze time"


def test_freeze_tick_that_is_itself_the_days_low_exits_immediately_same_tick():
    """User's explicit clarification: 'if for instance the lowest value is the
    freeze-time value then also we exit' -- no separate retest tick required
    when the freeze-time reading itself turns out to be the day's low."""
    s = _strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = dtime(0, 0)   # always in the past -- freezes on the very first tick
    s._defer_exit = lambda reason, now: True   # bypass the generic tf-boundary micro-timing (untouched by this feature)
    close_calls = _spy_close(s)

    s._position = _position(30.0, 20.0)   # first-ever tick: freezes at 50 AND IS the low so far
    asyncio.run(s._check_exits())

    assert close_calls == ["day_low_reversal_exit"], (
        "the freeze tick itself must fire the exit when its own reading is the day's low"
    )
    assert s._stop_for_day is True


def test_freeze_below_freeze_tick_value_waits_for_a_later_retest():
    s = _strategy()
    s._day_low_exit_enabled = True
    s._defer_exit = lambda reason, now: True
    close_calls = _spy_close(s)

    # Establish a lower running-min BEFORE the freeze time.
    s._day_low_freeze_time = dtime(23, 59, 59)   # not reached yet
    s._position = _position(25.0, 15.0)   # current_value = 40 -- becomes the running min
    asyncio.run(s._check_exits())
    assert s._session_min_straddle_value == 40.0
    assert not close_calls

    # Now cross the freeze time on a HIGHER reading (50) -- freezes at the
    # earlier-recorded 40, not at this tick's own 50, so no exit yet.
    s._day_low_freeze_time = dtime(0, 0)
    s._position = _position(30.0, 20.0)   # current_value = 50
    asyncio.run(s._check_exits())
    assert s._session_min_straddle_frozen == 40.0
    assert not close_calls, "the freeze tick's own value (50) is above the frozen low (40) -- no exit yet"

    # A later tick bounces further (55) -- still no exit.
    s._position = _position(33.0, 22.0)
    asyncio.run(s._check_exits())
    assert not close_calls

    # A later tick retests the frozen low exactly (40) -- exits.
    s._position = _position(25.0, 15.0)
    asyncio.run(s._check_exits())
    assert close_calls == ["day_low_reversal_exit"]
    assert s._stop_for_day is True


def test_retest_undercutting_the_frozen_low_also_exits():
    s = _strategy()
    s._day_low_exit_enabled = True
    s._defer_exit = lambda reason, now: True
    close_calls = _spy_close(s)

    s._day_low_freeze_time = dtime(23, 59, 59)
    s._position = _position(25.0, 15.0)   # running min = 40
    asyncio.run(s._check_exits())

    s._day_low_freeze_time = dtime(0, 0)
    s._position = _position(30.0, 20.0)   # freeze tick reading = 50 > frozen(40) -- no exit
    asyncio.run(s._check_exits())
    assert s._session_min_straddle_frozen == 40.0
    assert not close_calls

    s._position = _position(18.0, 12.0)   # 30 -- undercuts the frozen low (40)
    asyncio.run(s._check_exits())
    assert close_calls == ["day_low_reversal_exit"]
