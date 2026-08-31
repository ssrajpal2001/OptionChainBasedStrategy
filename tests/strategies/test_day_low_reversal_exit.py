"""
Regression tests for the day-low reversal exit (2026-08-18/19, direct user
spec; ONE-TIME REST CALC redesign 2026-08-21). The straddle's combined
premium typically bottoms out somewhere in the 09:15-15:00 window then
reverses upward into the close, eating back into profit already banked on
paper.

**2026-08-21 redesign** -- the original design tick-tracked a running
minimum in memory from whenever the pair started running, only persisting
to disk on real position-lifecycle events (entry/roll/exit). Real incident:
a genuine 95.05 low at 14:20 was correctly tracked live, then silently
erased by a routine mid-day restart at 14:49 -- well before the 15:00
freeze -- leaving the frozen value at 97.15 instead of the true 95.05.
Direct user correction: don't track anything continuously at all. Do
NOTHING until `day_low_freeze_time` (default 15:00 IST) is reached. The
moment it is (by however many restarts it took to get there), do exactly
ONE REST fetch covering the pair's entire real trading history up to that
moment and compute the true low directly from it -- `_compute_day_low_for_
pair()`, correct regardless of restart count since it's derived from real
historical data, not accumulated live state. Combines CE.low + PE.low
(direct user instruction, overriding this feature's own prior 2026-08-19
CLOSE-based design).

Opt-in (`day_low_exit_enabled`, default OFF) -- must never silently
activate on an existing live deployment.

**Scoped to the running pair, not the whole day** -- a rollover/re-entry
mid-day resets `_day_low_tracked_pair` and discards the prior pair's frozen
low; the new pair gets its own fresh one-time calc once freeze time is
reached (immediately, if freeze time has already elapsed by the time the
new pair starts running).

Tests mock `s._compute_day_low_for_pair` directly (an AsyncMock) rather
than hitting the network -- `tests/strategies/test_day_low_seed_rest_fetch.py`
covers the REST fetch/hour-minute-alignment logic itself in isolation.

Tests use `_day_low_freeze_time = time(23, 59, 59)` to guarantee the freeze
condition is false regardless of real wall-clock time the suite runs at, and
`time(0, 0)` to guarantee it's true -- avoids mocking datetime.now() entirely.
"""
import asyncio
from datetime import time as dtime
from unittest.mock import ANY, AsyncMock

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
    # 2026-08-31: post1500_exit_enabled now defaults ON (direct user spec)
    # and shares the SAME frozen-low tracking block as day_low_exit_enabled
    # -- this file tests day_low_exit_enabled in isolation, so force
    # post1500 off here (it has its own dedicated test file).
    s._post1500_exit_enabled = False
    # No REST calc result by default -- degrades to "freeze at the current
    # tick's own value", matching every test below unless overridden.
    s._compute_day_low_for_pair = AsyncMock(return_value=float("inf"))
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
    assert s._session_min_straddle_frozen is None, "must not freeze when the toggle is off"
    assert s._day_low_tracked_pair is None
    s._compute_day_low_for_pair.assert_not_called()
    assert s._position is not None and s._position.status == "open"


def test_no_computation_before_freeze_time():
    s = _strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = dtime(23, 59, 59)   # never reached during a normal test run

    s._position = _position(30.0, 20.0)
    asyncio.run(s._check_exits())
    s._position = _position(25.0, 15.0)
    asyncio.run(s._check_exits())

    assert s._session_min_straddle_frozen is None
    s._compute_day_low_for_pair.assert_not_called()
    assert s._position is not None and s._position.status == "open", "must not exit before freeze time"


def test_freeze_computes_once_via_rest_at_freeze_time():
    s = _strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = dtime(0, 0)   # always in the past -- computes on the very first tick
    s._defer_exit = lambda reason, now: True
    s._compute_day_low_for_pair = AsyncMock(return_value=95.05)

    s._position = _position(60.0, 40.0)   # current_value = 100, above the computed 95.05
    asyncio.run(s._check_exits())

    s._compute_day_low_for_pair.assert_awaited_once_with(24000, 24000, ANY)   # cutoff = real now.time(), not the fixed constant
    assert s._session_min_straddle_frozen == 95.05


def test_freeze_only_computed_once_not_recomputed_on_later_ticks():
    """Direct user spec (2026-08-21): 'it should call once at 15:00
    irrespective of how many times application starts' -- once frozen, later
    ticks (or later restarts that restore the already-frozen value) must
    never trigger a second REST calc."""
    s = _strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = dtime(0, 0)
    s._defer_exit = lambda reason, now: True
    s._compute_day_low_for_pair = AsyncMock(return_value=95.05)

    s._position = _position(60.0, 40.0)   # 100 -- triggers the one-time calc
    asyncio.run(s._check_exits())
    assert s._compute_day_low_for_pair.await_count == 1

    s._position = _position(55.0, 42.0)   # 97 -- still above frozen, must NOT recompute
    asyncio.run(s._check_exits())
    assert s._compute_day_low_for_pair.await_count == 1
    assert s._session_min_straddle_frozen == 95.05


def test_freeze_tick_that_is_itself_the_days_low_exits_immediately_same_tick():
    """If the REST-computed low exactly equals the freeze tick's own current
    value, no separate retest tick is required -- exits on the same tick the
    freeze happens."""
    s = _strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = dtime(0, 0)
    s._defer_exit = lambda reason, now: True
    close_calls = _spy_close(s)
    s._compute_day_low_for_pair = AsyncMock(return_value=50.0)

    s._position = _position(30.0, 20.0)   # current_value = 50, matches the computed low exactly
    asyncio.run(s._check_exits())

    assert close_calls == ["day_low_reversal_exit"], (
        "the freeze tick itself must fire the exit when its own reading equals the computed day-low"
    )
    assert s._stop_for_day is True


def test_freeze_above_current_tick_waits_for_a_later_retest():
    s = _strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = dtime(0, 0)
    s._defer_exit = lambda reason, now: True
    close_calls = _spy_close(s)
    s._compute_day_low_for_pair = AsyncMock(return_value=40.0)   # real historical low, below this tick

    s._position = _position(30.0, 20.0)   # current_value = 50 -- above the frozen 40, no exit yet
    asyncio.run(s._check_exits())
    assert s._session_min_straddle_frozen == 40.0
    assert not close_calls

    s._position = _position(33.0, 22.0)   # 55 -- still above, no exit
    asyncio.run(s._check_exits())
    assert not close_calls

    s._position = _position(25.0, 15.0)   # 40 -- retests the frozen low exactly -- exits
    asyncio.run(s._check_exits())
    assert close_calls == ["day_low_reversal_exit"]
    assert s._stop_for_day is True


def test_retest_undercutting_the_frozen_low_also_exits():
    s = _strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = dtime(0, 0)
    s._defer_exit = lambda reason, now: True
    close_calls = _spy_close(s)
    s._compute_day_low_for_pair = AsyncMock(return_value=40.0)

    s._position = _position(30.0, 20.0)   # 50 -- above frozen(40), no exit
    asyncio.run(s._check_exits())
    assert s._session_min_straddle_frozen == 40.0
    assert not close_calls

    s._position = _position(18.0, 12.0)   # 30 -- undercuts the frozen low (40)
    asyncio.run(s._check_exits())
    assert close_calls == ["day_low_reversal_exit"]


def test_rest_calc_failure_falls_back_to_current_tick_value():
    """If the REST calc fails (returns inf, e.g. network error), the freeze
    falls back to the current tick's own value rather than blocking or
    leaving the feature silently inert."""
    s = _strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = dtime(0, 0)
    s._defer_exit = lambda reason, now: True
    s._compute_day_low_for_pair = AsyncMock(return_value=float("inf"))

    s._position = _position(30.0, 20.0)   # current_value = 50
    asyncio.run(s._check_exits())
    assert s._session_min_straddle_frozen == 50.0


def test_persists_session_when_freeze_computes():
    s = _strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = dtime(0, 0)
    s._defer_exit = lambda reason, now: True
    s._compute_day_low_for_pair = AsyncMock(return_value=40.0)
    persisted = []
    s._persist_session = lambda: persisted.append(s._session_min_straddle_frozen)

    s._position = _position(30.0, 20.0)   # 50 -- triggers the one-time calc + freeze
    asyncio.run(s._check_exits())
    assert 40.0 in persisted


def test_tracking_resets_on_a_mid_day_rollover_onto_a_new_pair():
    """A rollover DISCARDS the previous pair's frozen low -- the day-low exit
    tracks only the currently running pair's own history, never the whole
    day/whole-book history."""
    s = _strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = dtime(23, 59, 59)   # not reached yet
    close_calls = _spy_close(s)

    s._position = _position(30.0, 20.0)   # 50
    asyncio.run(s._check_exits())
    assert s._day_low_tracked_pair == (24000, 24000)
    assert s._session_min_straddle_frozen is None   # not frozen yet -- before freeze time
    s._compute_day_low_for_pair.assert_not_called()

    # Rollover onto a DIFFERENT pair (24100/23900).
    s._position = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 24100, 40.0, 40.0),
        pe_leg=StraddleLeg("PE", 23900, 30.0, 30.0),
        net_credit=70.0, status="open",
    )
    asyncio.run(s._check_exits())
    assert s._day_low_tracked_pair == (24100, 23900)
    assert s._session_min_straddle_frozen is None

    # Cross freeze time on this NEW pair -- computes fresh for CE24100/PE23900,
    # never touching the discarded original pair's strikes.
    s._day_low_freeze_time = dtime(0, 0)
    s._defer_exit = lambda reason, now: True
    s._compute_day_low_for_pair = AsyncMock(return_value=60.0)
    s._position = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 24100, 38.0, 38.0),
        pe_leg=StraddleLeg("PE", 23900, 28.0, 28.0),
        net_credit=70.0, status="open",
    )   # current_value = 66 -- above the computed 60
    asyncio.run(s._check_exits())
    s._compute_day_low_for_pair.assert_awaited_once_with(24100, 23900, ANY)   # cutoff = real now.time(), not the fixed constant
    assert s._session_min_straddle_frozen == 60.0
    assert not close_calls

    # Retest of this pair's own frozen low (60) -- exits.
    s._position = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 24100, 35.0, 35.0),
        pe_leg=StraddleLeg("PE", 23900, 25.0, 25.0),
        net_credit=70.0, status="open",
    )   # 60
    asyncio.run(s._check_exits())
    assert close_calls == ["day_low_reversal_exit"]


def test_pair_starting_after_freeze_time_computes_immediately_using_now_as_cutoff():
    """A pair that only starts running AFTER the freeze time has already
    elapsed (e.g. a roll at 15:10 with freeze=15:00) still gets a correct
    one-time calc -- triggered immediately since 'now >= freeze_time' is
    already true, using the real cutoff time (not the fixed freeze-time
    constant) so it captures this pair's true history up to the moment it's
    actually checked."""
    s = _strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = dtime(0, 0)   # already "past" freeze time
    s._defer_exit = lambda reason, now: True
    close_calls = _spy_close(s)
    s._compute_day_low_for_pair = AsyncMock(return_value=50.0)

    s._position = _position(40.0, 30.0)   # 70 -- this pair's first-ever tick, already past freeze
    asyncio.run(s._check_exits())
    assert s._session_min_straddle_frozen == 50.0
    assert not close_calls, "must not self-fire at 70 -- the computed low (50) is below this tick's own value"

    s._position = _position(25.0, 25.0)   # 50 -- retests the computed low
    asyncio.run(s._check_exits())
    assert close_calls == ["day_low_reversal_exit"]
