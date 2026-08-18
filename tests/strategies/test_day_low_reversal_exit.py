"""
Regression tests for the day-low reversal exit (2026-08-18/19, direct user
spec): the straddle's combined premium (CE_ltp+PE_ltp) typically bottoms out
somewhere in the 09:15-15:00 window then reverses upward into the close,
eating back into profit already banked on paper. Track the CURRENTLY RUNNING
pair's own running-min combined premium since IT started running; freeze it
ONCE at `day_low_freeze_time` (default 15:00 IST) using whatever this pair's
own low is at that instant. From the freeze tick onward -- INCLUDING the
freeze tick itself, if that reading happens to BE this pair's low so far --
the moment the current combined premium reaches (or undercuts) the frozen
value, close the whole position and stop for the day.

Opt-in (`day_low_exit_enabled`, default OFF) -- brand new, unvalidated,
must never silently activate on an existing live deployment.

**Scoped to the running pair, not the whole day** -- user's explicit
correction after an earlier draft tracked a whole-day/whole-book low across
rollovers: "exit will fire from the current running pair not the pair which
was there prev when the roll happened... when roll happened prev data is of
no use... will focus only on running legs and its own lowest point of the
day." `_day_low_tracked_pair` (a `(ce_strike, pe_strike)` tuple) identifies
which pair the tracker is currently following; the moment the running
position's own strikes differ from it, tracking resets from scratch.

**"From scratch" seeds from REAL history, not a blank slate** -- second
correction: "from scratch" does not mean literally starting the count from
whatever tick the book happens to notice the new pair on. The new pair was
genuinely trading on the real market since 09:15 even though this book only
just started holding it -- `_seed_day_low_for_pair()` REST-fetches today's
actual 1-min intraday history for the new strikes and seeds the tracker with
the true low already achieved before this book ever touched them (falls back
to "start from this tick" if the fetch fails for any reason). Tests mock
`s._seed_day_low_for_pair` directly (an AsyncMock returning `inf` by default
via `_strategy()`, i.e. "no seed available") rather than hitting the network
-- `tests/strategies/test_day_low_seed_rest_fetch.py` covers the REST
fetch/minute-alignment logic itself in isolation.

Tests use `_day_low_freeze_time = time(23, 59, 59)` to guarantee the freeze
condition is false regardless of real wall-clock time the suite runs at, and
`time(0, 0)` to guarantee it's true -- avoids mocking datetime.now() entirely.
"""
import asyncio
from datetime import time as dtime
from unittest.mock import AsyncMock

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
    # No REST seed available by default -- degrades to "start from this
    # tick", matching every test below unless a test overrides it explicitly.
    s._seed_day_low_for_pair = AsyncMock(return_value=float("inf"))
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
    assert s._day_low_tracked_pair is None
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


def test_tracking_resets_on_a_mid_day_rollover_onto_a_new_pair():
    """User's explicit correction: a rollover DISCARDS the previous pair's
    tracked low -- the day-low exit tracks ONLY the currently running pair's
    own lowest point since IT started running, never the whole day/whole-book
    history."""
    s = _strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = dtime(23, 59, 59)   # not reached yet
    close_calls = _spy_close(s)

    # Original pair (strikes 24000/24000) dips to a combined premium of 35.
    s._position = _position(30.0, 20.0)   # 50
    asyncio.run(s._check_exits())
    s._position = _position(20.0, 15.0)   # 35
    asyncio.run(s._check_exits())
    assert s._session_min_straddle_value == 35.0
    assert s._day_low_tracked_pair == (24000, 24000)

    # Rollover onto a DIFFERENT pair (24100/23900). Its own first reading (70)
    # must become the new running min -- the old pair's 35 is discarded, not
    # inherited.
    s._position = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 24100, 40.0, 40.0),
        pe_leg=StraddleLeg("PE", 23900, 30.0, 30.0),
        net_credit=70.0, status="open",
    )   # current_value = 70
    asyncio.run(s._check_exits())
    assert s._day_low_tracked_pair == (24100, 23900)
    assert s._session_min_straddle_value == 70.0, "the old pair's 35 must be discarded, not inherited"
    assert s._session_min_straddle_frozen is None

    # New pair dips to its OWN low of 60 -- never touches the old pair's 35.
    s._position = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 24100, 35.0, 35.0),
        pe_leg=StraddleLeg("PE", 23900, 25.0, 25.0),
        net_credit=70.0, status="open",
    )   # current_value = 60
    asyncio.run(s._check_exits())
    assert s._session_min_straddle_value == 60.0
    assert not close_calls

    # Freeze at this pair's own low (60), then a later tick above it -- no exit yet.
    s._day_low_freeze_time = dtime(0, 0)
    s._defer_exit = lambda reason, now: True
    s._position = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 24100, 38.0, 38.0),
        pe_leg=StraddleLeg("PE", 23900, 28.0, 28.0),
        net_credit=70.0, status="open",
    )   # current_value = 66 -- above the soon-to-be-frozen 60
    asyncio.run(s._check_exits())
    assert s._session_min_straddle_frozen == 60.0
    assert not close_calls

    # Retest of THIS pair's own frozen low (60) -- exits.
    s._position = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 24100, 35.0, 35.0),
        pe_leg=StraddleLeg("PE", 23900, 25.0, 25.0),
        net_credit=70.0, status="open",
    )   # 60
    asyncio.run(s._check_exits())
    assert close_calls == ["day_low_reversal_exit"]


def test_pair_starting_after_freeze_time_arms_and_fires_on_its_own_first_tick_when_unseeded():
    """Edge case when the REST seed is unavailable (mocked to inf here, e.g. a
    genuine fetch failure): a pair that only starts running AFTER the freeze
    time has already elapsed (e.g. a roll at 15:10 with freeze=15:00) has zero
    real window to establish its own low before this check applies -- its very
    first tick freezes AND immediately satisfies the retest (that reading
    trivially IS its own low so far), so it fires on the very same tick it
    starts running. A direct, accepted consequence of scoping "own lowest
    point" to the running pair when no historical seed is available -- flagged
    here, not silently smoothed over. See
    test_pair_starting_after_freeze_time_uses_seeded_low_instead_of_self_firing
    for the normal (seed succeeds) case, which does NOT self-fire this way."""
    s = _strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = dtime(0, 0)   # already "past" freeze time
    s._defer_exit = lambda reason, now: True
    close_calls = _spy_close(s)

    s._position = _position(40.0, 30.0)   # 70 -- this pair's first-ever tick, already past freeze
    asyncio.run(s._check_exits())
    assert close_calls == ["day_low_reversal_exit"]
    assert s._stop_for_day is True


def test_pair_change_seeds_from_rest_history_not_a_blank_slate():
    """Second user correction: "from scratch" means seeded from the pair's
    REAL intraday history (09:15 onward), not a blank slate starting at
    whatever tick the book happens to notice it on. If the REST seed returns a
    genuine historical low BELOW anything the live ticks alone establish, that
    seeded value must be what gets tracked/frozen/retested -- not just the
    live-tick-only running min."""
    s = _strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = dtime(23, 59, 59)   # not reached yet
    close_calls = _spy_close(s)

    # The pair's real market history (fetched via REST) shows it dipped to 28
    # earlier today, well before this book ever opened it.
    s._seed_day_low_for_pair = AsyncMock(return_value=28.0)

    s._position = _position(40.0, 30.0)   # this book's own first tick = 70
    asyncio.run(s._check_exits())
    assert s._session_min_straddle_value == 28.0, "the REST-seeded historical low must win, not the live tick"

    # Live ticks from here only ever print 60+ -- never independently reach 28.
    s._position = _position(35.0, 25.0)   # 60
    asyncio.run(s._check_exits())
    assert s._session_min_straddle_value == 28.0, "a live tick above the seed must not overwrite it"

    # Cross freeze time -- freezes at the seeded 28, not at whatever the live
    # pair's own (never-that-low) ticks would have implied on their own.
    s._day_low_freeze_time = dtime(0, 0)
    s._defer_exit = lambda reason, now: True
    s._position = _position(32.0, 22.0)   # 54 -- above the frozen 28, no exit yet
    asyncio.run(s._check_exits())
    assert s._session_min_straddle_frozen == 28.0
    assert not close_calls

    # Retest of the seeded historical low -- exits.
    s._position = _position(15.0, 13.0)   # 28
    asyncio.run(s._check_exits())
    assert close_calls == ["day_low_reversal_exit"]


def test_pair_starting_after_freeze_time_uses_seeded_low_instead_of_self_firing():
    """Normal case (seed succeeds): unlike the unseeded edge case above, a
    pair that starts running after freeze time does NOT trivially self-fire on
    its own first tick, because it gets seeded with its real historical low
    from earlier in the session -- exactly like any other pair would."""
    s = _strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = dtime(0, 0)   # already "past" freeze time
    s._defer_exit = lambda reason, now: True
    close_calls = _spy_close(s)
    s._seed_day_low_for_pair = AsyncMock(return_value=50.0)   # real 09:15-now low

    s._position = _position(40.0, 30.0)   # 70 -- this book's first tick, but seed(50) is lower
    asyncio.run(s._check_exits())
    assert s._session_min_straddle_frozen == 50.0
    assert not close_calls, "must not self-fire -- the seeded historical low is below this tick's own value"

    s._position = _position(25.0, 25.0)   # 50 -- retests the seeded low
    asyncio.run(s._check_exits())
    assert close_calls == ["day_low_reversal_exit"]
