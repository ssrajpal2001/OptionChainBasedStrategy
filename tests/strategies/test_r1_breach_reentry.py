"""
Regression tests for the 2026-09-23 critical live-incident fix to
strategies/sell_straddle/r1_breach_reentry.py.

Real incident: a rolled-in leg's R1-breach close never set
pos.{ce,pe}_leg_closed, so the very next call to
_check_r1_breach_and_reentry() saw the leg as still "open", re-armed a fresh
watch against the same (never-updated) leg data, found the same breach true
again, and closed it again -- forever, each time booking another real leg_pnl
into session_realized_pnl_pts and (live) sending another real broker exit
order. Separately, _tick_loop and _eod_backstop_loop can both reach
_check_exits() -> _check_r1_breach_and_reentry() concurrently (same
documented race already fixed once for _post1500_closing), which compounded
the runaway into bursts of duplicate closes within the same second.

These tests drive the real R1BreachReentryMixin methods (via SellStraddleStrategy)
with a fake SupportResistanceCalculator so R1-breach state is controlled
directly, rather than replaying real candles through the actual calculator
(that state machine is already covered by test_post1500_r1_exit.py's
_establish_r1 helper / support_resistance.py's own tests). Follows this
file's own established async-test convention (asyncio.run(...) inside a
plain `def test_...`), matching test_post1500_r1_exit.py.
"""
import asyncio
from datetime import datetime, time as dtime
from unittest.mock import AsyncMock

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg


class _FakeOrderEvent:
    def __init__(self, close_aborted=False):
        self.close_aborted = close_aborted


class _FakeCalc:
    """Stands in for SupportResistanceCalculator -- controls breach state directly."""

    def __init__(self, r1_established: bool = False, phase: str = "R1_TRACKING"):
        self.r1_established = r1_established
        self.phase = phase

    def process_straddle_candle(self, *a, **k):
        pass

    def get_calculated_sr_state(self, key):
        return {
            "sr_levels": {
                "R1": {"is_established": self.r1_established, "high": 100.0},
                "S1": {"is_established": False, "low": 50.0},
            },
            "current_phase": self.phase,
            "r1_established": self.r1_established,
            "s1_established": False,
        }


def _strategy():
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
    s._lot_size = 75
    s._lot_multiplier = 1
    s._spot = 24000.0
    s._force_exit = dtime(23, 59)
    return s


def _position(ce_entry=100.0, ce_ltp=100.0, pe_entry=100.0, pe_ltp=120.0) -> StraddlePosition:
    return StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 24000, ce_entry, ce_ltp, open_reason="single_side_roll_vwap_rise_roll"),
        pe_leg=StraddleLeg("PE", 24000, pe_entry, pe_ltp, open_reason="single_side_roll_vwap_rise_roll"),
        net_credit=ce_entry + pe_entry,
        status="open",
    )


def _spy_close_leg(s, close_aborted=False, yield_before_finalize=False):
    """Spy mirroring the real _close_leg contract: booked P&L / close_time
    only finalize on a non-aborted close. `yield_before_finalize` inserts a
    real await point so two concurrent callers can genuinely interleave
    (proving the _r1_closing guard, not just accidental ordering)."""
    calls = []

    async def _fake(side, reason, now):
        calls.append((side, reason))
        if yield_before_finalize:
            await asyncio.sleep(0.01)
        leg = s._position.ce_leg if side == "CE" else s._position.pe_leg
        if not close_aborted:
            leg.close_time = now
        return _FakeOrderEvent(close_aborted=close_aborted)

    s._close_leg = _fake
    return calls


def test_confirmed_r1_breach_close_sets_leg_closed_flag():
    """Core fix: after a confirmed close, pos.pe_leg_closed must be True."""
    s = _strategy()
    s._position = _position()
    s._seed_r1s1_calc = AsyncMock(return_value=_FakeCalc(r1_established=False))
    calls = _spy_close_leg(s)
    s._persist = lambda: None

    # PE leg in loss (entry=100, ltp=120) -> arms watch -> breach immediately
    # true (r1_established=False) -> closes in the SAME call.
    now = datetime(2026, 9, 23, 12, 27, 35)
    asyncio.run(s._check_r1_breach_and_reentry(now))

    assert len(calls) == 1
    assert s._position.pe_leg_closed is True


def test_second_call_does_not_re_close_already_closed_leg():
    """THE bug: before the fix, a second call (simulating the very next real
    tick) would see pe_leg_closed still False, re-arm, and close again."""
    s = _strategy()
    s._position = _position()
    s._seed_r1s1_calc = AsyncMock(return_value=_FakeCalc(r1_established=False))
    calls = _spy_close_leg(s)
    s._persist = lambda: None

    now = datetime(2026, 9, 23, 12, 27, 35)
    asyncio.run(s._check_r1_breach_and_reentry(now))
    assert len(calls) == 1

    # A later tick with the SAME stale leg data (nothing replaced it yet --
    # Part 2 hasn't found an S1-breach re-entry candidate).
    asyncio.run(s._check_r1_breach_and_reentry(now))
    assert len(calls) == 1, "leg must not be re-armed/re-closed once already closed"


def test_concurrent_calls_only_close_once():
    """_tick_loop and _eod_backstop_loop can both reach this method for the
    same real moment -- the _r1_closing guard must serialize them."""
    s = _strategy()
    s._position = _position()
    s._seed_r1s1_calc = AsyncMock(return_value=_FakeCalc(r1_established=False))
    calls = _spy_close_leg(s, yield_before_finalize=True)
    s._persist = lambda: None

    now = datetime(2026, 9, 23, 12, 27, 35)

    async def _race():
        await asyncio.gather(
            s._check_r1_breach_and_reentry(now),
            s._check_r1_breach_and_reentry(now),
        )
    asyncio.run(_race())

    assert len(calls) == 1, "concurrent breach evaluations must not both close the same leg"
    assert s._position.pe_leg_closed is True


def test_aborted_close_clears_guard_and_allows_retry():
    s = _strategy()
    s._position = _position()
    s._seed_r1s1_calc = AsyncMock(return_value=_FakeCalc(r1_established=False))
    calls = _spy_close_leg(s, close_aborted=True)
    s._persist = lambda: None

    now = datetime(2026, 9, 23, 12, 27, 35)
    asyncio.run(s._check_r1_breach_and_reentry(now))
    assert len(calls) == 1
    assert s._position.pe_leg_closed is False  # never confirmed -- leg stays open
    assert s._r1_closing["PE"] is False  # guard cleared so a later tick can retry

    asyncio.run(s._check_r1_breach_and_reentry(now))
    assert len(calls) == 2


def test_open_leg_clears_leg_closed_and_r1_closing_guard():
    """A fresh _open_leg() (e.g. the S1-breach re-entry itself) must clear
    both flags so this side's P&L resumes counting and a FUTURE roll-in on
    this side isn't permanently blocked by a stale guard."""
    s = _strategy()
    s._position = _position()
    s._position.pe_leg_closed = True
    s._r1_watch = {}
    s._r1_pending = None
    s._r1_closing = {"CE": False, "PE": True}
    s._seed_exec_legs = AsyncMock()
    s._emit_order = AsyncMock()

    asyncio.run(s._open_leg(
        "PE", 23450, 106.0, datetime(2026, 9, 23, 12, 30, 0), "s1_breach_reentry_post_roll",
    ))

    assert s._position.pe_leg_closed is False
    assert s._r1_closing["PE"] is False
