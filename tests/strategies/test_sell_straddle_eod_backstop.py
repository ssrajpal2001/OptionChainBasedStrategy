"""
tests/strategies/test_sell_straddle_eod_backstop.py -- regression for the
2026-08-23 fix adding SellStraddleStrategy._eod_backstop_loop().

Real gap: self._check_exits() (the FULL exit ladder: EOD -> Day% ->
ITMgate -> DayLow -> LTPdecay -> Ratio -> ScalableTSL -> exit_rules ->
VWAPrise) was ONLY ever invoked from _tick_loop's own "a genuine IndexTick
arrived" branch. On a 1s queue-get timeout it just `continue`s, calling
nothing. Unlike OI-Flow/Liquidity Trap (both run EOD as their own
independent task), SellStraddle had no backstop: if Topic.INDEX_TICK
simply stopped being published for this underlying (a stale/zombie feed --
see this session's own DualFeeder._staleness_watchdog fix), the tick loop
stays alive and healthy with nothing to process, and EOD/Day%/TSL all
silently stop being evaluated for as long as the drought lasts. A position
could ride straight through 15:15 with zero force-exit.
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
    s._compute_day_low_for_pair = AsyncMock(return_value=float("inf"))
    return s


def _position(ce_ltp: float, pe_ltp: float) -> StraddlePosition:
    return StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 24000, ce_ltp, ce_ltp),
        pe_leg=StraddleLeg("PE", 24000, pe_ltp, pe_ltp),
        net_credit=ce_ltp + pe_ltp, status="open",
    )


def test_backstop_calls_check_exits_when_position_is_open():
    s = _strategy()
    s._position = _position(30.0, 20.0)
    s._check_exits = AsyncMock()
    asyncio.run(s._eod_backstop_check_once())
    s._check_exits.assert_called_once()


def test_backstop_is_a_noop_when_flat():
    s = _strategy()
    s._position = None
    s._check_exits = AsyncMock()
    asyncio.run(s._eod_backstop_check_once())
    s._check_exits.assert_not_called()


def test_backstop_is_a_noop_while_a_close_is_already_in_flight():
    """A "closing" position already has a real order dispatched, awaiting
    broker confirmation -- the backstop must not pile on a second
    _check_exits() call mid-close (matches _check_exits' own top-of-function
    guard, but confirmed here at the dispatch level too)."""
    s = _strategy()
    s._position = _position(30.0, 20.0)
    s._position.status = "closing"
    s._check_exits = AsyncMock()
    asyncio.run(s._eod_backstop_check_once())
    s._check_exits.assert_not_called()


def test_backstop_actually_force_closes_a_position_past_eod_with_zero_ticks():
    """The real scenario this fix exists for: EOD time has passed, but no
    IndexTick has arrived (feed stale/blocked) so _tick_loop never called
    _check_exits(). The backstop, calling the REAL _check_exits() (not
    mocked), must still force the EOD close using whatever price is
    already known (self._spot / leg LTPs), exactly as _tick_loop's own
    path would have."""
    s = _strategy()
    s._force_exit = dtime(0, 0)   # already past EOD relative to any real wall-clock time
    s._position = _position(30.0, 20.0)
    closed = []

    async def _fake_eod_close_or_hedge(pos, now):
        closed.append("eod_squareoff")
        s._position.status = "closed"
        s._position = None
    s._eod_close_or_hedge = _fake_eod_close_or_hedge

    asyncio.run(s._eod_backstop_check_once())

    assert closed == ["eod_squareoff"]
    assert s._position is None


def test_backstop_exception_is_recovered_loop_stays_usable():
    s = _strategy()
    s._position = _position(30.0, 20.0)

    async def _boom():
        raise RuntimeError("simulated failure inside the exit ladder")
    s._check_exits = _boom

    asyncio.run(s._eod_backstop_check_once())   # must not raise
