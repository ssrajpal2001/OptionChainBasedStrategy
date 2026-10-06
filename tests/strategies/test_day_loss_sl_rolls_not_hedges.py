"""Regression test for the 2026-10-06 direct user correction (REVERTS the
2026-09-29 change, commit 801929b, back to the 2026-09-11 spec): a
day-loss-SL breach must activate the 4-leg hedge (via
_hedge_or_roll_if_eligible, same mechanism EOD hedge-and-carry uses), never
a single-side roll. Real 2026-10-05 incident: day_loss_sl_roll fired 3x in
under 25 minutes, each roll landing worse than the last, because a roll has
no built-in loss cap. Option A per direct user spec: ALWAYS hedges, no roll
fallback even when hedging can't be built."""
import asyncio
import datetime
from unittest.mock import AsyncMock

from data_layer.base_feeder import EventBus
from config.global_config import IST, GlobalConfig
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg


def _strategy(bus):
    s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
    s._lot_size = 75
    s._lot_multiplier = 1
    s._spot = 24000.0
    s._force_exit = datetime.time(23, 59)
    s._ltp_decay_enabled = False
    s._tsl_enabled = False
    s._vwap_rise_enabled = False
    s._exit_rules = []
    s._day_profit_target_pct = 0.0
    s._day_loss_sl_pct = 10.0   # 10% loss triggers the guardrail
    s._initial_net_credit = 100.0  # gates the whole day-level guardrail block
    s._ratio_threshold = 999.0
    s._itm_pair_gate_enabled = False
    s._itm_roll_protection = {}
    s._day_low_exit_enabled = False
    s._post1500_exit_enabled = False
    s._persist = lambda: None
    s._ind_by_tf = lambda *a, **k: {}
    s._hedge_carry_enabled = True
    s._position = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        # net_credit=100, current_value=150 -> running loss = -50pts = -50% of credit,
        # well past the 10% day-loss threshold.
        ce_leg=StraddleLeg("CE", 23900, 60.0, 90.0),
        pe_leg=StraddleLeg("PE", 24100, 40.0, 60.0),
        net_credit=100.0, status="open",
    )
    s._close_position = AsyncMock()
    s._close_position_and_hedge = AsyncMock()
    s._single_side_roll = AsyncMock(return_value=True)  # must never be called
    return s


def test_day_loss_sl_activates_hedge_not_roll():
    async def run():
        bus = EventBus()
        s = _strategy(bus)
        s._hedge_or_roll_if_eligible = AsyncMock(return_value=True)
        await s._check_exits()
        s._hedge_or_roll_if_eligible.assert_awaited_once()
        _, kwargs = s._hedge_or_roll_if_eligible.await_args
        assert kwargs.get("stop_for_day_on_hedge") is False, (
            "a mid-day day-loss-SL hedge must NOT stop the book for the day"
        )
        s._single_side_roll.assert_not_awaited()
        s._close_position.assert_not_awaited()
        s._close_position_and_hedge.assert_not_awaited()
        assert s._position is not None and s._position.status == "open"
        assert s._stop_for_day is False

    asyncio.run(run())


def test_day_loss_sl_holds_position_when_no_hedge_can_be_built():
    """When _hedge_or_roll_if_eligible can't build a hedge (no valid strike,
    or hedging disabled for this binding), the position is left running
    unchanged -- no roll fallback, not closed, not stopped for the day."""
    async def run():
        bus = EventBus()
        s = _strategy(bus)
        s._hedge_or_roll_if_eligible = AsyncMock(return_value=False)
        await s._check_exits()
        s._hedge_or_roll_if_eligible.assert_awaited()
        s._single_side_roll.assert_not_awaited()
        s._close_position.assert_not_awaited()
        s._close_position_and_hedge.assert_not_awaited()
        assert s._position is not None and s._position.status == "open"
        assert s._stop_for_day is False

    asyncio.run(run())
