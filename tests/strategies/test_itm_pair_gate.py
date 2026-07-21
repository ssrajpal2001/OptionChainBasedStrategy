"""strategies/sell_straddle/exits.py's step-8 ITM pair gate call -- 2026-07-21
critical bugfix: the per-tick exit-check loop only called _check_itm_pair_gate
(rolling.py, the ONLY code path that can actually close the position on this
gate) when `self._itm_gate_armed` was already True. But `_itm_gate_armed` is
ONLY ever set True *inside* _check_itm_pair_gate itself. Since the other call
site (rolling.py, after a rollover) never fires on a session with zero
rollovers, the gate could never arm itself and the real close never ran --
even though exits.py's separate, display-only _build_exit_criteria kept
logging "ITMgate ... ✓HIT -> EXIT:ITMgate" every cycle, misleadingly implying
an exit had happened. Confirmed live: NIFTY straddle held both-ITM at
cumulative ₹530+ (well above the ₹500 threshold) for many minutes without
ever closing, on a session with no prior rollover."""
import asyncio
import datetime
from unittest.mock import AsyncMock

from data_layer.base_feeder import EventBus
from config.global_config import IST, GlobalConfig
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg


def _gated_strategy(bus):
    s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
    s._lot_size = 75
    s._lot_multiplier = 1
    s._spot = 24000.0
    s._force_exit = datetime.time(23, 59)   # never past EOD square-off in this test
    s._ltp_decay_enabled = False
    s._tsl_enabled = False
    s._vwap_rise_enabled = False
    s._exit_rules = []
    s._day_profit_target_pct = 0.0
    s._day_loss_sl_pct = 0.0
    s._ratio_threshold = 999.0
    s._itm_pair_gate_enabled = True
    s._itm_pair_gate_profit_inr = 500.0
    assert s._itm_gate_armed is False   # never armed -- no rollover has happened
    s._position = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 23900, 60.0, 30.0),   # ITM: strike < spot
        pe_leg=StraddleLeg("PE", 24200, 40.0, 20.0),   # ITM: strike > spot
        net_credit=100.0, status="open",
    )
    # unrealized_pnl = net_credit(100) - current_value(30+20=50) = 50 pts
    # pnl_rs = 50 * 75 * 1 = 3750, well above the 500 threshold.
    return s


def test_itm_pair_gate_closes_on_first_cycle_with_no_prior_rollover():
    """The exact live bug: gate must be able to arm AND close itself the very
    first time it's ever evaluated, without requiring a rollover to have
    happened first."""
    async def run():
        bus = EventBus()
        s = _gated_strategy(bus)
        s._close_position = AsyncMock(wraps=s._close_position)
        await s._check_exits()
        s._close_position.assert_awaited_once_with("itm_pair_gate_profit")
        assert s._position is None   # _close_position clears it synchronously
    asyncio.run(run())


def test_itm_pair_gate_arms_then_holds_below_threshold():
    """Below-threshold both-ITM must arm the gate (so the NEXT cycle can act)
    but must not close yet."""
    async def run():
        bus = EventBus()
        s = _gated_strategy(bus)
        s._itm_pair_gate_profit_inr = 10_000.0   # far above the 3750 available
        s._close_position = AsyncMock(wraps=s._close_position)
        await s._check_exits()
        s._close_position.assert_not_awaited()
        assert s._itm_gate_armed is True
        assert s._position is not None and s._position.status == "open"
    asyncio.run(run())
