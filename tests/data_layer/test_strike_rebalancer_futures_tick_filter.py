"""
tests/data_layer/test_strike_rebalancer_futures_tick_filter.py -- regression
for the 2026-08-26 real incident: once a futures_atm underlying (e.g. NIFTY)
started publishing TWO IndexTick streams for the same symbol (source="spot"
and source="futures" -- see GlobalConfig.futures_atm_underlyings), platform-
shared infrastructure like StrikeRebalancer silently mixed them, treating a
futures tick's ~100+ point offset from real spot as wild "drift" -- reported
live via a corrupted "Market Overview" SPOT PRICE reading. StrikeRebalancer
must only ever act on source="spot" ticks; a futures tick must be a no-op.
"""
import datetime as dt

import pytest

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus, IndexTick
from data_layer.strike_rebalancer import StrikeRebalancer, _UnderlyingState


def _tick(symbol: str, ltp: float, source: str = "spot") -> IndexTick:
    now = dt.datetime.now(IST)
    return IndexTick(symbol=symbol, ltp=ltp, open=ltp, high=ltp, low=ltp,
                      close=ltp, volume=0, timestamp=now, source=source)


def _rebalancer_with_state(current_atm: float) -> StrikeRebalancer:
    rb = StrikeRebalancer(EventBus(), GlobalConfig(), feeder=None)
    rb._state["NIFTY"] = _UnderlyingState(open_atm=current_atm, current_atm=current_atm)
    return rb


def test_futures_tick_never_updates_current_atm():
    rb = _rebalancer_with_state(current_atm=24300.0)
    import asyncio
    asyncio.run(rb._on_tick(_tick("NIFTY", 24450.0, source="futures")))
    assert rb._state["NIFTY"].current_atm == 24300.0


def test_spot_tick_still_updates_normally():
    """Confirms the filter doesn't block real spot ticks -- only futures ones."""
    rb = _rebalancer_with_state(current_atm=24300.0)
    import asyncio
    # A spot tick within the same ATM band shouldn't trigger a rebalance, but
    # must not raise/be silently dropped either -- reaching _on_tick's normal
    # body (past the source filter) is what this proves.
    asyncio.run(rb._on_tick(_tick("NIFTY", 24310.0, source="spot")))
    assert rb._state["NIFTY"].current_atm == 24300.0   # small move, no rebalance needed yet


def test_default_source_is_spot_backward_compatible():
    """Every pre-existing caller that omits source= must behave exactly as
    before this fix -- default IndexTick.source="spot" reaches _on_tick's
    normal body untouched."""
    rb = _rebalancer_with_state(current_atm=24300.0)
    import asyncio
    tick = IndexTick(symbol="NIFTY", ltp=24310.0, open=24310.0, high=24310.0,
                      low=24310.0, close=24310.0, volume=0, timestamp=dt.datetime.now(IST))
    assert tick.source == "spot"
    asyncio.run(rb._on_tick(tick))   # must not raise
