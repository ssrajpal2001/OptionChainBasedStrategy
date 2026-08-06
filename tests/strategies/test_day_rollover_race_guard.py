"""2026-08-06 HIGH-priority fix: _try_entry (driven by INDEX_TICK, its own
async loop) and reset_session() (driven by CANDLE_CLOSE, a separate async
loop) are unsynchronized. Index ticks can start flowing and _try_entry can
run before the first candle of a new day has closed and triggered
reset_session() -- in that window, self._primed/_trades_today/
_entry_expiry_date/_strike_prem are all still yesterday's stale values.
_try_entry must defer entirely until reset_session() has actually run for
today's session.
"""
import asyncio
import datetime
from unittest.mock import AsyncMock

from config.global_config import IST, GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy


def _strategy_ready_to_evaluate(bus, market_open_day: datetime.date):
    s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
    s._spot = 24600.0
    s._ce_ltp = 100.0
    s._pe_ltp = 100.0
    s._market_open_dt = datetime.datetime.combine(
        market_open_day, datetime.time(9, 15), tzinfo=IST)
    s._any_active_terminal = lambda: True
    s._eval_ruleset = AsyncMock()
    return s


def test_defers_when_market_open_dt_is_from_a_prior_day():
    """The exact race: market_open_dt still shows yesterday, but `now` is
    today -- reset_session() for today hasn't run yet. Must defer, never
    evaluate on stale state."""
    async def run():
        bus = EventBus()
        yesterday = datetime.date(2026, 8, 5)
        s = _strategy_ready_to_evaluate(bus, market_open_day=yesterday)
        today_now = datetime.datetime.combine(
            datetime.date(2026, 8, 6), datetime.time(9, 17), tzinfo=IST)

        await s._try_entry(today_now)

        s._eval_ruleset.assert_not_awaited()
    asyncio.run(run())


def test_evaluates_normally_once_market_open_dt_matches_today():
    """Sanity check: once reset_session() has actually run (market_open_dt
    is today), evaluation proceeds normally."""
    async def run():
        bus = EventBus()
        today = datetime.date(2026, 8, 6)
        s = _strategy_ready_to_evaluate(bus, market_open_day=today)
        now = datetime.datetime.combine(today, datetime.time(9, 17), tzinfo=IST)

        await s._try_entry(now)

        s._eval_ruleset.assert_awaited()
    asyncio.run(run())


def test_evaluates_normally_when_market_open_dt_is_none():
    """Sanity check: market_open_dt is None only very briefly at process
    start, before the first candle has EVER closed -- must not defer
    forever, only during a genuine same-day-vs-stale-day mismatch."""
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._spot = 24600.0
        s._ce_ltp = 100.0
        s._pe_ltp = 100.0
        s._market_open_dt = None
        s._any_active_terminal = lambda: True
        s._eval_ruleset = AsyncMock()
        now = datetime.datetime.combine(
            datetime.date(2026, 8, 6), datetime.time(9, 17), tzinfo=IST)

        await s._try_entry(now)

        s._eval_ruleset.assert_awaited()
    asyncio.run(run())
