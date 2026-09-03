"""2026-08-06 HIGH-priority fix: self._spot fed Day%/theta and the ITM-pair-
gate's both-ITM check with zero validation. A single garbage/glitched index
tick could misclassify both legs as ITM or distort the theta split enough
to fire a real close/roll off one bad tick. Reject a single-tick >20% jump
vs the last accepted spot, with a 5-in-a-row safety valve so a genuine
large gap doesn't get stuck forever.
"""
import asyncio
import datetime

from config.global_config import IST, GlobalConfig
from data_layer.base_feeder import EventBus, IndexTick
from config.global_config import Topic
from strategies.sell_straddle import SellStraddleStrategy


def _tick(underlying: str, ltp: float) -> IndexTick:
    now = datetime.datetime.now(IST)
    return IndexTick(symbol=underlying, ltp=ltp, open=ltp, high=ltp, low=ltp,
                     close=ltp, volume=0, timestamp=now)


async def _run_tick_loop_briefly(s: SellStraddleStrategy, bus: EventBus, ticks) -> None:
    s._running = True
    task = asyncio.create_task(s._tick_loop())
    await asyncio.sleep(0.01)
    for t in ticks:
        await bus.publish(Topic.INDEX_TICK, t)
        await asyncio.sleep(0.01)
    s._running = False
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def test_single_glitched_tick_is_ignored():
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        ticks = [
            _tick("NIFTY", 24600.0),   # first real tick, establishes baseline
            _tick("NIFTY", 49200.0),   # glitch: exactly 2x (e.g. wrong instrument/decimal)
            _tick("NIFTY", 24610.0),   # next real tick, small genuine move
        ]
        await _run_tick_loop_briefly(s, bus, ticks)
        # The glitch must never have been accepted as self._spot.
        assert s._spot == 24610.0
    asyncio.run(run())


def test_five_consecutive_large_jumps_eventually_accepted():
    """Safety valve: a genuine sustained large move (or a persistently
    misbehaving feed) must not leave self._spot stuck forever."""
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        ticks = [_tick("NIFTY", 24600.0)] + [_tick("NIFTY", 49200.0)] * 6
        await _run_tick_loop_briefly(s, bus, ticks)
        # After 5 rejections, the 6th identical "glitch" tick must be accepted.
        assert s._spot == 49200.0
    asyncio.run(run())


def test_normal_gradual_moves_all_accepted():
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        ticks = [_tick("NIFTY", p) for p in
                 [24600.0, 24610.0, 24625.0, 24615.0, 24640.0]]
        await _run_tick_loop_briefly(s, bus, ticks)
        assert s._spot == 24640.0
        assert s._spot_reject_streak == 0
    asyncio.run(run())
