"""
tests/ui_layer/test_ws_bridge_futures_tick_filter.py -- regression for the
2026-08-26 real incident: WsBridge's generic "Market Overview" SPOT PRICE/
ATM STRIKE broadcast (and its index RSI/EMA/ADX aggregation) mixed real spot
and futures ticks for a futures_atm underlying (e.g. NIFTY), reported live as
SPOT PRICE showing ~24467 (the futures value) while real spot was ~24289.
WsBridge must only ever act on source="spot" IndexTicks.
"""
import asyncio
import datetime as dt

from config.global_config import IST, Topic
from data_layer.base_feeder import EventBus, IndexTick
from ui_layer.ws_bridge import WsBridge


def _tick(symbol: str, ltp: float, source: str = "spot") -> IndexTick:
    now = dt.datetime.now(IST)
    return IndexTick(symbol=symbol, ltp=ltp, open=ltp, high=ltp, low=ltp,
                      close=ltp, volume=0, timestamp=now, source=source)


async def _run_tick_loop_briefly(bridge: WsBridge, bus: EventBus, ticks) -> None:
    bridge._running = True
    task = asyncio.create_task(bridge._tick_loop())
    await asyncio.sleep(0.01)
    for t in ticks:
        await bus.publish(Topic.INDEX_TICK, t)
        await asyncio.sleep(0.01)
    bridge._running = False
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def test_futures_tick_never_updates_spot_cache():
    async def run():
        bus = EventBus()
        bridge = WsBridge(bus)
        ticks = [
            _tick("NIFTY", 24289.65, source="spot"),
            _tick("NIFTY", 24450.00, source="futures"),
        ]
        await _run_tick_loop_briefly(bridge, bus, ticks)
        assert bridge._spot_cache["NIFTY"] == 24289.65
    asyncio.run(run())


def test_spot_tick_still_updates_cache():
    async def run():
        bus = EventBus()
        bridge = WsBridge(bus)
        ticks = [_tick("NIFTY", 24291.65, source="spot")]
        await _run_tick_loop_briefly(bridge, bus, ticks)
        assert bridge._spot_cache["NIFTY"] == 24291.65
    asyncio.run(run())


def test_default_source_is_spot_backward_compatible():
    async def run():
        bus = EventBus()
        bridge = WsBridge(bus)
        now = dt.datetime.now(IST)
        tick = IndexTick(symbol="NIFTY", ltp=24291.65, open=24291.65, high=24291.65,
                          low=24291.65, close=24291.65, volume=0, timestamp=now)
        assert tick.source == "spot"
        await _run_tick_loop_briefly(bridge, bus, [tick])
        assert bridge._spot_cache["NIFTY"] == 24291.65
    asyncio.run(run())
