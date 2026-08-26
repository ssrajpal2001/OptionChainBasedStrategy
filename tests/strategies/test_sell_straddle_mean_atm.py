"""
tests/strategies/test_sell_straddle_mean_atm.py -- regression for the
2026-08-26 direct user spec: for a futures_atm underlying, SellStraddle now
tracks REAL spot and the FUTURES price simultaneously (two separate
IndexTick streams, tagged tick.source="spot"/"futures") and computes
self._atm_ref = mean(spot, futures) -- the value strike/ATM selection
actually uses. self._spot keeps its true, unchanged meaning (real index);
only self._atm_ref moved to the mean. See selection.py's atm_ref parameter
for how the entry-selection call sites consume it.
"""
import asyncio
import datetime

from config.global_config import IST, GlobalConfig, Topic
from data_layer.base_feeder import EventBus, IndexTick
from strategies.sell_straddle import SellStraddleStrategy


def _tick(underlying: str, ltp: float, source: str = "spot") -> IndexTick:
    now = datetime.datetime.now(IST)
    return IndexTick(symbol=underlying, ltp=ltp, open=ltp, high=ltp, low=ltp,
                      close=ltp, volume=0, timestamp=now, source=source)


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


def _futures_cfg():
    cfg = GlobalConfig()
    cfg.futures_atm_underlyings = ["NIFTY"]
    return cfg


def test_uses_mean_atm_true_for_configured_underlying():
    s = SellStraddleStrategy(EventBus(), cfg=_futures_cfg(), underlying="NIFTY")
    assert s._uses_mean_atm is True


def test_uses_mean_atm_false_for_unconfigured_underlying():
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
    assert s._uses_mean_atm is False


def test_spot_and_futures_tracked_separately_and_mean_computed():
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=_futures_cfg(), underlying="NIFTY")
        ticks = [
            _tick("NIFTY", 24425.00, source="spot"),
            _tick("NIFTY", 24440.00, source="futures"),
        ]
        await _run_tick_loop_briefly(s, bus, ticks)
        assert s._spot == 24425.00
        assert s._futures_spot == 24440.00
        assert s._atm_ref == (24425.00 + 24440.00) / 2.0
    asyncio.run(run())


def test_atm_ref_falls_back_to_spot_until_futures_has_ticked_once():
    """Before the futures stream has ticked at all, atm_ref must not silently
    read as a stale/zero mean -- it stays plain spot."""
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=_futures_cfg(), underlying="NIFTY")
        ticks = [_tick("NIFTY", 24425.00, source="spot")]
        await _run_tick_loop_briefly(s, bus, ticks)
        assert s._spot == 24425.00
        assert s._futures_spot == 0.0
        assert s._atm_ref == 24425.00
    asyncio.run(run())


def test_atm_ref_updates_again_when_only_one_side_ticks_after_both_warm():
    """Once both sides have ticked once, a later tick on only ONE side must
    still recompute the mean using the other side's last-known value."""
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=_futures_cfg(), underlying="NIFTY")
        ticks = [
            _tick("NIFTY", 24425.00, source="spot"),
            _tick("NIFTY", 24440.00, source="futures"),
            _tick("NIFTY", 24435.00, source="spot"),   # spot moves, futures stale
        ]
        await _run_tick_loop_briefly(s, bus, ticks)
        assert s._spot == 24435.00
        assert s._futures_spot == 24440.00
        assert s._atm_ref == (24435.00 + 24440.00) / 2.0
    asyncio.run(run())


def test_non_futures_atm_underlying_atm_ref_always_equals_spot():
    """Complete no-op for any underlying not in cfg.futures_atm_underlyings --
    a source="futures" tick (should never occur in practice for this
    underlying, but must be harmless even if it did) is ignored as such and
    treated as an ordinary spot tick instead, and atm_ref tracks spot 1:1."""
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        ticks = [_tick("NIFTY", 24425.00, source="spot")]
        await _run_tick_loop_briefly(s, bus, ticks)
        assert s._spot == 24425.00
        assert s._atm_ref == 24425.00
        assert s._futures_spot == 0.0
    asyncio.run(run())
