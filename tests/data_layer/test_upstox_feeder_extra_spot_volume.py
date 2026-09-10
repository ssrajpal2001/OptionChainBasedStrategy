"""
tests/data_layer/test_upstox_feeder_extra_spot_volume.py -- 2026-09-10, real
incident: UpstoxFeeder._parse_frame() hardcoded volume=0 for every tick on
the index/spot/futures branch (used for _extra_spot_keys-registered stocks,
i.e. every OI-ORB dedicated-feeder stock). OI-ORB's tick-by-tick VWAP
accumulation only updates on a cumulative-volume delta > 0 -- with volume
always 0, that delta was always 0, so VWAP silently froze at its initial
historical-seed value for the entire session, no matter how many real
ticks arrived. Confirmed live: OIL's VWAP stuck at 508.73 for 20+ minutes
while spot genuinely fell from ~500 to ~499, so its VWAP-close SL could
never see the real, current gap.
"""
import asyncio

import pytest

from config.global_config import Topic
from data_layer.base_feeder import EventBus
from data_layer.global_feeder import UpstoxFeeder


@pytest.fixture
def bus():
    return EventBus()


@pytest.fixture
def feeder(bus):
    return UpstoxFeeder(bus)


def _upstox_feed(ltp: float, vtt: int) -> dict:
    """Minimal realistic MarketDataStreamerV3 full-mode feed entry."""
    return {
        "fullFeed": {
            "marketFF": {
                "ltpc": {"ltp": ltp},
                "vtt": vtt,
            }
        }
    }


@pytest.mark.asyncio
async def test_extra_spot_key_tick_carries_real_volume_not_zero(bus, feeder):
    """The exact real incident: a stock registered via _extra_spot_keys
    (register_extra_spot_keys, OI-ORB's own dedicated-feeder subscription)
    must publish IndexTick.volume from the real 'vtt' field, not hardcoded 0."""
    feeder._extra_spot_keys["NSE_EQ|INE274J01014"] = "OIL"
    q = bus.subscribe(Topic.INDEX_TICK)

    await feeder._parse_frame({
        "feeds": {
            "NSE_EQ|INE274J01014": _upstox_feed(ltp=499.25, vtt=1_234_567),
        }
    })

    tick = q.get_nowait()
    assert tick.symbol == "OIL"
    assert tick.ltp == 499.25
    assert tick.volume == 1_234_567


@pytest.mark.asyncio
async def test_extra_spot_key_tick_volume_increases_across_ticks(bus, feeder):
    """Two consecutive real ticks with a genuinely growing cumulative vtt --
    proves the volume DELTA (what OI-ORB's VWAP accumulation actually needs)
    is real and positive, not stuck at a flat 0."""
    feeder._extra_spot_keys["NSE_EQ|INE274J01014"] = "OIL"
    q = bus.subscribe(Topic.INDEX_TICK)

    await feeder._parse_frame({"feeds": {"NSE_EQ|INE274J01014": _upstox_feed(499.25, 1_000_000)}})
    await feeder._parse_frame({"feeds": {"NSE_EQ|INE274J01014": _upstox_feed(499.10, 1_002_500)}})

    first = q.get_nowait()
    second = q.get_nowait()
    assert first.volume == 1_000_000
    assert second.volume == 1_002_500
    assert second.volume > first.volume
