"""Pool-engine positions fill/exit on the TRACKING contract directly --
no execution-strike resolution, no execution-native-risk lookback. Strike
fields on the opened position match the book's own resolved tracking
strike for that side."""
import asyncio
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Optional
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook

IST = ZoneInfo("Asia/Kolkata")


@dataclass
class _FakeEvent:
    side: str
    price_hint: float
    sl_price: float = 5.0
    target_price: float = 20.0
    audit: Optional[dict] = None


class _FakeFeeder:
    def __init__(self) -> None:
        self.subscribed: List[str] = []

    async def subscribe_tokens(self, tokens) -> None:
        self.subscribed.extend(tokens)


class _FakeRebalancer:
    def __init__(self, feeder) -> None:
        self._feeder = feeder


@pytest.mark.asyncio
async def test_pool_engine_open_uses_tracking_strike_not_execution_strike():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._running = True
    book._ce_strike, book._pe_strike = 23700, 24100
    book._live_price["CE"] = 150.5

    # Directly exercise the pool engine's own open (mirrors what on_5m_bar would do),
    # then let _emit_order route it.
    book._pool_engine.position = None  # ensure clean state
    from strategies.v4_cascade.pool_engine import _ZoneSlot
    from strategies.v4_cascade.dataclasses import RollingBaseZone
    zone = RollingBaseZone(entry_line=145.0, sweep_low=135.0, sl_level=170.0,
                            reference_low_ts=datetime.now(IST), lock_ts=datetime.now(IST), locked=True)
    # Single-candidate scenario: the slot's own strike (set by the pool
    # engine from its candidate list) matches book._ce_strike -- as it does
    # in real single-candidate operation.
    slot = _ZoneSlot(zone, strike=23700)
    slot.ltf_zone = RollingBaseZone(entry_line=148.0, sweep_low=142.0, sl_level=155.0,
                                     reference_low_ts=datetime.now(IST), lock_ts=datetime.now(IST), locked=True)
    real_ev = book._pool_engine._open_position("CE", slot, 150.0, datetime.now(IST))

    book._emit_order(real_ev, pos_before=None)

    assert book._pool_engine.position.t1.strike == 23700
    assert book._pool_engine.position.execution_strike == 23700


@pytest.mark.asyncio
async def test_pool_engine_open_uses_event_execution_strike_under_multi_strike():
    """2026-07-24 regression: under multi-strike candidate scanning, a trade
    can fire on any of up to 5 CE/PE candidates -- PoolCascadeEngine.
    _open_position already stamps CascadeEvent.execution_strike with the
    REAL strike that triggered (slot.strike), which may not be the first
    candidate. _emit_order's pool-engine branch must use ev.execution_strike,
    NOT the scalar self._ce_strike/self._pe_strike (which is only ever
    _ce_strikes[0]/_pe_strikes[0] -- the FIRST candidate) or it silently
    trades the WRONG option contract whenever the trigger wasn't candidate #1."""
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._running = True
    # Multi-strike candidates: scalar self._ce_strike is only the FIRST
    # candidate (24000); the trade in this test fires on the THIRD (23800).
    book._ce_strikes = [24000, 23900, 23800]
    book._ce_strike, book._pe_strike = 24000, 24100
    book._live_price["CE"] = 150.5

    book._pool_engine.position = None  # ensure clean state
    from strategies.v4_cascade.pool_engine import _ZoneSlot
    from strategies.v4_cascade.dataclasses import RollingBaseZone
    zone = RollingBaseZone(entry_line=145.0, sweep_low=135.0, sl_level=170.0,
                            reference_low_ts=datetime.now(IST), lock_ts=datetime.now(IST), locked=True)
    slot = _ZoneSlot(zone, strike=23800)  # triggering candidate != scalar self._ce_strike
    slot.ltf_zone = RollingBaseZone(entry_line=148.0, sweep_low=142.0, sl_level=155.0,
                                     reference_low_ts=datetime.now(IST), lock_ts=datetime.now(IST), locked=True)
    real_ev = book._pool_engine._open_position("CE", slot, 150.0, datetime.now(IST))
    assert real_ev.execution_strike == 23800  # sanity: engine set the real triggering strike

    book._emit_order(real_ev, pos_before=None)

    # Must reflect the REAL triggering strike (23800), never the stale
    # first-candidate scalar (24000).
    assert book._pool_engine.position.execution_strike == 23800
    assert book._pool_engine.position.t1.strike == 23800
    assert book._pool_engine.position.t2.strike == 23800


@pytest.mark.asyncio
async def test_pool_engine_open_entry_async_skips_execution_subscribe_and_wait(monkeypatch):
    """2026-07-23 reviewer fix: pool-engine entries must never resolve/
    subscribe to the separate 'execution contract' or wait up to 3s for its
    tick -- they trade the tracking contract directly and already have a
    live price for it via self._live_price[side]. Confirms feeder.subscribe_
    tokens is never called and the coroutine returns immediately (no wait
    loop), sourcing price_hint straight from self._live_price[side]."""
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._running = True
    book._expiry = date(2026, 7, 21)
    feeder = _FakeFeeder()
    book._rebalancer = _FakeRebalancer(feeder)
    book._live_price["CE"] = 150.5

    # If REGISTRY.get_upstox_key were ever called for the pool-engine path,
    # that alone would indicate the (redundant) execution-contract
    # resolution ran -- fail loudly rather than silently succeeding.
    def _boom(*a, **kw):
        raise AssertionError("REGISTRY.get_upstox_key must not be called for pool-engine entries")
    monkeypatch.setattr("strategies.v4_cascade.book.REGISTRY.get_upstox_key", _boom)

    published = []

    async def fake_publish(topic, event):
        published.append(event)

    monkeypatch.setattr(book._bus, "publish", fake_publish)

    ev = _FakeEvent(side="CE", price_hint=999.0)  # would be wrong if this leaked through
    await asyncio.wait_for(
        book._open_entry_async(ev, exec_strike=23700.0, qty=75, event_id="evt-pool-1",
                                ts=datetime(2026, 7, 21, 9, 15, tzinfo=IST)),
        timeout=0.5,  # the old subscribe/wait path could take up to 3s -- this proves it never runs
    )

    assert feeder.subscribed == []  # never subscribed to a separate execution contract
    assert book._exec_symbol["CE"] == ""  # untouched
    assert book._exec_live_price["CE"] == 0.0  # untouched
    assert len(published) == 1
    assert published[0].price_hint == 150.5  # sourced from self._live_price[side], not ev.price_hint
