"""V4CascadeBook._open_entry_async -- 2026-07-21 wiring: uses
execution_risk.compute_execution_native_risk (fed by _fetch_execution_bars_5m)
to overwrite T1/T2's SL/target with execution-native numbers when a valid
zone is found on the execution strike's own bars; falls back to the
original tracking-scale values (already set by engine.py._open_position)
when it isn't. risk_basis is set accordingly so downstream consumers
(T2's live trailing, restart recovery) know which scale to use."""
from datetime import date, datetime
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
from strategies.v4_cascade.dataclasses import CascadeEvent, CascadeEventType, CascadePosition, TrancheLeg

IST = ZoneInfo("Asia/Kolkata")


class _FakeFeeder:
    async def subscribe_tokens(self, tokens):
        pass


class _FakeRebalancer:
    def __init__(self):
        self._feeder = _FakeFeeder()


def _book():
    cfg = GlobalConfig()
    book = V4CascadeBook(
        EventBus(), cfg, underlying="NIFTY", client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15",
    )
    book._running = True
    book._expiry = date(2026, 7, 21)
    book._rebalancer = _FakeRebalancer()
    return book


def _fresh_position(side="CE", strike=24200.0):
    t1 = TrancheLeg(tranche="T1", option_type=side, strike=strike, qty=65,
                     entry_price=100.0, sl_price=80.0, target_price=130.0, status="open")
    t2 = TrancheLeg(tranche="T2", option_type=side, strike=strike, qty=65,
                     entry_price=100.0, sl_price=80.0, status="open")
    return CascadePosition(
        underlying="NIFTY", side=side, tracking_strike=24000.0, execution_strike=strike,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, t2=t2,
        open_time=datetime(2026, 7, 21, 12, 0, tzinfo=IST), tracking_entry_price=100.0,
    )


def _entry_event(side="CE"):
    return CascadeEvent(event_type=CascadeEventType.OPEN_LONG_CE if side == "CE" else CascadeEventType.OPEN_LONG_PE,
                        side=side, price_hint=21.20, reason="gate3_bear_trap_reclaim",
                        sl_price=80.0, target_price=130.0, timestamp=datetime(2026, 7, 21, 12, 0, tzinfo=IST))


@pytest.mark.asyncio
async def test_uses_execution_native_risk_when_zone_found():
    book = _book()
    book._engine.position = _fresh_position()
    ev = _entry_event()
    with patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|1"), \
         patch.object(book, "_fetch_execution_bars_5m", new=AsyncMock(return_value=["some_bars"])), \
         patch("strategies.v4_cascade.book.compute_execution_native_risk", return_value=(21.0, 25.0)), \
         patch.object(book, "_bus") as mock_bus:
        mock_bus.publish = AsyncMock()
        await book._open_entry_async(ev, exec_strike=24200.0, qty=130, event_id="ev1",
                                     ts=datetime(2026, 7, 21, 12, 0, tzinfo=IST))
    pos = book._engine.position
    assert pos.risk_basis == "execution_native"
    assert pos.t1.sl_price == 21.0
    assert pos.t1.target_price == 25.0
    assert pos.t2.sl_price == 21.0


@pytest.mark.asyncio
async def test_falls_back_to_tracking_scale_when_no_zone_found():
    book = _book()
    book._engine.position = _fresh_position()
    ev = _entry_event()
    with patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|1"), \
         patch.object(book, "_fetch_execution_bars_5m", new=AsyncMock(return_value=[])), \
         patch("strategies.v4_cascade.book.compute_execution_native_risk", return_value=None), \
         patch.object(book, "_bus") as mock_bus:
        mock_bus.publish = AsyncMock()
        await book._open_entry_async(ev, exec_strike=24200.0, qty=130, event_id="ev1",
                                     ts=datetime(2026, 7, 21, 12, 0, tzinfo=IST))
    pos = book._engine.position
    assert pos.risk_basis == "tracking"
    assert pos.t1.sl_price == 80.0    # unchanged, original tracking-scale value
    assert pos.t1.target_price == 130.0


@pytest.mark.asyncio
async def test_crypto_skips_execution_native_lookup_entirely():
    book = _book()
    book._is_crypto = True
    book._engine.position = _fresh_position()
    ev = _entry_event()
    fetch_mock = AsyncMock(return_value=["bars"])
    with patch.object(book, "_fetch_execution_bars_5m", new=fetch_mock), \
         patch.object(book, "_bus") as mock_bus:
        mock_bus.publish = AsyncMock()
        await book._open_entry_async(ev, exec_strike=0.0, qty=130, event_id="ev1",
                                     ts=datetime(2026, 7, 21, 12, 0, tzinfo=IST))
    fetch_mock.assert_not_awaited()
    assert book._engine.position.risk_basis == "tracking"
