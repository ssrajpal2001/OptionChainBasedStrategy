"""strategies/v4_cascade/book.py's _open_entry_async -- 2026-07-21 bugfix:
paper fills (and the ongoing LTP/P&L shown while a position is open) must
use the EXECUTION contract's own real live price, not the TRACKING
contract's price_hint. Confirmed live bug: a NIFTY 24300 CE position's
entry/LTP were both actually the 24100 CE tracking contract's numbers."""
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


class _FakeFeeder:
    def __init__(self) -> None:
        self.subscribed: List[str] = []

    async def subscribe_tokens(self, tokens) -> None:
        self.subscribed.extend(tokens)


class _FakeRebalancer:
    def __init__(self, feeder) -> None:
        self._feeder = feeder


def _book():
    cfg = GlobalConfig()
    book = V4CascadeBook(
        EventBus(), cfg, underlying="NIFTY", client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15",
    )
    book._running = True
    book._expiry = date(2026, 7, 21)
    feeder = _FakeFeeder()
    book._rebalancer = _FakeRebalancer(feeder)
    return book, feeder


@pytest.mark.asyncio
async def test_uses_real_execution_tick_when_it_arrives_in_time(monkeypatch):
    book, feeder = _book()

    monkeypatch.setattr(
        "strategies.v4_cascade.book.REGISTRY.get_upstox_key",
        lambda und, exp, strike, opt_type: f"NSE_FO|{strike}{opt_type}",
    )

    published = []

    async def fake_publish(topic, event):
        published.append(event)

    monkeypatch.setattr(book._bus, "publish", fake_publish)

    async def deliver_tick_soon():
        await asyncio.sleep(0.3)
        # Simulate _option_loop having matched a real tick for the just-
        # subscribed execution contract.
        book._exec_live_price["CE"] = 154.35

    ticker = asyncio.create_task(deliver_tick_soon())
    ev = _FakeEvent(side="CE", price_hint=115.42)  # the WRONG tracking-contract price
    await asyncio.wait_for(
        book._open_entry_async(ev, exec_strike=24300.0, qty=65, event_id="evt1",
                                ts=datetime(2026, 7, 21, 9, 15, tzinfo=IST)),
        timeout=5.0,
    )
    await ticker

    assert feeder.subscribed == ["NSE_FO|24300CE"]
    assert book._exec_symbol["CE"] == "NSE_FO|24300CE"
    assert len(published) == 1
    assert published[0].price_hint == 154.35  # the REAL execution price, not price_hint=115.42


@pytest.mark.asyncio
async def test_falls_back_to_price_hint_when_no_execution_tick_arrives(monkeypatch):
    book, feeder = _book()

    monkeypatch.setattr(
        "strategies.v4_cascade.book.REGISTRY.get_upstox_key",
        lambda und, exp, strike, opt_type: f"NSE_FO|{strike}{opt_type}",
    )

    published = []

    async def fake_publish(topic, event):
        published.append(event)

    monkeypatch.setattr(book._bus, "publish", fake_publish)

    ev = _FakeEvent(side="CE", price_hint=115.42)
    await asyncio.wait_for(
        book._open_entry_async(ev, exec_strike=24300.0, qty=65, event_id="evt2",
                                ts=datetime(2026, 7, 21, 9, 15, tzinfo=IST)),
        timeout=6.0,
    )

    assert feeder.subscribed == ["NSE_FO|24300CE"]
    assert len(published) == 1
    assert published[0].price_hint == 115.42  # fell back, since no real tick ever arrived


@pytest.mark.asyncio
async def test_crypto_skips_subscription_and_uses_price_hint_directly(monkeypatch):
    book, feeder = _book()
    book._is_crypto = True

    published = []

    async def fake_publish(topic, event):
        published.append(event)

    monkeypatch.setattr(book._bus, "publish", fake_publish)

    ev = _FakeEvent(side="CE", price_hint=42000.0)
    await asyncio.wait_for(
        book._open_entry_async(ev, exec_strike=0.0, qty=1, event_id="evt3",
                                ts=datetime(2026, 7, 21, 9, 15, tzinfo=IST)),
        timeout=2.0,
    )

    assert feeder.subscribed == []  # never subscribed -- crypto has no separate execution instrument
    assert len(published) == 1
    assert published[0].price_hint == 42000.0


@pytest.mark.asyncio
async def test_resets_exec_state_at_start_of_every_new_entry(monkeypatch):
    book, feeder = _book()
    # Simulate stale state left over from a PREVIOUS trade.
    book._exec_symbol["CE"] = "NSE_FO|24100CE"
    book._exec_live_price["CE"] = 999.0

    monkeypatch.setattr(
        "strategies.v4_cascade.book.REGISTRY.get_upstox_key",
        lambda und, exp, strike, opt_type: f"NSE_FO|{strike}{opt_type}",
    )

    published = []

    async def fake_publish(topic, event):
        published.append(event)

    monkeypatch.setattr(book._bus, "publish", fake_publish)

    ev = _FakeEvent(side="CE", price_hint=200.0)
    await asyncio.wait_for(
        book._open_entry_async(ev, exec_strike=24300.0, qty=65, event_id="evt4",
                                ts=datetime(2026, 7, 21, 9, 15, tzinfo=IST)),
        timeout=6.0,
    )

    # Old stale price must not leak into the new trade's fallback.
    assert book._exec_symbol["CE"] == "NSE_FO|24300CE"
    assert published[0].price_hint == 200.0  # fell back to THIS trade's price_hint, not the stale 999.0
