"""strategies/v4_cascade/book.py's EXIT path -- 2026-07-21 bugfix (found live):
close_long_* events carry ev.price_hint = the TRACKING contract's own
SL/target/trail level (exits.py deliberately watches the tracking contract
to decide WHEN to exit, by design), but that is NOT a real price for the
EXECUTION contract actually held -- using it as the fill price produced a
confirmed live phantom profit of ~Rs11,755 on a real ~Rs15 premium move,
because entry (already fixed to use the real execution price) and exit
(still using the tracking-scale price_hint) were on completely different
price scales. EXIT must use the same real execution-contract live price
already being tracked continuously since entry (_exec_live_price)."""
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
from strategies.v4_cascade.dataclasses import (
    CascadeEvent, CascadeEventType, CascadePosition, TrancheLeg,
)

IST = ZoneInfo("Asia/Kolkata")


def _book():
    cfg = GlobalConfig()
    book = V4CascadeBook(
        EventBus(), cfg, underlying="NIFTY", client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15",
    )
    book._running = True
    book._expiry = date(2026, 7, 21)
    return book


def _open_position(side="CE", entry_price=27.25, strike=24250.0):
    t1 = TrancheLeg(tranche="T1", option_type=side, strike=strike, qty=65,
                     entry_price=entry_price, sl_price=208.10, target_price=250.0, status="open")
    t2 = TrancheLeg(tranche="T2", option_type=side, strike=strike, qty=65,
                     entry_price=entry_price, sl_price=208.10, status="open")
    return CascadePosition(
        underlying="NIFTY", side=side, tracking_strike=24000.0, execution_strike=strike,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, t2=t2, status="open",
    )


@pytest.mark.asyncio
async def test_exit_uses_real_execution_price_not_tracking_sl_level(monkeypatch):
    book = _book()
    book._engine.position = _open_position()
    # This is the exact scale mismatch confirmed live: SL fired at the
    # TRACKING contract's structural level (208.10), while the real
    # execution contract (24250 CE) was actually trading around 30.
    book._exec_live_price["CE"] = 30.85

    published = []

    async def fake_publish(topic, event):
        published.append(event)

    monkeypatch.setattr(book._bus, "publish", fake_publish)

    close_ev = CascadeEvent(
        event_type=CascadeEventType.CLOSE_LONG_CE, side="CE", tranche="T1",
        price_hint=208.10,  # the WRONG tracking-contract SL level
        reason="t1_sl_structural_floor", timestamp=datetime(2026, 7, 21, 10, 20, tzinfo=IST),
    )
    book._emit_order(close_ev)
    for _ in range(20):
        await __import__("asyncio").sleep(0)  # let the fire-and-forget publish task run

    assert len(published) == 1
    assert published[0].price_hint == 30.85  # the REAL execution price, not 208.10


@pytest.mark.asyncio
async def test_exit_falls_back_to_price_hint_when_no_execution_price_tracked(monkeypatch):
    book = _book()
    book._engine.position = _open_position()
    # No _exec_live_price set for CE -- simulates a position whose execution
    # contract was never subscribed/ticked (e.g. thin/illiquid strike).

    published = []

    async def fake_publish(topic, event):
        published.append(event)

    monkeypatch.setattr(book._bus, "publish", fake_publish)

    close_ev = CascadeEvent(
        event_type=CascadeEventType.CLOSE_LONG_CE, side="CE", tranche="T1",
        price_hint=208.10, reason="t1_sl_structural_floor",
        timestamp=datetime(2026, 7, 21, 10, 20, tzinfo=IST),
    )
    book._emit_order(close_ev)
    for _ in range(20):
        await __import__("asyncio").sleep(0)

    assert len(published) == 1
    assert published[0].price_hint == 208.10  # fell back, since no real execution price was tracked


@pytest.mark.asyncio
async def test_crypto_exit_unaffected_uses_price_hint_directly(monkeypatch):
    book = _book()
    book._is_crypto = True
    book._engine.position = _open_position()
    book._exec_live_price["CE"] = 999.0  # should be ignored entirely for crypto

    published = []

    async def fake_publish(topic, event):
        published.append(event)

    monkeypatch.setattr(book._bus, "publish", fake_publish)

    close_ev = CascadeEvent(
        event_type=CascadeEventType.CLOSE_LONG_CE, side="CE", tranche="T1",
        price_hint=42000.0, reason="t1_sl_structural_floor",
        timestamp=datetime(2026, 7, 21, 10, 20, tzinfo=IST),
    )
    book._emit_order(close_ev)
    for _ in range(20):
        await __import__("asyncio").sleep(0)

    assert len(published) == 1
    assert published[0].price_hint == 42000.0
