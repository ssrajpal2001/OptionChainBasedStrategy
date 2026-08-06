"""Regression test for the 2026-08-06 cross-client fill-contamination bug.

Topic.ORDER_FILL is a true broadcast (EventBus.publish() has no per-book
routing) -- every SellStraddleStrategy book subscribed to it receives every
StraddleFillEvent published anywhere in the process. Two different
(client_id, binding_id) books trading the SAME underlying is a real,
confirmed-live scenario (ssrajpal2001/SA5770 and gurmeet/zerodha both running
sell_straddle on NIFTY concurrently on 2026-08-06). Before this fix,
`_fill_loop` filtered incoming fills only by `underlying` -- an
`entry_aborted` fill belonging to one client's failed order would be
delivered to and processed by every other same-underlying book's `_on_fill`,
which unconditionally nulls `self._position` with no further guard. One
client's broker timeout was silently discarding a completely unrelated
client's real, already-confirmed position.

The fix adds a client_id/binding_id identity check in `_fill_loop` itself
(engine.py), before `_on_fill` is ever called -- so a foreign-identity fill
is dropped at the loop level and never reaches the mutation logic at all.
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from config.global_config import Topic
from execution_bridge.straddle_bridge import StraddleFillEvent
from strategies.sell_straddle import SellStraddleStrategy
from strategies.sell_straddle.dataclasses import StraddleLeg, StraddlePosition


def _open_position(ss: SellStraddleStrategy) -> StraddlePosition:
    pos = StraddlePosition(
        underlying=ss._underlying, atm_at_entry=24600.0, entry_spot=24623.0,
        ce_leg=StraddleLeg("CE", 24750.0, 85.40, 85.40, open_time=datetime.now(IST)),
        pe_leg=StraddleLeg("PE", 24600.0, 101.50, 101.50, open_time=datetime.now(IST)),
        net_credit=186.90, open_time=datetime.now(IST), status="open",
        lot_size=ss._lot_size * ss._lot_multiplier, expiry_date=date.today(),
    )
    ss._position = pos
    return pos


async def _publish_and_drain(ss: SellStraddleStrategy, bus: EventBus, event) -> None:
    """Publish `event` on the bus, run the real _fill_loop briefly so it can
    drain it through the real client/binding identity filter, then stop."""
    ss._running = True
    task = asyncio.create_task(ss._fill_loop())
    await asyncio.sleep(0.01)  # let the loop subscribe before we publish
    await bus.publish(Topic.ORDER_FILL, event)
    await asyncio.sleep(0.05)
    ss._running = False
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def test_foreign_client_entry_abort_does_not_touch_this_books_position():
    """The exact 2026-08-06 scenario: ssrajpal2001's real position must survive
    a broker-abort fill event that actually belongs to gurmeet's own order."""
    bus = EventBus()
    ss = SellStraddleStrategy(bus, GlobalConfig(), underlying="NIFTY",
                              client_id="ssrajpal2001", binding_id="SA5770")
    pos = _open_position(ss)
    ss._trades_today = 1

    foreign_abort = StraddleFillEvent(
        action="ENTRY", underlying="NIFTY", atm=24650.0,
        ce_strike=24800.0, pe_strike=24700.0, ce_fill=0.0, pe_fill=0.0,
        client_id="gurmeet", binding_id="zerodha", event_id="foreign_abort_1",
        entry_aborted=True,
    )
    asyncio.run(_publish_and_drain(ss, bus, foreign_abort))

    assert ss._position is pos
    assert ss._position.status == "open"
    assert ss._position.ce_leg.strike == 24750.0
    assert ss._position.pe_leg.strike == 24600.0
    assert ss._trades_today == 1


def test_own_client_entry_abort_still_clears_position():
    """Sanity check: the identity filter must not break the legitimate case --
    a book's OWN abort event must still discard its own optimistic position."""
    bus = EventBus()
    ss = SellStraddleStrategy(bus, GlobalConfig(), underlying="NIFTY",
                              client_id="ssrajpal2001", binding_id="SA5770")
    _open_position(ss)
    ss._trades_today = 1

    own_abort = StraddleFillEvent(
        action="ENTRY", underlying="NIFTY", atm=24600.0,
        ce_strike=24750.0, pe_strike=24600.0, ce_fill=0.0, pe_fill=0.0,
        client_id="ssrajpal2001", binding_id="SA5770", event_id="own_abort_1",
        entry_aborted=True,
    )
    asyncio.run(_publish_and_drain(ss, bus, own_abort))

    assert ss._position is None
    assert ss._trades_today == 0


def test_foreign_client_confirmed_entry_does_not_corrupt_this_books_prices():
    """A REAL confirmed fill for a different client's book must not overwrite
    this book's own open position's leg prices."""
    bus = EventBus()
    ss = SellStraddleStrategy(bus, GlobalConfig(), underlying="NIFTY",
                              client_id="ssrajpal2001", binding_id="SA5770")
    pos = _open_position(ss)

    foreign_confirmed = StraddleFillEvent(
        action="ENTRY", underlying="NIFTY", atm=24650.0,
        ce_strike=24800.0, pe_strike=24700.0, ce_fill=88.25, pe_fill=145.10,
        client_id="gurmeet", binding_id="zerodha", event_id="foreign_confirmed_1",
        entry_aborted=False, legs=["CE", "PE"],
    )
    asyncio.run(_publish_and_drain(ss, bus, foreign_confirmed))

    assert ss._position is pos
    assert ss._position.ce_leg.entry_price == 85.40
    assert ss._position.pe_leg.entry_price == 101.50
