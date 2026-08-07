"""
tests/strategies/test_d1trap_bear_only_entry_stop_for_day.py — BearTrap must stop
attempting new entries after repeated real broker rejections, exactly like
SellStraddle's 2026-08-06 fix (strategies/sell_straddle/engine.py _on_fill).

2026-08-07 finding: unlike SellStraddle, D1TrapBearOnlyBook's _on_fill correctly
discards the optimistic leg on entry_aborted (test_d1trap_bear_only_safety.py
already covers that), but had NO memory of repeated failures across entries --
a broker rejecting every order all day (e.g. insufficient funds) would let
every subsequent zone/tranche trigger dispatch a fresh real order forever,
never pausing. This mirrors the exact SellStraddle incident (ssrajpal2001,
2026-08-06/07) that motivated this fix, adapted to BearTrap's event-driven
(not tick-loop) entry shape.
"""
from __future__ import annotations

import asyncio
from datetime import datetime

import pytest

from config.global_config import IST
from strategies.d1_trap_option.bear_only_book import D1TrapBearOnlyBook


def _make_book() -> D1TrapBearOnlyBook:
    from data_layer.base_feeder import EventBus
    book = D1TrapBearOnlyBook(
        bus=EventBus(), cfg=None, underlying="NIFTY",
        client_id="C", binding_id="B",
    )
    book._ce_strike = 24500
    book._pe_strike = 24700
    return book


class _RecordingBus:
    def __init__(self) -> None:
        self.published: list = []

    async def publish(self, topic, event):
        self.published.append((topic, event))


async def _reject_one_entry(book: D1TrapBearOnlyBook) -> None:
    """Drive one real _enter_leg -> entry_aborted round trip (mirrors
    test_d1trap_bear_only_safety.py's pattern)."""
    from execution_bridge.d1_trap_bridge import D1TrapFillEvent

    class _AbortingBus(_RecordingBus):
        async def publish(self, topic, event):
            await super().publish(topic, event)
            fill = D1TrapFillEvent(
                action="BUY", underlying="NIFTY", option_type=event.option_type,
                strike=event.strike, fill_price=0.0, qty=event.quantity,
                client_id=event.client_id, binding_id=event.binding_id,
                event_id=event.event_id, entry_aborted=True, routing_failed=False,
            )
            book._on_fill(fill)

    book._bus = _AbortingBus()
    book._enter_leg(
        "PE", tranche="single", entry_price=100.0, sl=90.0,
        zone_lock_ts=datetime.now(IST), order_reason="bear_trap_ref_breach_t1",
    )
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_three_consecutive_entry_rejections_sets_stop_for_day():
    book = _make_book()
    for _ in range(3):
        await _reject_one_entry(book)
        book._positions = []  # each rejection reverts to flat, as the safety tests confirm

    assert book._stop_for_day is True
    assert book._positions == []

    # A further entry attempt must be a no-op -- no phantom leg, no real order dispatched.
    published: list = []

    class _TrackingBus(_RecordingBus):
        async def publish(self, topic, event):
            published.append(event)

    book._bus = _TrackingBus()
    book._enter_leg(
        "PE", tranche="single", entry_price=100.0, sl=90.0,
        zone_lock_ts=datetime.now(IST), order_reason="bear_trap_ref_breach_t1",
    )
    await asyncio.sleep(0)
    assert book._positions == []
    assert published == []


@pytest.mark.asyncio
async def test_confirmed_entry_resets_consecutive_rejection_counter():
    from execution_bridge.d1_trap_bridge import D1TrapFillEvent

    book = _make_book()
    await _reject_one_entry(book)
    book._positions = []
    await _reject_one_entry(book)
    book._positions = []
    assert book._consecutive_entry_rejections == 2
    assert book._stop_for_day is False

    # A confirmed (non-aborted) fill resets the streak.
    book._bus = _RecordingBus()
    book._enter_leg(
        "PE", tranche="single", entry_price=100.0, sl=90.0,
        zone_lock_ts=datetime.now(IST), order_reason="bear_trap_ref_breach_t1",
    )
    await asyncio.sleep(0)
    eid = book._positions[0]["_event_id"]
    fill = D1TrapFillEvent(
        action="BUY", underlying="NIFTY", option_type="PE", strike=24700,
        fill_price=100.0, qty=75, client_id="C", binding_id="B",
        event_id=eid, entry_aborted=False,
    )
    book._on_fill(fill)
    assert book._consecutive_entry_rejections == 0

    # Two more rejections after the reset must NOT trip stop_for_day yet (needs a
    # fresh streak of 3, the prior two don't carry over across a confirmed entry).
    book._positions = []
    await _reject_one_entry(book)
    book._positions = []
    await _reject_one_entry(book)
    book._positions = []
    assert book._stop_for_day is False


@pytest.mark.asyncio
async def test_reset_session_clears_stop_for_day():
    book = _make_book()
    for _ in range(3):
        await _reject_one_entry(book)
        book._positions = []
    assert book._stop_for_day is True

    book.reset_session()

    assert book._stop_for_day is False
    assert book._consecutive_entry_rejections == 0
