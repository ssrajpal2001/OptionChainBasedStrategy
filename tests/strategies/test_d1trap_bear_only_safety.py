"""
tests/strategies/test_d1trap_bear_only_safety.py — confirm-then-finalize safety
tests for D1TrapBearOnlyBook (strategies/d1_trap_option/bear_only_book.py).

2026-08-05: mirrors tests/strategies/test_sell_straddle_safety.py's
test_close_position_leaves_position_open_when_bridge_reports_exit_aborted shape
-- drives the REAL _enter_leg / _square_off_leg / _on_fill round trip through a
fake bus (not a hand-built isolated harness), asserting a broker-unreachable
EXIT leaves the leg exactly as it was: still in self._positions, unchanged,
still persisted as open.

All tests are async (@pytest.mark.asyncio) because _enter_leg calls
asyncio.create_task(self._bus.publish(...)) internally, which needs a running
event loop -- matches how the book is actually driven in production (from
async tick-processing tasks).
"""
from __future__ import annotations

import asyncio
from datetime import datetime

import pytest

from config.global_config import IST
from data_layer.base_feeder import EventBus
from strategies.d1_trap_option.bear_only_book import D1TrapBearOnlyBook


def _make_book() -> D1TrapBearOnlyBook:
    book = D1TrapBearOnlyBook(
        bus=EventBus(), cfg=None, underlying="NIFTY",
        client_id="C", binding_id="B",
    )
    book._ce_strike = 24500
    book._pe_strike = 24700
    return book


class _RecordingBus:
    """Captures every published (topic, event) pair. Does NOT auto-confirm --
    tests that need a fill wire it up explicitly via a callback."""

    def __init__(self) -> None:
        self.published: list = []

    async def publish(self, topic, event):
        self.published.append((topic, event))


async def _open_leg(book: D1TrapBearOnlyBook, side: str = "PE", entry_price: float = 100.0,
                     sl: float = 90.0) -> dict:
    """Drive a real leg into existence via the real _enter_leg (not a hand-built
    dict) so its shape (including the _event_id tag) is exactly what production
    code produces. _enter_leg's own dispatch is fire-and-forget
    (asyncio.create_task) -- yield once so it actually runs before returning."""
    book._enter_leg(
        side, tranche="single", entry_price=entry_price, sl=sl,
        zone_lock_ts=datetime.now(IST), order_reason="bear_trap_ref_breach_t1",
    )
    assert len(book._positions) == 1
    await asyncio.sleep(0)
    return book._positions[0]


# ── EXIT: confirm-then-finalize ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_square_off_leg_leaves_position_open_when_bridge_reports_exit_failed():
    """Real sequence: _square_off_leg dispatches the SELL via self._bus.publish
    (capturing the real event_id), then a synthetic exit_failed D1TrapFillEvent
    (what the bridge publishes when the broker is unreachable) is fed back
    through the real _on_fill using that same event_id. The leg must come out
    exactly as it went in -- still in self._positions, same strike/entry, not
    persisted as closed."""
    book = _make_book()
    book._bus = _RecordingBus()
    pos = await _open_leg(book)

    class _AbortingBus(_RecordingBus):
        async def publish(self, topic, event):
            await super().publish(topic, event)
            if event.action == "SELL":
                from execution_bridge.d1_trap_bridge import D1TrapFillEvent
                fill = D1TrapFillEvent(
                    action="SELL", underlying=event.underlying,
                    option_type=event.option_type, strike=event.strike,
                    fill_price=0.0, qty=event.quantity,
                    client_id=event.client_id, binding_id=event.binding_id,
                    event_id=event.event_id, exit_failed=True,
                )
                book._on_fill(fill)

    book._bus = _AbortingBus()

    await book._square_off_leg(pos, "sl_hit_hard_cap", 80.0)

    assert pos in book._positions
    assert len(book._positions) == 1
    assert book._positions[0]["side"] == "PE"
    assert book._positions[0]["entry_price"] == 100.0
    assert book._positions[0].get("_closing") is False  # free to retry on the next tick
    assert len(book._bus.published) == 1
    assert book._bus.published[0][0] == "d1_trap_order_request"


@pytest.mark.asyncio
async def test_square_off_leg_leaves_position_open_on_confirmation_timeout(monkeypatch):
    """If the fill event is simply lost (no exit_failed, no real fill -- just
    silence), the wait must time out and leave the leg open rather than hang or
    assume success."""
    book = _make_book()
    book._bus = _RecordingBus()
    pos = await _open_leg(book)
    monkeypatch.setattr(type(book), "_EXIT_CONFIRM_TIMEOUT_SEC", 0.05)

    book._bus = _RecordingBus()  # never delivers a fill

    await book._square_off_leg(pos, "sl_hit_hard_cap", 80.0)

    assert pos in book._positions
    assert len(book._positions) == 1
    assert pos.get("_closing") is False


@pytest.mark.asyncio
async def test_square_off_leg_confirmed_exit_still_finalizes():
    """Sanity check on the happy path: a REAL confirmed SELL fill (not aborted)
    must still remove the leg and persist the close."""
    book = _make_book()
    book._bus = _RecordingBus()
    pos = await _open_leg(book)

    class _ConfirmingBus(_RecordingBus):
        async def publish(self, topic, event):
            await super().publish(topic, event)
            if event.action == "SELL":
                from execution_bridge.d1_trap_bridge import D1TrapFillEvent
                fill = D1TrapFillEvent(
                    action="SELL", underlying=event.underlying,
                    option_type=event.option_type, strike=event.strike,
                    fill_price=80.0, qty=event.quantity,
                    client_id=event.client_id, binding_id=event.binding_id,
                    event_id=event.event_id, exit_failed=False,
                )
                book._on_fill(fill)

    book._bus = _ConfirmingBus()

    await book._square_off_leg(pos, "sl_hit_hard_cap", 80.0)

    assert pos not in book._positions
    assert book._positions == []
    assert len(book._bus.published) == 1


@pytest.mark.asyncio
async def test_square_off_leg_is_a_noop_if_already_closing():
    """A second concurrent square-off attempt on the same still-unconfirmed leg
    (e.g. TSL and EOD racing) must not dispatch a second EXIT."""
    book = _make_book()
    book._bus = _RecordingBus()
    pos = await _open_leg(book)
    pos["_closing"] = True
    bus = _RecordingBus()
    book._bus = bus

    await book._square_off_leg(pos, "eod", 80.0)

    assert bus.published == []
    assert pos in book._positions


# ── ENTRY: reactive revert on abort (Step 5 finding, see report) ────────────


@pytest.mark.asyncio
async def test_enter_leg_reverts_optimistic_leg_when_bridge_reports_entry_aborted():
    """_enter_leg has the SAME optimistic-mutation-before-confirmation shape as
    _square_off_leg (append + persist BEFORE the order is dispatched/confirmed)
    -- but unlike EXIT, ENTRY does not block waiting for confirmation (see
    report for why). Instead it must reactively discard the optimistic leg when
    the bridge reports entry_aborted, exactly mirroring SellStraddle's
    engine.py _on_fill ENTRY-abort branch (self._position = None)."""
    book = _make_book()
    published = []

    class _AbortingEntryBus:
        async def publish(self, topic, event):
            published.append(event)

    book._bus = _AbortingEntryBus()

    book._enter_leg(
        "PE", tranche="single", entry_price=100.0, sl=90.0,
        zone_lock_ts=datetime.now(IST), order_reason="bear_trap_ref_breach_t1",
    )
    assert len(book._positions) == 1
    eid = book._positions[0]["_event_id"]

    # Let the fire-and-forget asyncio.create_task(self._bus.publish(...)) run.
    await asyncio.sleep(0)
    assert len(published) == 1
    assert published[0].event_id == eid

    from execution_bridge.d1_trap_bridge import D1TrapFillEvent
    fill = D1TrapFillEvent(
        action="BUY", underlying="NIFTY", option_type="PE", strike=24700,
        fill_price=0.0, qty=75, client_id="C", binding_id="B",
        event_id=eid, entry_aborted=True, routing_failed=True,
    )
    book._on_fill(fill)

    assert book._positions == []


@pytest.mark.asyncio
async def test_enter_leg_confirmed_entry_is_left_alone():
    """A confirmed (not aborted) ENTRY fill must not touch self._positions --
    the leg already carries its entry_price from the local computation made at
    decision time (matches SellStraddle's shape: confirmed ENTRY updates fill
    prices in place, it doesn't remove anything)."""
    book = _make_book()
    book._bus = _RecordingBus()

    book._enter_leg(
        "PE", tranche="single", entry_price=100.0, sl=90.0,
        zone_lock_ts=datetime.now(IST), order_reason="bear_trap_ref_breach_t1",
    )
    eid = book._positions[0]["_event_id"]
    await asyncio.sleep(0)

    from execution_bridge.d1_trap_bridge import D1TrapFillEvent
    fill = D1TrapFillEvent(
        action="BUY", underlying="NIFTY", option_type="PE", strike=24700,
        fill_price=100.0, qty=75, client_id="C", binding_id="B",
        event_id=eid, entry_aborted=False,
    )
    book._on_fill(fill)

    assert len(book._positions) == 1
    assert book._positions[0]["entry_price"] == 100.0
