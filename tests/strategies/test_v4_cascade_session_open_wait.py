"""strategies/v4_cascade/book.py's _wait_for_session_open_then_fetch --
2026-07-21 bugfix: a book that boots BEFORE the real session-open time (e.g.
09:01 for NIFTY's 09:15 open) must wait for the genuine session-open candle
rather than falling back to whatever live tick happens to be available at
boot (which can be a pre-market/pre-open price, producing wrong strikes for
the whole day)."""
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook

IST = ZoneInfo("Asia/Kolkata")


def _book(underlying="NIFTY"):
    cfg = GlobalConfig()
    return V4CascadeBook(
        EventBus(), cfg, underlying=underlying, client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15",
    )


@pytest.mark.asyncio
async def test_returns_none_immediately_if_already_past_session_open():
    book = _book()
    book._running = True
    # session_open defaults to (9, 15) for NIFTY -- force "now" to be after
    # it by picking a session_open in the recent past relative to real now.
    past = datetime.now(IST) - timedelta(minutes=5)
    book._session_open = (past.hour, past.minute)
    result = await asyncio.wait_for(
        book._wait_for_session_open_then_fetch("fake-token", past.date()), timeout=2.0,
    )
    assert result is None


@pytest.mark.asyncio
async def test_waits_then_retries_fetch_once_session_open_passes(monkeypatch):
    """Fake ``datetime.now`` (module-level, as used inside book.py) to a
    fixed instant 1 second before a minute boundary, and set session_open to
    that next minute -- deterministically produces an ~11s wait (1s to the
    boundary + the function's fixed 10s landing buffer) regardless of when
    this test actually runs, instead of a flaky/variable real-clock delta."""
    book = _book()
    book._running = True
    fixed_now = datetime(2026, 7, 21, 9, 14, 59, tzinfo=IST)
    session_open_target = datetime(2026, 7, 21, 9, 15, 0, tzinfo=IST)
    book._session_open = (session_open_target.hour, session_open_target.minute)

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr("strategies.v4_cascade.book.datetime", _FixedDatetime)

    calls = []

    async def fake_fetch(token, day):
        calls.append((token, day))
        return 24337.50

    monkeypatch.setattr(book, "_fetch_session_open", fake_fetch)
    result = await asyncio.wait_for(
        book._wait_for_session_open_then_fetch("fake-token", fixed_now.date()), timeout=20.0,
    )
    assert result == 24337.50
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_stopping_the_book_interrupts_the_wait(monkeypatch):
    book = _book()
    book._running = True
    far = datetime.now(IST) + timedelta(minutes=30)
    book._session_open = (far.hour, far.minute)

    async def fail_fetch(token, day):
        raise AssertionError("must not be called if the wait is interrupted")

    monkeypatch.setattr(book, "_fetch_session_open", fail_fetch)

    async def stop_soon():
        await asyncio.sleep(0.2)
        book._running = False

    stopper = asyncio.create_task(stop_soon())
    # Outer timeout must comfortably exceed one inner poll iteration (up to
    # 5s) plus the 0.2s stop delay, so the loop gets a chance to re-check
    # self._running and exit on its own rather than racing the outer cancel.
    result = await asyncio.wait_for(
        book._wait_for_session_open_then_fetch("fake-token", far.date()), timeout=8.0,
    )
    await stopper
    assert result is None
