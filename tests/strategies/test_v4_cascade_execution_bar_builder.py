"""V4CascadeBook's execution-contract 5m bar-builder -- 2026-07-21: parallel
to the existing tracking-contract bucket-builder (_on_option_tick /
_close_5m_bucket), but keyed off _exec_symbol ticks and driving
engine.check_exits_execution_native instead of engine.update. Only active
for a risk_basis=="execution_native" open position -- a no-op otherwise, so
this never interferes with tracking-native positions' existing behavior.

NOTE on the first test: the brief's original version of this test was a
plain sync `def`, but `_close_execution_5m_bucket` -> `_emit_order` ->
`_fire` calls `asyncio.create_task()` on the fire-and-forget bus.publish(),
which needs a running loop -- exactly the reason every OTHER book test that
exercises `_emit_order` (see test_v4_cascade_exit_price_tracking.py) is
`@pytest.mark.asyncio` with `book._bus.publish` monkeypatched. Applied the
same established pattern here rather than the brief's sync version, which
fails with `RuntimeError: no running event loop` against the real code."""
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 21, 12, 0, tzinfo=IST)


def _book():
    cfg = GlobalConfig()
    book = V4CascadeBook(
        EventBus(), cfg, underlying="NIFTY", client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15",
    )
    book._running = True
    book._expiry = date(2026, 7, 21)
    return book


def _open_execution_native_position(book):
    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=24200.0, qty=65,
                     entry_price=21.0, sl_price=18.0, target_price=25.0, status="open")
    t2 = TrancheLeg(tranche="T2", option_type="CE", strike=24200.0, qty=65,
                     entry_price=21.0, sl_price=18.0, status="open")
    book._engine.position = CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24200.0,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, t2=t2, open_time=_BASE,
        risk_basis="execution_native",
    )
    return t1, t2


@pytest.mark.asyncio
async def test_execution_ticks_build_5m_bars_and_close_on_bucket_rollover(monkeypatch):
    book = _book()
    t1, t2 = _open_execution_native_position(book)
    book._exec_symbol["CE"] = "NSE_FO|24200CE"

    async def fake_publish(topic, event):
        pass

    monkeypatch.setattr(book._bus, "publish", fake_publish)

    book._on_execution_tick("CE", 22.0, _BASE)
    book._on_execution_tick("CE", 26.0, _BASE + timedelta(minutes=1))   # clears T1 target (25.0) intrabar
    # Next tick in a NEW 5m bucket forces the previous bucket to close and
    # be checked.
    book._on_execution_tick("CE", 24.0, _BASE + timedelta(minutes=6))
    for _ in range(20):
        await __import__("asyncio").sleep(0)  # let the fire-and-forget publish task run

    assert t1.status == "closed"
    assert t1.close_reason == "t1_target_2r"


@pytest.mark.asyncio
async def test_orphaned_bucket_cleared_on_close_does_not_poison_next_position(monkeypatch):
    """Regression for the Critical review finding: when a position closes
    (routinely -- via check_exits_execution_native itself, EOD, structural
    flip, or a roll), the in-progress bar sitting in _exec_buckets[side] must
    NOT survive to be fed -- with completely foreign OHLC -- into the NEXT
    position opened on the same side (the normal same-day re-entry case).

    Sequence:
      1. Open an execution-native CE position and feed ticks that build an
         in-progress bucket with a HIGH (30.0) that would clear ANY
         plausible new position's T1 target if it ever leaked through.
      2. Close the position directly (pos.status = "closed"), exactly as
         check_exits_execution_native / EOD / a roll would -- WITHOUT ever
         flushing that in-progress bucket.
      3. Feed one more CE tick while the position is closed. The fixed guard
         clause must clear _exec_buckets["CE"] to None here (proving the
         orphaned bar cannot survive to poison a future bucket-rollover).
      4. Open a brand-new CE position (different SL/target) and feed ticks
         building a genuinely fresh bucket, then roll into a new 5m bucket.
         T1 must NOT spuriously close off the OLD bar's stale high=30.0 --
         only the new ticks' own (much lower) high matters.
    """
    book = _book()
    t1_old, t2_old = _open_execution_native_position(book)
    book._exec_symbol["CE"] = "NSE_FO|24200CE"

    published = []

    async def fake_publish(topic, event):
        published.append(event)

    monkeypatch.setattr(book._bus, "publish", fake_publish)

    # (1) Build an in-progress bucket for the OLD position with a stale HIGH
    # (30.0) that would blow through a new position's target if it leaked.
    book._on_execution_tick("CE", 22.0, _BASE)
    book._on_execution_tick("CE", 30.0, _BASE + timedelta(minutes=1))
    assert book._exec_buckets["CE"] is not None
    assert book._exec_buckets["CE"].high == 30.0

    # (2) Position closes routinely -- WITHOUT this bucket ever being
    # flushed via _close_execution_5m_bucket (mirrors check_exits_execution_native
    # closing both legs, EOD square-off, structural flip, or a roll).
    t1_old.status = "closed"
    t2_old.status = "closed"
    book._engine.position.status = "closed"

    # (3) A tick still arrives for "CE" while flat (feed doesn't know the
    # position closed). The fixed guard must clear the orphaned bucket here.
    book._on_execution_tick("CE", 10.0, _BASE + timedelta(minutes=2))
    assert book._exec_buckets["CE"] is None, (
        "orphaned in-progress bucket must be cleared once the guard sees no "
        "matching open execution-native position for this side"
    )

    # (4) A brand-new CE position opens intraday (same-day re-entry), with a
    # target (26.0) that the OLD stale bar's high (30.0) would have cleared
    # spuriously if it had leaked into the rollover close.
    t1_new = TrancheLeg(tranche="T1", option_type="CE", strike=24300.0, qty=65,
                         entry_price=21.0, sl_price=18.0, target_price=26.0, status="open")
    t2_new = TrancheLeg(tranche="T2", option_type="CE", strike=24300.0, qty=65,
                         entry_price=21.0, sl_price=18.0, status="open")
    book._engine.position = CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24300.0,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1_new, t2=t2_new,
        open_time=_BASE + timedelta(minutes=3), risk_basis="execution_native",
    )

    # New position's own ticks build a genuinely fresh bucket, high 22.0 --
    # well under its 26.0 target.
    book._on_execution_tick("CE", 21.0, _BASE + timedelta(minutes=3))
    book._on_execution_tick("CE", 22.0, _BASE + timedelta(minutes=4))
    # Next tick in a NEW 5m bucket forces the (fresh) previous bucket closed
    # and checked -- this is the exact rollover branch the bug corrupted.
    book._on_execution_tick("CE", 23.0, _BASE + timedelta(minutes=6))
    for _ in range(20):
        await __import__("asyncio").sleep(0)  # let the fire-and-forget publish task run

    assert t1_new.status == "open", (
        "T1 must NOT spuriously close off the orphaned OLD position's stale "
        "bar (high=30.0 would have cleared this position's 26.0 target)"
    )
    assert published == []  # no spurious CLOSE order emitted for the new position


def test_noop_for_tracking_native_position():
    book = _book()
    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=24200.0, qty=65,
                     entry_price=21.0, sl_price=18.0, target_price=25.0, status="open")
    t2 = TrancheLeg(tranche="T2", option_type="CE", strike=24200.0, qty=65,
                     entry_price=21.0, sl_price=18.0, status="open")
    book._engine.position = CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24200.0,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, t2=t2, open_time=_BASE,
        risk_basis="tracking",
    )
    book._exec_symbol["CE"] = "NSE_FO|24200CE"

    book._on_execution_tick("CE", 26.0, _BASE)
    book._on_execution_tick("CE", 26.0, _BASE + timedelta(minutes=6))

    assert t1.status == "open"   # untouched -- tracking-native positions ignore this clock
