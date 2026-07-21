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
