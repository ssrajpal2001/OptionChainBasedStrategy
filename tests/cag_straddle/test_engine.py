"""
tests/cag_straddle/test_engine.py -- regression coverage for the 2026-09-08
cross-expiry price-mixing bug: CagStraddleStrategy used to accept ANY
OptionTick matching (strike, side), regardless of expiry, into
self._live_premium and the live hard_risk_cap SL check. Real incident:
CE23800 entered at 83.15 (next-week premium) closed 111ms later at 7.75
(today's near-worthless 0DTE premium for the same strike number) because
both expiries were ticking simultaneously on the shared feed. Fixed via
self._day_expiry, resolved once per day and filtered against every
incoming OptionTick.
"""
import asyncio
from datetime import date, datetime, timedelta

import pytest

from config.global_config import GlobalConfig, IST, Topic
from data_layer.base_feeder import EventBus, OptionTick
from strategies.cag_straddle.engine import CagStraddleStrategy


def _make_book(underlying="NIFTY") -> CagStraddleStrategy:
    bus = EventBus()
    cfg = GlobalConfig()
    book = CagStraddleStrategy(bus, cfg, underlying, "client1", "b1")
    book._loop_queues[Topic.OPTION_TICK] = asyncio.Queue()
    book._running = True  # _option_tick_loop's while-loop guard; not calling full start()
    return book


def _tick(strike, side, expiry, ltp, underlying="NIFTY"):
    return OptionTick(
        symbol=f"{underlying}{strike}{side}", underlying=underlying, strike=float(strike),
        option_type=side, expiry=expiry, ltp=ltp, bid=ltp, ask=ltp, oi=0, change_oi=0,
        volume=0, iv=0.0, delta=0.0, timestamp=datetime.now(IST),
    )


@pytest.mark.asyncio
async def test_mismatched_expiry_tick_is_ignored():
    book = _make_book()
    today = date(2026, 9, 8)
    next_week = date(2026, 9, 15)
    book._today = today
    book._day_expiry = next_week

    q = book._loop_queues[Topic.OPTION_TICK]
    await q.put(_tick(23800, "CE", today, 7.75))       # wrong expiry -- must be ignored
    await q.put(_tick(23800, "CE", next_week, 83.15))  # correct expiry -- must be accepted

    task = asyncio.create_task(book._option_tick_loop())
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert book._live_premium[(23800, "CE")] == 83.15


@pytest.mark.asyncio
async def test_sl_check_never_fires_off_a_different_expirys_tick():
    """The exact real-incident shape: a position is open on next-week's
    23800CE at entry=83.15; a same-strike tick from TODAY's expiry (a
    near-worthless 0DTE premium) must never trigger hard_risk_cap."""
    book = _make_book()
    today = date(2026, 9, 8)
    next_week = date(2026, 9, 15)
    book._today = today
    book._day_expiry = next_week
    book._entry_window_started = True
    book._selected_strikes["CE"] = 23800
    from strategies.cag_straddle.detector import BarAccumulator
    book._bar_accs["CE"] = BarAccumulator()
    book._position = {
        "side": "CE", "strike": 23800, "entry_price": 83.15,
        "entry_ts": datetime.now(IST), "qty_unit": 75, "expiry": next_week,
        "_entry_event_id": "evt1",
    }

    exits = []
    book._exit = lambda reason, exit_price: exits.append((reason, exit_price))

    q = book._loop_queues[Topic.OPTION_TICK]
    # Wrong-expiry tick at a price that WOULD breach hard_risk_cap if accepted.
    await q.put(_tick(23800, "CE", today, 7.75))

    task = asyncio.create_task(book._option_tick_loop())
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert exits == [], f"hard_risk_cap fired off a mismatched-expiry tick: {exits}"


@pytest.mark.asyncio
async def test_matching_expiry_tick_can_still_trigger_hard_risk_cap():
    """Sanity check the fix isn't over-broad -- a genuine same-expiry adverse
    tick must still be able to trigger the hard risk cap."""
    book = _make_book()
    today = date(2026, 9, 8)
    next_week = date(2026, 9, 15)
    book._today = today
    book._day_expiry = next_week
    book._entry_window_started = True
    book._selected_strikes["CE"] = 23800
    from strategies.cag_straddle.detector import BarAccumulator
    book._bar_accs["CE"] = BarAccumulator()
    book._position = {
        "side": "CE", "strike": 23800, "entry_price": 83.15,
        "entry_ts": datetime.now(IST), "qty_unit": 75, "expiry": next_week,
        "_entry_event_id": "evt1",
    }

    exits = []
    book._exit = lambda reason, exit_price: exits.append((reason, exit_price))

    q = book._loop_queues[Topic.OPTION_TICK]
    # Correct-expiry tick, genuinely adverse enough to breach hard_risk_cap
    # (default cap corresponds to entry_price - hard_risk_rs_per_lot/qty_unit).
    risk_floor = 83.15 - (book._hard_risk_rs_per_lot / 75)
    await q.put(_tick(23800, "CE", next_week, risk_floor - 1.0))

    task = asyncio.create_task(book._option_tick_loop())
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert len(exits) == 1
    assert exits[0][0].startswith("hard_risk_cap")
