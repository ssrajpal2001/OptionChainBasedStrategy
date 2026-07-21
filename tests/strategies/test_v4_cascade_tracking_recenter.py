"""V4CascadeBook._maybe_recenter_tracking_strikes -- 2026-07-21: re-centers
the tracking/scanner strikes when the underlying has drifted far enough from
the ATM the CURRENT tracking strikes were derived from, but ONLY while flat
(no open position) -- carrying an open position's zone/SL/target state
across a strike change has no valid conversion between two different
instruments' unrelated price scales, so re-centering never happens mid-trade.

2026-07-22: upgraded to `async def` -- a re-center now also re-warms the new
CE/PE tracking symbols' scanner state from real historical+intraday data
(the same fetch-and-replay sequence _ingest_history already performs at
boot), instead of leaving a re-centered book scanning cold from an empty
reset."""
from datetime import date, datetime
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg

IST = ZoneInfo("Asia/Kolkata")


def _book(underlying="NIFTY"):
    cfg = GlobalConfig()
    book = V4CascadeBook(
        EventBus(), cfg, underlying=underlying, client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15",
    )
    book._running = True
    book._expiry = date(2026, 7, 21)
    book._tracking_reference_atm = 24216.05
    book._ce_strike = 24000
    book._pe_strike = 24400
    return book


@pytest.mark.asyncio
async def test_recenters_when_flat_and_drift_exceeds_threshold_nifty():
    book = _book("NIFTY")
    book._engine.position = None
    assert book._v4cfg.tracking_recenter_pts == 100.0

    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=[])), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
         patch.object(book, "_access_token", return_value="tok"), \
         patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|new"):
        await book._maybe_recenter_tracking_strikes(current_atm=24320.0)   # drift = 103.95 >= 100

    assert book._tracking_reference_atm == 24320.0
    assert book._ce_strike != 24000 or book._pe_strike != 24400


@pytest.mark.asyncio
async def test_does_not_recenter_when_drift_under_threshold():
    book = _book("NIFTY")
    book._engine.position = None

    await book._maybe_recenter_tracking_strikes(current_atm=24250.0)   # drift = 33.95 < 100

    assert book._tracking_reference_atm == 24216.05
    assert book._ce_strike == 24000
    assert book._pe_strike == 24400


@pytest.mark.asyncio
async def test_does_not_recenter_while_position_open_regardless_of_drift():
    book = _book("NIFTY")
    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=24200.0, qty=65, entry_price=20.0, status="open")
    book._engine.position = CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24200.0,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, open_time=datetime(2026, 7, 21, 12, 0, tzinfo=IST),
    )

    await book._maybe_recenter_tracking_strikes(current_atm=25000.0)   # huge drift, but position open

    assert book._tracking_reference_atm == 24216.05   # unchanged
    assert book._ce_strike == 24000
    assert book._pe_strike == 24400


@pytest.mark.asyncio
async def test_crudeoil_uses_200_point_threshold():
    book = _book("CRUDEOIL")
    assert book._v4cfg.tracking_recenter_pts == 200.0


@pytest.mark.asyncio
async def test_recenter_rewarms_scanners_from_real_history_not_bare_reset():
    """The regression this guards against: a re-center that resets scanners
    without re-warming them, leaving the new strikes cold for hours until
    enough live bars accumulate a fresh pattern from scratch.

    Timestamps are 5 minutes apart (not consecutive minutes) so each row
    lands in its OWN 5m bar after _to_5m_bars resamples -- find_all_bear_
    traps_2candle needs >=3 DISTINCT 5m bars to detect a zone (ref candle,
    a sweep candle whose low < ref.low, and a later reclaim candle whose
    high > ref.high); three 1-minute rows inside a single 5-minute bucket
    would collapse to just one bar and could never produce a zone."""
    book = _book("NIFTY")
    book._engine.position = None

    range_rows = [
        {"ts": "2026-07-21T09:15:00", "open": 100, "high": 110, "low": 100, "close": 105, "volume": 10},
        {"ts": "2026-07-21T09:20:00", "open": 98, "high": 105, "low": 95, "close": 100, "volume": 10},
        {"ts": "2026-07-21T09:25:00", "open": 110, "high": 115, "low": 105, "close": 112, "volume": 10},
    ]
    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=range_rows)), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
         patch.object(book, "_access_token", return_value="tok"), \
         patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|new"):
        await book._maybe_recenter_tracking_strikes(current_atm=24320.0)

    # A real zone from the fetched history must now be present -- not an
    # empty reset.
    assert len(book._engine._scanners["CE"].setups) >= 1


@pytest.mark.asyncio
async def test_recenter_leaves_position_and_spot_confirm_untouched():
    book = _book("NIFTY")
    book._engine.position = None
    original_spot_confirm = book._engine._spot_confirm

    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=[])), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
         patch.object(book, "_access_token", return_value="tok"), \
         patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|new"):
        await book._maybe_recenter_tracking_strikes(current_atm=24320.0)

    assert book._engine._spot_confirm is original_spot_confirm   # untouched, same object
    assert book._engine.position is None
