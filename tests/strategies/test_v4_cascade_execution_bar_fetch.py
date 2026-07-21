"""V4CascadeBook._fetch_execution_bars_5m -- 2026-07-21, fetches the
execution strike's own historical+intraday 1m bars via the SAME REST
functions _ingest_history already uses for the tracking contract, merges
and resamples to 5m the same way. Needed so compute_execution_native_risk
has real bars to run its zone-detection against at entry time."""
from datetime import date
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook

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


_RANGE_ROWS = [
    {"ts": "2026-07-21T09:15:00", "open": 100, "high": 105, "low": 98, "close": 102, "volume": 10},
    {"ts": "2026-07-21T09:16:00", "open": 102, "high": 106, "low": 100, "close": 104, "volume": 10},
]
_INTRADAY_ROWS = [
    # 09:19, not 09:20 -- _to_5m_bars resamples with closed="left", so the
    # [09:15, 09:20) bucket EXCLUDES a 09:20:00 timestamp (it would fall
    # into the next bucket instead); 09:19:00 is the last minute that is
    # still inside the same bucket as the two 09:15/09:16 range rows.
    {"ts": "2026-07-21T09:19:00", "open": 104, "high": 108, "low": 103, "close": 106, "volume": 10},
]


@pytest.mark.asyncio
async def test_fetches_merges_and_resamples_to_5m_bars():
    book = _book()
    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=_RANGE_ROWS)), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=_INTRADAY_ROWS)), \
         patch.object(book, "_access_token", return_value="tok"):
        bars = await book._fetch_execution_bars_5m("NSE_FO|12345")
    assert len(bars) == 1   # 3 one-minute rows all within the same 09:15-09:20 5m bucket
    assert bars[0].open == 100
    assert bars[0].close == 106


@pytest.mark.asyncio
async def test_returns_empty_list_when_no_access_token():
    book = _book()
    with patch.object(book, "_access_token", return_value=""):
        bars = await book._fetch_execution_bars_5m("NSE_FO|12345")
    assert bars == []
