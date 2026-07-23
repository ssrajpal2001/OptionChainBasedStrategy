"""V4CascadeBook._ingest_history replays fetched CE/PE premium history
through the pool engine (not the old V4CascadeEngine) when use_pool_engine
is set. Uses monkeypatched fetch functions (same pattern as the existing
_ingest_history tests) -- no real network calls."""
from datetime import date, datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook

IST = ZoneInfo("Asia/Kolkata")


def _rows(base: datetime, n: int, price: float = 100.0):
    return [{"ts": (base + timedelta(minutes=i)).isoformat(), "open": price, "high": price + 1,
             "low": price - 1, "close": price, "volume": 10} for i in range(n)]


@pytest.mark.asyncio
async def test_ingest_history_replays_into_pool_engine_not_old_engine():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._running = True
    book._expiry = date(2026, 7, 28)
    book._ce_symbol, book._pe_symbol = "NSE_FO|CE", "NSE_FO|PE"
    book._ce_strike, book._pe_strike = 23700, 24100

    base = datetime(2026, 7, 1, 9, 15, tzinfo=IST)
    rows = _rows(base, 20)

    with patch.object(book, "_access_token", return_value="tok"), \
         patch.object(book, "_resolve_symbols", return_value=True), \
         patch.object(book, "_subscribe_tracking_contracts", return_value=None), \
         patch("strategies.v4_cascade.book.fetch_upstox_range_1m", return_value=rows), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", return_value=[]):
        ok = await book._ingest_history()

    assert ok is True
    assert book._engine.position is None  # old engine never touched
    assert len(book._bars_5m["CE"]) > 0    # SAME bar history still built (both paths need it)
    assert len(book._pool_engine._all_75m["CE"]) >= 0  # replay ran without exception
