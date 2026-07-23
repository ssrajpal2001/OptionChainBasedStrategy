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
from strategies.v4_cascade.dataclasses import CascadePosition

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

    # 100 one-minute rows starting at the 09:15 session open crosses a real
    # 75-minute bucket boundary (09:15 -> 10:30) as well as several 15-minute
    # boundaries, so on_75m_bar/on_15m_bar actually fire during replay
    # (20 rows previously used never reached a 75m boundary at all).
    base = datetime(2026, 7, 1, 9, 15, tzinfo=IST)
    rows = _rows(base, 100)

    with patch.object(book, "_access_token", return_value="tok"), \
         patch.object(book, "_resolve_symbols", return_value=True), \
         patch.object(book, "_subscribe_tracking_contracts", return_value=None), \
         patch("strategies.v4_cascade.book.fetch_upstox_range_1m", return_value=rows), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", return_value=[]):
        ok = await book._ingest_history()

    assert ok is True
    assert book._engine.position is None  # old engine never touched
    assert len(book._bars_5m["CE"]) > 0    # SAME bar history still built (both paths need it)
    # 100 1m rows from 09:15 cross a real 75m boundary (09:15->10:30), so
    # on_75m_bar must actually have fired and appended a bar -- a genuinely
    # meaningful assertion (the prior `>= 0` was always true regardless of
    # correctness and proved nothing about the 75m dispatch path).
    assert len(book._pool_engine._all_75m["CE"]) > 0
    assert len(book._pool_engine._all_75m["PE"]) > 0


@pytest.mark.asyncio
async def test_guard_replay_position_restores_into_pool_engine_not_old_engine():
    """Finding 1 regression (task-7 review): _guard_replay_position must
    restore a detected replay-caused position mutation into WHICHEVER
    engine's .position is actually active (self._pool_engine when
    use_pool_engine=True), mirroring the read-side ternary it already uses.
    Pre-fix it unconditionally wrote the restored snapshot into
    self._engine.position -- an object nobody reads when use_pool_engine is
    True -- leaving self._pool_engine.position (the position the rest of the
    system actually reads) with the fabricated mutation. Exercises the
    touched/restore branch directly (no full replay needed): seed a
    pre-replay snapshot of an OPEN position, then simulate replay having
    wrongly flattened self._pool_engine.position to None, and assert the
    guard restores the open position into _pool_engine (not _engine)."""
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)

    open_position = CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=23700, execution_strike=23750,
        atm_at_trigger=23700, entry_spot=23700, status="open",
    )
    pos_snapshot = book._position_snapshot(open_position)

    # Simulate replay fabricating a phantom close on the pool engine's own
    # position (the object the rest of the system reads when
    # use_pool_engine=True).
    book._pool_engine.position = None
    assert book._engine.position is None  # baseline: old engine never touched

    book._guard_replay_position(pos_snapshot)

    # The guard must restore into the ACTIVE engine (pool engine here) ...
    assert book._pool_engine.position is not None
    assert book._pool_engine.position.to_dict() == pos_snapshot
    # ... and must NOT write into the inactive old engine.
    assert book._engine.position is None
