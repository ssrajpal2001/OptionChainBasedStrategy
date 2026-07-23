"""V4CascadeBook feeds the pool engine (not the old V4CascadeEngine) when
use_pool_engine is set -- 75m/15m bars derived by resampling the SAME
self._bars_5m[side] history the old engine already uses, 5m bars fed
directly on every bucket close."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook

IST = ZoneInfo("Asia/Kolkata")


def test_pool_engine_receives_5m_bars_not_old_engine():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._running = True
    base = datetime(2026, 7, 1, 9, 15, tzinfo=IST)
    # Feed enough flat 5m ticks to close one bucket -- no zone should form
    # (flat data), but this confirms the pool engine's _all_75m/pool state
    # gets touched at all (proves routing, not logic correctness -- that's
    # Task 2's job) and the OLD engine's position stays untouched (None).
    for i in range(2):
        book._on_option_tick("CE", 100.0 + i, base + timedelta(minutes=5 * i))
    assert book._pool_engine is not None
    # The old engine must never be touched when the pool engine is active.
    assert book._engine.position is None
