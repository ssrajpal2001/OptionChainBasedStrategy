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
    # 2026-07-24: was book._on_option_tick("CE", ...) -- that method is now
    # legacy-engine-only (this task split pool-engine tick handling into
    # its own _on_option_tick_pool, which needs an explicit strike).
    # Updated so this test still actually exercises the pool engine's own
    # tick path instead of silently testing nothing.
    for i in range(2):
        book._on_option_tick_pool("CE", 23700, 100.0 + i, base + timedelta(minutes=5 * i))
    assert book._pool_engine is not None
    # The old engine must never be touched when the pool engine is active.
    assert book._engine.position is None
    # Strengthened: the pool engine's own per-candidate bucket must have
    # actually received these ticks -- the old assertions only checked
    # book._engine.position is None, which is trivially true for flat data
    # regardless of which engine processed it and proves nothing about
    # routing on its own.
    assert book._pool_buckets[("CE", 23700)] is not None
    assert book._pool_buckets[("CE", 23700)].close == 101.0


def test_pool_engine_two_candidates_bucket_independently():
    """Two different CE candidates (24000, 23900) receiving ticks must
    accumulate into SEPARATE 5m buckets -- a tick for 23900 must never be
    folded into 24000's in-progress bar, since they're different
    instruments at different price scales."""
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._running = True
    ts = datetime(2026, 7, 1, 9, 15, tzinfo=IST)
    book._on_option_tick_pool("CE", 24000, 100.0, ts)
    book._on_option_tick_pool("CE", 23900, 250.0, ts)
    assert book._pool_buckets[("CE", 24000)].close == 100.0
    assert book._pool_buckets[("CE", 23900)].close == 250.0
