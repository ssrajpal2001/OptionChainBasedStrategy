"""Pool-engine positions fill/exit on the TRACKING contract directly --
no execution-strike resolution, no execution-native-risk lookback. Strike
fields on the opened position match the book's own resolved tracking
strike for that side."""
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook

IST = ZoneInfo("Asia/Kolkata")


@pytest.mark.asyncio
async def test_pool_engine_open_uses_tracking_strike_not_execution_strike():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._running = True
    book._ce_strike, book._pe_strike = 23700, 24100
    book._live_price["CE"] = 150.5

    # Directly exercise the pool engine's own open (mirrors what on_5m_bar would do),
    # then let _emit_order route it.
    book._pool_engine.position = None  # ensure clean state
    from strategies.v4_cascade.pool_engine import _ZoneSlot
    from strategies.v4_cascade.dataclasses import RollingBaseZone
    zone = RollingBaseZone(entry_line=145.0, sweep_low=135.0, sl_level=170.0,
                            reference_low_ts=datetime.now(IST), lock_ts=datetime.now(IST), locked=True)
    slot = _ZoneSlot(zone)
    slot.ltf_zone = RollingBaseZone(entry_line=148.0, sweep_low=142.0, sl_level=155.0,
                                     reference_low_ts=datetime.now(IST), lock_ts=datetime.now(IST), locked=True)
    real_ev = book._pool_engine._open_position("CE", slot, 150.0, datetime.now(IST))

    book._emit_order(real_ev, pos_before=None)

    assert book._pool_engine.position.t1.strike == 23700
    assert book._pool_engine.position.execution_strike == 23700
