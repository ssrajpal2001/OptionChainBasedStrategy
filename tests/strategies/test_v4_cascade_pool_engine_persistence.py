"""V4CascadeBook._persist_position/_restore_position/
_restore_tracker_state_for_open_position all read/write the ACTIVE engine's
position -- self._pool_engine.position when use_pool_engine is set, not
self._engine.position (which stays None the whole time for a pool-engine
book). Without this, a restart mid-trade would silently lose the position
and drop T2's stop-loss enforcement entirely (same bug class fixed for the
old engine on 2026-07-21, here for the new one)."""
from datetime import datetime
from zoneinfo import ZoneInfo

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg

IST = ZoneInfo("Asia/Kolkata")


def _open_pos(side="CE"):
    t1 = TrancheLeg(tranche="T1", option_type=side, strike=23700, qty=65,
                     entry_price=100.0, sl_price=90.0, target_price=110.0)
    t2 = TrancheLeg(tranche="T2", option_type=side, strike=23700, qty=65,
                     entry_price=100.0, sl_price=90.0, tracking_current_stop=95.0)
    return CascadePosition(underlying="NIFTY", side=side, tracking_strike=23700,
                            execution_strike=23700, atm_at_trigger=23700, entry_spot=23700,
                            tracking_entry_price=100.0, t1=t1, t2=t2,
                            open_time=datetime.now(IST))


def test_persist_and_restore_roundtrip_pool_engine_position():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._pool_engine.position = _open_pos("CE")
    book._persist_position()
    assert book._engine.position is None  # old engine untouched

    book2 = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                           binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                           use_pool_engine=True)
    book2._restore_position()
    assert book2._pool_engine.position is not None
    assert book2._pool_engine.position.side == "CE"
    assert book2._engine.position is None
    book._persist_key and __import__("data_layer.position_store", fromlist=["clear"]).clear(book._persist_key)


def test_restore_tracker_state_rebuilds_pool_engine_t2_tracker():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._pool_engine.position = _open_pos("CE")
    book._restore_tracker_state_for_open_position()
    tracker = book._pool_engine._trail["CE"]
    assert tracker is not None
    assert tracker.current_stop == 95.0  # from t2.tracking_current_stop
