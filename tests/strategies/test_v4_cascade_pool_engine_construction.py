"""V4CascadeBook constructs a PoolCascadeEngine when V4CascadeConfig.
use_pool_engine is True, for NIFTY only -- CRUDEOIL/crypto never get one
regardless of the flag (pool engine is NIFTY-only per the 2026-07-23
design)."""
from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
from strategies.v4_cascade.pool_engine import PoolCascadeEngine


def test_pool_engine_off_by_default():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15")
    assert book._use_pool_engine is False
    assert book._pool_engine is None


def test_pool_engine_constructed_when_enabled():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    assert book._use_pool_engine is True
    assert isinstance(book._pool_engine, PoolCascadeEngine)


def test_pool_engine_never_used_for_crudeoil_even_if_flag_set():
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="CRUDEOIL", client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="23:15",
                          use_pool_engine=True)
    assert book._use_pool_engine is False
    assert book._pool_engine is None
