"""V4CascadeConfig.use_pool_engine -- opt-in flag for the new
PoolCascadeEngine (2026-07-23). Default False preserves today's exact
Gate1/Gate2/Gate3 behavior."""
from strategies.v4_cascade.config import V4CascadeConfig


def test_use_pool_engine_defaults_false():
    cfg = V4CascadeConfig()
    assert cfg.use_pool_engine is False


def test_pool_entry_offset_defaults_to_backtest_best():
    cfg = V4CascadeConfig()
    assert cfg.pool_entry_offset == 5.0
