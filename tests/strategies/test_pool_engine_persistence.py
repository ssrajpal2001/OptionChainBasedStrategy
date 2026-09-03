"""
Regression tests for SellStraddleStrategy's pool-engine persistence
(2026-08-21) -- restart-proofing VWAP/SLOPE/RSI/ROC, NOT REST-seeding.

VWAP/SLOPE are deliberately never REST-seeded (2026-08-19 "Seed VWAP
Contamination" fix -- REST-derived bars poisoned the intraday baseline).
That fix left a different gap exposed: every routine mid-day restart
cold-starts self._pool_engine from scratch, running VWAP/SLOPE degraded on
whatever pair is currently open until enough fresh live ticks
re-accumulate. _persist_pool_engine()/_restore_pool_engine() persist the
engine's OWN already-correctly-computed live bars verbatim (same data
surviving a restart, not a different source being fed in), gated to only
restore a SAME-TRADING-DAY snapshot (VWAP/SLOPE are inherently intraday).
"""
from datetime import date, timedelta
from unittest.mock import patch

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy


def _strategy():
    return SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")


def test_persist_pool_engine_saves_snapshot_with_session_day():
    s = _strategy()
    s._pool_engine.update_tick(24000, "CE", 60.0, 50.0)
    s._pool_engine.update_tick(24000, "PE", 40.0, 30.0)
    s._pool_engine.commit_bar(minute=560)

    saved = {}
    with patch("data_layer.position_store.save",
               side_effect=lambda key, data, product_type="MIS": saved.update(key=key, data=data) or True):
        s._persist_pool_engine()

    assert saved["key"] == s._persist_key + "_pool"
    assert saved["data"]["session_day"] == str(s._session_day(__import__("datetime").datetime.now(IST)))
    assert "24000|CE" in saved["data"]["pool_state"]["closes"]


def test_restore_pool_engine_reloads_same_day_snapshot():
    s = _strategy()
    s._pool_engine.update_tick(24000, "CE", 60.0, 50.0)
    s._pool_engine.update_tick(24000, "PE", 40.0, 30.0)
    s._pool_engine.commit_bar(minute=560)
    snapshot = {
        "session_day": str(s._session_day(__import__("datetime").datetime.now(IST))),
        "pool_state": s._pool_engine.to_dict(),
    }

    fresh = _strategy()
    assert fresh._pool_engine.pair_indicators(24000, 24000) is None   # nothing yet

    with patch("data_layer.position_store.load", return_value=snapshot):
        fresh._restore_pool_engine()

    assert fresh._pool_engine.pair_indicators(24000, 24000) is not None


def test_restore_pool_engine_discards_prior_day_snapshot():
    """VWAP/SLOPE are inherently intraday -- a snapshot from a different
    trading day must never be restored, matching the same day-freshness
    discipline _restore_session() already applies to its own fields."""
    s = _strategy()
    s._pool_engine.update_tick(24000, "CE", 60.0, 50.0)
    s._pool_engine.update_tick(24000, "PE", 40.0, 30.0)
    s._pool_engine.commit_bar(minute=560)
    stale_day = str(s._session_day(__import__("datetime").datetime.now(IST)) - timedelta(days=1))
    snapshot = {"session_day": stale_day, "pool_state": s._pool_engine.to_dict()}

    fresh = _strategy()
    with patch("data_layer.position_store.load", return_value=snapshot):
        fresh._restore_pool_engine()

    assert fresh._pool_engine.pair_indicators(24000, 24000) is None


def test_restore_pool_engine_handles_missing_snapshot_gracefully():
    s = _strategy()
    with patch("data_layer.position_store.load", return_value=None):
        s._restore_pool_engine()   # must not raise
    assert s._pool_engine.pair_indicators(24000, 24000) is None
