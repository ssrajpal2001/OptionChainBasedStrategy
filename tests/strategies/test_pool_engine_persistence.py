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


# ── 2026-09-16, direct user spec: overnight hedge-and-carry positions get a
# SEPARATE, cross-day-surviving copy of the pool-engine state, so a rollover/
# re-entry decision the very next trading morning has a warm baseline instead
# of waiting for VWAP/SLOPE to rebuild from scratch. A same-day close (never
# actually carried) never writes this key at all, and it's explicitly cleared
# the moment a hedged position is genuinely closed. This is per-(client,
# binding,underlying,strategy) via the existing self._persist_key -- N
# different clients/bindings each get their own independent carry file,
# never a shared/global one. ───────────────────────────────────────────────

class _FakePosition:
    def __init__(self, is_hedged_positional=True):
        self.is_hedged_positional = is_hedged_positional


def test_persist_pool_engine_also_saves_carry_key_when_hedged_positional():
    s = _strategy()
    s._position = _FakePosition(is_hedged_positional=True)
    s._pool_engine.update_tick(24000, "CE", 60.0, 50.0)
    s._pool_engine.commit_bar(minute=560)

    saves = []
    with patch("data_layer.position_store.save",
               side_effect=lambda key, data, product_type="MIS": saves.append((key, data, product_type)) or True):
        s._persist_pool_engine()

    keys = [k for k, _, _ in saves]
    assert s._persist_key + "_pool" in keys
    assert s._persist_key + "_pool_carry" in keys
    carry = next(d for k, d, _ in saves if k == s._persist_key + "_pool_carry")
    carry_pt = next(pt for k, _, pt in saves if k == s._persist_key + "_pool_carry")
    assert carry_pt == "NRML"   # so position_store's own MIS-new-day-discard never wipes it
    assert "24000|CE" in carry["pool_state"]["closes"]


def test_persist_pool_engine_never_saves_carry_key_when_not_hedged():
    s = _strategy()
    s._position = _FakePosition(is_hedged_positional=False)
    s._pool_engine.update_tick(24000, "CE", 60.0, 50.0)
    s._pool_engine.commit_bar(minute=560)

    saves = []
    with patch("data_layer.position_store.save",
               side_effect=lambda key, data, product_type="MIS": saves.append(key) or True):
        s._persist_pool_engine()

    assert s._persist_key + "_pool_carry" not in saves


def test_persist_pool_engine_never_saves_carry_key_when_flat():
    s = _strategy()
    assert s._position is None
    s._pool_engine.update_tick(24000, "CE", 60.0, 50.0)
    s._pool_engine.commit_bar(minute=560)

    saves = []
    with patch("data_layer.position_store.save",
               side_effect=lambda key, data, product_type="MIS": saves.append(key) or True):
        s._persist_pool_engine()

    assert s._persist_key + "_pool_carry" not in saves


def test_restore_pool_engine_uses_carry_key_ignoring_day_boundary_when_hedged():
    """The defining case: a hedged position restored on a genuinely NEW
    trading day must still get its warm pool state -- the day-boundary
    discard that applies to the plain intraday _pool key must NOT apply
    here."""
    s = _strategy()
    s._pool_engine.update_tick(24000, "CE", 60.0, 50.0)
    s._pool_engine.update_tick(24000, "PE", 40.0, 30.0)
    s._pool_engine.commit_bar(minute=560)
    carry_snapshot = {"pool_state": s._pool_engine.to_dict()}   # no session_day at all -- carry key is day-agnostic
    raw_position = {"date": "2026-09-16", "product_type": "NRML",
                    "position": {"is_hedged_positional": True}}

    def _fake_load(key):
        if key.endswith("_pool_carry"):
            return carry_snapshot
        if key.endswith("_pool"):
            raise AssertionError("must not read the plain intraday _pool key when hedged")
        return raw_position   # bare self._persist_key -- the raw saved position peek
    fresh = _strategy()
    with patch("data_layer.position_store.load", side_effect=_fake_load):
        fresh._restore_pool_engine()

    assert fresh._pool_engine.pair_indicators(24000, 24000) is not None


def test_restore_pool_engine_falls_back_to_intraday_key_when_not_hedged():
    s = _strategy()
    s._pool_engine.update_tick(24000, "CE", 60.0, 50.0)
    s._pool_engine.update_tick(24000, "PE", 40.0, 30.0)
    s._pool_engine.commit_bar(minute=560)
    intraday_snapshot = {
        "session_day": str(s._session_day(__import__("datetime").datetime.now(IST))),
        "pool_state": s._pool_engine.to_dict(),
    }
    raw_position = {"date": "2026-09-16", "product_type": "MIS",
                    "position": {"is_hedged_positional": False}}

    def _fake_load(key):
        if key.endswith("_pool_carry"):
            raise AssertionError("must not read the carry key when not hedged")
        if key.endswith("_pool"):
            return intraday_snapshot
        return raw_position
    fresh = _strategy()
    with patch("data_layer.position_store.load", side_effect=_fake_load):
        fresh._restore_pool_engine()

    assert fresh._pool_engine.pair_indicators(24000, 24000) is not None


def test_persist_clears_carry_key_when_position_genuinely_closes():
    """2026-09-16, direct user spec: 'if same day close we can wipe off the
    data' -- the moment a (previously hedged) position is genuinely closed,
    the stale carry-forward pool file must not linger to be picked up by a
    future, unrelated fresh position on the same persist key."""
    s = _strategy()
    s._position = None   # _persist()'s own clear branch fires when falsy

    cleared = []
    with patch("data_layer.position_store.clear",
               side_effect=lambda key: cleared.append(key) or True), \
         patch("data_layer.position_store.save", return_value=True):
        s._persist()

    assert s._persist_key + "_pool_carry" in cleared
