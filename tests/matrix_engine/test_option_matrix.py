"""
2026-08-13: unit tests for matrix_engine/option_matrix.py -- written after
finding LIVE, in production, that OptionMatrixEngine.initialize() had zero
callers anywhere in the codebase (confirmed via a repo-wide grep), meaning
Topic.MATRIX_SNAPSHOT had never been published for any underlying, ever --
regardless of real tick volume. Confirmed live: NIFTY was receiving ~1600
real option ticks/min (via SellStraddle's own independent consumption of
the same Topic.OPTION_TICK stream) while OI-Flow sat on WAIT for over an
hour, because OptionMatrix.on_option_tick()'s very first check is
`if self._snap is None: return False` -- and self._snap was NEVER set,
because the only thing that ever set it (initialize()) was never called.

No test existed for this file at all before this -- that's how it went
unnoticed. These tests cover the fix (lazy self-initialization from the
first real INDEX_TICK) and the underlying snapshot pipeline end to end.
"""
import asyncio
from datetime import date, datetime

import pytest

from config.global_config import GlobalConfig, IST, Topic
from data_layer.base_feeder import EventBus, IndexTick, OptionTick
from matrix_engine.option_matrix import OptionMatrix, OptionMatrixEngine


def _cfg() -> GlobalConfig:
    return GlobalConfig(monitored_indices=["NIFTY"], active_index="NIFTY")


def _index_tick(symbol="NIFTY", ltp=24450.0, ts=None) -> IndexTick:
    return IndexTick(symbol=symbol, ltp=ltp, open=ltp, high=ltp, low=ltp, close=ltp,
                      volume=0, timestamp=ts or datetime.now(IST))


def _option_tick(strike, side, ltp, oi=1000, symbol="NIFTY24AUG24450CE", ts=None) -> OptionTick:
    return OptionTick(symbol=symbol, underlying="NIFTY", strike=strike, option_type=side,
                       expiry=date(2026, 8, 27), ltp=ltp, bid=ltp, ask=ltp, oi=oi,
                       change_oi=0, volume=100, iv=15.0, delta=0.5, timestamp=ts or datetime.now(IST))


# ── OptionMatrix (per-underlying) ────────────────────────────────────────────

def test_on_option_tick_returns_false_before_initialize():
    """The exact bug: on_option_tick's first check is self._snap is None --
    before initialize() is ever called, every tick is silently dropped for
    snapshot purposes, regardless of volume."""
    mat = OptionMatrix("NIFTY", _cfg())
    tick = _option_tick(24450, "CE", 100.0)
    assert mat.on_option_tick(tick) is False
    assert mat.snapshot() is None


def test_on_option_tick_works_after_initialize():
    mat = OptionMatrix("NIFTY", _cfg())
    mat.initialize(spot=24450.0, expiry=date(2026, 8, 27))
    assert mat.is_initialized() is True
    tick = _option_tick(24450, "CE", 100.0)
    # _recompute_every=10 -- only the 10th tick should signal a recompute.
    for _ in range(9):
        assert mat.on_option_tick(tick) is False
    assert mat.on_option_tick(tick) is True


def test_on_spot_tick_noop_before_initialize():
    mat = OptionMatrix("NIFTY", _cfg())
    mat.on_spot_tick(24500.0)   # must not raise
    assert mat.snapshot() is None


# ── OptionMatrixEngine: lazy self-initialization (the fix) ──────────────────

@pytest.mark.asyncio
async def test_consume_index_lazily_initializes_on_first_real_tick(monkeypatch):
    """The actual fix: a real INDEX_TICK for an underlying with no snapshot
    yet must trigger self-initialization, not silently no-op forever."""
    from matrix_engine import option_matrix as om_module
    monkeypatch.setattr(om_module.REGISTRY, "get_active_expiry",
                         lambda underlying, from_date=None: date(2026, 8, 27))

    bus = EventBus()
    engine = OptionMatrixEngine(bus, _cfg())
    assert engine.get_snapshot("NIFTY") is None

    await bus.publish(Topic.INDEX_TICK, _index_tick(ltp=24450.0))
    engine._running = True
    try:
        await asyncio.wait_for(engine._consume_index(), timeout=0.2)
    except asyncio.TimeoutError:
        pass

    snap = engine.get_snapshot("NIFTY")
    assert snap is not None
    assert snap.spot == 24450.0
    assert snap.expiry == date(2026, 8, 27)


@pytest.mark.asyncio
async def test_consume_index_does_not_reinitialize_once_initialized(monkeypatch):
    from matrix_engine import option_matrix as om_module
    calls = {"n": 0}
    def _fake_expiry(underlying, from_date=None):
        calls["n"] += 1
        return date(2026, 8, 27)
    monkeypatch.setattr(om_module.REGISTRY, "get_active_expiry", _fake_expiry)

    bus = EventBus()
    engine = OptionMatrixEngine(bus, _cfg())
    engine._running = True

    await bus.publish(Topic.INDEX_TICK, _index_tick(ltp=24450.0))
    await bus.publish(Topic.INDEX_TICK, _index_tick(ltp=24460.0))
    try:
        await asyncio.wait_for(engine._consume_index(), timeout=0.2)
    except asyncio.TimeoutError:
        pass

    assert calls["n"] == 1   # only the FIRST tick triggered expiry resolution / init
    assert engine.get_snapshot("NIFTY").spot == 24460.0   # second tick still updated spot


@pytest.mark.asyncio
async def test_lazy_initialize_noop_when_registry_not_loaded(monkeypatch):
    """Registry not yet loaded (get_active_expiry returns None) must not
    crash, and must leave the matrix uninitialized for a later retry."""
    from matrix_engine import option_matrix as om_module
    monkeypatch.setattr(om_module.REGISTRY, "get_active_expiry", lambda underlying, from_date=None: None)

    bus = EventBus()
    engine = OptionMatrixEngine(bus, _cfg())
    engine._running = True

    await bus.publish(Topic.INDEX_TICK, _index_tick(ltp=24450.0))
    try:
        await asyncio.wait_for(engine._consume_index(), timeout=0.2)
    except asyncio.TimeoutError:
        pass

    assert engine.get_snapshot("NIFTY") is None


@pytest.mark.asyncio
async def test_lazy_initialize_noop_on_nonpositive_spot(monkeypatch):
    from matrix_engine import option_matrix as om_module
    monkeypatch.setattr(om_module.REGISTRY, "get_active_expiry",
                         lambda underlying, from_date=None: date(2026, 8, 27))

    bus = EventBus()
    engine = OptionMatrixEngine(bus, _cfg())
    engine._running = True

    await bus.publish(Topic.INDEX_TICK, _index_tick(ltp=0.0))
    try:
        await asyncio.wait_for(engine._consume_index(), timeout=0.2)
    except asyncio.TimeoutError:
        pass

    assert engine.get_snapshot("NIFTY") is None


# ── End-to-end regression: the exact scenario that broke in production ──────

@pytest.mark.asyncio
async def test_full_pipeline_publishes_matrix_snapshot_from_real_ticks(monkeypatch):
    """Regression guard for the exact production symptom: real INDEX_TICK
    followed by real OPTION_TICK volume must result in a genuine
    Topic.MATRIX_SNAPSHOT publish -- proving the full engine.run() pipeline
    (both _consume_index and _consume_options together) actually works end
    to end, not just the lazy-init piece in isolation."""
    from matrix_engine import option_matrix as om_module
    monkeypatch.setattr(om_module.REGISTRY, "get_active_expiry",
                         lambda underlying, from_date=None: date(2026, 8, 27))

    bus = EventBus()
    engine = OptionMatrixEngine(bus, _cfg())
    snap_q = bus.subscribe(Topic.MATRIX_SNAPSHOT)
    engine._running = True

    await bus.publish(Topic.INDEX_TICK, _index_tick(ltp=24450.0))
    for i in range(10):   # _recompute_every=10 -- the 10th option tick triggers a publish
        await bus.publish(Topic.OPTION_TICK, _option_tick(24450, "CE", 100.0 + i))

    async def _drain_both():
        idx_task = asyncio.create_task(engine._consume_index())
        opt_task = asyncio.create_task(engine._consume_options())
        await asyncio.sleep(0.15)
        engine._running = False
        await asyncio.gather(idx_task, opt_task, return_exceptions=True)

    await _drain_both()

    assert not snap_q.empty()
    snap = await snap_q.get()
    assert snap.underlying == "NIFTY"
    assert snap.rows[24450.0].call_ltp == 109.0   # last of the 10 ticks (100+9)
