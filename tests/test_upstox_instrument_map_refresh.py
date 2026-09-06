"""
tests/test_upstox_instrument_map_refresh.py -- tests for run_system.py's
_refresh_upstox_instrument_maps / _upstox_instrument_map_refresh_loop.

2026-09-04 real incident: the Upstox {canonical_symbol: instrument_key} map
used to be built ONLY ONCE, inline at process boot. A process left running
across a weekly options-expiry rollover kept using the stale map -- every
order attempt then failed with Upstox's own "UDAPI100011 Invalid Instrument
key" because place_order()'s lookup silently fell back to the raw canonical
string once the real key for the CURRENT week's expiry was missing.
Confirmed live: ssrajpal2001's UPSTOX-routed SellStraddle binding failed 3
straight placement retries on both legs, 10 days after the process's last
restart. These tests cover the extracted refresh function (now callable both
at boot and periodically) and the daily-loop's own defensiveness.
"""
from types import SimpleNamespace

import pytest

from run_system import _refresh_upstox_instrument_maps, _upstox_instrument_map_refresh_loop
from data_layer.instrument_registry import REGISTRY


class _FakeClientDB:
    def __init__(self, upstox_creds: dict) -> None:
        self._upstox_creds = upstox_creds

    def get_feeder_creds_sync(self, provider: str) -> dict:
        assert provider == "upstox"
        return self._upstox_creds


class _FakeBrokerWithMap:
    def __init__(self) -> None:
        self.injected: list = []

    def inject_instrument_map(self, mapping: dict) -> None:
        self.injected.append(mapping)


class _FakeBrokerWithoutMap:
    """Some brokers (Zerodha, Delta, ...) have no inject_instrument_map at
    all -- the refresh must skip these via hasattr(), never crash on them."""
    pass


def _fake_router(*brokers) -> SimpleNamespace:
    brokers_by_client = {"c1": {f"b{i}": b for i, b in enumerate(brokers)}}
    return SimpleNamespace(_brokers=brokers_by_client)


@pytest.fixture(autouse=True)
def _isolate_registry(monkeypatch):
    """REGISTRY is a real module-level singleton -- never let a test's
    monkeypatches leak into other tests or hit real network I/O."""
    monkeypatch.setattr(REGISTRY, "load_sync", lambda *a, **kw: None)
    monkeypatch.setattr(REGISTRY, "build_instrument_map", lambda underlying: {})
    yield


@pytest.mark.asyncio
async def test_refresh_injects_into_every_broker_with_the_method(monkeypatch):
    cfg = SimpleNamespace(monitored_indices=["NIFTY"])
    client_db = _FakeClientDB({"access_token": "tok123"})
    with_map = _FakeBrokerWithMap()
    without_map = _FakeBrokerWithoutMap()
    router = _fake_router(with_map, without_map)

    monkey_map = {"NIFTY:08SEP26:24000:CE": "NSE_FO|999"}

    def _build(underlying):
        assert underlying == "NIFTY"
        return monkey_map
    monkeypatch.setattr(REGISTRY, "build_instrument_map", _build)

    await _refresh_upstox_instrument_maps(cfg, router, client_db)

    assert with_map.injected == [monkey_map]
    # the broker with no inject_instrument_map attribute must not raise / be touched
    assert not hasattr(without_map, "injected")


@pytest.mark.asyncio
async def test_refresh_skips_index_when_no_upstox_token(monkeypatch):
    cfg = SimpleNamespace(monitored_indices=["NIFTY"])
    client_db = _FakeClientDB({})   # no access_token

    calls = []
    monkeypatch.setattr(REGISTRY, "load_sync", lambda *a, **kw: calls.append(a))

    await _refresh_upstox_instrument_maps(cfg, SimpleNamespace(_brokers={}), client_db)

    assert calls == []   # never even attempted to load NIFTY without a token


@pytest.mark.asyncio
async def test_refresh_recovers_from_a_failing_index_and_still_processes_the_rest(monkeypatch):
    cfg = SimpleNamespace(monitored_indices=["NIFTY", "SENSEX"])
    client_db = _FakeClientDB({"access_token": "tok123"})

    attempted = []

    def _load_sync(underlying, token):
        attempted.append(underlying)
        if underlying == "NIFTY":
            raise RuntimeError("simulated NSE/Upstox transient failure")

    monkeypatch.setattr(REGISTRY, "load_sync", _load_sync)
    monkeypatch.setattr(REGISTRY, "build_instrument_map", lambda underlying: {})

    # Must not raise -- a bad index must never block the rest of the refresh.
    await _refresh_upstox_instrument_maps(cfg, SimpleNamespace(_brokers={}), client_db)

    assert attempted == ["NIFTY", "SENSEX"]


@pytest.mark.asyncio
async def test_daily_loop_survives_a_refresh_exception(monkeypatch):
    """The loop must never let a refresh failure propagate out to the
    FIRST_COMPLETED task barrier in _run_live -- that would trigger a full
    shutdown+liquidate over a purely cosmetic instrument-map problem."""
    import run_system

    call_count = {"n": 0}

    async def _boom(cfg, router, client_db):
        call_count["n"] += 1
        raise RuntimeError("simulated refresh failure")

    monkeypatch.setattr(run_system, "_refresh_upstox_instrument_maps", _boom)

    sleep_calls = {"n": 0}

    async def _fake_sleep(seconds):
        sleep_calls["n"] += 1
        if sleep_calls["n"] >= 2:
            raise __import__("asyncio").CancelledError()

    monkeypatch.setattr(run_system.asyncio, "sleep", _fake_sleep)

    # Must return (loop breaks on CancelledError) rather than raise.
    await _upstox_instrument_map_refresh_loop(
        SimpleNamespace(monitored_indices=[]), SimpleNamespace(_brokers={}), object()
    )

    assert call_count["n"] == 1
