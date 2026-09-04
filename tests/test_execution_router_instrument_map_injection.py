"""
tests/test_execution_router_instrument_map_injection.py -- regression for a
real 2026-09-04 incident: run_system.py's boot-time Upstox instrument-map
build (_refresh_upstox_instrument_maps) runs BEFORE ExecutionRouter.start()
is even scheduled as a task, so on every single boot the map had nothing to
inject into -- ExecutionRouter._brokers is what start() itself populates.
Confirmed live twice: the map was built with the correct current-week expiry
seconds before the broker authenticated, and the very next SellStraddle
entry on that binding still failed with Upstox's "Invalid Instrument key".

Fix: ExecutionRouter.start() now injects whatever instrument map is
currently cached in the registry the moment each broker successfully
authenticates, instead of relying on a fixed point in the boot sequence
lining up with when authentication happens to finish.
"""
from types import SimpleNamespace

import pytest

from execution_bridge import execution_router as er_module
from execution_bridge.execution_router import ExecutionRouter
from data_layer.instrument_registry import REGISTRY


class _FakeBrokerWithMap:
    def __init__(self, *a, **kw) -> None:
        self.injected: list = []

    async def authenticate(self) -> bool:
        return True

    async def logout(self) -> None:
        pass

    def inject_instrument_map(self, mapping: dict) -> None:
        self.injected.append(mapping)


class _FakeBrokerWithoutMap:
    """Zerodha/Delta/etc -- no inject_instrument_map at all."""
    def __init__(self, *a, **kw) -> None:
        pass

    async def authenticate(self) -> bool:
        return True

    async def logout(self) -> None:
        pass


class _FakeBinding:
    def __init__(self, binding_id: str, provider: str, is_trade_enabled: bool = True) -> None:
        self.binding_id = binding_id
        self.provider = provider
        self.is_trade_enabled = is_trade_enabled


class _FakeClient:
    def __init__(self, client_id: str, bindings: list) -> None:
        self.client_id = client_id
        self._bindings = bindings

    def enabled_brokers(self) -> list:
        return self._bindings


class _FakeRegistry:
    def __init__(self, clients: list) -> None:
        self._clients = clients

    def all_active(self) -> list:
        return self._clients


class _FakeWorkerPool:
    def __init__(self) -> None:
        self.registered: list = []

    def register(self, worker) -> None:
        self.registered.append(worker)

    async def start_all(self) -> None:
        pass


@pytest.fixture(autouse=True)
def _isolate_registry(monkeypatch):
    monkeypatch.setattr(REGISTRY, "build_instrument_map", lambda underlying: {"K": f"NSE_FO|{underlying}"})
    yield


def _fake_create_broker(broker_map: dict):
    def _create(binding, client_id):
        cls = broker_map.get(binding.provider, _FakeBrokerWithoutMap)
        return cls()
    return _create


@pytest.mark.asyncio
async def test_broker_gets_the_currently_cached_map_the_moment_it_authenticates(monkeypatch):
    """The exact real-incident scenario: the map was already built (cached in
    REGISTRY) BEFORE this broker existed at all -- start() must still inject
    it right here, not rely on a separate boot-time pass that already ran."""
    client = _FakeClient("ssrajpal2001", [_FakeBinding("UPSTOX", "upstox")])
    registry = _FakeRegistry([client])
    cfg = SimpleNamespace(monitored_indices=["NIFTY"])

    monkeypatch.setattr(er_module, "create_broker", _fake_create_broker({"upstox": _FakeBrokerWithMap}))
    monkeypatch.setattr(er_module, "ClientExecutionWorker", lambda **kw: SimpleNamespace())

    router = ExecutionRouter(bus=SimpleNamespace(subscribe=lambda t: None), registry=registry, cfg=cfg)
    router._pool = _FakeWorkerPool()

    await router.start()

    broker = router._brokers["ssrajpal2001"]["UPSTOX"]
    assert broker.injected == [{"K": "NSE_FO|NIFTY"}]


@pytest.mark.asyncio
async def test_broker_without_inject_instrument_map_is_skipped_safely(monkeypatch):
    """Zerodha/Delta/etc brokers have no inject_instrument_map at all --
    must authenticate normally without raising."""
    client = _FakeClient("gurmeet", [_FakeBinding("zerodha", "zerodha")])
    registry = _FakeRegistry([client])
    cfg = SimpleNamespace(monitored_indices=["NIFTY"])

    monkeypatch.setattr(er_module, "create_broker", _fake_create_broker({"zerodha": _FakeBrokerWithoutMap}))
    monkeypatch.setattr(er_module, "ClientExecutionWorker", lambda **kw: SimpleNamespace())

    router = ExecutionRouter(bus=SimpleNamespace(subscribe=lambda t: None), registry=registry, cfg=cfg)
    router._pool = _FakeWorkerPool()

    await router.start()   # must not raise

    assert "zerodha" in router._brokers["gurmeet"]


@pytest.mark.asyncio
async def test_injection_failure_does_not_block_authentication(monkeypatch):
    """A broken/empty registry (build_instrument_map raises) must never
    prevent the broker itself from being registered and usable."""
    client = _FakeClient("ssrajpal2001", [_FakeBinding("UPSTOX", "upstox")])
    registry = _FakeRegistry([client])
    cfg = SimpleNamespace(monitored_indices=["NIFTY"])

    monkeypatch.setattr(REGISTRY, "build_instrument_map",
                         lambda underlying: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(er_module, "create_broker", _fake_create_broker({"upstox": _FakeBrokerWithMap}))
    monkeypatch.setattr(er_module, "ClientExecutionWorker", lambda **kw: SimpleNamespace())

    router = ExecutionRouter(bus=SimpleNamespace(subscribe=lambda t: None), registry=registry, cfg=cfg)
    router._pool = _FakeWorkerPool()

    await router.start()   # must not raise

    assert "UPSTOX" in router._brokers["ssrajpal2001"]


@pytest.mark.asyncio
async def test_empty_cached_map_is_not_injected(monkeypatch):
    """If the registry hasn't loaded this index yet, build_instrument_map()
    returns {} -- must not overwrite the broker's map with an empty one."""
    client = _FakeClient("ssrajpal2001", [_FakeBinding("UPSTOX", "upstox")])
    registry = _FakeRegistry([client])
    cfg = SimpleNamespace(monitored_indices=["NIFTY"])

    monkeypatch.setattr(REGISTRY, "build_instrument_map", lambda underlying: {})
    monkeypatch.setattr(er_module, "create_broker", _fake_create_broker({"upstox": _FakeBrokerWithMap}))
    monkeypatch.setattr(er_module, "ClientExecutionWorker", lambda **kw: SimpleNamespace())

    router = ExecutionRouter(bus=SimpleNamespace(subscribe=lambda t: None), registry=registry, cfg=cfg)
    router._pool = _FakeWorkerPool()

    await router.start()

    broker = router._brokers["ssrajpal2001"]["UPSTOX"]
    assert broker.injected == []
