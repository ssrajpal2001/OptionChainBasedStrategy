import asyncio
import pytest


@pytest.mark.asyncio
async def test_no_paper_fallback_when_broker_missing_in_live_mode(monkeypatch):
    """
    Real routing entry point is FVGExecutionBridge._handle(ev) -- it computes
    live_binding/db internally from self._router. This test drives it through
    that real path: a live-mode binding whose broker never resolves must NOT
    fall back to _paper_fill, and must publish a SYSTEM_EVENT alert instead.
    """
    from execution_bridge.fvg_bridge import FVGExecutionBridge

    calls = {"paper_fill": 0, "alerts": 0}

    class _FakeBus:
        async def publish(self, topic, event):
            calls["alerts"] += 1

        def subscribe(self, topic):
            class _Q:
                async def get(self):
                    await asyncio.sleep(3600)
            return _Q()

    class _FakeDB:
        def get_bindings_safe_sync(self, client_id):
            return [{
                "binding_id": "zerodha",
                "trading_mode": "live",
                "terminal_connected": True,
            }]

        def get_deployments_sync(self, client_id):
            return [{
                "binding_id": "zerodha",
                "strategy_name": "fvg",
                "underlying": "NIFTY",
                "is_running": 1,
            }]

    class _FakeRouter:
        _brokers = {}  # always empty -- broker never resolves
        _client_db = _FakeDB()

    bridge = FVGExecutionBridge.__new__(FVGExecutionBridge)
    bridge._bus = _FakeBus()
    bridge._router = _FakeRouter()
    bridge._trade_log = None

    async def _fake_paper_fill(ev):
        calls["paper_fill"] += 1
    bridge._paper_fill = _fake_paper_fill

    class _Ev:
        action = "BUY"
        client_id = "gurmeet"
        binding_id = "zerodha"
        underlying = "NIFTY"
        option_type = "PE"
        strike = 24600
        expiry = "2026-08-06"
        quantity = 75
        entry_price = 100.0
        exit_price = 0.0
        reason = "fvg_retest"

    await bridge._handle(_Ev())

    assert calls["paper_fill"] == 0, "must never fabricate a fill when broker is unavailable in live mode"
    assert calls["alerts"] >= 1, "must publish a SYSTEM_EVENT alert instead"


@pytest.mark.asyncio
async def test_paper_mode_still_local_sim_untouched(monkeypatch):
    """mode == 'paper' must still go straight to _paper_fill without ever
    touching resolve_broker_or_alert (no alert, pure local simulation)."""
    from execution_bridge.fvg_bridge import FVGExecutionBridge

    calls = {"paper_fill": 0, "alerts": 0}

    class _FakeBus:
        async def publish(self, topic, event):
            calls["alerts"] += 1

        def subscribe(self, topic):
            class _Q:
                async def get(self):
                    await asyncio.sleep(3600)
            return _Q()

    class _FakeDB:
        def get_bindings_safe_sync(self, client_id):
            return [{
                "binding_id": "zerodha",
                "trading_mode": "paper",
                "terminal_connected": True,
            }]

        def get_deployments_sync(self, client_id):
            return [{
                "binding_id": "zerodha",
                "strategy_name": "fvg",
                "underlying": "NIFTY",
                "is_running": 1,
            }]

    class _FakeRouter:
        _brokers = {}
        _client_db = _FakeDB()

    bridge = FVGExecutionBridge.__new__(FVGExecutionBridge)
    bridge._bus = _FakeBus()
    bridge._router = _FakeRouter()
    bridge._trade_log = None

    async def _fake_paper_fill(ev):
        calls["paper_fill"] += 1
    bridge._paper_fill = _fake_paper_fill

    class _Ev:
        action = "BUY"
        client_id = "gurmeet"
        binding_id = "zerodha"
        underlying = "NIFTY"
        option_type = "PE"
        strike = 24600
        expiry = "2026-08-06"
        quantity = 75
        entry_price = 100.0
        exit_price = 0.0
        reason = "fvg_retest"

    await bridge._handle(_Ev())

    assert calls["paper_fill"] == 1
    assert calls["alerts"] == 0
