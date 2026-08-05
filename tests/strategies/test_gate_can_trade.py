"""
tests/strategies/test_gate_can_trade.py — strategies/core/gate.py::can_trade().

can_trade() is the ONE shared entry gate every strategy bridge/manager is meant
to call before routing an ENTRY order. It must require ALL of:
  - a matching binding exists
  - binding.terminal_connected
  - binding.engine_active   (the "Trading Engine" toggle the live bridges gate on)
  - binding.is_trade_enabled (the "Trade" toggle; kept in lockstep with
    engine_active by the dashboard's set_trade endpoint, but independently
    settable via the separate engine-start/engine-stop endpoints -- so both
    must be checked, neither implies the other)
  - a deployment for this exact (binding_id, strategy_name, underlying) with
    is_running == 1
"""
from strategies.core.gate import can_trade, _cache


class _DB:
    def __init__(self, **binding_overrides):
        self._binding = {
            "binding_id": "B1",
            "terminal_connected": True,
            "engine_active": True,
            "is_trade_enabled": True,
        }
        self._binding.update(binding_overrides)
        self._deployments = [{
            "binding_id": "B1",
            "strategy_name": "sell_straddle",
            "underlying": "NIFTY",
            "is_running": 1,
        }]

    def get_bindings_safe_sync(self, cid):
        return [self._binding]

    def get_deployments_sync(self, cid):
        return self._deployments


def _clear_cache():
    _cache.clear()


def test_all_conditions_met_true():
    _clear_cache()
    db = _DB()
    assert can_trade("C1", "B1", db, "sell_straddle", "NIFTY") is True


def test_terminal_disconnected_false():
    _clear_cache()
    db = _DB(terminal_connected=False)
    assert can_trade("C1", "B1", db, "sell_straddle", "NIFTY") is False


def test_engine_inactive_false():
    _clear_cache()
    db = _DB(engine_active=False)
    assert can_trade("C1", "B1", db, "sell_straddle", "NIFTY") is False


def test_trade_disabled_false():
    _clear_cache()
    db = _DB(is_trade_enabled=False)
    assert can_trade("C1", "B1", db, "sell_straddle", "NIFTY") is False


def test_no_running_deployment_false():
    _clear_cache()
    db = _DB()
    db._deployments = []
    assert can_trade("C1", "B1", db, "sell_straddle", "NIFTY") is False


def test_deployment_wrong_strategy_false():
    _clear_cache()
    db = _DB()
    db._deployments[0]["strategy_name"] = "fvg"
    assert can_trade("C1", "B1", db, "sell_straddle", "NIFTY") is False


def test_deployment_not_running_false():
    _clear_cache()
    db = _DB()
    db._deployments[0]["is_running"] = 0
    assert can_trade("C1", "B1", db, "sell_straddle", "NIFTY") is False


def test_no_client_db_fails_open_true():
    _clear_cache()
    assert can_trade("C1", "B1", None, "sell_straddle", "NIFTY") is True
