"""
tests/strategies/test_gate_can_trade.py — strategies/core/gate.py::can_trade().

can_trade() is the ONE shared entry gate every strategy bridge/manager is meant
to call before routing an ENTRY order. It must require ALL of:
  - a matching binding exists
  - binding.terminal_connected
  - binding.is_trade_enabled
  - a deployment for this exact (binding_id, strategy_name, underlying) with
    is_running == 1

`binding.engine_active` is deliberately NOT part of this contract (see
strategies/core/gate.py::_evaluate() docstring, 2026-08-05 entry): no
currently-reachable dashboard control sets it True for a real trading
binding — the per-broker "Trade" toggle that used to drive it was removed
2026-06-11 in favor of per-strategy Run toggles, and a real production DB
snapshot confirms `engine_active=0` on every binding, including actively
trading ones. The default fixture below mirrors that real-world state
(`engine_active: False`) precisely so a regression that re-adds an
engine_active requirement gets caught here.
"""
from strategies.core.gate import can_trade, _cache


class _DB:
    def __init__(self, **binding_overrides):
        self._binding = {
            "binding_id": "B1",
            "terminal_connected": True,
            # Matches real production state (data/clients.db.bak_20260701_085629):
            # engine_active is 0 on every binding, including live-trading ones.
            "engine_active": False,
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


def test_engine_inactive_but_everything_else_fine_still_true():
    """Regression guard (2026-08-05 fix round 1): engine_active=0 is the REAL
    production state for every binding today (no reachable UI control ever
    sets it True). can_trade() must NOT require it — requiring it would
    silently block every live ENTRY for every strategy the moment this
    shipped. This is the exact scenario code review found in a real DB
    backup: terminal_connected=1, is_trade_enabled=1, engine_active=0, and
    the deployment running — must pass."""
    _clear_cache()
    db = _DB(engine_active=False)
    assert can_trade("C1", "B1", db, "sell_straddle", "NIFTY") is True


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
