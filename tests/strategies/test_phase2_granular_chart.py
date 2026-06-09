"""Phase 2b/2c — granular exit-audit client gate + premium chart series getter."""
from strategies.sell_straddle import SellStraddleStrategy
from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus


class _FakeDB:
    def __init__(self, granular): self._g = granular
    def get_all_clients_sync(self): return [{"client_id": "c1"}]
    def get_bindings_safe_sync(self, cid):
        return [{"binding_id": "b1", "show_granular_ticks": self._g}]
    def get_deployments_sync(self, cid):
        return [{"strategy_name": "sell_straddle", "underlying": "NIFTY", "binding_id": "b1"}]


def _ss():
    return SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")


def test_granular_audit_clients_empty_without_db():
    # Fail-closed: no DB ⇒ no audit stream (suppress payload entirely).
    assert _ss()._granular_audit_clients() == []


def test_granular_audit_clients_off():
    ss = _ss()
    ss.set_client_db(_FakeDB(granular=0))
    assert ss._granular_audit_clients() == []


def test_granular_audit_clients_on():
    ss = _ss()
    ss.set_client_db(_FakeDB(granular=1))
    ss._gran_check_t = 0.0  # bypass 5s cache
    assert ss._granular_audit_clients() == [("c1", "b1")]


def test_premium_series_getter_starts_empty():
    ss = _ss()
    assert ss.get_premium_series() == []


def test_premium_series_records_and_clears():
    ss = _ss()
    ss._chart_series.append({"ts": 1.0, "combined": 200.0, "ce_ltp": 100.0,
                             "pe_ltp": 100.0, "vwap": 199.0, "rsi": 50.0, "slope": -0.5})
    series = ss.get_premium_series()
    assert len(series) == 1 and series[0]["combined"] == 200.0
    # getter returns a copy — caller mutation must not corrupt internal buffer
    series.append({})
    assert len(ss.get_premium_series()) == 1
    ss.reset_session()
    assert ss.get_premium_series() == []
