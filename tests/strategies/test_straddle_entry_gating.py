from strategies.sell_straddle import SellStraddleStrategy
from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus

class _FakeDB:
    def __init__(self, active): self._active = active
    def get_all_clients_sync(self): return [{"client_id": "c1"}]
    def get_bindings_safe_sync(self, cid):
        return [{"binding_id": "b1", "engine_active": self._active, "terminal_connected": self._active}]
    def get_deployments_sync(self, cid):
        return [{"strategy_name": "sell_straddle", "underlying": "NIFTY", "binding_id": "b1"}]

def test_gate_open_when_no_db():
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    assert ss._any_active_terminal() is True   # no DB wired -> fail open

def test_gate_blocks_when_inactive():
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    ss.set_client_db(_FakeDB(active=0))
    assert ss._any_active_terminal() is False

def test_gate_allows_when_active():
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    ss.set_client_db(_FakeDB(active=1))
    # bypass the 5s cache from any prior call
    ss._term_check_t = 0.0
    assert ss._any_active_terminal() is True
