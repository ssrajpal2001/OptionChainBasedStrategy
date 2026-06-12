"""Per-binding book identity: own persist key, orders stamped with client/binding, and gating on
ONLY this binding's Terminal+Trade (independent of other clients)."""
import asyncio
from strategies.sell_straddle import SellStraddleStrategy
from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from execution_bridge.straddle_bridge import StraddleOrderEvent


def _ss(client_id="", binding_id=""):
    return SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY",
                                client_id=client_id, binding_id=binding_id)


def test_persist_key_per_binding():
    assert _ss()._persist_key == "NIFTY_sell_straddle"            # property
    assert _ss("C1", "Z1")._persist_key == "C1_Z1_NIFTY_sell_straddle"


def test_emit_order_stamps_identity():
    s = _ss("C1", "Z1")
    ev = StraddleOrderEvent(action="ENTRY", underlying="NIFTY", atm=0, ce_strike=0,
                            pe_strike=0, ce_ltp=1, pe_ltp=1)
    asyncio.run(s._emit_order(ev))
    assert ev.client_id == "C1" and ev.binding_id == "Z1"


class _DB:
    """C1/Z1 active+deployed; C2/Z9 also active but is a DIFFERENT binding."""
    def get_all_clients_sync(self): return [{"client_id": "C1"}, {"client_id": "C2"}]
    def get_bindings_safe_sync(self, cid):
        return [{"binding_id": ("Z1" if cid == "C1" else "Z9"),
                 "terminal_connected": True}]
    def get_deployments_sync(self, cid):
        return [{"strategy_name": "sell_straddle", "underlying": "NIFTY",
                 "binding_id": ("Z1" if cid == "C1" else "Z9"), "is_running": 1}]


def test_gate_is_own_binding_only():
    s = _ss("C1", "Z1"); s.set_client_db(_DB())
    assert s._any_active_terminal() is True            # own binding active

    # A book whose own binding is NOT active stays OFF even though C1/Z1 is active.
    s2 = _ss("C3", "Z3"); s2.set_client_db(_DB())
    assert s2._any_active_terminal() is False           # no matching active binding → independent
