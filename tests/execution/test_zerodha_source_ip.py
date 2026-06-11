"""Per-binding source-IP binding for Zerodha (multi-client static-IP whitelisting).
Each client egresses from their own IP so Kite's globally-unique static IP works."""
import requests
from execution_bridge.broker_zerodha import _bind_session_source_ip


class _FakeKite:
    def __init__(self):
        self.reqsession = requests.Session()


def test_binds_source_ip_adapter_onto_session():
    k = _FakeKite()
    _bind_session_source_ip(k, "172.31.16.130")
    ad = k.reqsession.get_adapter("https://api.kite.trade")
    # The mounted adapter must carry our source address into its pool manager.
    pm = ad.init_poolmanager(1, 1)  # returns None but sets self.poolmanager
    assert ad.poolmanager.connection_pool_kw.get("source_address") == ("172.31.16.130", 0)


def test_creates_session_if_missing():
    class _NoSession:
        pass
    k = _NoSession()
    _bind_session_source_ip(k, "10.0.0.5")
    assert isinstance(k.reqsession, requests.Session)
    ad = k.reqsession.get_adapter("https://x")
    ad.init_poolmanager(1, 1)
    assert ad.poolmanager.connection_pool_kw.get("source_address") == ("10.0.0.5", 0)
