"""Generic per-binding egress source-IP binding (SEBI static-IP, all brokers)."""
import requests
from execution_bridge.ip_bind import bind_source_ip, make_source_ip_adapter


def _adapter_source(sess):
    a = sess.get_adapter("https://api.example.com")
    return a.poolmanager.connection_pool_kw.get("source_address")


def test_binds_session_attr():
    class _SDK:  # mimics kiteconnect: keeps a requests.Session on .reqsession
        def __init__(self): self.reqsession = requests.Session()
    sdk = _SDK()
    assert bind_source_ip(sdk, "172.31.16.130") is True
    assert _adapter_source(sdk.reqsession) == ("172.31.16.130", 0)


def test_binds_session_object_itself():
    s = requests.Session()
    assert bind_source_ip(s, "172.31.29.159") is True
    assert _adapter_source(s) == ("172.31.29.159", 0)


def test_no_source_ip_is_noop():
    assert bind_source_ip(requests.Session(), "") is False


def test_creates_session_when_absent():
    class _Bare:  # no session attr
        pass
    sdk = _Bare()
    assert bind_source_ip(sdk, "10.0.0.5") is True
    assert isinstance(sdk.reqsession, requests.Session)
    assert _adapter_source(sdk.reqsession) == ("10.0.0.5", 0)


def test_adapter_factory():
    a = make_source_ip_adapter("172.31.0.1")
    assert a.poolmanager.connection_pool_kw.get("source_address") == ("172.31.0.1", 0)
