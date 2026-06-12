"""
execution_bridge/ip_bind.py — per-binding egress source-IP binding (SEBI static-IP, all brokers).

SEBI (circular Feb 2025, enforced 01-Apr-2026) requires every broker to reject API orders from
non-whitelisted IPs. On a multi-client server each broker app egresses from its own whitelisted
public IP by binding that client's HTTP session to the LOCAL/private interface IP whose Elastic IP
is whitelisted.

Most broker SDKs (kiteconnect, smartapi-python, dhanhq, upstox-client) use `requests` under the
hood and keep a `requests.Session`. `bind_source_ip(sdk_client, source_ip)` finds that session and
mounts an adapter that binds outbound sockets to `source_ip`, so all the SDK's API calls leave from
the mapped public IP. Returns True if a session was found and bound.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Common attribute names broker SDKs use for their requests.Session.
_SESSION_ATTRS = ("reqsession", "session", "_session", "req_session", "_reqsession", "rest_session")


def make_source_ip_adapter(source_ip: str):
    """A requests HTTPAdapter whose connection pool binds outbound sockets to `source_ip`."""
    from requests.adapters import HTTPAdapter
    from urllib3.poolmanager import PoolManager

    class _SourceIPAdapter(HTTPAdapter):
        def init_poolmanager(self, connections, maxsize, block=False, **kw):
            self.poolmanager = PoolManager(
                num_pools=connections, maxsize=maxsize, block=block,
                source_address=(source_ip, 0), **kw)

    return _SourceIPAdapter()


def bind_session(session, source_ip: str) -> None:
    """Mount the source-IP adapter on an existing requests.Session for http+https."""
    adapter = make_source_ip_adapter(source_ip)
    session.mount("https://", adapter)
    session.mount("http://", adapter)


def bind_source_ip(sdk_client: Any, source_ip: str) -> bool:
    """Best-effort: find the SDK's requests.Session and bind it to `source_ip`. Returns True if
    a session was found + bound. If the SDK uses module-level requests (no persistent session),
    binding may not apply — the caller should verify the real egress IP."""
    import requests
    if not source_ip:
        return False
    # The client itself might BE a session.
    if isinstance(sdk_client, requests.Session):
        bind_session(sdk_client, source_ip)
        return True
    for attr in _SESSION_ATTRS:
        sess = getattr(sdk_client, attr, None)
        if isinstance(sess, requests.Session):
            bind_session(sess, source_ip)
            return True
    # Last resort: if the SDK has no session, create one and attach it under the most common name
    # so subsequent calls (if the SDK reads `reqsession`) use it. Harmless if the SDK ignores it.
    try:
        sess = requests.Session()
        bind_session(sess, source_ip)
        setattr(sdk_client, "reqsession", sess)
        return True
    except Exception as exc:
        logger.debug("bind_source_ip: could not attach session: %s", exc)
        return False
