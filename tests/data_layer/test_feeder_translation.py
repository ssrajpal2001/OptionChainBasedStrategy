"""
Tests for cross-provider token translation and Fyers access-token normalization.
No live broker connection is required.
"""

import asyncio

import pytest
from datetime import date

from data_layer.base_feeder import EventBus
from data_layer.global_feeder import FyersFeeder, UpstoxFeeder
from data_layer.instrument_registry import REGISTRY, _MCX_UNDERLYINGS


@pytest.fixture
def bus():
    return EventBus()


def test_fyers_normalizes_prefixed_access_token(bus):
    f = FyersFeeder(bus)
    jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
    assert f._normalize_access_token(jwt) == jwt
    assert f._normalize_access_token(f"APPID-100:{jwt}") == jwt
    assert f._normalize_access_token("") == ""


def test_fyers_futures_symbol_recognized(bus):
    f = FyersFeeder(bus)
    assert f._is_fyers_symbol("NSE:NIFTY50-INDEX")
    assert f._is_fyers_symbol("MCX:CRUDEOIL26JUNFUT")
    assert f._is_fyers_symbol("MCX:CRUDEOIL26JUN7000CE")
    assert not f._is_fyers_symbol("NSE_FO|12345")
    assert not f._is_fyers_symbol("NIFTY:02JUN26:24500:CE")


def test_fyers_converts_upstox_mcx_option_key(bus):
    f = FyersFeeder(bus)
    # Pretend the registry knows this MCX_FO key.
    exp = date(2026, 7, 20)
    REGISTRY._upstox_keys.setdefault("CRUDEOIL", {})[(exp.isoformat(), 7000, "CE")] = "MCX_FO|123456"
    try:
        sym = f._to_fyers_symbol("MCX_FO|123456")
        assert sym == "MCX:CRUDEOIL26JUL7000CE"
    finally:
        REGISTRY._upstox_keys["CRUDEOIL"].pop((exp.isoformat(), 7000, "CE"), None)


def test_upstox_converts_fyers_mcx_option_symbol(bus):
    u = UpstoxFeeder(bus)
    exp = date(2026, 7, 20)
    REGISTRY._upstox_keys.setdefault("CRUDEOIL", {})[(exp.isoformat(), 7000, "CE")] = "MCX_FO|123456"
    REGISTRY._expiries.setdefault("CRUDEOIL", [])
    if exp not in REGISTRY._expiries["CRUDEOIL"]:
        REGISTRY._expiries["CRUDEOIL"].append(exp)
    try:
        key = u._to_upstox_key("MCX:CRUDEOIL26JUL7000CE")
        assert key == "MCX_FO|123456"
    finally:
        REGISTRY._upstox_keys["CRUDEOIL"].pop((exp.isoformat(), 7000, "CE"), None)
        if exp in REGISTRY._expiries["CRUDEOIL"]:
            REGISTRY._expiries["CRUDEOIL"].remove(exp)


def test_mcx_underlyings_include_crudeoil():
    assert "CRUDEOIL" in _MCX_UNDERLYINGS


def test_upstox_subscribe_tokens_dedupes_dual_format_same_leg(bus, monkeypatch):
    """2026-07-24 real production bug: strike_rebalancer._strikes_to_tokens()
    deliberately sends BOTH a native Upstox instrument_key and a Fyers-format
    symbol for every leg in a single subscribe_tokens() call (each feeder in a
    dual-active pair filters to its own format). Converting the Fyers-format
    token back to Upstox produces the SAME key as the native one -- so the old
    dedup (checked only against _subscribed_keys from PRIOR calls, never within
    the current call) appended every leg twice, inflating the ~50/connection WS
    limit warning 2x on every fresh subscribe. Confirmed live: every option key
    subscribed exactly twice, 74 "subscribed" vs 38 truly distinct symbols.

    Stubs _to_upstox_key directly (rather than round-tripping through the real
    registry/date-based symbol parser) to isolate the dedup behavior under test
    from unrelated expiry-resolution machinery."""
    u = UpstoxFeeder(bus)
    monkeypatch.setattr(u, "_to_upstox_key", lambda t: "NSE_FO|63915")
    asyncio.run(u.subscribe_tokens(["NSE_FO|63915", "NSE:NIFTY26JUL23500CE"]))
    assert u._subscribed_keys == ["NSE_FO|63915"]
