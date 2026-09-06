"""
Tests for cross-provider token translation and Fyers access-token normalization.
No live broker connection is required.
"""

import asyncio

import pytest

from data_layer.base_feeder import EventBus
from data_layer.global_feeder import FyersFeeder, UpstoxFeeder
from data_layer.instrument_registry import REGISTRY


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
    assert not f._is_fyers_symbol("NSE_FO|12345")
    assert not f._is_fyers_symbol("NIFTY:02JUN26:24500:CE")


# ── futures_atm_underlyings (2026-08-26, direct user spec) ─────────────────
# Lets any configured underlying (e.g. NIFTY) source its ATM input from the
# near-month futures price alongside real spot. 2026-08-26 revision: SellStraddle now
# wants BOTH the real spot AND the futures price simultaneously (to compute
# their mean for ATM), not futures-instead-of-spot -- so both keys are
# subscribed together for a futures_atm underlying.

def test_upstox_subscribes_both_spot_and_futures_key_for_configured_underlying(bus):
    from config.global_config import GlobalConfig
    from data_layer.symbol_translator import SymbolTranslator
    cfg = GlobalConfig()
    cfg.monitored_indices = ["NIFTY"]
    cfg.futures_atm_underlyings = ["NIFTY"]
    u = UpstoxFeeder(bus, cfg=cfg)
    REGISTRY._futures_upstox["NIFTY"] = "NSE_FO|999999"
    try:
        keys = u._index_instrument_keys()
        assert keys == [SymbolTranslator.to_upstox_index("NIFTY"), "NSE_FO|999999"]
    finally:
        REGISTRY._futures_upstox.pop("NIFTY", None)


def test_upstox_subscribes_spot_only_when_futures_key_not_yet_resolved(bus):
    """Safety fallback: a startup-ordering race (REGISTRY hasn't resolved the
    futures key yet) must never leave the underlying with ZERO subscription --
    subscribes to spot alone for that cycle instead of going dark."""
    from config.global_config import GlobalConfig
    from data_layer.symbol_translator import SymbolTranslator
    cfg = GlobalConfig()
    cfg.monitored_indices = ["NIFTY"]
    cfg.futures_atm_underlyings = ["NIFTY"]
    u = UpstoxFeeder(bus, cfg=cfg)
    REGISTRY._futures_upstox.pop("NIFTY", None)   # ensure genuinely unresolved
    keys = u._index_instrument_keys()
    assert keys == [SymbolTranslator.to_upstox_index("NIFTY")]


def test_fyers_subscribes_both_spot_and_futures_symbol_for_configured_underlying(bus):
    from config.global_config import GlobalConfig
    cfg = GlobalConfig()
    cfg.monitored_indices = ["NIFTY"]
    cfg.futures_atm_underlyings = ["NIFTY"]
    f = FyersFeeder(bus, cfg=cfg)
    REGISTRY._futures_fyers["NIFTY"] = "NSE:NIFTY26AUGFUT"
    try:
        syms = f._index_symbols()
        assert syms == ["NSE:NIFTY50-INDEX", "NSE:NIFTY26AUGFUT"]
    finally:
        REGISTRY._futures_fyers.pop("NIFTY", None)


def test_fyers_subscribes_spot_only_when_futures_symbol_not_yet_resolved(bus):
    from config.global_config import GlobalConfig
    cfg = GlobalConfig()
    cfg.monitored_indices = ["NIFTY"]
    cfg.futures_atm_underlyings = ["NIFTY"]
    f = FyersFeeder(bus, cfg=cfg)
    REGISTRY._futures_fyers.pop("NIFTY", None)
    syms = f._index_symbols()
    assert syms == ["NSE:NIFTY50-INDEX"]


def test_futures_tick_maps_back_to_internal_underlying_for_any_futures_atm_underlying():
    """The reverse-mapping helpers scan every underlying REGISTRY has resolved
    a futures key for -- a NIFTY futures tick must resolve back to 'NIFTY'."""
    from data_layer.global_feeder import _upstox_fut_to_internal, _fyers_fut_to_internal
    REGISTRY._futures_upstox["NIFTY"] = "NSE_FO|999999"
    REGISTRY._futures_fyers["NIFTY"] = "NSE:NIFTY26AUGFUT"
    try:
        assert _upstox_fut_to_internal("NSE_FO|999999") == "NIFTY"
        assert _fyers_fut_to_internal("NSE:NIFTY26AUGFUT") == "NIFTY"
        assert _upstox_fut_to_internal("NSE_FO|000000") is None
        assert _fyers_fut_to_internal("") is None
    finally:
        REGISTRY._futures_upstox.pop("NIFTY", None)
        REGISTRY._futures_fyers.pop("NIFTY", None)


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
