"""
Tests for AngelOneFeeder (data_layer/global_feeder.py) -- the free, genuinely
headless (no browser/OAuth/Cloudflare dependency) candidate data feeder built
to replace Fyers, whose own headless login is confirmed non-viable (Cloudflare
Turnstile -- see broker_auth/headless_totp_auth_fyers.py's own docstring).

No live SmartAPI connection is required -- SmartConnect/WebSocket calls are
mocked throughout.
"""
import asyncio
from datetime import date

import pytest

from config.global_config import Topic
from data_layer.base_feeder import EventBus
from data_layer.global_feeder import AngelOneFeeder, _ANGELONE_INDEX_TOKENS


@pytest.fixture
def bus():
    return EventBus()


@pytest.fixture
def feeder(bus):
    f = AngelOneFeeder(bus)
    f._smartapi = object()  # non-None sentinel; real calls are monkeypatched per test
    return f


# ── _resolve_option_token ──────────────────────────────────────────────────

def test_resolve_option_token_finds_and_caches_symboltoken(feeder, monkeypatch):
    calls = {"n": 0}

    def fake_search_scrip(exchange, tradingsymbol):
        calls["n"] += 1
        assert exchange == "NFO"
        return {"status": True, "data": [
            {"tradingsymbol": tradingsymbol, "symboltoken": "58809"},
        ]}

    feeder._smartapi = type("FakeSmartApi", (), {"searchScrip": staticmethod(fake_search_scrip)})()

    token = asyncio.run(feeder._resolve_option_token("NIFTY", 24000.0, "CE", date(2026, 9, 25)))
    assert token == "58809"
    assert feeder._token_meta["58809"] == ("NIFTY", 24000.0, "CE", date(2026, 9, 25))

    # Second call for the SAME contract must hit the cache, not searchScrip again.
    token2 = asyncio.run(feeder._resolve_option_token("NIFTY", 24000.0, "CE", date(2026, 9, 25)))
    assert token2 == "58809"
    assert calls["n"] == 1


def test_resolve_option_token_uses_bfo_for_sensex(feeder, monkeypatch):
    seen_exchange = {}

    def fake_search_scrip(exchange, tradingsymbol):
        seen_exchange["exchange"] = exchange
        return {"status": True, "data": [{"tradingsymbol": tradingsymbol, "symboltoken": "1"}]}

    feeder._smartapi = type("FakeSmartApi", (), {"searchScrip": staticmethod(fake_search_scrip)})()
    asyncio.run(feeder._resolve_option_token("SENSEX", 81000.0, "PE", date(2026, 9, 30)))
    assert seen_exchange["exchange"] == "BFO"


def test_resolve_option_token_returns_none_when_not_found(feeder):
    feeder._smartapi = type("FakeSmartApi", (), {
        "searchScrip": staticmethod(lambda exchange, ts: {"status": True, "data": []})
    })()
    token = asyncio.run(feeder._resolve_option_token("NIFTY", 24000.0, "CE", date(2026, 9, 25)))
    assert token is None
    assert feeder._token_meta == {}


def test_resolve_option_token_none_smartapi_returns_none(bus):
    f = AngelOneFeeder(bus)
    assert f._smartapi is None
    assert asyncio.run(f._resolve_option_token("NIFTY", 24000.0, "CE", date(2026, 9, 25))) is None


# ── _resolve_any_token (cross-format acceptance) ───────────────────────────

def test_resolve_any_token_accepts_upstox_key(feeder, monkeypatch):
    from data_layer.instrument_registry import REGISTRY
    monkeypatch.setattr(REGISTRY, "_upstox_keys", {
        "NIFTY": {("2026-09-25", 24000.0, "CE"): "NSE_FO|12345"},
    })
    feeder._smartapi = type("FakeSmartApi", (), {
        "searchScrip": staticmethod(lambda exchange, ts: {"status": True, "data": [
            {"tradingsymbol": ts, "symboltoken": "999"}]})
    })()
    resolved = asyncio.run(feeder._resolve_any_token("NSE_FO|12345"))
    assert resolved == ("999", 2)  # exchange_type 2 = nse_fo


def test_resolve_any_token_accepts_fyers_symbol(feeder, monkeypatch):
    from data_layer.symbol_translator import SymbolTranslator, InternalSymbol
    internal = InternalSymbol(underlying="NIFTY", strike=24000.0, option_type="CE", expiry=date(2026, 9, 25))
    fyers_sym = "NSE:" + SymbolTranslator.to_fyers(internal)
    feeder._smartapi = type("FakeSmartApi", (), {
        "searchScrip": staticmethod(lambda exchange, ts: {"status": True, "data": [
            {"tradingsymbol": ts, "symboltoken": "777"}]})
    })()
    resolved = asyncio.run(feeder._resolve_any_token(fyers_sym))
    assert resolved == ("777", 2)


def test_resolve_any_token_sensex_gets_bfo_exchange_type(feeder):
    from data_layer.symbol_translator import SymbolTranslator, InternalSymbol
    internal = InternalSymbol(underlying="SENSEX", strike=81000.0, option_type="PE", expiry=date(2026, 9, 30))
    fyers_sym = "BSE:" + SymbolTranslator.to_fyers(internal, is_monthly=False)
    feeder._smartapi = type("FakeSmartApi", (), {
        "searchScrip": staticmethod(lambda exchange, ts: {"status": True, "data": [
            {"tradingsymbol": ts, "symboltoken": "42"}]})
    })()
    resolved = asyncio.run(feeder._resolve_any_token(fyers_sym))
    assert resolved == ("42", 4)  # exchange_type 4 = bse_fo


def test_resolve_any_token_unrecognized_format_returns_none(feeder):
    assert asyncio.run(feeder._resolve_any_token("totally-unrecognized-garbage")) is None


# ── subscribe_tokens / unsubscribe_tokens (state-tracking regression) ──────

def test_subscribe_tokens_does_not_resubscribe_already_subscribed(feeder, monkeypatch):
    """Real bug caught before shipping: an earlier version mutated
    self._subscribed as a side effect of RESOLUTION, making the "already
    subscribed" check in subscribe_tokens always false. Resolution must be
    pure -- only subscribe_tokens/unsubscribe_tokens may touch
    self._subscribed."""
    from data_layer.instrument_registry import REGISTRY
    monkeypatch.setattr(REGISTRY, "_upstox_keys", {
        "NIFTY": {("2026-09-25", 24000.0, "CE"): "NSE_FO|12345"},
    })
    feeder._smartapi = type("FakeSmartApi", (), {
        "searchScrip": staticmethod(lambda exchange, ts: {"status": True, "data": [
            {"tradingsymbol": ts, "symboltoken": "999"}]})
    })()

    class FakeSocket:
        def __init__(self):
            self.subscribe_calls = []

        def subscribe(self, correlation_id, mode, token_list):
            self.subscribe_calls.append(token_list)

    feeder._socket = FakeSocket()
    feeder._connected = True

    asyncio.run(feeder.subscribe_tokens(["NSE_FO|12345"]))
    assert len(feeder._socket.subscribe_calls) == 1
    assert feeder._subscribed[2] == {"999"}

    # Subscribing the SAME token again must be a no-op -- no second WS call.
    asyncio.run(feeder.subscribe_tokens(["NSE_FO|12345"]))
    assert len(feeder._socket.subscribe_calls) == 1


def test_unsubscribe_tokens_removes_from_state(feeder, monkeypatch):
    from data_layer.instrument_registry import REGISTRY
    monkeypatch.setattr(REGISTRY, "_upstox_keys", {
        "NIFTY": {("2026-09-25", 24000.0, "CE"): "NSE_FO|12345"},
    })
    feeder._smartapi = type("FakeSmartApi", (), {
        "searchScrip": staticmethod(lambda exchange, ts: {"status": True, "data": [
            {"tradingsymbol": ts, "symboltoken": "999"}]})
    })()

    class FakeSocket:
        def subscribe(self, *a, **k): pass
        def unsubscribe(self, *a, **k): pass

    feeder._socket = FakeSocket()
    feeder._connected = True
    asyncio.run(feeder.subscribe_tokens(["NSE_FO|12345"]))
    assert feeder._subscribed[2] == {"999"}
    asyncio.run(feeder.unsubscribe_tokens(["NSE_FO|12345"]))
    assert feeder._subscribed[2] == set()


# ── _parse_frame ────────────────────────────────────────────────────────────

def test_parse_frame_publishes_index_tick_for_known_token(feeder, bus):
    et, tok = _ANGELONE_INDEX_TOKENS["NIFTY"]
    q = bus.subscribe(Topic.INDEX_TICK)
    asyncio.run(feeder._parse_frame({
        "exchange_type": et, "token": tok,
        "last_traded_price": 2450050,  # paise -> 24500.50
        "open_price_of_the_day": 2440000, "high_price_of_the_day": 2451000,
        "low_price_of_the_day": 2439000, "closed_price": 2438000,
        "volume_trade_for_the_day": 0,
    }))
    tick = q.get_nowait()
    assert tick.symbol == "NIFTY"
    assert tick.ltp == pytest.approx(24500.50)
    assert tick.high == pytest.approx(24510.0)


def test_parse_frame_publishes_option_tick_for_known_meta(feeder, bus):
    feeder._token_meta["999"] = ("NIFTY", 24000.0, "CE", date(2026, 9, 25))
    q = bus.subscribe(Topic.OPTION_TICK)
    asyncio.run(feeder._parse_frame({
        "exchange_type": 2, "token": "999",
        "last_traded_price": 15050,  # paise -> 150.50
        "open_interest": 123456,
        "volume_trade_for_the_day": 5000,
        "average_traded_price": 14900,
    }))
    tick = q.get_nowait()
    assert tick.underlying == "NIFTY"
    assert tick.strike == 24000.0
    assert tick.option_type == "CE"
    assert tick.ltp == pytest.approx(150.50)
    assert tick.oi == 123456
    assert tick.atp == pytest.approx(149.0)


def test_parse_frame_ignores_unknown_token(feeder, bus):
    q = bus.subscribe(Topic.OPTION_TICK)
    asyncio.run(feeder._parse_frame({
        "exchange_type": 2, "token": "unknown-token", "last_traded_price": 100,
    }))
    with pytest.raises(asyncio.QueueEmpty):
        q.get_nowait()


def test_parse_frame_ignores_non_dict(feeder):
    asyncio.run(feeder._parse_frame("not a dict"))  # must not raise


# ── Futures support (2026-09-09, "we require future subscription for angel
#    one that is must") ─────────────────────────────────────────────────────

def test_to_angelone_futures_format():
    from data_layer.symbol_translator import SymbolTranslator
    assert SymbolTranslator.to_angelone_futures("NIFTY", date(2026, 9, 30)) == "NIFTY30SEP26FUT"


def test_resolve_futures_token_finds_and_caches(feeder, monkeypatch):
    from data_layer.instrument_registry import REGISTRY
    monkeypatch.setattr(REGISTRY, "load_futures_only_sync", lambda underlying, today=None: None)
    monkeypatch.setattr(REGISTRY, "get_futures_expiry", lambda underlying: date(2026, 9, 30))
    calls = {"n": 0}

    def fake_search_scrip(exchange, tradingsymbol):
        calls["n"] += 1
        assert exchange == "NFO"
        assert tradingsymbol == "NIFTY30SEP26FUT"
        return {"status": True, "data": [{"tradingsymbol": tradingsymbol, "symboltoken": "54321"}]}

    feeder._smartapi = type("FakeSmartApi", (), {"searchScrip": staticmethod(fake_search_scrip)})()
    token = asyncio.run(feeder._resolve_futures_token("NIFTY"))
    assert token == "54321"
    # Second call must hit the cache, not searchScrip again.
    token2 = asyncio.run(feeder._resolve_futures_token("NIFTY"))
    assert token2 == "54321"
    assert calls["n"] == 1


def test_resolve_futures_token_none_when_expiry_unresolved(feeder, monkeypatch):
    from data_layer.instrument_registry import REGISTRY
    monkeypatch.setattr(REGISTRY, "load_futures_only_sync", lambda underlying, today=None: None)
    monkeypatch.setattr(REGISTRY, "get_futures_expiry", lambda underlying: None)
    assert asyncio.run(feeder._resolve_futures_token("NIFTY")) is None


def test_resolve_futures_token_uses_bfo_for_sensex(feeder, monkeypatch):
    from data_layer.instrument_registry import REGISTRY
    monkeypatch.setattr(REGISTRY, "load_futures_only_sync", lambda underlying, today=None: None)
    monkeypatch.setattr(REGISTRY, "get_futures_expiry", lambda underlying: date(2026, 9, 30))
    seen_exchange = {}

    def fake_search_scrip(exchange, tradingsymbol):
        seen_exchange["exchange"] = exchange
        return {"status": True, "data": [{"tradingsymbol": tradingsymbol, "symboltoken": "1"}]}

    feeder._smartapi = type("FakeSmartApi", (), {"searchScrip": staticmethod(fake_search_scrip)})()
    asyncio.run(feeder._resolve_futures_token("SENSEX"))
    assert seen_exchange["exchange"] == "BFO"


def test_parse_frame_tags_futures_tick_with_source_futures(feeder, bus):
    feeder._futures_token_to_underlying["54321"] = "NIFTY"
    q = bus.subscribe(Topic.INDEX_TICK)
    asyncio.run(feeder._parse_frame({
        "exchange_type": 2, "token": "54321",
        "last_traded_price": 2455000,  # paise -> 24550.00
        "volume_trade_for_the_day": 0,
    }))
    tick = q.get_nowait()
    assert tick.symbol == "NIFTY"
    assert tick.source == "futures"
    assert tick.ltp == pytest.approx(24550.00)


def test_index_subscribe_all_resolves_futures_for_configured_underlyings(feeder, monkeypatch):
    from data_layer.instrument_registry import REGISTRY
    monkeypatch.setattr(REGISTRY, "load_futures_only_sync", lambda underlying, today=None: None)
    monkeypatch.setattr(REGISTRY, "get_futures_expiry", lambda underlying: date(2026, 9, 30))
    feeder._cfg = type("Cfg", (), {"monitored_indices": ["NIFTY"], "futures_atm_underlyings": ["NIFTY"]})()
    feeder._smartapi = type("FakeSmartApi", (), {
        "searchScrip": staticmethod(lambda exchange, ts: {"status": True, "data": [
            {"tradingsymbol": ts, "symboltoken": "54321"}]})
    })()
    asyncio.run(feeder._index_subscribe_all())
    assert feeder._futures_token_to_underlying.get("54321") == "NIFTY"
    assert "54321" in feeder._subscribed.get(2, set())
