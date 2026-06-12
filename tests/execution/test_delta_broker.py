"""DeltaBroker — deterministic HMAC signing (matches Delta India spec) + product/chain parsing."""
import hashlib
import hmac

from execution_bridge.broker_delta import delta_signature, DeltaBroker


def test_signature_matches_spec():
    # signature = hex( HMAC_SHA256(secret, method + ts + path + query + payload) )
    secret, method, ts, path = "s3cr3t", "GET", "1700000000", "/v2/wallet/balances"
    expect = hmac.new(secret.encode(), (method + ts + path + "" + "").encode(), hashlib.sha256).hexdigest()
    assert delta_signature(secret, method, path, ts) == expect


def test_signature_with_payload():
    secret, ts, path = "abc", "1700000001", "/v2/orders"
    body = '{"product_id": 12456, "size": 1, "side": "buy"}'
    expect = hmac.new(secret.encode(), ("POST" + ts + path + "" + body).encode(), hashlib.sha256).hexdigest()
    assert delta_signature(secret, "POST", path, ts, "", body) == expect


class _Binding:
    binding_id = "delta_01"; api_key = "k"; api_secret = "s"; source_ip = ""; trading_mode = "live"


def test_discover_chain_from_products():
    b = DeltaBroker(_Binding(), "cli")
    # Inject a fake products map mirroring the real Delta India shape (non-uniform BTC steps).
    b._products = {
        "C-BTC-60000-130626": {"underlying_asset": {"symbol": "BTC"}, "strike_price": "60000",
                                "settlement_time": "2026-06-13T12:00:00Z", "tick_size": "0.1",
                                "contract_value": "0.001"},
        "C-BTC-60200-130626": {"underlying_asset": {"symbol": "BTC"}, "strike_price": "60200",
                                "settlement_time": "2026-06-13T12:00:00Z"},
        "C-BTC-60600-130626": {"underlying_asset": {"symbol": "BTC"}, "strike_price": "60600",
                                "settlement_time": "2026-06-13T12:00:00Z"},
        "C-ETH-3000-130626":  {"underlying_asset": {"symbol": "ETH"}, "strike_price": "3000",
                                "settlement_time": "2026-06-13T12:00:00Z"},
    }
    chain = b.discover_chain("BTC")
    exp = chain["expiries"]["2026-06-13"]
    assert exp["strikes"] == [60000, 60200, 60600]
    assert exp["steps"] == [200, 400]          # non-uniform — discovered, not hardcoded
    assert exp["min_step"] == 200
    assert chain["tick_size"] == "0.1"
