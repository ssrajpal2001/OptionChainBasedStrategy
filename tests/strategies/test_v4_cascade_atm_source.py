"""CRUDEOIL's ATM must be sourced from the futures instrument key
(historical_instrument_key), not the spot-index key (get_upstox_index_key,
which has no valid entry for MCX underlyings and would resolve to a bogus
NSE_INDEX|CRUDEOIL key)."""
from data_layer.instrument_registry import REGISTRY


def test_historical_instrument_key_prefers_futures_for_mcx(monkeypatch):
    monkeypatch.setitem(REGISTRY._futures_upstox, "CRUDEOIL", "MCX_FO|499095")
    assert REGISTRY.historical_instrument_key("CRUDEOIL") == "MCX_FO|499095"


def test_get_upstox_index_key_is_wrong_for_mcx():
    # Documents WHY book.py must not call this for MCX underlyings -- it has
    # no CRUDEOIL entry and falls through to a bogus NSE_INDEX key.
    assert REGISTRY.get_upstox_index_key("CRUDEOIL") == "NSE_INDEX|CRUDEOIL"
