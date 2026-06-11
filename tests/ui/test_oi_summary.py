"""PCR + Max-OI summary computed from the WS option cache (pool strikes, zero extra feed)."""
from ui_layer.ws_bridge import WsBridge
from data_layer.base_feeder import EventBus


def _bridge():
    return WsBridge(EventBus())


def test_pcr_and_max_oi():
    b = _bridge()
    b._option_cache = {
        "NIFTY_23000": {"strike": 23000, "call_oi": 100, "put_oi": 500},
        "NIFTY_23100": {"strike": 23100, "call_oi": 300, "put_oi": 400},
        "NIFTY_23200": {"strike": 23200, "call_oi": 900, "put_oi": 100},
    }
    s = b.oi_summary()["NIFTY"]
    assert s["total_ce_oi"] == 1300 and s["total_pe_oi"] == 1000
    assert s["pcr"] == round(1000 / 1300, 2)           # ΣPE / ΣCE
    assert s["max_ce_strike"] == 23200 and s["max_ce_oi"] == 900   # resistance
    assert s["max_pe_strike"] == 23000 and s["max_pe_oi"] == 500   # support
    assert s["strikes"] == 3


def test_zero_ce_oi_safe():
    b = _bridge()
    b._option_cache = {"NIFTY_23000": {"strike": 23000, "call_oi": 0, "put_oi": 50}}
    assert b.oi_summary()["NIFTY"]["pcr"] == 0.0


def test_multi_underlying_separation():
    b = _bridge()
    b._option_cache = {
        "NIFTY_23000": {"strike": 23000, "call_oi": 100, "put_oi": 100},
        "BANKNIFTY_50000": {"strike": 50000, "call_oi": 200, "put_oi": 600},
    }
    out = b.oi_summary()
    assert out["NIFTY"]["pcr"] == 1.0
    assert out["BANKNIFTY"]["pcr"] == 3.0
