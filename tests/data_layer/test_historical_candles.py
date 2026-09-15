import asyncio
import data_layer.historical_candles as hc

def test_holiday_step_back(monkeypatch):
    calls = {"n": 0}
    def fake_get(d):
        calls["n"] += 1
        # first two days empty (holiday), third returns one candle
        if calls["n"] < 3:
            return []
        return [{"ts": "t", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 100}]
    # patch the inner thread call by patching asyncio.to_thread to call fake_get directly
    async def fake_to_thread(fn, *a, **k):
        return fake_get(*a, **k)
    monkeypatch.setattr(hc.asyncio, "to_thread", fake_to_thread)
    rows = asyncio.run(hc.fetch_upstox_1m("KEY", "TOKEN"))
    assert calls["n"] == 3 and len(rows) == 1

def test_returns_empty_after_max_step_back(monkeypatch):
    async def fake_to_thread(fn, *a, **k):
        return []
    monkeypatch.setattr(hc.asyncio, "to_thread", fake_to_thread)
    rows = asyncio.run(hc.fetch_upstox_1m("KEY", "TOKEN", max_step_back=3))
    assert rows == []


def _resp(n, base):
    # Upstox returns newest-first; n candles, distinguishable close values.
    return {"data": {"candles": [["t%d" % (base + i), 1, 2, 0.5, base + i, 100]
                                 for i in reversed(range(n))]}}


def test_intraday_returns_today_bars(monkeypatch):
    def fake_http(url, token):
        assert "intraday" in url
        return _resp(3, 100)
    monkeypatch.setattr(hc, "_http_get_json", fake_http)
    rows = asyncio.run(hc.fetch_upstox_intraday_1m("KEY", "TOKEN"))
    assert len(rows) == 3
    # oldest-first
    assert [r["close"] for r in rows] == [100, 101, 102]


def test_warm_today_enough_no_prevday(monkeypatch):
    calls = {"intraday": 0, "hist": 0}
    def fake_http(url, token):
        if "intraday" in url:
            calls["intraday"] += 1
            return _resp(20, 200)
        calls["hist"] += 1
        return _resp(50, 0)
    monkeypatch.setattr(hc, "_http_get_json", fake_http)
    rows = asyncio.run(hc.fetch_upstox_warm_1m("KEY", "TOKEN", min_bars=15))
    assert len(rows) == 20
    assert calls["intraday"] == 1 and calls["hist"] == 0


def test_warm_today_short_prepends_prevday(monkeypatch):
    def fake_http(url, token):
        if "intraday" in url:
            return _resp(3, 200)   # today: closes 200,201,202
        return _resp(5, 0)         # prev-day: closes 0..4
    monkeypatch.setattr(hc, "_http_get_json", fake_http)
    rows = asyncio.run(hc.fetch_upstox_warm_1m("KEY", "TOKEN", min_bars=15))
    closes = [r["close"] for r in rows]
    # prev-day (older) first, then today
    assert closes == [0, 1, 2, 3, 4, 200, 201, 202]


# ── fetch_upstox_v3_quote (2026-09-16, OI-ORB futures-OI-regime gate) ──────

def test_fetch_upstox_v3_quote_returns_single_instrument_dict(monkeypatch):
    def fake_http(url, token):
        assert "v3/market-quote/quotes" in url
        assert "instrument_key=" in url
        return {"status": "success", "data": {
            "NSE_FO:MPHASIS26SEPFUT": {"oi": 5544000.0, "previous_oi": 5300075.0,
                                        "last_price": 2374.0},
        }}
    monkeypatch.setattr(hc, "_http_get_json", fake_http)
    row = asyncio.run(hc.fetch_upstox_v3_quote("NSE_FO|68736", "TOKEN"))
    assert row["oi"] == 5544000.0
    assert row["previous_oi"] == 5300075.0


def test_fetch_upstox_v3_quote_returns_none_on_non_success_status(monkeypatch):
    monkeypatch.setattr(hc, "_http_get_json", lambda url, token: {"status": "error"})
    row = asyncio.run(hc.fetch_upstox_v3_quote("NSE_FO|68736", "TOKEN"))
    assert row is None


def test_fetch_upstox_v3_quote_returns_none_on_empty_response(monkeypatch):
    monkeypatch.setattr(hc, "_http_get_json", lambda url, token: {})
    row = asyncio.run(hc.fetch_upstox_v3_quote("NSE_FO|68736", "TOKEN"))
    assert row is None


def test_fetch_upstox_v3_quote_returns_none_on_empty_data(monkeypatch):
    monkeypatch.setattr(hc, "_http_get_json",
                         lambda url, token: {"status": "success", "data": {}})
    row = asyncio.run(hc.fetch_upstox_v3_quote("NSE_FO|68736", "TOKEN"))
    assert row is None
