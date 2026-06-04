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
