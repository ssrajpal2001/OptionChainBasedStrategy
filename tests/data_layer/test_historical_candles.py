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


# ── fetch_upstox_prev_day_last_tick_oi (2026-09-16, OI-ORB "yday 15:39 tick"
# display -- distinct from previous_oi's NSE-settled EOD OI) ───────────────

def _candles_with_oi(rows):
    """rows: [(ts, close, oi), ...] oldest-first -- returns Upstox's own
    newest-first raw shape (as _parse_candles expects to reverse)."""
    return {"data": {"candles": [[ts, 1, 2, 0.5, close, 100, oi]
                                  for ts, close, oi in reversed(rows)]}}


def test_returns_last_bars_oi_from_the_most_recent_weekday(monkeypatch):
    import datetime as _dt

    class _FixedDateTime(_dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return _dt.datetime(2026, 9, 16, 12, 0, tzinfo=tz)  # a real Wednesday

    monkeypatch.setattr(hc, "datetime", _FixedDateTime)

    def fake_http(url, token):
        assert "2026-09-15" in url
        return _candles_with_oi([
            ("2026-09-15T15:37:00+05:30", 100.0, 1137500),
            ("2026-09-15T15:38:00+05:30", 100.5, 1137600),
            ("2026-09-15T15:39:00+05:30", 101.0, 1137750),
        ])
    monkeypatch.setattr(hc, "_http_get_json", fake_http)

    oi = asyncio.run(hc.fetch_upstox_prev_day_last_tick_oi("NSE_FO|68786", "TOKEN"))
    assert oi == 1137750.0


def test_steps_back_over_a_weekend(monkeypatch):
    import datetime as _dt

    class _FixedDateTime(_dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return _dt.datetime(2026, 9, 14, 12, 0, tzinfo=tz)  # a Monday -> yesterday is Sunday

    monkeypatch.setattr(hc, "datetime", _FixedDateTime)

    calls = []
    def fake_http(url, token):
        calls.append(url)
        # Only Friday 2026-09-11 has real data; Sat/Sun are skipped entirely
        # by fetch_upstox_range_1m's own weekday filter (never even called).
        if "2026-09-11" in url:
            return _candles_with_oi([("2026-09-11T15:39:00+05:30", 50.0, 999000)])
        return {"data": {"candles": []}}
    monkeypatch.setattr(hc, "_http_get_json", fake_http)

    oi = asyncio.run(hc.fetch_upstox_prev_day_last_tick_oi("NSE_FO|68786", "TOKEN"))
    assert oi == 999000.0
    assert all("2026-09-13" not in u and "2026-09-12" not in u for u in calls)


def test_returns_none_when_no_data_within_step_back_window(monkeypatch):
    import datetime as _dt

    class _FixedDateTime(_dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return _dt.datetime(2026, 9, 16, 12, 0, tzinfo=tz)

    monkeypatch.setattr(hc, "datetime", _FixedDateTime)
    monkeypatch.setattr(hc, "_http_get_json", lambda url, token: {"data": {"candles": []}})

    oi = asyncio.run(hc.fetch_upstox_prev_day_last_tick_oi("NSE_FO|68786", "TOKEN", max_step_back=3))
    assert oi is None
