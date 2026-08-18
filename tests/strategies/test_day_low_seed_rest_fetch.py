"""
Unit tests for strategies/sell_straddle/exits.py's `_seed_day_low_for_pair` --
the REST-history seeding step behind the day-low reversal exit's "from
scratch means seeded from real 09:15-onward history, not a blank slate"
correction (see tests/strategies/test_day_low_reversal_exit.py for the
higher-level `_check_exits()` integration).

Combines CE/PE 1-min CLOSE prices minute-by-minute (aligned by timestamp) --
deliberately NOT each leg's own independent low -- since summing two legs'
separate lows would combine two price extremes that almost certainly never
occurred at the same instant. Returns float('inf') on any failure so the
caller's min(seed, live_tick) always degrades safely.
"""
import asyncio
from datetime import date
from unittest.mock import AsyncMock, patch

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy


def _strategy():
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
    return s


def _bar(ts: str, close: float) -> dict:
    return {"ts": ts, "open": close, "high": close, "low": close - 5.0, "close": close, "volume": 0}


def test_seed_returns_min_of_minute_aligned_combined_close():
    s = _strategy()
    ce_bars = [_bar("t1", 40.0), _bar("t2", 30.0), _bar("t3", 38.0)]
    pe_bars = [_bar("t1", 35.0), _bar("t2", 20.0), _bar("t3", 33.0)]
    # combined per minute: t1=75, t2=50, t3=71 -> min = 50 (at t2, genuinely simultaneous)

    with patch("data_layer.client_db.ClientDB.get_feeder_creds_sync", return_value={"access_token": "TOK"}), \
         patch("data_layer.instrument_registry.REGISTRY.get_active_expiry", return_value=date(2026, 8, 20)), \
         patch("data_layer.instrument_registry.REGISTRY.get_broker_symbol",
               side_effect=lambda *a, **k: "CE_KEY" if a[3] == "CE" else "PE_KEY"), \
         patch("data_layer.historical_candles.fetch_upstox_intraday_1m",
               new=AsyncMock(side_effect=lambda key, tok: ce_bars if key == "CE_KEY" else pe_bars)):
        result = asyncio.run(s._seed_day_low_for_pair(24000, 24000))

    assert result == 50.0


def test_seed_does_not_use_each_legs_own_independent_low():
    """If this wrongly summed each leg's own `low` field independently, it
    would compute (30-5)+(20-5)=40 -- lower than the true minute-aligned 50.
    Confirms the real (higher, correct) value wins instead."""
    s = _strategy()
    ce_bars = [_bar("t1", 40.0), _bar("t2", 30.0)]
    pe_bars = [_bar("t1", 35.0), _bar("t2", 20.0)]

    with patch("data_layer.client_db.ClientDB.get_feeder_creds_sync", return_value={"access_token": "TOK"}), \
         patch("data_layer.instrument_registry.REGISTRY.get_active_expiry", return_value=date(2026, 8, 20)), \
         patch("data_layer.instrument_registry.REGISTRY.get_broker_symbol",
               side_effect=lambda *a, **k: "CE_KEY" if a[3] == "CE" else "PE_KEY"), \
         patch("data_layer.historical_candles.fetch_upstox_intraday_1m",
               new=AsyncMock(side_effect=lambda key, tok: ce_bars if key == "CE_KEY" else pe_bars)):
        result = asyncio.run(s._seed_day_low_for_pair(24000, 24000))

    assert result == 50.0, "must use minute-aligned combined close (50), not independent per-leg lows (40)"


def test_seed_returns_inf_when_no_token():
    s = _strategy()
    with patch("data_layer.client_db.ClientDB.get_feeder_creds_sync", return_value={}):
        result = asyncio.run(s._seed_day_low_for_pair(24000, 24000))
    assert result == float("inf")


def test_seed_returns_inf_for_crypto():
    s = _strategy()
    s._is_crypto = True
    result = asyncio.run(s._seed_day_low_for_pair(24000, 24000))
    assert result == float("inf")


def test_seed_returns_inf_when_no_overlapping_minutes():
    s = _strategy()
    ce_bars = [_bar("t1", 40.0)]
    pe_bars = [_bar("t2", 20.0)]   # no shared timestamp with ce_bars

    with patch("data_layer.client_db.ClientDB.get_feeder_creds_sync", return_value={"access_token": "TOK"}), \
         patch("data_layer.instrument_registry.REGISTRY.get_active_expiry", return_value=date(2026, 8, 20)), \
         patch("data_layer.instrument_registry.REGISTRY.get_broker_symbol",
               side_effect=lambda *a, **k: "CE_KEY" if a[3] == "CE" else "PE_KEY"), \
         patch("data_layer.historical_candles.fetch_upstox_intraday_1m",
               new=AsyncMock(side_effect=lambda key, tok: ce_bars if key == "CE_KEY" else pe_bars)):
        result = asyncio.run(s._seed_day_low_for_pair(24000, 24000))

    assert result == float("inf")


def test_seed_returns_inf_when_broker_symbol_missing():
    s = _strategy()
    with patch("data_layer.client_db.ClientDB.get_feeder_creds_sync", return_value={"access_token": "TOK"}), \
         patch("data_layer.instrument_registry.REGISTRY.get_active_expiry", return_value=date(2026, 8, 20)), \
         patch("data_layer.instrument_registry.REGISTRY.get_broker_symbol", return_value=""):
        result = asyncio.run(s._seed_day_low_for_pair(24000, 24000))
    assert result == float("inf")


def test_seed_returns_inf_when_fetch_raises():
    s = _strategy()
    with patch("data_layer.client_db.ClientDB.get_feeder_creds_sync", return_value={"access_token": "TOK"}), \
         patch("data_layer.instrument_registry.REGISTRY.get_active_expiry", return_value=date(2026, 8, 20)), \
         patch("data_layer.instrument_registry.REGISTRY.get_broker_symbol",
               side_effect=lambda *a, **k: "CE_KEY" if a[3] == "CE" else "PE_KEY"), \
         patch("data_layer.historical_candles.fetch_upstox_intraday_1m",
               new=AsyncMock(side_effect=RuntimeError("network down"))):
        result = asyncio.run(s._seed_day_low_for_pair(24000, 24000))
    assert result == float("inf")
