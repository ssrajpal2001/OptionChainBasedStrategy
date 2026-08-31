"""
Unit tests for strategies/sell_straddle/exits.py's `_compute_day_low_for_pair`
-- the ONE-TIME REST-fetch day-low calculation (2026-08-21 redesign) behind
the day-low reversal exit (see tests/strategies/test_day_low_reversal_exit.py
for the higher-level `_check_exits()` integration).

Combines CE/PE 1-min CLOSE (= LTP) prices, keyed by (hour, minute) rather than
the raw ISO string. 2026-08-31 CRITICAL FIX, direct user spec + real-data
verification: this briefly used LOW instead (2026-08-21: "we want the low
value not the close value"), but verified live that the low-based per-minute
sum can find its minimum at a moment the real combined premium never actually
traded at (two legs' own candle-lows can occur hours apart) -- reverted to
CLOSE, matching how a real trader's chart (and the live tracker itself,
ce_ltp+pe_ltp) actually reads the position's value. Only bars at/before
`cutoff` (a datetime.time) are considered. Returns float('inf') on any
failure so the caller always falls back to the current tick's own value.
"""
import asyncio
from datetime import date, time as dtime
from unittest.mock import AsyncMock, patch

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy


def _strategy():
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
    return s


def _bar(ts: str, low: float) -> dict:
    return {"ts": ts, "open": low + 5.0, "high": low + 8.0, "low": low, "close": low + 3.0, "volume": 0}


def test_returns_min_of_minute_aligned_combined_low():
    s = _strategy()
    ce_bars = [_bar("2026-08-21T09:15:00+05:30", 40.0),
               _bar("2026-08-21T09:16:00+05:30", 30.0),
               _bar("2026-08-21T09:17:00+05:30", 38.0)]
    pe_bars = [_bar("2026-08-21T09:15:00+05:30", 35.0),
               _bar("2026-08-21T09:16:00+05:30", 20.0),
               _bar("2026-08-21T09:17:00+05:30", 33.0)]
    # _bar() sets close = low + 3.0. Combined CLOSE per minute:
    # 09:15=(40+3)+(35+3)=81, 09:16=(30+3)+(20+3)=56, 09:17=(38+3)+(33+3)=77 -> min = 56

    with patch("data_layer.client_db.ClientDB.get_feeder_creds_sync", return_value={"access_token": "TOK"}), \
         patch("data_layer.instrument_registry.REGISTRY.get_active_expiry", return_value=date(2026, 8, 21)), \
         patch("data_layer.instrument_registry.REGISTRY.get_broker_symbol",
               side_effect=lambda *a, **k: "CE_KEY" if a[3] == "CE" else "PE_KEY"), \
         patch("data_layer.historical_candles.fetch_upstox_intraday_1m",
               new=AsyncMock(side_effect=lambda key, tok: ce_bars if key == "CE_KEY" else pe_bars)):
        result = asyncio.run(s._compute_day_low_for_pair(24000, 24000, dtime(15, 0)))

    assert result == 56.0


def test_matches_by_hour_minute_even_if_seconds_differ():
    """Robustness fix (2026-08-21, user spec): key by (hour, minute), not the
    raw ISO string -- a differing :seconds value between the two legs' bars
    for the "same" minute must still align correctly."""
    s = _strategy()
    ce_bars = [_bar("2026-08-21T09:16:00+05:30", 30.0)]
    pe_bars = [_bar("2026-08-21T09:16:47+05:30", 20.0)]   # same minute, different second

    with patch("data_layer.client_db.ClientDB.get_feeder_creds_sync", return_value={"access_token": "TOK"}), \
         patch("data_layer.instrument_registry.REGISTRY.get_active_expiry", return_value=date(2026, 8, 21)), \
         patch("data_layer.instrument_registry.REGISTRY.get_broker_symbol",
               side_effect=lambda *a, **k: "CE_KEY" if a[3] == "CE" else "PE_KEY"), \
         patch("data_layer.historical_candles.fetch_upstox_intraday_1m",
               new=AsyncMock(side_effect=lambda key, tok: ce_bars if key == "CE_KEY" else pe_bars)):
        result = asyncio.run(s._compute_day_low_for_pair(24000, 24000, dtime(15, 0)))

    assert result == 56.0   # (30+3) + (20+3) = 56, aligned despite differing seconds


def test_bars_after_cutoff_are_excluded():
    s = _strategy()
    ce_bars = [_bar("2026-08-21T09:15:00+05:30", 40.0),
               _bar("2026-08-21T15:05:00+05:30", 1.0)]   # after cutoff, must be ignored
    pe_bars = [_bar("2026-08-21T09:15:00+05:30", 35.0),
               _bar("2026-08-21T15:05:00+05:30", 1.0)]

    with patch("data_layer.client_db.ClientDB.get_feeder_creds_sync", return_value={"access_token": "TOK"}), \
         patch("data_layer.instrument_registry.REGISTRY.get_active_expiry", return_value=date(2026, 8, 21)), \
         patch("data_layer.instrument_registry.REGISTRY.get_broker_symbol",
               side_effect=lambda *a, **k: "CE_KEY" if a[3] == "CE" else "PE_KEY"), \
         patch("data_layer.historical_candles.fetch_upstox_intraday_1m",
               new=AsyncMock(side_effect=lambda key, tok: ce_bars if key == "CE_KEY" else pe_bars)):
        result = asyncio.run(s._compute_day_low_for_pair(24000, 24000, dtime(15, 0)))

    # only surviving bar: (40+3)+(35+3)=81; the after-cutoff (1+3)+(1+3)=8 bar
    # must be excluded, not win as the min
    assert result == 81.0


def test_returns_inf_when_no_token():
    s = _strategy()
    with patch("data_layer.client_db.ClientDB.get_feeder_creds_sync", return_value={}):
        result = asyncio.run(s._compute_day_low_for_pair(24000, 24000, dtime(15, 0)))
    assert result == float("inf")


def test_returns_inf_for_crypto():
    s = _strategy()
    s._is_crypto = True
    result = asyncio.run(s._compute_day_low_for_pair(24000, 24000, dtime(15, 0)))
    assert result == float("inf")


def test_returns_inf_when_no_overlapping_minutes():
    s = _strategy()
    ce_bars = [_bar("2026-08-21T09:15:00+05:30", 40.0)]
    pe_bars = [_bar("2026-08-21T09:20:00+05:30", 20.0)]   # no shared minute with ce_bars

    with patch("data_layer.client_db.ClientDB.get_feeder_creds_sync", return_value={"access_token": "TOK"}), \
         patch("data_layer.instrument_registry.REGISTRY.get_active_expiry", return_value=date(2026, 8, 21)), \
         patch("data_layer.instrument_registry.REGISTRY.get_broker_symbol",
               side_effect=lambda *a, **k: "CE_KEY" if a[3] == "CE" else "PE_KEY"), \
         patch("data_layer.historical_candles.fetch_upstox_intraday_1m",
               new=AsyncMock(side_effect=lambda key, tok: ce_bars if key == "CE_KEY" else pe_bars)):
        result = asyncio.run(s._compute_day_low_for_pair(24000, 24000, dtime(15, 0)))

    assert result == float("inf")


def test_returns_inf_when_broker_symbol_missing():
    s = _strategy()
    with patch("data_layer.client_db.ClientDB.get_feeder_creds_sync", return_value={"access_token": "TOK"}), \
         patch("data_layer.instrument_registry.REGISTRY.get_active_expiry", return_value=date(2026, 8, 21)), \
         patch("data_layer.instrument_registry.REGISTRY.get_broker_symbol", return_value=""):
        result = asyncio.run(s._compute_day_low_for_pair(24000, 24000, dtime(15, 0)))
    assert result == float("inf")


def test_returns_inf_when_fetch_raises():
    s = _strategy()
    with patch("data_layer.client_db.ClientDB.get_feeder_creds_sync", return_value={"access_token": "TOK"}), \
         patch("data_layer.instrument_registry.REGISTRY.get_active_expiry", return_value=date(2026, 8, 21)), \
         patch("data_layer.instrument_registry.REGISTRY.get_broker_symbol",
               side_effect=lambda *a, **k: "CE_KEY" if a[3] == "CE" else "PE_KEY"), \
         patch("data_layer.historical_candles.fetch_upstox_intraday_1m",
               new=AsyncMock(side_effect=RuntimeError("network down"))):
        result = asyncio.run(s._compute_day_low_for_pair(24000, 24000, dtime(15, 0)))
    assert result == float("inf")
