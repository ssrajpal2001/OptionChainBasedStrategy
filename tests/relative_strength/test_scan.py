"""
Integration tests for strategies/relative_strength/scan.py -- drives the
real scan_sectors/scan_sector_stocks/run_full_scan orchestration with every
network call (Upstox candle fetch, NSE constituent fetch, Upstox instrument
key resolution) mocked, so the ranking/skip-on-failure/best-sector-selection
logic is verified without hitting any real network.
"""
import asyncio
from datetime import date, timedelta
from unittest.mock import AsyncMock, patch

import strategies.relative_strength.scan as scan_mod


def _weekly_candles(n: int, start: float, step: float) -> list:
    # Real incrementing dates, not a hand-formatted string -- align_closes
    # sorts on the raw 'ts' value, and a naive "2020-01-{i:02d}" string
    # stops sorting correctly past index 99 (lexicographic, not numeric).
    base_date = date(2020, 1, 1)
    return [{"ts": (base_date + timedelta(weeks=i)).isoformat(), "close": start + i * step}
            for i in range(n)]


def test_scan_sectors_ranks_by_rs_and_skips_unresolvable_sector():
    nifty = _weekly_candles(150, 100.0, 0.3)
    strong_sector = _weekly_candles(150, 100.0, 1.0)   # clearly outperforms NIFTY
    weak_sector = _weekly_candles(150, 100.0, 0.1)     # clearly underperforms NIFTY

    async def _fake_weekly(key, token):
        if key == scan_mod._NIFTY_KEY:
            return nifty
        if key == "KEY_STRONG":
            return strong_sector
        if key == "KEY_WEAK":
            return weak_sector
        return []  # unresolvable/empty sector

    def _fake_resolve(name):
        return {"NIFTY AUTO": "KEY_STRONG", "NIFTY BANK": "KEY_WEAK"}.get(name)

    with patch.object(scan_mod, "fetch_upstox_weekly", _fake_weekly), \
         patch.object(scan_mod, "resolve_index_key", _fake_resolve), \
         patch.object(scan_mod, "SECTOR_INDEX_NAMES", ["NIFTY AUTO", "NIFTY BANK", "NIFTY IT"]):
        result = asyncio.run(scan_mod.scan_sectors("tok"))

    # NIFTY IT has no resolvable key -> skipped entirely, not a crash.
    names = [r.sector_name for r in result]
    assert names == ["NIFTY AUTO", "NIFTY BANK"]
    assert result[0].rs.rs > result[1].rs.rs


def test_scan_sectors_returns_empty_when_nifty_candles_missing():
    async def _fake_weekly(key, token):
        return []

    with patch.object(scan_mod, "fetch_upstox_weekly", _fake_weekly):
        result = asyncio.run(scan_mod.scan_sectors("tok"))
    assert result == []


def test_scan_sector_stocks_ranks_constituents_and_skips_unresolvable_symbol():
    sector_idx = _weekly_candles(150, 100.0, 0.3)
    strong_stock = _weekly_candles(150, 100.0, 1.2)
    weak_stock = _weekly_candles(150, 100.0, 0.1)

    async def _fake_hourly(key, token):
        if key == "SECTOR_KEY":
            return sector_idx
        if key == "KEY_STRONG_STOCK":
            return strong_stock
        if key == "KEY_WEAK_STOCK":
            return weak_stock
        return []

    def _fake_resolve_eq(symbol):
        return {"STRONGCO": "KEY_STRONG_STOCK", "WEAKCO": "KEY_WEAK_STOCK"}.get(symbol, "")

    def _fake_constituents(nse, names):
        return {"NIFTY AUTO": ["STRONGCO", "WEAKCO", "NOKEYCO"]}

    with patch.object(scan_mod, "fetch_upstox_hourly", _fake_hourly), \
         patch.object(scan_mod, "resolve_eq_instrument_key", _fake_resolve_eq), \
         patch.object(scan_mod, "fetch_all_sector_constituents", _fake_constituents), \
         patch.object(scan_mod, "fetch_large_cap_universe", lambda nse: set()), \
         patch.object(scan_mod, "NSESession", lambda: object()):
        result = asyncio.run(scan_mod.scan_sector_stocks("NIFTY AUTO", "SECTOR_KEY", "tok"))

    symbols = [r.symbol for r in result]
    assert symbols == ["STRONGCO", "WEAKCO"], "NOKEYCO must be skipped (unresolvable key), not crash"
    assert result[0].rs.rs > result[1].rs.rs
    assert result[0].is_large_cap is False and result[1].is_large_cap is False


def test_scan_sector_stocks_weights_large_cap_rs_down_before_ranking():
    """2026-09-30 direct user spec: for the 'wealth creation' use case, a
    mid/small-cap stock should be preferred over a blue-chip one with a
    similar raw RS -- STRONGCO has the higher RAW rs, but it's in the
    live-fetched large-cap set, so its weighted score should drop enough for
    the mid/small-cap WEAKISH_BUT_SMALL stock to rank first instead."""
    sector_idx = _weekly_candles(150, 100.0, 0.3)
    strong_bluechip = _weekly_candles(150, 100.0, 1.0)      # raw RS clearly higher...
    decent_midcap = _weekly_candles(150, 100.0, 0.8)        # ...but still strong on its own

    async def _fake_hourly(key, token):
        return {"SECTOR_KEY": sector_idx, "KEY_BLUECHIP": strong_bluechip,
                "KEY_MIDCAP": decent_midcap}.get(key, [])

    def _fake_resolve_eq(symbol):
        return {"STRONGCO": "KEY_BLUECHIP", "MIDCO": "KEY_MIDCAP"}.get(symbol, "")

    def _fake_constituents(nse, names):
        return {"NIFTY AUTO": ["STRONGCO", "MIDCO"]}

    with patch.object(scan_mod, "fetch_upstox_hourly", _fake_hourly), \
         patch.object(scan_mod, "resolve_eq_instrument_key", _fake_resolve_eq), \
         patch.object(scan_mod, "fetch_all_sector_constituents", _fake_constituents), \
         patch.object(scan_mod, "fetch_large_cap_universe", lambda nse: {"STRONGCO"}), \
         patch.object(scan_mod, "NSESession", lambda: object()):
        result = asyncio.run(scan_mod.scan_sector_stocks("NIFTY AUTO", "SECTOR_KEY", "tok"))

    by_symbol = {r.symbol: r for r in result}
    assert by_symbol["STRONGCO"].rs.rs > by_symbol["MIDCO"].rs.rs, \
        "sanity check: STRONGCO really must have the higher RAW rs for this test to prove anything"
    assert by_symbol["STRONGCO"].is_large_cap is True
    assert by_symbol["MIDCO"].is_large_cap is False
    assert by_symbol["STRONGCO"].cap_weighted_rs == by_symbol["STRONGCO"].rs.rs * scan_mod._LARGE_CAP_RS_WEIGHT
    assert by_symbol["MIDCO"].cap_weighted_rs == by_symbol["MIDCO"].rs.rs
    # Final ranking uses the WEIGHTED score -- the mid-cap now wins despite the lower raw RS.
    assert [r.symbol for r in result] == ["MIDCO", "STRONGCO"]


def test_scan_sector_stocks_does_not_penalize_when_large_cap_fetch_fails():
    """fetch_large_cap_universe degrading to an empty set (network failure)
    must not crash the whole scan -- no penalty applied that run, ranking
    falls back to plain raw RS."""
    sector_idx = _weekly_candles(150, 100.0, 0.3)
    stock_candles = _weekly_candles(150, 100.0, 1.0)

    async def _fake_hourly(key, token):
        return {"SECTOR_KEY": sector_idx, "KEY_A": stock_candles}.get(key, [])

    with patch.object(scan_mod, "fetch_upstox_hourly", _fake_hourly), \
         patch.object(scan_mod, "resolve_eq_instrument_key", lambda s: {"ONLYCO": "KEY_A"}.get(s, "")), \
         patch.object(scan_mod, "fetch_all_sector_constituents", lambda nse, names: {"NIFTY AUTO": ["ONLYCO"]}), \
         patch.object(scan_mod, "fetch_large_cap_universe", lambda nse: set()), \
         patch.object(scan_mod, "NSESession", lambda: object()):
        result = asyncio.run(scan_mod.scan_sector_stocks("NIFTY AUTO", "SECTOR_KEY", "tok"))

    assert len(result) == 1
    assert result[0].is_large_cap is False
    assert result[0].cap_weighted_rs == result[0].rs.rs


def test_scan_sector_stocks_returns_empty_when_no_sector_candles():
    async def _fake_hourly(key, token):
        return []

    with patch.object(scan_mod, "fetch_upstox_hourly", _fake_hourly):
        result = asyncio.run(scan_mod.scan_sector_stocks("NIFTY AUTO", "SECTOR_KEY", "tok"))
    assert result == []


def test_run_full_scan_picks_best_sector_then_ranks_its_stocks():
    from strategies.relative_strength.detector import RSReading
    best = scan_mod.SectorScanResult(
        sector_name="NIFTY AUTO", sector_key="KEY_AUTO",
        rs=RSReading(symbol="NIFTY AUTO", rs=0.5, rs_trend="rising", rs_ma=None, ma_trend=None),
    )
    second = scan_mod.SectorScanResult(
        sector_name="NIFTY BANK", sector_key="KEY_BANK",
        rs=RSReading(symbol="NIFTY BANK", rs=0.1, rs_trend="flat", rs_ma=None, ma_trend=None),
    )
    stock_results = [
        scan_mod.StockScanResult(symbol=f"S{i}", stock_key=f"K{i}",
                                  rs=RSReading(symbol=f"S{i}", rs=1.0 - i * 0.01,
                                               rs_trend="flat", rs_ma=None, ma_trend=None))
        for i in range(15)
    ]

    with patch.object(scan_mod, "scan_sectors", AsyncMock(return_value=[best, second])), \
         patch.object(scan_mod, "scan_sector_stocks", AsyncMock(return_value=stock_results)):
        result = asyncio.run(scan_mod.run_full_scan("tok", top_n_stocks=10))

    assert result.best_sector.sector_name == "NIFTY AUTO"
    assert len(result.stocks_ranked) == 10
    assert result.sectors_ranked == [best, second]


def test_run_full_scan_no_sectors_returns_empty_result():
    async def _empty_sectors(token):
        return []

    with patch.object(scan_mod, "scan_sectors", _empty_sectors):
        result = asyncio.run(scan_mod.run_full_scan("tok"))

    assert result.best_sector is None
    assert result.stocks_ranked == []
    assert result.sectors_ranked == []
