"""Unit tests for OptionChainSelector strike-by-premium logic."""
from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest

from strategies.trap_scanner.option_chain_selector import OptionChainSelector


def _make_chain():
    """Fake Upstox-style option-chain payload."""
    return {
        "status": "success",
        "data": [
            {
                "strike_price": 24300.0,
                "call_options": {"instrument_key": "k-ce-24300", "market_data": {"ltp": 210.0}},
                "put_options": {"instrument_key": "k-pe-24300", "market_data": {"ltp": 85.0}},
            },
            {
                "strike_price": 24350.0,
                "call_options": {"instrument_key": "k-ce-24350", "market_data": {"ltp": 175.0}},
                "put_options": {"instrument_key": "k-pe-24350", "market_data": {"ltp": 98.0}},
            },
            {
                "strike_price": 24400.0,
                "call_options": {"instrument_key": "k-ce-24400", "market_data": {"ltp": 140.0}},
                "put_options": {"instrument_key": "k-pe-24400", "market_data": {"ltp": 120.0}},
            },
            {
                "strike_price": 24450.0,
                "call_options": {"instrument_key": "k-ce-24450", "market_data": {"ltp": 95.0}},
                "put_options": {"instrument_key": "k-pe-24450", "market_data": {"ltp": 155.0}},
            },
            {
                "strike_price": 24500.0,
                "call_options": {"instrument_key": "k-ce-24500", "market_data": {"ltp": 70.0}},
                "put_options": {"instrument_key": "k-pe-24500", "market_data": {"ltp": 190.0}},
            },
        ],
    }


@pytest.fixture
def selector():
    registry = MagicMock()
    registry.get_upstox_index_key.return_value = "NSE_INDEX|Nifty 50"
    rebalancer = MagicMock()
    rebalancer.fetch_option_chain = AsyncMock(return_value=_make_chain())
    return OptionChainSelector(registry, rebalancer)


@pytest.mark.asyncio
async def test_pe_nearest_below_100(selector):
    selected = await selector.select_strike_by_premium(
        "NIFTY", date(2026, 7, 7), "PE", 100.0, "nearest_below"
    )
    assert selected is not None
    assert selected.strike == 24350
    assert selected.ltp == 98.0
    assert selected.instrument_key == "k-pe-24350"


@pytest.mark.asyncio
async def test_ce_cheapest_below_100(selector):
    selected = await selector.select_strike_by_premium(
        "NIFTY", date(2026, 7, 7), "CE", 100.0, "cheapest"
    )
    assert selected is not None
    assert selected.strike == 24500
    assert selected.ltp == 70.0


@pytest.mark.asyncio
async def test_no_strike_below_target(selector):
    selected = await selector.select_strike_by_premium(
        "NIFTY", date(2026, 7, 7), "PE", 50.0, "nearest_below"
    )
    assert selected is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
